#![allow(clippy::too_many_arguments)]

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{stderr, IsTerminal};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::{Int64Array, StringBuilder};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use clap::Parser;
use console::style;
use dashmap::DashMap;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use jwalk::{Parallelism, WalkDirGeneric};
use parking_lot::Mutex;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use xdu::{format_bytes, format_count, get_schema, FileRecord, SizeMode};

/// Special partition name for files directly in the top-level directory.
const ROOT_PARTITION: &str = "__root__";

#[derive(Parser, Debug)]
#[command(name = "xdu", about = "Build a distributed file metadata index in Parquet format")]
struct Args {
    /// Top-level directory to index
    #[arg(value_name = "DIR")]
    dir: PathBuf,

    /// Output directory for the Parquet index
    #[arg(short, long, value_name = "DIR")]
    outdir: PathBuf,

    /// Number of parallel threads
    #[arg(short, long, default_value = "4")]
    jobs: usize,

    /// Number of records per output chunk
    #[arg(short = 'B', long, default_value = "100000")]
    buffsize: usize,

    /// Report apparent sizes (file length) rather than disk usage.
    /// By default, xdu reports actual disk usage from st_blocks.
    #[arg(long)]
    apparent_size: bool,

    /// Round sizes up to this block size (e.g., 128K, 1M).
    /// Useful when st_blocks is inaccurate (e.g., over NFS) and you know the filesystem block size.
    /// Implies --apparent-size for the base size, then rounds up.
    #[arg(short = 'k', long, value_name = "SIZE")]
    block_size: Option<String>,

    /// Index only specific partitions (top-level subdirectory names, comma-separated).
    #[arg(short, long, value_name = "NAMES", value_delimiter = ',')]
    partition: Option<Vec<String>>,
}

/// Parse a human-readable size string into bytes.
fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim().to_uppercase();
    let (num, mult) = if let Some(n) = s.strip_suffix("TIB") {
        (n, 1024_u64 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("T") {
        (n, 1024_u64 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("GIB") {
        (n, 1024_u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("G") {
        (n, 1024_u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MIB") {
        (n, 1024_u64 * 1024)
    } else if let Some(n) = s.strip_suffix("M") {
        (n, 1024_u64 * 1024)
    } else if let Some(n) = s.strip_suffix("KIB") {
        (n, 1024_u64)
    } else if let Some(n) = s.strip_suffix("K") {
        (n, 1024_u64)
    } else if let Some(n) = s.strip_suffix("B") {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };

    let num: f64 = num.trim().parse()
        .with_context(|| format!("Invalid size: {}", s))?;
    Ok((num * mult as f64) as u64)
}

/// Per-partition buffer that accumulates records and flushes to Parquet.
struct PartitionBuffer {
    partition: String,
    outdir: PathBuf,
    records: Vec<FileRecord>,
    buffsize: usize,
    chunk_counter: AtomicUsize,
    schema: Arc<Schema>,
    /// Track all .partial files written for atomic finalization
    partial_files: Vec<PathBuf>,
    /// Track statistics for this partition
    file_count: u64,
    byte_count: u64,
}

impl PartitionBuffer {
    fn new(partition: String, outdir: PathBuf, buffsize: usize, schema: Arc<Schema>) -> Self {
        Self {
            partition,
            outdir,
            records: Vec::with_capacity(buffsize),
            buffsize,
            chunk_counter: AtomicUsize::new(0),
            schema,
            partial_files: Vec::new(),
            file_count: 0,
            byte_count: 0,
        }
    }

    fn add(&mut self, record: FileRecord) -> Result<()> {
        self.file_count += 1;
        self.byte_count += record.size as u64;
        self.records.push(record);
        if self.records.len() >= self.buffsize {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }

        let chunk_id = self.chunk_counter.fetch_add(1, Ordering::SeqCst);
        let partition_dir = self.outdir.join(&self.partition);
        fs::create_dir_all(&partition_dir)
            .with_context(|| format!("Failed to create partition dir: {}", partition_dir.display()))?;

        // Write to .partial file first
        let partial_path = partition_dir.join(format!("{:06}.parquet.partial", chunk_id));

        let mut path_builder = StringBuilder::new();
        let mut size_builder = Vec::with_capacity(self.records.len());
        let mut atime_builder = Vec::with_capacity(self.records.len());

        for record in &self.records {
            path_builder.append_value(&record.path);
            size_builder.push(record.size);
            atime_builder.push(record.atime);
        }

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(path_builder.finish()),
                Arc::new(Int64Array::from(size_builder)),
                Arc::new(Int64Array::from(atime_builder)),
            ],
        )?;

        let file = File::create(&partial_path)
            .with_context(|| format!("Failed to create file: {}", partial_path.display()))?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        self.partial_files.push(partial_path);
        self.records.clear();
        Ok(())
    }

    /// Atomically finalize all .partial files by renaming them and pruning stale chunks.
    fn finalize(&self) -> Result<usize> {
        let partition_dir = self.outdir.join(&self.partition);
        let num_chunks = self.partial_files.len();

        // Rename all .partial files to .parquet (atomic on POSIX)
        for partial_path in &self.partial_files {
            let final_path = partial_path.with_extension(""); // removes .partial, leaves .parquet
            fs::rename(partial_path, &final_path)
                .with_context(|| format!("Failed to rename {} to {}", partial_path.display(), final_path.display()))?;
        }

        // Prune any stale chunks beyond what we just wrote
        let mut pruned = 0;
        for chunk_id in num_chunks.. {
            let stale_path = partition_dir.join(format!("{:06}.parquet", chunk_id));
            if stale_path.exists() {
                fs::remove_file(&stale_path)
                    .with_context(|| format!("Failed to remove stale chunk: {}", stale_path.display()))?;
                pruned += 1;
            } else {
                break; // No more consecutive chunks
            }
        }

        Ok(pruned)
    }
}

/// Extract the partition name from a file path relative to the top directory.
/// Returns `__root__` for files directly in the top directory.
fn extract_partition(path: &Path, top_dir: &Path) -> Option<String> {
    let relative = path.strip_prefix(top_dir).ok()?;
    let mut components = relative.components();
    
    // Get first component (partition directory or filename if at root)
    let first = components.next()?;
    
    // If there's a second component, first is the partition; otherwise file is at root
    if components.next().is_some() {
        Some(first.as_os_str().to_string_lossy().to_string())
    } else {
        // File is directly in top_dir
        Some(ROOT_PARTITION.to_string())
    }
}

/// Statistics returned from a crawl operation.
struct CrawlStats {
    files: u64,
    bytes: u64,
    pruned: usize,
}

/// Crawl a directory tree using jwalk for adaptive parallelism.
fn crawl(
    top_dir: &Path,
    outdir: &Path,
    jobs: usize,
    buffsize: usize,
    size_mode: SizeMode,
    schema: &Arc<Schema>,
    partition_filter: Option<&HashSet<String>>,
    is_tty: bool,
) -> Result<CrawlStats> {
    let buffers: DashMap<String, Mutex<PartitionBuffer>> = DashMap::new();
    let global_file_count = AtomicU64::new(0);
    let global_byte_count = AtomicU64::new(0);

    // Progress reporting
    let mp = MultiProgress::new();
    if !is_tty {
        mp.set_draw_target(ProgressDrawTarget::hidden());
    }

    let global_style = ProgressStyle::default_bar()
        .template("{spinner:.green} {msg}")
        .unwrap();
    let global_bar = mp.add(ProgressBar::new_spinner());
    global_bar.set_style(global_style);
    global_bar.enable_steady_tick(std::time::Duration::from_millis(100));

    let filter_desc = if let Some(pf) = partition_filter {
        format!(" (partitions: {})", pf.iter().cloned().collect::<Vec<_>>().join(", "))
    } else {
        String::new()
    };

    if is_tty {
        eprintln!("{:>12} {}{}", style("Indexing").green().bold(), top_dir.display(), filter_desc);
    } else {
        eprintln!("Indexing {}{}", top_dir.display(), filter_desc);
    }

    // Configure jwalk for parallel traversal with parallel metadata loading.
    // The client_state stores (blocks, len, atime) fetched in the parallel callback.
    // This ensures stat() calls happen in rayon's thread pool, not the main thread.
    type ClientState = Option<(u64, u64, i64)>; // (blocks * 512, len, atime)
    
    let walker = WalkDirGeneric::<((), ClientState)>::new(top_dir)
        .parallelism(Parallelism::RayonNewPool(jobs))
        .skip_hidden(false)
        .follow_links(false)
        .process_read_dir(|_depth, _path, _state, children| {
            // Sort for deterministic output
            children.sort_by(|a, b| match (a, b) {
                (Ok(a), Ok(b)) => a.file_name.cmp(&b.file_name),
                (Ok(_), Err(_)) => std::cmp::Ordering::Less,
                (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
                (Err(_), Err(_)) => std::cmp::Ordering::Equal,
            });
            
            // Fetch metadata in parallel for files
            for entry in children.iter_mut().flatten() {
                if entry.file_type.is_file()
                    && let Ok(metadata) = entry.metadata()
                {
                    entry.client_state = Some((
                        metadata.blocks() * 512,
                        metadata.len(),
                        metadata.atime(),
                    ));
                }
            }
        });

    let mut last_update = Instant::now();
    let update_interval = std::time::Duration::from_millis(500);

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue, // Skip entries we can't read
        };

        // Skip non-files and entries without metadata
        if !entry.file_type.is_file() {
            continue;
        }
        
        let (disk_usage, file_len, atime) = match entry.client_state {
            Some(state) => state,
            None => continue, // Skip if metadata fetch failed
        };

        let path = entry.path();
        
        // Extract partition name
        let partition = match extract_partition(&path, top_dir) {
            Some(p) => p,
            None => continue, // Skip if we can't determine partition
        };

        // Apply partition filter if specified
        if let Some(pf) = partition_filter
            && partition != ROOT_PARTITION
            && !pf.contains(&partition)
        {
            continue;
        }

        let file_size = size_mode.calculate(disk_usage, file_len);
        let record = FileRecord {
            path: path.to_string_lossy().to_string(),
            size: file_size as i64,
            atime,
        };

        // Get or create buffer for this partition
        let buffer_entry = buffers.entry(partition.clone()).or_insert_with(|| {
            Mutex::new(PartitionBuffer::new(
                partition,
                outdir.to_path_buf(),
                buffsize,
                schema.clone(),
            ))
        });
        
        if let Err(e) = buffer_entry.lock().add(record) {
            eprintln!("Warning: failed to add record: {}", e);
            continue;
        }

        global_file_count.fetch_add(1, Ordering::Relaxed);
        global_byte_count.fetch_add(file_size, Ordering::Relaxed);

        // Update progress periodically
        let now = Instant::now();
        if now.duration_since(last_update) >= update_interval {
            let files = global_file_count.load(Ordering::Relaxed);
            let bytes = global_byte_count.load(Ordering::Relaxed);
            let partitions = buffers.len();
            global_bar.set_message(format!(
                "{} partitions, {} files, {}",
                partitions, format_count(files), format_bytes(bytes)
            ));
            last_update = now;
        }
    }

    global_bar.finish_and_clear();

    // Finalize all buffers and print per-partition summaries
    let mut total_pruned = 0;
    let mut partition_names: Vec<String> = buffers.iter().map(|e| e.key().clone()).collect();
    partition_names.sort();

    for partition_name in partition_names {
        if let Some(entry) = buffers.get(&partition_name) {
            let mut buffer = entry.lock();
            buffer.flush()?;
            let pruned = buffer.finalize()?;
            total_pruned += pruned;

            let prune_info = if pruned > 0 { format!(", pruned {} stale", pruned) } else { String::new() };

            if is_tty {
                mp.println(format!("{:>12} {} ({} files, {}{})",
                    style("Finished").green().bold(),
                    partition_name,
                    format_count(buffer.file_count),
                    format_bytes(buffer.byte_count),
                    prune_info
                ))?;
            } else {
                eprintln!("Finished {} ({} files, {}{})", 
                    partition_name, 
                    format_count(buffer.file_count), 
                    format_bytes(buffer.byte_count),
                    prune_info
                );
            }
        }
    }

    Ok(CrawlStats {
        files: global_file_count.load(Ordering::Relaxed),
        bytes: global_byte_count.load(Ordering::Relaxed),
        pruned: total_pruned,
    })
}

fn main() -> Result<()> {
    let args = Args::parse();
    let start_time = Instant::now();
    let is_tty = stderr().is_terminal();

    // Determine size calculation mode
    let size_mode = if let Some(ref bs) = args.block_size {
        let block_size = parse_size(bs)?;
        SizeMode::BlockRounded(block_size)
    } else if args.apparent_size {
        SizeMode::ApparentSize
    } else {
        SizeMode::DiskUsage
    };

    let top_dir = args.dir.canonicalize()
        .with_context(|| format!("Failed to resolve directory: {}", args.dir.display()))?;

    fs::create_dir_all(&args.outdir)
        .with_context(|| format!("Failed to create output directory: {}", args.outdir.display()))?;

    let outdir = args.outdir.canonicalize()?;

    let schema = get_schema();

    // Build partition filter if specified
    let partition_filter: Option<HashSet<String>> = args.partition.map(|p| p.into_iter().collect());

    // Validate partition filter if specified
    if let Some(ref pf) = partition_filter {
        for partition_name in pf {
            let partition_path = top_dir.join(partition_name);
            if !partition_path.is_dir() {
                anyhow::bail!("Partition '{}' not found in {}", partition_name, top_dir.display());
            }
        }
    }

    let stats = crawl(
        &top_dir,
        &outdir,
        args.jobs,
        args.buffsize,
        size_mode,
        &schema,
        partition_filter.as_ref(),
        is_tty,
    )?;

    let elapsed = start_time.elapsed();
    let prune_info = if stats.pruned > 0 { format!(", pruned {} stale", stats.pruned) } else { String::new() };

    if is_tty {
        eprintln!("{:>12} {} files ({}) in {:.2}s{}",
            style("Completed").green().bold(),
            format_count(stats.files),
            format_bytes(stats.bytes),
            elapsed.as_secs_f64(),
            prune_info
        );
    } else {
        eprintln!("Completed {} files ({}) in {:.2}s{}", format_count(stats.files), format_bytes(stats.bytes), elapsed.as_secs_f64(), prune_info);
    }

    Ok(())
}
