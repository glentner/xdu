#![allow(clippy::too_many_arguments)]
#![allow(clippy::manual_is_multiple_of)]

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
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use parking_lot::Mutex;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use walkdir::WalkDir;

use xdu::{format_bytes, format_count, get_schema, CrawlUnit, FileRecord, SizeMode};

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

    /// Index one or more partitions (top-level subdirectory names, comma-separated).
    /// All threads will be used to crawl each partition in parallel.
    #[arg(short, long, value_name = "NAMES", value_delimiter = ',')]
    partition: Option<Vec<String>>,

    /// After crawling specified partitions, do a full crawl excluding them.
    /// Use with --partition to first crawl large partitions with full parallelism,
    /// then crawl remaining partitions with one-thread-per-partition parallelism.
    #[arg(long, requires = "partition")]
    fast: bool,
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
        }
    }

    fn add(&mut self, record: FileRecord) -> Result<()> {
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

/// Helper to calculate file size from metadata using the given SizeMode.
fn calculate_size(size_mode: SizeMode, metadata: &std::fs::Metadata) -> u64 {
    size_mode.calculate(metadata.blocks() * 512, metadata.len())
}

/// Crawl a directory tree, writing records to the buffer.
fn crawl_directory(
    unit: &CrawlUnit,
    buffer: &Arc<Mutex<PartitionBuffer>>,
    size_mode: SizeMode,
    unit_file_count: &AtomicU64,
    unit_byte_count: &AtomicU64,
    global_file_count: &AtomicU64,
    global_byte_count: &AtomicU64,
    progress_bar: &ProgressBar,
    global_bar: &ProgressBar,
    completed_count: &AtomicUsize,
    total_count: usize,
    count_label: &str,
) -> Result<()> {
    for entry in WalkDir::new(&unit.path)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let metadata = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        let file_size = calculate_size(size_mode, &metadata);
        let record = FileRecord {
            path: path.to_string_lossy().to_string(),
            size: file_size as i64,
            atime: metadata.atime(),
        };
        buffer.lock().add(record)?;

        let count = unit_file_count.fetch_add(1, Ordering::Relaxed) + 1;
        unit_byte_count.fetch_add(file_size, Ordering::Relaxed);
        global_file_count.fetch_add(1, Ordering::Relaxed);
        global_byte_count.fetch_add(file_size, Ordering::Relaxed);

        if count % 1000 == 0 {
            let unit_bytes = unit_byte_count.load(Ordering::Relaxed);
            progress_bar.set_message(format!("{} files, {}", format_count(count), format_bytes(unit_bytes)));
            let total_files = global_file_count.load(Ordering::Relaxed);
            let total_bytes = global_byte_count.load(Ordering::Relaxed);
            let done = completed_count.load(Ordering::Relaxed);
            global_bar.set_message(format!(
                "{}/{} {}, {} files, {}",
                done, total_count, count_label, format_count(total_files), format_bytes(total_bytes)
            ));
        }
    }
    Ok(())
}

/// Statistics returned from a crawl operation.
struct CrawlStats {
    files: u64,
    bytes: u64,
    pruned: usize,
}

/// Crawl a single partition using all threads (parallel within partition).
/// This parallelizes across subdirectories within the partition.
fn crawl_partition(
    partition_name: &str,
    top_dir: &Path,
    outdir: &Path,
    buffsize: usize,
    size_mode: SizeMode,
    schema: &Arc<Schema>,
    is_tty: bool,
) -> Result<CrawlStats> {
    let partition_path = top_dir.join(partition_name);
    if !partition_path.is_dir() {
        anyhow::bail!("Partition '{}' not found in {}", partition_name, top_dir.display());
    }

    // Collect subdirectories for parallel processing
    let mut sub_dirs: Vec<PathBuf> = fs::read_dir(&partition_path)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    sub_dirs.sort();

    // Build work units
    let mut units: Vec<CrawlUnit> = sub_dirs.iter().map(|p| CrawlUnit::new(
        p.clone(),
        format!("{}:{}", partition_name, p.file_name().unwrap().to_string_lossy()),
    )).collect();

    // Also crawl files directly in the partition root (not in subdirs)
    let has_loose_files = fs::read_dir(&partition_path)?
        .filter_map(|e| e.ok())
        .any(|e| e.path().is_file());

    // If no subdirs or has loose files, we need special handling
    if units.is_empty() || has_loose_files {
        units.insert(0, CrawlUnit::new(
            partition_path.clone(),
            format!("{}:.", partition_name),
        ));
    }

    if units.is_empty() {
        if is_tty {
            eprintln!("{} No content found in partition {}", style("warning:").yellow().bold(), partition_name);
        } else {
            eprintln!("warning: No content found in partition {}", partition_name);
        }
        return Ok(CrawlStats { files: 0, bytes: 0, pruned: 0 });
    }

    let total_units = units.len();
    let global_file_count = Arc::new(AtomicU64::new(0));
    let global_byte_count = Arc::new(AtomicU64::new(0));
    let completed_units = Arc::new(AtomicUsize::new(0));

    // Shared buffer for all threads
    let buffer = Arc::new(Mutex::new(PartitionBuffer::new(
        partition_name.to_string(),
        outdir.to_path_buf(),
        buffsize,
        schema.clone(),
    )));

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

    if is_tty {
        eprintln!("{:>12} {} ({} subdirs)",
            style("Indexing").green().bold(),
            partition_path.display(),
            total_units
        );
    } else {
        eprintln!("Indexing {} ({} subdirs)", partition_path.display(), total_units);
    }

    let partition_path_clone = partition_path.clone();

    let result = units.par_iter().try_for_each(|unit| -> Result<()> {
        let unit_style = ProgressStyle::default_bar()
            .template("{spinner:.cyan} {prefix:.bold} {msg}")
            .unwrap();
        let unit_bar = mp.insert_before(&global_bar, ProgressBar::new_spinner());
        unit_bar.set_style(unit_style);
        unit_bar.set_prefix(format!("{:>16}", &unit.label));
        unit_bar.enable_steady_tick(std::time::Duration::from_millis(100));

        let unit_file_count = AtomicU64::new(0);
        let unit_byte_count = AtomicU64::new(0);

        // Special case: if this is the partition root, only process files at depth 1
        if unit.path == partition_path_clone {
            for entry in fs::read_dir(&unit.path)?.filter_map(|e| e.ok()) {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let metadata = match fs::metadata(&path) {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                let file_size = calculate_size(size_mode, &metadata);
                let record = FileRecord {
                    path: path.to_string_lossy().to_string(),
                    size: file_size as i64,
                    atime: metadata.atime(),
                };
                buffer.lock().add(record)?;
                unit_file_count.fetch_add(1, Ordering::Relaxed);
                unit_byte_count.fetch_add(file_size, Ordering::Relaxed);
                global_file_count.fetch_add(1, Ordering::Relaxed);
                global_byte_count.fetch_add(file_size, Ordering::Relaxed);
            }
        } else {
            crawl_directory(
                unit, &buffer, size_mode,
                &unit_file_count, &unit_byte_count,
                &global_file_count, &global_byte_count,
                &unit_bar, &global_bar,
                &completed_units, total_units, "subdirs",
            )?;
        }

        let final_count = unit_file_count.load(Ordering::Relaxed);
        let final_bytes = unit_byte_count.load(Ordering::Relaxed);
        completed_units.fetch_add(1, Ordering::Relaxed);

        unit_bar.finish_and_clear();

        if is_tty {
            mp.println(format!("{:>12} {} ({} files, {})",
                style("Finished").green().bold(),
                unit.label,
                format_count(final_count),
                format_bytes(final_bytes)
            ))?;
        } else {
            eprintln!("Finished {} ({} files, {})", unit.label, format_count(final_count), format_bytes(final_bytes));
        }

        Ok(())
    });

    global_bar.finish_and_clear();
    result?;

    // Finalize the shared buffer
    buffer.lock().flush()?;
    let buf = match Arc::try_unwrap(buffer) {
        Ok(mutex) => mutex.into_inner(),
        Err(_) => panic!("Buffer still has multiple references"),
    };
    let pruned = buf.finalize()?;

    Ok(CrawlStats {
        files: global_file_count.load(Ordering::Relaxed),
        bytes: global_byte_count.load(Ordering::Relaxed),
        pruned,
    })
}

/// Full crawl mode: one partition per work unit, each with its own buffer.
/// Optionally excludes partitions that have already been crawled.
fn crawl_full(
    top_dir: &Path,
    outdir: &Path,
    buffsize: usize,
    size_mode: SizeMode,
    schema: &Arc<Schema>,
    exclude: &HashSet<String>,
    is_tty: bool,
) -> Result<CrawlStats> {
    let mut partitions: Vec<PathBuf> = fs::read_dir(top_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter(|p| {
            let name = p.file_name().unwrap().to_string_lossy();
            !exclude.contains(name.as_ref())
        })
        .collect();
    partitions.sort();

    if partitions.is_empty() {
        if exclude.is_empty() {
            if is_tty {
                eprintln!("{} No subdirectories found in {}", style("warning:").yellow().bold(), top_dir.display());
            } else {
                eprintln!("warning: No subdirectories found in {}", top_dir.display());
            }
        }
        return Ok(CrawlStats { files: 0, bytes: 0, pruned: 0 });
    }

    let total_partitions = partitions.len();
    let global_file_count = Arc::new(AtomicU64::new(0));
    let global_byte_count = Arc::new(AtomicU64::new(0));
    let completed_partitions = Arc::new(AtomicUsize::new(0));
    let total_pruned = Arc::new(AtomicUsize::new(0));

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

    let phase_label = if exclude.is_empty() { "" } else { " (remaining)" };
    if is_tty {
        eprintln!("{:>12} {}{} ({} partitions)",
            style("Indexing").green().bold(),
            top_dir.display(),
            phase_label,
            total_partitions
        );
    } else {
        eprintln!("Indexing {}{} ({} partitions)", top_dir.display(), phase_label, total_partitions);
    }

    let result = partitions.par_iter().try_for_each(|partition_path| -> Result<()> {
        let partition_name = partition_path.file_name().unwrap().to_string_lossy().to_string();

        let unit = CrawlUnit::new(partition_path.clone(), partition_name.clone());

        let partition_style = ProgressStyle::default_bar()
            .template("{spinner:.cyan} {prefix:.bold} {msg}")
            .unwrap();
        let partition_bar = mp.insert_before(&global_bar, ProgressBar::new_spinner());
        partition_bar.set_style(partition_style);
        partition_bar.set_prefix(format!("{:>12}", &unit.label));
        partition_bar.enable_steady_tick(std::time::Duration::from_millis(100));

        let unit_file_count = AtomicU64::new(0);
        let unit_byte_count = AtomicU64::new(0);

        let buffer = Arc::new(Mutex::new(PartitionBuffer::new(
            partition_name.clone(),
            outdir.to_path_buf(),
            buffsize,
            schema.clone(),
        )));

        crawl_directory(
            &unit, &buffer, size_mode,
            &unit_file_count, &unit_byte_count,
            &global_file_count, &global_byte_count,
            &partition_bar, &global_bar,
            &completed_partitions, total_partitions, "partitions",
        )?;

        buffer.lock().flush()?;
        let buf = match Arc::try_unwrap(buffer) {
            Ok(mutex) => mutex.into_inner(),
            Err(_) => panic!("Buffer still has multiple references"),
        };
        let pruned = buf.finalize()?;
        total_pruned.fetch_add(pruned, Ordering::Relaxed);

        let final_count = unit_file_count.load(Ordering::Relaxed);
        let final_bytes = unit_byte_count.load(Ordering::Relaxed);
        completed_partitions.fetch_add(1, Ordering::Relaxed);

        partition_bar.finish_and_clear();

        let prune_info = if pruned > 0 { format!(", pruned {} stale", pruned) } else { String::new() };

        if is_tty {
            mp.println(format!("{:>12} {} ({} files, {}{})",
                style("Finished").green().bold(),
                unit.label,
                format_count(final_count),
                format_bytes(final_bytes),
                prune_info
            ))?;
        } else {
            eprintln!("Finished {} ({} files, {}{})", unit.label, format_count(final_count), format_bytes(final_bytes), prune_info);
        }

        Ok(())
    });

    global_bar.finish_and_clear();
    result?;

    Ok(CrawlStats {
        files: global_file_count.load(Ordering::Relaxed),
        bytes: global_byte_count.load(Ordering::Relaxed),
        pruned: total_pruned.load(Ordering::Relaxed),
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

    // Configure thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.jobs)
        .build_global()?;

    let schema = get_schema();

    // Track total stats across all phases
    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_pruned: usize = 0;

    // Partition mode: crawl specified partitions with full parallelism
    if let Some(ref partition_names) = args.partition {
        // Phase 1: Crawl each specified partition sequentially (each uses all threads internally)
        for partition_name in partition_names {
            let stats = crawl_partition(
                partition_name,
                &top_dir,
                &outdir,
                args.buffsize,
                size_mode,
                &schema,
                is_tty,
            )?;
            total_files += stats.files;
            total_bytes += stats.bytes;
            total_pruned += stats.pruned;

            let prune_info = if stats.pruned > 0 { format!(", pruned {} stale", stats.pruned) } else { String::new() };
            if is_tty {
                eprintln!("{:>12} {} ({} files, {}{})",
                    style("Partition").cyan().bold(),
                    partition_name,
                    format_count(stats.files),
                    format_bytes(stats.bytes),
                    prune_info
                );
            } else {
                eprintln!("Partition {} ({} files, {}{})", partition_name, format_count(stats.files), format_bytes(stats.bytes), prune_info);
            }
        }

        // Phase 2: If --fast, crawl remaining partitions
        if args.fast {
            let exclude: HashSet<String> = partition_names.iter().cloned().collect();
            let stats = crawl_full(
                &top_dir,
                &outdir,
                args.buffsize,
                size_mode,
                &schema,
                &exclude,
                is_tty,
            )?;
            total_files += stats.files;
            total_bytes += stats.bytes;
            total_pruned += stats.pruned;
        }
    } else {
        // Full crawl mode (no partitions specified)
        let stats = crawl_full(
            &top_dir,
            &outdir,
            args.buffsize,
            size_mode,
            &schema,
            &HashSet::new(),
            is_tty,
        )?;
        total_files = stats.files;
        total_bytes = stats.bytes;
        total_pruned = stats.pruned;
    }

    let elapsed = start_time.elapsed();
    let prune_info = if total_pruned > 0 { format!(", pruned {} stale", total_pruned) } else { String::new() };

    if is_tty {
        eprintln!("{:>12} {} files ({}) in {:.2}s{}",
            style("Completed").green().bold(),
            format_count(total_files),
            format_bytes(total_bytes),
            elapsed.as_secs_f64(),
            prune_info
        );
    } else {
        eprintln!("Completed {} files ({}) in {:.2}s{}", format_count(total_files), format_bytes(total_bytes), elapsed.as_secs_f64(), prune_info);
    }

    Ok(())
}
