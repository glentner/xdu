use std::fs::{self, File};
use std::io::{stderr, IsTerminal};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
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

use xdu::{format_bytes, format_count, get_schema, FileRecord};

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

    /// Index only a single partition (top-level subdirectory name).
    /// All threads will be used to crawl this partition in parallel.
    #[arg(short, long, value_name = "NAME")]
    partition: Option<String>,
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

/// Round size up to the nearest block boundary.
fn round_to_block(size: u64, block_size: u64) -> u64 {
    if block_size == 0 || size == 0 {
        return size;
    }
    ((size + block_size - 1) / block_size) * block_size
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

/// Determines how to calculate file size
#[derive(Clone, Copy)]
enum SizeMode {
    /// Use st_blocks * 512 (actual disk usage)
    DiskUsage,
    /// Use st_size (apparent/logical size)
    ApparentSize,
    /// Use st_size rounded up to block size
    BlockRounded(u64),
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

    // Collect all top-level subdirectories as partitions
    let mut partitions: Vec<PathBuf> = fs::read_dir(&top_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();

    partitions.sort();

    // Filter to single partition if --partition is specified
    if let Some(ref partition_name) = args.partition {
        let target = top_dir.join(partition_name);
        if !target.is_dir() {
            anyhow::bail!("Partition '{}' not found in {}", partition_name, top_dir.display());
        }
        partitions.retain(|p| p == &target);
    }

    if partitions.is_empty() {
        if is_tty {
            eprintln!("{} No subdirectories found in {}", style("warning:").yellow().bold(), top_dir.display());
        } else {
            eprintln!("warning: No subdirectories found in {}", top_dir.display());
        }
        return Ok(());
    }

    let total_partitions = partitions.len();
    let global_file_count = Arc::new(AtomicU64::new(0));
    let global_byte_count = Arc::new(AtomicU64::new(0));
    let completed_partitions = Arc::new(AtomicUsize::new(0));

    // Set up progress display
    let mp = MultiProgress::new();
    if !is_tty {
        mp.set_draw_target(ProgressDrawTarget::hidden());
    }

    // Global status bar at the bottom
    let global_style = ProgressStyle::default_bar()
        .template("{spinner:.green} {msg}")
        .unwrap();
    let global_bar = mp.add(ProgressBar::new_spinner());
    global_bar.set_style(global_style);
    global_bar.enable_steady_tick(std::time::Duration::from_millis(100));

    // Print initial indexing message
    if is_tty {
        eprintln!("{:>12} {} ({} partitions)",
            style("Indexing").green().bold(),
            top_dir.display(),
            total_partitions
        );
    } else {
        eprintln!("Indexing {} ({} partitions)", top_dir.display(), total_partitions);
    }

    // Process each partition in parallel
    let result = partitions.par_iter().try_for_each(|partition_path| -> Result<()> {
        let partition_name = partition_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();

        // Create a progress bar for this partition
        let partition_style = ProgressStyle::default_bar()
            .template("{spinner:.cyan} {prefix:.bold} {msg}")
            .unwrap();
        let partition_bar = mp.insert_before(&global_bar, ProgressBar::new_spinner());
        partition_bar.set_style(partition_style);
        partition_bar.set_prefix(format!("{:>12}", partition_name));
        partition_bar.enable_steady_tick(std::time::Duration::from_millis(100));

        let partition_file_count = AtomicU64::new(0);
        let partition_byte_count = AtomicU64::new(0);

        let buffer = Arc::new(Mutex::new(PartitionBuffer::new(
            partition_name.clone(),
            outdir.clone(),
            args.buffsize,
            schema.clone(),
        )));
        
        let size_mode = size_mode; // Copy for closure

        // Walk the partition directory
        for entry in WalkDir::new(partition_path)
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

            // Calculate size based on mode
            let file_size = match size_mode {
                SizeMode::DiskUsage => {
                    // st_blocks is in 512-byte units
                    metadata.blocks() * 512
                }
                SizeMode::ApparentSize => {
                    metadata.len()
                }
                SizeMode::BlockRounded(block_size) => {
                    round_to_block(metadata.len(), block_size)
                }
            };

            let record = FileRecord {
                path: path.to_string_lossy().to_string(),
                size: file_size as i64,
                atime: metadata.atime(),
            };
            buffer.lock().add(record)?;

            let count = partition_file_count.fetch_add(1, Ordering::Relaxed) + 1;
            partition_byte_count.fetch_add(file_size, Ordering::Relaxed);
            global_file_count.fetch_add(1, Ordering::Relaxed);
            global_byte_count.fetch_add(file_size, Ordering::Relaxed);

            // Update progress bars periodically
            if count % 1000 == 0 {
                let part_bytes = partition_byte_count.load(Ordering::Relaxed);
                partition_bar.set_message(format!("{} files, {}", format_count(count), format_bytes(part_bytes)));
                let total_files = global_file_count.load(Ordering::Relaxed);
                let total_bytes = global_byte_count.load(Ordering::Relaxed);
                let done = completed_partitions.load(Ordering::Relaxed);
                global_bar.set_message(format!(
                    "{}/{} partitions, {} files, {}",
                    done, total_partitions, format_count(total_files), format_bytes(total_bytes)
                ));
            }
        }

        // Flush any remaining records and finalize (atomic rename + prune)
        buffer.lock().flush()?;

        let buf = match Arc::try_unwrap(buffer) {
            Ok(mutex) => mutex.into_inner(),
            Err(_) => panic!("Buffer still has multiple references"),
        };
        let pruned = buf.finalize()?;

        let final_count = partition_file_count.load(Ordering::Relaxed);
        let final_bytes = partition_byte_count.load(Ordering::Relaxed);
        completed_partitions.fetch_add(1, Ordering::Relaxed);

        // Clear the spinner and print completion
        partition_bar.finish_and_clear();

        let prune_info = if pruned > 0 {
            format!(", pruned {} stale", pruned)
        } else {
            String::new()
        };

        if is_tty {
            mp.println(format!("{:>12} {} ({} files, {}{})",
                style("Finished").green().bold(),
                partition_name,
                format_count(final_count),
                format_bytes(final_bytes),
                prune_info
            ))?;
        } else {
            eprintln!("Finished {} ({} files, {}{})", partition_name, format_count(final_count), format_bytes(final_bytes), prune_info);
        }

        Ok(())
    });

    global_bar.finish_and_clear();

    result?;

    let elapsed = start_time.elapsed();
    let total_files = global_file_count.load(Ordering::Relaxed);
    let total_bytes = global_byte_count.load(Ordering::Relaxed);

    if is_tty {
        eprintln!("{:>12} {} files ({}) in {:.2}s",
            style("Completed").green().bold(),
            format_count(total_files),
            format_bytes(total_bytes),
            elapsed.as_secs_f64()
        );
    } else {
        eprintln!("Completed {} files ({}) in {:.2}s", format_count(total_files), format_bytes(total_bytes), elapsed.as_secs_f64());
    }

    Ok(())
}
