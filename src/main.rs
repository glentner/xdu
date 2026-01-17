use std::fs::{self, File};
use std::io::{stderr, IsTerminal};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::{Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
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
    #[arg(short, long, default_value = "100000")]
    buffsize: usize,
}

/// A single file metadata record.
#[derive(Clone)]
struct FileRecord {
    path: String,
    size: i64,
    atime: i64,
}

/// Per-partition buffer that accumulates records and flushes to Parquet.
struct PartitionBuffer {
    partition: String,
    outdir: PathBuf,
    records: Vec<FileRecord>,
    buffsize: usize,
    chunk_counter: AtomicUsize,
    schema: Arc<Schema>,
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

        let output_path = partition_dir.join(format!("{:06}.parquet", chunk_id));

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

        let file = File::create(&output_path)
            .with_context(|| format!("Failed to create file: {}", output_path.display()))?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        self.records.clear();
        Ok(())
    }
}

fn get_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("atime", DataType::Int64, false),
    ]))
}

fn format_count(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes >= TIB {
        format!("{:.2} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let start_time = Instant::now();
    let is_tty = stderr().is_terminal();

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

            let record = FileRecord {
                path: path.to_string_lossy().to_string(),
                size: metadata.len() as i64,
                atime: metadata.atime(),
            };

            let file_size = record.size as u64;
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

        // Flush any remaining records
        buffer.lock().flush()?;

        let final_count = partition_file_count.load(Ordering::Relaxed);
        let final_bytes = partition_byte_count.load(Ordering::Relaxed);
        completed_partitions.fetch_add(1, Ordering::Relaxed);

        // Clear the spinner and print completion
        partition_bar.finish_and_clear();

        if is_tty {
            mp.println(format!("{:>12} {} ({} files, {})",
                style("Finished").green().bold(),
                partition_name,
                format_count(final_count),
                format_bytes(final_bytes)
            ))?;
        } else {
            eprintln!("Finished {} ({} files, {})", partition_name, format_count(final_count), format_bytes(final_bytes));
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
