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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use tempfile::TempDir;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn create_test_schema() -> Arc<Schema> {
        get_schema()
    }

    // Tests for format_count()
    #[test]
    fn test_format_count_units() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(1), "1");
        assert_eq!(format_count(999), "999");
    }

    #[test]
    fn test_format_count_thousands() {
        assert_eq!(format_count(1_000), "1.0K");
        assert_eq!(format_count(1_500), "1.5K");
        assert_eq!(format_count(999_999), "1000.0K");
    }

    #[test]
    fn test_format_count_millions() {
        assert_eq!(format_count(1_000_000), "1.0M");
        assert_eq!(format_count(2_500_000), "2.5M");
        assert_eq!(format_count(999_999_999), "1000.0M");
    }

    #[test]
    fn test_format_count_billions() {
        assert_eq!(format_count(1_000_000_000), "1.0B");
        assert_eq!(format_count(5_500_000_000), "5.5B");
    }

    // Tests for format_bytes()
    #[test]
    fn test_format_bytes_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(1023), "1023 B");
    }

    #[test]
    fn test_format_bytes_kib() {
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1536), "1.50 KiB");
        assert_eq!(format_bytes(1024 * 1023), "1023.00 KiB");
    }

    #[test]
    fn test_format_bytes_mib() {
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 2 + 1024 * 512), "2.50 MiB");
    }

    #[test]
    fn test_format_bytes_gib() {
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 3 + 1024 * 1024 * 512), "3.50 GiB");
    }

    #[test]
    fn test_format_bytes_tib() {
        assert_eq!(format_bytes(1024_u64 * 1024 * 1024 * 1024), "1.00 TiB");
        assert_eq!(format_bytes(1024_u64 * 1024 * 1024 * 1024 * 2 + 1024_u64 * 1024 * 1024 * 512), "2.50 TiB");
    }

    // Tests for PartitionBuffer::add()
    #[test]
    fn test_partition_buffer_add_accumulates_records() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(
            "test_partition".to_string(),
            temp_dir.path().to_path_buf(),
            100, // large buffer so no flush
            schema,
        );

        // Add records below buffer size
        for i in 0..50 {
            buffer.add(FileRecord {
                path: format!("/path/to/file{}", i),
                size: 1024 * i,
                atime: 1700000000 + i,
            })?;
        }

        // Records should be accumulated, not flushed
        assert_eq!(buffer.records.len(), 50);
        assert_eq!(buffer.chunk_counter.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[test]
    fn test_partition_buffer_add_flushes_at_buffer_size() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(
            "test_partition".to_string(),
            temp_dir.path().to_path_buf(),
            10, // small buffer to trigger flush
            schema,
        );

        // Add exactly buffsize records to trigger flush
        for i in 0..10 {
            buffer.add(FileRecord {
                path: format!("/path/to/file{}", i),
                size: 1024,
                atime: 1700000000,
            })?;
        }

        // Buffer should have flushed and be empty
        assert_eq!(buffer.records.len(), 0);
        assert_eq!(buffer.chunk_counter.load(Ordering::SeqCst), 1);

        // Verify parquet file was created
        let parquet_path = temp_dir.path().join("test_partition").join("000000.parquet");
        assert!(parquet_path.exists());

        Ok(())
    }

    #[test]
    fn test_partition_buffer_add_multiple_flushes() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(
            "test_partition".to_string(),
            temp_dir.path().to_path_buf(),
            5,
            schema,
        );

        // Add 12 records: should trigger 2 flushes with 2 remaining
        for i in 0..12 {
            buffer.add(FileRecord {
                path: format!("/path/to/file{}", i),
                size: 100,
                atime: 1700000000,
            })?;
        }

        assert_eq!(buffer.records.len(), 2);
        assert_eq!(buffer.chunk_counter.load(Ordering::SeqCst), 2);

        // Verify both parquet files exist
        assert!(temp_dir.path().join("test_partition").join("000000.parquet").exists());
        assert!(temp_dir.path().join("test_partition").join("000001.parquet").exists());

        Ok(())
    }

    // Tests for PartitionBuffer::flush()
    #[test]
    fn test_partition_buffer_flush_empty_is_noop() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(
            "test_partition".to_string(),
            temp_dir.path().to_path_buf(),
            100,
            schema,
        );

        // Flush empty buffer
        buffer.flush()?;

        // No files should be created
        assert!(!temp_dir.path().join("test_partition").exists());
        assert_eq!(buffer.chunk_counter.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[test]
    fn test_partition_buffer_flush_writes_correct_parquet() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(
            "test_partition".to_string(),
            temp_dir.path().to_path_buf(),
            100,
            schema,
        );

        // Add specific records
        buffer.add(FileRecord {
            path: "/path/to/file1.txt".to_string(),
            size: 1024,
            atime: 1700000001,
        })?;
        buffer.add(FileRecord {
            path: "/path/to/file2.txt".to_string(),
            size: 2048,
            atime: 1700000002,
        })?;
        buffer.add(FileRecord {
            path: "/path/to/file3.txt".to_string(),
            size: 4096,
            atime: 1700000003,
        })?;

        buffer.flush()?;

        // Buffer should be cleared
        assert_eq!(buffer.records.len(), 0);

        // Read back the parquet file and verify contents
        let parquet_path = temp_dir.path().join("test_partition").join("000000.parquet");
        let file = File::open(&parquet_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        // Verify paths
        let paths = batch.column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(paths.value(0), "/path/to/file1.txt");
        assert_eq!(paths.value(1), "/path/to/file2.txt");
        assert_eq!(paths.value(2), "/path/to/file3.txt");

        // Verify sizes
        let sizes = batch.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sizes.value(0), 1024);
        assert_eq!(sizes.value(1), 2048);
        assert_eq!(sizes.value(2), 4096);

        // Verify atimes
        let atimes = batch.column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(atimes.value(0), 1700000001);
        assert_eq!(atimes.value(1), 1700000002);
        assert_eq!(atimes.value(2), 1700000003);

        Ok(())
    }

    #[test]
    fn test_partition_buffer_flush_creates_partition_dir() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(
            "nested/partition/name".to_string(),
            temp_dir.path().to_path_buf(),
            100,
            schema,
        );

        buffer.add(FileRecord {
            path: "/test".to_string(),
            size: 100,
            atime: 1700000000,
        })?;
        buffer.flush()?;

        // Verify nested directory was created
        assert!(temp_dir.path().join("nested/partition/name").join("000000.parquet").exists());

        Ok(())
    }

    // Integration test for indexing directories
    #[test]
    fn test_index_directories_creates_partitions() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_dir = temp_dir.path().join("source");
        let output_dir = temp_dir.path().join("output");

        // Create source directory structure with multiple partitions
        let partition_a = source_dir.join("partition_a");
        let partition_b = source_dir.join("partition_b");
        fs::create_dir_all(&partition_a)?;
        fs::create_dir_all(&partition_b)?;

        // Create test files in partition_a
        File::create(partition_a.join("file1.txt"))?.write_all(b"content1")?;
        File::create(partition_a.join("file2.txt"))?.write_all(b"content22")?;

        // Create test files in partition_b
        let subdir = partition_b.join("subdir");
        fs::create_dir_all(&subdir)?;
        File::create(partition_b.join("file3.txt"))?.write_all(b"content333")?;
        File::create(subdir.join("file4.txt"))?.write_all(b"content4444")?;

        // Run indexing
        fs::create_dir_all(&output_dir)?;
        let schema = get_schema();

        let partitions: Vec<PathBuf> = fs::read_dir(&source_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();

        for partition_path in &partitions {
            let partition_name = partition_path.file_name().unwrap().to_string_lossy().to_string();
            let mut buffer = PartitionBuffer::new(
                partition_name,
                output_dir.clone(),
                100,
                schema.clone(),
            );

            for entry in WalkDir::new(partition_path)
                .follow_links(false)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }

                let metadata = fs::metadata(path)?;
                buffer.add(FileRecord {
                    path: path.to_string_lossy().to_string(),
                    size: metadata.len() as i64,
                    atime: metadata.atime(),
                })?;
            }

            buffer.flush()?;
        }

        // Verify output structure
        assert!(output_dir.join("partition_a").join("000000.parquet").exists());
        assert!(output_dir.join("partition_b").join("000000.parquet").exists());

        // Verify partition_a contents
        let file_a = File::open(output_dir.join("partition_a").join("000000.parquet"))?;
        let reader_a = ParquetRecordBatchReaderBuilder::try_new(file_a)?.build()?;
        let batches_a: Vec<_> = reader_a.collect::<std::result::Result<Vec<_>, _>>()?;
        let total_rows_a: usize = batches_a.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows_a, 2); // 2 files in partition_a

        // Verify partition_b contents
        let file_b = File::open(output_dir.join("partition_b").join("000000.parquet"))?;
        let reader_b = ParquetRecordBatchReaderBuilder::try_new(file_b)?.build()?;
        let batches_b: Vec<_> = reader_b.collect::<std::result::Result<Vec<_>, _>>()?;
        let total_rows_b: usize = batches_b.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows_b, 2); // 2 files in partition_b (including subdir)

        // Verify file sizes are recorded correctly
        let batch_a = &batches_a[0];
        let sizes_a = batch_a.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let total_size_a: i64 = (0..sizes_a.len()).map(|i| sizes_a.value(i)).sum();
        assert_eq!(total_size_a, 8 + 9); // "content1" + "content22"

        Ok(())
    }

    #[test]
    fn test_index_with_progress_tracking() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_dir = temp_dir.path().join("source");
        let output_dir = temp_dir.path().join("output");

        // Create partitions with multiple files to test progress tracking
        let partition = source_dir.join("users");
        fs::create_dir_all(&partition)?;

        // Create enough files to trigger progress updates
        for i in 0..50 {
            let content = format!("file content {}", i);
            File::create(partition.join(format!("file{}.txt", i)))?.write_all(content.as_bytes())?;
        }

        fs::create_dir_all(&output_dir)?;
        let schema = get_schema();

        let global_file_count = Arc::new(AtomicU64::new(0));
        let global_byte_count = Arc::new(AtomicU64::new(0));

        let mut buffer = PartitionBuffer::new(
            "users".to_string(),
            output_dir.clone(),
            10, // Small buffer to force multiple flushes
            schema.clone(),
        );

        for entry in WalkDir::new(&partition)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let metadata = fs::metadata(path)?;
            let size = metadata.len() as i64;

            buffer.add(FileRecord {
                path: path.to_string_lossy().to_string(),
                size,
                atime: metadata.atime(),
            })?;

            global_file_count.fetch_add(1, Ordering::Relaxed);
            global_byte_count.fetch_add(size as u64, Ordering::Relaxed);
        }

        buffer.flush()?;

        // Verify progress counters
        assert_eq!(global_file_count.load(Ordering::Relaxed), 50);
        assert!(global_byte_count.load(Ordering::Relaxed) > 0);

        // Verify multiple parquet files were created due to small buffer
        let parquet_files: Vec<_> = fs::read_dir(output_dir.join("users"))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
            .collect();
        assert!(parquet_files.len() >= 5); // 50 files / 10 buffer size = 5 files

        Ok(())
    }
}
