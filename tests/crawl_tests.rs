//! Integration tests for xdu crawl functionality.
//!
//! These tests create temporary directory structures and verify that
//! crawling produces correct file counts, sizes, and records.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use jwalk::{Parallelism, WalkDir};
use parking_lot::Mutex;
use tempfile::TempDir;

use xdu::{FileRecord, SizeMode};

/// Special partition name for files directly in the top-level directory.
const ROOT_PARTITION: &str = "__root__";

/// Simple buffer that collects FileRecords for testing.
struct TestBuffer {
    records: Vec<FileRecord>,
}

impl TestBuffer {
    fn new() -> Self {
        Self { records: Vec::new() }
    }

    fn add(&mut self, record: FileRecord) {
        self.records.push(record);
    }

    fn records(&self) -> &[FileRecord] {
        &self.records
    }

    fn total_size(&self) -> i64 {
        self.records.iter().map(|r| r.size).sum()
    }
}

/// Create a test file with specific content size.
fn create_test_file(path: &PathBuf, size: usize) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(&vec![b'x'; size])?;
    Ok(())
}

/// Extract partition name from path, mirroring the logic in xdu.rs
fn extract_partition(path: &Path, top_dir: &Path) -> Option<String> {
    let relative = path.strip_prefix(top_dir).ok()?;
    let mut components = relative.components();
    let first = components.next()?;
    if components.next().is_some() {
        Some(first.as_os_str().to_string_lossy().to_string())
    } else {
        Some(ROOT_PARTITION.to_string())
    }
}

/// Helper to crawl a directory using jwalk and collect records into a TestBuffer.
fn crawl_directory_for_test(
    top_dir: &Path,
    buffer: &Arc<Mutex<TestBuffer>>,
    size_mode: SizeMode,
    file_count: &AtomicU64,
    byte_count: &AtomicU64,
) {
    let walker = WalkDir::new(top_dir)
        .parallelism(Parallelism::Serial)
        .skip_hidden(false)
        .follow_links(false);

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        let metadata = match fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        // Use apparent size for testing (st_blocks not reliable in tests)
        let file_size = size_mode.calculate(metadata.len(), metadata.len());
        let record = FileRecord {
            path: path.to_string_lossy().to_string(),
            size: file_size as i64,
            atime: 0, // Not testing atime
        };
        buffer.lock().add(record);

        file_count.fetch_add(1, Ordering::Relaxed);
        byte_count.fetch_add(file_size, Ordering::Relaxed);
    }
}

// =============================================================================
// Test: crawl_directory correctly accumulates file counts and sizes
// =============================================================================

#[test]
fn test_crawl_directory_accumulates_counts_and_sizes() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    // Create test files
    create_test_file(&base.join("file1.txt"), 100).unwrap();
    create_test_file(&base.join("file2.txt"), 200).unwrap();
    create_test_file(&base.join("file3.txt"), 300).unwrap();

    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let file_count = AtomicU64::new(0);
    let byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        base,
        &buffer,
        SizeMode::ApparentSize,
        &file_count,
        &byte_count,
    );

    // Verify counts
    assert_eq!(file_count.load(Ordering::Relaxed), 3);
    assert_eq!(byte_count.load(Ordering::Relaxed), 600);

    // Verify buffer has all records
    let buf = buffer.lock();
    assert_eq!(buf.records().len(), 3);
    assert_eq!(buf.total_size(), 600);
}

#[test]
fn test_crawl_directory_with_nested_subdirectories() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    // Create nested structure
    fs::create_dir_all(base.join("subdir1/nested")).unwrap();
    fs::create_dir_all(base.join("subdir2")).unwrap();

    create_test_file(&base.join("root.txt"), 50).unwrap();
    create_test_file(&base.join("subdir1/file1.txt"), 100).unwrap();
    create_test_file(&base.join("subdir1/nested/deep.txt"), 150).unwrap();
    create_test_file(&base.join("subdir2/file2.txt"), 200).unwrap();

    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let file_count = AtomicU64::new(0);
    let byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        base,
        &buffer,
        SizeMode::ApparentSize,
        &file_count,
        &byte_count,
    );

    assert_eq!(file_count.load(Ordering::Relaxed), 4);
    assert_eq!(byte_count.load(Ordering::Relaxed), 500);
}

#[test]
fn test_crawl_directory_adds_records_to_buffer() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    create_test_file(&base.join("test.txt"), 1024).unwrap();

    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let file_count = AtomicU64::new(0);
    let byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        base,
        &buffer,
        SizeMode::ApparentSize,
        &file_count,
        &byte_count,
    );

    let buf = buffer.lock();
    assert_eq!(buf.records().len(), 1);
    let record = &buf.records()[0];
    assert!(record.path.ends_with("test.txt"));
    assert_eq!(record.size, 1024);
}

// =============================================================================
// Test: Partition extraction logic
// =============================================================================

#[test]
fn test_extract_partition_with_subdirectory() {
    let top_dir = Path::new("/data/scratch");
    let file_path = Path::new("/data/scratch/alice/projects/file.txt");
    
    let partition = extract_partition(file_path, top_dir);
    assert_eq!(partition, Some("alice".to_string()));
}

#[test]
fn test_extract_partition_root_level_file() {
    let top_dir = Path::new("/data/scratch");
    let file_path = Path::new("/data/scratch/readme.txt");
    
    let partition = extract_partition(file_path, top_dir);
    assert_eq!(partition, Some(ROOT_PARTITION.to_string()));
}

#[test]
fn test_extract_partition_deeply_nested() {
    let top_dir = Path::new("/data/scratch");
    let file_path = Path::new("/data/scratch/bob/projects/2024/january/report.pdf");
    
    let partition = extract_partition(file_path, top_dir);
    assert_eq!(partition, Some("bob".to_string()));
}

#[test]
fn test_crawl_with_partition_structure() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    // Create partition-like structure
    fs::create_dir_all(base.join("alice")).unwrap();
    fs::create_dir_all(base.join("bob/projects")).unwrap();

    create_test_file(&base.join("alice/file1.txt"), 100).unwrap();
    create_test_file(&base.join("bob/file2.txt"), 200).unwrap();
    create_test_file(&base.join("bob/projects/file3.txt"), 300).unwrap();

    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let file_count = AtomicU64::new(0);
    let byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        base,
        &buffer,
        SizeMode::ApparentSize,
        &file_count,
        &byte_count,
    );

    // All files should be crawled
    assert_eq!(file_count.load(Ordering::Relaxed), 3);
    assert_eq!(byte_count.load(Ordering::Relaxed), 600);

    // Verify partition extraction works for collected records
    let buf = buffer.lock();
    for record in buf.records() {
        let path = Path::new(&record.path);
        let partition = extract_partition(path, base);
        assert!(partition.is_some());
        let p = partition.unwrap();
        assert!(p == "alice" || p == "bob");
    }
}

// =============================================================================
// Test: Full crawl mode correctly processes multiple partitions
// =============================================================================

#[test]
fn test_full_crawl_processes_multiple_partitions() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    // Create partition directories
    fs::create_dir_all(base.join("alice")).unwrap();
    fs::create_dir_all(base.join("bob")).unwrap();
    fs::create_dir_all(base.join("charlie")).unwrap();

    create_test_file(&base.join("alice/file1.txt"), 100).unwrap();
    create_test_file(&base.join("alice/file2.txt"), 200).unwrap();
    create_test_file(&base.join("bob/data.bin"), 500).unwrap();
    create_test_file(&base.join("charlie/notes.md"), 150).unwrap();

    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let file_count = AtomicU64::new(0);
    let byte_count = AtomicU64::new(0);

    // Single crawl over entire directory tree
    crawl_directory_for_test(
        base,
        &buffer,
        SizeMode::ApparentSize,
        &file_count,
        &byte_count,
    );

    // Verify totals
    assert_eq!(file_count.load(Ordering::Relaxed), 4);
    assert_eq!(byte_count.load(Ordering::Relaxed), 950);

    // Verify partition detection works
    let buf = buffer.lock();
    let mut partitions: Vec<String> = buf.records().iter()
        .filter_map(|r| extract_partition(Path::new(&r.path), base))
        .collect();
    partitions.sort();
    partitions.dedup();
    assert_eq!(partitions, vec!["alice", "bob", "charlie"]);
}

#[test]
fn test_crawl_accumulates_across_partitions() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    // Create two partitions
    fs::create_dir_all(base.join("part1")).unwrap();
    fs::create_dir_all(base.join("part2")).unwrap();

    create_test_file(&base.join("part1/a.txt"), 1000).unwrap();
    create_test_file(&base.join("part2/b.txt"), 2000).unwrap();
    create_test_file(&base.join("part2/c.txt"), 3000).unwrap();

    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let file_count = AtomicU64::new(0);
    let byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        base,
        &buffer,
        SizeMode::ApparentSize,
        &file_count,
        &byte_count,
    );

    // Verify totals from both partitions
    assert_eq!(file_count.load(Ordering::Relaxed), 3);
    assert_eq!(byte_count.load(Ordering::Relaxed), 6000);
}

// =============================================================================
// Test: SizeMode with crawl
// =============================================================================

#[test]
fn test_crawl_with_block_rounded_size_mode() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    // Create a small file that should round up to 4K
    create_test_file(&base.join("small.txt"), 100).unwrap();

    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let file_count = AtomicU64::new(0);
    let byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        base,
        &buffer,
        SizeMode::BlockRounded(4096),
        &file_count,
        &byte_count,
    );

    // 100 bytes should round up to 4096
    assert_eq!(byte_count.load(Ordering::Relaxed), 4096);

    let buf = buffer.lock();
    assert_eq!(buf.records()[0].size, 4096);
}

#[test]
fn test_crawl_empty_directory() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    // Empty directory
    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let file_count = AtomicU64::new(0);
    let byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        base,
        &buffer,
        SizeMode::ApparentSize,
        &file_count,
        &byte_count,
    );

    assert_eq!(file_count.load(Ordering::Relaxed), 0);
    assert_eq!(buffer.lock().records().len(), 0);
}
