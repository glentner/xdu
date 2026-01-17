//! Integration tests for xdu crawl functionality.
//!
//! These tests create temporary directory structures and verify that
//! crawling produces correct file counts, sizes, and records.

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tempfile::TempDir;

use xdu::{CrawlUnit, FileRecord, SizeMode};

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

/// Helper to crawl a directory and collect records into a TestBuffer.
/// This simulates the crawl_directory function from xdu.rs.
fn crawl_directory_for_test(
    unit: &CrawlUnit,
    buffer: &Arc<Mutex<TestBuffer>>,
    size_mode: SizeMode,
    unit_file_count: &AtomicU64,
    unit_byte_count: &AtomicU64,
    global_file_count: &AtomicU64,
    global_byte_count: &AtomicU64,
) {
    for entry in walkdir::WalkDir::new(&unit.path)
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

        // Use apparent size for testing (st_blocks not reliable in tests)
        let file_size = size_mode.calculate(metadata.len(), metadata.len());
        let record = FileRecord {
            path: path.to_string_lossy().to_string(),
            size: file_size as i64,
            atime: 0, // Not testing atime
        };
        buffer.lock().add(record);

        unit_file_count.fetch_add(1, Ordering::Relaxed);
        unit_byte_count.fetch_add(file_size, Ordering::Relaxed);
        global_file_count.fetch_add(1, Ordering::Relaxed);
        global_byte_count.fetch_add(file_size, Ordering::Relaxed);
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

    let unit = CrawlUnit::new(base.to_path_buf(), "test".to_string());
    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let unit_file_count = AtomicU64::new(0);
    let unit_byte_count = AtomicU64::new(0);
    let global_file_count = AtomicU64::new(0);
    let global_byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        &unit,
        &buffer,
        SizeMode::ApparentSize,
        &unit_file_count,
        &unit_byte_count,
        &global_file_count,
        &global_byte_count,
    );

    // Verify counts
    assert_eq!(unit_file_count.load(Ordering::Relaxed), 3);
    assert_eq!(unit_byte_count.load(Ordering::Relaxed), 600);
    assert_eq!(global_file_count.load(Ordering::Relaxed), 3);
    assert_eq!(global_byte_count.load(Ordering::Relaxed), 600);

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

    let unit = CrawlUnit::new(base.to_path_buf(), "test".to_string());
    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let unit_file_count = AtomicU64::new(0);
    let unit_byte_count = AtomicU64::new(0);
    let global_file_count = AtomicU64::new(0);
    let global_byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        &unit,
        &buffer,
        SizeMode::ApparentSize,
        &unit_file_count,
        &unit_byte_count,
        &global_file_count,
        &global_byte_count,
    );

    assert_eq!(unit_file_count.load(Ordering::Relaxed), 4);
    assert_eq!(unit_byte_count.load(Ordering::Relaxed), 500);
}

#[test]
fn test_crawl_directory_adds_records_to_buffer() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    create_test_file(&base.join("test.txt"), 1024).unwrap();

    let unit = CrawlUnit::new(base.to_path_buf(), "partition".to_string());
    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let unit_file_count = AtomicU64::new(0);
    let unit_byte_count = AtomicU64::new(0);
    let global_file_count = AtomicU64::new(0);
    let global_byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        &unit,
        &buffer,
        SizeMode::ApparentSize,
        &unit_file_count,
        &unit_byte_count,
        &global_file_count,
        &global_byte_count,
    );

    let buf = buffer.lock();
    assert_eq!(buf.records().len(), 1);
    let record = &buf.records()[0];
    assert!(record.path.ends_with("test.txt"));
    assert_eq!(record.size, 1024);
}

// =============================================================================
// Test: Partial partition crawl handles subdirectories and loose files
// =============================================================================

#[test]
fn test_partial_partition_crawl_with_subdirectories() {
    let temp_dir = TempDir::new().unwrap();
    let partition_path = temp_dir.path();

    // Create subdirectories (these would be crawled as separate units)
    fs::create_dir_all(partition_path.join("subdir1")).unwrap();
    fs::create_dir_all(partition_path.join("subdir2")).unwrap();

    create_test_file(&partition_path.join("subdir1/file1.txt"), 100).unwrap();
    create_test_file(&partition_path.join("subdir1/file2.txt"), 200).unwrap();
    create_test_file(&partition_path.join("subdir2/file3.txt"), 300).unwrap();

    // Simulate crawling each subdirectory as a separate unit
    let global_file_count = Arc::new(AtomicU64::new(0));
    let global_byte_count = Arc::new(AtomicU64::new(0));
    let buffer = Arc::new(Mutex::new(TestBuffer::new()));

    // Crawl subdir1
    let unit1 = CrawlUnit::new(
        partition_path.join("subdir1"),
        "partition:subdir1".to_string(),
    );
    crawl_directory_for_test(
        &unit1,
        &buffer,
        SizeMode::ApparentSize,
        &AtomicU64::new(0),
        &AtomicU64::new(0),
        &global_file_count,
        &global_byte_count,
    );

    // Crawl subdir2
    let unit2 = CrawlUnit::new(
        partition_path.join("subdir2"),
        "partition:subdir2".to_string(),
    );
    crawl_directory_for_test(
        &unit2,
        &buffer,
        SizeMode::ApparentSize,
        &AtomicU64::new(0),
        &AtomicU64::new(0),
        &global_file_count,
        &global_byte_count,
    );

    // Verify global totals
    assert_eq!(global_file_count.load(Ordering::Relaxed), 3);
    assert_eq!(global_byte_count.load(Ordering::Relaxed), 600);

    let buf = buffer.lock();
    assert_eq!(buf.records().len(), 3);
}

#[test]
fn test_partial_partition_crawl_with_loose_files() {
    let temp_dir = TempDir::new().unwrap();
    let partition_path = temp_dir.path();

    // Create a subdirectory and loose files at partition root
    fs::create_dir_all(partition_path.join("subdir")).unwrap();
    create_test_file(&partition_path.join("loose1.txt"), 50).unwrap();
    create_test_file(&partition_path.join("loose2.txt"), 75).unwrap();
    create_test_file(&partition_path.join("subdir/nested.txt"), 100).unwrap();

    // Simulate the loose files handling (depth=1 only)
    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let global_file_count = AtomicU64::new(0);
    let global_byte_count = AtomicU64::new(0);

    // Process loose files at partition root (simulating the special case)
    for entry in fs::read_dir(partition_path).unwrap().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let metadata = fs::metadata(&path).unwrap();
        let file_size = metadata.len();
        let record = FileRecord {
            path: path.to_string_lossy().to_string(),
            size: file_size as i64,
            atime: 0,
        };
        buffer.lock().add(record);
        global_file_count.fetch_add(1, Ordering::Relaxed);
        global_byte_count.fetch_add(file_size, Ordering::Relaxed);
    }

    // Should only have the 2 loose files, not the nested one
    assert_eq!(global_file_count.load(Ordering::Relaxed), 2);
    assert_eq!(global_byte_count.load(Ordering::Relaxed), 125);

    // Now crawl the subdirectory
    let unit = CrawlUnit::new(
        partition_path.join("subdir"),
        "partition:subdir".to_string(),
    );
    crawl_directory_for_test(
        &unit,
        &buffer,
        SizeMode::ApparentSize,
        &AtomicU64::new(0),
        &AtomicU64::new(0),
        &global_file_count,
        &global_byte_count,
    );

    // Now we should have all 3 files
    assert_eq!(global_file_count.load(Ordering::Relaxed), 3);
    assert_eq!(global_byte_count.load(Ordering::Relaxed), 225);
}

// =============================================================================
// Test: Full crawl mode correctly processes partitions
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

    // Simulate full crawl with one buffer per partition
    let global_file_count = Arc::new(AtomicU64::new(0));
    let global_byte_count = Arc::new(AtomicU64::new(0));
    let completed_partitions = AtomicUsize::new(0);

    let mut partitions: Vec<PathBuf> = fs::read_dir(base)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    partitions.sort();

    let total_partitions = partitions.len();
    assert_eq!(total_partitions, 3);

    for partition_path in &partitions {
        let partition_name = partition_path.file_name().unwrap().to_string_lossy().to_string();
        let unit = CrawlUnit::new(partition_path.clone(), partition_name);

        let buffer = Arc::new(Mutex::new(TestBuffer::new()));
        let unit_file_count = AtomicU64::new(0);
        let unit_byte_count = AtomicU64::new(0);

        crawl_directory_for_test(
            &unit,
            &buffer,
            SizeMode::ApparentSize,
            &unit_file_count,
            &unit_byte_count,
            &global_file_count,
            &global_byte_count,
        );

        completed_partitions.fetch_add(1, Ordering::Relaxed);
    }

    // Verify totals
    assert_eq!(completed_partitions.load(Ordering::Relaxed), 3);
    assert_eq!(global_file_count.load(Ordering::Relaxed), 4);
    assert_eq!(global_byte_count.load(Ordering::Relaxed), 950);
}

#[test]
fn test_full_crawl_updates_global_counts_correctly() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    // Create two partitions
    fs::create_dir_all(base.join("part1")).unwrap();
    fs::create_dir_all(base.join("part2")).unwrap();

    create_test_file(&base.join("part1/a.txt"), 1000).unwrap();
    create_test_file(&base.join("part2/b.txt"), 2000).unwrap();
    create_test_file(&base.join("part2/c.txt"), 3000).unwrap();

    let global_file_count = Arc::new(AtomicU64::new(0));
    let global_byte_count = Arc::new(AtomicU64::new(0));

    // Process part1
    let buffer1 = Arc::new(Mutex::new(TestBuffer::new()));
    crawl_directory_for_test(
        &CrawlUnit::new(base.join("part1"), "part1".to_string()),
        &buffer1,
        SizeMode::ApparentSize,
        &AtomicU64::new(0),
        &AtomicU64::new(0),
        &global_file_count,
        &global_byte_count,
    );

    // After part1
    assert_eq!(global_file_count.load(Ordering::Relaxed), 1);
    assert_eq!(global_byte_count.load(Ordering::Relaxed), 1000);

    // Process part2
    let buffer2 = Arc::new(Mutex::new(TestBuffer::new()));
    crawl_directory_for_test(
        &CrawlUnit::new(base.join("part2"), "part2".to_string()),
        &buffer2,
        SizeMode::ApparentSize,
        &AtomicU64::new(0),
        &AtomicU64::new(0),
        &global_file_count,
        &global_byte_count,
    );

    // After both partitions
    assert_eq!(global_file_count.load(Ordering::Relaxed), 3);
    assert_eq!(global_byte_count.load(Ordering::Relaxed), 6000);
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

    let unit = CrawlUnit::new(base.to_path_buf(), "test".to_string());
    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let unit_byte_count = AtomicU64::new(0);
    let global_byte_count = AtomicU64::new(0);

    crawl_directory_for_test(
        &unit,
        &buffer,
        SizeMode::BlockRounded(4096),
        &AtomicU64::new(0),
        &unit_byte_count,
        &AtomicU64::new(0),
        &global_byte_count,
    );

    // 100 bytes should round up to 4096
    assert_eq!(unit_byte_count.load(Ordering::Relaxed), 4096);
    assert_eq!(global_byte_count.load(Ordering::Relaxed), 4096);

    let buf = buffer.lock();
    assert_eq!(buf.records()[0].size, 4096);
}

#[test]
fn test_crawl_empty_directory() {
    let temp_dir = TempDir::new().unwrap();
    let base = temp_dir.path();

    // Empty directory
    let unit = CrawlUnit::new(base.to_path_buf(), "empty".to_string());
    let buffer = Arc::new(Mutex::new(TestBuffer::new()));
    let unit_file_count = AtomicU64::new(0);
    let global_file_count = AtomicU64::new(0);

    crawl_directory_for_test(
        &unit,
        &buffer,
        SizeMode::ApparentSize,
        &unit_file_count,
        &AtomicU64::new(0),
        &global_file_count,
        &AtomicU64::new(0),
    );

    assert_eq!(unit_file_count.load(Ordering::Relaxed), 0);
    assert_eq!(global_file_count.load(Ordering::Relaxed), 0);
    assert_eq!(buffer.lock().records().len(), 0);
}
