//! Integration tests for xdu-rm functionality.
//!
//! These tests create temporary directory structures, build indexes using xdu,
//! and verify that xdu-rm correctly deletes files based on query criteria.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use tempfile::TempDir;

/// Get the path to a built binary.
fn binary_path(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    // Try release first, then debug
    let release_path = path.join("release").join(name);
    if release_path.exists() {
        return release_path;
    }
    path.join("debug").join(name)
}

/// Create a test file with specific content size.
fn create_test_file(path: &Path, size: usize) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path)?;
    file.write_all(&vec![b'x'; size])?;
    Ok(())
}

/// Set the access time of a file to a specific number of days ago.
fn set_atime_days_ago(path: &Path, days: u64) -> std::io::Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let old_time = now - (days * 86400);

    // Get current mtime to preserve it
    let metadata = fs::metadata(path)?;
    let mtime = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Use libc to set atime while preserving mtime
    let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    let times = [
        libc::timespec {
            tv_sec: old_time as i64,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: mtime as i64,
            tv_nsec: 0,
        },
    ];
    unsafe {
        if libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Build an index using xdu.
/// Uses --apparent-size for consistent test results (disk usage varies by filesystem).
fn build_index(source: &Path, index: &Path) -> std::io::Result<()> {
    let output = Command::new(binary_path("xdu"))
        .arg("--apparent-size")
        .arg("-o")
        .arg(index)
        .arg(source)
        .output()?;

    if !output.status.success() {
        eprintln!("xdu stderr: {}", String::from_utf8_lossy(&output.stderr));
        return Err(std::io::Error::other(
            format!("xdu failed: {}", String::from_utf8_lossy(&output.stderr)),
        ));
    }
    Ok(())
}

/// Run xdu-rm and return (stdout, stderr, success).
fn run_xdu_rm(args: &[&str]) -> std::io::Result<(String, String, bool)> {
    let output = Command::new(binary_path("xdu-rm"))
        .args(args)
        .output()?;

    Ok((
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.success(),
    ))
}

// =============================================================================
// Test: Basic dry-run shows files without deleting
// =============================================================================

#[test]
fn test_dry_run_lists_files_without_deleting() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with files
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/file1.txt"), 100).unwrap();
    create_test_file(&source.join("user1/file2.txt"), 200).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Run dry-run
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--dry-run",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("file1.txt"));
    assert!(stdout.contains("file2.txt"));
    assert!(stdout.contains("2 file(s) would be deleted"));

    // Verify files still exist
    assert!(source.join("user1/file1.txt").exists());
    assert!(source.join("user1/file2.txt").exists());
}

// =============================================================================
// Test: Force delete actually removes files
// =============================================================================

#[test]
fn test_force_delete_removes_files() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with files
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/delete_me.txt"), 100).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Verify file exists before deletion
    assert!(source.join("user1/delete_me.txt").exists());

    // Run with --force to skip confirmation
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Deleted: 1"));

    // Verify file is deleted
    assert!(!source.join("user1/delete_me.txt").exists());
}

// =============================================================================
// Test: Pattern filter works correctly
// =============================================================================

#[test]
fn test_pattern_filter_deletes_matching_files() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with mixed files
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/keep.txt"), 100).unwrap();
    create_test_file(&source.join("user1/delete.log"), 100).unwrap();
    create_test_file(&source.join("user1/also_delete.log"), 100).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Delete only .log files
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--pattern", "\\.log$",
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Deleted: 2"));

    // Verify correct files remain
    assert!(source.join("user1/keep.txt").exists());
    assert!(!source.join("user1/delete.log").exists());
    assert!(!source.join("user1/also_delete.log").exists());
}

// =============================================================================
// Test: Size filters work correctly
// =============================================================================

#[test]
fn test_min_size_filter() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with files of different sizes
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/small.txt"), 100).unwrap();
    create_test_file(&source.join("user1/large.txt"), 10000).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Delete only files >= 1K
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--min-size", "1K",
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Deleted: 1"));

    // Verify small file remains, large file deleted
    assert!(source.join("user1/small.txt").exists());
    assert!(!source.join("user1/large.txt").exists());
}

#[test]
fn test_max_size_filter() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with files of different sizes
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/small.txt"), 100).unwrap();
    create_test_file(&source.join("user1/large.txt"), 10000).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Delete only files <= 1K
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--max-size", "1K",
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Deleted: 1"));

    // Verify large file remains, small file deleted
    assert!(!source.join("user1/small.txt").exists());
    assert!(source.join("user1/large.txt").exists());
}

// =============================================================================
// Test: Older-than filter works correctly
// =============================================================================

#[test]
fn test_older_than_filter() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with files
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/recent.txt"), 100).unwrap();
    create_test_file(&source.join("user1/old.txt"), 100).unwrap();

    // Set old.txt to have been accessed 90 days ago
    set_atime_days_ago(&source.join("user1/old.txt"), 90).unwrap();
    // recent.txt keeps current atime

    // Build index
    build_index(&source, &index).unwrap();

    // Delete only files older than 30 days
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--older-than", "30",
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Deleted: 1"));

    // Verify recent file remains, old file deleted
    assert!(source.join("user1/recent.txt").exists());
    assert!(!source.join("user1/old.txt").exists());
}

// =============================================================================
// Test: Partition filter works correctly
// =============================================================================

#[test]
fn test_partition_filter() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create multiple partitions
    fs::create_dir_all(source.join("alice")).unwrap();
    fs::create_dir_all(source.join("bob")).unwrap();
    create_test_file(&source.join("alice/file.txt"), 100).unwrap();
    create_test_file(&source.join("bob/file.txt"), 100).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Delete only alice's files
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--partition", "alice",
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Deleted: 1"));

    // Verify bob's file remains, alice's file deleted
    assert!(!source.join("alice/file.txt").exists());
    assert!(source.join("bob/file.txt").exists());
}

// =============================================================================
// Test: Limit option works correctly
// =============================================================================

#[test]
fn test_limit_option() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with multiple files
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/file1.txt"), 100).unwrap();
    create_test_file(&source.join("user1/file2.txt"), 100).unwrap();
    create_test_file(&source.join("user1/file3.txt"), 100).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Delete only 1 file
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--limit", "1",
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Deleted: 1"));

    // Count remaining files (should be 2)
    let remaining: Vec<_> = fs::read_dir(source.join("user1"))
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(remaining.len(), 2);
}

// =============================================================================
// Test: Safe mode protects recently accessed files (CRITICAL TEST)
// =============================================================================

#[test]
fn test_safe_mode_protects_recently_accessed_files() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with an old file
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/important.txt"), 100).unwrap();

    // Set the file to be accessed 90 days ago
    set_atime_days_ago(&source.join("user1/important.txt"), 90).unwrap();

    // Build the index (captures the old atime)
    build_index(&source, &index).unwrap();

    // Now simulate the user accessing the file AFTER the index was built
    // This updates the atime to now
    let _content = fs::read(source.join("user1/important.txt")).unwrap();

    // Attempt to delete with --safe mode
    // Without --safe, the file would be deleted because the INDEX shows it as old
    // With --safe, the file should be PROTECTED because its CURRENT atime is recent
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--older-than", "30",
        "--safe",
        "--force",
        "--verbose",
    ]).unwrap();

    assert!(success);

    // The file should be skipped due to safe mode
    assert!(stdout.contains("SKIP (accessed since index)"));
    assert!(stdout.contains("Skipped (safe mode): 1"));
    assert!(stdout.contains("Deleted: 0"));

    // CRITICAL: The file must still exist
    assert!(source.join("user1/important.txt").exists());
}

// =============================================================================
// Test: Safe mode with max-size protects files that grew
// =============================================================================

#[test]
fn test_safe_mode_protects_files_that_grew() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with a small file
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/growing.txt"), 100).unwrap();

    // Build the index (captures the small size)
    build_index(&source, &index).unwrap();

    // Now the file grows (user added data)
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(source.join("user1/growing.txt"))
            .unwrap();
        file.write_all(&vec![b'y'; 2000]).unwrap();
    }

    // Attempt to delete files <= 1K with --safe mode
    // Without --safe, it would try to delete (index shows 100 bytes)
    // With --safe, it should skip because current size is > 1K
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--max-size", "1K",
        "--safe",
        "--force",
        "--verbose",
    ]).unwrap();

    assert!(success);

    // The file should be skipped due to safe mode
    assert!(stdout.contains("SKIP (size changed)"));
    assert!(stdout.contains("Skipped (safe mode): 1"));
    assert!(stdout.contains("Deleted: 0"));

    // CRITICAL: The file must still exist
    assert!(source.join("user1/growing.txt").exists());
}

// =============================================================================
// Test: Missing files are handled gracefully
// =============================================================================

#[test]
fn test_handles_missing_files_gracefully() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with files
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/exists.txt"), 100).unwrap();
    create_test_file(&source.join("user1/will_be_gone.txt"), 100).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Delete one file manually (simulating external deletion)
    fs::remove_file(source.join("user1/will_be_gone.txt")).unwrap();

    // Run xdu-rm
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--force",
        "--verbose",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Missing: 1"));
    assert!(stdout.contains("Deleted: 1"));
}

// =============================================================================
// Test: Verbose output shows file-by-file status
// =============================================================================

#[test]
fn test_verbose_output() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with files
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/file1.txt"), 100).unwrap();
    create_test_file(&source.join("user1/file2.txt"), 100).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Run with verbose
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--force",
        "--verbose",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("DELETE:"));
    assert!(stdout.contains("file1.txt"));
    assert!(stdout.contains("file2.txt"));
}

// =============================================================================
// Test: No matching files produces appropriate message
// =============================================================================

#[test]
fn test_no_matching_files() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with files
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/file.txt"), 100).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Search for non-existent pattern
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--pattern", "\\.nonexistent$",
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("No matching files found"));
}

// =============================================================================
// Test: Combined filters work together
// =============================================================================

#[test]
fn test_combined_filters() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with various files
    fs::create_dir_all(source.join("user1")).unwrap();

    // File that matches all criteria (old, .log, large enough)
    create_test_file(&source.join("user1/old_large.log"), 2000).unwrap();
    set_atime_days_ago(&source.join("user1/old_large.log"), 90).unwrap();

    // File that's old but wrong extension
    create_test_file(&source.join("user1/old.txt"), 2000).unwrap();
    set_atime_days_ago(&source.join("user1/old.txt"), 90).unwrap();

    // File that's .log but recent
    create_test_file(&source.join("user1/recent.log"), 2000).unwrap();

    // File that's old and .log but too small
    create_test_file(&source.join("user1/old_small.log"), 100).unwrap();
    set_atime_days_ago(&source.join("user1/old_small.log"), 90).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Delete only files that are: .log AND older than 30 days AND >= 1K
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--pattern", "\\.log$",
        "--older-than", "30",
        "--min-size", "1K",
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Deleted: 1"));

    // Only old_large.log should be deleted
    assert!(!source.join("user1/old_large.log").exists());
    assert!(source.join("user1/old.txt").exists());
    assert!(source.join("user1/recent.log").exists());
    assert!(source.join("user1/old_small.log").exists());
}

// =============================================================================
// Test: Safe mode without relevant filter has no effect
// =============================================================================

#[test]
fn test_safe_mode_without_relevant_filter_still_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");

    // Create partition with a file
    fs::create_dir_all(source.join("user1")).unwrap();
    create_test_file(&source.join("user1/file.txt"), 100).unwrap();

    // Build index
    build_index(&source, &index).unwrap();

    // Run with --safe but no --older-than or --max-size
    // Safe mode has nothing to check, so deletion proceeds
    let (stdout, _stderr, success) = run_xdu_rm(&[
        "-i", index.to_str().unwrap(),
        "--safe",
        "--force",
    ]).unwrap();

    assert!(success);
    assert!(stdout.contains("Deleted: 1"));
    assert!(!source.join("user1/file.txt").exists());
}
