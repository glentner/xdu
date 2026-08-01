//! Integration tests for the `xdu` index-build crawl.
//!
//! These drive the **real** `xdu` binary against throwaway `tempfile` trees and assert
//! concrete post-conditions via `xdu-find` (row counts, per-partition counts, paths
//! present-or-absent, exit status) — never by reimplementing the crawler. Sizes use
//! `--apparent-size` so they are exact and filesystem-independent.

mod common;

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};

use tempfile::TempDir;

use common::{
    build_index, count_chunks, count_partials, create_test_file, find_count, find_paths, run_xdu,
};

/// Parse the leading size column of an `xdu-find -f size` line ("<size>\t<path>").
fn parse_first_size(out: &str) -> i64 {
    let line = out.lines().next().expect("no size output");
    line.split('\t')
        .next()
        .unwrap()
        .parse()
        .expect("size column not an integer")
}

// =============================================================================
// Basic counts, per-partition
// =============================================================================

#[test]
fn test_basic_and_per_partition_counts() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("alice/f1.txt"), 100).unwrap();
    create_test_file(&source.join("alice/f2.txt"), 200).unwrap();
    create_test_file(&source.join("bob/f3.txt"), 300).unwrap();

    build_index(&source, &index);

    assert_eq!(find_count(&index, &[]), 3);
    assert_eq!(find_count(&index, &["-u", "alice"]), 2);
    assert_eq!(find_count(&index, &["-u", "bob"]), 1);
}

// =============================================================================
// __root__: loose top-level files are their own partition; nested files are not
// =============================================================================

#[test]
fn test_root_partition_holds_only_loose_files() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("root.txt"), 10).unwrap(); // loose -> __root__
    create_test_file(&source.join("sub/nested.txt"), 20).unwrap(); // -> partition "sub"

    build_index(&source, &index);

    assert_eq!(find_count(&index, &[]), 2);
    assert_eq!(find_count(&index, &["-u", "__root__"]), 1);
    assert_eq!(find_count(&index, &["-u", "sub"]), 1);

    // The nested file must not leak into __root__.
    let root_paths = {
        let (out, _e, ok) = common::run_find(&[
            "-i",
            index.to_str().unwrap(),
            "-u",
            "__root__",
            "-f",
            "path",
        ]);
        assert!(ok);
        out
    };
    assert!(root_paths.contains("root.txt"));
    assert!(!root_paths.contains("nested.txt"));
}

// =============================================================================
// Deeply nested files are all captured under their top-level partition
// =============================================================================

#[test]
fn test_deeply_nested_files_counted() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("p/top.txt"), 10).unwrap();
    create_test_file(&source.join("p/a/b/c/deep.txt"), 20).unwrap();

    build_index(&source, &index);

    assert_eq!(find_count(&index, &[]), 2);
    assert_eq!(find_count(&index, &["-u", "p"]), 2);
}

// =============================================================================
// --partition filter selects a subset; an absent partition is a validation error
// =============================================================================

#[test]
fn test_partition_filter_and_absent_partition_error() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("alice/f.txt"), 100).unwrap();
    create_test_file(&source.join("bob/f.txt"), 100).unwrap();

    // Index only alice.
    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-o",
        index.to_str().unwrap(),
        "-p",
        "alice",
        source.to_str().unwrap(),
    ]);
    assert!(ok, "xdu -p alice failed: {e}");
    assert_eq!(find_count(&index, &[]), 1);
    assert_eq!(find_count(&index, &["-u", "alice"]), 1);

    // An absent partition name is rejected before crawling.
    let index2 = tmp.path().join("index2");
    let (_o2, e2, ok2) = run_xdu(&[
        "--apparent-size",
        "-o",
        index2.to_str().unwrap(),
        "-p",
        "ghost",
        source.to_str().unwrap(),
    ]);
    assert!(!ok2, "xdu should fail for an absent partition");
    assert!(
        e2.contains("Partition 'ghost' not found"),
        "stderr should name the missing partition, got: {e2}"
    );
}

// =============================================================================
// Size modes: apparent-size is exact; block-rounded rounds up
// =============================================================================

#[test]
fn test_size_modes() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");

    create_test_file(&source.join("p/file.bin"), 100).unwrap();

    // Apparent size is exact.
    let idx_apparent = tmp.path().join("apparent");
    build_index(&source, &idx_apparent);
    let (out, _e, ok) = common::run_find(&["-i", idx_apparent.to_str().unwrap(), "-f", "size"]);
    assert!(ok);
    assert_eq!(parse_first_size(&out), 100);

    // Block-rounded: 100 bytes rounds up to a full 4096-byte block.
    let idx_rounded = tmp.path().join("rounded");
    let (_o, e, ok) = run_xdu(&[
        "-k",
        "4096",
        "-o",
        idx_rounded.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);
    assert!(ok, "xdu -k 4096 failed: {e}");
    let (out, _e, ok) = common::run_find(&["-i", idx_rounded.to_str().unwrap(), "-f", "size"]);
    assert!(ok);
    assert_eq!(parse_first_size(&out), 4096);
}

// =============================================================================
// An empty tree fails loud with a clear diagnostic
// =============================================================================

#[test]
fn test_empty_tree_fails_with_no_partitions() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    fs::create_dir_all(&source).unwrap();

    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-o",
        index.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);
    assert!(!ok, "empty tree should not succeed");
    assert!(
        e.contains("No partitions found"),
        "stderr should explain the empty tree, got: {e}"
    );
}

// =============================================================================
// Re-indexing a shrunken tree drops stale rows (finalize prunes stale chunks)
// =============================================================================

#[test]
fn test_reindex_prunes_stale_rows() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    for name in ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"] {
        create_test_file(&source.join("p").join(name), 100).unwrap();
    }

    // Small buffsize => several chunks (ceil(5/2) = 3).
    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-B",
        "2",
        "-o",
        index.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);
    assert!(ok, "initial index failed: {e}");
    assert_eq!(find_count(&index, &[]), 5);
    assert!(count_chunks(&index, "p") >= 2);

    // Shrink the tree, then re-index the same directory.
    for name in ["c.txt", "d.txt", "e.txt"] {
        fs::remove_file(source.join("p").join(name)).unwrap();
    }
    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-B",
        "2",
        "-o",
        index.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);
    assert!(ok, "re-index failed: {e}");

    assert_eq!(find_count(&index, &[]), 2, "stale rows were not pruned");
    assert_eq!(count_chunks(&index, "p"), 1, "stale chunks were not pruned");
    assert_eq!(count_partials(&index), 0);
}

// =============================================================================
// A successful crawl leaves no *.partial files behind
// =============================================================================

#[test]
fn test_no_partial_files_after_success() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("alice/f1.txt"), 100).unwrap();
    create_test_file(&source.join("bob/f2.txt"), 100).unwrap();
    create_test_file(&source.join("loose.txt"), 100).unwrap();

    build_index(&source, &index);

    assert_eq!(find_count(&index, &[]), 3);
    assert_eq!(count_partials(&index), 0, "found leftover .partial files");
}

// =============================================================================
// Symlinks are excluded; only the regular file is indexed
// =============================================================================

#[test]
fn test_symlinks_excluded() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("p/real.txt"), 100).unwrap();
    symlink(source.join("p/real.txt"), source.join("p/link.txt")).unwrap();

    build_index(&source, &index);

    // Only the regular file is counted; the symlink is skipped.
    assert_eq!(find_count(&index, &[]), 1);
    let paths = find_paths(&index);
    assert!(paths.iter().any(|p| p.ends_with("real.txt")));
    assert!(!paths.iter().any(|p| p.ends_with("link.txt")));
}

// =============================================================================
// Determinism: two independent crawls of the same tree agree exactly
// =============================================================================

#[test]
fn test_crawl_is_deterministic() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");

    create_test_file(&source.join("alice/f1.txt"), 100).unwrap();
    create_test_file(&source.join("alice/f2.txt"), 200).unwrap();
    create_test_file(&source.join("bob/data.bin"), 500).unwrap();
    create_test_file(&source.join("charlie/notes.md"), 150).unwrap();
    create_test_file(&source.join("loose.txt"), 50).unwrap();

    let index1 = tmp.path().join("index1");
    let index2 = tmp.path().join("index2");
    build_index(&source, &index1);
    build_index(&source, &index2);

    assert_eq!(find_count(&index1, &[]), 5);
    assert_eq!(find_count(&index2, &[]), 5);
    assert_eq!(find_paths(&index1), find_paths(&index2));
}

// =============================================================================
// Buffsize chunking: a small -B splits a partition into multiple chunks
// =============================================================================

#[test]
fn test_buffsize_chunking() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    for name in ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"] {
        create_test_file(&source.join("p").join(name), 100).unwrap();
    }

    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-B",
        "2",
        "-o",
        index.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);
    assert!(ok, "chunked index failed: {e}");

    // All five files are present despite chunking...
    assert_eq!(find_count(&index, &[]), 5);
    // ...and they landed across more than one chunk file.
    assert!(
        count_chunks(&index, "p") >= 2,
        "expected multiple chunks with -B 2, got {}",
        count_chunks(&index, "p")
    );
    assert_eq!(count_partials(&index), 0);
}

// =============================================================================
// An unreadable subtree fails the run loud by default (the headline correctness fix)
// =============================================================================

#[test]
fn test_unreadable_subtree_fails_loud_by_default() {
    // Root bypasses permission bits, so this scenario cannot be reproduced as root.
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipping: running as root bypasses permission checks");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("data/readable.txt"), 100).unwrap();
    create_test_file(&source.join("data/secret/hidden.txt"), 100).unwrap();

    let secret = source.join("data/secret");
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o000)).unwrap();

    let (_out, err, ok) = run_xdu(&[
        "--apparent-size",
        "-o",
        index.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);

    // Restore perms before asserting so the TempDir can always be cleaned up.
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o755)).unwrap();

    assert!(!ok, "crawl must exit non-zero on an unreadable subtree");
    assert!(
        err.contains("secret"),
        "stderr must name the unreadable directory, got: {err}"
    );
    // The reachable file was still indexed; the hidden subtree was omitted.
    assert_eq!(find_count(&index, &["-u", "data"]), 1);
}

// =============================================================================
// --allow-errors downgrades the hard error to a warning and exits 0
// =============================================================================

#[test]
fn test_unreadable_subtree_allow_errors_continues() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipping: running as root bypasses permission checks");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("data/readable.txt"), 100).unwrap();
    create_test_file(&source.join("data/secret/hidden.txt"), 100).unwrap();

    let secret = source.join("data/secret");
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o000)).unwrap();

    let (_out, err, ok) = run_xdu(&[
        "--apparent-size",
        "--allow-errors",
        "-o",
        index.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);

    fs::set_permissions(&secret, fs::Permissions::from_mode(0o755)).unwrap();

    assert!(
        ok,
        "with --allow-errors the crawl must exit 0, stderr: {err}"
    );
    // The error is still reported (a non-zero count) and reachable files still indexed.
    assert!(
        err.contains("secret"),
        "stderr should still report the skipped path: {err}"
    );
    assert!(
        err.contains("errors"),
        "summary should still report the error count: {err}"
    );
    assert_eq!(find_count(&index, &["-u", "data"]), 1);
}
