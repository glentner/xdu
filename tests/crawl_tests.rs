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

/// The run-level completion marker `xdu` writes at the index root on success.
const COMPLETION_MARKER: &str = ".xdu-complete";

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
// A real __root__ subdirectory collides with the reserved partition and is rejected
// =============================================================================

#[test]
fn test_reserved_root_partition_name_is_rejected() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");

    // A real top-level subdirectory of the reserved name, alongside a loose file
    // that would create the synthetic __root__ partition: both would write chunk
    // 000000 into the same directory.
    create_test_file(&source.join("__root__/inner.txt"), 100).unwrap();
    create_test_file(&source.join("loose.txt"), 100).unwrap();
    create_test_file(&source.join("p/f.txt"), 100).unwrap();

    // Serialized (-j 1) the two work items would still collide via finalize's prune.
    for jobs in ["4", "1"] {
        let index = tmp.path().join(format!("index-j{jobs}"));
        let (_o, e, ok) = run_xdu(&[
            "--apparent-size",
            "-j",
            jobs,
            "-o",
            index.to_str().unwrap(),
            source.to_str().unwrap(),
        ]);

        assert!(!ok, "a __root__ collision must fail the run (-j {jobs})");
        assert!(
            e.contains("__root__"),
            "stderr must name the colliding partition (-j {jobs}), got: {e}"
        );
        // Rejected before any crawling: nothing was written to the index.
        assert!(
            !index.join(COMPLETION_MARKER).exists(),
            "a rejected run must not be marked complete"
        );
        assert_eq!(count_chunks(&index, "p"), 0);
        assert_eq!(count_partials(&index), 0);
    }
}

// =============================================================================
// The completion marker attests to a whole run: written on success, gone on failure
// =============================================================================

#[test]
fn test_completion_marker_written_on_success_and_cleared_on_failure() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("p/f1.txt"), 100).unwrap();
    create_test_file(&source.join("q/f2.txt"), 100).unwrap();

    build_index(&source, &index);

    let marker = index.join(COMPLETION_MARKER);
    assert!(
        marker.exists(),
        "a successful crawl must write the completion marker"
    );
    let body = fs::read_to_string(&marker).unwrap();
    assert!(
        body.contains("files=2"),
        "marker should record the run totals, got: {body}"
    );
    assert_eq!(find_count(&index, &[]), 2);
    assert_eq!(count_partials(&index), 0);

    // The marker is a top-level dotfile, never a partition: the readers still see
    // exactly the two indexed files.
    assert_eq!(find_count(&index, &["-u", "p"]), 1);

    // Sabotage one partition's output — a regular file where its directory belongs
    // makes that driver fail when it flushes.
    fs::remove_dir_all(index.join("p")).unwrap();
    fs::write(index.join("p"), b"not a directory").unwrap();

    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-o",
        index.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);

    assert!(!ok, "a driver failure must fail the run, stderr: {e}");
    assert!(
        !marker.exists(),
        "a failed run must not leave the previous run's completion marker behind"
    );
}

// =============================================================================
// A run rejected before it writes leaves a complete index's marker intact
// =============================================================================

#[test]
fn test_rejected_run_leaves_existing_marker_intact() {
    let tmp = TempDir::new().unwrap();
    let good = tmp.path().join("good");
    let index = tmp.path().join("index");

    create_test_file(&good.join("p/f1.txt"), 100).unwrap();
    create_test_file(&good.join("p/f2.txt"), 100).unwrap();

    build_index(&good, &index);

    let marker = index.join(COMPLETION_MARKER);
    let attestation = fs::read_to_string(&marker).expect("the initial run must be marked complete");

    // Each leg re-runs against the same index with a source that xdu rejects during
    // pre-flight, before it touches the index. The attestation describes an index that
    // was not rewritten, so it must survive byte-for-byte.
    let assert_intact = |leg: &str| {
        assert_eq!(
            fs::read_to_string(&marker).ok().as_deref(),
            Some(attestation.as_str()),
            "a run rejected in pre-flight must leave the marker untouched ({leg})"
        );
        assert_eq!(
            find_count(&index, &[]),
            2,
            "the pre-existing index must still be queryable ({leg})"
        );
    };

    // Leg 1: an empty source tree — nothing to index.
    let empty = tmp.path().join("empty");
    fs::create_dir_all(&empty).unwrap();
    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-o",
        index.to_str().unwrap(),
        empty.to_str().unwrap(),
    ]);
    assert!(!ok, "an empty tree must fail the run");
    assert!(e.contains("No partitions found"), "got: {e}");
    assert_intact("empty tree");

    // Leg 2: a real top-level directory using the reserved partition name.
    let reserved = tmp.path().join("reserved");
    create_test_file(&reserved.join("__root__/inner.txt"), 100).unwrap();
    create_test_file(&reserved.join("p/f.txt"), 100).unwrap();
    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-o",
        index.to_str().unwrap(),
        reserved.to_str().unwrap(),
    ]);
    assert!(!ok, "a __root__ collision must fail the run");
    assert!(e.contains("reserved"), "got: {e}");
    assert_intact("reserved __root__ name");

    // Leg 3: an unreadable source root — the top-level enumeration itself fails.
    // Root bypasses permission bits, so this leg cannot be reproduced as root.
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipping the unreadable-source leg: running as root bypasses permissions");
    } else {
        let locked = tmp.path().join("locked");
        create_test_file(&locked.join("p/f.txt"), 100).unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();

        let (_o, e, ok) = run_xdu(&[
            "--apparent-size",
            "-o",
            index.to_str().unwrap(),
            locked.to_str().unwrap(),
        ]);

        // Restore perms before asserting so the TempDir can always be cleaned up.
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(!ok, "an unreadable source root must fail the run");
        // Canonicalizing the root only lstats it, so the failure lands on the top-level
        // enumeration — the rejection site this leg exists to cover.
        assert!(
            e.contains("Failed to read directory"),
            "stderr must explain the unreadable source, got: {e}"
        );
        assert_intact("unreadable source root");
    }

    // The fail-safe still holds below the clear: a run that gets past pre-flight and
    // then fails mid-crawl must strip the attestation it can no longer support.
    fs::remove_dir_all(index.join("p")).unwrap();
    fs::write(index.join("p"), b"not a directory").unwrap();
    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-o",
        index.to_str().unwrap(),
        good.to_str().unwrap(),
    ]);
    assert!(!ok, "a driver failure must fail the run, stderr: {e}");
    assert!(
        !marker.exists(),
        "a run that started writing and then failed must leave the index unattested"
    );
}

// =============================================================================
// A markerless index still queries: readers warn on stderr, they do not refuse
// =============================================================================

#[test]
fn test_reader_warns_but_still_queries_markerless_index() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("p/f1.txt"), 100).unwrap();
    create_test_file(&source.join("p/f2.txt"), 100).unwrap();

    build_index(&source, &index);

    // A complete index is quiet.
    let (out, err, ok) = common::run_find(&["-i", index.to_str().unwrap(), "--count"]);
    assert!(ok);
    assert_eq!(out.trim(), "2");
    assert!(
        !err.contains("completion marker"),
        "a complete index must not warn: {err}"
    );

    // Indexes built before the marker existed have no marker; they must keep working.
    fs::remove_file(index.join(COMPLETION_MARKER)).unwrap();

    let (out, err, ok) = common::run_find(&["-i", index.to_str().unwrap(), "--count"]);
    assert!(ok, "a markerless index must still be queryable: {err}");
    assert_eq!(out.trim(), "2", "the warning must not change the results");
    assert!(
        err.contains("completion marker"),
        "stderr should carry the soft warning, got: {err}"
    );
    // Diagnostics stay off stdout so a piped count is still just a number.
    assert!(!out.contains("warning"));
}

// =============================================================================
// A loose top-level symlink is not a root file: no empty __root__ partition
// =============================================================================

#[test]
fn test_loose_symlink_does_not_create_root_partition() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("p/real.txt"), 100).unwrap();
    // The walk never indexes symlinks, so a loose one has nothing to contribute.
    symlink(source.join("p/real.txt"), source.join("link.txt")).unwrap();

    build_index(&source, &index);

    assert_eq!(find_count(&index, &[]), 1);
    assert!(
        !index.join("__root__").exists(),
        "a loose symlink must not spawn an empty __root__ partition"
    );
}

// =============================================================================
// A non-UTF-8 path is indexed lossily, but counted and reported (never fatal)
// =============================================================================

#[test]
fn test_non_utf8_path_is_counted_and_reported() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");

    create_test_file(&source.join("p/ok.txt"), 100).unwrap();

    // Filesystems that enforce UTF-8 filenames (APFS/HFS+) reject this name outright;
    // where raw bytes are allowed (ext4, XFS, Lustre) the warning path is exercised.
    let bad = source.join("p").join(OsStr::from_bytes(b"bad\xffname.txt"));
    if create_test_file(&bad, 50).is_err() {
        eprintln!("skipping: this filesystem rejects non-UTF-8 filenames");
        return;
    }

    let (_o, e, ok) = run_xdu(&[
        "--apparent-size",
        "-o",
        index.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);

    assert!(ok, "a non-UTF-8 path must not fail the run, stderr: {e}");
    // Both files are indexed; the lossy one just cannot round-trip.
    assert_eq!(find_count(&index, &[]), 2);
    assert!(
        e.contains("non-UTF-8"),
        "stderr must report the lossy path, got: {e}"
    );
    assert!(
        index.join(COMPLETION_MARKER).exists(),
        "lossy paths are counted, not fatal — the run still completes"
    );
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
