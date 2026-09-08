//! Integration tests for on-disk index format versioning.
//!
//! These drive the **real** binaries against throwaway `tempfile` indexes and assert the
//! version contract end to end: a fresh index queries cleanly, while a missing,
//! versionless, or unrecognized version refuses in every reader before any row is read —
//! and a refused `xdu-rm` unlinks nothing.

mod common;

use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use common::{build_index, create_test_file, run_find, run_rm, run_view};

/// The run-level completion marker carrying the format version.
const COMPLETION_MARKER: &str = ".xdu-complete";

/// A two-file source tree indexed into a fresh throwaway index; the `TempDir` is
/// returned to keep both paths alive.
fn fresh_index() -> (TempDir, PathBuf, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    create_test_file(&source.join("alice/f1.txt"), 100).unwrap();
    create_test_file(&source.join("bob/f2.txt"), 200).unwrap();
    build_index(&source, &index);
    (tmp, source, index)
}

fn index_arg(index: &Path) -> String {
    index.to_str().unwrap().to_string()
}

// =============================================================================
// A fresh index queries cleanly with no version diagnostic
// =============================================================================

#[test]
fn test_fresh_index_queries_without_version_diagnostic() {
    let (_tmp, _source, index) = fresh_index();
    let idx = index_arg(&index);

    let (out, err, ok) = run_find(&["-i", &idx, "--count"]);
    assert!(ok, "fresh index must query cleanly: {err}");
    assert_eq!(out.trim(), "2");
    assert!(
        !err.contains("format version"),
        "fresh index must not raise the version gate: {err}"
    );

    // A dry run prints the deletion set without prompting; the version gate must let
    // it through exactly like the query above.
    let (out, err, ok) = run_rm(&["-i", &idx, "--dry-run"]);
    assert!(ok, "fresh index must dry-run cleanly: {err}");
    assert!(out.contains("f1.txt"), "dry run must list the match: {out}");
    assert!(
        out.contains("2 file(s) would be deleted."),
        "dry run must report the full set: {out}"
    );
    assert!(
        !err.contains("format version"),
        "fresh index must not raise the version gate: {err}"
    );
}

// =============================================================================
// No marker: every reader refuses before reading a row
// =============================================================================

#[test]
fn test_missing_marker_refuses_all_readers() {
    let (_tmp, source, index) = fresh_index();
    let idx = index_arg(&index);
    fs::remove_file(index.join(COMPLETION_MARKER)).unwrap();

    let (out, err, ok) = run_find(&["-i", &idx, "--count"]);
    assert!(!ok, "markerless index must refuse");
    assert!(out.trim().is_empty(), "refusal must print no rows: {out:?}");
    assert!(
        err.contains("format version"),
        "refusal must name the cause: {err}"
    );
    assert!(
        err.contains("re-index"),
        "refusal must direct a re-index: {err}"
    );

    let (_out, err, ok) = run_rm(&["-i", &idx, "--dry-run"]);
    assert!(!ok, "markerless index must refuse deletion");
    assert!(
        err.contains("format version"),
        "refusal must name the cause: {err}"
    );
    assert!(
        source.join("alice/f1.txt").exists() && source.join("bob/f2.txt").exists(),
        "a refused rm must unlink nothing"
    );

    // Refusal happens before the terminal is touched, so no TTY is needed to see it.
    let (_out, err, ok) = run_view(&["-i", &idx]);
    assert!(!ok, "markerless index must refuse the viewer");
    assert!(
        err.contains("format version"),
        "refusal must name the cause: {err}"
    );
}

// =============================================================================
// A pre-versioning marker is versionless however clean its run was
// =============================================================================

#[test]
fn test_versionless_marker_refuses() {
    let (_tmp, _source, index) = fresh_index();
    let idx = index_arg(&index);
    fs::write(index.join(COMPLETION_MARKER), "xdu=test\nerrors=0\n").unwrap();

    let (out, err, ok) = run_find(&["-i", &idx, "--count"]);
    assert!(!ok, "versionless index must refuse, not read blind");
    assert!(out.trim().is_empty(), "refusal must print no rows: {out:?}");
    assert!(
        err.contains("no index format version"),
        "refusal must say no version was found: {err}"
    );
    assert!(
        err.contains("re-index"),
        "refusal must direct a re-index: {err}"
    );

    let (_out, err, ok) = run_rm(&["-i", &idx, "--dry-run"]);
    assert!(!ok, "versionless index must refuse deletion");
    assert!(
        err.contains("no index format version"),
        "refusal must say why: {err}"
    );
}

// =============================================================================
// An unrecognized version names both sides of the mismatch
// =============================================================================

#[test]
fn test_unknown_version_refuses_naming_both_sides() {
    let (_tmp, source, index) = fresh_index();
    let idx = index_arg(&index);
    fs::write(index.join(COMPLETION_MARKER), "xdu=test\nformat=999\n").unwrap();

    let (out, err, ok) = run_find(&["-i", &idx, "--count"]);
    assert!(!ok, "unknown version must refuse");
    assert!(out.trim().is_empty(), "refusal must print no rows: {out:?}");
    assert!(
        err.contains("999"),
        "refusal must name the found version: {err}"
    );
    assert!(
        err.contains("supports version"),
        "refusal must name the supported version: {err}"
    );
    assert!(
        err.contains("re-index"),
        "refusal must direct a re-index: {err}"
    );

    let (_out, err, ok) = run_rm(&["-i", &idx, "--dry-run"]);
    assert!(!ok, "unknown version must refuse deletion");
    assert!(
        err.contains("999"),
        "refusal must name the found version: {err}"
    );
    assert!(
        source.join("alice/f1.txt").exists() && source.join("bob/f2.txt").exists(),
        "a refused rm must unlink nothing"
    );

    let (_out, err, ok) = run_view(&["-i", &idx]);
    assert!(!ok, "unknown version must refuse the viewer");
    assert!(
        err.contains("999"),
        "refusal must name the found version: {err}"
    );
}
