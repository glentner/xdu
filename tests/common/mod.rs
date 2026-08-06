//! Shared helpers for the real-binary integration tests.
//!
//! These drive the actual `xdu` / `xdu-find` binaries (`std::process::Command`) against
//! throwaway `tempfile` indexes — never a real filesystem — and assert concrete
//! post-conditions (row counts, files present-or-absent), not merely exit 0.
//!
//! `dead_code` is allowed because each test file uses only a subset of these helpers,
//! and every `tests/*.rs` file compiles the module into its own separate crate.
#![allow(dead_code)]

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Path to a binary built for *this* test invocation.
///
/// `CARGO_BIN_EXE_<name>` is set by Cargo to the binary compiled for the current test
/// profile, so the tests never accidentally exercise a stale `target/release` artifact
/// left over from an earlier build.
pub fn binary_path(name: &str) -> PathBuf {
    match name {
        "xdu" => env!("CARGO_BIN_EXE_xdu").into(),
        "xdu-find" => env!("CARGO_BIN_EXE_xdu-find").into(),
        "xdu-rm" => env!("CARGO_BIN_EXE_xdu-rm").into(),
        other => panic!("unknown test binary: {other}"),
    }
}

/// Create a test file of a specific byte size, making parent dirs as needed.
pub fn create_test_file(path: &Path, size: usize) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path)?;
    file.write_all(&vec![b'x'; size])?;
    Ok(())
}

/// Run `xdu` with arbitrary args; returns (stdout, stderr, success).
pub fn run_xdu(args: &[&str]) -> (String, String, bool) {
    let output = Command::new(binary_path("xdu"))
        .args(args)
        .output()
        .expect("failed to spawn xdu");
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.success(),
    )
}

/// Build an index over `source` into `index`, asserting success.
///
/// Uses `--apparent-size` so sizes are exact and filesystem-independent.
pub fn build_index(source: &Path, index: &Path) {
    let (_out, err, ok) = run_xdu(&[
        "--apparent-size",
        "-o",
        index.to_str().unwrap(),
        source.to_str().unwrap(),
    ]);
    assert!(ok, "xdu build failed: {err}");
}

/// Run `xdu-find` with arbitrary args; returns (stdout, stderr, success).
pub fn run_find(args: &[&str]) -> (String, String, bool) {
    let output = Command::new(binary_path("xdu-find"))
        .args(args)
        .output()
        .expect("failed to spawn xdu-find");
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.success(),
    )
}

/// Run `xdu-rm` with arbitrary args; returns (stdout, stderr, success).
pub fn run_rm(args: &[&str]) -> (String, String, bool) {
    let output = Command::new(binary_path("xdu-rm"))
        .args(args)
        .output()
        .expect("failed to spawn xdu-rm");
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.success(),
    )
}

/// Query the index row count, optionally scoped by extra `xdu-find` args.
pub fn find_count(index: &Path, extra: &[&str]) -> i64 {
    let mut args: Vec<&str> = vec!["-i", index.to_str().unwrap(), "--count"];
    args.extend_from_slice(extra);
    let (out, err, ok) = run_find(&args);
    assert!(ok, "xdu-find --count failed: {err}");
    out.trim()
        .parse()
        .unwrap_or_else(|_| panic!("non-integer count output: {out:?}"))
}

/// Sorted list of indexed paths (via `xdu-find -f path`).
pub fn find_paths(index: &Path) -> Vec<String> {
    let (out, err, ok) = run_find(&["-i", index.to_str().unwrap(), "-f", "path"]);
    assert!(ok, "xdu-find -f path failed: {err}");
    let mut lines: Vec<String> = out.lines().map(|l| l.to_string()).collect();
    lines.sort();
    lines
}

/// Count leftover `*.partial` files anywhere under the index directory.
pub fn count_partials(index: &Path) -> usize {
    let mut count = 0;
    let entries = match fs::read_dir(index) {
        Ok(e) => e,
        Err(_) => return 0,
    };
    for part in entries.filter_map(|e| e.ok()) {
        let p = part.path();
        if p.is_dir()
            && let Ok(chunks) = fs::read_dir(&p)
        {
            for chunk in chunks.filter_map(|e| e.ok()) {
                if chunk.file_name().to_string_lossy().ends_with(".partial") {
                    count += 1;
                }
            }
        }
    }
    count
}

/// Count finalized `*.parquet` chunk files inside one partition dir of the index.
pub fn count_chunks(index: &Path, partition: &str) -> usize {
    let part_dir = index.join(partition);
    let entries = match fs::read_dir(&part_dir) {
        Ok(e) => e,
        Err(_) => return 0,
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
        .count()
}
