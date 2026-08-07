//! Offline-readiness tests: the readers must answer a query on a host whose DuckDB
//! extension cache is empty, and must not populate it.
//!
//! `xdu` targets HPC login nodes, where an air-gapped host is a normal deployment. DuckDB's
//! `bundled` feature compiles the engine but does **not** link the Parquet reader, and
//! autoinstall/autoload default to on — so without the `parquet` feature every `read_parquet`
//! resolves at runtime by downloading a 12 MB extension into `$HOME/.duckdb/extensions/`.
//! Redirecting `HOME` at an empty directory turns that download into a visible artifact, which
//! is what these tests assert the absence of. A green result here is only meaningful because
//! the assertion was observed failing against the pre-fix dependency line.
//!
//! The index is built under the **normal** environment: the crawler writes Parquet through the
//! `parquet` crate and links no DuckDB at all, so anything appearing in the cold `HOME` is
//! attributable to a reader.
//!
//! `xdu-view` is deliberately not driven — it is a full-screen TUI needing a terminal, and CI is
//! headless. That is not a coverage gap: the `parquet` feature applies to the single shared
//! `duckdb` dependency, so linkage is crate-wide and `xdu-view` cannot link a different DuckDB
//! than `xdu-find` does. Do not "fix" this by starting a TUI in CI.

mod common;

use std::fs;

use tempfile::TempDir;

use common::{build_index, create_test_file, list_files_recursive, run_binary_with_home};

/// A reader answers correctly against an empty extension cache and leaves it empty.
#[test]
fn test_readers_query_cold_cache_without_writing_to_it() {
    let temp_dir = TempDir::new().unwrap();
    let source = temp_dir.path().join("source");
    let index = temp_dir.path().join("index");
    let cold_home = temp_dir.path().join("cold-home");
    fs::create_dir_all(&cold_home).unwrap();

    // Three files, two of which match the pattern the dry-run below filters on.
    create_test_file(&source.join("user1/a.log"), 100).unwrap();
    create_test_file(&source.join("user1/b.log"), 100).unwrap();
    create_test_file(&source.join("user1/c.txt"), 100).unwrap();
    build_index(&source, &index);

    // Exit 0 is not the assertion — the row count is. A reader that fell back to a broken
    // path could still exit 0.
    let (out, err, ok) = run_binary_with_home(
        "xdu-find",
        &cold_home,
        &["-i", index.to_str().unwrap(), "--count"],
    );
    assert!(
        ok,
        "xdu-find failed against an empty extension cache: {err}"
    );
    assert_eq!(
        out.trim(),
        "3",
        "xdu-find returned the wrong count on a cold cache (stderr: {err})"
    );

    let (out_rm, err_rm, ok_rm) = run_binary_with_home(
        "xdu-rm",
        &cold_home,
        &[
            "-i",
            index.to_str().unwrap(),
            "--pattern",
            "\\.log$",
            "--dry-run",
        ],
    );
    assert!(
        ok_rm,
        "xdu-rm failed against an empty extension cache: {err_rm}"
    );
    assert!(
        out_rm.contains("2 file(s) would be deleted"),
        "xdu-rm matched the wrong set on a cold cache: {out_rm}"
    );

    // The whole point: no reader may install anything into the extension cache.
    let written = list_files_recursive(&cold_home);
    assert!(
        written.is_empty(),
        "a reader is autoloading a DuckDB extension at runtime — the Parquet reader is being \
         downloaded instead of statically linked, so the tools cannot query on an air-gapped \
         host. Files written under the cold HOME: {written:#?}"
    );
}
