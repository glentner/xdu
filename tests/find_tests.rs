//! Integration tests for the `xdu-find` owner/group/mode/mtime filters and the
//! widened csv/json output.
//!
//! These drive the **real** `xdu` / `xdu-find` binaries against throwaway
//! `tempfile` trees and assert concrete post-conditions (row counts, headers,
//! keys, exit status) — never by reimplementing the query. Expectations about
//! the current user and group come from `libc` directly, not from the
//! `xdu::resolve_user` helpers under test, so the tests do not grade the
//! library against itself.

mod common;

use std::ffi::{CStr, CString};
use std::os::unix::fs::PermissionsExt;

use tempfile::TempDir;

use common::{build_index, create_test_file, find_count, run_find, set_mtime_days_ago};

/// Current login name and uid from the system database, independent of `xdu`.
fn current_user() -> (String, u32) {
    unsafe {
        let euid = libc::geteuid();
        let entry = libc::getpwuid(euid);
        assert!(!entry.is_null(), "no passwd entry for euid {euid}");
        let name = CStr::from_ptr((*entry).pw_name)
            .to_string_lossy()
            .into_owned();
        (name, euid)
    }
}

/// Current group name and gid from the system database, independent of `xdu`.
fn current_group() -> (String, u32) {
    unsafe {
        let egid = libc::getegid();
        let entry = libc::getgrgid(egid);
        assert!(!entry.is_null(), "no group entry for egid {egid}");
        let name = CStr::from_ptr((*entry).gr_name)
            .to_string_lossy()
            .into_owned();
        (name, egid)
    }
}

/// Three-file fixture tree; every file belongs to the invoking user and group.
fn three_file_tree(source: &std::path::Path) {
    create_test_file(&source.join("part/a.txt"), 100).unwrap();
    create_test_file(&source.join("part/b.txt"), 200).unwrap();
    create_test_file(&source.join("part/c.txt"), 300).unwrap();
}

// =============================================================================
// --owner / --group
// =============================================================================

#[test]
fn test_find_owner_filter_current_user_by_name_and_id() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    three_file_tree(&source);
    build_index(&source, &index);

    let (name, uid) = current_user();
    assert_eq!(find_count(&index, &["--owner", &name]), 3);
    assert_eq!(find_count(&index, &["--owner", &uid.to_string()]), 3);
    // Negative control: every file belongs to the invoker, so an unrelated uid
    // must match nothing. Without it an ignored --owner still counts 3.
    assert_eq!(find_count(&index, &["--owner", "4294967294"]), 0);
}

#[test]
fn test_find_owner_filter_unresolvable_name_fails_clean() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    three_file_tree(&source);
    build_index(&source, &index);

    let (out, err, ok) = run_find(&[
        "-i",
        index.to_str().unwrap(),
        "--owner",
        "xdu-no-such-user",
        "--count",
    ]);
    assert!(!ok, "unresolvable owner must exit non-zero");
    assert!(out.is_empty(), "no rows may print, got: {out:?}");
    assert!(
        err.contains("xdu-no-such-user"),
        "stderr names the miss: {err:?}"
    );
}

#[test]
fn test_find_owner_filter_other_user() {
    // Needs privilege: handing a file to another uid fails for an unprivileged
    // invoker, and there is nothing to assert then.
    let nobody = unsafe {
        let cname = CString::new("nobody").unwrap();
        let entry = libc::getpwnam(cname.as_ptr());
        if entry.is_null() {
            eprintln!("SKIP test_find_owner_filter_other_user: no 'nobody' entry here");
            return;
        }
        (*entry).pw_uid
    };
    let (my_name, my_uid) = current_user();
    if nobody == my_uid {
        eprintln!("SKIP test_find_owner_filter_other_user: invoking user is nobody");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    three_file_tree(&source);
    let foreign = source.join("part/c.txt");
    let c_path = CString::new(foreign.to_str().unwrap()).unwrap();
    if unsafe { libc::chown(c_path.as_ptr(), nobody, u32::MAX) } != 0 {
        eprintln!("SKIP test_find_owner_filter_other_user: chown needs privilege here");
        return;
    }
    build_index(&source, &index);

    assert_eq!(find_count(&index, &["--owner", &my_name]), 2);
    assert_eq!(find_count(&index, &["--owner", "nobody"]), 1);
}

#[test]
fn test_find_group_filter_current_group() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    three_file_tree(&source);
    build_index(&source, &index);

    let (name, gid) = current_group();
    assert_eq!(find_count(&index, &["--group", &name]), 3);
    assert_eq!(find_count(&index, &["--group", &gid.to_string()]), 3);
    // Negative control, as for --owner: an unrelated gid must match nothing.
    assert_eq!(find_count(&index, &["--group", "4294967294"]), 0);

    let (out, _err, ok) = run_find(&[
        "-i",
        index.to_str().unwrap(),
        "--group",
        "xdu-no-such-group",
        "--count",
    ]);
    assert!(!ok, "unresolvable group must exit non-zero");
    assert!(out.is_empty(), "no rows may print, got: {out:?}");
}

// =============================================================================
// --mode
// =============================================================================

#[test]
fn test_find_mode_filter_exact_any_all() {
    use std::fs;

    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    three_file_tree(&source);
    fs::set_permissions(source.join("part/a.txt"), fs::Permissions::from_mode(0o644)).unwrap();
    fs::set_permissions(source.join("part/b.txt"), fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(source.join("part/c.txt"), fs::Permissions::from_mode(0o777)).unwrap();
    build_index(&source, &index);

    assert_eq!(find_count(&index, &["--mode", "644"]), 1);
    assert_eq!(find_count(&index, &["--mode", "/002"]), 1);
    assert_eq!(find_count(&index, &["--mode", "/400"]), 3);
    assert_eq!(find_count(&index, &["--mode", "&4000"]), 0);

    let (out, _err, ok) = run_find(&["-i", index.to_str().unwrap(), "--mode", "999", "--count"]);
    assert!(!ok, "invalid mode SPEC must exit non-zero");
    assert!(out.is_empty(), "no rows may print, got: {out:?}");
}

// =============================================================================
// --mtime-older-than / --mtime-newer-than
// =============================================================================

#[test]
fn test_find_mtime_filters() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    three_file_tree(&source);
    set_mtime_days_ago(&source.join("part/c.txt"), 30).unwrap();
    build_index(&source, &index);

    assert_eq!(find_count(&index, &["--mtime-older-than", "30"]), 1);
    assert_eq!(find_count(&index, &["--mtime-newer-than", "30"]), 2);
    assert_eq!(find_count(&index, &["--mtime-newer-than", "1"]), 2);
}

// =============================================================================
// csv / json carry the new columns; path format is untouched
// =============================================================================

#[test]
fn test_find_csv_carries_new_columns() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    three_file_tree(&source);
    build_index(&source, &index);

    let (out, err, ok) = run_find(&["-i", index.to_str().unwrap(), "-f", "csv"]);
    assert!(ok, "csv query failed: {err}");
    let mut lines = out.lines();
    assert_eq!(
        lines.next().unwrap(),
        "path,size,uid,gid,mode,atime,mtime,ctime"
    );
    let rows: Vec<&str> = lines.collect();
    assert_eq!(rows.len(), 3, "three data rows expected: {rows:?}");
    for row in rows {
        assert_eq!(row.split(',').count(), 8, "eight fields: {row:?}");
    }
}

#[test]
fn test_find_json_carries_new_columns() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    three_file_tree(&source);
    build_index(&source, &index);

    let (out, err, ok) = run_find(&["-i", index.to_str().unwrap(), "-f", "json"]);
    assert!(ok, "json query failed: {err}");
    assert_eq!(out.matches("\"path\":\"").count(), 3);
    for key in [
        "\"uid\":",
        "\"gid\":",
        "\"mode\":",
        "\"mtime\":",
        "\"ctime\":",
    ] {
        assert_eq!(out.matches(key).count(), 3, "key {key} on every row");
    }
}

#[test]
fn test_find_path_format_unchanged() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source");
    let index = tmp.path().join("index");
    three_file_tree(&source);
    build_index(&source, &index);

    let (out, err, ok) = run_find(&["-i", index.to_str().unwrap(), "-f", "path"]);
    assert!(ok, "path query failed: {err}");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(
        lines.iter().all(|l| l.contains("part/")),
        "one path per line, no header: {lines:?}"
    );
}
