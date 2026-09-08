# Name resolution, permission matching, and mtime CLI

Scope: product decisions for `xdu-find` owner/group/perm/mtime filters. The crawler stores
numeric uid/gid/mode/mtime/ctime; names are not columns. No NSS in the crawler.

## 1. NSS in this crate

Nothing in production calls `getpwnam`, `getgrnam`, `getpwuid`, or `getgrgid`. `libc` is a
dev-dependency (`Cargo.toml`) used by tests (`utimensat` in `tests/common/mod.rs`, `geteuid`
in `tests/crawl_tests.rs`, `mkfifo` in a `lib.rs` test). `MetadataExt` is already imported in
`crawl.rs` and `xdu-rm.rs` for `atime`/`blocks`; `uid()`, `gid()`, `mode()`, `mtime()`, and
`ctime()` are unread.

Promote `libc` to a runtime dependency. Do not add `users`, `uzers`, or `nix`. The helper is
two functions in `lib`:

```
fn resolve_user(name: &str) -> Result<u32, String>
fn resolve_group(name: &str) -> Result<u32, String>
```

`CString::new` failure (interior NUL) is unresolvable. Copy `pw_uid`/`gr_gid` off the
returned pointer immediately; do not retain it. Resolve in the bin at filter-build, store
the `u32` on `QueryFilters`, and emit `uid = N` / `gid = N` from `to_conditions`. NSS does
not belong inside SQL construction.

`-u` is already `--partition` on every query tool (`src/cli.rs`). Owner is a different axis
(alice's files inside bob's tree). Long flags only: `--owner` / `--group`.

## 2. Numeric-looking names (POSIX find -user)

POSIX `find -user uname` looks the name up; if uname is a digit string and no such user
exists, it is a user ID. Copy that.

Policy: `getpwnam` first. On miss, if the string is ASCII digits, parse as decimal `u32`.
Otherwise fail. Same for `getgrnam`. A user actually named `1000` wins over uid 1000.

R5 applies to an unresolvable *name*. A pure digit string that is not an NSS name is still
a uid: matching zero rows at exit 0 is correct, not an R5 miss. Overflow
(`999999999999`), a leading minus, and mixed tokens (`1000x`) are unresolvable and take
R5. `0` is uid 0.

Do not add a separate `--uid` flag this cycle.

## 3. Permission filter

`find -perm mode` is exact; `-perm -mode` is "all bits in mode are set"; GNU `/mode` is
any-bit. Symbolic `u=rwx` is a parser this cycle should not grow.

Exact `--mode 0644` cannot name an exposure class without enumerating every matching mode.
`--world-writable` answers one class and nothing else. R6 wants classes, plural.

One flag: `--perm-mask OCTAL`, meaning `(mode & mask) == mask` — `find -perm -mode`.
World-writable is `--perm-mask 002`; setuid is `--perm-mask 4000`; other-readable is
`--perm-mask 004`. Parse as octal (optional `0o` prefix); reject 8 and 9. Do not reuse
`parse_size` — it would read `0777` as decimal 777.

Exact match and OR-any-bit wait.

## 4. What to store for mode

`MetadataExt::mode()` is full `st_mode` (type bits | 07777). The index holds only regular
files (`is_file()` + `follow_links(false)`), so `S_IFREG` is constant. R1 says permission
bits. Store `st_mode & 0o7777` as Int64 (same width as `size`/`atime`): setuid, setgid,
sticky, plus rwxrwxrwx. Filters then do not have to mask type bits.

The crawler already holds `Metadata` from the walk's lstat; uid/gid/mode/mtime/ctime are
extra field reads, not extra syscalls. `file_size_and_atime` and `PartitionBuffer::add`
grow in the same commit as `get_schema()`.

## 5. mtime flags

`--older-than` / `--newer-than` take `Option<u64>` DAYS (`src/cli.rs`).
`QueryFilters::with_older_than` computes `now - days * 86400` and emits `atime < threshold`
/ `atime >= threshold`. The man page says "not accessed" / "accessed within". There is no
timestamp parser, and `parse_size` is the wrong shape.

R7 asks for the analogue, not a new time dialect. `--mtime-older-than DAYS` and
`--mtime-newer-than DAYS` reuse that conversion against the `mtime` column. Leave
`--older-than` as atime. `--modified-since TIMESTAMP` needs a parser the crate does not
have and is a different question than the DAYS pair already taught.

Do not copy `find -mtime`'s 24-hour truncation. xdu's `days * 86400` from now is the
shipped contract.

## 6. Thread-safety

`getpwnam`/`getgrnam` are not reentrant. R4–R7 this cycle are `xdu-find` only: one filter
build on the main thread before DuckDB runs. One resolve at startup is enough. Do not
resolve per row. `getpwnam_r` is extra buffer ceremony for a single-threaded call site;
skip it, and comment that the helper is not safe to call from a pool. `xdu-rm --jobs`
parallelizes unlink after the query, not NSS.

## Recommended trio (copy into PLAN)

1. **NSS.** `--owner NAME` / `--group NAME`. `getpwnam`/`getgrnam`, then decimal-digit
   uid/gid fallback. R5 on unresolvable non-numeric names (and overflow). libc only.
   Resolve once; `QueryFilters` stores `u32`.
2. **Permission.** `--perm-mask OCTAL` → `(mode & mask) == mask`. Store `st_mode & 0o7777`.
3. **mtime.** `--mtime-older-than DAYS` / `--mtime-newer-than DAYS`. Same 86400 helper as
   atime. `--older-than` stays atime.
