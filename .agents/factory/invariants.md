# Invariant gate & footgun checklist

A curated, explicitly-enumerated subset of the load-bearing invariants in
[`AGENTS.md`](../../AGENTS.md), maintained **in lockstep with it** (`AGENTS.md` is ground truth — if
this drifts, fix it). Two consumers:

- **`xdu-plan` (gate):** before research *and* after PLAN/TECH is drafted, walk the sections a change
  touches and confirm the design honors each. Record any bend in PLAN's deviation-justification table.
- **`xdu-review` (footgun list):** a violation of any §1–§12 invariant here is **auto-CRITICAL** and,
  when it touches a high-blast-radius file, forces a human sign-off gate. A §13 project-conventions
  violation is **HIGH**, not auto-CRITICAL.

Only invoke the sections relevant to the change — do not manufacture findings against untouched
subsystems.

## High-blast-radius files (any CONFIRMED finding here → mandatory human gate)

`src/bin/xdu-rm.rs` (destructive) · `src/bin/xdu.rs` (crawl + atomic finalize) ·
`src/lib.rs` (schema + `QueryFilters`/SQL) · `src/cli.rs` (the one CLI definition)

---

## 1. Parquet schema stability (`src/lib.rs::get_schema`) — highest blast radius

- `get_schema()` is the sole writer↔reader contract: exactly **three non-null** fields, fixed order —
  `path: Utf8`, `size: Int64`, `atime: Int64`. `size` is bytes as `i64` (meaning depends on the
  `SizeMode` chosen at index time — disk-usage vs apparent vs block-rounded — and is **not** recorded
  in the index); `atime` is Unix epoch seconds as `i64`.
- There is **no on-disk schema version.** Any change to `get_schema()`, `FileRecord`, or a reader's
  `read_parquet` column list is a **breaking, cross-cutting** index-format change touching `lib.rs` +
  the crawler + all three readers + every README `read_parquet` example.
- Issues #2 / #3 (add `owner`/`group`/`permissions`) are exactly this class — **require adding a
  schema version field before evolving the schema.**

## 2. Atomic finalization (`src/bin/xdu.rs::finalize`)

- Write each chunk to `NNNNNN.parquet.partial`, then `fs::rename` to `.parquet` **within the same
  directory** (POSIX-atomic); prune stale higher-numbered `.parquet` chunks left by a prior larger run.
- Never `File::create` a final `.parquet`, never cross-directory rename (breaks atomicity), never let
  a reader glob `.partial`.
- **Known limitation:** finalize is per-file, **not** per-partition atomic — a reader during finalize
  can see a mix of new and old chunks, and a crash mid-finalize leaves the partition inconsistent. Do
  **not** index a partition concurrently with purging it; document this rather than pretending
  otherwise.

## 3. Partition scheme

- Layout is `<index>/<partition>/NNNNNN.parquet`, `partition` = a top-level subdirectory name.
- Loose files directly under the indexed root go to the reserved partition **`__root__`**
  (`ROOT_PARTITION`), crawled at `max_depth(1)`; the two never overlap (no file double-counted).
- Chunk ids are zero-padded sequential; readers glob `*/*.parquet`.

## 4. `xdu-rm` destructive safety — read before touching `src/bin/xdu-rm.rs`

- Default requires an interactive `y/N` confirmation (anything but `y`/`yes` aborts, deleting nothing);
  `--dry-run` performs **zero** deletions; `--force` skips the prompt; empty match set prints a
  message and exits 0.
- `--safe` re-`stat`s each file immediately before unlink and skips it if the current metadata no
  longer matches the query (the stale-index guard). It must re-verify **every** filter it is combined
  with — today it checks only atime (`--older-than`) and size (`--max-size`); `--min-size`,
  `--newer-than`, and `--pattern` are **gaps** and the man page overclaims. Closing that gap must not
  weaken any existing check.
- **Any deletion (or previewable action) combined with `--limit` MUST carry a deterministic
  `ORDER BY`** so `--dry-run` and the real run select **identical** rows. A `LIMIT` without `ORDER BY`
  returns an arbitrary/unstable subset.

## 5. DuckDB injection surface (all readers)

- Every user-supplied value that reaches `read_parquet(...)` or a `WHERE`/`LIKE` clause is an
  injection surface: `--pattern`, `--partition` (`-u`), and the index path. Today only `--pattern` is
  escaped (single-quote doubling); partition names and index paths are interpolated **raw**.
- Route all such values through **one** validated escaping/quoting helper (or bound parameters). Numeric
  filters (`i64`) are injection-safe by type. Forbid raw `format!` of a partition name / index path
  into SQL — in the destructive `xdu-rm` a quote or `../`/glob-meta could escape the intended partition.

## 6. Unix-only

- `std::os::unix::fs::MetadataExt` is load-bearing: `atime()` for access time and `blocks()`×512 for
  `SizeMode::DiskUsage`. Do not introduce code paths that assume portability.
- A future S3 / cross-platform source (roadmap Phase 4) must special-case — S3 objects have no atime.

## 7. Shared rayon-pool concurrency (`src/bin/xdu.rs`)

- A **single** `Parallelism::RayonExistingPool` backs **all** jwalk walkers; up to `--jobs` driver
  `std::thread`s pull partitions from a `Mutex<VecDeque>` work queue; rayon work-stealing balances
  directory reads across active walkers so one huge partition can't starve the rest.
- `std::thread::scope` joins the drivers; the first `Err`/panic surfaces as the process result
  (non-zero exit). Thread budget: N pool + C drivers + 1 main. Preserve this shape.

## 8. Symlinks excluded

- `follow_links(false)` + `entry.file_type().is_file()` before recording — the index holds only
  regular files. Don't start following symlinks (double-counting / traversal risk).

## 9. `SortMode` age inversion (must stay consistent SQL ↔ in-memory)

- `age-desc` = oldest first = `atime ASC`; `age-asc` = newest first = `atime DESC` (inverted by
  design). The SQL builders (`to_order_by` / `to_partition_order_by`) and `xdu-view`'s in-memory
  `sort_entries` must agree; directories-first applies only to `Name` sort.

## 10. CLI single source of truth (`src/cli.rs`)

- `src/cli.rs` is the one definition of every flag; `gen-completions` emits completions from it and
  `doc/*.scd` man pages describe it — completions/man cannot advertise a flag the binaries lack.
- **A CLI change updates the affected `doc/*.scd` in the same commit.** Prefer clap `ValueEnum` /
  `value_parser` over free-form `String` validated late (so completion/help offer valid values).
- The `-p` overload is real (partition in `xdu`, pattern in the query tools; partition is `-u`
  there) — change both sides coherently, never one in isolation.

## 11. Altitude / testability

- Binary targets stay thin: arg parse, terminal setup/teardown, the event loop. All parsing,
  formatting, file-type sniffing, SQL-building, and state-transition logic belongs in the `xdu`
  library so `tests/` and unit tests can reach it. (This is the root cause of the untested 2487-line
  `xdu-view.rs`.) Model the TUI as a pure state machine where feasible (`update(&mut App, Action)`
  with no terminal I/O).

## 12. TUI terminal safety (`src/bin/xdu-view.rs`)

- Raw mode + alternate screen must be restored on **every** exit path **including panic** — via a
  `Drop` guard or panic hook, **not** plain sequential code after the event loop (`panic = "abort"`
  is set in `Cargo.toml`, so an unwind won't run trailing restore code).
- Sanitize previewed file bytes (`strip_ansi`) before any line reaches ratatui (terminal-escape
  injection from attacker-controlled file contents). Slice on char boundaries, never raw byte indices
  computed from a display width.

## 13. Project conventions (same-commit / packaging) — violations are HIGH, not CRITICAL

- **Version is single-sourced from `Cargo.toml`** (clap derives `--version` from `CARGO_PKG_VERSION`);
  never hardcode a version in `src/`.
- `share/` is a **generated** artifact (man via `scdoc` from `doc/*.scd`; completions via
  `gen-completions` from `src/cli.rs`), git-ignored, rebuilt in CI and by `/xdu-release`; CI asserts
  it generates. `.scd` sources carry no version string (a pure version bump doesn't touch them).
- Release tarball layout — `bin/{xdu,xdu-find,xdu-view,xdu-rm}` +
  `share/{man/man1/*.1, bash-completion/completions/*, zsh/site-functions/*}` — matches `install.sh`
  extraction exactly; keep them in lockstep.
- Non-TTY runs keep **stdout** clean/pipeable (progress and status go to **stderr**).
- Reuse shared helpers; do not duplicate logic across bins or reimplement production logic in tests.
- Comments are declarative statements of the invariant / the *why*; **never embed `spec/` `R#`/`P#`
  ids in source** (they restart per feature and collide across branches).
- Pre-release gate (mirrored by CI and `/xdu-release`): `cargo fmt --all -- --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test` all pass clean.
