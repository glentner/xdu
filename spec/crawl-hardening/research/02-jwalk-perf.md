# 02 — jwalk / crawl performance levers

Scope: `src/bin/xdu.rs` `crawl()` + `PartitionBuffer`; deps confirmed from `Cargo.lock`:
`jwalk 0.8.1`, `arrow 56.2.0`, `parquet 56.2.0`, `rayon 1.11`, `crossbeam 0.8` (already a
dep). API facts cited below are **confirmed** against docs.rs jwalk 0.8.1 (struct/source
pages) unless marked *assumed*.

## Premise correction (load-bearing)

The task frames the driver's `fs::metadata()` as a *second* stat. It is not, in the common
case. **jwalk does not stat during the walk.** Its `DirEntry` is built by `from_entry()`,
which copies only `file_name` + `file_type` from `std::fs::DirEntry` (confirmed:
jwalk source `core/dir_entry.rs`). `DirEntry` has **no cached metadata field**; `metadata()`
lazily calls `fs::symlink_metadata` when `follow_links(false)` (confirmed). So today there is
**one** stat per file — the driver's `fs::metadata()` — run **serially** on ≤`jobs` driver
threads. (Exception, *assumed*: on filesystems returning `DT_UNKNOWN`, `std`'s
`DirEntry::file_type()` falls back to an `lstat` inside the pool during walk — only *there* is
it a true double stat.)

The win is therefore **parallelize + relocate the one stat**, not "eliminate a double stat."
That reframes but does not weaken L1 below.

---

## L1 — Move stat into `process_read_dir`; driver does zero stats  ★ HIGHEST LEVERAGE

- **Lever:** compute `blocks()*512` / `len()` / `atime()` inside a `process_read_dir` callback
  (runs in the rayon pool, across directories concurrently) and stash the result in each
  entry's `client_state`. The driver then reads pre-computed values — no syscall on the driver.
  Today stat is the dominant per-file cost and is serialized on the driver; this spreads it
  over all N pool threads.
- **Magnitude:** order-of-magnitude for metadata-bound trees (HPC network FS: stat = a
  metadata-server RPC). Turns serial stat into pool-parallel stat.
- **API (confirmed):** switch `WalkDir` → `WalkDirGeneric<C>` with a custom `ClientState`
  whose `DirEntryState = Option<(i64 size, i64 atime)>` (`ReadDirState = ()`).
  `process_read_dir` sig: `Fn(Option<usize>, &Path, &mut C::ReadDirState,
  &mut Vec<Result<DirEntry<C>>>) + Send + Sync + 'static`. In the closure: for each `Ok(e)`
  with `e.file_type().is_file()`, call `e.metadata()` (→ `symlink_metadata`; `MetadataExt`
  works on the returned `std::fs::Metadata`), apply `size_mode.calculate(...)` (SizeMode is
  Copy, capture it), write into `e.client_state`. Keep `follow_links(false)`.
- **Correctness risk:** (a) `e.metadata()` is `symlink_metadata`, so it does **not** follow
  links — this preserves invariant #8 and is *safer* than the current `fs::metadata` (which
  follows; today only guarded by a prior `is_file` check, leaving a symlink-swap TOCTOU window).
  For regular files lstat≡stat, so size/atime/blocks semantics are byte-identical. (b) Must NOT
  remove directory entries from `children` — jwalk needs them to recurse; only annotate.
  (c) Preserve "skip on stat error" (store `None`, driver skips). (d) `busy_timeout: None`
  stays (never fall back off the shared pool).
- **Effort:** medium (~60-90 lines: state struct + closure + driver read path).

## L2 — Direct-to-Arrow builders; drop the `Vec<FileRecord>` intermediate

- **Lever:** `PartitionBuffer` currently pushes `FileRecord`s into a `Vec`, then `flush()`
  copies every path a *second* time into `StringBuilder`. Append straight into
  `StringBuilder` + two `Vec<i64>` as entries arrive; `flush()` just `finish()`es. Also
  `StringBuilder::new()` is uncapacitied — presize `StringBuilder::with_capacity(buffsize,
  buffsize * est_path_len)`.
- **Magnitude:** constant-factor, but real at billions (removes one full copy of every path
  string + the per-flush `Vec<FileRecord>` alloc + `to_string_lossy().to_string()` can become
  a single append). Path bytes copied once instead of twice.
- **API:** arrow 56 `StringBuilder::with_capacity(item_capacity, data_capacity)`,
  `Int64Builder::with_capacity`. No schema change (`get_schema()` unchanged).
- **Correctness risk:** low; internal to `PartitionBuffer`. `FileRecord` stays public in `lib`
  (still used by `tests/crawl_tests.rs` + unit tests) — only the buffer's internals change.
- **Effort:** low-medium.

## L3 — Raise the parallelism default / decouple pool size from driver count

- **Lever:** `jobs` (default **4**) sets *both* pool threads *and* ≤driver count. On a 64-core
  node this pins the readdir+stat pool to 4 threads. Once L1 puts stat in the pool, pool width
  is the throttle. Default `jobs` to `std::thread::available_parallelism()`; consider letting
  pool width exceed core count (2-4×) since metadata RPC latency-hiding on Lustre/GPFS benefits
  from oversubscription (*assumed* for network FS). Keep drivers few (they only encode/write
  after L1).
- **Magnitude:** order-of-magnitude on many-core / high-latency-metadata nodes; neutral on a
  4-core laptop.
- **Risk:** low; env `XDU_JOBS` + `-j` already exist. Don't silently oversubscribe a laptop —
  cap sensibly.
- **Effort:** low.

## L4 — Pipeline Parquet encode/compress/write off the driver

- **Lever:** after L1 the driver still serially encodes + Snappy-compresses + writes each
  100k-row chunk (blocking IO), stalling consumption (backpressure into the pool). Hand
  finished batches to a dedicated writer thread via a bounded `crossbeam`/`mpsc` channel;
  driver keeps collecting while the writer does IO.
- **Magnitude:** constant-factor (secondary to L1 — encode+write is a fraction of stat cost;
  matters most for a single huge *flat* partition where one driver owns everything).
- **API:** `crossbeam` (already vendored) bounded channel; writer thread owns the `ArrowWriter`
  / `.partial` files.
- **Correctness risk:** medium — must keep atomic finalize (invariant #2): writer writes
  `NNNNNN.parquet.partial`, chunk ids stay sequential, rename+prune happens only after the
  writer drains and joins. Don't let finalize race the writer.
- **Effort:** medium.

## L5 — Parquet writer settings (honest: mostly write-neutral)

- **Lever:** `path` is high-cardinality near-unique → **disable dictionary encoding for the
  path column** (`WriterProperties::set_column_dictionary_enabled` for `path`) to skip building
  a dictionary that never dedups (prefix-sharing does *not* help dictionary encoding, which
  dedups whole values). Optionally `DELTA_BINARY_PACKED` for `atime`/`size`. Keep **Snappy**
  (good speed/ratio). `set_max_row_group_size` is moot (chunk = 100k rows < 1M default = one
  row group already).
- **Magnitude:** minor write-throughput win (dictionary-disable); DELTA helps size/read, not
  write. Do not expect much.
- **Risk:** low; schema/read-compat unchanged (DuckDB reads any encoding).
- **Effort:** low.

## L6 — jwalk knobs: leave as-is, one structural caveat

- `skip_hidden(false)` correct (index hidden files); **keep `sort` off** (per-dir sort is pure
  overhead — query-side `ORDER BY path` already gives determinism). No `min_depth` needed.
- **Structural limit (flag):** jwalk's parallelism unit is the *directory*. A single
  billion-file *flat* directory is read by one `readdir` loop and its `process_read_dir` runs
  the whole children Vec on **one** pool thread — L1 parallelizes *across* dirs, not *within*
  one. Mitigation for that pathology: `children.par_iter_mut()` (via `pool.install`) inside the
  callback to fan the stats out. Add only if flat-dir partitions are real; adds complexity.

---

## Measure-first caveats

1. **Profile the read(pool)/stat/encode split first.** L1's payoff assumes stat dominates —
   true on network metadata FS, less so on warm-cache local NVMe where Snappy encode can rival
   stat (then L4 rises).
2. **Correct the "double stat" story** before quoting numbers: it's one serial stat today, not
   two. L1's benefit is parallelization + a zero-stat driver, not stat removal.
3. Benchmark on the real target filesystem (Lustre/GPFS metadata RPC latency behaves nothing
   like ext4/NVMe); tune L3 pool width empirically there.
4. Ship L1+L2+L3 together (they compose in `PartitionBuffer` + the walker), measure, then
   decide whether L4/L5 are worth their complexity.
