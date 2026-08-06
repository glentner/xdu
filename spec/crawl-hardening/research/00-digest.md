# 00 — Research digest (consolidated decisions)

Synthesis of the four briefs into the decisions `PLAN.md` builds on. Where briefs disagreed, the
single recommendation is stated here.

## Cross-brief contradiction resolved

- **"Double stat" premise (03 vs 02).** Brief 03 (benchmark) inherited the task's framing that the
  driver's `fs::metadata()` is a *second* stat after jwalk's own. Brief 02 read jwalk 0.8.1 source and
  refuted it: jwalk's `DirEntry` caches only `file_name`+`file_type` and never stats during the walk
  (except the `std` `DT_UNKNOWN` lstat fallback on some network FSes). **Ruling: there is ONE stat per
  file today, run serially on the driver thread.** The R5 lever is therefore *relocate + parallelize*
  the stat into the rayon pool (`process_read_dir`), **not** "eliminate a double stat." `strace -c`
  stat-count deltas still validate the change; they just won't halve.

## Correctness (the floor) — confirmed bugs, severity-ordered (from 01)

1. **CRITICAL — silent subtree loss.** `Err(_) => continue` on the jwalk iterator (`xdu.rs:355-359`)
   discards an entire unreadable subtree (EACCES/EIO/stale-NFS); jwalk emits one `Err` in place of a
   failed dir's children, so millions of files vanish while the run **exits 0**. No error counting
   exists. This is the headline R2/R3 violation.
2. **HIGH — partial index looks complete.** On a driver `Err`, other drivers still finalize their
   partitions to real `.parquet`; there is no run-level completion marker, so a reader globbing
   `*/*.parquet` cannot distinguish "3 of 10 partitions" from a complete index (`xdu.rs:456-514`).
3. **MED-HIGH — `__root__` collision.** A real top-level subdir literally named `__root__` **plus**
   loose root files creates two `WorkItem`s writing the same dir → colliding chunk ids + racing
   renames + prune-loop deletion → data loss, **even at `-j 1`** (`xdu.rs:29,220-237,143-153`).
4. **MED — `fs::metadata` error drops the file** silently (`xdu.rs:365-368`); same silent-loss class
   as #1, per-file blast radius.
5. **LATENT — non-UTF-8 path corruption** via `to_string_lossy` (`xdu.rs:376`, also partition names
   `:209`): mojibake won't round-trip to `xdu-rm`; two distinct names can collapse. True fix needs a
   bytes/schema change (**out of scope** per non-goals) → count+report + document as follow-up.
6. **LATENT — `panic="abort"` makes the join-`Err` "Driver thread panicked" arm dead code in
   release** (`xdu.rs:495-506`); comment it test-only, lean on the completion marker (#2) for safety.
7. **Minor — empty `__root__`** from a symlink-only root (`has_root_files` set on `is_symlink()`).
   Confirmed **NON-ISSUES:** `Relaxed` atomics, mutex poisoning, and `finalize()` prune *in isolation*
   (its gap-break is only reachable via the #3 collision).

## Performance levers (from 02), all measure-gated (characterize-and-justify)

- **L1 ★ highest — stat in `process_read_dir`.** Switch `WalkDir` → `WalkDirGeneric<C>` with
  `DirEntryState = Option<(size,atime)>`; compute `blocks()*512`/`len()`/`atime()` in the pool
  (parallel across dirs), driver reads pre-computed values → zero driver syscalls. `e.metadata()` is
  `symlink_metadata` (doesn't follow links) → **preserves §8 and closes the current fs::metadata
  symlink-swap TOCTOU**. Order-of-magnitude on metadata-bound HPC FS; keeps the shared pool (§7).
- **L2 — direct-to-Arrow builders.** Append into pre-sized `StringBuilder`/`Int64Builder` as entries
  arrive; drop the `Vec<FileRecord>` intermediate (one path copy instead of two). Constant-factor,
  real at billions. No schema change.
- **L3 — parallelism default.** *Constrained by change latitude:* the chosen latitude is
  "preserve default observable behavior," so we do **NOT** change the `-j` default (4). L1 makes the
  *existing* `-j` knob effective for stat scaling (pool now does stats), and the HPC protocol tells
  operators to raise `-j`. Decoupling pool-width from driver-count → **deferred/documented**.
- **L4 (pipeline writes) / L5 (disable path-column dictionary) / L6 (flat-dir `par_iter_mut`)** —
  secondary; **evaluate against the baseline, ship only if measured wins justify the complexity.**
- **Ship L1+L2 together, measure vs baseline, then decide L4/L5.** On warm-cache local NVMe the win
  is smaller than on Lustre/GPFS metadata RPC — hence the HPC handoff (R9).

## Benchmarking (from 03)

- Crawl is **metadata-bound, never content-bound** (never reads file bytes) → a **sparse-file**
  generator (`os.ftruncate`) gives full stat cost at ~0 disk cost. New top-level `bench/`:
  `gen_tree.py`, `run.sh` (emits one JSON row/run), `scenarios.md` (S1 deep-narrow, S2 flat-wide,
  S3 many-parts, S4 skewed, S5 mixed), `HPC-PROTOCOL.md` (R9), `results/` (git-ignored logs +
  committed `baseline.json`). Measure wall (xdu's own `Completed…in T.TTs`), files/sec, peak RSS,
  and — Linux only — `strace -c` stat counts. Compare vs prior commit via `git worktree` (the R5
  "no-regression/measured-win" gate). **Platform gaps:** no `strace`/`hyperfine`/cold-cache on the
  darwin dev box → syscall counting + cold-cache are Linux measurements; macOS is warm-cache
  iteration only.

## Architecture (from 04)

- New `pub mod crawl` in `lib.rs`. Extract (behavior-identical): `build_work_queue` (partition
  classify + `__root__`-first + filter + empty-check — natural home for the **#3 collision guard**),
  `PartitionBuffer` (whole; `finalize()` rename+prune is the high-value seam), `record_from_metadata`,
  chunk-name helpers, `CrawlStats` fold. `crawl()` becomes a thin orchestrator; **the pool + queue +
  `thread::scope` scaffold stays byte-identical (§7 intact).**
- **Delete `tests/crawl_tests.rs`** — it reimplements the walker with `Parallelism::Serial` and never
  calls production `crawl`/`finalize`, so green proves nothing. Replace with ~12 real-binary tests
  (mirror `rm_tests.rs`, assert via `xdu-find --count`): counts, `__root__`, filter+validation, size
  modes, empty, re-index prune, no leftover `.partial`, symlink exclusion, determinism, buffsize
  chunking. Factor shared helpers into `tests/common/mod.rs`.
- **Wider cleanups (R8) — do-now (safe, byte-identical):** `lib::index_glob()` to replace the
  `read_parquet` glob duplicated across all three readers (also the single seam for the future §5
  escaping). **Follow-up (record, don't do):** centralize the injection surface on that seam;
  reconcile `format_file_count` vs `format_count`; lift TUI `strip_ansi`/sniff helpers to `lib`.
  Nothing touches `get_schema`/`FileRecord`/CLI semantics.
