# PLAN — Harden & optimize the index-build crawl (and surrounding architecture)

> **Status:** Draft for review · **Last updated:** 2026-07-31
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). Backing detail is in
> [`research/`](research/). Every design element traces to a GOAL R-ID.

## 1. Summary

Harden the jwalk work-stealing crawl in three moves, in dependency order: **(a)** refactor the crawl
hot path out of `src/bin/xdu.rs` into a testable `lib::crawl` module and replace the *fake*
`tests/crawl_tests.rs` (which never calls production code) with real-binary tests — the safety net
everything else needs; **(b)** fix the confirmed correctness bugs the audit found — chiefly the
**silent loss of an entire unreadable subtree that still exits 0** — making the crawler fail loud and
never present a partial index as complete; **(c)** establish a reproducible synthetic benchmark +
committed baseline, then apply the measured-win performance levers (relocate the per-file `stat` into
the rayon pool; append straight into Arrow builders), and hand off an HPC benchmark protocol for
validation at a scale we cannot reproduce in-loop. The concurrency *shape* (single shared rayon pool
+ driver threads + work queue + `thread::scope`) is preserved throughout; the on-disk schema and
default observable behavior are untouched except where a fix corrects a bug.

## 2. Design

### Module boundary — new `pub mod crawl` in `src/lib.rs`

Lift the pure/near-pure crawl logic out of the bin so `tests/` and unit tests can reach it
([research/04](research/04-architecture.md)); keep it in its own module so `lib.rs` doesn't balloon
and readers don't pull it in. Extracted (behavior-identical): `build_work_queue` (partition
classification, `__root__`-first ordering, `--partition` filter, empty-check), `PartitionBuffer`
(whole — `add`/`flush`/`finalize` with the records→Arrow→Parquet + rename+prune logic),
`record_from_metadata(path, &Metadata, SizeMode) -> FileRecord`, chunk-name helpers
(`chunk_partial_name`/`chunk_final_name`), and `CrawlStats` folding. `crawl()` in `src/bin/xdu.rs`
shrinks to an **orchestrator**: build the queue (pure, tested) → build the pool + spawn drivers in
`thread::scope` (**unchanged**) → driver loop calls the extracted units → fold stats. The pool /
`Mutex<VecDeque>` queue / progress-bar / speed-window / first-error-propagation scaffold **stays in
the bin, byte-identical** (§7).

### Correctness fixes (the floor)

- **Fail loud on walk/stat errors** ([research/01](research/01-concurrency-audit.md) #1,#4). Replace
  the two silent `Err(_) => continue` sites with an error classifier: a **benign vanished-file race**
  (`ENOENT` between walk and stat, common on a live filesystem) is counted and skipped with exit 0; a
  **hard error** (EACCES/EIO/other, incl. a jwalk directory-read `Err` that hides a whole subtree) is
  counted, its path+errno reported to **stderr**, and the run exits **non-zero**. A new **opt-in**
  `--allow-errors` flag downgrades hard errors to warn-and-continue (exit 0) for operators who
  knowingly crawl trees with unreadable regions (R7). Per-partition and global skip/error counts are
  surfaced in the summary. This is a behavior change *only for the buggy path* — a previously
  silent-exit-0 data-loss case now fails loud (R3).
- **Run-level completion marker** ([research/01](research/01-concurrency-audit.md) #2). The crawler
  **removes any existing marker at the start** of a run and writes a small top-level completion marker
  (e.g. `<index>/.xdu-complete`, holding the version + UTC-ish run info) **only after all drivers
  return `Ok`**. A complete index carries the marker; a crashed/failed/partial run does not. Readers
  glob `*/*.parquet`, so a top-level dotfile is never mistaken for a partition. Reader-side awareness
  (a soft stderr warning when the marker is absent) is a small shared helper added in the wider-cleanup
  phase; readers are **not** made to hard-fail on a missing marker (backward compatibility with
  pre-existing indexes). See the deviation table for why marker-not-swap.
- **`__root__` collision guard** ([research/01](research/01-concurrency-audit.md) #3). In
  `build_work_queue`, if a real top-level subdirectory is literally named `__root__`, **error out
  clearly** (reserved-name conflict) rather than silently corrupting. Assert no two `WorkItem`s ever
  share a partition name.
- **Minor:** gate `has_root_files` on `is_file()` only (drop the `is_symlink()` trigger that spawns an
  empty `__root__`, #7); comment the `thread::scope` join-`Err` arm as reachable only under
  `unwind`/test builds since `panic="abort"` aborts release (#6); add an `AtomicBool` cancel flag
  checked at the driver loop top so a first error stops enlarging the on-disk partial index (#10).
- **Non-UTF-8 paths** (#5): **count + report** lossy conversions (so an operator knows the index has
  mojibake rows that won't round-trip to `xdu-rm`); the true fix needs a bytes/schema change and is a
  **non-goal** this pass — recorded as a follow-up.

### Performance (measure-gated, characterize-and-justify)

Establish `bench/` first (generator + runner + committed `baseline.json` taken *after* the correctness
fixes), then apply and measure ([research/02](research/02-jwalk-perf.md),
[research/03](research/03-benchmark-design.md)):

- **L1 — relocate `stat` into `process_read_dir`.** `WalkDir` → `WalkDirGeneric<C>` with
  `DirEntryState = Option<(i64,i64)>`; compute size/atime in the pool callback (`e.metadata()` =
  `symlink_metadata`, doesn't follow links → preserves §8, closes the current follow-links TOCTOU);
  the driver reads pre-computed values. Same shared pool (§7). This makes the existing `-j` knob scale
  stat throughput.
- **L2 — direct-to-Arrow.** `PartitionBuffer` appends into pre-sized `StringBuilder`/`Int64Builder`
  as records arrive; `flush()` just `finish()`es. Drops the `Vec<FileRecord>` intermediate and one
  path-string copy. No schema change.
- **Deferred/documented:** the `-j` default stays **4** (change latitude forbids a default change);
  pool/driver decoupling (L3), pipelined writes (L4), path-column dictionary-disable (L5), and
  flat-dir `par_iter_mut` (L6) are applied **only if** the baseline comparison shows they earn their
  complexity — otherwise the remaining ceiling is documented as inherent (metadata-server-bound;
  jwalk's parallelism unit is the directory, so a single billion-file flat dir stays single-threaded).

### CLI / docs surface

Only additive: `--allow-errors` (bool, opt-in) in `src/cli.rs` `XduArgs`, documented in
`doc/xdu.1.scd` in the same commit; completions regenerate from `cli.rs`. Non-TTY runs keep **stdout**
clean (all error/skip diagnostics → **stderr**). `AGENTS.md` is updated where the repo map (new
`bench/`), the CLI surface (`--allow-errors`), and the crawl invariants move.

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1 | The systematic concurrency audit, recorded + classified in [`research/01-concurrency-audit.md`](research/01-concurrency-audit.md) (real-bug/latent/non-issue per hazard); consumed by the correctness phases. |
| R2 | Confirmed bugs fixed with real-binary regression tests: silent subtree/file loss (P2), `__root__` collision + partial-index marker (P3); tests assert row counts / paths present-absent / exit codes. |
| R3 | Fail-loud error handling (P2) + run-level completion marker & cancel-on-first-error (P3): a permission/IO-unreadable region no longer exits 0, and a partial run is never marked complete. |
| R4 | `bench/` synthetic generator + runner + committed `baseline.json` with recorded env (P4). |
| R5 | L1 (stat-in-pool) + L2 (direct-to-Arrow), each merged only on a measured win vs baseline; remaining ceiling documented as jwalk/metadata-bound (P5). |
| R6 | `lib::crawl` extraction, thin `crawl()` orchestrator, declarative invariant comments; no schema/behavior change (P1). |
| R7 | `--allow-errors` opt-in flag; existing flags/defaults unchanged (P2). |
| R8 | Wider assessment + `lib::index_glob()` dedup across readers + `tests/common/mod.rs`; larger items recorded as follow-ups (P6). |
| R9 | `bench/HPC-PROTOCOL.md` (inputs, Lustre/GPFS/ZFS environment, metrics, expected saturation shape) (P4). |
| R10 | Schema (§1), atomic finalize (§2), partition scheme (§3), symlinks (§8), Unix-only (§6), clean non-TTY stdout preserved; fmt/clippy/test gate green — verified every phase (esp. P1, P5). |

## 3. Invariant gate (AGENTS.md constitution check)

Checked before research (see the chat gate #1) and again against this drafted design.

- **§1 schema stability** — no change to `get_schema()`/`FileRecord`/reader column lists; `FileRecord`
  stays public. Honored.
- **§2 atomic finalization** — `PartitionBuffer` moves *as-is*; partial→rename→prune unchanged. The
  new completion marker is **additive** (a top-level file), not a change to per-file finalize; the
  documented per-file-not-per-partition limitation stands. Honored (one deviation logged below).
- **§3 partition scheme** — `__root__` rule preserved; the collision guard *protects* the scheme
  (turns silent corruption into a clear error). Honored.
- **§6 Unix-only / §8 symlinks** — `MetadataExt` (atime, `blocks()×512`) retained; L1's `e.metadata()`
  is `symlink_metadata`, so it does **not** follow links (strictly safer than today's `fs::metadata`).
  Honored.
- **§7 shared rayon-pool concurrency** — single `RayonExistingPool`, driver threads, `Mutex<VecDeque>`
  queue, and `thread::scope` first-error propagation all preserved; L1 adds stat *work* to the pool
  callback without changing the model's shape; `-j` default and `num_drivers` derivation unchanged.
  Honored.
- **§10 CLI single source** — `--allow-errors` added in `src/cli.rs`; `doc/xdu.1.scd` updated same
  commit; bool flag (no late string validation). Honored.
- **§11 altitude/testability** — this pass *advances* it (logic → `lib::crawl`; real tests). Honored.
- **§13 conventions** — version stays `Cargo.toml`-sourced; error/skip output → stderr (clean stdout);
  `share/` untouched; `bench/` referenced from `AGENTS.md` Testing; gate green every phase. Honored.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| Add a run-level completion marker file under `<index>/` (P3) — new artifact + reader-side soft warning helper | R3 requires that a partial index not be presentable as complete; per-file finalize (§2) cannot express run-level completeness | **Temp-dir-then-atomic-swap** of the whole index rejected: `rename` is not atomic across the partition subdir tree on POSIX, doubles disk churn at HPC scale, and breaks in-place re-index/prune semantics. **Hard-requiring the marker in readers** rejected: it would break every pre-existing (markerless) index — soft-warn preserves backward compatibility. |

## 4. Rabbit holes (resolved)

- *Is the driver's `fs::metadata` a redundant second stat?* → **No** — jwalk 0.8 doesn't stat during
  the walk; it's one serial stat. The lever is to *relocate it into the pool*, not eliminate a double
  stat ([research/02](research/02-jwalk-perf.md), [research/00](research/00-digest.md)).
- *Does moving stat into `process_read_dir` risk following symlinks or changing size/atime?* → **No** —
  `e.metadata()` is `symlink_metadata`; for regular files lstat≡stat; §8/§6 preserved and a TOCTOU
  closes ([research/02](research/02-jwalk-perf.md) L1).
- *Can we benchmark billions of files in-loop?* → **No, but** a sparse-file generator gives full stat
  cost at ~0 disk cost; synthetic in-loop + an HPC protocol for real-scale validation
  ([research/03](research/03-benchmark-design.md)).
- *Is the current `tests/crawl_tests.rs` a usable safety net?* → **No** — it reimplements the walker
  and never calls production `crawl`/`finalize`; must be replaced before trusting any fix
  ([research/04](research/04-architecture.md)).
- *What is the true blast radius of the error-swallowing?* → an entire subtree per unreadable dir,
  exit 0 — the headline correctness bug ([research/01](research/01-concurrency-audit.md) #1).

## 5. Risks & open questions

- **Completion-marker reader policy (needs a call at/ before P3).** Recommended: crawler writes/removes
  the marker; readers emit a **soft stderr warning** when it's absent (backward-compatible). Confirm we
  don't want readers to hard-fail on markerless indexes (would break existing indexes).
- **`--allow-errors` default semantics.** Recommended: benign `ENOENT`-race → count+continue+exit 0
  even without the flag (live filesystems race constantly); only EACCES/EIO/other hard errors fail
  loud by default. Confirm this split is what operators want (vs. failing on *any* skipped file).
- **Perf wins may not show on the dev box.** L1's payoff is on metadata-RPC-bound HPC FS; on warm-cache
  local NVMe it can be modest. R5 is characterize-and-justify, so "documented ceiling + HPC-protocol
  handoff" is an acceptable outcome even if the in-loop delta is small — but set expectations.
- **P1 and P5 are large phases.** If either proves too big at build time, `xdu-build` may split it;
  the FSM permits amendment. P6 is the hammerable/trimmable slice if appetite runs short.
- **`process_read_dir` must exactly preserve** skip-non-files, the (now-counted) skip-on-stat-error
  path, and atime/blocks semantics — pinned by P1's tests before P5 changes the stat site.

## 6. Verification strategy

- **Every phase:** `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D
  warnings`, `cargo test` must pass (R10 gate).
- **P1 (refactor + test net):** `cargo test` — new `lib::crawl` unit tests (finalize rename/prune
  against a temp dir; `build_work_queue` ordering incl. `__root__`-first; `record_from_metadata` size
  modes) + the ~12 real-binary crawler tests that replace `crawl_tests.rs` (counts, `__root__`,
  filter+validation, size modes, empty, re-index prune, no leftover `.partial`, symlink exclusion,
  determinism, buffsize chunking).
- **P2 (fail-loud):** real-binary test — build a tree with an unreadable subtree (`chmod 000`, non-root),
  crawl → assert **non-zero exit** + a stderr diagnostic naming the dir, and that `--allow-errors`
  yields exit 0 with the reachable files indexed and a reported skip count. Restore perms in teardown.
- **P3 (marker + collision):** real-binary tests — a `__root__`-named subdir + loose file → clear
  non-zero error (also at `-j 1`); a forced mid-run driver error → **no completion marker** present and
  non-zero exit; a clean run → marker present and `*.partial` absent.
- **P4 (bench):** `bench/run.sh` smoke over the smallest scenario exits 0 and emits a JSON row;
  `bench/results/baseline.json` committed with its git commit recorded.
- **P5 (perf):** `cargo test` (semantics byte-identical) **plus** a recorded `bench/` comparison vs the
  pre-P5 commit (via `git worktree`) demonstrating no regression / a measured win; levers that don't
  earn it are dropped and the ceiling documented.
- **P6 (wider cleanups):** `cargo test` + a `temp_index.sh` drive of all three readers
  (`xdu-find`/`xdu-view` startup/`xdu-rm --dry-run`) confirming `index_glob` refactor is
  behavior-identical.
- Prefer real-binary drives in a throwaway index (`.agents/factory/bin/temp_index.sh sh -c "…"`) over
  unit tests alone, per house rule.

---

*Backing research: [`research/00-digest.md`](research/00-digest.md) +
[`01`](research/01-concurrency-audit.md)/[`02`](research/02-jwalk-perf.md)/[`03`](research/03-benchmark-design.md)/[`04`](research/04-architecture.md).*
