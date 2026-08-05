---
slug: crawl-hardening
title: Harden & optimize the index-build crawl
kind: refactor
appetite: big
status: in_review
branch: feature/crawl-hardening
base: main
current_phase: done
last_updated: '2026-08-04'
phases:
- id: P1
  name: Extract lib::crawl + replace the fake crawler tests with a real-binary suite
    (behavior-preserving)
  status: done
  satisfies:
  - R1
  - R6
  - R10
  depends_on: []
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test
- id: P2
  name: Fail loud on walk/stat errors; add --allow-errors opt-in
  status: done
  satisfies:
  - R2
  - R3
  - R7
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test
- id: P3
  name: Run-level completion marker + __root__ collision guard + cancel-on-first-error
  status: done
  satisfies:
  - R2
  - R3
  depends_on:
  - P2
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test
- id: P4
  name: Benchmark harness (bench/) + committed baseline.json + HPC protocol
  status: done
  satisfies:
  - R4
  - R9
  depends_on:
  - P3
  parallel: false
  hammerable: false
  hill: uphill
  verify: sh bench/run.sh smoke && test -f bench/results/baseline.json
- id: P5
  name: 'Perf: relocate stat into process_read_dir (L1) + direct-to-Arrow builders
    (L2), measured vs baseline'
  status: done
  satisfies:
  - R5
  - R10
  depends_on:
  - P4
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test
- id: P6
  name: 'Wider cleanups: lib::index_glob dedup across readers + tests/common + assessment
    & follow-ups'
  status: done
  satisfies:
  - R8
  depends_on:
  - P5
  parallel: false
  hammerable: true
  hill: uphill
  verify: cargo test && .agents/factory/bin/temp_index.sh sh -c 'xdu-find -i "$XDU_INDEX"
    --count'
review:
  last_reviewed_commit: ''
  verdict: none
  blocked_reason: ''
  cycle: 0
---
# TECH.md — Harden & optimize the index-build crawl

The **context engine and finite-state machine** for building this work. The YAML frontmatter above is
the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/crawl-hardening/TECH.md`); the
per-phase checklists below are the work.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R1–R10 are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).
- **Backing research:** [`research/00-digest.md`](research/00-digest.md) + briefs
  [`01`](research/01-concurrency-audit.md)/[`02`](research/02-jwalk-perf.md)/[`03`](research/03-benchmark-design.md)/[`04`](research/04-architecture.md).

## Conventions (apply to every phase)

- Commit conventions, code style, and load-bearing invariants come from [`AGENTS.md`](../../AGENTS.md);
  the curated footgun checklist is [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md)
  (§1 schema, §2 finalize, §3 partitions, §6 Unix, §7 shared-pool, §8 symlinks, §11 testability,
  §13 conventions are the ones in play).
- One phase per `xdu-build` invocation by default; one atomic commit containing **both** the code and
  the `TECH.md` state change. Subject: `[refactor] Build crawl-hardening P<n>: …`.
- **No `Co-Authored-By` trailer.** A CLI change (P2's `--allow-errors`) updates `doc/xdu.1.scd` **in
  the same commit**; completions regenerate from `src/cli.rs`.
- **No `spec/` `R#`/`P#` ids in source comments** (they restart per feature); comments state the *why*.
- Every phase's verify also runs the gate: `cargo fmt --all -- --check` and
  `cargo clippy --all-targets --all-features -- -D warnings` clean (R10).

---

## Phase P1 — Extract `lib::crawl`; replace the fake crawler tests
**Satisfies:** R1, R6, R10 · **Depends on:** —
**Goal:** Lift the crawl hot path into a testable `lib::crawl` module and replace the worthless
`tests/crawl_tests.rs` (it reimplements the walker and never calls production code) with a real-binary
suite — **behavior byte-identical**. This is the safety net every later phase relies on. R1's audit
artifact is [`research/01-concurrency-audit.md`](research/01-concurrency-audit.md) (produced during
planning); this phase acts on its "extract testable seams" recommendation.

- [x] Add `pub mod crawl;` to `src/lib.rs` (new file `src/crawl.rs` or inline module — keep readers
      from pulling it in). Move, **unchanged in behavior**: `PartitionBuffer` (whole:
      `add`/`flush`/`finalize`, records→Arrow→Parquet + rename+prune), `WorkItem`, `CrawlStats`.
- [x] Extract `build_work_queue(entries, top_dir, filter) -> Result<VecDeque<WorkItem>>` — pure
      decision/ordering: classify dir vs loose file, apply `--partition` filter, sort partitions asc,
      push `__root__` first iff a loose top-level **regular file** exists, empty-check→`bail`. The
      `fs::read_dir` I/O stays in the bin and feeds classified `TopEntry` values in. **Preserve the
      current `is_file()||is_symlink()` root trigger here for now** (P3 fixes the symlink quirk) — this
      phase changes no behavior. *(Amendment: the input carries a `TopEntry {path, name, is_dir,
      is_file, is_symlink}` struct rather than a `(name, is_dir, is_file)` tuple — `path` preserves the
      real (possibly non-UTF-8) directory bytes and `is_symlink` preserves the exact legacy root
      trigger, both required to change no behavior.)*
- [x] Extract `record_from_metadata(path, &Metadata, SizeMode) -> FileRecord` and chunk-name helpers
      `chunk_partial_name(id)`/`chunk_final_name(id)`. Keep the thin `MetadataExt` read in the caller.
- [x] Reduce `crawl()` in `src/bin/xdu.rs` to an orchestrator that calls the extracted units; **leave
      the pool build, `Mutex<VecDeque>` queue, `thread::scope` spawn/join + first-error propagation,
      and all progress-bar/speed churn byte-identical** (§7). Add declarative comments stating the
      concurrency contract (single shared pool; drivers pull partitions; work-stealing; scope
      propagates first error) — no `R#`/`P#` ids.
- [x] Unit tests in `lib::crawl` `#[cfg(test)]`: `finalize` against a `tempfile` dir (N `.partial`→N
      `.parquet`; stale `NNNNNN+` tail pruned; prune stops at first gap; `pruned` count exact);
      `build_work_queue` (`__root__` first; partitions sorted; filter excludes; empty→err; `__root__`
      `max_depth==Some(1)`, partitions `None`); `record_from_metadata` (disk-usage vs `--apparent-size`
      vs block-rounded); `CrawlStats` fold.
- [x] **Delete `tests/crawl_tests.rs`**; add `tests/common/mod.rs` (shared `binary_path` /
      `build_index` / `create_test_file`, mirroring `rm_tests.rs`) and `tests/crawl_tests.rs` rewritten
      to drive the **real `xdu` binary** (`std::process::Command` + `tempfile`), asserting via
      `xdu-find --count`: basic counts + per-partition; `__root__` (loose file counted, nested not);
      nested depth; `--partition` filter + absent-partition validation error; size modes; empty tree
      (non-zero + "No partitions found"); re-index prune (smaller re-index drops rows); **no `*.partial`
      left** after success; symlink exclusion (file + symlink → count 1); determinism (two runs equal);
      buffsize chunking (`-B 2` over 5 files → multiple chunks, count 5).
- **Verify:** `cargo test` (lib unit tests + the new real-binary `crawl_tests`). Optional drive:
  `.agents/factory/bin/temp_index.sh xdu-find --count`.
- **Touches:** `src/lib.rs` (+ `src/crawl.rs`), `src/bin/xdu.rs`, `tests/crawl_tests.rs`,
  `tests/common/mod.rs`.

## Phase P2 — Fail loud on walk/stat errors; `--allow-errors` opt-in
**Satisfies:** R2, R3, R7 · **Depends on:** P1
**Goal:** The headline correctness fix — an unreadable subtree must no longer silently vanish while the
run exits 0 ([research/01](research/01-concurrency-audit.md) #1, #4).

- [x] Replace the silent `Err(_) => continue` sites with an error classifier
      (`crawl::classify_io_error(Option<ErrorKind>) -> EntryError`): **benign vanished-file race**
      (`ErrorKind::NotFound` between walk and stat) → count as "vanished" + skip + does **not** fail the
      run; **hard error** (`PermissionDenied`/IO/other) → count, report `path: errno` to **stderr**, and
      make the run exit **non-zero**. *(Amendment: the audit/plan named **two** sites, but jwalk 0.8 does
      NOT surface a failed directory read as an iterator `Err` — it attaches it to the parent entry's
      `read_children_error` and yields the directory as `Ok`. So there are **three** sites: the iterator
      `Err` (per-entry construction failures), `entry.read_children_error` (the load-bearing
      whole-subtree loss, finding #1), and the `fs::metadata` re-stat. Verified against jwalk-0.8.1
      `dir_entry_iter.rs`.)*
- [x] Track per-partition and global skip/error counts (global `AtomicU64`s folded into `CrawlStats`
      `vanished`/`errors`); surface them in the per-partition `Finished` line and the final `Completed`
      summary (stderr); `main` exits non-zero when `errors > 0` unless `--allow-errors`.
- [x] Add `--allow-errors` (bool) to `XduArgs` in `src/cli.rs`: when set, hard errors are downgraded to
      warn-and-continue (exit 0). Default (unset) = fail loud. Documented in `doc/xdu.1.scd` (new OPTION,
      SYNOPSIS entry, and an EXIT STATUS section).
- [x] Keep all diagnostics on **stderr** (clean pipeable stdout, §13) — TTY runs route them through
      `MultiProgress::println` (above the bars); non-TTY runs `eprintln!`.
- **Verify:** `cargo test` — new `crawl_tests` cases: build a tree with an unreadable subtree
      (`std::fs::set_permissions` 0o000 on a subdir, skip if running as root), crawl → assert **non-zero
      exit** + stderr names the dir + index omits only that subtree; with `--allow-errors` → exit 0,
      reachable files indexed, reported skip count > 0; restore perms in teardown. A benign-race case
      (best-effort) → exit 0. Drive:
      `.agents/factory/bin/temp_index.sh sh -c 'xdu "$PWD/tree" -o /tmp/x --allow-errors; echo exit=$?'`.
- **Touches:** `src/bin/xdu.rs`, `src/lib.rs` (`crawl` error/stat plumbing + `CrawlStats`),
  `src/cli.rs`, `doc/xdu.1.scd`, `tests/crawl_tests.rs`.

## Phase P3 — Completion marker + `__root__` collision guard + cancel-on-first-error
**Satisfies:** R2, R3 · **Depends on:** P2
**Goal:** A partial/failed run is never presentable as complete, and the `__root__` collision can no
longer silently corrupt ([research/01](research/01-concurrency-audit.md) #2, #3, #7, #10).

- [x] **Completion marker:** at crawl start, remove any existing `<index>/.xdu-complete`; after **all**
      drivers return `Ok` (and only then), write it (contents: `xdu` version + run summary). Readers
      glob `*/*.parquet`, so a top-level dotfile is never a partition — confirm no reader globs it.
      Declarative comment on why (run-level completeness can't be expressed by per-file finalize, §2).
      *(Amendment: the write condition is **tightened** to the run's whole success path — all drivers
      `Ok` **and** the `--allow-errors` gate passed — so a default-policy run that hits a hard error
      leaves the index unmarked rather than marked-but-incomplete. Verified no reader enumerates the
      index root: `xdu-find`/`xdu-rm` build one `*/*.parquet` glob and `xdu-view` globs
      `*/*.parquet` or `<partition>/*.parquet`; none `read_dir`s it.)*
- [x] **`__root__` collision guard** in `build_work_queue`: if a real top-level subdir is literally
      named `__root__` (the `ROOT_PARTITION` reserved name), return a clear error; assert no two
      `WorkItem`s share a partition name. *(The duplicate-name check is a real guard, not an assert:
      two distinct non-UTF-8 directory names can collapse onto one lossy partition key — audit #5.)*
- [x] **Minor correctness:** gate the root trigger on `is_file()` only (drop `is_symlink()`, #7) so a
      symlink-only root no longer spawns an empty `__root__`; add an `AtomicBool` cancel flag checked at
      the driver loop top so a first hard error stops other drivers enlarging the on-disk partial index
      (#10); comment the `thread::scope` join-`Err` "panicked" arm as reachable only under
      `unwind`/test builds (`panic="abort"` aborts release, #6). *(The join-`Err` comment already
      landed with the P1 extraction; left as-is. The cancel flag fires on a driver `Err` only — a
      counted hard **entry** error is not a driver failure, and is still surveyed to the end so the
      operator gets the full list of unreadable paths in one run. The check sits at the partition-pull
      loop top, so a partition is always either fully crawled+finalized or never started.)*
- [x] **Non-UTF-8:** count + report (stderr) files whose path underwent lossy UTF-8 conversion so an
      operator knows those rows won't round-trip to `xdu-rm`; note in `doc/xdu.1.scd` that a true fix
      needs a schema change (out of scope — follow-up). *(No behavior change beyond the warning.)*
      *(`record_from_metadata` now returns `(FileRecord, bool)` — it owns the conversion, so it is the
      honest place to report lossiness; the driver reports the first per partition and counts the rest
      so a mojibake tree can't bury the hard errors.)*
- **Verify:** `cargo test` — cases: `__root__`-named subdir + loose file → clear non-zero error (also
      at `-j 1`); a clean run → `.xdu-complete` present and **no `*.partial`**; a forced mid-run driver
      error (e.g. an unwritable partition target, or fill a small tmpfs) → **marker absent** + non-zero
      exit; symlink-only root → no empty `__root__` partition.
      *(Result: green — 59 lib + 17 crawl + 16 rm tests, fmt/clippy clean. All four cases also driven
      through the release binaries: clean run → marker holding `files=4`, readers unchanged (4/2/1);
      sabotaged partition dir → exit 1, marker gone even though a prior run had left one; `__root__`
      collision → exit 1 with the naming diagnostic and an empty index dir. The non-UTF-8 integration
      test **skips on APFS/HFS+**, which reject such filenames — it runs on the Linux CI leg; the
      classification itself is unit-tested filesystem-independently.)*
- **Touches:** `src/bin/xdu.rs`, `src/lib.rs` (`crawl` guard + marker helpers), `doc/xdu.1.scd`,
  `tests/crawl_tests.rs`.

## Phase P4 — Benchmark harness + baseline + HPC protocol
**Satisfies:** R4, R9 · **Depends on:** P3
**Goal:** A reproducible synthetic baseline (taken on the post-correctness code, the reference for
P5's measured-win gate) and a written HPC protocol ([research/03](research/03-benchmark-design.md)).

- [x] New top-level `bench/`: `gen_tree.py` (sparse-file generator — `os.ftruncate`, ~0 disk cost, full
      stat cost), `scenarios.md` (S1 deep-narrow, S2 flat-wide, S3 many-parts, S4 skewed, S5 mixed with
      a `--scale` knob), `run.sh` (the measurement runner: builds release `xdu` if absent, runs a
      scenario, emits one JSON row — wall from xdu's own `Completed…in T.TTs`, files/sec, peak RSS via
      `/usr/bin/time`, and `strace -c` stat counts where available; a `smoke` arg runs the smallest
      scenario in a throwaway dir and asserts it executes).
      *(Amendments: (a) the research sketch's generator had a real defect — its `d{lvl}_{di%4}` path
      collapsed `dirs_per_part` onto 4 leaves, so files overwrote each other and the printed count was
      wrong; the leaf path now encodes the directory index in base-`branch` across the levels, giving a
      genuine tree of the requested depth with distinct leaves. (b) Base params are sized so `--scale 1`
      is a dev-box run and the shape is preserved as `--scale` grows (s2 base is 200k, not 1M).
      (c) `strace` profiling is **opt-in** (`--syscalls`) rather than automatic: it costs 10–50× and is
      Linux-only, so the JSON records an explicit null-with-reason elsewhere. (d) The runner emits one
      JSON **document** per invocation (env + N runs) rather than a bare row, which is exactly the
      `baseline.json` shape and lets one invocation sweep `-j` over a single generated tree.
      (e) Beyond the checklist: every configuration is checked with `xdu-find --count` and the run
      **fails** if the index lost files — a crawl that is fast because it dropped rows is not faster.)*
- [x] Capture and **commit `bench/results/baseline.json`** (records git commit, CPU/RAM/FS/kernel,
      tree params, `-j`/`-B`); git-ignore `bench/results/*.log`.
      *(Amendment: the ignore is the superset `bench/results/*` + `!bench/results/baseline.json`, so an
      ad-hoc local run cannot dirty the tree and block the next `xdu-build` pre-flight. Verified with
      `git check-ignore`. The baseline records `git_dirty: true` — honest, since `bench/` was still
      uncommitted at capture — and the document carries an automatic note that the measured binary is
      the build at the recorded commit.)*
- [x] `bench/HPC-PROTOCOL.md` (R9): purpose; tree-characteristic inputs to report; environment fields
      (Lustre stripe/OSTs/MDS, GPFS block/NSD/metanode, ZFS recordsize/ARC/vdev; cores/RAM/kernel);
      cache handling (MDS/ARC is the warm/cold factor; cold via freshly-written/never-read tree);
      `-j` sweep to metadata-server saturation; metrics incl. FS-side md-op rate; expected saturation
      shape (single-MDS Lustre ceiling; coordinate a billion-file stat storm); reporting template.
- [x] Reference `bench/` from `AGENTS.md` "Testing"; add `bench/results/*.log` to `.gitignore`.
      *(Also corrected two now-stale claims in that same section, per the constitution's "the code is
      ground truth — fix this file" rule: `crawl_tests.rs` no longer reimplements the crawler (P1), and
      the section now warns that a self-skipping test still prints `ok`.)*
- **Verify:** `sh bench/run.sh smoke && test -f bench/results/baseline.json`.
      *(Result: green. `smoke` asserts a real post-condition — 104 generated == 104 indexed, completion
      marker present — not merely exit 0. Baseline captured over ~5 min: s5 @ scale 8 (819,216 files)
      swept `-j 1/2/4/8` → 140K/228K/329K/314K files-per-sec, i.e. it **saturates near `-j 4` and
      regresses at 8**; s2 flat-wide @ 400k → 126K files/sec and the highest RSS (141 MiB), confirming a
      single flat directory cannot be split across threads; s3 @ 400k over 1000 partitions → 313K
      files/sec at 12 MiB. `indexed == generated` on every configuration. Rust gate re-run clean
      (fmt/clippy/59+17+16 tests).)*
- **Touches:** `bench/` (new), `.gitignore`, `AGENTS.md`.

## Phase P5 — Perf: stat-in-pool (L1) + direct-to-Arrow (L2), measured vs baseline
**Satisfies:** R5, R10 · **Depends on:** P4
**Goal:** Apply the highest-leverage, behavior-preserving levers and prove them against the baseline;
document the remaining ceiling ([research/02](research/02-jwalk-perf.md)).

- [x] **L1:** `WalkDir` → `WalkDirGeneric<C>` with `DirEntryState = Option<(i64 size, i64 atime)>`;
      compute `blocks()*512`/`len()`/`atime()` (via `e.metadata()` = `symlink_metadata`, **does not
      follow links** → §8 preserved, closes the current `fs::metadata` TOCTOU) in a `process_read_dir`
      callback (runs in the shared pool). Driver reads the pre-computed state; **no driver stat**. Route
      a stat error into P2's counted-skip path (store `None`). **Do not** remove dir entries from
      `children` (jwalk needs them to recurse). `busy_timeout: None` stays; `-j` default stays 4.
      *(**Implemented, measured, and REVERTED** — this is R5's "a change that does not measurably help
      SHALL NOT be merged" clause firing. Built exactly as specified (`CrawlState`/`EntryStat` carrying
      `Measured`/`Failed(ErrorKind)` so P2's error accounting was preserved), then benchmarked: **-54.8%
      on s3**, the 1000-partition shape that is precisely the per-user HPC layout xdu exists for; +50%
      on s2 flat-wide; noise elsewhere. Evidence: `bench/results/comparison-l1-l2.json`.
      **[research/02](research/02-jwalk-perf.md)'s premise for its highest-leverage lever is wrong:** it
      says stat "is serialized on the driver", but stats were already parallel *across* the `--jobs`
      driver threads — serial only within one partition. Moving them into the pool does not add stat
      concurrency, it relocates it from C drivers to N pool threads (and C == N == jobs), leaving the
      drivers idle: total metadata concurrency drops from ~C+N to ~N. The s2 gain was pipelining
      (driver encodes while pool stats), which is L4's lever, not L1's. Kept from this item: the
      `symlink_metadata` TOCTOU fix, now via `entry.metadata()` on the driver.)*
- [x] **L2:** `PartitionBuffer` appends straight into pre-sized `StringBuilder` /`Int64Builder` as
      records arrive; `flush()` just `finish()`es; drop the `Vec<FileRecord>` intermediate. No schema
      change (`get_schema()` untouched); `FileRecord` stays public.
      *(Amendments: (a) `add` takes `(&str, i64, i64)` instead of a `FileRecord`, so a valid UTF-8 path
      is borrowed from the walker straight into the column and its bytes are copied **once** — passing a
      `FileRecord` would have kept the per-row `String` allocation the lever exists to remove.
      `record_from_metadata` is therefore replaced by `file_size_and_atime` + `lossy_path`; `FileRecord`
      stays public with its lib tests. (b) Pre-sizing is capped at 8192 rows rather than a whole
      `--buffsize`: reserving a full chunk doubled RSS on the many-small-partitions shape (13.2 → 26.6
      MiB) for partitions holding a few hundred rows; capped, that cost falls to 17.0 MiB and the
      throughput win is unchanged. (c) Added a unit test that reads chunks back through the Parquet
      reader and asserts every row survived the builder reset across chunk boundaries.)*
- [x] Run `bench/run.sh` on the L1+L2 build **and** on the pre-P5 commit (via `git worktree`); record
      the comparison under `bench/results/`. Keep a lever only if it shows no regression / a measured
      win; if warm-cache local NVMe hides L1's gain, note it and rely on the HPC protocol (R9).
      *(Three documents committed, all measured back-to-back on one machine against identical generated
      trees: `comparison-pre-p5.json` (worktree at c9630c0), `comparison-l1-l2.json` (the rejected
      variant — kept as the evidence for the rejection), `comparison-l2-only.json` (shipped). Shipped
      vs pre-P5, median of 5 reps after a discarded warm-up, judged by whether the per-rep ranges
      overlap: **s2 +38.4%, s3 +18.0%, s5 -j2 +7.0%, s5 -j4 +13.6% — real wins** (disjoint ranges);
      s5 -j1 +7.5% and s5 -j8 ±0% within noise; **no regression anywhere**. Peak RSS improved in 4 of 6
      configurations. Row counts equal generated counts on every configuration and every build.
      Amendment: `.gitignore` gained a `comparison-*.json` exception so these are committable.)*
- [x] Document the remaining ceiling (metadata-server-bound; jwalk parallelizes per-directory so a
      single flat billion-file dir stays single-threaded) in `bench/scenarios.md` or a short note; list
      L3/L4/L5/L6 as evaluated-and-deferred with the reason.
      *(New "The performance ceiling, and what was tried" section in `bench/scenarios.md`: what shipped,
      why stat-in-pool was rejected with its numbers, L3/L4/L5/L6 deferred each with a reason, and the
      structural limit. L4 (pipelined writes) is named as the honest lever for the flat-wide shape.)*
- **Verify:** `cargo test` — the full `crawl_tests` suite must stay green (counts, determinism, size
      modes, symlink exclusion **unchanged** — proves L1/L2 preserved semantics). Perf comparison
      recorded in `bench/results/` (checked at review).
      *(Result: green — 60 lib + 17 crawl + 16 rm, fmt/clippy clean. All three reader tools driven
      against a throwaway index: counts 4/2/1 and `xdu-rm --dry-run` output identical to pre-P5.)*
- **Touches:** `src/lib.rs` (`crawl`: `PartitionBuffer`, walker state), `src/bin/xdu.rs` (walker
      construction + driver read path), `bench/results/`.

## Phase P6 — Wider cleanups + assessment (bounded, R8)
**Satisfies:** R8 · **Depends on:** P5
**Goal:** The bounded, low-risk wider-codebase pass + the recorded follow-ups
([research/04](research/04-architecture.md) (d)). **Hammerable** — trim to the assessment doc if
appetite runs short.

- [x] **Do-now:** extract `lib::index_glob(index, partition) -> String` and replace the duplicated
      `read_parquet` glob sites in `xdu-find`, `xdu-rm`, and the inline ones in `xdu-view` — pure,
      behavior-identical, unit-tested; this also creates the single seam for the future §5 escaping.
      *(Eight sites: 1 in `xdu-find`, 1 in `xdu-rm`, 6 in `xdu-view`. Amendment beyond the checklist:
      `xdu-view` also carried its own `const ROOT_PARTITION = "__root__"`, a second copy of a name that
      defines the on-disk layout — exactly the drift hazard §3 exists to prevent. `ROOT_PARTITION` and
      `COMPLETION_MARKER` moved to `lib` as index-layout constants shared by writer and readers, with
      `crawl` re-exporting them so no crawl-side code changed.)*
- [x] **Do-now (reader marker awareness):** a shared helper that emits a **soft stderr warning** when a
      queried index lacks `.xdu-complete`; wire into the readers. **Not** a hard failure (backward
      compatibility with pre-existing markerless indexes).
      *(`lib::index_completion_warning` returns `Option<String>` rather than printing, so the decision
      is unit-testable and each bin owns its own output moment — `xdu-view` prints before the alternate
      screen takes over, so the message survives on the terminal after the TUI exits. `xdu-rm`'s comment
      states the deletion-specific stake: a partial index means files it will never consider.)*
- [ ] Record the follow-ups (do **not** implement here) in `ROADMAP.md` and `spec/crawl-hardening/META.md`
      / a short assessment note: centralize the DuckDB injection surface (§5) on `index_glob`; reconcile
      `xdu-view::format_file_count` vs `lib::format_count`; lift TUI `strip_ansi`/file-sniff helpers to
      `lib` for testability (§11/§12). **Added during P4:** *no binary supports `--version`* — all four
      `doc/*.scd` pages document `-V, --version` and `AGENTS.md` claims clap derives it from
      `CARGO_PKG_VERSION`, but no `XduArgs`/`XduFindArgs`/`XduViewArgs`/`XduRmArgs` `#[command(...)]`
      sets `version`, so every binary rejects the flag. A one-line-per-struct fix, but it is a CLI
      change (invariant §10) and does not belong in a benchmark commit — evaluate for this phase or a
      standalone fix branch.
      *(Amendment: recorded in [`ASSESSMENT.md`](ASSESSMENT.md) — the R8 "what was applied, what was
      deferred" artifact — plus one `ROADMAP.md` entry in that file's own register that seeds a future
      `/xdu-feature`. **Not** in `META.md`: that file's stated bar excludes one-off code issues, which
      belong in `REVIEW.md` or the roadmap, so filing code follow-ups there would contradict its
      contract. `--version` is written up as a defect to fix on its own `fix/` branch, not as a feature
      to shape.)*
- [x] Confirm nothing here touched `get_schema`/`FileRecord`/reader column lists (§1) or CLI semantics
      (§10). *(Verified against the diff: `src/cli.rs` has zero changed lines; no `get_schema`,
      `FileRecord` or `Field::new` edits; no reader `SELECT` column list touched.)*
- **Verify:** `cargo test && .agents/factory/bin/temp_index.sh sh -c 'xdu-find -i "$XDU_INDEX" --count'`
      (readers still work after the `index_glob` refactor). Optional: drive `xdu-rm -n` and `xdu-view`
      startup against the temp index.
      *(Result: green — the verify command returns **4**, the fixture's exact row count; 63 lib + 18
      crawl + 16 rm tests pass; fmt/clippy clean. Drives: `xdu-find` 4/2/1 across total/alice/`__root__`
      and `xdu-rm --dry-run` reporting 1 file, both unchanged from before the refactor. Markerless
      index drive: stdout still exactly `4` with the warning on **stderr only**, and no warning at all
      when the marker is present. `xdu-view` cannot be driven headless — `enable_raw_mode` fails with
      "Device not configured" without a tty, which is pre-existing TUI behavior, and the marker warning
      was seen to print before that.)*
- **Touches:** `src/lib.rs` (`index_glob` + reader marker helper), `src/bin/xdu-find.rs`,
  `src/bin/xdu-rm.rs`, `src/bin/xdu-view.rs`, `ROADMAP.md`, `spec/crawl-hardening/META.md`.

---

## How `xdu-build` drives this

1. `next_phase.py` prints the next actionable phase (statuses authoritative; `current_phase`
   reconciled against them).
2. Pre-flight: clean tree, on `feature/crawl-hardening`, `main` reachable.
3. Execute every `[ ]` in the phase (consult `PLAN.md` / `research/` for detail).
4. Run the phase's `verify:` command — never advance on a checkbox alone.
5. Amend this file if reality diverges (regenerate frontmatter with `set_phase.py`; note it in the
   commit body). STOP and escalate only on a **`GOAL.md` contradiction**.
6. Mark the phase `done`, advance `current_phase`, `--touch`; one `[refactor]` commit (code + docs +
   tests + state); stop and report.
