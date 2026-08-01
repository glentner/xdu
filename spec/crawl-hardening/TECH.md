---
slug: crawl-hardening
title: "Harden & optimize the index-build crawl"
kind: refactor
appetite: big
status: in_progress
branch: feature/crawl-hardening
base: main
current_phase: P1
last_updated: "2026-07-31"
phases:
  - id: P1
    name: "Extract lib::crawl + replace the fake crawler tests with a real-binary suite (behavior-preserving)"
    status: pending
    satisfies: [R1, R6, R10]
    depends_on: []
    parallel: false
    hammerable: false
    hill: uphill
    verify: "cargo test"
  - id: P2
    name: "Fail loud on walk/stat errors; add --allow-errors opt-in"
    status: pending
    satisfies: [R2, R3, R7]
    depends_on: [P1]
    parallel: false
    hammerable: false
    hill: uphill
    verify: "cargo test"
  - id: P3
    name: "Run-level completion marker + __root__ collision guard + cancel-on-first-error"
    status: pending
    satisfies: [R2, R3]
    depends_on: [P2]
    parallel: false
    hammerable: false
    hill: uphill
    verify: "cargo test"
  - id: P4
    name: "Benchmark harness (bench/) + committed baseline.json + HPC protocol"
    status: pending
    satisfies: [R4, R9]
    depends_on: [P3]
    parallel: false
    hammerable: false
    hill: uphill
    verify: "sh bench/run.sh smoke && test -f bench/results/baseline.json"
  - id: P5
    name: "Perf: relocate stat into process_read_dir (L1) + direct-to-Arrow builders (L2), measured vs baseline"
    status: pending
    satisfies: [R5, R10]
    depends_on: [P4]
    parallel: false
    hammerable: false
    hill: uphill
    verify: "cargo test"
  - id: P6
    name: "Wider cleanups: lib::index_glob dedup across readers + tests/common + assessment & follow-ups"
    status: pending
    satisfies: [R8]
    depends_on: [P5]
    parallel: false
    hammerable: true
    hill: uphill
    verify: "cargo test && .agents/factory/bin/temp_index.sh sh -c 'xdu-find -i \"$XDU_INDEX\" --count'"
review:
  last_reviewed_commit: ""
  verdict: none
  blocked_reason: ""
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

- [ ] Add `pub mod crawl;` to `src/lib.rs` (new file `src/crawl.rs` or inline module — keep readers
      from pulling it in). Move, **unchanged in behavior**: `PartitionBuffer` (whole:
      `add`/`flush`/`finalize`, records→Arrow→Parquet + rename+prune), `WorkItem`, `CrawlStats`.
- [ ] Extract `build_work_queue(entries, top_dir, filter) -> Result<VecDeque<WorkItem>>` — pure
      decision/ordering: classify dir vs loose file, apply `--partition` filter, sort partitions asc,
      push `__root__` first iff a loose top-level **regular file** exists, empty-check→`bail`. The
      `fs::read_dir` I/O stays in the bin and feeds `(name, is_dir, is_file)` tuples in. **Preserve the
      current `is_file()||is_symlink()` root trigger here for now** (P3 fixes the symlink quirk) — this
      phase changes no behavior.
- [ ] Extract `record_from_metadata(path, &Metadata, SizeMode) -> FileRecord` and chunk-name helpers
      `chunk_partial_name(id)`/`chunk_final_name(id)`. Keep the thin `MetadataExt` read in the caller.
- [ ] Reduce `crawl()` in `src/bin/xdu.rs` to an orchestrator that calls the extracted units; **leave
      the pool build, `Mutex<VecDeque>` queue, `thread::scope` spawn/join + first-error propagation,
      and all progress-bar/speed churn byte-identical** (§7). Add declarative comments stating the
      concurrency contract (single shared pool; drivers pull partitions; work-stealing; scope
      propagates first error) — no `R#`/`P#` ids.
- [ ] Unit tests in `lib::crawl` `#[cfg(test)]`: `finalize` against a `tempfile` dir (N `.partial`→N
      `.parquet`; stale `NNNNNN+` tail pruned; prune stops at first gap; `pruned` count exact);
      `build_work_queue` (`__root__` first; partitions sorted; filter excludes; empty→err; `__root__`
      `max_depth==Some(1)`, partitions `None`); `record_from_metadata` (disk-usage vs `--apparent-size`
      vs block-rounded); `CrawlStats` fold.
- [ ] **Delete `tests/crawl_tests.rs`**; add `tests/common/mod.rs` (shared `binary_path` /
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

- [ ] Replace the two silent `Err(_) => continue` sites (jwalk iterator ~355-359; `fs::metadata`
      ~365-368) with an error classifier: **benign vanished-file race** (`ErrorKind::NotFound` between
      walk and stat) → count as "vanished" + skip + does **not** fail the run; **hard error**
      (`PermissionDenied`/IO/other, incl. a jwalk directory-read `Err` that hides a whole subtree) →
      count, report `path: errno` to **stderr**, and make the run exit **non-zero**.
- [ ] Track per-partition and global skip/error counts (thread the counts through `CrawlStats` /
      atomics); surface them in the summary line (stderr) and set the process exit code from whether any
      hard error occurred.
- [ ] Add `--allow-errors` (bool) to `XduArgs` in `src/cli.rs`: when set, hard errors are downgraded to
      warn-and-continue (exit 0). Default (unset) = fail loud. Document in `doc/xdu.1.scd` (new OPTION +
      a note that the crawl fails non-zero on unreadable regions unless this is passed).
- [ ] Keep all diagnostics on **stderr** (clean pipeable stdout, §13).
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

- [ ] **Completion marker:** at crawl start, remove any existing `<index>/.xdu-complete`; after **all**
      drivers return `Ok` (and only then), write it (contents: `xdu` version + run summary). Readers
      glob `*/*.parquet`, so a top-level dotfile is never a partition — confirm no reader globs it.
      Declarative comment on why (run-level completeness can't be expressed by per-file finalize, §2).
- [ ] **`__root__` collision guard** in `build_work_queue`: if a real top-level subdir is literally
      named `__root__` (the `ROOT_PARTITION` reserved name), return a clear error; assert no two
      `WorkItem`s share a partition name.
- [ ] **Minor correctness:** gate the root trigger on `is_file()` only (drop `is_symlink()`, #7) so a
      symlink-only root no longer spawns an empty `__root__`; add an `AtomicBool` cancel flag checked at
      the driver loop top so a first hard error stops other drivers enlarging the on-disk partial index
      (#10); comment the `thread::scope` join-`Err` "panicked" arm as reachable only under
      `unwind`/test builds (`panic="abort"` aborts release, #6).
- [ ] **Non-UTF-8:** count + report (stderr) files whose path underwent lossy UTF-8 conversion so an
      operator knows those rows won't round-trip to `xdu-rm`; note in `doc/xdu.1.scd` that a true fix
      needs a schema change (out of scope — follow-up). *(No behavior change beyond the warning.)*
- **Verify:** `cargo test` — cases: `__root__`-named subdir + loose file → clear non-zero error (also
      at `-j 1`); a clean run → `.xdu-complete` present and **no `*.partial`**; a forced mid-run driver
      error (e.g. an unwritable partition target, or fill a small tmpfs) → **marker absent** + non-zero
      exit; symlink-only root → no empty `__root__` partition.
- **Touches:** `src/bin/xdu.rs`, `src/lib.rs` (`crawl` guard + marker helpers), `doc/xdu.1.scd`,
  `tests/crawl_tests.rs`.

## Phase P4 — Benchmark harness + baseline + HPC protocol
**Satisfies:** R4, R9 · **Depends on:** P3
**Goal:** A reproducible synthetic baseline (taken on the post-correctness code, the reference for
P5's measured-win gate) and a written HPC protocol ([research/03](research/03-benchmark-design.md)).

- [ ] New top-level `bench/`: `gen_tree.py` (sparse-file generator — `os.ftruncate`, ~0 disk cost, full
      stat cost), `scenarios.md` (S1 deep-narrow, S2 flat-wide, S3 many-parts, S4 skewed, S5 mixed with
      a `--scale` knob), `run.sh` (the measurement runner: builds release `xdu` if absent, runs a
      scenario, emits one JSON row — wall from xdu's own `Completed…in T.TTs`, files/sec, peak RSS via
      `/usr/bin/time`, and `strace -c` stat counts where available; a `smoke` arg runs the smallest
      scenario in a throwaway dir and asserts it executes).
- [ ] Capture and **commit `bench/results/baseline.json`** (records git commit, CPU/RAM/FS/kernel,
      tree params, `-j`/`-B`); git-ignore `bench/results/*.log`.
- [ ] `bench/HPC-PROTOCOL.md` (R9): purpose; tree-characteristic inputs to report; environment fields
      (Lustre stripe/OSTs/MDS, GPFS block/NSD/metanode, ZFS recordsize/ARC/vdev; cores/RAM/kernel);
      cache handling (MDS/ARC is the warm/cold factor; cold via freshly-written/never-read tree);
      `-j` sweep to metadata-server saturation; metrics incl. FS-side md-op rate; expected saturation
      shape (single-MDS Lustre ceiling; coordinate a billion-file stat storm); reporting template.
- [ ] Reference `bench/` from `AGENTS.md` "Testing"; add `bench/results/*.log` to `.gitignore`.
- **Verify:** `sh bench/run.sh smoke && test -f bench/results/baseline.json`.
- **Touches:** `bench/` (new), `.gitignore`, `AGENTS.md`.

## Phase P5 — Perf: stat-in-pool (L1) + direct-to-Arrow (L2), measured vs baseline
**Satisfies:** R5, R10 · **Depends on:** P4
**Goal:** Apply the highest-leverage, behavior-preserving levers and prove them against the baseline;
document the remaining ceiling ([research/02](research/02-jwalk-perf.md)).

- [ ] **L1:** `WalkDir` → `WalkDirGeneric<C>` with `DirEntryState = Option<(i64 size, i64 atime)>`;
      compute `blocks()*512`/`len()`/`atime()` (via `e.metadata()` = `symlink_metadata`, **does not
      follow links** → §8 preserved, closes the current `fs::metadata` TOCTOU) in a `process_read_dir`
      callback (runs in the shared pool). Driver reads the pre-computed state; **no driver stat**. Route
      a stat error into P2's counted-skip path (store `None`). **Do not** remove dir entries from
      `children` (jwalk needs them to recurse). `busy_timeout: None` stays; `-j` default stays 4.
- [ ] **L2:** `PartitionBuffer` appends straight into pre-sized `StringBuilder` /`Int64Builder` as
      records arrive; `flush()` just `finish()`es; drop the `Vec<FileRecord>` intermediate. No schema
      change (`get_schema()` untouched); `FileRecord` stays public.
- [ ] Run `bench/run.sh` on the L1+L2 build **and** on the pre-P5 commit (via `git worktree`); record
      the comparison under `bench/results/`. Keep a lever only if it shows no regression / a measured
      win; if warm-cache local NVMe hides L1's gain, note it and rely on the HPC protocol (R9).
- [ ] Document the remaining ceiling (metadata-server-bound; jwalk parallelizes per-directory so a
      single flat billion-file dir stays single-threaded) in `bench/scenarios.md` or a short note; list
      L3/L4/L5/L6 as evaluated-and-deferred with the reason.
- **Verify:** `cargo test` — the full `crawl_tests` suite must stay green (counts, determinism, size
      modes, symlink exclusion **unchanged** — proves L1/L2 preserved semantics). Perf comparison
      recorded in `bench/results/` (checked at review).
- **Touches:** `src/lib.rs` (`crawl`: `PartitionBuffer`, walker state), `src/bin/xdu.rs` (walker
      construction + driver read path), `bench/results/`.

## Phase P6 — Wider cleanups + assessment (bounded, R8)
**Satisfies:** R8 · **Depends on:** P5
**Goal:** The bounded, low-risk wider-codebase pass + the recorded follow-ups
([research/04](research/04-architecture.md) (d)). **Hammerable** — trim to the assessment doc if
appetite runs short.

- [ ] **Do-now:** extract `lib::index_glob(index, partition) -> String` and replace the duplicated
      `read_parquet` glob sites in `xdu-find`, `xdu-rm`, and the inline ones in `xdu-view` — pure,
      behavior-identical, unit-tested; this also creates the single seam for the future §5 escaping.
- [ ] **Do-now (reader marker awareness):** a shared helper that emits a **soft stderr warning** when a
      queried index lacks `.xdu-complete`; wire into the readers. **Not** a hard failure (backward
      compatibility with pre-existing markerless indexes).
- [ ] Record the follow-ups (do **not** implement here) in `ROADMAP.md` and `spec/crawl-hardening/META.md`
      / a short assessment note: centralize the DuckDB injection surface (§5) on `index_glob`; reconcile
      `xdu-view::format_file_count` vs `lib::format_count`; lift TUI `strip_ansi`/file-sniff helpers to
      `lib` for testability (§11/§12).
- [ ] Confirm nothing here touched `get_schema`/`FileRecord`/reader column lists (§1) or CLI semantics
      (§10).
- **Verify:** `cargo test && .agents/factory/bin/temp_index.sh sh -c 'xdu-find -i "$XDU_INDEX" --count'`
      (readers still work after the `index_glob` refactor). Optional: drive `xdu-rm -n` and `xdu-view`
      startup against the temp index.
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
