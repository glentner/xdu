---
slug: crawl-hardening
title: Harden & optimize the index-build crawl
kind: refactor
appetite: big
status: in_review
branch: feature/crawl-hardening
base: main
current_phase: done
last_updated: '2026-08-05'
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
- id: P7
  name: 'F1: clear the completion marker after pre-flight, not before it'
  status: done
  satisfies:
  - R2
  - R3
  depends_on:
  - P6
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test --test crawl_tests -- --nocapture && cargo fmt --all -- --check
    && cargo clippy --all-targets --all-features -- -D warnings && cargo test
- id: P8
  name: 'F3: correct the man page EXIT STATUS for both error classes'
  status: done
  satisfies:
  - R3
  - R10
  depends_on:
  - P7
  parallel: false
  hammerable: false
  hill: uphill
  verify: 'if command -v scdoc >/dev/null 2>&1; then scdoc < doc/xdu.1.scd > /dev/null
    && echo "scdoc render: ok"; else echo "scdoc render: SKIPPED (not installed; gated
    in CI test.yaml)"; fi && cargo fmt --all -- --check && cargo clippy --all-targets
    --all-features -- -D warnings && cargo test --test crawl_tests -- --nocapture'
- id: P9
  name: 'F4: readers warn when the marker records tolerated errors (with a bounded,
    non-blocking read)'
  status: done
  satisfies:
  - R3
  - R8
  depends_on:
  - P8
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo fmt --all -- --check && cargo clippy --all-targets --all-features
    -- -D warnings && cargo test --lib && cargo test --test crawl_tests -- --nocapture
    && cargo test
- id: P10
  name: 'F2: interleaved A/B mode in bench/run.sh, honest re-capture, and rewritten
    perf claims'
  status: done
  satisfies:
  - R4
  - R5
  depends_on:
  - P9
  parallel: false
  hammerable: false
  hill: uphill
  verify: sh bench/run.sh smoke && python3 -c "import json; d=json.load(open(\"bench/results/comparison-p5-ab.json\"));
    assert {r[\"variant\"] for r in d[\"runs\"]}=={\"A\",\"B\"}; assert all(r[\"indexed_files\"]==r[\"generated_files\"]
    for r in d[\"runs\"]); assert len(d[\"comparisons\"])==6; assert all(len(c[\"paired_delta_pct\"][\"samples\"])==c[\"paired_delta_pct\"][\"reps\"]
    for c in d[\"comparisons\"]); print(\"A/B ok:\", [(c[\"scenario\"], c[\"jobs\"],
    c[\"paired_delta_pct\"][\"median\"]) for c in d[\"comparisons\"]])" && grep -q
    measures_recorded_commit bench/run.sh && grep -qi "noise floor" bench/scenarios.md
    && python3 -c "import json;B={(r['scenario'],r['jobs']):r['wall_s']['median']
    for r in json.load(open('bench/results/baseline.json'))['runs']};P={(r['scenario'],r['jobs']):r['wall_s']['median']
    for r in json.load(open('bench/results/comparison-pre-p5.json'))['runs']};K=sorted(set(B)&set(P));d=[(max(B[k],P[k])-min(B[k],P[k]))/max(B[k],P[k])*100
    for k in K];c='%.1f%s%.1f%%'%(min(d),chr(8211),max(d));t=open('bench/scenarios.md').read();assert
    t.count(c)==2,'documented drift range is not the recomputed '+c+' (found %d occurrences)'%t.count(c);assert
    all(B[k]<P[k] for k in K),'baseline-faster-in-all-six claim no longer holds';print('noise-floor
    range asserted against committed data:',c)" && git diff --quiet HEAD -- src doc
    tests && cargo fmt --all -- --check && cargo clippy --all-targets --all-features
    -- -D warnings && cargo test
- id: P11
  name: Record every follow-up this cycle defers (F5 + the P9/P10 deferrals)
  status: done
  satisfies:
  - R8
  depends_on:
  - P10
  parallel: false
  hammerable: false
  hill: uphill
  verify: 'test -d issues && for f in marker-scoped-run-attestation bench-baseline-overwrite-guard
    xdu-view-terminal-safety; do test -f "issues/$f.md" || { echo "MISSING issues/$f.md";
    exit 1; }; grep -q "status: unshaped" "issues/$f.md" || { echo "MISSING status:
    unshaped in $f"; exit 1; }; grep -q "issues/$f.md" ROADMAP.md || { echo "$f not
    linked from ROADMAP.md"; exit 1; }; done && grep -qF "### Restore the terminal
    on every exit path, including panic" spec/crawl-hardening/ASSESSMENT.md && grep
    -qF "### Truncate display names on char boundaries, not byte indices" spec/crawl-hardening/ASSESSMENT.md
    && ! grep -qF "None changes what the tools do" ROADMAP.md && git diff --quiet
    HEAD -- src tests bench doc Cargo.toml Cargo.lock && cargo fmt --all -- --check
    && echo PHASE-OK'
- id: P12
  name: 'C2-F2: resync AGENTS.md + invariants.md with the code this cycle landed'
  status: done
  satisfies:
  - R10
  depends_on:
  - P11
  parallel: false
  hammerable: false
  hill: uphill
  verify: grep -q 'src/crawl.rs' AGENTS.md && grep -q -- '--allow-errors' AGENTS.md
    && grep -q '\.xdu-complete' AGENTS.md && grep -q 'ROOT_PARTITION`, `lib\.rs' AGENTS.md
    && ! grep -q 'ROOT_PARTITION`, `xdu\.rs' AGENTS.md && grep -q '\.xdu-complete'
    .agents/factory/invariants.md && grep -q -- '--allow-errors' .agents/factory/invariants.md
    && grep -qi 'collision' .agents/factory/invariants.md && for scd in doc/*.scd;
    do scdoc < "$scd" > /dev/null || exit 1; done && git diff --quiet HEAD -- src
    tests bench Cargo.toml Cargo.lock && cargo fmt --all -- --check && cargo clippy
    --all-targets --all-features -- -D warnings && cargo test && echo PHASE-OK
- id: P13
  name: 'C3-F1: reject every reserved index-root name, not just __root__ (+ record
    C3-F2)'
  status: done
  satisfies:
  - R3
  - R8
  - R10
  depends_on:
  - P12
  parallel: false
  hammerable: false
  hill: downhill
  verify: cargo test --test crawl_tests -- --nocapture && cargo test --lib && scdoc
    < doc/xdu.1.scd > /dev/null && test -f issues/orphan-partition-survives-reindex.md
    && grep -q "issues/orphan-partition-survives-reindex.md" ROADMAP.md && cargo fmt
    --all -- --check && cargo clippy --all-targets --all-features -- -D warnings &&
    cargo test
- id: P14
  name: 'C4-F1/F2/F3/F4: record the prune defect, and stop the operating manual describing
    code that is gone'
  status: done
  satisfies:
  - R2
  - R6
  - R8
  - R10
  depends_on:
  - P13
  parallel: false
  hammerable: false
  hill: downhill
  verify: 'test -f issues/unreadable-partition-prunes-prior-chunks.md && grep -q "status:
    unshaped" issues/unreadable-partition-prunes-prior-chunks.md && grep -qF "issues/unreadable-partition-prunes-prior-chunks.md"
    ROADMAP.md && scdoc < doc/xdu.1.scd | mandoc -Tutf8 | col -b | tr "\n" " " | tr
    -s " " | grep -qF "removed rather than kept" && ! grep -rn "record_from_metadata"
    src/ && ! grep -rn "FileRecord" src/ tests/ && ! grep -rn "derives" AGENTS.md
    .agents/factory/invariants.md .agents/skills/xdu-release/SKILL.md .agents/skills/xdu-build/SKILL.md
    && ! grep -rn "FileRecord" AGENTS.md .agents/factory/invariants.md .agents/skills/xdu-plan/SKILL.md
    && grep -qF "issues/version-flag-missing.md" AGENTS.md && grep -qF "issues/version-flag-missing.md"
    .agents/factory/invariants.md && cargo fmt --all -- --check && cargo clippy --all-targets
    --all-features -- -D warnings && cargo test && echo PHASE-OK'
review:
  last_reviewed_commit: 0c77ccbf1de150bb271284dc36286a0fed390407
  verdict: changes-requested
  blocked_reason: 'review cycle 4 (human gate CLEARED 2026-08-05): land one P14 with
    exactly three parts. (1) C4-F1 - RECORD, DO NOT FIX: new issues/{slug}.md at status:
    unshaped for finalize''s prune scope destroying a partition''s prior chunks when
    its directory is unreadable (pre-existing in main, proven by differential drive;
    same mechanism as the recorded C3-F2), a ROADMAP **Seed:** entry, AND one sentence
    in doc/xdu.1.scd''s --allow-errors text warning that a tolerated error can prune
    previously-indexed rows. (2) C4-F2 + C4-F3 - FIX: correct AGENTS.md:48 and .agents/factory/invariants.md:183
    (clap does NOT derive --version; point at issues/version-flag-missing.md), and
    fix the stale record_from_metadata name at src/bin/xdu.rs:39 (it is crawl::file_size_and_atime).
    (3) C4-F4 - DELETE FileRecord from src/lib.rs with its unit tests, and drop its
    mention from invariant #1''s wording in AGENTS.md + invariants.md so get_schema()
    is named as the sole schema contract. No other src/ logic change; the SQL-escaping
    and orphan-partition deferrals stand. Human decided NO cycle 5: after P14 verifies
    clean, go straight to /xdu-publish.'
  cycle: 4
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
      *(**Superseded by P10 — the two load-bearing claims above are both false.** "Measured back-to-back
      on one machine against identical generated trees" is wrong twice over: the captures are 45 min to
      6 h apart (`captured_at` 18:49 / 19:34 / 20:58 / 00:52 next day), and every invocation regenerates
      its own tree, so no two documents share one. The "judged by whether the per-rep ranges overlap"
      method is invalid across invocations: it treats within-invocation spread as the error bar while
      the real between-invocation drift is 8.9–18.5% — demonstrated by `baseline.json` vs
      `comparison-pre-p5.json`, which measure **identical crawl source** yet disagree by that much. The
      quoted wins (s2 +38.4%, s3 +18.0%, s5 -j4 +13.6%) sit inside that drift and are not established.
      `comparison-p5-ab.json` supersedes all of these numbers; see P10 for the interleaved result.)*
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

# Review cycle 1 remediation (P7–P11)

Added 2026-08-05 from [`REVIEW.md`](REVIEW.md) cycle 1 (`changes-requested`, reviewed commit
`e1f5d7e`). Each phase closes one CONFIRMED finding. Designs were produced and adversarially verified
before authoring; **F1's placement and F4's blast radius were each attacked by an independent skeptic**
and the corrections are folded into the checklists below.

**Two human decisions are already made** (2026-08-05) and are not open for re-litigation mid-build:

1. **F2 goes the full route** — fix the harness so it can resolve a real difference, re-capture, then
   write the claims from the new document.
2. **The P5 direct-to-Arrow change is KEPT.** Under interleaved A/B it shows no measured throughput
   effect on any shape; it is kept for lower peak RSS on the worst-memory shape and the
   `symlink_metadata` TOCTOU it closed. P10 rewrites the claim to say exactly that. **Do not revert
   `src/crawl.rs`'s builder path.** If P10's fresh capture shows a *regression* clearing the paired
   spread, STOP and escalate — that changes the decision.

**Ordering constraint:** P7, P8 and P9 all edit `doc/xdu.1.scd`, and P8 re-wraps EXIT STATUS (the file
grows 135 → 144 lines). **Every line number cited below is as-of `e1f5d7e`** — re-locate by content,
never by line number, in P8 onward. The review's own F3 citation was already stale by ~9 lines.

---

## Phase P7 — F1: clear the completion marker after pre-flight, not before it
**Satisfies:** R2, R3 · **Depends on:** P6
**Goal:** A run rejected *before it writes anything* must leave a previously-complete index's marker
intact. Today `clear_completion_marker(outdir)?` is the **first** statement of `crawl()`, so four
pre-flight rejection paths strip the attestation from an index they never touched — recoverable only by
a full re-index. The existing in-code fail-safe rationale is **correct and must survive**: everything
after the clear must still leave a crashed run visibly unattested.

The complete enumeration of early exits between `crawl()` entry and the first index mutation (read from
source, independently confirmed by a second pass — `build_work_queue` makes **zero** filesystem calls,
which is the load-bearing fact licensing the move):

| site | index touched? |
|---|---|
| `xdu.rs:58` `clear_completion_marker(outdir)?` | **YES — the defect** (and itself fallible) |
| `xdu.rs:61-66` `ThreadPoolBuilder…build()?` | no |
| `xdu.rs:72-73` `fs::read_dir(top_dir)?` | no |
| `xdu.rs:75` `entry?` · `:77` `entry.file_type()?` | no |
| `xdu.rs:88` `build_work_queue(...)?` → `crawl.rs:229` reserved `__root__`, `:269` "No partitions found", `:279` lossy-name collision | no (pure over `TopEntry`) |
| `xdu.rs:150` `std::thread::scope(...)` | **point of no return** — first writes are `crawl.rs:378` `create_dir_all`, `:405` `File::create(*.partial)`, `:428` `rename`, `:442` prune |

- [x] Move the single statement `clear_completion_marker(outdir)?;` **and its comment** from the top of
      `crawl()` (`xdu.rs:54-58`) to immediately after `let num_items = work_queue.len();` (`xdu.rs:89`),
      directly before the `// Progress display` block. Nothing else moves; no signature, `lib`, schema,
      CLI or reader change. Placement is legal anywhere in `(89, 150]`; line 90 is the earliest point
      after every rejection that leaves the index untouched, and keeps the clear adjacent to the
      validation that licenses it.
- [x] Reword the moved comment to state the new ordering rule declaratively (no spec ids):
      pre-flight has passed, so from here the index is being rewritten and the previous run's
      attestation no longer describes it; dropping the marker after the last check that can still
      reject the run and before any driver writes means a crash below leaves an index that is visibly
      unattested, while a run rejected without touching the index leaves an already-complete index
      still attested.
- [x] Update `crawl()`'s doc comment (`xdu.rs:42-43`): "cleared here" → "cleared once pre-flight
      passes", adding that a run rejected before crawling leaves the previous marker intact.
- [x] **Fix two stale doc comments in `src/crawl.rs` in the same commit** (comment-only, zero
      behavioral risk — the skeptic flagged that the design wrongly claimed there was no drift here):
      (a) `crawl.rs:37-38` on the `COMPLETION_MARKER` re-export ("removed when a run starts"); (b)
      `crawl.rs:46-50`, the `clear_completion_marker` doc ("at the start of a run" / "A run in progress
      must never carry the previous run's attestation"). Restate both as *removed once a run's
      pre-flight has passed and it is about to write*. (b) matters most — it is the doc a future editor
      reads when deciding where this call belongs, so leaving it stale re-arms the regression.
- [x] Adjust `doc/xdu.1.scd:105`: "removed when a run starts" → "removed once a run has passed its
      pre-flight checks and is about to write, and written only once every partition has been walked
      and finalized".
      *(Located by content, not line number, as this section instructs; the paragraph also gained the
      converse — "a run rejected before it writes leaves an existing marker intact" — since the man
      page is where an operator learns the recovery property. Re-wrapped ≤ 79 cols; the file's two
      over-79 lines both pre-date this phase.)*
- [x] Add `test_rejected_run_leaves_existing_marker_intact` to `tests/crawl_tests.rs`: build a complete
      index, assert marker present, then assert it **survives** each rejection — (1) empty source tree
      → exit 1 "No partitions found"; (2) a real top-level `__root__` dir → exit 1 "reserved";
      (3) unreadable source root (`chmod 000`, skip under root via `libc::geteuid()`, restore perms in
      teardown) → exit 1 "Failed to read directory". Assert in each case: non-zero exit, **marker still
      present**, and the pre-existing row count still queryable.
      *(Amendment: the assertion is **stronger** than "still present" — the marker body is captured
      before the rejections and compared byte-for-byte after each, so a clear-then-rewrite could not
      pass. Two further tightenings: the `__root__` leg needs no loose file (the reserved-name check
      is unconditional), and the unreadable-root leg asserts specifically "Failed to read directory",
      confirmed by drive to be the site it exists to cover — canonicalizing the root only lstats it,
      so the failure lands on the top-level enumeration, not the earlier resolve. The test closes by
      re-proving the fail-safe below the clear in the same fixture: sabotaging a partition directory
      and re-running the good tree strips the attestation.)*
- [x] **Only three of the four bail classes are testable** — the lossy-partition-name collision
      (`crawl.rs:279`) needs a non-UTF-8 directory name that APFS rejects (the existing test at
      `crawl_tests.rs:434` self-skips for exactly this reason), and a `ThreadPoolBuilder` failure is not
      injectable. Do not try to force either; the three legs above are the right coverage.
- [x] Confirm the fail-safe still holds *after* the clear: a mid-run driver failure must still leave the
      index markerless. The existing `!marker.exists()` assertions (`crawl_tests.rs:279-315` fresh index
      per `-j`; `:349-365` sabotaged partition dir, detected by `create_dir_all` **below** the new
      clear) must pass unchanged — verify, don't assume.
      *(Verified, not assumed: both `test_reserved_root_partition_name_is_rejected` and
      `test_completion_marker_written_on_success_and_cleared_on_failure` pass with their assertions
      untouched. The first is unaffected because its index is fresh per `-j`, so there was never a
      marker to preserve; the second's sabotage is detected by `create_dir_all` below the new clear.)*
- **Verify:** `cargo test --test crawl_tests -- --nocapture` (nocapture so a root self-skip is visible)
  then the full gate. Drive: build a marked index, point `xdu` at an empty tree, assert exit 1 **and**
  `.xdu-complete` still present.
  *(Result: green — 19 crawl (up from 18) + 63 lib + 16 rm tests pass, fmt/clippy clean. The new test
  ran rather than self-skipping: no root-skip line in `--nocapture` output. Drive through the release
  binaries against a throwaway marked index (`temp_index.sh`), asserting concrete post-conditions
  rather than exit codes: baseline marker holds `files=4` and `xdu-find --count` returns 4; the
  empty-tree run exits 1 with "No partitions found" and the marker is **unchanged** (body compared,
  not just present) with the count still 4; the `chmod 000` source run exits 1 with "Failed to read
  directory: …/locked" + "Permission denied (os error 13)", marker still present, count still 4.
  `scdoc` is not installed here so the man page render was **not** checked locally — it is gated in
  CI, and P8 re-runs it as part of its own verify.)*
- **Touches:** `src/bin/xdu.rs`, `src/crawl.rs` (comments only), `doc/xdu.1.scd`, `tests/crawl_tests.rs`.
- **Out of scope:** do **not** hoist the `-p` validation or `fs::create_dir_all(&args.outdir)`
  (`xdu.rs:535`) — reordering those changes pre-existing default behavior (a stray empty outdir), which
  the GOAL's non-goals forbid.

## Phase P8 — F3: correct the man page EXIT STATUS for both error classes
**Satisfies:** R3, R10 · **Depends on:** P7
**Goal:** EXIT STATUS claims "the remaining threads stop taking on new partitions once one has failed"
inside the paragraph about *permission/I-O* errors. That is false for that class: an entry-level error
increments `part_errors`, reports the path, and the crawl **continues** — every partition is walked and
finalized, and the run fails only in `main()` afterwards. The sentence describes the *write-failure*
class instead, and even there imprecisely (an in-flight partition does finish). **Doc-only — the code
is R3-compliant.** The offending sentence is at `doc/xdu.1.scd:122-123` as of `e1f5d7e`, not the
`:113-114` the review cited; locate it by content.

- [x] Replace the EXIT STATUS body (`doc/xdu.1.scd:118-130`) with four paragraphs: **class A** —
      reports the offending path to stderr, goes on to index everything else it can reach, and exits
      non-zero *at the end of the run*, so a partial index is never presented as complete (keep the
      ENOENT-race sentence here); **class B** — a Parquet chunk that cannot be written or renamed stops
      the run: the partition being written is abandoned (chunks not yet renamed stay as `.partial`,
      which readers ignore), other threads finish the partition they are on but take on no new ones,
      and still-queued partitions go unindexed; **shared** — either way the failing run leaves no
      completion marker (keep the `_OUTPUT FORMAT_` cross-reference); **`--allow-errors`** — as today,
      plus "Write failures are not downgraded."
- [x] Delete the false clause "and the remaining threads stop taking on new partitions once one has
      failed" from the class-A paragraph.
      *(Falsified by drive before deletion, not just by reading: a two-partition tree with an
      unreadable dir inside `alpha` prints `Finished alpha` **and** `Finished beta` — the sibling was
      taken on after the error — then exits 1.)*
- [x] Keep the four correct existing claims in substance: no marker on a failing run; ENOENT races
      counted and skipped without failing; `--allow-errors` downgrades and exits 0; all diagnostics on
      stderr so piped stdout stays clean.
      *(All four kept. "hard errors" became "the read errors" in the `--allow-errors` paragraph: with
      two classes now named, "hard" would read as covering write failures, which it never did.)*
- [x] Keep the file's conventions: `*bold*` for program/flags/exit codes, `_italic_` for the section
      cross-reference, literal em dashes, `.partial` written plain as at `:101`, body wrapped ≤ 79 cols.
      A paragraph starting `*xdu*` at column 0 is existing precedent and safe.
      *(One hazard this item did not anticipate: the natural wrap put `.partial` at **column 0**, and a
      leading `.` is a roff control line. No line in any `doc/*.scd` starts with a period, so there was
      no precedent to lean on and `scdoc` is not installed here to settle it — rewrapped so `.partial`
      sits mid-line instead of betting on scdoc's escaping. The file now has **one** over-79 line, the
      pre-existing 121-col SYNOPSIS; the 80-col line this phase briefly introduced is rewrapped, and
      the one it replaced is gone.)*
- [x] Add `test_unreadable_path_does_not_stop_sibling_partitions` to `tests/crawl_tests.rs`: skip under
      root, `chmod 000` a nested dir inside one partition, restore perms **before** asserting, then
      assert exit non-zero, the offending path on stderr, the sibling partition indexed, **the erroring
      partition still finalized**, zero leftover `.partial`, and no marker.
      *(All seven assertions as specified; "still finalized" is asserted twice over — `alpha` yields 1
      row **and** holds ≥ 1 `.parquet` chunk.)*
- [x] Add `test_write_failure_abandons_queued_partitions`: four partitions, `<index>/p2` pre-created as
      a regular file, `-j 1` for a deterministic drain order; assert exit non-zero, `p1` indexed, `p3`
      and `p4` absent, no marker. Note in the test's doc comment that its determinism depends on
      `build_work_queue`'s ascending sort and on `num_drivers = jobs.min(num_items).max(1)`.
      *(As specified. The failure surfaces from `PartitionBuffer::flush`'s `create_dir_all` — "File
      exists (os error 17)" — so it is a genuine write-path failure, not a pre-flight rejection.)*
- [x] Do **not** also document that partitions a failed run never reached still hold the previous run's
      chunks (true, invariant §2, but it turns three tight paragraphs into a wall). Do not soften
      "exits non-zero" into "may exit non-zero".
      *(Both honored — neither claim was softened and the stale-chunk case stays undocumented.)*
- **Verify:** render with `scdoc` when available, else record that it was skipped (not installed here;
  gated by CI `.github/workflows/test.yaml`) — then `cargo test --test crawl_tests -- --nocapture` and
  the fmt/clippy gate. Commit nothing under `share/` (generated, git-ignored).
  *(Result: green — 21 crawl (up from 19) + 63 lib + 16 rm tests pass, fmt/clippy clean, nothing under
  `share/` touched. `scdoc` was **not installed here**, so the render was NOT checked locally; CI's
  `test.yaml` gates it. **Superseded the same day:** `scdoc` 1.11.5 was installed at the human's
  prompting and the render was run — it found `doc/xdu.1.scd` had not compiled since P3 (`*__root__*`
  nests italic inside bold) and, separately, that `_OUTDIR_/*/*.parquet` was publishing as
  `OUTDIR//.parquet`. Both fixed in a standalone `[fix]` commit after this phase; all four pages now
  render and the published literals were verified. P8's own EXIT STATUS rewrite was **not** implicated. Both new tests ran rather than self-skipping (`--nocapture`, not root).
  Because this phase's deliverable is prose whose only value is being **true**, every sentence was
  driven through the release binaries against a throwaway index rather than reasoned about: class A —
  a `chmod 000` dir inside `alpha` gives exit 1, one `error:` line, and **both** `Finished alpha` and
  `Finished beta`, alpha=1 row / 1 chunk, beta=1 row, 0 partials, no marker (the sibling was taken on
  after the error, which is precisely what the deleted clause denied); class B — `<index>/p2` as a
  regular file at `-j 1` gives exit 1 "File exists (os error 17)", 1 total row, `p3`/`p4` absent, no
  marker; and the new "Write failures are not downgraded" sentence was checked by re-running class B
  **with `--allow-errors`** — still exit 1, still no marker.)*
- **Touches:** `doc/xdu.1.scd`, `tests/crawl_tests.rs`. No `src/` change.

## Phase P9 — F4: readers warn when the marker records tolerated errors
**Satisfies:** R3, R8 · **Depends on:** P8
**Goal:** An `--allow-errors` index is knowingly incomplete yet carries the completion marker, and
`index_completion_warning` tests only `.exists()` — so the skipped regions are invisible to every query
tool, `xdu-rm` included, whose whole risk model is "files the index does not know about". The marker
body already records `errors=N`; nothing reads it. Finishing this branch's own signal, not new scope:
consent given by one operator at build time does not transport to whoever runs `xdu-rm` weeks later.

> **BLOCKING correction from the adversarial pass — do not skip.** The obvious implementation
> (`.exists()` then an unconditional `fs::read_to_string`) **introduces an indefinite hang in all three
> readers**: a read-only open of a FIFO (or symlink-to-FIFO, or character device) named `.xdu-complete`
> blocks forever, where today's `.exists()` returns instantly. This was executed both directions and
> confirmed. On shared HPC scratch the index directory is routinely group-writable. The read is also
> size-unbounded. The guard below is what makes the design's own safety claim ("worst case it is exactly
> today's code") true.

- [x] Add `const MARKER_READ_LIMIT: u64 = 64 * 1024;` to `src/lib.rs` (the writer emits ~100 bytes).
      *(Private, not `pub` — no caller outside `lib` needs it, and the guard test reaches it from
      `mod tests`.)*
- [x] Implement the presence test with **one** `fs::metadata(&marker)` — semantically identical to
      `Path::exists()`, which *is* `metadata().is_ok()`, so absent-marker behavior is preserved
      bit-for-bit at one `stat` instead of two:
      `Err` → the byte-identical absent-marker warning; `Ok(meta)` where
      `!meta.is_file() || meta.len() > MARKER_READ_LIMIT` → `None` (presence still attests; the body is
      not consulted); otherwise read with `read_to_string(...).unwrap_or_default()`. Comment the *why*:
      opening a FIFO or device node of that name would block the reader forever, and a huge one would
      be read into memory.
- [x] Add `pub fn completion_marker_errors(body: &str) -> Option<u64>` beside `COMPLETION_MARKER`: scan
      lines, `split_once('=')`, first key trimming to `errors` wins, `value.trim().parse().ok()`;
      `None` for a missing or unparseable key. Doc comment states the invariant — an unrecognized
      marker body says nothing about errors, so the reader stays as quiet as it was before the marker
      existed.
- [x] Keep `index_completion_warning`'s signature `(index: &Path) -> Option<String>` and return the new
      warning only for `Some(n) if n > 0`; every other arm returns `None`. **No `?`, no `unwrap`/
      `expect`, no new error type, no panic path** in either function — a corrupt, empty, non-UTF-8,
      directory-shaped, FIFO, oversized, or racily-deleted marker must yield `None`. Verify by reading
      the code, not only by test.
      *(Verified by reading: neither function contains `?`, `unwrap(`, `expect(` or `panic!`. The only
      fallible-call handling is `read_to_string(...).unwrap_or_default()`, which cannot panic, plus the
      `fs::metadata` `Err` arm. Confirmed mechanically by grepping both function bodies.)*
- [x] Warning text — must **not** contain the substring `completion marker`, so the existing
      markerless-index assertions keep discriminating: `warning: {index} was indexed with {n} tolerated
      error(s) (xdu --allow-errors); the affected paths were skipped, so results may be incomplete`.
      Note `classify_io_error` folds everything except `NotFound` into `errors`, so avoid the narrower
      "unreadable paths".
- [x] Add a lib unit test that **guards the guard**, or the metadata check will be "simplified" away
      later: `libc` is already a dev-dependency, so `libc::mkfifo` a `.xdu-complete`, then assert
      `index_completion_warning` returns `None` *and returns at all* (assert from a worker thread with
      `recv_timeout` — without the guard it hangs forever). Also assert a >64 KiB body yields `None`.
      *(Both added. The guard test was itself **proven to guard**: with the `is_file()/len()` check
      temporarily replaced by a no-op — exactly the "simplification" it exists to catch — the test hangs
      the full 10 s and fails with its intended message; guard then restored and re-verified green. A
      test that asserts the absence of a hang cannot be trusted until it has been seen to fail.)*
- [x] Add `test_completion_marker_errors_reads_the_writers_body` to `src/lib.rs`'s `mod tests`, building
      its input with `crawl::completion_marker_contents(&CrawlStats { errors: 3, .. }, t)` so writer and
      reader are pinned by one test; plus missing-key / empty / garbage / negative / no-trailing-newline
      / CRLF cases. Extend the existing `test_index_completion_warning` **in place**, keeping its
      current assertions verbatim.
- [x] Append reader assertions to `test_unreadable_subtree_allow_errors_continues`: `xdu-find --count`
      exits 0, **stdout trims to exactly `1`**, stderr contains `--allow-errors`, stdout contains no
      `warning` (invariant §13 — stdout stays pipeable). Add a root-safe
      `test_reader_warns_on_marker_recording_tolerated_errors`: clean index quiet at count 2; after
      rewriting the marker body with `errors=2`, `xdu-find --count` still prints exactly `2` on stdout
      and warns on stderr, and `xdu-rm --dry-run --force` warns on stderr while its stdout still lists
      the target and nothing is deleted.
- [x] Refresh the three call-site comments (`xdu-find.rs:19-20`, `xdu-view.rs:1839-1840`,
      `xdu-rm.rs:40-41`) so they no longer describe only the interrupted-run case. **Comment-only** —
      change no code there, keep every emission an `eprintln!`, and keep `xdu-view`'s call before
      `enable_raw_mode()` (verified: `:1841` is 24 lines ahead of `:1865`, and the only bails between
      are pre-terminal, so §12 is safe). Skip the `xdu-view` comment touch if it buys nothing.
      *(`xdu-find` and `xdu-rm` refreshed — both now name the `--allow-errors` case, and `xdu-rm`'s adds
      that whoever tolerated the errors at build time is rarely whoever is deleting now. **`xdu-view`
      deliberately left alone**, taking the escape this item offers: its comment explains *when* it
      prints (before the alternate screen claims the terminal), which is a §12 concern still exactly
      true and orthogonal to which case is being warned about. Rewording it would buy nothing and
      dilute the reason it is there.)*
- [x] Append one sentence to `doc/xdu.1.scd`'s `--allow-errors` paragraph: such a run still writes the
      marker, the marker carries the tolerated-error count, and the readers warn on stderr on a non-zero
      count. Coordinate with P8's rewrite of the same section; leave the three reader man pages alone.
- [x] Do **not** fix the partition-scoped marker limitation (marker-format/CLI-semantics work is a
      non-goal): `xdu.rs:58` clears the marker on *every* run including a partition-scoped one, and
      `:628` writes a whole-index marker from that run's stats, so a later clean `xdu -p onepartition`
      rewrites `errors=0` and silently retires the warning while other partitions' skipped regions
      remain. **Recording it is P11's job, not this phase's** — leave a `// Known limitation:` comment at
      the marker-write site stating the behavior, and let P11 carry the follow-up record. (Do not file it
      in `META.md`: that file is harness feedback only, per its own header and its F6 finding.)
- [x] Do **not** extend the warning to `vanished`/`lossy_paths`, add an `xdu-rm` refusal or extra
      prompt, add a suppression flag, or add a `format=`/version key to the marker body.
      *(All five held. `get_schema()`, `FileRecord`, every reader column list, the partition layout and
      `src/cli.rs` are untouched — `git diff` shows zero changed lines in `cli.rs`.)*
- **Verify:** fmt, clippy, `cargo test --lib`, `cargo test --test crawl_tests -- --nocapture` (confirm
  the new allow-errors assertions actually ran rather than hitting the root self-skip), full `cargo test`.
  *(Result: green — 66 lib (was 63) + 22 crawl (was 21) + 16 rm, fmt/clippy clean; all four `doc/*.scd`
  still render under `scdoc`. `--nocapture` confirms `test_unreadable_subtree_allow_errors_continues`
  **ran** rather than self-skipping (not root), so its new reader assertions genuinely executed.*
  *A **failed first drive is worth recording**: driving `temp_index.sh` immediately after the code change
  showed no warning at all on an `errors=3` marker, and the FIFO case "passing" — because that script
  rebuilds the release binaries only when **absent**, never when **stale**, so the drive exercised
  pre-P9 binaries. Every observation was vacuous: old code reads no body, so of course it neither warned
  nor hung. After `cargo build --release --bins`, the same six cases were re-driven and genuinely hold:
  (1) clean index silent at count 4; (2) `errors=3` → the exact warning on **stderr** while stdout stays
  exactly `4` and carries no `warning` (§13); (3) `xdu-rm -p '\.log$' --dry-run --force` warns on stderr,
  still lists `app.log` on stdout, and the file is still present afterwards; (4) a **FIFO** named
  `.xdu-complete` → `xdu-find` and `xdu-rm` both return `rc=0` under `timeout 10` instead of hanging,
  count still 4 — this is the case the guard exists for and it is only meaningful now that the code
  reads bodies; (5) a 70 KiB marker containing `errors=9` → zero warnings, the size guard beating the
  key; (6) markerless → the original "no completion marker" warning intact. Recorded as META F10.)*
- **Touches:** `src/lib.rs`, `doc/xdu.1.scd`, `tests/crawl_tests.rs`, optionally reader comments.
  **No** change to `get_schema()`, `FileRecord`, any reader's column list, the partition layout, or any
  CLI flag/default — if one becomes necessary, STOP: that is a GOAL non-goal.

## Phase P10 — F2: interleaved A/B in `bench/run.sh`, honest re-capture, rewritten claims
**Satisfies:** R4, R5 · **Depends on:** P9
**Goal:** R4 requires the committed baseline so a change is "quantified against it rather than
asserted"; R5 forbids merging a change that does not measurably help "as if it did". `scenarios.md`
claims "real wins on the flat-wide, many-partition and mid-`--jobs` mixed shapes, no measured regression
anywhere … back to back". None of that is established:

- Against `baseline.json` the shipped build is **flat-or-slower in 5 of 6** configs.
- `git diff b8f5f9c c9630c0 -- src/` is **empty** — `baseline.json` and `comparison-pre-p5.json` measure
  *identical crawl source* 45 min apart yet differ **1.1–18.5 %**, baseline faster in all six (a
  systematic session bias, not white noise). *(Figure corrected in review cycle 2 — see the remediation
  item below; this phase originally recorded the low end as 8.9 %.)*
- Between-invocation drift measured today: four back-to-back `s3 --scale 4` runs spanned 11 % (31 %
  across hours); three `s2 --scale 2` runs gave **3.08 / 4.05 / 6.76 s**. The committed "shipped" s2
  median of 2.71 lies *outside* the entire range observed for that same binary.
- `captured_at` = 18:49 / 19:34 / 20:58 / 00:52 **next day**. "back to back" is false, as is TECH.md
  P5's "measured back-to-back … against identical generated trees" (each invocation regenerates a tree).
- Three of four documents carry `git_dirty: true`; for the two l1-l2/l2-only captures the dirty paths
  included `src/`, making `run.sh`'s auto-note ("uncommitted changes outside `src/` do not affect it")
  **false** — and `run.sh:144-147` only ever computed whole-tree dirtiness, so the note asserts
  something the harness never checked.

The harness *does* record within-invocation spread (`spread()` emits `{median,min,max,samples[]}`); what
is missing is any between-invocation control. **Human decision: keep the P5 code, fix the harness,
re-capture, write the claims from the new document.**

- [x] `bench/run.sh`: add a build-input dirtiness probe beside the whole-tree one —
      `git status --porcelain -- src Cargo.toml Cargo.lock rust-toolchain.toml` decides
      `MEASURES_COMMIT`. Emit `xdu.measures_recorded_commit`. Comment the invariant: only a modified
      *build input* makes the recorded commit a lie about the measured binary.
- [x] Replace the false note (`run.sh:421-425`) with two verified branches — build input dirty →
      `git_commit` identifies the BASE commit, not the measured code; dirty but no build input touched →
      the measured binary IS the commit's build. Add an unconditional note that medians come from **one
      invocation**, that two invocations of the same binary differ by up to ~20 % on the reference host,
      and that `--compare-bin` is the way to A/B.
- [x] Record `xdu.binary = {path, bytes, mtime}`. Add `--bin PATH`, and when it is supplied explicitly
      do **not** auto-build (today `run.sh:157-160` builds only when the default path is *absent*, so a
      stale binary is measured silently).
- [x] Add the interleaved A/B mode: `--compare-bin PATH` plus optional `--compare-worktree DIR`
      (records B's commit and its own `measures_recorded_commit`). One generated tree per scenario; one
      warm-up per variant; **each timed rep runs both binaries** against the same tree with a fresh
      index, order alternating `A B` / `B A` by rep parity so neither runs systematically first. Add a
      `variant` column to the TSV and to the python grouping key `(scenario, jobs, variant)`, and verify
      the row count **per variant** so a build that is fast because it lost files fails for either side.
- [x] Emit top-level `comparisons[]`: per `(scenario, jobs)` — `a_median_s`, `b_median_s`,
      `paired_delta_pct {median, samples, a_faster_reps, reps}` from per-rep pairs matched by rep number,
      and `peak_rss_delta_pct`. The paired delta is the number a claim rests on, so the document carries
      it rather than leaving it to be re-derived.
- [x] **Non-negotiable, not polish:** extend `sh bench/run.sh smoke` with a second stage running the A/B
      path with the same binary as both variants, asserting both variants present,
      `indexed_files == generated_files` per variant, one `comparisons` entry with `reps` matching
      `len(samples)`, `measures_recorded_commit` true on a clean tree, and `xdu.binary.bytes` matching
      disk. Assert nothing about timing. Keep the one-line `smoke ok:` output; update `usage()`. Today
      smoke exits before the report path, so `comparisons[]` would otherwise ship untested.
- [x] Capture `bench/results/comparison-p5-ab.json`: A = HEAD's `xdu`, B = a `git worktree` build of
      `c9630c0`, `--compare-worktree` set, `--reps 7` (**9–11 for s2**, the only shape whose paired
      spread is wide enough to hide something), and the baseline config set (`s5 --scale 8 --jobs
      "1 2 4 8"`, `s2 --scale 2 --jobs 4`, `s3 --scale 4 --jobs 4`) so it is directly comparable.
      **Pass `--out` explicitly** — `baseline` mode defaults `OUT` to `bench/results/baseline.json`
      (`run.sh:300`) and would destroy the R4 reference. Quiet machine; ~10–12 min.
      **Build B in the worktree's OWN target dir** (or copy the binary out and
      `cargo clean --release -p xdu`): sharing `CARGO_TARGET_DIR` makes cargo reuse the pre-P5 `libxdu`
      rlib and breaks `xdu-find` with E0432 — hit for real during design.
      *(Amendments. (a) **`--reps 9` uniformly, not "7, with 9–11 for s2"**: one invocation carries one
      `--reps`, and the verify requires all six comparisons in a single document, so a split would have
      meant two documents. 9 satisfies ≥7 everywhere and lands inside the 9–11 band the wide shape
      needed. (b) Captured via `baseline` **mode**, which already is the required config set
      (`s5 --scale 8 -j "1 2 4 8"`, `s2 --scale 2 -j 4`, `s3 --scale 4 -j 4`), with `--out` passed
      explicitly — `baseline.json` verified unmodified afterwards. (c) Took the copy-out route rather
      than a separate target dir: a fresh `CARGO_TARGET_DIR` would rebuild bundled DuckDB from source,
      where building B into the shared dir, copying the binary out, then `cargo clean --release -p xdu`
      and rebuilding took ~70 s total. Both binaries confirmed to differ, and B confirmed to carry
      `records: Vec<FileRecord>` against A's builders. (d) A is HEAD (`2019bb1`), so it includes P6–P9 —
      none of which touch the crawl hot path; both sides recorded `measures_recorded_commit: true`.)*
- [x] Rewrite the claims **from the new document**: replace `scenarios.md:136-139`'s "What shipped"
      measurement claim with the A/B result (expected: no measured throughput effect on any shape, median
      paired delta within ±1 %, signs split, hidden effect bounded ~±3 % multi-directory / ~±10 %
      flat-wide); peak RSS down ~11 % on flat-wide (142 → 126 MiB) and **up ~35 % on many-partition**
      (13 → 17.5 MiB — the increase the old text omitted); and that the earlier 6–28 % apparent wins sit
      inside the documented drift. Keep the mechanism prose and say why the change is kept: one path
      copy instead of two, less memory on the shape with the most memory, and the `symlink_metadata`
      TOCTOU closed — **not** a throughput win.
- [x] Add a `## The noise floor — what this harness can resolve` section to `scenarios.md`:
      within-invocation spread (0.4–3 % multi-directory, 21–44 % flat-wide with a monotone within-run
      drift), between-invocation spread on identical source, the back-to-back series above, and the
      resulting rules — two documents cannot resolve < ~20 % and resolve nothing on flat-wide; a
      "faster" claim needs `--compare-bin`; peak RSS is the more portable signal. Label the numbers
      host-specific and frame them as rules about the harness, not about `xdu`.
- [x] Rewrite `scenarios.md:75-77` (comparability rule) to require an interleaved A/B rather than two
      invocations, and reword `:84-86` so it no longer calls the numbers reproducible across commits.
      Replace `:103-104` with a provenance table naming, per committed document, the commit recorded,
      whether it measures that commit, and what it is good for — stating plainly that
      `comparison-l1-l2.json` and `comparison-l2-only.json` carry an automatic note that is wrong for
      them, generated before the runner could tell the difference.
- [x] Correct the rejected stat-in-pool paragraph (`:141-151`) in place: note its numbers are a
      cross-invocation comparison, but the effect (many-partition **more than twice as slow in wall
      time**, not "50 % slower") is several times the largest observed drift, so **the rejection
      stands**; soften the flat-wide "the gain was really pipelining" to an unestablished
      cross-invocation number. Leave the deferred-levers list and the closing jwalk per-directory
      ceiling paragraph untouched — that material is exactly what R5's characterize-and-justify asks
      for and must not be overcorrected into "we measured nothing".
- [x] Fix `bench/HPC-PROTOCOL.md:80` (operators are currently told warm back-to-back runs are good for
      comparing two builds): require interleaving with paired per-rep deltas, cite the ~20 % spread, and
      add the `--compare-bin` invocation beside the runner example at `:113-119`. Update `AGENTS.md:213`
      from "Compare two builds via a `git worktree`, never a stash" to require the interleaved A/B in one
      invocation, with the reason.
- [x] Append an amendment note to this file's P5 comparison item recording that "measured back-to-back
      … against identical generated trees" and the overlapping-per-rep-ranges judgement were both wrong,
      and that `comparison-p5-ab.json` supersedes those numbers.
- [x] **STOP and escalate** if the fresh capture shows a regression clearing the paired spread — that
      reopens the keep-vs-revert decision, which is the human's. Do not revert `src/crawl.rs` on your
      own initiative.
- **Verify:** `sh bench/run.sh smoke`, then assert the new document's shape (both variants,
  `indexed_files == generated_files`, 6 comparisons, `samples` length == `reps`), `measures_recorded_commit`
  present in `run.sh`, a noise-floor section in `scenarios.md`, **no `src`/`doc`/`tests` diff**, and the
  full Rust gate green.
  *(Result: green, the whole chain in one run. `smoke ok:` (two-stage; the A/B self-check reports
  "2 runs over 10004 files, 2 paired reps"); `A/B ok: [('s2',4,0.0), ('s3',4,-0.6452), ('s5',1,-0.2548),
  ('s5',2,0.0), ('s5',4,0.0), ('s5',8,0.0)]`; `measures_recorded_commit` present; noise-floor section
  present; `git diff --quiet HEAD -- src doc tests` clean; fmt/clippy clean; 66 + 22 + 16 tests pass.*
  ***The escalation trigger did not fire.** No configuration shows a regression clearing the paired
  spread: every median is within ±1% and `a_faster_reps` is 1–4 of 9, i.e. signs split. The keep
  decision stands untouched and `src/crawl.rs` was not modified.*
  *Three things the capture **contradicted**, all corrected rather than papered over: (1) the
  design-time within-invocation spread of "0.4–3% on multi-directory shapes" is wrong — measured
  3.2–28.4% here, so `scenarios.md` now carries the measured range and says the old figure was
  optimistic; (2) I had drafted "peak RSS is nearly deterministic run to run" before the capture, and
  the data falsified it (spread reaches ~31% on `s5 -j1`) — the bullet now says RSS is trustworthy only
  when the difference is structural and consistent in sign per shape; (3) the predicted RSS outcome was
  incomplete — flat-wide 141.0 → 130.5 MiB (7.4% lower, predicted ~11%) and many-partition 12.9 → 16.8
  MiB (30% higher, predicted ~35%) both held, but the prediction omitted that mixed `-j8` also rises,
  102.4 → 114.4 MiB (11.7% higher). The claim now states the memory effect moves in **both**
  directions and gives the mechanism.*
  *Two harness bugs found by building the gate rather than by running it: `samples` could be filtered
  while `reps` still counted the dropped pair — breaking the very invariant this phase's verify
  asserts, now fixed by filtering `paired` once up front; and smoke's 104-file tree crawls in 0.00 s, so
  every paired delta was uncomputable and `comparisons[]` would have been asserted **empty** — smoke now
  runs at scale 100.)*
- [x] **Review cycle 2 · C2-F3 remediation.** The stated between-invocation drift range was wrong in
      both places it appeared (`scenarios.md:138` and `:172`): the document claimed the two baseline
      captures "differ by **8.9–18.5%** … in all six configurations", but recomputing `wall_s.median`
      from the committed JSON gives **1.1–18.5%** — `s5 -j8` drifted 1.14% and `s5 -j1` 8.85%, both
      below the claimed floor. Corrected in both passages, and in this phase's own problem statement
      above. The *direction* claim (baseline faster in all six) was re-verified and holds, so the
      "systematic session bias, not white noise" reading stands — but it now rests explicitly on the
      sign rather than on the magnitude, and the bullet says the magnitude is **not** uniform, so a
      configuration that drifts little in one pairing is not evidence the harness is quiet. The
      downstream "cannot resolve below ~20%" rule is sized against the worst case (18.5%) and is
      unaffected.
      *Gate tightened, not just the text fixed:* the old verify only ran `grep -qi "noise floor"`, which
      would pass with any number in the sentence. It now recomputes the range from
      `baseline.json` + `comparison-pre-p5.json` and asserts the document states exactly that figure
      twice, plus re-asserts the direction claim. **Seen to fail before being trusted** — reintroducing
      `8.9` into an isolated copy makes it exit 1 with
      `documented drift range is not the recomputed 1.1–18.5% (found 0 occurrences)`.
- **Touches:** `bench/run.sh`, `bench/scenarios.md`, `bench/HPC-PROTOCOL.md`,
  `bench/results/comparison-p5-ab.json` (new; `.gitignore:14` already whitelists `comparison-*.json`),
  `AGENTS.md`, this file. **No `src/` change** — the perf code is kept by decision.
- **Known footgun left as a follow-up:** `baseline` mode still defaults `--out` to the committed
  `baseline.json`, so an operator running a comparison the way the three existing comparison documents
  were captured can silently destroy the R4 reference. Do not fix here (it is CLI-surface work on the
  harness, and this phase is already the largest). **P11 carries the follow-up record** — this phase only
  needs the `--out` warning in `usage()` and a note in `scenarios.md`'s provenance table. (Not
  `META.md`: harness feedback only.)

## Phase P11 — Record every follow-up this cycle defers (F5 + the P9/P10 deferrals)
**Satisfies:** R8 · **Depends on:** P10

> **This phase is the single home for every follow-up P7–P10 deliberately did not fix.** R8 requires that
> "anything larger or riskier SHALL be recorded as an explicit follow-up rather than attempted here", and
> a deferral that is only mentioned in a phase checklist is not a record — the checklist is consumed and
> the item evaporates.
>
> **PREREQUISITE (human decision, 2026-08-05): the `issues/` convention lands via `/xdu-harness` first.**
> Deferred code work now goes in **`issues/<slug>.md`** (pre-shaped, `status: unshaped`, promoted into a
> real `spec/{slug}/GOAL.md` by `/xdu-feature`), with `ROADMAP.md` carrying an ordered abstract + link and
> `ASSESSMENT.md` *linking* rather than duplicating. The full spec is in [`META.md`](META.md) F6. **P7–P10
> do not depend on this** — only P11 does. If `issues/` does not exist when this phase is reached, STOP
> and report rather than improvising a destination or falling back to prose.
>
> **Never `META.md`** for a code follow-up — that file is the harness feedback log, scoped by its own
> header to "was this the skill's fault", and its F6 finding records a prior phase being wrongly told to
> file code follow-ups there. That boundary is what the `issues/` convention exists to make unmissable.
**Goal:** `xdu-view`'s terminal restore is plain sequential code after `run_app` — no `Drop` guard, no
panic hook, whole-file grep for `set_hook`/`impl Drop`/`catch_unwind` returns zero hits — and
`Cargo.toml:49` sets `panic = "abort"`, so `:1873-1874` are unreachable on any panic path. That is a
genuine invariant §12 violation, but it is **pre-existing and identical in `main`**, and this branch's
`xdu-view` diff (16+/10−) touches only the `index_glob`/`ROOT_PARTITION` dedup and the marker warning —
nothing in the raw-mode region. So it does **not** block, and fixing it here would be scope creep into a
reader rewrite. R8 requires it be *recorded* as an explicit follow-up, and the R8 record currently omits
it. The design pass found a **second**, related pre-existing §12 gap worth the same treatment.

- [x] Add two `###` sections to `ASSESSMENT.md`'s "Deferred, with reasons" list, after "Lift the pure
      TUI helpers into `lib`" and before "Decouple pool width from driver count", matching the file's
      format and ~100-col wrap: **"Restore the terminal on every exit path, including panic"** and
      **"Truncate display names on char boundaries, not byte indices"**.
- [x] The first must state all four facts: restore is plain sequential code after `run_app`;
      `panic = "abort"` removes the unwind that would otherwise run it; the fix is a `Drop` guard or
      panic hook; invariant §12 requires exactly that. Note the early-`?` case is real too —
      `Terminal::new` at `:1867` already runs after raw mode and the alternate screen are entered.
- [x] The second must name `render_list_content` and `render_tree_content`, the
      `&name[..width.saturating_sub(1)]` byte index, the bytes-vs-columns confusion, and §12's
      char-boundary clause, and tie the reachable panic back to the missing restore guard.
- [x] Both sections state they are pre-existing (identical in `main`) and give the reason they were not
      folded into this pass: they pair with the already-deferred TUI-helper lift as one §12
      terminal-safety change to the same 2,500-line file.
- [x] Extend `ROADMAP.md`'s "Internal cleanups surfaced by the crawl-hardening pass" with both gaps,
      replace the now-false "None changes what the tools do" sentence, and update the `**Seed:**` line to
      name the terminal-safety work so a future `/xdu-feature` does not drop it. Consider dropping
      ", low priority" — the bundle now includes a user-visible wedged terminal and a reachable panic.
- [x] Write **`issues/marker-scoped-run-attestation.md`**: `xdu.rs:58` clears the marker on every run
      including `xdu -p one`, and `:628` writes a whole-index marker from only that run's stats, so a
      later clean partition-scoped run resets `errors=0` and silently retires the tolerated-error warning
      while the other partitions' skipped regions remain. Record that it is a pre-existing limitation of
      the marker's semantics as introduced in this pass, that P9's warning is still a strict improvement
      over an index that said nothing at all, and that the fix is marker-format or CLI-semantics work
      (per-partition attestation, or refusing to write a whole-index marker from a scoped run) — both
      GOAL non-goals here, so it needs its own change.
- [x] Write **`issues/bench-baseline-overwrite-guard.md`**: `baseline` mode defaults `--out` to
      `bench/results/baseline.json` (`run.sh:300`), so an operator running a comparison the way the three
      existing comparison documents were captured silently destroys the R4 reference. Record that P10
      added a `usage()` warning but deliberately left the default unchanged, and that the fix is to
      require an explicit `--out` for any non-baseline capture (or refuse to overwrite an existing
      `baseline.json` without a flag).
- [x] Write **`issues/xdu-view-terminal-safety.md`** covering **both** §12 gaps as one change to one file
      (they are the same fix in the same 2,500-line binary, and pair with the already-recorded TUI-helper
      lift): the missing restore guard and the byte-index truncation. This is the `issues/` counterpart of
      the two `ASSESSMENT.md` sections above — write the mechanism and evidence here, and keep the
      `ASSESSMENT.md` sections short with a link, so R8's record stands alone without duplicating.
- [x] Add one `ROADMAP.md` entry per new issue in the established `## Title` + prose + `**Horizon:**` +
      `**Seed:**` shape, with `**Seed:**` pointing at the `issues/` file rather than carrying a one-line
      prompt, and **place them in intended remediation order** relative to the existing entries.
- [x] Do **not** touch `research/04-architecture.md` (a point-in-time input; `ASSESSMENT.md` is the R8
      deliverable and carries the correction). Add no test and change no code: the phase commit must
      touch only `ASSESSMENT.md`, `ROADMAP.md`, and new files under `issues/`, with zero paths under
      `src/`, `tests/`, `bench/`, `doc/`, or `Cargo.toml`/`Cargo.lock`.
- [x] Every new `issues/` file carries `status: unshaped` and the header line naming `/xdu-feature` as the
      promotion step — these are pre-shaped candidates, **not** locked contracts. Do not let one be copied
      into `spec/{slug}/GOAL.md` from this phase.
- [x] Cross-check before closing: walk P7–P10's checklists for every "do not fix" / "known limitation" /
      "left as a follow-up" line and confirm each has a matching `issues/` file **and** a ROADMAP entry.
      This phase closes the cycle's deferral ledger, so an unrecorded deferral is a phase failure.
- **Verify:** the grep assertions for all four new sections and the ROADMAP edits, a clean `src`/`tests`/
  `bench`/`doc` diff, `cargo fmt --all -- --check`, then `PHASE-OK`.
  *(Result: `PHASE-OK`. Every fact was re-derived from source rather than taken from this checklist,
  and all of it held: `grep -cE "set_hook|impl Drop|catch_unwind" src/bin/xdu-view.rs` → **0**;
  `Cargo.toml:49` `panic = "abort"`; raw mode `:1865`, alternate screen `:1866`, the fallible
  `Terminal::new` `:1867` **after** both, `run_app` `:1870`, restore `:1873-1874`; byte-index truncation
  at `:2211` in `render_list_content` and `:2356` in `render_tree_content` — two sites, as named. Both
  gaps confirmed **pre-existing**: this branch's `xdu-view` hunks end around `:1848`, touching neither
  region. Marker sites confirmed at `xdu.rs:92` (clear, not partition-filtered) and `:636` (write), with
  P9's `// Known limitation:` comment present at `:628`; the `baseline` `--out` default confirmed at
  `run.sh:458`.)*
- **Touches:** `spec/crawl-hardening/ASSESSMENT.md`, `ROADMAP.md`.
  *(Amendment — also `issues/`, which did not exist when this phase was written. The prerequisite
  landed via `/xdu-harness` (commits `de3e4ee`…`a64a9f1`) between this phase being blocked and run, so
  the three files follow `factory/templates/ISSUE.md` and sit alongside `issues/version-flag-missing.md`,
  which that pass created as the convention's worked example.)*

> **Ledger cross-check — the result.** Sweeping P7–P10 for every "do not fix" / "known limitation" /
> "left as a follow-up" / "out of scope" line returned 14 matches. **Three are genuine deferrals**, and
> each now has both an `issues/` file and a ROADMAP entry: P9's partition-scoped marker, P10's
> `baseline --out` footgun, and the §12 terminal-safety pair. The other eleven need no record and are
> accounted for rather than ignored: **scope boundaries** that are deliberate non-features, not defects
> (P8's decision to leave the stale-chunk case undocumented; P9's refusals to widen the warning to
> `vanished`/`lossy_paths`, add an `xdu-rm` prompt, a suppression flag, or a marker `format=` key; P7's
> instruction not to hoist the `-p` validation, which would *change* pre-existing behavior the GOAL's
> non-goals protect); **already fixed** (P10's `--bin` no-auto-build shipped in that phase, and its
> `temp_index.sh` analogue landed as META F10); and P11's own instructions.
>
> **Known gap, stated rather than silently left:** the five pre-P7 deferrals in `ASSESSMENT.md` (the
> `--version` defect, the DuckDB injection surface, the duplicated count formatters, the TUI-helper
> lift, and the perf levers) keep their existing homes — `ASSESSMENT.md`, `bench/scenarios.md`, and the
> ROADMAP "Internal cleanups" entry — and were **not** migrated to `issues/`. `--version` is the
> exception, promoted by the harness pass. That migration is the remaining half of META F6 item 5; it
> sits outside this phase's P7–P10 cross-check scope, and doing it here would have widened a
> records-only phase into a rewrite of records that are already durable.

> **BLOCKED 2026-08-05 — the prerequisite above is not met; no work was attempted.** Checked at the top
> of this phase, as instructed: `issues/` **does not exist** (absent from the working tree and untracked
> in git), `/xdu-harness` has **never run** (`.agents/factory/harness-log.md` holds only its template
> header, zero decision entries), and the convention is absent from `.agents/` apart from an incidental
> mention in `templates/GOAL.md`. This phase forbids improvising a destination or falling back to prose,
> so it stops here rather than inventing one — which is the whole point of the gate: the deferrals it
> must file are precisely the ones that evaporate when written somewhere provisional.
>
> **To unblock:** run **`/xdu-harness`** (human-gated) and apply [`META.md`](META.md) F6's agreed
> convention — items 1–4 are what this phase depends on: `issues/<slug>.md` reusing the `GOAL.md` body
> with `status: unshaped`, `ROADMAP.md`'s `**Seed:**` pointing at the issue file, the `AGENTS.md`
> repo-map entry stating the `META.md` / `issues/` / `ROADMAP.md` / `spec/` boundary, and the
> phase-authoring rule. F6 item 5 (migrating the existing deferrals, and promoting `--version` as
> `issues/version-flag-missing.md`) overlaps this phase's own ledger work — whoever runs `/xdu-harness`
> should decide whether the migration happens there or here, so the two do not write the same files
> twice. Then re-run `/xdu-build`; nothing in P11's checklist changes.
>
> **Nothing is lost by stopping:** every deferral this phase must record is already written down in the
> P7–P10 checklists and their amendment notes, plus the `// Known limitation:` comment at the
> marker-write site and P10's `usage()` warning and provenance-table note. This phase's job is to move
> them into a durable, indexed home — not to discover them.
>
> `review.blocked_reason` was repointed at this prerequisite; the cycle-1 reason it replaced ("marker
> cleared before work-queue validation; R5 perf evidence overclaims") named two findings that **P7 and
> P10 have since fixed**, so leaving it would have described a blocker that no longer exists. The full
> cycle-1 record remains in [`REVIEW.md`](REVIEW.md).

---

## Phase P12 — C2-F2: resync `AGENTS.md` + `invariants.md` with the code this cycle landed
**Satisfies:** R6, R10 · **Depends on:** P11
**Goal:** review cycle 2 found the operating manual drifted against the code this branch landed.
`AGENTS.md` opens by declaring itself the map and instructing that when it disagrees with the code,
**the code is ground truth — fix this file**; R6's outcome is that a maintainer can reason from "the
code and its documented invariants without re-deriving them". Four greppable gaps, all confirmed at
zero occurrences: `src/crawl.rs` (a new 874-line module holding the crawl's testable core) absent from
the repository map; `--allow-errors` absent from the CLI-surface list that enumerates every other `xdu`
flag; `ROOT_PARTITION` still attributed to `xdu.rs` though it moved to `lib.rs`; and the
`.xdu-complete` marker — a **new on-disk artifact at the index root**, load-bearing for all three
readers — documented nowhere, in neither file.

The compounding half is why this is not cosmetic: `invariants.md` is the curated gate that **both**
`/xdu-plan` and the next `/xdu-review` draw from. Stale there, it silently narrows every future cycle —
the next review would not check the marker contract or the `__root__` collision rejection at all.

- [x] `AGENTS.md` repository map: add `crawl.rs` with what it holds (work-queue construction incl. the
      `__root__` collision rejection, per-file record building, `PartitionBuffer` + atomic finalize,
      marker read/write) and the boundary that makes it testable — only the crawler uses it, the
      concurrency scaffold stays in `bin/xdu.rs`. Retune the `bin/xdu.rs` line to the scaffold role it
      now has.
- [x] `AGENTS.md` Architecture: re-attribute `ROOT_PARTITION` to `lib.rs`, state the `__root__`
      collision rejection, and add a **Run-level completion marker** bullet — path, dotfile rationale,
      the clear-after-pre-flight / write-on-success ordering and *why* per-chunk atomicity cannot express
      it, the `key=value` body, the soft-warning reader contract with its bounded non-blocking read, and
      the recorded scoped-run limitation. Extend the data-flow diagram to show the marker and the
      readers' warning path.
- [x] `AGENTS.md` CLI surface: add `--allow-errors` with its opt-in semantics (default fails non-zero
      and writes no marker; with it, errors are counted/reported, exit 0, `errors=N` recorded).
- [x] `AGENTS.md` invariants §2/§3 + the high-risk-files quick reference: point atomic finalize at
      `crawl.rs::PartitionBuffer::finalize()`, add the marker as a separate run-level mechanism, add the
      unconditional `__root__` rejection, and add `src/crawl.rs` as its own high-risk entry.
- [x] `.agents/factory/invariants.md`: same resync, as the curated gate — retitle §2 to the new
      finalize location, add **§2b** (the full marker contract, incl. the recorded scoped-run limitation
      marked *do not fix incidentally*) and **§2c** (fail-loud default + `--allow-errors` stays opt-in,
      with the jwalk one-`Err`-per-subtree reason it exists), and extend §3 with the collision rejection
      and `index_glob` as the single layout→SQL seam.
- [x] **Beyond the finding, same root cause:** `finalize()` moved to `crawl.rs`, so the
      **high-blast-radius file lists were stale** — and those lists are exactly what fires the review's
      mandatory human gate. A CONFIRMED finding in the atomic-finalize code would no longer have
      triggered it. Added `src/crawl.rs` to the list in both `invariants.md` and
      `review-rubric.md` (which duplicates it for the gate).
- **Verify:** greps asserting each of the eight facts above (including the *negative* assertion that
  the stale `ROOT_PARTITION`, `xdu.rs` attribution is gone), all four `doc/*.scd` still render under
  `scdoc`, **no `src`/`tests`/`bench` diff**, and the full Rust gate green.
  *(Result: green — `PHASE-OK`, 9 test-result blocks ok, 104 tests. Each of the eight assertions was
  then run individually to confirm none passes vacuously, and the gate was **seen to fail**: reverting
  the `ROOT_PARTITION` attribution in an isolated copy flips both the positive `lib.rs` assertion and
  the negative `xdu.rs` one.)*
- **Touches:** `AGENTS.md`, `.agents/factory/invariants.md`, `.agents/factory/review-rubric.md`, this
  file. **No `src/` change** — this phase is documentation resync only, per the cycle-2 human gate.

---

# Review cycle 3 remediation (P13)

Added by `xdu-build` under the cycle-3 human gate recorded in `REVIEW.md` ("Human sign-off on the
cycle-3 gate"), which authorized exceeding the documented ≤3 review-cycle bound to fix one CONFIRMED
finding in the coupled core and to record a second. Scope is exactly that: the `build_work_queue` guard
plus its regression test, and two deferral records. C2-F1 stays deferred on its existing record.

## Phase P13 — C3-F1: reject every reserved index-root name, not just `__root__` (+ record C3-F2)
**Satisfies:** R3, R8, R10 · **Depends on:** P12
**Goal:** this branch introduced a **second** reserved name at the index root (`.xdu-complete`) and
guarded only the first. A top-level source directory of that name is crawled as a partition, so it is
created as a *directory* at the marker path: the run indexes every file correctly but exits non-zero
(`EISDIR` writing the attestation), and **every later run against that outdir — from any source tree —
fails in `clear_completion_marker` with `EPERM`**. A correct index reported as failed, plus an outdir
that cannot be rebuilt or marked complete again without a manual `rmdir`. Reachability is low (a
pathological directory name), exactly as it is for `__root__` — which *is* rejected unconditionally.

The fix closes the **class**, not the instance: the guard iterates one `lib`-owned list of reserved
names, so reserving a name in future extends the rejection by construction rather than by someone
remembering the second half of the collision.

- [x] `lib.rs`: add `RESERVED_INDEX_NAMES` — `(name, what-claims-it)` pairs for `ROOT_PARTITION` and
      `COMPLETION_MARKER` — with the doc comment stating that `<index>/` holds exactly two kinds of
      entry and that adding a reserved name means adding it here. Record the reverse collision direction
      on `COMPLETION_MARKER` itself, whose doc comment previously stated only the one-way dotfile
      property.
- [x] `crawl.rs`: `build_work_queue` rejects against the whole list (before the `--partition` filter, so
      the check stays unconditional), naming both the entry and what claims it; re-export the list and
      retune the declarative comments to the class.
- [x] Unit test `test_build_work_queue_rejects_every_reserved_index_name`: **driven from the list
      itself** (so a future reserved name is covered without this test being remembered), each name
      checked with and without a `--partition` filter that would have excluded it; plus the negative
      case — a reserved name borne by a loose *file* is a `__root__` row, not a collision.
- [x] Real-binary regression test `test_reserved_marker_name_is_rejected_and_leaves_the_outdir_usable`:
      a source tree containing a top-level `.xdu-complete/` is rejected non-zero with a diagnostic naming
      the entry, nothing is indexed, the marker path is left unoccupied — **and an unrelated run against
      that same outdir then completes and attests itself**, which is precisely what the unguarded
      collision made permanently impossible.
- [x] `doc/xdu.1.scd`: OUTPUT FORMAT states the reserved-name rule for **both** names and both
      directions (the glob never mistakes the marker for a partition; a partition is never written over
      the marker), rendered and read as published text.
- [x] `AGENTS.md` + `.agents/factory/invariants.md` §3 (and §2b's cross-reference, the high-blast-radius
      file lines): restate the collision as a **namespace class** with the same-commit rule for a new
      reserved name — the drift META `F13` identifies as the reason two full blind passes missed this.
- [x] Record **C3-F2** (a removed top-level directory leaves a phantom partition that the marker
      attests as clean) per the four-homes rule: `issues/orphan-partition-survives-reindex.md` at
      `status: unshaped` + a `**Seed:**` entry in `ROADMAP.md`, both stating plainly that the stale
      partition is **pre-existing in `main`** (identical per-partition prune scope, verified via
      `git show main:src/bin/xdu.rs`) and that what this pass changed is the presence of an attestation
      that fails to detect it.
- **Verify:** `cargo test --test crawl_tests -- --nocapture && cargo test --lib && scdoc <
  doc/xdu.1.scd > /dev/null && test -f issues/orphan-partition-survives-reindex.md && grep -q
  "issues/orphan-partition-survives-reindex.md" ROADMAP.md && cargo fmt --all -- --check && cargo
  clippy --all-targets --all-features -- -D warnings && cargo test`
  *(Result: green. Both defects were reproduced first-hand against the real binaries before the fix —
  C3-F1's bricked outdir and C3-F2's `files=6` marker over a 9-row index — and both new gates were
  **seen to fail**: narrowing the guard to the list's first entry fails the unit test and the
  integration test, the latter with the pre-fix `Is a directory (os error 21)`.)*
- **Touches:** `src/lib.rs`, `src/crawl.rs`, `tests/crawl_tests.rs`, `doc/xdu.1.scd`, `AGENTS.md`,
  `.agents/factory/invariants.md`, `issues/orphan-partition-survives-reindex.md`, `ROADMAP.md`, this
  file. **No marker-format change, no other `src/` change**, per the cycle-3 human gate.

---

## Phase P14 — C4-F1/F2/F3/F4: record the prune defect, and stop the operating manual describing code that is gone
**Satisfies:** R2, R6, R8, R10 · **Depends on:** P13
**Goal:** close review cycle 4. Four findings, none of them a regression — one is recorded rather than
fixed, three are corrections to text that describes code inaccurately. The human gate (cycle-4
`blocked_reason`) authorized exactly this and **no other `src/` logic change**.

**A class-closure sweep ran before any edit**, because this branch's recurring failure is fixing the
named instance and missing the rest of the class (C3-F1 and META `F13` are both that mistake). It found
that each finding named **fewer sites than the class contains**: the `--version` falsehood is asserted
in **four** live operating files, not the two the review named, and `FileRecord` is named in **four**
live documents, not two. The extra sites are listed below and are corrected here — the alternative is
leaving the same trap armed for the next agent, which is the finding's own stated rationale. **The
`verify:` gate asserts the class is empty, not that the named lines changed.**

- [x] **C4-F1 — record, do not fix.** `finalize`'s prune runs from `num_chunks..`, so a partition that
      yielded a hard error and zero files finalizes with `num_chunks == 0` and prunes its **entire**
      prior contents. With `--allow-errors` the run then exits 0 and attests itself. Create
      `issues/unreadable-partition-prunes-prior-chunks.md` at `status: unshaped` + a `**Seed:**` entry in
      `ROADMAP.md`, both stating plainly that this is **pre-existing in `main`** — proved by a
      differential drive against a release build of `main`, where the same rows are destroyed, the run
      exits 0, and **no diagnostic is printed at all**. Cross-link it to the two sibling `finalize`
      prune-scope issues; it is the third member of that family and the only one that *destroys* rows.
- [x] **C4-F1, man page.** One sentence in `doc/xdu.1.scd`'s `--allow-errors` paragraph: tolerating a
      read error can prune previously-indexed rows. That paragraph is the only surface that enumerates
      what tolerance costs, so it is the only one that can contradict a new cost — the clap help text and
      `AGENTS.md`'s CLI bullet delegate to it and stay untouched. Escapes matched to the file's own
      conventions and **verified as published text** through `scdoc | mandoc | col -b`, not by exit 0.
- [x] **C4-F2 — the operating manual asserts a flag that does not exist.** Four live sites claim clap
      derives `--version`; all four binaries exit 2 on it. Correct `AGENTS.md:48`,
      `.agents/factory/invariants.md:183`, and — found by the sweep, **beyond the two the review
      named** — `.agents/skills/xdu-release/SKILL.md:93` (an instruction a human follows while cutting a
      release) and `.agents/skills/xdu-build/SKILL.md:79`. Preserve the *true* half verbatim (single-
      sourced from `Cargo.toml`; never hardcode a version in `src/`) and excise only the false mechanism
      clause; point at `issues/version-flag-missing.md` so the recorded defect stays discoverable from
      the manual it contradicts.
- [x] **C4-F3 — the concurrency-contract comment names a function that never existed under that name.**
      `src/bin/xdu.rs:39` cites `record_from_metadata`. Fix the **claim**, not just the token: the
      sweep confirmed nothing builds a record any more, so "per-file record building" is wrong
      independently of the name — the direct-to-Arrow rewrite replaced it with `file_size_and_atime` +
      `lossy_path` appending straight into the builders.
- [x] **C4-F4 — delete `FileRecord`** (`src/lib.rs`) and its three unit tests. The sweep proved it is
      unreachable from production: every `use xdu::{…}` is explicit and none names it, and there are no
      glob imports outside the two test modules. Then remove it from the **four** live documents that
      name it — `AGENTS.md`'s repo map **and** invariant #1, `.agents/factory/invariants.md` §1, and
      `.agents/skills/xdu-plan/SKILL.md` (a live instruction to future planners) — so `get_schema()` is
      named as the sole schema contract. Frozen `spec/` records, the `bench/results/*.json` label, and
      the `issues/`+`ROADMAP` deferral pair are **left alone**: they are point-in-time evidence, and
      retrofitting them would destroy the audit trail.
- **Verify:** the phase gate above. It asserts published man-page text (not exit 0), that
  `record_from_metadata` and `FileRecord` appear **nowhere** in `src/`/`tests/`, that `derives` appears
  in **none** of the four operating files, that both manual files now point at
  `issues/version-flag-missing.md`, that the new issue exists at `status: unshaped` and is linked from
  `ROADMAP.md`, plus the full pre-release gate.
  *(Result: green — `PHASE-OK`. 102 tests: 63 lib (66 − the three deleted `FileRecord` cases) + 23
  `crawl_tests` + 16 `rm_tests`; all four `.scd` render. **Each new gate was seen to fail first**, and
  per `F3`'s refinement the mutation was confirmed present before the red was trusted: deleting the
  man-page sentence reddens the flattened-render grep, restoring `derives` to `AGENTS.md` reddens the
  four-file class check, and a single `FileRecord` mention added to `invariants.md` reddens the
  manual check. The render grep matches the **flattened** published text, so a future rewrap of that
  paragraph cannot silently pass it. CLI drive on a throwaway index: 4 rows, `__root__` = 1, marker
  present, 0 `.partial`, stdout clean — behavior unchanged, as a docs-and-dead-code phase requires.)*
- **Touches:** `src/lib.rs`, `src/bin/xdu.rs`, `doc/xdu.1.scd`, `AGENTS.md`,
  `.agents/factory/invariants.md`, `.agents/skills/{xdu-release,xdu-build,xdu-plan}/SKILL.md`,
  `issues/unreadable-partition-prunes-prior-chunks.md`, `ROADMAP.md`, this file. **No `src/` logic
  change** — the only code removed is a struct with no production callers.

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
