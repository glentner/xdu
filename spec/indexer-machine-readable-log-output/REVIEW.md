# REVIEW — Machine-readable log output for scripted and cron-driven crawls

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** d14bd1508b08fa5e072d5127e8922f52fec16824  ·  **Base:** main  ·  **Date:** 2026-09-10
- **Verdict:** changes-requested
- **Cycle:** 1 of ≤3 — mirrors `review.cycle` in `TECH.md` (escalate to human on non-convergence)
- **Mode:** full blind pass over the spec-excluded diff (no scoping to prior findings; this is cycle 1).
- **Contract drift:** `git log --oneline main..HEAD -- spec/indexer-machine-readable-log-output/GOAL.md`
  shows only the shaping commit `474740c`; the locked contract did not move mid-build.
- **Artifact-deliverable R-IDs:** none. All of R1–R5 have CLI-observable evidence in the
  spec-excluded diff, so the blind reviewer graded all five; the orchestrator second-passed the
  failure path.

## Verification run

Commands actually executed and their outcomes (the spine of the review):

Blind reviewer (fresh subagent, no `PLAN.md`/`TECH.md`/`META.md`/`spec/`):

- `git diff main...HEAD -- . ':(exclude)spec/'` + `git log --oneline main..HEAD` → graded hunks in
  `src/lib.rs`, `src/bin/xdu.rs`, `tests/crawl_tests.rs` (+ lifecycle `issues/*.md` status flip).
- Success drive: 2-partition tree through `./target/debug/xdu --apparent-size -o "$IDX" "$SRC"` →
  5/5 stderr lines matched `^\d{4}-…Z (INFO|WARN|ERROR)`, 0-byte stdout; `Indexing … (jobs=4,
  size=apparent-size)`, `Finished alice/bob`, `Completed`, `Marker …/.xdu-complete written` all
  observed; marker file present (`errors=1` on the `--allow-errors` drive).
- Failure drives: missing-dir (`exit 1`) and fail-loud permission-denied (`exit 1`, 5 shaped
  records) → timestamped `ERROR` naming the path, no `.xdu-complete`, no `Marker … written` line.
  Same drives exposed the unshaped anyhow trailer (finding 1).
- TTY control under a pty (`script -q`) → styled `Indexing`/`Finished`/`Completed` with ANSI
  escapes, zero timestamped lines; TTY failure prints only the bare anyhow block.
- Timestamp cross-check: 7 values (epoch, 1999/2000 boundaries, both Feb-29 leap days, GOAL's own
  `2026-09-10T03:00:01Z`, 2038, 2100) against Python `datetime` → all match; pre-epoch saturation
  documented and unreachable at runtime.
- Embedded-newline drive (directory literally named `bad\ndir` through the permission-denied
  path) → `ERROR` and `Finished` lines each stayed one folded line.
- `cargo test` (full suite: 90 lib + all integration files) → green, per reviewer.
- `grep -rn` for `R#`/`P#` in `src/` → no matches; new symbols follow §11 altitude.

Orchestrator second pass (this session):

- `cargo build` → clean (`Finished dev profile`).
- Missing-dir reproduction: `./target/debug/xdu -o "$TMP/index" "$TMP/missing" >out 2>err` →
  `exit=1`, stdout 0 bytes, stderr 5 lines: 1 shaped
  (`2026-09-10T14:35:27Z ERROR Failed to resolve directory: …/missing: No such file or directory
  (os error 2)`) followed by 4 unshaped (`Error: …`, blank, `Caused by:`, `No such file…`).
  **Confirms finding 1.**
- `cargo test --test crawl_tests -- --nocapture test_non_tty` → ok (1 passed).
- `cargo test --test crawl_tests -- --nocapture test_failing_run` → ok (1 passed) despite the
  unshaped trailer above. **Confirms finding 2** (the test cannot catch finding 1 by construction).
- `grep -n 'println!' src/bin/xdu.rs | grep -v eprintln` → no bare `println!` (stdout clean).
- `grep -rn -E '\bR[0-9]+|\bP[0-9]+\b' src/lib.rs src/bin/xdu.rs` → exit 1, no matches (no spec ids
  in source, §13).
- `git diff main...HEAD -- src/cli.rs doc/ --stat` → empty: no CLI change, no `.scd` update owed.
  No `.scd` touched, so no `scdoc` render or literal assertion was owed by this diff.
- `git status --porcelain` → empty on hand-back from the reviewer and at orchestrator verify time.

Gate states: `cargo fmt` / `cargo clippy` / full `cargo test` were **not observed** by the
orchestrator in this session (reviewer reports full `cargo test` green; that is reviewer-observed,
not orchestrator-observed). Man-page gate: no `doc/*.scd` in the spec-excluded diff, so no render
owed by this diff; base gate state **not observed**. CI rollup state **not observed** (this session
has no `gh`; `xdu-publish` Step 1 reads the actual rollup).

## Requirement → evidence matrix

Bidirectional traceability. All R-IDs graded by the blind reviewer; orchestrator confirmed the
failure-path rows.

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 — non-TTY records are single timestamped severity-tagged lines on stderr | `LogLevel` + `format_log_timestamp` + `format_log_record` (`src/lib.rs`); all non-TTY arms in `src/bin/xdu.rs` routed through them (`Indexing`, `report` closure, `Finished`, `Completed`, failure record in `main`); unit + integration tests | Reviewer: success drive 5/5 lines shaped, permission-denied drive shaped, `bad\ndir` fold drive, unit shape/fold tests pass. Orchestrator: missing-dir drive shows 1 shaped + 4 unshaped (anyhow trailer) → partial. | ❌ partial (finding 1) |
| R2 — log carries run-start (args), per-partition finish (counts), warning, marker, summary | Run-start options string (`size_mode_label`, `jobs=`, `size=`, `allow-errors` in `src/bin/xdu.rs`); `Marker … written` verdict line after `write_completion_marker`; `Finished`/`Completed` wrappings | Reviewer: success drive observed all five record kinds; `size=disk-usage, allow-errors` confirmed on allow-errors drive; marker file on disk with `errors=1`. Warning facet inspection-only (only WARN emitter is the lossy-path branch through the live-verified closure; APFS rejects non-UTF-8 names so no live WARN drive — platform self-skip AGENTS.md documents). Run-start omits `outdir`/`buffsize`; GOAL does not enumerate arguments, so no gap. | ✅ |
| R3 — failure record + no marker-success record | Timestamped `ERROR` record in `main`'s `Err` arm (`src/bin/xdu.rs`); marker write + verdict line strictly after all failure returns | Reviewer + orchestrator: both failure drives (missing dir, fail-loud) emitted timestamped `ERROR` naming the path, exited non-zero, left no `.xdu-complete`, emitted no `Marker … written`. | ✅ |
| R4 — stdout stays clean and pipeable | No new stdout writes; indicatif draws to stderr (`src/bin/xdu.rs`) | Reviewer: 0-byte stdout on success and both failure drives; zero `println!`. Orchestrator: `wc -c out` → 0 on missing-dir drive; no bare `println!` in `src/bin/xdu.rs`. | ✅ |
| R5 — TTY rendering untouched; new shape is non-TTY only | Only `else` (non-TTY) branches changed; TTY arms byte-identical by inspection | Reviewer: pty drive shows styled records with ANSI escapes, zero timestamped lines; TTY failure prints only the bare anyhow block. | ✅ |

Unmapped changes (possible scope creep): none. Every product hunk maps to an R-ID: `LogLevel` /
timestamp / record helpers + unit tests (R1), `size_mode_label` + `allow_errors` threading +
run-start string (R2), all `report` / `Finished` / `Completed` wrappings (R1/R2), `main` → `run`
split + timestamped failure record (R3), marker-verdict line (R2), the two integration tests
(R1–R4). The `issues/*.md` status flip is lifecycle bookkeeping, not product scope.

## Findings

Severity: **CRITICAL** (any `invariants.md` §1–§12 violation is auto-CRITICAL, **including lettered
subsections** such as §2b/§2c; a §13 project-conventions violation is **HIGH**) · **HIGH** ·
**MEDIUM** · **LOW**. Verdict: **CONFIRMED** (reproduced) vs **PLAUSIBLE** (suspected, needs human
triage). Only CONFIRMED findings auto-loop to `xdu-build`.

### [HIGH/CONFIRMED] Non-TTY failure emits an untimestamped multi-line anyhow trailer after the shaped ERROR record (R1 partial)

- **Where:** `src/bin/xdu.rs:570-588` (`main` returning `Err` through `Termination` after already
  logging the shaped record).
- **Failure scenario:** any failing `xdu` run without a TTY — the exact 3 AM cron failure the GOAL
  motivates. The shaped `… ERROR …` record prints, then the runtime prints its own
  `Error: …\n\nCaused by:\n    …` block to stderr. That block is neither timestamped,
  severity-tagged, nor single-line, so R1 ("every diagnostic record SHALL go to stderr as a single
  timestamped, severity-tagged line") fails on the failure path. Success-path logs are unaffected.
- **Evidence:** orchestrator reproduction (branch head `d14bd15`):
  `./target/debug/xdu -o "$TMP/index" "$TMP/missing" >out 2>err` → `exit=1`, stdout 0 bytes,
  stderr = shaped `2026-09-10T14:35:27Z ERROR Failed to resolve directory: …/missing: No such file
  or directory (os error 2)` + unshaped `Error: Failed to resolve directory: …/missing`, blank,
  `Caused by:`, `    No such file or directory (os error 2)` (shape check: 5 lines, 1 shaped,
  4 unshaped). Reviewer independently reproduced on the missing-dir and fail-loud
  permission-denied drives (latter: 5 shaped records + trailing untagged
  `Error: encountered 1 unreadable path(s); …`; regex check reported `6 lines, 1 unshaped`).
  Fix direction (not applied; read-only session): report the timestamped record, then exit
  non-zero without returning `Err` through `Termination` — preserving exit code and marker
  ordering while emitting exactly one failure record.
- **Touches invariant / requirement:** R1 (HIGH: GOAL R-ID unmet on a common path). No §1–§12
  invariant violated: §2b marker ordering held (verdict line strictly after write, never on
  failure), §2c fail-loud held (non-zero exit naming path/errno, no marker), §13 stdout-clean
  held — so severity stays HIGH, not CRITICAL. **Touches high-blast-radius file `src/bin/xdu.rs`
  → human gate triggered (see below).**

### [LOW/CONFIRMED] Failure-path integration test asserts only ERROR lines, so it cannot catch the untagged trailer

- **Where:** `tests/crawl_tests.rs` — `test_failing_run_logs_error_record_without_marker`
  (companion to finding 1, not an independent defect).
- **Failure scenario:** the test filters with `err.lines().filter(|l| l.contains("ERROR"))` and
  shape-checks only those, while the success-path test checks every line. The unshaped
  `Error:`/`Caused by:` lines pass through unchecked by construction.
- **Evidence:** `cargo test --test crawl_tests -- --nocapture test_failing_run` → ok on a binary
  just shown to emit the unshaped trailer (orchestrator); reviewer ran the same (`cargo test
  --test crawl_tests -- --nocapture failing_run` → ok against the demonstrating binary). The test
  passes before and after a fix for finding 1; it just cannot catch this class. Tightening it to
  assert every stderr line is shaped (as the success test does) would lock finding 1's fix.
- **Touches invariant / requirement:** R1 test coverage (LOW: missing-but-non-blocking coverage).
  No invariant violated.

Dropped after refutation (no finding filed): timestamp math, embedded-newline forging, marker
ordering, fail-loud exits, TTY byte-shape, `R#`/`P#` ids in source, operating-manual drift — each
investigated with executed commands or `grep` and dissolved (see Verification run).

## Human-gate triggers

Set if any CONFIRMED finding touches the high-blast-radius core (`src/bin/xdu-rm.rs`,
`src/bin/xdu.rs`, `src/crawl.rs`, `src/lib.rs`, `src/cli.rs`) or a destructive-rm / schema-stability /
atomic-write / SQL-injection invariant — these **always** require human sign-off before
`xdu-publish`, regardless of auto-loop. (`invariants.md`'s *High-blast-radius files* header is the
authoritative path list; this copy may only ever **widen** to match it.)

- **TRIGGERED — finding 1 touches `src/bin/xdu.rs`** (crawl concurrency scaffold + marker
  sequencing, high-blast-radius core). Explicit human sign-off required before any further step,
  regardless of auto-loop. No destructive-rm / schema-stability / atomic-write / SQL-injection
  invariant is implicated, and no CONFIRMED finding touches `src/lib.rs`'s schema/SQL surface.

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

- Not requested (`completeness` argument absent); not run.

---

## Review cycle 2 — approved (2026-09-10)

- **Reviewed commit:** 9ae1b0c1952f0dc149cb2715f13659671d2592a4 · **Base:** main · **Date:** 2026-09-10
- **Verdict:** approved
- **Cycle:** 2 of ≤3 — mirrors `review.cycle` in `TECH.md`.
- **Mode:** full blind pass over the spec-excluded diff (no scoping to prior findings).
- **Contract drift:** `git log --oneline main..HEAD -- spec/indexer-machine-readable-log-output/GOAL.md`
  shows only the shaping commit `474740c`; the locked contract did not move mid-build.
- **Artifact-deliverable R-IDs:** none. All of R1–R5 have CLI-observable evidence in the
  spec-excluded diff, so the blind reviewer graded all five.

### Verification run

Blind reviewer (fresh subagent, no `PLAN.md`/`TECH.md`/`META.md`/`spec/`):

- `git diff main...HEAD -- . ':(exclude)spec/'` + `git log --oneline main..HEAD` → graded hunks in
  `src/lib.rs`, `src/bin/xdu.rs`, `tests/crawl_tests.rs` (+ lifecycle `issues/*.md` status flip).
- Success drive (multi-partition): every stderr line matched
  `^[0-9]{4}-…Z (INFO|WARN|ERROR)`, stdout 0 bytes; run-start with args, per-partition `Finished`
  with counts, `Marker … written`, `Completed` all observed; `--apparent-size` / `-k 4K` / default
  render `size=apparent-size` / `block-rounded(4096)` / `disk-usage`; `--allow-errors` appends
  `, allow-errors` and records `errors=1` in the marker.
- Failure drives (missing dir, reserved-name `__root__`, bad partition, outdir-blocked, fail-loud
  unreadable partition): each exited 1 with a shaped `ERROR` naming the cause, no `.xdu-complete`,
  no `Marker … written`. Missing-dir drive is exactly 1/1 shaped lines; fail-loud drive 6/6 shaped.
  Prior-cycle anyhow trailer (`Error:` / `Caused by:`) confirmed gone.
- Embedded-newline partition drive (`we\nird`): `Finished` line stayed one folded shaped line.
- Timestamp cross-check (hand-rolled Hinnant civil-from-days vs `datetime` reference: epoch, Y2K
  boundary, leap days 2000/2024, `2026-09-10`, 2038, 2100, 9999-12-31; live stamp vs `date -u`):
  all match.
- TTY control under pty (`script -q`): styled ANSI `Indexing`/`Finished`/`Completed` with spinners,
  zero timestamp-shaped lines; TTY failure keeps the native anyhow block with no tags. Correct per R5.
- `eprintln!` audit of `src/bin/xdu.rs`: every off-TTY diagnostic goes through `format_log_record`;
  the two bare `eprintln!` and both `mp.println` are inside TTY-only branches. No `println!` in
  `src/bin/xdu.rs`, `src/lib.rs`, `src/crawl.rs`.
- `grep -rn` for `R#`/`P#` in `src/` → no matches. `git diff main...HEAD -- src/cli.rs doc/ --stat`
  → empty: no CLI change, no `.scd` update owed.
- `cargo test --lib` (90 passed incl. 5 new log-shape unit tests), full `crawl_tests` 25/25 green,
  `cargo fmt --check` clean, `cargo clippy -D warnings` clean, per reviewer.
- `git status --porcelain` → empty on hand-back; temporary probes removed; no `target/` mutation
  requiring restore (normal builds + throwaway-index drives only).

Orchestrator second pass (this session):

- `git status --porcelain` → empty; `git rev-parse HEAD` → `9ae1b0c`; spec-excluded diffstat →
  `src/bin/xdu.rs`, `src/lib.rs`, `tests/crawl_tests.rs`, `issues/*.md` flip.
- Missing-dir reproduction (`./target/debug/xdu -o "$TMP/index" "$TMP/missing" >out 2>err`) →
  `exit=1`, stdout 0 bytes, stderr exactly 1 line
  (`2026-09-10T15:00:29Z ERROR Failed to resolve directory: … (os error 2)`), all-shaped,
  no marker. **Prior finding 1 confirmed fixed.**
- Success drive via `temp_index.sh` (2 files) → `exit=0`, stdout 0 bytes, 5/5 stderr lines shaped,
  `Marker … written` + `Completed` + marker file all present.
- `cargo fmt --all -- --check` → clean (exit 0, orchestrator-observed).
- `cargo test --test crawl_tests -- --nocapture test_non_tty test_failing_run` → 2 passed
  (orchestrator-observed). **Prior finding 2 confirmed fixed.**
- `grep -rn -E '\bR[0-9]+|\bP[0-9]+\b' src/lib.rs src/bin/xdu.rs` → exit 1, no matches.
- `git diff main...HEAD -- src/cli.rs doc/ --stat` → empty. No `println!` in `src/bin/xdu.rs`.

Gate states: `cargo fmt` clean and the two log-shape integration tests green are
orchestrator-observed; full `cargo test` / `cargo clippy` green is reviewer-observed, not
orchestrator-observed. Man-page gate: no `doc/*.scd` in the spec-excluded diff, so no render or
literal assertion owed by this diff; base gate state **not observed**. CI rollup state
**not observed** (this session has no `gh`; `xdu-publish` Step 1 reads the actual rollup).
`ReportFindings` skipped per harness portability (no such tool here; `REVIEW.md` is the record).

### Requirement → evidence matrix

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 — non-TTY records are single timestamped severity-tagged lines on stderr | `LogLevel` + `format_log_timestamp` + `format_log_record` (`src/lib.rs`); all non-TTY arms in `src/bin/xdu.rs` routed through them; `main`/`run(is_tty)` split with `process::exit(1)` off-TTY suppresses the anyhow trailer (`9ae1b0c`) | Reviewer: success 6/6 shaped, fail-loud 6/6 shaped, missing-dir 1/1 shaped, newline-fold drive, 5 unit tests. Orchestrator: missing-dir 1/1 shaped, success 5/5 shaped. | ✅ |
| R2 — run-start (args), per-partition finish (counts), warning, marker, summary | Run-start options string (`size_mode_label`, `jobs=`, `size=`, `allow-errors`); `Marker … written` after attestation; shaped `Finished`/`Completed` | Reviewer: all five record classes observed live; size-mode and allow-errors variants driven. Orchestrator: run-start / `Finished` / `Marker` / `Completed` observed on success drive. WARN facet inspection-only (only WARN emitter is the lossy-path branch; APFS rejects non-UTF-8 names so no live WARN drive — platform self-skip AGENTS.md documents). | ✅ |
| R3 — failure record + no marker-success record | Shaped `ERROR` in the `run` Err branch; marker write strictly on the success path | Reviewer + orchestrator: five failure drives each exit 1 with shaped `ERROR`, no `.xdu-complete`, no `Marker … written`. | ✅ |
| R4 — stdout stays clean and pipeable | No stdout writes added | Reviewer: 0-byte stdout on six drives; no `println!`. Orchestrator: 0-byte stdout on both drives; no bare `println!`. | ✅ |
| R5 — TTY rendering untouched; new shape is non-TTY only | Only non-TTY branches changed | Reviewer: pty drive shows styled records, zero timestamped lines; TTY failure shows native anyhow block only. | ✅ |

Unmapped changes (possible scope creep): one lifecycle hunk (`issues/*.md` status flip) — factory
bookkeeping, not product scope. Everything else maps to an R-ID. Clap parse errors (exit 2)
remain unshaped — considered and dropped by the reviewer (outside R1's evidence scope; reshaping
clap would be scope creep into `src/cli.rs`).

### Findings

No CONFIRMED or PLAUSIBLE findings. Every candidate dissolved under execution (timestamp math,
embedded-newline forging, marker ordering, fail-loud exits, `process::exit(1)` destructor safety,
TTY byte-shape, `R#`/`P#` ids, manual drift).

Remediation of cycle-1 findings, re-verified from scratch:

- Cycle-1 HIGH (anyhow trailer, R1 partial) — fixed by the `main`/`run` split in `9ae1b0c`;
  missing-dir and fail-loud drives now emit only shaped lines with identical exit codes.
- Cycle-1 LOW (failure test filtered to `ERROR` lines) — fixed; the test now asserts every stderr
  line keeps the record shape behind a non-empty guard.

### Human-gate triggers

- **Not triggered.** No CONFIRMED finding touches the high-blast-radius core or a
  destructive-rm / schema-stability / atomic-write / SQL-injection invariant, so no sign-off gate
  is owed by this cycle. (Cycle 1's gate on `src/bin/xdu.rs` is discharged by the verified fix.)

### Optional completeness sub-pass (separate reviewer; may see TECH.md)

- Not requested (`completeness` argument absent); not run.
