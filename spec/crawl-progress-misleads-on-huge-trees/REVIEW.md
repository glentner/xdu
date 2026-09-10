# REVIEW — Crawl progress misleads on huge trees

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** e4a22d6  ·  **Base:** main  ·  **Date:** 2026-09-10
- **Verdict:** approved
- **Cycle:** 1 of ≤3 — mirrors `review.cycle` in `TECH.md` (escalate to human on non-convergence)

## Verification run

Commands actually executed and their outcomes (the spine of the review).
Blind-reviewer pass plus orchestrator sanity re-checks:

- `cargo fmt --all -- --check` → exit 0, clean (reviewer-observed).
- `cargo clippy --all-targets --all-features -- -D warnings` → exit 0, clean (reviewer-observed).
- `cargo test` → all green, exit 0: lib 85 passed; `xdu-view` bin 5; `cli_version` 1;
  `crawl_tests` 23; `find_tests` 9; `offline_tests` 1; `rm_tests` 18; `version_tests` 6;
  0 failed (reviewer-observed).
- Real-binary drives via `.agents/factory/bin/temp_index.sh` (throwaway indexes only):
  multi-partition crawl → `xdu-find --count` returns expected rows with marker `format=2`;
  dirs-only partition (zero files) crawls exit 0 with `Finished emptydirs (0 files, 0 B)`;
  symlink excluded from the index; stdout empty on non-TTY runs (reviewer-observed).
- `sh bench/run.sh smoke` → ok (104/104 files indexed + marker present), exit 0
  (reviewer-observed; no interleaved A/B timing comparison — see R4 row).
- Orchestrator re-checks: `git show main:src/bin/xdu.rs | grep -n "driver_id\|\[T"`
  confirms the old `"... [T{}]", driver_id` line is gone on the branch; `grep` over the new
  `format_partition_progress` builder in `src/lib.rs` confirms three claim-free line states
  plus a regression test asserting no `[T` token.
- Man-page render gate: not observed — no `doc/*.scd` in the diff, nothing to render.

## Requirement → evidence matrix

Bidirectional traceability. Flag requirements with no implementing change **and** changes that map
to no requirement (scope creep).

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 (no exclusive-ownership label) | `src/bin/xdu.rs` (`[T{driver_id}]` dropped, `map(\|driver_id\|` → `map(\|_\|)`); `src/lib.rs` `format_partition_progress` (all states claim-free); commits 14078ae, e4a22d6 | `git show main:src/bin/xdu.rs` grep confirms old token gone; `cargo test --lib partition_progress` (4 passed, incl. no-`[T` assertion); exhaustive `set_message`/print enumeration shows no line names a driver/worker/thread; live drive prints `Finished alice (...)` with no `[T` token | ✅ |
| R2 (quiet-partition evidence of life) | `src/bin/xdu.rs` `dirs_visited` counter + `part_start`, tick block reached on directory entries; `src/lib.rs` builder `waiting... Ns` / `scanning N dirs, Ns, no files yet`; commits 14078ae, e4a22d6 | Builder unit tests (`waiting... 0s/47s`, `scanning 3 dirs, 12s`, `1.5K` scaling) pass; dirs-only live drive exit 0 with correct index (`--count` → file partition only) | ✅ with one PLAUSIBLE edge gap (F1) |
| R3 (global line keeps aggregate totals) | `src/bin/xdu.rs` global `set_message` retained in the hoisted tick, now also fired by directory entries; `enable_steady_tick(200ms)` untouched; commit e4a22d6 | Code inspection + live drives (`Completed 4 files (...)`); TTY rendering itself not observed (no PTY harness) — claim rests on preserved `MultiProgress` plumbing, stated as such | ✅ |
| R4 (single-pool + work-stealing kept, no display-caused regression) | Concurrency scaffold byte-identical apart from the `driver_id` → `_` rename; per-entry added cost is one `is_dir()` branch + `u64` increment with formatting confined to the 100 ms tick; commit e4a22d6 | Diff inspection (pool construction, queue drain, `thread::scope` unchanged); full `cargo test` green; `bench/run.sh smoke` ok; no interleaved A/B timing run (display-only diff, O(1) per-entry cost — stated as such) | ✅ |

Unmapped changes (possible scope creep): none in `src/`. Every `src/` hunk maps to R1, R2,
or R3. The `issues/crawl-progress-misleads-on-huge-trees.md` one-liner
(`status: unshaped` → `status: shaped on …`) is factory process bookkeeping, benign.

Non-goals held: non-TTY posture unchanged (stdout empty, status on stderr); index format
untouched (`get_schema` not in diff, marker still `format=2`); marker sequencing untouched
(clear-after-pre-flight / write-on-success identical).

## Findings

Severity: **CRITICAL** (any `invariants.md` §1–§12 violation is auto-CRITICAL, **including lettered
subsections** such as §2b/§2c; a §13 project-conventions violation is **HIGH**) · **HIGH** · **MEDIUM** · **LOW**. Verdict: **CONFIRMED**
(reproduced) vs **PLAUSIBLE** (suspected, needs human triage). Only CONFIRMED findings auto-loop to
`xdu-build`.

### [MEDIUM/PLAUSIBLE] F1 — per-partition liveness refresh is yield-gated; a zero-yield stall still shows bare `scanning...`
- **Where:** `src/bin/xdu.rs:213` (initial `bar.set_message(format!("{}: scanning...", …))`) vs
  `:372-441` (tick update inside `for entry in walker`, gated on `now - last_bar_update >= 100ms`).
- **Failure scenario:** the walker yields nothing for longer than the refresh interval — a blocked
  metadata read before the first yield, or a stall between yields — so the loop body never runs,
  `dirs_visited`/`part_start` never reach the display, and the line stays at the bare `scanning...`
  with no dirs count and no elapsed: the frozen state R2 was written to eliminate. The `waiting...`
  builder branch needs a tick with `files==0 && dirs==0`, which needs an entry that is neither file
  nor dir, so on a symlink-free tree the pre-first-yield window has no elapsed-bearing state either.
  The motivating Lustre case (millions of directories *flowing*, no files completed) IS covered —
  dirs flow, the tick fires, the line reads `scanning N dirs, Ns, no files yet`. The residual hole is
  only the no-yields-flowing window.
- **Evidence:** code-structure reading confirmed by the orchestrator (`:213` vs `:372-441`);
  flowing case verified by `cargo test --lib partition_progress` (4 passed) and the dirs-only live
  drive (exit 0, correct index). NOT reproduced against a wedged walker — building one needs a
  filesystem that blocks yields for seconds, which no harness here provides — hence PLAUSIBLE, not
  CONFIRMED, per the refutation protocol. Dropped alternatives recorded: "no stated interval"
  literalism (non-goals leave the interval to plan — manufacturing), the initially-empty global
  message (pre-existing, improved not regressed), and repo-map drift (nothing in `AGENTS.md` /
  `invariants.md` / `doc/xdu.1.scd` describes these lines — grep-verified, nothing now wrong).
- **Touches invariant / requirement:** R2 (edge path).
- **Suggested triage for the human:** either hoist the tick out of the entry loop (elapsed refresh
  on the existing 100 ms `steady_tick`, keeping the yield-driven counters), or record that
  zero-yield stalls intentionally read as quiet-against-moving-global and narrow R2's "yielded no
  completed files" to "yielded entries but no files". No auto-loop: PLAUSIBLE does not block.

No other findings. Invariants §§1–13 checked for touched sections only: schema/marker/partition/
rm-safety/SQL-injection untouched; §7 concurrency shape preserved; §8 symlink exclusion re-verified
by drive; §11 altitude improved (render logic in `lib` with unit tests); §13 clean (no `R#`/`P#`
ids in `src/`, non-TTY stdout verified empty, prose voice consistent).

## Human-gate triggers

Set if any CONFIRMED finding touches the high-blast-radius core (`src/bin/xdu-rm.rs`,
`src/bin/xdu.rs`, `src/crawl.rs`, `src/lib.rs`, `src/cli.rs`) or a destructive-rm / schema-stability /
atomic-write / SQL-injection invariant — these **always** require human sign-off before
`xdu-publish`, regardless of auto-loop. (`invariants.md`'s *High-blast-radius files* header is the
authoritative path list; this copy may only ever **widen** to match it.)

- Not triggered: zero CONFIRMED findings. Note for the human at publish time: F1 (PLAUSIBLE,
  R2 edge path, touches `src/bin/xdu.rs` + `src/lib.rs`) awaits a triage decision — fix now
  (hoisted tick) or narrow R2 — but it does not block under the review rules.

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

- Not run: no `completeness` argument was passed.
