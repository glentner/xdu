---
slug: indexer-machine-readable-log-output
title: Machine-readable log output for scripted and cron-driven crawls
kind: feature
appetite: small
status: in_review
branch: feature/indexer-machine-readable-log-output
base: main
current_phase: done
last_updated: '2026-09-10'
phases:
- id: P1
  name: Timestamped severity-tagged records end to end
  status: done
  satisfies:
  - R1
  - R4
  - R5
  depends_on: []
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test --lib && .agents/factory/bin/temp_index.sh sh -c 'mkdir -p t/a
    && head -c 512 /dev/zero > t/a/f && xdu t -o idx 2>err.log && grep -qE "^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    err.log && grep -q "INFO" err.log && grep -q "Completed" err.log'
- id: P2
  name: Run-start arguments, marker verdict, and failure record
  status: done
  satisfies:
  - R2
  - R3
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: uphill
  verify: .agents/factory/bin/temp_index.sh sh -c 'xdu /nonexistent-path-xyz -o idx2
    2>fail.log; test $? -ne 0 && grep -q "ERROR" fail.log && test ! -e idx2/.xdu-complete'
- id: P3
  name: Regression tests, full gate, and deferral ledger
  status: done
  satisfies:
  - R1
  - R2
  - R3
  - R4
  - R5
  depends_on:
  - P2
  parallel: false
  hammerable: true
  hill: uphill
  verify: cargo fmt --all -- --check && cargo clippy --all-targets --all-features
    -- -D warnings && cargo test
review:
  last_reviewed_commit: ''
  verdict: none
  blocked_reason: ''
  cycle: 0
---
# TECH.md — Machine-readable log output for scripted and cron-driven crawls

The **context engine and finite-state machine** for building this feature. The YAML
frontmatter above is the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/indexer-machine-readable-log-output/TECH.md`); the per-phase
checklists below are the work. `xdu-build` executes the next actionable phase, runs its
`verify:` command, updates state via
`uv run --with pyyaml python .agents/factory/bin/set_phase.py …`, and makes one atomic code+state commit.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R-IDs are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).

## Frontmatter field reference

- `status` (top): `planned | in_progress | blocked | in_review | done` (`done` is stamped by
  `xdu-publish` after confirmation, just before landing — the terminal state of the retained record)
- `appetite`: `small | big` — caps phase count and build-iteration budget (circuit breaker).
- phase `status`: `pending | in_progress | done | blocked`
- `satisfies`: GOAL R-IDs this phase delivers (traceability anchor for `xdu-review`).
  **Artifact-deliverable R-IDs.** Some requirements are met by a *committed document* — a research
  audit, a protocol doc, an assessment — not by CLI-observable behaviour. Attach such an R-ID to the
  phase that produces and commits the artifact, and give that phase a content `verify:`
  (`test -f …`, a `grep -q` for the sections that must exist) rather than forcing a CLI drive.
  **Say so in the phase body**, because it changes who can grade it: `xdu-review` blinds its reviewer
  to all of `spec/`, so an R-ID whose evidence lives there is graded by the *orchestrator*, not the
  blind reviewer. Naming it in `TECH.md` is what lets the review skill route it correctly instead of
  reporting an unverifiable requirement.
- `depends_on`: phase ids that must be `done` first (a phase is actionable only when met).
- `parallel`: `true` only for genuinely independent, non-coupled work (docs, tests, isolated `lib`
  helpers). The coupled core (`src/bin/xdu-rm.rs`, `src/bin/xdu.rs`, `src/lib.rs`, `src/cli.rs`) is
  always `false`.
- `hammerable`: `false` marks a correctness/security phase that scope-hammering must **never** cut.
- `hill`: `uphill` (still figuring it out) → `crest` (unknowns resolved) → `downhill` (just
  executing). A phase stuck `uphill` across builds is a raised hand → escalate to the human.
- `attempts`: durable failed-verify counter (absent = 0), bumped by `set_phase.py --phase P<n>
  --record-attempt` on every red verify gate; `next_phase.py` warns at ≥3 — the circuit breaker
  runs on this file, not on session memory.
- `verify`: the exact command that proves the phase (prefer driving the real CLI, not just tests —
  wrap CLI drives in `.agents/factory/bin/temp_index.sh sh -c "…"` so they hit a throwaway index,
  never a real index / the developer's real filesystem).
- `review.cycle`: completed review passes, auto-incremented by every `set_phase.py --verdict …`;
  REVIEW.md's "Cycle {n}" mirrors it and the ≤3-cycle bound is graded against it.

## Conventions (apply to every phase)

- Commit conventions, code style, and load-bearing invariants come from
  [`AGENTS.md`](../../AGENTS.md) — it is the constitution. Consult
  [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) for the curated footgun
  checklist relevant to this change.
- One phase per `xdu-build` invocation by default; one atomic commit containing **both** the code and
  the `TECH.md` state change. Branch commit subjects follow the house style `[{category}] Build {slug}
  P<n>: …` (no `WIP:` prefix) — squashed into the single PR-title commit at `xdu-publish`.
- **No `Co-Authored-By` trailer** (attribution lives in the PR body, not the commit); PR **bodies** end
  with an attribution trailer naming the actual harness, model, and variant.
- A CLI/feature change updates the affected `doc/*.scd` man page **in the same commit** (shell
  completions regenerate from `src/cli.rs`; the generated `share/` tree is git-ignored, not committed).

---

## Phase P1 — Timestamped severity-tagged records end to end
**Satisfies:** R1, R4, R5 · **Depends on:** —
**Goal:** Every existing non-TTY diagnostic goes to stderr through the new record format;
TTY output is provably untouched and stdout gains nothing.

- [x] Add `LogLevel` + `format_log_record` to `src/lib.rs` with unit tests (line shape, tag
  set, UTC timestamp format, hostile input such as embedded newlines stays one line).
- [x] Route all non-TTY arms in `src/bin/xdu.rs` through it (`Indexing`, per-partition
  `Finished`, `warning:`/`error:` reports, final `Completed`); keep every message tail a
  superstring of what the existing `tests/crawl_tests.rs` `contains` assertions match
  (prefix, never rewrite).
- [x] Confirm the TTY arms are byte-identical and no new write to stdout exists.
- **Verify:** `cargo test --lib && .agents/factory/bin/temp_index.sh sh -c 'mkdir -p t/a && head -c 512 /dev/zero > t/a/f && xdu t -o idx 2>err.log && grep -qE "^[0-9]{4}-[0-9]{2}-[0-9]{2}T" err.log && grep -q "INFO" err.log && grep -q "Completed" err.log'` (lib tests plus a real non-TTY crawl whose stderr carries timestamps, tags, and the summary).
- **Touches:** `src/lib.rs`, `src/bin/xdu.rs`.

## Phase P2 — Run-start arguments, marker verdict, and failure record
**Satisfies:** R2, R3 · **Depends on:** P1
**Goal:** The log alone reconstructs the run and explains its exit status: what was asked,
what was attested, and — on failure — what went wrong with no success marker in sight.

- [x] Extend the run-start record with effective arguments (jobs, size mode, partition
  filter, `--allow-errors`); keep the `Indexing <dir>` tokens.
- [x] Emit a marker-verdict record after `write_completion_marker` on the success path only.
- [x] Emit an explicit `ERROR` record before each failure `Err` return (fail-loud bail and
  pre-flight rejects); leave the `Err` returns, exit codes, and marker ordering untouched.
- **Verify:** `.agents/factory/bin/temp_index.sh sh -c 'xdu /nonexistent-path-xyz -o idx2 2>fail.log; test $? -ne 0 && grep -q "ERROR" fail.log && test ! -e idx2/.xdu-complete'` (failing drive exits non-zero, logs a timestamped failure, writes no marker).
- **Touches:** `src/bin/xdu.rs`.

## Phase P3 — Regression tests, full gate, and deferral ledger
**Satisfies:** R1, R2, R3, R4, R5 · **Depends on:** P2
**Goal:** Lock the whole contract in the test suite and leave the tree gate-clean. This phase
delivers no new CLI-observable behavior; its R-IDs are satisfied by the committed regression
tests below, so the orchestrator (not the blind reviewer) grades them.

- [x] Add an integration test in `tests/crawl_tests.rs` locking the non-TTY record shape
  (every stderr line timestamped and tagged; run-start / per-partition / marker / summary
  records present; stdout pipeable).
- [x] Run the pre-release mirror: `fmt --check`, `clippy -D warnings`, full `cargo test`.
- [x] Walk P1–P2 checklists for "do not fix" / "follow-up" / "known limitation" language and
  confirm each has a matching `issues/{slug}.md` plus `ROADMAP.md` entry; an unrecorded
  deferral fails this phase. (Walked 2026-09-10: the sole follow-up mention is the GOAL
  non-goal of a future `--log-format`/`--quiet` flag, negotiated at shaping — not a
  build-time deferral, so no new `issues/` file; P1–P2 bodies and the diff carry no
  "do not fix here".)
- **Verify:** `cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test`.
- **Touches:** `tests/crawl_tests.rs`.

---

## How `xdu-build` drives this

1. `next_phase.py` prints the next actionable phase (statuses are authoritative; the
   `current_phase` pointer is reconciled against them).
2. Pre-flight: clean tree, on `branch`, `base` reachable.
3. Execute every `[ ]` in the phase (consult `PLAN.md` for detail).
4. Run the phase's `verify:` command — never advance on a checkbox alone.
5. Amend this file freely if reality diverges (regenerate frontmatter with `set_phase.py`; note the
   amendment in the commit body). STOP and escalate only on a **`GOAL.md` contradiction**.
6. Mark the phase `done`, advance `current_phase`, `--touch`; one `[{category}]` commit; stop and report.
