---
slug: crawl-progress-misleads-on-huge-trees
title: Honest crawl progress on huge trees
kind: fix
appetite: small
status: done
branch: fix/crawl-progress-misleads-on-huge-trees
base: main
current_phase: P2
last_updated: '2026-09-10'
phases:
- id: P1
  name: Pure message builder in lib, [Tn] dropped
  status: done
  satisfies:
  - R1
  depends_on: []
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test --lib
- id: P2
  name: Quiet-partition liveness plus full gate and drive
  status: done
  satisfies:
  - R2
  - R3
  - R4
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo fmt --all -- --check && cargo clippy --all-targets --all-features
    -- -D warnings && cargo test && .agents/factory/bin/temp_index.sh xdu-find --count
review:
  last_reviewed_commit: e4a22d6
  verdict: approved
  blocked_reason: ''
  cycle: 1
---
# TECH.md — Honest crawl progress on huge trees

The **context engine and finite-state machine** for building this feature. The YAML
frontmatter above is the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/crawl-progress-misleads-on-huge-trees/TECH.md`); the per-phase
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

## Phase P1 — Pure message builder in lib, [Tn] dropped
**Satisfies:** R1 · **Depends on:** —
**Goal:** The per-partition line no longer claims driver ownership, and its rendering lives in a
unit-tested pure builder in `src/lib.rs`.

- [x] Add a pure partition-line builder to `src/lib.rs` (all three line states — waiting,
  quiet-scanning, lively; exact signature the implementer's choice within `PLAN.md` §2's
  required elements), reusing `format_count` / `format_bytes`; unit-test each state.
- [x] Wire the lively branch into `src/bin/xdu.rs` in place of the current inline `format!`,
  minus the `[T{driver_id}]` token; clean up the now-unused `driver_id` binding so
  `clippy -D warnings` stays green.
- [x] No behavior change to the walk, the index bytes, the global line, or non-TTY output.
- **Verify:** `cargo test --lib`.
- **Touches:** `src/lib.rs`, `src/bin/xdu.rs`.

## Phase P2 — Quiet-partition liveness plus full gate and drive
**Satisfies:** R2, R3, R4 · **Depends on:** P1
**Goal:** A partition yielding no files shows directories visited plus elapsed time instead of a
frozen `scanning...`; the global line refreshes on the same hoisted tick; the full gate and a
real-binary drive prove no regression.

- [x] Add a driver-local `dirs_visited` counter; hoist the throttled bar refresh so every
  walker entry (files and directories) triggers it, per `PLAN.md` §2.
- [x] Render the quiet states through the P1 builder — `waiting...` with elapsed at zero
  yields, `scanning` with dirs visited plus elapsed once entries flow; leave the global
  message shape unchanged.
- [x] Drive the real binaries on a throwaway index and assert the index is byte-complete
  (`xdu-find --count` over the fixture); run `fmt --check`, `clippy -D warnings`, full
  `cargo test`.
- [x] Deferral ledger: walk P1–P2 for any "do not fix here" / "known limitation" /
  follow-up language and confirm each has a matching `issues/` file plus `ROADMAP.md` entry.
  An unrecorded deferral fails this phase.
- **Verify:** `cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test && .agents/factory/bin/temp_index.sh xdu-find --count`.
- **Touches:** `src/bin/xdu.rs`, `src/lib.rs` (builder extension + tests).

---

## How `xdu-build` drives this

1. `next_phase.py` prints the next actionable phase (statuses are authoritative; the
   `current_phase` pointer is reconciled against them).
2. Pre-flight: clean tree, on `branch`, `base` reachable.
3. Execute every `[ ]` in the phase (consult `PLAN.md` / `research/` for detail).
4. Run the phase's `verify:` command — never advance on a checkbox alone.
5. Amend this file freely if reality diverges (regenerate frontmatter with `set_phase.py`; note the
   amendment in the commit body). STOP and escalate only on a **`GOAL.md` contradiction**.
6. Mark the phase `done`, advance `current_phase`, `--touch`; one `[{category}]` commit; stop and report.
