---
slug: index-schema-versioning
title: "Version-stamp the index marker and refuse unreadable versions"
kind: feature
appetite: small
status: in_progress
branch: feature/index-schema-versioning
base: main
current_phase: P1
last_updated: "2026-09-07"
phases:
  - id: P1
    name: "Lib core: version constant, marker line, gate, unit tests"
    status: pending
    satisfies: [R1]
    depends_on: []
    parallel: false
    hammerable: false
    hill: uphill
    verify: "cargo test --lib"
  - id: P2
    name: "Wire the gate into find/rm/view plus real-binary refusal tests"
    status: pending
    satisfies: [R2, R3, R4]
    depends_on: [P1]
    parallel: false
    hammerable: false
    hill: uphill
    verify: "cargo test --test version_tests"
  - id: P3
    name: "Mirror gate, refusal spot-drive, deferral ledger"
    status: pending
    satisfies: [R4]
    depends_on: [P2]
    parallel: false
    hammerable: false
    hill: uphill
    verify: "cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test"
review:
  last_reviewed_commit: ""
  verdict: none
  blocked_reason: ""
  cycle: 0
---

# TECH.md — Version-stamp the index marker and refuse unreadable versions

The **context engine and finite-state machine** for building this feature. The YAML
frontmatter above is the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/index-schema-versioning/TECH.md`); the per-phase
checklists below are the work. `xdu-build` executes the next actionable phase, runs its
`verify:` command, updates state via
`uv run --with pyyaml python .agents/factory/bin/set_phase.py …`, and makes one atomic code+state commit.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R-IDs are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).
- **Backing research:** none — lean path (see PLAN §4).

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
  helpers). The coupled core (`src/bin/xdu-rm.rs`, `src/bin/xdu.rs`, `src/cli.rs`) is
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

## Phase P1 — Lib core: version constant, marker line, gate, unit tests
**Satisfies:** R1 · **Depends on:** —
**Goal:** The version exists on disk and can be checked, with the contract pinned by unit tests
before any reader depends on it.

- [ ] Add `pub const INDEX_FORMAT_VERSION: u32 = 1` in `src/lib.rs` beside `COMPLETION_MARKER`.
- [ ] Append `format={INDEX_FORMAT_VERSION}` as the last line of
  `crawl::completion_marker_contents` (PLAN §2; never parse the `xdu=` tool-version key).
- [ ] Extract the guarded marker read into one helper shared by `index_completion_warning` and
  the new gate (same stat-once / skip-non-file / `MARKER_READ_LIMIT` / degrade-to-empty semantics).
- [ ] Add `completion_marker_format(body) -> Option<u32>` (first `format` key wins, strict parse)
  and `index_version_error(index) -> Option<String>` failing closed per PLAN §2.
- [ ] Extend the writer↔reader pin test to the `format=` line; add the gate matrix (fresh body
  accepted; missing / versionless / garbage / wrong-number refused; good version with
  `errors=N` accepted).
- **Verify:** `cargo test --lib`.
- **Touches:** `src/lib.rs`, `src/crawl.rs`.

## Phase P2 — Wire the gate into find/rm/view plus real-binary refusal tests
**Satisfies:** R2, R3, R4 · **Depends on:** P1
**Goal:** Every reader refuses an unreadable version before touching data, proven by driving the
real binaries end to end.

- [ ] Call `index_version_error` before `index_completion_warning` in `src/bin/xdu-find.rs`,
  `src/bin/xdu-rm.rs`, and `src/bin/xdu-view.rs` (view: inside the existing pre-terminal block);
  bail via the bins' `anyhow::Result` with the PLAN §2 diagnostic (found vs supported, re-index
  remedy). No query, no prompt, no deletion, no terminal on refusal; warning path unchanged on pass.
- [ ] Add the `xdu-view` arm to `tests/common/mod.rs::binary_path` (refusal needs no TTY).
- [ ] New `tests/version_tests.rs` (`mod common;`, shared helpers only): fresh-index round-trip
  accepted by find/rm with no version diagnostic (R4); deleted marker and `format=7` marker each
  make find/rm/view exit non-zero with a version-and-remedy diagnostic naming the failure (R2, R3);
  refused rm unlinks nothing.
- **Verify:** `cargo test --test version_tests`.
- **Touches:** `src/bin/xdu-find.rs`, `src/bin/xdu-rm.rs`, `src/bin/xdu-view.rs`,
  `tests/common/mod.rs`, `tests/version_tests.rs`.

## Phase P3 — Mirror gate, refusal spot-drive, deferral ledger
**Satisfies:** R4 · **Depends on:** P2
**Goal:** The tree is green under the release gate, the refusal flows are seen live once, and no
deferral escapes unrecorded.

- [ ] Spot-drive via `.agents/factory/bin/temp_index.sh sh -c '…'` against a throwaway index:
  delete the marker and run each reader (expect refusal); write a `format=999` marker and rerun
  find (expect refusal); fresh index queries clean (expect no diagnostic). Evidence only — the
  committed proof stays in P1/P2 tests.
- [ ] Run the mirror gate: `cargo fmt --all -- --check`, `cargo clippy --all-targets
  --all-features -- -D warnings`, full `cargo test`.
- [ ] Deferral ledger: walk P1/P2 checklists for "do not fix here" / "known limitation" /
  "follow-up" language and confirm each has a matching `issues/` file plus `ROADMAP.md` entry.
  Expected outcome: none — the scoped-marker limitation already lives in
  `issues/marker-scoped-run-attestation.md`; record that confirmation in the commit body.
- **Verify:** `cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test`.
- **Touches:** spec state only (plus any gate fallout the mirror gate exposes).

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
