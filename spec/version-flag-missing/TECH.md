---
slug: version-flag-missing
title: Answer -V/--version in all four binaries
kind: fix
appetite: small
status: done
branch: fix/version-flag-missing
base: main
current_phase: done
last_updated: '2026-09-09'
phases:
- id: P1
  name: Derive --version from Cargo.toml in all four clis plus regression test
  status: done
  satisfies:
  - R1
  - R2
  - R3
  depends_on: []
  parallel: false
  hammerable: false
  hill: downhill
  verify: 'cargo build --bins -q && want=$(grep -m1 "^version" Cargo.toml | cut -d\"
    -f2) && for b in xdu xdu-find xdu-view xdu-rm; do for f in --version -V; do out=$(./target/debug/$b
    $f) || exit 1; echo "$out" | grep -qF "$want" || { echo "MISMATCH $b $f got: $out
    want: $want"; exit 1; }; done; done && ! grep -rnF "$want" src/ && d=$(mktemp
    -d) && ./target/debug/gen-completions "$d/bash" "$d/zsh" >/dev/null && test $(grep
    -rl -- --version "$d/bash" | wc -l) -eq 4 || { echo "MISSING bash completions";
    exit 1; } && test $(grep -rl -- --version "$d/zsh" | wc -l) -eq 4 || { echo "MISSING
    zsh completions"; exit 1; } && rm -rf "$d" && ! grep -rn "flag does not exist"
    AGENTS.md .agents/factory/invariants.md && cargo clippy --all-targets --all-features
    -- -D warnings && cargo test'
review:
  last_reviewed_commit: af1ac9f5c66c46ed32e4564141e8d9994d563b93
  verdict: approved
  blocked_reason: ''
  cycle: 2
---
# TECH.md — Answer -V/--version in all four binaries

The **context engine and finite-state machine** for building this feature. The YAML
frontmatter above is the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/version-flag-missing/TECH.md`);
the per-phase checklists below are the work. `xdu-build` executes the next actionable phase, runs its
`verify:` command, updates state via
`uv run --with pyyaml python .agents/factory/bin/set_phase.py …`, and makes one atomic code+state commit.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R-IDs are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).
- **Backing research:** none on the lean path — see `PLAN.md` §4.

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
  For this fix the man pages already document the intended flags, so the code-only change satisfies
  the rule with nothing to update — see `PLAN.md` §3.

---

## Phase P1 — Derive --version from Cargo.toml in all four clis plus regression test
**Satisfies:** R1, R2, R3 · **Depends on:** —
**Goal:** all four binaries answer `-V`/`--version` with the `Cargo.toml` version, completions
offer the flag, and a committed test locks the behavior.

- [x] Add the bare `version` key (no value) to each of the four `#[command(...)]` blocks in
  `src/cli.rs`: `XduArgs`, `XduFindArgs`, `XduViewArgs`, `XduRmArgs`. No literal anywhere.
- [x] Add a `tests/` regression test driving the four real binaries with both flags via the
  shared `tests/common/mod.rs` helpers, expecting exit 0 and `env!("CARGO_PKG_VERSION")` on
  stdout. Name it for the behavior, with no spec R-IDs in file names, test names, or comments.
- [x] Leave `doc/*.scd` untouched (already correct) and `gen-completions` untouched (same
  `Command` objects); do not "fix" the stale `0.4.1` marker-parse fixtures in `lib.rs` unit
  tests — out of scope.
- [x] Remediate review F1 (operating-manual drift): reword the stale `--version` defect record in
  `AGENTS.md` (Version single-sourced bullet) and `.agents/factory/invariants.md` (§13) to state the
  flag exists and derives from `Cargo.toml`. Class sweep found one further live site,
  `.agents/skills/xdu-release/SKILL.md:94`, which is harness-owned and rides via `META.md` +
  `/xdu-harness`, not this branch; `spec/**`, `issues/` + `ROADMAP.md` are frozen records and stay.
- **Verify:** build all bins; drive every binary × both flags against the `Cargo.toml` version;
  assert the version string appears nowhere in `src/`; generate completions into scratch dirs
  and count `--version` in all four outputs per shell; then `clippy -D warnings` and full
  `cargo test`. (Exact command in frontmatter `verify:` — no `temp_index.sh` wrapper: flag
  handling exits before any index is touched.)
- **Touches:** `src/cli.rs`, `tests/`.

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
