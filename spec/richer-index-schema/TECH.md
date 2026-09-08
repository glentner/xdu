---
slug: richer-index-schema
title: 'Richer index schema: owner, group, permissions, mtime, ctime'
kind: feature
appetite: big
status: in_progress
branch: feature/richer-index-schema
base: main
current_phase: P3
last_updated: '2026-09-08'
phases:
- id: P1
  name: Schema, version, and crawler record the five columns
  status: done
  satisfies:
  - R1
  - R2
  - R3
  depends_on: []
  parallel: false
  hammerable: false
  hill: crest
  verify: cargo test --lib && .agents/factory/bin/temp_index.sh sh -c 'test $(xdu-find
    --count) -eq 4'
- id: P2
  name: 'Lib filter core: NSS resolution, mode SPEC, QueryFilters'
  status: done
  satisfies:
  - R4
  - R5
  - R6
  - R7
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test --lib
- id: P3
  name: 'xdu-find surface: flags, wiring, csv/json, man pages'
  status: pending
  satisfies:
  - R4
  - R5
  - R6
  - R7
  - R8
  - R9
  depends_on:
  - P2
  parallel: false
  hammerable: false
  hill: uphill
  verify: .agents/factory/bin/temp_index.sh sh -c 'test $(xdu-find --owner $(id -un)
    --count) -eq 4 && ! xdu-find --owner xdu-no-such-user --count && test $(xdu-find
    --mode /400 --count) -eq 4 && test $(xdu-find --mtime-newer-than 1 --count) -eq
    4 && test $(xdu-find --mtime-older-than 30 --count) -eq 0 && xdu-find -f csv |
    head -1 | grep -q path,size,uid,gid,mode,atime,mtime,ctime && xdu-find -f json
    | grep -q uid && test $(xdu-find -f csv | grep -c .) -eq 5 && xdu-find --top 3'
- id: P4
  name: Refusal and compat pins, remaining docs, gate, ledger
  status: pending
  satisfies:
  - R3
  - R9
  depends_on:
  - P3
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo fmt --all -- --check && cargo clippy --all-targets --all-features
    -- -D warnings && cargo test
review:
  last_reviewed_commit: ''
  verdict: none
  blocked_reason: ''
  cycle: 0
---
# TECH.md — Richer index schema: owner, group, permissions, mtime, ctime

The **context engine and finite-state machine** for building this feature. The YAML
frontmatter above is the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/richer-index-schema/TECH.md`); the per-phase
checklists below are the work. `xdu-build` executes the next actionable phase, runs its
`verify:` command, updates state via
`uv run --with pyyaml python .agents/factory/bin/set_phase.py …`, and makes one atomic code+state commit.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R-IDs are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).
- **Backing research:** [`research/00-digest.md`](research/00-digest.md) + briefs (if `appetite: big`).

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

## Phase P1 — Schema, version, and crawler record the five columns
**Satisfies:** R1, R2, R3 · **Depends on:** —
**Goal:** A fresh crawl writes eight-column rows stamped format 2, and the existing
gate refuses anything else. R3's mechanism lands here; its explicit pin lands in P4.

- [x] `src/lib.rs`: `get_schema()` appends `uid`, `gid`, `mode`, `mtime`, `ctime`
  (`Int64`, non-null); `INDEX_FORMAT_VERSION = 2` with its doc comment rewritten
  for the eight-column layout.
- [x] `src/crawl.rs`: measurement helper returns all seven integers from the held
  `Metadata` (`uid()`, `gid()`, `mode() & 0o7777`, `mtime()`, `ctime()` beside
  size/atime); `PartitionBuffer` gains five `Int64Builder`s (private builders
  struct, not a wider tuple); `flush` extends the batch vector in schema order.
- [x] `src/bin/xdu.rs`: the single `buffer.add` call site passes the new values.
- [x] Unit tests move with the change: schema field pin, buffer round-trip over
  all eight columns, size-mode helper arity.
- Amendment (post-P1): column order revised to
  `path,size,uid,gid,mode,atime,mtime,ctime` — identity then times — per digest
  C5. Ordinal preservation across versions is moot (the gate refuses v1), and
  P3's csv/json header follows the same order.
- **Verify:** `cargo test --lib && .agents/factory/bin/temp_index.sh sh -c 'test $(xdu-find --count) -eq 4'` — unit pins plus a v2 index the current find already counts.
- **Touches:** `src/lib.rs`, `src/crawl.rs`, `src/bin/xdu.rs`.

## Phase P2 — Lib filter core: NSS resolution, mode SPEC, QueryFilters
**Satisfies:** R4, R5, R6, R7 · **Depends on:** P1
**Goal:** Resolution, parsing, and SQL fragments pinned by unit tests. CLI-observable
proof waits for P3's wiring; this phase delivers the lib contract that wiring calls.

- [x] `Cargo.toml`: `libc` moves to `[dependencies]`; no new crates.
- [x] `src/lib.rs`: `resolve_user` / `resolve_group` (POSIX lookup-first, digit
  fallback; documented not pool-safe); mode-SPEC parser (bare exact, `/` any,
  `&` all); `QueryFilters` fields, builders, `to_conditions` fragments, and
  `is_active` / `clear` / `format_display` coverage.
- [x] Unit tests: fragment pins, parse matrix with rejects, NSS policy (current
  user, unknown name, digit fallback, overflow).
- **Verify:** `cargo test --lib`.
- **Touches:** `Cargo.toml`, `src/lib.rs`.

## Phase P3 — xdu-find surface: flags, wiring, csv/json, man pages
**Satisfies:** R4, R5, R6, R7, R8, R9 · **Depends on:** P2
**Goal:** The filters answer on the real CLI; csv/json carry the new fields; the
man pages describe exactly the new surface.

- [ ] `src/cli.rs`: `--owner`, `--group`, `--mode`, `--mtime-older-than`,
  `--mtime-newer-than` on `XduFindArgs` only (long-only, no shorts).
- [ ] `src/bin/xdu-find.rs`: resolve-then-build wiring with pre-query failure
  (R5-shaped, empty stdout, non-zero); csv/json SELECT, header, keys, `row.get`.
- [ ] `doc/xdu-find.1.scd`: the five flags; `doc/xdu-rm.1.scd`: the "same filter
  options" sentence rewritten — same commit as the clap change.
- [ ] Integration tests: per-filter behavior on a fixture with known
  owners/modes/mtimes (expectations derived from `geteuid` at runtime, multi-user
  case self-skips with a named skip); unresolvable owner; csv header; json keys.
- **Verify:** the frontmatter drive — owner count, unresolvable refusal, mask,
  mtime pair, csv header, json key, csv line count, top — each asserting a
  post-condition, not exit 0.
- **Touches:** `src/cli.rs`, `src/bin/xdu-find.rs`, `doc/xdu-find.1.scd`,
  `doc/xdu-rm.1.scd`, `tests/`.

## Phase P4 — Refusal and compat pins, remaining docs, gate, ledger
**Satisfies:** R3, R9 · **Depends on:** P3
**Goal:** The contract's refusal and compatibility requirements pinned in the
durable suite; prose ground truth updated; the mirror gate green; every deferral
recorded or confirmed recorded.

- [ ] `tests/version_tests.rs`: `format=1` planted on a fresh index refuses in
  all three readers (the R3 pin the suite cannot currently catch regressing).
- [ ] R9 pins: fresh-index full-surface run (find path/size/atime/count/top, rm
  dry-run) with no version diagnostic; view covered by the shared gate's unit
  pin, stated here so review routes it to the orchestrator, not the blind
  reviewer.
- [ ] Docs: `doc/xdu.1.scd` column list, `README.md` schema table and DuckDB
  example, `AGENTS.md` plus `invariants.md` §1 (eight fields, version 2);
  re-count asserted literals on touched `.scd` pages; render every touched page
  and read the published text.
- [ ] Deferral ledger: walk P1–P3 bodies for deferred, follow-up, and known-
  limitation language; confirm each has its `issues/` plus `ROADMAP.md` entry.
  An unrecorded deferral fails this phase.
- **Verify:** `cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test`.
- **Touches:** `tests/`, `doc/`, `README.md`, `AGENTS.md`,
  `.agents/factory/invariants.md`.

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
