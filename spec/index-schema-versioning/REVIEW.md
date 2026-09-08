# REVIEW — On-disk index schema versioning

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** 1e9bfe2a49e1f9c165687e9edb594e1dbd28b271  ·  **Base:** `main`  ·  **Date:** 2026-09-07
- **Verdict:** changes-requested
- **Cycle:** 1 of ≤3 — mirrors `review.cycle` in `TECH.md` (escalate to human on non-convergence)

## Verification run

Commands actually executed and their outcomes (the spine of the review).
The correctness pass ran in a fresh blind subagent; the orchestrator sanity-checked
file-content claims by direct read and left the tree otherwise untouched.

- Blind subagent, spec-excluded diff: `git diff main...HEAD -- . ':(exclude)spec/'` → 8 product
  files (`src/lib.rs`, `src/crawl.rs`, the three reader bins, `tests/common/mod.rs`,
  `tests/crawl_tests.rs`, new `tests/version_tests.rs`) plus the factory process stamp on
  `issues/index-schema-versioning.md`. No `doc/*.scd` touched, so the man-page render gate has
  no new inputs from this branch (orchestrator confirms by name list; render not re-run).
- `cargo test` (blind reviewer) → green: 74 lib + 5 view + 23 crawl + 1 offline + 18 rm +
  4 version tests, 0 failures. Orchestrator did not independently re-run the suite.
- `cargo clippy --all-targets --all-features -- -D warnings` (blind reviewer) → clean.
  `cargo fmt --all -- --check` (blind reviewer) → clean.
- Throwaway-index drives via `.agents/factory/bin/temp_index.sh` (blind reviewer, all executed):
  fresh index shows `format=1` in `.xdu-complete`; `format=999` marker makes find/rm/view each
  exit 1 naming `999` and the supported `1` with a re-index remedy and zero stdout bytes
  (csv/json formats included); removed, versionless (`xdu=test/errors=0`), and garbage
  (`format=new`) markers each refuse with the no-version re-index diagnostic; FIFO and
  over-`MARKER_READ_LIMIT` markers refuse fast instead of blocking or loading; refused
  `xdu-rm --force` (real run) unlinks nothing; versioned marker with `errors=2` still queries
  exit 0 with only the soft `--allow-errors` warning.
- Contract-drift check (orchestrator): `git log --oneline main..HEAD -- spec/index-schema-versioning/GOAL.md`
  → only the shaping commit `deadfb7`. The locked contract did not move mid-build.
- Tree-clean check (orchestrator): `git status --porcelain` empty before delegation and after
  hand-back. The reviewer reports no negative-control build-state mutation, so no restore was owed.
- Drift-claim check (orchestrator, by direct read): `AGENTS.md:405` still states "There is **no
  on-disk schema version**" and `AGENTS.md:360-361` enumerates the marker body without `format`;
  `.agents/factory/invariants.md:32` and `:64-65` carry the same two texts. Both confirmed present.

No artifact-deliverable R-IDs exist in this goal (R1–R4 are all behavioral), so the blind
reviewer owned the full matrix; nothing was routed to the orchestrator.

## Requirement → evidence matrix

Bidirectional traceability, graded by the blind reviewer with executed evidence.

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 — completed runs record the format version | `src/lib.rs` (`INDEX_FORMAT_VERSION: u32 = 1`); `src/crawl.rs::completion_marker_contents` appends `format={INDEX_FORMAT_VERSION}` | Throwaway index shows `format=1` in `.xdu-complete`; writer↔parser pin test asserts the `format=` line | ✅ |
| R2 — unknown version refuses in find/view/rm, naming found + supported, no rows / no deletion | `index_version_error` gate at the top of `src/bin/xdu-find.rs`, `src/bin/xdu-rm.rs`, `src/bin/xdu-view.rs` (pre-terminal) | `format=999` marker → all three exit 1 with "has index format version 999, but this xdu supports version 1; re-index"; find stdout 0 bytes (default/csv/json); `xdu-rm --force` leaves targets in place | ✅ |
| R3 — no / versionless / unparseable version refuses with re-index direction, never reads blind | `index_version_error` maps `Absent \| Unreadable` and `completion_marker_format == None` to the no-version refusal; guarded `read_completion_marker` (stat-once, non-file/oversize never opened) | Removed, versionless, and `format=new` markers each → exit 1, empty stdout, re-index diagnostic; FIFO/oversize/directory markers refuse (FIFO + oversize driven live) | ✅ |
| R4 — fresh index accepted by all three, no version diagnostic; tolerated-errors warning unchanged | Same gates pass through on `format=1`; `index_completion_warning` path untouched | Fresh index: find `--count` exit 0 with clean stderr, rm `--dry-run --force` lists the full set, view passes the gate (fails only at TTY setup); versioned `errors=2` marker still warns soft and queries | ✅ |

Unmapped changes (possible scope creep): none in product code. Every product hunk maps to R1–R4
(the `run_view` helper and `version_tests.rs` exist to drive the R2/R3/R4 paths; the
`crawl_tests.rs` expectation updates follow from the accepted R3 flag day). The single
non-product hunk (`issues/index-schema-versioning.md` status stamp) is a factory process marker.

## Findings

Severity: **CRITICAL** (any `invariants.md` §1–§12 violation is auto-CRITICAL, **including lettered
subsections** such as §2b/§2c; a §13 project-conventions violation is **HIGH**) · **HIGH** · **MEDIUM** · **LOW**. Verdict: **CONFIRMED**
(reproduced) vs **PLAUSIBLE** (suspected, needs human triage). Only CONFIRMED findings auto-loop to
`xdu-build`.

### [HIGH/CONFIRMED] Operating-manual drift: "no on-disk schema version" is now false

- **Where:** `AGENTS.md:405` (§ *Load-bearing invariants*, item 1); `.agents/factory/invariants.md:32` (§1)
- **Failure scenario:** a later cycle reads the gate "There is **no on-disk schema version**" and
  designs schema work (e.g. `issues/richer-index-schema.md`) as if versioning were still the missing
  prerequisite, or re-adds a version mechanism that already exists as the `format=` marker key.
- **Evidence:** direct read by the orchestrator — both lines confirmed present after a diff that
  introduces `INDEX_FORMAT_VERSION` and writes `format=1` into every completion marker (writer
  output observed live on a throwaway index by the blind reviewer).
- **Touches invariant / requirement:** rubric scope item 5 (operating-manual drift); HIGH per the
  rubric (stale text in `invariants.md` and in an `AGENTS.md` load-bearing invariant degrades the gate
  `xdu-plan` and the next `xdu-review` draw from). Not a §1 code violation — the code is correct;
  the map is stale. `AGENTS.md` opens by declaring the code ground truth ("fix this file"), so the
  branch that moved the code owns the map.
- **Remedy:** restate both sentences around the marker-carried version (version 1 names the current
  three-column layout; readers refuse what they do not understand), keeping the warning that changing
  `get_schema()` or a reader column list remains a breaking cross-cutting change.

### [HIGH/CONFIRMED] Operating-manual drift: marker-body key lists omit the new `format` key

- **Where:** `AGENTS.md:360-361` (Architecture, "Body is `key=value` lines (`xdu`, …,
  `lossy_paths`)"); `.agents/factory/invariants.md:64-65` (§2b, same enumeration)
- **Failure scenario:** both lists enumerate the body exhaustively, so the omission reads as "no such
  key" to the next author touching the marker — inviting a duplicate version field or a parse of the
  body that does not expect `format`.
- **Evidence:** direct read by the orchestrator — both enumerations confirmed without `format`
  against a writer that now emits seven `key=value` lines (observed live by the blind reviewer).
- **Touches invariant / requirement:** rubric scope item 5; HIGH for the same gate-degradation reason
  as above (§2b text feeds the marker-ordering gate).
- **Remedy:** add `format` to both enumerations in the same commit, noting it is the run-level index
  format version readers gate on.

## Human-gate triggers

Set if any CONFIRMED finding touches the high-blast-radius core (`src/bin/xdu-rm.rs`,
`src/bin/xdu.rs`, `src/crawl.rs`, `src/lib.rs`, `src/cli.rs`) or a destructive-rm / schema-stability /
atomic-write / SQL-injection invariant — these **always** require human sign-off before
`xdu-publish`, regardless of auto-loop. (`invariants.md`'s *High-blast-radius files* header is the
authoritative path list; this copy may only ever **widen** to match it.)

- Not triggered. Both CONFIRMED findings touch only `AGENTS.md` and
  `.agents/factory/invariants.md` (documentation drift, HIGH — not an invariant violation), and the
  blind pass reports no correctness, R-ID, invariant, or scope-creep finding against the product
  diff, including the high-blast-radius files the diff touches (`src/lib.rs`, `src/crawl.rs`,
  `src/bin/xdu-rm.rs`).

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

- Not run — no `completeness` argument was given. Noted for the record: all three TECH phases
  report `done` and the blind reviewer maps every product hunk to an R-ID with no scope ballooning
  observed, but that cross-check was not executed as a separate pass.

## Review cycle 2 — approved (2026-09-08)

Mode: fresh full blind pass over the whole spec-excluded diff (not a scoped
remediation check). Reviewed commit `0aef8fcb6a1146a228c74f5eb441c50f22fd8d35`, base
`main`. Contract-drift check: `git log --oneline main..HEAD --
spec/index-schema-versioning/GOAL.md` → only the shaping commit `deadfb7`; the locked
contract did not move.

### Verification run

- Blind subagent, spec-excluded diff: `git diff main...HEAD -- . ':(exclude)spec/'` →
  product hunks in `src/lib.rs`, `src/crawl.rs`, the three reader bins,
  `tests/common/mod.rs`, `tests/crawl_tests.rs`, new `tests/version_tests.rs`, plus the
  P4 manual remediation in `AGENTS.md` / `.agents/factory/invariants.md` and the factory
  process stamp on `issues/index-schema-versioning.md`. No `doc/*.scd` touched, so the
  man-page render gate has no new inputs (reviewer confirms no CLI change; render not
  applicable, not skipped).
- `cargo test` (blind reviewer) → green: 74 lib + 23 crawl + 4 version + 18 rm + 5 + 1,
  0 failures. `cargo clippy --all-targets --all-features -- -D warnings` → clean.
  `cargo fmt --all -- --check` → clean.
- Throwaway-index drives via `.agents/factory/bin/temp_index.sh` (blind reviewer, all
  executed): fresh index marker carries `format=1`; `format=999` makes find/rm/view each
  exit 1 naming `999` and supported `1` with a re-index remedy and empty stdout;
  removed, versionless, and garbage (`format=new`) markers each refuse with the
  no-version re-index diagnostic; FIFO and directory-at-marker-path refuse fast; refused
  `xdu-rm --force` unlinks nothing; versioned `format=1/errors=2` marker still queries
  exit 0 with only the soft `--allow-errors` warning.
- P4 remediation check (orchestrator, by direct read + `grep`): the old "no on-disk
  schema version" sentence is absent from `AGENTS.md`, `invariants.md`, and `src/`; both
  marker-body key lists now carry `format`; both files name `INDEX_FORMAT_VERSION` and
  the `index_version_error`-before-`index_completion_warning` refusal order.
- Tree-clean check (orchestrator): `git status --porcelain` empty before delegation and
  after hand-back. The reviewer reports no negative-control build-state mutation, so no
  restore was owed.
- Gates not observed in this session (recorded, not claimed): the CI rollup state and a
  `scdoc` render were not re-run here — no `.scd` is in the diff, so neither gates this
  verdict.

No artifact-deliverable R-IDs exist in this goal (R1–R4 are all behavioral), so the blind
reviewer owned the full matrix; nothing was routed to the orchestrator.

### Requirement → evidence matrix (blind reviewer, executed evidence)

| R-ID | Implemented by | Verified how | Status |
|------|----------------|--------------|--------|
| R1 — completed runs record the format version | `src/lib.rs` (`INDEX_FORMAT_VERSION = 1`); `src/crawl.rs::completion_marker_contents` appends `format=` | Throwaway index shows `format=1`; writer↔parser pin test asserts the line | ✅ |
| R2 — unknown version refuses in find/view/rm, naming found + supported, no rows / no deletion | `index_version_error` gate at the top of the three reader bins | `format=999` → all three exit 1 with found + supported + remedy, empty stdout; refused `xdu-rm` leaves targets in place | ✅ |
| R3 — no / versionless / unparseable version refuses with re-index direction, never reads blind | `Absent \| Unreadable` and `completion_marker_format == None` map to the no-version refusal; guarded single-stat read | Removed, versionless, `format=new`, FIFO, and directory markers each refuse exit 1 with empty stdout and a re-index diagnostic | ✅ |
| R4 — fresh index accepted by all three, no version diagnostic; tolerated-errors warning unchanged | Same gates pass through on `format=1`; `index_completion_warning` path runs after the gate | Fresh index queries exit 0 with clean stderr; `format=1/errors=2` marker still warns soft and queries | ✅ |

Unmapped changes (possible scope creep): none. Every product hunk maps to R1–R4; no
`R#`/`P#` ids in `src/`; `doc/*.scd` correctly untouched (no CLI change).

### Findings

No findings — nothing CONFIRMED, nothing PLAUSIBLE. Candidates investigated and dropped
per the refutation protocol: Absent/Unreadable/garbage markers using the no-version
diagnostic (correct fail-closed per R3); a `stat`-then-`read` TOCTOU (pre-existing shape
from `main`, speculative); a stale doc comment on `index_completion_warning`
(`src/lib.rs:161-163`) — source comment, not manual drift, out of rubric scope.

### Human-gate triggers

- Not triggered. No CONFIRMED finding exists, and the blind pass reports no correctness,
  R-ID, invariant, or scope-creep finding against the product diff, including the
  high-blast-radius files the diff touches (`src/lib.rs`, `src/crawl.rs`,
  `src/bin/xdu-rm.rs`).
