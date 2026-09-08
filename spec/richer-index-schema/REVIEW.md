# REVIEW — Richer index schema: owner, group, permissions, mtime, ctime

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** 09031e273f806ed000ac46aa0af55a67fc4305d6  ·  **Base:** main  ·  **Date:** 2026-09-08
- **Verdict:** changes-requested
- **Cycle:** 1 of ≤3 — mirrors `review.cycle` in `TECH.md` (escalate to human on non-convergence)

## Verification run

Blind correctness pass by a fresh subagent (full log in its return). Commands executed and outcomes:

- `cargo test` → all green (lib 81, view 5, crawl 23, find 9, offline 1, rm 18, version 6)
- `cargo clippy --all-targets --all-features -- -D warnings` → clean; `cargo fmt --all -- --check` → clean; `cargo build --locked --bins` → green
- Throwaway-index drives (`temp_index.sh`, never a real index): marker/csv/json dump with
  `uid/gid/mode` values cross-checked against `id -u`/`id -g` and a setuid fixture; owner/group
  positive and negative controls; unresolvable-name refusals; mode exact/any/all matrix plus invalid
  SPEC; mtime backdate matrix; `format=1` refusal in find, rm dry-run (file survives), and view
  pre-terminal; injection probes (`--owner "' OR '1'='1"`) failing closed; `xdu-view` under a pty
  entering and restoring the terminal cleanly
- Man pages: `scdoc` renders all three touched pages exit 0; published text read in full; CI literal
  gate replicated exactly → all OK (`xdu.1` 6/6 incl. `2x:.partial suffix`; `2x:XDU_INDEX` on
  find/view/rm; `2x:XDU_JOBS` on rm); no roff-control lines in `doc/*.scd`
- Hand-back: `git status --porcelain` empty (orchestrator re-verified); no build-state controls used

## Requirement → evidence matrix

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 | `src/crawl.rs::file_measurements` + `PartitionBuffer`, `src/lib.rs::get_schema` | csv dump cross-checked vs `id`, setuid masking, distinct mtime fixture | ✅ |
| R2 | `src/lib.rs::INDEX_FORMAT_VERSION = 2` | marker body reads `format=2` | ✅ |
| R3 | shared `lib::index_version_error`, called first in all three bins | `format=1` refusal: exit 1, empty stdout, diagnostic, rm unlinks nothing, view pre-terminal | ✅ |
| R4 | `resolve_user`/`resolve_group` + `QueryFilters` + `XduFindArgs` | name/id agreement, unrelated-id negative controls | ✅ |
| R5 | resolve-then-build wiring in `xdu-find` pre-query | exit 1, empty stdout, names the miss | ✅ |
| R6 | `parse_mode_spec`/`ModePredicate` + `with_mode` | exact/any/all matrix on chmodded fixture, invalid SPEC refusal | ✅ |
| R7 | `with_mtime_older/newer_than` | backdated fixture matrix | ✅ |
| R8 | csv/json arms in `src/bin/xdu-find.rs` | header bytes, 8-field rows, json keys on every row | ✅ |
| R9 | named projections unchanged; gate passes on v2 | full old surface green with empty stderr; view pty enter/restore | ✅ |

Unmapped changes (possible scope creep): none. Every non-test hunk maps to an R-ID or to
invariant §1's same-commit rule (`ChunkBuilders`, day-math refactors, README/man/AGENTS/invariants
updates, `libc` promotion). The committed `issues/broken-pipe-closed-stdout.md` + ROADMAP hunk is
docs-only with no code/gate/CLI surface — no interference, not graded.

## Findings

### [MEDIUM/CONFIRMED] F1 — `AGENTS.md` Project section still describes the three-column schema
- **Where:** `AGENTS.md:27` (binary table: "writes the Parquet index (path, size, atime)") and
  `AGENTS.md:33-34` ("deliberately minimal: `path` … `size` … `atime` …")
- **Failure scenario:** a future cycle reading the Project section as ground truth (the file
  declares itself the map) re-learns the old three-column contract, while the same diff moves the
  schema to eight columns and correctly updates the §1 gate text.
- **Evidence:** `grep -n "path, size, atime\|deliberately minimal" AGENTS.md` returns lines 27, 33;
  `git diff main...HEAD -- AGENTS.md` touches only the §1 hunk. Orchestrator re-verified both lines.
  Refutation attempted by reviewer (whether "minimal" could still read as intent) — rejected: the
  lines enumerate exactly three columns, now factually wrong.
- **Touches invariant / requirement:** operating-manual drift (rubric scope §5). MEDIUM, not HIGH:
  the gate text `xdu-plan`/`xdu-review` draw from (§1) is correct, so no gate degrades.
- **Remediation:** update both lines to the eight-column layout in `/xdu-build` (nearest home: the
  P4 docs phase). No R-ID or invariant behavior changes.

## Human-gate triggers

None. F1 is docs-only drift touching neither the high-blast-radius core nor a §4/§1/§2/§5
invariant behavior — no sign-off required before the remediation or a re-review.

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

Not run (not requested; cycle 1 scope is correctness).
