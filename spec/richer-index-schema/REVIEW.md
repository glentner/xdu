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

## Review cycle 2 — approved (2026-09-08)

- **Reviewed commit:** 8589074d8308d6923b28865369dc76a38efd1113 · **Base:** main
- **Mode:** fresh full blind pass over the full spec-excluded diff
  (`git diff main...HEAD -- . ':(exclude)spec/'`), not a scoped remediation check.
- **Contract-drift check:** `git log --oneline main..HEAD -- spec/richer-index-schema/GOAL.md`
  returns only the original shaping commit `c3faf0c` — the locked contract did not move mid-build.
- **Artifact-deliverable routing:** none excluded. Every R-ID R1–R9 is CLI-observable under the
  one-line test, so the blind reviewer owned all nine. The orchestrator additionally verified the
  cycle-1 F1 remediation (AGENTS.md prose) and the drift check above.
- **Verdict:** approved. No CONFIRMED findings, no PLAUSIBLE findings. Cycle 2 of ≤3.

### Verification run (blind reviewer, executed commands)

- `cargo test` → 143 passed / 0 failed (lib, view, crawl, find 9/9, offline, rm, version 6/6).
- `cargo clippy --all-targets --all-features -- -D warnings` → clean.
  `cargo fmt --all -- --check` → clean. `cargo build --release --locked --bins` → clean.
- Throwaway-index drives via `.agents/factory/bin/temp_index.sh` (never a real index): R1 csv
  dump cross-checked field-by-field against `os.stat` (`R1_EXACT_OK`); marker reads `format=2`;
  forged `format=1` refusal in find (exit 1, 0 stdout bytes), rm dry-run (file kept), and view
  pre-terminal; owner/group name/id agreement plus `4294967294` negative controls; unresolvable
  names refuse with empty stdout; mode exact/any/all matrix plus invalid-SPEC refusals; mtime
  backdate matrix (2020-01-01 fixture); csv header plus 8-field rows, json keys on every row,
  path format unchanged; fresh-v2 full old surface green with no version diagnostic.
- Injection probe (`--owner "' OR '1'='1"`) fails closed (refusal, empty stdout).
- Man pages: `scdoc` renders all four `doc/*.scd` pages exit 0; published text read in full.
  CI-matching literal checks (whitespace-stripped, counting where duplicated): `xdu.1` presence
  `OUTDIR/<partition>/<chunk>.parquet`, `OUTDIR/*/*.parquet`, `OUTDIR/.xdu-complete`, `__root__`,
  `st_blocks * 512` all PRESENT_OK plus `2x:.partial suffix` COUNT_OK (2); `xdu-find.1`
  `2x:XDU_INDEX` COUNT_OK (2); `xdu-rm.1` `2x:XDU_INDEX` and `2x:XDU_JOBS` COUNT_OK (2); no
  roff-control line starts. New literals (`644`, `--mtime-older-than`, `world-writable`) verified
  by reading, not in CI's hand-maintained list. `gen-completions` emits all five new flags.
- Hand-back: `git status --porcelain` empty (orchestrator re-verified); no negative-control build
  mutation, so no `target/` restore owed. One self-inflicted throwaway-site `chmod` was repaired
  inside its own `mktemp -d` scratch only.
- **Not observed:** CI rollup / `gh` status (this session has no `gh`). Recorded as not observed,
  never as satisfied.

### Requirement → evidence matrix (cycle 2)

| R-ID | Status | Verified how (who) |
|------|--------|--------------------|
| R1 | ✅ | csv dump vs `os.stat`, chunk-boundary unit test (blind reviewer) |
| R2 | ✅ | marker body `format=2` on fresh index (blind reviewer) |
| R3 | ✅ | forged `format=1` refusal in all three readers, rm unlinks nothing (blind reviewer) |
| R4 | ✅ | owner/group name/id agreement, unrelated-id negatives (blind reviewer) |
| R5 | ✅ | unknown user/group refuse exit 1, empty stdout (blind reviewer) |
| R6 | ✅ | exact/any/all matrix on chmodded fixtures, invalid SPEC refusal (blind reviewer) |
| R7 | ✅ | backdated-mtime matrix (blind reviewer) |
| R8 | ✅ | csv header plus 8-field rows, json keys, path format (blind reviewer) |
| R9 | ✅ | full old surface green with empty version-diagnostic stderr; view gate pre-terminal (blind reviewer) |

Cycle-1 matrix remains accurate; no R-ID changed between `09031e2` and `8589074` except the docs
remediation below. Unmapped changes: none — every non-test hunk maps to an R-ID or the §10/§13
same-commit rule; the `issues/broken-pipe-closed-stdout.md` plus ROADMAP hunk is docs-only.

### Findings

None. Two candidates pursued through refutation and dropped: `--count` ignoring `--limit`
(pre-existing on `main`, untouched by this diff); `-V`/`--version` documented but absent
(pre-existing recorded defect `issues/version-flag-missing.md`, untouched). Neither is in GOAL
scope.

Cycle-1 F1 (MEDIUM/CONFIRMED, AGENTS.md Project section describing the three-column schema) is
remediated: `git diff main...HEAD -- AGENTS.md` now carries the eight-column layout in both the
binary-table line and the schema paragraph plus the §1 gate text, and the stale three-column
strings are absent. Orchestrator re-verified by grep.

### Human-gate triggers

None. No CONFIRMED finding touches the high-blast-radius core or a §4/§1/§2/§5 invariant.
