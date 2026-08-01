# REVIEW — Make `xdu-rm --limit` selection deterministic

> Adversarial QA by `xdu-review` (debate variant + completeness sub-pass). Two **independent, blind**
> correctness reviewers (ship-advocate + block-advocate) graded the branch diff against
> [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — neither read `PLAN.md`/`TECH.md`/
> `META.md`. Every finding/claim cites an **executed** command. A separate completeness reviewer
> (allowed `TECH.md`) checked phases-shipped / scope.

- **Reviewed commit:** 77e257d0520fd0e46dda06bc6f5e267478fe6213  ·  **Base:** main  ·  **Date:** 2026-07-31
- **Verdict:** approved
- **Cycle:** 1 of ≤3
- **Mode:** blind debate (2 correctness reviewers) + isolated completeness pass

## Verification run

Commands executed by the reviewers (the spine of the review):

- `cargo test` → all pass: 16 `rm_tests` (incl. new `test_limit_deterministic_selection`), 11
  `crawl_tests`, lib unit tests (incl. new `test_deterministic_limit_clause_{none,some}`).
- `cargo fmt --all -- --check` → clean (exit 0).
- `cargo clippy --all-targets --all-features -- -D warnings` → clean (exit 0).
- `.agents/factory/bin/temp_index.sh sh -c '... xdu-rm --limit 3 --dry-run; xdu-rm --limit 3 --dry-run'`
  (throwaway index, files across partitions `alice`/`bob`/`__root__`) → the two dry-runs produced
  **byte-identical** output.
- `... xdu-rm --limit 3 --dry-run` then `... xdu-rm --limit 3 --force` on the same index → `Deleted: 3`;
  the three files deleted were **exactly** the three previewed; only the larger-path survivors remained.
- Boundary/combination drives: `--limit 0` → `No matching files found.`; `--limit 10` (> count) → all
  matches; `-p alice --limit 1` → single global-smallest matching path; `--limit 2 --safe --force` →
  the two smallest-path files deleted, rest retained.

## Requirement → evidence matrix

| R-ID | Implemented by | Verified how (executed) | Status |
|------|----------------|-------------------------|--------|
| R1 — repeated `--limit N` selects same set | `lib.rs::deterministic_limit_clause` (`ORDER BY path`) wired in `xdu-rm.rs` | two identical `--limit 3 --dry-run` runs → byte-identical output; unit + integration tests pass | ✅ |
| R2 — dry-run set == real deletion set | same ordered query feeds both the dry-run branch and the delete path (`xdu-rm.rs`) | dry-run previewed 3 paths; `--force` deleted exactly those 3 (checked on disk) | ✅ |
| R3 — N lexicographically-smallest paths | `ORDER BY path LIMIT n` over the union glob (global order across partitions) | `--limit 3` correctly spanned `alice`+`bob` and excluded `root.dat` (path `.../bob/…` < `.../root.dat`); `path` is unique → no ties | ✅ |
| R4 — no `--limit` unchanged | `deterministic_limit_clause(None) == ""` (identical to prior `String::new()`) | no-limit drive → all matches deleted; unit test `_none`; no `ORDER BY` emitted on the unlimited path | ✅ |

Unmapped changes (possible scope creep): **none** — the four touched files (`src/lib.rs`,
`src/bin/xdu-rm.rs`, `tests/rm_tests.rs`, `doc/xdu-rm.1.scd`) all map to R1–R4.

## Findings

**None.** Both independent blind reviewers reproduced every R-ID with executed drives and found no
correctness bug, no R-ID gap, no invariant violation, and no scope creep. Every candidate concern
dissolved under the refutation protocol.

Invariants checked against the touched subsystems:
- **§4 (rm destructive safety):** the diff *satisfies* the previously-unmet mandate "any deletion
  combined with `--limit` MUST carry a deterministic `ORDER BY`." Confirmed by R1/R2. The
  dry-run / confirm / `--force` / `--safe` gates are untouched.
- **§5 (SQL injection):** `LIMIT {n}` interpolates a type-safe `usize`; `ORDER BY path` is a literal
  column — no new user-controlled SQL.
- **§1 / §10 / §11 / §13:** schema `SELECT` unchanged; no `src/cli.rs` flag change and the man page
  updated in the same commit; selection logic lives in `lib` with unit tests; fmt/clippy/test clean
  and no `R#`/`P#` spec ids in source comments.

## Human-gate triggers

The rubric's mandatory human sign-off gate fires when a **CONFIRMED finding** touches the
high-blast-radius core or a §4/§1/§2/§5 invariant. **There are zero CONFIRMED findings, so the gate
is not triggered.** (The diff does touch `src/bin/xdu-rm.rs` + `src/lib.rs`; `/xdu-publish` is itself
a human-initiated step, so a human reviews the squash PR before it lands regardless.)

## Completeness sub-pass (separate reviewer; read TECH.md)

**COMPLETE.** The single planned phase P1 is fully shipped exactly as its checklist and "Touches"
describe; all four R-IDs have a corresponding change plus test; scope stayed bounded to the four
declared files (+81/−7); no non-goal was touched (xdu-find ordering, a user sort flag, `--safe`
re-stat coverage, and size/age prioritization all confirmed untouched).
