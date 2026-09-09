# REVIEW — All four binaries answer `--version`

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** 0181ac779c8efaf8ad206a8489e4f3eda8c8c226  ·  **Base:** main  ·  **Date:** 2026-09-09
- **Verdict:** changes-requested
- **Cycle:** 1 of ≤3 — mirrors `review.cycle` in `TECH.md` (escalate to human on non-convergence)
- **Mode:** full blind pass over the spec-excluded diff (no scoping to prior findings; first cycle).

## Verification run

Commands actually executed and their outcomes (the spine of the review):

- Blind reviewer: `git diff main...HEAD -- . ':(exclude)spec/'` → `src/cli.rs` (+4 `version,` lines),
  `tests/cli_version_tests.rs` (+37, new), `issues/version-flag-missing.md` (1-line status change). No
  `doc/*.scd` change — correct per GOAL non-goals.
- Blind reviewer: `cargo build --bins`, then all 8 binary×flag combos
  (`xdu`/`xdu-find`/`xdu-view`/`xdu-rm` × `--version`/`-V`) → each printed `<name> 0.5.0`, exit 0,
  empty stderr. `--help` on all four advertises `-V, --version`.
- Blind reviewer: `grep -m1 ^version Cargo.toml` → `0.5.0`; `grep -rnF "0.5.0" src/` → no hits.
- Blind reviewer: `gen-completions` into `mktemp -d` → all 8 outputs
  (`bash/xdu{,-find,-view,-rm}.bash`, `zsh/_xdu{,-find,-view,-rm}`) contain `--version`.
- Blind reviewer: `cargo test` → full suite green (81 lib + 5 xdu-view bin + 1 cli_version + 23 crawl
  + 9 find + 1 offline + 18 rm + 6 version, 0 failures);
  `cargo clippy --all-targets --all-features -- -D warnings` → clean;
  `cargo fmt --all -- --check` → clean.
- Orchestrator spot-checks (this session): rebuilt bins and re-drove all 8 combos → identical
  (`xdu 0.5.0`, etc., exit 0); `grep -rnF "0.5.0" src/` → no hits; `gen-completions` into scratch
  dir → all 4 bash + all 4 zsh outputs contain `--version`;
  `grep -rn "R[0-9]\|P[0-9]" tests/cli_version_tests.rs src/cli.rs` → no hits (§13 clean);
  new test uses `common::binary_path` (`CARGO_BIN_EXE_*`), not a re-declared resolver.
- Man-page render (`scdoc | mandoc | col -b`): **not observed** — no `doc/*.scd` hunk in the diff, so
  no render was run. Recorded as not observed, never as satisfied. The man pages' `-V, --version`
  lines (present on all four pages per reviewer inspection) are now true rather than aspirational.
- `ReportFindings`: skipped — no such tool in this harness; `REVIEW.md` is the durable record
  (per-skill portability fallback).

## Requirement → evidence matrix

Bidirectional traceability. Flag requirements with no implementing change **and** changes that map
to no requirement (scope creep).

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 — any of the four binaries with `--version`/`-V` prints version, exit 0 | `src/cli.rs` bare `version` in all four `#[command]` blocks (:14, :68, :152, :202) | Built bins, drove all 8 combos (reviewer + orchestrator re-drive): exit 0, `<name> 0.5.0` on stdout; `cargo test --test cli_version_tests` → 1 passed | ✅ |
| R2 — printed version agrees with `Cargo.toml`, no literal in `src/` | Same four lines (clap derives from `CARGO_PKG_VERSION`); test holds `env!("CARGO_PKG_VERSION")`, no literal | `grep -m1 ^version Cargo.toml` → `0.5.0` vs all 8 drives; `grep -rnF "0.5.0" src/` → empty | ✅ |
| R3 — generated completions offer `--version` for each binary | By construction from the same `Command` objects (no code change needed) | `gen-completions` into scratch dir; all 8 bash+zsh outputs contain `--version` (reviewer + orchestrator) | ✅ |

Unmapped changes (possible scope creep): `issues/version-flag-missing.md` status line
`unshaped` → `shaped on fix/version-flag-missing (2026-09-09)`. Maps to no R-ID but is process
bookkeeping, one line, behavior-free. Benign — noted, not a finding. No other unmapped hunks; no
`doc/*.scd`, `share/`, or release-mechanics changes (per non-goals).

## Findings

Severity: **CRITICAL** (any `invariants.md` §1–§12 violation is auto-CRITICAL, **including lettered
subsections** such as §2b/§2c; a §13 project-conventions violation is **HIGH**) · **HIGH** · **MEDIUM** · **LOW**. Verdict: **CONFIRMED**
(reproduced) vs **PLAUSIBLE** (suspected, needs human triage). Only CONFIRMED findings auto-loop to
`xdu-build`.

### [HIGH/CONFIRMED] Operating-manual drift: AGENTS.md + invariants.md still state the flag does not exist

- **Where:** `AGENTS.md:51-55`, `.agents/factory/invariants.md:195-199`
- **Failure scenario:** both sentences state no `#[command(...)]` in `src/cli.rs` sets `version`, so
  all four binaries reject `-V`/`--version`, pointing at `issues/version-flag-missing.md` as a live
  defect. After this diff merges, all four blocks set `version` and all 8 combos exit 0 — the text
  describes the code wrongly. A future `xdu-plan` gate or reviewer reading the invariant checklist
  takes false ground truth about the CLI surface (re-diagnosing a fixed defect).
- **Evidence:** `grep -n "flag does not exist" AGENTS.md .agents/factory/invariants.md` → hits at
  `AGENTS.md:53`, `invariants.md:196`; spec-excluded diff shows `+    version,` in all four blocks;
  orchestrator re-drive confirms `xdu --version` → `xdu 0.5.0`, exit 0 (all 8 combos identical).
  Refutation considered downgrading to MEDIUM as "merely wrong description", but the rubric rates
  stale text in `invariants.md` / an `AGENTS.md` load-bearing invariant as gate-degrading HIGH. Kept
  at HIGH with the qualifier that it is **not auto-CRITICAL** (§13 drift; no §§1–12 safety gate, no
  high-blast-radius behavior affected — the `src/cli.rs` change itself is correct).
- **Touches invariant / requirement:** invariants §13 (project conventions, version single-source) and
  §10 (CLI single source of truth, same-commit map ownership); rubric scope item 5 (operating-manual
  drift). `AGENTS.md` opens by declaring the code ground truth ("fix this file") — a diff that moves
  the code owns the map.
- **Fix:** reword both sentences to state the flag exists and is derived from `Cargo.toml` (all four
  `#[command(...)]` blocks set `version`; the `version-flag-missing` defect is fixed). Not a
  `doc/*.scd` change, so GOAL's no-docs non-goal is unaffected. Alternatively defer to `/xdu-harness`
  as manual maintenance — but it must not silently persist.

Dropped candidates (investigated, disproved, silent by default — listed here for the record):
`xdu-rm` `-v`/`-V` collision (disproved: `--help` lists `-v, --verbose` and `-V, --version`
distinctly; `xdu-rm -V` → `xdu-rm 0.5.0` exit 0); hardcoded version literal (disproved: no hits in
`src/`); `gen-completions` gaining `--version` (correctly still rejects it, per non-goal); stale
test-binary footgun (not repeated: test uses `common::binary_path`); `R#`/`P#` ids in source (none).

## Human-gate triggers

Set if any CONFIRMED finding touches the high-blast-radius core (`src/bin/xdu-rm.rs`,
`src/bin/xdu.rs`, `src/crawl.rs`, `src/lib.rs`, `src/cli.rs`) or a destructive-rm / schema-stability /
atomic-write / SQL-injection invariant — these **always** require human sign-off before
`xdu-publish`, regardless of auto-loop. (`invariants.md`'s *High-blast-radius files* header is the
authoritative path list; this copy may only ever **widen** to match it.)

- Not triggered. The single CONFIRMED finding is §13 drift located in `AGENTS.md` /
  `.agents/factory/invariants.md` — not in the high-blast-radius core, and not a §1/§2/§4/§5
  invariant. The `src/cli.rs` change itself is correct and carries no behavior finding.

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

- Not run — `completeness` was not requested.

## Review cycle 2 — approved (2026-09-09)

- **Reviewed commit:** af1ac9f5c66c46ed32e4564141e8d9994d563b93 · **Base:** main · **Date:** 2026-09-09
- **Verdict:** approved
- **Cycle:** 2 of ≤3 — mirrors `review.cycle` in `TECH.md`
- **Mode:** full blind pass over the spec-excluded diff (not scoped to prior findings).
  Contract-drift check: `git log --oneline main..HEAD -- spec/version-flag-missing/GOAL.md`
  shows only the shaping commit `47f473a` — the locked contract did not move mid-build.

### Verification run

Commands actually executed and their outcomes:

- Blind reviewer: `git diff main...HEAD -- . ':(exclude)spec/'` → `src/cli.rs` (+4 `version,`
  lines), `tests/cli_version_tests.rs` (+37, new), `AGENTS.md` + `invariants.md`
  (F1 remediation prose flip), `issues/version-flag-missing.md` (1-line status change).
  No `doc/*.scd` change — correct per GOAL non-goals.
- Blind reviewer: `cargo build --bins`, then all 8 binary×flag combos → each printed
  `<name> 0.5.0`, exit 0, one `-V, --version` line in each `--help`; `grep '^version'
  Cargo.toml` → `0.5.0`; `grep -rnF '0.5.0' src/` and a general semver-literal grep → none;
  `gen-completions` into `mktemp -d` → all 8 bash+zsh outputs contain `--version` exactly
  once; `cargo test` full suite green; `clippy -D warnings` clean; `fmt --check` clean.
  Dropped after refutation: `xdu-rm -v`/`-V` collision, `gen-completions` gaining
  `--version`, hardcoded literal, `R#`/`P#` in source, stale-binary hygiene.
- Orchestrator spot-checks (this session): `git status --porcelain` → clean on hand-back;
  rebuilt bins and re-drove all 8 combos → identical (`xdu 0.5.0`, etc., exit 0);
  `grep -rnF "0.5.0" src/` → empty; `grep -rn "R[0-9]\|P[0-9]" src/
  tests/cli_version_tests.rs` → none; `gen-completions` into scratch dir → all 4 bash +
  all 4 zsh outputs contain `--version`; `grep -rn "flag does not exist" AGENTS.md
  .agents/factory/invariants.md` → no hits (drift closed); `cargo test --test
  cli_version_tests` → 1 passed.
- Man-page render (`scdoc | mandoc | col -b`): **not observed** — no `doc/*.scd` hunk in
  the diff, so no render was run. Recorded as not observed, never as satisfied.
- `ReportFindings`: skipped — no such tool in this harness; `REVIEW.md` is the durable
  record (per-skill portability fallback).

### Requirement → evidence matrix

All R-IDs owned by the blind reviewer (no artifact-deliverable R-IDs on this fix —
every requirement has runnable evidence in the spec-excluded diff). Orchestrator
spot-checked each line.

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 — any of the four binaries with `--version`/`-V` prints version, exit 0 | `src/cli.rs` bare `version` in all four `#[command]` blocks (:14, :68, :152, :202) | Built bins, drove all 8 combos (reviewer + orchestrator re-drive): exit 0, `<name> 0.5.0` on stdout; `cargo test --test cli_version_tests` → 1 passed | ✅ |
| R2 — printed version agrees with `Cargo.toml`, no literal in `src/` | Same four lines (clap derives from `CARGO_PKG_VERSION`); test holds `env!("CARGO_PKG_VERSION")`, no literal | `grep -m1 ^version Cargo.toml` → `0.5.0` vs all 8 drives; `grep -rnF "0.5.0" src/` → empty (reviewer + orchestrator) | ✅ |
| R3 — generated completions offer `--version` for each binary | By construction from the same `Command` objects (no code change needed) | `gen-completions` into scratch dir; all 8 bash+zsh outputs contain `--version` (reviewer + orchestrator) | ✅ |

Unmapped changes (possible scope creep): `AGENTS.md` + `invariants.md` prose flip is the
cycle-1 F1 remediation required by rubric scope item 5 — justified, not creep.
`issues/version-flag-missing.md` status line is process bookkeeping, one line,
behavior-free — benign. No other unmapped hunks; no `doc/*.scd`, `share/`, or
release-mechanics changes (per non-goals).

### Findings

No CONFIRMED or PLAUSIBLE findings. Silence on a clean diff is a valid result.

Cycle-1 F1 (HIGH/CONFIRMED operating-manual drift): **CLOSED**. The stale
`flag does not exist` sentences are gone from `AGENTS.md:51-53` and
`invariants.md:195-197`; current text states all four blocks set `version` and clap
derives `-V`/`--version` — matching the code.

### Human-gate triggers

- Not triggered. No CONFIRMED finding touches the high-blast-radius core or a
  §1/§2/§4/§5 invariant. The `src/cli.rs` change itself is correct and carries no
  behavior finding.
