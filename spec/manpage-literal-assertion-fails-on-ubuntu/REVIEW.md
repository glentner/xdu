# REVIEW — The man-page literal gate asserts content, not layout

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** 82a0a4b1ce12c3357a263fe2947d5b09160245e1  ·  **Base:** main  ·  **Date:** 2026-08-14
- **Verdict:** changes-requested
- **Cycle:** 1 of ≤3 — mirrors `review.cycle` in `TECH.md`

**Contract-drift check:** `git log --oneline main..HEAD -- spec/{slug}/GOAL.md` → `4c1e2d1` only (the
shaping commit). The contract did not move mid-build; this review grades the GOAL as locked.

**Blindness:** the correctness pass ran as a fresh `general-purpose` subagent given `GOAL.md` inline,
`invariants.md`, `review-rubric.md`, and the diff produced with `':(exclude)spec/'` throughout. It
opened no file under `spec/` — including the author's `verify/*.sh` harness and `EVIDENCE.md`, so its
matrix below is measured independently rather than transcribed. It disclosed one incidental leak: a
repo-wide `grep -rn` printed matching *lines* from `spec/` files in its results; it did not open them
and no verdict rests on them. Judged not to compromise the pass.

## Verification run

Commands actually executed and their outcomes (the spine of the review). No Rust changed on this
branch, so `cargo test`/`clippy`/`fmt` were not re-run — CI runs them on the PR regardless.

- **The real CI toolchain, in a container** — `git archive HEAD` into `ubuntu:24.04`, then CI's exact
  `apt-get install -y scdoc mandoc bsdextrautils` → **scdoc 1.11.2-1, mandoc 1.14.6-1**. Ran the
  `Render man pages` step, then the assertion step body extracted verbatim (55 lines; extraction
  sanity-checked for `CORRUPT RENDER` before any verdict was trusted) under `bash -e`
  → `OK: every asserted literal survived into the published man-page text`, **exit 0**.
- **The pre-fix baseline on the same tree** — `main`'s step body, same container, same fixtures
  → `CORRUPT RENDER: share/man/man1/xdu.1 is missing the literal: OUTDIR/.xdu-complete`, **exit 1**.
  The reported CI failure reproduces, and the fix closes it.
- **Host toolchain** — macOS, homebrew `scdoc 1.11.5`, `/usr/bin/mandoc` → exit 0; plus a 2×2 cross
  matrix (roff from 1.11.5 **and** 1.11.2 × mandoc-macOS **and** mandoc-1.14.6) → exit 0 in all four
  cells. The roff genuinely differs: 1.11.5 emits `\fIOUTDIR\fR/.\&xdu\-complete`, 1.11.2 emits
  `…xdu-complete`.
- **Corruption mutations** — the historical un-escaped `_OUTDIR_/*/*.parquet` at `doc/xdu.1.scd:113`
  → `scdoc` **exit 0**, publishes `Readers glob OUTDIR//.parquet`, gate → `CORRUPT RENDER: … missing
  the literal: OUTDIR/*/*.parquet`, exit 1, on both toolchains. Marker literal `…complete` → `…complet`
  → red naming `OUTDIR/.xdu-complete` on both.
- **Layout sweeps** — `mandoc -O width=40…200` (161 widths) on both toolchains → **0 failures**;
  `-O indent=0,1,2,3,5,8,13,21,34` → 0 failures; a literal TAB confirmed present on 25 `col -b` lines.
  Contrast: `main`'s body fails **28 / 161** widths on ubuntu.
- **Render-failure cases** — deleted page, zero-byte page, 35-char garbage page, and a combined run
  breaking pages 2 and 4 → `RENDER FAILED` / `RENDER EMPTY` with **0** misleading `CORRUPT RENDER`
  lines; `mandoc` exits 0 on the empty file, so the length guard is genuinely non-redundant; the early
  `return` does not abort the step (a later page's `CORRUPT RENDER` was still reported in the same run).
- **Duplicate-occurrence cases** — source confirmed to hold exactly 2 (`doc/xdu.1.scd:105`, `:135`);
  corrupted occurrence #2, then #1, then both → `has 1 occurrence(s) … expected 2` / `has 0 … expected
  2`, exit 1, identically under `bash -e`, `bash -eo pipefail`, and `dash`. `main`'s presence check is
  **green** on the same single-occurrence corruption.
- **The documented local snippet, run verbatim** — clean tree: silent on both toolchains (agrees with
  CI green); marker corrupted: `MISSING: OUTDIR/.xdu-complete` on both (agrees with CI red); the
  *pre-fix un-normalized* form on ubuntu prints a false `MISSING`, confirming the normalization is what
  closes the gap.
- **Orchestrator-verified artifact (R4/Q4)** — read [`EVIDENCE.md`](EVIDENCE.md): 884 gate invocations,
  56 fixture variants × 4 pages × 2 real `scdoc`s, both platforms, with a pre-fix/post-fix contrast
  table and mutation-tested harness refusals. It also self-reports a contradiction with `PLAN.md` §4's
  pad-sweep numbers rather than quietly conforming to them.

## Requirement → evidence matrix

| R-ID | Implemented by | Verified how | Verified by | Status |
|------|----------------|--------------|-------------|--------|
| R1 | `.github/workflows/test.yaml:168-223` | `git archive` → `ubuntu:24.04` + CI's exact apt set (scdoc 1.11.2), render step then assertion step verbatim under `bash -e` | blind reviewer | ✅ exit 0, `OK:` line |
| R2 | same | host scdoc 1.11.5 + 2×2 roff × mandoc cross matrix | blind reviewer | ✅ exit 0 in 4/4 cells |
| R3 | `:200-203` (whitespace-stripped `grep -qF`) | un-escaped `_OUTDIR_/*/*.parquet` mutation, both toolchains | blind reviewer | ✅ exit 1, names the literal |
| R4 | `tr -d '[:space:]'` on both sides (`:173`, `:193`) | 161-width sweep × 2 toolchains; 9-value indent sweep; TAB presence confirmed | blind reviewer | ✅ 0/161 red (vs 28/161 on `main`) |
| R4 (Q4 recorded sweep) | `spec/…/EVIDENCE.md` | read the committed artifact — 884 invocations, both platforms, both real `scdoc`s, pre-fix contrast | **orchestrator** (blinded from reviewer) | ✅ satisfies Q4 |
| R5 | `:171-187` (render guard + length guard) | missing / empty / garbage page, and a two-page combined break | blind reviewer | ✅ `RENDER FAILED`/`RENDER EMPTY`, 0 misleading lines |
| R6 | `AGENTS.md:110-118` snippet + `:132-144` prose + `invariants.md:199-206` | ran the documented snippet verbatim on clean **and** corrupted trees, both toolchains | blind reviewer | ⚠️ met as written; see F1 |
| R7 | `'2x:.partial suffix'` (`:211`) + count branch `:194-199` | corrupted occurrence #2, #1, and both; 3 shells | blind reviewer | ✅ exit 1 on a *single* corruption (green on `main`) |

**Invariants walk:** no Rust, no `Cargo.toml`, no `src/` — §1–§12 untouched, and no findings were
manufactured against them. §13 is the live section and is satisfied: all three lockstep places
(`AGENTS.md` Commands, the `test.yaml` step, `invariants.md` §13) change together in this branch;
`share/` stays generated/ignored; no version string, tarball layout, or stdout-cleanliness rule is
touched.

**Non-goals held:** whole-branch `git diff --name-only` touches no `src/`, no `doc/*.scd`, no
`Cargo.toml`/`Cargo.lock`, no `tests/`, no `release.yaml`.

**Unmapped changes (possible scope creep):** one cluster, process-mandated rather than creep —
`ROADMAP.md:172-195` + `issues/manpage-groff-hyphenates-marker-path.md` +
`issues/manpage-gate-coverage-gaps.md` are the deferral ledger `AGENTS.md`'s four-homes table requires
for the GOAL's explicit non-goals; the seed issue's `status:` flip is the standard promote marker. The
reviewer independently reproduced every factual claim in both new issue files, because they contradict
a premise recorded in the locked GOAL: groff 1.23.0 U+2010 counts `xdu.1` **10**, `xdu-find.1` 1,
`xdu-view.1` 0, `xdu-rm.1` 1 (mandoc 0 on all four), the marker literal **ABSENT** under
`groff -mandoc -Tutf8` and **FOUND** under `mandoc`; coverage gap (a) — `cp xdu-find.1 xdu-view.1` →
gate **green**; gap (b) — a fifth `doc/xdu-new.1.scd` carrying `_OUTDIR_/*/*.parquet` → `scdoc` exit 0,
publishes `OUTDIR//.parquet`, gate **green**. The GOAL's parenthetical "one adversarial run measured
zero" is false, and `issues/manpage-groff-hyphenates-marker-path.md` says so explicitly. Recording a
falsified premise rather than inheriting it is the correct disposition; not scope creep.

## Findings

### [MEDIUM/CONFIRMED] The documented local check cannot reproduce CI's verdict for the one literal CI now counts
- **Where:** `AGENTS.md:110-118`; `.agents/factory/invariants.md:199-206`; the counted spec at
  `.github/workflows/test.yaml:211`
- **Failure scenario:** a maintainer corrupts *one* of the two `.partial suffix` occurrences
  (`doc/xdu.1.scd:105` / `:135`), runs the check `AGENTS.md` documents, sees green, and pushes — CI
  goes red. This diff introduced the `Nx:` occurrence-count mechanism, and neither `AGENTS.md`'s
  snippet nor `invariants.md` §13 mentions that CI's literal set contains a counted spec; both
  document presence-matching only.
- **Evidence:**
  ```
  $ lit='.partial suffix'                      # after corrupting ONE of the two occurrences
  $ scdoc < doc/xdu.1.scd | mandoc -Tutf8 | col -b | tr -d '[:space:]' |
      grep -qF -- "$(printf '%s' "$lit" | tr -d '[:space:]')" || echo "MISSING: $lit"
  (no output — local verdict GREEN)

  $ bash -e gate.sh                            # the step body extracted verbatim from test.yaml
  CORRUPT RENDER: share/man/man1/xdu.1 has 1 occurrence(s) of the literal '.partial suffix', expected 2
  EXIT=1
  ```
- **Touches invariant / requirement:** R6 is met **as written** — the normalization is byte-identical,
  and Q2's "note that homebrew diverges / name no minimum version" is honored. The gap is against the
  GOAL's Outcome sentence *"the local check `AGENTS.md` documents reaches the same verdict CI will"*,
  and against `invariants.md` §13's freshly-declared three-place lockstep, which this diff itself
  created. Rubric scope item 5 (operating-manual drift): a mechanism this diff introduced that the
  operating manual does not describe.
- **Severity note:** the blind reviewer graded this LOW. Regraded to **MEDIUM** by the orchestrator
  against the rubric's severity table — "operating-manual drift elsewhere (an `AGENTS.md` description
  merely wrong, **absent**, or misattributed)" is MEDIUM. Not HIGH: CI itself is fully correct, and
  only the *documented local* check's fidelity is degraded, for one literal.

### [LOW/CONFIRMED] The `Nx:` count spec parses only a single-digit N
- **Where:** `.github/workflows/test.yaml:190`
- **Failure scenario:** `case "$spec" in [0-9]x:*)` matches exactly one digit, so a future `10x:LIT`
  falls through to the `*)` branch and `lit` becomes the whole spec string including the `10x:` prefix
  — the gate then presence-checks a literal that can never appear. Latent: nothing in today's set uses
  N > 9, and it fails **loudly** (red) rather than silently, so it can never cause a false green. The
  cost is a misleading diagnostic.
- **Evidence:**
  ```
  $ sed "s|'2x:.partial suffix'|'10x:.partial suffix'|" gate.sh > gate_n10.sh
  $ bash -e gate_n10.sh
  CORRUPT RENDER: share/man/man1/xdu.1 is missing the literal: 10x:.partial suffix
  EXIT=1
  ```
- **Touches invariant / requirement:** none directly — robustness of the R7 mechanism.

### Candidates probed and dropped (refutation protocol)

Recorded so a later cycle does not re-litigate them:

- **Whitespace-fusion false green on the real page** — measured `.partial` = 2 and `.partialsuffix`
  = 2 on the actual rendered `xdu.1`; no synthetic third match exists there, so on the *real* page the
  longer needle is defensive rather than load-bearing. (It *is* load-bearing on a constructed
  `e.g. partial` fixture, where `.partial` matches 3 — see `EVIDENCE.md`'s fusion note. Both
  measurements are correct; they measure different inputs.)
- **The 200-char `RENDER EMPTY` threshold as a false red** — real pages are 1780 / 2157 / 2173 / 4921
  stripped characters, a ~9× margin.
- **`set -eu` killing the step at a legitimate non-zero exit** — verified it does not, under `bash -e`,
  `bash -eo pipefail`, and `dash`.
- **Locale sensitivity of the gate's verdict** — identical under `LC_ALL=C`, `C.UTF-8`, and
  `en_US.UTF-8` on both userlands (every asserted literal is ASCII).
- **`check`'s bare `return` aborting the run** — verified the step continues and still reports later
  pages.
- **The roff-control tripwire passes silently when `doc/*.scd` matches nothing** (grep exit 2 → `then`
  skipped). Real, but that line is **byte-identical on `main`**, unchanged by this diff, and
  unreachable in CI. Out of scope — not reported as a finding.

## Human-gate triggers

**None.** No CONFIRMED finding touches the high-blast-radius core (`src/bin/xdu-rm.rs`,
`src/bin/xdu.rs`, `src/crawl.rs`, `src/lib.rs`, `src/cli.rs`) — this branch changes no Rust at all —
and none touches a destructive-rm / schema-stability / atomic-write / SQL-injection invariant
(`invariants.md` §4 / §1 / §2 / §5). The live invariant section is §13 (project conventions), and it
is satisfied.

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

Not run (not requested — invoke `/xdu-review completeness` if wanted).
