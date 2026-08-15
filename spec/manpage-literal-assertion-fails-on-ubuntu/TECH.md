---
slug: manpage-literal-assertion-fails-on-ubuntu
title: The man-page literal gate asserts content, not layout
kind: fix
appetite: small
status: in_progress
branch: fix/manpage-literal-assertion-fails-on-ubuntu
base: main
current_phase: P2
last_updated: '2026-08-14'
phases:
- id: P1
  name: 'Rewrite the literal assertion: normalize, guard the render, count occurrences'
  status: done
  satisfies:
  - R1
  - R2
  - R3
  - R4
  - R5
  - R7
  depends_on: []
  parallel: false
  hammerable: false
  hill: crest
  verify: sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/gate-matrix.sh
- id: P2
  name: Reconcile the documented local check with CI (AGENTS.md + invariants.md lockstep)
  status: pending
  satisfies:
  - R6
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: crest
  verify: sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/doc-parity.sh
- id: P3
  name: Packaging-job simulation + deferral ledger
  status: pending
  satisfies: []
  depends_on:
  - P1
  - P2
  parallel: false
  hammerable: false
  hill: downhill
  verify: sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/job-sim.sh && test
    -f issues/manpage-groff-hyphenates-marker-path.md && test -f issues/manpage-gate-coverage-gaps.md
    && grep -qF 'issues/manpage-groff-hyphenates-marker-path.md' ROADMAP.md && grep
    -qF 'issues/manpage-gate-coverage-gaps.md' ROADMAP.md && git diff --quiet HEAD
    -- src tests bench Cargo.toml Cargo.lock && echo PHASE-OK
review:
  last_reviewed_commit: ''
  verdict: none
  blocked_reason: ''
  cycle: 0
---
# TECH.md — The man-page literal gate asserts content, not layout

The **context engine and finite-state machine** for building this fix. The YAML frontmatter above is
the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/manpage-literal-assertion-fails-on-ubuntu/TECH.md`);
the per-phase checklists below are the work.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R-IDs are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md) — read §2 before touching P1; the exact target shape
  of the shell body is there, and three of its four design points are non-obvious.
- **Backing research:** none (lean path). PLAN §4 records what direct measurement falsified.

## Conventions (apply to every phase)

- Commit conventions, code style, and load-bearing invariants come from [`AGENTS.md`](../../AGENTS.md);
  the curated footgun checklist is [`invariants.md`](../../.agents/factory/invariants.md). This change
  touches **§13 only**.
- One phase per `xdu-build` invocation; one atomic commit containing both the change and the `TECH.md`
  state update. Subjects: `[fix] Build manpage-literal-assertion-fails-on-ubuntu P<n>: …`.
- **No `Co-Authored-By` trailer.**
- **No Rust changes in this feature at all.** If a phase finds itself editing `src/`, stop — that is a
  `GOAL.md` contradiction.
- **`verify:` requires docker** in P1 and P3, deliberately (PLAN §3 deviation table). The defect is a
  cross-toolchain skew; a host-only check is exactly what let it reach CI unseen, so a "SKIPPED"
  fallback is forbidden here — it would repeat harness-log F7's unearned pass. If docker is
  unavailable, the phase is **blocked**, not passed.

---

## Phase P1 — Rewrite the literal assertion: normalize, guard the render, count occurrences
**Satisfies:** R1, R2, R3, R4, R5, R7 · **Depends on:** —
**Goal:** the assertion step in `.github/workflows/test.yaml` reaches the same verdict regardless of
which `scdoc` built the roff or where `mandoc` broke a line, still fires on real corruption, and
diagnoses a failed render as a failed render.

> **Why one phase for six R-IDs.** All six are the same ~30 lines of one shell body; splitting them
> would mean three commits re-editing the same block. The rule that a bundled phase must not get one
> blended verdict is honored by `verify:` **printing a separate PASS/FAIL line per R-ID** (see below) —
> a regression on any one of them fails the phase on its own.

- [x] Rewrite the body of the step named `Assert critical literals survive into the published man-page
      text` (`.github/workflows/test.yaml`, currently `:139-172`) to the shape in **PLAN §2**. Do not
      restructure into separate `check`/`check_count` helpers — the single-render-then-guard ordering is
      load-bearing (PLAN §2, point 1).
- [x] **Keep the step's name prefix `Assert critical literals`** and the `run: |` block at its current
      indentation — `verify/gate-matrix.sh` extracts the body by keying on them.
- [x] Literal list: unchanged except `'.partial'` → `'2x:.partial suffix'`. Do **not** shorten the
      needle to `'2x:.partial'`; PLAN §2 point 4 records the measured false-red.
- [x] Leave the source-side roff-control tripwire (`grep -nE "^[[:space:]]*['.]" doc/*.scd`) exactly as
      it is — it covers a class invisible from the rendered side.
- [x] Rewrite the step's leading comment to state the *invariants*, not the history: why whitespace is
      stripped on both sides (a break can land inside a token; `col -b` indents with tabs); that the
      strip is lossy so an exact-count needle must be specific enough that fusion cannot synthesize it;
      why the count exists at all; and why `|| got=0` is there (`pipefail`). Cross-reference
      `AGENTS.md`'s Commands section rather than restating the escaping rules. **No `R#`/`P#` ids.**
- [x] Write `spec/manpage-literal-assertion-fails-on-ubuntu/verify/gate-matrix.sh`. It must:
      - extract the gate body from `test.yaml` by `awk` and **refuse to run** unless the extraction is
        non-empty, >20 lines, and contains `CORRUPT RENDER` (guards the silent partial-extraction false
        green — PLAN §5 risk 4);
      - build fixtures from a **real** `scdoc` of each version: 1.11.5 on the host, 1.11.2 inside
        `ubuntu:24.04`. **Never** simulate one from the other with `sed`, and mount every fixture
        `:ro` (PLAN §4);
      - run, on **both** platforms and **both** roff variants: the width sweep 40–200; the pad sweep
        0–150 step 3 at default width; the mutation suite (un-escaped glob, one `.partial` corrupted,
        missing page, empty page); the `bash -e` vs `bash -eo pipefail` diagnostic-count check; and the
        `e.g. partial` fusion regression;
      - print one `PASS`/`FAIL` line **per R-ID** plus the raw counts, and exit non-zero if any line is
        FAIL.
- [x] Run it and record its full output to `spec/manpage-literal-assertion-fails-on-ubuntu/EVIDENCE.md`
      (this is the recorded sweep the GOAL's Q4 asked for; commit it).
- **Verify:** `sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/gate-matrix.sh`
- **Touches:** `.github/workflows/test.yaml`, `spec/manpage-literal-assertion-fails-on-ubuntu/verify/gate-matrix.sh`, `spec/manpage-literal-assertion-fails-on-ubuntu/verify/run-cases.sh`, `spec/manpage-literal-assertion-fails-on-ubuntu/EVIDENCE.md`.

> **Amendment (P1, build):** the harness is **two** files, not one. `gate-matrix.sh` orchestrates
> (extract, fixtures, docker, aggregation) and `verify/run-cases.sh` is the platform-portable case
> suite that gate-matrix.sh runs twice — once on the host, once inside `ubuntu:24.04` — with identical
> arguments. Keeping the suite in one file that both platforms execute is what makes "both platforms
> ran the same cases" true by construction rather than by two copies staying in sync. The `verify:`
> command is unchanged.
>
> **Amendment (P1, build):** `EVIDENCE.md` records a **contradiction with `PLAN.md` §4**. The plan
> states the pre-fix gate fails 51/51 pad values on the 1.11.2 roff and attributes the seed's
> green/red flipping to 1.11.5; re-measured here it is **9/51 red on 1.11.2** (including pad 0, the
> real CI failure) and **0/51 on 1.11.5** — the flipping is on 1.11.2. Almost certainly a different
> padding method. No R-ID or verdict depends on the number; pad 0 is red pre-fix and green post-fix
> either way. Left as a recorded correction rather than an edit to the plan's measurement.

## Phase P2 — Reconcile the documented local check with CI
**Satisfies:** R6 · **Depends on:** P1
**Goal:** the command a maintainer is told to run locally reaches the same verdict CI will, and the
`scdoc` skew that hid this defect is written down where they will read it.

- [ ] `AGENTS.md` "Commands" (currently `:105-108`): keep the readable
      `scdoc … | mandoc -Tutf8 | col -b` line for *reading* the page, and add the normalized form CI
      actually matches with (`… | tr -d '[:space:]'`, and the literal stripped the same way). Note that
      homebrew `scdoc` escapes hyphen-minus while the distro package does not, so an un-normalized local
      grep can pass for you and fail in CI. **Do not name a minimum `scdoc` version** — only
      ubuntu-24.04 = 1.11.2 is measured (GOAL Q2).
- [ ] `AGENTS.md` prose (`:111-120`): extend with the layout-vs-content distinction — a literal can be
      *present and intact* yet unfindable by a naive grep because the line broke inside it.
- [ ] `.agents/factory/invariants.md` §13 (`:193-198`): update the restated pipeline in lockstep. It
      restates the *command*, and `invariants.md` is required to track `AGENTS.md`; leaving it stale
      recreates the divergence through a different door.
- [ ] Add a short cross-reference in **all three** places (workflow comment, `AGENTS.md`,
      `invariants.md` §13) naming the other two, so changing the normalization becomes a same-commit
      obligation like the CLI↔man-page rule. This is the agreed mitigation for the drift risk the human
      accepted when choosing inline-and-restate over one shared script (PLAN §5 risk 1).
- [ ] Write `spec/manpage-literal-assertion-fails-on-ubuntu/verify/doc-parity.sh`: extract the
      documented command from `AGENTS.md`, run it against both real roff variants, and assert its
      verdict matches the committed gate's on a clean tree **and** on the un-escaped-glob mutation.
- **Verify:** `sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/doc-parity.sh`
- **Touches:** `AGENTS.md`, `.agents/factory/invariants.md`, `spec/manpage-literal-assertion-fails-on-ubuntu/verify/doc-parity.sh`.

## Phase P3 — Packaging-job simulation + deferral ledger
**Satisfies:** — · **Depends on:** P1, P2
**Goal:** the whole packaging job is green end to end on the real CI image, and every deferral this
pass generated has a home on disk.

- [ ] Write `verify/job-sim.sh`: in `ubuntu:24.04`, from a clean `git archive` of the branch, install
      `scdoc mandoc bsdextrautils`, run the packaging job's render step and the (rewritten) assertion
      step verbatim, and assert exit 0 plus the `OK:` line. Also assert the source-side tripwire passes.
- [ ] **Deferral ledger.** Walk P1 and P2 for "known limitation" / "do not fix here" / "follow-up" and
      confirm each has a matching `issues/{slug}.md` **and** a `ROADMAP.md` entry. An unrecorded
      deferral is a phase failure, not a tidy-up. The two known ones:
- [ ] `issues/manpage-groff-hyphenates-marker-path.md` (from [`templates/ISSUE.md`](../../.agents/factory/templates/ISSUE.md),
      `status: unshaped`) + a `ROADMAP.md` entry. Content: groff 1.23.0 publishes
      `OUTDIR/.xdu-com` + **U+2010** + newline + `plete` at default width on **both** roff variants
      (10 U+2010 per page); `man-db` uses groff, so this is the page real operators read and copy-paste.
      **State plainly that this falsifies the premise recorded in `GOAL.md`'s non-goals** ("one
      adversarial run measured zero U+2010"), and that CI is unaffected because it runs `mandoc`, which
      does not hyphenate. Options to weigh when shaped: also strip `U+2010`/`U+00AD`; assert a groff
      render in CI; or reword the source.
- [ ] `issues/manpage-gate-coverage-gaps.md` + a `ROADMAP.md` entry. Three measured gaps in the gate's
      coverage *model*, all out of scope here because no R-ID reaches them: (a) **no page identity** —
      copying `xdu-find.1` over `xdu-view.1` is green, since no literal names the binary it belongs to;
      (b) **the page list is hard-coded** — a fifth `doc/*.scd` is entirely unasserted and can ship the
      exact historical `OUTDIR//.parquet` corruption green; (c) **4 of 10 assertions are inert** —
      `XDU_INDEX`/`XDU_JOBS` have no silent-corruption mode (mid-word `_` is safe by design), so all of
      the gate's real detection power sits on `xdu.1`, and `xdu-rm.1` — the destructive binary — carries
      only two inert names.
- [ ] Confirm no Rust changed: `git diff --quiet HEAD -- src tests bench Cargo.toml Cargo.lock`. The
      `fmt`/`clippy`/`test` gate is not re-run here because nothing it covers was touched; CI runs it on
      the PR regardless.
- **Verify:** the frontmatter command (job simulation + both `issues/` files + both ROADMAP entries +
  no-Rust-diff).
- **Touches:** `issues/*.md`, `ROADMAP.md`, `spec/manpage-literal-assertion-fails-on-ubuntu/verify/job-sim.sh`.

---

## How `xdu-build` drives this

1. `next_phase.py` prints the next actionable phase (statuses are authoritative).
2. Pre-flight: clean tree, on `branch`, `base` reachable, **docker running**.
3. Execute every `[ ]` in the phase (consult `PLAN.md` for detail — especially §2 for P1).
4. Run the phase's `verify:` command — never advance on a checkbox alone.
5. Amend this file freely if reality diverges (regenerate frontmatter with `set_phase.py`; note the
   amendment in the commit body). STOP and escalate only on a **`GOAL.md` contradiction**.
6. Mark the phase `done`, advance `current_phase`, `--touch`; one `[fix]` commit; stop and report.
