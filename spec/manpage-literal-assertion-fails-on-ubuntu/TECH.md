---
slug: manpage-literal-assertion-fails-on-ubuntu
title: The man-page literal gate asserts content, not layout
kind: fix
appetite: small
status: in_review
branch: fix/manpage-literal-assertion-fails-on-ubuntu
base: main
current_phase: done
last_updated: '2026-08-15'
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
  status: done
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
  status: done
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
  last_reviewed_commit: b4d32d6971d676579278657a2200be65d6c882e4
  verdict: changes-requested
  blocked_reason: 'R7 unmet for 4 of 10 specs: XDU_INDEX x3 + XDU_JOBS occur twice
    per page but are presence-asserted; single-occurrence corruption ships green.
    invariants.md:207''s new COUNT rule is false as written.'
  cycle: 2
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
> **Amendment (P1, review cycle 2 remediation):** cycle 1's fix closed R7 for the literal the
> reviewer named and left it open for four others. `XDU_INDEX` (on `xdu-find.1`, `xdu-view.1`,
> `xdu-rm.1`) and `XDU_JOBS` (on `xdu-rm.1`) each publish **twice** — once as a cross-reference in the
> flag description, once as the `ENVIRONMENT` entry — and all four were presence-asserted, so
> corrupting only the `ENVIRONMENT` entry shipped a non-existent variable name past a green gate
> ([`REVIEW.md`](REVIEW.md) cycle 2, HIGH/CONFIRMED). `GOAL.md`'s parenthetical "Today `.partial` is
> the only such literal" is a **factual error in the contract**, not a scope boundary, so this was an
> unmet R-ID. All four are now `2x:` counted.
>
> The instance fix is not the point. **`run-cases.sh` gained `class-duplicate-scan`**, which parses
> *every* `check` invocation out of the gate body, derives each literal's published occurrence count,
> and fails on (a) any literal published more than once that is asserted by presence and (b) any
> declared `N` that no longer matches what the page publishes. "Occurs more than once" is a property
> of the *page*, so a future `.scd` edit that adds one cross-reference re-opens the hole silently — the
> scan is what makes R7 hold for literals, and pages, that do not exist yet. R7: 12 → **16** cases;
> `gate-matrix.sh`'s declared count updated in lockstep. Mutation-proved: reverting the four specs to
> presence turns the scan red on all four platform × roff combinations, naming each spec by derivation
> rather than from a list.
>
> **Amendment (P1, review cycle 1 remediation):** the `Nx:` prefix was matched by `case … in
> [0-9]x:*)`, which accepts **one digit only** — `12x:LIT` fell through to the presence branch and
> asserted a literal carrying its own `12x:` prefix, which can never appear on a page
> ([`REVIEW.md`](REVIEW.md), LOW/CONFIRMED). Now the prefix is accepted whenever *every* character
> before `x:` is a digit, so N is unbounded **and** a literal that merely contains `x:` is never
> mistaken for a count spec. `run-cases.sh` gained a `multi-digit-count` case (R7: 8 → **12** cases;
> `gate-matrix.sh`'s declared R7 count updated in lockstep). Mutation-proved: reverting the parser
> turns those 4 cases red with the reviewer's exact diagnostic,
> `missing the literal: 12x:.partial suffix`.
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

- [x] `AGENTS.md` "Commands" (currently `:105-108`): keep the readable
      `scdoc … | mandoc -Tutf8 | col -b` line for *reading* the page, and add the normalized form CI
      actually matches with (`… | tr -d '[:space:]'`, and the literal stripped the same way). Note that
      homebrew `scdoc` escapes hyphen-minus while the distro package does not, so an un-normalized local
      grep can pass for you and fail in CI. **Do not name a minimum `scdoc` version** — only
      ubuntu-24.04 = 1.11.2 is measured (GOAL Q2).
- [x] `AGENTS.md` prose (`:111-120`): extend with the layout-vs-content distinction — a literal can be
      *present and intact* yet unfindable by a naive grep because the line broke inside it.
- [x] `.agents/factory/invariants.md` §13 (`:193-198`): update the restated pipeline in lockstep. It
      restates the *command*, and `invariants.md` is required to track `AGENTS.md`; leaving it stale
      recreates the divergence through a different door.
- [x] Add a short cross-reference in **all three** places (workflow comment, `AGENTS.md`,
      `invariants.md` §13) naming the other two, so changing the normalization becomes a same-commit
      obligation like the CLI↔man-page rule. This is the agreed mitigation for the drift risk the human
      accepted when choosing inline-and-restate over one shared script (PLAN §5 risk 1).
- [x] Write `spec/manpage-literal-assertion-fails-on-ubuntu/verify/doc-parity.sh`: extract the
      documented command from `AGENTS.md`, run it against both real roff variants, and assert its
      verdict matches the committed gate's on a clean tree **and** on the un-escaped-glob mutation.
- **Verify:** `sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/doc-parity.sh`
- **Touches:** `AGENTS.md`, `.agents/factory/invariants.md`, `.github/workflows/test.yaml` (cross-reference comment only), `spec/manpage-literal-assertion-fails-on-ubuntu/verify/doc-parity.sh`.

> **Amendment (P2, review cycle 2 remediation):** the §13 bullet added in cycle 1 asserted that "a
> literal that occurs more than once is asserted by COUNT, not presence" — **false of the shipped gate
> for 4 of 10 specs at the time it was written**, which is operating-manual drift introduced by the
> remediation itself. Both `AGENTS.md` and `invariants.md` §13 now say the true thing: duplication is a
> property of the page rather than of the literal, every duplicated literal *is* counted, and the fact
> is derived by the harness rather than maintained by hand.
>
> **Amendment (P2, review cycle 1 remediation):** R6 was met *as written* — the normalization was
> byte-identical — but the documented check was **presence-only**, and CI counts one literal
> (`2x:.partial suffix`). Corrupting one of the two occurrences was green locally and red in CI
> ([`REVIEW.md`](REVIEW.md), MEDIUM/CONFIRMED): the exact local-vs-CI divergence this pass exists to
> close, reintroduced through the count mechanism P1 added. `AGENTS.md` and `invariants.md` §13 now
> both carry the counting form and state why presence *cannot* predict CI for a duplicated literal.
> `doc-parity.sh` was retuned to assert the **class**, not the cited lines: it extracts the count
> snippet as a second input and **refuses to report a verdict if AGENTS.md documents no count form**,
> adds a `one-partial` fixture variant, and compares four count verdicts per (variant × counted
> literal). Mutation-proved: deleting the snippet makes the gate die rather than pass. Note the
> `one-partial` presence rows all read `ok`/AGREE — presence is genuinely blind there — so only the
> count rows carry that case, which is precisely the finding.
>
> **Amendment (P2, review cycle 1 — class sweep disposition).** The remediation was scoped to the
> *class*, not the two cited lines. Live sites carrying the rule: `AGENTS.md` Commands,
> `invariants.md` §13, and the workflow step — all three changed here, as the lockstep requires.
> **`.agents/skills/xdu-review/SKILL.md:62` was deliberately left alone**: it teaches the *reading*
> pipeline (`scdoc | mandoc | col -b`), which is still correct and which `AGENTS.md:108` keeps for
> exactly that purpose — it is not a restatement of the assertion rule, so the lockstep remains three
> places. Its phrasing does invite misuse as a grep assertion; that is a skill-instruction defect,
> already recorded as [`META.md`](META.md) F2 for `/xdu-harness`, and not fixable from a fix branch
> without a harness change riding in on a product PR. Frozen sites left untouched by design:
> `spec/**`, `issues/manpage-*.md`, the `ROADMAP.md` deferral entries, and `spec/crawl-hardening/**`
> — all point-in-time evidence whose value is being contemporaneous.
>
> **Amendment (P2, build):** **P2's `verify:` requires docker too** — the conventions block above lists
> only P1 and P3. "The local check reaches the same verdict CI will" is only measurable across the two
> real `scdoc`s, and ubuntu-24.04's cannot be installed on the host, so a host-only parity run would
> assert nothing. Same rule as P1/P3: docker absent ⇒ the phase is **blocked**, not passed.
>
> **Amendment (P2, build):** `doc-parity.sh` compares **four** verdicts per (fixture × literal), not
> two — the documented snippet and the gate, each on both toolchains — and takes its literal list from
> the gate's own `xdu.1` call so this phase introduces no second list to drift. It also carries two
> **controls**: the pre-fix un-normalized form must still contradict the gate somewhere, and must give
> *different* answers on the two toolchains. Without them a parity gate passes trivially when both
> sides are equally blind, which is how the original defect survived.

## Phase P3 — Packaging-job simulation + deferral ledger
**Satisfies:** — · **Depends on:** P1, P2
**Goal:** the whole packaging job is green end to end on the real CI image, and every deferral this
pass generated has a home on disk.

- [x] Write `verify/job-sim.sh`: in `ubuntu:24.04`, from a clean `git archive` of the branch, install
      `scdoc mandoc bsdextrautils`, run the packaging job's render step and the (rewritten) assertion
      step verbatim, and assert exit 0 plus the `OK:` line. Also assert the source-side tripwire passes.
- [x] **Deferral ledger.** Walk P1 and P2 for "known limitation" / "do not fix here" / "follow-up" and
      confirm each has a matching `issues/{slug}.md` **and** a `ROADMAP.md` entry. An unrecorded
      deferral is a phase failure, not a tidy-up. The two known ones:
- [x] `issues/manpage-groff-hyphenates-marker-path.md` (from [`templates/ISSUE.md`](../../.agents/factory/templates/ISSUE.md),
      `status: unshaped`) + a `ROADMAP.md` entry. Content: groff 1.23.0 publishes
      `OUTDIR/.xdu-com` + **U+2010** + newline + `plete` at default width on **both** roff variants
      (10 U+2010 per page); `man-db` uses groff, so this is the page real operators read and copy-paste.
      **State plainly that this falsifies the premise recorded in `GOAL.md`'s non-goals** ("one
      adversarial run measured zero U+2010"), and that CI is unaffected because it runs `mandoc`, which
      does not hyphenate. Options to weigh when shaped: also strip `U+2010`/`U+00AD`; assert a groff
      render in CI; or reword the source.
- [x] `issues/manpage-gate-coverage-gaps.md` + a `ROADMAP.md` entry. Three measured gaps in the gate's
      coverage *model*, all out of scope here because no R-ID reaches them: (a) **no page identity** —
      copying `xdu-find.1` over `xdu-view.1` is green, since no literal names the binary it belongs to;
      (b) **the page list is hard-coded** — a fifth `doc/*.scd` is entirely unasserted and can ship the
      exact historical `OUTDIR//.parquet` corruption green; (c) **4 of 10 assertions are inert** —
      `XDU_INDEX`/`XDU_JOBS` have no silent-corruption mode (mid-word `_` is safe by design), so all of
      the gate's real detection power sits on `xdu.1`, and `xdu-rm.1` — the destructive binary — carries
      only two inert names.
- [x] Confirm no Rust changed: `git diff --quiet HEAD -- src tests bench Cargo.toml Cargo.lock`. The
      `fmt`/`clippy`/`test` gate is not re-run here because nothing it covers was touched; CI runs it on
      the PR regardless.
- **Verify:** the frontmatter command (job simulation + both `issues/` files + both ROADMAP entries +
  no-Rust-diff).
- **Touches:** `issues/manpage-groff-hyphenates-marker-path.md`, `issues/manpage-gate-coverage-gaps.md`, `ROADMAP.md`, `spec/manpage-literal-assertion-fails-on-ubuntu/verify/job-sim.sh`.

> **Amendment (P3, review cycle 2 remediation):** the deferral ledger carried a **disproven
> rationale**. `issues/manpage-gate-coverage-gaps.md` gap (c) deferred the four env-var literals on the
> grounds that they "have no silent-corruption mode: they can only fail if the page is missing or
> empty" — three reproductions with neither a missing nor an empty page falsify that. Gap (c) is now
> struck through and reclassified: the duplicate-occurrence half was an unmet R7 and is fixed here, and
> the surviving residue is the weaker "an env-var name is a thin literal" point, carried into R3 of
> that issue. The issue title, its problem statement, its "why it was deferred" paragraph and the
> `ROADMAP.md` entry were all corrected in step. A frozen deferral record is still frozen — but a
> *false reason* in a candidate spec that `/xdu-feature` will later promote is a trap, not evidence.
>
> **Amendment (P3, build):** `job-sim.sh` **hard-refuses** when `.github/workflows/test.yaml` or `doc/`
> carries uncommitted changes. It reads the tree through `git archive HEAD`, so an uncommitted edit to
> either input would make it report on something other than what it just simulated — the same
> "green here, red there" shape this whole pass is about. Refusing beats a silent divergence.
>
> **Amendment (P3, build):** the coverage-gaps issue records **four** gaps, not the three enumerated
> above. The fourth was measured while verifying the groff claim: outside a UTF-8 locale, `col -b`
> rewrites each multibyte character as the literal ASCII text `\xNN`. It is harmless today (every
> asserted literal is ASCII, and P1's matrix confirms host/container agreement) but it bounds what can
> ever be asserted, and it is what made an initial U+2010 count read zero against a demonstrably
> broken page.
>
> **Amendment (P3, build):** unlike `PLAN.md` §4's pad numbers (which P1 could not reproduce),
> **`PLAN.md` §5 risk 3 reproduced exactly** — 10 U+2010 on `xdu.1` under groff 1.23.0, on both roff
> variants, marker literal absent after stripping, `mandoc` unaffected at 0. `man(1)` via man-db shows
> the same. Re-measured rather than transcribed, and the per-page counts (`xdu-find.1` 1, `xdu-rm.1` 1,
> `xdu-view.1` 0) are new detail the plan did not carry.

---

## How `xdu-build` drives this

1. `next_phase.py` prints the next actionable phase (statuses are authoritative).
2. Pre-flight: clean tree, on `branch`, `base` reachable, **docker running**.
3. Execute every `[ ]` in the phase (consult `PLAN.md` for detail — especially §2 for P1).
4. Run the phase's `verify:` command — never advance on a checkbox alone.
5. Amend this file freely if reality diverges (regenerate frontmatter with `set_phase.py`; note the
   amendment in the commit body). STOP and escalate only on a **`GOAL.md` contradiction**.
6. Mark the phase `done`, advance `current_phase`, `--touch`; one `[fix]` commit; stop and report.
