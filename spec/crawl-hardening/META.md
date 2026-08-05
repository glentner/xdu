# META — Harden & optimize the index-build crawl

> **Harness feedback log** for this feature — the producer artifact of the factory's self-improvement
> loop. Written by the lifecycle skills (`xdu-feature` / `xdu-plan` / `xdu-build` / `xdu-review`) when the
> **skillset itself** costs something; read by `xdu-publish` (surfaced in the PR) and applied by
> `/xdu-harness`. This file is **orthogonal** to the `GOAL → PLAN → TECH → REVIEW` spine — it is about
> the *toolchain*, not the feature — and is retained on merge like the rest of `spec/{slug}/`.
>
> **Silence is the default.** The bar for a finding is one test: *was this the **skill's** fault — not
> mine, not the task's?* A merely-hard task, a self-inflicted error, or a one-off content/code issue
> (that belongs in `GOAL.md` / `REVIEW.md`) is **not** a finding. The blind `xdu-review` correctness
> reviewer never reads this file — it would leak author intent.

- **slug:** crawl-hardening

## What worked well

- `xdu-feature` Step 4's AskUserQuestion gate: front-loading the four scoping calls (perf bar,
  benchmark substrate, scope boundary, change latitude) turned an open-ended "revisit performance"
  ask into a bounded, testable contract before any code was read.
- `xdu-plan` Step 3 research fan-out + Step 3 digest: four parallel briefs surfaced a load-bearing
  premise error (the "double stat" — jwalk 0.8 does one serial stat, not two), and the digest step's
  "resolve cross-brief contradictions" mandate is exactly where it got caught and corrected before it
  could mislead the perf design.
- `xdu-build` Step 4's "exit 0 is necessary but not sufficient" directly shaped P4's `smoke` gate: it
  became "the index holds *exactly* the generated files, and a completion marker exists" rather than
  "the script ran". That check is what caught the generator defect in F4 below.

## Friction findings

<!-- Real findings are appended below this line by the lifecycle skills. -->

## F1 — Commit-category rule ignores `kind: refactor` · seen again (xdu-plan:step8)
`origin=xdu-feature:step7 severity=low category=instruction status=open target=.claude/skills/xdu-feature/SKILL.md`
- **What happened:** Step 7 says `{category} = fix for kind: fix, else feature`, so a `kind: refactor`
  shaping commit is told to use `[feature]` even though this skill accepts `kind: refactor` and
  `AGENTS.md` explicitly lists `refactor` as a valid commit category. I used `[refactor]` instead as
  the more accurate label.
- **Skill cause:** The category mapping was written for the feature/fix pair and never extended when
  `refactor` became a recognized `kind`; the instruction now under-specifies the refactor case.
- **Recommended fix:** Change Step 7 to `{category} = fix for kind: fix, refactor for kind: refactor,
  else feature` (or generally map category from kind). Same in `xdu-plan` Step 8.
- **Confidence:** high · **Effort:** small

## F2 — No guidance for an R-ID satisfied by a research/doc artifact, not a build phase · seen again (xdu-review:step2)
`origin=xdu-plan:step6 severity=low category=missing-guidance status=open target=.agents/factory/templates/TECH.md`
- **What happened:** GOAL R1 ("produce a written concurrency audit") and R9 ("HPC protocol doc") are
  satisfied by artifacts (`research/01`, a `bench/` doc), not by CLI-observable behavior. The FSM
  models every R-ID as `satisfies` on a build phase, and the skill steers `verify:` toward driving the
  real CLI — neither fits a document deliverable. I mapped R1 onto P1 ("acts on the audit") as a
  workaround, but that's a forced fit.
- **Skill cause:** `xdu-plan` (and the TECH template) assume R-IDs are delivered by executable phases
  with CLI-drivable verifies; there's no guidance for requirements whose deliverable is a committed
  document, so traceability and the eventual `xdu-review` grading of such an R-ID are ambiguous.
- **Recommended fix:** Add a note to `xdu-plan` Step 6 / the TECH template: an R-ID whose deliverable
  is a committed artifact may be `satisfies`ed by the phase that produces/commits it, with a
  file-existence/content `verify:` (e.g. `test -f …`) instead of a CLI drive; flag it so `xdu-review`
  grades it by inspecting the artifact, not by driving a binary.
- **Seen again from the review side (worse there):** that fix says "flag it so `xdu-review` grades it by
  inspecting the artifact" — but `xdu-review` blinds the reviewer to **all** of `spec/`, and R1's audit
  (`research/01`) and R8's `ASSESSMENT.md` live exactly there, so the blind reviewer *structurally
  cannot* verify 2 of 10 R-IDs. I improvised: hand-wrote an instruction telling the reviewer to treat R8
  as satisfied-by-filename while I verified the content myself as orchestrator. It works, but it is
  ad-hoc and it silently splits the evidence spine across two contexts. **Extend the fix to
  `xdu-review` Step 2:** state that artifact-deliverable R-IDs are graded by the *orchestrator* (who may
  read them), name them explicitly in the delegation prompt as out-of-scope-for-the-reviewer, and have
  `REVIEW.md` record who verified each. Does not weaken blindness — it makes the existing workaround a
  rule.
- **Confidence:** med · **Effort:** small

## F3 — A self-skipping test reports `ok`, so a green `cargo test` can hide an unrun case
`origin=xdu-build:P3 severity=low category=missing-guidance status=open target=.claude/skills/xdu-build/SKILL.md`
- **What happened:** P3's non-UTF-8 case can't run on the dev box (APFS/HFS+ reject such filenames), so
  the test early-returns — and prints `... ok` like every other test. The `verify:` gate was fully green
  while one of the phase's four listed cases had never executed. I only found out by re-running with
  `--nocapture`. The same shape already exists in this repo's P2 tests (`geteuid() == 0` root skips).
- **Skill cause:** Step 4's "exit 0 is necessary but not sufficient" is aimed at the *command's* exit
  status; it has no guidance for a green suite whose individual cases silently opted out. The gate reads
  as fully satisfied when it isn't.
- **Recommended fix:** Add to Step 4: when a phase's verify is `cargo test` and any case is
  platform-conditional, re-run the relevant test with `--nocapture` (or `-- --include-ignored`) to
  confirm it actually executed, and state in the final report which cases skipped and where they *do*
  run (e.g. the Linux CI leg). Strengthens the gate; weakens nothing.
- **Confidence:** high · **Effort:** small

## F4 — Research briefs carry unlabelled code sketches a build phase is invited to transcribe
`origin=xdu-build:P4 severity=low category=template status=open target=.agents/factory/templates/ (research brief) + .claude/skills/xdu-plan/SKILL.md`
- **What happened:** `research/03-benchmark-design.md` contains a ready-looking `gen_tree.py`, and P4's
  checklist says to build that generator. Its leaf-path expression (`d{lvl}_{di%4}`) collapses
  `dirs_per_part` onto four directories, so files silently overwrite each other while the script prints
  the count it *intended* to create. Transcribed as-is, the whole benchmark — the artifact P4 exists to
  produce — would have measured a tree several times smaller than the one it reported.
- **Skill cause:** research code blocks are presented in the same register as the verified findings
  around them, with nothing marking them as unrun illustrations. A build phase reading "the detail
  behind the checklist" has no signal that this particular detail was never executed.
- **Recommended fix:** have `xdu-plan` label code in research briefs as an unverified sketch (a one-line
  banner above such blocks, or a template convention), and add a line to `xdu-build` Step 2: research
  code is a design sketch to re-derive, not source to transcribe.
- **Confidence:** med · **Effort:** small

## F5 — A phase bundling several optimizations behind one measurement can ship a pessimization
`origin=xdu-build:P5 severity=medium category=missing-guidance status=open target=.claude/skills/xdu-build/SKILL.md`
- **What happened:** P5's checklist was "implement L1, implement L2, then benchmark the L1+L2 build
  against the pre-P5 commit" — two independent levers, one measurement. L1 turned out to be a 55%
  *regression* on the very shape xdu exists for, and L2 a solid win. Measured together the two partly
  cancel: the mixed-scenario numbers looked mildly positive, and only the many-partition scenario made
  the problem visible. Had L1's regression been milder, L2's win would have paid for it and a genuine
  pessimization would have shipped credited as an improvement — exactly what GOAL R5 forbids. I caught
  it only by reading per-scenario numbers rather than an aggregate, then re-measuring L2 alone.
- **Skill cause:** the verify discipline is stated per *phase* ("keep a lever only if it shows no
  regression / a measured win"), but a phase may contain several independent changes. Nothing says the
  unit of measurement must be the unit of *change*, so a bundled phase gets one verdict.
- **Recommended fix:** add to `xdu-build` Step 4 (and `xdu-plan`'s phase authoring): when a phase
  contains more than one independent performance change, measure each against the same reference and
  report a verdict per change; report per-scenario numbers, never a single mean. An aggregate can hide
  one change's regression behind another's win. Strengthens the gate; weakens nothing.
- **Confidence:** high · **Effort:** small

## F6 — A phase was told to file code follow-ups in META.md, which forbids exactly that · seen again (xdu-review:step4)
`origin=xdu-build:P6 severity=low category=instruction status=open target=.claude/skills/xdu-plan/SKILL.md`
- **What happened:** P6's checklist said to record the deferred cleanups "in `ROADMAP.md` and
  `spec/crawl-hardening/META.md` / a short assessment note". But META.md's own header, and `xdu-build`
  Step 6, both say this file is for skillset feedback and to stay silent for "a one-off content/code
  issue". Filing engineering follow-ups there would have contradicted the file's stated contract, so I
  put them in a new `ASSESSMENT.md` plus a `ROADMAP.md` entry and noted the divergence.
- **Skill cause:** `xdu-plan` authored a phase checklist naming META.md as a home for code follow-ups,
  which the META template explicitly excludes. Two parts of the factory disagree about what that file
  is for, and a build phase has to arbitrate mid-work.
- **Recommended fix:** in `xdu-plan`'s phase authoring, point follow-up records at `ROADMAP.md` or a
  spec-local assessment note, never `META.md`; optionally state in the META template that it is
  *skill* feedback only, so the boundary is unmissable from either side.
- **Seen again while authoring the cycle-1 remediation phases (independent evidence of the same gap):**
  two phases (P9's partition-scoped marker limitation, P10's `baseline --out` footgun) were written with
  "record it; do not fix here" and **no destination named**, because no part of the factory states where a
  deferral goes. The human reading the hand-back then asked whether they should go in `META.md` — i.e. an
  outside reader independently reached for the one file the contract forbids. That is the clearest signal
  yet that the destination is undocumented rather than merely mis-stated in one checklist. **Extend the
  fix:** whenever a phase defers work, the authoring skill should require the destination be named inline
  (`ROADMAP.md` and/or the spec-local assessment), and the cycle's last phase should own a deferral ledger
  that cross-checks every "do not fix" line for a matching record — a deferral mentioned only in a
  consumed checklist evaporates. Fixed locally by making P11 that ledger.
- **Agreed convention to apply (human decision, 2026-08-05) — this is the concrete `/xdu-harness` work
  item, not a suggestion:** give deferred *code* work a real home so it stops competing with `META.md`.
  1. **`issues/<slug>.md`** — one file per deferred defect, reusing the body of
     [`templates/GOAL.md`](templates/GOAL.md) (Problem / Outcome / R-IDs) so promotion is a move-and-fill
     rather than a rewrite. Front-matter carries **`status: unshaped`** and a header line stating that
     `/xdu-feature` promotes it into `spec/{slug}/GOAL.md`, where appetite and non-goals get negotiated.
     **Deliberately NOT named `GOAL-<slug>.md`:** every other GOAL in the factory is a locked contract
     `xdu-review` grades against, so a file carrying that name will eventually be copied into
     `spec/{slug}/GOAL.md` verbatim by an agent reading the name as authoritative — skipping the
     `/xdu-feature` shaping gate this log already credits (see *What worked well*). The `status` field is
     the guard that keeps a review-time finding from becoming a graded contract without a human.
  2. **`ROADMAP.md`** keeps its `## Title` + prose + `**Horizon:**` shape but its `**Seed:**` line points
     at the `issues/` file instead of carrying a one-line `/xdu-feature` prompt. Entries are ordered by
     intended remediation/build order. Today's seed one-liner throws away the expensive part of a
     deferral — the `file:line`, the mechanism, and *why it was not safe to fix in that pass* — which is
     precisely what the finder has at hand and a future session must otherwise re-derive.
  3. **A new `AGENTS.md` repo-map entry** for `issues/`, stating the three-way boundary so it cannot drift
     again: `META.md` = harness/skill feedback · `issues/<slug>.md` = deferred code work, pre-shaped ·
     `ROADMAP.md` = the ordered index · `spec/{slug}/` = work actually in flight. Reconcile with the
     GitHub tracker explicitly (`AGENTS.md` already cites issues #2/#3): a GH issue is the public-facing
     ticket, `issues/<slug>.md` is the pre-shaped spec, and they may point at each other.
  4. **The phase-authoring rule** from the paragraph above: a deferral must name its destination inline,
     and a cycle's final phase owns the ledger cross-check.
  5. **Migrate the existing deferrals** as part of establishing the convention (docs-only, and the natural
     proof that it works): `spec/crawl-hardening/ASSESSMENT.md`'s five "Deferred, with reasons" items and
     `ROADMAP.md`'s "Internal cleanups surfaced by the crawl-hardening pass". `ASSESSMENT.md` then *links*
     to the `issues/` files rather than duplicating them, so R8's record still stands on its own. Promote
     the `--version` item first, as **`issues/version-flag-missing.md`** (`kind: fix`, `appetite: small`)
     — it is a user-facing defect in a released version, not a cleanup. **Verified 2026-08-05, so the
     issue file can be written mechanically:** `grep -n version src/cli.rs` returns nothing, i.e. none of
     the four `#[command(...)]` blocks (`:12`, `:65`, `:122`, `:167`) sets `version`; all four man pages
     document it (`doc/xdu.1.scd:60`, `xdu-find:61`, `xdu-rm:73`, `xdu-view:56` — "*-V*, *--version*
     Print version information."); and at runtime `target/release/xdu --version` and
     `xdu-find --version` both fail with `error: unexpected argument '--version' found`.
     Two precisions worth carrying into the issue, because both cut against the obvious reading:
     (i) **completions are not affected** — `gen-completions` builds from the same `clap::Command`, so it
     omits the flag exactly as the binaries do; only the four man pages overclaim, which narrows the §10
     violation to man-vs-code; (ii) **the fix needs no doc change** — the man pages are already correct,
     so §10's same-commit rule is satisfied by a code-only change of four attributes. That makes this
     small enough that `AGENTS.md`'s "a one-sentence change may skip the lifecycle entirely" applies; it
     wants its own `fix/` branch, not a slot behind the cleanup queue.
- **Confidence:** high · **Effort:** medium (the convention is small; the migration is the bulk)

## F7 — The evidence spine requires an `scdoc` render with no fallback when `scdoc` is absent · seen again (xdu-build:P8)
`origin=xdu-review:step2 severity=low category=tooling status=open target=.claude/skills/xdu-review/SKILL.md`
- **Seen again at `xdu-build:P8`, and it bites harder on the build side:** P8 was a **doc-only** phase —
  its entire deliverable was the rendered EXIT STATUS prose — yet its `verify:` gate goes green while
  printing `scdoc render: SKIPPED`. A gate that cannot inspect the one artifact the phase produces is
  passing on the strength of `cargo test`, which never reads `doc/`. Same fix shape as below, applied to
  `xdu-build`/`xdu-plan`'s verify authoring: when the phase's deliverable *is* the man page, either
  require the render (installing `scdoc` is a one-liner on both CI and dev hosts) or make the phase state
  explicitly that its deliverable is unverified locally and CI is the only gate.
- **What happened:** the skill lists "the man-page render via `scdoc` when a `doc/*.scd` is touched" as
  part of the mandatory executed-evidence spine. `doc/xdu.1.scd` changed on this branch, but `scdoc` is
  not installed on this host, so that evidence line could not be produced. The reviewer substituted a
  `xdu --help` ↔ man-page flag-set comparison and flagged the gap; nothing in the skill sanctioned that
  substitution or told it what to do instead.
- **Skill cause:** the spine names a specific external tool as required evidence without stating a
  fallback or how to report its absence, so the reviewer has to invent both mid-pass — and a less
  careful one would either skip the check silently or report an unearned pass.
- **Recommended fix:** in `xdu-review` Step 2, make the man-page check conditional and explicit: render
  with `scdoc` when available, else compare `<bin> --help`'s flag set against the `.scd` by inspection
  **and record in `REVIEW.md` that the render was unavailable**. Keeps the evidence requirement; only
  removes the ambiguity about an absent tool.
- **Confidence:** high · **Effort:** small

## F8 — No recorded `.scd` authoring conventions, so a wrap can silently emit a roff control line
`origin=xdu-build:P8 severity=medium category=missing-guidance status=open target=.agents/factory/invariants.md`
- **What happened:** P8's checklist enumerated the man-page conventions to preserve (`*bold*`, `_italic_`,
  literal em dashes, `.partial` written plain, body wrapped ≤ 79 cols) — and following them produced a
  line **beginning** with `.partial`, because that is where the 79-col wrap fell. In roff a line starting
  with `.` is a control request, so the rendered page could drop or mangle the sentence. Caught only by
  noticing the byte pattern and grepping: no line in any `doc/*.scd` starts with a period, so there was
  no precedent to copy, and `scdoc` is absent here (F7) so the render could not settle it either.
- **Skill cause:** the `.scd` conventions live nowhere durable — they are re-derived per phase from
  whatever the current file happens to look like. `invariants.md` §10/§13 cover *that* man pages track
  the CLI and that `share/` is generated, but not *how* to write the source safely. A convention that
  must be rediscovered by inspection every time is a convention that will eventually be missed, and this
  one fails silently in a generated artifact nobody re-reads.
- **Recommended fix:** add a short "authoring `doc/*.scd`" note to `invariants.md` §13 (or a
  `factory/` reference): never begin a line with `.` or `'` (roff control chars — rewrap instead), keep
  the body ≤ 79 cols, `*bold*` for programs/flags/exit codes, `_italic_` for section cross-references.
  Pairs with F7: with the render unavailable locally, written conventions are the only guard.
- **Confidence:** high · **Effort:** small
