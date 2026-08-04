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

## F2 — No guidance for an R-ID satisfied by a research/doc artifact, not a build phase
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
