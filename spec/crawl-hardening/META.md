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
