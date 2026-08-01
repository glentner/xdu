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

## Friction findings

<!-- Real findings are appended below this line by the lifecycle skills. -->

## F1 — Commit-category rule ignores `kind: refactor`
`origin=xdu-feature:step7 severity=low category=instruction status=open target=.claude/skills/xdu-feature/SKILL.md`
- **What happened:** Step 7 says `{category} = fix for kind: fix, else feature`, so a `kind: refactor`
  shaping commit is told to use `[feature]` even though this skill accepts `kind: refactor` and
  `AGENTS.md` explicitly lists `refactor` as a valid commit category. I used `[refactor]` instead as
  the more accurate label.
- **Skill cause:** The category mapping was written for the feature/fix pair and never extended when
  `refactor` became a recognized `kind`; the instruction now under-specifies the refactor case.
- **Recommended fix:** Change Step 7 to `{category} = fix for kind: fix, refactor for kind: refactor,
  else feature` (or generally map category from kind).
- **Confidence:** high · **Effort:** small
