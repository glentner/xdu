# META — Glob as the default path-match dialect (pilot)

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

- **slug:** richer-search-glob-fuzzy-fulltext

## What worked well

- The Step 4 promotion paragraph named exactly what to carry from the issue into the GOAL,
  so the contract negotiation started from evidence rather than a blank page.

## Friction findings

<!-- Real findings are appended below this line by the lifecycle skills. -->

## F1 — Argument Parsing does not cover an `issues/{slug}.md` path argument
`origin=xdu-feature:argument-parsing severity=low category=instruction status=open target=.agents/skills/xdu-feature/SKILL.md`
- **What happened:** Invoked with an `issues/{slug}.md` path, the Argument Parsing rules only
  recognized a `spec/<slug>/GOAL.md` path and treated everything else as an inline seed prompt,
  so slug derivation, kind/appetite inheritance, and the branch mapping for the promotion flow
  had to be inferred from the Step 4 paragraph instead.
- **Skill cause:** Step 4 documents promoting an `issues/{slug}.md`, but Argument Parsing never
  names that input shape — the two sections disagree about what an invocation looks like.
- **Recommended fix:** Add one Argument Parsing bullet: a path matching `issues/<slug>.md`
  means promote that file (slug from the filename, kind/appetite seeded from its frontmatter,
  branch per the usual mapping).
- **Confidence:** high · **Effort:** small
