# META — The man-page literal gate asserts content, not layout

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

- **slug:** manpage-literal-assertion-fails-on-ubuntu

## What worked well

- `xdu-feature` Step 4's "an `issues/{slug}.md` is a candidate, not a contract — treat the draft R-IDs
  as input" is doing real work. This seed arrived with six polished R-IDs and heavy measured evidence;
  without that instruction the obvious move is to copy them across, and four contract-level questions
  (intra-literal whitespace, the `AGENTS.md` scope, the two fold-ins, how R4 is evidenced) would have
  been silently pre-decided by the issue's author rather than negotiated.

## Friction findings

Zero or more findings, appended below — each a markdown **section** so appending is a low-corruption
operation and a stdlib parser reads them (`uv run --with pyyaml python .agents/factory/bin/meta_status.py
spec/{slug}/META.md`). Skills always write `status=open`; only `/xdu-harness` flips it. `target` is a
best-guess file with **no line number** (re-derive the exact edit at apply time to avoid staleness). If
an equivalent finding already exists, append "· seen again" to its title instead of duplicating —
recurrence is signal, not bloat.

Field enums — `severity`: `high` (a safety / gate / correctness gap) `| medium | low`; `category`:
`instruction | steering | tooling | template | missing-guidance`; `status`: `open` (written by skills)
`| applied | rejected | deferred` (written by `/xdu-harness`).

Schema (copy one block per finding, appending it **after** this fence — the fence is illustrative and
is skipped by the parser):

```markdown
## F1 — <one-line title of the skillset problem>
`origin=<skill>:<step> severity=<high|medium|low> category=<instruction|steering|tooling|template|missing-guidance> status=open target=<best-guess file>`
- **What happened:** <what the skill made you do, or fail to do>.
- **Skill cause:** <why this is the instructions' fault — not yours, not the task's>.
- **Recommended fix:** <the concrete change to the skill / template / script>.
- **Confidence:** <high|med|low> · **Effort:** <small|medium|large>
```

<!-- Real findings are appended below this line by the lifecycle skills. -->

## F1 — No defined vocabulary for an `issues/{slug}.md` `status:` after promotion
`origin=xdu-feature:step-4 severity=low category=template status=open target=.agents/factory/templates/ISSUE.md`
- **What happened:** Step 4 says to leave the issue in place "with `status:` updated to name the slug
  that adopted it", but neither the step nor `templates/ISSUE.md` defines the value to write —
  the template documents only `status: unshaped`. I had to grep a sibling issue for precedent and found
  `resolved on <branch> (<date>) — see spec/<slug>/`, which is a *different* lifecycle state (written
  by a later skill, post-merge) than the one `/xdu-feature` should be writing at promotion time. I
  invented `shaped on <branch> (<date>) — see spec/<slug>/GOAL.md` by analogy.
- **Skill cause:** The instruction states the intent but not the value, and the template it points at
  enumerates one state out of at least three (`unshaped` → shaped/in-flight → resolved). Each promotion
  therefore re-invents the string, so the `ROADMAP.md` index and the `issues/` tree drift into
  per-promotion formats that `meta_status.py`-style tooling could never parse.
- **Recommended fix:** Enumerate the `status:` states in `templates/ISSUE.md` (a field-enum line like
  `META.md` already carries), and have `xdu-feature` Step 4 / `xdu-publish` each name the exact string
  they write.
- **Confidence:** high · **Effort:** small
