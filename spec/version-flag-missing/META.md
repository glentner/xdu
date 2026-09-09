# META — All four binaries answer `--version`

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

- **slug:** version-flag-missing

## What worked well

Brief, optional reinforcement: a part of a skill / the harness that materially helped, so `/xdu-harness`
knows what **not** to change. One line each, naming the skill/step. Skip the section entirely if
nothing stands out.

- `xdu-build` Step 4's "revert to *your* state, not to HEAD" (cp-aside/cmp-restore) made the
  fail-once red-check safe against discarding the uncommitted phase work it was mutating.

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

## F1 — xdu-release SKILL.md still forbids confirming a bump with xdu --version
`origin=xdu-build:P1 severity=medium category=instruction status=applied target=.agents/skills/xdu-release/SKILL.md`
- **What happened:** the class sweep for review F1 found the stale "that flag does not exist" product
  claim in a third live site (the "Do not confirm the bump with `xdu --version`" bullet), alongside
  the two manual sites fixed on this branch. It was left for harness because a skill's own
  instructions must not ride in on a product branch.
- **Skill cause:** not this skill's fault; recorded here per xdu-build Step 2 routing (skill defects
  go META.md + `/xdu-harness`, never a product diff). The failure direction is fail-safe (a
  prohibition, not a trap), so deferring it changes no release outcome.
- **Recommended fix:** reword the bullet to state clap derives `-V`/`--version` from `Cargo.toml` for
  every user-facing binary, and allow confirming the bump with `xdu --version` (or keep reading
  `Cargo.toml`; either is now true).
- **Confidence:** high · **Effort:** small

## F2 — set_phase.py --verdict approved leaves a stale blocked_reason behind
`origin=xdu-review:step-4 severity=low category=tooling status=applied target=.agents/factory/bin/set_phase.py`
- **What happened:** setting `--verdict approved` kept cycle 1's `blocked_reason` text, leaving
  frontmatter that reads approved yet blocked; a second `--blocked-reason ""` call was needed to clear it.
- **Skill cause:** the skill's Step 4 command lists no clearing flag, and the tool does not clear the
  reason on an approving verdict — neither side owns the transition.
- **Recommended fix:** have `set_phase.py` drop `blocked_reason` when `--verdict approved`, or add the
  clearing flag to the skill's clean-path command.
- **Confidence:** med · **Effort:** small
