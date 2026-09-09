# META — Richer index schema: owner, group, permissions, mtime, ctime

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

- **slug:** richer-index-schema

## What worked well

- `xdu-feature` size circuit-breaker: paused on a five-column bundled seed and offered the split
  before writing R-IDs, so appetite was negotiated rather than inherited from the issue frontmatter.
- The issue template's "not a contract" banner kept draft R-IDs from landing verbatim.

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

## F1 — Circuit-breaker questions land after the branch exists
`origin=xdu-feature:step-3 severity=low category=instruction status=applied target=.agents/skills/xdu-feature/SKILL.md`
- **What happened:** Step 3 creates `feature/{slug}` before Step 4's circuit-breaker and
  clarifications. This seed's split question could have changed the slug (owner/group/mode vs
  mtime/ctime vs the bundled name). Asking first was a deviation from the written order.
- **Skill cause:** The procedure orders branch creation ahead of the questions that can invalidate
  the slug. The size circuit-breaker lives in Safety Principles and Step 4, both after Step 3.
- **Recommended fix:** Resolve slug-affecting clarifications (at least the circuit-breaker split)
  before Step 3 creates the branch.
- **Confidence:** med · **Effort:** small

## F2 — Promotion status update is missing from the commit add-list
`origin=xdu-feature:step-7 severity=medium category=instruction status=applied target=.agents/skills/xdu-feature/SKILL.md`
- **What happened:** Step 4 requires setting `issues/{slug}.md` `status:` to `shaped on <branch>`,
  but Step 7's `git add` names only `spec/{slug}/GOAL.md` (and META.md). Following that list
  literally leaves the status change uncommitted.
- **Skill cause:** The promotion flow gained an issues-file write without a matching add in the
  commit step.
- **Recommended fix:** Step 7's add-list should include `issues/{slug}.md` when the invocation
  promoted one.
- **Confidence:** high · **Effort:** small

## F3 — No expansion-safe path for `--verify` retunes carrying `$( )`
`origin=xdu-build:P3 severity=low category=tooling status=applied target=.agents/factory/bin/set_phase.py`
- **What happened:** Retuning P3's gate to a drive containing `$( )` via `--verify "..."` let
  the calling shell expand the substitutions before the script saw them, storing a corrupted
  gate (empty `test -eq 4` fragments) that still passed YAML validation. Recovery needed a
  file staging plus a byte-exact round-trip check; a nested-quote fix (`'&4000'`) then collided
  with the outer `sh -c '...'` quoting and needed a second pass with the skill-blessed `\"` level.
- **Skill cause:** The only documented retune path takes the gate through argv, where no quoting
  survives `$( )` intact, and nothing in the procedure names the hazard or the round-trip guard.
- **Recommended fix:** Teach `set_phase.py` a `--verify-file` (or stdin) input, and note the
  round-trip check as the required close for any `--verify` retune.
- **Confidence:** high · **Effort:** small
