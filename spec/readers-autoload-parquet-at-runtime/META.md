# META — The readers must query offline, and the tests must exercise the binary they just built

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

- **slug:** readers-autoload-parquet-at-runtime

## What worked well

- `xdu-feature` Step 4's "do not copy it verbatim" rule earned its keep. Two of the seed's five draft
  R-IDs were wrong as written: R2 contracted "CI SHALL exercise a cold cache", which GitHub runners
  already do by construction, and the seed's own open question about `json`/`icu` was answerable in
  one grep. Both were caught only because the step forces renegotiation rather than a paste.
- `xdu-plan` Step 2/Step 5's *two* invariant checkpoints paid off in an unexpected direction: the
  post-design pass is what surfaced that §1 (schema stability) is only **apparently** touched — a diff
  enabling a feature named `parquet` reads as schema-adjacent but changes linkage, not row shape. That
  now sits in `PLAN.md` §3 as an explicit non-finding, which is worth more to the reviewer than silence.

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

## F1 — Promoting an issue demands carrying its evidence forward, while the same skill forbids looking at code

`origin=xdu-feature:step-4 severity=low category=instruction status=open target=.agents/skills/xdu-feature/SKILL.md`
- **What happened:** Step 4 says to "carry the evidence (`file:line`, mechanism, whether the defect is
  pre-existing) into **Problem**", but Safety Principles say "no broad code exploration" and the intro
  says "do **not** research or read a lot of code here". An `issues/` file can be arbitrarily old, so
  its `file:line` anchors may have moved — and a GOAL that inherits a stale anchor hands `xdu-plan` a
  bad map. I spot-checked the seed's four anchors (`Cargo.toml:17`, `tests/rm_tests.rs:15`,
  `tests/common/mod.rs:19`, `tests/crawl_tests.rs:8`) and answered the seed's own open question about
  `json`/`icu` with two greps, having to decide unaided whether that counted as the forbidden research.
- **Skill cause:** The two instructions are individually clear and jointly ambiguous; the skill never
  says which wins for a promotion, so the boundary is left to the agent's judgement on every run.
- **Recommended fix:** In Step 4's promotion paragraph, name the exception explicitly — something like
  "verifying a cited `file:line` still says what the issue claims, and resolving a question the issue
  itself flags for shaping, are *confirmation*, not research: bounded `grep`/`Read` on the cited
  anchors is expected. Broadening past them is `xdu-plan`'s job."
- **Confidence:** high · **Effort:** small

## F2 — `verify:` must be authored as embedded shell-in-YAML, with no guidance on the quoting traps

`origin=xdu-plan:step-6 severity=medium category=missing-guidance status=open target=.agents/skills/xdu-plan/SKILL.md`
- **What happened:** Step 6 requires a real `verify:` command per phase, and Step 7 lists "unquoted
  `verify:` YAML" as a qualifying friction class — so the hazard is known to the skill — but no step
  says how to author one safely. Two traps in one sitting: `templates/TECH.md`'s example wraps the
  command in `sh -c "…"` inside a double-quoted YAML scalar, so any nested quote needs a third level
  (`\\\"`); and backslash escapes are live in double-quoted YAML, so a `\n` intended for `printf`
  becomes a real newline before the shell sees it. I restructured to a single quoting level (dropping
  the `sh -c` wrapper — the runner is already a shell) and round-tripped all three strings through
  `yaml.safe_load` to confirm what the shell would actually receive.
- **Skill cause:** the skill mandates a non-trivial embedded-shell artifact and names its failure mode
  in the *retro* step, but offers no authoring rule at the point of authoring.
- **Recommended fix:** one line in Step 6 — "Author `verify:` as a plain shell command with at most one
  quoting level; the runner is already a shell, so wrap in `sh -c '…'` only when handing a command to a
  helper such as `temp_index.sh`. Backslash escapes are live inside double-quoted YAML (`\n` becomes a
  newline), so prefer one level of `\"` and confirm the round-trip before committing." Consider also
  fixing `templates/TECH.md`'s P1 example, which models the nested form.
- **Confidence:** high · **Effort:** small
