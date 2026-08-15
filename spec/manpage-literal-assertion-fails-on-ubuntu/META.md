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
- `xdu-build` Step 4's named hollow-gate shapes ("an assertion over a collection passing vacuously
  because the collection came back empty"; "a phase holding more than one independent change measured
  once") converted directly into harness design: each R-ID declares how many cases it must contribute
  and a count mismatch is a FAIL even when every line present says PASS, and each fixture mutation is
  re-read after being written so a mutation that failed to apply cannot let its case pass vacuously.
  Without that paragraph the natural implementation is "no FAIL lines → green", which a bad `awk`
  extraction would have satisfied silently.

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

## F2 — `xdu-review` teaches the un-normalized man-page pipeline as sufficient render evidence
`origin=xdu-plan:step-3 severity=medium category=instruction status=open target=.agents/skills/xdu-review/SKILL.md`
- **What happened:** `xdu-review/SKILL.md:62-63` says "A render is evidence only when the published
  text was read — `scdoc < f.scd | mandoc -Tutf8 | col -b`, diffed against the literal you intended".
  That pipeline is exactly the one this fix exists to correct: it is layout-sensitive, so a literal
  that is present and intact reads as missing (or vice versa) depending on where the line broke and
  which `scdoc` built the roff. A reviewer following it on a homebrew box gets the same false green
  that let this defect reach CI. `.agents/factory/invariants.md:198` restates the same command, but
  that one is in scope for this feature (R6) and gets fixed at P2; the skill is not.
- **Skill cause:** the instruction hard-codes a command rather than pointing at `AGENTS.md`'s Commands
  section, which the same paragraph already cites as the single source for both failure modes. The
  restatement is what drifted.
- **Recommended fix:** replace the inline pipeline in `xdu-review/SKILL.md` with a pointer to
  `AGENTS.md`'s Commands section (as the surrounding sentence already does for the failure modes), so
  there is one normalization to keep correct instead of three.
- **Confidence:** high · **Effort:** small

## F3 — The lean path has no design-correctness check, and for a gate-shaped deliverable that is invisible
`origin=xdu-plan:step-3 severity=medium category=missing-guidance status=open target=.agents/skills/xdu-plan/SKILL.md`
- **What happened:** Step 3 directs `appetite: small` + `kind: fix` to skip the research fan-out, and
  Step 5's second invariant gate re-walks `invariants.md` only. Following both faithfully produced a
  design carrying two defects that were later reproduced on the CI toolchain: whitespace-stripping
  fuses adjacent tokens so an ordinary sentence (`e.g. partial …`) reddens a correct page, and the new
  count check dies silently under `pipefail`, reporting **zero** diagnostics for two real corruptions.
  Neither is an invariant violation, so gate #2 could not see them. They were caught only by an
  adversarial pass this skill does not prescribe.
- **Skill cause:** the lean path assumes `xdu-build`'s `verify:` is the safety net. That holds when the
  deliverable is product code. It inverts when the deliverable **is** a verifier: a design error there
  makes the gate wrong in the *green* direction, so its own `verify:` passes and nothing downstream
  disagrees. `kind`/`appetite` are proxies for "is the root cause known?" — they say nothing about
  whether the artifact can grade itself.
- **Recommended fix:** add a clause to Step 3's lean path: when the deliverable is itself a gate,
  assertion, or verification harness (CI steps, `verify:` scripts, test infrastructure), the fan-out is
  **not** skippable — run at least one adversarial reviewer against the drafted design, tasked with
  finding cases where it passes on broken input or fails on good input, before `PLAN.md` is committed.
- **Confidence:** high · **Effort:** small

## F4 — The mandated mutation test says "revert" without naming a safe revert, and the reflex one destroys the phase's work · seen again
`origin=xdu-build:step-4 severity=medium category=missing-guidance status=open target=.agents/skills/xdu-build/SKILL.md`
- **What happened:** Step 4 requires "mutate what it checks, confirm the mutation landed, confirm red,
  revert … Revert before Step 7's `git add -A`". The file I had to mutate to see the gate fail was
  `.github/workflows/test.yaml` — the same file the phase had just rewritten and had **not** committed,
  because Step 7 commits after Step 4 by construction. I reverted the mutation with
  `git checkout .github/workflows/test.yaml`, which restored HEAD and silently discarded the entire
  phase deliverable. It was recoverable only because a `sed -i.bak` from the mutation itself happened
  to hold a copy; with a `Write`-based mutation the work would have been gone with no diagnostic.
- **Skill cause:** the step's own ordering guarantees the mutation target is dirty, and `git checkout`
  / `git restore` / `git stash pop` are the reflex reverts — every one of them resolves against HEAD,
  i.e. against the state *before* the phase. The instruction names the obligation ("revert") but not
  the hazard it creates, and the surrounding safety principles never say the working tree is the only
  copy of the phase's work between Step 3 and Step 7.
- **Recommended fix:** in Step 4, state the revert mechanism: copy the file aside before mutating and
  restore with `cp`, verifying with `cmp -s`; and add an explicit "never `git checkout`/`git restore`/
  `git stash` a file the phase has edited — HEAD predates your work". Optionally note that a Step 4
  mutation is the one place a phase can lose committed-quality work without any command failing.
- **Seen again (P3), second shape:** P3's gate reads its inputs through `git archive HEAD`, so mutating
  them requires a **commit** — a working-tree edit is invisible to it — and the "copy aside, restore
  with `cp`" remedy above does not reach that case at all. The safe mechanism there is a throwaway
  `git worktree add --detach`, commit the mutation inside it, run the gate, `git worktree remove
  --force`; the branch never moves. The fix should name both shapes: mutate the *working tree* →
  restore by `cp` + `cmp -s`; mutate *committed state* → do it in a detached worktree, never on the
  branch.
- **Confidence:** high · **Effort:** small
