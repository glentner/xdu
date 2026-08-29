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
- `xdu-review` Step 2's "artifact-deliverable R-IDs are graded by you, not by the reviewer" paragraph
  earned its length on this diff. `EVIDENCE.md` and 913 lines of `verify/*.sh` live under `spec/`, so
  the `':(exclude)spec/'` pathspec strips the majority of the branch from the reviewer's view. Without
  that instruction the two contexts would have split the evidence spine with neither owning the gap —
  the reviewer reporting R4's recorded sweep as unverifiable, and the orchestrator assuming it had
  been covered.

- `xdu-build` Step 2.3's "scope the class before editing … retune the gate to assert the pattern is
  absent, not that the named lines changed" changed the outcome twice in one phase. It is why the
  remediation swept `.agents/**` and recorded a disposition for `xdu-review/SKILL.md:62` instead of
  silently fixing two cited lines, and why `doc-parity.sh` now **refuses to report a verdict** when
  `AGENTS.md` documents no count form rather than merely testing that today's snippet works — a gate
  that fails on a regression nobody has written yet.

- `xdu-review`'s rubric item 5 (**operating-manual drift** — "a diff that moves the code owns the map",
  graded by checking that what `AGENTS.md`/`invariants.md` *claim* is true of what shipped) is the
  highest-yield item in the rubric on this feature. It is the sole source of every finding in cycles 2
  and 3. Without it all three cycles would have returned clean while §13 carried a rule the gate does
  not enforce — which is worse than no rule, because the next `xdu-plan` trusts it.

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
`origin=xdu-build:step-4 severity=medium category=missing-guidance status=applied target=.agents/skills/xdu-build/SKILL.md`
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

## F5 — Artifact-deliverable R-IDs are sourced from the wrong file, so the one on this branch was nearly missed
`origin=xdu-review:step-2 severity=medium category=instruction status=open target=.agents/skills/xdu-review/SKILL.md`
- **What happened:** Step 2 says to "identify them from `TECH.md`'s `satisfies` notes before
  delegating". I did, and `TECH.md` gave no signal: `satisfies:` lists R4 under P1 alongside R1/R2/R3/
  R5/R7, all of which are ordinary behavioral requirements verifiable from the workflow file. Nothing
  in the frontmatter distinguishes "R4's *behavior* is in the diff" from "R4's *demonstration* is a
  committed document under `spec/`". I only caught it by reading `GOAL.md`'s Clarifications, where Q4
  says R4 is demonstrated by a recorded one-off sweep rather than by new CI surface. Had I followed
  the instruction literally I would have delegated R4 whole, and the reviewer — blinded from
  `EVIDENCE.md` — would have graded only half of it while reporting a clean pass.
- **Skill cause:** `satisfies:` maps R-IDs to *phases*, not to deliverable *kind*; it structurally
  cannot carry the signal the step asks it for. The signal that a requirement is discharged by an
  artifact lives in `GOAL.md`'s Clarifications and non-goals ("demonstrated by a recorded sweep, not
  by new permanent CI surface"), which the step never mentions — and `GOAL.md` is the one file the
  orchestrator is guaranteed to read anyway.
- **Recommended fix:** change the step to source artifact-deliverable R-IDs from **`GOAL.md`'s
  Clarifications / non-goals first** (a requirement whose acceptance is phrased as "recorded",
  "documented", "assessed" or "evidenced in the spec" is one), with `TECH.md`'s `satisfies` as a
  secondary cross-check. Add the one-line test: *would the reviewer, seeing only the spec-excluded
  diff, have anything to run?* If no, the orchestrator owns it.
- **Confidence:** high · **Effort:** small

## F6 — Any CONFIRMED finding auto-blocks, so two documentation-level findings loop a contract-complete fix · seen again
`origin=xdu-review:step-4 severity=medium category=instruction status=open target=.agents/factory/review-rubric.md`
- **What happened:** all seven R-IDs verified PASS by executed command on both toolchains, no Rust
  touched, no human-gate trigger — and the pass still returns `blocked` / `changes-requested`, because
  Step 4 and the rubric's "Verdict & loop" both key on the *existence* of a CONFIRMED finding with no
  severity threshold. The two findings here are a MEDIUM doc omission and a LOW latent parse limit
  that can only ever fail loudly. Both are worth fixing; neither is worth spending one of the three
  bounded cycles on, and the binary rule gives no way to say so.
- **Skill cause:** the severity table exists and is used to *grade* findings, but nothing downstream
  consumes the grade — `CRITICAL` and `LOW` route identically. The PLAUSIBLE channel is the only
  non-blocking outlet and it is the wrong one: downgrading a reproduced finding to PLAUSIBLE to avoid
  a cycle would corrupt the evidence spine, so the instructions push toward either an over-heavy loop
  or a misclassification.
- **Recommended fix:** let severity route the verdict — CRITICAL/HIGH auto-block; MEDIUM/LOW return
  `approved-with-findings`, recorded in `REVIEW.md` and surfaced by `xdu-publish` in the PR body, with
  the human free to loop. **Handle with care:** this touches the review↔build loop, so the change must
  not become a way to ship a real defect. Keep auto-block unconditional for anything touching
  `invariants.md` §1–§12, the high-blast-radius core, or an unmet R-ID, regardless of graded severity.
- **Confidence:** med · **Effort:** medium
- **Seen again (cycle 3), sharper shape:** the *bounded loop* has the same flaw as the verdict routing.
  R1–R7 passed in all three cycles; every finding across cycles 1–3 was a prose overclaim in the
  operating manual, and no gate behaviour has been wrong since cycle 1. Yet cycle 3 consumes the last
  of the ≤3 budget and forces a STOP-and-escalate whose wording ("non-convergence") implies the fix is
  failing, when what actually failed to converge is documentation accuracy. The rubric should
  distinguish **the product regressed / an R-ID is unmet** (a real non-convergence signal) from
  **every R-ID passes and only manual text is wrong** (which should not consume a cycle of a loop
  budget sized for correctness risk). Same recommended remedy as above: let severity and R-ID status
  route both the verdict *and* the cycle accounting.

## F7 — Per-phase commits collide with a same-commit obligation when a remediation spans two phases
`origin=xdu-build:step-1.3 severity=medium category=missing-guidance status=open target=.agents/skills/xdu-build/SKILL.md`
- **What happened:** the review returned two findings — one mapping to P1 (`R7`, the workflow parser)
  and one to P2 (`R6`, `AGENTS.md` + `invariants.md` §13). Step 1.3 says to "prefer reopening the
  existing phase(s)", which I did, but Step 7 then commits **one phase per commit**. The fix those two
  phases share *is* the three-place lockstep this very branch declared — `AGENTS.md`, the workflow
  step, and `invariants.md` §13 must change in one commit. Following both steps literally would have
  split the lockstep across two commits and shipped a branch that violates the rule it added. I used
  `bundle` semantics without the argument having been passed, and recorded the reason as a `TECH.md`
  amendment.
- **Skill cause:** Step 1.3 explicitly contemplates a remediation reopening **multiple** phases, and
  Step 7's commit granularity is per phase, but nothing connects the two. `bundle` exists for exactly
  this and is documented only as a *user* argument in Argument Parsing — there is no instruction
  telling the skill to select it itself when the phases are coupled, and no mention that a product-side
  same-commit rule can force it.
- **Recommended fix:** in Step 1.3, add: "if the reopened phases share a same-commit obligation (a
  documented lockstep, a CLI↔man-page pair), commit them together as if `bundle` had been passed, and
  record why in a `TECH.md` amendment." Cross-reference it from Step 7.
- **Confidence:** high · **Effort:** small

## F8 — Step 2.3's class sweep claims `.agents/**` that `AGENTS.md` routes to `/xdu-harness`
`origin=xdu-build:step-2.3 severity=medium category=instruction status=open target=.agents/skills/xdu-build/SKILL.md`
- **What happened:** Step 2.3 directs the sweep to "list every **live** site: anything an agent or
  human still acts on, `.agents/**` and `AGENTS.md` included, since a stale instruction inside the
  factory re-arms the trap." The sweep surfaced `.agents/skills/xdu-review/SKILL.md:62`, which restates
  the man-page pipeline — a live instruction an agent acts on, squarely inside the named scope. But
  `AGENTS.md`'s four-homes table says skill feedback goes to `META.md` and is applied by the
  human-gated `/xdu-harness`, and that site is *already* recorded as F2. Two rules, opposite answers,
  no tiebreak. I had to adjudicate it myself and write the reasoning into a `TECH.md` amendment so the
  next reviewer would not read the omission as a missed site.
- **Skill cause:** Step 2.3 draws the sweep boundary by **directory** (`.agents/**`) while the
  constitution draws it by **kind of artifact** (product fact vs. agent instruction). `invariants.md`
  and `AGENTS.md` both live inside the sweep and are correctly in scope; a `skills/*/SKILL.md` is in
  the same directory and is not. The step never names the distinction, so every remediation that
  sweeps into `.agents/` has to re-derive it.
- **Recommended fix:** in Step 2.3, replace the directory-keyed scope with the artifact-keyed one:
  in-scope live sites are those stating a **fact about the product** (`AGENTS.md`, `invariants.md`,
  workflows, `doc/`); a defect in a **skill's own instructions** is `META.md` + `/xdu-harness`, never a
  fix-branch edit — a harness change must not ride in on a product PR. Add the one-line test: *would
  fixing this change what the tool does, or what an agent is told to do?*
- **Confidence:** high · **Effort:** small

## F9 — Nothing tells a remediation to close the class when the finding names one instance
`origin=xdu-build:step-2.3 severity=high category=missing-guidance status=applied target=.agents/skills/xdu-build/SKILL.md`
- **What happened:** cycle 1's finding named one duplicated literal. Step 2.3's class sweep is keyed on
  *textual* patterns ("the deleted symbol, the false claim's distinguishing phrase, the renamed
  identifier"), so I grepped for restatements of the rule and fixed those — and shipped a §13 invariant
  asserting duplicated literals are counted while four of ten specs were still presence-asserted. The
  defining property here was not textual at all: it was *"how many times does this literal appear in
  the rendered page"*, computable only by rendering all four pages and counting. Cycle 2 found it, at
  the cost of a full review cycle out of a bounded three.
- **Skill cause:** Step 2.3 says to retune the gate to "assert the pattern is absent", which I did for
  the two named instances — but its worked examples are all `Grep`-able identifiers, so the natural
  reading is a text sweep. Nothing prompts "what property makes this a defect, and can that property be
  *computed* over every candidate?" A gate that enumerates instances is exactly the failure the step is
  trying to prevent, and following it literally still produced one.
- **Recommended fix:** add to Step 2.3: "state the defect's defining property as a predicate, then find
  every site by **evaluating** it, not only by grepping for text. If the predicate needs a build or a
  render to evaluate, that is the strongest possible gate — encode the predicate itself so the check
  covers items nobody enumerated." Cite this pass: text-grep found 3 sites, the derived predicate found
  4 more that no grep could have.
- **Severity note:** `high` because it is a *gate* gap — it let a remediation ship an invariant that was
  false when written, and the review cycle it cost is a scarce resource under the ≤3 bound.
- **Confidence:** high · **Effort:** small

## F10 — Nothing warns that a `spec/*/verify/` harness is temporary when you cite it in the permanent manual
`origin=xdu-build:step-2.3 severity=medium category=missing-guidance status=open target=.agents/skills/xdu-build/SKILL.md`
- **What happened:** Step 2.3 says a diff that moves the code owns the map, so I updated `AGENTS.md`
  and `invariants.md` §13 to describe the new duplicate-count rule — and cited the harness I had just
  built as the thing that enforces it. That harness lives in `spec/{slug}/verify/`, is invoked only by
  this feature's `verify:` fields, and is a retained *record* the moment the spec merges. So a
  permanent invariant now pointed at a temporary mechanism, telling the next agent a hole was closed
  when it was not. It cost the last cycle of a bounded three.
- **Skill cause:** `verify:` harnesses are written and run exactly like durable tests — same shell,
  same green/red, same mutation discipline — and nothing in the skill marks the asymmetry between
  `tests/` (runs forever, on every PR) and `spec/*/verify/` (runs during this feature, then stops).
  Step 2.3 actively pushes toward updating the permanent manual in the same breath as building the
  temporary harness, which is precisely when the two get conflated.
- **Recommended fix:** state the lifetime explicitly where `verify:` is introduced — "a `verify:`
  harness proves the phase; it is a record after merge, not a gate" — and add to Step 2.3: "when
  writing into `AGENTS.md`/`invariants.md`, cite only mechanisms that survive merge (`tests/`, CI
  workflows, code). If the enforcement lives in `spec/*/verify/`, say the rule is maintained by hand
  and file the durable version as an issue."
- **Confidence:** high · **Effort:** small
