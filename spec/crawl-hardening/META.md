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
- `xdu-plan`'s adversarial pass on the P9 design earned the whole phase: the obvious
  `.exists()`-then-`read_to_string` implementation would have shipped an indefinite hang in all
  three readers on a FIFO named `.xdu-complete`, and the checklist carried that as a BLOCKING
  correction with the guard spelled out. A design review that produces an executable counter-example
  is worth more than one that produces advice.
- `xdu-plan` pre-committing P10's escalation trigger ("STOP if the fresh capture shows a regression
  clearing the paired spread") set the decision rule *before* the numbers existed. When the capture
  landed as a null, there was nothing to rationalise — the rule already said what a null meant. A
  measurement phase that defines its stopping condition in advance cannot talk itself into a result.
- F3's own lesson — *a gate should be seen to fail once before it is trusted* — paid for itself in P10
  and P12, and not in the way expected. Both new gates did pass their negative test, but the first
  attempt at negative-testing P10's was itself broken: byte-mode `perl` silently failed to substitute a
  UTF-8 en-dash, so the "wrong" file was never actually wrong and the gate printed `ok`. Reading that as
  "gate verified" would have shipped an unexercised gate on the strength of a test that never ran. The
  discipline caught a false green one level up from where it was aimed — worth generalising as *when a
  negative test passes, first confirm the mutation happened.*
- The full self-improvement loop closed for the first time on this feature, and the STOP gate is what
  made it work: P11 carried its prerequisite as a hard stop, refused to improvise a destination when
  `issues/` did not exist, `/xdu-harness` landed the convention, and the phase then ran clean against
  it. Had the gate been advisory, the deferrals would have been written somewhere provisional and the
  harness pass would have had to undo them — which is the exact failure F6 describes.
- `xdu-review`'s rule that **a later cycle defaults to a fresh full blind pass**, not a narrow
  re-verification of the named findings, earned cycle 3 outright. The delta under review was documentation
  only, and a scoped "did the two doc findings get fixed" pass would have returned clean and shipped. The
  full pass instead found C3-F1 — a MEDIUM coupled-core defect in `src/crawl.rs` + `src/lib.rs` that
  cycles 1 and 2 had both walked past. The cheap-looking option was the wrong one, and the skill had
  already decided that in advance.
- The same full-blind-pass default held up a second time in cycle 4, but the lesson is narrower and
  worth separating from the cycle-3 one: what made C4-F1 *actionable* was not finding it, it was the
  **differential drive against a release build of `main` in a throwaway worktree**. The reviewer's
  reproduction alone read as a MEDIUM data-loss defect in new code; running the identical scenario
  against `main` showed the same rows destroyed, exit 0, and no diagnostic at all — turning "this
  branch loses data" into "this branch is the first version that tells you". That single command
  decided the severity, the verdict, and whether a cycle 5 was warranted. See F14.
- `xdu-build` Step 4's "never advance on a checkbox alone" plus F3's own refinement — *when a negative
  test passes, first confirm the mutation happened* — are now cheap habit, and P14 shows why they
  should stay that way. All three of its new gate clauses were seen to fail with the mutation verified
  present first. One of them mattered: the man-page assertion originally grepped the rendered text for
  a hyphenated phrase, which `mandoc` is free to break across a line on any future rewrap. Watching it
  go red is what prompted flattening the render before grepping, turning a gate that happened to pass
  today into one that cannot silently pass tomorrow. The discipline caught a latent false green, which
  is the same place it paid out in P10.

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

## F2 — No guidance for an R-ID satisfied by a research/doc artifact, not a build phase · seen again (xdu-review:step2)
`origin=xdu-plan:step6 severity=low category=missing-guidance status=applied target=.agents/factory/templates/TECH.md`
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
- **Seen again from the review side (worse there):** that fix says "flag it so `xdu-review` grades it by
  inspecting the artifact" — but `xdu-review` blinds the reviewer to **all** of `spec/`, and R1's audit
  (`research/01`) and R8's `ASSESSMENT.md` live exactly there, so the blind reviewer *structurally
  cannot* verify 2 of 10 R-IDs. I improvised: hand-wrote an instruction telling the reviewer to treat R8
  as satisfied-by-filename while I verified the content myself as orchestrator. It works, but it is
  ad-hoc and it silently splits the evidence spine across two contexts. **Extend the fix to
  `xdu-review` Step 2:** state that artifact-deliverable R-IDs are graded by the *orchestrator* (who may
  read them), name them explicitly in the delegation prompt as out-of-scope-for-the-reviewer, and have
  `REVIEW.md` record who verified each. Does not weaken blindness — it makes the existing workaround a
  rule.
- **Confidence:** med · **Effort:** small

## F3 — A self-skipping test reports `ok`, so a green `cargo test` can hide an unrun case · seen again (xdu-build:P10, xdu-review:step3)
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
- **Seen again at `xdu-build:P10`, third instance and a different mechanism:** P10's new `smoke`
  stage asserts the A/B document's `comparisons[]`. Its 104-file fixture crawls in 0.00 s, so no
  paired delta is computable, `comparisons[]` comes back **empty**, and every assertion over it
  passes vacuously — a green self-check verifying nothing. Caught only by reading the emitted JSON.
  With F9 (a man-page gate that asserts "it compiled", not "it says what I wrote") that is three
  green-but-hollow gates in one feature, each a different mechanism: a case that opted out, an
  artifact never inspected, and a fixture too small to produce the thing asserted. **The
  generalisation worth applying:** an assertion over a collection must first assert the collection
  is non-empty, and a gate should be seen to fail once before it is trusted (as P9's FIFO guard was).
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

## F5 — A phase bundling several optimizations behind one measurement can ship a pessimization
`origin=xdu-build:P5 severity=medium category=missing-guidance status=open target=.claude/skills/xdu-build/SKILL.md`
- **What happened:** P5's checklist was "implement L1, implement L2, then benchmark the L1+L2 build
  against the pre-P5 commit" — two independent levers, one measurement. L1 turned out to be a 55%
  *regression* on the very shape xdu exists for, and L2 a solid win. Measured together the two partly
  cancel: the mixed-scenario numbers looked mildly positive, and only the many-partition scenario made
  the problem visible. Had L1's regression been milder, L2's win would have paid for it and a genuine
  pessimization would have shipped credited as an improvement — exactly what GOAL R5 forbids. I caught
  it only by reading per-scenario numbers rather than an aggregate, then re-measuring L2 alone.
- **Skill cause:** the verify discipline is stated per *phase* ("keep a lever only if it shows no
  regression / a measured win"), but a phase may contain several independent changes. Nothing says the
  unit of measurement must be the unit of *change*, so a bundled phase gets one verdict.
- **Recommended fix:** add to `xdu-build` Step 4 (and `xdu-plan`'s phase authoring): when a phase
  contains more than one independent performance change, measure each against the same reference and
  report a verdict per change; report per-scenario numbers, never a single mean. An aggregate can hide
  one change's regression behind another's win. Strengthens the gate; weakens nothing.
- **Confidence:** high · **Effort:** small

## F6 — A phase was told to file code follow-ups in META.md, which forbids exactly that · seen again (xdu-review:step4)
`origin=xdu-build:P6 severity=low category=instruction status=applied target=.claude/skills/xdu-plan/SKILL.md`
- **What happened:** P6's checklist said to record the deferred cleanups "in `ROADMAP.md` and
  `spec/crawl-hardening/META.md` / a short assessment note". But META.md's own header, and `xdu-build`
  Step 6, both say this file is for skillset feedback and to stay silent for "a one-off content/code
  issue". Filing engineering follow-ups there would have contradicted the file's stated contract, so I
  put them in a new `ASSESSMENT.md` plus a `ROADMAP.md` entry and noted the divergence.
- **Skill cause:** `xdu-plan` authored a phase checklist naming META.md as a home for code follow-ups,
  which the META template explicitly excludes. Two parts of the factory disagree about what that file
  is for, and a build phase has to arbitrate mid-work.
- **Recommended fix:** in `xdu-plan`'s phase authoring, point follow-up records at `ROADMAP.md` or a
  spec-local assessment note, never `META.md`; optionally state in the META template that it is
  *skill* feedback only, so the boundary is unmissable from either side.
- **Seen again while authoring the cycle-1 remediation phases (independent evidence of the same gap):**
  two phases (P9's partition-scoped marker limitation, P10's `baseline --out` footgun) were written with
  "record it; do not fix here" and **no destination named**, because no part of the factory states where a
  deferral goes. The human reading the hand-back then asked whether they should go in `META.md` — i.e. an
  outside reader independently reached for the one file the contract forbids. That is the clearest signal
  yet that the destination is undocumented rather than merely mis-stated in one checklist. **Extend the
  fix:** whenever a phase defers work, the authoring skill should require the destination be named inline
  (`ROADMAP.md` and/or the spec-local assessment), and the cycle's last phase should own a deferral ledger
  that cross-checks every "do not fix" line for a matching record — a deferral mentioned only in a
  consumed checklist evaporates. Fixed locally by making P11 that ledger.
- **Agreed convention to apply (human decision, 2026-08-05) — this is the concrete `/xdu-harness` work
  item, not a suggestion:** give deferred *code* work a real home so it stops competing with `META.md`.
  1. **`issues/<slug>.md`** — one file per deferred defect, reusing the body of
     [`templates/GOAL.md`](templates/GOAL.md) (Problem / Outcome / R-IDs) so promotion is a move-and-fill
     rather than a rewrite. Front-matter carries **`status: unshaped`** and a header line stating that
     `/xdu-feature` promotes it into `spec/{slug}/GOAL.md`, where appetite and non-goals get negotiated.
     **Deliberately NOT named `GOAL-<slug>.md`:** every other GOAL in the factory is a locked contract
     `xdu-review` grades against, so a file carrying that name will eventually be copied into
     `spec/{slug}/GOAL.md` verbatim by an agent reading the name as authoritative — skipping the
     `/xdu-feature` shaping gate this log already credits (see *What worked well*). The `status` field is
     the guard that keeps a review-time finding from becoming a graded contract without a human.
  2. **`ROADMAP.md`** keeps its `## Title` + prose + `**Horizon:**` shape but its `**Seed:**` line points
     at the `issues/` file instead of carrying a one-line `/xdu-feature` prompt. Entries are ordered by
     intended remediation/build order. Today's seed one-liner throws away the expensive part of a
     deferral — the `file:line`, the mechanism, and *why it was not safe to fix in that pass* — which is
     precisely what the finder has at hand and a future session must otherwise re-derive.
  3. **A new `AGENTS.md` repo-map entry** for `issues/`, stating the three-way boundary so it cannot drift
     again: `META.md` = harness/skill feedback · `issues/<slug>.md` = deferred code work, pre-shaped ·
     `ROADMAP.md` = the ordered index · `spec/{slug}/` = work actually in flight. Reconcile with the
     GitHub tracker explicitly (`AGENTS.md` already cites issues #2/#3): a GH issue is the public-facing
     ticket, `issues/<slug>.md` is the pre-shaped spec, and they may point at each other.
  4. **The phase-authoring rule** from the paragraph above: a deferral must name its destination inline,
     and a cycle's final phase owns the ledger cross-check.
  5. **Migrate the existing deferrals** as part of establishing the convention (docs-only, and the natural
     proof that it works): `spec/crawl-hardening/ASSESSMENT.md`'s five "Deferred, with reasons" items and
     `ROADMAP.md`'s "Internal cleanups surfaced by the crawl-hardening pass". `ASSESSMENT.md` then *links*
     to the `issues/` files rather than duplicating them, so R8's record still stands on its own. Promote
     the `--version` item first, as **`issues/version-flag-missing.md`** (`kind: fix`, `appetite: small`)
     — it is a user-facing defect in a released version, not a cleanup. **Verified 2026-08-05, so the
     issue file can be written mechanically:** `grep -n version src/cli.rs` returns nothing, i.e. none of
     the four `#[command(...)]` blocks (`:12`, `:65`, `:122`, `:167`) sets `version`; all four man pages
     document it (`doc/xdu.1.scd:60`, `xdu-find:61`, `xdu-rm:73`, `xdu-view:56` — "*-V*, *--version*
     Print version information."); and at runtime `target/release/xdu --version` and
     `xdu-find --version` both fail with `error: unexpected argument '--version' found`.
     Two precisions worth carrying into the issue, because both cut against the obvious reading:
     (i) **completions are not affected** — `gen-completions` builds from the same `clap::Command`, so it
     omits the flag exactly as the binaries do; only the four man pages overclaim, which narrows the §10
     violation to man-vs-code; (ii) **the fix needs no doc change** — the man pages are already correct,
     so §10's same-commit rule is satisfied by a code-only change of four attributes. That makes this
     small enough that `AGENTS.md`'s "a one-sentence change may skip the lifecycle entirely" applies; it
     wants its own `fix/` branch, not a slot behind the cleanup queue.
- **The gate fired as designed (2026-08-05).** P11 was authored with this convention as an explicit
  prerequisite and a STOP-do-not-improvise instruction. Reaching it, `issues/` did not exist, so the
  phase stopped with nothing attempted rather than inventing a destination — which is exactly the
  failure mode F6 describes. **The feature is now blocked on `/xdu-harness`**, making this finding
  the critical path rather than a backlog item. Worth noting for the harness pass: F6 item 5
  (migrate the existing deferrals) overlaps P11's own ledger work, so decide which one owns it
  before both write `ASSESSMENT.md` and `ROADMAP.md`.
- **Confidence:** high · **Effort:** medium (the convention is small; the migration is the bulk)

## F7 — The evidence spine requires an `scdoc` render with no fallback when `scdoc` is absent · seen again (xdu-build:P8)
`origin=xdu-review:step2 severity=high category=tooling status=applied target=.claude/skills/xdu-review/SKILL.md`
- **Severity raised low → high (2026-08-05), because the gap was measured, not theorized.** `scdoc` was
  installed on this host at the human's prompting, and the first render found `doc/xdu.1.scd` **had not
  compiled since P3 (`b8f5f9c`)** — `*__root__*` nests italic inside bold, a hard `scdoc` error. It rode
  through P3, P4, P5, P6, a **full `/xdu-review` cycle**, P7 and P8 — 6 commits — because no gate on this
  host could render. CI would have failed the moment it ran (its render step is `bash -e`), so the branch
  was un-shippable the whole time and every local gate stayed green. A skipped check that hides a broken
  deliverable across a review cycle is not a `low`. **The "or state it is unverified locally" escape
  hatch in this finding's own text is inadequate** — P8 *did* state it, honestly and prominently, and the
  break still survived. Install the tool; do not document its absence.
- **Seen again at `xdu-build:P8`, and it bites harder on the build side:** P8 was a **doc-only** phase —
  its entire deliverable was the rendered EXIT STATUS prose — yet its `verify:` gate goes green while
  printing `scdoc render: SKIPPED`. A gate that cannot inspect the one artifact the phase produces is
  passing on the strength of `cargo test`, which never reads `doc/`. Same fix shape as below, applied to
  `xdu-build`/`xdu-plan`'s verify authoring: when the phase's deliverable *is* the man page, either
  require the render (installing `scdoc` is a one-liner on both CI and dev hosts) or make the phase state
  explicitly that its deliverable is unverified locally and CI is the only gate.
- **What happened:** the skill lists "the man-page render via `scdoc` when a `doc/*.scd` is touched" as
  part of the mandatory executed-evidence spine. `doc/xdu.1.scd` changed on this branch, but `scdoc` is
  not installed on this host, so that evidence line could not be produced. The reviewer substituted a
  `xdu --help` ↔ man-page flag-set comparison and flagged the gap; nothing in the skill sanctioned that
  substitution or told it what to do instead.
- **Skill cause:** the spine names a specific external tool as required evidence without stating a
  fallback or how to report its absence, so the reviewer has to invent both mid-pass — and a less
  careful one would either skip the check silently or report an unearned pass.
- **Recommended fix:** in `xdu-review` Step 2, make the man-page check conditional and explicit: render
  with `scdoc` when available, else compare `<bin> --help`'s flag set against the `.scd` by inspection
  **and record in `REVIEW.md` that the render was unavailable**. Keeps the evidence requirement; only
  removes the ambiguity about an absent tool.
- **Confidence:** high · **Effort:** small

## F8 — No recorded `.scd` authoring conventions, so a wrap can silently emit a roff control line
`origin=xdu-build:P8 severity=medium category=missing-guidance status=open target=.agents/factory/invariants.md`
- **Premise confirmed by experiment (2026-08-05), and it is worse than written above.** With `scdoc`
  1.11.5 installed, the hazards were tested directly rather than reasoned about: a line starting with
  `.partial` renders as `partial` — the period is **silently dropped, no error**; and the intuitive escape
  `\.` at line start **destroys the remainder of the line**. So P8's rewrap was the only correct fix, and
  a future editor "simplifying" it back would corrupt the page invisibly. The same experiment exposed a
  **second, unrelated silent class already live in the tree**: `_OUTDIR_/*/*.parquet` published as
  `OUTDIR//.parquet`, because `*` is bold markup — a wrong glob handed to an operator, rendering at
  exit 0. Both are now fixed; the conventions below are what would have prevented them.
- **What happened:** P8's checklist enumerated the man-page conventions to preserve (`*bold*`, `_italic_`,
  literal em dashes, `.partial` written plain, body wrapped ≤ 79 cols) — and following them produced a
  line **beginning** with `.partial`, because that is where the 79-col wrap fell. In roff a line starting
  with `.` is a control request, so the rendered page could drop or mangle the sentence. Caught only by
  noticing the byte pattern and grepping: no line in any `doc/*.scd` starts with a period, so there was
  no precedent to copy, and `scdoc` is absent here (F7) so the render could not settle it either.
- **Skill cause:** the `.scd` conventions live nowhere durable — they are re-derived per phase from
  whatever the current file happens to look like. `invariants.md` §10/§13 cover *that* man pages track
  the CLI and that `share/` is generated, but not *how* to write the source safely. A convention that
  must be rediscovered by inspection every time is a convention that will eventually be missed, and this
  one fails silently in a generated artifact nobody re-reads.
- **Recommended fix:** add a short "authoring `doc/*.scd`" note to `invariants.md` §13 (or a
  `factory/` reference): never begin a line with `.` or `'` (roff control chars — rewrap instead), keep
  the body ≤ 79 cols, `*bold*` for programs/flags/exit codes, `_italic_` for section cross-references.
  **Extended after the experiment:** escape a literal asterisk as `\*` (precedent already in the tree at
  `xdu.1.scd`'s `st_blocks \* 512`) and a literal double underscore as `\_\_`; a mid-word `_` needs no
  escape (`*XDU_INDEX*` is correct). Note the trap that `*/*` is *legitimate* bold-slash in
  `xdu-view.1.scd:95` (the `/` key) but a corrupted glob in `xdu.1.scd` — so this cannot be
  mechanically find-and-replaced; intent has to be read. Pairs with F7: the written conventions and the
  render are complements, not substitutes — the render catches the loud class, the conventions the silent
  one. An interim version of this guidance now lives in `AGENTS.md`'s Commands section.
- **Confidence:** high · **Effort:** small

## F9 — The man-page gate asserts "it compiled", never "it says what I wrote"
`origin=xdu-build:P8 severity=medium category=missing-guidance status=open target=.github/workflows/test.yaml + .claude/skills/xdu-review/SKILL.md`
- **What happened:** every man-page gate in this repo — CI's render step, `xdu-review`'s evidence spine,
  P8's `verify:` — treats `scdoc` exiting 0 as the pass condition. `doc/xdu.1.scd` shipped
  `_OUTDIR_/*/*.parquet`, which renders at **exit 0** and publishes `OUTDIR//.parquet`. Every gate was
  green while the page told operators the wrong glob. Only diffing the *rendered text* against the
  intended literal catches it, and nothing in the repo does that.
- **Skill cause:** this is the documentation instance of the rule `xdu-build` Step 4 already states for
  code — "exit 0 is necessary but not sufficient; assert a concrete post-condition". That principle was
  never carried across to the doc gate, so the one artifact whose correctness *is* its text is checked
  only for compiling.
- **Recommended fix:** extend the existing CI render step to assert a short list of critical literals
  survives into the rendered output (`scdoc < "$scd" | mandoc -Tutf8 | col -b`, then `grep -qF` for
  `.xdu-complete`, `.partial`, `*/*.parquet`, `__root__`, each documented flag) and fail if one is
  missing. Mirror the same sentence in `xdu-review`'s spine: a man-page render is evidence only when the
  published text was read. Deliberately **not** applied in this pass — recorded per the human's call that
  gate changes go through `/xdu-harness`.
- **Confidence:** high · **Effort:** small

## F10 — `temp_index.sh` rebuilds release binaries only when absent, so a CLI drive can silently test stale code
`origin=xdu-build:P9 severity=high category=tooling status=applied target=.agents/factory/bin/temp_index.sh`
- **What happened:** P9 changed `lib::index_completion_warning`, then drove the readers through
  `temp_index.sh` per Step 4. The drive reported **no warning** on an `errors=3` marker and a **clean pass
  on the FIFO hang case** — both wrong. `temp_index.sh` guards its build with
  `if [ ! -x "$bindir/xdu" ] … then cargo build --release`, so with binaries already on disk from an
  earlier phase it never rebuilds. The drive exercised pre-P9 binaries, which read no marker body: of
  course they neither warned nor hung. After `cargo build --release --bins` the same six cases were
  re-driven and behaved correctly.
- **Skill cause:** this is a **false PASS in the factory's primary behavioral gate**, which is why it is
  `high`. `xdu-build` Step 4 says "verify by driving the CLI, not just tests" and "exit 0 is necessary but
  not sufficient — assert a concrete post-condition", and the script's own header claims "Release binaries
  are the source of truth for a verify drive". A drive can therefore satisfy every instruction, produce a
  concrete-looking post-condition, and still be evidence about code that is no longer in the tree. Worse,
  the failure is **inverted**: a stale binary most often lacks the new behavior, so the drive
  under-reports — and a phase whose drive *fails* gets investigated, while this one silently agreed with
  whatever the old binary did. Note the fix must **not** weaken the gate.
- **Recommended fix:** make `temp_index.sh` rebuild when stale rather than only when absent — drop the
  `-x` guard and let `cargo build --release --bins` decide (cargo is already incremental, so a no-op
  rebuild is cheap and correct). Alternatively compare binary mtime against the newest file under `src/`.
  Either way also fix the header comment, which currently states the "build once if absent" behavior as
  though it were sufficient.
- **Confidence:** high · **Effort:** small

## F11 — The verdict rules have no state for a CONFIRMED finding the build deliberately recorded
`origin=xdu-review:step4 severity=medium category=missing-guidance status=open target=.agents/factory/review-rubric.md + .claude/skills/xdu-review/SKILL.md`
- **What happened:** cycle 2's most severe finding (C2-F1, the scoped-run marker attestation) is a
  reproduced defect in `src/bin/xdu.rs` **and** an item the build had already decided to defer, with an
  in-code `// Known limitation:` comment, an `issues/marker-scoped-run-attestation.md` at
  `status: unshaped`, and a ROADMAP entry. The rubric's routing table knows only "CONFIRMED → blocked +
  changes-requested". So the finding is simultaneously (a) a confirmed defect the rule says must block
  and (b) an accepted trade-off the team recorded on purpose. I had to either mechanically block work
  that was consciously scoped out, or invent a discretion the rubric does not grant me. I blocked, and
  routed the substance to the human gate — but the rubric did not tell me to do that.
- **Skill cause:** the rubric's verdict table predates the `issues/` convention, which **landed during
  this very cycle** (`de3e4ee`). That convention created a new, legitimate end-state — "confirmed, and
  deliberately recorded elsewhere" — and nothing in the review skillset was updated to route it. The
  blind reviewer also cannot see this state cleanly: it read the in-code comment and the `issues/` file
  and correctly flagged the tension itself, calling it "a judgement call for the human", which is
  exactly the guidance the rubric should have supplied.
- **Recommended fix:** add a third routing row to the rubric's "Verdict & loop": a CONFIRMED finding
  already recorded in `issues/{slug}.md` + ROADMAP **before** the review began (verifiable from
  `git log`, so it cannot be back-filled to dodge a block) is reported at full severity and routed to
  the **human gate for an accept-or-remediate call**, not auto-blocked — *except* when it violates an
  `invariants.md` §1–§12 item or lands in a destructive path, which still blocks unconditionally. The
  reviewer prompt should also say plainly that a recorded deferral is still a finding: report it, and do
  not soften the severity because it is written down. **This must not become an escape hatch** — it
  routes to a human, it does not approve anything, and the unconditional-block carve-out is the
  load-bearing half. Framed any looser it would weaken the executed-evidence spine.
- **Confidence:** high · **Effort:** small

## F12 — No severity slot for AGENTS.md drifting from the code a pass just landed · seen again (xdu-build:P12)
`origin=xdu-review:step3 severity=low category=instruction status=open target=.agents/factory/review-rubric.md`
- **What happened:** C2-F2 is a real, greppable gap — `src/crawl.rs`, `--allow-errors`, and the new
  `.xdu-complete` marker appear nowhere in `AGENTS.md`, `ROOT_PARTITION` is misattributed to `xdu.rs`,
  and `invariants.md` (which `AGENTS.md` says is kept "in lockstep") was not touched at all. I could not
  place it on the severity scale. §13 violations are HIGH, but §13 enumerates specific packaging and
  convention items and never says "AGENTS.md must describe the code" — even though `AGENTS.md` opens by
  declaring itself the map and instructing that the code is ground truth, so **fix this file**. I rated
  it MEDIUM and had to write a sentence justifying why it wasn't HIGH.
- **Skill cause:** the severity table has rows for code defects, R-ID gaps and invariant violations, but
  documentation-drift-from-this-diff falls between them. It is a predictable outcome of every `refactor`
  pass that moves code between modules, and it has a compounding consequence the table doesn't capture:
  a stale `invariants.md` silently narrows the gate that `/xdu-plan` and the next `/xdu-review` both
  draw from, so the drift propagates into future cycles.
- **Recommended fix:** name it in the rubric's scope list as a fifth flaggable class — "**operating-manual
  drift**: a symbol, module, flag, or on-disk artifact this diff introduced or moved that `AGENTS.md` or
  `invariants.md` still describes wrongly or not at all" — at **HIGH** when the stale text is
  `invariants.md` or an `AGENTS.md` invariant (it degrades a downstream gate) and MEDIUM otherwise. A
  concrete check worth naming: for a `kind: refactor` diff, grep `AGENTS.md` + `invariants.md` for each
  new/moved module and each new CLI flag or on-disk artifact.
- **Seen again at `xdu-build:P12`, and the sharp case is worse than documentation.** While remediating
  this finding I found that `finalize()` moving from `bin/xdu.rs` to `crawl.rs` had left the
  **high-blast-radius file lists stale** in *both* `invariants.md` and `review-rubric.md`. Those lists
  are not prose — they are the trigger condition for `xdu-review`'s **mandatory human sign-off gate**.
  With `src/crawl.rs` missing from them, a CONFIRMED finding in the atomic-finalize code would not have
  fired the gate. So a pure code *move*, touching no behavior, silently disarmed a safety gate, and
  nothing in the build or review flow re-derives that list. **Raise this finding to `severity=high`
  scope-wise:** the fix must include a check that any file-path-keyed gate list is re-verified when code
  moves between modules — ideally by deriving the high-blast-radius list from something checkable rather
  than restating paths in two files that drift independently. Note this cuts against a non-negotiable
  gate (the human sign-off), so it must only ever *widen* the list, never narrow it.
- **Confidence:** med · **Effort:** small

## F13 — A newly-reserved on-disk name is documented for one collision direction only, so no gate asks about the other

`origin=xdu-review:step2 severity=medium category=missing-guidance status=open target=.agents/factory/invariants.md`

- **What happened:** cycle 3 found C3-F1 — this pass introduced `.xdu-complete` as a second reserved name
  at the index root and guarded only the first (`build_work_queue` rejects a source directory named
  `__root__`, not one named `.xdu-complete`, which bricks the outdir for every future run). Two prior full
  blind passes missed it, and so did the P12 build that had just finished writing the marker's invariant
  text. Every artifact in the chain states the *one-way* property and stops there: `invariants.md` §2b
  ("a dotfile, so the readers' `*/*.parquet` glob never mistakes it for a partition"),
  `COMPLETION_MARKER`'s doc comment (same sentence), and the man page. The reverse question — *can a
  partition be mistaken for the marker?* — is asked nowhere, so no reviewer working the checklist was
  ever pointed at it.
- **Skill cause:** §3 carries the reserved-name collision check as a fact about one specific name
  (`__root__`) rather than as a **class**, even though the guard's own in-code comment states the general
  principle ("the collision is with what is already on disk, not with what this run happens to select").
  When this pass added a second name in the same namespace, there was no checklist item of the form "for
  each reserved name in the index-root namespace, is the collision rejected in **both** directions?" —
  so the gate could be fully satisfied while half the question went unasked. This is the same
  shape as F12 (a gate keyed to enumerated specifics goes stale when the code adds a new instance), but
  the failing artifact is an *invariant clause* rather than a file-path list.
- **Recommended fix:** restate §3's collision item as a namespace rule rather than a `__root__` fact —
  "`<index>/` holds exactly two kinds of entry: partition directories, and the reserved
  `COMPLETION_MARKER` dotfile. Every reserved name in that namespace must be rejected as a source-tree
  top-level name, unconditionally, and a change that adds a reserved name must extend the guard in
  `build_work_queue` in the same commit" — and add the paired check to the review footgun checklist:
  *does this diff introduce a new reserved on-disk name, and is the collision guarded symmetrically?*
  Cheap and checkable: the guard should iterate a single list of reserved names that `lib` owns, so
  adding a constant to that list is what extends the guard.
- **Confidence:** high · **Effort:** small

## F14 — No verdict state for a CONFIRMED defect that is pre-existing in the base and never recorded

`origin=xdu-review:step3 severity=medium category=missing-guidance status=open target=.agents/factory/review-rubric.md`

- **What happened:** cycle 4's most severe finding (C4-F1 — an unreadable partition's `finalize` prunes
  every chunk it previously held, destroying real rows) is reproducible in `src/crawl.rs`, maps to
  invariant §2 and R2, and fires the mandatory human gate. It is also **identical in `main`**, where it
  additionally exits 0 with no diagnostic — so this branch is the first version that reports it at all.
  The rubric routes on "CONFIRMED → blocked + changes-requested" and F11's proposed carve-out only
  covers a finding **already recorded** in `issues/` + ROADMAP before the review. C4-F1 is neither: not
  a regression, not recorded. I had to invent the disposition (record it, don't fix it here, human
  decides) rather than apply one. Two of the reviewer's six findings sat in the same unhandled state.
- **Skill cause:** the rubric's severity and verdict rules are keyed entirely to *the diff* — invariant
  violation, R-ID gap, scope creep — and are silent on **provenance**. A blind reviewer given a large
  refactor diff will inevitably drive code paths nobody drove before and surface defects the base branch
  has always had; the skill's own "no speculative hardening" line is about *inventing* problems, and
  says nothing about *finding real pre-existing ones*. Left unhandled this has a specific failure mode,
  visible right here: the review stops grading the contract and starts auditing the codebase, and the
  cycle count runs past its bound on defects the branch did not cause. Worse, nothing in the skill asks
  the reviewer to establish provenance at all — the `git worktree` differential drive that settled
  C4-F1's severity was improvised by the orchestrator, and had it not been run the finding would have
  been reported as a MEDIUM defect *in this branch* and almost certainly blocked it.
- **Recommended fix:** two changes, both small. **(1)** Add a provenance step to the reviewer's
  refutation protocol: before assigning severity to a defect in code the diff touched, establish whether
  it reproduces on `base` — by `git worktree add` + build and the same drive, which is already in the
  reviewer's toolkit — and report the finding as `pre-existing` or `introduced`. **(2)** Add the fourth
  routing row to "Verdict & loop": a CONFIRMED, **`pre-existing`**, unrecorded defect does **not**
  auto-block; it is reported at full severity and routed to the human gate with a default disposition of
  *record it in `issues/` + ROADMAP this pass, fix it in its own change* — which is exactly the
  precedent the cycle-3 human gate set for C3-F2. **The carve-out must not become an escape hatch**, and
  it needs the same load-bearing exceptions as F11's: a pre-existing defect still blocks unconditionally
  when it lands in a destructive path (`xdu-rm`), and `pre-existing` must be **proved by an executed
  drive against `base`**, never asserted from reading — an unproven provenance claim is treated as
  `introduced`. Note this composes with F11 rather than duplicating it: F11 covers *recorded* findings,
  F14 covers *unrecorded pre-existing* ones, and the two together close the routing table.
- **Confidence:** high · **Effort:** small

## F15 — Remediation is scoped to the file:line a finding names, with no step that asks "is this an instance or a class?"

`origin=xdu-build:P14 severity=medium category=missing-guidance status=open target=.claude/skills/xdu-build/SKILL.md (Step 1.3 remediation mode)`

- **What happened:** P14 remediated four review findings, each stated as a `file:line`. Before editing I
  ran an unprompted sweep for every *other* instance of each defect, and every finding turned out to
  name fewer sites than the class held. The `--version` falsehood is asserted in **four** live operating
  files, not the two the review named — including `.agents/skills/xdu-release/SKILL.md:93`, an
  instruction a human follows *while cutting a release*, telling them to confirm the bump with a flag
  that exits 2. `FileRecord` is named in **four** live documents, not two, including
  `.agents/skills/xdu-plan/SKILL.md`, which instructs future planners to sequence schema phases around a
  struct that no longer exists. And C4-F3 was not a rename at all: the sweep showed nothing builds a
  record any more, so swapping the stale symbol for the right one would have left the surrounding
  sentence describing work the crawler stopped doing. Executing the gate literally would have shipped a
  phase that closed four `file:line`s and left the same four traps armed.
- **Skill cause:** Step 1.3 tells me how to *route* a finding — reopen a phase, or `--add-phase` — and
  Step 3 says to execute every `[ ]` item. Neither asks what the finding is an instance *of*. The
  handoff format guarantees the problem: `xdu-review` reports evidence-backed `file:line` findings
  (correctly — that is the executed-evidence spine), and `xdu-build` treats that location as the work
  item. Nothing in between converts one reproduced instance into the class it belongs to. This is not
  hypothetical or one-off: it is the **third** time on this feature. C3-F1 was a reserved name guarded
  in one direction and not the other; META `F13` records the same shape in `invariants.md`; `F12`'s
  "seen again" note records a code *move* silently disarming a human-sign-off gate because a
  path-keyed list was restated in two files. Each was found by a later review that should not have had
  to find it.
- **Recommended fix:** add a short **Step 2.3 — scope the class** to `xdu-build`, running only in
  remediation mode: for each finding, before editing, grep the whole repo for the defect's *pattern*
  rather than its location (the symbol being deleted, the false claim's distinguishing phrase, the
  renamed identifier), list every live site, and split them into must-change and deliberately-frozen
  (`spec/**` records, committed measurement labels, and the `issues/`+ROADMAP deferral pair are
  evidence — retrofitting them destroys the audit trail, so the step must say so explicitly or it will
  cause a worse problem than it solves). Then **write the phase's `verify:` against the class, not the
  instance** — assert the pattern appears nowhere, not that the named lines changed. P14's gate does
  exactly this (`! grep -rn "derives" <the four files>`, `! grep -rn "FileRecord" src/ tests/`) and it
  is strictly stronger: it fails on a *fifth* site nobody has found yet. Where the sweep widens scope
  beyond what a human gate authorized, the phase body must say which sites were added and why, so the
  widening is visible rather than smuggled — P14's body does.
- **Confidence:** high · **Effort:** small
