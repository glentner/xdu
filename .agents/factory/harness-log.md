# Harness change log (`xdu-harness`)

The cross-job ledger of every harness self-improvement **decision** — the *act* side of the factory's
self-improvement loop. `/xdu-harness` appends one entry per **applied** / **rejected** (and notable
**deferred**) finding, and **reads this file before applying**: a proposed fix that reverts a recent
change, or repeats a previously-rejected one, is flagged to the human rather than silently re-applied.
This is the loop's **anti-thrash memory**. (Findings themselves live in each feature's
`spec/{slug}/META.md`; this file is the durable record of what was *done* about them.)

Entry format — one section per decision, newest at the bottom:

```markdown
## {YYYY-MM-DD} — {slug} {F#}: {one-line title}
`decision=applied|rejected|deferred commit={sha|—} target={file}`
- **Rationale:** what was changed (and why it generalizes) / why rejected (overfit, stale, would-weaken-a-gate) / why deferred.
```

Read `origin`/`severity`/`category` from the finding in `META.md`; this ledger records the *outcome*.

---

<!-- Decisions are appended below this line by /xdu-harness. -->

## 2026-08-05 — crawl-hardening F6: give deferred code work a home (`issues/`)
`decision=applied commit=de3e4ee target=AGENTS.md + factory/templates/ISSUE.md + skills/xdu-plan,xdu-feature`
- **Rationale:** two parts of the factory disagreed about where a deferral goes, so each pass arbitrated
  it mid-work and an outside reader independently reached for the one file the contract forbids. Fixed by
  naming four homes with one rule each in `AGENTS.md` (META = harness feedback · `issues/{slug}.md` =
  deferred code work · `ROADMAP.md` = ordered index · `spec/{slug}/` = in flight), a template whose
  `status: unshaped` guard keeps a proposal from becoming a graded contract, and authoring rules in
  `xdu-plan` (name the destination inline; the cycle's last phase owns the ledger) and `xdu-feature`
  (promotion is a negotiation, not a copy). Generalizes: any repo running this factory defers work.
  **Scope note for the next run:** F6 item 5's migration of the existing crawl-hardening deferrals was
  deliberately left to that feature's P11, which owns the ledger — do not re-apply it here.

## 2026-08-05 — crawl-hardening F10: temp_index.sh rebuilds stale binaries
`decision=applied commit=9f85313 target=.agents/factory/bin/temp_index.sh`
- **Rationale:** a false PASS in the factory's primary behavioral gate. Building only when a binary was
  absent meant a drive after a code change measured the previous phase's artifact; it bit for real in
  crawl-hardening P9, where the drive "confirmed" behaviour the binary did not have. Inverted failure
  mode — a stale binary usually lacks the change, so the drive agrees with the old code instead of
  failing. Now cargo decides unconditionally; verified a touched `src/` triggers a real recompile and a
  clean tree costs ~1.1 s. Strengthens the gate, weakens nothing.

## 2026-08-05 — crawl-hardening F2: artifact-deliverable R-IDs graded by the orchestrator
`decision=applied commit=8236edc target=.agents/factory/templates/TECH.md + skills/xdu-review/SKILL.md`
- **Rationale:** `xdu-review` blinds its reviewer to all of `spec/`, so an R-ID satisfied by a committed
  document there is structurally unverifiable by it — 2 of 10 R-IDs in cycle 1, closed by a hand-written
  instruction mid-pass. Made a rule: flag such R-IDs in `TECH.md`, name them out-of-scope in the
  delegation prompt, orchestrator verifies, `REVIEW.md` records who verified each. **Blindness is
  unchanged** — this was checked explicitly, since a fix that loosened it would need a human override.

## 2026-08-05 — crawl-hardening F7: stated fallback when `scdoc` is absent
`decision=applied commit=0af5f08 target=.claude/skills/xdu-review/SKILL.md`
- **Rationale:** the spine required a man-page render with no fallback and no way to report the tool's
  absence. Cost measured, not theorized: `doc/xdu.1.scd` had not compiled for six commits, through a
  full review cycle, because nothing on that host could render it. Now: inspect `--help` vs the `.scd`,
  record the unavailability in `REVIEW.md`, flag it as an unclosed gap. **First draft told the reviewer
  to install scdoc and was rejected during self-review** — that session is read-only and allowed-tools
  grants no package manager, so it would have been a step/allowed-tools mismatch. Points at
  `AGENTS.md`'s documented one-liner instead.

## 2026-08-06 — crawl-hardening H1: re-arm the human sign-off gate for `src/crawl.rs`
`decision=applied commit=5b8baa9 target=.agents/skills/xdu-review/SKILL.md + factory/templates/REVIEW.md`
- **Rationale:** not a `META.md` finding — found by the Safety §1 re-derivation this run, and the most
  valuable item in the batch. The mandatory human-gate trigger list is restated in **four** places; P12
  added `src/crawl.rs` to `invariants.md` and `review-rubric.md` when `finalize()` moved there, but not
  to `xdu-review/SKILL.md` or `templates/REVIEW.md` — the two the orchestrator actually reads and
  writes from. A CONFIRMED finding in the atomic finalize or the reserved-name guard did not fire the
  gate by the executed path. Cycle 4's C4-F1 *was* a `crawl.rs` finding and the gate fired only because
  the orchestrator also happened to read `invariants.md`. Widen-only; both new copies now name
  `invariants.md` as authoritative and say they may only widen to match. Also: §2b/§2c were outside
  every "§1–§12" auto-CRITICAL range, so all four range references now include lettered subsections.

## 2026-08-06 — crawl-hardening F13: reserved names as a class (already landed; rubric resynced)
`decision=applied commit=685b90d target=.agents/factory/review-rubric.md`
- **Rationale:** the finding was **stale** — P13 had already landed both halves (`invariants.md:89-95`
  class restatement + same-commit obligation, `:101-103` the paired both-directions reviewer question),
  and `src/crawl.rs` genuinely iterates `lib::RESERVED_INDEX_NAMES`. Applying the recommendation
  verbatim would have rewritten correct text. The real residual was one hop away and was F13's own
  failure shape recurring: the rubric still described `crawl.rs` as "`__root__` collision rejection" —
  the one-name framing F13 blames — and had never picked up `lib.rs`'s layout constants. Now
  byte-identical to the authoritative list. Gated path *set* unchanged; only parentheticals moved.

## 2026-08-06 — crawl-hardening F1: commit category maps from `kind`, refactor included
`decision=applied commit=792a771 target=.agents/skills/xdu-feature,xdu-plan/SKILL.md`
- **Rationale:** `xdu-feature` said "`fix` for `kind: fix`, else `feature`" and `xdu-plan` said
  "`fix`|`feature`", while `xdu-build` already mapped category from kind and claimed "the same house
  style as `xdu-feature`/`xdu-plan`" — a **false** cross-reference. Every commit on the crawl-hardening
  branch disobeyed the instruction and used `[refactor]`, so this ratifies correct practice rather than
  changing it. Class-closed across all five lifecycle skills and both templates, not just the two named.
  Phrased as "the `kind` verbatim" so a fourth kind needs no fourth edit — enumerate-and-go-stale is the
  failure that produced the finding.

## 2026-08-06 — crawl-hardening F12: operating-manual drift is a flaggable class with a severity
`decision=applied commit=985d43b target=.agents/factory/review-rubric.md`
- **Rationale:** cycle 2's reviewer found real greppable drift and had no slot for it, rated MEDIUM, and
  wrote a paragraph justifying why not HIGH. Adds scope class 5 and slots it into the **existing**
  severity table: HIGH when the stale text degrades a downstream gate, MEDIUM otherwise. Limiter is
  "introduced or moved **by this diff**" — class 5 sits directly above "do NOT report style nits" and
  must not become licence to grade prose. F12's "seen again" half was cut from 11 lines to 3 and
  stripped of its self-staling "four places" count, because the general procedure now lives in
  `xdu-build` Step 2.3 (F15) and the list itself was re-armed in `5b8baa9` — three copies of an
  anti-drift rule is the exact failure F12 documents. Net +12 lines on a file `xdu-review` pastes
  verbatim into every blind-reviewer prompt, hence the deliberate terseness.

## 2026-08-06 — crawl-hardening F8: `.scd` authoring footgun recorded by pointer, not by copy
`decision=applied commit=067a68f target=AGENTS.md + .agents/factory/invariants.md`
- **Rationale:** `invariants.md` — the curated gate `xdu-plan` and `xdu-review` both draw from — said
  nothing about `.scd` authoring, so the checklist never raised it. Added as a **pointer** to
  `AGENTS.md`'s Commands section, not a duplicate: `invariants.md` declares itself in lockstep with
  `AGENTS.md`, so copying prose across both is precisely the drift F12 records, and this follows the F7
  precedent in this ledger. §13 not §10, because a mis-escaped asterisk is a docs defect (HIGH) and
  promoting it would make every markup slip auto-CRITICAL. `AGENTS.md` gained two facts paid for
  experimentally and recorded nowhere, both **re-verified here against scdoc 1.11.5** rather than taken
  from the finding: a line-start `\.` deletes the *entire* line at exit 0, and the `*/*` hazard is not
  mechanically fixable (corrupt glob in `xdu.1.scd`, legitimate `/`-key heading in `xdu-view.1.scd`).
  **Rejected from the finding:** its "keep the body ≤ 79 cols" rule — `doc/xdu.1.scd:9` is 121 cols, so
  the invariant would manufacture a finding on the next review.

## 2026-08-06 — crawl-hardening F15: scope the class before remediating an instance
`decision=applied commit=d8eae3f target=.agents/skills/xdu-build/SKILL.md`
- **Rationale:** `xdu-review` reports `file:line` findings (correctly — that is the evidence spine) and
  `xdu-build` treated the location as the work item; nothing converted an instance into its class. Three
  times on one feature (C3-F1, F13, F12's seen-again gate-disarm), each found by a later review that
  should not have had to. The procedure is not invented — P14 practised it and immediately found the
  `--version` falsehood in four live files rather than the two the review named. New Step 2.3, and the
  rule that a remediation `verify:` **asserts the pattern is absent**, which is strictly stronger
  because it fails on undiscovered sites. The frozen-sites carve-out is bounded by *kind* (point-in-time
  evidence only) and explicitly names `.agents/**` and `AGENTS.md` as **live**. Known unresolved
  tension recorded in the commit: `allowed-tools` grants the `Grep` tool but not `Bash(grep *)`, so a
  pattern-asserting `verify:` string prompts; the sweep routes through `Grep` and adds no new mismatch,
  and widening the permission was deliberately **not** taken.

## 2026-08-06 — crawl-hardening F3 + F5: name the two ways a green gate lies
`decision=applied commit=f1205a7 target=.agents/skills/xdu-build/SKILL.md + xdu-plan/SKILL.md`
- **Rationale:** one paragraph, not two — F3 (green because nothing ran) and F5 (green because one
  result absorbed another) are the same failure viewed twice. F3 was partly fixed: P12 put the self-skip
  fact in `AGENTS.md`'s Testing section, but the unfixed half was load-bearing, since Step 4 is where
  the agent decides to advance state. F5 landed in **both** `xdu-plan` Step 6 (primary — a bundle never
  written cannot be mis-measured) and `xdu-build` Step 4 (backstop — the build may amend `TECH.md`, so a
  second lever can appear after sign-off); build-only placement would leave the bundled phase getting
  written every cycle. The "confirm the mutation landed" clause is explicit because that is where the
  discipline nearly failed in P10: byte-mode `perl` silently did not substitute a UTF-8 en dash, so a
  negative test passed against an unmutated file and would have certified an unexercised gate.

## 2026-08-06 — crawl-hardening F9: CI asserts the published man-page text, not exit 0
`decision=applied commit=9c579cf target=.github/workflows/test.yaml + skills/xdu-review/SKILL.md`
- **Rationale:** the packaging job rendered `doc/*.scd` and checked only non-empty output, which is how
  a `doc/xdu.1.scd` defect survived six commits and a full review cycle. **Verified both directions
  before committing:** clean tree passes all 10 literals; un-escaping the glob at `doc/xdu.1.scd:113`
  (mutation confirmed present first) leaves scdoc at **exit 0**, publishes `OUTDIR//.parquet`, and turns
  the gate **red**. The assertion flattens newlines first because `mandoc` rewraps freely, so a
  wrap-sensitive grep would pass or fail on formatting rather than content; a source-side tripwire
  covers the roff-control-line class, which is invisible from the rendered side. Installs `mandoc` +
  `bsdextrautils`; not mirrored into `release.yaml` on purpose (its tag content is already gated by
  `test.yaml`, and a second literal list is a second thing to drift). **Stated tax:** the literal list
  is hand-maintained, so renaming an on-disk artifact reddens CI until someone updates it — the intended
  failure direction. The `xdu-review` half points at `AGENTS.md` rather than restating the rules, per
  F7; F7's scdoc-absent fallback is untouched (F7 = "the tool is missing", F9 = "the tool ran and lied").

## 2026-08-06 — crawl-hardening F11 + F14: verdict carve-outs deferred, rework required
`decision=deferred commit=— target=.agents/factory/review-rubric.md`
- **Rationale:** the largest and most confidently-argued proposal in the batch, and the one with a
  **working abuse case** — which is exactly the Safety §3 warning sign, since both findings argue to
  loosen a non-negotiable gate and both warned in their own text that they must not become an escape
  hatch. The drafted "already recorded" predicate was *"present in the tree the review graded"*, which
  is **weaker than F11 itself asked for** ("before the review began"). Mechanical exploit, no bad faith
  needed: a CONFIRMED non-destructive HIGH blocks at cycle 1; during normal remediation `xdu-build`
  writes `issues/the-defect.md` + a ROADMAP entry instead of fixing it; at cycle 2 the carve-out fires,
  the block becomes a discretionary ask on the strength of a file the build wrote itself, and the cycle
  counter stops advancing. `xdu-publish`'s staleness gate does not catch it — that only covers back-fill
  *after* the reviewed SHA. **Two further blockers:** the mandatory base-drive provenance step is not
  executable under `xdu-review`'s declared `allowed-tools` (no `git worktree`, no `cargo build`, and
  `temp_index.sh` roots at `pwd`) — the same step/allowed-tools class this ledger records rejecting in
  F7's first draft; and the routing table was to land in `review-rubric.md`, which `xdu-review` pastes
  **verbatim into every blind-reviewer prompt**, teaching the blind reviewer two exculpatory routes it
  is forbidden to act on, at +63% file length. **For the next run:** require the record to exist in
  `base` (or an explicit prior human acceptance recorded in an earlier `REVIEW.md` cycle section); make
  provenance **conditional** (default `introduced`; a base drive required only to *claim*
  `pre-existing`, putting the cost on the carve-out user); and put the routing table in
  `xdu-review/SKILL.md` Step 4, leaving the rubric a two-line pointer.

## 2026-08-06 — crawl-hardening F4: label unrun code sketches in research briefs
`decision=deferred commit=— target=.agents/skills/xdu-plan/SKILL.md + xdu-build/SKILL.md`
- **Rationale:** real and confirmed still-open — `research/03-benchmark-design.md:27-44` is a finished-
  looking `python` block sitting under verified ground truth, and its `di%4` line collapsed
  `dirs_per_part` onto four directories. A good two-sentence fix is drafted (a `SKETCH (unrun)` /
  `VERIFIED (executed: …)` banner in `xdu-plan`, and "research code is a design sketch to re-derive,
  not source to transcribe" in `xdu-build`). Deferred only to respect the ~8-per-run cap; it is the
  lowest-value item in the batch and depends on subagent compliance. Nothing breaks if it waits.

## 2026-08-29 — readers-autoload-parquet-at-runtime F6: `xdu-publish` must read the CI rollup before merging
`decision=applied commit=9ecfd5c target=.agents/skills/xdu-publish/SKILL.md + skills/xdu-review/SKILL.md`
- **Rationale:** the skill whose one irreversible action is a merge could reach it having never asked
  whether the project's own automation passes. `review.verdict` answers "did a human-equivalent approve
  this code", not "is CI green", and `main` has no branch protection to catch the difference — three
  red checks were one `gh pr merge` away from landing unexamined. New Step 1 item 4 (`gh pr checks`,
  STOP on any failing required check, override recorded in the PR body) plus a Safety Principle naming
  the two questions as distinct. **The finding was corrected twice.** Its recommended `gh run list`
  matches **no** granted pattern, so the frontmatter gained `Bash(gh run *)` in the same commit — the
  step/allowed-tools class this ledger records rejecting in F7's first draft and again in F11/F14. And
  its `xdu-review` half was **not** taken as written: that session has no `gh`, and at review time
  there is usually no PR and often no pushed branch, so "read the rollup" is unexecutable there. Took
  the sharper fix instead — Step 3 must record "**not observed**", never "not triggered"/"satisfied",
  for a gate whose state it did not observe. That closes the actual reported failure (a red man-page
  gate reported as owed-nothing) at zero tooling cost.

## 2026-08-29 — readers-autoload-parquet-at-runtime F4: a refusal that names an alternative is policy
`decision=applied commit=5de34c1 target=.agents/skills/xdu-build/SKILL.md`
- **Rationale:** the `AGENTS.md` half had already landed; what was missing is the **reflex**, and its
  absence had a measured cost — `command /bin/rm -f` ran here, deliberately defeating a guardrail the
  maintainer installed on purpose, and left `/usr/bin/false` installed as `target/release/xdu` for two
  later phases to measure. Generalized past deletion to **any** refusing shell function, wrapper or
  hook, naming `command` / `\cmd` / `env` / `sh -c` / absolute path as the bypasses, STOP-and-ask when
  the named alternative is unavailable, and an explicit ban on writing a bypass into a `verify:`, where
  it would propagate to every later phase. Strengthens a guardrail; weakens nothing.

## 2026-08-29 — readers-autoload-parquet-at-runtime F3: a gate's cleanup is part of the gate
`decision=applied commit=1aecae6 target=.agents/skills/xdu-build/SKILL.md`
- **Rationale:** Step 4's hollow-gate guard was entirely about the *assertion* (vacuous, skipped,
  aggregated); nothing said setup/teardown fails **independently of the verdict**. The P1 incident is
  the proof: the gate reported exactly the result it was designed to report while its cleanup silently
  no-opped, so it read as a clean run. Now the cleanup's post-condition must be asserted too, and a
  gate that verdicts correctly but leaves the build tree poisoned is **red**. Same root as F4 (the
  shadowing mechanism) and F5 (the same failure through the reviewer's door).

## 2026-08-29 — readers-autoload-parquet-at-runtime F5: reviewer cleanliness past `git status`
`decision=applied commit=da6d8e8 target=.agents/factory/review-rubric.md + skills/xdu-review/SKILL.md`
- **Rationale:** the rubric defined "clean" as no tracked-file edits with `git status --porcelain`
  empty as the hand-back check — and `target/` is git-ignored, so both conditions hold perfectly while
  `/usr/bin/false` sits installed as `target/release/xdu`. The natural negative control for a
  "tests must drive the built binary" requirement is exactly that mutation, so the hand-back check is
  blind to precisely the state the control touches; this run only escaped it because the orchestrator
  hand-wrote a cleanup instruction the skill never asked for. Three places in lockstep: the rubric's
  conduct bullet, `xdu-review` Step 2's delegation conduct rule, and Step 3's orchestrator-side
  confirmation. **+4 lines on `review-rubric.md`**, which is pasted verbatim into every blind-reviewer
  prompt (the F12 entry above flags that cost) — justified because reviewer conduct is what that
  section is for.

## 2026-08-29 — manpage-literal-assertion-fails-on-ubuntu F9: close the class by evaluating a predicate
`decision=applied commit=e900f2a target=.agents/skills/xdu-build/SKILL.md`
- **Rationale:** Step 2.3 (added by crawl-hardening F15, `d8eae3f`) says to sweep the *class*, but all
  its worked examples are `Grep`-able identifiers, so the natural reading is a text sweep — and
  following it literally still shipped a §13 invariant that was **false when written**, because the
  defining property was *"how many times does this literal appear in the rendered page"*, computable
  only by rendering and counting. Now: state the property as a predicate, find sites by **evaluating**
  it, and when evaluating needs a build or a render, encode the predicate itself as the gate. The
  numbers are kept (grep found 3, the predicate found 4 more) because the concrete cost is what makes
  the rule stick. **Appends after** the `.agents/**` scope sentence, so the still-open F8 — which
  rewrites that sentence — stays cleanly appliable.

## 2026-08-29 — manpage-literal-assertion-fails-on-ubuntu F4: name a safe revert for the mutation test
`decision=applied commit=1ced1b8 target=.agents/skills/xdu-build/SKILL.md`
- **Rationale:** Step 4 mandates "mutate, confirm red, revert" and Step 7 commits *after* Step 4, so
  the working tree is the only copy of the phase's work while the mutation is live — and the mutation
  target is very often the file the phase just rewrote. Every reflex revert (`git checkout` /
  `git restore` / `git stash`) resolves against HEAD, i.e. the state *before* the phase: the
  deliverable is discarded and **no command fails**. Both shapes named, per the finding's "seen
  again": working-tree mutation → copy aside, restore with `cp`, confirm with `cmp -s`;
  committed-state mutation (a gate reading its inputs through `git archive HEAD`) → detached
  `git worktree`, commit inside it, `git worktree remove --force`, branch never moves.
  **Frontmatter widened in the same commit** — `Bash(git worktree *)`, `Bash(git archive *)`,
  `Bash(cp *)`, `Bash(cmp *)` — because an instruction naming commands the skill cannot run is the
  mismatch class this ledger has twice rejected. All are non-destructive except
  `git worktree remove --force`, which `AGENTS.md` already carves out as a tool's own bookkeeping.

## 2026-08-29 — manpage-literal-assertion-fails-on-ubuntu F2: point at the man-page normalization, don't restate it
`decision=applied commit=e5f205a target=.agents/skills/xdu-review/SKILL.md`
- **Rationale:** `xdu-review` held the **fourth** restatement of a rule declared to live in three
  places, and it was the wrong one — it taught `scdoc | mandoc | col -b` "diffed against the literal
  you intended", the layout-sensitive form this very feature exists to correct, so a reviewer following
  it on a homebrew box gets the same false green that let the defect reach CI. Replaced with a pointer
  to `AGENTS.md`'s Commands section naming all three forms (reading, whitespace-stripped presence, and
  **counting** for a duplicated literal), copying no command — the F7/F8 by-pointer-not-by-copy
  precedent above. `REVIEW.md` must now record *which form* confirmed each literal. **Known residual,
  deliberately not fixed:** the pipeline's `mandoc`/`col`/`tr`/`grep`/`wc` are absent from
  `xdu-review`'s `allowed-tools` (only `Bash(scdoc *)` is granted) and whether that prefix covers the
  whole pipeline is untested. Widening to `Bash(grep *)` is what F15 explicitly declined for
  `xdu-build`, so this is recorded rather than taken. Pre-existing — line 62 already named `mandoc`.

## 2026-08-29 — manpage-literal-assertion-fails-on-ubuntu F10: cite only enforcement that survives the merge
`decision=applied commit=5ce1c67 target=.agents/skills/xdu-build/SKILL.md`
- **Rationale:** `verify:` harnesses are authored and run exactly like durable tests — same shell, same
  red/green, same mutation discipline — and Step 2.3 pushes toward updating the permanent manual in the
  same breath as building the temporary harness, which is precisely when the two get conflated. It
  happened: a §13 invariant cited a `spec/{slug}/verify/` harness as its enforcement, telling the next
  agent a hole was closed when that harness becomes a retained *record* the moment the spec merges. Two
  halves — the lifetime stated where `verify:` is introduced (Safety Principles), and the sweep rule in
  Step 2.3 (cite `tests/` / CI / code only; otherwise say the rule is hand-maintained and file the
  durable gate as an `issues/{slug}.md` + `ROADMAP.md` pair). Confirmed live before editing:
  `spec/manpage-literal-assertion-fails-on-ubuntu/verify/` exists and `invariants.md:214` cites it.

## 2026-08-29 — manpage-literal-assertion-fails-on-ubuntu F6: severity-routed verdict deferred a second time
`decision=deferred commit=— target=.agents/factory/review-rubric.md`
- **Rationale:** the **third instance** of one proposal. crawl-hardening F11+F14 were deferred on
  2026-08-06 with a working abuse case; this is the same ask ("MEDIUM/LOW return
  `approved-with-findings` instead of blocking") arriving from a different feature, with a sharpened
  second half about **cycle accounting** as well as the verdict. The recurrence is real signal and the
  observed cost is real: R1–R7 passed in all three cycles here, every finding was prose in the
  operating manual, and cycle 3 still consumed the last of the bounded budget and forced a STOP whose
  "non-convergence" wording implies the fix was failing. It remains a Safety §3 change to the
  review↔build loop and does not belong in a batch of eight — it needs its own run, taking the rework
  constraints already drafted in the F11/F14 entry above (the record must exist in `base` or in a prior
  `REVIEW.md` cycle's recorded human acceptance; provenance conditional, default `introduced`; routing
  table in `xdu-review/SKILL.md` Step 4, **never** in the rubric handed to the blind reviewer). Add
  from this instance: route the **cycle counter** as well as the verdict, and keep product regression
  and any unmet R-ID unconditionally blocking. **A fourth instance is not new evidence** — what is
  missing is the design under those constraints, not more reports of the symptom.
