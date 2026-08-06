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
