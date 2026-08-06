---
status: unshaped
kind: feature | fix | refactor
appetite: small | big
---

# {Title}

> **Pre-shaped candidate, not a contract.** This file records deferred work in enough detail that a
> future session does not have to re-derive it. It is **not** graded by `xdu-review` and must never be
> copied into `spec/{slug}/GOAL.md` verbatim — `/xdu-feature` promotes it, and that is where appetite,
> non-goals and the R-IDs get negotiated with a human. The `status: unshaped` field above is the guard:
> while it says `unshaped`, this is a proposal.
>
> Deliberately **not** named `GOAL-{slug}.md`: every other GOAL in the factory is a locked contract, so
> a file carrying that name eventually gets treated as one.
>
> Body sections mirror [`GOAL.md`](GOAL.md) so promotion is a move-and-fill rather than a rewrite.

## Problem

<What is wrong today, for whom, and why it matters. Include the evidence the finder had at hand —
`file:line`, the mechanism, the observed behaviour. This is the expensive part of a deferral and the
part a one-line roadmap seed throws away.>

## Why it was deferred

<Why it was not safe or sensible to fix in the pass that found it: scope, blast radius, a GOAL
non-goal, an appetite boundary, or a dependency on other work. Say plainly whether it is
**pre-existing** (present in `main`) or introduced by that pass — a reviewer will ask.>

## Outcome / vision

<What "good" looks like when this is fixed.>

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion. Prefer EARS phrasing (see
[`.agents/factory/ears.md`](../ears.md)).

- **R1** — WHEN <trigger>, the <component> SHALL <observable response>.

## Notes

- Related: <other `issues/` files, `spec/{slug}/` records, or GitHub issues>
- Found by: <slug + phase, e.g. `crawl-hardening` P9>
