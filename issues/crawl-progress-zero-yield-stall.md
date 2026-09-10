---
status: unshaped
kind: fix
appetite: small
---

# Crawl progress: quiet lines show no elapsed when the walker yields nothing

> **Pre-shaped candidate, not a contract.** This file records deferred work in enough detail that a
> future session does not have to re-derive it. It is **not** graded by `xdu-review` and must never be
> copied into `spec/{slug}/GOAL.md` verbatim — `/xdu-feature` promotes it, and that is where appetite,
> non-goals and the R-IDs get negotiated with a human. The `status:` field above is the guard, with
> three states: `unshaped` means proposal; `shaped on <branch> (<date>) — see spec/<slug>/GOAL.md`
> means a cycle adopted it and is still in flight; `resolved on <branch> (<date>) — see
> spec/<slug>/` means the cycle landed on `main`. Only `/xdu-feature` writes `shaped`, only the
> landing cycle writes `resolved`, and `/xdu-roadmap` deletes the file once the landing is confirmed.
>
> Deliberately **not** named `GOAL-{slug}.md`: every other GOAL in the factory is a locked contract, so
> a file carrying that name eventually gets treated as one.
>
> Body sections mirror [`GOAL.md`](GOAL.md) so promotion is a move-and-fill rather than a rewrite.

## Problem

The skewed-tree pass gives a quiet partition evidence of life (`scanning N dirs, Ns, no files yet`),
but the refresh that renders it runs only when the walker yields an entry: the tick block sits
inside `for entry in walker` (`src/bin/xdu.rs:372-441`), gated on the 100 ms interval, while the
line's content until the first tick is the bare `scanning...` set at partition start (`:213`). A
partition whose reads block entirely — no yield before the first entry, or a stall between
yields — therefore keeps the bare line with no dirs count and no elapsed, for as long as the
blockage lasts.
Two consequences follow. A quiet line carries no duration, so quiet-for-seconds and
quiet-for-an-hour read identically; the operator gets the wedged-against-moving-global reading but
not its age. And a stall across every partition at once freezes the global line too (`:434` sits in
the same gated block), leaving no motion anywhere on the display.

## Why it was deferred

The yield-gated structure is pre-existing (on `main` the per-file 100 ms block had the same shape);
the skewed-tree pass preserved it and widened it to directories rather than restructuring it. A
fix needs a yield-independent refresh — a timer that re-renders elapsed without entries
flowing — which is new concurrency inside the §7 crawl scaffold (`src/bin/xdu.rs`, high blast
radius) plus timing-sensitive tests, on a branch whose appetite is small and whose reported
symptom (flowing directories, no files) is already covered. Triaged as R2's
wedged-reading-quiet case, with the missing-elapsed half recorded here instead of fixed there.
Found by `xdu-review` cycle 1 (F1, PLAUSIBLE — never reproduced against a wedged walker, no
harness here can block yields on demand).

## Outcome / vision

An operator watching any stall can answer how long each quiet line has been quiet, from the TTY
display alone, while the crawl itself keeps the single-pool plus work-stealing model unchanged.

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion. Prefer EARS phrasing (see
[`.agents/factory/ears.md`](../.agents/factory/ears.md)).

- **R1** — WHILE a partition has produced no walker entry for longer than the quiet interval, its
  line SHALL show elapsed-since-activity rather than a frozen bare `scanning...`.
- **R2** — WHILE every partition is quiet at once, the global line SHALL still refresh on its own
  tick so the display never freezes display-wide.
- **R3** — The refresh SHALL NOT change walk throughput: no extra work per entry, no new
  contention on the pool or the work queue.

## Notes

- Related: `spec/crawl-progress-misleads-on-huge-trees/REVIEW.md` (cycle 1, F1 triage);
  `issues/crawl-progress-misleads-on-huge-trees.md` (the parent symptom, in flight on
  `fix/crawl-progress-misleads-on-huge-trees`).
- Found by: `crawl-progress-misleads-on-huge-trees` review cycle 1.
