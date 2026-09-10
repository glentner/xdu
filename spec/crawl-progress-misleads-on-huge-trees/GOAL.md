# GOAL — Crawl progress misleads on huge trees

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).
> Keep this at the right altitude: solved and bounded, but not over-specified — leave design
> freedom for the plan.

- **slug:** crawl-progress-misleads-on-huge-trees
- **kind:** fix
- **appetite:** small

## Problem

On very large filesystems (observed on Lustre scratch, screenshot 2026-08-05), the interactive
crawl display settles into a state that reads as hung: one partition shows file counts while every
other line sits at `<partition>: scanning...` for a very long time. An operator watching this
cannot tell a healthy skewed crawl from a stuck one, and on shared scratch that distinction
decides whether to let a job run or kill it.

Two display behaviors combine to produce it (verified on `main`, pre-existing). A partition start
sets `<part>: scanning...` (`src/bin/xdu.rs:210`); counts and speeds appear only once files flow,
inside the per-file 100 ms update block (`:359-427`). A partition stuck in slow metadata traversal
— millions of directories, no file completed yet — therefore shows a frozen line with no way to
distinguish "working" from "wedged". Separately, the `[Tn]` token on a progress line (`:413-420`)
names the bar-owning driver thread, while the actual walking happens on the single shared rayon
pool with work-stealing across all active walkers — so the line claims an exclusive ownership the
threading model does not provide, and the silent lines hide drivers starved of pool time rather
than idle. Throughput is likely correct; the display reports its inverse.

## Outcome / vision

An operator watching a skewed crawl can answer, from the TTY display alone: which partitions are
moving, what each line's label actually claims, and whether a quiet line is working or wedged.
The crawl itself runs exactly as before — the single-pool plus work-stealing model stays as is,
and the fix buys no display honesty at the price of throughput.

## Acceptance criteria (the contract)

- **R1** — WHEN progress bars render on a TTY, each in-progress partition line SHALL NOT carry a
  label that implies exclusive single-thread ownership of that partition (today: the `[Tn]`
  driver id). The line either drops the claim or states one that matches reality.
- **R2** — WHILE a partition has yielded no completed files for longer than a stated interval,
  its line SHALL show evidence of life rather than a frozen `scanning...`, so a slow-metadata
  walk reads as working and a truly wedged one reads as quiet against a moving global line.
- **R3** — WHILE any crawl is in progress on a TTY, the global line SHALL keep reporting
  aggregate totals, so a skewed run still shows forward motion when per-partition lines stall.
- **R4** — The crawl SHALL keep the single-pool plus work-stealing concurrency model with no
  throughput regression attributable to the display change.

## Non-goals (no-gos)

- Machine-readable log output for scripted and cron-driven runs — the companion seed
  (`issues/indexer-machine-readable-log-output.md`) owns that surface; this GOAL changes the
  interactive TTY display only and leaves the non-TTY stderr posture untouched.
- Any change to the index format, the partition scheme, or the completion-marker contract.
- The exact liveness signal (directories visited, time-since-last-file, queue position), the
  quiet-interval value, and the replacement (or removal) for `[Tn]` — `xdu-plan` chooses those;
  this GOAL grades only the observable behavior above.

## Clarifications

- **Q:** TTY display only, or include non-TTY heartbeat lines in this GOAL? — **A:** TTY display
  only; log output stays in its companion seed (resolved 2026-09-09).
- **Q:** How prescriptive should the GOAL be about the `[Tn]` label? — **A:** States the
  observable rule (no false ownership claim); the mechanism is `xdu-plan`'s (resolved 2026-09-09).
- **Q:** Should the GOAL pin the exact liveness signal for quiet partitions? — **A:** No;
  requires visible evidence of life after a stated interval, signal and interval are `xdu-plan`'s
  (resolved 2026-09-09).

## Related materials

- Seed: `issues/crawl-progress-misleads-on-huge-trees.md` (pre-existing on `main`; found by the
  maintainer from Lustre scratch observation; recorded 2026-09-07).
- Companion (out of scope): `issues/indexer-machine-readable-log-output.md`.
- Source anchors: `src/bin/xdu.rs:177` (per-driver bar), `:210` (`scanning...`),
  `:359-427` (counts only once files flow), `:413-420` (`[Tn]` driver id).
- Constraint: `AGENTS.md` §7 (shared rayon-pool concurrency) and §13 (non-TTY stdout stays clean).
