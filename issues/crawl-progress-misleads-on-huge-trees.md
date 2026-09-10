---
status: shaped on fix/crawl-progress-misleads-on-huge-trees (2026-09-09) — see spec/crawl-progress-misleads-on-huge-trees/GOAL.md
kind: fix
appetite: small
---

# Crawl progress output misleads on huge trees

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

On very large filesystems (observed on Lustre scratch), the interactive crawl display settles into
a state that reads as hung: one partition shows file counts, every other line sits at
`<partition>: scanning...` for a very long time. Two mechanisms combine to produce it, and the
display explains neither:

- Each driver thread owns **one** indicatif bar for its whole lifetime, reused across every
  partition it drains (`src/bin/xdu.rs:177`). A partition start sets `<part>: scanning...`
  (`:210`); counts appear only once files flow, inside the per-file 100 ms update block
  (`:359-425`). A partition stuck in slow metadata traversal — millions of directories, no file
  completed yet — therefore shows a frozen `scanning...` with no way to distinguish "working"
  from "wedged".
- The `[Tn]` label names the **bar-owning driver** (`:410-417`), not the threads doing the work.
  The actual file walking happens on the single shared rayon pool with work-stealing across all
  active walkers, so every worker can legitimately pile onto one partition's walk while the other
  drivers wait inside their own iterators. The display then reports the exact inverse of reality:
  one line claims `[T4]` as if a single thread were on that partition, while the silent lines
  hide that their drivers are starved of pool time, not idle.

Throughput is likely correct — work-stealing sends workers where the tasks are — but an operator
watching this cannot tell a healthy skewed crawl from a stuck one, and on shared scratch that
distinction decides whether to let a job run or kill it.

## Why it was deferred

Raised by the maintainer from a production Lustre observation (screenshot 2026-08-05), not found
in a factory pass. It is **pre-existing** on `main`. The fix direction is open (heartbeat activity
per line? per-partition instead of per-driver bars? queue-depth or worker-distribution visibility?
redefining what `[Tn]` claims?), and each option touches the one display operators stare at for
hours — worth shaping deliberately rather than tweaking blind.

## Outcome / vision

An operator watching a skewed crawl can answer, from the display alone: which partitions are
moving, what each line's label actually claims, and whether a silent line is working or wedged.
No throughput regression: the single-pool + work-stealing model (`AGENTS.md` §7) stays as is.

## Sketch of the acceptance criteria

- **R1** — WHEN bars render on a TTY, each in-progress line SHALL state what is actually working
  (partition, and a thread claim that matches the threading model), never a bare driver id that
  implies exclusive ownership.
- **R2** — WHILE a partition yields no completed files for longer than a stated interval, its line
  SHALL show evidence of life (directories visited, time since last file, queue position) rather
  than a frozen `scanning...`.
- **R3** — The global line SHALL keep reporting totals, so a skewed run still shows forward motion
  even when per-partition lines stall.

## Notes

- Related: the companion log-output seed
  [`indexer-machine-readable-log-output.md`](indexer-machine-readable-log-output.md) (the scripted
  half of the same output surface).
- Found by: maintainer, from Lustre scratch observation; recorded 2026-09-07.
