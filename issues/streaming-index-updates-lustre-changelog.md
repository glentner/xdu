---
status: unshaped
kind: feature
appetite: big
---

# Streaming index updates & Lustre changelog

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

Full re-crawls do not scale to filesystems that change constantly under billions of files: by the
time a crawl finishes it is already stale, and re-running it is enormously expensive. There is no
incremental path today — only full re-crawls.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the
back-reference; no new scoping was done. This is the largest effort on the roadmap; expect several
sub-phases at promotion.

## Outcome / vision

An incremental, Iceberg-style **merge-on-read** index: a base snapshot from a full crawl, augmented
by delta files fed from a storage change stream, with periodic compaction folding deltas back into
the base. The primary driver is the Lustre changelog (a modern Robinhood replacement), behind a
pluggable change-stream abstraction with an inotify-backed backend as the general-purpose demo and
community reference. Open question: whether native Lustre LFS/llapi bindings make the full crawl
itself faster or gentler on metadata servers than going through the VFS.

## Sketch of the acceptance criteria

- **R1** — A constantly-changing, billion-file index SHALL stay queryable without full re-crawls,
  driven by a storage change stream.
- **R2** — The change stream SHALL be a pluggable abstraction, Lustre changelog first with an
  inotify-backed reference backend.

## Notes

- Found by: original roadmap; back-reference retrofitted 2026-09-07.
