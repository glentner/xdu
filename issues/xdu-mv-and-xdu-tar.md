---
status: unshaped
kind: feature
appetite: big
---

# Bulk-op sibling tools: `xdu-mv` and `xdu-tar`

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

`xdu-rm` proved the pattern — select files with an index query, act on exactly that set, with
dry-run, confirmation, `--safe` re-stat, and deterministic ordering under `--limit` — but
relocation and archiving still go through `xdu-find | xargs` plumbing with none of that
safety. "Move everything untouched in two years to cold storage" and "tar up this user's stale
logs" are single safe commands waiting to exist.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the
back-reference; no new scoping was done.

## Outcome / vision

`xdu-mv` relocates and `xdu-tar` archives exactly the set of files matched by an index query, both
reusing `xdu-rm`'s destructive-safety model rather than reinventing it.

## Sketch of the acceptance criteria

- **R1** — WHEN files match an index query, `xdu-mv` SHALL relocate exactly that set with
  dry-run, confirmation, safe re-stat, and deterministic ordering under `--limit`.
- **R2** — `xdu-tar` SHALL archive exactly the matched set under the same safety model.

## Notes

- Related: GitHub issue #1.
- Found by: original roadmap; back-reference retrofitted 2026-09-07.
