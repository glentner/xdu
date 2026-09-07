---
status: unshaped
kind: feature
appetite: big
---

# Richer index schema: owner, group, permissions

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

On large shared filesystems (`/projects/{lab1,lab2,…}`) administrators need "which *user within*
a project is biggest?", plus permission bits to reason about exposure and cleanup. The
`path/size/atime` schema cannot answer per-owner or per-permission questions at all — today that
means falling back to a slow `find`. This is exactly the breaking, cross-cutting schema change
that `AGENTS.md` §1 warns about: it touches `get_schema()`, the crawler, all three readers, and
every documented `read_parquet` example, and it requires a schema version first.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the
back-reference; no new scoping was done.

## Outcome / vision

Owner, group, and mode recorded per file, unlocking per-user and per-permission storage
accounting over shared trees.

## Sketch of the acceptance criteria

- **R1** — The index SHALL record owner, group, and permission bits for every file, behind the
  on-disk schema version.
- **R2** — Queries SHALL attribute size and age to individual users and groups within a shared
  tree.

## Notes

- Depends on: on-disk index schema versioning.
- Related: GitHub issues #2 and #3.
- Found by: original roadmap; back-reference retrofitted 2026-09-07.
