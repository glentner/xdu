---
status: unshaped
kind: feature
appetite: big
---

# Permission-aware, access-scoped queries

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

Once ownership and permissions are indexed, the index itself leaks: a non-root user could read
sizes and paths they could never `stat` on the live filesystem. Without scoping, a shared,
centrally built index cannot be safely exposed to the tenants it describes.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the
back-reference; no new scoping was done.

## Outcome / vision

Borrowing from GUFI's shadow-tree model, queries scope so a user only sees data they could
normally access — by default or by explicit flag when `xdu` builds or serves an index as root.

## Sketch of the acceptance criteria

- **R1** — WHEN a non-root user queries an index built as root, they SHALL see only rows for
  files they could normally access.

## Notes

- Depends on: the richer index schema (owner/group/perms).
- Related: GitHub issue #3.
- Found by: original roadmap; back-reference retrofitted 2026-09-07.
