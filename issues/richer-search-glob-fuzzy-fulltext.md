---
status: unshaped
kind: feature
appetite: big
---

# Richer search: glob, fuzzy, full-text, content-type

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

Regex path matching is powerful but unfriendly: most users think in globs (`*.py`), not anchored
regex (`\.py$`). The matching surface is regex-only across `xdu-find`, `xdu-view`, and `xdu-rm`,
which narrows the audience for all three query tools.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the
back-reference; no new scoping was done.

## Outcome / vision

Glob syntax as a gentler alternative to regex, fuzzy matching for approximate filenames, and an
evaluation of DuckDB's full-text search — without giving up regex power. Content-type filtering
("all video files over 1 GB") is the natural next step but depends on MIME metadata living in the
index, so it waits on the schema-evolution work.

## Sketch of the acceptance criteria

- **R1** — `xdu-find`, `xdu-view`, and `xdu-rm` SHALL accept glob patterns as an alternative to
  regex path matching.
- **R2** — Fuzzy filename matching SHALL be available alongside exact matching.
- **R3** — DuckDB full-text search SHALL be evaluated as a richer query option.

## Notes

- Depends on: on-disk schema versioning plus the richer schema for content-type filtering.
- Found by: original roadmap; back-reference retrofitted 2026-09-07.
