---
status: shaped on feature/index-schema-versioning (2026-09-07) — see spec/index-schema-versioning/GOAL.md
kind: feature
appetite: small
---

# On-disk index schema versioning

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

The Parquet schema is three fixed, non-null columns with no version marker on disk
(`src/lib.rs::get_schema`), so any schema change would silently break every existing index and
every reader — the schema-stability invariant (`AGENTS.md` §1) with no escape hatch. Small on its
own, this is the hard prerequisite for enriching the schema at all: it must land before any column
is added.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the
back-reference; no new scoping was done.

## Outcome / vision

Every index carries its own format version, and readers detect it — rejecting or migrating older
indices instead of misreading them.

## Sketch of the acceptance criteria

- **R1** — Every index written by `xdu` SHALL carry an on-disk format version marker.
- **R2** — WHEN a reader opens an index whose version it does not understand, it SHALL refuse
  with a diagnostic (or migrate), never misread rows.

## Notes

- Enabler for: the richer schema and everything downstream of it.
- Found by: original roadmap; back-reference retrofitted 2026-09-07.
