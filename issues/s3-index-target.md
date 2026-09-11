---
status: unshaped
kind: feature
appetite: small
---

# S3 as an index target

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

Indices live on local disk, which ties them to the machine that built them. There is no way to store
an index centrally and point every tool at it.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the back-reference;
no new scoping was done.

## Outcome / vision

The Hive-partitioned layout (`<partition>/<chunk>.parquet`) maps directly onto object-store key
prefixes, so `xdu` writes the index to S3-compatible storage and any tool — or a future web client —
points at it without copying files around. Build-once, read-anywhere indices.

## Sketch of the acceptance criteria

- **R1** — `xdu` SHALL write the Hive-partitioned Parquet index to an S3-compatible bucket/prefix
  instead of a local directory.
- **R2** — The readers SHALL query the S3-backed index without copying it to local disk first.

## Notes

- **Tension flagged 2026-09-11 (not a change to R2).** R2 targets the general reader path and
  stands. But `xdu-api` can only run DuckDB in its hardened, `lock_configuration`-ed posture —
  external access disabled — if the index is on local disk, so the *service* is expected to hold a
  local copy and treat S3 as the distribution mechanism rather than the query path. Worth
  reconciling at promotion: either R2 is scoped to non-service readers, or the service is named as a
  stated exception.
- Related: [`xdu-api-query-service.md`](xdu-api-query-service.md),
  [`access-scoped-queries.md`](access-scoped-queries.md).
- Found by: original roadmap; back-reference retrofitted 2026-09-07.
