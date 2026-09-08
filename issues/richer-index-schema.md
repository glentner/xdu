---
status: shaped on feature/richer-index-schema (2026-09-08) — see spec/richer-index-schema/GOAL.md
kind: feature
appetite: big
---

# Richer index schema: owner, group, permissions, mtime, ctime

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

The `path/size/atime` schema cannot answer two questions administrators actually ask. On large
shared filesystems (`/projects/{lab1,lab2,…}`) "which *user within* a project is biggest?" needs
ownership per file, and reasoning about exposure and cleanup needs permission bits — today both
fall back to a slow `find`. And "changed since the last backup" is a modification-time question the
index cannot ask: atime moves on read (modulo `relatime`), and size misses same-size rewrites, so
neither the time filter nor an index diff can reliably name the changed set. This is exactly the
breaking, cross-cutting schema change that `AGENTS.md` §1 warns about: it touches `get_schema()`,
the crawler, all readers, and every documented `read_parquet` example, and it requires a schema
version first.

## Why it was deferred

A forward-looking intention from the original roadmap (owner/group/perms), expanded during
backup-vision scoping (2026-09-07) with mtime/ctime once incremental backup named them as the
comparator columns diffing needs. Still unshaped; no factory pass has scoped it.

## Outcome / vision

Owner, group, and mode recorded per file, unlocking per-user and per-permission storage accounting
over shared trees — and mtime plus ctime recorded per file, unlocking reliable what-changed
selection for incremental backup and index diffing.

## Sketch of the acceptance criteria

- **R1** — The index SHALL record owner, group, and permission bits for every file, behind the
  on-disk schema version.
- **R2** — Queries SHALL attribute size and age to individual users and groups within a shared
  tree.
- **R3** — The index SHALL record mtime and ctime per file, behind the same version;
  representation (epoch seconds like atime, or finer) is decided at shaping.
- **R4** — Queries SHALL select files modified since a timestamp on mtime — the column the
  bulk-operations theme diffs and its `--safe` re-stat checks against.

## Notes

- Depends on: on-disk index schema versioning.
- Downstream: content-type filtering ("all video files over 1 GB") waits on MIME metadata per
  file; shaping decides whether that column lives in this schema.
- Related: GitHub issues #2 and #3 (owner/group/perms); the bulk-operations theme consumes
  mtime/ctime, `issues/index-diff-incremental-select.md` first.
- Found by: original roadmap; mtime/ctime scope added by backup-vision scoping 2026-09-07.
