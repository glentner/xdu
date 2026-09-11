---
status: unshaped
kind: feature
appetite: big
---

# Crawl-time visibility classification

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

A shared index cannot be scoped per user until it knows who may see each row, and the obvious answer
— the `uid`/`gid`/`mode` columns delivered on `main` — is the wrong one. `xdu` never opens file
contents; it stats. Visibility for a stat-only tool is therefore a property of the **directory
chain**, not the leaf: `x` on every ancestor to traverse, `r` on the immediate parent to enumerate.
A `0600` file under `0755` directories is fully visible to `du` — name, size, mtime — and a `0644`
file under a single `0700` ancestor is invisible. Filtering on the leaf's own owner and mode gets
both cases wrong, in opposite directions.

Resolving the chain at query time means walking ancestors per row, which is precisely the work the
index exists to avoid.

## Why it was deferred

New work, not a factory-pass deferral. It falls out of the 2026-09-11 design pass that redirected
[`access-scoped-queries.md`](access-scoped-queries.md) away from the GUFI shadow-tree framing toward
a served index. Nothing is implemented; the schema columns it builds on are on `main`.

## Outcome / vision

The closure is computed once, top-down, at crawl time: a directory's visibility class is its
parent's class intersected with its own requirement, and files inherit their parent's. The pass runs
over the directory set — orders of magnitude smaller than the file set — so it is close to free next
to the walk itself. Each row carries a class key, and the reader's filter becomes a predicate on one
column instead of an ancestor walk.

Crawling as root is what makes this correct rather than merely convenient: only a privileged crawler
traverses every ancestor and sees the whole chain, so the classification is complete by
construction.

Because the classes form a tree — an unreachable directory implies an unreachable subtree — a
principal's visible set is a union of subtrees, which numbers as intervals and prunes on Parquet
row-group statistics rather than scanning.

The generation also needs its own identity snapshot. Centers recycle uids after account deletion, so
an index storing raw uids and read months later can attribute one person's paths to whoever holds
that uid now. Pinning a `uid`/`gid`→name map to the generation at crawl time makes the index
self-describing and the attribution stable.

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion. Prefer EARS phrasing (see
[`.agents/factory/ears.md`](../.agents/factory/ears.md)).

- **R1** — The `xdu` crawler *shall* record for every indexed row a visibility class derived from
  the permissions of every directory on that row's path, not from the row's own owner and mode.
- **R2** — *When* a directory denies traversal to a principal, the crawler *shall* classify every
  descendant as unreachable by that principal, with no descendant classified more permissively than
  its ancestor.
- **R3** — *If* a path's visibility cannot be represented exactly in the chosen classification,
  *then* the crawler *shall* classify it conservatively — fewer principals, never more.
- **R4** — The crawler *shall* write a `uid`/`gid`→name mapping alongside each generation, so a
  reader resolves identity through the snapshot the index was built with rather than the host's
  current passwd/group.
- **R5** — Classification *shall* cost no more than a stated fraction of baseline crawl wall clock
  on the benchmark tree (threshold negotiated at promotion).

## Notes

- Depends on: richer index schema (owner/group/mode), delivered on `main`.
- Related: [`access-scoped-queries.md`](access-scoped-queries.md) (umbrella),
  [`xdu-api-query-service.md`](xdu-api-query-service.md) (the consumer).
- Found by: shared-index design pass, 2026-09-11.
