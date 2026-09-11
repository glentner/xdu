---
status: unshaped
kind: feature
appetite: small
---

# Shared-index API contract: typed operations, not SQL

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

`xdu-find` exposes DuckDB SQL, which is exactly right for a local index the caller already owns and
unshippable for a served one. DuckDB "executes SQL with the full privileges of the user running it,
much like a shell" — its own security documentation says so — and its dialect includes
`read_parquet()`, `read_csv()`, `glob()`, `ATTACH` and `COPY ... TO`. Accepting caller-written SQL
is therefore accepting arbitrary file read and write as the service account, and with `httpfs`
loaded it is also SSRF: a fetch of a cloud instance-metadata endpoint hands back the service's own
credentials.

Appending a scoping predicate does not rescue it. A caller-supplied CTE, `UNION ALL`, or correlated
subquery reaches the base relation before any wrapper applies, so the filter is not a boundary. And
a caller-authored aggregate over a billion rows is a free out-of-memory: DuckDB has no
statement-timeout setting to lean on.

Separately, the shared index will have at least three consumers — the CLI readers, the Wasm client,
and whatever operators script — so it needs a surface that outlives any one schema change.

## Why it was deferred

New work, not a factory-pass deferral. It comes out of the 2026-09-11 design pass on
[`access-scoped-queries.md`](access-scoped-queries.md). Recorded separately from the service itself
because the contract can be settled before a server exists, and both clients bind to it.

## Outcome / vision

The API surface is the set of operations the readers actually need — prefix rollup to depth N, top-N
by size or count, filters on owner/age/size/pattern, histograms — each a typed request the service
compiles into SQL it authored itself. Nothing caller-written is ever parsed, so there is no
injection surface; the visibility predicate is structural rather than appended; and every operation
has a known cost shape that can be bounded per endpoint instead of bounding the unbounded. It is
also the shape a browser wants.

Ad-hoc SQL survives where it is safe: an operator endpoint for principals who could already read the
filesystem directly, where the expressiveness costs nothing.

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion.

- **R1** — The API *shall* accept only enumerated, typed query operations; no caller-supplied SQL
  text *shall* reach DuckDB.
- **R2** — Each capability `xdu-find`, `xdu-view` and `xdu-rm --dry-run` offer against a local index
  *shall* have an API operation, so no reader loses function when pointed at a shared index.
- **R3** — *Where* an ad-hoc SQL operation is offered, it *shall* be reachable only by a principal
  explicitly designated as an operator.
- **R4** — The API *shall* be versioned such that a client built against one version keeps working
  against a service that has since added operations.

## Notes

- Related: [`xdu-api-query-service.md`](xdu-api-query-service.md) (implements it),
  [`shared-index-client-routing.md`](shared-index-client-routing.md) and
  [`xdu-web-client.md`](xdu-web-client.md) (both bind to it).
- Found by: shared-index design pass, 2026-09-11.
