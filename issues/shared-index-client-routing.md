---
status: unshaped
kind: feature
appetite: small
---

# Client routing to a shared index

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

`xdu-find`, `xdu-view` and `xdu-rm` take an index path and open it. Once some trees are served by
`xdu-api` instead of being indexed locally, every user would have to know which is which and invoke
the tools differently — and every center would have to teach them. The distinction is an artifact of
how the index is stored, not something a user asking about `/scratch` should have to hold.

## Why it was deferred

New work, not a factory-pass deferral, from the 2026-09-11 design pass on
[`access-scoped-queries.md`](access-scoped-queries.md). It is plumbing that cannot be built before
the API contract it speaks.

## Outcome / vision

A routing table maps path prefixes to service endpoints. Centers ship it through config management
under `/etc/xdu/`, with an environment variable for module files and testing and a user-level file
last, so the common case is that a user runs `xdu-find /scratch/...` and never learns a service was
involved.

Routing is a convenience and never a control. The client is untrusted by construction, so the
service independently validates that a requested prefix is within its jurisdiction; the table only
decides *which* endpoint to ask. Paths are canonicalized before prefix matching — `..`, symlinks,
repeated separators — so a query reaches the service that actually owns the tree, and a rejection
surfaces rather than silently falling back to a local read.

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion.

- **R1** — *When* a queried path falls under a configured shared-index prefix, `xdu-find`,
  `xdu-view` and `xdu-rm` *shall* issue API operations against the configured endpoint instead of
  opening a local index, under the same invocation the user would have used locally.
- **R2** — The routing table *shall* be resolvable from system config, environment and user config
  with a documented, deterministic precedence (order negotiated at promotion).
- **R3** — The client *shall* canonicalize a path before prefix matching, so `..`, symlinks and
  repeated separators cannot route a query to the wrong service.
- **R4** — *If* the service rejects a prefix as outside its jurisdiction, *then* the client *shall*
  surface the rejection and *shall not* fall back to reading a local index.

## Notes

- Depends on: [`shared-index-api-contract.md`](shared-index-api-contract.md).
- Related: [`xdu-api-query-service.md`](xdu-api-query-service.md).
- Found by: shared-index design pass, 2026-09-11.
