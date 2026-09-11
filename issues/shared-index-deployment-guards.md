---
status: unshaped
kind: feature
appetite: small
---

# Deployment guards for a shared index

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

The whole architecture rests on one premise — that nothing but `xdu-api` can read the index store —
and as designed that premise is upheld by an administrator reading documentation and not making a
mistake. "Do not let anyone else read the bucket" is a negative instruction to an operator, which is
exactly the class of failure the served design was chosen to eliminate. A center that gets it wrong
publishes center-wide path and size metadata with nothing anywhere reporting that something is
wrong.

The object layout leaks independently of the contents. Keys named for their visibility class publish
a group-size census — `<class>/…` at some size tells a reader how much data each group holds — to
anyone who obtains `List`, whether or not they can read a single object.

## Why it was deferred

New work, not a factory-pass deferral, from the 2026-09-11 design pass on
[`access-scoped-queries.md`](access-scoped-queries.md). Separated from the service so it is not the
first thing cut when that cycle runs long: it is the part that makes the design defensible rather
than merely correct.

## Outcome / vision

The software refuses to run misconfigured. `xdu-api` preflights its index store at startup and
declines to serve if the store is reachable by anyone but itself — checking public-access settings,
policy status and ACLs where the backend offers them, and falling back to an anonymous fetch of a
known object, which works on the Ceph RGW and MinIO deployments where provider-specific policy APIs
do not exist. A check that cannot be completed fails closed.

Object names carry no meaning: per-generation random or content-addressed, with the class mapping
held only in the service's own manifest, so enumerating the store reveals no structure even if
enumeration somehow becomes possible. And the deny-by-default store policy ships with the project,
so a center applies a reviewed configuration instead of composing one.

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion.

- **R1** — *When* `xdu-api` starts, it *shall* verify that its index store is not readable by
  unauthenticated or non-service principals, and *shall* refuse to serve if the check fails.
- **R2** — *If* the preflight cannot be completed, *then* `xdu-api` *shall* refuse to serve rather
  than proceed on an unverified store.
- **R3** — The preflight *shall* degrade to a backend-agnostic reachability test where
  provider-specific policy APIs are unavailable, rather than being skipped.
- **R4** — Index object names *shall not* encode a visibility class, uid, gid or group name, so
  enumerating the store reveals no user or group structure.
- **R5** — The project *shall* ship the deny-by-default store policy alongside the service.

## Notes

- Depends on: [`xdu-api-query-service.md`](xdu-api-query-service.md) (enforces its R1 at startup).
- Found by: shared-index design pass, 2026-09-11.
