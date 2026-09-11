---
status: unshaped
kind: feature
appetite: big
---

# Authentication (`xdu-login`) and group resolution for a shared index

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

The service can only scope a query if it knows who is asking and which groups they are in, and
neither client can simply tell it. A browser has no POSIX identity at all — no uid, no supplementary
groups, nothing to peer-credential. A CLI on a login node has one, but nothing `xdu-api` can trust
across a network.

Nor is there anywhere for a CLI caller to keep proof of identity between invocations. `xdu-find` is
run in loops and from scripts; a reader that negotiates credentials on every call is both slow and
unusable non-interactively, and one that prompts partway through a query is worse.

The two halves of identity also have opposite freshness requirements, and collapsing them breaks one
or the other. Group membership must be current: someone added to an allocation this morning expects
to see it this morning. The `uid`→name mapping must be pinned to the generation, because centers
recycle uids and a months-old index read against today's passwd attributes one person's paths to
another. One table cannot be both.

Resolving membership from the directory per request would also put LDAP in the query path, as a
latency cost and an availability dependency on every query.

## Why it was deferred

New work, not a factory-pass deferral, from the 2026-09-11 design pass on
[`access-scoped-queries.md`](access-scoped-queries.md). Kept separate from the service because this
is where per-center variation lives — every site has a different identity provider — and that
variation deserves its own research rather than being smuggled into the service cycle.

## Outcome / vision

One authentication path and one authorization path, shared by both clients: bearer tokens, obtained
by OIDC against the center's identity provider for the browser and by device-code flow for the CLI,
with Kerberos where a center already runs it. The service then has a single implementation to review
rather than one per client.

On the CLI side that flow gets its own front end. **`xdu-login`** performs the exchange once, stores
the resulting token under the user's config directory with owner-only permissions, and refreshes it
when it expires — so `xdu-find`, `xdu-view` and `xdu-rm` carry no authentication code of their own,
never prompt mid-query, and work unchanged from a batch script. `xdu-login --status` answers the
first question anyone debugging a missing row will ask: who does the service think I am, and how
many visibility classes did I resolve to?

Group membership is precomputed into a table refreshed on a bounded TTL, out of the query path.
Identity is two tables with different lifetimes — generation-pinned `uid`→name, live-ish name→groups
— which makes the exposure window a number a center can publish rather than a property nobody has
measured: crawl interval plus group TTL.

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion.

- **R1** — `xdu-api` *shall* authenticate every request and *shall* reject an unauthenticated one
  without consulting the index.
- **R2** — `xdu-login` *shall* obtain a token for a configured service endpoint and *shall* store it
  under the user's config directory, readable only by that user.
- **R3** — *If* a stored token is absent or expired *when* a reader issues a query, *then* the
  reader *shall* fail with a message naming `xdu-login` and *shall not* prompt for credentials
  mid-query.
- **R4** — `xdu-login --status` *shall* report the authenticated principal and the number of
  visibility classes resolved for them, without returning index rows.
- **R5** — The CLI readers and the web client *shall* authenticate over the same token path, so the
  service carries one authentication implementation.
- **R6** — The service *shall* resolve group membership from a cached table refreshed on a bounded
  TTL, not by querying the directory per request.
- **R7** — The service *shall* resolve `uid`/`gid`→name through the snapshot pinned to the index
  generation being queried, not the host's current passwd/group.
- **R8** — The service *shall* report its staleness budget — crawl interval plus group TTL — in its
  status output, so an operator can state the exposure window without deriving it.

## Notes

- Depends on: [`index-visibility-classification.md`](index-visibility-classification.md) for the
  generation identity snapshot.
- Related: [`xdu-api-query-service.md`](xdu-api-query-service.md) (consumer);
  [`shared-index-client-routing.md`](shared-index-client-routing.md) — the token and the routing
  table share the user config directory, so their precedence rules should be settled together.
- Found by: shared-index design pass, 2026-09-11.
