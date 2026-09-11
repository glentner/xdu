---
status: unshaped
kind: feature
appetite: big
---

# Permission-aware, access-scoped queries

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

Once ownership and permissions are indexed, the index itself leaks: a non-root user could read sizes
and paths they could never `stat` on the live filesystem. Without scoping, a shared, centrally built
index cannot be safely exposed to the tenants it describes.

The trap is that scoping *looks* like a query-shaping problem and is not. DuckDB has no user model,
no accounts and no row-level security — it runs inside the caller's process with the caller's
privileges — so any filter the reader applies is a convenience for display, not a boundary. A user
who can open the index files reads every row in them regardless of what the tool chose to print.

## Why it was deferred

Originally a forward-looking intention from the first roadmap, recorded before the `issues/`
back-reference convention was enforced; that back-reference was retrofitted 2026-09-07.

Redesigned 2026-09-11. The GUFI-style shadow-tree framing this file previously carried assumed the
filesystem could enforce scoping on `xdu`'s behalf, via per-class chunks whose own modes encode who
may read them. That was explored and rejected: file modes express unions, not the intersections that
real permission chains produce; a mode baked at crawl time keeps granting access after the directory
it describes is locked down, durably, because a reader can copy the chunk while it is still
readable; and object stores have no modes at all, so the approach does not survive the move to S3.
The replacement is a served index, decomposed into the seeds below.

## Outcome / vision

Two invariants, each stateable in one line, replacing one general mechanism with knobs:

- **A local index inherits the permissions of whoever crawled it.** Nothing to configure. The crawl
  already applied the filesystem's own access control, so the index cannot describe anything its
  owner could not already see. This is what ships today and it needs no permission machinery at all.
- **A shared index is only ever read through the service.** Nothing to configure. The store is
  private to `xdu-api`, which authenticates the caller, resolves them to a visibility class set, and
  answers typed queries scoped to it.

The second invariant is what the web client makes unavoidable rather than merely preferable: a
browser has no POSIX identity, so a shared index needs an authenticated query service whether or not
anything else does. Building a second, credential-handing path alongside it would mean two
authorization paths enforcing one policy, which is the accidental-exposure surface the design exists
to remove.

Together with [`toward-v1-0-release.md`](toward-v1-0-release.md), this stack is the end state the
project is aiming at before a v1.0 cut.

## Decomposition

| Seed | What it settles |
|---|---|
| [`index-visibility-classification.md`](index-visibility-classification.md) | Who may see each row, computed at crawl time from the directory chain |
| [`shared-index-api-contract.md`](shared-index-api-contract.md) | The typed, versioned, non-SQL surface both clients bind to |
| [`xdu-api-query-service.md`](xdu-api-query-service.md) | The service that holds the boundary |
| [`shared-index-authentication.md`](shared-index-authentication.md) | Who is asking and which groups they are in; `xdu-login` on the CLI |
| [`shared-index-client-routing.md`](shared-index-client-routing.md) | CLI readers reaching a served tree transparently |
| [`shared-index-deployment-guards.md`](shared-index-deployment-guards.md) | Making the store's privacy impossible to get wrong |
| [`xdu-web-client.md`](xdu-web-client.md) | The browser client the served architecture exists to make possible |

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion. The operative criteria live in the seeds above; these are
the umbrella properties that hold across them.

- **R1** — *When* a non-root user queries a shared index built as root, they *shall* see only rows
  for files they could reach on the live filesystem, and *shall not* be able to obtain the withheld
  rows by any supported operation.
- **R2** — An index built by an unprivileged user *shall* remain usable with no service, no
  authentication and no configuration, exactly as it is today.

## Notes

- Depends on: the richer index schema (owner/group/perms), delivered on `main`.
- Related: GitHub issue #3. Supersedes the shadow-tree approach recorded here before 2026-09-11.
- Found by: original roadmap; back-reference retrofitted 2026-09-07; redesigned 2026-09-11.
