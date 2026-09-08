# GOAL — Richer index schema: owner, group, permissions, mtime, ctime

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).
> Keep this at the right altitude: solved and bounded, but not over-specified — leave design
> freedom for the plan. Edit requirements here; do **not** silently drift them during build.

- **slug:** richer-index-schema
- **kind:** feature
- **appetite:** big

## Problem

The index records three non-null columns — `path`, `size`, `atime` (`src/lib.rs::get_schema`,
format version 1). That cannot answer two questions administrators ask on shared trees such as
`/projects/{lab1,lab2,…}`.

"Which *user within* a project is biggest?" needs ownership per file, and reasoning about exposure
and cleanup needs permission bits; today both fall back to a live `find`. "Changed since the last
backup" is a modification-time question the index cannot ask: atime moves on read (modulo
`relatime`), and size misses same-size rewrites, so neither the existing time filter nor a later
index diff can reliably name the changed set.

This is the breaking, cross-cutting schema change that `AGENTS.md` §1 reserved versioning for.
Versioning is on `main` (`INDEX_FORMAT_VERSION = 1`). The enrichment is not.

## Outcome / vision

Every file row carries numeric uid and gid, permission bits, mtime, and ctime — the last two as
Unix epoch seconds, the same convention as atime — behind a new format version. Operators rebuild
once. `xdu-find` can then filter by owner, group, permission bits, and mtime, so per-user
accounting, exposure questions, and "modified since" selection run against the index instead of a
live walk. Owner and group filters take names and resolve them at query time; names are not extra
columns.

The TUI still explores path/size/atime. Bulk `--safe` re-stat, per-owner ranking, MIME, access
scoping, and index diffing consume these columns later.

## Acceptance criteria (the contract)

- **R1** — WHEN the `xdu` crawler indexes a tree, each file row SHALL include numeric uid, numeric
  gid, permission bits, mtime, and ctime. mtime and ctime SHALL be Unix epoch seconds, the same
  convention as atime.
- **R2** — WHEN a crawl completes successfully, the completion marker SHALL name a format version
  distinct from 1 that identifies this schema.
- **R3** — IF `xdu-find`, `xdu-view`, or `xdu-rm` is pointed at an index whose marker version it
  does not understand — including a format-1 index after this bump — THEN it SHALL refuse: exit
  non-zero, present no rows, and delete nothing.
- **R4** — WHEN `xdu-find` is given an owner filter naming a user, it SHALL return only files whose
  recorded uid is that user's uid, resolved at query time. WHEN given a group filter naming a
  group, it SHALL return only files whose recorded gid is that group's gid, resolved the same way.
- **R5** — IF the owner or group name cannot be resolved, THEN `xdu-find` SHALL exit non-zero and
  print no rows, rather than silently matching nothing.
- **R6** — WHEN `xdu-find` is given a permission-bits filter, it SHALL return only files whose
  recorded mode satisfies that filter, so an administrator can select exposure classes without a
  live `find`. Flag syntax and exact-versus-mask matching are left to the plan.
- **R7** — WHEN `xdu-find` is given an mtime filter, it SHALL return only files whose recorded
  mtime satisfies the bound — the mtime analogue of today's atime `--older-than` / `--newer-than`.
- **R8** — WHEN `xdu-find` emits csv or json, each row SHALL include uid, gid, permission bits,
  mtime, and ctime. The default path format SHALL remain one path per line.
- **R9** — WHEN `xdu-find`, `xdu-view`, or `xdu-rm` opens a freshly written index of the new
  format, it SHALL serve the existing path/size/atime surface (list, count, TUI, dry-run delete)
  without a version diagnostic.

## Non-goals (no-gos)

- MIME / content-type column; content-type filtering stays a later seed.
- Per-owner or per-group ranking (GROUP BY totals / "which user is biggest" as a summary table).
  `--owner alice --count` on a known user is in; a leaderboard is not.
- Displaying owner, mode, mtime, or ctime as `xdu-view` columns.
- Expanding `xdu-rm --safe` to re-stat the new fields.
- Query flags on ctime (the column is recorded; filters on it wait).
- Storing owner or group names as index columns.
- Reading format-1 indexes after the bump; the remedy is a re-crawl, as versioning already
  established.
- Access-scoped / permission-hiding queries (`issues/access-scoped-queries.md`).
- Index diffing and incremental select (`issues/index-diff-incremental-select.md`).
- Birth/creation time (`crtime` / `btime`); ctime is Unix inode-change time.
- Changing atime's resolution, or nanosecond mtime/ctime.
- S3 or Windows sources.

## Clarifications

- **Q:** What ships in this cycle, given five columns plus several query surfaces? — **A:** Columns
  plus `xdu-find` filters (owner/group, permission bits, mtime). No per-owner ranking, no TUI
  columns, no `xdu-rm --safe` expansion (resolved 2026-09-08).
- **Q:** MIME / content-type column this cycle? — **A:** No. Content sniffing is a different
  problem; content-type filtering stays a later seed (resolved 2026-09-08).
- **Q:** mtime/ctime representation? — **A:** Unix epoch seconds, matching atime. Same-second
  rewrites stay a known limitation (resolved 2026-09-08).
- **Q:** Owner/group identity in the index vs the CLI? — **A:** Store numeric uid/gid. Query flags
  take user/group names and resolve them at query time (find(1) style). Names are not extra columns
  (resolved 2026-09-08).

## Related materials

- Seed: [`issues/richer-index-schema.md`](../../issues/richer-index-schema.md)
- GitHub: [#2](https://github.com/glentner/xdu/issues/2),
  [#3](https://github.com/glentner/xdu/issues/3)
- Enabler: [`spec/index-schema-versioning/`](../index-schema-versioning/) (format version 1,
  refuse-unknown)
- Downstream: [`issues/index-diff-incremental-select.md`](../../issues/index-diff-incremental-select.md),
  [`issues/access-scoped-queries.md`](../../issues/access-scoped-queries.md),
  and [`issues/bulk-copy-move-xdu-cp-mv.md`](../../issues/bulk-copy-move-xdu-cp-mv.md)
- Source anchors: `src/lib.rs::get_schema` (three-column contract), `INDEX_FORMAT_VERSION`
- Invariant: `AGENTS.md` § *Load-bearing invariants* (1 — Parquet schema stability)
