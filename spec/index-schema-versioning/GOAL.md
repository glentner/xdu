# GOAL — On-disk index schema versioning

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).
> Keep this at the right altitude: solved and bounded, but not over-specified — leave design
> freedom for the plan. Edit requirements here; do **not** silently drift them during build.

- **slug:** index-schema-versioning
- **kind:** feature
- **appetite:** small

## Problem

The index Parquet schema is three fixed, non-null columns (`src/lib.rs::get_schema`) with no
version marker anywhere on disk, so any future schema change would silently break every existing
index and every reader. The schema-stability invariant (`AGENTS.md` §1) currently has no escape
hatch: there is no way for a reader to tell a v1 index from anything newer, and no defined
behavior for the mismatch beyond misreading rows. This blocks the richer schema
(`issues/richer-index-schema.md`) and everything downstream of it, all of which need a version
to key their compatibility on.

## Outcome / vision

Every completed index carries its own format version on disk, and every reader checks it before
trusting the rows. A reader that cannot establish a version it understands refuses with a
diagnostic that says what it found, what it expected, and what to do — never a silent misread.
The current three-column layout becomes version 1 by definition. There is an accepted flag day:
indexes written before versioning carry no marker and are refused until re-indexed.

## Acceptance criteria (the contract)

- **R1** — Every index run completed by `xdu` SHALL record the index format version on disk.
- **R2** — WHEN `xdu-find`, `xdu-view`, or `xdu-rm` opens an index whose recorded version it does
  not understand, it SHALL refuse: exit non-zero with a diagnostic naming the found version and
  the version it supports, and present no index rows (for `xdu-rm`, delete nothing).
- **R3** — WHEN a reader cannot establish any index version (no marker, a marker with no version,
  or an unparseable one), it SHALL refuse the same way, with a diagnostic directing a re-index —
  never fall back to reading the rows blind.
- **R4** — An index freshly written by the same build SHALL be accepted by all three readers with
  no version diagnostic. The existing soft warning for tolerated run errors on a versioned index
  stays as-is.

## Non-goals (no-gos)

- No schema change: the index stays exactly `path`/`size`/`atime` — this is the enabler, not the
  enrichment (`issues/richer-index-schema.md`).
- No migration or upgrade path for old indexes; the remedy is a re-crawl.
- No per-partition or per-chunk versions; the marker already attests run-level completeness, and
  the version is run-level with it.
- No backporting: binaries predating this change cannot detect versions they were never taught.
- The `--partition`-scoped marker limitation (`issues/marker-scoped-run-attestation.md`) stays
  open; this goal neither fixes nor widens it.

## Clarifications

- **Q:** On a version mismatch, refuse with a diagnostic or migrate/compat-read? — **A:** Refuse
  with a diagnostic; no compatibility shims in this goal (resolved 2026-09-07).
- **Q:** How do the new readers treat pre-versioning indexes with no marker? — **A:** Refuse with
  a re-index diagnostic; the flag day is accepted, no grandfathering (resolved 2026-09-07).

## Related materials

- Seed: `issues/index-schema-versioning.md`
- Downstream consumer: `issues/richer-index-schema.md`
- Adjacent limitation: `issues/marker-scoped-run-attestation.md`
- Source anchors: `src/lib.rs::get_schema` (the schema contract), `COMPLETION_MARKER` /
  `index_completion_warning` (the existing run-attestation the version rides with)
- Invariant: `AGENTS.md` § *Load-bearing invariants* (1 — Parquet schema stability)
