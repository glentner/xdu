---
status: unshaped
kind: feature
appetite: big
---

# Index diffing for incremental selection

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

"Changed since the last backup" has no primitive. `--newer-than` is a wall-clock atime filter —
the wrong clock for a modification question, since atime moves on read (modulo `relatime`), and one
whose answer shifts with clock skew. No A-to-B comparison of two indices exists either. Without one,
incremental backup is either a time guess or a full re-archive of the tree.

## Why it was deferred

Not deferred from a factory pass. Split from `issues/xdu-mv-and-xdu-tar.md` with the copy/move and
archive seeds, and sequenced third so the comparator columns it needs (mtime/ctime) are guaranteed
present by the richer schema landing earlier in the schedule. The theme ordering is what makes the
reliable form shippable instead of approximate.

## Outcome / vision

Two indices of the same root, taken at T0 and T1, diff into added/changed/removed/unchanged on the
path key — and the added-plus-changed set feeds archive selection, so `tar` stops guessing by clock
and starts reading the difference.

## Sketch of the acceptance criteria

- **R1** — WHEN two indices of the same root are given, the diff SHALL report
  added/changed/removed/unchanged keyed on path, comparing size plus mtime (ctime where it sharpens
  the verdict; the comparator set is final at shaping).
- **R2** — The added-plus-changed set SHALL be consumable as an archive selection: an output format
  `tar` reads, or a `tar --diff-from` seam. Shaping decides; one seam only.
- **R3** — The diff SHALL stream million-row inputs without full materialization: DuckDB anti-joins
  over the two Parquet globs, not in-memory sets.
- **R4** — Binary shape — a new `xdu-diff` or an `xdu-find --diff` mode — is decided at shaping;
  either way invariant §10 applies: `cli.rs` defines it, the man page and completions follow.
- **R5** — WHEN the two indices cover different roots, the tool SHALL refuse or report the mismatch
  explicitly: path keys from different trees compare as all-added/all-removed, which must never
  present as a change set.

## Notes

- Third of the bulk-operations theme; its consumer is `issues/xdu-tar-slice-archive.md`.
- Depends on `issues/richer-index-schema.md` (mtime/ctime comparator columns), satisfied by theme
  sequencing; transitively on `issues/index-schema-versioning.md`.
- Related: `issues/streaming-index-updates-lustre-changelog.md` answers the same staleness from the
  ingestion side (merge-on-read deltas); the two meet at "what changed".
- Found by: backup-vision scoping 2026-09-07; split from `xdu-mv-and-xdu-tar.md`.
