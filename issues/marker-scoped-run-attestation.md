---
status: unshaped
kind: fix
appetite: small
---

# A partition-scoped run rewrites the whole-index completion marker

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

The completion marker attests to a *whole index*, but its contents come from **one run's** stats, and
nothing scopes either half to the partitions that run actually touched.

- `src/bin/xdu.rs:92` — `clear_completion_marker(outdir)?` runs on **every** crawl, including
  `xdu -p onepartition`. The clear is not filtered by `partition_filter`.
- `src/bin/xdu.rs:636` — `write_completion_marker(&outdir, &completion_marker_contents(&stats, …))`
  writes `files=`, `errors=`, `vanished=`, `lossy_paths=` from that run alone.

So this sequence silently loses a warning that was correct:

1. `xdu /data -o /index --allow-errors` hits unreadable regions under `bob/` and records `errors=7`.
   Readers correctly warn that results may be incomplete.
2. Later, `xdu /data -o /index -p alice` re-indexes one clean partition. It clears the marker and
   writes a fresh one with `errors=0`.
3. The warning is gone. `bob/`'s skipped regions are still missing from the index, and every reader —
   `xdu-find`, `xdu-view`, and `xdu-rm`, whose entire risk model is the files an index does not know
   about — now reports a clean bill of health.

The scoped run is not wrong about itself; it is wrong about the index. It overwrites an attestation
covering partitions it never looked at.

## Why it was deferred

Recorded during `crawl-hardening` P9, which introduced the reader-side warning that makes this
reachable. Fixing it is **marker-format or CLI-semantics work** — per-partition attestation, or
refusing to write a whole-index marker from a scoped run — and both were explicit non-goals of that
GOAL. P9 left a `// Known limitation:` comment at the write site (`xdu.rs:628`) rather than smuggling
a format change into a reader-warning phase.

Worth stating plainly: P9's warning is **still a strict improvement**. Before it, an `--allow-errors`
index said nothing at all about what it had skipped. This issue is about the warning's durability, not
its correctness.

## Outcome / vision

A completion marker cannot claim more than the run that wrote it actually covered. Either the marker
becomes per-partition, or a scoped run declines to speak for the whole index — and in both cases a
previously-recorded tolerated-error count survives a later partition-scoped re-index.

## Sketch of the acceptance criteria

- **R1** — WHEN a partition-scoped run (`-p`) completes, the index SHALL NOT report a lower
  tolerated-error count than the regions outside that scope still warrant.
- **R2** — WHEN a full-index run completes cleanly after a previous `--allow-errors` run, the readers
  SHALL fall silent (a genuine clean re-index still clears the warning).
- **R3** — IF the marker format changes, THEN a marker written by an older `xdu` SHALL still be read
  without a reader error (readers degrade to silence on an unrecognized body today, per
  `lib::completion_marker_errors`).

## Notes

- Design options, roughly in increasing cost: per-partition marker files under each partition dir; a
  `partitions=` key listing the scope a marker covers; or making a scoped run merge into the existing
  marker rather than replace it. The merge option keeps one file but needs a read-modify-write, which
  reintroduces the FIFO/oversize guard concerns `lib::index_completion_warning` already handles.
- Interacts with the on-disk format: see `ROADMAP.md`'s "On-disk index schema versioning".
- Found by: `crawl-hardening` P9.
