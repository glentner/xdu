---
status: unshaped
kind: fix
appetite: small
---

# A removed top-level directory leaves a phantom partition the marker attests as clean

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

Re-indexing a tree reconciles each partition the run **walked** and nothing else. When a top-level
directory is removed from the source, its partition directory — chunks and rows — survives in the index
indefinitely, the run still exits 0, and it writes a completion marker. Every reader then reports a
clean bill of health for an index carrying rows for files that no longer exist.

- `src/crawl.rs:440` — `PartitionBuffer::finalize()` prunes stale chunks only *within* the partition it
  just wrote (`for chunk_id in num_chunks..` under `outdir.join(&self.partition)`). A partition the run
  never enqueued is never opened, so nothing prunes it.
- `src/bin/xdu.rs:636` — the marker is written from the run's own stats, so `files=` counts the rows this
  run wrote while the index still returns those plus every phantom row. Nothing cross-checks the two.

Reproduced against the real binaries (three partitions of three files; `p3` removed from the source,
then re-indexed into the same outdir):

```
initial rows=9  p3=3
reindex rc=0
rows after reindex=9   (ground truth 6)
phantom p3 still resolves: 3
marker: files=6  errors=0     <- the marker's own count contradicts the index's row count
reader warning: (silent)
```

The stakes are highest for `xdu-rm`, whose queries can now match rows describing paths that are gone
(they are skipped at unlink, or worse, match a path since recreated as something else), and for the
size accounting `xdu` exists to provide: a purged project keeps counting against its tree forever.

## Why it was deferred

Found by review cycle 3 of `crawl-hardening` (C3-F2) and confirmed independently in P13.

The stale-partition survival is **pre-existing in `main`** — `git show main:src/bin/xdu.rs` has the
identical per-partition prune scope (prune from `num_chunks..` inside the partition's own directory) —
and it was not in the hazard list that GOAL's R2 enumerated. What that pass *changed* is that an index
now carries a **completeness attestation**, and that attestation does not detect this case: the marker
says `files=6` while the index answers 9. The gap being recorded here is therefore the detection, not a
regression.

Fixing it properly means reconciling the index root against the source tree — deciding whether an
unwalked partition is stale (its source directory is gone) or deliberately out of scope (a
`--partition`-scoped run, which must *not* delete the partitions it was told to skip). That is
whole-index bookkeeping and CLI semantics, not the local prune `finalize` does, and it overlaps
[`marker-scoped-run-attestation.md`](marker-scoped-run-attestation.md), which has to answer the same
"what does this run speak for?" question. The cycle-3 human gate scoped that build to C3-F1 plus these
records.

## Outcome / vision

A completed full-tree re-index leaves no rows for a top-level directory that no longer exists — or, if
reconciliation is opt-in, an index carrying phantom partitions is not attested as clean, and readers say
so. A partition-scoped run never removes partitions outside its scope.

## Sketch of the acceptance criteria

- **R1** — WHEN a full-tree run completes and a partition in the index has no corresponding top-level
  source directory, the index SHALL NOT continue to return that partition's rows (or SHALL be reported
  as unreconciled rather than complete).
- **R2** — WHEN a `--partition`-scoped run completes, partitions outside its scope SHALL be left
  untouched.
- **R3** — WHERE a partition is removed by reconciliation, the removal SHALL be reported on stderr
  (count, and each partition name) so a destructive side effect of indexing is never silent.
- **R4** — IF the index root holds an entry that is not a partition directory (the `COMPLETION_MARKER`
  dotfile), THEN reconciliation SHALL NOT treat it as a stale partition.

## Notes

- Design options: prune-unwalked-partitions inside a full-tree run (cheapest, but makes `xdu` delete
  index data as a side effect — wants a flag and a loud report); or cross-check the marker's `files=`
  against the reader's row count and warn on mismatch (detection only, no deletion, and it composes with
  the existing soft-warning contract).
- The reconciliation step must respect §3's reserved names: `<index>/` legitimately holds the
  `.xdu-complete` dotfile and the synthetic `__root__` partition, neither of which maps to a source
  directory of that name.
- Related: [`marker-scoped-run-attestation.md`](marker-scoped-run-attestation.md) (the same
  what-does-this-run-cover question, from the marker's side);
  [`spec/crawl-hardening/REVIEW.md`](../spec/crawl-hardening/REVIEW.md) C3-F2 for the reviewer's
  original evidence.
- Found by: `crawl-hardening` review cycle 3 (C3-F2), recorded in P13.
