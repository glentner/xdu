---
status: unshaped
kind: fix
appetite: small
---

# An unreadable partition prunes every row it previously held

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

Re-indexing over an existing index **destroys** a partition's rows when that partition's source
directory has become unreadable. The walk yields one hard error and zero files, but the partition is
still finalized, and finalize prunes from chunk 0 — taking the entire contiguous run of chunks the
previous index held. With `--allow-errors` the run then exits 0 and writes a completion marker.

- `src/crawl.rs:458` — `PartitionBuffer::finalize()` prunes `for chunk_id in num_chunks..`, where
  `num_chunks` is the count *this* run wrote. A partition that produced no records finalizes with
  `num_chunks == 0`, so the prune starts at `000000.parquet` and walks the whole tail. The loop is
  correct for its intended job (retiring the surplus of a prior *larger* run); it has no way to
  distinguish "this partition is legitimately smaller now" from "this partition could not be read".
- `src/bin/xdu.rs:636` — with `--allow-errors` the marker is written anyway, recording `errors=1`, so
  the destroyed partition is attested by a run that exited 0.

Reproduced against the real binaries (`alpha` holding 10 files, `beta` holding 1; `alpha` then made
mode 000 and the same outdir re-indexed):

```
initial rc=0 · total rows=11  alpha=10 · alpha chunks: 000000.parquet
# chmod 000 src/alpha; re-index --allow-errors
error: .../src/alpha: Permission denied (os error 13)
Finished alpha (0 files, 0 B, pruned 1 stale, 1 errors)
rc=0 · alpha dir now: [] · total rows=1
marker: files=1  errors=1
```

The trigger is ordinary on shared storage — a permission change, a stale mount, an unmounted
sub-filesystem, an NFS blip — and the loss is not recoverable from the index. It is sharpest with
`--allow-errors`, the flag an operator reaches for *precisely because* they expect unreadable regions
and want the rest of the index preserved; the flag's contract is "index what you can reach", and today
it can leave less than it started with. The default path destroys the same rows and merely exits
non-zero afterwards, so the exit code is a report, not a protection.

## Why it was deferred

Found by review cycle 4 of `crawl-hardening` (C4-F1) and reproduced independently by the orchestrator.

The prune scope is **pre-existing in `main`**, and this branch strictly improves the situation. A
release build of `main` in a throwaway worktree, driven through the identical scenario:

```
main initial rc=0 · rows=11  alpha=10
Finished alpha (0 files, 0 B, pruned 1 stale)
main reindex rc=0 · alpha dir now: [] · rows=1 · marker present: no
```

`main` destroys the same 10 rows, exits 0, and prints **no diagnostic at all**. HEAD added the per-path
errno line, the `pruned N stale` report, a non-zero exit by default, `errors=N` in the marker, and a
reader warning on every later query. So what is recorded here is a long-standing defect that this pass
made *visible* for the first time, not a regression it introduced.

It was not fixed in-pass because the fix is a behavior change on the crawl's error path — finalize must
learn to distinguish "walked and genuinely empty" from "walked and failed", which means threading
per-partition error state into `PartitionBuffer` and deciding what a partially-readable partition
should do. That wants its own tests and its own measured pass. The cycle-4 human gate scoped the
remediation to recording it plus the documentation corrections.

## Outcome / vision

A partition that could not be read is left exactly as the previous index had it, and the run says so.
Indexing never removes data as a side effect of failing to read it — under `--allow-errors` least of
all, where the operator has explicitly asked to keep what is reachable.

## Sketch of the acceptance criteria

- **R1** — WHEN a partition's walk reports one or more hard errors, the crawler SHALL NOT prune that
  partition's pre-existing chunks, and the previously-indexed rows SHALL still resolve after the run.
- **R2** — WHEN a partition is walked cleanly and legitimately holds fewer files than before, stale
  chunk pruning SHALL continue to work exactly as it does today.
- **R3** — WHERE a partition is skipped or left unreconciled because it could not be read, the run
  SHALL report it on stderr distinctly from a partition it pruned normally.
- **R4** — IF a run tolerates errors via `--allow-errors`, THEN the completion marker SHALL record
  enough for a reader to tell that some partition is carrying rows from an earlier run.

## Notes

- Design options: track a per-partition error count on `PartitionBuffer` and make `finalize()` skip the
  prune (and the rename of an empty chunk set) when it is non-zero — smallest change, keeps stale rows
  rather than deleting good ones; or refuse to finalize an errored partition at all, leaving the prior
  chunks untouched on disk. The first is probably right: preserving stale data beats destroying live
  data, and the marker already carries the "this index is not clean" signal.
- Note the interaction with [`marker-scoped-run-attestation.md`](marker-scoped-run-attestation.md): a
  later `--partition`-scoped run rewrites the marker from its own stats and can retire the `errors=N`
  that was the only remaining trace of the loss.
- This is the third member of the `finalize`-prune-scope family, and the only one that removes rows the
  index already held. The other two leave *extra* rows:
  [`orphan-partition-survives-reindex.md`](orphan-partition-survives-reindex.md) (a partition whose
  source is gone is never reconciled) and the marker-scoping issue above. A fix for any of them is
  really a decision about what a run's finalize speaks for, so they are worth shaping together.
- Related: [`spec/crawl-hardening/REVIEW.md`](../spec/crawl-hardening/REVIEW.md) C4-F1 for the
  reviewer's evidence and the `main` differential drive.
- Found by: `crawl-hardening` review cycle 4 (C4-F1), recorded in P14.
