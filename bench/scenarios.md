# Benchmark scenarios

The synthetic benchmark for the `xdu` index-build crawl: what it measures, the tree
shapes it measures against, and the rules that make two runs comparable. The companion
document [`HPC-PROTOCOL.md`](HPC-PROTOCOL.md) covers validation on a real large-scale
filesystem, which this harness deliberately cannot simulate.

## What is being measured

The crawl is **metadata-bound and never content-bound** — `xdu` reads directory entries
and `stat`s each file, but never opens a file's contents. Per file the cost is roughly
one amortized `getdents` plus one `stat`. Everything downstream (building records,
Parquet encoding, writing chunks) is small beside that on any real tree.

Two consequences shape the harness:

- **Sparse files are legitimate.** A file created with `ftruncate` to 4 KiB occupies
  ~0 disk blocks but carries a full inode and the full `stat` cost. That is exactly the
  cost `xdu` pays, so `gen_tree.py` builds trees that are cheap to store and honest to
  measure. It is what makes a million-file tree feasible on a laptop.
- **File *size* is nearly irrelevant to throughput; file *count* and directory shape are
  everything.** The scenarios therefore vary fan-out, depth and partition count, not
  bytes. Sizes vary only so the byte column is not uniform.

Because sparse trees report ~0 disk usage, the runner passes `--apparent-size` by
default so the byte totals mean something. The `stat` path is identical either way;
pass `--disk-usage` to measure the default size mode.

## Scenario table

Each scenario fixes a *shape*. `--scale` multiplies files-per-directory and leaves every
other dimension alone, so a scenario keeps its character as it grows: `--scale 1` is a
development-machine size, `--scale 64`–`256` is the range for a real filesystem.

| id | shape | what it stresses | partitions | dirs/part | files/dir | depth | files at `--scale 1` |
|----|-------|------------------|-----------|-----------|-----------|-------|----------------------|
| `smoke` | tiny | harness self-check only, never a measurement | 2 | 2 | 25 | 2 | 104 |
| `s1` | deep-narrow | recursion depth; many small files in few deep directories | 4 | 64 | 64 | 8 | 16,384 |
| `s2` | flat-wide | one huge flat directory — jwalk's unit of parallelism is the directory, so this partition cannot be split across threads | 1 | 1 | 200,000 | 0 | 200,000 |
| `s3` | many-parts | the work queue and driver balance across 1000 partitions | 1000 | 1 | 100 | 1 | 100,000 |
| `s4` | skewed | work-stealing and starvation: one giant partition beside 500 tiny ones | 1 + 500 | 8 / 1 | 25,000 / 100 | 2 | 250,000 |
| `s5` | mixed | representative fan-out and size mix; the default for baseline and comparison runs | 32 | 32 | 100 | 3 | 102,416 |

`s5` and `smoke` also place loose files directly under the tree root, which exercises the
depth-1 `__root__` partition.

## Running it

```sh
sh bench/run.sh smoke                          # harness self-check (fast)
sh bench/run.sh s5 --scale 8 --jobs "1 2 4 8"  # a --jobs sweep on the mixed shape
sh bench/run.sh s2 --scale 2 --reps 5          # one configuration
sh bench/run.sh baseline                       # the committed reference set
sh bench/run.sh s5 --syscalls                  # add strace syscall counts (Linux)
```

Every run generates its tree and index in a throwaway directory that is removed on exit,
so a benchmark never touches a real filesystem or a real index. Output is one JSON
document on stdout (or `--out FILE`) recording the machine, the git commit, the tree
parameters, and per-configuration wall time, files/sec and peak RSS.

`smoke` is the non-rot check: it asserts the index holds **exactly** the files that were
generated and that the run left a completion marker. It fails loudly if the harness or
the crawler drifts — an exit code alone would prove nothing.

## Rules for comparability

- **Discard the warm-up.** The first pass populates the OS dentry/inode cache; the runner
  discards `--warmup` runs (default 1) before timing `--reps` (default 5) and reports
  median with min/max.
- **A fresh index every repetition.** Writing over an existing index changes the work
  (`finalize` prunes stale chunks), so the runner removes it between reps.
- **Sweep `-j`, hold `-B`.** Locate saturation with `--jobs "1 2 4 8 16"` at a fixed
  buffer size; sweep `-B` separately at the best `-j` if the buffer is in question.
- **Same tree, same cache state, one variable at a time.** To compare two builds of `xdu`,
  use a `git worktree` so both binaries exist at once and run the same scenario against
  both — do not stash between measurements.
- **Correctness is part of the measurement.** The runner queries the finished index and
  fails the run if the row count does not match the generated file count. A crawl that is
  fast because it lost files is not a faster crawl.

## Cache state, and what these numbers are not

The committed numbers are **warm-cache, local-filesystem** numbers: the tree was written
moments earlier, so its metadata sits in RAM. They are reproducible and good for
detecting a regression between two commits, which is what they exist for.

They are **not** a prediction of production throughput. A freshly-written sparse tree has
ideal dirent locality and no fragmentation; a real aged tree on a networked filesystem is
dominated by round-trips to a metadata server. Cold numbers on Linux need
`sync; echo 3 > /proc/sys/vm/drop_caches` (root) before each repetition; macOS has no
reliable equivalent, so treat any macOS figure as warm-cache iteration only. Real-scale
validation is [`HPC-PROTOCOL.md`](HPC-PROTOCOL.md)'s job.

## The committed baseline

[`results/baseline.json`](results/baseline.json) is the reference measurement, captured
with `sh bench/run.sh baseline`: an `s5` sweep over `--jobs 1 2 4 8` plus one `s2` and one
`s3` configuration. It records the git commit it was taken at, so a stale baseline is
self-evident — regenerate it when the crawl changes and the comparison is no longer
meaningful.

`comparison-pre-p5.json` and `comparison-l1-l2.json` are the before/after pair for the
stat-relocation and direct-to-Arrow work, measured back to back on one machine.

Numbers live in those files rather than in this document, so nothing here rots. What the
baseline shows in *shape*, and what a later change should be read against:

- Throughput on the mixed shape rises steeply from `-j 1` through `-j 4` and then
  **flattens or slightly regresses** — the metadata path saturates well below the core
  count, which is why the `-j` default is conservative.
- The **flat-wide** shape is by far the slowest per file, at a fraction of the mixed
  shape's rate, and carries the highest peak RSS. jwalk parallelizes across directories,
  so a single enormous directory is walked by one thread no matter what `-j` says. This
  is the structural ceiling of the approach, not a tuning failure.
- **Many small partitions** are handled at close to the mixed shape's rate with very low
  memory: the work queue distributes them well.

## The performance ceiling, and what was tried

The crawl is **metadata-bound**: it issues one `getdents` per directory and one `stat` per
file and never reads content. Everything else — building the columns, Snappy encoding,
writing chunks — is small beside that. So the ceiling is set by how many metadata
operations the filesystem will serve concurrently, and by how many the crawl can have in
flight.

`xdu` keeps stats in flight two ways: the shared rayon pool reads directories
concurrently, and each of the `--jobs` driver threads stats the files of the partition it
is walking. Both matter, and the second is easy to overlook.

**What shipped:** the per-partition buffer now appends into Arrow builders as records
arrive rather than collecting row structs and copying every path a second time at flush.
A valid UTF-8 path is borrowed straight from the walker into the column, so its bytes are
copied once. The per-file stat also moved from `fs::metadata` to the walker entry's
`symlink_metadata`, which closes the window where a file could be swapped for a symlink
between the directory read and the stat. Measured against the previous commit on the same
machine, back to back: real wins on the flat-wide, many-partition and mid-`--jobs` mixed
shapes, no measured regression anywhere, and lower peak RSS in most configurations
(`comparison-pre-p5.json` vs `comparison-l2-only.json`).

**What was tried and rejected — moving the stat into the pool.** The obvious lever is to
stat each file inside jwalk's `process_read_dir` callback, so stats run on the pool
instead of the driver. Implemented and measured (`comparison-l1-l2.json`), it was **50%
slower on the many-partition shape** and roughly neutral elsewhere. The reasoning that
motivated it was wrong: stats were never serialized globally — they already ran in
parallel across the driver threads. Moving them into the pool does not add stat
concurrency, it *relocates* it, from `--jobs` drivers to the pool's `--jobs` threads,
leaving the drivers idle. Total metadata concurrency therefore falls from roughly
(drivers + pool) to (pool). It looked like a win only on the single-flat-directory shape,
and there the gain was really pipelining — the driver encoding one batch while the pool
stats the next — which is a different lever entirely. Reverted.

**Deferred, with reasons:**

- **Decoupling pool width from driver count** (today `--jobs` sets both). This is the
  change that would make pool-side stat worthwhile, and on a high-latency metadata server
  oversubscribing the pool should hide RPC latency. It needs new tuning surface and a
  changed relationship between `--jobs` and thread counts, so it wants its own measured
  pass — ideally against the numbers `HPC-PROTOCOL.md` collects, since a warm local
  filesystem cannot show the latency-hiding effect it targets.
- **Pipelining the Parquet write off the driver** (a bounded channel to a writer thread).
  This is the honest lever for the flat-wide shape, where one driver owns an entire huge
  directory and stalls on encode and I/O. It must not weaken atomic finalization —
  chunk ids stay sequential and the rename/prune may only happen after the writer drains —
  so it deserves its own phase rather than a tail-end change here.
- **Parquet encoding settings** (disabling dictionary encoding for the near-unique `path`
  column, delta encoding for the integer columns). Expected to be minor for write
  throughput, which measurement here says is not the bottleneck.
- **Fanning stats out within a single directory** (`par_iter_mut` inside the walk
  callback). This addresses the one shape that genuinely cannot parallelize — a single
  directory of tens of millions of files — but it inherits the same flaw as the rejected
  lever: it competes for the same pool threads. It only makes sense together with the
  pool/driver decoupling above.

**The structural limit that remains:** jwalk's unit of parallelism is the directory. One
enormous flat directory is read by a single thread no matter what `--jobs` says, and no
amount of tuning inside the current walk changes that — it is a property of the approach,
not of this configuration. Trees with many directories parallelize well; a single
billion-entry directory does not.
