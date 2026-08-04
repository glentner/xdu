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

Numbers live in that file rather than in this document, so nothing here rots. What the
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
