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
- **Comparing two builds requires an interleaved A/B in one invocation.** Two separate
  invocations cannot resolve a change smaller than the between-invocation drift documented
  under [the noise floor](#the-noise-floor--what-this-harness-can-resolve) — on the
  reference host that drift reaches ~20%, larger than most changes worth making. Build both
  binaries (a `git worktree` for the older side) and pass `--compare-bin`: every timed rep
  then runs both against the same tree, alternating which goes first, and the document
  reports paired per-rep deltas. Do not stash between measurements, and do not compare
  medians taken from two documents.
- **Correctness is part of the measurement.** The runner queries the finished index and
  fails the run if the row count does not match the generated file count. A crawl that is
  fast because it lost files is not a faster crawl.

## Cache state, and what these numbers are not

The committed numbers are **warm-cache, local-filesystem** numbers: the tree was written
moments earlier, so its metadata sits in RAM. What they capture reliably is the *shape* of
the crawl — where it saturates, which layouts are slow, how memory scales — and that shape
is stable across captures. The absolute figures are **not** reproducible from one
invocation to the next, so a regression between two commits is established with an
interleaved A/B, never by reading two documents side by side. See
[the noise floor](#the-noise-floor--what-this-harness-can-resolve).

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

### What each committed document is, and is not

| Document | Commit recorded | Measures that commit? | What it is good for |
|---|---|---|---|
| `baseline.json` | `b8f5f9c` | yes — the tree was dirty, but only under `bench/`, which is not a build input | the reference *shape*: saturation curve, per-shape rates, memory |
| `comparison-pre-p5.json` | `c9630c0` | yes — captured on a clean tree | a single-invocation pre-P5 snapshot |
| `comparison-l1-l2.json` | `c9630c0` | **no** — `src/` was modified; it measures the unnamed stat-in-pool build | the stat-in-pool rejection, whose effect is far larger than the drift |
| `comparison-l2-only.json` | `c9630c0` | **no** — `src/` was modified; it measures the shipped P5 build | superseded by `comparison-p5-ab.json` |
| `comparison-p5-ab.json` | A `HEAD`, B `c9630c0` | yes, both sides | the only sound build-to-build comparison here |

Two of these carry an automatic note claiming the measured binary is the build of the
recorded commit. **That note is wrong for `comparison-l1-l2.json` and
`comparison-l2-only.json`**: both were captured with `src/` modified, and the runner of the
day only ever tested whole-tree dirtiness, so it asserted something it had never checked.
The runner now probes the build inputs (`src`, `Cargo.toml`, `Cargo.lock`,
`rust-toolchain.toml`) specifically and records `xdu.measures_recorded_commit`, so a
document can no longer make that claim falsely.

> **Footgun when capturing a comparison.** `run.sh baseline` defaults `--out` to
> `results/baseline.json`, so running the baseline configuration set to capture a comparison
> — which is exactly how the documents above were produced — silently overwrites the
> reference measurement. **Always pass `--out` explicitly** for anything that is not a
> deliberate re-capture of the baseline itself.

Note also that `baseline.json` (`b8f5f9c`) and `comparison-pre-p5.json` (`c9630c0`) contain
**no crawl source differences at all** — `git diff b8f5f9c c9630c0 -- src/` is empty — yet
their figures differ by 8.9–18.5%, with the baseline faster in all six configurations. That
pair is the cleanest available measurement of this harness's between-invocation drift, and
it is why the rule above exists.

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

## The noise floor — what this harness can resolve

Every number below is **host-specific** (the reference macOS/APFS laptop) and is a statement
about *the harness*, not about `xdu`. Re-measure them before trusting them elsewhere. They
exist so a claim can be checked against what the instrument can actually see.

**Within one invocation** (the `wall_s.min`/`max` band the runner records), spread across
reps is **3.2–28.4% on the multi-directory shapes** (`s3`, `s5`) and **21–33% on the
flat-wide shape** (`s2`), measured over the 12 runs in `comparison-p5-ab.json`. It is not
uniform per shape: the same `s5` configuration spread 3.2% at `-j2` and 28.4% at `-j4` in
one capture. On the flat-wide shape it also drifts monotonically within a run rather than
scattering, so a mean over few reps is biased. Earlier design notes put the
multi-directory figure at 0.4–3%; that was optimistic and this capture supersedes it.

**Between invocations of the identical binary** the spread is far larger:

- `baseline.json` vs `comparison-pre-p5.json` — identical crawl source, 45 minutes apart —
  differ by **8.9–18.5%**, the baseline faster in all six configurations. A systematic
  session bias, not white noise.
- Four back-to-back `s3 --scale 4` invocations spanned **11%**; across hours, **31%**.
- Three `s2 --scale 2` invocations gave **3.08 / 4.05 / 6.76 s** — a factor of 2.2. The
  figure `comparison-l2-only.json` records for that configuration, 2.71 s, lies *outside*
  the entire range later observed for the same binary.

**The rules that follow:**

- Two documents **cannot resolve a difference below ~20%**, and on the flat-wide shape they
  resolve nothing at all. Most changes worth making are smaller than that.
- Any "faster" claim needs `--compare-bin`: both binaries interleaved inside one invocation,
  paired per-rep deltas, order alternating by rep parity. The pairing is what cancels the
  session bias, because both sides meet the same machine in the same second.
- Read `paired_delta_pct.median` together with `a_faster_reps`/`reps`. A real effect shows a
  consistent sign; 4-of-9 with a median near zero is a null result however large the
  individual samples look.
- Give the flat-wide shape more reps than the rest (9–11 against 7): it is the only shape
  whose within-run spread is wide enough to hide a real change.
- **Peak RSS is not the low-noise signal it looks like.** Its within-run spread reaches
  ~31% (`s5 -j1`), so a single RSS figure is no safer than a single wall time. What *is*
  trustworthy is a peak-RSS difference that is **structural** — traceable to a buffer or
  allocation change and therefore consistent in sign across every rep of a shape, as the
  direct-to-Arrow pre-sizing is below. Read `peak_rss_delta_pct` per shape, never pooled:
  the same change moves memory in opposite directions on different layouts.
- A tree small enough to crawl in under ~10 ms yields no usable delta at all — `xdu` prints
  wall time to two decimals, so the quantisation swamps everything.

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
between the directory read and the stat.

**It is not a throughput win, and is not kept as one.** Measured by interleaved A/B against
the pre-P5 build — both binaries alternating within each of 9 timed reps per configuration,
`comparison-p5-ab.json` — the paired median deltas are:

| Configuration | pre-P5 | shipped | paired median | A faster in |
|---|---|---|---|---|
| `s2 -j4` flat-wide | 3.77 s | 3.77 s | **+0.00%** | 4 of 9 |
| `s3 -j4` many-partition | 1.56 s | 1.57 s | **−0.65%** | 1 of 9 |
| `s5 -j1` mixed | 7.88 s | 7.92 s | **−0.25%** | 2 of 9 |
| `s5 -j2` mixed | 4.42 s | 4.42 s | **+0.00%** | 4 of 9 |
| `s5 -j4` mixed | 2.75 s | 2.78 s | **+0.00%** | 4 of 9 |
| `s5 -j8` mixed | 2.64 s | 2.64 s | **+0.00%** | 3 of 9 |

Every median sits within ±1% and the signs are split, which is what a null result looks
like. The 6–28% "wins" the earlier before/after documents appeared to show were
between-invocation drift, not code — see the noise floor above.

**Peak RSS moves, in both directions, and only the flat-wide direction is favourable:**
flat-wide **141.0 → 130.5 MiB (−10.4 MiB, 7.4% lower)**, but many-partition
**12.9 → 16.8 MiB (+3.9 MiB, 30% higher)** and mixed at `-j8` **102.4 → 114.4 MiB
(+12.0 MiB, 11.7% higher)**. The direction is explained by the change itself: the builders
are pre-sized per chunk, which pays for itself where one partition is enormous and costs
where there are many small ones. It is capped at 8192 rows precisely to bound that cost.

**Why it is kept anyway:** one path copy instead of two, ~10 MiB less on the shape that
uses the most memory of any measured configuration, and the `symlink_metadata` TOCTOU
closed. Those are the reasons — not speed. The honest summary is that the crawl is
metadata-bound, so removing a memory copy from the encode path is invisible in wall time.

**What was tried and rejected — moving the stat into the pool.** The obvious lever is to
stat each file inside jwalk's `process_read_dir` callback, so stats run on the pool
instead of the driver. Implemented and measured (`comparison-l1-l2.json`), it took the
many-partition shape from 1.57 s to 3.47 s — **2.2× the wall time**, a 54.8% throughput
loss. Those figures are a *cross-invocation* comparison and so carry the drift described
above, but the effect is several times larger than the largest drift ever observed here
(~33%), so **the rejection stands** on this evidence. The reasoning that motivated the
lever was wrong: stats were never serialized globally — they already ran in parallel
across the driver threads. Moving them into the pool does not add stat concurrency, it
*relocates* it, from `--jobs` drivers to the pool's `--jobs` threads, leaving the drivers
idle. Total metadata concurrency therefore falls from roughly (drivers + pool) to (pool).
It appeared to gain ~50% on the single-flat-directory shape, which was read at the time as
pipelining — the driver encoding one batch while the pool stats the next. That reading is
**not established**: it rests on a cross-invocation difference on the one shape whose
spread is widest, and the interleaved A/B later measured that same shape as a dead heat
between the buffer variants. Treat the pipelining hypothesis as untested, not as a
finding. Reverted.

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
