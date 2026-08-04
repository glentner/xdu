# HPC benchmark protocol

A procedure for measuring `xdu`'s index-build crawl on a real large-scale filesystem —
Lustre, GPFS/Spectrum Scale, or ZFS — and reporting the result back to the project.

## 1. Purpose and scope

`xdu` exists for trees of hundreds of millions to billions of files. The project's own
[synthetic harness](scenarios.md) runs on a developer machine against a freshly written
tree on a local disk: reproducible, but warm-cached and metadata-local. It cannot tell us
what happens when every `stat` is a round trip to a shared metadata server, which is the
regime that actually decides whether an index finishes overnight.

This protocol closes that gap. It asks a site operator to run a defined measurement on
real storage and return a small table. It measures **only** the crawl (`xdu`), not the
query tools, and it changes nothing on the filesystem it measures — the crawl is
read-only apart from the index it writes, which should be placed on a *different*
filesystem from the tree being walked.

**Before you run this at scale, read §4 on load.** A billion-file `stat` storm is visible
to every other user of a shared metadata server.

## 2. Inputs — describe the tree

Prefer a real production or scratch tree; it is the point of the exercise. Report:

- total files and total bytes (an existing `du`/`find` figure or a previous `xdu` index is
  fine — do not run an extra full walk just to produce this)
- number of top-level subdirectories (these become xdu's partitions) and whether the
  distribution is even or skewed
- maximum depth, and typical fan-out (files per directory) — an order of magnitude is
  enough, plus the size of the largest single directory
- file size histogram, at least coarsely
- presence of hardlinks, sparse files, snapshots, or a heavy symlink population
- whether the tree is live (files being created/deleted during the crawl)

If no suitable real tree is available, generate one with the project's generator, which
builds sparse files and so costs almost no space:

```sh
python3 bench/gen_tree.py --root /scratch/$USER/xdu-bench --scenario s5 --scale 128
```

`--scale` multiplies files-per-directory while preserving the shape; see
[`scenarios.md`](scenarios.md) for the table. Note in your report that the tree was
synthetic and freshly written — that flatters metadata locality relative to an aged tree.

## 3. Environment — record the storage

The storage configuration explains the numbers, so please record it. Common:

- filesystem type and version; mount options; whether tree and index are on the same
  filesystem (they should not be)
- client node: core count, RAM, kernel, interconnect (IB / Ethernet, speed)
- `xdu` version and git commit; the `-j` and `-B` values used
- whether other significant load was on the storage during the run

Filesystem-specific:

| filesystem | record |
|------------|--------|
| **Lustre** | stripe count and size (`lfs getstripe`), number of OSTs, number of MDSs/MDTs and whether DNE is in use, client version (`lctl get_param version`) |
| **GPFS** | block size, number of NSDs, metadata replication, metanode/token server configuration, whether metadata is on separate SSD NSDs |
| **ZFS** | `recordsize`, ARC size and hit rate, pool/vdev layout, `special` vdev for metadata if present, SSD vs HDD, compression setting |

## 4. Procedure

1. **Build.** `cargo build --release` on the client node (or use a release tarball). The
   crawl binary is `xdu`.
2. **Coordinate.** On a shared filesystem, tell your storage administrators before a
   large run. Metadata operations at this rate are visible cluster-wide; a full-scale
   crawl of a production Lustre filesystem can degrade interactive response for other
   users. Prefer a maintenance window or a quiet period, and start with a subtree.
3. **Choose the cache state and say which you used.** This is the single largest factor.
   - *Cold* is the honest number for "how long does an index take". Cluster-wide
     `drop_caches` is usually impossible, so approximate it with a tree that has never
     been read on this client (freshly written, or a rebooted/newly allocated compute
     node). Note that the **server-side** cache — the Lustre MDS or the ZFS ARC — matters
     more than the client's, and you generally cannot clear it.
   - *Warm* is reproducible and good for comparing two `xdu` builds: run back to back.
4. **Sweep `-j`.** Run at `-j` = 1, 2, 4, 8, 16, 32 and beyond the node's core count if
   throughput is still climbing — the interesting result is *where it stops climbing*.
   Hold `-B` fixed (default 100000) throughout.
5. **Repeat.** At least 5 timed repetitions per configuration after one discarded
   warm-up. Remove the index between repetitions (`rm -rf`), since re-indexing in place
   does different work. Report median with min/max — a single number hides the variance
   that shared storage always has.
6. **Verify the crawl was correct.** Query the finished index and confirm the row count
   matches what you expect for the tree:
   `xdu-find -i INDEX --count`. A run that exits non-zero, or reports errors in its
   summary, indexed an incomplete tree — say so rather than reporting its timing. `xdu`
   exits non-zero when a region was unreadable, and writes no completion marker.

## 5. Metrics

Per configuration:

- **wall time** — `xdu` prints `Completed N files (BYTES) in T.TTs` on stderr, so no
  external timer is required
- **files/sec** — the headline number: files ÷ wall time
- **bytes/sec** — secondary; the crawl never reads content, so this tracks the tree's
  size mix rather than the storage's bandwidth
- **peak RSS and %CPU** — `/usr/bin/time -v` on Linux
- **syscall counts** — `strace -f -c -e trace=%file,%stat ./xdu ...` if permitted. This
  costs 10–50× in time, so run it once at a modest scale, for *counts* not timing. It is
  the direct way to see how many `stat` calls each file costs.
- **filesystem-side metadata op rate**, where you can read it — this is what distinguishes
  "the client is slow" from "the metadata server is saturated":
  - Lustre: `lctl get_param mdt.*.md_stats`, `lctl get_param mdc.*.stats`
  - GPFS: `mmpmon` counters
  - ZFS: `arcstat`, `zpool iostat -v`

The project's runner emits all of the client-side metrics as one JSON document:

```sh
sh bench/run.sh s5 --scale 128 --jobs "1 2 4 8 16 32" --out /tmp/mysite.json
```

Attaching that JSON is the most useful form of report; the table in §7 is the minimum.

## 6. What the results should look like

- **files/sec rises with `-j`, then flattens or regresses.** The crawl is stat-bound, so
  throughput climbs while the metadata path has capacity and stops when it does not. The
  saturation point is the result worth reporting — not the peak number.
- **Lustre's single-MDS ceiling is the classic wall.** Where one MDS serves the namespace,
  its stat rate caps the crawl no matter how many client cores you add, and adding `-j`
  past that point only increases queueing. Sites with DNE across several MDTs should scale
  further, and confirming that would be a genuinely useful data point.
- **GPFS** typically walls on token/metanode contention rather than a single server.
- **ZFS** is dominated by warm vs cold ARC. Local NVMe with a `special` metadata vdev can
  scale with client cores far past anything networked.
- **There is no read-bound regime.** `xdu` never opens file contents, so runs stay
  metadata-bound end to end; if throughput correlates with file *sizes*, something else is
  going on and that is worth reporting.
- **One enormous flat directory will not parallelize.** jwalk's unit of parallelism is the
  directory, so a single directory of tens of millions of files is walked by one thread
  regardless of `-j`. If your tree has such a directory, report it separately — it
  dominates the run and is a known structural limit rather than a tuning problem.
- **Expect a gap versus the synthetic harness.** A freshly written sparse tree has ideal
  dirent locality; an aged, fragmented production tree does not. A large gap is the
  expected finding, and quantifying it is much of this protocol's value.

## 7. Reporting template

Fill in and send with the environment details from §3 (or attach the runner's JSON).

```
site / filesystem:        e.g. Purdue Anvil / Lustre 2.15, 1 MDS, 12 OSTs
client node:              e.g. 128 cores, 256 GB RAM, Linux 5.14, HDR IB
tree:                     files, bytes, top-level dirs, max depth, largest single dir
tree provenance:          real production tree | synthetic (gen_tree.py, scenario, scale)
cache state:              cold (never read on this client) | warm (back to back)
xdu version / commit:
index location:           filesystem the index was written to
buffsize (-B):

  -j | wall_s (median) | wall_s min/max | files/sec | peak RSS | %CPU | FS md-ops/sec
-----+-----------------+----------------+-----------+----------+------+---------------
   1 |                 |                |           |          |      |
   2 |                 |                |           |          |      |
   4 |                 |                |           |          |      |
   8 |                 |                |           |          |      |
  16 |                 |                |           |          |      |
  32 |                 |                |           |          |      |

row count verified (xdu-find --count):     yes / no — value
run exited 0 and wrote a completion marker: yes / no
notes: concurrent load, anomalies, where throughput stopped scaling
```

Please report results — including disappointing ones — as a GitHub issue. A measurement
showing where `xdu` stops scaling on real storage is more useful to this project than one
showing where it does.
