# 03 — Benchmark design (synthetic harness + HPC protocol)

Research brief for R4 (reproducible synthetic baseline) and R9 (HPC protocol). Read-only research;
no source edited. Ground-truth facts this rests on:

- **The crawl is metadata-bound, never content-bound.** `xdu.rs` does `readdir` (jwalk) then
  `fs::metadata(entry.path())` per file (`xdu.rs:365`) — it *never reads file bytes*. jwalk also
  stats to resolve `entry.file_type` when the dirent `d_type` is `DT_UNKNOWN`. So per file the hot
  path is ~1 `getdents` amortized + 1–2 `stat`. This is *the* thing to measure, and the reason a
  **sparse-file** generator is legitimate: a `truncate -s`'d file costs ~0 disk blocks (verified:
  `blocks=0 size=4096`) but incurs the full `stat` cost. The double-stat is the prime R5 target;
  `strace -c` stat counts before/after prove any de-dup.
- xdu prints `Completed N files (BYTES) in T.TTs` to **stderr** (`xdu.rs:602`) — a zero-dependency
  wall-clock source usable even where `hyperfine` is absent. `--jobs`/`-j` (env `XDU_JOBS`, dflt 4)
  and `-B/--buffsize` (dflt 100000) are the tuning knobs. Default size mode is disk-usage, so sparse
  trees report ~0 bytes unless you pass `--apparent-size`.
- Dev box (darwin) tooling verified present: `dtruss`, BSD `/usr/bin/time` (`-l` → "maximum resident
  set size" in **bytes**), `truncate`, `python3`, `du`, `find`, 14 logical CPUs. **Absent:**
  `hyperfine`, `strace`, `fallocate`, `gtime`. Linux HPC has `strace`, GNU `/usr/bin/time -v` (RSS in
  **kbytes**), `fallocate`, and (as root) `/proc/sys/vm/drop_caches`.

## 1. Synthetic tree generator

Python (fast, portable) writing sparse files with `os.truncate`. Parameters give one scenario table
that scales from ~100k files (macOS iteration) to 10M+ (Linux validation) via a single `--scale`.

```python
#!/usr/bin/env python3
# bench/gen_tree.py — sparse synthetic trees; disk cost ~0, full stat cost.
import argparse, os, random
def mkfile(p, size): fd=os.open(p, os.O_CREAT|os.O_WRONLY, 0o644); os.ftruncate(fd, size); os.close(fd)
def build(root, parts, dirs_per_part, files_per_dir, depth, sizes, seed=0):
    rng=random.Random(seed); os.makedirs(root, exist_ok=True); n=0
    for pi in range(parts):
        base=os.path.join(root, f"part{pi:05d}")
        for di in range(dirs_per_part):
            d=base
            for lvl in range(depth): d=os.path.join(d, f"d{lvl}_{di%4}")
            os.makedirs(d, exist_ok=True)
            for fi in range(files_per_dir):
                mkfile(os.path.join(d, f"f{fi:06d}.dat"), rng.choice(sizes)); n+=1
    print(f"{n} files across {parts} partitions -> {root}")
# CLI: --root --parts --dirs-per-part --files-per-dir --depth --scale (multiplies files-per-dir)
```

Named scenarios (base params → multiply `files_per_dir` by `--scale`; ~1 on macOS, ~64–256 on HPC):

| id | shape / what it stresses | parts | dirs/part | files/dir | depth | sizes |
|----|--------------------------|-------|-----------|-----------|-------|-------|
| **S1 deep-narrow** | recursion depth, many small files, few deep dirs | 4 | 64 | 64 | 8 | 1K–16K |
| **S2 flat-wide** | ONE huge flat dir → single-driver-per-partition serialization | 1 | 1 | 1_000_000 | 0 | 4K |
| **S3 many-parts** | 1000 top-level partitions → work-queue/driver balance | 1000 | 1 | 100 | 1 | 4K |
| **S4 skewed** | 1 giant + 500 tiny → work-stealing / starvation | 501* | — | — | 2 | 4K |
| **S5 mixed** | representative fan-out + size mix | 32 | 32 | 100 | 3 | 512B–4M |

\*S4 built with two `build()` calls into one root (one `part` at `files_per_dir=2_000_000`, then 500
parts at `files_per_dir=100`). Generation is I/O-cheap (sparse); a 1M-file S2 builds in seconds. Also
create a `__root__` stressor: N loose files directly under the tree root (exercises the depth-1 walk).

## 2. Baseline methodology

**Build once:** `cargo build --release` (bundled DuckDB is slow but unrelated to the crawl bin).

Per run, capture: wall time (xdu's own `Completed … in T.TTs`, or `hyperfine` if present),
**files/sec** = files/wall, bytes/sec, **peak RSS** (`/usr/bin/time -l` macOS bytes / `-v` Linux
kbytes), **%CPU** (from `time`), and **syscall counts** (Linux `strace -c -f`; macOS `dtruss -c`,
flaky under SIP on Apple Silicon — treat syscall counting as a Linux measurement).

```bash
# Wall + RSS + CPU, non-TTY so progress is quiet and stderr is parseable:
/usr/bin/time -l ./target/release/xdu "$TREE" -o "$IDX" -j "$J" -B "$B" 2>>run.log   # macOS
/usr/bin/time -v ./target/release/xdu "$TREE" -o "$IDX" -j "$J" -B "$B" 2>>run.log   # Linux
# Syscall profile (Linux; ~10-50x slowdown — for COUNTS not timing). Confirms double-stat fix:
strace -f -c -e trace=%file,%stat ./target/release/xdu "$TREE" -o "$IDX" -j "$J"
#   watch newfstatat/statx (per-file stat) and getdents64 counts.
# hyperfine if available (auto warmup + stats):
hyperfine -w1 -r5 "./target/release/xdu $TREE -o $IDX -j $J -B $B"
```

Rules for comparability: **discard 1 warm-up**, then **≥5 reps**, report median + min/max.
`rm -rf "$IDX"` between reps (writing over an existing index changes work). **Sweep `-j` = 1,2,4,8,16**
(dev box has 14 CPUs) to locate saturation; hold `-B` fixed, then sweep `-B` once at best `-j`.
**Cache state:** *warm* = back-to-back reps (default, reproducible on both OSes); *cold* on Linux =
`sync; echo 3 > /proc/sys/vm/drop_caches` (root) before each rep; macOS has no reliable equivalent
(`sync && sudo purge` is approximate) — so treat macOS numbers as warm-cache iteration only, and
gather cold numbers on Linux. Record everything (git commit, CPU, RAM, FS, kernel, toolchain, tree
params, `-j`/`-B`) as one JSON/CSV row per run so PLAN `verify:` and REVIEW can diff. Commit a
`bench/results/baseline.json` (the reference number) and git-ignore raw `bench/results/*.log`.

## 3. Comparison baselines

- **vs today's tools on the same tree, same cache state:** `du -s "$TREE"` and
  `find "$TREE" -type f | wc -l` (both metadata-walk the tree; du also stats every file). Not
  apples-to-apples — they don't build an index — but it is the honest "what an operator does now,"
  which is xdu's raison d'être. Time all three with the same `/usr/bin/time`/`hyperfine` wrapper.
- **vs xdu's own prior commit (regression / measured-win gate):** don't stash — use a worktree so
  both binaries coexist: `git worktree add ../xdu-base <baseline-sha>; (cd ../xdu-base && cargo build
  --release)`. Run the runner against both binaries on the **same generated tree** and diff the JSON
  rows. This is exactly how R5 ("no regression / measured win") is proven.

## 4. HPC protocol (R9) — doc outline

`bench/HPC-PROTOCOL.md`, filesystem-agnostic where possible, for a community operator on
Lustre/GPFS/ZFS. Sections:

1. **Purpose/scope** — validate crawl throughput at a scale we cannot reproduce in-loop; report back.
2. **Inputs to report (tree characteristics):** total files, total bytes, max depth, fan-out
   distribution, # top-level partitions, size histogram, presence of hardlinks/sparse. Prefer a real
   scratch tree; the operator may also run `bench/gen_tree.py` at large `--scale`.
3. **Environment to record:** FS type + version; **Lustre**: stripe count/size, # OSTs, # MDS/MDTs,
   `lctl` client version; **GPFS**: block size, # NSDs, metanode config; **ZFS**: `recordsize`, ARC
   size, pool/vdev layout, dataset on SSD vs HDD; node core count + RAM; kernel; mount opts;
   interconnect (IB/Eth); `xdu --version` + git commit; `-j`/`-B` used.
4. **Procedure:** build; select/generate tree; **cache handling** — cluster-wide `drop_caches` is
   usually impossible, so run *cold* via a freshly-written/never-read tree or a rebooted compute node,
   and note the **MDS/ARC** cache is the dominant warm-vs-cold factor; ≥5 reps; `-j` sweep across the
   node's core count (and beyond, to find the metadata-server ceiling).
5. **Metrics:** wall, files/sec, bytes/sec, peak RSS, %CPU, syscalls (if `strace` permitted), plus
   **FS-side metadata op rate** where readable: Lustre `lctl get_param mdt.*.md_stats` /
   `mdc.*.stats`, GPFS `mmpmon`, ZFS `arcstat`/`zpool iostat`.
6. **Expected shape of results:** files/sec rises with `-j` until it **saturates the metadata server**
   (stat-bound), then flattens or regresses. **Lustre single-MDS is the classic ceiling** — one shared
   MDS caps stat rate regardless of client cores, and a billion-file stat storm loads it for *all*
   users (coordinate first). GPFS: token/metanode contention becomes the wall. ZFS: warm ARC vs cold
   dominates; local NVMe scales with client cores far higher than networked Lustre metadata. Because
   xdu never reads content, runs stay metadata-bound end-to-end (no read-bound regime). Note where the
   synthetic harness diverges from real FS: page-cache/dirent locality on a freshly-written sparse tree
   flatters the numbers vs a cold, aged, fragmented production tree.
7. **Reporting template:** a CSV/table the operator fills (env + per-`-j` metrics) and returns.

## 5. Where artifacts live & staying un-stale

New top-level `bench/` (sits beside `.agents/`, `spec/`): `gen_tree.py`, `run.sh` (the measurement
runner emitting JSON rows), `scenarios.md` (the S1–S5 table), `HPC-PROTOCOL.md` (R9), `results/`
(git-ignored raw logs + committed `baseline.json`). Keep runnable: `run.sh` references the binary by
`target/release/` path and takes `-j`/`-B` as args (no hardcoded perf numbers to rot); add a tiny
**smoke** invocation of the smallest scenario (temp_index.sh-style, throwaway dir) that a CI job or
the factory can run to assert the harness *executes* (non-rot check, not a perf gate); reference
`bench/` from AGENTS.md "Testing" so future agents find it. The committed `baseline.json` records the
git commit it was taken at, so a stale baseline is self-evident.
