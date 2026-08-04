#!/usr/bin/env python3
"""Generate a synthetic directory tree for benchmarking the xdu crawl.

The crawl is metadata-bound and never reads file content: per file it costs one
amortized `getdents` plus a `stat`. So the benchmark trees are built from *sparse*
files (`ftruncate` to a size without writing bytes) — they cost ~0 disk blocks but
carry the full inode and `stat` cost that xdu actually pays. That is what makes a
100M-file tree feasible to generate on a machine that could never store one.

Trees are deterministic for a given seed and parameter set, so two runs of the same
scenario measure the same work. A summary is printed to stdout as one JSON object
for `run.sh` to fold into its result row.

Usage:
    gen_tree.py --root DIR --scenario s5 [--scale N] [--seed N]
    gen_tree.py --root DIR --parts 8 --dirs-per-part 4 --files-per-dir 100 --depth 3

See scenarios.md for the scenario table and what each shape stresses.
"""

import argparse
import json
import math
import os
import random
import shutil
import sys

KB = 1024
MB = 1024 * 1024

# Named scenarios. Each is a list of groups so a single scenario can mix shapes
# (s4 needs one giant partition beside many tiny ones). `files_per_dir` is the knob
# `--scale` multiplies; every other dimension is fixed so the *shape* is preserved
# as a scenario is scaled from a laptop to an HPC filesystem.
#
# Base sizes are tuned so a full scenario generates and crawls in seconds-to-minutes
# on a development machine. Real validation runs the same shapes at --scale 64-256.
SCENARIOS = {
    "smoke": {
        "desc": "tiny tree for the harness self-check (not a measurement)",
        "groups": [
            dict(prefix="part", parts=2, dirs_per_part=2, files_per_dir=25, depth=2,
                 sizes=[4 * KB]),
        ],
        "loose": 4,
    },
    "s1": {
        "desc": "deep-narrow: recursion depth and many small files in few deep dirs",
        "groups": [
            dict(prefix="part", parts=4, dirs_per_part=64, files_per_dir=64, depth=8,
                 sizes=[1 * KB, 4 * KB, 16 * KB]),
        ],
        "loose": 0,
    },
    "s2": {
        "desc": "flat-wide: one huge flat directory; jwalk parallelizes per directory, "
                "so this partition cannot be split across threads",
        "groups": [
            dict(prefix="flat", parts=1, dirs_per_part=1, files_per_dir=200_000, depth=0,
                 sizes=[4 * KB]),
        ],
        "loose": 0,
    },
    "s3": {
        "desc": "many-parts: 1000 top-level partitions stressing the work queue and "
                "driver balance",
        "groups": [
            dict(prefix="part", parts=1000, dirs_per_part=1, files_per_dir=100, depth=1,
                 sizes=[4 * KB]),
        ],
        "loose": 0,
    },
    "s4": {
        "desc": "skewed: one giant partition beside 500 tiny ones — work-stealing and "
                "starvation behaviour",
        "groups": [
            dict(prefix="giant", parts=1, dirs_per_part=8, files_per_dir=25_000, depth=2,
                 sizes=[4 * KB]),
            dict(prefix="tiny", parts=500, dirs_per_part=1, files_per_dir=100, depth=2,
                 sizes=[4 * KB]),
        ],
        "loose": 0,
    },
    "s5": {
        "desc": "mixed: representative fan-out and size distribution (the default "
                "scenario for baseline and comparison runs)",
        "groups": [
            dict(prefix="part", parts=32, dirs_per_part=32, files_per_dir=100, depth=3,
                 sizes=[512, 4 * KB, 64 * KB, 1 * MB, 4 * MB]),
        ],
        "loose": 16,
    },
}


def make_sparse(path, size):
    """Create a file of `size` apparent bytes without allocating blocks."""
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        os.ftruncate(fd, size)
    finally:
        os.close(fd)


def leaf_path(base, index, depth, branch):
    """Path of the `index`-th leaf directory, `depth` levels below `base`.

    The index is written out in base-`branch` across the levels, so the directories
    form a real tree of the requested depth with distinct leaves — rather than a
    single chain that would collapse every leaf onto one path.
    """
    if depth == 0:
        return base
    parts = []
    remaining = index
    for level in range(depth):
        parts.append(f"d{level}_{remaining % branch}")
        remaining //= branch
    return os.path.join(base, *parts)


def build_group(root, group, scale, rng):
    """Create one group of partitions; returns (file_count, apparent_bytes)."""
    files_per_dir = group["files_per_dir"] * scale
    depth = group["depth"]
    dirs_per_part = group["dirs_per_part"]
    sizes = group["sizes"]

    # Branching factor that fits `dirs_per_part` distinct leaves into `depth` levels.
    branch = 2 if depth == 0 else max(2, math.ceil(dirs_per_part ** (1.0 / depth)))

    count = 0
    total_bytes = 0
    for part_index in range(group["parts"]):
        part_root = os.path.join(root, f"{group['prefix']}{part_index:05d}")
        for dir_index in range(dirs_per_part):
            directory = leaf_path(part_root, dir_index, depth, branch)
            os.makedirs(directory, exist_ok=True)
            for file_index in range(files_per_dir):
                size = rng.choice(sizes)
                make_sparse(os.path.join(directory, f"f{file_index:06d}.dat"), size)
                count += 1
                total_bytes += size
    return count, total_bytes


def build(root, scenario, scale, seed, force):
    spec = SCENARIOS[scenario]
    rng = random.Random(seed)

    if os.path.exists(root):
        if not force:
            sys.exit(f"gen_tree.py: {root} already exists (pass --force to replace it)")
        shutil.rmtree(root)
    os.makedirs(root)

    count = 0
    total_bytes = 0
    for group in spec["groups"]:
        group_count, group_bytes = build_group(root, group, scale, rng)
        count += group_count
        total_bytes += group_bytes

    # Loose files directly under the root exercise the depth-1 __root__ partition.
    for loose_index in range(spec["loose"]):
        size = 4 * KB
        make_sparse(os.path.join(root, f"loose{loose_index:04d}.dat"), size)
        count += 1
        total_bytes += size

    return {
        "scenario": scenario,
        "description": spec["desc"],
        "root": os.path.abspath(root),
        "files": count,
        "apparent_bytes": total_bytes,
        "scale": scale,
        "seed": seed,
        "loose_files": spec["loose"],
        "groups": [
            {k: v for k, v in group.items() if k != "sizes"} | {"sizes": group["sizes"]}
            for group in spec["groups"]
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", required=True, help="directory to build the tree in")
    parser.add_argument(
        "--scenario",
        default="s5",
        choices=sorted(SCENARIOS),
        help="named scenario shape (default: s5)",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=1,
        help="multiply files-per-directory; the shape is unchanged (default: 1)",
    )
    parser.add_argument("--seed", type=int, default=0, help="RNG seed for file sizes")
    parser.add_argument(
        "--force", action="store_true", help="replace --root if it already exists"
    )
    parser.add_argument(
        "--list", action="store_true", help="print the scenario table and exit"
    )
    args = parser.parse_args()

    if args.list:
        for name in sorted(SCENARIOS):
            print(f"{name}\t{SCENARIOS[name]['desc']}")
        return

    if args.scale < 1:
        sys.exit("gen_tree.py: --scale must be >= 1")

    summary = build(args.root, args.scenario, args.scale, args.seed, args.force)
    json.dump(summary, sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
