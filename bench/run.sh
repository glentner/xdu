#!/bin/sh
# SPDX-FileCopyrightText: 2026 Geoffrey Lentner
# SPDX-License-Identifier: MIT
#
# bench/run.sh — measurement runner for the xdu index-build crawl.
#
# Generates a synthetic tree (bench/gen_tree.py), crawls it with the release `xdu`
# some number of times, and emits one JSON document describing the machine, the tree,
# and the per-configuration timings. Trees and indexes live in a throwaway directory
# that is removed on exit — this never touches a real filesystem or a real index.
#
# Wall time comes from xdu's own "Completed N files (...) in T.TTs" line, so the
# harness needs no external timing tool; /usr/bin/time supplies peak RSS when present.
#
# Run from the repository root. See scenarios.md for the scenario table and the
# methodology this implements.
set -eu

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
BENCH_DIR="$REPO_ROOT/bench"
RESULTS_DIR="$BENCH_DIR/results"

usage() {
    cat <<'EOF'
Usage (from the repository root):

  sh bench/run.sh smoke
      Build a tiny tree in a throwaway directory, crawl it, and assert the index
      holds exactly the files that were generated. This proves the harness still
      executes end to end; it is a self-check, not a measurement.

  sh bench/run.sh SCENARIO [options]        (SCENARIO is s1..s5)
      -j, --jobs "N [N ...]"   xdu --jobs values to sweep       (default: 4)
      -B, --buffsize N         xdu --buffsize                   (default: 100000)
          --scale N            gen_tree.py --scale              (default: 1)
          --reps N             timed repetitions per -j value   (default: 5)
          --warmup N           discarded warm-up runs per -j    (default: 1)
          --disk-usage         measure the default size mode rather than
                               --apparent-size (sparse trees then report ~0 bytes)
          --syscalls           also profile one rep under strace (Linux only, slow)
          --out FILE           write the JSON document here     (default: stdout)
          --label TEXT         free-form label recorded in the document

  sh bench/run.sh baseline [--out FILE]
      Run the committed baseline set and write bench/results/baseline.json.

Requirements: python3, cargo (to build the release binaries once), and optionally
/usr/bin/time for peak RSS and strace for syscall counts.
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

MODE=""
JOBS_LIST="4"
BUFFSIZE=100000
SCALE=1
REPS=5
WARMUP=1
SIZE_MODE_FLAG="--apparent-size"
SIZE_MODE_NAME="apparent-size"
SYSCALLS=0
OUT=""
LABEL=""

if [ $# -eq 0 ]; then
    usage
    exit 2
fi

MODE=$1
shift

case "$MODE" in
    -h|--help|help) usage; exit 0 ;;
esac

while [ $# -gt 0 ]; do
    case "$1" in
        -j|--jobs)      JOBS_LIST=$2; shift 2 ;;
        -B|--buffsize)  BUFFSIZE=$2; shift 2 ;;
        --scale)        SCALE=$2; shift 2 ;;
        --reps)         REPS=$2; shift 2 ;;
        --warmup)       WARMUP=$2; shift 2 ;;
        --disk-usage)   SIZE_MODE_FLAG=""; SIZE_MODE_NAME="disk-usage"; shift ;;
        --syscalls)     SYSCALLS=1; shift ;;
        --out)          OUT=$2; shift 2 ;;
        --label)        LABEL=$2; shift 2 ;;
        -h|--help)      usage; exit 0 ;;
        *) echo "run.sh: unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------------------
# Environment discovery
# ---------------------------------------------------------------------------

OS_NAME=$(uname -s)

case "$OS_NAME" in
    Darwin)
        CORES=$(sysctl -n hw.ncpu)
        RAM_BYTES=$(sysctl -n hw.memsize)
        CPU_MODEL=$(sysctl -n machdep.cpu.brand_string)
        TIME_FLAG="-l"
        ;;
    *)
        CORES=$(nproc 2>/dev/null || echo 0)
        RAM_BYTES=$(awk '/MemTotal/ {print $2 * 1024; exit}' /proc/meminfo 2>/dev/null || echo 0)
        CPU_MODEL=$(awk -F': ' '/model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null || echo unknown)
        TIME_FLAG="-v"
        ;;
esac

# Filesystem type holding a directory — the dominant factor in any crawl number.
fs_type() {
    _dir=$1
    case "$OS_NAME" in
        Darwin)
            _dev=$(df -P "$_dir" | awk 'NR==2 {print $1}')
            mount | awk -v d="$_dev" '$1 == d' | sed -n 's/.*(\([^,)]*\).*/\1/p'
            ;;
        *)
            df -PT "$_dir" 2>/dev/null | awk 'NR==2 {print $2}'
            ;;
    esac
}

if [ -x /usr/bin/time ]; then
    HAVE_TIME=1
else
    HAVE_TIME=0
fi

if command -v strace >/dev/null 2>&1; then
    HAVE_STRACE=1
else
    HAVE_STRACE=0
fi

GIT_COMMIT=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)
if [ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null)" ]; then
    GIT_DIRTY=true
else
    GIT_DIRTY=false
fi

# ---------------------------------------------------------------------------
# Build + workspace
# ---------------------------------------------------------------------------

XDU="$REPO_ROOT/target/release/xdu"
XDU_FIND="$REPO_ROOT/target/release/xdu-find"

if [ ! -x "$XDU" ] || [ ! -x "$XDU_FIND" ]; then
    echo "run.sh: building release binaries (one-time)…" >&2
    ( cd "$REPO_ROOT" && cargo build --release --bin xdu --bin xdu-find >&2 )
fi

# Prefer the binary's own --version; fall back to the single source in Cargo.toml so
# the harness records a version regardless of which flags the CLI exposes.
XDU_VERSION=$("$XDU" --version 2>/dev/null | awk 'NR==1 {print $NF}')
if [ -z "$XDU_VERSION" ]; then
    XDU_VERSION=$(awk -F'"' '/^version = / {print $2; exit}' "$REPO_ROOT/Cargo.toml")
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/xdu-bench.XXXXXX")
trap 'rm -rf "$WORK"' EXIT INT TERM

ROWS="$WORK/rows.tsv"
: > "$ROWS"

# ---------------------------------------------------------------------------
# smoke — the harness self-check
# ---------------------------------------------------------------------------

if [ "$MODE" = "smoke" ]; then
    tree="$WORK/tree"
    index="$WORK/index"

    summary=$(python3 "$BENCH_DIR/gen_tree.py" --root "$tree" --scenario smoke)
    generated=$(printf '%s' "$summary" | python3 -c 'import json,sys; print(json.load(sys.stdin)["files"])')

    "$XDU" "$tree" -o "$index" -j 2 --apparent-size >/dev/null 2>&1
    indexed=$("$XDU_FIND" -i "$index" --count)

    # Exit 0 is not the assertion — the index must hold exactly what was generated.
    if [ "$generated" != "$indexed" ]; then
        echo "run.sh: SMOKE FAILED — generated $generated files, index holds $indexed" >&2
        exit 1
    fi
    if [ ! -f "$index/.xdu-complete" ]; then
        echo "run.sh: SMOKE FAILED — the run left no completion marker" >&2
        exit 1
    fi

    echo "smoke ok: generated $generated files, indexed $indexed, marker present"
    exit 0
fi

# ---------------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------------

# Run one scenario across a list of --jobs values, appending a TSV row per timed rep.
# Args: scenario scale jobs_list
run_scenario() {
    _scenario=$1
    _scale=$2
    _jobs_list=$3

    _tree="$WORK/tree-$_scenario"
    _index="$WORK/index-$_scenario"

    echo "run.sh: generating $_scenario (scale $_scale)…" >&2
    _summary=$(python3 "$BENCH_DIR/gen_tree.py" --root "$_tree" --scenario "$_scenario" \
                       --scale "$_scale" --force)
    printf '%s\n' "$_summary" > "$WORK/gen-$_scenario.json"
    _generated=$(printf '%s' "$_summary" | python3 -c 'import json,sys; print(json.load(sys.stdin)["files"])')
    _bytes=$(printf '%s' "$_summary" | python3 -c 'import json,sys; print(json.load(sys.stdin)["apparent_bytes"])')

    for _jobs in $_jobs_list; do
        echo "run.sh: $_scenario -j $_jobs — $WARMUP warm-up + $REPS timed rep(s)…" >&2

        _total=$(( WARMUP + REPS ))
        _rep=0
        while [ "$_rep" -lt "$_total" ]; do
            _rep=$(( _rep + 1 ))

            # A fresh index every rep: writing over an existing index changes the work
            # (finalize prunes stale chunks), which would make reps incomparable.
            rm -rf "$_index"

            _log="$WORK/run.log"
            if [ "$HAVE_TIME" -eq 1 ]; then
                /usr/bin/time $TIME_FLAG "$XDU" "$_tree" -o "$_index" \
                    -j "$_jobs" -B "$BUFFSIZE" $SIZE_MODE_FLAG > /dev/null 2> "$_log" || {
                        echo "run.sh: xdu failed (see below)" >&2; cat "$_log" >&2; exit 1; }
            else
                "$XDU" "$_tree" -o "$_index" \
                    -j "$_jobs" -B "$BUFFSIZE" $SIZE_MODE_FLAG > /dev/null 2> "$_log" || {
                        echo "run.sh: xdu failed (see below)" >&2; cat "$_log" >&2; exit 1; }
            fi

            # Discard warm-ups: the first pass populates the OS dentry/inode cache.
            if [ "$_rep" -le "$WARMUP" ]; then
                continue
            fi

            _wall=$(awk '/^Completed /{ for (i=1;i<=NF;i++) if ($i ~ /^[0-9.]+s$/) { sub(/s$/,"",$i); print $i } }' "$_log" | tail -1)
            _rss=$(awk '
                /maximum resident set size/ { print $1; found=1 }
                /Maximum resident set size/ { gsub(/[^0-9]/,"",$NF); print $NF * 1024; found=1 }
                END { if (!found) print "" }' "$_log" | head -1)

            if [ -z "$_wall" ]; then
                echo "run.sh: could not parse a wall time from xdu output" >&2
                cat "$_log" >&2
                exit 1
            fi

            printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                "$_scenario" "$_scale" "$_generated" "$_bytes" "$_jobs" \
                "$BUFFSIZE" "$SIZE_MODE_NAME" "$_rep" "$_wall" "$_rss" >> "$ROWS"
        done

        # The measured crawl must also be a *correct* crawl: a benchmark over an index
        # that silently lost files measures nothing worth knowing.
        _indexed=$("$XDU_FIND" -i "$_index" --count)
        printf '%s\t%s\t%s\n' "$_scenario" "$_jobs" "$_indexed" >> "$WORK/indexed.tsv"
        if [ "$_indexed" != "$_generated" ]; then
            echo "run.sh: FAILED — $_scenario -j $_jobs indexed $_indexed of $_generated files" >&2
            exit 1
        fi
    done

    # Optional syscall profile: one extra untimed rep. This is how the per-file stat
    # count is proven to have moved or shrunk, independent of wall-clock noise.
    if [ "$SYSCALLS" -eq 1 ] && [ "$HAVE_STRACE" -eq 1 ]; then
        echo "run.sh: profiling $_scenario under strace (slow)…" >&2
        rm -rf "$_index"
        strace -f -c -e trace=%file,%stat -o "$WORK/strace-$_scenario.txt" \
            "$XDU" "$_tree" -o "$_index" -j 4 -B "$BUFFSIZE" $SIZE_MODE_FLAG \
            > /dev/null 2>&1 || true
    fi

    rm -rf "$_tree" "$_index"
}

: > "$WORK/indexed.tsv"

case "$MODE" in
    baseline)
        # The committed reference set. s5 sweeps --jobs to show the scaling curve;
        # s2 and s3 pin the two shapes that stress the model's edges (a single flat
        # directory that cannot be split, and 1000 partitions through the work queue).
        [ -n "$LABEL" ] || LABEL="committed baseline"
        [ -n "$OUT" ] || OUT="$RESULTS_DIR/baseline.json"
        run_scenario s5 8 "1 2 4 8"
        run_scenario s2 2 "4"
        run_scenario s3 4 "4"
        ;;
    s1|s2|s3|s4|s5)
        run_scenario "$MODE" "$SCALE" "$JOBS_LIST"
        ;;
    *)
        echo "run.sh: unknown mode: $MODE" >&2
        usage >&2
        exit 2
        ;;
esac

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

TREE_FS=$(fs_type "$WORK")
REPO_FS=$(fs_type "$REPO_ROOT")

export ROWS INDEXED_TSV="$WORK/indexed.tsv" WORK LABEL MODE
export XDU_VERSION GIT_COMMIT GIT_DIRTY
export OS_NAME CORES RAM_BYTES CPU_MODEL TREE_FS REPO_FS
export HAVE_TIME HAVE_STRACE SYSCALLS REPS WARMUP
export KERNEL="$(uname -r)" ARCH="$(uname -m)"

REPORT=$(python3 - <<'PYTHON'
import json
import os
import re
import statistics
import subprocess
from datetime import datetime, timezone


def num(value):
    return None if value in ("", None) else float(value)


rows = []
with open(os.environ["ROWS"]) as handle:
    for line in handle:
        fields = line.rstrip("\n").split("\t")
        rows.append({
            "scenario": fields[0],
            "scale": int(fields[1]),
            "generated_files": int(fields[2]),
            "apparent_bytes": int(fields[3]),
            "jobs": int(fields[4]),
            "buffsize": int(fields[5]),
            "size_mode": fields[6],
            "rep": int(fields[7]),
            "wall_s": float(fields[8]),
            "rss_bytes": num(fields[9]),
        })

indexed = {}
with open(os.environ["INDEXED_TSV"]) as handle:
    for line in handle:
        scenario, jobs, count = line.rstrip("\n").split("\t")
        indexed[(scenario, int(jobs))] = int(count)


def spread(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    return {
        "median": round(statistics.median(values), 4),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
        "samples": [round(v, 4) for v in values],
    }


def syscall_counts(scenario):
    """Parse `strace -c` totals for the calls the crawl is made of."""
    path = os.path.join(os.environ["WORK"], f"strace-{scenario}.txt")
    if not os.path.exists(path):
        return None
    wanted = ("newfstatat", "statx", "stat", "lstat", "getdents64", "getdents", "openat")
    counts = {}
    with open(path) as handle:
        for line in handle:
            fields = line.split()
            if len(fields) >= 4 and fields[-1] in wanted:
                try:
                    counts[fields[-1]] = int(fields[-2])
                except ValueError:
                    continue
    return counts or None


runs = []
for key in sorted({(r["scenario"], r["jobs"]) for r in rows}):
    scenario, jobs = key
    group = [r for r in rows if (r["scenario"], r["jobs"]) == key]
    first = group[0]
    walls = [r["wall_s"] for r in group]
    rates = [first["generated_files"] / w for w in walls if w > 0]

    runs.append({
        "scenario": scenario,
        "scale": first["scale"],
        "jobs": jobs,
        "buffsize": first["buffsize"],
        "size_mode": first["size_mode"],
        "generated_files": first["generated_files"],
        "indexed_files": indexed.get(key),
        "apparent_bytes": first["apparent_bytes"],
        "reps": len(group),
        "warmup_discarded": int(os.environ["WARMUP"]),
        "wall_s": spread(walls),
        "files_per_sec": spread(rates),
        "peak_rss_bytes": spread([r["rss_bytes"] for r in group]),
        "syscalls": syscall_counts(scenario),
    })

notes = []
if os.environ["GIT_DIRTY"] == "true":
    notes.append(
        "The working tree was dirty at capture. The measured binary is the build at "
        "the recorded commit; uncommitted changes outside src/ do not affect it."
    )
if os.environ["HAVE_TIME"] != "1":
    notes.append("/usr/bin/time is absent: peak RSS was not measured.")
if os.environ["SYSCALLS"] == "1" and os.environ["HAVE_STRACE"] != "1":
    notes.append("strace is absent on this platform: syscall counts were not collected.")
elif os.environ["SYSCALLS"] != "1":
    notes.append("Syscall counts not collected (pass --syscalls on Linux to collect them).")
notes.append(
    "Warm-cache numbers on a local filesystem. A networked metadata server "
    "(Lustre MDS, GPFS metanode) is the dominant cost at real scale — see "
    "HPC-PROTOCOL.md."
)

document = {
    "schema": 1,
    "label": os.environ["LABEL"],
    "mode": os.environ["MODE"],
    "captured_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "xdu": {
        "version": os.environ["XDU_VERSION"],
        "git_commit": os.environ["GIT_COMMIT"],
        "git_dirty": os.environ["GIT_DIRTY"] == "true",
    },
    "host": {
        "os": os.environ["OS_NAME"],
        "kernel": os.environ["KERNEL"],
        "arch": os.environ["ARCH"],
        "cpu": os.environ["CPU_MODEL"].strip(),
        "cores": int(os.environ["CORES"]),
        "ram_bytes": int(os.environ["RAM_BYTES"]),
        "tree_filesystem": os.environ["TREE_FS"].strip(),
        "repo_filesystem": os.environ["REPO_FS"].strip(),
    },
    "runs": runs,
    "notes": notes,
}

print(json.dumps(document, indent=2))
PYTHON
)

if [ -n "$OUT" ]; then
    mkdir -p "$(dirname "$OUT")"
    printf '%s\n' "$REPORT" > "$OUT"
    echo "run.sh: wrote $OUT" >&2
else
    printf '%s\n' "$REPORT"
fi
