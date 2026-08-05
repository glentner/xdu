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
      Two-stage harness self-check, not a measurement. Stage 1 builds a tiny tree,
      crawls it, and asserts the index holds exactly the files generated. Stage 2
      exercises the A/B path with one binary as both variants and asserts the shape
      of the emitted document (both variants, per-variant row counts, one paired
      comparison, binary and commit provenance). Asserts nothing about timing.

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
          --bin PATH           measure this xdu binary as variant A. Supplying it
                               suppresses the auto-build: the binary is measured
                               exactly as given.
          --compare-bin PATH   interleaved A/B. Each timed rep runs both binaries
                               against the same tree, alternating which goes first
                               by rep parity, and the document gains comparisons[]
                               with paired per-rep deltas. This is the ONLY way to
                               compare two builds — see "the noise floor" in
                               scenarios.md.
          --compare-worktree DIR
                               record variant B's commit and build-input state from
                               this git worktree.

  sh bench/run.sh baseline [--out FILE]
      Run the committed baseline set and write bench/results/baseline.json.
      WARNING: --out defaults to the committed reference. Pass --out explicitly for
      anything that is not a deliberate re-capture of the baseline itself.

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
BIN=""
BIN_EXPLICIT=0
COMPARE_BIN=""
COMPARE_WORKTREE=""

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
        --bin)          BIN=$2; BIN_EXPLICIT=1; shift 2 ;;
        --compare-bin)  COMPARE_BIN=$2; shift 2 ;;
        --compare-worktree) COMPARE_WORKTREE=$2; shift 2 ;;
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

# Whole-tree dirtiness says nothing about which binary was measured: editing bench/ or a
# spec/ document cannot change the build. Only a modified *build input* makes the recorded
# commit a lie about the measured code, so that is probed separately and is what the
# document's provenance note is allowed to claim.
BUILD_INPUTS="src Cargo.toml Cargo.lock rust-toolchain.toml"
build_inputs_clean() {
    # Args: a git directory. Empty porcelain output over the build inputs == clean.
    [ -z "$(git -C "$1" status --porcelain -- $BUILD_INPUTS 2>/dev/null)" ]
}

if build_inputs_clean "$REPO_ROOT"; then
    MEASURES_COMMIT=true
else
    MEASURES_COMMIT=false
fi

# ---------------------------------------------------------------------------
# Build + workspace
# ---------------------------------------------------------------------------

XDU="$REPO_ROOT/target/release/xdu"
XDU_FIND="$REPO_ROOT/target/release/xdu-find"

# An explicitly named binary is measured exactly as given — never rebuilt underneath the
# caller, who may well be pointing at a worktree build or an archived artifact on purpose.
if [ "$BIN_EXPLICIT" -eq 1 ]; then
    XDU="$BIN"
    if [ ! -x "$XDU" ]; then
        echo "run.sh: --bin is not an executable: $XDU" >&2
        exit 2
    fi
    if [ ! -x "$XDU_FIND" ]; then
        echo "run.sh: building release xdu-find (needed to verify row counts)…" >&2
        ( cd "$REPO_ROOT" && cargo build --release --bin xdu-find >&2 )
    fi
elif [ ! -x "$XDU" ] || [ ! -x "$XDU_FIND" ]; then
    echo "run.sh: building release binaries (one-time)…" >&2
    ( cd "$REPO_ROOT" && cargo build --release --bin xdu --bin xdu-find >&2 )
fi

if [ -n "$COMPARE_BIN" ] && [ ! -x "$COMPARE_BIN" ]; then
    echo "run.sh: --compare-bin is not an executable: $COMPARE_BIN" >&2
    exit 2
fi

# Identity of the bytes actually executed. A path and a commit can both be right while the
# binary on disk is stale; size and mtime are what distinguish one build from another.
file_mtime_epoch() {
    case "$OS_NAME" in
        Darwin) stat -f %m "$1" ;;
        *)      stat -c %Y "$1" ;;
    esac
}

XDU_BIN_BYTES=$(wc -c < "$XDU" | tr -d ' ')
XDU_BIN_MTIME=$(file_mtime_epoch "$XDU")

COMPARE_BIN_BYTES=""
COMPARE_BIN_MTIME=""
COMPARE_COMMIT=""
COMPARE_MEASURES_COMMIT=""
if [ -n "$COMPARE_BIN" ]; then
    COMPARE_BIN_BYTES=$(wc -c < "$COMPARE_BIN" | tr -d ' ')
    COMPARE_BIN_MTIME=$(file_mtime_epoch "$COMPARE_BIN")
    if [ -n "$COMPARE_WORKTREE" ]; then
        COMPARE_COMMIT=$(git -C "$COMPARE_WORKTREE" rev-parse HEAD 2>/dev/null || echo unknown)
        if build_inputs_clean "$COMPARE_WORKTREE"; then
            COMPARE_MEASURES_COMMIT=true
        else
            COMPARE_MEASURES_COMMIT=false
        fi
    fi
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

SMOKE_GENERATED=""
SMOKE_INDEXED=""

if [ "$MODE" = "smoke" ]; then
    tree="$WORK/tree"
    index="$WORK/index"

    summary=$(python3 "$BENCH_DIR/gen_tree.py" --root "$tree" --scenario smoke)
    generated=$(printf '%s' "$summary" | python3 -c 'import json,sys; print(json.load(sys.stdin)["files"])')

    "$XDU" "$tree" -o "$index" -j 2 --apparent-size >/dev/null 2>&1
    indexed=$("$XDU_FIND" -i "$index" --count 2>/dev/null)

    # Exit 0 is not the assertion — the index must hold exactly what was generated.
    if [ "$generated" != "$indexed" ]; then
        echo "run.sh: SMOKE FAILED — generated $generated files, index holds $indexed" >&2
        exit 1
    fi
    if [ ! -f "$index/.xdu-complete" ]; then
        echo "run.sh: SMOKE FAILED — the run left no completion marker" >&2
        exit 1
    fi
    rm -rf "$tree" "$index"

    # Stage 2 runs the A/B path with one binary as both variants and falls through to the
    # ordinary measurement and report code, so comparisons[] and the provenance fields are
    # exercised rather than shipping untested. Timing is irrelevant here and not asserted.
    SMOKE_GENERATED=$generated
    SMOKE_INDEXED=$indexed
    # Same binary on both sides, so variant B's identity is variant A's. Set here because
    # the metadata block above ran before this mode chose to compare at all.
    COMPARE_BIN="$XDU"
    COMPARE_BIN_BYTES="$XDU_BIN_BYTES"
    COMPARE_BIN_MTIME="$XDU_BIN_MTIME"
    COMPARE_WORKTREE=""
    JOBS_LIST="2"
    # Big enough that the crawl takes longer than xdu's 10 ms print resolution: at scale 1
    # every wall time is 0.00s, no paired delta is computable, and the comparison this
    # stage exists to test would be vacuously empty.
    SCALE=100
    REPS=2
    WARMUP=0
    LABEL="harness self-check"
    OUT="$WORK/smoke-ab.json"
fi

# ---------------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------------

# One timed crawl with one binary, appending a TSV row unless it is a warm-up.
# Reads the scenario globals set by run_scenario. Args: binary variant jobs rep index
# A rep of 0 means warm-up: the crawl runs (populating the dentry/inode cache) but
# nothing is recorded.
time_one() {
    _bin=$1
    _variant=$2
    _jb=$3
    _rp=$4
    _idx=$5

    # A fresh index every rep: writing over an existing index changes the work
    # (finalize prunes stale chunks), which would make reps incomparable.
    rm -rf "$_idx"

    _log="$WORK/run-$_variant.log"
    if [ "$HAVE_TIME" -eq 1 ]; then
        /usr/bin/time $TIME_FLAG "$_bin" "$_tree" -o "$_idx" \
            -j "$_jb" -B "$BUFFSIZE" $SIZE_MODE_FLAG > /dev/null 2> "$_log" || {
                echo "run.sh: xdu (variant $_variant) failed (see below)" >&2
                cat "$_log" >&2; exit 1; }
    else
        "$_bin" "$_tree" -o "$_idx" \
            -j "$_jb" -B "$BUFFSIZE" $SIZE_MODE_FLAG > /dev/null 2> "$_log" || {
                echo "run.sh: xdu (variant $_variant) failed (see below)" >&2
                cat "$_log" >&2; exit 1; }
    fi

    if [ "$_rp" -eq 0 ]; then
        return 0
    fi

    _wall=$(awk '/^Completed /{ for (i=1;i<=NF;i++) if ($i ~ /^[0-9.]+s$/) { sub(/s$/,"",$i); print $i } }' "$_log" | tail -1)
    _rss=$(awk '
        /maximum resident set size/ { print $1; found=1 }
        /Maximum resident set size/ { gsub(/[^0-9]/,"",$NF); print $NF * 1024; found=1 }
        END { if (!found) print "" }' "$_log" | head -1)

    if [ -z "$_wall" ]; then
        echo "run.sh: could not parse a wall time from xdu output (variant $_variant)" >&2
        cat "$_log" >&2
        exit 1
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$_scenario" "$_scale" "$_generated" "$_bytes" "$_jb" \
        "$BUFFSIZE" "$SIZE_MODE_NAME" "$_rp" "$_wall" "$_rss" "$_variant" >> "$ROWS"
}

# The measured crawl must also be a *correct* crawl: a benchmark over an index that
# silently lost files measures nothing worth knowing. Checked per variant, so a build
# that is fast because it dropped rows fails whichever side it is on.
check_indexed() {
    _v=$1
    _idx=$2
    _jb=$3

    _indexed=$("$XDU_FIND" -i "$_idx" --count 2>/dev/null)
    printf '%s\t%s\t%s\t%s\n' "$_scenario" "$_jb" "$_v" "$_indexed" >> "$WORK/indexed.tsv"
    if [ "$_indexed" != "$_generated" ]; then
        echo "run.sh: FAILED — $_scenario -j $_jb variant $_v indexed $_indexed of $_generated files" >&2
        exit 1
    fi
}

# Run one scenario across a list of --jobs values, appending a TSV row per timed rep.
# With --compare-bin, every timed rep runs both binaries against the same tree and the
# order alternates by rep parity, so neither variant systematically runs first.
# Args: scenario scale jobs_list
run_scenario() {
    _scenario=$1
    _scale=$2
    _jobs_list=$3

    _tree="$WORK/tree-$_scenario"
    _index_a="$WORK/index-$_scenario-A"
    _index_b="$WORK/index-$_scenario-B"

    echo "run.sh: generating $_scenario (scale $_scale)…" >&2
    _summary=$(python3 "$BENCH_DIR/gen_tree.py" --root "$_tree" --scenario "$_scenario" \
                       --scale "$_scale" --force)
    printf '%s\n' "$_summary" > "$WORK/gen-$_scenario.json"
    _generated=$(printf '%s' "$_summary" | python3 -c 'import json,sys; print(json.load(sys.stdin)["files"])')
    _bytes=$(printf '%s' "$_summary" | python3 -c 'import json,sys; print(json.load(sys.stdin)["apparent_bytes"])')

    for _jobs in $_jobs_list; do
        if [ -n "$COMPARE_BIN" ]; then
            echo "run.sh: $_scenario -j $_jobs — A/B, $WARMUP warm-up + $REPS interleaved rep(s)…" >&2
        else
            echo "run.sh: $_scenario -j $_jobs — $WARMUP warm-up + $REPS timed rep(s)…" >&2
        fi

        # Warm-ups populate the cache once per variant and are never recorded.
        _w=0
        while [ "$_w" -lt "$WARMUP" ]; do
            _w=$(( _w + 1 ))
            time_one "$XDU" A "$_jobs" 0 "$_index_a"
            if [ -n "$COMPARE_BIN" ]; then
                time_one "$COMPARE_BIN" B "$_jobs" 0 "$_index_b"
            fi
        done

        _rep=0
        while [ "$_rep" -lt "$REPS" ]; do
            _rep=$(( _rep + 1 ))

            if [ -z "$COMPARE_BIN" ]; then
                time_one "$XDU" A "$_jobs" "$_rep" "$_index_a"
            elif [ $(( _rep % 2 )) -eq 1 ]; then
                time_one "$XDU" A "$_jobs" "$_rep" "$_index_a"
                time_one "$COMPARE_BIN" B "$_jobs" "$_rep" "$_index_b"
            else
                time_one "$COMPARE_BIN" B "$_jobs" "$_rep" "$_index_b"
                time_one "$XDU" A "$_jobs" "$_rep" "$_index_a"
            fi
        done

        check_indexed A "$_index_a" "$_jobs"
        if [ -n "$COMPARE_BIN" ]; then
            check_indexed B "$_index_b" "$_jobs"
        fi
    done

    # Optional syscall profile: one extra untimed rep. This is how the per-file stat
    # count is proven to have moved or shrunk, independent of wall-clock noise.
    if [ "$SYSCALLS" -eq 1 ] && [ "$HAVE_STRACE" -eq 1 ]; then
        echo "run.sh: profiling $_scenario under strace (slow)…" >&2
        rm -rf "$_index_a"
        strace -f -c -e trace=%file,%stat -o "$WORK/strace-$_scenario.txt" \
            "$XDU" "$_tree" -o "$_index_a" -j 4 -B "$BUFFSIZE" $SIZE_MODE_FLAG \
            > /dev/null 2>&1 || true
    fi

    rm -rf "$_tree" "$_index_a" "$_index_b"
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
    smoke)
        # Stage 2 of the self-check, configured in the smoke block above.
        run_scenario smoke "$SCALE" "$JOBS_LIST"
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
export XDU_VERSION GIT_COMMIT GIT_DIRTY MEASURES_COMMIT
export XDU_BIN="$XDU" XDU_BIN_BYTES XDU_BIN_MTIME
export COMPARE_BIN COMPARE_BIN_BYTES COMPARE_BIN_MTIME
export COMPARE_COMMIT COMPARE_WORKTREE COMPARE_MEASURES_COMMIT
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
            "variant": fields[10],
        })

indexed = {}
with open(os.environ["INDEXED_TSV"]) as handle:
    for line in handle:
        scenario, jobs, variant, count = line.rstrip("\n").split("\t")
        indexed[(scenario, int(jobs), variant)] = int(count)


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


def binary_identity(path, size, mtime_epoch):
    """A path and a commit can both be right while the binary on disk is stale."""
    return {
        "path": path,
        "bytes": int(size),
        "mtime": datetime.fromtimestamp(int(mtime_epoch), timezone.utc)
                         .replace(microsecond=0).isoformat(),
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
for key in sorted({(r["scenario"], r["jobs"], r["variant"]) for r in rows}):
    scenario, jobs, variant = key
    group = [r for r in rows if (r["scenario"], r["jobs"], r["variant"]) == key]
    first = group[0]
    walls = [r["wall_s"] for r in group]
    rates = [first["generated_files"] / w for w in walls if w > 0]

    runs.append({
        "scenario": scenario,
        "variant": variant,
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

# Paired per-rep deltas are the number a "faster" claim rests on, so the document carries
# them rather than leaving them to be re-derived from runs[]. Pairing is by rep number,
# which is exactly the interleaved pair that ran back to back against one tree — that is
# what removes the between-invocation drift a two-document comparison cannot see.
comparisons = []
if os.environ.get("COMPARE_BIN"):
    for scenario, jobs in sorted({(r["scenario"], r["jobs"]) for r in rows}):
        a = {r["rep"]: r for r in rows
             if (r["scenario"], r["jobs"], r["variant"]) == (scenario, jobs, "A")}
        b = {r["rep"]: r for r in rows
             if (r["scenario"], r["jobs"], r["variant"]) == (scenario, jobs, "B")}
        # Filter once, up front: every number below is derived from the same set of pairs,
        # so reps, samples and a_faster_reps cannot disagree about which reps they describe.
        # A zero B wall time (a tree too small for the crawler's 10 ms print resolution)
        # yields no delta, so that pair is not a rep for this purpose.
        paired = [i for i in sorted(set(a) & set(b)) if b[i]["wall_s"] > 0]
        if not paired:
            continue

        # Positive means variant A finished faster than variant B on that rep.
        samples = [round((b[i]["wall_s"] - a[i]["wall_s"]) / b[i]["wall_s"] * 100.0, 4)
                   for i in paired]
        a_rss = [a[i]["rss_bytes"] for i in paired if a[i]["rss_bytes"] is not None]
        b_rss = [b[i]["rss_bytes"] for i in paired if b[i]["rss_bytes"] is not None]

        rss_delta = None
        if a_rss and b_rss:
            b_rss_median = statistics.median(b_rss)
            if b_rss_median > 0:
                rss_delta = round(
                    (b_rss_median - statistics.median(a_rss)) / b_rss_median * 100.0, 4)

        comparisons.append({
            "scenario": scenario,
            "jobs": jobs,
            "a_median_s": round(statistics.median([a[i]["wall_s"] for i in paired]), 4),
            "b_median_s": round(statistics.median([b[i]["wall_s"] for i in paired]), 4),
            "paired_delta_pct": {
                "median": round(statistics.median(samples), 4) if samples else None,
                "samples": samples,
                "a_faster_reps": sum(1 for i in paired if a[i]["wall_s"] < b[i]["wall_s"]),
                "reps": len(paired),
            },
            "peak_rss_delta_pct": rss_delta,
        })

notes = []
# Only claim what was actually probed: git_dirty covers the whole tree, but whether the
# recorded commit describes the measured binary depends solely on the build inputs.
if os.environ["MEASURES_COMMIT"] == "true":
    if os.environ["GIT_DIRTY"] == "true":
        notes.append(
            "The working tree was dirty at capture, but no build input (src/, Cargo.toml, "
            "Cargo.lock, rust-toolchain.toml) was modified, so the measured binary is the "
            "build of the recorded commit."
        )
else:
    notes.append(
        "A build input (src/, Cargo.toml, Cargo.lock, rust-toolchain.toml) was modified at "
        "capture: git_commit names the BASE commit the working tree sat on, NOT the code "
        "that was measured. Treat this document as measuring an unnamed build."
    )
notes.append(
    "Every median here comes from a SINGLE invocation. Two invocations of the same binary "
    "have been observed to differ by up to ~20% on the reference host, so comparing "
    "medians across two documents cannot resolve a small change. Use --compare-bin, which "
    "interleaves both binaries inside one invocation and reports paired per-rep deltas — "
    "see 'The noise floor' in scenarios.md."
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
        "measures_recorded_commit": os.environ["MEASURES_COMMIT"] == "true",
        "binary": binary_identity(
            os.environ["XDU_BIN"],
            os.environ["XDU_BIN_BYTES"],
            os.environ["XDU_BIN_MTIME"],
        ),
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

if os.environ.get("COMPARE_BIN"):
    document["compare"] = {
        "delta_sign": (
            "positive paired_delta_pct means variant A finished faster than variant B"
        ),
        "interleaved": (
            "each timed rep ran both binaries against the same tree, alternating which "
            "went first by rep parity"
        ),
        "variant_a": {"binary": document["xdu"]["binary"],
                      "git_commit": os.environ["GIT_COMMIT"],
                      "measures_recorded_commit": os.environ["MEASURES_COMMIT"] == "true"},
        "variant_b": {
            "binary": binary_identity(
                os.environ["COMPARE_BIN"],
                os.environ["COMPARE_BIN_BYTES"],
                os.environ["COMPARE_BIN_MTIME"],
            ),
            "worktree": os.environ.get("COMPARE_WORKTREE") or None,
            "git_commit": os.environ.get("COMPARE_COMMIT") or None,
            "measures_recorded_commit": (
                None if not os.environ.get("COMPARE_MEASURES_COMMIT")
                else os.environ["COMPARE_MEASURES_COMMIT"] == "true"
            ),
        },
    }
    document["comparisons"] = comparisons

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

# ---------------------------------------------------------------------------
# smoke stage 2 — assert the A/B document's shape (never its timings)
# ---------------------------------------------------------------------------

if [ "$MODE" = "smoke" ]; then
    SMOKE_JSON="$OUT" SMOKE_GENERATED="$SMOKE_GENERATED" \
    SMOKE_BIN="$XDU" SMOKE_MEASURES_COMMIT="$MEASURES_COMMIT" python3 - <<'PYTHON' || exit 1
import json
import os
import sys

document = json.load(open(os.environ["SMOKE_JSON"]))
generated = int(os.environ["SMOKE_GENERATED"])
failures = []

variants = {run["variant"] for run in document["runs"]}
if variants != {"A", "B"}:
    failures.append(f"expected variants A and B, got {sorted(variants)}")

for run in document["runs"]:
    if run["indexed_files"] != run["generated_files"]:
        failures.append(
            f"variant {run['variant']}: indexed {run['indexed_files']} of "
            f"{run['generated_files']} generated")

comparisons = document.get("comparisons", [])
if len(comparisons) != 1:
    failures.append(f"expected exactly one comparison, got {len(comparisons)}")
else:
    delta = comparisons[0]["paired_delta_pct"]
    if delta["reps"] != len(delta["samples"]):
        failures.append(
            f"reps {delta['reps']} does not match {len(delta['samples'])} samples")
    if delta["reps"] < 1:
        failures.append("no paired reps were recorded")

# Both directions, so the build-input probe is tested rather than assumed: a clean tree
# must claim the commit, a dirty one must refuse to.
expected = os.environ["SMOKE_MEASURES_COMMIT"] == "true"
if document["xdu"]["measures_recorded_commit"] is not expected:
    failures.append(
        f"measures_recorded_commit is {document['xdu']['measures_recorded_commit']}, "
        f"expected {expected} for this tree")

on_disk = os.path.getsize(os.environ["SMOKE_BIN"])
if document["xdu"]["binary"]["bytes"] != on_disk:
    failures.append(
        f"recorded binary size {document['xdu']['binary']['bytes']} != {on_disk} on disk")

if failures:
    for failure in failures:
        print(f"run.sh: SMOKE FAILED — {failure}", file=sys.stderr)
    sys.exit(1)

# stdout belongs to the single "smoke ok:" line the shell prints below.
ab_files = {run["generated_files"] for run in document["runs"]}
print(f"run.sh: A/B self-check passed ({len(document['runs'])} runs over "
      f"{max(ab_files)} files, {comparisons[0]['paired_delta_pct']['reps']} paired reps)",
      file=sys.stderr)
PYTHON

    echo "smoke ok: generated $SMOKE_GENERATED files, indexed $SMOKE_INDEXED, marker present; A/B document shape verified"
fi
