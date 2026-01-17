<div align="center">

# xdu

**High-performance file system indexer for large-scale storage administration**

[![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/glentner/xdu)](https://github.com/glentner/xdu/releases)
[![Tests](https://img.shields.io/github/actions/workflow/status/glentner/xdu/test.yaml?branch=main&label=tests)](https://github.com/glentner/xdu/actions/workflows/test.yaml)

</div>

Builds Hive-partitioned Parquet indexes for instant analytics on filesystems with hundreds of millions of files.

## Motivation

System administrators managing large shared storage (e.g., HPC clusters, research computing, NAS appliances) need to regularly audit disk usage and file access patterns. Common tasks include:

- Identifying users consuming excessive space
- Finding stale data for purge policies
- Generating usage reports by user or project
- Enforcing quotas and retention policies

Traditional tools like `du` and `find` are designed for interactive, one-off queries. They re-traverse the filesystem every time, which is prohibitively slow on systems with hundreds of millions of files. They also produce flat text output that's difficult to analyze at scale.

**xdu** solves this by building a persistent, queryable index once—then enabling instant analytics via the included `xdu-find` command, interactive exploration with `xdu-view`, or external tools like DuckDB, Polars, or Apache Spark.

## Design

### Hive-Partitioned Output

The index is partitioned by top-level subdirectory. For a `/home` filesystem:

```
index/
├── alice/
│   ├── 000000.parquet
│   └── 000001.parquet
├── bob/
│   └── 000000.parquet
└── charlie/
    └── 000000.parquet
```

This layout enables:
- **Partition pruning**: Query a single user's data without scanning the entire index
- **Parallel writes**: Each partition is processed independently
- **Incremental updates**: Re-index individual partitions without rebuilding everything

### Schema

| Column | Type  | Description                    |
|--------|-------|--------------------------------|
| path   | UTF-8 | Absolute file path             |
| size   | INT64 | File size in bytes             |
| atime  | INT64 | Last access time (Unix epoch)  |

### Performance

**Why xdu is faster than `du` for this use-case:**

1. **Parallel traversal**: Multiple threads crawl different partitions simultaneously
2. **Columnar storage**: Parquet compresses paths efficiently (often 10:1) and enables predicate pushdown
3. **Buffered writes**: Records accumulate in memory before flushing, minimizing I/O syscalls
4. **One traversal, many queries**: Build the index once, query it thousands of times instantly

A typical 100M file filesystem might take 2-3 hours to `du`. With xdu, the index builds in 20-30 minutes and subsequent queries complete in seconds.

## Installation

### Quick Install (Recommended)

```bash
curl -sSfL https://raw.githubusercontent.com/glentner/xdu/main/install.sh | sh
```

This downloads the latest release binary for your platform and installs it to `~/.local/bin`.

**Options:**
- `XDU_VERSION=v0.1.0` — Install a specific version
- `XDU_INSTALL=/usr/local/bin` — Change install directory

### From Source

```bash
cargo install --git https://github.com/glentner/xdu.git
```

Requires [Rust](https://rustup.rs) nightly toolchain.

## Usage

### Building an Index

```bash
xdu /home -o /var/lib/xdu/home -j 16 -b 100000
```

| Option | Description | Default |
|--------|-------------|---------|
| `-o, --outdir` | Output directory for index | Required |
| `-j, --jobs` | Parallel threads | 4 |
| `-b, --buffsize` | Records per Parquet chunk | 100000 |

### Querying with xdu-find

The `xdu-find` command provides a convenient CLI for common queries:

```bash
# Find all Python files
xdu-find -i /var/lib/xdu/home -p '\.py$'

# Find large files (>1GB) not accessed in 90 days
xdu-find -i /var/lib/xdu/home --min-size 1G --older-than 90

# Query a specific user's partition, sorted by size
xdu-find -i /var/lib/xdu/home -u alice --min-size 100M -f size

# Count matching files
xdu-find -i /var/lib/xdu/home -p '\.tmp$' --older-than 30 --count

# Pipe to xargs for bulk operations
xdu-find -i /var/lib/xdu/home -p '\.tmp$' --older-than 30 | xargs rm
```

| Option | Description | Default |
|--------|-------------|---------|
| `-i, --index` | Path to Parquet index directory | Required |
| `-p, --pattern` | Regex pattern to match paths | |
| `-u, --partition` | Filter by partition (user directory) | |
| `--min-size` | Minimum file size (e.g., 1K, 10M, 1G) | |
| `--max-size` | Maximum file size | |
| `--older-than` | Files not accessed in N days | |
| `--newer-than` | Files accessed within N days | |
| `-f, --format` | Output format: path, size, atime, csv, json | path |
| `-l, --limit` | Limit number of results | |
| `-c, --count` | Count matching records | |

### Interactive Exploration with xdu-view

The `xdu-view` command provides an ncdu-style TUI for browsing the index interactively, with powerful filtering and sorting capabilities.

```bash
# Browse all partitions
xdu-view -i /var/lib/xdu/home

# Start in a specific partition
xdu-view -i /var/lib/xdu/home -u alice

# View only files not accessed in 30 days, sorted by size
xdu-view -i /var/lib/xdu/home --older-than 30 -s size-desc

# View large Python files (>1MB)
xdu-view -i /var/lib/xdu/home -p '\.py$' --min-size 1M
```

#### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-i, --index` | Path to Parquet index directory | Required |
| `-u, --partition` | Start in a specific partition | |
| `-p, --pattern` | Regex pattern to filter paths | |
| `--min-size` | Minimum file size (e.g., 1K, 10M, 1G) | |
| `--max-size` | Maximum file size | |
| `--older-than` | Files not accessed in N days | |
| `--newer-than` | Files accessed within N days | |
| `-s, --sort` | Sort order (see below) | name |

**Sort modes:** `name` (default, directories first), `size-desc`, `size-asc`, `count-desc`, `count-asc`, `age-desc` (oldest first), `age-asc` (newest first)

#### Keybindings

**Navigation:**
| Key | Action |
|-----|--------|
| `↑`/`k` | Move selection up |
| `↓`/`j` | Move selection down |
| `→`/`Enter`/`Space` | Enter directory |
| `←`/`Backspace` | Go up / back |
| `q`/`Esc` | Quit |

**Sorting:**
| Key | Action |
|-----|--------|
| `s` | Open sort selector (use `↑↓`/`jk` to choose, `Enter` to confirm, `Esc` to cancel) |

**Filtering (interactive):**
| Key | Action |
|-----|--------|
| `/` | Set path pattern filter (regex) |
| `o` | Set older-than filter (days) |
| `n` | Set newer-than filter (days) |
| `>` | Set minimum size filter |
| `<` | Set maximum size filter |
| `c` | Clear all filters |

When entering a filter value, type the value and press `Enter` to apply, or `Esc` to cancel.

#### UI Elements

**Title bar** shows the current location and any active filters:
```
┌─ alice/projects [older:30d] [min:1.00 MiB] [/\.py$/] ─────────────────────┐
```

**Status bar** shows entry count, current sort mode, and available keybindings:
```
 42 entries in 0.15s (filtered) │ sort:size-desc │ q:quit jk:nav /:pattern ...
```

**List entries** show name, total size, file count, and most recent access time:
```
▸ src                          1.23 GiB    4.2K files    3 days ago
▸ tests                      128.50 MiB      892 files    1 month ago
  README.md                    4.50 KiB        1 file     today
```

#### Common Use Cases

**Find where old data is hiding:**
```bash
# Launch with 90-day stale filter, sorted by size
xdu-view -i /var/lib/xdu/home --older-than 90 -s size-desc
```
Drill down into the largest directories to find purgeable data.

**Audit a specific user's storage:**
```bash
# Jump directly to a user, show files >100MB
xdu-view -i /var/lib/xdu/home -u alice --min-size 100M
```

**Identify recently active directories:**
```bash
# Show only data accessed in the last 7 days
xdu-view -i /var/lib/xdu/home --newer-than 7 -s count-desc
```

**Find specific file types:**
```bash
# Explore all Jupyter notebooks
xdu-view -i /var/lib/xdu/home -p '\.ipynb$' -s size-desc
```

### Querying with DuckDB

The Parquet index integrates seamlessly with DuckDB for instant analytics:

```sql
-- Total usage per user
SELECT
    regexp_extract(path, '/home/([^/]+)/', 1) AS user,
    sum(size) / 1e12 AS tb
FROM read_parquet('/var/lib/xdu/home/*/*.parquet')
GROUP BY user
ORDER BY tb DESC;

-- Files not accessed in 180 days
SELECT path, size, atime
FROM read_parquet('/var/lib/xdu/home/*/*.parquet')
WHERE atime < epoch(now()) - 86400 * 180
ORDER BY size DESC
LIMIT 100;

-- Query single user (partition pruning)
SELECT sum(size) / 1e9 AS gb
FROM read_parquet('/var/lib/xdu/home/alice/*.parquet');
```

## Scheduling

Run xdu via cron to maintain a fresh index:

```cron
0 2 * * * /usr/local/bin/xdu /home -o /var/lib/xdu/home -j 32
```

## Pro Tips

### Index ZFS Snapshots for Point-in-Time Accuracy

On large ZFS filesystems (multi-petabyte scale), a full index can take 8-12+ hours. During that time, users may create, modify, or delete files—leading to an index that represents a "smeared" view of the filesystem rather than a consistent point-in-time snapshot.

For accurate auditing, **index a ZFS snapshot instead of the live filesystem**:

```bash
#!/bin/bash
# snapshot-index.sh - Atomic snapshot + index workflow

POOL="tank/home"
SNAP="$POOL@xdu-$(date +%Y%m%d)"
MOUNT="/mnt/xdu-snapshot"
INDEX="/var/lib/xdu/home"

# Create snapshot (instantaneous)
zfs snapshot "$SNAP"

# Mount read-only
mkdir -p "$MOUNT"
mount -t zfs -o ro "$SNAP" "$MOUNT"

# Build index from snapshot
xdu "$MOUNT" -o "$INDEX" -j 32

# Cleanup
umount "$MOUNT"
zfs destroy "$SNAP"

echo "Index complete: $INDEX"
```

**Benefits:**
- **Consistency**: The index reflects an exact point-in-time state
- **Safety**: Indexing a read-only snapshot eliminates any risk of accidental modification
- **Reproducibility**: If questions arise about the data, you can re-mount the same snapshot
- **No user impact**: Snapshot creation is instantaneous; users don't experience slowdowns

For rolling retention, keep the last few snapshots:

```bash
# Keep snapshots for 7 days
zfs destroy tank/home@xdu-$(date -d '7 days ago' +%Y%m%d) 2>/dev/null || true
```

### Incremental Partition Updates

If only a few users have changed significantly, re-index just their partitions:

```bash
# Re-index only alice's data
xdu /home -o /var/lib/xdu/home -j 8 --partition alice
```

This is much faster than a full re-index and automatically prunes stale chunks from previous runs.

## License

MIT
