# xdu

High-performance file system indexer and query tools for storage administration.

## Overview

xdu provides four commands:
- **xdu**: Crawls a filesystem and builds a Hive-partitioned Parquet index containing file paths, sizes, and access times
- **xdu-find**: Queries the index using DuckDB, with filters for path patterns, size, and access time
- **xdu-view**: Interactive TUI for exploring the index, inspired by ncdu
- **xdu-rm**: Bulk file deletion with parallel processing and safe mode for stale index protection

Designed for HPC and enterprise storage environments where traditional tools like `du` and `find` are too slow for regular auditing.

## Architecture

- **CLI**: `clap` for argument parsing with env var fallbacks (`XDU_JOBS`, `XDU_INDEX`)
- **Parallelism**: `jwalk` for adaptive parallel directory traversal with parallel metadata loading, backed by a shared `rayon` thread pool (`RayonExistingPool`)
- **Concurrency**: C driver threads (`std::thread`) each pull partitions from a work queue; N rayon pool threads handle directory reads across all active walkers via work-stealing
- **Output**: `arrow` + `parquet` crates write Snappy-compressed Parquet files with atomic `.partial` → `.parquet` finalization
- **Query**: `duckdb` crate for efficient Parquet queries with partition pruning
- **TUI**: `ratatui` + `crossterm` for interactive terminal interface
- **Progress**: `indicatif` MultiProgress with per-partition bars showing file count, bytes, crawl speed (1s rolling window), peak speed, and driver thread ID

## Key Files

- `src/lib.rs` - Shared types (FileRecord, schema) and formatting utilities
- `src/bin/xdu.rs` - Indexer binary
- `src/bin/xdu-find.rs` - Query binary using DuckDB
- `src/bin/xdu-view.rs` - Interactive TUI explorer
- `src/bin/xdu-rm.rs` - Bulk deletion binary with safe mode

## Build & Run

```bash
cargo build --release

# Build index
./target/release/xdu /path/to/scan -o /path/to/index -j 8

# Query index
./target/release/xdu-find -i /path/to/index -p '\.py$' --min-size 1M

# Interactive explorer
./target/release/xdu-view -i /path/to/index
```

## Design Decisions

- **Shared-pool concurrent walks**: A single rayon thread pool is shared across all jwalk walkers. Driver threads consume walker iterators and write buffers; rayon work-stealing balances directory reads across all active partitions. Thread budget: N pool + C drivers + 1 main.
- **Partition by top-level subdirectory**: Enables per-user queries without full scans. `--partition` allows incremental re-indexing of specific partitions.
- **Atomic writes**: Chunks are written as `.partial` files, then renamed to `.parquet` on finalization. Stale chunks from previous runs are pruned.
- **Buffered writes**: Accumulate records in memory to reduce I/O overhead
- **Unix-only**: Uses `MetadataExt` for atime; not portable to Windows
- **Snappy compression**: Fast encode/decode, reasonable compression ratio
- **DuckDB for queries**: Native Parquet support with glob patterns and partition pruning

## Pre-Release Checklist

Before tagging a release, always run:

```bash
# Run clippy with CI-equivalent flags
cargo clippy --all-targets --all-features -- -D warnings

# Run all tests
cargo test
```

Both must pass with zero warnings/errors before pushing a release tag.
