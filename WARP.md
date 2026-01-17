# xdu

High-performance file system indexer for storage administration.

## Overview

xdu crawls a filesystem and builds a Hive-partitioned Parquet index containing file paths, sizes, and access times. Designed for HPC and enterprise storage environments where traditional tools like `du` are too slow for regular auditing.

## Architecture

- **CLI**: `clap` for argument parsing
- **Parallelism**: `rayon` thread pool processes top-level subdirectories concurrently
- **Output**: `arrow` + `parquet` crates write Snappy-compressed Parquet files
- **Traversal**: `walkdir` for recursive directory iteration

## Key Files

- `src/main.rs` - Single-file implementation with CLI, traversal, and Parquet writing

## Build & Run

```bash
cargo build --release
./target/release/xdu /path/to/scan -o /path/to/index -j 8
```

## Design Decisions

- **Partition by top-level subdirectory**: Enables per-user queries without full scans
- **Buffered writes**: Accumulate records in memory to reduce I/O overhead
- **Unix-only**: Uses `MetadataExt` for atime; not portable to Windows
- **Snappy compression**: Fast encode/decode, reasonable compression ratio
