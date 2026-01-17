# xdu

A high-performance file system indexer that builds Hive-partitioned Parquet indexes for large-scale storage administration.

## Motivation

System administrators managing large shared storage (e.g., HPC clusters, research computing, NAS appliances) need to regularly audit disk usage and file access patterns. Common tasks include:

- Identifying users consuming excessive space
- Finding stale data for purge policies
- Generating usage reports by user or project
- Enforcing quotas and retention policies

Traditional tools like `du` and `find` are designed for interactive, one-off queries. They re-traverse the filesystem every time, which is prohibitively slow on systems with hundreds of millions of files. They also produce flat text output that's difficult to analyze at scale.

**xdu** solves this by building a persistent, queryable index once—then enabling instant analytics via modern tools like DuckDB, Polars, or Apache Spark.

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

```bash
cargo install --path .
```

## Usage

```bash
xdu /home -o /var/lib/xdu/home -j 16 -b 100000
```

| Option | Description | Default |
|--------|-------------|---------|
| `-o, --outdir` | Output directory for index | Required |
| `-j, --jobs` | Parallel threads | 4 |
| `-b, --buffsize` | Records per Parquet chunk | 100000 |

## Querying with DuckDB

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

## License

MIT
