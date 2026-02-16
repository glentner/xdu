xdu(1)

# NAME

xdu - build a distributed file metadata index in Parquet format

# SYNOPSIS

*xdu* _DIR_ *-o* _OUTDIR_ \[*-j* _N_] \[*-B* _N_] \[*--apparent-size*] \[*-k* _SIZE_] \[*-p* _NAMES_]

# DESCRIPTION

*xdu* crawls a filesystem directory tree and builds a Hive-partitioned Parquet
index containing file paths, sizes, and access times. The index is partitioned
by top-level subdirectory, enabling per-user queries without full-index scans.

Designed for HPC and enterprise storage environments where traditional tools
like *du*(1) and *find*(1) are too slow for regular auditing of filesystems
with hundreds of millions of files.

The index can be queried with *xdu-find*(1), explored interactively with
*xdu-view*(1), or analyzed directly with tools like DuckDB, Polars, or
Apache Spark.

# OPTIONS

_DIR_
	Top-level directory to index.

*-o*, *--outdir* _DIR_
	Output directory for the Parquet index. Required.

*-j*, *--jobs* _N_
	Number of parallel threads. Default: 4. See also *XDU_JOBS* in
	_ENVIRONMENT_.

*-B*, *--buffsize* _N_
	Number of records per output Parquet chunk. Default: 100000.

*--apparent-size*
	Report apparent file sizes (file length from stat) rather than actual
	disk usage (st_blocks \* 512). By default, *xdu* reports disk usage.

*-k*, *--block-size* _SIZE_
	Round sizes up to this block size (e.g., 128K, 1M). Useful when
	st_blocks is inaccurate (e.g., over NFS) and you know the filesystem
	block size. Implies *--apparent-size* for the base size, then rounds up.

*-p*, *--partition* _NAMES_
	Index only specific partitions (top-level subdirectory names,
	comma-separated). Useful for incremental re-indexing.

*-h*, *--help*
	Print help information.

*-V*, *--version*
	Print version information.

# ENVIRONMENT

*XDU_JOBS*
	Default number of parallel threads if *-j* is not specified.

# EXAMPLES

Index an entire filesystem:

	xdu /data/scratch -o /index/scratch -j 8

Re-index specific partitions:

	xdu /data/scratch -o /index/scratch --partition alice,bob

Index with apparent sizes:

	xdu /data/scratch -o /index/scratch --apparent-size

Index with block size rounding for NFS:

	xdu /data/scratch -o /index/scratch --block-size 128K

# OUTPUT FORMAT

The index is structured as _OUTDIR_/<partition>/<chunk>.parquet, where
partitions correspond to top-level subdirectories of _DIR_.

Each Parquet file contains three columns:

- *path* (UTF-8): Absolute file path
- *size* (INT64): File size in bytes
- *atime* (INT64): Last access time as Unix epoch seconds

Files are written atomically using a .partial suffix, then renamed on
completion. Stale chunks from previous runs are automatically pruned.

# SEE ALSO

*xdu-find*(1), *xdu-view*(1), *xdu-rm*(1)
