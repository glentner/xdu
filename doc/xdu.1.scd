xdu(1)

# NAME

xdu - build a distributed file metadata index in Parquet format

# SYNOPSIS

*xdu* _DIR_ *-o* _OUTDIR_ \[*-j* _N_] \[*-B* _N_] \[*--apparent-size*] \[*-k* _SIZE_] \[*-p* _NAMES_] \[*--allow-errors*]

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

*--allow-errors*
	Continue past unreadable files or directories (permission or I/O
	errors) instead of failing the run. See _EXIT STATUS_.

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
partitions correspond to top-level subdirectories of _DIR_. Files lying
directly in _DIR_ are indexed under the reserved partition name *__root__*.
A real top-level subdirectory of that name would write into the same
partition directory and clobber its chunks, so *xdu* rejects it with an
error rather than corrupting the index; rename the directory to index it.

Each Parquet file contains three columns:

- *path* (UTF-8): Absolute file path
- *size* (INT64): File size in bytes
- *atime* (INT64): Last access time as Unix epoch seconds

Files are written atomically using a .partial suffix, then renamed on
completion. Stale chunks from previous runs are automatically pruned.

On success *xdu* writes a run-level completion marker, _OUTDIR_/.xdu-complete,
holding the version and this run's totals. The marker is removed when a run
starts and written only once every partition has been walked and finalized, so
an index left behind by a failed or interrupted run carries no marker. Readers
glob _OUTDIR_/*/*.parquet, so the marker is never mistaken for a partition.

Paths are stored as UTF-8. A filename containing invalid UTF-8 is stored with
replacement characters, so that row no longer names a real file and cannot be
removed with *xdu-rm*(1). *xdu* reports the first such path in each partition
and counts the remainder in its summary; storing the raw bytes would require an
index format change and is not done here.

# EXIT STATUS

*xdu* exits *0* on success. If the crawl cannot read a file or directory
(a permission or I/O error — for example an unreadable subtree that would
otherwise be silently omitted), it reports the offending path and error to
standard error and exits *non-zero*, so a partial index is never presented as
complete. A failing run also leaves no completion marker (see _OUTPUT FORMAT_),
and the remaining threads stop taking on new partitions once one has failed.
Files that vanish between being listed and being stat'd (an ENOENT race on a
live filesystem) are counted and skipped without failing the run.

Pass *--allow-errors* to downgrade the hard errors to warnings: the crawl then
indexes every reachable file and exits *0*. The per-path diagnostics and the
skipped/error counts in the summary are printed either way, and all such
diagnostics go to standard error so piped output on standard output stays clean.

# SEE ALSO

*xdu-find*(1), *xdu-view*(1), *xdu-rm*(1)
