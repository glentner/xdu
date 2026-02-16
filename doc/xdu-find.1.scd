xdu-find(1)

# NAME

xdu-find - query a file metadata index for matching paths

# SYNOPSIS

*xdu-find* *-i* _DIR_ \[*-p* _REGEX_] \[*-u* _NAME_] \[*--min-size* _SIZE_] \[*--max-size* _SIZE_]
	\[*--older-than* _DAYS_] \[*--newer-than* _DAYS_] \[*-f* _FORMAT_] \[*-l* _N_]
	\[*-c*] \[*--top* _N_]

# DESCRIPTION

*xdu-find* queries a Parquet index built by *xdu*(1) for files matching the
given filter criteria. Results are printed to standard output in the requested
format, suitable for piping to other tools.

Uses DuckDB for efficient Parquet queries with partition pruning.

# OPTIONS

*-i*, *--index* _DIR_
	Path to the Parquet index directory. Required. See also *XDU_INDEX* in
	_ENVIRONMENT_.

*-p*, *--pattern* _REGEX_
	Regular expression pattern to match file paths.

*-u*, *--partition* _NAME_
	Filter by partition (top-level subdirectory name, typically a username).

*--min-size* _SIZE_
	Minimum file size. Supports suffixes: K, M, G, T (and KiB, MiB, GiB,
	TiB variants).

*--max-size* _SIZE_
	Maximum file size. Same suffix support as *--min-size*.

*--older-than* _DAYS_
	Match files not accessed in _DAYS_ days.

*--newer-than* _DAYS_
	Match files accessed within _DAYS_ days.

*-f*, *--format* _FORMAT_
	Output format. One of: *path* (default), *size*, *atime*, *csv*, *json*.

*-l*, *--limit* _N_
	Limit number of results.

*-c*, *--count*
	Print the count of matching records instead of listing them.

*--top* _N_
	Show the top _N_ partitions by file count.

*-h*, *--help*
	Print help information.

*-V*, *--version*
	Print version information.

# ENVIRONMENT

*XDU_INDEX*
	Default index directory if *-i* is not specified.

# EXAMPLES

Find all Python files:

	xdu-find -i /index/scratch -p '\\.py$'

Find large files not accessed in 90 days:

	xdu-find -i /index/scratch --min-size 1G --older-than 90

Query a specific user's partition, sorted by size:

	xdu-find -i /index/scratch -u alice --min-size 100M -f size

Count matching files:

	xdu-find -i /index/scratch -p '\\.tmp$' --older-than 30 --count

Pipe results to xargs for bulk operations:

	xdu-find -i /index/scratch -p '\\.tmp$' --older-than 30 | xargs rm

Show top 10 partitions by file count:

	xdu-find -i /index/scratch --top 10

# SEE ALSO

*xdu*(1), *xdu-view*(1), *xdu-rm*(1)
