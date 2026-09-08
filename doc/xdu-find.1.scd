xdu-find(1)

# NAME

xdu-find - query a file metadata index for matching paths

# SYNOPSIS

*xdu-find* *-i* _DIR_ \[*-p* _PATTERN_] \[*--regex*] \[*-u* _NAME_] \[*--min-size* _SIZE_] \[*--max-size* _SIZE_]
	\[*--older-than* _DAYS_] \[*--newer-than* _DAYS_] \[*--owner* _USER_] \[*--group* _GROUP_]
	\[*--mode* _SPEC_] \[*--mtime-older-than* _DAYS_] \[*--mtime-newer-than* _DAYS_]
	\[*-f* _FORMAT_] \[*-l* _N_]
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

*-p*, *--pattern* _PATTERN_
	Glob pattern to match file paths. Supports \*, ?, and [...] classes;
	\* crosses directories, so \*.py matches Python files at any depth. A bare
	word matches only that exact name — write \*py\* for a substring search.

*--regex*
	Interpret *-p* as a full regular expression instead of a glob.

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

*--owner* _USER_
	Match files owned by _USER_: a user name resolved at query time, or a
	numeric uid. An unresolvable name fails without printing rows.

*--group* _GROUP_
	Match files owned by group _GROUP_: a group name resolved at query time,
	or a numeric gid. An unresolvable name fails without printing rows.

*--mode* _SPEC_
	Match permission bits. Bare octal matches exactly (*644*); a */* prefix
	matches when any listed bit is set (*/002* is world-writable); an *&*
	prefix matches when all listed bits are set (*&4000* is setuid).

*--mtime-older-than* _DAYS_
	Match files not modified in _DAYS_ days.

*--mtime-newer-than* _DAYS_
	Match files modified within _DAYS_ days.

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

	xdu-find -i /index/scratch -p '\*.py'

Find large files not accessed in 90 days:

	xdu-find -i /index/scratch --min-size 1G --older-than 90

Query a specific user's partition, sorted by size:

	xdu-find -i /index/scratch -u alice --min-size 100M -f size

Count matching files:

	xdu-find -i /index/scratch -p '\*.tmp' --older-than 30 --count

Pipe results to xargs for bulk operations:

	xdu-find -i /index/scratch -p '\*.tmp' --older-than 30 | xargs rm

The same search with a regular expression:

	xdu-find -i /index/scratch --regex -p '\\.tmp$' --older-than 30 --count

Show top 10 partitions by file count:

	xdu-find -i /index/scratch --top 10

# SEE ALSO

*xdu*(1), *xdu-view*(1), *xdu-rm*(1)
