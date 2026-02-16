xdu-view(1)

# NAME

xdu-view - interactive TUI for exploring a file metadata index

# SYNOPSIS

*xdu-view* *-i* _DIR_ \[*-u* _NAME_] \[*-p* _REGEX_] \[*--min-size* _SIZE_]
	\[*--max-size* _SIZE_] \[*--older-than* _DAYS_] \[*--newer-than* _DAYS_]
	\[*-s* _MODE_]

# DESCRIPTION

*xdu-view* provides an ncdu-style terminal user interface for browsing a
Parquet index built by *xdu*(1). It supports two view modes:

- *List mode* (default): Traditional single-list view with directory navigation.
- *Tree mode*: Miller columns showing directory hierarchy left-to-right, with
  file type detection and plain-text preview in the rightmost pane.

All command-line filters can also be modified interactively.

# OPTIONS

*-i*, *--index* _DIR_
	Path to the Parquet index directory. Required. See also *XDU_INDEX* in
	_ENVIRONMENT_.

*-u*, *--partition* _NAME_
	Start in a specific partition instead of the partition list.

*-p*, *--pattern* _REGEX_
	Regular expression pattern to filter file paths.

*--min-size* _SIZE_
	Minimum file size. Supports suffixes: K, M, G, T.

*--max-size* _SIZE_
	Maximum file size.

*--older-than* _DAYS_
	Show only files not accessed in _DAYS_ days.

*--newer-than* _DAYS_
	Show only files accessed within _DAYS_ days.

*-s*, *--sort* _MODE_
	Sort order. One of: *name* (default, directories first), *size-desc*,
	*size-asc*, *count-desc*, *count-asc*, *age-desc* (oldest first),
	*age-asc* (newest first).

*-h*, *--help*
	Print help information.

*-V*, *--version*
	Print version information.

# KEYBINDINGS

## Navigation

*↑*/*k*
	Move selection up.

*↓*/*j*
	Move selection down.

*→*/*Enter*/*Space*
	Enter directory.

*←*/*Backspace*
	Go up / back.

*g*
	Jump to first entry.

*G*
	Jump to last entry.

*t*
	Toggle between list and tree view modes.

*q*/*Esc*
	Quit.

## Sorting

*s*
	Open sort selector. Use *↑↓*/*jk* to choose, *Enter* to confirm,
	*Esc* to cancel.

## Filtering (interactive)

*/*
	Set path pattern filter (regex).

*o*
	Set older-than filter (days).

*n*
	Set newer-than filter (days).

*>*
	Set minimum size filter.

*<*
	Set maximum size filter.

*c*
	Clear all filters.

## Tree View Preview

*Tab*
	Toggle focus to the file preview pane.

*j*/*k* (when preview focused)
	Scroll preview content.

*Esc* (when preview focused)
	Return focus to the directory columns.

# ENVIRONMENT

*XDU_INDEX*
	Default index directory if *-i* is not specified.

# EXAMPLES

Browse all partitions:

	xdu-view -i /index/scratch

Start in a specific partition:

	xdu-view -i /index/scratch -u alice

View only stale files, sorted by size:

	xdu-view -i /index/scratch --older-than 30 -s size-desc

View large Python files:

	xdu-view -i /index/scratch -p '\\.py$' --min-size 1M

# SEE ALSO

*xdu*(1), *xdu-find*(1), *xdu-rm*(1)
