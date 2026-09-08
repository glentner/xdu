# Readers, column lists, and csv/json output

Every `read_parquet` site projects named columns or aggregates over named columns. There is no
`SELECT *` in `src/`. Extra Parquet columns are ignored by DuckDB named projection. They break a
reader only if that site's SELECT list grows and the positional `row.get(N)` indices are not
updated with it.

`QueryFilters::to_conditions` names `path` (`regexp_matches`), `size`, and `atime` in WHERE.
`SortMode` orders by query aliases (`total_size`, `latest_atime`, `component`, `partition`), not by
raw extra columns. Those WHERE/ORDER fragments keep working on a v2 file that still carries
path/size/atime.

## 1. Per-binary SELECT lists

**`xdu-find`** (`src/bin/xdu-find.rs`):

| Mode | SQL |
|---|---|
| `--top N` | `regexp_extract(filename, …) as partition, COUNT(*)` from `read_parquet(…, filename=true)` |
| `--count` | `SELECT COUNT(*)` |
| `-f path` (default) | `SELECT path` |
| `-f size` | `SELECT path, size … ORDER BY size DESC` |
| `-f atime` | `SELECT path, atime … ORDER BY atime ASC` |
| `-f csv` / `-f json` | `SELECT path, size, atime` (no ORDER BY unless `--limit`) |

**`xdu-view`** (`src/bin/xdu-view.rs`), list and tree paths share the same projections:

- `__root__` files (`query_root_files`): `SELECT path, size, atime`.
- Partition listing (`load_partitions`, `make_partition_column`): `SUM(size)`, `MAX(atime)`,
  `COUNT(*)` / filtered `SUM(CASE WHEN {filter} …)`, plus `regexp_extract(filename, …)` with
  `filename=true`.
- Shortest-path probe (`discover_partition_root`): `SELECT path … ORDER BY length(path) LIMIT 1`.
- Directory CTE (`load_directory`, `make_directory_column_tree`): inner `SELECT path, size, atime`,
  then `SUM(size)`, `COUNT(*)`, `MAX(atime)` grouped by path component.

`DirEntry` holds `name`, `path`, `is_dir`, `total_size`, `file_count`, `latest_atime`. Result
columns are read by position.

**`xdu-rm`** (`src/bin/xdu-rm.rs`): one query, `SELECT path, size, atime`, plus
`deterministic_limit_clause` (`ORDER BY path LIMIT n` when `--limit` is set).

All three bins call `index_version_error` before any query (view: before the terminal is touched).

## 2. csv/json today, and where R8 lands

csv writes a header line `path,size,atime`. Each row is `path,size,atime` with integers unquoted.
A path containing `,` or `"` is wrapped in quotes with `"` doubled. Newlines in paths are not
escaped. `--limit` is a bare `LIMIT` (no ORDER BY), unlike rm.

json writes a single array, not JSON Lines:

```
[
  {"path":"…","size":N,"atime":N},
  …
]
```

Two-space indent. Path escapes `\`, `"`, `\n`, `\r`, `\t` only. An empty match is `[\n]\n`. Keys
are `path`, `size`, `atime`.

R8 grows **only those two arms**: extend the SELECT list and the header/keys together, reading the
new integers from `row.get(3..)`. Append after `atime` so the existing three stay the first three
columns. Use the schema field names as csv headers and json keys (`uid`, `gid`, `mode`, `mtime`,
`ctime` if that is what `get_schema()` names; permission bits are the mode column). Leave `-f path`
(one path per line), `-f size`, `-f atime`, `--count`, and `--top` untouched.

`src/cli.rs` and `doc/xdu-find.1.scd` name the five format tokens, not the csv/json columns. R8
does not add a flag, so the man page is not required to change for the CLI-in-the-same-commit rule.
A column list on the page is documentation of the new row shape, not a CLI change.

## 3. xdu-view and R9

R9 for the TUI is accept v2 and keep serving path/size/atime. No SELECT list must grow. Extra
columns are unused. TUI columns for the new fields are a non-goal.

## 4. xdu-rm query and `--safe`

The deletion query is the same three-column named SELECT. Extra columns do not change the match
set. `FileInfo` stores `size` and `atime` under `#[allow(dead_code)]`; neither field is read after
the query. Dry-run prints `file.path` only.

`--safe` re-stats with `fs::metadata` and re-applies **CLI** thresholds to **live** metadata, not
to indexed values:

- `--older-than`: skip if `meta.atime() >= now - days*86400`.
- `--max-size`: skip if `meta.len() as i64 > parsed max`.

It does not re-check `--min-size`, `--newer-than`, or `--pattern`, and it does not look at uid,
gid, mode, mtime, or ctime. With neither `--older-than` nor `--max-size`, `--safe` is a no-op
(`test_safe_mode_without_relevant_filter_still_deletes`). The man page's "confirm its access time
and size still match the index" overstates what the code does: the live values are compared to the
CLI bounds, not to the indexed row. Leave `--safe` alone. Do not claim it covers the new columns.

## 5. Docs that pin `path,size,atime`

- `README.md` schema table: three rows, path/size/atime.
- `README.md` DuckDB example `SELECT path, size, atime … WHERE atime < …`. The neighbouring
  `sum(size)` / `regexp_extract(path, …)` examples keep working without a column-list edit.
- `doc/xdu.1.scd`: "Each Parquet file contains three columns:" plus the three bullets.
- `AGENTS.md` and `.agents/factory/invariants.md` §1: three non-null fields, version 1 names that
  layout. Those move with `get_schema()`, not with a reader SELECT.

CI's man-page literal list does not assert the three-column block.

## 6. Tests that parse csv/json or pin three columns / format=1

No integration test parses csv or json. `tests/common/mod.rs::find_paths` drives `-f path`;
`find_count` drives `--count`. `tests/crawl_tests.rs` parses the leading integer of `-f size`
(`<size>\t<path>`).

`tests/version_tests.rs` asserts a fresh index queries with no "format version" on stderr (R9,
once the crawler stamps 2) and that missing / versionless / `format=999` markers refuse with empty
stdout and no deletions. Nothing plants `format=1` and expects refusal.

`src/lib.rs::test_schema_fields` asserts `get_schema()` length 3 and names path/size/atime (writer
contract). `src/crawl.rs::test_buffer_writes_every_row_across_chunk_boundaries` downcasts parquet
columns 0/1/2 as path/size/atime (writer). Neither is a reader SELECT.

## 7. R3 and `index_version_error`

`index_version_error` already implements R3 once `INDEX_FORMAT_VERSION` becomes 2. The accept arm
is `version == INDEX_FORMAT_VERSION`. Readers never hardcode `== 1`. A format-1 marker takes the
mismatch arm: exit non-zero, no query, no rows, no deletions. The diagnostic interpolates the
constant (`supports version 2`).

`completion_marker_format("format=1") == Some(1)` is a parser pin of the digit, not "current
version is 1". `test_index_version_error` asserts the diagnostic contains
`INDEX_FORMAT_VERSION.to_string()`.

The existing suite would stay green if a regression accepted format 1 after the bump. A
`version_tests` case that overwrites a fresh marker with `format=1` and asserts refuse is the
missing R3 pin.

## Recommendation

R3 is the version bump in `src/lib.rs` (`INDEX_FORMAT_VERSION` 1 → 2). All three readers inherit
it. R9 for find path/size/atime/count/top, for view, and for rm is no SELECT-list edit: they
already name path/size/atime. R8 is the csv and json arms of `src/bin/xdu-find.rs` only.

**Must change for R3/R8/R9 (reader side):**

- `src/lib.rs` — bump `INDEX_FORMAT_VERSION` (R3). `get_schema()` is the writer contract, not a
  reader SELECT, but it moves in the same commit as the bump.
- `src/bin/xdu-find.rs` — csv and json SELECT + header/keys + `row.get` only.
- `tests/version_tests.rs` — plant `format=1`, assert refuse (R3). Fresh-index case already covers
  R9 once the writer stamps 2.
- A find integration test that parses csv header/json keys for the five new fields (R8). None
  exists today.
- Docs that pin the three-column row shape: `README.md` schema table and `SELECT path, size,
  atime`; `doc/xdu.1.scd` column list; `AGENTS.md` / `invariants.md` §1.

**Keep working because they SELECT by name:**

- `src/bin/xdu-view.rs` — every query site.
- `src/bin/xdu-rm.rs` — query, dry-run, confirm, `--safe`.
- `QueryFilters` WHERE (until a later slice adds owner/mode/mtime filters).
- find `-f path` / `-f size` / `-f atime` / `--count` / `--top`.
- `tests/common/mod.rs` `find_paths` / `find_count`.
