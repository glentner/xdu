# Schema, crawler, and format versioning

Maps to GOAL R1 (columns recorded from the existing `stat`) and R2 (marker format
distinct from 1). Readers, filters, and csv/json emission are out of this brief.

## `get_schema()` is the one Arrow contract

`src/lib.rs::get_schema` returns an `Arc<Schema>` of exactly three non-null fields, in
this order:

- `path`: `DataType::Utf8`, nullable `false`
- `size`: `DataType::Int64`, nullable `false`
- `atime`: `DataType::Int64`, nullable `false`

No other schema constructor exists. Readers never call it; they `SELECT` by column name
through DuckDB. The only unit pin is `test_schema_fields`, which asserts `fields().len()
== 3` and those three names. It does not pin `DataType` or nullability — extend it to
both when the tuple grows.

## Version 1 names the three-column layout

`INDEX_FORMAT_VERSION: u32 = 1` sits beside `COMPLETION_MARKER`. Its doc comment is the
src-side pin of the three-column shape: "1 names the current layout: three-column
`get_schema()` rows". `crawl.rs` adds a second: `file_size_and_atime` is "the two index
columns derived from a file's metadata" (size and atime; path is the UTF-8 conversion
beside them). Both comments go stale the moment columns are added.

The writer interpolates the constant. `completion_marker_contents` ends
`format={INDEX_FORMAT_VERSION}\n`. There is no hardcoded `format=1` in production code.
`index_version_error` accepts exactly `INDEX_FORMAT_VERSION` and refuses every other
value, including a missing key.

Bumping the constant to 2 is therefore sufficient for R2. Tests that follow the constant
move with it: `test_completion_marker_errors_reads_the_writers_body` (writer body parses
as `Some(INDEX_FORMAT_VERSION)`), `test_completion_marker_lifecycle` (`format={}` of the
constant), and `test_index_version_error` (mismatch diagnostic names the constant; a
writer-emitted body is accepted). The parser fixture
`completion_marker_format("format=1") == Some(1)` is a literal, not a pin of the current
version, and stays green. Integration `version_tests.rs` and `crawl_tests.rs` never
assert `format=1`; they drive a fresh index or sabotage with `999` / versionless /
missing.

## One `lstat`, seven integers

The crawl hot path lives in `src/bin/xdu.rs`. The walker is `follow_links(false)`.
Regular files only (`entry.file_type.is_file()`). Then one
`entry.metadata()` — documented as `symlink_metadata`, so a file cannot be swapped for a
symlink between the directory read and this `stat`. `file_size_and_atime` borrows that
`Metadata` and returns `(size, atime)`:

- `metadata.blocks() * 512` and `metadata.len()`, folded through `SizeMode`
- `metadata.atime()` (`MetadataExt`, `i64` Unix epoch seconds)

`uid`, `gid`, `mode`, `mtime`, and `ctime` are the same `MetadataExt` trait on the same
`Metadata`. They do not need a second `stat`. Signatures:

- `uid() -> u32`, `gid() -> u32`, `mode() -> u32` (full `st_mode`)
- `mtime() -> i64`, `ctime() -> i64` (already epoch seconds, matching `atime()`)

Cast the three `u32`s `as i64`. `mtime`/`ctime` are already `i64`. A failed `stat` skips
the row (`Vanished` / `Hard`); nothing is written as null.

There is no `FileRecord`. The bin comment forbids an intermediate row struct: measured
columns append straight into the Arrow builders. Expand `file_size_and_atime` into one
measurement helper that returns all seven integers from the borrowed `Metadata`, still
`Copy`, still unpacked into `PartitionBuffer::add`. A heap row type is the rejected
shape.

The only production call site is `buffer.add(&path_str, file_size, atime)` in
`bin/xdu.rs`. `lossy_path` is independent and stays as it is.

## `PartitionBuffer` must grow in four places

Three builders today: `StringBuilder` (`path`) and two `Int64Builder`s (`size`,
`atime`). `new_builders` returns that 3-tuple. `add(&mut self, path: &str, size: i64,
atime: i64)` appends one value to each, then auto-flushes at `buffsize`. `flush`
`finish`es the three builders into `RecordBatch::try_new` in schema order, then
replaces them with a fresh pre-sized set. Snappy, `.partial` → `rename`, stale-chunk
prune are schema-agnostic.

What must grow: five more `Int64Builder`s; `add` arity; `new_builders` (an 8-tuple is
ugly — a private builders struct is the natural split); the `RecordBatch` `vec!` in
`flush`. `finalize` does not touch columns.

## Recommended schema

Append after `atime` so ordinals 0–2 stay `path`/`size`/`atime`. The round-trip unit
test reads by column index, not name. Readers `SELECT` by name, so physical order does
not bind them.

| name    | `DataType` | nullable |
|---------|------------|----------|
| `path`  | `Utf8`     | false    |
| `size`  | `Int64`    | false    |
| `atime` | `Int64`    | false    |
| `uid`   | `Int64`    | false    |
| `gid`   | `Int64`    | false    |
| `mode`  | `Int64`    | false    |
| `mtime` | `Int64`    | false    |
| `ctime` | `Int64`    | false    |

Names: `uid`/`gid`, not `owner`/`group` — names are not stored. `mode`, not `perms` —
POSIX `st_mode`. Store `metadata.mode() as i64` unmasked. File-type bits are redundant
(the index holds only regular files) but a crawler mask cannot be undone; query filters
mask at read time. `Int64` for all five: it is the house numeric type, and `u32` does
not fit in `Int32`. `UInt32` would be a new Arrow kind in this schema for no gain.
All non-null, like the existing three: `MetadataExt` methods are infallible on a
successful `stat`.

`INDEX_FORMAT_VERSION = 2`. Rewrite the constant's doc comment in the same commit.

Crawler extraction, same `Metadata` already in hand:

```
metadata.uid() as i64
metadata.gid() as i64
metadata.mode() as i64
metadata.mtime()
metadata.ctime()
```

## Tests that go red

Compile-fail until `add` and the measurement helper change arity:

- `test_finalize_renames_partials_and_prunes_stale_tail` (`buf.add(path, 10, 0)`)
- `test_finalize_prune_stops_at_first_gap` (same)
- `test_buffer_writes_every_row_across_chunk_boundaries` (`Vec<(String, i64, i64)>`,
  `column(0/1/2)` only)
- `test_file_size_and_atime_size_modes` if the helper's return type changes

Runtime:

- `test_schema_fields` (`len() == 3`)

Stay green on a version bump plus extra columns, provided readers still `SELECT path`
(they do): `crawl_tests.rs` (counts, `files=N` in the marker, no column inspection),
`version_tests.rs` (fresh index carries whatever the constant is), parser fixtures that
feed `format=1` as an example integer, `QueryFilters` unit tests (still `atime`/`size`
only). csv/json still emit three fields until a later pass; DuckDB `read_parquet` of a
wider schema does not break a narrower `SELECT`.
