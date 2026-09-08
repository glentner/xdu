# QueryFilters, CLI sharing, and find-only flags

How today's filters become SQL, why the three query CLIs are copies rather than a
shared clap struct, and where owner/group/mode/mtime flags go without fighting the
CLI-single-source invariant or widening `xdu-rm --safe`.

## How `QueryFilters` becomes SQL

`src/lib.rs::QueryFilters` holds five query predicates plus a display alias:

- `pattern: Option<String>` — a regex. A glob is translated by `with_path_pattern`
  before it is stored, so the SQL builder never learns a second dialect.
- `pattern_display: Option<String>` — the glob the user typed, for TUI chrome only.
- `min_size` / `max_size: Option<i64>` — bytes, parsed from the human string by
  `parse_size` inside `with_min_size` / `with_max_size`.
- `older_than` / `newer_than: Option<i64>` — Unix epoch seconds. `with_older_than`
  and `with_newer_than` convert clap's `DAYS: u64` at builder time
  (`now - days * 86400`). SQL never sees a day count.

`to_conditions` emits one fragment per set field, joined with `AND` by
`to_where_clause`. `to_full_where_clause` prefixes `WHERE` or returns empty.

Interpolation is two kinds:

- **String.** The pattern is the only `WHERE` operand that is text. It is escaped
  by doubling single quotes (`'` → `''`) and dropped into
  `regexp_matches(path, '{}')`.
- **Numeric.** Size and atime thresholds are `i64` formatted with `{}`:
  `size >= 1048576`, `atime < 1710000000`. Injection-safe by type.

There are no bound parameters. Every reader prepares a fully interpolated string
and calls `stmt.query([])`. `index_glob` interpolates the index path and `-u`
partition name raw into `read_parquet('{}')`; that is a recorded invariant-5 gap
and is not this feature.

`is_active`, `clear`, and `format_display` enumerate the same five predicates.
`xdu-view` calls all three (status line, SQL branch, interactive `clear()`), and
mutates `filters.pattern` directly from the TUI. New fields must join those three
methods or a later caller leaks them.

Unit tests already pin empty / pattern / size / combined / full-WHERE fragments.
That is the pattern new predicates copy.

## CLI structs are per-binary copies

`src/cli.rs` defines four independent `clap::Parser` structs. Filter flags are
duplicated by hand across `XduFindArgs`, `XduViewArgs`, and `XduRmArgs`. There is
no `#[command(flatten)]` and no shared filter type. `gen-completions` calls
`CommandFactory::command()` on each struct separately.

Adding `--owner` to a new flattened struct would make `xdu-view` and `xdu-rm`
inherit it automatically. Completions and `--help` would advertise it on both.
That is the wrong default for this cycle: R4–R7 scope the new filters to
`xdu-find`; TUI columns and `xdu-rm --safe` expansion are non-goals. The
CLI-single-source invariant requires that `cli.rs` be the one definition
completions and `doc/*.scd` describe — it does not require one Flatten shared by
the query tools. Per-binary structs already exist. Find-only fields on
`XduFindArgs`, documented only in `doc/xdu-find.1.scd`, satisfy both the GOAL
scope and the invariant.

`doc/xdu-rm.1.scd` currently claims `xdu-rm` "uses the same filter options as
`xdu-find`". That sentence becomes false the moment find grows flags rm does not
carry, and it must be rewritten in the same commit as the clap change.

## Flag names

`-p` is `--pattern` on every query tool and `--partition` on `xdu`. `-u` is
partition on the query tools. `-o` is `--outdir` on `xdu`; giving find `-o` for
owner repeats that short-flag split. `--older-than` / `--newer-than` are atime
DAYS; reusing them for mtime would silently change every existing invocation.

Long-only names, no shorts:

| Flag | Value | Predicate |
|------|-------|-----------|
| `--owner` | user name (or decimal uid) | `uid = N` |
| `--group` | group name (or decimal gid) | `gid = N` |
| `--mode` | octal SPEC (below) | exact or mask on permission bits |
| `--mtime-older-than` | DAYS | `mtime < now - DAYS*86400` |
| `--mtime-newer-than` | DAYS | `mtime >= now - DAYS*86400` |

Never "changed" in a flag: Unix "changed" is ctime, and ctime query flags are a
non-goal. The mtime pair mirrors the atime pair's conversion and comparison
operators so R7 is the analogue, not a new time model.

## Permission matching (R6)

Exact equality cannot select an exposure class. World-writable is "bit 002 is
set", not "mode is 0002". Mask matching is the administrator question; exact
matching is the secondary form.

One flag, `--mode SPEC`, parsed in Rust before any SQL. Grammar:

- Bare octal (`644`, `0644`) — exact: `mode = n` (permission bits, 07777).
- `/OCTAL` (`/002`) — any-bit: `(mode & n) != 0`. `--mode /002` is
  world-writable. This is GNU find's `/` form.
- `&OCTAL` (`&4000`) — all-bit: `(mode & n) = n`. `--mode &4000` is setuid.

GNU find's all-bit prefix is a leading hyphen (`-002`). Clap treats a token
starting with `-` as a flag, so that form is rejected rather than supported with
`allow_hyphen_values` (which would also swallow a following `--owner`). `&`
replaces find's `-` so the value is clap-safe. Invalid octal, empty SPEC, or an
unknown prefix fails before a query is built — same shape as an unresolvable
owner (R5), no rows.

The user string never reaches SQL. The parser yields an operator and an `i64`;
`to_conditions` formats those.

## `xdu-rm --safe`

`--safe` re-stats each path and re-checks two criteria only: `--older-than`
against current `atime()`, and `--max-size` against current `len()`. Tests cover
those two. Documented gaps already: `--min-size`, `--newer-than`, `--pattern`.
Expanding `--safe` to uid/gid/mode/mtime is a non-goal. If rm inherited the new
flags, the selection set would filter on them from the index while unlink-time
verification still ignored them — the gap widens, and `xdu-rm.1.scd`'s "re-stat
to confirm atime and size" would under-describe the new predicates.

Keep the new clap fields off `XduRmArgs` and `XduViewArgs`. `QueryFilters` may
still grow the fields so SQL lives in one place; view and rm never set them.

## Injection and altitude

The owner *name* is not a `QueryFilters` field and is not a SQL operand. Resolve
at query time with `getpwnam` / `getgrnam` (`libc` is already a crate
dependency), store `Option<i64>` uid/gid, emit `uid = 1000`. An all-digit
argument is a literal uid/gid and skips NSS — find(1) does this, and HPC login
nodes often lack the passwd entry for a uid the index recorded. A non-digit that
does not resolve exits non-zero with no rows (R5). Same for group.

Mode SPEC parsing, DAYS→epoch, SQL fragment construction, and the digit-vs-name
split belong in `lib` with unit tests that pin the fragments (`uid = 1000`,
`(mode & 2) != 0`, `mtime < …`). The find binary stays thin: parse `XduFindArgs`,
resolve names, build `QueryFilters`, run the query. View's `clear` / `is_active`
/ `format_display` gain the new fields so they cannot leak if a later caller
sets them; the TUI does not grow filter chrome for them this cycle.

## CLI recommendation

- **Flags (find only, long only):** `--owner NAME`, `--group NAME`, `--mode SPEC`
  (`644` exact, `/002` any-bit, `&4000` all-bit), `--mtime-older-than DAYS`,
  `--mtime-newer-than DAYS`.
- **Clap:** new fields on `XduFindArgs` only. Do not introduce a flattened shared
  filter struct. Update `doc/xdu-find.1.scd` in the same commit; rewrite the
  "same filter options as xdu-find" sentence in `doc/xdu-rm.1.scd`.
- **Types reaching SQL:** `i64` uid, gid, mode bits, mtime epoch. No names, no
  octal strings.
- **Resolution:** `lib` helpers over `getpwnam`/`getgrnam`; all-digit skip NSS;
  find bin maps `Err` to a non-zero exit before `read_parquet`.
- **`QueryFilters`:** add `owner_uid`, `group_gid`, a small mode predicate
  (exact / any / all + bits), `mtime_older_than`, `mtime_newer_than`. Builders
  take already-resolved integers (and a parsed mode SPEC). `to_conditions`
  interpolates them as numbers, the same way size and atime already work.
