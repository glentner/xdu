# PLAN — Richer index schema: owner, group, permissions, mtime, ctime

> **Status:** Draft for review · **Last updated:** 2026-09-08
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). Backing detail is in
> [`research/`](research/) (appetite big; four briefs plus digest).

## 1. Summary

The eight-column schema lands in one breaking change behind format version 2. The
crawler reads uid, gid, masked mode, mtime, and ctime from the `Metadata` it already
holds — no second stat — and the version bump alone drives refusal of format-1
indexes through the existing gate. `xdu-find` gains five long-only flags whose values
are resolved or parsed in Rust before SQL is built, so only integers reach DuckDB;
csv and json grow the same five fields while every other output stays byte-identical.
Four vertical phases: schema plus crawler, lib filter core, find surface, pins plus
docs plus gate.

## 2. Design

**Writer (`src/lib.rs`, `src/crawl.rs`, `src/bin/xdu.rs`).**
`get_schema()` appends five non-null `Int64` fields after `atime`: `uid`, `gid`,
`mode`, `mtime`, `ctime`. `INDEX_FORMAT_VERSION` becomes 2 and its doc comment is
rewritten to name the eight-column layout in the same commit; the marker writer
interpolates the constant, so R2 follows without further edits. The crawler's
`file_size_and_atime` grows into one measurement helper returning all seven integers
from the borrowed `Metadata` (`uid()`, `gid()`, `mode() & 0o7777`, `mtime()`,
`ctime()` alongside size and atime), still `Copy`, still unpacked into
`PartitionBuffer::add` — no intermediate row struct. `PartitionBuffer` gains five
`Int64Builder`s; the 8-tuple this implies becomes a private builders struct rather
than a wider tuple. `flush` extends the `RecordBatch` column vector in schema order;
`.partial` → rename, stale-chunk prune, and `finalize` are schema-agnostic and do
not move. Masking happens once in the crawler: the stored `mode` is permission bits
(`0o7777`), so SQL and csv/json read the column bare
([digest C1](research/00-digest.md)).

**Reader gate (no code change).** `index_version_error` already accepts exactly
`INDEX_FORMAT_VERSION` and every bin already calls it pre-query, so the bump is the
whole of the R3 mechanism: a format-1 marker takes the mismatch arm — non-zero
exit, no rows, no deletions, diagnostic naming found 1 against supported 2. The
missing piece is a pin, not logic: a `version_tests` case planting `format=1` on a
fresh index, which P4 adds because the suite would stay green if a regression
re-accepted it.

**Filter core (`src/lib.rs`).** `QueryFilters` gains `owner_uid: Option<u32>`,
`group_gid: Option<u32>`, a mode predicate (operator enum exact/any/all plus
`u32` bits), and `mtime_older_than` / `mtime_newer_than: Option<i64>` epochs.
Builders take already-resolved values: `with_owner_uid(u32)`,
`with_mtime_older_than(days: Option<u64>)` reusing the `now - days*86400`
conversion, and a parsed mode SPEC. `to_conditions` emits `uid = N`, `gid = N`,
`mode = N` / `(mode & N) != 0` / `(mode & N) = N`, `mtime < T` / `mtime >= T` —
numbers only, the same interpolation the size and atime arms use. `is_active`,
`clear`, and `format_display` enumerate the new fields so a later caller cannot
leak them; the TUI sets none of them this cycle.

**Name resolution and mode parsing (`src/lib.rs`, `libc`).** `libc` moves from
dev-dependencies to `[dependencies]`; no new crates. Two helpers resolve at
filter-build time in the bin: `getpwnam` first, decimal-`u32` fallback on miss
(POSIX `find -user`); overflow, minus-prefixed, and mixed tokens fail. The helper
copies the id off the returned pointer immediately, resolves once on the main
thread, and is documented as not pool-safe. The mode parser accepts bare octal
(`644`, optional `0o` prefix) as exact, `/OCTAL` as any-bit, `&OCTAL` as all-bit;
anything else — bad digits 8/9, empty SPEC, unknown prefix — fails before a query
is built. Unresolvable owner/group (R5) and unparsable mode fail in the find bin
with a stderr diagnostic and a non-zero exit before any `read_parquet`.

**CLI (`src/cli.rs`, `src/bin/xdu-find.rs`).** Five long-only flags on
`XduFindArgs`, no shorts (`-o` and `-p` are taken; `-u` is partition):
`--owner NAME`, `--group NAME`, `--mode SPEC`, `--mtime-older-than DAYS`,
`--mtime-newer-than DAYS`. "Changed" appears in no flag name — Unix "changed" is
ctime, whose query flags are a non-goal. The bin stays thin: parse args, resolve
names, build filters, run the query. View and rm structs are untouched, so their
help, completions, and `--safe` semantics cannot drift; the `xdu-rm.1.scd`
sentence claiming identical filter options is rewritten in the same commit.

**csv/json (`src/bin/xdu-find.rs` only).** Both arms extend their SELECT to
`path, size, atime, uid, gid, mode, mtime, ctime` and read the new integers from
`row.get(3..=7)`. The csv header becomes
`path,size,atime,uid,gid,mode,mtime,ctime`; json objects gain the same five keys
after `atime`. Path, size, atime, count, and top arms are untouched.

**Docs.** `doc/xdu-find.1.scd` documents the five flags (same commit as the clap
change); `doc/xdu.1.scd` column list and the `README.md` schema table plus
DuckDB example move from three columns to eight; `AGENTS.md` and
`invariants.md` §1 name the eight non-null fields and version 2. CI asserts no
new literal in the three-column block, so no workflow edit.

**Tests.** Lib unit tests: extended `test_schema_fields` (eight names, types,
nullability), builder round-trip across a chunk boundary over all eight columns,
mode-SPEC parse matrix (exact/any/all, rejects), fragment pins
(`uid = 1000`, `(mode & 2) != 0`, `mtime < …`), NSS policy (current user resolves
to `geteuid`, unknown name errors, digit fallback, overflow fails). Integration:
filter behavior against a fixture with known owners/modes/mtimes (R4–R7),
unresolvable owner exits non-zero with empty stdout (R5), csv header plus json
keys (R8), `format=1` refusal (R3), fresh-index full-surface run with no version
diagnostic (R9).

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1 | Eight-column `get_schema()`; measurement helper over the held `Metadata`; five new builders; masked `mode` |
| R2 | `INDEX_FORMAT_VERSION = 2` interpolated into the marker body by the existing writer |
| R3 | Existing `index_version_error` mismatch arm (mechanism); new `format=1` refusal pin |
| R4 | `--owner` / `--group` → POSIX resolution → `uid = N` / `gid = N` |
| R5 | Bin-side failure before `read_parquet` on unresolvable names; parser rejects on bad mode SPEC |
| R6 | `--mode SPEC` exact/any/all parsed in Rust; numeric predicate in `to_conditions` |
| R7 | `--mtime-older-than` / `--mtime-newer-than` DAYS reusing the atime conversion against `mtime` |
| R8 | csv/json SELECT, header, keys, and `row.get` growth in `xdu-find` only |
| R9 | Named projection everywhere else: path/size/atime/count/top, view, rm work unchanged on v2 |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against
[`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) (§1–§13)
**before** research and **again** after this design was drafted.

- §1 schema stability — honored: this is the versioned breaking change the
  invariant prescribes. Schema, bump, crawler, readers, and README examples move
  together; `AGENTS.md`/`invariants.md` §1 text moves with them.
- §2/§2b atomicity and marker — honored: nothing in the write path moves except
  column count; the version rides the existing clear-after-pre-flight /
  write-on-success ordering. The `--partition`-scoped marker limitation is neither
  fixed nor widened.
- §2c fail-loud — honored: measurement failures keep their current
  Vanished/Hard classification; resolution and parse failures are fail-loud by
  construction (non-zero before any query).
- §4 rm safety — honored by avoidance: rm's query, flags, and `--safe` are
  untouched, and the man-page sentence that would otherwise go stale is fixed in
  the flag commit.
- §5 injection — honored: names and octal strings never reach SQL; only `u32`/`i64`
  interpolate, the same numeric safety size and atime rely on. `index_glob` raw
  interpolation is a recorded gap and stays out of scope.
- §6 Unix-only — honored: `MetadataExt` uid/gid/mode/mtime/ctime and NSS are
  Unix APIs; no portability path is added.
- §7 concurrency — honored: the crawl scaffold does not move; resolution happens
  once on the main thread, never in the pool.
- §8 symlinks — honored: file selection does not move.
- §10/§13 CLI single source — honored: flags on `XduFindArgs` only, both affected
  `.scd` pages in the same commit, clap-typed values, no spec ids in source.
- §11 altitude — honored: parsing, resolution, and SQL-building live in `lib`
  with unit tests; bins stay thin. `libc` promotion is a dependency move, not a
  helper duplication.
- §3 partitions, §9 sort, §12 TUI safety — untouched by design.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| — | — | — |

## 4. Rabbit holes (resolved)

- **What adding columns really touches** → named projection everywhere means
  readers survive extra columns; only csv/json arms grow; the bump alone refuses
  v1 ([research/01-schema-crawler-versioning.md](research/01-schema-crawler-versioning.md),
  [research/02-readers-output.md](research/02-readers-output.md)).
- **Where new flags live without widening rm/view** → per-binary structs stay;
  find-only fields; rm man-page sentence rewritten
  ([research/03-queryfilters-cli.md](research/03-queryfilters-cli.md)).
- **NSS policy, permission grammar, mtime dialect** → POSIX lookup-first with
  digit fallback; `--mode SPEC` exact/any/all; DAYS pair mirroring atime —
  with C1–C4 rulings in [research/00-digest.md](research/00-digest.md)
  ([research/04-nss-perm-mtime.md](research/04-nss-perm-mtime.md)).

## 5. Risks & open questions

- **HPC login nodes without passwd entries.** Uids the index recorded may not
  resolve on the querying host — but resolution runs forward (name → uid), never
  reverse, so a missing entry only affects the filter the operator typed, and the
  digit fallback still selects by raw uid. No mitigation beyond documenting it in
  the man page.
- **Fixture ownership in integration tests.** A test asserting `--owner` needs
  files owned by a known user; the suite may run as root (where `chown` to
  `nobody` works) or as a user (where only the current uid is available). Tests
  must derive expectations from `geteuid`/`getpwuid` at runtime, not hardcode
  `alice`, and self-skip the multi-user case where `chown` is unavailable —
  naming the skip with `--nocapture` per the suite's convention.
- **Duplicate-literal man-page gate.** `doc/xdu-find.1.scd` gains five flags;
  re-count asserted literals on touched pages per §13 before committing.

## 6. Verification strategy

Each phase drives the real binaries in a throwaway index
(`.agents/factory/bin/temp_index.sh sh -c "…"`) and asserts post-conditions, not
exit 0: row counts per filter, non-zero plus empty stdout for unresolvable names,
csv header bytes, json keys, and refusal diagnostics. Lib phases pin SQL
fragments and the parse matrix with `cargo test --lib`. The last phase runs the
full pre-release mirror (`fmt --check`, `clippy -D warnings`, `cargo test`) and
renders every touched `.scd`.

---

*Backing research (if present): [`research/00-digest.md`](research/00-digest.md).*
