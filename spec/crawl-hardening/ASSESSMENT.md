# Wider architecture assessment — what was applied, what was deferred

> The R8 deliverable. The analysis behind it is
> [`research/04-architecture.md`](research/04-architecture.md) (the readers, `lib`, and `cli` relative
> to the indexer); this record states what that assessment led to — the cleanups applied in this pass,
> and the items deliberately left, each with the reason it was not safe to do here.

## Applied this pass

| Change | Why it was safe to do now |
|--------|---------------------------|
| `lib::index_glob(index, partition)` replaces the eight hand-built `read_parquet` globs across `xdu-find` (1), `xdu-rm` (1), and `xdu-view` (6) | Pure string construction, byte-identical output, unit-tested. The index layout is now expressed once. |
| `ROOT_PARTITION` and `COMPLETION_MARKER` moved to `lib` as index-layout constants | `xdu-view` had its own `const ROOT_PARTITION = "__root__"`. Two copies of a name that defines the on-disk layout can drift; one cannot. `crawl` re-exports them, so crawl-side code is unchanged. |
| `lib::index_completion_warning` + wiring into all three readers | The crawler gained a run-level completion marker in this feature; without a reader that notices, an index from an interrupted run looks the same as a finished one. Soft stderr warning, never a refusal. |
| `tests/common/mod.rs` shared integration helpers | Removed the duplication between `crawl_tests.rs` and `rm_tests.rs`. (Landed with P1.) |

Nothing above touches `get_schema()`, `FileRecord`, any reader's column list, or CLI semantics —
verified against the diff. This is altitude-only work.

## Deferred, with reasons

**These are recorded, not scheduled.** Each is a real item; none was safe to fold into this pass.

### `xdu`, `xdu-find`, `xdu-view`, `xdu-rm` all reject `--version`

Found while building the benchmark harness, which wanted to record the version of the binary it was
measuring. Every one of the four `doc/*.scd` man pages documents `-V, --version`, and `AGENTS.md`
states the version is single-sourced from `Cargo.toml` via clap — but no argument struct in
`src/cli.rs` sets `version` in its `#[command(...)]` attribute, so clap never registers the flag and
every binary errors on it.

This is a **defect, not a feature**: documented surface that does not exist, and a violation of the
CLI-single-source invariant. The fix is one attribute per struct. It was left out of this feature
because a CLI change carries the same-commit man-page rule and belongs nowhere near a crawl-hardening
or benchmark commit. It wants a small `fix/` branch of its own.

### Centralize the DuckDB injection surface on `index_glob`

Every user-supplied index path and partition name reaches `read_parquet(...)` and `WHERE` clauses by
string interpolation. `index_glob` now gives that a single seam — which was much of the point of
extracting it — but adding escaping or bound parameters *changes the emitted SQL* for exotic names,
so it is a behavior change needing its own tests and review rather than a rider on a pure dedup.

### Reconcile `xdu-view::format_file_count` with `lib::format_count`

Near-duplicates, but not equal: the TUI version takes `i64` and appends a pluralized `" file"`/
`" files"`. Unifying them changes strings the TUI renders, so it is not behavior-preserving and needs
a deliberate call about what the TUI should say.

### Lift the pure TUI helpers into `lib`

`detect_file_type`, `describe_{elf,macho,shebang,text_by_extension}`, and especially `strip_ansi` are
pure functions with high test value sitting untested inside a 2,500-line binary — `strip_ansi` is
load-bearing for terminal safety (invariant §12). Worth doing; it is a sizeable move through a large
file and deserves its own change rather than a tail-end edit here.

### Decouple pool width from driver count, and pipeline the Parquet write

Surfaced by the P5 measurements rather than by the static assessment, and written up in
[`bench/scenarios.md`](../../bench/scenarios.md) alongside the levers that were evaluated and
rejected. Both are performance work needing their own measured pass — ideally against numbers from
[`bench/HPC-PROTOCOL.md`](../../bench/HPC-PROTOCOL.md), since a warm local filesystem cannot show the
metadata-latency effects they target.
