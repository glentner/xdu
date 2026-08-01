# 04 — Architecture & Testability (crawl hardening)

**Scope:** make the index-build crawl cleanly factored and unit-testable by lifting pure/near-pure
logic out of `src/bin/xdu.rs` into the `xdu` library, replace the fake `tests/crawl_tests.rs` with
real-binary integration tests, and take a **bounded** low-risk cleanup pass across the readers. This
is a **refactor, not a redesign** — every seam below must preserve observable behavior and keep
invariant #7 (single shared rayon pool + driver threads + work-stealing + `thread::scope` error
propagation) and invariant #1 (schema stability) untouched.

Suggested home for extracted crawl logic: a new `pub mod crawl` in `lib.rs` (keeps `lib.rs` from
ballooning; readers don't need it).

## (a) Extraction map — `xdu.rs` → `lib`

| Extract (current location) | → lib name | Pure core vs I/O shell | Unlocks test |
|---|---|---|---|
| Work-queue build (`crawl()` lines ~197–245: classify dir/root-file, apply `partition_filter`, sort, push `__root__` first, empty-check) | `crawl::build_work_queue(entries: Vec<(name,is_dir,is_loosefile)>, top_dir, filter) -> Result<Vec<WorkItem>>` | **Pure** decision/ordering. I/O shell = `fs::read_dir` stays in bin and feeds `(name, is_dir, is_file||is_symlink)` tuples in. | `__root__` inserted **first** iff a loose top-level file/symlink exists; dirs sorted asc; filter excludes non-listed; empty tree → `bail`; a root of only symlinks still triggers `__root__` (preserve quirk). |
| `WorkItem` struct (fields `path/partition/max_depth`) | `crawl::WorkItem` (pub fields) | Pure data | assert `__root__` gets `max_depth==Some(1)`, partitions `None`. |
| Partition-filter validation (`main` lines ~559–571) | fold into `build_work_queue` as a "requested-but-absent" check, or `crawl::validate_partition_filter(existing:&HashSet, requested:&HashSet)` | Pure set diff; `is_dir` I/O supplies `existing`. | requested partition absent → error; also a natural **`__root__`-collision guard** (a real subdir literally named `__root__`). |
| `PartitionBuffer` whole struct (`add`/`flush`/`finalize`, records→Arrow→Parquet, rename+prune) | `crawl::PartitionBuffer` (move as-is) | Already self-contained; operates over a real dir. `finalize()`'s rename→prune is the high-value seam. | **finalize against a temp dir**: N `.partial` → N `.parquet`; stale chunk `NNNNNN+` pruned; prune loop stops at first gap; `pruned` count exact. **flush** writes a readable Parquet with correct row count + schema. **add** flushes at `buffsize`. |
| chunk-id / path formatting (`format!("{:06}.parquet.partial", id)`, `.with_extension("")`) | `crawl::chunk_partial_name(id)` / `chunk_final_name(id)` | Pure strings | zero-pad width; suffix round-trip (`.partial` strip → `.parquet`). |
| Per-record build from `Metadata` (lines ~365–379) | `crawl::record_from_metadata(path, &Metadata, SizeMode) -> FileRecord` | Pure given `(blocks*512, len, atime)`; `SizeMode::calculate` already tested. Thin `MetadataExt` read stays. | disk-usage vs apparent vs block-rounded wiring; atime passthrough. |
| Stats aggregation (`CrawlStats`) | `crawl::CrawlStats` + `merge`/fold of per-partition `(files,bytes,pruned)` | Pure add | summed partials == totals. |

## (b) Thin-orchestrator sketch for `crawl()`

`crawl()` stays in the bin as an **orchestrator** and shrinks to wiring:

1. `let queue = crawl::build_work_queue(read_dir_tuples(top_dir)?, top_dir, filter)?;` — pure, tested.
2. Build pool + spawn `num_drivers` in `thread::scope` — **unchanged**; invariant #7 intact.
3. Driver loop: `let mut buf = crawl::PartitionBuffer::new(...)`; per entry
   `buf.add(crawl::record_from_metadata(path, &meta, size_mode)?)?`; then `buf.flush()?;
   let pruned = buf.finalize()?;`.
4. Aggregate atomics → `crawl::CrawlStats`.

**Leave in the bin (the I/O/UI shell, do not test):** all progress-bar / speed-window / `MultiProgress`
churn (lines ~346–453, 460–483), the pool build, the `Mutex<VecDeque>` queue, and the first-error
propagation in `thread::scope`. These are inherently threaded/terminal and carry no logic worth
unit-testing; the concurrency scaffold must stay byte-identical.

**Behavior-change risks to guard:** (1) work-queue **ordering** — `__root__` first, then partitions
sorted asc — a reorder is observable in "Finished …" lines; (2) the `__root__` trigger predicate
(`is_file()||is_symlink()`) and its `max_depth(1)`; (3) finalize's prune-until-first-gap semantics.
Pin all three with tests before/after the move.

## (c) Replacement crawler tests (drive the real `xdu` binary)

Delete `tests/crawl_tests.rs` — it reimplements the walker with `Parallelism::Serial` + a `TestBuffer`
and **never calls production `crawl`/`finalize`**, so green proves nothing. Replace with real-binary
tests mirroring `rm_tests.rs` (`std::process::Command` + `tempfile`), asserting counts via
`xdu-find --count`. Factor the shared `binary_path`/`build_index`/`create_test_file` into
`tests/common/mod.rs` (rm_tests + the new file both use it).

Cases:
1. **Basic counts** — N files / 2 partitions → `xdu-find --count`==N; `-u alice --count` correct.
2. **`__root__`** — loose top-level file → `index/__root__/` exists; `-u __root__ --count`==#loose;
   nested files **not** in `__root__`.
3. **Nested depth** — deeply nested file counted; partition == top-level name (via `-u`).
4. **Partition filter** — `xdu --partition alice,bob` indexes only those; `-u charlie --count`==0.
5. **Filter validation** — `--partition nope` → non-zero exit + stderr message.
6. **Size modes** — default disk-usage vs `--apparent-size` vs `--block-size 4K` give expected totals
   (`-f size`/csv).
7. **Empty tree** — non-zero exit, "No partitions found".
8. **Re-index prune** — index a large tree, re-index a smaller one into the same outdir; count drops,
   no stale rows (exercises `finalize` prune through a real run).
9. **Atomicity** — after success, **no `*.partial`** remain under the index (glob check).
10. **Symlinks excluded** — regular file + symlink to it → count==1.
11. **Determinism** — two runs over the same tree → identical `--count` and identical parquet file set.
12. **Buffsize chunking** — `-B 2` over 5 files in one partition → multiple `NNNNNN.parquet`; count==5.

## (d) Wider cleanups (bounded — R8)

**Do now (small, safe, byte-identical, no schema/CLI change):**
- Extract `lib::index_glob(index: &Path, partition: Option<&str>) -> String` and replace the
  duplicated `format!("{}/{}/*.parquet",…)` / `"{}/*/*.parquet"` sites in **xdu-find, xdu-rm, and the
  ~10 inline ones in xdu-view**. Pure dedup, unit-testable, and creates the **single seam** for the
  invariant #5 escaping later.
- Add `tests/common/mod.rs` for the shared integration helpers (removes duplication with rm_tests).

**Follow-up (larger/riskier — record, don't do in this pass):**
- **Centralize the DuckDB injection surface (invariant #5):** route partition name + index path through
  one validated/escaped helper built on `index_glob`. The escaping *changes output* for exotic names, so
  it needs its own review — keep separate from the pure dedup above.
- **Reconcile `xdu-view::format_file_count` vs `lib::format_count`** — near-duplicates but not equal
  (`format_file_count` appends " files"/" file" and pluralizes, takes `i64`). Unifying changes TUI
  strings → **not** behavior-preserving; needs a deliberate call.
- **Lift pure TUI helpers to `lib`** for testability (invariant #11/#12): `detect_file_type`,
  `describe_{elf,macho,shebang,text_by_extension}`, `strip_ansi`. High test value (esp. `strip_ansi`),
  but a sizeable move in a 2487-line file — schedule on its own.

None of the above touches `get_schema`/`FileRecord`/reader column lists (invariant #1) or CLI
semantics (#10). This is altitude-only work.
