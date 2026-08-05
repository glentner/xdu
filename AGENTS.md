# AGENTS.md

Guidance for coding agents (Claude Code and others) working in this repository.
`CLAUDE.md` is a symlink to this file — edit `AGENTS.md`, never a separate copy. (Warp reads
`AGENTS.md` directly, so there is no separate `WARP.md`.) `.claude` is likewise a symlink to
`.agents`, so Claude Code discovers the factory skills and settings through it.

This document is the operating manual: the architecture, the load-bearing invariants, and the
process rules an autonomous agent needs to make correct, safe changes here without rediscovering
them each session. When something below disagrees with the code, **the code is ground truth — fix
this file.**

---

## Project

**xdu** is a high-performance filesystem indexer and query suite for large-scale storage
administration (HPC / enterprise: hundreds of millions to billions of files) where `du`(1) and
`find`(1) are too slow for regular auditing. You point it at a directory tree; it builds a
persistent, queryable **Hive-partitioned Parquet index** (partitioned by top-level subdirectory)
once, then answers size/age/pattern questions instantly.

Five binaries (`Cargo.toml [[bin]]`), four user-facing plus one build helper:

| Binary | Role |
|--------|------|
| **`xdu`** | Crawler/indexer — walks a tree, writes the Parquet index (path, size, atime). |
| **`xdu-find`** | Query CLI — DuckDB over the index; filters + `--count`/`--top`/formats. |
| **`xdu-view`** | ncdu-style interactive TUI (ratatui/crossterm); read-only list + tree views. |
| **`xdu-rm`** | **Destructive** bulk deletion of files matching an index query, with `--safe` re-stat. |
| **`gen-completions`** | Dev helper — emits bash+zsh completions from the `src/cli.rs` clap structs. |

The index schema is deliberately minimal: `path` (UTF-8), `size` (INT64 bytes), `atime` (INT64
Unix epoch seconds). It is **Unix-only** (`std::os::unix::fs::MetadataExt` for atime and disk
usage). Snappy compression. Queries use the **bundled** DuckDB (`duckdb` crate, `bundled`
feature) so there is no external DuckDB dependency.

## Environment & working rules

- **Commit only when explicitly asked.** When you do: this is **GitHub Flow on `main`** — branch
  off `main` (`feature/{slug}` or `fix/{slug}`), open a **squash** PR back to `main`. There is no
  `develop` branch; releases are `vX.Y.Z` tags on `main`.
- **Commit subjects follow `[category] Imperative summary`.** Common categories: `feature`, `fix`,
  `docs`, `ci`, `refactor`, `test`, `release` (version bumps / rebuilt assets), and `harness` (the
  `.agents/` factory). This set is **not closed** — coin a new lowercase category when one fits.
- **No `Co-Authored-By:` trailer on commits** — it is noise in `git log`. Authorship/AI-assistance
  is tracked in the **PR body** instead, which ends with the Claude Code generation line.
- **Version is single-sourced from `Cargo.toml`** — `clap` derives `--version` from
  `CARGO_PKG_VERSION`. Never hardcode a version string in `src/`.
- **A CLI change updates its `doc/*.scd` man page source in the same commit.** Shell completions
  regenerate automatically from `src/cli.rs` (`gen-completions`), so they are not committed; the
  generated `share/` tree is git-ignored (built in CI and by `/xdu-release`).

## Commands

Toolchain is pinned by **`rust-toolchain.toml`** (edition 2024; the crate uses let-chains). Do not
assume `nightly` — use the pinned toolchain.

```bash
cargo build --release                                   # all bins (bundled DuckDB C++ is the slow part)
cargo test                                              # lib unit tests + tests/ integration
cargo test --test rm_tests                              # one integration file
cargo clippy --all-targets --all-features -- -D warnings   # lint gate (must be clean)
cargo fmt --all -- --check                              # format gate

# Drive the real binaries against a throwaway index (never a real one):
.agents/factory/bin/temp_index.sh xdu-find --count
.agents/factory/bin/temp_index.sh sh -c 'xdu-rm --dry-run -p "\.log$" --force'

# Man page gate. INSTALL scdoc — without it a doc/*.scd defect reaches CI unseen:
brew install scdoc                     # macOS  (apt-get install -y scdoc on Debian/Ubuntu, as CI does)
for scd in doc/*.scd; do scdoc < "$scd" > /dev/null || echo "FAILED $scd"; done
scdoc < doc/xdu.1.scd | mandoc -Tutf8 | col -b   # read the PUBLISHED text, not just exit 0
```

**Rendering is not optional, and exit 0 is not sufficient.** `scdoc` markup fails in two ways: a
nesting error is loud (`*__root__*` — `_` opens italic inside bold), but a mis-escaped literal is
**silent** — `_OUTDIR_/*/*.parquet` published as `OUTDIR//.parquet` because `*` is bold markup, and a
line *starting* with `.` has that period silently dropped. Escape a literal asterisk `\*` and a
double underscore `\_\_` (mid-word `_` as in `*XDU_INDEX*` is safe); never start a line with `.` or
`'` — rewrap instead. Catching the silent class requires diffing the rendered text against the
literal you intended, which is what the `mandoc | col -b` line above is for.

## Repository map

```
src/
  lib.rs         # shared core: FileRecord, get_schema() (THE index schema), SizeMode, parse_size,
                 # SortMode, QueryFilters (DuckDB WHERE/ORDER BY builders), format_{count,bytes,speed}
  cli.rs         # the SINGLE clap CLI definition (XduArgs/XduFindArgs/XduViewArgs/XduRmArgs) —
                 # gen-completions + man pages describe exactly this
  crawl.rs       # index-build hot path lifted out of the bin so it is unit-testable: work-queue
                 # construction (incl. the __root__ collision rejection), per-file record building,
                 # PartitionBuffer + atomic finalize, completion-marker read/write. Only the crawler
                 # uses it; the reader tools never do. The concurrency scaffold stays in bin/xdu.rs.
  bin/xdu.rs     # indexer: shared-rayon-pool concurrent walk, driver threads, thread::scope error
                 # propagation, progress — the scaffold around lib::crawl
  bin/xdu-find.rs   # DuckDB query CLI
  bin/xdu-view.rs   # 2487-line ratatui/crossterm TUI (read-only)
  bin/xdu-rm.rs     # destructive bulk delete (safe mode, confirm, dry-run, parallel)
  bin/gen-completions.rs   # emits share/ completions from cli.rs
tests/           # crawl_tests.rs, rm_tests.rs (integration)
doc/*.scd        # scdoc man page SOURCES (authored); render to share/man/man1/*.1
share/           # GENERATED (man + completions); git-ignored; built in CI / by /xdu-release
build.sh install.sh cruft.sh Dockerfile   # packaging / ops scripts
.agents/         # the spec-driven "software factory" (see below)
spec/{slug}/     # committed, dated per-feature design records the factory produces (retained on merge)
issues/{slug}.md # deferred code work, pre-shaped (status: unshaped); /xdu-feature promotes one to a GOAL
ROADMAP.md       # forward-looking feature roadmap — prose intentions that seed future /xdu-feature
```

Two repo-level trees sit outside `src/`: **`.agents/`** — the software factory (the
`xdu-feature|plan|build|review|publish` lifecycle skills plus operational siblings `xdu-harness`
(meta/maintenance) and `xdu-release` (version cuts); `factory/` methodology, invariants, EARS,
templates, the non-Claude `portability.md` contract, and the `bin/` FSM scripts + `meta_status.py`);
and **`spec/{slug}/`** — the `GOAL/PLAN/TECH/REVIEW.md` + `META.md` records the factory produces and
**retains on merge**. `AGENTS.md` stays ground truth; `spec/{slug}/` is a point-in-time record of
intent. See `.agents/factory/methodology.md`.

**Where a deferral goes — four homes, one rule each.** A pass that decides not to fix something must
record it, and the destination is not a matter of taste:

| File | Holds | Written by |
|---|---|---|
| `spec/{slug}/META.md` | **Harness/skill feedback only** — "was this the *factory's* fault". Never code follow-ups. | the lifecycle skills |
| `issues/{slug}.md` | **Deferred code work**, pre-shaped from [`templates/ISSUE.md`](.agents/factory/templates/ISSUE.md) with `status: unshaped` | whoever defers it |
| `ROADMAP.md` | the **ordered index** — one entry per issue, `**Seed:**` pointing at the `issues/` file | whoever defers it |
| `spec/{slug}/` | work **actually in flight** | the lifecycle skills |

An `issues/{slug}.md` is a *candidate*, not a contract: `/xdu-feature` promotes it into
`spec/{slug}/GOAL.md`, and that promotion is where appetite, non-goals and R-IDs get negotiated. Never
copy one into a `GOAL.md` verbatim — `xdu-review` grades a GOAL, and an unshaped proposal is not a
contract anyone agreed to.

This coexists with the **GitHub tracker** (`AGENTS.md` already cites issues #2/#3): a GH issue is the
public-facing ticket, `issues/{slug}.md` is the pre-shaped spec behind it, and the two may link to each
other. Neither replaces the other.

## Architecture & data flow

```
xdu ──walk (jwalk + shared rayon pool)──▶ PartitionBuffer ──.partial──▶ fs::rename ──▶ <index>/<part>/NNNNNN.parquet
                                    │                                                         │
                        on success  └──▶ <index>/.xdu-complete  (run-level attestation)        │
                                                     │                                         │
xdu-find / xdu-view / xdu-rm ◀── warn if absent/errors ┘  ◀── DuckDB read_parquet('<index>/<part-or-*>/*.parquet') ◀┘
```

- **Index layout:** `<outdir>/<partition>/NNNNNN.parquet`, where `partition` is a top-level
  subdirectory name. Loose files directly under the indexed root go to the reserved partition
  **`__root__`** (`ROOT_PARTITION`, `lib.rs`), crawled with `max_depth(1)`. Readers glob `*/*.parquet`.
  A real top-level subdirectory *named* `__root__` would collide with that synthetic partition and
  clobber its chunk ids, so `crawl::build_work_queue` **rejects** one instead.
- **Run-level completion marker:** `<outdir>/.xdu-complete` (`COMPLETION_MARKER`, `lib.rs`) — a
  dotfile, so the readers' `*/*.parquet` glob never mistakes it for a partition. Per-chunk
  `.partial`→rename is atomic for one *file* but cannot express whether the **run** finished: when one
  driver fails, partitions that already succeeded stay on disk as real `.parquet` chunks,
  indistinguishable from a complete index. So the marker is **cleared once pre-flight has passed** and
  the crawl is about to write, and **written only on the success path** — its presence attests to the
  whole run. Body is `key=value` lines (`xdu`, `completed_at`, `files`, `bytes`, `vanished`, `errors`,
  `lossy_paths`), so an `--allow-errors` run still records how much it skipped. All three readers call
  `lib::index_completion_warning` and emit a **soft stderr warning** (never a refusal) when the marker
  is absent or records tolerated errors; reads are capped (`MARKER_READ_LIMIT`) and non-blocking, so a
  FIFO or huge file left at that path cannot hang or exhaust a reader. **Known limitation:** the marker
  describes the whole index but its counts come from one run, so a `--partition`-scoped run rewrites it
  from its own stats — see `issues/marker-scoped-run-attestation.md`.
- **Concurrency (indexer):** a **single** rayon pool (`Parallelism::RayonExistingPool`) backs **all**
  jwalk walkers; up to `--jobs` driver `std::thread`s pull partitions from a `Mutex<VecDeque>` work
  queue; rayon work-stealing balances directory reads across all active walkers so one huge partition
  can't starve the rest. `std::thread::scope` joins the drivers; the first `Err`/panic fails the run.
  Thread budget: N pool + C drivers + 1 main.
- **Atomic writes:** each chunk is written to `NNNNNN.parquet.partial`, then `fs::rename`d to
  `.parquet` in `finalize()` (atomic within a dir on POSIX); stale higher-numbered chunks from a prior
  larger run are pruned. This is **per-file** atomic, **not** per-partition atomic — see invariants.
- **Query:** `xdu-find`/`xdu-view`/`xdu-rm` open an in-memory DuckDB, glob the index, and apply a
  `QueryFilters` WHERE clause. `xdu-view` is a read-only TUI; `xdu-rm` unlinks the matched files.

## CLI surface (`src/cli.rs` is the one definition)

- `xdu DIR -o/--outdir DIR [-j/--jobs N (env XDU_JOBS, dflt 4)] [-B/--buffsize N (100000)]
  [--apparent-size] [-k/--block-size SIZE] [-p/--partition NAMES] [--allow-errors]`
  — `--allow-errors` is **opt-in**: by default any walk/stat error fails the run non-zero and no
  completion marker is written; with it, unreadable entries are counted, reported on stderr, the run
  exits 0, and the marker records the tolerated `errors=N`.
- `xdu-find -i/--index DIR (env XDU_INDEX) [-p/--pattern REGEX] [-u/--partition NAME]
  [--min-size/--max-size SIZE] [--older-than/--newer-than DAYS] [-f/--format path|size|atime|csv|json]
  [-l/--limit N] [-c/--count] [--top N]`
- `xdu-view -i/--index DIR [-u/--partition NAME] [-p/--pattern] [--min/max-size] [--older/newer-than]
  [-s/--sort name|size-asc|size-desc|count-asc|count-desc|age-asc|age-desc]`
- `xdu-rm -i/--index DIR [-p/--pattern] [-u/--partition] [--min/max-size] [--older/newer-than]
  [-l/--limit N] [-n/--dry-run] [--safe] [-f/--force] [-v/--verbose] [-j/--jobs (env XDU_JOBS)]`

**Footgun:** `-p` means `--partition` in `xdu` but `--pattern` in the three query tools (where
partition moves to `-u`). Same short flag, different meaning per binary — do not "fix" one side in
isolation.

## Load-bearing invariants (footguns)

The curated, numbered gate is [`.agents/factory/invariants.md`](.agents/factory/invariants.md),
kept **in lockstep** with this section (this file wins if they drift). The `xdu-plan` gate and the
`xdu-review` footgun checklist both draw from it. Summary of what must not silently break:

1. **Parquet schema stability.** `lib.rs::get_schema()` is the ONE contract: exactly three
   **non-null** fields in fixed order — `path: Utf8`, `size: Int64`, `atime: Int64`. Every reader
   selects these by name. There is **no on-disk schema version**, so any change to `get_schema()`,
   `FileRecord`, or a reader's column list is a breaking, cross-cutting index-format change (issues
   #2/#3 — owner/group/perms — are exactly this: add a schema version first).
2. **Atomic finalization.** Write `NNNNNN.parquet.partial` → `fs::rename` (same dir) → prune stale
   higher chunks (`crawl.rs::PartitionBuffer::finalize()`). Never `File::create` a final `.parquet`,
   never cross-dir rename, never let a reader glob `.partial`. **Known limitation:** finalize is
   per-file, not per-partition atomic — do not index a partition while purging it. Run-level
   completeness is a *separate* mechanism: the `.xdu-complete` marker (see Architecture), cleared after
   pre-flight and written only on success. Never write it on a failure path.
3. **Partition scheme.** `<index>/<partition>/NNNNNN.parquet`; `__root__` reserved for loose
   top-level files (depth-1 walk); zero-padded sequential chunk ids; readers glob `*/*.parquet`. A real
   top-level dir named `__root__` is **rejected** by `crawl::build_work_queue` (it would clobber the
   synthetic partition's chunk ids), unconditionally — even when a `--partition` filter would exclude
   it, because the collision is with the layout, not with the selection.
4. **`xdu-rm` destructive safety.** Default requires interactive `y/N` (anything but `y`/`yes`
   aborts); `--dry-run` deletes nothing; `--force` skips the prompt; `--safe` re-stats each file
   immediately before unlink. **Any deletion combined with `--limit` MUST carry a deterministic
   `ORDER BY`** so `--dry-run` and the real run select identical rows.
5. **DuckDB injection surface.** Every user value reaching `read_parquet(...)`/`WHERE` is an
   injection surface. Route it through one validated escaping/quoting helper (or bound parameters);
   never raw `format!` a partition name or index path into SQL.
6. **Unix-only.** `MetadataExt` (atime; disk usage = `st_blocks`×512) is load-bearing; don't add
   code assuming portability. A future S3/Windows source must special-case (S3 has no atime).
7. **Shared rayon-pool concurrency.** See Architecture. Preserve the single-pool + driver-thread +
   work-stealing model and `thread::scope` error propagation.
8. **Symlinks excluded.** `follow_links(false)` + `is_file()` — the index holds only regular files.
9. **`SortMode` age inversion.** `age-desc` = oldest first = `atime ASC` (and vice versa); the SQL
   (`to_order_by`/`to_partition_order_by`) and `xdu-view`'s in-memory sort must agree.
10. **CLI single source of truth.** `src/cli.rs` is the one definition; completions and `doc/*.scd`
    describe it; a CLI change updates `doc/*.scd` in the same commit. Prefer clap `ValueEnum`/
    `value_parser` over late string validation.
11. **Altitude / testability.** Bins stay thin (arg parse, terminal setup/teardown, event loop);
    parsing/formatting/sniffing/SQL-building/state-transition logic lives in `lib` so tests reach it.
12. **TUI terminal safety.** Raw mode + alternate screen restore on **every** exit path including
    panic (Drop guard / panic hook, not sequential code — `panic="abort"` is set); previewed file
    bytes are `strip_ansi`-sanitized before rendering.
13. **Project conventions.** Version single-sourced from `Cargo.toml`; `share/` generated + ignored +
    CI-asserted; tarball layout (`bin/` + `share/{man,bash-completion,zsh}`) matches `install.sh`;
    non-TTY runs keep stdout clean/pipeable (progress → stderr); reuse shared helpers; declarative
    comments and **no `spec/` R#/P# ids in source** (they restart per feature and collide across
    branches).

## High-risk files & footguns (quick reference)

- **`src/bin/xdu-rm.rs`** — destructive. `--limit` needs a deterministic `ORDER BY`; `--safe` must
  re-verify the criteria it claims to (atime/size today; min-size/newer-than/pattern are gaps); the
  confirm/dry-run/force gates are load-bearing.
- **`src/lib.rs`** — `get_schema()` (schema stability), `QueryFilters` (the SQL/injection surface),
  `index_glob` (the one place the index layout becomes SQL), and the `ROOT_PARTITION` /
  `COMPLETION_MARKER` layout constants.
- **`src/crawl.rs`** — `PartitionBuffer::finalize()` atomicity + stale-chunk pruning; the `__root__`
  collision rejection in `build_work_queue`; completion-marker write/clear ordering.
- **`src/bin/xdu.rs`** — the shared-pool concurrency model (driver threads, `thread::scope` error
  propagation); marker clear-after-pre-flight / write-on-success sequencing.
- **`src/bin/xdu-view.rs`** — terminal restore on panic; multibyte-safe truncation; empty-list
  bounds; unbounded preview memory.

## Testing

- `lib.rs` carries strong pure-function unit tests (parse_size, SizeMode, SortMode, QueryFilters,
  formatters, get_schema). Keep new logic in `lib` so it is testable.
- Integration tests in `tests/` drive the **real binaries** (`std::process::Command`) against real
  temp indexes (`tempfile` + `libc` dev-deps; `rm_tests.rs` backdates atimes via `utimensat`;
  `tests/common/mod.rs` holds the shared helpers). Never reimplement production logic in a test.
  Some cases self-skip where the platform can't host them (running as root; APFS rejecting non-UTF-8
  filenames) — a skipping test still prints `ok`, so check with `--nocapture` before trusting a green
  suite to mean a case actually ran.
- **Verify by driving the CLI, not just tests.** Use `.agents/factory/bin/temp_index.sh` so a drive
  hits a throwaway index, never a real one. Exit 0 is necessary but not sufficient — assert a concrete
  post-condition (row count, a stdout token, files actually gone/kept).
- **Performance work is measured, not asserted.** `bench/` holds the crawl benchmark: `gen_tree.py`
  (sparse synthetic trees — full `stat` cost, ~0 disk), `run.sh` (the runner; emits one JSON document
  per invocation), `scenarios.md` (the shape table + comparability rules), `HPC-PROTOCOL.md` (the
  protocol community operators run on real Lustre/GPFS/ZFS), and the committed
  `results/baseline.json` reference. `sh bench/run.sh smoke` is the fast non-rot check and asserts the
  index holds exactly the generated files. **Comparing two builds requires an interleaved A/B in a
  single invocation** — `run.sh --compare-bin PATH` (build the older side in a `git worktree`, never
  a stash). Two separate invocations cannot resolve it: on the reference host the same binary drifts
  up to ~20% between invocations, and >2× on the flat-wide shape, so a two-document comparison
  measures the session, not the code. See `bench/scenarios.md` "The noise floor".

## Packaging & release

- `share/` is generated: man pages via `scdoc` from `doc/*.scd`; completions via `gen-completions`
  from `src/cli.rs`. The `.scd` sources carry **no version string**, so a pure version bump does not
  touch them — they change only when the CLI changes.
- Release profile (`Cargo.toml`): `lto = true`, `codegen-units = 1`, `panic = "abort"`, `strip = true`.
- Release tarball layout is a contract: `bin/{xdu,xdu-find,xdu-view,xdu-rm}` +
  `share/{man/man1/*.1, bash-completion/completions/*, zsh/site-functions/*}`; `install.sh`'s
  extraction mirrors it exactly. `/xdu-release` cuts versions (bump `Cargo.toml` + `Cargo.lock`,
  gate, tag, publish); see its skill.
- **Pre-release gate (mirrored by CI and `/xdu-release`):** `cargo fmt --all -- --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test` must all pass clean.

## Working on this codebase as an agent

- **Use the factory for non-trivial work.** A feature/fix/refactor flows through the `.agents/`
  spec-driven lifecycle — `/xdu-feature` (shape `GOAL.md`) → `/xdu-plan` (research + `PLAN.md`/
  `TECH.md`) → `/xdu-build` (execute phases) → `/xdu-review` (blind, evidence-based QA) →
  `/xdu-publish` (squash PR to `main`), each on a `feature/`|`fix/` branch with artifacts under
  `spec/{slug}/`. `.agents/factory/methodology.md` is the *why*; `.agents/factory/invariants.md` is
  the curated footgun checklist derived from this file. **Ceremony scales to appetite** — a
  one-sentence change may skip the lifecycle entirely. Each lifecycle skill ends with a
  silence-by-default meta-note to `spec/{slug}/META.md`; `/xdu-publish` surfaces substantial notes in
  the PR; the human-gated **`/xdu-harness`** applies them back to `.agents/` (logging
  `factory/harness-log.md`) and **never weakens a non-negotiable gate** on a finding's say-so.
- **Cut releases with `/xdu-release`** (operational, not a lifecycle step): bump `Cargo.toml`, run the
  CI-mirror gate, sign a tag, publish — rehearsed in a `git worktree` dry-run and gated on an explicit
  human OK before any irreversible push.
- **Put logic where it belongs:** shared types/parsing/SQL-building in `lib`; keep bins thin. Don't
  duplicate a helper across bins (the crawl-test reimplementation is the anti-pattern).
- **Comments are declarative statements of the invariant / the *why*** — never embed feature-scoped
  spec ids (`R#`, `P#`) in source: they restart per feature and mean nothing in the merged tree.
  Requirement provenance lives in the commit, the PR, and the retained `spec/{slug}/`.
- **This file is the map, but it drifts.** For a deep change, re-verify the specific invariant against
  the source before relying on it, and update this file when the code moves.
