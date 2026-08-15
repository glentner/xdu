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
- **Version is single-sourced from `Cargo.toml`** — never hardcode a version string in `src/`; read it
  from `CARGO_PKG_VERSION` (the completion marker does, `crawl.rs`). **The `--version` flag itself does
  not exist:** no `#[command(...)]` in `src/cli.rs` sets `version`, so all four binaries reject `-V`
  and `--version` even though every `doc/*.scd` documents the flag. That mismatch is a recorded defect,
  not a convention — see [`issues/version-flag-missing.md`](issues/version-flag-missing.md).
- **A CLI change updates its `doc/*.scd` man page source in the same commit.** Shell completions
  regenerate automatically from `src/cli.rs` (`gen-completions`), so they are not committed; the
  generated `share/` tree is git-ignored (built in CI and by `/xdu-release`).
- **Delete with `del`, never `rm`** — and read the exceptions in the next bullet before applying this
  to committed code. `del` ([`delete-cli`](https://pypi.org/project/delete-cli/), the maintainer's)
  moves a path to the `$HOME` trash instead of unlinking it, so a mistake is recoverable: `del PATH…`
  removes, `del --list` shows what is there, `del --restore PATH` undoes, `del --empty` reclaims. This
  binds **every** harness working in this repo, not just Claude Code. It is about the *act*, not the
  spelling — any irreversible removal of something you did not just create counts, whether written
  `rm`, `find -delete`, `git clean -fdx`, `shutil.rmtree` or `truncate`. Three sharp edges, each
  measured: **`-r` means `--restore`, not recursive** — `del -r dir` exits 0 and deletes *nothing*,
  while plain `del dir` handles directories natively; there is **no `-f`**, and while a missing path
  is exit 0, a *usage* error is not (`del -rf` exits 2), so do not assume success; and `del` needs a
  real `$HOME` — unset, it writes `.Trash/` and `.Trash.db` into the working directory, and under a
  redirected `$HOME` (as `tests/offline_tests.rs` and several `verify:` gates use) it lands in the
  fixture and reddens it. Not on `PATH`? In order: `uvx --from delete-cli del …` (ephemeral, installs
  nothing), else `uv tool install delete-cli` once. If neither resolves — no `uv`, no network — **stop
  and ask.** The interactive shell defines `rm` as a function that refuses, and routing around it with
  `command /bin/rm`, `\rm`, `env rm` or `sh -c 'rm …'` defeats a guardrail that exists on purpose.
  That bypass already happened here, and because the refusing `rm` **exits 0**, it read as a clean run
  while leaving `/usr/bin/false` installed as `target/release/xdu` for two later phases to measure.
- **…and the deletions that stay `rm`.** The rule above governs what an *agent* removes; it does not
  reach committed code, and converting these breaks things. Keep `rm` wherever there is no trash to
  move to, or nothing worth recovering: **container builds** (`Dockerfile` — the point of
  `rm -rf /var/lib/apt/lists/*` is layer size, and a move keeps the bytes in the layer); **the
  installer** (`install.sh` runs on a stranger's machine via `curl | sh`, is POSIX `sh` with no Python,
  and must not seed their trash); **CI**, which has no `del`; **HPC operator instructions**
  (`bench/HPC-PROTOCOL.md`) — there `$HOME` is a small quota'd filesystem separate from the scratch one,
  so a trash move becomes a cross-device copy of a multi-million-file index; **the benchmark trees**
  (`bench/`), which reach millions of sparse files, where trashing frees no disk and puts a rename
  inside the timed region; **self-cleaning scratch** — any `mktemp -d` that its creator disposes of,
  script or agent alike; **a tool's own bookkeeping** (`git rm`, `git worktree remove`, `cargo clean`,
  `docker prune`), which maintains metadata a trash move would strand; **published examples**
  (`README.md`, `doc/*.scd`) that teach users `xdu-find … | xargs rm`; and **`xdu-rm` itself**, whose
  entire product purpose is to free blocks.

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

# ...and to ASSERT a literal survived, match it the way CI does. Strip all whitespace from the page
# AND from the literal: mandoc breaks lines wherever the fill lands, including inside a token, so an
# un-normalized grep calls an intact literal missing. Your scdoc is probably not CI's — homebrew's
# escapes hyphen-minus, the distro package does not — so the un-normalized form can pass for you and
# fail on ubuntu-24.04. Keep this in lockstep with the "Assert critical literals" step in
# .github/workflows/test.yaml and its restatement in .agents/factory/invariants.md §13.
lit='OUTDIR/.xdu-complete'
scdoc < doc/xdu.1.scd | mandoc -Tutf8 | col -b | tr -d '[:space:]' |
  grep -qF -- "$(printf '%s' "$lit" | tr -d '[:space:]')" || echo "MISSING: $lit"

# A literal that occurs MORE THAN ONCE needs a count, not a presence check — `grep -q` is satisfied
# by the surviving copy, so corrupting one of two occurrences reads green here and red in CI, which
# asserts it as `2x:.partial suffix`. Count the same normalized way. Keep the needle specific enough
# that stripping whitespace cannot FUSE two adjacent tokens into a synthetic match: `e.g. partial`
# becomes `e.g.partial`, so count `.partial suffix`, never a bare `.partial`.
lit='.partial suffix'; want=2
got=$(scdoc < doc/xdu.1.scd | mandoc -Tutf8 | col -b | tr -d '[:space:]' |
  grep -oF -- "$(printf '%s' "$lit" | tr -d '[:space:]')" | wc -l | tr -d ' ') || got=0
[ "$got" -eq "$want" ] || echo "MISCOUNT: $lit — $got, expected $want"
```

**Rendering is not optional, and exit 0 is not sufficient.** `scdoc` markup fails in two ways: a
nesting error is loud (`*__root__*` — `_` opens italic inside bold) and exits 1, but a mis-escaped
literal is **silent at exit 0** — `_OUTDIR_/*/*.parquet` published as `OUTDIR//.parquet` because `*`
is bold markup, and a line *starting* with `.` has that period silently dropped. Escape a literal
asterisk `\*` and a double underscore `\_\_` (mid-word `_` as in `*XDU_INDEX*` is safe); never start
a line with `.` or `'` — **rewrap, never escape**: `\.` at line start deletes the rest of the line,
also silently. None of this is mechanically fixable — `*/*` is a corrupted glob in `xdu.1.scd` and a
legitimate bold-slash (the `/` key) in `xdu-view.1.scd`, so intent has to be read. Catching the
silent class requires diffing the rendered text against the literal you intended, which is what the
`mandoc | col -b` line above is for.

**A third mode is not a markup error at all: the literal is intact and still unfindable.** `mandoc`
fills each paragraph to its own width and will break a line *inside* a token — a bare roff `-` is a
legal break opportunity — so `OUTDIR/.xdu-complete` can publish as `OUTDIR/.xdu-` + newline +
`complete`. Collapsing newlines to spaces does not recover it (the break was *within* a word, not
between two), and `col -b` indents continuation lines with a literal TAB, which squeezing spaces
never touches. That is **layout, not content**: nothing is corrupt, and the fix is to compare with
all whitespace stripped from both sides — the second snippet above. Whether you ever see it depends
on your `scdoc`: 1.11.5 substitutes `\-` for a bare `-` (upstream `1d4143d`), ubuntu-24.04's 1.11.2
does not, so the same `.scd` yields two different roffs and a check can be green on your box while
`main` is red. Which is exactly what happened — the gate arrived already broken and had never once
been green in CI. The normalization now lives in three places that must change together:
`.github/workflows/test.yaml`'s assertion step, the snippet above, and
[`invariants.md`](.agents/factory/invariants.md) §13.

**Presence is the wrong question for a literal that appears twice.** `grep -q` is satisfied by
whichever copy survived, so corrupting exactly one of `doc/xdu.1.scd`'s two `.partial suffix`
occurrences is invisible to it — which is why CI counts that one (`2x:.partial suffix`) instead. The
same is true of every `*XDU_INDEX*` / `*XDU_JOBS*`: each appears twice on its page, once as a
cross-reference in the flag description and once as the `ENVIRONMENT` entry, so corrupting only the
`ENVIRONMENT` entry publishes a variable name that does not exist while a presence check stays green.
All of them are counted for that reason. **Whether a literal is duplicated is a fact about the page,
not about the literal** — adding one cross-reference to a `.scd` can duplicate a previously-unique
literal — so it is derived by the verify harness rather than remembered, and a new page joins the
build with its counts checked. A local presence check on a duplicated literal therefore *cannot*
predict CI's verdict no matter how correctly it normalizes; use the counting form above. Counting is what makes stripping whitespace
dangerous in the other direction: the strip is lossy and can fuse adjacent tokens into a match that
never appeared on the page, which is harmless for presence (at worst a false green) but a false
**red** for a count — so a counted needle must be specific enough that fusion cannot synthesize it.

## Repository map

```
src/
  lib.rs         # shared core: get_schema() (THE index schema), SizeMode, parse_size,
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
tests/           # crawl_tests.rs, rm_tests.rs, offline_tests.rs (integration); common/ = shared helpers
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
  `<index>/` holds exactly two kinds of entry — partition directories and the marker dotfile below —
  so **both** their names are reserved (`RESERVED_INDEX_NAMES`, `lib.rs`, pairing each name with what
  claims it). A top-level source subdirectory of either name is **rejected** by
  `crawl::build_work_queue`, which iterates that list: it would clobber the synthetic partition's chunk
  ids, or occupy the marker path and brick the outdir for every later run.
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
   selects these by name. There is **no on-disk schema version**, so any change to `get_schema()` or
   a reader's column list is a breaking, cross-cutting index-format change (issues
   #2/#3 — owner/group/perms — are exactly this: add a schema version first).
2. **Atomic finalization.** Write `NNNNNN.parquet.partial` → `fs::rename` (same dir) → prune stale
   higher chunks (`crawl.rs::PartitionBuffer::finalize()`). Never `File::create` a final `.parquet`,
   never cross-dir rename, never let a reader glob `.partial`. **Known limitation:** finalize is
   per-file, not per-partition atomic — do not index a partition while purging it. Run-level
   completeness is a *separate* mechanism: the `.xdu-complete` marker (see Architecture), cleared after
   pre-flight and written only on success. Never write it on a failure path.
3. **Partition scheme.** `<index>/<partition>/NNNNNN.parquet`; `__root__` reserved for loose
   top-level files (depth-1 walk); zero-padded sequential chunk ids; readers glob `*/*.parquet`. The
   index root's namespace is a **class, not two facts**: it holds partition directories plus the
   `COMPLETION_MARKER` dotfile, and *every* name in `lib::RESERVED_INDEX_NAMES` is **rejected** as a
   top-level source directory by `crawl::build_work_queue`, unconditionally — even when a `--partition`
   filter would exclude it, because the collision is with the layout, not with the selection. Reserving
   a new name at the index root means adding it to that list in the same commit; the guard iterates the
   list, so the rejection follows by construction.
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
  `COMPLETION_MARKER` layout constants with the `RESERVED_INDEX_NAMES` list that guards both.
- **`src/crawl.rs`** — `PartitionBuffer::finalize()` atomicity + stale-chunk pruning; the
  reserved-index-name collision rejection in `build_work_queue`; completion-marker write/clear ordering.
- **`src/bin/xdu.rs`** — the shared-pool concurrency model (driver threads, `thread::scope` error
  propagation); marker clear-after-pre-flight / write-on-success sequencing.
- **`src/bin/xdu-view.rs`** — terminal restore on panic; multibyte-safe truncation; empty-list
  bounds; unbounded preview memory.

## Testing

- `lib.rs` carries strong pure-function unit tests (parse_size, SizeMode, SortMode, QueryFilters,
  formatters, get_schema). Keep new logic in `lib` so it is testable.
- Integration tests in `tests/` drive the **real binaries** (`std::process::Command`) against real
  temp indexes (`tempfile` + `libc` dev-deps). **`tests/common/mod.rs` is the one home for binary
  resolution and fixtures** — every `tests/*.rs` carries `mod common;` and takes `binary_path` (which
  is `CARGO_BIN_EXE_*`, so a test always drives the artifact Cargo built for the profile under test),
  `build_index`, the `run_*` wrappers, and `set_atime_days_ago` (backdates atimes via `utimensat`)
  from it. A fixture that lives in one test file instead is how `rm_tests.rs` came to re-declare a
  `binary_path` that preferred whatever was lying in `target/release/`, so all 16 destructive tests
  ran a stale binary while `cargo test` stayed green. Never reimplement production logic in a test.
  Some cases self-skip where the platform can't host them (running as root; APFS rejecting non-UTF-8
  filenames) — a skipping test still prints `ok`, so check with `--nocapture` before trusting a green
  suite to mean a case actually ran.
- **`offline_tests.rs` guards air-gapped operation.** The readers must answer a query with an empty
  DuckDB extension cache and write nothing into it; the test redirects `HOME` and asserts that
  directory stays empty. This is why `Cargo.toml` asks `duckdb` for `parquet` alongside `bundled` —
  `bundled` alone leaves the Parquet reader to autoinstall a 12 MB extension at runtime. Dropping the
  feature restores a product defect on air-gapped HPC login nodes, not just a slow first run.
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
