# GOAL — The readers must query offline, and the tests must exercise the binary they just built

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).

- **slug:** readers-autoload-parquet-at-runtime
- **kind:** fix
- **appetite:** small

## Problem

**`xdu-find`, `xdu-view` and `xdu-rm` cannot read an index without the network.** `Cargo.toml:17`
declares `duckdb = { version = "1", features = ["bundled"] }`. `bundled` builds DuckDB from source but
does **not** statically link the Parquet reader; that is a separate opt-in feature, and DuckDB's
extension autoinstall/autoload defaults are left on. So every reader ships without a Parquet reader
and fetches a 12 MB `parquet.duckdb_extension` from the internet on first use, into the shared
`~/.duckdb/extensions/`. Reproduced against the real binaries with `HOME` pointed at an empty temp
directory: a single `xdu-find --count` returns the right answer *and* leaves a freshly downloaded
12 MB extension behind.

This is a product defect first and a CI defect second. `xdu` exists for HPC storage administration,
where an air-gapped login node is a normal deployment target — and there the query tools simply fail
on first run, with an autoload error that names nothing about the real cause. The CI failure is the
same bug's concurrent case: on a cold `ubuntu-24.04` runner, `tests/crawl_tests.rs` fires 39
`xdu-find` invocations in parallel and they race to install the same file (run `31114106017` on
`main`, `c4618c6`: *"Could not remove file … parquet.duckdb\_extension: No such file or directory"* and
*"Extension … not found. Install it first"*). It has never reproduced on a developer machine because
`~/.duckdb/extensions/` goes warm on first use and stays warm. **`main` is red until this lands.**

**Separately, `tests/rm_tests.rs` has not been testing the code under build.** It defines its own
`binary_path()` (`tests/rm_tests.rs:15`) that returns `target/release/<bin>` *if that file exists* and
only otherwise falls back to the current profile's artifact. `tests/common/mod.rs:19` already carries
the correct `CARGO_BIN_EXE_`-based resolver, and its doc comment names exactly this hazard — but
`rm_tests.rs` has no `mod common;` and never adopted it (it re-declares `binary_path`,
`create_test_file`, `build_index` and its own runner). All 16 `rm_tests` — including every destructive
deletion, `--safe` and `--limit`-determinism case — have been asserting against whatever release
binary happened to be lying around, built from different source with different Cargo features, while
`cargo test` reported green. It is also why the first defect took an afternoon to pin down: `rm_tests`
kept "downloading" after the dependency fix was applied, because it was running a pre-fix binary.

Both defects are pre-existing. What the `crawl-hardening` merge changed is exposure — it took
`crawl_tests.rs` from a reimplementation making zero real binary calls to a suite making 39, turning a
latent install race into a reproducible red gate.

## Outcome / vision

A reader queries an index successfully on a machine that has never had network access, and writes
nothing outside the index and its own output while doing so. `cargo test` exercises the binaries Cargo
just built for the profile under test, so a green suite is evidence about the current tree. Both
properties are asserted by a test rather than inferred from a run that happened not to fail.

## Acceptance criteria (the contract)

- **R1** — WHEN `xdu-find`, `xdu-view` or `xdu-rm` queries a valid index on a host whose DuckDB
  extension cache is empty and which has no network access, the tool SHALL complete the query
  successfully and produce the same results it produces on a warm host.
- **R2** — WHILE any reader executes a query, it SHALL NOT create or modify any file in the DuckDB
  extension cache; an automated test that runs in CI SHALL assert this by directing a reader's
  extension cache at an empty directory and requiring that directory to stay empty.
- **R3** — WHEN an integration test invokes a project binary, it SHALL invoke the artifact Cargo built
  for the current test profile. No test SHALL select a binary by probing `target/release/` for
  existence.
- **R4** — `tests/rm_tests.rs` SHALL take its binary resolution and shared fixtures from
  `tests/common`, and SHALL NOT retain local definitions duplicating helpers that module provides.
- **R5** — The full `rm_tests` suite SHALL pass while resolving binaries per R3. IF a case fails once
  correctly resolved, THEN the defect it exposes SHALL either be fixed here or recorded as an
  `issues/{slug}.md` with a `ROADMAP.md` entry before this work merges — a previously-green result is
  not evidence about the current code.

**Sequencing constraint (contract-relevant, not a design choice).** R3/R4 land before R1/R2 are
verified. Until the harness resolves binaries correctly, no `rm_tests` outcome — including any
observation about extension downloads — is evidence about the tree under change.

## Non-goals (no-gos)

- **`.agents/factory/bin/temp_index.sh:34`**, which also hardcodes `target/release`. Same defect
  class, but it is already recorded as `spec/crawl-hardening/META.md` finding **F10** and belongs to
  the human-gated `/xdu-harness` lane, not to a source fix branch.
- **`bench/run.sh:193-194`**, which resolves `target/release/xdu` deliberately — performance work must
  measure the release profile. Not a defect; named here so the class is closed rather than half-swept.
- **Statically linking any DuckDB extension the readers do not use.** The readers' entire SQL surface
  is `read_parquet` and `regexp_matches`; the latter is core DuckDB. `json`, `icu` and `httpfs` are
  not reachable and are out of scope.
- **Optimizing binary size or build time.** A one-time DuckDB rebuild and a size increase (~+7 MB
  observed on the debug binary; release impact unmeasured) are accepted costs, not problems to solve
  here. Measuring the release delta is worth reporting, not worth engineering against.
- **Widening what `--safe` re-verifies.** The known gaps (`--min-size`, `--newer-than`, `--pattern`
  are not re-checked before unlink) are a separate defect. R5 covers *running* the existing suite
  honestly, not adding coverage to it.
- **Retro-editing `spec/crawl-hardening/ASSESSMENT.md`**, whose claim that `tests/common/mod.rs`
  "removed the duplication between `crawl_tests.rs` and `rm_tests.rs`" is overstated — only
  `crawl_tests` adopted it. That file is a frozen point-in-time record.
- Any change to the index format, the Parquet schema, or the CLI surface.

## Clarifications

- **Q:** Two defects — one GOAL or two? — **A:** One GOAL, two sequenced phases on a single branch.
  They are independent in mechanism but ordered in evidence (see the sequencing constraint), and a
  single small-appetite fix is the right ceremony (resolved 2026-08-07).
- **Q:** Draft R2 said "CI SHALL exercise a cold cache", but GitHub runners are already cold — that is
  *why* CI is red. What does R2 actually contract? — **A:** A positive assertion that no download
  occurs, not the absence of a failure. A green cold runner only proves the race did not fire on that
  run and says nothing about air-gapped deployment; an assertion also survives a future `duckdb` crate
  bump that flips the feature default back (resolved 2026-08-07).
- **Q:** The stale-binary class also appears outside `tests/`. How far does the fix reach? — **A:**
  `tests/` only. The two non-test instances are named in Non-goals with their owners, so the class is
  scoped even though only one instance is remediated (resolved 2026-08-07).
- **Q:** The seed asked whether `json`/`icu` are autoloaded too. — **A:** No. Resolved during shaping:
  `grep` over `src/` shows the readers' only SQL surface is `read_parquet` (17 sites) and
  `regexp_matches` (3 sites, core DuckDB). Scope is `parquet` alone (resolved 2026-08-07).

## Related materials

- Seed: [`issues/readers-autoload-parquet-at-runtime.md`](../../issues/readers-autoload-parquet-at-runtime.md)
  · index entry in [`ROADMAP.md`](../../ROADMAP.md) ("The readers need the network on first run…")
- Failing CI: run `31114106017` on `main` (`c4618c6`) — `test_basic_and_per_partition_counts`,
  `test_crawl_is_deterministic`
- Sources: `Cargo.toml:17`; `tests/rm_tests.rs:15` (the local resolver); `tests/common/mod.rs:19-21`
  (the correct one, with the doc comment naming the hazard); `tests/crawl_tests.rs:8` (`mod common;`)
- Same defect class, different owner: `spec/crawl-hardening/META.md` finding **F10**
- Upstream mechanism: `libduckdb-sys/build.rs` gates `add_extension(..., "parquet", ...)` behind its
  own `parquet` feature and defaults `DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT` / `..._AUTOLOAD_DEFAULT`
  to 1; the `duckdb` crate exposes `bundled` and `parquet` as separate features
