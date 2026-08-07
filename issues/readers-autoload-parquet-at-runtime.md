---
status: adopted by spec/readers-autoload-parquet-at-runtime/ (2026-08-07)
kind: fix
appetite: small
---

# The readers download a Parquet extension at runtime, and `rm_tests` drives stale release binaries

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

**Two independent defects, recorded together because they were found together and the second one
masked the first for most of an afternoon.** They have different fixes and different blast radii; see
"Shape them as two phases" below before treating this as one change.

## Problem A — every reader needs the network on first run (and this is why CI is red)

`Cargo.toml:17` asks for `duckdb = { version = "1", features = ["bundled"] }`. The `bundled` feature
builds DuckDB from source but does **not** statically link the Parquet reader — that is a separate
opt-in, and autoinstall is on by default:

- `libduckdb-sys/build.rs:128-129` — `#[cfg(feature = "parquet")] add_extension(..., "parquet", ...)`
- `libduckdb-sys/build.rs:135-136` — `DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT=1`, `AUTOLOAD_DEFAULT=1`
- `duckdb`'s own manifest: `bundled = ["libduckdb-sys/bundled"]` and, separately,
  `parquet = ["libduckdb-sys/parquet", "bundled"]`

So `xdu-find`, `xdu-view` and `xdu-rm` ship without a Parquet reader and fetch a **12 MB**
`parquet.duckdb_extension` from the internet on first use, into the shared `~/.duckdb/extensions/`.

Reproduced against the real binaries with a cold cache (`HOME` pointed at an empty temp dir):

```
$ env HOME=$COLD ./target/debug/xdu-find -i "$IDX" --count
1
$ find $COLD -type f
$COLD/.duckdb/extensions/v1.4.4/osx_arm64/parquet.duckdb_extension.info
$COLD/.duckdb/extensions/v1.4.4/osx_arm64/parquet.duckdb_extension     # 12 MB, downloaded
```

**The CI failure is the concurrent case of this.** On a cold `ubuntu-24.04` runner,
`tests/crawl_tests.rs` fires 39 `xdu-find` invocations in parallel and they race to install the same
file. Run `31114106017` on `main` (`c4618c6`) shows both halves of the race:

```
test_basic_and_per_partition_counts ... FAILED
test_crawl_is_deterministic ... FAILED

Could not remove file ".../v1.4.4/linux_amd64/parquet.duckdb_extension": No such file or directory
Extension ".../parquet.duckdb_extension" not found. Install it first using "INSTALL parquet".
```

macOS, lint and packaging all passed, and every assertion that ran passed — 21 of 23 were fine. It
has never reproduced on a developer machine because `~/.duckdb/extensions/` goes warm on first use and
stays warm.

**The flake is the least of it.** `xdu` exists for HPC storage administration, and a login node on an
air-gapped cluster is a normal deployment target. As shipped, the query tools fail there on first run
with an extension-autoload error that says nothing about the real cause. That is a product defect that
happens to surface as a red gate.

Fix and verification, both already run:

```diff
-duckdb = { version = "1", features = ["bundled"] }
+duckdb = { version = "1", features = ["bundled", "parquet"] }
```

With that applied: a cold-cache `xdu-find`, `xdu-rm --dry-run/--force/--safe/--limit/--min-size`, and
the whole 23-test `crawl_tests` suite all complete with **zero** files written under
`$COLD/.duckdb` — fully offline. Cost is a one-time DuckDB rebuild and roughly +7 MB on the debug
binary (release impact not measured).

## Problem B — `tests/rm_tests.rs` runs whatever is in `target/release/`

`tests/rm_tests.rs` defines its own binary resolver instead of using the shared one:

```rust
// tests/rm_tests.rs
fn binary_path(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    // Try release first, then debug
    let release_path = path.join("release").join(name);
    if release_path.exists() { return release_path; }
    path.join("debug").join(name)
}
```

`tests/common/mod.rs` already has the correct version, and its doc comment states exactly the hazard:

> `CARGO_BIN_EXE_<name>` is set by Cargo to the binary compiled for the current test profile, so the
> tests never accidentally exercise a stale `target/release` artifact left over from an earlier build.

`grep -c "mod common"` returns `1` for `crawl_tests.rs` and **`0`** for `rm_tests.rs` — it never
adopted the helper. Caught by polling for the spawned process during a test run:

```
/Users/.../xdu/target/release/xdu-rm -i /var/.../index --older-than 30 --force
```

So all 16 `rm_tests` — including every destructive-deletion, `--safe` and `--limit`-determinism case —
were asserting against a release binary built hours earlier, from different source and different
Cargo features. `cargo test` was green the whole time. This is the same class as harness finding
`F10` (a CLI drive silently measuring a stale artifact), and it is the reason Problem A took so long
to pin down: `rm_tests` kept "downloading" after the fix because it was running a pre-fix binary.

It also means `spec/crawl-hardening/ASSESSMENT.md`'s claim that `tests/common/mod.rs` "removed the
duplication between `crawl_tests.rs` and `rm_tests.rs`" is **overstated** — only `crawl_tests` adopted
it. That file is a frozen point-in-time record and should not be retro-edited; noting it here so the
next reader does not trust it.

## Why it was deferred

Found on 2026-08-06 while diagnosing why `main` went red on the `crawl-hardening` merge. Problem A is
pre-existing — `git show main:Cargo.toml` has the same feature list well before that branch — and
Problem B is pre-existing too. What `crawl-hardening` changed is exposure: P1 took `crawl_tests.rs`
from a fake reimplementation making **zero** real binary calls to a 23-test suite making 39, which is
what turned a latent install race into a reproducible CI failure.

Recorded rather than fixed because the session that found it was diagnosing, not building, and because
Problem B changes what every destructive-deletion test actually exercises — that deserves its own
review rather than riding along on a one-line dependency change.

## Outcome / vision

The query tools work on a machine that has never had network access, and `cargo test` exercises the
binaries it just built. Neither of those is currently true, and both are silent.

## Sketch of the acceptance criteria

- **R1** — WHEN a reader (`xdu-find`, `xdu-view`, `xdu-rm`) runs on a host with no DuckDB extension
  cache and no network, it SHALL complete a query successfully and SHALL NOT attempt to download an
  extension.
- **R2** — The test suite SHALL pass against a cold extension cache, and CI SHALL exercise that case
  rather than relying on a warm runner.
- **R3** — WHEN an integration test invokes a project binary, it SHALL invoke the artifact built for
  the current test profile, and no test SHALL resolve a binary from `target/release/` by existence.
- **R4** — `rm_tests.rs` SHALL use the shared `tests/common` helpers, and the duplicate local
  `binary_path`/`create_test_file`/`run_*` definitions SHALL be removed.
- **R5** — The destructive `xdu-rm` behaviours SHALL be re-verified against a correctly-resolved
  binary once R3/R4 land, since their previous green results are not evidence about current code.

## Notes

- **Shape them as two phases, not one.** They are independent, and bundling them means one gate
  covers both — a regression in the test-harness change could hide behind the dependency fix passing.
  This is the `xdu-plan` "unit of measurement is the unit of change" rule.
- Sequence matters: land R3/R4 **first**. Until the harness resolves binaries correctly, no `rm_tests`
  result is evidence about the code under change, including evidence about R1.
- Design options for R1: the one-line `features = ["bundled", "parquet"]` (verified working, costs
  build time and binary size); or keep autoload and pre-warm the cache in CI, which fixes the gate but
  leaves the air-gapped deployment broken and so is not really an option.
- Worth checking during shaping whether `json`/`icu` are autoloaded anywhere too — only `parquet` was
  observed, but the same mechanism would apply to any extension a query pulls in.
- Related: [`spec/crawl-hardening/META.md`](../spec/crawl-hardening/META.md) `F10` (the same
  stale-binary class, in `temp_index.sh`); CI run `31114106017` for the failure log.
- Found by: post-merge CI diagnosis on `main`, 2026-08-06.
