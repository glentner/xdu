# PLAN — The readers must query offline, and the tests must exercise the binary they just built

> **Status:** Draft for review · **Last updated:** 2026-08-07
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). Every design element traces to a GOAL R-ID.

## 1. Summary

Two small, independent edits with one hard ordering constraint between them. **The dependency fix is
one line** — add `parquet` to the `duckdb` features in `Cargo.toml:17` — which statically links the
Parquet reader instead of autoinstalling a 12 MB extension on first use. **The harness fix deletes
code** — `tests/rm_tests.rs` drops its five local helpers and adopts `tests/common`. The design work
is not in either edit; it is in **proving each one**, because both defects are of the class that a
green gate cannot see. So each phase carries an explicit negative control: P1 poisons `target/release/`
and requires the suite to stay green, P2 requires the new offline test to be observed **red** on the
pre-fix dependency line before the fix is applied. No `appetite: small` budget is spent on design
options; it is spent on evidence.

## 2. Design

### 2.1 The dependency fix (R1, R2)

```diff
-duckdb = { version = "1", features = ["bundled"] }
+duckdb = { version = "1", features = ["bundled", "parquet"] }
```

`bundled` compiles DuckDB from source; it does **not** link the Parquet reader, which is a separate
opt-in in `libduckdb-sys`, and the crate leaves `DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT` /
`..._AUTOLOAD_DEFAULT` at 1. The result is that `read_parquet` resolves at *runtime* by downloading
`parquet.duckdb_extension` into `$HOME/.duckdb/extensions/`.

**This is a linkage property of one shared dependency, not a per-binary runtime property.** All three
readers — `src/bin/xdu-find.rs:29`, `src/bin/xdu-view.rs:1859`, `src/bin/xdu-rm.rs:51` — call
`Connection::open_in_memory()` against the same `duckdb` crate. That is what makes the test in §2.2
sufficient despite covering only two of the three: `xdu-view` cannot link a different DuckDB than
`xdu-find` does. `src/bin/xdu.rs` and `src/crawl.rs` link no DuckDB at all (the crawler writes Parquet
through the `parquet` crate), so index *building* is not on this path.

No source file changes. `src/lib.rs::get_schema()`, `index_glob`, and every reader's column list are
untouched (see §3 §1).

### 2.2 The offline regression test (R2)

New `tests/offline_tests.rs`, using `tests/common`:

1. Build a small fixture and index it with `xdu` under the **normal** environment — the crawler links
   no DuckDB, so keeping it out of the cold environment makes the assertion attribute cleanly to the
   readers.
2. Run `xdu-find --count` and `xdu-rm --dry-run` as subprocesses with `HOME` pointed at an empty
   directory inside the test's `TempDir`.
3. Assert each exits 0 **and returns the right answer** (a row count, not merely success — a reader
   that fell back to a broken path could still exit 0).
4. Assert **zero regular files** were created anywhere under that cold `HOME`, recursively, and name
   the diagnosis in the failure message ("a reader is autoloading a DuckDB extension at runtime").

`HOME` is the lever because DuckDB's extension directory defaults to `$HOME/.duckdb`. That this is the
*right* lever is not taken on faith — P2's negative control proves it empirically by observing the
test fail before the `Cargo.toml` edit.

Two additions to `tests/common/mod.rs` support it, keeping §13's reuse rule intact rather than
re-forking helpers in a second file:

- `run_binary_with_home(name: &str, home: &Path, args: &[&str]) -> (String, String, bool)` —
  `Command::new(binary_path(name)).env("HOME", home)`.
- `list_files_recursive(dir: &Path) -> Vec<PathBuf>` — so the assertion can *report* what was written,
  not just that something was.

The test's module doc records why `xdu-view` is absent (headless, no TTY) and why that is not a gap
(the linkage argument in §2.1), so a later reader does not "fix" it by driving a TUI in CI.

### 2.3 The harness fix (R3, R4)

`tests/rm_tests.rs:15` defines a resolver that returns `target/release/<bin>` **if that path exists**
and only otherwise falls back to the current profile. `tests/common/mod.rs:21` already has the correct
`CARGO_BIN_EXE_*` version, with a doc comment naming this exact hazard. The fix is adoption:

| local in `rm_tests.rs` | disposition |
|---|---|
| `binary_path` (l.15) | **delete** — defective duplicate of `common::binary_path` |
| `create_test_file` (l.27) | **delete** — byte-identical duplicate |
| `build_index` (l.74) | **delete** — `common::build_index` asserts internally and returns `()`; drop `.unwrap()` at 16 call sites |
| `run_xdu_rm` (l.93) | **delete** — becomes `common::run_rm`; drop `.unwrap()` at ~18 call sites |
| `set_atime_days_ago` (l.37) | **move into `tests/common/mod.rs`** |

`set_atime_days_ago` is *not* a duplicate — nothing in `common` provides it — so R4 does not compel
moving it. It moves anyway, and the reason is the defect itself: **leaving a `utimensat` wrapper
outside the shared module recreates the precise condition that caused this bug**, which is that a test
file needed a fixture, did not find it in `common`, and wrote its own. The shared module is the one
home for integration fixtures or it is not a shared module.

The migration is mechanical (delete 5 functions, add `mod common;`, rewrite call sites, drop
`.unwrap()`); no test *assertion* changes. Any assertion that must change is a finding under R5, not a
migration step.

### 2.4 Why this repo is exposed, and why CI is not where B bites

`.agents/factory/bin/temp_index.sh:36` runs `cargo build --release --bins` on **every** factory verify
drive. It is not itself a stale-binary instance — crawl-hardening F10 is `status=applied` and the
script now always rebuilds — but it is what **populates** `target/release/`, which is exactly what
`rm_tests`' resolver probes for. On this working copy `target/release/xdu` and `target/release/xdu-rm`
are present right now, so `cargo test --test rm_tests` is running release binaries today.

The corollary matters for expectations: **CI's `test` job never builds a release profile** (it runs
`cargo build --locked --all-features` then `cargo test --locked --all-features`, both debug), so the
resolver's fallback already selects debug there. Defect B is therefore primarily a **local-developer**
hazard — a green `cargo test` on a maintainer's box says nothing about their working tree — and it is
why defect A took an afternoon to pin down. Fixing B will not change a single CI result; the red gate
is entirely defect A, via `crawl_tests`. (Caveat: a `Swatinem/rust-cache` restore that carried a
release artifact into the `test` job would change this. Not observed, and not something to rely on.)

### 2.5 Rejected: disabling autoload at runtime

The belt-and-braces alternative is `SET autoinstall_known_extensions=false; SET
autoload_known_extensions=false;` on each connection, turning a silent download into a loud error.
Rejected: it adds runtime SQL to three binaries to guard a property the R2 test already asserts in CI,
and it cannot be exercised except by the same offline test. The residual exposure it would have
covered is recorded in §5 as a risk, not as deferred work — there is nothing to do about it today.

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1 | §2.1 `Cargo.toml:17` `features = ["bundled", "parquet"]`; linkage is crate-wide so all three readers are covered. Confirmed at release profile and whole-suite scope by P3's cold-`HOME` run. |
| R2 | §2.2 `tests/offline_tests.rs` + `common::run_binary_with_home` / `list_files_recursive`; runs under plain `cargo test`, so CI executes it. P2's negative control proves it is not vacuous. |
| R3 | §2.3 `common::binary_path` (`CARGO_BIN_EXE_*`) becomes the only resolver; enforced two ways — statically by a `grep` for `join("release")` over `tests/` in P1's gate, behaviourally by the poisoned-`target/release` control. |
| R4 | §2.3 deletion table: five local helpers removed, `set_atime_days_ago` relocated to `tests/common/mod.rs`. |
| R5 | P3 re-runs the full suite **after** the dependency change, so the evidence is taken against the final binary. Triage rule in §5. |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) before research
and again against this drafted design.

- **§1 Parquet schema stability — touched only in appearance, not in fact.** A diff adding a feature
  named `parquet` reads as schema-adjacent; it is not. `get_schema()`, the crawler's writer, and every
  reader's `read_parquet` column list are unchanged. This changes *where the code that parses a
  Parquet file lives* (linked in vs downloaded), never the on-disk row shape. No schema version is
  needed, and none is added.
- **§4 `xdu-rm` destructive safety — not modified, but its evidence is restored.** No change to
  `src/bin/xdu-rm.rs`. The confirm/dry-run/force/`--safe` gates and the `--limit` deterministic
  `ORDER BY` are untouched; what changes is that the 16 tests asserting them finally run against the
  built binary. The §4 `--safe` gaps (`--min-size`, `--newer-than`, `--pattern` unverified) stay open
  by GOAL non-goal — closing them is separate work, and R5 covers running the existing suite honestly,
  not widening it.
- **§10 CLI single source of truth — not triggered.** No `src/cli.rs` change, therefore no `doc/*.scd`
  change is owed in this commit. Stated explicitly so the same-commit rule is not read as unmet.
- **§11 Altitude / testability — honored.** No logic moves between bin and lib; the only new code is
  test-support in `tests/common/mod.rs`, which is where it belongs.
- **§13 Project conventions — this is the invariant the defect violated.** "Reuse shared helpers; do
  not duplicate logic across bins or reimplement production logic in tests" is precisely R4. P3 runs
  the full pre-release gate (`fmt`, `clippy -D warnings`, `cargo test`). Packaging is unaffected: no
  new artifacts, tarball layout and `install.sh` unchanged; only binary size grows (§5).
- **§5 injection, §6 Unix-only, §7 concurrency, §8 symlinks, §9 SortMode, §12 TUI safety** — untouched.
  The new test's use of `HOME` is a POSIX env var, consistent with the crate's existing Unix-only
  stance and adding no new portability assumption.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| P1's gate mutates build output — it copies `/usr/bin/false` over `target/release/{xdu,xdu-rm}` for the duration of the run, then removes them | It is the only way to *demonstrate* R3 rather than assert it. A correctly-resolving suite and a stale-but-compatible binary produce the identical green result, which is how this defect survived | Just running `cargo test --test rm_tests` cannot distinguish the two states — that indistinguishability **is** the defect. `/usr/bin/false` (not `/bin/false`, which is absent on macOS) is used so the poison is a real executable on both CI hosts |

## 4. Rabbit holes (resolved)

`appetite: small` with a known, seed-verified root cause — no research fan-out. The three unknowns
that could have grown it were closed by targeted inspection during planning:

- *Do other DuckDB extensions autoload too (the seed's open question)?* **No.** The readers' entire SQL
  surface is `read_parquet` (17 sites) and `regexp_matches` (3 sites, core DuckDB). `json`, `icu` and
  `httpfs` are unreachable. Scope stays at one feature flag.
- *Is `set_atime_days_ago` available in `common`, making the migration a pure deletion?* **No** — it
  exists only in `rm_tests`. Resolved by moving it (§2.3) rather than discovering it mid-build.
- *Does `common`'s API match the local helpers', or does adoption ripple into every test?* **It
  ripples, shallowly:** `build_index` and `run_rm` have different signatures (assert-and-return-unit
  vs `io::Result`), so ~34 call sites lose an `.unwrap()`. Mechanical, but it is the bulk of P1's diff
  and would have looked like scope creep if discovered during the build.

## 5. Risks & open questions

- **CI build time (medium).** Linking the Parquet extension compiles more DuckDB C++ into the bundled
  amalgamation. The `test` job has a 60-minute timeout and the first post-merge build is uncached.
  P3 records the local build-time delta. If the uncached build approaches the timeout, raise the
  timeout in a follow-up — do not revert the fix, which would restore the product defect.
- **`Cargo.lock` under `--locked` (low, but a hard CI failure if missed).** CI runs
  `cargo build --locked`. If enabling the feature resolves a new package, the lock changes and
  `--locked` fails. P2 runs `cargo build --locked` explicitly and commits any lock change in the same
  commit.
- **Residual autoload exposure (low).** Per §2.5, autoinstall/autoload stay enabled at runtime. If a
  future query needs a different extension, it will silently download again, and only an offline test
  exercising *that* query would catch it. **This is a residual risk, not a deferral** — there is no
  work being declined, so it takes no `issues/` file, and P3's deferral ledger should not flag it.
- **R5 may surface real `xdu-rm` defects (this is its purpose, not a risk to avoid).** Triage rule: a
  failure that is a stale *test expectation* gets fixed in P3; a failure exposing a genuine `xdu-rm`
  behaviour defect is written to `issues/{slug}.md` + a `ROADMAP.md` entry and left for its own pass,
  per R5. Do not repair destructive-deletion semantics inside a close-out phase.
- **Release binary-size delta is reported, not measured against a rebuilt baseline.** Measuring it
  honestly would need a second full `lto = true, codegen-units = 1` build of bundled DuckDB in a
  worktree. P2 records the cheap **debug** before/after delta (both builds happen anyway for the
  negative control; the seed observed ~+7 MB) and P3 records absolute release sizes. Saying so here so
  the missing number is a stated omission rather than an implied measurement.

## 6. Verification strategy

The governing rule for this change: **a green result is only evidence if a red result was reachable.**
Each phase therefore pairs its gate with a control.

- **P1 — poisoned release directory.** Copy `/usr/bin/false` over `target/release/xdu` and
  `target/release/xdu-rm`, run `cargo test --test rm_tests`, then remove them. Pre-fix this suite goes
  **red** (it executes the poison); post-fix it must be **green**, which is only possible if every
  binary resolves through `CARGO_BIN_EXE_*`. Paired with a static class check — `grep -rn
  'join("release")' tests/` must find nothing — so a future test cannot re-introduce the pattern.
  Run the suite with `--nocapture` at least once: per `AGENTS.md`, a self-skipping test still prints
  `ok`.
- **P2 — observe the test fail first.** Write `tests/offline_tests.rs`, run it against the *unmodified*
  `Cargo.toml`, and record the failure (a 12 MB extension appearing under the cold `HOME`, or the
  query erroring on an air-gapped box). Only then apply the one-line feature change and re-run green.
  A pass that was never preceded by a fail proves the fix works *or* that the test looks at the wrong
  directory, and those are not the same claim. Then drive the real CLI: `temp_index.sh` with
  `HOME` redirected, which additionally exercises the **release** profile.
- **P3 — the whole suite, cold, then the CI mirror.** Run the entire test suite with `HOME` pointed at
  a fresh empty directory and assert nothing is written into it. `CARGO_HOME` and `RUSTUP_HOME` must be
  preserved explicitly when doing this or cargo itself follows `HOME` and tries to re-fetch the
  registry — that is the non-obvious part. Then the CI-mirror gate: `cargo fmt --all -- --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --locked --all-features`.
  The cold directory is deliberately **not** deleted: if it is non-empty, its contents are the evidence.

R5's evidence must be taken **here**, in P3, and not earlier — P2 changes the linked dependency, so a
destructive-suite result recorded at the end of P1 would be stale by the time the branch merges. This
is the same reasoning as the GOAL's sequencing constraint, applied to the other end of the branch.
