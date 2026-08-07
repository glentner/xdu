# REVIEW — The readers must query offline, and the tests must exercise the binary they just built

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** `5ee1fa9e3726c32f119eda41fcc6d57e250537bc`  ·  **Base:** `main`  ·  **Date:** 2026-08-07
- **Verdict:** approved
- **Cycle:** 1 of ≤3 — mirrors `review.cycle` in `TECH.md`

**Method.** Blind correctness pass delegated to a fresh `general-purpose` subagent given `GOAL.md`,
`invariants.md`, the rubric, and `git diff main...HEAD -- . ':(exclude)spec/'`. It confirmed it read no
`spec/` content and handed back with `git status --porcelain` empty. The orchestrator re-verified the
load-bearing claims independently (see *Orchestrator spot-checks*). No R-ID on this branch is satisfied
by a `spec/`-only artifact, so the reviewer graded all five — nothing was split across contexts.

**Contract drift:** none. `git log --oneline main..HEAD -- spec/{slug}/GOAL.md` returns only the
original shaping commit `bae9d04`, so the contract graded here is the one that was locked.

**Man-page gate:** not triggered — `git diff --stat main...HEAD -- doc/ src/` is empty, so no `.scd`
render was owed. (`scdoc` **is** installed on this host at `/opt/homebrew/bin/scdoc`, so the gate was
available had it been needed; this is not an unclosed tooling gap.)

## Verification run

Commands actually executed and their outcomes (the spine of the review):

- `cargo fmt --all -- --check` → **pass** (exit 0)
- `cargo clippy --locked --all-targets --all-features -- -D warnings` → **pass** (exit 0)
- `cargo test --locked` → **pass** — 63 lib + 23 crawl + 1 offline + 16 rm, 0 failed
- `cargo build --locked --all-features` / `cargo test --locked --all-features` (CI mirror,
  `.github/workflows/test.yaml:86,89`) → **pass**
- `cargo metadata --locked --format-version 1` → **pass** — `Cargo.lock` consistent with the edited
  `Cargo.toml`; `duckdb@1.4.4` + `libduckdb-sys@1.4.4` resolve with `parquet`. (The lock is
  legitimately unchanged in the diff: Cargo.lock does not record feature selections.)
- **R3 negative control** — `cp /usr/bin/false target/release/{xdu,xdu-rm}` then
  `cargo test --locked --test rm_tests -- --nocapture` → **16 passed; 0 failed** under a hostile
  release tree. Poison removed with `del` (confirmed in `del --list`), then
  `cargo build --release --locked` restored genuine artifacts (verified genuine by size + symbol
  count, not by assumption).
- **R5 skip audit** — `cargo test --locked --test rm_tests -- --nocapture | grep -i skipping` → **no
  matches**. All 16 destructive/`--safe`/`--limit` cases genuinely executed; none self-skipped.
  (AGENTS.md: a self-skipping test still prints `ok`.)
- **R2 non-vacuity, lever proven** — `HOME=$COLD duckdb -c "FORCE INSTALL json;"` wrote
  `$COLD/.duckdb/extensions/v1.5.5/osx_arm64/json.duckdb_extension{,.info}` — exactly the shape
  `list_files_recursive` reports. So the assertion can observe a cache write.
- **R2 non-vacuity, pre-fix reproduction** — a scratch crate pinned to
  `duckdb = {version = "=1.4.4", features = ["bundled"]}` (bundled **only**), driven as
  `HOME=$COLD ./prefix-check '<index>/*/*.parquet'` → returned `COUNT=3` **and** left
  `parquet.duckdb_extension{,.info}` in `$COLD`. The offline test fails against the pre-fix dependency
  line; its green is therefore evidence.
- **R2 lever exclusivity** — DuckDB resolves the cache via `config.options.extension_directory` →
  `FileSystem::GetHomeDirectory()` → `home_directory` setting → `getenv("HOME")`
  (`src/common/file_system.cpp:348-364`, `src/main/extension/extension_install.cpp:61-90`); no env
  override exists, and `grep -rn "extension\|autoload\|INSTALL" src/` shows xdu sets neither setting.
  `HOME` is the only lever, so the empty-dir assertion cannot be bypassed.
- **R1 release-profile cold drive** — release binaries with a fresh empty `$HOME`:
  `xdu-find -i IDX --count` → `3`; `xdu-rm -i IDX -p '\.log$' --dry-run` → `2 file(s) would be
  deleted`; plus `-f json`, `-f csv`, `--top 2`, `-p '\.log$' -f path`, `-u user1 --count`,
  `-p nomatch --dry-run` — all correct, and `find $COLD -type f` empty afterwards. Also correct with
  `HOME` pointed at a **nonexistent** directory (the pre-fix path throws `Can't find the home
  directory`), and that directory was not created.
- **Static linkage, per binary** —
  `strings -a target/release/{xdu,xdu-find,xdu-rm,xdu-view} | grep -c parquet_kv_metadata` → `0,2,2,2`;
  `nm -u target/release/xdu | grep -ci duckdb` → `0`. This is what closes the `xdu-view` coverage
  question: the TUI links the reader even though CI cannot drive it, and it independently confirms the
  offline test's premise that the crawler links no DuckDB.
- `.agents/factory/bin/temp_index.sh sh -c 'xdu-find --count; xdu-rm --dry-run -p "\.log$" --force'`
  → `4`, then `1 file(s) would be deleted` (throwaway index, never a real one).
- `git ls-files -i -c --exclude-standard` → **empty** — the new `.gitignore` rules mask no tracked file.
- `del --help`; `del -r dir1`; `del -rf dir1`; `env -u HOME del victim.txt`; `del missing` → all four
  `AGENTS.md` `del` claims reproduce exactly (exit 0 with nothing deleted; exit 2; `.Trash/` +
  `.Trash.db` in cwd; exit 0).

## Requirement → evidence matrix

All five R-IDs were verified by the blind reviewer (no `spec/`-only artifact deliverables on this
branch); the orchestrator re-verified the structural claims marked ✔ below.

| R-ID | Implemented by | Verified how | Status |
|------|----------------|--------------|--------|
| **R1** — readers query a cold/air-gapped cache successfully, same results as warm | `Cargo.toml:19` (`features = ["bundled", "parquet"]`) ✔ | Release binaries driven across 7 flag combinations with a fresh empty `$HOME` and with a nonexistent `$HOME` — all correct, `$COLD` empty after. Linkage confirmed per binary via `strings`/`nm` (`xdu-find`/`xdu-rm`/`xdu-view` = 2 hits each; `xdu` = 0). | ✅ |
| **R2** — a CI test points the cache at an empty dir and requires it to stay empty | `tests/offline_tests.rs:31-89`; helpers `tests/common/mod.rs:137` (`run_binary_with_home`), `:154` (`list_files_recursive`) ✔ | `cargo test --locked --all-features` → offline_tests 1 passed; runs in CI at `.github/workflows/test.yaml:89` ✔. Non-vacuity proved twice: the lever writes observable files (`FORCE INSTALL json`), and a bundled-only scratch build reproduces the download the test forbids. Asserts the **row count** (`3`) and the `xdu-rm` match count (`2`), not exit 0 ✔. | ✅ |
| **R3** — tests invoke the current-profile artifact; no `target/release/` probing | `tests/common/mod.rs:22-29` — compile-time `env!("CARGO_BIN_EXE_*")` ✔ | `grep -rn 'join("release")\|target/release' tests/` → only two doc-comment prose hits, **zero** path probing ✔. Negative control: 16/16 `rm_tests` pass with `/usr/bin/false` installed over `target/release/{xdu,xdu-rm}`. | ✅ |
| **R4** — `rm_tests.rs` sources resolution + fixtures from `tests/common`, no duplicate locals | `tests/rm_tests.rs:10` (`mod common;`), `:17` (`use common::{build_index, create_test_file, run_rm, set_atime_days_ago};`) ✔ | `grep -n "^fn " tests/rm_tests.rs` → only the 16 `test_*` fns; `binary_path`, `create_test_file`, `set_atime_days_ago`, `build_index`, `run_xdu_rm` all gone. `set_atime_days_ago` moved verbatim to `tests/common/mod.rs:46` ✔. Refactor is assertion-preserving: `diff <(git show main:tests/rm_tests.rs \| grep assert) <(grep assert tests/rm_tests.rs)` → **identical**; 77 assertions and 16 `#[test]` on both sides. | ✅ |
| **R5** — full `rm_tests` passes under R3 resolution; any newly-exposed failure fixed or recorded | — (nothing to record) | `cargo test --locked --test rm_tests -- --nocapture` → 16 passed, 0 failed, **0 self-skipped**. R5's conditional never fires, so no `issues/` + `ROADMAP.md` entry was owed. Verified on macOS/APFS only — the Linux CI runner was not exercised locally (CI will). | ✅ |

**Unmapped changes (possible scope creep) — three, all benign, one worth the human's eye at publish:**

1. **`AGENTS.md` — the two new `del`/`rm` bullets** (from `[harness]` commit `5ee1fa9`). Maps to no
   R-ID; this is `/xdu-harness`-lane content riding on a `fix/` branch, so a squash merge lands a
   harness policy change under a `[fix]` PR subject. **Content is accurate** — every testable claim
   was reproduced (`-r` is `--restore` and deletes nothing at exit 0; no `-f`, and `del -rf` exits 2;
   a `HOME`-less `del` writes `.Trash/` + `.Trash.db` into the cwd; a missing path is exit 0; `rm` is
   a refusing shell function here). It **weakens no invariant** — the "…deletions that stay `rm`"
   carve-out explicitly preserves `xdu-rm`, `install.sh`, `Dockerfile`, CI and `bench/`. Not a
   blocker; noted so `/xdu-publish` describes it in the PR body rather than burying it.
2. **`.gitignore:9-13` — `.Trash/` + `.Trash.db`.** Maps to no R-ID. Masks nothing tracked
   (`git ls-files -i -c --exclude-standard` empty). One trade-off the comment only half-states: the
   rule prevents an accidental commit, but it also makes a `HOME`-less `del` in the repo root
   invisible to `git status`, so real trashed files would accumulate silently rather than showing up
   as untracked. Defensible either way.
3. **`ROADMAP.md` (entry removed) + `issues/readers-autoload-parquet-at-runtime.md:2`
   (`unshaped` → `resolved`).** Housekeeping for the issue this branch closes; accurate. Two
   non-blocking nits: `templates/ISSUE.md` documents `unshaped` as the guard value and defines no
   `resolved` state (no precedent exists — `git log --diff-filter=D -- issues/` is empty, this is the
   first issue ever closed), and `AGENTS.md` calls `ROADMAP.md` "the ordered index — one entry per
   issue", now literally false (7 issue files, 6 roadmap entries). Retaining the file with a resolved
   status is the better of the two options; the `AGENTS.md` sentence is the slightly stale part.

**Not scope creep:** the `AGENTS.md` Testing-section rewrite and the repository-map line naming
`offline_tests.rs` are the same-commit manual updates R2/R3/R4 owe. Each assertion was checked — all
three `tests/*.rs` carry `mod common;`, and `binary_path` is `CARGO_BIN_EXE_*`.

## Findings

One finding; **no CONFIRMED findings**. Nothing else survived refutation.

### [LOW/PLAUSIBLE] The offline-linkage property is enforced only by a test, not by the invariant gate

- **Where:** `.agents/factory/invariants.md` (whole file — **unchanged** by this diff);
  `Cargo.toml:19` is the property it fails to cover.
- **Failure scenario:** a later cycle edits the `duckdb` dependency line (crate bump, feature prune
  for build time, a move off `bundled`) or widens the readers' SQL surface beyond
  `read_parquet`/`regexp_matches`. `/xdu-plan`'s invariant walk has no section to trip on, so the
  design gate passes silently. The property is load-bearing precisely because
  `libduckdb-sys/build.rs` still compiles with `DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT=1` and
  `DUCKDB_EXTENSION_AUTOLOAD_DEFAULT=1` — offline operation rests entirely on "the one extension we
  need is statically linked."
- **Evidence:** `grep -rn "parquet\|offline\|extension" .agents/factory/invariants.md` → **no
  matches**. `sed -n '100,140p' ~/.cargo/registry/src/*/libduckdb-sys-1.4.4/build.rs` →
  `#[cfg(feature = "parquet")] add_extension(…)` and both autoload defines set to `"1"`.
- **Why LOW and PLAUSIBLE, not blocking:** this is a *narrowed checklist*, not an unguarded
  regression — `tests/offline_tests.rs` catches the same regression in CI, which is exactly what R2
  contracts and what the GOAL's second clarification asked for. It is also not drift *between*
  `AGENTS.md` and `invariants.md`: `AGENTS.md`'s numbered invariants section was not touched either,
  so the two remain in lockstep. Adding a new numbered invariant is a change to `.agents/`, which
  AGENTS.md assigns to the **human-gated `/xdu-harness` lane** — not to a `fix/` source branch, and
  not something this review should force into scope.
- **Touches:** rubric scope item 5 (operating-manual drift), adjacent to R1/R2.

**Dropped after refutation** (recorded so a later cycle does not re-litigate them): `-f json` output
triggering the DuckDB `json` extension — disproved, the escaping is Rust-side at
`src/bin/xdu-find.rs:167`; the offline test passing vacuously — disproved twice over (see R2);
`xdu-view` being an uncovered reader — disproved, `strings` shows it links the extension;
`list_files_recursive` missing a directory-only write — dissolves, because a failed download also
fails the query and the test asserts the row count.

## Human-gate triggers

**None.** There are no CONFIRMED findings, and the diff touches **no** `src/` file at all
(`git diff --stat main...HEAD -- src/` is empty) — so no high-blast-radius path
(`src/bin/xdu-rm.rs`, `src/bin/xdu.rs`, `src/crawl.rs`, `src/lib.rs`, `src/cli.rs`) and no
destructive-rm / schema-stability / atomic-write / SQL-injection invariant (§4/§1/§2/§5) is in play.
The single finding is LOW/PLAUSIBLE against a `.agents/` file, which routes to `/xdu-harness` for
triage rather than gating this branch.

## Orchestrator spot-checks

Independent of the reviewer, to confirm its structural claims rather than take them on trust:

- `git status --porcelain` → empty on hand-back ✔ (the reviewer poisoned and then restored
  `target/release/`; that is untracked, git-ignored build state, and it verified the restore rather
  than assuming it)
- `Cargo.toml:19` carries `["bundled", "parquet"]`; `Cargo.lock` correctly absent from the diff ✔
- `tests/rm_tests.rs:10,17` — `mod common;` present, helpers imported ✔
- `grep -rn 'join("release")\|target/release' tests/` → doc-comment prose only ✔
- `tests/common/mod.rs:22-29` — `binary_path` is compile-time `env!("CARGO_BIN_EXE_*")` with an
  explicit `panic!` on an unknown name ✔
- `tests/offline_tests.rs:55-59,76-79,82-88` — asserts row count `3`, the `xdu-rm` match count, and
  an empty cold `HOME` with a diagnosis-naming failure message ✔
- `.github/workflows/test.yaml:89` runs `cargo test --locked --all-features`, so R2's "runs in CI"
  clause holds ✔
- `git diff --stat main...HEAD -- doc/ src/` empty → man-page gate genuinely not owed ✔

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

Not run — `/xdu-review` was invoked without the `completeness` argument. Phase-shipment coverage is
partially evidenced anyway: all three `TECH.md` phases report `done`, and the correctness pass
independently verified the artifact each phase was to produce (P1 → R3/R4, P2 → R1/R2, P3 → R5).
