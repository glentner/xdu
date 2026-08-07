---
slug: readers-autoload-parquet-at-runtime
title: The readers must query offline, and the tests must exercise the binary they
  just built
kind: fix
appetite: small
status: in_progress
branch: fix/readers-autoload-parquet-at-runtime
base: main
current_phase: P3
last_updated: '2026-08-07'
phases:
- id: P1
  name: rm_tests adopts tests/common (proved by a poisoned target/release)
  status: done
  satisfies:
  - R3
  - R4
  depends_on: []
  parallel: false
  hammerable: false
  hill: downhill
  verify: mkdir -p target/release && cp /usr/bin/false target/release/xdu && cp /usr/bin/false
    target/release/xdu-rm; cargo test --test rm_tests -- --nocapture; rc=$?; command
    /bin/rm -f target/release/xdu target/release/xdu-rm; grep -rn 'join("release")'
    tests/ && exit 1; exit $rc
- id: P2
  name: Statically link the Parquet reader; regression-test the cold cache
  status: done
  satisfies:
  - R1
  - R2
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: downhill
  verify: cargo test --test offline_tests -- --nocapture && COLD=$(mktemp -d) && .agents/factory/bin/temp_index.sh
    sh -c "HOME=$COLD xdu-find --count" && test -z "$(find $COLD -type f)" && echo
    COLD-HOME-CLEAN=$COLD
- id: P3
  name: Re-verify the destructive suite against the final binary; close out
  status: pending
  satisfies:
  - R5
  depends_on:
  - P2
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo fmt --all -- --check && cargo clippy --all-targets --all-features
    -- -D warnings && COLD=$(mktemp -d) && env HOME=$COLD CARGO_HOME=${CARGO_HOME:-$HOME/.cargo}
    RUSTUP_HOME=${RUSTUP_HOME:-$HOME/.rustup} cargo test --locked --all-features &&
    test -z "$(find $COLD -type f)" && echo FULL-SUITE-COLD-CLEAN=$COLD
review:
  last_reviewed_commit: ''
  verdict: none
  blocked_reason: ''
  cycle: 0
---
# TECH.md — The readers must query offline, and the tests must exercise the binary they just built

The **context engine and finite-state machine** for building this fix. The YAML frontmatter above is
the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/readers-autoload-parquet-at-runtime/TECH.md`);
the per-phase checklists below are the work.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R-IDs are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).
- **Backing research:** none — `appetite: small` with a seed-verified root cause; the three unknowns
  that could have grown it are closed in [`PLAN.md`](PLAN.md) §4.

## Conventions (apply to every phase)

- Commit conventions, code style, and load-bearing invariants come from [`AGENTS.md`](../../AGENTS.md).
  The curated footgun checklist is [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md);
  [`PLAN.md`](PLAN.md) §3 records which sections this change touches and why §1 and §10 do **not** apply.
- One phase per `xdu-build` invocation; one atomic commit containing both the change and the `TECH.md`
  state update. Subjects follow `[fix] Build readers-autoload-parquet-at-runtime P<n>: …`.
- **No `Co-Authored-By` trailer.**
- No `src/cli.rs` change is planned, so **no `doc/*.scd` update is owed** by the same-commit rule
  (PLAN §3). If a phase ends up touching `src/cli.rs`, that rule reactivates.

**The rule that governs all three phases: a green result is only evidence if a red result was
reachable.** Both defects here survived precisely because a passing gate could not distinguish the
broken state from the fixed one. Every phase below therefore runs its gate *before* the edit and
records the failure. Skipping that step forfeits the phase's evidence even if the checkboxes are all
ticked.

---

## Phase P1 — `rm_tests` adopts `tests/common`
**Satisfies:** R3, R4 · **Depends on:** —
**Goal:** every integration test resolves the binary Cargo built for the current profile, demonstrated
by making `target/release/` hostile and requiring the suite to stay green.

- [x] **Run the phase's `verify:` command first, before editing anything, and record that it fails.**
  It should fail twice over — the poisoned `/usr/bin/false` binaries get executed by the current
  resolver, and the `grep` still finds `join("release")`. This is the negative control; without it a
  later green proves nothing. Note both outcomes in the commit body.
- [x] Move `set_atime_days_ago` (`tests/rm_tests.rs:37`) into `tests/common/mod.rs`, keeping the
  `utimensat` call and its mtime-preservation comment intact. It is not a duplicate — it moves because
  a fixture living outside the shared module is what caused this defect in the first place
  (PLAN §2.3).
- [x] Add `mod common;` to `tests/rm_tests.rs` and delete the local `binary_path` (l.15),
  `create_test_file` (l.27), `build_index` (l.74) and `run_xdu_rm` (l.93).
- [x] Rewrite the call sites: `build_index(&source, &index).unwrap()` → `common::build_index(&source,
  &index)` (16 sites; the shared helper asserts internally and returns `()`), and
  `run_xdu_rm(&[…]).unwrap()` → `common::run_rm(&[…])` (~18 sites). Prune the imports that go unused
  (`File`, `Write`, `PathBuf`, `Command`, `SystemTime`, `UNIX_EPOCH`) — `clippy -D warnings` will catch
  any that are left, but do not leave them for P3.
- [x] **Change no test assertion.** If a case only passes by altering what it asserts, stop and record
  it: that is an R5 finding for P3's triage, not a migration step. Note it in the commit body so P3
  does not have to rediscover it.
- **Verify:** `mkdir -p target/release && cp /usr/bin/false target/release/xdu && cp /usr/bin/false target/release/xdu-rm; cargo test --test rm_tests -- --nocapture; rc=$?; command /bin/rm -f target/release/xdu target/release/xdu-rm; grep -rn 'join("release")' tests/ && exit 1; exit $rc`
  — `/usr/bin/false`, not `/bin/false`, which does not exist on macOS. `--nocapture` because a
  self-skipping test still prints `ok` (`AGENTS.md`, Testing). The cleanup is `command /bin/rm`, not
  bare `rm`: an interactive shell may shadow `rm` with a function that refuses, and a cleanup that
  silently no-ops leaves `/usr/bin/false` installed as `target/release/xdu` for every later phase.
- **Touches:** `tests/rm_tests.rs`, `tests/common/mod.rs`.

## Phase P2 — Statically link the Parquet reader
**Satisfies:** R1, R2 · **Depends on:** P1
**Goal:** the readers complete a query on a host with an empty extension cache and write nothing into
it, with a test that has been observed failing before the fix.

- [x] **Write the test first and watch it fail.** Add `tests/offline_tests.rs` plus the two helpers it
  needs in `tests/common/mod.rs` — `run_binary_with_home(name, home, args)` and
  `list_files_recursive(dir)` (PLAN §2.2). Build the fixture index with the normal environment (the
  crawler links no DuckDB), then run `xdu-find --count` and `xdu-rm --dry-run` with `HOME` pointed at
  an empty dir inside the test's `TempDir`. Assert exit 0, the **correct row count** (not merely
  success), and zero regular files under that `HOME`, with a failure message naming the diagnosis.
- [x] Run `cargo test --test offline_tests` against the **unmodified** `Cargo.toml` and record the
  failure, including what appeared under the cold `HOME`. This is what proves `HOME` is the right
  lever and the assertion is not vacuous.
- [x] Record `ls -l target/debug/xdu-find` now, as the pre-fix size baseline.
- [x] Apply the one-line change: `Cargo.toml:17` → `duckdb = { version = "1", features = ["bundled",
  "parquet"] }`.
- [x] Run `cargo build --locked`. **If `Cargo.lock` changes, stage it in this same commit** — CI runs
  `cargo build --locked` and `cargo test --locked` and will fail otherwise (PLAN §5).
- [x] Record the post-fix debug size and the delta, plus the wall-clock cost of the DuckDB rebuild;
  both go in the commit body. The release delta is deliberately not measured here (PLAN §5).
- [x] In `tests/offline_tests.rs`'s module doc, record why `xdu-view` is not driven (headless) and why
  that is not a coverage gap (the `parquet` feature is on the shared `duckdb` dependency, so linkage
  is crate-wide — PLAN §2.1), so a later reader does not try to run a TUI in CI.
- **Verify:** `cargo test --test offline_tests -- --nocapture && COLD=$(mktemp -d) && .agents/factory/bin/temp_index.sh sh -c "HOME=$COLD xdu-find --count" && test -z "$(find $COLD -type f)" && echo COLD-HOME-CLEAN=$COLD`
  — the second half drives the real CLI at the **release** profile, which the `cargo test` half never
  touches.
- **Touches:** `Cargo.toml`, `Cargo.lock` (if resolution changes), `tests/offline_tests.rs`,
  `tests/common/mod.rs`.

## Phase P3 — Re-verify the destructive suite against the final binary; close out
**Satisfies:** R5 · **Depends on:** P2
**Goal:** the `xdu-rm` safety behaviours are re-established as evidence — taken after the last change
to the binary, not before it — and every deferral this branch made has a home.

**Why R5 lands here and not at the end of P1:** P2 changes the linked dependency, so a destructive-suite
result recorded at the end of P1 would be stale by the time the branch merges. The GOAL's sequencing
constraint applies to both ends of the branch.

- [ ] Run the `verify:` command and confirm the **whole** suite passes with `HOME` pointed at a fresh
  empty directory and nothing written into it. Note that `CARGO_HOME`/`RUSTUP_HOME` are preserved
  explicitly — without that, cargo itself follows the redirected `HOME` and tries to re-fetch the
  registry. The cold directory is intentionally left on disk: if it is non-empty, its contents are the
  evidence.
- [ ] **R5 triage.** Review all 16 `rm_tests` outcomes against the final binary, including anything P1
  flagged. A failure that is a stale *test expectation* gets fixed here. A failure exposing a genuine
  `xdu-rm` behaviour defect gets an `issues/{slug}.md` (from
  [`templates/ISSUE.md`](../../.agents/factory/templates/ISSUE.md)) plus a `ROADMAP.md` entry, and is
  left for its own pass — do not repair destructive-deletion semantics inside a close-out phase
  (GOAL non-goals; PLAN §5).
- [ ] Record absolute release binary sizes (`cargo build --release`; `ls -l target/release/xdu-find
  target/release/xdu-rm target/release/xdu-view`) in the commit body, and state plainly that the
  release *delta* was not measured against a rebuilt pre-fix baseline and why (PLAN §5). A stated
  omission, not an implied measurement.
- [ ] Housekeeping: delete the `ROADMAP.md` entry "The readers need the network on first run, and
  `rm_tests` tests the wrong binary" — `ROADMAP.md` is the forward-looking index, and a landed item
  left in it is a false backlog — and set the `status:` line in
  `issues/readers-autoload-parquet-at-runtime.md` to resolved, naming this branch.
- [ ] **Deferral ledger.** Walk P1 and P2 for "do not fix here", "known limitation", "follow-up" and
  confirm each has a matching `issues/` file and `ROADMAP.md` entry. Two things are deliberately
  **not** deferrals and need no file: the rejected runtime `autoload` disable (PLAN §2.5 — a rejected
  alternative) and the residual autoload exposure (PLAN §5 — a stated risk with no work being
  declined). An unrecorded deferral is a phase failure, not a tidy-up.
- **Verify:** `cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && COLD=$(mktemp -d) && env HOME=$COLD CARGO_HOME=${CARGO_HOME:-$HOME/.cargo} RUSTUP_HOME=${RUSTUP_HOME:-$HOME/.rustup} cargo test --locked --all-features && test -z "$(find $COLD -type f)" && echo FULL-SUITE-COLD-CLEAN=$COLD`
- **Touches:** `ROADMAP.md`, `issues/readers-autoload-parquet-at-runtime.md`, possibly new
  `issues/*.md`, possibly `tests/rm_tests.rs` (stale expectations only).

---

## How `xdu-build` drives this

1. `next_phase.py` prints the next actionable phase (statuses are authoritative).
2. Pre-flight: clean tree, on `fix/readers-autoload-parquet-at-runtime`, `main` reachable.
3. Execute every `[ ]` in the phase (consult [`PLAN.md`](PLAN.md) for detail).
4. Run the phase's `verify:` command — never advance on a checkbox alone, and never on a green that
   was not preceded by the recorded red.
5. Amend this file freely if reality diverges (regenerate frontmatter with `set_phase.py`; note the
   amendment in the commit body). STOP and escalate only on a **`GOAL.md` contradiction**.
6. Mark the phase `done`, advance `current_phase`, `--touch`; one `[fix]` commit; stop and report.
