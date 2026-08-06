# REVIEW — Harden & optimize the index-build crawl

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** e1f5d7e93138ed38b607bb6097c67ca720fc6979 · **Base:** main · **Date:** 2026-08-04
- **Verdict:** changes-requested
- **Cycle:** 1 of ≤3
- **Mode:** full blind pass over the spec-excluded diff (`git diff main...HEAD -- . ':(exclude)spec/'`).
- **Contract drift:** none. `git log --oneline main..HEAD -- spec/crawl-hardening/GOAL.md` returns only
  the original shaping commit `f6be759`, so this review graded the contract as locked.

## Verification run

Commands actually executed by the blind reviewer (spot-checked by the orchestrator against the source).

**Pre-release gate (R10) — all three clean at `e1f5d7e`:**

- `cargo fmt --all -- --check` → exit 0, no output
- `cargo clippy --all-targets --all-features -- -D warnings` → exit 0, **zero** warnings
- `cargo test` → exit 0 — 63 lib + 18 `crawl_tests` + 16 `rm_tests` = **97 passed, 0 failed**
- `cargo test -- --nocapture` → exactly **one** silent self-skip: `test_non_utf8_path_is_counted_and_reported`
  (APFS rejects the name). Both unreadable-subtree / `--allow-errors` cases **did** execute.

**CLI drives (all against throwaway `mktemp -d` trees and indexes):**

- **File accounting vs ground truth** — 200-wide fan-out + 40-deep nesting + 3 loose root files (incl.
  spaces and a `'`) + empty dirs + hard links + a unicode name; `find -type f | wc -l` = 246. Crawled
  at `-j {1,2,8}` × `-B {1,3,100000}` = 9 combinations → **246 rows every time**, per-partition counts
  matching per-directory ground truth, full path-set `diff` vs `find` clean, 0 `.partial` left.
- **Parquet identity vs `main`** — built `main`'s `xdu` in a `git worktree` (separate target dir) and
  indexed the same tree with both binaries in all three size modes: `parquet_schema()` identical
  (`path BYTE_ARRAY/UTF8 REQUIRED, size INT64 REQUIRED, atime INT64 REQUIRED`, in order), **every chunk
  file byte-identical**, `EXCEPT` both ways = 0 rows. Worktree removed and pruned.
- **Error paths** — unreadable top-level partition → exit 1, `error: …/secret: Permission denied`,
  **stdout 0 bytes**, no marker, sibling partition still indexed. Unreadable root → exit 1. Unreadable
  nested dir (depth > 1) → exit 1, path named, no marker. Mode-000 *file* → exit 0, indexed (correct —
  `stat` still works). `--allow-errors` → exit 0, marker written with `errors=1`.
- **Vanished-file race** — 200 000 files across 8 partitions with a concurrent unlinker: **1756 real
  ENOENT races**, exit 0, marker records `vanished=1756 errors=0`, zero `error:` lines.
- **Pruning** — 20 files at `-B 3` (7 chunks) → re-index a 3-file tree → **1 chunk, 3 rows**, 0 partials.
- **Marker/glob** — dotfile at the index root, not matched by `*/*.parquet`, never indexed
  (`grep -c xdu-complete` on `-f path` = 0); a markerless index still queries, warning on **stderr**
  only, stdout still just a number.
- **Injection (§5)** — `-u "…') UNION ALL SELECT 'PWNED'…"` and `-p "x') OR 1=1 --"` → DuckDB parser
  errors, no data leak. `lib::index_glob` emits the byte-identical `format!` `main` used, so the
  surface is unchanged (neither improved nor worsened).
- **Bench** — `sh bench/run.sh smoke` → `smoke ok: generated 104 files, indexed 104, marker present`.
  `gen_tree.py`'s self-report verified exact against `os.walk`. Reviewer also ran
  `sh bench/run.sh s3 --scale 4 --reps 5` independently (see F2).
- **Regressions are real** — reproduced the pre-fix bugs on `main`: unreadable subtree → **exit 0**
  with 1 of 2 files and no diagnostic; a real top-level `__root__` dir + loose files → **6 of 9 files
  indexed, exit 0**, nondeterministic. Both are fixed on this branch.
- **CLI ↔ man** — `xdu --help` and `doc/xdu.1.scd` list an identical flag set; `--allow-errors`
  documented; every pre-existing default confirmed unchanged (`-j 4`, `-B 100000`). `scdoc` is not
  installed on this host, so the man page could not be **rendered** — flag-set parity was checked by
  inspection instead. *(Gap in the evidence, noted rather than papered over.)*

## Requirement → evidence matrix

| R-ID | Implemented by | Verified how | Status |
|------|----------------|--------------|--------|
| R1 | `src/crawl.rs:179-200` (`EntryError`/`classify_io_error`), `src/bin/xdu.rs:27-43` (concurrency-contract comment) | Audit artifact is `research/01-concurrency-audit.md` — outside the blind reviewer's inputs, confirmed present by the orchestrator. Code-side output verified: hazards named in declarative comments, classified in the type system, **3 distinct real bugs** reproduced on `main` and absent on HEAD. | ✅ |
| R2 | `src/crawl.rs:212-289` (`__root__` + dup-name guards), `src/bin/xdu.rs:237-325` (walk / `read_children_error` / stat classification); tests `tests/crawl_tests.rs:279,322,559,600` | Reproduced each bug on `main` and its absence on HEAD (exit status, row counts, stderr tokens). Tests drive the real binary and assert concrete post-conditions, not exit 0. | ✅ |
| R3 | `src/bin/xdu.rs:609-618`, `:58`; `src/crawl.rs:51-86` | 6 executed drives: non-zero exit + named path + no marker for permission/IO at root, top-level and nested depth; benign ENOENT (1756 races) exits 0. Driver-panic path reasoned only (`panic="abort"`; `join()` maps `Err(_)`), not executed. | ✅ (doc defect → F3) |
| R4 | `bench/gen_tree.py`, `bench/run.sh`, `bench/scenarios.md`, `bench/results/baseline.json` | `smoke` passes and asserts row count == generated (not exit 0); `run.sh:273` fails a measurement whose index row count ≠ generated; generator self-report exact; baseline committed with commit/host/FS. | ✅ |
| R5 | `src/crawl.rs:291-418` (direct-to-Arrow builders), `src/bin/xdu.rs:302` (`entry.metadata()`), `bench/scenarios.md:119-179` | Arithmetic on the committed JSONs: vs `comparison-pre-p5` = −27.7/−15.3/−7.0/−6.5/−12.0/+0.0 %, but **vs the R4 `baseline.json` flat-or-slower in 5 of 6 configs**, and two captures of *identical* `src/` differ 8.9–18.5 %. Rejected lever honestly recorded; ceiling documented. | ⚠️ **partial → F2** |
| R6 | new `src/crawl.rs` (454 non-test lines lifted into lib, 16 unit tests), `src/bin/xdu.rs:27-43` | Schema and behavior unchanged proven at the **byte level** vs `main` (chunks byte-identical across all 3 size modes incl. `__root__`). No `R#`/`P#` ids in source (`grep` clean). Note `xdu.rs` went 611 → 631 lines — testable logic moved out, error handling moved in; the bin did not net-shrink. | ✅ |
| R7 | `src/cli.rs:53-60` (`--allow-errors`), `doc/xdu.1.scd:53-56,104-130` | One new flag, opt-in bool defaulting false, documented on the same branch; `--help` ↔ man-page flag sets identical; every pre-existing default confirmed unmoved. | ✅ |
| R8 | `spec/crawl-hardening/ASSESSMENT.md`; cleanups at `src/lib.rs:22-64` (`ROOT_PARTITION`, `COMPLETION_MARKER`, `index_glob`, `index_completion_warning`) applied across all 3 readers; follow-ups in `ROADMAP.md:149-163` + ASSESSMENT.md "Deferred" | 8 hand-built `format!` glob sites collapsed to one `lib::index_glob`; duplicate `ROOT_PARTITION` deleted from `xdu-view.rs`. Readers verified to still find every chunk. Deferred items each carry a stated reason. | ✅ (one omission → F5) |
| R9 | `bench/HPC-PROTOCOL.md` | Read in full: scope §1, inputs §2, environment + per-FS tables §3, method §4 (incl. load warning + correctness-verification step), metrics §5 (per-FS md-op counters), expected result shape §6, fill-in template §7. Actionable as written. | ✅ |
| R10 | — | §1 byte-identical Parquet; §2 partial→rename + prune verified (7 chunks → 1, 0 partials); §3 `__root__` incl. the new collision guard; §6 `MetadataExt` retained; §7 single pool + `Mutex<VecDeque>` + `thread::scope` first-error preserved; §8 symlink exclusion by drive and test; §13 stdout clean (0 bytes on the failing run). Full gate clean. | ✅ |

**Unmapped changes (possible scope creep):** none blocking.

| Change | Judgment |
|--------|----------|
| `.gitignore` + `/.idea/` (own `[chore]` commit `c613634`) | Maps to no R-ID. Trivial, isolated, benign. |
| `.gitignore` `bench/results/*` + `!baseline.json` + `!comparison-*.json` | In scope — R4 needs the reference committed, per-machine runs ignored. |
| `AGENTS.md` (bench section, test-skip warning, `tests/common/mod.rs`) | In scope — R4/R6 bookkeeping. The "check with `--nocapture`" note is borne out: one test does silently skip here. |
| `ROADMAP.md` +16 | In scope — R8 mandates recording larger/riskier items as explicit follow-ups. |

All four stated non-goals respected: no schema change, no backend work, no new query/deletion
features, no changed default or repurposed flag.

## Findings

Severity: **CRITICAL** (any `invariants.md` §1–§12 violation is auto-CRITICAL; §13 is **HIGH**) ·
**HIGH** · **MEDIUM** · **LOW**. Verdict: **CONFIRMED** (reproduced) vs **PLAUSIBLE** (needs human
triage). No CRITICAL or HIGH finding survived refutation; no invariant §1–§12 violation was introduced
by this branch.

### F1 — [MEDIUM/CONFIRMED] A run rejected *before writing anything* still strips a previously-complete index's marker
- **Where:** `src/bin/xdu.rs:58` (`clear_completion_marker(outdir)?` is the first statement of `crawl()`)
- **Failure scenario:** An operator holds a complete 400 M-file index at `/idx`. They fat-finger the
  source path to a directory with no subdirectories. `xdu` exits 1 with `No partitions found`; `/idx`
  is byte-for-byte unchanged and still complete — but the marker is already gone, so every subsequent
  `xdu-find`/`xdu-view`/`xdu-rm` prints `has no completion marker … results may be incomplete`.
  Nothing restores it but a full re-index. The same holds for the new `__root__` reserved-name
  rejection, an unreadable root, and a thread-pool build failure — all of which `bail` *after* the
  marker is dropped but *before* any chunk is touched.
- **Evidence:**
  ```
  after good run: marker=YES rows=2
  A) empty source tree, same outdir → exit=1 "No partitions found"
       marker now: NO   ← stripped;  rows still: 2 (data untouched)
       reader stderr: warning: …/idx has no completion marker (.xdu-complete)…
  B) via the __root__ reserved-name rejection → exit=1 "reserved"
       marker now: NO   ← stripped;  rows still: 2
  C) contrast: -p ghost is validated in main() before crawl() → marker survives: YES
  ```
  Orchestrator confirmed by reading `src/bin/xdu.rs:45-58`: the clear precedes `fs::read_dir(top_dir)`
  and `build_work_queue`.
- **Touches:** R3 ("SHALL NOT silently finalize a partial index as if it were complete" — this is the
  mirror-image failure: a complete index flagged as suspect). The in-code comment states the
  fail-safe rationale, which is right — the defect is only the guard's *placement* relative to
  validation, not the idea. `tests/crawl_tests.rs:279` asserts `!marker.exists()` on a *fresh* index
  only, so it cannot catch this.
- **High-blast-radius file:** `src/bin/xdu.rs` → **human gate**.

### F2 — [MEDIUM/CONFIRMED] The shipped perf change is quantified against a non-baseline capture, and `scenarios.md` claims more than the numbers establish
- **Where:** `bench/scenarios.md:131-139` + `bench/results/*.json`
- **Failure scenario:** R4 requires the committed baseline "so any performance change is quantified
  against it rather than asserted"; R5 forbids merging a change "that does not measurably help … as if
  it did". `scenarios.md` claims "real wins on the flat-wide, many-partition and mid-`--jobs` mixed
  shapes, no measured regression anywhere … measured … on the same machine, back to back." Measured
  against `baseline.json` instead of `comparison-pre-p5.json`, the shipped build is flat-or-slower in
  **5 of 6** configurations.
- **Evidence:**
  ```
  cfg          pre-p5  shipped   delta% |  baseline  base-vs-pre%
  ('s2', 4)      3.75     2.71    -27.7 |      3.17        -15.5
  ('s3', 4)      1.57     1.33    -15.3 |      1.28        -18.5
  ('s5', 1)      6.44     5.99     -7.0 |      5.87         -8.9
  ('s5', 2)      3.97     3.71     -6.5 |      3.59         -9.6
  ('s5', 4)      2.84     2.50    -12.0 |      2.49        -12.3
  ('s5', 8)      2.64     2.64     +0.0 |      2.61         -1.1
  ```
  - **The noise floor swamps most of the claimed wins.** `git diff --stat b8f5f9c c9630c0 -- src/` is
    **empty** (orchestrator re-verified) — `baseline.json` and `comparison-pre-p5.json` measure
    *identical crawl source* on the same host (Apple M4 Max, apfs) 45 minutes apart, yet differ
    **8.9–18.5 %**. `scenarios.md:84-86` nonetheless calls the numbers "reproducible and good for
    detecting a regression between two commits".
  - **Independent re-measurement:** `git diff 7bad497 e1f5d7e -- src/crawl.rs` is a pure comment /
    `pub use` relocation, so HEAD's crawl is the code in `comparison-l2-only.json`. Same host, same
    config (`sh bench/run.sh s3 --scale 4 --reps 5`) → **median 1.73 s** (`[1.71,1.77,1.67,1.73,1.73]`)
    vs the committed **1.33 s** — 30 % apart.
  - **"back to back" is inaccurate:** `captured_at` = baseline `18:49:52`, pre-p5 `19:34:35`, shipped
    `00:52:33` **the next day** (orchestrator re-verified).
  - `comparison-l1-l2.json` and `comparison-l2-only.json` carry `git_dirty: true` with the change
    **in `src/`**, while `run.sh`'s auto-generated note asserts "uncommitted changes outside `src/` do
    not affect it" — false for exactly those two documents.
- **Touches:** R5, R4. The *direction* is consistent across all 6 configs and the `s2` flat-wide win
  (−27.7 % vs pre-p5, −14.5 % vs baseline) clears the drift, so the change plausibly does help — the
  defect is that the recorded evidence overclaims. The honest statement is "one shape improves
  measurably; the rest are within this harness's run-to-run drift", plus either a stated noise floor
  or enough reps to establish one.

### F3 — [LOW/CONFIRMED] The man page's EXIT STATUS section describes early-abort behavior the code does not have for the error class it is about
- **Where:** `doc/xdu.1.scd:113-114`
- **Failure scenario:** The text says a failing run "leaves no completion marker …, **and the remaining
  threads stop taking on new partitions once one has failed.**" The `cancel` flag
  (`src/bin/xdu.rs:138,192,469`) is raised **only** when `drain_queue()` returns a hard `Result` error
  (e.g. a Parquet write failure). A permission/I-O *entry* error — the exact case that paragraph
  introduces — increments `part_errors`, is reported, and the crawl continues: every partition is
  walked and finalized, and the run fails only in `main()` afterwards. An operator on a billion-file
  tree reads this, expects a bail on the first `EACCES`, and budgets minutes instead of a full walk.
- **Evidence:**
  ```
  unreadable TOP-LEVEL partition dir (default) → exit=1
    error: …/tree/secret: Permission denied (os error 13)
    Finished secret (0 files, 0 B, 1 errors)
    Finished ok (1 files, 0 B)   ← the other partition was still taken on and finalized
    marker present: NO
  ```
  Orchestrator confirmed by `grep -n cancel src/bin/xdu.rs`: set at `:469` only, inside the `Err` arm.
- **Touches:** invariant §10 (man page describes the code) — doc-only; the *code* behavior is correct
  and R3-compliant.

### F4 — [LOW/CONFIRMED] An `--allow-errors` index is knowingly incomplete yet carries the completion marker, and readers only test the marker's existence
- **Where:** `src/bin/xdu.rs:620-628` + `src/lib.rs:52-64` (`index_completion_warning` calls only `.exists()`)
- **Failure scenario:** The marker body records `errors=N` (`src/crawl.rs:68-79`), but nothing ever
  reads the count. An index built weeks ago with `--allow-errors` is indistinguishable at query time
  from a clean one — which matters most for `xdu-rm`, whose risk model is precisely "files the index
  does not know about".
- **Evidence:**
  ```
  --allow-errors run → exit=0, marker present: YES
    marker body: xdu=0.4.1 / files=1 / errors=1
    reader warns on --allow-errors index? → (nothing; xdu-find is silent)
  ```
- **Counter-argument for triage:** `--allow-errors` is the operator explicitly electing to accept
  incompleteness, so "marker present, count recorded in the body" is a defensible design. Reasonable
  to close as won't-fix; recorded because the branch introduced the signal and this is its one gap.
- **Touches:** R3 (the "never present a partial index as complete" spirit).

### F5 — [LOW/CONFIRMED · pre-existing, NOT introduced by this branch · non-blocking] `xdu-view` terminal restore is sequential, with no `Drop` guard and no panic hook
- **Where:** `src/bin/xdu-view.rs:1865-1874`
- **Failure scenario:** `enable_raw_mode()`/`EnterAlternateScreen` at 1865-1866 and
  `disable_raw_mode()`/`LeaveAlternateScreen` at 1873-1874, with `run_app` between. Any panic (or an
  early `?`) inside `run_app` leaves the user's terminal in raw mode inside the alternate screen —
  invariant §12, with `panic = "abort"` set.
- **Evidence:** `git show main:src/bin/xdu-view.rs` is **identical** here, so the branch did not
  introduce it; no `panic::set_hook`, no `impl Drop`, no `TerminalGuard` anywhere in the file. The
  terminal-handling code is untouched by this diff.
- **Why it is recorded at all:** R8 contracted an assessment of the readers. `ASSESSMENT.md`'s
  "Deferred" list names `strip_ansi`'s test value and calls it load-bearing for §12, but does not
  record this restore-path gap; `ROADMAP.md:149-163` likewise. It belongs on the follow-up list.
  **This does not block the branch** — it is a pre-existing defect in code this diff does not touch,
  and the rubric forbids manufacturing findings against untouched subsystems.

### Dropped under refutation

Recorded so the human can see what was considered and disproven:

- *Direct-to-Arrow rewrite silently changed nullability / field order / values* — refuted: every chunk
  **byte-identical** to `main`'s across all three size modes; `parquet_schema` identical; `EXCEPT` both
  ways = 0 rows.
- *File lost or double-counted under concurrency* — refuted: 9 `-j`/`-B` combinations, all exactly
  246 == `find -type f | wc -l`; per-partition counts matched too.
- *A benign vanished-file race fails the run* — refuted: 1756 real ENOENT races, exit 0,
  `vanished=1756 errors=0`.
- *Marker breaks the `*/*.parquet` glob or gets itself indexed* — refuted: dotfile at index root,
  `--count` unaffected, `grep -c xdu-complete` on `-f path` = 0.
- *Prune-stops-at-first-gap orphans ghost chunks* — dropped: the crawler always writes a contiguous
  sequence, so a gap requires external tampering; identical on `main`, now explicitly tested.
- *Leftover `.parquet.partial` after a failed run is reader-visible* — dropped: `*.parquet` cannot
  match `…parquet.partial`; identical on `main`.
- *`lib::index_glob` worsened the §5 injection surface* — refuted: byte-identical `format!` to `main`'s,
  now a single seam; escaping explicitly deferred in `ASSESSMENT.md`/`ROADMAP.md`. Both injection
  attempts died on DuckDB parser errors.
- *Queue-mutex deadlock / lock-ordering inversion* — dropped: `queue.lock()` is scoped to `pop_front()`
  and never held while taking `global_speed_state`.
- *Cancel-on-first-error could abort a successful run, or a driver panic could be swallowed* — dropped:
  `cancel` is set only on `Err`; `join()` maps `Err(_)` to `"Driver thread panicked"`; release
  `panic="abort"` exits non-zero with the panic message and no marker.
- *Per-partition tallies lost on the failure path understate `errors`* — dropped: `crawl()` returns
  `Err`, so the summary is never printed and the exit status is already non-zero.
- *`-p PART` also re-crawls `__root__`* — dropped: identical on `main`, not a regression.
- *Deleting a partition from the source leaves ghost rows* — dropped: pre-existing; §2 already
  documents finalize as per-file, not per-partition, atomic.
- *All four bins reject `--version`, contradicting §13* — dropped as not-a-regression: identical on
  `main` (`git show main:src/cli.rs | grep -c 'command(version'` = 0), and `ASSESSMENT.md` already
  records it as a deferred `fix/` branch. `bench/run.sh:162-167` handles it with a deliberate
  `Cargo.toml` fallback.
- *`build_work_queue` rejecting a real `__root__` dir is a behavior regression* — dropped: `main`
  **corrupts** in that case (6 of 9 files, exit 0), so the rejection is the fix; documented at
  `doc/xdu.1.scd:88-92`.
- *`tests/crawl_tests.rs:20` re-declares `".xdu-complete"` instead of importing `xdu::COMPLETION_MARKER`* —
  dropped as a style nit (out of rubric scope).

## Human-gate triggers

**TRIGGERED.** F1 (CONFIRMED) is in `src/bin/xdu.rs` and F4 (CONFIRMED) spans `src/bin/xdu.rs` +
`src/lib.rs` — both high-blast-radius core files. Per the rubric, a human must sign off before
`xdu-publish` regardless of the auto-loop.

No CONFIRMED finding touches a destructive-`rm` (§4), schema-stability (§1), atomic-write (§2), or
SQL-injection (§5) invariant — those four were each verified intact by executed command (byte-identical
Parquet; partial→rename + prune; unchanged injection surface; `xdu-rm` untouched but for the shared
glob helper).

## Reviewer conduct

`git status --porcelain` empty on hand-back (orchestrator re-verified); `git worktree list` shows only
the primary tree; `git stash list` empty. No tracked file was edited.

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

Not run — `/xdu-review` was invoked without the `completeness` argument.

---

## Review cycle 2 — changes-requested (2026-08-05)

- **Reviewed commit:** 08fe099389199f223bafcf621117c80124b8f3fc · **Base:** main
- **Cycle:** 2 of ≤3
- **Mode:** **full blind pass** over the spec-excluded diff (`git diff main...HEAD -- . ':(exclude)spec/'`)
  — the cycle-2 default, not a narrow re-verification of cycle 1's named findings. A fresh
  `general-purpose` reviewer was given `GOAL.md`, `invariants.md`, `review-rubric.md` and the runnable
  repo, and was denied `PLAN.md`/`TECH.md`/`research/`/`META.md`/`ASSESSMENT.md`/this file.
- **Contract drift:** none. `git log --oneline main..HEAD -- spec/crawl-hardening/GOAL.md` still returns
  only the original shaping commit `f6be759`.
- **Cycle 1's findings:** all four (F1–F4 of cycle 1) were re-derived as fixed or accounted for by this
  independent pass; none reappears below. The `doc/xdu.1.scd` render defect that cycle 1 could not test
  is now closed — `scdoc` is installed on this host and the **published text** was read, not just exit 0.

### Verification run

**Every gate green at `08fe099`:**

| Gate | Result |
|---|---|
| `cargo fmt --all -- --check` | exit 0, no output |
| `cargo clippy --all-targets --all-features -- -D warnings` | exit 0, **zero** warnings |
| `cargo test --release` (full) | **104 passed, 0 failed** — 66 lib + 22 `crawl_tests` + 16 `rm_tests` |
| `cargo test --release --test crawl_tests -- --nocapture` | 22 passed — **1 silent self-skip** (F4 below) |
| `scdoc < doc/*.scd` | exit 0, no stderr, all four pages |
| `scdoc < doc/xdu.1.scd \| mandoc -Tutf8 \| col -b` | **published text read in full** — `__root__` renders as `__root__` (not `*__root__*`); `OUTDIR/*/*.parquet` renders intact (not `OUTDIR//.parquet`); no line-leading `.` dropped |
| `sh bench/run.sh smoke` | 104 generated / 104 indexed, marker present, A/B document shape verified |

**The man-page render gate actually ran this cycle.** Cycle 1 recorded it as an unclosed gap because no
host tool could render it; `scdoc` (`/opt/homebrew/bin/scdoc`) is now present and both the loud
(nesting) and silent (mis-escaped literal) failure classes were checked by diffing rendered text against
the intended literal. Help-vs-man flag sets match for all four binaries **except** the pre-existing
`-V/--version` gap (recorded, see below).

**Executed CLI drives** (all via throwaway trees/indexes, never a real index):

- **R3 fail-loud:** `chmod 000` on a subtree → `EXIT=1`, `error: …/bob/secret: Permission denied
  (os error 13)`, summary `1 errors`, **no `.xdu-complete` written**, 5 reachable rows still indexed,
  **0** `.partial` files left behind. An unreadable *top-level* dir is reported exactly once (no
  `__root__`/partition double-count).
- **R7 opt-in:** without `--allow-errors` → exit 1; with it → exit 0 and the marker records `errors=1`.
  No existing flag or default changed.
- **R2 accounting vs ground truth:** `bench/run.sh s3 --scale 1 --jobs "1 4 8 16"` →
  `generated_files 100000 / indexed_files 100000` at **every** job count; `s4 --scale 1 -j 8` →
  250000/250000. Hard-kill mid-crawl → index left markerless and readers warn.
- **§2 atomic finalize:** emptying a partition and re-crawling pruned both stale chunks (rows 3→1) and
  rewrote the marker.
- **§5 injection posture unchanged:** `lib::index_glob` emits **byte-identical** SQL to each reader's
  former inline `format!` — verified against `main`.
- **§6 Unix-only:** jwalk 0.8.1 `DirEntry::metadata()` confirmed to be `symlink_metadata` under
  `follow_link=false` (`dir_entry.rs:181-188`), so `MetadataExt` semantics are retained.
- **§13 non-TTY:** piped `xdu-find --count` emits exactly `5` on stdout; all diagnostics on stderr.

### Requirement → evidence matrix (cycle 2) — with who verified each

| R-ID | Verified by | Verdict | Evidence |
|---|---|---|---|
| **R1** — concurrency audit artifact | **orchestrator** (artifact under `spec/`, reviewer structurally blinded) | **Met** | `research/01-concurrency-audit.md` enumerates **11** hazards across the shared pool, driver threads, the `Mutex<VecDeque>` queue, work-stealing balance (#11) and `thread::scope` propagation (#2/#6), each classified **REAL BUG** (#1–#4) · **LATENT HAZARD** (#5–#7) · **NON-ISSUE with rationale** (#8, #9) with line refs, a concrete failure scenario, HPC severity and fix direction — the classification scheme R1 names, exactly. Each REAL BUG traces to a landed fix. |
| **R2** — bugs fixed + real-binary regression tests | blind reviewer | **Met** (one case self-skipped — F4) | 104 tests pass; `--nocapture` confirms 21/22 crawl cases genuinely ran; file-accounting exact at every job count. |
| **R3** — fail loud, non-zero, no false-complete index | blind reviewer | **Met**, with the scoped-run marker hole (**F1**) | Drives above; man-page EXIT STATUS matches observed behavior line for line. |
| **R4** — reproducible benchmark + recorded baseline | blind reviewer | **Met** (F3 is a doc-accuracy defect) | `run.sh smoke` asserts rows == generated per variant; `baseline.json` committed with commit/host/median/min/max/files-per-sec/peak-RSS over 6 configs. |
| **R5** — remove real inefficiencies, don't merge a non-win as a win, document the ceiling | blind reviewer | **Met** | `comparison-p5-ab.json` is a genuine **interleaved** A/B (order alternates by rep parity, `run.sh:417-429`), 9 paired reps/config. All six paired medians within ±1%, signs split — and `scenarios.md` says so plainly: "**It is not a throughput win, and is not kept as one**", justifying the change on one-copy + −10.4 MiB flat-wide RSS + a closed `symlink_metadata` TOCTOU instead. The rejected stat-in-pool lever is documented with its 2.2× regression *and* an explicit "untested, not a finding" caveat. This is R5's "SHALL NOT be merged as if it did" clause honored, not evaded. |
| **R6** — refactor for clarity/testability; schema + defaults unchanged | blind reviewer | **Met** in code; documentation half is **F2** | `xdu.rs` 639 lines (was ~800) with the concurrency contract as a declarative doc comment at `:27-44`; 874-line `src/crawl.rs` in `lib` (~460 logic + ~415 unit tests). `get_schema()`/`FileRecord` untouched; a test reads chunks back and downcasts `StringArray`/`Int64Array`/`Int64Array` in order. |
| **R7** — new surface opt-in only, `.scd` same commit | blind reviewer | **Met** | Drive above + full published-text render. |
| **R8** — assessment produced; low-risk cleanups applied; larger items recorded | **orchestrator** (document) + blind reviewer (visible consequences) | **Met** | `ASSESSMENT.md` states what was applied (4 cleanups, each with why it was safe) and defers **7** items each with the reason it was not safe here, cross-linked to `issues/`. Reviewer independently confirmed the visible half: `index_glob`/`ROOT_PARTITION` behind all three readers (7+1+1 call sites, byte-identical SQL), and four `issues/*.md` at `status: unshaped` each mirrored by a ROADMAP `**Seed:**` entry — and **independently reproduced two of the four** write-ups to confirm they are accurate. |
| **R9** — HPC benchmark protocol | blind reviewer | **Met** | `bench/HPC-PROTOCOL.md` read in full: inputs, per-FS Lustre/GPFS/ZFS environment tables, procedure with load-coordination warning and cache-state decision, metrics incl. FS-side md-op counters, expected result shapes, fill-in reporting template; mandates `--compare-bin` interleaving and folds correctness verification into the protocol. |
| **R10** — invariants preserved + full pre-release gate clean | blind reviewer | **Met** for code; documented-invariant half is **F2** | Gate table above; §1/§2/§3/§5/§6/§7/§8/§13 each verified by executed command. |

### Findings — cycle 2

Four CONFIRMED, none CRITICAL, none HIGH. Ordered most-severe first.

#### C2-F1 — MEDIUM · CONFIRMED · `src/bin/xdu.rs:92`, `src/bin/xdu.rs:636`

A **partition-scoped** run (`xdu -p NAME`) unconditionally clears the *whole-index* completion marker
and then rewrites it from its own stats, so a clean scoped re-index silently retires a correct
incompleteness warning — and can attest an index it never fully walked.

- *Leg (a):* `xdu tree -o idx --allow-errors` records `errors=1` and readers correctly warn. A later
  `xdu tree -o idx -p alice` (clean, one partition) writes `errors=0`; every reader — **including the
  destructive `xdu-rm`** — then reports a clean bill of health while `bob/secret/hidden.txt` remains
  absent from the index (`xdu-find -i idx -p hidden --count` → `0`).
- *Leg (b):* a markerless index (interrupted run, or one predating the marker) is converted to
  "attested" by a one-partition run whose marker records `files=1` for an index holding 2 rows.
- **Evidence:** both legs reproduced end-to-end; marker contents grepped before/after; reader stderr
  captured as silent post-scoped-run.
- **R3** (the marker *is* this pass's mechanism for "SHALL NOT silently finalize a partial index as if
  it were complete"), **R2**.
- **Triage context (orchestrator):** this is **knowingly recorded** — an in-code
  `// Known limitation:` at `src/bin/xdu.rs:628-631` (verified verbatim), plus
  `issues/marker-scoped-run-attestation.md` at `status: unshaped` and a ROADMAP entry. The recorded
  framing is accurate and covers both legs. The open question for the human is whether *recorded* is
  an acceptable resting state for a defect in a mechanism **this same pass introduced**, given the
  consumer is destructive. See the human gate below.

#### C2-F2 — MEDIUM · CONFIRMED · `AGENTS.md:87`, `:144`, `:158-159`; `.agents/factory/invariants.md` (unchanged)

The operating manual drifted against the code this branch landed. `AGENTS.md` declares itself ground
truth ("when something below disagrees with the code, **the code is ground truth — fix this file**"),
and R6's outcome is that a maintainer can reason from "the code and its documented invariants without
re-deriving them". Four concrete gaps, each verified by `grep`:

1. `src/crawl.rs` — the new 874-line module holding the crawl's testable core — is **absent** from the
   `src/` repository map (`grep -ic crawl.rs AGENTS.md` → **0**).
2. `--allow-errors` is **absent** from the "CLI surface" list, which enumerates every other `xdu` flag
   (`grep -ic allow-errors AGENTS.md` → **0**).
3. `ROOT_PARTITION` is still attributed to `xdu.rs` (`AGENTS.md:144`); it moved to `src/lib.rs`.
4. The completion marker (`.xdu-complete`) — a **new on-disk artifact at the index root**, load-bearing
   for all three readers — appears nowhere in `AGENTS.md` (not Architecture, not "Index layout", not the
   invariants list) **nor in `.agents/factory/invariants.md`**, which `AGENTS.md` says is kept "in
   lockstep" (`grep -ic "xdu-complete\|completion marker"` → **0** in both;
   `git diff --stat main...HEAD -- .agents/factory/invariants.md` → no change).
- **Failure scenario:** the next `/xdu-plan` gate and `/xdu-review` footgun checklist both draw from
  `invariants.md` and will not check the marker contract or the `__root__`-collision rejection at all —
  so a future pass breaks the marker or re-derives it from scratch.
- **R6, R10**; §13-adjacent. Rated MEDIUM rather than HIGH because `AGENTS.md` accuracy is not literally
  one of §13's enumerated items — but note §13 *does* require the docs-follow-code discipline.

#### C2-F3 — LOW · CONFIRMED · `bench/scenarios.md:138`, `:172`

The stated between-invocation drift range is contradicted by the committed data it claims to summarize.
Both passages say the two documents "differ by **8.9–18.5%** … in all six configurations". The actual
range, recomputed by the orchestrator directly from the committed JSON, is **1.14–18.47%**:

```
('s2',4) 3.17 vs 3.75 → 15.47%      ('s5',2) 3.59 vs 3.97 →  9.57%
('s3',4) 1.28 vs 1.57 → 18.47%      ('s5',4) 2.49 vs 2.84 → 12.32%
('s5',1) 5.87 vs 6.44 →  8.85%      ('s5',8) 2.61 vs 2.64 →  1.14%   ← far below the claimed 8.9% floor
```

Two of six configs fall below the claimed floor (`s5/8` at 1.14%, and `s5/1` at 8.85% marginally).
The *conclusion* drawn from it ("two documents cannot resolve below ~20%") errs on the safe side, so
nothing downstream is unsafe — the stated range is simply wrong, in the document that defines the
harness's noise floor for every future comparison. **R4/R5** (accuracy of the recorded baseline and the
documented ceiling rationale). Everything else cross-checked in `scenarios.md` matches the JSON exactly:
all six paired medians, `a_faster_reps/reps`, and all three peak-RSS figures.

#### C2-F4 — LOW · CONFIRMED · `tests/crawl_tests.rs:555`

The real-binary regression test for the non-UTF-8 lossy-path fix **self-skips on this host** (APFS
rejects the filename) while the suite still prints `ok`, so that R2 fix has **no executed real-binary
evidence** here. The duplicate-lossy-partition-name guard (`src/crawl.rs:280-290`) has unit coverage
only — no real-binary test at all.

```
$ cargo test --release --test crawl_tests -- --nocapture --test-threads=1
test test_non_utf8_path_is_counted_and_reported ... skipping: this filesystem rejects non-UTF-8 filenames
test result: ok. 22 passed; 0 failed
```

Independently reproduced by the orchestrator. All other 21 crawl tests genuinely ran (the three
root-gated permission tests printed no skip message; uid is 501). **R2** — a coverage-evidence gap, not
a defect in the fix: the pure `lossy_path` / `build_work_queue` functions are unit-tested and pass.
This is the same host limitation cycle 1 recorded; it is noted again because a green suite still reads
as covered.

### Verified and deliberately NOT reported as findings

- **`-V, --version` rejected by all four binaries** while documented in all four man pages
  (`error: unexpected argument '--version' found`, exit 2). A genuine §10 violation, but **pre-existing
  in `main`**, independently rediscovered by this pass, and recorded in `issues/version-flag-missing.md`
  + ROADMAP. Not introduced here.
- **`xdu-view` §12 gaps** (no panic hook / Drop guard for raw-mode restore; byte-index truncation that
  panics on a multibyte name) — pre-existing, untouched by this diff's `xdu-view` hunks, recorded in
  `issues/xdu-view-terminal-safety.md`.
- **Symlink-only root now errors** "No partitions found" where `main` exited 0 with an empty index
  (`main` set `has_root_files` on `is_file() || is_symlink()`). Deliberate, tested, a strict
  improvement; maps to R2 (audit hazard #7).

### Unmapped changes (scope creep)

- **`.gitignore` `/.idea/`** (`c613634`, `[chore]`) — maps to no R-ID. **Benign** one-line
  developer-convenience ignore.
- **`AGENTS.md`'s "Where a deferral goes — four homes" section + `issues/` map entry** — process
  documentation; reasonably attributable to R8's "recorded as an explicit follow-up" mechanism.
  **Benign.**
- **Worth the human's attention, not filed as a finding:** all three readers now emit an unconditional
  stderr warning for *any* index without a marker — which includes **every index built before this
  branch**. Verified: `xdu-find -i legacy --count` prints the warning on stderr on every invocation
  while stdout stays exactly `2`. Deliberate (`2019bb1`) and §13 pipeability is intact, but it is a
  default-behavior change on the **reader** side, whereas the GOAL's no-gos discuss unchanged defaults
  for the *crawler*.

### `.agents/` observations (factory tooling, `[harness]` commits)

Reviewed separately from the product diff. **Nothing weakens a documented gate.**

- `temp_index.sh` (`9f85313`) now builds unconditionally instead of only when a binary is absent —
  this **strengthens** the factory's primary behavioral gate by removing a false-PASS mode where a
  drive measured a stale artifact.
- `xdu-review/SKILL.md` (`0af5f08`) adds the missing-`scdoc` fallback: compare `--help` against the
  `.scd` by inspection, state the unavailability in `REVIEW.md`, flag it as an unclosed gap — "An absent
  tool is a reported gap, never a pass." Moot this cycle: `scdoc` is installed and the full render ran.
- `xdu-review/SKILL.md` + `templates/TECH.md` (`8236edc`) route artifact-deliverable R-IDs to the
  orchestrator. **A trade worth naming:** it closes a real structural hole (a blind reviewer cannot
  verify an R-ID whose evidence lives under `spec/`) but shifts grading of those R-IDs from a blind
  adversarial reviewer to the plan-aware orchestrator. Blindness itself is unchanged, and the mandated
  "who verified each" column is the right mitigation — applied in this cycle's matrix above.
- `issues/` convention (`de3e4ee`) + `ISSUE.md` template + `methodology.md`: a fourth deferral home
  with a `status: unshaped` guard and an explicit "never copy verbatim into `GOAL.md`" rule. All four
  `issues/*.md` on this branch conform.
- **Gap (folded into C2-F2):** `.agents/factory/invariants.md` was **not** updated, so the curated gate
  that `/xdu-plan` and `/xdu-review` both draw from knows nothing about the completion marker or the
  `__root__`-collision rejection.

### Human-gate triggers

**TRIGGERED.** `C2-F1` is CONFIRMED and lives in `src/bin/xdu.rs` — a high-blast-radius core file
(crawl + atomic finalize). Per the rubric a human must sign off before `/xdu-publish` regardless of the
auto-loop, and the substance of C2-F1 is itself a judgement call (is *recorded* an acceptable resting
state for a defect in a mechanism this pass introduced, when the downstream consumer is destructive?).

**No CONFIRMED finding touches a destructive-`rm` (§4), schema-stability (§1), atomic-write (§2), or
SQL-injection (§5) invariant.** Each of those four was verified intact by executed command this cycle:
`get_schema()`/`FileRecord` untouched and chunks read back column-by-column; partial→rename + stale
prune reproduced; `index_glob` byte-identical to the prior inline SQL; `xdu-rm`'s
confirm/`--dry-run`/`--force`/`--safe` and `--limit`+`ORDER BY` gates driven and intact.

### Reviewer conduct

`git status --porcelain` **empty** on hand-back, re-verified independently by the orchestrator before
any other step. No tracked file was edited; all scratch work stayed in the session scratchpad;
`bench/results/` unchanged (5 committed JSON documents). The reviewer wrote no `REVIEW.md`, called no
`ReportFindings`, and ran no `set_phase.py`.

### Optional completeness sub-pass (separate reviewer; may see TECH.md)

Not run — `/xdu-review` was invoked without the `completeness` argument.

### Human sign-off on the cycle-2 gate (2026-08-05)

The mandatory gate fired on **C2-F1** (CONFIRMED, `src/bin/xdu.rs` — high-blast-radius core). The human
was given the accept-or-remediate call and chose: **remediate the documentation findings; keep C2-F1
deferred on its existing record.**

- **C2-F1 — accepted as recorded.** It stays deferred on `issues/marker-scoped-run-attestation.md`
  (`status: unshaped`) + its ROADMAP entry + the in-code `// Known limitation:` at
  `src/bin/xdu.rs:628-631`, all three verified accurate and covering both legs. **No crawl-core change
  this cycle.** Rationale: the limitation is honestly documented at the point of use, and re-opening the
  marker logic in cycle 3 of ≤3 would spend the last of the review budget on the highest-risk item.
- **C2-F2 — remediate.** Update `AGENTS.md` (add `src/crawl.rs` to the repository map, add
  `--allow-errors` to the CLI surface, re-attribute `ROOT_PARTITION` to `src/lib.rs`, document
  `.xdu-complete` in Architecture / Index layout) **and** `.agents/factory/invariants.md` (the marker
  contract and the `__root__`-collision rejection), which `AGENTS.md` says is kept in lockstep.
- **C2-F3 — remediate.** Correct the stated drift range at `bench/scenarios.md:138` and `:172` from
  8.9–18.5% to the actual 1.14–18.47%. The conclusion drawn from it is unaffected.
- **C2-F4 — no code change required.** Recorded as a host-coverage limitation; the case runs on the
  Linux CI leg. Cross-referenced to META `F3`, whose recommended fix (make a self-skip visible in the
  gate's report rather than silent) is the durable remedy.

Scope for the remediation build: **documentation and one benchmark-doc number only — no `src/` change.**

---

## Review cycle 3 — changes-requested (2026-08-05)

- **Reviewed commit:** a6b8a385869cc80d7cebb202c8da77503050e195 · **Base:** main
- **Cycle:** 3 of ≤3 — **the bound is now exhausted.** See "Non-convergence" below.
- **Mode:** **full blind pass** over the spec-excluded diff (`git diff main...HEAD -- . ':(exclude)spec/'`)
  — the cycle-3 default, deliberately *not* narrowed to re-verifying cycle 2's named findings. A fresh
  `general-purpose` reviewer was given `GOAL.md`, `invariants.md`, `review-rubric.md` and the runnable
  repo, and was denied `PLAN.md`/`TECH.md`/`research/`/`META.md`/`ASSESSMENT.md`/this file. **This choice
  is why cycle 3 was not a formality:** it surfaced a coupled-core defect (C3-F1) that two prior full
  passes missed.
- **Contract drift:** none. `git log --oneline main..HEAD -- spec/crawl-hardening/GOAL.md` still returns
  only the original shaping commit `f6be759`.
- **Delta under review since cycle 2's commit `08fe099`:** documentation only —
  `AGENTS.md`, `.agents/factory/invariants.md`, `.agents/factory/review-rubric.md`,
  `bench/scenarios.md`. `git diff --stat 08fe099..HEAD` touches **no** `src/`, `tests/`, `bench/*.sh`,
  `bench/results/`, `doc/`, `Cargo.toml` or `Cargo.lock`. The cycle-2 human gate's scope
  ("documentation and one benchmark-doc number only — no `src/` change") was honored exactly.

### Cycle-2 remediation: both findings closed

- **C2-F2 — CLOSED.** All four gaps remediated, and verified against the **source**, not by keyword
  presence: `crawl.rs` in the `src/` map (`AGENTS.md:92`), `--allow-errors` in the CLI surface (`:181-183`),
  `ROOT_PARTITION` re-attributed to `lib.rs` (`:151`, matching `src/lib.rs:28`), `.xdu-complete`
  documented in Architecture (`:144`), Index layout (`:154`) and invariants #2/#3. `invariants.md` gained
  **§2b** (marker contract: `MARKER_READ_LIMIT`, clear-after-pre-flight / write-on-success ordering, the
  recorded scoped-run limitation) and **§2c** (fail-loud default + `--allow-errors` opt-in), plus the
  `__root__`-collision rejection and `index_glob` in §3, and §2 re-attributed to
  `crawl.rs::PartitionBuffer::finalize`. Each claim traced to code: `src/lib.rs:28/33/38/82`,
  `src/crawl.rs:40` (re-export), `src/crawl.rs:232` (rejection), and all three reader call sites
  (`xdu-find.rs:22`, `xdu-rm.rs:44`, `xdu-view.rs:1841`). The blind reviewer independently cross-checked
  the same documents against the code and found no mismatch in either direction.
- **C2-F3 — CLOSED.** `bench/scenarios.md:138` and `:172` now state **1.1–18.5%**. Recomputed by the
  orchestrator from the committed JSON: **1.14–18.47%**, baseline faster in **all six** configurations —
  the claim now matches its data. The rewrite also adds a correction the finding did not ask for and that
  improves the document: it distinguishes *direction* (all six point the same way → systematic session
  bias) from *magnitude* (non-uniform, 1.1% at `s5/-j8` vs 18.5% at `s3/-j4`), and states that a
  config which happens to drift little is no evidence the harness is quiet.
- **`.agents/factory/review-rubric.md`** was also touched. It **strengthens** the human gate — adding
  `src/crawl.rs` (atomic finalize + `__root__` rejection) and `index_glob` to the high-blast-radius list.
  Nothing was relaxed. Noted because a review-gate edit on a branch under review deserves explicit
  scrutiny; the same expansion is mirrored in `invariants.md`.

### Verification run

The blind reviewer executed 25 numbered gates/drives. Every gate green at `a6b8a38`:

| Gate | Result |
|---|---|
| `cargo fmt --all -- --check` | exit 0, no output |
| `cargo clippy --all-targets --all-features -- -D warnings` | exit 0, clean |
| `cargo test` | **104 passed, 0 failed** — 66 lib + 22 `crawl_tests` + 16 `rm_tests` |
| `cargo test --test crawl_tests -- --nocapture --test-threads=1` | 22 passed, **1 self-skip** (verbatim: `test_non_utf8_path_is_counted_and_reported ... skipping: this filesystem rejects non-UTF-8 filenames`). Root-gated cases **did** run (`id -u`=501) |
| `scdoc < doc/*.scd` (all four) | no failures |
| `scdoc < doc/xdu.1.scd \| mandoc -Tutf8 \| col -b` | **published text read in full**; literals intact (`__root__`, `OUTDIR/*/*.parquet`, `st_blocks * 512`, `.xdu-complete`, `.partial`); `grep -nE "^[[:space:]]*['.]" doc/*.scd` → none |
| `--help` vs `.scd`, all 4 binaries | only the **pre-existing** `--version` gap |
| `sh bench/run.sh smoke` | exit 0 — 104 generated / 104 indexed, marker present, A/B shape verified |

**Executed drives** (throwaway trees/indexes only, never a real index) — the substantive ones:

- **Exact accounting, 693-file tree** (17 partitions, depth 1–5, loose root files, 2 file + 1 dir
  symlink) at `-j 1 2 3 4 8 16 32`: every run `rc=0`, `rows=693`, `marker files=693`, `partials=0`,
  `xdu` stdout **0 bytes**; `xdu-find -f path | sort` **byte-identical to `find -type f | sort`** at
  every job count; no duplicates; per-partition counts identical `-j1` vs `-j16`; `__root__` holds
  exactly the 9 loose files and no symlink.
- **86,400-file tree, reader polled during the live crawl:** `rc=0`, 86,400 rows, `partials=0`, **0**
  reader failures — no `.partial` ever globbed, no torn chunk observed.
- **86,400-file tree with 12 concurrent `rm -rf` streams:** `rc=0`, marker
  `files=80142 vanished=454 errors=0`, rows match, **0** hard-error lines — the vanished-vs-hard-error
  classification holds under real races.
- **Fail-loud:** hard read error → `rc=1`, path+errno on stderr, **no marker**, reachable rows still
  indexed. With `--allow-errors` → `rc=0`, marker records `errors=1`, per-path report still printed.
  Unreadable *top-level* partition dir → `rc=1`, no marker.
- **Reader warning propagation:** `xdu-find --count` → stdout `647`, warning on stderr; `xdu-rm
  --dry-run` → warning **before** anything unlinked; `xdu-view` → warning before the alternate screen.
- **§4 destructive gates:** `--dry-run` deleted nothing; `-l 2` dry-run twice byte-identical
  (deterministic `ORDER BY`); `n` → `Aborted.` with the file intact; `--force` → `Deleted: 1`.
- **Edge inputs:** `-B 0`, `-B 1`, `-B 3`, `-j 0` all `rc=0`, 693 rows, 0 partials.
- **R5 provenance:** `git diff 2019bb1..HEAD -- src/ Cargo.toml Cargo.lock rust-toolchain.toml` is
  **empty**, so `comparison-p5-ab.json`'s A side measures HEAD's crawl source exactly.
- **§10 same-commit rule:** the `--allow-errors` CLI field and its `.scd` entry landed together in
  `278f8f9`.

### Requirement → evidence matrix (cycle 3) — with who verified each

| R-ID | Verified by | Verdict | Evidence |
|---|---|---|---|
| **R1** — concurrency audit artifact | **orchestrator** (artifact under `spec/`; reviewer structurally blinded) | **Met** | `research/01-concurrency-audit.md` enumerates **11** hazards with line refs, a concrete failure scenario, HPC severity and fix direction each, across the shared pool, driver threads, the `Mutex<VecDeque>` queue, work-stealing balance (#11) and `thread::scope` propagation (#2/#6). *One imprecision, not a finding:* hazard **#10** ("No early cancellation after first error") is classed `minor` rather than one of R1's three vocabulary terms (real bug / latent hazard / non-issue). It carries the required rationale and was in fact fixed (P3 cancel-on-first-error), so R1's substance is met; cycle 2's matrix row overstated slightly by describing all 11 as fitting the three-way scheme. |
| **R2** — bugs fixed + real-binary regression tests | blind reviewer | **Met** (one case self-skips on this host) | 22 `crawl_tests` drive the real binaries; exact 693-row accounting across 7 job counts with path sets byte-identical to `find`; symlink exclusion, `__root__` rejection, fail-loud all independently reproduced. |
| **R3** — fail loud, non-zero, never a silently-complete index | blind reviewer | **Met** in the enumerated cases; **C3-F1** is a new failure mode of the marker mechanism, **C3-F2** a gap in what it detects | Drives above; no marker on any failure path. Driver-panic leg remains structural (`panic="abort"` → SIGABRT, non-zero, no marker); the unwind branch (`xdu.rs:492`) is debug-only and not independently reproduced. |
| **R4** — reproducible benchmark + recorded baseline | blind reviewer | **Met** | `run.sh smoke` and `s1 --reps 2` both assert indexed == generated; `baseline.json` = 6 runs with commit/host/medians/files-per-sec/peak-RSS; `scenarios.md` documents all six shapes. |
| **R5** — remove real inefficiencies, document the ceiling, don't merge a non-win as a win | blind reviewer | **Met** | A-side provenance proven empty-diff vs HEAD; `scenarios.md` states the shipped change is a **null** in wall time (all six paired medians within ±1%, signs split) and keeps it for the copy reduction + closed `symlink_metadata` TOCTOU, "not speed"; the stat-in-pool lever is measured at 2.2× worse and reverted; the jwalk ceiling (directory = unit of parallelism) is named; four levers deferred with reasons. R5's "SHALL NOT be merged as if it did" clause is honored, not evaded. |
| **R6** — refactor for clarity/testability; schema + defaults unchanged | blind reviewer | **Met** | `src/crawl.rs` (874 lines, 13 unit tests) holds work-queue construction, record building, `PartitionBuffer`, marker I/O; the bin keeps the scaffold. `get_schema()` untouched. Default behavior preserved except the R3-mandated fail-loud and the R2-mandated symlink-`__root__` fix. |
| **R7** — new surface opt-in only, `.scd` same commit | blind reviewer | **Met** | `--allow-errors` is the only addition (one field, `#[arg(long)]`, no short, defaults `false`); same-commit rule verified at `278f8f9`; no `--jobs`/`--buffsize` default changed; completions regenerate with the flag. |
| **R8** — assessment produced; low-risk cleanups applied; larger items recorded | **orchestrator** (document) + blind reviewer (visible consequences) | **Met**, with **C3-F2** as an unrecorded item | `ASSESSMENT.md` records 4 applied cleanups each with why-it-was-safe and **7** deferrals each with why-not-here, cross-linked to `issues/`. Orchestrator counted its "eight hand-built globs (find 1 / rm 1 / view 6)" claim exactly. Reviewer independently confirmed the code half and checked each `issues/*.md`'s factual claims against the source. |
| **R9** — HPC benchmark protocol | blind reviewer | **Met** | `bench/HPC-PROTOCOL.md` §1 purpose · §2 inputs · §3 environment · §4 procedure · §5 metrics · §6 expected result shapes · §7 reporting template; mandates the `--compare-bin` interleave. |
| **R10** — invariants preserved + full pre-release gate clean | blind reviewer | **Met** for the gate and §1–§13 as written; **C3-F1 is a gap in §3's reasoning applied to the new marker name** | Gate table above; §1 schema untouched; §2 partial→rename + prune verified by executed drives; §2b ordering verified; §6 jwalk `DirEntry::metadata()` re-confirmed as `symlink_metadata` under `follow_links(false)`; §8 symlinks excluded; §13 stdout clean in every drive. |

### Findings — cycle 3

Two CONFIRMED, none CRITICAL, none HIGH. Both independently reproduced by the orchestrator.

#### C3-F1 — MEDIUM · CONFIRMED · `src/crawl.rs:232` (the guard) + `src/lib.rs:33` (`COMPLETION_MARKER`)

**This pass introduced a second reserved name at the index root and guarded only the first.**
`build_work_queue` rejects a top-level source directory named `__root__`, but not one named
`.xdu-complete`. Such a directory becomes a partition directory at exactly the marker path, which
**bricks the outdir for every future run, from any source tree.**

The asymmetry is visible in the code's own words. The guard's declarative comment states the general
principle — *"the check is unconditional because the collision is with what is already on disk, not
with what this run happens to select"* — which applies verbatim to `.xdu-complete`; and
`COMPLETION_MARKER`'s doc comment records only the one-way property (*"a dotfile, so the readers'
`*/*.parquet` glob never mistakes it for a partition"*). The reverse direction — a partition being
mistaken for the marker — is neither guarded nor documented. The published man page has the same
one-way framing.

- *Run 1* indexes every file **correctly** (reader count 2) but exits **non-zero**, because
  `write_completion_marker` hits `EISDIR`: a correct index reported as a failed run and left
  permanently unattested.
- *Run 2 and every run after*, **for any source tree pointed at that outdir**, fail earlier still — in
  `clear_completion_marker` with `EPERM`. The index directory can never be rebuilt or marked complete
  again without a manual `rmdir`.

**Evidence** (orchestrator's independent reproduction, matching the reviewer's):

```
$ mkdir -p $T/normal $T/.xdu-complete   # .xdu-complete is a dir in the SOURCE tree
$ xdu --apparent-size -o $I $T; echo rc=$?
Finished normal (1 files, 4 B)
Finished .xdu-complete (1 files, 4 B)
Completed 2 files (8 B) in 0.00s
Error: Failed to write completion marker: .../f1i/.xdu-complete
Caused by: Is a directory (os error 21)
rc=1
$ xdu-find -i $I --count        # the index is in fact complete and correct
2
$ ls -ld $I/.xdu-complete
drwxr-xr-x@ 3 geoffrey wheel 96 ... .xdu-complete
$ xdu --apparent-size -o $I $SP/other; echo rc=$?    # UNRELATED source tree, same outdir
Error: Failed to remove stale completion marker: .../f1i/.xdu-complete
Caused by: Operation not permitted (os error 1)
rc=1
```

**Maps to:** the reasoning of §3 / §2b, and R2/R3 adjacently. Reachability is low (a pathological
directory name) — but so is `__root__`, which *is* guarded unconditionally, and the consequence here is
worse: a bricked outdir plus a correct index reported as failed. **No written §1–§13 clause names
`.xdu-complete` as reserved, so this is a defect on an edge path rather than a literal invariant breach
— it is therefore MEDIUM, not auto-CRITICAL.** It touches `src/crawl.rs` and `src/lib.rs`, both on the
high-blast-radius list (which this very branch expanded), so the **mandatory human gate fires**.

#### C3-F2 — LOW · CONFIRMED · `src/crawl.rs:443` (`finalize` prune scope) + `src/bin/xdu.rs:636`

A top-level directory **removed from the source** leaves its entire partition — chunks and rows — in the
index indefinitely, and the run still exits 0 and writes a completion marker, so every reader reports a
clean bill of health for an index carrying phantom rows. `finalize` prunes stale chunks only *within*
partitions this run walked; a partition the run never saw is never reconciled.

**Evidence** (orchestrator's independent reproduction):

```
initial rows=120  p3=40
# rm -rf p3 from the SOURCE, then re-index
reindex rc=0
rows=120        (ground truth: 80)
phantom p3 still resolves: 40 rows
marker: files=80  errors=0        <- marker's own count contradicts the index's row count
reader warning: (silent)
```

The marker records `files=80` for an index returning 120 rows, and nothing cross-checks the two.

**Stated plainly, both ways:** the stale-partition survival is **pre-existing** — `main`'s `finalize`
has the identical per-partition prune scope (`git show main:src/bin/xdu.rs`, prune from `num_chunks..`
inside the partition's own directory) — and it is not in R2's enumerated hazard list. What is *new* is
that a completeness attestation now exists and does not detect this case. **Unlike C2-F1, this variant
is recorded nowhere:** the orchestrator grepped `issues/`, `ROADMAP.md`, `ASSESSMENT.md`, `AGENTS.md`,
`invariants.md` and `src/` for it and found nothing.
`issues/marker-scoped-run-attestation.md` covers only the `--partition`-scoped-run variant. Maps to
**R8**'s "recorded as an explicit follow-up rather than attempted here" — the recording is the gap, not
the fix.

### Dropped under refutation

The reviewer investigated and discarded these, each with executed evidence:

- **`--version` rejected by all four binaries while documented in all four man pages** — real §10
  mismatch, verified **pre-existing** (`main:src/cli.rs` sets no `version`; `main:doc/xdu.1.scd:56`
  already documents it), correctly recorded in `issues/version-flag-missing.md` + ROADMAP. The
  reviewer re-verified both of that file's precisions (completions genuinely omit it; the `.scd` needs
  no edit).
- **§12 `xdu-view` gaps** (no Drop guard / panic hook; byte-index truncation) — verified line-for-line,
  pre-existing and identical in `main`, this diff's hunks stop well short of those regions, accurately
  recorded in `issues/xdu-view-terminal-safety.md`.
- **§5 injection surface** — unchanged by the diff; `index_glob` consolidates the seam without adding
  escaping, which §5 already describes as the current state; escaping work is in the ROADMAP.
- **`xdu -p one` still crawls `__root__`** (the `has_root_files` flag ignores `partition_filter`) —
  identical in `main`. Pre-existing, unchanged.
- **Read-only index root now breaks a partition re-index** (fails at `clear_completion_marker` where
  `main` succeeded) — dropped: it fails **before any write**, with a clear diagnostic, leaving the index
  and its attestation intact. An inherent and correct consequence of the R2/R3-mandated marker.
- **Leftover high-id `.partial` from a crashed run is never cleaned** — readers ignore it (count
  unchanged) and the man page documents exactly this. Benign.
- **`baseline.json` records a commit whose `src/` differs from HEAD** — that is what a baseline *is*;
  the A/B document's A side measures HEAD exactly. Not a gap.
- **Hidden top-level dirs becoming invisible partitions** — refuted empirically: DuckDB's `*` does
  match leading-dot directories (`.hidden` → `-u .hidden --count` = 1, total = marker `files`), and the
  marker is a file, so `*/*.parquet` never matches it.
- **`-j 0` / `-B 0`** — refuted; both index correctly.
- **Broken-stderr-pipe failure in the driver loop** — same pattern in `main`; fails non-zero with no
  marker, i.e. the safe direction.

### Unmapped changes (scope creep)

- **`.agents/` process changes** (~150 lines across `invariants.md`, `methodology.md`,
  `review-rubric.md`, `harness-log.md`, `templates/{ISSUE,TECH}.md`, three `SKILL.md`s,
  `bin/temp_index.sh`) — map to no R-ID. Committed as separate `[harness]` commits, each logged in
  `harness-log.md` with rationale as `AGENTS.md` requires. The reviewer read **all** of them
  specifically for gate weakening: every one **strengthens** a gate. **Benign** — but note they land in
  the same squash PR as the refactor.
- **`.gitignore` `/.idea/`** (`c613634`, `[chore]`) — benign one-liner.
- **`.gitignore` `bench/results/*` with `!baseline.json` / `!comparison-*.json`** — in scope (R4/R5).
- **`issues/*.md` (4) + `ROADMAP.md` (+65)** — R8's follow-up half; the reviewer checked each issue's
  factual claims against the code and found them correct.
- **`bench/results/comparison-*.json`** — in scope; `scenarios.md` explicitly labels which two carry a
  wrong auto-generated provenance note rather than hiding it.

### Human-gate triggers

**TRIGGERED.** `C3-F1` is CONFIRMED and lives in `src/crawl.rs` **and** `src/lib.rs` — two
high-blast-radius core files. Per the rubric a human must sign off before `/xdu-publish` regardless of
the auto-loop.

**No CONFIRMED finding touches a destructive-`rm` (§4), schema-stability (§1), atomic-write (§2), or
SQL-injection (§5) invariant.** Each was verified intact by executed command this cycle: `get_schema()`
untouched; partial→rename + in-partition prune reproduced correct (including prune-to-zero); `index_glob`
the sole glob seam with no hand-built glob remaining; `xdu-rm`'s confirm / `--dry-run` / `--force` /
`--safe` and `--limit`+deterministic-`ORDER BY` gates all driven and intact.

### Non-convergence — escalation

**This is cycle 3 of the ≤3 bound, and it did not converge.** Cycles 1 and 2 closed every finding they
raised, and cycle 2's docs-only remediation is verified complete — but cycle 3's fresh full pass found a
**new** MEDIUM defect in the coupled core (C3-F1) that the two prior full passes missed. The loop budget
is now spent, so the decision is the human's, not the loop's. The three coherent options:

1. **Fix C3-F1 in a P13** — the fix is small and local (extend the `build_work_queue` guard to reject
   `COMPLETION_MARKER` as well, with a regression test driving the real binary), and record C3-F2 in
   `issues/` + ROADMAP. This exceeds the ≤3 bound and needs explicit authorization.
2. **Record both and publish** — treat C3-F1 the way C2-F1 was treated (an `issues/` entry, a ROADMAP
   seed, and an in-code `// Known limitation:`). Cheapest, but it defers a self-inflicted defect in a
   mechanism this pass introduced, on an edge path that bricks an outdir.
3. **Publish as-is and open a follow-up `fix/` branch** for both, decoupling the large refactor from the
   two edge-path defects.

The reviewer's own read is worth relaying: C3-F1's blast radius is high but its reachability is low, and
the fix is genuinely small — option 1 is the cheapest route to a clean core, option 3 the cheapest route
to shipping. **No further step is taken until the human chooses.**

### Reviewer conduct

`git status --porcelain` **empty** on hand-back, re-verified independently by the orchestrator before
anything else; `git stash list` also empty. No tracked file was edited. All scratch work stayed in the
session scratchpad and system temp; `bench/run.sh` drives used `--out <scratch>` and never `baseline`
mode, so `bench/results/` was untouched; completions were generated to scratch, not `share/`. The
reviewer wrote no `REVIEW.md`, called no `ReportFindings`, and ran no `set_phase.py`. It read nothing
under `spec/` — every diff used `':(exclude)spec/'`.

### Optional completeness sub-pass (separate reviewer; may see TECH.md)

Not run — `/xdu-review` was invoked without the `completeness` argument.

### Human sign-off on the cycle-3 gate (2026-08-05)

The mandatory gate fired on **C3-F1** (CONFIRMED, `src/crawl.rs` + `src/lib.rs` — two high-blast-radius
core files), and the ≤3 cycle bound was simultaneously exhausted. The human was given the
fix / record / defer-to-a-follow-up-branch call and chose: **fix C3-F1 in a new phase, and explicitly
authorized exceeding the ≤3 review-cycle bound to do it.**

- **C3-F1 — remediate in a new `P13`.** Extend the reserved-name rejection in
  `crawl::build_work_queue` to cover `COMPLETION_MARKER` as well as `ROOT_PARTITION`, with a
  **real-binary regression test** asserting a concrete post-condition (a source tree containing a
  top-level `.xdu-complete` directory is rejected with a clear diagnostic, and the outdir is left
  usable). Prefer closing the **class** over the instance: have the guard iterate a single
  reserved-name list that `lib` owns, so adding a future reserved constant extends the guard by
  construction rather than by remembering. This is the fix META `F13` recommends.
- **C3-F2 — record, do not fix.** The prune scope is pre-existing in `main` and outside this GOAL's
  scope; the gap is that it is recorded nowhere. It gets an `issues/{slug}.md` at `status: unshaped`
  from [`templates/ISSUE.md`](../../.agents/factory/templates/ISSUE.md) plus a `**Seed:**` entry in
  `ROADMAP.md`, per `AGENTS.md`'s four-homes rule. The write-up must state plainly that the stale
  partition survives in `main` too, and that what this pass changed is the presence of an attestation
  that does not detect it.
- **Scope for the remediation build:** the `build_work_queue` guard plus its regression test, and the
  two deferral records. **No other `src/` change**, no marker-format change, and C2-F1 stays deferred on
  its existing record. The full pre-release gate must pass clean.
- **Cycle bound:** this remediation lands as cycle 4, above the documented ≤3. The authorization is
  recorded here so the overrun is deliberate and auditable rather than silent. The re-review may be
  scoped to verifying C3-F1's fix and C3-F2's records, since the rest of the diff has now had three
  independent full blind passes.

---

## Review cycle 4 — changes-requested (2026-08-05)

- **Reviewed commit:** 0c77ccbf1de150bb271284dc36286a0fed390407 · **Base:** main
- **Cycle:** 4 — **beyond the ≤3 bound for the second time.** Cycle 3's `blocked_reason` records that
  the human explicitly authorized this cycle. It did **not** authorize a fifth. See "Non-convergence".
- **Mode:** **full blind pass** over the spec-excluded diff (`git diff main...HEAD -- . ':(exclude)spec/'`)
  — the cycle default, deliberately *not* narrowed to re-verifying C3-F1's remediation. A fresh
  `general-purpose` reviewer got `GOAL.md` (inlined verbatim), `invariants.md`, `review-rubric.md` and
  the runnable repo, and was denied everything under `spec/`. The choice paid for itself again: the one
  materially new finding (C4-F1) is in a region no prior cycle had driven.
- **Tooling:** `scdoc` 1.11.5 and `mandoc` both present. **No unclosed man-page gap this cycle** — the
  published text was rendered and read, not merely exit-checked.
- **Contract drift:** none. `git log --oneline main..HEAD -- spec/crawl-hardening/GOAL.md` still returns
  only the shaping commit `f6be759`.
- **Delta under review since cycle 3's commit `a6b8a38`:** P13 only —
  `src/lib.rs` (+24), `src/crawl.rs` (+91/−54), `tests/crawl_tests.rs` (+50), `doc/xdu.1.scd` (+12/−9),
  `issues/orphan-partition-survives-reindex.md` (new), `ROADMAP.md` (+15), and the two doc files. The
  cycle-3 gate's scope ("no other `src/` change; C2-F1 stays deferred") was honored exactly.
- **Reviewer conduct:** verified. `git status --porcelain` empty on hand-back, `git worktree list` shows
  only the main checkout, HEAD unmoved. No `spec/` file was read by the reviewer.

### Cycle-3 remediation: C3-F1 closed as a class, C3-F2 recorded

- **C3-F1 — CLOSED, and closed correctly.** The gate asked for the *class* to close, not the instance,
  and it did. `lib::RESERVED_INDEX_NAMES` (`src/lib.rs:36-52`) pairs every name the index root claims
  with what claims it; `crawl::build_work_queue` (`src/crawl.rs:241-252`) `find`s against that list
  rather than testing names one by one, so reserving a future name extends the rejection by
  construction. The unit test is **driven from the list itself**, so a later addition is covered without
  anyone remembering to extend the test — which is precisely the failure mode that produced C3-F1. The
  real-binary regression (`tests/crawl_tests.rs:322`) asserts both halves of the original damage: the
  run is rejected in pre-flight with the marker path left free, **and** an unrelated later run over the
  same outdir still completes and attests — the bricking, not just the rejection. The test also pins the
  correct negative: a reserved name borne by a loose *file* is not a collision and becomes a `__root__`
  row. Independently driven (reviewer rows 12–14): both names rejected, rc=1, and **still rejected under
  `-p alpha`** where the filter would have excluded the directory. `doc/xdu.1.scd` was updated in the
  same commit and its rendered text reads correctly.
- **C3-F2 — RECORDED, as gated.** `issues/orphan-partition-survives-reindex.md` exists with
  `status: unshaped` and a `**Seed:**` entry in `ROADMAP.md:216`. The blind reviewer re-derived the
  behavior independently and fact-checked every claim in the issue file against source — all accurate,
  including "pre-existing in `main`" (`git show main:src/bin/xdu.rs` has the identical
  `for chunk_id in num_chunks..` per-partition prune scope).

### Verification run

The blind reviewer executed 28 numbered gates/drives at `0c77ccb`; the orchestrator independently
re-ran the ones underpinning findings. Selected results:

| Gate | Result |
|---|---|
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --all-targets --all-features -- -D warnings` | exit 0, clean |
| `cargo test` | **105 passed, 0 failed** — 66 lib + 23 `crawl_tests` + 16 `rm_tests` (was 104; P13 adds one) |
| `cargo test --test crawl_tests -- --nocapture --test-threads=1` | 23 passed, **1 self-skip** (`test_non_utf8_path_is_counted_and_reported` — APFS rejects the name). `geteuid()!=0`, so all four root-gated cases **really ran** |
| `scdoc` all four `.scd`; `scdoc \| mandoc -Tutf8 \| col -b` | no failures; published text read in full — `__root__`, `OUTDIR/*/*.parquet`, `.xdu-complete`, `.partial` all intact; no dropped leading `.`/`'`, no swallowed `*` |
| `--help` vs `.scd`, 4 binaries | only the **pre-existing** `--version` gap (see C4-F2) |
| `sh bench/run.sh smoke` | exit 0 — 104 generated / 104 indexed, marker present |
| **Accounting at scale:** `gen_tree.py --scenario s3` (100 000 files / 1000 partitions) at `-j 1/8/16` | rc=0 each; **rows = 100000 exactly at every job count**; 1000 partition dirs; 0 `.partial`; marker present |
| Same tree at `-j 8 -B 7` (chunk-boundary stress) | rows=100000, **distinct paths=100000** — no loss or duplication across flushes |
| Fail-loud | unreadable subtree → per-path errno, rc=1, **no marker**, sibling partition still indexed; `--allow-errors` → rc=0, marker `errors=1`, diagnostic still printed |
| Reader warnings | markerless and tolerated-errors warnings from all three readers, **stderr only**; stdout stayed exactly `2` |
| §4 destructive gates | `n` → `Aborted.`, 5 files intact; `--dry-run --force` deletes nothing; two `--dry-run -l 2` runs byte-identical |
| Benchmark arithmetic re-derived from `comparison-p5-ab.json` | every figure in `scenarios.md` exact, incl. both noise-floor ranges; `git diff b8f5f9c c9630c0 -- src/` empty, so the drift argument holds |

**Orchestrator's independent reproductions** (throwaway trees under `/tmp`, never a real index):

- **C4-F1 on HEAD** — 11-row index, then `chmod 000` the `alpha` partition and re-index
  `--allow-errors`: `Finished alpha (0 files, 0 B, pruned 1 stale, 1 errors)`, rc=**0**, `out/alpha`
  now empty, total rows **11 → 1**, marker written with `files=1 errors=1`.
- **The same scenario on `main`**, via `git worktree add /tmp/xdu-main-wt main` + a release build:
  `Finished alpha (0 files, 0 B, pruned 1 stale)`, rc=**0**, rows **11 → 1**, **no error diagnostic at
  all**, no marker mechanism. Worktree removed; tree clean.
- **C4-F2/F3/F4** — `grep -rn` over `src/`, `tests/`, `AGENTS.md`, `.agents/factory/invariants.md`.
- **Reviewer's §5 claim** — `git show main:src/bin/xdu-find.rs` carries the identical raw `format!`
  into `read_parquet`; the ROADMAP follow-up at `:166` and `:176` records the escaping work.

### Requirement → evidence matrix (cycle 4) — with who verified each

| R-ID | Verified by | Verdict | Evidence |
|---|---|---|---|
| **R1** — concurrency audit artifact | **orchestrator** (artifact under `spec/`; reviewer structurally blinded) | **Met** | `research/01-concurrency-audit.md` is byte-unchanged since cycle 3: 11 hazards, each with line refs, a concrete failure scenario, HPC severity and fix direction. Cycle 3's noted imprecision (hazard #10 classed `minor` rather than one of R1's three terms) stands unchanged and remains not-a-finding — it carries its rationale and was fixed anyway. *Corroborating, not grading:* the reviewer's 100 000-file × 1000-partition drives at `-j 1/8/16` and `-B 7` found no lost, duplicated or starved file. |
| **R2** — bugs fixed + real-binary regression tests | blind reviewer | **Met**, with **C4-F1** as an unrecorded residual | 23 `crawl_tests` drive the real binaries and assert concrete post-conditions; `tests/common/mod.rs` reimplements no production logic. Fixes proven against `main` by differential drive: swallowed walk/metadata errors, the unchecked `read_children_error`, the `is_symlink()` phantom `__root__`, and both reserved-name collisions. Residual: C4-F1 (new, unrecorded) and C3-F2 (recorded). |
| **R3** — fail loud, non-zero, never a silently-complete index | blind reviewer | **Met** | Unreadable subtree → rc=1, per-path errno, no marker; write failure abandons queued partitions and writes no marker; a pre-flight rejection leaves a prior marker intact. Marker sequencing matches §2b (cleared `xdu.rs:92` after the last rejecting check, written `xdu.rs:636` after the `errors>0` bail). Driver-panic leg remains structural (`panic="abort"` → SIGABRT); the unwind branch is debug-only and still not independently reproduced. |
| **R4** — reproducible benchmark + recorded baseline | blind reviewer | **Met** | `smoke` asserts index-holds-exactly-N, not exit 0; `baseline.json` records 6 configs with commit, host, medians, per-rep samples and a dirty flag; every flag cited by `scenarios.md`/`HPC-PROTOCOL.md` exists. |
| **R5** — remove real inefficiencies, document the ceiling, don't merge a non-win as a win | blind reviewer | **Met** | Every table figure and both noise-floor ranges recomputed from the committed JSON and exact. The shipped direct-to-Arrow change is presented as an explicit **null** result kept for the copy reduction and the closed `symlink_metadata` TOCTOU, "not speed"; stat-in-pool measured 2.2× worse and reverted; the jwalk ceiling (directory = unit of parallelism) named; four levers deferred with reasons. |
| **R6** — refactor for clarity/testability; schema + defaults unchanged | blind reviewer | **Met**, with nits **C4-F3** and **C4-F4** | `src/crawl.rs` (901 lines, 14 unit tests) holds the work queue, per-file measurement, `PartitionBuffer` and marker I/O; the bin keeps the concurrency scaffold. `get_schema()` **byte-identical to `main`** and absent from the diff. Crawl output unchanged. |
| **R7** — new surface opt-in only, `.scd` same commit | blind reviewer | **Met** | No tuning knob was added — the perf work was a null result and the one lever needing a knob is deferred. `--allow-errors` is the sole addition: `#[arg(long)]`, no short, defaults false, defined only in `src/cli.rs`, `.scd` updated in the same commit. No pre-existing default or short-flag meaning moved. |
| **R8** — assessment produced; low-risk cleanups applied; larger items recorded | **orchestrator** (document) + blind reviewer (code half) | **Met** | `ASSESSMENT.md` byte-unchanged since cycle 3: 4 applied cleanups with why-safe, 7 deferrals with why-not-here. Code half: `index_glob` across all three readers, layout constants centralized (the duplicate `const ROOT_PARTITION` deleted from `xdu-view.rs`), `RESERVED_INDEX_NAMES` added. **The reviewer fact-checked all five `issues/*.md` against source and every claim holds**, including reproducing the scoped-run marker reset. |
| **R9** — HPC benchmark protocol | blind reviewer | **Met** | `bench/HPC-PROTOCOL.md` §1–§7 incl. per-filesystem environment tables, load/coordination warning, cache-state rules, FS-side counters (`lctl md_stats`, `mmpmon`, `arcstat`) and a fill-in reporting template. Every command it cites exists. |
| **R10** — invariants preserved + full pre-release gate clean | blind reviewer | **Met** for the gate and §1–§13 as written; **C4-F2** is a doc-accuracy breach of §13 | Gate clean. §1 byte-identical. §2 partial→rename→prune preserved, 0 `.partial` after every success, readers never glob `.partial`. §3 reserved-name rejection unconditional. §4 re-driven. §6/§7/§8/§9/§12 unchanged. §13 stdout clean and pipeable in every drive. §5 unchanged from `main` and recorded as a follow-up (see "Surfaced, no action"). |

### Findings — cycle 4

Four CONFIRMED: one MEDIUM, three LOW. None CRITICAL, none HIGH, **no regression introduced by this
branch**. Every one independently reproduced by the orchestrator.

#### C4-F1 — MEDIUM · CONFIRMED · `src/crawl.rs:458` (`finalize` prune loop) + `src/bin/xdu.rs:636`

**A partition whose directory becomes unreadable is finalized as empty, so the prune loop deletes every
row that partition previously held.** `finalize()` prunes from `num_chunks..`; a partition that yielded
a hard error and zero files still finalizes with `num_chunks == 0`, so the prune starts at chunk 0 and
walks the whole contiguous tail — the entire prior partition. With `--allow-errors` the run then exits
**0** and writes a completion marker.

Failure scenario: an index holds 10 rows for `alpha`. A permission change makes the source `alpha`
directory mode 000 — routine on shared HPC scratch. The operator re-runs with `--allow-errors`, the flag
this branch added precisely so a crawl survives unreadable regions. The 10 good rows are destroyed.

```
initial rc=0 · total rows=11  alpha=10 · alpha chunks: 000000.parquet
# chmod 000 src/alpha; re-index --allow-errors
error: /private/tmp/xdu-f1/src/alpha: Permission denied (os error 13)
Finished alpha (0 files, 0 B, pruned 1 stale, 1 errors)
rc=0 · alpha dir now: [] · total rows=1
marker: files=1  errors=1
```

**Not a regression — this branch strictly improves it.** The identical scenario driven against a release
build of `main` from a throwaway worktree:

```
main initial rc=0 · rows=11  alpha=10
Finished alpha (0 files, 0 B, pruned 1 stale)
main reindex rc=0 · alpha dir now: [] · rows=1 · marker present: no
```

`main` destroys the same 10 rows, exits 0, and prints **no error diagnostic whatsoever**. HEAD adds the
per-path errno line, the `pruned 1 stale` report, a non-zero exit in default mode, `errors=1` in the
marker, and a reader warning on every subsequent query. The defect is the prune *scope*, which is
textually identical in `main` (`for chunk_id in num_chunks..`).

**Same mechanism as C3-F2**, which the cycle-3 human gate resolved as record-don't-fix: both are
`finalize`'s per-partition prune reconciling against this run's chunk count rather than against reality.
C3-F2 leaves phantom rows; C4-F1 destroys real ones — the sharper half of the pair, and the one that is
**not recorded anywhere**. `issues/marker-scoped-run-attestation.md` and
`issues/orphan-partition-survives-reindex.md` both circle it without naming it, and the `--allow-errors`
man-page text does not mention that a tolerated error can delete prior rows.

**Maps to:** R2 (a lost file), invariants §2/§2b/§2c. Touches `src/crawl.rs` and `src/bin/xdu.rs` →
**mandatory human gate fires.**

**Recommended disposition:** record, do not fix here — matching the C3-F2 precedent one cycle ago.
A fix changes crawl behavior on an error path and needs its own tests and measurement; folding it into
cycle 4 of a ≤3-cycle budget is how a hardening pass never lands. The minimum this pass should carry is
an `issues/{slug}.md` + `ROADMAP.md` seed and one sentence in `doc/xdu.1.scd`'s `--allow-errors` text.
**The human decides.**

#### C4-F2 — LOW · CONFIRMED · `AGENTS.md:48` + `.agents/factory/invariants.md:183`

**Ground truth asserts a fact the code contradicts.** Both files state *"Version is single-sourced from
`Cargo.toml` — `clap` derives `--version` from `CARGO_PKG_VERSION`."* No argument struct in
`src/cli.rs` sets `version` in its `#[command(...)]` attribute, so clap never registers the flag:

```
$ ./target/release/xdu --version ; echo $?
error: unexpected argument '--version' found
2                                     (identical for xdu-find, xdu-view, xdu-rm)
$ grep -n version src/cli.rs
(no output)
```

The `--version` defect itself is pre-existing and correctly recorded in
`issues/version-flag-missing.md` — which quotes **this exact AGENTS.md sentence** as its evidence. So
`a6b8a38` ("resync AGENTS.md + invariants.md with the code") left the resynced document asserting the
opposite of the code, in the one file `AGENTS.md` itself designates as ground truth
(*"When something below disagrees with the code, the code is ground truth — fix this file."*).

**Maps to:** invariant §13, R10. **Recommended disposition:** fix — it is a two-line documentation edit
in files this branch already rewrote, and leaving it re-arms the same trap for the next agent.

#### C4-F3 — LOW · CONFIRMED · `src/bin/xdu.rs:39`

The concurrency-contract doc comment — the artifact R6 requires, in a high-blast-radius file — names a
function that does not exist:

```
$ grep -rn "record_from_metadata" src/ tests/
src/bin/xdu.rs:39:/// (`record_from_metadata`), and Parquet finalization (`PartitionBuffer`) live in
```

The real helper is `crawl::file_size_and_atime`. A stale name in the one comment a maintainer is meant
to navigate by. **Maps to:** R6. **Recommended disposition:** fix — one word.

#### C4-F4 — LOW · CONFIRMED · `src/lib.rs:17`

`FileRecord` — named by invariant §1 as part of the schema contract, alongside `get_schema()` — is dead
production code after the direct-to-Arrow rewrite. Its only remaining references are its own unit tests:

```
$ grep -rn "FileRecord" src/ tests/
src/lib.rs:17:pub struct FileRecord {
src/lib.rs:740,743,756,761,766,778   (all inside #[cfg(test)])
```

A `pub` struct raises no dead-code warning, so it can drift out of agreement with `get_schema()`
unnoticed — the exact coupling §1 exists to prevent, now with nothing holding the two together.
`get_schema()` itself is byte-identical to `main`, so this is hygiene, not a violation. **Maps to:** R6,
invariant §1. **Recommended disposition:** the human's call — delete it with its tests, or keep it and
add a comment binding it to `get_schema()`. Either closes the drift.

### Surfaced, no action — checked and dismissed

Both were raised by the blind reviewer and both are real behaviors; neither is a finding against *this*
contract, and recording why is part of the evidence spine.

- **DuckDB injection surface (invariant §5).** `index_glob` still raw-`format!`s the index path and
  partition name into `read_parquet(...)`; a `'` in `-u` escapes the string literal (reproduced:
  `Parser Error: syntax error at or near "UNION"`), and `-u '*'` silently widens to the whole index.
  The rubric makes a §5 violation auto-CRITICAL, so this deserved the explicit check it got.
  **Dismissed because:** `git show main:src/bin/xdu-find.rs` carries the identical raw `format!` — this
  pass *centralized* the seam without changing what reaches SQL, and R8's own text authorizes recording
  rather than attempting anything "larger or riskier". It **is** recorded, in `ASSESSMENT.md` and at
  `ROADMAP.md:166` with a `**Seed:**` at `:176`, with a sound reason: escaping changes the emitted SQL
  for exotic names, which is a behavior change the GOAL's non-goals put out of bounds here. The
  deferral is contract-compliant. It is also now the single highest-value item in the follow-up queue.
- **Orphan partition survives re-index.** Independently re-derived by the reviewer; this is C3-F2,
  already recorded per the cycle-3 human gate. No new action.

### Unmapped changes (possible scope creep)

| Change | Judgment |
|---|---|
| `.gitignore` `+/.idea/` (own `[chore]` commit `c613634`) | Maps to no R-ID. One ignore line; trivial and isolated. Not worth a loop — unchanged from cycle 1's judgment. |
| Six `[harness]` commits under `.agents/` | Out-of-GOAL but **process-sanctioned** — `AGENTS.md` designates `/xdu-harness` as the route from `META.md` findings back to `.agents/`. The reviewer read the review-facing ones and confirmed **every change strengthens a gate**: `temp_index.sh` now rebuilds stale binaries instead of measuring them, this skill now forbids reporting an unearned pass when `scdoc` is absent, and the rubric/invariants **added** `src/crawl.rs` and `index_glob` to the high-blast-radius list. No non-negotiable gate was weakened. |
| `.gitignore` `bench/results/*` + `!baseline.json` + `!comparison-*.json` | In scope — R4 needs the reference committed and local runs ignored. |
| `AGENTS.md` / `ROADMAP.md` / `issues/` growth | In scope — R4/R6/R8 bookkeeping and the deferral convention R8's records depend on. |

All six stated non-goals respected: no schema change, no backend work, no new query/deletion features,
no changed default, no repurposed flag, no TUI rewrite.

### Human-gate triggers

**The mandatory human gate fires.** C4-F1 is CONFIRMED and touches `src/crawl.rs` and `src/bin/xdu.rs`
— both on the high-blast-radius list this branch itself expanded — and maps to the atomic-finalize
invariant (§2). C4-F4 touches `src/lib.rs` and invariant §1. Per this skill's Step 4, **no further
lifecycle step may run without explicit human sign-off.**

### Non-convergence

This is **cycle 4 of a ≤3-cycle bound, the second consecutive overrun.** Cycle 3's `blocked_reason`
authorized this cycle specifically; nothing authorizes a fifth.

The honest read of the trend: cycles 3 and 4 each found exactly one MEDIUM defect plus low-severity
residue, and **neither was introduced by this branch** — C3-F1 was a gap this pass opened and half-closed
in the same pass, C4-F1 is behavior `main` has always had and that HEAD strictly improves. The pass is
not diverging; a large, genuinely load-bearing diff is being read more carefully each round, which is
what a blind full pass is *for*. But a review that keeps finding pre-existing defects in adjacent code
has stopped grading the contract and started auditing the codebase — and R8 exists precisely to route
that work to `issues/` instead of into this branch.

**Recommendation:** land C4-F2 and C4-F3 (documentation and a one-word comment — no `src/` logic), take
the human's call on C4-F4, and record C4-F1 as an `issues/` follow-up rather than fixing it here. Then
**publish without a cycle 5**: none of the four findings is a regression, R1–R10 are all met, and the
remaining items are pre-existing defects that the deferral convention was built to carry.
