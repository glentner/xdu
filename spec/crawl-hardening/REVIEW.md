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
