# REVIEW — Glob as the default path-match dialect (pilot)

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** `18bb76bea98a74ec40a9b97d26bc4bc3dff9df73`  ·  **Base:** `main`  ·  **Date:** 2026-09-07
- **Verdict:** changes-requested
- **Cycle:** 1 of ≤3 — mirrors `review.cycle` in `TECH.md` (escalate to human on non-convergence)

Contract-drift check: `git log --oneline main..HEAD -- spec/richer-search-glob-fuzzy-fulltext/GOAL.md`
shows only the original shaping commit `c1e51eb` — the locked contract did not move mid-build.

## Verification run

Commands actually executed and their outcomes (the spine of the review):

All dynamic evidence below was executed by the blind correctness subagent in the runnable repo;
the orchestrator sanity-checked the cited code locations and tree state but did not re-run the
full gate. Tree state was observed directly by the orchestrator.

- `cargo test` (blind reviewer) → all green: 69 lib + 5 view + 23 crawl + 1 offline + 18 rm,
  including new `test_glob_to_regex_*`, `test_with_path_pattern_glob_and_regex`,
  `test_regex_flag_restores_regex_matching`, `test_invalid_glob_deletes_nothing`.
- `cargo fmt --all -- --check` (blind reviewer) → clean.
- `cargo clippy --all-targets --all-features -- -D warnings` (blind reviewer) → clean.
- `.agents/factory/bin/temp_index.sh` drives (blind reviewer, fixture with `alice/logs/app.log`,
  `alice/data.bin`, `bob/notes.txt`, `root.dat`):
  - `xdu-find -p "*.log" --count` → `1`; `-p "*.bin"` lists `alice/data.bin`; `-p "*app*"` → `1`;
    bare `-p "app.log"` → `0` (whole-path anchoring, as documented).
  - `xdu-find -p "*" --count` → `4` = unfiltered count = `xdu-find --regex -p ".*" --count`.
  - `xdu-find -p "*.txt"` → `1` = `xdu-find --regex -p ".*\\.txt$" --count` → `1`.
  - `xdu-find -p "*.[l]og"` → `1`; `*.[lm]og` → `1`; `*.lo[g]` → `1`; `*.b[i]n` → `1`;
    `*.[!b]in` → `0`; `*.[a-m]og` → `0` (expected `1` — finding 1); `*.[a-z][a-z][a-z]` → `0`
    (expected `4` — finding 1).
  - `xdu-find -p "\\.log$"` → `0` vs `xdu-find --regex -p "\\.log$"` → `1` (default flipped,
    old spelling preserved behind the switch).
  - `xdu-find -p "' OR '1'='1"` → `0` (not `4`); `xdu-find -p "*'*"` → `0` exit 0
    (single-quote doubling intact).
  - `xdu-find -p "["` → exit `1`, stdout empty,
    stderr `Error: Unterminated character class at byte 0 in glob pattern: [`.
  - `xdu-find -p ""` → exit `1`, `Error: Empty glob pattern matches no path`.
  - `xdu-rm --dry-run -p "*.log" --force` → lists `app.log`, `1 file(s) would be deleted.`,
    count matches find; `xdu-rm -p "[" --force` → exit `1`, same diagnostic, target file
    still exists, post-check `xdu-find -p "*.log" --count` → `1` (fail-closed ordering).
  - `xdu-rm --dry-run -p "*" --force -l 2` twice → identical 2 rows
    (`deterministic_limit_clause` intact).
  - `printf "" | xdu-view -p "[" </dev/null` → exit `1`, same diagnostic, no TUI hang
    (translation precedes `enable_raw_mode`).
- Man-page renders (blind reviewer, `scdoc` present at `/opt/homebrew/bin/scdoc`):
  `doc/xdu-find.1.scd`, `doc/xdu-rm.1.scd`, `doc/xdu-view.1.scd` all render exit 0; no
  `^[ .']` roff-control lines. Reading form confirms the glob paragraphs, `* crosses
  directories`, bare-word notes, `--regex` entries, and the `--regex -p '\.tmp$'` find example.
  Stripped-presence form (`tr -d '[:space:]' | grep -qF`): `*.py` present on find+view,
  `*.tmp` present on rm, `\.tmp$` present on find, `--regex` present on all three.
  Counting form: `XDU_INDEX` 2× on find/view/rm → `got=2 want=2` each; `XDU_JOBS` 2× on rm →
  `got=2 want=2`. `--help` flag sets match the pages.
- `git status --porcelain` (orchestrator, post-handoff) → empty; no probe files left.
  No negative-control mutation of `target/` was performed by the reviewer, so no
  build-state restore was owed; release binaries intact
  (`./target/release/xdu-find --help | grep -c PATTERN` → `1`).
- Orchestrator spot-checks: `src/lib.rs:410-415` escapes `-` unconditionally (finding 1
  root cause confirmed by reading); `AGENTS.md:385,388,390` still document the regex
  default with no `--regex` (finding 2 confirmed, `git diff main...HEAD -- AGENTS.md`
  empty); `src/bin/xdu-view.rs:372` prompt still `"Pattern (regex): "` while `:974`
  translates glob and the man page says glob (finding 3 confirmed by reading).
- Gate applicability: the CI rollup state on `base` was not observed in this session
  (no `gh` here); nothing below claims a gate is satisfied that was not executed above.

## Requirement → evidence matrix

Bidirectional traceability. All three R-IDs were verified by the blind reviewer (spec-excluded
diff + executed drives); the orchestrator owns none (no R-ID is satisfied by a committed
document under `spec/` — the `':(exclude)spec/'` pathspec strips nothing graded here).

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 — bare `-p` means glob in find, view, rm | `src/lib.rs` `glob_to_regex` + `with_path_pattern`; `src/cli.rs` `PATTERN` help; `src/bin/xdu-find.rs`, `src/bin/xdu-rm.rs`, `src/bin/xdu-view.rs` call sites; `doc/*.scd` | `cargo test` green; `xdu-find -p "*.log"/"*.bin"/"*app*"` counts and listings; `-p "*"` equals unfiltered equals `--regex -p ".*"`; `?`, explicit classes, negation drives; `xdu-rm --dry-run -p "*.log"` matches find count; view startup shares `with_path_pattern`, interactive `/` shares `glob_to_regex`, `--help` shows glob dialect | ✅ with gap (finding 1: `[...]` ranges broken) |
| R2 — `--regex` restores full regex in all three | `src/cli.rs` long-only `--regex` on all three arg structs; regex bypass in `with_path_pattern`; `src/bin/*` passthrough | `xdu-find --regex -p "\\.log$"` → `1` vs bare → `0`; `xdu-rm --dry-run --regex -p "\\.log$"` lists target; `--regex -p "["` fails at DuckDB prepare (raw passthrough, not glob validation); injection probes closed; `--limit` determinism intact; `rm_tests` regex test ok | ✅ |
| R3 — invalid glob fails non-zero with diagnostic; rm deletes nothing | `with_path_pattern` `Err` propagation via `map_err` in find/rm before query/unlink; view startup translation before `enable_raw_mode`; view `/` prompt `Err` → status-bar message | `xdu-find -p "["/""/"abc\\"` exit `1` with named diagnostics, stdout empty; `xdu-rm -p "[" --force` exit `1`, file still exists, post-count intact; `xdu-view -p "["` exit `1` without owning terminal; `rm_tests` invalid-glob test ok | ✅ |

Unmapped changes (possible scope creep): none blocking. `ROADMAP.md` glob-pilot rewrite plus new
`issues/fuzzy-filename-matching.md` / `issues/duckdb-fts-evaluation.md` and the
`issues/richer-search-glob-fuzzy-fulltext.md` status flip map to GOAL non-goals (fuzzy/FTS
deferred as follow-up seeds — the factory deferral ledger, benign). Test conversions
(`crawl_tests.rs:861`, `offline_tests.rs:67`, `rm_tests.rs:566,610`) and the two new `rm_tests`
cases map to R1–R3. `pattern_display`/`format_display`/`clear` support maps to R1.

## Findings

Severity: **CRITICAL** (any `invariants.md` §1–§12 violation is auto-CRITICAL, **including lettered
subsections** such as §2b/§2c; a §13 project-conventions violation is **HIGH**) · **HIGH** · **MEDIUM** · **LOW**. Verdict: **CONFIRMED**
(reproduced) vs **PLAUSIBLE** (suspected, needs human triage). Only CONFIRMED findings auto-loop to
`xdu-build`.

### [MEDIUM/CONFIRMED] Glob character ranges broken: `-` always escaped in `push_class_literal`
- **Where:** `src/lib.rs:410-415`
- **Failure scenario:** any glob relying on a `[...]` range (`[a-z]`, `[0-9]`) matches only the
  literal set `{a, -, z}`, not the range. `xdu-find -p "*.[a-m]og"` over a fixture containing
  `app.log` returns `0` (expected `1`, since `l` is in `a-m`);
  `xdu-find -p "*.[a-z][a-z][a-z]"` returns `0` (expected `4`). Explicit chars, sets, and
  negation are unaffected (`*.[l]og`, `*.[lm]og`, `*.lo[g]`, `*.b[i]n` → `1`; `*.[!b]in` → `0`),
  isolating the defect to `-` handling.
- **Evidence:** blind-reviewer `temp_index.sh` drives above (observed `0` where `1`/`4` expected);
  root cause read by reviewer and confirmed by orchestrator: `push_class_literal` escapes `-`
  unconditionally, so `[a-z]` translates to regex `[a\-z]`.
- **Touches invariant / requirement:** R1 (partial — the contracted `*.py` example works; range
  subclasses do not). No `invariants.md` §1–§12 violation, so not auto-CRITICAL. Touches
  high-blast-radius `src/lib.rs`, so the human gate triggers regardless (see below).

### [MEDIUM/CONFIRMED] Operating-manual drift: `AGENTS.md` CLI surface still documents the regex default
- **Where:** `AGENTS.md:385,388,390` (vs `src/cli.rs` `--regex` ×3, `doc/*.scd` ×3)
- **Failure scenario:** the diff flips `-p/--pattern` to glob-by-default with a long-only `--regex`
  on all three tools and updates all three man pages, but `AGENTS.md` still reads
  `xdu-find … [-p/--pattern REGEX]` and shows no `--regex` on any tool. A later cycle reading
  the map (including the `xdu-plan` gate and the `-p` footgun note) reasons from the wrong default.
- **Evidence:** `git diff main...HEAD -- AGENTS.md` → empty; `grep -n "REGEX\|--regex\|PATTERN"
  AGENTS.md` → only `REGEX` at line 385, no `--regex` anywhere; `grep -n -- "--regex" src/cli.rs`
  → 3 hits; `./target/release/xdu-find --help` shows the new glob help. (Also `README.md:133,167,273`
  still says "Regex pattern" — outside the rubric drift scope of `AGENTS.md`/`invariants.md`,
  noted for completeness, not graded.)
- **Touches invariant / requirement:** rubric scope item 5 (operating-manual drift), descriptive
  section rather than a gate list → MEDIUM, not HIGH. `invariants.md` needs no update (§5
  "`--pattern` is escaped" still holds — the translated regex still passes through single-quote
  doubling, proven by the injection probes; §4 `--safe` gaps unchanged).

### [LOW/PLAUSIBLE] Interactive `/` prompt still advertises regex while the logic speaks glob
- **Where:** `src/bin/xdu-view.rs:372` (vs `:974` translation, `doc/xdu-view.1.scd` glob line)
- **Failure scenario:** the `/` entry now routes through `glob_to_regex` and the man page publishes
  `Set path pattern filter (glob).`, but the on-screen prompt remains `"Pattern (regex): "`. A user
  obeying the prompt types `\.py$` and gets zero rows (as a glob it becomes literal
  `^(?:\.py\$)$`); a glob user is told the wrong dialect.
- **Evidence:** `grep -n "Pattern" src/bin/xdu-view.rs` → `:372` untouched by the diff (diff context
  at `:967–986` shows the surrounding glob translation); rendered view page publishes the glob line
  while the prompt disagrees. Dialect-mismatch failure mode proven by analogy on the shared path
  (`xdu-find -p "\\.py$"` → `0` vs `--regex -p "\\.py$"` → `1`); the TUI prompt itself was not
  driven (no TTY in review) → PLAUSIBLE, not CONFIRMED. Needs human triage, does not auto-loop.

## Human-gate triggers

Set if any CONFIRMED finding touches the high-blast-radius core (`src/bin/xdu-rm.rs`,
`src/bin/xdu.rs`, `src/crawl.rs`, `src/lib.rs`, `src/cli.rs`) or a destructive-rm / schema-stability /
atomic-write / SQL-injection invariant — these **always** require human sign-off before
`xdu-publish`, regardless of auto-loop. (`invariants.md`'s *High-blast-radius files* header is the
authoritative path list; this copy may only ever **widen** to match it.)

- **TRIGGERED** — finding 1 (glob range defect) touches high-blast-radius `src/lib.rs`.
  Explicit human sign-off is required before `xdu-publish`, and the fix loops back through
  `/xdu-build` first. Findings 2 (map prose) and 3 (PLAUSIBLE prompt text) do not independently
  trigger the gate; no finding touches a §4/§1/§2/§5 invariant.

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

- Not run — plain `/xdu-review` invocation, no `completeness` argument.
