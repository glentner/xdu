# PLAN — Machine-readable log output for scripted and cron-driven crawls

> **Status:** Draft for review · **Last updated:** 2026-09-10
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). No `research/` — lean path
> (`appetite: small`): a few targeted reads of `src/bin/xdu.rs`, `tests/crawl_tests.rs`,
> and `doc/xdu.1.scd` settled the design.

## 1. Summary

Every non-TTY stderr diagnostic in `src/bin/xdu.rs` already funnels through two sites —
the driver-thread `report` closure and direct `eprintln!` calls for run-start and the final
summary — so the change is a routing job, not a rewrite. A new lib-level formatter prefixes
each record with a UTC timestamp and severity tag while keeping the existing human message
as the line's tail; the TTY arms stay byte-identical. Two records are missing and get added:
run-start with effective arguments, and the completion-marker verdict, plus an explicit
failure record on the paths where today only anyhow's untimestamped `Error:` print speaks.
No CLI change, no index or marker change, three phases.

## 2. Design

**Record formatter (new, in `src/lib.rs`).** A `LogLevel` (`Info` / `Warn` / `Error`) and a
pure `format_log_record(level, message) -> String` producing one line:
`<timestamp> <LEVEL> <message>`, e.g. `2026-09-10T03:00:01Z INFO Indexing /data …`.
The timestamp is UTC at second precision, computed from `SystemTime` with a std-only
civil-from-days conversion — `chrono` is only a transitive dependency (via `arrow`) and one
timestamp line does not justify promoting it to a direct one. The function lives in `lib`
per §11 so unit tests reach it; the binaries stay thin callers.

**Routing (in `src/bin/xdu.rs`, non-TTY arms only).** Each existing emission keeps its
message tail and gains the prefix:

- Run-start `Indexing <dir> …` → `INFO`, extended with the effective arguments (jobs, size
  mode, partition filter, `--allow-errors`) so the log records what was asked, not just what
  was walked. The `Indexing <dir>` tokens stay.
- Per-partition `Finished <part> (… files, …)` (driver threads) → `INFO`, content unchanged.
- `warning: …` → `WARN`; `error: <path>: <detail>` → `ERROR`. Tails stay byte-identical —
  see the prefix-only rule below.
- Final `Completed … files (…) in …s` summary → `INFO`, content unchanged.
- New: a marker-verdict `INFO` record after `write_completion_marker` on the success path
  (what was attested: files, bytes, tolerated counts).
- New: an explicit `ERROR` record on each failure path before returning `Err` — the fail-loud
  bail and the early pre-flight rejects. anyhow's `Error: …` print carries no timestamp, so
  without this R3 has no timestamped witness. The `Err` return itself is untouched; exit
  codes and marker ordering do not move.

The `report` closure branches once: TTY → `mp.println` exactly as today; non-TTY →
formatted `eprintln!`. Each record remains a single `eprintln!` call, so no new output
interleaving class appears beyond what driver threads already produce.

**Prefix-only rule.** `tests/crawl_tests.rs` asserts on stderr with substring `contains`
checks (`"secret"`, `"errors"`, partition names, `"No partitions found"`). Prefixing keeps
every one of those green; rewriting a tail risks them. Build task: grep the tests for the
`error:` / `warning:` literals before deciding whether the ad-hoc inline prefixes stay in
the tail or yield to the tag — the constraint is that existing assertions keep passing
unmodified, plus one new integration test that locks the record shape.

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1 | `format_log_record` in `src/lib.rs` + routing of all non-TTY arms in `src/bin/xdu.rs` through it |
| R2 | Run-start record extended with arguments; per-partition `Finished`, warnings, marker-verdict, and final-summary records |
| R3 | Explicit `ERROR` record before each failure `Err` return; marker write stays success-path-only (ordering untouched) |
| R4 | Zero new stdout writes; no existing stdout path touched |
| R5 | TTY arms (styled `Indexing`/`Finished`/`Completed`, progress bars) byte-identical |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) (§1–§13)
**before** research and **again** after this design was drafted.

- §1 schema — untouched. No column, reader, or example changes.
- §2/§2b marker — ordering untouched: clear-after-pre-flight and write-on-success stay where
  they are; the verdict record only *reads* the stats the marker was just written from. The
  §2b scoped-run limitation stays recorded, not fixed.
- §2c fail-loud — untouched and reinforced: default still fails non-zero with no marker;
  `--allow-errors` stays opt-in; the failure record is additive, never a substitute for the
  `Err` return.
- §3 partition scheme — partition names reach only stderr text, never a glob or SQL string.
  No §5 surface: nothing new reaches DuckDB.
- §7 concurrency — the single-pool + driver-thread + `thread::scope` shape is untouched; the
  formatter is a pure function with no shared state.
- §10 CLI — no CLI change by GOAL non-goal, so no `cli.rs` change. `doc/xdu.1.scd` needs none
  either: its OUTPUT FORMAT section describes the Parquet layout, and the diagnostics prose
  speaks at behavior altitude ("go to standard error"), which stays true.
- §11 altitude — formatting logic in `lib` with unit tests; `xdu.rs` only routes.
- §13 conventions — stdout stays clean; no `R#`/`P#` ids in source; no version strings; prose
  voice per AGENTS.md.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| — | — | — |

## 4. Rabbit holes (resolved)

- Timestamp without a time crate → std-only UTC civil-date conversion, unit-tested; no new
  dependency for one line. Second precision suffices for cron correlation; sub-second
  ordering is still line order.
- Where the failure record lives → explicit record before the `Err` return, because anyhow's
  verdict print is untimestamped and R3 demands a timestamped witness.
- Existing stderr assertions → the prefix-only rule (verified against `tests/crawl_tests.rs`
  `contains` checks); exact tail wording is build's choice under that constraint.
- Man page → read `doc/xdu.1.scd`: nothing describes the stderr record shape, so no same-commit
  `.scd` change. If build finds otherwise, the §10 same-commit rule applies.

## 5. Risks & open questions

- Two records within one wall-clock second share a stamp; correlation across hosts assumes
  roughly-synced clocks. Both accepted for cron use — no NTP-grade promise is being made.
- Per-thread `eprintln!` interleave is a pre-existing class; one call per record keeps it
  where it is.
- No open questions for the human; timestamp precision and tag vocabulary (`INFO`/`WARN`/
  `ERROR`) are design choices recorded here, not clarifications owed.

## 6. Verification strategy

- Unit: `cargo test --lib` covers the formatter (line shape, tag set, timestamp format,
  single-line guarantee for hostile message input such as embedded newlines).
- CLI drives via `.agents/factory/bin/temp_index.sh` (pipes are non-TTY, so drives exercise
  the new path): every stderr line matches the timestamped-tagged shape; run-start /
  per-partition / marker / summary records present; a failing drive exits non-zero with an
  `ERROR` record and no marker file; stdout stays pipeable (existing reader tests cover it).
- New integration test in `tests/crawl_tests.rs` locks the record shape for the non-TTY path.
- Final phase runs the pre-release mirror: `fmt --check` + `clippy -D warnings` + `cargo test`.
