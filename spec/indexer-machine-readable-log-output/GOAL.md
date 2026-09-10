# GOAL — Machine-readable log output for scripted and cron-driven crawls

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).
> Keep this at the right altitude: solved and bounded, but not over-specified — leave design
> freedom for the plan. Edit requirements here; do **not** silently drift them during build.

- **slug:** indexer-machine-readable-log-output
- **kind:** feature
- **appetite:** small
- **promotes:** `issues/indexer-machine-readable-log-output.md`

## Problem

`xdu` has one output posture: rich indicatif spinners on a TTY, plain lines otherwise. The
non-TTY path already keeps stdout clean and prints `Indexing …` / `Finished …` records to
stderr, so a cron job captures *something* — but the records carry no timestamp, no severity,
and no stable machine-parseable shape. A 3 AM cron failure leaves a log that says what
finished without saying when, in what order relative to wall-clock events elsewhere, or at
which severity a warning was emitted. There is no `--log-format` or `--quiet` flag; the only
dial is "TTY or not", which conflates rendering with record structure.

The evidence is the non-TTY stderr path in `src/bin/xdu.rs`: the draw target is hidden off-TTY
(~line 100), the `Indexing …` record (~lines 120–122), the per-partition `Finished …` record
(~lines 476–482), and the `report` closure that coordinates diagnostics with the progress bars
on a TTY while keeping stdout clean and pipeable (~lines 184–192). Line numbers have drifted
a few lines since the seed was recorded; the mechanism is unchanged (verified 2026-09-10).

This matters for scripted auditing: the maintainer runs `xdu` from cron, where the log is the
only witness to the run. Today reconstructing a run — start time and arguments, per-partition
completions with counts, which warnings fired, whether the completion marker was written, and
the final verdict — requires correlating an untimestamped log with external evidence.

## Outcome / vision

A cron-driven `xdu` run emits a log from which a script (or a human at 3 AM) can reconstruct
the run: when it started and with what arguments, per-partition completions with counts,
warnings with severity, the completion-marker verdict, and a final summary whose presence
explains the exit status. The interactive TTY display is untouched.

## Acceptance criteria (the contract)

- **R1** — WHEN `xdu` runs without a TTY, every diagnostic record SHALL go to stderr as a
  single timestamped, severity-tagged, human-readable text line.
- **R2** — The non-TTY log SHALL contain run-start (with arguments), per-partition finish
  (with file and byte counts), warning, completion-marker, and final-summary records, so the
  log alone explains the exit status.
- **R3** — IF the run fails, THEN the log SHALL carry a record stating the failure, and no
  completion-marker success record SHALL appear.
- **R4** — Stdout SHALL stay clean and pipeable in every log mode (the existing non-TTY
  invariant).
- **R5** — WHILE attached to a TTY, `xdu` SHALL keep the existing interactive rendering;
  the new record shape applies to the non-TTY path only.

## Non-goals (no-gos)

- JSON-lines output, `key=value` records, or a syslog transport. The negotiated shape is
  timestamped severity-tagged text lines: greppable and skimmable without a parser.
- A `--log-format` selector or `--quiet` / severity-floor flag. Selection is the TTY itself;
  if a flag is ever wanted, it is a follow-up, not this cycle.
- Changes to the interactive TTY display, which stays as-is.
- Changes to the index format, the completion-marker schema, or stdout content.

## Clarifications

- **Q:** What record format should the log use — timestamped text, JSON lines, or minimal
  tagging? — **A:** Timestamped severity-tagged human-readable text lines (resolved
  2026-09-10). Rules out JSON and structured transports for this appetite.
- **Q:** Opt-in flag or new non-TTY default? — **A:** New default for the non-TTY path;
  no flag (resolved 2026-09-10). Accepts that existing cron logs change shape.
- **Q:** Is the TTY display in scope? — **A:** No; untouched (resolved 2026-09-10).

## Related materials

- Seed: `issues/indexer-machine-readable-log-output.md` (pre-existing on `main`, raised by
  the maintainer from scripted-audit practice, recorded 2026-09-07).
- Companion work, shipped: `spec/crawl-progress-misleads-on-huge-trees/` (the interactive
  half of the same output surface).
- Source anchors (verified 2026-09-10, drifted a few lines from the seed): `src/bin/xdu.rs`
  non-TTY draw-target hiding, `Indexing` record, per-partition `Finished` record, `report`
  closure; stdout-clean invariant per `AGENTS.md` §13.
