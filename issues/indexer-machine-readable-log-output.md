---
status: unshaped
kind: feature
appetite: small
---

# Machine-readable log output for scripted and cron-driven crawls

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

`xdu` has one output posture: rich indicatif spinners on a TTY, plain lines otherwise. The
non-TTY path already keeps stdout clean and prints `Indexing …` / `Finished …` records to stderr
(`src/bin/xdu.rs:101-103`, `:117-119`, `:460-467`), so a cron job captures *something* — but the
records carry no timestamp, no severity, and no stable machine-parseable shape. A 3 AM cron
failure leaves a log that says what finished without saying when, in what order relative to
wall-clock events elsewhere, or at which severity a warning was emitted. There is no `--log-format`
or `--quiet` flag; the only dial is "TTY or not", which conflates rendering with record structure.

## Why it was deferred

Raised by the maintainer from production practice (regular scripted auditing), not found in a
factory pass — so there is no passing scope it fell out of. It is **pre-existing** on `main`. The
genuine open question is the record format (timestamped severity-tagged lines? JSON lines?
key=value? a real syslog transport?), and that choice determines the appetite — which is why this
is a seed, not a plan.

## Outcome / vision

A cron-driven `xdu` run emits a log from which a script (or a human at 3 AM) can reconstruct the
run: when it started and with what arguments, per-partition completions with counts, warnings with
severity, the completion-marker verdict, and a final summary whose presence implies the exit
status. The interactive TTY display is untouched.

## Sketch of the acceptance criteria

- **R1** — WHEN `xdu` runs without a TTY (or with an explicit log-format flag), every diagnostic
  record SHALL go to stderr as a single timestamped, severity-tagged line.
- **R2** — The log SHALL contain run-start (with arguments), per-partition finish (with file and
  byte counts), warning, completion-marker, and final-summary records, so the log alone explains
  the exit status.
- **R3** — Stdout SHALL stay clean and pipeable in every log mode (the existing non-TTY invariant
  in `src/bin/xdu.rs:185`, `AGENTS.md` §13).

## Notes

- Related: the companion progress-trust work, shipped on `main` — see
  `spec/crawl-progress-misleads-on-huge-trees/` (the interactive half of the same output surface).
- Found by: maintainer, from scripted-audit practice; recorded 2026-09-07.
