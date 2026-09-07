---
status: unshaped
kind: refactor
appetite: small
---

# Internal cleanups surfaced by the crawl-hardening pass

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

The crawl-hardening assessment applied its low-risk cleanups at the time (a shared
`lib::index_glob` behind every reader's Parquet glob, one home for the index-layout constants,
reader awareness of the completion marker) and recorded what was too risky or too large to fold
in: routing the DuckDB injection surface through validated escaping on the `index_glob` seam,
reconciling `xdu-view`'s `format_file_count` with `lib::format_count`, and lifting the pure TUI
helpers — `strip_ansi` above all, which is load-bearing for terminal safety — out of the
2,500-line `xdu-view` into `lib` where they can be tested. Most of this is invisible to users and
decides how much of the codebase stays testable as it grows.

## Why it was deferred

Too risky or too large to fold into the hardening pass itself; recorded in
`spec/crawl-hardening/ASSESSMENT.md` instead. This file retrofits the `issues/` back-reference so
the roadmap entry complies; no new scoping was done.

## Outcome / vision

The injection surface escaped behind one validated helper, one count formatter, and the pure TUI
helpers living in `lib` under test — coordinated with the terminal-safety fix, which touches the
same file.

## Sketch of the acceptance criteria

- **R1** — All reader SQL construction SHALL route through one validated escaping helper at the
  `index_glob` seam.
- **R2** — The duplicated count formatters SHALL be reconciled into one.
- **R3** — The pure `xdu-view` helpers (first `strip_ansi`) SHALL live in `lib` with unit tests.

## Notes

- Full record: `spec/crawl-hardening/ASSESSMENT.md` (including evaluated-and-rejected
  performance levers).
- Coordinate with: `issues/xdu-view-terminal-safety.md` (same file).
- Found by: crawl-hardening pass; back-reference retrofitted 2026-09-07.
