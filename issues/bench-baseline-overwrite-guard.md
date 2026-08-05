---
status: unshaped
kind: fix
appetite: small
---

# `run.sh baseline` defaults `--out` to the committed reference and silently destroys it

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

`bench/run.sh:458` — in `baseline` mode, `[ -n "$OUT" ] || OUT="$RESULTS_DIR/baseline.json"`.

`baseline` mode is also the *configuration set* anyone reaches for when capturing a comparison: it is
exactly the `s5 --scale 8 --jobs "1 2 4 8"` + `s2 --scale 2` + `s3 --scale 4` sweep that every
committed comparison document uses, and running it is how all of them were produced. So the natural
command for "measure my build against the reference" overwrites **the reference itself**, in place,
with no prompt and no backup.

`bench/results/baseline.json` is the R4 deliverable — the committed measurement a change is quantified
against. It is the one file in `bench/results/` that is not reproducible on demand: regenerating it
produces a *different* baseline (see `bench/scenarios.md`, "The noise floor" — two invocations of the
same binary differ by up to ~20% on the reference host), so an accidental overwrite does not just lose
a file, it silently redefines what "no regression" means for every later comparison.

The failure is quiet in both directions: the run succeeds, the JSON looks right, and the only evidence
is a `git diff` on a file the operator was not thinking about.

## Why it was deferred

Recorded during `crawl-hardening` P10, which hit the hazard directly — the phase checklist had to carry
an explicit "**Pass `--out` explicitly**" warning to avoid destroying the reference while capturing
`comparison-p5-ab.json`. P10 added a `usage()` warning and a note in `scenarios.md`'s provenance table
but **deliberately left the default unchanged**: changing it is CLI-surface work on the harness, and
that phase was already the largest in the cycle.

A warning in `usage()` only helps someone who reads `usage()`. The default is still loaded.

## Outcome / vision

Capturing a comparison cannot destroy the baseline by accident. Re-capturing the baseline on purpose
stays possible and obvious.

## Sketch of the acceptance criteria

- **R1** — IF `baseline` mode would write to an existing `bench/results/baseline.json` without an
  explicit opt-in, THEN `run.sh` SHALL refuse and exit non-zero, naming the flag that permits it.
- **R2** — WHEN `--out` is passed explicitly, `run.sh` SHALL write there without prompting.
- **R3** — The committed `baseline.json` SHALL remain regenerable by a single documented command.

## Notes

- Cheapest shape: require `--out` for `baseline` mode (no default at all), plus an explicit
  `--refresh-baseline` that is the only thing allowed to write the committed path. That makes the
  destructive case opt-in and the common case unambiguous.
- Alternative: refuse to overwrite an existing `baseline.json` unless `--force`, mirroring how the
  crawler treats a reserved partition name — reject rather than corrupt.
- Note the asymmetry worth preserving: comparison documents are cheap to regenerate, the baseline is
  not.
- Found by: `crawl-hardening` P10.
