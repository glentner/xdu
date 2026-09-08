---
status: unshaped
kind: fix
appetite: small
---

# Broken pipe: piping `xdu-find` into `head` exits 1 with an EPIPE error

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

A routine Unix pipeline fails:

```
$ xdu-find -f csv | head -1 > /dev/null
Error: Broken pipe (os error 32)
$ echo "${PIPESTATUS[0]}"
1
```

Measured on `feature/richer-index-schema` via `.agents/factory/bin/temp_index.sh`
(`xdu-find -f csv | head -1`; find's own exit is 1). The mechanism is ordinary:
Rust starts with `SIGPIPE` ignored, so once `head` exits and closes the read end,
the next `writeln!(out, …)` in `src/bin/xdu-find.rs` returns EPIPE instead of
killing the process, and `?` carries it out through `anyhow` as a stderr
diagnostic plus a non-zero exit. Every find output arm writes through that same
locked-stdout `writeln!` (`--top`, `--count`, all five `-f` formats), so all of
them share it. Under `set -o pipefail` — or any caller that checks pipeline
status — `xdu-find … | head` reads as a failure, which punishes exactly the
interactive exploration (`--top 3`, "show me one row") the tool invites.

Likely pre-existing on `main` (the `writeln!` loop predates the schema work);
confirmed here, not bisected there.

## Why it was deferred

Found during the P3 verify drive of `spec/richer-index-schema/`, whose `head -1`
csv-header assertion surfaced the diagnostic as noise beside a green gate. Fixing
it there would have changed process exit semantics and stdout error handling —
a behavior contract of its own — inside a cycle whose contract is the schema.
Recorded instead for a dedicated small cycle.

## Outcome / vision

`xdu-find` whose reader went away behaves like a conventional Unix writer: no
EPIPE diagnostic, and an exit status a `pipefail` caller accepts. The rows
already delivered stay intact; only the torn-downstream case changes.

## Sketch of the acceptance criteria

- **R1** — WHEN `xdu-find` output is piped to a reader that exits early (e.g.
  `head -1`), `xdu-find` SHALL exit without an EPIPE diagnostic and with a
  status a `pipefail` pipeline treats as success.
- **R2** — WHEN the reader consumes everything, output and exit status SHALL be
  exactly today's (no rows lost, no status changed).
- **R3** — Non-EPIPE write failures (e.g. `/dev/full`) SHALL still fail loudly:
  non-zero exit naming the error.

## Notes

- Suspected same class, worse shape, in `xdu-rm`: its dry-run/verbose paths use
  `println!`, which *panics* on EPIPE ("failed printing to stdout") rather than
  returning `Err`. Not reproduced — the throwaway fixture's six-line dry-run
  fits one pipe write and wins the race — so the fix cycle confirms it with a
  listing large enough to block, and decides whether rm rides along or stays a
  separate cycle.
- Out of scope: `xdu-view` (ratatui owns the terminal, not a stdout pipeline)
  and the crawler (progress goes to stderr).
- Shaping decides the mechanism: map `ErrorKind::BrokenPipe` to a silent exit 0
  at the `main` boundary, restore default `SIGPIPE` disposition so the process
  dies by signal, or something narrower. The mapping must not swallow R3's
  genuine failures.
- Found by: `richer-index-schema` P3 verify drive (2026-09-08).
