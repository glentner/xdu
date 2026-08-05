---
status: unshaped
kind: fix
appetite: small
---

# All four binaries reject `--version`, which every man page documents

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

Every `xdu` binary rejects the `--version` flag, in a released version:

```
$ target/release/xdu --version
error: unexpected argument '--version' found
$ target/release/xdu-find --version
error: unexpected argument '--version' found
```

`grep -n version src/cli.rs` returns nothing: none of the four `#[command(...)]` blocks (`XduArgs`,
`XduFindArgs`, `XduViewArgs`, `XduRmArgs`) sets `version`, so clap never derives the flag. Meanwhile
all four man pages document it —

- `doc/xdu.1.scd`, `doc/xdu-find.1.scd`, `doc/xdu-rm.1.scd`, `doc/xdu-view.1.scd`, each with
  "*-V*, *--version*  Print version information."

— and `AGENTS.md` states the version is single-sourced from `Cargo.toml` because "clap derives
`--version` from `CARGO_PKG_VERSION`". It would, if any command asked it to.

This is a **user-facing defect in a released version**, not a cleanup: `--version` is the first thing
an operator or a packaging script runs, and the documentation promises it works.

Two precisions that cut against the obvious reading, both verified:

1. **Shell completions are *not* affected.** `gen-completions` builds from the same `clap::Command`, so
   it omits the flag exactly as the binaries do. The mismatch is man-pages-vs-code only, which narrows
   the invariant §10 violation ("`src/cli.rs` is the one definition; completions and `doc/*.scd`
   describe it") to precisely that pair.
2. **The fix needs no documentation change.** The man pages are already correct — they describe the
   intended behaviour. So §10's same-commit rule ("a CLI change updates its `doc/*.scd` in the same
   commit") is satisfied by a code-only change of four attributes.

## Why it was deferred

Surfaced during `crawl-hardening` P4 while auditing the CLI surface for the benchmark harness. It is a
**CLI change** (invariant §10) and had no place in a benchmarking commit, and the crawl-hardening GOAL's
non-goals exclude CLI-surface work. Recorded rather than smuggled in.

It is **pre-existing** — identical in `main`, not introduced by that pass.

## Outcome / vision

`xdu`, `xdu-find`, `xdu-view` and `xdu-rm` all print their version and exit 0, the string comes from
`Cargo.toml` with no hardcoded copy anywhere in `src/`, and the man pages become true rather than
aspirational.

## Sketch of the acceptance criteria

- **R1** — WHEN a user runs any of the four binaries with `--version` or `-V`, the binary SHALL print
  its version and exit 0.
- **R2** — The reported version SHALL derive from `CARGO_PKG_VERSION`, with no version string literal
  in `src/`.
- **R3** — The generated shell completions SHALL offer `--version` for all four binaries.

## Notes

- Likely a one-line-per-struct change: add `version` to the four `#[command(...)]` attributes in
  `src/cli.rs`. Small enough that `AGENTS.md`'s "a one-sentence change may skip the lifecycle entirely"
  may apply — it wants its own `fix/` branch, not a slot behind the cleanup queue.
- Verify by driving all four binaries, not just one: the attribute is per-struct, so a partial fix is
  easy to ship.
- Found by: `crawl-hardening` P4; verified again 2026-08-05.
