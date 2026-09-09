# GOAL — All four binaries answer `--version`

- **slug:** version-flag-missing
- **kind:** fix
- **appetite:** small

## Problem

Every `xdu` binary rejects the `--version` flag, in a released version:

```
$ xdu --version
error: unexpected argument '--version' found
```

The same holds for `xdu-find`, `xdu-view`, and `xdu-rm`, for both the long
and the short form. `--version` is the first thing an operator or a packaging
script runs, and all four man pages promise it works — each of `doc/xdu.1.scd`,
`doc/xdu-find.1.scd`, `doc/xdu-view.1.scd`, and `doc/xdu-rm.1.scd` documents
"`-V`, `--version`: Print version information." So the documentation describes
intended behaviour the binaries do not provide.

The mechanism is confirmed, not diagnosed: `grep -n version src/cli.rs` returns
nothing, so none of the four `#[command(...)]` blocks (`XduArgs`,
`XduFindArgs`, `XduViewArgs`, `XduRmArgs`) asks clap to derive the flag, while
`AGENTS.md` records the version as single-sourced from `Cargo.toml`. The
mismatch is man-pages-vs-code only: `gen-completions` builds from the same
`clap::Command` objects, so completions omit the flag exactly as the binaries
do. This defect is pre-existing on `main`, surfaced during `crawl-hardening` P4
while auditing the CLI surface, and deferred because a CLI change had no place
in a benchmarking commit.

## Outcome / vision

`xdu`, `xdu-find`, `xdu-view`, and `xdu-rm` all print their version and exit 0
for `-V` and `--version`. The string comes from `Cargo.toml` with no hardcoded
copy in `src/`, the generated completions offer the flag, and the man pages
become true rather than aspirational — with no documentation change required.

## Acceptance criteria (the contract)

- **R1** — WHEN a user runs any of `xdu`, `xdu-find`, `xdu-view`, or `xdu-rm`
  with `--version` or `-V`, the binary SHALL print version information and exit
  0 instead of rejecting the argument.
- **R2** — WHEN a user compares the printed version against the package version
  in `Cargo.toml`, the two SHALL agree, and no version string literal SHALL
  exist in `src/`.
- **R3** — WHEN completions are generated from `src/cli.rs`, the output for
  each of the four binaries SHALL offer `--version`.

## Non-goals (no-gos)

- No `doc/*.scd` change: the man pages already describe the intended behaviour,
  so a code-only change satisfies the same-commit rule rather than violating it.
- No `--version` for the `gen-completions` dev-helper binary itself; the
  contract covers exactly the four user-facing binaries.
- No version-output redesign: no custom format, no extra fields, no change to
  which package version is reported — whatever clap derives from `Cargo.toml`
  is the answer.
- No release mechanics: no version bump, no tag, no rebuild of the generated
  `share/` tree (completions and man pages are git-ignored build products).

## Clarifications

- None open. Both `-V` and `--version` are in scope because every man page
  documents both forms; `R1` covers the pair for all four binaries.

## Related materials

- Candidate: [`issues/version-flag-missing.md`](../../issues/version-flag-missing.md)
- CLI definition: `src/cli.rs` — the four `#[command(...)]` blocks with no
  `version` key (`XduArgs`, `XduFindArgs`, `XduViewArgs`, `XduRmArgs`)
- Man pages documenting the flag: `doc/xdu.1.scd`, `doc/xdu-find.1.scd`,
  `doc/xdu-view.1.scd`, `doc/xdu-rm.1.scd`
- Convention: `AGENTS.md` § *Version is single-sourced from `Cargo.toml`* and
  invariant §10 (CLI single source of truth)
