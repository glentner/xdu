# PLAN — All four binaries answer `--version`

> **Status:** Draft for review · **Last updated:** 2026-09-09
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). No backing `research/` — lean path
> (`appetite: small`, root cause confirmed by `grep` before shaping).

## 1. Summary

Add the bare `version` key to each of the four `#[command(...)]` blocks in `src/cli.rs`
(`XduArgs`, `XduFindArgs`, `XduViewArgs`, `XduRmArgs`). In clap 4 derive that one key enables
`-V`/`--version` on the command, with the string read from `CARGO_PKG_VERSION` at compile time —
so the version stays single-sourced from `Cargo.toml` by construction, and `gen-completions`
(which builds from the same `Command` objects) offers the flag with no change of its own. No
`doc/*.scd` change: the man pages already document exactly the `-V`/`--version` pair clap
derives, so the code becomes true to the docs rather than the other way around. One phase, one
small commit, proven by driving all four binaries plus the completion generator.

## 2. Design

The change is four identical one-line edits in `src/cli.rs` (attribute blocks at lines 12, 65,
148, and 197):

```rust
#[command(
    name = "xdu",
    version,
    about = "...",
    ...
)]
```

Bare `version` — no `= ...` value — is the whole point. With no explicit value clap falls back
to `CARGO_PKG_VERSION`, so there is never a string literal to drift from `Cargo.toml`. The
explicit form (`version = env!("CARGO_PKG_VERSION")`) would say the same thing in more words;
a hand-rolled `--version` arg would duplicate what the derive already provides and could drift
from it. Both rejected in favor of the idiomatic key.

Behavior after the change, per binary: clap intercepts `-V`/`--version` during argument parsing
and exits 0 after printing `<name> <version>`. The interception happens before any required-arg
validation and before `main` logic runs, which matters for `xdu-view`: the flag must print
without entering raw mode or the alternate screen, and it does so because parsing completes
before terminal setup is reached. The non-TTY drive in verification proves this rather than
asserting it from reading order.

`gen-completions` (`src/bin/gen-completions.rs`) needs no edit: it calls `XduArgs::command()`
(and the three siblings) and feeds the result to `clap_complete`, so the new flag appears in
the generated bash/zsh output by construction. That is what R3 asserts — a drive of the
generator into a scratch directory, not a reading of its source.

A regression test belongs in `tests/` alongside the other binary-driving integration tests,
using the shared helpers in `tests/common/mod.rs` (binary resolution, run wrappers) and
`env!("CARGO_PKG_VERSION")` as the expected string so the test itself holds no version
literal. It must not embed GOAL R-IDs in names or comments — spec ids restart per feature and
are forbidden in committed code. Name it for what it locks (e.g. `cli_version_tests.rs`), not
for the requirement id.

Out of scope, per GOAL non-goals: `doc/*.scd` (already correct), the `gen-completions` binary's
own flags (it takes positional dirs, not clap args), output-format design, and release mechanics.

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1   | Bare `version` key on all four `#[command(...)]` blocks; clap intercepts `-V`/`--version` pre-validation and exits 0. |
| R2   | No value on the key ⇒ clap reads `CARGO_PKG_VERSION` at compile time; no literal added to `src/`. |
| R3   | Same `Command` objects feed `gen-completions`; flag appears in generated output unmodified. |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) (§1–§13)
**before** research (gate #1, at shaping review) and **again** after this design was drafted
(gate #2, below). Touched sections only; the rest (§1–§9, §11, §12) are unaffected — no schema,
finalize, marker, rm-safety, SQL, concurrency, symlink, sort, or TUI change.

- §10 CLI single source of truth — the edit lands in `src/cli.rs`, the one definition;
  completions follow from it by construction. The same-commit doc rule is satisfied with a
  code-only change because the man pages already describe the intended flags; there is nothing
  to update, which is a fact about this fix, not a waiver of the rule.
- §13 Version single-sourced from `Cargo.toml` — bare `version` keeps the property by
  construction; the design adds no literal and the verify step greps for the current package
  version in `src/` to prove it. (Pre-existing `0.4.1` fixtures in `lib.rs` unit tests are stale
  marker-parse inputs, not the package version — out of scope, not to be "fixed" here.)
- §13 `share/` generated + ignored — completions regenerate from the edited structs; nothing
  under `share/` is committed.
- §13 No spec R#/P# ids in source — the new test file is named for the behavior, with no R-ID
  references.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| —         | —         | — |

## 4. Rabbit holes (resolved)

No fan-out on the lean path. Targeted reads only, all confirming rather than discovering:

- clap 4 `version` semantics → bare key reads `CARGO_PKG_VERSION`; the `-V`/`--version` pair it
  derives matches the man-page spelling on all four pages, so no doc edit is needed either way.
  Residual doubt (if any) dies at the build's binary drive, not in further reading.
- `-V` short-flag availability → no `short = 'V'` anywhere in `src/cli.rs`, so clap's default
  short flag collides with nothing on any of the four structs.
- `tests/version_tests.rs` → on-disk *index format* versioning (`INDEX_FORMAT_VERSION`), an
  unrelated contract this change does not touch; no test there can break, and none covers the
  CLI flag.
- `xdu-view` terminal safety → `--version` exits during clap parsing, before raw mode; the
  verify drive runs it non-TTY to prove the ordering end to end.

## 5. Risks & open questions

- Bash completion filenames in the R3 drive (`$dir/bash/<bin>`): assumed from `clap_complete`
  conventions. The verify as authored counts `--version`-containing files per directory rather
  than asserting exact paths, so a naming surprise fails loudly with a count instead of a
  confusing missing-file error. No human input needed.
- No open questions. The design is four attribute keys plus a regression test; nothing here
  needs a human decision before build.

## 6. Verification strategy

One phase, one `verify:` (see `TECH.md` P1): build all bins, then drive each of the four
binaries with both `--version` and `-V`, asserting exit 0 and that stdout contains the
`Cargo.toml` package version; assert that version string appears nowhere in `src/`; run
`gen-completions` into a scratch directory and assert all four outputs per shell offer
`--version`; finish with the standing gate (`clippy -D warnings`, full `cargo test`) to prove
no regression. The committed regression test in `tests/` locks the same behavior for the
future; the phase verify drives the real binaries regardless.

---

*Backing research: none on the lean path — see §4.*
