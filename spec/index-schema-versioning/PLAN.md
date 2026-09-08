# PLAN — On-disk index schema versioning

> **Status:** Draft for review · **Last updated:** 2026-09-07
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). No backing `research/` — this is an
> `appetite: small` lean plan; the unknowns were settled by targeted reads of the marker and
> reader-startup code, recorded in §4.

## 1. Summary

The format version rides the existing run-completion marker: `crawl::completion_marker_contents`
gains one `format=N` line sourced from a new `lib::INDEX_FORMAT_VERSION` constant (1 for the
current three-column layout), and a new lib-level gate refuses before any query when the version
is missing or unrecognized. No new on-disk names, no CLI flags, no SQL changes — the writer's
clear-after-pre-flight / write-on-success sequencing and the readers' guarded marker read are
reused as-is, which keeps the change inside the existing §2b machinery instead of beside it.

## 2. Design

**Writer (`src/crawl.rs`, `src/lib.rs`).** New constant `pub const INDEX_FORMAT_VERSION: u32 = 1`
in `src/lib.rs`, next to `COMPLETION_MARKER` — the single source both sides name, so writer and
gate cannot drift apart. `completion_marker_contents` appends `format={INDEX_FORMAT_VERSION}\n`
as the last body line (append, not prepend, so existing `contains`-style assertions on the body
stay stable). Nothing else in the write path moves: the version is attested by the same
clear-after-pre-flight / write-only-on-success ordering that attests the counts, so R1 inherits
that ordering by construction. A `--partition`-scoped run rewrites the marker from its own stats
as today; the version line carries the same constant either way, so the scoped-run limitation is
neither fixed nor widened.

The existing `xdu=<crate version>` body key is the *tool* version, not the format version, and
must not be read as one: tool releases ship without format changes, so keying compatibility off
it would refuse readable indexes on every upgrade. The gate parses only `format=`.

**Reader gate (`src/lib.rs`).** New `pub fn completion_marker_format(body: &str) -> Option<u32>`,
mirroring `completion_marker_errors` line-scan style (first key trimming to `format`, strict
`u32` parse, `None` for absent or garbage). The guarded marker read currently inline in
`index_completion_warning` (one stat; skip non-files and over-`MARKER_READ_LIMIT` sizes; degrade
unreadable bodies to empty) is extracted into one shared helper both the warning and the gate
use, so the FIFO/oversize/huge-file protections stay single-sourced. New
`pub fn index_version_error(index: &Path) -> Option<String>` returns `Some(diagnostic)` when no
understood version can be established — absent marker, non-file, oversized, unreadable, missing
`format=` key, unparseable value, or a value != `INDEX_FORMAT_VERSION` — and `None` when the
version matches. Non-file/oversized/unreadable fail closed to refusal rather than the warning
path's silence: when the question is "which layout are these rows", an unreadable attestation
is not queryable, and those states are pathological, not legacy.

Each bin calls the gate where it already calls `index_completion_warning`, *before* it, and
bails on `Some`: stderr diagnostic plus non-zero exit, before any DuckDB connection, before any
prompt, and — for `xdu-view` — before raw mode / the alternate screen, alongside the existing
warning block that already runs pre-terminal. On `None` the existing soft-warning path runs
unchanged, so a versioned `--allow-errors` index still warns exactly as today (R4). The version
is compared as a parsed `u32` in Rust and never reaches SQL.

Diagnostics name the found version, the supported one, and the remedy, e.g.
`error: index <path> has format version 7, but this xdu supports version 1; re-index with this
xdu to query it`, and for the unversioned case
`error: index <path> carries no format version (predates index versioning or from an
interrupted run); re-index with this xdu to query it`. Bins surface them via their existing
`anyhow::Result` failure (stderr + exit 1).

**No CLI or man-page change.** No flag is added or altered, so `src/cli.rs` and `doc/*.scd` are
untouched; the refusal diagnostic itself documents the remedy at the point of failure. Adding
`.scd` prose for a behavior with no flag would buy scdoc render risk for no discoverability.

**Tests.** Lib unit tests: writer↔reader pin extended to the `format=` line (same pattern as the
existing `completion_marker_errors` pin test), plus gate matrix — fresh body accepted, missing
marker refused, versionless pre-versioning body refused, garbage refused, wrong number refused,
tolerated-errors body with good version accepted. New `tests/version_tests.rs` driving the real
binaries via `tests/common` (`mod common;`, `binary_path`, never reimplemented helpers):
fresh-index round-trip accepted by find/rm with no version diagnostic; marker deleted →
find/rm/view all exit non-zero naming the version and re-index, rm unlinks nothing; `format=7`
marker → same refusal. `binary_path` gains the `xdu-view` arm (refusal happens pre-terminal, so
view is drivable without a TTY; only the refusal path is tested for view).

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1 | `INDEX_FORMAT_VERSION` + `format=` line in `completion_marker_contents`; existing write-on-success ordering attests it |
| R2 | `index_version_error` gate in all three bins pre-query (view: pre-terminal); bail with found-vs-supported diagnostic, non-zero exit, no rows shown, rm deletes nothing |
| R3 | Gate fails closed on absent / versionless / unparseable markers with a re-index diagnostic; never falls through to the query |
| R4 | Gate returns `None` for same-build indexes; `index_completion_warning` path untouched, so tolerated-error warnings persist |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) (§1–§13)
before research and again after this design was drafted.

- §1 schema stability — honored: zero column changes; this *is* the version field §1 demands before any schema evolution.
- §2/§2b marker ordering — honored: no new sequencing; the version is written and cleared exactly when the marker is. Guarded-read protections (FIFO, oversize cap) preserved via the shared helper.
- §2c fail-loud — untouched: failure paths still write no marker, which now additionally reads as unversioned.
- §3 partition scheme — honored: no new index-root names, so `RESERVED_INDEX_NAMES` and the work-queue guard are untouched in both directions.
- §4 rm safety — honored: refusal precedes query/prompt/deletion; no `--limit` interaction (no query runs at all).
- §5 injection — clean: version parsed as `u32` in Rust, compared in Rust, never interpolated into SQL.
- §6 Unix-only, §7 concurrency, §8 symlinks, §9 sort — untouched.
- §10 CLI truth — honored: no CLI change, hence no `.scd` churn; diagnostics carry the remedy.
- §11 altitude — honored: parsing and gating live in lib, bins stay thin; integration tests use `tests/common`.
- §12 terminal safety — honored: view gates before touching raw mode or the alternate screen.
- §13 conventions — honored: diagnostics to stderr, stdout stays pipeable; no `spec/` R-IDs in source; prose voice per AGENTS.md.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| R3 hardens absent-marker from §2b soft warning to hard refusal | The GOAL's flag-day contract: a versionless index is exactly the silent-misread hole being closed | Grandfathering markerless indexes keeps every pre-versioning index blind-readable forever, which defeats the goal; explicitly decided against in shaping (GOAL Clarifications) |
| Non-file / oversized / unreadable marker now refuses instead of staying silent | Fail-closed version establishment: an unreadable attestation cannot answer "which layout are these rows" | Treating them as "no information, carry on" re-opens blind reads for corrupt states; these are pathological, never legitimate legacy indexes |

## 4. Rabbit holes (resolved)

No fan-out (lean path). Targeted reads settled the small unknowns:

- Marker body already carries an `xdu=` tool-version key — resolved to add a separate `format=`
  key and never parse `xdu=` as compatibility, since tool releases outpace format changes.
- The guarded marker read (stat once, skip non-files, `MARKER_READ_LIMIT`, degrade-to-empty) is
  inline in `index_completion_warning` — resolved to extract one shared helper rather than duplicate
  the FIFO/oversize protections in the gate.
- `xdu-view` warns pre-terminal with a comment saying so — resolved to gate at that same block,
  keeping §12 untouched.
- `tests/common::binary_path` knows `xdu`/`xdu-find`/`xdu-rm` but not `xdu-view` — resolved to add
  the arm; view's refusal path needs no TTY.
- New-dotfile vs marker-body for the version — resolved to marker body: no new §3 reserved name,
  no new collision class, zero new write sequencing.

## 5. Risks & open questions

- Flag day is real: every index built before this ships refuses until re-indexed. Accepted in the
  GOAL; the diagnostic must make the remedy obvious since this is the first refusal these users see.
- A reader from *before* this change cannot diagnose a versioned index it doesn't understand —
  no backport (GOAL non-goal). Only forward-facing detection is built here.
- Scoped (`--partition`) runs keep rewriting the whole-index marker; the version line is constant
  so it survives that correctly, but the counts limitation stays open in its own issue.

## 6. Verification strategy

Lib unit tests pin the writer↔reader contract and the gate matrix (`cargo test --lib`);
`tests/version_tests.rs` drives the real `xdu`/`xdu-find`/`xdu-rm`/`xdu-view` binaries against
throwaway `tempfile` indexes asserting exit codes, stderr diagnostics naming the version and the
re-index remedy, and (for rm) that refused runs unlink nothing. Spot-drive the refusal flows via
`.agents/factory/bin/temp_index.sh sh -c '…'` (delete/tamper the marker, run each reader).
Final phase runs the mirror gate: `cargo fmt --check`, `clippy -D warnings`, full `cargo test`.

---

*Backing research (if present): none — lean path, see §4.*
