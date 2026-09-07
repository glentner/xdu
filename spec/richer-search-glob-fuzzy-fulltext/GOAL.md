# GOAL — Glob as the default path-match dialect (pilot)

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).
> This cycle is the glob pilot of the broader
> [`richer-search-glob-fuzzy-fulltext`](../../issues/richer-search-glob-fuzzy-fulltext.md)
> seed; fuzzy matching and full-text search ship as follow-ups, not here.

- **slug:** richer-search-glob-fuzzy-fulltext
- **kind:** feature
- **appetite:** small · *the seed proposed `big` for glob+fuzzy+FTS together; shaping narrowed
  this cycle to the glob pilot, so the appetite shrinks with it.*

## Problem

Regex path matching is powerful but unfriendly: most users think in globs (`*.py`), not
anchored regex (`\.py$`). The matching surface is regex-only across `xdu-find`, `xdu-view`,
and `xdu-rm`, which narrows the audience for all three query tools. The project is pre-v1,
so the default dialect can still change without carrying a compatibility burden.

## Outcome / vision

`-p/--pattern` takes a glob by default in every query tool, and users who need full regex
power opt back into it with an explicit switch. Glob users get the syntax they already know;
regex users lose nothing except the default.

## Acceptance criteria (the contract)

- **R1** — WHEN a user passes `-p/--pattern PATTERN` without the regex opt-in switch, each
  of `xdu-find`, `xdu-view`, and `xdu-rm` SHALL interpret `PATTERN` with glob semantics
  (e.g. `*.py` matches indexed paths ending in `.py`).
- **R2** — WHEN the user passes the regex opt-in switch alongside `-p/--pattern PATTERN`,
  each of the three tools SHALL interpret `PATTERN` as a full regular expression, matching
  today's behavior.
- **R3** — IF `PATTERN` is not a valid glob and the regex switch is absent, THEN the tool
  SHALL exit non-zero with a stderr diagnostic; `xdu-rm` SHALL delete nothing in that case.

## Non-goals (no-gos)

- Fuzzy filename matching — a follow-up cycle, not this one.
- DuckDB full-text search — a follow-up; when it is ever scoped, the agreed first step is a
  written evaluation (spike report), not a shipped query path.
- Content-type filtering ("all video files over 1 GB") — waits on the on-disk schema
  versioning plus richer-schema work, so it cannot land here.
- A compatibility shim preserving regex-as-default beyond the opt-in switch. This is an
  accepted pre-v1 breaking change: a bare `-p` means glob now.

## Clarifications

- **Q:** Should this cycle ship glob+fuzzy+FTS together, or split into a pilot plus
  follow-ups? — **A:** Glob pilot only; fuzzy and FTS stay as follow-up seeds (resolved
  2026-09-07).
- **Q:** How should glob relate to the existing `-p/--pattern` regex flag? — **A:** Pre-v1,
  breaking changes are acceptable: glob becomes the default interpretation and a new CLI
  switch enables full regex behavior; the switch's exact spelling is `xdu-plan`'s call
  (resolved 2026-09-07).
- **Q:** What would the FTS criterion mean if it stayed in scope? — **A:** A spike report
  only; moot this cycle since FTS is deferred (resolved 2026-09-07).

## Related materials

- Seed: `issues/richer-search-glob-fuzzy-fulltext.md` (pre-shaped candidate, not a contract).
- `src/cli.rs` — the `-p/--pattern` definitions on all three query arg structs (and the
  `-p`/`--partition` footgun in `xdu` itself).
- `src/lib.rs` — `QueryFilters`, the `regexp_matches(path, …)` WHERE builder the glob
  translation must route through (injection surface, invariant 5).
