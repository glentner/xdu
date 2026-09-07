---
status: unshaped
kind: feature
appetite: small
---

# Fuzzy filename matching

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

Path matching is exact-only: `src/lib.rs` `QueryFilters` builds a `regexp_matches` predicate,
and the glob pilot (`glob_to_regex`, same file) translates globs to anchored exact regexes.
A user who misremembers a filename by a character or two — a typo, a version suffix, an
uncertain spelling — gets zero rows and no hint of what nearly matched.

## Why it was deferred

GOAL non-goal of the glob pilot (`spec/richer-search-glob-fuzzy-fulltext/GOAL.md`): the
matching algorithm (edit distance vs trigram vs DuckDB similarity functions), where the
match runs (SQL predicate vs post-filter), and how results rank are all open design with
scale implications on hundred-million-row indexes. None of that belongs in a dialect
change. Not a regression — approximate matching never existed.

## Outcome / vision

Approximate filename matching alongside exact matching: a query that names a nearly-right
filename returns the near misses ranked, without giving up the exact glob/regex paths.

## Sketch of the acceptance criteria

- **R1** — `xdu-find`, `xdu-view`, and `xdu-rm` SHALL accept an approximate-match option
  alongside the exact pattern.
- **R2** — The match tolerance SHALL be bounded and observable (a threshold or rank
  cutoff), so a deletion tool never acts on an unbounded similarity cloud.

## Notes

- Related: [`issues/richer-search-glob-fuzzy-fulltext.md`](richer-search-glob-fuzzy-fulltext.md)
  (parent seed), `spec/richer-search-glob-fuzzy-fulltext/` (glob pilot record).
- Found by: `richer-search-glob-fuzzy-fulltext` P4 deferral ledger.
