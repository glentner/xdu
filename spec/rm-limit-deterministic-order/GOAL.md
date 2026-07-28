# GOAL — Make `xdu-rm --limit` selection deterministic

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).
> Keep this at the right altitude: solved and bounded, but not over-specified — leave design
> freedom for the plan. Edit requirements here; do **not** silently drift them during build.

- **slug:** rm-limit-deterministic-order
- **kind:** fix
- **appetite:** small

## Problem

`xdu-rm` deletes files. When a run is capped with `--limit N`, the tool selects some N files from
the set matching the filters — but the selection has no defined, stable order. Two runs with the
same arguments against the same index can pick a **different** N files.

This is worst-case dangerous for a destructive tool: the standard safety workflow is to run
`--dry-run` first, read the preview, then re-run for real. If the preview and the real run can
select different rows, the preview is not a reliable promise of what will be deleted — the operator
can approve one set of files and destroy another. This is a HIGH-severity item in the bug backlog
and a violation of the `xdu-rm` deletion-safety invariant.

## Outcome / vision

`xdu-rm --limit N` always selects the same N files for a given index and filter set, so the
`--dry-run` preview is an exact, trustworthy manifest of what a subsequent real run will delete. An
operator who reads the dry-run output can rely on it completely.

## Acceptance criteria (the contract)

- **R1** — WHEN `xdu-rm --limit N` runs twice with identical arguments against an unchanged index,
  it SHALL select the exact same set of N files each time.
- **R2** — WHEN `xdu-rm --dry-run --limit N` is followed by the same command without `--dry-run`
  against an unchanged index, the files listed by the dry-run SHALL be exactly the files deleted by
  the real run — no more, no fewer, no substitutions.
- **R3** — WHEN `--limit N` caps a deletion, the selected files SHALL be the N matching files with
  the lexicographically smallest `path` values (ascending path order); `path` is unique per index,
  so this fully determines the selection with no ties.
- **R4** — WHEN `--limit` is not supplied, `xdu-rm` SHALL continue to act on all matching files as
  before (no behavioral change to the unlimited path).

## Non-goals (no-gos)

- **Prioritizing which files to delete by size or age.** `--limit` selects by path order only; it is
  a deterministic batch cap, not a "delete the biggest/oldest N" feature.
- **A user-configurable sort/order flag for `--limit` selection.** Out of scope; the order is fixed.
- **Changing `xdu-find`'s `--limit`/`--top` ordering.** `xdu-find` is a non-destructive query tool;
  its determinism (if any) is a separate concern, not part of this fix.
- **Fixing `--safe`'s incomplete re-stat coverage** (backlog item — min-size/newer-than/pattern not
  re-verified). Tracked separately; do not fold it in here.

## Clarifications

- **Q:** When `--limit N` caps a deletion to N files, which N should it select? — **A:** The N files
  with the lexicographically smallest paths (ascending `path` order). Purely deterministic; no size
  or age prioritization — the minimal fix (resolved 2026-07-28).

## Related materials

- Invariant §4 (`xdu-rm` destructive safety): "Any deletion combined with `--limit` MUST carry a
  deterministic `ORDER BY` so `--dry-run` and the real run select identical rows." —
  [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md)
- Source: `src/bin/xdu-rm.rs` (selection query), `src/lib.rs` (`QueryFilters` ORDER BY builders)
- Bug backlog: HIGH item #1 (arbitrary/unstable `--limit` selection)
