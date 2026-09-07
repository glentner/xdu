---
status: unshaped
kind: feature
appetite: big
---

# Web client (`xdu-web`)

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

`xdu-view` is terminal-only, which limits who can explore an index and from where: every consumer
needs a shell account on a machine holding the index.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the
back-reference; no new scoping was done.

## Outcome / vision

A Wasm-compiled progressive web app that browses an S3-backed index straight from the browser —
the web equivalent of `xdu-view`, with the same list and tree views and the same search and
filtering — making a centrally stored index explorable by anyone with a link.

## Sketch of the acceptance criteria

- **R1** — The web app SHALL browse an S3-backed index with list and tree views plus search,
  mirroring `xdu-view`, with no shell account required.

## Notes

- Depends on: S3 as an index target.
- Found by: original roadmap; back-reference retrofitted 2026-09-07.
