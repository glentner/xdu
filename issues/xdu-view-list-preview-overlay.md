---
status: shaped on feature/xdu-view-list-preview-overlay (2026-09-07) — see spec/xdu-view-list-preview-overlay/GOAL.md
kind: feature
appetite: small
---

# In-list file preview overlay (`xdu-view`)

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

The tree view previews a file inline, but the list view cannot — to see what a file actually *is*,
the user must switch modes or leave the list, which breaks the flow of scanning a large directory.
This is the last unfinished piece of the original tree-view work.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the
back-reference; no new scoping was done.

## Outcome / vision

An overlay pops file-type info and a short text preview over the current list, then dismisses back
to it, making the list view as inspectable as the tree view. The trigger key is open: the natural
`<space>` binding is already taken by list-mode drill-in.

## Sketch of the acceptance criteria

- **R1** — WHEN the user invokes the preview trigger in list mode, `xdu-view` SHALL pop an overlay
  with the selected file's type and a short text preview without leaving the list.
- **R2** — WHEN the overlay is dismissed, the list SHALL restore its prior selection and scroll
  position.

## Notes

- Found by: original roadmap; back-reference retrofitted 2026-09-07.
