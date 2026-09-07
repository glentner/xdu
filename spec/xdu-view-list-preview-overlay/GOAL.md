# GOAL — In-list file preview overlay (`xdu-view`)

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).
> Keep this at the right altitude: solved and bounded, but not over-specified — leave design
> freedom for the plan. Edit requirements here; do **not** silently drift them during build.
>
> Promoted from [`issues/xdu-view-list-preview-overlay.md`](../../issues/xdu-view-list-preview-overlay.md),
> which remains the pre-shaped evidence record. Do not treat the seed's draft R-IDs as settled —
> this file is the contract.

- **slug:** xdu-view-list-preview-overlay
- **kind:** feature
- **appetite:** small

## Problem

The tree view can show what a file is — type detection and a plain-text preview in the
rightmost pane — without leaving the current directory. The list view cannot. Scanning a
large directory in list mode means switching to tree mode or leaving the list to inspect a
file, which breaks the scan. This is the last unfinished piece of the original tree-view
work.

List-mode Space currently shares the "enter directory" binding with Enter and →. Pre-v1,
that binding can move: Enter and the arrows already navigate, so Space is available as the
preview trigger.

## Outcome / vision

In list mode, Space pops an overlay with the selected file's type and a short text preview,
then dismisses back to the same list at the same selection and scroll. The list becomes as
inspectable as the tree without leaving it. Directories are not previewed. Tree-mode
preview and its keybinds stay as they are.

## Acceptance criteria (the contract)

- **R1** — WHEN the user presses Space in list mode with a file selected, `xdu-view` SHALL
  pop an overlay showing that file's type and a short text preview, without leaving the
  list or changing its selection.
- **R2** — WHEN the overlay is dismissed, the list SHALL restore the selection and scroll
  position it had when the overlay opened.
- **R3** — WHEN the user presses Space in list mode with a directory selected, or with no
  selection, `xdu-view` SHALL leave the list unchanged: no overlay, no navigation.
- **R4** — WHILE the overlay is open, Esc SHALL dismiss it and return to the list rather
  than quit the TUI.
- **R5** — Enter and → in list mode SHALL continue to enter the selected directory. Space
  SHALL no longer drill in.
- **R6** — `doc/xdu-view.1.scd` SHALL document list-mode Space as the preview trigger, and
  SHALL no longer list Space as an enter-directory key for list mode.

## Non-goals (no-gos)

- Tree-mode preview pane, Tab-focus, scroll, and Esc-to-unfocus. Unchanged, including
  Space as enter-directory in tree mode.
- A full pager, hex dump, syntax highlighting, or an editor. The overlay is type plus
  short text.
- Previewing directories (name, child count, total size). Space on a directory is a no-op
  (R3).
- Changing other list-mode navigation (j/k, g/G, Backspace, filters, sort).
- A new file-type detector. Reuse whatever tree mode already uses; plan decides how.

## Clarifications

- **Q:** Which key invokes list-mode preview, given Space is currently drill-in? — **A:**
  Space. Pre-v1; Enter and the arrow keys already navigate the list, so Space can move
  (resolved 2026-09-07).
- **Q:** What happens when the trigger is pressed on a directory or an empty list? —
  **A:** No-op: stay in the list, no overlay (resolved 2026-09-07).
- **Q:** Does rebinding Space apply in tree mode too? — **A:** No. This feature is
  list-mode only; tree mode already has a preview pane and keeps its current keybinds,
  including Space as enter-directory (resolved 2026-09-07).
- **Q:** How is the overlay dismissed? — **A:** Esc, matching the sort selector and
  tree-preview unfocus. Whether Space also toggles the overlay closed is left to plan
  (assumed at shaping 2026-09-07 from the existing modal pattern).

## Related materials

- Seed: [`issues/xdu-view-list-preview-overlay.md`](../../issues/xdu-view-list-preview-overlay.md)
- Roadmap: [`ROADMAP.md`](../../ROADMAP.md) (near-term entry "In-list file preview overlay")
- `doc/xdu-view.1.scd` KEYBINDINGS (`→`/`Enter`/`Space` enter directory; Tab tree-preview
  focus)
- `src/bin/xdu-view.rs` (list mode and the existing tree-mode preview pane)
