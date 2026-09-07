# PLAN — In-list file preview overlay (`xdu-view`)

> **Status:** Draft for review · **Last updated:** 2026-09-07
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). Lean path: no `research/` briefs.

## 1. Summary

List-mode Space currently shares "enter directory" with Enter and →. On a *file* it already
does nothing (`enter_selected` returns on `!is_dir`); on a *directory* it drills in. Rebind
list-mode Space to open a centered overlay that reuses the tree view's `load_file_preview`
(type description + first text chunk, `strip_ansi` already applied), and leave Enter / → as
the drill-in keys. Dismiss with Esc (and Space as a toggle). The list's `ListState` is never
touched while the overlay is open, so selection and scroll come back for free. Tree-mode
keybinds are unchanged. Man page in the same commit.

## 2. Design

All of this lives in `src/bin/xdu-view.rs` and `doc/xdu-view.1.scd`. No clap, no schema, no
SQL, no other binary.

### Overlay state

A dedicated `list_preview: Option<FilePreview>` on `App`, not a reuse of `file_preview`. Tree
mode already owns that field; mixing the two would leak overlay state across `t` even though
the overlay swallows `t`. `InputMode` gains `ListPreview`, handled in `run_app` in the same
early-return slot as `SortSelect` — *before* the generic "not Normal → text input" branch,
which would otherwise eat keys into `input_buffer`.

Opening and closing are two methods. They do not save/restore `list_state`; they simply never
write it.

- `open_list_preview` — `list_preview_entry(&self.entries, self.list_state.selected())`
  returns `Some(file)` only for a non-directory row. On `Some`, set
  `list_preview = Some(Self::load_file_preview(&entry.path, entry.total_size,
  entry.latest_atime))` and `input_mode = ListPreview`. On `None` (directory, `..`, empty
  list, no selection), return without changing anything.
- `close_list_preview` — `list_preview = None`, `input_mode = Normal`.

`list_preview_entry` is a pure function next to `DirEntry` so `cargo test --bin xdu-view` can
reach it without constructing `App` or opening DuckDB.

### Keybinds (`run_app`)

List-mode `ViewMode::List` arm today:

```
KeyCode::Enter | KeyCode::Right | KeyCode::Char(' ') => app.enter_selected()
```

becomes:

```
KeyCode::Enter | KeyCode::Right => app.enter_selected()
KeyCode::Char(' ') => app.open_list_preview()
```

Tree-mode arm (`Right | Enter | Char(' ')` → drill-in or `preview_focused = true`) does not
change.

While `input_mode == ListPreview`:

- Esc closes (R4). Space also closes (GOAL left the toggle to plan; matches tree pager,
  where Space unfocuses).
- `q` closes rather than quitting, same as the tree pager. A second `q` after dismiss still
  quits.
- Every other key is swallowed, including `j`/`k`, `t`, Left/Backspace, and Enter. Navigation
  under the overlay would move the selection the overlay is supposed to restore.

### Render

`ui` already splits content / status. After `render_list_content`, if `list_preview` is
`Some`, draw a centered block over the list with `Clear` (already imported; the tree preview
pane uses it the same way) so the list remains visible around the popup.

Body is the same class of information as `render_file_preview_pane`: filename title, type
description, then either the loaded text lines or the existing "(binary file — no preview)" /
"(unreadable)" placeholders. Cap to the overlay's inner height — first chunk from
`load_file_preview` (64 KiB), no `preview_load_more_lines`. This is a short overlay, not a
pager: no j/k scroll. If a line is wider than the box, cut on a char boundary, never a raw
byte index.

Status bar while open: type description plus `Esc/Space: close`.

### Reuse, not a second detector

`detect_file_type`, `strip_ansi`, `FilePreview`, and `load_file_preview` stay where they are.
The overlay calls `load_file_preview`. Do not copy them, and do not lift them into `lib` in
this pass — that is a refactor of the existing tree-preview stack, out of appetite, and a
GOAL non-goal ("no new file-type detector").

### Man page (`doc/xdu-view.1.scd`)

The Navigation line `*→*/*Enter*/*Space*` currently claims Space enters a directory in both
modes. Split it:

- `*→*/*Enter*` — enter directory (both modes).
- `*Space*` (list mode) — preview overlay for the selected file; no-op on a directory; Esc
  dismisses; Enter and → still enter directories. The phrase **preview overlay** is the
  verify needle.
- `*Space*` (tree mode) — enter directory, or focus the preview pane when a file is selected.
  Tree already does this; documenting it is what keeps the split from silently dropping the
  binding.

Do not start a source line with `.` or `'`; escape literal `*`. Same-commit as the keybind
(invariant §10 / §13). Completions are unchanged (no clap change).

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1   | List-mode Space → `open_list_preview` → centered overlay of `load_file_preview` output. |
| R2   | Overlay never writes `list_state`; `close_list_preview` only clears `list_preview` and `input_mode`. |
| R3   | `list_preview_entry` returns `None` for directories, `..`, empty list, no selection; `open_list_preview` is then a no-op. |
| R4   | `InputMode::ListPreview` early-return: Esc calls `close_list_preview` instead of `return Ok(())`. |
| R5   | List-mode Enter / Right stay on `enter_selected`; Space is removed from that or-pattern. |
| R6   | `doc/xdu-view.1.scd` KEYBINDINGS split as above; verify asserts `preview overlay` present and `Enter/Space` gone. |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) (§1–§13)
before the targeted reads and again after this design.

Touched:

- **§10 CLI single source of truth** — no clap change. The keybind is documented only in
  `doc/xdu-view.1.scd`, updated in the same commit as the `run_app` split.
- **§11 Altitude / testability** — new decision (`list_preview_entry`) is a pure function
  with bin unit tests. Sniffing stays in the bin because it already lives there; this pass
  does not add a second copy. A lift of `detect_file_type` / `load_file_preview` into `lib`,
  and an `update(&mut App, Action)` rewrite of the event loop, are pre-existing altitude
  debt in this 2,500-line binary — out of appetite, not introduced here.
- **§12 TUI terminal safety** — overlay renders `FilePreview` lines that already passed
  `strip_ansi`. Line-width truncation cuts on a char boundary. The overlay is inside
  `run_app`; it does not add a restore path. The sequential raw-mode restore (no Drop, no
  panic hook, `panic = "abort"`) is a known gap, already
  [`issues/xdu-view-terminal-safety.md`](../../issues/xdu-view-terminal-safety.md). Do not
  fix it here.
- **§13 Project conventions** — scdoc authoring rules; no `R#`/`P#` in source; man page
  same-commit; `share/` still generated. Do not add a new CI literal for this page (coverage
  of *which* literals are counted is
  [`issues/manpage-gate-coverage-gaps.md`](../../issues/manpage-gate-coverage-gaps.md)); the
  phase `verify:` asserts the two needles locally.

Untouched: §1 schema, §2/§2b/§2c crawl, §3 partitions, §4 `xdu-rm`, §5 DuckDB injection
(preview reads the live path from `DirEntry`, not SQL), §6 Unix-only, §7 rayon, §8
symlinks (index holds regular files; `File::open` is what tree preview already does), §9
`SortMode`.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| —         | —         | — |

## 4. Rabbit holes (resolved)

Lean path — targeted reads of `src/bin/xdu-view.rs` and `doc/xdu-view.1.scd`, no `research/`
briefs.

- **What does list-mode Space do today?** On a file, nothing (`enter_selected` returns). On a
  directory, drill-in. Rebinding Space only changes the directory case; file-Space becomes
  the overlay rather than stealing a useful action.
- **New loader vs reuse?** `load_file_preview` already sniffs, classifies, `strip_ansi`s, and
  loads the first 64 KiB. Overlay calls it. A second detector would drift.
- **Overlay vs status-bar modal?** Sort select is a status-bar widget. GOAL requires a popup
  over the list. `Clear` + centered `Block` is already how the tree preview pane paints.
- **Pager or short preview?** GOAL non-goal is a full pager. First chunk, cap to overlay
  height, no `preview_load_more_lines`, no j/k. Tree keeps its Tab-focused pager.
- **How to verify a TUI?** Do not drive ratatui. Prove the gate with `cargo test --bin
  xdu-view` and the man page with `scdoc | mandoc` needles. Overlay paint is reviewed by
  reading the render function.

## 5. Risks & open questions

- Overlay paint is not mechanically asserted. Review reads `render_list_preview_overlay` (or
  whatever it is named) against R1.
- `q` dismissing rather than quitting is a plan choice, not an R-ID. Status bar must say
  `Esc/Space: close` so it is discoverable; `q` is extra.
- `load_file_preview` on a huge sparse file still reads 8 KiB + 64 KiB. Same cost as tree
  selection. Acceptable.
- `DirEntry.path` is the on-disk path tree preview already opens. A vanished file yields
  `(unreadable)` and the overlay still opens (file row, not a directory no-op).

## 6. Verification strategy

- **Unit:** `#[cfg(test)]` in `src/bin/xdu-view.rs` covering `list_preview_entry` — file →
  `Some`, directory → `None`, `..` → `None`, no selection → `None`, empty slice → `None`.
- **Man page:** `scdoc < doc/xdu-view.1.scd | mandoc -Tutf8 | col -b`, whitespace-stripped,
  must contain `preview overlay` and `Space (list mode)`, must not contain the combined
  `Enter/Space` enter-directory token.
- **Not in verify:** driving `xdu-view` under a fake backend. No such harness exists; do not
  invent one in this appetite.
