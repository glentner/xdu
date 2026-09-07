---
slug: xdu-view-list-preview-overlay
title: "In-list file preview overlay for xdu-view"
kind: feature
appetite: small
status: in_progress
branch: feature/xdu-view-list-preview-overlay
base: main
current_phase: P1
last_updated: "2026-09-07"
phases:
  - id: P1
    name: "List-mode Space overlay + man page"
    status: pending
    satisfies: [R1, R2, R3, R4, R5, R6]
    depends_on: []
    parallel: false
    hammerable: false
    hill: uphill
    verify: "cargo test --bin xdu-view && page=$(scdoc < doc/xdu-view.1.scd | mandoc -Tutf8 | col -b | tr -d '[:space:]') && printf %s \"$page\" | grep -qF previewoverlay && printf %s \"$page\" | grep -qF 'Space(listmode)' && ! printf %s \"$page\" | grep -qF Enter/Space"
review:
  last_reviewed_commit: ""
  verdict: none
  blocked_reason: ""
  cycle: 0
---

# TECH.md — In-list file preview overlay for xdu-view

The **context engine and finite-state machine** for building this feature. The YAML
frontmatter above is the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/xdu-view-list-preview-overlay/TECH.md`);
the per-phase checklist below is the work. `xdu-build` executes the next actionable phase,
runs its `verify:` command, updates state via
`uv run --with pyyaml python .agents/factory/bin/set_phase.py …`, and makes one atomic
code+state commit.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R-IDs are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).

## Conventions (apply to every phase)

- Commit conventions, code style, and load-bearing invariants come from
  [`AGENTS.md`](../../AGENTS.md) — it is the constitution. Consult
  [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) for the curated
  footgun checklist. This change touches §10 (man page same-commit), §11 (pure gate
  function, no second detector), §12 (`strip_ansi`, char-boundary truncation, do not add a
  restore path), §13 (scdoc authoring, no spec ids in source).
- One phase per `xdu-build` invocation by default; one atomic commit containing **both** the
  code and the `TECH.md` state change. Subject:
  `[feature] Build xdu-view-list-preview-overlay P1: …`.
- **No `Co-Authored-By` trailer.**
- The man-page (`doc/xdu-view.1.scd`) update lands **in the same commit** as the keybind
  (invariant §10 / §13). Completions are unchanged (no clap change).

---

## Phase P1 — List-mode Space overlay + man page
**Satisfies:** R1, R2, R3, R4, R5, R6 · **Depends on:** —
**Goal:** In list mode, Space on a file pops a type + short-text overlay and Esc dismisses
back to the same selection and scroll; Space on a directory is a no-op; Enter and → still
drill in; the man page documents the split.

- [ ] Add `list_preview: Option<FilePreview>` to `App` (not a reuse of `file_preview`) and
      `InputMode::ListPreview`. Initialize to `None` / unused in `App::new`.
- [ ] Add a pure `list_preview_entry(entries, selected) -> Option<&DirEntry>` next to
      `DirEntry`: `Some` only for a non-directory row. No spec ids in the comment.
- [ ] `App::open_list_preview` calls that gate; on `Some`, `load_file_preview` into
      `list_preview` and set `input_mode = ListPreview`; on `None`, do nothing. Do not
      write `list_state`.
- [ ] `App::close_list_preview` clears `list_preview` and returns `input_mode` to `Normal`.
      Do not write `list_state`.
- [ ] `run_app`: handle `InputMode::ListPreview` in the same early-return slot as
      `SortSelect`, *before* the generic text-input branch. Esc and Space and `q` close;
      every other key is swallowed. Do not `return Ok(())` on Esc here.
- [ ] List-mode arm: drop `Char(' ')` from the `enter_selected` or-pattern; Space calls
      `open_list_preview`. Enter and Right stay on `enter_selected`. Tree-mode arm
      unchanged.
- [ ] After `render_list_content`, if `list_preview` is `Some`, paint a centered overlay
      with `Clear` + `Block`: filename, type description, then text lines capped to inner
      height *or* the existing binary / unreadable placeholders. No
      `preview_load_more_lines`. Truncate wide lines on a char boundary. Status bar:
      `Esc/Space: close`.
- [ ] `#[cfg(test)]` in `src/bin/xdu-view.rs`: `list_preview_entry` for a file, a
      directory, `..`, `selected = None`, and an empty slice.
- [ ] `doc/xdu-view.1.scd` KEYBINDINGS: `*→*/*Enter*` enter directory; `*Space*` (list
      mode) preview overlay, no-op on a directory; `*Space*` (tree mode) enter directory or
      focus the preview pane. Phrase **preview overlay** must survive render. Never start a
      source line with `.` or `'`. Escape literal `*`. Do not add a CI literal for this
      page.
- [ ] Do not add a Drop guard or panic hook for terminal restore — already tracked in
      [`issues/xdu-view-terminal-safety.md`](../../issues/xdu-view-terminal-safety.md). Do
      not lift `detect_file_type` / `load_file_preview` into `lib` (GOAL non-goal; no new
      `issues/` file).
- [ ] Deferral ledger (this is the last phase): the two "do not" items above already name
      their destinations (existing issue; GOAL non-goal). Confirm no other "do not fix" /
      "known limitation" / "follow-up" in this checklist lacks an `issues/` file and
      ROADMAP entry.
- **Verify:** `cargo test --bin xdu-view`, then the rendered man page (whitespace-stripped)
  contains `previewoverlay` and `Space(listmode)` and does not contain `Enter/Space`. See
  the `verify:` field.
- **Touches:** `src/bin/xdu-view.rs`, `doc/xdu-view.1.scd`.

---

## How `xdu-build` drives this

1. `next_phase.py` prints the next actionable phase (statuses are authoritative; the
   `current_phase` pointer is reconciled against them).
2. Pre-flight: clean tree, on `branch`, `base` reachable.
3. Execute every `[ ]` in the phase (consult `PLAN.md` for detail).
4. Run the phase's `verify:` command — never advance on a checkbox alone.
5. Amend this file freely if reality diverges (regenerate frontmatter with `set_phase.py`;
   note the amendment in the commit body). STOP and escalate only on a **`GOAL.md`
   contradiction**.
6. Mark the phase `done`, advance `current_phase`, `--touch`; one `[feature]` commit; stop
   and report.
