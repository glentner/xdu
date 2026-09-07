# REVIEW — In-list file preview overlay for xdu-view

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** 013adf9c63bd00899194e4c33a263c44be26e6bf  ·  **Base:** main  ·  **Date:** 2026-09-07
- **Verdict:** approved
- **Cycle:** 1 of ≤3 — mirrors `review.cycle` in `TECH.md` (escalate to human on non-convergence)

## Verification run

Commands actually executed and their outcomes (the spine of the review):

- `cargo test` (full suite, orchestrator) → green: 63 lib + 5 `xdu-view` + 23 / 1 / 16 integration, 0 failed.
- `cargo test --bin xdu-view` (blind reviewer + orchestrator) → 5 passed: file, directory, `..`, no-selection, empty.
- `cargo clippy --all-targets --all-features -- -D warnings` → clean (exit 0).
- `cargo fmt --all -- --check` → clean (exit 0).
- `scdoc < doc/xdu-view.1.scd | mandoc -Tutf8 | col -b` → exit 0; published KEYBINDINGS read in full. Normalized checks per `AGENTS.md` Commands (whitespace-stripped): contains `previewoverlay`, contains `Space(listmode)`, does not contain `Enter/Space`; `→/Enter` occurs exactly 1x (the single enter-directory line, Space absent from it).
- `grep -nE 'R#[0-9]|P#[0-9]' src/bin/xdu-view.rs doc/xdu-view.1.scd` → no match (no spec ids in source).
- `git status --porcelain` after reviewer hand-back → empty; no `target/` negative-control mutation was performed, so no restore was required.
- Contract-drift check `git log --oneline main..HEAD -- spec/xdu-view-list-preview-overlay/GOAL.md` → only the original shaping commit; no drift.
- CI rollup (`gh`) → **not observed** (this session has no `gh`); gate states above are local only.

## Requirement → evidence matrix

Bidirectional traceability. All six R-IDs verified by the blind reviewer; the orchestrator re-ran the gates and read the decisive code regions to confirm.

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 | `list_preview_entry` gate + `open_list_preview` reusing `load_file_preview`; `render_list_preview_overlay` painted over the list (`src/bin/xdu-view.rs`) | `cargo test --bin xdu-view` 5 passed; reviewer + orchestrator read of gate/open/render paths | ✅ |
| R2 | `open_list_preview` / `close_list_preview` write only `list_preview` + `input_mode`; zero `list_state` writes | Full-function read of both fns; selection/scroll survive by construction | ✅ |
| R3 | Gate returns `None` for `is_dir`, `None` selection, OOB → early return, no mode change, no navigation | Unit tests directory / `..` / no-selection / empty all ok; production `..` and partition entries are `is_dir: true` | ✅ |
| R4 | `ListPreview` arm maps Esc (also Space, `q`) to `close_list_preview` with `continue` before the quit branch | Read ordering `ListPreview` arm vs `q/Esc => return Ok(())`; Esc cannot reach quit while open | ✅ |
| R5 | List arm split: `Enter\|Right` → `enter_selected`, `Space` → `open_list_preview`; tree arm (`Right\|Enter\|Space`) untouched | Diff shows list split and tree arm as context; clippy/test green | ✅ |
| R6 | `*→*/*Enter*/*Space*` → `*→*/*Enter*` plus new `*Space* (list mode)` / `*Space* (tree mode)` stanzas (`doc/xdu-view.1.scd`) | Rendered page read; normalized presence/absence checks above | ✅ |

Unmapped changes (possible scope creep): none. `issues/xdu-view-list-preview-overlay.md` one-line `unshaped` → `shaped` flip is factory bookkeeping, not creep. `centered_rect` / `truncate_to_chars` / status-bar branch / tests all serve R1/R4; extra Space- and `q`-to-close sits inside R4 dismissal (GOAL clarification left Space-toggle to plan).

## Findings

No CONFIRMED findings. No PLAUSIBLE findings (candidates considered — Space on `..`, Esc-quit ordering, key leakage through the overlay, scroll loss, ANSI/char-boundary, terminal restore, stale overlay across `t` toggle — each dissolved under executed evidence or certain-by-inspection refutation; per the rubric, dropped silently rather than filed).

Invariant sweep: no §1–§12 touch (no schema, SQL, partition, marker, concurrency, symlink, or sort coupling). §11 altitude holds (pure gate fn, char-based truncate helper, unit-tested). §12 holds (overlay reuses `load_file_preview` whose lines are `strip_ansi`'d; all overlay truncation via char-based `truncate_to_chars`; no new exit path, no Drop/panic hook added). No §13 violation (CLI untouched; `.scd` ships in the same commit as the behavior).

## Human-gate triggers

Not triggered. No CONFIRMED finding touches the high-blast-radius core (`src/bin/xdu-rm.rs`, `src/bin/xdu.rs`, `src/crawl.rs`, `src/lib.rs`, `src/cli.rs`) or a destructive-rm / schema-stability / atomic-write / SQL-injection invariant. This diff touches `src/bin/xdu-view.rs` (not a listed core path) with no such invariant in play.

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

Not run (no `completeness` argument; single-phase feature, all R-IDs mapped above).
