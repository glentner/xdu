---
status: unshaped
kind: fix
appetite: small
---

# `xdu-view` leaves the terminal wedged on panic, and can panic on a multibyte name

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

Two invariant §12 gaps in the same file, filed as one change because they are the same fix in the same
2,500-line binary — and the second is only user-visible *because* of the first.

## Problem

### 1. Terminal restore is not panic-safe

`src/bin/xdu-view.rs` enters raw mode and the alternate screen, then restores them with plain
sequential code after `run_app` returns:

```
:1865  enable_raw_mode()?;
:1866  stdout().execute(EnterAlternateScreen)?;
:1867  let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
:1870  let result = run_app(&mut terminal, &mut app);
:1873  disable_raw_mode()?;
:1874  stdout().execute(LeaveAlternateScreen)?;
```

`grep -cE "set_hook|impl Drop|catch_unwind" src/bin/xdu-view.rs` returns **0** — there is no Drop
guard and no panic hook. And `Cargo.toml:49` sets `panic = "abort"`, so there is no unwind that could
run a destructor even if one existed. **Any** panic between `:1865` and `:1873` therefore leaves the
user's terminal in raw mode, on the alternate screen, with no echo — recoverable only by a blind
`reset`.

The `?` paths are reachable too, not just panics: `Terminal::new` at `:1867` is fallible and already
runs *after* raw mode and the alternate screen are entered, so an early return there wedges the
terminal without any panic at all.

Invariant §12 requires exactly what is missing: "Raw mode + alternate screen restore on **every** exit
path including panic (Drop guard / panic hook, not sequential code — `panic="abort"` is set)."

### 2. Display names are truncated on byte indices, not char boundaries

```
:2211  format!("{}…", &name[..name_width.saturating_sub(1)])   // render_list_content
:2356  format!("{}…", &name[..name_max.saturating_sub(1)])     // render_tree_content
```

Slicing a `&str` at an arbitrary byte offset **panics** if the index is not a char boundary. Any
filename containing a multibyte character — an accented letter, CJK, an emoji — that happens to land
across the cut will abort the TUI. The widths are computed as terminal *columns* and then used as
*byte* offsets, which is the same bytes-vs-columns confusion twice; even for pure ASCII the two only
coincide by accident.

§12's char-boundary clause covers this ("multibyte-safe truncation"), and the two gaps compound: the
panic in (2) is precisely the panic that (1) fails to clean up after. A user with a CJK filename gets a
wedged terminal.

## Why it was deferred

Both are **pre-existing and identical in `main`** — neither was introduced by `crawl-hardening`. That
branch's `xdu-view` diff is 16+/10− and touches only the `index_glob`/`ROOT_PARTITION` dedup and the
completion-marker warning; its hunks stop around `:1848`, nothing near the raw-mode region at
`:1865-1875` or the truncation sites at `:2211`/`:2356`. So they did not block that review, and fixing
them there would have been scope creep into a reader rewrite.

They also pair naturally with the already-recorded "Lift the pure TUI helpers into `lib`" deferral —
`strip_ansi` is itself load-bearing for terminal safety — so one §12 pass over `xdu-view` should do all
of it rather than three separate visits to the same large file.

## Outcome / vision

`xdu-view` cannot leave a terminal unusable, whatever happens inside it, and no filename can crash the
renderer.

## Sketch of the acceptance criteria

- **R1** — IF `xdu-view` panics at any point after entering raw mode, THEN the terminal SHALL be
  restored to its prior state before the process exits.
- **R2** — IF `xdu-view` returns early via `?` after entering raw mode, THEN the terminal SHALL be
  restored.
- **R3** — WHEN a filename containing multibyte characters is too long for its column, the renderer
  SHALL truncate it without panicking, and SHALL measure the fit in terminal columns rather than bytes.
- **R4** — The restore SHALL be expressed as a guard whose correctness does not depend on control flow
  reaching a particular line.

## Notes

- `panic = "abort"` means a `Drop` guard alone is **not** sufficient for the panic case — a
  `std::panic::set_hook` that restores the terminal before aborting is the part that actually covers
  R1. A guard covers the `?`/early-return case (R2). Both are needed; that is easy to get half-right.
- For R3, prefer measuring with a grapheme/width-aware helper over `char_indices`-based slicing;
  either is fine as long as the width is columns and the slice is a boundary.
- Testing the panic path needs care under `panic = "abort"` — an integration test that spawns the
  binary and inspects terminal state, rather than a unit test, is the realistic shape.
- Related: `ROADMAP.md`'s "Internal cleanups surfaced by the crawl-hardening pass" (the `strip_ansi`
  lift), and `spec/crawl-hardening/ASSESSMENT.md`.
- Found by: `crawl-hardening` review cycle 1 / P11 design pass.
