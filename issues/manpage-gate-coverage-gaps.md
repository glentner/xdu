---
status: unshaped
kind: fix
appetite: small
---

# The man-page literal gate is correct but narrow: four measured gaps in what it can see

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

The `Assert critical literals survive into the published man-page text` step in
`.github/workflows/test.yaml` is now robust *at what it checks* — its verdict depends on content, not
layout, across both `scdoc` versions, 161 render widths and both userlands. These are gaps in its
**coverage model**: things it is structurally unable to see. All four were measured; none is a
regression, and no R-ID in the pass that found them reaches any of them.

**(a) No page identity.** No literal names the binary it belongs to, so the gate cannot tell the four
pages apart. Copying `xdu-find.1` over `xdu-view.1` and running the gate is **green** — both pages
assert only `XDU_INDEX`. A build that shipped the same page four times would pass.

**(b) The page list is hard-coded.** The gate names exactly four `check` targets, while the render
step loops over `doc/*.scd`. A fifth page is therefore *entirely* unasserted. Measured: a new
`doc/xdu-new.1.scd` containing the un-escaped `_OUTDIR_/*/*.parquet` renders at `scdoc` exit 0,
publishes the exact historical corruption `OUTDIR//.parquet`, and the gate is **green**. The
source-side roff-control tripwire does glob `doc/*.scd`, but it only catches lines beginning with `.`
or `'` — a different class. A new binary's man page joins the build with no literal coverage and
nothing says so.

**(c) Four of the ten assertions are inert.** The set is 6 literals on `xdu.1`, 1 on `xdu-find.1`, 1
on `xdu-view.1`, 2 on `xdu-rm.1`. The four env-var names (`XDU_INDEX` ×3, `XDU_JOBS`) are written
`*XDU_INDEX*` in the sources — bold with a **mid-word** underscore, which `AGENTS.md` documents as
safe by construction. They have no silent-corruption mode: they can only fail if the page is missing
or empty, which `RENDER FAILED`/`RENDER EMPTY` already report. So the gate's entire real detection
power sits on `xdu.1`, and **`xdu-rm.1` — the destructive binary — is covered by two inert names**.

**(d) `col -b` is locale-dependent, which silently bounds what can ever be asserted.** Under a
non-UTF-8 locale, `col -b` rewrites each multibyte character as the literal ASCII text `\xNN`:

```sh
$ printf 'a‐b\n' | LC_ALL=C     col -b | od -c | head -1
0000000   a   \   x   e   2   \   x   8   0   \   x   9   0   b  \n
$ printf 'a‐b\n' | LC_ALL=C.UTF-8 col -b | od -c | head -1
0000000   a 342 200 220   b  \n
```

Harmless **today** — every asserted literal is pure ASCII, so the gate's verdict is locale-invariant,
and P1's matrix confirms host and container agree. But the man pages do contain non-ASCII (em dashes
throughout `doc/*.scd`), so the moment anyone asserts a literal containing one, the gate's verdict
starts depending on the runner's locale — which is the same shape as the defect that produced this
whole pass. It also mis-measures adjacent work: it is why an initial count of U+2010 in the `groff`
render read zero while the page was demonstrably broken.

## Why it was deferred

All four are **pre-existing** — (a), (b) and (c) arrived with the gate in `9c579cf`; (d) is a property
of `col` that predates the repo. `spec/manpage-literal-assertion-fails-on-ubuntu/GOAL.md` scoped that
pass to making the existing assertion layout-insensitive (R1–R5, R7) and reconciling the documented
local check (R6). Widening *what* is asserted is a different question from making the existing
assertions correct, and folding it in would have meant negotiating new coverage under an appetite
sized for a two-line normalization fix.

## Outcome / vision

The gate's coverage grows with the project instead of silently lagging it: a new man page cannot join
the build unasserted, a page cannot be confused with another page, every binary the project ships has
at least one literal that would actually catch a mis-escape, and the verdict never depends on the
caller's locale.

## Sketch of the acceptance criteria

- **R1** — WHEN a `doc/*.scd` exists that the literal gate does not assert, the packaging job SHALL
  fail and name the unasserted page (closes (b) by construction rather than by remembering).
- **R2** — Each of the four pages SHALL assert at least one literal unique to that page, so swapping
  two rendered pages fails (closes (a)).
- **R3** — `xdu-rm.1` SHALL assert at least one literal with a real silent-corruption mode — a path,
  glob or escaped-`*` construct — rather than only env-var names (closes (c)).
- **R4** — The gate's verdict SHALL be identical under `LC_ALL=C` and a UTF-8 locale, or the job
  SHALL pin the locale it requires (closes (d)).

## Notes

- (b) is the one worth doing first and is nearly free: derive the page list from `doc/*.scd` and fail
  on any page with no `check` entry, rather than maintaining a parallel list. That converts a silent
  gap into a build error the next time a binary is added — and `ROADMAP.md` has `xdu-mv`/`xdu-tar`
  queued, so it will be exercised.
- (c) suggests the fix is choosing better literals, not more of them. One well-chosen path per page
  beats four env-var names.
- Reproductions for (a) and (b): render `doc/*.scd`, then either `cp share/man/man1/xdu-find.1
  share/man/man1/xdu-view.1`, or add a fifth `.scd` carrying `_OUTDIR_/*/*.parquet`; run the extracted
  gate body in either case and observe `OK:`.
- Related: [`manpage-groff-hyphenates-marker-path.md`](manpage-groff-hyphenates-marker-path.md) (the
  other deferral from the same pass — and the one (d) interferes with measuring);
  [`ci-gates-are-advisory.md`](ci-gates-are-advisory.md) (a gate that binds nothing is the wider
  version of this); `spec/manpage-literal-assertion-fails-on-ubuntu/EVIDENCE.md`.
- Found by: `manpage-literal-assertion-fails-on-ubuntu` P3.
