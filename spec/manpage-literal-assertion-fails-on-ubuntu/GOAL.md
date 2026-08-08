# GOAL — The man-page literal gate asserts content, not layout

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).

- **slug:** manpage-literal-assertion-fails-on-ubuntu
- **kind:** fix
- **appetite:** small

## Problem

The Tests workflow's `packaging` job fails on `ubuntu-24.04` with
`CORRUPT RENDER: share/man/man1/xdu.1 is missing the literal: OUTDIR/.xdu-complete`, and `main` has
been red because of it. **The man page is not corrupt** — no character is lost. `mandoc` fills the
paragraph and breaks the line inside `xdu-complete`, and the gate's flatten
(`.github/workflows/test.yaml:142`) collapses a newline to a *space*, which survives a rewrap
*between* words but not a break *inside* a token: the published text becomes `OUTDIR/.xdu- complete,`
and the `grep -qF` at `:148` misses.

The variable is `scdoc`, not `mandoc`. Upstream commit `1d4143d` ("Substitute `-` with `\-`") landed
in **scdoc 1.11.5**; a bare roff `-` is a legal break opportunity, `\-` is not. ubuntu-24.04 ships
**1.11.2** (what CI installs at `test.yaml:122`); the maintainer's homebrew box has **1.11.5** (what
`AGENTS.md:106` tells you to install). Same `doc/xdu.1.scd:108`, two different roffs — so the gate
passes for its author and fails for everyone else, and the local check `AGENTS.md:108` documents
cannot predict CI's verdict. Reproduced in `ubuntu:24.04` with CI's exact apt packages; causation
isolated to that one character (patching only that hyphen in the 1.11.2 roff makes the assertion
pass), and `mandoc` exonerated in both directions.

The gate is worth keeping. It exists because a mis-escaped literal renders at `scdoc` **exit 0** —
`doc/xdu.1.scd` once published `OUTDIR//.parquet` for `_OUTDIR_/*/*.parquet`, a wrong glob handed to
an operator past a green build. But a **second brittleness class is already latent in the same
line**: `col -b` indents with a literal TAB while `tr -s ' '` squeezes only spaces, so a multi-word
literal that wraps mid-literal also false-alarms (`st_blocks * 512` measured failing at widths 46 and
60, under *both* scdoc versions). It is invisible today only because CI's width happens to be 78. The
current gate has been passing by wrapping luck: padding the marker paragraph in 3-character steps
flips it green and red again at several pad values. The defect is **pre-existing** — it arrived with
the gate itself in `9c579cf`, a `[harness]` commit pushed straight to `main` with no PR and therefore
never executed by CI before it landed; it has never once been green.

## Outcome / vision

The gate asserts **content**, not layout. It stays red for the class it was built for — silent scdoc
markup corruption that publishes a wrong literal at exit 0 — and green regardless of which `scdoc`
built the roff, what width `mandoc` filled to, or where a future paragraph edit pushes a line break.
When it does fire, it distinguishes "this page did not render" from "this page rendered wrong". `main`
is green again, and the local check `AGENTS.md` documents reaches the same verdict CI will.

## Acceptance criteria (the contract)

- **R1** — WHEN the packaging job renders `doc/*.scd` with the `scdoc` that `ubuntu-24.04` provides,
  the literal assertion SHALL pass on an unmodified tree, exiting 0.
- **R2** — WHEN the same assertion runs against roff produced by a `scdoc` that escapes hyphen-minus
  (1.11.5 and later), it SHALL also pass, so a local check and CI reach the same verdict.
- **R3** — IF a `doc/*.scd` literal is silently corrupted by mis-escaped markup — the historical
  `_OUTDIR_/*/*.parquet` publishing as `OUTDIR//.parquet`, at `scdoc` exit 0 — THEN the assertion
  SHALL fail non-zero and name the missing literal.
- **R4** — The assertion's verdict SHALL NOT depend on the rendering width, on where `mandoc` places a
  line break, or on whether the indent is a space or a tab.
- **R5** — IF `mandoc` fails to render a page at all, THEN the step SHALL report that failure rather
  than reporting every literal on that page as corrupt.
- **R6** — `AGENTS.md`'s documented local man-page check SHALL apply the same normalization CI applies,
  and SHALL note that homebrew `scdoc` diverges from the distro package.
- **R7** — IF an asserted literal occurs more than once in its source page, THEN corruption of any
  *single* occurrence SHALL be detected — or that literal SHALL be dropped from the set, with the class
  it covered stated as delegated to the source-side roff-control tripwire (`test.yaml:166`). Today
  `.partial` is the only such literal (`doc/xdu.1.scd:105` and `:135`), and a presence check on it
  fires only if *every* occurrence is corrupted.

## Non-goals (no-gos)

- **Asserting whitespace *inside* a literal.** Accepted cost of a whitespace-insensitive comparison:
  a published `st_blocks*512` would satisfy the literal `st_blocks * 512`. Cosmetic, and never the
  class this gate exists for (see Clarifications Q1).
- **A standing multi-width render in CI.** R4 is demonstrated by a recorded one-off sweep, not by new
  permanent CI surface (Q4).
- **Rewording `doc/xdu.1.scd:108`** to dodge the break. Width-dependent and non-deterministic — the
  next paragraph edit above it reflows everything below; fixes one instance of a general defect.
- **Pinning or building `scdoc >= 1.11.5` in CI.** Makes CI *less* representative (distro packagers
  build these pages with whatever `scdoc` they ship), leaves the tab class open, and adds a network
  fetch to a gate that has none. Reasonable as a supplement, never as the fix.
- **Widening the render** (`-O width=200 | col -bx`). Measurably not a fix: lines already exceed 200
  columns, and a ~100-character paragraph edit re-breaks the literal at width 200. Converts a
  reproducible red into a latent one.
- **Mirroring the assertion into `.github/workflows/release.yaml`.** The gate stays single-location by
  design (`.agents/factory/harness-log.md:156-157`).
- **Any change to `src/`, to the man-page *content*, or to the shipped CLI.** All four `doc/*.scd` were
  audited clean; there is no man-page source defect here.
- **The Docker builder `c++` failure** from the same CI run — `issues/dockerfile-builder-missing-cxx-toolchain.md`.
- **Making any CI check binding** (branch protection / required checks) — `issues/ci-gates-are-advisory.md`,
  which must land *after* this, or it blocks its own remediation.
- **Settling the seed's unestablished open questions**: the distro-`scdoc` census beyond ubuntu-24.04,
  and whether `groff`/`man-db` renders a bare `-` as U+2010 (one adversarial run measured zero).

## Clarifications

- **Q1:** Must the gate keep asserting whitespace *inside* a literal (so `st_blocks*512` still fails)?
  — **A:** No. Content only; intra-literal spacing is not asserted. The gate exists for silent markup
  corruption, not typography, and this keeps the fix legible while closing the hyphen and tab classes
  together (resolved 2026-08-08).
- **Q2:** How far should the `AGENTS.md` change go? — **A:** The documented local check applies CI's
  normalization verbatim so local and CI agree, plus a note that homebrew `scdoc` escapes hyphen-minus
  and distro packages do not. **No minimum version is named** — only ubuntu-24.04 = 1.11.2 is measured,
  so a stated minimum would be partly inferred (resolved 2026-08-08).
- **Q3:** Which secondary findings in the same step belong in this pass? — **A:** Both. The `mandoc`
  exit-status defect (R5) and the duplicate-occurrence `.partial` literal (R7) are in scope; they touch
  the same few lines (resolved 2026-08-08).
- **Q4:** How is R4 demonstrated? — **A:** `/xdu-build` re-runs the seed's width sweep (40–200, both
  scdoc versions) and records the evidence in the spec. CI keeps its single default-width render
  (resolved 2026-08-08).

## Related materials

- Seed issue (full reproduction, measured width sweep, and the four rejected candidates):
  [`issues/manpage-literal-assertion-fails-on-ubuntu.md`](../../issues/manpage-literal-assertion-fails-on-ubuntu.md)
- The gate: `.github/workflows/test.yaml:139-172` — `published()` at `:142`, the `grep -qF` at `:148`,
  the apt install at `:122`, the source-side roff-control tripwire at `:166`.
- Why the flatten exists, and why the gate is single-location:
  `.agents/factory/harness-log.md:153-157`; `spec/crawl-hardening/META.md` (F9, the author's
  contemporaneous reasoning about rewrap).
- The local check to reconcile: `AGENTS.md:104-109` ("Commands" → man page gate).
- Sources involved: `doc/xdu.1.scd:108` (the marker sentence), `:105` and `:135` (`.partial` twice),
  `:42` (`st_blocks \* 512`), `:113` (the correctly-escaped `\*/\*`).
- History: `c4618c6` (PR #9, the marker sentence) · `9c579cf` (the gate, pushed direct to `main`,
  never CI-executed before landing) · run `92967235978` (identical failure on `main` before PR #10).
- Siblings from the same CI triage:
  [`issues/dockerfile-builder-missing-cxx-toolchain.md`](../../issues/dockerfile-builder-missing-cxx-toolchain.md),
  [`issues/ci-gates-are-advisory.md`](../../issues/ci-gates-are-advisory.md).
