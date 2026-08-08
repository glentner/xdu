---
status: unshaped
kind: fix
appetite: small
---

# The man-page literal gate false-alarms on every distro `scdoc`, and CI is red on `main`

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

The Tests workflow's `packaging` job fails on `ubuntu-24.04`:

```
CORRUPT RENDER: share/man/man1/xdu.1 is missing the literal: OUTDIR/.xdu-complete
##[error]Process completed with exit code 1.
```

**The man page is not corrupt.** No character is lost; `mandoc` simply fills the paragraph and breaks
the line at the hyphen inside `xdu-complete`. The defect is in the assertion, at
`.github/workflows/test.yaml:142` and `:148`:

```sh
published() { mandoc -Tutf8 "$1" | col -b | tr '\n' ' ' | tr -s ' '; }
...
if ! printf '%s' "$text" | grep -qF -- "$lit"; then
```

The flatten collapses a newline to a *space*. That defends against a rewrap **between** words; it does
nothing for a break **inside** a token, because the published text becomes `OUTDIR/.xdu- complete,`
and `grep -qF 'OUTDIR/.xdu-complete'` misses.

**The variable is `scdoc`, not `mandoc`.** Upstream commit `1d4143d` ("Substitute `-` with `\-`")
landed in **scdoc 1.11.5** and is absent from 1.11.2/1.11.3/1.11.4. A bare roff `-` is a legal
line-break opportunity; `\-` is a minus glyph and is not. ubuntu-24.04 noble ships **scdoc 1.11.2**
(`.github/workflows/test.yaml:122` installs it from apt); the maintainer's macOS host has homebrew
**scdoc 1.11.5** (`AGENTS.md:106` says `brew install scdoc`). Same `doc/xdu.1.scd:108`, two different
roffs:

```
1.11.5:  ...\fIOUTDIR\fR/.\&xdu\-complete,     <- unbreakable
1.11.2:  ...\fIOUTDIR\fR/.\&xdu-complete,      <- breakable
```

Reproduced in `ubuntu:24.04` amd64 with the exact apt packages CI installs (`scdoc 1.11.2-1`,
`mandoc 1.14.6-1`, `bsdextrautils 2.39.3-9ubuntu6.5`), running the workflow's own pipeline. Exactly one
literal of the ten fails:

```
ok   :: OUTDIR/<partition>/<chunk>.parquet
FAIL :: OUTDIR/.xdu-complete
ok   :: __root__   .partial   st_blocks * 512   OUTDIR/*/*.parquet
--- rendered ---
105:       On success xdu writes a run-level completion marker, OUTDIR/.xdu-
106:       complete, holding the version and this run's totals. The marker is
```

Causation is isolated to **one character**. Patching only that hyphen in the real 1.11.2 roff — a
one-line diff, nothing else touched — makes the assertion pass:

```
170c170
< On success \fBxdu\fR writes a run-level completion marker, \fIOUTDIR\fR/.\&xdu-complete,
> On success \fBxdu\fR writes a run-level completion marker, \fIOUTDIR\fR/.\&xdu\-complete,
$ ... | grep -oF 'OUTDIR/.xdu-complete'  ->  OUTDIR/.xdu-complete
```

`mandoc` is exonerated in both directions: the *same* container mandoc 1.14.6 renders the 1.11.5 roff
with the literal intact, and macOS `/usr/bin/mandoc` fed the 1.11.2 roff fails identically. The two
mandoc builds agree; the two scdocs do not.

**There is a second, independent brittleness class already latent in the same line.** `col -b` indents
option blocks with a literal **TAB**, and `tr -s ' '` squeezes spaces only. A multi-word literal that
wraps mid-literal therefore also false-alarms: `st_blocks * 512` flattens to `st_blocks *<TAB>512`.
Measured failing at widths **46 and 60, under both scdoc versions**. It is invisible today only
because CI's width happens to be 78.

Width sweep of the current gate against the 1.11.2 roff — the failures are wherever a literal lands on
the fill boundary, not a bounded window:

```
w=40  FAIL [OUTDIR/.xdu-complete]
w=46  FAIL [OUTDIR/.xdu-complete] [st_blocks * 512]
w=50 55 ok · w=60 FAIL [st_blocks * 512] · w=65 70 ok
w=72 74 76 78 80  FAIL [OUTDIR/.xdu-complete]
w=90 100 120 200  ok
```

The failure is **deterministic, not flaky**: mandoc ignores `MANWIDTH` and `COLUMNS` (measured
identical output with each set to 120) and honours only `-O width=`, so CI renders at the default 78 —
dead centre of a failing band. Equally, the gate has been **passing by wrapping luck**: padding the
marker paragraph in 3-character steps from 0 to 150 flips the current gate green and red again at
several pad values. The exact thresholds are environment-specific — pad=3 green / 54–60 red / 114–120
red measured in `ubuntu:24.04` on the 1.11.2 roff; a re-run on the macOS host against the 1.11.5 roff
put the first flip at pad=54. Treat the *class* as the finding and the thresholds as illustrative.

## Why it was deferred

**Pre-existing, and not caused by PR #10.** The marker sentence landed in `c4618c6` (PR #9). The
assertion step landed in `9c579cf`, a `[harness]` commit pushed **directly to `main`** with no PR
(`gh api /repos/glentner/xdu/commits/9c579cf/pulls` → empty), applying crawl-hardening finding F9 after
that PR merged. The prior `main` run's packaging job (`92967235978`) shows the identical
`CORRUPT RENDER` line, before PR #10 existed. PR #10 neither introduced nor touched it; it merely
inherited a red `main`.

The gate's author already anticipated this class and got it half right —
`.agents/factory/harness-log.md:153-154` records that the flatten exists because "mandoc rewraps
freely, so a wrap-sensitive grep would pass or fail on formatting rather than content." The flatten
fixes breaks *between* words but not *inside* a token, and it was only ever validated on scdoc 1.11.5,
where `\-` made the token unbreakable and hid the gap. The local-vs-CI toolchain skew that
`AGENTS.md:106` bakes in (`brew install scdoc` = 1.11.5, CI apt = 1.11.2) is what let it reach CI
unseen.

**And it was never executed by CI before it landed.** The nine `[harness]` commits including `9c579cf`
were pushed to `main` as one batch, and GitHub creates a run only for the pushed tip, so no workflow
ever ran against `9c579cf` itself (`gh api "/repos/glentner/xdu/actions/runs?head_sha=$(git rev-parse
9c579cf)"` → `total_count: 0`). The gate's first CI execution was the later `e865e9f` run — which it
failed. It has never once been green in CI.

Deferred rather than hot-fixed because two candidate fixes were in direct conflict, and settling which
one actually holds required an adversarial reproduction (below) rather than a one-line patch under
time pressure. It is also CI-surface work with an AGENTS.md documentation consequence, which belongs
in a shaped pass, not a drive-by.

## Outcome / vision

The gate asserts **content**, not layout. It stays red for the class it was built for — silent scdoc
markup corruption, where `scdoc` exits 0 and publishes a wrong literal to an operator — and green
regardless of which `scdoc` built the roff, what width `mandoc` filled to, or where a future
paragraph edit happens to push a line break. `main` is green again, and a local render matches what CI
will conclude.

## Sketch of the acceptance criteria

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

## Notes

- **Cheapest shape — normalize whitespace away on both sides.** Two lines in
  `.github/workflows/test.yaml`:

  ```sh
  published() { mandoc -Tutf8 "$1" | col -b | tr -d '[:space:]'; }          # :142
  if ! printf '%s' "$text" | grep -qF -- "$(printf '%s' "$lit" | tr -d '[:space:]')"; then   # :148
  ```

  Independently validated in `ubuntu:24.04`: clean at every width 40–200 across all four pages and all
  ten literals, on both the 1.11.2 and 1.11.5 roff, and still **red** on the historical unescaped-glob
  corruption. Its one honest cost: intra-literal whitespace is no longer asserted, so `st_blocks * 512`
  would also match a published `st_blocks*512`. That distinction is cosmetic and was never the class
  the gate exists for. It closes both brittleness classes at once.
- **Rejected — `mandoc -Tutf8 -O width=200 | col -bx`.** This was one investigator's recommendation and
  it is measurably not a fix. Its premise ("at width 200 no line in any of the four pages wraps") is
  false: lines do exceed 200 columns (measured max 222 for `xdu.1` across 74 filled lines in
  `ubuntu:24.04`/mandoc 1.14.6; 204 with 2 lines over 200 on the macOS host — the numbers are
  environment-specific, the falsification is not). Padding the marker paragraph by roughly 100
  characters — a routine doc edit — re-breaks `OUTDIR/.xdu-complete` **at width 200** (96–102 measured
  in the container, 120 on the macOS host), while the whitespace-stripping variant passes at every pad
  from 0 to 150 in both. Widening moves the break; it never removes it, and it converts a reproducible
  red into a latent one.
- **Rejected — de-hyphenate before flattening** (`sed -e ':a' -e '$!N' -e 's/-\n[[:space:]]*/-/' …`).
  Works, and is strictly more precise (it preserves intra-literal space assertions). Rejected on
  legibility: this step's whole value is being a gate a human can read, and it also fuses genuinely
  hyphenated phrases broken at a line end. Worth reconsidering only if R4-plus-precision is wanted.
- **Rejected — pin `scdoc >= 1.11.5` in CI** (build from the sr.ht tarball). It makes CI *less*
  representative: distro packagers build these pages with whatever `scdoc` they ship, so pinning hides
  the rendering most users get. It also leaves the tab class open and adds a network fetch to a gate
  that has none. Reasonable as a *supplement*, never as the fix.
- **Rejected — reword `doc/xdu.1.scd:108`.** Width-dependent and non-deterministic; the next paragraph
  edit above it reflows everything below. Fixes one instance of a general defect.
- **Two smaller findings in the same step, both reproduced, both cheap to fold in.**
  `published()` ends in `tr`, so its pipeline status is always 0 and `set -eu` cannot see `mandoc`
  fail — a missing page yields six misleading `CORRUPT RENDER` lines instead of "render failed" (R5).
  And `.partial` occurs **twice** in `doc/xdu.1.scd` (`:105` and `:135`), so a presence assertion on it
  only detects corruption of *every* occurrence; the source-side `^[[:space:]]*['.]` tripwire at
  `:166` is what actually covers that class reliably.
- **Fix in `test.yaml` only.** `.github/workflows/release.yaml:57` installs apt `scdoc` and renders
  `doc/*.scd` with **no** literal assertion (verified: no `CORRUPT RENDER`, `mandoc` or `col` anywhere
  in that file). Released tarballs built on ubuntu runners therefore already ship bare-hyphen pages —
  cosmetic, since no character is lost. Keeping the assertion in one place preserves the deliberate
  single-location property recorded at `.agents/factory/harness-log.md:156-157`.
- **Not this issue: the Docker workflow.** Jobs `92997599332`/`92997599519` fail with
  `ToolNotFound: failed to find tool "c++"` building `libduckdb-sys v1.4.4` — a missing C++ compiler in
  the image. Unrelated cause, separate fix — tracked in
  [`issues/dockerfile-builder-missing-cxx-toolchain.md`](dockerfile-builder-missing-cxx-toolchain.md).
- **All four `doc/*.scd` are clean.** A mechanical source-intent-vs-published-text token audit over all
  four pages (markup stripped, escapes resolved, rendered wide enough that nothing wraps) reported zero
  discrepancies, the roff-control tripwire is clean, and both ambiguous `*/*` sites are correctly
  disambiguated — escaped as `\*/\*` at `doc/xdu.1.scd:113`, left as the legitimate bold-slash at
  `doc/xdu-view.1.scd:95`. There is no man-page source defect to fix here.
- **Open — unestablished, do not promote as fact.** (1) Which distros ship which `scdoc`: only
  `ubuntu-24.04 = 1.11.2` is measured, and 1.11.5 is the newest upstream tag; the broader claim that
  "essentially every environment except a homebrew box fails this" is inference — repology was not
  reachable. (2) Whether `man-db`/`groff` renders a bare `-` as U+2010 (the upstream commit body says
  so, which would break copy-paste for real Debian/Ubuntu readers); one adversarial run measured **zero**
  U+2010 from `groff -Tutf8 -mandoc` on both roffs, so this is at most an untested upstream remark, not
  a user-facing consequence. (3) Whether the source-side roff-control grep at `:166` has any analogous
  version sensitivity — it reads the `.scd` source rather than rendered output, so probably immune, but
  it was not stressed. (4) Whether `AGENTS.md` should name a minimum `scdoc` version or simply warn
  about the skew — a shaping question, not a measurement.
- Related: `.agents/factory/harness-log.md:153-154` (why the flatten exists),
  `spec/crawl-hardening/META.md` (the author's contemporaneous reasoning about rewrap), `c4618c6`
  (PR #9, the marker sentence) / `9c579cf` (the gate, pushed straight to `main`),
  [`issues/dockerfile-builder-missing-cxx-toolchain.md`](dockerfile-builder-missing-cxx-toolchain.md)
  (the other red gate from the same CI run).
- Found by: post-merge triage of the red `main` after PR #10, reproduced in `ubuntu:24.04`.
