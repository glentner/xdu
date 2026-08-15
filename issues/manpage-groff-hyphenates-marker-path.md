---
status: unshaped
kind: fix
appetite: small
---

# `groff` hyphenates the completion-marker path, so the page operators actually read is wrong

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

`man xdu` on a Debian/Ubuntu box publishes the completion-marker path **broken across a line by a
Unicode hyphen**. `man-db` renders with `groff`, and `groff` — unlike `mandoc` — hyphenates. At the
default width, `doc/xdu.1.scd:108`'s `_OUTDIR_/.xdu-complete` comes out as:

```
       On  success  xdu writes a run-level completion marker, OUTDIR/.xdu-com‐
       plete, holding the version and this run's totals. The marker is removed
```

The character at the break is **U+2010 HYPHEN**, not the ASCII `-` already in the token. An operator
who copy-pastes that path gets `OUTDIR/.xdu-com‐plete` — a name that does not exist, containing a
byte sequence no shell completion will ever produce. The same paragraph is where the reader is being
told how to check whether an index is complete.

Measured on `ubuntu:24.04`, **groff 1.23.0**, `LC_ALL=C.UTF-8`, default width:

| Page | U+2010 under `groff` | U+2010 under `mandoc` |
|---|---|---|
| `xdu.1` | **10** | 0 |
| `xdu-find.1` | 1 | 0 |
| `xdu-view.1` | 0 | 0 |
| `xdu-rm.1` | 1 | 0 |

and for the marker literal specifically, after stripping whitespace the way the CI gate does:

| Renderer | `OUTDIR/.xdu-complete` |
|---|---|
| `groff -mandoc -Tutf8` | **ABSENT** |
| `man(1)` (man-db, `MANWIDTH=80`) | **ABSENT** |
| `mandoc -Tutf8` | FOUND |

Crucially this is **independent of the `scdoc` version**: it reproduces identically on roff from
1.11.2 and from 1.11.5. `scdoc` 1.11.5's `\-` substitution stops the token *breaking*, but `groff`
hyphenates at a different opportunity and inserts its own U+2010 regardless. So the fix that closed
the CI failure does not close this, and neither would pinning a newer `scdoc`.

**Reproduce:**

```sh
docker run --rm -v "$PWD/doc:/doc:ro" ubuntu:24.04 sh -c '
  apt-get update -qq && apt-get install -y -qq scdoc groff >/dev/null
  export LC_ALL=C.UTF-8
  scdoc < /doc/xdu.1.scd | groff -mandoc -Tutf8 | col -b | grep -A1 "xdu-com"'
```

*Measurement note:* observing this needs a UTF-8 locale. Under the C locale `col -b` rewrites
multibyte characters as the literal ASCII text `\xe2\x80\x90`, so a naive count reports zero U+2010
while the page is still broken — see [`manpage-gate-coverage-gaps.md`](manpage-gate-coverage-gaps.md).

## Why it was deferred

**Pre-existing**, and out of scope for the pass that measured it. `spec/manpage-literal-assertion-fails-on-ubuntu/GOAL.md`
lists as an explicit non-goal "settling the seed's unestablished open questions: … whether
`groff`/`man-db` renders a bare `-` as U+2010 **(one adversarial run measured zero)**".

**That parenthetical premise is false.** The measurement above says 10 per page on `xdu.1`, on both
roff variants. The non-goal boundary itself still holds — CI runs `mandoc`, which never hyphenates,
so this changes no verdict in that pass and `main` being green is not in question — but the reason
recorded for setting the boundary was wrong, and a future shaping session must not inherit it. Stated
plainly here because "`main` is green" must not be read as "the page reads correctly for users".

## Outcome / vision

The path an operator reads out of `man xdu` is the path that exists on disk, under the renderer their
distribution actually ships. Copy-pasting any literal from any of the four man pages produces a
working string.

## Sketch of the acceptance criteria

- **R1** — WHEN `man xdu` is rendered by `man-db`/`groff` at any terminal width, the completion-marker
  path SHALL appear as a copy-pastable literal, with no U+2010 or U+00AD inserted into it.
- **R2** — The same SHALL hold for every literal the CI gate asserts, across all four pages.
- **R3** — WHEN the check is run locally, it SHALL reach the same verdict regardless of the caller's
  locale (the `col -b` multibyte behaviour above must not silently pass it).

## Notes

- Options to weigh at shaping, roughly increasing cost:
  1. **Extend the normalization** — strip `U+2010`/`U+00AD` alongside whitespace in the gate and in
     `AGENTS.md`'s documented check. Cheapest, but it makes the *gate* tolerant of a page that is
     still wrong for the human reader; it detects nothing new.
  2. **Assert a `groff` render in CI** — adds `groff` to the packaging job and asserts the literals
     survive both renderers. Catches the user-visible defect rather than hiding it; costs one apt
     package and a second render.
  3. **Disable hyphenation in the source** — `.nh` is a roff control request and `scdoc` has no
     passthrough for it, so this likely means rewording so the token cannot land at a break, which
     `GOAL.md` already rejected as width-dependent and non-deterministic for the mandoc case.
  Option 2 is the only one that would have *caught* this; 1 is the only one that is free.
- Note the asymmetry: `mandoc` never hyphenates, so the renderer CI uses is strictly weaker at
  finding this class than the one users have. A gate is only as good as the renderer it runs.
- Related: [`manpage-gate-coverage-gaps.md`](manpage-gate-coverage-gaps.md) (the other deferrals from
  the same pass), `spec/manpage-literal-assertion-fails-on-ubuntu/PLAN.md` §5 risk 3,
  `spec/manpage-literal-assertion-fails-on-ubuntu/EVIDENCE.md`.
- Found by: `manpage-literal-assertion-fails-on-ubuntu` — raised at `/xdu-plan`, re-measured and
  confirmed at P3.
