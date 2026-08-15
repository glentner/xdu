# EVIDENCE — the recorded cross-toolchain sweep for P1

> This is the artifact [`GOAL.md`](GOAL.md) Q4 asked for: R4 is demonstrated by a recorded one-off
> sweep rather than by new permanent CI surface. Produced by
> `sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/gate-matrix.sh` on **2026-08-14**.

## What was measured, and against what

The harness does not test a copy of the gate. It **extracts the live body** of the
`Assert critical literals survive into the published man-page text` step from
`.github/workflows/test.yaml` by `awk` and runs that, so the harness cannot drift green while CI is
red — the failure mode that produced this defect.

Both roff variants come from a **real `scdoc` of the version they are labelled with**; neither is
simulated from the other with `sed`. The clean `xdu.1` roff differs by **196 changed lines** between
them (`.PP` vs `.P`, and structurally different bullet lists), which is why simulation was rejected
in [`PLAN.md`](PLAN.md) §4. The container renders into its own output directory only and mounts the
fixture tree read-only for the measurement phase, so it cannot regenerate the host fixtures with its
own `scdoc`.

| | |
|---|---|
| Host | Darwin 25.5.0 (BSD userland) · `scdoc` **1.11.5** (homebrew, escapes hyphen-minus) |
| Container | `ubuntu:24.04` (GNU userland) · `scdoc` **1.11.2** (apt — what CI installs at `test.yaml:122`) |
| Gate body | 55 lines extracted |
| Fixtures | 56 variants × 4 pages × 2 real `scdoc`s |
| Sweeps | widths 40–200 · pads 0–150 step 3, at `mandoc`'s own default width |
| Total | 884 gate invocations · 47 s wall clock |

Every case runs on **both** platforms against **both** roff variants, so a verdict that depends on
BSD vs GNU `grep`/`wc`/`col` shows up as a disagreement between the two streams instead of as a green
run on the author's laptop.

## Result: green

```
=== gate matrix: manpage-literal-assertion-fails-on-ubuntu P1 ===
gate      : .github/workflows/test.yaml 'Assert critical literals…' — 55 lines extracted
host      : host-darwin · Darwin 25.5.0 · scdoc-1.11.5
container : ubuntu-24.04 · scdoc-1.11.2
fixtures  : 56 variants × 4 pages × 2 real scdocs (clean xdu.1 roff differs by 196 lines between them)
sweeps    : widths 40..200 · pads 0..150 step 3 at the default width

R1     PASS  2/2 cases green — clean tree passes with the scdoc ubuntu-24.04 ships
         PASS host-darwin   scdoc-1.11.2   control                        exit 0 with the OK line
         PASS ubuntu-24.04  scdoc-1.11.2   control                        exit 0 with the OK line
R2     PASS  2/2 cases green — clean tree passes with a hyphen-escaping scdoc (local == CI)
         PASS host-darwin   scdoc-1.11.5   control                        exit 0 with the OK line
         PASS ubuntu-24.04  scdoc-1.11.5   control                        exit 0 with the OK line
R3     PASS  4/4 cases green — mis-escaped markup published at scdoc exit 0 is caught, by name
         PASS host-darwin   scdoc-1.11.5   mut-unescaped-glob             exit 1 naming OUTDIR/*/*.parquet
         PASS host-darwin   scdoc-1.11.2   mut-unescaped-glob             exit 1 naming OUTDIR/*/*.parquet
         PASS ubuntu-24.04  scdoc-1.11.5   mut-unescaped-glob             exit 1 naming OUTDIR/*/*.parquet
         PASS ubuntu-24.04  scdoc-1.11.2   mut-unescaped-glob             exit 1 naming OUTDIR/*/*.parquet
R4     PASS  8/8 cases green — verdict independent of render width, break position and tab indent
         PASS host-darwin   scdoc-1.11.5   width-sweep-40..200            161/161 widths green
         PASS host-darwin   scdoc-1.11.5   pad-sweep-0..150               51/51 pad values green at the default width
         PASS host-darwin   scdoc-1.11.2   width-sweep-40..200            161/161 widths green
         PASS host-darwin   scdoc-1.11.2   pad-sweep-0..150               51/51 pad values green at the default width
         PASS ubuntu-24.04  scdoc-1.11.5   width-sweep-40..200            161/161 widths green
         PASS ubuntu-24.04  scdoc-1.11.5   pad-sweep-0..150               51/51 pad values green at the default width
         PASS ubuntu-24.04  scdoc-1.11.2   width-sweep-40..200            161/161 widths green
         PASS ubuntu-24.04  scdoc-1.11.2   pad-sweep-0..150               51/51 pad values green at the default width
R5     PASS  8/8 cases green — a page that did not render is reported as that, not as corrupt literals
         PASS host-darwin   scdoc-1.11.5   mut-missing-page               RENDER FAILED, 0 misleading CORRUPT RENDER lines
         PASS host-darwin   scdoc-1.11.5   mut-empty-page                 RENDER EMPTY, 0 misleading CORRUPT RENDER lines
         PASS host-darwin   scdoc-1.11.2   mut-missing-page               RENDER FAILED, 0 misleading CORRUPT RENDER lines
         PASS host-darwin   scdoc-1.11.2   mut-empty-page                 RENDER EMPTY, 0 misleading CORRUPT RENDER lines
         PASS ubuntu-24.04  scdoc-1.11.5   mut-missing-page               RENDER FAILED, 0 misleading CORRUPT RENDER lines
         PASS ubuntu-24.04  scdoc-1.11.5   mut-empty-page                 RENDER EMPTY, 0 misleading CORRUPT RENDER lines
         PASS ubuntu-24.04  scdoc-1.11.2   mut-missing-page               RENDER FAILED, 0 misleading CORRUPT RENDER lines
         PASS ubuntu-24.04  scdoc-1.11.2   mut-empty-page                 RENDER EMPTY, 0 misleading CORRUPT RENDER lines
R7     PASS  8/8 cases green — corruption of ONE of two occurrences is caught; fusion does not false-red
         PASS host-darwin   scdoc-1.11.5   mut-one-partial                exit 1 reporting 1 occurrence, expected 2
         PASS host-darwin   scdoc-1.11.5   fusion-regression              exit 0 — the fused token did not inflate the count
         PASS host-darwin   scdoc-1.11.2   mut-one-partial                exit 1 reporting 1 occurrence, expected 2
         PASS host-darwin   scdoc-1.11.2   fusion-regression              exit 0 — the fused token did not inflate the count
         PASS ubuntu-24.04  scdoc-1.11.5   mut-one-partial                exit 1 reporting 1 occurrence, expected 2
         PASS ubuntu-24.04  scdoc-1.11.5   fusion-regression              exit 0 — the fused token did not inflate the count
         PASS ubuntu-24.04  scdoc-1.11.2   mut-one-partial                exit 1 reporting 1 occurrence, expected 2
         PASS ubuntu-24.04  scdoc-1.11.2   fusion-regression              exit 0 — the fused token did not inflate the count
SHELL  PASS  8/8 cases green — same diagnostics under bash -e and bash -eo pipefail
         PASS host-darwin   scdoc-1.11.5   two-corruptions-bash-plain     exit 1 with 2 diagnostics
         PASS host-darwin   scdoc-1.11.5   two-corruptions-bash-pipefail  exit 1 with 2 diagnostics
         PASS host-darwin   scdoc-1.11.2   two-corruptions-bash-plain     exit 1 with 2 diagnostics
         PASS host-darwin   scdoc-1.11.2   two-corruptions-bash-pipefail  exit 1 with 2 diagnostics
         PASS ubuntu-24.04  scdoc-1.11.5   two-corruptions-bash-plain     exit 1 with 2 diagnostics
         PASS ubuntu-24.04  scdoc-1.11.5   two-corruptions-bash-pipefail  exit 1 with 2 diagnostics
         PASS ubuntu-24.04  scdoc-1.11.2   two-corruptions-bash-plain     exit 1 with 2 diagnostics
         PASS ubuntu-24.04  scdoc-1.11.2   two-corruptions-bash-pipefail  exit 1 with 2 diagnostics

note: host-darwin: fusion fixture (scdoc-1.11.5): needle '.partial' matches 3 (false red), '.partial suffix' matches 2 (correct)
note: host-darwin: fusion fixture (scdoc-1.11.2): needle '.partial' matches 3 (false red), '.partial suffix' matches 2 (correct)
note: ubuntu-24.04: fusion fixture (scdoc-1.11.5): needle '.partial' matches 3 (false red), '.partial suffix' matches 2 (correct)
note: ubuntu-24.04: fusion fixture (scdoc-1.11.2): needle '.partial' matches 3 (false red), '.partial suffix' matches 2 (correct)

GATE-MATRIX-OK: every R-ID green on both platforms and both roff variants
```

## The same harness against the pre-fix gate

A green gate proves nothing until it has been seen to fail. The `Assert critical literals` body was
temporarily reverted to its committed pre-fix form (`git show HEAD:.github/workflows/test.yaml`) and
the harness re-run unchanged. It went red, and **reproduced the reported CI failure verbatim**:

```
RESULT|R1|ubuntu-24.04|scdoc-1.11.2|control|FAIL|exit 1: CORRUPT RENDER: share/man/man1/xdu.1 is missing the literal: OUTDIR/.xdu-complete
```

Pre-fix vs post-fix, same harness, same fixtures:

| Dimension | roff | pre-fix | post-fix |
|---|---|---|---|
| Clean tree (R1) | 1.11.2 | **red** on both platforms | green |
| Clean tree (R2) | 1.11.5 | green on both platforms | green |
| Width sweep (R4) | 1.11.2 | **28/161 red** (first: width 40, `OUTDIR/.xdu-complete`) | 0/161 red |
| Width sweep (R4) | 1.11.5 | **16/161 red** (first: width 43, `st_blocks * 512`) | 0/161 red |
| Pad sweep (R4) | 1.11.2 | **9/51 red** (first: pad 0 — the clean tree) | 0/51 red |
| Pad sweep (R4) | 1.11.5 | 0/51 red | 0/51 red |
| Missing page (R5) | both | `mandoc: BADARG` + **6 misleading `CORRUPT RENDER` lines** | `RENDER FAILED`, 0 |
| Empty page (R5) | both | **6 misleading `CORRUPT RENDER` lines** | `RENDER EMPTY`, 0 |
| One `.partial` corrupted (R7) | 1.11.5 | **exit 0 — undetected** | exit 1, `has 1 occurrence(s) … expected 2` |

The R2/R1 row pair is the defect in one line: the pre-fix gate was **green for its author** on
1.11.5 and **red in CI** on 1.11.2, from the same source tree.

The width-sweep row for 1.11.5 is the second, latent class the GOAL predicted: `st_blocks * 512`
breaking mid-literal, invisible today only because CI's width happens to be 78. It fails pre-fix on
the *escaping* `scdoc` too, so pinning `scdoc >= 1.11.5` would never have closed it.

## The harness's own guards were also seen to fire

[`PLAN.md`](PLAN.md) §5 risk 4: if a future edit renames or reindents the step, `awk` could extract a
fragment and every case would pass against a truncated script — a false green. All three refusals
were mutation-tested:

| Mutation to `test.yaml` | Harness output | Exit |
|---|---|---|
| Step renamed `Assert…` → `Verify…` | `extracted an EMPTY gate body … did the step name or indentation change?` | 1 |
| Body line 6 dedented (truncates extraction) | `extracted only 5 lines of gate body … refusing to report a verdict on a fragment` | 1 |
| Body line 25 dedented (24 lines, no diagnostic) | `extracted gate body has no CORRUPT RENDER diagnostic — extraction is wrong, not the gate` | 1 |

The fixture builder carries the same kind of guard: each mutation is re-read after it is written
(`grep -qF '_OUTDIR_/*/*.parquet'`, exactly-one/zero `.partial suffix` counts, the pad prefix), so a
mutation that silently failed to apply cannot let its case pass vacuously. Each R-ID additionally
declares how many cases it must contribute, and a count mismatch is a FAIL even when every line
present says PASS.

## Where this contradicts a recorded plan-time measurement

`PLAN.md` §4 records that on the real 1.11.2 roff at CI's own default width the pre-fix gate "fails
**51 of 51** pad values — it is not luck-dependent there, it is unconditionally red", and attributes
the seed's observed green/red flipping to the 1.11.5 roff. **This harness measures the opposite
distribution:** on 1.11.2 the pre-fix gate is red at **9 of 51** pad values (including pad 0, the
clean tree — which is the real CI failure), and on 1.11.5 it is red at **0 of 51**. The flipping is
on 1.11.2, not 1.11.5.

The most likely cause is a different padding method — this harness prepends whole two-letter words to
the first line of the marker paragraph in the `.scd` source, and where the filler is inserted decides
how the fill shifts. No verdict in this pass depends on the number: pad 0 is red pre-fix and green
post-fix under both methods, and post-fix the count is 0/51 on both roffs and both platforms.
Recorded here rather than quietly corrected, because `PLAN.md` §4 exists precisely to stop the next
pass inheriting an unverified number.

## Reproducing

```sh
sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/gate-matrix.sh   # ~47 s; needs docker
KEEP_WORK=1 sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/gate-matrix.sh  # keep fixtures
```

Docker is required and there is no host-only fallback: the defect is a cross-toolchain skew, and a
host-only check is exactly what kept the gate green for its author while it had never once been green
in CI.
