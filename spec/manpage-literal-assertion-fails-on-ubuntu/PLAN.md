# PLAN — The man-page literal gate asserts content, not layout

> **Status:** Draft for review · **Last updated:** 2026-08-09
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). Every design element traces to a GOAL R-ID.

## 1. Summary

Replace the body of one workflow step — `.github/workflows/test.yaml:139-172`, "Assert critical
literals survive into the published man-page text" — so its verdict depends on the page's **content**
rather than on where `mandoc` happened to break a line. Three changes in the same ~30 lines: normalize
whitespace out of *both* the published text and the literal before matching; render each page exactly
once and guard that render (so a failed or empty render is reported as such, not as six corrupt
literals); and add an exact-occurrence assertion for the one literal that appears twice. Then reconcile
the local check `AGENTS.md` documents — and the copy of it in `invariants.md` §13 — with what CI
actually does. No `src/`, no `doc/*.scd`, no Rust.

The root cause was already measured before this plan ([`GOAL.md`](GOAL.md) §Problem), so this was
planned on the lean path. What research *did* buy was falsifying three things that looked settled —
see §4.

## 2. Design

Everything lands in one shell body inside one YAML step. Final shape:

```sh
set -eu
published() {                                  # render ONCE per page; surface mandoc's status
  raw=$(mandoc -Tutf8 "$1") || return 1        # the old form ended in `tr`, so status was always 0
  printf '%s\n' "$raw" | col -b | tr -d '[:space:]'
}
fail=0
check() {
  page="$1"; shift
  if ! text=$(published "$page"); then
    echo "RENDER FAILED: mandoc could not render $page" >&2; fail=1; return
  fi
  if [ "${#text}" -lt 200 ]; then              # mandoc exits 0 on an empty OR garbage file
    echo "RENDER EMPTY: $page produced only ${#text} characters — nothing to assert" >&2; fail=1; return
  fi
  for spec in "$@"; do
    case "$spec" in
      [0-9]x:*) want="${spec%%x:*}"; lit="${spec#*x:}" ;;   # 'Nx:LITERAL' = assert exactly N
      *)        want=""; lit="$spec" ;;                     # bare LITERAL  = assert present
    esac
    needle=$(printf '%s' "$lit" | tr -d '[:space:]')
    if [ -n "$want" ]; then
      got=$(printf '%s' "$text" | grep -oF -- "$needle" | wc -l | tr -d ' ') || got=0
      [ "$got" -eq "$want" ] || { echo "CORRUPT RENDER: $page has $got occurrence(s) of the literal '$lit', expected $want" >&2; fail=1; }
    elif ! printf '%s' "$text" | grep -qF -- "$needle"; then
      echo "CORRUPT RENDER: $page is missing the literal: $lit" >&2; fail=1
    fi
  done
}
```

Four design points that are load-bearing rather than incidental:

- **One render per page, guarded before any assertion.** The obvious structuring — a `check` for
  presence literals and a separate `check_count` — renders the page twice, so a missing page prints
  `RENDER FAILED` twice and an empty page still emits a misleading `CORRUPT RENDER` after the empty
  guard fires. Measured; both disappear when the render and both guards move above the assertion loop.
- **The empty guard is not belt-and-braces.** `mandoc` exits **0** on an empty file *and* on a garbage
  file (measured), so an exit-status check alone leaves the "rendered to nothing" case producing the
  exact pile of misleading `CORRUPT RENDER` lines R5 exists to remove. Threshold 200 against a shortest
  real page of 1780 characters — a ~9× margin.
- **`|| got=0` is a correctness guard, not tidiness.** With zero matches `grep` exits 1. Today's
  GitHub Actions default shell is `bash -e` (no `pipefail`), so the pipeline status is `tr`'s 0. Under
  `pipefail` it becomes 1, the assignment is a simple command, and `set -e` kills the step *before* the
  diagnostic and before the remaining pages are checked. Measured: two genuine corruptions produce **2
  diagnostics** under `bash -e` and **0 diagnostics, bare exit 1** under `bash -eo pipefail`. Nothing in
  the repo pins the safe shell — adding `shell: bash` to the step, a workflow-level
  `defaults: run: shell: bash`, or hardening to the near-universal `set -euo pipefail` all switch it on,
  and the zero-count case is reachable *precisely by the corruption the count check was added to catch*.
- **The count needle is `'.partial suffix'`, not `'.partial'`.** Stripping whitespace makes the haystack
  lossy, so a needle can match across a token boundary that never existed. `e.g. partial` →
  `e.g.partial` contains `.partial`, so an ordinary sentence inflates the count to 3 and reddens a
  correct page. Both real occurrences (`doc/xdu.1.scd:105`, `:135`) read `… .partial suffix,`, so the
  longer needle is exactly 2 on clean pages and immune to the fusion. **General rule this establishes:
  an exact-count assertion is only sound on a needle specific enough that whitespace removal cannot
  synthesize it** — record that next to the `Nx:` convention.

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1 | `tr -d '[:space:]'` on both sides — verified against roff from **real apt scdoc 1.11.2** on ubuntu-24.04, widths 40–200 and 51 pad values at CI's own width |
| R2 | Same normalization — verified against roff from **real homebrew scdoc 1.11.5**, same sweeps; local and CI reach one verdict |
| R3 | Presence checks retained unchanged in kind; mutation-verified by un-escaping `doc/xdu.1.scd:113` (`scdoc` exit 0, publishes `OUTDIR//.parquet`, gate red naming the literal) |
| R4 | Whitespace removal makes the match layout-independent — covers the intra-token break (`xdu-` / `complete`) *and* the latent TAB class (`col -b` indents with tabs; the old `tr -s ' '` squeezed only spaces) |
| R5 | `published()` returns `mandoc`'s status; single render per page; `RENDER FAILED` / `RENDER EMPTY` short-circuit before any literal is judged |
| R6 | `AGENTS.md` "Commands" gains the normalized form CI runs + a note on the homebrew/distro `scdoc` skew; `.agents/factory/invariants.md` §13 updated in lockstep (it restates the same pipeline at `:198`) |
| R7 | `Nx:` count convention; `'2x:.partial suffix'` catches corruption of *either* single occurrence (measured: count 1 → red) |

## 3. Invariant gate (AGENTS.md constitution check)

Walked against [`invariants.md`](../../.agents/factory/invariants.md) before research and again after
this design. This change touches **§13 only** — no `src/`, no schema, no CLI surface, no destructive
path, no concurrency. §1–§12 are not engaged.

- **§13 — `share/` is generated, git-ignored, CI-asserted.** Untouched: the render step and the
  "Assert all man pages + completions were produced" step are unchanged; only the assertion body
  between them changes. Nothing is committed into `share/`.
- **§13 — `doc/*.scd` authoring rules are single-sourced in `AGENTS.md`'s Commands section.** Honored
  and reinforced: R6 edits that single source. `invariants.md:198` restates the *command* (not the
  rules), and `invariants.md` is required to be kept "in lockstep" with `AGENTS.md`, so it is updated
  in the same commit rather than left to drift.
- **§13 — pre-release gate (`fmt`/`clippy`/`test`).** Unaffected; no Rust changes. Still run at P3.
- **§13 — no `spec/` `R#`/`P#` ids in source.** The workflow comments describe the *invariant* (why
  the normalization exists), never an R-ID.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| `verify:` invokes committed scripts under `spec/{slug}/verify/` instead of the house-style inline `&&` chain | The gate must be proven across two `scdoc` versions × two toolchains × width and pad sweeps × six mutations, with docker orchestration and fixture generation | An inline chain is several hundred characters of quote-escaped shell in a YAML scalar. This pass's whole lesson is that a gate nobody can read is a gate nobody can trust; making the *verifier* unreadable to satisfy formatting would be self-defeating. The scripts double as the evidence artifact Q4 asked to be recorded in the spec |
| P1/P3 `verify:` **require docker** and fail loudly when it is absent | The defect *is* a cross-toolchain skew; ubuntu-24.04's `scdoc` cannot be installed on the host | A host-only verify is precisely the mistake that produced this bug — it is why the gate was green for its author and had never once been green in CI. A "SKIPPED" fallback would repeat harness-log F7's unearned pass |
| The `Nx:` prefix adds a micro-DSL to an otherwise plain literal list | R7's stronger branch (detect single-occurrence corruption) needs a count, and the alternative was deleting coverage | Dropping `.partial` was offered and the human chose the count branch at shaping. Confined to ~6 lines and one call site |

## 4. Rabbit holes (resolved)

No `research/` fan-out (lean path). These were settled by direct measurement, and each **falsified
something that had been treated as settled** — recorded because the PLAN would otherwise inherit them:

- **"Undo `\-` with `sed` to simulate the distro `scdoc`."** False. Real scdoc 1.11.2 differs from
  1.11.5 by 76–129 lines per page: `.PP`→`.P`, and structurally different bullet lists (`.RS 4` +
  `\(bu` + `.ie n`/`.el` conditionals vs `.PD 0`). Any measurement against a simulated roff is
  measuring an artifact. **Both variants must come from a real `scdoc` of that version** — and the
  container must never regenerate a mounted fixture (mount `:ro`; an earlier run silently clobbered the
  1.11.5 fixtures with the container's 1.11.2 and thereby tested one variant twice).
- **"`mandoc` failing is the render failure mode to guard."** Incomplete. `mandoc` exits 0 on empty and
  garbage input; only a length guard catches "rendered to nothing".
- **"The gate passes today by wrapping luck."** Understated. On the real 1.11.2 roff at CI's own width
  the current gate fails **51 of 51** pad values — it is not luck-dependent there, it is unconditionally
  red. (The green/red flipping recorded in the seed was measured on the 1.11.5 roff.)

## 5. Risks & open questions

1. **Doc drift is now a known, accepted risk.** The human chose to keep the gate inline and restate the
   normalization in prose rather than extract one script both invoke — the smaller diff, at the cost
   that `AGENTS.md`/`invariants.md` can drift from CI again, which is the miniature of this very bug.
   *Mitigation (in scope):* a same-commit cross-reference comment in all three places, making it an
   obligation like the existing CLI↔man-page rule rather than a hope.
2. **Token fusion is inherent to the approved normalization, not fully eliminated.** It is closed for
   the one count assertion by a specific needle. For *presence* checks a fused phantom would be a false
   **green**; the dedicated false-negative lens could not construct one, but the class exists and should
   be stated in the gate's comment rather than left implicit.
3. **`groff` breaks the very literal this fix is about, in the page real users read — and the GOAL
   excluded that on a premise now measured false.** `GOAL.md` non-goals decline to settle "whether
   `groff`/`man-db` renders a bare `-` as U+2010", citing an adversarial run that measured zero.
   Measured here on groff 1.23.0, **both** roff variants, default width: `OUTDIR/.xdu-complete`
   publishes as `OUTDIR/.xdu-com` + **U+2010** + newline + `plete`, 10 U+2010 per page, and the literal
   is absent after whitespace-stripping. `man-db` uses groff, so this is what a Debian/Ubuntu operator
   sees and copy-pastes. It does **not** affect this fix's verdict (CI runs `mandoc`, which never
   hyphenates), so it is deferred to `issues/` — but the premise change is the human's to weigh, and
   "`main` is green" must not be read as "the page reads correctly for users".
4. **The verify harness extracts the gate body from `test.yaml` by `awk`** so it tests CI's real code
   rather than a copy. If P1 renames the step or changes its indentation, the harness could extract a
   *partial* script and report a false green. *Mitigation:* the harness asserts the extracted body is
   non-empty, exceeds 20 lines, and contains `CORRUPT RENDER`, before running it.

## 6. Verification strategy

Everything below was already executed at plan time against the candidate body; P1 re-runs it against
the real committed step. The decisive property is **cross-toolchain**, so a host-only run does not
count.

| Dimension | Method | Result at plan time |
|---|---|---|
| R1 + R2 + R4 (width) | widths 40–200 × {real 1.11.5 roff, real 1.11.2 roff} × {macOS/BSD, ubuntu:24.04/GNU} | **644 runs, 0 failures** |
| R4 (the realistic trigger) | pad the marker paragraph 0–150 in steps of 3, at CI's own default width, real 1.11.2 | current **51/51 fail** · proposed **0/51** |
| R3 | un-escape `doc/xdu.1.scd:113`; `scdoc` exit 0 publishes `OUTDIR//.parquet` | red, names the literal |
| R7 | corrupt one of two `.partial` occurrences | red: `has 1 occurrence(s) … expected 2` |
| R5a | delete a page | `RENDER FAILED`, **0** misleading `CORRUPT RENDER` lines |
| R5b | truncate a page to empty (`mandoc` exits 0) | `RENDER EMPTY`, **0** misleading lines |
| Shell hardening | two real corruptions under `bash -e` and `bash -eo pipefail` | 2 diagnostics under both (pre-fix: **0** under pipefail) |
| Regression guard | valid page containing `e.g. partial …` | green (pre-fix: false `CORRUPT RENDER`, count 3 vs real 2) |
| Control | clean tree, both roffs, both platforms | green |

R6 is verified by running the command `AGENTS.md` documents, verbatim, against both roff variants and
confirming it reaches the same verdict as the committed step.

---

*No `research/` directory: `appetite: small`, `kind: fix`, root cause measured before planning. The
adversarial verification that produced §4 and §5 is summarised in [`TECH.md`](TECH.md) P3's ledger.*
