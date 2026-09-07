---
slug: richer-search-glob-fuzzy-fulltext
title: Glob as the default path-match dialect (pilot)
kind: feature
appetite: small
status: blocked
branch: feature/richer-search-glob-fuzzy-fulltext
base: main
current_phase: done
last_updated: '2026-09-07'
phases:
- id: P1
  name: Glob translator in lib, wired through xdu-find
  status: done
  satisfies:
  - R1
  - R2
  - R3
  depends_on: []
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test --lib && .agents/factory/bin/temp_index.sh sh -c 't=$(xdu-find
    -i "$XDU_INDEX" --count); a=$(xdu-find -i "$XDU_INDEX" -p "*" --count); b=$(xdu-find
    -i "$XDU_INDEX" --regex -p ".*" --count); [ "$t" = "$a" ] && [ "$a" = "$b" ] &&
    ! xdu-find -i "$XDU_INDEX" -p "[" --count'
- id: P2
  name: xdu-rm on the glob dialect
  status: done
  satisfies:
  - R1
  - R2
  - R3
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test --test rm_tests && .agents/factory/bin/temp_index.sh sh -c '!
    xdu-rm -i "$XDU_INDEX" -p "[" --dry-run --force'
- id: P3
  name: xdu-view startup and interactive pattern on the glob dialect
  status: done
  satisfies:
  - R1
  - R2
  - R3
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo build --bins && .agents/factory/bin/temp_index.sh sh -c '! xdu-view
    -i "$XDU_INDEX" -p "["'
- id: P4
  name: Full gate and deferral ledger
  status: done
  satisfies: []
  depends_on:
  - P1
  - P2
  - P3
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo fmt --all -- --check && cargo clippy --all-targets --all-features
    -- -D warnings && cargo test && test -f issues/fuzzy-filename-matching.md && test
    -f issues/duckdb-fts-evaluation.md
review:
  last_reviewed_commit: 18bb76bea98a74ec40a9b97d26bc4bc3dff9df73
  verdict: changes-requested
  blocked_reason: Glob range defect (lib.rs) + AGENTS.md CLI drift; TUI prompt PLAUSIBLE
  cycle: 1
---
# TECH.md — Glob as the default path-match dialect (pilot)

The **context engine and finite-state machine** for building this feature. The YAML
frontmatter above is the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/{slug}/TECH.md`); the per-phase
checklists below are the work. `xdu-build` executes the next actionable phase, runs its
`verify:` command, updates state via
`uv run --with pyyaml python .agents/factory/bin/set_phase.py …`, and makes one atomic code+state commit.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R-IDs are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).
- **Backing research:** none — lean path (`appetite: small`).

## Frontmatter field reference

- `status` (top): `planned | in_progress | blocked | in_review | done` (`done` is stamped by
  `xdu-publish` after confirmation, just before landing — the terminal state of the retained record)
- `appetite`: `small | big` — caps phase count and build-iteration budget (circuit breaker).
- phase `status`: `pending | in_progress | done | blocked`
- `satisfies`: GOAL R-IDs this phase delivers (traceability anchor for `xdu-review`).
  P4 carries `[]`: it delivers no R-ID itself — it owns the full gate and the deferral
  ledger the factory requires of the last phase. `xdu-review` grades R1–R3 against P1–P3.
- `depends_on`: phase ids that must be `done` first (a phase is actionable only when met).
- `parallel`: `true` only for genuinely independent, non-coupled work (docs, tests, isolated `lib`
  helpers). The coupled core (`src/bin/xdu-rm.rs`, `src/bin/xdu.rs`, `src/lib.rs`, `src/cli.rs`) is
  always `false`.
- `hammerable`: `false` marks a correctness/security phase that scope-hammering must **never** cut.
- `hill`: `uphill` (still figuring it out) → `crest` (unknowns resolved) → `downhill` (just
  executing). A phase stuck `uphill` across builds is a raised hand → escalate to the human.
- `attempts`: durable failed-verify counter (absent = 0), bumped by `set_phase.py --phase P<n>
  --record-attempt` on every red verify gate; `next_phase.py` warns at ≥3 — the circuit breaker
  runs on this file, not on session memory.
- `verify`: the exact command that proves the phase (prefer driving the real CLI, not just tests —
  wrap CLI drives in `.agents/factory/bin/temp_index.sh sh -c "…"` so they hit a throwaway index,
  never a real index / the developer's real filesystem).
- `review.cycle`: completed review passes, auto-incremented by every `set_phase.py --verdict …`;
  REVIEW.md's "Cycle {n}" mirrors it and the ≤3-cycle bound is graded against it.

## Conventions (apply to every phase)

- Commit conventions, code style, and load-bearing invariants come from
  [`AGENTS.md`](../../AGENTS.md) — it is the constitution. Consult
  [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) for the curated footgun
  checklist relevant to this change.
- One phase per `xdu-build` invocation by default; one atomic commit containing **both** the code and
  the `TECH.md` state change. Branch commit subjects follow the house style `[{category}] Build {slug}
  P<n>: …` (no `WIP:` prefix) — squashed into the single PR-title commit at `xdu-publish`.
- **No `Co-Authored-By` trailer** (attribution lives in the PR body, not the commit); PR **bodies** end
  with an attribution trailer naming the actual harness, model, and variant.
- A CLI/feature change updates the affected `doc/*.scd` man page **in the same commit** (shell
  completions regenerate from `src/cli.rs`; the generated `share/` tree is git-ignored, not committed).

---

## Phase P1 — Glob translator in lib, wired through xdu-find
**Satisfies:** R1, R2, R3 · **Depends on:** —

**Goal:** The translation core exists, is unit-tested, and is observable end to end through
`xdu-find`: glob by default, regex behind `--regex`, invalid globs rejected.

- [x] `src/lib.rs`: add `glob_to_regex` per PLAN §2.1 plus `QueryFilters::with_path_pattern`
  and `pattern_display` per §2.2, with unit tests for each metacharacter, escaping rule,
  and rejection.
- [x] `src/cli.rs`: add long-only `--regex` to all three query arg structs now (one
  coherent CLI change, not three), rewrite `-p/--pattern` help (`REGEX` → `PATTERN`).
  Wiring the rm/view call sites is P2/P3; the flag simply exists there until then.
- [x] `src/bin/xdu-find.rs`: build filters via `with_path_pattern`; `after_help` examples
  to glob-first plus one `--regex` example.
- [x] `doc/xdu-find.1.scd`: synopsis, flag text, `--regex` entry, glob examples (`\*`
  escaped); render and literal-check per PLAN §2.5.
- **Verify:** `cargo test --lib && .agents/factory/bin/temp_index.sh sh -c '…'` — the drive
  asserts `-p '*'` count equals the unfiltered count, equals the `--regex -p '.*'` count,
  and that `-p '['` exits non-zero (fixture-independent by construction).
- **Touches:** `src/lib.rs`, `src/cli.rs`, `src/bin/xdu-find.rs`, `doc/xdu-find.1.scd`.

## Phase P2 — xdu-rm on the glob dialect
**Satisfies:** R1, R2, R3 · **Depends on:** P1

**Goal:** The destructive tool takes glob by default with the failure closed before any
deletion set is selected; its tests prove both dialects and the fail-closed path.

- [x] `src/bin/xdu-rm.rs`: build filters via `with_path_pattern`; `after_help` example to
  glob-first (`--regex` is documented in help text and the man page).
- [x] `doc/xdu-rm.1.scd`: same man-page treatment as P1's find page.
- [x] `tests/rm_tests.rs`: convert the regex-valued `--pattern` assertions (`\.log$`,
  `\.nonexistent$`) to glob; add `--regex` coverage proving the old behavior survives
  behind the switch, and an invalid-glob case asserting non-zero exit with files kept.
  Never reimplement production logic in a test — assert through the real binary.
- **Verify:** `cargo test --test rm_tests` plus a `temp_index.sh` drive proving an invalid
  glob under `--dry-run --force` still exits non-zero (prompt skipped, nothing selected).
- **Touches:** `src/bin/xdu-rm.rs`, `doc/xdu-rm.1.scd`, `tests/rm_tests.rs`.

## Phase P3 — xdu-view startup and interactive pattern on the glob dialect
**Satisfies:** R1, R2, R3 · **Depends on:** P1

**Goal:** The TUI takes glob on startup and in its interactive `/` prompt, with validation
ordered before the terminal is touched.

- [x] `src/bin/xdu-view.rs` startup: build filters via `with_path_pattern` before
  `enable_raw_mode`, so an invalid glob exits non-zero without ever owning the terminal.
- [x] `confirm_input` (`InputMode::Pattern`): route new input through `glob_to_regex`;
  report the `Err` as a status-bar message, matching the existing bad-number path. The
  stored field stays one dialect (regex) regardless of who set it.
- [x] `doc/xdu-view.1.scd`: same man-page treatment, including the `/`-prompt line that
  today says regex.
- [x] `tests/offline_tests.rs`: convert the `\.log$` dry-run assertion to glob (it drives
  `xdu-rm`, but the conversion belongs with the last behavior phase; P2 must stay green
  with either spelling since translation is exact).
- **Verify:** build plus a `temp_index.sh` drive proving an invalid startup glob exits
  non-zero. The interactive prompt shares `glob_to_regex` with P1's unit tests; exercise it
  once by hand during the phase and note the result in the commit body.
- **Touches:** `src/bin/xdu-view.rs`, `doc/xdu-view.1.scd`, `tests/offline_tests.rs`.

## Phase P4 — Full gate and deferral ledger
**Satisfies:** (none — gate and ledger phase; see note under Frontmatter) · **Depends on:**
P1, P2, P3

**Goal:** The tree is gate-clean and every scope deferred along the way is recorded where
the next cycle will find it.

- [x] Run the pre-release gate exactly as CI mirrors it: `cargo fmt --all -- --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test`. Requires
  `scdoc` for the render checks below (`brew install scdoc`); do not weaken the gate if
  it is missing — install it.
- [x] Re-render all three touched man pages; literal-check the glob examples and the
  `--regex` entries with the whitespace-stripped match from `AGENTS.md` Commands.
- [x] Deferral ledger: walk P1–P3 for "do not fix here" / "known limitation" /
  "follow-up" language. Each item gets an `issues/{slug}.md` pre-shaped from
  `.agents/factory/templates/ISSUE.md` (`status: unshaped`) plus a `ROADMAP.md` entry.
  Known at plan time: `issues/fuzzy-filename-matching.md` and
  `issues/duckdb-fts-evaluation.md`, and the `ROADMAP.md` "Richer search" entry splits —
  glob pilot ships in this cycle, fuzzy and FTS point at the new seeds. Never `META.md`: that
  file is harness feedback, not a code-follow-up record.
- [x] Confirm each ledger item or record its absence; an unrecorded deferral fails the
  phase.
- **Verify:** gate commands plus `test -f` on the two known follow-up seeds (further
  seeds discovered in the walk join both the ledger and this command).
- **Touches:** `issues/`, `ROADMAP.md`, `doc/*.scd` (only if the render check finds a
  defect).

---

## How `xdu-build` drives this

1. `next_phase.py` prints the next actionable phase (statuses are authoritative; the
   `current_phase` pointer is reconciled against them).
2. Pre-flight: clean tree, on `branch`, `base` reachable.
3. Execute every `[ ]` in the phase (consult `PLAN.md` / `research/` for detail).
4. Run the phase's `verify:` command — never advance on a checkbox alone.
5. Amend this file freely if reality diverges (regenerate frontmatter with `set_phase.py`; note the
   amendment in the commit body). STOP and escalate only on a **`GOAL.md` contradiction**.
6. Mark the phase `done`, advance `current_phase`, `--touch`; one `[{category}]` commit; stop and report.
