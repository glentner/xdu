---
slug: rm-limit-deterministic-order
title: Make xdu-rm --limit selection deterministic
kind: fix
appetite: small
status: in_review
branch: fix/rm-limit-deterministic-order
base: main
current_phase: done
last_updated: '2026-07-28'
phases:
- id: P1
  name: Deterministic ORDER BY for --limit (lib helper + wire-in + docs + tests)
  status: done
  satisfies:
  - R1
  - R2
  - R3
  - R4
  depends_on: []
  parallel: false
  hammerable: false
  hill: uphill
  verify: cargo test
review:
  last_reviewed_commit: ''
  verdict: none
  blocked_reason: ''
  cycle: 0
---
# TECH.md — Make xdu-rm --limit selection deterministic

The **context engine and finite-state machine** for building this fix. The YAML frontmatter above is
the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/rm-limit-deterministic-order/TECH.md`);
the per-phase checklist below is the work.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R1–R4 are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).

## Conventions (apply to every phase)

- Commit conventions, code style, and load-bearing invariants come from [`AGENTS.md`](../../AGENTS.md);
  the curated footgun checklist is [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md)
  (§4 destructive safety, §5 injection, §11 testability are the ones in play).
- One atomic commit containing **both** the code and the `TECH.md` state change. Subject:
  `[fix] Build rm-limit-deterministic-order P1: …`.
- **No `Co-Authored-By` trailer.**
- The man-page (`doc/xdu-rm.1.scd`) update lands **in the same commit** as the code (invariant §13).

---

## Phase P1 — Deterministic `ORDER BY` for `--limit`
**Satisfies:** R1, R2, R3, R4 · **Depends on:** —
**Goal:** `xdu-rm --limit N` selects the N lexicographically-smallest paths, deterministically, so a
`--dry-run` preview exactly equals the subsequent real deletion. One end-to-end vertical slice: the
shared rule, the wire-in, the docs, and the tests that prove it.

- [x] Add `pub fn deterministic_limit_clause(limit: Option<usize>) -> String` to `src/lib.rs` —
      returns `"ORDER BY path LIMIT {n}"` for `Some(n)`, `""` for `None`. Declarative comment stating
      *why* (bare `LIMIT` is non-deterministic; `path` is the unique key) — **no `R#`/`P#` ids in the
      comment** (invariant §13).
- [x] Add unit tests in `src/lib.rs` `#[cfg(test)]`: `None → ""`, `Some(5) → "ORDER BY path LIMIT 5"`.
- [x] In `src/bin/xdu-rm.rs`, replace the bare `limit_clause` block (lines ~58-68) with a call to
      `xdu::deterministic_limit_clause(args.limit)`; add it to the `use xdu::{…}` import. Leave the
      dry-run / confirm / `--force` / `--safe` / parallel-unlink paths untouched.
- [x] Update `doc/xdu-rm.1.scd` (the `-l, --limit` entry, ~lines 47-48): document that the N
      lexicographically-smallest paths are selected and that this makes `--dry-run` an exact preview
      of the real run.
- [x] Add/strengthen an integration test in `tests/rm_tests.rs` driving the real binaries: create ≥5
      known-named files in one partition; assert `--dry-run --limit 3` is (a) stable across two runs
      (R1) and (b) equals the 3 lexicographically-smallest paths (R3); then `--limit 3 --force` and
      assert exactly those 3 are gone, the rest remain (R2). Confirm an existing no-limit test still
      passes (R4).
- **Verify:** `cargo test` (lib unit tests + `rm_tests` integration, which drive the real
  `xdu`/`xdu-rm`). Optional manual drive:
  `.agents/factory/bin/temp_index.sh sh -c 'xdu-rm -i "$XDU_INDEX" --limit 2 -n; echo ---; xdu-rm -i "$XDU_INDEX" --limit 2 -n'`
  (the two previews must match).
- **Touches:** `src/lib.rs`, `src/bin/xdu-rm.rs`, `doc/xdu-rm.1.scd`, `tests/rm_tests.rs`.

---

## How `xdu-build` drives this

1. `next_phase.py` prints the next actionable phase (P1).
2. Pre-flight: clean tree, on `fix/rm-limit-deterministic-order`, `main` reachable.
3. Execute every `[ ]` above (consult `PLAN.md` for detail).
4. Run `cargo test` — never advance on a checkbox alone.
5. Mark P1 `done` via `set_phase.py`; one `[fix]` commit containing code + docs + tests + state; stop.
