# PLAN — Make `xdu-rm --limit` selection deterministic

> **Status:** Draft for review · **Last updated:** 2026-07-28
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md).

## 1. Summary

`src/bin/xdu-rm.rs` builds its selection query with a bare `LIMIT {n}` and **no `ORDER BY`**
(`xdu-rm.rs:58-68`), so DuckDB returns an arbitrary, unstable subset — a `--dry-run` preview and the
real run can pick different rows. The fix: whenever `--limit` is present, order the query by `path`
(unique per index) before applying `LIMIT`. To keep the destructive tool thin and the rule
unit-testable (invariant §11), the clause is built by a small shared helper in `src/lib.rs` rather
than inline in the binary. This is a one-phase, `appetite: small` correctness fix.

## 2. Design

### The change

**`src/lib.rs` — new helper (the single home of the deterministic-selection rule):**

```rust
/// Build the deterministic `ORDER BY path LIMIT n` tail for a capped query.
///
/// A bare `LIMIT` returns an arbitrary, unstable subset, so ordering and limiting are
/// inseparable: whenever a limit is present the query orders by `path` — unique per index —
/// so a `--dry-run` preview and the subsequent real deletion select identical rows. With no
/// limit the tail is empty (the unlimited path is unchanged; ordering the whole match set
/// would be wasted work).
pub fn deterministic_limit_clause(limit: Option<usize>) -> String {
    match limit {
        Some(n) => format!("ORDER BY path LIMIT {n}"),
        None => String::new(),
    }
}
```

**`src/bin/xdu-rm.rs` — wire it in, replacing the bare `limit_clause` block (lines 58-68):**

```rust
let limit_clause = xdu::deterministic_limit_clause(args.limit);

let sql = format!(
    "SELECT path, size, atime FROM read_parquet('{}') {} {}",
    glob_pattern, where_clause, limit_clause
);
```

(add `deterministic_limit_clause` to the existing `use xdu::{…}` import). No other logic in the
binary changes: the dry-run branch (`xdu-rm.rs:88`), confirm/force gates, `--safe` re-stat, and the
parallel unlink all consume `files` exactly as before — they simply now receive a stable, ordered
slice.

**`doc/xdu-rm.1.scd:47-48` — document the now-defined behavior** so the man page matches reality:
the `--limit` entry states that the N files with the lexicographically smallest paths are selected,
and that this makes `--dry-run` an exact preview of the real run.

### Why `path`, why coupled to `LIMIT`

- `path` is the schema's unique key (invariants §1/§3: each regular file recorded once; `__root__`
  and named partitions never overlap), so `ORDER BY path` is a **total order with no ties** —
  fully determining the selection (R3) with no secondary sort key needed.
- The `ORDER BY` is emitted **only** when `--limit` is set. Without a limit every match is deleted
  regardless of order (R4), and sorting a potentially billion-row match set would be pure waste.
- `n` is a `usize` from clap and the ordering column is a hardcoded literal — the helper introduces
  **no** new user-controlled SQL (invariant §5).

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1   | `deterministic_limit_clause` emits a stable `ORDER BY path` → repeated identical runs select the same N rows; unit test + integration test assert stability. |
| R2   | Both the dry-run branch and the real-deletion branch consume the *same* ordered `files` vector from one query; integration test runs dry-run then real and asserts the deleted set equals the previewed set. |
| R3   | `ORDER BY path` (ascending) selects the N lexicographically-smallest paths; `path` uniqueness guarantees no ties. |
| R4   | `limit == None` → helper returns `""` → query and downstream behavior are byte-for-byte the current unlimited path; existing no-limit tests (`test_force_delete_removes_files`, etc.) still pass. |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) before
research and again after this design.

- **§4 (`xdu-rm` destructive safety)** — this change *implements* the mandated "any deletion combined
  with `--limit` MUST carry a deterministic `ORDER BY`" rule. The confirm / `--dry-run` / `--force` /
  empty-match gates and `--safe` re-stat are untouched. **Directly satisfies the invariant.**
- **§5 (DuckDB injection surface)** — the ordering column is a fixed literal (`path`); the limit is a
  type-safe `usize`. No new interpolated user value. (The pre-existing raw interpolation of the glob
  pattern / partition name is a *separate* backlog item — see Risks; explicitly not touched here.)
- **§11 (altitude / testability)** — the selection rule lives in `lib` as a pure function with unit
  tests; the binary stays thin. Honored.
- **§10 / §13 (CLI single source / same-commit docs)** — no flag is added or changed, but the
  observable behavior of `--limit` is now defined, so `doc/xdu-rm.1.scd` is updated in the same
  commit. `src/cli.rs` is unchanged, so completions are unaffected. Honored.
- **§1 (schema stability)** — untouched; `path` is an existing column. No schema change.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| —         | —         | — |

Empty — the design bends no invariant.

## 4. Rabbit holes (resolved)

Small fix, known root cause — no research fan-out. The one thing to confirm was where the rule
belongs: **`lib` helper** (testable, §11) over inline SQL in the binary. Resolved in favor of the
helper.

## 5. Risks & open questions

- **Adjacent, deliberately-untouched bug:** `glob_pattern` and the `--partition` name are still
  `format!`-interpolated raw into the SQL (`xdu-rm.rs:37-41,65-68`) — the injection gap (backlog #2,
  invariant §5). It is a **non-goal** here; do not fold it in. Noted so review does not read its
  presence as a regression introduced by this fix nor expect it fixed.
- **Non-UTF-8 paths** are stored lossily (`to_string_lossy`, a separate backlog item); two distinct
  files could in principle map to the same stored string. `ORDER BY path` is still deterministic over
  the *stored* strings, so R1/R2 hold. Out of scope.
- No open questions — the selection order was resolved in `GOAL.md` (path order only).

## 6. Verification strategy

- **Unit (`src/lib.rs`):** `deterministic_limit_clause(None) == ""` and
  `deterministic_limit_clause(Some(5)) == "ORDER BY path LIMIT 5"`.
- **Integration (`tests/rm_tests.rs`, drives the real `xdu`/`xdu-rm` binaries):** create ≥5
  known-named files in one partition; `--dry-run --limit 3` twice and assert both previews are
  identical **and** equal the 3 lexicographically-smallest paths (R1, R3); then `--limit 3 --force`
  and assert exactly those 3 are gone and the rest remain (R2). The existing `test_limit_option`
  (count-only) is strengthened or joined by this determinism test.
- **Real-CLI drive (manual confidence):**
  `.agents/factory/bin/temp_index.sh sh -c 'xdu-rm -i "$XDU_INDEX" --limit 2 -n; echo ---; xdu-rm -i "$XDU_INDEX" --limit 2 -n'`
  — the two previews must be identical.
- **Gate before publish:** `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features
  -- -D warnings`, `cargo test` all clean.
