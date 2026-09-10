# PLAN — Crawl progress misleads on huge trees

> **Status:** Draft for review · **Last updated:** 2026-09-09
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). Lean path: `kind: fix`,
> `appetite: small`, root cause verified on `main` — no `research/` fan-out.

## 1. Summary

Display-only fix in `src/bin/xdu.rs`, backed by pure message builders in `src/lib.rs`. The
`[Tn]` driver token is deleted from the per-partition line; the throttled bar refresh is hoisted
so directory entries trigger it too, with a quiet-state message carrying directories visited and
elapsed time. No new threads, no pool change, no CLI change, non-TTY output untouched.

## 2. Design

All line numbers below are `src/bin/xdu.rs` on `main`.

**R1 — drop the false ownership claim.** The per-partition message at `:413-420` interpolates
`[T{driver_id}]`, naming the bar-owning driver while the shared rayon pool does the walking.
Delete the token; claim nothing in its place. A replacement thread claim (worker counts,
queue depth) would need new shared state and risks restating the same falsehood in fancier
terms; the global totals already answer "is the run moving". Removing `driver_id` from the
message leaves it moved-but-unused in the driver closure — clean that up (`_driver_id` or drop
the binding) so `clippy -D warnings` stays green.

**R2 — feed directory entries into the refresh.** Today the `now - last_bar_update >= 100ms`
block (`:363-428`) sits behind the `!is_file → continue` (`:297-299`), so a partition in slow
metadata traversal never refreshes. Restructure: count non-file entries in a driver-local
`dirs_visited: u64`, then run the same throttled refresh for every walker entry. When
`buffer.file_count == 0`, render the quiet branch — required elements are the partition name,
directories visited, and elapsed time since the partition started; exact wording is the build's
choice. When files flow, the lively branch keeps its current shape (counts, bytes, speed) minus
`[Tn]`. One interval (`bar_interval`, 100 ms) governs both branches; no second clock.

**R3 — the global line rides the same hoist.** The global totals refresh (`:421-426`) lives
inside the same gated block, so it freezes exactly when every partition is quiet. Hoisting the
block fixes the global line with no separate change; its shape is unchanged.

**R4 — the walk path is untouched.** Per entry, the added cost is one counter increment and one
`Instant` comparison — the clock read already happens for every file, and directory entries now
pay it too. No new threads (the thread budget in §7 stands), no new shared atomics, no pool or
queue change. `enable_steady_tick` on both bar kinds is unchanged.

**Testability (§11).** Extract the partition-line rendering into `src/lib.rs` as a pure
function (one function covering both branches, e.g. taking partition, file/dir counts, bytes,
elapsed, and the optional speed fragment — exact signature is the build's choice) and unit-test
both branches there, reusing `format_count`/`format_bytes`. The bin keeps orchestration:
counting, throttling, `set_message`. The global line stays inline; its shape does not change.

**Out of scope, restated.** Non-TTY stderr posture byte-identical (draw target hidden, `Finished`
lines as-is). No flag, no schema, no partition-scheme, no marker change.

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1   | Delete `[T{driver_id}]` from the per-partition message (`:413-420`); clean up the unused binding. |
| R2   | Driver-local `dirs_visited` + hoisted 100 ms refresh + quiet-branch message (partition, dirs, elapsed) via a new pure `lib` builder with unit tests. |
| R3   | Global totals refresh covered by the same hoist; message shape unchanged. |
| R4   | No thread/pool/queue change; per-entry cost is one increment plus one clock read; existing crawl tests plus a `temp_index.sh` drive prove no regression. |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) (§1–§13)
**before** research and **again** after this design was drafted.

- §7 shared rayon-pool concurrency — single pool, driver threads, `thread::scope` propagation all
  unchanged; the design adds no thread and no shared state.
- §11 altitude/testability — new rendering logic lands in `lib` as a pure, unit-tested builder;
  the bin keeps only counting, throttling, and `set_message`.
- §13 project conventions — no CLI change, so no `doc/*.scd` update; non-TTY stdout stays clean
  (untouched code path); no version strings; no `spec/` R-IDs in source; `fmt`/`clippy -D
  warnings`/`test` gate every phase.
- §1 schema, §2/2b/2c finalize/marker/fail-loud, §3 partitions, §4 `xdu-rm`, §5 SQL injection
  (partition names reach only `set_message`, never SQL), §6 unix-only, §8 symlinks, §9 sort,
  §10 CLI single source, §12 TUI safety — untouched by construction; the diff cannot reach them.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| —         | —         | — |

## 4. Rabbit holes (resolved)

Lean path — no fan-out. Three unknowns were settled by direct reads of `src/bin/xdu.rs`:

- Liveness without extra syscalls: directory entries already flow through the walker loop
  (`:297` filters them after yield), so counting them is free — no `stat`, no readdir change.
- No ticker thread needed: a slow-metadata walk still yields entries continuously, so the loop
  is never blocked between files — it just never takes the file branch. Hoisting the refresh
  into the common path suffices, and adding a thread would bend the §7 budget for nothing.
- indicatif 0.17 needs no new API: `set_message` under the existing `enable_steady_tick`
  spinner already animates; only message freshness was missing.

## 5. Risks & open questions

- TTY rendering itself is not integration-testable without a pty: the contract is proven by
  unit tests on the message builder plus review of the hoist, while integration proves the
  index is unchanged. That split is honest about what each layer can show.
- A single gigantic directory *read* (one `getdents` returning millions of entries) still
  yields entries one at a time to the loop, so the refresh fires; a truly blocked syscall
  shows a stale message under a still-spinning spinner — accepted, and the spinner is the
  residual life signal there.
- Exact quiet-branch wording and builder signature are the build's choice within §2's required
  elements; no human input needed before build.

## 6. Verification strategy

- `cargo test --lib` covers the new builder (both branches) alongside the existing formatters.
- Full `cargo test` plus a `temp_index.sh` drive (`xdu-find --count` against the throwaway
  index) prove the crawl still indexes every fixture file — the display change moves no byte
  of index output.
- `cargo fmt --all -- --check` and `cargo clippy --all-targets --all-features -- -D warnings`
  run in the final phase; TTY visuals get a manual drive (operator reads the lines on a skewed
  tree) outside the automated gate.
