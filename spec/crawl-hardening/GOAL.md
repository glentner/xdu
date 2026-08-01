# GOAL — Harden & optimize the index-build crawl (and surrounding architecture)

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).
> Keep this at the right altitude: solved and bounded, but not over-specified — leave design
> freedom for the plan. Edit requirements here; do **not** silently drift them during build.

- **slug:** crawl-hardening
- **kind:** refactor
- **appetite:** big

## Problem

`xdu` exists because `du`(1) and `find`(1) collapse at HPC / enterprise scale (hundreds of millions
to billions of files). Its whole value proposition rests on one hot path: the **index-build crawl** —
a jwalk walk driven by a single shared rayon pool, with `--jobs` driver threads pulling top-level
partitions off a `Mutex<VecDeque>` work queue, rayon work-stealing balancing directory reads across
active walkers, and `thread::scope` propagating the first error. That path buffers records into
`PartitionBuffer`s and writes them out as atomically-finalized Parquet chunks.

We pivoted to jwalk and hand-rolled this work-stealing model relatively recently, and it has **never
been rigorously audited**. Meanwhile the project has become load-bearing and has drawn the attention
of the national research-computing and data community — the exact population that will point it at
the largest, gnarliest trees in existence and trust its output for storage administration decisions.

Two things are now unacceptable to leave unverified. **First, correctness:** a concurrency bug in this
model — a lost or double-counted file, a starved partition, a swallowed error, a torn/partial chunk a
reader can observe, a mis-attributed `__root__` file — silently corrupts an index that operators act
on. That is the floor we must not be below. **Second, performance:** at billions of files, constant
factors and pipeline stalls in the crawl are the difference between an overnight index and one that
never finishes. jwalk is where we are holding for now (alternative backends like Lustre/GPFS/ZFS are a
later conversation), so we need to know we have extracted the performance this approach can reasonably
give — and to have said clearly, with evidence, where the remaining ceiling comes from. Around that
core, the wider codebase has accreted and deserves a design pass so the load-bearing parts are as
cleanly architected and testable as the stakes now demand.

## Outcome / vision

The index-build crawl is **provably correct and cleanly architected**, and its performance envelope is
**measured, understood, and documented** rather than assumed.

- Every plausible concurrency hazard in the shared-pool / driver-thread / work-queue / finalize model
  has been examined, classified (real bug · latent hazard · non-issue-with-rationale), and every
  confirmed bug fixed with a regression test that drives the real binary.
- The crawl's throughput has a reproducible baseline, the concrete inefficiencies we could find have
  been removed, and any remaining gap is explained as inherent to the jwalk approach — not hand-waved.
- A maintainer can reason about the concurrency model from the code and its documented invariants
  without re-deriving them, and the wider codebase reads as one coherent, testable design.
- The national-scale community has a written benchmark protocol they can run on real Lustre/GPFS/ZFS
  to validate performance at a scale we cannot reproduce in-loop.
- None of this changes the on-disk index format or the default observable behavior of the tools.

## Acceptance criteria (the contract)

- **R1** — The concurrency model of the index-build crawl (shared rayon pool, `--jobs` driver
  threads, the `Mutex<VecDeque>` work queue, work-stealing balance, and `thread::scope` error
  propagation) SHALL be systematically audited, and the audit recorded as a written artifact that
  enumerates each hazard considered and classifies it as **real bug**, **latent hazard**, or
  **non-issue** with a concrete rationale for each.
- **R2** — IF the audit confirms a correctness bug (e.g. a lost or double-counted file, a starved or
  skipped partition, a swallowed error/panic, a reader-observable partial chunk, or a mis-attributed
  `__root__` file), THEN it SHALL be fixed and covered by a regression test that drives the real
  binary and asserts a concrete post-condition (index row count / a specific path present-or-absent /
  correct exit status), not merely exit 0.
- **R3** — WHEN a walk encounters an I/O error or a driver thread panics on any partition, the `xdu`
  crawler SHALL fail the run with a non-zero exit and a clear diagnostic, and SHALL NOT silently
  finalize a partial or corrupt index as if it were complete.
- **R4** — The crawl SHALL have a reproducible synthetic benchmark (documented, scalable trees
  spanning varied fan-out/depth/file-size mixes) with a recorded **baseline** measurement of the
  current implementation, so any performance change is quantified against it rather than asserted.
- **R5** — Concrete crawl inefficiencies identified against the baseline SHALL be removed where the
  change is evidence-backed and preserves correctness, and the remaining performance ceiling SHALL be
  documented with a rationale attributing it to the jwalk approach (characterize-and-justify — there
  is no fixed throughput number this pass must hit, and a change that does not measurably help SHALL
  NOT be merged as if it did).
- **R6** — The index-build hot path SHALL be refactored for clarity and testability — bins stay thin,
  logic that can live in `lib` does, and the concurrency model's contract is expressed in-code as
  declarative invariant comments — WITHOUT changing the on-disk index schema (`get_schema()` — three
  non-null fields, fixed order) or the default observable behavior of the crawl.
- **R7** — WHERE performance evidence justifies a new tuning surface, it SHALL be exposed only as an
  **opt-in** flag defined in `src/cli.rs` with its `doc/*.scd` man page updated in the same change;
  existing flags, their defaults, and their observable behavior SHALL remain unchanged.
- **R8** — A wider architecture assessment of the codebase (the readers `xdu-find`/`xdu-view`/`xdu-rm`
  and shared `lib`/`cli` relative to the indexer) SHALL be produced, and the clearly-warranted,
  low-risk cleanups it surfaces applied this pass; anything larger or riskier SHALL be recorded as an
  explicit follow-up rather than attempted here.
- **R9** — A written HPC benchmark protocol (targeting Lustre/GPFS/ZFS) SHALL be delivered — inputs,
  method, metrics, and the shape of expected results — such that a community operator can run it on a
  real large-scale filesystem to validate performance independent of our synthetic harness.
- **R10** — All load-bearing invariants SHALL be preserved (schema stability, atomic
  partial→rename finalization + stale-chunk pruning, the partition scheme incl. `__root__`, symlink
  exclusion, Unix-only assumptions, clean pipeable non-TTY stdout with progress on stderr), and the
  full pre-release gate (`cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features --
  -D warnings`, `cargo test`) SHALL pass clean.

## Non-goals (no-gos)

- **Alternative filesystem backends.** No Lustre/GPFS/ZFS-specific crawl paths, no S3/object-store or
  Windows source. jwalk is the held approach for this pass; other backends are a later conversation
  (R9 only *hands off* a protocol for the current implementation).
- **Any on-disk index-format change.** No new columns (owner/group/perms — issues #2/#3), no schema
  version, no partition-layout change. Schema stability (invariant #1) is untouched here.
- **Changing existing default behavior or CLI semantics.** No new default `--jobs`/`--buffsize`
  values, no repurposed flags; new surfaces are opt-in only (R7).
- **A full rewrite of the reader tools or the TUI.** R8 is an assessment plus bounded low-risk
  cleanups, not an open-ended rearchitecture of `xdu-view`/`xdu-find`/`xdu-rm`.
- **A guaranteed performance number.** The perf bar is characterize-and-justify (R5); this pass does
  not commit to a specific speedup gate.
- **New query/deletion features.** ROADMAP items are out of scope; this pass hardens the foundation
  they will build on.

## Clarifications

- **Q:** How is the performance half judged "done"? — **A:** Characterize-and-justify: baseline,
  remove concrete inefficiencies, document remaining gaps as inherent to jwalk; no fixed throughput
  target (resolved 2026-07-31).
- **Q:** What can we benchmark against during the work? — **A:** Synthetic trees in-loop for
  iteration, plus a documented HPC benchmark protocol for the community to run on real
  Lustre/GPFS/ZFS (resolved 2026-07-31).
- **Q:** What is the scope boundary? — **A:** Broader codebase design: centered on the index-build
  crawl (the mandatory core), extended to a wider architecture assessment across the readers/CLI with
  bounded low-risk cleanups (resolved 2026-07-31).
- **Q:** How much behavioral/CLI change is allowed? — **A:** Internal refactor + performance work;
  preserve default observable behavior and the index format; new tuning knobs allowed only as opt-in
  flags (resolved 2026-07-31).

## Related materials

- Operating manual & invariants: [`AGENTS.md`](../../AGENTS.md),
  [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) (esp. #1 schema, #2 atomic
  finalize, #3 partition scheme, #7 shared-pool concurrency, #8 symlinks).
- Crawl source: [`src/bin/xdu.rs`](../../src/bin/xdu.rs) (shared-pool walk, `PartitionBuffer`,
  `finalize()`, `__root__`), shared core [`src/lib.rs`](../../src/lib.rs), CLI
  [`src/cli.rs`](../../src/cli.rs).
- Existing crawl tests: [`tests/crawl_tests.rs`](../../tests/crawl_tests.rs) (note: currently
  reimplements the crawler — a known gap, not a pattern to copy).
- Forward context: [`ROADMAP.md`](../../ROADMAP.md) — this pass hardens the foundation before its
  first item.
