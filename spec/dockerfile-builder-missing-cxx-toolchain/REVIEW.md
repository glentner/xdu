# REVIEW — Restore the container build with an explicit C++ toolchain

> Adversarial QA by `xdu-review`, run in an isolated/clean context. The correctness pass grades the
> branch diff against [`GOAL.md`](GOAL.md) + the AGENTS.md invariants **only** — it does not see
> `PLAN.md`/`TECH.md` (avoids grading-its-own-homework / plan-sycophancy). Every finding cites an
> **executed** command, not an assertion.

- **Reviewed commit:** c571dd93d1be322e27cb70b9849c8f3d38b104f6
- **Base:** main
- **Date:** 2026-09-07
- **Verdict:** approved
- **Cycle:** 1 of ≤3 (mirrors review.cycle in TECH.md; escalate on non-convergence)

Mode: full blind pass over the spec-excluded diff (not a scoped remediation re-check).
Contract-drift check — only the shaping commit touches the locked contract:

    git log --oneline main..HEAD -- spec/dockerfile-builder-missing-cxx-toolchain/GOAL.md

so the locked contract did not move mid-build.
Branch status note: `TECH.md` was `in_progress` (P1–P3 done, P4 pending by design as post-merge
human-gated), not `in_review`; the human approved proceeding with the review on that basis.

## Verification run

Commands actually executed and their outcomes (the spine of the review):

- Blind reviewer, premise probes (all `--rm` throwaway containers, nothing persisted):
  `docker run --rm rust:1-slim-bookworm …` → `cc: present`, `gcc: present`, `c++: MISSING`,
  `g++: MISSING` (GOAL premise holds).
  `docker run --rm debian:bookworm-slim sh -c 'apt-get install -y g++ …'` →
  `update-alternatives: using /usr/bin/g++ to provide /usr/bin/c++ (c++)` and post-install
  `/usr/bin/c++ -> /etc/alternatives/c++` present (the `g++` fix provides the literal `c++`).
  `docker run --rm debian:bookworm-slim dpkg -l libstdc++6 libgcc-s1 ca-certificates` →
  `ii libgcc-s1`, `ii libstdc++6`, `un ca-certificates` (R4 comment factually correct).
- Blind reviewer, diff-shape: `git diff main...HEAD --name-only -- . ':(exclude)spec/'` →
  exactly `Dockerfile`, `.github/workflows/docker.yaml`,
  `issues/dockerfile-builder-missing-cxx-toolchain.md`. `git diff … -- src/ tests/ Cargo.toml
  Cargo.lock rust-toolchain.toml | wc -l` → `0` (no Rust touched; invariants §1–§12 untouched
  by construction). `grep -rn "R#\|P#" Dockerfile .github/workflows/docker.yaml` → clean
  (no spec ids leaked into source).
- Blind reviewer, R1 wiring: `sed -n '18,52p' Dockerfile` → `g++` layer precedes
  `cargo build --release --locked --bin xdu --bin xdu-find --bin xdu-view --bin xdu-rm`;
  `sed -n '53,79p' Dockerfile` → all four `COPY`ed to `/usr/local/bin/`;
  `grep -A2 '\[\[bin\]\]' Cargo.toml` → names match (`gen-completions` correctly excluded).
- Blind reviewer, YAML: `ruby -ryaml` parse of `.github/workflows/docker.yaml` green →
  `validate: 30, build: 30, merge: 15`; `grep -n timeout-minutes` confirms only the two build
  jobs changed 20→30, each with the cold-compile comment; `merge` untouched at 15.
- Orchestrator re-checks (2026-09-07): `git status --porcelain` empty (tree clean on hand-back);
  `ruby -ryaml` re-parse → `validate: 30, build: 30, merge: 15`;
  `grep -q 'literally named' Dockerfile` OK; `grep -q 'effectively installs only' Dockerfile` OK;
  `git log main..HEAD --format='%h %s%n%b'` → P1 body records local build green in 3m6s on
  arm64, P2 body notes the CI-duration half stays open for the PR body, P3 body records the
  offline drive and the arm64/amd64 split.
- Gates **not observed** (recorded as such, never as satisfied): `cargo test` / clippy / fmt
  were not run in this review — the diff touches no Rust, and the pre-release gate still applies
  at publish. A full `docker build` was not re-run here — R1's mechanism is proven above and the
  P1 commit body records the green 3m6s build; both-arch proof is the PR's `validate` legs.
  No `doc/*.scd` is touched, so the scdoc render gate is not applicable.

## Requirement → evidence matrix

Bidirectional traceability. R6 and the R5 duration half are orchestrator-graded (not executable
on the branch); R1–R4 and the R5 timeout half were graded by the blind reviewer.

| R-ID | Implemented by (file/commit) | Verified how | Status |
|------|------------------------------|--------------|--------|
| R1 | `Dockerfile` builder `g++` layer (P1, `8ab5556`) | Reviewer: premise + fix probes above, build→COPY→`[[bin]]` name match; full-image compile left to PR `validate` legs | ✅ (mechanism proven; both-arch green owed by CI) |
| R2 | Unchanged image posture + `bundled,parquet` offline mechanism (P3, `c571dd9`, verification-only) | Reviewer (static): `USER xdu` + real `$HOME`, `Cargo.toml` `parquet` feature comment; no live run required absent a defect candidate | ✅ on the branch |
| R3 | `Dockerfile:23-24` builder comment (P1) | `grep -n "c++" Dockerfile` → lines 23–24, immediately above the install, naming `bundled`, `cc-rs`, literal `c++`, `rust:1-slim-bookworm` | ✅ |
| R4 | `Dockerfile:58-61` runtime comment (P2, `65c6819`) | Phrase grep OK + `dpkg -l` factuality probe (`ii`/`ii`/`un`); old false claim removed in the same hunk | ✅ |
| R5 | `.github/workflows/docker.yaml` timeouts 20→30 (P2) + durations | Reviewer+orchestrator: ruby parse `30/30/15`, diff-hunk check, cold-compile comments; local 3m6s in P1 commit body; GitHub-runner durations owed by the PR `validate` legs and quoted into the PR body at publish (orchestrator) | ✅ half on branch / half owed by CI |
| R6 | Post-merge republish (P4, pending by design) | Orchestrator: not executable on the branch; handoff is the P4 runbook (patch release → `build`+`merge` → dual-arch `command -v xdu-rm` verify) | ⏳ pending post-merge, not a branch gap |

Unmapped changes (possible scope creep): none. Every hunk maps to R1 (g++ layer), R3 (builder
comment), R4 (runtime comment), or R5 (timeouts); the `issues/*.md` one-line `unshaped` →
`shaped as spec/…` flip is lifecycle bookkeeping, not creep.

## Findings

No CONFIRMED or PLAUSIBLE findings. The diff is clean — silence is the valid result.

Refuted-and-dropped candidates (proof of work, not findings): `g++` fails to provide `c++`
(disproven via alternatives output); slim already ships `c++` (disproven — `c++: MISSING`);
R4 comment factually wrong (disproven via `dpkg -l`); `.dockerignore` breaks the build
(disproven — ignored paths intersect no `COPY` source); build-vs-install binary list mismatch
(disproven — four names agree); `rm -rf` vs `del` rule (not a violation — `AGENTS.md` exempts
container builds); `ENTRYPOINT` blocking multi-binary use (pre-existing line, untouched —
grading it would be manufacturing a gap); `timeout-minutes: 30` sufficiency (speculative
without CI timings — dropped per rubric).

No invariant violation: §1–§12 untouched (zero `src/` bytes in the diff); §13 clean (no spec
ids in source, no version hardcoding, no `share/` commits, prose voice not graded as a finding).

## Human-gate triggers

- None. No CONFIRMED finding touches the high-blast-radius core (`src/bin/xdu-rm.rs`,
  `src/bin/xdu.rs`, `src/crawl.rs`, `src/lib.rs`, `src/cli.rs`) or a destructive-rm /
  schema-stability / atomic-write / SQL-injection invariant — the diff touches none of those
  files. No sign-off gate is triggered; recommended next step is `/xdu-publish` (which owns the
  R5-duration quoting and the P4 post-merge handoff).

## Optional completeness sub-pass (separate reviewer; may see TECH.md)

- Not run — `completeness` was not requested. Orchestrator notes: P1–P3 done with green phase
  verifies recorded in commit bodies; P4 pending by design (not a ship gap); appetite small
  honored (no scope balloon — non-goals respected, no toolchain superset, no digest pinning,
  no Dockerfile deletion).
