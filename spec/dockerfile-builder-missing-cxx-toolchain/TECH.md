---
slug: dockerfile-builder-missing-cxx-toolchain
title: Restore the container build with an explicit C++ toolchain
kind: fix
appetite: small
status: in_progress
branch: fix/dockerfile-builder-missing-cxx-toolchain
base: main
current_phase: P4
last_updated: '2026-09-06'
phases:
- id: P1
  name: Builder g++ layer with the c++ requirement stated
  status: done
  satisfies:
  - R1
  - R3
  depends_on: []
  parallel: false
  hammerable: false
  hill: uphill
  verify: docker build -t xdu:cxx-verify . && docker run --rm --entrypoint sh xdu:cxx-verify
    -c 'for b in xdu xdu-find xdu-view xdu-rm; do command -v /usr/local/bin/$b; done'
    && grep -q 'literally named' Dockerfile
- id: P2
  name: Runtime comment correction and CI timeout headroom
  status: done
  satisfies:
  - R4
  - R5
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: uphill
  attempts: 1
  verify: 'uv run --with pyyaml python -c ''import yaml; yaml.safe_load(open(".github/workflows/docker.yaml"))''
    && test $(grep -c ''timeout-minutes: 30'' .github/workflows/docker.yaml) = 2 &&
    grep -q ''effectively installs only'' Dockerfile'
- id: P3
  name: Offline functional drive of the built image
  status: done
  satisfies:
  - R2
  depends_on:
  - P1
  parallel: false
  hammerable: false
  hill: uphill
  verify: FIXT=$(mktemp -d) && mkdir -p $FIXT/proj && echo hi > $FIXT/proj/a.txt &&
    echo lo > $FIXT/top.txt && chmod -R a+rX $FIXT && docker run --rm --network none
    -v $FIXT:/data:ro --entrypoint sh xdu:cxx-verify -c 'set -e; [ $(whoami) = xdu
    ]; /usr/local/bin/xdu /data -o /tmp/idx; test -d /tmp/idx/__root__; test -f /tmp/idx/.xdu-complete;
    [ $(/usr/local/bin/xdu-find -i /tmp/idx --count) = 2 ]; test ! -e /home/xdu/.duckdb';
    rc=$?; rm -rf $FIXT; exit $rc
- id: P4
  name: Post-merge republish verification (human-gated)
  status: pending
  satisfies:
  - R6
  depends_on:
  - P2
  - P3
  parallel: false
  hammerable: false
  hill: uphill
  verify: docker run --rm --platform linux/amd64 --entrypoint sh ghcr.io/glentner/xdu:latest
    -c 'command -v /usr/local/bin/xdu-rm' && docker run --rm --platform linux/arm64
    --entrypoint sh ghcr.io/glentner/xdu:latest -c 'command -v /usr/local/bin/xdu-rm'
review:
  last_reviewed_commit: 7737fc8a4a2a87341d747342b0104b1cdccc8930
  verdict: approved
  blocked_reason: ''
  cycle: 1
---
# TECH.md — Restore the container build with an explicit C++ toolchain

The **context engine and finite-state machine** for building this feature. The YAML
frontmatter above is the resume ground-truth (read it with
`uv run --with pyyaml python .agents/factory/bin/next_phase.py spec/{slug}/TECH.md`); the per-phase
checklists below are the work. `xdu-build` executes the next actionable phase, runs its
`verify:` command, updates state via
`uv run --with pyyaml python .agents/factory/bin/set_phase.py …`, and makes one atomic code+state commit.

- **Vision / requirements (locked):** [`GOAL.md`](GOAL.md) — R-IDs are the contract.
- **Authoritative design:** [`PLAN.md`](PLAN.md).
- **Backing research:** none — lean path (`kind: fix`, reproduced root cause, proven fix).

## Frontmatter field reference

- `status` (top): `planned | in_progress | blocked | in_review | done` (`done` is stamped by
  `xdu-publish` after confirmation, just before landing — the terminal state of the retained record)
- `appetite`: `small | big` — caps phase count and build-iteration budget (circuit breaker).
- phase `status`: `pending | in_progress | done | blocked`
- `satisfies`: GOAL R-IDs this phase delivers (traceability anchor for `xdu-review`).
  **Artifact-deliverable R-IDs.** Some requirements are met by a *committed document* — a research
  audit, a protocol doc, an assessment — not by CLI-observable behaviour. Attach such an R-ID to the
  phase that produces and commits the artifact, and give that phase a content `verify:`
  (`test -f …`, a `grep -q` for the sections that must exist) rather than forcing a CLI drive.
  **Say so in the phase body**, because it changes who can grade it: `xdu-review` blinds its reviewer
  to all of `spec/`, so an R-ID whose evidence lives there is graded by the *orchestrator*, not the
  blind reviewer. Naming it in `TECH.md` is what lets the review skill route it correctly instead of
  reporting an unverifiable requirement.
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
  with the Claude Code generation line.
- A CLI/feature change updates the affected `doc/*.scd` man page **in the same commit** (shell
  completions regenerate from `src/cli.rs`; the generated `share/` tree is git-ignored, not committed).
- This fix touches no Rust code, so `cargo test` / clippy / fmt are unaffected and are not phase
  gates; the standard pre-release gate still applies at publish.

---

## Phase P1 — Builder g++ layer with the c++ requirement stated
**Satisfies:** R1, R3 · **Depends on:** —
**Goal:** The Dockerfile builder stage installs `g++` with a comment that states the literal-`c++`
requirement at the point of the install, and the image builds green with all four binaries.

- [x] Insert the `RUN` layer immediately after `FROM rust:1-slim-bookworm AS builder`
  (`Dockerfile:21`), before the `COPY`s so it caches independently of source churn. Exact text:
  `# The duckdb crate's \`bundled\` feature compiles DuckDB from C++ source; cc-rs invokes a tool`
  / `# literally named \`c++\`, which rust:1-slim-bookworm does not ship (it has cc/gcc only).`
  followed by the `apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf
  /var/lib/apt/lists/*` layer. `rm` stays `rm` — `AGENTS.md` exempts container builds. No R-IDs
  in the comment (§13).
- [x] Run the phase `verify:` — a full local `docker build` (~5 min on Apple Silicon; expect
  `Finished release profile` then the four `command -v` paths). Record the wall-clock build
  duration in the commit body; it is the local half of R5's evidence.
- **Verify:** `docker build -t xdu:cxx-verify . && docker run --rm --entrypoint sh xdu:cxx-verify
  -c 'for b in xdu xdu-find xdu-view xdu-rm; do command -v /usr/local/bin/$b; done' && grep -q
  'literally named' Dockerfile`.
- **Touches:** `Dockerfile`.

## Phase P2 — Runtime comment correction and CI timeout headroom
**Satisfies:** R4, R5 · **Depends on:** P1
**Goal:** The runtime-stage comment describes what bookworm-slim actually provides, and both
build jobs get timeout headroom for the unmeasured cold DuckDB compile.

- [x] Rewrite the `Dockerfile:52-53` comment to state that bookworm-slim already ships
  `libstdc++6` and `libgcc-s1`, so the `RUN` effectively installs only `ca-certificates`, while
  keeping `libstdc++6` in the install list to pin the runtime contract the DuckDB-linked
  binaries need. The comment MUST contain the phrase `effectively installs only` (the verify
  greps for it).
- [x] Raise `timeout-minutes: 20` to `30` on the `validate` job (`docker.yaml:54`) and the
  `build` job (`docker.yaml:86`), each with a one-line comment citing the cold bundled-DuckDB
  compile. Leave `merge` at 15 — it assembles a manifest list, it compiles nothing. Do not
  assert headroom comfort in the comment; the estimate is ~14–16 min and the first real
  duration comes from the PR's `validate` runs.
- [x] R5's CI half cannot close on the branch: note in the commit body that the GitHub-runner
  durations are read from the PR's `validate` legs and quoted into the PR body at publish.
- **Verify:** `uv run --with pyyaml python -c 'import yaml;
  yaml.safe_load(open(".github/workflows/docker.yaml"))' && test $(grep -c 'timeout-minutes:
  30' .github/workflows/docker.yaml) = 2 && grep -q 'effectively installs only' Dockerfile`.
- **Touches:** `Dockerfile`, `.github/workflows/docker.yaml`.

## Phase P3 — Offline functional drive of the built image
**Satisfies:** R2 · **Depends on:** P1
**Goal:** Prove the built image is functionally an xdu distribution with no network, as the
non-root user, and with no DuckDB extension autoinstall.

- [x] Run the phase `verify:` against the `xdu:cxx-verify` image from P1 (rebuild first if the
  image is absent — the tag is local-only and never pushed). It mounts a two-file fixture tree
  read-only, crawls it, and asserts `whoami = xdu`, the `__root__` partition and
  `.xdu-complete` exist, `xdu-find --count` prints exactly `2` (bare integer per
  `src/bin/xdu-find.rs:86`), and `/home/xdu/.duckdb` was not created. `chmod -R a+rX` on the
  fixture precedes the run because `mktemp -d` is `700` and the container `xdu` user is a
  different uid; `rm -rf` on the self-created scratch dir is allowed per `AGENTS.md`.
- [x] If the amd64-identity of the local image matters to the reader, note the host arch in the
  commit body — the second arch is proven by the PR's `validate` legs, not here.
- **Verify:** the `mktemp` + `docker run --network none` one-liner in the frontmatter (exit code
  is the verdict; the ` trap`-less `rm -rf` cleanup runs on both paths via `rc=$?`).
- **Touches:** nothing committed except `TECH.md` state (verification-only phase).

## Phase P4 — Post-merge republish verification (human-gated)
**Satisfies:** R6 · **Depends on:** P2, P3
**Goal:** After merge, a patch release republishes `ghcr.io/glentner/xdu:latest` with all four
binaries, and both arches prove it. **This phase is not executable on the branch.**

- [ ] `xdu-build` MUST NOT attempt this phase: cutting a release is irreversible and outward
  (`/xdu-release`, human-gated). Leave P4 `pending`, stop the roadmap after P3, and report the
  handoff. Attempting the verify before the release exists proves nothing and fails closed.
- [ ] Human runbook (post-merge): cut a patch release via `/xdu-release`; watch the release's
  `docker.yaml` `build` + `merge` jobs — this is the first-ever execution of the publish half,
  so a green `validate` on the PR predicts nothing about it; then run the phase `verify:`.
- [ ] On a green verify the human flips P4 via `set_phase.py --phase P4 --status done
  --touch` in a follow-up commit on `main` (or records the digest in the release notes if the
  FSM is already landed). On a red publish, the failure is live firefighting, not a deferral.
- [ ] Deferral ledger (this phase owns it): walk the P1–P3 bodies for "do not fix here",
  "known limitation", "left as a follow-up" or equivalent. Each MUST name
  `issues/{slug}.md` + a `ROADMAP.md` entry inline, or the phase fails. Standing inventory to
  confirm rather than duplicate: the failure-visibility canary lives at
  `issues/ci-gates-are-advisory.md` + `ROADMAP.md:340` (referenced, owned there); the
  `CARGO_BUILD_JOBS` cap is a contingency that activates only on a CI OOM signature, not a
  deferral; stray ghcr tags and digest pinning stay open questions in the seed record until the
  maintainer files them. An unrecorded deferral is a phase failure, not a tidy-up.
- **Verify:** `docker run --rm --platform linux/amd64 --entrypoint sh
  ghcr.io/glentner/xdu:latest -c 'command -v /usr/local/bin/xdu-rm' && docker run --rm
  --platform linux/arm64 --entrypoint sh ghcr.io/glentner/xdu:latest -c 'command -v
  /usr/local/bin/xdu-rm'`.
- **Touches:** `TECH.md` state only (post-merge).

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
