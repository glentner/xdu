# GOAL — Restore the container build with an explicit C++ toolchain

> **Origin spec.** The *what* and *why* — the locked contract `xdu-review` grades against.
> The *how* lives in [`PLAN.md`](PLAN.md) and [`TECH.md`](TECH.md) (written by `xdu-plan`).
> Promoted from [`issues/dockerfile-builder-missing-cxx-toolchain.md`](../../issues/dockerfile-builder-missing-cxx-toolchain.md),
> which remains the pre-shaped evidence record. Do not treat the seed's draft R-IDs or open
> questions as settled — this file is the contract.

- **slug:** dockerfile-builder-missing-cxx-toolchain
- **kind:** fix
- **appetite:** small

## Problem

The container builder stage (`Dockerfile`, `FROM rust:1-slim-bookworm`) installs nothing, and
that image ships `cc`/`gcc` but no tool literally named `c++`. The `duckdb` crate's `bundled`
feature compiles DuckDB from C++ source through cc-rs, which invokes `c++` — so every container
build aborts with `ToolNotFound`, exit 101. The requirement was known and solved under the
previous Alpine base (it installed `g++` explicitly); the January base-image swap to slim threw
the solution away, and a later hardening pass reasoned about C++ *runtime* linkage while walking
past the builder stage that has no C++ compiler. No image has built since January.

What is published today is worse than "no new image": `ghcr.io/glentner/xdu:latest` is a January
v0.2.1 musl image that predates `xdu-rm`, so no published image has ever contained all four
binaries. Blast radius stops at the container — host builds, release tarballs and `install.sh`
are unaffected — and the container is documented nowhere outside the Dockerfile's own header.

## Outcome / vision

`docker build` succeeds on both arches from a clean checkout, the PR guardrail goes green, and
what `ghcr.io/glentner/xdu` publishes is an image of the current release containing all four
binaries, including `xdu-rm`. The C++ toolchain requirement is stated in the Dockerfile at the
point of the install, so the next base-image change cannot silently delete it again.

## Acceptance criteria (the contract)

- **R1** — WHEN `docker build` is run against the committed `Dockerfile` on `linux/amd64` or
  `linux/arm64`, the build SHALL exit 0 and the resulting image SHALL contain all four binaries
  (`xdu`, `xdu-find`, `xdu-view`, `xdu-rm`).
- **R2** — WHILE the container has no network, the image SHALL index a tree, write the `__root__`
  partition and `.xdu-complete`, answer `xdu-find --count` correctly as the non-root `xdu` user,
  and create no DuckDB extension cache under `$HOME`.
- **R3** — The `Dockerfile` SHALL state, at the point of the install, that the `bundled` DuckDB
  build needs a compiler named `c++` which the `rust:*-slim` images do not ship.
- **R4** — The `Dockerfile`'s runtime-stage comment SHALL describe what `debian:bookworm-slim`
  actually provides: it already ships `libstdc++6` and `libgcc-s1`, so that `RUN` effectively
  installs only `ca-certificates`.
- **R5** — WHEN the container build runs on a GitHub-hosted runner from a cold cache, the job
  SHALL complete within its `timeout-minutes`, and the observed duration SHALL be recorded.
- **R6** — The tag `ghcr.io/glentner/xdu:latest` SHALL NOT resolve to an image that lacks `xdu-rm`,
  satisfied either by republishing from a current release or by withdrawing the stale tags.

## Non-goals (no-gos)

- Failure-visibility CI topology (a scheduled build-only signal, or coupling release success to
  the image build). Owned by [`issues/ci-gates-are-advisory.md`](../../issues/ci-gates-are-advisory.md)
  R4; referenced here, not shaped twice.
- Digest-pinning the two `FROM` lines. A pin defends against base-image drift, a different failure
  that did not occur here; worth its own issue.
- Toolchain supersets or alternatives (`build-essential`, `clang`, non-slim base). The `g++`-only
  build is proven end to end; the alternatives add weight or an untested compiler for no gain.
- Deleting the Dockerfile, `.dockerignore` and `docker.yaml`. That answers a different question and
  leaves the stale live image untouched either way.
- The `build.sh` output listing missing `xdu-rm`, and the Ubuntu man-page literal defect. Both are
  pre-existing, unrelated, and tracked separately.

## Clarifications

- **Q:** Does this fix own the republish-or-withdraw registry action, or only the Dockerfile/CI so
  future releases publish correctly? — **A:** Unresolved at shaping (scope question lost to a
  session restart). R6 contracts the outcome only; the mechanism is a maintainer decision deferred
  to plan/release. Flagged for sign-off review below.
- **Q:** Who owns the build-failure canary the seed hedged as R6? — **A:** `ci-gates-are-advisory`
  R4 owns it, per the seed's own recommendation; this GOAL carries no duplicate criterion
  (resolved at shaping 2026-09-03).
- **Q:** Open evidence gaps from the seed (amd64 leg unverified, cold post-fix duration on GitHub
  hardware an estimate, the publish half of `docker.yaml` never executed, the January first-failure
  link resting on inference)? — **A:** None are assumed; `/xdu-plan` closes what CI can prove and
  records what it cannot (noted at shaping 2026-09-03).

## Related materials

- Seed: [`issues/dockerfile-builder-missing-cxx-toolchain.md`](../../issues/dockerfile-builder-missing-cxx-toolchain.md)
- Canary owner: [`issues/ci-gates-are-advisory.md`](../../issues/ci-gates-are-advisory.md)
- `Dockerfile` (builder stage, runtime-stage comment), `.github/workflows/docker.yaml` (guardrail,
  timeouts, cache scopes, concurrency group), `Cargo.toml:19` (`bundled` feature),
  `rust-toolchain.toml` (pinned toolchain vs floating `rust:1` tag)
