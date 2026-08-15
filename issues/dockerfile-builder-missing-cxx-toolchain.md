---
status: unshaped
kind: fix
appetite: small
---

# The container builder stage has no C++ compiler, so no image has built since January

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

`Dockerfile:21` starts the builder stage at `FROM rust:1-slim-bookworm AS builder` and installs
nothing. That image ships `cc`, `gcc`, `ld` and `ar` but **no `c++`** (also no `g++`, `clang`, `make`,
`cmake`, `pkg-config`) — verified on both arches:

```
$ docker run --rm --platform linux/arm64 rust:1-slim-bookworm sh -c 'for t in cc gcc c++ g++ make cmake pkg-config ld ar; do ...'
cc /usr/bin/cc · gcc /usr/bin/gcc · ld /usr/bin/ld · ar /usr/bin/ar
c++ MISSING · g++ MISSING · make MISSING · cmake MISSING · pkg-config MISSING
# same result under --platform linux/amd64
```

`duckdb`'s `bundled` feature (`Cargo.toml:19`) makes `libduckdb-sys`'s build script compile DuckDB
from C++ source through cc-rs with `.cpp(true)` (`libduckdb-sys-1.4.4/build.rs:151`), and cc-rs
resolves the C++ driver by the literal tool name `c++`. So the `cargo build` at `Dockerfile:36-45`
aborts, which is exactly what CI reports:

```
warning: libduckdb-sys@1.4.4: Compiler family detection failed due to error:
  ToolNotFound: failed to find tool "c++": No such file or directory (os error 2)
error: failed to run custom build command for `libduckdb-sys v1.4.4`
ERROR: ... did not complete successfully: exit code: 101
```

Reproduced locally against the unmodified tracked Dockerfile at `89a5178`, byte-identical error and
exit code, in 34–46 s.

**The breaking change is a single deleted line, not drift and not a dependency bump.**
`git diff 4c29b07 766cf16 -- Dockerfile` ("Revert back to GNU/Linux builds instead of MUSL",
2026-01-20 15:55:45 -0500):

```
-FROM rust:alpine AS builder
-
-# Install build dependencies (Alpine uses musl natively)
-RUN apk add --no-cache musl-dev gcc g++
+FROM rust:slim AS builder
```

`rust:alpine` does not ship `g++` either, which is why the first Dockerfile installed it explicitly.
The requirement was known and solved; the base-image swap threw the solution away. `bundled` was not
a later arrival — it landed 2026-01-16 (`e751c62`), one day *before* the Dockerfile existed
(`4c29b07`, 2026-01-17). The July hardening pass (`445aa5b`) re-templated the file and reasoned
explicitly about C++ *runtime* linkage — it added `libstdc++6` to the runtime stage with a comment —
and walked straight past the builder stage that has no C++ compiler.

**What is actually published today is worse than "no new image."** Anonymous ghcr pull, re-verified
for this record:

```
$ curl ghcr.io/v2/glentner/xdu/tags/list
{"tags":["latest","0.0.0-test","0.0","0","0.2.0","0.2","0.2.2","0.2.1"]}

:latest = :0 = :0.2 = :0.2.1 = sha256:fd7ea111a0bd8cac0d23fe10c318c82da99c7f59121e88db63252153fc50769e
amd64 child config: created 2026-01-18T05:07:24Z, revision d0a7400  (git tag --points-at d0a7400 -> v0.2.1)
history: COPY xdu / xdu-find / xdu-view  ->  / and /usr/local/bin/   (six layers, FROM scratch)
```

Because it predates `766cf16`, `:latest` is still the `FROM scratch` musl image: **`xdu-rm` is absent
entirely** and there is no `/bin/sh`. Tag `0.2.2` is a different digest of the same shape, so *no*
published image has ever contained `xdu-rm`. Current `Cargo.toml` is `0.4.1`; no `0.3.x` or `0.4.x`
tag exists in the registry. (Note the per-arch split: the amd64 child config says
`created 2026-01-18T05:07:24Z`, the arm64 child `08:26:52Z` — same index, not a contradiction.)

**Why the rot survived seven months and three releases** — three independent gaps, none of which is a
broken guardrail:

- The `validate` guardrail job did not exist in January; `445aa5b` added it 2026-07-24 and was pushed
  straight to `main` with no PR (`gh api /repos/glentner/xdu/commits/445aa5b/pulls` returns empty), so
  it never validated itself. PRs #6–#9 touched none of its `paths:` filters
  (`.github/workflows/docker.yaml:26-32`). **PR #10 is the first `pull_request` run of `docker.yaml`
  in the repository's history — the guardrail fired correctly the very first time it was allowed to.**
- `release.yaml` and `docker.yaml` are independent workflows both triggered on `release: published`
  with no `needs:` between them, so a red image never fails or blocks a release.
- `docker.yaml` has no `schedule:` canary.

Blast radius is bounded and does **not** extend beyond the container: on the same commit, Tests
`ubuntu-24.04` and `macos-14` both passed (the only failing Tests job is the unrelated man-page
defect); `release.yaml`'s aarch64 cross leg explicitly installs `g++-aarch64-linux-gnu` (line 131) and
v0.3.0/v0.4.0/v0.4.1 all published four tarballs; `install.sh` downloads prebuilt tarballs and
`build.sh` is a plain host `cargo build`. The Dockerfile is the only minimal build environment in the
repo. Mitigating the user-facing harm somewhat: the container is documented **nowhere** — no reference
in `README.md`, `doc/*.scd` or `install.sh`, only the Dockerfile's own header comment
(`Dockerfile:9-10`) advertising `COPY --from=ghcr.io/glentner/xdu:latest`.

## Why it was deferred

**Pre-existing in `main`** since 2026-01-20, and untouched by the branch that surfaced it. PR #10
(`readers-autoload-parquet-at-runtime`) changed `Cargo.toml`, which is what pulled `docker.yaml` into
its `paths:` filter for the first time; the failure it exposed has nothing to do with the `parquet`
feature. That feature adds **no** new build-tool requirement (`libduckdb-sys`'s `add_extension()`
feeds more `.cpp` files into the same `cc::Build`) and no new runtime shared-object dependency —
verified by `ldd` and an offline in-container query, below.

Deferred because the PR it fell out of was a scoped fix to the DuckDB Parquet linkage, and this is
container/CI-packaging work with an attached publishing decision (what to do about the live stale
image) that only the maintainer can make.

## Outcome / vision

`docker build` succeeds on both arches from a clean checkout, the PR guardrail goes green, and what is
published at `ghcr.io/glentner/xdu` is an image of the current release containing all four binaries —
including `xdu-rm`, which no published image has ever had. The C++ toolchain requirement is stated
in the Dockerfile so the next base-image change cannot silently delete it again.

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion.

- **R1** — WHEN `docker build` is run against the committed `Dockerfile` on `linux/amd64` or
  `linux/arm64`, the build SHALL exit 0 and the resulting image SHALL contain all four binaries
  (`xdu`, `xdu-find`, `xdu-view`, `xdu-rm`).
- **R2** — WHILE the container has no network (`docker run --network none`), the image SHALL index a
  tree, write the `__root__` partition and `.xdu-complete`, answer `xdu-find --count` correctly as the
  non-root `xdu` user, and create no DuckDB extension cache under `$HOME`.
- **R3** — The `Dockerfile` SHALL state, at the point of the install, that the `bundled` DuckDB build
  needs a compiler named `c++` which the `rust:*-slim` images do not ship.
- **R4** — The `Dockerfile`'s runtime-stage comment SHALL describe what `debian:bookworm-slim`
  actually provides (see Notes: it already ships `libstdc++6` and `libgcc-s1`; that `RUN` effectively
  installs only `ca-certificates`).
- **R5** — WHEN the container build runs on a GitHub-hosted runner from a cold cache, the job SHALL
  complete within its `timeout-minutes`, and the observed duration SHALL be recorded (see the
  unmeasured-cost caveat in Notes).
- **R6** — IF the container image fails to build, THEN that failure SHALL become visible without
  waiting for a PR that happens to touch `Cargo.toml` — e.g. a scheduled build-only run, or a release
  that declines to complete quietly. (**Overlaps** [`issues/ci-gates-are-advisory.md`](ci-gates-are-advisory.md)
  R4, which states the same requirement as CI topology. One of the two must own it at promotion —
  probably R4 — and the other reference it; do not shape it twice.)
- **R7** — The tag `ghcr.io/glentner/xdu:latest` SHALL NOT resolve to an image that lacks `xdu-rm`
  (satisfied either by republishing from a current release or by withdrawing the stale tags).

## Notes

**Cheapest shape — one apt layer, proven end to end.** Immediately after `Dockerfile:21`, before the
`COPY`s so it caches independently of source churn:

```dockerfile
# The duckdb crate's `bundled` feature compiles DuckDB from C++ source; cc-rs invokes a tool
# literally named `c++`, which rust:1-slim-bookworm does not ship (it has cc/gcc only).
RUN apt-get update && \
    apt-get install -y --no-install-recommends g++ && \
    rm -rf /var/lib/apt/lists/*
```

`rm -rf` stays `rm` here — `AGENTS.md` exempts container builds explicitly, and layer size is the
whole point.

`g++` alone is the **complete** fix: no cmake, make, git or pkg-config. Three independent runs on
linux/arm64 built the full image (`Finished release profile in 4m31s` / `5m11s` / `5m17s`), and the
strictest of them rewrote both `--mount=type=cache` ids to unique values so no warm cache from an
earlier attempt could mask a partially-cached C++ build. `ldd` inside the result shows only
`libstdc++.so.6`, `libgcc_s.so.1`, `libm.so.6`, `libc.so.6` for the three DuckDB-linked binaries, and
a `--network none` run crawled, wrote `.xdu-complete` + `__root__`, answered `--count` and a regex
query as the non-root `xdu` user, and created no `~/.duckdb`.

**Two costs that are real and one that is not.**

- *Not real:* the shipped image does not grow. The builder stage is discarded wholesale; `docker
  history` on the fixed image shows no `g++` layer. Measured builder-stage deltas (arm64, single run,
  not independently reproduced) were `g++` +16.7 MB/8 s, `build-essential` +28.7 MB/16 s, `clang`
  +92.8 MB/26 s — informative for choosing the package, irrelevant to the artifact.
- *Real and unmeasured:* **cold build time on GitHub hardware.** Both `validate` legs currently die at
  83–100 s, so the green path has never been timed there. The nearest runner-hardware datapoints:
  `release.yaml`'s `Build (x86_64-unknown-linux-gnu)` job — a `cargo build --release --locked` of this
  crate on `ubuntu-24.04`, *with* `Swatinem/rust-cache`, which the container build has no equivalent of
  — took 13m09s / 13m34s / 13m13s across v0.4.1 / v0.4.0 / v0.3.0; and PR #10's Test job spent 8m52s
  on `libduckdb-sys` alone at the cheaper `dev` profile. Estimate ~14–16 min against the
  `timeout-minutes: 20` at `docker.yaml:54` and `:86`. **Do not repeat the claim that this has
  comfortable headroom** — the 5m25s figure that claim rested on came from `docker run --cpus 4` on
  Apple Silicon, which caps concurrency, not per-core speed. Cheapest resolution: raise both timeouts
  to 30 (a job bills only what it uses) and read the first real duration.
- *Real:* **memory, not cores, is the thing that can break this.** An unbounded-parallelism release
  build of `libduckdb-sys` in a 16 GB Docker VM was OOM-killed at 4m24s — `c++: fatal error: Killed
  signal terminated program cc1plus` → BuildKit `ResourceExhausted: cannot allocate memory`; the same
  build with `CARGO_BUILD_JOBS=4` finished in 5m18s. So the obvious answer to the timeout risk —
  "use a bigger runner" — is the one that can convert a slow build into an OOM. Also note that more
  cores buy nothing: 4 vCPU took 5m21s versus 5m17s on 14, because the DuckDB amalgamation is one
  serial translation unit.

**The gha cache is not the mitigation it looks like.** `docker.yaml:76-77` and `:138-139` set
`cache-from/cache-to: type=gha`, but that covers the *layer* cache only — BuildKit does not export
`--mount=type=cache` contents through any `--cache-to` backend (documented behaviour, not verified
here), so the cargo registry and target dirs never survive between CI runs. And the builder `RUN`'s
own layer cache is invalidated by four of the five `paths:` triggers, since `COPY Cargo.toml
Cargo.lock ./` and `COPY src ./src` precede it and a Dockerfile edit changes the instruction itself.
Budget for a fully cold DuckDB compile on essentially every triggered run. The Dockerfile's comment
at `:31-34` ("keep the cargo registry and the target directory warm across builds") is true locally
and false in CI.

**Second, harmless defect worth fixing in the same commit.** `Dockerfile:52-53` says "bookworm-slim
ships neither libstdc++6 by default, so install them explicitly." It ships both:
`docker run --rm debian:bookworm-slim dpkg-query -W` reports `libstdc++6 12.2.0-14+deb12u1` and
`libgcc-s1 12.2.0-14+deb12u1` as `install ok installed`, `ca-certificates` as `not-installed`, and
CI's own log says `libstdc++6 is already the newest version (12.2.0-14+deb12u1)`. That `RUN`
effectively installs only `ca-certificates`. It documents the runtime contract, so the wrong version
of it is load-bearing.

**Rejected alternatives.**

- `build-essential` — a strict superset (make, dpkg-dev and their deps) for zero functional gain; the
  `g++`-only build succeeding proves none of it is needed.
- `clang` — does satisfy cc-rs, but it would compile DuckDB with clang-14 while every other
  build path in this project uses gcc, shipping binaries produced by a toolchain no test here
  exercises. No upside.
- Non-slim `rust:1-bookworm` — one line, but a ~1.4 GB builder pull and it makes the C++ requirement
  implicit again, which is precisely the failure mode that survived `445aa5b`.
- Deleting `Dockerfile` + `.dockerignore` + `docker.yaml` — deserves consideration (three releases
  shipped red and nobody noticed), but it does not fix the user-facing harm: the live stale image has
  to be republished or withdrawn either way, and republishing costs five lines. Its best instinct — a
  periodic build-only signal — is R6.
- Digest-pinning the two `FROM` lines, which `Dockerfile:12-16` already contemplates — deliberately
  **out of scope**, and worth its own issue. It would not have prevented this: the compiler install
  was deleted by a repo commit, and a pin defends against base-image drift, a different failure that
  did not occur here. Bundling it also makes the fix commit unreviewable as a fix. Two things to
  carry into that issue: pins go stale without a renovate/dependabot cadence, which is a worse failure
  than this one; and there is a *build-time* argument for pinning beyond supply chain —
  `rust-toolchain.toml` pins `channel = "1.97.1"` and `rust:1-slim-bookworm` currently ships exactly
  `rustc 1.97.1`, so nothing is downloaded today, but the moment the floating `rust:1` tag moves past
  it, every container build additionally rustup-downloads the pinned toolchain plus clippy and rustfmt.

**Open questions — none of these are settled, do not let a later session read them as facts.**

- The amd64 leg of the fix is unverified. Every successful build here was native linux/arm64; the
  package set and the CI error text are identical across arches, but only CI will prove it.
- Cold post-fix duration on GitHub hardware is an estimate (~14–16 min), not a measurement, and the
  `ubuntu-24.04-arm` leg has no datapoints at all on either side.
- **The publish half of `docker.yaml` has never executed.** The last successful publish (2026-01-18)
  was under the previous hand-rolled workflow; the `build` → `merge`/`imagetools create` /
  `attest-build-provenance` / keyless-cosign jobs added by `445aa5b` have never run once. A green
  `validate` proves nothing about them.
- What to do about the live `:latest`. Fixing the Dockerfile republishes nothing; a branch
  `workflow_dispatch` publishes only `sha-<short>` under `latest=auto`, so `:latest` stays a January
  v0.2.1 musl image without `xdu-rm` until a tagged release. Cut a patch release, or delete the stale
  tags — a maintainer decision. Whether that deserves its own issue or is subsumed by the republish is
  also open.
- Whether to prune the stray `0.0.0-test` (and bare `0` / `0.0`) tags from ghcr. Also: no pull counts
  were obtainable — `gh api /users/glentner/packages/container/xdu/versions` returns 403 without
  `read:packages` — so there is no evidence for how many people have actually pulled the stale image.
- The January 2026 first failure was **not** observed: its logs are past retention
  (`gh api .../actions/runs/21186951115/logs` → HTTP 410). The attribution to `766cf16` rests on the
  diff above plus a 72-second commit→run-start correlation (commit 20:55:45 UTC, first failing run
  20:56:57 UTC) plus a verbatim reproduction of the same Dockerfile shape today. Today's failure and
  today's fix are reproduced; the January link is inference.
- Why `:latest`/`:0`/`:0.2` sit on the v0.2.1 digest while `0.2.2` exists separately: the three
  January release runs overlapped (v0.2.0 04:12→07:59Z, v0.2.1 04:27→08:30Z, v0.2.2 04:31→08:22Z, each
  a single ~4-hour QEMU build), and v0.2.1 finished **last** and won the `:latest` push race. This is a
  concurrency race, not tag logic, and the new workflow does not close the class:
  `docker.yaml:38` scopes `concurrency.group` per-ref, so two tags published minutes apart still race —
  the faster native build only shrinks the window from ~4 h to ~15 min. Worth deciding whether that
  belongs here or in its own issue.

**Adjacent, keep out of this fix:** `build.sh:8` and `:44` still enumerate only `xdu`, `xdu-find` and
`xdu-view` — `xdu-rm` is missing from its output listing too. Pre-existing and unrelated.

**Appetite justification (`small`).** The code change is one `RUN` layer, one comment correction and
possibly two `timeout-minutes` edits, proven end to end. What is *not* small is the surrounding
decision surface — republish-or-withdraw, whether to add a canary, digest pinning — which is why R6
and R7 are hedged and the publish path is an open question rather than a criterion.

- Related: `.github/workflows/docker.yaml` (guardrail, timeouts, cache scopes, concurrency group);
  the man-page defect from the same CI run is tracked separately in
  [`issues/manpage-literal-assertion-fails-on-ubuntu.md`](manpage-literal-assertion-fails-on-ubuntu.md).
- Found by: post-merge CI triage of PR #10 (`readers-autoload-parquet-at-runtime`), Tests run
  `31218538139` / Docker run `31218538118`, jobs `92997599332` (amd64) and `92997599519` (arm64).
