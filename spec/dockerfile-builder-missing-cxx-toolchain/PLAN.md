# PLAN — Restore the container build with an explicit C++ toolchain

> **Status:** Draft for review · **Last updated:** 2026-09-06
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). No `research/` — `kind: fix` with a
> reproduced root cause takes the lean path. Every design element traces to a GOAL R-ID.

## 1. Summary

Add one `g++` install layer to the Dockerfile builder stage with a comment that names the
`c++` requirement at the point of the install, correct the runtime-stage comment that
misdescribes bookworm-slim, and raise both build-job timeouts in `docker.yaml` from 20 to 30
minutes against the unmeasured cold-compile cost. Local `docker build` plus an offline
functional drive proves R1–R4 before the PR; the PR's own `validate` legs prove both arches
and record the cold-runner durations for R5; a patch release cut after merge republishes
`:latest` with all four binaries for R6.

## 2. Design

Three edits, two files, no Rust code. `Dockerfile` and `.github/workflows/docker.yaml` are the
entire blast radius; `src/`, `Cargo.toml`, `doc/*.scd` and `install.sh` are untouched.

**Builder toolchain (R1, R3).** Immediately after `FROM rust:1-slim-bookworm AS builder`
(`Dockerfile:21`), before the `COPY`s so the layer caches independently of source churn:

```dockerfile
# The duckdb crate's `bundled` feature compiles DuckDB from C++ source; cc-rs invokes a tool
# literally named `c++`, which rust:1-slim-bookworm does not ship (it has cc/gcc only).
RUN apt-get update && \
    apt-get install -y --no-install-recommends g++ && \
    rm -rf /var/lib/apt/lists/*
```

`g++` alone is the complete fix — no make, cmake, or pkg-config — proven by three end-to-end
linux/arm64 builds in the seed record, the strictest with rewritten cache-mount ids so no warm
cache could mask a partial C++ build. Placement before the `COPY`s matters: the builder `RUN`'s
own layer cache is invalidated by four of the five `paths:` triggers, and putting the toolchain
after the `COPY`s would re-run `apt-get` on every source change instead of caching it. `rm -rf`
stays `rm`: `AGENTS.md` exempts container builds explicitly, and layer size is the point. The
comment carries no R-ID or spec reference — `invariants.md` §13 forbids embedding them in
committed files.

**Runtime comment correction (R4).** `Dockerfile:52-53` claims bookworm-slim ships "neither
libstdc++6", but the image carries `libstdc++6` and `libgcc-s1` as `install ok installed` and
CI's own log confirms `libstdc++6 is already the newest version`. Rewrite the comment to say
what the `RUN` actually does — installs `ca-certificates` only, while pinning the
`libstdc++6`/`libgcc-s1` runtime contract against a future slim that drops them. Keep
`libstdc++6` in the install list: it documents the contract the three DuckDB-linked binaries
need (`ldd` shows `libstdc++.so.6`, `libgcc_s.so.1`, `libm.so.6`, `libc.so.6`), and the package
is a no-op while the base provides it.

**Timeouts (R5).** Raise `timeout-minutes: 20` to `30` on the `validate` job
(`docker.yaml:54`) and the `build` job (`docker.yaml:86`), with a one-line comment citing the
cold bundled-DuckDB compile. Both legs currently die at 83–100 s, so the green path has never
been timed on GitHub hardware; the nearest datapoints (release builds at 13m09s–13m34s with a
warm cargo cache the container build lacks) put the estimate at ~14–16 min against the current
cap. A job bills only what it uses, so the raise buys headroom without cost. The `merge` job
stays at 15 — it assembles a manifest list, it compiles nothing. Do not claim comfortable
headroom: the first real duration is read from the PR's `validate` runs, not extrapolated.

**Republish (R6).** Fixing the Dockerfile republishes nothing — a branch `workflow_dispatch`
publishes only `sha-<short>` under `latest=auto`, so `:latest` stays the January v0.2.1 musl
image until a tagged release. After merge the maintainer cuts a patch release through
`/xdu-release`; the release triggers `docker.yaml`'s `build` → `merge` path, which publishes
the full semver set plus `:latest` and signs it. R6 is then verified by pulling `:latest` on
both arches and asserting `xdu-rm` is present. The stale-tag pruning question (`0.0.0-test`,
bare `0`/`0.0`) stays open and out of scope.

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1 | Builder `g++` layer; verified by local `docker build` exit 0 + four binaries in `/usr/local/bin`, and by the PR's `validate` legs on both arches |
| R2 | Offline functional drive of the built image: crawl, `__root__` + `.xdu-complete`, `xdu-find --count`, no `~/.duckdb`, all as `USER xdu` under `--network none` |
| R3 | Builder-stage comment naming the literal-`c++` requirement and the slim images' gap, at the install |
| R4 | Corrected runtime-stage comment describing what bookworm-slim provides and what the `RUN` installs |
| R5 | `timeout-minutes: 30` on `validate` + `build`; durations recorded from the PR's CI legs (local build duration recorded in the phase commit) |
| R6 | Post-merge patch release republishes `:latest`; verified by pulling it per-arch and asserting `xdu-rm` |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) (§1–§13)
**before** research and **again** after this design was drafted.

- §1–§12 — untouched. No schema, finalization, marker, partition-scheme, `xdu-rm`, SQL,
  portability, concurrency, symlink, sort-order, CLI, altitude, or TUI change. R2 drives the
  real binaries inside the container but modifies none of them.
- §13 (project conventions) — honored. The new apt layer keeps `rm -rf` per the explicit
  container-build exemption in `AGENTS.md`; Dockerfile comments carry no `R#`/`P#` ids; comment
  voice follows the declarative, why-not-what style; the runtime stage's four-binary copy
  continues to match the release tarball contract. No `.scd`, `share/`, or version-string
  involvement.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| — | — | — |

## 4. Rabbit holes (resolved)

Lean path — no fan-out, no `research/`. The root cause arrived reproduced (byte-identical
`ToolNotFound`, exit 101, at the tracked Dockerfile) and the fix arrived proven end to end
(three green arm64 image builds plus an offline functional run). The remaining unknowns are
not design inputs but CI-observable facts, listed under §5.

## 5. Risks & open questions

- **The amd64 leg is unverified.** Every successful local build was native linux/arm64; the
  package set and CI error text match across arches, but only the PR's `validate` legs prove
  it. Mitigation: R1's CI half is the proof; a red amd64 leg blocks merge by the guardrail's
  design.
- **Cold post-fix duration on GitHub hardware is an estimate (~14–16 min), not a measurement,**
  and the `ubuntu-24.04-arm` leg has no datapoints at all. Mitigation: the 30-minute timeouts;
  the first real durations are read from the PR runs and recorded before merge.
- **The publish half of `docker.yaml` has never executed.** The `build` → `merge` /
  `imagetools create` / attestation / keyless-cosign jobs added by the July hardening pass
  have never run once; a green `validate` proves nothing about them. Mitigation: none
  available short of a release — R6's patch release is the first execution, and its publish
  jobs are watched, not assumed.
- **Memory, not cores, is what can break the CI build.** An unbounded-parallelism release
  build of `libduckdb-sys` was OOM-killed in a 16 GB Docker VM; `CARGO_BUILD_JOBS=4` fixed it
  locally, and more cores buy nothing (the DuckDB amalgamation is one serial translation
  unit). If a `validate` leg dies with `cc1plus` killed rather than a timeout, cap build jobs
  in the Dockerfile `RUN` — not a bigger runner. Left as a contingency, not a phase, because
  no CI datapoint yet calls for it.
- **R6's phase is human-gated and post-merge.** Cutting a release is `/xdu-release` territory
  (irreversible, outward) and cannot run on the branch. `TECH.md` P4 records the runbook and
  its verify; `xdu-build` stops there and the maintainer executes it after merge.
- **R5's CI half closes at PR time.** The per-phase commit records the local build duration;
  the GitHub-runner durations are quoted from the PR's `validate` runs into the PR body at
  publish. Review confirms both numbers are on record before merge.

## 6. Verification strategy

Drive the real artifact, in layers of increasing fidelity. Local `docker build` proves the fix
on the developer's arch; the offline run proves the image is functionally an xdu distribution
(R2 doubles as the air-gap check: `--network none`, non-root user, no extension cache); the
PR's `validate` legs prove the second arch and supply the R5 durations; the post-release
`:latest` pull proves R6. No Rust unit or integration tests change — `cargo test` is
unaffected by this fix and is not a phase gate here, though the standard pre-release gate
still applies at publish. Each `TECH.md` `verify:` is the exact command, not a checkbox.
