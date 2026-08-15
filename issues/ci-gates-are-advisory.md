---
status: unshaped
kind: fix
appetite: small
---

# Every CI gate in this repository is advisory, so a red one lands unnoticed

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and R-IDs get negotiated. Do not copy it verbatim.

## Problem

The repository has good gates — `fmt`, `clippy -D warnings`, the test matrix, the man-page literal
assertion, a Dockerfile build guardrail — and **nothing anywhere enforces any of them.** Not the
branch, not the release workflow, not the factory skills. A gate can go red and stay red for months
without blocking a single action.

`main` is unprotected, with no rulesets and no required checks:

```
$ gh api repos/glentner/xdu/branches/main/protection
{"message":"Branch not protected", … "status":"404"}
$ gh api repos/glentner/xdu/rulesets
[]
$ gh api repos/glentner/xdu/branches/main -q '{protected:.protected,protection:.protection}'
{"protected":false,"protection":{"enabled":false,
 "required_status_checks":{"checks":[],"contexts":[],"enforcement_level":"off"}}}
```

Five independent consequences, each measured:

**1. A pull request merges over failing checks.** PR #10 merged at `2026-08-07T21:26:23Z` with three
`FAILURE` entries in its own rollup and nothing objected:

```
$ gh pr view 10 --json statusCheckRollup -q '.statusCheckRollup[]|select(.conclusion=="FAILURE")|.name'
Validate build (linux/amd64)
Validate build (linux/arm64)
Packaging (man + completions generate)
```

**2. Direct pushes to `main` bypass PR gating entirely** — including the two guardrails at issue here.
`445aa5b` (`[ci] Modernize CI/CD workflows and packaging`, which added the Dockerfile build guardrail)
and `9c579cf` (`[harness] Make CI assert what the man page SAYS…`, which added the literal assertion)
both return empty from `gh api /repos/glentner/xdu/commits/<sha>/pulls`. Ten of the last fifteen
commits on `main` have no PR. This is also in tension with `AGENTS.md`'s own stated process
("**GitHub Flow on `main`** — branch off `main` …, open a **squash** PR back to `main`"), which the
repository does not currently enforce on itself.

**3. A batched push skips per-commit gating, so a gate can be added and never run.** GitHub creates one
workflow run per push, for the tip only. The nine `[harness]` commits above were pushed as a batch:

```
$ for s in 651a3a7 9c579cf f1205a7 5b8baa9; do
    gh api "/repos/glentner/xdu/actions/runs?head_sha=$(git rev-parse $s)" -q .total_count; done
0 0 0 0
```

So `9c579cf`'s man-page gate was **never executed by CI before it landed**, and its first execution —
in a later run — failed. A gate that has never been green is indistinguishable, from the repository's
point of view, from one that has never been run.

**4. A release does not require a working container image.** `Publish Image` was `failure` for
`v0.3.0`, `v0.4.0` and `v0.4.1`; all three releases shipped anyway. `release.yaml` has no `needs:` on
the image build (its two `needs:` are internal), and `docker.yaml` has no `schedule:`, so the only
thing that ever runs it is a release or a PR touching one of its five `paths:` entries. The result is
recorded in [`issues/dockerfile-builder-missing-cxx-toolchain.md`](dockerfile-builder-missing-cxx-toolchain.md):
seven months of rot, and a published `:latest` that is still v0.2.1 and has never contained `xdu-rm`.

**5. The factory's own skills reason about CI from the diff, not from CI.** `/xdu-review` declared the
man-page gate "not triggered — `git diff --stat main...HEAD -- doc/ src/` is empty" while that gate was
red on `main`, and `/xdu-publish`'s pre-flight checks the branch, the review verdict and post-review
code drift but never the check rollup. **That half is factory work and is recorded separately** as
finding **F6** in `spec/readers-autoload-parquet-at-runtime/META.md` — it belongs to the human-gated
`/xdu-harness` lane, not here. It is listed here only because it is the fifth reason a red gate
survives: the last layer that could have caught it also does not look.

## Why it was deferred

Found during post-merge triage of PR #10, which surfaced two long-red gates at once and prompted the
question of why neither had ever blocked anything. It is **pre-existing** and predates both: `main` has
never been protected.

Deferred rather than fixed in that triage for two reasons. First, it is a different *kind* of work
from the two defects it explains — repository configuration and workflow topology, not a code fix — and
folding it into either would have made that file multi-cause and unreviewable as a fix. Second, and
more practically, **enabling required checks today would block every PR immediately**, because
`Packaging` is red on `main` right now. There is a mandatory ordering here, and it is the main thing
this issue has to get right.

## Outcome / vision

A red gate cannot reach `main` quietly. Enforcement matches what `AGENTS.md` already claims the process
is, and the gap between "the gate exists" and "the gate is binding" is closed — so the next seven-month
rot is caught in days, by the repository rather than by a human noticing during unrelated triage.

## Sketch of the acceptance criteria

Draft R-IDs, to be firmed up at promotion.

- **R1** — WHEN a pull request targeting `main` has a failing required check, the merge SHALL be
  refused by the repository, not merely reported.
- **R2** — WHEN a commit is pushed directly to `main` without a pull request, the push SHALL be
  refused, so `AGENTS.md`'s stated GitHub Flow is enforced rather than conventional.
- **R3** — The set of required checks SHALL be enumerated explicitly, and adding a new CI job SHALL NOT
  silently become non-required.
- **R4** — IF the container image build breaks, THEN the failure SHALL become visible without waiting
  for a pull request that happens to touch one of `docker.yaml`'s `paths:` entries.
- **R5** — WHILE any required gate is failing on `main`, enforcement SHALL NOT be enabled in a way that
  blocks the very pull request that would fix it (see the sequencing note in Notes).

## Notes

- **Mandatory sequencing — this is the load-bearing part.** Enforcement must go last:
  1. Land [`issues/manpage-literal-assertion-fails-on-ubuntu.md`](manpage-literal-assertion-fails-on-ubuntu.md)
     so `Packaging` is green on `main`.
  2. Land [`issues/dockerfile-builder-missing-cxx-toolchain.md`](dockerfile-builder-missing-cxx-toolchain.md)
     so `Validate build` is green.
  3. *Then* enable the ruleset. Doing it in any other order bricks the repository against its own
     remediation, which R5 exists to prevent.
- **Cheapest shape — one repository ruleset**, not classic branch protection: a ruleset targeting
  `main` with `required_status_checks` (`Lint (fmt + clippy)`, `Test (ubuntu-24.04)`,
  `Test (macos-14)`, `Packaging (man + completions generate)`) plus `pull_request` required and
  non-fast-forward/deletion blocked. Rulesets are the current GitHub mechanism, are readable via
  `gh api repos/glentner/xdu/rulesets`, and support a bypass list — which matters, because a
  single-maintainer repository still wants an escape hatch for the case this issue's own sequencing
  describes. Whether the maintainer sits in the bypass list is a genuine shaping question, not a
  detail: a bypass that is always used is the current state with extra steps.
- **`Validate build (linux/{amd64,arm64})` should probably NOT be a required check**, even after it is
  fixed. It runs on `docker.yaml`'s `paths:` filter, so it is absent from most PRs, and a required
  check that does not run blocks the merge. R4 wants a `schedule:` canary (or a `needs:` from
  `release.yaml`) instead — different mechanism, same goal.
- **Declared overlap with the Docker issue.** That file's **R6** ("IF the container image fails to
  build, THEN that failure SHALL become visible without waiting for a PR…") is the same requirement as
  R4 here. At promotion, one of them must own it — probably R4, since the canary is CI topology rather
  than Dockerfile content — and the other should reference it. `/xdu-feature` should not shape the same
  requirement twice.
- **Not in scope: making the gates themselves stricter.** The two red gates are tracked in their own
  files, and this issue deliberately takes no position on what they should assert. It is only about
  whether their verdict binds anything.
- **Not in scope: the factory-skill half.** `/xdu-review` and `/xdu-publish` reading the check rollup
  is `spec/readers-autoload-parquet-at-runtime/META.md` **F6**, applied by `/xdu-harness`. Kept out
  because `AGENTS.md`'s four-homes table is explicit that harness feedback lives in `META.md` and never
  in `issues/`. The two fixes are complementary: enforcement stops a bad merge, and the skill change
  stops the agent from proposing one.
- **Worth checking at promotion, not established here:** whether `/xdu-release` verifies CI state before
  cutting a tag. The three releases that shipped with a red image build suggest not, but the release
  path was not audited during this triage.
- Related: [`issues/manpage-literal-assertion-fails-on-ubuntu.md`](manpage-literal-assertion-fails-on-ubuntu.md)
  and [`issues/dockerfile-builder-missing-cxx-toolchain.md`](dockerfile-builder-missing-cxx-toolchain.md)
  (the two gates this one explains), `spec/readers-autoload-parquet-at-runtime/META.md` F6 (the factory
  half), PR #10 (merged over three red checks).
- Found by: post-merge triage of PR #10 — a completeness pass asking why two long-red gates had never
  blocked anything.
