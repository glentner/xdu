---
name: xdu-roadmap
description: >-
  Retire the seeds whose cycles have landed, and keep ROADMAP.md true. Finds every
  issues/{slug}.md carrying a shaped/resolved status, confirms the cycle actually reached main, and
  deletes the seed with its ROADMAP entry after a human-gated preview. Also repairs the drift a
  shipped cycle leaves behind: dangling cross-references, stale figures in surviving seeds, and
  adoption markers left by abandoned branches. Operational sibling of the lifecycle, run between
  cycles (see .agents/factory/methodology.md).
disable-model-invocation: true
argument-hint: "[--dry-run] [slug] [--all] [status]"
allowed-tools: Read, Grep, Glob, Edit, AskUserQuestion, Bash(git status *), Bash(git branch *), Bash(git log *), Bash(git ls-tree *), Bash(git rev-parse *), Bash(git add *), Bash(git commit *), Bash(grep *), Bash(del *), Bash(ls *), Bash(head *)
---

# xdu-roadmap — retire what shipped, keep the index true

## When to Use

Invoke `/xdu-roadmap` between cycles, or any time `ROADMAP.md` has stopped describing the work
that is actually left. Its main job is the one step the lifecycle never had: a cycle seeded
from an `issues/{slug}.md` ships, and nothing retires the seed, so the backlog keeps advertising
work that is already on `main`.

This is **maintenance, not a lifecycle step.** It edits `ROADMAP.md`, deletes seeds under
`issues/`, and repairs the references a deletion breaks. It never edits `spec/{slug}/` (it reads
`GOAL.md` § *Non-goals* once, for the remainder check in Step 3), never touches product source, and
never advances an FSM.

Deliberately **not** part of `/xdu-publish`. Retirement writes to `issues/` and `ROADMAP.md`, which
is exactly what publish's staleness gate watches — anything outside `spec/` after the approved
review invalidates it. Folding retirement into publish would burn the approval publish just earned.
Publish leaves retirement alone and stops there.

Reference: [`methodology.md`](../../factory/methodology.md),
[`templates/ISSUE.md`](../../factory/templates/ISSUE.md) (the `status:` vocabulary), and `AGENTS.md`
§ *Where a deferral goes*.

**Harness portability.** Runs on any harness — see
[`portability.md`](../../factory/portability.md). Run the *Current state* commands yourself if not
auto-injected; ask in plain text and STOP if `AskUserQuestion` is unavailable.

## User Instructions

Additional instructions provided with the invocation: $ARGUMENTS

## Current state (injected at load)

- Branch: !`git branch --show-current`
- Tree: !`git status --porcelain | head -n 20`
- Shaped seeds: !`grep -rl "^status: shaped" issues 2>/dev/null || true`
- Resolved seeds: !`grep -rl "^status: resolved" issues 2>/dev/null || true`
- Queued entries: !`grep -c "^## " ROADMAP.md 2>/dev/null || true`

## Argument Parsing

- No argument → consider every shaped/resolved seed whose cycle has landed. The default.
- `<slug>` → restrict to the seed adopted by that cycle.
- `--all` → widen Step 5's drift sweep to seeds this retirement does not otherwise touch.
- `--dry-run` → run Steps 1–4 and present the preview, then STOP. No edits, no deletions, no
  commit.
- `status` / `report` → list shaped/resolved seeds with landed/in-flight/stale for each; no work.

## Safety Principles

- **Deleting a seed destroys the only copy outside git history.** Preview every retirement and
  confirm with the human before acting. Never delete on inference alone.
- **`del`, not `rm`.** `rm` refuses in this repo by design and `del` is reversible trash. The seed
  is tracked, so follow with `git add -A` to stage the deletion; that is why this skill does not
  need `git rm`.
- **Landed means on `main`.** A seed is retired only when `git ls-tree HEAD -- spec/{slug}` is
  non-empty. A `shaped` marker proves a cycle *started*, never that it finished — a branch that
  bounced at review or was abandoned still carries the marker, and retiring its seed deletes the
  justification for work nobody did. There is no `develop` branch; every feature squashes back onto
  `main`, so that is where "landed" is decided.
- **A GOAL is negotiated down from a seed.** Anything the cycle cut and still wants must survive
  retirement. Step 3 checks this against `GOAL.md` § *Non-goals*; it is the one failure here that
  loses work rather than leaving litter.
- **Never edit `spec/{slug}/`.** It is a dated record of what was true when written. A retired seed
  leaves a dangling `Seed:` link in `GOAL.md` § *Related materials*, and that link stays: it is the
  signpost that makes `git log --diff-filter=D -- issues/{slug}.md` a two-step recovery instead of
  archaeology.
- **One commit per retirement**, `[harness]`-prefixed imperative subject (the 72-column budget in
  `AGENTS.md` § *Prose and comments* applies), **no trailers**. Never push.

## Procedure

### Step 0 — status / dry-run (when requested)
`status`: classify each shaped/resolved seed and report; no work. `--dry-run`: Steps 1–4, present
the preview, STOP.

### Step 1 — Pre-flight
Clean tree; non-empty → STOP. Confirm you are on `main`; this skill does not run on a feature
branch, because a seed retired on a branch that never merges takes the backlog entry with it.

### Step 2 — Find the shaped seeds and classify each
```
grep -rl "^status: shaped" issues 2>/dev/null || true
grep -rl "^status: resolved" issues 2>/dev/null || true
git ls-files 'issues/*.md'          # cross-check: an untracked seed is not yet part of the index
```

Match on the frontmatter, never on the filename — nothing constrains a seed's filename and its
cycle slug to agree, because `/xdu-feature` derives the slug in the shaping conversation rather
than copying it off the file it promotes. A filename guess deletes nothing, or deletes the wrong
thing.
The marker itself is prose (`shaped on feature/{slug} (…) — see spec/{slug}/GOAL.md`, `shaped as
spec/{slug}`, `resolved on fix/{slug} (…) — see spec/{slug}/`), so read the `{slug}` out of the
`spec/{slug}` path in the value, falling back to the branch suffix, and classify:

| `git ls-tree HEAD -- spec/{slug}` | Meaning | Action |
|---|---|---|
| non-empty | the cycle landed | retire (Steps 3–4) |
| empty, branch exists | in flight | leave alone |
| empty, no branch | abandoned; the marker is stale | offer to reset `status:` to `unshaped`, never delete |

"Branch exists" means local **or** remote — after a `git fetch` it may be only the latter:
```
git rev-parse --verify --quiet {branch}
```
for the branch the marker names, or for each plausible `{kind}/{slug}` (`feature/`, `fix/` — the
set is open, so check the branch `TECH.md` names if one exists). The stale case matters because
`/xdu-feature` treats a shaped marker as a live adoption. Left alone, an abandoned cycle permanently
bricks its own seed: it can never be promoted and its ROADMAP entry can never be worked.

### Step 3 — Check the seed shipped whole
Read `spec/{slug}/GOAL.md` § *Non-goals* against the seed's problem statement and its sketch of the
acceptance criteria. Non-goals are the written record of what the promotion negotiated away.

Anything cut and still wanted does not die with the seed. Either rewrite the seed down to the
remainder and reset `status:` to `unshaped`, re-wording its ROADMAP entry to match, or file a fresh
seed from [`templates/ISSUE.md`](../../factory/templates/ISSUE.md). Only a seed with no live
remainder is deleted.

**A non-goal that discharges itself by pointing elsewhere is conditional, and the condition is
what you verify.** "Record it there", "that is a seed for `issues/`" — each of those is the reason
the cycle was allowed to ship without the work. Open the file it names and confirm the obligation
is *in* it: an `issues/{slug}.md` plus a `ROADMAP.md` entry. An intention stated in `GOAL.md`, in
`PLAN.md`, or in the ROADMAP entry is not the record; the named destination is. Where it is
missing, put it there first, in the retirement's own commit.

Check this before deleting, not after. The obligation is frequently written in exactly one
place — the roadmap entry the retirement removes — so the deletion is what makes the loss
irreversible, and this is the one failure in the sweep that costs work rather than leaving litter.

### Step 4 — Preview, confirm, retire
Present per seed: the file to delete, the ROADMAP entry to remove, any remainder being preserved,
and the cross-references Step 5 will repair. Confirm with `AskUserQuestion`.

Then, for each confirmed seed:
```
del issues/{seed}.md     # the path Step 2 printed, never a name reconstructed from {slug}
```
Remove its `## ` block from `ROADMAP.md` — the `## ` heading through the `**Seed:**` line
inclusive, plus the prose between and the trailing blank line. Leave the `## Delivered to date`
heading standing.

### Step 5 — Repair what the removal broke
Find the references; do not recall them. Both strings matter, and Step 2 already says they need
not be the same:

```
grep -rn '{seed-filename}\|{slug}' --include='*.md' --include='*.rs' --include='*.py' --include='*.toml' . \
    | grep -vE '(^|/)\.git/'
```

Triage every hit by where it lands, because two of these destinations are deliberately left alone:

| Where the hit is | Action |
|---|---|
| `ROADMAP.md`, other `issues/*.md` | Repair. This is the work described below. |
| `.agents/` skills, templates, factory docs | Repair. An example citing a file this sweep deletes is drift; one asserting something the promotion never did is a wrong instruction. |
| `spec/**` | **Leave.** Never edit `spec/{slug}/`. The dangling `Seed:` link is the signpost that makes recovery two steps instead of archaeology. |
| `.agents/factory/harness-log.md` | **Leave.** A dated record of what was decided, not an index of what exists. |
| product source (`src/`, `tests/`, `bench/`, packaging scripts) | **Report, do not edit.** Source is never supposed to cite a seed or a feature-scoped id (`AGENTS.md`); a hit here is a finding for the human, not a fix for this sweep. |

Entries carry no numbers, so removing one renumbers nothing. What still breaks is prose:

- A cross-reference to the retired cycle by name. The dependency is discharged, so say so rather
  than deleting the sentence — a reader needs to know the ordering constraint existed and cleared.
- A count that the retirement makes wrong, in `ROADMAP.md` and inside surviving seeds.
- A figure the shipped cycle falsified — a count, a `file:line` citation, a quoted output —
  anywhere in a file this retirement already edits. Read those whole: a stale figure standing
  beside a freshly repaired link is worse than one in a file nobody opened, because the repair is
  what tells a later reader the file was reviewed.
- With `--all`: the same figures in seeds this retirement never touches — a line count, an
  occurrence table, a file inventory. A stale baseline in a seed whose own acceptance criterion is
  a line-count guard is the one number in it that has to be right.

Do not rewrite a `Found by:` line. Those ordinals are provenance, not queue position.

### Step 6 — Commit
```
git add -A
git commit -m "[harness] Retire the {slug} seed and its roadmap entry"
```
One commit per retirement; fold Step 5's repairs into the commit that caused them. Plain imperative
subject inside the 72-column budget; **no trailers**. Do not push.

For a stale marker reset with no deletion: `[harness] Reset the stale adoption marker on {slug}`.

### Step 7 — Report
Seeds retired, seeds left in flight, stale markers found and what was done about them, remainders
preserved, and cross-references repaired. Name anything you chose not to touch.

Name any **harness friction** this sweep exposed and offer to record it, on the same rule the
methodology carries: `/xdu-harness` reads only `spec/*/META.md`, so on the human's OK it goes to
the retired cycle's `spec/{slug}/META.md` with `origin=xdu-roadmap:<step>` and `status=open`,
as its own `[harness]` commit.

## Examples

- `/xdu-roadmap` — retire every landed seed, one commit each.
- `/xdu-roadmap --dry-run` — preview the whole sweep; change nothing.
- `/xdu-roadmap readers-autoload-parquet-at-runtime` — retire just that cycle's seed.
- `/xdu-roadmap status` — classify every shaped/resolved seed; no work.

## Notes

- Nothing in `/xdu-publish` retires a seed: this skill is where deletion lives, so publish
  keeps neither an `Edit` tool nor a deletion verb.
- A seed that shipped leaves no terminal record. For shipped work the refutation is free: someone
  re-filing it greps the code and finds it already done, and `spec/{slug}/` holds the account.
- This skill never touches product source, never advances an FSM, and never ships to `main` on a
  branch — it commits retirements directly, one per seed.
