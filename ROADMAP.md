# XDU Roadmap

xdu is a high-performance filesystem indexer and query suite for HPC and enterprise storage, where
`du` and `find` collapse under billion-file trees. It builds a persistent, Hive-partitioned Parquet
index once, then answers size/age/pattern questions instantly via DuckDB — on the command line
(`xdu-find`), in an interactive TUI (`xdu-view`), or through guarded bulk deletion (`xdu-rm`). This
document records the larger-scale features still intended: reaching beyond the local disk to object
storage and change-stream ingestion, beyond `path/size/atime` to richer metadata, and beyond the
terminal to the browser.

This is a **forward-looking roadmap, not an implementation plan.** Each entry states a user problem
and the intention behind solving it — a seed for `/xdu-feature` to shape into a `GOAL.md`, leaving
the *how* to `/xdu-plan`. As-built architecture and the load-bearing invariants live in
[`AGENTS.md`](AGENTS.md); this file is only about what comes next. Horizons below (near / mid / long
term) are **indicative** — the hard constraints are the stated dependencies.

## Delivered to date

The foundation is in place and in daily use: a shared-rayon-pool concurrent crawler that walks a tree
into a Hive-partitioned Parquet index (`path`, `size`, `atime`), and the three tools that read it —
`xdu-find` for scripted DuckDB queries, `xdu-rm` for guarded bulk deletion, and `xdu-view` for
interactive exploration with both a list view and a Miller-columns tree view (file-type detection and
a scrollable text preview pane). Packaging is established too: the release tarball, `install.sh`, the
scdoc man pages, and generated shell completions (so GitHub issue #4 is effectively resolved).
Everything below builds on that baseline.

---

## In-list file preview overlay (`xdu-view`)

The tree view can preview a file inline, but the list view cannot — to see what a file actually *is*
you have to switch modes or leave the list, which breaks the flow of scanning a large directory. An
overlay that pops file-type info and a short text preview over the current list, then dismisses back
to it, would make the list view as inspectable as the tree view and close the last unfinished piece
of the original tree-view work. (The natural `<space>` binding is already taken by list-mode
drill-in, so the trigger key is something `/xdu-plan` will need to reconcile.)

*Horizon: near-term · Depends on: — · Refs: —*
**Seed:** `/xdu-feature In xdu-view list mode, let the user pop an overlay with the selected file's type and a short text preview without leaving the list.`

## Bulk-op sibling tools: `xdu-mv` and `xdu-tar`

`xdu-rm` proved a powerful pattern: select a set of files with an index query, then act on exactly
that set — no `xdu-find | xargs` plumbing, and with real destructive safety (dry-run, confirmation,
`--safe` re-stat, deterministic ordering under `--limit`). The same leverage is wanted for
*relocating* matched files (`xdu-mv`) and *archiving/packing* them (`xdu-tar`), turning common admin
chores — "move everything untouched in two years to cold storage," "tar up this user's stale logs" —
into single, safe, index-driven commands. Both tools should reuse `xdu-rm`'s destructive-safety
model rather than reinvent it.

*Horizon: near-term · Depends on: — (reuses xdu-rm's safety model) · Refs: #1*
**Seed:** `/xdu-feature Add xdu-mv and xdu-tar that relocate or archive the exact set of files matched by an index query, reusing xdu-rm's dry-run/confirm/safe-mode safety.`

## Richer search: glob, fuzzy, full-text, content-type

Regex path matching is powerful but not friendly — most users think in globs (`*.py`), not anchored
regex (`\.py$`). Broadening the matching options across `xdu-find`, `xdu-view`, and `xdu-rm` — glob
syntax as a gentler alternative, fuzzy matching for approximate filename search, and DuckDB's
full-text search extension for richer queries — would reach a wider audience without giving up the
current regex power. Filtering by content type ("all video files over 1 GB") is the natural next
step, but it depends on MIME metadata living in the index, which ties back to the schema-evolution
work below.

*Horizon: mid-term · Depends on: content-type filtering needs the richer schema · Refs: —*
**Seed:** `/xdu-feature Add glob and fuzzy path matching (and evaluate DuckDB full-text search) as friendlier alternatives to regex across xdu-find, xdu-view, and xdu-rm.`

## On-disk index schema versioning

Today the Parquet schema is three fixed, non-null columns with no version marker on disk, so any
change to it would silently break every existing index and every reader (the schema-stability
invariant). Before the index can grow new columns, it needs to carry its own format version so
readers can detect, reject, or migrate older indices instead of misreading them. Small on its own,
this is the hard prerequisite for enriching the schema at all — it must land before any column is
added.

*Horizon: near-term · Depends on: — · Refs: — (enabler for the two features below)*
**Seed:** `/xdu-feature Add an on-disk schema-version marker to the index so readers can detect the format version and safely reject or migrate indices written by other versions.`

## Richer index schema: owner, group, permissions

On large shared filesystems (`/projects/{lab1,lab2,…}`) administrators need more than "which project
is biggest?" — they need "which *user within* a project is biggest?", plus octal permissions to
reason about exposure and cleanup. The current `path/size/atime` schema cannot answer per-owner or
per-permission questions at all; today that means falling back to a slow `find`. Recording owner,
group, and mode in the index unlocks a whole class of storage-accounting queries.

*Horizon: mid-term · Depends on: on-disk schema versioning (breaking, cross-cutting index-format change) · Refs: #2, #3*
**Seed:** `/xdu-feature Record file owner, group, and permission bits in the index so queries can attribute size and age to individual users and groups within a shared tree.`

## Permission-aware, access-scoped queries

Once ownership and permissions are indexed, the index itself becomes a way to leak information: a
non-root user could read sizes and paths they'd never be allowed to `stat` on the live filesystem.
Borrowing from GUFI's shadow-tree model, queries could be scoped so a user only sees data they could
normally access — applied by default (or by an explicit flag) when `xdu` builds or serves an index as
root. This is what makes a shared, centrally built index safe to expose to the tenants it describes.

*Horizon: long-term · Depends on: richer index schema (owner/group/perms) · Refs: #3*
**Seed:** `/xdu-feature Add access-scoped querying so a non-root user only sees index rows for files they could normally access, using the indexed owner/group/permission data.`

## S3 as an index target

Indices today live on local disk, which ties them to the machine that built them. The existing
Hive-partitioned layout (`<partition>/<chunk>.parquet`) maps directly onto object-store key prefixes,
so writing the index to S3-compatible storage is cheap — and it unlocks centralized,
build-once/read-anywhere indices that any tool (or a future web client) can point at without copying
files around.

*Horizon: mid-term · Depends on: — (write path only; layout carries over) · Refs: —*
**Seed:** `/xdu-feature Let xdu write its Hive-partitioned Parquet index to an S3-compatible bucket/prefix instead of a local directory, so indices can be stored and queried centrally.`

## S3 as a crawl source

Organizations increasingly park huge datasets in object storage — data lakes, cold archives, tiered
backups — and get none of the size/age/pattern auditing there that xdu gives a POSIX tree. Treating
an S3 bucket as a *source* of file metadata, alongside the local filesystem, brings that same
accounting to object storage. Architecturally this is a larger move than the write path: it means
abstracting the crawler behind a trait so a local jwalk backend and an S3-listing backend are
interchangeable behind one CLI, and expressing per-source capability differences (object storage has
no atime — the Unix-only/no-atime assumption no longer holds for every backend).

*Horizon: long-term · Depends on: crawler-source abstraction; expect dedicated research + sub-phases · Refs: —*
**Seed:** `/xdu-feature Let xdu audit files stored in an S3-compatible bucket the same way it audits a local tree, so object-store datasets get the same size/age/pattern reporting.`

## Streaming index updates & Lustre changelog

Full re-crawls don't scale to filesystems that change constantly under billions of files — by the
time a crawl finishes it's already stale, and re-running it is enormously expensive. The intent is an
incremental, Iceberg-style **merge-on-read** index: a base snapshot from a full crawl, augmented by
delta files fed from a storage change stream, with periodic compaction folding deltas back into the
base. The primary driver is the Lustre changelog (a modern replacement for Robinhood), but the change
stream should be a pluggable abstraction — with an inotify-backed backend as a general-purpose demo
and community reference. A related open question is whether native Lustre LFS/llapi bindings would
make the full crawl itself faster or gentler on metadata servers than going through the VFS.

*Horizon: long-term · Depends on: — (largest effort on the roadmap; expect several sub-phases) · Refs: —*
**Seed:** `/xdu-feature Keep the index of a constantly-changing, billion-file filesystem queryable without full re-crawls, driven first by the Lustre changelog.`

## Web client (`xdu-web`)

`xdu-view` is terminal-only, which limits who can explore an index and from where. Once indices live
in S3, a Wasm-compiled progressive web app could browse them straight from the browser — the web
equivalent of `xdu-view`, with the same list and tree views and the same search and filtering —
making a centrally stored index explorable by anyone with a link, no shell account required.

*Horizon: long-term · Depends on: S3 as an index target · Refs: —*
**Seed:** `/xdu-feature Build xdu-web, a Wasm progressive web app that browses an S3-backed index in the browser with list/tree views and search, mirroring xdu-view.`

## The man-page gate false-alarms on distro `scdoc`, and `main` is red because of it

CI's packaging job fails on `ubuntu-24.04` with `CORRUPT RENDER: share/man/man1/xdu.1 is missing the
literal: OUTDIR/.xdu-complete` — but the man page is fine. No character is lost; `mandoc` simply fills
the paragraph and breaks the line inside `xdu-complete`, and the gate's newline-to-space flatten cannot
survive a break *inside* a token. The variable turns out to be `scdoc`, not `mandoc`: 1.11.5 (homebrew,
what a maintainer runs locally) escapes hyphen-minus so the token cannot break, and 1.11.2 (what noble
ships, what CI installs) does not. So the gate passes for the author and fails for everyone else, and
the local check `AGENTS.md` documents cannot predict CI's verdict.

The gate is worth keeping — it exists because a mis-escaped literal renders at `scdoc` exit 0 and once
published a wrong glob to an operator past a green build. What it needs is to assert *content* rather
than *layout*, so its verdict no longer depends on which `scdoc` built the roff, what width `mandoc`
filled to, or where the next paragraph edit pushes a line break. A second brittleness class is already
latent in the same line (mandoc indents with tabs, the flatten squeezes only spaces), and one
normalization closes both. This is **pre-existing** — it arrived with the gate itself, in the
post-merge harness commit `9c579cf`, and `main` was already red before PR #10 — and the fix is two
lines, but settling *which* two required an adversarial reproduction: the obvious widen-the-render fix
is measurably defeated by a routine documentation edit.

*Horizon: near-term · Depends on: — · Refs: `.agents/factory/harness-log.md` (why the flatten exists); the Docker builder defect from the same CI run*
**Seed:** [`issues/manpage-literal-assertion-fails-on-ubuntu.md`](issues/manpage-literal-assertion-fails-on-ubuntu.md)

## `man xdu` hyphenates the completion-marker path, so the page operators read is wrong

Fixing the CI gate made `main` green; it did not make the page correct. `man-db` renders with `groff`,
and `groff` hyphenates where `mandoc` does not — so at the default width `man xdu` publishes
`OUTDIR/.xdu-com` + **U+2010** + newline + `plete`, ten such hyphens on that page alone, and an operator
who copy-pastes the marker path gets a filename that cannot exist. It reproduces identically on roff
from both `scdoc` versions, so neither the normalization fix nor pinning a newer `scdoc` touches it,
and CI cannot see it because CI renders with `mandoc`. Notably this **falsifies the premise** recorded
when the question was set aside ("one adversarial run measured zero U+2010") — the boundary was
reasonable, the reason given for it was not. The choice to weigh is whether to teach the gate to
tolerate the hyphen, or to render with `groff` in CI and actually catch the class users are exposed to.

*Horizon: near-term · Depends on: — (independent of the gate fix; the fix does not address it) · Refs: the man-page gate entry above; `spec/manpage-literal-assertion-fails-on-ubuntu/EVIDENCE.md`*
**Seed:** [`issues/manpage-groff-hyphenates-marker-path.md`](issues/manpage-groff-hyphenates-marker-path.md)

## The man-page literal gate is correct but narrow

Four measured gaps in what the literal gate can structurally see, all pre-existing. Three were left
alone because the pass that found them was scoped to making the *existing* assertions
layout-insensitive; the fourth turned out to be an unmet requirement of that pass and was fixed there. No
literal names the binary it belongs to, so copying one rendered page over another is green. The page
list is hard-coded while the render step globs `doc/*.scd`, so a fifth man page is entirely unasserted
— measured shipping the exact historical `OUTDIR//.parquet` corruption past a green gate, which matters
because `xdu-mv`/`xdu-tar` are queued above. The four env-var assertions are thin literals — a
well-chosen path per page would catch more than a variable name does (their *duplicate-occurrence*
blind spot was an unmet R7 and is already fixed). And
`col -b` rewrites multibyte characters as literal `\xNN` text outside a UTF-8 locale, which bounds what
can ever be asserted and already mis-measured the `groff` work above. Deriving the page list from
`doc/*.scd` is nearly free and converts the widest gap into a build error.

*Horizon: near-term, low priority · Depends on: — · Refs: the two entries above; `issues/ci-gates-are-advisory.md` (a gate that binds nothing is the wider version)*
**Seed:** [`issues/manpage-gate-coverage-gaps.md`](issues/manpage-gate-coverage-gaps.md)

## `--version` is documented but rejected by every binary

All four man pages document `-V, --version`, and `AGENTS.md` states the version is single-sourced from
`Cargo.toml` via clap — but no `#[command(...)]` block in `src/cli.rs` sets `version`, so every binary
exits with `error: unexpected argument '--version' found`. A user-facing defect in a released version,
and an invariant §10 (man-pages-vs-code) violation narrowed to exactly that pair: the generated
completions omit the flag correctly, and the man pages need no edit because they already describe the
intended behaviour. Likely one attribute per struct.

*Horizon: near-term · Depends on: — · Refs: —*
**Seed:** [`issues/version-flag-missing.md`](issues/version-flag-missing.md)

## Internal cleanups surfaced by the crawl-hardening pass

The crawl-hardening work produced a wider architecture assessment whose low-risk cleanups were applied
at the time (a shared `lib::index_glob` behind every reader's Parquet glob, one home for the
index-layout constants, reader awareness of the completion marker). It also recorded what was too
risky or too large to fold in: routing the DuckDB injection surface through validated escaping on the
`index_glob` seam, reconciling `xdu-view`'s `format_file_count` with `lib::format_count`, and lifting
the pure TUI helpers — `strip_ansi` above all, which is load-bearing for terminal safety — out of the
2,500-line `xdu-view` into `lib` where they can be tested. Most of these are invisible to users and
decide how much of the codebase stays testable as it grows; the terminal-safety pair tracked separately
below is **not** — a wedged terminal and a filename that crashes the TUI are both user-facing. The full
record, including the performance levers the benchmark work evaluated and rejected, is
[`spec/crawl-hardening/ASSESSMENT.md`](spec/crawl-hardening/ASSESSMENT.md).

*Horizon: near-term · Depends on: — · Refs: —*
**Seed:** `/xdu-feature Work through the deferred cleanups recorded in spec/crawl-hardening/ASSESSMENT.md: escape the DuckDB injection surface behind lib::index_glob, reconcile the duplicated count formatters, and lift the pure xdu-view helpers into lib with tests — coordinating with the xdu-view terminal-safety fix, which touches the same file.`

## `xdu-view` terminal safety: panic-safe restore and multibyte truncation

Two invariant §12 gaps in `xdu-view`, both pre-existing and both user-facing. The terminal restore is
plain sequential code after `run_app` with no Drop guard and no panic hook, and `panic = "abort"` means
no unwind would run one anyway — so any panic, or an early `?` from the fallible `Terminal::new` that
already runs after raw mode is entered, leaves the terminal wedged. Separately, both renderers truncate
display names by slicing `&str` at a byte offset computed in terminal columns, which panics outright on
a multibyte filename. The two compound: the second is exactly the panic the first fails to clean up
after. One change to one file, best done together with the `strip_ansi` lift above.

*Horizon: near-term · Depends on: — · Refs: —*
**Seed:** [`issues/xdu-view-terminal-safety.md`](issues/xdu-view-terminal-safety.md)

## Completion marker: scoped runs should not speak for the whole index

`xdu` clears the completion marker on every run — including `xdu -p onepartition` — and rewrites it
from that run's stats alone. So a clean partition-scoped re-index resets `errors=0` and silently retires
the tolerated-error warning that an earlier `--allow-errors` run recorded, while the skipped regions in
other partitions remain missing. The readers, `xdu-rm` included, then report a clean bill of health for
an index that is still incomplete. Needs per-partition attestation, or a scoped run declining to write a
whole-index marker — marker-format or CLI-semantics work either way.

*Horizon: near-term · Depends on: — · Refs: On-disk index schema versioning*
**Seed:** [`issues/marker-scoped-run-attestation.md`](issues/marker-scoped-run-attestation.md)

## Re-indexing never retires a partition whose source directory is gone

Delete a top-level directory from an indexed tree, re-index, and its partition — chunks and rows — stays
in the index forever: `finalize` prunes stale chunks only *within* the partitions a run actually walked,
so one it never enqueued is never reconciled. The run exits 0 and writes a completion marker, so every
reader reports a clean index that is still answering queries with rows for files that no longer exist —
`xdu-rm` matches them, and a purged project keeps counting against the tree's size forever. The marker's
own `files=` count contradicts the row count the readers return, and nothing compares the two. The stale
partition is **pre-existing behaviour**; what is new is having an attestation that fails to detect it.
Needs whole-index reconciliation (and a scoped run must never delete the partitions it was told to skip),
so it lands next to the scoped-marker work above.

*Horizon: near-term · Depends on: — · Refs: Completion marker scoped runs (same question, marker side)*
**Seed:** [`issues/orphan-partition-survives-reindex.md`](issues/orphan-partition-survives-reindex.md)

## Re-indexing an unreadable partition deletes the rows it already held

The sharpest member of the same family, and the only one that destroys data rather than leaving extra.
When a partition's source directory cannot be read, its walk yields one error and zero files — but the
partition is still finalized, and `finalize` prunes from chunk 0, taking every chunk the previous index
held. The prune loop is correct for the job it was written for (retiring the surplus of a prior larger
run) and simply cannot tell "legitimately smaller now" from "could not be read". With `--allow-errors`
the run then exits 0 and attests itself, which is the cruel case: that flag exists so an operator who
*expects* unreadable regions keeps the rest of the index, and today it can leave them with less than
they started with. The trigger is ordinary on shared storage — a permission change, a stale mount, an
NFS blip. The prune scope is **pre-existing in `main`**, where the same rows vanish with no diagnostic
at all; what this pass added was the first visibility into it. Wants per-partition error state on
`PartitionBuffer` so finalize can decline to prune what it could not read.

*Horizon: near-term · Depends on: — · Refs: the two reconciliation items above — same finalize scope*
**Seed:** [`issues/unreadable-partition-prunes-prior-chunks.md`](issues/unreadable-partition-prunes-prior-chunks.md)

## Benchmark harness: stop `baseline` mode overwriting the committed reference

`bench/run.sh baseline` defaults `--out` to `bench/results/baseline.json`, and `baseline` mode is also
the configuration set anyone reaches for to capture a comparison — so the natural command for "measure
my build against the reference" destroys the reference. It is the one file in `bench/results/` that is
not reproducible on demand: regenerating it yields a *different* baseline, silently redefining what "no
regression" means. A `usage()` warning exists; the loaded default does not.

*Horizon: near-term · Depends on: — · Refs: —*
**Seed:** [`issues/bench-baseline-overwrite-guard.md`](issues/bench-baseline-overwrite-guard.md)

## The container image has not built since January, and what is published is missing `xdu-rm`

The Dockerfile's builder stage installs no packages, and `rust:1-slim-bookworm` ships a C compiler but
no `c++` — which is the tool name cc-rs looks for when the `bundled` DuckDB feature compiles DuckDB
from C++ source. Every `docker build` since 2026-01-20 has died at `ToolNotFound: failed to find tool
"c++"`. The cause is one deleted line: the musl→glibc revert swapped `FROM rust:alpine` for
`FROM rust:slim` and dropped the `apk add … g++` that went with it. Nothing caught it because the PR
guardrail did not exist until July, `release.yaml` has no dependency on a green image build, and there
is no scheduled canary — so three tagged releases shipped with the image build red.

The user-facing half is worse than a stale tag. Because `ghcr.io/glentner/xdu:latest` predates that
same commit, it is still the January `FROM scratch` musl image built from v0.2.1: it has no shell and
contains only `xdu`, `xdu-find` and `xdu-view` — **`xdu-rm` has never been in any published image**.
Fixing the Dockerfile is one apt layer and is proven to work end to end, offline and non-root; the
harder part is deciding what happens to the live tags, and putting some signal in place so a red image
is noticed in days rather than months.

*Horizon: near-term, low priority · Depends on: — · Refs: the man-page gate defect from the same CI run; base-image digest pinning is a deliberate follow-up, not part of this*
**Seed:** [`issues/dockerfile-builder-missing-cxx-toolchain.md`](issues/dockerfile-builder-missing-cxx-toolchain.md)

## CI gates are advisory: nothing enforces a red check

The repository has good gates — format, clippy, the test matrix, the man-page literal assertion, a
container build guardrail — and nothing anywhere makes any of them binding. `main` is unprotected with
no rulesets, so a pull request merges over failing checks (PR #10 did, with three), commits reach
`main` directly without a PR at all (ten of the last fifteen), and a batched push means a newly added
gate may never be executed before it lands — which is exactly how the man-page assertion arrived
already broken and stayed that way. Releases do not depend on a green image build either, so three of
them shipped with it red.

This is the reason the other two defects survived rather than a defect in its own right, and it is why
they are being written down instead of quietly fixed: the gap is between "the gate exists" and "the
gate binds anything". The fix is small — a ruleset with required checks and no direct pushes, plus a
scheduled canary for the image build that no PR reliably triggers — but it has a hard ordering
constraint. Enforcement has to land *after* the two red gates are green, or it blocks the very changes
that would fix them.

*Horizon: near-term · Depends on: the man-page gate and container-image fixes above must land first (enforcement would otherwise block its own remediation) · Refs: `spec/readers-autoload-parquet-at-runtime/META.md` F6 covers the factory-skill half*
**Seed:** [`issues/ci-gates-are-advisory.md`](issues/ci-gates-are-advisory.md)

## Native OS packages (DEB / RPM)

Installation today is a release tarball plus `install.sh`. Native `.deb` and `.rpm` packages would
let HPC and enterprise Linux users install and upgrade xdu through their system package manager,
removing a small but real adoption friction. The existing tarball layout and `install.sh` contract
already define the exact file map these packages would ship.

*Horizon: near-term, low priority · Depends on: — (builds on the established release layout) · Refs: #5*
**Seed:** `/xdu-feature Produce native DEB and RPM packages for xdu using the existing release tarball file layout so users can install and upgrade via their system package manager.`

## Toward v1.0: narrative, branding, and community

A v1.0 cut is as much about explaining the project as shipping code. Why does an extreme-scale
storage indexer deserve attention, how was it built, and how can others contribute? This covers the
non-code work that turns a capable tool into a maintained project: a written motivation-and-
architecture piece, a project identity, a contribution guide and maintenance plan, and a README that
goes beyond basic usage.

*Horizon: long-term · Depends on: — (a release checkpoint, not a feature) · Refs: —*
**Seed:** `/xdu-feature Prepare xdu for a v1.0 release with a motivation-and-architecture writeup, project branding, a contribution/maintenance guide, and an expanded README.`
