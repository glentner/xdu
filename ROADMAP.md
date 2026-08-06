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
