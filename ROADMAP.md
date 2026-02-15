---
status: in-progress
current_phase: 2
last_updated: 2026-02-15
---

# XDU Roadmap

## Overview

xdu is a high-performance file system indexer and query suite for storage administration.
It builds Hive-partitioned Parquet indices that can be queried with DuckDB, explored in
an interactive TUI, and (eventually) served over the web from S3-backed storage.

Designed for HPC and enterprise storage environments where traditional tools like `du`
and `find` are too slow for regular auditing of billion-file filesystems.

## Phase 1: Cleanup & Fixes

Stabilize the jwalk-based crawler, fix the broken progress display, polish the CLI.

Detailed implementation plan: <plan:25d5d2a7-20e9-4bc0-83d1-2d9de17a5a43>

- [x] Create `ROADMAP.md` with YAML frontmatter and bootstrap prompt
- [x] Refactor crawler to shared-pool concurrent partition walks (`RayonExistingPool`)
- [x] Real-time per-partition progress with `indicatif` MultiProgress (per-partition bars, "Finished" lines, global summary)
- [x] CLI polish: `help_template`, `after_help` examples, environment variable fallbacks (`XDU_INDEX`, `XDU_JOBS`)
- [x] Code cleanup: consolidate `parse_size`, extract `PartitionBuffer` to `lib.rs`
- [x] Remove `--serial` flag, `DashMap`, `parking_lot` from crawler
- [x] Pre-release verification: clippy, tests, macOS + HPC manual testing

## Phase 2: Tree View

Add a Miller columns ("tree") view to `xdu-view` inspired by macOS Finder's column view,
with file type detection and preview capabilities.

- [ ] Miller columns layout: horizontal cascade showing directory hierarchy left-to-right
- [ ] Adaptive column count based on terminal width (gracefully show last 3–5 columns as depth increases)
- [ ] File type detection (equivalent to Unix `file` command) via Rust ecosystem
- [ ] Plain-text file preview with built-in pager and scroll keybindings
- [ ] "List" mode: `<space>` opens overlay modal with file type info and text preview
- [ ] "Tree" mode: rightmost column shows automatic preview of selected file

## Phase 3: S3 Target

Write indices to S3-compatible object storage, enabling centralized index storage
accessible from anywhere. The existing Hive-partitioned Parquet layout maps naturally
to object storage paths.

- [ ] Add S3-compatible write support (likely `object_store` from the Arrow ecosystem)
- [ ] Preserve Hive-partitioned directory structure in S3 (`s3://bucket/prefix/<partition>/<chunk>.parquet`)
- [ ] CLI option to specify S3 bucket/prefix as output target
- [ ] Credential configuration (environment variables, AWS profiles, etc.)

## Phase 4: S3 Source

Add S3 buckets as a crawl source alongside local filesystem, introducing a crawler
abstraction layer. This is a larger architectural change — plan to break into sub-phases
with dedicated research and planning before implementation.

- [ ] Crawler trait abstraction (local filesystem via jwalk, S3 via listing API)
- [ ] S3 bucket/prefix listing as file metadata source
- [ ] Unified CLI interface for both local and S3 sources

## Phase 5: Streaming & Lustre

Build an abstraction layer for incremental index updates from storage change streams.
Lustre changelog is the primary motivation (as a viable replacement to the Robinhood
solution), but the abstraction should support pluggable storage backends.

### 5A: Streaming Infrastructure

- [ ] Iceberg-style merge-on-read architecture (base snapshot + delta change files)
- [ ] Periodic compaction job to consolidate deltas back into the base index
- [ ] Storage change stream abstraction trait (different backends produce differently structured events)

### 5B: Lustre Changelog Integration

- [ ] Implement change stream trait for Lustre changelog
- [ ] Delta capture from changelog events into Parquet delta files

### 5C: General-Purpose Demo

- [ ] inotify-based change stream implementation for demonstration and community reference

### 5D: Lustre Native Crawl (Research)

Modern Lustre native clients may already saturate RPC calls efficiently through the VFS
layer, but native LFS/llapi bindings could potentially make parallel crawls more polite
to metadata servers. This sub-phase is exploratory.

- [ ] Investigate if native LFS/llapi bindings improve crawl efficiency or MDS politeness
- [ ] Build Rust bindings to Lustre C library if benchmarks justify it

## Phase 6: Search

Explore enhanced search capabilities beyond regex path matching. The current regex
approach (used in `xdu-find`, `xdu-view`, and `xdu-rm`) is solid but could be more
accessible and powerful.

- [ ] Glob pattern support as an alternative to regex (e.g., `*.py` instead of `\.py$`)
- [ ] Fuzzy matching for approximate filename search
- [ ] Evaluate DuckDB full-text search extension for richer queries
- [ ] Content-type filtering if MIME type metadata is added to the index

## Phase 7: Web Client

Wasm-compiled PWA for browsing S3-backed indices from the web — essentially `xdu-web`,
the browser equivalent of `xdu-view`.

- [ ] Wasm compilation of core query logic (DuckDB + Parquet reading)
- [ ] Web UI with list and tree view modes
- [ ] Connect directly to S3-backed indices
- [ ] Search and filter capabilities matching `xdu-view`

## Phase 8: User Facing (v1.0)

Long-form written justification of the project, project identity, and community
preparation for a v1.0 release. Why does a modern extreme-scale storage indexing
tool deserve attention, and how was it built?

- [ ] Blog post / article explaining the project's motivation and architecture
- [ ] Project identity and branding
- [ ] Maintenance plan and contribution guide
- [ ] README expansion beyond basic usage

---

## Design Considerations

### Shared-Pool Concurrent Walks (Phase 1)

The crawler uses jwalk's `RayonExistingPool` to share a single `Arc<ThreadPool>` across
concurrent per-partition walks. Each walker spawns one task onto the pool; rayon
work-stealing naturally balances load across all active walkers. Driver threads
(`std::thread`) are separate from the pool and handle iterator consumption + buffer writes.

Thread budget: N pool threads + C driver threads + 1 main thread.

### Progress Display (Phase 1)

TTY-connected users see real-time `indicatif` MultiProgress output:
- Per-active-partition progress bars with live file count and byte accumulation
- "Finished" lines printed as each partition completes
- Global summary bar with aggregate statistics
- Non-TTY mode: plain-text status lines

### Hive-Partitioned Parquet

The index is structured as `outdir/<partition>/<chunk>.parquet`, where partitions
correspond to top-level subdirectories. This layout enables per-user queries without
full-index scans and maps naturally to S3 object storage (Phase 3).

### Iceberg-Style Streaming (Phase 5)

Incremental updates use a merge-on-read approach: the base index (from a full crawl)
is augmented by delta files from change streams. Periodic compaction merges deltas
back into the base for query efficiency. The change stream abstraction allows different
storage backends (Lustre changelog, inotify, etc.) to plug in.

---

## Bootstrap Prompt

Use this prompt to resume development on this project:

```
I'm working on xdu — a high-performance file system indexer and query suite for
storage administration, built in Rust.

Please read:
- ROADMAP.md for current status and next tasks
- WARP.md for architecture overview and design decisions
- src/bin/xdu.rs for the crawler implementation
- src/lib.rs for shared types and utilities

Check the ROADMAP.md YAML frontmatter for the current phase. Implement the next
unchecked item(s) in the current phase, then:
1. Update ROADMAP.md to check off completed items
2. Update the frontmatter (current_phase, last_updated)
3. Commit with "WIP: <description>"

When a phase is complete, check in before proceeding to the next phase.
```
