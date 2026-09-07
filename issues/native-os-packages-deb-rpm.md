---
status: unshaped
kind: feature
appetite: small
---

# Native OS packages (DEB / RPM)

> **Pre-shaped candidate, not a contract.** `/xdu-feature` promotes this into `spec/{slug}/GOAL.md`,
> where appetite, non-goals and the R-IDs get negotiated. Do not copy it verbatim.

## Problem

Installation is a release tarball plus `install.sh`. HPC and enterprise Linux users cannot install
or upgrade `xdu` through their system package manager — a small but real adoption friction. Low
priority, but the groundwork is done: the tarball layout and `install.sh` contract already define
the exact file map the packages would ship.

## Why it was deferred

Not deferred from a factory pass — a forward-looking intention from the original roadmap, recorded
before the `issues/` back-reference convention was enforced. This file retrofits the
back-reference; no new scoping was done.

## Outcome / vision

Native `.deb` and `.rpm` packages shipping the established release-tarball file layout.

## Sketch of the acceptance criteria

- **R1** — `.deb` and `.rpm` packages SHALL install and upgrade `xdu` via the system package
  manager, shipping the exact file map the release tarball defines.

## Notes

- Related: GitHub issue #5.
- Found by: original roadmap; back-reference retrofitted 2026-09-07.
