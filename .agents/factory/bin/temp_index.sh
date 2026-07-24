#!/bin/sh
# SPDX-FileCopyrightText: 2026 Geoffrey Lentner
# SPDX-License-Identifier: MIT
#
# Run a command against a throwaway xdu index so factory `verify:` commands and review
# CLI drives never touch a real filesystem or a real index. Builds a tiny deterministic
# fixture tree, indexes it with the freshly-built `xdu`, puts the release binaries on
# PATH, exports XDU_INDEX (+ XDU_JOBS), and runs "$@" with the working directory inside
# the throwaway site. The site is removed on exit (any exit path). This is the xdu
# analogue of HyperShell's factory `temp_site.sh`.
#
# Usage:
#   .agents/factory/bin/temp_index.sh xdu-find --count
#   .agents/factory/bin/temp_index.sh sh -c 'xdu-find -i "$XDU_INDEX" -u alice --count'
#   .agents/factory/bin/temp_index.sh sh -c 'xdu-rm --dry-run -p "\.log$" --force'
#
# Fixture layout (partitions = top-level subdirs; loose files -> the __root__ partition):
#   alice/logs/app.log (~2K) · alice/data.bin (~4K) · bob/notes.txt (~1K) · root.dat (~512, loose)
# Sizes are disk-usage (block-rounded) unless a drive passes --apparent-size; treat them
# as approximate — this helper exercises flows, it is not a byte-exact fixture.
#
# Callers invoke this from the repo root (it needs target/release and cargo there).
set -eu

root="$(pwd)"

# Release binaries are the source of truth for a verify drive; build once if absent.
bindir="$root/target/release"
if [ ! -x "$bindir/xdu" ] || [ ! -x "$bindir/xdu-find" ] \
   || [ ! -x "$bindir/xdu-rm" ] || [ ! -x "$bindir/xdu-view" ]; then
  echo "temp_index.sh: building release binaries (one-time)…" >&2
  ( cd "$root" && cargo build --release --bins >&2 )
fi

site="$(mktemp -d "${TMPDIR:-/tmp}/xdu-temp-index.XXXXXX")"
trap 'rm -rf "$site"' EXIT INT TERM

# Deterministic fixture tree.
tree="$site/tree"
mkdir -p "$tree/alice/logs" "$tree/bob"
head -c 2048 /dev/zero > "$tree/alice/logs/app.log"
head -c 4096 /dev/zero > "$tree/alice/data.bin"
head -c 1024 /dev/zero > "$tree/bob/notes.txt"
head -c  512 /dev/zero > "$tree/root.dat"

index="$site/index"
"$bindir/xdu" "$tree" -o "$index" -j "${XDU_JOBS:-2}" >&2

# Export the throwaway index + put release binaries first on PATH, so a drive can call
# `xdu-find`/`xdu-rm`/`xdu-view` bare and omit `-i` (they read XDU_INDEX).
XDU_INDEX="$index"
XDU_JOBS="${XDU_JOBS:-2}"
PATH="$bindir:$PATH"
export XDU_INDEX XDU_JOBS PATH

# Run inside the site so relative writes in a drive stay contained instead of leaking
# into the working tree (where an `xdu-build`-style `git add -A` would sweep them up).
cd "$site"
"$@"
