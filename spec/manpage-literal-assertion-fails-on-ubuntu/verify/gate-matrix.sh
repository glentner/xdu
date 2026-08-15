#!/bin/sh
# gate-matrix.sh — the verify gate for P1 of spec/manpage-literal-assertion-fails-on-ubuntu.
#
# Proves that the committed CI assertion step reaches the same verdict regardless of which scdoc
# built the roff or where mandoc broke a line, still fires on real corruption, and diagnoses a
# failed render as a failed render.
#
# It tests CI's REAL code: the gate body is extracted from .github/workflows/test.yaml rather than
# copied here, so this harness cannot drift green while CI is red — the failure mode that produced
# the defect it is verifying.
#
# DOCKER IS REQUIRED, deliberately. The defect is a cross-toolchain skew (ubuntu-24.04 ships scdoc
# 1.11.2, which emits a bare roff `-`; 1.11.5 and later escape it), and a host-only check is exactly
# what let the gate be green for its author and red in CI for its whole life. There is no "SKIPPED"
# fallback: without docker this exits non-zero.
#
# Usage: sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/gate-matrix.sh
#   KEEP_WORK=1  keep the scratch tree and print its path (for debugging a red run)

set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
IMAGE=xdu-manpage-gate:ubuntu-24.04

WIDTH_MIN=40
WIDTH_MAX=200
PAD_MAX=150
PAD_STEP=3

WORK=$(mktemp -d "${TMPDIR:-/tmp}/xdu-gate-matrix.XXXXXX")
# Scratch this script created and disposes of itself: AGENTS.md keeps `rm` for exactly this case,
# because there is nothing here worth recovering and a trash move would accumulate hundreds of
# fixture trees in $HOME.
cleanup() {
	if [ -n "${KEEP_WORK:-}" ]; then
		echo "KEEP_WORK set — scratch retained at $WORK" >&2
	else
		rm -rf "$WORK"
	fi
}
trap cleanup EXIT

die() { echo "gate-matrix: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------------------------
# 1. Extract the gate body from the workflow — and refuse to run on a partial extraction.
# ---------------------------------------------------------------------------------------------
# Keyed on the step's name prefix and the indentation of its `run: |` block. If a future edit
# renames the step or reindents it, awk would silently yield a fragment and every case would run
# against a truncated script — a false green. The three assertions below make that a hard stop.
awk '
	/^      - name: Assert critical literals/ { instep = 1; next }
	instep && /^        run: \|/             { inbody = 1; next }
	inbody {
		if ($0 ~ /^[[:space:]]*$/) { print ""; next }
		if ($0 !~ /^          /)   { exit }
		sub(/^          /, ""); print
	}
' "$REPO/.github/workflows/test.yaml" > "$WORK/gate.sh"

GATE_LINES=$(wc -l < "$WORK/gate.sh" | tr -d ' ')
[ -s "$WORK/gate.sh" ] || die "extracted an EMPTY gate body from .github/workflows/test.yaml — did the step name or indentation change?"
[ "$GATE_LINES" -gt 20 ] || die "extracted only $GATE_LINES lines of gate body — expected the whole step; refusing to report a verdict on a fragment"
grep -q 'CORRUPT RENDER' "$WORK/gate.sh" || die "extracted gate body has no CORRUPT RENDER diagnostic — extraction is wrong, not the gate"

# ---------------------------------------------------------------------------------------------
# 2. Resolve both real scdocs. Never simulate one from the other.
# ---------------------------------------------------------------------------------------------
# 1.11.2 and 1.11.5 differ by 76-129 lines per page (.PP vs .P, and structurally different bullet
# lists), so roff faked from the other version measures an artifact. Both fixture sets come from a
# real scdoc of the version they are labelled with, and the two labels must differ — if they ever
# collide, the matrix would be testing one variant twice and reporting it as two.
command -v scdoc >/dev/null 2>&1 || die "no scdoc on the host (brew install scdoc)"
command -v mandoc >/dev/null 2>&1 || die "no mandoc on the host"
docker info >/dev/null 2>&1 || die "docker is not available — this gate is cross-toolchain by design and has no host-only fallback"

HOST_SCDOC=$(scdoc -v 2>&1 | awk '{print $2}')
HOST_LABEL="scdoc-$HOST_SCDOC"

docker build -q -t "$IMAGE" - >/dev/null <<'DOCKERFILE'
FROM ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends scdoc mandoc bsdextrautils \
 && rm -rf /var/lib/apt/lists/*
DOCKERFILE

CTR_SCDOC=$(docker run --rm "$IMAGE" scdoc -v 2>&1 | awk '{print $2}')
CTR_LABEL="scdoc-$CTR_SCDOC"
[ "$HOST_LABEL" != "$CTR_LABEL" ] || die "host and container both report $HOST_LABEL — the matrix would test one roff variant twice"

# ---------------------------------------------------------------------------------------------
# 3. Build the .scd variants (clean, four mutations, and the pad sweep).
# ---------------------------------------------------------------------------------------------
SCD="$WORK/fixtures/scd"
mkdir -p "$SCD/clean"
cp "$REPO"/doc/*.scd "$SCD/clean/"

new_variant() { mkdir -p "$SCD/$1"; cp "$REPO"/doc/*.scd "$SCD/$1/"; }

# The historical silent corruption: `\*` un-escaped, so scdoc reads it as bold markup and publishes
# `OUTDIR//.parquet` at exit 0 (doc/xdu.1.scd:113).
new_variant unescaped-glob
sed 's|/\\\*/\\\*\.parquet|/*/*.parquet|' "$SCD/clean/xdu.1.scd" > "$SCD/unescaped-glob/xdu.1.scd"

# Corrupt only the FIRST of the two `.partial suffix` occurrences: invisible to a presence check.
new_variant one-partial
awk 'BEGIN { done = 0 }
	{ if (!done && sub(/\.partial suffix/, "partial suffix")) done = 1; print }
' "$SCD/clean/xdu.1.scd" > "$SCD/one-partial/xdu.1.scd"

# Two independent corruptions on one page: drives the count assertion to zero (the grep-exits-1
# case) *and* breaks a presence literal, so the diagnostic count is the observable.
new_variant two-corruptions
sed -e 's|/\\\*/\\\*\.parquet|/*/*.parquet|' -e 's|\.partial suffix|partial suffix|g' \
	"$SCD/clean/xdu.1.scd" > "$SCD/two-corruptions/xdu.1.scd"

# A valid page carrying an ordinary `e.g. partial` sentence. Whitespace removal fuses it into
# `e.g.partial`, which the short needle `.partial` matches and the committed `.partial suffix`
# does not.
new_variant fusion
awk '
	{ print }
	/^completion\. Stale chunks from previous runs are automatically pruned\.$/ {
		print ""
		print "A run interrupted midway can leave behind, e.g. partial chunks from the"
		print "partition it was writing."
	}
' "$SCD/clean/xdu.1.scd" > "$SCD/fusion/xdu.1.scd"

# The pad sweep: shift the fill of the marker paragraph in 3-character steps, which is how an
# ordinary edit above the literal moves the line break onto it. Padding is whole two-letter words
# so the filler itself never becomes an unbreakable token.
p=0
while [ "$p" -le "$PAD_MAX" ]; do
	v="pad-$(printf '%03d' "$p")"
	new_variant "$v"
	awk -v n=$((p / 3)) '
		BEGIN { pad = ""; for (i = 0; i < n; i++) pad = pad "xx " }
		/^On success \*xdu\* writes a run-level completion marker,/ { print pad $0; next }
		{ print }
	' "$SCD/clean/xdu.1.scd" > "$SCD/$v/xdu.1.scd"
	p=$((p + PAD_STEP))
done

# Fixture sanity: a mutation that silently failed to apply would make its case pass vacuously.
grep -qF '_OUTDIR_/*/*.parquet' "$SCD/unescaped-glob/xdu.1.scd" || die "unescaped-glob mutation did not apply"
[ "$(grep -cF '.partial suffix' "$SCD/one-partial/xdu.1.scd")" -eq 1 ] || die "one-partial mutation did not leave exactly one occurrence"
[ "$(grep -cF '.partial suffix' "$SCD/two-corruptions/xdu.1.scd")" -eq 0 ] || die "two-corruptions mutation left a .partial suffix behind"
grep -qF 'e.g. partial chunks' "$SCD/fusion/xdu.1.scd" || die "fusion mutation did not apply"
grep -q '^xx xx xx On success' "$SCD/pad-009/xdu.1.scd" || die "pad mutation did not apply"
NVARIANT=$(ls "$SCD" | wc -l | tr -d ' ')

# ---------------------------------------------------------------------------------------------
# 4. Render every variant with BOTH real scdocs.
# ---------------------------------------------------------------------------------------------
ROFF="$WORK/fixtures/roff"
mkdir -p "$ROFF/$HOST_LABEL" "$ROFF/$CTR_LABEL"

for v in "$SCD"/*; do
	vn=$(basename "$v")
	mkdir -p "$ROFF/$HOST_LABEL/$vn"
	for scd in "$v"/*.scd; do
		name=$(basename "$scd" .1.scd)
		scdoc < "$scd" > "$ROFF/$HOST_LABEL/$vn/$name.1"
	done
done

# The container gets the sources read-only and can write ONLY into its own output directory, so it
# cannot regenerate the host fixtures with its own scdoc (an earlier attempt did exactly that and
# silently measured 1.11.2 twice).
docker run --rm \
	-v "$SCD:/scd:ro" \
	-v "$ROFF/$CTR_LABEL:/out" \
	"$IMAGE" sh -c '
		set -eu
		for v in /scd/*; do
			vn=$(basename "$v")
			mkdir -p "/out/$vn"
			for scd in "$v"/*.scd; do
				name=$(basename "$scd" .1.scd)
				scdoc < "$scd" > "/out/$vn/$name.1"
			done
		done
	'

# The two roff sets must actually differ, or one scdoc silently produced the other's output.
if cmp -s "$ROFF/$HOST_LABEL/clean/xdu.1" "$ROFF/$CTR_LABEL/clean/xdu.1"; then
	die "the $HOST_LABEL and $CTR_LABEL renders of xdu.1 are byte-identical — the fixtures are not two variants"
fi
ROFF_DELTA=$(diff "$ROFF/$HOST_LABEL/clean/xdu.1" "$ROFF/$CTR_LABEL/clean/xdu.1" | grep -c '^[<>]' || true)

# ---------------------------------------------------------------------------------------------
# 5. Run the identical case suite on both platforms.
# ---------------------------------------------------------------------------------------------
HOST_PLATFORM="host-$(uname -s | tr 'A-Z' 'a-z')"
sh "$HERE/run-cases.sh" "$WORK/fixtures" "$WORK/gate.sh" "$WORK/scratch-host" \
	"$HOST_PLATFORM" "$HOST_LABEL" "$CTR_LABEL" > "$WORK/results-host.txt"

docker run --rm \
	-v "$WORK/fixtures:/fixtures:ro" \
	-v "$WORK/gate.sh:/gate.sh:ro" \
	-v "$HERE/run-cases.sh:/run-cases.sh:ro" \
	"$IMAGE" sh /run-cases.sh /fixtures /gate.sh /scratch ubuntu-24.04 "$HOST_LABEL" "$CTR_LABEL" \
	> "$WORK/results-ctr.txt"

cat "$WORK/results-host.txt" "$WORK/results-ctr.txt" > "$WORK/results.txt"

# ---------------------------------------------------------------------------------------------
# 6. Report one PASS/FAIL line per R-ID.
# ---------------------------------------------------------------------------------------------
# Each R-ID declares how many cases it must contribute. A count mismatch is a FAIL even when every
# line present says PASS: a phase bundling six R-IDs must not be able to go green because a case
# never ran.
echo "=== gate matrix: manpage-literal-assertion-fails-on-ubuntu P1 ==="
echo "gate      : .github/workflows/test.yaml 'Assert critical literals…' — $GATE_LINES lines extracted"
echo "host      : $HOST_PLATFORM · $(uname -sr) · $HOST_LABEL"
echo "container : ubuntu-24.04 · $CTR_LABEL"
echo "fixtures  : $NVARIANT variants × 4 pages × 2 real scdocs (clean xdu.1 roff differs by $ROFF_DELTA lines between them)"
echo "sweeps    : widths $WIDTH_MIN..$WIDTH_MAX · pads 0..$PAD_MAX step $PAD_STEP at the default width"
echo

awk -F'|' -v expected="R1=2 R2=2 R3=4 R4=8 R5=8 R7=16 SHELL=8" '
	BEGIN {
		n = split(expected, kv, " ")
		for (i = 1; i <= n; i++) { split(kv[i], p, "="); want[p[1]] = p[2]; order[i] = p[1] }
		norder = n
		desc["R1"]    = "clean tree passes with the scdoc ubuntu-24.04 ships"
		desc["R2"]    = "clean tree passes with a hyphen-escaping scdoc (local == CI)"
		desc["R3"]    = "mis-escaped markup published at scdoc exit 0 is caught, by name"
		desc["R4"]    = "verdict independent of render width, break position and tab indent"
		desc["R5"]    = "a page that did not render is reported as that, not as corrupt literals"
		desc["R7"]    = "single-occurrence corruption caught; fusion no false-red; N parses at any width; NO uncounted duplicate anywhere"
		desc["SHELL"] = "same diagnostics under bash -e and bash -eo pipefail"
	}
	$1 == "RESULT" { seen[$2]++; if ($6 == "FAIL") { bad[$2]++; fails[++nf] = $0 } detail[$2] = detail[$2] sprintf("         %-4s %-13s %-14s %-30s %s\n", $6, $3, $4, $5, $7) }
	$1 == "INFO"   { infos[++ni] = $2 ": " $3 }
	END {
		rc = 0
		for (i = 1; i <= norder; i++) {
			r = order[i]
			if (seen[r] != want[r]) {
				printf "%-6s FAIL  %d of %d cases ran — %s\n", r, seen[r] + 0, want[r], desc[r]
				rc = 1
			} else if (bad[r] > 0) {
				printf "%-6s FAIL  %d/%d cases red — %s\n", r, bad[r], seen[r], desc[r]
				rc = 1
			} else {
				printf "%-6s PASS  %d/%d cases green — %s\n", r, seen[r], want[r], desc[r]
			}
			printf "%s", detail[r]
		}
		print ""
		for (i = 1; i <= ni; i++) print "note: " infos[i]
		if (nf > 0) {
			print ""
			print "=== failures ==="
			for (i = 1; i <= nf; i++) print fails[i]
		}
		print ""
		if (rc == 0) print "GATE-MATRIX-OK: every R-ID green on both platforms and both roff variants"
		else         print "GATE-MATRIX-FAILED"
		exit rc
	}
' "$WORK/results.txt"
