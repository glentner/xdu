#!/bin/sh
# job-sim.sh — the verify gate for P3 of spec/manpage-literal-assertion-fails-on-ubuntu.
#
# Runs the packaging job's man-page half end to end on CI's real image: `ubuntu:24.04`, CI's apt
# packages, a clean `git archive` of the branch, and the workflow's own render and assertion steps
# extracted verbatim from .github/workflows/test.yaml. P1's gate-matrix.sh proves the assertion body
# is correct across a fixture matrix; this proves the whole job is green on the tree as committed.
#
# Nothing is retyped. Both step bodies come out of the workflow by awk, so this cannot pass against
# a script CI does not run — the failure mode that produced the defect being fixed.
#
# DOCKER IS REQUIRED and there is no host-only fallback: the point is CI's toolchain.
#
# Usage: sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/job-sim.sh
#   KEEP_WORK=1  keep the scratch tree and print its path

set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
IMAGE=xdu-manpage-gate:ubuntu-24.04

WORK=$(mktemp -d "${TMPDIR:-/tmp}/xdu-job-sim.XXXXXX")
# Scratch this script created and disposes of itself — AGENTS.md keeps `rm` for exactly this case.
cleanup() {
	if [ -n "${KEEP_WORK:-}" ]; then
		echo "KEEP_WORK set — scratch retained at $WORK" >&2
	else
		rm -rf "$WORK"
	fi
}
trap cleanup EXIT

die() { echo "job-sim: $*" >&2; exit 1; }

docker info >/dev/null 2>&1 || die "docker is not available — this gate simulates CI's image and has no host-only fallback"

# ---------------------------------------------------------------------------------------------
# 1. Export the branch exactly as CI would check it out.
# ---------------------------------------------------------------------------------------------
# `git archive` sees HEAD, not the working tree, so an uncommitted edit to either input would make
# this simulation quietly test something other than what it reports on. Refuse instead.
cd "$REPO"
git diff --quiet HEAD -- .github/workflows/test.yaml doc \
	|| die "uncommitted changes in .github/workflows/test.yaml or doc/ — this gate simulates the COMMITTED branch; commit them first"

mkdir -p "$WORK/src"
git archive HEAD | tar -x -C "$WORK/src"
[ -f "$WORK/src/.github/workflows/test.yaml" ] || die "git archive produced no workflow file"
NSCD=$(ls "$WORK/src"/doc/*.scd 2>/dev/null | wc -l | tr -d ' ')
[ "$NSCD" -ge 4 ] || die "git archive produced only $NSCD doc/*.scd — expected the whole tree"

# ---------------------------------------------------------------------------------------------
# 2. Extract the two step bodies from the archived workflow, verbatim.
# ---------------------------------------------------------------------------------------------
extract_step() { # $1 = name prefix regex, $2 = output file
	awk -v want="$1" '
		$0 ~ "^      - name: " want { instep = 1; next }
		instep && /^        run: \|/ { inbody = 1; next }
		inbody {
			if ($0 ~ /^[[:space:]]*$/) { print ""; next }
			if ($0 !~ /^          /)   { exit }
			sub(/^          /, ""); print
		}
	' "$WORK/src/.github/workflows/test.yaml" > "$2"
}

extract_step "Render man pages" "$WORK/step-render.sh"
extract_step "Assert critical literals" "$WORK/step-assert.sh"

RENDER_LINES=$(wc -l < "$WORK/step-render.sh" | tr -d ' ')
ASSERT_LINES=$(wc -l < "$WORK/step-assert.sh" | tr -d ' ')
[ -s "$WORK/step-render.sh" ] || die "extracted an EMPTY render step — did its name or indentation change?"
[ -s "$WORK/step-assert.sh" ] || die "extracted an EMPTY assertion step — did its name or indentation change?"
grep -q 'scdoc' "$WORK/step-render.sh" || die "extracted render step does not call scdoc — extraction is wrong"
[ "$ASSERT_LINES" -gt 20 ] || die "extracted only $ASSERT_LINES lines of assertion step — refusing to report on a fragment"
grep -q 'CORRUPT RENDER' "$WORK/step-assert.sh" || die "extracted assertion step has no CORRUPT RENDER diagnostic — extraction is wrong"

# ---------------------------------------------------------------------------------------------
# 3. Run them in CI's image, with CI's packages.
# ---------------------------------------------------------------------------------------------
docker build -q -t "$IMAGE" - >/dev/null <<'DOCKERFILE'
FROM ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends scdoc mandoc bsdextrautils \
 && rm -rf /var/lib/apt/lists/*
DOCKERFILE

set +e
OUT=$(docker run --rm \
	-v "$WORK/src:/src:ro" \
	-v "$WORK/step-render.sh:/step-render.sh:ro" \
	-v "$WORK/step-assert.sh:/step-assert.sh:ro" \
	"$IMAGE" bash -e -c '
		set -eu
		# CI checks out into a writable workspace; the mount is read-only so copy first.
		cp -R /src /work && cd /work

		echo "SIM: scdoc $(scdoc -v 2>&1 | awk "{print \$2}") · $(mandoc -V 2>&1 | head -1 | cut -c1-30)"

		echo "SIM: --- step: Render man pages ---"
		bash -e /step-render.sh
		echo "SIM: render step exit 0"

		echo "SIM: --- step: Assert critical literals ---"
		bash -e /step-assert.sh
		echo "SIM: assert step exit 0"

		# Post-conditions, asserted separately from the steps own exit status.
		for p in xdu xdu-find xdu-view xdu-rm; do
			[ -s "share/man/man1/$p.1" ] || { echo "SIM: MISSING PAGE $p.1" >&2; exit 1; }
		done
		echo "SIM: all four pages produced non-empty"

		# The source-side roff-control tripwire, run standalone so its verdict is visible even
		# though the assertion step already contains it.
		if grep -nE "^[[:space:]]*['"'"'.]" doc/*.scd; then
			echo "SIM: TRIPWIRE FIRED" >&2; exit 1
		fi
		echo "SIM: source-side roff-control tripwire clean"
	' 2>&1)
RC=$?
set -e

# ---------------------------------------------------------------------------------------------
# 4. Report. Exit 0 is necessary but not sufficient — the OK line and each post-condition must
#    actually appear, or a step that silently did nothing would read as a pass.
# ---------------------------------------------------------------------------------------------
echo "=== packaging-job simulation: ubuntu:24.04, clean git archive of $(git rev-parse --short HEAD) ==="
echo "render step : $RENDER_LINES lines extracted from the workflow"
echo "assert step : $ASSERT_LINES lines extracted from the workflow"
echo
printf '%s\n' "$OUT" | sed 's/^/  /'
echo

fail=0
check() { # description, needle
	if printf '%s\n' "$OUT" | grep -qF -- "$2"; then
		printf 'PASS  %s\n' "$1"
	else
		printf 'FAIL  %s (expected %s)\n' "$1" "$2"
		fail=1
	fi
}
[ "$RC" -eq 0 ] && printf 'PASS  the job exited 0\n' || { printf 'FAIL  the job exited %s\n' "$RC"; fail=1; }
check "the render step completed"                    "SIM: render step exit 0"
check "the assertion step completed"                 "SIM: assert step exit 0"
check "the assertion printed its OK line"            "OK: every asserted literal survived into the published man-page text"
check "all four man pages were produced non-empty"   "SIM: all four pages produced non-empty"
check "the source-side roff-control tripwire is clean" "SIM: source-side roff-control tripwire clean"
if printf '%s\n' "$OUT" | grep -q 'CORRUPT RENDER\|RENDER FAILED\|RENDER EMPTY'; then
	printf 'FAIL  the job emitted a corruption diagnostic\n'
	fail=1
else
	printf 'PASS  no corruption diagnostic was emitted\n'
fi

echo
if [ "$fail" -eq 0 ]; then
	echo "JOB-SIM-OK: the packaging job's man-page half is green on CI's image, from the committed tree"
else
	echo "JOB-SIM-FAILED"
	exit 1
fi
