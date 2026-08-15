#!/bin/sh
# doc-parity.sh — the verify gate for P2 of spec/manpage-literal-assertion-fails-on-ubuntu.
#
# Asserts that the man-page literal check AGENTS.md tells a maintainer to run locally reaches the
# SAME verdict as the assertion step CI runs. That is the whole of R6: before this fix the two
# disagreed, so the gate was green for its author on homebrew scdoc 1.11.5 and red in CI on
# ubuntu-24.04's 1.11.2, from one source tree.
#
# Nothing here is transcribed. The documented snippet is extracted from AGENTS.md, the gate body
# from .github/workflows/test.yaml, and the literal list from the gate itself — so there is no
# second copy of any of the three to drift, and a doc edit that breaks parity fails this gate.
#
# DOCKER IS REQUIRED: "local agrees with CI" is only meaningful across the two real scdocs, and
# ubuntu-24.04's cannot be installed on the host. There is no host-only fallback.
#
# Usage: sh spec/manpage-literal-assertion-fails-on-ubuntu/verify/doc-parity.sh
#   KEEP_WORK=1  keep the scratch tree and print its path

set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
IMAGE=xdu-manpage-gate:ubuntu-24.04

WORK=$(mktemp -d "${TMPDIR:-/tmp}/xdu-doc-parity.XXXXXX")
# Scratch this script created and disposes of itself — AGENTS.md keeps `rm` for exactly this case.
cleanup() {
	if [ -n "${KEEP_WORK:-}" ]; then
		echo "KEEP_WORK set — scratch retained at $WORK" >&2
	else
		rm -rf "$WORK"
	fi
}
trap cleanup EXIT

die() { echo "doc-parity: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------------------------
# 1. Extract the three inputs, refusing to report a verdict on a bad extraction.
# ---------------------------------------------------------------------------------------------
# (a) The snippet AGENTS.md documents: from its `lit=` line through the `MISSING:` report.
awk '
	/^lit=/ { inblk = 1 }
	inblk   { print }
	inblk && /MISSING: \$lit/ { exit }
' "$REPO/AGENTS.md" > "$WORK/doccheck.tmpl"

DOC_LINES=$(wc -l < "$WORK/doccheck.tmpl" | tr -d ' ')
[ -s "$WORK/doccheck.tmpl" ] || die "found no documented literal check in AGENTS.md — expected a 'lit=' snippet in the Commands section"
[ "$DOC_LINES" -ge 3 ] || die "documented snippet is only $DOC_LINES lines — refusing to judge parity on a fragment"
grep -q 'col -b' "$WORK/doccheck.tmpl" || die "documented snippet does not render the page (no 'col -b')"
grep -q 'grep -qF' "$WORK/doccheck.tmpl" || die "documented snippet does not match a literal (no 'grep -qF')"
# Occurrences, not matching lines: the page-side and literal-side strips may share a line, and
# `grep -c` would then report 1 and refuse a perfectly good snippet. This is only a smoke test —
# a snippet that normalizes the right number of times in the wrong places is caught below, by the
# parity comparison itself.
NNORM=$(grep -oF "tr -d '[:space:]'" "$WORK/doccheck.tmpl" | wc -l | tr -d ' ')
[ "$NNORM" -ge 2 ] \
	|| die "documented snippet strips whitespace $NNORM time(s) — the page AND the literal must be normalized"

# (a2) The documented COUNT snippet: from its `lit=…; want=` line through the `MISCOUNT:` report.
# A presence check is satisfied by whichever copy survived, so it structurally cannot reproduce
# CI's verdict for a literal the gate counts. Its absence from AGENTS.md is the defect this
# extraction exists to make un-missable — refusing here is the point, not an inconvenience.
awk '
	/^lit=.*want=/ { inblk = 1 }
	inblk          { print }
	inblk && /MISCOUNT:/ { exit }
' "$REPO/AGENTS.md" > "$WORK/doccount.tmpl"

CNT_LINES=$(wc -l < "$WORK/doccount.tmpl" | tr -d ' ')
[ -s "$WORK/doccount.tmpl" ] \
	|| die "AGENTS.md documents no COUNT form — a presence check cannot predict CI for a literal the gate asserts as 'Nx:'"
[ "$CNT_LINES" -ge 3 ] || die "documented count snippet is only $CNT_LINES lines — refusing to judge parity on a fragment"
grep -q 'grep -oF' "$WORK/doccount.tmpl" || die "documented count snippet does not count occurrences (no 'grep -oF')"
CNORM=$(grep -oF "tr -d '[:space:]'" "$WORK/doccount.tmpl" | wc -l | tr -d ' ')
[ "$CNORM" -ge 2 ] \
	|| die "documented count snippet strips whitespace $CNORM time(s) — the page AND the literal must be normalized"

# (b) The live gate body — same extraction and same refusals as gate-matrix.sh.
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
[ -s "$WORK/gate.sh" ] || die "extracted an EMPTY gate body — did the step name or indentation change?"
[ "$GATE_LINES" -gt 20 ] || die "extracted only $GATE_LINES lines of gate body — refusing to judge parity on a fragment"
grep -q 'CORRUPT RENDER' "$WORK/gate.sh" || die "extracted gate body has no CORRUPT RENDER diagnostic — extraction is wrong"

# (c) The literals, taken from the gate's own xdu.1 call so this script owns no list of its own.
# The `Nx:` count prefix is stripped: parity is about whether the two sides FIND the literal, and
# the documented one-literal snippet has no notion of an occurrence count.
awk '
	/^check share\/man\/man1\/xdu\.1/ { inlist = 1; next }
	inlist && /^check /               { exit }
	inlist {
		line = $0
		sub(/^[[:space:]]+/, "", line)
		sub(/[[:space:]]*\\$/, "", line)
		if (line ~ /^'"'"'.*'"'"'$/) {
			gsub(/^'"'"'|'"'"'$/, "", line)
			sub(/^[0-9]+x:/, "", line)
			print line
		}
	}
' "$WORK/gate.sh" > "$WORK/literals.txt"

NLIT=$(wc -l < "$WORK/literals.txt" | tr -d ' ')
[ "$NLIT" -ge 5 ] || die "extracted only $NLIT literals from the gate's xdu.1 check — expected the whole list"

# (c2) The COUNTED literals, with their expected N — same source, prefix kept this time. Emitted as
# `N|literal`. If the gate ever stops counting anything this is empty and the count parity below
# becomes vacuous, so the runner asserts non-emptiness rather than skipping silently.
awk '
	/^check share\/man\/man1\/xdu\.1/ { inlist = 1; next }
	inlist && /^check /               { exit }
	inlist {
		line = $0
		sub(/^[[:space:]]+/, "", line)
		sub(/[[:space:]]*\\$/, "", line)
		if (line ~ /^'"'"'[0-9]+x:.*'"'"'$/) {
			gsub(/^'"'"'|'"'"'$/, "", line)
			n = line; sub(/x:.*$/, "", n)
			sub(/^[0-9]+x:/, "", line)
			print n "|" line
		}
	}
' "$WORK/gate.sh" > "$WORK/counts.txt"

NCNT=$(wc -l < "$WORK/counts.txt" | tr -d ' ')
[ "$NCNT" -ge 1 ] \
	|| die "the gate asserts no 'Nx:' counted literal — count parity would be vacuous"

# ---------------------------------------------------------------------------------------------
# 2. Both real scdocs, both fixtures.
# ---------------------------------------------------------------------------------------------
command -v scdoc >/dev/null 2>&1 || die "no scdoc on the host (brew install scdoc)"
command -v mandoc >/dev/null 2>&1 || die "no mandoc on the host"
docker info >/dev/null 2>&1 || die "docker is not available — parity is cross-toolchain by design and has no host-only fallback"

HOST_LABEL="scdoc-$(scdoc -v 2>&1 | awk '{print $2}')"

docker build -q -t "$IMAGE" - >/dev/null <<'DOCKERFILE'
FROM ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends scdoc mandoc bsdextrautils \
 && rm -rf /var/lib/apt/lists/*
DOCKERFILE

CTR_LABEL="scdoc-$(docker run --rm "$IMAGE" scdoc -v 2>&1 | awk '{print $2}')"
[ "$HOST_LABEL" != "$CTR_LABEL" ] || die "host and container both report $HOST_LABEL — there is no skew left to test"

# `clean` must agree because nothing is wrong; `unescaped-glob` must agree because something is, and
# a check that never fires would agree with everything. `one-partial` corrupts exactly ONE of the two
# `.partial suffix` occurrences — the case a presence check is blind to by construction, so it is the
# variant that separates "the local check normalizes correctly" from "the local check predicts CI".
VARIANTS='clean unescaped-glob one-partial'
for LBL in "$HOST_LABEL" "$CTR_LABEL"; do
	for V in $VARIANTS; do
		mkdir -p "$WORK/t/$LBL/$V/doc" "$WORK/t/$LBL/$V/share/man/man1"
		cp "$REPO"/doc/*.scd "$WORK/t/$LBL/$V/doc/"
	done
	sed 's|/\\\*/\\\*\.parquet|/*/*.parquet|' "$REPO/doc/xdu.1.scd" \
		> "$WORK/t/$LBL/unescaped-glob/doc/xdu.1.scd"
	grep -qF '_OUTDIR_/*/*.parquet' "$WORK/t/$LBL/unescaped-glob/doc/xdu.1.scd" \
		|| die "unescaped-glob mutation did not apply for $LBL"
	awk '{ if (!done && sub(/\.partial suffix/, "partial suffix")) done = 1; print }' \
		"$REPO/doc/xdu.1.scd" > "$WORK/t/$LBL/one-partial/doc/xdu.1.scd"
	[ "$(grep -cF '.partial suffix' "$WORK/t/$LBL/one-partial/doc/xdu.1.scd")" -eq 1 ] \
		|| die "one-partial mutation did not leave exactly one occurrence for $LBL"
done

# Render each tree's pages with the scdoc that tree is labelled with. The container writes only
# into its own label's tree and never sees the host's.
for V in $VARIANTS; do
	for scd in "$WORK/t/$HOST_LABEL/$V/doc"/*.scd; do
		scdoc < "$scd" > "$WORK/t/$HOST_LABEL/$V/share/man/man1/$(basename "$scd" .1.scd).1"
	done
done
docker run --rm -v "$WORK/t/$CTR_LABEL:/t" "$IMAGE" sh -c '
	set -eu
	for v in /t/*; do
		for scd in "$v"/doc/*.scd; do
			scdoc < "$scd" > "$v/share/man/man1/$(basename "$scd" .1.scd).1"
		done
	done
'

# ---------------------------------------------------------------------------------------------
# 3. The per-platform runner: documented check vs gate, one row per (variant, literal).
# ---------------------------------------------------------------------------------------------
# Emits ROW|<label>|<variant>|<literal>|<doc verdict>|<gate verdict>|<un-normalized verdict>.
# The last column is a control: the form AGENTS.md used to document (no whitespace stripping). It
# must DISAGREE with the gate somewhere, or this script is not discriminating and its agreement
# columns prove nothing.
cat > "$WORK/parity-run.sh" <<'RUNNER'
#!/bin/sh
set -u
ROOT="$1"; LABEL="$2"; SCRATCH="$3"
mkdir -p "$SCRATCH"
for V in clean unescaped-glob one-partial; do
	D="$ROOT/t/$LABEL/$V"
	GOUT=$(cd "$D" && bash -e "$ROOT/gate.sh" 2>&1) || true
	while IFS= read -r L; do
		# The documented snippet, verbatim except for the literal it is parameterized by.
		{ printf "lit='%s'\n" "$L"; tail -n +2 "$ROOT/doccheck.tmpl"; } > "$SCRATCH/dc.sh"
		DOUT=$(cd "$D" && sh "$SCRATCH/dc.sh" 2>&1)
		case "$DOUT" in *"MISSING:"*) DV=missing ;; *) DV=present ;; esac

		# The gate's verdict for this same literal, read off its diagnostics.
		GV=present
		case "$GOUT" in
			*"missing the literal: $L"*)               GV=missing ;;
			*"has 0 occurrence(s) of the literal '$L'"*) GV=missing ;;
		esac

		# Control: the pre-fix documented form — render, but do not normalize either side.
		if (cd "$D" && scdoc < doc/xdu.1.scd | mandoc -Tutf8 | col -b | grep -qF -- "$L"); then
			UV=present
		else
			UV=missing
		fi

		printf 'ROW|%s|%s|%s|%s|%s|%s\n' "$LABEL" "$V" "$L" "$DV" "$GV" "$UV"
	done < "$ROOT/literals.txt"

	# Counted literals: the documented COUNT snippet against the gate's count diagnostic. On the
	# `one-partial` variant the presence rows above legitimately AGREE on "present" — one copy
	# survived — so only these rows can tell whether a maintainer would see what CI sees.
	while IFS='|' read -r WANT L; do
		[ -n "$L" ] || continue
		{ printf "lit='%s'; want=%s\n" "$L" "$WANT"; tail -n +2 "$ROOT/doccount.tmpl"; } > "$SCRATCH/dn.sh"
		NOUT=$(cd "$D" && sh "$SCRATCH/dn.sh" 2>&1)
		case "$NOUT" in *"MISCOUNT:"*) NV=wrong ;; *) NV=ok ;; esac

		GN=ok
		case "$GOUT" in
			*"occurrence(s) of the literal '$L', expected"*) GN=wrong ;;
		esac

		printf 'CROW|%s|%s|%s|%s|%s|%s\n' "$LABEL" "$V" "$L" "$WANT" "$NV" "$GN"
	done < "$ROOT/counts.txt"
done
RUNNER

sh "$WORK/parity-run.sh" "$WORK" "$HOST_LABEL" "$WORK/scratch-host" > "$WORK/rows.txt"
docker run --rm -v "$WORK:/w:ro" -v "$WORK/scratch-ctr:/scratch" "$IMAGE" \
	sh /w/parity-run.sh /w "$CTR_LABEL" /scratch >> "$WORK/rows.txt"

# ---------------------------------------------------------------------------------------------
# 4. Report.
# ---------------------------------------------------------------------------------------------
NVAR=$(set -- $VARIANTS; echo $#)

echo "=== doc/CI parity: AGENTS.md's documented check vs the committed gate ==="
echo "documented : AGENTS.md 'Commands' — $DOC_LINES lines presence + $CNT_LINES lines count"
echo "gate       : .github/workflows/test.yaml 'Assert critical literals…' — $GATE_LINES lines extracted"
echo "literals   : $NLIT ($NCNT counted), taken from the gate itself (no second list to drift)"
echo "variants   : $NVAR — $VARIANTS"
echo "toolchains : host $HOST_LABEL · container $CTR_LABEL (ubuntu-24.04, what CI installs)"
echo

awk -F'|' -v nlit="$NLIT" -v ncnt="$NCNT" -v nvar="$NVAR" \
    -v hostlbl="$HOST_LABEL" -v ctrlbl="$CTR_LABEL" '
	$1 == "ROW" {
		key = $3 "|" $4
		doc[$2 "|" key] = $5
		gate[$2 "|" key] = $6
		unnorm[$2 "|" key] = $7
		if (!(key in seenkey)) { seenkey[key] = ++nkeys; keys[nkeys] = key }
	}
	$1 == "CROW" {
		ckey = $3 "|" $4
		cwant[ckey] = $5
		cdoc[$2 "|" ckey] = $6
		cgate[$2 "|" ckey] = $7
		if (!(ckey in cseen)) { cseen[ckey] = ++nckeys; ckeys[nckeys] = ckey }
	}
	function mark(v) { return (v == "present") ? "ok" : "MISS" }
	function cmark(v) { return (v == "ok") ? "ok" : "WRONG" }
	END {
		printf "%-15s %-38s  %-6s %-6s %-6s %-6s   %-6s %-6s  %s\n", \
			"variant", "literal", "doc@h", "doc@d", "gate@h", "gate@d", "raw@h", "raw@d", "verdict"
		rows = 0; bad = 0; unnorm_vs_gate = 0; unnorm_split = 0
		for (i = 1; i <= nkeys; i++) {
			split(keys[i], p, "|")
			dh = doc[hostlbl "|" keys[i]];    dd = doc[ctrlbl "|" keys[i]]
			gh = gate[hostlbl "|" keys[i]];   gd = gate[ctrlbl "|" keys[i]]
			uh = unnorm[hostlbl "|" keys[i]]; ud = unnorm[ctrlbl "|" keys[i]]
			rows++
			if (dh == "" || dd == "" || gh == "" || gd == "" || uh == "" || ud == "") {
				printf "%-15s %-38s  %s\n", p[1], p[2], "MISSING-DATA"
				bad++
				continue
			}
			# The decisive comparison is doc@h vs gate@d: what a maintainer runs on their own box
			# against what CI will say. The other two columns make it four-way.
			v = (dh == dd && dd == gh && gh == gd) ? "AGREE" : "DISAGREE"
			if (v == "DISAGREE") bad++
			# Controls: the pre-fix un-normalized form, compared with the gate on the SAME toolchain.
			if (uh != gh) unnorm_vs_gate++
			if (ud != gd) unnorm_vs_gate++
			if (uh != ud) unnorm_split++
			printf "%-15s %-38s  %-6s %-6s %-6s %-6s   %-6s %-6s  %s\n", \
				p[1], p[2], mark(dh), mark(dd), mark(gh), mark(gd), mark(uh), mark(ud), v
		}
		print ""

		# --- count parity: the rows a presence check structurally cannot produce -----------------
		printf "%-15s %-38s  %-6s %-6s %-6s %-6s  %s\n", \
			"variant", "counted literal (want N)", "cnt@h", "cnt@d", "gate@h", "gate@d", "verdict"
		crows = 0; cbad = 0; cfired = 0
		for (i = 1; i <= nckeys; i++) {
			split(ckeys[i], q, "|")
			nh = cdoc[hostlbl "|" ckeys[i]];  nd = cdoc[ctrlbl "|" ckeys[i]]
			kh = cgate[hostlbl "|" ckeys[i]]; kd = cgate[ctrlbl "|" ckeys[i]]
			crows++
			if (nh == "" || nd == "" || kh == "" || kd == "") {
				printf "%-15s %-38s  %s\n", q[1], q[2], "MISSING-DATA"
				cbad++
				continue
			}
			cv = (nh == nd && nd == kh && kh == kd) ? "AGREE" : "DISAGREE"
			if (cv == "DISAGREE") cbad++
			if (kh == "wrong" || kd == "wrong") cfired++
			printf "%-15s %-38s  %-6s %-6s %-6s %-6s  %s\n", \
				q[1], sprintf("%s (%s)", q[2], cwant[ckeys[i]]), \
				cmark(nh), cmark(nd), cmark(kh), cmark(kd), cv
		}
		print ""

		want = nlit * nvar
		cwantrows = ncnt * nvar
		rc = 0
		if (rows != want) {
			printf "R6     FAIL  %d of %d (variant x literal) rows produced — cases are missing\n", rows, want
			rc = 1
		} else if (bad > 0) {
			printf "R6     FAIL  %d/%d rows disagree — the documented local check does not predict CI\n", bad, rows
			rc = 1
		} else {
			printf "R6     PASS  %d/%d rows agree across all four columns (doc@h == doc@d == gate@h == gate@d)\n", rows, rows
		}
		if (crows != cwantrows) {
			printf "R6cnt  FAIL  %d of %d (variant x counted-literal) rows produced — cases are missing\n", crows, cwantrows
			rc = 1
		} else if (cbad > 0) {
			printf "R6cnt  FAIL  %d/%d count rows disagree — the documented COUNT form does not predict CI\n", cbad, crows
			rc = 1
		} else {
			printf "R6cnt  PASS  %d/%d count rows agree — single-occurrence corruption reads the same locally and in CI\n", crows, crows
		}
		# Anti-vacuity for the count rows: if the gate never reported a wrong count, the one-partial
		# mutation did not land and every count row agreed on "ok" for free.
		if (cfired > 0) {
			printf "CTRL   PASS  the gate reported a wrong count on %d count row(s) — the counted case is live, not vacuous\n", cfired
		} else {
			printf "CTRL   FAIL  the gate never reported a wrong count — the one-partial mutation did not fire\n"
			rc = 1
		}
		# Anti-vacuity. If the un-normalized form reached the same verdicts, the normalization would
		# be unmotivated and these columns would be measuring nothing.
		if (unnorm_vs_gate > 0) {
			printf "CTRL   PASS  the un-normalized form (raw@*) contradicts the gate on %d of %d toolchain-matched comparisons\n", unnorm_vs_gate, rows * 2
		} else {
			printf "CTRL   FAIL  the un-normalized form never contradicts the gate — this gate is not discriminating\n"
			rc = 1
		}
		if (unnorm_split > 0) {
			printf "CTRL   PASS  and it answers DIFFERENTLY on the two toolchains for %d/%d rows — the skew R6 exists to close\n", unnorm_split, rows
		} else {
			printf "CTRL   FAIL  the un-normalized form gives one answer on both toolchains — the fixture does not reproduce the skew\n"
			rc = 1
		}
		print ""
		if (rc == 0) print "DOC-PARITY-OK: the documented local check and the committed gate reach one verdict"
		else         print "DOC-PARITY-FAILED"
		exit rc
	}
' "$WORK/rows.txt"
