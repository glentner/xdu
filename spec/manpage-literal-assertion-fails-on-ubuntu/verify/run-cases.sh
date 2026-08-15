#!/bin/sh
# run-cases.sh — the platform-portable half of gate-matrix.sh.
#
# Runs the committed CI gate body against a matrix of pre-rendered man-page fixtures and writes one
# machine-readable RESULT line per case to stdout. gate-matrix.sh executes it twice with identical
# arguments — once on the host, once inside ubuntu:24.04 — so a verdict that depends on the userland
# (BSD vs GNU grep/wc/col) surfaces as a disagreement between the two streams instead of as a green
# run on the author's laptop. It never renders anything itself: every fixture arrives read-only from
# a real scdoc of the version it is labelled with.
#
# Usage: run-cases.sh FIXTURES GATE SCRATCH PLATFORM HOST_LABEL DISTRO_LABEL
#   FIXTURES      read-only fixture root — scd/<variant>/*.scd and roff/<label>/<variant>/*.1
#   GATE          the gate body extracted from .github/workflows/test.yaml
#   SCRATCH       writable scratch dir (must not be inside FIXTURES)
#   PLATFORM      label for the RESULT lines
#   HOST_LABEL    roff label produced by the host scdoc      (R2: the escaping scdoc)
#   DISTRO_LABEL  roff label produced by ubuntu-24.04's scdoc (R1: the scdoc CI installs)
#
# Output lines: RESULT|<rid>|<platform>|<roff>|<case>|<PASS|FAIL>|<detail>
#               INFO|<platform>|<free text>

set -u

FIX="$1"
GATE="$2"
SCRATCH="$3"
PLATFORM="$4"
HOST_LABEL="$5"
DISTRO_LABEL="$6"

WIDTH_MIN=40
WIDTH_MAX=200
PAD_MAX=150
PAD_STEP=3

REAL_MANDOC=$(command -v mandoc) || REAL_MANDOC=""
[ -n "$REAL_MANDOC" ] || { echo "run-cases: no mandoc on $PLATFORM" >&2; exit 2; }

# The gate calls `mandoc` with no width option, exactly as CI does. Sweeping the render width
# therefore has to happen underneath it: a shim earlier on PATH injects -O width when MANDOC_WIDTH
# is set and is a transparent pass-through when it is not, so the unswept cases run CI's own
# default width rather than a width this harness chose.
mkdir -p "$SCRATCH/bin"
{
	echo '#!/bin/sh'
	echo "if [ -n \"\${MANDOC_WIDTH:-}\" ]; then exec $REAL_MANDOC -O width=\"\$MANDOC_WIDTH\" \"\$@\"; fi"
	echo "exec $REAL_MANDOC \"\$@\""
} > "$SCRATCH/bin/mandoc"
chmod 755 "$SCRATCH/bin/mandoc"
PATH="$SCRATCH/bin:$PATH"
export PATH
MANDOC_WIDTH=""
export MANDOC_WIDTH

emit() { printf 'RESULT|%s|%s|%s|%s|%s|%s\n' "$1" "$PLATFORM" "$2" "$3" "$4" "$5"; }
info() { printf 'INFO|%s|%s\n' "$PLATFORM" "$*"; }

# Materialize a run directory for one fixture variant: the gate resolves share/man/man1/*.1 and
# doc/*.scd relative to the working directory, so each case gets a tree shaped like the repo.
prepare() {
	_v="$1"; _s="$2"; _d="$SCRATCH/run/$_s-$_v"
	if [ ! -d "$_d" ]; then
		mkdir -p "$_d/doc" "$_d/share/man/man1"
		cp "$FIX/scd/$_v"/*.scd "$_d/doc/"
		cp "$FIX/roff/$_s/$_v"/*.1 "$_d/share/man/man1/"
	fi
	printf '%s' "$_d"
}

# Run the gate in DIR and leave its exit status in RC and its combined output in OUT.
# GitHub Actions' default shell for `run:` is `bash -e`; the pipefail mode exists to prove the
# gate reaches the same diagnostics under the hardened shell anyone might switch it to.
run_gate() {
	_d="$1"; _mode="${2:-plain}"; _g="${3:-$GATE}"
	if [ "$_mode" = pipefail ]; then
		OUT=$(cd "$_d" && bash -eo pipefail "$_g" 2>&1); RC=$?
	else
		OUT=$(cd "$_d" && bash -e "$_g" 2>&1); RC=$?
	fi
}

count_in_out() { printf '%s\n' "$OUT" | grep -cF "$1" || true; }
out_has() { printf '%s\n' "$OUT" | grep -qF "$1"; }

for S in "$HOST_LABEL" "$DISTRO_LABEL"; do
	if [ "$S" = "$DISTRO_LABEL" ]; then CLEAN_RID=R1; else CLEAN_RID=R2; fi

	# --- control: an unmodified tree at CI's own default width must exit 0 and say so ----------
	MANDOC_WIDTH=""
	d=$(prepare clean "$S")
	run_gate "$d"
	if [ "$RC" -eq 0 ] && out_has 'OK: every asserted literal survived'; then
		emit "$CLEAN_RID" "$S" control PASS "exit 0 with the OK line"
	else
		emit "$CLEAN_RID" "$S" control FAIL "exit $RC: $(printf '%s' "$OUT" | tr '\n' ' ')"
	fi

	# --- R7 class scan: EVERY asserted literal, not the subset someone remembered to count.
	#     "Occurs more than once" is a property of the PAGE, not of the literal, so a future
	#     cross-reference added to any .scd silently re-opens the hole. Derive each literal's
	#     published count and fail on an uncounted duplicate — and on a declared N that no longer
	#     matches, which is the same drift from the other direction. This is what makes R7 hold
	#     for literals nobody has looked at yet, including a page that does not exist today. -----
	MANDOC_WIDTH=""
	awk -v q="'" '
		{
			line = $0
			buf = cont ? buf " " line : line
			if (line ~ /\\$/) { cont = 1; sub(/\\[ \t]*$/, "", buf); next }
			cont = 0
			if (buf !~ /^check[ \t]/) next
			split(buf, f, /[ \t]+/); page = f[2]
			rest = buf
			sub(/^check[ \t]+[^ \t]+[ \t]*/, "", rest)
			while (match(rest, q "[^" q "]*" q)) {
				print page "|" substr(rest, RSTART + 1, RLENGTH - 2)
				rest = substr(rest, RSTART + RLENGTH)
			}
		}
	' "$GATE" > "$SCRATCH/specs-$S.txt"
	nspec=$(wc -l < "$SCRATCH/specs-$S.txt" | tr -d ' ')
	if [ "$nspec" -lt 5 ]; then
		emit R7 "$S" class-duplicate-scan FAIL "parsed only $nspec spec(s) from the gate — the extraction is wrong, not the gate"
	else
		cbad=0; cdetail=""
		while IFS='|' read -r pg spec; do
			[ -n "$spec" ] || continue
			case "$spec" in
				[0-9]*x:*)
					want="${spec%%x:*}"; lit="${spec#*x:}"
					case "$want" in *[!0-9]*) want=""; lit="$spec" ;; esac ;;
				*) want=""; lit="$spec" ;;
			esac
			needle=$(printf '%s' "$lit" | tr -d '[:space:]')
			got=$(mandoc -Tutf8 "$d/$pg" | col -b | tr -d '[:space:]' \
				| grep -oF -- "$needle" | wc -l | tr -d ' ') || got=0
			if [ -z "$want" ] && [ "$got" -gt 1 ]; then
				cbad=$((cbad + 1))
				cdetail="$cdetail; $pg '$lit' publishes $got copies but is asserted by presence"
			elif [ -n "$want" ] && [ "$got" -ne "$want" ]; then
				cbad=$((cbad + 1))
				cdetail="$cdetail; $pg '$lit' declares ${want}x but publishes $got"
			fi
		done < "$SCRATCH/specs-$S.txt"
		if [ "$cbad" -eq 0 ]; then
			emit R7 "$S" class-duplicate-scan PASS "$nspec specs scanned; every literal published more than once is counted, every declared N matches"
		else
			emit R7 "$S" class-duplicate-scan FAIL "$cbad of $nspec specs wrong$cdetail"
		fi
	fi

	# --- R4 width sweep: the verdict must not depend on where mandoc breaks a line -------------
	wfail=0; wtot=0; wfirst=""
	w=$WIDTH_MIN
	while [ "$w" -le "$WIDTH_MAX" ]; do
		MANDOC_WIDTH="$w"
		run_gate "$d"
		wtot=$((wtot + 1))
		if [ "$RC" -ne 0 ]; then
			wfail=$((wfail + 1))
			[ -n "$wfirst" ] || wfirst="width=$w: $(printf '%s' "$OUT" | tr '\n' ' ')"
		fi
		w=$((w + 1))
	done
	MANDOC_WIDTH=""
	if [ "$wfail" -eq 0 ]; then
		emit R4 "$S" "width-sweep-${WIDTH_MIN}..${WIDTH_MAX}" PASS "$wtot/$wtot widths green"
	else
		emit R4 "$S" "width-sweep-${WIDTH_MIN}..${WIDTH_MAX}" FAIL "$wfail/$wtot widths red; first: $wfirst"
	fi

	# --- R4 pad sweep: the realistic trigger — an edit above the literal reflows the page ------
	# Runs at CI's own default width, so a red here is a red CI run, not a hypothetical one.
	pfail=0; ptot=0; pfirst=""
	p=0
	while [ "$p" -le "$PAD_MAX" ]; do
		pd=$(prepare "pad-$(printf '%03d' "$p")" "$S")
		run_gate "$pd"
		ptot=$((ptot + 1))
		if [ "$RC" -ne 0 ]; then
			pfail=$((pfail + 1))
			[ -n "$pfirst" ] || pfirst="pad=$p: $(printf '%s' "$OUT" | tr '\n' ' ')"
		fi
		p=$((p + PAD_STEP))
	done
	if [ "$pfail" -eq 0 ]; then
		emit R4 "$S" "pad-sweep-0..${PAD_MAX}" PASS "$ptot/$ptot pad values green at the default width"
	else
		emit R4 "$S" "pad-sweep-0..${PAD_MAX}" FAIL "$pfail/$ptot pad values red; first: $pfirst"
	fi

	# --- R3: the class the gate exists for — mis-escaped markup, published at scdoc exit 0 -----
	d3=$(prepare unescaped-glob "$S")
	run_gate "$d3"
	if [ "$RC" -ne 0 ] && out_has "missing the literal: OUTDIR/*/*.parquet"; then
		emit R3 "$S" mut-unescaped-glob PASS "exit $RC naming OUTDIR/*/*.parquet"
	else
		emit R3 "$S" mut-unescaped-glob FAIL "exit $RC: $(printf '%s' "$OUT" | tr '\n' ' ')"
	fi

	# --- R5a: a page that does not render is reported as that, not as six corrupt literals -----
	d5a="$SCRATCH/run/$S-missing-page"
	if [ ! -d "$d5a" ]; then
		mkdir -p "$d5a"
		cp -R "$d/doc" "$d/share" "$d5a/"
		mv "$d5a/share/man/man1/xdu.1" "$SCRATCH/removed-xdu.1"
	fi
	run_gate "$d5a"
	n5a=$(count_in_out 'CORRUPT RENDER')
	if [ "$RC" -ne 0 ] && out_has 'RENDER FAILED' && [ "$n5a" -eq 0 ]; then
		emit R5 "$S" mut-missing-page PASS "RENDER FAILED, 0 misleading CORRUPT RENDER lines"
	else
		emit R5 "$S" mut-missing-page FAIL "exit $RC, $n5a CORRUPT RENDER line(s): $(printf '%s' "$OUT" | tr '\n' ' ')"
	fi

	# --- R5b: mandoc exits 0 on an empty file, so only a length guard catches "rendered to
	#          nothing" — the case that otherwise produces the exact pile of lines R5 removes ---
	d5b="$SCRATCH/run/$S-empty-page"
	if [ ! -d "$d5b" ]; then
		mkdir -p "$d5b"
		cp -R "$d/doc" "$d/share" "$d5b/"
		: > "$d5b/share/man/man1/xdu.1"
	fi
	run_gate "$d5b"
	n5b=$(count_in_out 'CORRUPT RENDER')
	if [ "$RC" -ne 0 ] && out_has 'RENDER EMPTY' && [ "$n5b" -eq 0 ]; then
		emit R5 "$S" mut-empty-page PASS "RENDER EMPTY, 0 misleading CORRUPT RENDER lines"
	else
		emit R5 "$S" mut-empty-page FAIL "exit $RC, $n5b CORRUPT RENDER line(s): $(printf '%s' "$OUT" | tr '\n' ' ')"
	fi

	# --- R7: corruption of ONE of the two .partial occurrences (a presence check cannot see it) -
	d7=$(prepare one-partial "$S")
	run_gate "$d7"
	if [ "$RC" -ne 0 ] && out_has "has 1 occurrence(s)"; then
		emit R7 "$S" mut-one-partial PASS "exit $RC reporting 1 occurrence, expected 2"
	else
		emit R7 "$S" mut-one-partial FAIL "exit $RC: $(printf '%s' "$OUT" | tr '\n' ' ')"
	fi

	# --- R7 regression: whitespace removal fuses tokens, so a counted needle must be specific
	#     enough that fusion cannot synthesize it. A page containing an ordinary `e.g. partial`
	#     sentence stays green with `.partial suffix`; the raw counts below show it would not
	#     with the shorter `.partial`. --------------------------------------------------------
	d7b=$(prepare fusion "$S")
	run_gate "$d7b"
	if [ "$RC" -eq 0 ]; then
		emit R7 "$S" fusion-regression PASS "exit 0 — the fused token did not inflate the count"
	else
		emit R7 "$S" fusion-regression FAIL "exit $RC: $(printf '%s' "$OUT" | tr '\n' ' ')"
	fi
	fused=$(mandoc -Tutf8 "$d7b/share/man/man1/xdu.1" | col -b | tr -d '[:space:]')
	c_short=$(printf '%s' "$fused" | grep -oF -- '.partial' | wc -l | tr -d ' ') || c_short=0
	c_long=$(printf '%s' "$fused" | grep -oF -- '.partialsuffix' | wc -l | tr -d ' ') || c_long=0
	info "fusion fixture ($S): needle '.partial' matches $c_short (false red), '.partial suffix' matches $c_long (correct)"

	# --- R7 parser: `Nx:` must parse an N of any width. A single-digit-only pattern silently
	#     degrades `12x:LIT` to a PRESENCE check on a literal carrying its own `12x:` prefix — still
	#     red, but red for the wrong reason and naming a literal that can never appear on any page.
	#     Assert the gate still counts: against the clean 2-occurrence page it must report the
	#     observed count against the requested one, and must NOT report a presence miss. ----------
	gmulti="$SCRATCH/gate-multidigit.sh"
	sed "s/'2x:\.partial suffix'/'12x:.partial suffix'/" "$GATE" > "$gmulti"
	if grep -qF '12x:.partial suffix' "$gmulti"; then
		run_gate "$d" plain "$gmulti"
		if [ "$RC" -ne 0 ] && out_has 'expected 12' && ! out_has 'missing the literal: 12x:'; then
			emit R7 "$S" multi-digit-count PASS "N=12 parsed; reported the observed count, not a presence miss"
		else
			emit R7 "$S" multi-digit-count FAIL "exit $RC: $(printf '%s' "$OUT" | tr '\n' ' ')"
		fi
	else
		emit R7 "$S" multi-digit-count FAIL "could not build the 12x: gate variant — the mutation did not apply"
	fi

	# --- Shell hardening: nothing in the repo pins the shell. Two genuine corruptions must
	#     produce two diagnostics under `bash -e` (today's default) AND under `bash -eo pipefail`,
	#     where grep's exit-1-on-zero-matches would otherwise kill the step silently. -----------
	dsh=$(prepare two-corruptions "$S")
	for mode in plain pipefail; do
		run_gate "$dsh" "$mode"
		nsh=$(count_in_out 'CORRUPT RENDER')
		if [ "$RC" -ne 0 ] && [ "$nsh" -eq 2 ]; then
			emit SHELL "$S" "two-corruptions-bash-$mode" PASS "exit $RC with 2 diagnostics"
		else
			emit SHELL "$S" "two-corruptions-bash-$mode" FAIL "exit $RC with $nsh diagnostic(s): $(printf '%s' "$OUT" | tr '\n' ' ')"
		fi
	done
done

exit 0
