# PLAN — Glob as the default path-match dialect (pilot)

> **Status:** Draft for review · **Last updated:** 2026-09-07
> **Authoritative technical design.** The *how*. Vision/contract is [`GOAL.md`](GOAL.md);
> the phased executable roadmap is [`TECH.md`](TECH.md). Lean path (`appetite: small`):
> no `research/` fan-out; the unknowns below were settled by targeted reads during planning.

## 1. Summary

Path-pattern translation moves into the library as a pure glob→regex function, and every
query tool translates at filter ingestion while the stored form — and the one SQL path behind
it — stays a regular expression. `-p/--pattern` therefore means glob unless `--regex` is
passed, invalid globs fail closed before any query is built, and the man pages learn the new
dialect in the same commits as the flags. No new dependency, no schema touch, no new SQL
surface.

## 2. Design

### 2.1 Glob translator (`src/lib.rs`)

New `pub fn glob_to_regex(glob: &str) -> Result<String, String>`: pure, unit-testable,
no I/O. `*` becomes `.*` — it crosses `/`, so `*.py` matches at any depth, which is what
GOAL R1's example promises. `?` becomes `.`. `[...]` passes through after validation
(`[!...]` negates to `[^...]`); a trailing `\` quotes the next character literally. Every
other regex metacharacter in a literal position is escaped. Output is wrapped `^(?:…)$`
because DuckDB `regexp_matches` is an unanchored substring search and a glob names the
whole path. Rejections, each with a message naming the position: unterminated `[`, empty
class `[]`, trailing `\`. Hand-rolled (~60 lines) rather than the `glob` crate: the crate's
matcher cannot run inside DuckDB SQL anyway, so a translation step is required regardless,
and a new dependency risks the air-gapped story `offline_tests.rs` guards.

### 2.2 Filter ingestion (`src/lib.rs`, `QueryFilters`)

The stored `pattern` field keeps meaning regex, and `with_pattern` keeps its signature, so
the existing unit tests (`lib.rs` filter tests) and `to_conditions` stay green untouched.
New `with_path_pattern(pattern: Option<String>, regex: bool) -> Result<Self, String>`:
with `regex` it stores the raw string (today's behavior); without, it stores
`glob_to_regex` output and records the user's original text in a new
`pattern_display: Option<String>` field that `format_display` prefers, so the TUI filter
bar keeps showing `[/umeur/*.py]` instead of the translated regex. Translation happens at
ingestion rather than in `to_conditions` because that builder returns `Vec<String>` and
cannot fail — and keeping its signature means the existing single-quote-doubling escape
stays the one SQL path for both dialects (§5).

### 2.3 CLI surface (`src/cli.rs`)

A long-only `--regex` flag on `XduFindArgs`, `XduViewArgs`, and `XduRmArgs` — long-only
because the free short letters differ per binary and a shared short would collide with
different neighbors. `value_name = "REGEX"` becomes `"PATTERN"` on all three `-p/--pattern`
flags, help text is rewritten around glob-first with one `--regex` example each, and
`after_help` examples drop the `'\.py$'`-style regexes for glob ones. `xdu`'s own `-p`
(`--partition`) is untouched: the overload footgun stays exactly as documented.

### 2.4 Bin wiring (three call sites plus one)

`xdu-find` (filter build), `xdu-rm` (filter build), and `xdu-view` (startup filter build)
call `with_path_pattern(args.pattern, args.regex)`; the `Err` propagates through the
existing `anyhow` return, which prints to stderr and exits 1 — before any query is built,
and for `xdu-rm` before any deletion set is selected (R3 fails closed). In `xdu-view` the
validation must run before `enable_raw_mode`, so the failure path never owns the terminal.
The fourth site is the TUI's interactive `/` entry (`confirm_input`, `InputMode::Pattern`),
which today assigns the raw string into `filters.pattern`: it routes new input through
`glob_to_regex` instead, keeping the stored field one dialect everywhere, and surfaces the
`Err` as a status-bar message rather than exiting, matching how that prompt already reports
bad numeric input.

### 2.5 Man pages (`doc/xdu-find.1.scd`, `doc/xdu-view.1.scd`, `doc/xdu-rm.1.scd`)

Synopsis `_REGEX_` becomes `_PATTERN_`, flag text describes glob-first with a `--regex`
entry each, and examples use glob. Every literal `*` is escaped `\*`: a bare `*` is bold
markup and publishes the wrong page at exit 0 (§13). Each touched page is re-rendered and
checked with the whitespace-stripped literal match from `AGENTS.md` Commands before commit.

### Requirement → design map

| R-ID | Design element(s) that satisfy it |
|------|-----------------------------------|
| R1 | §2.1 translator (`*` crosses `/`) + §2.2 glob ingestion + §2.4 wiring in all three tools and the TUI `/` prompt |
| R2 | §2.3 `--regex` flag + §2.2 raw passthrough + §2.4 wiring |
| R3 | §2.2 `Result` ingestion + §2.4 fail-closed ordering (rm selects nothing; view validates pre-terminal) |

## 3. Invariant gate (AGENTS.md constitution check)

Checked against [`.agents/factory/invariants.md`](../../.agents/factory/invariants.md) (§1–§13)
before research and again after this design was drafted.

- §4 (`xdu-rm` safety) — R3 fails closed before query construction, so an invalid glob
  cannot select a deletion set; confirm/dry-run/force and the `--limit` deterministic
  `ORDER BY` are untouched. The `--safe` pattern gap is neither widened nor closed here.
- §5 (injection) — translator output uses a constrained alphabet and still flows through
  the existing quote-doubling in `to_conditions`; no new SQL path, no raw `format!` of
  user input beyond today's.
- §10 (CLI source of truth) — flags land in `cli.rs` with clap validation; each phase
  updates its man page in the same commit as the flag change.
- §11 (altitude) — translation and dialect bookkeeping live in `lib`; bins map one
  `Result` to an exit path. The TUI reuses `glob_to_regex` rather than reimplementing it.
- §13 (conventions) — errors go to stderr via `anyhow`, stdout stays pipeable; `*` escaping
  in `.scd` per §2.5; no `R#`/`P#` ids in source.
- Untouched by this change: §1 (no schema change), §2/§2b/§2c (no finalize, marker, or
  error-tolerance change), §3 (layout; `index_glob` is an unrelated name), §6–§9, §12.

### Deviation justifications

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|-----------|--------------------------------------|
| — | — | — |

## 4. Rabbit holes (resolved)

Lean path — settled by targeted reads, no briefs. LIKE-vs-`regexp_matches`: translating to
regex keeps the single existing SQL path and preserves `[...]` classes, which `LIKE`
cannot express. `*` crossing `/`: required by GOAL R1's `*.py` example; `?` crosses too,
and the man page says so — one coherent rule, not two. New crate vs hand-roll: translation
is unavoidable (matchers do not run inside DuckDB), and zero new dependencies keeps the
offline build story intact. TUI `/` dialect: glob, like the flag, or one struct field would
hold two dialects depending on who set it.

## 5. Risks & open questions

- A glob anchors the whole path while regex-default stays a substring search. Intended, and
  the man page states it — but a user porting `py` (regex, matches anywhere) to bare `py`
  (glob, matches only a file literally named `py`) will see fewer rows. Examples steer
  toward `*py*`.
- Saved regex invocations change meaning under a bare `-p`. Accepted pre-v1 break per the
  GOAL non-goals; `--regex` is the escape hatch.
- An invalid regex under `--regex` still fails at DuckDB prepare time, as today. Unchanged
  behavior, still stderr plus non-zero exit.
- No open question needs a human before build; the `--regex` spelling was already chosen
  over a mode switch or short flag in §2.3.

## 6. Verification strategy

Unit tests in `lib` cover the translator (each metacharacter, escaping, each rejection).
Integration tests convert the four regex-valued assertions (`tests/rm_tests.rs:103,503,547`,
`tests/offline_tests.rs:67`) to glob and gain `--regex` plus invalid-glob cases. CLI drives
use `.agents/factory/bin/temp_index.sh` with fixture-independent assertions: `-p '*'`
count equals unfiltered count, glob count equals the equivalent `--regex` count, invalid
globs exit non-zero, and `xdu-rm --dry-run` on an invalid glob deletes nothing. Man pages
are rendered and literal-checked the way CI does.

---

*Backing research (if present): none — lean path.*
