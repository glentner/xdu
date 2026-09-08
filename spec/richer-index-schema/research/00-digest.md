# 00 — Research digest (consolidated decisions)

Synthesis of the four briefs into the decisions `PLAN.md` builds on. Where briefs
disagreed, the single recommendation is stated here with its reason.

## Agreements (no ruling needed)

- **Schema.** Append `uid`, `gid`, `mode`, `mtime`, `ctime` after `atime`, all `Int64`,
  all non-null. `INDEX_FORMAT_VERSION` 1 → 2. No second `stat`: all five come from the
  same `Metadata` via `MetadataExt` ([01](01-schema-crawler-versioning.md)).
- **Version bump carries R2/R3.** The writer interpolates the constant; the gate accepts
  exactly it. No hardcoded `format=1` in production; readers never hardcode `== 1`
  ([01](01-schema-crawler-versioning.md), [02](02-readers-output.md)).
- **Readers keep working by named projection.** No `SELECT *` anywhere. find
  path/size/atime/count/top, all of view, all of rm need no SELECT change. R8 touches
  only the csv and json arms of `xdu-find` ([02](02-readers-output.md)).
- **Find-only flags, no flatten.** Per-binary clap structs stay; new fields land on
  `XduFindArgs` only. `doc/xdu-find.1.scd` updates in the same commit; the "same filter
  options as xdu-find" sentence in `doc/xdu-rm.1.scd:16` is rewritten with it
  ([03](03-queryfilters-cli.md)).
- **mtime pair.** `--mtime-older-than` / `--mtime-newer-than` in DAYS, same
  `now - days*86400` conversion and same `<` / `>=` operators as atime. `--older-than`
  stays atime ([03](03-queryfilters-cli.md), [04](04-nss-perm-mtime.md)).
- **Names never reach SQL.** Resolution yields integers; `to_conditions` interpolates
  numbers exactly as size and atime do today. New fields join `is_active`, `clear`,
  and `format_display` ([03](03-queryfilters-cli.md)).
- **`libc` promotion.** NSS needs `getpwnam`/`getgrnam`; `libc` is currently
  dev-only (`Cargo.toml:24-26`) and moves to `[dependencies]`. No `users`/`nix` crate
  ([04](04-nss-perm-mtime.md)).
- **Single resolve at startup.** The find bin resolves once on the main thread before
  DuckDB runs; the helper is documented as not pool-safe (`getpwnam` is not
  reentrant). No per-row resolution ([04](04-nss-perm-mtime.md)).
- **`--safe` untouched.** Its atime/max-size re-stat and its documented gaps stay as
  they are; the man page must not claim the new columns ([02](02-readers-output.md),
  [03](03-queryfilters-cli.md)).

## C1 — Mode storage: masked (04 over 01)

Brief 01 recommends storing full `st_mode` unmasked so no crawler decision is
irreversible; brief 04 recommends `st_mode & 0o7777`. **Ruling: store masked.**

R1 says permission bits, and R8 prints them: a csv `mode` of `33188` (full `st_mode`
decimal) is not permission bits as any administrator reads them. The type bits are
constant — the index holds only regular files — so masking discards nothing a query
can use, and every filter and every emission avoids re-masking. The crawler masks
once; SQL and csv/json read the column bare.

## C2 — Permission flag: `--mode SPEC` (03 over 04)

Brief 04 proposes `--perm-mask OCTAL` (all-bits only); brief 03 proposes `--mode SPEC`
with three forms: bare octal exact (`644`), `/OCTAL` any-bit (`/002` world-writable),
`&OCTAL` all-bit (`&4000` setuid). **Ruling: `--mode SPEC`.**

R6 wants exposure *classes*, plural. All-bits alone cannot ask "any of these bits"
without enumerating modes, and exact alone cannot ask it at all — the two gaps 04
defers are the question. The three-form grammar subsumes `--perm-mask`
(`&4000` is that flag), stays one small Rust parser with unit-pinned fragments, and
keeps find's `-` prefix out of clap's reach by spelling all-bits `&`. Invalid octal,
empty SPEC, or unknown prefix fails before any query is built, in the shape of R5.

## C3 — Numeric-looking names: POSIX lookup-first (04 over 03)

Brief 03 resolves an all-digit argument as a literal uid/gid skipping NSS; brief 04
calls `getpwnam` first and falls back to decimal parse on miss. **Ruling: POSIX —
lookup first, digits on miss.** A user actually named `1000` wins over uid 1000;
otherwise a digit string is its uid. This costs one NSS call at startup, where 04
already pays it. Overflow, leading minus, and mixed tokens (`1000x`) are
unresolvable and take R5. A pure digit string that parses is a uid even when it
matches zero rows — exit 0, not R5. No separate `--uid` flag.

## C4 — uid/gid field type: `u32` on `QueryFilters`

Brief 03 stores resolved ids as `i64` (matching existing filter style); brief 04 as
`u32` (matching `uid_t`). **Ruling: `Option<u32>`.** Identity stays identity-typed
through resolution; SQL emission formats identically either way. Mode bits likewise
`u32` beside a small exact/any/all operator enum; mtime thresholds stay `i64`
epochs like the atime pair they mirror.

## Test gaps

The missing R3 pin is a `version_tests` case planting `format=1` and asserting
refuse — the suite would stay green today if a regression re-accepted it
([02](02-readers-output.md) §7). No integration test parses csv/json output; the
filter behavior tests do not exist yet and ride the phase that wires the flags.
