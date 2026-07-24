# EARS — Easy Approach to Requirements Syntax

A lightweight controlled-natural-language convention for writing acceptance criteria that are
**testable and low-ambiguity**. Used by `xdu-feature` to shape `GOAL.md` acceptance criteria (R-IDs).

**Nudge, don't hard-enforce.** EARS reduces ambiguity; it does not eliminate it, and forcing it onto
genuinely exploratory or ubiquitous requirements stilts them. Prefer EARS where it clarifies; fall
back to plain, unambiguous prose where EARS would be contrived. Every criterion still gets a stable
R-ID.

## Generic template

> **While** \<optional precondition/state>, **when** \<optional trigger>, the \<component> **shall**
> \<observable response>.

Keep the `<component>` a real xdu part (`xdu-rm`, the `xdu` crawler, `QueryFilters`, the `xdu-view`
TUI, `xdu-find`) and the `<response>` **observable** (an exit status, index rows, a printed stdout
token, a file actually deleted-or-kept) so `xdu-review` can check it by driving the CLI.

## The six patterns

| Pattern | Keyword | Form |
|---|---|---|
| **Ubiquitous** | *(none)* | The \<component> shall \<response>. |
| **State-driven** | `While` | While \<state>, the \<component> shall \<response>. |
| **Event-driven** | `When` | When \<trigger>, the \<component> shall \<response>. |
| **Optional-feature** | `Where` | Where \<feature is included>, the \<component> shall \<response>. |
| **Unwanted-behavior** | `If … Then` | If \<unwanted condition>, then the \<component> shall \<response>. |
| **Complex** | combo | While \<state>, when \<trigger>, the \<component> shall \<response>. |

## xdu-flavored examples

- **R1 (event):** *When* `xdu-rm` is invoked without `--force` on a non-empty match set, the deletion
  command *shall* prompt for interactive `y/N` confirmation and, on any answer other than `y`/`yes`,
  exit having deleted zero files.
- **R2 (unwanted):** *If* a `--pattern` value reaching `QueryFilters` contains a single quote, *then*
  `xdu-find` *shall* escape it before it reaches the DuckDB `WHERE` clause and still return the
  correctly-matched index rows (no SQL error, no injection).
- **R3 (state):** *While* `--safe` is set, `xdu-rm` *shall* re-`stat` each file immediately before
  unlink and skip any whose current metadata no longer matches the query, deleting only the files
  that still match.
- **R4 (ubiquitous):** The `xdu` crawler *shall* write each partition chunk to
  `NNNNNN.parquet.partial` and `fs::rename` it into place, so a reader globbing `*/*.parquet` never
  observes a partial chunk.

## Anti-patterns

- Untestable adjectives ("fast", "robust", "user-friendly") — replace with an observable threshold.
- Multiple requirements in one line — split so each has its own R-ID and pass/fail.
- Specifying the *how* (implementation) in a criterion — that belongs in `PLAN.md`.
- Encoding a **suspected cause/mechanism** in a *fix's* criterion (e.g. "the fix must not use the
  broken code path") — the root cause is unverified until `/xdu-plan`; state the observable broken→fixed
  behavior instead.
