---
created: 2026-09-07
updated: 2026-09-07
---

# `logfmt` and `kv`: the de-facto key=value parsers, and why they stay two kinds

## Status

Accepted

## Context

`json` (`docs/adr/json-parsing-into-attributes.md`) covers structured JSON bodies, but a large
share of real log lines are flat `key=value` text instead -- Heroku/`go-kit`-style logfmt
(`level=info msg="hello world" dur=3ms`) on one hand, and ad-hoc literal-separator formats
(`a=1&b=2` query-string shapes, `a: 1, b: 2`) on the other. `docs/design/lua-api.md` already names
both among the "built-in native processors ... meant to sit in front of user Lua" that "handle the
common structured-parsing cases without per-event VM overhead" -- LuaJIT ships Lua patterns, not a
general parser or a real regex engine, so hand-writing a `k=v` parser per event in a script would be
both slower and not something users should have to write by hand. This ADR is that work landing for
`logfmt`/`kv` specifically.

## Decision

**Two distinct `ComponentKind`s/`Transform`s, `logfmt` and `kv`, sharing one module
(`crates/logit-transforms/src/logfmt.rs`) and a scan tail (`merge_into`, `unescape`,
`find_bytes`/`trim_ws_range`) but not a mode flag.** `keep`/`remove` and `keep_signals`/
`drop_signals` are the existing precedent for "separate kinds sharing a module" over "one kind
with a `mode:` field" -- the two grammars diverge enough (quoting/escaping vs. none, fixed
delimiters vs. configurable ones) that a shared config surface would need per-mode fields anyway,
with no shared behavior to actually unify.

**`logfmt`'s grammar is fixed and requires no config beyond `bare_keys`:** whitespace-delimited
`key=value` tokens, `"`-quoted values with backslash escapes (`\\`, `\"`, `\n`, `\r`, `\t` --
anything else, including `\uXXXX`/`\xNN`, is preserved verbatim, backslash included), first `=`
wins (`a=b=c` -> `a` -> `"b=c"`). An unterminated quote fails the *whole line* (token boundaries
become unknowable past that point) with a throttled `parse_failure` diagnostic and the event
passed through untouched, exactly `json`'s posture toward a malformed object. A keyless `=`
(`=1 a=2`) only skips that one token and resynchronizes at the next whitespace -- the rest of the
line still parses. This asymmetry is intentional: an unterminated quote corrupts everything after
it, a keyless token corrupts nothing but itself.

**A bareword (a token with no `=`) is skipped by default; `bare_keys: true` opts into
`Value::Bool(true)`.** This is the one place this ADR departs from strict Heroku logfmt fidelity,
and deliberately so: a bareword promotes an arbitrary, peer-supplied input token into attribute-*key*
position, and every attribute key is interned into the process-wide, monotonic, never-shrinking
interner (`docs/design/memory.md` §4) -- keys are always interned, values never are. The most
common near-logfmt shape in the wild is timestamp-prefixed (`2026/09/07 12:00:00 level=info ...`,
Go's stdlib `log` produces exactly this), and there is no `json`-style `skip_to_brace` escape hatch
for logfmt (no unambiguous start delimiter to skip to). With Heroku's default bareword semantics,
that timestamp prefix would become new, never-repeating interner keys on every single line --
unbounded growth driven directly by input volume, the exact risk `docs/design/memory.md` §4 flags.
Turning `bare_keys` on is a deliberate opt-in for a source that genuinely emits flags, not the
default. A line consisting *only* of barewords still fails as a `NoPairs` parse failure even with
`bare_keys: true` -- a bareword never sets the "this line looks like logfmt" flag, only a real
`key=value` pair does, so word-salad prose doesn't get promoted to "successfully parsed" just
because every word happened to become a `true` flag.

**`kv` requires both `pair_sep` and `kv_sep`, with no defaults.** Defaulting them to `" "`/`"="`
would make a bare `kv:` component silently mis-parse quoted logfmt (a value containing a space
would be split across multiple bogus pairs) -- `logfmt` is the right component for that shape, and
`kv`'s whole reason to exist is a *different*, literal-separator grammar (`a=1&b=2`, `a: 1, b: 2`).
Splitting is byte-literal: `pair_sep` first (no quoting means no ambiguity about what counts as a
separator), then the *first* `kv_sep` inside each resulting segment (`a=b=c` -> `a` -> `"b=c"`,
matching `logfmt`'s own first-`=`-wins rule). Whitespace around each key and value is **trimmed
unconditionally** (not a `trim: bool` flag) -- this is what lets `pair_sep: ","` work uniformly on
both `a=1,b=2` and `a=1, b=2` without an operator needing to think about it, and there's no grammar
reason a `kv` user would ever want literal leading/trailing whitespace preserved around a key or
value it can't quote anyway. An empty segment (`a=1&&b=2`) is skipped silently (nothing to report);
a segment with no `kv_sep` at all follows the same `bare_keys` rule as `logfmt`; a segment whose key
is empty after trimming is skipped and counted. If no segment anywhere contains `kv_sep`, the whole
line fails exactly like `logfmt`'s `NoPairs` case.

**Both are rejected at graph-validation time when `kv`'s separators can only ever misbehave**
(new rule 29, `crates/logit-pipeline/src/graph.rs`): an empty `pair_sep` or `kv_sep`, identical
`pair_sep`/`kv_sep`, or a `kv_sep` that contains `pair_sep`. Each is a certain no-op (an empty
separator splits between every byte; identical separators mean every segment is split away from
its own separator, so no line could ever produce a pair) or a certain garbage result (a `kv_sep`
containing `pair_sep` can never appear intact inside a segment, since the `pair_sep` split always
runs first) -- exactly the "can only ever be a no-op" family `kv_metrics`/`scale`/`set` already
established (rules 10-12, 19, 20).

**Values are always `Value::Str` -- never numeric coercion, ever**, unlike `json`'s type-by-JSON-
syntax rule. `crate::numeric` (already shared by `scale`/`kv_metrics`) accepts a `Value::Str` that
parses cleanly to a finite `f64`, so a downstream `scale`/`kv_metrics` reading a `logfmt`/`kv`-
produced attribute costs nothing extra -- proven by
`logfmt_scale_kv_metrics_keep_aggregate_chain_produces_correctly_tagged_metrics`
(`crates/logit-transforms/src/lib.rs`), which threads `request_time="0.012"` through `scale` into
`Value::F64(12.0)` exactly as the JSON-sourced version does.

**Namespace: flat `event.attributes`, no prefix** -- identical to `json`. **Duplicate keys:**
last-write-wins, for free, via `AttrMap::insert_sym` overwriting in push order -- identical to
`json`'s own policy, and to `merge_into`'s reuse of `JsonParser::scratch`'s exact "accumulate
separately, merge only on full success" shape, so a line that fails partway (an unterminated quote)
never leaves `event.attributes` half-populated.

**Allocation posture: a hand-rolled scanner that tracks its own byte offsets, not `syslog.rs`'s
pointer-arithmetic `slice_of` or `json.rs`'s guarded `borrowed_str_bytes`.** Neither of those
existing precedents' constraints apply here: `syslog.rs` needs pointer arithmetic because
`str::split` hands back offset-less `&str`s, and `json.rs` needs a guarded reconstruction because
`serde_json` may hand back an unescape scratch buffer outside the input buffer. This scanner never
loses its own indices, so every unquoted or escape-free-quoted value is a plain, infallible
`raw.slice(a..b)` -- a `Bytes` refcount bump, not a copy. Every key is interned straight off the
message's own byte slice (`&text[key_start..key_end]`), never through an owned `String` -- safe
because every delimiter these scanners split on is single-byte ASCII, so a byte range either
scanner ever slices always lands on a UTF-8 char boundary in the (already UTF-8-validated) message.
`AttrMap::insert_sym` is used for the merge, not `resolve()` -> `insert(&str)` -- `json.rs`'s own
comment calls that round trip out as a known wart; `logfmt`/`kv` simply don't reproduce it, but
`json.rs` itself is deliberately left untouched (a drive-by fix there would move an already-pinned
allocation row and confuse this diff).

## Alternatives considered

- **One `ComponentKind` (`kv`) with a `format: logfmt | literal` mode flag**, instead of two
  kinds. Rejected: the two grammars share no config fields at all (`logfmt` needs none beyond
  `bare_keys`; `kv` needs two required separators `logfmt` has no use for), so a merged config
  surface would need `Option`al fields gated on the mode anyway -- strictly more surface for no
  shared behavior, the same reasoning `keep_signals`/`drop_signals` already settled
  (`docs/adr/signal-filtering-components.md`).
- **Heroku-faithful default bareword-as-`true` semantics** (no `bare_keys` gate, always on).
  Rejected -- see "Decision" above: the interner-growth risk on a timestamp-prefixed line is real
  and immediate, and there's no `skip_to_brace`-equivalent way to route around a non-logfmt prefix
  for this grammar the way `json` can.
- **Numeric coercion for `logfmt`/`kv` values that look like numbers** (mirroring `json`'s
  type-by-syntax rule). Rejected: unlike JSON, logfmt/kv values are never typed by their own
  syntax -- `status=200` and `path=/a/b` look identical at the grammar level (an unquoted run of
  non-whitespace bytes), so "coerce when it parses as a number" would silently flip an attribute's
  type between events depending on what a particular line's value happened to look like, the exact
  hazard `docs/adr/scale-transform.md` rejected for `scale`'s own output. Every downstream
  consumer that wants a number already has `crate::numeric`.
- **Defaulting `kv`'s `pair_sep`/`kv_sep` to `" "`/`"="`** to make a bare `type: kv` usable
  out of the box. Rejected -- see "Decision" above: it would make `kv` a strictly worse `logfmt`
  for the one shape they'd overlap on (unquoted flat pairs), while silently mangling any quoted
  value a `logfmt`-shaped line actually needed `kv` never to see in the first place.
- **A `trim: bool` on `kv`** instead of always trimming. Rejected: there's no grammar reason a
  `kv` value (which can never be quoted) would need literal surrounding whitespace preserved, and
  always-on trimming is what makes `pair_sep: ","` behave identically on `a=1,b=2` and
  `a=1, b=2` without an operator needing to reason about which shape a given source emits.

## Consequences

- `logfmt`/`kv` ship tested (44 new tests across `crates/logit-transforms/src/logfmt.rs`,
  `crates/logit-cli/src/pipeline.rs`, and `crates/logit-pipeline/src/graph.rs`, plus a new
  integration test in `crates/logit-transforms/src/lib.rs`) but, like `otlp_in`, unexercised by
  `demo/`/`examples/` -- no shipped config needs one yet, and adding an example config for its own
  sake isn't this PR's job.
- A value containing `pair_sep` is not representable in `kv` -- an operator with that shape needs
  `logfmt` (which quotes) instead, or a different `pair_sep` choice. This is a real, documented
  limitation of a literal, unquoted splitter, not an oversight.
- `\uXXXX`/`\xNN` escapes are unsupported in `logfmt`'s quoted values (an unknown escape is kept
  verbatim, backslash included) -- no producer observed in this codebase's fixtures emits either,
  and adding them later is purely additive to `unescape`.
