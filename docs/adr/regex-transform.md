---
created: 2026-09-07
updated: 2026-09-07
---

# `regex`: named captures into attributes, and taking the `regex` crate

## Status
Accepted

## Context

`ComponentKind::Regex { pattern: String }` (`crates/logit-config/src/lib.rs`) has been a declared-
but-unimplemented variant since the component-graph work landed, carried alongside `logfmt`/`kv`/
`csv`/`rename`/`filter`/`sample`/`throttle`/`dedup` as `ComponentKind` variants with no
implementation behind them yet, so a config referencing one gets a clear "not implemented yet" at
validation time rather than a deserialization error. `regex` is the first of that list to land, for
a reason bound up in what LuaJIT actually gives an operator: it ships Lua patterns, not a general
parser or a real regex engine -- no alternation, no `\d`, no named captures.

That gap is not hypothetical. `demo/logit.yaml`'s `postgres_trace_lift` is a `lua` component whose
entire body is `string.match(msg, "traceparent='([^']+)'")` -- a Lua pattern standing in for a
regex, costing a dedicated OS thread and a LuaJIT VM per `docs/design/pipeline-graph.md`'s "Node
kinds" section, and (per `docs/design/memory.md`'s `lua` proxy row) roughly 9 allocations and ~1.07
microseconds per event where a native transform costs on the order of 1 allocation and a few
hundred nanoseconds. Lua patterns have no alternation, no `\d`, and no named captures, so anything
past a single literal-anchored extraction is not merely slow but unwritable.

What this record settles: which regex engine to depend on, how captures become attributes, what a
non-matching line does, and where an invalid pattern is caught.
[ADR `json-parsing-into-attributes`](json-parsing-into-attributes.md) fixes the semantics this
follows rather than re-decides -- additive merge into `event.attributes`, pass-through on failure,
last-writer-wins on collision.

## Decision

**The `regex` crate, default features -- not `regex-lite`, and not a hand-rolled matcher.** Both
engines are RE2-style with a linear-time guarantee (`regex-lite` is a PikeVM; neither backtracks),
so the catastrophic-backtracking argument does not discriminate between them. Two things do.
First, `regex-lite` ships only the PikeVM: no lazy DFA, no `memchr`/Teddy literal prefilters. On a
per-event hot path in a project that asserts exact allocation counts and publishes a ns/event
table (`docs/design/memory.md`), giving up the prefilter on a pattern like
`traceparent='(?P<tp>[^']+)'` -- where the literal prefix is the whole game -- is the wrong trade.
Second, `regex-lite` is ASCII-only for `\w`/`\d`/`\s`/`(?i)` and has no `\p{...}`; `pattern:` is
operator-supplied config usually copied from another tool, and a pattern that works everywhere
else and either fails at `logit validate` or, worse, silently stops matching non-ASCII is a bad
operator experience for a config-driven product.

**This is a real new dependency, not a promotion -- recorded plainly because the shape resembles
one and isn't.** `regex-lite 0.1.9` is in `Cargo.lock` today, but only as a dependency of `divan`,
itself a `[dev-dependencies]` entry of `logit-bench` ("Dev-only; ships in nothing"). It is not in
the release binary's closure. So neither engine is already shipped, and the "already present
transitively at this exact version" reasoning that justifies `socket2`, `libc`, and the rustls
stack in the root `Cargo.toml` does not apply here. The cost is four new crates -- `regex`,
`regex-automata`, `regex-syntax`, `aho-corasick` (`memchr` is already shipped via `serde_json`) --
and roughly a megabyte of Unicode tables. All pure Rust, so ADR `containerized-development`'s
"no host toolchain needed" holds; all MIT OR Apache-2.0 except `aho-corasick`'s `Unlicense OR MIT`,
which resolves through MIT under `deny.toml`'s allowlist exactly as the already-present `memchr`
does. `script/audit` is what confirms this, not this paragraph.

**Named captures only. The config surface is `pattern` plus an optional `field`.** Every named
group -- `(?P<name>...)` or `(?<name>...)` -- becomes an attribute of that name; an unnamed group
is grouping and alternation only. There is no positional `fields:` naming list, for two reasons: a
positional list silently desyncs when a group is inserted mid-pattern (graph validation can check
*count* but never *meaning*), and nothing else in this config addresses anything positionally --
`MetricSpec`'s `field`, `scale`'s map keys, `trace_context`'s three field names are all names.
Named-captures-only is the convention-consistent choice here, not the deviation.

**`field: Option<String>` names an attribute to match against instead of the log message.** Absent
(the default) reads `log.message`, like `json`. This exists for `demo/logit.yaml`'s postgres tier
and nothing else: the SQL statement arrives inside a Postgres jsonlog record, so `json` has already
lifted it to `attributes.message` by the time a pattern can be run over it. A speculative field
would not have earned its place; this one replaces a real committed `lua` component.

**A non-matching line passes through unchanged, with no diagnostic -- only a counter.** Pass-through
is the whole codebase's posture and is not re-litigated here. The *absence* of a
`Diagnostics::warn_throttled` is the decision, and it deliberately departs from `json`'s
`parse_failure`: `json`'s means "this claimed to be JSON and wasn't," a genuine malformation, while
a regex not matching a given line is routine operation. `demo/logit.yaml` already documents that
most Postgres lines carry no `traceparent` at all. This follows
[ADR `scale-transform`](scale-transform.md)'s own rule verbatim -- "a silent per-field skip is
documented behavior, not a failure worth a throttled diagnostic." Visibility is
`logit.transform.matched` and `logit.transform.matched.skipped`, mirroring `scale`'s
`scaled`/`scaled.skipped` and `kv_metrics`'s `derived`/`derived.skipped`; every non-contributing
path (no log, non-string message, missing `field` attribute, no match) increments the second.

**First match only, never `captures_iter`.** Under an additive merge into a flat `AttrMap`, a
second match's `(?P<status>...)` would simply overwrite the first's -- "last match wins" is a
semantic nobody would ask for. Making multi-match meaningful requires accumulating a `Value::Array`
per name, a materially different design that collides with the one-name-one-attribute model every
other transform uses. And the real multi-match log case -- pulling every `k=v` off a line -- is
what the sibling `logfmt`/`kv` components are for.

**A named group that does not participate, and one that matches the empty string, both produce no
attribute at all** -- not `Value::Null`, not `""`. `docs/design/data-model.md` already rules that
`""`, `"-"`, and `Null` all count as absent, so writing either would set a value this codebase's own
convention reads back as absent: a distinction with no downstream meaning that still costs an
`AttrMap` slot and a field in every sink's output. Treating both cases identically is also what
collapses them to one `end > start` check.

**Every capture becomes `Value::Str`. No numeric coercion.** This is not the same decision `json`
makes and is not a departure from it: `json` reads types from JSON's own grammar
(`visit_i64`/`visit_f64`), from the document. A regex capture has no type grammar, so coercing
would mean inferring one from the shape of the text -- something `json` never does. `scale`'s ADR
named the hazard exactly: a `(?P<id>\w+)` capturing `123` on one line and `abc123` on the next
would flap between `U64` and `Str` for the same attribute. It also costs nothing downstream --
`numeric` (`crates/logit-transforms/src/lib.rs`) already coerces a numeric `Str`, so
`regex -> kv_metrics` and `regex -> scale` work today with no coercion here -- and it preserves the
zero-allocation property below.

**Captures are zero-copy: a matched line allocates nothing.** `Regex::captures_read` fills a
`CaptureLocations` held on the transform (reused across events, the same idea as `JsonParser`'s
`scratch`) rather than allocating a `Captures` per line, and each capture becomes
`Value::Str(haystack.slice(start..end))` -- a refcount bump on the message buffer, never a
`String`. Matching is over `&str` via one `str::from_utf8` on the whole line, not `regex::bytes` --
that one scan simultaneously establishes every capture slice is valid UTF-8, so `Value::Str`'s
invariant holds with no per-capture check. Measured rows are in `docs/design/memory.md` section 2.

**The pattern is compiled inside `graph::resolve`, and compiled a second time in `build_spec`.**
Compiling at resolve time is what keeps `docs/deploying.md`'s preflight promise --
`validate_semantics` is literally `graph::resolve(config)?`, so a check placed anywhere else
escapes `logit validate`. The compiled `Regex` is then dropped, not threaded through:
`ResolvedComponent` carries no parsed artifact for *any* kind today, and one `Regex::new` per
component at process start -- on a path that already reads `lua_file` scripts and TLS certificates
off disk -- is not worth inventing a mechanism for. The validation rule also rejects a pattern with
no named group and an empty `field` name. A duplicate capture-group name needs no rule of its own:
the `regex` crate rejects it at compile time already.

## Alternatives considered

- **`regex-lite`.** Its appeal was that it is already in `Cargo.lock` -- but only via `divan`, a
  dev-dependency of a crate that ships in nothing, so it would have been just as new to the release
  binary. Once that argument evaporates, what remains is one crate instead of four against a
  PikeVM-only engine with no literal prefilters and ASCII-only Perl classes, on a per-event hot
  path. Rejected. Worth revisiting only if binary size becomes a stated constraint, which it isn't
  today (`logit` ships as a container image).
- **`regex` with `default-features = false` and a narrowed Unicode feature set.** Rejected: it
  reintroduces the exact "compiles in every other tool, fails here" failure mode that ruled out
  `regex-lite`, to save size in a product distributed as an image.
- **A hand-rolled matcher.** This project hand-rolls parsers rather than take dependencies
  (traceparent, RFC 3339, gRPC framing, span/trace ids), but every one is against a *fixed*
  grammar, trivially auditable. This component's grammar is supplied by the operator at run time.
  A partial hand-rolled regex engine would be the worst outcome -- a subset that fails on patterns
  users legitimately expect to work.
- **A positional `fields: Vec<String>` alongside `pattern`.** Rejected -- see the Decision.
- **Coercing numeric-looking captures to `I64`/`F64`.** Rejected -- see the Decision. Flagged as a
  cross-cutting question: `logfmt`, `kv`, and `csv` face the identical decision over identically
  untyped input, and all landed on the same rule (no coercion, always `Value::Str`).
- **Threading the compiled `Regex` through `ResolvedComponent` into `build_spec`.** Rejected: it
  leaks a concrete engine type into a struct that `logit-cli::pipeline`, `logit-cli::dot`, and the
  node runtime all consume, against a precedent (every kind re-derives from raw `ComponentKind`)
  that is currently without exception.
- **Setting a non-participating capture to `Value::Null`.** Rejected: `docs/design/data-model.md`
  already defines `Null` as absent, so the distinction would not survive the first consumer that
  reads it.
- **`captures_iter` for multiple matches per line.** Rejected as ill-defined under a flat
  attribute map, and unmotivated -- the use case belongs to `logfmt`/`kv`.

## Consequences

- **The shipped dependency graph grows by four crates**, the first non-trivial addition since the
  TLS stack. Compile time and binary size both go up measurably.
- **`logit-pipeline` now depends on `regex` solely to validate a pattern it then throws away.** The
  same shape a validate-time-only check already takes elsewhere in `graph::resolve` (e.g. an
  `otlp_out` `tls:` block's internal consistency, checked and discarded, never carried into
  `ResolvedComponent`) -- compiling and dropping a `Regex` is one more instance of it, not a new
  precedent.
- **A pattern's own errors become config errors.** A pattern exceeding the crate's default 10MB
  compiled-size limit fails at `logit validate` as `CompiledTooBig` rather than exhausting memory
  at run time.
- **`regex` and `lua` now overlap.** Anything `regex` extracts, a `lua` component could also
  extract; the overlap is accepted here because the capability genuinely differs (Lua patterns
  have no alternation and no named captures) and the cost differs by roughly 9x in allocations.
- **`docs/design/pipeline-graph.md`'s not-yet-landed list narrows again** -- `logfmt`, `kv`, and
  `csv` remain, alongside `rename`/`filter`/`sample`/`throttle`/`dedup` and `logit_in`/`logit_out`.
- **A capture can silently overwrite an attribute set earlier in the pipeline**, same as `json`'s
  own consequence. `field:` makes one new variant possible -- a capture overwriting the very
  attribute it was read from -- which is well-defined (the `Bytes` clone keeps the buffer alive)
  and occasionally useful.
