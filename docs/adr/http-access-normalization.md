---
created: 2026-09-22
updated: 2026-09-22
---

# `http_access`: web-server access lines normalized to OTel semconv once, natively, from raw fields

## Status
Accepted

## Context

An access log line from nginx, HAProxy, Apache, Varnish, Squid, Caddy, Envoy, or Traefik is the
richest telemetry most deployments produce about a request: it is a log record, the source of
several metrics, and — since [ADR `trace-context-span-lifting`](trace-context-span-lifting.md) —
a real `SpanRecord` on the same event. Getting it into a shape that a trace store, a metrics
sink, and a log store all find useful means conforming to the OpenTelemetry HTTP semantic
conventions: `http.request.method`, `url.path`, `http.response.status_code`, `server.address`,
`client.address`, `network.protocol.version`, `user_agent.original`; a span named `{method}
{route}` with a *low-cardinality* route, never the raw path; `span.status` left unset except on a
server error; a bounded attribute set on the metric side.

Today every operator has to do that themselves, on the server side, per server. A real nginx
deployment adapted from `examples/nginx/nginx.conf` grew to roughly a hundred lines of `map`
blocks to get there: one to rename each variable to its semconv name, one per string field to cap
it to N printable-ASCII bytes (`$request_uri`, `$args`, `$http_user_agent`, `$http_referer`,
`$http_traceparent`), one to turn nginx's `000` status into an integer `0` so the JSON line stays
parseable, one to turn `HTTP/1.1` into `1.1`, an ordered regex table classifying the user agent
into `browser`/`crawler`/`tool`/`scanner`/`none`/`other`, a second ordered table classifying the
path into a handful of routes to serve as the span-name suffix, a status-to-`span.status` map,
and a fourth `traceparent` regex to surface a malformed inbound header. Each of those is a
correct, defensible decision — and each is one *every* nginx user would have to rediscover, and
every HAProxy, Varnish, or Squid user would have to reinvent in a different config language with
different escaping rules. The two shipped nginx configs in this repo are a smaller version of
the same thing: three `map` blocks that exist only because nginx's `log_format` cannot call a
parser, and — found while writing this ADR — an unquoted `"status":$status` that produces invalid
JSON (`{"status":000}`, a leading zero) on every client-aborted request, losing the whole line.

That is the wrong place for the work. The server's job is to *report* what it saw; deciding what
is a bounded value, what is an attacker-controlled string, what a route is, and what semconv
wants is a pipeline concern, and it should be decided once, in one implementation, with tests.

Two prior ADRs constrain how. [ADR `operator-declared-resource-attributes`](operator-declared-resource-attributes.md)
settled that `logit`'s own code never *invents* a fact about data it didn't produce — an
operator declares one. [ADR `value-allowlist-cardinality-clamp`](value-allowlist-cardinality-clamp.md)
settled that bounding a field's value set is a named, bounded matcher (`keep_values`), not a
predicate language. And [ADR `trace-context-span-lifting`](trace-context-span-lifting.md)
already owns the trace/span attribute convention and the lift itself, with an explicit rejection
of a second component that would re-parse the same names.

## Decision

**A new native transform, `http_access`.** It reads the raw, unnormalized attributes a web server
logged under OTel semconv names, rewrites each one it finds into its conformant form, and fills in
whichever of a small set of derived attributes the line doesn't already carry. It is placed by
the operator between `json` (or whatever parsed the line) and `trace_context`:

```
syslog_in / docker_in / tail_in -> json -> http_access -> trace_context -> kv_metrics -> keep -> aggregate -> sink
```

**Applications send raw semconv names, not a `logit`-specific raw namespace.** An nginx
`log_format` writes `"url.original":"$request_uri"`, `"http.response.status_code":"$status"`,
`"user_agent.original":"$http_user_agent"` — the standard name, the untouched value. Nothing is
capped, coerced, or classified on the server. Three consequences follow, and all are wanted: a
pipeline *without* `http_access` still carries standard names, just unhardened; an operator or
an agent reading the config recognizes every key; and the conversion table from any server's
variables to the schema is a doc, not code. A few names are `logit`'s own because semconv has
none: two composites the component decomposes when their atomic parts are absent
(`http.request.line` = `GET /p?q HTTP/1.1`, `url.original` = `/p?q`), the request duration in
its unit-suffixed forms (`http.request.duration_{s,ms,us}`), and the proxy vocabulary
(`upstream.address`, `upstream.status`, `upstream.{duration,connect,header}_{s,ms,us}`,
`cache.status`, `network.connection.{id,requests}`, `http.termination_state`,
`http.response.compression_ratio`). Trace and timing names are the *existing* well-known table
in `docs/design/data-model.md`, unchanged: `traceparent`, `trace.id`, `span.id`,
`span.end_s`/`span.start_us`/`span.duration_ms`, and so on. The full table, per server, is
`docs/http-access-logs.md`.

**What it does to what it finds — and nothing to what it doesn't.** Composites are decomposed
into whichever atomic fields are absent, then removed. Numeric fields arrive as an integer or a
quoted numeric string and become `I64`; `"000"` becomes `0`. Durations in any suffixed unit
become `_s` (`F64`), the suffixed source removed. `http.request.method` is compared
case-sensitively against semconv's known set and, on a miss, becomes `_OTHER` with the raw value
preserved as `http.request.method_original` — the one place a pre-normalization value is kept,
because semconv itself asks for it. `network.protocol.version` loses its `HTTP/` prefix (`2.0`
becomes `2`). `url.query` loses a leading `?` and has the values of semconv's sensitive keys
(`AWSAccessKeyId`, `Signature`, `sig`, `X-Goog-Signature`, `X-Amz-Signature`, `X-Amz-Credential`,
`X-Amz-Security-Token`, extensible) replaced with `REDACTED` — before capping, so a secret can't
survive by being cut mid-value. Every free-text field is capped to a per-field character limit
(overridable) and any control byte in the capped prefix becomes `_`. Then the derived set:
`user_agent.class` from an ordered built-in regex table (config rules prepended), plus
`user_agent.synthetic.type = bot` for a crawler or scanner; `http.route` from the operator's
ordered `routes:` rules (a regex over the capped `url.path` and a *literal* route value, plus
three named built-in sets: `assets`, `well_known`, `probes`) with an optional `route_other`
fallback; `error.type` (the status code, 5xx only); `span.status` (`error` for 5xx or `0`,
otherwise `unset` — never `ok`, which semconv reserves for an explicit override and today's
configs write wrongly for every non-5xx response); `span.name` (`{method} {route}`, or `{method}`
alone when no route matched, `HTTP` in place of `_OTHER`); and `span.duration_s` mirrored from
`http.request.duration_s` unless the event already states a span duration, or both a start and
an end — the two shapes `trace_context` resolves on its own. A lone end (nginx's `$msec`) and a
lone start (HAProxy's `request_date(us)`) both get the mirror, which is what turns each into a
resolvable pair instead of a span with receipt time borrowed for its missing bound. Under an
explicit `forwarded: {trust: true}`, `client.address` is overwritten from the first hop of
`http.request.header.x-forwarded-for`; off by default because the header is client-supplied.

A field that is absent produces nothing — no default `url.scheme`, no `user_agent.class: none`
for a producer that doesn't log the header at all (an *empty* header is `none`; an *absent* one is
silence), no fabricated route. This is `operator-declared-resource-attributes`' rule applied one
component over.

**It fills what is missing, and never overrides a derived field the producer already sent.**
`http.route`, `user_agent.class`, `user_agent.synthetic.type`, `error.type`, `span.name`,
`span.status`, and the `span.duration_s` mirror are each written only when absent (by the same
absent rule as everything else: `""`, `-`, and null count as absent). Two audiences want this at
once. The drop-in user sends raw fields and gets every derivation. The advanced user, who wants a
route or a user-agent class *on the server* so it can drive a decision there — a rate limit, a
block, a different upstream — computes it in the server's own config language, logs it under the
standard name, and `http_access` honours it while still filling in everything they didn't bother
with. The same rule makes a second pass over an event a no-op, and makes `http_access` safe to
place on top of a pipeline that already carries the field from an upstream `set` or `lua` stage.
There is deliberately no `overwrite:` option: a producer that logs a derived field under its
standard name has made a decision, and a component silently reversing it is the failure mode this
ADR exists to remove, not a feature.

What `http_access` itself writes stays bounded by construction: an `http.route` it derives comes
from config or the built-in table, never from a capture group, and a `user_agent.class` from the
table. A producer's own value is the producer's — it is bounded by whatever bounded it there, and
`keep_values` after `http_access` is the tool if that needs enforcing in the pipeline. The review
question for any change here is therefore: does any value *this component writes* derive from the
input? It must not. Normalizing a value the producer did send — coercing its status to an
integer, capping its user agent, redacting its query — is not overriding; that hardening applies
regardless, which is what makes the drop-in and the server-side shapes meet in the middle.

The one deliberate overwrite is `client.address` under `forwarded: {trust: true}`: nginx and
HAProxy always log the peer address, so a fill-only rule would make the option a no-op. It is
gated behind an explicit operator statement about their own topology, and it replaces exactly one
field with exactly one thing.

**Best-effort per field, never all-or-nothing, and never a dropped event.** `trace_context` is
all-or-nothing per event because it writes *identity* — a half-lifted trace id is a corrupt trace.
`http_access` writes *descriptions*, where a half-normalized line is strictly more useful than an
untouched one: a status that fails to parse is left exactly as it arrived, counted
`invalid{field}`, and diagnosed once, while every other field is still normalized. `process`
always returns `true`.

**A dashed alias spelling for emitters whose key grammar forbids dots.** HAProxy's own JSON
encoder (`%{+json}o`) names each item with `%(name)`, and that grammar accepts only alphanumerics,
`_`, and `-` — verified against HAProxy 3.0's `src/log.c`, and the reason `demo/haproxy/haproxy.cfg`
hand-rolls its JSON today. So every canonical name is also accepted as the same name with each
`.` replaced by `-`: `http-request-method`, `url-path`, `user_agent-original`, `span-start_us`,
`http-request-header-x-forwarded-for` (a header key keeps its own dashes after the fixed prefix).
`-`, not `_`, because canonical names already contain `_` and an underscore spelling would not be
decodable without a lookup. The alias set is a **fixed table** of pre-interned pairs, never a
blanket `-`→`.` rewrite that would corrupt a legitimately dashed key; the dotted spelling wins
when both are present, the dashed key is removed on rename, and because `http_access` runs before
`trace_context`, it aliases the trace/timing names too, leaving `trace_context` dotted-only. A
name with no dots (`traceparent`) needs no alias; a non-canonical extra (`haproxy_timer_queue_ms`)
has none and should simply avoid dots.

**No metrics are emitted here.** The component guarantees the attribute names semconv's
`http.server.request.duration` and its low-cardinality attribute set expect; a stock
`kv_metrics` + `keep` block, shipped in the doc, produces the metric. One component per job, as
`set`/`keep_values`/`flatten` already are.

**`json` gains an opt-in `invalid_utf8: replace`.** nginx's `escape=json` escapes `"`, `\`, and
control bytes, but passes bytes `>= 0x80` through raw, so a client sending a Latin-1 `User-Agent`
or a percent-*decoded* `$uri` puts invalid UTF-8 into the line; `syslog_in` carries it as
`Value::Bytes`, and `json`'s strict parse then fails the *whole* line. That is the one thing
`http_access` cannot fix from behind `json`, and it is exactly the case the server-side
printable-ASCII `map`s were defending against. Under `replace`, a failed parse whose input is not
valid UTF-8 is retried on a `from_utf8_lossy` copy — failure path only, so a valid line's cost is
unchanged — counted and diagnosed once. The default stays `reject`.

**The repo's own configs move onto it.** `examples/nginx/nginx.conf` loses all three `map`s
(`trace_context`'s `mint_id: true` replaces the `$request_id`-prefix span-id trick, and a
malformed inbound `traceparent` now reaches `trace_context` raw and is counted `invalid` instead
of being blanked into `missing`); `demo/nginx/nginx.conf` keeps its propagation maps (it forwards
the `traceparent` it logs) and loses only `$span_status`; `demo/haproxy/haproxy.cfg` moves to
`%{+json}o` with dashed keys; both `logit.yaml`s insert the component and drop the `scale` stage
that existed only to reconcile nginx's seconds with HAProxy's milliseconds. `$status` is quoted
everywhere.

## Alternatives considered

- **Keep doing it on the server side, and document the recipe.** Rejected: it is the status quo,
  and the motivating config shows where it ends — a hundred lines per tier, per server dialect,
  with every escaping and quoting rule rediscovered independently, and no tests.
- **Per-server presets (`preset: nginx`), accepting native variable names.** Rejected: the
  component would then own every server's variable vocabulary and its drift; a
  `log_format`/`log-format`/`LogFormat` already lets the emitter choose its key names, so the
  mapping is a doc snippet per server, not code. Tracked in `docs/known-gaps.md` in case a server
  turns up whose key names are not choosable.
- **A `logit`-specific raw namespace (`http.raw.path`) renamed to semconv by the component.**
  Rejected: a pipeline without the component would carry non-standard names, and every user
  would learn two vocabularies for one field.
- **Underscore as the alias separator (`url_path`).** Rejected: `user_agent.original`,
  `span.start_us`, and `http.request.method_original` already contain underscores, so the
  underscore spelling is only decodable with a table, whereas the dashed one is a rule a human or
  an agent can apply from the dotted table alone. HAProxy accepts both characters.
- **A blanket `-`→`.` key rewrite, or a generic `rename` transform.** The rewrite is rejected
  because it corrupts any legitimately dashed key. A generic `rename` component is a fine tool in
  its own right but the wrong answer here: it hands every HAProxy user a twenty-line mapping
  block, which is the burden this ADR exists to remove.
- **Doing it in Lua.** Expressible today, and `demo/logit.yaml`'s postgres tier did exactly this
  kind of extraction in Lua before [ADR `regex-transform`](regex-transform.md) moved it native. Rejected as the
  only path for the same reason: a Lua stage costs a VM per worker and ~9 allocations per event
  (`docs/design/memory.md`), Lua patterns have no alternation or named captures, and the whole
  point is that nobody should have to write this logic at all.
- **Emitting `http.server.request.duration` and friends from the component.** Rejected: the
  `kv_metrics` + `keep` pair already does it, and a second implementation of "field to
  distribution" inside one transform is the shape `set`/`keep_values`/`flatten` all avoided.
- **All-or-nothing per event, matching `trace_context`.** Rejected for the reason given in the
  decision: this component writes descriptions, not identity, and a visible partial result beats
  a silently untouched line.
- **A `keep_source` knob, matching `trace_context`.** Rejected: nothing here moves off the
  attribute map onto a typed field, so there is no duplicate to keep. The only consumed inputs
  are the two composites (whose atomic parts are strictly more useful) and the suffixed duration
  forms (where keeping two spellings of one quantity is the contradiction `trace_context` already
  refuses). `http.request.method_original` is preserved unconditionally because semconv says so.
- **`RegexSet` for the user-agent table.** Rejected on measurement: `RegexSet::matches` allocates
  a `Vec<bool>` per call, and a single set cannot express class *priority* (a UA claiming both
  `Mozilla/` and `bot` is a crawler) — an ordered `Vec<(class, Regex)>` scanned with `is_match`
  can, and allocates nothing.
- **An untagged enum for `routes:` entries (built-in vs. pattern).** Rejected: serde's untagged
  failure is "did not match any variant" with no pointer to the offending key. One flat struct
  with optional `builtin`/`match`/`route` fields, validated by a graph rule to be exactly one
  shape, gives an operator an error that names what is wrong.
- **Overwriting a producer-sent derived field, or an `overwrite:` option to do so.** Rejected.
  The original draft of this ADR wrote every derived field unconditionally so that every value
  was bounded by construction; that made `http_access` clobber a real router's `http.route` or a
  server-side user-agent class, which is exactly the "the server may want to do some of this
  itself" case the drop-in shape must coexist with. Fill-only costs nothing when the producer
  sent nothing, and an option to reverse it would only ever be reached for by an operator who
  should instead stop sending the field.
- **Minting a trace id when no `traceparent` arrived.** Never — [ADR
  `trace-context-span-lifting`](trace-context-span-lifting.md)'s rejection stands. The edge that
  minted the request id is the only party that can mint the trace; nginx's `map $http_traceparent
  $trace_id { default $request_id; ... }` is the correct, two-line, server-side answer, and the doc
  says so.

## Consequences

- **Config surface:** `ComponentKind::HttpAccess { routes, route_other, user_agent_rules,
  max_length, redact_query, forwarded }`, every field optional — a bare `type: http_access` is
  meaningful, so unlike `keep_values`/`flatten` there is no "nothing configured is a no-op" rule.
  Graph rule 60 compiles every pattern at validate time (rule 31's reasoning) and rejects the
  usual empties, a duplicate built-in, a `max_length` key that names nothing this component caps,
  and `forwarded: {trust: false}` (omit the block instead). `CAPPED_FIELDS` and its defaults live
  in `logit-config` so the rule and the transform cannot disagree. `ComponentKind::Json` gains
  `invalid_utf8: reject | replace`. Schema regenerated.
- **Allocation contract**, enforced by `crates/logit-bench/tests/allocations.rs` and recorded in
  `docs/design/memory.md`: zero for an event with no HTTP attributes; zero (or one `AttrMap`
  spill, whichever is measured) for an already-conforming line on a warm component — every
  constant is `Bytes::from_static`, every substring is a `Bytes::slice`, every config-derived
  value is a pre-built `Value` cloned by refcount; plus one per field that needed control-byte
  cleaning and one per redaction. The numbers are pinned from a measurement, not this ADR.
- **Telemetry**, all `&'static`-tagged from closed tables: `logit.transform.http_access.{normalized,
  derived,truncated,cleaned,invalid}{field=…}`, `.redacted`, `.routed{outcome=rule|builtin|other|
  none}` — `outcome`, not the route value, since route names are operator-declared and unbounded
  in number. Three throttled diagnostics for genuine producer malformation: `bad_request_line`,
  `bad_status`, `bad_duration`. An absent field, an unknown method, an unclassifiable UA, and an
  unrouted path are normal traffic and get counters only.
- **`span.status` stops being `ok` in the demo and the example.** Semconv leaves a server span's
  status unset on success and on a 4xx; today's `map`s write `ok` for everything non-5xx. Tempo's
  colouring changes, and any dashboard filtering on `status = ok` loses rows. Correct, and worth
  a line in `demo/README.md`.
- **The demo loses its only `scale` stage.** It existed to turn nginx's seconds into HAProxy's
  milliseconds; with both tiers normalized onto `http.request.duration_s` it has nothing to do.
  `scale` stays a real, tested component (`AGENTS.md`: the demo isn't meant to stay exhaustive).
- **Some names in an otherwise-semconv table are `logit`'s own** — `http.request.duration_s`,
  `upstream.*`, `cache.status`, `user_agent.class`. They read as semconv but are not. If semconv
  later defines a conflicting attribute under one of them, the rename is a breaking config change;
  pre-release, that is the accepted cost of not prefixing them `logit.` and breaking the
  unit-suffix convention `span.duration_s` already set.
- **Known gaps**, recorded in `docs/known-gaps.md`: no per-server presets; XFF trust is
  all-or-nothing (no trusted-proxy list, no hop count); route rules are regex-only (no path
  templates, no prefix trie) and matched O(rules) per event with no prefilter; the UA table is a
  heuristic bucket classifier, not a parser (no `user_agent.name`/`.version`, and the built-in
  table cannot be disabled, only pre-empted); the percent-*decoded* form of a path is never
  produced, so route rules match the encoded form.
- **Docs:** `docs/http-access-logs.md` is the operator-facing schema for both audiences: it
  states, in the order the component applies them, what the normalizations and derivations are
  and which of them an advanced user might want to do server-side, and points at the source and
  its corpus tests for anyone who wants an exact match — the canonical field table,
  start-versus-end timestamp semantics per server, unit suffixes, nginx `escape=json` quoting
  rules, the dashed-alias section with the HAProxy `%{+json}o` worked example, one copy-pasteable
  snippet per server, the `kv_metrics` + `keep` metrics block, and cutover advice.
  `docs/deploying.md`'s nginx recipe points at it; `docs/design/data-model.md`'s well-known
  table gains a pointer and the alias note; `docs/design/pipeline-graph.md` and
  `docs/design/internal-telemetry.md` gain the rule and the counters.
