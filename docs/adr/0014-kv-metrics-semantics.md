# 0014 — `kv_metrics`: skip rules, numeric coercion, and no `tags:` field

## Status

Accepted

## Context

`docs/plans/0002-nginx-integration.md`'s workstream E needs a way to turn attributes already on an
event -- typically merged there by `json` from an nginx access-log line -- into metrics on that
*same* event: `nginx.requests` (a per-request counter), `nginx.bytes_sent` (from
`$body_bytes_sent`), `nginx.request_time` (a distribution from `$request_time`). Nothing in
`logit-transforms` does this yet; `aggregate` only merges metrics that already exist, and `json`
only produces attributes, never metrics.

Two things need pinning down before writing the config surface: what happens when a source field is
missing or unusable (real nginx access logs guarantee this happens routinely — `$upstream_response_time`
is `-` on a non-proxied request and a comma-separated list on a retried one), and whether this
component also owns *which* attributes become tags on the metrics it derives. Per `AGENTS.md`'s "a
new design decision worth remembering gets an ADR," those are what this record settles. `keep`/
`remove` (the companion allowlist/denylist transforms landing in the same PR) need no ADR of their
own — their behavior is mechanical once `kv_metrics` decides it doesn't own tag selection.

## Decision

**No `field` means `+1` per event (counter) or "set to 1" (gauge); a distribution with no `field` is
a config error, rejected at graph-validation time.** A distribution of nothing is meaningless, and
catching this at `logit validate`/`logit run`'s validation step (`crates/logit-pipeline/src/graph.rs`)
is strictly better than a runtime no-op nobody notices — the same reasoning as rule 7's "every
non-sink component has a consumer" catching a different silent-black-hole shape.

**A missing, non-numeric, or non-finite field skips *that metric for that event* — no metric
emitted, no error, no dropped event, and no effect on the event's other derived metrics or its log
half.** This is deliberately the common path, not an edge case: `kv_metrics` is built to point at
real, messy access-log fields, and `$upstream_response_time`'s `-`/list-on-retry shapes above are
exactly what this rule exists for. No diagnostic fires either — unlike `json`'s parse-failure
diagnostic (ADR 0010), a field being absent or non-numeric on some fraction of events is expected
steady-state behavior for a real nginx log, not a signal something is broken; a diagnostic that fires
on every request without `$upstream_response_time` set would be noise, not signal.

**Numeric coercion accepts `I64`/`U64`/`F64` directly, and a `Str` that parses cleanly to a finite
`f64`** — so `kv_metrics` works whether the upstream JSON quoted a number or not (`"status": 200` and
`"status": "200"` coerce identically). `Bool`, `Null`, `Bytes`, `Timestamp`, `Array`, and `Map` never
coerce; there's no reasonable numeric reading of any of them. Implemented as a private
`fn numeric(value: &Value) -> Option<f64>` in `crates/logit-transforms/src/kv_metrics.rs`, not as a
`Value::as_f64` method on `logit-core`: a general method that silently parses strings out of `Value`
would be a surprising, easy-to-misuse addition for every other caller of `Value` in the codebase —
keeping it local also keeps this workstream's footprint out of `logit-core` entirely.

**No `tags:` field on `kv_metrics`, deliberately.** A metric this component derives rides on the same
event it read attributes from, and every metrics sink already reads that event's attributes for tags
(`render_tag_suffix`, `crates/logit-outputs/src/influxdb.rs`) — there is nothing for `kv_metrics`
itself to select. Tag selection is `keep`'s job: an operator names the attributes that should survive
as tags once, in one place in the pipeline, rather than restating a tag allowlist on every metrics
producer that happens to run before a sink. This also keeps `kv_metrics` genuinely single-purpose:
reading numeric fields into metrics, nothing else.

**`keep` is an allowlist, not just a denylist, and this is the point of having both.** A denylist
(`remove`) only ever protects against fields a config author already knows exist; a field added to a
log format later (an nginx `log_format` gaining a directive) would silently become a new tag
dimension on every metrics sink downstream, with nothing in the config visibly wrong. `keep`'s
allowlist makes that structurally impossible: anything not explicitly named is dropped, known or not.
`remove` still exists, for the narrower case of pruning a small number of known-noisy fields (a
one-off debug attribute) without having to enumerate everything else that should survive.

**`keep` must sit before `aggregate` in a pipeline.** `aggregate`'s `SeriesKey` includes the whole of
`event.attributes` (`crates/logit-transforms/src/aggregate.rs`), so an attribute `kv_metrics` didn't
need but the event still carries (client address, full user agent, request path) would key a separate
series per distinct value if it reaches `aggregate` unpruned — exploding both series cardinality and
per-window memory. `keep` ahead of `aggregate` is what bounds the tag set aggregation ever keys on;
this is stated in `keep`'s own module doc comment (`crates/logit-transforms/src/keep.rs`) and belongs
in the reference nginx example (workstream F) and its accompanying docs (workstream G) too.

**Nested fields are not addressable.** `field: http.status` names an attribute literally called
`http.status`, not a `status` key nested inside a `Value::Map` under `http`. nginx's access-log
format is flat by construction, so nothing needs this today, and a path syntax invented without a
concrete consumer would need escaping rules for a literal dot in a real attribute name — speculative
complexity with no requirement driving it.

**Distributions produce a single-sample `DdSketch`**, exactly as `crates/logit-inputs/src/statsd.rs`
does for its `ms`/`h`/`d` metric types — one `DdSketch::new()` plus one `add`, left for `aggregate`
to merge across events the same way statsd-sourced timings already are.

**Metric names and units are interned once, at construction (`KvMetrics::new`), not per event.**
`intern`/`resolve` are hash lookups; this runs on the hot path once per configured metric per event,
so paying the lookup cost once at startup instead is a straightforward, free win.

**Metrics are appended to `event.metrics`, in config order (counters, then gauges, then
distributions), never replacing what's already there — and the event is always forwarded.**
`kv_metrics` never returns `None`; deriving zero metrics from one event (every field absent, say) is
exactly as valid an outcome as deriving all of them. `log`, `span`, `attributes`, and `timestamp` are
never touched — only `metrics` is written.

## Alternatives considered

- **A `tags:` field on `kv_metrics` naming which attributes to carry onto the derived metric.**
  Rejected: metrics ride on the same event as the attributes they're derived from, so every sink
  already sees the full attribute set regardless of anything `kv_metrics` would record — a `tags:`
  field here would either do nothing (attributes are visible either way) or require `kv_metrics` to
  actually *filter* `event.attributes`, which is `keep`'s job and would make the two components
  overlap and disagree about who owns tag selection.
- **A general `Value::as_f64` on `logit-core`.** Rejected — see "Numeric coercion" above: a method
  that silently parses strings out of any `Value` caller is a surprising API to add to a
  general-purpose type for one caller's convenience.
- **Emitting a diagnostic on every skipped metric**, matching `json`'s parse-failure diagnostic.
  Rejected: `json`'s failure is exceptional (a malformed line); a missing/non-numeric field here is
  routine and expected on a real access log (`$upstream_response_time`), so a diagnostic would either
  fire constantly (noise) or need its own separate throttling design with no clear signal it would
  actually surface.
- **Rejecting a non-numeric field as a hard pipeline error.** Rejected outright, for the same reason
  every other transform in this codebase avoids dropping telemetry over one bad value (ADR 0008, ADR
  0010): one un-parseable field on one event must not take down a metric derivation for the rest of
  that event, let alone the pipeline.
- **A dotted-path syntax for nested fields** (`field: http.status` reaching into a `Value::Map`).
  Rejected for now — see "Nested fields are not addressable" above; revisit if a real nested-JSON
  source needs it.

## Consequences

- A pipeline that runs `kv_metrics` without a following `keep` gets metrics tagged with *every*
  attribute the event happens to carry at that point — correct per this ADR's design (tag selection
  isn't `kv_metrics`'s job), but a config author who forgets `keep` gets high-cardinality tags with no
  error telling them so. Worth calling out in the reference example (workstream F) and its docs
  (workstream G), not fixed by validation here: `kv_metrics` has no way to know a downstream `keep`
  is or isn't coming.
- Silently skipped metrics (a missing/non-numeric field) are invisible in the running process — no
  counter, no log line — by design, but this means a config typo in a `field:` name (pointing at an
  attribute that will never exist) produces no metric and no error, ever. `logit validate` cannot
  catch this either: whether a named attribute exists depends on upstream data, not the config graph.
- `KvMetrics::new` interning names/units once means a `kv_metrics` component's `counters`/`gauges`/
  `distributions` lists are fixed for that component's lifetime — there is no way to add or rename a
  derived metric without restarting the process, same as every other config-driven component today.
