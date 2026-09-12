---
created: 2026-09-11
updated: 2026-09-11
---

# Prometheus scrape ingestion and exposition: transports, dialects, and the model mapping

## Status
Accepted

## Context

`docs/OVERVIEW.md` names Prometheus in `logit`'s scope, and
[`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md)'s "Prometheus exposition
format / OpenMetrics" and "Prometheus remote-write" sections survey what the wire can express, but
no `prometheus_in`/`prometheus_out` `ComponentKind` and no `logit_proto::prometheus` codec exist
yet — Prometheus is the one protocol in the survey with nothing built against it.

[ADR `lossless-transit`](lossless-transit.md) requires every `P_in`/`P_out` pair to be a lossless
relay modulo a named list of permitted normalizations, and the model work it drove
([`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)'s W1/W2/W4/W5, all landed) gave
`Event`/`MetricRecord` exactly what Prometheus needs: `Sum { value, temporality, monotonic }`,
`Histogram { buckets, sum, min, max }`, `ExponentialHistogram`, `Summary { count, sum }`,
exemplars, `start_timestamp`, `description`, `unit`, `flags`, and batch-level `Scope`. This is the
first like-protocol pair added *after* the internal model became a superset of every protocol
surveyed, rather than the third or fourth pair to retrofit fidelity onto after the fact
(`statsd_in`/`statsd_out`, `otlp_in`/`otlp_out`, `syslog_in`/`syslog_out` each needed their own
amendment once the model grew). It should land lossless from day one instead of joining the debt
`lossless-transit` was written to close.

Why now, and why scrape-and-exposition first rather than remote-write: every Prometheus
deployment already scrapes `/metrics` endpoints and exposes its own; remote-write is a newer,
protobuf-and-Snappy transport that not every consumer speaks yet, and requires vendoring protobuf
types `logit` doesn't otherwise need. Building the text-format semantic mapping first, in a shape
remote-write can reuse without touching it, is the smaller first step that still covers the common
case end to end.

Decisions settled with Ross (2026-09-11), recorded in full in
[`docs/plans/prometheus-scrape-and-exposition.md`](../plans/prometheus-scrape-and-exposition.md):
scrape-and-exposition now, remote-write as a non-breaking future addition to the same two kinds;
both text dialects negotiated on `Accept`/`Content-Type`; delta temporality resolved by `aggregate`,
not the sink; `prometheus_in` always synthesizes `up`/`scrape_duration_seconds`/
`scrape_samples_scraped`.

## Decision

### Transports

`prometheus_in` and `prometheus_out` cover exactly one transport kind each for now:
`prometheus_in` **scrapes** — it pulls `/metrics` from a configured list of targets on an
interval, the way Prometheus's own server does. `prometheus_out` **exposes** — it serves a
`/metrics` endpoint on a bound address for something else to scrape, the way every Prometheus
client library's HTTP handler does. Neither kind receives or sends remote-write today. This keeps
each kind's v1 surface matched to the single most common Prometheus integration shape (a `logit`
edge instance scraping node/service exporters; a `logit` instance being scraped in turn) rather
than trying to cover receive-and-send in the same release. See "Remote-write forward
compatibility" below for how the same two kinds grow into it later without a breaking change.

### Dialects and negotiation

Both text exposition dialects are supported, on both kinds: Prometheus text format `0.0.4` and
OpenMetrics `1.0`. `prometheus_in` parses whichever dialect a scraped target sent, determined from
the response's own `Content-Type` header (`application/openmetrics-text` selects OpenMetrics;
anything else, including a missing header, is treated as text `0.0.4`) — it does not send an
`Accept` header that forces one dialect, so it works against exporters that only understand the
older format. `prometheus_out` negotiates the other direction: it inspects the scraping client's
`Accept` header and serves OpenMetrics when the client asks for it, text `0.0.4` otherwise — the
default a client gets when it sends no `Accept` header at all (or a bare `*/*`) matches what
Prometheus's own server does when scraped by an old client.

The two dialects disagree on timestamp units, which the codec (not this ADR) has to get exactly
right in both directions: OpenMetrics timestamps and the `_created` series are **float seconds**;
text `0.0.4` timestamps are **integer milliseconds**. A value crossing from one dialect to the
other on a relay converts, it never reinterprets the same digits under the other unit.

### Model mapping

**Decode (families → `Event`s).** One `Event { timestamp, attributes: labels, metrics: [one
MetricRecord] }` per series; labels become `Value::Str` attributes verbatim.

| Wire | Model |
|---|---|
| `counter` sample `X` (name kept verbatim, `_total` included) | `Sum { value, Cumulative, monotonic: true }`, `name = X` |
| `gauge` | `Gauge(v)` |
| `histogram` (`_bucket{le}` cumulative, `_sum`, `_count`) | `Histogram { buckets: per-bucket counts by successive difference, trailing (+Inf, n), Cumulative, sum, min/max: None }` |
| `gaugehistogram` (OM, `_gsum`/`_gcount`) | same `Histogram` + attr `prometheus.type: "gaugehistogram"` |
| `summary` (`{quantile}`, `_sum`, `_count`) | `Summary { quantiles, count, sum }` |
| `untyped` / `unknown` / no `# TYPE` | `Gauge(v)` + attr `prometheus.type: "untyped"` \| `"unknown"` |
| `info` (OM, `X_info{...} 1`) | `Gauge(1)` + `prometheus.type: "info"`, `name = X` (family name, suffix stripped) |
| `stateset` (OM) | one `Gauge(0\|1)` per state line + `prometheus.type: "stateset"` |
| `# HELP` / `# UNIT` | `description` / `unit` (interned) |
| `_created` (OM) | `start_timestamp` (seconds → ns) |
| sample timestamp | `Event::timestamp` + marker attr `prometheus.timestamp: true`; absent → scrape start time, no marker |
| OM exemplar | `Exemplar { value, timestamp, trace: TraceRef when trace_id/span_id labels are valid hex (consumed), filtered_attributes: remaining labels }`; histogram bucket exemplars collect onto the record |

**Encode (`Event`s → families)** is the inverse, plus rules for model kinds Prometheus's wire
can't carry natively:

| Model | Wire |
|---|---|
| `Sum{Cumulative, monotonic}` | `counter`; text mode: name verbatim; OM mode: family = name with trailing `_total` stripped, sample `<family>_total`, `_created` from `start_timestamp` when non-zero |
| `Sum{Cumulative, !monotonic}` | `gauge` (Prometheus has no non-monotonic counter) — counted `logit.output.metrics.degraded{metric_kind="non_monotonic_sum"}` |
| `Sum{Delta}` / `Histogram{Delta}` | **skipped**, `logit.output.metrics.skipped{metric_kind="delta_sum"\|"delta_histogram"}` + `warn_throttled("delta_temporality_unresolved")` naming `aggregate`'s `temporality: cumulative` |
| `Gauge` | `gauge`; `prometheus.type` attr (consumed) overrides the family type to `untyped`/`unknown`/`info`/`stateset` |
| `GaugeDelta` | skipped, `warn_throttled("gauge_delta_unresolved")` (shared key across sinks) |
| `Histogram{Cumulative}` | `histogram`: running-sum buckets, `+Inf` = total, `_sum` only when `Some`, `_count` = bucket total; `min`/`max` dropped (known-gaps row); `prometheus.type: gaugehistogram` → `_gsum`/`_gcount` |
| `Summary` | `summary` + `_created` (OM) |
| `Distribution(sketch)` | `summary` with `DISTRIBUTION_QUANTILES` (reused from `otlp/metrics.rs`, made `pub(crate)`) and `_count`, **no `_sum`** (a sketch has none; OM permits omission) — `degraded{metric_kind="distribution"}` |
| `Samples` | `s.sketch()` then as above — `degraded{metric_kind="samples"}` |
| `Set` / `SetMembers` | `gauge` of `estimate()` / distinct count — `degraded{metric_kind="set"\|"set_members"}` |
| `ExponentialHistogram` | skipped, `skipped{metric_kind="exponential_histogram"}` (text has no native-histogram syntax) |
| `flags & NO_RECORDED_VALUE` | skipped, `skipped{reason="no_recorded_value"}` |
| exemplars | OM only, on `_total`/`_bucket` lines (bucket chosen by value); text mode drops them (dialect choice, not counted) |
| `Event::timestamp` | emitted only when `prometheus.timestamp: true` is present (consumed); ms in text, float s in OM |
| labels | `crate::attrs::merged(resource, event)` (event wins), skipping `prometheus.*`; `Value::Str/I64/U64/F64/Bool` stringified; `Null/Bytes/Timestamp/Array/Map` dropped — `logit.output.labels.dropped{reason="unrepresentable"}` |
| names | see "Names and sanitization" below |
| `unit` / `description` | `# UNIT` (OM only) / `# HELP` |
| `EventBatch::scope`, `Resource.schema_url`, `dropped_attributes_count` | dropped (known-gaps rows) |

### Attribute conventions

Four `prometheus.*` well-known attributes (`docs/design/data-model.md`'s table, rule (b) of
[ADR `lossless-transit`](lossless-transit.md): a protocol's own raw encoding of something the
model already normalizes rides as a protocol-namespaced attribute that outranks the normalized
field on that protocol's own egress) — all consumed by `prometheus_out`, never rendered as an
ordinary label there, though every other sink treats them as plain tags like any other attribute:

- `prometheus.type` (`Value::Str`): the wire family type when the model has no distinct kind for
  it (`untyped`, `unknown`, `info`, `stateset`, `gaugehistogram`).
- `prometheus.timestamp` (`Value::Bool(true)`): the sample carried its own timestamp on the wire;
  `prometheus_out` re-emits one on that line only. Same shape as the planned `statsd.timestamp`.
- `prometheus.instance` (`Value::Str`, a **resource** attribute): `host:port` of the scraped
  target — what Prometheus's own `instance` label holds.
- `prometheus.target` (`Value::Str`, a **resource** attribute): the full scrape URL.

Both `instance` and `target` are factual, like `docker_in`'s `container.*` attributes — neither is
an invented `service.name`. `job` is deliberately **not** stamped by `prometheus_in`: it is
operator identity (which scrape config this target belongs to), not a fact about the target
itself, and Prometheus's own server derives it from the scrape config, not the target's response.
An operator gets the same result explicitly with a downstream `set` component (or Lua) stamping
`job` from whatever they know about the topology. Because `prometheus_out` never renders
`prometheus.*` attributes as labels, `prometheus_in -> prometheus_out` with no transforms is an
exact fixed point on labels: nothing `prometheus_in` stamped leaks onto the exposed series as an
unwanted label, and an operator who wants federation-style `instance`/`job` labels sets them
explicitly.

### Names and sanitization

A metric name must match `[a-zA-Z_:][a-zA-Z0-9_:]*`; a label name must match
`[a-zA-Z_][a-zA-Z0-9_]*`. Every other byte is replaced with `_` — substitution, not deletion, so
two distinct names that differ only in a forbidden byte don't collapse onto the same wire name
silently (the same reasoning [ADR `statsd-output`](statsd-output.md)'s "Sanitization" section gives
for statsd's own name/tag rules). A name that begins with a digit gets a leading `_` prefix instead
of substituting the digit itself, since `_1` remains a legal identifier where `1` would not.
Because sanitization is a many-to-one substitution, two source labels can collide after
sanitizing (e.g. `a.b` and `a-b` both becoming `a_b`); the rule is first-wins — the first label in
attribute-map order keeps the sanitized name, and every later collision on the same series is
dropped and counted `logit.output.labels.dropped{reason="collision"}` rather than silently
overwriting the first value or emitting two labels with the same name (which the exposition
grammar cannot represent).

### Permitted normalizations for this pair

Per [ADR `lossless-transit`](lossless-transit.md), the following count as normalization, not loss,
for `prometheus_in -> prometheus_out`, and are what the round-trip fixed-point test asserts
equality modulo:

- Family/series reordering: families sorted by name, series within a family sorted by label set.
- Label reordering within a series (sorted).
- Float formatting: shortest round-trip representation (`1.0` renders as `1`).
- `# TYPE x untyped` synthesized for a family that arrived with no `# TYPE` metadata at all.
- `_created`, `# UNIT`, and exemplars dropped when the *output* dialect is text `0.0.4` — none of
  the three has a text `0.0.4` wire representation, and the operator chose that output dialect.
- `# EOF` presence: written when (and only when) the output dialect is OpenMetrics, per that
  dialect's own grammar.
- Blank lines and comments other than `HELP`/`TYPE`/`UNIT` dropped.

### Temporality is `aggregate`'s job

`prometheus_out` does not accumulate a delta `Sum` or delta `Histogram` into a running total
itself. A delta-temporality record reaching the sink is skipped and counted
(`logit.output.metrics.skipped{metric_kind="delta_sum"|"delta_histogram"}`), with a throttled
diagnostic naming the fix. This follows directly from `lossless-transit`'s "summarization is
opt-in and named" rule: turning a stream of deltas into a running cumulative total is exactly the
kind of information-discarding decision that rule reserves for a component whose stated purpose is
to summarize, not for a codec or a sink to do implicitly on the way out the door. `aggregate`
gains a `temporality: cumulative` mode (an amendment to
[ADR `aggregation-window-semantics`](aggregation-window-semantics.md)) that is that named stage: a
delta accumulator survives flush and keeps summing, emitting `Sum{Cumulative}` /
`Histogram{Cumulative}` with `start_timestamp` pinned to the series' first-seen time. With it,
both `statsd_in -> aggregate(cumulative) -> prometheus_out` and
`internal -> aggregate(cumulative) -> prometheus_out` work end to end with the summarizing step
explicit and named in the config, exactly where an operator reading the pipeline would look for
it.

### Synthetic scrape metrics

`prometheus_in` always emits three synthetic series per scrape, on the same resource as the
scraped target's own series: `up` (`Gauge(0|1)`, whether the scrape succeeded), plus
`scrape_duration_seconds` and `scrape_samples_scraped` (both `Gauge`) — exactly what Prometheus's
own server synthesizes for every target it scrapes. These are always on, not configurable off at
the source, because per-target scrape liveness cannot be expressed as `logit`'s own internal
telemetry: this repo's telemetry tags are constrained to `(&'static str, &'static str)` pairs
precisely to bound cardinality against the process-wide interner (`AGENTS.md`, `docs/known-
gaps.md`), and a target URL or `host:port` is neither `&'static` nor safe to intern per-target —
it's exactly the kind of value that convention exists to keep out of a tag. Emitting `up` as an
ordinary metric on the scraped resource, the way Prometheus itself does, sidesteps the problem
entirely: cardinality is bounded by the number of configured targets, which the operator already
controls, and the series lives in the pipeline where an operator who doesn't want it can drop it
downstream with an ordinary Lua filter — droppable, not omittable at the source, because the
alternative (a config flag suppressing it) would just be a worse version of the filter an operator
can already write.

### Exposition state and expiry

`prometheus_out` is a stateful sink: `send` upserts into an in-memory registry keyed by series
(family name + sorted label set), and the bound HTTP server renders the current registry contents
on each scrape request rather than replaying a stream of deliveries. Two knobs bound that state:

- `expire_after` (default **5 minutes**): a series not updated within this window is dropped from
  the registry and stops appearing in the exposition. Five minutes is not an arbitrary default —
  it matches Prometheus's own staleness horizon (the window after which its query engine treats a
  series with no new sample as stale and stops returning it), so an operator relying on
  `prometheus_out` behaves the way a Prometheus-native exporter already would. `0s` disables
  expiry entirely (series accumulate until explicitly evicted by the cardinality cap or the
  process restarts).
- `max_series` (default **100000**): a hard cap on distinct series held in the registry. Once
  reached, the least-recently-updated series is evicted to admit a new one — the same
  least-recently-used shape `aggregate`'s own `max_retained_gauge_series` cap already uses for
  bounding retained gauge state, applied here to bound registry memory instead of window memory.
  Every eviction is counted (`logit.output.series.evicted{reason="expired"|"cardinality"}`).

A record whose type conflicts with its family's already-registered type (e.g. a series named `x`
arrives as a `gauge` after having been registered as a `counter`) replaces the family's type and
evicts every existing series under the old type, counted
`logit.output.metrics.type_conflict` — the exposition format requires exactly one `# TYPE` per
family name, so there is no way to represent both interpretations at once, and silently keeping
the old type would mean silently dropping the new sample instead.

`send` only ever mutates in-memory state under a lock; it never performs network I/O and can never
partially apply a batch such that a redelivery would corrupt state — a redelivered batch just
upserts the same values again, `write_loop`'s retry needs no help from this sink to make a resend
safe. `duplicate_safe() -> true` follows directly.

### `Output::bind`

`logit_pipeline::Output` gains `async fn bind(&mut self) -> anyhow::Result<()>` (default `Ok(())`,
mirroring `Input::bind`'s contract exactly: idempotent, called by the runtime's pre-spawn pass in
sorted id order before any node task starts, and lazily invoked by `run_output` itself if nobody
called it first). `prometheus_out` is the first sink that needs it — it's the first sink that
listens rather than only connecting outward, so its startup failure mode (the configured `bind:`
address already in use, permission denied on a privileged port) is exactly the failure mode
`Input::bind`'s pre-pass already exists to turn into a startup failure (process exit code 1, with
nothing else running yet) instead of a runtime one discovered only when the first scrape request
comes in and nothing answers. Every input already gets this property; a sink that opens a
listening socket has the identical property to guarantee, so it needs the identical mechanism, not
a sink-specific ad hoc one. The runtime's node-spec dispatch and `readiness.set_node(id,
NodeState::Bound)` bookkeeping extend to `NodeSpec::Output` alongside `NodeSpec::Input` to make
this true.

### Remote-write forward compatibility

Nothing here is built yet; this section fixes the seam so that adding it later is additive to
`prometheus_in`/`prometheus_out`, never a third `_in`/`_out` pair or a breaking change to config
already shipped:

- The semantic mapping between the wire and the model already lives behind `MetricFamily`
  (`logit_proto::prometheus::mod`), independent of the text syntax that `text.rs` implements over
  it. A future `remote_write.rs` maps prompb messages to and from the same `MetricFamily` type, so
  none of the "Model mapping" tables above change when remote-write lands — only a second syntax
  module is added beside `text.rs`.
- `prometheus_in` gains an optional `bind:` field (a remote-write receiver) on the same
  `ComponentKind::PrometheusIn` variant, with a graph rule requiring exactly one of `targets`/
  `bind` to be set. `prometheus_out` gains an optional `endpoint:` field (a remote-write sender)
  under the identical "exactly one of `bind`/`endpoint`" shape. Both are purely additive fields on
  existing config shapes, not new kinds — an existing `prometheus_in`/`prometheus_out` config
  keeps working unchanged.
- The vendored prompb types live under `crates/logit-proto/proto/prometheus/`, regenerated by
  `tools/protogen` the same way the OTLP protos are
  ([ADR `committed-pregenerated-otlp-protobuf`](committed-pregenerated-otlp-protobuf.md)): no
  `protoc` invocation in any build path, generated output committed. `snap`, the Snappy
  compression remote-write requires on the wire, is BSD-3-Clause, already an allowed license in
  `deny.toml`.
- `prometheus.*` attributes and the name/label sanitization rules above are dialect- and
  transport-independent — they're about the model↔`MetricFamily` mapping, not about text syntax —
  so a remote-write relay is a fixed point on the same terms as the text-format one, with no
  separate attribute convention to design later.

### No `logit_proto::Encoder`

Same reasoning as [ADR `statsd-output`](statsd-output.md)'s "No `logit_proto::Encoder`" section:
that trait returns one opaque `Bytes` per batch with no framing metadata, and both `prometheus_in`
and `prometheus_out` are stateful in a way a stateless per-batch `Decoder`/`Encoder` can't express
— `prometheus_in` is an HTTP client polling on its own interval, not a batch decoder invoked per
incoming frame, and `prometheus_out` renders its whole current registry on demand from a scrape
request, not one batch at a time. The codec instead exposes plain functions
(`families_to_events`/`events_to_families`, `text::parse`/`text::write`) plus a
`PrometheusDecoder { telemetry, diagnostics }` / `PrometheusEncoder` pair with
`with_telemetry`/`with_diagnostics` builders for their own counters, the same shape `syslog_out`/
`statsd_out` already established.

## Alternatives considered

- **Remote-write first.** Rejected for now: it needs vendored protobuf types plus Snappy framing
  before anything at all can round-trip, where every existing Prometheus deployment already
  scrapes `/metrics` and exposes it — text-format scrape-and-exposition covers the common case
  with less new surface, and is a strict subset of what remote-write needs from the model mapping
  anyway (see "Remote-write forward compatibility").
- **One `ComponentKind` per transport** (`prometheus_scrape_in`/`prometheus_remote_write_in`, and
  the symmetric pair on the output side), rather than one kind per direction with the transport
  chosen by config field. Rejected in favor of `otlp_in`'s own precedent: `protocol: http | grpc`
  on one `OtlpIn` variant, not two kinds. A scrape and a remote-write receive are the same
  direction (data arriving at `logit`) with the same downstream shape (an `EventBatch` on the
  input's `Fanout`); splitting them into separate kinds would only duplicate the graph-rule and
  registry wiring `otlp_in`'s single-kind, mode-by-field precedent already avoids.
- **Accumulating delta temporality into a running total inside `prometheus_out` itself.** Rejected:
  this is exactly the per-series accumulator state `aggregate` already owns, and doing it again in
  the sink would mean two independent pieces of code both claiming to track a series' running
  total — a correctness hazard the moment they disagree after a partial restart of one but not the
  other — and it violates `lossless-transit`'s "summarization is opt-in and named" rule by doing
  silently, inside a sink, exactly the kind of information-discarding decision that rule reserves
  for a named, visible-in-config stage.
- **Stamping `instance`/`job` as plain labels on decode, federation-style**, the way Prometheus's
  own federation endpoint adds them. Rejected: it breaks the exact label fixed point
  `prometheus_in -> prometheus_out` otherwise has — a label `prometheus_in` added unconditionally
  would show up as an actual exposed label on `prometheus_out`'s output whether or not the operator
  wanted it there, with no way to remove it short of an explicit drop. Attribute conventions,
  consumed at the sink instead, give an operator the identical federation-style result with one
  explicit `set` — the same outcome, opt-in rather than assumed.
- **UTF-8, quoted metric/label names (the Prometheus 3 "UTF-8 names" feature).** Deferred, not
  rejected: it's a real feature of newer Prometheus versions, but adopting it now would mean
  designing the sanitization/collision rules above twice — once for the ASCII-safe grammar every
  exporter still targets today, and once for the quoted grammar few consumers speak yet. Tracked
  as follow-up work once a concrete consumer needs it.

## Consequences

- `prometheus_in`/`prometheus_out` become the fourth like-protocol pair under
  [ADR `lossless-transit`](lossless-transit.md), alongside `statsd`, `otlp`, and `syslog` — and the
  first one built lossless against the current model from its first PR rather than retrofitted.
- `logit_pipeline::Output` grows a `bind` method with a default no-op impl; every existing `Output`
  implementer is unaffected, and the runtime's pre-spawn bind pass and `NodeState::Bound`
  bookkeeping extend from inputs-only to inputs-and-outputs.
- `aggregate` gains a `temporality: cumulative` mode, amending
  [ADR `aggregation-window-semantics`](aggregation-window-semantics.md); its bounded-retention
  machinery (renamed from the gauge-specific `gauge_retention`/`max_retained_gauge_series` to a
  general `series_retention`/`max_retained_series`) now also bounds cumulative-mode accumulator
  state.
- Known-gaps rows: `min`/`max` dropped on encoded histograms; native (sparse exponential)
  histograms not supported (`ExponentialHistogram` is skipped, not down-converted); no TLS on
  `prometheus_out`'s server side in v1 (tracked alongside `admin:`'s own TLS gap); UTF-8 quoted
  names deferred; `EventBatch::scope`/`Resource.schema_url`/`dropped_attributes_count` dropped on
  encode.
- [ADR `internal-telemetry-as-pipeline-events`](internal-telemetry-as-pipeline-events.md)'s
  rejected "pull-based `/metrics` scrape endpoint" alternative is not reversed by this ADR — see
  that ADR's amendment. Internal telemetry still flows through the graph as ordinary events;
  `prometheus_out` is an ordinary sink fed by whatever the graph routes to it, including
  `internal`, not a second telemetry representation bypassing the pipeline.
