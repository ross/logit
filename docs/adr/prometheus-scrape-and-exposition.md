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
yet. Prometheus is not alone in that — the same survey names collectd and Graphite with nothing
built against them either — but it is the first of the surveyed-but-unbuilt protocols to get a
full `_in`/`_out` pair.

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

**The `+Inf` bucket is a successive difference like every other bucket, not a separate total.**
Decoding `histogram`/`gaugehistogram` computes each finite bucket's count as the difference
between its cumulative value and the previous (lower) bound's, exactly as the table above says;
the `+Inf` bucket (always present and always cumulative-equal-to-`_count` per the exposition
grammar) follows the identical rule: `n = _count − cumulative(last finite bound)`. When `_count`
disagrees with the `+Inf` line's own cumulative value (a malformed or hand-written exposition),
the `+Inf` line's value wins — it's the one actually walked bucket-by-bucket — and the mismatch is
counted `logit.input.metrics.degraded{reason="histogram_count_mismatch"}` rather than silently
picked one way.

**Encode (`Event`s → families)** is the inverse, plus rules for model kinds Prometheus's wire
can't carry natively:

| Model | Wire |
|---|---|
| `Sum{Cumulative, monotonic}` | `counter`; family name (used for `# TYPE`/`# HELP`/`# UNIT`) = name with any trailing `_total` stripped, in **both** dialects; sample name = `<family>_total`, appended if the model name lacked it, in **both** dialects; `_created` from `start_timestamp` when non-zero (OM only) |
| `Sum{Cumulative, !monotonic}` | `gauge` (Prometheus has no non-monotonic counter) — counted `logit.output.metrics.degraded{metric_kind="non_monotonic_sum"}` |
| `Sum{Delta}` / `Histogram{Delta}` | **skipped**, `logit.output.metrics.skipped{metric_kind="delta_sum"\|"delta_histogram"}` + `warn_throttled("delta_temporality_unresolved")` naming `aggregate`'s `temporality: cumulative` |
| `Gauge` | `gauge`; `prometheus.type` attr (consumed) overrides the family type to `untyped`/`unknown`/`info`/`stateset` — see "Cross-dialect family types" below for what each becomes when the output dialect lacks that wire type |
| `GaugeDelta` | skipped, `warn_throttled("gauge_delta_unresolved")` (shared key across sinks) |
| `Histogram{Cumulative}` | `histogram`: running-sum buckets, `+Inf` = total, `_sum` only when `Some`, `_count` = bucket total; `min`/`max` dropped (known-gaps row); `prometheus.type: gaugehistogram` → `_gsum`/`_gcount` on OM egress, `_sum`/`_count` on text egress (text has no `gaugehistogram`) |
| `Summary` | `summary` + `_created` (OM) |
| `Distribution(sketch)` | `summary` with `DISTRIBUTION_QUANTILES` (reused from `otlp/metrics.rs`, made `pub(crate)`) and `_count`, **no `_sum`** (a sketch has none; OM permits omission) — `degraded{metric_kind="distribution"}` |
| `Samples` | `s.sketch()` then as above — `degraded{metric_kind="samples"}` |
| `Set` / `SetMembers` | `gauge` of `estimate()` / distinct count — `degraded{metric_kind="set"\|"set_members"}` |
| `ExponentialHistogram` | skipped, `skipped{metric_kind="exponential_histogram"}` (text has no native-histogram syntax) |
| `flags & NO_RECORDED_VALUE` | skipped, `skipped{reason="no_recorded_value"}` |
| exemplars | OM only, on `_total`/`_bucket` lines (bucket chosen by value); text mode drops them (dialect choice, not counted) |
| `Event::timestamp` | emitted only when `prometheus.timestamp: true` is present (consumed); ms in text, float s in OM |
| labels | `logit_core::attrs::merged(resource, event)` (event wins), skipping `prometheus.target`; `Value::Str/I64/U64/F64/Bool` stringified; `Null/Bytes/Timestamp/Array/Map` dropped — `logit.output.labels.dropped{reason="unrepresentable"}` |
| names | see "Names and sanitization" below |
| `unit` / `description` | `# UNIT` (OM only) / `# HELP` |
| `EventBatch::scope`, `Resource.schema_url`, `dropped_attributes_count` | dropped (known-gaps rows) |

**Cross-dialect family types.** `prometheus.type` and `gaugehistogram` cover model kinds that have
no wire type of their own, but the two output dialects don't have the same *set* of wire types as
each other, so encoding to the dialect that lacks one has to pick something else rather than
emitting a spec-invalid family:

- **OM egress:** `untyped` (text 0.0.4's catch-all, which OpenMetrics dropped) becomes `unknown`
  (OpenMetrics's own catch-all). A `prometheus.type: "info"` attribute re-appends the `_info`
  suffix OM's `info` type requires on the sample name (decode strips it — see the decode table
  above — so this is the exact inverse).
- **Text 0.0.4 egress:** `unknown` (OM's catch-all, which text lacks) becomes `untyped`. `info`
  becomes `gauge` with the sample named `<name>_info` (text has no `info` type, but the `_info`
  suffix convention still reads sensibly as an ordinary gauge name). `stateset` becomes `gauge`
  (one series per state, unchanged from the decode shape). `gaugehistogram` becomes `histogram`,
  with `_gsum`/`_gcount` renamed to `_sum`/`_count` (text has no `gaugehistogram`, and a histogram
  whose count can decrease is still a histogram on the wire).

Each of these is a dialect-choice normalization (the operator picked the output dialect), listed
again under "Permitted normalizations for this pair" below.

**Where the resource⊕event label merge lives.** The "labels" row's merge has to be callable from
`logit_proto::prometheus::events_to_families`, which cannot depend on `logit-outputs` (the
dependency runs the other way). `logit_core::attrs::merged` is the merge function itself, made
`pub` there instead of `pub(crate)` in `crates/logit-outputs/src/attrs.rs`; `logit-outputs`
re-exports it under its existing path for the sinks that already call it
(`influxdb_out`/`statsd_out`), so neither of them changes its own import. `events_to_families`
takes `(&Resource, &Event)` pairs and merges internally, rather than requiring its caller to
pre-merge — the same shape `families_to_events`'s per-series `Event` construction already implies.

### Attribute conventions

`prometheus_in` stamps two facts about the target it scraped onto that scrape's `Resource`:
**unprefixed `instance`** (`Value::Str`, `host:port` — exactly what Prometheus's own scrape adds
to every series it collects) and `prometheus.target` (`Value::Str`, the full scrape URL, namespaced
because it has no equivalent normalized field anywhere else in the model). `instance` is
deliberately **not** namespaced: `prometheus_out` renders resource attributes as ordinary labels,
the same as every other sink renders resource attributes as tags, so `instance` needs no special
casing to reach the exposed series — it's just a resource attribute like any other, present on
every series from that target's batch, including the synthetic `up`. This is what keeps two
targets running the same exporter from colliding: without a per-target label the registry's key
(family name + label set) would be identical for both, and one target's series would silently
overwrite the other's, including their `up` (see "Exposition state and expiry" below for the
registry key). An event-level `instance` attribute (one a transform stamped downstream) wins over
the resource-level one `prometheus_in` set, the same "event wins" merge every other resource/event
attribute conflict already resolves by — `honor_labels` semantics, in Prometheus's own terms.

Two remaining `prometheus.*` well-known attributes (`docs/design/data-model.md`'s table, rule (b)
of [ADR `lossless-transit`](lossless-transit.md): a protocol's own raw encoding of something the
model already normalizes rides as a protocol-namespaced attribute that outranks the normalized
field on that protocol's own egress) are consumed by `prometheus_out`, never rendered as an
ordinary label there:

- `prometheus.type` (`Value::Str`): the wire family type when the model has no distinct kind for
  it (`untyped`, `unknown`, `info`, `stateset`, `gaugehistogram`).
- `prometheus.timestamp` (`Value::Bool(true)`): the sample carried its own timestamp on the wire;
  `prometheus_out` re-emits one on that line only. Same shape as the planned `statsd.timestamp`.

Plus the resource-level `prometheus.target` above, consumed the same way — `prometheus.target` is
factual, like `docker_in`'s `container.*` attributes and `instance` above (none is an invented
`service.name`); `prometheus.type`/`prometheus.timestamp` instead record how to re-encode a
sample, not a fact about it. Every other sink treats all three as plain tags like any other
attribute, since only `prometheus_out` knows to consume them. `job` is
deliberately **not** stamped by `prometheus_in`, unlike `instance`: it is operator identity (which
scrape config this target belongs to), not a fact about the target itself, and Prometheus's own
server derives it from the scrape config, not the target's response — there is no factual value
`prometheus_in` could stamp the way it can for `instance`. An operator gets `job` with a downstream
`set` component (or Lua) stamping it from whatever they know about the topology.

Because of this, `prometheus_in -> prometheus_out` with no transforms is not quite an untouched
byte-for-byte relay on labels — it is a fixed point modulo one named, permitted normalization: the
scrape adds an `instance` label that wasn't on the wire at the target, exactly as Prometheus's own
scrape does (see "Permitted normalizations for this pair" below). Nothing else `prometheus_in`
stamped (`prometheus.target`) leaks onto the exposed series, since it stays consumed; an operator
who wants a `job` label, or federation-style relabeling beyond `instance`, sets it explicitly.

### Names and sanitization

A metric name must match `[a-zA-Z_:][a-zA-Z0-9_:]*`; a label name must match
`[a-zA-Z_][a-zA-Z0-9_]*`. Every other byte is replaced with `_` — substitution, not deletion, so
two distinct names that differ only in a forbidden byte don't collapse onto the same wire name
silently (the same reasoning [ADR `statsd-output`](statsd-output.md)'s "Sanitization" section gives
for statsd's own name/tag rules). A name that begins with a digit gets a leading `_` prefix instead
of substituting the digit itself, since `_1` remains a legal identifier where `1` would not.
Because sanitization is a many-to-one substitution, two source labels can collide after
sanitizing (e.g. `a.b` and `a-b` both becoming `a_b`). The tie-break, and the label emission order
generally, is decided on **rendered** (post-sanitization) names, never on the original attribute
map's own order: `AttrMap`'s order is by interned `Symbol`, which depends on process-global
first-intern order — unrelated traffic that happened to intern a string earlier would silently
change which label wins a collision, and would change the byte-exact label order a round-trip test
depends on, neither of which has anything to do with this codec. Labels on an emitted series are
sorted by their rendered name; on a collision after sanitizing, the label whose *original* name
sorts first (by ordinary string order) keeps the rendered name, and every other colliding label is
dropped and counted `logit.output.labels.dropped{reason="collision"}` — deterministic given the
same input event, independent of interning history, rather than silently overwriting the first
value or emitting two labels with the same name (which the exposition grammar cannot represent).

### Permitted normalizations for this pair

Per [ADR `lossless-transit`](lossless-transit.md), the following count as normalization, not loss,
for `prometheus_in -> prometheus_out`, and are what the round-trip fixed-point test asserts
equality modulo:

- Family/series reordering: families sorted by name, series within a family sorted by label set.
- Label reordering within a series, sorted by **rendered** (post-sanitization) name — see "Names
  and sanitization" above for the collision tie-break this implies.
- Float formatting: shortest round-trip representation (`1.0` renders as `1`).
- `# TYPE x untyped` synthesized for a family that arrived with no `# TYPE` metadata at all.
- `_created`, `# UNIT`, and exemplars dropped when the *output* dialect is text `0.0.4` — none of
  the three has a text `0.0.4` wire representation, and the operator chose that output dialect.
- `# EOF` presence: written when (and only when) the output dialect is OpenMetrics, per that
  dialect's own grammar.
- Blank lines and comments other than `HELP`/`TYPE`/`UNIT` dropped.
- **An `instance` label is added**, `host:port` of the scraped target, exactly as Prometheus's own
  scrape adds one — see "Attribute conventions" above. An event-level `instance` label from a
  downstream transform wins over it (`honor_labels` semantics).
- **A counter sample name gains a `_total` suffix if it lacked one**, in either dialect — see the
  "Model mapping" encode table above; every client library does this normalization on the way out
  regardless of which dialect it's writing.
- **A cross-dialect family-type substitution** per "Cross-dialect family types" above
  (`untyped`↔`unknown`, `info`/`stateset`/`gaugehistogram` down-converted on text egress, `_info`
  re-appended on OM egress) when the output dialect lacks the wire type the model attribute names.
- The three synthetic families `prometheus_in` always adds (`up`, `scrape_duration_seconds`,
  `scrape_samples_scraped` — see "Synthetic scrape metrics" below) are excluded from the
  round-trip fixed-point comparison by name, not asserted equal at all: `scrape_duration_seconds`
  is wall-clock and cannot be byte-exact against a fixture by construction, and the other two are
  scrape-outcome facts with no corresponding input to compare against, not something that round-
  trips.

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
`scrape_duration_seconds` and `scrape_samples_scraped` (both `Gauge`) — a subset of what
Prometheus's own server synthesizes for every target it scrapes (which also adds
`scrape_samples_post_metric_relabeling` and `scrape_series_added`; the two omitted here have no
relabeling stage or persisted prior-scrape series set in `prometheus_in` to compute them from).
These three are always on, not configurable off at
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

**Security posture: no TLS, no auth, in v1.** `prometheus_out` serves the entire registry — every
label on every series it currently holds — to any client that connects to `bind:`, with no
credential check and no transport encryption, the same posture
[ADR `admin-readiness-endpoint`](admin-readiness-endpoint.md) accepted for `/readyz`/`/healthz`.
Unlike that endpoint, though, `bind:` is a *required* field an operator sets themselves, not a
fixed loopback default, and the payload here is the full metric surface rather than a coarse
lifecycle phase — so this ADR states the gap explicitly rather than leaving it implicit: an
example config for this pair binds `127.0.0.1`, not `0.0.0.0`, so a first-time reader gets a safe
default to start from rather than an accidental network-wide exposure, and an operator who needs
`prometheus_out` reachable from outside the host is the one making that choice, not inheriting it
from the example. Real TLS/auth support is tracked in `docs/known-gaps.md` alongside `admin:`'s
own entry, not designed here.

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
- **Also stamping `job` as a plain resource attribute, the way `instance` is stamped**, rather than
  leaving it to a downstream `set`. Rejected: `instance` is a fact `prometheus_in` genuinely knows
  (the target it just connected to); `job` is not a fact about the target at all — it's the name of
  the *scrape config* that named this target, and Prometheus's own server derives it from
  configuration, never from anything a target's response carries. `prometheus_in` has no scrape-
  config identity of its own to draw `job` from beyond "which `prometheus_in` component this is,"
  which is already the component's own graph id, not a value worth duplicating onto every event as
  a label the operator didn't ask to see.
- **Leaving `prometheus.type` and `prometheus.timestamp` as plain, rendered labels too**, matching
  `instance`, rather than a consumed attribute pair. Rejected: unlike `instance`, both describe how
  `prometheus_out` should encode the record, not a fact about the series itself — rendering them
  would leak `logit`'s own decode bookkeeping onto the wire as an extra label no Prometheus-native
  producer would ever emit, breaking the fixed point in the other direction instead of preserving
  it.
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
  histograms not supported (`ExponentialHistogram` is skipped, not down-converted); no TLS or auth
  on `prometheus_out`'s server side in v1 (tracked alongside `admin:`'s own no-TLS/no-auth gap);
  UTF-8 quoted names deferred; `EventBatch::scope`/`Resource.schema_url`/`dropped_attributes_count`
  dropped on encode.
- [ADR `internal-telemetry-as-pipeline-events`](internal-telemetry-as-pipeline-events.md)'s
  rejected "pull-based `/metrics` scrape endpoint" alternative is not reversed by this ADR — see
  that ADR's amendment. Internal telemetry still flows through the graph as ordinary events;
  `prometheus_out` is an ordinary sink fed by whatever the graph routes to it, including
  `internal`, not a second telemetry representation bypassing the pipeline.
