# Telemetry landscape survey

A reference, not a plan: what each wire protocol `logit` implements or has stated an intent to
implement ([`docs/OVERVIEW.md`](../OVERVIEW.md)'s "Ingest"/"Emit" scope) can actually express, per
signal, so [ADR `lossless-transit`](../adr/lossless-transit.md)'s "the internal model is a superset
of every supported protocol" has concrete edges to check against instead of being argued from
memory each time a new codec needs a decision. [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)
is where today's model and codecs get assessed against the matrices here and a target model gets
proposed — this document only describes the protocols themselves.

Every field list below was checked against the protocol's own specification or reference
implementation at the URL given; where a detail couldn't be verified from a primary source it says
so explicitly rather than presenting a guess as fact.

## Metrics

### statsd (Etsy's original protocol)

Reference: <https://github.com/statsd/statsd/blob/master/docs/metric_types.md>.

`<name>:<value>|<type>[|@<sample-rate>]`, one or more per newline-delimited datagram.

- **Types:** `c` (counter, `@rate` optional), `g` (gauge — absolute; a leading `+`/`-` is a
  *relative* adjustment, not a negative absolute value), `ms` (timer — the server derives
  count/mean/sum/upper/lower and configured percentiles), `s` (set — cardinality of distinct
  string values seen), `h` (histogram, several server implementations treat this as an alias of
  `ms`).
- No tags, no unit, no explicit timestamp, no per-point metadata. Identity is the metric name
  alone; multiple metrics in one datagram are newline-separated, each fully self-contained.

### DogStatsD

Reference: <https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/>.

Superset of statsd's grammar: `<name>:<value>[:<value>...]|<type>|@<rate>|#<tag>:<v>,<tag>|c:<id>|T<ts>`.

- **Types:** adds `d` (distribution — server-side sketch, distinct from `h`, which DataDog treats
  as a plain histogram) to statsd's `c`/`g`/`ms`/`s`/`h`.
- **Multi-value:** `name:v1:v2:...:vN|type` — one type/rate/tags shared across N values.
- **Tags:** `|#k1:v1,k2:v2,bare_tag` — comma-separated, colon-separated key:value or a bare
  valueless tag.
- **Sample rate `@rate`:** valid on `c`, `ms`, `h`, `d` — explicitly *not* applied to `g` or `s`.
- **Container ID `|c:<id>`** (v1.2+): appended after tags; v1.4+ adds prefixed variants
  `c:ci-<container-id>` / `c:in-<cgroup-inode>` for the same slot.
- **Timestamp `|T<unix-seconds>`** (v1.3+): only valid on `c` and `g` — explicitly not aggregated
  by the receiving agent when present (each point is submitted as its own instant).
- **Events:** `_e{<title-utf8-len>,<text-utf8-len>}:<title>|<text>|d:<ts>|h:<host>|p:<priority>|t:<alert_type>|#<tags>`
  (also `k:<aggregation_key>` and `s:<source_type_name>`, order among the optional pipe-segments
  not fixed by the format beyond `_e{...}:` leading and title/text following the first `|`).
- **Service checks:** `_sc|<name>|<status 0-3: OK/WARNING/CRITICAL/UNKNOWN>|d:<ts>|h:<host>|#<tags>|m:<message>` —
  the message segment must be last if present, since its value can itself contain `|`.

### OTLP metrics

Reference: <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto>.

One `Metric{name, description, unit, metadata: []KeyValue}` wraps exactly one of five point-set
oneofs:

- **`Gauge{data_points: []NumberDataPoint}`** — instantaneous value, no temporality.
- **`Sum{data_points: []NumberDataPoint, aggregation_temporality, is_monotonic}`** — temporality is
  `AGGREGATION_TEMPORALITY_DELTA` (change since last report) or `_CUMULATIVE` (change since a fixed
  start); `is_monotonic` says whether it can only increase.
- **`Histogram{data_points: []HistogramDataPoint, aggregation_temporality}`** — `explicit_bounds`
  (strictly increasing) with one more `bucket_counts` entry than bounds (the implicit final
  `(last_bound, +Inf]` bucket); `count`, optional `sum`/`min`/`max`.
- **`ExponentialHistogram{data_points: []ExponentialHistogramDataPoint, aggregation_temporality}`** —
  `scale` (resolution; base = 2^(2^-scale)), `zero_count`, `zero_threshold`, separate `positive`/
  `negative` bucket sets each as `(offset, []bucket_counts)` (sparse, geometrically spaced), plus
  `count`, optional `sum`/`min`/`max`.
- **`Summary{data_points: []SummaryDataPoint}`** — pre-computed `quantile_values: []{quantile,
  value}`, plus `count` and `sum` (no temporality concept — a summary is inherently a point-in-time
  computation over the exporter's own window).

Every data point (all five types) carries `attributes`, `start_time_unix_nano`,
`time_unix_nano`, `flags` (bit 0 = `DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK`, "this point is
explicitly absent, not zero"), and `exemplars: []Exemplar{value: double|int, time_unix_nano,
filtered_attributes, trace_id?, span_id?}`. `NumberDataPoint.value` is `oneof{as_double, as_int}` —
OTLP has both integer and float wire representations. Batches nest as
`ResourceMetrics{resource{attributes, dropped_attributes_count}, schema_url,
scope_metrics: []ScopeMetrics{scope{name, version, attributes, dropped_attributes_count},
schema_url, metrics}}`.

### Prometheus exposition format / OpenMetrics

References: <https://prometheus.io/docs/instrumenting/exposition_formats/>,
<https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md>.

Text format: `# HELP <name> <description>`, `# TYPE <name> <type>`, then
`<name>{label="value",...} <float value> [<timestamp ms>]` lines per sample. The timestamp unit
differs by dialect: text 0.0.4's is an integer count of milliseconds since the epoch, while
OpenMetrics's `Timestamp` (and its `_created` series) is Unix epoch time in **float seconds** —
the two are not interchangeable digit-for-digit, a detail a codec crossing between them has to
convert rather than reinterpret.

- **Types:** `counter` (with a companion `_total` suffix and, in OpenMetrics, an optional
  `_created` timestamp series), `gauge`, `histogram` (`_bucket{le="<bound>"}` cumulative counts
  including `le="+Inf"`, plus `_sum`/`_count`/`_created`), `summary` (pre-computed
  `{quantile="q"}` lines plus `_sum`/`_count`), and, OpenMetrics-only, `unknown`, `info`
  (a single always-1 series carrying identifying labels), `stateset` (a set of boolean-valued
  states), `gaugehistogram` (a histogram of a quantity that can decrease, e.g. a size
  distribution sampled from a gauge).
- **Exemplars** (OpenMetrics): `# {trace_id="...",...} <value> <timestamp>` trailing a bucket or
  counter line.
- **Native histograms** (Prometheus-specific extension to the protobuf exposition format, not
  plaintext): sparse exponential bucketing structurally identical in spirit to OTLP's
  `ExponentialHistogram` — `schema` (resolution), `zero_threshold`, `zero_count`, sparse
  positive/negative spans+deltas, plus a float-count variant for pre-aggregated inputs.
- Labels are always string-valued; a metric name plus its label set is the series identity, exactly
  OTLP's `(name, attributes)` shape.

### Prometheus remote-write

References: <https://prometheus.io/docs/specs/remote_write_spec/> (1.0),
<https://prometheus.io/docs/specs/remote_write_spec_2_0/> (2.0).

1.0: `WriteRequest{timeseries: []TimeSeries{labels, samples: []{value, timestamp_ms}, exemplars,
histograms}, metadata}`. 2.0 replaces this with `io.prometheus.write.v2.Request`, whose headline
changes are a deduplicated **symbol table** (every label/metadata string in the request is
interned once and referenced by index — a wire-efficiency change, not a semantic one), a
`Metadata{type, help, unit}` message attached per series rather than out-of-band, first-class
**native histogram** samples in the wire format itself (the `schema`/`zero_count`/
`zero_threshold`/positive-negative-spans shape above), a `created_timestamp` per series, and
per-series `Exemplar` support. Semantically remote-write is a transport for exactly what the
exposition/OpenMetrics format already describes — it doesn't add new metric semantics beyond
native histograms.

### InfluxDB line protocol

Reference: <https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/>.
(`logit`'s `influxdb_out` is egress-only — no `influxdb_in` exists — so this is a target for
egress fidelity, not a like-to-like pair under [ADR `lossless-transit`](../adr/lossless-transit.md).)

`<measurement>[,<tag_key>=<tag_value>...] <field_key>=<field_value>[,<field_key>=<field_value>...] [<timestamp>]`.

- No metric *kind* at all — a field is just a typed value (`1.0` float, `1i` signed 64-bit,
  `1u` unsigned 64-bit, `"text"` string, `true`/`t`/`false`/`f` boolean); "this is a counter" vs.
  "this is a gauge" is purely a convention the writer and reader agree on out of band.
  Multiple fields may share one point (one timestamp, one tag set).
- Tag values are always strings; comma, equals sign, and space are escaped with a backslash in
  measurement names, tag keys/values, and field keys; double quote and backslash are escaped in
  string field values. Timestamp precision is configurable per write, nanoseconds by default.

### collectd binary/network protocol

Reference: <https://github.com/collectd/collectd/wiki/Binary-protocol>. (Named in
[`docs/OVERVIEW.md`](../OVERVIEW.md)'s ingest scope; no `collectd_in` exists yet.)

A stream of TLV "parts." Identity parts precede value parts: `Host` (string), `Plugin`/
`PluginInstance` (string, e.g. `"cpu"`/`"1"`), `Type`/`TypeInstance` (string, e.g. `"cpu"`/
`"idle"`), `Interval` (numeric, collection period), `Time` (unix seconds) or, v5.0+, a
higher-resolution time encoded in 2⁻³⁰-second units instead of a float, avoiding floating-point
time arithmetic.

- **Value types**, each a fixed-width wire value: `COUNTER` (u64, network/big-endian, semantics:
  wraps on overflow — a monotonic counter with no OTLP-style explicit temporality flag, delta is
  computed downstream by differencing), `GAUGE` (f64, **little-endian**, the one value type not in
  network byte order), `DERIVE` (i64, network byte order — a signed monotonic-or-not counter,
  collectd's answer to "a counter that can also decrease or reset without wrapping"), `ABSOLUTE`
  (u64, network byte order — a counter reset to the reported value on every read, e.g. a queue
  depth sampled destructively).
- Optional signing/encryption parts wrap the payload; not a value-semantics concern.

### Graphite

References: <https://graphite.readthedocs.io/en/latest/feeding-carbon.html> (plaintext),
<https://graphite.readthedocs.io/en/latest/tags.html> (tags).

Plaintext: `<path> <value> <timestamp>`, one per line, `path` a dot-separated hierarchy
(`servers.web01.cpu.idle`). No kind, no explicit metadata — retention and aggregation function
(sum/average/max/last) are configured server-side per path pattern, not carried on the wire.
**Tagged** extension: `<path>;tag1=value1;tag2=value2 <value> <timestamp>` — tag names forbid
`;`, `!`, `^`, `=`; tag values forbid `;` and a leading `~`; Carbon normalizes tag order on
ingest. A pickle-serialized batch protocol exists as a transport optimization, same semantics.

### Metrics comparison matrix

| Feature | statsd | DogStatsD | OTLP | Prometheus (exposition/OM) | Prom. remote-write | InfluxDB LP | collectd | Graphite |
|---|---|---|---|---|---|---|---|---|
| Counter/monotonic sum | `c` | `c` | `Sum{monotonic:true}` | `counter` | via `Sum` type | untyped field | `COUNTER`, `DERIVE` | untyped |
| Gauge | `g` (absolute) | `g` | `Gauge` | `gauge` | via type | untyped field | `GAUGE` | untyped |
| Relative gauge delta | `+`/`-` on `g` | `+`/`-` on `g` | — | — | — | — | — | — |
| Temporality (delta/cumulative) | — (implicit delta) | — | explicit field | cumulative only (`_bucket`) | via native histogram | — | — | — |
| Timer/raw samples | `ms` (server-summarized) | `ms` | — | — | — | — | — | — |
| Distribution (sketch) | — | `d` | `Summary` (fixed quantiles) or native histogram | native histogram | native histogram | — | — | — |
| Set/cardinality | `s` | `s` | — | — | — | — | — | — |
| Histogram (explicit buckets) | `h` (~alias of `ms`) | `h` | `Histogram` | `histogram` | via native | — | — | — |
| Histogram sum/count/min/max | — | — | yes | `_sum`/`_count` (no min/max) | yes | — | — | — |
| Exponential/native histogram | — | — | `ExponentialHistogram` | native histogram ext. | yes (2.0) | — | — | — |
| Summary (pre-computed quantiles) | — | — | `Summary` | `summary` | — | — | — | — |
| Exemplars | — | — | yes | OpenMetrics only | yes (2.0) | — | — | — |
| Unit | — | — | `Metric.unit` | `# UNIT` (OM) | via metadata (2.0) | — | — | — |
| Description | — | — | `Metric.description` | `# HELP` | via metadata (2.0) | — | — | — |
| Start time | — | — | `start_time_unix_nano` | — (`_created`, OM) | `created_timestamp` (2.0) | — | — | — |
| Point timestamp | — | `\|T` (c/g only) | `time_unix_nano` (ns) | ms | ms | configurable, ns default | s or 2⁻³⁰s | s |
| Sample rate | `@rate` | `@rate` (not g/s) | — | — | — | — | — | — |
| Tags/labels | none | string k:v, bare | typed `AnyValue` attrs | string labels | string labels (interned, 2.0) | string tag values | identity parts only | `k=v`, string |
| Resource/scope identity | — | — | `Resource`+`Scope` | job/instance labels (convention) | job/instance labels | tags (convention) | host/plugin parts | path prefix (convention) |
| schema_url | — | — | yes | — | — | — | — | — |
| Events/service checks | — | `_e{}` / `_sc` | (as logs, not metrics) | — | — | — | — | — |
| Container id | — | `\|c:` | resource attrs | — | — | — | — | — |
| Multi-value point | `a:1:2:3\|c` | yes | one point per `Metric` (batch-level regroup) | one line per series | one series per point | multiple fields/point | one value/part | one value/line |
| No-recorded-value / stale marker | — | — | `flags` bit 0 | staleness marker (internal) | — | — | — | — |
| Int vs. float value | float only | float only | `oneof{int,double}` | float only (text) | float | typed (`i`/`u`/float) | typed per value-type | float only |

## Logs

### RFC 3164 (BSD syslog)

Reference: <https://www.rfc-editor.org/rfc/rfc3164>.

`<PRI>TIMESTAMP HOSTNAME TAG[PID]: MSG` — `PRI` is `<facility*8+severity>` (facility 0-23,
severity 0-7); `TIMESTAMP` is `Mmm dd hh:mm:ss` with **no year and no timezone**; `HOSTNAME` and
`TAG`/`PID` are conventional, not formally delimited (many real senders, `nginx` among them, omit
HOSTNAME entirely); `MSG` is free text, historically ASCII but the RFC places no hard encoding
requirement on it.

### RFC 5424

Reference: <https://www.rfc-editor.org/rfc/rfc5424>.

`<PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG`.

- `TIMESTAMP`: RFC 3339 with mandatory uppercase `T`/`Z`, up to 6 fractional-second digits, no
  leap seconds, or the NILVALUE `-`.
- `HOSTNAME`/`APP-NAME`/`PROCID`/`MSGID`: `PRINTUSASCII` (33-126) up to a fixed length cap each,
  or NILVALUE `-` when unknown/undisclosed.
- `STRUCTURED-DATA`: NILVALUE `-`, or one or more `SD-ELEMENT`s: `[SD-ID PARAM-NAME="PARAM-VALUE" ...]`.
  `SD-ID`/`PARAM-NAME` are 1-32 `PRINTUSASCII` characters excluding `=`, space, `]`, `"`; an
  `SD-ID` may be `name@<private enterprise number>` for a vendor-specific element, or one of a
  small set of IANA-registered names (`timeQuality`, `origin`, `meta`) with no PEN. `PARAM-VALUE`
  is any UTF-8 string with `"`, `\`, `]` backslash-escaped; a PARAM-NAME may repeat within one
  element (multi-valued parameters are legal). `MSG` may carry a leading UTF-8 BOM to signal
  `MSG-UTF8`; without one, MSG-ANY permits arbitrary octets.
- Transports: RFC 5425 (TLS), RFC 5426 (plain UDP, one message per datagram), RFC 6587 (TCP,
  either octet-counting framing or non-transparent `\n`-delimited framing).

### OTLP logs

Reference: <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto>.

`LogRecord{time_unix_nano, observed_time_unix_nano, severity_number, severity_text, body: AnyValue,
attributes, dropped_attributes_count, flags, trace_id, span_id, event_name}`.

- `severity_number`: 24 levels in six named bands of four (`TRACE`=1-4, `DEBUG`=5-8, `INFO`=9-12,
  `WARN`=13-16, `ERROR`=17-20, `FATAL`=21-24; `UNSPECIFIED`=0) — a producer can express fine
  gradations within a band (`INFO2`=10) that a coarser 6-or-8-level scheme collapses.
  `severity_text` is the source's own free-text label, independent of the numeric band.
- `time_unix_nano == 0` means "unknown," distinct from the actual Unix epoch, per the field's own
  spec comment; `observed_time_unix_nano` is when the collection pipeline itself saw the record,
  which for an externally-sourced event (not originated by an OTel SDK) differs from `time_unix_nano`.
- `body` is a full `AnyValue` — string, bytes, or a nested map/array — not text-only.
- `flags`: low 8 bits are W3C trace flags (mirroring the log's own `trace_id`/`span_id`
  correlation), upper 24 bits reserved.
- `event_name`: a short, low-cardinality category identifier distinct from the free-text body —
  "this record is an instance of event X," not the message itself.
- Nests in `ResourceLogs{resource, schema_url, scope_logs: []ScopeLogs{scope, schema_url, log_records}}`,
  identical shape to metrics/traces.

### Files / Docker json-file (`tail_in`/`docker_in`'s wire shape)

No spec — a convention. A line of text (or, for Docker's json-file driver, one JSON object per
line: `{"log": "...", "stream": "stdout"|"stderr", "time": "<RFC3339Nano>"}`), an originating file
path, and for Docker, which of the container's two streams it came from.

### Logs comparison matrix

| Feature | RFC 3164 | RFC 5424 | OTLP | Docker json-file |
|---|---|---|---|---|
| Severity granularity | 8 levels (in PRI) | 8 levels (in PRI) | 24 levels (6 named bands) | — |
| Facility | 24 values (in PRI) | 24 values (in PRI) | — | — |
| Timestamp precision | second, no year/tz | up to µs, full RFC 3339 | ns | ns (RFC3339Nano) |
| Observed vs. event time | — (one timestamp) | — (one timestamp) | both, distinct fields | — |
| Hostname/app/proc/msgid identity | hostname, tag, pid (informal) | all four, formal, capped, nilable | — (would ride as attributes) | — |
| Structured data | — | `[SD-ID PARAM="v"]`, repeatable | attributes (typed) | — |
| Body type | free text | free text or arbitrary octets | text, bytes, or structured `AnyValue` | text or embedded JSON |
| Trace correlation | — | — | `trace_id`/`span_id`/`flags` | — |
| Event name (category, not message) | — | — | `event_name` | — |
| Dropped-attribute accounting | — | — | `dropped_attributes_count` | — |
| Framing/injection constraint | none (`\n` implicit line end) | octet-counting or `\n` framing (TCP) | length-prefixed (protobuf/gRPC framing) | `\n`-delimited JSON |
| Max length | none specified (implementations vary) | none specified (implementations vary) | none | none |

## Traces

### OTLP traces

Reference: <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto>.

`Span{trace_id, span_id, trace_state, parent_span_id, flags, name, kind, start_time_unix_nano,
end_time_unix_nano, attributes, dropped_attributes_count, events: []Event{time_unix_nano, name,
attributes, dropped_attributes_count}, dropped_events_count, links: []Link{trace_id, span_id,
trace_state, attributes, dropped_attributes_count, flags}, dropped_links_count,
status: Status{message, code: UNSET|OK|ERROR}}`.

- `kind`: `INTERNAL`, `SERVER`, `CLIENT`, `PRODUCER`, `CONSUMER` (`UNSPECIFIED` recommended to
  decode as `INTERNAL`).
- `trace_state`: the raw W3C `tracestate` header value — vendor-specific key=value pairs riding
  alongside a trace, opaque to `logit`.
- `flags` (on both `Span` and `Span.Link`): low 8 bits are W3C trace flags (bit 0 = sampled);
  bits 8-9 record whether the span's parent context is known to be remote and, if so, whether it
  actually was; bits 10-31 reserved. A `Span.Link`'s own `flags` describes the *linked* span's
  context the same way.
- Every attribute-bearing sub-message (`Span`, `Event`, `Link`) carries its own independent
  `dropped_attributes_count`; `Span` additionally tracks `dropped_events_count`/`dropped_links_count`.
- Nests identically to metrics/logs: `ResourceSpans{resource, schema_url, scope_spans: []ScopeSpans{scope, schema_url, spans}}`.

### W3C Trace Context, Zipkin, Jaeger (reference only — not implemented as `logit` codecs)

References: <https://www.w3.org/TR/trace-context/>, <https://zipkin.io/zipkin-api/>,
<https://www.jaegertracing.io/docs/1.6/apis/>. `logit` already parses the W3C `traceparent`
header format as an attribute convention (`docs/design/data-model.md`'s well-known attribute
table) rather than as a separate wire codec. Zipkin's span model (`traceId`, `id`, `parentId`,
`kind`, `name`, timestamps, `localEndpoint`/`remoteEndpoint`, `annotations`, `tags`) and Jaeger's
(`traceID`, `spanID`, `operationName`, `references`, `tags`, `logs`) are both strict subsets of
what OTLP's `Span` above already expresses — named here only to support the claim that OTLP is the
superset among trace formats `logit` might ever need to bridge, not because either is a planned
codec.

### Traces comparison matrix

| Feature | OTLP | W3C Trace Context (header only) | Zipkin | Jaeger |
|---|---|---|---|---|
| Trace/span id | 16/8 bytes | 16/8 bytes (hex in header) | 16 or 8 bytes, hex | 16 bytes, hex |
| Parent reference | `parent_span_id` | (implicit: the incoming header) | `parentId` | `references` (CHILD_OF/FOLLOWS_FROM) |
| trace_state (vendor extension) | yes | yes (`tracestate` header) | — | — |
| Flags (sampled, remote-parent) | yes, both on span and on links | sampled bit only | — (`debug` annotation convention) | — |
| Kind | 5 values | — | 4 values (client/server/producer/consumer) | tag convention |
| Events (timestamped annotations) | yes, with own attrs + dropped count | — | `annotations` (timestamp+value only) | `logs` (timestamp + fields) |
| Status + message | code + message | — | — (tag convention) | tag convention |
| Dropped-attribute/event/link counts | yes, per sub-message | — | — | — |

## Superset requirements

The properties [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)'s target model is
checked against, derived from the matrices above:

1. A metric point carries temporality (delta/cumulative) and monotonicity, and an optional series
   start time.
2. A distribution can be either raw, unsummarized samples (with the sample rate that produced
   them) or a mergeable sketch — decode produces the former, only an explicit summarizing stage
   produces the latter.
3. A set can be either exact members or a mergeable cardinality estimate, on the same terms as (2).
4. A histogram carries `sum`/`count`/`min`/`max` alongside its buckets, in both the explicit-bound
   and the exponential/native forms — as two distinct representations, not one lossily converted
   to the other, so an exponential histogram round-trips as itself.
5. A summary carries `count` and `sum` alongside its quantiles.
6. A metric point can carry exemplars (value, timestamp, trace correlation, filtered attributes).
7. A metric records a description and a unit, not only a name.
8. A numeric value distinguishes "this was an integer" from "this was a float," and "this was
   unsigned" from "this was signed," at least as far as any protocol in scope round-trips that
   distinction itself (OTLP does not, beyond int/double).
9. A log record carries its raw severity number (up to 24 levels) and raw severity text alongside
   the normalized `Severity`, an observed-time distinct from event-time, an event name, a body that
   can be text or arbitrary bytes or structured data, and a dropped-attribute count.
10. A span (and its links) carries `trace_state`, `flags`, a status message, and independent
    dropped-attribute/event/link counts.
11. A batch carries scope identity (name, version, attributes, schema_url) alongside resource
    identity, both with their own dropped-attribute counts and schema_url.
12. Syslog structured data round-trips as structured data, not as discarded bytes.
13. Syslog's 8-level severity, facility, and the sender's own origin timestamp all survive a relay,
    independent of the normalized `Severity`/receipt-time fields.
14. DogStatsD's container id, `|T` timestamp, events, and service checks are representable, not
    silently ignored.
