# Telemetry landscape survey

What each wire protocol `logit` implements or intends to implement
([`docs/OVERVIEW.md`](../OVERVIEW.md)'s "Ingest"/"Emit" scope) can express, per signal. It gives
[ADR `lossless-transit`](../adr/lossless-transit.md)'s rule, "the internal model is a superset of
every supported protocol", concrete edges to check a new codec against. This document describes
only the protocols. [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md) assesses
`logit`'s model and codecs against these matrices.

Every field list was checked against the protocol's specification or reference implementation at
the URL given. A detail that couldn't be verified from a primary source says so.

## Metrics

### statsd (Etsy's original protocol)

Reference: <https://github.com/statsd/statsd/blob/master/docs/metric_types.md>.

`<name>:<value>|<type>[|@<sample-rate>]`, one or more per newline-delimited datagram.

- **Types:** `c` (counter, `@rate` optional), `g` (gauge — absolute; a leading `+`/`-` is a
  *relative* adjustment, not a negative absolute value), `ms` (timer — the server derives
  count/mean/sum/upper/lower and configured percentiles), `s` (set — cardinality of distinct
  string values seen), `h` (histogram; several servers treat it as an alias of `ms`).
- No tags, unit, explicit timestamp, or per-point metadata. The metric name alone is the identity.
  Each newline-separated metric in a datagram is self-contained.

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
- **Timestamp `|T<unix-seconds>`** (v1.3+): valid only on `c` and `g`. The receiving agent doesn't
  aggregate a timestamped point; it submits each as its own instant.
- **External data `|e:<data>`** (v1.5+, Agent 7.57+): origin-detection data a client reads from its
  environment, itself a comma-separated list (`it-<bool>,cn-<container-name>,pu-<pod-uid>`).
- **Cardinality `|card:<none|low|orchestrator|high>`** (v1.6+, Agent 7.64+): the tag cardinality
  the agent should enrich this point with.
- **Events:** `_e{<title-utf8-len>,<text-utf8-len>}:<title>|<text>|d:<ts>|h:<host>|p:<priority>|t:<alert_type>|#<tags>`,
  plus `k:<aggregation_key>` and `s:<source_type_name>`. The format fixes only that `_e{...}:`
  leads and title/text follow the first `|`; the optional pipe-segments can come in any order.
- **Service checks:** `_sc|<name>|<status 0-3: OK/WARNING/CRITICAL/UNKNOWN>|d:<ts>|h:<host>|#<tags>|m:<message>`.
  The message segment, if present, must be last, because its value can contain `|`.

### Datadog intake (series and sketches)

References: <https://docs.datadoghq.com/api/latest/metrics/>, the Datadog Agent's
`pkg/serializer` and `agent-payload`'s `metrics/agent_payload.proto`.

What an Agent sends Datadog after it has aggregated DogStatsD and its checks, and what
`datadog_in` and `datadog_out` speak. A series (`/api/v2/series`, protobuf from an Agent or JSON;
`/api/v1/series`, JSON) is a name, a `type` (`count`, `rate`, `gauge`, or unspecified), whole-second
points, `interval`, `unit`, tags, and typed `resources`. Raw distribution values go to
`/api/v1/distribution_points`; the Agent's own sketches go to `/api/beta/sketches` as DDSketch
bins with a count, sum, min, and max, and no mapping parameters: a receiver has to assume the
Agent's. `logit`'s mapping is `crates/logit-proto/src/datadog/mod.rs`'s "Metrics" section.

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
  value}`, plus `count` and `sum`. No temporality: a summary is a point-in-time computation over
  the exporter's own window.

Every data point of all five types carries `attributes`, `start_time_unix_nano`,
`time_unix_nano`, `flags` (bit 0 = `DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK`, "this point is
explicitly absent, not zero"), and `exemplars: []Exemplar{value: double|int, time_unix_nano,
filtered_attributes, trace_id?, span_id?}`. `NumberDataPoint.value` is `oneof{as_double, as_int}`,
so OTLP has both integer and float wire representations. Batches nest as
`ResourceMetrics{resource{attributes, dropped_attributes_count}, schema_url,
scope_metrics: []ScopeMetrics{scope{name, version, attributes, dropped_attributes_count},
schema_url, metrics}}`.

### Prometheus exposition format / OpenMetrics

References: <https://prometheus.io/docs/instrumenting/exposition_formats/>,
<https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md>.

Text format: `# HELP <name> <description>`, `# TYPE <name> <type>`, then
`<name>{label="value",...} <float value> [<timestamp ms>]` lines per sample. The timestamp unit
differs by dialect: text 0.0.4 uses integer milliseconds since the epoch, while OpenMetrics's
`Timestamp` (and its `_created` series) uses **float seconds**. A codec crossing between them must
convert the value, not reinterpret its digits.

- **Types:** `counter` (with a companion `_total` suffix and, in OpenMetrics, an optional
  `_created` timestamp series), `gauge`, `histogram` (`_bucket{le="<bound>"}` cumulative counts
  including `le="+Inf"`, plus `_sum`/`_count`/`_created`), `summary` (pre-computed
  `{quantile="q"}` lines plus `_sum`/`_count`), and, OpenMetrics-only, `unknown`, `info`
  (a single always-1 series carrying identifying labels), `stateset` (a set of boolean-valued
  states), `gaugehistogram` (a histogram of a quantity that can decrease, e.g. a size
  distribution sampled from a gauge).
- **Exemplars** (OpenMetrics): `# {trace_id="...",...} <value> <timestamp>` trailing a bucket or
  counter line.
- **Native histograms** (a Prometheus extension to the protobuf exposition format, absent from
  plaintext): sparse exponential bucketing, the same idea as OTLP's `ExponentialHistogram` —
  `schema` (resolution), `zero_threshold`, `zero_count`, sparse positive/negative spans+deltas,
  plus a float-count variant for pre-aggregated inputs.
- Labels are always strings. A metric name plus its label set is the series identity, the same
  shape as OTLP's `(name, attributes)`.

### Prometheus remote-write

References: <https://prometheus.io/docs/specs/remote_write_spec/> (1.0),
<https://prometheus.io/docs/specs/remote_write_spec_2_0/> (2.0).

`logit` supports both versions over the same `MetricFamily` seam the exposition codec uses:
`prometheus_in`'s `bind:` receives either on one listener, and `prometheus_out`'s `endpoint:` sends
the one set by an explicit `version:`. **`logit` doesn't map native histograms**, the one part of
the format it skips; they're counted in both directions and deferred to a follow-up
(`docs/known-gaps.md`). [ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md) has the
model mapping, the timestamp-group decomposition, and the permitted normalizations.

1.0: `WriteRequest{timeseries: []TimeSeries{labels, samples: []{value, timestamp_ms}, exemplars,
histograms}, metadata}`. 2.0 replaces it with `io.prometheus.write.v2.Request`, which adds:

- A deduplicated **symbol table**: each label or metadata string in the request is interned once
  and referenced by index. This changes wire efficiency, not semantics.
- A `Metadata{type, help, unit}` message per series, instead of out-of-band.
- First-class **native histogram** samples (the `schema`/`zero_count`/
  `zero_threshold`/positive-negative-spans shape above).
- A `created_timestamp` per series, and per-series `Exemplar` support.

Apart from native histograms, remote-write adds no metric semantics; it transports what the
exposition/OpenMetrics format already describes.

### InfluxDB line protocol

Reference: <https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/>.
`logit` has `influxdb_out` but no `influxdb_in`, so this is a target for egress fidelity, not a
like-protocol pair under [ADR `lossless-transit`](../adr/lossless-transit.md).

`<measurement>[,<tag_key>=<tag_value>...] <field_key>=<field_value>[,<field_key>=<field_value>...] [<timestamp>]`.

- No metric *kind*. A field is a typed value (`1.0` float, `1i` signed 64-bit, `1u` unsigned
  64-bit, `"text"` string, `true`/`t`/`false`/`f` boolean); whether it's a counter or a gauge is a
  convention the writer and reader agree on out of band. Multiple fields can share one point (one
  timestamp, one tag set).
- Tag values are always strings. A backslash escapes comma, equals sign, and space in measurement
  names, tag keys and values, and field keys, and escapes double quote and backslash in string
  field values. Timestamp precision is set per write; the default is nanoseconds.

### collectd binary/network protocol

Reference: <https://github.com/collectd/collectd/wiki/Binary-protocol>. See
[ADR `collectd-binary-relay`](../adr/collectd-binary-relay.md) for `logit`'s `collectd_in`/
`collectd_out` model mapping, attribute convention, and permitted normalizations.

A stream of TLV "parts": `type u16 BE, len u16 BE`, where `len` includes the 4-byte header. Strings
are NUL-terminated (`len = 4 + n + 1`); numeric parts are a single `u64` BE (`len = 12`). Identity
parts precede value parts and are **sticky within one datagram**: a `Host`/`Plugin`/etc. part
applies to every following `Values` part until replaced. A sender can therefore elide a string part
that hasn't changed since the last one it wrote, and the receiver keeps the last value it saw.

Part types:

- `Host` (0x0000, string).
- `Time` (0x0001, unix seconds) and, v5.0+, `TimeHR` (0x0008): time in 2⁻³⁰-second units
  ("cdtime"), which avoids floating-point time arithmetic.
- `Plugin`/`PluginInstance` (0x0002/0x0003, string, e.g. `"cpu"`/`"1"`) and `Type`/`TypeInstance`
  (0x0004/0x0005, string, e.g. `"cpu"`/`"idle"`).
- `Values` (0x0006); layout below.
- `Interval` (0x0007, seconds) / `IntervalHR` (0x0009, cdtime). The sender always writes
  `TimeHR`/`IntervalHR` per value list, never the legacy pair.
- `Message` (0x0100) and `Severity` (0x0101, one of 1 FAILURE/2 WARNING/4 OKAY) carry a
  notification instead of a metric value, sent in the order TimeHR, Severity, Host, Plugin,
  PluginInstance, Type, TypeInstance, Message. A receiver drops a notification with severity
  outside `{1,2,4}`, time `0`, or an empty message. `NOTIF_MAX_MSG_LEN` is 256.
- `Signature` (0x0200) and `Encryption` (0x0210) optionally wrap the rest of the payload. They don't
  affect value semantics, but a receiver that can't verify or decrypt has nothing further to parse.

Behavior:

- **`Values` layout**: `u16 count`, then `count` one-byte data-source-type tags, then `count`
  8-byte values, so `len == 6 + 9*count` (2-byte count + `count` type bytes + `count*8` value bytes,
  plus the 4-byte part header). The four value types:
  - `COUNTER`: u64, network (big-endian) byte order. Wraps on overflow: a monotonic counter with
    no OTLP-style temporality flag; consumers compute the delta by differencing.
  - `GAUGE`: f64, **little-endian**, the one value type not in network byte order.
  - `DERIVE`: i64, network byte order. A signed counter that can also decrease or reset without
    wrapping.
  - `ABSOLUTE`: u64, network byte order. A counter reset to the reported value on every read, such
    as a queue depth sampled destructively.
- **Sender behavior** (`add_to_buffer`): elides only the five string identity parts, compared with
  what it last wrote to the packet. Packs value lists up to `MaxPacketSize` (default **1452** bytes,
  a typical Ethernet MTU after IP/UDP headers on a slightly tunneled path), and starts a new packet
  when a list wouldn't fit. Default port 25826; default multicast groups `239.192.74.66` (v4) /
  `ff18::efc0:4a42` (v6).
- **Receiver behavior** (`network_dispatch_values`): rejects a value list (`-EINVAL`) when its time
  is `0` or its host, plugin, or type string is empty. `plugin_dispatch_values` also rejects a type
  missing from its configured `types.db`, or a value count that doesn't match that type's declared
  data-source count. `escape_slashes` turns a literal `/` into `_` in every identity string,
  because several collectd write plugins use these fields in file paths. `DATA_MAX_NAME_LEN` is 128
  bytes (127 plus the trailing NUL). If a string part exceeds that bound or lacks its NUL,
  `parse_part_string` fails and the parser abandons the rest of the packet; value lists already
  dispatched from earlier parts stand.

### Graphite

References: <https://graphite.readthedocs.io/en/latest/feeding-carbon.html> (plaintext + pickle),
<https://graphite.readthedocs.io/en/latest/tags.html> (tags). `logit`'s relay:
[ADR `graphite-carbon-relay`](../adr/graphite-carbon-relay.md).

Plaintext: `<path> <value> <timestamp>`, one per line, where `path` is a dot-separated hierarchy
(`servers.web01.cpu.idle`). No kind and no explicit metadata: retention and aggregation function
(sum/average/max/last) are configured server-side per path pattern, not carried on the wire.
**Tagged** extension: `<path>;tag1=value1;tag2=value2 <value> <timestamp>`. Tag names forbid
`;`, `!`, `^`, `=`; tag values forbid `;` and a leading `~`. Carbon normalizes tag order on
ingest.

Pickle is a batch transport with the same semantics, on its own port (2004; plaintext uses 2003).
Each message is a 4-byte **big-endian** length prefix (Twisted's `Int32StringReceiver` framing)
followed by exactly that many bytes of a pickled `[(path, (timestamp, value)), ...]`: a flat list
of `(str, (number, number))` tuples, one per datapoint, in no particular grouping.

- Real senders (`carbon-relay`'s own pickle client, collectd's `write_graphite` plugin in
  `Protocol Pickle` mode) emit protocol 2 or `-1`, which resolves to the sender's highest available
  protocol. Nothing in the field emits protocol 0 or 1 for this payload shape.
- Carbon's receiver treats `timestamp <= 0` specially: `-1` means "now", so the point gets receipt
  time instead of being rejected. Otherwise `pickle.dumps`'s float/int formatting passes straight
  through.

Both wire forms share the tag grammar above and one value rule: Carbon silently drops a `nan`
datapoint and never writes it to Whisper.

### Splunk HEC metrics

References: <https://help.splunk.com> (HTTP Event Collector, "Get metrics in from other sources"),
and the OpenTelemetry Collector contrib `exporter/splunkhecexporter` and
`pkg/translator/splunk`; checked against Splunk Enterprise 10.4.3 and Splunk Cloud Platform
10.5.2605.9 by `script/splunk-interop`.

A HEC metric is a JSON object with `"event":"metric"` (or no `event`) and its measurements in
`fields`: any number of `metric_name:<name>` numbers (the multi-metric form), or one
`metric_name`/`_value` pair. Every other key in `fields` is a dimension, and the envelope's
`host`, `source`, and `sourcetype` are added as dimensions. A value is one integer or double; a
name is `[A-Za-z0-9_.:]`, with no leading digit or `_`. There is no type, temporality, unit, or
histogram: the OTel exporter writes `metric_type` (`Gauge`, `Sum`, `Histogram`, `Summary`) as an
ordinary dimension, and histograms and summaries as Prometheus-style `_bucket` with `le`, `_sum`,
`_count`, and `<name>_<q>` with `qt`, which `mstats` and `histperc` read. Splunk 10.4.3 and Splunk
Cloud 10.5.2605.9 indexed 1,000 dimensions on one object. `logit`'s mapping is
`crates/logit-proto/src/splunk/metrics.rs`'s module doc.

### Metrics comparison matrix

| Feature | statsd | DogStatsD | Datadog intake | OTLP | Prometheus (exposition/OM) | Prom. remote-write | InfluxDB LP | collectd | Graphite | Splunk HEC |
|---|---|---|---|---|---|---|---|---|---| --- |
| Counter/monotonic sum | `c` | `c` | `count` (a delta over `interval`) | `Sum{monotonic:true}` | `counter` | via `Sum` type | untyped field | `COUNTER`, `DERIVE` | untyped → `Gauge` (temporality/monotonicity dropped, `docs/adr/graphite-carbon-relay.md`) | `metric_type` `Sum` dimension (convention; no temporality or monotonicity) |
| Gauge | `g` (absolute) | `g` | `gauge` | `Gauge` | `gauge` | via type | untyped field | `GAUGE` | untyped → `Gauge` | `metric_name:<n>`, `metric_type` `Gauge` (convention) |
| Relative gauge delta | `+`/`-` on `g` | `+`/`-` on `g` | — | — | — | — | — | — | — | — |
| Temporality (delta/cumulative) | — (implicit delta) | — | — (implicit delta; a `rate` is per second) | explicit field | cumulative only (`_bucket`) | cumulative only, same as exposition (a native histogram's `reset_hint` is the one exception) | — | — | — | — (values are samples; `mstats rate()` derives rates) |
| Timer/raw samples | `ms` (server-summarized) | `ms` | distribution points (raw values) | — | — | — | — | — | — | — |
| Distribution (sketch) | — | `d` | sketches: DDSketch bins, the mapping assumed, not on the wire | `Summary` (fixed quantiles) or native histogram | native histogram | native histogram (in `logit`: a `summary` of 5 fixed quantiles, as on exposition -- native histograms are skipped) | — | — | none natively; `multi_value: expand` → `.count`/`.sum`/`.q0_5`…`.q0_99` sub-paths, else dropped | none natively; `multi_value: expand` → `_count`/`_sum`/`_p50`/`_p90`/`_p99`, else dropped |
| Set/cardinality | `s` | `s` | — (an Agent sends `s` as a gauge) | — | — | — | — | — | none natively; `expand` → `.count`, else dropped | none natively; `expand` → the count, else dropped |
| Histogram (explicit buckets) | `h` (~alias of `ms`) | `h` | — | `Histogram` | `histogram` | `_bucket{le}`/`_sum`/`_count` flat series, exactly as on exposition (supported, 1.0 and 2.0) | — | — | none natively; `expand` → `.count`/`.sum`/`.min`/`.max`/`.bucket_<b>`, else dropped | `_bucket{le}`/`_sum`/`_count` series (the exporter's convention, read by `histperc`) |
| Histogram sum/count/min/max | — | — | a sketch's `cnt`/`sum`/`min`/`max` | yes | `_sum`/`_count` (no min/max) | yes | — | — | `expand` only (see row above) | `_sum`/`_count` (no min/max) |
| Exponential/native histogram | — | — | — | `ExponentialHistogram` | native histogram ext. | yes (2.0) -- **skipped by `logit` in both directions**, counted, deferred to a follow-up (`docs/known-gaps.md`) | — | — | none natively; `expand` → `.count`/`.sum`/`.min`/`.max`/`.zero_count`, no buckets, else dropped | — (the exporter drops it) |
| Summary (pre-computed quantiles) | — | — | — | `Summary` | `summary` | — | — | — | none natively; `expand` → `.count`/`.sum`/`.q<q>`, else dropped | `_sum`/`_count`/`<n>_<q>{qt}` series (convention) |
| Exemplars | — | — | — | yes | OpenMetrics only | yes, both versions (`TimeSeries.exemplars`) | — | — | — | — |
| Unit | — | — | series `unit` | `Metric.unit` | `# UNIT` (OM) | via metadata, both versions (1.0 `MetricMetadata.unit`, 2.0 inline `Metadata`) | — | — | — | — |
| Description | — | — | — | `Metric.description` | `# HELP` | via metadata, both versions (1.0 `MetricMetadata.help`, 2.0 inline `Metadata`) | — | — | — | — |
| Start time | — | — | — | `start_time_unix_nano` | — (`_created`, OM) | `Sample.start_timestamp` (2.0 only; `TimeSeries` field 6 is `reserved`, and 1.0 has no field at all) | — | — | — | — |
| Point timestamp | — | `\|T` (c/g only) | s, always | `time_unix_nano` (ns) | ms | ms | configurable, ns default | s or 2⁻³⁰s | s | `time`, epoch s with a decimal fraction |
| Collection interval | — | — | series `interval` | — | — | — | — | `Interval`/`IntervalHR` per value list | — | — |
| Sample rate | `@rate` | `@rate` (not g/s) | — | — | — | — | — | — | — | — |
| Tags/labels | none | string k:v, bare | string k:v, bare | typed `AnyValue` attrs | string labels | string labels (interned, 2.0) | string tag values | identity parts only | `k=v`, string | `fields` dimensions, flat, string or number |
| Resource/scope identity | — | — | `resources` (`host`, `device`, others) | `Resource`+`Scope` | job/instance labels (convention) | job/instance labels | tags (convention) | host/plugin parts | path prefix (convention) | `host`/`source`/`sourcetype`/`index` envelope |
| schema_url | — | — | — | yes | — | — | — | — | — | — |
| Events/service checks | — | `_e{}` / `_sc` (in `logit`: `log` (`_e`) / `Gauge` + `statsd.service_check.*` carriers (`_sc`), both ways -- `docs/adr/statsd-output.md`'s amendment) | `/intake/` or `/api/v1/events` / `/api/v1/check_run` (in `logit`: DogStatsD's carriers) | (as logs, not metrics) | — | — | — | notifications (`Message`+`Severity` parts; in `logit`: `log` + `collectd.severity`) | — | (as log events) |
| Container id | — | `\|c:` | — | resource attrs | — | — | — | — | — | — |
| Multi-value point | `a:1:2:3\|c` | yes | several points per series | one point per `Metric` (batch-level regroup) | one line per series | one series per point | multiple fields/point | one value/part | one value/line (expandable — `multi_value: expand` renders several dotted sub-paths) | many `metric_name:<n>` per object (multi-metric form), one value each |
| No-recorded-value / stale marker | — | — | — | `flags` bit 0 | staleness marker (internal) | — | — | — | — | — |
| Int vs. float value | float only | float only | float only | `oneof{int,double}` | float only (text) | float | typed (`i`/`u`/float) | typed per value-type | float only | integer or double (a JSON number) |

## Logs

### RFC 3164 (BSD syslog)

Reference: <https://www.rfc-editor.org/rfc/rfc3164>.

`<PRI>TIMESTAMP HOSTNAME TAG[PID]: MSG`.

- `PRI` is `<facility*8+severity>` (facility 0-23, severity 0-7).
- `TIMESTAMP` is `Mmm dd hh:mm:ss`, with **no year and no timezone**.
- `HOSTNAME` and `TAG`/`PID` are conventional, not formally delimited. Many real senders, `nginx`
  among them, omit HOSTNAME entirely.
- `MSG` is free text, historically ASCII; the RFC sets no hard encoding requirement.

### RFC 5424

Reference: <https://www.rfc-editor.org/rfc/rfc5424>.

`<PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG`.

- `TIMESTAMP`: RFC 3339 with mandatory uppercase `T`/`Z`, up to 6 fractional-second digits, no
  leap seconds, or the NILVALUE `-`.
- `HOSTNAME`/`APP-NAME`/`PROCID`/`MSGID`: `PRINTUSASCII` (33-126) up to a fixed length cap each,
  or NILVALUE `-` when unknown/undisclosed.
- `STRUCTURED-DATA`: NILVALUE `-`, or one or more `SD-ELEMENT`s: `[SD-ID PARAM-NAME="PARAM-VALUE" ...]`.
  `SD-ID`/`PARAM-NAME` are 1-32 `PRINTUSASCII` characters excluding `=`, space, `]`, `"`; an
  `SD-ID` is either `name@<private enterprise number>` for a vendor-specific element or one of a
  few IANA-registered names (`timeQuality`, `origin`, `meta`) with no PEN. `PARAM-VALUE` is any
  UTF-8 string with `"`, `\`, `]` backslash-escaped. A PARAM-NAME can repeat within one element,
  so multi-valued parameters are legal.
- `MSG`: a leading UTF-8 BOM signals `MSG-UTF8`; without one, MSG-ANY permits arbitrary octets.
- Transports: RFC 5425 (TLS), RFC 5426 (plain UDP, one message per datagram), RFC 6587 (TCP,
  either octet-counting framing or non-transparent `\n`-delimited framing).

### OTLP logs

Reference: <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto>.

`LogRecord{time_unix_nano, observed_time_unix_nano, severity_number, severity_text, body: AnyValue,
attributes, dropped_attributes_count, flags, trace_id, span_id, event_name}`.

- `severity_number`: 24 levels in six named bands of four (`TRACE`=1-4, `DEBUG`=5-8, `INFO`=9-12,
  `WARN`=13-16, `ERROR`=17-20, `FATAL`=21-24; `UNSPECIFIED`=0). A producer can express gradations
  within a band (`INFO2`=10) that a coarser 6- or 8-level scheme collapses. `severity_text` is the
  source's own free-text label, independent of the numeric band.
- `time_unix_nano == 0` means "unknown", not the Unix epoch, per the field's spec comment.
  `observed_time_unix_nano` is when the collection pipeline saw the record; for an event that
  didn't originate in an OTel SDK, it differs from `time_unix_nano`.
- `body` is a full `AnyValue` — string, bytes, or a nested map/array — not only text.
- `flags`: the low 8 bits are W3C trace flags, mirroring the log's own `trace_id`/`span_id`
  correlation; the upper 24 bits are reserved.
- `event_name`: a short, low-cardinality category ("this record is an instance of event X"),
  distinct from the free-text body.
- Nests in `ResourceLogs{resource, schema_url, scope_logs: []ScopeLogs{scope, schema_url, log_records}}`,
  the same shape as metrics and traces.

### Files / Docker json-file (`tail_in`/`docker_in`'s wire shape)

No spec, only a convention: a line of text and the file path it came from. Docker's json-file
driver writes one JSON object per line,
`{"log": "...", "stream": "stdout"|"stderr", "time": "<RFC3339Nano>"}`, which adds which of the container's two streams the line came from.

### Datadog logs intake

Reference: <https://docs.datadoghq.com/api/latest/logs/>.

`/api/v2/logs` takes a JSON array of objects, from an Agent or any HTTP client. `message`,
`status`, `timestamp`, `hostname`, `service`, `ddsource`, and `ddtags` are reserved; every other
key is an attribute, nested objects included. There's no numeric severity, only `status` as free
text. The intake links a log to a trace from attributes it detects by name: Datadog's decimal
`dd.trace_id`/`dd.span_id`, or OTel's hex `trace_id`/`span_id`. `logit`'s mapping is
`crates/logit-proto/src/datadog/mod.rs`'s "Decode: logs" and "Encode: events → logs, events,
service checks" sections.

### Splunk HEC logs

References: <https://help.splunk.com> (HTTP Event Collector, "Format events for HTTP Event
Collector"), and the OpenTelemetry Collector contrib `exporter/splunkhecexporter`; checked
against Splunk Enterprise 10.4.3 and Splunk Cloud Platform 10.5.2605.9 by
`script/splunk-interop`.

`/services/collector/event` takes JSON objects, concatenated or in an array, each with its own
envelope: `time` (epoch seconds, decimals allowed), `host`, `source`, `sourcetype`, `index`,
`event` (a string or any JSON value), and `fields` (a flat object of indexed fields; Splunk rejects
a nested value). `/services/collector/raw` takes bytes, with the envelope in the query string and
line breaking and timestamps from the sourcetype's `props.conf`. HEC has no severity, event name,
or trace fields of its own; the OTel exporter writes `otel.log.severity.text`,
`otel.log.severity.number`, `otel.log.name`, `trace_id`, and `span_id` into `fields`, which is the
de facto schema. `logit`'s mapping is `crates/logit-proto/src/splunk/logs.rs`'s module doc.

### Logs comparison matrix

| Feature | RFC 3164 | RFC 5424 | OTLP | Docker json-file | Datadog | Splunk HEC |
|---|---|---|---|---|---| --- |
| Severity granularity | 8 levels (in PRI) | 8 levels (in PRI) | 24 levels (6 named bands) | — | `status`, free text (in `logit`: mapped onto 6 levels, raw kept) | — (the exporter's `otel.log.severity.text`/`.number` in `fields`) |
| Facility | 24 values (in PRI) | 24 values (in PRI) | — | — | — | — |
| Timestamp precision | second, no year/tz | up to µs, full RFC 3339 | ns | ns (RFC3339Nano) | ms, or an RFC 3339 string | `time`, epoch s with a decimal fraction; Splunk stores its own precision |
| Observed vs. event time | — (one timestamp) | — (one timestamp) | both, distinct fields | — | — (one `timestamp`) | — (one `time`) |
| Hostname/app/proc/msgid identity | hostname, tag, pid (informal) | all four, formal, capped, nilable | — (would ride as attributes) | — | `hostname`, `service`, `ddsource` | `host`, `source`, `sourcetype`, `index` |
| Structured data | — | `[SD-ID PARAM="v"]`, repeatable | attributes (typed) | — | any other key, nested JSON | `fields` (flat, indexed); an `event` object |
| Body type | free text | free text or arbitrary octets | text, bytes, or structured `AnyValue` | text or embedded JSON | `message`, text | `event`: a string or any JSON value |
| Trace correlation | — | — | `trace_id`/`span_id`/`flags` | — | attributes: `dd.trace_id`/`dd.span_id` (decimal) or `trace_id`/`span_id` (hex), detected by the intake | `trace_id`/`span_id` in `fields` (the exporter's convention) |
| Event name (category, not message) | — | — | `event_name` | — | — | `otel.log.name` in `fields` (the exporter's convention) |
| Dropped-attribute accounting | — | — | `dropped_attributes_count` | — | — | — |
| Framing/injection constraint | none (`\n` implicit line end) | octet-counting or `\n` framing (TCP) | length-prefixed (protobuf/gRPC framing) | `\n`-delimited JSON | a JSON array per HTTP request | concatenated JSON objects or an array per HTTP request; `/raw`: lines, broken by the sourcetype |
| Max length | none specified (implementations vary) | none specified (implementations vary) | none | none | 1 MB per log (truncated, still accepted); 1,000 logs and 5 MB per request | `max_content_length` per request: 1,000,000 B on old releases, 838,860,800 B on 10.4.3; Splunk Cloud 10.5.2605.9 caps between 5,242,881 and 6,000,000 B and answers code 6, not `413` |

## Traces

### OTLP traces

Reference: <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto>.

`Span{trace_id, span_id, trace_state, parent_span_id, flags, name, kind, start_time_unix_nano,
end_time_unix_nano, attributes, dropped_attributes_count, events: []Event{time_unix_nano, name,
attributes, dropped_attributes_count}, dropped_events_count, links: []Link{trace_id, span_id,
trace_state, attributes, dropped_attributes_count, flags}, dropped_links_count,
status: Status{message, code: UNSET|OK|ERROR}}`.

- `kind`: `INTERNAL`, `SERVER`, `CLIENT`, `PRODUCER`, `CONSUMER`. The spec recommends decoding
  `UNSPECIFIED` as `INTERNAL`.
- `trace_state`: the raw W3C `tracestate` header value, vendor-specific key=value pairs that ride
  alongside a trace and are opaque to `logit`.
- `flags` (on both `Span` and `Span.Link`): the low 8 bits are W3C trace flags (bit 0 = sampled).
  Bits 8-9 record whether the parent context is known to be remote and, if so, whether it was.
  Bits 10-31 are reserved. A `Span.Link`'s `flags` describes the *linked* span's context the same
  way.
- Each attribute-bearing sub-message (`Span`, `Event`, `Link`) carries its own
  `dropped_attributes_count`; `Span` also tracks `dropped_events_count`/`dropped_links_count`.
- Nests the same way as metrics and logs: `ResourceSpans{resource, schema_url, scope_spans: []ScopeSpans{scope, schema_url, spans}}`.

### Datadog Agent APM protocol

References: the Datadog Agent's `pkg/trace/api` (the tracer API), `pkg/trace/writer` (the intake
client), and `agent-payload`'s `trace/*.proto`.

Two hops, each its own wire. A tracer sends a local Agent msgpack on `:8126`: `/v0.4/traces` (an
array of traces, each an array of span maps), `/v0.5/traces` (a string table and 12-element span
arrays), or `/v0.7/traces` (one `TracerPayload`), plus its own client stats on `/v0.6/stats`. The
Agent processes the spans and sends the intake a protobuf `AgentPayload` on `/api/v0.2/traces`
and a msgpack `StatsPayload` on `/api/v0.2/stats`. Datadog derives no trace metrics from spans:
the stats are the only source of a service's hits, errors, and latency.

A span has `service`, `name`, `resource`, and `type` strings; uint64 `trace_id`, `span_id`, and
`parent_id`; `start` and `duration` in ns; an `error` flag; `meta` (string to string), `metrics`
(string to double), and `meta_struct` (string to bytes); and span links and events. A 128-bit
trace id carries its high 64 bits as 16 hex characters in `meta["_dd.p.tid"]` on the first span
of a chunk. A stats bucket groups hits, errors, and durations by service, name, resource, type,
kind, and status code, with ok and error latency as DDSketch protobufs that carry their own
mapping. `logit`'s mapping is `crates/logit-proto/src/datadog/mod.rs`'s "Traces" and "APM stats"
sections.

### Splunk HEC spans (the OTel exporter's shape)

Reference: the OpenTelemetry Collector contrib `exporter/splunkhecexporter`'s `hecSpan`, checked
against a recording of Collector contrib 0.161.0 (`testdata/interop/splunk/`) and indexed by
Splunk Enterprise 10.4.3 and Splunk Cloud Platform 10.5.2605.9 (`script/splunk-interop`).

Splunk Enterprise and Splunk Cloud Platform have no trace store; the OTel exporter sends each span
as an ordinary HEC event whose `event` is a JSON object: `trace_id`, `span_id`, and
`parent_span_id` (`""` on a root) as hex strings, `name`, `attributes`, `start_time` and
`end_time` in Unix nanoseconds, `kind` and `status.code` as the protobuf enum names
(`SPAN_KIND_SERVER`, `STATUS_CODE_ERROR`), `status.message`, `events[]`, and `links[]`. The
envelope `time` is the start in seconds, and `fields` carry the span's resource attributes. The
Collector's own `splunk_hec` receiver keeps such an object as a log; `logit`'s mapping, which
decodes it back to a span, is `crates/logit-proto/src/splunk/spans.rs`'s module doc.

### W3C Trace Context, Zipkin, Jaeger (reference only — not implemented as `logit` codecs)

References: <https://www.w3.org/TR/trace-context/>, <https://zipkin.io/zipkin-api/>,
<https://www.jaegertracing.io/docs/1.6/apis/>.

`logit` parses the W3C `traceparent` header format as an attribute convention
(`docs/design/data-model.md`'s well-known attribute table), not as a wire codec. Zipkin's span
model (`traceId`, `id`, `parentId`, `kind`, `name`, timestamps, `localEndpoint`/`remoteEndpoint`,
`annotations`, `tags`) and Jaeger's (`traceID`, `spanID`, `operationName`, `references`, `tags`,
`logs`) are both strict subsets of OTLP's `Span`. Neither is a planned codec; they're listed to
support the claim that OTLP is the superset among trace formats `logit` might bridge.

### Traces comparison matrix

| Feature | OTLP | Datadog | W3C Trace Context (header only) | Zipkin | Jaeger | Splunk HEC (exporter span events) |
|---|---|---|---|---|---| --- |
| Trace/span id | 16/8 bytes | uint64 each; a 128-bit trace id's high half as `_dd.p.tid` (hex) in `meta` | 16/8 bytes (hex in header) | 16 or 8 bytes, hex | 16 bytes, hex | hex strings in the span object |
| Parent reference | `parent_span_id` | `parent_id` (0 = root) | (implicit: the incoming header) | `parentId` | `references` (CHILD_OF/FOLLOWS_FROM) | `parent_span_id` (`""` on a root) |
| trace_state (vendor extension) | yes | on span links only | yes (`tracestate` header) | — | — | on links only |
| Flags (sampled, remote-parent) | yes, both on span and on links | on span links only; a trace chunk's sampling `priority` instead | sampled bit only | — (`debug` annotation convention) | — | — |
| Kind | 5 values | `meta["span.kind"]` convention | — | 4 values (client/server/producer/consumer) | tag convention | `SPAN_KIND_*` names |
| Events (timestamped annotations) | yes, with own attrs + dropped count | `span_events`, typed attrs, no dropped count | — | `annotations` (timestamp+value only) | `logs` (timestamp + fields) | `events[]` (`name`, `timestamp`, `attributes`), no dropped count |
| Status + message | code + message | `error` (int) + `meta` tag convention | — | — (tag convention) | tag convention | `status{message, code}`, `STATUS_CODE_*` names |
| Dropped-attribute/event/link counts | yes, per sub-message | — | — | — | — | — |

## Superset requirements

These properties, derived from the matrices above, are what
[`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)'s target model is checked against.
That plan's "Closing assessment" records how the landed model (2026-09-12) meets them and names
the residual debt, which `docs/known-gaps.md` tracks.

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
14. DogStatsD's container id, `|T` timestamp, `|e:` external data, `|card:` cardinality, events,
    and service checks are representable, not silently ignored.
