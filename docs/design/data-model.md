# Internal data model

This is the representation every input decodes into and every output encodes from, and the type
Lua scripts operate on ([docs/design/lua-api.md](lua-api.md)). It has to represent logs, metrics,
and traces uniformly, cheaply, and without losing the fields any of the target protocols need.

## Top-level shape

Events travel through the pipeline in **batches**, never individually — per-event channel sends and
per-event allocation would dominate the profile at any interesting throughput.

```rust
pub struct EventBatch {
    pub resource: Arc<Resource>,   // host/service/container id -- shared across the whole batch
    pub scope: Option<Arc<Scope>>, // OTLP instrumentation scope; None for most non-OTLP producers
    pub events: Vec<Event>,
}

pub struct Event {
    pub timestamp: i64,            // unix nanos
    pub attributes: AttrMap,
    pub log: Option<LogRecord>,
    pub metrics: MetricList,       // SmallVec<[MetricRecord; 1]>
    pub span: Option<SpanRecord>,
}
```

**An event is whatever it carries, not a tagged one-of** ([ADR `multi-payload-events`](../adr/multi-payload-events.md)).
An access log line is a log record and, once a transform like `kv_metrics` derives request/byte
counts and latency from its fields, a source of several metrics at once — the same event, not two
related-but-separate ones. `log`/`span` stay `Option` (an event can have at most one of each); an
event with none of the three is legal and representable. A sink emits whatever it finds:
`influxdb_out` writes every metric on an event and ignores its log/span.

`Resource` is `Arc`-shared rather than copied onto every event — a batch typically comes from one
socket/file/OTLP request and shares one origin. It's per-batch, not immutable, though: a transform
or Lua script may substitute it for the batch currently in hand by minting a new `Arc`, the
mechanism an operator uses to declare a resource identity `logit`'s own code won't invent on its
own (`logit_pipeline::Transform::map_resource`,
[ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md)).

**`Event` is 864 bytes**, and that size is paid unconditionally — a statsd counter with three tags
costs exactly as much to move as a fully-populated nginx access log, because `AttrMap`'s inline
capacity and `MetricKind`'s inlined `DDSketch`/`Samples` are reserved whether or not they're used. Since an
event is moved by value on every hop between nodes and deep-cloned once per extra fan-out consumer,
that number is a throughput property. [memory.md](memory.md) breaks it down term by term, measures
what each pipeline stage allocates, and lists what could be reclaimed;
`crates/logit-core/tests/type_sizes.rs` asserts it so it can't drift silently.

## Values

```rust
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Bytes(bytes::Bytes),
    Str(bytes::Bytes),   // UTF-8, validated at construction
    Timestamp(i64),      // unix nanos
    Array(Vec<Value>),
    Map(Box<AttrMap>),   // boxed: an unboxed AttrMap here would make Value infinitely sized
}
```

This is deliberately also the type the Lua API exposes ([docs/design/lua-api.md](lua-api.md)) —
designing it twice would mean keeping two conversions in sync forever.

**`bytes::Bytes` everywhere strings and blobs appear.** A syslog line parsed out of a socket read
buffer should end up as a zero-copy slice of that buffer, not a fresh allocation. `Bytes` is
cheaply `Clone`-able (refcounted) and cheaply sliced, which both the parsing path and the Lua proxy
depend on.

Measured, `syslog_in`, `json`, and `statsd_in` all keep that promise now: decoding a line costs one
allocation (`statsd_in`: two, split across a per-line and a per-batch `Vec<Event>` by its
multi-value grammar) regardless of how many fields, tag values, or set members it yields.
`statsd_in`'s tag values, `|c:<id>`, and a `SetMembers` line's members are all zero-copy slices of
the datagram, the same pointer-arithmetic reconstruction (`slice_of`) `syslog_in`'s own fields use.
See [memory.md](memory.md)'s zero-copy section — pinned by tests, not left to inspection.

## Attributes: interned keys, small-map storage

Attribute keys repeat enormously across telemetry — `host`, `env`, `service.name`, and so on appear
on nearly every event. Two optimizations, both hard to retrofit once scripts and codecs depend on
the shape:

- **Interning.** A process-wide symbol table (`lasso::ThreadedRodeo` or equivalent) maps attribute
  keys to `Symbol(u32)`. `AttrMap` then compares, hashes, and stores `u32`s instead of repeated
  string allocations, and the same table backs the wire format's dictionary encoding
  ([docs/design/wire-protocol.md](wire-protocol.md)).
- **Small-map layout.** Most events carry well under a dozen attributes.
  `AttrMap = SmallVec<[(Symbol, Value); 8]>`, kept sorted by `Symbol`, beats a `HashMap` at this
  size for both lookup and iteration, and gives deterministic ordering for free — which matters for
  the wire format's dictionary encoding and for reproducible tests.

## Well-known attribute names

`syslog_in` (`syslog.facility`/`.severity`/`.timestamp`/`.hostname`/`.tag`/`.pid`/`.msgid`/`.sd`)
and the OTLP codec (`otel.severity_number`/`otel.severity_text`) already stamp dotted, `service.name`-style attribute
names as a convention rather than a typed field, when the data belongs on the event but doesn't
rise to a core-model field of its own. `syslog.pid` may be `Value::Str` as well as `Value::U64`
(RFC 5424's PROCID is free-form PRINTUSASCII, not necessarily numeric), and `syslog.timestamp` may
be `Value::Null` (a nil `-` RFC 5424 TIMESTAMP) as well as `Value::Timestamp`/`Value::Str` — see
[ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md).

**OTLP severity is the OTLP instance of the same precedent `syslog.severity` already set** —
`syslog_in`/`syslog_out` deliberately let the raw, protocol-native value outrank the normalized
field on egress ([ADR `syslog-output`](../adr/syslog-output.md)'s "Header-field precedence",
generalized into a repo-wide rule by [ADR `lossless-transit`](../adr/lossless-transit.md)'s rule
(b)): OTLP's 24 raw severity numbers collapse onto this model's 6-variant `Severity` on decode, so
the raw value rides alongside the normalized one and wins on the way back out.

| Attribute | Value | Meaning |
|---|---|---|
| `otel.severity_number` | `Value::I64`, `1..=24` | Stamped by `otlp_in` when the wire's `severity_number` is non-zero. `otlp_out` prefers this over the band-derived value when present, consuming (removing) it from the emitted attribute set the same way `otel.status_message` used to. |
| `otel.severity_text` | `Value::Str` | Stamped by `otlp_in` when the wire's `severity_text` is non-empty. Same precedence and consumption rule as `otel.severity_number`. |

**The Prometheus codec is the same precedent again** (`crates/logit-proto/src/prometheus/`,
[ADR `prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md)): the
exposition format distinguishes things this model has one kind for — an `untyped` sample from a
`gauge`, a sample that carried its own timestamp from one that didn't — so the protocol-native fact
rides alongside as an attribute and wins on the way back out. Every `prometheus.*` attribute is
**consumed** by `prometheus_out` (never rendered as a label) and appears as an ordinary tag at every
other sink, which is what makes `prometheus_in -> prometheus_out` an exact fixed point.

| Attribute | Value | Meaning |
|---|---|---|
| `prometheus.type` | `Value::Str`: `untyped`\|`unknown`\|`info`\|`stateset`\|`gaugehistogram` | The wire family type for the five cases the model has no distinct kind for: `untyped`/`unknown` (both a `Gauge`, one spelling per dialect), `info` (a `Gauge(1)` whose labels are the payload), `stateset` (one `Gauge(0\|1)` per state), `gaugehistogram` (a `Histogram` of a quantity that can decrease). Stamped by `prometheus_in`; read and consumed by `prometheus_out`, which re-emits that exact family type. |
| `prometheus.timestamp` | `Value::Bool(true)` | The sample carried its own timestamp on the wire (most don't — a scrape stamps them all with its own start time). `prometheus_out` re-emits a timestamp on that line only, in the output dialect's own unit. The same "the wire carried its own timestamp" convention `statsd.timestamp` below already uses for DogStatsD's `|T` segment, in the shape a boolean marker needs: a Prometheus sample's timestamp is the event's own, so there is nothing to carry but the fact that it was sent. |
| `prometheus.target` | `Value::Str`, a **resource** attribute | The full scrape URL of the target this batch came from (`http://node-exporter:9100/metrics`), stamped once per target by `prometheus_in`. Factual, like `docker_in`'s `container.*` — not an invented `service.name`. Consumed by `prometheus_out`. |
| `instance` | `Value::Str`, a **resource** attribute | `host:port` of the scraped target — deliberately **unprefixed**, so it renders as a label like any other resource attribute, exactly the `instance` label Prometheus's own scrape adds. Without it two targets running the same exporter would collapse onto one series through a relay. An event-level `instance` wins over the resource's (`honor_labels` semantics), which falls out of the ordinary resource/event attribute merge. `job` is operator identity, not a scrape fact, and comes from a downstream `set`. |

`trace_context`'s `span:` block
([ADR `trace-context-span-lifting`](../adr/trace-context-span-lifting.md)) is the first place the
same *reserved-attribute* convention is deliberately *read* by more than one producer, so it's worth
naming as a real table too rather than leaving it to be reverse-engineered from that transform's
source:

| Attribute | Value | Meaning |
|---|---|---|
| `traceparent` | `Value::Str`, `00-<32 hex>-<16 hex>-<2 hex>` | The W3C Trace Context header (<https://www.w3.org/TR/trace-context/>), logged verbatim by a tier that received or forwarded one. Yields a trace id, this line's *parent* span id, and flags (hex, by that header's own definition) — an explicit field below always wins over the header's corresponding piece. |
| `trace.id` | `Value::Str`, 32 hex, non-zero | |
| `trace.flags` | `Value::I64`/`U64`/`Str`, decimal 0-255 | Decimal only, never hex — see `trace_context`'s own doc comment for why a `traceparent`'s hex octet and this field are never the same number by accident. |
| `span.id` | `Value::Str`, 16 hex, non-zero | This line's own span, not the caller's. |
| `span.parent_id` | `Value::Str`, 16 hex | |
| `span.name` | `Value::Str` | |
| `span.kind` | `Value::Str`: `server`\|`client`\|`producer`\|`consumer`\|`internal` | |
| `span.status` | `Value::Str`: `ok`\|`error`\|`unset` | |
| `span.start`, `span.end` | integer unix nanoseconds (`I64`/`U64`, or an all-digit `Str`), or `Value::Timestamp` | Mirrors OTLP's `start_time_unix_nano`/`end_time_unix_nano` exactly. A float here is invalid, never rounded: an `f64` can't represent an epoch-nanosecond instant (2^53 ≈ 9e15 &lt; 1.7e18). |
| `span.duration` | integer nanoseconds (`I64`/`U64`, or an all-digit `Str`) | OTLP has no duration field; nanoseconds is the only unit consistent with the two above. Float → invalid, same rule. |
| `span.{start,end}_{us,ms}` | integer in that unit | For a source whose clock is coarser than nanoseconds (haproxy's `request_date(us)`) — the suffix is an honest label of the source's resolution, not a convenience. |
| `span.{start,end}_s` | decimal seconds: `I64`/`U64`/`F64`, or a decimal `Str` | The nginx case (`$msec`). A `Str` is parsed digit-exact (`logit_core::parse_decimal_nanos`); an `F64` is only as exact as an epoch-magnitude float can be (~1μs), which already exceeds a `_s` source's own resolution. |
| `span.duration_{us,ms}` | integer | haproxy's `%Ta` → `span.duration_ms`. |
| `span.duration_s` | decimal seconds (number or `Str`) | nginx's `$request_time`. |
| `span.{start,end}_rfc3339` | RFC 3339 string | Parsed by `logit_core::parse_rfc3339_to_nanos`, up to 9 fractional digits. |
| `syslog.sd` | `Value::Map { "<SD-ID>" -> Value::Map { "<PARAM-NAME>" -> Value::Str \| Value::Array<Value::Str> } }` | `syslog_in`'s parsed RFC 5424 STRUCTURED-DATA (absent when the wire carried the nil `-`); a repeated PARAM-NAME within one SD-ELEMENT becomes the `Array` form, in order. `syslog_out` re-emits every element, escaped per RFC 5424 §6.3.3; see [ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md). |
| `statsd.type` | `Value::Str`: `ms`\|`h`\|`d` | `statsd_in`'s wire-type letter for a timer/histogram/distribution line, stamped on the `MetricKind::Samples` record it decodes to since all three land on the same shape; `statsd_out` reads it to pick the wire-type letter it re-emits, defaulting to `ms` when absent or unrecognized. See [ADR `statsd-output`](../adr/statsd-output.md)'s amendment. |
| `statsd.container_id` | `Value::Str` | `statsd_in`'s `\|c:<container-id>` segment (DogStatsD v1.2+, accepted here on every metric type, not only `c`/`g`), round-tripped by `statsd_out` under `format: dogstatsd` only. |
| `statsd.timestamp` | `Value::U64` | The raw seconds off an incoming `\|T<unix-seconds>` segment, stamped by `statsd_in` alongside moving the same value onto `Event::timestamp` (as `secs * 1_000_000_000`) -- carrying the wire value itself, not just a marker bit, so a stage that rebuilds `Event::timestamp` after decode (`aggregate`'s flush, notably) can't fabricate or collapse a `\|T` on the way back out; `statsd_out` re-emits `\|T<secs>` from this attribute's own `U64` value (never from `Event::timestamp`) under `format: dogstatsd` only, and not at all when the attribute is absent or not a `U64`. Also stamped, with the identical value/round-trip contract, from a DogStatsD event's or service check's own `d:<unix-seconds>` field -- `statsd_out` re-emits it as `d:<secs>` (never `\|T`) on those two line shapes. |
| `statsd.event.title` | `Value::Str` | `statsd_in`'s decode of a DogStatsD event (`_e{TITLE_LEN,TEXT_LEN}:title\|text\|...`) -- always present. `statsd_out` renders it as the line's `title`, sanitized (control bytes only; a bare `\|` is fine, since the byte-length prefix delimits the field). See [ADR `statsd-output`](../adr/statsd-output.md)'s "DogStatsD events and service checks" amendment. |
| `statsd.event.priority` | `Value::Str`: `normal`\|`low` | `statsd_in`'s `p:` field, only when sent. `statsd_out` writes it verbatim when it's exactly `normal`/`low`; any other value is omitted and counted (`EncodeStats::dropped_invalid_event_fields`), not synthesized or substituted. |
| `statsd.event.alert_type` | `Value::Str`: `info`\|`success`\|`warning`\|`error` | `statsd_in`'s `t:` field, only when sent -- also what `t:warning`/`t:error`/`t:success`/`t:info` map onto `LogRecord.severity` (`Warn`/`Error`/`Info`/`Info`) at decode time. `statsd_out` never re-derives this from `LogRecord.severity`: an absent carrier means an absent `t:` field, never an invented one. Same omit-and-count treatment as `statsd.event.priority` for an out-of-set value. |
| `statsd.event.aggregation_key` | `Value::Str` | `statsd_in`'s `k:` field, only when sent; round-tripped as `k:` verbatim (`\|`/control bytes substituted). |
| `statsd.event.source_type` | `Value::Str` | `statsd_in`'s `s:` field, only when sent; round-tripped as `s:` the same way as `statsd.event.aggregation_key`. |
| `statsd.event.host` | `Value::Str` | `statsd_in`'s `h:` field on an event line, only when sent; round-tripped as `h:` the same way. |
| `statsd.service_check.name` | `Value::Str` | `statsd_in`'s decode of a DogStatsD service check (`_sc\|name\|status\|...`) -- always present; the raw wire spelling, kept separately from the metric's own (normalized, interned) name since `MetricRecord` has nowhere else for it to land (rule (b), [ADR `lossless-transit`](../adr/lossless-transit.md)). `statsd_out` renders it as `name`, sanitized with the same `\|`/control-byte rule as `statsd.event.host` (not the metric-name sanitizer), so `.` and spaces survive as sent. |
| `statsd.service_check.status` | `Value::U64`: `0..=3` | `statsd_in`'s `STATUS` field -- always present, and also what the event's own `MetricKind::Gauge` value is set to. `statsd_out` prefers this carrier for the wire `status`, falling back to the gauge's own value (finite, rounding into `0..=3`) only when the carrier is absent or out of range; a service check with neither is dropped whole and counted (`EncodeStats::dropped_invalid_service_check`). |
| `statsd.service_check.message` | `Value::Str` | `statsd_in`'s `m:` field, only when sent -- verbatim, including any `\|` it contains (`m:` is always the wire line's last field, so nothing after it needs its own delimiter). `statsd_out` re-emits it last, with control bytes (including a real newline) substituted and `\|` left alone. |
| `statsd.service_check.host` | `Value::Str` | `statsd_in`'s `h:` field on a service-check line, only when sent; round-tripped the same way as `statsd.event.host`/`statsd.event.aggregation_key`. |

Rules that apply across the whole table (the trace/span rows above; `syslog.sd`'s own rules are the
linked ADR's, not these): `""`, `"-"`, and `Null` all count as absent — how nginx's
`escape=json` and a plain log format spell "this variable had no value," and how an unset HAProxy
`txn` var renders. Exactly one form of a given timing quantity may be present — the base
nanosecond form together with any suffix, or two suffixes, for the *same* quantity is invalid, not
resolved by precedence. Any two of a span's start/end/duration determine the third; a lone start
or duration borrows the event's own (receipt) timestamp as the end, which is what lets an
unchanged nginx line carrying only `request_time` still yield a span. Everything is carried and
computed as `i64` nanoseconds with checked arithmetic — `logit` never rounds a value below the
precision the source actually offered.

**The collectd codec is the same precedent once more** (`crates/logit-proto/src/collectd/`,
[ADR `collectd-binary-relay`](../adr/collectd-binary-relay.md)): collectd identifies every value
list by a five-tuple — host, plugin, plugin instance, type, type instance — that this model has one
`MetricRecord.name` for, and carries a per-list reporting interval it has no field for at all. So
the raw wire facts ride alongside as attributes and win on the way back out. Every `collectd.*`
attribute is **consumed** by `collectd_out` (never re-emitted as anything else) and appears as an
ordinary tag at every other sink, which is what makes `collectd_in -> collectd_out` a fixed point.
These are **event** attributes, never resource ones: `logit_pipeline::BatchAccumulator::absorb` keys
accumulation on `Arc::ptr_eq`, so a per-host resource would split every batch by sender — the same
reasoning behind `syslog.hostname`. The presence of `collectd.type` is what selects like-relay
encoding at `collectd_out`; an event without it is encoded through that sink's fallback naming path
instead. `crates/logit-proto/src/collectd/mod.rs`'s module doc is the full mapping table.

| Attribute | Value | Meaning |
|---|---|---|
| `collectd.host` | `Value::Str`, or `Value::Bytes` when the wire bytes aren't UTF-8 | The wire Host. Absent when the wire carried an empty one (an empty string part is how a sender *clears* a sticky field). Outranks `host.name` on `collectd_out`, which then falls back to `host.name` and finally to the sink's own `hostname:` — with none of the three, the value list is dropped and counted (`logit.output.metrics.skipped{reason="no_host"}`), since collectd's receiver rejects an empty host and inventing one would merge every unlabelled sender into a single host's metrics. |
| `collectd.plugin` | `Value::Str`/`Value::Bytes` | The wire Plugin (`cpu`, `load`, `df`). Required for like-relay encoding: a list whose plugin or type sanitizes to nothing is dropped (`{reason="empty_name"}`). |
| `collectd.plugin_instance` | `Value::Str`/`Value::Bytes`, only when non-empty | The wire PluginInstance (a CPU number, a mount point). |
| `collectd.type` | `Value::Str`/`Value::Bytes` | The wire Type — a `types.db` entry name (`cpu`, `if_octets`, `df_complex`) naming the data-source layout the list's values follow. **Its presence is what selects like-relay encoding** on `collectd_out`. |
| `collectd.type_instance` | `Value::Str`/`Value::Bytes`, only when non-empty | The wire TypeInstance (`user`, `system`, `free`). |
| `collectd.interval` | `Value::F64` seconds | The list's reporting interval, exactly `cdtime / 2³⁰` — collectd's own 2⁻³⁰-second tick unit, converted losslessly. Absent when the wire carried no interval part or a zero one; re-emitted as `IntervalHR round(v · 2³⁰)`, or as `IntervalHR 0` when absent. A non-`F64` or non-positive value is counted `logit.output.tags.dropped{reason="unrepresentable"}` and written as zero. |
| `collectd.severity` | `Value::U64` ∈ {1, 2, 4} | **Reserved for notifications** (W5 of [docs/plans/collectd-binary-relay.md](../plans/collectd-binary-relay.md)); nothing reads or writes it yet. The raw wire severity (1 FAILURE, 2 WARNING, 4 OKAY), which marks the event as a notification rather than a value list and will outrank `LogRecord.severity` on egress. |

Two model-side rules follow from that table rather than from any one attribute. A Values part
carrying N data sources becomes **one** event whose `metrics` holds N `MetricRecord`s in wire order
(`logit_core::MetricList` is a `SmallVec` inlined at 1, so the common single-source list costs
nothing extra) — not N events, which is what lets it be re-encoded as the same single list. And the
record names are display/cross-protocol only: `<plugin>.<type>` for a single-source list,
`<plugin>.<type>.<i>` (0-based) otherwise, with W2 resolving `<ds_name>` from an operator-supplied
`types.db`. Like-relay fidelity rides on the attributes, the `MetricList` order and the metric
kinds, never on the name.

`tail_in`/`docker_in` (`crates/logit-inputs/src/tail/`, `crates/logit-inputs/src/docker.rs`,
[ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)) stamp two
more event attributes, and a resource sub-convention of their own:

| Attribute | Value | Meaning |
|---|---|---|
| `log.file.path` | `Value::Str` | `tail_in` only. The absolute path of the file this line was read from. |
| `log.iostream` | `Value::Str`: `stdout`\|`stderr` | `docker_in` only. Which of the container's own two streams this line came from — `docker_in` has no per-stream filter of its own (a downstream stage, e.g. a `lua` component reading this attribute, does that). |

`docker_in`'s resource carries `container.id`, `container.name`, `container.image.name`,
`container.image.tag` (absent for an untagged/digest reference), and `container.label.<key>` for
every key named in its `labels:` config (opt-in, never every label — see the ADR's event/resource
shape section). These are read locally from the sibling `config.v2.json` at open time, once per
container, never re-read afterward — a `docker rename` after that point is a known gap
([docs/known-gaps.md](../known-gaps.md)).

## Record types

The three record types an event can independently carry ([ADR `multi-payload-events`](../adr/multi-payload-events.md)) —
no longer variants of one enum, just three fields on `Event`:

```rust
pub struct LogRecord {
    pub message: Value,
    pub severity: Option<Severity>,   // normalized syslog-style level
    pub body_format: BodyFormat,      // Raw | Json | Structured -- hints downstream parsers
    pub trace: Option<TraceRef>,      // application trace/span this log was emitted under
    pub event_name: Option<Symbol>,   // OTLP's LogRecord.event_name, mapped both ways by the OTLP codec
    pub observed_timestamp: i64,      // unix nanos observed by the collector; 0 = unset
    pub dropped_attributes_count: u32,
}

pub struct TraceRef {
    pub trace_id: [u8; 16],
    pub span_id: Option<[u8; 8]>,     // OTLP: a span_id implies a trace_id, never the reverse
    pub flags: u8,                    // W3C trace flags; bit 0 is SAMPLED
}

pub struct MetricRecord {
    pub name: Symbol,
    pub unit: Option<Symbol>,
    pub description: Option<Symbol>,
    pub start_timestamp: i64,         // 0 = unknown, OTLP's own convention -- avoids Option<i64>
    pub exemplars: Vec<Exemplar>,     // empty Vec allocates nothing on the common no-exemplars path
    pub flags: u32,                   // OTLP DataPointFlags bitmask; bit 0 = FLAG_NO_RECORDED_VALUE
    pub kind: MetricKind,
}

pub enum MetricKind {
    Sum(Sum),                             // replaces Counter; MetricKind::counter(v) for delta+monotonic
    Gauge(f64),
    GaugeDelta(f64),   // unresolved relative adjustment; resolved into Gauge by `aggregate` only
    Samples(Samples),                     // raw observations -- statsd_in's ms/h/d decode straight to this
    Distribution(DdSketch),               // produced only by `aggregate`, merging a run of Samples
    SetMembers(Vec<bytes::Bytes>),        // raw set members -- statsd_in's s decodes straight to this
    Set(HyperLogLog),                     // produced only by `aggregate`, merging a run of SetMembers
    Histogram(Histogram),                 // fixed, explicit bucket bounds
    ExponentialHistogram(ExpHistogram),   // OTLP/Prometheus base-2 exponential bucketing, kept
                                           // distinct so otlp_in -> otlp_out is a fixed point
    Summary(Summary),                     // pre-computed quantiles
}

pub struct Sum { pub value: f64, pub temporality: Temporality, pub monotonic: bool }

pub struct Samples { pub values: SmallVec<[f64; SAMPLES_INLINE]>, pub sample_rate: f64 }

pub struct Histogram {
    pub buckets: Vec<(f64, u64)>, pub temporality: Temporality,
    pub sum: Option<f64>, pub min: Option<f64>, pub max: Option<f64>,
}

pub struct ExpHistogram {
    pub scale: i32, pub zero_count: u64, pub zero_threshold: f64,
    pub positive: (i32, Vec<u64>), pub negative: (i32, Vec<u64>),   // each (offset, bucket_counts)
    pub temporality: Temporality, pub count: u64,
    pub sum: Option<f64>, pub min: Option<f64>, pub max: Option<f64>,
}

pub struct Summary { pub quantiles: Vec<(f64, f64)>, pub count: u64, pub sum: f64 }

pub struct Exemplar {
    pub timestamp: i64, pub value: f64, pub trace: Option<TraceRef>,
    pub filtered_attributes: AttrMap,
}

pub enum Temporality { Delta, Cumulative }

pub struct SpanRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: Value,
    pub kind: SpanKind,
    pub status: SpanStatus,
    pub events: Vec<SpanEvent>,
    pub links: Vec<SpanLink>,
    pub end_timestamp: i64,
    pub flags: u32,                   // W3C trace flags (low 8 bits of OTLP's Span.flags); 0 = unset
    pub ext: Option<Box<SpanExt>>,    // boxed: only an error span or one with tracestate pays for it
}

pub struct SpanExt {
    pub status_message: Option<bytes::Bytes>,
    pub trace_state: Option<bytes::Bytes>,
    pub dropped_attributes_count: u32,
    pub dropped_events_count: u32,
    pub dropped_links_count: u32,
}

pub struct SpanEvent {
    pub timestamp: i64,
    pub name: Value,
    pub attributes: AttrMap,
    pub dropped_attributes_count: u32,
}

pub struct SpanLink {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attributes: AttrMap,
    pub flags: u32,
    pub trace_state: Option<bytes::Bytes>,
    pub dropped_attributes_count: u32,
}

pub struct Scope {
    pub name: bytes::Bytes,
    pub version: bytes::Bytes,
    pub attributes: AttrMap,
    pub dropped_attributes_count: u32,
    pub schema_url: Option<bytes::Bytes>,
}

pub struct Resource {
    pub attributes: AttrMap,
    pub dropped_attributes_count: u32,
    pub schema_url: Option<bytes::Bytes>,
}
```

**`LogRecord::trace` is the application's trace context, not `logit`'s own.** `logit`'s internal
pipeline trace context (`logit_pipeline::fanout::TraceContext`, which node-visit produced what) is
a separate thing, propagated on `Delivered` and exposed to Lua as the `trace` global
([pipeline-graph.md](pipeline-graph.md)'s "Trace context propagation") -- it never appears on an
`Event`. A `TraceRef` only ever holds what a codec decoded off the wire, or what an operator's
config or script explicitly set; `logit`'s own code never invents one. See
[ADR `log-record-trace-context`](../adr/log-record-trace-context.md).

**Metric kinds are chosen to be mergeable**, because the split-collection topology
([overview](../OVERVIEW.md)) means two edge nodes' aggregates may need to combine into one
downstream, and that has to be correct, not approximate-and-hope. [ADR `metrics-model-v2`](../adr/metrics-model-v2.md)
reshaped these kinds to close the gaps [ADR `lossless-transit`](../adr/lossless-transit.md) named —
raw-sample (`Samples`) and raw-member (`SetMembers`) representations alongside the sketch/HLL ones,
temporality and monotonicity on `Sum`, sum/count/min/max on `Histogram`/`ExponentialHistogram`/
`Summary` — per [docs/plans/lossless-transit.md](../plans/lossless-transit.md)'s target model:

- `Distribution` uses **DDSketch** (`sketches-ddsketch`), which merges with a guaranteed relative
  error bound. Plain reservoir sampling or naive percentile-of-percentiles does not merge correctly
  — merging two nodes' p99s is not the p99 of the merged data — so DDSketch is load-bearing for the
  whole distributed-aggregation story, not a nice-to-have. `Samples` (raw statsd `ms`/`h`/`d`
  observations, `statsd_in`'s own decode target since W3) is what `aggregate` sketches into a
  `Distribution` by default, or retains raw under `distributions: samples` (a real absorb rule
  since W2, above).
- `Set` uses a **HyperLogLog** (wrapping the `cardinality-estimator` crate), which merges (union)
  exactly by construction. `SetMembers` (raw statsd `s` members, `statsd_in`'s own decode target
  since W3) is `Set`'s own raw counterpart, same relationship as `Samples`/`Distribution`.
  `aggregate` (W2) absorbs both raw pairs: a `Samples` series sketches into a `Distribution` by
  default (or retains raw values under `distributions: samples`, bounded by a cap), and a
  `SetMembers` series estimates into a `Set` by default (or retains an exact deduplicated member
  set under `sets: members`, bounded by a cap) — see
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s amendment for the
  full design, including the fallback rule each raw mode's cap (or, for `distributions: samples`,
  a `sample_rate` mismatch) triggers, and [ADR `statsd-output`](../adr/statsd-output.md)'s amendment
  for `statsd_in`/`statsd_out`'s own side of the raw pair.
- `Sum`/`Gauge` merge trivially (sum / last-write-wins by timestamp) for a **delta** `Sum`
  (monotonic or not — `monotonic` is carried, not merged on) and a `Gauge`; an *incoming* cumulative
  `Sum` has no merge rule defined here and passes through unmerged, the same as
  `ExponentialHistogram`/`Summary`. What a *flushed* `Sum` is labelled is a separate question,
  answered by `aggregate`'s `temporality:` mode rather than by this table: `delta` (the default)
  emits each window's own increment, while `temporality: cumulative` keeps the accumulator alive
  across flushes and emits the running total as `Sum { temporality: Cumulative }` with
  `start_timestamp` set to the series' first-seen time — the reset signal OTLP and Prometheus
  consumers need. A delta `Histogram` merges per bucket under that same mode (and passes through
  under `delta`); see [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s
  cumulative amendment.
- `GaugeDelta` is not mergeable on its own terms — it's statsd/DogStatsD's relative gauge
  adjustment (a leading `+`/`-`), decoded by `statsd_in` but left explicitly **unresolved**: it
  must never reach a sink. Only `aggregate` resolves it, applying it to a `Gauge`'s running value
  in arrival order (never touching the value's last-write-wins timestamp, asymmetric on purpose —
  see [ADR `relative-gauge-adjustments`](../adr/relative-gauge-adjustments.md)). This is the one metric kind whose
  aggregation state *always* needs to survive a flush to be correct, regardless of how `aggregate`
  is configured — see
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s gauge-retention
  amendment for why that's true for gauges specifically and only opt-in (`temporality: cumulative`)
  for a delta `Sum`.

A `Distribution`'s `count()` becomes a **population estimate**, not a count of raw observations
retained, wherever sample-rate extrapolation is in play: `aggregate`'s default `distributions:
sketch` mode inserts `(1.0 / sample_rate).round()` weighted samples per absorbed `Samples` record
via `Samples::sketch`/`DdSketch::add_weighted` (`crates/logit-core/src/metric.rs`), so a sketch fed
by the `Samples` record `statsd_in` decodes from `100|ms|@0.1` reports `count() == 10` even though
that record itself held one raw value. This is the same relationship
`MetricKind::counter(value / sample_rate)` already has for counters, made explicit for
distributions too — `count` answers "how many events this represents," not "how many raw
observations were retained." Since [ADR `lossless-transit`](../adr/lossless-transit.md)'s W3,
`statsd_in` itself performs no such extrapolation at decode time at all: the raw `sample_rate`
rides verbatim on the `Samples` record it decodes to, and only `aggregate` (or a sink encoding
`Samples` directly) ever reads it.

## What lives outside `Event`

Two things are deliberately *not* part of the per-event type, because putting them there would
either bloat every event or fight the ownership model:

- **Aggregation state** (the running DDSketch/HLL/counter between flushes) belongs to the
  stateful `aggregate` processor, not to `Event` — see [docs/design/lua-api.md](lua-api.md)'s
  `flush()` contract. `Event`/`MetricRecord` is what a processor *emits*, not what it accumulates
  into.
- **Buffering/retry state** belongs to the output layer's buffer trait
  ([docs/design/wire-protocol.md](wire-protocol.md)), not to events sitting in a queue somewhere.
- **Batch provenance** (which component created a batch, which one most recently handled it) is
  pipeline-graph identity, not data — it travels alongside `EventBatch` on the graph edge
  (`logit_pipeline::fanout::Delivered`), never inside it, so a transform has no way to forge or
  silently drop it. See [pipeline-graph.md](pipeline-graph.md)'s "Provenance propagation" and
  [ADR `batch-provenance-on-delivered`](../adr/batch-provenance-on-delivered.md). A script that
  wants it in the data copies it into an attribute explicitly; `logit` never stamps it there on its
  own.

## Codecs

Every input and output is a codec against this model:

```rust
trait Decoder { fn decode(&mut self, bytes: Bytes) -> Result<EventBatch>; }
trait Encoder { fn encode(&mut self, batch: &EventBatch) -> Result<Bytes>; }
```

statsd, syslog, collectd, OTLP, and the native protocol
([docs/design/wire-protocol.md](wire-protocol.md)) are all just implementations of these two
traits — OTLP has no special status in the core, per [ADR `native-wire-format-with-otlp-bridge`](../adr/native-wire-format-with-otlp-bridge.md).
[ADR `lossless-transit`](../adr/lossless-transit.md) generalizes this from OTLP specifically to
every protocol `logit` ships an `_in`/`_out` pair for: the model has to be a strict superset of
what each of them can express, or that codec's own relay becomes lossy. See
[docs/design/telemetry-landscape.md](telemetry-landscape.md) for what each protocol can express and
[docs/plans/lossless-transit.md](../plans/lossless-transit.md) for the resulting target shape.

## Open question

Whether `Value`/`Event` need a `#[non_exhaustive]`-style extensibility story for payload variants
users might want without a core change (a `Custom(Bytes)` escape hatch, for instance) is unresolved
— revisit once the first few real protocols are implemented and it's clear what, if anything, the
model is missing.
