# Internal data model

Every input decodes into this model, every output encodes from it, and Lua scripts operate on it
([docs/design/lua-api.md](lua-api.md)). It represents logs, metrics, and traces uniformly and
cheaply, without losing any field a supported protocol needs.

## Top-level shape

Events travel through the pipeline in **batches**, never individually, because per-event channel
sends and allocations would dominate the profile at any real throughput.

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

**An event is whatever it carries, not a tagged one-of**
([ADR `multi-payload-events`](../adr/multi-payload-events.md)). An access log line is a log record,
and once a transform like `kv_metrics` derives request/byte counts and latency from its fields, the
same event also carries those metrics. An event has at most one `log` and one `span`, and an event
with none of the three payloads is legal. A sink emits whatever it finds: `influxdb_out` writes
every metric on an event and ignores its log and span.

`Resource` is `Arc`-shared across the batch rather than copied onto every event, because a batch
typically comes from one socket, file, or OTLP request with one origin. It is per-batch, not
immutable: a transform or Lua script can replace it for the batch in hand by minting a new `Arc`.
That is how an operator declares a resource identity that `logit`'s own code won't invent
(`logit_pipeline::Transform::map_resource`,
[ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md)).

**`Event` is 864 bytes, paid unconditionally.** A statsd counter with three tags costs as much to
move as a fully populated nginx access log, because `AttrMap`'s inline capacity and `MetricKind`'s
inlined `DDSketch`/`Samples` are reserved whether or not they're used. An event is moved by value on
every hop between nodes and deep-cloned once per extra fan-out consumer, so its size is a
throughput property. [memory.md](memory.md) breaks it down term by term, measures what each
pipeline stage allocates, and lists what could be reclaimed.
`crates/logit-core/tests/type_sizes.rs` asserts the size so it can't drift silently.

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

The Lua API exposes this same type ([docs/design/lua-api.md](lua-api.md)), so there is no second
value model to keep in sync.

**Strings and blobs are `bytes::Bytes` everywhere.** A syslog line parsed out of a socket read
buffer ends up as a zero-copy slice of that buffer, not a fresh allocation. `Bytes` is cheap to
`Clone` (refcounted) and to slice, which both the parsing path and the Lua proxy depend on.

`syslog_in`, `json`, and `statsd_in` each decode a line with one allocation, however many fields,
tag values, or set members it yields. `statsd_in` takes two, a per-line and a per-batch
`Vec<Event>`, because of its multi-value grammar. `statsd_in`'s tag values, `|c:<id>`, and a
`SetMembers` line's members are zero-copy slices of the datagram, reconstructed by the same pointer
arithmetic (`slice_of`) as `syslog_in`'s fields. Tests pin these counts; see
[memory.md](memory.md)'s zero-copy section.

## Attributes: interned keys, small-map storage

Attribute keys such as `host`, `env`, and `service.name` repeat on nearly every event. Two
optimizations exploit that. Both are hard to retrofit once scripts and codecs depend on the shape:

- **Interning.** A process-wide symbol table (`lasso::ThreadedRodeo`) maps attribute keys to
  `Symbol(u32)`. `AttrMap` compares, hashes, and stores `u32`s instead of repeated string
  allocations, and the same table backs the wire format's dictionary encoding
  ([docs/design/wire-protocol.md](wire-protocol.md)).
- **Small-map layout.** Most events carry well under a dozen attributes. At that size
  `AttrMap = SmallVec<[(Symbol, Value); 8]>`, kept sorted by `Symbol`, beats a `HashMap` for both
  lookup and iteration. It also gives a deterministic order, which the wire format's dictionary
  encoding and reproducible tests rely on.

## Well-known attribute names

When data belongs on an event but doesn't warrant a core-model field, a codec stamps it as a
dotted, `service.name`-style attribute. `syslog_in` stamps
`syslog.facility`/`.severity`/`.timestamp`/`.hostname`/`.tag`/`.pid`/`.msgid`/`.sd`, and the OTLP
codec stamps `otel.severity_number`/`otel.severity_text`. `syslog.pid` may be `Value::Str` as well
as `Value::U64`, because RFC 5424's PROCID is free-form PRINTUSASCII. `syslog.timestamp` may be
`Value::Null` (a nil `-` RFC 5424 TIMESTAMP) as well as `Value::Timestamp`/`Value::Str`. See
[ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md).

**The raw, protocol-native value rides alongside the normalized field and wins on the way back
out.** `syslog_in`/`syslog_out` set this precedent for `syslog.severity`
([ADR `syslog-output`](../adr/syslog-output.md)'s "Header-field precedence"), and
[ADR `lossless-transit`](../adr/lossless-transit.md)'s rule (b) makes it a repo-wide rule. OTLP
severity follows it: OTLP's 24 raw severity numbers collapse onto this model's 6-variant `Severity`
on decode, so the raw number travels as an attribute.

| Attribute | Value | Meaning |
|---|---|---|
| `otel.severity_number` | `Value::I64`, `1..=24` | Stamped by `otlp_in` when the wire's `severity_number` is non-zero. `otlp_out` prefers this over the band-derived value when present, and consumes (removes) it from the emitted attribute set. |
| `otel.severity_text` | `Value::Str` | Stamped by `otlp_in` when the wire's `severity_text` is non-empty. Same precedence and consumption rule as `otel.severity_number`. |

**The Prometheus codec follows the same rule** (`crates/logit-proto/src/prometheus/`,
[ADR `prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md)). The
exposition format distinguishes cases this model has one kind for: an `untyped` sample from a
`gauge`, and a sample that carried its own timestamp from one that didn't. `prometheus_out`
**consumes** every `prometheus.*` attribute (never renders it as a label); every other sink sees it
as an ordinary tag. That is what makes `prometheus_in -> prometheus_out` an exact fixed point.

| Attribute | Value | Meaning |
|---|---|---|
| `prometheus.type` | `Value::Str`: `untyped`\|`unknown`\|`info`\|`stateset`\|`gaugehistogram` | The wire family type for the five cases the model has no distinct kind for: `untyped`/`unknown` (both a `Gauge`, one spelling per dialect), `info` (a `Gauge(1)` whose labels are the payload), `stateset` (one `Gauge(0\|1)` per state), `gaugehistogram` (a `Histogram` of a quantity that can decrease). Stamped by `prometheus_in`; read and consumed by `prometheus_out`, which re-emits that exact family type. |
| `prometheus.timestamp` | `Value::Bool(true)` | The sample carried its own timestamp on the wire (most don't; a scrape stamps them all with its own start time). `prometheus_out` re-emits a timestamp on that line only, in the output dialect's own unit. This is the "the wire carried its own timestamp" convention `statsd.timestamp` below uses for DogStatsD's `|T` segment, reduced to a boolean: the sample's timestamp already is the event's own, so only the fact that it was sent needs carrying. |
| `prometheus.target` | `Value::Str`, a **resource** attribute | The full scrape URL of the target this batch came from (`http://node-exporter:9100/metrics`), stamped once per target by `prometheus_in`. Factual, like `docker_in`'s `container.*`, not an invented `service.name`. Consumed by `prometheus_out`. |
| `instance` | `Value::Str`, a **resource** attribute | `host:port` of the scraped target. Deliberately **unprefixed**, so it renders as a label like any other resource attribute: the same `instance` label Prometheus's own scrape adds. Without it, two targets running the same exporter would collapse onto one series through a relay. An event-level `instance` wins over the resource's (`honor_labels` semantics), which falls out of the ordinary resource/event attribute merge. `job` is operator identity, not a scrape fact, and comes from a downstream `set`. |

The next table starts with the trace/span names `trace_context`'s `span:` block reads
([ADR `trace-context-span-lifting`](../adr/trace-context-span-lifting.md)), then lists the syslog
and statsd carriers. The trace/span names are reserved attributes that several producers write and
a transform reads, so they are listed here rather than left to that transform's source.

A protocol whose tag/param namespace is a **multiset** (a key can legally repeat on one line, each
occurrence a distinct value) folds the repeats into one attribute holding a `Value::Array` in wire
order. A last-write-wins map insertion would silently destroy every occurrence but the last.
`syslog.sd` (a repeated RFC 5424 PARAM-NAME within one SD-ELEMENT) and DogStatsD's `|#` tags (a
repeated tag key) produce this shape; see
[ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md) and [ADR
`statsd-output`](../adr/statsd-output.md)'s amendment.

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
| *(any DogStatsD tag key)* | `Value::Str`\|`Value::Bool`, or `Value::Array` of either when the key repeated | `statsd_in`'s `insert_tags` folds a repeated `\|#` tag key into a `Value::Array` in wire order (`#team:a,team:b` -> `Array[Str("a"), Str("b")]`), the same multiset fold `syslog.sd` above uses; an exact-duplicate token dedupes at decode instead, so a non-repeated tag's shape is unchanged (`Value::Str` for `key:value`, `Value::Bool(true)` for a bare `key`). `statsd_out` expands an `Array` back into one wire tag per element. See [ADR `statsd-output`](../adr/statsd-output.md)'s amendment. |
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

Rules for the trace/span rows (`syslog.sd` follows its own ADR's rules instead):

- `""`, `"-"`, and `Null` all count as absent. That is how nginx's `escape=json` and a plain log
  format spell "this variable had no value," and how an unset HAProxy `txn` var renders.
- Exactly one form of a timing quantity may be present. The base nanosecond form together with a
  suffixed form, or two suffixed forms, of the *same* quantity is invalid, not resolved by
  precedence.
- Any two of a span's start, end, and duration determine the third. A lone start or duration
  borrows the event's own (receipt) timestamp as the end, which lets an unchanged nginx line
  carrying only `request_time` still yield a span.
- Everything is carried and computed as `i64` nanoseconds with checked arithmetic. `logit` never
  rounds a value below the precision the source offered.

**The collectd codec follows the same rule** (`crates/logit-proto/src/collectd/`,
[ADR `collectd-binary-relay`](../adr/collectd-binary-relay.md)). collectd identifies every value
list by a five-tuple (host, plugin, plugin instance, type, type instance) where this model has one
`MetricRecord.name`, and carries a per-list reporting interval the model has no field for. The raw
wire facts ride alongside as attributes and win on the way back out. `collectd_out` **consumes**
every `collectd.*` attribute (never re-emits it as anything else); every other sink sees it as an
ordinary tag. That is what makes `collectd_in -> collectd_out` a fixed point.

These are **event** attributes, never resource ones. `logit_pipeline::BatchAccumulator::absorb`
keys accumulation on `Arc::ptr_eq`, so a per-host resource would split every batch by sender; the
same reasoning keeps `syslog.hostname` on the event. The presence of `collectd.type` selects
like-relay encoding at `collectd_out`; an event without it goes through that sink's fallback naming
path. `crates/logit-proto/src/collectd/mod.rs`'s module doc has the full mapping table.

| Attribute | Value | Meaning |
|---|---|---|
| `collectd.host` | `Value::Str`, or `Value::Bytes` when the wire bytes aren't UTF-8 | The wire Host. Absent when the wire carried an empty one (an empty string part is how a sender *clears* a sticky field). Outranks `host.name` on `collectd_out`, which then falls back to `host.name` and finally to the sink's own `hostname:` — with none of the three, the value list is dropped and counted (`logit.output.metrics.skipped{reason="no_host"}`), since collectd's receiver rejects an empty host and inventing one would merge every unlabelled sender into a single host's metrics. |
| `collectd.plugin` | `Value::Str`/`Value::Bytes` | The wire Plugin (`cpu`, `load`, `df`). Required for like-relay encoding: a list whose plugin or type sanitizes to nothing is dropped (`{reason="empty_name"}`). |
| `collectd.plugin_instance` | `Value::Str`/`Value::Bytes`, only when non-empty | The wire PluginInstance (a CPU number, a mount point). |
| `collectd.type` | `Value::Str`/`Value::Bytes` | The wire Type — a `types.db` entry name (`cpu`, `if_octets`, `df_complex`) naming the data-source layout the list's values follow. **Its presence is what selects like-relay encoding** on `collectd_out`. |
| `collectd.type_instance` | `Value::Str`/`Value::Bytes`, only when non-empty | The wire TypeInstance (`user`, `system`, `free`). |
| `collectd.interval` | `Value::F64` seconds | The list's reporting interval, exactly `cdtime / 2³⁰` — collectd's own 2⁻³⁰-second tick unit, converted losslessly. Absent when the wire carried no interval part or a zero one; re-emitted as `IntervalHR round(v · 2³⁰)`, or as `IntervalHR 0` when absent. A non-`F64` or non-positive value is counted `logit.output.tags.dropped{reason="unrepresentable"}` and written as zero. |
| `collectd.severity` | `Value::U64` ∈ {1, 2, 4} | The raw wire severity of a *notification* (1 FAILURE, 2 WARNING, 4 OKAY). Its presence is what marks an event as a notification rather than a value list; decoded from a `0x0101` Severity part alongside a `0x0100` Message into an `Event::log` (`LogRecord.severity` 1→`Error`/2→`Warn`/4→`Info`). On `collectd_out`, this raw attribute — not the normalized `LogRecord.severity` — is what reaches the wire, outranking it exactly as `syslog.severity`/`otel.severity_number` outrank their own normalized field (rule (b), [ADR `lossless-transit`](../adr/lossless-transit.md)); absent or out of `{1, 2, 4}` → the notification is dropped (`logit.output.metrics.skipped{reason="notification_dropped"}`). |

Two model-side rules follow from that table:

- A Values part carrying N data sources becomes **one** event whose `metrics` holds N
  `MetricRecord`s in wire order, not N events, so it re-encodes as the same single list.
  `logit_core::MetricList` is a `SmallVec` inlined at 1, so the common single-source list costs
  nothing extra.
- Record names are **display/cross-protocol only**. Like-relay fidelity rides on the attributes, the
  `MetricList` order, and the metric kinds, never on the name. The naming rules below change what an
  InfluxDB, Prometheus, or statsd sink calls the series, and nothing about what `collectd_out` puts
  back on the wire.

The wire carries no data-source names, so records are named as follows:

| The list's `collectd.type` | Record name |
|---|---|
| resolved in an operator-supplied `types.db` (`collectd_in`'s `types_db:`) with a matching data-source **count and kinds**, single-source | `<plugin>.<type>` — the lone data source (conventionally `value`) is omitted, collectd's own `write_graphite` default |
| resolved likewise, multi-source | `<plugin>.<type>.<ds_name>` (`load.load.shortterm`) |
| resolved, but its data-source count or kinds disagree with the wire | index naming, plus a throttled `types_db_mismatch` diagnostic — the configured file is not the one the sender is running against |
| not in the configured files, or no `types_db:` configured at all | index naming, no diagnostic: a type missing from `types.db` is routine |

Index naming is `<plugin>.<type>` for a single-source list and `<plugin>.<type>.<i>` (0-based)
otherwise.

**A protocol-namespaced attribute is not the default for a new codec.** A codec adds one only when
the wire says something the model would otherwise throw away or reinterpret. The Graphite/Carbon
codec (`crates/logit-proto/src/graphite/`,
[ADR `graphite-carbon-relay`](../adr/graphite-carbon-relay.md)) adds **none**: no `graphite.*`
namespace, no `pub const ATTR_*`. Carbon's wire carries exactly four facts, and each already has a
lossless home in this model: the dotted path is `MetricRecord.name`, the `;k=v` tags are event
attributes, the number is `MetricKind::Gauge`'s payload, and the whole second is
`Event.timestamp`. Rule (b) of [ADR `lossless-transit`](../adr/lossless-transit.md) exists to
resolve a *conflict* between a raw wire fact and the normalized model field, and here there is
none. A carrier would be a second spelling of something already stored once, and
`graphite_in -> graphite_out` is a fixed point without one.

This makes Graphite *less* forgiving than collectd or syslog in one place: with no
`graphite.path` carrier, a `lua`/`set` stage that renames `MetricRecord.name` silently changes the
wire path. That is the intended way to rename a series, because neither `graphite_in` nor
`graphite_out` has a `prefix:`/`template:` field; [docs/deploying.md](../deploying.md) says so.

`tail_in`/`docker_in` (`crates/logit-inputs/src/tail/`, `crates/logit-inputs/src/docker.rs`,
[ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md)) stamp two
event attributes and a set of resource attributes:

| Attribute | Value | Meaning |
|---|---|---|
| `log.file.path` | `Value::Str` | `tail_in` only. The absolute path of the file this line was read from. |
| `log.iostream` | `Value::Str`: `stdout`\|`stderr` | `docker_in` only. Which of the container's two streams this line came from. `docker_in` has no per-stream filter; a downstream stage, such as a `lua` component reading this attribute, does that. |

`docker_in`'s resource carries `container.id`, `container.name`, `container.image.name`,
`container.image.tag` (absent for an untagged or digest reference), and `container.label.<key>` for
every key named in its `labels:` config (opt-in, never every label; see the ADR's event/resource
shape section). `docker_in` reads these from the sibling `config.v2.json` once per container, when
it opens the log, and never re-reads them, so a `docker rename` after that point is not picked up,
a known gap ([docs/known-gaps.md](../known-gaps.md)).

### HTTP access-log names

[`docs/http-access-logs.md`](../http-access-logs.md) is the canonical table of HTTP attribute
names: the OTel semconv names a web server's access line is logged under, `logit`'s own composites
and proxy fields, and what `http_access` does to each
([ADR `http-access-normalization`](../adr/http-access-normalization.md)). It overlaps the
trace/span table above in two ways:

- `http_access` *emits* `span.name` (`{method} {route}`), `span.status` (`error` for a 5xx or `0`
  status, `unset` otherwise, never `ok`), and `span.duration_s` (mirrored from the request duration
  unless the line already states a span duration, or both a start and an end). `trace_context` then
  reads them as documented above.
- `http_access` accepts every trace/span name in that table spelled with each `.` replaced by `-`
  (`trace-id`, `span-parent_id`, `span-start_us`) and renames it to the dotted spelling, for
  emitters whose key grammar forbids a dot (HAProxy's `%{+json}o`). `trace_context` reads only the
  dotted names, so the dashed spelling works only with `http_access` ahead of it.

## Record types

The three record types are independent fields on `Event`, not variants of one enum
([ADR `multi-payload-events`](../adr/multi-payload-events.md)):

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
pipeline trace context (`logit_pipeline::fanout::TraceContext`, which node visit produced what)
travels on `Delivered` and is exposed to Lua as the `trace` global
([pipeline-graph.md](pipeline-graph.md)'s "Trace context propagation"); it never appears on an
`Event`. A `TraceRef` holds only what a codec decoded off the wire or what an operator's config or
script explicitly set. `logit`'s own code never invents one. See
[ADR `log-record-trace-context`](../adr/log-record-trace-context.md).

**Metric kinds are mergeable**, because in the split-collection topology
([overview](../OVERVIEW.md)) two edge nodes' aggregates may combine into one downstream, and the
result has to be correct, not approximate. [ADR `metrics-model-v2`](../adr/metrics-model-v2.md)
shaped these kinds to close the gaps [ADR `lossless-transit`](../adr/lossless-transit.md) named, per
[docs/plans/lossless-transit.md](../plans/lossless-transit.md)'s target model: raw-sample
(`Samples`) and raw-member (`SetMembers`) representations alongside the sketch and HyperLogLog
ones, temporality and monotonicity on `Sum`, and sum/count/min/max on
`Histogram`/`ExponentialHistogram`/`Summary`.

- `Distribution` uses **DDSketch** (`sketches-ddsketch`), which merges with a guaranteed relative
  error bound. Reservoir sampling and percentile-of-percentiles don't merge correctly (two nodes'
  p99s don't combine into the p99 of the merged data), so the whole distributed-aggregation story
  depends on DDSketch. `Samples` holds raw statsd `ms`/`h`/`d` observations; `statsd_in` decodes
  those lines to it.
- `Set` uses a **HyperLogLog** (wrapping the `cardinality-estimator` crate), whose merge (union) is
  exact by construction. `SetMembers` holds raw statsd `s` members; `statsd_in` decodes those lines
  to it. It is `Set`'s raw counterpart, as `Samples` is `Distribution`'s.
- `aggregate` absorbs both raw kinds. By default a `Samples` series sketches into a `Distribution`
  and a `SetMembers` series estimates into a `Set`. Under `distributions: samples` it retains the
  raw values instead, and under `sets: members` an exact deduplicated member set, each bounded by a
  cap. [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s amendment has
  the full design, including the fallback each raw mode's cap (or, for `distributions: samples`, a
  `sample_rate` mismatch) triggers. [ADR `statsd-output`](../adr/statsd-output.md)'s amendment
  covers `statsd_in`/`statsd_out`'s side of the raw pair.
- A **delta** `Sum` merges by summing (monotonic or not; `monotonic` is carried, not merged on), and
  a `Gauge` by last-write-wins on timestamp. An *incoming* cumulative `Sum` has no merge rule and
  passes through unmerged, like `ExponentialHistogram`/`Summary`. `aggregate`'s `temporality:` mode
  decides how a *flushed* `Sum` is labeled: `delta` (the default) emits each window's own
  increment; `temporality: cumulative` keeps the accumulator across flushes and emits the running
  total as `Sum { temporality: Cumulative }`, with `start_timestamp` set to the series' first-seen
  time (the reset signal OTLP and Prometheus consumers need). A delta `Histogram` merges per bucket
  under `cumulative` and passes through under `delta`; see
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s cumulative
  amendment.
- `GaugeDelta` is statsd/DogStatsD's relative gauge adjustment (a leading `+`/`-`) and is not
  mergeable on its own. `statsd_in` decodes it but leaves it explicitly **unresolved**, and it must
  never reach a sink. Only `aggregate` resolves it, applying it to a `Gauge`'s running value in
  arrival order without touching that value's last-write-wins timestamp, asymmetric on purpose (see
  [ADR `relative-gauge-adjustments`](../adr/relative-gauge-adjustments.md)). It is the one metric
  kind whose aggregation state must *always* survive a flush to be correct, however `aggregate` is
  configured; for a delta `Sum` that is opt-in (`temporality: cumulative`). See
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s gauge-retention
  amendment for why.

**A `Distribution`'s `count()` is a population estimate**, not a count of retained raw observations,
wherever a sample rate applies. `aggregate`'s default `distributions: sketch` mode inserts
`(1.0 / sample_rate).round()` weighted samples per absorbed `Samples` record via
`Samples::sketch`/`DdSketch::add_weighted` (`crates/logit-core/src/metric.rs`). A sketch fed the
`Samples` record `statsd_in` decodes from `100|ms|@0.1` reports `count() == 10`, though that record
held one raw value. Counters already work this way (`MetricKind::counter(value / sample_rate)`):
`count` answers "how many events this represents." `statsd_in` performs no extrapolation at decode
time ([ADR `lossless-transit`](../adr/lossless-transit.md)): the raw `sample_rate` rides verbatim on
the `Samples` record, and only `aggregate`, or a sink encoding `Samples` directly, reads it.

## What lives outside `Event`

Three things are deliberately *not* part of the per-event type, because putting them there would
bloat every event or fight the ownership model:

- **Aggregation state** (the running DDSketch, HyperLogLog, or counter between flushes) belongs to
  the stateful `aggregate` processor; see [docs/design/lua-api.md](lua-api.md)'s `flush()`
  contract. `Event`/`MetricRecord` is what a processor *emits*, not what it accumulates into.
- **Buffering and retry state** belongs to the sink's delivery queue (`SinkQueue`, or a
  `buffer.disk:` spool), not to the events in it
  ([docs/design/wire-protocol.md](wire-protocol.md)).
- **Batch provenance** (which component created a batch and which one last handled it) is
  pipeline-graph identity, not data. It travels alongside `EventBatch` on the graph edge
  (`logit_pipeline::fanout::Delivered`), never inside it, so a transform can't forge or silently
  drop it. See [pipeline-graph.md](pipeline-graph.md)'s "Provenance propagation" and
  [ADR `batch-provenance-on-delivered`](../adr/batch-provenance-on-delivered.md). A script that
  wants it in the data copies it into an attribute explicitly; `logit` never stamps it there.

## Codecs

Every input and output is a codec against this model. Listeners implement one decoder trait;
sinks implement whichever of three encoder shapes their wire format is:

```rust
trait Decoder { fn decode(&mut self, bytes: Bytes) -> Result<EventBatch>; }

// One opaque blob per batch: the native protocol, InfluxDB line protocol, stdio/file.
trait Encoder { fn encode(&mut self, batch: &EventBatch) -> Result<Bytes>; }
// One payload per signal: OTLP, whose logs/metrics/traces are three RPCs.
trait SignalEncoder { fn encode_signals(&mut self, batch: &EventBatch) -> Result<Vec<(Signal, Bytes)>>; }
// N framed messages per batch, never failing, per-message drops counted: syslog, statsd.
trait FramedEncoder { type Meta; type Stats; fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf<Self::Meta>) -> Self::Stats; }
```

statsd, syslog, OTLP, collectd, graphite, and the native protocol
([docs/design/wire-protocol.md](wire-protocol.md)) are all implementations of these traits. What
the transport needs to frame decides which shape a codec gets, not the protocol's importance
([ADR `framed-encoder`](../adr/framed-encoder.md)), and OTLP has no special status in the core
([ADR `native-wire-format-with-otlp-bridge`](../adr/native-wire-format-with-otlp-bridge.md)).
`prometheus` is the one pair outside these traits: a scrape client and a registry rendered on
demand ([ADR `prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md)).

**The model must be a strict superset of what every `_in`/`_out` protocol can express**, or that
protocol's relay becomes lossy ([ADR `lossless-transit`](../adr/lossless-transit.md)). See
[docs/design/telemetry-landscape.md](telemetry-landscape.md) for what each protocol can express and
[docs/plans/lossless-transit.md](../plans/lossless-transit.md) for the resulting target shape.

## Open question

Should `Value`/`Event` have a `#[non_exhaustive]`-style extension point, such as a `Custom(Bytes)`
escape hatch, for payloads users want without a core change? This is unresolved. `Value` is a
closed enum today, and the native wire format decodes an unrecognized value tag to `Value::Null`
(`crates/logit-proto/src/native/value.rs`). No shipped protocol has needed an escape hatch yet.
