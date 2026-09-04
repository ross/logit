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

**`Event` is 800 bytes**, and that size is paid unconditionally — a statsd counter with three tags
costs exactly as much to move as a fully-populated nginx access log, because `AttrMap`'s inline
capacity and `MetricKind`'s inlined `DDSketch` are reserved whether or not they're used. Since an
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

Measured, `syslog_in` and `json` keep that promise (decoding a line costs one allocation regardless
of how many fields it yields) and `statsd_in` does not (it copies each tag value out of the
datagram instead of slicing it). See [memory.md](memory.md)'s zero-copy section — both facts are
pinned by tests, not left to inspection.

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

`syslog_in` (`syslog.facility`/`.severity`/`.timestamp`/`.hostname`/`.tag`/`.pid`/`.msgid`) and the
OTLP codec (`otel.status_message`) already stamp dotted, `service.name`-style attribute names as a
convention rather than a typed field, when the data belongs on the event but doesn't rise to a
core-model field of its own. `trace_context`'s `span:` block
([ADR `trace-context-span-lifting`](../adr/trace-context-span-lifting.md)) is the first place this
convention is deliberately *read* by more than one producer, so it's worth naming as a real table
rather than leaving it to be reverse-engineered from that transform's source:

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

Rules that apply across the whole table: `""`, `"-"`, and `Null` all count as absent — how nginx's
`escape=json` and a plain log format spell "this variable had no value," and how an unset HAProxy
`txn` var renders. Exactly one form of a given timing quantity may be present — the base
nanosecond form together with any suffix, or two suffixes, for the *same* quantity is invalid, not
resolved by precedence. Any two of a span's start/end/duration determine the third; a lone start
or duration borrows the event's own (receipt) timestamp as the end, which is what lets an
unchanged nginx line carrying only `request_time` still yield a span. Everything is carried and
computed as `i64` nanoseconds with checked arithmetic — `logit` never rounds a value below the
precision the source actually offered.

## Record types

The three record types an event can independently carry ([ADR `multi-payload-events`](../adr/multi-payload-events.md)) —
no longer variants of one enum, just three fields on `Event`:

```rust
pub struct LogRecord {
    pub message: Value,
    pub severity: Option<Severity>,   // normalized syslog-style level
    pub body_format: BodyFormat,      // Raw | Json | Structured -- hints downstream parsers
    pub trace: Option<TraceRef>,      // application trace/span this log was emitted under
}

pub struct TraceRef {
    pub trace_id: [u8; 16],
    pub span_id: Option<[u8; 8]>,     // OTLP: a span_id implies a trace_id, never the reverse
    pub flags: u8,                    // W3C trace flags; bit 0 is SAMPLED
}

pub struct MetricRecord {
    pub name: Symbol,
    pub kind: MetricKind,
    pub unit: Option<Symbol>,
}

pub enum MetricKind {
    Counter(f64),
    Gauge(f64),
    GaugeDelta(f64),  // unresolved relative adjustment; resolved into Gauge by `aggregate` only
    Set(HyperLogLog),
    Distribution(DdSketch),
    Histogram { buckets: Vec<(f64, u64)> },   // fixed-bucket, e.g. Prometheus-style input
    Summary { quantiles: Vec<(f64, f64)> },   // pre-computed quantiles, e.g. some scrape inputs
}

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
downstream, and that has to be correct, not approximate-and-hope:

- `Distribution` uses **DDSketch** (`sketches-ddsketch`), which merges with a guaranteed relative
  error bound. Plain reservoir sampling or naive percentile-of-percentiles does not merge correctly
  — merging two nodes' p99s is not the p99 of the merged data — so DDSketch is load-bearing for the
  whole distributed-aggregation story, not a nice-to-have.
- `Set` uses a **HyperLogLog**, which merges (union) exactly by construction.
- `Counter`/`Gauge` merge trivially (sum / last-write-wins by timestamp).
- `GaugeDelta` is not mergeable on its own terms — it's statsd/DogStatsD's relative gauge
  adjustment (a leading `+`/`-`), decoded by `statsd_in` but left explicitly **unresolved**: it
  must never reach a sink. Only `aggregate` resolves it, applying it to a `Gauge`'s running value
  in arrival order (never touching the value's last-write-wins timestamp, asymmetric on purpose —
  see [ADR `relative-gauge-adjustments`](../adr/relative-gauge-adjustments.md)). This is the one metric kind whose
  aggregation state genuinely needs to survive a flush to be correct — see
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s amendment for why that's true for
  gauges specifically and not for `Counter`.

A `Distribution`'s `count()` becomes a **population estimate**, not a count of received
datagrams, wherever sample-rate extrapolation is in play: `statsd_in`'s `ms`/`h`/`d` decoding
(`crates/logit-inputs/src/statsd.rs`) inserts `(1.0 / sample_rate).round()` weighted samples per
line via `DdSketch::add_weighted`, so a sketch fed by `100|ms|@0.1` reports `count() == 10` even
though only one datagram arrived. This is the same relationship `Counter(value / sample_rate)`
already has for counters, made explicit for distributions too — `count` answers "how many events
this represents," not "how many datagrams I received."

## What lives outside `Event`

Two things are deliberately *not* part of the per-event type, because putting them there would
either bloat every event or fight the ownership model:

- **Aggregation state** (the running DDSketch/HLL/counter between flushes) belongs to the
  stateful `aggregate` processor, not to `Event` — see [docs/design/lua-api.md](lua-api.md)'s
  `flush()` contract. `Event`/`MetricRecord` is what a processor *emits*, not what it accumulates
  into.
- **Buffering/retry state** belongs to the output layer's buffer trait
  ([docs/design/wire-protocol.md](wire-protocol.md)), not to events sitting in a queue somewhere.

## Codecs

Every input and output is a codec against this model:

```rust
trait Decoder { fn decode(&mut self, bytes: Bytes) -> Result<EventBatch>; }
trait Encoder { fn encode(&mut self, batch: &EventBatch) -> Result<Bytes>; }
```

statsd, syslog, collectd, OTLP, and the native protocol
([docs/design/wire-protocol.md](wire-protocol.md)) are all just implementations of these two
traits — OTLP has no special status in the core, per [ADR `native-wire-format-with-otlp-bridge`](../adr/native-wire-format-with-otlp-bridge.md).
This is also why the model has to be a strict superset of what OTLP can express: anything OTLP can
carry that `Event` can't represent makes the OTLP codec lossy.

## Open question

Whether `Value`/`Event` need a `#[non_exhaustive]`-style extensibility story for payload variants
users might want without a core change (a `Custom(Bytes)` escape hatch, for instance) is unresolved
— revisit once the first few real protocols are implemented and it's clear what, if anything, the
model is missing.
