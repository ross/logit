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

**An event is whatever it carries, not a tagged one-of** ([ADR 0012](../adr/0012-multi-payload-events.md)).
An access log line is a log record and, once a transform like `kv_metrics` derives request/byte
counts and latency from its fields, a source of several metrics at once — the same event, not two
related-but-separate ones. `log`/`span` stay `Option` (an event can have at most one of each); an
event with none of the three is legal and representable. A sink emits whatever it finds:
`influxdb_out` writes every metric on an event and ignores its log/span.

`Resource` is `Arc`-shared rather than copied onto every event — a batch typically comes from one
socket/file/OTLP request and shares one origin.

**`Event` is 792 bytes**, and that size is paid unconditionally — a statsd counter with three tags
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

## Record types

The three record types an event can independently carry ([ADR 0012](../adr/0012-multi-payload-events.md)) —
no longer variants of one enum, just three fields on `Event`:

```rust
pub struct LogRecord {
    pub message: Value,
    pub severity: Option<Severity>,   // normalized syslog-style level
    pub body_format: BodyFormat,      // Raw | Json | Structured -- hints downstream parsers
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
  see [ADR 0026](../adr/0026-relative-gauge-adjustments.md)). This is the one metric kind whose
  aggregation state genuinely needs to survive a flush to be correct — see
  [ADR 0008](../adr/0008-aggregation-window-semantics.md)'s amendment for why that's true for
  gauges specifically and not for `Counter`.

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
traits — OTLP has no special status in the core, per [ADR 0004](../adr/0004-native-wire-format-with-otlp-bridge.md).
This is also why the model has to be a strict superset of what OTLP can express: anything OTLP can
carry that `Event` can't represent makes the OTLP codec lossy.

## Open question

Whether `Value`/`Event` need a `#[non_exhaustive]`-style extensibility story for payload variants
users might want without a core change (a `Custom(Bytes)` escape hatch, for instance) is unresolved
— revisit once the first few real protocols are implemented and it's clear what, if anything, the
model is missing.
