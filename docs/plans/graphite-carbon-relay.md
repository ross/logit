---
created: 2026-09-13
updated: 2026-09-13
---

# Enabling plan: `graphite_in`/`graphite_out` — a lossless Graphite/Carbon relay

## Context

`logit` has five lossless like-protocol pairs under ADR [`lossless-transit`](../adr/lossless-transit.md)
(statsd, otlp, syslog, prometheus, collectd). [`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md)
surveys Graphite (its "Graphite" section and the metrics matrix's `Graphite` column) but nothing is
built against it, and ADR [`framed-encoder`](../adr/framed-encoder.md)'s Consequences names "a
Graphite/Carbon line sink" as the anticipated fourth framed sink. Ross asked for a Graphite/Carbon
sink; on questioning he widened it to the full pair so `graphite_in -> graphite_out` is the sixth
fixed point, with both Carbon wire protocols (plaintext and pickle), a config switch for
multi-value metric kinds (skip by default, expand opt-in), and tags on by default.

Like collectd, this needs **no core-model change**: a Graphite datapoint is one untyped number at
one second (`MetricKind::Gauge(f64)` + `Event::timestamp`), tags are string attributes. **No new
crate dependency**: the pickle writer and restricted reader are hand-rolled, so `deny.toml` and
`script/audit` stay unchanged.

## Decisions already settled

Settled with Ross (2026-09-13), recorded in full in the ADR:

1. **Both directions, one lossless pair.** `graphite_in` (carbon receiver) and `graphite_out`
   (carbon sender); `graphite_in -> graphite_out` is a fixed point modulo the numbered list below.
2. **Both wire protocols.** Plaintext `path[;k=v...] value timestamp\n` (TCP or UDP, port 2003) and
   the pickle batch protocol (TCP only, port 2004: 4-byte **big-endian** length prefix — Twisted's
   `Int32StringReceiver` — then a pickled `[(path, (timestamp, value)), ...]`). Hand-rolled writer
   (protocol-2 subset) and **restricted reader** accepting what real senders emit
   (`pickle.dumps(..., protocol=2)` and `protocol=-1`: `FRAME`, `SHORT_BINUNICODE`, `MEMOIZE`),
   rejecting everything else (no `GLOBAL`/`STACK_GLOBAL`/`REDUCE`/`BUILD`; bounded depth, memo,
   items; lengths validated before allocation).
3. **Multi-value kinds are a `graphite_out` switch: `multi_value: skip | expand`** (name from the
   landscape matrix row "Multi-value point"). `skip` (default): `logit.output.metrics.skipped
   {metric_kind=…}`. `expand`: dotted sub-paths per the table below, counted
   `logit.output.metrics.degraded{metric_kind=…}` once per record. `Samples` go through
   `logit_core::Samples::sketch()` (`crates/logit-core/src/metric.rs:192`); quantiles are
   `crate::otlp::metrics::DISTRIBUTION_QUANTILES` (`crates/logit-proto/src/otlp/metrics.rs:102`,
   already `pub(crate)`, reachable from `logit-proto/src/graphite/` exactly as
   `prometheus/mod.rs:143` does). influxdb's `[0.5,0.9,0.99]` set is left alone.
4. **Tags.** `tags: carbon` (default): resource⊕event attributes via `logit_core::attrs::merged`
   rendered `;k=v` in ascending **rendered-name** order. `tags: drop`: no tag segment, counted
   `logit.output.tags.dropped{reason="dialect"}` per wire tag. `Value::Array` → last representable
   element, `logit.output.tags.normalized{reason="multi_value"}`. Graphite rules: tag name forbids
   `;` `!` `^` `=`; tag value forbids `;` and a leading `~`; path forbids whitespace and `;`.
   Substitution with `_`, never deletion. **No prefix/template/path-override field and no
   `graphite.path` carrier**: the path *is* `MetricRecord.name`; a `lua` stage renames.
5. **Model mapping.** Wire value → `MetricKind::Gauge(f64)`; one `Event` with one `MetricRecord`
   per line/datapoint; `event.timestamp = ts * 1e9`; tags are **event** attributes; the decoder
   returns one shared `Resource::default()` (`BatchAccumulator` keys on `Arc::ptr_eq`). Both `Sum`
   temporalities encode as the bare value — a **named normalization, not a skip** (not
   `prometheus_out`'s delta-skip). `GaugeDelta` skipped with the shared `gauge_delta_unresolved`
   key. `FLAG_NO_RECORDED_VALUE` skipped `{reason="no_recorded_value"}` (`metric.rs:36-40`).
6. **`duplicate_safe() -> true`** for `graphite_out`: whisper is last-write-wins per
   `(path, second)` — `influxdb.rs:191-199`'s argument. First non-HTTP sink with a real
   destination to report `true` (`null_out` reports it trivially, having no destination).
7. **Timestamps.** Egress: `event.timestamp.div_euclid(1_000_000_000)`; a result `<= 0` drops the
   record, `{reason="unencodable_timestamp"}` (collectd `encode.rs:52-58` precedent). Ingress: `-1`
   = receipt time (carbon's rule); any other non-positive timestamp rejects the line.
8. **Stacked PRs** `feat/graphite-w0` → `w1` → (`w2` ‖ `w3`) → (`w4a` ‖ `w4b`), each branched from
   its parent, retargeted to `main` when the parent merges.

Settled by the lead while designing (Ross can veto any at review):

9. **Repeated tag key collapses to its last value at decode**, counted
   `logit.input.tags.normalized{reason="duplicate_key"}` — carbon's own `TaggedSeries.parse` builds
   a `dict`. Not folded into a `Value::Array` like `statsd_in`; this keeps decision 4's
   array→last-element rule from ever firing inside the pair.
10. **Non-finite values rejected both directions.** Carbon drops NaN on receipt itself.
    `graphite_in`: `logit.input.metrics.skipped{reason="non_finite_value"}`; `graphite_out`:
    `{reason="unencodable_value"}`.
11. **`.sum` is emitted for sketches under `expand`** — `DdSketch::sum()` (`metric.rs:340`) is
    exact. (Prometheus's "a sketch has no sum" doc claim at `prometheus/mod.rs:81` is stale; note
    it as a follow-up in W4b's closeout, don't touch prometheus in this effort.)
12. **Default `transport: tcp`** for both kinds — carbon's own default listener is TCP 2003.
13. **Resource attributes become tags on egress** (same as `influxdb_out`/`statsd_out`). A bare
    `graphite_in` resource is empty so the pair stays a fixed point; cross-protocol it's a
    `docs/known-gaps.md` row, not a normalization.
14. **No path truncation.** Carbon has no length bound; whisper's 255-byte filesystem component
    limit is a known-gaps row. `/` and `\` in a path are substituted (whisper directory separators).
15. **Sanitization counter** reuses the existing family: `logit.output.metrics.normalized
    {reason="path_sanitized"|"tag_sanitized"}`, once per record per reason. No new counter family.
16. **No shared `logit_inputs::tcp` driver yet.** There is nothing to extract from (`syslog_in` is
    UDP-only, `otlp_in`/`logit_in` own bespoke loops). Write it in `logit-inputs/src/graphite/tcp.rs`;
    the ADR names the extraction trigger (a second line listener, e.g. `syslog_in` TCP).

## Design

### Codec: `crates/logit-proto/src/graphite/`

`pub mod graphite;` in `crates/logit-proto/src/lib.rs`. Module doc IS the mapping table + the
numbered normalization list (identical numbering to the ADR — the collectd lesson).

```
mod.rs     doc = mapping tables + normalizations; consts DEFAULT_PLAINTEXT_PORT=2003,
           DEFAULT_PICKLE_PORT=2004, DEFAULT_MAX_PACKET_BYTES=1432, DEFAULT_MAX_LINE_BYTES=8192,
           DEFAULT_MAX_FRAME_BYTES=1<<20 (carbon's Int32StringReceiver.MAX_LENGTH),
           MAX_PICKLE_DEPTH=16, MAX_PICKLE_ITEMS=500_000;
           pub enum Protocol { Plaintext, Pickle }, Tags { Carbon, Drop }, MultiValue { Skip, Expand };
           pub use decode::GraphiteDecoder; pub use encode::{GraphiteEncoder, EncodeStats};
decode.rs  GraphiteDecoder { protocol, resource: Arc<Resource>, diag, telemetry, pickle scratch }
           impl logit_proto::Decoder (plaintext: split on '\n', trim '\r'; pickle: one unframed payload)
encode.rs  GraphiteEncoder + EncodeStats; impl FramedEncoder { type Meta = usize; type Stats = EncodeStats }
pickle.rs  write_datapoints / read_datapoints, opcode allowlist, PickleError
```

**No `pub const ATTR_*` / no `graphite.*` namespace** — the wire carries four facts (path, tags,
number, second), each represented exactly once in the model with no lossy normalization, so
`lossless-transit` rule (b) has nothing to outrank. State in the ADR's Alternatives.

Codec holds its own `Telemetry`/`Diagnostics` and emits every counter itself (collectd's model,
`collectd/encode.rs:31-36`); `EncodeStats` is returned for tests/benches only.

#### Decode: wire → model

| Wire | Model | Counter / diag |
|---|---|---|
| one line / one pickle datapoint | one `Event`, one `MetricRecord` | — |
| `path` (before first `;`) | `name = intern(path)`, `Gauge(v)` | — |
| `;name=value` | event attribute `Value::Str`, zero-copy `Bytes` slice | — |
| repeated tag key | last wins | `logit.input.tags.normalized{reason="duplicate_key"}` |
| finite value (`3`, `-1.5`, `1e5`, pickle `"3.14"` string) | `Gauge(v)` | — |
| NaN / ±inf | line skipped | `logit.input.metrics.skipped{reason="non_finite_value"}` + diag |
| `timestamp == -1` | `received_at` | — |
| `timestamp > 0` (int or fractional) | `(ts * 1e9) as i64` | — |
| other `timestamp <= 0` | skipped | `{reason="bad_timestamp"}` + diag |
| not exactly 3 whitespace-separated fields; non-UTF-8; empty path | skipped | `{reason="bad_line"}` + diag |
| malformed tag (`;` without `=`, empty name or value) | whole line skipped (carbon raises too) | `{reason="bad_tag"}` + diag |
| empty / whitespace-only line | skipped, uncounted | — |
| line > `max_line_bytes` (TCP) | drain to next `\n`, next line still decodes | `{reason="oversize_line"}` + diag |
| pickle frame > `max_frame_bytes` | connection closed (no resync in a length-framed stream) | diag `oversize_frame` |
| disallowed opcode / depth / item cap | `CodecError::Malformed`, whole frame dropped | diag `bad_pickle` |
| pickle item not `(str, (num, num))` | that datapoint skipped, rest of frame decodes | `{reason="bad_shape"}` |
| `Resource` / `Scope` | shared default / `None` | — |

#### Encode: model → wire

Prefix `logit.output.metrics.skipped{reason=…}` unless noted; kind drops `{metric_kind=…}`.

| Model | Wire | Counter |
|---|---|---|
| event with N metrics | N datapoints in `MetricList` order | — |
| `name`, sanitized | the path, verbatim | — |
| `Sum{Delta\|Cumulative, mono\|!mono}` | bare value | — (normalization 12) |
| `Gauge(v)` finite | `path v ts` | — |
| NaN / ±inf | dropped | `{reason="unencodable_value"}` + diag |
| `NO_RECORDED_VALUE` | dropped | `{reason="no_recorded_value"}` + diag |
| `GaugeDelta` | dropped | `{metric_kind="gauge_delta"}` + diag `gauge_delta_unresolved` |
| `Samples`/`Distribution`/`Histogram`/`ExponentialHistogram`/`Summary`/`Set`/`SetMembers`, `multi_value: skip` | dropped, one exhaustive arm each | `{metric_kind="samples"\|"distribution"\|"histogram"\|"exponential_histogram"\|"summary"\|"set"\|"set_members"}` |
| same, `multi_value: expand` | sub-path table below | `logit.output.metrics.degraded{metric_kind=…}` once per record |
| `floor(ts/1e9) <= 0` | dropped | `{reason="unencodable_timestamp"}` + diag |
| attrs, `tags: carbon` | `;name=value`, ascending rendered name | — |
| attrs, `tags: drop` | no tag segment | `logit.output.tags.dropped{reason="dialect"}` per tag |
| `Str/I64/U64/F64/Bool` | stringified | — |
| `Array` | last representable element | `logit.output.tags.normalized{reason="multi_value"}` per attr |
| `Null/Bytes/Timestamp/Map`, or empty-representable `Array` | tag dropped | `logit.output.tags.dropped{reason="unrepresentable"}` |
| forbidden byte in path / tag name / tag value | `_` | `logit.output.metrics.normalized{reason="path_sanitized"\|"tag_sanitized"}` per record |
| tag empty after sanitizing | tag dropped | `logit.output.tags.dropped{reason="empty"}` |
| two tags collide after rendering | keep the one whose original name sorts first | `logit.output.tags.dropped{reason="collision"}` |
| path empty after sanitizing | dropped | `{reason="empty_name"}` |
| plaintext line > `max_packet_bytes` (UDP) | dropped whole | `{reason="oversize_line"}` + diag |
| single pickle datapoint > `max_frame_bytes` | dropped whole | `{reason="oversize_datapoint"}` + diag |
| `unit`/`description`/`start_timestamp`/exemplars/`scope`/`schema_url`/dropped counts | dropped | none; known-gaps rows |
| event with no metrics | skipped | `EncodeStats::skipped_no_metrics`, no counter |

**Sanitization** (substitute `_`, never delete; collisions resolved on rendered names, never on
intern order — prometheus ADR `:220-224`):

| Field | Forbidden → `_` |
|---|---|
| path | whitespace (`char::is_whitespace`, matching carbon's `str.split()`), `char::is_control`, `;`, `/`, `\` |
| tag name | `;` `!` `^` `=`, whitespace, control |
| tag value | `;`, whitespace, control; a **leading** `~` only |

The path field forbids Unicode whitespace rather than ASCII-only because carbon's plaintext
receiver splits a decoded Python `str` with `str.split()`, which is Unicode-aware; sanitizing only
ASCII would let a U+00A0 survive encode and re-split into a spurious fourth field on decode,
breaking the pair's fixed point.

**`multi_value: expand` sub-paths** (every expanded kind adds ≥1 suffix, so it can never collide
with a skip-mode path):

| Kind | Sub-paths |
|---|---|
| `Samples` (via `sketch()`) / `Distribution` | `.count`, `.sum`, `.q0_5`, `.q0_75`, `.q0_9`, `.q0_95`, `.q0_99` |
| `Histogram` | `.count` (Σ buckets), `.sum`/`.min`/`.max` when `Some`, `.bucket_<b>` per bucket (own count, not cumulative — `metric.rs:209-218`) |
| `ExponentialHistogram` | `.count`, `.sum`/`.min`/`.max` when `Some`, `.zero_count`; **no buckets** |
| `Summary` | `.count`, `.sum`, `.q<q>` per its own quantiles |
| `Set` | `.count` = `estimate()` |
| `SetMembers` | `.count` = distinct member count |

Number tokens: format `f64` with `{}`, substitute `.` → `_` (`0.99 → q0_99`, `1.5 → bucket_1_5`,
`inf → bucket_inf`). This is injective (Rust `Display` emits only `-`, digits, ≤1 `.`), so it
satisfies `influxdb.rs:628-631`'s collision argument, which rejects *rounding*; Graphite forces the
substitution because `.` is the hierarchy separator. Say so in `mod.rs`.

#### `Protocol` and `Meta`

`type Meta = usize` (datapoints per message — collectd's `MessageBuf<usize>` reasoning).
Plaintext: one entry per line, meta 1. Pickle: one entry per complete length-prefixed frame (prefix
written by the encoder), meta = datapoint count; a new frame opens when the next datapoint would
exceed `max_frame_bytes`. All of `Protocol`/`Tags`/`MultiValue`/caps are encoder state via
`with_*` at construction. UDP+pickle is rejected by a graph rule, not the codec.

```rust
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EncodeStats {
    pub skipped_no_metrics, dropped_unsupported_kind, degraded_expanded_kind,
    pub dropped_unencodable_value, dropped_no_recorded_value, dropped_gauge_delta,
    pub dropped_unencodable_timestamp, dropped_empty_name, dropped_oversize_line,
    pub dropped_oversize_datapoint, tags_dropped_dialect, tags_dropped_unrepresentable,
    pub tags_dropped_empty, tags_dropped_collision, tags_normalized_multi_value,
    pub paths_sanitized, tags_sanitized, datapoints: usize,   // datapoints == Σ Meta
}
```

#### Pickle opcode subset

**Writer** (protocol 2, no memo): `PROTO`(0x80,2) `EMPTY_LIST`(0x5d) `MARK`(0x28); per datapoint
`BINUNICODE`(0x58, LE u32 len + UTF-8 — not `SHORT_BINSTRING`, which is `bytes` on py3),
`BININT`(0x4a) for i32-range timestamps else `LONG1`(0x8a), `BINFLOAT`(0x47, BE f64), `TUPLE2`(0x86)
×2; then `APPENDS`(0x65) `STOP`(0x2e).

**Reader accepts**: framing `PROTO` `FRAME`(0x95, length validated) `STOP`; memo `BINPUT` 0x71,
`LONG_BINPUT` 0x72, `MEMOIZE` 0x94, `BINGET` 0x68, `LONG_BINGET` 0x6a (bounded); containers
`MARK`, `EMPTY_LIST`, `LIST` 0x6c, `APPEND` 0x61, `APPENDS`, `EMPTY_TUPLE` 0x29, `TUPLE` 0x74,
`TUPLE1/2/3` 0x85-0x87; strings `BINUNICODE` 0x58, `SHORT_BINUNICODE` 0x8c, `BINUNICODE8` 0x8d,
`BINSTRING` 0x54, `SHORT_BINSTRING` 0x55, `BINBYTES` 0x42, `SHORT_BINBYTES` 0x43, `BINBYTES8` 0x8e
(UTF-8 validated); numbers `BININT` 0x4a, `BININT1` 0x4b, `BININT2` 0x4d, `LONG1` 0x8a, `LONG4`
0x8b (≤8-byte magnitude), `BINFLOAT` 0x47; inert `NONE` 0x4e, `NEWTRUE` 0x88, `NEWFALSE` 0x89
(a stray one = skipped datapoint, not a rejected frame).

**Rejected** with `CodecError::Malformed("pickle opcode 0x.. is not permitted")`: `GLOBAL`,
`STACK_GLOBAL`, `REDUCE`, `BUILD`, `INST`, `OBJ`, `NEWOBJ`, `NEWOBJ_EX`, `EXT1/2/4`, `PERSID`,
`BINPERSID`, `DUP`, `POP`, `POP_MARK`, all dict/set opcodes, `BYTEARRAY8`/`NEXT_BUFFER`/
`READONLY_BUFFER`, and every protocol-0 textual opcode. Bounds: `MAX_PICKLE_DEPTH`,
`MAX_PICKLE_ITEMS` on memo and top-level list, every length validated against remaining input
before allocating (`frame::read_frame` discipline, `crates/logit-proto/tests/robustness.rs`).
Stack and memo are struct fields cleared per frame (warm decode allocates only the caller's
`Vec<Event>`). `read_datapoints` yields `(path: &str, timestamp: f64, value: f64)`; numeric strings
parse via `str::parse::<f64>`; the stack must hold exactly one list at `STOP`.

A newly accepted opcode is an ADR-level change (`docs/adr/graphite-carbon-relay.md`'s "Pickle
opcode subset" section) — the pickle reader is reviewed as a security surface, not a routine codec
path.

### `graphite_in`: one component, `transport` × `protocol`

- `transport: udp` → `UdpListener<GraphiteDecoder>` (`crates/logit-inputs/src/udp.rs`), same shape
  as `CollectdInput` (`collectd.rs:88-120`): `ReceiveQueue`, batching, `SO_RCVBUF`, multicast
  auto-join, shutdown drain all free. `protocol: pickle` rejected here by graph rule.
- `transport: tcp` → new accept loop in `crates/logit-inputs/src/graphite/tcp.rs`. **No
  `ReceiveQueue`**: TCP's own flow control is the backpressure (ADR `decoupled-listener-io` exists
  for UDP's silent drops). Each connection owns a read buffer, a `GraphiteDecoder`, and a
  `logit_pipeline::BatchAccumulator`, sending into the shared `Fanout`. Model `Input::bind` on
  `otlp_in` (`otlp.rs:196-262`) and the connection cap on `logit_in` (`logit.rs:155-300`,
  `MAX_CONCURRENT_CONNECTIONS = 1024`, semaphore, per-connection shutdown racing).
  - Lines: read into `Vec<u8>`, hand `decode_into` the slice through the last `\n`, compact; over
    `max_line_bytes` with no `\n` → drain-to-newline state, counted once.
  - Length-prefixed: read 4-byte BE length, validate ≤ `max_frame_bytes` (else close), read
    payload, `decode_into`.

```rust
pub struct GraphiteInput { bind, transport, protocol, max_line_bytes, max_frame_bytes,
    udp: Option<UdpListener<GraphiteDecoder>>, listener: Option<TcpListener>,
    receive (batch assembly), diag, telemetry }
new(bind, transport, protocol) / with_diagnostics (propagates into decoder, like CollectdInput)
/ with_telemetry / with_receive(UdpListenerConfig) / with_max_line_bytes / with_max_frame_bytes
/ local_addr()
```

No shared `logit_inputs::tcp` driver is extracted yet — see decision 16 and the ADR's named
extraction trigger (a second line-oriented TCP listener, e.g. `syslog_in` gaining TCP;
`docs/known-gaps.md`'s "`syslog_in` is UDP-only" entry is the open row that trigger closes).

### `graphite_out`: `crates/logit-outputs/src/graphite.rs`

Modelled on `collectd.rs` (module-doc headings `## Config / ## Packing / ## Faults / ## Telemetry /
## Duplicate safety`, zero-I/O tests against `127.0.0.1:1`, `counted()`/`metric_sum()` helpers)
plus `statsd.rs:1638-2045` for the TCP half (copy + cite; no shared transport module exists).

```rust
enum Conn { Udp(UdpSocket), Tcp { stream: Option<TcpStream>, connect_timeout: Duration } }
pub struct GraphiteOutput { endpoint, conn, encoder: GraphiteEncoder, max_packet_bytes,
    buf: MessageBuf<usize>, packet_buf: Vec<u8>, diag, telemetry }
udp(endpoint) -> anyhow::Result<Self> (eager local bind) / tcp(endpoint, connect_timeout)
with_encoder (re-applies cap + diag + telemetry — collectd.rs:120-135, order-independent)
/ with_max_packet_bytes / with_diagnostics / with_telemetry
```

- UDP: `lookup_host` once per batch; lines `\n`-joined into datagrams ≤ `max_packet_bytes`, no
  trailing newline; `EMSGSIZE` counts that datagram's datapoints
  `logit.output.messages.dropped{reason="oversize_datagram"}` and continues; other send errors
  `Fault::Clean` if nothing sent yet else `Fault::Ambiguous`.
- TCP: lazy connect with `connect_timeout`; exactly one reconnect before any byte is written;
  every line `\n`-terminated including the last; partial write → `Fault::Ambiguous`; `flush()`
  flushes. Pickle frames: one `write_all` per already-prefixed frame.
- `duplicate_safe() -> true` with the whisper argument in the doc comment; the argument's boundary
  (a non-whisper backend could differ) is stated there too.
- Sink emits only socket facts: `logit.output.batch.bytes`, `logit.output.request.duration`,
  `logit.output.requests{class="ok"|"error"}`, `logit.output.messages` (entries),
  `logit.output.datapoints` (Σ Meta), `logit.output.datagrams` (UDP), the oversize_datagram drop.

### Configuration (`crates/logit-config/src/lib.rs`)

Two `ComponentKind` variants after `CollectdIn`/`CollectdOut`; four **own** enums
(`GraphiteTransport { Tcp (default), Udp }`, `GraphiteProtocol { Plaintext (default), Pickle }`,
`GraphiteTags { Carbon (default), Drop }`, `GraphiteMultiValue { Skip (default), Expand }`) — never
reuse `StatsdTransport` (schemars `$defs` naming, `:1580-1585`). Defaults as free fns near `:1437`.

```rust
GraphiteIn { bind: String, transport, protocol,
    max_line_bytes: u64 (human_bytes, "8192", plaintext+tcp),
    max_frame_bytes: u64 (human_bytes, "1MiB", pickle) },
GraphiteOut { endpoint: String, transport, protocol, tags, multi_value,
    max_packet_bytes: u64 (human_bytes, "1432", udp),
    max_frame_bytes: u64 (human_bytes, "1MiB", pickle),
    connect_timeout: Duration (humantime, 5s, tcp) },
```

`buffer:`/`receive:` are sibling fields on `Component` — nothing per-kind.

### Graph rules (`crates/logit-pipeline/src/graph.rs`)

- `role` (`:233`), `kind_name` (`:255`), `is_implemented` (`:302`): both kinds.
- `is_datagram_listener` (`:1667`) gains `GraphiteIn { transport: Udp, .. }`; new sibling
  `is_stream_listener` for `GraphiteIn { transport: Tcp, .. }`, folded into rules 17/18 beside
  `is_tail_listener`: batch-assembly and `shutdown_grace` fields allowed, queue-bounding fields
  (`max_datagrams`, `max_bytes`, `overflow`, `receive_buffer_bytes`) rejected by name.
- Rule 38 (`:1422-1452`) gains `GraphiteOut { max_packet_bytes: 0, .. }`; no collectd-style range
  clamp.
- **New rule (next free number in the module doc's list)**: `protocol: pickle` requires
  `transport: tcp` on both kinds; `max_line_bytes`/`max_frame_bytes`/`connect_timeout` of 0
  rejected; `max_frame_bytes` bounded `1024..=16 MiB`.
- `docs/design/pipeline-graph.md:179` arity table + rule text.

### Registry (`crates/logit-cli/src/pipeline.rs`)

`GraphiteIn` arm after `CollectdIn` (`:335`), `GraphiteOut` after `CollectdOut` (`:728`); UDP binds
eagerly, TCP lazily (the `StatsdOut` split `:709-712`); converters `graphite_transport/_protocol/
_tags/_multi_value` beside `statsd_format` (`:1014`).

### Telemetry inventory

`graphite_in` (TCP additions; UDP inherits the `UdpListener` set): `logit.input.connections`
(gauge), `logit.input.connections.rejected{reason="limit"}`, `logit.input.lines`/`.line.bytes`
(plaintext TCP), `logit.input.frames`/`.frame.bytes` (pickle), `logit.input.metrics.skipped
{reason=bad_line|bad_tag|bad_timestamp|non_finite_value|oversize_line|bad_shape}`,
`logit.input.tags.normalized{reason="duplicate_key"}`. Diagnostics: `bad_line`, `bad_tag`,
`bad_timestamp`, `non_finite_value`, `oversize_line`, `oversize_frame`, `bad_pickle`,
`connection_error`, `bound`.

`graphite_out`: codec counters per the encode table + sink socket counters above.
`docs/design/internal-telemetry.md` gains both sections (after `collectd_out`, `:649-703`) and the
new names in the naming section (`:340-354`).

### Permitted normalizations (identical numbering in `graphite/mod.rs` and the ADR)

1. Re-framing (lines→datagrams/stream writes, datapoints→frames ≤ `max_frame_bytes`).
2. Operator-chosen dialect change: plaintext↔pickle.
3. Datapoint reordering within a batch.
4. Tag order canonicalized to ascending rendered name (carbon's `TaggedSeries.format` sorts too).
5. Repeated tag key → last occurrence at decode.
6. Timestamps floor to whole seconds on egress.
7. `-1` → receipt time on ingress, leaves as that absolute second.
8. Number formatting → shortest round-trip `f64` (`1.50`/`1.5e0`/`+1.5` → `1.5`; `3.0` → `3`).
9. Field whitespace → single space; `\r\n` → `\n`; trailing newline on TCP, none on UDP's last line.
10. Sanitizer substitutions / empty-tag drops / collision drops (all counted).
11. `tags: drop` drops the tag set (counted).
12. `Sum` temporality and monotonicity dropped — a normalization, not a skip.

## Workstreams

| # | PR | Files | Depends |
|---|---|---|---|
| W0 | **Docs** | `docs/adr/graphite-carbon-relay.md` + row atop `docs/adr/README.md`; `docs/plans/graphite-carbon-relay.md` + row atop `docs/plans/README.md`; `docs/adr/lossless-transit.md` amendment (sixth pair); `docs/design/telemetry-landscape.md` Graphite section + matrix cells; `docs/adr/framed-encoder.md:138` bullet pointed here | — |
| W1 | **Codec** | `crates/logit-proto/src/lib.rs`; `src/graphite/{mod,decode,encode,pickle}.rs`; `tests/graphite_fixed_point.rs`; `tests/robustness.rs` additions; `docs/design/data-model.md` (codec list + "no well-known attributes" paragraph); `docs/known-gaps.md` cross-protocol rows | W0 |
| W2 | **`graphite_in`** | `crates/logit-inputs/src/graphite/{mod,tcp}.rs`, `lib.rs`; config `GraphiteIn` + `GraphiteTransport`/`GraphiteProtocol` + defaults + tests; `graph.rs` role/kind_name/is_implemented/is_datagram_listener/is_stream_listener/new rule (input half)/rules 17-18 text + tests; CLI arm + converters + test; `script/schema`; bench fixtures (`graphite_decoder`, `graphite_pickle_decoder`, `graphite_datagram(lines)`, `graphite_pickle_frame(n)`) + `allocations.rs` decode rows + `docs/design/memory.md` §2; `pipeline-graph.md`; `internal-telemetry.md` `graphite_in`; `deploying.md` `### graphite_in` | W1 |
| W3 | **`graphite_out`** | `crates/logit-outputs/src/graphite.rs`, `lib.rs`; config `GraphiteOut` + `GraphiteTags`/`GraphiteMultiValue` + defaults + tests; `graph.rs` rule 38 + new rule (output half) + tests; CLI arm + converters + test; `script/schema`; bench fixtures (`graphite_encoder`, `graphite_batch(n)`, `graphite_distribution_batch(n)`) + `allocations.rs` encode rows + `memory.md` §3; `internal-telemetry.md` `graphite_out`; `deploying.md` `### graphite_out` (model: `collectd_out` at `:403`) | W1 (‖ W2) |
| W4a | **Recorded interop** | `script/record-fixtures` `record_graphite()` + `all=`; `tools/record-fixtures/collectd-write-graphite.conf` (collectd `write_graphite` → plaintext TCP to `capture:2003`) and `python_graphite_pickle_producer.py` (stdlib `pickle` at protocol 2 and -1, length-prefixed to `capture:2004`); `testdata/interop/graphite/{README.md,*.raw}`; `testdata/interop/README.md` row; `interop_fixture_*` tests in `logit-inputs/src/graphite/mod.rs`; `docs/plans/recorded-interop-fixtures.md` follow-on list | W2 |
| W4b | **Round trip + closeout** | `crates/logit-cli/tests/graphite_round_trip.rs` + `tests/fixtures/graphite/*.in\|.expected`; `examples/graphite-relay.yaml`, `examples/statsd-to-graphite.yaml`; `docs/OVERVIEW.md` scope line; `AGENTS.md` current-state paragraph + lossless-pairs bullet; `deploying.md` cross-links; plan status paragraph; known-gaps follow-up note on prometheus `_sum` | W2, W3 |

**Landing order: W0 → W1 → (W2 ‖ W3) → (W4a ‖ W4b).** W2/W3 share only disjoint variants/arms in
`logit-config`, `graph.rs`, `pipeline.rs`; W3 branches from `w1`, not `w2`. Each PR is opened against
its parent workstream's branch (`feat/graphite-w0` from `origin/main`; `w1` from `w0`; `w2` and `w3`
both from `w1`; `w4a` and `w4b` both from `w2` merged with `w3`) and retargeted to `main` once its
parent merges, so later workstreams can proceed without waiting on every earlier PR to land — the
same convention `docs/plans/collectd-binary-relay.md`'s stacked PRs used.

**Status (2026-09-13): W0 in flight; W1–W4 not started.**

## Verification

Every PR: `script/check` and `script/cibuild` pass; `script/schema` regenerated + committed for
W2/W3 (staleness test `logit-config/src/lib.rs:2272`); `script/validate` passes the new examples
(W4b); `script/audit`/`deny.toml` unchanged everywhere. Allocation constants and `memory.md` rows
change together, never relaxed to inequalities; `type_sizes.rs` untouched.

**W0.** Links resolve; ADR headings match `TEMPLATE.md` exactly; both READMEs gain a row; ADR and
plan mapping tables agree cell for cell.

**W1.** `decode.rs` tests: plain line → one gauge event; tagged line → event attrs; repeated key
last-wins; `-1` → receipt time; fractional ts keeps ns; NaN/inf skipped; non-positive ts rejected;
2- and 4-field lines rejected; tabs/space runs separate; CRLF trimmed; empty line uncounted; tag
without `=`/empty name rejects line; non-UTF-8 rejected; many lines → many events; one shared
resource; tag value slices input; pickle frame → one event per datapoint; numeric-string value;
wrong-shape item skipped, rest decodes. `pickle.rs` tests with CPython byte literals carrying
provenance comments (exact `pickle.dumps` call): protocol 2 and 5 dumps decode; memoized repeated
path via `BINGET`; `LONG1` timestamp past 2038; `GLOBAL`/`STACK_GLOBAL`/`REDUCE`/`BUILD`/dict/
protocol-0 rejected; depth and item caps; truncation doesn't panic; writer emits only the ten
permitted opcodes; write→read round-trips; CPython dump and our writer decode identically.
`encode.rs` tests: gauge → one line; cumulative and delta sums bare; tags sorted; `tags: drop`
counts every tag; array → last element counted; unrepresentable dropped; forbidden path byte
substituted not deleted; tag-name forbidden set; tag-value `;` and leading-`~` only; collision keeps
first-by-original-name; NaN/flagged/GaugeDelta skipped with keys; every multi-value kind skipped
by default; table-driven `expand` over all seven kinds; quantile `.`→`_`; bucket rendering
injective (0.5 / 5 / 0.05 / -0.5 / +Inf distinct); non-positive ts skipped; floor incl. negative;
oversize line dropped whole; pickle packs frames under cap; pickle carries the same tagged path;
`encode_into` clears output. `tests/graphite_fixed_point.rs`: `decode(encode(b)) == b` and
`encode(decode(encode(b))) == encode(b)` for plain/tagged/integer/negative/large-ts/pickle/
plaintext→pickle/pickle→plaintext, tag order canonical after first hop, plus two proptests over
already-normalized generators (`[A-Za-z0-9_.-]{1,20}`, distinct keys, finite f64, ts in
`1..=2e9`, unsorted tag order). `tests/robustness.rs`: plaintext and pickle survive every
single-byte truncation and seeded bit flips; inflated `BINUNICODE` length rejected; no allocation
proportional to hostile length (`peak_live_bytes`); depth cap edge.

**W2.** Socket tests: UDP datagram → one batch; `bind` makes TCP port live before `run`, idempotent,
reports unbindable; two concurrent connections deliver; line split across writes reassembled;
oversize line skipped and next decodes; pickle frame split across writes reassembled; oversize
frame closes connection; connection past cap rejected+counted; closed connection doesn't take
listener down; `with_diagnostics` reaches decoder; shutdown drains within grace; UDP still batches
via receive queue. Config: defaults, snake_case enums, human-byte fields. Graph: listener +
implemented; `sources` rejected; pickle-on-UDP rejected; zero bounds rejected; `receive:` on UDP
accepted; queue-bounding field on TCP rejected by name; batch-assembly field on TCP accepted.
Registry: builds an input. Allocation rows (`memory.md` §2): decode 1 plaintext line = **1**
(caller's `Vec<Event>` only); 1 tagged line (2 tags) = **1**; warm `decode_into` = **0**; 25-line
datagram = 1 + reallocs; 100-datapoint pickle frame = 1 + reallocs. If measured higher, fix the
codec, not the assertion.

**W3.** Socket tests: line round-trips through a real collector + the real decoder (whole
`EventBatch` equality); TCP terminates every line; UDP no trailing newline; low cap packs several
datagrams none over cap; pickle send writes one prefixed frame a real reader accepts;
nothing-encodable does zero I/O; unresolvable endpoint → `Clean`; partial write → `Ambiguous`;
exactly one reconnect before any byte; nothing resent after a byte left; EMSGSIZE counted not
faulted; `duplicate_safe` true; encoder cap order-independent; diag/telemetry survive late
`with_encoder`; successful send reports batch.bytes/messages/datapoints/ok; skipped kind counted
through shared handle. Config/graph/registry tests analogous to W2 (sink + implemented, no
sources rejected, pickle-on-UDP, zero `max_packet_bytes`, `max_frame_bytes` range, TCP doesn't
bind eagerly). Allocation rows via `measure_framed` (`allocations.rs:2331`): 100 plaintext events
= **0**; 100 pickle = **0**; 100 Distribution `expand` = **0**; 100 Samples `expand` = N > 0
measured and pinned (inherent to `Samples::sketch()`, same as influxdb).

**W4a.** Producers through existing `raw_capture.py --proto tcp`: collectd `write_graphite`
(`Hostname "logit-fixture"`, `Interval 1`) and the stdlib-pickle Python producer at protocol 2 and
-1. Tests: `interop_fixture_write_graphite_plaintext_decodes`,
`interop_fixture_pickle_protocol_2_decodes`, `interop_fixture_pickle_protocol_5_decodes` (paths,
`Gauge`, host segment). READMEs record versions and capture commands.

**W4b.** `graphite_round_trip.rs` following `collectd_round_trip.rs:1-70` (module doc transcribes
the 12 normalizations; per-fixture table; `SAME_AS_INPUT`; test-side builder independent of the
codec; `every_committed_in_file_matches_its_builder` for pickle). Corpus: `plain-line` (none),
`tagged-line` (4), `integer-value`/`negative-value`/`exponent-value` (8), `many-lines` (1),
`crlf`/`extra-whitespace` (9), `duplicate-tag-key` (5), `fractional-timestamp` (6),
`sanitizer-path`/`sanitizer-tag` (10, wire-bytes only), `tags-drop` (11),
`pickle-protocol-2`/`pickle-protocol-5`/`plaintext-to-pickle` (2), `repacked-at-512` (1),
`minus-one-timestamp`/`nan-value`/`bad-line` (7/10, decode-only + diagnostic counters). Cross-
protocol: `statsd_in -> graphite_out` renders DogStatsD tags as carbon tags (repeated key → last,
counted); `statsd_in -> aggregate -> graphite_out` with `expand` yields the documented sub-paths.

**Manual smoke test (not automated; CI has no docker services):**

```
docker run -d --name graphite -p 2003:2003 -p 2004:2004 -p 8080:80 graphiteapp/graphite-statsd
```

1. `logit run examples/statsd-to-graphite.yaml`; `echo 'page.views:1|c' | nc -u -w0 127.0.0.1 8125`;
   `curl 'http://localhost:8080/render?target=page.views&from=-5min&format=json'`.
2. Same with `protocol: pickle`, `endpoint: 127.0.0.1:2004` — proves carbon's unpickler accepts
   our writer.
3. `multi_value: expand` with a timer: `page.latency.count/.sum/.q0_5…q0_99` render.
4. `tags: carbon`: `/tags/findSeries?expr=env=prod` returns the series; `tags: drop` shows the
   untagged path.
5. `examples/graphite-relay.yaml` in front of the container: render output identical to direct.

## Open risks

- **The pickle reader is the highest-risk surface in the repo**: a parser for a format whose
  purpose is arbitrary object construction, fed from a socket. Safety = the opcode allowlist +
  bounds + pre-validated lengths; `robustness.rs` and the rejection tests are the gate.
- **Real-sender pickle variance**: CPython picks `BININT1/2`/`BININT`/`LONG1` by magnitude and
  protocol; tested against protocol 2 and -1 captures only; protocol 0/1 rejected outright.
- **The TCP listener has no in-tree driver to copy**: connection lifetime, per-connection batch
  assembly, backpressure, shutdown drain are new; the socket-test list is sized accordingly.
- **No `graphite.*` namespace** means a rename transform silently changes the wire path (unlike
  collectd/syslog carriers). `deploying.md` says so plainly.
- **`tags: carbon` against pre-1.1 Graphite** writes `;` into whisper directory names, silently.
  `tags: drop` is the escape; document loudly.
- **`duplicate_safe() -> true`** rests on whisper's per-slot last-write-wins; a non-whisper backend
  could differ. Stated as the argument's boundary in the doc comment.
- Wire-chosen metric names grow the interner (accepted on `memory.md` §4's premise, as for
  `statsd_in`/`collectd_in`).
- Known follow-ups recorded, not built: shared `logit_inputs::tcp` driver (trigger: second line
  listener), prometheus `_sum` for sketches, whisper 255-byte path components.
