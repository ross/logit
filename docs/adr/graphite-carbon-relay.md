---
created: 2026-09-13
updated: 2026-09-14
---

# Graphite/Carbon relay: untyped datapoints as `Gauge`, tags as attributes, a restricted pickle codec, and a multi-value switch

## Status
Accepted

## Context

[`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md) surveys Graphite (its
"Graphite" section and the metrics matrix's `Graphite` column) but nothing is built against it, and
[ADR `framed-encoder`](framed-encoder.md)'s Consequences names "a Graphite/Carbon line sink" as the
anticipated fourth framed sink. Ross asked for a Graphite/Carbon sink; on questioning he widened it
to the full pair, so `graphite_in -> graphite_out` becomes the sixth fixed point under
[ADR `lossless-transit`](lossless-transit.md), alongside `statsd`, `otlp`, `syslog`, `prometheus`,
and `collectd` — with both Carbon wire protocols (plaintext and pickle), a config switch for
multi-value metric kinds (skip by default, expand opt-in), and tags on by default.

Like collectd, this needs **no core-model change**: a Graphite datapoint is one untyped number at
one second (`MetricKind::Gauge(f64)` + `Event::timestamp`), and tags are string attributes. **No
new crate dependency**: the pickle writer and restricted reader are hand-rolled, so `deny.toml` and
`script/audit` stay unchanged, and `crates/logit-core/tests/type_sizes.rs` is untouched by this
pair.

Decisions settled with Ross (2026-09-13), recorded in full in
[`docs/plans/graphite-carbon-relay.md`](../plans/graphite-carbon-relay.md): both directions ship as
one lossless pair; both wire protocols (plaintext and the pickle batch protocol) are supported, the
pickle reader accepting only a bounded opcode subset; multi-value metric kinds are a `graphite_out`
switch (`skip`/`expand`), not a fixed behavior; tags default on (`carbon`) with a `drop` escape
hatch; and there is no `graphite.*` carrier-attribute namespace and no prefix/template field — the
wire path *is* `MetricRecord.name`.

## Decision

### Transport: `transport` × `protocol`

`graphite_in` and `graphite_out` each take a `transport` (`tcp`, default; `udp`) and a `protocol`
(`plaintext`, default; `pickle`). Plaintext is `path[;k=v...] value timestamp\n`, carried over
either TCP or UDP at Carbon's default port 2003. Pickle is the batch protocol — a 4-byte
**big-endian** length prefix (Twisted's `Int32StringReceiver`) framing a pickled
`[(path, (timestamp, value)), ...]` — carried over **TCP only**, Carbon's default port 2004;
`protocol: pickle` with `transport: udp` is rejected by a graph rule, not the codec, since Carbon
itself never offers pickle over UDP. Packets/frames are capped by `max_line_bytes` (plaintext,
default 8192), `max_packet_bytes` (plaintext UDP, default 1432), and `max_frame_bytes` (pickle,
default 1 MiB — Carbon's own `Int32StringReceiver.MAX_LENGTH`).

The pickle writer emits a hand-rolled protocol-2 subset (no crate dependency — see Alternatives);
the reader is a **restricted** decoder accepting only what real senders emit
(`pickle.dumps(..., protocol=2)` and `protocol=-1`), rejecting every opcode that could construct an
arbitrary Python object. See "Pickle opcode subset" below.

### Model mapping

**Decode (wire → model).** One line or one pickle datapoint decodes to one `Event` carrying one
`MetricRecord`.

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

**Encode (model → wire)** is the inverse, plus rules for model kinds Carbon's wire can't carry
natively. Prefix `logit.output.metrics.skipped{reason=…}` unless noted; kind drops use
`{metric_kind=…}`.

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

### Sanitization

Substitute `_`, never delete; collisions resolved on rendered names, never on intern order —
[ADR `prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md)'s equivalent rule:

| Field | Forbidden → `_` |
|---|---|
| path | whitespace (`char::is_whitespace`, matching carbon's `str.split()`), `char::is_control`, `;`, `/`, `\` |
| tag name | `;` `!` `^` `=`, whitespace, control |
| tag value | `;`, whitespace, control; a **leading** `~` only |

`/` and `\` are substituted in paths specifically because Whisper stores a path's dot-separated
segments as filesystem directory components — the same reasoning `collectd_out`'s
`escape_slashes`-derived sanitization already applies to identity fields, applied here to the one
field Graphite's own wire format treats the same way. The path field forbids Unicode whitespace
rather than ASCII-only because carbon's plaintext receiver splits a decoded Python `str` with
`str.split()`, which is Unicode-aware; sanitizing only ASCII would let a U+00A0 survive encode and
re-split into a spurious fourth field on decode, breaking the pair's fixed point.

### `multi_value: expand` sub-paths

A multi-datapoint metric kind (`Samples`, `Distribution`, `Histogram`, `ExponentialHistogram`,
`Summary`, `Set`, `SetMembers`) has no native Carbon representation — one path carries exactly one
value per line. `graphite_out`'s `multi_value` switch decides what happens: `skip` (default) drops
the whole record, counted per kind; `expand` renders it as several dotted sub-paths, counted once
per record as a degradation (not a skip) regardless of how many sub-paths result. Every expanded
kind adds **at least one** suffix, so an expanded path can never collide with a `skip`-mode path.

| Kind | Sub-paths |
|---|---|
| `Samples` (via `sketch()`) / `Distribution` | `.count`, `.sum`, `.q0_5`, `.q0_75`, `.q0_9`, `.q0_95`, `.q0_99` |
| `Histogram` | `.count` (Σ buckets), `.sum`/`.min`/`.max` when `Some`, `.bucket_<b>` per bucket (own count, not cumulative — `metric.rs:209-218`) |
| `ExponentialHistogram` | `.count`, `.sum`/`.min`/`.max` when `Some`, `.zero_count`; **no buckets** |
| `Summary` | `.count`, `.sum`, `.q<q>` per its own quantiles |
| `Set` | `.count` = `estimate()` |
| `SetMembers` | `.count` = distinct member count |

`Samples` goes through `logit_core::Samples::sketch()` (`crates/logit-core/src/metric.rs:192`);
quantiles are `crate::otlp::metrics::DISTRIBUTION_QUANTILES`
(`crates/logit-proto/src/otlp/metrics.rs:102`, already `pub(crate)`, reachable from
`logit-proto/src/graphite/` exactly as `prometheus/mod.rs:143` already reaches it).
`DdSketch::sum()` (`metric.rs:340`) is exact, so `.sum` is emitted for sketches under `expand` —
Prometheus's own "a sketch has no sum" doc claim (`prometheus/mod.rs:81`) is stale and is left alone
here; noted as a follow-up in this plan's W4b closeout, not fixed by this effort. influxdb's
`[0.5,0.9,0.99]` quantile set is left alone; Graphite's own `expand` uses the same
`DISTRIBUTION_QUANTILES` set Prometheus uses, not influxdb's.

Number tokens format `f64` with `{}` and substitute `.` → `_` (`0.99 → q0_99`, `1.5 → bucket_1_5`,
`inf → bucket_inf`). This is **injective**: Rust's `Display` for `f64` emits only `-`, digits, and
at most one `.`, so no two distinct finite values can format to the same string before the
substitution, and the substitution itself (one character for one character, no merging) can't
introduce a new collision. This is a materially different argument from
`crates/logit-outputs/src/influxdb.rs:628-631`'s: that comment rejects *rounding* a quantile to a
fixed-width label (`0.991` and `0.994` both round to `p99` and collide), a lossy transform this
codec never performs. Graphite forces the substitution not to avoid a collision like influxdb's,
but because `.` is the wire's own hierarchy separator — the number itself is carried in full,
merely re-punctuated. `crates/logit-proto/src/graphite/mod.rs`'s module doc says so.

### `Protocol` and `Meta`

`type Meta = usize` — datapoints per message, the same reasoning `collectd`'s `MessageBuf<usize>`
already established (`docs/adr/collectd-binary-relay.md`'s `FramedEncoder` amendment): a sink needs
to attribute an oversize/`EMSGSIZE` drop to the right number of metrics, and a plain count is
exactly what both wire shapes need. Plaintext: one entry per line, meta 1. Pickle: one entry per
complete length-prefixed frame (the length prefix is written by the encoder), meta = that frame's
datapoint count; a new frame opens when the next datapoint would exceed `max_frame_bytes`.
`Protocol`/`Tags`/`MultiValue`/the size caps are all encoder state applied via `with_*` builders at
construction, not per-call arguments — UDP+pickle is rejected by a graph rule, not the codec.

### Pickle opcode subset

Pickle's own purpose is arbitrary Python object construction, which makes an unrestricted reader a
remote-code-construction primitive fed straight from a socket. `graphite_in`'s pickle reader instead
accepts a **fixed allowlist** of opcodes — exactly what CPython's own `pickle.dumps(obj, protocol=2)`
(and `protocol=-1`, which resolves to the highest available protocol) emits for a list of
`(str, (number, number))` tuples — and rejects everything else outright.

**Writer** (protocol 2, no memo): `PROTO`(0x80,2) `EMPTY_LIST`(0x5d) `MARK`(0x28); per datapoint
`BINUNICODE`(0x58, LE u32 len + UTF-8 — not `SHORT_BINSTRING`, which is `bytes` on py3),
`BININT`(0x4a) for i32-range timestamps else `LONG1`(0x8a), `BINFLOAT`(0x47, BE f64), `TUPLE2`(0x86)
×2; then `APPENDS`(0x65) `STOP`(0x2e).

**Reader accepts**: framing `PROTO` `FRAME`(0x95, length validated) `STOP`; memo `BINPUT` 0x71,
`LONG_BINPUT` 0x72, `MEMOIZE` 0x94, `BINGET` 0x68, `LONG_BINGET` 0x6a (bounded); containers `MARK`,
`EMPTY_LIST`, `LIST` 0x6c, `APPEND` 0x61, `APPENDS`, `EMPTY_TUPLE` 0x29, `TUPLE` 0x74,
`TUPLE1/2/3` 0x85-0x87; strings `BINUNICODE` 0x58, `SHORT_BINUNICODE` 0x8c, `BINUNICODE8` 0x8d,
`BINSTRING` 0x54, `SHORT_BINSTRING` 0x55, `BINBYTES` 0x42, `SHORT_BINBYTES` 0x43, `BINBYTES8` 0x8e
(UTF-8 validated); numbers `BININT` 0x4a, `BININT1` 0x4b, `BININT2` 0x4d, `LONG1` 0x8a, `LONG4`
0x8b (≤8-byte magnitude), `BINFLOAT` 0x47; inert `NONE` 0x4e, `NEWTRUE` 0x88, `NEWFALSE` 0x89
(a stray one is a skipped datapoint, not a rejected frame).

**Rejected** with `CodecError::Malformed("pickle opcode 0x.. is not permitted")`: `GLOBAL`,
`STACK_GLOBAL`, `REDUCE`, `BUILD`, `INST`, `OBJ`, `NEWOBJ`, `NEWOBJ_EX`, `EXT1/2/4`, `PERSID`,
`BINPERSID`, `DUP`, `POP`, `POP_MARK`, every dict/set opcode, `BYTEARRAY8`/`NEXT_BUFFER`/
`READONLY_BUFFER`, and every protocol-0 textual opcode. `GLOBAL`/`STACK_GLOBAL`/`REDUCE`/`BUILD` are
specifically the opcodes that let a pickle stream construct and invoke an arbitrary callable — the
reason this format is unsafe to parse unrestricted at all. Bounds: `MAX_PICKLE_DEPTH` (16),
`MAX_PICKLE_ITEMS` (500,000) on the memo and the top-level list, and every length-prefixed value
(string, bytes, frame) validated against remaining input **before** allocating, the same discipline
`crates/logit-proto/tests/robustness.rs` already holds every other codec to.

**A newly accepted opcode is an ADR-level change.** The allowlist above is not a starting point to
extend as real-world producers turn up edge cases; it is the complete set this codec ever accepts.
Widening it — even to a seemingly-inert opcode — is reviewed as a security decision against this
ADR, not folded into a routine codec PR, because the allowlist's safety property is that it is
closed, not merely generous.

### `graphite_in`'s TCP listener: no `ReceiveQueue`, and no shared driver yet

`transport: udp` reuses the shared `UdpListener<GraphiteDecoder>` exactly as `collectd_in`/
`statsd_in`/`syslog_in` do, `ReceiveQueue` and all —
[ADR `decoupled-listener-io`](decoupled-listener-io.md) exists precisely because a UDP socket's own
receive buffer cannot exert backpressure on a sender: once full, the kernel drops the datagram
silently and uncounted, so `logit` interposes a bounded, counted queue between `recv_from` and a
possibly-slow `Fanout::send`.

`transport: tcp` has no such gap to paper over. TCP's own flow control **is** the backpressure: a
slow `sink.send(batch).await` blocking the per-connection read loop stalls that connection's socket
buffer, which the sending client feels directly as its own write blocking — the same argument
`otlp_in`'s module doc already makes ("This is the first listener with real backpressure to its
source... TCP... has no such escape hatch," `crates/logit-inputs/src/otlp.rs:18-24`) and the same
shape `logit_in`'s connection loop relies on. So `graphite_in`'s TCP half carries **no
`ReceiveQueue`**: nothing between a connection's read buffer and its own `BatchAccumulator`/
`Fanout::send` needs bounding beyond the connection cap already established by `logit_in`
(`MAX_CONCURRENT_CONNECTIONS = 1024`, a semaphore, per-connection shutdown racing —
`crates/logit-inputs/src/logit.rs:155-300`), which `graphite_in`'s TCP accept loop follows.

No shared `logit_inputs::tcp` driver is extracted for this pair. There is exactly one existing TCP
accept loop to generalize from — `otlp_in`'s, itself specialized to HTTP/1.1 and gRPC/h2c framing —
plus `logit_in`'s own bespoke connection cap and shutdown-racing logic; neither is a plain
line-oriented listener, so `graphite_in::tcp` would be the first of its kind, and generalizing a
driver from a single occurrence risks guessing the wrong seams. The trigger for extracting one is
named, not left implicit: `docs/known-gaps.md`'s open **"`syslog_in` is UDP-only"** entry ("a TCP
accept loop would buy the driving integration nothing... [s]tays additive-later on the *input* side
specifically") is exactly the second line-oriented TCP listener that would justify pulling
`graphite_in`'s accept loop, line-buffering, and oversize-line handling into a shared
`logit_inputs::tcp` module. Until that entry closes, `crates/logit-inputs/src/graphite/tcp.rs` is
written as its own thing, modeled on `otlp_in`'s `Input::bind` shape and `logit_in`'s connection cap
but sharing no code with either.

### Duplicate safety

`graphite_out.duplicate_safe() -> true`: Whisper (Carbon's on-disk storage) is last-write-wins per
`(path, second)` — re-sending the same batch on retry overwrites the same slots with the same
values, not a second, distinct data point. This is
[`crates/logit-outputs/src/influxdb.rs:191-199`](../../crates/logit-outputs/src/influxdb.rs)'s
argument (an identical `(measurement, tag set, timestamp)` write is an idempotent overwrite, not a
duplicate point) applied to Whisper instead of InfluxDB's own idempotent-write semantics, and makes
`graphite_out` the **first non-HTTP sink with a real destination** to report `duplicate_safe: true`
(`null_out` reports it trivially, having no destination). The argument's boundary, stated plainly
because it does not generalize automatically: it rests on Whisper's specific storage semantics, and
a non-Whisper Carbon-protocol backend (a relay that batches/derives rather than storing per-slot
values) could behave differently. An operator pointing `graphite_out` at something other than a
Whisper-backed Carbon receiver should verify that backend's own duplicate handling before relying on
this.

### Default `transport: tcp`

Both `graphite_in` and `graphite_out` default to `transport: tcp` — Carbon's own default listener
(`carbon-cache`'s line receiver) is TCP on port 2003, and TCP is the transport pickle requires
regardless, so defaulting the plaintext half to match keeps the two `protocol` choices consistent
under one `transport` default rather than switching transport defaults per protocol.

### Configuration

Two new `ComponentKind` variants, `GraphiteIn`/`GraphiteOut`, alongside `CollectdIn`/`CollectdOut`.
Four **own** enums — never reused from an existing type, see Alternatives:
`GraphiteTransport { Tcp (default), Udp }`, `GraphiteProtocol { Plaintext (default), Pickle }`,
`GraphiteTags { Carbon (default), Drop }`, `GraphiteMultiValue { Skip (default), Expand }`.

```
GraphiteIn { bind: String, transport, protocol,
    max_line_bytes: u64 (human_bytes, "8192", plaintext+tcp),
    max_frame_bytes: u64 (human_bytes, "1MiB", pickle) },
GraphiteOut { endpoint: String, transport, protocol, tags, multi_value,
    max_packet_bytes: u64 (human_bytes, "1432", udp),
    max_frame_bytes: u64 (human_bytes, "1MiB", pickle),
    connect_timeout: Duration (humantime, 5s, tcp) },
```

`buffer:`/`receive:` are sibling fields on `Component`, not per-kind fields, matching every other
input/output pair.

### Permitted normalizations

Per [ADR `lossless-transit`](lossless-transit.md), the following count as normalization, not loss,
for `graphite_in -> graphite_out`, identically numbered in `crates/logit-proto/src/graphite/mod.rs`'s
module doc — the collectd lesson (its own ADR's amendment needed a second numbering reconciled by
hand after the fact; this pair starts with one list, not two):

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

## Alternatives considered

- **A `graphite.*` carrier attribute namespace**, mirroring `collectd.*`/`syslog.*`. Rejected: the
  wire carries exactly four facts (path, tags, number, second), and each is represented exactly
  once in the model with no lossy normalization standing between the raw wire spelling and the
  normalized field — there is no genuine raw/normalized split the way collectd's five-tuple
  identity or syslog's severity have, and no other sink needs a normalized value this pair would
  otherwise have to fall back to. `lossless-transit` rule (b) exists to let a raw encoding outrank a
  normalized field on that protocol's own egress; with nothing here that a normalization has
  touched, there is nothing for a carrier attribute to outrank.
- **A `prefix`/template path-override field**, letting `graphite_out` rewrite the outgoing path.
  Rejected: renaming a metric is a transform's job (`lua`, per
  [ADR `routing-by-condition-is-lua`](routing-by-condition-is-lua.md)'s precedent for anything
  shaped like a rewrite), not a sink's; the path *is* `MetricRecord.name`, and giving the sink its
  own renaming field would create two ways to do the same thing.
- **Always-expand or always-drop for multi-value metric kinds**, rather than an operator switch.
  Rejected: both are defensible defaults for different deployments (a dashboard built against
  Carbon's flat namespace wants `.count`/`.q0_99` sub-paths; a pure relay wants to skip and let a
  downstream OTLP/Prometheus sink keep the sketch intact) and neither dominates, so it became a
  named `multi_value: skip | expand` switch instead of a unilateral choice.
- **A pickle crate dependency** (e.g. `serde-pickle`), rather than a hand-rolled writer and
  restricted reader. Rejected: this pair needs to write exactly one shape (a list of
  `(str, (number, number))` tuples) and read a narrow, security-bounded allowlist of opcodes — both
  far short of general pickle support — and a general-purpose pickle crate would both pull in
  support for the unsafe opcodes this ADR specifically rejects and add a new dependency this
  project's zero-new-dependency budget for the effort doesn't have room for (`deny.toml`/
  `script/audit` stay unchanged).
- **Reusing `StatsdTransport`** for `GraphiteTransport` (or `GraphiteProtocol` off an existing
  dialect enum). Rejected: schemars publishes a type's own name into the generated schema's
  `$defs`, so sharing `StatsdTransport` would make `graphite_in`/`graphite_out` document their
  transport by pointing at a statsd-named schema type — the same reasoning
  `crates/logit-config/src/lib.rs`'s `StatsdTransport` doc comment already gives for why it isn't
  shared with `SyslogTransport`.
- **A shared `logit_inputs::tcp` driver now**, generalizing `graphite_in::tcp`'s accept loop ahead
  of a second user. Deferred, not rejected: there is nothing to extract from yet (`otlp_in`'s accept
  loop is HTTP/gRPC-specific; `logit_in`'s is bespoke to the native protocol's handshake and
  ack-driven backpressure), and guessing the right shared abstraction from a single occurrence risks
  building the wrong one. See "`graphite_in`'s TCP listener" above for the named trigger.

## Consequences

- `graphite_in`/`graphite_out` become the **sixth** like-protocol pair under
  [ADR `lossless-transit`](lossless-transit.md), alongside `statsd`, `otlp`, `syslog`, `prometheus`,
  and `collectd` — see [`docs/plans/graphite-carbon-relay.md`](../plans/graphite-carbon-relay.md)'s
  Workstreams table for the files each workstream (codec, `graphite_in`, `graphite_out`, recorded
  interop, round-trip closeout) touches.
- Known-gaps rows this commits to, added alongside the codec's own workstream: `unit`/`description`/
  `start_timestamp`/exemplars/`scope`/`schema_url` dropped on encode with no wire form to carry
  them; Whisper's 255-byte filesystem path-component limit is not enforced (no path truncation is
  done — Carbon's own wire format has no length bound); resource attributes becoming tags is a
  cross-protocol behavior, not a normalization this pair's fixed point relies on (a bare `graphite_in`
  resource is always empty); Prometheus's stale "a sketch has no sum" doc claim
  (`prometheus/mod.rs:81`), left uncorrected by this effort and noted as a follow-up in this plan's
  W4b closeout; no shared `logit_inputs::tcp` driver yet, with the extraction trigger named above.
- **The pickle reader is a new, meaningful security surface**: a parser for a format whose purpose
  is arbitrary object construction, fed straight from a socket. Its safety rests entirely on the
  fixed opcode allowlist, bounded depth/memo/item counts, and every length validated against
  remaining input before allocating — not on best-effort hardening. A newly accepted opcode is
  therefore an ADR-level change (see "Pickle opcode subset" above), and the codec's own robustness
  tests (truncation, bit-flips, inflated lengths) are the gate this ADR treats as load-bearing, not
  incidental.
- No new crate dependency: `deny.toml` and `script/audit` output are expected unchanged.
- `crates/logit-core/tests/type_sizes.rs` is untouched by this pair: nothing in the core model
  changes shape to support it.

## Amendment: the shared-driver trigger fired; `graphite_in` TCP is now `logit_inputs::tcp` (2026-09-14)

"No shared `logit_inputs::tcp` driver is extracted for this pair" above named its own trigger: a
second line-oriented TCP listener. `syslog_in` over TCP became one
([ADR `syslog-tcp-ingress-and-tls`](syslog-tcp-ingress-and-tls.md)) and, in building it, extracted
exactly the driver that section anticipated — accept loop, connection cap, gauge and rejection
counter, TLS termination, per-connection decoder clone and batch assembly, a first-byte deadline.
So `crates/logit-inputs/src/graphite/tcp.rs` is deleted and `graphite_in` runs on
`crates/logit-inputs/src/tcp.rs`, as `enum Inner { Udp, Tcp }` over the two shared drivers, exactly
the shape `SyslogInput` already had.

**Framing became an explicit per-listener choice** rather than the driver's one built-in guess. The
driver used to sniff each connection's first byte for an RFC 6587 octet count, which is right for
syslog (a non-transparent message always starts `<`) and wrong here: a carbon path may legitimately
begin with a digit, and the sniff would reframe the whole connection on it. `FramingMode` is now
set through `TcpListener::with_framing`, and `graphite_in` picks from `protocol:`:

| `protocol:` | mode | bound | over the bound |
|---|---|---|---|
| `plaintext` | `Lines { oversize: DrainToNextLine }` | `max_line_bytes` | that line is dropped and counted; the framer resynchronizes at the next `\n` and the connection stays up |
| `pickle` | `LengthPrefixed` (Twisted's `Int32StringReceiver`) | `max_frame_bytes` | the connection is closed — a length-framed stream has no resync point |

Both bounds keep their operator-facing meanings and defaults; the driver's own `MAX_FRAME_BYTES`
remains what a listener that never calls `with_framing` gets. `Oversize::DrainToNextLine` is this
ADR's original recoverable-oversize behaviour, moved into the driver as a mode rather than
reimplemented — it is a non-fatal `FrameError` the frame loop continues past.

`FramingMode::Lines` also decides what a **terminator-less remainder at EOF** means, and it is not
what the driver did for syslog: it is `FrameError::Truncated`, counted
`logit.input.frames.dropped{reason="truncated"}` and dropped, rather than emitted as a final
message. Carbon's `\n` is the only completeness signal a line carries, so a sender that dies
mid-line has truncated, not finished — and the failure is silent if you get it wrong, since
`svc.web01.cpu 42.5 17000` with its newline missing still parses as three fields and yields a
well-formed gauge stamped 1970-01-01. This preserves carbon parity (its own receiver discards such
a tail, as did the accept loop deleted here, which only ever decoded through the last `\n`) and
makes a clean FIN agree with an abrupt RST, which `report_buffered_tail` already counted
`truncated`. A whitespace-only remainder is still dropped uncounted. `FramingMode::Rfc6587Auto`
keeps emitting, because RFC 6587 §3.4.2 says a final syslog message needs no terminator.

**What `graphite_in` gains.** `tls:` and `handshake_timeout:`, both the driver's and both TCP-only
(graph rules 43 and 45 now cover this listener). The second closes the known gap this listener
carried: a peer that connected and sent nothing held one of its 1024 permits indefinitely. Neither
is an idle timeout; the post-first-byte silence gap is still the accepted one every TCP listener
has. There is no `graphite_out` TLS half — carbon's own senders speak none, so the listener side is
for a `logit`-to-`logit` or stunnel-shaped relay hop.

**What it costs.** The bespoke loop handed the decoder everything through the read buffer's last
`\n` in one `decode_into` call; the driver frames first, so plaintext is now **one call per line**,
with its `Arc<Resource>` clone, two telemetry counts and `BatchAccumulator::absorb` per line rather
than per read. The decoded events are identical (`decode_plaintext` splits on `\n` internally
either way). This is carbon's hottest path, so the cost is recorded rather than assumed: a
`FramingMode::LineChunk` — emit through the last `LF` as one frame, which both line decoders
already split internally — is the reserve fix if it ever stops being acceptable, and is
deliberately not built speculatively.

**Telemetry follows the driver's vocabulary**, one per driver rather than one per listener:
`logit.input.lines`/`.line.bytes` become `logit.input.frames`/`.frame.bytes`, and
`logit.input.metrics.skipped{reason="oversize_line"}` becomes
`logit.input.frames.dropped{reason="oversize"}` — which now covers the pickle case too, where the
old listener had a diagnostic (`oversize_frame`) and no counter at all. The `oversize_line`/
`oversize_frame` diagnostic keys are gone; both are `framing_error`. `frame.bytes` counts the
payload the decoder was handed, so a pickle frame no longer includes its own 4-byte length prefix.
Pre-release, so no compatibility shim: `docs/design/internal-telemetry.md` is the record.

**What did not survive the port.** The old loop needed its own teardown signal, because it drained
a `JoinSet` of connection tasks *after* `accept` failed and an idle client would otherwise park
that drain forever; its regression test drove a real `accept` failure through
`libc::shutdown(SHUT_RD)`. The shared driver returns an accept error straight out of its loop with
nothing to park on, so both the signal and the test are gone rather than ported.
