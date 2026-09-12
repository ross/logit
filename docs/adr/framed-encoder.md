---
created: 2026-09-12
updated: 2026-09-12
---

# `FramedEncoder`: a third codec trait for sinks that need per-message framing, over a shared `MessageBuf`

## Status
Accepted

## Context

`docs/design/data-model.md`'s "Codecs" section, `logit_proto::Encoder`'s own doc comment, and
`AGENTS.md`'s protocol convention all said the same thing: every output implements
`logit_proto::Encoder` (`fn encode(&mut self, &EventBatch) -> Result<Bytes, CodecError>`). By the
end of [ADR `lossless-transit`](lossless-transit.md)'s workstreams that was false in four
different ways, and the tree had four encoder shapes rather than one:

1. **`Encoder`**, one opaque `Bytes` per batch: the native format (`logit_proto::native`),
   InfluxDB line protocol (`influxdb_out`, one HTTP body), and `EventDump`/`StreamEncoder`
   (`stdio_out`/`file_out`, [ADR `rotating-file-output`](rotating-file-output.md)).
   `StreamOutput<E: Encoder>` is the one generic consumer.
2. **`SignalEncoder`**, one `Bytes` per non-empty `Signal`: OTLP, whose logs, metrics, and traces
   are three RPCs. Added as a *sibling* trait rather than by generalizing `Encoder` -- the tree's
   existing precedent for "a second shape gets a second trait".
3. **A bespoke `encode_into(&batch, &mut MessageBuf) -> EncodeStats`** on `SyslogEncoder` and
   `StatsdEncoder` ([ADR `syslog-output`](syslog-output.md), [ADR `statsd-output`](statsd-output.md),
   each with its own "No `logit_proto::Encoder`" section). Both sinks need per-*message*
   boundaries -- one datagram or one octet-counted frame per message for syslog, one line per
   metric packed into datagrams up to a size cap for statsd -- which one opaque `Bytes` per batch
   can't carry without reinventing the boundaries on the far side. Both never fail: every
   per-message problem is a counted drop in a sink-specific `EncodeStats`, not an error. Statsd's
   method took the datagram cap as a third, per-call argument, so the two signatures weren't even
   identical. `MessageBuf` (one contiguous `Vec<u8>` plus one range per message, reused across
   calls so a batch allocates once, not once per message) lived in `logit-outputs`'s private
   `msgbuf` module, re-exported through `logit_outputs::syslog::MessageBuf` because
   `crates/logit-bench` named it by that path.
4. **`prometheus_out`**, a stateful registry rendered on scrape -- no per-batch encode at all
   ([ADR `prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md)).

`docs/known-gaps.md` carried (3) as an open entry: "two sinks now independently need this shape,
which is exactly the signal that was being waited for", with the generalization "still deferred,
but no longer for lack of a second caller to design against". The concrete trigger to stop
deferring was a **third** framed codec: the collectd codec (PR #137, `crates/logit-proto/src/
collectd/encode.rs`) needs the identical buffer -- and, because it lives in `logit-proto`, which
cannot depend on `logit-outputs`, it was re-copying `MessageBuf` as its own `Packets` type,
with a per-datagram value-list count bolted on. A third copy of the same buffer being written
while the gap entry said "not yet done" is the point at which docs-only stops being honest.

## Decision

**A third codec trait, `logit_proto::FramedEncoder`, over a shared, generic
`logit_proto::MessageBuf<M>`.** `Encoder` and `SignalEncoder` are untouched.

```rust
pub trait FramedEncoder {
    type Meta;
    type Stats: Default + std::fmt::Debug + PartialEq;
    fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf<Self::Meta>) -> Self::Stats;
}
```

- **`MessageBuf` moves to `logit_proto::msgbuf`** (re-exported at the crate root) and gains a
  `Vec<M>` pushed in lock-step with its ranges: `push_with(&[u8], M)`, `iter_with() ->
  (&[u8], &M)`. `M = ()` is the default and costs nothing (a `Vec<()>` never allocates), so
  syslog's and statsd's `MessageBuf` is byte-for-byte the two-`Vec` buffer it was; the existing
  `push(&str)`/`push_bytes(&[u8])` are kept on `MessageBuf<()>`. The "allocate once per batch,
  never free between calls" contract is unchanged and still load-bearing for the allocation rows.
  `logit-outputs` has no `msgbuf` module and no `syslog::MessageBuf` re-export any more --
  pre-release, no compat shims.
- **`Stats` is per sink.** A syslog drop reason (`dropped_oversize_header`,
  `dropped_invalid_sd`) is not a statsd drop reason (`dropped_gauge_delta`,
  `dropped_dialect_events`, ...); the trait only requires the type be `Default + Debug +
  PartialEq`, and each sink's `send` keeps mapping its own struct onto `logit.output.*` counters
  by hand. No common core is split out.
- **Never fails.** The signature returns `Stats`, not `Result<Stats, CodecError>`: there is
  nothing a caller can do about one bad message beyond counting it, and a `Result` that is
  always `Ok` is a lie in the type.
- **Anything an encoder needs beyond the batch is encoder state, never a per-call argument.**
  Statsd's datagram cap becomes `StatsdEncoder::max_packet_bytes` (default `usize::MAX`,
  uncapped) with a `with_max_packet_bytes` builder mirroring `format`/`relative_gauges`.
  `StatsdOutput` decides the cap in effect for its transport -- the configured value on UDP,
  `usize::MAX` on TCP, exactly what `send` used to pass per call -- **at build time**, in
  `with_encoder`/`with_max_packet_bytes` (either builder order), so `send` never mutates the
  encoder per call. The send-time *packing* cap in `send_udp` is unchanged.
- **`Meta` is what the encoder needs to say about a message beyond its bytes.** `()` for syslog
  and statsd; a per-datagram value-list count for a collectd-style packer.

`SyslogEncoder` and `StatsdEncoder` implement the trait; their inherent `encode_into` methods are
gone (one way to call it). The one generic consumer is the allocation-row helper in
`crates/logit-bench/tests/allocations.rs`, which measures both sinks through the same
warm-then-measure call.

## Alternatives considered

- **An associated output type on `Encoder`** (`type Output; type Stats; fn encode(&mut self,
  &EventBatch, &mut Self::Output) -> Result<Self::Stats, CodecError>`). Rejected: it forces a
  `Result` onto codecs that never fail, disturbs `StreamOutput<E: Encoder>` and every blob
  codec for the benefit of two sinks that don't use that seam, and still leaves OTLP's
  per-signal labels needing a third instantiation. The tree already chose a sibling trait once
  (`SignalEncoder`) over this.
- **A push-style `FrameSink`** (`trait FrameSink { fn push(&mut self, &[u8]); }` with
  `MessageBuf` as one implementor, so a future streaming transport could push straight to a
  socket buffer). Rejected: it loses per-message meta, which is the one thing the third
  implementor needs, so collectd would still have to keep a second buffer type of its own; and
  nothing in the tree wants to stream past the buffer today -- every transport resolves its
  endpoint once per batch and needs the whole batch encoded before its first write.
- **Docs only** (rewrite `Encoder`'s doc comment and `AGENTS.md`'s convention to say there are
  three shapes; keep `MessageBuf`/`EncodeStats` as the documented pattern). Rejected: it would
  have been the honest closure a week ago, but a third copy of the buffer was being written at
  the moment of deciding. The doc fixes are done regardless, as part of this.
- **`MessageBuf` in `logit_core`** ("it's a plain buffer"). Rejected: it is a codec output shape
  -- nothing about it is meaningful without an encoder filling it -- and `logit_proto` is where
  the trait that names it lives.

## Consequences

- `crates/logit-proto/src/msgbuf.rs` (new, moved from `crates/logit-outputs/src/msgbuf.rs`):
  `MessageBuf<M = ()>`, with unit tests for push order, `iter_with` pairing, and `clear` keeping
  capacity.
- `crates/logit-proto/src/lib.rs`: `FramedEncoder`; the crate, `Decoder`, `Encoder`, and
  `SignalEncoder` doc comments now describe the three encoder shapes and which codecs use which,
  instead of "every output implements this".
- `crates/logit-outputs/src/syslog.rs`/`statsd.rs`: `impl FramedEncoder`; no inherent
  `encode_into`; statsd's cap is encoder state, pinned per transport by
  `the_encoder_line_cap_follows_the_transport_at_build_time`. Wire bytes, packing, truncation,
  `EncodeStats` fields, and telemetry counter names are all unchanged.
- `crates/logit-bench`: one `measure_framed` helper over `E: FramedEncoder<Meta = ()>` drives
  the `syslog_out` row (still 100) and a new `statsd_out: encode_into 100 events` row (0 --
  `docs/design/memory.md`), plus a `statsd` arm in `benches/pipeline.rs`'s `encode` group.
- **The collectd codec should replace its `Packets` copy with `MessageBuf<usize>` and implement
  `FramedEncoder` when its W3 lands** -- a follow-up on that branch, not part of this change.
- `prometheus_out` remains the one sink outside all three traits, by design (its ADR's "No
  `logit_proto::Encoder`" section).
- A fourth framed sink (a Graphite/Carbon line sink, a per-record Kafka/NATS producer) gets the
  buffer, the trait, and the bench harness for free; what it still writes by hand is its own
  `Stats` and the `send`-side telemetry mapping, which this ADR deliberately leaves per sink.
- [ADR `syslog-output`](syslog-output.md), [ADR `statsd-output`](statsd-output.md), and
  [ADR `prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md) each carry an
  amendment pointing here; `docs/known-gaps.md`'s entry and `docs/plans/lossless-transit.md`'s
  residual item are closed.
