---
created: 2026-09-10
updated: 2026-09-10
---

# statsd/DogStatsD egress: dialect, transport, packing, and the v1 metric-kind deferral

## Status
Accepted

## Context

`statsd_in` (`crates/logit-inputs/src/statsd.rs`) has ingested statsd/DogStatsD metrics since
v0.1, but `logit` has had no way to emit them: there was no `statsd_out` `ComponentKind`. That's
the one asymmetry in the `_in`/`_out` naming convention every other protocol pair satisfies
(`docs/design/pipeline-graph.md`), and it blocks the obvious relay topology — a `logit` edge node
receiving statsd, aggregating, and forwarding to a central statsd/DogStatsD agent (a Datadog
agent, an Etsy-statsd instance, another `logit`).

Four questions had to be settled before implementation: which statsd dialect to emit, which
transport(s) to support, how a merged `DdSketch` (no longer holding the original samples) should
become wire lines, and whether `GaugeDelta` — statsd's own native relative-gauge syntax — should
be encoded natively or treated like every other sink treats it.

## Decision

### Dialect: configurable, default DogStatsD

`format: dogstatsd | statsd`, defaulting to **DogStatsD**. DogStatsD's tag extension
(`|#k:v,k:v`) is what carries an event's attributes across the wire at all; a plain-statsd
receiver that doesn't expect the segment can reject the whole line, so `format: statsd` omits it
entirely — not an empty `|#`, which is equally liable to be rejected — and counts every dropped
attribute (`logit.output.tags.dropped{reason="dialect"}`) rather than silently discarding data
with nothing to show for it.

### Transport: both UDP and TCP

`transport: udp | tcp`, defaulting to **UDP** — classic statsd and every DogStatsD client is UDP,
and it needs no ordering guarantee against the receiver's startup, the same reasoning
`docs/adr/syslog-output.md` gives for its own default. TCP is what makes `Fault` classification
meaningful for this sink, same as `syslog_out`: a connect failure is unambiguously `Fault::Clean`.
`send_udp`/`send_tcp` are near-verbatim ports of `syslog_out`'s, including its two TCP
correctness properties (cancellation-safe via `stream.take()`, and never resending once a byte has
left the host).

### Packing: UDP packs several lines into one datagram — the opposite of `syslog_out`

`max_packet_bytes` (default **1432**) bounds one **datagram**, into which `statsd_out` newline-
joins as many lines as fit before starting the next one. This is a deliberate divergence from
`syslog_out`, which refuses to pack, precisely because packing there would depend on the receiver
splitting on a delimiter its own injection-safety section exists to avoid relying on. The
reasoning inverts here: splitting on `\n` **is** the statsd grammar — every buffered statsd client
packs a send this way, and `StatsdDecoder::decode_into` splits an incoming datagram on `\n`
directly (`crates/logit-inputs/src/statsd.rs`) — and this sink's own sanitizers (below) make an
embedded `\n` unrepresentable in a name, key, or value, so a packed datagram cannot forge an extra
metric the way a packed syslog datagram could forge an extra log line. A line that would overflow
the cap starts a new datagram; a single line longer than the cap is **dropped whole**, never
truncated (`syslog_out` truncates) — a truncated statsd line decodes as a different metric or a
parse error, never a shorter version of the same one.

**1432, not 512 or 8192.** 512 is Etsy statsd's conservative internet-safe figure; 8192 is
DataDog's loopback/UDS figure (and `syslog_out`'s own `DEFAULT_MAX_MESSAGE_BYTES`, since Alloy's
receiver is local to the demo stack). 1432 is both Etsy statsd's own commodity-Ethernet-LAN
recommendation and DataDog's documented DogStatsD client default: 1500 MTU minus IPv4/UDP headers
minus headroom for VXLAN/IPsec encapsulation — exactly where a 1472-byte datagram would silently
fragment or `EMSGSIZE`. It doesn't assume the destination is loopback the way 8192 does.

TCP terminates **every** line with `\n`, including the last (no per-batch EOF on a stream, so
omitting it would glue the last line of one batch onto the next batch's first). No octet-counting
on TCP, unlike `syslog_out`: statsd has no such framing convention, and no receiver auto-detects
one the way Alloy's `go-syslog` does.

### Metric-kind coverage: `Counter`/`Gauge`/`GaugeDelta` only in v1

Only `Counter` (`|c`) and `Gauge`/`GaugeDelta` (`|g`) are encoded. `Distribution`, `Set`,
`Histogram`, and `Summary` are dropped with a clear "not implemented yet" diagnostic
(`unsupported_metric_kind`) — the same shape `influxdb_out`'s `render_fields` already uses for
`Set`.

**This means a `statsd_in -> aggregate -> statsd_out` relay drops every timer metric today.**
`ms`/`h`/`d` on the wire all decode to `MetricKind::Distribution`
(`logit_inputs::statsd::build_event`), so the single most common statsd workload — timers — makes
it through the input and the aggregator, then dies at this sink, loudly counted but dropped.
Deferred rather than guessed at: the aggregator's `DdSketch` no longer holds the original samples
it merged, so "how does a merged sketch become one or more statsd lines" is a real design
question — emit one line per fixed quantile (losing the ability to compute an arbitrary
downstream percentile)? Synthesize N samples at the sketch's own quantile boundaries (fabricating
a population that never existed)? — and deserves its own ADR once there's a concrete consumer to
design against, not a default picked in passing here.

### Relative gauges: opt-in native encoding, off by default

statsd is the one protocol that natively expresses a *relative* gauge adjustment (`+n|g`/`-n|g`)
— exactly `MetricKind::GaugeDelta`'s wire origin (`docs/adr/relative-gauge-adjustments.md`). By
default a `GaugeDelta` reaching this sink is dropped with the identical message `influxdb_out`
uses (`gauge_delta_unresolved`): it means the pipeline is missing an `aggregate` component, not
that the metric is malformed — and keeping that one invariant true across every sink by default
matters more than the convenience of a lossless relay. `relative_gauges: true` opts into encoding
it natively instead: `statsd_out` is the only sink that *can* round-trip a delta losslessly, since
every other sink's wire format has no equivalent concept at all, so it's a deliberate, named
exception rather than a silent default.

A positive delta needs an explicit `+`: plain float formatting of `5.0` yields `"5"`, which the
decoder reads back as an *absolute* `Gauge`, not a delta — silently corrupting the exact
round-trip this feature exists to preserve.

### Negative absolute gauges: the two-line `0|g` / `-n|g` idiom

The statsd/DogStatsD grammar has no wire syntax for setting a gauge to a negative absolute value
at all — `logit_inputs::statsd::build_event`'s `"g"` arm reads *any* leading `-` as a relative
delta, unconditionally. A naive `Gauge(-5.0)` would therefore render as `name:-5|g` and decode
back as `GaugeDelta(-5.0)`: a silent semantic corruption of a value type this sink otherwise
preserves exactly. `statsd_out` instead emits the idiom both Etsy statsd and DogStatsD document
for exactly this case: `name:0|g` immediately followed by `name:-5|g`. The two lines are pushed as
**one indivisible `MessageBuf` entry** (joined by an embedded `\n`) so the UDP packer can never
split them across two datagrams — a lost first datagram would otherwise apply `-5` to whatever
stale value the gauge already held at the receiver, rather than to the `0` this sink meant to
reset it to first. This is the only place an entry contains a newline; every sanitizer this sink
applies exists precisely to guarantee nothing else ever does.

`Gauge(-0.0)` is the one negative-signed value that is *not* sent as a pair: it is numerically
zero, and `0` is directly representable as an absolute gauge, so its sign is normalized away and it
renders as the plain `name:0|g`. Without that normalization `f64`'s `Display` would write
`name:-0|g`, which the decoder's leading-`-` dispatch reads as a delta — the exact corruption the
pair exists to prevent, for the one value the pair would be pointless for.

### Sanitization

A metric name has every one of `: | @ # , \n \r \0`, ASCII control characters, and whitespace
replaced with `_` (substitution, not deletion, so distinct names stay distinct — following
`syslog_out`'s `sanitize_5424_field` precedent). Each character is forbidden because of a specific
way `StatsdDecoder::parse_line` would otherwise misparse the result: `:` splits name from values,
`|` splits segments, `@`/`#` open the sample-rate/tag segments, `,` separates tags, `\n` separates
lines; whitespace is stripped because the decoder trims every line before parsing it.

Tag *keys* forbid the same set, **and additionally forbid `:`** for a different reason than the
name does: `parse_line` splits a tag on its *first* colon only, so a `:` inside a key would
silently reparse as a shorter key with the remainder folded into the value. Tag *values* forbid
the same set **except `:`, which is deliberately allowed**: since only the first colon is
significant, `env:a:b` round-trips as key `env`, value `a:b` — an asymmetry between key and value
sanitization that is easy to get backwards, so it carries its own dedicated test.

A `Value::Bool(true)` attribute renders as a bare tag (`#urgent`, no `:value`) — exactly what the
decoder produces for a valueless tag — rather than `key:true`, which would round-trip as
`Value::Str("true")`, silently changing the value's type.

### `duplicate_safe()` is `false`

Stronger than `syslog_out`'s reasoning: a redelivered `hits:5|c` doesn't just duplicate a record
the way a repeated log line would — it **increments the destination counter a second time**,
silently corrupting the value with no trace at the receiver. This yields the conservative
`AtMostOnce` default posture, under which `Fault::Clean` still retries, covering the common
receiver-restart outage with zero duplicate risk.

### No `logit_proto::Encoder`

Same reasoning as `syslog_out`: that trait returns one opaque `Bytes` per batch with no framing
metadata, which can't express the per-line/per-datagram boundaries this sink genuinely needs.
`StatsdEncoder::encode_into` is a bespoke, pure, directly-unit-testable method instead.

### No sample rate, no timestamp, no unit

Never `@<rate>`: `statsd_in` already extrapolated at decode time (a `Counter`'s value already has
the sample rate divided out; a `Distribution`'s samples are already replicated to the extrapolated
weight), so emitting `@1` would be a no-op at best and anything else would double-extrapolate
downstream. Never `|T<ts>`: the classic grammar has no timestamp segment at all, and
`logit_inputs::statsd::parse_line` would silently ignore one if emitted, so it wouldn't even
round-trip through this repo's own input — a receiver stamps with its own receipt time instead.
`MetricRecord::unit` has no statsd wire representation either and is dropped the same way. All
three are recorded in `docs/known-gaps.md`.

## Alternatives considered

- **One UDP datagram per line, mirroring `syslog_out`.** Rejected: it throws away the packing
  every real statsd client already does, for a safety property (no delimiter-splitting
  assumption) that doesn't apply here — this sink's sanitizers already make an embedded `\n`
  unrepresentable, so packing costs nothing that unpacking would have bought.
- **Octet-counted TCP framing, mirroring `syslog_out`.** Rejected: no statsd receiver auto-detects
  it, and there's no equivalent injection-safety property it would be defense-in-depth for —
  plain newline framing is what every statsd/DogStatsD TCP client and server already speaks.
- **A shared `DatagramTransport` enum with `syslog_out`.** Rejected even though the two variants
  are identical: `schemars` publishes a type's own name into the schema's `$defs`, so sharing one
  would make `statsd_out` document its transport by pointing at a syslog-named type. Precedent for
  coexisting near-duplicate config enums: `Compression` vs. `OtlpCompression`.
- **A `prefix`/`namespace` config field.** Rejected for v1: there is genuinely no way to rename or
  namespace a metric on egress anywhere in the pipeline today (`docs/design/lua-api.md` notes a
  metric's value/fields are unexposed to Lua), which is a real gap — but a sink-side prefix would
  pre-empt the general metric-rename transform that gap actually calls for, and coexisting with
  one awkwardly once it exists. Schema-additive later at zero compatibility cost.
- **`Distribution` support in v1, guessing at a `DdSketch` -> lines mapping.** Rejected: see the
  Decision section above — the design question deserves its own ADR against a concrete consumer,
  not a guess made in passing here.
- **The one-line multi-value idiom `name:0:-5|g` for negative gauges.** Atomic for free (statsd's
  own `:`-separated multi-value grammar decodes it to exactly the same `Gauge(0.0)` +
  `GaugeDelta(-5.0)` pair), but it's a DogStatsD-only extension that Etsy statsd rejects — it
  couldn't be used under `format: statsd` without a second code path, where the two-line idiom
  works identically under both dialects.
- **`|T<ts>` timestamps on egress.** Rejected: the classic grammar has none, and `statsd_in` would
  silently ignore one, so it wouldn't even round-trip through this repo's own input.

## Consequences

- `crates/logit-outputs/src/msgbuf.rs` (new): `MessageBuf` lifted out of `syslog.rs` (now needed
  by both sinks); `syslog.rs` re-exports it under its original path for source compatibility.
- `crates/logit-outputs/src/attrs.rs` (new): the resource⊕event attribute merge-join lifted out of
  `influxdb_out::render_tag_suffix` (also now needed by both sinks); `influxdb.rs` switches to it.
- `crates/logit-outputs/src/statsd.rs` (new): `StatsdEncoder` + `StatsdOutput`, per this ADR.
- `crates/logit-config/src/lib.rs`: new `ComponentKind::StatsdOut`, `StatsdTransport`,
  `StatsdFormat`; `schema/logit.schema.json` regenerated.
- `crates/logit-pipeline/src/graph.rs`: `role`/`kind_name`/`is_implemented` all gain the variant;
  a new rule rejects `max_packet_bytes: 0` (an impossible bound, not a small one — the same shape
  as the existing `buffer.max_batches`/`max_bytes: 0` rule).
- `crates/logit-cli/src/pipeline.rs`: `build_spec`'s `StatsdOut` arm, and the sole place a
  `logit_config::StatsdFormat`/`StatsdTransport` value crosses into `logit_outputs::statsd`'s own
  mirror types.
- `examples/statsd-relay.yaml` (new): a runnable relay config exercising the sink.
- `docs/known-gaps.md`: new entries for the v1 metric-kind deferral (and its timer-drop
  consequence for the `statsd_in -> aggregate -> statsd_out` path), no egress timestamp, no
  `unit`, no metric prefix/rename anywhere in the pipeline, and no TLS/DTLS.
