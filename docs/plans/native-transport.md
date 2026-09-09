---
created: 2026-09-09
updated: 2026-09-09
---

# Enabling plan: `logit_out`/`logit_in` — the native transport

## Context

`docs/OVERVIEW.md`'s headline bet is that "a `logit` talking to a `logit` is a first-class,
efficient path." The *format* half exists: `logit_proto::native` (`crates/logit-proto/src/frame.rs`
+ `src/native/`) is a tested `Encoder`/`Decoder`, decided by
[ADR `native-wire-format-encoding`](../adr/native-wire-format-encoding.md). The *transport* half
does not: `ComponentKind::LogitIn { bind }` / `LogitOut { endpoint }`
(`crates/logit-config/src/lib.rs`) are declared, published in `schema/logit.schema.json`, and
rejected by `graph.rs`'s `is_implemented` (rule 8) — the last two declared-but-unrunnable kinds
(`docs/known-gaps.md`, "Native wire protocol: the format is done, the transport isn't").

**Scope.** A `logit_out` sink that opens one TCP (optionally TLS) connection to a `logit_in`
listener, performs a version/codec/compression handshake, sends one native frame per batch, and
waits for a per-batch acknowledgement before committing the batch out of its `SinkQueue`. A
`logit_in` listener that accepts many such connections, decodes each frame into an `EventBatch`,
forwards it via `Fanout::send`, and acknowledges only after that send returns. Single-in-flight
per connection in this plan; the handshake carries a `window` field so credit-based flow control
(several unacked frames in flight, cumulative acks) is a later, additive change. This plan ends
with `docs/known-gaps.md`'s schema-drift entry closed and a two-process forwarding example.

## What established implementations do

Read before designing, matching the field rather than inventing: Vector's native `vector` sink /
source (length-prefixed protobuf frames over TCP, TLS optional, per-batch ack via a gRPC
response), Fluent Bit's `forward` protocol (MessagePack over TCP, optional `chunk`/`ack` fields —
ack is opt-in and per-chunk), and OTLP/gRPC (request/response per batch). Every one of them:
authenticates/negotiates once per connection, then streams; acknowledges per batch (or opts in
to); treats an ack timeout as "unknown outcome," not failure. `logit` matches that shape.

## Decisions already settled

| Question | Decision |
|---|---|
| Framing | The existing 24-byte `frame.rs` header, unchanged. **`flags` bit 0 = control frame** (handshake/ack); bit clear = data frame carrying a native-v1 payload. `flags`/`reserved` are documented in `wire-protocol.md` as "spare room for a flag a later version needs" — this is that use |
| Control payload | Hand-rolled TLV via `native::varint`, in `crates/logit-proto/src/native/control.rs`: `Hello { version: u16, codecs: Vec<u8>, compressions: Vec<u8>, max_frame_bytes: u32, window: u32 }`, `HelloAck { version, codec, compression, max_frame_bytes, window }`, `Ack { seq: u64 }`, `Reject { code: u16, message: String }`. Unknown TLV tags skipped, same forward-compat rule as `native/record.rs` |
| Sequence numbers | **Implicit.** TCP is ordered; each data frame on a connection is seq `n+1`; `Ack.seq` is the cumulative count of data frames the receiver has forwarded. No seq field in the data frame, so the v1 payload is untouched and a future window > 1 uses cumulative acks unchanged |
| Ack point | Receiver acks **after `Fanout::send` returns**, i.e. after the batch is in every downstream inbox. A stalled downstream delays the ack, which stalls the sender's `write_loop` — that *is* the backpressure, so `logit_in` needs no `ReceiveQueue`/`BatchAccumulator` and graph rule 17 keeps rejecting `receive:` on it |
| In-flight | One frame per connection; the sender's `SinkQueue` `peek`/`commit` is the retransmit state. `window` is negotiated and recorded but the sender uses 1. Credit > 1 is out of scope and needs `logit-pipeline` queue changes (`known-gaps.md`, "No out-of-order/credit-based acknowledgement") |
| Handshake | Client sends `Hello` first; server replies `HelloAck` (chosen codec/compression = intersection, its own `max_frame_bytes`, `window`) or `Reject` and closes. Version mismatch / no common codec → `Reject` → `Fault::Permanent` on the sink |
| Compression | `logit_out.compression: none \| lz4` (reuse `logit_config::Compression`; `to_native_compression` in `crates/logit-cli/src/pipeline.rs` is the existing crossing). Negotiated down to `none` if the server doesn't offer it |
| TLS | Symmetric `Option<...>`: `logit_out.tls: Option<TlsClientConfig>` and `logit_in.tls: Option<TlsServerConfig>` — presence turns TLS on (the `otlp_in` precedent; `otlp_out`'s scheme-selection doesn't apply since `endpoint` is a bare `host:port`, like `syslog_out`). Server name = the endpoint's host. `rustls` `ring` provider only, `tokio-rustls` `TlsConnector`/`TlsAcceptor`, no ALPN |
| TLS helper reuse | Extract `build_rustls_server_config` (private in `crates/logit-inputs/src/otlp.rs`) into `crates/logit-inputs/src/tls.rs` and `build_rustls_client_config`/`AcceptAnyServerCert` (private in `crates/logit-outputs/src/otlp.rs`) into `crates/logit-outputs/src/tls.rs`; `otlp_*` call the shared functions. Behaviour-preserving refactor, its own commit |
| Frame size bound | Make `frame::MAX_SANE_UNCOMPRESSED_LEN` `pub`; the sink checks the encoded frame against `min(MAX_SANE_UNCOMPRESSED_LEN, peer.max_frame_bytes)` and fails that batch `Fault::Permanent` (never retryable, counted `batches.dropped{reason="send_failed"}`, diagnosed `frame_too_large`) rather than letting the peer reject it after the bytes are sent |
| Stream reading | Add `pub fn FrameHeader::read(&mut Bytes)` (currently private) so a socket reader can `read_exact` 24 bytes, parse, then `read_exact(compressed_len)`, then hand one whole frame to `read_frame`. No streaming API inside `frame.rs`; it stays sync and tokio-free |
| Connection lifecycle (sink) | Lazy connect inside `send` (the `syslog_out` `Conn::Tcp` precedent: a not-yet-up peer must not block startup). Cancel-safety: `stream.take()` into a local before any write, exactly as `send_tcp` does, because `deliver_with_retry` races each attempt against a timeout and `write_all` is not cancel-safe |
| Fault classification | connect/handshake I/O failure → `Fault::Clean` (nothing sent); `Reject` → `Permanent`; any error *after the first byte of a data frame left* → `Ambiguous`; ack timeout (`request_timeout`, default 10s like `otlp_out`) → `Ambiguous`. `duplicate_safe() = false` (no receiver-side dedupe identity) |
| Sink telemetry | `logit.proto.frames{direction="out",codec,compression}`, `logit.proto.frame.bytes`, `logit.output.ack.duration` (timing), `logit.output.reconnects` (count), `logit.output.requests{class=...}` for parity with `otlp_out` |
| Listener telemetry | `logit.proto.frames{direction="in",...}`, `logit.proto.frame.bytes`, `logit.proto.errors{reason="magic"\|"version"\|"crc"\|"truncated"\|"too_large"\|"codec"}` (names pre-committed in `known-gaps.md`), `logit.input.connections` (gauge), `logit.input.connections.rejected{reason="limit"}` |
| Connection limit | `MAX_CONCURRENT_CONNECTIONS = 1024`, same semaphore-after-accept shape as `otlp_in` (permit acquired after `accept`, held for the connection's life; TLS handshake inside the spawned task) |
| Decoder robustness gate | Before `logit_in` listens on a network, `crates/logit-proto` gains seeded mutation tests (bit flips, truncation at every offset, length-field inflation, nested-depth abuse) over `read_frame`, `decode_batch`, and `control::decode`, asserting "returns `Err`, never panics, never allocates from a length field before bounding it." `cargo-fuzz` needs nightly and is recorded as a `known-gaps.md` follow-up, not built here |
| Where things live | Control messages + header API: `logit-proto` (sync, no tokio). Listener: `crates/logit-inputs/src/logit.rs`. Sink: `crates/logit-outputs/src/logit.rs`. Config crossing: `crates/logit-cli/src/pipeline.rs` only |

## The constraint everything is designed around

`Input::run(&mut self, sink: Fanout)` takes its `Fanout` by value and cancel-by-drop is the only
shutdown mechanism ([ADR `service-lifecycle-and-output-retry`](../adr/service-lifecycle-and-output-retry.md),
`docs/plans/decoupled-listener-io.md`'s "constraint" section). Every per-connection task in
`logit_in` holds a `Fanout` clone (as `otlp_in`'s do), so **a spawned connection task must not
outlive the accept loop's drop**: connection tasks are spawned, but each `select!`s its read loop
against a `watch::Receiver<bool>` cloned from `run_until_shutdown`, and `logit_in` overrides
`run_until_shutdown` to (a) stop accepting, (b) send `Reject{code=GOING_AWAY}` on idle connections
and let in-flight frames finish within `shutdown_grace`, (c) return. Test this explicitly: the
graph must close after shutdown with an open, idle client connection.

On the sink side, the constraint is `write_loop`'s contract: a sink implements **one attempt**;
retry, budget, and backoff are `write_loop`'s, and every attempt is raced against
`tokio::time::timeout`. Hence the take-the-stream-before-writing rule above.

## Reference architecture

```
logit_out (sink, one tokio task via run_output)
  SinkQueue ──peek──▶ encode (NativeEncoder) ──▶ [connect+Hello/HelloAck if needed]
             ──▶ write frame ──▶ await Ack{seq==sent} (timeout) ──commit──▶ next
                                                   │
                                            TCP / TLS (rustls)
                                                   ▼
logit_in (listener) accept loop ──spawn per conn──▶ read Hello → HelloAck
                                                   loop { read header, read body,
                                                          read_frame, decode_into,
                                                          Fanout::send(batch).await,
                                                          write Ack{seq} }
```

## Workstream dependency graph

```
A (proto: control msgs, header API, robustness tests) ──┐
B (tls.rs extraction, both crates)                      ──┼── C (logit_in) ──┐
                                                          └── D (logit_out) ──┼── E (config/graph/build_spec) ── F (round-trip tests, allocations, docs, example)
```

A and B are independent. C and D each need A and B and are independent of each other. E needs
C and D. F needs E. Suggested landing: A, B as two small PRs (B is a pure refactor); C+D+E as
one PR; F as a follow-up PR (or folded in if small).

## A. `logit-proto`: control messages, public header read, robustness tests

**Goal:** everything the transport needs from the codec crate, still sync and tokio-free.

- `crates/logit-proto/src/frame.rs`: `pub const FLAG_CONTROL: u16 = 1 << 0`; `write_frame` gains
  a `flags` parameter (or a `write_frame_with_flags` sibling — keep `write_frame`'s signature for
  existing callers); `read_frame` returns the header's `flags` alongside `(codec, payload)` (a new
  `read_frame_with_header -> (FrameHeader, Bytes)`; keep `read_frame` as a thin wrapper);
  `FrameHeader::read` and `MAX_SANE_UNCOMPRESSED_LEN` become `pub`. Add
  `CodecError::Truncated { needed: usize }` distinct from `Malformed` so a stream reader can tell
  "need more bytes" from "corrupt" (also what the disk-buffer plan needs).
- `crates/logit-proto/src/native/control.rs`: `Hello`/`HelloAck`/`Ack`/`Reject` with
  `encode(&self) -> Bytes` / `decode(&mut Bytes) -> Result<Self, CodecError>`, TLV over
  `varint`, skip-unknown, bounded string lengths (`Reject.message` ≤ 1 KiB), bounded list
  lengths (≤ 16 codecs/compressions). `pub const PROTOCOL_VERSION: u16 = 1`. `Reject` codes:
  `VERSION_MISMATCH`, `NO_COMMON_CODEC`, `FRAME_TOO_LARGE`, `GOING_AWAY`, `INTERNAL`.
- Robustness tests (`crates/logit-proto/tests/robustness.rs`): seeded `SmallRng`-free LCG (no
  new dep), for each of `read_frame`, `decode_batch`, `control::*::decode`: every
  single-byte truncation of a valid frame; 10k random bit-flips; every length-bearing field
  inflated to `u32::MAX`; nesting depth of `Value::Map`/`List` past the decoder's bound. Assert
  no panic; assert `peak_live` bytes (reuse `logit-bench`'s `CountingAlloc` pattern or a simple
  `assert!(elapsed_alloc < X)`) stays below 2× the input size for any single decode.

**Test list:** control round-trips (each message, empty lists, max-length message); unknown TLV
tag skipped; `FLAG_CONTROL` round-trips through the header; `Truncated` returned for a header
short by 1 byte and for a body short by 1 byte, `Malformed` for a bad CRC; the mutation suites
above; existing `frame.rs`/`native` tests unchanged.

**Done:** `script/test` green; `crates/logit-bench/tests/allocations.rs`'s
`native_encode_one_event` (23) / `native_decode_one_event` (8) unchanged.

## B. Shared TLS builders

**Goal:** `logit_in`/`logit_out` reuse `otlp_in`/`otlp_out`'s rustls construction verbatim.

- `crates/logit-inputs/src/tls.rs`: `pub(crate) fn build_server_config(settings:
  &TlsServerSettings, base_dir: &Path, alpn: &[&[u8]]) -> anyhow::Result<rustls::ServerConfig>`;
  `TlsServerSettings` moves here (re-exported from `otlp` for the existing `pipeline.rs` path).
- `crates/logit-outputs/src/tls.rs`: `build_client_config(&TlsClientSettings, base_dir)` and
  `AcceptAnyServerCert`; `TlsClientSettings` moves here.
- `otlp.rs` in both crates call the shared functions. No behaviour change.

**Test list:** existing TLS tests in both crates and `crates/logit-cli/tests/otlp_round_trip.rs`
pass unmodified. **Done:** `script/cibuild` clean; diff of `otlp.rs` is deletions + two calls.

## C. `logit_in`

**Goal:** a listener that speaks the handshake, decodes frames, acks after forward, and shuts
down without orphaning `Fanout` clones.

- `crates/logit-inputs/src/logit.rs`: `LogitInput::new(bind)`, `with_diagnostics`,
  `with_telemetry`, `with_tls(&TlsServerSettings, base_dir)`, `with_max_frame_bytes` (default
  `MAX_SANE_UNCOMPRESSED_LEN`). Accept loop copied from `otlp_in` (semaphore after accept, TLS
  inside the task). Per connection: read one control frame, expect `Hello` (anything else, or >
  5s without it → close, `logit.proto.errors{reason="handshake"}`), reply `HelloAck`; then loop:
  `read_exact(24)` → `FrameHeader::read` → bound check → `read_exact(compressed_len)` →
  `read_frame_with_header` → if control: `Reject`/unexpected → close; else
  `NativeDecoder.decode_into(frame, now, &mut scratch)` → `Fanout::send(EventBatch{..}).await` →
  write `Ack{seq}`. Scratch `Vec<Event>` reused per connection (`clear()`, not `mem::take`).
  Generic over `S: AsyncRead + AsyncWrite + Unpin + Send` so plaintext and TLS share one body.
- `run_until_shutdown` override per the constraint section; idle detection = no data frame in
  flight when shutdown fires.

**Test list (`logit.rs` module, loopback sockets, `tokio::pin!` + `select!`, no spawn where a
borrow is needed):** handshake happy path returns the negotiated compression; a client offering
only `zstd` gets `Reject{NO_COMMON_CODEC}`; a wrong-version `Hello` gets `Reject{VERSION_MISMATCH}`;
a data frame before `Hello` closes the connection; a frame larger than `max_frame_bytes` is
rejected on the header alone without reading the body; an ack is written only after the
downstream inbox accepted the batch (capacity-1 channel, assert ack is delayed until the test
drains it); a CRC-corrupt frame closes the connection and increments
`logit.proto.errors{reason="crc"}`; the graph closes after shutdown with an idle client still
connected; the connection limit rejects the 1025th connection (use a small override for the
test). **Done:** zero new dependencies in `logit-inputs/Cargo.toml`.

## D. `logit_out`

**Goal:** a sink that delivers one batch per frame with a per-batch ack, honouring `write_loop`'s
one-attempt contract.

- `crates/logit-outputs/src/logit.rs`: `LogitOutput::new(endpoint)`, `with_compression`,
  `with_timeout` (ack + connect, default 10s), `with_tls(&TlsClientSettings, base_dir)`,
  `with_diagnostics`, `with_telemetry`. State: `stream: Option<Conn>` where `Conn` holds the
  (TLS or plain) stream plus negotiated `HelloAck` and `seq: u64`; `encoder: NativeEncoder`;
  reusable `BytesMut` for the header read.
- `send(&mut self, batch)`: encode → size check (`Permanent`) → `let conn = self.stream.take()`
  or connect+handshake (`Clean` on failure) → single `write` of the first bytes to learn whether
  anything left (`syslog_out::send_tcp`'s two-property rule: never resend once a byte left; a
  zero-byte failure reconnects once) → `write_all` rest → read ack frame with
  `tokio::time::timeout` → `Ack.seq == conn.seq + 1` → put `conn` back. Any failure after the
  first byte → drop `conn` (next `send` reconnects) and `Fault::Ambiguous`.
- `flush()`: flush the live stream. `duplicate_safe() = false`.
- Promote `tokio-rustls` from dev- to real dependency of `logit-outputs` (same version, already
  in `Cargo.lock`; note it in the Cargo.toml comment as the other deps do).

**Test list (`logit.rs` module, against an in-test minimal server or the real `LogitInput`
via a dev-dependency on `logit-inputs`, which already exists):** first `send` connects and
handshakes, second reuses the connection (`logit.output.reconnects` stays 0); ack timeout →
`Fault::Ambiguous`; peer closes mid-frame → `Ambiguous`, next `send` reconnects; connect refused →
`Clean`; `Reject{VERSION_MISMATCH}` → `Permanent` and `is_explicitly_permanent`; an oversized
batch → `Permanent` without touching the socket; lz4 negotiated down to none when the server
offers none; a cancelled `send` (dropped mid-await) leaves `stream == None`. **Done:** every
`Fault` arm covered by a test.

## E. Config, graph, `build_spec`, schema

- `crates/logit-config/src/lib.rs`: `LogitIn { bind, #[serde(default)] tls: Option<TlsServerConfig>,
  #[serde(default)] max_frame_bytes: Option<u64> (human_bytes::option) }`; `LogitOut { endpoint,
  #[serde(default)] compression: Compression, #[serde(default)] tls: Option<TlsClientConfig>,
  #[serde(default = "10s")] request_timeout: Duration }`. Docs on every field.
- `crates/logit-pipeline/src/graph.rs`: add both to `is_implemented`; retarget
  `unimplemented_kind_is_rejected` (no unimplemented kind remains — delete the test and rule 8's
  "declared, not runnable" carve-out text, keep rule 8 itself for future kinds); new rule 32:
  `logit_in`/`logit_out` TLS consistency mirroring rule 22 (`cert_file`/`key_file` paired,
  `insecure_skip_verify` + `ca_file` contradictory); `logit_in.max_frame_bytes` ≤ 64 MiB and > 0.
  `docs/design/pipeline-graph.md`'s rule list updated.
- `crates/logit-cli/src/pipeline.rs`: `LogitIn`/`LogitOut` arms modelled on the `OtlpIn`/`OtlpOut`
  arms; remove `LogitIn`/`LogitOut` from the `unreachable!` fallback's expectations.
- `script/schema`; `script/validate`.

**Test list:** config round-trips for every new field; graph rule 32 cases; a `build_spec` test
per kind asserting the constructed spec carries `tls`/`compression`. **Done:** schema diff is
exactly the new fields; `known-gaps.md`'s "schema advertises kinds the binary can't run" entry
can be deleted outright.

## F. End-to-end, allocations, docs, example

- `crates/logit-cli/tests/logit_round_trip.rs` in the shape of `otlp_round_trip.rs`: plaintext,
  lz4, server TLS, mutual TLS, wrong-CA rejection (`testdata/tls/other-ca.pem`), and **a
  forwarding chain** `statsd fixture → logit_out → logit_in → stdio` asserting timestamps and
  resource attributes survive (native ignores `received_at` by design).
- `crates/logit-bench/tests/allocations.rs`: extend the native section — `logit_out: encode +
  frame one batch` and `logit_in: read + decode one batch into a warm scratch` — and update
  `docs/design/memory.md` §2 in the same commit; rewrite that section's "no `ComponentKind`
  consumes this codec yet" sentence.
- `examples/forwarder-edge.yaml` + `examples/forwarder-central.yaml`: edge = `statsd_in →
  logit_out`; central = `logit_in → aggregate → stdio_out`. Both pass `script/validate`.
- Docs: ADR `native-transport-handshake-and-ack.md` (decisions table above, alternatives:
  gRPC-over-hyper reuse — rejected, the whole point of the native path is no per-request HTTP
  framing; explicit seq in the data frame — rejected, TCP ordering makes it redundant; window > 1
  now — deferred to keep `SinkQueue` unchanged); `docs/design/wire-protocol.md` "Connection
  protocol" rewritten from future tense to as-shipped, with the control TLV table;
  `docs/known-gaps.md`: delete the schema-drift entry, rewrite "the transport isn't" into what
  remains (credit window > 1, QUIC, OTLP passthrough codec, cargo-fuzz), amend "No end-to-end
  acknowledgement" (now one hop further); `docs/deploying.md` gains a "Forwarding between
  `logit` nodes" section (TLS, sizing `request_timeout` vs `retry_budget`, what to watch);
  `AGENTS.md` "Current state" paragraph; `docs/design/internal-telemetry.md` catalog rows.

## Verification, across the whole plan

- `script/cibuild` clean at every workstream boundary.
- `script/test` includes the new robustness suite (must run in < 10s; tune iteration counts).
- Manual: two `logit run` processes on one host (`examples/forwarder-*.yaml`), `kill -STOP` the
  central one → edge's `logit.component.buffer.utilization` climbs, no process exits; `kill
  -CONT` → drains, `logit.output.reconnects` stays 0; restart central → edge reconnects, exactly
  one `Ambiguous` classification for the in-flight batch (visible in
  `logit.output.requests{class}`); repeat over TLS with `testdata/tls`.
- `script/audit` clean (only same-version promotions).

## Explicitly out of scope (file in `known-gaps.md`)

Credit window > 1 / cumulative acks against several in-flight frames; QUIC; an OTLP passthrough
codec on the native link; `cargo-fuzz` targets (nightly); receiver-side dedupe / idempotent batch
ids (`duplicate_safe` stays `false`); `SO_REUSEPORT`/multi-acceptor; a `demo/` second-`logit`
tier (the examples suffice; revisit with the demo plan).
