---
created: 2026-09-13
updated: 2026-10-26
---

# `syslog_in` gains TCP and TLS ingress; `syslog_out` gains TLS

## Status
Accepted

## Context

`syslog_out` (the egress side, [ADR `syslog-output`](syslog-output.md)) has spoken TCP since it was
built, but never TLS -- `docs/known-gaps/syslog.md`'s "`syslog_out` has no TLS" entry already names
the fix as "config-plumbing against `TlsClientConfig`/`TlsServerConfig`... not a design decision to
redo." When this was written, `syslog_in` (`crates/logit-inputs/src/syslog.rs`) had no TCP at all:
its own module doc, and `ComponentKind::SyslogIn { bind: String }`'s doc comment
(`crates/logit-config/src/lib.rs`), both said UDP-only, and `docs/known-gaps/syslog.md`'s
"`syslog_in` is UDP-only" entry recorded why that was a
deliberate, not-yet gap -- the driving integration, nginx's `syslog:` writer, is UDP-only, so a TCP
accept loop would have bought that integration nothing. [ADR `syslog-output`](syslog-output.md)'s
"asymmetry is deliberate" note is the same call from the egress side.

That asymmetry no longer holds once TLS ingress is in scope. RFC 5425 ("TLS Transport Mapping for
Syslog") is syslog framed per RFC 6587 §3.4.1, carried over TLS, which is carried over TCP -- there
is no such thing as syslog-over-TLS without a TCP accept loop underneath it first. This ADR
supersedes both known-gaps entries above: `syslog_in` gains `transport: tcp`, and both `syslog_in`
and `syslog_out` gain a `tls:` block. DTLS (RFC 6012, syslog-over-TLS's UDP-carried sibling) is out
of scope -- see Alternatives.

Everything this decision needs already exists in-tree and is reused, not re-decided:

- Shared TLS builders (now `logit_pipeline::tls::build_server_config` and `build_client_config`,
  per [ADR `tls-certificate-reload`](tls-certificate-reload.md)):
  `crates/logit-inputs/src/tls.rs::build_server_config` and
  `crates/logit-outputs/src/tls.rs::build_client_config`, both `pub(crate)`, already used by
  `otlp_in`/`otlp_out` and `logit_in`/`logit_out`
  ([ADR `otlp-tls-and-pooled-grpc-client`](otlp-tls-and-pooled-grpc-client.md),
  [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md)). No new
  dependency: `rustls`/`tokio-rustls` are already direct dependencies of both crates.
  `testdata/tls/` already holds a committed CA, server cert (`localhost`/`127.0.0.1` SANs), a
  second "wrong CA," and a client cert for mutual TLS.
- Raw-TCP-plus-TLS templates: `crates/logit-inputs/src/logit.rs` (`logit_in`'s accept loop:
  semaphore-guarded connection cap, handshake timeout, TLS accept inside the per-connection task)
  and `crates/logit-outputs/src/logit.rs` (`logit_out`'s connect path: TCP connect, SNI from the
  endpoint's host, both under one timeout, `Fault::Clean` on failure).
  [ADR `decoupled-listener-io`](decoupled-listener-io.md)'s `logit_inputs::udp::UdpListener<D>` is
  the shape a shared driver on the TCP side should mirror -- a listener generic over
  `D: Decoder + Clone + Send`, built once and reused by every protocol on that transport.
- `logit_pipeline::BatchAccumulator` (already `pub`, `docs/adr/decoupled-listener-io.md`) is the
  datagram-to-batch assembly this ADR reuses per connection instead of per socket.

## Decision

### `syslog_in` gains `transport: tcp`

`SyslogIn` gains `#[serde(default)] transport: SyslogTransport` (the same enum `syslog_out` already
publishes -- see "Config surface" below) and `#[serde(default)] tls: Option<TlsServerConfig>`.
`SyslogIn`'s doc comment, which at the time asserted UDP-only is deliberate, and
`docs/known-gaps/syslog.md`'s "`syslog_in` is UDP-only" entry, and [ADR `syslog-output`](syslog-output.md)'s
"that asymmetry is deliberate" note, are all superseded by this decision, not merely narrowed.

### Framing is auto-detected per connection, not configured

There is no `framing:` field. Each accepted connection peeks its first byte: an ASCII digit means
RFC 6587 §3.4.1 octet-counting (`MSG-LEN SP SYSLOG-MSG`, where MSG-LEN is that section's own
`NONZERO-DIGIT *DIGIT` production -- a decimal byte count with no leading zero); anything else means
non-transparent (LF-delimited) framing, because a well-formed syslog message always begins with
`<`PRI`>` -- never a digit. The result is latched for the connection's whole life; a sender does not
switch framing mid-connection, and there is no in-band signal that would let one.

This is not a novel heuristic invented for this ADR -- it is the same detection Grafana Alloy's
`loki.source.syslog` receiver already performs (`go-syslog`'s `syslogparser.ParseStream` peeks the
first byte to choose between its octet-counting and non-transparent scanners), which [ADR
`syslog-output`](syslog-output.md) already leans on from the sending side: that ADR picked
octet-counting as `syslog_out`'s only TCP framing specifically because Alloy auto-detects it with
no receiver-side configuration. This ADR is the ingress mirror of the same fact: a receiver in this
codebase can lean on the identical leading-byte signal a receiver outside it already does, safely,
because it's a property of the syslog message grammar (PRI is mandatory and numeric-in-angle-brackets,
never a bare digit) rather than a guess about sender behavior.

### The 64 KiB frame bound is a constant, not config

`MAX_FRAME_BYTES = 64 KiB`, fixed in code. This mirrors the UDP path's own ceiling -- a UDP
listener's read buffer is sized to 65507 bytes, the largest possible UDP payload -- rather than
introducing a second, TCP-specific size an operator has to reason about. It is deliberately not the
same knob as `syslog_out`'s `max_message_bytes` (default 8192): that field bounds what a
`logit`-authored sender chooses to emit and is meant to be raised by an operator who wants larger
messages on the wire; this bound protects the receiving side of a stream any peer controls, and
raising it would only grow how much unbounded-looking data one hostile or buggy connection can make
this process hold before the frame is even fully read.

A frame that would exceed the bound, or one whose octet-counting header is malformed (a non-digit
before the separating space, a MSG-LEN with a leading zero, or one so large it couldn't fit in the
bound), does not get skipped or resynchronized -- the connection is closed. Octet-counting has no
resync point: once a MSG-LEN is wrong, every subsequent byte offset in the stream is wrong too, so
there is nothing to recover to. Both cases count
`logit.input.frames.dropped{reason="oversize"|"malformed"}` and log a throttled diagnostic before
the close.

A trailing partial frame at a clean connection close (the peer closed its write half mid-message)
is handled per framing: under non-transparent framing, the partial line is still a complete,
well-formed message missing only its LF, so it is emitted; under octet-counting, a partial frame
carries no information about where the *next* message would have started even if there were one, so
it is dropped, counted `logit.input.frames.dropped{reason="truncated"}`.

### No receive queue on TCP -- the connection itself is the backpressure

A UDP listener needs `receive:`'s queue (`docs/adr/decoupled-listener-io.md`) because its producer
-- the kernel's UDP socket buffer -- cannot be asked to wait; blocking the reader just relocates
loss somewhere this process can no longer see or count. A TCP connection has no such producer: its
own flow control **is** the backpressure a blocked `Fanout::send` should apply. A connection task
that blocks trying to hand a batch downstream simply stops reading its own socket; TCP's own window
mechanics stop the peer, which is exactly the behavior an operator wants from a stream transport
(the same reasoning [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md)
gives for why `logit_in` needs no receive-side queue either).

Consequently, `receive:`'s queue-only fields -- `max_datagrams`, `max_bytes`, `overflow`,
`receive_buffer_bytes` -- are rejected by name on a TCP `syslog_in`, via the same shape rule 17
already uses to reject them on a tail listener (`tail_in`/`docker_in`, which likewise has no receive
queue because the tailed file is its own durable buffer). The batch-assembly fields
(`batch_max_events`, `batch_max_bytes`, `batch_flush_interval`) and `shutdown_grace` still apply,
scoped **per connection** -- an operator with N concurrent connections and
`batch_max_events: 1000` should expect up to N × 1000 events in flight, not one shared bound across
every connection.

### A generic TCP+TLS listener driver, syslog-only for now

`crates/logit-inputs/src/tcp.rs` (new) is `TcpListener<D: Decoder + Clone + Send + 'static>`,
mirroring `udp::UdpListener<D>` closely enough that `statsd_in` can adopt it later without a second
driver being written -- but only `syslog_in` is wired to it in this decision; `statsd_in` stays on
UDP-only until a real TCP statsd need appears. `D: Clone` is load-bearing, not incidental: a future
decoder with real per-connection scratch state needs its own clone per connection. `SyslogDecoder`
itself has no such state — its `Diagnostics` is a shared handle, so `bad_line` throttles
listener-wide (see [ADR `service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md)'s
2026-09-14 amendment).

The accept loop is copied from `logit_in`'s (`crates/logit-inputs/src/logit.rs`,
[ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md)): a
`Semaphore`-backed cap of 1024 concurrent connections, `try_acquire_owned` (reject outright, never
queue -- a `logit`-to-`logit` peer is expected to retry on its own, but a syslog sender is not
necessarily under this project's control, so "reject fast" is the safer default either way), a 5s
handshake timeout, and the TLS accept happening inside the per-connection spawned task rather than
the shared accept loop, so one slow or hostile handshake only ever stalls its own connection's
concurrency-cap slot.

**Amendment (2026-09-13):** that handshake timeout is operator-tunable now, not a constant.
`SyslogIn` carries a `handshake_timeout: Duration` field (default 5s, the same number this section
names, still applied *per* pre-message phase -- the TLS accept, then the wait for the first byte --
rather than as one shared deadline), and `logit_in`/`otlp_in` gained the identically-named field at
the same time so one number and one field name cover every ingress listener kind (`syslog_in`,
`logit_in`, `otlp_in`). Graph rule 45 keeps it
non-zero and, on `transport: udp`, rejects a non-default value outright: a datagram listener has no
connection to hand shake, so set-but-ignored would be the wrong outcome for the same reason rule 43
refuses a `tls:` block there. What did *not* change: this is still a pre-message bound only, never
an idle timeout on an established connection -- the idle-connection row in `docs/known-gaps/intake.md`
recorded why closing that gap is its own effort with its own ADR, since closed by [ADR
`idle-connection-timeout`](idle-connection-timeout.md).

**One simplification against the `logit_in` template.** `logit_in` writes a clean `Reject` control
message to a past-the-cap connection, which means doing the TLS handshake first even for a
connection that will be refused, so the reject can be written encrypted rather than looking like a
protocol violation on the wire. Syslog has no in-band reject message of any kind -- there is nothing
this listener could write that a syslog sender would recognize as "try again later." A past-the-cap
connection is therefore closed **before** any TLS handshake is attempted, saving that handshake
cost entirely; `logit.input.connections.rejected{reason="limit"}` is still counted, and
`logit.input.connections` (gauge) counts only connections that actually hold a permit, exactly as
`logit_in`'s does.

### The framer sits under the decoder, and the decoder stops re-splitting on `\n`

`SyslogDecoder::decode_into` (`crates/logit-proto/src/syslog/decode.rs`) currently splits its input
on `\n` because a UDP datagram can carry more than one line-delimited message. On the TCP arm, the
framer above has already delimited one message per frame -- including an octet-counted MSG, which
may legally *contain* an embedded newline as ordinary message content, not a delimiter. Re-splitting
on `\n` inside the decoder would shred such a message into multiple spurious events. `SyslogDecoder`
gains `with_line_splitting(bool)` (default `true`, preserving UDP's existing behavior unchanged);
`SyslogInput::tcp` turns it off for both TCP framings, since the framer is the sole delimiter once a
frame has been produced. `SyslogDecoder` also gains `#[derive(Clone)]`, needed because
`TcpListener<D>` clones its decoder once per accepted connection.

### TLS: presence turns it on, and it is required once on

`tls:` on either `syslog_in` (`TlsServerConfig`) or `syslog_out` (`TlsClientConfig`) follows the
`logit_in`/`logit_out` shape, not `otlp_in`/`otlp_out`'s scheme-selected one: both `bind` and
`endpoint` are bare `host:port` strings with no URL scheme to read a signal from, exactly like
`logit_out`'s `endpoint` and unlike `otlp_out`'s `https://`-vs-`http://`. A `tls:` block's mere
presence is therefore the only signal available, and it always turns TLS on -- there is no
plaintext fallback once a `tls:` block is configured, on either side. `tls:` under
`transport: udp` is a config error on both `syslog_in` and `syslog_out`: DTLS (RFC 6012) is out of
scope (see Alternatives), so a `tls:` block under UDP could never have any effect, and silently
ignoring it would hide a likely operator mistake rather than surface it.

No ALPN is offered, the same choice `logit_in`/`logit_out` already made for the same reason: syslog
is not an HTTP-shaped protocol, so there is nothing for a client to negotiate down to.

This reuses `logit_inputs::tls::build_server_config`/`logit_outputs::tls::build_client_config`
and `testdata/tls/` outright -- `docs/known-gaps/syslog.md`'s existing `syslog_out` entry already
names this as "config plumbing... not a design decision to redo," and nothing about the TCP-ingress
work above changes that call.

### `syslog_out`'s TCP connection becomes TLS-capable the same way `logit_out`'s does

`Conn::Tcp`'s `stream: Option<TcpStream>` becomes `Option<Box<dyn AsyncStream>>` -- `AsyncStream`
(currently a private trait in `crates/logit-outputs/src/logit.rs`) and `host_only` (also currently
private there) are hoisted into `crates/logit-outputs/src/tls.rs` as `pub(crate)`, in their own
commit, so `logit.rs` and `syslog.rs` share one definition of each rather than a second copy. The
connect path taken when `stream` is `None` follows `logit_out`'s exactly: TCP connect, then (when
TLS is configured) `ServerName::try_from(host_only(endpoint))` and a `TlsConnector::connect`, both
races against `connect_timeout`, both classified `Fault::Clean` on failure -- nothing has been
written yet at that point.

`send_tcp`'s two invariants -- documented at length in its own doc comment and unchanged here --
stay exactly as they are: the live connection is always `stream.take()`n into a local before any
write (cancellation safety against `deliver_with_retry`'s per-attempt timeout, since
`AsyncWriteExt::write_all` is not cancel-safe), and the very first write of each attempt is a single
non-`write_all` `write()` call, so a zero-byte failure proves nothing left the host (safe to
reconnect once and retry the whole frame) while any failure after that proves at least one byte
landed (`Fault::Ambiguous`, never resent). Only the `None =>` connect arm changes.

**Amendment (2026-09-13, PR review):** those invariants are *not* transport-agnostic, and the
erased `Box<dyn AsyncStream>` hid the difference. `tokio_rustls`' `poll_write` copies plaintext
into the rustls session and then writes the socket until one write returns `Pending`, so it
returns `Ok(n)` with finished records still queued in userspace, and it returns `Err` after
earlier socket writes in the same call already succeeded. So on TLS an `Ok` proves only that the
session accepted the bytes, and an `Err` is never proof of a zero-byte attempt. `send_tcp`
therefore asks `TcpDial` which transport it is on: plaintext keeps the behaviour above verbatim,
while on TLS there is no internal reconnect-and-retry and no resend once an application write has
been attempted — every such failure is `Fault::Ambiguous`, and `Fault::Clean` survives only for
failures inside `TcpDial::connect`, which precede every byte of the frame. The success path now
also `flush`es on both transports before the batch may be reported delivered (a failed flush is
`Fault::Ambiguous` and the connection is discarded); without it a TLS batch could be committed
off the sink queue with its records still in the rustls buffer, to be dropped with the boxed
stream by the next reconnect or cancelled attempt. `logit_out` never needed this because it waits
for a per-batch ack; `syslog_out` is write-only and has no such backstop.

`connect_timeout` bounds each connect *phase* separately — the TCP connect, then the TLS
handshake — so a TLS connect can take up to twice it. That matches `logit_out`, which races every
step of its own connect against its single timeout; the config field's doc says so rather than
implying one shared deadline.

`SyslogOutput` gains `with_tls(&TlsClientSettings, base_dir)`. Because `tls` is
`Option<TlsClientConfig>` here -- presence itself is what turns TLS on, decided at the call site in
`crates/logit-cli/src/pipeline.rs`, not inside the builder -- `with_tls` has no `is_empty()` early
return of its own: like `logit_out`'s own `with_tls`, it always builds a `rustls::ClientConfig` from
whatever `settings` it's given, so an explicit but otherwise-empty `tls: {}` block still means TLS,
with the bundled Mozilla root set as trust. It does gain the `insecure_skip_verify` startup warning
`otlp_out` already logs -- `logit_out` omits that warning today, which this decision does not copy
-- and a `logit.output.reconnects` counter, incremented on every connect after the first -- the same
signal `logit_out` already publishes for its own reconnects, giving `syslog_out` parity rather than
a differently-shaped metric for the same event.

## Alternatives considered

- **A `framing:` config field**, `octet_counting | non_transparent`. Rejected: the leading-byte
  signal is unambiguous (PRI is mandatory and never starts with a digit), Alloy's own receiver
  already relies on the same detection with no configuration, and a config field would just be one
  more way for an operator's config and a sender's actual behavior to silently disagree.
- **Octet-counting only, no non-transparent framing at all.** Rejected: `syslog_out`'s own TCP
  framing is octet-counting-only by design ([ADR `syslog-output`](syslog-output.md)), but this is
  the *ingress* side, which has to interoperate with senders `logit` doesn't control -- rsyslog's
  `omfwd` defaults to non-transparent (LF) framing over TCP, and requiring every sender to opt into
  octet-counting would narrow this listener's compatibility for no correctness gain, given that
  auto-detection is free.
- **Reusing `UdpListener`'s `ReceiveQueue` on the TCP path.** Rejected: that queue exists because a
  UDP socket's producer (the kernel) cannot be made to wait; a TCP connection's producer (the peer,
  across the socket) can, and already does, via ordinary TCP flow control the moment this listener
  stops reading. Adding a queue here would just relocate a backpressure signal that already works
  correctly into a place it has to be reinvented.
- **DTLS (RFC 6012) alongside TLS.** Rejected as out of scope for this decision: it is UDP-carried,
  so it would extend the *existing* UDP path rather than build on any of the TCP work here, has
  essentially no footprint in this codebase's driving integrations, and would require a distinct
  handshake/record-layer implementation `rustls`/`tokio-rustls` do not provide. `tls:` under
  `transport: udp` stays a hard config error rather than a silent no-op so this gap is visible if
  an operator reaches for it.
- **Per-event peer-address attributes** (e.g. `net.peer.address` from the accepted connection).
  Rejected for this decision: nothing in the settled config surface asked for it, and adding it
  would be new lossless-transit-relevant model surface decided in passing rather than as its own
  question; revisit if a real need for source-address-based routing or auth surfaces.
- **A syslog-specific (non-generic) accept loop**, rather than a driver other protocols could
  adopt. Rejected: the accept-loop shape (semaphore cap, handshake timeout, TLS-inside-the-task) is
  already duplicated once between `otlp_in` and `logit_in`; writing a third bespoke copy for
  `syslog_in` when the shape is again identical would be the same reinvention
  [ADR `decoupled-listener-io`](decoupled-listener-io.md) already rejected on the UDP side, this
  time on TCP.
- **A configurable frame-size bound.** Rejected: the UDP path's own ceiling (65507 bytes, the
  largest possible UDP payload) has never been configurable either, and a configurable TCP bound
  would invite conflating it with `syslog_out`'s already-configurable `max_message_bytes`, which
  answers a different question (how large a message this process chooses to send) from the one this
  bound answers (how much unbounded-looking data one connection can make this process hold).

## Consequences

- `crates/logit-config/src/lib.rs`: `SyslogIn` gains `transport: SyslogTransport` and
  `tls: Option<TlsServerConfig>`; `SyslogOut` gains `tls: Option<TlsClientConfig>`. `SyslogIn`'s doc
  comment and `SyslogTransport`'s (currently `syslog_out`-only) are rewritten to describe both
  directions. `schema/logit.schema.json` regenerated.
- `crates/logit-inputs/src/tcp.rs` (new): the generic `TcpListener<D>` driver and its
  `Framing`/framer, described above; `pub mod tcp;` in `crates/logit-inputs/src/lib.rs`. The
  framer has since moved to `crates/logit-proto/src/framing.rs`, and the decoder to
  `crates/logit-proto/src/syslog/`.
  `crates/logit-inputs/src/syslog.rs`: `SyslogInput::tcp`, `with_tls`, `with_line_splitting` wiring.
- `crates/logit-outputs/src/syslog.rs`: `Conn::Tcp`'s boxed stream, `SyslogOutput::with_tls`,
  `logit.output.reconnects`. `crates/logit-outputs/src/tls.rs`: `AsyncStream` and `host_only`
  hoisted in from `crates/logit-outputs/src/logit.rs` (own commit, behavior-preserving).
- `crates/logit-pipeline/src/graph.rs`: new rules 43 (`syslog_in`'s `tls:` rejected under
  `transport: udp`) and 44 (`syslog_out`'s `tls:` internal consistency, the twin of rule 34's
  `logit_out` checks, plus the same tls-requires-tcp check as rule 43); `is_datagram_listener`
  narrows to `SyslogIn { transport: Udp, .. }`, and a new `is_stream_listener` predicate covers
  `SyslogIn { transport: Tcp, .. }` for rules 17/18's `receive:` handling.
  `docs/design/pipeline-graph.md`'s rule list documents all three changes.
- `crates/logit-cli/src/pipeline.rs`: `SyslogIn`/`SyslogOut` arms gain their `tls:` crossing,
  following `OtlpIn`'s template; a new `tcp_receive_config` helper, the stream-listener sibling of
  the existing `receive_config`.
- New telemetry (catalogued in `docs/design/internal-telemetry.md`): `logit.input.connections`
  (gauge) and `logit.input.connections.rejected{reason="limit"}` (count), reused verbatim from
  `logit_in`; `logit.input.frames` and `logit.input.frame.bytes` (count/sum), the stream-transport
  twin of `logit.input.datagrams`/`.datagram.bytes`; `logit.input.frames.dropped{reason=
  "oversize"|"malformed"|"truncated"}` (count), the same per-reason shape `logit.proto.errors`
  already uses. No TLS-specific metric, on the same footing `docs/deploying.md`'s existing TLS
  section already gives `otlp_in`/`otlp_out`: a handshake failure surfaces through the same
  connection-error diagnostics and counters any other transport failure would.
- `docs/known-gaps/syslog.md`: "`syslog_in` is UDP-only" and "`syslog_out` has no TLS" both close
  outright (the latter narrowed to DTLS, which stays open as its own row). Two existing rows
  generalize rather than close: "TLS certificates are loaded once at startup" (since closed by
  [ADR `tls-certificate-reload`](tls-certificate-reload.md)) and "no `server_name`
  override" (since narrowed to SNI by [ADR `tls-client-server-name`](tls-client-server-name.md)),
  both currently scoped to `otlp_in`/`otlp_out`, now also describe
  `syslog_in`/`syslog_out` (and `logit_in`/`logit_out`), since none of those components' TLS
  construction differs in either respect. A new row: no idle-connection timeout on a TCP listener --
  a handshaken-then-silent connection holds a concurrency-cap permit indefinitely, a gap this
  decision shares with `otlp_in` rather than introducing fresh.
- `demo/` stays plaintext -- the same call [ADR `otlp-tls-and-pooled-grpc-client`](otlp-tls-and-pooled-grpc-client.md)
  made for OTLP: TLS in the demo would need a certificate story (self-signed with a trust warning,
  or a real CA) that answers a question this decision doesn't need to answer to ship TCP/TLS
  support that a real deployment can use today.
- Test fixtures: a recorded rsyslog-over-TCP interop capture (`testdata/interop/syslog/
  rsyslog-tcp-000.raw`) closes `testdata/interop/syslog/README.md`'s existing "TCP framing... not
  covered (yet)" note. TLS interop is proven in-process against `testdata/tls/`, the same way
  `logit_in`/`logit_out`'s own TLS tests are, rather than via a second recorded fixture.

## Verification against a real forwarder

A manual run on 2026-09-14 pointed rsyslog 8.2302.0 (Debian bookworm's package, with
`rsyslog-gnutls` for the `gtls` netstream driver) at a TLS `syslog_in`
(`transport: tcp`, `tls:` over `testdata/tls/server.*`) feeding `stdio_out`. Each run sent two
`logger` messages:

- **TLS, default LF framing** (`omfwd` with `StreamDriver="gtls"`, `StreamDriverMode="1"`,
  `StreamDriverAuthMode="x509/certvalid"`, `DefaultNetstreamDriverCAFile` set to
  `testdata/tls/ca.pem`): the handshake completed, both messages arrived intact with correct
  `syslog.*` attributes, and the listener logged one `connection_error` at teardown (the peer
  closed without a TLS `close_notify`) and dropped the connection. `x509/name` wasn't reachable
  because the server certificate's SANs (`localhost`, `127.0.0.1`) don't cover the container's
  hostname; `x509/certvalid` still validates the chain, which `anon` doesn't.
- **TLS, `TCP_Framing="octet-counted"`:** both messages arrived byte-correct and in order with no
  framing diagnostic. A mis-parsed octet count would have desynchronized the stream or raised a
  framing error.
- **Plaintext TCP control:** both messages arrived with no diagnostics.

`logger` escapes an embedded newline (`\n` becomes `#012`) before `omfwd` sees it, so this setup
can't distinguish the two framings by an embedded-newline probe. The round-trip tests in
`crates/logit-cli/tests/syslog_round_trip.rs` cover a multiline octet-counted message.

## Amendment: the driver serves more than one listener now (2026-09-14)

This decision built `crates/logit-inputs/src/tcp.rs` as a shared driver but wired exactly one
listener to it. `graphite_in` is the second ([ADR `graphite-carbon-relay`](graphite-carbon-relay.md)'s
2026-09-14 amendment deletes its own 490-line accept loop in favour of this one), and `statsd_in`
is the third -- the adoption "A generic TCP+TLS listener driver, syslog-only for now" above held
out until a real need appeared, now landed as `transport: tcp` plus a `tls:` block on that
listener, adding no code to the driver at all beyond what the two changes below already required.
Three things that were tacitly "what syslog needs" have had to become explicit as a result.

**Framing is chosen per listener, not sniffed by the driver.** The driver latched RFC 6587's two
framings from each connection's first byte, reading a leading ASCII digit as an octet count. That
is sound for syslog and only for syslog, where a non-transparent message always begins `<`; a
carbon path or a statsd metric name may legitimately begin with a digit, and the sniff would
reframe the whole connection on it. So `TcpListener::with_framing` now takes a `FramingMode`:
`Rfc6587Auto` (the default, and `syslog_in`'s — byte-identical behaviour to before),
`Lines { oversize }`, or `LengthPrefixed`. A builder rather than a `TcpListenerConfig` field, since
that struct is the image of the `receive:` block an operator writes and framing is not something
an operator sets.

**A frame bound is per listener too.** `MAX_FRAME_BYTES` stays the default and stays
non-configurable on `syslog_in`, for the reason its own doc gives. But `graphite_in` has two
operator-facing bounds that carbon's own receivers expose (`max_line_bytes`, and `max_frame_bytes`
defaulting to Twisted's megabyte), so `Framer::new` takes the bound alongside the mode and
`Framer`'s `Default` is gone: both arguments are real decisions, and a default would silently pick
syslog's.

**Not every framing failure is fatal.** `FrameError` gained `OversizeSkipped`, the one variant with
`is_fatal() == false`: under `Oversize::DrainToNextLine` an over-bound line is dropped, counted as
`frames.dropped{reason="oversize"}` like its fatal sibling, and the framer resynchronizes at the
next `LF` instead of the connection closing. `syslog_in` keeps `Oversize::Fatal`, so nothing about
its behaviour changes. This is carbon's own recoverable-oversize rule, moved into the driver as a
mode rather than reimplemented outside it.

**The first-byte deadline's predicate changed, and had to.** It was `framer.framing().is_none()` —
"has this connection latched a framing yet", which is only ever a first-byte question under
`Rfc6587Auto`. Under either explicit mode the framing is known at construction, so that predicate
reads "already framed" on a connection that has said nothing, and `handshake_timeout` would
silently never fire on `graphite_in` or `statsd_in` while continuing to work perfectly on
`syslog_in`. It is now `Framer::first_byte_seen()`, and
`the_first_byte_deadline_applies_under_every_framing_mode` pins it across all three modes.

**`frame_diag` is gone.** The driver kept an `Arc<Mutex<Diagnostics>>` beside the per-connection
clone, so that `framing_error` and `bad_frame` throttled listener-wide while the clone's other keys
did not. `Diagnostics` now shares its counts across every clone of one component's value
([ADR `service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md)'s 2026-09-14
amendment), which is the general form of what that workaround bought for two keys, so the
connection's own `&mut Diagnostics` is threaded through instead and the `Mutex` is deleted.

## Amendment: idle-connection timeout closes the gap this ADR held out (2026-09-14)

The "Pre-handshake timeout" section above and the Consequences list both named a specific gap this
decision deliberately did not close: `handshake_timeout` bounds only the pre-message phases, and a
connection that gets past them and then goes quiet held its connection-cap permit indefinitely,
with "closing that gap is its own effort with its own ADR" the explicit call at the time. That
effort is done: [ADR `idle-connection-timeout`](idle-connection-timeout.md) adds an opt-in
`idle_timeout:` field to `syslog_in` (`transport: tcp`) and the same TCP driver's other listeners,
closing exactly the row this ADR opened. The reset rule answers the design question this ADR's own
no-receive-queue posture raised without settling: the clock runs only while the driver is waiting
on the peer's socket, reset by bytes read and by an interval flush actually emitting a batch, so
time blocked handing a batch to a full downstream never counts against a peer -- a connection
stalled on backpressure is never mistaken for a silent one. See
[`docs/deploying.md`'s "`idle_timeout` on a TCP listener"](../deploying.md#idle_timeout-on-a-tcp-listener)
for the operator-facing account, including the recommendation to enable it wherever consistent
traffic is expected.

## Amendment: `framing:` exists, for UDP only (2026-10-16)

"Framing is auto-detected per connection, not configured" still holds for TCP. A TCP connection's
framing stays detected from its first byte, and `framing:` under `transport: tcp` is a config
error. `syslog_in` now has a `framing: lines | datagram` field that applies to `transport: udp`
only. See [ADR `syslog-udp-datagram-framing`](syslog-udp-datagram-framing.md).

## Amendment: stream sinks read for a client-certificate rejection (2026-10-26)

### Context

Under TLS 1.3 the server checks the client certificate after the client's `Finished`, so
`TlsConnector::connect` returns before the verdict. A rejection arrives later, as an alert or a
bare close, and only a read sees it. Go's maintainers state the same for `crypto/tls`: the client
can't learn the verdict from `Handshake`, and the error appears on the first `Read`
([golang/go#32202](https://github.com/golang/go/issues/32202),
[#34796](https://github.com/golang/go/issues/34796),
[#42656](https://github.com/golang/go/issues/42656)). OpenSSL's maintainers give the same answer:
there's no client-auth status to query, and a client learns it only by reading
([openssl-users, 2021-09](https://mta.openssl.org/pipermail/openssl-users/2021-September/014214.html)).
RFC 8446 has no message that confirms acceptance (§2), and RFC 5425 has no application-layer
acknowledgment.

`syslog_out` and `statsd_out` never read. A rejected connection took the first batch, counted it
delivered, and lost it. On a pooled connection the reuse probe read the alert as EOF and redialed,
so every later batch went the same way: a sink whose certificate a server rejected lost every
batch, uncounted. "`syslog_out`'s TCP connection becomes TLS-capable" above held that a flush
before success was enough; a flush proves the bytes left, not that the server took them.

A session ticket isn't a signal either way. RFC 8446 §4.6.1 makes NewSessionTicket a MAY, sent "at
any time after the server has received the client Finished", with nothing tying it to client
authentication. OpenSSL sends its tickets at the end of `SSL_accept`, before the application
checks the peer's name, so rsyslog's `PermittedPeer` rejects a connection that already holds two
tickets (verified below).

### Decision

A new TLS 1.3 connection from `syslog_out` or `statsd_out` to a destination that has asked for a
client certificate reads for `accept_timeout:` (default `1s`, `0s` off) before its first write. An
alert or a close in that window is `Fault::Refused`; silence for the whole bound is acceptance.


`crates/logit-outputs/src/stream.rs`'s module doc, "The acceptance wait", holds the fault table.

- **The wait is a plain bounded read.** Tickets are consumed and ignored. An alert is `Refused`,
  its message naming the alert (`certificate_required`, `bad_certificate`, or the number of an
  unnamed one). Any close is `Refused` too: EOF, `close_notify`, a reset, or another read error.
  Nothing was written, so a close here is the server turning the client away, and a redial meets
  the same answer. Refused holds the queue and retries with no budget
  ([ADR `sink-fault-classes`](sink-fault-classes.md)): a rejected certificate stays rejected until
  an operator fixes it, so dropping batches as `Rejected` would lose data the fix could save.
- **Application data before the first write stays `Fault::Clean`.** A line receiver never sends
  any, so the connection isn't one to write into, but nothing says the server rejected a
  certificate. `PendingClose::Bytes` already reads data on a reused connection the same way.
  Calling it `Refused` would hold the queue forever behind a server that greets every connection,
  where `Clean` retries on a new connection.
- **Each destination remembers that it asked.** rustls calls the config's client-certificate
  resolver only on a CertificateRequest, so `TlsTarget::new` wraps the resolver once in
  `RecordsCertRequest`, which sets a task-local flag for the one handshake and delegates to the
  inner resolver, so certificate reload still works. A full handshake that records a request sets
  `TlsTarget`'s `asks_for_client_cert`, and the flag never clears. A resumed TLS 1.3 handshake
  carries no CertificateRequest, so the flag is what makes a resumed connection to that
  destination wait too. The wrapper is one `Arc` for the config's life, not one per dial: rustls
  offers a stored ticket only to a config whose resolver is the `Arc` the ticket was issued
  under, so a per-dial wrapper would end session resumption.
- **A destination that never asked never waits**, on a full or a resumed handshake. TLS 1.2 doesn't
  wait either: its server checks the certificate inside the handshake, so a rejection already
  fails the dial `Clean`. `logit_out` and `graphite_out` pass a zero bound.
- **On a reused connection, an alert the probe reads is `Fault::Refused`**, with nothing written
  and no redial (`crate::tls::poll_pending_close_or_alert`). A redial would hide a rejection that
  already cost the batch written on that connection. `poll_pending_close` keeps its old answer for
  `logit_out` and `kafka_out`.
- **`accept_timeout` defaults to `1s`.** Silence is the only acceptance TLS gives, so every new
  connection to a destination that asks for a certificate pays the whole bound once, full or
  resumed. Pooled connections make new ones rare. A rejection on a LAN or a nearby region arrives
  well inside a second; rsyslog's arrived within 50 ms below. The field is accepted without `tls:`
  and has no effect there, so a config can set it ahead of TLS.

### Consequences

- A rejected client certificate now reads `Refused` and holds the queue, where it lost every batch
  before.
- A new connection to a server that asks for a client certificate opens `accept_timeout` later, a
  resumed one included. A server that asks for none costs nothing.
- A rejection that arrives after `accept_timeout` still loses that connection's first batch. The
  next send's probe reports an alert as `Refused`; a bare close looks like an idle close and
  redials, and the redial's own wait reads the rejection. The write-only senders under
  "Alternatives considered" accept the same loss on every connection
  (`docs/known-gaps/intake.md`).

### Alternatives considered

- **A session ticket as the acceptance signal.** The first version of this amendment ended the wait
  on a ticket, which costs one round trip instead of the whole bound. It's unsound: RFC 8446 makes a
  ticket optional and unrelated to client authentication, and OpenSSL-based servers send theirs
  before the application rejects the peer, so rsyslog's `ossl` driver sent two tickets and then
  closed. A ticket can't prove acceptance and its absence proves nothing.
- **Forcing TLS 1.2.** A TLS 1.2 server rejects the certificate inside the handshake, so the dial
  fails with no wait; Go's maintainers suggest it as the diagnostic. It gives up TLS 1.3 for every
  destination to fix one failure mode, and a server that allows only 1.3 can't be reached at all.
- **No wait, the reuse probe only**, as Vector's socket sink does with a one-byte non-blocking
  read before each flush (`shutdown_check` in
  [`src/sinks/util/tcp.rs`](https://github.com/vectordotdev/vector/blob/78e5330bb31d8c8b06bf86fad61299a2f62aaaf4/src/sinks/util/tcp.rs)).
  rsyslog's `omfwd` peeks the raw socket with `MSG_PEEK` in `CheckConnection`, which sees a FIN but
  not an alert. A probe sees a rejection only after a batch went into the connection, so against a
  server that rejects every connection it loses every batch, one per connection. That's the loss
  the ticket design kept on resumed connections, and why resumed connections wait here.
- **A read watcher armed for the connection's whole life**, as syslog-ng's `logwriter.c` registers
  an input handler on an idle writer and reconnects on EOF. It catches a late rejection sooner but
  still after the first batch, and it needs a reader task per pooled connection where the probe
  reads at the moment it matters. The wait plus the probe covers the window a watcher would.

Telegraf's `socket_writer` and the OpenTelemetry Collector's syslog exporter don't read at all. The
Exim fix for the same TLS 1.3 behavior ([fc243e94](https://github.com/Exim/exim/commit/fc243e944ec00b59b75f41d07494116f925d58b4))
works because SMTP reads a response after every command; a syslog or statsd line gets none.

### Verified behavior

Run on 2026-10-26 against rsyslog 8.2504.0 (`rsyslog 8.2504.0-1+deb13u2`, Debian 13, image built
`FROM debian:13-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132`,
local image `sha256:c9a87bfd0401f6ca055e2e77b30f53bffd2ae6ab87abec1736fc2f8ce8e70427`) with
OpenSSL 3.5.7 (`ossl`) and GnuTLS 3.8.9 (`gtls`). Each `imtcp` input ran `StreamDriver.Mode="1"`,
`StreamDriver.AuthMode="x509/name"`, and a `PermittedPeer` that did or didn't match the client
certificate's CN. `AuthMode` and `PermittedPeer` work only as `input()` parameters: set on
`module()`, the `ossl` driver accepted every peer. `openssl s_client -tls1_3 -msg` recorded the
wire; then a `logit run` of `generate_in` into `syslog_out` (one event a second) ran against each
server.

| Driver | Case | What rsyslog sent |
|---|---|---|
| `ossl` | name doesn't match | two NewSessionTickets, then `close_notify` (a warning, no fatal alert), about 50 ms after the client's `Finished` |
| `ossl` | no client certificate | fatal `certificate_required` at once |
| `ossl` | any resumed handshake | fatal `internal_error` inside the handshake (its log: `session id context uninitialized`) |
| `gtls` | name doesn't match, or no certificate | a bare TCP close, no alert, about 40 ms after the handshake; no tickets |

- `syslog_out` read every rejection as `Refused` before writing (`the peer closed the
  connection`, `the peer sent alert certificate_required`, and the bare close as `peer closed
  connection without sending TLS close_notify`), and held the queue: all six batches were still
  queued at shutdown and none reached the server.
- With a matching `PermittedPeer`, the first message arrived 1.002 to 1.003 s after the handshake
  under both drivers, the `1s` wait, and every message arrived.
- A resumed connection never reached rsyslog's peer check. The `ossl` driver fails every
  resumption with `internal_error` (a `Clean` dial failure; rustls spends one ticket per attempt
  and then makes a full handshake), and the `gtls` driver issues no TLS 1.3 tickets.
- The `ossl` driver enforced a non-matching `PermittedPeer` only on the first connection after
  `rsyslogd` started. Of four full handshakes in a row with the same certificate, it turned away
  the first and accepted the other three, whose messages it wrote to its file. `syslog_out`
  delivered there after its first refusal for the same reason. That's rsyslog's behavior, not
  something a sender can detect.
