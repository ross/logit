---
created: 2026-09-03
updated: 2026-09-14
---

# TLS for `otlp_out`/`otlp_in`, and a pooled gRPC client to carry it

## Status
Accepted

## Context

`otlp_out`'s HTTP transport already speaks TLS today, for free, via `reqwest`'s default `rustls`
backend (`docs/adr/otlp-compression-and-decompression-bounds.md`'s workspace already pins
`reqwest = { features = ["rustls-tls"] }`) -- an `https://` endpoint just works, trusting the
bundled Mozilla root set. What doesn't work:

- `otlp_out`'s gRPC transport hard-rejects `https://` (`reject_insecure_grpc_endpoint`,
  `crates/logit-outputs/src/otlp.rs`) -- its hand-rolled client
  ([ADR `hand-rolled-grpc-over-hyper`](hand-rolled-grpc-over-hyper.md)) drives a raw `TcpStream`
  with no TLS layer at all. Filed as a known gap ("`otlp_out` has no gRPC TLS",
  `docs/known-gaps.md`) while evaluating whether `otlp_out` could replace the demo's
  `syslog_out` → Alloy → Loki log leg
  ([docs/plans/otlp-logs-and-resource-identity.md](../plans/otlp-logs-and-resource-identity.md)).
- Neither transport can trust a private CA, present a client certificate for mutual TLS, or
  (deliberately, for a throwaway/pre-production endpoint) skip verification.
- `otlp_in` terminates nothing -- plaintext HTTP/1.1, h2c, and h2 only.

A real deployment needs all three: Tempo/Loki behind an ingress with a private CA, a `logit`
edge → `logit` central hop over an untrusted network, and sometimes mutual TLS in a zero-trust
mesh.

## Decision

**TLS itself is `rustls`, not hand-rolled.** The full stack -- `rustls`, `tokio-rustls`,
`hyper-rustls`, `rustls-pki-types`, `webpki-roots`, `ring` (the crypto provider) -- already
resolves in `Cargo.lock` at these exact versions, transitively via `reqwest`'s `rustls-tls`
feature. Promoting them to direct workspace dependencies adds **no new crate version** to the
graph (confirmed via `script/audit`, not assumed) -- the same move
[ADR `hand-rolled-grpc-over-hyper`](hand-rolled-grpc-over-hyper.md) made for `hyper`/`hyper-util`/
`http`/`http-body-util`. `ring` only, never `aws-lc-rs` -- the latter needs a C toolchain, which
would break [ADR `containerized-development`](containerized-development.md)'s "no host toolchain
needed" property. PEM parsing is `rustls-pki-types`'s `pem` support (gated by its `std` feature,
not a separate `pem` feature -- confirmed against the vendored crate source); `rustls-pemfile` is
deprecated in its favor, so this is the one PEM parser in the graph, not a second one.

**The gRPC client's connection management moves to `hyper-util`'s pooled client, amending
[ADR `hand-rolled-grpc-over-hyper`](hand-rolled-grpc-over-hyper.md).** That ADR's hand-rolled
`grpc_roundtrip` opened a fresh `TcpStream` and did a fresh HTTP/2 handshake per request --
already a filed gap ("opens a fresh connection per request", `docs/known-gaps.md`). Layering
`tokio-rustls` onto that same per-request connect (the known-gaps entry's own sketch) would add a
full TLS 1.3 handshake to every one of those requests and leave the reuse gap exactly where it
was -- the "build, test, debug, maintain" trap of extending hand-rolled infrastructure past the
point it earns its keep. Instead, `grpc_roundtrip`'s connect+handshake step is replaced by
`hyper_util::client::legacy::Client` over a `hyper-rustls` `HttpsConnector` -- both already
compiled into the binary via `reqwest` (which uses `hyper-util`'s `client-legacy` and
`hyper-rustls` internally), so this needs only their `http2` feature enabled as direct
dependencies. It gives TLS, `https://`-vs-`http://` dispatch by URI scheme, connection pooling,
and `Error::is_connect()` for the existing `Fault::Clean` classification (confirmed to cover a
TLS handshake failure too, not just TCP connect -- `HttpsConnector::call` does both as one step
under `hyper-util`'s `ErrorKind::Connect`) -- all from the same crates the HTTP transport already
trusts. This also retires the "opens a fresh connection per request" gap as a side effect, not a
separate effort.

**What stays hand-rolled, and why that's still right.** `grpc_frame`/`grpc_unframe`, the
trailers/status parsing, `GrpcBody` (`otlp_in`), and `parse_partial_success` are unchanged --
genuinely small, fully-specified pieces with no TLS-shaped surface at all.
[ADR `hand-rolled-grpc-over-hyper`](hand-rolled-grpc-over-hyper.md)'s rejection of `tonic` stands
for the same reasons it gave; the tipping point to revisit that is a *third* piece of real gRPC
infrastructure (streaming, keepalive tuning, load balancing), not TLS, which turned out to need no
hand-rolling at all.

**Server-side TLS termination (`otlp_in`) has no equivalent library layer to reach for** short of
pulling in `axum`/`tonic` themselves. `tokio_rustls::TlsAcceptor` wrapping each accepted
`TcpStream` -- the same idiom `tonic`'s own server uses underneath -- is small (~15 lines) and
fully specified, so it stays hand-rolled. It runs *inside* the per-connection spawned task, after
that connection's `MAX_CONCURRENT_CONNECTIONS` permit is acquired, not in `run`'s own accept loop
-- a slow or hostile handshake stalls only its own connection and counts against the same
concurrency bound as a slow request, rather than blocking the listener from accepting the next
one.

**Selection: TLS is scheme-selected on `otlp_out`, block-selected on `otlp_in`.** An `https://`
endpoint means TLS on either `otlp_out` transport, matching every OTel SDK's
`OTEL_EXPORTER_OTLP_ENDPOINT` convention; `grpc://` (this codebase's own plaintext spelling) and a
bare `host:port` both still mean plaintext gRPC. `otlp_out`'s new `tls:` block
(`TlsClientConfig`: `ca_file`, `cert_file`+`key_file` for mutual TLS, `insecure_skip_verify`)
*tunes* an already-TLS connection -- it never turns TLS on by itself, and `graph::resolve`'s rule
22 rejects a non-empty `tls:` under a plain `http://`/`grpc://` endpoint rather than silently
ignoring it. `otlp_in` has no endpoint to read a scheme from, so its mere presence of a `tls:`
block (`TlsServerConfig`: `cert_file`+`key_file` required, `client_ca_file` optional) is what turns
TLS on for that listener, on both transports.

**Trust default: the bundled `webpki-roots` set, not the system trust store.** Matches what the
HTTP transport already does today via `reqwest`, keeps the dependency graph unchanged (no
`rustls-native-certs`), and needs no `ca-certificates` package inside the running container --
though the production image already installs one (`Dockerfile`), currently unused by this path.
An operator reaching a privately-CA'd endpoint sets `tls.ca_file` explicitly.

## Alternatives considered

- **`tokio-rustls` layered directly into the existing hand-rolled `TcpStream::connect` +
  `http2::handshake`.** The known-gaps entry's own sketch. Rejected: keeps paying a full
  connect+TLS-handshake cost on every request (worse than today, which only pays TCP
  connect+handshake) and leaves the connection-pooling gap unaddressed -- the reinvention-risk
  case this decision explicitly weighs against.
- **`tonic`, again.** Rejected for the reasons ADR `hand-rolled-grpc-over-hyper` already gives --
  `axum`/`tower`/`tower-http` for the ~95% of surface (streaming, reflection, health checking,
  interceptors, load balancing) OTLP's three unary `Export` methods never touch. TLS turned out to
  need none of that; `tonic` would have bought nothing this decision doesn't already get from
  `hyper-rustls` directly.
- **System trust store (`rustls-native-certs`) as the default, instead of `webpki-roots`.**
  Rejected as the *default* -- a new crate for a use case (`ca_file` already covers a private CA
  explicitly) that duplicates a decision `reqwest`'s existing default already made for the HTTP
  transport. Revisit only if an operator need for OS-trust-store parity actually surfaces.
- **Certificate rotation via a background reload.** Out of scope -- certs load once at
  `OtlpOutput`/`OtlpInput` construction (`logit run` startup); a renewed cert needs a restart.
  Filed in `docs/known-gaps.md`; `rustls::ServerConfig`'s `ResolvesServerCert` (a file-watcher
  hook) or a SIGHUP-triggered reload are the shapes to reach for if this becomes real.

## Consequences

- `crates/logit-outputs/src/otlp.rs`'s `OtlpOutput` gains a `grpc_client:
  hyper_util::client::legacy::Client<HttpsConnector<HttpConnector>, Full<Bytes>>` field, built once
  at construction (against a default trust config, so plaintext gRPC gets pooling too) and rebuilt
  whenever `with_tls` sets a customized one. `grpc_authority`/`reject_insecure_grpc_endpoint` are
  gone, replaced by `normalize_grpc_endpoint` (maps every plaintext spelling to an absolute
  `http://` base URI; keeps `https://` exactly as written).
- `crates/logit-inputs/src/otlp.rs`'s per-connection handler now branches on an optional
  `tokio_rustls::TlsAcceptor` before dispatching to the (unchanged) HTTP/gRPC serving code, via a
  new `serve_connection<IO>` helper generic over the plaintext vs. TLS stream type.
- `logit_config::ComponentKind::OtlpOut`/`OtlpIn` gain `tls` fields (`TlsClientConfig`,
  `Option<TlsServerConfig>`); `graph::resolve` gains rule 22. Both new types belong in
  `logit-config` rather than being sink/listener-specific, so `influxdb_out`/`syslog_out`/a future
  native `logit_in`/`logit_out` can reuse them without re-deciding this shape.
- Test fixtures: `testdata/tls/` (repo root) holds a committed self-signed test CA plus server,
  client, and "wrong CA" leaf certificates, regenerated via `testdata/tls/regen.sh`. Both crates'
  unit tests now exercise a real TLS handshake (HTTP-over-TLS, gRPC-over-TLS, mutual TLS
  accept/reject, a plaintext client against a TLS-only listener) rather than only asserting scheme
  strings, and `crates/logit-cli/tests/otlp_round_trip.rs` gained the TLS/mTLS counterparts to its
  existing plaintext and gzip round trips.
- `docs/known-gaps.md`'s "`otlp_out` has no gRPC TLS" and "opens a fresh connection per request"
  entries are retired; "certificates are loaded once at startup" and "no `server_name` override"
  are filed as new, smaller ones.

## Amendment: `otlp_in` rejects at the cap and bounds a plaintext connection's first byte (2026-09-14)

This ADR left `otlp_in`'s accept loop as it found it: a blocking
`connection_limit.acquire_owned().await` after `accept`, with `handshake_timeout` (added shortly
afterwards, alongside `syslog_in`'s TCP transport) wrapping the TLS accept and nothing else. Both
halves of that are now changed, and `docs/known-gaps.md`'s "a plaintext `otlp_in` has no
pre-first-byte bound" row narrows to a much smaller residual.

**Reject, don't queue.** The loop uses `try_acquire_owned`; a connection past
`MAX_CONCURRENT_CONNECTIONS` is dropped immediately and counted as
`logit.input.connections.rejected{reason="limit"}`, and the new `logit.input.connections` gauge
tracks permit holders. This is `logit_in`'s and the shared TCP driver's shape
(`crates/logit-inputs/src/tcp.rs`'s "Connection limit" section), including the one deliberate
difference from `logit_in`: the rejection happens *before* any TLS accept, because OTLP — unlike
the native protocol's `Reject` control frame — has no in-band way to tell a peer why it is being
closed, so there is nothing to say and no reason to spend a handshake saying it. What the blocking
version cost was not fairness but liveness: under it, connections that completed the TCP handshake
and then sent nothing pinned every permit, and the accept loop stopped draining its backlog at
all, so the 1025th peer got neither service nor a refusal.

**`handshake_timeout` on a plaintext listener now means something.** It bounds each of a
connection's pre-request phases, one budget each: the TLS accept when `tls:` is set, and — on the
plaintext arm, which has no TLS accept — the wait for the connection's very first byte. That
second bound is `tokio::net::TcpStream::peek`, i.e. `recv(..., MSG_PEEK)`, run under the same
timeout: it waits for a byte to become *available* and consumes nothing, so the stream handed to
`hyper` afterwards is byte-for-byte the one it would have been with no bound at all and
`hyper_util::server::conn::auto::Builder`'s own `ReadVersion` sniff still does the HTTP/1.1-vs-h2
detection over a pristine socket. That is the whole reason the bound is a peek: wrapping the sniff
itself would mean reimplementing it behind a `Rewind`-shaped buffer. The TLS arm deliberately gets
no peek — `acceptor.accept` is already waiting on that connection's first bytes under the same
budget. Graph rule 45 correspondingly no longer rejects a non-default `handshake_timeout` on a
plaintext `otlp_in`; the value is live with or without `tls:`, and only the `0s` check still names
that kind (`docs/design/pipeline-graph.md`).

**No `header_read_timeout`, and the reason is worth recording.** The obvious next step — the
`hyper_util::rt::TokioTimer` + `http1().header_read_timeout(..)` pair `prometheus_out` already
installs on its own client-side `http1::Builder` — was verified against the pinned sources and
deliberately not taken. The API is available: hyper-util 0.1.20's `auto::Builder::http1()` returns
an `Http1Builder` that forwards `timer()`/`header_read_timeout()` to the inner `http1` builder and
whose `serve_connection` delegates straight back to the auto builder, so h2 auto-detection would
survive it. The *semantics* are the problem. In hyper 1.11.1 (`src/proto/h1/conn.rs`) the timer is
armed at the top of `poll_read_head`, before a single header byte has been parsed, and
`State::idle` sets `notify_read = true` whenever `h1_header_read_timeout.is_some()` — its own
comment reads "Next read will start and poll the header read timeout, so we can close the
connection if another header isn't received in a timely manner." It re-arms across every idle
keep-alive gap, which makes it an idle timeout wearing a first-head name, and a 5s one would close
a long-interval OTLP exporter's pooled connection between exports. Idle-connection timeouts across
all four listeners are held out for their own effort and ADR (`docs/known-gaps.md`'s
"no idle-connection timeout on a TCP listener"); half-building one here, per transport, is exactly
what that row exists to prevent. So the residual gap is now: one byte of a request head, or one
byte of an HTTP/2 preface under `protocol: grpc` (where `http2::Builder` has no such knob at all),
followed by silence.
