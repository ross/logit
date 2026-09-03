---
created: 2026-09-03
updated: 2026-09-03
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
