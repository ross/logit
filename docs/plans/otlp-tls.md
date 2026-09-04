---
created: 2026-09-03
updated: 2026-09-03
---

# Closing plan: TLS for `otlp_out`/`otlp_in`

## Context

[docs/plans/signal-filtering-and-otlp-out-config-gaps.md](signal-filtering-and-otlp-out-config-gaps.md)
filed "gRPC TLS is out of scope" as its own workstream, alongside `docs/known-gaps.md`'s
"`otlp_out` has no gRPC TLS" entry — `otlp_out`'s HTTP transport already speaks TLS today (via
`reqwest`'s default `rustls` backend), but its gRPC transport hard-rejects `https://` outright, and
`otlp_in` terminates nothing. See [ADR `otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md)
for the design decision this plan implements — most notably, that the gRPC client's connection
management moves to a pooled `hyper-util`/`hyper-rustls` client rather than layering `tokio-rustls`
onto the existing per-request hand-rolled connect (the known-gaps entry's own sketch), which would
have left the "opens a fresh connection per request" gap unaddressed while adding a full TLS 1.3
handshake to every request.

## What landed

- **`otlp_out`**: `tls:` config (`TlsClientConfig` — `ca_file`, `cert_file`+`key_file` for mutual
  TLS, `insecure_skip_verify`) on both transports. TLS itself is selected by `endpoint`'s
  `https://` scheme; `tls:` only tunes an already-TLS connection. The gRPC transport's
  `grpc_roundtrip` no longer drives a raw `TcpStream` — `OtlpOutput` carries a pooled
  `hyper_util::client::legacy::Client` over a `hyper-rustls` `HttpsConnector`, giving TLS and
  connection reuse together. `reject_insecure_grpc_endpoint`/`grpc_authority` are gone, replaced by
  `normalize_grpc_endpoint`.
- **`otlp_in`**: `tls:` config (`TlsServerConfig` — `cert_file`+`key_file` required,
  `client_ca_file` optional for mutual TLS) on both transports. The TLS handshake
  (`tokio_rustls::TlsAcceptor`) runs inside the per-connection spawned task, after that
  connection's `MAX_CONCURRENT_CONNECTIONS` permit is acquired, so a slow or hostile handshake
  can't stall the accept loop.
- **`logit-pipeline::graph::resolve`** rule 22: `cert_file`/`key_file` paired,
  `insecure_skip_verify` + `ca_file` rejected as contradictory, and a non-empty `tls:` under a
  plaintext endpoint rejected.
- **Test fixtures**: `testdata/tls/` (repo root) — a committed self-signed test CA, server/client
  leaves, an unrelated "wrong CA," and a `regen.sh` script, following the precedent
  ADR `committed-pregenerated-otlp-protobuf` set for generated-once-and-committed test assets. Both
  crates' unit tests, and `crates/logit-cli/tests/otlp_round_trip.rs`, now exercise real TLS
  handshakes (HTTP-over-TLS, gRPC-over-TLS, mutual TLS accept/reject, a plaintext client against a
  TLS-only listener) rather than only asserting scheme strings.
- **Dependencies**: `rustls`, `rustls-pki-types`, `tokio-rustls`, `hyper-rustls`, `webpki-roots`
  promoted to direct workspace dependencies, all already resolving in `Cargo.lock` at these exact
  versions transitively via `reqwest`'s existing `rustls-tls` feature — confirmed via
  `cargo tree -i aws-lc-rs` (nothing) and a `Cargo.lock` diff of only new dependency edges, no new
  package versions. `cargo deny check` passes with no `deny.toml` change.
- **Docs**: this plan; [ADR `otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md);
  `docs/design/pipeline-graph.md`'s validation list gained rule 22 (and, in passing, the
  previously-undocumented rules 20/21 from the headers/paths workstream); `docs/deploying.md`
  gained a TLS section; `docs/known-gaps.md`'s two TLS-shaped entries retired, two smaller ones
  (startup-only cert loading, no `server_name` override) filed; `AGENTS.md`'s "Current state"
  paragraph updated.

## Explicitly out of scope (filed in `docs/known-gaps.md`)

- **Certificate rotation.** Loaded once at `logit run` startup; a renewed cert needs a restart.
- **`server_name` override** for an endpoint reached by IP or through a proxy.
- **`syslog_out` TLS** (RFC 5425) stays unimplemented, but can now reuse `TlsClientConfig`/
  `TlsServerConfig` directly rather than re-deciding their shape.
- **`influxdb_out` TLS tuning** (a private CA for InfluxDB) — the same `TlsClientConfig` applies;
  not wired in this plan.
- **The demo (`demo/`) stays plaintext.** Tempo/Loki would need their own certificate generation
  in `compose.yaml`; a `demo/tls/` opt-in profile is a possible later workstream. Verified instead
  by hand: pointing a `tls`-configured `otlp_out` at a locally-TLS-terminated Tempo/Loki and
  confirming data lands, and that `logit.output.requests{class="network_error"}` stays flat with a
  single pooled connection across many gRPC batches.

## Verification

1. `script/test` (`cargo nextest run --workspace`) — 848 tests, all green, including every new
   TLS-specific test across `logit-outputs`, `logit-inputs`, and the `logit-cli` round-trip suite.
2. `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings` — clean.
3. `script/schema` — `schema/logit.schema.json` regenerated and committed.
4. `script/validate` — every shipped config (`demo/logit.yaml`, `examples/*.yaml`) still resolves.
5. `cargo deny check` — advisories/bans/licenses/sources all pass; `cargo tree -i aws-lc-rs` finds
   nothing.
6. Manual: `cargo run -p logit-cli -- graph demo/logit.yaml` still resolves (no `tls:` in the demo
   config, so this is a no-regression check on the unrelated-config path).
