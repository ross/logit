---
created: 2026-10-25
updated: 2026-10-25
---

# TLS client `server_name`: verify the server's certificate against a configured name, and leave SNI alone

## Status
Accepted

## Context
Every TLS client in `logit` verified the server's certificate against the host it dialed: each
sink's endpoint host, `prometheus_in`'s scrape target host, and `kafka_out`'s advertised broker
host. Two common deployments can't pass that check:

- An endpoint dialed by IP address, whose certificate names a DNS host and has no IP SAN.
- A proxy or load balancer whose certificate names a host other than the one in the endpoint.

OpenTelemetry's equivalent setting, `tls.server_name_override`, sets both the verification name and
the SNI the client sends.

Every client builds its `rustls::ClientConfig` through one function,
`logit_pipeline::tls::build_client_config`, from the shared `tls:` block
(`logit_config::TlsClientConfig`), so a setting there reaches every client at once. The clients
don't all control SNI equally, though:

- The `reqwest` clients (the shared HTTP driver in `crates/logit-outputs/src/http.rs` and
  `prometheus_in`'s scrape client) can't set SNI. `reqwest` 0.12.28 takes SNI from the connection
  URI's host (`ServerName::try_from(dst.host())` in its `connect.rs`), offers only
  `tls_sni(bool)`, and wraps the URI in a type no connector layer can rewrite.
- The raw-TCP sinks (`logit_out`, and `syslog_out` and `statsd_out` through `stream.rs`),
  `kafka_out`, and `otlp_out`'s gRPC transport pass a `ServerName` to `connect` themselves, so they
  could override SNI directly.

## Decision
`tls.server_name` (and the same key under `scrape_tls:` and `endpoint_tls:`) sets the name the
server's certificate is verified against, on every TLS client, and nothing else. SNI stays the
dialed host on every transport, and none for an IP endpoint.

- `build_client_config` parses the name at startup as a DNS name or an IP literal, and fails startup
  on one that doesn't parse, naming `tls.server_name`.
- It wraps the server-certificate verifier in `VerifyAsServerName`, which passes the configured
  name to the inner verifier's `verify_server_cert` in place of the dialed one and delegates every
  other method. The wrapper sits over the `ReloadingServerVerifier` for a `ca_file`, so a rotated CA
  still reaches it. For the bundled Mozilla roots it sits over a `WebPkiServerVerifier`, built only
  when `server_name` is set, so a config without one is built as before.
- Under `insecure_skip_verify` the field is ignored and isn't parsed. Validation doesn't reject the
  combination, because a pointless config isn't an invalid one.
- On `kafka_out` every broker is verified against the one name.
- The resumption cache stays keyed by the dialed name. A resumed session skips verification, but
  every session in a config's cache was verified against that config's one `server_name`.
  A test dials twice with one config and asserts the second handshake is resumed.

## Alternatives considered
- **Set SNI and the verification name, as OTel does.** The non-`reqwest` clients could do it
  directly. The `reqwest` clients could do it only with a workaround: a request URL whose host is
  the `server_name`, an explicit `Host` header carrying the real authority, and a custom
  `dns_resolver` that maps the `server_name` to the real endpoint. That's a lot of moving parts in
  the request path for a case nobody has asked for.
- **Set SNI where the transport allows it, and verification only elsewhere.** Rejected: one field
  in the shared `tls:` block would mean different things on different sinks, and an operator moving
  a config from `logit_out` to `otlp_out` over HTTP would get a different handshake with no error.

## Consequences
- An endpoint dialed by IP, or a proxy whose certificate names another host, verifies with
  `server_name` on every client.
- A front end that routes on SNI and is reached by IP still can't be reached by name: the client
  sends no SNI for an IP, and the dialed host's otherwise. `docs/known-gaps/intake.md`'s "TLS and
  connection lifecycle" section tracks it, with the `reqwest` workaround above as the way to close
  it once there's a use case.
