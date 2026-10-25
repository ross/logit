---
created: 2026-10-25
updated: 2026-10-25
---

# Listener authentication: static bearer and basic credentials on the HTTP listeners, and a proxy for everything else

## Status
Accepted

## Context

Before this ADR, a listener's only identity check was the token its own protocol defines:
`datadog_in`'s `api_keys:` (`DD-API-KEY`), `splunk_hec_in`'s `tokens:` (`Authorization: Splunk`),
and `influxdb_in`'s `tokens:` (`Authorization: Token`). Server TLS with `client_ca_file` is mutual
TLS, which admits any client holding a certificate from that CA. Three HTTP surfaces had nothing
beyond that:

- `otlp_in`, whose senders (OTel SDKs, the Collector's `otlphttp` and `otlp` exporters) all send a
  configured `Authorization` header.
- `prometheus_in`'s remote-write receiver. Prometheus's `remote_write` and vmagent send bearer or
  basic credentials. `docs/known-gaps/prometheus.md` carried "The remote-write receiver has TLS but
  no authentication".
- `prometheus_out`'s exposition, which served the whole registry with no TLS and no credential
  check. A Prometheus scrape config sends bearer or basic credentials and scrapes `https`.
  `docs/known-gaps/prometheus.md` carried "`prometheus_out` has no TLS and no auth either".

[ADR `deployment-threat-model`](deployment-threat-model.md) leaves the trust boundary to the
operator, and network policy, mutual TLS, and a proxy still keep strangers out. A shared secret on
the request is a lighter tool than a CA, every one of these senders already speaks it, and
checking one is a header lookup and a comparison. It's also the first auth with no protocol of its
own behind it, so the set of mechanisms needed deciding before code.

## Decision

**The rule: a listener accepts the static credentials its protocol's standard clients send, so
a stock client configuration works against it.** An OTel SDK or Collector exporter, a Prometheus
`remote_write` block or scrape config, and vmagent each send a configured bearer token or basic
pair in `Authorization`, so those are what the three HTTP surfaces accept. A listener that stands
in for a vendor's endpoint authenticates the way that vendor does: `datadog_in`'s `api_keys:`,
`splunk_hec_in`'s `tokens:`, and `influxdb_in`'s `tokens:` stay as they are, and get no second
mechanism beside them.

**Static credentials only.** Each of the three HTTP surfaces takes an optional block of bearer
tokens and basic username and password pairs. The protocol tokens stay as they are, and mutual
TLS by CA (`client_ca_file`) stays the certificate-based option. Nothing else is built in.

| Component | Field | Mode |
|---|---|---|
| `otlp_in` | `auth:` | both transports |
| `prometheus_in` | `bind_auth:` | receiver only; graph rule 55 rejects it beside `scrape_targets:` |
| `prometheus_out` | `bind_auth:`, and new server TLS `bind_tls:` | exposition only; graph rule 56 rejects both beside `endpoint:` |

```yaml
auth:
  bearer_tokens: [!env OTLP_TOKEN]
  basic:
    - username: collector
      password: !env OTLP_PASSWORD
```

- **Matching.** An empty block accepts every request. Otherwise a request needs one
  `Authorization` header whose scheme (case-insensitive, RFC 9110 §11.1) is `Bearer` or `Basic`
  with that scheme's list non-empty, and whose credential matches one entry. A basic entry is
  compared as the client sends it, `base64(username:password)`, encoded once at startup, so a
  password may hold `:` and nothing is decoded per request. Every entry of the scheme is compared
  in constant time over its whole length. One copy of this lives in `logit_proto::auth`, beside
  the comparison the protocol tokens share, and has a fuzz target (`http_auth`).
- **Validation.** Graph rule 97 rejects an empty token, a token with surrounding whitespace, and
  an empty username or password, none of which a client could ever match. It also rejects a basic
  username containing `:`, which RFC 7617 forbids: `a:b` with password `c` encodes the same as `a`
  with password `b:c`, so it would alias another entry. A no-op block is legal.
- **The credential is checked before the body.** A refused request costs a header read, never a
  body read, a decompression, or a decode, so a sender with a bad credential can't spend
  `logit`'s CPU on its payload. The check runs after routing, so a wrong path is still a `404`.
- **A refusal.** HTTP gets `401` with one `WWW-Authenticate` challenge per configured scheme
  (`Basic realm="logit"`, then `Bearer`). An OTLP/gRPC call gets status `16`
  (`UNAUTHENTICATED`) in the trailers, as every other gRPC refusal on `otlp_in` is sent. A
  listener counts `logit.input.requests.rejected{reason="auth"}` and logs a throttled diagnostic
  naming the peer and why (no credential, a scheme it isn't configured for, or no match), never
  any part of the credential. `prometheus_in` also counts the write as
  `logit.input.writes{class="unauthorized"}` and logs under its existing `write_rejected` key.
  `prometheus_out` counts `logit.output.scrapes{class="unauthorized"}` and logs nothing, because a
  scraper retries on its interval and an unauthenticated probe would log forever. No refusal
  counts `*.dropped`: the sender still holds the data
  ([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)).
- **Secrets stay secret.** The config types' `Debug` prints counts only, and the credential never
  reaches a log line, a tag, or an event. Values come from `!env`. They're read at startup: to
  rotate, add the new value, restart, move the senders, remove the old one, and restart again.
- **`prometheus_out`'s `bind_tls:`** is `TlsServerConfig`, built and reloaded as every other
  listener's ([ADR `tls-certificate-reload`](tls-certificate-reload.md)), HTTP/1.1 only. It's
  prefixed by the mode it serves, as `prometheus_in`'s `scrape_tls:` and `bind_tls:` are, so a
  kind with two TLS roles never has a bare `tls:`.

**Through a proxy, not in `logit`:** OAuth2 and OIDC token validation, JWT and JWKS, token
introspection, AWS SigV4, Azure AD, Google IAM, matching a client certificate's subject or SAN,
and rate limiting. Each needs a key fetch, a clock, a policy language, or a vendor SDK, and each is
what an ingress, a service mesh, or an authenticating reverse proxy already does well.

OAuth2 client-credentials senders are on this list even though they send `Authorization: Bearer`:
Prometheus's `oauth2:`, vmagent's `-remoteWrite.oauth2.*`, and the Collector's `oauth2client`
extension. The token they send is minted by an identity provider and changes on every refresh, so
no static list can match it. Accepting one means validating it against that provider, which is
the token-validation work above.

**Not an auth gate:** Envoy's `X-Forwarded-Client-Cert` and the `Client-Cert` header (RFC 9440)
name a certificate a proxy terminated. They may become labels on an event, as `forwarded:`
records a client address. They never admit or refuse a request, since any client that can reach
the listener can write them.

**No auth on two surfaces:**

- **`admin:`** stays plaintext and unauthenticated. `/readyz` and `/healthz` serve a lifecycle
  phase to a kubelet or a local probe on loopback, a pod-local address, or a Unix socket
  ([ADR `admin-readiness-endpoint`](admin-readiness-endpoint.md)). A credential would have to be
  given to every probe, for a caller already inside the process's network namespace.
- **`datadog_trace_in`** gets nothing. dd-trace tracers send no credential to a local Agent's APM
  API and can't be configured to, so a check could only refuse every tracer.

## Alternatives considered

- **A JWT or OIDC validator in `logit`.** Rejected: key rotation, clock skew, issuer and audience
  policy, and a JWKS fetch are a project of their own, and a proxy in front does it for every
  listener at once.
- **Certificate subject or SAN matching.** Rejected for now: `client_ca_file` already decides who
  holds a certificate, and per-identity policy belongs to a mesh. An operator who needs two
  populations on one listener uses two CAs and two listeners.
- **An auth block on every listener, the protocol-token ones and the TCP ones included.** Rejected.
  The protocol listeners already check what their senders send, and a TCP or UDP wire has no
  header to carry a credential. Mutual TLS covers the stream listeners.
- **Reading credentials from a file, reloaded like certificates.** Open, for the maintainer to
  decide. Rotation is restart-based today: add the new value beside the old with `!env`, restart,
  move the senders, remove the old value, and restart again. A file reloaded on
  `tls_reload_interval:` and SIGHUP, as certificates are, would make that restart-free.
- **One `auth:` key on every kind.** Rejected for the two Prometheus kinds, which serve both a
  client and a server role. `bind_auth:` beside `bind_tls:` names the role, as the TLS keys do.

## Consequences

- Three known gaps close: the remote-write receiver's missing authentication, and the
  exposition's missing TLS and auth. The admin endpoint's entry goes too, because this ADR records
  its posture as a decision rather than a gap.
- `docs/deploying.md`'s "Listener authentication" is the operator-facing account, and its "Trust
  boundary" names the credential as one of the operator's tools.
- An OTLP exporter and Prometheus drop a batch that gets `401` rather than retrying it, where
  vmagent holds and retries. A credential change is checked against one sender before it rolls
  out, and the field docs say so.
- A browser OTel SDK can't keep a secret, so an `otlp_in` a web page sends to stays without
  `auth:`, behind the operator's own boundary.
- A new HTTP listener that takes a credential reuses `logit_proto::auth::Credentials` and
  `logit-inputs`' `AuthGate`, rather than parsing `Authorization` itself.
