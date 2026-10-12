---
created: 2026-10-11
updated: 2026-10-11
---

# `kafka_out` SASL: PLAIN, SCRAM, OAUTHBEARER, and AWS MSK IAM, authenticated in the dialer and renewed by redial

## Status
Accepted

## Context

`kafka_out` authenticates only with TLS and a client certificate ([ADR
`kafka-output`](kafka-output.md), item 10). Most hosted Kafka requires SASL instead: Confluent
Cloud takes PLAIN, Amazon MSK takes SCRAM or IAM, and Aiven and Redpanda Cloud take SCRAM. Such a
cluster refuses the sink's connection, and the sink holds its queue
(`docs/known-gaps/kafka.md`, "No SASL").

Every piece this needs is already in the dependency graph. `kafka-protocol` 0.18 generates
`SaslHandshake` (v0 to v1) and `SaslAuthenticate` (v0 to v2), whose v1 response carries
`session_lifetime_ms`. `ring` 0.17 (rustls's crypto provider) has HMAC-SHA-256/512, PBKDF2,
SHA-2, and `SystemRandom`. `base64` 0.22 and `reqwest` 0.12 are workspace dependencies.

The work lands as stream `ksasl`: W1 adds the seam, PLAIN, and SCRAM; W2 adds OAUTHBEARER; W3 adds
AWS MSK IAM.

## Decision

`kafka_out` takes an optional `sasl:` block and authenticates every broker connection in
`Dialer::connect`, after `ApiVersions` and before the connection is returned. A session that the
broker gives a lifetime is renewed by dialing a new connection, not in-band.

### 1. Config

`sasl:` is an enum internally tagged on `mechanism`, each variant `deny_unknown_fields`.
Secrets come from `!env`:

```yaml
sasl: {mechanism: plain, username: u, password: !env KAFKA_PASSWORD}
sasl: {mechanism: scram_sha_256, username: u, password: !env KAFKA_PASSWORD}  # or scram_sha_512
sasl: {mechanism: oauthbearer, token: !env KAFKA_TOKEN}                      # static token
sasl: {mechanism: oauthbearer, token_endpoint: https://idp.example/token,
       client_id: c, client_secret: !env CLIENT_SECRET, scope: kafka,
       extensions: {logicalCluster: lkc-1}}                                   # client credentials
sasl: {mechanism: aws_msk_iam, region: us-east-1}                             # region optional
```

Graph rule 85 adds two checks. `oauthbearer` takes one of `token` or `token_endpoint`, never both
and never neither. `token_endpoint` is `https`, or `http` to a loopback address only.

SASL without `tls:` is allowed, as the Java client's `SASL_PLAINTEXT` is. PLAIN then sends the
password in clear, and the operator docs say so.

No error, log, or diagnostic prints the `sasl:` block. A message names the component, the
mechanism, and the username, and nothing else from the block (AGENTS.md's config-printing rule).

### 2. Where the exchange runs

`Dialer` gains `sasl: Option<Arc<dyn Authenticator>>`. With one set, `Dialer::connect` sends
`SaslHandshake` v1 naming the mechanism, then one or more `SaslAuthenticate` requests at the
highest version both sides support, up to v2, until the mechanism finishes. A `kafka/sasl/`
module holds the trait, the exchange loop, and one file per mechanism. The authenticator is async,
because OAUTHBEARER and AWS MSK IAM fetch tokens and credentials over HTTP, and it caches what it
fetches across connections.

### 3. Re-authentication by redial

A broker with `connections.max.reauth.ms` set answers `SaslAuthenticate` v1 or later with a
`session_lifetime_ms`, and closes a connection that sends anything but a re-authentication after
that lifetime ends (KIP-368). `Conn` records a deadline: the lifetime minus the sink's `timeout`.
The producer's reuse check (`Conn::is_open`) reads a connection past its deadline as closed, so
the producer dials a new one, which is `Clean`. A lifetime of `0` means no deadline.

This is safe because `kafka_out` keeps one request in flight per broker, so no request is written
into a connection the broker is about to close for expiry. A broker that closes one mid-request
anyway gives an `Ambiguous` failure, and the batch's idempotent sequences make its resend safe.

### 4. Fault classes

These rows join the "Response classes" table in the `kafka_out` module doc, the canonical copy
([ADR `kafka-output`](kafka-output.md), item 5), each backed by a fake-broker test:

| Outcome | Class | Why |
|---|---|---|
| `UNSUPPORTED_SASL_MECHANISM` (33) | `Refused` | The broker doesn't offer the configured mechanism. Only a config or broker change fixes it. |
| `ILLEGAL_SASL_STATE` (34) | `Refused` | The exchange broke the broker's state machine; a retry repeats the same exchange. |
| `SASL_AUTHENTICATION_FAILED` (58) | `Refused` | Wrong credentials, or a token the broker won't take. |
| A transport failure during the exchange | `Clean` | No produce byte has been written on this connection. |
| A token, STS, IMDS, or container credential endpoint unreachable, or answering `5xx` | `Clean` | A transient fault upstream of the broker. |
| One of those endpoints answering `4xx` (`invalid_client`, a bad role) | `Refused` | Its credentials or config are wrong. |

A refused authentication holds the queue and retries with no budget, as a failed TLS handshake
does today. Nothing drops.

### 5. Mechanisms

- **PLAIN** (RFC 4616): one message, `\0username\0password`.
- **SCRAM-SHA-256 and SCRAM-SHA-512** (RFC 5802, RFC 7677):
  - The client nonce comes from `SystemRandom`, and the username escapes `=` and `,` as RFC 5802
    requires.
  - There's no SASLprep. The Java client sends the username and password as UTF-8 unmodified, and
    a broker's stored credentials are derived the same way, so applying SASLprep would break a
    non-ASCII password that works with Java.
  - The client verifies the server signature and refuses an iteration count below 4096, the
    floor that RFC 7677 and Kafka's own SCRAM implementation set.
  - RFC 7677's SCRAM-SHA-256 example pins it in a unit test.
- **OAUTHBEARER** (RFC 7628, KIP-255, KIP-342, KIP-768):
  - The message is the GS2 header, `auth=Bearer <token>`, and each extension as `key=value`,
    separated by `\x01`.
  - A static `token` is sent as given.
  - Under `token_endpoint`, the authenticator POSTs an OAuth 2.0 client-credentials request with
    `reqwest`, caches the access token, and refreshes it at 80% of `expires_in`, the Java
    client's `sasl.login.refresh.window.factor` default. A connection dialed after the token
    expires gets a fresh one.
  - When the broker refuses a token, it answers with an RFC 7628 JSON error, and the sink's error
    carries that JSON's `status`.
- **AWS_MSK_IAM**: the JSON payload that `aws-msk-iam-auth` sends.
  - Fields: version `2020_10_22`, action `kafka-cluster:Connect`, and a SigV4 presigned query
    for service `kafka-cluster` with `x-amz-expires` 900.
  - SigV4 is hand-rolled on `ring`, tested against AWS's published SigV4 test suite and a golden
    payload.
  - The region is `region:` when set, else the one in the broker's MSK hostname
    (`*.kafka.<region>.amazonaws.com`), else `AWS_REGION`.
  - Credentials come from a hand-rolled subset of the AWS default chain, first source wins, cached
    until 5 minutes before they expire:
    1. Environment: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`.
    2. Web identity: `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`, through one STS
       `AssumeRoleWithWebIdentity` call (EKS IRSA).
    3. Shared files: `AWS_PROFILE`'s static keys from `~/.aws/credentials` and `~/.aws/config`.
    4. Container endpoint: `AWS_CONTAINER_CREDENTIALS_FULL_URI` or
       `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, with the authorization token or its file (ECS and
       EKS Pod Identity).
    5. IMDSv2, with its session token.

## Alternatives considered

- **The AWS SDK (`aws-config`, `aws-sigv4`, `aws-credential-types`).** Rejected: it brings a
  large dependency tree for one signature and a credential lookup, and its HTTP and TLS stack
  would need pinning to rustls on `ring` to keep `aws-lc-rs` and a second TLS client out of the
  build. The subset it would add over the hand-rolled chain (SSO, `credential_process`, profile
  role chaining) is listed in Consequences.
- **In-band re-authentication (KIP-368).** Rejected: the connection must stop sending produce
  requests while a second `SaslHandshake` and `SaslAuthenticate` run on it, which adds a second
  state to every connection the producer holds. A redial costs one connect and exchange per
  session lifetime and reuses the dial path every connection already takes.
- **librdkafka.** Already rejected by [ADR `kafka-output`](kafka-output.md); its SASL support
  doesn't change any of the reasons.

## Consequences

- No new crate enters `Cargo.lock`. W1 makes `ring` a workspace dependency at the version
  `Cargo.lock` already holds, and `ring` and `base64` direct dependencies of `logit-outputs`.
- Unsupported, and listed in `docs/known-gaps/kafka.md` once the stream lands:
  - AWS SSO, `credential_process`, and `role_arn` chaining in profiles;
  - GSSAPI (Kerberos);
  - an OAUTHBEARER token from anything but a static value or a client-credentials grant.
- `script/kafka-interop` verifies OAUTHBEARER against Apache Kafka only: Redpanda's OIDC support
  is an Enterprise feature, so its row reads `GAP`.
- AWS MSK IAM is verified against fixtures and AWS's test vectors. A live MSK run is optional, as
  the Splunk Cloud trial was.
- A connection to a broker that sets `connections.max.reauth.ms` is redialed once per session
  lifetime, which adds a connect, an optional TLS handshake, `ApiVersions`, and the SASL exchange
  to that period.
