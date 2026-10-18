---
created: 2026-10-11
updated: 2026-10-17
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
the producer dials a new one, which is `Clean`. One attempt can write several produce requests to
a broker in turn, so the deadline is checked again before each one (`Conn::past_deadline`). Once
it has passed, the partition batches not yet written are `Clean` and the connection is dropped, so
the next attempt dials a new one. A lifetime of `0` means no deadline. Each redial counts
`logit.output.reconnects`, so the renewal rate shows in the sink's telemetry.

This is safe because `kafka_out` keeps one request in flight per broker, no request is written
past the deadline, and the `timeout` margin means a request written before the deadline is
answered or failed before the session ends. A broker that closes one mid-request anyway gives an
`Ambiguous` failure, and the batch's idempotent sequences make its resend safe.

### 4. Fault classes

These rows join the "Response classes" table in the `kafka_out` module doc, the canonical copy
([ADR `kafka-output`](kafka-output.md), item 5), each backed by a fake-broker test:

| Outcome | Class | Why |
|---|---|---|
| `UNSUPPORTED_SASL_MECHANISM` (33) | `Refused` | The broker doesn't offer the configured mechanism. Only a config or broker change fixes it. |
| `ILLEGAL_SASL_STATE` (34) | `Refused` | The exchange broke the broker's state machine; a retry repeats the same exchange. |
| `SASL_AUTHENTICATION_FAILED` (58) | `Refused` | Wrong credentials, or a token the broker won't take. |
| A transport failure during the exchange | `Clean` | No produce byte has been written on this connection. |
| A token, STS, IMDS, or container credential endpoint unreachable, or answering `429` or `5xx` | `Clean` | A transient fault upstream of the broker. |
| One of those endpoints answering any other `4xx` (`invalid_client`, a bad role) | `Refused` | Its credentials or config are wrong. |

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

## Amendment: what W1 settled (2026-10-11)

W1 built PLAIN and SCRAM and settled these points the decision above left open:

1. **The trait splits per connection.** `Authenticator` is shared by a sink's connections and
   has `mechanism()`, `principal()` (the user for messages, never a secret), and an async
   `start(host)` that returns a `Session`. A `Session` is synchronous: `first()` writes the first
   client message, and `next(server)` reads each broker answer and returns the next message or
   `Done`. OAUTHBEARER and AWS MSK IAM do their fetching in `start`, cached in the
   authenticator, and AWS MSK IAM reads the region from `start`'s host. An error from `start` is
   `Clean` unless it carries a class, so an unreachable token endpoint retries; an error from a
   session is `Refused` unless it carries one, because a server message the mechanism rejects is
   rejected the same way on every connection. An exchange that hasn't finished after 8
   `SaslAuthenticate` round trips is `Refused`.
2. **A short session lifetime keeps half of itself.** The deadline is the lifetime minus
   `timeout`, measured from the moment before the last `SaslAuthenticate` was written, which is
   no later than the broker's own start of the session. When the lifetime is at most twice
   `timeout`, the margin is half the lifetime instead, so the connection still carries requests.
   Section 3's guarantee then doesn't hold: a request written near the deadline can outlast the
   session, and the broker closes the connection. That failure is `Ambiguous`, and the idempotent
   resend is deduplicated.
3. **SCRAM takes 4096 to 1048576 iterations.** The floor is Kafka's and Redpanda's minimum. The
   ceiling only bounds the PBKDF2 work a broken server message can ask of the connecting task:
   Kafka stores no credential above 16384 and Redpanda sets no maximum, so a real broker is never
   refused. A server-first message with a
   mandatory extension (`m=`) is refused, as RFC 5802 requires.
4. **Two more `Refused` outcomes.** With `sasl:` set, a broker that doesn't offer
   `SaslHandshake` v1 and a `SaslAuthenticate` version from v0 to v2 is `Refused`, as a broker
   without `Produce` v3 is. `UNSUPPORTED_VERSION` (35) from either SASL request is `Refused`
   beside codes 33, 34, and 58, and any other code is `Clean`, as `kafka_out` reads the codes of
   a `Metadata` or `InitProducerId` answer.
5. **Graph rule 85 also rejects an empty `sasl.username`,** which no broker takes.
6. **`Debug` leaves the password out** of both `logit_config::KafkaSasl` and the sink's own
   `KafkaSasl`, so printing a component or the sink's settings can't leak it.

## Amendment: what W2 settled (2026-10-11)

W2 built OAUTHBEARER and settled these points the decision above left open:

1. **The config is flat.** `oauthbearer` takes optional `token`, `token_endpoint`, `client_id`,
   `client_secret`, `scope`, and `extensions` fields rather than a nested enum, so the YAML reads
   as section 1 shows. Graph rule 85 adds: `client_id` and `client_secret` are required with
   `token_endpoint`, and none of `client_id`, `client_secret`, or `scope` may come with `token`;
   a fixed `token` must be an RFC 6750 `b64token`; and each extension key is letters only and
   not `auth`, and each value is non-empty printable ASCII, space, tab, CR, or LF (KIP-342's
   patterns). `Debug` leaves out the token, the client secret, and the extension values.
2. **The token request follows the Java client's default.** It's a form `POST` of
   `grant_type=client_credentials` and the optional `scope`, with the client id and secret in
   HTTP Basic authentication as they are. RFC 6749 §2.3.1 asks for each to be form-urlencoded
   first, but Kafka 4.2.0's client does that only when `sasl.oauthbearer.header.urlencode` is
   `true`, and its default is `false` (KIP-1025), so a provider that works with a Java client at
   its defaults works with `kafka_out`. The two forms differ only for an id or secret with a
   character outside letters, digits, and `*-._`. The `scope` is form-urlencoded in the body,
   which the Java client sends unencoded at its default; the two agree on any scope without a
   space or a reserved character.
3. **Reuse and refresh.** A fetched token is reused by every connection until 80% of its
   `expires_in` has passed, measured from before the request. Without `expires_in`, a JWT is
   reused until 80% of the time to its `exp` claim, and any other token isn't reused. Dials that
   start together wait on one fetch, behind an async mutex in the authenticator. A broker's
   refusal drops a fetched token from the cache, so the next dial fetches a new one instead of
   resending a token the broker refused until its refresh time. `authenticate` tells the session
   of any error code on a `SaslAuthenticate` through a `Session::refused` method, so a refusal
   with no error challenge, as Redpanda sends, drops the token too.
4. **The error challenge's status reaches the error.** On a refused token, Kafka answers the
   first message with an RFC 7628 JSON error and no error code; the session answers with a lone
   `\x01`, and the broker then fails the exchange with `SASL_AUTHENTICATION_FAILED`. The session
   keeps the JSON's `status` (and `scope`), and `authenticate` appends it to the broker's
   message through a `Session::failure` method, so the error reads `status "invalid_token"`
   whatever text the broker sends with code 58.
5. **The token endpoint's faults.** Section 4's rows hold. A redirect is `Refused`, since the
   client follows none and a moved endpoint is a config change. A `2xx` whose body, read up to
   a 64 KiB cap, isn't JSON, lacks a string `access_token`, or holds one that isn't an RFC 6750
   `b64token` is `Refused`: every fetch gets the same answer, and a token outside `b64token` could carry a
   `\x01` into the message's framing. A `2xx` whose body the timeout or a closed connection cuts
   off is `Clean`, as a timed-out request is, rather than read as a body that isn't JSON. The
   fetch is bounded by the sink's `timeout`, and an error quotes only
   the OAuth `error` and `error_description` of the body, never the rest, which may echo the
   request.
6. **The token endpoint trusts the bundled Mozilla roots only.** The broker `tls:` block
   doesn't apply to it, and there's no separate one, so an identity provider behind a private CA
   can't be used (`docs/known-gaps/kafka.md`). A `token_endpoint_tls:` block would close the gap
   with no change to the rest of this design.
7. **Redpanda's OAUTHBEARER is verified too.** Redpanda documents OIDC as an Enterprise feature,
   but a new cluster runs on a 30-day trial license ("Redpanda Built-In Evaluation Period"), and
   `script/kafka-interop` starts a new one every run. So the harness turns OAUTHBEARER on against
   both brokers, and a Redpanda row reads `GAP` only when Redpanda refuses the configuration,
   rather than always, as the Consequences above expected. Redpanda v26.2.3 differs from Kafka
   4.2.0 in two ways the sink allows for: it reads everything between `auth=Bearer ` and the
   closing `\x01\x01` as the token, so a configured extension corrupts the token and Redpanda
   drops the connection (`Clean`, retried; `docs/known-gaps/kafka.md`), and it refuses a token
   with `SASL_AUTHENTICATION_FAILED` at once, with no error challenge, so its refusal carries no
   OAuth status. Kafka 4.2.0 sends the challenge's JSON again as the code-58 message.

## Amendment: what W3 settled (2026-10-12)

W3 built AWS_MSK_IAM and settled these points the decision above left open:

1. **The payload is `aws-msk-iam-auth`'s, field for field.** `AWS4SignedPayloadGenerator` presigns
   `GET https://<broker host>/?Action=kafka-cluster:Connect` through the AWS SDK for Java v2's
   `Aws4Signer.presign`, then writes each query parameter as a JSON key in lowercase with its
   value as it is (not percent-encoded), beside `version`, `host`, and `user-agent`. The
   signature covers `host` alone and the SHA-256 of an empty payload, not `UNSIGNED-PAYLOAD`,
   and a session token is in the signed query, as the SDK's presigner puts it there before
   signing. `host` is the broker's host as the connection names it, with no port. The
   `user-agent` is `logit/<version>`; the Java client's names itself, and MSK doesn't check it.
2. **The broker's answer isn't parsed.** MSK answers with JSON holding `version` and
   `request-id`, and the Java client checks the version. `kafka_out` ends the exchange on any
   answer that isn't empty: the broker has already checked the signature, and an error code is
   how it refuses one.
3. **The vectors.** `sigv4`'s tests pin header-signed and presigned signatures from AWS's SigV4
   test suite as `awslabs/aws-c-auth` carries it (commit
   `84a26ea184d9ccd322c8164759510b3604ab13fd`: `get-vanilla`, `get-vanilla-query-order-key-case`,
   `get-utf8`, `get-vanilla-utf8-query`, `get-vanilla-query-unreserved`,
   `get-vanilla-with-session-token`, `post-sts-header-before`, and `post-sts-header-after`).
   `msk_iam`'s tests pin two golden payloads, with and without a session token, whose signatures
   botocore 1.40.40's `SigV4QueryAuth` computed with its clock frozen, over an MSK broker host;
   botocore 1.43.112 and a standard-library re-derivation agree. The fake broker recomputes each
   payload's signature with the same signer, and the golden signatures keep a mistake both sides
   share from passing.
4. **The region.** `region:` when set; else the label right before `.amazonaws.com`,
   `.amazonaws.com.cn`, or `.api.aws` at the end of the broker's host, when it has a region's
   shape, which covers provisioned (`*.kafka.<region>.amazonaws.com`) and serverless
   (`*.kafka-serverless.<region>...`) hosts. `aws-msk-iam-auth`'s `RegionUtils` reads every
   partition's DNS suffix from the SDK's metadata; `kafka_out` reads those three, the `aws`,
   `aws-cn`, and `aws-us-gov` partitions' domains. The ISO partitions have no MSK, and the AWS
   European Sovereign Cloud (`aws-eusc`, `amazonaws.eu`) has MSK but unverified broker host
   names, so neither is supported (`docs/known-gaps/kafka.md`);
   else `AWS_REGION`, then `AWS_DEFAULT_REGION`, which the Java SDK doesn't read but the AWS CLI
   does. With none, the exchange is `Refused`. Graph rule 85 checks a set `region:`'s shape: a
   two-letter prefix, one or more lowercase words, and a number. There's no `profile:` field:
   `AWS_PROFILE` picks the profile, as it does for every AWS tool.
5. **The credential chain's order is `aws-msk-iam-auth`'s.** Its `MSKCredentialProvider` builds
   the Java SDK v2 providers as environment, system properties, web identity, profile, container,
   and instance profile, which differs from the SDK's own `DefaultCredentialsProvider` only in
   putting the environment before system properties. `logit` has no system properties, so the
   chain is the five sources of section 5, in that order:
   - Web identity calls STS at the broker's region (`sts.<region>.amazonaws.com`, or
     `.amazonaws.com.cn` in China) with an unsigned form `POST`. A region outside the three
     supported partitions is `Refused` rather than given an `amazonaws.com` STS host that
     doesn't exist. Signing needs no partition: the region is only in the credential scope. The session name is
     `AWS_ROLE_SESSION_NAME`, else `logit-<milliseconds>`, as the Java SDK's is
     `aws-sdk-java-<milliseconds>`. A minimal parser reads `AccessKeyId`, `SecretAccessKey`,
     `SessionToken`, `Expiration`, and an error's `Code` and `Message` as leaf elements, from at
     most 64 KiB of the answer.
   - The shared files follow the Java SDK's parser: `[profile name]` or `[default]` in the config
     file, where `[profile default]` wins, `[name]` in the credentials file, which wins a key
     both set, and `#` or `;` comments.
   - The container endpoint takes `AWS_CONTAINER_AUTHORIZATION_TOKEN` over the file
     `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` names when both are set, as the Java SDK does, and
     a full URI that is `https`, or `http` to a loopback address, `169.254.170.2`,
     `169.254.170.23`, or `fd00:ec2::23`.
   - IMDS is IMDSv2 only, at `169.254.169.254`, with a 6-hour session token. The Java SDK falls
     back to IMDSv1 when the token request fails; `kafka_out` doesn't, and lists it in
     `docs/known-gaps/kafka.md`.
   - The container endpoint and IMDS are asked with an HTTP client that ignores every proxy
     setting, as the Java SDK opens both with `Proxy.NO_PROXY`: a proxy would see the container
     authorization token and the credentials in cleartext, and can't reach a link-local
     address anyway. STS's request honors `HTTPS_PROXY` and `NO_PROXY`, as the Java SDK's STS
     client uses its configured HTTP client.
   - The IMDS, container, and STS base URLs move only in tests, through a constructor parameter,
     and the environment is read through an injected function, so no test sets a process
     variable another reads.
6. **A source that is present and fails ends the lookup.** The Java chain catches each
   provider's failure and tries the next. `kafka_out` skips a source only when its variables or
   profile aren't there, or when the profile is the implicit `default` and holds no keys, as
   the `region`-only `[default]` that `aws configure` writes does; once a source is there, its
   answer is final, so a profile `AWS_PROFILE` names without keys is `Refused`. So IRSA whose STS call is throttled
   is retried as `Clean` instead of authenticating as the node's instance role, and a profile
   using `role_arn`, `credential_process`, or SSO is refused by name instead of skipped. With
   every source absent, IMDS is still asked, and an unreachable IMDS is `Clean`, as section 4's
   row says; the error lists every source looked for. With `AWS_EC2_METADATA_DISABLED=true` as
   well, it's `Refused`.
7. **Caching and renewal.** Credentials with an expiry (web identity, the container endpoint,
   IMDS) are reused by every connection until 5 minutes before it, behind one async lookup as
   OAUTHBEARER's token is. The environment and the files have no expiry and are read again on
   every dial. MSK sets a session's lifetime to the credentials' expiry, so the redial of
   section 3 happens before it with credentials already renewed. A broker's refusal drops
   fetched credentials from the cache through `Session::refused`, as it drops a token.
   `authenticate` asks the authenticator for its principal again after `start`, so a refusal
   names where the credentials came from (`AWS credentials from the environment`).
8. **STS's transient codes.** STS answers throttling with `400` and the code `Throttling`, and
   a web identity provider it can't reach with `400` and `IDPCommunicationError`, which the AWS
   SDKs retry. Both are `Clean`; every other `4xx` is `Refused`.
9. **The producer's IAM policy needs `WriteDataIdempotently`.** MSK's IAM authorizer doesn't
   apply KIP-679's rule that `WriteData` on a topic implies idempotent writes, so the default
   idempotent producer needs `kafka-cluster:WriteDataIdempotently` on the cluster beside
   `Connect`, `DescribeTopic`, and `WriteData` (`docs/deploying.md`).
