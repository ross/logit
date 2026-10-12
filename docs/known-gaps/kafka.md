# Known gaps: Kafka

Entry format and the other areas: [the known-gaps index](README.md).

- **AWS_MSK_IAM reads a subset of the AWS default credential chain.** It takes credentials from
  the environment, a web identity token file, a profile's static keys in the shared files, the
  container endpoint, and IMDSv2, in that order. It doesn't run AWS SSO, `credential_process`,
  or a profile's `role_arn` chaining (`source_profile`, `credential_source`), and a profile that
  sets one of those keys is refused by name. IMDS is reached at `169.254.169.254` over IPv4
  only, with no IMDSv1 fallback and no `AWS_EC2_METADATA_SERVICE_ENDPOINT`
  ([ADR `kafka-sasl`](../adr/kafka-sasl.md), "Amendment: what W3 settled").
  - **Consequence:** a host whose only credentials come from an unsupported source never
    authenticates, and the sink holds its queue. An instance that requires IMDSv1, or reaches
    IMDS over IPv6 only, gets no instance role credentials.
  - **Workaround:** export the credentials into the environment (`aws configure
    export-credentials --format env`) or a profile's static keys from a wrapper, or attach the
    role through IRSA, EKS Pod Identity, ECS, or an IMDSv2 instance profile.
  - **Revisit trigger:** an operator whose credentials only one of those sources can supply.
- **AWS_MSK_IAM supports the `aws`, `aws-cn`, and `aws-us-gov` partitions only.** The ISO
  partitions have no MSK. The AWS European Sovereign Cloud (`aws-eusc`, `eusc-de-east-1`,
  under `amazonaws.eu`) offers MSK, but its broker host names are unverified, so `region:`
  rejects `eusc-de-east-1`, a broker host under `amazonaws.eu` gives no region, and web identity
  in a region outside the three partitions is refused rather than sent to an STS host that
  doesn't exist ([ADR `kafka-sasl`](../adr/kafka-sasl.md), "Amendment: what W3 settled").
  - **Consequence:** a sink in `aws-eusc` can't authenticate through IRSA, and needs the region
    from `AWS_REGION` to sign at all.
  - **Workaround:** none for IRSA. With credentials from another source, set `AWS_REGION`.
  - **Revisit trigger:** an operator running MSK in `aws-eusc`, with a broker host name to
    verify against.
- **AWS_MSK_IAM hasn't run against a live MSK cluster.** No broker outside AWS implements its
  server side, so `script/kafka-interop` has no leg for it. The signature is pinned by AWS's SigV4
  test suite and a golden payload from botocore, the payload's fields by `aws-msk-iam-auth`'s
  source, and the fake broker recomputes each signature
  ([ADR `kafka-sasl`](../adr/kafka-sasl.md), "Amendment: what W3 settled").
  - **Consequence:** a difference between MSK and the Java client's source, such as a field MSK
    checks that the source doesn't show, would surface only in production, as a `refused`
    authentication that holds the queue.
  - **Revisit trigger:** a run against a real MSK cluster, as the Splunk Cloud trial was run.
- **OAUTHBEARER gets a token two ways only: fixed, or the client-credentials grant.** There's no
  token file re-read on change, no JWT-bearer or private-key-JWT client authentication, and no
  client id and secret in the request body instead of HTTP Basic authentication
  ([ADR `kafka-sasl`](../adr/kafka-sasl.md), "Amendment: what W2 settled").
  - **Consequence:** an identity provider that only issues tokens another way, or a token that a
    sidecar rotates on disk, can't be used directly; a fixed `token:` stops working when it
    expires.
  - **Workaround:** a provider client that supports the client-credentials grant with Basic
    authentication, which every major provider does.
  - **Revisit trigger:** an operator whose provider needs another grant or client
    authentication method.
- **An OAUTHBEARER token endpoint must chain to a bundled Mozilla root.** The sink checks the
  endpoint's certificate against the bundled roots, and `tls:` applies to the brokers only
  ([ADR `kafka-sasl`](../adr/kafka-sasl.md), "Amendment: what W2 settled").
  - **Consequence:** an identity provider behind a private CA, such as an in-house Keycloak,
    fails the TLS handshake on every fetch, which is `clean` and retried with no end.
  - **Workaround:** front the endpoint with a certificate from a public CA, or run a relay on
    the `logit` host and point `token_endpoint` at it over `http://` to loopback.
  - **Revisit trigger:** an operator with a private-CA identity provider; the fix is a
    `token_endpoint_tls:` block built by `logit_pipeline::tls`.
- **Redpanda takes no OAUTHBEARER extensions.** Redpanda v26.2.3 reads everything between
  `auth=Bearer ` and the closing `\x01\x01` of the client message as the token, so an
  `extensions:` entry becomes part of it, the token fails to decode, and Redpanda closes the
  connection. `script/kafka-interop`'s Redpanda `oauth_static` sink sends none
  ([ADR `kafka-sasl`](../adr/kafka-sasl.md), "Amendment: what W2 settled", item 7).
  - **Consequence:** a `kafka_out` with `extensions:` against Redpanda never authenticates; each
    attempt is `clean` and retried with no end.
  - **Workaround:** leave `extensions:` out against Redpanda, which doesn't read them.
  - **Revisit trigger:** Redpanda parsing RFC 7628's key-value pairs.
- **Brokers older than Kafka 2.4 are unsupported.** The producer sends `ApiVersions` at v3 only,
  which Kafka 2.4 introduced, so version negotiation with an older broker fails
  ([ADR `kafka-output`](../adr/kafka-output.md), "Amendment: what W1 settled", item 5).
  - **Consequence:** every connection to such a broker fails, and the sink retries until shutdown.
  - **Revisit trigger:** none planned; Kafka 2.4 shipped in 2019.
- **The broker's duplicate detection doesn't survive a restart, a new producer id, or an epoch
  bump.** Within one producer epoch, a resend carries its first attempt's sequence, and the
  broker discards it if it already appended it. A `logit` restart mints a new producer id, and
  any partition batch that settles without being appended (a rejection, the named-record drop,
  the `MESSAGE_TOO_LARGE` split, or a batch dropped under `at_most_once`) bumps the epoch, which
  renumbers every unsettled batch ([ADR `kafka-output`](../adr/kafka-output.md), item 4).
  - **Consequence:** a batch whose first attempt landed before one of those events and whose
    retry went after it is stored twice. A `buffer.disk:` batch replayed after a restart is sent
    under the new producer id, so it lands again if it had landed before.
  - **Workaround:** deduplicate downstream, or set `buffer.delivery: at_most_once` and accept the
    loss instead.
- **Unkeyed records cluster per batch.** All of one batch's unkeyed records go to one partition,
  picked at random per batch, where the Java producer switches partitions every 16 KiB
  ([ADR `kafka-output`](../adr/kafka-output.md), item 2).
  - **Consequence:** a topic fed by one small, steady sink fills its partitions less evenly over a
    short window, and a consumer group's members see uneven load.
  - **Workaround:** set `key:` to a field with many values, or let the upstream batch size bound
    how much lands on one partition at a time.
- **A record stamped more than an hour ahead of the broker's clock is dropped.** Kafka 4.x
  rejects it under `log.message.timestamp.after.max.ms` (one hour by default), naming the record,
  and the sink drops it, counted `logit.output.records.dropped{reason="invalid_timestamp"}`, and
  delivers the rest of its batch ([ADR `kafka-output`](../adr/kafka-output.md), item 6).
  - **Consequence:** an event whose timestamp a `timestamp` stage resolved into the future, or
    from a sender with a fast clock, never reaches the topic.
  - **Workaround:** raise `log.message.timestamp.after.max.ms` on the topic, or fix the clock or
    the `timestamp` stage upstream.
- **Redpanda rejects a whole batch for one future-stamped record.** Redpanda 26.2 also refuses a
  record stamped more than an hour ahead with `INVALID_TIMESTAMP` (32), but names no record in
  `record_errors`, so `kafka_out` can't tell the bad record from the rest and rejects the whole
  partition batch, counted `logit.output.records.dropped{reason="rejected"}`
  ([ADR `kafka-output`](../adr/kafka-output.md), item 5). `script/kafka-interop`'s `timestamp`
  leg shows it: against Apache Kafka 4.2.0, one record in each batch of 10 is dropped; against
  Redpanda v26.2.3, all 10.
  - **Consequence:** on Redpanda, one future-stamped event loses every record that shares its
    partition batch, counted, never silently.
  - **Workaround:** fix the clock or the `timestamp` stage upstream, or raise the topic's
    `message.timestamp.after.max.ms` (the cluster's `log_message_timestamp_after_max_ms`), one
    hour by default, as on Kafka.
  - **Revisit trigger:** Redpanda filling `record_errors`, or a decision to split a batch that a
    broker rejects without naming records.
- **A `text` batch record can't tell a newline inside a body from the one between bodies.**
  `format: text` with `record: batch` joins the log bodies with `\n`, with no escaping or length
  prefix ([ADR `kafka-output`](../adr/kafka-output.md), "Amendment: what W3 settled", item 5).
  - **Consequence:** a consumer splitting the record on newlines reads a multiline body, such as
    a stack trace, as several bodies.
  - **Workaround:** `record: event` for a multiline source, or `format: json`, whose lines escape
    the newline.
- **A `native` batch record over the cap costs several encodes.** A native frame's dictionary is
  shared by its events, so its size isn't the sum of theirs, and `record: batch` finds each
  record's longest run of events that fits by encoding candidate frames: the whole batch once,
  then, for each record, doubling and bisecting over the run length
  ([ADR `kafka-output`](../adr/kafka-output.md), "Amendment: what W3 settled", item 3).
  - **Consequence:** a batch that fits one record costs one encode. One that needs several
    records costs, per record, about twice the logarithm of its event count in encodes, each of
    up to twice the record's size.
  - **Workaround:** keep upstream batches under `max_request_bytes`, or raise the cap.
