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
- **The OTLP formats write no `otelcol.signal` header.** The OpenTelemetry Collector's Kafka
  exporter adds one naming the signal when its `signal_header` is on; `kafka_out` tells signals
  apart by topic only ([ADR `kafka-otlp-formats`](../adr/kafka-otlp-formats.md),
  Consequences).
  - **Consequence:** a consumer that reads the signal from that header, rather than from the
    topic, can't use a `topic:` that every signal shares.
  - **Workaround:** keep each signal on its own topic, the default, or add the header with
    `headers:` on a sink per signal, each behind `has_signal`.
  - **Revisit trigger:** a consumer that needs the header on a shared topic.
- **The OTLP formats have no byte parity with the exporter's resource keys.** The exporter's
  `partition_metrics_by_resource_attributes` and `partition_logs_by_resource_attributes` key by a
  hash of every resource attribute, computed by a package internal to the Collector.
  `key: {resource: NAME}` keys by one attribute's text instead
  ([ADR `kafka-otlp-formats`](../adr/kafka-otlp-formats.md), item 5).
  - **Consequence:** a `logit` sink and an exporter writing one topic place the same resource on
    different partitions.
  - **Workaround:** don't mix the two writers on one topic when consumers depend on one
    resource's records sharing a partition.
  - **Revisit trigger:** an operator who needs that placement shared.
- **The Collector's `kafkaexporter` places a trace id differently by default.** Its key bytes
  match `kafka_out`'s (32 lowercase hex characters), but its `record_partitioner` defaults to
  `sticky_key` with a Sarama-compatible FNV-1a hasher, where `kafka_out` hashes with the Java
  client's murmur2. Measured against Collector v0.162.0 with `partition_traces_by_id: true`: 14 of
  16 trace ids landed on a different partition, and with the exporter's `murmur2` hasher none did
  ([ADR `kafka-otlp-formats`](../adr/kafka-otlp-formats.md), item 9; [ADR
  `kafka-output`](../adr/kafka-output.md), item 3).
  - **Consequence:** a `logit` sink and a default exporter writing one topic split one trace's
    records across partitions.
  - **Workaround:** set `record_partitioner: {sticky_key: {hasher: murmur2}}` on the exporter, or
    don't mix the two writers on one topic when consumers depend on a trace sharing a partition.
  - **Revisit trigger:** an operator who needs `kafka_out` to match the exporter's default.
- **A refused signal topic holds every signal in the sink.** Each signal's topic settles on its
  own, but the sink has one queue, so a topic the broker refuses (authorization, a missing topic)
  holds the logs and spans behind it although their own topics accept writes
  ([ADR `kafka-otlp-formats`](../adr/kafka-otlp-formats.md), item 4).
  - **Consequence:** one misconfigured topic stops every signal of the sink until it's fixed.
  - **Workaround:** run one `kafka_out` per signal, each behind `has_signal` (and `keep_signals`
    when an event carries more than one signal). Each opens a producer for its own topic only.
  - **Revisit trigger:** a multi-topic producer, or a per-signal queue.
- **An OTLP record is lost whole.** The broker rejects a record whole, so a rejected
  `otlp_proto` or `otlp_json` record drops every log record, span, or metric in its request, where the other
  formats lose one event or one batch record
  ([ADR `kafka-otlp-formats`](../adr/kafka-otlp-formats.md), Consequences).
  - **Consequence:** `logit.output.records.dropped{reason="rejected"}` counts 1 for a request
    that held hundreds of spans; the events behind it aren't counted.
  - **Workaround:** a smaller `max_request_bytes` bounds how many events one request holds.
  - **Revisit trigger:** an operator who needs the loss counted per event.
- **A sink writing several signals holds a producer per topic.** Each signal's topic has its own
  producer, so a sink writing three signals holds up to three connections to each broker and
  three producer ids, and sends a broker three produce requests where one could carry every
  topic ([ADR `kafka-otlp-formats`](../adr/kafka-otlp-formats.md), item 3).
  - **Consequence:** more connections and producer ids per sink than the Java client or the
    Collector's exporter uses for the same topics.
  - **Revisit trigger:** a multi-topic producer, which per-event topics need too.
- **`kafka_in` speaks the classic group protocol only, with eager rebalancing.** It has no
  KIP-848 (`group.protocol=consumer`) and no cooperative-sticky assignor ([ADR `kafka-input`](../adr/kafka-input.md), items 2 and 4).
  - **Consequence:** every rebalance revokes every partition of every member, and the group stops
    until all of them rejoin, for up to `drain_timeout` plus the join. A Java member configured
    with `cooperative-sticky` alone can't share a group with `kafka_in`.
  - **Workaround:** keep `drain_timeout` short, and give `logit` a group of its own.
  - **Revisit trigger:** KIP-848, opt-in for Kafka 4.0 and later; Redpanda 26.2 doesn't serve it.
- **`kafka_in` has no static membership.** It sends no `group.instance.id` ([ADR `kafka-input`](../adr/kafka-input.md), Consequences).
  - **Consequence:** a restart rebalances the group twice, once as the member leaves and once as
    it joins, so a rolling restart of `n` members rebalances `2n` times.
  - **Revisit trigger:** rolling restarts whose rebalances cost an operator enough to matter.
- **`kafka_in` sends full fetches only.** Every Fetch is a full request that opens no KIP-227
  fetch session ([ADR `kafka-input`](../adr/kafka-input.md), item 7). The cost hasn't been measured with many partitions.
  - **Consequence:** each Fetch names every partition the member reads from that broker, so a
    member owning hundreds of partitions sends a larger request than the Java client would.
  - **Revisit trigger:** a measurement showing full fetches cost enough to matter.
- **`kafka_in` fetches from leaders only.** It has no rack-aware fetch from followers (KIP-392)
  ([ADR `kafka-input`](../adr/kafka-input.md), Consequences).
  - **Consequence:** in a cluster spread across zones, fetches cross zones when the leader is
    elsewhere, which some clouds bill.
  - **Revisit trigger:** a deployment where cross-zone fetch traffic costs enough to matter.
- **`kafka_in` subscribes to named topics only.** `topics:` lists names; there's no
  regular-expression subscription ([ADR `kafka-input`](../adr/kafka-input.md), Consequences).
  - **Consequence:** a topic created after startup isn't read until the config names it.
  - **Workaround:** list every topic, and restart to add one.
  - **Revisit trigger:** an operator whose topics come and go by a naming pattern.
- **A record relayed through `kafka_in` and `kafka_out` loses its key and headers.** `kafka_in`
  keeps them as the `_logit.kafka.key` and `_logit.kafka.headers` carriers, and `kafka_out` leaves
  every `_logit.kafka.*` carrier out and writes neither back ([ADR `kafka-input`](../adr/kafka-input.md), item 11).
  - **Consequence:** the relayed record goes to the partition `kafka_out`'s own `key:` picks, with
    its own `headers:`, so a consumer that partitions or routes on the source key or a header
    sees neither.
  - **Workaround:** a `set` or `lua` stage copies `_logit.kafka.key` to a public attribute that
    `key: {attribute: ..}` names.
  - **Revisit trigger:** `kafka_out` restoring both from the carriers.
- **`kafka_in` can't skip a record batch larger than its Fetch cap.** The consumer reads a Fetch
  answer of at most `fetch.max_bytes` plus `max_batch_bytes` plus 1 MiB. An answer over that is
  refused before any of it is read, so no batch header in it is seen and there is no offset to
  skip to; the next fetch from the same offset draws the same answer
  ([ADR `kafka-input`](../adr/kafka-input.md), item 9). Any batch whose stored bytes pass the
  cap does this, compressed or not. Each broker's fetch loop asks for all its ready partitions
  in one request, so the refusal holds every partition that broker leads for this consumer, not
  only the one holding the batch.
  - **Consequence:** those partitions hold at their offsets, counting
    `logit.input.broker.errors{api="fetch", error="response_too_large"}` with a throttled
    `response_too_large` diagnostic on every fetch, and none of their later records are read.
  - **Workaround:** raise `max_batch_bytes` above the batch's size.
  - **Revisit trigger:** a reader that skips a frame by its batch header alone, without holding
    the whole answer. Fetching that broker's partitions one per request after a refusal, so
    only the partition holding the batch stays held, is the open remedy short of that.
- **`kafka_in` skips a corrupt batch to the high watermark when nothing verified follows it.**
  A batch that fails its CRC twice, or doesn't decompress or parse, gives no trustworthy length
  or last offset, so the skip lands on the next batch in the answer whose CRC passes. When none
  follows even after a refetch with `max_batch_bytes` of room for that partition, the skip lands
  on the high watermark, or the last stable offset under `read_committed`
  ([ADR `kafka-input`](../adr/kafka-input.md), item 9).
  - **Consequence:** every record between the corrupt batch and the high watermark is counted
    `logit.input.records.dropped{reason="corrupt"}` and not read, good batches included, when no
    batch after the corrupt one lies within `max_batch_bytes` of it.
  - **Workaround:** none in `logit`; a log with corrupt batches needs repair at the broker.
  - **Revisit trigger:** a reader that can probe offsets past a corrupt batch, such as one
    ListOffsets or fetch per candidate offset, or a report of this loss in practice.
