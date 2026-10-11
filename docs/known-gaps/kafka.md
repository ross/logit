# Known gaps: Kafka

Entry format and the other areas: [the known-gaps index](README.md).

- **No SASL.** `kafka_out` authenticates only with TLS and a client certificate (`tls:`); it has
  no SASL PLAIN, SCRAM, OAUTHBEARER, or AWS MSK IAM ([ADR `kafka-output`](../adr/kafka-output.md),
  Consequences).
  - **Consequence:** a cluster that requires SASL, which includes most hosted Kafka services,
    refuses the connection, and the sink holds its queue.
  - **Workaround:** a listener on the cluster that takes mutual TLS, or a local relay that holds
    the SASL credentials.
  - **Revisit trigger:** the SASL follow-up the ADR lists, PLAIN and SCRAM first.
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
