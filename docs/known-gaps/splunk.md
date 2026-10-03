# Known gaps: Splunk

Entry format and the other areas: [the known-gaps index](README.md).

- **No Splunk-to-Splunk (S2S) listener.** A universal forwarder speaks only S2S, over `:9997` or
  `[httpout]` to `/services/collector/s2s`, and the protocol has no public specification: Splunk
  9.1+ requires v4, the open implementations stop at v3, and Cribl's is proprietary
  ([plan §9](../plans/splunk-relay.md#9-not-in-this-stack)).
  - **Consequence:** a universal forwarder can't point at `logit`, so a forwarder-fed Splunk
    can't be teed through `splunk_hec_in`.
  - **Workaround:** a heavy forwarder's `outputs.conf [syslog]` stanza into `syslog_in` (RFC 3164
    over UDP or TCP).
  - **Revisit trigger:** a published S2S specification, or a user whose forwarders can't be given
    a `[syslog]` output.
- **No REST search export input.** Splunk's `search/jobs/export` on the management port `:8089`
  streams search results as CSV, JSON, or raw text; `logit` has no poll-driven source for it.
  Splunk Cloud opens that port only by support ticket.
  - **Consequence:** data already indexed in Splunk can't be pulled out through `logit`.
  - **Revisit trigger:** a migration that needs historical data moved, not only new data teed.
- **No listener for a forwarder's `[tcpout] sendCookedData = false` output.** Splunk Enterprise
  10.4.3 writes each event's `_raw` followed by one LF, with no header, length, or metadata
  (`tools/splunk-interop/README.md`, "What the run showed"). `logit` has no plain-lines TCP
  listener; `syslog_in` would parse each line as a syslog message. The "No plain-lines
  listener" entry in [the Datadog gaps](datadog.md) is the same gap.
  - **Consequence:** this output can't feed `logit`. Even a line listener would split an event
    with an embedded newline in two, and would receive no `host`, `source`, `sourcetype`, or
    `index`.
  - **Workaround:** the heavy forwarder's `[syslog]` output, as above.
  - **Revisit trigger:** a `lines_in` on the `TcpListener` driver, which would serve this and the
    Datadog case ([plan §9](../plans/splunk-relay.md#9-not-in-this-stack)).
- **An Edge Processor's HEC destination pointed at `splunk_hec_in` is UNVERIFIED.** Splunk's docs
  describe the HEC destination only for Splunk targets, with acknowledgment off on the destination
  token. Edge Processor runs in Splunk Cloud and in a Splunk Enterprise 10.x edition that the
  `splunk/splunk` image isn't, so the Enterprise runs exercised none
  ([plan, "Settled by W5"](../plans/splunk-relay.md#settled-by-w5-2026-09-25), item 3). The Splunk
  Cloud trial stack has none provisioned either, because enabling it takes Splunk's support or
  account team
  ([plan, "Settled by the Cloud run"](../plans/splunk-relay.md#settled-by-the-cloud-run-2026-09-26),
  item 8). Ingest Processor sends only to Splunk indexes, S3, and Observability Cloud, so it has no
  destination that reaches `logit`.
  - **Consequence:** the one Splunk-side HEC sender that could tee a forwarder-fed Splunk into
    `logit` is untested.
  - **Revisit trigger:** access to an Edge Processor; record what it sends with
    `script/record-fixtures`.
- **Acknowledgment on Splunk Cloud depends on the stack.** Splunk documents HEC indexer
  acknowledgment on Splunk Cloud Platform only for its Firehose path, but the 10.5.2605.9 trial
  stack offered it on its tokens and acknowledged `splunk_hec_out`'s requests with none timed out
  ([plan, "Settled by the Cloud run"](../plans/splunk-relay.md#settled-by-the-cloud-run-2026-09-26),
  item 3). A customer stack may differ, so `ack: true` stays opt-in. Against a token that doesn't
  acknowledge, each request counts as delivered on its `200`, and
  `logit.output.acks{result="unsupported"}` counts it.
  - **Consequence:** on a stack without acknowledgment, delivery ends at a `200`, which means
    received, not indexed.
  - **Revisit trigger:** a customer stack that refuses acknowledgment, or Splunk documenting it
    for HEC on Splunk Cloud.
- **`splunk_hec_in` keeps acknowledgment state within fixed bounds, not Splunk's.** It keeps
  `max_ack_channels` channels (default 256), evicting the least recently used, and per channel an
  issue window of the most recent `max_pending_acks` ids (default 1,000,000). Splunk's
  `max_number_of_acked_requests_pending_query_per_ack_channel` caps ids outstanding, so an id
  answered in the middle frees a slot there. Here the window moves with every id issued, so an
  unpolled id expires after that many newer ones, whatever was answered in between. The bounds are
  for accidental data under
  [ADR `deployment-threat-model`](../adr/deployment-threat-model.md): a client that sends a channel
  and never polls, or many short-lived channels.
  - **Consequence:** an id on an evicted channel, or one that expired, answers `false`, and a
    client polling for it times out; `logit.input.acks.dropped{reason}` counts it.
    `splunk_hec_out` then fails the batch `Ambiguous`, and resends it under the default,
    `at_least_once`.
  - **Workaround:** raise `max_ack_channels` above the number of clients that send a channel at
    once.
  - **Revisit trigger:** a client that relies on Splunk's outstanding-id count, or channel churn
    that evicts live channels.
- **HEC codes 21, 22, 24, and 25 aren't modeled, and the texts for 18 through 27 are from
  Splunk's documentation.** `logit_proto::splunk::response`'s `HecStatus` has no entry for the
  four, so `splunk_hec_out` counts one as `logit.output.requests.rejected{code="other"}` when it
  arrives with a non-retryable status, and `splunk_hec_in` never answers one. `splunk_hec_in`'s
  busy `/health` answers code 18 with the documented text. Code 28, Splunk Cloud's answer to a
  `useACK` request without a channel, is modeled from Splunk Cloud 10.5.2605.9's verbatim reply.
  No `script/splunk-interop` run (two Enterprise, three Cloud) provoked a code 18 through 27.
  - **Consequence:** a rejection with one of these codes is counted under `other`, and the
    diagnostic's body quote is what names it.
  - **Revisit trigger:** a real Splunk answers one of them.
- **Splunk Cloud's oversize answer is inferred from the body's size.** The 10.5.2605.9 trial
  stack accepted bodies up to 5,242,881 bytes and answered 6,000,000 and above with `400`
  `{"text":"Invalid data format","code":6,"invalid-event-number":0}`, not `413`, and that reply
  says nothing a malformed object 0 wouldn't. `splunk_hec_out` reads it as oversize only when the
  body is over 5,242,880 bytes (`SPLUNK_CLOUD_BODY_CAP`) before compression: it splits the body in
  two once, or drops a lone object counted `records.dropped{reason="oversize"}`, and a half refused
  the same way fails the batch. Rule 70 warns at startup about a `max_body_bytes` above that.
  - **Consequence:** a stack whose cap is under 5 MiB gets the drop-one-object rule on an
    oversize body, losing a valid object. The split cuts at an object boundary, so a body that one
    split can't bring under the cap fails the batch, which any `max_body_bytes` above the cap
    allows (objects of 0.1, 5.3, and 0.1 MB leave a 5.4 MB first half). A busy answer later in the
    same batch retries it whole, re-sending the dropped object and counting it in
    `records.dropped` again on each retry; the record is never delivered twice. The exact cap
    between 5,242,881 and 6,000,000 bytes wasn't bisected.
  - **Workaround:** keep `max_body_bytes` at or under 5 MiB against Splunk Cloud, as the startup
    warning says; the 2 MiB default does.
  - **Revisit trigger:** a Splunk Cloud stack whose cap is under 5 MiB, or one that answers
    `413`.
- **`splunk_hec_out` ignores `Retry-After`.** A busy answer (`429`, or `503` code 9) before any
  body of the batch was accepted is retried on `write_loop`'s own backoff (200 ms doubling to
  `buffer.retry_max_delay`), whatever delay Splunk asks for; the runtime's retry loop has no seam
  for a server-supplied delay.
  - **Consequence:** a retry can come sooner than Splunk asked, drawing another busy answer, until
    the batch's retry budget runs out.
  - **Revisit trigger:** a `Retry-After`-carrying sink that needs it honored, which would give
    `Fault` or `deliver_with_retry` a delay hint every HTTP sink could use.
- **`splunk_hec_out` treats codes 7, 12, 13, and 15 as permanent.** Each names an object in
  `invalid-event-number`, and Splunk 10.4.3 indexed the objects before the bad one and none from
  it on, as with code 6. Only code 6 gets the drop-one-and-resend rule; the others fail the batch.
  `splunk_hec_out` never writes the shapes behind 12, 13, and 15, so in practice this means code
  7, an index the token isn't allowed to write.
  - **Consequence:** one object with a disallowed `com.splunk.index` loses the objects after it in
    its request and the rest of the batch.
  - **Workaround:** keep every stamped index in the token's allowed list.
  - **Revisit trigger:** a pipeline that mixes indexes a token can and can't write.
- **Four `splunk_hec_out` behaviors Vector's HEC sinks have are deferred.** A comparison with
  Vector's `splunk_hec_logs` and `splunk_hec_metrics` sinks
  ([plan, "What's left"](../plans/splunk-relay.md#whats-left)) found them, and none has a user yet:
  - acknowledgment blocks a batch until its ids are acknowledged, within `ack_timeout` (30 s),
    where Vector keeps a window of pending batches for up to 300 s;
  - every attribute becomes an indexed field in `fields`, as the OTel exporter writes it, where
    Vector indexes only an allowlist and puts the rest in the `event` object;
  - there's no startup healthcheck (`GET /health/1.0`) to report a bad endpoint before the first
    batch;
  - a batch's bodies go out one after another, not in parallel.
  - **Consequence:** under load, `ack: true` can stall the sink or fail batches `Ambiguous` that a
    longer window would have seen acknowledged; a parsed log with many attributes grows Splunk's
    index-time field storage; a bad endpoint shows only when the first batch fails; and the
    sink's throughput to Splunk Cloud is bound by one round trip per body.
  - **Workaround:** leave `ack` off, or raise `ack_timeout`; `keep` or `remove` the attributes
    that needn't be indexed before the sink.
  - **Revisit trigger:** a deployment where one of these consequences shows: acknowledgment
    timeouts under load, a Splunk index size complaint, or a throughput shortfall to Cloud.
- **`splunk_hec_out` has no `auto_extract_timestamp` option.** Probes against Splunk Enterprise
  10.4.3 showed `/event` runs a sourcetype's `TRANSFORMS-*` (index routing, sourcetype renaming)
  and skips only timestamp extraction, which `/event?auto_extract_timestamp=true` restores
  (`tools/splunk-interop/README.md`, "What the run showed"). So a `/raw` egress mode would add
  only line-breaking props, and the follow-up is that query parameter, not `/raw`
  ([plan, "Reassessed: `/raw` egress"](../plans/splunk-relay.md#reassessed-raw-egress)).
  - **Consequence:** Splunk indexes what `splunk_hec_out` sends at the event's own `time`, never
    at a time its sourcetype's `TIME_PREFIX`/`TIME_FORMAT` would extract from the text.
  - **Revisit trigger:** a user whose sourcetype's extracted time should win over the event's.
- **What neither the recorded corpus nor the Splunk runs exercised.** Each is implemented from the
  exporter's source or Splunk's docs and covered by the codec's own tests:
  - the exporter's `Summary` shape and a span link's `trace_state` member (telemetrygen writes
    neither), and `otel.log.name`;
  - Vector's `splunk_hec_logs` and `splunk_hec_metrics` sinks as clients of `splunk_hec_in`;
  - a third-party HEC client using `useACK` against `splunk_hec_in`, whose per-channel ids and
    polls the round-trip tests drive only with `splunk_hec_out` and hand-written requests;
  - any Splunk Enterprise release other than 10.4.3, including which release raised
    `max_content_length` from 1,000,000 bytes;
  - a paid Splunk Cloud Platform stack's `http-inputs-<stack>` endpoint and its certificate: the
    runs exercised Splunk Cloud Platform 10.5.2605.9 on a trial stack, whose HEC is
    `<stack>.splunkcloud.com:8088` with Splunk's default self-signed certificate;
  - Splunk Observability Cloud through `fixtures/splunk-observability.yaml`, which no trial org
    has received.
  - **Consequence:** a difference here shows up in a deployment first, as a listener's
    `rejected` counters or a sink's `requests.rejected`.
  - **Revisit trigger:** re-record with `script/record-fixtures splunk` against another client or
    version, or rerun `script/splunk-interop` against another release.
