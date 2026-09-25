---
created: 2026-09-25
updated: 2026-09-25
---

# Splunk HEC: a lossless pair in the OpenTelemetry exporter's vocabulary, spans as HEC events, and opt-in acknowledgment

## Status
Accepted

## Context

`logit` has no Splunk component. `syslog_out` can feed a Splunk network input and `otlp_out` can
reach Splunk Observability Cloud, but neither is what Splunk's own tooling speaks today: every
third-party integration with Splunk Enterprise and Splunk Cloud Platform is a client of the HTTP
Event Collector (HEC), a JSON-over-HTTPS listener on every indexer and heavy forwarder.
[`docs/plans/splunk-relay.md`](../plans/splunk-relay.md) has the survey, the coverage table, and
the workstreams; this record holds the decisions.

Four facts from the survey drive the shape of the decision:

- The HEC wire is fully documented, and the OpenTelemetry Collector's `splunk_hec` exporter is
  its de facto schema for OTel data: resource attributes onto the envelope under
  `com.splunk.*` and `host.name`, log severity and name under `otel.log.*`, metrics as
  `metric_name:<n>` fields with a `metric_type` dimension, and spans as a JSON object in `event`.
  Splunk users' dashboards are built against that schema.
- Splunk has one metric shape, `name = number`. Histograms and summaries reach it only through a
  naming convention (Prometheus-style `_bucket`/`le`, `_sum`, `_count`), and Splunk has no store
  for traces: the exporter's span objects are ordinary events.
- HEC `time` is epoch seconds with a decimal fraction. An epoch value with nanosecond decimals has
  19 significant digits, which an `f64` doesn't hold.
- Acknowledgment (`useACK`) is per token, changes every response, requires a channel header on
  every request once on, and isn't available on Splunk Cloud at all.

## Decision

1. **One new like-protocol pair**, `splunk_hec_in -> splunk_hec_out`, added to
   [ADR `lossless-transit`](lossless-transit.md) by amendment. The codec lives in
   `crates/logit-proto/src/splunk/`; its module doc is the canonical mapping table and
   permitted-normalization list, the collectd and Datadog pattern.

2. **Two kinds; the envelope comes from the resource.** `splunk_hec_in` serves `/event` (JSON,
   concatenated objects or an array), `/raw` (lines), `/health`, and `/ack` under Splunk's route
   aliases, answers Splunk's own `{"text","code"}` bodies, decompresses gzip only, checks an
   optional `tokens:` allowlist, and never keeps the token as an attribute. `splunk_hec_out` posts
   `/event` bodies, gzip by default, split by a body-size cap. `index`, `source`, `sourcetype`,
   and `host` come from resource attributes set upstream with `set`, never per-sink fields, by
   [ADR `operator-declared-resource-attributes`](operator-declared-resource-attributes.md).
   `duplicate_safe()` is false: Splunk indexes a resent event twice. The config fields themselves
   are operator docs in `logit-config` (W2, W3).

3. **Reuse, not new plumbing.** The listener reuses `crates/logit-inputs/src/http.rs`'s
   idle-timeout driver, bounded read, decompression, and bounded-wait delivery, and `datadog_in`'s
   accept loop and TLS peek; the sink reuses `logit-outputs`' `crate::http` client helpers,
   `otlp_out`'s header and gzip handling, and `write_loop`'s bounded retry with `Fault`
   classification. `MultiValue` moves from `graphite` to the `logit-proto` crate root, re-exported
   from `graphite`, so both sinks share one switch.

4. **Multi-number kinds follow `graphite_out`'s switch.** Under `multi_value: skip` (the
   default) `Samples`, `SetMembers`, `Distribution`, `Set`, `Histogram`, and `Summary` are dropped
   and counted `logit.output.metrics.skipped{metric_kind}`. Under `expand` each renders as a
   counted `degraded{metric_kind}` series set: `Histogram` and `Summary` in the exporter's shape
   (`_sum`, `_count`, cumulative `_bucket` with `le` including `+Inf`; `<n>_<q>` with `qt`), so an
   `mstats`/`histperc` dashboard built for the Collector's output works unchanged; `Samples` and
   `Distribution` as `influxdb_out`'s summary set under `metric_type` `Summary`; `Set` and
   `SetMembers` as a count. `ExponentialHistogram` is skipped under both, as the exporter does.
   Summarization stays `aggregate`'s job upstream.

5. **Acknowledgment is opt-in on the sink.** `ack: true` reads each response's `ackID` and polls
   `/services/collector/ack` until every id is `true` or `ack_timeout` (default 30 s) elapses, at
   which point the batch is a `Fault::Ambiguous` for `write_loop` to retry. Off by default:
   Splunk Cloud doesn't support it, and a token without `useACK` returns no id. The listener
   answers `/ack` with every asked id `true`, because its `200` already means delivered to the
   pipeline, and a full pipeline answers `503` code 9, which every HEC client retries. Channel and
   ack id never enter the event.

6. **The OpenTelemetry exporter's attribute vocabulary, not a `splunk.*` namespace.** The
   envelope's `host`, `source`, `sourcetype`, and `index` are the resource attributes `host.name`,
   `com.splunk.source`, `com.splunk.sourcetype`, and `com.splunk.index`; `otel.log.severity.*`
   and `otel.log.name` decode into `LogRecord`'s typed fields and are written back from them;
   `trace_id`/`span_id` decode into `TraceRef`. One vocabulary across `otlp_out` → Collector →
   HEC and `splunk_hec_out` is what makes the two paths interchangeable. The survey found nothing
   the OTel schema has no word for, so no `splunk.*` name exists.

7. **Timestamps keep every digit.** The decoder turns `time` into `Event::timestamp` without an
   `f64` (decision 21); an absent `time` takes receipt time, as `syslog_in` does, and `0` is the
   epoch. The encoder always writes `time`, as `secs.nanos` trimmed of trailing zeros, so Splunk
   never applies a sourcetype's timestamp extraction to what `logit` sends. There is no
   stale-data window to enforce: Splunk indexes any `time`.

8. **Receive side first.** The codec (W1) fixes the vocabulary and mappings, `splunk_hec_in` (W2)
   lands before `splunk_hec_out` (W3), and the recorded fixtures (W5) verify both.

9. **Not in this stack:** Splunk-to-Splunk (S2S), the only protocol a universal forwarder speaks,
   which has no public specification (a heavy forwarder's `[syslog]` output into `syslog_in` is
   the workaround); REST search export as an input; a raw-TCP line listener for a forwarder's
   `sendCookedData = false`; SignalFx's pre-OTLP JSON APIs. Each is recorded in
   `docs/known-gaps.md` (W6).

10. **The encoder writes one object per `MessageBuf` entry; it isn't a `FramedEncoder`.**
    `SplunkEncoder::encode_objects` writes one HEC JSON object per payload into a
    `MessageBuf<ObjectMeta>` (`event_index`, `signal`, record count) and never fails; every loss
    is a `Telemetry` counter, as in every other JSON codec. The sink packs objects greedily into
    bodies under its size cap and drops object `N` by slicing the same buffer (decision 18), with
    no re-encode. Its `Encoder` impl is the concatenation, one body per batch. `FramedEncoder`
    was the other candidate and doesn't fit: a HEC object isn't a transport message (the request
    is), and its per-call `Stats` would duplicate the counters. The decoder isn't a `Decoder`
    either: one request carries several resources, so `decode_events` returns
    `Vec<EventBatch>`. The response bodies (`HecStatus`, the ack request and reply) live in the
    codec because only `logit-proto` depends on `serde_json`.

11. **Batches are grouped by resource, in first-appearance order.** Every HEC object carries its
    own envelope, so the decoder groups objects by their resulting `Resource` (keyed on its
    attributes' JSON text), keeping wire order within a group. A syntax error in any object
    rejects the whole body with code 6 and `invalid-event-number` set to that object's index, and
    nothing is delivered, as Splunk does. An unknown envelope key is dropped and counted.

12. **`metric_type` carries `Sum` versus `Gauge`, and nothing carries temporality.** A metric
    object's `metric_type` `Sum` decodes every record to `Sum{Cumulative, monotonic}`; `Gauge` or
    absent decodes to `Gauge`, and both are consumed. Any other value (the exporter's `Histogram`,
    `Summary` on an expanded series) stays a verbatim attribute on a `Gauge`, and rides back out
    as that object's `metric_type`. The encoder writes the exporter's form: one multi-metric
    object per `metric_type` per event, `Gauge` when the event carries none. A delta or
    non-monotonic `Sum` leaves as `Sum`, counted `degraded`: HEC has no carrier for temporality
    or monotonicity, the Graphite precedent, and a `known-gaps.md` row (W6).

13. **Metric names are sanitized with an `m` prefix.** Outside `[A-Za-z0-9_.:]` becomes `_`, a
    `metric_name` substring becomes `metricname`, and a leading digit or `_` gets an `m` prefix,
    counted `logit.output.metrics.normalized{reason="name_sanitized"}`. Prometheus's sanitizer
    prefixes `_`, which HEC forbids as a leading character. The decoder never sanitizes.

14. **A span is detected from its shape, and falls back to a log.** An `event` object carrying a
    non-zero hex `trace_id` and `span_id` and integer `start_time` and `end_time` decodes to a
    `SpanRecord`; if any other member doesn't parse or isn't one the exporter writes, the whole
    object stays a log with a `Map` body, counted `logit.input.spans.degraded`. The envelope
    `fields` of a span go to the batch **resource**, because the exporter fills them from the
    span's resource attributes; the span's own attributes are the object's `attributes` member.
    The encoder writes the exporter's `hecSpan` member order, `time` as the start. `flags`,
    `trace_state`, and the dropped counts have no member and are counted.

15. **The exporter's severity fields are kept beside the typed field.** `otel.log.severity.number`
    and `.text` decode into `LogRecord::severity` by OTLP's band-then-text rule and stay as
    attributes, because the band collapses 24 numbers to 6 (the rule (b) precedent `otlp_in`
    sets with `otel.severity_number`); the encoder writes them from `severity` only when neither
    attribute is present. `otel.log.name` and a valid `trace_id`/`span_id` pair are consumed
    into `event_name` and `trace`, which hold them without loss.

16. **Lenient where Splunk rejects a whole request.** `/raw` needs no channel (Splunk requires one
    only with `useACK`, and `splunk_hec_in` has no ack state). An object with no `event` or a
    blank one is skipped and counted, and the rest of the body delivered, where Splunk answers
    code 12 or 13 and indexes nothing. On the way out, a log whose message is `null` or `""` is
    never sent, counted, because Splunk would reject the whole request for it.

17. **The sink sends a channel header on every request; `ack` only switches polling on.** A
    `useACK` token answers `400` code 10 to a request without `X-Splunk-Request-Channel`, and a
    token without it ignores the header, so always sending one per-sink GUID makes the sink work
    against both token kinds with `ack: false`.

18. **A `400` code 6 drops one object and resends the rest, once.** When Splunk names
    `invalid-event-number: N`, the sink drops object `N` of that body (counted, with a
    diagnostic) and resends objects `N+1` onward, once; a second code 6 is permanent. This
    assumes Splunk indexed the objects before `N`, which Splunk doesn't document: UNVERIFIED item
    8 in the plan, and W5 settles it against a real Splunk. The rule sits behind one function so
    W5 can change it without touching the rest of the sink.

19. **Graph rules 69 and 70** validate the two kinds: 69 for `splunk_hec_in` (a non-empty
    `bind`, no empty or whitespace-padded token, a positive `max_request_bytes`), 70 for
    `splunk_hec_out` (an absolute `http(s)` base `endpoint` not ending in a route path, a
    non-empty token, positive `timeout` and `max_body_bytes`, `ack_timeout` only with `ack`, and
    rule 24's TLS checks).

20. **The connection cap stays an internal constant.** No HTTP listener exposes
    `max_connections`; `splunk_hec_in` keeps the shared cap, not a new operator knob.

21. **`serde_json`'s `raw_value` feature, not `arbitrary_precision`.** The decoder reads each
    object as `BTreeMap<String, &RawValue>` and parses `time` from the number's own text with
    `logit_core::time::parse_decimal_nanos`, so `1700000000.123456789` is exact.
    `arbitrary_precision` would have done the same, but changes every `serde_json::Number` in the
    workspace (cargo unifies features); `raw_value` is additive.

## Alternatives considered

- **A `splunk.*` attribute namespace.** It would say where a value came from, but the Collector's
  names are the ones Splunk users already search and chart, and a `splunk.*` vocabulary would make
  `otlp_out` → Collector and `splunk_hec_out` put different names in the same index. The
  survey found no field the OTel schema can't name.
- **A plain `Encoder` only, re-encoding on a code-6 answer.** Simpler, but the sink would have to
  re-encode a batch to drop one object and would size bodies only after encoding them whole.
  Per-object output costs one `MessageBuf` and makes both operations a slice.
- **Not decoding spans, as the Collector's `splunk_hec` receiver doesn't.** It keeps every span
  object as a log, lossless as JSON but useless to `otlp_out` or a trace backend downstream. The
  detection rule is narrow enough (four valid members, and nothing the exporter never writes)
  that an ordinary JSON log is never mistaken for a span.
- **Writing no `metric_type` for a `Gauge`.** It keeps a non-OTel client's metric object
  byte-identical on relay, but makes `splunk_hec_out`'s output differ from the exporter's for
  every gauge. The exporter's shape was chosen; the added dimension on a relayed metric that had
  none is a permitted normalization.
- **Rejecting a nested `fields` value, as Splunk does.** It would turn one nested attribute into
  a failed request for the whole batch. Flattening to dotted keys is what the exporter does and
  what an operator would place a `flatten` for anyway. The codec's `flatten_into` departs from
  the `flatten` transform where a flat JSON object has no room for what `flatten` keeps: it drops
  an empty map, and writes an array holding a container, or a map nested past the depth bound, as
  its JSON text.
- **Rejecting a whole body on a blank event, as Splunk does.** Faithful, but one bad object from
  one client would lose every other object in the request.
- **Reading `time` through `f64`.** Loses up to a few hundred nanoseconds at epoch magnitude on
  every hop, which the fixed-point test would catch as a drift.
- **Putting channel and ack state on the event.** It would let a downstream `splunk_hec_out`
  reuse a client's channel, but a channel is a connection-scoped handle between one client and
  one server, meaningless past the hop that minted it.

## Consequences

- A ninth like pair, with a fixed-point suite (`crates/logit-proto/tests/splunk_fixed_point.rs`)
  over a grammar of HEC bodies. The codec changes neither `Event` nor any allocation count.
- A resource attribute that isn't a carrier leaves a log or metric object in `fields` and comes
  back an event attribute; only a span's `fields` return to the resource. That, `Sum`
  temporality, the multi-number kinds, and the span fields without a member are
  cross-protocol losses for `docs/known-gaps.md` (W3 drafts the rows, W6 lands them).
- A relayed metric object with no `metric_type` gains a `metric_type=Gauge` dimension in Splunk.
- The JSON plumbing the Datadog routes used (`json_to_value`, the ordered writer) moves to
  `crate::json`, shared by both codecs, with a dotted-key `flatten_into` beside it; the
  `raw_value` feature is on for every crate that uses `serde_json`.
- Encoded `fields` keys follow interned-symbol order, so two processes can write the same object
  with keys in a different order. The byte fixed point holds within one process; a comparison
  across processes (W5's recorded fixtures) compares decoded batches or parsed JSON.
- The texts for codes 18 and up are best readings of Splunk's documentation, and codes 21, 22,
  24, and 25 aren't modeled; W5 records a real Splunk's answers. The `hecSpan` member order and
  the link's `trace_state` member come from the exporter's source and are verified by W5's
  recorded exporter fixture, as is the code-6 rule (decision 18).
- `splunk_hec_in`'s bounded-wait `503` can follow a partial delivery of a multi-resource request,
  so a client's retry duplicates the batches already delivered: the Datadog listener's trade-off,
  documented in W2's module doc.

## Amendment: what the listener settled (2026-09-25)

Building `splunk_hec_in` (W2) fixed the details below. `crates/logit-inputs/src/splunk.rs`'s
module doc describes the behavior; this list is the record of the choices.

- **Decision 2, config:** the TLS field is `tls:`, the single-mode listener shape `datadog_in`
  and `otlp_in` use; `bind_tls:` is for a two-mode kind such as `prometheus_in`.
  `max_request_bytes` is a byte-count string (default `"5MiB"`), like every other byte field,
  and one cap covers both the body as sent and its gzip-decompressed form.
- **Decision 2, authentication:** a `token` query parameter is always `400` code 16, even with
  no `tokens` configured. A missing `Authorization` header, an empty one, or `Splunk` with no
  token is `401` code 2; another scheme, or a `Basic` value that doesn't decode to
  `user:password`, is `401` code 3; a token not listed is `403` code 4. Scheme names match in any
  case. An empty `tokens` passes any request, whatever it carries.
- **Decision 2, errors:** code 5 answers an empty body on any `POST` route and a `/raw` body with
  no non-empty line. A malformed `/ack` body is `400` code 6 with no `invalid-event-number`. A
  gzip body that doesn't decompress is `400` code 6, counted `malformed_encoding`. The HTTP-level
  errors carry the status as `code` with these texts: `404` "Not Found", `405` "Method Not
  Allowed", `408` "Request Timeout", `413` "Request Entity Too Large", `415` "Unsupported Media
  Type". The rejection reasons beyond `datadog_in`'s are `query_token` and `no_data`.
- **Decision 5, acknowledgment:** an `ackID` is drawn only on a `200`, never on a `503` or a
  rejection, and a channel header or `?channel=` with an empty value names no channel. A body
  whose every object the codec skips sends nothing and still answers `200`, with an `ackID` when
  it named a channel.
- **Decision 16:** no channel is required on any route, `/raw` and `/ack` included.

## Amendment: what the sink settled (2026-09-25)

Building `splunk_hec_out` (W3) fixed the details below. `crates/logit-outputs/src/splunk.rs`'s
module doc describes the behavior; this list is the record of the choices.

- **Decision 2, config:** `max_body_bytes` is a byte-count string (default `"2MiB"`), like
  `splunk_hec_in`'s `max_request_bytes`, and caps a body before compression. There is no
  `headers:` field: nothing in the survey needs one, and `datadog_out`'s reserved-name rules
  would come with it. `ack_timeout`'s 30s default is applied when the component is built, so an
  explicit `ack_timeout` without `ack: true` is rule 70's error rather than a silent no-op.
- **Decision 2, bodies:** objects are packed greedily in batch order and concatenated with no
  separator. An object over the cap alone is dropped and counted by the records it carries, and
  packing carries on past it rather than closing the body. The first failing body aborts the
  rest of the batch, `datadog_out`'s rule.
- **Decision 17, the channel:** one random v4 GUID per sink instance, drawn when it is built, so
  a restart starts a new channel. It goes on `/ack` polls too, which a `useACK` token requires.
- **Decision 18, code 6:** a `400` whose body is code 6 with no `invalid-event-number`, or one
  that names no object of the body, is permanent with a `request_rejected` diagnostic, as is a
  code 6 on the resend. A code 6 naming the body's last object needs no resend. The records
  ahead of the named object count as delivered (`logit.output.records`), under the same
  unverified assumption the resend makes.
- **Decision 2, faults across requests:** a connect failure is `Fault::Clean` only until a
  `/event` request of the `send` is accepted (a 2xx, or a code 6 that counts objects ahead of the
  named one as delivered); after that every transport failure is `Fault::Ambiguous`. `write_loop`
  retries `Clean` under every posture, so a `Clean` there would index the accepted bodies twice.
  `logit_out`'s "`Clean` at the handshake, `Ambiguous` after a data frame left" is the precedent.
- **Decision 5, acknowledgment:** the deadline runs from when the batch's last body was
  accepted, and a last poll runs at the deadline before the batch fails as ambiguous. Every poll
  failure other than code 14 (a transport error, another non-2xx, a body that isn't an ack reply)
  is retried on the same schedule, since a poll that fails says nothing about indexing. A code 14
  poll counts every id still pending as delivered. Polls aren't compressed: an ack request is a
  few bytes per body. `logit.output.acks` counts per `/event` request (one id each).
- **Telemetry:** `logit.output.requests`, `request.duration`, and `request.bytes` carry
  `route` (`event` or `ack`), so a poll is visible beside the requests it confirms.
  `logit.output.requests.rejected{code}` counts every non-retryable `/event` answer, a code 6
  that leads to a resend included; `code` is the body's HEC code when Splunk documents it (the
  codes in `HecStatus::ALL`), else `other`, so a proxy's error page can't grow the tag set. A
  `401` or `403` has its own diagnostic key, `token_rejected`.
- **Decision 3, reuse:** `datadog_out`'s bounded, secret-scrubbing error-body read moves into
  `logit-outputs`' `crate::http` (`error_read_bytes`, `redacted_snippet`), shared by both sinks.
  The HTTP client is built on `Output::bind`, or by the first `send` when nothing called it.
