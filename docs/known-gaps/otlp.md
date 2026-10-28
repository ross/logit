# Known gaps: OTLP

Entry format and the other areas: [the known-gaps index](README.md).

- **`otlp_in`'s `partial_success` response is always empty.** OTLP's
  `Export*ServiceResponse.partial_success` lets a receiver accept most of a request while
  reporting rejected records. `otlp_out` implements the reading half
  (`a_partial_success_response_is_counted_not_failed`), but
  `logit_proto::SignalDecoder::decode_signal` returns no per-call skip/reject count, only a
  self-telemetry counter (`logit.input.metrics.skipped{metric_kind, reason}`).
  - **Consequence:** every successful decode replies with an empty (all-default, "fully
    accepted") `partial_success`, even when a point was skipped. The one skip case is a `Metric`
    whose `data` oneof isn't set (`crates/logit-proto/src/otlp/metrics.rs::decode_metric`'s `None`
    arm). A malformed request (bad protobuf, an invalid span id) fails whole
    (`400`/`grpc-status: 3`).
  - **To close:** when OTLP input volume makes it worth it, thread a per-call count through
    `SignalDecoder` (a `crates/logit-proto` API change). On the JSON path the count renders as
    `rejectedSpans`/`rejectedLogRecords`/`rejectedDataPoints`, a different key per `Signal`, where
    protobuf shares one tag number across all three `Export*ServiceResponse` messages
    (`export_response_json`'s doc comment, `crates/logit-inputs/src/otlp.rs`).
- **`otlp_in` has no CORS support: `OPTIONS` 404s, no `Access-Control-Allow-Origin`.** A browser
  exporter posting cross-origin fails at preflight: `handle_http`
  (`crates/logit-inputs/src/otlp.rs`) answers any non-`POST` method, `OPTIONS` included, with
  `404`.
  - **Workaround:** same-origin export through a reverse proxy in front of both the page and
    `otlp_in`, as `demo/haproxy/haproxy.cfg` routes `/v1/traces` to `logit`; see
    [ADR `browser-tracing-sdk`](../adr/browser-tracing-sdk.md).
  - **To close:** a `cors:` config surface (allowed origins, an `OPTIONS` handler, response
    headers), which is a config and security design of its own (the allowed-origins list, whether
    a reflexive `*` is ever appropriate).
- **`otlp_in` answers every 4xx/5xx with `text/plain`, on both encodings; the spec wants a
  protobuf-encoded `Status`.** The spec: *"The response body for all HTTP 4xx and HTTP 5xx
  responses MUST be a Protobuf-encoded Status message"*. `text_response`
  (`crates/logit-inputs/src/otlp.rs`) always builds a plain-text body, on the protobuf and JSON
  paths alike.
  - **Consequence:** none observed: every real client checked (including `opentelemetry-js`) reads
    only the HTTP status code on error, never the body's content-type.
  - **Revisit trigger:** a client that parses the error body.
- **VictoriaTraces's OTLP/gRPC listener fails a request that races its connection close.**
  VictoriaTraces v0.11.1 closes every gRPC connection about 5 seconds after it opens, with a TCP
  FIN and no HTTP/2 `GOAWAY`
  ([`tools/victoria-interop/README.md`](../../tools/victoria-interop/README.md)'s "Recorded run",
  leg 7). Every request in flight on that connection at that moment gets no response frame and
  fails `Fault::Ambiguous`, because the server may have processed it. Above `concurrency: 1`,
  every batch in flight shares the one connection, so a close fails each of their requests, not
  only one.
  - **Consequence:** under the default posture, `at_least_once`, `otlp_out` retries each failed
    batch, at the cost of a duplicate span when the first attempt was stored. Under
    `buffer.delivery: at_most_once` it drops each one. An isolated 20 s run at 1 batch/s under `at_most_once` saw 3 closes and 2
    dropped batches. `script/victoria-interop`'s leg-7 row can pass a run in which no request
    raced a close; it counts `send_failed` lines but can't force the race.
  - **Workaround:** OTLP over HTTP to VictoriaTraces (`docs/deploying.md`'s "VictoriaMetrics,
    VictoriaLogs, and VictoriaTraces"), checked over `http://`. Over `https://`, a VictoriaTraces
    that picks h2 carries every batch in flight on one connection, as gRPC does, and whether its
    HTTP listener closes connections the same way hasn't been checked.
  - **Revisit trigger:** VictoriaTraces sending a `GOAWAY` (the upstream fix). Whether `otlp_out`
    should retry a gRPC request that got no response frame before the connection closed is a
    larger question: without a `GOAWAY`, the request may have been processed.
- **A `Histogram` carries no point `count`, so a `count` that disagrees with the buckets is
  lost.** `logit_core::Histogram`'s population is the sum of its buckets. `otlp_in` reads a point
  with only `count` and `sum` as one `+Inf` bucket holding `count`, but a point whose `count`
  isn't the sum of its `bucket_counts` keeps the buckets and loses the `count`, counted
  `logit.input.metrics.degraded{reason="histogram_count_mismatch"}`
  ([ADR `lossless-transit`](../adr/lossless-transit.md)'s 2026-10-21 amendment).
  - **Consequence:** `otlp_out` writes such a point with the buckets' total as its `count`. The
    point breaks `metrics.proto` ("The sum of the bucket_counts must equal the value in the count
    field"), so only a nonconforming producer sends one.
  - **Revisit trigger:** a producer whose points disagree by design, which needs a `count` field
    on `Histogram` (and its size checked against `docs/design/memory.md`).
- **`otlp_out` writes Prometheus's `target_info` as a metric.** The OTel Prometheus compatibility
  spec turns the `target` info metric into resource attributes rather than a metric, and
  `otlp_out` writes it as any other info metric: a non-monotonic `Sum` named `target_info`.
  - **Consequence:** a backend sees a `target_info` series, and the resource lacks its labels.
  - **Revisit trigger:** an operator relaying a Prometheus target into OTLP who needs the resource
    attributes. It needs `target_info` joined to its target's other batches by `job` and
    `instance`, which `otlp_out` encodes one event at a time.
- **A sketch's summary quantiles can be negative.** `metrics.proto` says "Quantile values must NOT
  be negative", and `otlp_out` writes a `Distribution` or `Samples` holding a negative observation
  as a `Summary` with a negative quantile value, quantile `0`, its minimum, among them.
  - **Consequence:** a consumer that enforces the rule may refuse the point.
  - **Revisit trigger:** a consumer that refuses one.
- **`otlp_out` writes a negative span time as 0, uncounted.** OTLP's span and span event times
  are unsigned nanoseconds, and the model's are signed, so `otlp_out` clamps a span start, end, or
  span event time before 1970 to 0 (`crates/logit-proto/src/otlp/traces.rs`'s `max(0)`) with no
  counter. No transit case holds such a time, so no cell states it.
  - **Consequence:** a span from a producer with a broken clock reaches the backend at the epoch,
    with a changed duration, and nothing in the sink's telemetry shows it.
  - **Revisit trigger:** a producer that sends one. The fix is a counter for the clamp, or a
    drop counted in its place.
- **`otlp_in` drops `keyStrindex`, `stringValueStrindex`, `entityRefs`, and `metadata`.** These
  fields belong to the profiles signal and to experimental parts of the data model. The JSON
  decoder skips them and the protobuf decoder never maps them
  (`crates/logit-proto/src/otlp/json/mod.rs` and `json/metrics.rs`), so `otlp_out` doesn't send
  them either. A `stringValueStrindex` value decodes as `Null`.
  - **Consequence:** a sender that uses string indexes or entity references loses them at
    `otlp_in`.
  - **Revisit trigger:** a sender that emits them on logs, metrics, or traces.
