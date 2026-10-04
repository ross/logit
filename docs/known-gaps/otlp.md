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
    `docs/plans/browser-tracing.md`.
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
- **An OTLP/JSON request costs more peak memory per byte than a same-sized protobuf one, under the
  same `MAX_REQUEST_BYTES` cap.** The JSON path parses into a `serde_json::Value` tree
  (`crates/logit-proto/src/otlp/json/`) first, one `Map`/`Vec`/`String`/`Number` allocation per
  node, where `prost::Message::decode` builds the target structs directly. Peak live heap bytes
  per input byte, debug build, measured 2026-09-25:
  - Ordinary OTLP/JSON structure (`testdata/interop/otlp/logs.json`, a real SDK export): about 19.
    The same batch as protobuf: about 17.
  - Crafted input, a body of tiny `{"":0}` objects under a key OTLP doesn't define, which
    serde_json builds in full and the decoder then ignores: about 98, at both 1 MiB and 4 MiB. At
    4 MiB that's about 400 MiB for one request.

  `crates/logit-proto/tests/robustness.rs`'s `otlp_json_peak_memory_per_input_byte_is_documented`
  asserts ceilings of 24 and 128 over these two shapes, so a change that moves either ratio fails
  a test. The bound still holds: `OtlpInput::max_connections`'s field doc
  (`crates/logit-inputs/src/otlp.rs`) states the worst case across all connections is a finite
  multiple of the protobuf path's 1.6 TiB, itself a bound rather than a memory budget
  (`MAX_CONCURRENT_STREAMS` in `crates/logit-inputs/src/http.rs` has the formula). There's no cap
  and no streaming parser, because the 98× shape needs crafted input, a non-goal under
  [ADR `deployment-threat-model`](../adr/deployment-threat-model.md).
  - **Revisit trigger:** an OTLP listener that faces an untrusted network.
- **VictoriaTraces's OTLP/gRPC listener fails a request that races its connection close.**
  VictoriaTraces v0.11.1 closes every gRPC connection about 5 seconds after it opens, with a TCP
  FIN and no HTTP/2 `GOAWAY`
  ([`docs/plans/victoriametrics-interop.md`](../plans/victoriametrics-interop.md)'s "Findings", leg
  7). A request in flight at that moment gets no response frame and fails `Fault::Ambiguous`,
  because the server may have processed it.
  - **Consequence:** under the default posture, `at_least_once`, `otlp_out` retries it, at the cost
    of a duplicate span when the first attempt was stored. Under `buffer.delivery: at_most_once` it
    drops the batch. An isolated 20 s run at 1 batch/s under `at_most_once` saw 3 closes and 2
    dropped batches. `script/victoria-interop`'s leg-7 row can pass a run in which no request
    raced a close; it counts `send_failed` lines but can't force the race.
  - **Workaround:** OTLP over HTTP to VictoriaTraces (`docs/deploying.md`'s "VictoriaMetrics,
    VictoriaLogs, and VictoriaTraces").
  - **Revisit trigger:** VictoriaTraces sending a `GOAWAY` (the upstream fix). Whether `otlp_out`
    should retry a gRPC request that got no response frame before the connection closed is a
    larger question: without a `GOAWAY`, the request may have been processed.
- **`otlp_out` gives the runtime one verdict per batch, not one per signal.** A traces-only
  backend (Tempo) answers a metrics or logs request with gRPC `UNIMPLEMENTED` or HTTP `404`, and a
  credential scoped per signal (a Grafana Cloud access policy granting `traces:write` without
  `metrics:write`) answers the other signals with HTTP `401`/`403` or gRPC
  `UNAUTHENTICATED`/`PERMISSION_DENIED`. `otlp_out` reads each as `Rejected` for that signal
  (`crates/logit-outputs/src/otlp.rs`'s module doc, "An answer that names one signal"). A mixed
  batch delivers the signals the backend takes and counts the others' records
  `logit.output.records.dropped{signal, reason="rejected"}`; a batch carrying only signals answered
  this way is dropped, counted
  `logit.component.batches.dropped{reason="rejected"}`. Neither holds the queue or ends the
  process.
  - **Consequence:** every mixed batch spends a request the backend refuses, and a retryable
    failure on one signal retries the whole batch, resending the signals already accepted.
  - **Workaround:** `has_signal` or `keep_signals` ahead of the sink, as `demo/logit.yaml`'s
    `trace_only` does.
  - **To close:** per-signal outcomes ([`docs/plans/sink-fault-model.md`](../plans/sink-fault-model.md),
    "W4: `otlp_out` per-signal outcomes").
- **An OTLP timestamp past `i64::MAX` saturates to `i64::MAX`.** A wire timestamp
  (`time_unix_nano`, `observed_time_unix_nano`, `start_time_unix_nano`, and the span, span event,
  and exemplar times) past `i64::MAX` nanoseconds decodes as `i64::MAX` through one helper
  (`wire_nanos`, `crates/logit-proto/src/otlp/common.rs`), and relays as
  2262-04-11T23:47:16.854775807Z, not the original. It's listed under
  [ADR `lossless-transit`](../adr/lossless-transit.md)'s "Permitted normalizations"
  ([ADR `untrusted-input-bounds`](../adr/untrusted-input-bounds.md) has the rule).
