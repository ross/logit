# Architecture decision records

Each ADR is a single Markdown file named after the decision (`slug.md`, no number — see
[`TEMPLATE.md`](TEMPLATE.md)). A slug is permanent once written: don't rename a file to fix a typo
or reword a title after the fact, since every other doc, source comment, and config in the repo
cites it by that exact path. `created`/`updated` in each file's frontmatter is what orders this
index, not the filename.

New ADR checklist: copy `TEMPLATE.md` to `docs/adr/<slug>.md`, fill in `created` (today) and
`updated` (same date), write the record, then add a row here.

| ADR | Created | Updated |
|---|---|---|
| [`FramedEncoder`: a third codec trait for sinks that need per-message framing, over a shared `MessageBuf`](framed-encoder.md) | 2026-09-12 | 2026-09-12 |
| [Prometheus scrape ingestion and exposition: transports, dialects, and the model mapping](prometheus-scrape-and-exposition.md) | 2026-09-11 | 2026-09-12 |
| [RFC 5424 structured-data convention: nested `syslog.sd`, strict parsing, opt-in PEN-qualified emission](syslog-structured-data-convention.md) | 2026-09-11 | 2026-09-11 |
| [Metrics model v2: `Sum` replaces `Counter`, raw/summarized pairs, boxed span fidelity, batch-level `Scope`](metrics-model-v2.md) | 2026-09-11 | 2026-09-12 |
| [A shared build cache and one-container check execution for the local development loop](fast-local-development-loop.md) | 2026-09-11 | 2026-09-11 |
| [Lossless like-protocol transit: the internal model is a superset of every supported wire protocol](lossless-transit.md) | 2026-09-10 | 2026-09-12 |
| [Browser tracing: the real OTel-JS SDK, `addLink` for sub-resources, and living with document-load's parent (not link) behaviour](browser-tracing-sdk.md) | 2026-09-10 | 2026-09-10 |
| [OTLP/JSON decoding: hand-written against `serde_json::Value`, not generated](otlp-json-decoding.md) | 2026-09-10 | 2026-09-10 |
| [statsd/DogStatsD egress: dialect, transport, packing, and the v1 metric-kind deferral](statsd-output.md) | 2026-09-10 | 2026-09-12 |
| [Provenance filtering is two transform components, combining has_attributes' and has_signal's shapes](provenance-filtering-components.md) | 2026-09-10 | 2026-09-10 |
| [Batch provenance (`origin`/`previous`) carried on `Delivered`, stamped by `Fanout`](batch-provenance-on-delivered.md) | 2026-09-10 | 2026-09-10 |
| [Attribute filtering is two transform components, and a bounded matcher is not a predicate language](attribute-filtering-components.md) | 2026-09-10 | 2026-09-10 |
| [`tracing` for self-logging, with `Diagnostics` as its producer](tracing-for-self-logging.md) | 2026-09-09 | 2026-09-09 |
| [A top-level `admin:` block, not a component, for readiness/liveness](admin-readiness-endpoint.md) | 2026-09-09 | 2026-09-09 |
| [Native transport: handshake, implicit sequencing, and per-batch acknowledgement](native-transport-handshake-and-ack.md) | 2026-09-09 | 2026-09-09 |
| [Disk-backed durable buffering for a sink's delivery queue](disk-backed-sink-buffer.md) | 2026-09-09 | 2026-09-09 |
| [`stdio_out`/`file_out` gain a `native` wire-format option](file-output-native-format.md) | 2026-09-09 | 2026-09-09 |
| [`file_out`: a rotating file sink, sharing `stdio_out`'s implementation](rotating-file-output.md) | 2026-09-08 | 2026-09-08 |
| [Native wire format encoding: hand-rolled, not `rkyv` or a `serde`/`postcard` derive](native-wire-format-encoding.md) | 2026-09-08 | 2026-09-08 |
| [Routing by condition, sampling, throttling, dedup, and renaming are `lua` components](routing-by-condition-is-lua.md) | 2026-09-07 | 2026-09-10 |
| [`logfmt` and `kv`: the de-facto key=value parsers, and why they stay two kinds](logfmt-and-kv-parsing.md) | 2026-09-07 | 2026-09-07 |
| [`regex`: named captures into attributes, and taking the `regex` crate](regex-transform.md) | 2026-09-07 | 2026-09-07 |
| [`csv`: positional columns from config, not a header row, and no type coercion](csv-positional-columns.md) | 2026-09-07 | 2026-09-07 |
| [`tail_in`: generic file tailing, and `docker_in` on top of it for Docker's json-file logs](file-tailing-and-docker-json-logs.md) | 2026-09-06 | 2026-09-06 |
| [`trace_context` grows a `span:` block, and a native `traceparent` parser](trace-context-span-lifting.md) | 2026-09-04 | 2026-09-11 |
| [`scale`: unit conversion by constant factor, and why it stays out of `kv_metrics`](scale-transform.md) | 2026-09-03 | 2026-09-03 |
| [TLS for `otlp_out`/`otlp_in`, and a pooled gRPC client to carry it](otlp-tls-and-pooled-grpc-client.md) | 2026-09-03 | 2026-09-03 |
| [`LogRecord` gains a native application trace/span reference](log-record-trace-context.md) | 2026-09-03 | 2026-09-03 |
| [Operator-declared resource attributes: a `set` transform, not a per-input config field](operator-declared-resource-attributes.md) | 2026-09-03 | 2026-09-03 |
| [`otlp_out`/`otlp_in` gzip: client never accepts a compressed response, server bounds decompressed size](otlp-compression-and-decompression-bounds.md) | 2026-09-03 | 2026-09-03 |
| [Signal filtering is two transform components, not a sink field](signal-filtering-components.md) | 2026-09-03 | 2026-09-03 |
| [Syslog egress: format, transport, and header-field precedence](syslog-output.md) | 2026-09-02 | 2026-09-12 |
| [Relative gauge adjustments (`+`/`-` in statsd)](relative-gauge-adjustments.md) | 2026-09-02 | 2026-09-11 |
| [Internal span emission, one span per node-visit, and deterministic-on-`trace_id` sampling](internal-span-emission-and-deterministic-sampling.md) | 2026-09-02 | 2026-09-02 |
| [Hand-rolled unary gRPC over `hyper`, not `tonic`](hand-rolled-grpc-over-hyper.md) | 2026-09-02 | 2026-09-02 |
| [Decoupled listener I/O](decoupled-listener-io.md) | 2026-09-02 | 2026-09-11 |
| [Committed, pre-generated OTLP protobuf types; no `protoc` in any build path](committed-pregenerated-otlp-protobuf.md) | 2026-09-02 | 2026-09-11 |
| [Buffered, decoupled sink delivery](buffered-sink-delivery.md) | 2026-09-01 | 2026-09-02 |
| [Propagate real trace context on `Delivered`, for the node kinds with one unambiguous parent](trace-context-propagation-on-delivered.md) | 2026-09-01 | 2026-09-01 |
| [Lua-authored telemetry: cardinality is convention-enforced, not type-system-enforced](lua-authored-telemetry-cardinality.md) | 2026-09-01 | 2026-09-01 |
| [A separate demo stack, not an extension of the dev stack](demo-stack-separate-from-dev-stack.md) | 2026-09-01 | 2026-09-01 |
| [Minimize allocations over event size, when the two conflict](minimize-allocations-over-event-size.md) | 2026-08-31 | 2026-08-31 |
| [jemalloc as the global allocator](jemalloc-global-allocator.md) | 2026-08-31 | 2026-08-31 |
| [Internal telemetry as ordinary pipeline events, drained from a component-level buffer](internal-telemetry-as-pipeline-events.md) | 2026-08-31 | 2026-09-12 |
| [`Arc<EventBatch>` copy-on-write on channels](arc-eventbatch-copy-on-write.md) | 2026-08-31 | 2026-08-31 |
| [Service lifecycle: signal-driven shutdown and bounded output retry](service-lifecycle-and-output-retry.md) | 2026-08-30 | 2026-09-02 |
| [`Event` carries a log, metrics, and a span at once, not one of the three](multi-payload-events.md) | 2026-08-30 | 2026-08-30 |
| [`kv_metrics`: skip rules, numeric coercion, and no `tags:` field](kv-metrics-semantics.md) | 2026-08-30 | 2026-08-30 |
| [`aggregate` transform: tumbling windows, pass-through, and the flush-tick contract](aggregation-window-semantics.md) | 2026-08-29 | 2026-09-12 |
| [`json` transform: structured attributes, additive, pass-through on failure](json-parsing-into-attributes.md) | 2026-08-29 | 2026-08-30 |
| [Secrets in config: a general `!env` YAML tag, not per-field `*_env` indirection](env-yaml-tag.md) | 2026-08-29 | 2026-08-30 |
| [Preserving `Value` variant identity across a Lua round-trip](lua-value-identity-preservation.md) | 2026-08-29 | 2026-08-29 |
| [Configuration: a component graph, not inputs/outputs/pipelines](component-graph-configuration.md) | 2026-08-29 | 2026-08-29 |
| [Service language: Rust](service-language-rust.md) | 2026-08-28 | 2026-08-28 |
| [Developer workflow: Scripts to Rule Them All, and PR-based development](scripts-to-rule-them-all.md) | 2026-08-28 | 2026-08-28 |
| [User scripting language: Lua (LuaJIT)](scripting-language-lua.md) | 2026-08-28 | 2026-08-28 |
| [Service-to-service protocol: native wire format, OTLP as a bridge](native-wire-format-with-otlp-bridge.md) | 2026-08-28 | 2026-08-28 |
| [Containerized development environment](containerized-development.md) | 2026-08-28 | 2026-08-28 |
| [Configuration: YAML with a generated JSON Schema](config-yaml-jsonschema.md) | 2026-08-28 | 2026-09-11 |
