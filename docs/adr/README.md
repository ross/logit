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
| [`scale`: unit conversion by constant factor, and why it stays out of `kv_metrics`](scale-transform.md) | 2026-09-03 | 2026-09-03 |
| [`LogRecord` gains a native application trace/span reference](log-record-trace-context.md) | 2026-09-03 | 2026-09-03 |
| [Operator-declared resource attributes: a `set` transform, not a per-input config field](operator-declared-resource-attributes.md) | 2026-09-03 | 2026-09-03 |
| [Syslog egress: format, transport, and header-field precedence](syslog-output.md) | 2026-09-02 | 2026-09-02 |
| [Relative gauge adjustments (`+`/`-` in statsd)](relative-gauge-adjustments.md) | 2026-09-02 | 2026-09-02 |
| [Internal span emission, one span per node-visit, and deterministic-on-`trace_id` sampling](internal-span-emission-and-deterministic-sampling.md) | 2026-09-02 | 2026-09-02 |
| [Hand-rolled unary gRPC over `hyper`, not `tonic`](hand-rolled-grpc-over-hyper.md) | 2026-09-02 | 2026-09-02 |
| [Decoupled listener I/O](decoupled-listener-io.md) | 2026-09-02 | 2026-09-02 |
| [Committed, pre-generated OTLP protobuf types; no `protoc` in any build path](committed-pregenerated-otlp-protobuf.md) | 2026-09-02 | 2026-09-02 |
| [Buffered, decoupled sink delivery](buffered-sink-delivery.md) | 2026-09-01 | 2026-09-02 |
| [Propagate real trace context on `Delivered`, for the node kinds with one unambiguous parent](trace-context-propagation-on-delivered.md) | 2026-09-01 | 2026-09-01 |
| [Lua-authored telemetry: cardinality is convention-enforced, not type-system-enforced](lua-authored-telemetry-cardinality.md) | 2026-09-01 | 2026-09-01 |
| [A separate demo stack, not an extension of the dev stack](demo-stack-separate-from-dev-stack.md) | 2026-09-01 | 2026-09-01 |
| [Minimize allocations over event size, when the two conflict](minimize-allocations-over-event-size.md) | 2026-08-31 | 2026-08-31 |
| [jemalloc as the global allocator](jemalloc-global-allocator.md) | 2026-08-31 | 2026-08-31 |
| [Internal telemetry as ordinary pipeline events, drained from a component-level buffer](internal-telemetry-as-pipeline-events.md) | 2026-08-31 | 2026-08-31 |
| [`Arc<EventBatch>` copy-on-write on channels](arc-eventbatch-copy-on-write.md) | 2026-08-31 | 2026-08-31 |
| [Service lifecycle: signal-driven shutdown and bounded output retry](service-lifecycle-and-output-retry.md) | 2026-08-30 | 2026-09-02 |
| [`Event` carries a log, metrics, and a span at once, not one of the three](multi-payload-events.md) | 2026-08-30 | 2026-08-30 |
| [`kv_metrics`: skip rules, numeric coercion, and no `tags:` field](kv-metrics-semantics.md) | 2026-08-30 | 2026-08-30 |
| [`aggregate` transform: tumbling windows, pass-through, and the flush-tick contract](aggregation-window-semantics.md) | 2026-08-29 | 2026-09-02 |
| [`json` transform: structured attributes, additive, pass-through on failure](json-parsing-into-attributes.md) | 2026-08-29 | 2026-08-30 |
| [Secrets in config: a general `!env` YAML tag, not per-field `*_env` indirection](env-yaml-tag.md) | 2026-08-29 | 2026-08-30 |
| [Preserving `Value` variant identity across a Lua round-trip](lua-value-identity-preservation.md) | 2026-08-29 | 2026-08-29 |
| [Configuration: a component graph, not inputs/outputs/pipelines](component-graph-configuration.md) | 2026-08-29 | 2026-08-29 |
| [Service language: Rust](service-language-rust.md) | 2026-08-28 | 2026-08-28 |
| [Developer workflow: Scripts to Rule Them All, and PR-based development](scripts-to-rule-them-all.md) | 2026-08-28 | 2026-08-28 |
| [User scripting language: Lua (LuaJIT)](scripting-language-lua.md) | 2026-08-28 | 2026-08-28 |
| [Service-to-service protocol: native wire format, OTLP as a bridge](native-wire-format-with-otlp-bridge.md) | 2026-08-28 | 2026-08-28 |
| [Containerized development environment](containerized-development.md) | 2026-08-28 | 2026-08-28 |
| [Configuration: YAML with a generated JSON Schema](config-yaml-jsonschema.md) | 2026-08-28 | 2026-08-28 |
