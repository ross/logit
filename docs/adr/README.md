# Architecture decision records

Each ADR is a single Markdown file named after the decision (`slug.md`, no number — see
[`TEMPLATE.md`](TEMPLATE.md)). A slug is permanent once written. Don't rename a file to fix a typo
or reword a title, because docs, source comments, and configs across the repo cite it by that exact
path. The `created`/`updated` dates in each file's frontmatter order this index, not the filename.

To add an ADR, copy `TEMPLATE.md` to `docs/adr/<slug>.md`, set `created` to today and `updated` to
the same date, write the record, then add a row here.

| ADR | Created | Updated |
|---|---|---|
| [Lua `vars`: a component's `vars:` map reaches its script as a plain-table global](lua-script-vars.md) | 2026-10-21 | 2026-10-21 |
| [Full-buffer diagnostics: a full sink buffer or receive queue logs once a minute, and a gauge reads a blocked push live](buffer-full-diagnostics.md) | 2026-10-21 | 2026-10-21 |
| [`datadog_out` maps OTLP metric kinds as Datadog's OTel exporter does, statelessly](datadog-otlp-metric-mapping.md) | 2026-10-20 | 2026-10-20 |
| [`cumulative_to_delta`: a stateful transform that turns cumulative sums, histograms, and summaries into deltas](cumulative-to-delta-transform.md) | 2026-10-20 | 2026-10-20 |
| [`influxdb_out` writes histograms and summaries in Telegraf's `prometheus-v1` shape](influxdb-histogram-summary-shape.md) | 2026-10-20 | 2026-10-20 |
| [`replay_in`: a finite input that reads a `format: json` or `format: native` file once and ends](replay-input.md) | 2026-10-19 | 2026-10-19 |
| [Transit spec: a mapping table under `spec/transit/` is the canonical statement of every cross-protocol transition, checked by flows and rendered into the docs](transit-spec.md) | 2026-10-19 | 2026-10-21 |
| [`route_signals`: a native signal router that splits a mixed event into one part per target](route-signals.md) | 2026-10-16 | 2026-10-21 |
| [LuaJIT's trace compiler is off by default, and `jit: true` turns it on per component](lua-jit-opt-in.md) | 2026-10-16 | 2026-10-16 |
| [Lua in-place edits: writable log fields, `= nil` deletes an attribute, and `pairs` enumerates a proxy](lua-in-place-edits.md) | 2026-10-16 | 2026-10-18 |
| [`line_protocol`: parse a log message as InfluxDB line protocol into one event per point, skipping bad lines](line-protocol-transform.md) | 2026-10-16 | 2026-10-19 |
| [Native transform fan-out: `Transform::process_into` emits extra events into the same batch](transform-fan-out.md) | 2026-10-16 | 2026-10-18 |
| [`syslog_in` UDP framing: `framing: lines \| datagram`, with today's per-line split as the default](syslog-udp-datagram-framing.md) | 2026-10-16 | 2026-10-18 |
| [Native hop trace context: the hop trailer carries a sampled batch's trace context, and `logit_in` continues it](native-hop-trace-context.md) | 2026-10-15 | 2026-10-15 |
| [Process hostname: one resolved name, `host.name` on `internal`'s resource](process-hostname.md) | 2026-10-14 | 2026-10-14 |
| [CI saves the cargo cache from `main` only, and every run restores it with a prefix fallback](ci-cargo-cache-saved-on-main.md) | 2026-10-14 | 2026-10-14 |
| [CI runs no advisory check: Dependabot files the bump PRs, and `script/audit` checks locally](advisories-off-ci.md) | 2026-10-14 | 2026-10-14 |
| [Private attribute namespace: protocol carriers live under `_logit.<protocol>.`, and every other protocol's sink ignores them](private-attribute-namespace.md) | 2026-10-13 | 2026-10-19 |
| [Stale events are filtered by a `drop_stale`/`has_stale` transform pair](stale-event-filtering.md) | 2026-10-13 | 2026-10-13 |
| [InfluxDB line-protocol relay: fields as typed `Gauge` records under a measurement carrier, strings as attributes, and an all-or-nothing write](influxdb-line-protocol-relay.md) | 2026-10-13 | 2026-10-21 |
| [Typed metric scalar: `MetricValue { F64, I64, U64, Bool }` replaces the `f64` in `Gauge`, `GaugeDelta`, and `Sum`](typed-metric-scalar.md) | 2026-10-13 | 2026-10-18 |
| [Telemetry loss vocabulary: any nonzero `*.dropped` is loss, and intended outcomes use other words](telemetry-loss-vocabulary.md) | 2026-10-13 | 2026-10-21 |
| [`kafka_out` OTLP formats: one export request per signal per record, a topic per signal, and a producer per topic behind a multi-topic facade](kafka-otlp-formats.md) | 2026-10-12 | 2026-10-13 |
| [`kafka_out` SASL: PLAIN, SCRAM, OAUTHBEARER, and AWS MSK IAM, authenticated in the dialer and renewed by redial](kafka-sasl.md) | 2026-10-11 | 2026-10-17 |
| [`kafka_out`: a pure-Rust producer on `kafka-protocol`, with the Java producer's semantics and named divergences](kafka-output.md) | 2026-10-11 | 2026-10-18 |
| [Lua `random`: a global that mints trace ids, span ids, UUIDs, and ULIDs, with the time passed in](lua-random-ids.md) | 2026-10-11 | 2026-10-11 |
| [`bucket`: a numeric-range classifier that writes a bounded attribute value](bucket-transform.md) | 2026-10-11 | 2026-10-11 |
| [Run CI in an image prebaked from `Dockerfile.dev` and published to GHCR](ci-image-on-ghcr.md) | 2026-10-09 | 2026-10-09 |
| [Parsers of producer bytes live in `logit-proto`, and listeners and transforms wrap them](parsers-live-in-logit-proto.md) | 2026-10-09 | 2026-10-09 |
| [A soak harness: real containers over ordinary config, one-shot netem, and a reset-aware ledger as the loss oracle](soak-harness.md) | 2026-10-08 | 2026-10-18 |
| [`statsd_out` multi-value expansion: summarized kinds leave as dotted counter and gauge lines by default](statsd-out-multi-value-expansion.md) | 2026-10-07 | 2026-10-21 |
| [Listener port sharing with `SO_REUSEPORT`, a SIGTERM drain delay, and an admin Unix socket](listener-port-sharing-and-shutdown-delay.md) | 2026-10-07 | 2026-10-13 |
| [TLS certificate reload: swappable resolvers and verifiers, triggered by a file poll and SIGHUP](tls-certificate-reload.md) | 2026-10-07 | 2026-10-07 |
| [Forwarded-header parsing: one named header per component, the leftmost client, and a shared parser for the HTTP listeners and `http_access`](forwarded-header-parsing.md) | 2026-10-05 | 2026-10-05 |
| [Signal handling: SIGHUP reopens file targets and never exits, and every handler is installed before config load](signal-handling.md) | 2026-10-05 | 2026-10-07 |
| [Listener peer address: opt-in `network.peer.*` on the shared drivers, and PROXY protocol on TCP](listener-peer-address.md) | 2026-10-05 | 2026-10-18 |
| [`lines_in`: a plain-lines listener that emits one raw log event per line and parses nothing](plain-lines-listener.md) | 2026-10-05 | 2026-10-05 |
| [Native hop ack status: the sender pair leads the hop payload, and a rejected `Ack` settles one frame by name](native-hop-ack-status.md) | 2026-10-04 | 2026-10-15 |
| [Sink fault classes: a rejected batch drops, a refused destination holds, and the process never exits for a sink](sink-fault-classes.md) | 2026-10-04 | 2026-10-18 |
| [jiff is the one calendar implementation, behind the codecs' strict RFC 5424 contract](jiff-for-calendar-time.md) | 2026-10-04 | 2026-10-04 |
| [`timestamp`: resolving `event.timestamp` from an attribute, with jiff for calendar time](timestamp-transform.md) | 2026-10-04 | 2026-10-18 |
| [Lua refusals: `Event.new` raises a refusal from a Lua shim, not as an mlua callback error](lua-refusals-raised-from-lua.md) | 2026-10-03 | 2026-10-03 |
| [Crate layout: don't split crates to speed up the dev build](crate-layout-and-build-speed.md) | 2026-10-03 | 2026-10-11 |
| [Release profile: fat LTO and one codegen unit, measured against thin LTO and 16 units](release-build-profile.md) | 2026-10-03 | 2026-10-15 |
| [Native hop acks: a named cumulative `Ack`, coalesced at `logit_in`, and a resume mark in `HelloAck`](native-hop-named-acks.md) | 2026-10-02 | 2026-10-18 |
| [Native hop: two payload shapes named by shape, every hop frame sequenced, and strict control messages](native-hop-no-compatibility.md) | 2026-10-01 | 2026-10-18 |
| [Native hop send window: several frames in flight, acknowledged in frame order](native-hop-send-window.md) | 2026-10-01 | 2026-10-18 |
| [Native hop identity and sequence: a per-store sender identity and sequence on every hop frame, and a high-water mark at `logit_in`](native-hop-identity-and-sequence.md) | 2026-10-01 | 2026-10-18 |
| [Sink send path and attempt accounting: counters that say what they count, one pooled-stream driver, and TLS writes that are flushed](sink-send-path-and-attempt-accounting.md) | 2026-09-29 | 2026-10-13 |
| [Delivery semantics: at-least-once per hop, duplicates absorbed by the data model, and an effectively-once native hop](delivery-semantics.md) | 2026-09-29 | 2026-10-20 |
| [Tail discovery failure and resume identity: a failed listing is no information, and a resume verifies the file's head](tail-discovery-failure-and-resume-identity.md) | 2026-09-28 | 2026-10-18 |
| [Process-level metrics from procfs and the allocator](process-level-metrics.md) | 2026-09-28 | 2026-10-13 |
| [`stdio_out`/`file_out`'s human render: an exhaustive, sectioned block per event](human-render-block-format.md) | 2026-09-28 | 2026-10-02 |
| [`stdio_out`/`file_out` gain `format: json`, and every machine reader of the text render moves to it](stream-json-format.md) | 2026-09-28 | 2026-10-19 |
| [Test timing: wait on an observable, size every sleep, and share one set of helpers](test-timing-and-observables.md) | 2026-09-28 | 2026-10-19 |
| [Shutdown accounting and cancellation safety: every shutdown loss counted, and one table of cancellation points](shutdown-accounting-and-cancellation-safety.md) | 2026-09-26 | 2026-10-13 |
| [Lua scripts: stall detection, a progress-based wedge check, opt-in `max_memory`, and a table-depth cap](lua-runaway-script-bounds.md) | 2026-09-26 | 2026-10-16 |
| [Deployment threat model: accidental data inside a trust boundary, not a malicious peer](deployment-threat-model.md) | 2026-09-25 | 2026-09-25 |
| [Untrusted-input bounds: one set of rules for every decoder and listener a peer can reach](untrusted-input-bounds.md) | 2026-09-25 | 2026-10-09 |
| [Out-of-CI fuzzing: a `cargo-fuzz` workspace in the unsafe-check image, with every crash landed as a stable test](out-of-ci-fuzzing.md) | 2026-09-25 | 2026-10-18 |
| [Splunk HEC: a lossless pair in the OpenTelemetry exporter's vocabulary, spans as HEC events, and opt-in acknowledgment](splunk-hec-relay.md) | 2026-09-25 | 2026-10-18 |
| [VictoriaMetrics interop: existing components, plus zstd on Prometheus remote-write](victoriametrics-interop.md) | 2026-09-24 | 2026-10-18 |
| [Durable checkpoint writes, observed spool I/O failures, and a feature-gated fault-injection seam](durable-checkpoint-writes-and-fault-injection.md) | 2026-09-24 | 2026-09-28 |
| [Datadog: two lossless pairs, the Agent's own protocols, and a Datadog-mapped `DdSketch`](datadog-agent-and-intake-relay.md) | 2026-09-23 | 2026-10-20 |
| [`sample`: consistent, keyed event sampling with an operator override](consistent-sampling-component.md) | 2026-09-22 | 2026-10-13 |
| [Publish the release image to GHCR, `latest` only, on manual dispatch](publish-release-image-to-ghcr.md) | 2026-09-22 | 2026-10-09 |
| [`http_access`: web-server access lines normalized to OTel semconv once, natively, from raw fields](http-access-normalization.md) | 2026-09-22 | 2026-10-11 |
| [Out-of-CI verification of raw-`libc` `unsafe`: a throwaway nightly image, not a Dockerfile.dev change](out-of-ci-unsafe-verification.md) | 2026-09-21 | 2026-09-25 |
| [`flatten`: dotted-key expansion as an opt-in, operator-placed transform](flatten-transform.md) | 2026-09-21 | 2026-10-13 |
| [Event sizing: `AttrMap`'s inline capacity stays 8, and attribute maps are not pre-sized](event-sizing-and-allocation-strategy.md) | 2026-09-21 | 2026-10-18 |
| [`shape`: an observer component that turns each event into measurements of its own shape](shape-observer-component.md) | 2026-09-20 | 2026-10-18 |
| [A disposable Azure VM for perf measurement: `up`/`down` only, no stop and no snapshot](disposable-azure-perf-vm.md) | 2026-09-18 | 2026-10-18 |
| [UDP intake batching and socket visibility](udp-intake-batching-and-socket-visibility.md) | 2026-09-18 | 2026-10-18 |
| [Prometheus remote-write: a receiver on `prometheus_in`, a sender on `prometheus_out`](prometheus-remote-write.md) | 2026-09-17 | 2026-10-20 |
| [`docker_in`: live container identity, and a minimal `inotify` watch set](docker-container-identity-and-minimal-watches.md) | 2026-09-17 | 2026-10-18 |
| [`Transform::process` transforms in place: `&mut Event -> bool`, not `Event -> Option<Event>`](in-place-transform-process.md) | 2026-09-17 | 2026-10-16 |
| [`keep_values`: an attribute-value allowlist, and an optional normalize-before-compare step](value-allowlist-cardinality-clamp.md) | 2026-09-15 | 2026-09-15 |
| [A Lua `flush()` runs in a root context, not the last batch's](lua-flush-root-context.md) | 2026-09-15 | 2026-09-15 |
| [`Event.new(t)`: Lua constructs events from the table shape `event:to_table()` already emits](lua-event-constructor.md) | 2026-09-15 | 2026-10-18 |
| [Idle-connection timeouts on TCP listeners: an opt-in `idle_timeout`, a next-byte deadline, and a client-side pooled-connection probe](idle-connection-timeout.md) | 2026-09-14 | 2026-10-11 |
| [`syslog_in` gains TCP and TLS ingress; `syslog_out` gains TLS](syslog-tcp-ingress-and-tls.md) | 2026-09-13 | 2026-10-18 |
| [Graphite/Carbon relay: untyped datapoints as `Gauge`, tags as attributes, a restricted pickle codec, and a multi-value switch](graphite-carbon-relay.md) | 2026-09-13 | 2026-10-21 |
| [`target` components: named destinations a router directs events into, beside `sources:`](target-components.md) | 2026-09-13 | 2026-10-21 |
| [A load-test harness: the real binary, a declarative event template, and CPU per event as the signal](load-test-harness.md) | 2026-09-12 | 2026-10-18 |
| [`FramedEncoder`: a third codec trait for sinks that need per-message framing, over a shared `MessageBuf`](framed-encoder.md) | 2026-09-12 | 2026-10-18 |
| [collectd binary-protocol relay: identity as attributes, value types as `Sum`/`Gauge`, and a packing encoder](collectd-binary-relay.md) | 2026-09-12 | 2026-10-18 |
| [Prometheus scrape ingestion and exposition: transports, dialects, and the model mapping](prometheus-scrape-and-exposition.md) | 2026-09-11 | 2026-10-21 |
| [Metrics model v2: `Sum` replaces `Counter`, raw/summarized pairs, boxed span fidelity, batch-level `Scope`](metrics-model-v2.md) | 2026-09-11 | 2026-10-18 |
| [RFC 5424 structured-data convention: nested `syslog.sd`, strict parsing, opt-in PEN-qualified emission](syslog-structured-data-convention.md) | 2026-09-11 | 2026-10-18 |
| [A shared build cache and one-container check execution for the local development loop](fast-local-development-loop.md) | 2026-09-11 | 2026-09-11 |
| [statsd/DogStatsD egress: dialect, transport, packing, and the v1 metric-kind deferral](statsd-output.md) | 2026-09-10 | 2026-10-19 |
| [Lossless like-protocol transit: the internal model is a superset of every supported wire protocol](lossless-transit.md) | 2026-09-10 | 2026-10-21 |
| [Browser tracing: the real OTel-JS SDK, `addLink` for sub-resources, and living with document-load's parent (not link) behaviour](browser-tracing-sdk.md) | 2026-09-10 | 2026-10-18 |
| [OTLP/JSON decoding: hand-written against `serde_json::Value`, not generated](otlp-json-decoding.md) | 2026-09-10 | 2026-10-18 |
| [Provenance filtering is two transform components, combining has_attributes' and has_signal's shapes](provenance-filtering-components.md) | 2026-09-10 | 2026-10-13 |
| [Batch provenance (`origin`/`previous`) carried on `Delivered`, stamped by `Fanout`](batch-provenance-on-delivered.md) | 2026-09-10 | 2026-10-15 |
| [Attribute filtering is two transform components, and a bounded matcher is not a predicate language](attribute-filtering-components.md) | 2026-09-10 | 2026-10-18 |
| [Native transport: handshake, implicit sequencing, and per-batch acknowledgement](native-transport-handshake-and-ack.md) | 2026-09-09 | 2026-10-18 |
| [`tracing` for self-logging, with `Diagnostics` as its producer](tracing-for-self-logging.md) | 2026-09-09 | 2026-09-09 |
| [A top-level `admin:` block, not a component, for readiness/liveness](admin-readiness-endpoint.md) | 2026-09-09 | 2026-10-07 |
| [Disk-backed durable buffering for a sink's delivery queue](disk-backed-sink-buffer.md) | 2026-09-09 | 2026-10-18 |
| [`stdio_out`/`file_out` gain a `native` wire-format option](file-output-native-format.md) | 2026-09-09 | 2026-10-19 |
| [`file_out`: a rotating file sink, sharing `stdio_out`'s implementation](rotating-file-output.md) | 2026-09-08 | 2026-10-05 |
| [Native wire format encoding: hand-rolled, not `rkyv` or a `serde`/`postcard` derive](native-wire-format-encoding.md) | 2026-09-08 | 2026-09-25 |
| [Routing by condition, sampling, throttling, dedup, and renaming are `lua` components](routing-by-condition-is-lua.md) | 2026-09-07 | 2026-10-16 |
| [`logfmt` and `kv`: the de-facto key=value parsers, and why they stay two kinds](logfmt-and-kv-parsing.md) | 2026-09-07 | 2026-10-11 |
| [`regex`: named captures into attributes, and taking the `regex` crate](regex-transform.md) | 2026-09-07 | 2026-09-07 |
| [`csv`: positional columns from config, not a header row, and no type coercion](csv-positional-columns.md) | 2026-09-07 | 2026-09-07 |
| [`tail_in`: generic file tailing, and `docker_in` on top of it for Docker's json-file logs](file-tailing-and-docker-json-logs.md) | 2026-09-06 | 2026-10-18 |
| [`trace_context` grows a `span:` block, and a native `traceparent` parser](trace-context-span-lifting.md) | 2026-09-04 | 2026-10-18 |
| [TLS for `otlp_out`/`otlp_in`, and a pooled gRPC client to carry it](otlp-tls-and-pooled-grpc-client.md) | 2026-09-03 | 2026-10-18 |
| [`scale`: unit conversion by constant factor, and why it stays out of `kv_metrics`](scale-transform.md) | 2026-09-03 | 2026-09-22 |
| [`LogRecord` gains a native application trace/span reference](log-record-trace-context.md) | 2026-09-03 | 2026-10-18 |
| [Operator-declared resource attributes: a `set` transform, not a per-input config field](operator-declared-resource-attributes.md) | 2026-09-03 | 2026-10-18 |
| [`otlp_out`/`otlp_in` gzip: client never accepts a compressed response, server bounds decompressed size](otlp-compression-and-decompression-bounds.md) | 2026-09-03 | 2026-10-18 |
| [Signal filtering is two transform components, not a sink field](signal-filtering-components.md) | 2026-09-03 | 2026-10-21 |
| [Syslog egress: format, transport, and header-field precedence](syslog-output.md) | 2026-09-02 | 2026-10-18 |
| [Decoupled listener I/O](decoupled-listener-io.md) | 2026-09-02 | 2026-10-07 |
| [Relative gauge adjustments (`+`/`-` in statsd)](relative-gauge-adjustments.md) | 2026-09-02 | 2026-10-13 |
| [Committed, pre-generated OTLP protobuf types; no `protoc` in any build path](committed-pregenerated-otlp-protobuf.md) | 2026-09-02 | 2026-09-11 |
| [Internal span emission, one span per node-visit, and deterministic-on-`trace_id` sampling](internal-span-emission-and-deterministic-sampling.md) | 2026-09-02 | 2026-10-15 |
| [Hand-rolled unary gRPC over `hyper`, not `tonic`](hand-rolled-grpc-over-hyper.md) | 2026-09-02 | 2026-10-18 |
| [Buffered, decoupled sink delivery](buffered-sink-delivery.md) | 2026-09-01 | 2026-10-18 |
| [Propagate real trace context on `Delivered`, for the node kinds with one unambiguous parent](trace-context-propagation-on-delivered.md) | 2026-09-01 | 2026-09-15 |
| [Lua-authored telemetry: cardinality is convention-enforced, not type-system-enforced](lua-authored-telemetry-cardinality.md) | 2026-09-01 | 2026-09-01 |
| [A separate demo stack, not an extension of the dev stack](demo-stack-separate-from-dev-stack.md) | 2026-09-01 | 2026-10-18 |
| [Internal telemetry as ordinary pipeline events, drained from a component-level buffer](internal-telemetry-as-pipeline-events.md) | 2026-08-31 | 2026-10-18 |
| [Minimize allocations over event size, when the two conflict](minimize-allocations-over-event-size.md) | 2026-08-31 | 2026-08-31 |
| [jemalloc as the global allocator](jemalloc-global-allocator.md) | 2026-08-31 | 2026-09-28 |
| [`Arc<EventBatch>` copy-on-write on channels](arc-eventbatch-copy-on-write.md) | 2026-08-31 | 2026-08-31 |
| [Service lifecycle: signal-driven shutdown and bounded output retry](service-lifecycle-and-output-retry.md) | 2026-08-30 | 2026-10-18 |
| [`Event` carries a log, metrics, and a span at once, not one of the three](multi-payload-events.md) | 2026-08-30 | 2026-10-18 |
| [`kv_metrics`: skip rules, numeric coercion, and no `tags:` field](kv-metrics-semantics.md) | 2026-08-30 | 2026-10-18 |
| [Configuration: a component graph, not inputs/outputs/pipelines](component-graph-configuration.md) | 2026-08-29 | 2026-09-13 |
| [`aggregate` transform: tumbling windows, pass-through, and the flush-tick contract](aggregation-window-semantics.md) | 2026-08-29 | 2026-10-19 |
| [`json` transform: structured attributes, additive, pass-through on failure](json-parsing-into-attributes.md) | 2026-08-29 | 2026-09-22 |
| [Secrets in config: a general `!env` YAML tag, not per-field `*_env` indirection](env-yaml-tag.md) | 2026-08-29 | 2026-10-21 |
| [Preserving `Value` variant identity across a Lua round-trip](lua-value-identity-preservation.md) | 2026-08-29 | 2026-10-16 |
| [Configuration: YAML with a generated JSON Schema](config-yaml-jsonschema.md) | 2026-08-28 | 2026-10-04 |
| [Service language: Rust](service-language-rust.md) | 2026-08-28 | 2026-08-28 |
| [Developer workflow: Scripts to Rule Them All, and PR-based development](scripts-to-rule-them-all.md) | 2026-08-28 | 2026-08-28 |
| [User scripting language: Lua (LuaJIT)](scripting-language-lua.md) | 2026-08-28 | 2026-08-28 |
| [Service-to-service protocol: native wire format, OTLP as a bridge](native-wire-format-with-otlp-bridge.md) | 2026-08-28 | 2026-08-28 |
| [Containerized development environment](containerized-development.md) | 2026-08-28 | 2026-10-09 |
