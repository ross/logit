# Known gaps: Datadog

Entry format and the other areas: [the known-gaps index](README.md).

- **`datadog_in` doesn't speak every route an Agent can send to.** Each of these gets `404`,
  counted `logit.input.requests.rejected{reason="unknown_route"}`, so an Agent reports it rather
  than losing data silently:
  - The v3 columnar series routes (`/api/intake/metrics/v3/series` and its siblings). An Agent
    sends v3 only to Datadog's own URLs (`use_v3_api.series.enabled: datadog_only`), so a
    redirected Agent sends v2, which `datadog_in` serves. A recorded Agent 7.83 with `dd_url` at
    another host sent every series request to `/api/v2/series`
    (`testdata/interop/datadog/README.md`), and Agent 7.83.3 with `dd_url` at a `datadog_in` sent
    only v2 protobuf series.
  - The legacy TCP logs intake (port 10516, `<api-key> <json>\n` or length-prefixed protobuf).
    It isn't HTTP, so it can't share this listener. Set `logs_config.force_use_http: true` on the
    Agent.
  - An API key in the query string (`?api_key=`) or the path (`/v1/input/<key>`) on a data route.
    Only the `DD-API-KEY` header authenticates a data route, and a current Agent sends it on every
    one. The validate and `/_health` probes also read `?api_key=`, because that's the only way an
    Agent's own key check sends its key (`testdata/interop/datadog/README.md`).
  - **Consequence:** an Agent configured for any of these shows errors against `datadog_in`.
  - **Revisit trigger:** a later Agent's recorded traffic, re-recorded with
    `script/record-fixtures datadog-agent`, shows one of them. A 7.83 Agent's doesn't.
- **`datadog_in` with no `api_keys` accepts any key, the validate routes included.** Validation
  always answers `200`, so an Agent pointed at it can't detect a mistyped key.
  - **Consequence:** a key typo surfaces only when the same Agent also talks to Datadog.
  - **Workaround:** set `api_keys`, which makes `/api/v1/validate` and `/api/v2/validate` check
    the key.
- **`datadog_trace_in` doesn't decode JSON trace bodies.** A `/v0.3/traces` or `/v0.4/traces`
  request with `Content-Type: application/json` gets `415`, counted
  `logit.input.requests.rejected{reason="json_traces"}`. The Agent accepts that form; the codec
  implements only msgpack.
  - **Consequence:** a tracer or client that sends JSON traces loses them. No current dd-trace
    library sends JSON by default.
  - **Revisit trigger:** a user shows a JSON sender. The recorded dd-trace-py 4.15 sends msgpack
    on both of its forms (`testdata/interop/datadog/README.md`).
- **`datadog_trace_in` doesn't speak the v1.0 string-table trace form (`idx`).** `/v1.0/traces`
  gets `404`, and `/info` doesn't list it, so a tracer that reads `/info` falls back to v0.4 or
  v0.5.
  - **Consequence:** none for a tracer that honors `/info`. A tracer hard-configured for v1.0
    loses its traces.
  - **Revisit trigger:** a tracer that sends v1.0 whatever `/info` says. dd-trace-py 4.15 reads
    `/info` and sends v0.5.
- **`datadog_trace_in` does none of the Agent's processing, and no Agent-equivalent trace
  processor exists.** Spans relay as the tracer wrote them. Between tracer and intake, the Agent
  (`pkg/trace/agent/agent.go`, `Process`) does what Datadog's trace intake expects to have been
  done:
  - normalization: `service`/`name` at most 100 chars with `unnamed-service`/`unnamed_operation`
    fallbacks, an empty `resource` set to `name`, `env` normalized, an invalid `http.status_code`
    removed;
  - obfuscation by `span.type` (SQL, Redis, memcached, HTTP URLs, MongoDB, Elasticsearch), unless
    the tracer sent `Datadog-Obfuscation-Version`;
  - truncation: `resource` at most 5,000 chars, meta values at most 25,000;
  - `_top_level` marking of root, orphan, and service-boundary spans, or the tracer's
    `_dd.top_level` when `Datadog-Client-Computed-Top-Level` is set;
  - sampler tags (`_dd.agent_psr`, `_dd.rule_psr`, chunk `_dd.p.dm`, `_dd1.sr.rcusr`), and the
    chunk's `priority` and `origin` lifted from the root span's `_sampling_priority_v1` and
    `_dd.origin`;
  - a stats concentrator: 10 s buckets keyed by service, name, resource, type, kind, and status,
    DDSketch durations, and `1/_sample_rate` weights, computed from every span before sampling.

  A transform of its own (`datadog_apm`, say) between `datadog_trace_in` and `datadog_out` could
  do this, with obfuscation as an operator choice. Its output would be ready to send because it
  writes the same `_top_level` mark `datadog_out` keys on
  ([ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md), decision 2).
  - **Consequence:** `datadog_trace_in`'s output must reach Datadog through a real Agent
    (`datadog_trace_out`) or go to an OTLP backend. Fed straight to `datadog_out`, its spans are
    dropped as not yet processed, counted
    `logit.output.records.dropped{reason="needs_agent_processing"}`. OTel-origin spans reach
    Datadog through `otlp_out`.
  - **Revisit trigger:** a deployment that needs tracer spans in Datadog with no Agent in the
    path.
- **A libdatadog tracer sends `datadog_trace_in` no client stats.** dd-trace-py 4.x computes them
  only when `/info` says `client_drop_p0s: true`, and `datadog_trace_in` says `false` so the
  tracer drops no span before the relay sees it (`testdata/interop/datadog/README.md`).
  - **Consequence:** none in Datadog, where a downstream Agent computes the stats from the relayed
    spans. A pipeline that reads the stats themselves from `datadog_trace_in` gets only an older
    tracer's.
  - **Revisit trigger:** a pipeline that needs a tracer's own stats more than its priority-0
    spans, which `client_drop_p0s: true` would let the tracer drop before the relay sees them.
- **`datadog_out`'s size limits for distribution points and logs are tighter than the
  intake's.** Only the series limit is the intake's own: a 512,180 B gzip series body drew `413`
  ("limit=512 kB").
  - Distribution points reuse the series limits, but a trial org accepted a 1,052,533 B gzip
    distribution-points body (150,000 values, all counted).
  - Logs keep the documented 5,000,000 B, but the trial org accepted a 5,252,247 B logs body (all
    21 logs stored).
  - Sketches also reuse the series limits, unmeasured: no oversized sketch body was sent.
  - Service checks and stats are sent uncapped.
  - **Consequence:** extra requests on these routes, never a `413`.
  - **Revisit trigger:** Datadog documents these routes' limits, or the extra requests show up in
    a sink's request rate.
- **`datadog_out` drops the OTLP log fields a Datadog log has no slot for, and writes a
  non-string message as text.** A log's `event_name`, observed time, body format, trace flags,
  and dropped-attribute count leave nowhere, and `_logit.otel.severity_number` and
  `_logit.otel.severity_text` are left off, so OTLP's `INFO2` arrives as `info`. A `Map` or
  `Array` message arrives as its JSON text, a `Bytes` one as lossy UTF-8, a log with no body as
  the message `null`, and a `Bytes` attribute as base64 text. The trace and span ids arrive as the
  `trace_id` and `span_id` attributes, unless the log already has an attribute of either name:
  then the attribute is written, so a relayed Datadog log leaves as it arrived, and the log's own
  ids are left off, counted as `logit.output.tags.omitted{reason="reserved_key"}` once per log
  (`log/trace_beside_id_attribute` in the transit spec).
  - **Consequence:** an OTLP log relayed to Datadog, or back out of `datadog_in`, can't be
    rebuilt field for field.
  - **Revisit trigger:** an operator needs one of these fields in Datadog.
  - The transit spec's `otlp-logs-to-datadog` flow states each as a cell.
- **A `datadog_out` or `datadog_trace_out` resend isn't idempotent: Datadog stores a resent log
  twice.** Both sinks remember which requests of a batch the destination settled, so a retry
  resends only the rest ([ADR `sink-fault-classes`](../adr/sink-fault-classes.md), "Amendment: the
  Datadog sinks retry per request (2026-10-09)"). A resend still happens in two places: the
  request that drew an `Ambiguous` answer is resent under `at_least_once` and may have been
  applied, and a `buffer.disk:` replay after a crash resends every request of the batch because the
  memory is in-process (`docs/known-gaps/sinks.md`, "The per-request retry memory of `otlp_out` and
  the Datadog sinks is in-process"; `docs/deploying.md`'s "Durable buffering"). A trial org received two resends:
  - A series point resent at the same `(series, timestamp)` was stored once, the last write
    winning: a count sent twice read 5, not 10, and a gauge sent as 7 then 9 read 9.
  - An identical log posted twice was stored as two logs.

  Every other route (distribution points, sketches, events, checks, traces, stats) is assumed to
  store a resend again until measured.
  - **Consequence:** the default posture, `at_least_once`, accepts a duplicate of the one request
    that drew a `5xx` or timeout, and of a whole batch on a crash replay: duplicate logs, and
    assumed inflated distribution, sketch, event, check, trace, and stats counts. Series points
    overwrite. `buffer: {delivery: at_most_once}` drops that request's batch instead.
  - **Revisit trigger:** a measurement showing another route dedupes a resend, or an operator who
    needs the replay to be duplicate-free.
- **`datadog_out` collapses a series' points in one second only within one batch.** Of the
  points of one series in one second in a batch, the sink sends the latest and leaves the rest
  off, counted `metrics.dropped{reason="duplicate_timestamp"}`, or
  `metrics.normalized{reason="duplicate_timestamp"}` for a gauge (not a `rate`) or cumulative
  point equal to the one sent, because
  Datadog keeps only the higher of two in one request (verified 2026-10-21;
  [ADR `dedup-timestamps-transform`](../adr/dedup-timestamps-transform.md)'s "Verified destination
  behavior"). It plans that over the whole batch before cutting requests, so no cut splits a pair,
  but it holds no state between batches.
  - **Consequence:** two such points in two batches are both sent, and Datadog keeps the one that
    arrives later, so the other is lost, uncounted; for a `count`, that's an increment. Above
    `concurrency: 1`, the one that arrives later can be the older batch's. `dedup_timestamps`
    holds no state between batches either.
  - **Workaround:** keep one series' readings of one second in one batch: an `aggregate` upstream
    leaves one point per series per window.
- **`datadog_out` sends a container id unresolved.** `container.id` leaves as a bare
  `container_id` tag (a `ddtags` entry on a log, the payload field on a trace or stats payload).
  An Agent resolves a container id through its tagger into the container's image, name, and
  orchestrator tags, and a DogStatsD client's `|c:in-<inode>` through the cgroup; `logit` has no
  tagger, so it sends neither the extra tags nor anything for an inode, which it carries as
  `_logit.statsd.cgroup_inode`.
  - **Consequence:** a DogStatsD sample relayed straight to the intake lacks the container tags an
    Agent would add, and one that named only an inode carries no container at all.
  - **Workaround:** send DogStatsD through a local Agent with `statsd_out`, which writes `|c:` back
    in the form the Agent reads, or stamp the tags upstream with `set`.
  - `datadog_in` doesn't read the tag back as `container.id`: a metric's arrives as the
    attribute `container_id`, and a log's stays an entry of its `ddtags` string, so a log relayed
    on to `otlp_out` or `syslog_out` carries no `container.id`.
- **A relayed log keeps one value per service identity key.** `datadog_in` lifts a log's `ddtags`
  `env:`, `version:`, and `service:` entries onto one `deployment.environment.name`,
  `service.version`, and `service.name` each, the log's own `service` field first. A `service:`
  entry beside that field, and a second valued entry of any of the three, is left off, counted
  `logit.input.tags.omitted{reason="reserved_key"}`.
  - **Consequence:** an Agent-tailed container log that carries its service as both the field
    and a tag reaches Datadog through the relay with the field alone, and a log tagged with two
    environments keeps the first.
  - **Workaround:** none needed for the twice-carried service, since Datadog reads the field as
    the log's service. Give a log one value per tag upstream.
- **`datadog_trace_out` derives no Datadog fields from an OTel span.** A span without
  `service.name`, `resource.name`, or `span.type` reaches the Agent with `service`, `resource`, or
  `type` empty, and the Agent computes no stats for it.
  - **Consequence:** OTel-origin spans through this sink show up in Datadog poorly named.
  - **Workaround:** send OTel spans with `otlp_out`, to the Agent's OTLP receiver or to Datadog.
  - **Revisit trigger:** the Agent-equivalent trace processor (the `datadog_trace_in` entry
    above).
- **`datadog_trace_out` and `datadog_out` send every numeric span attribute as a Datadog
  `metrics` entry.** The span encoder both sinks share puts each `F64`, `I64`, and `U64` attribute in the span's `metrics` map, each
  `Bytes` attribute in `meta_struct`, and strings, bools (as `"true"` or `"false"`), and every
  other value in `meta`. That is right for a span `datadog_trace_in` decoded, because the decoder
  stores a tracer's `metrics` map as `F64` attributes, its `meta` map as strings, and its
  `meta_struct` as `Bytes` (the mapping table in `logit_proto::datadog`). It is wrong for a span
  from any other source: an OTel span's integer attribute such as `http.response.status_code`
  (`I64`) leaves in `metrics`, where Datadog reads a numeric span measure rather than a tag. The
  sink counts an integer that loses bits in the `f64` conversion (`degraded{reason="int_as_f64"}`)
  but not this misclassification. The decode side counts `key_collision` and has no such gap.
  - **Consequence:** numeric attributes from OTel-origin and Lua-authored spans reach Datadog as
    span measures, not tags.
  - **Workaround:** a `lua` stage ahead of the sink that rewrites the integer attributes you want
    as tags into strings.
  - **Revisit trigger:** a user who needs numeric OTel attributes as Datadog tags. Possible fixes,
    not decided: a typed home for span metrics (a `metrics` map on `SpanExt`), or a rule that
    depends on where the span came from.
- **`datadog_trace_out` counts nothing for an event with no span.** The sink sends spans and APM
  stats alone. A log-only or metrics-only event takes no route and is left off, and so is a log
  or a metric beside a span, with no `logit.output.events.skipped` or other counter, where
  `datadog_out` counts an event with no payload as `events.skipped{reason="no_payload"}`. The
  transit spec's `log` and `gauge` cells for the sink say `skipped`, uncounted.
  - **Consequence:** a pipeline that routes logs or metrics to `datadog_trace_out` by mistake
    loses them with nothing in the sink's telemetry to show it.
  - **Workaround:** put `has_signal` or `route_signals` ahead of the sink so only span events
    reach it, and compare the upstream node's `events.sent` with the sink's.
  - **Revisit trigger:** an operator who loses data this way, or a sink-wide rule that an ignored
    signal is counted.
- **`datadog_out` sends no Agent-style `h`/`ms` aggregates.** An Agent turns a DogStatsD `h` or
  `ms` into `.avg`, `.count`, `.median`, `.95percentile`, and `.max` series. `aggregate` in front
  of `datadog_out` sends a sketch instead, which Datadog computes quantiles from.
  - **Consequence:** dashboards built on an Agent's `.95percentile`-style series find no data
    when the same metrics arrive through `aggregate` and `datadog_out`; query the distribution
    instead.
  - **Revisit trigger:** an operator who needs the Agent's five series.
- **Whether an Agent's TCP `logs` listener parses a JSON body from `syslog_out` is untested.** An
  Agent's TCP `logs` listener takes `syslog_out`'s syslog-formatted lines, but no run has checked
  that it parses a JSON body into attributes.
  - **Consequence:** logs into an Agent go through `otlp_out` with the Agent's OTLP logs turned on.
  - **Revisit trigger:** a user who needs to send logs to an Agent's TCP `logs` listener.
- **Some Datadog behavior wasn't exercised by the recorded corpus or the trial-org run.** Each of
  these is implemented from the Agent's source or Datadog's docs and covered by the codec's own
  tests, but no real sender or Datadog org has checked it:
  - Whether the intake accepts an APM stats sketch whose gamma isn't 1.0202. A relayed stats
    sketch keeps its own gamma, and dd-trace-py 4.15 computes on 1.015625; nothing in `logit`
    sends one to Datadog yet ([ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md)'s
    "Consequences").
  - Service checks sent by `datadog_out`, which got `202` but which Datadog has no API to query.
  - What the check and logs routes drop from a request they answer `202`. A check and a log with
    101 tags each drew `202` (`{"status":"ok"}` and `{}`) on 2026-10-08, and `datadog_out` reads
    neither body, so a drop there goes uncounted.
  - The Agent's dual-shipping (`additional_endpoints`) and TLS settings against `datadog_in`.
  - v0.7 traces and a `PUT` from a real tracer, and any tracer other than dd-trace-py.
  - The nested `dd` log fields that dd-trace-js and dd-trace-rb write, which `flatten` expands.
  - **Consequence:** a failure here shows up in a deployment first, as `datadog_out` request
    errors or a listener's `rejected` counters.
  - **Revisit trigger:** re-record with `script/record-fixtures datadog` against another tracer,
    or repeat the trial-org run for the item in question.
- **The hand-rolled `DdSketch` store costs about 4% CPU on a sketch-heavy stage.** `aggregate`
  sketches every series, and `kv_metrics` sketches a `Samples` metric at the sink on
  `json-parse`'s path, so both pay for it. Measured before and after the store landed, `aggregate`
  went from 0.325 to 0.339 µs/event and `json-parse` from 0.895 to 0.934 µs/event
  ([performance.md §9](../design/performance.md#9-datadog-stack-the-sparse-sketch-store-and-serde_jsons-float_roundtrip-2026-09-24)).
  - **Consequence:** accepted for bin-for-bin Datadog parity. A cheaper store that didn't match
    the Agent's own bin mapping would relay a sketch that reads differently at Datadog's end ([ADR
    `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md)).
  - **Revisit trigger:** a sketch-heavy pipeline where 4% matters. The Agent's own mitigation is
    a fixed-size key buffer (`pkg/util/quantile/agent.go` buffers 512 keys and merges them into
    the sorted store in one pass) instead of a binary search plus `Vec::insert` per value.
    Measure on the VM before believing it helps.
- **`datadog_out` sketches an OTLP histogram point with no buckets as a one-bucket point.**
  `otlp_in` reads a point with only `count` and `sum` as one `+Inf` bucket holding `count`, the
  model shape of a point with one bucket and no bounds. Datadog's exporter treats the two apart:
  given a bucketless delta point with a `min` and a `max`, it adds a bound at each and spreads the
  count between them, and under `histograms.mode: counters` it writes no `.bucket` series for a
  bucketless point. `datadog_out` writes the one-bucket point's sketch, every observation at the
  Agent's key -1, and its `lower_bound:-inf,upper_bound:inf` `.bucket` series. With no `min` or
  `max`, the sketch matches the exporter's. The `no-buckets-count*` cases of
  `testdata/differential/datadog-sketch/` record the exporter's output, and each corpus reader
  lists them in its `COLLAPSED`.
  - **Consequence:** a bucketless point with `min` and `max` reaches Datadog with its count and
    sum exact but every observation in one bin at about `-9.94e-10` rather than between them,
    and counters mode adds one `.bucket` series.
  - **Revisit trigger:** a producer that sends bucketless histograms to Datadog. Telling the
    shapes apart needs a `count` field on `Histogram` (`docs/known-gaps/otlp.md`).
- **`datadog_out` drops a sketch that would encode as more than 2^20 `k`/`n` entries**
  (`MAX_DOGSKETCH_ENTRIES`, `crates/logit-proto/src/datadog/sketches.rs`), rather than splitting
  each bin's count into `uint16` entries without bound.
  - **Consequence:** a sketch with per-bin counts in the millions across many bins (a statsd
    sample-rate typo extrapolated through `aggregate`) never reaches Datadog. It's counted
    `logit.output.metrics.dropped{reason="oversized_sketch"}` with diag `oversized_sketch`.
    [ADR `deployment-threat-model`](../adr/deployment-threat-model.md) treats that as an accident to
    bound, not data to scale down ([ADR `untrusted-input-bounds`](../adr/untrusted-input-bounds.md)
    has the rule).
- **Some Datadog codec counters count once per request body, not once per batch.** They
  describe a body, not a record:
  - `spans.degraded{reason="no_wire_form"}` and `{reason="json_text"}` for a batch-resource
    carrier.
  - `tags.omitted{reason="no_wire_form"|"unrepresentable"}` for a stats payload's resource
    attributes.
  - `stats.degraded{reason="negative_timestamp"}`.

  A count-capped request is one body, and `datadog_trace_out` cuts a batch of more than 1,000
  traces or stats groups into several. `datadog_out` doesn't cut a traces or stats request by
  count, so there it's once per batch.
  - **Consequence:** a batch of 1,001 traces reports a carrier the form can't hold twice. The
    count is stable across retries, which is what the attempt accounting guarantees, but it isn't
    a per-batch measure.
  - **Revisit trigger:** a dashboard that needs the per-batch figure
    ([ADR `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md),
    decision 2).
- **A record a Datadog codec degraded and the sink then dropped as oversize is reported under
  both counters.** The codec counts the degradation (`no_wire_form`, `json_text`, and the like)
  at the record's first encode. `split_encode`'s bisection may then find the record alone over
  the route's byte limit and drop it, counted `records.dropped{reason="oversize"}`. The record is
  never sent, so its degradation counter describes a wire form that never left. Suppressing it
  would need the codec to defer its counts until the request is accepted.
  - **Consequence:** the degradation counters of a route that also drops oversize records read
    high by those records.
- **Some rows of the Datadog sinks' response-class tables rest on documentation, not a recorded
  response.** The status rows follow Datadog's documented intake statuses and the Agent's source.
  The `{"errors":["Forbidden"]}` body a `403` carries appears only in a test
  (`datadog::tests::a_403_forbidden_is_refused`); no recorded response from a real intake or
  Agent backs any `4xx` row. The tables are `crates/logit-outputs/src/datadog.rs` and
  `crates/logit-outputs/src/datadog_trace.rs`, "Faults, retries, and duplicate safety".
  - **Consequence:** none known. Both sinks classify by status alone, so a body shape that
    differs changes only the text the diagnostics quote.
  - **Revisit trigger:** a recorded `4xx` response from a real Datadog intake or Agent.
- **A sink sent to again without `observe_batch`, after a batch whose last attempt failed
  retryably, keeps that batch's state.** `observe_batch` arms the attempt gate of every sink that
  has one, and on `datadog_out` it also fixes the batch's send time. A final send disarms the gate
  and clears the time: `Ok`, or a fault `write_loop` won't retry under the sink's posture
  (`Rejected`, or `Ambiguous` under `at_most_once`). A retryable failure (`Clean`, `Refused`, or
  `Ambiguous` under `at_least_once`) leaves them, because the runtime may retry and the retry needs
  the gate, the settled-request memory, and the send time ([ADR
  `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md),
  decisions 2 and 3). A caller that then calls `send` with a new batch and no `observe_batch`
  finds the gate armed: the new batch's encode-side counts for units the failed batch encoded are
  muted, on `datadog_out` the stale windows are measured from the failed batch's send time, and
  both Datadog sinks and `otlp_out` skip the requests at the settled positions.
  - **Consequence:** none on a shipped path. `write_loop` calls `observe_batch` before every
    batch, which re-arms the gate and replaces the time. `logit_pipeline::send_batch`, which the
    benchmarks use, is the one caller that skips it. Because it never calls `observe_batch`, no
    gate is armed and no send time is stored, so its repeated sends to one sink leave no state.
  - **Fix, if a caller ever needs it:** a runtime signal that a batch ended (an `Output` method,
    which decision 2 declined to add). A sink can't clear on a retryable `Err` without breaking
    the reuse the retries need.
  - `datadog::tests::a_direct_send_after_a_retryable_failure_reuses_its_send_time_and_armed_gate`
    pins the behavior, so a change to it is noticed.
