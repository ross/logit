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
- **`datadog_trace_in` does none of the Agent's processing.** Spans relay as the tracer wrote them:
  no obfuscation, normalization, `_top_level` marking, sampling, or stats computation
  ([plan §14](../plans/datadog-relay.md#14-not-in-this-stack-an-agent-equivalent-trace-processor)).
  - **Consequence:** its output must reach Datadog through a real Agent (`datadog_trace_out`) or go
    to an OTLP backend. Fed straight to `datadog_out`, its spans are skipped as not yet processed.
  - **Revisit trigger:** the Agent-equivalent trace processor the plan defers.
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
- **A `datadog_out` or `datadog_trace_out` resend isn't idempotent: Datadog stores a resent log
  twice.** Both sinks remember which requests of a batch the destination settled, so a retry
  resends only the rest ([ADR `sink-fault-classes`](../adr/sink-fault-classes.md), "Amendment: the
  Datadog sinks retry per request (2026-10-09)"). A resend still happens in two places: the
  request that drew an `Ambiguous` answer is resent under `at_least_once` and may have been
  applied, and a `buffer.disk:` replay after a crash resends every request of the batch because the
  memory is in-process (`docs/known-gaps/sinks.md`, "The per-request retry memory of `otlp_out` and
  the Datadog sinks is in-process"; `docs/known-gaps/native-hop.md`, "A disk-backed sink replays
  delivered and dropped batches after a crash"). A trial org received two resends:
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
- **`datadog_out` drops metric points older than 1 hour, which Datadog would store.** The sink
  uses the documented series window, which is stricter than the intake;
  [the plan's "Timestamp windows" section](../plans/datadog-relay.md#11-timestamp-windows-w5) has
  what a trial org stored.
  - **Consequence:** a `buffer.disk:` replay after an outage longer than 1 hour drops metrics
    the intake might still have stored, counted `records.dropped{reason="stale"}`.
  - **Revisit trigger:** Datadog documents a longer window, or an operator needs the replay.
- **`datadog_out` copies Datadog's 100-tag limit on metrics.** A series, distribution point, or
  sketch whose `tags` list holds more than 100 strings is dropped before sending, counted
  `records.dropped{reason="too_many_tags"}`, because the intake drops it and only the series
  route says so (measured on a trial org on 2026-10-08;
  [the plan's "Timestamp windows" section](../plans/datadog-relay.md#11-timestamp-windows-w5)).
  - **Consequence:** if Datadog raises the limit, or an org has a higher one, the sink still drops
    what Datadog would store.
  - **Workaround:** keep metrics under 100 tags with `keep` or `remove` upstream.
  - **Revisit trigger:** Datadog documents or answers a different limit; the series route's `202`
    body states it (`limit=100`).
- **`datadog_out` sends a Datadog event's and a service check's host as a tag.** Their encoders
  read the host only from `statsd.event.host` and `statsd.service_check.host`, so a `host.name`
  that `set` stamps on the resource renders as a `host.name:<value>` tag, and Datadog shows the
  event or check with no host.
  - **Consequence:** events and checks sent directly, not through an Agent, have no host in
    Datadog unless the DogStatsD client set `h:`.
  - **Workaround:** `set` `statsd.event.host` and `statsd.service_check.host` as attributes.
- **`datadog_trace_out` under `version: v0.4` drops the trace chunk and tracer payload fields.**
  v0.4 has no field for a chunk's `datadog.chunk.*` fields (sampling priority, origin, dropped
  flag, tags) or for the tracer payload fields no request header carries
  (`datadog.tracer.runtime_id`, `.env`, `.hostname`, `.app_version`, `.tags`, `.container_debug`).
  Each is counted `logit.output.spans.degraded{reason="no_wire_form"}`.
  - **Consequence:** only a v0.7-origin batch loses anything, and the Agent derives a chunk's
    priority and origin from the root span again on its side.
  - **Workaround:** `version: v0.7`, which carries all of them.
- **`datadog_trace_out` derives no Datadog fields from an OTel span.** A span without
  `service.name`, `resource.name`, or `span.type` reaches the Agent with `service`, `resource`, or
  `type` empty, and the Agent computes no stats for it.
  - **Consequence:** OTel-origin spans through this sink show up in Datadog poorly named.
  - **Workaround:** send OTel spans with `otlp_out`, to the Agent's OTLP receiver or to Datadog.
  - **Revisit trigger:** the Agent-equivalent trace processor the plan defers
    ([plan §14](../plans/datadog-relay.md#14-not-in-this-stack-an-agent-equivalent-trace-processor)).
- **`datadog_out` sends no Agent-style `h`/`ms` aggregates, and no explicit-bucket
  `Histogram`.** An Agent turns a DogStatsD `h` or `ms` into `.avg`, `.count`, `.median`,
  `.95percentile`, and `.max` series. `aggregate` in front of `datadog_out` sends a sketch
  instead, which Datadog computes quantiles from. `datadog_out` skips a `Histogram`,
  `ExponentialHistogram`, `Summary`, `GaugeDelta`, `SetMembers`, and a cumulative or non-monotonic
  `Sum`, counted `logit.output.metrics.skipped{metric_kind}`.
  - **Consequence:** dashboards built on an Agent's `.95percentile`-style series find no data
    when the same metrics arrive through `aggregate` and `datadog_out`; query the distribution
    instead. An OTLP explicit-bucket histogram reaches Datadog only through `otlp_out`.
  - **Revisit trigger:** an operator who needs the Agent's five series, or a `Histogram` as
    Datadog's `.bucket` counters ([plan §2](../plans/datadog-relay.md#2-kinds-and-config-w3w6)).
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
    sends one to Datadog yet ([plan §4](../plans/datadog-relay.md#4-sketch-compatibility-w1-settled)).
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
    or repeat the plan's trial-org run (W7b) for the item in question.
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
- **`datadog_out` drops a sketch that would encode as more than 2^20 `k`/`n` entries**
  (`MAX_DOGSKETCH_ENTRIES`, `crates/logit-proto/src/datadog/sketches.rs`), rather than splitting
  each bin's count into `uint16` entries without bound.
  - **Consequence:** a sketch with per-bin counts in the millions across many bins (a statsd
    sample-rate typo extrapolated through `aggregate`) never reaches Datadog. It's counted
    `logit.output.metrics.skipped{reason="oversized_sketch"}` with diag `oversized_sketch`.
    [ADR `deployment-threat-model`](../adr/deployment-threat-model.md) treats that as an accident to
    bound, not data to scale down ([ADR `untrusted-input-bounds`](../adr/untrusted-input-bounds.md)
    has the rule).
- **Some Datadog codec counters count once per request body, not once per batch.** They
  describe a body, not a record:
  - `spans.degraded{reason="no_wire_form"}` and `{reason="json_text"}` for a batch-resource
    carrier.
  - `tags.dropped{reason="no_wire_form"|"unrepresentable"}` for a stats payload's resource
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
    `statsd_out` doesn't forward `observe_posture` (its default posture is `at_most_once`), so its
    accounting stays armed after an `Ambiguous` failure too; it keeps no per-request memory, so
    the only effect is the muted counts.
  - **Fix, if a caller ever needs it:** a runtime signal that a batch ended (an `Output` method,
    which decision 2 declined to add). A sink can't clear on a retryable `Err` without breaking
    the reuse the retries need.
  - `datadog::tests::a_direct_send_after_a_retryable_failure_reuses_its_send_time_and_armed_gate`
    pins the behavior, so a change to it is noticed.
