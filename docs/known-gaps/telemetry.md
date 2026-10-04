# Known gaps: Internal telemetry and perf tooling

Entry format and the other areas: [the known-gaps index](README.md).

## Internal telemetry and self-logging

- **Internal spans have six open residuals.** Emission, sampling, and OTLP export are built
  ([internal-telemetry.md](../design/internal-telemetry.md)'s "Spans",
  [ADR `trace-context-propagation-on-delivered`](../adr/trace-context-propagation-on-delivered.md),
  [ADR `internal-span-emission-and-deterministic-sampling`](../adr/internal-span-emission-and-deterministic-sampling.md)).
  These are still open:
  1. **The listener span's window is the `send` call only, not decode-to-send.** `Fanout::send`
     can't see how long a listener spent building the batch, because `Input::run` is a free-form
     loop.
  2. **An accumulating listener puts independently arrived input under one root.** `sink.send`
     mints one `TraceContext::new_root` per accumulated batch, not per datagram or frame, so a
     `batch_max_events` above 1 groups unrelated arrivals under one trace (the `emit` functions in
     `crates/logit-inputs/src/udp.rs` and `crates/logit-inputs/src/tcp.rs`). It's the same
     many-to-one shape as a stateful transform's `flush()`, which at least carries bounded
     contributing-context links.
  3. **Lua `flush()` gets a link-less root.** It gets a real span but no links; see
     [the Lua entry](transforms.md#lua) on `flush()` attribution.
  4. **Every `SinkQueue` slot carries a 24-byte `TraceContext`** (inside its 32-byte
     `BatchContext`), so `write_loop` can parent its sink span on the context the batch arrived
     under. It's the same size-for-a-span trade `Delivered` made and measured
     ([memory.md](../design/memory.md)'s "Costing internal spans").
  5. **`service.name` is the only resource identity `internal` sets**
     ([internal-telemetry.md](../design/internal-telemetry.md)'s "Resource identity"). Its
     `Resource` has no `host.name`/`service.instance.id`, the semconv way to tell `logit`
     instances apart in Tempo, because nothing in the running service reads the OS hostname.
     `SyslogEncoder::default_hostname` in `crates/logit-outputs/src/syslog.rs` is
     config-supplied, and only the `logit-perf` harness calls `gethostname`. Deferred until the
     service has that dependency for another reason.
  6. **The demo's Tempo service graph panel is empty.** It needs three things, and
     `demo/compose.yaml` and `demo/tempo/tempo.yaml` have none of them: Tempo's
     `metrics_generator` (`service-graphs`/`span-metrics` processors) with `remote_write` to a
     Prometheus-compatible store, that store as a Grafana datasource, and
     `serviceMap.datasourceUid` on the Tempo datasource. A real multi-service trace exists to draw:
     [demo-tracing-stack.md](../plans/demo-tracing-stack.md)'s HAProxy → nginx → app chain, plus
     [ADR `trace-context-span-lifting`](../adr/trace-context-span-lifting.md)'s `span:` block.
     It's extra stack for one panel, not a `logit`-side gap. Whether `logit` should compute a
     service graph itself as a component is unexplored.

- **`host_metrics` isn't built.** Facts about the machine (CPU, disks, NICs) are a different kind
  of source than `internal`: read from the OS rather than `logit`'s own counters, with their own
  config and failure modes an in-process atomic read never has. When it lands, it's a separate
  component kind, not a field on `internal`
  ([ADR `internal-telemetry-as-pipeline-events`](../adr/internal-telemetry-as-pipeline-events.md)).

- **A component's internal-telemetry buffer caps distinct `(name, tags)` keys at 1024**
  (`MAX_KEYS_PER_COMPONENT`, `crates/logit-core/src/telemetry.rs`), and the cap isn't
  configurable. It bounds a component that ignores the tag-cardinality convention
  (`&'static str` values only) instead of letting it grow the process-wide interner. A dropped
  key is counted (`logit.internal.points.dropped{reason="cardinality"}`), never silent.
  **Revisit** if a legitimate component ever needs more than 1024 distinct points between drains.
- **Lua-authored telemetry (`crates/logit-script/src/telemetry.rs`,
  [ADR `lua-authored-telemetry-cardinality`](../adr/lua-authored-telemetry-cardinality.md)) trades the
  type-system cardinality guarantee the rest of `internal-telemetry.md` relies on for a
  convention-enforced one.** A script's metric name or tag value goes through the process interner
  instead of being a Rust `&'static str`, so a script that builds one from per-event data leaks
  the interner one entry at a time.
  - **Accepted** for the same reason as "The attribute/metric-name interner never frees" (under
    [Event model and interner](runtime.md#event-model-and-interner)): it's bounded in the
    intended, documented use, a fixed literal in the script's source.
  - **If that stops holding,** the fix is a bounded per-`ScriptWorker` cache instead of the
    process-wide interner, which the ADR records as a considered-and-deferred alternative.
- **Two self-telemetry candidates aren't built**, and each needs more than a
  `telemetry.count(...)` call:
  - **`json`'s parse-outcome counts.** Its two failure modes (`no_brace`, `parse_failure`) already
    reach `logit.component.diagnostics{key=...}` through the `Diagnostics` bridge, so a dedicated
    metric would mostly restate them.
  - **Lua per-call latency, error classification, flush-tick-empty tracking.** A per-event
    `ScriptWorker::process` timing distribution would isolate one pathological event in a big
    batch (`logit.component.process.duration` is whole-batch), at the cost of a clock read per
    event. Every `process()` error (`ScriptError::MissingProcess`, `ScriptError::Lua`) collapses
    into one `logit.component.errors{reason="process"}`, and every `flush()` error into
    `reason="flush"`, so you can't tell a script bug from a runtime error. Each is real; none was
    a default yes.
- **Internal-log sampling is the existing per-key occurrence throttle, nothing finer.**
  `Diagnostics::warn_throttled`'s powers-of-two throttle bounds a chatty diagnostic before it
  reaches `tracing`; `TelemetryLayer` adds no sampling or rate limit after that. Only
  `MAX_LOGS_PER_COMPONENT`'s bound-and-drop bounds a component that logs at `warn`/`error` outside
  the throttle (a lifecycle event, an unthrottled `Diagnostics::error`). It isn't built because
  nothing shipped needs it, and the throttle already covers the hot path (a malformed line, a
  parse failure).
- **Shutdown-time counts never reach an exported pipeline.** `internal`'s `run_until_shutdown`
  does its final drain the moment the shutdown signal fires, and every count recorded after that
  stays in the component buffers:
  - the UDP listeners' `datagrams.dropped`/`bytes.dropped{reason="shutdown"}`
    ([ADR `shutdown-accounting-and-cancellation-safety`](../adr/shutdown-accounting-and-cancellation-safety.md),
    decision 4);
  - the drops a sink or Lua node counts during the drain;
  - the final drain's own `logit.internal.*.emitted`/`drain.duration`.

  They reach a test `Registry` but no `otlp_out` or `prometheus_out`. An operator sees them in the
  self-log instead: each UDP guard that counts a nonzero remainder logs a `warn` naming the
  listener and the count, and `drain complete` logs the sinks' total. Exporting them would need
  `internal` to drain once more after every other node has stopped, into a pipeline that has
  itself already stopped.
- **A self-telemetry batch `internal` has drained is lost uncounted if the grace backstop drops it
  mid-send.** Each `tick` drains the registry, then awaits `Fanout::send`. A tick parked on a full
  downstream when shutdown fires keeps the `select!` from seeing the signal until the send
  completes. If the downstream stays full for the 5 s grace, `run_input` drops the task with the
  drained points, spans, and logs in the send. The registry no longer holds them, and nothing
  counts them. This is the general dropped-send loss under
  [Pipeline runtime and graph](runtime.md#pipeline-runtime-and-graph), for the one listener
  whose data is `logit`'s own.
- **A drop a kernel or a destination decided counts again on a retried batch.** Encode-side
  counters count once per batch, but these repeat on every attempt that gets the same answer:
  Splunk's code 6 (`records.dropped{reason="invalid_event"}`) and Splunk Cloud's oversize answer,
  an OTLP `partial_success` (`records.rejected`), a datagram refused with `EMSGSIZE`, and the
  packer's skip of an entry over the datagram cap (`oversize_datagram`).
  [internal-telemetry.md](../design/internal-telemetry.md)'s class table lists them.
  - **Consequence:** on a sink that retries, these counters read high by the number of attempts
    that met the verdict, and the batch's own retries account for the growth. `otlp_out`'s and
    the Datadog sinks' `records.dropped{reason="rejected"}` count a rejected request again on
    every retry of a batch held on a later request, and a hold has no end but success or
    shutdown, so that re-count is unbounded for as long as the hold lasts.
  - **Why it stays:** each attempt got its own answer, and a retry might get a different one, so
    counting once would need the sink to remember what an earlier attempt learned
    ([ADR `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md),
    decision 1).
- **The HTTP sinks' `logit.output.requests` doesn't use the four fault classes.** The stream and
  datagram sinks (`statsd_out`, `syslog_out`, `graphite_out`, `collectd_out`, `logit_out`) tag
  each attempt `class=ok|clean|ambiguous|rejected|refused`. `influxdb_out`, `otlp_out`,
  `prometheus_out`'s remote-write mode, `datadog_out`, `datadog_trace_out`, and `splunk_hec_out`
  tag each request with its status class (`2xx`, `5xx`, `network_error`, and the gRPC status name
  for `otlp_out`) and count one per request, so a `send` that issues several requests counts
  several.
  - **Consequence:** one alert on `class="ambiguous"` covers the first group and not the second,
    and a dashboard needs a query per group.
  - **Revisit trigger:** aligning the vocabulary, which the ADR names as follow-up work
    ([ADR `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md),
    decision 4).

## Load-test harness and perf tooling

- **`script/perf compare` has no cross-run noise model.** It diffs two results files' medians
  directly against `--threshold`, ignoring the run-to-run variance each file's own `repeats:`
  already show.
  - **Consequence:** a scenario whose repeats spread more than `--threshold` can trip a
    "regression" on scheduling luck, and a real regression smaller than its noise floor can pass
    silently. `compare` warns on host, CPU model, THP, `rmem_max`, profile, rustc, count, and
    pacing mismatches, but not on "this scenario's own repeats disagree by more than the
    threshold you're gating on".
  - The motivating case (`aggregate` spreading about ±25% between laptop repeats) doesn't
    reproduce on the perf VM, where two independent 5-repeat samples agree within 0.7%
    ([performance.md](../design/performance.md) §1, "Noise").
  - **Revisit:** candidate fixes (gate on each file's `min`, or a per-scenario threshold) wait
    until a scenario needs them.
- **`DiskQueue::open` reads the spool twice at startup, and nobody has measured what that costs
  `buffered`.** It reads and CRC-walks the whole active segment to find a torn tail (bounded by
  `segment_bytes`, 64 MiB by default), then reads every segment from the read cursor onward again
  to count what's left to replay (`crates/logit-pipeline/src/disk_queue.rs`). That's real work
  whenever the cursor lags the end of the spool, and a first-open cost even on a cleared one. The
  harness clears every `buffer.disk.path` before each spawn (`crates/logit-perf/src/spool.rs`),
  and the perf VM measures `buffered` within about 7% events/s and under 1% CPU µs/event across
  five repeats ([performance.md](../design/performance.md) §3). Whether the scan explains any
  remaining spread is an open, low-priority question.
- **`generate_in`'s `rate:` pacing is millisecond-granular above roughly 1k batches/s.** Before
  each batch it sleeps until `start + sent / rate`, and one OS sleep can't go below about 1 ms. So
  a `rate` above roughly 1,000 batches/s (above ~100k events/s at the default `batch: 100`) is
  accurate on average but bursty within each millisecond
  ([ADR `load-test-harness`](../adr/load-test-harness.md)). Nothing shipped is affected: no
  `perf/scenarios/*.yaml` sets `rate:`, because every scenario measures unthrottled,
  backpressure-only throughput.
- **`RunReport.box_state` doesn't record IMDS facts or most kernel tunables.** `logit-perf run`
  reads:
  - the governor, EPP, platform profile, and AC power, best-effort (an Azure guest exposes none of
    them, so they stay absent there);
  - THP `enabled`/`defrag`, `net.core.rmem_max`/`rmem_default`, the online CPU count, and SMT,
    read inside the dev container and so the host kernel's.

  `compare` warns when two files differ in THP `enabled` or `rmem_max`. It doesn't record IMDS
  facts such as `vCPUsPerCore` and the VM size, or any other kernel tunable (`rmem` is the one a
  measured finding turned on).
- **`script/perf attribute` can't decode a dump from a binary older than the dump's current
  format.** `attribute` reads the `internal` leg's native dump with the checkout's decoder, so a
  `--logit-bin` from before a format change fails with `decoding frame 0 ... bad distribution blob:
  Version`. **Workaround:** to compare a per-node breakdown across a format change, build a harness
  from each binary's own commit, or fall back to flamegraph shares.
- **`native-relay` fell 7.0% between `844be079` and `efd50c1e` with no production change on its
  path.** The only difference is `FrameReadError::Malformed { reason, err }`, which runs on the
  error path. The saving is all user time (0.780 → 0.695 µs/event at `window: 32`, 0.890 → 0.798
  at `window: 1`) and system time is flat, which points at codegen. It's unattributed, and it
  favors the code. Confirming it needs a pinned flamegraph pair of the two binaries
  ([performance.md](../design/performance.md) §1, "What moved since 2026-09-28").
- **When and how the load-test harness runs in the development process is undecided.** Nightly,
  manually triggered, a PR gate on a `compare --threshold` regression, or another cadence is open
  ([ADR `load-test-harness`](../adr/load-test-harness.md)'s "Open question" section). The harness
  is built and runnable by hand; nothing wires it into CI, a pre-merge gate, or a schedule.
