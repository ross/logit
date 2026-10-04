---
created: 2026-10-04
updated: 2026-10-04
---

# Enabling plan: sink fault model — a bad batch drops, a non-functional destination holds, the process never exits

## Goal

Replace the sink-failure behavior that grew out of [ADR
`buffered-sink-delivery`](../adr/buffered-sink-delivery.md) with the one the field has converged
on, informed by a survey of eleven peers (below). Stream key `fault`. `fault/w0` is this plan and
the ADR, and changes no code.

Today a sink classifies a failed send as `Clean`, `Ambiguous`, or `Permanent`. `Permanent`
conflates two things that need opposite handling: a destination that rejects *this batch* (a
`413`, a bad body `400`, `logit_out`'s size cap) and a destination that rejects *every* batch (a
bad token, an unknown bucket or tenant, a protocol mismatch). Sixty seconds of only-`Permanent`
outcomes ends `logit run` with exit code `2`, taking every other sink's data with it.

After this stream:

- **The process never exits for a sink failure.** The only process-ending faults are a listener's
  loop dying, a Lua thread panicking or exceeding `max_memory`, and a script wedged across
  shutdown (`docs/deploying.md`, "Probes and exit codes").
- **A batch the destination rejects for its own content is dropped at once**, counted under its
  own reason, and announced with the destination's text in a diagnostic that `internal` carries as
  a log event. Nothing behind it waits.
- **A destination that isn't functional holds the queue.** Unreachable, timing out, answering
  `5xx`/`429`, refusing credentials, or rejecting every request for a reason that isn't the
  batch: the sink retries the head immediately, then with exponential backoff to a cap, for as
  long as it takes. The sink's `buffer:` (`max_batches`/`max_bytes`, `overflow`, `disk:`) is the
  bound on what accumulates, as it is for every peer that retries indefinitely. There is no retry
  budget and no sustained-history state.
- **Posture still governs an unknown outcome.** An `Ambiguous` fault under `at_most_once` is
  dropped, as [ADR `delivery-semantics`](../adr/delivery-semantics.md) item 5 decides; a `Clean`
  or `Refused` fault is retried under either posture, because nothing was applied.
- **The native hop can say why.** `logit_in` answers a frame it can't take with a status, so a
  `logit_out` drops a rejected batch instead of retrying it, in order, with the cumulative mark
  still advancing.
- **`otlp_out` fails per signal**, so a traces-only backend that answers `UNIMPLEMENTED` for
  metrics doesn't fail the batch's traces with it (`docs/known-gaps/otlp.md`).

PR #522 (`feat/sink-backoff`) built an intermediate design: hold the head after a 60 s streak and
probe it once a minute. Review found that a held batch rejected for its own content wedges the
sink forever, and the survey below found no peer that holds a rejected batch. #522 is closed
unmerged; its exit removal, its tests, the `queued()` accessors, the error text on
`Delivery::Dropped`, and its operator prose are cherry-pick material for W1.

## Non-goals

- A dead-letter path for dropped batches (Fluent Bit's `storage.keep.rejected`, Logstash's DLQ,
  Fluentd's `<secondary>`). A real follow-up with its own record; this stream only makes the drop
  visible.
- Splitting an oversize batch and resending the halves (libbeat `SplitRetry`, Telegraf
  `splitAndWrite`). Same: a follow-up, once a sink can say "too large" distinctly.
- Partial success on the native hop. A frame decodes whole or not at all.
- Giving up in-order delivery to let a failing request sit in a concurrency slot while others
  proceed, as Vector and the OTel Collector do. The native hop's sequence numbers and remote
  write's per-series order rule it out, and with a `Rejected` class nothing poisonous is held.
- Readiness changes. A sink holding its queue still shows nowhere on `/readyz`
  (`docs/known-gaps/runtime.md`, "Readiness is per-process, not per-sink").

## Survey: what eleven peers do with a destination that rejects

Collected 2026-10-04 from primary sources (docs, source, specs, issues). Items a page summarizer
produced that weren't checked against the raw source are marked UNVERIFIED in the notes under
`perf/results/sink-backoff/research.md` on the branch that collected them; this table carries
only the claims that bear on the decisions.

| System | Final rejection (`4xx` not `429`) | Retryable (`5xx`/`429`/connect) | Holds a head? | Breaker or exit | Poison tools |
|---|---|---|---|---|---|
| Vector | `RetryAction::DontRetry`: drop, `component_discarded_events_total{intentional=false}` | Fibonacci backoff, `retry_attempts` default `isize::MAX`, 1..30 s between | No: retry occupies a per-request slot, ARC up to 200 concurrent | None; ARC ignores `4xx`; never exits | `batch.max_bytes` up front; ES `retry_partial`; no 413 split; no DLQ (issues #10870, #20266 open) |
| OTel Collector `exporterhelper` | `consumererror.NewPermanent`: retry sender returns at once, "Exporting failed. Dropping data." | exponential 5 s to 30 s, `max_elapsed_time` 300 s (`0` forever) | No: `num_consumers` 10; strict order only with 1 | None; never exits | `sending_queue.batch.max_size`; partial_success log-only; no DLQ |
| Fluent Bit | `out_http`: `4xx` except 408/429 is `FLB_ERROR`, dropped; `out_es` retries any non-200/201 | per-chunk, `Retry_Limit` default 1, `False` forever, 5 s to 2000 s | No: each chunk its own task | None | opt-in DLQ `storage.keep.rejected`; ES per-item; `storage.total_limit_size` drops oldest |
| Fluentd | `out_http`: any non-2xx not in `retryable_response_codes` (default `[503]`) is `UnrecoverableError`: `<secondary>` or backup file | per-output state, `retry_timeout` 72 h, `retry_forever` | One retry state per output | None; retry exhaustion drops the whole queue | `<secondary>`; `overflow_action`; oversize record skipped at enqueue |
| Logstash ES output | 400/404 to DLQ or dropped; 409 dropped; everything else, 413 included, retried forever | forever, worker blocks, PQ back-pressures inputs | Yes | None | pre-split over 20 MB; per-item; `dlq_custom_codes` |
| libbeat (Filebeat) | ES per-item: `4xx` dropped or `dead_letter_index`; 409 dropped | forever ("ignores `max_retries`"), `backoff.init/max` 1 s/60 s | No, but the queue fills | None | 413: `SplitRetry` halves to one event, then drop; per-item |
| Prometheus remote write | spec: MUST NOT retry `4xx` other than 429: dropped, `prometheus_remote_storage_samples_failed_total` | forever, `min_backoff`/`max_backoff`, shard blocks, WAL backs up, `sample_age_limit` the only bound | Yes per shard (in order per series) | None | RW 2.0 `X-Prometheus-Remote-Write-*-Written` headers |
| Grafana Alloy `loki.write` / Promtail | "Only retry 429s, 500s and connection-level errors"; 400, 401, 413 dropped, `dropped_entries_total` | `max_backoff_retries` 10 (`0` forever), 500 ms to 5 m | One batch at a time per client | None; opt-in WAL `max_segment_age` | none beyond drop |
| Telegraf | `influxdb_v2`: 400 is `Retryable: false`, rejected; v1: any `4xx` dropped except database not found | every flush, forever; `metric_buffer_limit` drops oldest; `retryTime` gate after 429/`5xx` | Yes, bounded by drop-oldest | None | 413: `splitAndWrite`; `PartialWriteError` accept/reject sets |
| Datadog Agent forwarder | 400 and 413 dropped; 403 refreshes the secret once, then drops; 404 and other `4xx` retried | exponential with jitter, 2 s to 64 s (logs 120 s, indefinitely); retry queue spills to disk | Transactions, not a head | None documented | none beyond classification |
| Kafka Connect sinks | `errors.tolerance=none` (default): task FAILED, stays stopped; `all`: DLQ topic, converter/transform stage only (KIP-298) | `RetriableException` from `put()`: same call again, unbounded | Yes | Stops the one task, not the worker | KIP-610 `ErrantRecordReporter` per record |

What they agree on:

1. **Nobody holds a positively rejected batch and retries it.** A final rejection is dropped and
   counted, diverted, or stops the one task. The two that retry a rejection forever (Logstash on
   413, Fluent Bit `out_es` under `Retry_Limit False`) are the subjects of user complaints, and
   each grew an opt-out.
2. **Nobody has a sustained-history state or a circuit breaker, and nobody exits the process.**
3. **Where the destination/batch ambiguity is resolved, it's by the response, not by history.**
   Datadog treats 403 as credentials and 400/413 as the batch. Telegraf and the remote-write and
   Loki specs define a `4xx` other than 429 as "about this request". Vector and the OTel Collector
   don't resolve it, and Vector #10870 ("dropped on 403/404 with invalid credentials") and #20266
   ("backpressure instead of drop on 403") are operators asking for the hold this plan gives a
   refused destination.
4. **The poison tools in use** are a size split, per-item isolation where the wire reports it, and
   a dead-letter path. None is a probe strategy.
5. **Ordering is what makes a held head fatal.** The two peers without head-of-line blocking gave
   up order to get it.

## Decisions settled before W0

Agreed with Ross on 2026-10-04; the ADR records the reasoning.

1. **Four fault classes.** `Clean` (nothing left the process), `Ambiguous` (may have been
   applied), `Rejected` (the destination refused *this batch*: drop it), `Refused` (the
   destination refuses *every* batch for now: hold and retry). `Permanent` goes away.
2. **A `400` isn't automatically `Rejected`.** A response that means every batch will fail (a
   protocol mismatch, an unknown tenant, an unknown bucket) is `Refused`, the same as a connection
   failure or bad credentials. Each sink classifies from everything its destination gives it
   (status, error code in the body, text), and W2's survey records the mapping per sink with its
   evidence. A response a sink can't attribute is settled below ("Settled in W0", item 1).
3. **No retry budget, no streak, no probe state.** A retryable fault (`Clean`, `Refused`, and
   `Ambiguous` under `at_least_once`) retries the head immediately, then with exponential backoff
   from `base_delay` to `retry_max_delay`, indefinitely. The queue's `buffer:` bounds the cost.
   `retry_budget`, `backoff_after`, and `backoff_interval` don't exist.
4. **A drop is announced.** `batches.dropped{reason="rejected"}` and a throttled diagnostic
   carrying the destination's text; `internal` turns the diagnostic into a log event.
5. **A hold is announced.** A paced error line while a destination stays non-functional, and a
   gauge (`logit.component.retrying`, or the existing `degraded` vocabulary) for alerting, since
   `batches.dropped` stops moving while a queue holds.
6. **Order is kept.** No concurrency slots, no probing with a later batch.
7. **The native hop's acknowledgment carries a status.** A rejection is an `Ack` whose status says
   `rejected(reason)`; the receiver has handled that sequence by dropping it, so the cumulative mark
   advances and the sender commits it as dropped. Transient trouble needs no message: not
   acknowledging is the backpressure, as today.

## Workstreams

### W0: ADR and this plan (`fault/w0`)

[ADR `sink-fault-classes`](../adr/sink-fault-classes.md), from `docs/adr/TEMPLATE.md`: the four
classes, the no-exit rule, indefinite retry bounded by the buffer, the per-sink attribution rule
and the HTTP driver's status-only default, the announcements, order kept. It supersedes the
`Permanent` rule, the retry budget, and the rejected "never exit" alternative in
`buffered-sink-delivery`, the outage paragraph in `service-lifecycle-and-output-retry`, and the
pipeline ending in decision 11 of `sink-send-path-and-attempt-accounting`; each carries an
`updated:` and a dated pointer. It amends `delivery-semantics` item 2 (the "`retry_budget` ran
out" loss goes away) and item 5's table. The questions this plan opened are settled below. No
code.

### W1: runtime (`fault/w1`)

- `Fault` gains `Rejected` and `Refused`; `Permanent` is removed; `classify`'s default for an
  error with no `Fault` attached becomes `Rejected` (drop, never retry: the conservative default
  for an error the sink didn't recognize). `is_explicitly_permanent` goes away.
- `is_retryable`: `Clean` and `Refused` under both postures, `Ambiguous` under `at_least_once`,
  `Rejected` under neither.
- A mechanical first mapping, so the crate compiles with `Permanent` gone: the shared HTTP driver
  returns the ADR's status-only default (`401`, `403`, `404`, `405`, `407`, `501` are `Refused`;
  `429` and `5xx` stay `Ambiguous`; every other `4xx` and `3xx` is `Rejected`), `RefusesSink` is
  replaced by `Refused`, `logit_out`'s handshake rejects become `Refused`, and every other
  `Fault::Permanent` becomes `Rejected`. The `class` label's `permanent` value is renamed with the
  variant. Body-aware refinement is W2's, not W1's.
- `write_loop`: remove `PERMANENT_FAILURE_WINDOW` and the `Err` return (cherry-pick #522's
  commit for this and its tests); `deliver_with_retry`/`deliver_window` lose the budget deadline
  and retry until success or shutdown, with the backoff schedule unchanged; the shutdown grace
  still cuts a retry (`Delivery::GraceExpired`, `Fault::Ambiguous` for an in-flight send).
- `BufferConfig`: remove `retry_budget`; keep `retry_max_delay`; graph rule 15's zero-duration
  clause narrows to it. Regenerate the schema.
- Telemetry and diagnostics per decisions 4 and 5: `batches.dropped{reason="rejected"}`, the
  throttled diagnostic carrying the destination's text, the paced error line while a head holds,
  and the `logit.component.retrying` gauge; `docs/design/internal-telemetry.md` rows.
- Tests: the retry-budget tests become retry-until-success and retry-cut-by-grace tests; a
  `Rejected` batch drops at once with the reason and text; a `Refused` head holds while the queue
  fills under `block` and evicts under `drop_oldest`; the gauge rises on the first failure and
  falls on delivery or drop; shutdown accounting for a held head (memory counts `shutdown`, disk
  spools); a sibling sink keeps delivering; the run never ends for a sink.
- Operator docs: `docs/deploying.md`'s "Sink failure semantics" and the exit-code table (reuse
  #522's prose), `docs/known-gaps/otlp.md`'s Tempo entry, `demo/logit.yaml`'s `trace_only`
  comment, `docs/design/pipeline-graph.md`'s cancellation-points rows.

### W2: per-sink classification (`fault/w2`)

The evidence-backed refinement of W1's mechanical mapping. For each of `influxdb_out`, `otlp_out`
(HTTP and gRPC), `prometheus_out` (remote-write), `datadog_out`, `datadog_trace_out`,
`splunk_hec_out`, `logit_out`, and the shared HTTP driver (`crates/logit-outputs/src/http.rs`): a
table in the module doc mapping every response the sink distinguishes to a class, with the
destination's documentation as evidence (InfluxDB's JSON `code`, Datadog's per-product retry
guide, Splunk's HEC codes, the OTLP spec's retryable set, the remote-write spec, Loki's status
page), and a test per row. This is where a `400` whose body names an unknown bucket or tenant
becomes `Refused`, and where a sink whose destination answers `404` per request overrides the
driver's default. `docs/deploying.md` points at the tables; there is no second copy. The
stream-and-datagram sinks (`syslog_out`, `statsd_out`, `graphite_out`, `collectd_out`, `file_out`,
`stdio_out`) have no application response and classify by I/O error only: a connect failure is
`Clean`, a mid-write failure `Ambiguous`, a local encode-side refusal `Rejected`. Tempo's
`UNIMPLEMENTED` for metrics is `Rejected` for that signal's request, which W4 makes per signal.

### W3: native hop acknowledgment status (`fault/w3`)

Its own wire ADR amending `native-hop-named-acks`: `Ack` gains a status
(`accepted` | `rejected(reason)`), `logit_in` sends `rejected` for a frame it decodes but can't
forward (an oversize payload past its cap, a payload it won't take) and keeps the cumulative mark
moving, `logit_out` commits a rejected sequence as `dropped{reason="rejected"}`. Pre-release: no
compatibility shim ([ADR `native-hop-no-compatibility`](../adr/native-hop-no-compatibility.md)).
Decide whether a `Hello` refusal (version, codec) is `Refused` at the sender, which it is in
substance.

### W4: `otlp_out` per-signal outcomes (`fault/w4`)

`OtlpOutput::attempt` sends every non-empty signal and reports per request: a `Rejected` signal
is dropped and counted for that signal alone, the others are delivered. Closes
`docs/known-gaps/otlp.md`'s mixed-signal entry; `demo/logit.yaml`'s `trace_only` gate becomes a
noise filter rather than a correctness requirement, and its comment says so.

### W5: operator docs sweep (`fault/w5`)

`docs/deploying.md`, `docs/datadog.md`, `docs/splunk.md`, each sink's `docs/` mention of retry or
exit, `fixtures/statsd-to-influxdb.yaml`'s `buffer:` example, `AGENTS.md`'s runtime section and
its invariants list (a reviewer checks a sink's class mapping first on any change to a sink's
response handling).

## Settled in W0

Agreed with Ross on 2026-10-04 and recorded in [ADR `sink-fault-classes`](../adr/sink-fault-classes.md).

1. **A response the sink can't attribute** (a bare `400` with no body it can read) is `Rejected`:
   what every peer does and what the remote-write and Loki specifications define. But a status
   that describes the destination rather than the request holds even with no body: the shared
   HTTP driver's status-only default reads `401`, `403`, `404`, `405`, `407`, and `501` as
   `Refused`, `429` and `5xx` as `Ambiguous`, and every other `4xx` and `3xx` as `Rejected`. A
   sink's body-aware table (W2) overrides the default.
2. **No bounded guard against misattribution.** A cap on consecutive refusals turns every real
   outage into one lost batch per cap; the hold is visible through the gauge and the paced error
   line, and a dead-letter path is the remedy for what a table can't resolve.
3. **`Ambiguous` under `at_least_once` retries forever**, `logit_out` included, with `at_most_once`
   the opt-out.
4. **The per-sink table lives in each sink's module doc**, pointed at from `docs/deploying.md`.
5. **The gauge is `logit.component.retrying`**: `1` while the head has failed at least once and a
   retry is pending or in flight. `degraded` is a per-record codec counter and isn't reused.
6. **W1 maps mechanically, W2 refines with evidence.** Twelve files construct `Fault::Permanent`,
   so W1 can't remove it without a first mapping; it uses the driver default above and makes every
   other `Permanent` a `Rejected`, and W2 writes the tables.
