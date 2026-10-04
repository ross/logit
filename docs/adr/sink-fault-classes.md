---
created: 2026-10-04
updated: 2026-10-04
---

# Sink fault classes: a rejected batch drops, a refused destination holds, and the process never exits for a sink

## Status
Accepted

## Context

A sink classifies a failed send as `Clean`, `Ambiguous`, or `Permanent` ([ADR
`buffered-sink-delivery`](buffered-sink-delivery.md), "Delivery posture is a per-sink policy").
`Permanent` covers two situations that need opposite handling:

- The destination rejects *this batch*: a `413`, a `400` for a malformed body, `logit_out`'s
  pre-connect size cap. Retrying it can't succeed, and holding it blocks every batch behind it.
- The destination rejects *every* batch for now: a bad token, an unknown bucket or tenant, a
  protocol mismatch. Dropping the batch loses data that would have landed once the operator fixed
  the config or the destination recovered.

The runtime drops both after a bounded retry, and ends `logit run` with exit code `2` when
`Permanent` has been the only outcome for 60 s (`PERMANENT_FAILURE_WINDOW`). That exit takes every
other sink's queued data with it, and [ADR `delivery-semantics`](delivery-semantics.md)'s
"Amendment: per-request verdicts (2026-10-04)" records a well-formed mixed-signal batch tripping it
against a traces-only backend.

PR #522 built an intermediate design: hold the head after a 60 s streak of `Permanent` and probe it
once a minute. Review found that a held batch rejected for its own content wedges the sink forever,
because the probe resends the same batch. The PR is closed unmerged.

[docs/plans/sink-fault-model.md](../plans/sink-fault-model.md), "Survey: what eleven peers do with
a destination that rejects", compares Vector, the OpenTelemetry Collector, Fluent Bit, Fluentd,
Logstash, libbeat, Prometheus remote write, Grafana Alloy, Telegraf, the Datadog Agent forwarder,
and Kafka Connect. Five points hold across all eleven:

1. Nobody holds a positively rejected batch and retries it. A final rejection is dropped and
   counted, diverted to a dead-letter path, or stops the one task. The two that retry a rejection
   forever are the subjects of user complaints, and each grew an opt-out.
2. Nobody keeps a sustained-history state or a circuit breaker, and nobody exits the process.
3. Where the batch-or-destination ambiguity is resolved, the response resolves it, not history.
   The Datadog Agent treats `403` as credentials and `400`/`413` as the batch; Telegraf and the
   remote-write and Loki specifications define a `4xx` other than `429` as "about this request".
   The two that don't resolve it, Vector and the Collector, have open issues from operators asking
   for a hold on `403` instead of a drop.
4. The poison tools in use are a size split, per-item isolation where the wire reports it, and a
   dead-letter path. None is a probe strategy.
5. Ordering is what makes a held head fatal. The two peers without head-of-line blocking gave up
   order to get it.

## Decision

A sink's failed send falls in one of four classes, the process never exits for a sink, and a
retryable fault is retried until it succeeds or shutdown cuts it, bounded by the sink's `buffer:`.

### Four classes

`Permanent` is removed. `Fault` has four variants:

| Fault | Meaning | Retried under `at_most_once` | Retried under `at_least_once` |
|---|---|---|---|
| `Clean` | the destination never saw the batch (connect refused, DNS failure) | yes | yes |
| `Ambiguous` | the batch may have been applied before the response was lost (timeout, `5xx`, `429`) | no | yes |
| `Rejected` | the destination refused *this batch* for its own content; a resend can't succeed | no | no |
| `Refused` | the destination refuses *every* batch for now (credentials, an unknown tenant or bucket, a protocol mismatch); nothing was applied | yes | yes |
| unclassified | an error with no `Fault` attached | no | no |

`Refused` retries under both postures because, as with `Clean`, nothing reached the data: a
resend risks no duplicate. `Rejected` retries under neither: the resend would get the same answer
and hold the queue behind it. An unclassified error is `Rejected`, which keeps the existing rule
that a failure the sink didn't recognize is never retried; it's a retry decision, not a claim about
the destination.

### A `Rejected` batch drops at once

The runtime commits a `Rejected` batch off the queue on the attempt that got the verdict, counts
it as `logit.output.batches.dropped{reason="rejected"}`, and emits a throttled diagnostic carrying
the destination's text (status, body code, message). `internal` turns the diagnostic into a log
event, so a reader sees what the destination said. Nothing behind the batch waits.

### A retryable fault retries until it succeeds

A `Clean` or `Refused` fault under either posture, and an `Ambiguous` fault under
`at_least_once`, retries the head with exponential backoff from the runtime's `base_delay`
(200 ms) to the sink's `buffer.retry_max_delay`, with no end other than success or shutdown. The shutdown grace still cuts a retry (`Delivery::GraceExpired`; an in-flight send is
`Ambiguous`, per `buffered-sink-delivery`'s "Amendment: a send cut off by the shutdown grace is
`Ambiguous`").

The sink's `buffer:` is the bound on what accumulates while the head holds: `max_batches`,
`max_bytes`, `overflow` (`block`, `drop_oldest`, `drop_newest`), and `disk:`. That's the bound
every surveyed peer that retries indefinitely relies on. `buffer.retry_budget` doesn't exist, and
neither does any streak, probe, or sustained-history state; `backoff_after` and `backoff_interval`
(PR #522's fields) don't exist either. A zero `retry_max_delay` remains a config error.

`Ambiguous` under `at_least_once` retries forever too, `logit_out` included. A destination that
answers `5xx` on every attempt holds the head where it used to drop it after 60 s. That's the
peers' behavior, the buffer bounds it, and `buffer.delivery: at_most_once` is the opt-out that
drops an `Ambiguous` fault at once (`delivery-semantics`, item 5).

### The process never exits for a sink

`PERMANENT_FAILURE_WINDOW` and `write_loop`'s `Err` return go away. A sink that can't deliver
holds or drops; it never ends `logit run`. The process-ending faults are a listener's loop dying,
a Lua thread panicking or exceeding `max_memory`, and a script wedged across shutdown
([docs/deploying.md](../deploying.md), "Probes and exit codes"). A misconfigured sink is visible
through the announcements below, not through an exit code.

### A hold is announced

While a sink's head has failed at least once and a retry is pending or in flight, the gauge
`logit.component.retrying` reads `1`; it returns to `0` when the head is delivered or dropped. It
exists because `batches.dropped` stops moving while a queue holds, so an alert needs a signal
that does. The sink also writes a paced error line while the destination stays non-functional,
carrying the destination's text and the class it was read as. The existing `degraded` vocabulary
isn't reused: it counts records a codec degraded, and a gauge under the same name would carry a
different thing.

### Each sink attributes from everything its destination gives it

Each sink classifies a response from status, body code, and text, and records the mapping as a
table in its module doc, one row per response it distinguishes, each row backed by a test and
by the destination's documentation. [docs/deploying.md](../deploying.md) points at the tables;
there is no second copy in this ADR, per the one-copy rule in `AGENTS.md`, "Conventions to hold
to".

Before a sink reads a body, the shared HTTP driver (`crates/logit-outputs/src/http.rs`) applies a
status-only default:

| Status | Class | Why |
|---|---|---|
| `401`, `403`, `407` | `Refused` | credentials: the same answer for every batch |
| `404` | `Refused` | a missing endpoint, an unknown tenant or bucket, or a wrong base path; a sink whose destination answers `404` per request overrides this |
| `405`, `501` | `Refused` | a wrong method or an unsupported feature: a protocol mismatch |
| `429`, any `5xx` | `Ambiguous` | the request reached the server and may have been applied; unchanged |
| any other `4xx`, any `3xx` | `Rejected` | about this request, as the remote-write and Loki specifications define it; `400`, `413`, `415`, `422` among them |
| connect failure, DNS failure | `Clean` | unchanged |
| request timeout | `Ambiguous` | unchanged |

A response a sink can't attribute (a bare `400` with no body it can read) is `Rejected`. Holding
it would be safe when the destination is the fault and a wedge when a stream of bad batches is,
and the specifications' definition and every peer's behavior read it as the request's fault.

A `400` isn't automatically `Rejected` once a sink reads the body. InfluxDB's JSON `code`,
Datadog's per-product error bodies, Splunk HEC's numeric codes, and gRPC's status codes each
distinguish "this body is malformed" from "this bucket doesn't exist" or "this token can't write
here", and a sink's table maps each to the class it means. The `RefusesSink` marker in the HTTP
driver, which the per-request verdicts amendment introduced for `datadog_out`'s `403`, is
replaced by the `Refused` class.

Sinks with no application response (`syslog_out`, `statsd_out`, `graphite_out`, `collectd_out`,
`file_out`, `stdio_out`) classify by I/O error alone: a connect failure is `Clean`, a mid-write
failure `Ambiguous`, and a local encode-side refusal `Rejected`.

### No cap on consecutive refusals

A `Refused` head that is in truth a bad batch, misattributed by its sink's table, holds forever.
A cap on consecutive refusals (Grafana Alloy's `max_backoff_retries` shape) would bound that, and
is declined: a cap turns every real outage longer than the cap into one lost batch per cap, which
is the loss this record exists to stop. The hold is visible through `logit.component.retrying`
and the paced error line carrying the destination's text, so a misattribution is seen and fixed
in the sink's table. A dead-letter path for dropped and held batches is the remedy for the case
the table can't resolve, and is a follow-up with its own record.

### Order is kept

One batch at a time per sink, in queue order. There are no concurrency slots that let a failing
request sit while later batches proceed, and no probing with a later batch. The native hop's
sequence numbers and remote write's per-series order rule both out, and with `Rejected` dropped at
once nothing poisonous is held.

### The native hop's acknowledgment carries a status

`Ack` gains a status, `accepted` or `rejected(reason)`. `logit_in` answers `rejected` for a frame
it decodes but can't take (an oversize payload past its cap, a payload it won't forward); the
receiver has handled that sequence by dropping it, so the cumulative mark advances and `logit_out`
commits the sequence as `batches.dropped{reason="rejected"}`. Transient trouble needs no message:
not acknowledging is the backpressure, as today. The wire form is W3's own ADR amending
[ADR `native-hop-named-acks`](native-hop-named-acks.md); there is no compatibility shim
([ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md)).

## Alternatives considered

- **Keep `Permanent`, a retry budget, and the 60 s exit (the previous design).** Rejected: the
  exit loses every other sink's queued data for one sink's misconfiguration, the budget drops a
  good batch during any outage longer than 60 s, and no surveyed peer does either. The exit's
  original purpose, a bad token visible to a restart-policy supervisor, is served by the hold's
  gauge and error line instead.
- **Hold the head after a 60 s streak and probe it once a minute (PR #522).** Rejected: a batch
  rejected for its own content is probed with the same batch forever, so the sink wedges on it.
  No surveyed peer probes with a held batch; the ones that hold a rejection are the subjects of
  complaints.
- **A cap on consecutive refusals of one head.** Declined, as above: it converts an outage into a
  steady drip of lost batches to guard against a misattribution that the announcements make
  visible and a table fix resolves.
- **Give up order for concurrency slots**, as Vector and the Collector do, so a failing request
  doesn't block the queue. Rejected: the native hop's sequence and remote write's per-series order
  need in-order delivery, and once a rejection drops at once there's nothing poisonous left to
  block on.
- **Treat every `4xx` other than `429` as `Rejected`**, as the specifications define it. Rejected
  as the whole rule: a `401`, `403`, or `404` describes the destination or the config, not the
  batch, and dropping a good batch on each is the Vector behavior operators filed issues against.
  It remains the default for the statuses that do describe the request.
- **One class table in this ADR.** Rejected: the table changes with each sink's evidence, and the
  comment rules make a module doc describing behavior the canonical copy.

## Consequences

- W1 (`fault/w1`) changes the runtime: `Fault` gains `Rejected` and `Refused` and loses
  `Permanent`, `classify`'s default becomes `Rejected`, `is_explicitly_permanent` and
  `PERMANENT_FAILURE_WINDOW` go away, `deliver_with_retry` and `deliver_window` retry until
  success or shutdown, `BufferConfig` loses `retry_budget` (schema regenerated), and the
  `retrying` gauge and `rejected` drop reason land in `docs/design/internal-telemetry.md`. Every
  `Fault::Permanent` construction becomes `Rejected` or, where the status alone says so through
  the driver default above, `Refused`. That mapping is mechanical.
- W2 (`fault/w2`) is the evidence-backed refinement: each sink's module-doc table from its
  destination's documentation, a test per row, and the body-aware mappings that turn a `400` into
  `Refused` where the body says so.
- W3 is the `Ack` status, W4 makes `otlp_out` report per signal so a `Rejected` signal drops alone,
  and W5 sweeps the operator docs. [docs/plans/sink-fault-model.md](../plans/sink-fault-model.md)
  has each.
- A destination down for hours holds a sink's queue for hours. An operator who would rather drop
  than hold sizes `buffer:` with `overflow: drop_oldest` or `drop_newest`, or chooses
  `at_most_once` for the `Ambiguous` case. Memory and disk cost follow `buffer:`, which already
  bounds them.
- Readiness is unchanged: a sink holding its queue shows nowhere on `/readyz`
  (`docs/known-gaps/runtime.md`, "Readiness is per-process, not per-sink").
- A dead-letter path and an oversize split are the follow-ups this record points at and doesn't
  build. Until the first exists, a `Rejected` batch is visible in a counter and a log event and
  nowhere else.
- This record supersedes the `Permanent` rule, the retry budget, and the rejected "never exit"
  alternative in `buffered-sink-delivery`; the outage paragraph in
  [ADR `service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md); and the
  pipeline ending in decision 11 of
  [ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md). It
  amends `delivery-semantics` item 2 and item 5's table. Each carries a dated pointer here.

## Amendment: `otlp_out` departs from the OTLP specification's non-retryable list (2026-10-04)

The [OTLP specification](https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#failures)
says a client MUST NOT retry three answers that the
[retryable set](https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#retryable-response-codes)
leaves out: an HTTP `5xx` other than `502`, `503`, and `504` ("All other `4xx` or `5xx` response
status codes MUST NOT be retried"), a gRPC `INTERNAL`, and a gRPC `RESOURCE_EXHAUSTED` without a
`RetryInfo` detail. The OpenTelemetry Collector's OTLP exporters drop all three as permanent.

`otlp_out` reads them as `Ambiguous` instead, the class this record's status-only default gives a
`5xx`:

- **The server failed, not the batch.** A `Rejected` answer means the destination refused this
  batch for its own content. A `500` or an `INTERNAL` says the server couldn't process the
  request, and a `RESOURCE_EXHAUSTED` says it ran short; none says a resend would get the same
  answer.
- **The specification's rule avoids a duplicate, and the posture governs duplicates.** A request
  that failed this way may have been applied. `at_least_once` accepts that risk and retries;
  `at_most_once` refuses it and drops ([ADR `delivery-semantics`](delivery-semantics.md), item 5).
  Reading the answer as `Rejected` would drop the batch under both, overriding the operator's
  choice.

Consequence: under `at_least_once`, a backend that answers `RESOURCE_EXHAUSTED` for a rate limit,
or `500` for a transient failure, as New Relic's OTLP intake documents
([docs/plans/newrelic-relay.md](../plans/newrelic-relay.md)), holds the batch and retries it with
backoff until the backend recovers, where the Collector would drop it; a resend can store the batch
twice if the first attempt was applied. Under `at_most_once` the batch drops at once, counted
`batches.dropped{reason="ambiguous_at_most_once"}`. Every other row of `otlp_out`'s table follows the
specification; the table is `crates/logit-outputs/src/otlp.rs`'s module doc, "Response classes".
