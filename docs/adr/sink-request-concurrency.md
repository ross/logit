---
created: 2026-10-25
updated: 2026-10-25
---

# Sink request concurrency: a per-sink `concurrency:` keeps several batches in flight, each settled on its own

## Status
Accepted. Supersedes in part:

- [ADR `sink-fault-classes`](sink-fault-classes.md): the "Order is kept" section and the rejected
  alternative "Give up order for concurrency slots". Order stays the default, and a sink gives it
  up only where its `concurrency:` says so.
- [ADR `native-hop-ack-status`](native-hop-ack-status.md): decision 4's "The marker is legal only
  beside `Fault::Rejected`". That holds for `logit_out`; a sink with independent request outcomes
  returns `HeadOnly` beside any class.
- [ADR `native-hop-send-window`](native-hop-send-window.md): decision 2's `window` field on
  `logit_out`, renamed `concurrency`, and decision 4's "Under `at_most_once` an `Ambiguous` fault
  also commits every other outstanding item", which holds for a connection-level fault alone.

## Context

Every sink that waits for a response, `logit_out` apart, sends one request and takes the next
batch only after the response arrives. Its throughput is one batch per round trip plus the
destination's own time: 50-event batches over a 200 ms round trip move 250 events a second,
whatever the hardware. Once the offered rate passes that bound the sink's buffer fills, and the
sink drops (`overflow: drop_newest`) or stalls every source (`block`) with nothing naming the
round trip as the cause. The `otlp-fan-out` soak run recorded in
[`docs/plans/soak-harness.md`](../plans/soak-harness.md)'s "Findings" showed it: 500 records a
second in batches of 50 over OTLP/HTTP, the responses delayed 200 ms, and a fifth of the offered
batches counted `overflow_newest`. `docs/known-gaps/sinks.md` listed it as a known gap, with a
`logit_out` hop to a `logit_in` near the destination as the workaround, until the plan's W5
closed the entry.

What the code has, and what stands in the way:

- **A window path exists, built for the native hop.** `Output::window`, `submit`, and
  `await_ack` (`crates/logit-pipeline/src/output.rs`) and `write_loop`'s `deliver_window`
  (`crates/logit-pipeline/src/runtime.rs`) keep up to a window of batches submitted and
  unacknowledged, reserved as a prefix of the sink's store ([ADR
  `native-hop-send-window`](native-hop-send-window.md)). Its fault model is a connection's:
  `await_ack` answers the oldest submission, and any error means the sink dropped its connection,
  so the loop zeroes its count and resubmits every unacknowledged batch from the head. The one
  exception, `HeadOnly`, settles the head alone and is legal only beside `Rejected` ([ADR
  `native-hop-ack-status`](native-hop-ack-status.md)). Resubmitting a window is safe for the hop
  because `logit_in` deduplicates by sequence; an HTTP destination would store the resent
  batches again.
- **HTTP outcomes are per request and arrive in any order.** A `500` on batch 3 says nothing
  about batches 4 and 5, which may already be stored.
- **Every HTTP sink keeps its per-batch state in one slot.** `BatchAccounting`
  (`crates/logit-outputs/src/accounting.rs`) holds one armed batch's counted encode units and
  settled-request bits behind one shared `CountGate`; `datadog_out` fixes one `batch_now` per
  batch; `splunk_hec_out` reuses one object buffer; each encoder's stats and each sink's
  throttled `Diagnostics` are `&mut` fields. A second `observe_batch` before the first batch
  settles would erase the first batch's memory ([ADR
  `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md), decision 2).
- **Transport.** `reqwest` is built without its `http2` feature (`Cargo.toml`), so `otlp_out`
  over HTTP, `datadog_out`, `datadog_trace_out`, `splunk_hec_out`, `influxdb_out`, and
  `prometheus_out`'s remote-write sender speak HTTP/1.1 and would carry concurrent requests on
  several pooled connections. `otlp_out`'s gRPC client is an h2-only `hyper_util` client that
  takes `&self`, so it can multiplex streams on its one connection, and nothing has asked it to.
- **[ADR `sink-fault-classes`](sink-fault-classes.md) chose order.** "One batch at a time per
  sink, in queue order. There are no concurrency slots", citing the hop's sequence numbers and
  remote write's per-series order.

Whether a destination tolerates batches landing out of order decides where concurrency is
safe. Reordering changes only which of two writes to the same point wins, or whether a receiver
accepts an older sample after a newer one:

| Sink | Batches landing out of order |
|---|---|
| `influxdb_out` | Last to arrive wins. InfluxDB overwrites a point by `(measurement, tag set, timestamp)`, so when two batches write one point with different values, the batch that lands last leaves its value, which above 1 can be the older batch. |
| `datadog_out` | Series: last to arrive wins, as for InfluxDB, at one point per series per second. Every other route stores what it receives. |
| `datadog_trace_out`, `splunk_hec_out` | Safe. Nothing at the destination depends on order across requests. Splunk's `Clean`-before-acceptance rows depend on order within one `send`, which stays sequential. |
| `otlp_out` | Logs and traces: safe. Metrics into a Prometheus-backed receiver: as remote write. |
| `prometheus_out` remote write | Both specs require one series' samples in timestamp order. A receiver with no out-of-order window answers `400`, the sink reads `Rejected`, and the batch drops ([`docs/known-gaps/prometheus.md`](../known-gaps/prometheus.md), "The remote-write sender does no cross-batch reordering"). Which receivers accept out-of-order samples, and under what setting, is the receiver table in `crates/logit-outputs/src/prometheus.rs`'s module doc, "Concurrency". |

A single chain into one `prometheus_out` can't draw an out-of-order `400` today, because
batches leave in order. Concurrency would turn that chain into the fan-in case the known-gaps
entry describes.

## Decision

Each HTTP sink gets a `concurrency:` field, default 1, and the write loop settles every batch
in flight on its own outcome, so a fault on one batch neither resends nor drops the others.

### 1. Config: `concurrency:` on every sink that waits for a response

- **The field.** `concurrency: u32` on `influxdb_out`, `otlp_out`, `datadog_out`,
  `datadog_trace_out`, `splunk_hec_out`, and `prometheus_out` (read in remote-write mode;
  exposition mode rejects it, as graph rule 56 rejects every field the other mode owns).
  Default 1. Range `1..=1024`, a graph rule in
  [`docs/design/pipeline-graph.md`](../design/pipeline-graph.md)'s rule table.
- **`logit_out.window` becomes `logit_out.concurrency`**, with no alias, under
  [ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md)'s posture. Graph rule 75
  reads the new name. The wire's `Hello.window` and `HelloAck.window`, the negotiated value
  `logit.output.window` reports, and the hop's docs keep "window": that's the protocol's fact,
  negotiated per connection, where `concurrency` is the operator's bound on it. The operator doc
  says the two are the same number on a connection that answered at or above it.
- **The operator doc on each field** says what raising it costs: batches can land out of order,
  and under `at_most_once` each in-flight batch is its own unknown outcome. The remote-write and
  `otlp_out` docs name the receivers that accept out-of-order samples (the table above) before
  the operator raises it.

### 2. The write loop settles each in-flight batch on its own

- **`HeadOnly` beside any class.** An `await_ack` error carrying `HeadOnly` means "this fault is
  the oldest submission's alone; every later submission is still outstanding, in order". The
  class decides the head's fate as it does on the fast path:
  - `Rejected` drops the head alone and commits it, as today.
  - A retryable fault (`Clean`, `Refused`, or `Ambiguous` under `at_least_once`) backs off and
    resubmits the head alone. The sink replaces that slot and the others keep running. The
    backoff schedule and `logit.component.retrying` stay the loop's, keyed to the head. A failed
    resubmit follows the same mark: with `HeadOnly` it's the head's own fault and is handled as
    above, and without it the sink dropped its connection, so nothing is outstanding and the
    loop resubmits from the head.
  - `Ambiguous` under `at_most_once` drops the head alone, counted
    `ambiguous_at_most_once`, and nothing else is committed. The native hop's "every other
    outstanding item" rule holds for a connection-level fault, which arrives without `HeadOnly`.
- **A fault behind the head waits its turn.** A slot whose request failed while an older batch
  is still outstanding keeps that outcome, and the task never retries on its own; the loop reads
  it from `await_ack` once the batch is the head and handles it as above. In-order commit holds,
  and the backoff schedule stays one per head.
- **A non-`HeadOnly` error keeps the connection model.** `logit_out` returns it for a dropped
  connection; the loop zeroes its count and resubmits from the head, as today. An HTTP sink never
  returns one.
- **The fill is the exposure bound.** The loop submits while `outstanding < concurrency`. A held
  head (a `Refused` destination) stops the fill at one window, so a refusing destination sees at
  most `concurrency` batches per backoff, and `buffer:` bounds what queues behind them, as [ADR
  `sink-fault-classes`](sink-fault-classes.md)'s hold rule intends.
- **A submit error past the head** stays unclassified and stops the fill, as today. A sink that
  can't start a request (the encode failed, or nothing could be spawned) reports it when that
  batch becomes the head.
- **Telemetry.** `logit.component.send.duration` records one sample per batch on a sink using
  the slot helper (decision 3): the batch's own request, from its send to its response, timed by
  the helper. A batch answered behind a held head waits for the head's retries before the loop
  reads its answer, so the loop's clock would count that wait. The hop keeps one sample per
  windowed round. `logit.output.in_flight` becomes a gauge every sink above a concurrency of
  1 reports. The `buffer_full` warning on a sink whose `in_flight`
  equals its `concurrency` names the limit and the recent send duration, so an operator reading
  the line at the default sees the round trip as the cause and `concurrency:` as the remedy.
  [`docs/design/internal-telemetry.md`](../design/internal-telemetry.md) records each.
- **Shutdown.** Unchanged: the grace cut counts every outstanding batch, under `at_most_once`
  commits them as shutdown drops, and under `at_least_once` leaves them reserved for a memory
  store's `finish` or a disk store's replay. A spool replay resends every batch in flight at the
  crash, delivered ones included, which `docs/known-gaps/sinks.md`'s "The per-request retry
  memory of `otlp_out` and the Datadog sinks is in-process" already records.

### 3. A shared slot helper in the HTTP driver

- **One helper in `crates/logit-outputs/src/http.rs`** gives the six sinks `window`, `submit`,
  and `await_ack` over a `VecDeque` of slots keyed by the batch's `SeqId`.
- **`submit` encodes on the loop task, synchronously**, as `send` does today. The `CountGate`
  mute around a re-encode stays synchronous, so one gate per sink still serves every slot; the
  per-batch memory (`BatchAccounting`'s `counted`, `accepted`, and `rejected` bits, and
  `datadog_out`'s `batch_now`) moves into the slot. `observe_batch` arms the slot for its
  `SeqId` instead of the sink.
- **The transport runs as a spawned task per slot**, holding a clone of the client and the
  encoded bodies, and returns an outcome: per-request verdicts, the status class, the settled
  bits, and the diagnostic text. `await_ack` awaits the front slot's task and applies the
  outcome on the loop task: the transport counters, the throttled diagnostics, and the slot's
  `finish`. Every `&mut` field stays on the loop task.
- **A batch's own requests stay sequential within its slot**: `otlp_out` one per signal,
  `datadog_out` and `datadog_trace_out` one per route and cut, `splunk_hec_out` one per body
  and then its acknowledgment poll. The settled-request memory and the per-request retry rules
  ([ADR `sink-fault-classes`](sink-fault-classes.md)'s amendments) are unchanged per slot, and
  `splunk_hec_out`'s `Clean`-before-acceptance rows read the slot's own `sent_any`.
- **A resubmitted head reuses its slot's memory**: the same `SeqId` finds the settled bits, so a
  retry resends only the unsettled requests, as today.
- **Cancellation aborts the tasks.** A cancelled `submit` or `await_ack` (the grace cut) drops
  the helper's tasks; the destination may still apply a request in flight, which is the
  `Ambiguous`-at-shutdown case the postures already cover.
- **Transport bound.** Each request keeps its `timeout:`; the loop applies no attempt time past
  the head, so the sink's timeouts bound every slot, as the trait requires.

### 4. HTTP/2 through `reqwest`'s `http2` feature

- **Enabled, in its own PR.** With the feature on, `reqwest` offers `h2` and `http/1.1` through
  ALPN on a TLS endpoint and takes what the server picks; a plaintext endpoint stays HTTP/1.1.
  No per-sink knob. The feature sets ALPN only on the config `reqwest` builds itself: a sink with
  a `tls:` block hands it a preconfigured config, used as given, and the shared client builder
  leaves ALPN empty for `otlp_out`'s gRPC connector, so the HTTP sinks' client builder sets
  `h2` and `http/1.1` on its own copy (`logit_pipeline::tls::with_http_alpn`). `prometheus_in`'s
  scrape client does the same under `scrape_tls:`, so a target is scraped over h2 with or without
  the block.
- **A cold pool dials per request.** Until a handshake has shown h2, `hyper-util`'s pool dials
  a connection for each request waiting, so a burst of `concurrency` requests to an idle sink
  opens that many; the first to negotiate h2 carries them all and the rest close unused.
- **What it buys.** Concurrent requests to an h2 server multiplex on one connection instead of
  opening `concurrency` pooled connections.
- **What it costs.** One connection is one failure domain: a reset or a `GOAWAY` fails every
  request in flight on it at once, each `Ambiguous`, where separate connections fail one.
  Packet loss on that one connection stalls every stream (TCP head-of-line blocking), where
  separate connections stall one. [`docs/known-gaps/otlp.md`](../known-gaps/otlp.md)'s
  VictoriaTraces close is the recorded case of the first. The gRPC transport already carries
  both costs.
- **Dependencies.** `h2` is already in the build through `hyper`'s `http2` feature, so the
  feature adds no crate.

### 5. Default 1, and it stays

Every sink defaults to `concurrency: 1`, and the default doesn't move. Above 1, "last write
wins" at a destination means the last to arrive, so an older batch landing after a newer one
overwrites the newer value for every point the two share. An operator raises the field knowing
that for their data; the soak scenarios in the plan measure the gain and the fault handling at 8
and inform that choice, not the default.

### 6. Soak coverage is part of the change

The stream isn't done until the soak harness runs the HTTP sinks above 1: a slow-link scenario
measured at 1 and 8, the `otlp-fan-out` scenario at 8, and a stand-in scenario cueing a held
head, a rejected slot, and an ambiguous slot while other slots are in flight. The plan names
them.

### 7. Idle pooled connections close first (2026-10-25)

- **The race.** Above `concurrency: 1` a sink's pool keeps spare connections idle between
  bursts. A destination that closes an idle connection sooner than the pool does can receive a
  write on it after the close: the sink wrote it before the close reached it. The destination
  resets the connection, and the sink reads `Ambiguous` ("connection closed before message
  completed"), which `at_most_once` drops and `at_least_once` resends. A delay on the link widens
  the window to the delay. The plan's W5 `standin-concurrency` run measured it: under a 1 s
  delay, a stand-in closing idle connections at 5 s reset 12 writes no fault caused in about
  2,300 batches, against the pool's 90 s default.
- **The field.** `idle_timeout: Duration` on `influxdb_out`, `otlp_out`, `datadog_out`,
  `datadog_trace_out`, `splunk_hec_out`, and `prometheus_out` (remote-write mode; exposition mode
  rejects a non-default value, as graph rule 56 rejects `concurrency`). A pooled connection idle
  longer than this is closed instead of reused. `0s` keeps no idle connection, so every request
  opens its own. `crate::http::client_builder` applies it as `reqwest`'s `pool_idle_timeout`, or
  `pool_max_idle_per_host(0)` at `0s`; `http::hyper_client_builder` applies the same settings
  to `otlp_out`'s gRPC pool and `datadog_trace_out`'s Unix-socket pool. A `hyper-util` pool with
  no pool timer drops an expired connection only at the next checkout, so its socket would stay
  open; the builder sets `TokioTimer` for that reason.
- **Default `30s`.** Two bounds decide it. Below the destination's keep-alive idle timeout, by more
  than a round trip, so the sink closes first; above the interval between a sink's batches, so a
  sink flushing on a 10 s or 15 s `aggregate` window reuses its connection instead of redialing,
  handshake included, per batch. 30 s sits under every destination below except the three 5 s
  servers, and above both cadences. A destination that closes at 5 s is the case for lowering it,
  and the field's doc says so.

  | Destination | Keep-alive idle timeout, by default |
  |---|---|
  | Node.js `http.Server` (`keepAliveTimeout`) | 5 s |
  | Apache httpd (`KeepAliveTimeout`) | 5 s |
  | uvicorn (`--timeout-keep-alive`) | 5 s |
  | VictoriaMetrics (`-http.idleConnTimeout`) | 60 s |
  | OpenTelemetry Collector, `otlp` receiver over HTTP (`protocols.http.idle_timeout`) | 60 s |
  | AWS Application Load Balancer (`idle_timeout.timeout_seconds`) | 60 s |
  | nginx (`keepalive_timeout`) | 75 s |
  | Grafana Mimir (`-server.http-idle-timeout`) | 120 s |
  | InfluxDB 2.x (`http-idle-timeout`) | 180 s |

  This table is the one copy; `docs/deploying.md` points here, and the field's doc points at
  `docs/deploying.md`.
- **What's left.** A destination that closes a kept-alive connection for any other reason (a
  restart, a per-connection request cap, an idle timeout below `idle_timeout:`) still races a
  write; `docs/known-gaps/sinks.md`'s "A kept-alive connection a destination closed first reads as
  `Ambiguous`" keeps that residual.
- **h2's `GOAWAY` is the other mitigation.** A server closing an idle h2 connection sends
  `GOAWAY` naming the last stream it processed, so the client knows which requests the server
  never saw. `reqwest` 0.12 resends a request a graceful `GOAWAY` (`NO_ERROR`) or a
  `REFUSED_STREAM` turned away on a new connection, and `hyper-util`'s pool resends one it
  hadn't started on a reused connection that closed, so neither reaches the sink as a fault. The
  `otlp-concurrency-tls` soak scenario pauses its generator past the listener's `idle_timeout:`
  to exercise that path. HTTP/1.1 has no such signal for a request already written, which is
  why the field is the remedy there.
- **Rejected: resend a write that met a reused connection's close before any byte came back.**
  Chromium resends a request that failed on a reused connection with zero bytes received, since
  the server can't have answered it. `reqwest` doesn't expose whether a failed request's
  connection was reused, so the sink can't apply the rule; owning connections through `hyper`
  directly to learn it is a stream of its own. The rule also still resends a request the server
  may have applied before closing, which `at_most_once` forbids.

## Alternatives considered

- **Concurrency inside each sink's `send`.** A sink could run its own requests for several
  batches and report one result. It would copy `write_loop`'s backoff, `retrying`, drop
  accounting, and posture handling into six sinks, and `send` takes one batch.
- **Independent per-batch delivery tasks over a new `&self` trait.** Each slot would run its own
  `deliver_with_retry`, committing out of order. The store's `commit` is a cursor on both the
  memory queue and the disk spool, so out-of-order commit needs a per-record acknowledged mark,
  persisted in the spool, and every `Output` would change signature. The prefix-reserved window
  with in-order commit gives the same throughput at the cost of a slow slot delaying later
  commits, not later sends.
- **Coalescing queued batches into one request.** Fewer, larger requests raise the bound without
  reordering. It needs every encoder to take several batches with distinct resources and
  contexts, doesn't help a listener already assembling batches at the destination's size cap,
  and is orthogonal: an operator can raise the listener's batch bounds today.
- **A default above 1 on the sinks where reordering is harmless.** Rejected. `datadog_trace_out`
  and `splunk_hec_out` could default higher with no reordering a destination reads as wrong, and
  `otlp_out` can't, since one `concurrency:` covers its logs, traces, and metrics. The default is
  1 on every sink anyway, so an operator raising it reads the tolerance table for that sink once
  instead of learning a different default per kind. The gain above 1 is throughput alone, and the
  opt-in is one line. On the other sinks, only the operator knows whether their series repeat a
  point across batches.
- **Refusing `concurrency > 1` on remote write in validation.** Declined. VictoriaMetrics, the
  main remote-write target here, accepts out-of-order samples, and a Prometheus or Mimir with its
  window open does too. The field's doc carries the warning; `logit validate` doesn't know the
  receiver.
- **Credits or a receiver-driven window.** An HTTP destination grants nothing; `429` and `503`
  are its backpressure, and the fault classes already hold on them.
- **Out-of-order acknowledgment through the hop.** `logit_in` forwards in order, so the hop keeps
  its connection model; this record changes nothing on the wire.

## Consequences

- **Code.** `Output`'s `await_ack` contract and `window_round`'s `HeadOnly` handling, with the
  `WindowedOutput` fake's tests in `runtime.rs`; the `concurrency` field, graph rule, and
  `script/schema`; the rename on `logit_out` through `crates/logit-cli/src/pipeline.rs`; the slot
  helper in `http.rs` and each sink's `submit`/`await_ack`; `BatchAccounting` per slot; the
  `http2` feature. [`docs/plans/sink-request-concurrency.md`](../plans/sink-request-concurrency.md)
  sequences it.
- **Telemetry.** `send.duration` per batch on a sink using the slot helper and one per windowed
  round on `logit_out`, `logit.output.in_flight` on every sink above 1, and the `buffer_full`
  line's cause.
- **Docs.** [`docs/deploying.md`](../deploying.md)'s "Send window" section and its
  `at_most_once` note move to `concurrency`; each sink's module doc gains the field and its
  ordering note; the remote-write and `otlp_out` docs carry the receiver table;
  `docs/design/pipeline-graph.md`'s rule table and "Cancellation points" row; the amended ADRs'
  Status lines.
- **Known gaps.** `docs/known-gaps/sinks.md`'s one-request-at-a-time entry closes with the
  plan's W5, whose soak runs are the evidence in the plan's "Findings". `docs/known-gaps/prometheus.md`'s fan-in entry gains the single-chain case under
  `concurrency > 1`. `docs/known-gaps/otlp.md`'s VictoriaTraces entry widens to every request in
  flight on the closed connection.
- **Measurement.** The plan's "Findings" records the soak numbers at 1 and 8, for the operator
  docs' guidance on raising the field.
- **Idle connections.** Decision 7's `idle_timeout:` on the six sinks, its graph rule 56 check,
  and the two soak scenarios that exercise it; `docs/known-gaps/sinks.md`'s idle-connection
  entry narrows to the residual.
