---
created: 2026-10-25
updated: 2026-10-25
---

# Enabling plan: per-sink request concurrency — several batches in flight, each settled on its own

## Context

Every sink that waits for a response, `logit_out` apart, sends one request per round trip, so
over a slow link its buffer fills and it drops or stalls with nothing naming the cause. That was
a [`docs/known-gaps/sinks.md`](../known-gaps/sinks.md) entry until W5 closed it. [ADR
`sink-request-concurrency`](../adr/sink-request-concurrency.md) decides the fix: a
`concurrency:` field on each HTTP sink, a write loop that settles each in-flight batch on its
own outcome, a shared slot helper in the HTTP driver, and HTTP/2 through `reqwest`.

Stream key: `conc`.

Goals:

- An HTTP sink at `concurrency: N` keeps up to N batches in flight, and a fault on one batch
  neither resends nor drops the others.
- Nothing changes at the default of 1, and the `buffer_full` line at the default names the
  round trip as the cause.
- The soak harness measures the gain and exercises the fault handling above 1.

Non-goals:

- A default above 1 on any sink. The ADR's decision 5 keeps 1: above it, a destination's
  "last write wins" is the last to arrive, so the operator opts in.
- Out-of-order commit in the sink store, or a per-record acknowledged mark in the spool.
- Concurrency on `kafka_out` (its producer has its own in-flight model), on the datagram and
  stream sinks (no response to wait for), or on `logit_out` beyond the field's rename.
- Coalescing queued batches into one request.

## Design

### W1: the loop, the trait, the field, and the rename

- **`HeadOnly` beside any class.** `Output::await_ack`'s doc and `window_round` in
  `crates/logit-pipeline/src/runtime.rs`: a `HeadOnly` error settles the head alone under its
  class, a retryable one backs off and resubmits the head alone with the others outstanding,
  and `Ambiguous` under `at_most_once` drops the head alone. A non-`HeadOnly` error keeps the
  connection model. `deliver_window` gains a "resubmit the head" step, and the `at_most_once`
  multi-commit runs only for a non-`HeadOnly` `Ambiguous`.
- **Tests on the `WindowedOutput` fake** (`runtime.rs`'s window test block): a retryable
  `HeadOnly` resubmits the head alone and the others deliver in order; `HeadOnly` `Ambiguous`
  under `at_most_once` drops one batch; a `Refused` head holds the fill at one window; a
  resubmitted head is observed once; the grace cut with a head mid-retry. Each regression test
  is run once with its loop change reverted, and the PR body says so.
- **Config.** `concurrency: u32` on the six sinks in `crates/logit-config/src/lib.rs`, with the
  operator doc the ADR's decision 1 describes; `logit_out.window` renamed `concurrency`, no
  alias; rule 75 reads the new name and a new rule covers the six sinks' range;
  `script/schema`. `crates/logit-cli/src/pipeline.rs` passes each through `with_concurrency`.
  Every shipped config naming `window:` on a `logit_out` moves (`examples/`, `fixtures/`,
  `tools/soak/scenarios/`, `demo/`), and `script/validate` confirms it.
- **Telemetry.** `send.duration` per batch on the window path; `logit.output.in_flight` on
  `logit_out` is unchanged. [`docs/design/internal-telemetry.md`](../design/internal-telemetry.md)
  and [`docs/design/pipeline-graph.md`](../design/pipeline-graph.md)'s rule table and
  "Cancellation points" row.
- **Docs.** [`docs/deploying.md`](../deploying.md)'s "Send window" and its `at_most_once` note
  under the new name; the hop ADRs' Status lines are already amended by W0.

### W2: the slot helper, `influxdb_out`, remote write, and the cause

- **The helper** in `crates/logit-outputs/src/http.rs`: a `VecDeque` of slots keyed by `SeqId`,
  each holding its `BatchAccounting`, its encoded bodies, and a spawned transport task; `window`
  reads the sink's `concurrency`; `submit` encodes on the loop task and spawns; `await_ack`
  awaits the front slot and applies its outcome (transport counters, throttled diagnostics,
  `finish`) on the loop task; drop aborts every task. `BatchAccounting` moves from the sink to
  the slot, with `observe_batch` arming the slot.
- **The two single-request sinks first**: `influxdb_out` and `prometheus_out`'s remote-write
  sender, each a `send` that becomes one slot's task. Their tests cover: N requests in flight
  against a canned receiver that answers out of order; a `500` on one slot resent alone while
  the others deliver; a `400` on one slot dropped alone; a `401` holding the head at one window;
  `at_most_once` dropping one ambiguous slot; the grace cut aborting the tasks.
- **Naming the cause.** `logit.output.in_flight` on every sink above 1, and the `buffer_full`
  line on a sink whose `in_flight` equals its `concurrency` names the limit and the recent send
  duration. [ADR `buffer-full-diagnostics`](../adr/buffer-full-diagnostics.md)'s key table gains
  the fields.
- **Docs.** Each sink's module doc gains the field and its ordering note, and the remote-write
  doc the receiver table. `docs/known-gaps/prometheus.md`'s fan-in entry gains the single-chain
  case under `concurrency > 1`.

### W3: the multi-request sinks

- **`otlp_out`** over HTTP and gRPC: a slot's task sends its signals in order, skipping settled
  ones; the gRPC client's clone multiplexes slots on its connection. `docs/known-gaps/otlp.md`'s
  VictoriaTraces entry widens to every request in flight.
- **`datadog_out`** and **`datadog_trace_out`**: `batch_now` moves into the slot; the plan and
  cuts stay pure functions of the batch and the slot's time, so request numbering holds across
  a head's resubmit.
- **`splunk_hec_out`**: one object buffer per slot; the acknowledgment poll runs inside the
  slot's task; the `Clean`-before-acceptance rows read the slot's `sent_any`.
- **Tests** per sink as W2's list, plus each sink's per-request retry memory surviving a head
  resubmit with other slots in flight.

### W4: HTTP/2

- `reqwest`'s `http2` feature in `Cargo.toml`, `cargo deny` clean, and a test that an h2
  listener (`influxdb_in` under `bind_tls:`, which advertises `h2` through ALPN) sees one
  connection carrying several slots, where a plaintext listener sees several connections.
- The ADR's decision 4 trade-offs in `docs/deploying.md`'s sink section.

### W5: soak, findings, and the known-gaps closing

- **`http-concurrency`** (`tools/soak/scenarios/http-concurrency/`): `influxdb_out` into
  `influxdb_in`, `wire = "http"` as `http-ingress` declares it, 200 ms ± 50 ms netem on
  the responses, run at `concurrency: 1` and `8` as two scenarios (`http-concurrency-1`), with
  `[[expect]]` rows on `logit.output.in_flight` and on
  `batches.dropped{reason="overflow_newest"}` at 0 under 8. It runs over plaintext:
  `influxdb_out` has no `tls:` field, so it can't trust the run's private CA.
  `otlp-concurrency-tls` runs `otlp_out` into `otlp_in` over TLS instead, where ALPN picks
  HTTP/2.
- **`otlp-fan-out`** at `concurrency: 8`, as a second config or a sibling scenario, judged by the
  same ledger as today's run, with its `overflow_newest` count compared with the run recorded
  in [`docs/plans/soak-harness.md`](soak-harness.md)'s "Findings".
- **`standin-concurrency`**, `standin-remote-write`'s shape: `prometheus_out` at 8 against the
  stand-in, `respond` steps cueing a `401` held head with slots behind it, a `400` on one slot,
  and a dropped response on one slot under `at_most_once`, each step naming the class and the
  count the sink must report, with the stand-in's log as V.
- **Findings** below: the throughput at 1 and 8 and the fault rows, feeding the operator docs'
  guidance on raising the field. `docs/known-gaps/sinks.md`'s entry closes.

### Mechanics

Per AGENTS.md "Workflow": each workstream is a PR stacked on the one before (`gh stack` from
W0), `pr-review` runs on each code PR before the fix rounds, every regression test is run once
with its fix reverted and the PR body says so, and the prose sweep (docs, ADRs, module docs,
config field docs, `docs/known-gaps/`) precedes each review. A concurrency test waits on an
observable through `logit_pipeline::test_util`, never a sleep.

## Workstreams

| # | Branch | PR title | Size | Depends on | Status |
|---|---|---|---|---|---|
| W0 | `conc/w0` | `conc/w0: ADR and plan for per-sink request concurrency` | S | — | open |
| W1 | `conc/w1` | `conc/w1: per-slot HeadOnly faults, the concurrency field, and the logit_out rename` | M | W0 | open (#870) |
| W2 | `conc/w2` | `conc/w2: the HTTP slot helper, influxdb_out, remote write, and the buffer_full cause` | L | W1 | open (#872) |
| W3 | `conc/w3` | `conc/w3: otlp_out, datadog_out, datadog_trace_out, and splunk_hec_out on the slot helper` | L | W2 | open (#873) |
| W4 | `conc/w4` | `conc/w4: HTTP/2 through reqwest` | S | W3 | open (#874) |
| W5 | `conc/w5` | `conc/w5: soak scenarios for request concurrency and the known-gaps closing` | M | W4 | — |

## Settled questions

- **Field name: decided by the maintainer, 2026-10-25.** `concurrency`, on `logit_out` too, with
  no alias for `window`.
- **Default: decided by the maintainer, 2026-10-25.** 1 on every sink, and it stays: above 1 a
  destination's "last write wins" is the last to arrive, which can be the older batch, so
  reordering is the operator's opt-in. The soak findings inform the docs, not the default.
- **Remote write above 1: decided by the maintainer, 2026-10-25.** Allowed, with the field's doc
  naming the receivers that accept out-of-order samples.
- **Where the head retries: decided by the maintainer, 2026-10-25.** In the loop, so backoff,
  `retrying`, and the counters stay in one place.
- **HTTP/2: decided by the maintainer, 2026-10-25.** Enabled, in its own PR, with the two
  trade-offs recorded.

## Findings

Recorded per workstream as each lands.

### W5: soak runs (2026-10-25)

Each scenario ran once on the development host, `script/soak run`, and every row of each final
run reads PASS or SKIP. `http-concurrency`, `http-concurrency-1`, and `otlp-fan-out-conc` ran on
an image built from `conc/w3`, which speaks HTTP/1.1 over plaintext either way;
`otlp-concurrency-tls` and `standin-concurrency` ran on one with W4 merged.

**Throughput on a slow link.** The generator offers ten batches a second (500 events) to an
HTTP sink, and the SUT's answers are delayed 200 ms ± 50 ms:

| Run | Sink | Delivered under the delay | `send.duration` mean | `in_flight` max (mean) | Overflowed under the delay |
|---|---|---|---|---|---|
| `http-concurrency-1` | `influxdb_out`, 1 | 5.00 batches/s (250 events/s) | 200 ms | 1 | 999 batches in 205 s |
| `http-concurrency` | `influxdb_out`, 8 | 10.00 batches/s (500 events/s) | 202 ms | 3 (2.0) | 0 |
| `otlp-fan-out-conc` | `otlp_out`, 8 | 10.00 batches/s | 200 ms | 3 (2.0) | 0 |
| `otlp-concurrency-tls` | `otlp_out`, 8, TLS | 9.99 batches/s | 208 ms | 4 (2.1) | 0 |

At 1 the sink moves one batch per round trip, so it delivers half the offered rate and the
50-batch queue overflows at the other half. At 8 two requests in flight on average carry the
whole rate, and the round trip, unchanged, stops being the bound.

**A stopped destination.** While VictoriaMetrics stops for 90 s, the SUT's listener holds or
refuses each request, and all eight slots fill (`in_flight` 8 in `http-concurrency` and
`otlp-concurrency-tls`). Concurrency doesn't help when nothing is answered: `http-concurrency`
overflowed 518 batches around the stop and `http-concurrency-1` 501, each retrying the listener's
busy `503`s (16 and 9) with no sink drop. Each run's ledger balances: G = W = V = 291,900 events
at 8, and 242,850 at 1, with the overflow counted outside G (1,500 batches at 1, 518 at 8).

**`otlp-fan-out` at 8.** The run overflowed 1,346 batches, against 2,033 in the run recorded at
concurrency 1 in [`soak-harness.md`](soak-harness.md)'s "Findings" (W13b). None fell under the
200 ms delay; the rest fell where no request is answered: 329 in the VictoriaMetrics stop, 257
in the SUT's pause, 286 in its stop, 450 in the partition and its recovery, and 24 under 30 %
packet loss.

**One connection under HTTP/2.** Over TLS the listener's `logit.input.connections` read 1 from
start to end, eight requests in flight included: ALPN picked `h2`, and every request shared the
connection. Over plaintext HTTP/1.1, `otlp-fan-out-conc`'s listener read 8 during the
VictoriaMetrics stop and up to 21 after the later faults, since a cold pool dials a connection
for each waiting request and keeps the extras until its 90 s idle timeout. The SUT's file
descriptors rose from 17 to 27 by the end of that run, which is why the scenario's `fd_growth`
is 24.

**Fault classes per slot.** `standin-concurrency` runs `prometheus_out` at 8, `at_most_once`,
under a 1 s delay that keeps about three batches in flight (`in_flight` max 4, mean 2.75), with
each cue spent on one write:

| Cue | Class | What the sink counted |
|---|---|---|
| `401` | `Refused` | 1 retry, 0 drops: the batch held at the head and its resend landed |
| `400` "out of order sample" | `Rejected` | 1 batch dropped, `reason="rejected"`; the batches in flight beside it landed |
| connection reset | `Ambiguous` | 1 batch dropped, `reason="ambiguous_at_most_once"`, 0 retries |

The stand-in recorded 2,365 requests and stored 2,362, and V matched what `aggregate` absorbed,
1,181,300 samples, with only the cued batch rejected.

**Idle keep-alive connections and a slow link.** The first `standin-concurrency` run dropped
13 batches `ambiguous_at_most_once` where one was cued: 12 requests were reset with "connection
closed before message completed". At concurrency above 1 the sink's pool holds spare
connections idle between bursts. A request written on one shortly before the destination's
keep-alive timeout (the stand-in's 5 s) arrives, 1 s later, after the destination closed it.
`reqwest` keeps an idle connection for 90 s, so any destination with a shorter keep-alive
timeout on a link with real delay reads some writes as `Ambiguous`: dropped under
`at_most_once`, resent under `at_least_once`. The scenario now raises the stand-in's idle timeout
to 120 s. The race needs both a destination idle timeout under the client's and a delay
comparable to the gap between them; it isn't a `docs/known-gaps/` entry yet.
