---
created: 2026-11-03
updated: 2026-11-03
---

# `otlp_in` answers busy: a request's first batch waits at most 2 seconds, then `503` or `UNAVAILABLE`

## Status
Accepted

## Context
`otlp_in` sent every request's batches through `crate::http::deliver_detached`, which waits for
as long as the pipeline takes. Under backpressure a request parked past its client's timeout:
`otlp_out` gives up at 10 seconds, the Collector's `otlphttp` and `otlp` exporters at 5. The
client then resent the request, and the detached task delivered the original anyway, so every
timeout under a full pipeline produced one duplicate. Each parked request also held its body in
memory and, over HTTP/2, one of the connection's streams.

[ADR `datadog-agent-and-intake-relay`](datadog-agent-and-intake-relay.md) decision 5 gave
`datadog_in` and `datadog_trace_in` a bounded wait and a busy `503`, and rejected blocking for
them because an Agent gives up and retries. Its rejected alternative described `otlp_in`'s and
`prometheus_in`'s clients as ones that "wait for as long as the pipeline takes". That premise is
false for OTLP: every exporter has a request timeout and retries a timed-out request, as the
Agent does. `splunk_hec_in` and `influxdb_in` answer busy for the same reason
([ADR `splunk-hec-relay`](splunk-hec-relay.md),
[ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)).

The OTLP specification makes `503` over HTTP and `UNAVAILABLE` over gRPC retryable, and says a
client SHOULD retry them with backoff
([OTLP/HTTP response](https://opentelemetry.io/docs/specs/otlp/#failures-1),
[OTLP/gRPC failures](https://opentelemetry.io/docs/specs/otlp/#failures)).

## Decision
`otlp_in` follows `splunk_hec_in`'s rule, through the helper both now share,
`crate::http::deliver_first_bounded`:

1. **The first batch of a request waits under a deadline.** It goes through
   `Fanout::send_with_deadline`, reaching every consumer or none, with a deadline of
   `BUSY_AFTER`, 2 seconds, from the start of delivery.
2. **A deadline that passes answers busy, with nothing taken.** HTTP answers `503` with
   `Retry-After: 1` and the body `busy`; gRPC answers status 14 (`UNAVAILABLE`) with the message
   `busy` and no `RetryInfo`. Every batch of the request is counted
   `logit.input.batches.deferred`, since the client still holds the body, and a throttled `warn`
   is logged under the diagnostic key `busy`.
3. **Once the first batch is in, the rest wait without a deadline.** A busy answer after a batch
   was taken would make the client's retry deliver that batch twice, so the remaining batches go
   through `deliver_detached` as before, and the request is answered `200` (gRPC `OK`).
4. **A closed consumer is answered as before.** `503` with `Retry-After: 1` (gRPC
   `UNAVAILABLE`) and `logit.input.batches.dropped{reason="closed_consumer"}` for the refused
   batch and every batch after it, whether the first batch or a later one was refused.
5. **The listener counts its requests.** `logit.input.requests{route, class}`, one per request
   on every exit, as its siblings count: `route` is `logs`, `metrics`, or `traces`, or `unknown`
   for a path or method it doesn't serve, and `class` is `ok`, `rejected`, `busy`, or
   `closed_consumer`.
6. **The bound is a constant, not a config field.** `OtlpInput::with_busy_after` overrides it for
   tests, as on the siblings. Nothing in the schema changes.

2 seconds is under the Collector exporters' 5-second default timeout, so a Collector reads the
`503` before it times out and resends. It's `datadog_trace_in`'s bound, and long enough for a
pipeline that is draining, not stalled, to take a batch.

`prometheus_in`'s remote-write receiver has the same gap and stays blocking under this record:
[`known-gaps/prometheus.md`](../known-gaps/prometheus.md) tracks it.

## Alternatives considered
- **Keep blocking.** No duplicates when the pipeline frees up before the client's timeout, and
  none of the costs below. Rejected: under a pipeline that stays full past the timeout, each
  request is delivered once by the detached task and again by the client's retry, and the parked
  bodies and streams grow with the backlog.
- **Bound every batch, as `datadog_in` does.** Rejected: a request with several resources
  decodes to several batches, and a `503` after the first was taken makes the retry deliver it
  twice. The first-batch rule answers busy only while nothing was taken.
- **A config field for the bound.** Rejected for now: no operator has asked for one, and the
  siblings' bounds are constants with a test hook.
- **The 5 seconds `datadog_in`, `splunk_hec_in`, and `influxdb_in` use.** Rejected: it equals the
  Collector exporters' timeout, so a Collector would often time out before the `503` arrived and
  resend into the same duplicate this record removes.

## Consequences
- **A client that doesn't retry loses what waiting would have delivered.** An exporter with a
  timeout past 2 seconds and no retry, or `otlp_out` under `buffer.delivery: at_most_once`, which
  drops an `Ambiguous` `503`, gets a busy answer it drops where it used to get a slow `200`. The
  OTLP specification says a client SHOULD retry both answers, and every exporter's default does.
- **A multi-resource request can still duplicate.** Once its first batch is taken, the rest wait
  without a bound, so a client whose timeout passes during that wait resends the request, first
  batch included. Never on a `logit` hop: `otlp_out` sends one resource per request.
- **Upstream overflow doesn't go away; the duplicates do.** A busy answer moves the backlog into
  the client's retry queue, which drops when it fills, as it would on a timeout. What changes is
  that each retried request is delivered once.
- **`otlp_in` now counts `logit.input.requests` in its handler.** A multi-batch request whose
  client closes during the detached wait counts its batches sent and no class, as
  `splunk_hec_in` does ([`known-gaps/intake.md`](../known-gaps/intake.md)).
- A test that parks a request on a full pipeline sets a long bound through `with_busy_after`, or
  sends a two-resource request whose second batch parks.
