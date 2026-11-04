---
created: 2026-11-03
updated: 2026-11-04
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

`prometheus_in`'s remote-write receiver had the same gap; the amendment "`prometheus_in`'s
remote-write receiver answers busy too" below closes it.

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

## Amendment: `prometheus_in`'s remote-write receiver answers busy too (2026-11-04)

The receiver sent a write's batch through `deliver_detached` and answered `204` once it was in,
however long that took. Every remote-write sender times a write out and resends it: Prometheus's
`remote_timeout` and Grafana Alloy's default to 30 seconds, vmagent's `-remoteWrite.sendTimeout`
to 1 minute, and `prometheus_out`'s `timeout:` to 10 seconds. A test that parked a write on a full
consumer saw no answer after 11 seconds, and once the client closed, the detached task still
delivered the write, so the resend was a duplicate, as `otlp_in`'s were.

The receiver now follows this record through `deliver_first_bounded`, with what differs:

1. **A write is one batch, so the bound covers all of it.** The receiver decodes a write to one
   batch, so a write is delivered whole or answered busy whole, and no write waits without a
   bound.
2. **The busy answer is `503`, not `429`.** Both remote-write specifications say a sender MUST
   retry a `5xx` and MAY retry a `429`
   ([1.0](https://prometheus.io/docs/specs/prw/remote_write_spec/),
   [2.0](https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/)). Prometheus's
   `storage/remote/client.go` (`Client.Store`) makes a `5xx` recoverable always and a `429` only
   under `queue_config.retry_on_http_429`, off by default, so a default Prometheus drops a batch
   answered `429`. vmagent's `app/vmagent/remotewrite/client.go` retries every status but `400`,
   `409`, and `415`. Grafana Alloy's `prometheus.remote_write` documents a 30-second
   `remote_timeout` and `retry_on_http_429` on by default. Prometheus (`retryAfterDuration`) and
   vmagent read `Retry-After` on any answer they retry, so the busy answer carries
   `Retry-After: 1`. `prometheus_out` reads any `5xx` as `Ambiguous` under both
   `version: 1` and `version: 2`: its `Samples-Written` check applies only to a `2xx`.
3. **A 2.0 busy answer carries the `-Written` zeros.** The 2.0 specification says a receiver MUST
   send the `X-Prometheus-Remote-Write-*-Written` headers once a write completes "with success or
   failure", so the receiver sends them on every answer to a 2.0 write, `503`s included, as it
   already did on its closed-consumer `503`. Zeros say nothing was written, which is true.
   Prometheus's sender decides a retry on the status alone and reads the headers only for its
   stats.
4. **The receiver keeps its own counter.** A busy write counts `logit.input.writes{class="busy"}`
   beside the existing classes, rather than moving to `otlp_in`'s `logit.input.requests{route,
   class}`: the receiver has one route, and renaming would break every dashboard and the soak
   harness's intake row for no new information. The batch counts `logit.input.batches.deferred`,
   and a throttled `busy` line is logged. An `ok` write is counted the moment its batch is in
   every inbox, before the `204` is written.
5. **The bound is the same 2 seconds, a constant.** It's under every sender's default timeout,
   and `PrometheusReceiver::with_busy_after` overrides it for tests.

Consequences, beside this record's:

- **A sender that closes during the bounded wait delivers nothing**, where the detached delivery
  used to send the write anyway. Its resend is the one copy. The write counts no class, as on
  `otlp_in`.
- **A shutdown backstop that aborts a waiting write's connection drops the write unsent.** It was
  delivered by the detached task before. The sender wasn't answered and resends it.
- **A closed-consumer `503` now carries `Retry-After: 1`** too, as `otlp_in`'s does.
- **A Prometheus with `retry_on_http_429: false` is unaffected by the choice of status**, and one
  under `buffer.delivery: at_most_once` on `prometheus_out` drops a busy `503` as it drops any
  `Ambiguous` answer.
