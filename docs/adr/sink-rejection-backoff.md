---
created: 2026-10-04
updated: 2026-10-04
---

# Sink rejection backoff: a sink that only sees rejections holds its queue and probes, and the process never exits for it

## Status
Accepted. Supersedes the `Permanent` rule in [ADR
`buffered-sink-delivery`](buffered-sink-delivery.md)'s "Failure handling" section, that ADR's
rejected "Never exit on a sink failure" alternative, the outage paragraph in [ADR
`service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md)'s "Retry: a tight
wall-clock budget, not an attempt count" section, and the pipeline ending in [ADR
`sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md)'s decision 11.

## Context

[ADR `buffered-sink-delivery`](buffered-sink-delivery.md) gave `write_loop`
(`crates/logit-pipeline/src/runtime.rs`) one way to end `logit run` on a sink failure: when a sink
saw nothing but explicit `Fault::Permanent` failures (a `401`, `403`, or `400`; gRPC
`UNAUTHENTICATED` or `UNIMPLEMENTED`) for a fixed 60-second window with no success, `write_loop`
returned an error and the process exited with code `2`. The reasoning was that a bad token should
fail loudly enough for a restart supervisor to notice.

That trade is wrong for `logit`:

- **One sink takes every other sink with it.** The exit ends every listener and every sibling
  sink in the process. A single destination's configuration error becomes a total loss of
  visibility for everything the node carries.
- **A rejection isn't proof that the fault is on `logit`'s side.** The destination may be
  misconfigured, not configured yet, or temporarily broken. A restart doesn't fix any of those;
  it repeats the same rejections and exits again.
- **The guard has a known false positive.** `docs/known-gaps/otlp.md`'s mixed-signal entry
  describes an `otlp_out` to Tempo that fails every batch with `UNIMPLEMENTED` on its metrics
  request while its traces succeed. The guard ended the process about a minute after startup.

Dropping every rejected batch forever isn't the answer either: data that the sink's queue could
store is lost, and the destination receives a full send for every batch.

[ADR `delivery-semantics`](delivery-semantics.md)'s item 2 permits a batch whose fault is
`Permanent` as a counted loss, and its "Retry ownership" section places retry in the runtime.
This record keeps both: the runtime decides when to stop attempting, and a single rejected batch
is still a counted loss.

## Decision

1. **A sink failure never ends the process.** `write_loop` has no failure path that returns an
   error for a send outcome. There's no option to restore the exit.
2. **The streak rule is unchanged.** Only a failure the sink itself classifies as
   `Fault::Permanent` (`is_explicitly_permanent`, `crates/logit-pipeline/src/output.rs`) extends
   the streak. A success, a `Clean` or `Ambiguous` failure that exhausted `retry_budget`, or an
   unclassified failure resets it. Until the streak is long enough, each rejected batch is dropped
   and counted `batches.dropped{reason="send_failed"}`, as before. One malformed batch (a `413`,
   say) is dropped and can't by itself back a sink off.
3. **After `buffer.backoff_after` of streak, the sink backs off.** `backoff_after` defaults to
   `60s`. The batch whose rejection completes the streak is held instead of dropped: `write_loop`
   doesn't commit it, doesn't count it dropped, and keeps it at the queue head. The batch's span
   still records the error and its `fault` tag.
4. **A backed-off sink holds its whole queue, not only its head.** Nothing more is attempted, and
   nothing is dropped by the sink's writer. Batches that arrive keep entering the sink's queue
   (memory or `buffer.disk:`) under its `overflow` policy.
5. **A backed-off sink probes once per `buffer.backoff_interval`.** `backoff_interval` defaults to
   `60s`. A probe is an ordinary delivery of the held head through `deliver_with_retry` or
   `deliver_window`, retry budget included. It doesn't call `Output::observe_batch` again: the
   batch was observed once, on its first attempt. A probe counts the same telemetry any attempt
   does (`errors`, `retries`, `send.duration`).
   - **Delivered:** the head is committed, the sink leaves backoff, the existing `recovered`
     diagnostic is logged, and the queue drains at normal speed.
   - **`Fault::Permanent` again:** the sink stays backed off and waits another interval.
   - **Any other failure** (a `Clean` or `Ambiguous` failure that exhausted the retry budget, or
     an unclassified one): the streak-reset rule applies. The sink leaves backoff, and the head
     takes the ordinary drop path: committed, counted `send_failed`, and warned.
6. **The backoff wait races the shutdown grace.** If the grace expires while a sink waits for its
   next probe, `write_loop` returns with the head uncommitted. `finish_and_flush` then counts the
   held batches `dropped{reason="shutdown"}` for a memory queue, or leaves them in the spool for
   a disk queue, the same as any batch still queued at the deadline.
7. **Diagnostics.** Entering backoff logs an `error` with the key `backoff`, naming how long the
   destination has rejected every batch, the probe interval, and the time since the last
   successful delivery. Each failed probe logs another `error` with the same key, naming the
   rejection and how many batches are held. The probe interval paces these lines, so they aren't
   throttled. A successful probe logs `recovered` at `info`.
8. **Telemetry.** A new gauge, `logit.component.backoff`, reads `1` while the sink is backed off
   and `0` otherwise. During backoff `batches.dropped{reason="send_failed"}` stops moving, so this
   gauge and `logit.component.buffer.utilization` are the signals to alert on.
9. **Validation.** `backoff_after` and `backoff_interval` must each be greater than `0s`, checked
   by graph rule 15 beside `retry_budget` and `retry_max_delay`. A zero `backoff_after` would back
   a sink off on its first rejection, and a zero `backoff_interval` would probe with no pause.

### Hold, not drop

A backed-off sink holds its head because the head is the probe. Holding means the batch that
proves the destination works again is a real batch the destination is owed, and it's delivered
first, in order. Dropping it and probing with the next batch would lose one batch per interval for
the whole outage and gain nothing.

### What holding costs under the default `overflow: block`

A backed-off sink's queue fills. Under the default `overflow: block`, a full queue parks the sink's
`drain_inbox`, its 64-slot inbox fills, and `Fanout` blocks on it. That backs up into every
sibling sink fed by the same producer, and into the inputs. A destination that fails ambiguously
on every attempt does the same thing today (`docs/deploying.md`, "A destination that fails
ambiguously on every attempt holds the queue head"); backoff makes a rejecting destination behave
like an unreachable one.

An operator chooses the trade per sink:

- `overflow: drop_oldest` (or `drop_newest`) isolates the rest of the pipeline from the backed-off
  sink, and loses the evicted batches, counted.
- `buffer.disk:` rides out a long rejection with no loss, up to `disk.max_bytes`.

A backed-off sink isn't visible on `/readyz` or `/healthz`: neither reports per-sink state
(`docs/known-gaps/runtime.md`, "Readiness is per-process, not per-sink").

## Alternatives considered

- **Keep the exit.** Rejected: one sink's configuration error ends every other sink's delivery,
  a restart repeats the rejection, and the guard already fired on a destination that was
  accepting the signal it existed for (the Tempo entry in `docs/known-gaps/otlp.md`).
- **Drop every rejected batch and keep attempting, with a time-paced error line.** Rejected: data
  the queue could store is lost for the whole outage, and the destination receives a full send for
  every batch while it rejects them.
- **Retry a `Permanent` failure like a `Clean` one, within `retry_budget`.** Rejected: a stream of
  batches the destination rejects for their content (`413`s) holds each one at the head for the
  whole budget, 60 s by default, and fills the queue, where today each is dropped at once.
- **An opt-in `fail_fast` option that restores the exit.** Rejected: `logit` is pre-release, and
  no deployment has shown a need for it. A supervisor that wants to act on a rejecting sink can
  alert on `logit.component.backoff` or the `backoff` error line.

## Consequences

- `crates/logit-pipeline/src/runtime.rs`: `write_loop` loses `PERMANENT_FAILURE_WINDOW` and its
  error return, and gains the backoff state, the probe wait raced against the shutdown grace, the
  held-head path that doesn't re-observe the batch, the gauge, and the `backoff` diagnostics.
  `WriteLoopConfig` carries the two durations, mapped by `logit-cli`'s `write_config`.
- `crates/logit-config`: `BufferConfig` gains `backoff_after` and `backoff_interval`, both
  `60s` by default; `schema/logit.schema.json` is regenerated.
- `crates/logit-pipeline/src/graph.rs`: graph rule 15 rejects a `0s` value for either field.
- Tests: the tests that asserted the exit now assert the hold, the probe, recovery, the
  non-permanent exit from backoff, shutdown accounting for memory and disk queues, and a sibling
  sink delivering while one sink is backed off. Exit code `2` stays covered by a listener or Lua
  failure.
- Docs: `docs/deploying.md` (the exit-code table, the self-logging table, and "Sink failure
  semantics"), `docs/design/internal-telemetry.md` (the gauge), `docs/design/pipeline-graph.md`
  ("Cancellation points"), `docs/known-gaps/otlp.md` (the Tempo entry), `docs/known-gaps/runtime.md`
  (readiness), and the `tempo_out` gate's comment in `demo/logit.yaml`. The superseded ADRs carry a
  pointer here.
- The `has_signal` gate in front of a signal-partial destination stays required. Without it, a
  mixed-signal `otlp_out` to Tempo now backs off and holds its queue instead of ending the
  process.
- An operator who relied on exit code `2` to restart a misconfigured sink must alert on
  `logit.component.backoff` or the `backoff` error line instead.
