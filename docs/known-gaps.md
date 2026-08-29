# Known gaps

Deliberate, already-identified gaps that don't block whatever they were found alongside, tracked
here so they don't only live in an untracked scratch file or someone's memory. Each one either has
a `todo!()`/doc-comment pointer at its actual location too, or is cheap enough to describe fully
here. Not a roadmap — see [OVERVIEW.md](OVERVIEW.md) for planned scope; this is specifically things
already built that have a known, accepted rough edge.

- **`HyperLogLog` is a stub** (`crates/logit-core/src/metric.rs`) — no methods, just a placeholder
  pending a real crate (`cardinality-estimator` is the candidate). Consequences: statsd's `s` (set)
  metric type is a clear decode error rather than silently losing data
  (`crates/logit-inputs/src/statsd.rs`); `logit-transforms::Aggregator` passes `MetricKind::Set`
  through unaggregated rather than fake-merging it
  ([ADR 0008](adr/0008-aggregation-window-semantics.md)); `logit-outputs::influxdb` errors on it
  rather than writing a wrong encoding.
- **Native wire protocol** (`crates/logit-proto/src/frame.rs`) — the frame header type exists; no
  actual encode/decode, no connection/handshake, no dictionary encoding. The `rkyv`-vs-hand-rolled
  encoding choice is an explicit open, benchmark-gated decision
  ([wire-protocol.md](design/wire-protocol.md)).
- **Output buffering** (`crates/logit-proto/src/buffer.rs`) — the `Buffer` trait exists (deliberately
  defined ahead of any implementation, since retrofitting it onto call sites that assumed an
  in-memory queue is the expensive direction); no in-memory implementation yet, no ack/retry hooks.
  This is also where real backoff for a transient output failure belongs — today a persistent output
  failure just ends `logit run` (see `crates/logit-cli/src/pipeline.rs`'s output task), which is the
  right tradeoff *without* buffering (silently swallowing is worse) but not the final answer.
- **Relative gauge adjustment (`+`/`-`) and sample-rate extrapolation for distributions** — the
  aggregator that would hold the needed state now exists (`crates/logit-transforms`), but the statsd
  decoder still has no representation for "this is a delta, not an absolute value" to hand it, and
  any leading sign is ambiguous with a plain negative number at the wire level regardless. Relative
  gauges are explicitly rejected with a clear decode error rather than silently miscoded
  (`crates/logit-inputs/src/statsd.rs`).
- **`eprintln!` instead of a real diagnostics facility** — statsd input, InfluxDB output, pipeline
  orchestration's per-event script errors and per-batch output errors, and the aggregator's
  kind-conflict reports. Cosmetic while there's one input/output/pipeline; matters once there's more
  than one running at once and stderr becomes an unattributable mess.
- **No graceful shutdown for `logit run`** — Ctrl-C falls through to the OS default (immediate
  termination); no installed handler. Partially softened by the aggregate/flush work: a pipeline
  worker now flushes every flush-bearing stage once when its inbound channel closes *normally*
  (`crates/logit-cli/src/pipeline.rs`), but nothing today closes it that way on its own (every input
  loops forever), so in practice this doesn't yet cover the Ctrl-C case — the real gap is
  specifically "no installed signal handler," not "no drain logic at all."
- **A named input/output referenced by more than one pipeline is a config-time error** — real
  fan-out/fan-in support (one input feeding multiple pipelines, or vice versa) is a legitimate future
  need, not yet built (`crates/logit-cli/src/pipeline.rs`'s `validate_semantics`).
- **A Lua stage's `flush()` has no resource of its own at a timer tick** — unlike an `aggregate`
  stage, which tracks its own per-resource windows, a Lua stage's flushed events are stamped with
  whichever resource the worker most recently saw on a real batch (`crates/logit-cli/src/pipeline.rs`,
  see [ADR 0008](adr/0008-aggregation-window-semantics.md)). Fine for every pipeline today (one input,
  one resource); would need a real answer once a pipeline has more than one.
- **A criterion benchmark of the event proxy against plain table conversion is still outstanding**
  ([lua-api.md](design/lua-api.md)) — the design commits to the proxy on reasoning (avoiding a full
  table conversion per stage per event), not yet confirmed with numbers.
