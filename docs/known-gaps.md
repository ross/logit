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
- **`eprintln!` instead of a real diagnostics facility** — statsd input, InfluxDB output, the node
  runtime's per-event script errors and per-batch output errors, the aggregator's kind-conflict
  reports, and the `json` transform's parse-failure reports
  (`docs/adr/0010-json-parsing-into-attributes.md`). Cosmetic while there's one of each component;
  matters once there's more than one running at once and stderr becomes an unattributable mess —
  and a `json` component in front of a high-volume source of malformed lines is a concrete way to
  hit that mess sooner than most, one line per event with no rate limiting.
- **No graceful shutdown for `logit run`** — Ctrl-C falls through to the OS default (immediate
  termination); no installed handler. Partially softened by the aggregate/flush work: a node now
  flushes once when its inbound channel closes *normally* (`crates/logit-pipeline/src/runtime.rs`),
  but nothing today closes it that way on its own (every listener loops forever), so in practice
  this doesn't yet cover the Ctrl-C case — the real gap is specifically "no installed signal
  handler," not "no drain logic at all."
- **Fan-out/fan-in is unbuffered/uncoordinated** — the component graph (ADR 0009,
  [pipeline-graph.md](design/pipeline-graph.md)) makes arbitrary fan-out/fan-in the normal case (a
  sink shared by two branches, one listener feeding several filters), but a stalled sink backs up
  every branch sharing an upstream with it, not just its own, and each extra consumer of a node
  costs a full `EventBatch` clone. `Arc<EventBatch>` with copy-on-write is the identified future
  fix; a per-edge `on_full: block | drop` policy for the backpressure question is an open one, not
  yet designed.
- **A Lua component's `flush()` has no resource of its own at a timer tick** — unlike an `aggregate`
  component, which tracks its own per-resource windows, a Lua component's flushed events are
  stamped with whichever resource it most recently saw on a real batch
  (`crates/logit-pipeline/src/runtime.rs`, see [ADR 0008](adr/0008-aggregation-window-semantics.md)).
  Fine for every config today (one listener, one resource); would need a real answer once a
  component has more than one upstream resource.
- **A criterion benchmark of the event proxy against plain table conversion is still outstanding**
  ([lua-api.md](design/lua-api.md)) — the design commits to the proxy on reasoning (avoiding a full
  table conversion per stage per event), not yet confirmed with numbers.
- **`!env` is invisible to `schema/logit.schema.json`** ([ADR 0011](adr/0011-env-yaml-tag.md)) —
  resolution happens on the parsed YAML tree before serde ever sees it
  (`crates/logit-cli/src/config.rs`), so the schema describes the substituted shape, never the tag
  itself. A schema-aware YAML editor will flag a `!env`-tagged value it can't resolve against the
  schema.
- **Config deserialization errors lose line/column information** once `!env` is in the picture
  (`crates/logit-cli/src/config.rs`) — resolving the tag requires parsing to
  `serde_norway::Value` first and deserializing from that, and `serde_norway::from_value` carries
  no source location the way `serde_norway::from_str` does directly on the raw file. Partly offset
  by `!env`'s own errors naming a config path (`components.influx_out.token`) and by the note
  appended when a substitution's resolved type likely caused the failure.
- **`graph::is_implemented`'s error Debug-prints a whole `ComponentKind`**
  (`"kind {:?} is not implemented yet"`, `crates/logit-pipeline/src/graph.rs`) — harmless today,
  since no *unimplemented* kind carries a secret field, but with `!env` now used to inline secrets
  directly into fields (ADR 0011) rather than referencing them by name, this becomes a real leak
  the moment an unimplemented kind gains one. Fix before that happens: redact or field-list instead
  of a blanket `{:?}`.
