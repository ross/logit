---
created: 2026-09-11
updated: 2026-09-11
---

# Enabling plan: Prometheus scrape ingestion and exposition

## Context

[ADR `prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md) decides the
shape of `prometheus_in` (scrape) and `prometheus_out` (exposition): transports, dialect
negotiation, the model mapping in both directions, attribute conventions, name sanitization, why
delta temporality is resolved by `aggregate` rather than the sink, the always-on synthetic scrape
metrics, exposition state and expiry, the new `Output::bind` hook, and the seam left for a future
non-breaking remote-write addition. This plan is the concrete build-out of that decision: what
lands in which order, in which files, and how each piece is verified.

## Decisions already settled

Settled with Ross (2026-09-11), recorded in full in the ADR:

- **Scrape + exposition now.** `prometheus_in` pulls `/metrics` from configured targets on an
  interval; `prometheus_out` serves a `/metrics` endpoint. Remote-write (receive and send) must be
  a **non-breaking future addition** to the same two kinds.
- **Both text flavors, negotiated on `Accept`**: Prometheus text 0.0.4 and OpenMetrics 1.0 are
  parsed by `prometheus_in` (dialect from the response `Content-Type`) and served by
  `prometheus_out` (OpenMetrics when the scraper asks, text 0.0.4 otherwise).
- **Delta temporality is not resolved in the sink.** `prometheus_out` skips and counts a delta
  `Sum`/`Histogram`; `aggregate` gains a `temporality: cumulative` mode (an amendment to
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)) so
  `statsd_in -> aggregate -> prometheus_out` and `internal -> aggregate -> prometheus_out` work
  with an explicit, named summarizing stage.
- **`prometheus_in` synthesizes `up`, `scrape_duration_seconds`, `scrape_samples_scraped`** per
  scrape, exactly as Prometheus does. Always on; droppable downstream (Lua).

## Design

### 1. Codec: `logit-proto`'s `prometheus` module

A new module beside [`crates/logit-proto/src/otlp/mod.rs`](../../crates/logit-proto/src/otlp/mod.rs),
structured so a future remote-write addition can reuse the semantic mapping without touching the
text syntax:

- `mod.rs` — the module doc *is* the mapping table, following house convention (see `otlp/mod.rs`'s
  own doc header). Holds the syntax-independent intermediate type, `MetricFamily { name, kind:
  FamilyType, help, unit, series: Vec<Series> }`, `Series { labels, point: Point, timestamp,
  created, exemplars }`, `Point::{Counter | Gauge | Histogram | Summary | Info | StateSet |
  Unknown}`, plus the two conversions `families_to_events(&[MetricFamily], received_at, resource)
  -> Vec<Event>` and `events_to_families(&[(&Resource, &Event)]) -> Vec<MetricFamily>` (model ↔
  families) — the full field-by-field mapping is in the ADR's "Model mapping" tables.
  `events_to_families` merges each pair internally via `logit_core::attrs::merged` (moved there
  from `logit-outputs`, pub instead of `pub(crate)`, re-exported from its old path — see the ADR's
  "Where the resource⊕event label merge lives" note) rather than requiring the caller to pre-merge,
  since `logit-proto` cannot depend on `logit-outputs` for it. A future `remote_write.rs` maps
  prompb messages to and from this same `MetricFamily`, so nothing in the model mapping changes
  when that lands.
- `text.rs` — parser and writer for both dialects (`enum Dialect { Text0_0_4, OpenMetrics1_0 }`):
  `parse(bytes, dialect) -> Result<Vec<MetricFamily>, CodecError>`, `write(&[MetricFamily],
  dialect, &mut Vec<u8>)`. It has to get the dialect differences exactly right: OpenMetrics
  timestamps and `_created` are float seconds, text 0.0.4 timestamps are integer milliseconds
  (verified against both specs — see the telemetry-landscape survey below); OpenMetrics requires a
  trailing `# EOF`, contiguous families, `_total` on counter samples, and `# UNIT`; OpenMetrics has
  exemplars and the `info`/`stateset`/`gaugehistogram`/`unknown` types that text 0.0.4 lacks; text
  0.0.4 has `untyped`, which OpenMetrics lacks. Escaping follows each dialect's own grammar for
  `HELP` text and label values; special floats (`+Inf`/`-Inf`/`NaN`) render and parse in both
  directions (Rust's own `f64` formatter writes lowercase `inf`, which needs a special case).
  Timestamps parse through `logit_core::time::parse_decimal_nanos` (digit-exact) for the plain
  `DIGIT+[.DIGIT*]` form every writer emits; OpenMetrics's own `realnumber` grammar additionally
  permits a leading sign and an exponent (and negative timestamps are legal), which
  `parse_decimal_nanos` rejects by design (it has no sign or exponent handling) — a timestamp using
  either falls back to ordinary `f64` parsing instead of failing the whole scrape, accepting the
  small rounding a signed/exponent form was never guaranteed to avoid in the first place.
- No `SignalDecoder`/`SignalEncoder` implementation. Like `syslog_out`/`statsd_out`
  ([ADR `statsd-output`](../adr/statsd-output.md)'s "No `logit_proto::Encoder`" section), both
  `prometheus_in` and `prometheus_out` are stateful in a way a stateless per-batch trait can't
  express — the codec exposes plain functions plus a `PrometheusDecoder`/`PrometheusEncoder` pair
  with `with_telemetry`/`with_diagnostics` builders, mirroring
  [`crates/logit-outputs/src/statsd.rs`](../../crates/logit-outputs/src/statsd.rs).

The full decode and encode mapping tables, the well-known attributes (an unprefixed `instance` plus
two consumed `prometheus.*` attributes), the name/label sanitization rule, and the list of
permitted normalizations for this pair are in the ADR and are not repeated here — this plan only
orders the work that implements them.

### 2. `prometheus_in`

An interval-driven input, modeled on [`crates/logit-inputs/src/internal.rs`](../../crates/logit-inputs/src/internal.rs)'s
ticker shape (no `bind` override needed — it opens no listening socket, only outbound HTTP
requests), built on a `reqwest` client promoted into `logit-inputs`.

- Config (`ComponentKind::PrometheusIn`, tag `prometheus_in`): `targets` (required, non-empty,
  absolute `http(s)` URLs), `interval` (default-driven, humantime-serialized, same shape every
  other interval-driven component uses), `timeout` (per request, default 10s), `headers` (same
  validation `otlp_out` already applies to header maps), `tls` (a client `TlsClientConfig` —
  CA/cert/key/insecure-skip-verify — built directly with `reqwest`'s own TLS configuration methods,
  no rustls code added to `logit-inputs` itself). The future remote-write receiver is an optional
  `bind:` field on the same variant, additive and mutually exclusive with `targets` by graph rule —
  see the ADR's "Remote-write forward compatibility" section.
- Each tick scrapes every configured target concurrently, sending an `Accept` header that lists
  both dialects (OpenMetrics preferred, text 0.0.4 as fallback, matching Prometheus's own scraper),
  capping response size, and choosing the parse dialect from the response's `Content-Type`. One
  `EventBatch` per target per tick goes out, carrying a per-target resource (an unprefixed
  `instance` — `host:port` — and `prometheus.target`, the full scrape URL) built once when the
  input starts; `instance` is a plain resource attribute, not `prometheus`-namespaced, since
  `prometheus_out` renders every resource attribute as a label and this one needs no special-casing
  to reach the exposed series (see the ADR's "Attribute conventions"). A failed scrape still emits
  a batch — with `up=0` and the two scrape-stat gauges the ADR names — rather than emitting
  nothing, so a target going down is itself observable through the pipeline like anything else.
- Telemetry counters classify each scrape outcome (`&'static str` tags only, per this repo's
  cardinality convention — see the ADR's "Synthetic scrape metrics" section for why the target
  itself can never be a tag value) and record scrape duration and sample counts; diagnostics report
  a failing target through `warn_throttled`.
- Graph wiring: a `role` of Listener, `kind_name`, `is_implemented`, and an `interval()` arm in
  [`crates/logit-pipeline/src/graph.rs`](../../crates/logit-pipeline/src/graph.rs); a new
  validation rule requiring non-empty, absolute `http`/`https` targets and a positive timeout; a
  registry arm in [`crates/logit-cli/src/pipeline.rs`](../../crates/logit-cli/src/pipeline.rs)'s
  `build_spec`, mirroring the existing `OtlpIn` arm.
- Unit tests drive a tick against a canned local HTTP server serving fixture bodies under both
  content types, plus failure/timeout/oversize cases, asserting on the emitted batches, the
  per-target resource attributes, and the synthetic metrics.

### 3. `prometheus_out`

A stateful exposition sink: `send` upserts into an in-memory registry; a small HTTP server renders
the registry on demand when scraped.

- **The new `Output::bind` hook.** `prometheus_out` is the first sink that listens rather than
  only connecting outward, so it needs the same pre-spawn bind guarantee
  [`crates/logit-pipeline/src/input.rs`](../../crates/logit-pipeline/src/input.rs)'s `Input::bind`
  already gives every listener: `logit_pipeline::Output` gains `async fn bind(&mut self) ->
  anyhow::Result<()>` with a default no-op body, called by the runtime's pre-pass
  ([`crates/logit-pipeline/src/runtime.rs`](../../crates/logit-pipeline/src/runtime.rs)'s existing
  input-only bind loop, extended to `NodeSpec::Output` too) in sorted id order before any node task
  spawns, with [`crates/logit-pipeline/src/readiness.rs`](../../crates/logit-pipeline/src/readiness.rs)'s
  `NodeState::Bound` bookkeeping following the same path. This is what turns "the configured
  address is already in use" into a startup failure (exit code 1, nothing else running yet) rather
  than a runtime failure discovered only when the first scrape request arrives and gets nothing
  back. [`docs/design/pipeline-graph.md`](../design/pipeline-graph.md)'s lifecycle section gets a
  matching update once this lands.
- Config (`ComponentKind::PrometheusOut`, tag `prometheus_out`): `bind` (required listen address),
  `path` (default `/metrics`), `expire_after` (default 5 minutes — Prometheus's own staleness
  horizon; `0s` disables expiry), `max_series` (a hard cap, default 100000, least-recently-updated
  evicted first — the same shape
  [`crates/logit-transforms/src/aggregate.rs`](../../crates/logit-transforms/src/aggregate.rs)'s
  gauge-retention cap already uses). `buffer:` composes unchanged, as a sibling of `kind`. The
  future remote-write sender is an optional `endpoint:` field, additive and mutually exclusive with
  `bind` by graph rule. No TLS and no auth in v1 — the sink serves its whole registry to any client
  that connects to `bind:` with no credential check, so the runnable example for this pair binds
  `127.0.0.1` rather than `0.0.0.0` (see the ADR's "Security posture" note); tracked as a known gap
  alongside `admin:`'s own no-TLS/no-auth entry.
- State: an `Arc<Mutex<Registry>>` of families keyed by name, each holding series keyed by their
  sorted label set. `send` converts the batch through `events_to_families` and replaces each
  series — cumulative semantics, latest write wins, exactly the ADR's "Exposition state and
  expiry" section describes, including the type-conflict-replaces-and-evicts rule and why `send`
  never touches the network and is therefore `duplicate_safe`.
- The server itself is bound in `bind`, with the accept loop spawned there over a clone of the
  registry handle — the same connection-handling shape
  [`crates/logit-cli/src/admin.rs`](../../crates/logit-cli/src/admin.rs)'s readiness server already
  uses (a semaphore bounding concurrent connections, a per-connection timeout, HTTP/1.1 framing).
  The handler negotiates dialect from `Accept` (OpenMetrics when asked for, text 0.0.4 otherwise),
  supports gzip response compression when the client advertises it, and renders under the registry
  lock into a byte buffer (families sorted by name, series by label key — the same normalization
  the round-trip fixed-point test checks against) before releasing the lock and responding. An
  expiry sweep runs both inline in `send` and in the handler.
- Telemetry counts scrape outcomes by class, response bytes, current series count (a gauge sampled
  after each `send`), and evictions by reason, alongside the codec's own degraded/skipped/dropped
  counters.
- Graph wiring: `role` of Sink, `kind_name`, `is_implemented`, a validation rule that `path` starts
  with `/` and `max_series` is positive, and a `build_spec` registry arm mirroring the pattern
  every other sink follows.
- Unit tests hit the bound server with an HTTP client for both `Accept` flavors (byte-exact against
  expected bodies), expiry, cardinality eviction, the delta-skip-and-count path, type conflict, and
  the `HEAD`/404/405 cases.

### 4. `aggregate` amendment: `temporality: cumulative`

`ComponentKind::Aggregate` and
[`crates/logit-transforms/src/aggregate.rs`](../../crates/logit-transforms/src/aggregate.rs) gain a
`temporality: delta | cumulative` field, defaulting to `delta` (today's behavior, unchanged). In
`cumulative` mode, a delta `Sum` or delta `Histogram` accumulator survives past flush — the same
way a retained gauge already does — and keeps summing every subsequent increment (bucket adds,
`sum` adds, `min`/`max` folds) rather than resetting to empty. Every flush then emits
`Sum{Cumulative}`/`Histogram{Cumulative}`, with `start_timestamp` pinned to the series' first-seen
time in nanoseconds — the restart-detection signal Prometheus and OTLP both expect from a
cumulative counter's `start_timestamp`/`_created`.

This state is bounded by generalizing the same two mechanisms that already bound retained-gauge
state (a windows-count TTL and a maximum retained-series count — `gauge_retention` is a `u32`
count of consecutive idle flush windows, not a duration, per
[ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md); the rename below
keeps that type) rather than inventing a second bounding scheme for this one mode — renamed from
their gauge-specific names to `series_retention`/`max_retained_series` now that they bound more
than gauges, with `demo/`/`examples/` and the existing config tests in
[`crates/logit-config/src/lib.rs`](../../crates/logit-config/src/lib.rs) updated for the rename.
Every eviction is counted, distinguishing cardinality pressure from a series aging out under the
windows-count TTL — `logit.transform.series.evicted{reason="cardinality"|"idle"}`, keeping the
existing `idle` reason name `gauge_retention` eviction already uses rather than introducing a
second name for the same TTL concept now that it also applies to cumulative accumulators.
Reconciling two cumulative accumulations of the same series whose histogram bucket boundaries
don't match between one flush and the next (an upstream exporter changed its bucket layout
mid-stream) is a distinct failure from either eviction path — counted
`logit.transform.metrics.degraded{reason="histogram_bounds_mismatch"}`, keeping the newer
accumulation's boundaries and discarding the mismatched buckets from the older one, rather than
silently merging counts that don't line up.

`series_retention: 0` means "drained every window," exactly like `gauge_retention: 0` does today —
which is a contradiction under `temporality: cumulative`: the mode is defined as the accumulator
surviving flush, and a retention of zero windows would silently degrade every flush to emitting a
per-window delta mislabeled `Cumulative`, with nothing in config or at runtime saying so. A graph
rule closes this: `temporality: cumulative` requires both `series_retention >= 1` and
`max_retained_series >= 1` (a cap of zero would evict every series the instant it's retained,
which is the same contradiction from the other knob) — numbered rule 39 in
[`crates/logit-pipeline/src/graph.rs`](../../crates/logit-pipeline/src/graph.rs)'s validation list
as part of the W4 workstream.

This amends [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md): its
original objection to cumulative counters was that there was no way to detect a series restarting
from the stream alone — `start_timestamp` (already landed as part of the metrics-model-v2 work) is
precisely that signal, and the bounded-retention machinery the gauge amendment already built
answers the other half of the original objection, unbounded per-series state.

### 5. Remote-write forward compatibility (design only, no code this round)

Recorded in the ADR, not built here: (a) the semantic mapping already lives behind `MetricFamily`
in `logit_proto::prometheus::{mod, text}`, so a future `remote_write.rs` maps prompb messages to
and from that same type without touching anything the mapping tables describe; (b) `prometheus_in`
gains an optional `bind:` field (a remote-write receiver), `prometheus_out` gains an optional
`endpoint:` field (a remote-write sender), each mutually exclusive with today's field by graph
rule — additive to existing config, not a new kind; (c) the vendored prompb types would live under
`crates/logit-proto/proto/prometheus/`, regenerated by
[`tools/protogen`](../../tools/protogen) the same way the OTLP protos already are
([ADR `committed-pregenerated-otlp-protobuf`](../adr/committed-pregenerated-otlp-protobuf.md)), and
`snap` (BSD-3-Clause) is already an allowed license in [`deny.toml`](../../deny.toml); (d)
`prometheus.*` attributes and the name/label sanitization rules are dialect- and
transport-independent, so a remote-write relay is a fixed point on the same terms as the
text-format one.

## Workstreams

| # | PR | Depends on |
|---|---|---|
| W0 | **Docs:** this plan; ADR [`prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md); amendments to [`internal-telemetry-as-pipeline-events`](../adr/internal-telemetry-as-pipeline-events.md) and [`lossless-transit`](../adr/lossless-transit.md); index rows in both `docs/adr/README.md` and `docs/plans/README.md`. | — |
| W1 | **Codec:** `logit-proto/src/prometheus/{mod,text}.rs`, both dialects, decode+encode, the shared distribution-quantile constant, telemetry counters; inline tests drawn from the exposition-format docs' and the OpenMetrics spec's own examples; a codec-level fixed-point test (`families -> events -> families`, and byte-level `write(parse(x)) == canonical(x)` over a fixture set plus a property-based grammar generator); `docs/design/data-model.md`'s well-known-attribute rows; `docs/known-gaps.md` cross-protocol rows. | W0 |
| W2 | **`prometheus_in`:** input, config variant, graph rules, registry arm, the `reqwest` dependency, unit tests against a canned server, a runnable example config, regenerated schema, `docs/design/pipeline-graph.md`'s arity table and rules. | W1 |
| W3 | **`prometheus_out` + `Output::bind`:** the runtime hook, the sink, config, graph rules, registry arm, unit tests, regenerated schema, `docs/design/pipeline-graph.md`'s lifecycle note. | W1 (parallel with W2) |
| W4 | **`aggregate` cumulative mode:** config, accumulators, the retention-mechanism rename and generalization, the ADR amendment, tests. | — (parallel with W1–W3 by dependency ordering, not file disjointness — W2/W3/W4 all edit `ComponentKind` in `crates/logit-config/src/lib.rs` and regenerate `schema/logit.schema.json`; merges resolve the overlap) |
| W5 | **Integration and closeout:** an end-to-end test — a canned server through `prometheus_in` through `prometheus_out` to a scrape request, byte-exact in both dialects against fixtures; a `statsd_in -> aggregate(cumulative) -> prometheus_out` case; an `internal -> aggregate(cumulative) -> prometheus_out` case; allocation-count benchmark cases with `docs/design/memory.md` rows; a runnable relay example config; `AGENTS.md`'s current-state paragraph; `docs/known-gaps.md` follow-ups (remote-write, UTF-8 names, TLS on `prometheus_out`, native histograms, histogram `min`/`max`, `otel_scope_*` labels). | W2, W3, W4 |

Landing order: W0 → (W1, W4) → (W2, W3) → W5.

## Verification

- `script/check` (format check, clippy with warnings denied, and the workspace test suite) and
  `script/cibuild` pass for every workstream's PR; `script/schema` regenerated and committed for
  any workstream that changes a config type; `script/validate` passes over `demo/` and `examples/`.
- W1: the codec's fixed-point tests pass for every fixture drawn from the exposition-format docs
  and the OpenMetrics spec's own examples.
- W2/W3: the unit tests described above pass; `logit validate` accepts the new example configs.
- W5: the end-to-end round-trip test is byte-exact in both dialects modulo the permitted
  normalizations in the ADR (including the three synthetic families excluded from the comparison
  by name); `type_sizes.rs` and
  `allocations.rs`'s exact-equality assertions either hold unchanged or are updated together with
  `docs/design/memory.md` in the same commit, per `AGENTS.md`'s own rule for those two files; a
  manual smoke test runs the relay example against a canned `/metrics` file served over plain HTTP
  and confirms both dialects render correctly from a scrape request.
- This PR (W0) is documentation only: no code changes, so `script/cibuild` is not applicable;
  verification here is that every relative link in the touched files resolves, ADR headings match
  `docs/adr/TEMPLATE.md` exactly, and both README indexes gained a row.
