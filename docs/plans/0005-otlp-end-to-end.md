# Closing plan: OTLP end-to-end — internal spans, an OTLP codec, `otlp_in`/`otlp_out`, and the demo

## Context

`logit` carries logs, metrics, and traces internally, but before this plan only **metrics** could
leave it for a real backend. `influxdb_out` writes metrics and deliberately ignores an event's log
and span; `stdio_out` renders all three but only to a terminal or a file. That was the output gap.

Two things had to be true before traces could leave `logit`, and neither was true going in:

1. **Nothing produced a span.** The PR series ending at #42 built the *substrate* only — ADR 0020
   put a real `TraceContext` on every `Delivered`, and `Transform::flush` returned `Vec<SpanLink>`
   per emitted event (`Aggregator` populated it from a bounded `ContributingContexts` set). But
   `run_flush` (`crates/logit-pipeline/src/runtime.rs`) threw those links away, and
   `Registry::drain` (`crates/logit-core/src/telemetry.rs`) emitted only `Event::metric`. Every
   `Event::span(...)` call in the tree was in a test or a bench fixture. This was item 1 of the
   still-open list in `docs/known-gaps.md`'s internal-spans entry.
2. **There was no OTLP code at all.** `ComponentKind::OtlpIn { bind }` and `OtlpOut { endpoint }`
   were declared in `logit-config` but rejected by `graph::is_implemented`. ADR 0004 settled that
   OTLP is a first-class interop codec (not the internal transport) and left the gRPC-vs-HTTP
   question open.

`demo/` ([0003-demo-stack.md](0003-demo-stack.md)) was the forcing function: Tempo stood up,
provisioned as a Grafana datasource, with OTLP receivers on `:4317`/`:4318` — and genuinely empty,
with a commented-out `trace_out` stanza in `demo/logit.yaml` naming exactly what had to land first.

The intended outcome, and what this plan closed: `logit` observes itself as *traces* as well as
metrics, exports all three signals over OTLP, ingests all three over OTLP, and the demo stack shows
real traces in Tempo correlated with the existing InfluxDB metrics dashboard.

**Deliberately out of scope:** `syslog_out`, and therefore the demo's Loki/Alloy log leg. That
stays commented out and documented as pending. It was dropped deliberately — OTLP was large enough
to warrant the whole session on its own.

## Decisions already settled

| Question | Decision |
|---|---|
| Span emission default | On, with a deterministic-on-`trace_id` `sample_rate` defaulting to `0.1`. The demo sets `1.0` explicitly. |
| Sampler mechanism | Deterministic on `trace_id` — every node computes the same keep/drop answer independently. No `sampled` flag, so `TraceContext`/`Delivered` do not grow. |
| OTLP signals | All three (logs, metrics, traces), both directions. |
| OTLP transports | Both gRPC and HTTP, selected by a `protocol: grpc \| http` field on `otlp_in`/`otlp_out`, both shipped in one PR (PR3) — the demo deliberately exercises gRPC against Tempo's `:4317`, which removes the benefit a smaller HTTP-first split would otherwise have bought. |
| Protobuf toolchain | Generated once, **committed** — no `protoc` at build time (ADR 0005). |
| Lossy metric kinds | `Distribution`→`Summary` and `Set`→skip, both counted, matching `influxdb_out`'s existing precedent. Filed in `docs/known-gaps.md` as a "Cross-protocol semantic gaps" entry meant to grow, not a one-off footnote. |
| Shape | 3 PRs (spans → OTLP codec → the two components), then a 4th for the demo. |
| Demo scope | `otlp_out` → Tempo, over **gRPC** specifically. `otlp_in` ships tested but unexercised by the demo. |
| Branching | One git worktree per PR, each branched off `main`. PR1 ‖ PR2 in parallel; PR3 off PR2; PR4 last. |

## Shape of the series

```
PR1  feat/internal-span-emission   ──┐
     (logit-core, logit-pipeline,    │
      logit-inputs, logit-config)    │
                                     ├──> PR4  feat/demo-traces
PR2  feat/otlp-codec  ──> PR3  feat/otlp-components
     (logit-proto)          (logit-inputs, logit-outputs,
                             logit-config, logit-pipeline, logit-cli)
```

PR1 and PR2 touched disjoint crates and started in parallel. PR3 branched off PR2's branch rather
than `main`, since it is meaningless without the codec. PR4 waited on all three, merging them
together (`feat/otlp-codec` + `feat/otlp-components` already combined by construction, then
`feat/internal-span-emission` merged in) before touching any demo file.

**Shared-file conflict surface** (PR1 and PR3 both touch these; small, localized conflicts, resolved
by `git merge`, never a rebase — AGENTS.md is explicit): `crates/logit-config/src/lib.rs`
(`ComponentKind`), `crates/logit-pipeline/src/graph.rs` (`is_implemented` + a validation rule
each — `role`/`kind_name` already covered `OtlpIn`/`OtlpOut`, so PR3 didn't touch those),
`crates/logit-cli/src/pipeline.rs` (PR1 edited `prepare`, PR3 edited `build_spec` — different
functions, same file), and `schema/logit.schema.json` (regenerated with `script/schema`, never
hand-merged). In practice, merging PR1 into the combined PR2+PR3 branch produced exactly two
conflicting hunks — `logit-config/src/lib.rs` (both PRs added an item right after `MetricSpec`) and
`logit-cli/src/pipeline.rs` (both PRs added a `match` arm next to `Internal`) — both additive,
resolved by keeping both sides; `graph.rs` merged clean.

## Conventions every PR in this series followed

Non-negotiable, from `AGENTS.md`, restated here because this plan cites them repeatedly:

- **Run `script/*`, never bare `cargo`** — nothing is installed on the host. `script/cibuild` before
  opening a PR; it is byte-for-byte what CI runs.
- **An ADR per real decision**, numbered, in `docs/adr/`, following the existing
  Status/Context/Decision/Alternatives/Consequences shape. This series originally reserved **0022**
  through **0024**; `feat/syslog-out` (#47, an independent PR, not part of this series) landed its
  own real `0022-syslog-output.md` first, so PR1's own ADR (internal span emission) renumbered
  to **0025** when merging into `main` to avoid the collision -- `docs/adr/`'s existing files
  finished at **0023** (`otlp-codec`), **0024** (`otlp-components`'/hand-rolled gRPC), and **0025**
  (internal span emission). `0020` was already used twice before this series started; no third use
  was added.
- **Every config type derives `Serialize + Deserialize + JsonSchema` together** (ADR 0003), and
  `schema/logit.schema.json` is regenerated via `script/schema` and committed in the same change.
  `script/cibuild` fails on drift.
- **Tests are inline `#[cfg(test)] mod tests`** at the bottom of the file under test, with long
  full-sentence names and an assertion message interpolating the actual value.
- **No test or benchmark fixture depends on a running service.**
- **Sinks split into a pure encoder + a thin I/O wrapper**, with format tests run against the
  encoder alone.
- **Retry never lives in a sink** — `send` is one attempt, classifying failure via
  `.context(Fault::{Clean,Ambiguous,Permanent})`; `logit-pipeline`'s `write_loop` owns timing
  (ADR 0021).
- **Doc-comment density matches the surrounding code**, comments use `--`, not em-dashes.
- **Exact-equality size and allocation assertions are the tests working** — never relaxed to `<=`.

## PR1 — `feat/internal-span-emission`

Closed item 1 of `docs/known-gaps.md`'s internal-spans list. Independent of all OTLP work — the
`SpanRecord` it produces is consumed by `stdio_out`, which already rendered spans in full.

**One span is one node's minted `TraceContext`** — not "one node's processing of one batch". The
runtime mints the context unconditionally, exactly once per unit of work, and uses that one context
both as the span's `span_id` and for the send — an additive method on `Fanout`
(`send_with_own_context`/`send_blocking_with_own_context`) rather than a return value, since a node
may record a span without sending anything at all.

| Node | `trace_id` | `span_id` | `parent_span_id` | `SpanKind` | Recorded in | Window measured |
|---|---|---|---|---|---|---|
| Listener | fresh root | that root's | none | `Producer` | `Fanout::send`/`send_blocking` | the `send` call only |
| `Transform::process` | inherited | `parent.child()`, minted in `run_transform` | incoming | `Internal` | `run_transform` | `process_batch` + send |
| Lua `process` | inherited | same, minted in `run_lua` | incoming | `Internal` | `run_lua` | `process` + blocking send |
| `Transform::flush` | fresh root | that root's | none | `Internal` | `run_flush` | `flush()` + every group's send |
| Lua `flush()` | fresh root | that root's | none | `Internal` | `run_lua`'s `flush_now` | `flush()` + send |
| `run_output` | inherited | `ctx.child()`, minted then discarded | incoming | `Client` | `write_loop` | the whole `deliver_with_retry` |

**Two deliberate behavior changes, each recorded as an ADR line:** `run_flush` now mints **one**
root before `transform.flush(now)` and uses `send_with_own_context` for every resource group — one
flush is one unit of work, and N groups is an internal detail of `aggregate`'s per-resource
windowing (ADR 0008), not N hops. And the listener span's window is the `send` call, not decode —
`Fanout::send` genuinely cannot know when the listener began building the batch (`Input::run` is a
free-form loop), so the span says so rather than fabricating a start time.

**The sink span forced `SinkQueue` to carry the context.** The only span that can carry
`SpanStatus::Error` and a fault tag is the one around `deliver_with_retry`, which is the whole point
of instrumenting a sink — so `SinkQueue::push`/`peek` gained a `TraceContext` parameter, riding
inline (`TraceContext` is `Copy`, no new allocation, the queue entry grows 24 bytes). `Output::send`
itself is untouched.

**The emit API and the buffer**, following ADR 0018's shape — spans ride the same `ComponentBuffer`
→ `Registry::drain` → `internal` path as metrics:

```rust
impl Telemetry {
    pub fn span(
        &self, op: &'static str, kind: SpanKind,
        trace_id: [u8; 16], span_id: [u8; 8], parent_span_id: Option<[u8; 8]>,
    ) -> SpanGuard;
}
```

`ComponentBuffer` gained a separate bounded structure for spans (`MAX_SPANS_PER_COMPONENT = 512`, a
volume bound, not a cardinality one — spans never coalesce), and links per span capped at
`MAX_LINKS_PER_SPAN = 32`. Over-cap is counted, never silent:
`logit.internal.spans.dropped{reason="buffer_full"}` on the buffer,
`logit.internal.span.links.dropped{reason="cardinality"}` on the guard.

**The sampler** is deterministic on `trace_id`, the same shape as OTel's `TraceIdRatioBased`: the
top 53 bits of the low 8 `trace_id` bytes (f64's exact-integer range) compared against
`rate * 2^53`, called once inside `Telemetry::span` before any span-shaped state is built. The rate
lives on `Registry` (process-wide, set once) and is copied into each `ComponentBuffer` at
construction. Config, on `ComponentKind::Internal`: `span_sample_rate: f64`, default `0.1`
(`logit_core::DEFAULT_SPAN_SAMPLE_RATE`), validated by graph rule 16 to be finite and in `[0, 1]` —
`trace_is_sampled` treats NaN as "keep everything," which would be a surprising thing to get from a
typo, so it's rejected at validation time instead.

**Confirmed unchanged, as designed:** `size_of::<Delivered>()` stayed exactly 56 — the whole point
of the deterministic-on-`trace_id` sampler is that it needs no propagated bit. Every `SpanGuard` on
a disabled or unsampled handle is `Option::None`, so the allocation-count assertions against
`Telemetry::default()` also held unchanged.

**New residuals recorded in `known-gaps.md`** (deliberate, not oversights): the listener span's
window is the `send` call only, not decode-to-send; Lua `flush()` still gets a link-less root (no
accumulator exists for it, same as the pre-existing stale-context limitation); a `SinkQueue` entry
is 24 bytes larger.

## PR2 — `feat/otlp-codec`: committed OTLP protobuf + the Event↔OTLP mapping

Independent of PR1, entirely inside `crates/logit-proto`; nothing wired into a component yet.

**Getting committed, protoc-free types.** Generated once with `prost-build`, offline, and
committed — not the `opentelemetry-proto` crate (its `Cargo.toml` pulls in `opentelemetry`/
`opentelemetry_sdk` even under minimal features, dragging in the OTel SDK this repo deliberately
deferred, pinning `prost 0.14`/`tonic 0.14`, and its own `schemars` is 1.0 against this workspace's
0.8), not hand-rolled (OTLP metrics' ~30 nested-oneof/packed-varint messages is exactly the class of
thing AGENTS.md's `HyperLogLog` stance argues against doing by hand). Runtime dep: `prost = "0.14"`
only — OTLP's protos use no `google.protobuf` well-known types, so no `prost-types`. Regeneration is
a new, deliberately non-`cibuild` script (`script/protogen`, per ADR 0006) that builds a one-shot
image with `protoc`; **no drift check in `script/cibuild`** — that would put `protoc` in the CI
image, exactly what ADR 0005 forbids. See [ADR 0023](../adr/0023-committed-pregenerated-otlp-protobuf.md).

**`Decoder`/`Encoder` gained a sibling, not a change** — OTLP has three services, and one
`EventBatch` can hold all three signals on one `Event` (ADR 0012), which the existing
single-payload `Decoder`/`Encoder` don't fit and must not change (statsd/syslog/the native format
all still fit them). New, additive traits: `SignalEncoder::encode_signals` (one `EventBatch` → up to
three `(Signal, Bytes)` payloads, empty signals omitted) and `SignalDecoder::decode_signal` (one
OTLP payload → one or more `EventBatch`, since one request can carry N `Resource*` groups and an
`EventBatch` holds exactly one `Arc<Resource>`).

**The mapping**, summarized (the full tables live in `crates/logit-proto/src/otlp/*.rs`'s module
docs, which *are* the authoritative mapping reference): `Value` ↔ `AnyValue` is total except `U64`
above `i64::MAX` (lossy above 2^53, encodes as `DoubleValue`) and `Timestamp` (no OTLP timestamp
type, round-trips as `I64`). `Severity` ↔ `SeverityNumber` encodes to each band's base and decodes
preferring the number, falling back to text. Traces are near-total (`trace_state`/`flags`/
`dropped_*_count` dropped, documented, not errors). Metrics are the hard part:

| `MetricKind` | Encodes to | Fidelity |
|---|---|---|
| `Counter(v)` | `Sum{DELTA, monotonic}` | exact |
| `Gauge(v)` | `Gauge` | exact |
| `Histogram{buckets}` | `Histogram{DELTA}` | exact |
| `Summary{quantiles}` | `Summary` | `count`/`sum` have no source → `0`/`0.0`, documented |
| `Distribution(DdSketch)` | `Summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) | **Lossy, deliberately** — `DDSketch` exposes no bin iteration to convert to OTLP's `ExponentialHistogram`. Counted: `logit.output.metrics.degraded{metric_kind="distribution"}`. |
| `Set(HyperLogLog)` | **skipped** | The stub has no cardinality to read, matching `influxdb.rs`'s existing precedent. Counted: `logit.output.metrics.skipped{metric_kind="set"}`. |

Both rows are a real, if narrow, qualification of [ADR 0004](../adr/0004-native-wire-format-with-otlp-bridge.md)'s
claim that the internal model "must be a superset of what OTLP can express" — here it's `logit`'s
own model (a mergeable sketch, a cardinality stub) that can't be losslessly re-expressed *as* OTLP,
the direction ADR 0004 didn't anticipate. Decode: `Sum` monotonic + `DELTA` → `Counter`; `Sum`
monotonic + `CUMULATIVE` → `Gauge` with an `otel.temporality` attribute (summing a running total
would be wrong); `ExponentialHistogram` → `Histogram{buckets}` with bounds materialized from
`scale`/`offset` — this direction is **exact, not lossy** — capped at `MAX_DERIVED_BUCKETS = 512`.

A dedicated `docs/known-gaps.md` entry — "Cross-protocol semantic gaps" — was filed for this, meant
to grow as more codecs join, rather than staying scattered across doc comments.

One golden-bytes fixture (`OTLP_TRACE_REQUEST`, captured from a real collector with a provenance
comment) proves the vendored `.proto`s match reality without any test needing a running service.

## PR3 — `feat/otlp-components`: `otlp_in` + `otlp_out`

Branched off PR2's branch. Added the `protocol: grpc | http` field and both components.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    #[default]
    Http,   // it's what an `http://host:4318` endpoint already implies
    Grpc,
}
```

**Transport: hyper directly, hand-rolled unary gRPC — not tonic.** `tonic` 0.14's server is an axum
router, pulling `axum`+`h2`+`tower`+`tower-http`+`tonic-prost` against a workspace that already pins
`hyper`/`http`/`tower` through `reqwest` — real `[bans] multiple-versions` risk for ~95% unused
surface (streaming, reflection, health, interceptors, load balancing — none of which OTLP's three
unary `Export` methods need). Unary gRPC over HTTP/2 is fully specified and small enough to
hand-roll, squarely this codebase's culture (hand-rolled statsd, hand-rolled syslog UDP, hand-rolled
interner). See [ADR 0024](../adr/0024-hand-rolled-grpc-over-hyper.md).

**`otlp_out`** (`crates/logit-outputs/src/otlp.rs`) calls `encoder.encode_signals(batch)` and issues
one request per non-empty signal, sequentially, to `{endpoint}/v1/{logs,metrics,traces}` (HTTP) or
the gRPC `Export` method. Its own `Fault` classification: connect refused/DNS → `Clean`; timeout,
429/5xx, or gRPC `UNAVAILABLE`/`RESOURCE_EXHAUSTED`/`DEADLINE_EXCEEDED`/`ABORTED`/`INTERNAL` →
`Ambiguous`; other 4xx or gRPC `INVALID_ARGUMENT`/`UNAUTHENTICATED`/`PERMISSION_DENIED`/
`UNIMPLEMENTED` → `Permanent`. Partial success (`rejected_* > 0` in the response) still counts as a
successful `send` — the accepted portion landed, retrying would duplicate it — but is counted and
throttle-warned. **`duplicate_safe() -> false`**, unlike `influxdb_out`: a batch producing three
signals issues three requests, so a mid-batch failure re-sends an already-delivered signal on
retry, and OTLP has no idempotency identity at all — replaying spans creates duplicates, replaying
delta sums double-counts.

**`otlp_in`** (`crates/logit-inputs/src/otlp.rs`) binds a `TcpListener` and per-connection spawns a
handler. HTTP via `hyper_util::server::conn::auto::Builder` (handles h1 and h2c); routes
`POST /v1/{logs,metrics,traces}`, `415` for `application/json`. gRPC via
`hyper::server::conn::http2::Builder`; routes the three `Export` methods, unknown method →
`grpc-status: 12`. `MAX_REQUEST_BYTES = 4MiB`. **Compression is not supported** — `Content-Encoding:
gzip`/`grpc-encoding: gzip` are rejected outright rather than silently mishandled, recorded as a
known gap (the OTel Collector's own default exporter sends gzip, so pointing a real Collector at
`otlp_in` fails on day one even though `otlp_out → otlp_in` and `otlp_out → Tempo` both work).

The strongest test in this PR needed no external service:
`otlp_output_to_otlp_input_round_trips_a_batch_through_http`/`_through_grpc` — `OtlpInput` stood up
on an ephemeral port in-process, `OtlpOutput` pointed at it, asserting what came out the `Fanout`
matched what went in. That round trip exercised the gRPC client and server against each other with
no external dependency, which mattered since the server side (HTTP/2 trailers, per-method routing,
error-code mapping) was the largest single risk in this series.

## PR4 — `feat/demo-traces`: the demo integration

Depended on PR1, PR2, PR3. Config and docs only, no new Rust.

**`demo/logit.yaml`** uncommented and corrected the `trace_out` stanza: `protocol: grpc`,
`endpoint: tempo:4317` (Tempo's gRPC receiver, deliberately, to exercise the transport PR3 built
rather than the easier HTTP path), and `internal`'s `span_sample_rate` set to `1.0` explicitly (the
default `0.1` would show only a tenth of the demo's traces). `demo/compose.yaml`'s `logit` gained a
`depends_on: tempo` with `condition: service_started` (Tempo declares no healthcheck, and
`otlp_out` retries through `write_loop` like any sink regardless — ADR 0013's ordering rule).
`demo/grafana/dashboards/logit-internal.json` gained a `traces`-type panel (TraceQL `{}` against the
already-provisioned Tempo datasource) and a `logit.internal.spans.dropped` panel.

**A real interaction this plan's design didn't anticipate, found running the demo, not just
reading the code:** `trace_out`'s only available source, `self` (`internal` observing `logit`'s own
pipeline), doesn't distinguish signals — every drain carries both spans and this process's own
`logit.*` metrics. `OtlpOutput::send` issues one request per signal and aborts the rest of a `send`
call on the first failure. Tempo is a traces-only OTLP receiver (it registers a `TraceService` but
no `MetricsService`), so every mixed batch's traces request succeeded and its metrics request that
followed failed with `grpc-status: 12` (`UNIMPLEMENTED`, correctly classified `Fault::Permanent`).
That alone would have been recoverable noise — the traces had already landed, confirmed directly
against Tempo's `/api/search`/`/api/traces` endpoints — except that pointed straight at `self` with
nothing in between, `send` *never once* returned `Ok` for `trace_out`, and `write_loop`'s ~60s
sustained-permanent-failure guard ([ADR 0013](../adr/0013-service-lifecycle-and-output-retry.md),
revised by [ADR 0021](../adr/0021-buffered-sink-delivery.md)) killed the entire `logit` process
about a minute after every startup — taking the InfluxDB metrics path down with it, not just Tempo.
Fixed at the config layer, within this PR's no-new-Rust scope: a dedicated `aggregate` node
(`trace_windowed`) sits between `self` and `trace_out`. `aggregate` absorbs every mergeable metric
(`internal`'s own metrics are all `Counter`/`Gauge`/`Distribution`) into window state and forwards a
metric-less event — a pure span — untouched and immediately
(`crates/logit-transforms/src/aggregate.rs`'s `process` doc comment), so the overwhelming majority
of `trace_out`'s batches end up traces-only and `send` succeeds. `trace_windowed`'s own periodic
60s `flush` still occasionally emits a real metrics-only batch that fails the same way, but the many
successful pure-span deliveries surrounding it (roughly one every 10s) reset the guard's streak long
before it reaches 60s. Recorded in full, including why this is specific to a mixed-signal source
feeding a signal-partial backend rather than a general `otlp_out` problem, in `docs/known-gaps.md`'s
"`otlp_out` aborts an entire batch's `send`..." entry, with the same account inline in
`demo/logit.yaml`.

**`demo/README.md`**'s "What isn't wired yet" section was rewritten: traces work now, the Tempo row
in the URL table stopped saying "empty," and `syslog_out`/Loki is the one remaining open leg —
along with a note on the (harmless, at-most-once-a-minute) `trace_out` warning above, so a
first-time user watching `docker compose logs -f logit` isn't alarmed by it.

## Follow-ups deliberately left out

- **`syslog_out`** and the demo's Loki/Alloy log leg — dropped from this session by choice, not
  oversight. `demo/logit.yaml`'s commented `log_out` and Alloy's unfed listener stay as-is.
- **Reworking `demo/hello/app.py` to use a real Python tracing library**, which would then exercise
  `otlp_in` with genuine third-party SDK traffic and put application spans in Tempo alongside
  `logit`'s internal ones. The natural home for demonstrating `otlp_in`.
- **`Output::send` taking `&Delivered`** — not needed (see PR1), so the trait stays as it is.
- **OTLP request compression** (gzip) on `otlp_in` — the OTel collector's default exporter sends
  it; PR3 rejects it explicitly (`415`/`grpc-status: 12`) rather than silently mishandling it.
- **A `keep`-in-front recommendation for `otlp_in`** in `docs/deploying.md` — peer-supplied
  attribute keys hit the never-evicting interner (`docs/known-gaps.md`), and `otlp_in` is the
  sharpest form of that gap yet.
- **A config-layer way to filter an event stream by which payload (log/metric/span) it carries**,
  or a per-signal partial-failure mode on `OtlpOutput::send` — either would let `trace_out` (or any
  future `otlp_out` fed a mixed-signal source) skip `trace_windowed`'s workaround entirely. Recorded
  alongside the interaction it would fix, in `docs/known-gaps.md`.

## Verification

Per PR: `script/cibuild` — `script/format --check`, `script/lint` (`clippy -D warnings`),
`script/test` (`cargo nextest run --workspace`), `script/validate` (`logit validate` over
`demo/logit.yaml` and every `examples/*.yaml`), the `script/schema` drift check, and `script/audit`
(`cargo-deny` + `cargo-audit`) — passed clean at every PR boundary, including on the combined
`feat/demo-traces` branch after merging all three prior branches together.

End to end, after PR4, run against a real Docker Compose stack in this session:

1. `cd demo && docker compose up --build -d` — every service healthy;
   `docker compose ps -a` showed `graph-dot`/`graph-svg` at `Exited (0)`.
2. `docker compose logs logit` — `stdio_out` blocks appeared per synthetic line.
3. Tempo's own `/api/search`/`/api/traces` endpoints (queried directly, since Grafana's own
   click-through wasn't exercised interactively in this session) returned real traces: a
   `syslog_in send` root, `json process` → `kv_metrics process` → ... → `influxdb_out deliver`
   children, correct `parentSpanId` chaining throughout, matching the configured topology exactly.
4. InfluxDB, queried directly via its Flux API: `web.requests`/`web.request_time` still tagged
   exactly `host`/`request_method`/`status`, populating continuously and unaffected by `trace_out`'s
   own (harmless) periodic failures.
5. `logit.internal.spans.dropped` stayed empty (zero) at demo volume, confirming the bounded
   per-component span buffer was never hit.
6. The `logit` container ran stably for several minutes with zero restarts across a fresh
   `docker compose up --build -d` — the specific regression this session found and fixed
   (`trace_windowed`, above) was re-verified fixed on this from-scratch run, not just the run that
   originally caught it.
7. `script/server` and `make up` (the dev stack) were not disturbed by anything in this plan — no
   file either targets was touched.
