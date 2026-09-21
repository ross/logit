---
created: 2026-09-20
updated: 2026-09-20
---

# Enabling plan: a data-shape survey, and the `shape` component that measures it

## Context

[`docs/design/memory.md`](../design/memory.md) §8 items 12–13 and its "Open questions" defer two
sizing decisions — `AttrMap`'s inline capacity (8 × 48 B = 384 B of every 864 B `Event`) and
`MetricList`'s (1 × 224 B) — as needing "a real distribution of attribute/metric counts across
production traffic, which doesn't exist yet and can't be synthesized honestly." The evidence today
is a handful of hand-modeled fixture shapes (statsd 0–4 attributes, sshd 6, logfmt 9, nginx 10,
wide-JSON 32) and exactly one measured capture
([`testdata/interop/statsd/README.md`](../../testdata/interop/statsd/README.md), median 6 tags per
tagged line). [`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md) surveys what
each protocol *can* express; nothing in the repo records what real producers *do* send.

The flamegraph work behind [`in-place-transform-process`](in-place-transform-process.md) already
showed the spill itself is a small lever: on the 9-attribute logfmt scenario, growing past the
inline slots was ~0.6% of the process and dropping the spilled `Vec` ~2.6% — the same order as
`KeyCache::get_or_intern` (2.2%) and per-`Value` drop (5.2%). The unit of analysis is therefore
**the cost of an N-string-attribute event, not N alone**, and this survey collects the whole shape
— counts, key and value sizes, types, nesting, repetition, batching — rather than one histogram.

**This pass is data collection only.** The inline-capacity question is one consumer of the result;
the same numbers feed fixtures, perf scenarios, examples, and priorities. No sizing decision is made
here, and no ADR on sizing is written here — that is follow-up 1. The one ADR this plan does produce
is for the `shape` component (W1), which is a design decision in its own right.

Stream key **`shapes`**. Branches `shapes/w0`…`shapes/w3`, a linear stack — each branch is cut from
its parent's, its PR targets that branch, retargeted to `main` once the parent merges
(`AGENTS.md`'s "Branches and PR titles"). PR stack only: nothing here merges on its own initiative;
Ross directs merging.

## Settled decisions

- **Public sources only**, desk research plus runnable captures. No production traffic is available
  to this pass.
- **The measuring instrument is a real component**, `shape` — an observer tapped off a flow by
  ordinary fan-out, emitting per-event shape measurements as metrics. It is useful beyond this
  survey: an operator can tap their own traffic with it, and because it emits counts and lengths
  only — never a key or a value — its output can leave an environment the traffic itself can't.
- **Captures run on a workstation.** These are counts, not timings; a busy box doesn't matter.
- **Desk breadth is top-heavy and time-boxed.** Python, JS/TS, Go, Java, Ruby, Rust, .NET; the top
  few frameworks and libraries in each; ≤40 rows per track. Gaps are recorded, not chased —
  anything that looks worth a deep dive goes on the follow-up list with a one-line reason.
- **Desk research is not its own PR.** Its working notes live outside the repo; a PR with no diff
  is the wrong split. It runs in parallel with W1/W2 and lands in W3.

## Workstreams

| WS | Branch | Content |
|---|---|---|
| W0 | `shapes/w0` | This plan; [`docs/design/data-shapes.md`](../design/data-shapes.md) skeleton and methodology |
| W1 | `shapes/w1` | The `shape` component, ADR `shape-observer-component`, schema, allocation pins, `examples/shape-tap.yaml` |
| W2 | `shapes/w2` | Capture harness (`script/shape-survey`, `tools/shape-survey/`); `shape` replayed over `testdata/interop/` as its acceptance test |
| W3 | `shapes/w3` | The filled doc: desk rows, measured rows, synthesis; `memory.md`'s open question pointed at it |

## What gets measured

Each dimension names the unit its distribution is weighted by. "p99 attributes" means three
different things per event, per series, and per byte; mixing them silently is the easiest way to get
a precise wrong answer.

| Dimension | Weight unit | Feeds |
|---|---|---|
| Top-level attribute count per event, **at two taps**: straight off the input, and after a realistic transform chain — events widen downstream (`syslog_in` adds `syslog.*`, `json` merges fields, `set`/`kv_metrics` add more) | per event | `AttrMap`'s inline capacity (`crates/logit-core/src/attrs.rs`) |
| Nested maps per event and their widths — each `Value::Map` is its own boxed 392 B `AttrMap`, and `json` merges only the top level | per event | multiplies any capacity change; the dominant term for ECS-, CloudTrail-, and k8s-enriched shapes |
| Key length, value length, value-type mix | per attribute | the real per-event cost: interning, `Value` drop, `Bytes` zero-copy |
| Distinct keys per source; key-set repetition across events and **within a batch** | per series / per batch | `KeyCache`'s caps, interner growth, the native format's dictionary, the shared-key layout `memory.md` §1 leaves on the table |
| Resource/scope attribute count **together with** events per batch — `EventBatch` holds `Arc<Resource>`/`Arc<Scope>`, so this cost is per batch, never per event | per batch | `Resource` (432 B) / `Scope` (496 B) |
| Batch size: wire grouping (`receive.batch_max_events: 1`) and accumulator flush size (defaults). `otlp_in` and `prometheus_in` bypass the accumulator, so their batches already are wire groups | per batch | `receive.*` defaults, queue sizing, perf scenarios |
| `MetricList` length — **a constant 1 for `otlp_in`, `prometheus_in`, `statsd_in`, and `graphite_in`**, which emit one event per point/series/line. The real evidence is collectd's `types.db` (data sources per type map 1:1), `kv_metrics` configs, and derived metrics per access-log line | per event | `MetricList`'s inline capacity (`crates/logit-core/src/event.rs`) |
| OTLP *wire* grouping: points per `Metric`, metrics per `ScopeMetrics` | per request | a **design question** — should `otlp_in` group a scope's points onto one event? — recorded as such, not as `MetricList` data |
| Signal co-occurrence: events carrying two or more of log/metrics/span | per event | `memory.md` §0's workload table; the "compact `Event`" open question |
| Log body length | per byte | decode buffers, UDP/syslog limits |
| Label-value cardinality per key | per series | `aggregate` series counts, `keep_values` guidance |
| Signal mix and adoption by ecosystem | — | priorities, examples |
| Span events/links per span; raw samples per timer | per event | **recorded, decides nothing** — plain `Vec`s have no capacity to pick, and `SAMPLES_INLINE = 19` is pinned by `MetricKind`'s 176 B, not by workload |

Byte-weighting is rejected outright for the two inline-capacity rows: every event pays 392/232 B
and at most one spill allocation regardless of how large it is.

## Grading: two axes on every row

**Fidelity** — did we get the number right? *Measured* (captured and counted by `shape`) ·
*Counted* (static, from a pinned commit or spec) · *Reported* (a cited claim) · *Estimated* (our
inference, reasoning stated).

**Representativeness** — does it generalize? *Demo* app · library/vendor *Default* · typical
operator-*Configured* · *Production*-reported.

A row from the OpenTelemetry Demo is Measured/Demo: highest fidelity, lowest representativeness. One
grade would hide exactly the bias a pass with no production traffic is most exposed to.

**Static counts are ranges.** Semantic-convention YAML carries requirement levels, and
instrumentation code sets attributes conditionally, so a desk row is a triple (required ·
+conditionally required · +recommended), never a point. One or two of the same libraries are then
measured live, and the static-to-runtime ratio is recorded as a named correction factor — the same
measured-versus-chosen discipline [`perf/load/README.md`](../../perf/load/README.md) applies to its
traffic model.

**No blended global average.** There is no honest volume weighting across the industry, so results
are presented per deployment archetype — k8s with OpenTelemetry microservices; a classic VM/web
stack (syslog and statsd); a DogStatsD-and-JSON-logs shop; Prometheus-centric infrastructure;
edge/access-log heavy; a wide-event shop. Within a captured archetype the event mix is weighted by
that capture's own `logit.component.events.received`. An archetype with no capture behind it is
labeled Estimated rather than presented as a peer of the measured ones.

## Desk survey

Parallel research tracks, each boxed to ≤40 rows and a page of notes. Every row carries a checkable
citation (URL, or repo + pinned SHA + path); counts come from the real source via script, never
from memory; an unverifiable figure is recorded as unverified. A separate verification pass
re-derives a ~15% random sample of each track's rows before any enter the doc.

| Track | Scope |
|---|---|
| T1 Schemas and limits | OpenTelemetry semantic conventions (triple per group), ECS, Datadog standard attributes, OCSF, Splunk CIM; vendor and SDK limits as proxies for the tail |
| T2 OpenTelemetry instrumentation | the highest-adoption instrumentations across Python, JS, Ruby, Java, Go, .NET, Rust; default resource detectors; SDK batch and limit defaults |
| T3 Vendor agents | Datadog `integrations-core` `metadata.csv` (metrics and tags per integration), agent host/container tagging, `dd-trace` span tags; New Relic, Sentry, and Elastic APM event attributes from public docs and open-source agents |
| T4 Logging libraries | default and typically-configured structured records: stdlib `logging`, structlog, django-structlog, loguru; slog, zap, zerolog; pino, winston; lograge, semantic_logger; logback/log4j2 JSON and ECS layouts; Serilog, MEL; `tracing-subscriber`; monolog |
| T5 Infrastructure | Prometheus exporters' real exposition output (labels per series, series per scrape); Telegraf plugins; collectd `types.db`; access-log formats; cloud audit/flow logs; journald; collector-side enrichment (`k8sattributes`, Fluent Bit's kubernetes filter, Vector, Filebeat) |
| T6 Peers and published numbers | how Vector, the OpenTelemetry Collector's `pdata`, Fluent Bit, Prometheus's label representations, and `tracing` size the same structures, and on what evidence; production-derived figures from engineering posts and papers; adoption surveys for signal mix |

## W1 — the `shape` component

```
statsd_in ─┬─> (real pipeline)
           └─> shape ─> aggregate ─> any sink
```

- `crates/logit-transforms/src/shape.rs`, implementing `logit_pipeline::Transform`, following
  `Aggregator`/`KvMetrics`. Tapped by ordinary fan-out, so it never perturbs the flow it measures;
  placing two gives the two taps.
- `process` rewrites the event **in place** into a measurement event and returns `true`: the
  payload is taken with `mem::take`, `event.metrics` is filled with `logit.shape.*` records, and the
  attributes are replaced by a small fixed tag set (`signal`, and `source` from the existing
  `observe_provenance` hook). Raw values out, no sketching inside — `aggregate` downstream builds
  the distributions, per [ADR `lossless-transit`](../adr/lossless-transit.md)'s "summarization is
  opt-in and named."
- Per event: attribute count, nested-map count and widths, value depth, metric count, span events
  and links, body bytes, key and value bytes, value-type counts, signal flags.
- Per batch (between `observe_batch_context` calls): events in the batch, resource and scope
  attribute counts, distinct key-sets within the batch.
- On flush (stateful, capped, drop-and-count past the cap): distinct keys, distinct key-sets (a
  hash of the sorted `Symbol` list — `AttrMap` is already sorted), top-N key-set share.
- Emits counts and lengths only, never a key or a value.
- The usual landing list: a `logit-config` type (`Serialize + Deserialize + JsonSchema`),
  `script/schema`, the `logit-cli::pipeline` registry entry, allocation pins in
  `crates/logit-bench/tests/allocations.rs` with `memory.md`'s row in the same commit, component
  docs, `AGENTS.md`'s current-state paragraph. Its measurement event carries around a dozen metrics
  and so spills `MetricList` by design — on a tap branch only; the ADR says so.

## W2 — capture harness

Real software → the matching input → `shape` (tap 1), and → a realistic transform chain → `shape`
(tap 2) → `aggregate` → `file_out`, with the histograms read off the shutdown flush.
`script/shape-survey` follows `script/record-fixtures`' precedent: a deliberate, reviewed act, never
run by CI, one function per producer, software versions and date recorded.
`tools/shape-survey/*.yaml` joins the globs in `script/validate` and
`every_shipped_config_loads_and_validates`. A p99 is quoted only with ≥100k events behind it.

**First, and free:** replay `testdata/interop/{statsd,otlp,prometheus,collectd,graphite,syslog}`
through `shape`. The statsd corpus's tags-per-line and lines-per-datagram figures are independently
known, so `shape` either reproduces them or is wrong — that is its acceptance test.

| Capture | Signals | Box |
|---|---|---|
| `demo/` as it stands (nginx, haproxy, postgres, redis, Django, Celery) | logs | 15 min under `traffic` |
| `demo/` with an OpenTelemetry auto-instrumentation overlay on Django and Celery → `otlp_in` (an overlay; `demo/` itself unchanged) | all three | 15 min; also yields the static-to-runtime correction factor |
| OpenTelemetry Demo → `otlp_in` | all three | one 30-minute run — the most expensive item for one archetype, so boxed hardest |
| node_exporter, cAdvisor, postgres and redis exporters → `prometheus_in` | metrics | 10 scrapes; kube-state-metrics Counted from published output unless a `kind` cluster is trivial |
| Telegraf and collectd default plugin sets → `statsd_in`/`collectd_in`/`graphite_in` | metrics | 10 min |
| Real JSON loggers (pino, structlog, zap, lograge) in minimal apps → `syslog_in`/`tail_in` + `json` | logs | replaces `fixtures.rs`'s "no live pino process was captured" caveat |
| Loghub samples → `tail_in` | logs | body length only |

Raw captures never enter the repo.

## W3 — synthesis

Per-signal tables by archetype; the source-to-`Event` mapping; a "what this says about today's
constants" section that states implications **without deciding** — the fraction of each archetype
that spills at 4/8/12/16 inline slots, what a plain `Vec`, a per-batch arena, or a shared-key
layout would see, the typical string-attribute cost per event; gaps and low-confidence areas.
`memory.md`'s open question is updated to point at the result.

## Follow-ups (listed in the doc; not done in this pass)

1. An ADR on `AttrMap`/`MetricList` sizing — keep 8, grow to 12/16, a plain `Vec`, a per-batch
   arena, a shared-key layout — measured with the allocation pins and `script/perf`.
2. Survey-derived fixtures (`crates/logit-bench/src/fixtures.rs`) and `perf/scenarios/` at
   p50/p90/p99 per signal.
3. Priorities: protocols, components, examples, and optimizations ranked by the evidence.
4. `shape` beyond the survey: a Grafana dashboard for it in `demo/`, an opt-in anonymous shape
   report operators could share back, width alarms.
5. The `otlp_in` grouping design question, if the wire-grouping data says it matters.
6. Emerging shapes: `gen_ai` semantic conventions (very large values), the profiling signal, eBPF
   sources.
7. Whatever deep dives the time-boxed pass surfaces, and the breadth it deliberately left out (PHP,
   mobile/RUM, serverless, IoT; per-framework conditional-attribute tracing).

## Verification

- `script/cibuild` green per PR.
- `shape` unit-tested over directly-constructed events (no running service), cross-checked against
  the known fixtures (`nginx_event` 10 attributes / 4 metrics, the wide-JSON shape's 32,
  `span_event`'s 2 events / 1 link).
- `shape` over `testdata/interop/statsd/` reproduces that README's figures.
- End to end in the dev stack: `statsd_in → shape → aggregate → file_out`, output inspected.
- Every doc row carries both grades and a citation; the verification sample pass is recorded per
  track.
