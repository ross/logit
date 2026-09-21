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
run by CI, software versions and date recorded. It goes one step further on the "one function per
producer" rule: a producer is **one file**, `tools/shape-survey/producers/<name>.sh`, discovered by
glob rather than named in a list, so producers can be added in parallel with no shared file to
edit — nothing producer-specific lives in `lib.sh` or in the dispatcher.

`tools/shape-survey/configs/*.yaml` joins the globs in `script/validate` and
`every_shipped_config_loads_and_validates`. A p99 is quoted only with ≥100k events behind it.

**First, and free:** replay `testdata/interop/{statsd,otlp,prometheus,collectd,graphite,syslog}`
through `shape`. The statsd corpus's tags-per-line and lines-per-datagram figures are independently
known, so `shape` either reproduces them or is wrong — that is its acceptance test.
`tools/shape-survey/check_interop.py` is that test: it re-derives events-per-datagram and
attributes-per-event from the `.raw` files with its own parser (importing nothing from
`summarize.py`, copying nothing from the corpus README) and asserts `shape` reported the same.
As built, it reproduces exactly — events per datagram `[1×48, 7×1, 8×3, 10×3, 11×1]` and
attributes per event `[0×38, 1×17, 5×4, 6×9, 7×20, 8×32]` over the corpus's 56 datagrams and 120
lines.

**The `exporters` producer** is the first captured-from-scratch one: official exporter images
(node_exporter, postgres_exporter, redis_exporter, nginx-prometheus-exporter, blackbox_exporter's
`/probe` and its own `client_golang` registry) in **default** configuration, one `prometheus_in`
per target so `source` separates them, at a 5s interval for ≥10 scrapes each. At a scrape-mode tap
`logit.shape.attributes` *is* labels per series and `logit.shape.batch.events` *is* series per
scrape; `instance`/`prometheus.target` ride on the resource (dropped at the tap), so a count is the
wire's own labels plus `prometheus.type` on an untyped family. cAdvisor is **not** captured: it
needs more than read-only `/`, `/sys` and `/var/lib/docker` on this daemon (`inotify_add_watch
/sys/fs/cgroup: permission denied` without `--privileged`), and the run records that rather than
measuring a privileged configuration nobody would call default. Series counts from idle
single-instance services are a floor; the label *structure* is not.

**`combine.py`** folds N run directories into one cross-producer report — one table per dimension,
one row per producer × source × tap × signal, each row carrying its run's representativeness line.
That is what W3's tables get written from, and it recomputes the >4/>8/>12/>16 spill fractions from
`summary.json`'s exact value→count tables rather than re-reading any capture.

**Every producer states its representativeness in one line**, written into the run's
`provenance.txt` and printed by `summarize.py` as the banner at the top of `summary.md`, above any
number. That is structural rather than a footnote because these tables get quoted: the `demo`
producer is the harness's best end-to-end exercise and its weakest evidence — the stack exists to
demonstrate `logit`, parts of it are configured for visibility rather than the way an operator
would run them, and several of the formats it measures (nginx's JSON `log_format`, the Django and
Celery logging configs) were authored in this repository, which makes measuring them circular. That
producer therefore also labels each tier with whether its format is the software's own default
(HAProxy's `option httplog`, Postgres's `jsonlog`, Redis's log line, Docker's json-file envelope)
or one of ours, and `summarize.py` renders that table above the distributions.

### What was actually built and run

Six producers, all captured. The window is each producer's default, overridable per run.

| Producer | What it captures | Signals | Window |
|---|---|---|---|
| `interop` | every recorded corpus under `testdata/interop/` replayed at the listener that decoded it — statsd, syslog (UDP and a real RFC 6587 TCP stream), collectd, carbon plaintext and pickle, Prometheus remote-write, OTLP/JSON. Also the instrument's acceptance test | all three | corpus-driven |
| `exporters` | node, postgres, redis, nginx, blackbox and Go-runtime exporters, official images in default configuration, one `prometheus_in` per target at 5 s | metrics | 70 s (≥10 scrapes each) |
| `applogs` | eight log streams from five tiny HTTP apps — structlog, python-json-logger (both its documented and its bare-default config), pino-http, bare pino, Go `log/slog`, zap, semantic_logger — each through `tail_in` + `json`; plus one Django app under `opentelemetry-instrument` straight into `otlp_in` with no Collector | all three | 300 s |
| `oteldemo` | the OpenTelemetry Demo at a pinned tag, cloned at run time, under its own Locust generator, through the demo's own Collector into `otlp_in` | all three | 1200 s, plus an opt-in 180 s `resource: keep` run |
| `hostagents` | collectd and Telegraf in default configuration over five wires at once: collectd binary, carbon plaintext from each agent, a scraped Telegraf Prometheus endpoint, Telegraf OTLP/gRPC | metrics | 600 s, then a 180 s wire-grouped second run |
| `demo` | this repo's own `demo/` stack, config generated from `demo/logit.yaml` at run time through a compose overlay; `demo/` itself untouched | logs, spans | 900 s |

Raw captures never enter the repo.

### Deviations from the planned capture list, and why

The table above replaced the one this plan opened with. Four planned items were not captured, and
one was replaced; none of it is a gap somebody forgot.

- **kube-state-metrics was not captured.** It needs a real cluster (`kind` or otherwise) to have
  any objects to report on, and an empty one reports an empty shape. Nothing was counted from
  published output in its place either, so there is no kube-state-metrics row at all.
- **cAdvisor was not captured.** It cannot start without `--privileged` on this daemon
  (`inotify_add_watch /sys/fs/cgroup: permission denied` with read-only `/`, `/sys` and
  `/var/lib/docker`), and a survey does not run a privileged container to measure a label set.
  `exporters`' `provenance.txt` records the attempt and the error.
- **Loghub samples were not replayed.** The item was body-length-only from the start, and every
  other producer now measures body length from live software; a corpus of anonymized 2010s log
  files would have added a row whose provenance nobody could state in one line.
- **Rails/lograge was not captured.** lograge is a Rails railtie with no supported use outside
  Rails, and a `gem install rails` + `rails new` inside an image build is minutes of build for four
  routes. The brief's own fallback, **semantic_logger**'s `formatter: :json`, was taken instead —
  so `applogs`' Ruby row is a semantic_logger row, not a Rails row, and its provenance says so.
- **Postgres-backed Django was not captured.** `applogs`' Django leg runs on **sqlite**. The dbapi
  span comes from the same `opentelemetry-instrumentation-dbapi` either way, but sqlite's carries
  no network peer, so that span's attribute count sits at the low end of the desk range rather than
  the middle.
- **The `demo/` OpenTelemetry overlay was replaced.** Rather than bolt auto-instrumentation onto
  `demo/`'s Django and Celery tiers, `applogs` runs a **standalone** auto-instrumented Django app —
  at the owner's request, because `demo/` numbers carry little weight (the stack exists to
  demonstrate `logit`, and several of the formats it measures were authored here). The
  static-to-runtime correction factor the overlay was meant to yield is produced there instead, and
  `applogs`' own summary section prints the desk count beside the measured one per span kind.

## W3 — synthesis

Per-signal tables by archetype; the source-to-`Event` mapping; a "what this says about today's
constants" section that states implications **without deciding** — the fraction of each archetype
that spills at 4/8/12/16 inline slots, what a plain `Vec`, a per-batch arena, or a shared-key
layout would see, the typical string-attribute cost per event; gaps and low-confidence areas.
`memory.md`'s open question is updated to point at the result.

**As built.** [`docs/design/data-shapes.md`](../design/data-shapes.md) is the synthesis, led by its
five findings; [`docs/design/data-shapes-rows.md`](../design/data-shapes-rows.md) is the appendix
the plan called for — the 132 desk rows the synthesis draws on, condensed from about 300, each with
its citation, its two grades, and a mark saying whether the verification pass confirmed or corrected
it (106 rows re-derived; about 83% confirmed exactly, the rest off by a small count or a label, none
changing a headline). Three things differ from what this plan set out:

- The archetype table gained an "evidence" column, because two of the six archetypes — edge/access
  logs and wide events — ended with no capture behind them, and the Kubernetes one has no cluster
  capture. The doc says so where it matters rather than presenting six peers.
- `demo/`'s numbers are recorded and used for nothing. It gets its own representativeness tier,
  below a third party's demo: a stack built to demonstrate `logit`, with formats authored here.
- The doc's follow-up list leads with two things this plan did not foresee: an NDJSON format for
  `file_out`/`stdio_out` (the survey's readout depends on parsing a human text render, the only
  sink that carries raw `Samples`), and running a peer's own flat-map benchmark, which measures the
  same question directly.

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
