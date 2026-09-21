# Data shapes: what actually flows through `logit`

A reference, not a plan, and the third of three siblings:
[`telemetry-landscape.md`](telemetry-landscape.md) records what each protocol *can* express,
[`memory.md`](memory.md) records what the event model *costs*, and this document records what real
producers *do* send — how many attributes, how long, how nested, how repetitive, how batched. It
exists because `memory.md` §8 defers two inline-capacity decisions as needing "a real distribution
of attribute/metric counts," and because the same distribution is what fixtures, perf scenarios,
examples, and priorities should be built from rather than from one reference pipeline.

[`docs/plans/data-shape-survey.md`](../plans/data-shape-survey.md) is the plan that produced it.
Collected 2026-09-20: a desk survey of about 300 rows counted from pinned sources and
specifications (condensed to the 132 in [`data-shapes-rows.md`](data-shapes-rows.md), every one
with its citation), and live captures of real third-party software measured by the
[`shape`](../adr/shape-observer-component.md) component through `script/shape-survey`. §2–§4 read
the two together by signal, §5 records the captures, §6 says what the numbers imply, §7 says how
far to trust them.

This document describes data. It makes no sizing decision — §6 states what the numbers imply for
today's constants and stops there.

**The five findings, for a reader who stops here:**

1. **Per-event width is set by the signal, not the ecosystem.** Metric points are narrow (median
   0–2 labels for every scraped exporter measured, above 8 under 0.5% of the time; 5–7 where a
   series carries its own identity as labels — a tagged statsd line, a collectd value list, a
   remote-write or cAdvisor series — with a ceiling of 10–11). Spans straddle the 8-slot boundary (desk typical 5–11, ceiling 14–17; measured median 8,
   35% above 8, 14% above 16). Parsed structured logs sit above it — a request-log
   record of eight ordinary access fields (method, path, status, duration, …) landed at a median of
   9–14 top-level attributes through every JSON logging library measured, 99.9–100% of events above
   8 and none above 16, before any operator-added context.
2. **Width comes from enrichment and identity, and the model already pays for that per batch.** A
   bare SDK resource is 4–6 attributes in most languages (9 in Ruby, 12–15 from the Java agent); through one collector it measured 17 (median) to 29, and
   the enrichment layers on offer go to 30–43 fixed names plus unbounded label maps. That cost
   lands on `Resource`, shared by a batch whose median size was 5 events.
3. **More than one metric per event is rare and shallow.** Live collectd with its default plugins:
   82.3% of events carry one metric, 17.4% two, 0.4% three, nothing more. Every other metric input
   is a constant one by construction.
4. **Key-sets repeat heavily on homogeneous legs and not on a mixed one.** One key-set covers
   96.7–97.3% of events for every logging library and 63–91% for most exporters — but 9.5% on a
   mixed OTLP gateway carrying 196 of them.
5. **Nobody else has measured this.** Peer collectors size these structures from limits and
   synthetic benchmark widths; the one "typical" attribute count found anywhere in their
   repositories is an uncited line in an unmerged proposal.

## 0. What these numbers can and can't tell you

No production traffic was available to this survey. Every number here is one of: counted from a
producer's pinned source or specification, measured from real software run for the purpose, or
cited from someone else's published figure. None is a sample of anyone's production. The grading in
§1 exists so that limitation stays visible on every row instead of being stated once and forgotten.

Three consequences worth holding onto while reading anything below:

- **A demo is not a deployment.** Instrumentation demos exercise every feature of every library at
  once and carry no operator-added context; real services are configured, enriched, and trimmed.
  Demo-derived rows are the most precisely measured and the least representative in the document.
- **A default is a floor, not a typical.** A logging library's default record is what it emits
  before anyone adds a request id, a user, a tenant, and a trace reference. Where a library's own
  documentation shows a production configuration, that is recorded alongside the default.
- **A static count is a range.** An instrumentation sets attributes conditionally, so reading its
  source yields a minimum, a maximum, and a judgment about the middle — not the runtime
  distribution. §1's correction factor is the one place that gap is measured rather than guessed.

## 1. Methodology

### Dimensions and their weight units

Each dimension is weighted by the unit the cost it informs is paid in. An inline slot is paid per
*event* regardless of that event's size, so attribute-count distributions are per event and never
per byte; interner and `KeyCache` pressure is per *series*; `Resource`/`Scope` width is per *batch*,
because `EventBatch` shares both behind an `Arc`.

| Dimension | Weight unit | What it informs |
|---|---|---|
| Top-level attributes per event, at the input and after a transform chain | per event | `AttrMap`'s inline capacity |
| Nested maps per event, and their widths | per event | every `Value::Map` is its own boxed `AttrMap` |
| Key length, value length, value-type mix | per attribute | interning, `Value` drop, zero-copy `Bytes` |
| Distinct keys per source; key-set repetition across and within batches | per series / per batch | `KeyCache`, interner growth, the native dictionary, shared-key layouts |
| Resource and scope attributes, with events per batch | per batch | `Resource`/`Scope` |
| Batch size, as wire grouping and as accumulator flush | per batch | `receive.*` defaults, queue sizing |
| Metrics per event | per event | `MetricList`'s inline capacity |
| Wire grouping of metric points (OTLP) | per request | whether a decoder *should* group |
| Signal co-occurrence per event | per event | `memory.md` §0's workload table |
| Log body length | per byte | decode buffers, datagram limits |
| Label-value cardinality | per series | `aggregate`, `keep_values` |
| Span events and links; raw samples per timer | per event | recorded for completeness; no constant depends on them |

"As the source emits it" and "as `logit` lands it" are different numbers and both are recorded: a
JSON access log with 12 fields arriving over syslog is a 12-field source shape and, after
`syslog_in` and `json`, a 16-to-18-attribute `Event`.

### Grading

Every row carries two grades.

| Fidelity | Meaning |
|---|---|
| Measured | captured from running software and counted by the `shape` component |
| Counted | counted statically from a pinned commit, release, or specification |
| Reported | a figure someone else published, cited |
| Estimated | our inference; the reasoning is stated with the row |

| Representativeness | Meaning |
|---|---|
| Own demo | this repository's own `demo/` stack — below every other tier, and used for nothing but exercising the harness. Several of its formats (nginx's JSON `log_format`, the Django and Celery logging configuration) were authored here, so measuring them is circular |
| Demo | a third party's demonstration or sample application |
| Default | a library's or vendor's out-of-the-box configuration |
| Configured | a typical operator configuration, per the producer's own documentation |
| Production | a figure derived from production traffic, by whoever published it |

### Static-to-runtime correction

Statically counted instrumentation rows are triples — required · +conditionally required ·
+recommended, or min · typical · max with the conditions named. For the libraries that are also
captured live (§5), the ratio between the static triple and the measured distribution is reported
as a correction factor, named as such wherever it is applied to a library that was only counted.

### Archetypes, not an average

There is no honest way to weight the industry by volume, so there is no global distribution here.
Results are read per deployment archetype, and each says what stands behind it:

| Archetype | Dominant signals and protocols | Evidence here |
|---|---|---|
| Kubernetes, OpenTelemetry-instrumented microservices | OTLP logs/metrics/traces; heavy resource enrichment | Measured/Demo (OpenTelemetry Demo; a Django app under SDK defaults) + Counted instrumentation and enrichment rows. **No Kubernetes capture** — enrichment width is Counted only |
| Classic VM/web stack | syslog and access logs, statsd, a Prometheus or collectd host agent | Measured/Default (collectd, Telegraf, exporters, recorded statsd/syslog producers) + Counted access-log formats |
| DogStatsD-and-JSON-logs shop | tagged statsd, structured JSON logs, vendor tracing | Measured (eight logging-library configurations: six documented-production, two bare-default) + the recorded DogStatsD client; vendor tracing Counted only |
| Prometheus-centric infrastructure | scrape and remote-write; few logs through `logit` | Measured/Default (six exporters, a recorded remote-write sender); cluster-scale series counts Estimated |
| Edge / access-log heavy | very high-rate, fixed-schema log lines | **Counted only** — no capture of a load balancer, CDN, or ingress |
| Wide-event shop | one deliberately wide structured event per unit of work | **Reported only** — one vendor's observation, no capture |

### Provenance

Desk rows cite a URL or a repository, pinned revision, and path; all of them are in
[`data-shapes-rows.md`](data-shapes-rows.md). Measured rows cite their producer under
`tools/shape-survey/producers/`, which records software versions, image digests and the date in
each run's provenance; re-running `script/shape-survey <producer>` reproduces them. Raw captures
are not in the repository. About a third of the desk rows were independently re-derived before
inclusion; §7 records the result.

## 2. Logs

### What a producer writes

| Source family | Top-level fields | Nesting | Grade |
|---|---|---|---|
| Logging libraries, **bare default** | 2–7: winston 2 (no timestamp), slog/zerolog/logrus 3, zap 4, pino 5, bunyan 7. structlog, semantic_logger, Rails and `env_logger` default to **unstructured text** — there is no JSON default to count | flat | Counted / Default |
| Logging libraries writing a request log of eight access fields — six in their **documented production configuration**, two bare-default; no operator-added context | **measured median 9–14, max 10–15** (§5.3) — the library's own envelope is the remainder, 1–6 fields (pino-http instead folds the request fields into nested objects) | flat, except pino-http (4 nested maps per event, 3 wide, depth 2), semantic_logger (1 map, 8 wide), and structlog's error path (depth 5, traceback frames) | Measured / Configured and Default |
| JVM / .NET JSON layouts | logstash-logback 7 (MDC flattened onto the root); log4j2's three shipped templates 7, 11 and 13, each handling MDC differently — dotted keys, a real nested object, a `_` prefix; Serilog CLEF 2 + every property flattened; .NET `JsonConsoleFormatter` 4, up to depth 3 with scopes | mixed, up to depth 3 | Counted / Default |
| Access logs | nginx `combined` 8, Apache `combined` 9, Envoy 15, HAProxy `httplog` 16 slots (~28 atomic values), ingress-nginx 17; widely copied nginx JSON formats 11–28; ALB 34, CloudFront 33; Cloudflare Logpush 174 available with no default subset | flat, except Caddy (11 top-level over `request{9}` over `headers{}`/`tls{5}`, depth 3) | Counted / Default |
| System and service logs | RFC 5424 header 7 (+ structured data); Docker `json-file` 3; CRI 4; PostgreSQL `jsonlog` 29; journald **median 27, p90 33, max 40** per entry on one workstation (23 of them journald's own trusted fields) | flat | Counted; journald is one host's aggregate |
| Cloud audit / flow | VPC Flow Logs 14 of 54; Kubernetes audit 19; CloudTrail 31 top-level, `userIdentity` chain to depth 5 | CloudTrail is the deepest shape in the survey | Counted / Default |

A library's default is a floor in a specific sense: across roughly twenty libraries, the bare
default is either tiny or not structured at all, and every wide record is the product of
configuration. The width a collector sees is the *configured* width plus whatever the application
and the shipper add.

### What lands in an `Event`

Parsing is what widens a log. Straight off `tail_in` every library measured is **one** attribute
(the file path) and a body of 280–470 bytes at the median; after `json` it is 9–14. The same holds
for `syslog_in`, which stamps its own `syslog.*` attributes first (a median of 5 on the recorded
producers) and then takes whatever `json` merges in on top. The two-tap measurement exists because
of this — an input-side count says nothing about a log leg.

Two consequences of how `json` merges. It merges the **top level only**, so a nested object becomes
a `Value::Map` — its own boxed `AttrMap` — rather than more top-level attributes. pino-http is the
instructive case: its request serializers make the record *narrower* at the top (9 against bare
pino's 14) and *deeper*, carrying four nested maps per event; the top-level count understates it and
the nested-map count is where the cost went. And key-sets barely vary: every library produced two or
three distinct key-sets over ~7,300 events, the most common one covering 96.7–97.3% of them — the
second shape is the error line.

Keys are short and values are not always. Measured key length is 4–9 bytes at the median for JSON
logs, 14 for OpenTelemetry log attributes, 11 for journald; values run 10–16 bytes at the median
and 26–37 at p90, with a tail of user-agents and stack traces to 135–323 bytes, and one 6,103-byte
value in the OpenTelemetry Demo's logs. Roughly half to three-quarters of values are strings.

OpenTelemetry log records are their own shape: a median of 9 attributes (max 11) through the demo's
collector, 7 (max 10) from a Django app under SDK defaults, almost never nested (268 of 36,524
records), with a median body of 84 bytes. The log bridges are lean by default — Java's logback
bridge fills the record's core fields and captures no MDC or code attributes unless asked, and
Rust's `appender-tracing` sets no attributes of its own.

## 3. Metrics

Every metric input except `collectd_in` emits one event per point, series or line, so labels per
series *is* per-event attribute width.

### Labels per series

| Source | n | p50 | p90 | max | > 8 | Grade |
|---|--:|--:|--:|--:|--:|---|
| node_exporter, containerised | 19,292 | 1 | 2 | 16 | 0.2% | Measured / Default |
| node_exporter, one real host (its own e2e fixture) | 3,034 | 2 | 4 | 19 | — | Counted / Default |
| postgres_exporter | 7,294 | 0 | 3 | 7 | 0% | Measured / Default |
| redis_exporter | 4,004 | 0 | 1 | 10 | 0.3% | Measured / Default |
| nginx-prometheus-exporter | 756 | 0 | 0 | 7 | 0% | Measured / Default |
| blackbox_exporter, one HTTP probe | 280 | 0 | — | 1 | 0% | Measured / Default |
| a Go `client_golang` default registry | 777 | 0 | 1 | 7 | 0% | Measured / Default |
| Spring Boot Micrometer, one real pod scrape | 109 | 1 | 2 | 4 | — | Counted / Production (third-party sample) |
| cAdvisor, one container | — | 7 | 8 | 10 | — | Counted / Default |
| kube-state-metrics, pod family | 60 | 4 | — | 10 | — | Counted / Default |
| Prometheus remote-write, a recorded sender | 60 | 5 | — | 11 | 6.7% | Measured / Default (small corpus) |
| Telegraf, default inputs → Prometheus or OTLP | 56,579 / 26,270 | 2 | 3 | 5 | 0% | Measured / Default |
| DogStatsD and plain statsd, recorded clients | 120 lines | 6 | — | 8 | 0% | Measured / Default (synthetic workload) |
| collectd binary protocol, default plugins | 16,590 | 6 | 6 | 6 | 0% | Measured / Default |
| carbon plaintext — collectd `write_graphite`, Telegraf `outputs.graphite` | 19,296 / 26,270 | **0** | 0 | 0 | 0% | Measured / Default |
| OpenTelemetry Demo, metric points (most from the demo collector's own scrapers and span-metrics connector, a minority from an application SDK) | 60,840 | 1 | 5 | 7 | 0% | Measured / Demo |

Scraped exporters are the narrowest thing in the survey, and what width they have is on the wire:
`prometheus_in` puts `instance` and its target on the `Resource`, so the counts above are the
exporter's own labels plus, for about 2% of node_exporter's series, one `prometheus.type` marking a
family the exporter left untyped. Series that carry their identity as labels run wider —
remote-write (which has nowhere else to put `job` and `instance`), cAdvisor, kube-state-metrics, a
tagged statsd line, a collectd value list — but top out at 10–11. A default carbon plaintext feed
carries **no** attributes at all: both agents bake identity into the dotted path unless tag support
is switched on. Label names are 3–7 bytes at the median and values 4–7 for exporters; collectd's
identity keys are long (median 15) and its values short.

Series *counts* are a different matter and are not settled here. A containerised node_exporter
exposed 1,378 series against 3,034 for the fixture from a real host; kube-state-metrics and cAdvisor
scale with object count (roughly 5–15 series per pod and 100–300 per container); one histogram
family in the kubelet is 126 series. Cloudflare reports about 5 million series per Prometheus
instance in production. Everything measured here is a floor on counts and is not a floor on label
structure.

### Metrics per event

| Source | Events | 1 metric | 2 | 3 | > 3 | Grade |
|---|--:|--:|--:|--:|--:|---|
| collectd binary protocol, distro-default plugin set | 16,590 | 82.3% | 17.4% | 0.4% | 0 | Measured / Default |
| collectd, recorded corpus | 75 | — | — | — | max 3 | Measured / Default |
| every other metric input (OTLP, Prometheus, statsd, graphite) | — | 100% | | | | by construction |
| collectd `types.db`, by type *definition* | 391 types | 89.3% | 9.0% | 0.3% | 1.5% (4–5) | Counted / Default |

The definition count understates the live share of multi-valued events (10.7% of types against
17.7% of events), because `load`, `if_octets` and `disk_octets` fire every interval, and it
overstates the tail: no four- or five-source type appeared in a default plugin set at all. `df`,
`memory` and `cpu` repeat a one-source type per mount or per state rather than widening.

Telegraf is the one ecosystem whose points are genuinely wide — a median of 9 fields on the classic
system inputs, 64 on `redis` — and none of that reaches `logit` as width: every Telegraf output
serializer flattens one field to one series, so a 64-field point arrives as 64 one-metric events.
`kv_metrics` is the other source of multi-metric events, and what it produces is whatever the
operator configures (four in this repository's own nginx example — an illustration, not evidence).

### Batches

| Source | Events per batch, wire grouping | Under default accumulator | Grade |
|---|---|---|---|
| Prometheus scrape | one batch per target per scrape: 20 to 1,378 here | same — bypasses the accumulator | Measured / Default |
| Telegraf flush → Prometheus / OTLP | 483 / 442 | same | Measured / Default |
| collectd `network` plugin | **median 32, max 37** value lists per datagram | median 278 | Measured / Default |
| carbon plaintext | exactly 1 — a line stream has no grouping | median 320–442 | Measured / Default |
| DogStatsD / statsd, recorded clients | 1 unbuffered; 10–11 (DogStatsD) and 7–8 (statsd pipeline) buffered | — | Measured / Default |
| OpenTelemetry SDK export, no collector | median 3, p90 32, max 55 per (Resource, Scope) group | same | Measured / Default |
| OpenTelemetry Demo collector export | median 5, p90 16, p99 50, max 309 | same | Measured / Demo |

Client batching defaults do not converge: DogStatsD packs to 1,432 bytes over UDP and 8,192 over
UDS, pystatsd to 512 bytes, statsd-ruby batches by count (10), node-statsd not at all. All six
OpenTelemetry SDKs read agree on a 512-record export batch over a 2,048 queue, flushing spans every
5 s and logs every 1 s; the Collector's own batch processor defaults to 8,192. An OTLP batch as
`otlp_in` delivers it is one (Resource, Scope) group of one request, which is why the measured
medians are single digits against a 512-record export.

## 4. Traces

### Attributes per span

| Instrumentation | min · typical · max (default config) | Grade |
|---|---|---|
| OpenTelemetry semantic conventions, HTTP server | 3 required · +7 conditionally required · +6 recommended = **16**; opt-in takes it to 27–29 depending on the revision | Counted / spec |
| …HTTP client 12 (23 with opt-in) · DB client 14 (8–20 by technology) · RPC **9**, the smallest · `gen_ai` inference 27 (**31**, the largest) | | Counted / spec |
| Java agent, HTTP server | 16 with no configuration; client 12; DB 12–13; + one per allow-listed header | Counted / Default |
| Go `otelhttp` server | 3 · — · 16; client 9; `otelgrpc` 5; `otelsql` 1 | Counted / Default |
| .NET AspNetCore | 5 · 10 · 17; HttpClient 6 on .NET ≤ 8 and **0 on .NET 9+**, where the runtime emits them itself | Counted / Default |
| Python WSGI (Django, Flask) | 7 · 10 · 14 — frameworks add only `http.route`; **measured 11 · 12 · 13**, 1.2× the static typical | Counted + Measured |
| Python `requests` client | 3 · 4 · 6; **measured 4 · 4 · 4** | Counted + Measured |
| Python dbapi | 3 · 6 · 7; **measured 2 · 2 · 2 on sqlite**, which has no server, user or peer to report — not a Postgres figure | Counted + Measured |
| Python Celery task | 2 · 8 · 14 | Counted / Default |
| JS `http` server | 7 · 11 · 14, + an unbounded hook surface; express 2–3 per layer; pg 4 · 5 · 7; ioredis 5 | Counted / Default |
| Ruby Rack | 5 · 7 · 11+; `action_pack` adds 2–4 to that span and opens none; **`active_record` spans carry 0 attributes on 18 methods**; pg 6 · 9 · 13 | Counted / Default |
| Rust `tracing-opentelemetry` | 6 of its own (code location, thread, target) before any user field; user fields unbounded — `tracing`'s 32-field cap was removed in 2023 | Counted / Default |
| dd-trace, web and DB spans | 4 · 8–9 · 13–15 and 3 · 5–9 · 10 — the same band as the OpenTelemetry instrumentations | Counted / Default |
| **OpenTelemetry Demo, all services** | **n = 114,551: p50 8, p90 17, max 18; > 8 35.0%, > 12 23.7%, > 16 14.4%**; per-service medians from 1 to 14 | Measured / Demo |

Spans are the signal that straddles today's inline boundary, and they do it bimodally rather than
around a mean: a request is many narrow spans (a database call, a middleware layer, an
`active_record` method with nothing on it) and one or two wide ones (the HTTP server span, whose
ceiling under default configuration is 14–17 in every language with a stated maximum — Python
and JS 14, Java and Go 16, .NET 17, the measured maximum 18 — because they share one convention). The static-to-runtime
correction is **not one factor** — 1.2× for the server span, 1.0× for the client span, 0.33× for a
database span whose backend has nothing to report — so a desk triple is best read as a range the
runtime lands inside, toward the top for server spans.

Two configuration effects move whole populations. Python still defaults to the pre-stable HTTP
conventions, and opting into `http/dup` roughly doubles span width (about 19–24). And `db.statement`
is raw, unsanitised, unbounded SQL under Python's dbapi default, while Ruby replaces any statement
over 2,000 characters wholesale and Redis instrumentation caps at 1,000 — value length on spans is
bounded by no default anywhere, including the SDK limit (128 attributes, no length cap).

### Span events, links, and traces

Measured on the OpenTelemetry Demo: 76% of spans have **no** events, p90 is 8, max 31, and an event
carries a median of 0 and a p90 of 3 attributes; **no span in 114,551 carried a link**. From the
Django app: no events except one (4 attributes) on each error span, no links. Instrumentation
source agrees — an exception event on failure and nothing otherwise. The best production figure for
trace shape is Alibaba's (SoCC '21, over ten billion traces): mean call-graph depth 4.27, more than
4% deeper than 10, more than 10% touching over 40 services.

### Resource and enrichment

| Layer | Attributes | Grade |
|---|---|---|
| SDK default resource | Go, .NET, Rust **4**; Python, JS **5**; Ruby **9**; the Java agent **12–15** (it ships host, OS, process and container providers) | Counted / Default |
| …measured, Django under SDK defaults, no collector | **6 on every batch** | Measured / Default |
| …measured, through the OpenTelemetry Demo's collector (`resource_detection` on) | **min 10, median 17, p90 28, max 29** per batch; scope attributes median 0, max 4 | Measured / Demo |
| The conventions' identity groups (service, host, os, process, container, k8s.pod, cloud, …) | 38 without opt-ins, 56 with | Counted / spec |
| OpenTelemetry Collector `k8sattributes` | 6 by default → **30** fixed names fully enabled, **plus** unbounded label and annotation extraction | Counted |
| …`resourcedetection` | `system` 2 of 17 by default; `ec2` 9 of 9; `gcp` 17 of 19; `azure` 10 of 11 | Counted |
| Log shippers' Kubernetes metadata | Fluent Bit 11 (14), Vector 16, Filebeat host metadata 12 — labels and annotations arrive as **nested maps** | Counted / Default |
| Datadog Agent, Kubernetes pod | 43 tag names on offer; a point realistically carries 10–20 | Counted / Estimated |

Four independent sources size identity and per-record payload *separately* rather than as one
attribute set: Loki allows 15 indexed stream labels and, apart from them, 128 structured-metadata
entries per line; Mimir 30 labels (80 on info series); Datadog's own load generator draws 1–20
resource attributes against 0–10 per record; and Fluent Bit keeps a group level above the record.
Only the load generator says which side is wider, and it says the resource. This is the
part of the picture with **no capture behind it**: no Kubernetes cluster was measured, and no source
states how many labels a real pod carries, so the width of those nested maps is unknown.

## 5. Measured captures

All captured 2026-09-20 by `script/shape-survey <producer>`, through the release image built from
this tree, one `shape` tap per source (two on log legs), into `aggregate` with
`distributions: samples` so every figure is computed from retained raw values rather than a sketch.
Percentiles are nearest-rank. Each producer's one-line statement of what it is evidence *for* is
quoted because it is part of the result.

### 5.1 `interop` — the acceptance test

Every corpus under `testdata/interop/` replayed at the listener that decodes it. *"Recorded real
producers running synthetic workloads — grammar/packing evidence, not traffic mix."*

The statsd corpus's shape was derived independently from the captured bytes, by a parser sharing
nothing with the harness, and `shape` reproduced it exactly: events per datagram
`1×48, 7×1, 8×3, 10×3, 11×1`; attributes per event `0×38, 1×17, 5×4, 6×9, 7×20, 8×32`. Both agree
with the corpus's own README. Attribute counts sit above wire tag counts by exactly the `statsd.*`
carriers the decoder stamps. This is what licenses reading the other captures as measurements.

### 5.2 `exporters`, `hostagents` — infrastructure metrics

Official exporter images in default configuration against idle single-instance services, 14 scrapes
each at 5 s; collectd 5.12 (Debian's package and default plugin set) and Telegraf 1.32 (its eight
default inputs) for 10 minutes, then 3 minutes with `receive.batch_max_events: 1` for wire grouping.
*"Label/series structure per exporter; series counts scale with real object counts and are a
floor."* Results are the Measured rows of §3. cAdvisor was not captured (it needs a privileged
container); kube-state-metrics was not captured (no cluster).

Key-set repetition, by exporter — distinct key-sets, and the share of events carried by the most
common one and five: node_exporter 39 / 28% / 71%; postgres_exporter 13 / 63% / 91%;
redis_exporter 7 / 66% / 99%; nginx 4 / 91% / 100%; collectd 4 / 74% / 100%; Telegraf 5–6 /
51–56% / 100%.

### 5.3 `applogs` — logging libraries, and a Django app under OpenTelemetry

Pinned library versions in their **documented production configuration**, in minimal apps that log
one request line carrying the same eight ordinary access fields (method, path, status, duration and
the like) under a synthetic request mix (a few routes, some 404s, an occasional 500), about 7,300
events each; where a library has a JSON default, that too. The widths below are therefore the
library's envelope *plus* those eight fields — what is absent is operator-added context (tenant,
user, trace reference, deployment identity). *"Library record structure; no application-specific fields an
operator would add, so widths are a floor."*

| Library and configuration | Events | Attributes after `json`: p50 / max | > 8 | > 12 | Nested maps / width / depth (p50) |
|---|--:|--:|--:|--:|---|
| pino-http 11 (request serializers) | 7,344 | 9 / 10 | 100% | 0% | **4 / 3 / 2** |
| python-json-logger 4.2, default | 1,834 | 10 / 11 | 99.9% | 0% | flat |
| semantic_logger 5.1 | 7,336 | 11 / 14 | 100% | 2.7% | 1 / 8 / 1 |
| `log/slog` JSONHandler (Go 1.23) | 7,286 | 12 / 13 | 100% | 3% | flat |
| structlog 26.1, docs' production recipe | 7,339 | 12 / 13 | 100% | 3% | flat; depth 5 on the error path |
| python-json-logger 4.2, production | 7,333 | 13 / 14 | 100% | 100% | flat |
| zap 1.28 `NewProduction` | 7,286 | 13 / 15 | 100% | 100% | flat |
| pino 10.3, default | 1,837 | 14 / 15 | 99.9% | 100% | flat |

Nothing exceeded 16. At the input tap every one of these is a single attribute and a 280–470-byte
body. Two substitutions are recorded in the run's provenance: semantic_logger stands in for Rails
with lograge, and the Django app uses sqlite rather than Postgres — which is why its database span
reads 2 attributes. The Django leg's span, resource and batch figures are in §4 and §3.

One hazard met on the way is worth keeping: passing pino-http its destination as the first argument
silently drops its serializers, and the record then carries kilobytes of raw socket internals. A
single misconfiguration is all it takes to produce a pathologically wide event.

### 5.4 `oteldemo` — the OpenTelemetry Demo

OpenTelemetry Demo 3.1.0 (about ten languages, its own load generator), its core compose file only,
exporting through its own collector to `otlp_in`; 20 minutes, 211,915 events in 29,181 batches.
*"A demo app: every instrumentation enabled at once, no operator-added context, no production
enrichment."* What "through its own collector" includes at this release: `resource_detection`,
`memory_limiter`, and sanitising/redaction transforms on traces and logs — and **no** batch
processor, so a batch here is one (Resource, Scope) group of one collector export. One deviation:
the collector's `docker_stats` receiver was removed because it cannot reach the Docker socket on an
SELinux-enforcing host, and a receiver that fails to start takes the collector down with it.

| Signal | Events | p50 | p90 | p99 | max | > 4 | > 8 | > 12 | > 16 |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| span | 114,551 | 8 | 17 | 17 | 18 | 74.2% | 35.0% | 23.7% | 14.4% |
| metric point | 60,840 | 1 | 5 | 5 | 7 | 10.5% | 0% | 0% | 0% |
| log record | 36,524 | 9 | 11 | 11 | 11 | 68.6% | 50.4% | 0% | 0% |

Most of the metric points are the collector's own receivers (`host_metrics`, `nginx`, `redis`,
`postgresql`, a Prometheus scrape) and its span-metrics connector rather than application SDK
output; traces and logs have no such dilution.

264 distinct keys and 196 distinct key-sets on the one leg; the most common key-set carries 9.5% of
events and the top five 36.1%. Keys are long (median 14 bytes for spans and logs, 9 for metrics, max
41); about 86% of values are strings, 12–14% integers. Span values are 8 bytes at the median and 109
at p99.

### 5.5 `demo` — this repository's own demo stack

Run for three minutes to exercise the harness end to end, and **not used as evidence anywhere in
this document**: it is a stack built to demonstrate `logit`, parts of it are not configured as a
production system would be, and several of its log formats were authored here. For the record, its
software-default tiers landed at 17 (PostgreSQL `jsonlog`) and 19 (HAProxy `httplog` plus the span
lifted from it) attributes, in line with the Counted rows for those formats in §2.

## 6. What this says about today's constants

Implications, stated so a later decision can cite them. None of this decides anything; the sizing
ADR is follow-up 1 in §7.

**`AttrMap`, 8 inline slots on every `Event`.** Read per archetype, as the share of events whose
top-level attributes exceed a given inline capacity:

| Leg | > 4 | > 8 | > 12 | > 16 | Basis |
|---|--:|--:|--:|--:|---|
| Scraped exporters | 0–7% | ≤ 0.3% | ≤ 0.1% | 0% | Measured / Default |
| Tagged statsd; collectd | 54–98% | 0% | 0% | 0% | Measured / Default |
| carbon plaintext | 0% | 0% | 0% | 0% | Measured / Default |
| OpenTelemetry metric points | 10.5% | 0% | 0% | 0% | Measured / Demo |
| OpenTelemetry spans | 74% | 35% | 24% | 14% | Measured / Demo |
| OpenTelemetry log records | 69% | 50% | 0% | 0% | Measured / Demo |
| Parsed JSON request logs (eight access fields + the library's envelope) | 100% | 99.9–100% | 0–100% | 0% | Measured — a floor: no operator context |
| Access logs: nginx/Apache `combined` | yes | 0–yes (8, 9) | no | no | Counted |
| …Envoy (15), HAProxy (16), ingress-nginx (17) | yes | yes | yes | only ingress-nginx | Counted |
| …ALB, CloudFront, journald, `jsonlog`, CloudTrail | yes | yes | yes | yes (27–34) | Counted |

- **Shrinking below 8 has no support anywhere.** Four slots would spill most statsd, collectd, span
  and log events. This agrees with `memory.md`'s earlier "don't shrink," now on measured data.
- **8 is a metrics-and-simple-spans number.** It holds essentially every metric event measured and
  about two thirds of spans, and it held essentially no parsed structured log measured (at most 0.1% of any
  configuration's events): on such a leg the inline slots are 384 bytes that are paid and then
  spilled past.
- **No single larger number covers logs cheaply.** Twelve held five of the eight library
  configurations (two entirely, three at about 97%) and none of the other three; sixteen held all
  of them, the OpenTelemetry logs, and 86% of spans — at +384 bytes
  on every event, including every statsd counter — and still spills the access-log and audit shapes,
  which begin at 15 and run past 30. Those are also the highest-rate, most fixed-schema streams a
  collector sees.
- **The population is bimodal, not centred.** Metric events cluster at 0–6 and log events at 9–34,
  with spans spread across both. A capacity chosen for the mean serves neither mode; the sizing question
  is therefore not only "which constant" — representations that differ by leg, or that do not
  inline, are candidates the data does not rule out.
- **Key-set repetition is where a different representation would pay — on the right legs.** One
  key-set carries 97% of a logging library's events and 63–91% of most exporters'; a shared-key or
  per-batch layout would amortise almost all key storage there. On a mixed OTLP gateway (196
  key-sets, 9.5% for the most common) it would amortise little. Within one OTLP batch the measured
  median was 2 key-sets (max 9); a node_exporter scrape carries all 39 of its key-sets in one batch.
- **Nested maps multiply whatever is chosen.** Most sources are flat, but the ones that nest do it
  on every event — pino-http carries four boxed `AttrMap`s per event, Kubernetes-enriched logs carry
  label and annotation maps of unmeasured width — and each pays the full inline footprint again.
- **Whether a benchmark clones changes the answer.** VRL measured a flat-map-versus-tree crossover at
  about 128 fields in isolation and about 16 once the benchmark cloned the event, which is what a
  pipeline does; a micro-benchmark without the clone reads differently from the pipeline. The same
  work found an inline-string key type made a scan-based map about 10% *slower*; `logit` compares
  interned symbols and does not pay that cost.
- **The unit is still the cost of an N-string-attribute event.** Measured medians give the N to
  price: about 12 attributes with 8-byte keys and 12-byte values for a JSON log; 8 with 14-byte keys
  for a span; 0–2 with 5-byte keys for a scraped series.

**`MetricList`, 1 inline slot.** For `otlp_in`, `prometheus_in`, `statsd_in` and `graphite_in` every
event carries exactly one metric by construction, so one slot is never exceeded. For `collectd_in` it spills on 17.7% of events under a default plugin
set, never past three records. For `kv_metrics` the answer is the operator's configuration. Growing
it costs 224 bytes a slot on every event to save an allocation on a minority of one input's events;
whether `otlp_in` *should* group a scope's points onto one event is a separate design question the
wire data does not force (a collector export's (Resource, Scope) group had a median of 5 events).

**`Resource` and `Scope`.** Resource width is the widest attribute set in the OpenTelemetry picture
— measured 17 at the median and 29 at the maximum through one collector, against 8 inline slots —
and it is paid once per batch, with a median of 3–5 events sharing it. Whatever `AttrMap` becomes,
`Resource` is the consumer that is already always spilled; `Scope` measured 0 attributes at the
median and 4 at the maximum, so its inline attribute capacity is almost entirely unused.

**Interner and `KeyCache`.** Distinct keys per leg were 0–29 for every homogeneous source (0 on
carbon plaintext, 1 for a blackbox probe, 6–22 for the other agents and exporters, 10–17 for a
logging library, 29 for the Django app), 83 for node_exporter, and 264 on the mixed OTLP gateway — against a per-component `KeyCache` of 64 entries.
No measured key exceeded 41 bytes, well inside its 128-byte cap.

**Batching defaults.** Wire batches measured from 1 (carbon, unbuffered statsd) through about 30
(collectd), 440–480 (a Telegraf flush), to 1,378 (one node_exporter scrape). Only the scrape exceeds
`batch_max_events: 1000`, and it bypasses the accumulator.

**Fixtures.** `crates/logit-bench`'s hand-modelled shapes hold up better than their caveats suggest:
the sshd shape (6) sits inside the measured syslog range; the statsd shape (3 tags) is on the low
side of the recorded clients' 0–8 (median 6 on a tagged line, once the decoder's own `statsd.*`
carriers are counted); and the
"modelled on pino, no live process captured" wide-JSON shape (32) is wider than any library measured
(9–15) — it is an access-log or audit-log width, not an application-log one. There is no fixture for
the commonest measured log shape (12 flat string attributes), for a span at the 16–17 ceiling, or
for a nested-map record.

## 7. Confidence, gaps, and follow-ups

### How far to trust the desk rows

A second pass independently re-derived 106 of roughly 300 rows — a seeded random 15% of each track
plus every row the synthesis leans on hardest — with instructions not to trust the first count.

| Tracks | Checked | Confirmed | Minor | Wrong |
|---|--:|--:|--:|--:|
| Schemas and limits; peers and published numbers | 24 | 20 | 2 | 2 |
| Vendor agents; infrastructure metrics | 19 | 15 | 2 | 2 |
| Logging libraries; OpenTelemetry (Python, JS, Ruby) | 31 | 27 | 4 | 0 |
| OpenTelemetry (Java, Go, .NET, Rust); infrastructure logs | 32 | 26 | 3 | 2 |

About 83% confirmed exactly (one row could not be checked). Most errors were counts off by one to
four or a mislabel; the larger ones were a subtotal reported as a total (25 for 30), a transposed
table cell (98 for 9), a default that had moved between the docs and the pinned source (8 MiB for
20), and a limit that no longer exists. None changed a headline, and every correction is applied and listed in the appendix. Two claims two tracks
disagreed on were settled against source: Rust `tracing`'s 32-field cap was removed in 2023, and the
HTTP server span count differs between two revisions of the conventions six weeks apart (opt-in
attributes grew from 11 to 13). The least-checked rows are the unsampled ones from the Python, JS
and Ruby instrumentation track, whose parent review was lost to a tooling failure. A handful of
figures could not be established at all and are kept out of the tables: New Relic's per-event
attribute counts (its reference is client-rendered and returned inconsistent lists), Datadog's and
InfluxDB's tag-count caps (unpublished), and any vendor's "average event size" beyond one
product-scoped figure.

### What this survey cannot say

- **It is not production traffic.** Ten desk rows carry a Production grade, and only three of them
  describe per-event shape (one workstation's journald, one third-party pod scrape, one vendor's
  observation); the captures are
  defaults, documented configurations and a demo. Defaults are systematically narrower than
  deployments, and every measured log width is explicitly a floor — no application fields, no
  shipper enrichment.
- **The two archetypes with the widest shapes have no capture.** Edge and access-log streams
  (15–34 fields, the highest event rates) rest on Counted rows; wide events (a vendor's "200–500
  dimensions") on one Reported claim.
- **Kubernetes enrichment is Counted, not Measured**, and the width of pod label and annotation maps
  is unknown — no source states it.
- **Nothing here bounds value cardinality**, only key counts and lengths.
- **Volume weighting does not exist.** Signal mix by respondent (metrics 95%, logs 87%, traces 57%
  of organisations, by a vendor's own survey) is not signal mix by event or by byte.
- **The instrument has limits of its own**, recorded in `docs/known-gaps.md`: distinct-key tracking
  is top-level only and cumulative; resource and scope width is a count without lengths; and the
  survey's readout depends on parsing `file_out`'s human text render, the only sink that carries raw
  `Samples` values.

### Follow-ups

Decisions and builds this data is for:

1. **An ADR on `AttrMap` and `MetricList` sizing.** **Done for `AttrMap`, 2026-09-21:**
   [ADR `event-sizing-and-allocation-strategy`](../adr/event-sizing-and-allocation-strategy.md) —
   8 stays, measured on both sides on the perf VM, and pre-sizing the spill was built and measured
   *slower* end to end (`performance.md` §8). `MetricList` remains open. As first framed, §6 frames it: the candidates are keeping 8, a
   larger constant, no inline storage, a per-batch arena, and a shared-key layout; measured with the
   allocation pins and `script/perf`, on benchmarks that clone, across the bimodal population rather
   than one shape.
2. **Survey-derived fixtures and perf scenarios** (**done**, `crates/logit-bench/src/fixtures.rs` and
   `perf/scenarios/json-parse-{app,nested,access}-log.yaml`) at the measured medians and tails: a 12-attribute
   flat JSON log, a pino-http-style nested record, a 16–17-attribute server span, a 30-field
   access-log line, a 3-record collectd event, and a 17-attribute resource over a 5-event batch.
3. **An NDJSON format for `file_out`/`stdio_out`.** It would give `shape` — and any operator — a
   machine-readable readout, and retire the text parser this survey depends on.
4. **`shape` for operators:** a dashboard in `demo/`, width alarms, and an opt-in report an operator
   could share back — the only route to production-derived numbers this project is likely to have.
5. **Whether `otlp_in` should group a scope's points onto one event**, informed by §3's batch data.

Deep dives this pass turned up, ranked by value for the effort:

1. **Run VRL's `objectmap_cliff` benchmark** from the open flat-map proposal — a peer's directly
   comparable measurement of the same question, whose per-width numbers the proposal only summarises.
2. **A capture of any real traffic** — one cluster's scrape, one ingress's access log, one OTLP feed.
   `shape` emits counts and lengths only, so this can be run where the traffic cannot leave.
3. **A Kubernetes capture**: kube-state-metrics, cAdvisor, `k8sattributes`, and real pod label
   counts — the largest Counted-only area in the document.
4. **An edge capture**: a real ingress or load-balancer log at rate, the widest high-volume shape.
5. **Rails with lograge, and Django on Postgres** — the two substitutions in §5.3.
6. **Alibaba's public trace dataset**, to count spans per trace and attributes per span rather than
   read them off a paper's summary.
7. **Vector's `real_world_1` regression corpus**, which may be the first measured peer shape.
8. **New Relic's attribute dictionary**, the one major vendor whose per-event budget is missing.
9. **OpenTelemetry log-record conventions**: logs have no requirement-level table like spans and
   metrics, so no static count exists for the signal `logit` handles most.
10. **Breadth deliberately left out**: PHP, mobile and browser RUM, serverless, IoT; messaging spans
    beyond Kafka; Windows Event Log, GCP and Azure audit logs; `gen_ai` conventions (very large
    values) and the profiling signal as they mature.
