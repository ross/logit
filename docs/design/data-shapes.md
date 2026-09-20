# Data shapes: what actually flows through `logit`

A reference, not a plan, and the third of three siblings:
[`telemetry-landscape.md`](telemetry-landscape.md) records what each protocol *can* express,
[`memory.md`](memory.md) records what the event model *costs*, and this document records what real
producers *do* send — how many attributes, how long, how nested, how repetitive, how batched. It
exists because `memory.md` §8 defers two inline-capacity decisions as needing "a real distribution
of attribute/metric counts," and because the same distribution is what fixtures, perf scenarios,
examples, and priorities should be built from rather than from one reference pipeline.

[`docs/plans/data-shape-survey.md`](../plans/data-shape-survey.md) is the plan that fills it.
**Status: skeleton.** The methodology below is settled; §2 onward are filled by that plan's W3.

This document describes data. It makes no sizing decision — §6 states what the numbers imply for
today's constants and stops there.

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
| Demo | a demonstration or sample application |
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
Results are presented per deployment archetype, and the archetypes with no capture behind them say
so:

| Archetype | Dominant signals and protocols |
|---|---|
| Kubernetes, OpenTelemetry-instrumented microservices | OTLP logs/metrics/traces; heavy resource enrichment |
| Classic VM/web stack | syslog and access logs, statsd, a Prometheus or collectd host agent |
| DogStatsD-and-JSON-logs shop | tagged statsd, wide structured JSON logs, vendor tracing |
| Prometheus-centric infrastructure | scrape and remote-write; few logs through `logit` |
| Edge / access-log heavy | very high-rate, fixed-schema log lines |
| Wide-event shop | one deliberately wide structured event per unit of work |

### Provenance

Desk rows cite a URL or a repository, pinned revision, and path. Measured rows cite the capture's
producer script in `tools/shape-survey/`, which records software versions and the date. Raw
captures are not in the repository. A sample of every desk track's rows was independently
re-derived before inclusion; §7 records the result.

## 2. Logs

*To be filled (W3).* Source shape by producer family — framework and library records, access logs,
system and service logs, cloud audit logs — then the landed `Event` shape per input and parse
chain.

## 3. Metrics

*To be filled (W3).* Labels per series and series per scrape by exporter; tags per line for
statsd/DogStatsD; values per emission for collectd and Telegraf (the `MetricList` evidence); OTLP
data-point attributes and wire grouping.

## 4. Traces

*To be filled (W3).* Attributes per span by instrumentation family, as triples; span events and
links; resource width and the enrichment that drives it.

## 5. Measured captures

*To be filled (W3).* One subsection per capture: what ran, for how long, how many events, and the
`shape` distributions at both taps.

## 6. What this says about today's constants

*To be filled (W3).* For each archetype: the fraction of events past 4, 8, 12, and 16 top-level
attributes; what a plain `Vec`, a per-batch arena, or a shared-key layout would see; the typical
string-attribute cost per event; the `MetricList` picture. Implications only — the decision belongs
to a follow-up ADR.

## 7. Confidence, gaps, and follow-ups

*To be filled (W3).* The verification sample results per track, the low-confidence areas, what was
deliberately left out, and the deep dives this pass turned up.
