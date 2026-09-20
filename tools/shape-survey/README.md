# `shape-survey` — the data-shape capture harness

The capture half of [`docs/plans/data-shape-survey.md`](../../docs/plans/data-shape-survey.md).
Runs real software (or replays recorded traffic from it) through `logit`'s own
[`shape`](../../docs/adr/shape-observer-component.md) component and folds the result into a
summary: how many attributes an event carries, how long its keys and values are, how deeply they
nest, how many events ride in a batch, how many distinct key-sets a source produces.

Driven by [`script/shape-survey`](../../script/shape-survey), which runs on the host and drives
docker — like `script/record-fixtures` and `script/perf`, and for the same reason: it runs real
third-party software with its own nondeterminism, so it is a deliberate, reviewed act and
**deliberately not part of `script/cibuild`**.

```
script/shape-survey                  # every producer
script/shape-survey interop          # one
SHAPE_SURVEY_DURATION=180 script/shape-survey demo     # a short verification run
SHAPE_SURVEY_SKIP_IMAGE=1 script/shape-survey interop  # reuse the already-built logit:shape-survey
SHAPE_SURVEY_OUT=/tmp/x script/shape-survey interop    # write runs somewhere else
```

## What comes out

One directory per run, `perf/results/shape-survey/<producer>/<UTC timestamp>/` by default
(`/perf/results/` is already gitignored):

| File | What it is |
|---|---|
| `shape.log` | the capture — `file_out`'s human render of every `logit.shape.*` measurement |
| `logit.log` | the `logit` container's own output for the run |
| `provenance.txt` | date, this repo's SHA and dirty state, docker and image ids, the producer's own software versions, and its **representativeness** line |
| `summary.json` | every series: count, min, max, mean, nearest-rank p50/p90/p99, and an exact value→count table for integer-valued series |
| `summary.md` | the same, as human tables |
| *(producer artifacts)* | e.g. `demo`'s generated `logit.yaml`, `compose.env`, `source-labels.json` |

**Raw captures never enter the repo, and nothing here ever writes under `testdata/`.** `shape`
itself emits counts and lengths only — never a key, a value, a log body or a metric name from an
observed event — which is what lets a summary leave an environment the traffic could not.

## How to read `summary.md`

- **Start with the banner.** Every summary opens with its run's representativeness line, and (for
  a producer that supplies one) a per-source table saying where each source's *format* came from.
  See "Representativeness is structural" below — this is not a footnote to skip past.
- **Percentiles are nearest-rank**, over the whole capture: every flush window's raw samples
  concatenated, not a per-window average of summaries. A p99 prints `n/a` below 100 values and a
  p90 below 10, and every table carries its own value count. The plan's own rule is stricter still
  for anything that lands in `docs/design/data-shapes.md`: a p99 is quoted only with ≥100k events
  behind it.
- **`logit.shape.attributes` at `tap_input` vs `tap_landed`** is the two-tap pattern: the same
  events measured straight off the input and again after that source's realistic transform chain.
  The difference is what parsing and enrichment added, measured rather than assumed.
- **`logit.shape.batch.events`** is events per batch. With `receive.batch_max_events: 1` that is
  the *wire's* own grouping — one datagram, one framing decision, one scrape — rather than the
  `BatchAccumulator`'s default regrouping. `otlp_in` and `prometheus_in` bypass the accumulator, so
  theirs is always the wire's.
- **The cumulative gauges** (`distinct_keys`, `distinct_keysets`, `keyset_share.top1`/`top5`,
  `tracking_overflow`) are since process start, not windowed, and cover top-level keys only.
  `tracking_overflow` at `1` means a cap was hit and the key/key-set numbers below it are floors.
- **A sketched series is a failure, not a row.** If `aggregate` ever falls back past
  `max_samples_per_series`, `summarize.py` exits non-zero naming the series instead of reporting an
  approximation as a measurement.

## Representativeness is structural

Every producer must pass a one-line representativeness statement to `survey_provenance`; it lands
in `provenance.txt` and `summarize.py` prints it as the banner at the top of `summary.md`, above
any number. That placement is the point: these tables get quoted and pasted, and a measurement of
a stack this project built to demonstrate itself says something very different from a measurement
of somebody else's producer.

Two current examples, and what each is worth:

- **`interop`** — *"recorded real producers running synthetic workloads — grammar/packing
  evidence, not traffic mix."* Real third-party senders (rsyslog, collectd, a real Prometheus,
  Datadog's client, the OpenTelemetry Collector), each recorded for a few seconds against a small
  synthetic workload. Good evidence about grammar, carriers and how a client packs a datagram;
  no evidence at all about volume or mix.
- **`demo`** — *"own demo stack — harness exercise; not evidence of production shape."* The best
  end-to-end exercise of the harness, and the weakest evidence in it. The stack exists to
  demonstrate `logit`; its tiers were chosen to show components off, and parts of it are
  configured for visibility rather than the way an operator would run them
  (`log_min_duration_statement=0`, minute-scale rotation, one synthetic request generator).

**Some formats in `demo/` were authored by this repository.** nginx's JSON `log_format` and the
Django app's and Celery worker's logging configs are ours; measuring the width of an event whose
field list we chose is circular. HAProxy's `option httplog` line, Postgres's `jsonlog`, Redis's
server log line and Docker's json-file envelope are those projects' own formats, configured on but
not designed here. The `demo` producer labels every tier with which it is (`source-labels.json`,
rendered as a table above the distributions), so a reader never has to remember which is which.
A producer whose sources are uniform can skip that file; one whose sources aren't should write it.

## Adding a producer

**One file.** Write `producers/<name>.sh` defining `survey_<name>()`, plus any config it needs
under `configs/`. `script/shape-survey` discovers producers by globbing `producers/*.sh` (a `-` in
the filename maps to `_` in the function name), so there is no list to join and two people can add
a producer in parallel without touching the same file. **Nothing producer-specific goes in
`lib.sh` or in the dispatcher** — if a producer needs something they do not have, add it as a
general facility, the way `survey_on_cleanup` is general rather than "tear down the demo stack".

What `lib.sh` gives you:

| Function | What it does |
|---|---|
| `survey_out_dir <producer>` | creates the run directory and sets `SURVEY_RUN_DIR` (call it plainly, never in `$( )` — a subshell would throw the assignment away) |
| `survey_provenance <producer> <representativeness>` | writes `provenance.txt`'s common half; append your own software versions to the same file |
| `start_logit <producer> <config>` | `logit validate`s the config in the image, runs it on the run's network under the alias `logit` with `/out` mounted, and waits on `logit ready` |
| `stop_logit` | `docker stop -t 60` (so the final flush lands), captures `logit.log`, fails if `shape.log` is missing or empty |
| `survey_python <suffix> <docker args> -- <cmd>` | runs `replay.py`/`summarize.py`/`check_interop.py` in a throwaway `python:3.12-slim` with `/tools` and `/out` mounted |
| `survey_summarize` | `summarize.py` over the run's `shape.log`, with the banner and any `source-labels.json` |
| `survey_on_cleanup <snippet>` | an idempotent teardown hook for a producer whose lifecycle is not "remove these containers" |
| `survey_track <name>` | register a container you started yourself for cleanup |

Rules the harness holds to, which a new producer inherits:

- **Everything is named `shape-survey-*`** (the image is `logit:shape-survey`), so cleanup
  enumerates exactly what this run created. The daemon is shared with other sessions: never prune,
  and never remove a resource this run did not create. If `docker network create` fails because
  the daemon is out of subnets, **stop and report it** rather than deleting someone else's network.
- **Never modify the thing you are measuring.** The `demo` producer generates its config from
  `demo/logit.yaml` at run time and mounts it through a compose overlay; `demo/` itself is
  untouched, and the generated config is a run artifact rather than a committed second copy that
  would have to be kept in step by hand.
- **Fail loudly.** A config that does not validate, a replay that could not send, an empty
  `shape.log`, a sketched `logit.shape.*` series — all of those stop the run. A survey that
  quietly produces a thin summary is worse than one that fails, because a thin summary looks like
  data.

### The config pattern

Everything under `configs/` is ordinary `logit` YAML, and joins the shipped-config globs
(`script/validate`, `every_shipped_config_loads_and_validates`) so a component field rename cannot
leave a capture config that only fails the next time somebody runs a 15-minute survey with it.

```
<input> ─┬─> tap_input   (shape, straight off the input)          ─┬─> shape_rollup ─> shape_out
         └─> <realistic chain> ─> tap_landed (shape)              ─┘   (aggregate)     (file_out)
```

- **Two taps where there is a real chain to measure across**, one where there isn't — a metrics
  input whose events arrive finished has nothing an operator would realistically parse out of it
  afterwards, and a second tap there measures `shape` measuring an identity function. Say so in a
  comment rather than leaving the asymmetry unexplained.
- **`receive.batch_max_events: 1`** on accumulator-fronted inputs, so a batch is a wire group.
- **`aggregate` with `distributions: samples`** and a `max_samples_per_series` far above what the
  capture can produce — that is what keeps `shape`'s raw observations raw through the window.
- **`file_out` with a huge `rotate.max_bytes`** — `file_out` requires a rotation trigger, and a
  survey that rotated would silently lose the start of its own capture.
- **An `admin:` block**, because `start_logit` waits on `logit ready` rather than sleeping.

## The acceptance test

`check_interop.py` re-derives, straight from `testdata/interop/statsd/*.raw` with its own parser,
the two numbers `shape` is supposed to report for that corpus — events per datagram and attributes
per event — and asserts they match. It imports nothing from `summarize.py` and copies nothing from
`testdata/interop/statsd/README.md`: a shared parser would let a parser bug cancel itself out, and
a number copied from a document would only prove the document and the checker agree.

The mapping is deliberately not 1:1, and that is the interesting part — `statsd_in` emits one
event per *value* on a `c`/`g` line and one per *line* for `ms`/`h`/`d`/`s`, and stamps
`statsd.container_id`, `statsd.timestamp` and (on `ms`/`h`/`d` only) `statsd.type` on top of the
wire tags. The script's docstring spells all of it out.

**If this ever fails, stop.** Do not adjust the check to pass: either the instrument, the decoder,
or the derivation is wrong, and which one has to be established before any survey result built on
`shape` means anything.
