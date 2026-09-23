# `shape-survey` — the data-shape capture harness

This harness measures what real telemetry looks like: how many attributes an event carries, how
long its keys and values are, how deeply they nest, how many events ride in a batch, and how many
distinct key-sets a source produces. It runs real software, or replays recorded traffic from it,
through `logit`'s own [`shape`](../../docs/adr/shape-observer-component.md) component and folds the
result into a summary. It's the capture half of
[`docs/plans/data-shape-survey.md`](../../docs/plans/data-shape-survey.md).

[`script/shape-survey`](../../script/shape-survey) drives it. Like `script/record-fixtures` and
`script/perf`, it runs on the host and drives docker. It's **deliberately not part of
`script/cibuild`** for the same reason: it runs real third-party software with its own
nondeterminism, so a capture is a deliberate, reviewed act.

```sh
script/shape-survey                  # every producer
script/shape-survey interop          # one
SHAPE_SURVEY_DURATION=180 script/shape-survey demo     # a short verification run
SHAPE_SURVEY_SKIP_IMAGE=1 script/shape-survey interop  # reuse the already-built logit:shape-survey
SHAPE_SURVEY_OUT=/tmp/x script/shape-survey interop    # write runs somewhere else
```

Before any producer runs, the dispatcher runs `summarize.py --self-test` and builds
`logit:shape-survey` from the current tree. A render change that breaks the summarizer fails there,
not after a 15-minute capture. `SHAPE_SURVEY_DURATION` replaces the main capture window of every
producer except `interop`, which has no window.

## Producers

The dispatcher discovers producers by globbing `producers/*.sh`; there are six. Each row's
representativeness line is what that producer passes to `survey_provenance`, verbatim.
`summarize.py` prints it as `summary.md`'s banner, and `combine.py` puts it on every row that
producer contributes. Read it before any number under it.

| producer | what it runs, and how it reaches `logit` | signals | taps | default window |
|---|---|---|---|---|
| `interop` | **No live software.** `replay.py` re-sends every recorded corpus under `testdata/interop/` at the listener that decoded it — statsd (UDP), syslog (UDP *and* a real RFC 6587 TCP stream), collectd (UDP), carbon plaintext and pickle (TCP), Prometheus remote-write (HTTP), OTLP/JSON logs+metrics+traces (HTTP) | log, metric, span | `tap_input` on every leg; `tap_landed` on the two syslog legs (the only ones with a real chain) | corpus-driven (no window) |
| `exporters` | Six official Prometheus exporter images in default configuration — node, postgres, redis, nginx, blackbox, and the Go runtime — against idle single-instance Postgres/Redis/nginx, scraped by `prometheus_in` at 5 s | metric | one per exporter (`tap_<exporter>`); no second tap — a scrape arrives finished | 70 s |
| `applogs` | Eight log streams from five tiny HTTP apps (structlog, python-json-logger ×2, pino-http, pino, Go `log/slog`, zap, semantic_logger), each writing JSON lines a `tail_in` follows, plus one Django app under `opentelemetry-instrument` exporting OTLP/gRPC straight to `otlp_in` with no Collector | log, metric, span | shared `tap_input` (raw line) **and** one `tap_<stream>` after `json`; `tap_django` (one tap, all three signals) | 300 s |
| `oteldemo` | The **OpenTelemetry Demo** at a pinned tag, shallow-cloned at run time, under its own Locust load generator, exporting through the demo's **own** Collector into `otlp_in` | log, metric, span | `tap_input` only — the events arrive finished from a real collector | 1200 s (+ an opt-in 180 s `resource: keep` second capture) |
| `hostagents` | collectd and Telegraf, each from its own official distribution in **default** configuration, over five wires at once: collectd binary (UDP), carbon plaintext from each agent (TCP), a Telegraf Prometheus endpoint scraped, and Telegraf OTLP/gRPC | metric | one per wire (`tap_<leg>`); no second tap — agent output arrives finished | 600 s, then a second 180 s run |
| `demo` | This repository's **own** `demo/` stack (nginx, HAProxy, Postgres, Redis, a Django app, a Celery worker, a traffic generator), config generated from `demo/logit.yaml` at run time through a compose overlay | log, span | `tap_input` and `tap_landed`, each fed by every tier | 900 s |

| producer | representativeness (verbatim) |
|---|---|
| `interop` | *recorded real producers running synthetic workloads -- grammar/packing evidence, not traffic mix* |
| `exporters` | *official exporter images in default configuration against idle single-instance services -- label/series structure per exporter; series counts scale with real object counts and are a floor* |
| `applogs` | *real logging libraries at pinned versions in their documented production configuration, in minimal apps under a synthetic request mix -- library record structure; no application-specific fields an operator would add, so widths are a floor. Django leg: SDK defaults with auto-instrumentation, no collector enrichment* |
| `oteldemo` | *OpenTelemetry Demo &lt;tag&gt;: real SDK auto-instrumentation in ~10 languages under a synthetic load generator, via the demo's own collector -- a demo app: every instrumentation enabled at once, no operator-added context, no production enrichment* |
| `hostagents` | *collectd and Telegraf with their distro/default plugin sets inside containers on an idle host -- agent packing and identity structure; device/filesystem counts, and so series counts, are a floor* (each of its two runs then names its own variant inside the line: default batching, or wire grouping) |
| `demo` | *own demo stack -- harness exercise; not evidence of production shape* |

### Caveats each author recorded

These are collected here so that a reader comparing two rows of `combine.py`'s output doesn't
have to open six files. Most also appear in the producer's own header or its `provenance.txt`;
`hostagents`' two are recorded only here.

- **`exporters`: cAdvisor isn't captured.** It was tried and couldn't start without `--privileged`
  (`inotify_add_watch /sys/fs/cgroup: permission denied`), and a survey doesn't run a privileged
  container to measure a label set. Series counts are a floor everywhere: one idle container has
  fewer block devices, filesystems, and interfaces than a real host, and `node_exporter`'s row
  scales with those. The *label structure* isn't a floor; it's the exporter's own.
- **`oteldemo`: the demo's `docker_stats` receiver is dropped**, the one deviation from the demo's
  own pipelines. It can't reach `/var/run/docker.sock` on an SELinux-enforcing host, and it
  measures the daemon rather than application SDK output. On such a host, the producer relabels
  (`chcon -R -t container_file_t`) this run's own freshly cloned copy and nothing else. Only the
  demo's **core `compose.yaml`** layer runs (its own "core/minimal" set), and even that **peaks
  around 20 GB of RAM**. Budget for that before starting a second survey beside it.
- **`applogs`: Rails and lograge were dropped.** lograge is a Rails railtie with no supported use
  outside Rails, and a `rails new` inside an image build costs minutes to measure four routes. The
  brief's own fallback, **semantic_logger**'s JSON formatter, replaced it, so the Ruby row isn't a
  Rails row. The Django leg runs on **sqlite, not Postgres**. The dbapi span comes from the same
  instrumentation either way, but sqlite's carries no network peer, so its attribute count sits at
  the low end.
- **`hostagents`: both graphite legs carry zero attributes** in the default configuration.
  Carbon's wire is one dotted name and one number, and neither agent is configured to emit tags, so
  `logit.shape.attributes` is a flat `0` there by construction. `graphite_in`'s 5 s
  `handshake_timeout` closes Telegraf's **first** graphite connection once, because Telegraf
  connects before it has anything to write. It reconnects and the capture is unaffected; the
  resulting `logit.log` line is expected, not a fault.
- **`demo`: the weakest evidence in the harness and its best exercise.** See
  [Representativeness is structural](#representativeness-is-structural), and `source-labels.json`
  for which tiers' formats this repository authored.

### `resource: keep` captures

`oteldemo`'s optional second run (`SHAPE_SURVEY_OTELDEMO_KEEP=1`) forwards the observed `Resource`
instead of dropping it. `aggregate` then keys its series per resource, which makes a
**per-service** breakdown possible: the one question a pooled capture structurally can't answer
when nine SDKs feed one gateway.

**That capture is identity-bearing and stays in its run directory.** Keeping the resource means
`service.name`, `host.name`, `container.id`, and everything else `resource_detection` stamped reach
`shape.log`, which is exactly what `resource: drop` exists to deny. The run writes to its own
`resource-keep/` subdirectory, with its own provenance saying so. `perf/results/` is gitignored,
and nothing derived from this capture belongs in a shared summary or in this repository.

It summarizes through the ordinary `summarize.py` path. That works because `summarize.py`'s
`parse_attrs` knows the whole `render_value` grammar: arrays, maps, nested ones, `<N bytes>`, and
strings carrying the delimiters. A keep capture needs it, because `process.command_args` rides on
the resource of every OTel SDK that detects a process, rendered as a bare array
(`["/usr/bin/node", "--require=…", …]`) that contains spaces without being quoted. An earlier
parser split an `attrs` line as if only a quoted value could contain a space, walked into that
array, and asserted. `--self-test` now carries a structurally verbatim keep-capture line, with
neutral values, so this can't regress.

`summarize.py`'s series key is still the metric name plus `signal`/`source`/`tap`. It has no room
for a resource, so its tables stay **pooled** across services. The per-service split is the
producer's own `per-service.md`, and that file names services.

## Two surveys at once

**Two `script/shape-survey <producer>` invocations may run at the same time on one docker
daemon.** Producers are written by different people and a capture takes minutes to a quarter of an
hour, so that's the normal case, not an edge one. Everything a run creates is namespaced by
*producer*:

| Resource | Name |
|---|---|
| network | `shape-survey-<producer>-net` |
| containers | `shape-survey-<producer>-<suffix>` (`survey_container_name`) |
| compose project | `shape-survey-<producer>-<suffix>` (`survey_compose`) |
| run directory | `perf/results/shape-survey/<producer>/<UTC timestamp>/` |

`survey_cleanup` only names resources from this invocation's own bookkeeping, so it can't touch
another run's, or another session's.

The one shared resource is the image tag `logit:shape-survey`. `survey_image` takes an `flock`
around the build so two invocations can't build it at once. For the second invocation, set
`SHAPE_SURVEY_SKIP_IMAGE=1`: it reuses the already-built image, and fails loudly if there isn't
one, rather than re-tagging it underneath a running survey.

The network alias `logit`, and each service's own alias, stay unscoped on purpose. They live
*inside* one producer's network, where nothing can collide with them.

The daemon is shared with other work besides surveys, so these rules always apply:

- Never prune.
- Never remove a resource this run didn't create.
- If `docker network create` fails with *"all predefined address pools have been fully
  subnetted"*, **stop and report it**. The fix is somebody else releasing a network, not this
  harness deleting one.

## What comes out

Each run writes one directory, `perf/results/shape-survey/<producer>/<UTC timestamp>/` by default
(`/perf/results/` is already gitignored):

| File | What it is |
|---|---|
| `shape.log` | the capture — `file_out`'s human render of every `logit.shape.*` measurement |
| `logit.log` | the `logit` container's own output for the run |
| `provenance.txt` | date, this repo's SHA and dirty state, docker and image ids, the producer's own software versions, and its **representativeness** line |
| `summary.json` | every series: count, min, max, mean, nearest-rank p50/p90/p99, and an exact value→count table for integer-valued series |
| `summary.md` | the same, as human tables |
| *(producer artifacts)* | for example, `demo`'s generated `logit.yaml`, `compose.env`, `source-labels.json` |

**Raw captures never enter the repo, and nothing here writes under `testdata/`.** `shape` itself
emits counts and lengths only, never a key, a value, a log body, or a metric name from an observed
event. That's what lets a summary leave an environment the traffic couldn't.

## How to read `summary.md`

- **Start with the banner.** Every summary opens with its run's representativeness line and, for a
  producer that supplies one, a per-source table saying where each source's *format* came from.
  It's not a footnote to skip; see
  [Representativeness is structural](#representativeness-is-structural).
- **Percentiles are nearest-rank** over the whole capture: every flush window's raw samples
  concatenated, not a per-window average of summaries. A p99 prints `n/a` below 100 values and a
  p90 below 10, and every table carries its own value count. The plan's rule is stricter for
  anything that lands in `docs/design/data-shapes.md`: quote a p99 only with ≥100k events behind
  it.
- **`logit.shape.attributes` at `tap_input` vs `tap_landed`** is the two-tap pattern: the same
  events measured straight off the input and again after that source's realistic transform chain.
  The difference is what parsing and enrichment added, measured rather than assumed.
- **`logit.shape.batch.events`** is events per batch. With `receive.batch_max_events: 1`, that's
  the *wire's* own grouping (one datagram, one framing decision, one scrape) rather than the
  `BatchAccumulator`'s default regrouping. `otlp_in` and `prometheus_in` bypass the accumulator, so
  theirs is always the wire's.
- **The cumulative gauges** (`distinct_keys`, `distinct_keysets`, `keyset_share.top1`/`top5`,
  `tracking_overflow`) count since process start, not per window, and cover top-level keys only.
  `tracking_overflow` at `1` means a cap was hit and the key and key-set numbers below it are
  floors.
- **A sketched series is a failure, not a row.** If `aggregate` ever falls back past
  `max_samples_per_series`, `summarize.py` exits non-zero and names the series instead of
  reporting an approximation as a measurement.

## Representativeness is structural

Every producer must pass a one-line representativeness statement to `survey_provenance`. It lands
in `provenance.txt`, and `summarize.py` prints it as the banner at the top of `summary.md`, above
any number. That placement is the point: these tables get quoted and pasted, and a measurement of
a stack this project built to demonstrate itself says something very different from a measurement
of somebody else's producer.

Two current examples, and what each is worth:

- **`interop`**: *"recorded real producers running synthetic workloads — grammar/packing
  evidence, not traffic mix."* Real third-party senders (rsyslog, collectd, a real Prometheus,
  Datadog's client, the OpenTelemetry Collector), each recorded for a few seconds against a small
  synthetic workload. It's good evidence about grammar, carriers, and how a client packs a
  datagram, and no evidence at all about volume or mix.
- **`demo`**: *"own demo stack — harness exercise; not evidence of production shape."* The best
  end-to-end exercise of the harness, and the weakest evidence in it. The stack exists to
  demonstrate `logit`. Its tiers were chosen to show components off, and parts of it are
  configured for visibility rather than the way an operator would run them
  (`log_min_duration_statement=0`, minute-scale rotation, one synthetic request generator).

**This repository authored some of the formats in `demo/`.** nginx's JSON `log_format` and the
Django app's and Celery worker's logging configs are ours, and measuring the width of an event
whose field list we chose is circular. HAProxy's `option httplog` line, Postgres's `jsonlog`,
Redis's server log line, and Docker's json-file envelope are those projects' own formats,
configured on but not designed here. The `demo` producer labels every tier with which it is
(`source-labels.json`, rendered as a table above the distributions), so a reader never has to
remember. A producer whose sources are uniform can skip that file; one whose sources aren't should
write it.

## Adding a producer

Write **one file**, `producers/<name>.sh`, defining `survey_<name>()`, plus any config it needs
under `configs/`. `script/shape-survey` discovers producers by globbing `producers/*.sh` (a `-` in
the filename maps to `_` in the function name), so there's no list to join, and two people can add
a producer in parallel without touching the same file.

**Nothing producer-specific goes in `lib.sh` or in the dispatcher.** If a producer needs something
they don't have, add it as a general facility, the way `survey_on_cleanup` is general rather than
"tear down the demo stack".

`lib.sh` provides these functions:

| Function | What it does |
|---|---|
| `survey_out_dir <producer>` | creates the run directory and sets `SURVEY_RUN_DIR` (call it plainly, never in `$( )` — a subshell would throw the assignment away) |
| `survey_provenance <producer> <representativeness>` | writes `provenance.txt`'s common half; append your own software versions to the same file |
| `start_logit <config> [docker run args…]` | `logit validate`s the config in the image, runs it on the producer's network under the alias `logit` with `/out` mounted, and waits on `logit ready`. Extra args are passed through (for example, an `-e` a config resolves with `!env`) |
| `stop_logit` | `docker stop -t 60` (so the final flush lands), captures `logit.log`, fails if `shape.log` is missing or empty |
| `survey_start_service <suffix> [--ready-cmd '<cmd>' \| --ready-log '<regex>' \| --ready-http <url>] [--ready-timeout <s>] -- <docker run args…>` | starts a long-lived **service under test** on the producer's network under the alias `<suffix>`, with a bounded readiness wait (never a blind sleep), its image and digest appended to `provenance.txt`, and its log captured at teardown |
| `survey_service_logs <suffix>` | that service's log into `service-<suffix>.log` — automatic at cleanup, callable mid-run |
| `survey_capture_for <seconds>` | holds the capture open for a fixed window, progress line every 30 s |
| `survey_capture_until '<cmd>' <timeout>` | polls `<cmd>` (through `eval`, so a shell function name works) until it succeeds; a timeout is mandatory and failing it fails the run |
| `survey_compose <project-suffix> <compose global args…> -- <compose args…>` | `docker compose` under project `shape-survey-<producer>-<suffix>`, with the "somebody else's stack is already up" guard and `down -v --remove-orphans` teardown registered on first use |
| `survey_python <suffix> <docker args> -- <cmd>` | runs `replay.py`/`summarize.py`/`check_interop.py`/a generated script in a throwaway `python:3.12-slim` with `/tools` and `/out` mounted |
| `survey_summarize [--append <file in the run dir>]` | `summarize.py` over the run's `shape.log`, with the banner, any `source-labels.json`, and optionally your own markdown section on the end |
| `survey_on_cleanup <snippet>` | an idempotent teardown hook for a producer whose lifecycle isn't "remove these containers" |
| `survey_track <name>` | register a container you started yourself for cleanup |
| `survey_container_name <suffix>` / `survey_project_name <suffix>` | the namespaced names; never build one any other way |

`survey_begin <producer>` and `survey_cleanup` bracket each producer. They're the dispatcher's
business, not a producer's.

A new producer inherits these rules:

- **Name everything `shape-survey-*`** (the image is `logit:shape-survey`), so cleanup enumerates
  exactly what this run created. The shared-daemon rules in
  [Two surveys at once](#two-surveys-at-once) apply.
- **Never modify the thing you're measuring.** The `demo` producer generates its config from
  `demo/logit.yaml` at run time and mounts it through a compose overlay. `demo/` itself is
  untouched, and the generated config is a run artifact, not a committed second copy that would
  have to be kept in step by hand.
- **Fail loudly.** A config that doesn't validate, a replay that couldn't send, an empty
  `shape.log`, or a sketched `logit.shape.*` series stops the run. A survey that quietly produces a
  thin summary is worse than one that fails, because a thin summary looks like data.

### Your own summary section

`summarize.py` stays the general engine: it knows about series, percentiles, and value→count
tables, and nothing about what a source *is*. A reading that needs to know, such as "series per
scrape, per exporter", belongs to the producer and goes in through `--append`:

```sh
survey_summarize                        # writes summary.json first
...generate ${SURVEY_RUN_DIR}/section.md from summary.json...
survey_summarize --append section.md    # and again, with the section on the end
```

`summary.json` carries the **full value→count table** for every integer-valued series, so a
producer's section script computes whatever fraction or median it needs from that file and never
re-parses `shape.log`.

### Combining runs

`combine.py` reads N run directories' `summary.json` and `provenance.txt` and emits one markdown
table per dimension: the attribute widths, the per-batch series, the `values.*` type mix as
percentages, the counters, and the cumulative gauges. Each table has one row per producer × source
× tap × signal, and each row carries its run's representativeness line.
[`docs/design/data-shapes.md`](../../docs/design/data-shapes.md) is written from this output.

```sh
python3 tools/shape-survey/combine.py perf/results/shape-survey/*/*/ --out /tmp/combined.md
python3 tools/shape-survey/combine.py --self-test
```

### The config pattern

Everything under `configs/` is ordinary `logit` YAML and joins the shipped-config globs
(`script/validate`, `every_shipped_config_loads_and_validates`). That way a component field rename
can't leave behind a capture config that fails only the next time somebody runs a 15-minute survey
with it.

```text
<input> ─┬─> tap_input   (shape, straight off the input)          ─┬─> shape_rollup ─> shape_out
         └─> <realistic chain> ─> tap_landed (shape)              ─┘   (aggregate)     (file_out)
```

- **Two taps where there's a real chain to measure across, one where there isn't.** A metrics
  input whose events arrive finished has nothing an operator would realistically parse out of it
  afterward, and a second tap there measures `shape` measuring an identity function. Say so in a
  comment rather than leaving the asymmetry unexplained.
- **`receive.batch_max_events: 1`** on accumulator-fronted inputs, so a batch is a wire group.
- **`aggregate` with `distributions: samples`** and a `max_samples_per_series` far above what the
  capture can produce. That's what keeps `shape`'s raw observations raw through the window.
- **`file_out` with a huge `rotate.max_bytes`.** `file_out` requires a rotation trigger, and a
  survey that rotated would silently lose the start of its own capture.
- **An `admin:` block**, because `start_logit` waits on `logit ready` rather than sleeping.

## The acceptance test

`check_interop.py` re-derives, straight from `testdata/interop/statsd/*.raw` with its own parser,
the two numbers `shape` should report for that corpus: events per datagram and attributes per
event. It then asserts they match. It imports nothing from `summarize.py` and copies nothing from
`testdata/interop/statsd/README.md`. A shared parser would let a parser bug cancel itself out, and
a number copied from a document would only prove the document and the checker agree.

The mapping is deliberately not 1:1, and that's the interesting part. `statsd_in` emits one event
per *value* on a `c`/`g` line and one per *line* for `ms`/`h`/`d`/`s`. On top of the wire tags, it
stamps `statsd.container_id` and `statsd.timestamp` when the line carries those segments, and
`statsd.type` on `ms`/`h`/`d` lines only. The script's docstring spells out all of it.

**If this check ever fails, stop.** Don't adjust the check to pass. The instrument, the decoder, or
the derivation is wrong, and you have to establish which before any survey result built on `shape`
means anything.
