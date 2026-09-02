# Deploying `logit`

How to run `logit` outside this repo's own dev stack: get the image, run it against a config, and
what to expect from `logit`'s signal and restart behavior once something under it (a sink, a signal
from an orchestrator) doesn't cooperate. For the nginx-specific side of pointing a real nginx at a
running `logit`, see [the nginx-side recipe](#the-nginx-side-recipe) below.

## Getting the image

`script/image [tag]` builds the production runtime image from `Dockerfile` (not `Dockerfile.dev`,
which is the contributor dev environment — [ADR 0005](adr/0005-containerized-development.md)) and
tags it `logit:<tag>` (default `local`):

```sh
script/image        # -> logit:local
script/image v0.1.0  # -> logit:v0.1.0
```

There's no published image to pull yet — no registry push step exists in this repo today — so
"build" is the operative word here, not "pull." Build it wherever you intend to run it, or push the
result to your own registry.

## Running it

The image's `ENTRYPOINT` is `["logit"]`, so a config path is the whole invocation. Config is a
read-only bind mount, not baked into the image:

```sh
docker run --rm \
  -v /path/to/config.yaml:/config.yaml:ro \
  -e INFLUXDB_TOKEN=... \
  logit:local run /config.yaml
```

Secrets and deployment-specific values (a token, a URL, a bind address) go through `!env VAR_NAME`
in the config rather than being inlined — see [ADR 0011](adr/0011-env-yaml-tag.md) for the full
mechanism and its edge cases (any field on any component can use it, not just `influxdb_out`'s
`token`). Pass the corresponding environment variables to the container with `-e` or `--env-file`.

## `logit validate` as a preflight

Before restarting a running `logit` with a new config, validate the candidate first:

```sh
docker run --rm \
  -v /path/to/new-config.yaml:/config.yaml:ro \
  -e INFLUXDB_TOKEN=... \
  logit:local validate /config.yaml
```

`validate` shares the exact same resolution and validation path `run` uses
(`graph::resolve`, invoked from `validate_semantics` in
`crates/logit-cli/src/pipeline.rs`) — a config that validates cleanly is guaranteed not to fail at
that stage when actually run. It still needs every `!env` reference in the config to resolve, the
same as `run` does, so pass the same environment.

## Signal and restart behavior

Covered in full by [ADR 0013](adr/0013-service-lifecycle-and-output-retry.md); the operator-facing
summary:

- **SIGTERM or SIGINT triggers a graceful drain**, not an immediate kill. Every listener's inbox
  closes the same way it would if the listener finished on its own, which flushes any in-flight
  `aggregate` window before the process exits — an unattended restart (a container orchestrator
  sending SIGTERM ahead of SIGKILL) doesn't silently drop a window's worth of metrics.
- **A second signal during a wedged drain exits immediately**, with status 130 — a drain that's
  stuck stays killable by the same signal that started it, which matters once a restart policy,
  not a person at a terminal, is what's waiting on the process to exit.
- **A sink failure — transient or extended — no longer ends the process by default.** Every sink
  now sits behind a decoupled delivery buffer with its own retry budget
  ([ADR 0020](adr/0020-buffered-sink-delivery.md), revising ADR 0013's retry-budget rationale
  without superseding its other decisions); see [Sink delivery buffering](#sink-delivery-buffering)
  below for the full failure and sizing story, including the one case that still exits the process
  (a sustained, purely-configuration-error failure).

## Sink delivery buffering

Every sink (`influxdb_out`, `stdio_out`, ...) sits behind a per-component, in-memory delivery
queue that decouples receiving events from delivering them
([ADR 0020](adr/0020-buffered-sink-delivery.md)). This is what lets a slow or temporarily-down
destination be ridden out instead of stalling or killing the whole pipeline. It's tunable per sink
via a `buffer:` block on that component (`buffer:` is rejected at validation time on anything but a
sink) — see the commented example in
[`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml). Every field defaults, so
an omitted `buffer:` is the values below.

### Failure semantics: degrade to dropping, don't exit

Unlike the pre-0020 behavior, a sink that can't reach its destination no longer ends `logit run`:

- A **retryable** failure (per the sink's fault classification and delivery posture) is retried
  within `retry_budget` (60s by default) before the batch is dropped and counted.
- A **non-retryable** failure (including retry-budget exhaustion) drops the batch, counts it, and
  logs a throttled warning — the writer moves on to the next batch. The rest of the pipeline, and
  every other sink, keeps running.
- **The one exception:** if a sink sees *nothing but* configuration-error failures (a bad token, a
  bad bucket — the kind no amount of retrying fixes) for a sustained ~60-second window with no
  intervening success, `logit run` exits. This is deliberate — a genuinely misconfigured sink
  should still fail loudly enough for a restart-policy supervisor to notice, rather than silently
  dropping every batch forever. A destination that's merely slow or temporarily down never trips
  this; only a failure `logit` can tell is a configuration problem does.
- On SIGTERM/SIGINT, each sink gets up to `shutdown_grace` (5s by default) to drain its queue
  before the process exits; whatever's still queued past that deadline is dropped and counted, not
  held onto indefinitely.

### Sizing: `max_bytes` × number of sinks

`buffer.max_bytes` (64MiB default) bounds *one sink's* queue — a config with several sinks (or
several `influxdb_out`/`stdio_out` components fed by different branches) multiplies that by however
many sinks it defines when you're sizing the container's memory limit. `buffer.max_batches` (1024
default) is the second, independent bound — whichever of the two trips first governs. Size for the
worst case you actually intend to ride out: `max_bytes` deep enough to hold a real destination
outage's worth of buffered data, weighed against the memory budget you're willing to commit to a
sink that's doing nothing but holding data no one can currently accept.

`buffer.overflow` decides what happens once both bounds are full: `block` (the default) applies
backpressure all the way back to intake rather than losing data silently; `drop_oldest`/
`drop_newest` trade data loss for keeping intake unblocked — pick one deliberately per sink rather
than leaving the default in place for a destination you know is unreliable.

### What to watch

Every component already exposes its own delivery metrics once telemetry is wired to `internal`
(`docs/design/internal-telemetry.md`) — no separate opt-in beyond adding an `internal` component to
the config. The two most directly actionable for buffering:

- `logit.component.buffer.utilization` (gauge) — the fill ratio of whichever of `max_batches`/
  `max_bytes` is closer to tripping. Sustained values near 1.0 mean a sink is falling behind its
  destination; under `block`, that's also back-pressuring intake.
- `logit.component.batches.dropped` (count, tagged `reason`) — `overflow_oldest`/`overflow_newest`
  (a `drop_*` policy actually dropped something), `send_failed` (retry gave up on a batch),
  `shutdown` (the queue still held data when `shutdown_grace` expired). Any sustained nonzero rate
  here is data loss worth alerting on; which `reason` tells you whether the cause is an overflowing
  queue, a failing destination, or a slow drain racing shutdown.

## The nginx-side recipe

Concrete, working reference config lives in this repo: [`examples/nginx/nginx.conf`](../examples/nginx/nginx.conf)
(the `access_log`/`log_format` directives) and
[`examples/nginx-to-influxdb.yaml`](../examples/nginx-to-influxdb.yaml) (the `logit` side —
`syslog_in` → `json` → `kv_metrics` → `keep` → `aggregate` → `influxdb_out`, plus `stdio_out` for
visibility). Point at those directly rather than re-deriving the log-format syntax here; this
section is the operational notes around using them against a real nginx, not a restatement of their
contents.

### Which directives to add

Two `log_format`s and two `access_log` lines per `server {}` block, as in
`examples/nginx/nginx.conf`: a lean, `escape=json` format containing exactly the fields your
`kv_metrics` component reads, sent over syslog/UDP —

```nginx
access_log syslog:server=<logit-host>:5140,tag=nginx_access,nohostname access_json_syslog;
```

— and, during cutover, the existing verbose stdout format left in place alongside it as a second
`access_log` line (nginx allows more than one per block). `error_log` needs no change: it stays
nginx's own non-JSON format, out of scope here.

### Why keep the existing stdout destination during cutover

The second `access_log` line isn't a permanent duplicate — it's a safety net for the transition.
Point `logit` at the syslog line while leaving the verbose stdout line running unchanged, confirm
metrics are landing where you expect (a `stdio_out` block per request, a Grafana/InfluxDB query
against the fields your `kv_metrics` component derives, or whatever verification your environment
uses), and only then drop the stdout line once the `logit` path is trusted. Running both costs
nothing but a slightly larger nginx log volume during that window.

### The syslog message-size limit and its symptom

`docs/known-gaps.md` has [the full write-up](known-gaps.md) of what happens when a syslog-bound
access log line gets too large to fit in one datagram — worth reading in full since the actual
finding is more reassuring than it sounds at first: nginx's own `large_client_header_buffers`
rejects an oversized request with a 400 before nginx ever builds a log line for it, which closes off
the specific "attacker sends a huge `Host` header" vector by nginx's own default behavior, not
anything `logit` does. The pipeline's graceful degradation on a truncated line either way (a
different unbounded field, a larger `large_client_header_buffers`, a different syslog client) was
verified directly by sending a hand-truncated datagram straight to `syslog_in`, bypassing nginx
entirely.

Concretely, if a syslog datagram does truncate mid-JSON-object for any reason, here's what it looks
like in `logit`'s own output — not a crash, not a stuck listener:

- `stdio_out` shows a log-only block: the raw (truncated) message and its `syslog.*` attributes,
  with none of the JSON body's fields merged in.
- stderr gets a throttled `parse_failure` diagnostic naming the `json` component that failed to
  parse it.
- Any *fieldless* counter (`nginx.requests` in the reference config, which counts every event
  regardless of attributes) still increments for that request. Any metric that reads a field out of
  the JSON body (`nginx.bytes_sent`, the two distributions) derives nothing for it, since there's no
  field to read.
- Sibling requests before and after are unaffected — the blast radius is exactly the one truncated
  line.

### The ordering rule

Start `logit` and confirm it's actually listening *before* pointing nginx's `access_log syslog:`
directive at it. UDP is fire-and-forget: a line nginx sends before `logit`'s listener is bound is
gone, with no error anywhere — not in nginx, not in `logit`.

`logit run` prints nothing on a clean, successful start — no output at all is itself the
confirmation that startup didn't hit an error. To confirm the listener is actually bound and
accepting, send it a manual line and watch for the corresponding `stdio_out` block:

```sh
logger -n <logit-host> -P 5140 -d -t smoke '{}'
```

(`-d` forces UDP — `logger`'s default without `-T`/`-d` depends on `/etc/services`, which isn't
reliably UDP-first everywhere.) A `stdio_out` block appearing for that line means the listener is up
and reachable; nothing appearing means nginx pointing at it next would just be feeding the same
fire-and-forget void.
