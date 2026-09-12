# Deploying `logit`

How to run `logit` outside this repo's own dev stack: get the image, run it against a config, and
what to expect from `logit`'s signal and restart behavior once something under it (a sink, a signal
from an orchestrator) doesn't cooperate. For the nginx-specific side of pointing a real nginx at a
running `logit`, see [the nginx-side recipe](#the-nginx-side-recipe) below. If you just want to see
`logit` running rather than deploy it, [`demo/`](../demo/README.md) is a self-contained
`docker compose up` — no image-building steps to follow.

## Getting the image

`script/image [tag]` builds the production runtime image from `Dockerfile` (not `Dockerfile.dev`,
which is the contributor dev environment — [ADR `containerized-development`](adr/containerized-development.md)) and
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
in the config rather than being inlined — see [ADR `env-yaml-tag`](adr/env-yaml-tag.md) for the full
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

`validate` doesn't check that a referenced *file* actually exists or parses — `lua_file`, a
`stdio_out`/`file_out` path, and `otlp_out`/`otlp_in`'s `tls.*_file` fields are all read only once
`run` actually constructs the component. A typo'd `tls.ca_file` path passes `validate` and fails at
startup instead, with the path in the error.

## Signal and restart behavior

Covered in full by [ADR `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md); the operator-facing
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
  ([ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md), revising ADR `service-lifecycle-and-output-retry`'s retry-budget rationale
  without superseding its other decisions); see [Sink delivery buffering](#sink-delivery-buffering)
  below for the full failure and sizing story, including the one case that still exits the process
  (a sustained, purely-configuration-error failure).

## Probes and exit codes

`logit` distinguishes three outcomes on exit, and (when `admin:` is configured) answers a
readiness/liveness probe live — see [ADR `admin-readiness-endpoint`](adr/admin-readiness-endpoint.md) for the design.

| Exit code | Meaning |
|---|---|
| `0` | Clean shutdown — a signal arrived, every listener drained, every sink flushed. |
| `1` | A startup failure — a bad config, a port already in use, a bad `lua_file`, a bad `--log-level`. Nothing was ever running. |
| `2` | A runtime failure after the process reported ready — a sustained, purely-configuration-error sink failure (see [Sink delivery buffering](#sink-delivery-buffering) below), a listener's accept loop dying. |
| `130` | A second SIGTERM/SIGINT arrived before a graceful drain finished. |

Enable the probe endpoint with a top-level `admin:` block:

```yaml
admin:
  bind: 0.0.0.0:9600
```

`GET /readyz` returns `200 ok` once every listener and every listening sink is bound and every node
task is running, `503
starting` before that, `503 draining` after a shutdown signal, and `503 degraded` if any node has
exited with an error while the process is still draining. `GET /healthz` returns `200 ok`
whenever the admin task itself can still answer, regardless of the pipeline's own state. Add
`?format=json` to `/readyz` for `{status, since, components: {id: "pending"|"bound"|
"running"|"finished"|"failed"}}` instead of the bare status word; `/healthz?format=json` returns
just `{status}`, since it has nothing else to report. No TLS, no auth — this is a
loopback/pod-local endpoint by design, not one meant to cross a real network boundary.

A Kubernetes deployment maps naturally onto the two routes:

```yaml
readinessProbe:
  httpGet: { path: /readyz, port: 9600 }
  periodSeconds: 5
livenessProbe:
  httpGet: { path: /healthz, port: 9600 }
  periodSeconds: 10
```

`logit ready [--admin http://127.0.0.1:9600]` is the probe helper `Dockerfile`'s `HEALTHCHECK`
uses — the shipped image is `bookworm-slim` with no `curl`, so this is what a container-level
health check runs instead:

```dockerfile
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s CMD ["logit", "ready"]
```

It exits 0 and prints the status word on `200`; anything else exits 1, printing the status word
the server returned or — with nothing listening at all, e.g. `admin:` was never configured — the
connection error instead.

### What to watch

- `/readyz` flipping to `503 degraded` and staying there means a node has actually failed, not
  merely that a sink is retrying — see [Sink delivery buffering](#sink-delivery-buffering)'s own
  failure semantics for what does and doesn't trip that.
- An orchestrator that never sees `/readyz` return `200` within its own startup timeout has a
  listener — or a listening sink like `prometheus_out` — that can't bind (check the
  `starting`/`bound`/`ready` lifecycle log lines below) or a Lua script that fails to load.

## Self-logging

`logit run` emits leveled, structured self-diagnostics through `tracing`
([ADR `tracing-for-self-logging`](adr/tracing-for-self-logging.md)) — `schema`/`validate`/`graph` stay print-only, since they
run once and exit.

```sh
logit run /config.yaml --log-level info --log-format text   # the defaults
logit run /config.yaml --log-level debug                    # or LOGIT_LOG=debug
logit run /config.yaml --log-format json                    # one JSON object per line
```

`--log-level`/`LOGIT_LOG` takes `tracing`'s `EnvFilter` syntax — a bare level (`info`, `debug`) or
a per-module override (`logit_pipeline=trace,info`). `--log-format json` emits one JSON object
per line with `timestamp`, `level`, `target`, `component`, `key`, and `message` fields, for a log
collector to parse directly rather than scraping text.

Every *component-scoped* self-diagnostic carries a `component` field naming which component
reported it, and (for a throttled diagnostic, or a component-owned lifecycle message like
`bound`/`recovered`) a `key` naming *why*. The process-level lifecycle events below (`starting`,
`ready`, `shutdown signal received`, `drain complete`, `exiting`) carry neither — they are about
the process, not any one component. Lifecycle events are stable, `&'static str` names — safe to
alert on directly:

| Event | Level | When |
|---|---|---|
| `starting` | info | Config loaded, before graph resolution — named even if the config goes on to fail. |
| `bound` | info | One component's socket opened, during the pre-bind pass — listeners (`syslog_in`/`statsd_in`/`collectd_in`/`otlp_in`; `tail_in`/`docker_in` emit none) and sinks that listen (`prometheus_out`). A `collectd_in` (or any UDP listener) whose `bind` names a multicast group says so, naming the group it joined. |
| `ready` | info | Every socket bound, every node task running, nothing has failed. |
| `shutdown signal received` | info | A SIGTERM/SIGINT arrived. |
| `drain complete` | info/warn | Every node has exited after a shutdown or failure — `warn` if any batch was dropped mid-drain. |
| `degraded` | warn | A sink's first dropped batch (its retry budget exhausted) since it was last healthy. |
| `recovered` | info | A sink's first successful delivery after `degraded`. |
| `exiting` | info/error | The process is about to exit — `info` at `0`, `error` at any failure code (`1` or `2`). A config error that fails before the pipeline starts exits without this line. |

`internal`'s own `logs:` setting (`warn` by default, `error`, or `off`) routes every `warn`-or-above
self-diagnostic into the pipeline as an ordinary log event, alongside its existing points and
spans — see [`docs/design/internal-telemetry.md`](design/internal-telemetry.md)'s "Logs" section. A sink already attached to
`internal` (or a downstream `keep`/`aggregate`/`lua`) carries `logit`'s own self-logs the same way
it carries any other signal, with no separate log-shipping setup.

## Sink delivery buffering

Every sink (`influxdb_out`, `stdio_out`, `file_out`, ...) sits behind a per-component delivery
queue, in memory by default, that decouples receiving events from delivering them
([ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md)). This is what lets a slow or temporarily-down
destination be ridden out instead of stalling or killing the whole pipeline. It's tunable per sink
via a `buffer:` block on that component (`buffer:` is rejected at validation time on anything but a
sink) — see the commented example in
[`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml). Every field defaults, so
an omitted `buffer:` is the values below. `buffer.disk:` opts a sink into a crash-recoverable,
disk-backed queue instead — see [Durable buffering](#durable-buffering) below.

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

`buffer.max_bytes` (64MiB default) bounds *one sink's* queue — RAM for the in-memory default, disk
for `buffer.disk:` — a config with several sinks (or several `influxdb_out`/`stdio_out` components
fed by different branches) multiplies that by however many sinks it defines when you're sizing the
container's memory (or disk) limit. `buffer.max_batches` (1024 default) is the second, independent
bound for an in-memory queue — whichever of the two trips first governs; a disk-backed queue drops
that bound entirely in favor of `buffer.disk.max_bytes` alone (graph validation rejects setting
both). Size for the worst case you actually intend to ride out: `max_bytes` deep enough to hold a
real destination outage's worth of buffered data, weighed against the memory (or disk) budget
you're willing to commit to a sink that's doing nothing but holding data no one can currently
accept.

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

### Durable buffering

An in-memory queue is lost on a process restart, a `SIGKILL`, or a shutdown grace that expires
mid-drain. A `buffer.disk:` block replaces one sink's queue with a crash-recoverable spool on disk
([ADR `disk-backed-sink-buffer`](adr/disk-backed-sink-buffer.md)) — a restart resumes delivery from
the last persisted read cursor, replaying at most the batches committed since the last checkpoint
(at-least-once, the same trade `tail_in`'s own checkpoint already makes):

```yaml
buffer:
  disk:
    path: spool/influxdb_out   # required, resolved relative to this config file's own directory
    max_bytes: "1GiB"          # bound on the sum of on-disk segment sizes -- replaces buffer.max_bytes
    segment_bytes: "64MiB"     # soft rotation trigger, not a hard cap
    compression: none          # none | lz4
    checkpoint_interval: 1s    # how often the read cursor is persisted during normal operation
```

Use it for a sink whose destination has outages long enough, or restarts frequent enough, that an
in-memory queue's loss window is a real cost — not for every sink by default: it costs a real
`write` per batch (`logit_proto::native` encode plus one file append) that an in-memory queue never
pays. `buffer.max_batches`/`buffer.max_bytes` are rejected if left non-default alongside `disk:` —
disk replaces the in-memory bound, it doesn't size beside it.

**Durability level:** `fdatasync` on segment rotation, on the cursor file, and at shutdown, not per
push. A process crash (including `SIGKILL`) loses nothing already written; a genuine power loss can
lose the most recent, not-yet-synced tail of the active segment. Put the spool directory on a
volume that survives the container — an ephemeral container filesystem defeats the entire point,
the same as any other durable state (`crates/logit-inputs/src/tail/checkpoint.rs`'s own checkpoint
file, a database's data directory).

**What to watch**, in addition to the metrics above (`buffer.utilization`/`.bytes` mean the same
thing, sized against `buffer.disk.max_bytes`; `batches.dropped` gains `frame_too_large`,
`disk_corrupt`, `disk_full`, and `disk_io_error` as possible `reason`s, and never emits
`reason="shutdown"` for a disk-backed sink, which drops nothing at shutdown):

- `logit.component.buffer.disk.segments` (gauge) — segment files currently on disk.
- `logit.component.buffer.disk.replayed` (count) — records found between the resume point and the
  end of all segments, once at process start. Consistently zero after the first tick following a
  clean start; a nonzero value on every restart under normal operation means something is
  preventing the queue from ever fully draining.
- `logit.component.buffer.disk.truncated` (count) — a torn tail found and truncated at open. Any
  nonzero value here means the previous process ended mid-write (an ordinary `SIGKILL`, not
  necessarily a problem) — worth noting, not alerting on by itself.

## Listener intake

Every UDP listener (`statsd_in`, `collectd_in`, `syslog_in`) sits in front of a per-component, in-memory receive
queue that decouples reading the socket from decoding and batching what it received
([ADR `decoupled-listener-io`](adr/decoupled-listener-io.md)) — the listener-side sibling of the sink delivery
buffering above. This is what lets a slow or backed-up destination downstream be ridden out without
the socket itself going unread. It's tunable per listener via a `receive:` block on that component
(`receive:` is rejected at validation time on any kind but a datagram listener or a tail listener
(`tail_in`/`docker_in`) — and a tail listener has no receive *queue* at all, so only its four
batch-assembly fields apply; see "Tailing files and Docker logs" below) — see the commented example
in [`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml). Every field defaults,
so an omitted `receive:` is the values below.

### Failure semantics: `drop_oldest`, not `block` — the opposite default from `buffer:`

`buffer:`'s default is `block`, and that's the right call there: the producer being backpressured
is an in-process drain that can afford to wait. `receive:`'s default is `drop_oldest`, and that's
deliberately the opposite call, for a reason worth understanding rather than just remembering: the
producer behind a UDP listener is the kernel's socket receive buffer, which *cannot* wait. Setting
`receive.overflow: block` doesn't prevent loss under sustained overload, it just relocates it from a
place `logit` can count and report (`logit.component.datagrams.dropped`) to a place it can't see at
all (the kernel silently discarding into a counter this process never reads). Every mature UDP
listener in the field — syslog-ng, rsyslog, Telegraf, gostatsd — treats this the same way. Leave
`overflow` at its default unless you have a specific reason to want backpressure to propagate all
the way back to the sender instead.

- `overflow: drop_oldest` (the default) or `drop_newest` both keep the socket being read
  unconditionally, evicting from the queue instead. `drop_oldest` favors fresh data over stale under
  sustained overload; `drop_newest` favors whatever's already queued, at the cost of losing an
  entire burst's tail once the queue fills, since a full queue then stays full.
- `overflow: block` genuinely stops `recv_from` once the queue is full — the one configuration
  under which this listener can itself apply backpressure, and the one place `receive.push.blocked.
  duration` (below) actually records anything.
- On SIGTERM/SIGINT, the listener gets up to `receive.shutdown_grace` (5s by default) to decode and
  deliver whatever's still queued before the process exits; whatever's still queued past that
  deadline is dropped, uncounted (nothing is left running to count it once the grace expires).

### Sizing, and `SO_RCVBUF`

`receive.max_bytes` (32MiB default) and `receive.max_datagrams` (10,000 default) bound the receive
queue itself — undecoded bytes, not decoded events — whichever trips first. `receive.
batch_max_events`/`batch_max_bytes` (1,000 / 1MiB default) are a second, independent bound one
layer downstream: how much a listener accumulates across datagrams before sending one batch on,
capped by `batch_flush_interval` (100ms default) regardless of size so a quiet listener never stalls
data waiting to fill a batch.

`receive.receive_buffer_bytes` requests a specific `SO_RCVBUF` at bind time (omitted, the default,
leaves the kernel's own default alone). Linux doubles whatever you request for its own bookkeeping,
so a successful request routinely reports back roughly 2× what was asked — `logit` accounts for
that when deciding whether to warn. If you do set this and see a startup warning naming
`net.core.rmem_max`, that sysctl is clamping the request below what you asked for; raise it to get
the full requested size. The granted value is always gauged
(`logit.input.receive_buffer.bytes`), even when you never set an override, so you can see the
kernel default before deciding whether to raise it.

### What to watch

- `logit.component.receive.utilization` (gauge) — the fill ratio of whichever of `max_datagrams`/
  `max_bytes` is closer to tripping. Sustained values near 1.0 mean decode is falling behind the
  socket; under `block`, that's also back-pressuring the sender (or, for a local process, the OS).
- `logit.component.datagrams.dropped` / `.bytes.dropped` (count, tagged `reason`:
  `overflow_oldest`/`overflow_newest`) — every datagram this listener itself decided to drop. This
  is *better* news than it sounds: it's the visible, attributable counterpart to a kernel drop you'd
  otherwise never see at all. A sustained nonzero rate here means the listener is genuinely
  overloaded relative to how fast downstream is decoding/consuming, and is worth sizing `receive:`
  or the downstream chain against.
- `logit.component.receive.latency` (timing) — arrival-to-dequeue per datagram. Since decode now
  runs on its own loop, this is the number that says whether event timestamps (always receipt time,
  stamped at arrival, never decode time) are still trustworthy under load — a healthy listener keeps
  this small; a climbing value under sustained load means decode is genuinely falling behind.

### `collectd_in`: multicast groups and `types_db`

`collectd_in` ([ADR `collectd-binary-relay`](adr/collectd-binary-relay.md)) is an ordinary UDP
listener — everything above applies to it unchanged — with two settings specific to collectd's own
deployment conventions:

- **A multicast `bind` is joined automatically.** collectd's `network` plugin defaults to the group
  `239.192.74.66` (or `ff18::efc0:4a42`) on port `25826`, which is what a sender configured with a
  bare `Server "239.192.74.66"` writes to. Give `collectd_in` that same address and it sets
  `SO_REUSEADDR`, binds the unspecified address on the port and joins the group on the host's
  default multicast interface; the `bound` info line names the group. There is no `multicast:`
  field — the address says it. A failed join **fails startup** rather than warning, since a
  listener that bound but never joined would look healthy and receive nothing; in a container this
  usually means the network has no route for `224.0.0.0/4`, and a unicast `bind` with `Server
  "<host>" "25826"` on the sender side is the simpler deployment. Note that a group `bind` is not a
  filter: the socket is bound to the unspecified address on that port, so the listener also accepts
  ordinary unicast datagrams sent to that port from any source, and reports its address as
  `0.0.0.0:<port>` rather than the group.
- **`types_db:` is optional and only affects names.** Point it at the `types.db` your collectd
  installation already ships (conventionally `/usr/share/collectd/types.db`; `logit` does not ship
  one, as collectd's is GPL-licensed) and a multi-data-source list is named after its data sources
  — `load.load.shortterm` rather than `load.load.0`. List several files to merge them in order, a
  later file overriding an earlier one. A file that cannot be read or parsed fails startup, naming
  the path and the line. This changes only what a cross-protocol sink (InfluxDB, Prometheus,
  statsd) calls the series: `collectd_out` re-encodes from the `collectd.*` attributes, so a
  `collectd_in -> collectd_out` relay puts the same bytes back on the wire either way.

## Tailing files and Docker logs

`tail_in` reads one or more files line by line; `docker_in` builds on the same driver to tail
Docker's json-file container logs, enriched with per-container identity read locally from the
sibling `config.v2.json` — no docker socket, no HTTP client
([ADR `file-tailing-and-docker-json-logs`](adr/file-tailing-and-docker-json-logs.md)). Both are
rotation- and truncation-aware, and optionally checkpointed so a restart resumes instead of
replaying or skipping.

### Root, and a read-only bind mount, for `docker_in`

Docker's per-container state directories are `root:root 0710` and the log files `root:root 0640`
on a stock install — `docker_in` needs the process to run as root, with the host's
`/var/lib/docker/containers` (or wherever `root:` points) bind-mounted read-only. This is real,
unavoidable cost specific to reading the json-file driver directly rather than through the docker
socket/API (which brokers access via group membership on the socket instead) — see the ADR's "Root
privileges" section. `demo/compose.yaml`'s `logit` service is the worked example: `user: "0:0"`,
the bind mount, and (SELinux hosts only) `security_opt: ["label=disable"]` — never `:z` on that
mount, which would relabel the daemon's own live state, not something this stack owns.
**Native Linux Docker Engine only**: rootless Docker uses `~/.local/share/docker/containers`,
Docker Desktop's paths live inside its VM, and Podman uses a different log format entirely — none
of these match `docker_in`'s `root:` default, which is the only layout this driver understands.

### `read_from`, and why a checkpoint matters more here than for a plain UDP listener

`read_from: end` (the default) skips whatever a file already holds and tails only new lines;
`read_from: beginning` replays it first. Either way, this only governs a file present at the very
first scan with no checkpoint entry naming it — a file discovered afterward (a new log, a rotated
one, a newly-selected container) always starts at its own beginning, since it has nothing "before
`logit` started" to skip. A checkpoint entry, when present, always wins over `read_from` for the
file it names.

Optional (`checkpoint_path`, unset by default — every restart re-applies `read_from` as if every
file were newly discovered), but usually worth setting for `docker_in` specifically: a
long-running container's log easily exceeds what a re-read-from-end restart would silently skip.
The checkpoint is written on `checkpoint_interval` (5s default) only when dirty, plus on every
file close and on shutdown — never per line, so a crash between two writes can replay up to
`checkpoint_interval` worth of already-emitted lines on restart. This is a deliberate at-least-once
boundary, the same trade-off `buffer:`'s sink-side retry already makes on the delivery half of this
pipeline: bounds how much a crash can replay, and replay itself is always safe. Give the
checkpoint file a persistent volume (`demo/compose.yaml`'s `logit_state`) or it resets on every
container recreate.

### `watch: auto | inotify | poll`

`auto` (the default) uses `inotify` where available (Linux only) for near-immediate discovery of a
new or rotated file, falling back to polling (`watch_error` diagnosed) if `inotify` setup fails;
`poll` always uses the `poll_interval` tick (1s default) instead, with no OS-specific dependency —
the right choice over some network/FUSE mounts, where `inotify` events don't reliably fire; `inotify`
fails startup outright on setup failure rather than degrading silently. **This only speeds up
*discovering* a path** (a new file, a rotation, a truncation) — reading more bytes off an
already-tracked file is never gated by either the watch mode or `poll_interval`, since the driver's
own read loop runs on every iteration regardless of what woke it, and an already-open file handle
simply sees new bytes on its next read.

### What to watch

- `logit.input.files.open` (gauge) — how many files this listener currently has open. Zero when a
  `docker_in` config's `containers:`/`discover:` selection matches nothing, or a `tail_in` config's
  `paths:` glob matches no files yet — both silent by design (a directory that doesn't exist yet is
  the ordinary "not there yet" case, retried next cycle), so this is the number to alert on if
  "nothing is flowing" needs to be distinguished from "nothing to flow yet."
- `logit.input.watch.overflows` (count) — the `inotify` event queue overflowed; the driver responds
  with a full rescan rather than losing track of what changed, but a sustained nonzero rate means
  `poll_interval` is doing more of the real work than the wake source is.
- `logit.component.diagnostics{key="long_line"|"truncated"}` (count, via the `Diagnostics` bridge)
  — a line dropped whole for exceeding `max_line_bytes` (never truncated and passed through — a
  truncated line would silently hand a downstream JSON parser something that looks well-formed but
  isn't the real line), or a tracked file's length shrinking underneath it (real, if rare, on a
  tool that recreates a log file in place rather than renaming it away first).
- `logit.component.diagnostics{key="metadata_error"}` (`docker_in` only) — a container's
  `config.v2.json` couldn't be read or parsed; that container's lines still flow, just with a
  `container.id`-only resource instead of the full identity.

## Series retention

`aggregate` normally drains every series on every flush (tumbling). A statsd gauge is an
exception: the sender transmits only on change and expects the last value to persist, and a
relative adjustment (`+`/`-`, `docs/adr/relative-gauge-adjustments.md`) sent in a later window
needs the gauge's last-known value to apply against. `series_retention` (on by default, `5`
windows) and `max_retained_series` (on by default, `10,000` series) on an `aggregate`
component control this — see the field doc comments in the schema (`logit schema`) for the exact
semantics. `series_retention: 0` opts out entirely, reproducing the strictly-tumbling behavior every
config had before this existed; both fields are additive and optional, so no existing config needs
updating to keep validating. The same two bounds are what makes `temporality: cumulative` possible
(below), which is why they are named for series in general rather than for gauges.

**What retention does not fix:** a delta against a series evicted by the cardinality cap, or a
delta sent after a process restart, resolves against `0.0` — reported (`logit.transform.gauge
.delta.unseeded`, `logit.transform.series.evicted{reason="cardinality"}`), never silent, but not
prevented. The restart case is unfixable without durable aggregator state, which this project has
deliberately not built (`docs/adr/aggregation-window-semantics.md`'s Alternatives — a cumulative
series has the same exposure, which is why every cumulative record carries a `start_timestamp` a
consumer can detect the restart from). If a config's gauges see relative adjustments and an
operator needs the post-restart value to be exact rather than "resolves against 0 until the next
absolute," the sending side's own zero-then-set convention (send an absolute periodically, not only
deltas) is the mitigation, not `logit` itself.

**What to watch:** `logit.transform.series.retained` (gauge) — how many gauge series are currently
carried idle; a number that keeps climbing past what `series_retention × <series churn per window>`
would predict is a sign of a leak (each series' name/tags never repeating) worth investigating with
`keep` the same way unbounded `series.active` growth already is.
`logit.transform.series.evicted{reason="cardinality"}` — any sustained nonzero rate here means
`max_retained_series` is undersized for this pipeline's actual gauge cardinality, and deltas
are silently resolving against 0 as a result.

## Counter temporality (`delta` vs. `cumulative`)

`aggregate`'s `temporality:` decides what a flushed `Sum`/`Histogram` *means*. `delta` (the default)
emits each window's own increment — what InfluxDB and statsd expect, and what every config had
before this key existed. `cumulative` instead keeps the accumulator alive across flushes and emits
the running total since the series was first seen, labelled `Cumulative` and stamped with that
first-seen time (`start_timestamp`) so a consumer can tell a genuine restart from a decrease. That
is the shape a Prometheus scrape carries, so a `prometheus_out` leg needs it: that sink skips delta
records rather than resolving them itself
(`docs/adr/prometheus-scrape-and-exposition.md`), making
`statsd_in -> aggregate(temporality: cumulative) -> prometheus_out` the intended pipeline.

`cumulative` is bounded by the same `series_retention`/`max_retained_series` pair above — that is
what keeps the running totals alive — so both must be above `0`; `logit validate` rejects the
combination otherwise, since with no retention every window's increment would be emitted labelled as
a running total.

**Size `max_retained_series` from `series.active + series.retained`, not from `.retained` alone.**
The cap bounds every series that survives a flush, but the two gauges split that population by
whether it saw data *this* window: a counter incremented every window reports under
`logit.transform.series.active`, and `logit.transform.series.retained` counts only the
idle-but-carried tail. A healthy cumulative pipeline whose counters are all live therefore reports
`retained = 0` while sitting right at the cap — so watching `.retained` alone shows nothing until
`logit.transform.series.evicted{reason="cardinality"}` starts firing, which is already the symptom.
An evicted cumulative series restarts from zero with a new `start_timestamp` (correct, and visible to
a consumer, but a gap in that series' graph), and hitting the cap also warns under
`logit.component.diagnostics{key="series_retention_full"}`.

## Raw samples and set members

By default, `aggregate` summarizes a raw `Samples`/`SetMembers` series the moment it absorbs it —
sketched into a `DdSketch` (`distributions: sketch`) or estimated into a `HyperLogLog`
(`sets: estimate`) — so no raw observation ever survives past the window. Two config keys per pair
opt into keeping the raw data instead, for a `statsd_in -> aggregate -> statsd_out` relay (or any
consumer downstream) that wants the individual values or the exact member set rather than a
summary: `distributions: samples` retains raw values for the whole window, bounded by
`max_samples_per_series` (default `1000`); `sets: members` retains an exact, deduplicated member
set, bounded by `max_set_members_per_series` (default `1000`). See the field doc comments in the
schema (`logit schema`) for the exact semantics; all four fields are additive and optional, so no
existing config needs updating to keep validating.

**Both raw modes fall back to their summarized counterpart, never drop data.** Growing past either
cap converts what's held (plus the record that tripped the cap) into a sketch or a fresh
`HyperLogLog` and counts it, rather than dropping the overflow or growing the accumulator
unboundedly — the same DoS/memory-guard role `max_retained_series` plays for gauge retention.
`distributions: samples` has a second fallback trigger a raw member set can't: an incoming record's
`sample_rate` disagreeing with the series' first one, since a `samples`-mode accumulator can only
ever report one rate for the whole series, and there's no correct single rate to pick between two
that disagree.

**What to watch:** `logit.transform.samples.fallback{reason="cap"|"rate_mismatch"}` and
`logit.transform.set_members.fallback{reason="cap"}` (count) — either firing at a sustained rate
means the configured cap is undersized for this pipeline's actual per-window sample/member volume,
so `distributions`/`sets` is spending real memory retaining raw data that keeps getting thrown away
anyway; the matching throttled diagnostics (`samples_cap_exceeded`, `samples_rate_mismatch`,
`set_members_cap_exceeded`) name which series and why. `logit.transform.samples.weight_clamped`
(count) — a `sample_rate` implying a weight beyond `Samples::MAX_WEIGHT` (1000, i.e. `@0.001`) was
clamped rather than extrapolated without bound; fires in both `distributions` modes (the sketch-mode
absorb and the `samples`-mode fallback's own re-sketch). `statsd_in` itself no longer has a copy of
this diagnostic — [ADR `lossless-transit`](adr/lossless-transit.md)'s W3 deleted it, so `aggregate`
is now the only place `sample_rate_clamped` ever fires.

Neither raw mode changes tumbling: a `Samples`/`SetMembers`/`Set` series never survives a flush,
even with `series_retention` set — retention exists specifically for a gauge's sticky-value
semantics, which nothing about a raw sample or set member shares (see
[ADR `aggregation-window-semantics`](adr/aggregation-window-semantics.md)'s amendment).

## `otlp_in`: put `keep` in front of it

`otlp_in`'s attribute *keys* are arbitrary peer-supplied strings, not something `logit`'s own
config or a fixed protocol grammar bounds — unlike every other listener here (statsd's
`#tag:value`, syslog's structured-data field names), where the set of possible attribute keys is
fixed by `logit`'s own decoder, not by whatever a remote OTLP exporter happens to send.
`crates/logit-proto/src/otlp/common.rs`'s `key_values_into_attrs` interns every OTLP
`KeyValue.key` it decodes into the process-wide interner (`crates/logit-core/src/interner.rs`),
which never evicts (`docs/known-gaps.md`'s interner entry) — so a client that sends a *different*
attribute key on every request (an id embedded in a key name, a misbehaving or malicious exporter)
grows that table for the life of the process, with nothing here to stop it.

The existing mitigation for that gap applies directly: put a `keep` component immediately
downstream of `otlp_in`, naming only the attribute keys you actually intend to keep. That turns an
unbounded, peer-controlled key set into the fixed, `logit`-controlled one every other listener
already gets for free — the same reasoning [`examples/nginx-to-influxdb.yaml`](../examples/nginx-to-influxdb.yaml)
already applies ahead of `aggregate`, extended here to cover interning too, not just series
cardinality. This matters most for a deployment where `otlp_in` faces something other than
`logit`'s own trusted fleet (a third-party exporter, a multi-tenant ingest path) — see
`docs/known-gaps.md`'s interner entry for when the underlying "listeners are private by deployment
shape" premise is worth re-checking at all.

## `otlp_in`: accepted `Content-Type`s, and what a browser client needs

`otlp_in`'s HTTP transport accepts a POST body as `application/x-protobuf`, `application/protobuf`,
or `application/json` — an absent or empty `Content-Type` is treated as protobuf, matching every
client that predates this input's OTLP/JSON support. The response mirrors whichever encoding the
request used: a protobuf request gets a protobuf response, a JSON request gets a JSON one. gRPC is
protobuf-only regardless — OTLP/gRPC's framing *is* protobuf by definition. See
[ADR `otlp-json-decoding`](adr/otlp-json-decoding.md) for the JSON decoding design.

That covers a **same-origin** browser exporter — one reaching `otlp_in` through a reverse proxy
sharing the page's own origin. `otlp_in` has no CORS support of any kind (`handle_http` 404s an
`OPTIONS` preflight, and sets no `Access-Control-Allow-Origin`), so a **cross-origin** browser
exporter — one pointed at `otlp_in` directly, on a different origin than the page — cannot reach it
at all; put a reverse proxy in front that shares the page's origin instead of trying to open
`otlp_in` up to arbitrary browser origins (`docs/known-gaps.md`).

## TLS

`otlp_out` (both `protocol: http` and `protocol: grpc`) and `otlp_in` (both transports) can speak
TLS -- see [ADR `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md) for the
design. On `otlp_out`, TLS is selected by `endpoint`'s scheme, the same convention every OTel SDK
uses:

```yaml
components:
  trace_out:
    type: otlp_out
    endpoint: https://tempo.internal:4317
    protocol: grpc
    sources: [enrich]
    tls:
      ca_file: /etc/logit/tls/ca.pem   # trust this CA instead of the bundled Mozilla set
```

`http://`/`grpc://` (or a bare `host:port` under `protocol: grpc`) stays plaintext regardless of
`tls:` — a non-empty `tls:` block under a plaintext endpoint is a config error (rule 22), not
silently ignored, since it would otherwise have no effect. `ca_file`/`cert_file`/`key_file` paths
resolve relative to the config file's own directory, the same rule `lua_file` follows, and (like
any other field) accept `!env` if the certificate material needs to come from the environment
rather than a mounted file (ADR `env-yaml-tag`).

Mutual TLS adds a client certificate:

```yaml
    tls:
      ca_file: /etc/logit/tls/ca.pem
      cert_file: /etc/logit/tls/client.pem
      key_file: /etc/logit/tls/client.key
```

`otlp_in` has no endpoint of its own to read a scheme from — the presence of a `tls:` block turns
TLS on for that listener, on both transports:

```yaml
components:
  log_in:
    type: otlp_in
    bind: 0.0.0.0:4318
    tls:
      cert_file: /etc/logit/tls/server.pem
      key_file: /etc/logit/tls/server.key
      client_ca_file: /etc/logit/tls/ca.pem   # omit for server-auth-only TLS
```

`client_ca_file` requires every connecting client to present a certificate chaining to it (mutual
TLS); omit it to accept any client once the handshake itself completes.

**`tls.insecure_skip_verify`** (`otlp_out` only) disables server-certificate verification — the
connection is still encrypted, but any certificate is accepted. `logit` logs a startup warning
whenever it's set; it's meant for a throwaway or pre-production endpoint, not a real deployment,
and is rejected at config-validation time together with `ca_file` (contradictory: a specific
trusted CA and "trust nothing" can't both be meant).

**What to watch.** A handshake failure on either side surfaces through the same
`connection_error`/`network_error` diagnostics and `logit.output.requests{class="network_error"}`/
listener-side `logit.component.diagnostics` counters as any other transport failure — nothing TLS
-specific to watch beyond that. `docs/known-gaps.md` tracks two open items: certificates are read
once at startup (a renewed cert needs a restart, not a live reload), and `otlp_out` has no
`server_name` override for an endpoint reached by IP or through a proxy.

## Forwarding between `logit` nodes

`logit_out`/`logit_in` are the native `logit`-to-`logit` transport
([ADR `native-transport-handshake-and-ack`](adr/native-transport-handshake-and-ack.md)) -- the
"split collection from processing across nodes" shape [`docs/OVERVIEW.md`](OVERVIEW.md) names as
the whole point of the native wire format existing. A sidecar/edge process collects and forwards
unaggregated; a central process receives, aggregates, and delivers. See
[`examples/forwarder-edge.yaml`](../examples/forwarder-edge.yaml)/
[`examples/forwarder-central.yaml`](../examples/forwarder-central.yaml) for a complete, runnable
pair.

```yaml
# edge
components:
  central_out:
    type: logit_out
    sources: [edge_in]
    endpoint: central.internal:5140
```

```yaml
# central
components:
  central_in:
    type: logit_in
    bind: 0.0.0.0:5140
```

**TLS.** `logit_out`'s `endpoint` is a bare `host:port` with no scheme to read a TLS signal from
(unlike `otlp_out`'s URL-shaped endpoint) -- a `tls:` block's mere presence turns TLS on, the same
convention `otlp_in` already uses server-side:

```yaml
# edge
    tls:
      ca_file: /etc/logit/tls/ca.pem
```

```yaml
# central
    tls:
      cert_file: /etc/logit/tls/server.pem
      key_file: /etc/logit/tls/server.key
      client_ca_file: /etc/logit/tls/ca.pem   # omit for server-auth-only TLS
```

**Sizing `request_timeout` against `buffer.retry_budget`.** `logit_out.request_timeout` (default
10s) bounds one attempt -- connect, handshake, and the ack wait, all sharing that one knob, the
same shape `otlp_out`'s own timeout has. `buffer.retry_budget` (default 60s, see "Sink delivery
buffering" above) is the *outer* bound across every retried attempt. Keep `request_timeout`
comfortably under `retry_budget` -- a `request_timeout` close to or above the retry budget leaves
room for at most one attempt before the budget itself expires, which defeats retry's purpose.
`request_timeout` also bounds `logit_in`'s own handshake grace on the far end only loosely: a
`logit_out` configured with a shorter `request_timeout` than its peer's handshake patience just
means *this* side gives up first, not that the connection is unsafe. That far-end grace is 5s per
pre-`Hello` phase, applied independently to the TLS accept and to the `Hello` read that follows
it -- so a TLS peer that connects and then goes silent is dropped after at most 10s, not 5s.
A peer that gets `Reject{code: REJECT_INTERNAL}` from a `logit_in` at its connection cap never
classifies it `permanent`: at the handshake (nothing of the batch written yet) it's `clean` and
the batch is retried within `retry_budget`; once a frame has already left on that connection it's
`ambiguous`, which under `logit_out`'s default `at_most_once` posture is *not* retried -- that
batch is dropped and counted, and only the connection itself recovers. Either way the sink
reconnects on its own once the peer has capacity again, with no operator intervention needed; set
`buffer.delivery: at_least_once` on the `logit_out` component if you would rather risk a duplicate
than lose that batch. The same holds for `Reject{code: REJECT_GOING_AWAY}` during the peer's own
shutdown.

**What to watch.** `logit_out`: `logit.output.requests{class}` (`ok`/`clean`/`ambiguous`/
`permanent`, one per `send` attempt), `logit.output.reconnects` (should stay near zero in steady
state -- a climbing count means the peer or the network is unstable), `logit.output.ack.duration`.
`logit_in`: `logit.input.connections` (a gauge; should match the number of `logit_out` peers
actually connected), `logit.input.connections.rejected{reason="limit"}` (nonzero means the 1024
-connection cap is actually binding -- raise it or shed load upstream), `logit.proto.errors{reason}`
(`magic`/`version`/`crc`/`truncated`/`too_large`/`codec`/`handshake` -- any of these on a healthy
link points at a version-mismatched or misbehaving peer, not routine loss). Both sides:
`logit.proto.frames{direction,codec,compression}` and `logit.proto.frame.bytes` for throughput.
`docs/known-gaps.md` tracks what's still open: no credit-based flow control (this plan's sender
never has more than one frame outstanding), and `logit_in`'s shutdown grace is fixed at 5s with no
`receive:`-shaped knob to change it.

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

The honest answer used to be "there's no way to know a UDP listener is actually bound short of a
manual probe" — that gap is what [Probes and exit codes](#probes-and-exit-codes) above closes.
With `admin: { bind: ... }` set, wait for `/readyz` to return `200` (or run `logit ready`) before
starting nginx; `/readyz` only reports `ready` once every listener, `syslog_in` included, has
actually bound its socket:

```sh
until logit ready --admin http://<logit-host>:9600; do sleep 0.5; done
```

Without `admin:` configured, the `bound`/`ready` lifecycle log lines (default `--log-level info`,
[Self-logging](#self-logging) above) are the fallback — `bound` names each socket listener's
address as it opens (`syslog_in` included), and `ready` fires once every listener is bound. A
manual smoke test still works if neither is wired up: send a line and watch for the corresponding
`stdio_out` block:

```sh
logger -n <logit-host> -P 5140 -d -t smoke '{}'
```

(`-d` forces UDP — `logger`'s default without `-T`/`-d` depends on `/etc/services`, which isn't
reliably UDP-first everywhere.) A `stdio_out` block appearing for that line means the listener is up
and reachable; nothing appearing means nginx pointing at it next would just be feeding the same
fire-and-forget void.
