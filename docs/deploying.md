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
| `2` | A runtime failure after the process reported ready — a sustained, purely-configuration-error sink failure (see [Sink delivery buffering](#sink-delivery-buffering) below), a listener's accept loop dying, a `lua`/`lua_file` component's thread panicking (a script's own `process()`/`flush()` errors are not this: they're logged and counted, never fatal). |
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
"running"|"finished"|"failed"|"alias"}}` instead of the bare status word; `/healthz?format=json`
returns just `{status}`, since it has nothing else to report. A `target` component
([ADR `target-components`](adr/target-components.md)) is always and only `alias`: it has no task
and no inbox — it is a name for its routers' outbound edges, so its liveness is theirs, and none
of the other states can apply to it. No TLS, no auth — this is a loopback/pod-local endpoint by
design, not one meant to cross a real network boundary.

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
| `bound` | info | One component's socket opened, during the pre-bind pass — listeners (`syslog_in`/`statsd_in`/`collectd_in`/`graphite_in`/`otlp_in`; `tail_in`/`docker_in` emit none) and sinks that listen (`prometheus_out`). A `collectd_in` (or any UDP listener) whose `bind` names a multicast group says so, naming the group it joined. |
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

Every UDP listener (`collectd_in`, and a `statsd_in`, `syslog_in` or `graphite_in` with `transport: udp`) sits in front of a per-component, in-memory receive
queue that decouples reading the socket from decoding and batching what it received
([ADR `decoupled-listener-io`](adr/decoupled-listener-io.md)) — the listener-side sibling of the sink delivery
buffering above. This is what lets a slow or backed-up destination downstream be ridden out without
the socket itself going unread. It's tunable per listener via a `receive:` block on that component
(`receive:` is rejected at validation time on any kind but a datagram listener or a tail listener
(`tail_in`/`docker_in`) — and a tail listener has no receive *queue* at all, so only its four
batch-assembly fields apply; see "Tailing files and Docker logs" below) — see the commented example
in [`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml). Every field defaults,
so an omitted `receive:` is the values below.

### `handshake_timeout` on a TCP listener

A stream listener has no receive queue (its connection's own flow control is the backpressure), but
it does have something a datagram listener doesn't: a connection that can be opened and then left
saying nothing, holding one of the listener's 1024 concurrency-cap permits. `syslog_in`,
`graphite_in` and `statsd_in` (each `transport: tcp`), `logit_in`, and `otlp_in` each bound that
with a `handshake_timeout:` field — **5s by default**, a humantime string like `connect_timeout`:

```yaml
components:
  syslog_in:
    type: syslog_in
    bind: 0.0.0.0:6514
    transport: tcp
    handshake_timeout: 5s        # the default; per pre-message phase, not a total
```

**It is a per-phase budget, not one deadline for the connection.** Each pre-message phase gets its
own budget of the configured length, so a TLS connection that says nothing at all can cost up to
two of them — 10s at the default — before it is closed and its permit released. The phases are:

| Kind | Phases bounded |
|---|---|
| `syslog_in` (`transport: tcp`) | the TLS accept (under `tls:`), then the wait for the connection's first byte — on the plaintext arm too |
| `graphite_in` (`transport: tcp`) | the same two phases, on the same shared driver |
| `statsd_in` (`transport: tcp`) | the same two phases, on the same shared driver |
| `logit_in` | the TLS accept (under `tls:`), then the `Hello` read |
| `otlp_in` | the TLS accept (under `tls:`), or — on the plaintext arm, which has no TLS accept — the wait for the connection's first byte |

`otlp_in` is the narrow one, and not by choice: it bounds one phase per connection rather than two.
It hands each accepted connection straight to `hyper`, whose connection builder reads the first
bytes itself to tell HTTP/1.1 from an HTTP/2 preface — a read this listener never sees. What it
*can* do on a plaintext listener is wait for the first byte to become available without consuming
it (a `MSG_PEEK`), which is the bound this knob applies there; the version sniff then proceeds over
an untouched socket. So a connection that sends nothing at all is closed inside the budget on
either arm, and a connection that sends **one byte** and then goes silent is past everything this
knob reaches — what bounds *that* gap is the separate, opt-in `idle_timeout` below. `hyper`'s own
HTTP/1 header-read timeout is deliberately not used to close it — it re-arms on every idle
keep-alive gap, so it would behave as an idle timeout and kill a long-interval exporter's pooled
connection; `idle_timeout` is that bound made explicit and opt-in instead.

**It is not an idle timeout, on any of them.** Once a connection has got past its pre-message
phases, the gap before its next frame/request is unbounded by `handshake_timeout` — a long-lived,
mostly-quiet sender is ordinary traffic, not a fault, so this field never closes a connection for
going quiet after its handshake. What *does* bound that gap, opt-in and separate from this field,
is `idle_timeout` — see the next section. Lowering `handshake_timeout` does not help with that
case either; it only tightens how fast a connection that never said anything at all is given up on.

`handshake_timeout` must be greater than `0s` (rule 45 — `0` would close every connection before
its handshake could start), and on a `syslog_in`, `graphite_in` or `statsd_in` with
`transport: udp` it must be left at its default: a datagram listener has no connection to hand
shake, so a value set there is rejected at validation time rather than silently ignored.

### `idle_timeout` on a TCP listener

`handshake_timeout` above bounds only the *pre*-message phases. What it deliberately leaves open is
everything after: a connection that gets past its handshake (or, on a plaintext listener, delivers
at least one byte) and then goes quiet holds its connection-cap permit indefinitely — right up to
the 1024-connection cap itself — with nothing closing it. `idle_timeout:` is the opt-in field that
bounds exactly that gap, on the same five kinds `handshake_timeout` covers: `syslog_in`,
`graphite_in`, `statsd_in` (each `transport: tcp`), `logit_in`, and `otlp_in`. See
[ADR `idle-connection-timeout`](adr/idle-connection-timeout.md) for the full design; this section
is the operator-facing summary.

```yaml
components:
  syslog_in:
    type: syslog_in
    bind: 0.0.0.0:6514
    transport: tcp
    handshake_timeout: 5s        # the default; per pre-message phase, not a total
    idle_timeout: 5m             # off by default; see the recommendation below
```

**Off unless set.** With no value, a connection that finished its handshake and then went silent is
never closed for silence alone — today's behaviour, unchanged. `logit validate` rejects `0s` by
name (rule 53 — "omit the field to disable the idle timeout") and, on `syslog_in`/`graphite_in`/
`statsd_in`, rejects any value at all under `transport: udp`, where a datagram listener has no
connection to time out.

**What it bounds, and the reset rule.** The clock runs only while the listener is waiting on the
peer's socket, and only two things reset it: bytes actually read from the peer, and the listener
finishing its own work on the connection — a batch handed downstream, a response completed, an
`Ack` written. Time spent blocked handing a batch to a full downstream never counts, because the
clock has not yet been re-armed while that block is in progress; it only starts running again once
that work returns. A connection stalled on backpressure therefore never looks idle no matter how
long the stall lasts, and a periodic flush tick that finds nothing to send touches neither event, so
it never quietly re-arms the clock on its own.

**Per-kind notes:**

| Kind | What resets the clock | How the close happens |
|---|---|---|
| `syslog_in`, `graphite_in`, `statsd_in` (`transport: tcp`, the shared driver) | bytes read from the peer; an interval flush that actually emits a batch | the connection is closed directly; any complete buffered batch is flushed first |
| `logit_in` | the handshake completing, and every `Ack` this listener writes; a peer waiting on a delayed ack is by definition not idle. A frame header whose first byte has already arrived is progress too: the absolute idle deadline bounds only the wait for that first byte, and the rest of the header — like the body — is read under the per-`read` stall bound instead, so a frame that starts arriving right at the deadline is read and acked rather than rejected after the peer already wrote it | `Reject{GOING_AWAY, "idle for <dur>"}` is written first, the same signal an ordinary shutdown sends, then the connection closes |
| `otlp_in` | a request *completing* — hyper owns the bytes, so this is the finest grain visible here; a request head that dribbles in slower than `idle_timeout` on an otherwise-quiet keep-alive connection is closed by this rule, a documented narrowing; a stalled request *body* gets its own bound, `idle_timeout` itself, per read frame | `graceful_shutdown()` is called and the connection is polled for up to `handshake_timeout` (reused as the grace period — no new knob); if that grace elapses with nothing in flight the connection is dropped regardless of what the poll returned, and if a request arrives inside the grace instead, see the note below the table; a stalled body instead answers `408` (`protocol: http`) or `grpc-status: 4` (`protocol: grpc`) and closes the connection once the handler returns — that close is counted the same `reason="idle"` as any other, one policy close reached one path earlier |

A request that arrives inside the bounded grace is served to completion, not dropped underneath
it: the connection is kept open while a request is in flight, and the grace runs again once that
request completes so its response actually reaches the wire — dropping it mid-flight would discard
a batch already handed to `Fanout::send`. The cost is at most a reconnect for the *next* request on
that connection, not a lost response or a lost batch. A silent peer cannot exploit this to hold the
connection open indefinitely: with nothing in flight the drop still happens at the end of the
grace, and a request body that stalls mid-upload is bounded by the same per-frame stall timeout
regardless.

**An idle close is policy, not a fault.** All five listeners return `Ok(())` from the connection
task the same success path a graceful shutdown takes, so an idle close never reaches the
`connection_error` diagnostic. It is counted instead: **`logit.input.connections.closed
{reason="idle"}`** — a rising count here with no corresponding movement in `connection_error` is
the feature doing its job, not something to investigate. Whatever the connection had already
buffered is not silently dropped: a complete accumulated batch is flushed before the close, and a
partial frame still sitting in the framer is counted `truncated` — the same accounting a `Failed` or
`Shutdown` close already gets.

**The client side: a pooled connection is probed before it is reused.** A server-side idle close is
not free for a sink holding a pooled connection to it: writing into a socket the peer already
closed either lands as `Fault::Ambiguous` (`logit_out`, whose native protocol has ack framing to
notice the failed write) or is silently lost with no ambiguity at all (`syslog_out`, `statsd_out`,
`graphite_out`, whose plaintext wire protocols have no way to tell the sender anything went wrong).
Every one of those four pooled TCP sinks now polls a *reused* pooled connection once before its
first write of a send attempt — a single non-cancellable `poll_read`, never a `timeout(read)`, since
a timeout on a real read could cancel mid-TLS-record and discard bytes that had already arrived. An
immediate EOF or unsolicited bytes (the only thing a peer ever sends unprompted on the native
protocol is `logit_in`'s own `Reject{GOING_AWAY}`) drops the pooled connection and dials a fresh one
before anything is written — the ordinary `Clean`/reconnect path, not a lost or ambiguous batch.
This closes the common case for free: a peer that idle-timed-out and closed some time ago is caught
before the write that would otherwise race its FIN. The residual case is the FIN racing the probe
itself — the peer closing *while* the sink is writing — which is today's unchanged
`Fault::Ambiguous` on `logit_out` and a genuinely silent loss on the three plaintext sinks; the probe
narrows the window, it does not close it.

**Recommendation: enable it wherever consistent traffic is expected.** On a listener receiving
steady traffic, a connection quiet for longer than the timeout is by definition an anomaly — a dead
peer, a half-open socket, or a slow-loris attempt — so closing it costs nothing real and returns the
permit. Size the value comfortably above the sender's longest normal gap (several flush intervals,
for instance), so the timeout never fires against legitimate traffic. Leave it unset only for
genuinely sparse or bursty senders, where a long quiet period is expected and normal, and think
twice about enabling it at all on plaintext `syslog_in`/`graphite_in`/`statsd_in`, where the sender
has no way to learn its connection was closed.

### Failure semantics: `drop_oldest`, not `block` — the opposite default from `buffer:`

`buffer:`'s default is `block`, and that's the right call there: the producer being backpressured
is an in-process drain that can afford to wait. `receive:`'s default is `drop_oldest`, and that's
deliberately the opposite call, for a reason worth understanding rather than just remembering: the
producer behind a UDP listener is the kernel's socket receive buffer, which *cannot* wait. Setting
`receive.overflow: block` doesn't prevent loss under sustained overload, it just relocates it from a
place `logit` can do something about (`logit.component.datagrams.dropped`, a queue you can size) to
one it can only report on (`logit.input.kernel.drops`, the kernel discarding datagrams before
`recv_from` ever sees them). Every mature UDP listener in the field — syslog-ng, rsyslog, Telegraf,
gostatsd — treats this the same way, and most of them can't even tell you the second number. Leave
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
  is *better* news than it sounds: it's the drop you can size your way out of, by raising
  `receive.max_datagrams`/`max_bytes` or speeding up what's downstream. A sustained nonzero rate
  here means the listener is genuinely overloaded relative to how fast downstream is
  decoding/consuming, and is worth sizing `receive:` or the downstream chain against.
- `logit.input.kernel.drops` (count) — datagrams the *kernel* threw away before `recv_from` could
  return them, read from the listening socket itself (Linux only). This is the loss nothing else
  in the field reports in-process: it's the same number `/proc/net/udp`'s `drops` column shows for
  this socket, and it is not covered by the queue counter above — the two are separate losses that
  add up. Any sustained nonzero rate means datagrams are arriving faster than this process takes
  them off the socket.
- `logit.input.receive_buffer.utilization` (gauge), with
  `logit.input.receive_buffer.used.bytes` / `.bytes` behind it — how full the kernel's own socket
  buffer is, sampled once a second. This is the leading indicator for the counter above: the
  kernel drops at 1.0, so a value climbing toward it is the warning, and the drops are the event.
  (A reading a little over 1.0 is normal at saturation, not a bug — the kernel charges an arriving
  packet before testing the total against the ceiling, so a sample can catch it mid-drop.) **What to do about a high value depends on which way the drops move with it.** If raising
  `receive.receive_buffer_bytes` (and, if the startup warning names it, `net.core.rmem_max`) makes
  the drops go away, the traffic was bursty and the buffer was too small for the bursts. If it
  doesn't — the buffer simply fills up again at its new size — then nothing is wrong with the
  buffer and the reader is the bottleneck: check `logit.component.receive.utilization` and
  `receive.latency` below, which say whether decode is what's behind, and size the downstream
  chain rather than the socket. A bigger buffer absorbs a burst; it cannot absorb a sustained
  arrival rate faster than this process can read.

  Two footnotes on the numbers, so they aren't misread. The `used.bytes` figure is what the kernel
  *charges* this socket, not the payload bytes queued: each datagram costs several hundred bytes of
  packet-structure overhead on top of its own length, so a queue of small statsd datagrams charges
  far more than their combined size — which is the right accounting, because it's the one the
  kernel drops against. And `receive_buffer.bytes` is the doubled value Linux reports for a
  `SO_RCVBUF` request, not what you asked for; the ratio is computed from the kernel's own pair, so
  it's directly comparable across listeners regardless of what any of them requested.
- `logit.component.receive.latency` (timing) — arrival-to-dequeue per datagram. Since decode now
  runs on its own loop, this is the number that says whether event timestamps (always receipt time,
  stamped at arrival, never decode time) are still trustworthy under load — a healthy listener keeps
  this small; a climbing value under sustained load means decode is genuinely falling behind.

On a **TCP** listener there is no receive queue and no kernel receive buffer to size (TCP's own
flow control is the backpressure), but there is an accept queue, and it has the same shape of
problem:

- `logit.input.accept_queue.depth` / `.limit` / `.utilization` (gauges, Linux only) — connections
  that have completed their TCP handshake and are waiting for this listener to accept them, the
  backlog ceiling the kernel enforces, and the first as a fraction of the second. Sampled before
  each accept and once a second while waiting, so an idle listener still reports. `.limit` is
  reported on its own so you can see what `listen(2)` actually got after `net.core.somaxconn`
  clamped it, without having to back it out of the ratio. A depth that is anything but near-zero
  means connections are arriving faster than they're being accepted; a utilization approaching 1.0
  means the kernel is about to start refusing new connections outright, which a client sees as a
  connect timeout or a reset with nothing in `logit`'s own logs to explain it. Sustained pressure
  here is usually connection churn — senders reconnecting per batch rather than holding one
  connection open — and is worth fixing at the sender before it's worth raising
  `net.core.somaxconn`.

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

A complete, runnable topology is
[`examples/collectd-to-influxdb.yaml`](../examples/collectd-to-influxdb.yaml): `collectd_in` on
`0.0.0.0:25826` straight into `influxdb_out`, with both settings above present as commented
alternatives. It deliberately has **no** `aggregate` in the middle, unlike
[`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml) — a collectd value list is
already one pre-aggregated reading per `Interval`, carrying its own timestamp, so re-windowing it
would average averages and re-stamp them with the flush time. The file's header comment lists what
that cross-protocol hop costs: the `collectd.*` attributes become ordinary InfluxDB tags rather than
wire identity, and one N-data-source list becomes N measurements named `plugin.type.ds` sharing a
tag set and a timestamp.

### `graphite_in`: carbon plaintext and pickle, TCP or UDP

`graphite_in` ([ADR `graphite-carbon-relay`](adr/graphite-carbon-relay.md)) is a carbon receiver:
point a `write_graphite` plugin, a StatsD backend, a `carbon-relay` or anything else that speaks
carbon at it. It is one component with two settings that change a great deal about how it behaves.
[`examples/graphite-relay.yaml`](../examples/graphite-relay.yaml) is the like-for-like runnable
topology (`graphite_in` straight into `graphite_out`, every default present as a commented
reference); [`examples/statsd-to-graphite.yaml`](../examples/statsd-to-graphite.yaml) is the
cross-protocol one, `statsd_in` through an `aggregate` window into `graphite_out`.

- **`transport:` picks the driver.** `tcp` is the default, matching carbon's own default listener
  (plaintext on port 2003). A TCP listener serves up to 1024 connections at once; one arriving past
  that cap is closed immediately and counted
  (`logit.input.connections.rejected{reason="limit"}`), because carbon's wire has no way to say
  "try later" and a sender holding an accepted-but-unread connection would look healthy while
  delivering nothing. `udp` runs the same shared datagram listener `statsd_in`/`collectd_in`/
  `syslog_in` do, so everything in the receive-queue section above applies to it unchanged. The TCP
  driver is the same one a TCP `syslog_in` runs on, so `tls:` and `handshake_timeout:` mean exactly
  what they mean there (next bullet).
- **`tls:`, `handshake_timeout:`, and `idle_timeout:` are TCP-only, and behave as `syslog_in`'s do.**
  A `tls:` block's mere presence turns TLS on and makes it required — there is no plaintext
  fallback on a TLS listener — and `logit validate` rejects one under `transport: udp` (carbon has
  no DTLS receiver). Plain carbon senders have no TLS of their own, so this is for a `logit`-to-
  `logit` or stunnel-shaped relay hop. `handshake_timeout:` (default `5s`) bounds each pre-message
  phase independently: the TLS accept when `tls:` is set, then the wait for the connection's very
  first byte, so a TLS connection that says nothing at all costs up to two of them before its
  permit comes back. It is **not** an idle timeout — once a connection has sent a byte, the gap
  before the next datapoint is bounded by the separate, opt-in `idle_timeout:` if one is set, and
  unbounded if it is not; see ["`idle_timeout` on a TCP
  listener"](#idle_timeout-on-a-tcp-listener) above.
- **`receive:` means different halves on the two transports.** A UDP `graphite_in` takes the whole
  block. A TCP one has **no receive queue at all** — TCP's own flow control is the backpressure,
  and the queue exists (ADR `decoupled-listener-io`) for a UDP socket's *silent* drops, which a
  stream cannot have — so only `batch_max_events`, `batch_max_bytes`, `batch_flush_interval` and
  `shutdown_grace` apply to it. A queue-bounding field (`max_datagrams`, `max_bytes`, `overflow`,
  `receive_buffer_bytes`) on a TCP `graphite_in` is a `logit validate` error naming the field, not
  a setting that is silently ignored. A stalled TCP `graphite_in` therefore shows up as
  backpressure at the *sender*, which is what you want, rather than as a drop counter here.
- **`protocol: pickle` requires `transport: tcp`.** Carbon's pickle batch protocol (port 2004) is a
  4-byte big-endian length prefix around each batch — Twisted's `Int32StringReceiver` — which has
  no meaning in a datagram that already delimits itself, so the combination is rejected at
  validation time rather than mis-framing at runtime. The pickle reader is a **restricted** one: it
  accepts the opcodes real senders emit (`pickle.dumps(..., protocol=2)` and `protocol=-1`) and
  rejects everything capable of constructing an object, with bounded depth, memo and item counts
  and every declared length validated before anything is allocated. It is deliberately not a
  general unpickler.
- **The two size bounds are the ones you may need to raise.** `max_line_bytes` (default `"8192"`)
  bounds one TCP plaintext line: past it the line is abandoned and counted once
  (`logit.input.frames.dropped{reason="oversize"}`, diagnostic `framing_error`) and the reader
  drains to the next newline — so the line *after* an oversize one still decodes, and the
  connection stays up. `max_frame_bytes` (default `"1MiB"`, Twisted's own `MAX_LENGTH`) bounds one
  pickle frame; a frame declaring more is counted the same way but **closes the connection**,
  because a length-framed stream has no resync point to skip forward to. `logit validate` holds it
  to `1024..=16MiB`.

**The path is the metric name, and there is no `graphite.*` namespace.** Unlike `collectd_in` and
`syslog_in`, which park their wire identity in attributes a matching sink reads back,
`graphite_in` maps the four facts carbon carries straight onto the model: the dotted path *is*
`MetricRecord.name`, the `;k=v` tags *are* event attributes, the number is a `Gauge`, the second is
the event timestamp. Nothing is duplicated, and nothing is reserved. State that plainly because it
cuts both ways: **a `lua`/`set` stage that renames the metric silently changes the wire path** a
downstream `graphite_out` writes. That is the intended way to rename a series — there is no
`prefix:` or `template:` field on either component — but it means a rename transform in the middle
of a relay is a wire-visible change, not a display one.

Two smaller behaviours worth knowing before deploying one:

- **A `-1` timestamp means receipt time**, carbon's own rule. Any other non-positive timestamp
  rejects the line (`bad_timestamp`), rather than being quietly stamped with "now".
- **A malformed tag rejects the whole line**, not just that tag — carbon's own
  `TaggedSeries.parse` raises too, and dropping one tag would silently change the series identity
  the receiver keys on. A repeated tag key keeps its **last** value, counted
  `logit.input.tags.normalized{reason="duplicate_key"}`, which is again what carbon does (it
  builds a `dict`).

### `statsd_in`: `transport: tcp` and TLS

`statsd_in` defaults to UDP, which is what classic statsd and every DogStatsD client speak, and
what [`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml) and
[`examples/statsd-relay.yaml`](../examples/statsd-relay.yaml) use. `transport: tcp` runs the same
shared stream driver a TCP `syslog_in`/`graphite_in` runs on, so everything the two bullets above
say about connections, `handshake_timeout:`, `idle_timeout:` and `receive:` applies here unchanged:

```yaml
components:
  statsd_in:
    type: statsd_in
    bind: 0.0.0.0:8125
    transport: tcp                 # udp (the default) | tcp -- tls: below requires tcp
    tls:                           # presence turns TLS on and makes it *required*
      cert_file: /etc/logit/tls/server.pem
      key_file: /etc/logit/tls/server.key
      client_ca_file: /etc/logit/tls/ca.pem   # omit for server-auth-only TLS
    handshake_timeout: 5s          # the default; per pre-message phase, tcp only
    idle_timeout: 5m               # off by default; see "idle_timeout on a TCP listener" above
```

- **A TCP message is one LF-delimited line, always.** There is no `framing:` field and no
  octet-counted alternative the way `syslog_in` has one, because there could not be: a statsd
  metric name may legally begin with a digit (`1.hits:1|c`), so sniffing a leading digit as a
  length prefix could only ever mis-frame the connection. This is what `statsd_out`'s own
  `transport: tcp` has always emitted, and what the Etsy reference server and the Datadog agent
  accept.
- **An oversize line costs that line, not the connection.** A line past 64 KiB is dropped and
  counted once (`logit.input.frames.dropped{reason="oversize"}`, diagnostic `framing_error`), the
  reader drains to the next newline, and the line after it still decodes. There is deliberately no
  `max_line_bytes` knob to tune — unlike carbon, no statsd server exposes one for you to match.
- **An unterminated final line is dropped, not delivered.** A sender that closes with a partial
  line leaves bytes the listener will not emit: they are counted
  `logit.input.frames.dropped{reason="truncated"}` and discarded, the same as a connection that
  dies mid-line. The LF is a statsd line's only completeness signal, and half of `page.views:1|c`
  still looks like a valid metric — delivering it would be silent corruption. (This is where a
  line protocol differs from `syslog_in`, whose RFC 6587 framing explicitly permits a
  terminator-less last message.) Trailing whitespace-only padding is not counted.
- **`tls:` is TCP-only, and its presence makes TLS required.** There is no plaintext fallback on a
  TLS listener, and `logit validate` rejects a `tls:` block under `transport: udp` (rule 43 — DTLS
  is out of scope everywhere in this project, and no statsd client speaks it). Plain statsd clients
  have no TLS of their own either, so this is for a `logit`-to-`logit` or stunnel-shaped relay hop.
  See ["TLS"](#tls) below for the full field reference.
- **`receive:` means different halves on the two transports**, exactly as for `graphite_in` above:
  a UDP `statsd_in` takes the whole block; a TCP one has no receive queue at all, so only
  `batch_max_events`, `batch_max_bytes`, `batch_flush_interval` and `shutdown_grace` apply to it
  (scoped per connection), and a queue-bounding field on one is a `logit validate` error naming the
  field rather than a silently ignored setting.
- **What to watch.** Under `transport: udp`, the `logit.input.datagrams`/`.datagram.bytes` pair and
  the receive-queue gauges above. Under `transport: tcp`, `logit.input.connections` (a gauge —
  should match the number of senders actually connected),
  `logit.input.connections.rejected{reason="limit"}` (nonzero means the 1024-connection cap is
  binding), `logit.input.frames`/`.frame.bytes` (one frame is one statsd line), and
  `logit.input.frames.dropped{reason="oversize"|"truncated"}`. A malformed *line* is the decoder's
  own `logit.component.diagnostics{key="bad_line"}` on either transport, not a framing error.

### `collectd_out`: relaying back onto the wire

`collectd_out` is the like-for-like other half, and the sink to reach for when the destination is
another collectd (or anything else speaking its `network` protocol) rather than a time-series
database: `collectd_in -> collectd_out` is a fixed point modulo the named normalization list in
[ADR `collectd-binary-relay`](adr/collectd-binary-relay.md), which
`crates/logit-cli/tests/collectd_round_trip.rs` pins fixture by fixture over real sockets.
[`examples/collectd-relay.yaml`](../examples/collectd-relay.yaml) is the runnable topology —
`collectd_in` on `0.0.0.0:25826` straight into `collectd_out`, every default present as a commented
reference. Three things worth knowing before deploying one:

- **UDP only, and no `aggregate` in the middle.** collectd's `network` plugin has no TCP mode to
  relay onto. And unlike [`examples/statsd-relay.yaml`](../examples/statsd-relay.yaml), the collectd
  relay example has no `aggregate` between the two ends: collectd data is already one pre-aggregated
  reading per `Interval`. A window there would re-window it, and would stop the relay being
  byte-for-byte for the kinds `aggregate` genuinely absorbs — a GAUGE and an ABSOLUTE come back
  stamped with the flush time, where a COUNTER/DERIVE (a cumulative `Sum`) passes straight through
  untouched. Add one only to re-window deliberately.
- **A relay emits more bytes than it received — size for that.** `collectd_out` writes a `TimeHR`
  and an `IntervalHR` part for *every* value list, where collectd's own sender elides one that
  hasn't changed since the last list in the same datagram (normalization 11 in the ADR's list). The
  restored parts carry exactly what the receiver's sticky state already held, so nothing about the
  data changes — but the datagram grows, and may split. The recorded capture
  `testdata/interop/collectd/collectd-000.raw` is a real Debian `collectd`'s output and shows the
  scale: 26 value lists behind 17 `TimeHR` parts and a single `IntervalHR`, 1296 bytes in one
  datagram, which this relay re-emits as 1717 bytes across two. Budget roughly a third more egress
  bytes and packets than the fleet sends, and expect a capture of the relayed traffic to look
  chattier than the original.
- **`max_packet_bytes:` bounds a datagram, not a value list**, and defaults to collectd's own
  `MaxPacketSize` default of `1452`. Graph validation rejects anything outside `1024..=65535`,
  collectd's own range. Lower it to match a path MTU; the encoder re-packs incoming lists into
  datagrams of its own choosing regardless of how the sender packed them, so this is the setting
  that decides egress framing — together with the inflation above, which is what pushes that
  1296-byte capture over the default cap. A single value list too large to fit even alone is
  dropped whole and counted `logit.output.metrics.skipped{reason="oversize_value_list"}` rather
  than split.
- **`hostname:` is the fallback for events that never came from `collectd_in`.** A relayed list
  already carries its origin's host on `collectd.host`, so a pure relay never needs this. A pipeline
  that also carries metrics from a `statsd_in`/`internal` does: without `collectd.host` or
  `host.name` and with no `hostname:` set, such a list is dropped and counted
  `logit.output.metrics.skipped{reason="no_host"}` with a `no_host` diagnostic. That is deliberate —
  collectd's receiver rejects an empty host, and inventing one would merge every unlabelled sender
  into a single host's metrics.

### `graphite_out`: relaying to Carbon

`graphite_out` is the sink to reach for when the destination is a real Carbon/Graphite listener
(or anything else speaking its wire protocols) rather than a general time-series database:
`graphite_in -> graphite_out` is a fixed point modulo the named normalization list in
[ADR `graphite-carbon-relay`](adr/graphite-carbon-relay.md). Unlike `collectd_out`, both transports
are supported — carbon's own plaintext listener (port 2003) speaks either UDP or TCP — and there is
a second wire protocol entirely, carbon's length-prefixed pickle batch format (port 2004, **TCP
only**: a length prefix has no meaning in a datagram, and `logit validate` rejects `protocol:
pickle` under `transport: udp`). See `graphite_in`'s own section above for the two runnable
examples pointing at this sink. Four things worth knowing before deploying one:

- **There is no `graphite.*` carrier, unlike `collectd_out`'s `collectd.*` or `syslog_out`'s
  `syslog.*`.** The wire path *is* [`MetricRecord::name`](design/data-model.md) — there is no
  separate `prefix:`/`template:` field and nothing to restore identity from if it changes downstream.
  This means a `lua`/`set` stage that renames a metric between `graphite_in` and `graphite_out`
  **silently changes the series carbon stores it under** — there is no wire fact left to notice the
  rename against, unlike a collectd or syslog relay, where the identity attributes ride alongside
  the (possibly transformed) rest of the event. If a pipeline renames metrics on the way through,
  that rename *is* the intended new wire path; there is no way to keep the old one going out this
  sink.
- **`tags: carbon` (the default) against a pre-1.1 Graphite silently corrupts data on disk.**
  Carbon versions before 1.1 have no tag support at all, and their whisper backend takes whatever
  the plaintext path contains straight into a filesystem path — a `;env=prod` tag suffix becomes
  literal `;` characters in a **whisper directory name**, not a rejected line. There is no error to
  see; `carbon-cache` simply creates directories nobody intended. If the destination might be an
  older Graphite, set `tags: drop` — every attribute is then omitted from the wire entirely (counted
  `logit.output.tags.dropped{reason="dialect"}`), which is the escape hatch this switch exists for.
  Confirm the destination's tag support before turning `tags: carbon` on against an unfamiliar
  cluster.
- **`multi_value: skip` (the default) drops anything carbon's one-number-per-datapoint wire can't
  carry** — `Samples`, `Distribution`, `Histogram`, `ExponentialHistogram`, `Summary`, `Set`, and
  `SetMembers` records are all dropped whole and counted
  `logit.output.metrics.skipped{metric_kind=...}` rather than guessing at a convention. Set
  `multi_value: expand` to render the dotted sub-paths `logit_proto::graphite`'s module doc tables
  instead (`.count`, `.sum`, `.q0_5`...`.q0_99`, per-bucket counts, and so on) — an explicit,
  named convention rather than a silent default, counted
  `logit.output.metrics.degraded{metric_kind=...}` once per record.
- **`max_packet_bytes:` (UDP only, default `1432`) bounds a datagram, not a single line**, the same
  shape as `statsd_out`'s own setting; `max_frame_bytes:` (default `1MiB`, Twisted's own
  `Int32StringReceiver.MAX_LENGTH`) bounds one pickle frame instead, and applies regardless of
  transport since pickle is TCP-only anyway. `connect_timeout:` (TCP only, default `5s`) is
  `statsd_out`'s/`syslog_out`'s own default. This sink is also the first non-HTTP sink with a real
  destination to report `duplicate_safe: true` (`null_out` reports it trivially, having no
  destination) — whisper is last-write-wins per `(path, second)`, so a
  redelivered datapoint on retry simply overwrites itself with the same number rather than
  double-counting, unlike a collectd COUNTER or a statsd `|c`. That argument is specifically about
  whisper's own storage semantics, not the carbon wire protocol in the abstract — a non-whisper
  Graphite-protocol receiver could treat a redelivered datapoint as an addition instead, and this
  sink has no way to tell the difference.

### `statsd_out`: `transport: tcp` and TLS

`statsd_out` defaults to UDP, like every statsd client, and
[`examples/statsd-relay.yaml`](../examples/statsd-relay.yaml) is the runnable topology with every
default present as a commented reference. `transport: tcp` swaps the packed datagram for one
LF-terminated line per metric on a lazily-opened connection, and is what a `tls:` block requires:

```yaml
components:
  statsd_out:
    type: statsd_out
    sources: [enrich]
    endpoint: relay.internal:8125
    transport: tcp                  # udp (the default) | tcp -- tls: below requires tcp
    connect_timeout: 5s             # the default; bounds the connect and the handshake separately
    tls:                            # presence turns TLS on and makes it *required*
      ca_file: /etc/logit/tls/ca.pem          # trust this CA instead of the bundled Mozilla set
      cert_file: /etc/logit/tls/client.pem    # mutual TLS; needs key_file too
      key_file: /etc/logit/tls/client.key
```

- **`tls:` is TCP-only, and its presence makes TLS required.** There is no plaintext fallback, and
  `logit validate` rejects a `tls:` block under `transport: udp` (rule 52 — DTLS is out of scope
  everywhere in this project). No statsd client in the wild speaks TLS, so — exactly like
  `statsd_in`'s own listener block — this is for a `logit`-to-`logit` or stunnel-shaped relay hop,
  not for an application's DogStatsD client. See ["TLS"](#tls) below for the full field reference.
- **`connect_timeout:` bounds the TCP connect and the TLS handshake as two separate phases**, not
  one combined deadline, so a TLS connect can take up to twice the configured value —
  `syslog_out`'s arrangement. Size it accordingly if raising it from the default.
- **A retry never redelivers a batch over TLS.** On plaintext a write that fails having accepted
  zero bytes is provably retryable, so this sink reconnects once and rewrites the frame
  (`Fault::Clean`). A TLS write gives no such proof — rustls may already have put complete records
  on the wire — so every failure at or after the first write is `Fault::Ambiguous` and the batch is
  never resent ([ADR `statsd-output`](adr/statsd-output.md)'s TLS amendment). That is deliberately
  conservative: `statsd_out` reports `duplicate_safe: false` because a redelivered `hits:5|c`
  *increments the destination counter a second time*. Expect a TLS relay to drop a batch where a
  plaintext one would have retried it, and watch `logit.component.batches.dropped` accordingly.
- **What to watch.** `logit.output.requests{class="ok"|"error"}` (one per attempt) and, on TCP,
  `logit.output.reconnects` — it should stay near zero in steady state; a climbing count means the
  peer or the network, not this sink, is unstable. It is counted identically on a plaintext and a
  TLS connection, since both take the same connect path. `logit.output.datagrams` exists only
  under `transport: udp`.

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

`auto` (the default) uses `inotify` where available (Linux only), falling back to polling
(`watch_error` diagnosed) if `inotify` setup fails; `poll` always uses the `poll_interval` tick (1s
default) instead, with no OS-specific dependency — the right choice over some network/FUSE mounts,
where `inotify` events don't reliably fire; `inotify` fails startup outright on setup failure
rather than degrading silently. Reading more bytes off an already-tracked file is never gated by
`poll_interval` alone: the driver's own read loop runs on every iteration regardless of what woke
it, so once something wakes it — a content change on a tracked file's own watch, or a poll tick —
it reads everything currently available.

What `inotify` actually watches is deliberately narrow: one watch on the single directory a
pattern reaches (`paths:` for `tail_in`, `root` for `docker_in`), plus one watch per file the
listener currently has open — nothing for a container this listener isn't tailing, and nothing
that wakes on a write to a file that isn't being tracked. For `docker_in` specifically, that
directory watch on `root` alone is enough to catch a container's own directory arriving or leaving
near-instantly (Docker's per-container state directories are direct children of `root`), but *not*
enough to catch a log file's own first appearance inside an already-existing container directory,
a rotation, or a `config.v2.json` change — those three ride `poll_interval` regardless of `watch`
mode. This is a deliberate trade: a design that additionally watched every container's own
subdirectory would catch all three near-instantly too, but at a cost of O(containers on the host)
work for every log line written anywhere on the host, selected or not — see [ADR
`docker-container-identity-and-minimal-watches`](adr/docker-container-identity-and-minimal-watches.md).

### What to watch

- `logit.input.files.open` (gauge) — how many files this listener currently has open. Zero when a
  `docker_in` config's `containers:`/`discover:` selection matches nothing, or a `tail_in` config's
  `paths:` glob matches no files yet — both silent by design (a directory that doesn't exist yet is
  the ordinary "not there yet" case, retried next cycle), so this is the number to alert on if
  "nothing is flowing" needs to be distinguished from "nothing to flow yet."
- `logit.input.watch.watches` (gauge) — the size of the watch set this listener maintains: the
  watched directory, plus one entry per file currently open. Under `watch: poll` this counts the
  same set with zero real `inotify` descriptors behind it (`Watcher::watch_dir`/`watch_file` are
  no-ops in that mode) — it reflects the *intended* watch set, not live kernel watches, so a `poll`
  config still shows the directory held even though nothing is actually registered. Under
  `inotify`/`auto` the two coincide. Either way, proportional to what's actually being tailed, not
  to how much any of it writes — the number that makes "the watch set stays minimal" checkable
  from outside.
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
  `container.id`-only resource instead of the full identity. A file that isn't there at all is
  retried on every poll tick; one that exists but wouldn't parse is retried when its own stat next
  changes, since the failed read is cached against that stat exactly as a successful one is (a
  torn read racing the daemon's own rewrite is therefore picked up as soon as the rewrite lands).
  Either way this fires once per failure, not once per tick for as long as it persists.
- `logit.input.files.identity_changed` / `.deselected` (count, `docker_in` only) — a container's
  identity (name, image, or a watched label) changed, or a tracked container was renamed out of
  `containers:` and stopped flowing. The matching `container_renamed`/`container_deselected`
  diagnostics name which container and, for a deselection, that it's process-local: a `logit`
  restart before the container is renamed back loses the retained resume offset.

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

## Prometheus remote-write: receiving, sending, and picking a version

`prometheus_in` and `prometheus_out` are each two components in one, with the mode chosen by which
field is set and a config error — not a silently ignored setting — if a field of the *other* mode
comes along with it (graph rules 55 and 56). `prometheus_in` scrapes `scrape_targets:` or binds a
remote-write **receiver** on `bind:`; `prometheus_out` exposes a registry on `bind:` or **sends**
remote-write to an `endpoint:`. See [ADR `prometheus-remote-write`](adr/prometheus-remote-write.md)
for the design and
[`examples/prometheus-remote-write-receive.yaml`](../examples/prometheus-remote-write-receive.yaml)/
[`examples/prometheus-remote-write-send.yaml`](../examples/prometheus-remote-write-send.yaml) for
runnable configs.

**The receiver's bind posture is the exposition server's, not the scrape client's.** `bind_tls:`
gives it real server TLS, and that is transport security and nothing else: there is no bearer token,
no basic auth, and no mutual-TLS identity check beyond `rustls` accepting whatever chain a client
presents when `client_ca_file` is set. Anything that can reach the socket can write series into the
pipeline. So bind loopback or pod-local — `127.0.0.1:9201`, as
[`examples/prometheus-remote-write-receive.yaml`](../examples/prometheus-remote-write-receive.yaml)
does — and front it with something that authenticates (an ingress, a service mesh, an
authenticating reverse proxy), exactly the posture `admin:` and `prometheus_out`'s exposition
`bind:` already take. An operator who
needs it reachable from off-host is making that choice deliberately rather than inheriting it from
an example. Tracked in `docs/known-gaps.md`.

```yaml
components:
  rw_in:
    type: prometheus_in
    bind: 127.0.0.1:9201
    path: /api/v1/write      # default; the route POSTs are accepted on
    idle_timeout: 60s        # off by default -- set it, see below
    metadata_cache:          # what 1.0 metric types are remembered between requests
      max_families: 10000    #   0 turns the cache off entirely
      ttl: 10m
```

**Set `idle_timeout:` on a remote-write receiver.** It is opt-in across every listener
([ADR `idle-connection-timeout`](adr/idle-connection-timeout.md)), and its own rule — recommend it
on wherever consistent traffic is expected — describes a remote-write listener exactly: senders
write on a fixed cadence, so a connection quiet for a minute is a connection that is not coming
back. It also does a second job here that nothing else does: the bound on a request whose **body
stalls mid-upload** is derived from this field, so with `idle_timeout:` unset a half-uploaded
request holds one of the listener's 1024 connection permits until the sender goes away, and the
`408` the routes table describes never fires. Size it above the senders' longest normal gap;
`60s` is comfortable for Prometheus's default `remote_timeout` of 30s.

A Prometheus writing into that needs a `remote_write:` block of its own and nothing else — it is the
sender, so no server-side flag is involved:

```yaml
remote_write:
  - url: http://logit:9201/api/v1/write
    # protobuf_message: io.prometheus.write.v2.Request   # omit for 1.0
```

**Keep `metadata_cache:` on for a 1.0 fleet.** Prometheus's own 1.0 sender ships a family's type,
`# HELP` and `# UNIT` in *separate* requests on its own schedule (`metadata_config`, once a minute
by default) rather than attached to the samples they describe, so a receiver that remembers nothing
decodes nearly every 1.0 request as untyped `unknown` families — and a histogram arrives as three
unrelated `_bucket`/`_sum`/`_count` series instead of one record. No samples are dropped either way;
what you lose without it is the metric *kinds*, and with them the ability to write a rate over a
counter or a quantile over a histogram downstream. The cache is what fixes that;
`max_families: 0` turns it off, which is the right setting only for a pure-2.0 fleet. Watch
`logit.input.metadata_cache.evicted{reason="expired"}` against a live sender: a steady stream there
means `ttl` is shorter than that sender's metadata cadence, and families are lapsing back to untyped
between refreshes.

**`--web.enable-remote-write-receiver` is the *other* direction's flag.** It belongs on the
Prometheus side when `logit` is the **sender** — a stock Prometheus does not accept remote-write at
all until it is started with it:

```yaml
components:
  rw_out:
    type: prometheus_out
    sources: [enrich]
    endpoint: http://prometheus:9090/api/v1/write   # absolute URL, write path included
    version: 1                                      # default
    timeout: 10s
    headers:
      X-Scope-OrgID: tenant-a                       # e.g. a Mimir tenant; !env works on a value
```

`endpoint:` is the receiver's full write URL with its path, not a host and a separate `path:` —
`path:` belongs to the *other* mode of this kind and setting it here is rule 56. TLS is selected by
the scheme, and `endpoint_tls:` tunes it (a private CA, a client certificate, or the deliberately
awkward `insecure_skip_verify`, which logs a startup warning). The four protocol headers
(`Content-Type`, `Content-Encoding`, `X-Prometheus-Remote-Write-Version`, `User-Agent`) plus
`Content-Length` are reserved: rule 56 rejects them in `headers:` at config time rather than letting
the sink silently override what config asked for.

**Choosing `version: 1` or `2`.** There is no negotiation and no fallback — the operator picks the
one their receiver speaks, exactly as they already pick an exposition dialect — so the choice is
about the destination, not about `logit`:

- **`version: 1`** (`prometheus.WriteRequest`) is the default, and is what every remote-write
  receiver deployed today accepts. Pick it unless you know the far end speaks 2.0. Its one real cost
  is that 1.0 has no field for a counter's start time, so `Series::created` (an OpenMetrics
  `_created` series, an OTLP `start_time_unix_nano`) is dropped on the way out.
- **`version: 2`** (`io.prometheus.write.v2.Request`) is worth setting when the receiver is a recent
  Mimir, Thanos, VictoriaMetrics, Grafana Cloud, or a Prometheus 3.x started with
  `--web.enable-remote-write-receiver`. It interns every label and metadata string in a request-wide
  symbol table (smaller bodies for the same series), carries `Metadata` inline on each series rather
  than in separate requests, carries the created timestamp per sample, and answers with
  `X-Prometheus-Remote-Write-{Samples,Histograms,Exemplars}-Written` so a sender learns what was
  actually stored. A receiver that does not speak it answers `415`, which this sink classifies
  permanent — a misconfiguration reported immediately rather than retried.

Native histograms are skipped and counted on both wires regardless of version
(`docs/known-gaps.md`), so nothing about this choice affects them.

**What to watch.** Receiver: `logit.input.writes{class}` (`ok` against `bad_request`/`unsupported`/
`oversize` — a non-zero `unsupported` is usually a sender whose `Content-Type` or
`Content-Encoding` doesn't match what it is actually sending), `logit.input.write.duration`,
`logit.input.samples`, and the `metadata_cache` trio above. Sender: `logit.output.requests{class}`
(a `4xx` is permanent and the batch is dropped — the throttled `remote_write_rejected` diagnostic
quotes the receiver's own message, which for Prometheus and Mimir names the offending series; a
`3xx` means the endpoint is redirecting and this client deliberately does not follow it),
`logit.output.request.duration`, `logit.output.samples`. A sender feeding one series from two
upstream branches can draw out-of-order `400`s from a receiver with no out-of-order window: that is
the topology, not the sink, and `docs/known-gaps.md` has the row.

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

`otlp_in.handshake_timeout` (default 5s) bounds that handshake: a client that completes the TCP
connect and then never sends a ClientHello is closed and its concurrency-cap permit released. It
also applies to a plaintext `otlp_in` — there is no TLS context clause rejecting it under rule 45
the way there is on `syslog_in`/`graphite_in`/`statsd_in`'s `transport: udp` — where it bounds the
wait for the connection's very first byte instead, via a non-consuming `TcpStream::peek` rather
than a read, so the byte is still there for `hyper`'s own version sniff afterwards. See
["`handshake_timeout` on a TCP listener"](#handshake_timeout-on-a-tcp-listener) above for why, and
for what that leaves open — which `otlp_in.idle_timeout` (off by default) closes: see
["`idle_timeout` on a TCP listener"](#idle_timeout-on-a-tcp-listener) above, including the note on
a request that starts right at the idle deadline.

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

### syslog (RFC 5425)

`syslog_in`/`syslog_out` can speak TLS too — RFC 5425, syslog framed per RFC 6587 carried over TLS
over TCP — see [ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md). Same shape as
`logit_in`/`logit_out` just above: both `bind` and `endpoint` are bare `host:port` strings with no
URL scheme to read a TLS signal from, so a `tls:` block's mere **presence turns TLS on and makes it
required** — there is no plaintext fallback once one is configured — and it applies to
`transport: tcp` only; DTLS (syslog over TLS over UDP) is out of scope, so `tls:` under
`transport: udp` is a config error rather than a silently ignored block. The fields are the same
`TlsServerConfig`/`TlsClientConfig` pair every other TLS-capable component uses:

```yaml
# sender
components:
  syslog_out:
    type: syslog_out
    sources: [enrich]
    endpoint: collector.internal:6514   # RFC 5425's registered port
    transport: tcp
    tls:
      ca_file: /etc/logit/tls/ca.pem    # trust this CA instead of the bundled Mozilla set
```

```yaml
# collector
components:
  syslog_in:
    type: syslog_in
    bind: 0.0.0.0:6514
    transport: tcp
    tls:
      cert_file: /etc/logit/tls/server.pem
      key_file: /etc/logit/tls/server.key
      client_ca_file: /etc/logit/tls/ca.pem   # omit for server-auth-only TLS
```

Mutual TLS adds a client certificate on `syslog_out`'s `tls:` block, exactly `logit_out`'s example
above (`cert_file`/`key_file` together). `tls.insecure_skip_verify` (`syslog_out` only, same
contradictory-with-`ca_file` rejection) behaves identically too.

`syslog_out.connect_timeout` bounds the TCP connect and the TLS handshake as two separate phases,
not one combined deadline — a TLS connect can therefore take up to twice the configured value
([ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)'s amendment). Size it
accordingly if raising it from the default.

`syslog_in.handshake_timeout` (default 5s) is the receiving side's own version of the same
arrangement: one budget of that length for the TLS accept, then a fresh one for the wait for the
connection's first byte, so a TLS peer that connects and then goes quiet is dropped after at most
10s. It applies on the plaintext TCP arm too (where only the first-byte phase exists), and not at
all under `transport: udp`. See
["`handshake_timeout` on a TCP listener"](#handshake_timeout-on-a-tcp-listener) above.
`syslog_in.idle_timeout` (off by default) bounds the gap after that — see ["`idle_timeout` on a TCP
listener"](#idle_timeout-on-a-tcp-listener) above.

**What to watch.** `syslog_out`: `logit.output.requests{class="ok"|"error"}` (one per attempt) and
`logit.output.reconnects` (should stay near zero in steady state — a climbing count on a TLS
connection means the peer or the network, not this sink, is unstable; counted identically on a
plaintext and a TLS connection, since both take the same connect path). `syslog_in`:
`logit.input.connections` (a gauge — should match the number of `syslog_out` peers actually
connected) and `logit.input.connections.rejected{reason="limit"}` (nonzero means the 1024
-connection cap is binding). Both: a handshake failure, a framing violation, or an oversize/
malformed frame all surface through
`logit.component.diagnostics{key="connection_error"|"framing_error"}` and
`logit.input.frames.dropped{reason="oversize"|"malformed"|"truncated"}` — there is no separate
TLS-specific counter, the same call this section's `otlp_in`/`otlp_out` paragraphs already make.
`docs/known-gaps.md` tracks what's still open: DTLS, certificates read once at startup, and no
`server_name` override; the post-handshake idle case is closed by `idle_timeout` above.

A **TCP `graphite_in`** takes the identical `tls:` block, because it runs on the same listener
driver ([ADR `graphite-carbon-relay`](adr/graphite-carbon-relay.md)'s 2026-09-14 amendment):
`cert_file`/`key_file`, optional `client_ca_file` for mutual TLS, presence turns TLS on and makes
it required, `transport: tcp` only. There is no matching `graphite_out` half — carbon's own senders
speak no TLS, so the listener side is for a `logit`-to-`logit` or stunnel-shaped relay hop. What to
watch is the same set as `syslog_in`'s above, `logit.input.frames.dropped{reason}` included.

A **TCP `statsd_in`** is the third listener on that same driver, and takes the same block on the
same terms — see ["`statsd_in`: `transport: tcp` and
TLS"](#statsd_in-transport-tcp-and-tls) above for the listener's own framing and sizing behaviour.
Plain statsd clients speak no TLS either, so this too is a `logit`-to-`logit` or stunnel-shaped
relay hop rather than something an application's statsd client dials directly. What to watch is
again `syslog_in`'s set.

**`statsd_out` completes that pair**, and is the sink half of the same hop
([ADR `statsd-output`](adr/statsd-output.md)'s TLS amendment): the identical `TlsClientConfig`
`syslog_out`/`logit_out` take, `transport: tcp` only, presence turns TLS on and makes it required,
`connect_timeout` bounding the connect and the handshake as two separate phases, and
`insecure_skip_verify` behaving (and warning) exactly as it does on those sinks. See
["`statsd_out`: `transport: tcp` and TLS"](#statsd_out-transport-tcp-and-tls) above for the one
behaviour that is *not* shared with the other sinks — a TLS write failure is `Fault::Ambiguous` and
the batch is never resent, because a redelivered statsd counter corrupts a value rather than
duplicating a line.

Which component takes which block, in one place:

| Component | Block | Turned on by | Notes |
|---|---|---|---|
| `otlp_out` | `TlsClientConfig` | an `https://` `endpoint` | `tls:` under a plaintext endpoint is rule 22 |
| `logit_out`, `syslog_out`, `statsd_out` | `TlsClientConfig` | the block's presence | bare `host:port`; stream transport only (rules 34/44/52) |
| `prometheus_in` (`scrape_tls:`) | `TlsClientConfig` | an `https://` scrape target | scrape mode is a client, not a listener; a set block with no `https://` target is rule 40 |
| `prometheus_in` (`bind_tls:`) | `TlsServerConfig` | the block's presence | the remote-write receiver's own listener; bind mode only (rule 55) |
| `otlp_in` | `TlsServerConfig` | the block's presence | both transports |
| `syslog_in`, `graphite_in`, `statsd_in` | `TlsServerConfig` | the block's presence | `transport: tcp` only (rule 43) |

`TlsClientConfig` is `ca_file`/`cert_file`/`key_file`/`insecure_skip_verify`; `TlsServerConfig` is
`cert_file`/`key_file`/`client_ca_file`. Every path resolves relative to the config file's own
directory and accepts `!env`. There is no `collectd_out`/`graphite_out` row: collectd's `network`
plugin is UDP-only, and carbon's own senders speak no TLS.

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
means *this* side gives up first, not that the connection is unsafe. That far-end grace is
`logit_in.handshake_timeout` (default 5s) and it is *per pre-`Hello` phase*, applied independently
to the TLS accept and to the `Hello` read that follows it -- so a TLS peer that connects and then
goes silent is dropped after at most 10s, not 5s. See
["`handshake_timeout` on a TCP listener"](#handshake_timeout-on-a-tcp-listener) above; it is a
pre-`Hello` bound only. What bounds an already-handshaken connection that goes quiet is the
separate, opt-in `logit_in.idle_timeout` (off by default) -- see ["`idle_timeout` on a TCP
listener"](#idle_timeout-on-a-tcp-listener) above; a `logit_out` peer sees `Reject{GOING_AWAY,
"idle for <dur>"}` before that close and probes for exactly that signal before reusing a pooled
connection, so an idle-timed-out `logit_in` costs `logit_out` a reconnect, not a lost batch.
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
