# Deploying `logit`

This guide covers running `logit` outside this repo's dev stack: getting the image, running and
validating a config, probing readiness, and what `logit` does when a sink, a sender, or an
orchestrator's signal doesn't cooperate. To point a real nginx at a running `logit`, see
[the nginx-side recipe](#the-nginx-side-recipe). To see `logit` running without deploying it, use
[`demo/`](../demo/README.md), a self-contained `docker compose up` with no image-building steps.

## Getting the image

Pull it from GHCR:

```sh
docker pull ghcr.io/ross/logit:latest
```

`latest` is the only published tag. It is amd64 only, built and pushed by hand
([ADR `publish-release-image-to-ghcr`](adr/publish-release-image-to-ghcr.md)), and moves whenever
someone dispatches that workflow. Treat it as "the current build," not a pin: two pulls a week
apart can return different images.

To build it yourself, `script/image [tag]` builds the production runtime image from `Dockerfile`
and tags it `logit:<tag>` (default `local`). `Dockerfile.dev` is the contributor dev environment,
not this image ([ADR `containerized-development`](adr/containerized-development.md)).

```sh
script/image        # -> logit:local
script/image v0.1.0  # -> logit:v0.1.0
```

## Running it

The image's `ENTRYPOINT` is `["logit"]`, so the subcommand and a config path are the whole
invocation. Mount the config read-only; it isn't baked into the image:

```sh
docker run --rm \
  -v /path/to/config.yaml:/config.yaml:ro \
  -e INFLUXDB_TOKEN=... \
  ghcr.io/ross/logit:latest run /config.yaml
```

Put secrets and deployment-specific values (a token, a URL, a bind address) in the environment and
reference them with `!env VAR_NAME` in the config instead of inlining them. Any field on any
component accepts `!env`, not only `influxdb_out`'s `token`; see
[ADR `env-yaml-tag`](adr/env-yaml-tag.md) for the mechanism and its edge cases. Pass the variables
to the container with `-e` or `--env-file`.

## `logit validate` as a preflight

Before restarting a running `logit` with a new config, validate the candidate. Pass the same
environment `run` gets, because `validate` also needs every `!env` reference to resolve:

```sh
docker run --rm \
  -v /path/to/new-config.yaml:/config.yaml:ro \
  -e INFLUXDB_TOKEN=... \
  ghcr.io/ross/logit:latest validate /config.yaml
```

`validate` runs the same resolution and validation path as `run` (`graph::resolve`, invoked from
`validate_semantics` in `crates/logit-cli/src/pipeline.rs`), so a config that validates can't fail
that stage at `run`.

**`validate` doesn't open referenced files.** `lua_file`, a `stdio_out`/`file_out` path, and
`otlp_out`/`otlp_in`'s `tls.*_file` fields are read only when `run` constructs the component. A
mistyped `tls.ca_file` path passes `validate` and fails at startup instead, with the path in the
error.

## Signal and restart behavior

[ADR `service-lifecycle-and-output-retry`](adr/service-lifecycle-and-output-retry.md) has the full
design. What an operator needs:

- **SIGTERM or SIGINT starts a graceful drain**, not an immediate kill. Every listener's inbox
  closes as if the listener had finished on its own, which flushes any in-flight `aggregate` window
  before exit. An orchestrator sending SIGTERM ahead of SIGKILL doesn't silently drop a window of
  metrics.
- **A second signal during a stuck drain exits immediately** with status 130, so a restart policy
  waiting on the process can still kill it with the same signal.
- **A sink failure, transient or extended, doesn't end the process by default.** Every sink sits
  behind a decoupled delivery buffer with its own retry budget
  ([ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md), which revises ADR
  `service-lifecycle-and-output-retry`'s retry-budget rationale without superseding its other
  decisions). The one case that still exits is a sustained, purely configuration-error failure;
  see [Sink delivery buffering](#sink-delivery-buffering).

## Probes and exit codes

`logit` exits with a code that separates startup failures from runtime ones, and, when `admin:` is
configured, answers readiness and liveness probes. See
[ADR `admin-readiness-endpoint`](adr/admin-readiness-endpoint.md) for the design.

| Exit code | Meaning |
|---|---|
| `0` | Clean shutdown — a signal arrived, every listener drained, every sink flushed. |
| `1` | A startup failure — a bad config, a port already in use, a bad `lua_file`, a bad `--log-level`. Nothing was ever running. |
| `2` | A runtime failure after the process reported ready — a sustained, purely-configuration-error sink failure (see [Sink delivery buffering](#sink-delivery-buffering) below), a listener's accept loop dying, a `lua`/`lua_file` component's thread panicking (a script's own `process()`/`flush()` errors are not this: they're logged and counted, never fatal). |
| `130` | A second SIGTERM/SIGINT arrived before a graceful drain finished. |

To enable the probe endpoint, add a top-level `admin:` block:

```yaml
admin:
  bind: 0.0.0.0:9600
```

**The endpoint has no TLS and no auth.** It is meant to be loopback or pod-local, not exposed
across a real network boundary.

`GET /readyz` returns:

- `200 ok` once every listener and every listening sink is bound and every node task is running.
- `503 starting` before that.
- `503 draining` after a shutdown signal.
- `503 degraded` if any node has exited with an error while the process is still draining.

`GET /healthz` returns `200 ok` whenever the admin task itself can answer, regardless of the
pipeline's state.

Add `?format=json` to `/readyz` for `{status, since, components: {id: "pending"|"bound"|
"running"|"finished"|"failed"|"alias"}}` instead of the bare status word. `/healthz?format=json`
returns only `{status}`, since it has nothing else to report. A `target` component
([ADR `target-components`](adr/target-components.md)) is always `alias` and nothing else: it has no
task and no inbox, only a name for its routers' outbound edges, so its liveness is theirs.

In Kubernetes, map the two routes onto the two probes:

```yaml
readinessProbe:
  httpGet: { path: /readyz, port: 9600 }
  periodSeconds: 5
livenessProbe:
  httpGet: { path: /healthz, port: 9600 }
  periodSeconds: 10
```

For a container-level health check, use `logit ready [--admin http://127.0.0.1:9600]`. The shipped
image is `bookworm-slim` with no `curl`, so `Dockerfile`'s `HEALTHCHECK` runs this instead:

```dockerfile
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s CMD ["logit", "ready"]
```

On `200` it prints the status word and exits 0. Otherwise it exits 1 and prints the status word the
server returned, or the connection error if nothing is listening (for example, `admin:` isn't
configured).

### What to watch on `/readyz`

- **`/readyz` stuck at `503 degraded`** means a node has failed, not that a sink is retrying. See
  [Sink failure semantics](#sink-failure-semantics-degrade-to-dropping-dont-exit) for what does
  and doesn't trip it.
- **`/readyz` never returning `200` within the orchestrator's startup timeout** means a listener,
  or a listening sink like `prometheus_out`, can't bind, or a Lua script fails to load. Check the
  `starting`/`bound`/`ready` lifecycle log lines in [Self-logging](#self-logging).

## Self-logging

`logit run` emits leveled, structured self-diagnostics through `tracing`
([ADR `tracing-for-self-logging`](adr/tracing-for-self-logging.md)). `schema`, `validate`, and
`graph` only print, since they run once and exit.

```sh
logit run /config.yaml --log-level info --log-format text   # the defaults
logit run /config.yaml --log-level debug                    # or LOGIT_LOG=debug
logit run /config.yaml --log-format json                    # one JSON object per line
```

`--log-level` (or `LOGIT_LOG`) takes `tracing`'s `EnvFilter` syntax: a bare level (`info`,
`debug`) or a per-module override (`logit_pipeline=trace,info`). `--log-format json` writes one
JSON object per line with `timestamp`, `level`, `target`, `component`, `key`, and `message` fields,
so a log collector can parse it instead of scraping text.

Every component-scoped diagnostic carries a `component` field naming the component that reported
it. A throttled diagnostic, or a component-owned lifecycle message like `bound`/`recovered`, also
carries a `key` naming *why*. The process-level lifecycle events (`starting`, `ready`,
`shutdown signal received`, `drain complete`, `exiting`) carry neither, because they describe the
process, not a component.

**Lifecycle event names are stable `&'static str` values, so you can alert on them directly:**

| Event | Level | When |
|---|---|---|
| `starting` | info | Config loaded, before graph resolution — named even if the config goes on to fail. |
| `bound` | info | One component's socket opened, during the pre-bind pass — listeners (`syslog_in`/`statsd_in`/`collectd_in`/`graphite_in`/`otlp_in`/`logit_in`, and `prometheus_in` in receiver mode; `tail_in`/`docker_in` emit none) and sinks that listen (`prometheus_out`). A `collectd_in` (or any UDP listener) whose `bind` names a multicast group says so, naming the group it joined. |
| `ready` | info | Every socket bound, every node task running, nothing has failed. |
| `shutdown signal received` | info | A SIGTERM/SIGINT arrived. |
| `drain complete` | info/warn | Every node has exited after a shutdown or failure — `warn` if any batch was dropped mid-drain. |
| `degraded` | warn | A sink's first dropped batch (its retry budget exhausted) since it was last healthy. |
| `recovered` | info | A sink's first successful delivery after `degraded`. |
| `exiting` | info/error | The process is about to exit — `info` at `0`, `error` at any failure code (`1` or `2`). A config error that fails before the pipeline starts exits without this line. |

To ship `logit`'s own logs with no separate log-shipping setup, use the `internal` component's
`logs:` setting (`warn` by default, `error`, or `off`). It routes every `warn`-or-above
self-diagnostic into the pipeline as an ordinary log event, alongside `internal`'s points and spans;
any sink attached to `internal` (directly or through a `keep`/`aggregate`/`lua`) carries them like
any other signal. See [`docs/design/internal-telemetry.md`](design/internal-telemetry.md)'s "Logs"
section.

## Sink delivery buffering

Every sink (`influxdb_out`, `stdio_out`, `file_out`, ...) sits behind its own delivery queue,
in memory by default, that decouples receiving events from delivering them
([ADR `buffered-sink-delivery`](adr/buffered-sink-delivery.md)). The queue lets `logit` ride out a
slow or temporarily down destination instead of stalling or killing the whole pipeline.

To tune it, add a `buffer:` block to the sink; see the commented example in
[`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml). Validation rejects
`buffer:` on anything but a sink. Every field has a default, so omitting `buffer:` gives the values
in this section. To make the queue survive a restart, see [Durable buffering](#durable-buffering).

### Sink failure semantics: degrade to dropping, don't exit

A sink that can't reach its destination drops and counts batches; it doesn't end `logit run`:

- **A retryable failure** (per the sink's fault classification and delivery posture) is retried
  within `retry_budget` (60s by default), then the batch is dropped and counted.
- **A non-retryable failure**, including retry-budget exhaustion, drops the batch, counts it, and
  logs a throttled warning. The writer moves on to the next batch; the rest of the pipeline and
  every other sink keep running.
- **The one exception exits the process.** If a sink sees *only* configuration-error failures (a
  bad token, a bad bucket: failures no retry can fix) for a sustained ~60-second window with no
  success in between, `logit run` exits. This is deliberate: a misconfigured sink should fail
  loudly enough for a restart-policy supervisor to notice, not drop every batch forever. A slow or
  temporarily down destination never trips this; only a failure `logit` can identify as a
  configuration problem does.
- **On SIGTERM/SIGINT**, each sink gets up to `shutdown_grace` (5s by default) to drain its queue.
  Anything still queued at that deadline is dropped and counted.

### Sink buffer sizing: `max_bytes` × number of sinks

**`buffer.max_bytes` (64MiB default) bounds one sink's queue**, in RAM for the in-memory default
or on disk for `buffer.disk:`. When you size the container's memory (or disk) limit, multiply it
by the number of sinks in the config, including several `influxdb_out`/`stdio_out` components fed
by different branches.

`buffer.max_batches` (1024 default) is a second, independent bound on an in-memory queue; whichever
trips first governs. A disk-backed queue drops that bound and uses `buffer.disk.max_bytes` alone
(graph validation rejects setting both).

Size for the worst outage you intend to ride out: make `max_bytes` deep enough to hold a real
destination outage's worth of data, weighed against the memory or disk you're willing to commit to
a sink holding data nothing can currently accept.

**`buffer.overflow` decides what happens when both bounds are full.** `block` (the default) applies
backpressure all the way back to intake instead of silently losing data. `drop_oldest` and
`drop_newest` trade data loss for keeping intake unblocked. For a destination you know is
unreliable, pick a policy deliberately instead of leaving the default.

### What to watch for sink buffering

Every component exposes its delivery metrics once the config has an `internal` component
(`docs/design/internal-telemetry.md`); there's no other opt-in. The two most actionable for
buffering:

- `logit.component.buffer.utilization` (gauge): the fill ratio of whichever of `max_batches`/
  `max_bytes` is closer to tripping. Sustained values near 1.0 mean the sink is falling behind its
  destination; under `block`, it is also back-pressuring intake.
- `logit.component.batches.dropped` (count, tagged `reason`): `overflow_oldest`/`overflow_newest`
  (a `drop_*` policy dropped something), `send_failed` (retry gave up on a batch), or `shutdown`
  (the queue still held data when `shutdown_grace` expired). Any sustained nonzero rate is data
  loss worth alerting on. The `reason` says whether the cause is an overflowing queue, a failing
  destination, or a slow drain racing shutdown.

### Durable buffering

An in-memory queue is lost on a restart, a `SIGKILL`, or a shutdown grace that expires mid-drain.
To keep it, add a `buffer.disk:` block, which replaces that sink's queue with a crash-recoverable
spool on disk ([ADR `disk-backed-sink-buffer`](adr/disk-backed-sink-buffer.md)). A restart resumes
delivery from the last persisted read cursor and replays at most the batches committed since the
last checkpoint: at-least-once, the same trade `tail_in`'s checkpoint makes.

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
in-memory queue's loss window is a real cost. Don't enable it on every sink by default: it costs a
real `write` per batch (a `logit_proto::native` encode plus one file append) that an in-memory
queue never pays. Validation rejects a non-default `buffer.max_batches`/`buffer.max_bytes`
alongside `disk:`, because disk replaces the in-memory bound instead of sizing beside it.

**Put the spool directory on a volume that survives the container.** An ephemeral container
filesystem defeats the point, as it would for any durable state (`tail_in`'s checkpoint file in
`crates/logit-inputs/src/tail/checkpoint.rs`, a database's data directory).

**Durability level:** `logit` calls `fdatasync` on segment rotation, on the cursor file, and at
shutdown, not per push. A process crash (including `SIGKILL`) loses nothing already written; a
power loss can lose the most recent, not-yet-synced tail of the active segment.

**What to watch.** The metrics above still apply, with these differences:
`buffer.utilization`/`.bytes` are sized against `buffer.disk.max_bytes`; `batches.dropped` gains
the `reason`s `frame_too_large`, `disk_corrupt`, `disk_full`, and `disk_io_error`; and a
disk-backed sink never emits `reason="shutdown"`, because it drops nothing at shutdown. Also watch:

- `logit.component.buffer.disk.segments` (gauge): segment files currently on disk.
- `logit.component.buffer.disk.replayed` (count): records found between the resume point and the
  end of all segments, counted once at process start. After a clean start it should read zero
  from the first tick on. A nonzero value on every restart under normal operation means something
  keeps the queue from ever fully draining.
- `logit.component.buffer.disk.truncated` (count): a torn tail found and truncated at open. Nonzero
  means the previous process ended mid-write, which an ordinary `SIGKILL` does. Note it; don't
  alert on it alone.

## Listener intake

Every UDP listener (`collectd_in`, and `statsd_in`, `syslog_in`, or `graphite_in` with
`transport: udp`) sits in front of its own in-memory receive queue that decouples reading the
socket from decoding and batching what it read
([ADR `decoupled-listener-io`](adr/decoupled-listener-io.md)). It is the listener-side sibling of
[sink delivery buffering](#sink-delivery-buffering): it keeps the socket being read while a slow
or backed-up destination downstream is ridden out.

To tune it, add a `receive:` block to the listener; see the commented example in
[`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml). Every field has a
default, so omitting `receive:` gives the values below. Validation rejects `receive:` on any kind
except a datagram listener, a TCP listener, or a tail listener (`tail_in`/`docker_in`). A TCP or
tail listener has no receive *queue*, so only the four batch-assembly fields apply to it; see
[Tailing files and Docker logs](#tailing-files-and-docker-logs).

A TCP listener has no receive queue either; see
[`handshake_timeout` on a TCP listener](#handshake_timeout-on-a-tcp-listener) and
[`idle_timeout` on a TCP listener](#idle_timeout-on-a-tcp-listener) for the connection bounds it
has instead.

### Listener failure semantics: `drop_oldest`, not `block` — the opposite default from `buffer:`

**Leave `receive.overflow` at its default, `drop_oldest`, unless you specifically want
backpressure to reach the sender.** This is the opposite of `buffer:`'s `block` default, for a
reason. Behind a sink's queue is an in-process drain that can afford to wait. Behind a UDP
listener's queue is the kernel's socket receive buffer, which *can't* wait. Setting
`receive.overflow: block` doesn't prevent loss under sustained overload; it moves the loss from a
place `logit` can act on (`logit.component.datagrams.dropped`, a queue you can size) to one it can
only report (`logit.input.kernel.drops`, the kernel discarding datagrams before `recv_from` sees
them). Mature UDP listeners (syslog-ng, rsyslog, Telegraf, gostatsd) make the same choice, and most
can't report the second number at all.

- `overflow: drop_oldest` (the default) and `drop_newest` both keep reading the socket
  unconditionally and evict from the queue instead. `drop_oldest` favors fresh data over stale
  under sustained overload. `drop_newest` favors what's already queued, at the cost of losing a
  burst's whole tail once the queue fills, since a full queue then stays full.
- `overflow: block` stops calling `recv_from` once the queue is full. It is the only configuration
  in which the listener itself applies backpressure, and the only one in which
  `receive.push.blocked.duration` (below) records anything.
- On SIGTERM/SIGINT, the listener gets up to `receive.shutdown_grace` (5s by default) to decode and
  deliver what's still queued. Anything still queued at that deadline is dropped **uncounted**,
  because nothing is left running to count it.

### Listener sizing and `SO_RCVBUF`

The receive queue holds undecoded bytes, not decoded events, and is bounded by
`receive.max_bytes` (32MiB default) and `receive.max_datagrams` (10,000 default), whichever trips
first.

One layer downstream, `receive.batch_max_events`/`batch_max_bytes` (1,000 / 1MiB default) bound
how much the listener accumulates across datagrams before sending one batch on.
`batch_flush_interval` (100ms default) caps the wait regardless of size, so a quiet listener never
holds data waiting to fill a batch.

`receive.receive_buffer_bytes` requests a specific `SO_RCVBUF` at bind time. If omitted (the
default), the kernel's default is left alone. Linux doubles the requested value for its own
bookkeeping, so a successful request usually reports back about 2× what you asked for; `logit`
accounts for that when deciding whether to warn. **If you set it and see a startup warning naming
`net.core.rmem_max`**, that sysctl is clamping the request; raise it to get the full size. The
granted value is always gauged (`logit.input.receive_buffer.bytes`), even without an override, so
you can see the kernel default before deciding whether to raise it.

### `read_batch`: how many datagrams one syscall takes

`receive.read_batch` (64 by default) sets how many datagrams one `recvmmsg(2)` call may return, and
also how many the decode half takes off the receive queue at a time: one knob for both ends of one
queue. `read_batch: 1` reads one datagram per syscall. See
[ADR `udp-intake-batching-and-socket-visibility`](adr/udp-intake-batching-and-socket-visibility.md)
for the design.

```yaml
components:
  statsd_in:
    type: statsd_in
    bind: 0.0.0.0:8125
    receive:
      read_batch: 64             # the default
```

**It only takes effect on Linux.** `recvmmsg` is a Linux syscall; on other targets the read loop
takes one datagram per `recv_from`, and this field is parsed, validated, and ignored, so one config
stays portable. The decode-side batch it also sets applies everywhere.

`logit validate` rejects `0` and anything above `1024`. That ceiling matches `UIO_MAXIOV` but is
`logit`'s own limit, not the kernel's: it bounds the per-listener buffer slab (below) and how many
datagrams a shutdown can discard mid-push.

**To decide whether raising it helps, watch the mean fill.** Divide `logit.input.datagrams` by
`logit.input.reads` to get how many datagrams an average syscall returned:

- **A fill at or near `read_batch`** means every read comes back full, so the batch size is the
  limit and raising it takes more datagrams per syscall. A busy listener fed by many unbuffered
  clients lands here.
- **A fill near 1** means datagrams arrive one at a time and there is never more than one waiting.
  Raising `read_batch` can't help, because it was never the constraint; lowering it costs nothing.
  A low-rate listener, or one fed by a single buffered client sending large packed datagrams, looks
  like this.

**What it costs.** The read half holds one buffer per message the syscall may return: a slab of
`read_batch` × 65,507 bytes per listener (IPv4's largest payload, the size every UDP read buffer
here uses), allocated once at startup. At the default that's 4 MiB of *address space* and, in
practice, a few hundred KiB of real memory. Only the pages a datagram is written into are faulted
in, so a listener seeing ordinary small statsd or syslog datagrams touches one 4 KiB page per slot
(`docs/design/memory.md` §5 has the measured figures).

**That holds only where transparent huge pages are `madvise` or `never`.** Under `THP=always`, a
touched page can fault in its whole enclosing 2 MiB huge page, making much more of the slab
resident (`docs/design/performance.md` §7). On small-datagram traffic the 1024 ceiling is a
~4 MiB resident decision, not a 64 MiB one, but it is still 64 MiB of address space per listener,
and there is rarely a reason to go near it.

**It widens one shutdown loss.** A shutdown that lands while the reader is handing a batch to a full
queue drops what the reader still holds, uncounted: up to `read_batch` datagrams instead of one.
The loss is bounded and happens only on the shutdown path.

A `read_batch` larger than `receive.max_datagrams` is legal. A batch that can't fit is admitted
item by item under the configured `overflow` policy, as a sequence of single pushes would be.

### What to watch for listener intake

For a UDP listener:

- `logit.input.datagrams` / `logit.input.reads` (counts): datagrams read off the socket, and the
  read syscalls that returned them. Their ratio is the mean fill described under
  [`read_batch`](#read_batch-how-many-datagrams-one-syscall-takes), and the only number that says
  whether that knob does anything for this listener.
- `logit.input.datagrams.truncated` (count, Linux only): datagrams longer than the 65,507-byte read
  slot, delivered only as far as the slot holds. This happens only on an **IPv6** listener: IPv6
  permits a 65,527-byte payload, so the last 20 bytes of a maximum-size IPv6 datagram have nowhere
  to go. `recv_from` truncated the same datagrams without reporting it; `recvmmsg` reports it.
  Anything but zero means a sender is emitting datagrams larger than any IPv4 path could carry; fix
  it at the sender.
- `logit.component.receive.utilization` (gauge): the fill ratio of whichever of `max_datagrams`/
  `max_bytes` is closer to tripping. Sustained values near 1.0 mean decode is falling behind the
  socket; under `block`, it is also back-pressuring the sender (or, for a local process, the OS).
- `logit.component.datagrams.dropped` / `.bytes.dropped` (count, tagged `reason`:
  `overflow_oldest`/`overflow_newest`): every datagram this listener chose to drop. This is the drop
  you can size your way out of, by raising `receive.max_datagrams`/`max_bytes` or speeding up
  what's downstream. A sustained nonzero rate means the listener is overloaded relative to how fast
  downstream decodes and consumes; size `receive:` or the downstream chain against it.
- `logit.input.kernel.drops` (count, Linux only): datagrams the *kernel* discarded before
  `recv_from` could return them, read from the listening socket itself. It is the same number
  `/proc/net/udp`'s `drops` column shows for this socket. It is a separate loss from the queue
  counter above; the two add up. Any sustained nonzero rate means datagrams arrive faster than this
  process takes them off the socket.
- `logit.input.receive_buffer.utilization` (gauge), with
  `logit.input.receive_buffer.used.bytes` / `.bytes` behind it: how full the kernel's socket buffer
  is, sampled once a second. It is the leading indicator for `kernel.drops`: the kernel drops at
  1.0, so a value climbing toward it is the warning and the drops are the event. A reading slightly
  over 1.0 is normal at saturation, because the kernel charges an arriving packet before testing
  the total against the ceiling.

  **What to do about a high value depends on how the drops respond.** Raise
  `receive.receive_buffer_bytes` (and `net.core.rmem_max`, if the startup warning names it). If the
  drops stop, the traffic was bursty and the buffer was too small for the bursts. If the buffer
  fills again at its new size, the reader is the bottleneck: check
  `logit.component.receive.utilization` and `receive.latency` to see whether decode is behind, and
  size the downstream chain instead of the socket. A bigger buffer absorbs a burst; it can't absorb
  a sustained arrival rate faster than this process reads.

  Two notes on reading these numbers. `used.bytes` is what the kernel *charges* this socket, not
  the queued payload bytes: each datagram costs several hundred bytes of packet-structure overhead
  on top of its length, so a queue of small statsd datagrams is charged far more than their
  combined size. That is the right accounting, because it is what the kernel drops against. And
  `receive_buffer.bytes` is the doubled value Linux reports for a `SO_RCVBUF` request, not what you
  asked for; the ratio uses the kernel's own pair, so it's comparable across listeners regardless
  of what each requested.
- `logit.component.receive.push.blocked.duration` (timing): how long a push waited for room in the
  queue. It records only under `overflow: block`, and only when a push had to wait.
- `logit.component.receive.latency` (timing): arrival-to-dequeue time per datagram. Event
  timestamps are always receipt time, stamped at arrival, never at decode, and decode runs on its
  own loop, so this is the number that says whether those timestamps are still trustworthy under
  load. A healthy listener keeps it small; a climbing value under sustained load means decode is
  falling behind.

A **TCP** listener has no receive queue or kernel receive buffer to size (TCP's flow control is the
backpressure), but its accept queue has the same shape of problem:

- `logit.input.accept_queue.depth` / `.limit` / `.utilization` (gauges, Linux only): connections
  that have completed the TCP handshake and wait for this listener to accept them, the backlog
  ceiling the kernel enforces, and the first as a fraction of the second. They're sampled before
  each accept and once a second while waiting, so an idle listener still reports. `.limit` is
  reported separately so you can see what `listen(2)` got after `net.core.somaxconn` clamped it.
  A depth that isn't near zero means connections arrive faster than they're accepted. A
  utilization approaching 1.0 means the kernel is about to refuse new connections, which a client
  sees as a connect timeout or reset with nothing in `logit`'s logs to explain it. Sustained
  pressure here is usually connection churn (senders reconnecting per batch instead of holding one
  connection); fix it at the sender before raising `net.core.somaxconn`.

### `handshake_timeout` on a TCP listener

A stream listener has no receive queue (its connection's flow control is the backpressure), but a
connection can open and then say nothing while holding one of the listener's 1024
concurrency-cap permits. `syslog_in`, `graphite_in`, and `statsd_in` (each with `transport: tcp`),
`logit_in`, and `otlp_in` bound that with `handshake_timeout:`, **5s by default**, a humantime
string like `connect_timeout`:

```yaml
components:
  syslog_in:
    type: syslog_in
    bind: 0.0.0.0:6514
    transport: tcp
    handshake_timeout: 5s        # the default; per pre-message phase, not a total
```

**It is a per-phase budget, not one deadline per connection.** Each pre-message phase gets its own
budget of the configured length, so a TLS connection that says nothing costs up to two of them
(10s at the default) before it is closed and its permit released. The phases:

| Kind | Phases bounded |
|---|---|
| `syslog_in` (`transport: tcp`) | the TLS accept (under `tls:`), then the wait for the connection's first byte — on the plaintext arm too |
| `graphite_in` (`transport: tcp`) | the same two phases, on the same shared driver |
| `statsd_in` (`transport: tcp`) | the same two phases, on the same shared driver |
| `logit_in` | the TLS accept (under `tls:`), then the `Hello` read |
| `otlp_in` | the TLS accept (under `tls:`), or — on the plaintext arm, which has no TLS accept — the wait for the connection's first byte |

**`otlp_in` bounds one phase per connection, not two**, and not by choice. It hands each accepted
connection straight to `hyper`, whose connection builder reads the first bytes itself to tell
HTTP/1.1 from an HTTP/2 preface, a read this listener never sees. On a plaintext listener it can
wait for the first byte without consuming it (a `MSG_PEEK`), and that wait is what this knob
bounds; `hyper`'s version sniff then proceeds over an untouched socket. So a connection that sends
nothing is closed within the budget on either arm, but a connection that sends **one byte** and
then goes silent is beyond this knob; the separate, opt-in `idle_timeout` bounds that gap. `hyper`'s
own HTTP/1 header-read timeout is deliberately not used for it: that timeout re-arms on every idle
keep-alive gap, so it would act as an idle timeout and kill a long-interval exporter's pooled
connection. `idle_timeout` is that bound made explicit and opt-in.

**It is not an idle timeout on any listener.** After a connection passes its pre-message phases,
`handshake_timeout` doesn't bound the gap before its next frame or request, because a long-lived,
mostly quiet sender is ordinary traffic, not a fault. Lowering `handshake_timeout` doesn't help
with quiet connections; it only tightens how fast a connection that never said anything is given
up on. To bound the gap, use `idle_timeout`; see the next section.

**Validation:** `handshake_timeout` must be greater than `0s` (rule 45), since `0` would close
every connection before its handshake could start. On `syslog_in`, `graphite_in`, or `statsd_in`
with `transport: udp`, leave it at its default: a datagram listener has no connection to
handshake, so a set value is rejected instead of silently ignored.

### `idle_timeout` on a TCP listener

**Enable `idle_timeout:` wherever you expect consistent traffic.** Without it, a connection that
passes its handshake (or, on a plaintext listener, delivers at least one byte) and then goes quiet
holds its connection-cap permit forever, and enough of them fill the 1024-connection cap.
`idle_timeout:` is the opt-in field that closes such a connection. It applies to the five kinds
`handshake_timeout` covers (`syslog_in`, `graphite_in`, and `statsd_in` with `transport: tcp`;
`logit_in`; and `otlp_in`) and to `prometheus_in` in remote-write receiver mode, which shares
`otlp_in`'s HTTP idle machinery and has no `handshake_timeout`; see
[Prometheus remote-write](#prometheus-remote-write-receiving-sending-and-picking-a-version). See
[ADR `idle-connection-timeout`](adr/idle-connection-timeout.md) for the full design.

```yaml
components:
  syslog_in:
    type: syslog_in
    bind: 0.0.0.0:6514
    transport: tcp
    handshake_timeout: 5s        # the default; per pre-message phase, not a total
    idle_timeout: 5m             # off by default; see the recommendation below
```

**Choosing a value.** On a listener with steady traffic, a connection quiet for longer than the
timeout is an anomaly (a dead peer, a half-open socket, or a slow-loris attempt), so closing it
costs nothing and returns the permit. Size the value comfortably above the sender's longest normal
gap (several flush intervals, for instance) so it never fires on legitimate traffic. Leave it unset
for genuinely sparse or bursty senders, where long quiet periods are normal. **Think twice before
enabling it on plaintext `syslog_in`/`graphite_in`/`statsd_in`**, because the sender has no way to
learn its connection was closed (see the client-side note below).

**Off unless set.** With no value, a connection that finished its handshake and went silent is
never closed for silence alone. `logit validate` rejects `0s` by name (rule 53: "omit the field to
disable the idle timeout") and, on `syslog_in`/`graphite_in`/`statsd_in`, rejects any value under
`transport: udp`, where there is no connection to time out.

**What it bounds, and what resets it.** The clock runs only while the listener waits on the peer's
socket. Only two things reset it: bytes read from the peer, and the listener finishing its own work
on the connection (a batch handed downstream, a response completed, an `Ack` written). Time spent
blocked handing a batch to a full downstream never counts, because the clock isn't re-armed until
that work returns. So a connection stalled on backpressure never looks idle, however long the
stall. A periodic flush tick that finds nothing to send touches neither event, so it never re-arms
the clock by itself.

**Per-kind notes:**

| Kind | What resets the clock | How the close happens |
|---|---|---|
| `syslog_in`, `graphite_in`, `statsd_in` (`transport: tcp`, the shared driver) | bytes read from the peer; an interval flush that actually emits a batch | the connection is closed directly; any complete buffered batch is flushed first |
| `logit_in` | the handshake completing, and every `Ack` this listener writes; a peer waiting on a delayed ack is by definition not idle. A frame header whose first byte has already arrived is progress too: the absolute idle deadline bounds only the wait for that first byte, and the rest of the header — like the body — is read under the per-`read` stall bound instead, so a frame that starts arriving right at the deadline is read and acked rather than rejected after the peer already wrote it | `Reject{GOING_AWAY, "idle for <dur>"}` is written first, the same signal an ordinary shutdown sends, then the connection closes |
| `otlp_in` | a request *completing* — hyper owns the bytes, so this is the finest grain visible here; a request head that dribbles in slower than `idle_timeout` on an otherwise-quiet keep-alive connection is closed by this rule, a documented narrowing; a stalled request *body* gets its own bound, `idle_timeout` itself, per read frame | `graceful_shutdown()` is called and the connection is polled for up to `handshake_timeout` (reused as the grace period — no new knob); if that grace elapses with nothing in flight the connection is dropped regardless of what the poll returned, and if a request arrives inside the grace instead, see the note below the table; a stalled body instead answers `408` (`protocol: http`) or `grpc-status: 4` (`protocol: grpc`) and closes the connection once the handler returns — that close is counted the same `reason="idle"` as any other, one policy close reached one path earlier |

On `otlp_in`, a request that arrives inside the grace is served to completion, not dropped: the
connection stays open while a request is in flight, and the grace restarts once it completes so
its response reaches the wire. Dropping it mid-flight would discard a batch already handed to
`Fanout::send`. The cost is at most a reconnect for the *next* request on that connection, never a
lost response or batch. A silent peer can't use this to hold the connection open: with nothing in
flight the drop still happens when the grace ends, and a request body that stalls mid-upload is
bounded by the per-frame stall timeout regardless.

**An idle close is policy, not a fault.** All five kinds in the table end the connection task
with `Ok(())`,
the same success path as a graceful shutdown, so an idle close never reaches the `connection_error`
diagnostic. It is counted as **`logit.input.connections.closed{reason="idle"}`** instead. A rising
count there with no matching movement in `connection_error` is the feature working, not something
to investigate. Buffered data isn't silently dropped: a complete accumulated batch is flushed
before the close, and a partial frame still in the framer is counted `truncated`, the same
accounting a `Failed` or `Shutdown` close gets.

**On the client side, a pooled sink probes a reused connection before writing to it.** A
server-side idle close isn't free for a sink holding a pooled connection. Writing into a socket the
peer already closed either becomes `Fault::Ambiguous` (`logit_out`, whose native protocol's ack
framing notices the failed write) or is silently lost (`syslog_out`, `statsd_out`, `graphite_out`,
whose plaintext protocols can't tell the sender anything went wrong). So each of these four pooled
TCP sinks polls a *reused* pooled connection once before the first write of a send attempt. The
poll is a single non-cancellable `poll_read`, never a `timeout(read)`, because a timeout on a real
read could cancel mid-TLS-record and discard bytes that had already arrived. An immediate EOF, or
unsolicited bytes (the only thing a peer sends unprompted on the native protocol is `logit_in`'s
`Reject{GOING_AWAY}`), drops the pooled connection and dials a fresh one before anything is
written: the ordinary `Clean`/reconnect path, not a lost or ambiguous batch. That catches the
common case, a peer that idle-closed some time ago. It doesn't catch the peer's FIN racing the
probe itself (the peer closing *while* the sink writes): that remains `Fault::Ambiguous` on
`logit_out` and a silent loss on the three plaintext sinks. The probe narrows the window; it doesn't
close it.

### `collectd_in`: multicast groups and `types_db`

`collectd_in` ([ADR `collectd-binary-relay`](adr/collectd-binary-relay.md)) is an ordinary UDP
listener, so everything above applies to it unchanged. Two settings follow collectd's own
deployment conventions:

- **A multicast `bind` is joined automatically.** collectd's `network` plugin defaults to the group
  `239.192.74.66` (or `ff18::efc0:4a42`) on port `25826`, which is where a sender configured with a
  bare `Server "239.192.74.66"` writes. Give `collectd_in` that address and it sets
  `SO_REUSEADDR`, binds the unspecified address on the port, and joins the group on the host's
  default multicast interface; the `bound` info line names the group. There is no `multicast:`
  field; the address says it.
  - **A failed join fails startup** instead of warning, because a listener that bound but never
    joined would look healthy and receive nothing. In a container this usually means the network
    has no route for `224.0.0.0/4`; a unicast `bind`, with `Server "<host>" "25826"` on the sender,
    is the simpler deployment.
  - **A group `bind` is not a filter.** The socket is bound to the unspecified address on that
    port, so the listener also accepts ordinary unicast datagrams sent to that port from any
    source, and it reports its address as `0.0.0.0:<port>`, not the group.
- **`types_db:` is optional and only affects names.** Point it at the `types.db` your collectd
  installation already ships (conventionally `/usr/share/collectd/types.db`; `logit` ships none,
  because collectd's is GPL-licensed) and a multi-data-source list is named after its data sources:
  `load.load.shortterm` instead of `load.load.0`. List several files to merge them in order, a later
  file overriding an earlier one. A file that can't be read or parsed fails startup, naming the
  path and line. It changes only what a cross-protocol sink (InfluxDB, Prometheus, statsd) calls the
  series: `collectd_out` re-encodes from the `collectd.*` attributes, so a
  `collectd_in -> collectd_out` relay puts the same bytes on the wire either way.

[`examples/collectd-to-influxdb.yaml`](../examples/collectd-to-influxdb.yaml) is a complete,
runnable topology: `collectd_in` on `0.0.0.0:25826` straight into `influxdb_out`, with both settings
above as commented alternatives. **Don't put an `aggregate` between them.** Unlike
[`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml), it has none, because a
collectd value list is already one pre-aggregated reading per `Interval` with its own timestamp;
re-windowing it would average averages and re-stamp them with the flush time. The file's header
comment lists what the cross-protocol hop costs: the `collectd.*` attributes become ordinary
InfluxDB tags instead of wire identity, and one N-data-source list becomes N measurements named
`plugin.type.ds` sharing a tag set and a timestamp.

### `graphite_in`: carbon plaintext and pickle, TCP or UDP

`graphite_in` ([ADR `graphite-carbon-relay`](adr/graphite-carbon-relay.md)) is a carbon receiver:
point a `write_graphite` plugin, a StatsD backend, a `carbon-relay`, or anything else that speaks
carbon at it. Two settings, `transport:` and `protocol:`, change a lot about how it behaves.
[`examples/graphite-relay.yaml`](../examples/graphite-relay.yaml) is the like-for-like runnable
topology (`graphite_in` straight into `graphite_out`, every default present as a commented
reference); [`examples/statsd-to-graphite.yaml`](../examples/statsd-to-graphite.yaml) is the
cross-protocol one, `statsd_in` through an `aggregate` window into `graphite_out`.

- **`transport:` picks the driver.** `tcp` is the default, matching carbon's own default listener
  (plaintext on port 2003). A TCP listener serves up to 1024 connections at once; one arriving past
  that cap is closed immediately and counted
  (`logit.input.connections.rejected{reason="limit"}`), because carbon's wire has no way to say
  "try later", and a sender holding an accepted-but-unread connection would look healthy while
  delivering nothing. `udp` runs the same shared datagram listener as `statsd_in`/`collectd_in`/
  `syslog_in`, so the receive-queue sections above apply unchanged. The TCP driver is the one a TCP
  `syslog_in` runs on, so `tls:` and `handshake_timeout:` mean what they mean there.
- **`tls:`, `handshake_timeout:`, and `idle_timeout:` are TCP-only and behave as `syslog_in`'s do.**
  A `tls:` block's presence turns TLS on and makes it required; a TLS listener has no plaintext
  fallback. `logit validate` rejects `tls:` under `transport: udp` (carbon has no DTLS receiver).
  Plain carbon senders have no TLS, so this is for a `logit`-to-`logit` or stunnel-shaped relay hop.
  `handshake_timeout:` (default `5s`) bounds each pre-message phase independently: the TLS accept
  when `tls:` is set, then the wait for the connection's first byte, so a silent TLS connection
  costs up to two budgets before its permit comes back. It is **not** an idle timeout: once a
  connection has sent a byte, the gap before the next datapoint is bounded only by the opt-in
  `idle_timeout:`, if set; see
  ["`idle_timeout` on a TCP listener"](#idle_timeout-on-a-tcp-listener) above.
- **`receive:` means different things on the two transports.** A UDP `graphite_in` takes the whole
  block. A TCP one has **no receive queue**: TCP's flow control is the backpressure, and the queue
  exists (ADR `decoupled-listener-io`) for a UDP socket's *silent* drops, which a stream can't have.
  Only `batch_max_events`, `batch_max_bytes`, `batch_flush_interval`, and `shutdown_grace` apply
  to it. A queue-bounding field (`max_datagrams`, `max_bytes`, `overflow`, `receive_buffer_bytes`,
  `read_batch`) on a TCP `graphite_in` is a `logit validate` error naming the field, not a silently
  ignored setting. A stalled TCP `graphite_in` therefore shows up as backpressure at the *sender*,
  which is what you want, not as a drop counter here.
- **`protocol: pickle` requires `transport: tcp`.** Carbon's pickle batch protocol (port 2004)
  wraps each batch in a 4-byte big-endian length prefix (Twisted's `Int32StringReceiver`), which
  means nothing in a self-delimiting datagram, so validation rejects the combination instead of
  mis-framing at runtime. The pickle reader is **restricted**: it accepts the opcodes real senders
  emit (`pickle.dumps(..., protocol=2)` and `protocol=-1`) and rejects everything that can
  construct an object, with bounded depth, memo, and item counts, and every declared length
  validated before anything is allocated. It is deliberately not a general unpickler.
- **Two size bounds may need raising.** `max_line_bytes` (default `8192`) bounds one TCP
  plaintext line. Past it, the line is abandoned and counted once
  (`logit.input.frames.dropped{reason="oversize"}`, diagnostic `framing_error`) and the reader
  drains to the next newline, so the following line still decodes and the connection stays up.
  `max_frame_bytes` (default `"1MiB"`, Twisted's own `MAX_LENGTH`) bounds one pickle frame. A frame
  declaring more is counted the same way but **closes the connection**, because a length-framed
  stream has no resync point to skip to. `logit validate` holds it to `1024..=16MiB`.

**The path is the metric name; there is no `graphite.*` namespace.** `collectd_in` and `syslog_in`
park their wire identity in attributes a matching sink reads back. `graphite_in` instead maps the
four facts carbon carries straight onto the model: the dotted path *is* `MetricRecord.name`, the
`;k=v` tags *are* event attributes, the number is a `Gauge`, and the second is the event timestamp.
Nothing is duplicated or reserved. **So a `lua`/`set` stage that renames the metric silently changes
the wire path** a downstream `graphite_out` writes. That is the intended way to rename a series
(neither component has a `prefix:` or `template:` field), but a rename in the middle of a relay is
a wire-visible change, not a display one.

Two smaller behaviors to know before deploying one:

- **A `-1` timestamp means receipt time**, carbon's own rule. Any other non-positive timestamp
  rejects the line (`bad_timestamp`) instead of being stamped with "now".
- **A malformed tag rejects the whole line**, not only that tag. Carbon's `TaggedSeries.parse`
  raises too, and dropping one tag would silently change the series identity the receiver keys on.
  A repeated tag key keeps its **last** value, counted
  `logit.input.tags.normalized{reason="duplicate_key"}`, which is also what carbon does (it builds
  a `dict`).

### `statsd_in`: `transport: tcp` and TLS

`statsd_in` defaults to UDP, which classic statsd and every DogStatsD client speak, and which
[`examples/statsd-to-influxdb.yaml`](../examples/statsd-to-influxdb.yaml) and
[`examples/statsd-relay.yaml`](../examples/statsd-relay.yaml) use. `transport: tcp` runs the same
shared stream driver as a TCP `syslog_in`/`graphite_in`, so what the `graphite_in` section says
about connections, `handshake_timeout:`, `idle_timeout:`, and `receive:` applies unchanged:

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

- **A TCP message is always one LF-delimited line.** There is no `framing:` field and no
  octet-counted alternative like `syslog_in`'s, because there can't be: a statsd metric name may
  legally begin with a digit (`1.hits:1|c`), so sniffing a leading digit as a length prefix would
  mis-frame the connection. This is what `statsd_out`'s `transport: tcp` emits, and what the Etsy
  reference server and the Datadog agent accept.
- **An oversize line costs that line, not the connection.** A line past 64 KiB is dropped and
  counted once (`logit.input.frames.dropped{reason="oversize"}`, diagnostic `framing_error`), the
  reader drains to the next newline, and the following line still decodes. There is deliberately no
  `max_line_bytes` knob: unlike carbon, no statsd server exposes one for you to match.
- **An unterminated final line is dropped, not delivered.** If a sender closes with a partial line,
  those bytes are counted `logit.input.frames.dropped{reason="truncated"}` and discarded, the same
  as a connection that dies mid-line. The LF is a statsd line's only completeness signal, and half
  of `page.views:1|c` still looks like a valid metric, so delivering it would be silent corruption.
  (`syslog_in` differs here: RFC 6587 framing explicitly permits a terminator-less last message.)
  Trailing whitespace-only padding isn't counted.
- **`tls:` is TCP-only, and its presence makes TLS required.** A TLS listener has no plaintext
  fallback, and `logit validate` rejects `tls:` under `transport: udp` (rule 43: DTLS is out of
  scope everywhere in this project, and no statsd client speaks it). Plain statsd clients have no
  TLS either, so this is for a `logit`-to-`logit` or stunnel-shaped relay hop. See ["TLS"](#tls)
  below for the full field reference.
- **`receive:` means different things on the two transports**, as for `graphite_in`: a UDP
  `statsd_in` takes the whole block; a TCP one has no receive queue, so only `batch_max_events`,
  `batch_max_bytes`, `batch_flush_interval`, and `shutdown_grace` apply (scoped per connection).
  A queue-bounding field on a TCP `statsd_in`, including `read_batch`, is a `logit validate` error
  naming the field, not a silently ignored setting.
- **What to watch.** Under `transport: udp`: the `logit.input.datagrams`/`.datagram.bytes` pair and
  the receive-queue gauges above. Under `transport: tcp`: `logit.input.connections` (a gauge that
  should match the number of connected senders),
  `logit.input.connections.rejected{reason="limit"}` (nonzero means the 1024-connection cap is
  binding), `logit.input.frames`/`.frame.bytes` (one frame is one statsd line), and
  `logit.input.frames.dropped{reason="oversize"|"truncated"}`. On either transport, a malformed
  *line* is the decoder's `logit.component.diagnostics{key="bad_line"}`, not a framing error.

### `statsd_in`: DogStatsD over a Unix socket

A Datadog Agent also listens for DogStatsD on a Unix datagram socket (`dogstatsd_socket`, by
default `/var/run/datadog/dsd.socket`), which is how Kubernetes clients usually reach it. To stand
in for that socket, set `transport: unix` and put the socket's absolute path in `bind:`. Clients
then use `DD_DOGSTATSD_URL=unix:///var/run/datadog/dsd.socket`:

```yaml
components:
  dogstatsd:
    type: statsd_in
    bind: 127.0.0.1:8125           # UDP, as before
  dsd_socket:
    type: statsd_in
    transport: unix                # unix (datagram) | unix_stream
    bind: /var/run/datadog/dsd.socket
```

- **One `statsd_in` per transport.** A component listens on one socket, so to accept UDP and the
  Unix socket at once, configure two components, as above, and list both as sources downstream.
  [`examples/datadog-agent-standin.yaml`](../examples/datadog-agent-standin.yaml) carries the
  socket component, commented out.
- **Create the directory first.** `logit` never creates the socket's directory, because its owner
  and mode are the access control. At startup a stale socket file from an earlier run is replaced;
  anything else at the path (a regular file, say) fails startup rather than being deleted. The
  socket file isn't removed on shutdown.
- **The socket file is mode `0722`**, the Agent's own mode for this socket: a client needs only
  write permission to send, so any user's process can send to it. Restrict senders with the
  directory's permissions.
- **`transport: unix` behaves like UDP.** One datagram carries one or more newline-separated lines,
  and the whole `receive:` block applies. The kernel counters differ: a full Unix datagram queue
  makes the *client's* send block or fail with `EAGAIN` rather than dropping in the kernel, so
  `logit.input.kernel.drops` stays at zero and any loss shows up in the client's own telemetry
  (`datadog-go` counts dropped packets). `logit.input.receive_buffer.*` is still reported.
- **`transport: unix_stream` is the Agent's `dogstatsd_stream_socket`.** Each packet (one
  datagram's worth of lines) follows its length as a 4-byte little-endian integer; clients use
  `DD_DOGSTATSD_URL=unixstream:///path`. It runs on the TCP stream driver, so `handshake_timeout:`,
  `idle_timeout:`, and the batch-assembly half of `receive:` apply, and the queue fields are
  rejected. A packet declaring more than 64 KiB closes its connection
  (`logit.input.frames.dropped{reason="oversize"}`), since a length-framed stream has no point to
  resynchronize at. This framing hasn't yet been checked against a real Agent or client
  ([`known-gaps.md`](known-gaps.md)).
- **No TLS, and the path must be absolute.** A Unix socket is local and always plaintext, so
  `logit validate` rejects `tls:` under either Unix transport, and a relative `bind:` (rule 64),
  which a client's `unix:///` URL couldn't name.

### `collectd_out`: relaying back onto the wire

Use `collectd_out` when the destination is another collectd (or anything else speaking its
`network` protocol), not a time-series database. `collectd_in -> collectd_out` is a fixed point
modulo the named normalization list in
[ADR `collectd-binary-relay`](adr/collectd-binary-relay.md), which
`crates/logit-cli/tests/collectd_round_trip.rs` pins fixture by fixture over real sockets.
[`examples/collectd-relay.yaml`](../examples/collectd-relay.yaml) is the runnable topology:
`collectd_in` on `0.0.0.0:25826` straight into `collectd_out`, every default present as a commented
reference. Before deploying one:

- **It is UDP only; don't put an `aggregate` in the middle.** collectd's `network` plugin has no
  TCP mode to relay onto. Unlike [`examples/statsd-relay.yaml`](../examples/statsd-relay.yaml), the
  collectd relay has no `aggregate`, because collectd data is already one pre-aggregated reading
  per `Interval`. A window would re-window it and break byte-for-byte relay for the kinds
  `aggregate` absorbs: a GAUGE and an ABSOLUTE come back stamped with the flush time, while a
  COUNTER/DERIVE (a cumulative `Sum`) passes through untouched. Add one only to re-window
  deliberately.
- **Budget about a third more egress bytes and packets than the fleet sends.** `collectd_out`
  writes a `TimeHR` and an `IntervalHR` part for *every* value list, where collectd's own sender
  omits one that hasn't changed since the last list in the same datagram (normalization 11 in the
  ADR's list). The restored parts carry exactly what the receiver's sticky state already held, so
  the data doesn't change, but datagrams grow and may split. The recorded capture
  `testdata/interop/collectd/collectd-000.raw`, a real Debian `collectd`'s output, shows the scale:
  26 value lists behind 17 `TimeHR` parts and a single `IntervalHR`, 1296 bytes in one datagram,
  which this relay re-emits as 1717 bytes across two. Expect a capture of relayed traffic to look
  chattier than the original.
- **`max_packet_bytes:` bounds a datagram, not a value list**, and defaults to `1452`, collectd's
  own `MaxPacketSize` default. Graph validation rejects values outside `1024..=65535`, collectd's
  range. Lower it to match a path MTU. The encoder re-packs incoming lists into datagrams of its own
  choosing, however the sender packed them, so this setting (together with the inflation above,
  which pushes that 1296-byte capture over the default cap) decides egress framing. A single value
  list too large to fit alone is dropped whole and counted
  `logit.output.metrics.skipped{reason="oversize_value_list"}`, not split.
- **Set `hostname:` if the pipeline carries metrics that didn't come from `collectd_in`.** A relayed
  list already carries its origin's host on `collectd.host`, so a pure relay never needs it. Metrics
  from a `statsd_in`/`internal` with no `collectd.host` or `host.name`, and no `hostname:` set, are
  dropped and counted `logit.output.metrics.skipped{reason="no_host"}` with a `no_host` diagnostic.
  That is deliberate: collectd's receiver rejects an empty host, and inventing one would merge every
  unlabeled sender into one host's metrics.

### `graphite_out`: relaying to Carbon

Use `graphite_out` when the destination is a real Carbon/Graphite listener (or anything else
speaking its wire protocols), not a general time-series database. `graphite_in -> graphite_out` is
a fixed point modulo the named normalization list in
[ADR `graphite-carbon-relay`](adr/graphite-carbon-relay.md). Unlike `collectd_out`, it supports
both transports, because carbon's plaintext listener (port 2003) speaks UDP or TCP. It also speaks
carbon's length-prefixed pickle batch format (port 2004), **TCP only**: a length prefix means
nothing in a datagram, and `logit validate` rejects `protocol: pickle` under `transport: udp`. See
`graphite_in`'s section above for the two runnable examples that use this sink. Before deploying
one:

- **Renaming a metric upstream silently changes the series carbon stores it under.** There is no
  `graphite.*` carrier like `collectd_out`'s `collectd.*` or `syslog_out`'s `syslog.*`: the wire
  path *is* [`MetricRecord::name`](design/data-model.md), with no `prefix:`/`template:` field and
  nothing to restore identity from. A `lua`/`set` stage that renames a metric between `graphite_in`
  and `graphite_out` leaves no wire fact to notice the rename against, unlike a collectd or syslog
  relay, where identity attributes ride alongside the (possibly transformed) event. If a pipeline
  renames metrics, the rename *is* the new wire path; this sink can't keep sending the old one.
- **`tags: carbon` (the default) against a pre-1.1 Graphite silently corrupts data on disk.**
  Carbon before 1.1 has no tag support, and its whisper backend turns the plaintext path straight
  into a filesystem path: a `;env=prod` tag suffix becomes literal `;` characters in a **whisper
  directory name**, not a rejected line. There is no error; `carbon-cache` creates directories
  nobody intended. If the destination might be an older Graphite, set `tags: drop`: every attribute
  is then left off the wire (counted `logit.output.tags.dropped{reason="dialect"}`). Confirm an
  unfamiliar cluster's tag support before using `tags: carbon` against it.
- **`multi_value: skip` (the default) drops what carbon's one-number-per-datapoint wire can't
  carry.** `Samples`, `Distribution`, `Histogram`, `ExponentialHistogram`, `Summary`, `Set`, and
  `SetMembers` records are dropped whole and counted `logit.output.metrics.skipped{metric_kind=...}`
  instead of guessing at a convention. To keep them, set `multi_value: expand`, which renders the
  dotted sub-paths tabled in `logit_proto::graphite`'s module doc (`.count`, `.sum`,
  `.q0_5`...`.q0_99`, per-bucket counts, and so on), an explicit, named convention counted
  `logit.output.metrics.degraded{metric_kind=...}` once per record.
- **Size and timeout bounds.** `max_packet_bytes:` (UDP only, default `1432`) bounds a datagram, not
  a single line, like `statsd_out`'s setting. `max_frame_bytes:` (default `1MiB`, Twisted's
  `Int32StringReceiver.MAX_LENGTH`) bounds one pickle frame and applies regardless of transport,
  since pickle is TCP-only anyway. `connect_timeout:` (TCP only, default `5s`) matches
  `statsd_out`'s and `syslog_out`'s default.
- **Retries rely on whisper's semantics.** This is the first non-HTTP sink with a real destination
  to report `duplicate_safe: true` (`null_out` reports it trivially, having no destination):
  whisper is last-write-wins per `(path, second)`, so a datapoint redelivered on retry overwrites
  itself with the same number instead of double-counting, unlike a collectd COUNTER or a statsd
  `|c`. That argument holds for whisper's storage, not the carbon wire protocol in general: a
  non-whisper Graphite-protocol receiver could treat a redelivered datapoint as an addition, and
  this sink can't tell the difference.

### `statsd_out`: `transport: tcp` and TLS

`statsd_out` defaults to UDP, like every statsd client;
[`examples/statsd-relay.yaml`](../examples/statsd-relay.yaml) is the runnable topology with every
default present as a commented reference. `transport: tcp` replaces the packed datagram with one
LF-terminated line per metric on a lazily opened connection, and a `tls:` block requires it:

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
  `logit validate` rejects `tls:` under `transport: udp` (rule 52: DTLS is out of scope everywhere
  in this project). No statsd client in the wild speaks TLS, so, like `statsd_in`'s listener block,
  this is for a `logit`-to-`logit` or stunnel-shaped relay hop, not an application's DogStatsD
  client. See ["TLS"](#tls) below for the full field reference.
- **`connect_timeout:` bounds the TCP connect and the TLS handshake as two separate phases**, not
  one combined deadline, so a TLS connect can take up to twice the configured value (`syslog_out`'s
  arrangement). Account for that if you raise it.
- **A retry never redelivers a batch over TLS, so expect a TLS relay to drop batches a plaintext
  one would retry.** On plaintext, a write that fails having accepted zero bytes is provably
  retryable, so this sink reconnects once and rewrites the frame (`Fault::Clean`). A TLS write gives
  no such proof, because rustls may already have put complete records on the wire, so every failure
  at or after the first write is `Fault::Ambiguous` and the batch is never resent
  ([ADR `statsd-output`](adr/statsd-output.md)'s TLS amendment). That is deliberately conservative:
  `statsd_out` reports `duplicate_safe: false` because a redelivered `hits:5|c` *increments the
  destination counter a second time*. Watch `logit.component.batches.dropped` accordingly.
- **What to watch.** `logit.output.requests{class="ok"|"error"}` (one per attempt) and, on TCP,
  `logit.output.reconnects`, which should stay near zero in steady state; a climbing count means the
  peer or the network is unstable, not this sink. It counts plaintext and TLS connections the same
  way, since both take the same connect path. `logit.output.datagrams` exists only under the
  datagram transports, `udp` and `unix`.

### `statsd_out`: sending to a DogStatsD Unix socket

To hand metrics to a local Datadog Agent over its Unix socket, set `transport: unix` (the
`dogstatsd_socket`) or `unix_stream` (the `dogstatsd_stream_socket`) and put the socket's absolute
path in `endpoint:`:

```yaml
components:
  to_agent:
    type: statsd_out
    sources: [enrich]
    transport: unix
    endpoint: /var/run/datadog/dsd.socket
    max_packet_bytes: 8192          # DogStatsD clients' default over a Unix socket
```

- **Raise `max_packet_bytes:` to `8192`.** The `1432` default is sized for a UDP path MTU; DogStatsD
  clients pack up to 8192 bytes into a Unix-socket packet, which is also the Agent's default read
  buffer. Lines are packed into packets as into UDP datagrams, on both Unix transports.
- **A full Agent queue makes a `unix` send wait, not drop.** Unlike UDP, a Unix datagram socket
  pushes back on the sender. Each datagram's wait is bounded by `connect_timeout:` (default `5s`);
  past it the send fails and the batch is retried or dropped under the sink's usual rules.
- **`unix` connects its socket to the path and follows a restarted Agent.** A connected sender
  waits for room without spinning. When the Agent restarts and rebinds the path, the next send is
  refused on the old connection. If that's a batch's first datagram, `statsd_out` reconnects and
  sends it again at once, so a restart between batches loses nothing; later in a batch, the batch
  fails under the sink's usual rules and the next send reconnects. Each reconnect counts
  `logit.output.reconnects`.
- **`unix_stream` connects lazily and reconnects like TCP.** Each packet follows its length as a
  4-byte little-endian integer (unverified against a real Agent,
  [`known-gaps.md`](known-gaps.md)). A write that fails having accepted zero bytes is retried once
  on a fresh connection, as on plaintext TCP.
- **No TLS.** `logit validate` rejects `tls:` under either Unix transport, and a relative
  `endpoint:` (rule 64).

## Tailing files and Docker logs

`tail_in` reads one or more files line by line. `docker_in` uses the same driver to tail Docker's
json-file container logs, enriched with per-container identity read locally from the sibling
`config.v2.json`, with no docker socket and no HTTP client
([ADR `file-tailing-and-docker-json-logs`](adr/file-tailing-and-docker-json-logs.md)). Both handle
rotation and truncation, and can checkpoint so a restart resumes instead of replaying or skipping.

### Root, and a read-only bind mount, for `docker_in`

**`docker_in` must run as root, with the host's `/var/lib/docker/containers` (or wherever `root:`
points) bind-mounted read-only.** On a stock install, Docker's per-container state directories are
`root:root 0710` and the log files `root:root 0640`. This cost comes from reading the json-file
driver directly; the docker socket/API would broker access through group membership on the socket
instead. See the ADR's "Root privileges" section.

`demo/compose.yaml`'s `logit` service is the worked example: `user: "0:0"`, the bind mount, and, on
SELinux hosts only, `security_opt: ["label=disable"]`. **Never use `:z` on that mount**: it would
relabel the Docker daemon's own live state, which this stack doesn't own.

**Native Linux Docker Engine only.** Rootless Docker uses `~/.local/share/docker/containers`,
Docker Desktop's paths live inside its VM, and Podman uses a different log format. None of these
match `docker_in`'s `root:` default, the only layout this driver understands.

### `read_from`, and why a checkpoint matters more here than for a plain UDP listener

`read_from: end` (the default) skips what a file already holds and tails only new lines;
`read_from: beginning` replays it first. Either way, `read_from` governs only a file present at
the first scan with no checkpoint entry naming it. A file discovered later (a new log, a rotated
one, a newly selected container) always starts at its beginning, since it has nothing "from before
`logit` started" to skip. A checkpoint entry, when present, always wins over `read_from` for the
file it names.

**Set `checkpoint_path` for `docker_in`.** It is optional and unset by default, in which case every
restart re-applies `read_from` as if every file were newly discovered. A long-running container's
log easily holds more than a restart reading from `end` would silently skip. **Put the checkpoint
file on a persistent volume** (`demo/compose.yaml`'s `logit_state`), or it resets on every
container recreate.

The checkpoint is written every `checkpoint_interval` (5s default) when dirty, plus on every file
close and at shutdown, never per line. A crash between two writes can therefore replay up to
`checkpoint_interval` worth of already-emitted lines on restart. This is a deliberate
at-least-once boundary, the same trade `buffer:`'s sink-side retry makes: it bounds how much a
crash can replay, and replay is always safe.

### `watch: auto | inotify | poll`

- `auto` (the default) uses `inotify` where available (Linux only) and falls back to polling, with
  a `watch_error` diagnostic, if `inotify` setup fails.
- `poll` always uses the `poll_interval` tick (1s default), with no OS-specific dependency. Use it
  on network or FUSE mounts where `inotify` events don't fire reliably.
- `inotify` fails startup outright if setup fails, instead of degrading silently.

`poll_interval` never gates reading more bytes from an already-tracked file: the driver's read loop
runs on every iteration, whatever woke it, so once a content change on a tracked file's watch or a
poll tick wakes it, it reads everything available.

`inotify` watches deliberately little: one watch on the single directory a pattern reaches
(`paths:` for `tail_in`, `root` for `docker_in`), plus one per file the listener has open. Nothing
watches a container this listener isn't tailing, and nothing wakes on a write to an untracked file.

**For `docker_in`, three changes wait for `poll_interval` in every `watch` mode:** a log file's
first appearance inside an existing container directory, a rotation, and a `config.v2.json`
change. The watch on `root` alone catches a container's directory arriving or leaving almost
instantly (Docker's per-container state directories are direct children of `root`), but not those
three. Watching every container's subdirectory would catch them instantly too, at the cost of
O(containers on the host) work for every log line written anywhere on the host, selected or not;
see [ADR
`docker-container-identity-and-minimal-watches`](adr/docker-container-identity-and-minimal-watches.md).

### What to watch for file tailing

- `logit.input.files.open` (gauge): how many files this listener has open. **Alert on this to tell
  "nothing is flowing" from "nothing to flow yet."** It is zero, without an error, when a
  `docker_in`'s `containers:`/`discover:` selection matches nothing or a `tail_in`'s `paths:` glob
  matches no files yet. A directory that doesn't exist yet is the ordinary "not there yet" case,
  retried next cycle; under `inotify`/`auto`, the retry also re-arms the directory watch and, while
  the directory is missing, counts a throttled `watch_dir_error` diagnostic per scan.
- `logit.input.watch.wakes{source="inotify"|"poll"}` (count): which wake source fired. This is the
  health signal for the low-latency path: `{source="inotify"}` flatlining while `{source="poll"}`
  continues at `1/poll_interval` means discovery has silently fallen back to polling, because a
  watch couldn't be registered or the wake source itself failed. Under `watch: poll` only the `poll`
  series increments, so alert on the `inotify` series reaching zero only where you configured
  `inotify`/`auto`. Pair it with `logit.component.diagnostics{key="watch_error"}` (a file watch, or
  the wake source, failing; counted once) and `{key="watch_dir_error"}` (a directory watch failing;
  counted once per scan until it succeeds), each incremented and logged (throttled) at the point of
  failure with the errno.
- `logit.input.watch.watches` (gauge): the size of the watch set: the watched directory, plus one
  entry per open file. It is proportional to what's being tailed, not to how much any of it writes,
  which makes "the watch set stays minimal" checkable from outside. It reflects the *intended*
  watch set, not live kernel watches:
  - Under `watch: poll`, it counts the same set with no real `inotify` descriptors behind it
    (`Watcher::watch_dir`/`watch_file` are no-ops in that mode), so a `poll` config still shows the
    directory held although nothing is registered.
  - Under `inotify`/`auto`, the two are close but not identical. A directory whose watch failed is
    left out of the set (and diagnosed), but a file draining after its inode was deleted still
    counts one after the kernel has released its descriptor, and two spellings of one directory (a
    symlink, say) count two against one real watch. The exact live count is the kernel's: one
    `inotify wd:` line per watch in `/proc/self/fdinfo/<the inotify fd>`.
- `logit.input.watch.overflows` (count): the `inotify` event queue overflowed. The driver responds
  with a full rescan instead of losing track of changes, but a sustained nonzero rate means
  `poll_interval` is doing more of the real work than the wake source.
- `logit.component.diagnostics{key="long_line"|"truncated"}` (count, via the `Diagnostics` bridge):
  a line dropped whole for exceeding `max_line_bytes`, or a tracked file's length shrinking under
  it (rare, but real for a tool that recreates a log file in place instead of renaming it away
  first). A long line is never truncated and passed through, because a truncated line would hand a
  downstream JSON parser something that looks well-formed but isn't the real line.
- `logit.component.diagnostics{key="metadata_error"}` (`docker_in` only): a container's
  `config.v2.json` couldn't be read or parsed. That container's lines still flow, with a
  `container.id`-only resource instead of the full identity. A missing file is retried on every poll
  tick. A file that exists but won't parse is retried when its stat next changes, since the failed
  read is cached against that stat like a successful one; a torn read racing the daemon's rewrite is
  therefore picked up as soon as the rewrite lands. Either way, it fires once per failure, not once
  per tick while the failure persists.
- `logit.input.files.identity_changed` / `.deselected` (count, `docker_in` only): a container's
  identity (name, image, or a watched label) changed, or a tracked container was renamed out of
  `containers:` and stopped flowing. The matching `container_renamed`/`container_deselected`
  diagnostics name the container. **A deselection is process-local:** if `logit` restarts before the
  container is renamed back, the retained resume offset is lost.

## Series retention

`aggregate` normally drains every series on every flush (tumbling). Statsd gauges are the
exception: a sender transmits a gauge only when it changes and expects the last value to persist,
and a relative adjustment (`+`/`-`, `docs/adr/relative-gauge-adjustments.md`) sent in a later window
needs the gauge's last-known value to apply against. Two `aggregate` fields control this, both on
by default: `series_retention` (`5` windows) and `max_retained_series` (`10,000` series). See the
field doc comments in the schema (`logit schema`) for the exact semantics. `series_retention: 0`
opts out, giving strictly tumbling behavior. Both fields are optional, so an existing config needs no
change. The same two bounds make `temporality: cumulative` possible (next section), which is why
they're named for series in general, not for gauges.

**Retention doesn't cover eviction or restarts.** A delta against a series evicted by the
cardinality cap, or sent after a process restart, resolves against `0.0`. It's reported
(`logit.transform.gauge.delta.unseeded`,
`logit.transform.series.evicted{reason="cardinality"}`), never silent, but not
prevented. The restart case can't be fixed without durable aggregator state, which this project has
deliberately not built (`docs/adr/aggregation-window-semantics.md`'s Alternatives). A cumulative
series has the same exposure, which is why every cumulative record carries a `start_timestamp` from
which a consumer can detect the restart. If an operator needs a gauge that receives relative
adjustments to be exact after a restart, instead of resolving against 0 until the next absolute
value, the mitigation is on the sending side: send an absolute value periodically, not only deltas
(the zero-then-set convention).

**What to watch:**

- `logit.transform.series.retained` (gauge): how many gauge series are carried idle. A number that
  keeps climbing past what `series_retention × <series churn per window>` predicts suggests a leak
  (series whose name/tags never repeat); investigate with `keep`, as for unbounded `series.active`
  growth.
- `logit.transform.series.evicted{reason="cardinality"}`: any sustained nonzero rate means
  `max_retained_series` is undersized for the pipeline's gauge cardinality, and deltas are silently
  resolving against 0 as a result.

## Counter temporality (`delta` vs. `cumulative`)

**For a `prometheus_out` leg, set `temporality: cumulative` on the upstream `aggregate`.**
`aggregate`'s `temporality:` decides what a flushed `Sum`/`Histogram` *means*:

- `delta` (the default) emits each window's own increment, which is what InfluxDB and statsd expect.
- `cumulative` keeps the accumulator alive across flushes and emits the running total since the
  series was first seen, labeled `Cumulative` and stamped with that first-seen time
  (`start_timestamp`) so a consumer can tell a real restart from a decrease.

A Prometheus scrape carries the cumulative shape, and `prometheus_out` skips delta records instead
of resolving them itself (`docs/adr/prometheus-scrape-and-exposition.md`), so
`statsd_in -> aggregate(temporality: cumulative) -> prometheus_out` is the intended pipeline.

`cumulative` relies on the `series_retention`/`max_retained_series` pair above to keep running
totals alive, so both must be above `0`. `logit validate` rejects the combination otherwise, since
with no retention every window's increment would be emitted labeled as a running total.

**Size `max_retained_series` from `series.active + series.retained`, not `.retained` alone.** The
cap bounds every series that survives a flush, but the two gauges split that population by whether
it saw data *this* window. A counter incremented every window reports under
`logit.transform.series.active`; `logit.transform.series.retained` counts only the idle tail that is
carried. A healthy cumulative pipeline whose counters are all live therefore reports
`retained = 0` while sitting at the cap, so watching `.retained` alone shows nothing until
`logit.transform.series.evicted{reason="cardinality"}` fires, which is already the symptom. An
evicted cumulative series restarts from zero with a new `start_timestamp` (correct and visible to a
consumer, but a gap in that series' graph). Hitting the cap also warns under
`logit.component.diagnostics{key="series_retention_full"}`.

## Raw samples and set members

By default, `aggregate` summarizes a raw `Samples`/`SetMembers` series as soon as it absorbs it:
into a `DdSketch` (`distributions: sketch`) or a `HyperLogLog` (`sets: estimate`), so no raw
observation survives past the window. To keep the individual values or the exact member set, for a
`statsd_in -> aggregate -> statsd_out` relay or any consumer downstream, opt into raw retention:

- `distributions: samples` retains raw values for the whole window, bounded by
  `max_samples_per_series` (default `1000`).
- `sets: members` retains an exact, deduplicated member set, bounded by
  `max_set_members_per_series` (default `1000`).

See the field doc comments in the schema (`logit schema`) for the exact semantics. All four fields
are optional, so an existing config needs no change.

**Both raw modes fall back to their summarized counterpart; neither drops data.** Growing past
either cap converts what's held (plus the record that tripped the cap) into a sketch or a fresh
`HyperLogLog` and counts it, instead of dropping the overflow or growing without bound: the same
DoS and memory guard `max_retained_series` provides for gauge retention. `distributions: samples`
has a second trigger a member set can't: an incoming record whose `sample_rate` disagrees with the
series' first one. A `samples`-mode accumulator can report only one rate for the whole series, and
there's no correct single rate to pick between two that disagree.

**Neither raw mode changes tumbling.** A `Samples`/`SetMembers`/`Set` series never survives a flush,
even with `series_retention` set: retention exists for a gauge's sticky-value semantics, which a raw
sample or set member doesn't share (see
[ADR `aggregation-window-semantics`](adr/aggregation-window-semantics.md)'s amendment).

**What to watch:**

- `logit.transform.samples.fallback{reason="cap"|"rate_mismatch"}` and
  `logit.transform.set_members.fallback{reason="cap"}` (count): a sustained rate means the cap is
  undersized for the pipeline's per-window sample or member volume, so `distributions`/`sets` spends
  memory retaining raw data that keeps getting thrown away. The matching throttled diagnostics
  (`samples_cap_exceeded`, `samples_rate_mismatch`, `set_members_cap_exceeded`) name the series and
  the reason.
- `logit.transform.samples.weight_clamped` (count): a `sample_rate` implying a weight beyond
  `Samples::MAX_WEIGHT` (1000, that is `@0.001`) was clamped instead of extrapolated without bound.
  It fires in both `distributions` modes (the sketch-mode absorb and the `samples`-mode fallback's
  re-sketch). `aggregate` is the only place the `sample_rate_clamped` diagnostic fires; `statsd_in`
  doesn't emit it ([ADR `lossless-transit`](adr/lossless-transit.md), W3).

## Measuring a flow's shape with `shape`

`shape` answers "what do the events on this leg look like?": how many attributes they carry, how
long their keys and values are, how deeply values nest, how many metric records ride on each event,
how many distinct key-sets a source produces, and how many events arrive per batch.

**Put `shape` on its own branch of a fan-out, never in the flow you care about.** It is a
transform that rewrites each event into a measurement of that event and drops the original payload.
[`examples/shape-tap.yaml`](../examples/shape-tap.yaml) is the runnable shape:

```
statsd ─┬─> rollup ─> metrics          (the real pipeline, unchanged)
        └─> tap ─> shape_rollup ─> shape_out
```

**It emits counts and lengths only**: never an attribute key, attribute value, log body, or metric
name, in a metric, tag, diagnostic, or telemetry point
([ADR `shape-observer-component`](adr/shape-observer-component.md)). Rely on that when sending the
result somewhere the traffic itself could never go: what leaves the tap describes the traffic's
shape, not a sample of it. Two edges to know:

- `resource: drop` (the default) replaces the batch's `Resource` with an empty one, so no resource
  attribute value flows out. `resource: keep` forwards it unchanged; set it only when a per-service
  breakdown downstream is worth that identity traveling with the measurements. It applies to the
  per-event measurements only. Per-batch and cumulative measurements go out at each flush under an
  empty `Resource` regardless, because a flush window spans many batches and has no single resource
  to keep. For a per-service view of those, place one `shape` per source.
- The batch's `Scope` passes through either way. A scope names an instrumentation library instead
  of carrying payload, and a transform has no hook to substitute one.

**Put an `aggregate` after it.** Every distribution-shaped quantity goes out raw (one
`MetricKind::Samples` value per key, per value, per nested map, per batch), because summarization is
an explicit, operator-chosen stage in `logit`; see "Raw samples and set members" above. The default
`distributions: sketch` gives percentiles; `distributions: samples` keeps exact values, for
collecting a survey instead of watching a dashboard.

**To measure how much an event widens, use two taps**: one straight off the listener and one after
your transform chain. Each tags its output with its own component name (`tap`), so the two stay
distinct series through a shared `aggregate`. Measurements are also tagged `source` (the batch's
origin component) and, per event, `signal` (`log`, `metric`, `span`, or a `+`-joined combination),
so signal co-occurrence is an ordinary tag value.

**A tap isn't free; remove it when you're done.** Adding one turns a single-consumer edge, which
costs nothing, into a fan-out with a *mutating* branch. The cost depends on the other branch:
against a sink it's racy (one `Arc`, plus a whole-batch clone only when the timing goes the wrong
way); against another transform or a Lua stage, one of the two always clones.
`docs/design/memory.md` §3 has the case-by-case account. The tap's own per-event cost is pinned in
`crates/logit-bench/tests/allocations.rs`.

**What to watch:**

- `logit.shape.tracking_overflow` (gauge, 0/1): one of the two cumulative tables hit its cap
  (`max_tracked_keys`/`max_tracked_keysets`, both 4096 by default). New keys and key-sets are then
  counted instead of tracked, so `logit.shape.distinct_keys` and `.distinct_keysets` become lower
  bounds and `.keyset_share.top1`/`.top5` under-report. Raise the cap or, better, put a `keep` in
  front of the tap so it measures the key-set you intend to carry.
- `logit.transform.keys.untracked`/`.keysets.untracked` (count): the matching drop counters.
- `logit.transform.batches.dropped`: more than 4096 batches arrived in one flush window. Shorten
  `interval`, or accept that the per-batch distribution samples the window instead of covering all
  of it.

All three counters name nothing observed; like everything else here, they are counts.

`shape`'s "batch" is whatever its upstream delivered: a listener's accumulator flush by default,
the wire's own grouping when that listener runs `receive.batch_max_events: 1`, and always the
wire's grouping for `otlp_in` and `prometheus_in`, which have no accumulator.

## `otlp_in`: put `keep` in front of it

**Put a `keep` component immediately downstream of `otlp_in`, naming only the attribute keys you
intend to keep.** `otlp_in`'s attribute *keys* are arbitrary strings the peer supplies. Every other
listener here (statsd's `#tag:value`, syslog's structured-data field names) has its possible keys
fixed by `logit`'s own decoder; `otlp_in` takes whatever a remote OTLP exporter sends.
`crates/logit-proto/src/otlp/common.rs`'s `key_values_into_attrs` interns every OTLP
`KeyValue.key` it decodes into the process-wide interner (`crates/logit-core/src/interner.rs`),
which never evicts (`docs/known-gaps.md`'s interner entry). A client that sends a *different* key on
every request (an ID embedded in a key name, a misbehaving or malicious exporter) grows that table
for the life of the process, and nothing else stops it.

A `keep` turns the unbounded, peer-controlled key set into a fixed, `logit`-controlled one like
every other listener's, the same reasoning
[`examples/nginx-to-influxdb.yaml`](../examples/nginx-to-influxdb.yaml) applies ahead of `aggregate`,
extended to cover interning as well as series cardinality. It matters most where `otlp_in` faces
something other than `logit`'s own trusted fleet (a third-party exporter, a multi-tenant ingest
path); see `docs/known-gaps.md`'s interner entry for when the underlying "listeners are private by
deployment shape" premise is worth re-checking.

## `otlp_in`: accepted `Content-Type`s, and what a browser client needs

`otlp_in`'s HTTP transport accepts a POST body as `application/x-protobuf`,
`application/protobuf`, or `application/json`. An absent or empty `Content-Type` is treated as
protobuf, matching clients that predate OTLP/JSON support. The response uses the request's
encoding: protobuf for a protobuf request, JSON for a JSON one. gRPC is protobuf-only regardless,
since OTLP/gRPC's framing *is* protobuf. See [ADR `otlp-json-decoding`](adr/otlp-json-decoding.md)
for the JSON decoding design.

**A browser exporter must reach `otlp_in` through a same-origin reverse proxy.** `otlp_in` has no
CORS support (`handle_http` returns 404 for an `OPTIONS` preflight and sets no
`Access-Control-Allow-Origin`), so a **cross-origin** browser exporter, pointed at `otlp_in`
directly from a page on a different origin, can't reach it at all. Put a reverse proxy in front that
shares the page's origin instead of opening `otlp_in` to arbitrary browser origins
(`docs/known-gaps.md`).

## `datadog_in`: standing in for Datadog's intake

`datadog_in` answers a Datadog Agent the way Datadog's intake does, so an Agent sends it series,
sketches, service checks, events, logs, APM traces, and APM stats with nothing changed but its URLs.
Point the Agent's `dd_url`, `logs_config.logs_dd_url`, and `apm_config.apm_dd_url` at it to replace
Datadog, or add it under `additional_endpoints` (and the `logs_config`/`apm_config` equivalents) to
receive a copy while Datadog keeps receiving everything.
[`examples/datadog-intake-standin.yaml`](../examples/datadog-intake-standin.yaml) has a runnable
config and the Agent-side settings for both. See
[ADR `datadog-agent-and-intake-relay`](adr/datadog-agent-and-intake-relay.md) for the design.

```yaml
components:
  datadog:
    type: datadog_in
    bind: 127.0.0.1:8080
    api_keys: [!env DD_API_KEY]   # empty or absent accepts any key
    idle_timeout: 120s            # off by default
```

**Routes.** Each of these decodes into events:

| Route | What an Agent sends there |
|---|---|
| `/api/v2/series` (protobuf or JSON), `/api/v1/series` | metric series |
| `/api/v1/distribution_points` | raw distribution values |
| `/api/beta/sketches`, `/api/v1/sketches` | distribution sketches |
| `/api/v1/check_run`, `/api/v2/service_checks` | service checks |
| `/api/v2/events`, `/api/v1/events`, `/intake/` | events |
| `/api/v2/logs`, `/v1/input` | logs |
| `/api/v0.2/traces` | APM traces (`AgentPayload`) |
| `/api/v0.2/stats` | APM stats, relayed rather than recomputed |

`/api/v1/validate` answers `200` for a valid key. Host and inventory metadata
(`/api/v2/host_metadata`, `/api/v1/metadata`, host metadata on `/intake/`) and the process and
orchestrator collectors (`/api/v1/collector`, `/api/v1/container`, `/api/v2/orch`) are answered
`202` and discarded, counted `logit.input.requests.acknowledged{route}`. **Any other path gets
`404`**, deliberately: an Agent feature this listener doesn't speak then shows up as errors in the
Agent's own status and logs, instead of as data acknowledged and silently lost. A known path with
the wrong method gets `405`.

**Authentication.** With `api_keys` set, a request whose `DD-API-KEY` header matches none of them
gets `403`, counted `logit.input.requests.rejected{reason="auth"}`. A key is never logged. Take the
keys from the environment with `!env`, as the example does. `api_keys` is a shared secret, not
transport security: add `tls:` before binding beyond loopback, since otherwise the key crosses the
network in the clear. With `api_keys` empty, every request is accepted and `/api/v1/validate`
answers `200` to any key, so an Agent can't tell a wrong key from a right one.

**Compression.** The Agent compresses with zstd by default, and `datadog_in` decodes zstd, gzip, and
deflate (the zlib-wrapped form the Agent sends under that name), so nothing needs changing on the
Agent. Any other `Content-Encoding` gets `415`.

**Size caps.** These are fixed, sized to what an Agent sends, not configuration:

- A compressed body over 5 MiB gets `413`, on every route.
- A body that decompresses past 5,242,880 bytes gets `413`, except on `/api/v0.2/traces`, whose cap
  is 16 MiB. The first is the Agent's own serializer limit for series and sketches, and the trace
  agent caps its payloads at 3.2 MB, so a conforming Agent stays under both.
- A zstd frame that declares a window above `max(the route's decompressed cap, 8 MiB)` gets `413`
  before anything is decompressed, because the decoder would reserve that window up front. The 8
  MiB floor exists because a Go `klauspost/compress` streaming writer -- what the Agent's forwarder
  uses -- declares an 8 MiB window regardless of how little it actually writes, so the 5 MiB
  metrics/logs cap still accepts a legitimately small body sent under that window. The floor
  doesn't raise how much decoded data a route accepts: the decompressed output is still capped at
  the route's own limit.

**A full pipeline gets `503`, not a blocked connection.** When the pipeline doesn't accept a
request's batches within 5 seconds, `datadog_in` answers `503` with `Retry-After: 1` rather than
holding the connection open, which is what `otlp_in` and `prometheus_in` do. The Agent's forwarder
retries a `503` with backoff and holds the payload in its retry queue meanwhile, so nothing is lost
until that queue fills. A blocked connection would instead cost the Agent 20 seconds before its own
timeout, and then the same retry.

- **A `503` means no consumer holds the batch that timed out.** Each batch goes to every consumer
  downstream of `datadog_in` or to none, however many there are, so the Agent's retry is its only
  copy.
- **Delivery is at-least-once.** A traces or stats request carries one batch per tracer or client
  payload, and a `503` partway through means the retry delivers the batches already delivered
  before the deadline again. Datadog's own intake has the same shape: a resent series point
  overwrites, a resent log or span duplicates.
- **Watch `logit.input.requests{class="busy"}`.** A steady rate means the pipeline can't keep up
  with its Agents, and the Agents' retry queues are absorbing the difference.
  `logit.input.batches.dropped{reason="busy"}` counts the batches those `503`s left undelivered:
  deferred to the Agent, not lost. It and `logit.component.batches.sent` are disjoint: a batch
  counts under one or the other, never both.

**What to watch.** `logit.input.requests{route, class}` shows which routes are arriving and how
they're answered, and `logit.input.requests.rejected{reason}` says why a `4xx` happened: a nonzero
`unknown_route` means an Agent is using a route this listener doesn't speak, and `auth` a key
mismatch. `docs/design/internal-telemetry.md`'s `datadog_in` section has every counter, and its
`datadog` codec section the per-item drops inside a request that decoded.

## `datadog_trace_in`: standing in for the Agent's APM API

`datadog_trace_in` answers a dd-trace tracer the way a local Datadog Agent's APM receiver does, so
an application sends it traces and client-computed stats with nothing changed but where it points:
`DD_AGENT_HOST` and `DD_TRACE_AGENT_PORT`, or `DD_TRACE_AGENT_URL` (`http://HOST:8126` or
`unix:///PATH`). [`examples/datadog-agent-standin.yaml`](../examples/datadog-agent-standin.yaml)
pairs it with a DogStatsD `statsd_in` on `:8125`, the Agent's other application-side listener.

```yaml
components:
  apm:
    type: datadog_trace_in
    bind: 127.0.0.1:8126                  # and/or:
    socket: /var/run/datadog/apm.socket   # the directory must exist
```

**Send its output to a real Agent or an OTLP backend, never straight to `datadog_out`.** Spans
arrive exactly as the tracer wrote them: nothing here obfuscates SQL or URLs, normalizes names,
marks top-level spans, applies sampling, or computes APM stats, all of which an Agent does before
Datadog sees a span. Route them to `datadog_trace_out` in front of a real Agent, or to `otlp_out`.
`datadog_out` skips a span no Agent has processed.

**Routes.** `/v0.3/traces`, `/v0.4/traces`, `/v0.5/traces`, and `/v0.7/traces` (msgpack, `POST` or
`PUT`) decode into span events, and `/v0.6/stats` into APM stats events. A JSON v0.3/v0.4 body gets
`415`: only msgpack is decoded, which is what every current tracer sends. Every trace reply sets
every service's sampling rate to 1.0, so the tracer keeps everything. Two groups of routes are
answered without relaying anything:

- **`404`, as an Agent with the feature turned off answers:** `/v0.1/traces`, `/v0.2/traces`,
  `/v1.0/traces`, `/v0.1/pipeline_stats`, `/telemetry/proxy/`, and `/v0.7/config`. A tracer
  doesn't enable these, because `/info` doesn't list them.
- **`200` and discarded:** `evp_proxy`, profiling, debugger, symbol-database, DogStatsD-proxy,
  tracer-flare, and OpenLineage uploads. Several tracers send these whatever `/info` says, and
  answering stops them logging an error per upload. Each is counted
  `logit.input.requests.acknowledged{route}`.

Any other path gets `404`.

**`/info` shapes what the tracer sends.** A tracer reads it at startup. This listener's document
lists only the routes above, so the tracer doesn't turn on telemetry forwarding, Remote
Configuration, or the v1.0 trace form. It sets `client_drop_p0s: false`, so the tracer sends every
trace rather than dropping priority-0 ones, which would leave them out of the relay. It lists
`/v0.6/stats`, so a tracer that computes stats keeps sending them. The module doc in
`crates/logit-inputs/src/datadog_trace.rs` gives the reason for every field.

**Tracer headers become resource attributes.** `Datadog-Meta-Lang`, `-Lang-Version`,
`-Tracer-Version`, `Datadog-Container-ID`, and the tracer's other identity and client-computation
headers land on the batch resource as `datadog.tracer.*`, which `datadog_trace_out` writes back as
headers. For `/v0.7/traces`, the payload's own fields win over a header.

**The Unix socket.** `socket:` binds a Unix stream socket, as the Agent's `receiver_socket` does.
The directory must already exist. A stale socket file from an earlier run is replaced, but a path
that exists and isn't a socket is refused, so a typo can't delete a file. The new socket is mode
`0666`, so a tracer running as any user can connect. Restrict access with the directory's
permissions if that's too open. `tls:` applies to `bind` only.

**A full pipeline loses spans.** When the pipeline doesn't accept a request's batch within 2
seconds, `datadog_trace_in` answers `503` with `Retry-After: 1`, and — unlike `datadog_in`, whose
own Agent retries — that `503` is loss, counted `logit.input.batches.dropped{reason="busy"}`
([ADR `datadog-agent-and-intake-relay`](adr/datadog-agent-and-intake-relay.md), decision 11).
Prevent it downstream: give the sinks this listener feeds a `buffer:` (memory, or `disk:` for a
long outage) large enough to absorb a stall, so the channel `datadog_trace_in` sends into keeps
draining.

**What to watch.** `logit.input.spans` counts spans delivered, `logit.input.requests{route, class}`
which routes arrive, `logit.input.batches.dropped{reason="busy"}` loss, and
`logit.input.requests.rejected{reason="unsupported_route"}` a tracer trying a feature this listener
doesn't speak. `docs/design/internal-telemetry.md`'s `datadog_trace_in` section has every counter.

## Prometheus remote-write: receiving, sending, and picking a version

`prometheus_in` and `prometheus_out` each have two modes, chosen by which field is set:

- `prometheus_in` scrapes `scrape_targets:`, or binds a remote-write **receiver** on `bind:`.
- `prometheus_out` exposes a registry on `bind:`, or **sends** remote-write to an `endpoint:`.

Setting a field that belongs to the *other* mode is a config error (graph rules 55 and 56), not a
silently ignored setting. See [ADR `prometheus-remote-write`](adr/prometheus-remote-write.md) for
the design and
[`examples/prometheus-remote-write-receive.yaml`](../examples/prometheus-remote-write-receive.yaml)/
[`examples/prometheus-remote-write-send.yaml`](../examples/prometheus-remote-write-send.yaml) for
runnable configs.

**Bind the receiver to loopback or pod-local, and front it with something that authenticates.**
`bind_tls:` gives it real server TLS, but that is transport security only: there is no bearer token,
no basic auth, and no mutual-TLS identity check beyond `rustls` accepting whatever chain a client
presents when `client_ca_file` is set. Anything that can reach the socket can write series into the
pipeline. So bind `127.0.0.1:9201`, as
[`examples/prometheus-remote-write-receive.yaml`](../examples/prometheus-remote-write-receive.yaml)
does, and put an ingress, a service mesh, or an authenticating reverse proxy in front, the same
posture as `admin:` and `prometheus_out`'s exposition `bind:`. Making it reachable from off-host is
a deliberate choice, not one to inherit from an example. Tracked in `docs/known-gaps.md`.

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

**Set `idle_timeout:` on a remote-write receiver.** It is opt-in on every listener
([ADR `idle-connection-timeout`](adr/idle-connection-timeout.md)), and remote-write is the case its
recommendation describes: senders write on a fixed cadence, so a connection quiet for a minute
isn't coming back. It does a second job here too: the bound on a request whose **body stalls
mid-upload** is derived from it. With `idle_timeout:` unset, a half-uploaded request holds one of the
listener's 1024 connection permits until the sender goes away, and the stalled body never gets its
`408`. Size it above the senders' longest normal gap; `60s` is comfortable for Prometheus's default
`remote_timeout` of 30s.

A Prometheus writing into the receiver needs only its own `remote_write:` block. It is the sender,
so no server-side flag is involved:

```yaml
remote_write:
  - url: http://logit:9201/api/v1/write
    # protobuf_message: io.prometheus.write.v2.Request   # omit for 1.0
```

**Keep `metadata_cache:` on for a 1.0 fleet.** Prometheus's 1.0 sender ships a family's type,
`# HELP`, and `# UNIT` in *separate* requests on its own schedule (`metadata_config`, once a minute
by default), not attached to the samples they describe. Without the cache, the receiver decodes
nearly every 1.0 request as untyped `unknown` families, and a histogram arrives as three unrelated
`_bucket`/`_sum`/`_count` series instead of one record. No samples are dropped either way; what you
lose is the metric *kinds*, and with them the ability to compute a rate over a counter or a quantile
over a histogram downstream. `max_families: 0` turns the cache off, which is right only for a
pure-2.0 fleet. **Watch `logit.input.metadata_cache.evicted{reason="expired"}` against a live
sender:** a steady stream means `ttl` is shorter than that sender's metadata cadence, and families
lapse back to untyped between refreshes.

**`--web.enable-remote-write-receiver` belongs to the other direction.** Set it on the Prometheus
side when `logit` is the **sender**; a stock Prometheus doesn't accept remote-write until started
with it:

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

- **`endpoint:` is the receiver's full write URL, path included**, not a host plus a separate
  `path:`. `path:` belongs to the other mode, and setting it here violates rule 56.
- **TLS is selected by the scheme**, and `endpoint_tls:` tunes it: a private CA, a client
  certificate, or the deliberately awkward `insecure_skip_verify`, which logs a startup warning.
- **Five headers are reserved:** the four protocol headers (`Content-Type`, `Content-Encoding`,
  `X-Prometheus-Remote-Write-Version`, `User-Agent`) plus `Content-Length`. Rule 56 rejects them in
  `headers:` at config time instead of letting the sink silently override them.

**Choosing `version: 1` or `2`.** There is no negotiation and no fallback: pick the version your
receiver speaks, as you pick an exposition dialect. The choice depends on the destination:

- **`version: 1`** (`prometheus.WriteRequest`) is the default, and every remote-write receiver
  deployed today accepts it. Use it unless you know the receiver speaks 2.0. Its one real cost: 1.0
  has no field for a counter's start time, so `Series::created` (an OpenMetrics `_created` series,
  an OTLP `start_time_unix_nano`) is dropped on the way out.
- **`version: 2`** (`io.prometheus.write.v2.Request`) is worth setting when the receiver is a recent
  Mimir, Thanos, VictoriaMetrics, Grafana Cloud, or a Prometheus 3.x started with
  `--web.enable-remote-write-receiver`. It interns every label and metadata string in a request-wide
  symbol table (smaller bodies for the same series), carries `Metadata` inline on each series instead
  of in separate requests, carries the created timestamp per sample, and answers with
  `X-Prometheus-Remote-Write-{Samples,Histograms,Exemplars}-Written` so a sender learns what was
  stored. A receiver that doesn't speak 2.0 answers `415`, which this sink classifies as permanent:
  the misconfiguration is reported immediately instead of retried.

Native histograms are skipped and counted on both wires regardless of version
(`docs/known-gaps.md`), so this choice doesn't affect them.

**What to watch.**

- Receiver: `logit.input.writes{class}` (`ok` against `bad_request`/`unsupported`/`oversize`; a
  nonzero `unsupported` usually means a sender whose `Content-Type` or `Content-Encoding` doesn't
  match what it sends), `logit.input.write.duration`, `logit.input.samples`, and the
  `metadata_cache` metrics above.
- Sender: `logit.output.requests{class}`, `logit.output.request.duration`, `logit.output.samples`.
  A `4xx` is permanent and the batch is dropped; the throttled `remote_write_rejected` diagnostic
  quotes the receiver's message, which for Prometheus and Mimir names the offending series. A `3xx`
  means the endpoint is redirecting; this client deliberately doesn't follow redirects.
- A sender feeding one series from two upstream branches can draw out-of-order `400`s from a
  receiver with no out-of-order window. That is the topology, not the sink; `docs/known-gaps.md` has
  the row.

## TLS

`otlp_out` (both `protocol: http` and `protocol: grpc`) and `otlp_in` (both transports) can speak
TLS; see [ADR `otlp-tls-and-pooled-grpc-client`](adr/otlp-tls-and-pooled-grpc-client.md) for the
design. On `otlp_out`, `endpoint`'s scheme selects TLS, the same convention every OTel SDK uses:

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
`tls:`. A non-empty `tls:` block under a plaintext endpoint is a config error (rule 24), not
silently ignored, since it would have no effect. `ca_file`/`cert_file`/`key_file` paths resolve
relative to the config file's own directory, like `lua_file`, and, like any other field, accept
`!env` if the certificate material comes from the environment instead of a mounted file
(ADR `env-yaml-tag`).

Mutual TLS adds a client certificate:

```yaml
    tls:
      ca_file: /etc/logit/tls/ca.pem
      cert_file: /etc/logit/tls/client.pem
      key_file: /etc/logit/tls/client.key
```

`otlp_in` has no endpoint to read a scheme from, so a `tls:` block's presence turns TLS on for the
listener, on both transports:

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

`client_ca_file` requires every connecting client to present a certificate that chains to it
(mutual TLS). Omit it to accept any client that completes the handshake.

`otlp_in.handshake_timeout` (default 5s) bounds that handshake: a client that completes the TCP
connect and never sends a ClientHello is closed and its concurrency-cap permit released. It also
applies to a plaintext `otlp_in`, where it bounds the wait for the connection's first byte instead,
using a non-consuming `TcpStream::peek` so the byte is still there for `hyper`'s version sniff.
Rule 45 has no TLS-context clause rejecting it there, unlike on `syslog_in`/`graphite_in`/
`statsd_in` under `transport: udp`. See
["`handshake_timeout` on a TCP listener"](#handshake_timeout-on-a-tcp-listener) above for why, and
for the gap that leaves, which `otlp_in.idle_timeout` (off by default) closes: see
["`idle_timeout` on a TCP listener"](#idle_timeout-on-a-tcp-listener) above, including the note on
a request that starts right at the idle deadline.

**`tls.insecure_skip_verify`** (`otlp_out` only) disables server-certificate verification: the
connection is still encrypted, but any certificate is accepted. `logit` logs a startup warning
whenever it's set. Use it only for a throwaway or pre-production endpoint, not a real deployment.
Validation rejects it together with `ca_file`, because a specific trusted CA and "trust nothing"
contradict each other.

**What to watch.** A handshake failure on either side surfaces through the same
`connection_error`/`network_error` diagnostics and `logit.output.requests{class="network_error"}`/
listener-side `logit.component.diagnostics` counters as any other transport failure; there's
nothing TLS-specific beyond that. `docs/known-gaps.md` tracks two open items: **certificates are
read once at startup, so a renewed certificate needs a restart**, not a live reload; and `otlp_out`
has no `server_name` override for an endpoint reached by IP or through a proxy.

### Syslog over TLS (RFC 5425)

`syslog_in`/`syslog_out` can speak TLS too: RFC 5425, syslog framed per RFC 6587 over TLS over TCP
([ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)). As with
`logit_in`/`logit_out` ([Forwarding between `logit` nodes](#forwarding-between-logit-nodes) below),
`bind` and `endpoint` are bare `host:port` strings with no URL scheme to signal TLS, so **a `tls:`
block's presence turns TLS on and makes it required**; there is no plaintext fallback once one is
configured. It applies to `transport: tcp` only. DTLS (syslog over TLS over UDP) is out of scope,
so `tls:` under `transport: udp` is a config error, not a silently ignored block. The fields are the
same `TlsServerConfig`/`TlsClientConfig` pair every other TLS-capable component uses:

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

For mutual TLS, add `cert_file`/`key_file` together to `syslog_out`'s `tls:` block, as in
`otlp_out`'s mutual TLS example above. `tls.insecure_skip_verify` (`syslog_out` only) behaves
identically too, including the rejection alongside `ca_file`.

**`syslog_out.connect_timeout` bounds the TCP connect and the TLS handshake as two separate
phases**, not one combined deadline, so a TLS connect can take up to twice the configured value
([ADR `syslog-tcp-ingress-and-tls`](adr/syslog-tcp-ingress-and-tls.md)'s amendment). Account for
that if you raise it from the default.

`syslog_in.handshake_timeout` (default 5s) is the receiving side's equivalent: one budget for the
TLS accept, then a fresh one for the wait for the connection's first byte, so a TLS peer that
connects and goes quiet is dropped after at most 10s. It applies on the plaintext TCP arm too
(where only the first-byte phase exists), and not at all under `transport: udp`. See
["`handshake_timeout` on a TCP listener"](#handshake_timeout-on-a-tcp-listener) above.
`syslog_in.idle_timeout` (off by default) bounds the gap after that; see ["`idle_timeout` on a TCP
listener"](#idle_timeout-on-a-tcp-listener) above.

**What to watch.**

- `syslog_out`: `logit.output.requests{class="ok"|"error"}` (one per attempt) and
  `logit.output.reconnects`, which should stay near zero in steady state. A climbing count on a TLS
  connection means the peer or the network is unstable, not this sink. Plaintext and TLS
  connections are counted the same way, since both take the same connect path.
- `syslog_in`: `logit.input.connections` (a gauge that should match the number of connected
  `syslog_out` peers) and `logit.input.connections.rejected{reason="limit"}` (nonzero means the
  1024-connection cap is binding).
- Both: a handshake failure, a framing violation, or an oversize or malformed frame surfaces through
  `logit.component.diagnostics{key="connection_error"|"framing_error"}` and
  `logit.input.frames.dropped{reason="oversize"|"malformed"|"truncated"}`. There is no separate
  TLS-specific counter, as with `otlp_in`/`otlp_out`.

`docs/known-gaps.md` tracks what's still open: DTLS, certificates read once at startup, and no
`server_name` override. `idle_timeout` covers the post-handshake idle case.

A **TCP `graphite_in`** takes the identical `tls:` block, because it runs on the same listener
driver ([ADR `graphite-carbon-relay`](adr/graphite-carbon-relay.md)'s 2026-09-14 amendment):
`cert_file`/`key_file`, optional `client_ca_file` for mutual TLS, presence turns TLS on and makes
it required, `transport: tcp` only. There is no matching `graphite_out` half, because carbon's own
senders speak no TLS; the listener side is for a `logit`-to-`logit` or stunnel-shaped relay hop.
Watch the same set as `syslog_in`, including `logit.input.frames.dropped{reason}`.

A **TCP `statsd_in`** is the third listener on that driver, and takes the same block on the same
terms; see ["`statsd_in`: `transport: tcp` and
TLS"](#statsd_in-transport-tcp-and-tls) above for its framing and sizing behavior. Plain statsd
clients speak no TLS either, so this too is for a `logit`-to-`logit` or stunnel-shaped relay hop,
not something an application's statsd client dials directly. Watch `syslog_in`'s set again.

**`statsd_out` is the sink half of that hop**
([ADR `statsd-output`](adr/statsd-output.md)'s TLS amendment). It takes the same
`TlsClientConfig` as `syslog_out`/`logit_out`, for `transport: tcp` only; the block's presence turns
TLS on and makes it required; `connect_timeout` bounds the connect and the handshake as two separate
phases; and `insecure_skip_verify` behaves (and warns) exactly as on those sinks. **One behavior
differs from the other sinks:** a TLS write failure is `Fault::Ambiguous` and the batch is never
resent, because a redelivered statsd counter corrupts a value instead of duplicating a line. See
["`statsd_out`: `transport: tcp` and TLS"](#statsd_out-transport-tcp-and-tls) above.

Which component takes which block:

| Component | Block | Turned on by | Notes |
|---|---|---|---|
| `otlp_out` | `TlsClientConfig` | an `https://` `endpoint` | `tls:` under a plaintext endpoint is rule 24 |
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
([ADR `native-transport-handshake-and-ack`](adr/native-transport-handshake-and-ack.md)). Use them to
split collection from processing across nodes, the shape [`docs/OVERVIEW.md`](OVERVIEW.md) names as
the reason the native wire format exists: an edge or sidecar process collects and forwards
unaggregated, and a central process receives, aggregates, and delivers.
[`examples/forwarder-edge.yaml`](../examples/forwarder-edge.yaml)/
[`examples/forwarder-central.yaml`](../examples/forwarder-central.yaml) are a complete, runnable
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

**TLS.** `logit_out`'s `endpoint` is a bare `host:port` with no scheme to signal TLS (unlike
`otlp_out`'s URL-shaped endpoint), so a `tls:` block's presence turns TLS on, the same convention
`otlp_in` uses server-side:

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

**Sizing `request_timeout` against `buffer.retry_budget`.** Keep `request_timeout` comfortably
under `retry_budget`. `logit_out.request_timeout` (default 10s) bounds one attempt: the connect,
the handshake, and the ack wait all share it, as with `otlp_out`'s timeout. `buffer.retry_budget`
(default 60s; see [Sink delivery buffering](#sink-delivery-buffering)) bounds all retried attempts
together. A `request_timeout` close to or above the retry budget leaves room for at most one attempt
before the budget expires, which defeats retrying.

`request_timeout` relates only loosely to the far end's handshake grace. A `logit_out` whose
`request_timeout` is shorter than its peer's handshake patience gives up first; the connection
isn't unsafe. That far-end grace is `logit_in.handshake_timeout` (default 5s), applied *per
pre-`Hello` phase*: independently to the TLS accept and to the `Hello` read that follows, so a TLS
peer that connects and goes silent is dropped after at most 10s, not 5s. See
["`handshake_timeout` on a TCP listener"](#handshake_timeout-on-a-tcp-listener) above; it bounds
only the pre-`Hello` phases.

An already-handshaken connection that goes quiet is bounded by the separate, opt-in
`logit_in.idle_timeout` (off by default); see ["`idle_timeout` on a TCP
listener"](#idle_timeout-on-a-tcp-listener) above. Before that close, the `logit_out` peer receives
`Reject{GOING_AWAY, "idle for <dur>"}`, and it probes for exactly that signal before reusing a
pooled connection, so an idle-timed-out `logit_in` costs `logit_out` a reconnect, not a lost batch.

**A `logit_in` at its connection cap can cost a batch under the default delivery posture.** A peer
that gets `Reject{code: REJECT_INTERNAL}` never classifies it `permanent`:

- At the handshake, with nothing of the batch written yet, it's `clean`, and the batch is retried
  within `retry_budget`.
- Once a frame has left on that connection, it's `ambiguous`. Under `logit_out`'s default
  `at_most_once` posture that isn't retried: the batch is dropped and counted, and only the
  connection recovers.

Either way the sink reconnects on its own once the peer has capacity, with no operator action. To
risk a duplicate instead of losing that batch, set `buffer.delivery: at_least_once` on the
`logit_out` component. The same holds for `Reject{code: REJECT_GOING_AWAY}` during the peer's own
shutdown.

**What to watch.**

- `logit_out`: `logit.output.requests{class}` (`ok`/`clean`/`ambiguous`/`permanent`, one per `send`
  attempt), `logit.output.reconnects` (should stay near zero in steady state; a climbing count means
  the peer or the network is unstable), and `logit.output.ack.duration`.
- `logit_in`: `logit.input.connections` (a gauge that should match the number of connected
  `logit_out` peers), `logit.input.connections.rejected{reason="limit"}` (nonzero means the
  1024-connection cap is binding; raise it or shed load upstream), and `logit.proto.errors{reason}`
  (`magic`/`version`/`crc`/`truncated`/`too_large`/`codec`/`handshake`; any of these on a healthy
  link points at a version-mismatched or misbehaving peer, not routine loss).
- Both sides: `logit.proto.frames{direction,codec,compression}` and `logit.proto.frame.bytes` for
  throughput.

`docs/known-gaps.md` tracks what's still open: there is no credit-based flow control (the sender
never has more than one frame outstanding), and `logit_in`'s shutdown grace is fixed at 5s with no
`receive:`-shaped knob to change it.

## The nginx-side recipe

This section covers the operational side of running `logit` against a real nginx. The schema
(which attribute name each nginx variable is logged under, the quoting rules, what `http_access`
does to each field, and the equivalent snippet for Apache, HAProxy, Varnish, Squid, Envoy, Caddy,
and Traefik) lives in [`docs/http-access-logs.md`](http-access-logs.md). The working reference
config is [`examples/nginx/nginx.conf`](../examples/nginx/nginx.conf) (its `access_semconv`
`log_format`) with [`examples/nginx-to-influxdb.yaml`](../examples/nginx-to-influxdb.yaml)
(`syslog_in` → `json` → `http_access` → `trace_context` → `kv_metrics` → `keep` → `keep_values` →
`aggregate` → `influxdb_out`, plus `stdio_out` for visibility).

Two fixes from that doc bear repeating, because skipping either fails silently:

- **Quote `$status`** (`"http.response.status_code":"$status"`). nginx can log a literal `000` on
  an abnormal termination, and an unquoted `000` is invalid JSON that loses the whole line.
- **Set `invalid_utf8: replace` on the `json` component in front of `http_access`.** `escape=json`
  passes bytes `>= 0x80` through raw, so one Latin-1 `User-Agent` otherwise fails the entire line's
  parse.

### Which directives to add

Add one `access_log` line per `server {}` block, pointing the `access_semconv` format at `logit`
over syslog/UDP:

```nginx
access_log syslog:server=<logit-host>:5140,tag=nginx_access,nohostname access_semconv;
```

During cutover, leave the existing access log in place as a second `access_log` line (nginx allows
more than one per block). `error_log` needs no change: it stays in nginx's own non-JSON format,
which is out of scope here.

### Why keep the existing stdout destination during cutover

The second `access_log` line is a temporary safety net, not a permanent duplicate. Point `logit` at
the syslog line while the verbose stdout line keeps running, confirm metrics land where you expect
(a `stdio_out` block per request, a Grafana/InfluxDB query against the fields your `kv_metrics`
component derives, or whatever verification your environment uses), and drop the stdout line only
once the `logit` path is trusted. Running both costs only a slightly larger nginx log volume during
that window.

### The syslog message-size limit and its symptom

`docs/known-gaps.md` has [the full write-up](known-gaps.md) of what happens when a syslog-bound
access log line is too large for one datagram. It's worth reading, and more reassuring than it
first sounds: nginx's `large_client_header_buffers` rejects an oversized request with a 400 before
nginx builds a log line for it, which closes off the "attacker sends a huge `Host` header" vector by
nginx's default behavior, not by anything `logit` does. The pipeline's graceful degradation on a
truncated line from any other cause (a different unbounded field, a larger
`large_client_header_buffers`, a different syslog client) was verified by sending a hand-truncated
datagram straight to `syslog_in`, bypassing nginx.

If a syslog datagram does truncate mid-JSON-object, `logit` neither crashes nor wedges the
listener. The symptoms:

- `stdio_out` shows a log-only block: the raw (truncated) message and its `syslog.*` attributes,
  with none of the JSON body's fields merged in.
- stderr gets a throttled `parse_failure` diagnostic naming the `json` component that failed to
  parse it.
- Any *fieldless* counter (`nginx.requests` in the reference config, which counts every event
  regardless of attributes) still increments for that request. Any metric that reads a field from
  the JSON body (`nginx.bytes_sent`, the two distributions) derives nothing for it, since there's no
  field to read.
- Requests before and after are unaffected; the blast radius is exactly the one truncated line.

### The ordering rule

**Start `logit` and confirm it's listening *before* pointing nginx's `access_log syslog:` directive
at it.** UDP is fire-and-forget: a line nginx sends before `logit`'s listener is bound is lost with
no error anywhere, in nginx or in `logit`.

With `admin: { bind: ... }` set (see [Probes and exit codes](#probes-and-exit-codes)), wait for
`/readyz` to return `200`, or for `logit ready` to succeed, before starting nginx. `/readyz` reports
`ready` only once every listener, `syslog_in` included, has bound its socket:

```sh
until logit ready --admin http://<logit-host>:9600; do sleep 0.5; done
```

Without `admin:`, fall back to the `bound`/`ready` lifecycle log lines (default `--log-level info`;
see [Self-logging](#self-logging)): `bound` names each socket listener's address as it opens
(`syslog_in` included), and `ready` fires once every listener is bound. If neither is wired up, a
manual smoke test still works: send a line and watch for the corresponding `stdio_out` block:

```sh
logger -n <logit-host> -P 5140 -d -t smoke '{}'
```

`-d` forces UDP; without `-T` or `-d`, `logger`'s default depends on `/etc/services`, which isn't
reliably UDP-first everywhere. A `stdio_out` block for that line means the listener is up and
reachable. If nothing appears, anything nginx sent would be lost the same way.
