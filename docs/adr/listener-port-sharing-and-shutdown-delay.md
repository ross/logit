---
created: 2026-10-07
updated: 2026-10-07
---

# Listener port sharing with `SO_REUSEPORT`, a SIGTERM drain delay, and an admin Unix socket

## Status
Accepted

## Context
In Docker and Kubernetes a new version of `logit` is a new container, so no socket survives an
upgrade. For a node-local UDP listener, such as a statsd or syslog agent run as a DaemonSet with
`hostNetwork: true`, the only gap-free upgrade is overlap: with `maxSurge: 1` the new pod starts
before the old one stops, and both bind the same port on the node. That needs `SO_REUSEPORT` on
both sockets
([research note: live reload and socket handover](../plans/live-reload-and-socket-handover.md),
"The two worlds"). Three things stand in the way today:

- **Nothing sets `SO_REUSEPORT`.** `crates/logit-inputs/src/udp.rs`'s `bind_one` sets
  `SO_REUSEADDR` only for a multicast bind, and every TCP listener and the admin server bind
  through `tokio::net::TcpListener::bind` directly. The new pod's bind fails with `EADDRINUSE`,
  and `logit run` exits `1`.
- **A SIGTERM closes every listener socket the instant it arrives.** `/readyz` flips to
  `503 draining` at the same moment
  ([ADR `service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md)'s
  "Shutdown" section, [ADR `signal-handling`](signal-handling.md)). Behind a Service, a pod should
  keep reading until the endpoints have moved off it. Today a pod does that only when a `preStop`
  sleep delays the signal, and that needs a `sleep` binary that a distroless image doesn't have.
  On a host there's no `preStop` at all.
- **Two overlapping pods collide on the `admin:` port.** Sharing that port with `SO_REUSEPORT`
  would make the kubelet's readiness probe ambiguous: the kernel hashes each connection by its
  4-tuple, so a probe meant for the new pod can be answered by the old one. The rollout would then
  gate on the wrong process.

### What the kernel does
The behavior below was measured on Linux 7.1.13 (Fedora 44) over loopback, three runs per cell.
The experiment scripts lived in a gitignored scratch directory; the numbers in this section are
the record.

- **Membership.** Both sockets must set `SO_REUSEPORT` and share an effective UID. A second bind
  from a different UID, or from a socket without the flag, fails with `EADDRINUSE`, for UDP and TCP
  alike. Two sockets that both set `SO_REUSEADDR` can also bind one address before either listens,
  so the flags a bind sets are a decision, not a default.
- **Distribution.** The kernel pins each source flow (4-tuple) to one member and never splits a
  flow. The split is uneven: with 8 client flows, one run sent 2000 datagrams to one member and
  6000 to the other. Connected and unconnected senders behave the same.
- **A member joins.** Adding a third socket mid-run moved 6 to 9 of 16 flows, and 25 to 34 of 64,
  to a different member. No datagram was lost.
- **A member closes.** The closing socket loses what sat in its receive queue at the instant of
  close. For a stalled reader that's one full `SO_RCVBUF`: 221 datagrams, 212160 bytes of a
  212992-byte buffer, at 10k, 50k, and 100k datagrams/s. For a reader keeping up at 50k/s it was
  11 to 14 datagrams. Afterward the kernel re-hashes the closed member's flows to the survivors:
  for the next second at 50k/s, every datagram reached the surviving socket.
- **Multicast.** Two sockets joined to one group each received all 1000 of 1000 datagrams, with
  `SO_REUSEADDR`, `SO_REUSEPORT`, or both. `SO_REUSEPORT` doesn't split multicast.
- **TCP accept queue at close.** With `net.ipv4.tcp_migrate_req=0`, closing a listener reset every
  connection in its accept queue: 67 to 89 at 500 connections/s and 129 at 2000/s (a full
  backlog of 128), equal to the queue length in every run. Each of those clients had a successful
  `connect()` and got `ECONNRESET` after sending its line. With `=1` (Linux 5.14 and later) the
  kernel migrated the queue to the surviving listener: zero resets in all 12 runs. A listener that
  closed while still accepting had an empty queue and reset nothing at either rate under either
  setting.
- **An accepted connection closed with unread data** gets a reset (`ECONNRESET`). Closed after
  reading, or with nothing sent, it gets a clean EOF.
- **Kernel counters stay per socket.** `SO_MEMINFO` on a group member reports that member's own
  queue and drops, not the port's.
- **Checked against `logit`.** On the same host, two release-build `logit` processes shared one
  `statsd_in` UDP port with `reuse_port: true`, each with its own admin Unix socket. 16 source
  sockets sent 100-byte lines at 50k datagrams/s for 6 s, about 300k per run, and one process got
  SIGTERM at 3 s; three runs per arm.
  - With `shutdown: { delay: 5s }`, `/readyz` on the signalled process read `draining` at once
    while the other read `ok`, the port stayed bound through the delay, and
    `shutdown delay elapsed` followed the signal by 5.001 s. That socket closed about 2 s after
    the senders stopped, so its zero loss in all three runs says nothing about loss at a close
    under load.
  - With the default `0s` delay, the only arm that closed under load, the loss was 0, 0, and 85
    datagrams: what reached the socket after its read loop stopped and before it closed, which
    no `logit` counter sees. `logit.input.kernel.drops` counts drops on a full buffer, not a
    queue discarded at close. A delay doesn't shrink that window; it opens when the drain
    starts.
  - With `reuse_port` left off, the second process exited `1` with `Address already in use`.

## Decision
`logit` lets two processes share a listener's port when the operator opts in, keeps serving for a
configured delay after SIGTERM before it drains, and can serve its admin endpoint on a pod-local
Unix socket.

### 1. `reuse_port: true` on every listener that binds over IP
A per-kind field beside `bind:`, default `false`, on `statsd_in`, `lines_in`, `collectd_in`,
`graphite_in`, `syslog_in`, `otlp_in`, `datadog_in`, `datadog_trace_in`, `splunk_hec_in`,
`logit_in`, `prometheus_in` in receiver mode, and `prometheus_out` in exposition mode. It isn't on
`admin:` (decision 4).

When it's set, the listener's socket sets `SO_REUSEPORT` before it binds, and another `logit` (or
anything else) can join the port under the rules in [What the kernel does](#what-the-kernel-does):
the same effective UID, the flag set on both sides, each flow pinned to one member, and about half
the flows re-hashed when a member joins.

A graph rule (rule 80; `crates/logit-pipeline/src/graph.rs`'s rule list is canonical) rejects
`reuse_port: true`:

- under a Unix-socket transport (`transport: unix` or `unix_stream`), where there's no port;
- on a `datadog_trace_in` with no `bind:`, which listens on its Unix socket only;
- on a literal multicast `bind:`, because every member receives every datagram whatever flags
  are set, so there's nothing to share.

`prometheus_in`'s scrape mode and `prometheus_out`'s send mode reject it through their existing
mode rules, as a non-default field of the other mode.

**Mechanism.** A shared `logit_pipeline::listen::bind_tcp` helper builds every TCP listener on
`tokio::net::TcpSocket`: `SO_REUSEADDR` set, `SO_REUSEPORT` when asked, a backlog of 1024, and
the same address resolution as tokio's own `TcpListener::bind`. It replaces the nine direct
`tokio::net::TcpListener::bind` calls, the admin server's among them. The UDP `bind_one` sets
`SO_REUSEPORT` before its bind, and leaves `SO_REUSEADDR` to the multicast path as it does today.
Because `SO_MEMINFO` and `TCP_INFO` are per socket, every listener's
`logit.input.kernel.drops`, `receive_buffer.*`, and `accept_queue.*` gauges keep describing its
own socket inside a group.

### 2. TCP close: keep accepting until the close, and document the sysctl
No code changes how a TCP listener closes. `logit` keeps accepting until the instant it drops the
listener, which is what the drain already does and what decision 3 extends. A listener that keeps
accepting keeps its accept queue near empty: a close while accepting reset nothing at 2000
connections/s in the measurements.

The connections still exposed are the few that complete their handshake between the last accept
and the close. Operators close that with `net.ipv4.tcp_migrate_req=1`, which `logit` documents as
a node-level sysctl. A pod can't set it: under `hostNetwork` the pod shares the node's network
namespace, and a `hostNetwork` pod can't set network sysctls. Without it, the clients in the
queue get a reset, and every stream client `logit` serves reconnects after one.

### 3. `shutdown: { delay }`: keep serving after the first signal
A top-level `shutdown:` block with one field, `delay`, default `0s`, which keeps today's
behavior. On the first SIGTERM or SIGINT:

1. `/readyz` flips to `503 draining` at once, as it does today.
2. The runtime waits `delay`. Every listener stays bound, reading, and accepting, and every sink
   keeps delivering.
3. The runtime logs `shutdown delay elapsed` and starts the existing drain. The listener and sink
   `shutdown_grace` timers start then, as they do today.

A second SIGTERM or SIGINT during the delay exits `130` at once, as it does during a drain. A node
that fails during the delay starts the drain at once. A signal that arrives before the process
ever reported ready skips the delay: a process that was never ready was never in an endpoint set,
so there's no traffic to move away from it, and holding its ports would only slow a failed rollout
down. The `drain complete` line's `duration` excludes the delay, so it keeps measuring the drain
alone.

The delay covers the time the orchestrator needs to stop sending. Kubernetes withdraws a
terminating pod's endpoint without waiting for a probe, so the delay is sized by how long that
withdrawal takes to reach kube-proxy, ingresses, and load balancers; a client that routes on its
own `/readyz` probe adds its probe period times its failure threshold. The orchestrator's
termination grace (`terminationGracePeriodSeconds`, 30 s by default in Kubernetes) must cover the
delay plus the drain, or SIGKILL cuts the drain short.

### 4. `admin:` gains a Unix socket
`admin:` gains `socket: <path>` and `socket_mode:` beside `bind:`. A config can set either, both,
or neither. Both serve the same `/readyz` and `/healthz`.

`logit ready --admin unix:<path>` probes the socket. `logit ready` also reads its target from a
`LOGIT_ADMIN` environment variable, so the image's `HEALTHCHECK` stays `logit ready` and a
deployment points it at the socket without overriding the command.

A pod-local path, such as one on an `emptyDir` volume, never collides between two overlapping
pods, and a probe on it reaches only the pod that owns it. The kubelet can't `httpGet` a Unix
socket, so the Kubernetes recipe uses an `exec` probe running `logit ready`.

### 5. One socket per listener
This decision shares one socket per listener across processes. Several readers on one port inside
one process stays out of scope: that form needs an answer to the single-`Fanout`-owns-shutdown
constraint ([ADR `decoupled-listener-io`](decoupled-listener-io.md)), and this ADR doesn't change
that constraint.

### What an overlap means for the data
During a rolling overlap, two `logit` processes run the same config against the same ports:

- **Flows pin and re-hash.** Each sender's flow reaches one process. When the new pod joins, about
  half the flows move to it, with no loss. When the old pod drains, its flows move to the new one.
- **Loss at close is the closing socket's queue.** For UDP, that's what sat in the old socket's
  receive queue when it closed: near zero for a reader keeping up, one `SO_RCVBUF` for a stalled
  one. For TCP, it's the accept queue under `tcp_migrate_req=0` (decision 2).
- **Summaries split.** A flow that moves mid-window has its `aggregate` window split across two
  processes, and each emits a partial sum for the same series. Summarization is per process; the
  operator accepts that with the overlap. Delivery stays at-least-once, and nothing is lost at the
  hop ([ADR `delivery-semantics`](delivery-semantics.md)).
- **Multicast is duplicated.** Both pods receive every datagram of a multicast `collectd_in`, so a
  downstream count doubles for the overlap. `reuse_port` isn't involved: the multicast bind already
  sets `SO_REUSEADDR`, which lets both pods join.
- **Prometheus exposition is split.** Each scrape connection reaches one process's registry, so
  successive scrapes can alternate between the two, and counters can look reset during the overlap.
- **Long-lived connections stay put.** A `logit_out` connection to `logit_in`, an OTLP/gRPC
  stream, or a keep-alive HTTP connection stays with the old process until its drain closes it.
  The client's reconnect then lands on a surviving member. For the native hop, the new `logit_in`
  holds no marks for that sender, so its in-flight frames are forwarded again, which at-least-once
  permits ([ADR `native-hop-named-acks`](native-hop-named-acks.md)).

### Trust
Only a process with the same effective UID can join a reuseport group, and only when both sides
set the flag. A same-UID neighbour that joins receives a share of the port's traffic. That's a
cooperating neighbour, a non-goal under
[ADR `deployment-threat-model`](deployment-threat-model.md). The defense is that the field is
opt-in and off by default. The same holds for the admin socket: a `hostPath` directory shared
between pods would let a new pod unlink and take over the old pod's socket file. That's documented,
not defended.

## Alternatives considered
- **A pre-close accept sweep on TCP listeners.** Rejected. Accepting everything queued
  immediately before the close moves the reset from the accept queue to a freshly accepted
  connection the process is about to close, and a connection closed with unread data gets a reset
  anyway (measured). The sysctl fixes the queue case in the kernel, where the connections can move.
- **A `preStop` hook as the only answer for the delay.** Rejected as the only answer, kept as a
  documented alternative. A `preStop` sleep needs a `sleep` binary in the image, which distroless
  images don't ship, and does nothing for a host or systemd deployment.
- **Folding the delay into `shutdown_grace`.** Rejected. That grace starts after a socket stops
  being read; reusing it would change what every operator who set it configured.
- **A per-listener delay.** Rejected. Readiness is per process, and so is the window the
  orchestrator needs to move traffic.
- **`admin.reuse_port`.** Rejected. Both pods would answer probes on the shared port, so the
  kubelet couldn't tell which pod it reached, and the rollout gate would be unsound.
- **Omitting `admin:` under `hostNetwork`.** Rejected. It avoids the collision by giving up the
  readiness gate.
- **Fd handover to the new process.** Not chosen here. A container upgrade can't pass fds between
  pods without a shared `hostPath` socket and a handover protocol, and reuseport loses only the old
  socket's queue for a fraction of the surface (the research note's "Fd handover to a new
  process").

## Consequences
- **`logit-pipeline`**: a new `listen` module with `bind_tcp`. Graph rule 80, and rules 55 and 56
  cover `reuse_port` as a field of the other mode. `runtime.rs` gains a `RunOptions` and a
  `run_with_options` entry point that carry the delay, and the shutdown driver waits the delay
  between `readiness.draining()` and telling the nodes, cutting the wait short on a second signal
  or a node failure.
- **`logit-inputs`**: `udp.rs`'s `bind_one` and `tcp.rs`'s stream driver take the flag, and every
  HTTP listener and `logit_in` binds through `bind_tcp`.
- **`logit-outputs`**: `prometheus_out`'s exposition server binds through `bind_tcp`.
- **`logit-config`**: `reuse_port` on the twelve kinds, the top-level `shutdown:` block, and
  `admin.socket`/`admin.socket_mode`; `schema/logit.schema.json` regenerates.
- **`logit-cli`**: an `AdminListener` that serves over TCP, a Unix socket, or both; `logit ready`
  accepts `unix:<path>` and reads `LOGIT_ADMIN`.
- **Docs that change with the behavior**: `docs/deploying.md`'s "Signal and restart behavior"
  and "Probes and exit codes", plus a rolling-overlap recipe with the sysctl;
  `docs/design/pipeline-graph.md`'s cancellation table gains the delay; the lifecycle line
  `shutdown delay elapsed` joins `docs/design/internal-telemetry.md`'s lifecycle events and
  [ADR `tracing-for-self-logging`](tracing-for-self-logging.md)'s set.
- **Known gaps that open**, in [intake gaps](../known-gaps/intake.md): a closing TCP listener
  resets its accept queue unless `net.ipv4.tcp_migrate_req=1`, a multicast `collectd_in` is
  delivered to every overlapping instance, and a closing UDP socket loses its kernel receive queue
  uncounted. "One reader per UDP listener" narrows to the in-process form.
- **Earlier records**:
  [ADR `udp-intake-batching-and-socket-visibility`](udp-intake-batching-and-socket-visibility.md)
  and [ADR `decoupled-listener-io`](decoupled-listener-io.md) keep in-process fan-in out of scope
  and point here for the cross-process form.
  [ADR `admin-readiness-endpoint`](admin-readiness-endpoint.md)'s deferred Unix-socket
  alternative is decided here, and
  [ADR `service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md)'s shutdown
  gains the delay.
- An operator running an overlap accepts split summaries, duplicated multicast, and a split
  Prometheus exposition for its duration, and sizes the termination grace for delay plus drain.
