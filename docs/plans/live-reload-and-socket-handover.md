---
created: 2026-10-05
updated: 2026-10-07
---

# Research note: live reload, socket handover, and what each deployment world needs

**Status: research. Nothing here is decided, scheduled, or a commitment.** This note records what
was learned while weighing SIGHUP handling, so a future design starts from the constraints instead
of rediscovering them. Three items it names are explored separately, each with its own decision:
SIGHUP handling, `SO_REUSEPORT` plus shutdown-time socket draining, and TLS certificate reload.
Nothing in this note pre-empts those.

The question that started it: should `logit` reload its config or its TLS certificates on SIGHUP,
and how much machinery does each take? The answer splits by deployment world, and the split is the
one thing to take away. A host or systemd deployment and a container deployment need different
mechanisms, because the container model replaces the thing an in-process reload or an fd handover
preserves.

## What exists today

- **SIGHUP reopens file targets and never exits.** `crates/logit-cli/src/signals.rs` installs
  the SIGTERM, SIGINT, and SIGHUP handlers before the config loads. A SIGHUP bumps a reopen
  generation that `stdio_out` and `file_out` file targets watch
  ([ADR `signal-handling`](../adr/signal-handling.md)). It doesn't reload the config;
  `docs/known-gaps/runtime.md` records that gap.
- **A SIGTERM drain stops every listener first**, then cascades the close-time flush through the
  graph ([Signal and restart behavior](../deploying.md#signal-and-restart-behavior)). That order is
  right under a supervisor that restarts the process and wrong in Kubernetes without a `preStop`
  delay, where a pod should keep reading its sockets until the Service's endpoints have moved.
- **Certificates load once at construction.** Every TLS listener holds one
  `Arc<rustls::ServerConfig>` built by `logit_inputs::tls::build_server_config` and wraps it in a
  `TlsAcceptor` once. Every TLS sink builds a `rustls::ClientConfig` through
  `logit_outputs::tls::build_client_config`; the HTTP sinks hand theirs to `reqwest` with
  `use_preconfigured_tls`. Only `prometheus_in`'s scrape client takes PEM through `reqwest`'s own
  builders (`apply_client_tls`). `docs/known-gaps/intake.md` has the entry.
- **Listener sockets bind from config, never from an inherited fd.** `udp.rs`'s `bind_one` goes
  through `socket2` and sets `SO_REUSEADDR` only for a multicast bind. `tcp.rs` binds through
  `tokio::net::TcpListener::bind` directly. `unix.rs` unlinks a stale socket path and binds fresh.
  No listener sets `SO_REUSEPORT`.
- **`Input::bind` is a pre-pass.** Every listener's socket opens before any task spawns, so a bind
  failure fails startup with nothing else running. Any handover design has to feed this pre-pass,
  because that's where a second process's bind would collide with the first's.

## Config reload

### The state problem is larger than files and sockets

Handing open sockets to a rebuilt graph is the easy part. What doesn't survive a rebuild is
per-component state, and each item below is its own handover design:

- `aggregate` windows in flight, and `temporality: cumulative` accumulators, whose reset shows
  downstream as a counter reset.
- A sink's in-memory `buffer:` queue, including batches held behind a `Refused` head that can't be
  drained because the destination is down.
- `logit_out`'s sender identity, sequence, and in-flight window; `logit_in`'s per-sender marks.
  A reconnecting sender whose marks were lost gets its in-flight frames forwarded again, which
  at-least-once permits ([ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md)).
- `splunk_hec_in`'s per-channel acknowledgment tables, `prometheus_in`'s remote-write metadata
  cache, `prometheus_out`'s exposition registry, `shape`'s cumulative gauges, and Lua globals.

What does survive is what's on disk: a sink's `buffer.disk:` spool and `tail_in`/`docker_in`
checkpoints.

### Three shapes, from hardest to cheapest

1. **Diff the resolved graph and swap the changed nodes.** Every node owns its inbox and its
   `Fanout`, so replacing one node or one edge means rewiring channels inside the runtime core that
   the drain and shutdown-accounting work hardened
   ([ADR `shutdown-accounting-and-cancellation-safety`](../adr/shutdown-accounting-and-cancellation-safety.md)).
   Its only benefit over shape 2 is preserving the state above for the nodes a diff leaves alone.
   This is the shape the known-gaps entry assumed, and it's the one that's harder than it looks.
2. **Treat SIGHUP as a SIGTERM that doesn't exit.** Load and fully resolve the new config. If it
   fails, log and keep running. If it passes, start the new graph with the listener sockets handed
   over inside the process, then stop the old listeners, which cascades the existing close-time
   flush through the old graph while the new one serves. Every loss is one a SIGTERM already
   incurs and the docs already describe. The new pieces are in-process socket handover and two
   graphs alive for a few seconds. No per-component state handover at all.
3. **Hand sockets to a new process**, the HAProxy and nginx shape, described next. Same losses as
   shape 2, plus free rollback and binary upgrades, at the cost of process management that differs
   per deployment world.

### Fd handover to a new process

The old process forks and execs the binary it was started from. The child inherits every listener
socket, validates its config, binds from the inherited fds, builds its whole graph, and reports
ready. Only then does the old process stop its listeners and drain. If the child fails at any
step, the old process never stops. Rollback is free, which no in-process design gets, and the same
path serves a config change, a certificate rotation, and a new binary at the same path.

Points specific to `logit` that the HAProxy model doesn't cover on its own:

- **Exclusive resources need a serial handoff, not a shared one.** Sockets can overlap: two
  processes reading one UDP socket is fine, and a shared TCP listener's accept queue continues. A
  disk spool, a tail checkpoint, and a Unix socket path can't overlap; two processes tailing one
  file both advance and the overlap double-reads. So the protocol is two-phase. The child takes
  the sockets at once, and takes the file-backed inputs and spooled sinks only after the parent
  reports it has stopped them and written their checkpoints. During that gap the child's sinks
  absorb into their memory `buffer:`. This ordering is the one new design question.
- **Multicast membership and Unix sockets carry over.** `collectd_in`'s group membership is per
  socket, so the fd carries it. A Unix socket fd works too, as long as the child doesn't unlink
  and rebind the path when handed an inherited one, which `unix.rs` does today.
- **Match inherited fds by `getsockname` and socket type**, not by position or component name, so
  a reload that renames or reorders listeners still maps, and a listener the new config dropped is
  closed by the parent at drain.
- **The `LISTEN_FDS` convention fits.** Plain inheritance with `CLOEXEC` cleared needs no
  `SCM_RIGHTS` and adds no raw-`libc` site to the three
  [ADR `out-of-ci-unsafe-verification`](../adr/out-of-ci-unsafe-verification.md) covers, and
  systemd socket activation then works unchanged.
- **Overlap doubles memory.** Two Lua VM sets, two `aggregate` states, two sets of sink queues.
  `max_memory` caps each Lua VM separately, and nothing caps the process total across both graphs.
- **The main PID changes.** Under systemd that's `Type=notify` with a `MAINPID=` update, the
  standard pattern for a service that re-execs. In a container the parent is PID 1 and its exit
  ends the container, which is where this shape stops working as described. See [The two worlds](#the-two-worlds).

Why fd passing rather than `SO_REUSEPORT` for the handover: with reuseport, a closing TCP listener
resets whatever is in its accept queue, and a closing UDP socket drops whatever is in its receive
queue. A shared fd has neither problem. HAProxy added its `-x` fd transfer for the TCP case.

## The two worlds

| | Host or systemd | Docker or Kubernetes |
|---|---|---|
| What a new binary is | the same path, re-executed | a new image, so a new container, PID namespace, filesystem, and usually network namespace |
| Can fds cross to the new version? | yes, by inheritance across `exec` | no, except `SCM_RIGHTS` over a Unix socket on a shared `hostPath` between two pods in one network namespace |
| Can the old process drain and exit? | yes; `Type=notify` and `MAINPID=` follow the swap | no; PID 1 exiting ends the container, so a drain needs a supervisor process that outlives the worker |
| How config changes arrive | a file edit and a signal | a ConfigMap volume update with no signal: an atomic symlink swap, up to about a minute late, and never under a `subPath` mount |
| How certificates rotate | a certbot deploy hook, which can signal | cert-manager rewrites a mounted Secret with no signal |
| Who can send a signal | the operator or the supervisor | `docker kill -s HUP`, `kubectl exec`, or a sidecar only with `shareProcessNamespace: true` |
| How upgrades usually happen | in place | pod replacement; a config change is commonly a checksum annotation that rolls the pods, so config reload becomes the upgrade case |
| What closes the gap on a UDP port | fd handover to the new process | overlap: a `hostNetwork` DaemonSet with `maxSurge: 1` and `SO_REUSEPORT` on both pods |

### Host and systemd

The fd-handover shape fits as described. systemd socket activation is the same mechanism with
systemd as the parent, and one `LISTEN_FDS` reader serves both.

### Docker and Kubernetes

- **Binary upgrades can't use handover.** The gap is managed by overlap, not by passing anything.
- **Config reload inside one container forces a supervisor.** Either the old process drains and
  then execs itself into a supervisor that only reaps and forwards signals, or `logit` runs
  HAProxy's master-worker shape from the start. Both add the process-management surface that
  makes shape 3 expensive. Most Kubernetes deployments avoid the question by rolling the pods on
  a config checksum.
- **The upgrade case needs overlap on the port.** For a node-local statsd or syslog agent as a
  DaemonSet with `hostNetwork: true`, old and new pods share the node's network namespace. With
  `maxSurge: 1` the new pod starts before the old stops, and it can bind the same UDP port only
  with `SO_REUSEPORT` set on both. The kernel then splits datagrams across both sockets, and the
  loss at the old pod's close is whatever sat in its receive queue, near zero if it was keeping up.
  Linux requires the same effective UID on every socket in a reuseport group. For TCP, a closing
  listener's accept queue is reset unless the kernel migrates it (`net.ipv4.tcp_migrate_req`,
  Linux 5.14 and later).
- **HAProxy's `-x` applies, with conditions.** Fds are kernel objects, so a new pod can receive
  the old pod's listener fds over `SCM_RIGHTS` on a Unix socket in a `hostPath` directory, across
  containers and PID namespaces. It's meaningful only when both pods share a network namespace, so
  again `hostNetwork`. It's the zero-gap version of the DaemonSet case. Reuseport loses only the
  old socket's receive queue and takes a fraction of the surface.
- **Without `hostNetwork`, nothing in-process is the bottleneck.** Behind a ClusterIP Service, UDP
  flows pin to a backend through conntrack. The gap is at the Service layer: endpoints update,
  stale conntrack entries are flushed, and a `preStop` sleep keeps the old pod serving until the
  endpoints have propagated. What `logit` must do right is keep reading its sockets through the
  grace period rather than closing them the instant SIGTERM arrives. Today it closes them first,
  and that works in Kubernetes only because `preStop` delays the signal.

## TLS certificate reload

Decided in [ADR `tls-certificate-reload`](../adr/tls-certificate-reload.md), which settles the
points below differently in two places: the poll compares file content rather than file identity,
and `prometheus_in`'s scrape client moves onto a shared rustls config instead of being rebuilt
behind a swap. The rest of this section is the research as it stood.

Smaller than either reload shape and independent of both.

- **Server side has a seam.** `rustls::ServerConfig`'s `cert_resolver` is an
  `Arc<dyn ResolvesServerCert>`. A resolver holding a swappable `CertifiedKey` lets the config and
  the `TlsAcceptor` stay as built: open connections keep their session, new handshakes get the new
  certificate, and rotation is one atomic store. The client-CA verifier (`WebPkiClientVerifier`) is
  baked into the config and needs the same wrapper treatment to rotate.
- **Client side has a seam, except the scrape client.** The pooled `hyper-rustls` and
  `tokio-rustls` connectors, and the HTTP sinks' preconfigured `reqwest` clients, each hold an
  `Arc<ClientConfig>` with `client_auth_cert_resolver` and a replaceable server-cert verifier.
  Only `prometheus_in`'s scrape client, which loads PEM through `reqwest`, needs the client rebuilt
  behind a swap. A renewed client certificate affects new handshakes only.
- **Trigger on a file poll, not only a signal.** cert-manager and a mounted Secret signal no one.
  A poll on the cert and key files' identity (mtime, size, inode) with SIGHUP as an extra trigger
  covers both worlds.
- **Load the whole pair before swapping, and keep the old one on any failure.** A cert and key
  written separately can be observed mid-rotation.

## What the research suggests, without deciding it

- SIGHUP needed a handler regardless of any reload semantics, because without one it was an
  undrained kill under systemd. Decided in [ADR `signal-handling`](../adr/signal-handling.md):
  SIGHUP reopens file targets and never exits.
- `SO_REUSEPORT` is needed in the container world whichever reload path is taken or not taken, and
  comes with two open questions: the TCP accept-queue behavior at close, and whether a SIGTERM
  drain should keep reading sockets for a grace period before closing them. Explored separately.
- TLS reload has a clear operator need (90-day certificates) and a contained design. Decided in
  [ADR `tls-certificate-reload`](../adr/tls-certificate-reload.md).
- Config reload stays a known gap. If it's built, the shapes worth building are 2 or 3, not the
  diff. Shape 3 is the robust one for hosts and systemd and doesn't translate to containers
  without a supervisor process. A shared "acquire listeners from a fresh bind, an inherited set,
  or a handover socket" seam in the bind pre-pass would let the host and container mechanisms share
  code without committing to the supervisor.

## Related

- [ADR `service-lifecycle-and-output-retry`](../adr/service-lifecycle-and-output-retry.md) for the
  drain this note builds on.
- [ADR `otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md) and
  [ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md) for where certificates
  load today.
- [ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)
  for the UDP read path a shared socket would feed.
- [ADR `deployment-threat-model`](../adr/deployment-threat-model.md): every listener is on a
  private network, which is what makes a reuseport group or a handover socket a question about a
  cooperating neighbor rather than a hostile one.
- [Operator surface plan](operator-surface.md), which set config reload aside as needing its own
  design.
