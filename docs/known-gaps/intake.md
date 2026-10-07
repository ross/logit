# Known gaps: UDP intake, TLS, and connection lifecycle

Entry format and the other areas: [the known-gaps index](README.md).

## UDP intake

- **A UDP listener's read and decode loops share one task.** `read_loop` and `decode_loop`
  (`crates/logit-inputs/src/udp.rs`) run under `UdpListener::drive`'s one two-arm `select!`, so
  they interleave, yielding to each other on the coop budget, but never run on two cores at once.
  The sharing costs nothing measurable
  ([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)'s
  "The coop-budget question a batched read raises").

  A report-only experiment that spawned `decode_loop` onto its own task, pinned to cores 2, 3, 14,
  and 15 (two fast physical cores plus their SMT siblings), found headroom in splitting them.
  These numbers are laptop-provisional: the experiment branch was never rebuilt on the reference
  VM, which would need `script/vm build` from a local directory source.

  | | single task | decode spawned |
  |---|---|---|
  | `udp-statsd-small` CPU µs/event | 1.229 | 1.082 (−12%) |
  | `udp-statsd-small` peak RSS | 22.0 MiB | 38.0 MiB |
  | `udp-statsd` CPU µs/event | 0.659 | 0.696 (+5.6%) |
  | `udp-statsd` kernel drop % | 0.69 | 0.00 |
  | `udp-statsd` mean fill | 22.8 | 2.6 |
  | `udp-statsd` peak RSS | 86.4 MiB | 264.8 MiB |

  - **Benefit:** splitting took `udp-statsd`'s kernel drops to zero and its mean fill from ~23 to
    ~2.6, because the reader no longer waits behind decode and keeps the socket drained. It also
    cut `udp-statsd-small`'s CPU/event by ~12%.
  - **Cost:** +5.6% CPU/event on `udp-statsd` (the cross-core handoff) and roughly 3× peak RSS,
    because nothing paces the reader against the decoder once they stop sharing a poll budget.

  Shipping it needs four things designed together:
  1. A join-handle-plus-cancellation story to replace the two-arm `select!`, which carries the
     shutdown and drain ordering: read finishing closes the queue, which lets decode discover
     closed-and-empty and flush its accumulator.
  2. Moving the decoder out of `&mut self` so it can live on a `'static` task (`D: 'static`).
  3. A `Fanout` ownership answer: dropping the decode future is what closes every downstream inbox
     today, and a caller can't drop it directly once it's on a task.
  4. `receive.max_bytes`'s default, revisited against real measurements, because nothing bounds
     the reader once it's decoupled from decode's pace.

  This overlaps heavily with "One reader per UDP listener" (next entry), which needs answers to the
  same shutdown-cascade and `Fanout`-ownership questions for N readers, each with its own `Fanout`
  clone. Design the two together.

- **One reader per UDP listener.** A single read loop is one core's worth of read capacity.
  `SO_REUSEPORT` lets several sockets share one port, with the kernel load-balancing datagrams
  across them: gostatsd's `--max-readers` (default `min(8, NumCPU)`), rsyslog's per-listener thread
  count (capped at 32). `logit` builds the cross-process form only: `reuse_port: true` lets two
  `logit` processes, one socket each, share a port for a rolling overlap
  ([ADR `listener-port-sharing-and-shutdown-delay`](../adr/listener-port-sharing-and-shutdown-delay.md)).
  Several readers on one port inside one process is still not built.
  - The batched `recvmmsg(2)` read raised the single-reader ceiling
    ([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)'s
    sweep), so whether one reader is still the bottleneck is now measurable rather than assumed.
  - N readers, each holding its own `Fanout` clone, need their own answer to the cancel-by-drop
    shutdown cascade
    ([ADR `service-lifecycle-and-output-retry`](../adr/service-lifecycle-and-output-retry.md)),
    which assumes one `Fanout` per listener. The same work would settle "A UDP listener's read and
    decode loops share one task" (previous entry).
- **UDP reads batch only on Linux.** On Linux, `read_loop` takes up to `receive.read_batch`
  datagrams per `recvmmsg(2)` call. Other targets keep one `recv_from` per datagram behind the
  same `BatchReader` interface (`crates/logit-inputs/src/udp.rs`), because `recvmmsg` has no
  portable equivalent worth a second implementation, and `logit` ships Linux containers only. On
  those targets, `read_batch` still validates and sizes the decode half's `pop_many`, but the read
  half ignores it, and `logit.input.datagrams.truncated` stays at zero.
- **No runtime `recvmmsg(2)` → `recvmsg(2)` fallback when a sandbox blocks the syscall.** On Linux
  a UDP listener always calls `recvmmsg(2)`. A seccomp profile (or an LSM) that refuses it returns
  `ENOSYS`/`EPERM` on the first call, which `read_loop` treats as fatal: the listener fails and the
  process exits with the runtime-failure code (`2`, not the bind-time `1`, because the socket bound
  fine). The error (`describe_read_failure`) names the syscall and the bound socket, and says that
  `receive.read_batch: 1` won't help.
  - quinn hit the same wall on Android x86 (quinn#1947), and bun hit a worse one, where the
    refusal produced no datagrams and a 100% CPU spin (bun#42678).
  - Not built: bun's and quinn's other half, a one-shot `AtomicBool` latch that on the first
    `ENOSYS`/`EPERM` falls back to per-datagram `recvmsg(2)` for the life of the process
    (quinn#2079's pattern for its own `sendmsg` `EINVAL` fallback). That means carrying a second
    Linux read path forever for an environment `logit` has never been reported to run in. A
    listener silently running `read_batch` times slower than configured may also be worse than one
    that refuses to start.
  - **Revisit trigger:** a real deployment that asks.
- **`received_at`'s strict ordering survives a wall-clock *step* only within one read batch.**
  `now_nanos()` is `SystemTime::now()` because `received_at` is the event's wall-clock timestamp,
  which a monotonic instant can't be. Within a batch, ordering doesn't depend on the clock: one
  read gives `base`, and datagram `i` gets `base + i`. Across batches, it does.
  - **Consequence:** a backwards `clock_settime` between two batches (chrony's `makestep`, an NTP
    correction after a long outage, a VM suspend/restore or live migration) can move the clock back
    far more than the `≤ read_batch` nanoseconds of offset. Two datagrams in consecutive batches
    can then share a `received_at`: the `(series, timestamp)` collision the `+ i` offset exists to
    prevent (ADR `udp-intake-batching-and-socket-visibility`'s "One `received_at` per syscall
    batch"). The damage is bounded: `decode_loop`'s latency computation clamps with `.max(0)`, and
    `influxdb_out`'s `allocate_timestamp` disambiguates within an output batch.
  - The listener can't close this, because the fix, a monotonic clock, would be the wrong
    timestamp. `every_datagram_in_a_batch_gets_its_own_received_at`'s doc comment states the same
    distinction next to the assertion.
- **`ReceiveBufferSampler` gauges a descriptor it captured at construction, rather than one taken
  from the socket at each sample.** Nothing is wrong today, but the compiler isn't asked to prove
  it. The TCP twin, `AcceptQueueSampler`, reads the fd off the `listener` argument it's handed, so
  the gauged socket and the accepting socket are the same by construction
  ([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)'s
  "Identity, not lifetime, was the fd risk").
  - The UDP sampler can't get the same treatment without changing `sample_while`'s signature,
    because that function holds the sampler and the read future, not the socket.
  - The lifetime is enforced indirectly: the combined future carries `&socket` through its sibling
    `read_loop` arm, so the borrow checker won't let it outlive the socket, and there's one
    `ReceiveBufferSampler` per `read_loop_sampled` per socket, with no way to reach a second.
  - It's left as is because `sample_while`'s arm ordering matters (its doc says why), and a
    signature change risks it.
- **A UDP sink's send failures are not counted by cause.** `statsd_out`, `syslog_out`,
  `graphite_out`, and `collectd_out` count an `EMSGSIZE` refusal (a datagram past the path MTU: a
  configuration problem) as `logit.output.messages.dropped{reason="oversize_datagram"}`, but end
  the batch alike on every other failed send. An operator can't tell `ENOBUFS` (local
  socket-buffer pressure: a tuning problem) from `ECONNREFUSED` (an ICMP port-unreachable from a
  missing receiver: a deployment problem).
  - **To close:** add a `logit.output.send.errors{errno="..."}` count at the one send site the
    four share (`send_one` in `crates/logit-outputs/src/datagram.rs`), with the errno set bounded
    by the handful a UDP `sendmsg` can return. Only the call site can see the errno.
  - The receive side's kernel counters have no useful send-side twin. `SO_MEMINFO`'s `wmem_alloc`
    is ~always 0 on a UDP socket, because a datagram is charged and uncharged inside one
    `sendmsg`, so a send-buffer gauge would be a flat zero. `SockMeminfo` carries the field only
    because the option returns it.
- **A cancelled datagram send loses the counts of what it already sent.** A UDP sink, or
  `statsd_out` under `transport: unix`, counts `logit.output.messages` and
  `logit.output.datagrams` (and `graphite_out`'s `datapoints`) once `send_datagrams` returns, on
  success and failure alike. A `send` dropped mid-batch, by the shutdown grace, never returns, so
  the datagrams it already handed the kernel are never counted
  (`crates/logit-outputs/src/datagram.rs`'s module doc). `logit.component.errors` records the
  cancelled attempt.
  - **To close:** count each datagram as it goes, at a telemetry call per datagram.
- **A UDP sink reaches only the IPv4 address of a name that resolves to both families.** Each
  UDP sink (`statsd_out`, `syslog_out`, `graphite_out`, `collectd_out`) sends to the first IPv4
  address its endpoint resolves to, and falls back to the first IPv6 one only when there's no
  IPv4 address (`pick_addr`). Binding by the first resolved address instead would turn a loud
  failure into silent loss where `localhost` resolves to `::1` first and the receiver listens on
  `127.0.0.1` only.
  - **Consequence:** a receiver that listens on IPv6 only, behind a name that also has an IPv4
    address, gets nothing, and the send reports `ok`.
  - **Workaround:** write the IPv6 address in `endpoint:`, for example `[::1]:8125`.
  - **Revisit trigger:** an operator who needs the IPv6 address preferred
    ([ADR `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md),
    decision 9).
- **The four IPv6 UDP sink tests skip where IPv6 loopback is unavailable.**
  `statsd::tests::an_ipv6_udp_endpoint_is_delivered` and its `syslog`, `graphite`, and `collectd`
  twins each bind a collector on `[::1]:0`, and print a reason and return when they can't. The dev
  container has IPv6 loopback, so `script/test` and CI run them, but a host without it shows four
  passes that tested nothing. The datagram module's
  `resolution_selects_the_socket_of_the_chosen_address_family` never skips and covers the family
  choice.
- **Netns-wide UDP counters (`/proc/net/snmp`, `netstat -su`) are not collected.**
  `Udp: InErrors` / `RcvbufErrors` / `NoPorts` and the `UdpLite` block answer questions the
  per-socket counters can't, most usefully `NoPorts`: datagrams for a port nothing listens on,
  which is what a misconfigured sender looks like from the receiver.
  - They're totals for the whole network namespace, every process and socket in it, but `logit`'s
    telemetry is per component (`design/internal-telemetry.md`: every point carries the
    `component`/`kind`/`role` of what recorded it). Publishing a namespace-wide number under one
    listener's identity would mislead in the deployments where it matters, such as a host agent
    sharing a netns with everything else on the box.
  - If wanted, they belong in a process-level scope beside `logit.process.*`, which `internal`
    already samples, not on any listener.
- **Events a UDP listener has already decoded are lost uncounted when the grace backstop drops
  it.** Every datagram reconciles at shutdown: `logit.input.datagrams` equals the
  `receive.latency` sample count plus `datagrams.dropped` under every reason, `shutdown` included
  ([ADR `shutdown-accounting-and-cancellation-safety`](../adr/shutdown-accounting-and-cancellation-safety.md),
  decision 1). Past that point the unit is events, and two event-level losses stay uncounted when
  `run_input`'s backstop drops a listener still draining after `receive.shutdown_grace`:
  - The events in the `BatchAccumulator` and the batch parked in `emit`'s `Fanout::send`. Their
    datagrams already count as decoded, so the datagram contract still holds, but no event-level
    counter records them.
  - A batch cut off partway through `Fanout::deliver`. It sends to each consumer in turn, so a
    drop mid-fan-out reaches a prefix of the consumers. The batch still counts as `batches.sent`
    and `receive.flushed`, never as a drop, and the consumers after the prefix never see it.
    `a_batch_cut_off_mid_fan_out_reaches_a_prefix_of_consumers`
    (`crates/logit-inputs/src/udp.rs`) pins this.

  Both need a downstream that stays full for the whole grace (5 s by default). Counting them would
  need an event-level drop counter on the accumulator and a per-consumer delivery record in
  `Fanout`, for a loss the grace already bounds.
- **A multicast `collectd_in` is delivered to every overlapping instance.** Every socket joined to
  a multicast group receives every datagram, whatever `SO_REUSEADDR` or `SO_REUSEPORT` it sets
  (measured: 1000 of 1000 at each of two members), and the multicast bind already sets
  `SO_REUSEADDR`, so two overlapping `logit` instances both bind and both receive.
  - **Consequence:** during a rolling overlap, every value list reaches both instances, and a
    downstream count or sum doubles until the old one exits.
  - **Workaround:** none beyond not overlapping multicast listeners: stop the old instance before
    the new one starts. `reuse_port` is refused on a multicast `bind:`, because there's nothing for
    it to share
    ([ADR `listener-port-sharing-and-shutdown-delay`](../adr/listener-port-sharing-and-shutdown-delay.md)).

## TLS and connection lifecycle

- **Every TLS-capable component's certificates are loaded once at startup; rotation needs a
  restart.** All of them read every PEM file at construction (`logit run` startup):
  - Every TLS listener (`statsd_in`, `graphite_in`, `syslog_in`, `otlp_in`, `datadog_in`,
    `datadog_trace_in`, `splunk_hec_in`, `logit_in`, and `prometheus_in`'s `bind_tls:`) builds its
    `rustls::ServerConfig` through `crates/logit-inputs/src/tls.rs::build_server_config`.
  - Every TLS sink builds its `rustls::ClientConfig` through
    `crates/logit-outputs/src/tls.rs::build_client_config`.
  - `prometheus_in`'s scrape client applies `scrape_tls:` to its `reqwest` client through
    `apply_client_tls`.

  A renewed certificate (a 90-day Let's Encrypt cert, a `cert-manager`-issued one) has no effect
  until restart ([ADR `otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md),
  [ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)).
  - **To close:** `rustls::ServerConfig`'s `ResolvesServerCert` (a file-watcher hook) on the server
    side and an equivalent reload on the client side, behind a SIGHUP or a poll.
- **No TLS client has a `server_name` override.** Every TLS sink (`otlp_out`, `datadog_out`,
  `datadog_trace_out`, `splunk_hec_out`, `prometheus_out`'s remote-write sender, `logit_out`,
  `syslog_out`, `statsd_out`) and `prometheus_in`'s scrape client check the peer's certificate
  against the configured endpoint's own host. An endpoint reached by IP, or through a proxy whose
  certificate names something else, can't be verified by name. OTel's equivalent knob is
  `tls.server_name_override`.
  - **To close:** it's cheap to add
    ([ADR `otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md),
    [ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)):
    `hyper-rustls`'s `HttpsConnectorBuilder::with_server_name_resolver` for `otlp_out`'s gRPC
    transport, an equivalent `reqwest` override for the HTTP sinks and `prometheus_in`, and a plain
    `ServerName` override ahead of `host_only(endpoint)` for the raw-TCP sinks (`logit_out`, and
    `syslog_out`/`statsd_out` through `crates/logit-outputs/src/stream.rs`).
- **A write-only TLS sink (`syslog_out`, `statsd_out` over TCP) cannot observe a peer's
  post-handshake rejection.** Under TLS 1.3, the server sends its whole handshake flight,
  `Finished` included, before it sees the client's certificate. So a client-cert rejection (a
  `client_ca_file`-requiring collector, no matching cert presented) arrives as an alert after
  `TlsConnector::connect` has already succeeded.
  - **Consequence:** these sinks flush before reporting a batch delivered
    ([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)), but a flush only
    proves the bytes left this process, not that the peer accepted them, and the sink never reads
    the alert. `send` reports the batch delivered, which is why
    `syslog::tests::tls_tcp_without_a_client_certificate_delivers_nothing_to_a_client_ca_requiring_collector`
    asserts at the collector.
  - **Unaffected:** `logit_out`, which reports a batch delivered only on its `Ack`, and a
    *server*-certificate rejection, which happens inside the client's own handshake, before any
    write, and always surfaces as `Fault::Clean` (`crates/logit-cli/tests/syslog_round_trip.rs`'s
    `mod tls`).
  - **To close:** the client-cert case needs the sink to read and interpret TLS alerts (or
    application-level acks) it never looks at.
- **A peer's FIN arriving during a write on a pooled connection is lost silently on the plaintext
  stream sinks.** `syslog_out`, `statsd_out`, and `graphite_out` probe a reused pooled connection
  before writing (`crate::tls::poll_pending_close`). That catches a peer that closed while the
  connection sat idle, but not a FIN that arrives between the probe and the write. Their wire
  protocols give the sender no way to learn the write failed, so that batch is an unclassified
  loss. `logit_out` classifies the same race `Fault::Ambiguous` once the ack wait meets it, and
  `logit_in` deduplicates its resend.
- **A hyper listener's `idle_timeout` resets on request completion, not on bytes.** `hyper` owns
  the bytes on every HTTP listener (`otlp_in`, `prometheus_in`'s remote-write receiver,
  `datadog_in`, `datadog_trace_in`, `splunk_hec_in`), so `crate::http::drive_with_idle` sees only
  requests starting and finishing.
  - **Consequence:** a request head that dribbles in more slowly than `idle_timeout` on an
    otherwise-quiet keep-alive connection is closed. That's a documented cost, not a bug
    (`crates/logit-inputs/src/otlp.rs`'s module doc, "Idle timeout";
    [ADR `idle-connection-timeout`](../adr/idle-connection-timeout.md)).
  - A request whose head arrives at the idle deadline is served to completion inside the bounded
    grace (`graceful_shutdown`, then polling for up to `handshake_timeout`), and the grace runs
    again so the response reaches the wire, because dropping it mid-flight would discard a batch
    already handed to `Fanout::send`. The cost is at most a reconnect for the next request, never a
    lost response or batch.
- **A stalled or dribbled body holds a connection permit, without bound unless `idle_timeout` is
  set.** A body read's only time bound is a per-frame stall bound (per `read` on `logit_in`), and
  that bound is the listener's `idle_timeout`, which is off by default.
  - With `idle_timeout` unset, a peer that stops sending mid-body, or sends one byte at a time,
    holds its request and its connection permit indefinitely.
  - With it set, a peer that sends one byte per frame, each slightly under `idle_timeout`, holds
    them for up to `MAX_REQUEST_BYTES × idle_timeout` per request on an HTTP listener (`otlp_in`,
    `prometheus_in`'s remote-write receiver, `datadog_in`, `datadog_trace_in`, `splunk_hec_in`),
    and up to `max_frame_bytes × idle_timeout` per frame on `logit_in`.
  - With enough connections, such a peer can hold the connection cap (`max_connections`).

  This is a cost of the per-frame design, and a non-goal under
  [ADR `deployment-threat-model`](../adr/deployment-threat-model.md): a total body deadline was
  declined because a slow link sending a large legitimate body looks the same
  ([ADR `untrusted-input-bounds`](../adr/untrusted-input-bounds.md),
  [ADR `idle-connection-timeout`](../adr/idle-connection-timeout.md)'s 2026-09-25 amendments).
  - **Revisit trigger:** a listener exposed to untrusted networks, where a total deadline, a
    minimum transfer rate, or a per-peer connection cap is worth the false positives.
- **`logit_in`'s `idle_timeout` bounds reads only; a blocked write is bounded by
  `handshake_timeout`.** `idle_timeout` can't reach a write that a peer has stopped reading, so
  `logit_in` writes every `HelloAck`, `Ack`, and `Reject` (`GOING_AWAY` included) within
  `handshake_timeout` instead (`crates/logit-inputs/src/logit.rs`'s module doc, "Bounded, flushed
  writes"). A peer that sends frames but never reads its `Ack`s is disconnected once the listener's
  send buffer fills and one `Ack` write stalls for `handshake_timeout`
  (`logit.proto.errors{reason="ack_write_stalled"}`).
  - **Consequence:** one knob covers two waits. An operator who raises `handshake_timeout` for
    slow TLS handshakes also lengthens how long a wedged peer holds its connection slot.
  - A conforming `logit_out` never trips the bound: it keeps at most its window of frames in
    flight, so at most that many unread `Ack`s sit in its receive buffer, even while it's paused.
    `logit_in` writes one `Ack` per run of frames, so the common case is far fewer. The worst
    case, one `Ack` per frame, is about 80 KB at the 1024-frame window cap under TLS (a named
    `Ack` is about 70 to 80 bytes there), which fits the default `tcp_rmem`.
  - **Revisit trigger:** an operator who needs the two waits set apart.
- **No per-listener in-flight byte budget on the HTTP listeners.** Each hyper listener
  (`otlp_in`, `prometheus_in`'s remote-write receiver, `datadog_in`, `datadog_trace_in`,
  `splunk_hec_in`) caps concurrent connections and, per connection, concurrent streams
  (`crate::http::MAX_CONCURRENT_STREAMS`, hyper's default of 200, pinned). Its worst case is
  `max_connections × MAX_CONCURRENT_STREAMS × 2 × MAX_REQUEST_BYTES`: 1024 × 200 × 2 × 4 MiB =
  1.6 TiB for `otlp_in` at the default `max_connections` of 1024. The stream cap bounds one factor
  of that product, not the product.
  - A budget over the bytes held in request bodies across a listener (a semaphore acquired per
    body chunk) would bound the product directly. It isn't built: it changes how every HTTP
    listener reads a body
    ([ADR `untrusted-input-bounds`](../adr/untrusted-input-bounds.md)'s "Alternatives considered"),
    and the concurrent large requests it guards against are a non-goal under
    [ADR `deployment-threat-model`](../adr/deployment-threat-model.md).
  - **Revisit trigger:** a public listener, or an operator seeing memory pressure from concurrent
    large requests.
- **A closing TCP listener resets its accept queue unless `net.ipv4.tcp_migrate_req=1`.** When a
  stream listener under `reuse_port: true` closes, the kernel resets every connection sitting in
  its accept queue, connections whose client already saw `connect()` succeed and may have sent
  data. With `net.ipv4.tcp_migrate_req=1` (Linux 5.14 and later) the kernel moves them to a
  surviving member of the group instead (measured: resets equal to the queue length, up to a full
  backlog of 128, under `=0`; zero in 12 runs under `=1`). `logit` keeps accepting until the
  instant it closes, so its queue is near empty and the exposure is the few connections that
  complete a handshake in between
  ([ADR `listener-port-sharing-and-shutdown-delay`](../adr/listener-port-sharing-and-shutdown-delay.md),
  decision 2).
  - **Consequence:** a client whose connection was reset reconnects and resends; a line it had sent
    into the reset connection is lost unless its protocol acknowledges.
  - **Workaround:** set `net.ipv4.tcp_migrate_req=1` on the node. A pod can't set it: under
    `hostNetwork` it's the node's own network namespace, and a `hostNetwork` pod can't set network
    sysctls.
- **A request handler blocked forever in a `Fanout` send holds its connection and permit.** A
  handler parked on a full downstream is backpressure, not idleness, so neither `idle_timeout` nor
  the grace after it closes the connection. It ends when the send completes or the client goes
  away ([ADR `idle-connection-timeout`](../adr/idle-connection-timeout.md)'s 2026-09-25 amendments).
  Closing it would drop a batch that never reached the fanout.
- **`proxy_protocol:` accepts a PROXY header from any peer.** A stream listener under
  `proxy_protocol: true` has no allowlist of trusted proxy addresses, so a client that reaches the
  port directly can write its own header and name any address as `client.address`
  ([ADR `listener-peer-address`](../adr/listener-peer-address.md)).
  - **Workaround:** make the port reachable only through the proxy, with network policy or a
    firewall rule.
  - **Revisit trigger:** a deployment where the proxy and direct clients share a network path to
    the listener. The fix is a list of trusted source addresses, with a header from any other peer
    refused.
  - **Forwarding headers:** an HTTP listener's `forwarded:` has the same exposure for a client
    that writes its own forwarding header, recorded as a non-goal in
    [transform gaps](transforms.md)' "`forwarded:` trusts the header it names" entry; the
    trusted-source allowlist above stays deferred work, not part of that non-goal.
