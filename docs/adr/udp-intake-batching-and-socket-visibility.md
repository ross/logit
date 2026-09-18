---
created: 2026-09-18
updated: 2026-09-18
---

# UDP intake batching and socket visibility

## Status
Accepted

## Context

[ADR `decoupled-listener-io`](decoupled-listener-io.md) closed the event-level half of UDP intake:
`UdpListener<D>` (`crates/logit-inputs/src/udp.rs`) races a `read_loop` against a `decode_loop`
through a bounded `ReceiveQueue`, and `BatchAccumulator` amortizes decoded events into one
`Fanout::send` per ≤1000 events instead of one per datagram. What that ADR deliberately left
untouched is the *syscall and bookkeeping* side underneath it, shared by all four UDP inputs
(`statsd_in`, `syslog_in`, `graphite_in`, `collectd_in`) through that same `udp.rs`:

- **One `recv_from` per datagram** (`docs/known-gaps.md:185-191`) — syscall overhead is now the
  read half's dominant remaining cost, since a stalled downstream no longer stops it running.
- **Three mutex-locked gauge updates per push *and* per pop** (`docs/known-gaps.md:200-214`) —
  `BoundedQueue::push`/`pop` (`crates/logit-pipeline/src/queue.rs:237,330`) call `update_gauges`
  unconditionally on every accepted item, and `read_loop` (pushing) and `decode_loop` (popping) run
  concurrently against the identical lock.
- **No visibility into kernel-side drops** (`docs/known-gaps.md:176-184`) — a datagram the kernel
  discards before `recv_from` ever returns it is invisible to `logit` entirely.
- **No perf scenario touches a real socket** — every scenario under `perf/scenarios/*.yaml`
  ([ADR `load-test-harness`](load-test-harness.md)) drives its listener through `generate_in`
  in-process; nothing exercises a UDP socket, so none of the above can be measured today.

This ADR is the decision record for closing all four, plus the load-test-harness extension needed
to measure the result. [`docs/plans/udp-intake.md`](../plans/udp-intake.md) is the workstream
schedule and baseline/delta protocol this record is built on; this document is about the decisions,
not the sequencing.

**This revises [ADR `decoupled-listener-io`](decoupled-listener-io.md)'s "Alternatives considered"
section**, specifically its `recvmmsg`/`SO_REUSEPORT` bullet and its kernel-drop-counter bullet
(`decoupled-listener-io.md:344-352`), both of which deferred with "recorded as a new
`docs/known-gaps.md` entry" — this record is that deferred work, now designed.
`decoupled-listener-io` stays **Accepted**; this repo's existing convention for a revision like this
one is a forward-pointer left on the older record itself, not a silent one-way reference — the
`> **Revised by [ADR ...]**` blockquote `buffered-sink-delivery` added to
`service-lifecycle-and-output-retry.md:72` when it revised that ADR's retry-budget section is the
precedent — so `decoupled-listener-io.md` gains the equivalent blockquote at those two bullets, and
its own `updated` frontmatter (and README index row) move to today. Its surrounding Decision text is
otherwise untouched.

## Decision

### Socket-level stats come from `getsockopt(SO_MEMINFO)`, not `/proc/net/udp[6]`

The kernel drop counter and receive-buffer occupancy for a UDP socket are available two ways: parse
`/proc/net/udp`/`/proc/net/udp6` and match a row to this process's socket by local address/port and
inode, or call `getsockopt(fd, SOL_SOCKET, SO_MEMINFO, ...)` (Linux ≥ 4.12) directly on the socket's
own file descriptor. The two differ in every dimension that matters here:

| | `/proc/net/udp[6]` | `SO_MEMINFO` |
|---|---|---|
| Cost per sample | O(sockets in the netns) — read and parse the whole table to find one row | O(1) — one syscall on a known fd |
| Identification | match by local address:port (+ inode, to disambiguate) | none needed — it's this fd |
| Multicast / `SO_REUSEADDR` | several sockets can share one bound address; address-based matching can't tell them apart | not a problem — the fd is unambiguous regardless of what it shares an address with |
| What it returns | `drops` column only (plus `tx_queue`/`rx_queue`, not directly comparable to `SO_MEMINFO`'s fields) | `DROPS`, `RMEM_ALLOC`, `RCVBUF`, `WMEM_ALLOC`, `SNDBUF` in one call |
| Portability | Linux-only, format has changed across kernel versions before | Linux-only, stable ABI since 4.12 |

`SO_MEMINFO`'s `DROPS` field is exactly `/proc/net/udp`'s `drops` column for the same socket — same
kernel counter, same semantics (`sk_drops`, incremented wherever the kernel discards a datagram for
this socket: receive-buffer full, memory pressure, a bad checksum after the socket already
committed to receiving it). Reading it via `getsockopt` instead of procfs is not a weaker signal,
it's the same signal without the O(sockets) scan or the multicast/`SO_REUSEADDR` identification
problem that made address-based matching genuinely fragile — a listener bound with `SO_REUSEADDR`
alongside another instance, or a multicast group with several subscribers, can have more than one
socket at the same local address, and nothing in `/proc/net/udp`'s columns tells them apart.

`SO_MEMINFO`'s `RMEM_ALLOC`/`RCVBUF` pair is also what actually answers "is this listener close to
dropping" — `RMEM_ALLOC` is the kernel's own current accounting of bytes queued against this
socket's receive buffer, `RCVBUF` is the granted ceiling (the same value ADR `decoupled-listener-io`
already gauges once at bind as `logit.input.receive_buffer.bytes`, doubled by Linux's own
bookkeeping convention). `RMEM_ALLOC` against `RCVBUF` is the very pair the kernel's UDP enqueue
path (`__udp_enqueue_schedule_skb`) compares when deciding whether to drop an arriving datagram —
this gauge samples the drop condition's own inputs, not a proxy for them. That's deliberately not
the same as claiming a fixed operator here: the exact comparison has changed across kernel versions
(it also weighs the incoming skb's own `truesize`, not just its payload length), so `logit` reports
the two raw numbers rather than asserting a threshold it doesn't own. Sampling both turns
`logit.input.kernel.drops` from a bare count into an explainable one: an operator watching a rising
`receive_buffer.utilization` alongside `kernel.drops` sees the mechanism, not just the symptom.

**`SO_RXQ_OVFL` was also considered and rejected.** It's the other Linux mechanism for the same
data — enabling it makes the kernel attach an ancillary `SCM_RXQ_OVFL` control message (a `u32`
drop count at time of receipt) to every `recvmsg` call, rather than a value read on demand. Two
things rule it out here: it's a per-message cmsg, which couples the drop signal to the receive path
itself (every `recv_from`, and later every `recvmmsg`, would need to parse and accumulate it) rather
than being sampleable independently on a timer the way `SO_MEMINFO` is; and it carries only a `u32`
running count with no accompanying buffer-occupancy context, so it would need `SO_MEMINFO` (or
procfs) alongside it anyway to explain *why* drops are happening, not just that they are. `SO_MEMINFO`
alone gives both, from a call site that isn't the hot path.

No new crate dependency either way. This ADR originally anticipated declaring `SO_MEMINFO` and the
`SK_MEMINFO_*` field indices locally with a header citation, in case libc 0.2.189 (the version
already in `Cargo.lock`) lacked them; **it does not** — that version exports `SO_MEMINFO` and all
nine `SK_MEMINFO_*` indices, and `sockstat` uses `libc::`'s own constants for them. The one constant
that genuinely is not in libc for Linux is `TCP_LISTEN` (only its Hurd module defines one), so that
single value is declared locally with a comment citing `include/net/tcp_states.h` — which is the
codebase convention this paragraph was reaching for: a small local constant with a citation over a
crate for one syscall.

### `sockstat` lives in `logit-pipeline`

Three crates could hold it, and two of them are wrong for reasons worth recording.

Not **`logit-inputs`**, where the only caller lives today: `logit-outputs` is the foreseeable second
consumer (a UDP sink's own `sk_drops`, a TCP sink's send-side fill), and an output crate must not
depend on an input crate to read a socket counter.

Not **`logit-core`**, which is where this ADR first put it on the strength of that same argument.
That crate's own doc says "no I/O, no pipeline, no protocol codecs live here," and
[`docs/design/pipeline-graph.md`](../design/pipeline-graph.md)'s "Crate layout" section leans on
that sentence explicitly ("weakening that would blur a boundary the crate exists to hold") when it
places socket-level mechanics — the `SO_RCVBUF` setsockopt, the `recv_from` loop — outside it. A raw
`getsockopt` plus a `libc` dependency would have been the first I/O in a crate that says it has
none, and neither text would have been true any more.

**`logit-pipeline`** satisfies the original requirement without that cost: both impl crates already
depend on it, it is where the generic, protocol-free machinery already lives (`BoundedQueue`,
`BatchAccumulator`, `Fanout`), and it already performs real I/O without claiming otherwise —
`disk_queue.rs` writes and fsyncs segment files. The split with `logit-inputs` is the one that crate
boundary already draws everywhere else: fd-level *readings* any component could want live in
`logit-pipeline`; opening the socket, sizing it and reading datagrams off it stay in
`logit-inputs::udp`/`tcp`.

### TCP listeners: `TCP_INFO` on the `LISTEN` socket, same PR

`syslog_in`'s TCP/TLS ingress ([ADR `syslog-tcp-ingress-and-tls`](syslog-tcp-ingress-and-tls.md))
has the same blind spot on the accept side that UDP had on the receive side: a `LISTEN` socket's
accept queue can back up with no visibility into it. `getsockopt(fd, SOL_TCP, TCP_INFO, ...)` on a
listening socket is a Linux-specific special case, and a long-standing one — since Linux 2.6.24
(2007), so unlike `SO_MEMINFO`'s 4.12 floor there is no practical kernel-version floor to worry
about here: `tcpi_unacked` reports the current accept-queue depth and `tcpi_sacked` reports the
configured backlog, rather than their ordinary per-connection meanings (unacked segments / SACK'd
segments), because the kernel reuses the same `struct tcp_info` fields for both roles. This is a
genuinely separate socket type and syscall from the UDP work, but it's the same shape (one
`getsockopt` on a fd this process already owns, sampled on a timer) and the same helper module, so
it lands in the same PR/commit as the UDP `sockstat` work rather than as a follow-up — Ross's call,
recorded here since it widens this ADR's scope beyond its title.

### What socket visibility deliberately does not cover

Three things this work does not build, each recorded as its own `docs/known-gaps.md` entry rather
than folded in:

- **UDP sink send-buffer gauges.** A sink's `wmem_alloc` is sampled almost always at or near zero
  in practice — a `sendto` either completes immediately or fails, there's no analogous "queued
  waiting to be sent" state worth polling for a connectionless socket the way there is on the
  receive side. Send-side loss is better observed by counting send errors by `errno` at the call
  site (`EAGAIN`/`ENOBUFS` and the like), not by polling a gauge that's almost never informative.
- **Netns-wide `/proc/net/snmp`/`netstat` counters.** `Udp: RcvbufErrors`/`InErrors` and friends are
  aggregated across every socket in the network namespace, not attributable to any one `logit`
  component — exactly the identification problem `SO_MEMINFO` was chosen to avoid, at a wider scope
  where there's no fd to disambiguate with at all.
- **`SK_MEMINFO_BACKLOG`/`FWD_ALLOC`/`WMEM_QUEUED`/`OPTMEM`.** Four of `SO_MEMINFO`'s nine returned
  fields — real, but socket-internal bookkeeping (the backlog processing queue, forward-allocated
  memory, the queued-for-send byte count, ancillary option memory) that doesn't map to an
  operator-actionable question the way `RMEM_ALLOC`/`RCVBUF`/`DROPS` do. Noise, not signal, for a
  metrics catalog that already has a convention (`docs/design/internal-telemetry.md`'s "Why this
  exists") of learning what matters by running the thing rather than exposing everything a syscall
  happens to return. Of the remaining five, `SockMeminfo` reads all of `RMEM_ALLOC`/`RCVBUF`/
  `WMEM_ALLOC`/`SNDBUF`/`DROPS` (`crates/logit-pipeline/src/sockstat.rs`) — but W1 only emits
  `DROPS`/`RMEM_ALLOC`/`RCVBUF` as metrics. `WMEM_ALLOC`/`SNDBUF` are carried on the struct for a future
  sink-side consumer rather than read and then discarded; they're not emitted here because that's
  exactly the send-buffer visibility the bullet above rules out building today, on the sink side —
  reading them once, cheaply, alongside the receive-side fields costs nothing and leaves the seam
  open without shipping a gauge nobody asked for yet.

### Sampling cadence: a 1 s timer, no config knob, a guaranteed final sample

Socket stats are sampled on a fixed 1 s interval by a new `read_loop_sampled` wrapper
(`crates/logit-inputs/src/udp.rs`) — **not** a separate task, and **not** a third arm of either
`select!` that already exists there. It holds a pinned `read_loop` future and a 1 s
`tokio::time::interval` in one new `select!`, looped so a tick never resolves the wrapper itself:
each tick samples and the loop goes back around to `select!` again, while `read_loop`'s own future —
polled by reference every iteration — keeps running underneath it, untouched. `read_loop_sampled`
then takes `read_loop`'s place as the future `run_until_shutdown` races against `decode`
(`udp.rs:269-270,296-298`), so sampling continues while `read_loop` is parked inside a blocked `push`
(`overflow: block`) — exactly the moment drops are most likely to be happening.

This has to be a wrapping loop, not a third arm of either existing `select!`, for two different
reasons:

- **`run_until_shutdown`'s own `select!` (`udp.rs:296-298`) is a one-shot race**, not a loop: it
  decides once which of the whole `read`/`decode` futures finishes first, guarded by the `Option`
  indirection the code comment there explains is specifically there to avoid double-polling
  whichever one didn't win. A periodically-firing arm there would resolve that same race on every
  tick instead of only when `read`/`decode` actually finish — it would need its own wrapping loop
  regardless, which is exactly what `read_loop_sampled` provides, one layer down, next to the socket
  it's actually sampling.
- **`read_loop`'s own two nested per-iteration `select!`s** (`recv_from` vs. `shutdown`, `queue.push`
  vs. `shutdown`) are genuine two-way races, and `tokio::select!` drops whichever future didn't win.
  Adding a timer arm to either would cancel an in-flight `recv_from`/`push` on every ordinary tick —
  silently dropping the very datagram the sampler exists to observe, exactly the loss this ADR
  elsewhere bounds to the shutdown path only (see "Cancellation of `push_many`" below). Sampling has
  to run *alongside* whatever `read_loop` is doing, never race it.

**The sampler's `select!` is `biased` with the timer arm first, and W4 must not undo that.** The
intuitive ordering is the other one — prefer the work, sample while idle — and it silences the
sampler for the entire duration of the overload it exists to report. `tokio` gives each task a
cooperative-scheduling budget of 128 units per poll and every resource operation spends one, so
under a flood `read_loop` never parks for a real reason: it returns `Pending` only once that budget
is gone. `Sleep::poll` opens with `coop::poll_proceed` (`tokio/src/time/sleep.rs`), so a timer arm
polled *after* the read arm sees a budget of zero and returns `Pending` however far past its
deadline it is; the next wake repeats it, forever. A `Pending` caused by the coop budget is not a
park, and an arm behind one never runs. Measured on a release build, eight senders flooding one
listener for 10 s at ~90% kernel loss: read-arm-first carried the drop counter and buffer gauges in
0 of 10 one-second windows (41–47M drops arriving as a single lump from the final sample after
SIGTERM); timer-arm-first carried them in 11 of 11, with no measurable throughput difference. This
is a property of `read_loop` being one long-lived future that does not return between samples, so
W4's `recvmmsg` rewrite of that loop inherits the constraint unchanged — `crate::udp::sample_while`
carries the full reasoning, and a regression test that fails with the arms swapped pins it.

A **guaranteed final sample** runs after the read loop exits (shutdown or fatal error), immediately
before `read_loop_sampled` itself returns — the one thing no `select!`'s arm ordering already
guarantees — so a short-lived process — the same concern ADR `load-test-harness` raised for
`internal`'s own drain tick — doesn't lose its last interval of drop/buffer data.

No config knob for the interval. Every other timer this codebase exposes as configuration
(`aggregate`'s `interval`, `internal`'s `interval`) controls something an operator trades off
against a real cost — memory held across a longer window, event volume out. A socket-stats sample is
one `getsockopt` call a second; there's no cost dimension for a knob to trade against, and adding
one now would be speculative configuration surface with no motivating request behind it.

**First failure → one `diag.warn`, disabled for the process lifetime.** `getsockopt(SO_MEMINFO)`
failing at all (wrong socket type, a kernel older than 4.12, a sandboxed environment that blocks the
call) is not a transient condition worth retrying every second and not worth a log line every
second either — `Diagnostics::warn_throttled` exists for repeating conditions, but this isn't one:
if it fails once it will fail identically forever, so sampling for that listener disables itself
after the first failure and says so exactly once, rather than a throttled line still firing every
few seconds for the rest of the process's life.

### Metric names, and why `kernel` appears only on `logit.input.kernel.drops`

Following `docs/design/internal-telemetry.md`'s "Naming" convention (`logit.component.*` for the
uniform per-node set every component gets from the runtime; `logit.<kind-family>.*` for
component-specific detail nothing generic could know), the new points are:

- `logit.input.kernel.drops` (count, delta) — from `SO_MEMINFO`'s `DROPS`.
- `logit.input.receive_buffer.used.bytes` (gauge) — `SO_MEMINFO`'s `RMEM_ALLOC`.
- `logit.input.receive_buffer.utilization` (gauge) — `used.bytes / RCVBUF`.
- `logit.input.receive_buffer.bytes` (gauge, re-emitted each tick) — the existing granted-`SO_RCVBUF`
  gauge ADR `decoupled-listener-io` samples once at bind; folding it into the same per-tick sample
  means a bind-only value no longer silently persists once the listener's own `ComponentBuffer`
  drains it (`mem::take` on each drain window, `docs/design/internal-telemetry.md`'s buffer
  section) — a bind-only gauge would otherwise read as "vanished" after one window rather than
  "unchanged."
- `logit.input.accept_queue.depth` / `.limit` / `.utilization` (gauge) — `TCP_INFO`'s
  `tcpi_unacked`, `tcpi_sacked`, and the first over the second, sampled on the same cadence, plus
  once in the accept loop right before each `accept()` so an idle listener that never accepts still
  reports a value. `.limit` is re-emitted each tick for the same `mem::take` reason
  `receive_buffer.bytes` is, and is a gauge in its own right rather than left implicit in the ratio:
  an operator deciding whether to raise `net.core.somaxconn` needs the ceiling itself, and backing
  it out of `depth / utilization` is undefined at the depth of 0 an idle listener always reports.

All six are `logit.input.*`, not `logit.component.*` — they're genuinely impl-known, the same
reasoning `internal-telemetry.md` already gives for the pre-existing `logit.input.datagrams`/
`.datagram.bytes` arrival counters and `receive_buffer.bytes`/`.requested.bytes`: nothing generic in
the runtime can see a socket's kernel-side state, only the listener impl holding the fd can.

`kernel` appears in exactly one of the six names, `logit.input.kernel.drops`, and deliberately not
in the others, for the same reason `internal-telemetry.md` already gives for keeping *userspace*
drops (`logit.component.datagrams.dropped{reason=...}`, `ReceiveQueue` eviction) unqualified: an
operator alerting on data loss shouldn't have to union namespaces, but a drop `logit` counted itself
and a drop the kernel made before `logit` ever saw the datagram are not the same event and must not
share a name an alert could double-count or silently prefer one over the other. `receive_buffer.*`
and `accept_queue.*` don't need the qualifier because there's no *userspace* buffer-occupancy or
accept-queue metric they could be confused with — `kernel` only earns its place on the one name that
would otherwise collide in meaning with an existing, differently-sourced counter.

### `recvmmsg(2)` unconditionally on Linux; `read_batch` default 64, ceiling 1024

On Linux, the read path becomes unconditionally `recvmmsg(2)` with `vlen = read_batch` — there is no
second Linux code path retained for `read_batch: 1`; a `vlen` of 1 is simply one datagram per
syscall, structurally the same call already made today, so there's nothing an `if read_batch == 1`
branch would buy over letting the general case degrade to it. Non-Linux targets keep today's
`recv_from` loop unconditionally; `read_batch` still parses and validates there (so a config is
portable across targets), and is documented as ignored.

**Default 64 — provisional pending W4's sweep.** Telegraf's UDP reader and rsyslog's `imudp` both
default their own batch-equivalent knob in the same rough range (rsyslog's reference high-throughput
config sets `batchSize: 128`; gostatsd's `--receive-batch-size` defaults to 50), and 64 sits between
them as a starting point, not a measured optimum for this codebase's own decode/queue costs.

> **Evidence placeholder — W4.** The 16/32/64/128 sweep that either confirms or revises this default
> is a W4 deliverable (`docs/plans/udp-intake.md`'s W4 row), run against the pinned `udp-statsd`
> scenario once `recvmmsg` exists to sweep over. This section gets the sweep's numbers and the
> resulting default (confirmed-64 or a revised value) filled in as part of that workstream; until
> then, 64 is a reasoned starting point, not a measured one.

**Ceiling 1024 = `UIO_MAXIOV`.** `recvmmsg` takes an array of `mmsghdr`, each wrapping an `iovec`;
`UIO_MAXIOV` (1024 on Linux) is the kernel's hard limit on how many `iovec`s a single vectored I/O
call can carry, enforced by `sendmsg`/`recvmsg`/`recvmmsg` alike. `read_batch` values above it would
either be silently clamped by the kernel or rejected outright depending on call path — rejecting
above 1024 at config-validation time (new graph rule 57, alongside rule 18's existing `read_batch: 0`
rejection) turns a kernel-dependent runtime surprise into a config-time error with a name attached
to it.

### One `received_at` per syscall batch — a named accuracy concession

`decode_into`'s `received_at` parameter (ADR `decoupled-listener-io`) already means "arrival time,"
not "decode time" — widened specifically so decode running behind arrival under backlog doesn't
silently corrupt it. `recvmmsg` returning several datagrams in one syscall return means those
datagrams no longer have individually-observable arrival instants at the point `logit` learns about
them at all: the kernel doesn't report a per-message receive timestamp through this path (that would
need `SO_TIMESTAMP`, a separate cmsg-based mechanism, per message, which reintroduces exactly the
per-message ancillary-data cost `SO_RXQ_OVFL` was rejected for above). One `now_nanos()` call is
made per batch, immediately after the syscall returns, and stamped on every datagram the batch
contains. This is a real, named accuracy concession — datagrams later in a large batch are stamped
slightly earlier than they actually arrived — bounded by how large a batch actually gets (`read_batch`
at most, typically far fewer under normal load), and strictly better than today's alternative of
`decode_loop`'s own clock skewing arbitrarily far behind arrival under backlog, which this
concession does not reintroduce: the stamp is still taken at receipt (of the batch), not at decode.

**Headers rebuilt per call, not held across an await, so the read future stays `Send`.** The
`mmsghdr`/`iovec` arrays `recvmmsg` needs are raw-pointer-bearing C structs; building them once and
reusing them across calls would mean holding raw pointers into a buffer across the `.await` inside
`socket.async_io(Interest::READABLE, ...)`, which is exactly the shape that forces an `unsafe impl
Send` or blocks compilation outright depending on how the pointers are held. Building the arrays
fresh inside the `async_io` closure on every call — pure CPU, no allocation the buffer reuse doesn't
already amortize — keeps the whole read path an ordinary `Send` future with no unsafe trait impl,
matching this codebase's existing raw-fd precedent (`crates/logit-inputs/src/tail/watch.rs`'s
`inotify` read: a `// SAFETY:` comment on the one `unsafe` block, no pointer held across an await).

### `push_many`/`pop_many` live on `BoundedQueue` itself; `push`/`pop` untouched

`docs/known-gaps.md:200-214`'s entry names the fix explicitly: gauge-update contention belongs in
`BoundedQueue` itself, "not as a receive-only special case," because `BoundedQueue` is one
implementation serving both the sink queue and the receive queue by design. `push_many`/`pop_many`
land as new methods on `BoundedQueue<T: Queued>` (`crates/logit-pipeline/src/queue.rs`) — **not** a
receive-side wrapper type — and existing `push`/`pop` (and every sink-side call site using them) are
untouched, so the 18 existing queue tests keep exercising exactly the code path they always have.

`push_many` drains an input `&mut Vec<T>`: per-item weight/overflow/drop counting and per-item
`Block`-policy waiting are preserved exactly as `push` already does them (an item that would never
fit still falls through to the same eviction path, an item that has to wait for room under `Block`
still waits for room), but **one lock is held per contiguous run of items that currently fit**, and
critically **one `update_gauges` call covers the whole `push_many` invocation** — not one per item,
which is the entire point: the receive side's two hottest loops (`read_loop` pushing, `decode_loop`
popping) stop contending on the gauge lock once per datagram and instead contend once per batch.
`pop_many` is `push_many`'s mirror: fills a reused output `&mut Vec<T>` up to `max` items, returns
the count actually popped (`0` means closed-and-empty, `pop`'s existing sentinel), one lock and one
`update_gauges` per call, cancellation-safe the same way `pop` is (no `.await` between reserving and
removing an item).

**One `notify_one` per call is an honest observation, not an optimization.** `tokio::sync::Notify`
stores at most one permit regardless of how many times `notify_one` is called before the next
`notified().await` consumes it — calling it once after a whole batch lands is not fewer wakeups than
calling it once per item followed immediately by more items landing before the waiter runs; either
way the waiter sees "something is ready" once and re-checks state under the lock, which is the same
condvar pattern `push`'s own doc comment already documents. Recording this here so a future reader
doesn't credit the batched call with a latency win the `Notify` semantics don't actually provide.

`decode_loop` calls `pop_many` into a reused `Vec<Datagram>` with `max` equal to a constant matching
today's implicit batch size (`read_batch`'s own default, 64) until W4 threads the real config value
through; `receive.latency` (arrival → dequeue) stays recorded per datagram inside the popped batch,
not coarsened to a per-batch figure — the number that says whether event timestamps are trustworthy
under load loses none of its resolution from this change.

### Cancellation of `push_many`: the remainder is dropped uncounted, ≤ `read_batch`, shutdown path only

`push_many` holds a `Peekable<std::vec::Drain<'_, T>>` across its internal `.await` points
specifically so a cancellation (the caller's future dropped mid-call — `read_loop`'s own shutdown
race, the same shape `udp.rs:474-477`'s existing single-item cancellation already documents) leaves
the caller's `Vec` **empty**, not partially drained: whatever was already accepted into the queue
stays accepted, and whatever hadn't yet been reached is dropped along with the `Drain` iterator,
uncounted. This is a direct widening of the loss `decoupled-listener-io` already accepted for a
single in-flight datagram at shutdown (`udp.rs:474-477`, "drops the one datagram it was holding,
uncounted") from exactly one datagram to at most `read_batch` datagrams — still bounded, still
shutdown-path-only (ordinary operation never cancels a `push_many` call mid-flight), and named here
explicitly rather than left implicit in the widened bound.

**The decode side has the same shape, for the same reason.** `decode_loop` popping a batch means a
cancelled decode loop (the shutdown-grace backstop dropping that future) discards whatever it had
popped but not yet decoded — the same widening, from the one datagram `pop` held to at most one
`pop_many` batch, on the same shutdown-only path, uncounted for the same reason. `pop_many` itself
loses nothing: it only ever awaits on an iteration that removed nothing at all, so a cancellation
while it is waiting leaves the queue and the caller's `Vec` exactly as they were. The loss is in
`decode_loop`'s own iteration over what it already holds, and it is bounded by the same constant.

**Why the remainder can't be counted.** Counting a drop needs a `Telemetry::gauge`/count call, which
needs the same lock `push_many` already released after its last successful batch — re-acquiring it
from inside a `Drop` impl (the only code that runs on cancellation) to record a handful of
uncounted-item drops would add lock-acquisition-from-`Drop` machinery, with its own poisoning and
ordering hazards, to shave a rare, already-bounded, already-accepted loss down from "≤ read_batch,
uncounted" to "≤ read_batch, counted." Not worth the hazard for a shutdown-only, already-bounded
loss path.

### The perf harness: real sockets, sender in the harness process, delivered-side denominators

A UDP scenario's sender runs inside the `logit-perf` process itself, not as a separate spawned
process — which means the child `logit` process's own `wait4` rusage is **pure receive-side CPU**:
nothing about generating or transmitting the load competes for CPU accounting on the measured
process's own resource-usage figure. This is what keeps `cpu_us_per_event` meaningful as the gate
`compare` already uses it for (ADR `load-test-harness`'s "CPU microseconds per event is the headline
number").

Both `events_per_s` and `cpu_us_per_event` are computed over events **delivered to `null_out`**, not
events sent — a UDP scenario is the first one in this codebase where those two counts can honestly
differ (a `generate_in` scenario's generator and its downstream `null_out` see the same count by
construction; a real socket can drop between the two). Denominating over sent count would silently
understate the workload's true per-event cost whenever any drop occurs — exactly the regime this
work's own baseline is tuned into (see below) — so `events delivered` is the only denominator that
stays honest across a run with a nonzero drop rate.

**A run-time-only telemetry leg, never shipped in the scenario YAML.** Per-node attribution
(`ADR load-test-harness`'s `internal → file_out native` dump) is appended by the harness at run
time for `Driven` (real-socket) scenarios exactly as it already is for `Generated` ones; `attribute`
already refuses to run against a scenario config that already has an `internal` component defined,
so the leg staying purely run-time-attached rather than checked into `perf/scenarios/udp-statsd.yaml`
is required by that existing guard, not a new rule invented for UDP.

**Absolute CPU/event is comparable only to `udp-statsd`'s own history, explicitly.** Every other
scenario's absolute numbers are at least notionally comparable to each other (same machine, same
harness overhead, generator-driven). `udp-statsd`'s absolute CPU/event additionally reflects
kernel/socket-stack overhead specific to loopback UDP on this dev-container's kernel — a real cost,
but one the other scenarios don't pay at all, so cross-scenario absolute comparison (e.g. "`udp-statsd`
costs 3× `json-parse`") would mix two different kinds of cost. The number is real and worth tracking
run over run for `udp-statsd` itself; it was never meant to rank against a `generate_in` scenario's
number in the first place.

**Pinned runs only.** This dev box's heterogeneous cores (Zen 5 performance cores vs. Zen 5c
efficiency cores) make unpinned runs bimodal by roughly 2× — an established finding from prior
benchmarking work on this box, not specific to UDP. `--pin-sender`/`--pin-child`
(`sched_setaffinity`) are not optional flourishes for this scenario family; every recorded
`udp-statsd*` number states which CPUs it pinned to.

### Representative traffic, calibrated against a recorded real-client capture

"Real statsd traffic" is a *traffic model calibrated against a recorded real-client capture*, not N
copies of one line. No recorded statsd producer corpus exists in this repo today —
`crates/logit-cli/tests/fixtures/statsd/` is hand-made round-trip cases (one line, one expected
decode, no volume or mix), `demo/` has no statsd producer, and
[`docs/plans/recorded-interop-fixtures.md`](../plans/recorded-interop-fixtures.md) already lists real
statsd/DogStatsD producer captures as owed, un-built future work. `tools/record-fixtures/raw_capture.py`
is extended with a real DogStatsD client and a plain-statsd client (buffered and unbuffered) emitting
an app-like workload; the captured traffic becomes the model's ground truth (line shapes, tag
cardinality, datagram-size distribution derived from it, documented in `perf/load/README.md`) rather
than replayed directly — this is a load *generator* calibrated by a capture, not a replay engine, so
it can still hit an arbitrary target rate.

**Datagram-size mix matters most, which is why there are three scenarios, not one.** A single-metric,
unbuffered-client datagram (~40–120 B) makes every `recvmmsg`/gauge/queue improvement in this ADR
count for the most, since the per-datagram fixed cost dominates a payload that small — this is the
syscall-bound worst case the batching work specifically targets, and a headline number computed only
against MTU-packed traffic would hide exactly the regime this work most helps. An MTU-packed
datagram (≤1432 B, DogStatsD's own UDP default) shifts the balance toward decode cost instead — more
metrics processed per syscall, so `recvmmsg`'s win per datagram shrinks while `BatchAccumulator`'s
and the decoder's own cost dominate more of the total. A number from only one of the two would
mislead about which half of the pipeline actually benefits; `udp-statsd` (mixed, the headline),
`udp-statsd-small` (all single-line), and `udp-statsd-packed` (all ≤1432 B) each report separately
at W3 and W4 so a reader can see which regime moved.

### `SO_REUSEPORT` and `UDP_GRO`: explicitly out of scope, and why they can wait

Both are real, field-precedented techniques for going further than this ADR does (`docs/known-gaps.md`'s
existing "One reader per UDP listener" entry already names `SO_REUSEPORT`; `UDP_GRO` — generic
receive offload coalescing several datagrams from the same flow into one larger buffer the kernel
hands back — is a newer mechanism neither entry above mentions yet). Both wait for the same reason:
this ADR's whole premise is *measure first, then decide what's still worth building* — `recvmmsg`
and gauge-batching raise the single-reader, single-syscall-per-datagram ceiling this process already
has; whether that ceiling is still the bottleneck after this work lands, and whether it's worth
`SO_REUSEPORT`'s own real cost (N readers each needing their own answer to the cancel-by-drop
shutdown cascade `decoupled-listener-io` built around exactly one `Fanout` per listener) or
`UDP_GRO`'s own real cost (a socket-option-gated code path only useful when senders are on the same
host or the NIC hands off coalesced segments — not guaranteed in most deployments), is a question W4's
measurements are positioned to answer and this ADR is not. Building either now would be designing
ahead of evidence this same plan is about to produce.

## Alternatives considered

- **`/proc/net/udp[6]` scraping, matched by local address/inode.** Rejected — see the `SO_MEMINFO`
  comparison table above: O(sockets) per sample, and no way to disambiguate multiple sockets sharing
  one bound address under multicast or `SO_REUSEADDR`.
- **`SO_RXQ_OVFL` (per-message `SCM_RXQ_OVFL` cmsg).** Rejected — couples the drop signal to the
  receive path itself rather than being independently sampleable, and returns only a bare `u32`
  count with no buffer-occupancy context.
- **A configurable socket-stats sampling interval.** Rejected — a `getsockopt` call a second has no
  cost dimension worth trading off, unlike every other interval this codebase exposes as config.
- **Netns-wide `/proc/net/snmp`/`netstat` counters.** Rejected — not attributable to any one
  component; the same identification problem `SO_MEMINFO` was chosen specifically to avoid, at a
  wider scope with no fd to disambiguate against.
- **Exposing `SK_MEMINFO_BACKLOG`/`FWD_ALLOC`/`WMEM_QUEUED`/`OPTMEM` alongside `DROPS`/`RMEM_ALLOC`/
  `RCVBUF`.** Rejected as noise — real fields, but socket-internal bookkeeping with no operator-actionable
  question behind them.
- **UDP sink send-buffer gauges (`wmem_alloc` polling).** Rejected — almost always reports ~0 for a
  connectionless socket in practice; send-side loss is better observed as errno-tagged send-error
  counts at the call site.
- **A second Linux code path for `read_batch: 1`, avoiding `recvmmsg` for the single-datagram case.**
  Rejected — `vlen = 1` already is one datagram per syscall; a branch to avoid it buys nothing and
  doubles the code paths to test.
- **Holding `mmsghdr`/`iovec` arrays across the read future's await points, reused between calls.**
  Rejected — forces either an `unsafe impl Send` or blocks compilation outright; rebuilding them
  inside the `async_io` closure each call is pure CPU with no added allocation and keeps the future
  ordinarily `Send`.
- **`SO_TIMESTAMP` per-message receive timestamps, to avoid the one-`received_at`-per-batch
  concession.** Rejected — a per-message cmsg mechanism, the same shape rejected for `SO_RXQ_OVFL`
  above and for the same reason: it couples timestamping to the receive path per message rather than
  staying a cheap, batch-level call.
- **A receive-side wrapper type for `push_many`/`pop_many`, instead of adding them to `BoundedQueue`
  itself.** Rejected — `docs/known-gaps.md:200-214` is explicit that the fix belongs in `BoundedQueue`
  itself since it serves both the sink and receive queues by design; a wrapper would re-split
  behavior the generic type exists to keep unified.
- **Counting `push_many`'s cancelled remainder as a drop.** Rejected — would need re-acquiring the
  queue's lock from inside a `Drop` impl to shave an already-bounded, already-accepted,
  shutdown-path-only loss from "uncounted" to "counted"; not worth the poisoning/ordering hazard.
- **N synthetic copies of one statsd line as the perf load, instead of a calibrated traffic model.**
  Rejected — Ross's explicit direction: real statsd traffic varies in type mix, name length, tag
  cardinality, and datagram packing in ways that change which half of the pipeline is the
  bottleneck; one repeated line would benchmark a workload nothing sends in practice.
  A full replay engine over the captured traffic (rather than a calibrated model) was also
  considered and rejected — a fixed-rate capture can't be driven to an arbitrary target load, which
  a sweep across drop regimes needs.
- **`SO_REUSEPORT` multi-reader and `UDP_GRO`, built now rather than deferred.** Rejected for this
  ADR — see the dedicated section above; both are real future work, gated on this work's own
  measurements first.

## Consequences

- New `crates/logit-pipeline/src/sockstat.rs` (Linux-gated `libc` dep on `logit-pipeline` — see
  "`sockstat` lives in `logit-pipeline`" above for why not `logit-core` or `logit-inputs`):
  `SockMeminfo { rmem_alloc, rcvbuf, wmem_alloc, sndbuf, drops }`, `meminfo(RawFd) -> Option<_>`,
  `listen_queue(RawFd) -> Option<(u32, u32)>`, a wrap-safe `DropCounter::delta`. Non-Linux twins
  return `None`/0.
- `crates/logit-inputs/src/udp.rs` gains a `read_loop_sampled` wrapper (a new, looped `select!`
  racing a pinned `read_loop` against a 1 s interval, guaranteed final sample after `read_loop`
  exits — see "Sampling cadence" above for why this is a new select, not a third arm of either
  existing one) and, on Linux, a `BatchReader` built on `recvmmsg(2)`. `crates/logit-inputs/src/tcp.rs`
  gains a `listen_queue` sample in its accept loop.
- Six new metrics: `logit.input.kernel.drops`, `logit.input.receive_buffer.used.bytes`,
  `logit.input.receive_buffer.utilization`, `logit.input.accept_queue.depth`, `.limit`,
  `.utilization` — see `docs/design/internal-telemetry.md`'s catalog once W1 lands.
- New config field `read_batch: usize` (default 64, provisional pending W4's sweep) on
  `ReceiveConfig`/`UdpListenerConfig`; new graph rule 57 (reject `read_batch > 1024`), alongside
  existing rule 18. `script/schema` regenerated in that commit.
- `crates/logit-pipeline/src/queue.rs`: `BoundedQueue::push_many`/`pop_many`, additive; `push`/`pop`
  and every existing call site unchanged.
- New `crates/logit-perf` module (`load.rs`) and a real-socket `Workload::Driven` scenario kind,
  alongside today's `Workload::Generated`; new `perf/load/*.yaml` sidecar directory and
  `perf/load/README.md`; a small committed real-client capture and its provenance, partly paying
  down `docs/plans/recorded-interop-fixtures.md`'s owed statsd producer fixtures.
- `docs/known-gaps.md`: the kernel-drop-visibility entry (`:176-184`) and the one-datagram-per-syscall
  entry (`:185-191`) close as their respective workstreams land (W1, W4); the gauge-update-contention
  entry (`:200-214`) closes at W3.
- Revises `decoupled-listener-io.md`'s "Alternatives considered" recvmmsg/`SO_REUSEPORT` bullet and
  kernel-drop-counter bullet — both now designed and scheduled rather than merely deferred;
  `decoupled-listener-io.md` gains a `> **Revised by ...**` forward-pointer blockquote at those
  bullets and a bumped `updated` date, following the same convention `buffered-sink-delivery` used
  on `service-lifecycle-and-output-retry.md`.
- The `read_batch` default (64) and the eventual `SO_REUSEPORT`/`UDP_GRO` decision both remain
  explicitly open, pending W4's sweep and this plan's measurements respectively — not assumed by
  anything built here.
