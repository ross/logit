---
created: 2026-09-18
updated: 2026-09-21
---

# UDP intake batching and socket visibility

## Status

Accepted — and built. Every workstream in [`docs/plans/udp-intake.md`](../plans/udp-intake.md) has
landed on its stacked branch: **W0 #250, W1 #251, W2 #252, W3 #253, W4 #254, W5 (this closeout)**.
Per Ross's direction the stack is not merged to `main` by this workstream, so "landed" means complete
and pushed, not merged; that plan's own status paragraph is the authority. See "As built" below for
where the implementation sharpened or corrected this record as it went.

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
  Adding a timer arm to the **push** one would cancel an in-flight `push_many` on every ordinary
  tick — dropping whatever the reader was still holding, uncounted, exactly the loss this ADR
  elsewhere bounds to the shutdown path only (see "Cancellation of `push_many`" below). Sampling has
  to run *alongside* whatever `read_loop` is doing, never race it.

  *(Corrected 2026-09-21: this bullet originally made the same claim about the **recv** arm —
  "adding a timer arm to either would cancel an in-flight `recv_from`… silently dropping the very
  datagram the sampler exists to observe" — and that half is wrong. It also contradicted the code's
  own, correct, cancel-safety comment. `async_io` suspends only *before* it calls its closure, so a
  cancelled read either never made the syscall or had already returned its datagrams; nothing is
  consumed and discarded. See the amendment below. The architectural conclusion is unaffected: the
  push half alone requires the wrapping loop, and a `select!` arm that resolves the whole race on
  every tick would be wrong for `run_until_shutdown`'s one-shot `select!` regardless.)*

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

A **final sample** runs after the read loop exits (shutdown or fatal error), immediately before
`read_loop_sampled` itself returns — the one thing no `select!`'s arm ordering already guarantees —
so a short-lived process — the same concern ADR `load-test-harness` raised for `internal`'s own
drain tick — doesn't lose its last interval of drop/buffer data. *(Amended 2026-09-21: this said
"guaranteed", which is one word stronger than it is. It runs on every path `sample_while` itself
returns on; a future that is **dropped** runs nothing, and `run_input`'s grace backstop drops this
one when the grace expires. See the amendment below for why production never reaches that path.)*

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

**Default 64 — confirmed by W4's sweep, and for a reason worth knowing.** Telegraf's UDP reader and
rsyslog's `imudp` both default their own batch-equivalent knob in the same rough range (rsyslog's
reference high-throughput config sets `batchSize: 128`; gostatsd's `--receive-batch-size` defaults to
50), and 64 sat between them as a starting point. The sweep says it is past the knee for both shapes
of traffic this family models, and that where the knee is has almost nothing to do with 64 itself. A
second sweep on an isolated Azure VM (`docs/design/performance.md` §7) confirms the same plateau
shape and the choice of 64 — that part is no longer a laptop-only claim.

`read_batch` ∈ {1, 16, 32, 64, 128, 256}, `--repeat 3`, pinned `--pin-sender 0,1 --pin-child 2,3`,
each value set on both scenarios at once, everything else held (see "Pinned runs only" and the
`powersave` caveat below). **The table below is from the disposable perf VM**
(`Standard_F8as_v6`, 2026-09-20), superseding the dev-laptop numbers this table used to carry — the
plateau shape and the choice of 64 were already confirmed independently of the laptop
(`docs/design/performance.md` §7); this is that same VM session's own dedicated re-run of this
ADR's specific sweep, at the calibrated knee:

| `read_batch` | **`udp-statsd-small`** µs/ev | fill | kernel drop % | max rcvbuf | | **`udp-statsd`** µs/ev | fill | kernel drop % | max rcvbuf |
|---|---|---|---|---|---|---|---|---|---|
| 1 | **4.173** | 1.0 | **11.60%** | **1.00** | | 0.961 | 1.0 | 4.63% | 1.00 |
| 16 | 3.410 | 3.7 | 0.00% | 0.01 | | 0.797 | 6.3 | 0.00% | 0.28 |
| 32 | 3.618 | 3.5 | 0.00% | 0.01 | | 0.808 | 12.1 | 0.00% | 0.27 |
| **64** | **3.486** | **4.0** | **0.00%** | **0.03** | | **0.795** | **7.6** | **0.00%** | **0.28** |
| 128 | 3.386 | 4.2 | 0.00% | 0.01 | | 0.808 | 14.3 | 0.00% | 0.26 |
| 256 | 3.315 | 3.2 | 0.00% | 0.02 | | 0.795 | 6.3 | 0.00% | 0.27 |

Two things fall out of it, unchanged in shape from the laptop version of this table. **The step is
from 1 to "batched at all", not from 64 to 128.** On the single-datagram-per-packet scenario,
`read_batch: 1` costs 4.17 µs/event, loses 11.6% of datagrams to the kernel and sits at 100%
receive-buffer utilization; every value from 16 up delivers the whole paced load with a kernel
drop rate of zero, at 3.3-3.6 µs/event within ordinary noise of each other. `udp-statsd` shows the
same step, 0.96 µs/event and 4.6% loss at `read_batch: 1` down to a flat ~0.80 µs/event and zero
loss from 16 upward.

**The `fill` column is noisier on this box than the laptop's clean plateau, but the conclusion it
supports is the same.** `fill` is `logit.input.datagrams / logit.input.reads`, the mean number of
datagrams one syscall actually returned. On `udp-statsd-small` it holds in a narrow 3.2-4.2 band
from `read_batch: 16` upward — consistent with the laptop's own ~3 finding, just with more
run-to-run wobble at only 3 repeats each. On `udp-statsd` fill ranges 6.3-14.3 across the same span
without a clean upward trend the way the laptop's ~24-then-plateau shape showed; either way, the
practical reading is unchanged — raising the ceiling above the arrival burst cannot buy anything,
and µs/event is flat regardless of exactly where `fill` lands within that noise.

So 64 is chosen as the smallest power of two comfortably above both workloads' observed plateau,
with the slab cost that implies (4 MiB of address space, a few hundred KiB resident — see below)
rather than the 16 MiB/64 MiB a higher default would ask every listener to reserve for nothing. The
knob still earns its place: a deployment whose fill sits pinned at 64 is telling its operator that
its arrival bursts are larger than this default, and `docs/deploying.md` says so in those terms.

**The slab is not resident under `madvise`/`never`, confirmed directly, not just on the dev
container.** A dedicated repeat of this exact sweep with `transparent_hugepage` forced to `madvise`
on the same VM (`docs/design/performance.md` §7) shows `udp-statsd-small`'s peak RSS flat in a
14.4-16.8 MiB band across the *entire* `read_batch` range — no rise at 128/256 the way the default
`THP=always` sweep above shows (34.1 → 39.2 → 47.6 MiB over the same range). `docs/design/
memory.md` §5 carries the direct allocation-side probe alongside it. **This does not hold under
`THP=always`, confirmed rather than merely likely now:** touching one 4 KiB page per slot under
`THP=always` faults in the whole enclosing 2 MiB huge page, making most of the slab resident rather
than just the touched pages — a same-box, THP-toggled repeat of the identical sweep, not a
different box's different finding. "The slab is not resident" is a claim about `madvise`/`never`
specifically, confirmed on two different boxes now, not a universal one.

**Ceiling 1024 = `UIO_MAXIOV`'s number.** *(Corrected 2026-09-21 — see the amendment below. The
original text of this paragraph claimed `UIO_MAXIOV` was "the kernel's hard limit on how many
`iovec`s a single vectored I/O call can carry, enforced by `sendmsg`/`recvmsg`/`recvmmsg` alike"
and that a larger `read_batch` "would either be silently clamped by the kernel or rejected outright
depending on call path". Both are false for this call. `UIO_MAXIOV` bounds `msg_iovlen` within one
`msghdr` — `__copy_msghdr`, `net/socket.c`, returns `-EMSGSIZE` above it — and this read path sets
`msg_iovlen` to 1. `do_recvmmsg` clamps no `vlen` at all; its loop is a plain
`while (datagrams < vlen)`, and the only `UIO_MAXIOV` clamp on a `vlen` anywhere in the kernel is
`__sys_sendmmsg`'s, on the send side.)* The number stays 1024 and the rule stays: what it bounds is
`logit`'s own cost, not the kernel's tolerance — the `read_batch × 65,507`-byte slab each listener
reserves (67 MB of address space at this ceiling) and how many datagrams a cancelled `push_many`
can discard on the shutdown path. Rejecting above it at config-validation time (graph rule 57,
alongside rule 18's existing `read_batch: 0` rejection) gives both costs a named ceiling rather
than an unbounded one.

**`read_batch` larger than `receive.max_datagrams` is deliberately legal, and there is no rule
against it.** It looks like it should be one — a single read whose batch cannot fit in the whole
queue even when the queue is empty — but `push_many` already has a defined answer for exactly that
case, per item, identical to what a sequence of single `push` calls would have done: evict or reject
under `drop_oldest`/`drop_newest`, or wait for room under `block`. A validation rule here would refuse
a configuration that works. It is worth naming, though, because it is the configuration that made a
real bug visible: `push_many` under `block` originally notified `not_empty` only once its whole batch
had landed, which with `max_datagrams < read_batch` and no flush timer left the reader waiting for
room while the decoder waited for an item already sitting in the queue. Fixed in W3 (notify before
each wait), and pinned end to end from the listener's own tests rather than only from the queue's.

### One `received_at` per syscall batch, offset per datagram — a named accuracy concession

`decode_into`'s `received_at` parameter (ADR `decoupled-listener-io`) already means "arrival time,"
not "decode time" — widened specifically so decode running behind arrival under backlog doesn't
silently corrupt it. `recvmmsg` returning several datagrams in one syscall return means those
datagrams no longer have individually-observable arrival instants at the point `logit` learns about
them at all: the kernel doesn't report a per-message receive timestamp through this path (that would
need `SO_TIMESTAMP`, a separate cmsg-based mechanism, per message, which reintroduces exactly the
per-message ancillary-data cost `SO_RXQ_OVFL` was rejected for above). One `now_nanos()` call is
made per batch, immediately after the syscall returns, and datagram `i` of that batch is stamped
`base + i` nanoseconds.

**Exactly what that guarantees, and what it does not.** It guarantees that every datagram's stamp is
distinct, and that stamps increase in arrival order — within a batch by construction, and across
batches because the next batch's `base` is read only after the previous batch's per-datagram copies
have run, which takes microseconds against offsets of at most `read_batch` nanoseconds. It does
**not** claim the one-nanosecond spacing measures anything: the real inter-arrival gaps inside a
batch are unknown and certainly not uniform. So this stays a named accuracy concession — datagrams
later in a large batch are stamped earlier than they actually arrived, bounded by however long the
batch took to accumulate — and it stays strictly better than the alternative of `decode_loop`'s own
clock skewing arbitrarily far behind arrival under backlog, which it does not reintroduce: the stamp
is still taken at receipt (of the batch), not at decode. Nothing here forces monotonicity across a
backwards wall-clock step, either; `received_at` follows `SystemTime` exactly as it always has,
because an arrival timestamp that silently stopped tracking the clock would be worse than one that
reflects an NTP correction.

**The `+ i` is not decoration, and it was not in this ADR's first draft.** A sink keyed on
(series, timestamp) treats two points sharing both as *one* point. `influxdb_out` is the live case:
line protocol overwrites on exactly that key, and its `allocate_timestamp` disambiguation
(`crates/logit-outputs/src/influxdb.rs`) is cleared at the top of every `Encoder::encode` — it is
deliberately batch-scoped, so it only ever resolves collisions *inside* one output batch. A whole
read batch stamped with a single instant would routinely produce same-series same-timestamp points
straddling an output-batch boundary, where nothing disambiguates them and the later silently
overwrites the earlier. One nanosecond per datagram costs no extra clock read, is strictly ordered,
and is orders of magnitude below the batch's own arrival uncertainty, so it removes the collision
without pretending to precision the batch does not have.

Two neighbours worth naming, because `+ i` does not fix them and is not meant to.
**`prometheus_out`'s remote-write** encoder truncates to milliseconds and collapses same-series
samples landing in the same millisecond, counting each dropped reading as
`logit.output.metrics.degraded{reason="sub_ms_collapsed"}`; a nanosecond offset cannot separate
those. **`graphite_out`** works in whole seconds, with the same answer. Neither is a new consequence
of the batched read — a statsd datagram's own multi-value expansion already shared one timestamp
across many points long before this ADR, which is what both mechanisms were built for.

**Headers rebuilt per call, not held across an await, so the read future stays `Send`.** The
`mmsghdr`/`iovec` arrays `recvmmsg` needs are raw-pointer-bearing C structs; building them once and
reusing them across calls would mean holding raw pointers into a buffer across the `.await` inside
`socket.async_io(...)`, which is exactly the shape that forces an `unsafe impl
Send` or blocks compilation outright depending on how the pointers are held. Building the arrays
fresh inside the `async_io` closure on every call — pure CPU, no allocation the buffer reuse doesn't
already amortize — keeps the whole read path an ordinary `Send` future with no unsafe trait impl,
matching this codebase's existing raw-fd precedent (`crates/logit-inputs/src/tail/watch.rs`'s
`inotify` read: a `// SAFETY:` comment on the one `unsafe` block, no pointer held across an await).

The implementation took one step further than this paragraph originally
described, and the extra step is the load-bearing one. "Rebuilt per call" only settles *when* the
structs are written; the storage they are written *into* still has to live somewhere, and a
`Vec<libc::mmsghdr>` field on `BatchReader` would be `!Send` whether or not its contents are
refreshed each call — the struct outlives the `.await` regardless. So the backing storage is
`Vec<u64>`, plain integer words that the closure casts and writes the C structs into on each call
(`BatchReader::hdr_words`/`iov_words`), with a compile-time assertion that `u64`'s alignment is at
least the structs' own. That keeps the alloc-free reuse this paragraph claims *and* the `Send`-ness
it claims, which storing the structs themselves would have made mutually exclusive. A compile-time
`assert_send` on the read future pins the property next to the code that exists for it.

**One correction to the call itself:** the interest is `READABLE | ERROR`, not `READABLE` alone.
That is what `tokio::net::UdpSocket::recv_from` — the call this replaces — waits on internally, and
for a good reason: a socket with only a pending error queued is not "readable" to the poller, so an
arm registered for readability alone can fail to wake at all. Matching the interest the replaced
call used keeps the failure behaviour identical rather than subtly narrower.

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

### The coop-budget question a batched read raises, and why it needed no answer

`tokio` gives each task a cooperative-scheduling budget of 128 units per poll and every resource
operation spends one — the property the "Sampling cadence" section above already turns on. A batched
read changes the arithmetic underneath it: `read_loop` spends about three units per iteration (the
syscall plus its two `shutdown.wait_for` arms) whether that iteration returned one datagram or
sixty-four, so per *datagram* it now spends roughly forty times less budget than the `recv_from` loop
did. Since `read_loop` and `decode_loop` are driven by **one** task — `run_until_shutdown`'s two-arm
`select!`, deliberately, so the shutdown/drain ordering stays a local property — the obvious worry is
that the read arm now runs far longer before a coop-`Pending` hands the poll to the decode arm,
deepening the receive queue and inflating `logit.component.receive.latency`.

It does not, and the reason is that the same arithmetic applies to the other arm. `decode_loop`'s
per-iteration cost is also ~2 units per *batch* (one `pop_many`, one flush-deadline `Sleep`), because
W3 batched the pop for the same reason W4 batched the read. Both sides got roughly forty times
cheaper in budget per datagram at once, so their ratio — which is what fairness actually depends on —
is unchanged. The outer `select!` is not `biased`, so tokio polls its two arms in a random order each
time, which is what keeps a budget exhausted by one arm from systematically starving the other.

Measured rather than argued, since the reasoning above would be easy to get wrong: **queue drops are
zero in every run at every `read_batch` from 1 to 256**, so the queue never came close to its 10,000
bound; and `logit.component.receive.latency` did not regress — on `udp-statsd-small` it *improved*
(p50 64 µs → 55 µs, p99 ~190-410 µs → ~100-250 µs) and on `udp-statsd` it is unchanged within the
run-to-run spread (p50 ~0.35-0.59 ms before, ~0.29-0.80 ms after). **No fairness yield was added** —
no `consume_budget`/`yield_now` every N batches — because there is nothing in the measurements for
one to fix, and an unmotivated yield in the hottest loop in the read path is a cost with no benefit
behind it.

What the batched read *did* move is peak RSS on the two large-datagram scenarios: `udp-statsd-packed`
went from a tight 44.4-47.5 MiB across five repeats to 50.4-62.3 MiB. That is not the slab — the
sweep holds RSS flat across a sixteenfold change in slab size — it is simply more datagram bytes in
flight per turn of the loop. Worth recording, not worth acting on: the receive queue's own
`max_bytes` bound (32 MiB by default) is what an operator sizes this with, and it was never reached.

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

**And paired in one sitting, interleaved — not diffed against a stored baseline.** Pinning fixes
*which* cores a run lands on; it does nothing about what those cores are willing to do an hour
later. Measured during W2: the same specs, the same commit and the same pins, re-run 90 minutes
further into a benchmarking session, moved CPU µs/event by ~23% and `udp-statsd-small`'s drop rate
from 3.1% to 12.4%; re-running the *earlier* commit immediately afterwards reproduced the *later*
numbers, so it was the box settling into a lower sustained power state rather than anything in the
code. A drop rate is especially exposed to this, being the difference between two nearly-equal
rates. So a delta is a parent/branch pair taken back to back and interleaved (parent, branch,
parent, branch …) within one session, and a results file from another day is not a control.
`docs/plans/udp-intake.md`'s "Baseline/delta recording protocol" is the procedure; `logit-perf run`
records what it can of the box's power state into the results file and warns on `powersave` or
battery, because a number whose conditions aren't recorded can't be re-read later.

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
- **Sizing the read slots at 65,527 bytes (IPv6's maximum payload) rather than 65,507 (IPv4's).**
  Rejected. 65,507 is the bound every UDP listener in this codebase has always used, including the
  `recv_from` loop `recvmmsg` replaces, so an IPv6 datagram past it was already truncated silently
  and nothing about that behaviour changes here. Twenty extra bytes per slot is 20 x `read_batch` of
  address space to remove a case only a deliberately-jumbo IPv6 sender produces, and it would leave
  a `65_527` next to every other `65_507` in the codebase inviting the question forever. What the
  batched read does add for free is *visibility*: `recvmmsg` reports `MSG_TRUNC` in each message's
  `msg_flags`, in the same header the reader already reads `msg_len` out of, so a truncated datagram
  is counted as `logit.input.datagrams.truncated` instead of losing bytes with nothing to show for
  it. (Linux-only: `recv_from` gives a build on any other target no way to see it. And `msg_len`
  alone cannot detect it — it is the *copied* length, so it reads exactly 65,507 both for a
  truncated datagram and for one that happened to fit precisely.)
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
- Eight new metrics: `logit.input.kernel.drops`, `logit.input.receive_buffer.used.bytes`,
  `logit.input.receive_buffer.utilization`, `logit.input.accept_queue.depth`, `.limit`,
  `.utilization` (all W1), and `logit.input.reads` plus `logit.input.datagrams.truncated` (W4 —
  the denominator that turns `logit.input.datagrams` into a mean batch fill, and the IPv6-only
  oversize-datagram loss `recvmmsg`'s `MSG_TRUNC` makes visible) — see
  `docs/design/internal-telemetry.md`'s catalog.
- New config field `read_batch: usize` (default 64, confirmed by W4's sweep) on
  `ReceiveConfig`/`UdpListenerConfig`; new graph rule 57 (reject `read_batch > 1024`), alongside
  rule 18 (reject `0`) and rule 17 (queue-only, so rejected by name on a stream or tail listener).
  `script/schema` regenerated in that commit. `read_batch` also replaces W3's `DECODE_POP_BATCH`
  stand-in, so one setting governs both ends of the receive queue.
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
- The `SO_REUSEPORT`/`UDP_GRO` decision remains explicitly open, pending this plan's measurements —
  not assumed by anything built here. The `read_batch` default is no longer open: W4's sweep
  confirmed 64, and recorded that the flat part of the curve above ~16 is a property of the arrival
  pattern rather than of the knob.
- A question this work surfaced and did not answer: `read_loop` and `decode_loop` share one task, so
  they interleave but never run on two cores at once. The coop-budget section above shows that
  sharing is not currently costing anything measurable; whether *splitting* them would buy anything
  is a separate question, and one that overlaps with `SO_REUSEPORT`'s own redesign of
  `run_until_shutdown`'s two-arm select, the `&mut self.decoder` borrow and `Fanout` ownership.
  Recorded in `docs/known-gaps.md` next to the `SO_REUSEPORT` entry rather than designed here.

## As built

What follows is new information the implementation surfaced, not restated from the sections above —
read those first. Each bullet is something this record did not, or could not, say at design time.

- **`sockstat` landed exactly where "`sockstat` lives in `logit-pipeline`" above decided**, at
  `crates/logit-pipeline/src/sockstat.rs` — not `logit-core`, despite an early W1 PR description
  that named the wrong crate in passing. The code and the decision agree; only the prose of one PR
  body briefly didn't.
- **`DropCounter`'s first sample reports its absolute value**, not a zero baseline. `logit` opens
  every socket it samples itself, so a fresh socket's counter is genuinely zero at birth, and the
  drops accumulating between `Input::bind` and the run loop's first sample — while the process is
  still binding other listeners, with traffic already arriving at a socket nothing is reading yet —
  are real and would otherwise be silently absorbed into the first interval's baseline instead of
  counted.
- **`SockMeminfo::receive_utilization` can read slightly above 1.0, and that is not a bug.**
  `__udp_enqueue_schedule_skb` charges an arriving packet's `truesize` to `sk_rmem_alloc` and *then*
  compares the result against `sk_rcvbuf`, uncharging it again on the drop path — so a sample taken
  between those two steps sees the overshoot. A caller must not clamp it, and a test must not assert
  an upper bound of 1.0 against a real socket under load; values a little over 1.0 mean "saturated
  and dropping," which is exactly what they look like.
- **The accept-queue sampler is wired into all four accept loops that exist**, not described
  generically: the shared `crate::tcp` driver (`syslog_in`, `graphite_in`, TCP `statsd_in`, one call
  site), plus `logit_in`, `otlp_in`, and `prometheus_in`'s remote-write receiver, each of which keeps
  an accept loop of its own and needed its own `AcceptQueueSampler::accept()` call site.
- **The `push_many`/`not_empty` lost-wakeup fix this record's "`read_batch` larger than
  `receive.max_datagrams` is deliberately legal" section already describes was found in review, not
  at design time** — this ADR's first draft had `push_many` notify `not_empty` once, at the end of
  the call, and the parked-consumer-before-an-oversized-batch deadlock that shape allows was caught
  reading the code, not by the exhaustive push/pop-equivalence test (that test's own doc comment
  names its `Block` exclusion as why). Pinned by three new tests rather than one: a consumer parked
  first in `pop_many`/`pop`/`peek` against an over-capacity batch on both the item and byte bounds,
  the same case cancelled mid-wait, and a `Block` equivalence sweep (5 bound shapes × 4 batch
  sequences × both park orderings) — plus, end to end, W4's `overflow: block` /
  `max_datagrams: 4` / `read_batch: 64` / `batch_flush_interval: 0s` listener test, which fails
  against the pre-fix queue and passes against the merged one.

## Amendment: kernel- and tokio-cited facts behind the UDP read path (2026-09-21)

`libc/w1` of the raw-`libc` verification workstream
([`docs/plans/critical-sections-inventory.md`](../plans/critical-sections-inventory.md), NET-01 and
the UDP half of NET-12) re-derived this record's runtime claims from primary sources rather than
from reasoning about them. Most held; the ones that did not are corrected inline above, and the
facts worth having written down — because nothing in the code could state them, and a future reader
would otherwise have to rediscover them — are collected here.

Sources: `torvalds/linux` master (`net/socket.c`, `net/ipv4/udp.c`, `net/ipv6/udp.c`,
`net/core/datagram.c`, `net/ipv4/datagram.c`) and **tokio tag `tokio-1.53.1`**, the version pinned
in `Cargo.lock`. Everything below was checked against those two on 2026-09-21.

### `recvmmsg` cannot return `0`, so the spin the inventory feared does not exist

`do_recvmmsg` (`net/socket.c`) loops `while (datagrams < vlen)` and ends
`if (err == 0) return datagrams; if (datagrams == 0) return err;`. Every exit with `datagrams == 0`
returns a negative `err`; every exit with a count returns a positive one. `vlen` is clamped to at
least 1 twice over (`UdpListenerConfig::read_batch`, `BatchReader::new`), so `Ok(0)` is unreachable
by construction — not merely unobserved. A zero-*length* datagram is still a datagram:
`___sys_recvmsg` returns `0`, the `if (err < 0)` test is false, and `++datagrams` runs, which is
exactly the case `BatchReader::read_batch`'s doc already describes delivering as an empty `Bytes`.

### A mid-batch error is stashed and delivered one call late, and cannot drop the batch

Also in `do_recvmmsg`: after at least one datagram has been received, a non-`EAGAIN` error does not
discard them. The count is returned and the error is stashed —
`if (err != -EAGAIN) { WRITE_ONCE(sock->sk->sk_err, -err); }` — for the *next* call to pick up
through the `sock_error(sk)` check `do_recvmmsg` performs before its receive loop. So no
already-received batch is ever lost to a late error, which answers NET-01's third observed concern
directly. What is true, and was nowhere recorded, is that an errno can arrive one call after its
cause. For this call shape the only stashable mid-batch errors are `EFAULT`-class ones from the
copy-out, which require an invalid buffer. `recvmmsg(2)`'s own BUGS section adds that a stashed
code can be overwritten by an unrelated network event before it is read.

### Errno reachability on *this* socket, and why "everything but `EAGAIN` is fatal" is right

The listener socket is never `connect(2)`ed, never sets `IP_RECVERR`/`IPV6_RECVERR`, and never has
`shutdown(2)` called on it. Those three negatives are load-bearing and are now recorded at
`bind_one` itself. From them:

| errno | Reachable here? | Why |
|---|---|---|
| `EAGAIN`/`EWOULDBLOCK` | yes, constantly | the ordinary "nothing queued" answer; `async_io` clears readiness and waits |
| `ECONNREFUSED`, `EHOSTUNREACH`, `ENETUNREACH`, `EPROTO`, PMTU `EMSGSIZE` | **no** | `udp_err` (`net/ipv4/udp.c`; `udpv6_err` identically) sets `sk_err` only when `IP_RECVERR` is set or `sk_state == TCP_ESTABLISHED`, and `sk_state` becomes that only in `__ip4_datagram_connect` |
| `ENOBUFS`/`ENOMEM` | **no** | receive-buffer exhaustion is handled entirely on the softirq enqueue side: `__udp_enqueue_schedule_skb` returns `-ENOMEM` and its caller `__udp_queue_rcv_skb` bumps `UDP_MIB_RCVBUFERRORS`/`UDP_MIB_MEMERRORS` and drops the skb. Nothing surfaces to `recvmsg` — the strongest possible confirmation of this ADR's premise that `SO_MEMINFO` is the *only* way to see that loss |
| `ENOTCONN` | **no** | `__skb_wait_for_more_packets`'s `-ENOTCONN` is gated on `connection_based(sk)`, i.e. `SOCK_SEQPACKET \|\| SOCK_STREAM` (`net/core/datagram.c`) |
| `EINTR` | **no** | see below |
| `EMSGSIZE` (caller bug) | only if `msg_iovlen > UIO_MAXIOV`; this path passes 1 | `__copy_msghdr`, `net/socket.c` |
| `EBADF`, `ENOTSOCK`, `EINVAL`, `EFAULT` | only via a caller bug | permanent |
| `EPERM`, `EACCES`, `ENOSYS` | yes, under seccomp or an LSM | permanent; first call, deterministic |
| `ECONNABORTED` | **yes, externally triggerable** | `udp_abort` (`net/ipv4/udp.c`) sets `sk_err` and `__udp_disconnect`s, reached from a `SOCK_DESTROY` netlink request — i.e. `ss -K 'sport = :8125'` |

Every reachable non-`EAGAIN` entry is permanent, which is what makes the fatal policy correct
rather than merely convenient.

**`EINTR` is unreachable, and the retry arm stays anyway.** The only source is `sock_intr_errno`
inside `__skb_wait_for_more_packets`, which `__skb_recv_udp` reaches only through
`while (timeo && …)`. `timeo` is zero twice over here: `MSG_DONTWAIT` is passed explicitly, and
`____sys_recvmsg` ORs it in regardless for any `O_NONBLOCK` descriptor, which a tokio-registered
socket always is. The arm is kept — it is what `quinn-udp` keeps for the same call, it costs one
never-taken comparison per error, and it is the right behaviour the moment any of those
preconditions changes — but its comment now says so, instead of implying a signal could produce it.
NET-01's suggested verification ("send a signal to the reading thread") cannot work; forcing it
takes `strace -e inject=recvmmsg:error=EINTR`.

**`ss -K` killing a listener is intended behaviour, and a blanket retry must not be added.** The
socket really is destroyed and unhashed, so it will never receive again; failing loudly (process
exit code 2) is the honest outcome, and `describe_read_failure` now names the cause. The attractive
hardening — "retry once on an unexpected errno before declaring it fatal" — is specifically wrong
here: `sock_error`'s `xchg` clears `sk_err`, so the retry after a `udp_abort` returns `EAGAIN` and
the listener sits on a dead socket forever, silently. A loud fatal beats a silent zombie.

### Cancel-safety: the recv arm loses nothing; the push arm is where the loss is

`Registration::async_io` (`tokio/src/runtime/io/registration.rs`) has exactly two suspension
points — `self.readiness(interest).await?` and the `poll_fn(coop::poll_proceed).await` after it —
and **both are strictly before** it calls the closure; once the closure returns anything but
`WouldBlock` it returns in that same poll. So a `select!` that drops the read future either drops
it before the syscall or never: the datagrams are already in `out` by the time the future could be
dropped. This corrects two things. The "Sampling cadence" bullet above claimed that cancelling an
in-flight `recv_from` "silently drops the very datagram the sampler exists to observe" — true of
the **push** arm (`push_many`'s accepted prefix stays queued; the remainder is dropped uncounted,
bounded by `read_batch`, shutdown path only) and false of the **recv** arm. And the code's own
comment said `async_io` has "one `.await`"; it has two, and the second can genuinely return
`Pending` on coop exhaustion. Neither weakens the guarantee — both awaits are pre-syscall — but a
reader checking either statement against tokio would have found a discrepancy.

### The coop-budget argument, and exactly what a tokio bump must re-check

The conclusion in "Sampling cadence" stands, and the source makes it stronger; two of the three
mechanisms it named were wrong. Four facts, all at `tokio-1.53.1`, all of which a version bump
needs to re-verify:

1. `task/coop/mod.rs`: `const fn initial() -> Budget { Budget(Some(128)) }`.
2. `runtime/io/registration.rs`: a **successful** `async_io` spends one unit (`coop.made_progress()`
   on the success arm); a **`WouldBlock`** one spends **zero**, because the `RestoreOnPending` guard
   is dropped without `made_progress` and its `Drop` writes the pre-decrement budget back. The
   original text said "every resource operation spends one: each `recv_from`, and each
   `shutdown.wait_for`" — `watch::Receiver::wait_for` has no coop call on its path at all, and a
   `WouldBlock` read spends nothing. Under a flood every read succeeds, so the budget does drain;
   the conclusion is untouched, the mechanism was misdescribed.
3. `time/sleep.rs`: `poll_elapsed` runs `let coop = ready!(crate::task::coop::poll_proceed(cx));`
   before it consults the deadline or the timer entry — so a timer arm polled second finds a budget
   of zero, returns `Pending` however far past its deadline, and is never even registered with the
   timer driver.
4. `macros/select.rs`: the generated `poll_fn` begins `ready!(poll_budget_available(cx))`, gating
   on the budget **before any arm is polled at all**.

`the_sampler_keeps_ticking_while_the_read_future_burns_its_whole_coop_budget` remains the pin, and
is genuinely order-sensitive: with the arms reversed its `ticks` would be 0 against an assertion of
`>= 3` of 6 windows.

### The final sample runs on every path `sample_while` returns on — not on every path

Corrected in "Sampling cadence" above. The one path that skips it is the whole future being
*dropped*, which is what `run_input`'s grace backstop does when `shutdown_grace` expires
(`logit_pipeline::runtime`). Production does not reach it: `read_loop` races `shutdown` in both of
its own `select!`s and so returns within microseconds of the signal, long before a 5 s grace could
fire, and `input_runtime_config` supplies `ReceiveConfig::default()`'s 5 s for every listener —
including one whose config omits `receive:` entirely, since `ReceiveConfig` is itself
serde-defaulted. `InputRuntimeConfig::default()`'s `Duration::ZERO` is reached only from tests, and
there both arms are ready at once and `select!`'s rotation drops the listener roughly half the
time. `runtime.rs`'s own comment claimed the ZERO default was what production used; that comment is
corrected too.

### `msg_len` under truncation, and the clamp that is deliberately dead

`udp_recvmsg` (`net/ipv4/udp.c`; `udpv6_recvmsg` identically) sets `MSG_TRUNC` in the *output*
`msg_flags` exactly when `copied < ulen`, and returns
`err = copied; if (flags & MSG_TRUNC) err = ulen;` — the real length only when `MSG_TRUNC` was
passed as an **input** flag, which this code never does. So this ADR's claim that "`msg_len` alone
cannot detect it" is correct, and `read_batch`'s `.min(MAX_DATAGRAM_BYTES)` clamp is provably dead
code, kept as defence in depth. The `debug_assert_eq!` beside it is what keeps that honest, and
`an_oversized_ipv6_datagram_is_delivered_truncated_and_counted` drives the truncating case through
it in every test build. (`MSG_CTRUNC` is a different bit and cannot fire here — no cmsg options are
enabled — so the flag test cannot confuse the two. The `csum_copy_err` path clears `MSG_TRUNC`
before retrying, so a bad-checksum skb cannot leak a stale flag onto the next datagram.)

### `READABLE | ERROR` is correct, and the liveness precondition beside it

The wait mask matches what tokio's own `UdpSocket::recv_from` passes (`tokio/src/net/udp.rs`), and
the tokio#5550 hazard class does not apply: `ScheduledIo::clear_readiness` excludes only
`READ_CLOSED`/`WRITE_CLOSED` from what it clears, so `Ready::ERROR` *is* clearable. The real sharp
edge is `Ready::READ_CLOSED`, which `Ready::from_interest` adds to any readable interest and which
`clear_readiness` can never clear: were it ever set while the syscall kept returning `EAGAIN`,
`async_io` would loop inside a single poll and — since its `WouldBlock` arm restores the coop
budget — never yield, wedging the worker thread (open tokio issue #6971). It is unreachable only
because `EPOLLRDHUP`/`EPOLLHUP` on this socket come from `sk->sk_shutdown`, which nothing sets
without a `shutdown(2)` call on the fd. That is now written down at `bind_one` rather than left
implicit.

### Verification added, and what remains out of reach

`BatchReader::read_batch`'s closure is split into three named pieces — `build_headers`,
`recvmmsg_into`, `harvest_headers` — so that the two pure ones can be executed under `miri`, which
has no shim for `recvmmsg` and so can never run the closure as a whole. `mod batch_reader_helpers`
(six tests, `vlen ∈ {1, 2, 63, 64, 1024}`) covers slot disjointness and containment,
`msg_iovlen == 1`, NULL name/control, full re-initialization after a simulated kernel writeback, the
harvest's exact-`n` behaviour, and a write through each header's *own* stored `iov_base` pointer —
the provenance chain production depends on. It is in `MIRI_TARGETS` (`script/unsafe-check`) and in
ordinary CI. Alongside it, a `const` block asserts every layout fact the three functions rest on
(alignment, per-slot capacity, the `size_of`-is-a-multiple-of-`align_of` stride, and that both
harvested fields lie inside one header). The fatal path now has a test too — a readable non-socket
descriptor produces a real `ENOTSOCK` with no fault injection — and `describe_read_failure` gives it
a message naming the syscall, the bound address, and, for `ENOSYS`/`EPERM`, the fact that
`read_batch: 1` is not the workaround it looks like. What remains out of reach in CI: the injected
errno sequences (`script/unsafe-check inject`) and a real `SOCK_DESTROY`.

### Re-verify on a dependency bump

- **tokio**: the four coop facts above (budget 128; `async_io`'s two pre-closure awaits and its
  `WouldBlock`-restores-budget arm; `poll_elapsed`'s `poll_proceed`-before-deadline; `select!`'s
  `poll_budget_available`), `clear_readiness`'s exclusion list, `Ready::from_interest` adding
  `READ_CLOSED`, and that `recv_from` still uses `READABLE | ERROR`.
- **libc**: the `const` block in `crates/logit-inputs/src/udp.rs` is the tripwire — a change to
  `mmsghdr`/`iovec`'s size, alignment or field offsets on any supported target is a compile error,
  not a runtime surprise.
