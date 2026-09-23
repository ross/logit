# Load specs: driving a perf scenario over a real socket

A **driven** scenario receives its load over a real UDP socket instead of generating it in-process,
so it measures intake: the socket, datagram parsing, and kernel receive-buffer overflow. This
directory holds the load specs that `logit-perf` sends from ([ADR
`udp-intake-batching-and-socket-visibility`](../../docs/adr/udp-intake-batching-and-socket-visibility.md)).

Every other scenario under `perf/scenarios/` generates its events from a `generate_in` component
([ADR `load-test-harness`](../../docs/adr/load-test-harness.md)). That measures the graph and
deliberately measures nothing about *intake*: no socket is opened, no datagram is parsed, and no
kernel receive buffer can overflow. A driven scenario is an ordinary validating `logit` config with
no generator in it at all. `logit-perf` itself sends the load from the matching spec in **this**
directory:

```text
perf/scenarios/udp-statsd.yaml   ->   perf/load/udp-statsd.yaml   ->   perf/load/statsd-app.yaml
        the graph                          how hard to send it              what to send
```

The specs live in their own directory because both `script/validate` and
`crates/logit-cli/src/config.rs`'s `every_shipped_config_loads_and_validates` glob
`perf/scenarios/*.yaml` unconditionally. Any file there must be a `logit` config, or the build
fails. A future `syslog`/`graphite` UDP scenario is one new pair under the same layout, with no
change to `crates/logit-perf`: the spec format is protocol-agnostic apart from the wire syntax
inside `lines:`.

```sh
script/perf run --scenario udp-statsd --repeat 5 --pin-sender 0,1 --pin-child 2,3
script/perf run --scenario udp-statsd-small --verify         # the strict, zero-drop self-check
script/perf run --scenario udp-statsd --rate-scale 0.5       # a stable point below the drop knee
```

## The spec format

```yaml
target: statsd            # the component in the scenario whose `bind:` receives the load. Named,
                          # not repeated as an address, so the two files can't disagree on the port
# sink: out               # the component whose `events.received` is the run's denominator. Omitted
                          # by every scenario today: with one sink in the graph the harness finds it
                          # by its `role` stamp, and only a multi-sink graph has to say which counts
datagrams: 620000         # total datagrams to send. NOT the measurement's denominator -- that is
                          # events *delivered* -- but it is what sets how long the run takes
sockets: 8                # distinct connected sockets, i.e. distinct source ports ~ distinct clients
threads: 2                # OS threads the sockets are spread over; must be <= sockets
rate: 90000               # optional, datagrams/s across all threads. Omit for an unpaced blast
seed: 20260918            # seeds the ring's weighted choices: same spec, same bytes, every run
ring_datagrams: 8191      # how many distinct datagrams to pre-render, then cycle
model: statsd-app.yaml    # the shared line model, relative to this file

datagram_mix:             # weighted packing targets. Exactly one of `single`/`max_bytes` per entry
  - { weight: 45, single: true }     # one line per datagram -- an unbuffered client
  - { weight: 40, max_bytes: 1432 }  # packed to DogStatsD's own UDP default
  - { weight: 15, max_bytes: 8192 }  # packed local-agent style
```

`target`, `datagrams`, `model`, and `datagram_mix` are required. `sockets`, `threads`, `seed`, and
`ring_datagrams` default to the values shown, and `sink` and `rate` are optional. Unknown fields are
rejected.

The model file is a weighted list of line templates:

```yaml
lines:
  - weight: 150
    template: "app.http.requests.count:1|c|#env:prod,host:web-{seq%50},endpoint:/api/v1/orders/{seq%100},status:200"
```

`{seq%N}` is the only placeholder (`logit_core::template`, the same grammar `generate_in` uses). A
bare `{seq}` is **rejected**, because the receiver interns every distinct metric name. An unbounded
name would grow the interner for the length of the run, and the scenario would measure a leak
rather than a workload.

Each template has its own counter, so `{seq%N}` cycles 0..N-1 per *template*. Two placeholders in
one template therefore advance together: a `{seq%50}` host and a `{seq%100}` endpoint are
correlated, and a template's distinct-name count is the lcm of the moduli in its name. This
simplification is deliberate. What the receiver pays for is the number of distinct metric *names*
(interner pressure), the number of distinct tag *keys* (its `KeyCache`), and the line's length. None
of those depend on whether two tag values co-vary.

**Everything is pre-rendered before the blast starts.** `logit-perf` builds `ring_datagrams`
finished datagrams up front and then cycles them. At send time there is no formatting, allocation,
or weighted choice left: the sender is a `sendmmsg(2)` loop over immutable slices, and must never be
what a `udp-statsd` number measures. Ring sizes are prime so that cycling never falls into step with
`sockets`, `threads`, or the 64-datagram send batch. They're also large enough that every template
gets enough turns for its `{seq%N}` moduli to come round. `udp-statsd-small` needs four times the
ring of the other two, because a single-line ring holds one line per datagram rather than a dozen.

## Calibration: what came from the capture, and what didn't

The traffic model is calibrated against a **real client capture**, committed at
[`testdata/interop/statsd/`](../../testdata/interop/statsd/) and recorded by
`script/record-fixtures statsd`. It holds 56 UDP datagrams from Datadog's `datadog` 0.53.0
(`DogStatsd`) and the plain-statsd `statsd` 4.0.1 (`StatsClient`), each in a buffered and an
unbuffered mode, all four running one shared app-like workload. That directory's README has the
provenance table and how to re-record.

Part of the model is measured from that capture, and part is chosen. The two sections below keep
them apart.

### Measured from the capture

| Property | Capture | What the model does with it |
|---|---|---|
| Buffered DogStatsD datagram size | 1,341–1,418 B, 10–11 lines each | `max_bytes: 1432` is the client's own ceiling, and it never splits a line — so the model packs to a ceiling rather than to a line count |
| Buffered plain-statsd datagram size | 467–507 B, 7–8 lines each | Confirms a second, independent client cuts at *its* own buffer (512 B), not at an MTU — which is why `datagram_mix` is a weighted list rather than one number |
| Unbuffered datagram size | 95–165 B (tagged), 53–76 B (tagless) | `single: true` datagrams land in the same band without being told to; the model's line lengths are what put them there |
| Line length | 94–164 B tagged (median 139), 53–76 B tagless (median 61) | Model: 33–134 B, median 116 over all templates. Slightly shorter at the top end — the capture's longest lines carry a `\|c:in-<id>` container-id segment this model omits (see below) |
| Tags per line | 4–6, median 6 | Model: 3–8, median 6. Widened on purpose, to exercise the decoder's tag loop either side of what this one app happened to emit |
| Tag keys | `env`, `service`, `region`, `host`, `endpoint`, `status`, `queue`, `query`, `db` | The model uses these plus `tier`, `payment_method`, `priority`, `shard`, `dc`, `component`, `upstream`, `job`, `pool`, `node_role`, `az` — ~20 distinct keys, which is what exercises the decoder's `KeyCache` |
| Metric name length | 16–28 B tagged (median 23), 46–64 B tagless (median 55) | The model keeps that split: a tagged client's cardinality lives in tags and its names stay short; a tagless one has to put everything in the name |
| Both dialects are real | 2 of the 4 captures are tagless | The model is 80% tagged / 20% tagless |

Every capture figure in the table is over the whole corpus (all 65 tagged and all 55 tagless
lines), not over any one file; `testdata/interop/statsd/`'s README records the per-file medians that
differ. No weight in the model comes from a median. The packing targets come from the two clients'
buffer ceilings (1,432 B and 512 B), and the tag range from the tag counts (4–6). The line lengths
are an *output* of the templates, which the table compares against the capture, not an input taken
from it. Because the model mixes both dialects, its median line is ~20 B shorter than the tagged
capture's and ~55 B longer than the tagless one's.

### Chosen, not measured

- **The metric-type mix.** The model is counters 50%, timers+histograms+distributions 30%, gauges
  15%, and sets 5%. The capture's own mix across its 120 lines is `c` 43 / `ms` 22 / `g` 21 / `h`
  14 / `d` 13 / `s` 7, but **that's a property of the producer script**
  (`tools/record-fixtures/python_statsd_producer.py`, a synthetic app emitting three gauges and a
  set every fifth request), not of production traffic, and presenting it as calibration would be
  dishonest. The model's weights are the commonly cited shape of real statsd traffic: a reasoned
  choice that a real production corpus would either confirm or move.
- **Cardinality.** 50 hosts, 100 endpoints, about five statuses, and ~600 distinct metric names
  across the model. Same reason: the capture's own cardinality is whatever the producer script's
  topology was (8 tagged names, 18 tagless).
- **The 80/20 tagged/tagless split.** Both dialects are in the capture, but their 1:1 *ratio* there
  comes from the recording running two clients. It says nothing about how common each is.
- **The ≤8192 B packing target.** Nothing in the capture measures it, because neither client packs
  that large over UDP. It's in `udp-statsd`'s mix at the smallest weight because a local-agent-style
  sender on a loopback or UDS-adjacent path does pack that big. Leaving it out would mean nothing in
  the scenario family ever exercises a large datagram.
- **The sampled share (13%).** Both clients sample *client-side*: the call returns without sending
  at `1 - rate`. So the capture contains very few `|@0.1` lines by construction, which understates
  how often a real hot path is configured to sample. 13% makes sure the decoder's sample-rate
  extrapolation path is exercised.
- **No `|c:<container-id>` segment.** The captured DogStatsD lines carry one, because the client
  detected the recording container and volunteered it. It's in the capture (and asserted by
  `crates/logit-inputs/src/statsd.rs`'s interop tests) but deliberately not in the load model: it
  would add a constant ~11 B to 80% of lines on the strength of one recording environment's
  accident.

CI checks every rendered line against the real `StatsdDecoder`
(`crates/logit-perf/src/load.rs`'s `every_line_the_shipped_model_renders_decodes_cleanly`), and
every run re-checks it from the child's own telemetry (see [Self-checks](#self-checks)). This
matters because a model that rendered lines the decoder rejects would benchmark the malformed-line
path and look *fast* doing it, since a rejected line never becomes an event.

## The three scenarios

The three scenarios share one `model:` and differ only in `datagram_mix:`, the axis that decides
which half of the pipeline a number is about.

| Scenario | Packing | What it isolates |
|---|---|---|
| `udp-statsd` | 45% single / 40% ≤1432 B / 15% ≤8192 B | The headline. ~17.9 lines per datagram on average |
| `udp-statsd-small` | all single-line | The **syscall-bound worst case**: per-datagram fixed cost dominates a payload too small to amortize it, so this is where `recvmmsg`/batched gauge updates should show most |
| `udp-statsd-packed` | all ≤1432 B | The **decode-bound** end: ~13.5 metrics per syscall, so per-datagram costs are amortized away and the decoder and `BatchAccumulator` dominate |

A number from only one of them would mislead about which half of the pipeline a change helped, so
all three are reported together.

## Tuning

Each spec's `datagrams` and `rate`, and each scenario's `receive_buffer_bytes`, are set so that a
pinned release run takes **5–10 s** and the baseline has a **small, non-zero kernel drop rate**.
That's the regime a later improvement has room to move in. A zero-drop baseline couldn't show an
improvement, and a saturated 95%-drop baseline is a regime nobody deploys in. `rate` is set a few
percent above what the receiver sustains, per scenario, which both lengthens the run into the
target band and puts the drop rate where it's useful.

Rates are calibrated on the disposable perf VM (`docs/adr/disposable-azure-perf-vm.md`), the
project's reference box since 2026-09-20:

- `udp-statsd` and `udp-statsd-packed` were bisected there directly on 2026-09-20, on then-current
  `main`, to 2.31% and 3.97% kernel drop.
- `udp-statsd-small` keeps its original laptop-tuned rate. The automated calibration on the VM
  never found a drop rate above ~0% within the range it searched, meaning `recvmmsg`'s real
  capacity gain for this scenario exceeds what a 3× search ceiling can probe. That spec's own
  comment explains why, and `docs/design/performance.md` §7's per-binary capacity table has the
  numbers.

The first VM session (2026-09-18, `Standard_F4as_v6`) needed `--rate-scale` 0.49–0.83 against the
then laptop-tuned rates. Rates are hardware-specific: don't assume a rate calibrated on one box
carries over to another.

The unpaced extreme was measured on the original dev laptop when these specs were first tuned.
Pinned but **unpaced**, the sender outran the receiver by 4–30×: `udp-statsd` dropped 95.8%,
`udp-statsd-packed` 95.1%, and `udp-statsd-small` 76.2%, with wall times of 0.19–0.99 s. That's far
too short to measure and far too lossy to be a regime anybody runs in. The VM shows the same
qualitative shape (an unscaled `--rate-scale 1.0` still drops well above target on every scenario),
so calibration is necessary on the reference box too, not a laptop artifact.

**The drop rate is the tuning's sensitive number, not its robust one.** Because the specs are paced
a few percent above capacity, the drop rate is the *difference* between two nearly equal rates, and
amplifies anything that moves either one. This was first found on the laptop:

- A run taken while another build ran on the same machine turned `udp-statsd-small`'s 1.4% into
  35%.
- After ~90 minutes of continuous benchmarking, the same scenario went from 3.1% to 12.4% (CPU
  µs/event up ~23%) as the laptop-class CPU settled into a lower sustained power state. Re-running
  the earlier commit reproduced the later numbers.

The reference VM removes the governor, thermal, and battery half of this, but not all of it.
Last-level cache and memory bandwidth are still shared with other tenants on the physical host, and
a new `script/vm up` may land on different hardware entirely
(`docs/adr/disposable-azure-perf-vm.md`'s "Consequences" section). So two rules still apply:

- Take a baseline on a freshly provisioned, idle VM.
- **Take a baseline and the delta compared against it back to back in one sitting, interleaved**
  (parent, branch, parent, branch, and so on). A delta measured an hour after its baseline measures
  the machine as much as the change, and two unbroken blocks put one side on the cool half of the
  session and the other on the warm half. `compare` warns on a host or CPU-model mismatch for a
  related reason, but it can't see this one.

**`rate` stays fixed across a baseline/delta pair**, so the comparison isolates the change. Retune
it only when establishing a new baseline, which you must do whenever the receiver's own speed
changes: after a workstream like `udp-intake-batching-and-socket-visibility`, and after a change of
*box*, not just a change of code.

`receive_buffer_bytes` is `1MiB` in all three scenarios. Linux grants double what's requested and
clamps at `net.core.rmem_max`, which is **16 MiB by cloud-init default on the reference VM** (left at
that default, not clamped down to match any particular container the way an earlier session did).
So 1 MiB is requested, 2 MiB is granted, and nothing is clamped. If that sysctl differs on your
machine, re-tune. `logit.input.receive_buffer.bytes` in the run's own telemetry reports what was
actually granted.

### Box state

Before a run whose numbers you'll write down anywhere, check the reference VM:

| Check | Why | Where to look |
|---|---|---|
| Nothing else running on the VM | The whole reason the VM exists — no other tenant of *ours*, no concurrent `script/*` work | `ps aux --sort=-%cpu`, `docker ps` |
| Freshly provisioned this session, not left over from another | Last-level cache and memory bandwidth are still shared with other physical-host tenants; a fresh `up` is the closest thing to a controlled baseline | `script/vm status`'s "running for" |
| Thermal/host-maintenance headroom: gaps between repeats | Azure hosts support live migration/memory-preserving maintenance mid-session, which can freeze the guest briefly (inflates wall-clock, not CPU time) | check the scheduled-events endpoint before a long run: `curl -H Metadata:true 'http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01'` |
| Sender and child pinned to distinct physical cores | See "Pinning" below | `--pin-sender`/`--pin-child`, always |

`logit-perf run` still reads governor, EPP, platform profile, and AC power best-effort into the
results file's preamble (`box_state`). On the Azure guest, that's an **empty `{}`** in every result
file, because the guest exposes none of the `cpufreq`/`power_supply` sysfs nodes those checks read.
That isn't a gap to work around; it's the isolation the VM is for, with no governor to drift and no
battery to run down. The laptop-era checklist this section used to carry (AC power, `performance`
governor, energy-performance preference, ACPI platform profile, `grep MHz /proc/cpuinfo`) doesn't
apply to a VM that exposes none of those knobs.

## Self-checks

`logit-perf` checks every driven run before its numbers are believed
(`crates/logit-perf/src/run.rs`'s `self_check`):

0. **The kernel socket sampler reported at all.** `getsockopt(SO_MEMINFO)` needs Linux 4.12+ and a
   sandbox that permits it. Where it isn't available, W1's sampler disables itself for the process
   after one failed call and says so in a log line that carries no counter. `logit.input.kernel.drops`
   then reads as a flat zero, the accounting below can't close, and every repeat would fail blaming
   a `--settle` that was never the problem. The check looks for the *presence* of a
   `receive_buffer.*` gauge rather than its value, since each of those numbers is legitimately zero
   at times, and reports this failure as itself.
1. **`sent == received + kernel-dropped`, exactly.** On loopback a datagram either arrives or the
   kernel drops it: there's no lossy link, fragmentation, or middlebox. A mismatch means something
   the harness believes about the run is wrong, not that something interesting happened, and the
   run fails. The most common cause while this was being built was a settle too short for the
   receive queue to drain and the listener's final `SO_MEMINFO` sample to land. That's why `run.rs`
   applies a 3 s floor to `--settle` for a driven scenario.
2. **No decode diagnostics.** Any `logit.component.diagnostics{key=...}` on the listener, such as a
   `bad_line` or `bad_datagram`, fails the run.
3. **`--verify`**: the strict form. Zero kernel drops, zero queue drops, and a delivered event count
   *exactly* equal to what the ring says it sent. That isn't the same as the line count: a
   multi-value counter or gauge line (`a:1:2:3|c`) decodes to one event per value, which the ring
   accounts for per line. Because the shipped specs are paced deliberately *above* capacity,
   `--verify` quarters each spec's own `rate` for the run, so it runs against the specs as they ship
   without a hand-edited copy of each. A spec with no `rate:` at all is rejected rather than asked to
   be lossless.

### `--verify` and `--rate-scale` together

These are two knobs, and `--verify` only *defaults* one of them:

| Invocation | Pace | Asserts |
|---|---|---|
| (neither) | the spec's `rate` | accounting closes, no decode diagnostics |
| `--rate-scale 0.5` | half the spec's `rate` | the same |
| `--verify` | a **quarter** of the spec's `rate` | the above, plus zero drops and an exact delivered count |
| `--verify --rate-scale 1.0` | the spec's own `rate` | the same strict set, at the shipped pace |

An explicit `--rate-scale` replaces `--verify`'s 0.25 derate and **nothing else**; the exactness
assertion always stays on. So `--verify --rate-scale 1.0` asks "is this spec's own rate
loss-free?", and it's *expected to fail* whenever anything drops. That isn't a misuse of the flag:
the shipped rates are tuned to drop a little, so on a healthy box that combination should fail, and
a passing run means the receiver got faster.

## Pinning

Pinning isn't optional, though the reason changed with the reference box. On the original dev
laptop (heterogeneous Zen 5 performance and Zen 5c efficiency cores), an unpinned run landed on one
kind or the other by scheduler luck, making every number bimodal by roughly 2×. The reference VM's
cores are identical, so that failure mode is gone. Pinning still keeps the sender and the measured
child from contending for the same core's time, which would inflate both sides' numbers together
and make a delta harder to trust.

Every recorded `udp-statsd*` number states which CPUs it pinned to. The recorded baseline uses
`--pin-sender 0,1 --pin-child 2,3`: two cores for the sender and two for the child, disjoint from
each other, on the reference VM's 8-core `Standard_F8as_v6`. Cores 4–7 stay free, headroom the
earlier 4-core session didn't have. `--pin-child` is applied between `fork` and `exec`, so every
thread the child creates inherits the mask. Pinning after spawn would leave the threads created
during startup on whatever CPU the scheduler picked.

## Portability notes from the Azure perf-VM sessions

- **Rates are hardware-specific and must be recalibrated per box.** See [Tuning](#tuning) for what
  was calibrated where and the one spec (`udp-statsd-small`) still on its laptop value.
- **Verify each binary's sha256 before every run when measuring several refs.** Building several
  refs' binaries under one shared `CARGO_TARGET_DIR` (even from separate source trees extracted at
  nearly the same wall-clock time) once let cargo's mtime-based fingerprinting match a later ref's
  freshly extracted files against an earlier ref's build record. It silently reused the earlier
  binary under the later ref's label, which only a sha256 comparison caught. The build output's
  only tell was the reused build reporting `0.11s` and zero `Compiling` lines. `script/vm build` now
  builds git refs one at a time with sequential `git checkout`s in one clone, which keeps mtime
  fingerprinting honest, and gives each directory or tarball source its own `CARGO_TARGET_DIR`.
  `LOGIT_VM_TARGET_PER_REF` opts ref builds into a per-ref target directory too, and
  `script/vm build` refuses two sources that produce an identical binary unless given
  `--allow-identical`
  ([ADR `disposable-azure-perf-vm`](../../docs/adr/disposable-azure-perf-vm.md)'s "Multiple
  sources, one VM" section).
