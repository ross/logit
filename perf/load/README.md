# Load specs: driving a perf scenario over a real socket

Every scenario under `perf/scenarios/` used to generate its own events, in-process, from a
`generate_in` component ([ADR `load-test-harness`](../../docs/adr/load-test-harness.md)). That
measures the graph and deliberately measures nothing about *intake*: no socket is opened, no
datagram is parsed, no kernel receive buffer can overflow.

A **driven** scenario is the other kind ([ADR
`udp-intake-batching-and-socket-visibility`](../../docs/adr/udp-intake-batching-and-socket-visibility.md)).
It is an ordinary validating `logit` config with no generator in it at all, and the load arrives
over a real UDP socket, sent by `logit-perf` itself from the matching spec in **this** directory:

```
perf/scenarios/udp-statsd.yaml   ->   perf/load/udp-statsd.yaml   ->   perf/load/statsd-app.yaml
        the graph                          how hard to send it              what to send
```

This is a separate directory rather than more files under `perf/scenarios/` because both
`script/validate` and `crates/logit-cli/src/config.rs`'s `every_shipped_config_loads_and_validates`
glob `perf/scenarios/*.yaml` unconditionally — anything dropped in there is a `logit` config or it
is a build failure. A future `syslog`/`graphite` UDP scenario is one new pair under this same
layout, with no change to `crates/logit-perf`: the spec format is protocol-agnostic apart from the
wire syntax inside `lines:`.

```
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

The model file is a weighted list of line templates:

```yaml
lines:
  - weight: 150
    template: "app.http.requests.count:1|c|#env:prod,host:web-{seq%50},endpoint:/api/v1/orders/{seq%100},status:200"
```

`{seq%N}` is the only placeholder (`logit_core::template`, the same grammar `generate_in` uses). A
bare `{seq}` is **rejected**: the receiver interns every distinct metric name, so an unbounded one
would grow the interner for the length of the run and the scenario would be measuring a leak rather
than a workload.

Each template carries its own counter, so `{seq%N}` cycles 0..N-1 per *template*. Two placeholders
in one template therefore advance together — a `{seq%50}` host and a `{seq%100}` endpoint are
correlated, and a template's distinct-name count is the lcm of the moduli in its name. That is a
named simplification, not an oversight: what the receiver actually pays for is the number of
distinct metric *names* (interner pressure), the number of distinct tag *keys* (its `KeyCache`), and
the line's length. None of those depend on whether two tag values happen to co-vary.

**Everything is pre-rendered before the blast starts.** `ring_datagrams` finished datagrams are
built up front and then cycled, so at send time there is no formatting, no allocation and no
weighted choice left — the sender is a `sendmmsg(2)` loop over immutable slices and must never be
the thing a `udp-statsd` number is measuring. The ring sizes are prime so cycling never falls into
step with `sockets`, `threads` or the 64-datagram send batch, and are large enough that every
template gets enough turns for its `{seq%N}` moduli to come round (`udp-statsd-small` needs four
times the ring of the other two, because a single-line ring holds one line per datagram rather than
a dozen).

## Calibration: what came from the capture, and what didn't

The traffic model is calibrated against a **real client capture**, committed at
[`testdata/interop/statsd/`](../../testdata/interop/statsd/) and recorded by
`script/record-fixtures statsd`: 56 UDP datagrams from Datadog's `datadog` 0.53.0 (`DogStatsd`) and
the plain-statsd `statsd` 4.0.1 (`StatsClient`), each in a buffered and an unbuffered mode, all four
running one shared app-like workload. That directory's README has the provenance table and how to
re-record.

Be precise about which half of this model is measured and which is chosen.

### Measured from the capture

| Property | Capture | What the model does with it |
|---|---|---|
| Buffered DogStatsD datagram size | 1,341–1,418 B, 10–11 lines each | `max_bytes: 1432` is the client's own ceiling, and it never splits a line — so the model packs to a ceiling rather than to a line count |
| Buffered plain-statsd datagram size | 467–507 B, 7–8 lines each | Confirms a second, independent client cuts at *its* own buffer (512 B), not at an MTU — which is why `datagram_mix` is a weighted list rather than one number |
| Unbuffered datagram size | 95–165 B (tagged), 53–76 B (tagless) | `single: true` datagrams land in the same band without being told to; the model's line lengths are what put them there |
| Line length | 94–164 B tagged (median 139), 53–76 B tagless (median 61) | Model: 33–134 B, median 116 over all templates. Slightly shorter at the top end — the capture's longest lines carry a `\|c:in-<id>` container-id segment this model omits (see below) |
| Tags per line | 4–6, median 6 | Model: 3–8, median 6. Widened on purpose, to exercise the decoder's tag loop either side of what this one app happened to emit |
| Tag keys | `env`, `service`, `region`, `host`, `endpoint`, `status`, `queue`, `query`, `db` | The model uses these plus `tier`, `payment_method`, `priority`, `shard`, `dc`, `component`, `upstream`, `job`, `pool`, `node_role`, `az` — ~20 distinct keys, which is what exercises the decoder's `KeyCache` |
| Metric name length | 16–28 B tagged (median 23), 46–64 B tagless (median 56) | The model keeps that split: a tagged client's cardinality lives in tags and its names stay short; a tagless one has to put everything in the name |
| Both dialects are real | 2 of the 4 captures are tagless | The model is 80% tagged / 20% tagless |

Every capture figure in that table is over the whole corpus — all 65 tagged and all 55 tagless
lines — not over any one file. An earlier revision quoted two per-file medians (137 B and 60 B, from
the buffered DogStatsD and pipelined plain-statsd captures respectively) as if they were corpus-wide;
the corpus-wide figures are 139 B and 61 B. **Nothing in the model moves as a result**: no weight
here was derived from a median. The packing targets come from the two clients' buffer ceilings
(1,432 B and 512 B, unchanged), the tag range from the tag counts (4–6, unchanged), and the line
lengths are an *output* of the templates that the table compares against the capture rather than an
input taken from it. The comparison reads the same either way — the model's median line is ~20 B
shorter than the tagged capture's and ~55 B longer than the tagless one's, because it mixes both.

### Chosen, not measured

- **The metric-type mix.** The model is counters 50%, timers+histograms+distributions 30%, gauges
  15%, sets 5%. The capture's own mix across its 120 lines is `c` 43 / `ms` 22 / `g` 21 / `h` 14 /
  `d` 13 / `s` 7 — but **that is a property of the producer script**
  (`tools/record-fixtures/python_statsd_producer.py`, a synthetic app emitting three gauges and a
  set every fifth request), not of production traffic, and it would be dishonest to present it as
  calibration. The weights above are the commonly-cited shape of real statsd traffic; they are a
  reasoned choice, and a real production corpus would be the thing that either confirms or moves
  them.
- **Cardinality.** 50 hosts, 100 endpoints, 5-ish statuses, ~600 distinct metric names across the
  model. Same reason: the capture's own cardinality is whatever the producer script's topology was
  (8 tagged names, 18 tagless).
- **The 80/20 tagged/tagless split.** Both dialects are in the capture; their *ratio* there is
  1:1 because the recording runs two clients, which says nothing about how common each is.
- **The ≤8192 B packing target.** Nothing in the capture measures it — neither client packs that
  large over UDP. It is in `udp-statsd`'s mix at the smallest weight because a local-agent-style
  sender on a loopback or UDS-adjacent path genuinely does pack that big, and leaving it out
  entirely would mean nothing in the whole scenario family ever exercises a large datagram.
- **The sampled share (13%).** Both clients sample *client-side* — the call returns without sending
  at `1 - rate` — so the capture contains very few `|@0.1` lines by construction, which understates
  how often a real hot path is configured to sample. 13% is a share chosen to make sure the
  decoder's sample-rate extrapolation path is exercised.
- **No `|c:<container-id>` segment.** The captured DogStatsD lines carry one, because the client
  detected the recording container and volunteered it. It is in the capture (and asserted by
  `crates/logit-inputs/src/statsd.rs`'s interop tests) but deliberately not in the load model: it
  would add a constant ~11 B to 80% of lines on the strength of one recording environment's
  accident.

Every rendered line is checked against the real `StatsdDecoder` in CI
(`crates/logit-perf/src/load.rs`'s `every_line_the_shipped_model_renders_decodes_cleanly`), and
every run re-checks it from the child's own telemetry — see "Self-checks" below. A model that
rendered lines the decoder rejects would benchmark the malformed-line path and look *fast* doing
it, since a rejected line never becomes an event.

## The three scenarios

They share one `model:` and differ only in `datagram_mix:`, which is the axis that decides which
half of the pipeline a number is about.

| Scenario | Packing | What it isolates |
|---|---|---|
| `udp-statsd` | 45% single / 40% ≤1432 B / 15% ≤8192 B | The headline. ~17.9 lines per datagram on average |
| `udp-statsd-small` | all single-line | The **syscall-bound worst case**: per-datagram fixed cost dominates a payload too small to amortize it, so this is where `recvmmsg`/batched gauge updates should show most |
| `udp-statsd-packed` | all ≤1432 B | The **decode-bound** end: ~13.5 metrics per syscall, so per-datagram costs are amortized away and the decoder and `BatchAccumulator` dominate |

A number from only one of them would mislead about which half of the pipeline a change actually
helped, which is why all three are reported together.

## Tuning

Each spec's `datagrams` and `rate`, and each scenario's `receive_buffer_bytes`, are set so a pinned
release run takes **5–10 s** and the baseline sits in a regime with a **small, non-zero kernel drop
rate** — the regime a later improvement has somewhere to move. A zero-drop baseline could not show
one, and a saturated 95%-drop baseline is a regime nobody deploys in.

The other extreme was measured before settling here. Pinned but **unpaced**, the sender outruns the
receiver by 4–30×: `udp-statsd` dropped 95.8%, `udp-statsd-packed` 95.1% and `udp-statsd-small`
76.2%, with wall times of 0.19–0.99 s — far too short to measure and far too lossy to be a regime
anybody runs in. `rate` is therefore set a few percent above what the receiver sustains, per
scenario, which both lengthens the run into the target band and puts the drop rate where it's
useful.

**The drop rate is the tuning's sensitive number, not its robust one.** These specs are paced a few
percent above capacity, so the drop rate is the *difference* between two nearly-equal rates and
amplifies anything that moves either of them. Two measured examples, same specs, same commit, same
pins:

- A run taken while another build was going on the same machine turned `udp-statsd-small`'s 1.4%
  into 35%.
- After ~90 minutes of continuous benchmarking, the same scenario went from 3.1% to 12.4% and
  0.45% to 2.2% on `udp-statsd`, with CPU µs/event up ~23% — on a laptop-class part settling into a
  lower sustained power state. Checked, not assumed: re-running the *earlier* commit right
  afterwards reproduced the *later* numbers (0.677 vs 0.678 µs/event, 13.2% vs 12.4%), so it is the
  box that moved, not the code.

Two rules follow. Take a baseline on an idle, rested box; and **take a baseline and the delta it is
compared against back to back in one sitting, interleaved** (parent, branch, parent, branch …) — a
delta measured an hour after its baseline is measuring the machine as much as the change, and two
unbroken blocks put one side on the cool half of the session and the other on the warm half.
`compare` warns on a host/CPU-model mismatch for a related reason, but it cannot see this one.

### Box state

Before a run whose numbers are going to be written down anywhere:

| Check | Why | Where to look |
|---|---|---|
| On AC, not battery | Every absolute number is depressed on battery, and drifts as the run goes on | `/sys/class/power_supply/*/online` for the supply whose `type` is `Mains` (`ACAD` on this box, not `AC`) |
| Governor is `performance`, not `powersave` | The single largest source of unexplained movement here | `/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor` |
| Energy-performance preference is not a power-saving one | The finer knob under the governor | `…/cpu0/cpufreq/energy_performance_preference` |
| Platform profile is not `low-power`, where the machine has one | Firmware-level cap under everything above | `/sys/firmware/acpi/platform_profile` — **absent on this box**, so don't expect it |
| Thermal headroom: rested box, gaps between repeats, watch for a frequency drop | The 90-minute drift above is exactly this | `grep MHz /proc/cpuinfo` before and after |
| Nothing else building | A concurrent `cargo build` turned 1.4% into 35% | `uptime`, `ps aux --sort=-%cpu` |
| Sender and child pinned to distinct **fast physical** cores | See "Pinning" below | `lscpu -e`'s `MAXMHZ` column |

`logit-perf run` reads the first four best-effort and records them in the results file's preamble
(`box_state`), printing a warning before the first scenario when the governor is `powersave` or the
box is on battery — early enough to stop and fix it rather than discover it in the JSON afterwards.
Anything it can't see is recorded as `null`: inside this repo's dev container the two `cpufreq`
files and the mains supply are visible, and the ACPI platform profile is not.

`receive_buffer_bytes: 1MiB` on all three. Linux grants double what is requested, and clamps at
`net.core.rmem_max` — **4 MiB in this dev container**, so 1 MiB is requested, 2 MiB is granted, and
nothing is clamped. Re-tune if that sysctl differs on the machine you are running on;
`logit.input.receive_buffer.bytes` in the run's own telemetry reports what was actually granted.

Retune whenever the receiver's own speed changes — which is exactly what the workstreams after this
one do. The rule is: **`rate` stays fixed across a baseline/delta pair**, so the comparison isolates
the change; it moves only when a new baseline is being established.

## Self-checks

Every driven run is checked before its numbers are believed (`crates/logit-perf/src/run.rs`'s
`self_check`):

0. **The kernel socket sampler reported at all.** `getsockopt(SO_MEMINFO)` needs Linux 4.12+ and a
   sandbox that permits it; where it isn't available W1's sampler disables itself for the process
   after one failed call and says so through a log line that carries no counter. Then
   `logit.input.kernel.drops` reads as a flat zero, the accounting below cannot close, and every
   repeat would fail blaming a `--settle` that was never the problem. Detected by the *presence* of
   a `receive_buffer.*` gauge rather than its value — each of those numbers is legitimately zero at
   times — and reported as itself.
1. **`sent == received + kernel-dropped`, exactly.** On loopback a datagram either arrives or the
   kernel drops it — there is no lossy link, no fragmentation, no middlebox. A mismatch means
   something the harness believes about the run is wrong, not that something interesting happened,
   and the run fails. (The most common cause while this was being built: a settle too short for the
   receive queue to drain and the listener's final `SO_MEMINFO` sample to land. Hence the 3 s floor
   `run.rs` applies to `--settle` for a driven scenario.)
2. **No decode diagnostics.** Any `logit.component.diagnostics{key=...}` on the listener — a
   `bad_line`, a `bad_datagram` — fails the run.
3. **`--verify`**: the strict form. Zero kernel drops, zero queue drops, and the delivered event
   count equal to what the ring says it sent, *exactly*. Note that is not the same as the line
   count: a multi-value counter or gauge line (`a:1:2:3|c`) decodes to one event per value, which
   the ring accounts for per line. Since the shipped specs are paced deliberately *above* capacity,
   `--verify` quarters each spec's own `rate` for the run — so it is runnable against the specs as
   they ship rather than needing a hand-edited copy of each. A spec with no `rate:` at all is
   rejected rather than asked to be lossless.

### `--verify` and `--rate-scale` together

They are two knobs, and `--verify` only *defaults* one of them:

| Invocation | Pace | Asserts |
|---|---|---|
| (neither) | the spec's `rate` | accounting closes, no decode diagnostics |
| `--rate-scale 0.5` | half the spec's `rate` | the same |
| `--verify` | a **quarter** of the spec's `rate` | the above, plus zero drops and an exact delivered count |
| `--verify --rate-scale 1.0` | the spec's own `rate` | the same strict set, at the shipped pace |

An explicit `--rate-scale` replaces `--verify`'s 0.25 derate and **nothing else** — the exactness
assertion always stays on. So `--verify --rate-scale 1.0` is the way to ask "is this spec's own rate
loss-free?", and it is *expected to fail* whenever anything drops. That is the question it answers,
not a misuse of the flag: the shipped rates are tuned to drop a little, so on a healthy box that
combination should fail, and a run of it that passes means the receiver got faster.

## Pinning

Not optional here. This dev box has heterogeneous cores (Zen 5 performance vs. Zen 5c efficiency),
and an unpinned run lands on one kind or the other by scheduler luck, making every number bimodal
by roughly 2×. `lscpu -e`'s `MAXMHZ` column is what tells them apart — on this box CPUs 0–3 and
12–15 are the 5,158 MHz cores (four physical, plus their SMT siblings) and 4–11/16–23 are the
3,289 MHz ones.

Every recorded `udp-statsd*` number states which CPUs it pinned to. The recorded baseline uses
`--pin-sender 0,1 --pin-child 2,3`: four distinct fast *physical* cores, sender and child disjoint,
so neither is ever competing with the other for a core and neither lands on an efficiency core.
`--pin-child` is applied between `fork` and `exec`, so every thread the child ever creates inherits
the mask — pinning after spawn would leave the threads created during startup on whatever CPU the
scheduler picked.

## Portability notes from the Azure perf-VM session

- **Rates are hardware-specific and must be recalibrated per box.** The shipped specs' rates are
  tuned against this dev laptop; an `Standard_F4as_v6` Azure VM's cores needed `--rate-scale`
  0.49–0.83 (roughly half, and non-uniformly across scenarios) to reach the same 1–5% calibration
  target this README's "Tuning" section describes. Don't assume a rate that's calibrated on one box
  carries over to another — retune whenever the receiver's own speed changes, and that includes a
  change of *box*, not just a change of code.
- **Give each ref its own `CARGO_TARGET_DIR` when building several for one comparison.** Building
  multiple refs' binaries under one shared target directory (even across separate source trees
  extracted at nearly the same wall-clock time) let cargo's mtime-based fingerprinting falsely match
  a later ref's freshly-extracted files against an earlier ref's build record, silently reusing the
  earlier binary under the later ref's label — caught only by comparing sha256 checksums, not by
  build output (the reused build reported `0.11s` and zero `Compiling` lines, which is itself a
  tell). Use a distinct `CARGO_TARGET_DIR` per ref, and verify each binary's checksum before every
  run, not just once at the start of a session.
