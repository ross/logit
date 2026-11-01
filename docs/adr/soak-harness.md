---
created: 2026-10-08
updated: 2026-10-31
---

# A soak harness: real containers over ordinary config, one-shot netem, and a reset-aware ledger as the loss oracle

## Status
Accepted

## Context

`logit` has harnesses for throughput (`script/perf`,
[ADR `load-test-harness`](load-test-harness.md)), interop (`script/victoria-interop`,
`script/splunk-interop`, [ADR `victoriametrics-interop`](victoriametrics-interop.md)), and data
shape (`script/shape-survey`). None runs `logit` for long under faults. Three soaks were
designed and never run: stopping a sink for 90 s
([`buffered-sink-delivery`](buffered-sink-delivery.md)), a `kill -9` with a disk spool
and a replay check ([`disk-backed-sink-buffer`](disk-backed-sink-buffer.md)), and flooding
`statsd_in` while its sink is stopped
([`decoupled-listener-io`](decoupled-listener-io.md)). `tc netem` appears in the repo
only as a hand-run latency measurement
([`docs/design/performance.md`](../design/performance.md) §1, "`native-relay`: window 32 against
window 1 under a 10 ms round trip").

The goal is robustness: a repeatable way to run real `logit` containers against real or
generated sources and real targets, inject network and lifecycle faults on a timeline, and
report crashes, hangs, memory and file-descriptor growth, and **uncounted** data loss. Counted
loss is a policy outcome (`drop_oldest`, a rejected batch, a shutdown drop); uncounted loss is a
bug. The first scenario is a generated statsd stream into `logit` and out to VictoriaMetrics.

Four facts about the running system shape the design:

- `docker compose logs` merges a container's stdout and stderr, and `docker logs` keeps them
  apart across every life of a container that is stopped and started but never recreated.
- `internal`'s final drain runs when the shutdown signal fires, and no drain exports what
  happens after it ([`internal-telemetry.md`](../design/internal-telemetry.md), "`internal`: the
  drain", and the `shutdown` row of "UDP listeners: `ReceiveQueue` and the kernel socket"):
  - the listener decodes what its receive queue still holds and flushes its accumulator;
  - `aggregate` absorbs those events and flushes its close-time window, which still reaches the
    backend;
  - the sink makes its last delivery;
  - `datagrams.dropped{reason="shutdown"}` and a sink's `batches.dropped{reason="shutdown"}` are
    counted. These reach stderr only, as the listener's `warn` lines and the `drain complete`
    line's `batches_dropped`.

  So per-hop counts are exact only for a process life whose input stopped before the signal:
  the final life, after the generator stops and the backend's total holds. A life that ends
  under load leaves a bounded residual that no drain reports.
- `/readyz` stays `200 ok` while a sink retries and holds its queue
  ([`deploying.md`](../deploying.md), "Probes and exit codes"), so readiness can't detect a sink
  that stops making progress.
- Some UDP loss is uncounted by design: datagrams lost on the network or to netem, the kernel
  receive queue at the instant a socket closes, and the counts of a datagram send cancelled
  mid-batch ([`docs/known-gaps/intake.md`](../known-gaps/intake.md), "UDP intake").

## Decision

**The harness runs the real release image in compose over ordinary YAML configs, outside
`script/cibuild`.**

- `script/soak` builds the image from the working tree, validates each scenario's `logit`
  configs inside that image, and runs the driver. No test depends on it running, as with the
  other harnesses.
- A scenario is a directory under `tools/soak/scenarios/`: a `scenario.toml` timeline beside
  ordinary `logit` YAML configs, one per `logit` service. The configs join `script/validate` and
  `every_shipped_config_loads_and_validates`.
- The timeline is TOML because the driver reads it with Python's standard-library `tomllib`, and
  the standard library has no YAML parser. The `logit` configs stay YAML because they are `logit`
  configs.
- The driver is standard-library Python (3.11 or later) on the host, not in a container, because
  it drives the Docker daemon: lifecycle actions, one-shot fault containers, `docker inspect`,
  `docker stats`, and `docker logs`. It uses `$DOCKER` as the other harnesses do.
- Each scenario runs as compose project `soak-<scenario>`, so two scenarios can run at once on one
  daemon. The driver refuses a project that already has containers and never prunes.

**Network faults are one-shot `tc netem` runs; lifecycle faults are Docker actions.**

- Each netem change is a `docker run --rm --network container:<id> --cap-add NET_ADMIN` of a small
  iproute2 image that applies, clears, or shows a root qdisc on the target's default-route
  interface and exits. The qdisc outlives the `tc` process.
- A root qdisc shapes the target container's egress only. Netem on the `logit` under test impairs
  its sends to the backend, not the traffic arriving at it.
- Lifecycle faults are `pause`, `stop`, `restart`, a network `partition` (disconnect, then
  reconnect with the service aliases recorded before the disconnect, because a reconnect drops
  them), and a `kill` of the SUT with SIGKILL. A `kill` tests what the process left on disk,
  such as a disk spool, because nothing in the process runs after the signal.
- Every fault has a duration, so its revert is scheduled with it.

**The load source is a second `logit`: `generate_in` into `statsd_out` over UDP, one line per
datagram.**

- No new Rust, and the generator reports what it sent through the same telemetry the system under
  test (SUT) uses.
- `max_packet_bytes` is set so each datagram carries one line, which makes a datagram the ledger's
  unit at every hop. The sender's count is `logit.output.messages`, what the kernel took, not
  `events.sent`, because while the SUT is down `statsd_out` fails each batch's name resolution as
  `Clean` and sends nothing.
- The trade-offs: packing is uniform, every datagram leaves one source port, and the generator
  shares the SUT's binary, so a bug in shared code can hide on both sides. That is acceptable for
  fault testing, where the question is what `logit` does to traffic under faults, not how it
  handles a varied client population. Recorded-client traffic is the interop harnesses' job.

**The oracle is `logit`'s own telemetry plus the backend's stored totals, and the boundary of
"uncounted by design" is stated, not judged.**

- Every `logit` container writes `internal` telemetry to stdout as NDJSON (`stdio_out`,
  `format: json`) and its self-log to stderr (`--log-format json`). The driver collects both
  through `docker logs`, kept separate.
- Counters sum per process life, split where `logit.process.uptime` decreases.
- The egress oracle is the backend's cumulative totals. `aggregate` runs
  `temporality: cumulative`, so a duplicate or a lost-then-retried remote-write batch changes
  nothing, and egress loss shows only as a lost final total. Each SUT restart resets the series,
  so the total is summed reset-aware: the first value, plus each non-negative step, plus the new
  value after each decrease or SUT restart, since a restart whose first total isn't below the
  last one shows no decrease. `series_retention` is raised so a fault never evicts a series.
  An external target, such as Datadog, has no backend query: the ledger ends at the sink's
  telemetry, and a `2xx` is the end of the evidence.
- Loss the system can't count by design is reported and never fails a run: UDP loss between the
  generator and the SUT, the kernel queue at socket close, and a cancelled generator send, each
  linked to its entry in [`docs/known-gaps/intake.md`](../known-gaps/intake.md). The exception is
  wire loss outside every window that affects UDP ingress, which fails above the scenario's
  threshold.
- Shutdown-time drops are read from stderr, and the sink conservation identity is checked at the
  last quiet telemetry drain before shutdown, not at exit.
- The per-hop ledger is judged with no tolerance for the final SUT life only. A life that ends
  under load is compared at its last drain and judged within a bound: what the receive queue,
  one accumulator batch, and `aggregate`'s inbox can hold at the signal.
- A hang is the absence of sink progress (`batches.delivered` flat with work queued) outside fault
  windows and their recovery bound, not a readiness failure.

The plan, [`docs/plans/soak-harness.md`](../plans/soak-harness.md), holds the scenario schema,
the compose layout, the driver loop, the full list of checks, and the workstreams.

## Alternatives considered

- **toxiproxy.** TCP only, so it can't impair the statsd leg, and it adds a hop to the data path
  that changes connection behavior. A candidate later for TCP-level faults (partial writes,
  slow close) that netem can't express.
- **pumba.** A wrapper around the same netem and Docker actions, adding a dependency and its own
  scheduling where the driver already keeps the timeline.
- **A long-lived netem sidecar sharing the target's namespace (`network_mode: service:...`).** It
  keeps the old namespace across a `stop` and `start` of the target and loses its interface on a
  network disconnect, so it fails during the faults it exists to apply.
- **`logit-perf` as the sender.** Its UDP sender is finite, reads its target from a scenario
  file, stops through the harness's own channel, and reports nothing about what it sent. Making it
  a standalone paced sender with telemetry is a Rust change and an image before any scenario runs.
- **A Python producer.** It would need its own counting and pacing, and its sent count would
  share no unit or code path with the SUT's telemetry.
- **A checker container for VictoriaMetrics queries.** One more image and lifecycle to manage; a
  loopback-only ephemeral published port lets the host driver query directly.
- **Scraping `prometheus_out` exposition for telemetry.** A scrape returns current totals only and
  stops when the process stops; stdout NDJSON keeps every drain, survives a stopped container,
  and reuses the grammar other harnesses already read.

## Consequences

- A new `tools/soak/` (driver package, compose file, netem image, scenarios) and `script/soak`.
  `tools/soak/` reads the NDJSON grammar, so `crates/logit-outputs/src/ndjson.rs`'s module doc
  gains it in its list of readers that mirror the grammar.
- `script/validate` and `every_shipped_config_loads_and_validates`
  (`crates/logit-cli/src/config.rs`) gain the `tools/soak/scenarios/*/` configs.
- The host needs Python 3.11 or later for `tomllib`. `Dockerfile.dev` has no Python, so the
  driver's self-test runs at the start of every `run` and by hand, not in CI. Only the config
  globs are checked in CI.
- Shared-daemon etiquette: a fixed project name per scenario, refusal of a live project, and no
  prune.
- The ledger's "uncounted by design" rows stay reported until the gaps in
  `docs/known-gaps/intake.md` close. Closing one turns its row into a judged check.

## Amendment: shutdown drops come from telemetry (2026-10-11)

A dedicated `internal` pipeline now drains last at shutdown
([ADR `shutdown-accounting-and-cancellation-safety`](shutdown-accounting-and-cancellation-safety.md),
"Amendment: internal drains last"), and every SUT config has one (`self -> telemetry`). Its
final drain runs after every data node has exited, so it exports the listener's
`datagrams.dropped{reason="shutdown"}` and each node's `batches.dropped{reason="shutdown"}`. The
Context bullet on what no drain exports, and the Decision's "Shutdown-time drops are read from
stderr", no longer hold for those counts:

- D and the counted verdict of `ledger.egress` read telemetry alone. Adding the stderr copy as
  well counts each shutdown drop twice.
- The listener's `warn` lines and the `drain complete` line's `batches_dropped` stay as a
  cross-check: a life whose stderr count exceeds telemetry's WARNs. A telemetry count above
  stderr's does not, because under `receive: { overflow: block }` a `push_many` cancelled while
  blocked counts its datagrams as shutdown drops without logging a line. `drain complete` also
  counts the `internal` pipeline's own shutdown drops, which no drain exports.
- A SUT config that sends `self` into a pipeline another input also reaches has one shutdown
  stage, so its shutdown drops miss telemetry. The cross-check WARNs on it, and the ledger reads
  its losses as uncounted.
- `identity.sink` stays at the last quiet drain before the final one, with its one-batch
  allowance. The per-hop rows stay exact for the final SUT life only, and an earlier stopped
  life keeps its [0, R] residual bound.

## Amendment: every drop reason counts a gap (2026-10-11)

`ledger.egress` credits a positive gap with a node's `batches.dropped` under every reason, not
`shutdown` alone. The plan's [6. The checks](../plans/soak-harness.md#6-the-checks-w1a-w1b), under
"The ledger and identities", is the one list of what counts. The count is of batches and is never
reconciled with the gap. A `rejected` batch, which the destination refused, or a `disk_corrupt`
one, which a spool couldn't read back, counts the gap but FAILs the life, because neither happens
against a healthy backend and a later cumulative total can hide the lost increments. The stderr
cross-check stays `shutdown`-only, since stderr carries no other reason.

## Amendment: a Kafka target's oracle is its topic (2026-10-11)

A scenario can name the stack's own Kafka broker as its target (`[target] kind = "kafka"`). The
egress oracle is then the topic, read back with the broker image's own tools after the SUT stops,
not a stored total. The SUT's `aggregate` runs `temporality: delta`, so V is the sum of the
records' increments and a lost record shows as a deficit. A duplicate would show as a surplus
that can cancel a loss of the same size, so the harness deduplicates records on the value's raw
bytes before summing them, and `ledger.egress` FAILs any surplus left. `ledger.topic` compares
the topic's distinct record count with the records the sink saw accepted, and judges the
duplicate copies on their own: one FAILs in a SUT life with no producer epoch bump, because the
broker discards a resend within an epoch, and WARNs after a bump, as `at_least_once` permits.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "Kafka targets", has the rules.

## Amendment: an `otlp_proto` topic is read per data point (2026-10-18)

A Kafka target's topic can hold `kafka_out`'s `format: otlp_proto` records. The harness takes
the format from the sink's own config rather than a scenario key, so the two can't disagree.
The broker image's console consumer prints a binary value through Kafka's `BytesDeserializer`,
whose escaped form keeps each record on one line, and a stdlib protobuf walker decodes it, so
the harness stays stdlib-only. One record holds a whole batch, so records stay the unit of T
and of duplicate copies, which `logit.output.records` counts, but V counts each `Sum` data
point once per series and time: a resend that splits a batch another way stores different
bytes holding the same points. `ledger.topic` judges such a repeated point as it judges a
duplicate copy. The walker is held to a vector that a Rust test pins to the encoder's output,
not to a hand-written encoding.

## Amendment: a stand-in's oracle is its record, and a cued rejection is counted (2026-10-15)

A scenario can point its sink at a scripted HTTP stand-in (`[target] kind = "standin"`), so every
row of the sink's response-class table runs under load. The stand-in is stdlib Python in a stock
image, cued per step by the driver, and it prints each request it answers to stdout, raw body
included. That record is the egress oracle: V is every sample of every write the stand-in says it
stored, a later write of one `(label set, timestamp)` replacing an earlier one, and
`ledger.standin` checks the record against the sink's request counts and each step's expected
fault class. httpbin was considered and not used: its status is fixed by the URL, it keeps no
record, and it can't store a write before failing it or reset a connection.

A stand-in told to reject isn't a healthy backend, so this narrows the amendment above: a
`rejected` batch drop whose drain falls inside a `respond` step that expects the `rejected`
class, through one aggregate interval and `recovery_bound` after it, counts the gap and is named,
but doesn't FAIL. A `rejected` drop anywhere else still FAILs.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "Stand-in targets", has the rules.

## Amendment: a Datadog stand-in counts a resend twice, and `ledger.sent` excuses what a step cued (2026-10-15)

A stand-in target can play Datadog's series route (`[target] shape = "datadog"`), and its V is
the sum of the `count` points it stored. Datadog documents no deduplication of a `count` point,
and [ADR `delivery-semantics`](delivery-semantics.md) says a kind with no remedy is
double-counted, so the harness assumes a resent point adds rather than overwrites.
`datadog_out`'s module doc records a trial org storing a resent series point once; that's an
observation, not a documented guarantee, so the harness doesn't rely on it.

As for a Kafka topic, V counts each distinct stored copy of a `(series, timestamp, value)` once,
so a duplicate can't cancel a loss of the same size, and a negative gap is a surplus that FAILs.
`ledger.standin` reports each extra copy per SUT life, with the amount the destination counted
twice. Under `at_least_once` a duplicate is permitted and reported, not failed. It FAILs under
`at_most_once`, which never resends, and under either posture when the copy it repeats was
answered `2xx` in the same life, because the sink then resent a point it saw accepted.

`ledger.sent`, which ends an external target's ledger at the sink, runs against this shape too,
so its verdicts are tested without a real org. A stand-in told to refuse or reject isn't a
healthy destination, so this extends the cued-rejection rule above: a refusal line inside a
`respond` step that expects `refused`, a `rejected` batch inside one that expects `rejected`, an
`oversize` record drop inside a `413` step classed `rejected`, and a `rejected` record drop inside
another step classed `rejected` or a step whose `202` names dropped series are counted as cued and
don't FAIL. The same drops outside such a step FAIL as on any target.

A `respond` step's `class` is the harness's only statement of the class a sink must read an
answer as, and under this shape every non-`2xx` step must carry one. A second copy of the
sink's table in the harness was considered and not kept: it checked the scenario against itself,
never against `logit`, and could drift from the Rust table unnoticed. A class change in a sink
updates its stand-in scenario's steps instead.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "Stand-in targets", has the rules.

## Amendment: an InfluxDB stand-in overwrites a resend, and its rejected points are the whole gap (2026-10-17)

A stand-in target can play InfluxDB 2.x's write API (`[target] shape = "influxdb"`), and its V
is the sum of the delta points it stored, one per `(series, timestamp)`. Unlike Datadog's
`count`, InfluxDB documents an identical `(measurement, tag set, field, timestamp)` write as an
overwrite, and `influxdb_out` re-encodes a retried batch byte for byte, so the harness treats a
resend as a rewrite: reported, never counted twice, and permitted under `at_least_once`. A
resend can't change a value, so a second value at one key FAILs `ledger.standin`. A rewrite
FAILs under `at_most_once`, which never resends, and under either posture when the write it
repeats was answered `2xx` in the same SUT life, as a Datadog duplicate does.

Because a cued `rejected` drop credits any positive gap in its life (the amendment above),
`ledger.egress` alone can't tell a rejected window from a loss beside it. Under this shape,
every point of a request answered under a step classed `rejected` must be absent from the
store, and the life's egress gap must equal those points' sum, with no allowance. The writes'
queries must name one `(org, orgID, bucket)`, because each scenario configures or carries one.

Every non-`2xx` `respond` under this shape must carry a `class`, as under the `datadog` shape,
for the same reason: it's the only statement of what the sink must do with the answer.

The carried-mode runs stamp the bucket and org carriers with a `set` after `aggregate`, in place
of a relay through `influxdb_in`. That keeps the generator on UDP statsd, and the classes the
sink reads depend only on where a value came from, which the carriers decide either way. The
stand-in compares the request path without its query string, because `influxdb_out` always
sends one; compared whole, every write would answer `404`, which a carried-mode sink reads as
`Rejected`, a silent drop.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "Stand-in targets", has the rules.

## Amendment: several SUT services, each scored on its own lives (2026-10-15)

A scenario can run more than one `logit` under test, so the native hop can be soaked between two
processes. Both keep the role `sut`; no new role was added, because the watchdog, the kill rules,
and the readiness probes already apply to every `sut` service alike. `logit` stays the ingress
SUT, which every generator sends to, and a further SUT service is declared by a `[configs]` key
of its own, which the stack must run as a `sut` service. A separate table naming the SUTs was
considered and not used: each needs a config anyway, and a `[configs]` key is already how a
generator is declared.

Process lives are per service. Each SUT's lives come from its own telemetry, cross-checked
against the starts the timeline gives that service, and a kill on one ends a life of that
service alone. The listener symbols are read on the ingress SUT's lives and everything from
`aggregate` on on the lives of the service holding it, with V split on those. A row that equates
the two sides in one life can't be judged across two processes until the ledger names the hop
between them, so it SKIPs then. The end sequence stops the SUT services in chain order, each
only once what it holds has been forwarded.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "Several SUT services", has the rules.

## Amendment: the native hop is judged on run totals, with an end-to-end bound (2026-10-15)

Two SUT services are joined by a `[[ledger.hop]]` naming the `logit_out` and the `logit_in`
between them, and a scenario has at most one. The two processes' lives can't be paired: a kill
on one side starts no life on the other, and frames a killed sender spooled cross in a later
life of either. So each side's rows run on its own lives, as the amendment above set, and the
hop itself gets its own row, `ledger.hop`, judged on run totals: the events the hop sink
received, less its counted drops, against the events the `logit_in` forwarded. It passes at 0,
and a kill or a pause is allowed only what it can cause: a killed side's last drain interval of
counts, and, for a kill or a pause of the receiving side, one send window of frames forwarded
twice ([ADR `native-hop-named-acks`](native-hop-named-acks.md),
[`docs/known-gaps/native-hop.md`](../known-gaps/native-hop.md)). A graceful stop of the
receiving side is judged exact, because `logit_in` acknowledges everything it forwarded before
it closes, except that a killed sender's spool replays the frames it committed after its last
cursor persist, which a receiving side restarted since forwards again, so each killed sender
life is allowed that many frames too.

A frame forwarded twice adds the same increments to every count after the hop and to V, so
every per-life term on the receiving side balances around it. `ledger.summary` adds one bound no
kill can loosen: V over the whole run is at most what the generators sent less every counted
loss. A surplus past the hop breaks it once it exceeds the run's uncounted loss, the wire loss
among it; `ledger.hop` judges a smaller one.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "The native hop (W10b)", has the rules.

## Amendment: each source has a wire, and TCP is judged where nothing resets it (2026-10-15)

The load source no longer has to send UDP. A `[[ledger.source]]` names its `wire`: `udp`, the
default, `tcp`, or `file`. The loader checks it against the listener's `type:` and `transport:`
in the SUT config, so a source is never scored in another wire's unit. The unit stays a line,
and each wire has its own symbols. On TCP, G is still the generator sink's `output.messages`,
which `statsd_out` counts only once a batch's write completes, and the sink must count no
datagram. W is the listener's `input.frames`, one per line. The framer's own drops,
lines W never counted, are F, counted wire loss. On a file wire G is the writer `file_out`'s `events.delivered` and W the tailer's
`input.lines`.

TCP narrows what "uncounted by design" covers. TCP retransmits what netem drops, so netem on the
generator is judged at 0, not excused. A fault window is reported, not judged, only when the
fault can reset the connection with lines in its socket buffers: a stop, kill, restart, or
`exit`, or a partition after which Docker gave the container another address, so the driver
records the address on each side of every partition. Every other window that can affect a TCP
wire (netem on the SUT, whose egress carries the ACKs, a pause, a partition that kept the
address) is judged with steady state's limit and edge tolerance, plus a surplus of one generator
batch per `ambiguous_at_most_once` drop counted in it: a write that failed partway leaves whole
lines the SUT reads, and `statsd_out`'s `at_most_once` posture drops the batch, uncounted in G.
Reporting every TCP-affecting window, as UDP's are, would leave unjudged the claim the
`tcp-ingress` scenario exists to make, that TCP ingress loses nothing a reset doesn't explain.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "The ledger and identities", has the
symbols per wire, and its `ledger.wire` row the rules.

## Amendment: a file wire's writer and tailer share a directory in the run directory (2026-10-15)

A `file` wire's writer and the SUT that tails it need a directory both can write and read, and
the harness needs the writer's files after the run, to count G outside both processes while
the writer deletes none (see the 2026-10-18 amendment). The
driver creates `<run>/shared` with mode 0777, because the image's unprivileged user has a uid
the host doesn't know, and the scenario's override mounts it into both. This reverses "nothing
in a container writes to the run directory" for these scenarios only. The alternatives were a
named volume, which compose removes with the stack and leaves nothing to count, and the
writer's own telemetry alone, which is the count the shared directory exists to check while
the writer deletes nothing. A file
wire is judged on the run's W − G against a replay band per SUT life, not per drain interval,
because a tailer reads behind its writer: a kill replays what was read after the last checkpoint
and loses the counts of the last drain interval, and a clean stop splits a line held
unterminated ([`docs/known-gaps/tailing.md`](../known-gaps/tailing.md)). The band belongs on the
wire and on `ledger.summary`'s end-to-end bound, not on the killed life's egress band, which is
read from `aggregate`'s window.

## Amendment: a transform's counted discards leave every row of the ledger (2026-10-17)

A node in a source's `sut_transforms` may discard events it counts in
`logit.component.events.discarded`, as `drop_stale` does. `ledger.edge` judges each node's
received less discarded against what it sent, with no tolerance, and every row after the edge
subtracts the nodes' summed discards from what reached `aggregate`, in every SUT life, as it
subtracts a script's: the residual, a stopped life's egress gap, a killed life's band, the path
term of `ledger.summary`, and the end-to-end bound, which counts them with the counted loss.
Subtracting them at the edge alone would leave the residual and the stopped-life gap reading
each discard as uncounted loss, and an end-to-end bound that ignored them would let a surplus of
up to their number pass. A discard is an outcome the config asked for, never a `*.dropped` count
([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)), so the ledger takes the
counter as given: it needs no per-node scenario key and no expected rate, and a scenario that
wants a rate bounds it with `[[expect]]`. A discard of a line a kill replays is counted again
as its read counts in W again, so the replay band still bounds a file wire's surplus.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "6. The checks", has the rules.

## Amendment: an HTTP wire's G is what the listener answered, and a resend may surplus (2026-10-17)

A source's `wire` can be `http`: a generator `influxdb_out` or `otlp_out` writes to
`influxdb_in` or `otlp_in`. G is the writer's `events.delivered`, the events of the batches the
listener answered with a success, and W is the listener's `events.sent`, since neither listener
counts points before it sends them. The wire compares G with W − X: a batch refused with `503`
because every consumer had closed is in `events.sent` and its events in X, and the client still
holds it and resends it, so X is never counted loss on this wire. A busy `503` takes nothing.
G counts an event only after the listener's batch was in every inbox and W had counted it, so
only a `kill` or an `exit` of the SUT, which can lose its last unexported interval, is reported;
every other window is judged. A judged window may show a surplus, W − X above G, of one
generator batch per attempt the writer counted `logit.output.requests{class="network_error"}`
in it, plus one per edge (a failed dial, one `connect_timeout:` ended included, counts
`connect_error` and allows none; see "a boundary pair nets in full" below): the writer sends one batch per request, one request in flight, and
resends it after a transport error, when the listener may already have taken it. The same
allowance, with X left out of the counted loss, bounds `ledger.summary`'s end to end.
`influxdb_in`'s request identities are judged in `ledger.intake`.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "6. The checks", has the rules.

## Amendment: a deleting writer's G stands alone (2026-10-18)

A writer that bounds its retention (`file_out`'s `rotate.max_files`) deletes its oldest file at
every rotation past it, and the shared directory then holds fewer lines than the writer wrote.
The lines a deleted file held can't be counted at the end, so once the writer has deleted one,
G, its own `events.delivered`, stands alone: the lines left in the directory must not exceed
G, and the directory must hold the files the writer keeps, read from its rotation count and
config. The equality with the count still holds while the writer deletes none. The end
sequence waits for the tailers' W to reach G, read from the writer's telemetry after it stops,
rather than the count, which a deleting writer leaves short.

A deleted file the tailer hadn't read is lost, and `tail_in` counts that loss only as far as the
last checkpoint knew: a restarted tailer counts the bytes the entry it can't use recorded as
unread, `logit.input.bytes.dropped{reason="gone"}`, but not what the file gained after that
checkpoint ([`docs/known-gaps/tailing.md`](../known-gaps/tailing.md), "A file removed or rotated
out of every pattern before `tail_in` read it to its end"). The ledger can't see such a loss in
W − G, because G stands alone, so a retention guard FAILs `ledger.wire` for any SUT fault during
which the writer rotated often enough to delete a file the tailer hadn't read: `max_files − 2`
rotations in a stop, kill, restart, or `exit`, which close the tailer's files, and `max_files`
in a pause, which keeps them open. Reading the loss from that counter instead would miss what a
file gained after its last checkpoint, so `tail-rotate-delete` expects it at 0 beside the guard.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "6. The checks", has the rules.

## Amendment: a copytruncate's step records its lost lines, and a running one may re-read its copy (2026-10-29)

A file wire's writer can be rotated from outside by `copytruncate`, `gzip`, and `replace` steps,
which do to the shared directory what logrotate and a restore do, so the content claim, gone
counting, and compressed skip of [ADR `tail-content-identity`](tail-content-identity.md) run
under soak. The writer's files belong to its container's `logit` user, so each step runs a
stdlib script in a one-shot container as root rather than on the host, and `file_out` holds its
file `O_APPEND`, so a truncate behind it leaves no hole. A `copytruncate` reads the bytes the
writer appended after the copy's last read on its way to the truncate and records their lines:
they're in neither file, so the shared directory's count is held to G less them, with at most
one writer batch a step lost unrecorded between that read and the truncate. The replay band
takes each step's terms from that ADR's residuals rather than from what a run shows: below, the
lost lines, which a tailer reads only if a read lands between the copy and the truncate; above,
for a step while the tailer runs whose record shows bytes appended after the copy's last read
(or every such step, when the count shows a loss no record holds), the copy's lines and its cut
line, since a read of those bytes leaves the copy shorter than what the tailer read and it's
read from 0. Any other step adds nothing above: a copy with nothing appended after it, or made
while the SUT is down, claims, so a read from 0 FAILs the row. The measurement is the harness's
own, taken by the step, never the SUT's. The alternatives were freezing the writer around the
truncate, which would remove the window the ADR's residual names, and judging each step by
whether the SUT's own `files.claimed` rose, which would take the claim's outcome from the code
under test. The scenario's `[[expect]]` rows anchor the down-path claims to their steps as
well: one after the stop, two after the kill (its copy's and the replace's).
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "6. The checks", has the rules.

## Amendment: a fan-out node's events are carried through its chain, and adjacent judged windows net at their boundary (2026-10-18)

A source's `fan_out` names a `line_protocol` node and the k events it emits per event. Every
row after the listener then reads W′, each listener's W − D − B − X carried through its chain:
a fan-out node turns x events into k × x − T_lines + (T_in − T_disc), its children and the
parents it sent on, and any other node into x − T_disc. W′ takes W − D − B − X − ΣT_disc's place
in the residual, a stopped life's egress gap, and the end-to-end bound, and R and a killed
life's ingest grow by each chain's product of k. `ledger.edge` holds the node to children ==
k × T_in − T_lines with no tolerance, since the count is the ADR `transform-fan-out` count.

`ledger.wire` anchors G at every generator drain, a drain with no G point at the running total.
On TCP and HTTP, where acknowledgments can bunch, G's interpolation at a boundary between two
judged windows can err by the G that moved in the generator interval straddling it, and shows
as an opposite-signed pair in the two runs that touch. So the part of one run's surplus past
its own allowance nets with the loss across that boundary, up to that G, shown in the detail;
the part inside the allowance nets nothing, and runs that don't touch never net. Netting more
widely would let one window's surplus hide another's loss.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "6. The checks", has the rules.

## Amendment: three ways to share the sinks, and a target split for a router that copies (2026-10-18)

A scenario with several sinks names how they share the SUT's events in `[ledger] split`, and
the ledger follows each `[[ledger.sink]]`. Under `copy`, the default, each sink holds a full
copy of `aggregate`'s output, so each sink's V is judged alone against Ab. Under `route` a
`route` after `aggregate` fills one target per sink, so the sinks' V add up to Ab and are
judged on their sum; `load()` refuses a list-valued `routes:` value and any `route_signals`
there, because one event can then reach several sinks.

Those routers run under `target`: the router, `[ledger] sut_router`, sits between the listeners
and one `aggregate` per sink, and each sink is judged from its own target's `events.sent`,
which the target's `Fanout` counts under the target's id. The listeners' side ends at the
router: a killed life's residual stays in [0, R], and a stopped life's must be 0, since its
final drain runs after every data node has exited and nothing downstream judges it. The router
balances with no tolerance, received + copied − discarded == its forward edge's sends + its
targets', in the final life and every stopped one. That balance pools the targets, so beside it
each target's sends may not exceed what the router received less what it discarded, and its
forward edge's may not exceed what it received: a target receives at most one delivery of each
event (graph rule 51 for a `route`, one part per target for a `route_signals`). The bound
catches a target receiving more than the router took in, not events sent to one target in
place of another below that. Each sink's branch is judged hop by hop, its egress against its
own Ab, and `ledger.summary` judges every term on its own. `load()` holds the router's targets
to the sinks' targets, refuses any other router or script that fills one, and refuses the split
beside `sut_script`, a hop, `vm_every_window`, `[overlap]`, or a non-local target, whose rows
read one `aggregate`. The alternative, summing the sinks' V against the router's received
events times a copy factor, needs a factor the config doesn't fix: a `route` copies only the
events whose value lists several targets. [`docs/plans/soak-harness.md`](../plans/soak-harness.md),
"The target split (W15)", has the rules.

## Amendment: a run the host slept through ends at its first stall and exits 3 (2026-10-18)

The host running a soak may suspend, as a laptop does with its lid closed, and nothing should
keep it awake. A run it slept through can't be judged: nothing was scheduled or polled in the
gap. So the driver rejects the run instead of running out its schedule:

- **The first stall ends the schedule.** A tick more than 120 s late applies no further step,
  because every step overdue by then would run back to back. The driver records `stalled` and
  an `aborted` phase naming it, and the end sequence still runs and reverts the open faults, so
  the run directory is complete and `check` scores it like any other. Its waits keep their own
  bounds, so an aborted run ends within minutes of the host waking.
- **A stalled run exits 3**, apart from a FAIL's 1. A FAIL needs someone to read it; a stall
  needs only a rerun once the host is awake, and that rerun can pass. `check` exits 3 on the
  same directory, since both read the stall from the `run` row.
- **3 outranks 1 but not 130.** Nothing else in a stalled run can be judged, so its other
  FAILs don't change what to do next. An interrupt is the operator's own stop, and 130 says so.
- **`--retry-stalled N` is opt-in** (default 0): it reruns a stalled run up to N times in new
  run directories with the same duration and seed. It's refused with `--keep`, because the kept
  stack would block the rerun. Without it, the caller reruns on a 3.

## Amendment: `rss_slope` excuses a settle window past `recovery_bound` (2026-10-18)

A process can give back a fault's backlog memory in steps, over minutes. After a 45 s
partition, `fanin`'s generator went from about 18 MiB to 86 MiB, then held 62.6 MiB from 14 s
after the revert, 52.6 MiB from 84 s, and about 32 MiB from 104 s. `recovery_bound` ends the
excusal at 45 s, so the plateaus sat in the fit beside the baseline, and `rss_slope` WARNed at
+240 MiB/h on memory the allocator was returning. Widening `recovery_bound` would have widened
every row's excusal, and the ledger and `recovery` need it tight.

- **`rss_slope` alone widens each fault window by `rss_settle`**, an optional top-level
  scenario duration. It applies after every fault, as `recovery_bound` does for this row.
- **It's opt-in, 0 s by default.** A life needs 6 samples over 60 s outside the windows to be
  fitted, and most shipped schedules leave so little past `recovery_bound` that any settle
  window leaves some service no fit: a 60 s default took `kafka-broker-restart`,
  `kafka-sasl-reauth`, `statsd-vm-lost-total`, `fanout-stop-outage`, `spool-kill-replay`,
  `standin-at-most-once`, and `statsd-datadog` to no fittable life, and a recorded
  `kafka-sasl-reauth` run from 29% judged to none. `fanin` and `tcp-ingress`, whose generator or
  SUT showed the stepped release, set 60 s and keep a fit on every service.
- **A run directory recorded before the key existed reads 0 s**, so it scores as it did.
- **`fd_slope` keeps `recovery_bound`.** It compares single samples with the warmup median, and
  a closed connection releases its fd with no allocator in between.
- **Coverage stays visible.** A service whose samples after warmup all fall in the widened
  windows WARNs on coverage instead of SKIPping, and the row SKIPs for want of a fit only when
  the run after warmup is shorter than one fit's 60 s.

Growth across a whole run still FAILs under a settle window: a fixture of `cardinality`'s +73
and +61 MiB/h leak FAILs with 60 s in force.

## Amendment: a boundary pair nets in full, and only an attempt the listener may have taken allows a resend (2026-10-25)

An `otlp-fan-out-conc` run FAILed `ledger.wire` with nothing lost. After the SUT's restart the
generator's reconnect landed about 0.4 s later against its drain than in a passing run, so G's
interpolation put the restart's backlog across a run boundary: −2,154 before it and +2,159 after
it, with 4,950 of G moved over the generator interval straddling it. Under the rule above only
the surplus past its run's allowance netted, and that allowance, 850, was mostly 15 transport
errors that were refused dials and DNS failures, which take nothing. 855 of phantom loss stayed
in the pool, past its limit of 650.

- **A boundary pair nets in full.** At a boundary that touches a judged run, with a judged or a
  steady run on the other side, an opposite-signed pair nets up to the G that moved over the
  generator interval straddling it, before the surplus run's allowance applies to what's left.
  The counts can't tell interpolation error from a resend beside a loss, and interpolation can
  move that much G. The cap is what keeps a resend's surplus, or a steady run's, from hiding
  more loss than interpolation explains. This replaces the previous amendment's rule that only
  the surplus past the allowance nets.
- **Only an ambiguous attempt allows a resend.** The HTTP sinks count a request whose dial
  failed as `logit.output.requests{class="connect_error"}`, apart from `network_error`
  ([ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md)'s
  amendment of the same date). Only `network_error`, a timeout or a reset after which the
  listener may hold the batch, allows one batch of surplus. A dial the writer's
  `connect_timeout:` ends counts `connect_error`, so a dial stalled by netem allows nothing. The
  allowance is loose only when the writer's request timeout is shorter than its `connect_timeout:`,
  which counts a stalled dial `network_error`
  ([ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md)'s "Amendment: a failed dial counts `connect_error`"). A run recorded before the split
  counts every failed dial as `network_error`, and its allowance is looser still.

Re-scored, the failed run PASSes with 90 of loss judged against 650, and the passing run reads
96 as before. A backend stop still opens no judged HTTP window: whether it reaches the wire
depends on the listener (`otlp_in` holds a request past the writer's timeout, `influxdb_in`
answers a busy `503`), so its resends' surplus counts 0 as steady state's does.

## Amendment: a merge is credited back as delivered, and a keyed discard is judged by absence (2026-10-29)

Two transforms in the path need rules a counted discard doesn't give.

- **A merge is no loss.** `dedup_timestamps` folds a series' records of one timestamp bucket
  into one record carrying their sum, counts each folded record in
  `logit.transform.metrics.merged` (M), and counts an event it leaves with nothing in
  `events.discarded`. Under the discard amendment above, the ledger would read each merge as
  counted loss and V, which holds the folded increments, as a surplus of M. A source now names
  such a node in `merge`. `ledger.edge` still judges its discards as for any node, and holds
  them to M, which is exact while each event carries one record, as a statsd line does. Every
  later row credits M back: Ab + M wherever it reads `aggregate`'s Ab, and ΣT_disc − M wherever
  it subtracts the discards. The node's `logit.transform.metrics.absorbed`, a superseded gauge
  or cumulative reading, loses that reading's value, so `ledger.edge` requires it to stay 0 in
  every life a kill didn't end. The declaration is per node, and `load()` takes only a
  `dedup_timestamps` there, so the credit applies only where a merge is known to carry its
  value.
- **A keyed discard is judged by what never arrives.** `sample`'s discards balance under the
  discard amendment, but a balanced count can't tell a stable verdict from one that changed
  across a restart: a life that kept the wrong half would still balance. A sink may name a
  `vm_absent_selector`, exported beside its `vm_selector` at the end, and `ledger.absent` FAILs
  any series it matches, from any SUT life, and an export that failed or never ran. The kept set
  is computed before the run from the frozen hash (`soaklib/sampling.py`, an XXH64 checked
  against xxHash's published vectors and `logit_core::sampling`'s pinned ones), so a scenario
  states it in its selectors rather than learning it from the run.

[`docs/plans/soak-harness.md`](../plans/soak-harness.md), item 19 of "10. Next scenarios", has
the scenarios that use them.

## Amendment: a delta transform's value is judged beside its points (2026-10-29)

A `cumulative_to_delta` node's point counts can balance while its values don't: a delta that
leaves a series' baseline wrong carries the wrong value in a point the count rows accept. So a
scenario that names the node in `[ledger] sut_delta` gains `ledger.value`, which holds the sum
of the deltas a Datadog-shaped stand-in stored, V, to the increments the generator's
`generate_in` sent less its counted drops, I.

- **The record holds the values a cue loses.** A `rejected` step and a `202` naming
  `series_errors` lose deltas the sink counts only as records, and the transform has already
  moved its baseline past them. The stand-in records every request body, so the row reads each
  never-stored point whose last request drew one of those answers, U, and judges I − V − U
  exactly. Reporting those cues instead, as first planned, would excuse whatever else their
  windows lost.
- **A first point the transform skips is reported inside a bound.** Under `initial_value: auto`
  the transform skips a series' first point after its start, a restart, or an idle eviction,
  and no counter gives that point's value. Each drain that counts `skipped{reason="initial"}`
  or `series.evicted` opens a window back to the node's previous receiving drain. Each window
  is bounded alone by what it can lose: its skipped series times one generator window of one
  series' increments, plus one window of every series as slack. It widens to its span's
  increments only when the generator's sink counted a drop in it, which moves the first point
  the SUT sees further on. After a kill, which loses the deltas the SUT's sink still held, the
  window adds the points that killed life lost, its Ab − V, each up to the largest stored delta,
  rather than widening: a kill's span holds its whole outage, which would excuse a loss
  anywhere in it. The gap is reported if it fits the sum of the windows' bounds, and FAILs past
  it. Bounding the windows' whole spans instead, a first version did, let a loss of 13% of the
  run's increments outside every window pass on the first run: the windows bound the loss they
  explain, and excuse none elsewhere.
- **The count rows judge points with the node in `aggregate`'s place.** Ab is its
  `events.sent`, its skipped points leave every row as a transform's counted discards do, and V
  is the stand-in's distinct points, each credited to the life its request arrived in, because a
  point's timestamp is the generator's window and can precede the SUT life that sent it.

- **Two values at one second under the `datadog` shape are a WARN, not a changed resend.**
  `datadog_out` re-encodes a resend's points unchanged, so a second value of one series in one
  second is a second point its whole-second timestamps merged, sent in another request. The
  first run met one when the generator's `aggregate` flushed a partial window on shutdown.
  Datadog keeps the later, an uncounted loss
  [`docs/known-gaps/datadog.md`](../known-gaps/datadog.md) lists, so `ledger.standin` reports it
  rather than failing every run whose generator restarts at the wrong phase.

The node replaces `sut_aggregate` rather than sitting beside one, since an `aggregate` after it
would resum the deltas the row judges. [`docs/plans/soak-harness.md`](../plans/soak-harness.md),
"10. Next scenarios", item 20, has the rules.

## Amendment: three more HTTP listeners, and an acknowledgment timeout allows a resend (2026-10-29)

The HTTP wire follows `splunk_hec_in`, `datadog_in`, and `prometheus_in`'s remote-write
receiver, from `splunk_hec_out`, `datadog_out`, and `prometheus_out`. `splunk_hec_in` and
`datadog_in` count requests and answer busy as `influxdb_in` does, so `ledger.intake` holds them
to its two identities on their write routes. `prometheus_in` counts `logit.input.writes{class}`,
with no `route` tag, and never answers busy, so it holds only the first. It has no `transport:`,
so its wire is keyed on the mode its config sets: `bind:` is the receiver, and
`scrape_targets:` the scrape wire, the next amendment.

- **An acknowledgment timeout allows one batch of surplus, as a transport error does.**
  `splunk_hec_out` under `ack: true` counts a batch delivered only once `/ack` answers every id
  of it, and resends the whole batch once `ack_timeout` passes. `splunk_hec_in` keeps its ack
  ledger in memory, so a restart forgets ids it issued for bodies it had already delivered, and
  the resend puts each such batch in W a second time. Each id counted
  `logit.output.acks{result="timeout"}` allows one batch, an upper bound because one batch can
  hold several ids. A poll the writer counts under `route="ack"` sends no data, so a poll that
  fails in transport allows none.
- **A judged run's trailing edge allows the batches the writer read in flight there.** The
  listener counts a batch in W before the writer reads its answer, and under `ack: true` before
  its acknowledgment arrives, half a second or more later. So a writer at `concurrency: 8`
  holds batches in W and not in G at any edge, and the lag drifts by up to that many between
  drains. `splunk-hec-ingress`'s first run FAILed a SUT restart's window, beside a reported
  kill, by 930 points against 700 allowed with nothing lost. The surplus lag leaves in a run is
  its lag at the end less its lag at the start, so a judged run's trailing edge now allows
  `max(1, in_flight)` batches, `in_flight` being the higher of the writer's
  `logit.output.in_flight` readings at the two generator drains straddling it, whatever the
  neighbour, and its leading edge one batch. Three other forms were rejected. The writer's
  `concurrency:` on every edge passes a loss of 300 beside 1,000 duplicates in one judged run
  where nothing was in flight. The reading on both edges passes 1,500 unexplained with 8 in
  flight at each. `concurrency:` only at a trailing edge beside a reported run FAILed the
  rerun's generator-netem window on lag alone: a steady neighbour's own drift hid the opposite
  sign `_boundary_nets` nets. The gauge measures the batches at the edge that backs a surplus,
  so it covers the drift and widens nothing it didn't see.

## Amendment: a scrape wire judges increments per generator life (2026-10-29)

`prometheus_in` scraping a generator's `prometheus_out` exposition is a fifth wire, `scrape`,
judged in value: per generator life, G, the increments its `generate_in` sent less what the
`generate_in`s dropped and its `aggregate`s counted in `logit.transform.metrics.dropped`,
against V, the stored counters' reset-aware total in that
life.

A pull carries running totals, not lines. A sample is a series' total at the scrape, so there
is no count of lines on either side to compare, and the generator's `aggregate` exports no
per-series total to read G from. The increments are the one quantity both ends hold: the
generator counts them as it generates, and the backend's reset-aware total sums them back. A
failed, timed-out, or skipped scrape loses no increment, since the next one reads the total
that includes it, so the only loss is intermediate values, and no fault window excuses
anything.

- **V splits at the generator's lives, not the SUT's.** A generator restart resets its totals,
  which the backend records as a counter reset, and a SUT restart doesn't reset them. So each
  sample is credited by its scrape time to the generator life in force, and a SUT restart
  splits nothing. A scrape that starts before a generator restart and waits for the new
  process could read its totals and credit them to the old life, so `load()` requires the
  scrape `timeout` below the generator's `aggregate` window, before which the new process
  exposes nothing.
- **Only the drops before a total count against G.** A dropped event at a `generate_in`, or a
  metric an `aggregate` dropped, is an increment no total holds. A drop anywhere else loses
  none: `internal` and its sinks carry telemetry, and a cumulative point the `aggregate`
  failed to send is carried by its next one.
- **Each generator life has an end band.** The increments after a life's last scrape never
  reach a scrape: up to one scrape `interval`, plus one generator `aggregate` window, since the
  scraped totals lag the generation by up to a window, plus the scrape `timeout`, since a
  scrape reads the totals up to that long after it starts. A drain interval more would hide
  that much uncounted loss, since this band alone judges a life's end values. A life the end
  sequence stops exports all its counts, so its G − V can't go below 0;
  a killed life's last drain interval of counts is never exported, so G can fall short of the
  increments V holds by one drain interval of them. A life outside its band FAILs, a surplus
  below it and lost increments above it.
- **A kill on the generator is allowed on this wire alone.** Elsewhere a generator killed
  after its last drain has sent lines with no G behind them. A scrape target sends nothing; a
  kill only ends a life early, which its end band covers.
- **The count rows judge samples.** W is the listener's `logit.input.samples`, which leaves out
  the three synthetic samples each scrape appends, `up`, `scrape_duration_seconds`, and
  `scrape_samples_scraped`, failures included. They are Y, three per scrape, so the listener
  balances W + Y − D == E + B, and the sink's `vm_selector` leaves them out too, so the
  distinct stored samples per SUT life are judged against W − X. A dropped batch's events
  count its three synthetic samples too, one batch per scrape, so `ledger.egress` takes
  3 per dropped batch out of X, and out of the sink's counted drops. The rows that read an
  `aggregate` SKIP: the SUT relays samples and has none, and a node between the listener and
  the sink could change the values the wire judges, so `load()` refuses `sut_transforms` and
  every `[ledger]` key that names such a node.

[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "10. Next scenarios", item 22, has the
scenario.

## Amendment: a TCP window's resend allowance counts ambiguous attempts (2026-10-29)

A TCP source's judged window allowed one generator batch of surplus per
`ambiguous_at_most_once` drop, which only a sink under `at_most_once` counts. `statsd_out`
defaults to that posture, but `syslog_out` and `graphite_out` default to `at_least_once`, which
resends the batch after an ambiguous attempt and drops nothing, so a `syslog_out` generator at
its default would have no allowance for the lines its failed attempts left at the SUT.

The allowance now counts the generator sink's ambiguous attempts,
`logit.output.requests{class="ambiguous"}`, which the stream driver counts once per attempt
under either posture (`crates/logit-outputs/src/stream.rs`). Under `at_most_once` each such
attempt is one `ambiguous_at_most_once` drop, so `statsd_out`'s allowance is unchanged; under
`at_least_once` each leaves at most one batch the SUT read beside the resend G counts. A TLS
first write that fails is ambiguous, where a plaintext one is clean, and adds to the count the
same way. A drop counted with no ambiguous attempt beside it allows nothing.

## Amendment: the end run nets at its boundary (2026-10-29)

A `syslog-tls-ingress` run FAILed `ledger.wire` with nothing lost: G fell 100.1 short of W at
the end, against an end allowance of 100, while the steady run before the end read 98 of loss.
The generator counted a batch in the drain interval before the end run's boundary and the
listener read it in the one after, the opposite-signed pair "a boundary pair nets in full" describes. That rule netted only
at a boundary touching a judged run, so the end run's shortfall met its allowance whole.

The end run now nets with the run before it, steady or judged, on every wire, under the same
cap: up to the G that moved over the generator interval straddling the boundary, and never past
either gap. The end allowance then applies to what's left, and the end run's netted gap is what
the loss outside the reported windows counts. A reported run still never nets. Re-scored, the
run PASSes with 98 netted across the boundary and 2 left at the end.

## Amendment: a relay keeps `aggregate` in `logit`, and V splits on its lives (2026-10-29)

A sink whose destination is another `logit` (`otlp_out` over gRPC to `otlp_in`, passed on by
`prometheus_out` to VictoriaMetrics, whose OTLP route is HTTP only) needs a second SUT that
isn't the far side of a hop: it has no `aggregate`, and the running totals and their timestamps
are `logit`'s. Each `[[ledger.sink]]` names that service in `relay`. `logit` then holds the
listeners, `sut_aggregate`, and every sink, and the resolved scenario records it as `sink_sut`,
so every ledger row reads `logit`'s lives and V splits on them. The relay is still a SUT: it is
scored on its own lives by the watchdog rows, and a fault may name it.

The relay's losses are judged only through V. Under `temporality: cumulative` a write it loses
in the middle of a `logit` life, such as the queue of its own sink at a kill, changes no
stored total, so the egress rows see it only when it holds a life's last value. That's the
property the scenario soaks: a cumulative relay through a stateless hop. A relay can't be
combined with a hop, every sink names the same one, and the target is local.

## Amendment: OTLP and HEC stand-ins overwrite under cumulative temporality, and name loss by position (2026-10-29)

A stand-in target can play OTLP/HTTP's metrics route (`[target] shape = "otlp"`) and Splunk's
HTTP Event Collector (`[target] shape = "splunk"`). Both run the SUT's `aggregate` under
`temporality: cumulative`, the remedy both sinks' module docs name for a resent `Sum`, and take
`remote_write`'s V: one sample per `(label set, timestamp)`, a later write replacing an earlier
one, so a resend is a rewrite and a rejected window is overwritten by the next. Delta temporality
with the Datadog shape's duplicate counting was the alternative; neither destination documents
how it stores a resent point, so counting copies would judge a semantic the harness can't
verify.

Two answers lose part of a request, and the stand-in decodes nothing, so the record names the
lost part by position, as a `series_errors` answer does under `datadog`. A `partial_success`
of N rejects the request's first N data points, and `ledger.standin` holds N to the sink's
`records.dropped{reason="rejected"}`, beside the points of each request answered under a step
classed `rejected`, which `otlp_out` counts the same way. An `invalid_event` of N is HEC's code
6 naming object N: the objects before it count as stored, as Splunk indexes them, and that
object's records are held to `records.dropped{reason="invalid_event"}`. An `invalid_event`
step takes no `class`, since the sink resends the rest and settles the batch.

The HEC shape has a second path. Each default `/event` answer mints an `ackId` per channel,
without which `splunk_hec_out` reads the token as not acknowledging, and `/ack` polls are
answered from the ids minted; a `withhold_acks` cue answers every poll `false`, which the sink
must read as `ambiguous` at its `ack_timeout`. A poll is no write: it spends no `count` and
holds no samples. `after = K` lets K writes through before a cue answers, so a busy `503` lands
on a batch's second body, after its first was accepted.
[`docs/plans/soak-harness.md`](../plans/soak-harness.md), "Stand-in targets", has the rules.

## Amendment: a `cri` wire judges logical lines from a writer of the harness's own (2026-10-30)

`cri_in` reads a kubelet pod log tree in the CRI format, which no `logit` sink writes: a line split
into 16 KiB `P` pieces and an `F`, interleaved streams, a short `P` whose `F` follows seconds later,
as CRI-O's conmon writes a partial write (containerd writes a `P` only at 16 KiB or at a reopen),
and the kubelet's rotation, which renames the live file to a dated name, has the runtime reopen it,
and gzips the older rotated files. So a `cri` source's generator is
`tools/soak/soaklib/criwriter.py`, a stdlib writer run in the stand-in's Python image with a JSON
config. It reports on stdout in `internal`'s NDJSON grammar, so every row reads it as it reads a
`logit` writer, without a second telemetry reader. Its config is named `*.json`, so
`script/validate` never reads it as a `logit` config, and `load()` checks it with the writer's own
rules.

G is the logical lines it finished, one per `F` record. W is `cri_in`'s
`logit.component.events.sent`, not `logit.input.lines`, which counts each record. A run held past
`partial_timeout` leaves as two events, and `cri_in` counts each such flush once in
`logit.input.frames.truncated{reason="partial_timeout"}`, S, so `ledger.wire` judges W − S + F − G
against the replay band, and the end-to-end bound adds S. That holds while every flushed run's rest
arrives, which the writer guarantees by finishing every line before it exits; a container exit
mid-line, whose head never gets its rest, is left out of the writer for that reason. `cri_in` pins a
file's checkpoint at a held run's first record, after which it has sent the other stream's lines, so
a stop or a kill while a run is held replays them. The writer logs each window it holds a line
open, and on a `cri` wire a stopped or killed life whose end landed while that line's pin was held,
until the idle flush or the `F`, or for a kill while the last interval checkpoint still carried it,
widens the band above by the lines it finished in that window, capped at its rate times
`partial_timeout` plus `poll_interval`; any other end widens it by nothing. A `cri.dangles` row
reports whether any end landed at a held pin, and WARNs when none did, since the run then never
exercised one. The writer also logs each rotation's held `P`, and `drops` excuses the
`bytes.dropped{reason="gone"}` a stop or kill shortly after a rotation counts when the next rotation
compresses the pinned file while the SUT is down. The end count reads
the tree's `F` records, gzip files decompressed, and that count plus the lines the writer's
rotations removed, which it reports, must equal G. `cri_in` never reads a `.gz`, so a SUT down
across two rotations, or paused across three, loses a file's unread tail, and the row FAILs such a
window as the scenario's fault.

## Amendment: `rss_slope` skips each life's warm-up and fits only over 300 s (2026-10-31)

`cri-rotate` restarts the SUT four times a cycle, so its only steady samples sat 40 to 100 s into
a fresh life, and `rss_slope` fit a line over 60 s of them. Over an hour of 33 lives the SUT's RSS
stayed between 26 and 35 MiB and jemalloc's allocated bytes between 4 and 7 MB, yet those fits read
from −186 to +135 MiB/h, and the row FAILed. A started `logit` gains 6 to 10 MiB in its first
30 s and moves up to 3 MiB more until about 70 s. Past that its RSS still steps by up to 2.8 MiB
within 60 s at a 5 s drain, and a step of A MiB in the middle of a span of T s biases a
least-squares slope by 1.5 × A / T: 270 MiB/h for a 3 MiB step over 60 s.

- **A life's first 120 s isn't fitted.** The warm-up runs from the life's start, read from its
  uptime, and applies to every life, the first included, on top of the run's warmup.
- **A fit needs 300 s of samples past the warm-up.** A 3 MiB step needs 253 s to stay under the
  64 MiB/h limit, and over 300 s it fits as 54 MiB/h. Six samples is still the floor. An
  `[[expect]]` `slope` keeps its 60 s, because its bounds are a scenario's own.
- **A service with no life to fit SKIPs when the schedule couldn't hold one**: when no stretch
  outside the widened fault windows lasts 300 s, the service SKIPs with that stretch's length,
  and the row SKIPs when every service does. Where a 300 s stretch existed, a service with no
  fit still WARNs on coverage, since something other than the schedule took its fit away. This
  replaces the coverage WARN for a schedule that excuses every sample.
- **Fault-dense scenarios lose their SUT's verdict.** At default durations, about half the
  shipped scenarios leave no life of the SUT 300 s, and their `rss_slope` SKIPs for that
  service; their fits under 60 s to 200 s spans were within the noise of a step anyway. The
  scenarios built to judge memory, `cardinality`, `cardinality-capped`, `partition-rss`,
  `tail-rotate-delete`, and `cri-rotate-rss`, keep a fit on every service.
