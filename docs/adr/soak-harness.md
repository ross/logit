---
created: 2026-10-08
updated: 2026-10-18
---

# A soak harness: real containers over ordinary config, one-shot netem, and a reset-aware ledger as the loss oracle

## Status
Accepted

## Context

`logit` has harnesses for throughput (`script/perf`,
[ADR `load-test-harness`](load-test-harness.md)), interop (`script/victoria-interop`,
`script/splunk-interop`, [ADR `victoriametrics-interop`](victoriametrics-interop.md)), and data
shape (`script/shape-survey`). None runs `logit` for long under faults. Three plans describe
soaks that were never run: stopping a sink for 90 s
([`buffered-sink-delivery`](../plans/buffered-sink-delivery.md)), a `kill -9` with a disk spool
and a replay check ([`durable-sink-buffer`](../plans/durable-sink-buffer.md)), and flooding
`statsd_in` while its sink is stopped
([`decoupled-listener-io`](../plans/decoupled-listener-io.md)). `tc netem` appears in the repo
only as a hand-run latency measurement
([`native-send-window`](../plans/native-send-window.md)).

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

## Amendment: a deleting writer's G stands alone (2026-10-18)

A writer that bounds its retention (`file_out`'s `rotate.max_files`) deletes its oldest file at
every rotation past it, and the shared directory then holds fewer lines than the writer wrote.
The lines a deleted file held can't be counted at the end, so once the writer has deleted one,
G, its own `events.delivered`, stands alone: the lines left in the directory must not exceed
G, and the directory must hold the files the writer keeps, read from its rotation count and
config. The equality with the count still holds while the writer deletes none. The end
sequence waits for the tailers' W to reach G, read from the writer's telemetry after it stops,
rather than the count, which a deleting writer leaves short.

A deleted file the tailer hadn't read is lost, and `tail_in` counts that loss nowhere: a
restarted tailer drops the checkpoint entry it can't use with no counter and no diagnostic
([`docs/known-gaps/tailing.md`](../known-gaps/tailing.md), "A file removed or rotated out of
every pattern loses its unread tail"). The ledger can't see such a loss in W − G, because G
stands alone, so a retention guard FAILs `ledger.wire` for any SUT fault during which the
writer rotated often enough to delete a file the tailer hadn't read: `max_files − 2` rotations
in a stop, kill, restart, or `exit`, which close the tailer's files, and `max_files` in a
pause, which keeps them open. The alternative, reading the loss from the product, needs a
counter `tail_in` doesn't have. [`docs/plans/soak-harness.md`](../plans/soak-harness.md),
"6. The checks", has the rules.

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
in it, plus one per edge: the writer sends one batch per request, one request in flight, and
resends it after a transport error, when the listener may already have taken it. The same
allowance, with X left out of the counted loss, bounds `ledger.summary`'s end to end.
`influxdb_in`'s request identities are judged in `ledger.intake`.
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
