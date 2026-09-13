---
created: 2026-09-12
updated: 2026-09-13
---

# A load-test harness: the real binary, a declarative event template, and CPU per event as the signal

## Status
Accepted

## Context

`logit` already measures itself at the micro level in real detail:
`crates/logit-bench/tests/allocations.rs` pins exact allocation counts per stage in CI, and
`crates/logit-core/tests/type_sizes.rs` pins `Event`'s exact layout — both documented and
cross-referenced from [`docs/design/memory.md`](../design/memory.md). What's missing sits one
level up. `memory.md` §7 draws the boundary precisely: `crates/logit-bench/benches/pipeline.rs`'s
own module doc states that almost every bench there calls decoders, transforms, and
encoders **directly**, sidestepping the tokio runtime and the channels between nodes entirely,
because `divan::AllocProfiler` only counts allocations on threads it controls — and the one
exception (`mod runtime`) stays trustworthy only by never spawning a task or a thread, running
everything through a `current_thread` runtime's `block_on` on the one thread Divan already watches.
That same section names what's left over in so many words: "what a full multi-node graph costs end
to end, spread across the real worker threads and OS threads `run_with_shutdown` actually spawns,
is still a separate question needing a load generator, not a microbenchmark." Worker-thread
scheduling, cross-thread channel hops, bounded-queue backpressure, sink-buffer draining, and a Lua
script's own OS thread are all real costs a microbench that stays on one thread structurally cannot
see, and every wall-clock number recorded anywhere in the docs today is a hand-transcribed one-off
from a single `script/bench` run — nothing stores a prior run for comparison, and finding which
node in a graph is the bottleneck currently means attaching a profiler by hand.

This ADR decides how `logit` closes that gap: a repeatable, out-of-CI harness that runs the real
release binary against ordinary YAML configs, measures throughput and CPU cost, attributes time
per node, and makes a flamegraph a one-liner. The concrete build-out — file layout, workstream
order, scenario list — is [`docs/plans/load-test-harness.md`](../plans/load-test-harness.md); this
record is about the decisions that plan is built on, not the schedule.

## Decision

### Measure the real binary through ordinary config, not an in-process harness

A scenario is nothing more than a `logit` YAML under `perf/scenarios/` — a listener, some
transforms, a sink — run the same way an operator would run it: `logit run <config>`, release
profile, real worker threads, real bounded channels, real sink queues, a real Lua OS thread when a
scenario has one. The harness spawns that process, watches its stderr for a completion signal, and
reads `wait4`'s rusage once it exits. Nothing about the pipeline runtime changes to make this
possible, and nothing added for it costs anything when it isn't configured (next section).

The alternative — an in-process harness driving `logit_pipeline::run_with_telemetry` directly, the
same entry point `logit-cli` itself calls — was real enough to consider seriously, is faster to
iterate on, and needs no child-process bookkeeping. It's rejected because `crates/logit-cli` is
bin-only: nothing else in the workspace is meant to embed the runtime and call it a `logit`
process, and a harness that did would be measuring a `#[test]`-shaped call into the runtime, with
its own thread pool sizing and its own process-level allocator/scheduler warm-up state, not a
`logit run` process the way one actually gets deployed. The two are not guaranteed to cost the
same, and the entire premise of this effort is that end-to-end, real-process cost is exactly what
microbenches can't show. Spawning the real binary is the only way to be measuring the thing that
ships.

### Two new component kinds, shipped unconditionally

`generate_in` (a finite or unbounded event generator, `Role::Listener`) and `null_out` (a sink that
discards) are the harness's only two additions to the pipeline runtime proper, and both are
ordinary, always-compiled `ComponentKind` variants — not behind a feature flag, not
`#[cfg(test)]`, not conditionally registered. This follows the same reasoning every other
`ComponentKind` in the registry already rests on: an unconfigured kind costs nothing. Its variant
exists in an enum match, its `build_spec` arm never runs unless a config names it, and `logit run`
already rejects any config referencing a kind that isn't implemented — shipping `generate_in` and
`null_out` in the release binary changes nothing about what any existing config does or costs, and
gating them behind a build flag would only buy a distinction the runtime doesn't otherwise draw
between "a real protocol" and "a synthetic one," at the cost of a harness that can't just point at
`logit-cli`'s ordinary registry.

### A declarative event template, not a shape enum

`generate_in`'s `event:` block takes a small set of string fields — `log`, `attributes` values,
a metric name — each of which may contain `{seq}`/`{seq%N}` placeholders, rather than an enum of
canned event shapes (`Nginx`, `Statsd`, `Wide`, …) the way `crates/logit-bench/src/fixtures.rs`
enumerates its own. A shape enum would need a new variant, and a `logit-perf` rebuild, for every
new scenario anyone wants to try; a template is composable instead — a scenario that wants JSON
parsed writes a JSON-shaped string into `event.log` and puts an ordinary `json` transform
downstream of the generator, the same way any real ingest pipeline parses its input. Parsing is a
downstream concern the template doesn't need to know about, matching how every other input in this
codebase already works: no listener pre-parses on the input's behalf what a transform is meant to
do.

The template parser itself, `logit_core::template`, is deliberately **consumer-agnostic** — it
knows the grammar (`{name}` placeholders in literal text, `{{`/`}}` to escape a literal brace) and
nothing about which names are valid or what they resolve to. `Template::compile` takes a resolver
closure that maps each var name to a consumer-chosen enum once, at construction, so the per-event
hot path is rendering pre-resolved segments with no string matching left in it. `generate_in` is
the first, and for this ADR the only, consumer — it compiles `seq`/`seq%N` into `GenVar::{Seq,
SeqMod(u64)}` and rejects anything else at config-validation time. It is placed in `logit-core`,
not `logit-inputs`, specifically so it can be reused without a dependency change: both
`logit-config` (which needs it to validate `event:` at graph-build time) and `logit-inputs` (which
needs it to render) already depend on `logit-core`, and an impl crate reaching sideways into
another impl crate isn't a shape this codebase has anywhere else. That placement is also the whole
point, not an incidental convenience — a future `stdio_out` custom line format is exactly the kind
of consumer this was built to let land without touching `logit_core::template` at all: it would
compile `attributes.host`/`timestamp`/`log.message` into its own resolved field-accessor enum
against the same parser, the same way `generate_in` compiles `seq`.

### CPU microseconds per event is the headline number; wall clock and RSS ride along

Every scenario run reports three numbers, but they don't carry equal weight. `docs/design/
memory.md` §2 already records, more than once, that a `script/bench` run on a busier machine comes
out "uniformly ~20% slower across every unchanged benchmark" — wall-clock throughput on a shared or
otherwise-loaded box is contaminated by whatever else that box is doing, and this harness runs on
the same kind of machine the rest of the dev-container workflow already assumes, not a dedicated,
isolated bench host. `(ru_utime + ru_stime) / count` from the same `wait4` call doesn't have that
problem: CPU time actually spent by this process is far less sensitive to what else the box is
doing, which is what makes it the signal `compare` gates a regression on. Events/s (from wall time
to the generator's own completion line) and peak RSS (`ru_maxrss`) are recorded and shown alongside
every run — they're real, useful numbers, RSS especially so for the `buffered` disk-spool scenario
— but neither is the thing a before/after check fails the comparison on.

### Results as local, gitignored JSON, `compare` as the tool, docs tables as the record

A run writes one timestamped JSON file per invocation under `perf/results/`, and `compare` reads
two of them back. That split — machine-readable, ephemeral, per-run artifacts feeding a small
purpose-built comparison tool — is deliberately how `memory.md` itself already records numbers:
every table in that document is a hand-copied snapshot from one run, captioned with the machine and
the day, precisely because a raw number with no comparison context and no reproduction recipe isn't
worth much on its own. `perf/results/` is gitignored for the same reason `/target` is — it's
per-run/per-machine output, not source — while `docs/design/performance.md`'s tables stay the
durable, hand-curated record of what a measured run showed and when, following that same
established convention rather than inventing a second one.

### Per-node attribution via `internal → file_out format: native`, decoded by the harness

Attribution needs to know, for a given scenario, how much time each node spent — which is exactly
what `internal`'s own self-telemetry already carries as ordinary `process.duration`/
`send.blocked.duration` points, stamped with the emitting component's id
([`internal-telemetry.md`](../design/internal-telemetry.md)). The harness gets at those points by
appending an `internal` listener and a `file_out` sink to a temporary copy of the scenario, with
`format: native`, and decoding the resulting file with `logit_proto`'s own frame reader and native
decoder afterward. `native` is the only format this can use with byte-exact confidence: `format:
human` is a readable render meant for a person, not a wire format meant to round-trip, and there is
no `json` stream format on `stdio_out`/`file_out` at all — `StreamFormat` is `Human | Native`, full
stop. Decoding `internal`'s own wire format with the same codec the runtime uses to write it is the
only path that doesn't ask the harness to parse a human-readable render or invent a stream encoding
that doesn't exist elsewhere in the codebase.

That path needs one small runtime change beyond the harness itself: today, `internal`'s draining
ticker is cancelled by drop like any other listener on shutdown, which means the tick interval
between the last flush and the process exiting is never emitted — for a short-lived, finite
`generate_in` scenario where the whole run might last a handful of ticks, losing the last one is a
real fraction of the data, not a rounding error. `internal` gains a final drain on shutdown: an
override of `run_until_shutdown` that runs one more tick after the shutdown signal fires, before
returning, so the attribution dump reflects the whole run rather than all-but-the-last-interval of
it. This is a narrow, additive change to one listener's shutdown path, not a change to the
`Input`/`Output` shutdown contract itself.

### Profiling: `[profile.profiling]` and perf/inferno in a throwaway image

A flamegraph needs symbols and unwind info a stripped release build doesn't keep, so `flamegraph`
builds against a new root profile (`inherits = "release"`, `debug = "line-tables-only"`, `strip =
false`) rather than reusing `[profile.release]` itself or asking every release build to carry debug
info it doesn't need. The `perf`/`inferno` toolchain that turns a capture into an SVG lives in its
own throwaway image built from `logit-dev:local`, run with the elevated capabilities `perf record`
needs — precedent for a purpose-built, non-default container exists already in `script/protogen`.
`Dockerfile.dev`, the image every other `script/*` command runs in, is untouched: nothing about the
ordinary edit/check/test loop should carry `perf`'s extra tooling or its elevated container
capabilities.

Three flags turned out to be necessary, not two: `--cap-add SYS_ADMIN` and `--security-opt
seccomp=unconfined` were predicted (`perf_event_open`'s `CAP_SYS_ADMIN`/`CAP_PERFMON` requirement
and docker's default seccomp profile gating it), but this repo's own dev box (Fedora, SELinux
Enforcing) needed a third: `--security-opt label=disable`. With the default container label,
SELinux denies the `perf_event` class outright regardless of capabilities — `perf record` still
runs and still exits 0, but writes a zero-sample `perf.data`, which reads exactly like the
`kernel.perf_event_paranoid` capability problem the first two flags exist for and isn't. Nothing
in this ADR's "Alternatives considered" or `docs/plans/load-test-harness.md`'s risk list predicted
an SELinux-specific denial; `script/perf`'s own comment on the `flamegraph` path now carries the
full account, verified on this repo's actual dev box rather than assumed from precedent.

### A workspace member under `crates/`, not a `tools/*` standalone workspace

`logit-perf` is a new bin crate at `crates/logit-perf`, joining the root `[workspace]` — not a
sibling of `tools/protogen`/`tools/record-fixtures`, which are each their own, deliberately
separate `[workspace]` (see `tools/protogen/Cargo.toml`'s own comment on why: keeping `prost-build`
and its `protoc` dependency out of every ordinary build graph). `logit-perf` doesn't have that
problem to solve, and does have the opposite one: attributing per-node time means decoding a real
`native` frame, which means depending on `logit-proto` (and `logit-core` for the event/metric types
underneath it) directly, the same way `crates/logit-bench` already does. `tools/*`'s isolation
exists to keep something out of the main build graph; `logit-perf` needs to be squarely inside it
to reuse the exact codec the runtime it's measuring already ships.

### Open question, deliberately not decided here: when the harness runs

This ADR and its plan build the harness; they do not decide when or how often it runs. Running a
release build plus one or more multi-second scenario loads per PR or per commit would load the
build machine in a way the rest of `script/cibuild` deliberately doesn't — `script/bench` is
already kept out of `cibuild` for exactly this reason, and this harness is heavier than a divan
run, not lighter. For now, the harness is built and runnable by hand
(`script/perf run`/`compare`/`attribute`/`flamegraph`) and nothing wires it into CI, a pre-merge
gate, or a schedule. Deciding that — nightly runs, a manually-triggered workflow, a threshold that
fails a PR, some other cadence entirely — is real future work, tracked here as open rather than
answered, because answering it well needs experience running the harness by hand first.

## Alternatives considered

- **An in-process harness over `run_with_telemetry`.** Rejected — see "Measure the real binary"
  above. `logit-cli` is bin-only, and an in-process caller measures a different thing than a real
  `logit run` process.
- **A shape enum for generated events**, mirroring `crates/logit-bench/src/fixtures.rs`'s canned
  fixtures. Rejected — a new scenario would need a new Rust variant and a rebuild; a template
  composes with ordinary downstream transforms the same way every real input already does, and
  needs no `logit-perf` change to try a new shape.
- **A `json` stream format on `stdio_out`/`file_out`**, so attribution could decode
  human-readable-adjacent JSON instead of `native`. Rejected — no such format exists
  (`StreamFormat` is `Human | Native`), adding one purely to serve this harness would be scope this
  effort doesn't need, and `native` is already exact, already implemented, and already what the
  runtime itself would use for anything meant to round-trip.
- **Running the harness in CI**, gating PRs on a throughput or CPU/event threshold. Rejected for
  now, and recorded above as an open question rather than a decision — see that section for why.

## Consequences

- Two new, unconditionally-shipped `ComponentKind` variants (`generate_in`, `null_out`) join the
  registry; every existing config is unaffected.
- `logit_core::template` becomes a real, reusable module — the first consumer is `generate_in`, and
  it's designed for a second one (a future `stdio_out` custom line format) to compile against
  without touching the parser.
- `internal`'s shutdown path gains a final drain tick — a small, narrowly-scoped behavior change to
  one listener, needed for attribution accuracy on short scenario runs, that also improves
  `internal`'s general fidelity on any short-lived process, not just under this harness.
- A new workspace member, `crates/logit-perf`, with its own `Dockerfile` for the profiling image;
  `Dockerfile.dev` and the production `Dockerfile` (which builds `-p logit-cli` only) are both
  unaffected.
- `perf/results/` joins `.gitignore`; `perf/scenarios/*.yaml` join the set of shipped configs
  `script/validate` and `every_shipped_config_loads_and_validates` cover.
- The question of running this in CI, and on what cadence or gate, is explicitly left open — a
  follow-on decision, not assumed by anything built here.
