---
created: 2026-09-12
updated: 2026-09-21
---

# Enabling plan: a load-test harness for the real `logit` binary

## Context

[ADR `load-test-harness`](../adr/load-test-harness.md) decides the shape of this effort: measure a
real `logit run <config>` process rather than an in-process harness; a declarative event template
in `logit_core::template` rather than a shape enum, designed for reuse by a future `stdio_out` line
format; two unconditionally-shipped component kinds, `generate_in` and `null_out`; CPU
microseconds per event as the headline regression signal, with events/s and peak RSS alongside;
per-node attribution via a temporary `internal → file_out format: native` leg the harness decodes
itself, which needs a small final-drain change to `internal`'s shutdown path; a new workspace
member, `crates/logit-perf`, rather than a `tools/*` standalone workspace, because attribution
needs `logit-proto`/`logit-core` directly; and profiling tooling (`perf`/`inferno`) confined to a
throwaway image, `Dockerfile.dev` untouched. This plan is the concrete build-out of that decision:
what lands in which order, in which files, against which scenarios, and how each piece is
verified. Read the ADR first — this document doesn't repeat its reasoning, only its consequences.

`docs/design/memory.md` §7 and `crates/logit-bench/benches/pipeline.rs`'s own module doc name the
gap this closes: everything below the runtime boundary (decoders, transforms, encoders, one
single-threaded channel hop) is already measured and pinned; a full multi-node graph on the real
runtime — worker threads, cross-thread channel hops, bounded queues, sink buffers, a Lua OS thread
— is not.

## Workstreams

Stacked branches `feat/perf-w<N>`, one PR each, following the sub-agent/PR mechanics already
established for the `collectd`/`lossless-transit` efforts.

| # | Scope | Files | Done when |
|---|---|---|---|
| W0 | **Landed** (#147). **Docs.** This ADR and plan. | `docs/adr/load-test-harness.md` (+ row atop `docs/adr/README.md`); `docs/plans/load-test-harness.md` (this file, + row atop `docs/plans/README.md`) | Both docs exist, follow `TEMPLATE.md`'s headings exactly, every relative link resolves, and both README indexes gain a row in `created`-order. Docs only — no code, no `script/cibuild`. |
| W1 | **Landed** (#149). **`logit_core::template` + config types + graph rules.** The parser (`Template`/`Segment`/`Compiled<V>`/`TemplateError`), `GenerateEvent`/`GenerateMetric`/`GenerateMetricKind` and the `GenerateIn`/`NullOut` `ComponentKind` variants, `role`/`kind_name`/`is_implemented` wiring, graph rule 42 (reject `count: 0`/`batch: 0`/`rate: 0`, empty `metric.name`, non-finite `metric.value`, empty attribute/resource key, a malformed placeholder) plus a `receive:`-on-`generate_in` rejection test against existing rule 17. | `crates/logit-core/src/template.rs` (new) + unit tests; `crates/logit-config/src/lib.rs` (new types, two `ComponentKind` variants); `crates/logit-pipeline/src/graph.rs` (rule 42 + tests); `schema/logit.schema.json` (regenerated); `docs/design/pipeline-graph.md` (arity rows + rule 42 + rule 17 note) | `script/cibuild` green; `script/schema` produces no diff after commit; deserialization tests cover both new component kinds including their defaults; a `generate_in` with a `receive:` block fails validation; two disconnected `generate_in → null_out` chains resolve as a valid graph. `pr-review --comment` addressed. |
| W2 | **Landed** (#151). **`generate_in` implementation.** Templates compiled once at construction, attribute keys interned once, `Arc`-shared resource per batch, the two render paths (prerendered prototype clone vs. per-event template render), wall-clock `rate` pacing, the `generation complete` completion line, the `build_spec` arm. | `crates/logit-inputs/src/generate.rs` (new) + unit tests (exact count, partial last batch, prerendered `Bytes` pointer sharing, `{seq%N}` per-event, all three metric kinds, `Arc::ptr_eq` resource reuse, paused-clock rate pacing); `crates/logit-cli/src/pipeline.rs` (`build_spec` arm after `Internal`); `crates/logit-cli/tests/exit_codes.rs` (finite `generate_in → null_out` exits 0 with a `generation complete` line and no `drain complete`); `examples/generate-to-null.yaml`; `crates/logit-bench/src/fixtures.rs` + `benches/pipeline.rs` (divan bench, both render paths) + `crates/logit-bench/tests/allocations.rs` (exact-alloc cases) + `docs/design/memory.md` (new rows) | `script/cibuild` green; the exit-code test proves the finite-generator shutdown cascade end to end; allocation-count assertions committed alongside their `memory.md` rows in the same commit, never relaxed to an inequality. `pr-review --comment` addressed. |
| W3 | **Landed** (#150). **`null_out` implementation.** A sink whose `send` always succeeds and does nothing, `duplicate_safe() = true`, no telemetry override (layer 2 already counts received/`send.duration`), the `build_spec` arm with `queue_config`/`write_config` so `buffer:` (including disk) still applies. | `crates/logit-outputs/src/null.rs` (new) + unit tests (`send` ok, `duplicate_safe`, `build_spec_builds_a_null_sink`); `crates/logit-cli/src/pipeline.rs` (`build_spec` arm after `PrometheusOut`) | `script/cibuild` green; a `null_out` behind `buffer: { disk: ... }` builds and runs. `pr-review --comment` addressed. |
| W4 | **Landed** (#148). **`internal`'s final drain on shutdown.** Override `run_until_shutdown` with a `select!` over the ticker and the shutdown signal; run one more tick after shutdown fires, then return, so the last partial interval isn't lost on a short-lived process. `DdSketch::sum()` accessor the harness's `attribute` command needs to total a distribution's contribution. | `crates/logit-inputs/src/internal.rs` (`run_until_shutdown` override + paused-clock test proving the final tick lands after shutdown and before return); `crates/logit-core/src/metric.rs` (`DdSketch::sum()` + test) | `script/cibuild` green; the paused-clock test proves the final drain's own ordering (the `Fanout` stays open until this future returns, so downstream inboxes see the last tick); documented one-tick-behind caveat for the drain's own `points.emitted`. `pr-review --comment` addressed. |
| W5 | **Landed** (#152). **`crates/logit-perf` scaffolding: `run`/`compare`/`list`, `script/perf`, wiring.** The bin crate itself, the results JSON schema, `run`'s build-once/spawn/`wait4` loop, `compare`'s per-scenario Δ% and threshold-gated exit code, `list`, the first three scenarios (`passthrough`, `json-parse`, `fanout` — the ones needing nothing from W6). | `crates/logit-perf/{Cargo.toml,src/main.rs,src/run.rs,src/compare.rs,src/scenario.rs,...}` (new workspace member, `publish = false`); root `Cargo.toml` (`members` list); `script/perf` (new); `Makefile` (`perf` target); `AGENTS.md` (script table row); `.gitignore` (`/perf/results/`); `perf/scenarios/{passthrough,json-parse,fanout}.yaml`; `script/validate` (loop addition); `crates/logit-cli/src/config.rs`'s `every_shipped_config_loads_and_validates` (the scenarios list) | `script/cibuild` green including the new crate under clippy `-D warnings`; unit tests cover the results JSON round-trip, median/min computation, and `compare`'s verdict against canned JSON fixtures with no process spawned; `script/perf run --repeat 3` writes a results file and prints a table; `script/perf compare a b --threshold 0` exits 1 on any regression and 0 on identical files; `script/validate` and the shipped-config test both cover the three new scenario YAMLs. Lighter lead review (delegated read-through + `script/cibuild`), not a full `pr-review`. |
| W6 | **Landed** (#153). **`attribute` + `flamegraph` + `[profile.profiling]` + perf image.** The temp-scenario-copy + `internal`/`file_out` append, SIGTERM-after-settle, native-frame decode + per-node grouping/sort; the `[profile.profiling]` root profile; `crates/logit-perf/Dockerfile` and the `perf record`/`inferno` pipeline. | `crates/logit-perf/src/{attribute.rs,flamegraph.rs}`; root `Cargo.toml` (`[profile.profiling]`); `crates/logit-perf/Dockerfile` (new, `FROM logit-dev:local`); `docs/design/internal-telemetry.md` (both kinds are layer-2 only; `internal`'s final drain) | `script/perf attribute --scenario json-parse` shows per-node received/sent summing to the scenario's `count`, with `json` showing the largest Σ process time; `script/perf flamegraph --scenario passthrough` writes an SVG with resolved symbols; `Dockerfile.dev` diff is empty. The `[profile.profiling]`/validate-loop parts get `pr-review --comment`; the perf-image plumbing gets lighter lead review. |
| W7 | **Landed as two PRs, W7a/W7b** (#154 W7a; this PR W7b). **Remaining scenarios + `performance.md` + docs closeout.** `aggregate`, `lua`, `native-relay`, `encode-human-devnull`/`encode-native-devnull`, `buffered`; first recorded results; the rest of the docs sweep. | `perf/scenarios/{aggregate,lua,native-relay,encode-human-devnull,encode-native-devnull,buffered}.yaml`; `docs/design/performance.md` (new: methodology, reproduction table, first results table with host/sha/cpu preamble, before/after workflow, reading `attribute` output); `docs/design/memory.md` (§7/open-questions pointer to `performance.md`); `docs/known-gaps.md` (rate granularity above ~1k batches/s; no cross-run noise model); `docs/OVERVIEW.md`/`AGENTS.md` (where-things-live + current-state blurbs) | All scenarios in `script/validate` and the shipped-config test; `script/perf run` covers every scenario in one pass in the 5-10s/scenario range on the dev box; `performance.md` carries one real recorded run. Lighter lead review (delegated read-through + `script/cibuild`). |

Planned landing order: **W0 → W1 → (W2, W3) → W5 → W6 → W7**, with W4 branching independently from
`main` and landing whenever it's ready — before W6, which depends on it. W2 and W3 both branch from
`feat/perf-w1` and can proceed in parallel since neither touches the other's files. Each PR targets
its parent workstream's branch and is retargeted to `main` once that parent merges, the same
convention `collectd-binary-relay.md` uses, so later workstreams aren't blocked on every earlier PR
landing first.

**Actual landing order** (by merge time, not PR number): #147 (W0) → #149 (W1) → #148 (W4) → #150
(W3) → #151 (W2) → #152 (W5) → #154 (W7a) → #153 (W6) → this PR (W7b). W4 did land independently
and early, as planned — right after W1, well ahead of W6, the workstream that depends on it. W2 and
W3 landed in the opposite order from the table above (W3 first) but both still branched from
`feat/perf-w1` and stayed parallel, so that reordering cost nothing. W6 and W7a did not land in
their planned order: W7a (the remaining scenarios + this doc closeout's groundwork) merged before
W6 (`attribute`/`flamegraph`) did, even though W6 was scoped and reviewed first — a queuing
artifact of the stacked-PR/retarget mechanics, not a dependency violation (W7a's own scenarios
don't need anything W6 built).

**This PR (W0) is docs only** — no code, no `script/cibuild` run, per the ADR's own scope. W1
onward touch code and follow the review cadence above: `pr-review --comment` on every workstream
that touches a runtime/config/graph code path (W1 through the `[profile.profiling]`/validate-loop
half of W6), and lighter, delegated lead review for scaffolding and docs-heavy workstreams (W5's
harness crate, W6's perf-image plumbing, W7's scenarios and `performance.md`).

## Scenarios

`perf/scenarios/*.yaml` are ordinary, shipped `logit` configs — no `!env`, inline `script:` for any
Lua a scenario needs — validated the same way every other example config is.

| Scenario | Graph | Measures | Count | events/s (measured) |
|---|---|---|---|---|
| `passthrough` | `generate_in` (6 attributes, no log) → `null_out` | Runtime floor: scheduling, channel hops, no parsing | 25M | ~3.58M/s |
| `json-parse` | `generate_in` (JSON body) → `json` → `kv_metrics` → `null_out` | The parse-into-attributes path | 19M | ~2.07M/s |
| `aggregate` | `generate_in` (distribution metric, `host: h{seq%1000}`) → `aggregate` (1s window) → `null_out` | Aggregation + flush-tick cost | 20M | ~3.09M/s |
| `lua` | `generate_in` → `lua` (inline enrichment script) → `null_out` | The Lua hop and its event proxy | 4M | ~0.60M/s |
| `fanout` | `passthrough`'s `generate_in` (same 6 attributes) → 3 × `null_out` | `Arc`-based fan-out to multiple sinks, read against `passthrough` | 25M | ~2.59M/s |
| `native-relay` | `generate_in` → `logit_out` → `logit_in` → `null_out` (one graph, one process) | Native encode + decode + per-batch ack round trip | 7M | ~0.88M/s |
| `encode-human-devnull` | `generate_in` → `file_out` (`/dev/null`, `format: human`) | The human-readable encoder in situ | 8M | ~1.23M/s |
| `encode-native-devnull` | `generate_in` → `file_out` (`/dev/null`, `format: native`) | The native encoder in situ | 10M | ~1.41M/s |
| `buffered` | `passthrough`'s graph with `buffer: { disk: ... }` on the sink | Disk-backed sink-buffer spool cost | 1.2M | ~0.71M/s (median; see note) |
| `buffered-small-segments` | `buffered` with `segment_bytes: 1MiB` | Segment-roll cost (a durable cursor persist and an unlink per roll, ~70 per run), read against `buffered` | 1.2M | ~0.67M/s on `main` (`performance.md` §3) |
| `route` | `passthrough`'s `generate_in` → `route` (by `host`) → 3 × `target` → 3 × `null_out`, plus an unrouted `null_out` | The router hop and `target` delivery, read against `passthrough` | 25M | ~3.15M/s |
| `logfmt-parse` | `generate_in` (logfmt line, `fixtures::LOGFMT_LINE`) → `logfmt` → `null_out` | The logfmt parse alone; nine interner probes per event until `logfmt` adopts `KeyCache` | 9.5M | ~1.60M/s |
| `json-parse-x3` | `json-parse`'s `generate_in` → 3 × `json` → 3 × `null_out` (no `kv_metrics`) | Three parsers contending on the process-wide interner at once, read against `json-parse` | 9.5M | ~1.44M/s (generated; each parsed 3×) |
| `json-parse-app-log` | `generate_in` (`fixtures::FLAT_JSON_LOG_BODY` + `tail_in`'s `log.file.path`) → `json` → `null_out` | The parse at the commonest *measured* log width, 12 attributes (`docs/design/data-shapes.md` §5.3), read against `json-parse` | 12M | run, not yet retuned (10.9 s/repeat) |
| `json-parse-nested-log` | `generate_in` (`fixtures::PINO_HTTP_LOG_BODY` + the same path attribute) → `json` → `null_out` | The same parse on a *nested* record: 10 attributes but four boxed `Value::Map`s | 8M | run, not yet retuned (13.9 s/repeat) |
| `json-parse-access-log` | `generate_in` (`fixtures::POSTGRES_JSONLOG_BODY` + the same path attribute) → `json` → `null_out` | The widest, highest-rate log class, 30 attributes — the only shipped scenario whose `AttrMap` reallocs | 5M | run, not yet retuned (12.4 s/repeat) |

The last three rows are [`docs/plans/event-sizing.md`](event-sizing.md)'s W1 (2026-09-21): three
widths of the same `json` parse, so a sizing arm can be read against the bimodal log population
`docs/design/data-shapes.md` §6 describes rather than against one shape. Their counts were first
estimates scaled off `json-parse`'s by key count. **They have since been run** — the event-sizing
bake-off (`docs/design/performance.md` §8) ran all three on `main`, median of six repeats — and none
landed in the 5–10 s band: 10.9 s, 13.9 s, and 12.4 s respectively, all overshooting. Their counts
were then lowered to 9M, 4.5M and 3M — each scaled to ~8 s from its measured wall time; a
`--repeat 5` pass against this table's own protocol, at the new counts, is the next thing owed to
them.

Counts target roughly 5-10 seconds of wall time each. **Retuned 2026-09-20 for the disposable perf
VM** (`docs/adr/disposable-azure-perf-vm.md`), the project's reference box — most scenarios already
sat in the target band at their original, dev-laptop-tuned counts and were left unchanged
(`aggregate`, `buffered`, `encode-human-devnull`, `lua`, `native-relay`); `encode-native-devnull`,
`json-parse`, `json-parse-x3`, `logfmt-parse`, and the `passthrough`/`fanout`/`route` trio (which
share one count by design, so retuned together) needed raising. `native-relay` never self-exits
(`logit_in` is a socket listener), so it's the scenario that exercises the harness's
settle-then-SIGTERM path rather than the plain wait-for-exit path every other scenario takes.

`events/s` above is now a clean median from a real `--repeat 5` recorded run on an idle reference
VM (`docs/design/performance.md` §1), not a range observed while tuning on a contended machine —
this table used to carry the latter, from the dev-laptop tuning pass, with a caveat about
concurrent `script/perf` work inflating the spread. `buffered` was the outlier by far under that
old regime: real disk I/O (segment writes, periodic checkpoints) made it far more sensitive to
contention than any in-memory scenario — observed throughput ranged from roughly 16k to 790k
events/s across repeated runs at the same count on the laptop, a swing since resolved (a harness
bug, not the box or the scenario — `docs/design/performance.md` §3 has the full account) and not
reproduced at anywhere near that magnitude on the VM;
expect its wall time to vary more than every other scenario's when re-run.

`fanout`'s count was bumped again, 25M → 55M, after the first *solo, uncontended* recorded run
([`docs/design/performance.md`](../design/performance.md)) showed it clearing 25M events in ~3.2s
— comfortably under this table's target band once nothing else was loading the machine at the same
time. Every other scenario's original count held up under that same solo run.

`route` was added on 2026-09-14 (after the target/route stack, #155–#181, landed) as the one
scenario exercising a router and `target`s; its numbers are in `performance.md` §1.

`fanout` was then re-shaped on 2026-09-14 to generate `passthrough`'s exact six-attribute event
at `passthrough`'s exact count (20M then, 25M since the 2026-09-20 VM retune above — the two have
moved together every time either has changed, by design), rather than its original one-attribute
event at 55M. The
original shape made `fanout` look cheaper per event than `passthrough` (0.466 vs 0.478 µs on the
quiet run), which was read as the delivery path being cheap — it was actually a 6× lighter
generator. Holding the event shape fixed, one consumer is cheaper than three in both directions
(`performance.md` §1). The two scenarios now differ only in consumer count, which is what the
scenario was always meant to isolate; on a quiet box it clears 20M in ~6.0s, inside the target
band without a further retune.

## Verification

- Every code-bearing workstream (W1 onward): `script/cibuild` green (fmt, clippy `-D warnings`
  including the new `logit-perf` crate once it exists, nextest, deny/audit); `script/schema`
  produces no diff for any workstream touching a config type (W1); `script/validate` and
  `every_shipped_config_loads_and_validates` cover every scenario YAML as it's added.
- `script/perf run --repeat 3` writes a timestamped results file under `perf/results/` and prints a
  table (W5).
- `script/perf compare a.json b.json --threshold 0` exits 1 on any regression in events/s or CPU
  µs/event, 0 when comparing a results file against itself (W5).
- `script/perf attribute --scenario json-parse` decodes a per-node breakdown whose received/sent
  sums equal the scenario's configured `count`, with the `json` node showing the largest Σ
  `process.duration` (W6).
- `script/perf flamegraph --scenario passthrough` produces an SVG with resolved symbols (W6).
- Manual, not automated: `generate_in` with `rate: 10000` piped through `internal → stdio_out`
  shows roughly 10k events/s.
- This workstream (W0): no `script/cibuild` — verified by every relative link resolving, both new
  docs' headings matching `docs/adr/TEMPLATE.md` exactly, and both README indexes gaining a row in
  `created`-order.

## Open risks

Carried over from the ADR and worth tracking as the later workstreams land, not repeated in full
here — see [ADR `load-test-harness`](../adr/load-test-harness.md) for the reasoning behind each:
`{seq}` template rendering allocates on the hot path (pinned by W2's allocation tests, not a
regression to chase); `rate` pacing is millisecond-granular above roughly 1k batches/s; cross-machine
wall-clock noise means `compare` warns on a host/CPU-model mismatch rather than trusting wall time
across machines, with CPU µs/event as the actual gate; `native-relay` needs the SIGTERM-after-settle
path exercised for real, not just for scenarios that self-exit; and a container's `perf record`
needs `CAP_SYS_ADMIN` plus an unconfined seccomp profile, worth a one-time verification against this
repo's actual dev-container base image rather than assumed from precedent. When and how this
harness runs in the ongoing process — per-PR, nightly, manually triggered, gating or advisory — is
the ADR's open question, not a risk to this plan: nothing here assumes an answer to it.

## Closing assessment

W0-W7 have all landed (PR numbers on each row above); the harness the ADR decided on is built,
runnable by hand, and now carries one real recorded run
([`docs/design/performance.md`](../design/performance.md)) rather than only a design.

- **The measurement gap `memory.md` §7 named is closed.** All ten scenarios in the table above
  exist, run in the 5-10s-per-scenario range this plan targeted (`docs/design/performance.md`'s
  results table), and `script/perf run --repeat 3` produces exact-equality-free but real,
  repeatable events/s, CPU µs/event, and peak RSS numbers for a real release-profile `logit run`
  process — not a microbenchmark, not a one-off hand-transcribed number. `compare --threshold`
  gates on CPU µs/event as designed; `attribute` decodes a real per-node breakdown
  (`json-parse`'s `kv_metrics`/`json` split, `aggregate`'s single-node verdict, both in
  `performance.md`); `flamegraph` produces a real, symbol-resolved SVG through the throwaway
  profiling image, three container flags deep (`CAP_SYS_ADMIN`, unconfined seccomp, and — found
  only by running it for real on this repo's own Fedora/SELinux dev box, not predicted by the ADR
  — `label=disable`).
- **One real, unpredicted finding came out of first use, not design review:** `buffered`'s
  wild run-to-run swings (W7a: 16k-790k events/s) turned out to be dominated by
  `DiskQueue::open`'s mandatory whole-active-segment validation read, compounding across repeats
  and runs that share a spool the harness never clears — see `performance.md`'s `buffered` notes,
  the scenario's own updated comment, and `docs/known-gaps.md`'s new entry. That's exactly the
  kind of thing this plan's "build the harness only, how it runs in the process is TBD" scoping
  was for: a real finding from real use, not a decision to reverse-engineer from a design doc.
- **The quiet-machine follow-up this section used to defer has landed:** `buffered`'s harness-side
  spool-clearing fix (W8, #165) is now confirmed idle and on battery, not only on a busy box —
  `performance.md`'s quiet-machine run closes out the spool-accumulation variance, leaving only a
  narrower, product-side question (`DiskQueue::open`'s double-read startup scan) open in
  `docs/known-gaps.md`.
- **What's left is exactly what the ADR left open, plus the one bug first use surfaced** — all
  tracked in `docs/known-gaps.md` rather than newly discovered here: `generate_in`'s `rate:`
  millisecond pacing granularity above ~1k batches/s (a known limitation nothing shipped
  currently exercises), `compare`'s lack of a cross-run noise model (`aggregate`'s own ordinary
  variance trips it, not just `buffered`'s old one), `DiskQueue::open`'s double-read startup scan
  (the narrowed, product-side remainder of `buffered`'s investigation), and — the ADR's own
  explicitly-deferred decision — when and how this harness runs in the ongoing development
  process. None of these block using the harness by hand today, which is all this plan ever
  promised.

## Extension, 2026-09-18: real-socket scenarios

[ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)
and [`docs/plans/udp-intake.md`](udp-intake.md)'s W2 add a second workload kind to this harness:
`Workload::{Generated, Driven}` on `Scenario`, a sidecar load-spec directory (`perf/load/`, with
its own README), a `sendmmsg(2)` sender inside the `logit-perf` process (`load.rs`), the
`internal` telemetry leg hoisted out of `attribute.rs` into `telemetry_leg.rs` and shared with
`run`, optional `udp:` fields on `Sample`/`compare`, and `--pin-sender`/`--pin-child`.

Everything this plan built stays as it was — every `generate_in` scenario runs on exactly the
path it always did, and the four checks `scenario.rs` applies to a `generate_in` component are
unchanged, message for message. What is new is a scenario kind this plan could not have measured:
one where the load arrives over a real socket, the denominator is events *delivered* rather than
generated, and a drop rate is a first-class number rather than an impossibility. The details, the
calibration against a recorded real-client capture, and the tuning live with that plan and with
`perf/load/README.md`, not here.
