# AGENTS.md

Guidance for AI coding agents working in this repo. Humans: see [README.md](README.md).

## What this is

`logit` is a logging/metrics/tracing multiplexer written in Rust, with user transforms in
LuaJIT. Read [docs/OVERVIEW.md](docs/OVERVIEW.md) first (~1 page) for scope and positioning, then
[docs/adr/](docs/adr) for *why* the stack is what it is, then [docs/design/](docs/design) for the
internal event model, the Lua scripting API, the pipeline component graph, the native wire
protocol, and internal telemetry — those five design docs are load-bearing; don't improvise around
them without reading them first. Check [docs/known-gaps.md](docs/known-gaps.md) before "fixing"
something that looks broken — it's likely a documented, deliberate gap, not an oversight.

**Current state:** v0.1's statsd/InfluxDB slice is complete — statsd in, a 10s `aggregate` window, a
Lua enrichment stage, InfluxDB 2.x out, via `logit run <config>` (see
[examples/statsd-to-influxdb.yaml](examples/statsd-to-influxdb.yaml), `script/server`). Since then,
`syslog_in`, `stdio_out`, `otlp_in`, and `otlp_out` (`crates/logit-inputs`/`crates/logit-outputs`,
`crates/logit-proto`'s `otlp` codec) and `json`, `kv_metrics`, `keep`, `remove`, and `set`
(`crates/logit-transforms`) have all landed as real, implemented `ComponentKind`s —
[examples/nginx-to-influxdb.yaml](examples/nginx-to-influxdb.yaml) exercises the syslog/InfluxDB
side together against a real nginx (`examples/nginx/`), and
[docs/deploying.md](docs/deploying.md) is the operator-facing doc for running any of this outside
the dev stack. `examples/` is contributor-facing fixtures the dev stack (`script/server`) runs
against, kept real because other things in the repo depend on them (`compose.yaml`'s `nginx`
service, `crates/logit-bench/src/fixtures.rs`'s `NGINX_SYSLOG_LINE`) — `demo/` is the answer to
"let me see this work" for anyone else, a self-contained `docker compose up` against the release
image. All three signals now flow through it end to end — logs, metrics, and traces alike, into
Loki, InfluxDB, and Tempo respectively
([docs/plans/demo-stack.md](docs/plans/demo-stack.md),
[docs/plans/otlp-end-to-end.md](docs/plans/otlp-end-to-end.md)). `syslog_out` (RFC
3164/5424 over UDP or TCP, header fields round-tripped from an event's `syslog.*` attributes,
[ADR `syslog-output`](docs/adr/syslog-output.md)) is implemented and fully covered by its own
unit/integration tests but no longer exercised by the demo, which moved its log leg onto
`otlp_out` straight to Loki ([docs/plans/otlp-logs-and-resource-identity.md](docs/plans/otlp-logs-and-resource-identity.md)'s
workstream B) — the demo isn't meant to stay exhaustive over every component as more land.
`otlp_in`/`otlp_out` (`crates/logit-inputs`/`crates/logit-outputs`, OTLP for logs,
metrics, and traces, both OTLP/HTTP and a hand-rolled OTLP/gRPC transport,
[ADR `committed-pregenerated-otlp-protobuf`](docs/adr/committed-pregenerated-otlp-protobuf.md)/
[ADR `hand-rolled-grpc-over-hyper`](docs/adr/hand-rolled-grpc-over-hyper.md)) are real, implemented `ComponentKind`s —
`otlp_out` is live in `demo/logit.yaml`'s both `log_out` (HTTP, straight to Loki) and `trace_out`
(gRPC, to Tempo); `otlp_in` ships tested but unexercised by the demo. Config is a flat graph of named components (ADR `component-graph-configuration`,
[pipeline-graph.md](docs/design/pipeline-graph.md)) resolved and validated by
`logit-pipeline::graph`, then run by `logit-pipeline::run`'s node runtime -- `logit-cli::pipeline`
is now just the kind → implementation registry. Config files are read and parsed exclusively
through `logit_cli::config::load` (`crates/logit-cli/src/config.rs`), which also resolves `!env
VAR_NAME` -- any field on any component can pull its value from the environment this way (ADR
0011), which is why `influxdb_out`'s `token` is a plain string, not an env-specific field. `logit
graph <config>` prints the resolved graph as graphviz DOT (`crates/logit-cli/src/dot.rs`).
`logit run` rejects a config referencing any other unimplemented kind with a clear error; see
[ADR `aggregation-window-semantics`](docs/adr/aggregation-window-semantics.md) for `aggregate`'s windowing semantics,
[ADR `json-parsing-into-attributes`](docs/adr/json-parsing-into-attributes.md) for `json`'s parsing semantics,
[ADR `kv-metrics-semantics`](docs/adr/kv-metrics-semantics.md) for `kv_metrics`'/`keep`'s semantics,
[ADR `service-lifecycle-and-output-retry`](docs/adr/service-lifecycle-and-output-retry.md) for signal-driven shutdown and
`influxdb_out`'s bounded output retry, `crates/logit-inputs/src/statsd.rs` and
`crates/logit-outputs/src/influxdb.rs` for the listener/sink side, `crates/logit-pipeline/src/runtime.rs`
for orchestration and the per-node flush-tick timer. `internal` (`crates/logit-inputs/src/internal.rs`)
is `logit` observing itself — a listener like any other, draining `logit_core::telemetry`'s
per-component buffers into ordinary events on its own `interval`; see
[ADR `internal-telemetry-as-pipeline-events`](docs/adr/internal-telemetry-as-pipeline-events.md) and
[internal-telemetry.md](docs/design/internal-telemetry.md) for the framework, and
[examples/internal-telemetry.yaml](examples/internal-telemetry.yaml) for a runnable config. Every
`Delivered` (one `Fanout` edge's channel payload) carries a real `TraceContext`, propagated as a
child of its parent for the two node kinds with an unambiguous one to propagate
(`Transform::process`/`ScriptWorker::process`'s non-flush path, and `run_output`); see
[ADR `trace-context-propagation-on-delivered`](docs/adr/trace-context-propagation-on-delivered.md) and
[pipeline-graph.md](docs/design/pipeline-graph.md)'s "Trace context propagation" section. That
context is now a real `SpanRecord` too, not just substrate -- every node visit (a listener's send,
a transform's process/flush, a sink's deliver) mints exactly one span, deterministically sampled on
`trace_id` (`span_sample_rate`, default `0.1`, `1.0` in the demo) so every `logit` process in a
split-collection topology reaches the same keep/drop verdict independently with no propagated bit;
see [ADR `internal-span-emission-and-deterministic-sampling`](docs/adr/internal-span-emission-and-deterministic-sampling.md) and
`internal-telemetry.md`'s "Spans" section. `docs/known-gaps.md`'s internal-spans entry tracks what's
still open (the listener span's window, Lua `flush()`'s link-less root). `otlp_out` is what carries
those spans, and the `internal` metrics alongside them, out over the wire (both OTLP/HTTP and
OTLP/gRPC, a hand-rolled unary gRPC client/server over `hyper` rather than `tonic`,
[ADR `hand-rolled-grpc-over-hyper`](docs/adr/hand-rolled-grpc-over-hyper.md)); `otlp_in` is the mirror, implemented and
tested but not yet exercised by the demo. `demo/`'s `trace_out` proves the whole chain against a
real Tempo, exactly the way `log_out` proves `syslog_out` against a real Loki. `statsd_in`/`syslog_in`
(`crates/logit-inputs/src/statsd.rs`/`syslog.rs`) are now thin wrappers over a shared
`logit-inputs::udp::UdpListener` driver: a UDP listener's socket read and its decode/batch-assembly
loop run decoupled through a `ReceiveQueue`, the listener-side mirror of `SinkQueue`'s sink-side
decoupling, so a stalled downstream no longer stops the socket being read; see
[ADR `decoupled-listener-io`](docs/adr/decoupled-listener-io.md) and the `receive:` config block it introduces.

## Environment

Everything runs in a container — **do not** assume Rust, LuaJIT, or `cargo` are on the host; they
usually aren't. Use `script/*`, not bare `cargo`:

| Command | What it does |
|---|---|
| `script/bootstrap` | Build the dev container image (run first, or after touching `Dockerfile.dev`) |
| `script/test [args]` | `cargo nextest run --workspace` |
| `script/lint` | `cargo clippy --workspace --all-targets -- -D warnings` |
| `script/format [--check]` | `cargo fmt --all` |
| `script/schema` | Regenerate `schema/logit.schema.json` — run after any `logit-config` type change, and commit the result |
| `script/validate` | `logit validate` over every shipped config (`demo/`, `examples/`) — part of `cibuild` |
| `script/bench [filter]` | `cargo bench -p logit-bench` — throughput + per-benchmark allocation counts. Not part of `cibuild` |
| `script/audit` | `cargo-deny` + `cargo-audit` |
| `script/cibuild` | The exact sequence CI runs, in order — run this before opening a PR |
| `script/console` | Interactive shell in the dev container, for anything not covered above |
| `script/image [tag]` | Build the production runtime image (`Dockerfile`, not `Dockerfile.dev`) |
| `script/demo [compose args]` | Run the self-contained demo stack (`demo/`) — the release image, no dev container |

All default to `sudo docker`; `DOCKER=docker` or `DOCKER=podman` overrides. See
[ADR `containerized-development`](docs/adr/containerized-development.md) and
[ADR `scripts-to-rule-them-all`](docs/adr/scripts-to-rule-them-all.md).

## Workflow

Work happens on a branch, landed via pull request — never commit straight to `main`. Run
`script/cibuild` locally before opening one; it's the same sequence `.github/workflows/ci.yml`
runs, so a clean local run means a clean CI run.

**To bring a branch with an open PR up to date with `main`, `git merge origin/main` — don't
rebase.** A rebase rewrites the branch's commits, which means a force-push to update the PR; that's
disruptive for an open PR (review-comment associations, anyone else with the branch checked out)
for no real benefit here. A merge commit costs nothing extra and pushes normally.

## Conventions to hold to

- **A new design decision worth remembering gets an ADR** (`docs/adr/<slug>.md`, copied from
  [`docs/adr/TEMPLATE.md`](docs/adr/TEMPLATE.md)) — not just a comment or a PR description. Name
  the file after the decision, not a number: parallel branches racing for "the next number" was a
  recurring source of merge churn (see [`docs/adr/README.md`](docs/adr/README.md) for the full
  index and the `created`/`updated` frontmatter that orders it). Check the existing ADRs before
  re-deciding something they already settled. `docs/plans/` follows the same convention.
- **`rustfmt.toml`/`clippy.toml` are enforced**, not advisory — `script/cibuild` fails the build on
  either. Run `script/format` before committing rather than hand-formatting.
- **Every config type derives `Serialize + Deserialize + JsonSchema` together**
  ([ADR `config-yaml-jsonschema`](docs/adr/config-yaml-jsonschema.md)) — the published schema is generated from
  the Rust types specifically so it can't drift. If `schemars` needs a hint `serde` doesn't give it
  (as with the hand-rolled `Duration` codec in `logit-config`), add `#[schemars(with = "...")]`
  alongside `#[serde(with = "...")]` rather than dropping the derive.
- **Stub code says so.** Unimplemented pieces are `todo!()` with a comment pointing at the design
  doc section and, where relevant, what to build next — see `logit-script`, `logit-proto`, and the
  `statsd`/`influxdb` stubs. Follow that pattern for new stubs rather than silently returning a
  default.
- **A config file is always read through `logit_cli::config::load`**, never a bare
  `std::fs::read_to_string` + `serde_norway::from_str` — that's what resolves `!env` and rejects an
  unknown YAML tag (ADR `env-yaml-tag`); a call site that bypasses it silently loses both.

## Design constraints that aren't optional

These come directly out of [docs/design/lua-api.md](docs/design/lua-api.md) and
[docs/design/data-model.md](docs/design/data-model.md) — violating them means redoing work later,
not a style preference:

- **`mlua::Lua` is neither `Send` nor `Sync`.** One Lua VM per pipeline worker, no implicit shared
  mutable state across workers. `ScriptWorker` in `logit-script` enforces this with a
  `PhantomData<*const ()>` marker — don't remove it to make something compile.
- **Events reach Lua through a proxy (`EventProxy`, userdata + metamethods), not a converted
  table.** The whole point is avoiding a full table conversion on every stage for every event —
  don't "simplify" this back into `event:to_table()`-by-default.
- **Metric kinds must stay mergeable.** `Distribution` needs a sketch with a real error bound
  (`DDSketch`, not a naive percentile), `Set` needs a real union (`HyperLogLog`) — this is what
  makes the split-collection topology in `docs/OVERVIEW.md` correct rather than approximate.
  `logit-core::metric::DdSketch` is a real wrapper with a working `merge` (`crates/logit-transforms`'
  `aggregate` is its first caller); `HyperLogLog` is still a stub pending a real crate — don't fill
  it with a non-mergeable implementation to get `Set` aggregation working faster.
- **The wire encoding (`rkyv` vs. hand-rolled) is an open, benchmark-gated decision** — see
  `docs/design/wire-protocol.md`. Don't pick one in passing while implementing something else;
  benchmark it and record the outcome as an ADR. `crates/logit-bench` is the harness to do it in.
- **Memory behavior is measured, not assumed** — `docs/design/memory.md` records what every
  pipeline stage allocates and what `Event` costs to move, and both are enforced by tests:
  `crates/logit-core/tests/type_sizes.rs` asserts exact `size_of`s, and
  `crates/logit-bench/tests/allocations.rs` asserts exact allocation counts per stage. They're
  exact equality on purpose. **When one fails, that's the test working** — decide whether the
  change is worth it, then update the constant *and* `docs/design/memory.md`'s table in the same
  commit. Don't relax an assertion to a `<=` bound to make it pass; that removes the only thing
  stopping `Event` from quietly growing.
- **Benchmark and test fixtures never depend on a running service.** No nginx, no InfluxDB, no
  container — `crates/logit-bench/src/fixtures.rs` holds `const` wire-format literals and
  directly-constructed events, and components are called directly rather than through the runtime
  (`docs/design/memory.md`'s "Fixtures" section has the pattern, including why a literal should
  carry provenance). Standing up a service against real software to *inform* a fixture is fine and
  encouraged; committing a fixture that needs one is not.
- **Don't generalize a measurement from one event shape.** `Event` carries any combination of log,
  metrics, and span, and `logit` targets logs-only, metrics-only, traces-only, and mixed pipelines
  alike (`docs/OVERVIEW.md`). The fixtures now cover logs-only, wide-JSON, distribution-heavy, and
  span shapes alongside the original mixed one, but that closes the *measurement* gap, not the
  sizing *decisions* those numbers feed — see `docs/design/memory.md` §0 and §8 before treating any
  one number as settled across workloads.

## Where things live

```
crates/
  logit-core        internal event model: Event, Value, Resource, metric kinds, interner, self-telemetry
  logit-config      YAML config types + generated JSON Schema
  logit-script      LuaJIT embedding (mlua), the Event proxy
  logit-proto       codec traits, native wire format, output buffering
  logit-pipeline    Input/Output/Transform traits, Fanout, graph resolution+validation, node runtime
  logit-inputs      per-protocol listeners implementing logit-pipeline::Input; statsd (v0.1 target), syslog, internal (self-telemetry)
  logit-outputs     per-protocol sinks implementing logit-pipeline::Output; InfluxDB (v0.1 target), stdio, syslog
  logit-transforms  native transforms implementing logit-pipeline::Transform; aggregate (v0.1 target), json, kv_metrics, keep, remove, set
  logit-cli         the `logit` binary: the kind → implementation registry, `Command::{Schema,Validate,Run,Graph}`
  logit-bench       dev-only: allocation-count tests + divan throughput benches (docs/design/memory.md)
```

`logit-inputs`/`logit-outputs`/`logit-transforms` depend on `logit-pipeline` for their trait, not
the other way around (`docs/design/pipeline-graph.md`'s "Crate layout" section) -- this is what
keeps the pipeline runtime from having to know about any concrete protocol or transform. A new
protocol (listener or sink) implements `logit_proto::Decoder`/`Encoder` plus
`logit_pipeline::Input` or `logit_pipeline::Output`, and gets a variant in `logit_config`'s
`ComponentKind` — follow the `statsd`/`influxdb` stubs as the template. A new native transform
implements `logit_pipeline::Transform`, following `logit-transforms::Aggregator`.
