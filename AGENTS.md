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
`syslog_in` and `stdio_out` (`crates/logit-inputs`/`crates/logit-outputs`) and `json`, `kv_metrics`,
`keep`, and `remove` (`crates/logit-transforms`) have all landed as real, implemented
`ComponentKind`s — [examples/nginx-to-influxdb.yaml](examples/nginx-to-influxdb.yaml) exercises all
of them together against a real nginx (`examples/nginx/`), and
[docs/deploying.md](docs/deploying.md) is the operator-facing doc for running any of this outside
the dev stack. `examples/` is contributor-facing fixtures the dev stack (`script/server`) runs
against, kept real because other things in the repo depend on them (`compose.yaml`'s `nginx`
service, `crates/logit-bench/src/fixtures.rs`'s `NGINX_SYSLOG_LINE`) — `demo/` is the answer to
"let me see this work" for anyone else, a self-contained `docker compose up` against the release
image, and the forcing function for `syslog_out`/`otlp_out` next
([docs/plans/0003-demo-stack.md](docs/plans/0003-demo-stack.md)). Config is a flat graph of named components (ADR 0009,
[pipeline-graph.md](docs/design/pipeline-graph.md)) resolved and validated by
`logit-pipeline::graph`, then run by `logit-pipeline::run`'s node runtime -- `logit-cli::pipeline`
is now just the kind → implementation registry. Config files are read and parsed exclusively
through `logit_cli::config::load` (`crates/logit-cli/src/config.rs`), which also resolves `!env
VAR_NAME` -- any field on any component can pull its value from the environment this way (ADR
0011), which is why `influxdb_out`'s `token` is a plain string, not an env-specific field. `logit
graph <config>` prints the resolved graph as graphviz DOT (`crates/logit-cli/src/dot.rs`).
`logit run` rejects a config referencing any other unimplemented kind with a clear error; see
[ADR 0008](docs/adr/0008-aggregation-window-semantics.md) for `aggregate`'s windowing semantics,
[ADR 0010](docs/adr/0010-json-parsing-into-attributes.md) for `json`'s parsing semantics,
[ADR 0014](docs/adr/0014-kv-metrics-semantics.md) for `kv_metrics`'/`keep`'s semantics,
[ADR 0013](docs/adr/0013-service-lifecycle-and-output-retry.md) for signal-driven shutdown and
`influxdb_out`'s bounded output retry, `crates/logit-inputs/src/statsd.rs` and
`crates/logit-outputs/src/influxdb.rs` for the listener/sink side, `crates/logit-pipeline/src/runtime.rs`
for orchestration and the per-node flush-tick timer. `internal` (`crates/logit-inputs/src/internal.rs`)
is `logit` observing itself — a listener like any other, draining `logit_core::telemetry`'s
per-component buffers into ordinary events on its own `interval`; see
[ADR 0018](docs/adr/0018-internal-telemetry-as-pipeline-events.md) and
[internal-telemetry.md](docs/design/internal-telemetry.md) for the framework, and
[examples/internal-telemetry.yaml](examples/internal-telemetry.yaml) for a runnable config. Every
`Delivered` (one `Fanout` edge's channel payload) carries a real `TraceContext`, propagated as a
child of its parent for the two node kinds with an unambiguous one to propagate
(`Transform::process`/`ScriptWorker::process`'s non-flush path, and `run_output`); see
[ADR 0020](docs/adr/0020-trace-context-propagation-on-delivered.md) and
[pipeline-graph.md](docs/design/pipeline-graph.md)'s "Trace context propagation" section. That
context is now turned into real spans: every node kind records a `SpanRecord`-carrying `Event` for
its own visit to a unit of work (a listener's send, a transform's process-or-flush, a sink's
delivery), sampled deterministically on `trace_id` (`span_sample_rate` on the `internal` component,
default 0.1); see [ADR 0022](docs/adr/0022-internal-span-emission-and-deterministic-sampling.md)
and [internal-telemetry.md](docs/design/internal-telemetry.md)'s "Spans" section. `docs/known-gaps.md`'s
internal-spans entry tracks what's still open (the listener span's window, Lua `flush()`'s
link-less root).

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
[ADR 0005](docs/adr/0005-containerized-development.md) and
[ADR 0006](docs/adr/0006-scripts-to-rule-them-all.md).

## Workflow

Work happens on a branch, landed via pull request — never commit straight to `main`. Run
`script/cibuild` locally before opening one; it's the same sequence `.github/workflows/ci.yml`
runs, so a clean local run means a clean CI run.

**To bring a branch with an open PR up to date with `main`, `git merge origin/main` — don't
rebase.** A rebase rewrites the branch's commits, which means a force-push to update the PR; that's
disruptive for an open PR (review-comment associations, anyone else with the branch checked out)
for no real benefit here. A merge commit costs nothing extra and pushes normally.

## Conventions to hold to

- **A new design decision worth remembering gets an ADR** (`docs/adr/`, numbered, following the
  existing files' Status/Context/Decision/Alternatives/Consequences shape) — not just a comment or
  a PR description. Check the existing ADRs before re-deciding something they already settled.
- **`rustfmt.toml`/`clippy.toml` are enforced**, not advisory — `script/cibuild` fails the build on
  either. Run `script/format` before committing rather than hand-formatting.
- **Every config type derives `Serialize + Deserialize + JsonSchema` together**
  ([ADR 0003](docs/adr/0003-config-yaml-jsonschema.md)) — the published schema is generated from
  the Rust types specifically so it can't drift. If `schemars` needs a hint `serde` doesn't give it
  (as with the hand-rolled `Duration` codec in `logit-config`), add `#[schemars(with = "...")]`
  alongside `#[serde(with = "...")]` rather than dropping the derive.
- **Stub code says so.** Unimplemented pieces are `todo!()` with a comment pointing at the design
  doc section and, where relevant, what to build next — see `logit-script`, `logit-proto`, and the
  `statsd`/`influxdb` stubs. Follow that pattern for new stubs rather than silently returning a
  default.
- **A config file is always read through `logit_cli::config::load`**, never a bare
  `std::fs::read_to_string` + `serde_norway::from_str` — that's what resolves `!env` and rejects an
  unknown YAML tag (ADR 0011); a call site that bypasses it silently loses both.

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
  logit-outputs     per-protocol sinks implementing logit-pipeline::Output; InfluxDB (v0.1 target), stdio
  logit-transforms  native transforms implementing logit-pipeline::Transform; aggregate (v0.1 target), json, kv_metrics, keep, remove
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
