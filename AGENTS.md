# AGENTS.md

Guidance for AI coding agents working in this repo. Humans: see [README.md](README.md).

## What this is

`logit` is a logging/metrics/tracing multiplexer written in Rust, with user transforms in
LuaJIT. Read [docs/OVERVIEW.md](docs/OVERVIEW.md) first (~1 page) for scope and positioning, then
[docs/adr/](docs/adr) for *why* the stack is what it is, then [docs/design/](docs/design) for the
internal event model, the Lua scripting API, and the native wire protocol — those three design
docs are load-bearing; don't improvise around them without reading them first. Check
[docs/known-gaps.md](docs/known-gaps.md) before "fixing" something that looks broken — it's likely
a documented, deliberate gap, not an oversight.

**Current state:** v0.1 is complete — statsd in, a 10s `aggregate` window, a Lua enrichment stage,
InfluxDB 2.x out, via `logit run <config>` (see [examples/statsd-to-influxdb.yaml](examples/statsd-to-influxdb.yaml),
`script/server`). `aggregate` (`crates/logit-transforms`) is the only built-in transform implemented
so far — `logit run` rejects a config referencing any other `builtin:` stage with a clear error; see
[ADR 0008](docs/adr/0008-aggregation-window-semantics.md) for its windowing semantics,
`crates/logit-inputs/src/statsd.rs` and `crates/logit-outputs/src/influxdb.rs` for the input/output
side, `crates/logit-cli/src/pipeline.rs` for orchestration and the flush-tick timer.

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
| `script/audit` | `cargo-deny` + `cargo-audit` |
| `script/cibuild` | The exact sequence CI runs, in order — run this before opening a PR |
| `script/console` | Interactive shell in the dev container, for anything not covered above |

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
  benchmark it and record the outcome as an ADR.

## Where things live

```
crates/
  logit-core        internal event model: Event, Value, Resource, metric kinds, interner
  logit-config      YAML config types + generated JSON Schema
  logit-script      LuaJIT embedding (mlua), the Event proxy
  logit-proto       codec traits, native wire format, output buffering
  logit-inputs      the Input trait; statsd (v0.1 target)
  logit-outputs     the Output trait; InfluxDB (v0.1 target)
  logit-transforms  built-in native transform stages; aggregate (v0.1 target), more to come
  logit-cli         the `logit` binary
```

A new protocol (input or output) implements `logit_proto::Decoder`/`Encoder` plus
`logit_inputs::Input` or `logit_outputs::Output`, and gets a variant in `logit_config`'s
`InputConfig`/`OutputConfig` — follow the `statsd`/`influxdb` stubs as the template.
