---
created: 2026-09-03
updated: 2026-09-03
---

# Closing plan: signal-aware filter components, and `otlp_out`'s remaining config gaps

## Context

[docs/plans/otlp-logs-and-resource-identity.md](otlp-logs-and-resource-identity.md)'s workstream E
lists six `otlp_out` gaps found while checking whether the demo could ship logs straight to Loki over
OTLP. One of them — "no signal filter" — is deliberately **not** solved inside `otlp_out`, or inside
any `_out` component. Filtering by signal type is pipeline composition, not sink configuration: a
`signals:` field on `otlp_out` would have to be re-invented on every future sink, and it would be a
second way to express something the component graph already has a vocabulary for (ADR
`component-graph-configuration`'s "filter components as the only branching mechanism"). It becomes
insertable transform components instead, placed ahead of whatever sink needs them.

Two components, not one, because `logit`'s event model needs both. Under ADR `multi-payload-events`
a single `Event` carries `log`, `metrics`, and `span` at once, so "filter by signal" splits into two
genuinely different operations:

- **Drop events** that don't carry the wanted signal — `internal` emits single-payload events
  (metric-only and span-only, `crates/logit-core/src/telemetry.rs:615,634`), so a traces-only sink
  fed from `internal` needs exactly this.
- **Strip payloads** off events that carry several — an nginx access-log event that `json` +
  `kv_metrics` turned into a log *and* derived metrics needs those metrics removed before it
  reaches a logs-only sink, without losing the log.

This retires a live workaround. `demo/logit.yaml`'s `trace_windowed` is an `aggregate` node that
exists solely to absorb metrics before `trace_out` (Tempo, traces-only) sees them; without it every
batch mixed signals, `send` never returned `Ok`, and `write_loop`'s sustained-failure guard killed
the whole process a minute after startup. `docs/known-gaps.md`'s "`otlp_out` aborts an entire
batch's `send`..." entry names "a config-layer way to filter an event stream by which payload it
carries" as the real fix. This plan's workstream 1 is that fix.

The remaining in-scope `otlp_out` items — custom headers, gzip compression, configurable per-signal
paths, and `observed_time_unix_nano` — are ordinary config surface: none blocks the demo, all block
a real deployment. **gRPC TLS is out of scope** and stays a filed known gap; it needs a
`tokio-rustls` layer in the hand-rolled gRPC client and amends ADR `hand-rolled-grpc-over-hyper`,
which is its own workstream. **Landed** in
[docs/plans/otlp-tls.md](otlp-tls.md) / [ADR `otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md)
— TLS on both `otlp_out` transports and `otlp_in`, plus mutual TLS; the gRPC client's connection
management moved to a pooled `hyper-util`/`hyper-rustls` client rather than layering `tokio-rustls`
onto the per-request hand-rolled connect this plan sketched, closing the "opens a fresh connection
per request" gap as a side effect.

## Decisions already settled

- Component names: **`has_signal`** (drops events), **`keep_signals`** / **`drop_signals`** (strip
  payload slots) — an allowlist/denylist pair mirroring the existing `keep`/`remove` pair for
  attributes. `filter { where }` is already a reserved, not-yet-implemented `ComponentKind`
  variant for a future predicate transform — none of these names collide with it.
- `has_signal` takes `mode: any_of | only`, defaulting to `any_of`.
- All three drop an event that ends up carrying nothing.
- Config vocabulary is OTLP's — `logs`, `metrics`, `traces` — matching `logit_proto::Signal`
  (`crates/logit-proto/src/lib.rs:76-80`) and the `signal` telemetry tag, not the `Event` field
  names. `traces` maps to `event.span`.

## Workstream ordering

1. **Signal components.** Self-contained; retires the demo workaround.
2. **`otlp_out` custom headers.** Smallest `otlp_out` item, no new dependency, and it establishes
   the reserved-header list workstream 4 adds `grpc-encoding` to.
3. **Configurable per-signal HTTP paths + `observed_time_unix_nano`.** Two small "stop hardcoding"
   fixes, independent of the others.
4. **Gzip compression.** Last: the only one with a new workspace dependency, a `deny.toml` risk, and
   a coupled `otlp_in` change, and the only one that can break
   `crates/logit-cli/tests/otlp_round_trip.rs`.

Each workstream adds a numbered validation rule to `graph::resolve` and to
`docs/design/pipeline-graph.md`'s validation list (rules run to 18 today): 19 for the signal
components, 20 for reserved headers, 21 for `paths` under `protocol: grpc`. Renumber if they land
out of order. Landing as separate PRs, in the order above, per workstream — each is independently
reviewable and shippable, and the dependency on 2 for 4's reserved-header list is the only real
ordering constraint.

---

## Workstream 1 — the signal components

### New code: `crates/logit-transforms/src/signals.rs`

One module holding all three, the way `keep.rs` holds both `Keep` and `Remove`. `logit-transforms`
depends on neither `logit-config` nor `logit-proto` (`crates/logit-transforms/Cargo.toml`), so the
signal set is its own type here, converted from config in `build_spec` exactly as `to_metric_specs`
converts `MetricSpec` today (`crates/logit-cli/src/pipeline.rs:451-459`).

```rust
/// Which payload slots a signal-aware transform acts on. Named for OTLP's signals, not `Event`'s
/// field names -- `traces` is `event.span`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SignalSet { pub logs: bool, pub metrics: bool, pub traces: bool }

pub struct HasSignal   { signals: SignalSet, mode: MatchMode, telemetry: Telemetry }
pub struct KeepSignals { signals: SignalSet, telemetry: Telemetry }
pub struct DropSignals { signals: SignalSet, telemetry: Telemetry }
```

Each implements `logit_pipeline::Transform` (`crates/logit-pipeline/src/transform.rs:23-67`) with
only `process` overridden, plus a `.with_telemetry()` builder and no `Diagnostics` — matching
`Keep`/`Remove`, which have nothing that can fail and so no `warn_throttled` call site
(`crates/logit-transforms/src/keep.rs:35-41`).

**Semantics.**

| Component | `signals: [traces]` on `{span, metrics}` | on `{metrics}` | on `{}` |
|---|---|---|---|
| `has_signal` (`any_of`) | forwarded intact, metrics kept | dropped | dropped |
| `has_signal` (`only`) | dropped | dropped | dropped |
| `keep_signals` | `{span}` | dropped | dropped |
| `drop_signals` | `{metrics}` | `{metrics}` | dropped |

- `has_signal` never mutates. `any_of` forwards an event carrying at least one listed signal;
  `only` additionally requires it carry nothing outside the set. Both modes require at least one
  listed signal present, so an empty event is always dropped and `only` can never be satisfied
  vacuously.
- `keep_signals`/`drop_signals` clear the disallowed slots (`event.log = None`,
  `event.metrics.clear()`, `event.span = None`), then return `None` if nothing survives.

`Transform::process` returning `None` already means "don't forward", and `process_batch` sends
nothing downstream when every event in a batch drops
(`crates/logit-pipeline/src/runtime.rs:1001-1005`) — no runtime change needed. These are the first
native transforms to return `None` for a reason other than accumulation.

**Telemetry.** `process_batch` hardcodes `reason = "absorbed"` on its
`logit.component.events.dropped` counter (`runtime.rs:1003-1007`) — accurate for `aggregate`,
misleading for a filter. Rather than widen the trait, each component records its own counters, the
way `keep` records `logit.transform.attributes.kept`/`.dropped` (`keep.rs:88-99`):
`logit.transform.events.filtered` (events dropped) and `logit.transform.payloads.stripped{signal}`
(slots cleared). Note the `reason` tag's imprecision in the module doc; fixing it is not this
workstream. Document both counters in `docs/design/internal-telemetry.md`, next to the `kv_metrics`
and `keep`/`remove` entries.

### Config: `crates/logit-config/src/lib.rs`

Three variants on `ComponentKind` (the internally-tagged enum at `:110-112`), placed next to
`Keep`/`Remove` (`:229-241`), plus a `Signal` enum and a `MatchMode` enum with a `Default` impl
following `OtlpProtocol`'s shape (`:82-95`):

```rust
HasSignal   { signals: Vec<Signal>, #[serde(default)] mode: MatchMode },
KeepSignals { signals: Vec<Signal> },
DropSignals { signals: Vec<Signal> },
```

Doc comments on every variant and field are the user-facing JSON Schema descriptions — they must
carry the table above, and `has_signal`'s must say plainly that it never mutates an event while
`keep_signals` does, since that is the whole reason there are three kinds.

### Wiring — the four match arms a new kind always needs

1. `graph::role` (`crates/logit-pipeline/src/graph.rs:92-106`) — add to the `Role::Transform` arm.
2. `graph::kind_name` (`:122-153`) — `"has_signal"`, `"keep_signals"`, `"drop_signals"`, exactly
   the config `type` tags.
3. `graph::is_implemented` (`:158-177`) — otherwise rule 8 rejects them as unimplemented.
4. `logit-cli::pipeline::build_spec` (`crates/logit-cli/src/pipeline.rs:253-262`, beside the
   `Keep`/`Remove` arms) — plus a `to_signal_set` converter alongside `to_metric_specs`.

### Validation — rule 19

Its own loop in `graph::resolve`, following the `kv_metrics` precedent (`graph.rs:316-340`):

> An empty `signals:` list on `has_signal`, `keep_signals`, or `drop_signals` is rejected, as is a
> `drop_signals` naming all three signals — each can only ever drop every event, the silent black
> hole rule 7 exists to catch.

(`keep`'s empty `fields` list stays legal — "drop every attribute" is a real operation; "drop every
event" is not.)

### ADR: `docs/adr/signal-filtering-components.md`

Records: why signal filtering is a component and not a field on `otlp_out` or any other sink; why
the multi-payload model forces two operations rather than one; the drop-emptied-event rule; the
`mode: any_of | only` split; and the OTLP config vocabulary (`traces`, not `span`). Add the row to
`docs/adr/README.md`.

### Docs and demo

- `docs/design/pipeline-graph.md` — transform-kind lists, the `ComponentKind` sketch, and rule 19
  in the validation list.
- `demo/logit.yaml` — replace `trace_windowed` (aggregate) with `type: has_signal` /
  `signals: [traces]`, and rewrite the topology comment and the long explanation above it: the
  workaround becomes the intended mechanism, and `trace_out`'s occasional metrics-only flush
  failure disappears entirely rather than being outrun by successful sends. Metrics still reach
  InfluxDB via `self_windowed`, so nothing is lost.
- `docs/known-gaps.md` — the "aborts an entire batch's `send`" gap itself is unchanged (the abort
  behavior stays), but its demo-workaround paragraphs now describe `has_signal` as the fix rather
  than `aggregate` as a hack.
- `docs/plans/otlp-logs-and-resource-identity.md` — workstream E's "No signal filter" bullet marked
  landed, pointing at the new ADR.
- `AGENTS.md`'s "Current state" paragraph and its `logit-transforms` line in "Where things live" —
  both enumerate the implemented transform kinds by name.

### Tests

- Unit tests in `signals.rs`, following `keep.rs:102-262`: the full semantics matrix above, an
  emptied-event-is-dropped case per component, `only` vs `any_of` on a mixed event, and telemetry
  assertions via `Registry::new()` + `registry.telemetry_for(..)` + `registry.drain(0)`
  (`keep.rs:216-237`).
- A chain test in `crates/logit-transforms/src/lib.rs`'s `chained_pipeline_test`: a
  `json -> kv_metrics -> keep_signals[logs]` chain proves the derived metrics are stripped while
  the log body survives — the workstream B (Loki-direct) shape from the parent plan.
- Config deserialization tests beside `keep_component_deserializes`
  (`crates/logit-config/src/lib.rs:1113-1123`), including `mode` defaulting to `any_of`.
- `graph.rs` role/kind_name tests (`:944-990`) and a rule-19 rejection test.
- `build_spec` tests mirroring `build_spec_builds_a_keep_transform`
  (`crates/logit-cli/src/pipeline.rs:909-935`).

---

## Workstream 2 — `otlp_out` custom headers

**Config.** `headers: HashMap<String, String>` with `#[serde(default)]` on
`ComponentKind::OtlpOut` (`crates/logit-config/src/lib.rs:285-289`); `HashMap` is already imported
and an empty map is today's behavior. The doc comment (→ schema description) states: sent on every
export request on **both** transports; values are plain strings so `!env` works (ADR `env-yaml-tag`
— this is how `Authorization: Bearer …` stays out of the config file); the canonical use is
`X-Scope-OrgID` for multi-tenant Loki/Mimir/Grafana Cloud; and protocol-owned headers are rejected
at load rather than silently overridden.

**Rule 20** in `graph::resolve` rejects, case-insensitively: `content-type`, `content-length`,
`content-encoding`, `host`, `te`, `transfer-encoding`, `connection`, `grpc-encoding`,
`grpc-accept-encoding`, `grpc-timeout`, `grpc-status`, `grpc-message`, an empty name, and anything
starting with `:` (HTTP/2 pseudo-headers, which hyper would otherwise reject with an opaque error
at send time). It belongs in `graph::resolve`, not `OtlpOutput::new`, because that is where a
config-shaped mistake gets a component id in its message.

**Applying them — avoid the `with_timeout` hazard.** Do *not* bake headers into `build_client` via
`reqwest`'s `default_headers`: `with_timeout` (`crates/logit-outputs/src/otlp.rs:93`) rebuilds the
client unconditionally, so `.with_headers(h).with_timeout(t)` would silently discard them while the
reverse order kept them. Instead store `headers: http::HeaderMap` as a field on `OtlpOutput`
(`:63-71`), set by a fallible `with_headers(&HashMap<String,String>) -> anyhow::Result<Self>` that
does the lexical `HeaderName`/`HeaderValue` validation `graph` can't, and apply it per request:

- HTTP (`send_http`, `:132-142`): `.headers(self.headers.clone())` *before* the explicit
  `Content-Type` header, so the protocol-owned one always wins.
- gRPC: `grpc_roundtrip` (`~:400-470`) takes a `&HeaderMap` parameter and extends the builder's
  `headers_mut()` *after* its fixed `content-type`/`te`/`grpc-accept-encoding` headers.

**Tests.** `canned_http_server` (`otlp.rs:677`) currently discards the request — have it capture
raw request text into an `Arc<Mutex<Vec<String>>>` and assert the header arrives; same for
`canned_grpc_server` (`:841`) via `req.headers()`. Plus
`with_timeout_after_with_headers_keeps_the_headers` (the regression test for the hazard, asserting
both builder orders send identically), `an_invalid_header_value_fails_construction`, and rule-20
accept/reject tests in `graph.rs`. No ADR — config surface only; the reserved list's reasoning
lives in the field doc comment and rule 20's inline comment.

---

## Workstream 3 — configurable per-signal HTTP paths + `observed_time_unix_nano`

**Paths, config.** `paths: OtlpPaths` (`#[serde(default)]`), a struct of three `Option<String>`
(`logs`/`metrics`/`traces`) — not a `HashMap` (a typo'd key would silently do nothing; a struct
gets schema validation for free) and not a path prefix, which is *already* expressible: `send_http`
joins `endpoint` to the signal path, so `endpoint: http://host/otlp` already yields `/otlp/v1/logs`.
The doc comment says exactly that, plus that a leading `/` is expected and what the defaults are.

**Where consulted.** A `logit-outputs`-local `SignalPaths` field on `OtlpOutput` (mirrored from
config the same way `OtlpTransport` is), a private `path_for(&self, signal) -> &str` returning the
override or `signal.path()`, and `send_http:132` using it. `Signal::path()`
(`crates/logit-proto/src/lib.rs:82-91`) stays the default source and is otherwise untouched, so
`otlp_in`'s router is unaffected — the input's mount points are not the output's.

**gRPC: rejected, not ignored.** gRPC method names are fixed by the `.proto` service definitions and
`Signal::grpc_method()` is shared with `otlp_in`'s router. **Rule 21** rejects a non-empty `paths`
on an `otlp_out` with `protocol: grpc`, in the same spirit as rule 14 (`buffer:` on a non-sink) and
rule 17 (`receive:` on a non-datagram listener).

**`observed_time_unix_nano`: stamp `now_nanos()` at encode.**
`crates/logit-proto/src/otlp/logs.rs:114` becomes `crate::now_nanos().max(0) as u64` (the helper at
`lib.rs:61-63`, made `pub(crate)` if it isn't already). Mirroring `time_unix_nano` instead would
make the decode fallback at `:134-141` a tautology and would write `observed = 0` in exactly the
case that fallback exists for; adding an `Event` field is a `logit_core` data-model change rippling
through every input, the native wire format, the Lua proxy, and
`crates/logit-core/tests/type_sizes.rs` — for a value no input actually carries. Stamping at encode
matches OTLP's own definition (observed time is when the collector observed the record; at encode
time `logit` is that collector) and makes a `syslog_in` event with no parseable timestamp export as
`time=0, observed=<now>`, which a downstream — `logit`'s own `otlp_in` included — recovers a sane
timestamp from instead of 1970.

**Tests.** No existing test asserts encode-side observed time, so nothing breaks; add
`encode_stamps_a_nonzero_observed_time_unix_nano` and an unknown-timestamp round-trip case, note in
`body_format_survives_a_full_round_trip` (`logs.rs:186`) that its decoded timestamp is now a real
clock value, and update the module doc at `:14-19`. `encode_log_record` becomes non-deterministic
on that one field — no test may assert whole-record equality. Path tests use the existing
`canned_http_server` with an override asserting the URL actually requested.

---

## Workstream 4 — gzip compression (`otlp_out` **and** `otlp_in`, one PR)

**Config.** `OtlpCompression { #[default] None, Gzip }` next to `OtlpProtocol`, field
`#[serde(default)] compression`. **Default `none`, deliberately**: flipping it would change existing
pipelines' wire behavior and break `logit → logit` across versions, since `otlp_in` only learns gzip
in this same workstream. Say so in the doc comment.

**`otlp_out`.** HTTP: gzip the payload and add `Content-Encoding: gzip`; leave `Content-Type` and
response decoding alone. gRPC: `grpc_frame` (`otlp.rs:479-489`) takes a `compressed: bool` and sets
the flag byte to `1` over an already-compressed message (the 5-byte header is never compressed),
and the request gains `grpc-encoding: gzip`. **`grpc-accept-encoding` stays `identity`** — by never
accepting a compressed response, `grpc_unframe` (`:496+`) keeps rejecting a set flag byte and its
test at `:998` stands; OTLP responses are single-digit-byte `partial_success` messages, so inflating
them would add an inbound untrusted-decompression path on the output side for nothing. Update
`grpc_unframe`'s doc comment: "never *accepts* compressed responses", not "never asked for".

**`otlp_in`, the coupled half.** Accept `gzip` alongside `identity` on both transports
(`crates/logit-inputs/src/otlp.rs:207-218` HTTP, `:262-269` gRPC, and its own frame decode for the
per-message flag), advertise `grpc-accept-encoding: identity, gzip` on responses, and rewrite the
"Compression is not supported" module-doc paragraph (`:26-32`). The existing rejection tests
(`:552`, `:570`) become "an unknown encoding is still rejected", plus new positive gzip tests.

**Bomb guard.** `MAX_REQUEST_BYTES` (4 MiB, `:70`) already bounds the *compressed* body via
`Limited` (`:220`, `:272`) but not the inflated output. One shared helper inflates through
`flate2::read::GzDecoder` wrapped in `Read::take(MAX_REQUEST_BYTES + 1)` and rejects an
over-limit result with the existing over-limit responses (`413` / `grpc-status: 8`
`RESOURCE_EXHAUSTED`), not a decode error. Test it with a gzip stream of `MAX_REQUEST_BYTES + 1`
zero bytes (a few KB compressed).

**Dependency.** `flate2 = "1"` in `[workspace.dependencies]`, default features (the pure-Rust
`miniz_oxide` backend — no C toolchain, per ADR `containerized-development`), used by both
`logit-outputs` and `logit-inputs`. Not `async-compression`: both bodies are already fully buffered
into `Bytes` before any codec runs, so there is no stream to adapt, and a few-MiB gzip is
sub-millisecond (no `spawn_blocking`) — say so in a comment. `deny.toml`'s allow-list already covers
`flate2`/`crc32fast` (MIT OR Apache-2.0) and `miniz_oxide` (MIT OR Zlib OR Apache-2.0, satisfied by
MIT); if `cargo deny` disagrees, add `"Zlib"` with a comment in the style of the existing rustls
note rather than silencing the check.

**Round-trip.** Add a gzip case per transport to `crates/logit-cli/tests/otlp_round_trip.rs` — the
test that proves both halves landed together, and the reason they are one PR.

**ADR: `docs/adr/otlp-compression-and-decompression-bounds.md`.** Pins four decisions with yes/no
answers: `grpc-accept-encoding: identity` on the client so responses are never compressed;
decompressed output bounded by the same `MAX_REQUEST_BYTES` as compressed input; `flate2` over
`async-compression`; default `none`.

---

## Cross-cutting mechanics

- **Struct-variant destructures break on new `OtlpOut` fields** — `crates/logit-cli/src/pipeline.rs:273`
  plus its test constructions at `:697`/`:723`, and `crates/logit-config/src/lib.rs:1166`.
  `graph.rs:108,148,173` already use `{ .. }`.
- **Schema drift is CI-enforced** — every workstream touching `logit-config` runs `./script/schema`
  and commits the result (`script/cibuild`).
- **Every config type derives `Serialize + Deserialize + JsonSchema` together** (ADR
  `config-yaml-jsonschema`), with `#[schemars(with = "...")]` alongside any custom `#[serde(with)]`.
- **Branch + PR per workstream, `script/cibuild` clean locally first** (`AGENTS.md`'s "Workflow");
  never commit to `main`.

## Verification

1. `./script/test` — new unit, config, graph, `build_spec`, codec, and round-trip tests green.
2. `./script/cibuild` — the full CI sequence: format, lint, test, `script/validate` over every
   shipped config, the schema-drift diff, and `cargo deny` (the check that matters for
   workstream 4).
3. `cargo run -q -p logit-cli -- graph demo/logit.yaml` — the demo graph still resolves with
   `has_signal` in place of `trace_windowed`.
4. Demo stack up (`script/demo`), then against the live stack: Tempo's `/api/search` still returns
   traces; `trace_out`'s `logit.output.requests{signal="metrics"}` counter stays at zero, since no
   metrics request is ever issued now; and the process survives well past the ~60s
   sustained-failure window the pre-workaround config died inside.
5. For workstream 4, an end-to-end check the round-trip test can't give: point a gzip-configured
   `otlp_out` at the demo's Tempo and confirm traces still land — a real third-party receiver
   negotiating the encoding, not `logit` talking to itself.
