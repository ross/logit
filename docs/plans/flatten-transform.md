---
created: 2026-09-21
updated: 2026-09-21
---

# Enabling plan: `flatten` — dotted-key expansion of nested attributes

## Context

[ADR `flatten-transform`](../adr/flatten-transform.md) decides the shape: a new `flatten`
transform rewriting a nested `Value::Map`/`Value::Array` attribute into flat, dot-joined keys
(`foo.key`, `tags.0`), opt-in and operator-placed rather than a decoder or sink behavior. This plan
is the build-out: what lands in which order, in which files, and how each piece is verified. Read
the ADR first — this document doesn't repeat its reasoning, only its consequences.

Stream key **`flat`**: branches `flat/w0`…`flat/w2`, a strictly linear stack, each PR based on and
targeting its parent's branch, brought up to date with `git merge origin/main` (never rebase).

## Decisions already settled

| Question | Decision |
|---|---|
| Scope | Blanket by default (`attributes: all`); an optional named list (or `none`) narrows it. Same three-shape field for `resource:`, default `none` |
| Fields are literal names | A configured entry is a literal top-level attribute name, never a path — `kv-metrics-semantics`' rule, unchanged |
| Separator | Fixed at `.`, not configurable — `prometheus_out`/`statsd_out` already sanitize `.` on their own wire |
| Containers | Event attributes always; resource behind `resource:`; scope impossible (no `map_scope` hook); span-event/link/exemplar attrs deliberately untouched |
| Collisions | Last-write-wins, silent — `AttrMap::insert_sym`'s existing semantics, no new counter |
| Interner growth | No cap beyond a fixed internal recursion-depth bound (stack safety, not policy) — documented as a known gap, not guarded |
| Arrays | Flatten by index (`tags.0`) by default; `arrays: skip` treats an array as a leaf everywhere |
| Leaf rule | A leaf is any non-container value, or an empty `Map`/`Array`; a source attribute is removed only once its leaves are written. `flatten` never deletes an attribute |
| Statefulness | Effectively stateless — only `process`/`map_resource` overridden, plus reused per-instance scratch buffers (not per-event state) |
| Diagnostics | None — flattening cannot fail |
| Landing | PR stack only. Nothing merged by this workstream; Ross directs merging |

## Design

### `crates/logit-core/src/attrs.rs` (W1)

```rust
/// Consumes the map, yielding its (Symbol, Value) pairs in sorted order. The owned-map mirror of
/// `iter()` -- for a caller that already holds this AttrMap by value and is about to move every
/// value out of it (`flatten`'s expansion is the first caller: a nested `Value::Map` reached
/// during its walk was `remove_sym`'d out of its parent, so every child is about to move again
/// rather than being cloned).
pub fn into_pairs(self) -> impl Iterator<Item = (Symbol, Value)>
```

### `crates/logit-config/src/lib.rs` (W1)

```rust
#[serde(untagged)]
pub enum FlattenFields {
    All(FlattenAll),        // "all"
    None(FlattenNone),      // "none"
    Named(Vec<String>),
}
// or, simpler and matching SetValue's precedent of a plain untagged enum over literal strings
// vs. a list -- resolved during implementation to whichever serde shape round-trips cleanly.

#[serde(rename_all = "snake_case")]
pub enum FlattenArrays { Index, Skip }   // default Index

Flatten {
    #[serde(default = "FlattenFields::all")]
    attributes: FlattenFields,
    #[serde(default = "FlattenFields::none")]
    resource: FlattenFields,
    #[serde(default)]
    arrays: FlattenArrays,
},
```

Shaped on `KeepValues`'s `resource:`/`attributes:` split; every variant and field carries rustdoc
pointing at the ADR — those comments flow straight into the published schema's `description`s.

### `crates/logit-transforms/src/flatten.rs` (new; W1)

```rust
enum Arrays { Index, Skip }              // mirrors logit_config::FlattenArrays
enum Fields { All, None, Named(Vec<Symbol>) }  // mirrors logit_config::FlattenFields, interned once

struct Scratch {
    pending: Vec<(Symbol, Value)>,  // selected source entries, taken out before any expansion
    path: String,           // one buffer for the whole walk, truncate-on-backtrack
    keys: KeyCache,         // path -> Symbol memo, JsonParser's pattern
}

pub struct Flatten {
    attribute_fields: Fields,
    resource_fields: Fields,
    arrays: Arrays,
    scratch: Scratch,
    cache: Option<(Arc<Resource>, Arc<Resource>)>,   // keep_values::map_resource's cache
    telemetry: Telemetry,
}
```

`process`/`map_resource` share a free `flatten_map(&mut AttrMap, &Fields, Arrays, &mut Scratch,
&Telemetry)`, the `keep_values::clamp_field` shape: phase 1 scans `attrs.iter()` and collects
selected, expandable top-level symbols into `scratch.pending` (cannot mutate while iterating);
phase 2 `remove_sym`s every one of them, so no selected value is still in the map once expansion
starts; phase 3 recursively `expand`s each, writing leaves back via
`scratch.keys.get_or_intern(&scratch.path)` + `attrs.insert_sym`. Recursion is bounded by a fixed
internal `MAX_DEPTH` constant (not config), mirroring
`logit_proto::native::value::MAX_VALUE_DEPTH`; a value that hits the wall is written back whole,
unexpanded, at the path reached, and counted. `map_resource` is `keep_values`'s cache verbatim:
`Arc::ptr_eq` short-circuit, rebuild carrying `dropped_attributes_count`/`schema_url` forward,
`None` when `resource: none`.

Telemetry: `logit.transform.values.flattened` (one per leaf written),
`logit.transform.values.unflattened{reason="max_depth"}` (a source value the depth wall refused).
Both untagged — see the ADR's rationale (blanket mode makes the source key data, not config).

### `crates/logit-pipeline/src/graph.rs` (W1)

`role` → `Transform`; `kind_name` → `"flatten"`; `is_implemented` → `true`. New **rule 59** (58 is
the current highest): `attributes: none` + `resource: none` together rejected as a guaranteed
no-op; an empty named list rejected (message: write `none` to mean nothing, `all` to mean every
nested attribute); an empty field name rejected; a duplicate field name within one list rejected.
Explicitly *not* rejected: `attributes: all` (the default), and a field name containing `.` (it
names a literal attribute).

### `crates/logit-cli/src/pipeline.rs` (W1)

A `build_spec` arm plus `to_flatten_fields`/`to_flatten_arrays` beside `to_allow_lists`, converting
`logit_config::FlattenFields -> logit_transforms::Fields` and
`logit_config::FlattenArrays -> logit_transforms::Arrays`.

### Docs (W2)

`docs/design/pipeline-graph.md` (`ComponentKind` sketch, arity table, rule 59 text),
`docs/design/internal-telemetry.md` (the two counters, and why untagged),
`docs/design/memory.md` (the allocation rows), `crates/logit-transforms/src/lib.rs` (crate doc +
`mod`/`pub use`, alphabetical between `csv` and `json`), `AGENTS.md` (current-state paragraph,
crate-layout line), `README.md`, `docs/known-gaps.md` (interner-growth entry, extending the
existing `syslog.sd`/`json`/`otlp_in` bullet), `examples/nested-json-to-influxdb.yaml` (new
example), `schema/logit.schema.json` (regenerated).

### The example (W2)

New file `examples/nested-json-to-influxdb.yaml`: `tail_in -> json -> flatten -> keep ->
kv_metrics -> aggregate -> influxdb_out` over a small nested-JSON fixture log (an
`http: {method, status}` shape). New rather than folded into `examples/nginx-to-influxdb.yaml`
because the point is demonstrating a *nested* source, which no shipped example currently has.
Covered automatically by `every_shipped_config_loads_and_validates`
(`crates/logit-cli/src/config.rs`) and `script/validate`.

## Workstreams

| # | PR | Files | Depends |
|---|---|---|---|
| W0 | **ADR and plan.** | `docs/adr/flatten-transform.md` (new, + row atop `docs/adr/README.md`); `docs/plans/flatten-transform.md` (new, + row atop `docs/plans/README.md`) | — |
| W1 | **`flatten` — the component.** | `crates/logit-core/src/attrs.rs`; `crates/logit-config/src/lib.rs`; `crates/logit-pipeline/src/graph.rs`; `crates/logit-transforms/src/flatten.rs` (new); `crates/logit-transforms/src/lib.rs`; `crates/logit-cli/src/pipeline.rs`; `schema/logit.schema.json` | W0 |
| W2 | **Docs, example, and the allocation pins.** | `examples/nested-json-to-influxdb.yaml` (new); `docs/design/pipeline-graph.md`; `docs/design/internal-telemetry.md`; `docs/design/memory.md`; `crates/logit-bench/src/fixtures.rs`; `crates/logit-bench/tests/allocations.rs`; `crates/logit-transforms/src/lib.rs` (`chained_pipeline_test`); `AGENTS.md`; `README.md`; `docs/known-gaps.md` | W1 |

Landing order: **W0 → W1 → W2**, strictly linear. Config, validation, transform, and registry are
one PR because `build_spec`'s match is exhaustive — a variant added without its arm doesn't
compile, and `is_implemented` returning `true` without one is the "schema advertises what the
binary won't run" failure `routing-by-condition-is-lua` closed.

### Per-workstream detail

**W0** — Done when: both README index tables have their row, dated 2026-09-21, and every
cross-reference resolves.

**W1** — Tests: `into_pairs` unit tests on `AttrMap`; config deserialization (JSON, per the
crate's own convention) for `attributes`/`resource` in all three shapes (`all`/`none`/named list),
`FlattenArrays`'s snake_case tag; graph rule 59, one test per clause plus the negative-space "no
fields configured (both default) is accepted"; `build_spec_builds_a_working_flatten_transform` in
the stronger run-it form. Unit tests in `flatten.rs`: a nested map becomes dotted keys; the source
attribute is removed only once its leaves are written; an array flattens by index; nesting and
arrays compose (`items.0.name`); recursion composes at depth 3; flattening is idempotent; a flat
event fires no counter; an empty map/array attribute is left untouched; an empty container nested
inside is written back as a leaf; `arrays: skip` treats a nested array as a leaf and leaves a
top-level array untouched; a value past the depth wall is written back whole and counted; a named
list restricts which top-level attributes expand; a field name is literal, not a path; a configured
field the event lacks is a silent no-op; `attributes: none` expands nothing; a flattened key
overwrites an existing attribute, last-write-wins (the `json` ADR's own `http.status` example,
pinned here); resource attributes untouched by default, flattened when configured, cache hit via
`Arc::ptr_eq`, `dropped_attributes_count`/`schema_url` carried forward, `None` when `resource:
none`; log/metrics/span payloads untouched; span-event/link/exemplar attributes untouched; both
counters fire under their documented names. Done when: `script/check` green and `script/schema`
leaves no diff.

**W2** — Tests: `every_shipped_config_loads_and_validates` covers the new example for free; extend
`lib.rs`'s `chained_pipeline_test` to `json -> flatten -> keep -> aggregate` over a pino-http-shaped
fixture line, asserting the flattened keys survive to the aggregated series. Allocation tests
beside `keep_values`' two: a flat event (0 allocations), the measured pino-http shape warm (pin
whatever `crates/logit-bench` actually measures — do not assume a number before running it), and a
cold-`KeyCache` first event, with matching `docs/design/memory.md` rows. Done when: `script/cibuild`
green, `script/validate` clean, no `type_sizes.rs` change (this component adds no new type to
`Event`'s graph — verify before landing, not after).

## Verification

- `script/check` during the loop; `script/cibuild` before each PR.
- `script/schema` after the W1 config change; commit the result.
- `script/validate` over `demo/`, `examples/`, `perf/scenarios/`, `tools/shape-survey/configs/`.
- Negative config check by hand: `attributes: none` + `resource: none`; `attributes: []`;
  `attributes: [""]`; `attributes: [http, http]`.
- `logit graph examples/nested-json-to-influxdb.yaml` — `flat` renders as an ordinary transform
  node between `json` and `keep`.
- Manual smoke through `script/server` against the new example: a nested JSON log line in, dotted
  tags visible in InfluxDB out — the actual point of the component, and not provable by unit tests
  alone.
- `script/bench`/the allocation tests to fix the pinned numbers before writing them into
  `docs/design/memory.md`.

## Open risks

- **Width is cardinality.** A blanket `flatten` on an unbounded map (Kubernetes annotations, say)
  multiplies tag count per event with no automatic limit. The mitigation is operator discipline —
  a narrowed `attributes:` list, `arrays: skip`, and a `keep`/`keep_values` placed after it — not
  anything this component enforces.
- **Interner growth is unbounded by design**, beyond the fixed depth bound. `tags.0`…`tags.N-1` and
  a map keyed by rotating identifiers mint symbols that are never freed for the life of the
  process. Documented in `docs/known-gaps.md`, not guarded.
- **A flattened key can silently collide with a real attribute of the same name**, with no counter
  — a deliberate, not accidental, gap (see the ADR's Alternatives). Revisit if it turns out to
  matter in practice.
- **The allocation numbers in the design section above are estimates** until `crates/logit-bench`
  actually measures them; the plan pins whatever the real numbers turn out to be, not what's
  guessed here.
