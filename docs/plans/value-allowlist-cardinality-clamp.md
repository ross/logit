---
created: 2026-09-15
updated: 2026-09-15
---

# Enabling plan: `keep_values` — clamping tag-value cardinality against an allow-list

## Context

[ADR `value-allowlist-cardinality-clamp`](../adr/value-allowlist-cardinality-clamp.md) decides the
shape: a new `keep_values` transform that allowlists attribute *values* (as `keep` already does for
keys), an optional ordered `normalize:` step (today just `lower`) applied and written back before
the allow test, and a per-field `other:` fallback that defaults to removing the attribute. This plan
is the build-out: what lands in which order, in which files, and how each piece is verified. Read
the ADR first — this document doesn't repeat its reasoning, only its consequences.

Stream key **`kvals`**: branches `kvals/w0`…`kvals/w2`, a strictly linear stack, each PR based on
and targeting its parent's branch, brought up to date with `git merge origin/main` (never rebase).

## Decisions already settled

| Question | Decision |
|---|---|
| Name | `keep_values` — sibling to `keep` (keys) as the value-side allowlist |
| Matching | Exact equality only, via the existing `logit_transforms::value_matches` — no operators, no globs |
| Normalization | An optional, ordered `normalize:` list per field, applied before the allow test and written back to the event. One step today, `lower` (ASCII-only, bytewise) |
| Config-side normalization | A non-lowercase `Str` in `allow`/`other` under `normalize: [lower]` is rejected at validate time, not silently rewritten |
| Fallback | Per field: each carries its own `normalize:`, `allow:`, and optional `other:` |
| Default fallback | `other:` absent removes the attribute |
| Absent attribute | Silent no-op for that field, never a stamp |
| Scope | Both `resource:` and `attributes:` maps, mirroring `set` |
| Dynamic cap / normalize-without-allow | Out of scope, considered and deferred (ADR's Alternatives) |
| Statefulness | Stateless — only `process`/`map_resource` overridden |
| Diagnostics | None — a clamp is documented behavior, not a failure |
| Landing | PR stack only. Nothing merged by this workstream; Ross directs merging |

## Design

### `crates/logit-config/src/lib.rs` (W1)

```rust
#[serde(rename_all = "snake_case")]
pub enum NormalizeStep { Lower }

pub struct ValueAllowList {
    #[serde(default)]
    pub normalize: Vec<NormalizeStep>,
    pub allow: Vec<SetValue>,
    #[serde(default)]
    pub other: Option<SetValue>,
}

KeepValues {
    #[serde(default)]
    resource: std::collections::BTreeMap<String, ValueAllowList>,
    #[serde(default)]
    attributes: std::collections::BTreeMap<String, ValueAllowList>,
},
```

Shaped on `Set` and `HasAttributes`; every variant and field carries rustdoc pointing at the ADR —
those comments flow straight into the published schema's `description`s.

### `crates/logit-transforms/src/keep_values.rs` (new; W1)

```rust
pub enum Normalize { Lower }   // mirrors logit_config::NormalizeStep; logit-transforms doesn't
                               // depend on logit-config, so the CLI converts (signals.rs's pattern)

struct Clamp { field: Symbol, normalize: Vec<Normalize>, allow: Vec<Value>, other: Option<Value> }

pub struct KeepValues {
    resource_fields: Vec<Clamp>,
    attribute_fields: Vec<Clamp>,
    cache: Option<(Arc<Resource>, Arc<Resource>)>,   // Set::map_resource's Arc::ptr_eq cache
    telemetry: Telemetry,
}
```

`process` resolves normalization and the match into owned locals before mutating (`scale.rs`'s
borrow-then-mutate shape): `Clamp::normalize(&Value) -> Option<Value>` returns `None` unless a step
actually changed the value (the common case, and what keeps the allowed path allocation-free);
`Lower` scans for an uppercase ASCII byte before building a new `Bytes`, and passes non-`Str`/`Bytes`
values through untouched. `map_resource` is `Set::map_resource` with the clamp loop swapped in,
behind the same one-entry cache, carrying `dropped_attributes_count`/`schema_url` forward.

Reuses rather than adds: `crate::value_matches`, `AttrMap::{get_sym,insert_sym,remove_sym}`,
`interner::{intern,resolve}`.

Telemetry: `logit.transform.values.allowed`, `.clamped`, `.normalized` (only on an actual change),
each tagged `&[("field", name)]` — `signals.rs`'s tagged-counter precedent.

### `crates/logit-pipeline/src/graph.rs` (W1)

`role` → `Transform`; `kind_name` → `"keep_values"`; `is_implemented` → `true`. New rule 54: both
maps empty rejected; empty field name rejected; empty `allow` rejected (message names `set`/`remove`
as the alternative); non-finite `F64` in `allow`/`other` rejected; a non-lowercase `Str` under
`normalize: [lower]` rejected, naming field and literal; a duplicate `normalize` step rejected.
Duplicates within `allow` and `other` appearing in its own `allow` are deliberately legal.

### `crates/logit-cli/src/pipeline.rs` (W1)

A `build_spec` arm plus `fn to_allow_lists(...)` beside `to_set_pairs`, converting
`SetValue -> logit_core::Value` and `NormalizeStep -> Normalize` (`to_match_mode`'s pattern).

### Docs (W2)

`docs/design/pipeline-graph.md` (`ComponentKind` sketch, arity table, rule 54 text),
`docs/design/internal-telemetry.md` (the three counters), `docs/design/memory.md` (the allocation
rows), `crates/logit-transforms/src/lib.rs` (module doc + `mod`/`pub use`), `AGENTS.md`
(current-state paragraph, crate-layout line), `examples/nginx-to-influxdb.yaml` (the demonstrator),
`schema/logit.schema.json` (regenerated).

### The example (W2)

Inserted between `trimmed` (`keep`) and `windowed` (`aggregate`) in
`examples/nginx-to-influxdb.yaml`:

```yaml
  bounded:
    type: keep_values
    sources: [trimmed]
    attributes:
      host:
        normalize: [lower]
        allow: [static.local, proxy.local]
        other: other
```

`windowed.sources` becomes `[bounded]`.

## Workstreams

| # | PR | Files | Depends |
|---|---|---|---|
| W0 | **ADR and plan.** | `docs/adr/value-allowlist-cardinality-clamp.md` (new, + row atop `docs/adr/README.md`); `docs/plans/value-allowlist-cardinality-clamp.md` (new, + row atop `docs/plans/README.md`) | — |
| W1 | **`keep_values` — the component.** | `crates/logit-config/src/lib.rs`; `crates/logit-pipeline/src/graph.rs`; `crates/logit-transforms/src/keep_values.rs` (new); `crates/logit-transforms/src/lib.rs`; `crates/logit-cli/src/pipeline.rs`; `schema/logit.schema.json` | W0 |
| W2 | **Docs, example, and the allocation pin.** | `examples/nginx-to-influxdb.yaml`; `docs/design/pipeline-graph.md`; `docs/design/internal-telemetry.md`; `docs/design/memory.md`; `crates/logit-bench/src/fixtures.rs`; `crates/logit-bench/tests/allocations.rs`; `AGENTS.md`; `docs/known-gaps.md` | W1 |

Landing order: **W0 → W1 → W2**, strictly linear. Config, validation, transform and registry are one
PR because `build_spec`'s match is exhaustive — a variant added without its arm doesn't compile, and
`is_implemented` returning `true` without one is the exact "schema advertises what the binary won't
run" failure `routing-by-condition-is-lua` closed.

### Status (2026-09-16)

All three workstreams built as a linear stack of PRs, each targeting its parent: W0 #227
(`kvals/w0` → `main`), W1 #228, W2 (this closeout) on `kvals/w2` → `kvals/w1`. Nothing merged;
Ross directs merging. Allocation pins landed exactly as designed: **0** for an already-allowed,
already-lowercase `host`, **1** for one that needs lowering before it's allowed.

### Per-workstream detail

**W0** — Done when: both README index tables have their row, dated 2026-09-15, and every
cross-reference resolves.

**W1** — Tests: config deserialization (JSON, per the crate's own convention) for the full form,
`normalize`/`other` defaulting, `NormalizeStep`'s snake_case tag, `SetValue`'s untagged-order pin;
graph rule 54, one test per clause; `build_spec_builds_a_keep_values_transform`. Unit tests in
`keep_values.rs`: allowed value untouched; disallowed replaced; disallowed removed when `other`
absent; absent attribute is a no-op, not a stamp; numeric coercion; `Bool` never coerces; resource
clamping through `map_resource` plus its cache hit; `dropped_attributes_count`/`schema_url` carried
forward; log/metrics/span untouched; idempotent clamping when `other` is in `allow`; all three
telemetry counters. Normalization: uppercase value written back lowercased when allowed; an
already-lowercase value doesn't fire `.normalized`; non-`Str`/`Bytes` values are untouched; a
non-UTF-8 `Bytes` value lowercases its ASCII bytes only; normalization happens before the allow
test. Done when: `script/check` green and `script/schema` leaves no diff.

**W2** — Tests: `every_shipped_config_loads_and_validates` covers the edited example for free;
extend `lib.rs`'s `chained_pipeline_test` so the headline chain is
`json -> scale -> kv_metrics -> keep -> keep_values -> aggregate`, asserting a junk `host` collapses
into one `other`-tagged series. Two allocation tests beside `keep_one_event`: an already-lowercase
allowed value pinning 0, and an uppercase value that must be lowered pinning 1, with matching
`docs/design/memory.md` rows. Done when: `script/cibuild` green, `script/validate` clean, no
`type_sizes.rs` change.

## Verification

- `script/check` during the loop; `script/cibuild` before each PR.
- `script/schema` after the W1 config change; commit the result.
- `script/validate` over `demo/`, `examples/`, `perf/scenarios/`.
- Negative config check by hand: both maps empty; empty field name; `allow: []`; `allow: [.nan]`;
  `normalize: [lower]` with `allow: [Static.Local]`; `normalize: [lower, lower]`.
- `logit graph` on the edited example — `bounded` renders as an ordinary transform node between
  `trimmed` and `windowed`.
- Manual smoke: `script/server` against `examples/nginx-to-influxdb.yaml` plus the real nginx,
  `curl -H 'Host: static.local'`, `-H 'Host: STATIC.Local'`, `-H 'Host: junk.example'`, `-H
  'Host: proxy.local'`, and no `Host` at all. Expect InfluxDB to show `host=static.local` (both
  casings folded) and a single `host=other` series, no per-junk-value series. **Done 2026-09-16**:
  queried `metrics.nginx.requests` after the requests above and a 10s aggregate flush -- exactly
  three distinct `host` values came back (`static.local`, `proxy.local`, `other`), with per-value
  counts `static.local=3` (the two casings plus nginx's own default-server resolution of the
  empty-Host request), `proxy.local=1`, `other=1` (the one junk value). No stray series for
  `STATIC.Local` or `junk.example` at all.

## Open risks

- Ordering is the operator's responsibility, as with `scale`/`keep` — no error for a `keep_values`
  placed after `aggregate` or after a sink has already consumed the tag.
- `other` colliding with a real value (a vhost literally named `other`) is the operator's choice of
  bucket name, not reserved or escaped.
- `normalize:`'s write-back is visible to every downstream consumer, not just the allow-list test —
  stated in the ADR's Consequences alongside `scale`'s identical ordering caveat.
- A tag stuffed with junk still costs everything upstream of the clamp (decode, parse); `keep_values`
  cannot undo work already done by earlier components.
