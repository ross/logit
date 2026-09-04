---
created: 2026-09-03
updated: 2026-09-03
---

# Operator-declared resource attributes: a `set` transform, not a per-input config field

## Status
Accepted

## Context

PR #60 settled a rule for `internal`'s own telemetry: `service.name` names the producer of
telemetry, not the source of ingested data — `syslog_in`/`statsd_in` must not invent one for
traffic they merely receive (`docs/design/internal-telemetry.md`'s "Resource identity" section).
That's correct as far as it goes, but it left a real gap: **nothing in `logit` lets an operator
declare a resource identity either.** `docs/plans/otlp-logs-and-resource-identity.md`'s workstream
A found this while investigating a Loki-direct log leg for the demo — every OTLP-native backend
keys behavior off resource attributes (Loki's `index_label` action, for one, applies only to
resource attributes, never log-record ones), so `syslog_in` traffic with no configured identity
reaches such a backend as `{service_name="unknown_service"}`, indistinguishable from every other
unlabeled source.

The fix is not a data-model change. `EventBatch { resource: Arc<Resource>, events }` and
`Resource { attributes: AttrMap }` (`crates/logit-core/src/resource.rs`,
[data-model.md](../design/data-model.md)) already support a resource that varies per batch and is
replaced by minting a new `Arc` — `otlp_in` already does exactly this, decoding one resource per
`ResourceLogs`/`ResourceSpans` entry, and `aggregate` already groups by resource *value*, so a new
`Arc` flowing through costs it nothing. What's missing is a way for the pipeline to *produce* a
substituted resource: no `ComponentKind` inserts a constant value into either an event's or a
batch's attributes, and neither `Transform::process` nor Lua's `EventProxy` has a mutation path to
a batch's resource at all.

The workstream A plan sketched fixing this with a `resource:` field on `syslog_in`. This ADR
rejects that in favor of a graph component.

## Decision

**An operator declares a resource (or event-attribute) identity by inserting a `set` component
into the graph — `logit-transforms::Set` (`ComponentKind::Set` in config) — not by adding a field
to a listener.**

```yaml
web_identity:
  type: set
  sources: [web_logs]
  resource:
    service.name: nginx
    service.namespace: demo
  attributes:
    env: prod
```

`resource` is applied once per batch, to `EventBatch::resource`; `attributes` is applied per event,
to `event.attributes`. Both maps are optional but at least one must be non-empty (a `set` with
neither is a graph-validation error, the same rule `kv_metrics` already has for "can only ever be
a no-op"). Both overwrite on key collision: a configured value always wins over whatever the wire
carried, matching `syslog_out`'s existing `hostname`/`app_name` config fields — an operator naming
a value here is declaring what a specific pipeline stage carries, the same category of knowledge
those fields already represent, not code inventing an identity for data it didn't produce. The
rule from PR #60 stands unchanged: `logit`'s own code still may not guess a `service.name`.
`Set` overwrites; it never invents.

Mechanically, this needed one addition to `logit_pipeline::Transform`
(`crates/logit-pipeline/src/transform.rs`):

```rust
fn map_resource(&mut self, resource: &Arc<Resource>) -> Option<Arc<Resource>> { None }
```

Called once per incoming batch, before any event reaches `process` — `None` (the default, every
transform before `Set`) forwards the batch under the resource it arrived with, at no cost beyond
the call itself; `Some` substitutes it for both `process`'s argument and the outgoing batch.
`process_batch` (`crates/logit-pipeline/src/runtime.rs`) moves the incoming `Arc` straight through
on `None` rather than cloning it — confirmed by allocation tests
(`crates/logit-bench/tests/allocations.rs`), not just asserted.

Lua gets the same capability through a `resource` global (`crates/logit-script/src/resource.rs`),
readable and writable, mirroring `event.attributes`' `AttrsProxy` — copy-on-write, so a script that
never touches it costs nothing. `docs/design/lua-api.md` has the full contract.

## Alternatives considered

- **A `resource:` config field on `syslog_in`/`statsd_in`.** The workstream A plan's original
  sketch. Rejected: it only ever covers the input it's attached to (an operator wanting the same
  identity on two `syslog_in`s configures it twice, or a `kv_metrics`-derived batch downstream of
  several inputs has no field to reach for at all), it needs a second mechanism for event
  attributes (which already needed something like `set` regardless), and `otlp_in` would need
  either the field forever-unused or a special-cased "doesn't apply here" carve-out. A `set`
  component composes: insert it anywhere in the graph, feed it from one input or several, and it's
  the same mechanism for both resource and event attributes.
- **A special resource-setting mode bolted onto `keep`/`remove`.** Rejected as scope creep on two
  transforms whose whole contract is "filter, never insert" — `keep`'s module doc is explicit that
  its allowlist semantics are what make it safe against a new field appearing later; folding
  insertion into the same type muddies that.
- **Only a Lua `resource` global, no native `set` transform.** Rejected: the common case (stamp a
  few constant key/values) shouldn't require writing Lua, and a native transform is measurably
  cheaper — no VM, no proxy boundary.

## Consequences

- A new `ComponentKind::Set` and `logit_transforms::Set`, following the `Keep`/`Remove` pattern.
- `Transform::map_resource` is now part of the trait every native transform implements (with a
  free default), and `run_lua`'s per-batch loop threads a resource through `ScriptWorker::
  set_resource`/`take_resource` the same way it already threads trace context.
- A Lua `flush()` can now stamp a real resource on its own emission instead of relying purely on
  `last_resource`'s "whichever batch was last seen" approximation — see
  `docs/known-gaps.md`'s Lua-flush-staleness entry, amended, not resolved, by this.
- Demo-stack workstream B (`docs/plans/otlp-logs-and-resource-identity.md`) landed on this: the
  demo's log leg now sets `service.name`/`service.namespace` via `set`, giving Loki real index
  labels with no `demo/loki/loki.yaml` change (both are already in Loki's default index-label set).
- `otlp_in` gets no special treatment and needs none: an operator simply doesn't put a `set` after
  it, since it already carries a real resource off the wire.
