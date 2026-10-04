---
created: 2026-08-28
updated: 2026-10-04
---

# Configuration: YAML with a generated JSON Schema

## Status
Accepted

## Context
Config needs to describe inputs, transforms (including inline or referenced Lua), and outputs, be
comfortable to hand-write, and be validatable by editors and CI before `logit` ever runs.

## Decision
YAML, deserialized with `serde`. A JSON Schema is generated directly from the Rust config types via
[`schemars`](https://github.com/GREsau/schemars) and published (`logit schema` prints it). An
ordinary `logit-config` test compares generated output with `schema/logit.schema.json`, so every
workspace test run detects drift without regenerating a tracked file as a side effect.

For YAML parsing: use a maintained fork — `serde_norway` or `serde_yaml_ng` (decide at
implementation time; check crates.io activity) — rather than `serde_yaml`, which its author archived
in 2024.

## Alternatives considered
- **TOML.** Common in the Rust ecosystem, but far less common in this project's actual domain
  (observability-pipeline configs — Vector, the OTel Collector, Fluent Bit, Telegraf — are
  overwhelmingly YAML or a custom DSL), and nests less comfortably for deep pipeline definitions.
- **A hand-maintained JSON Schema, written separately from the Rust types.** Rejected outright: two
  sources of truth for the same shape will drift the first time someone adds a field and forgets the
  schema.

## Consequences
- Every config type derives `Deserialize` and `JsonSchema` together; adding a field updates
  validation for free.
- Inline Lua in YAML needs a documented multiline-string convention (YAML block scalars) as well as
  the file-reference form, both covered in the schema.

## Amendment: unknown keys are rejected (2026-10-04)

An unknown key anywhere in a config file is a deserialization error, so `logit validate` and
`logit run` fail at load and name it. A key `logit` doesn't read is either a typo or a field that
no longer exists, and dropping it starts the component with a default the operator didn't choose:
a bare `tls:` under `prometheus_in`, whose keys are `scrape_tls:` and `bind_tls:`, started the
scrape client without the operator's CA.

- **Every fixed-shape config type carries `#[serde(deny_unknown_fields)]`.** Free-form maps whose
  keys are the operator's data (`set`'s attributes, `headers`, `routes`, and the like) stay open.
- **`ComponentKind` denies at the enum.** `Component` flattens the `type`-tagged enum beside its
  four common keys (`sources`, `targets`, `buffer`, `receive`). serde gives `Component` those keys
  first and hands the enum the rest, so the enum's container attribute rejects anything no variant
  field names. The same holds for `tail_in` and `docker_in`, which flatten `TailOptions`: serde
  documents `deny_unknown_fields` with `flatten` as unsupported, but serde_derive rejects whatever
  the flattened fields leave unclaimed, and `logit-config`'s tests pin that for both kinds and for
  every shape of variant.
- **A component's error names its id.** A flattened, tagged enum's error carries no location, so
  `Config` deserializes `components:` itself and prefixes each error with
  ``component `<id>`:``. An unknown-key error also notes the four common keys, which serde's
  "expected one of" list for the kind omits. Only the id and the key are printed, never a value.
- **The schema matches.** `logit_config::json_schema()` post-processes `Component` so every `oneOf`
  variant has `additionalProperties: false` and lists the four common keys (as `true`, since
  `Component`'s own `properties` already describe their shape). An editor then flags the same keys
  `logit validate` rejects.

Rejected: a hand-written `Deserialize` for `Component` that peels the common keys off a map and
deserializes `ComponentKind` from the remainder. The derive already does that once the enum
denies, and a hand-written impl is one more place a new common key has to be added.
