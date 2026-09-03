---
created: 2026-08-28
updated: 2026-08-28
---

# Configuration: YAML with a generated JSON Schema

## Status
Accepted

## Context
Config needs to describe inputs, transforms (including inline or referenced Lua), and outputs, be
comfortable to hand-write, and be validatable by editors and CI before `logit` ever runs.

## Decision
YAML, deserialized with `serde`. A JSON Schema is generated directly from the Rust config types via
[`schemars`](https://github.com/GREsau/schemars) and published (`logit schema` prints it; CI writes
it to `schema/logit.schema.json`), so the schema can never drift from what the binary actually
accepts.

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
