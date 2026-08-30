# 0010 — Secrets in config: a general `!env` YAML tag, not per-field `*_env` indirection

## Status
Accepted

## Context
`influxdb_out` needs an auth token, and a token doesn't belong inlined in a config file checked
into version control. The only mechanism for that today is a bespoke indirection field:
`token_env: String` names an environment variable, and `logit-cli::pipeline::build_spec` calls
`std::env::var` on it at startup (`crates/logit-cli/src/pipeline.rs`) — the only `std::env::var`
call in the workspace.

That doesn't scale past one field. Every future secret needs its own parallel `*_env` twin —
its own schema entry, its own doc line, its own read site in `build_spec` — and the split is
arbitrary about *which* fields qualify: `url`, `org`, and `bucket` are just as deployment-specific
as `token`, but only `token` can currently be pulled from the environment. A `syslog_in` TLS key, a
future `otlp_out` bearer token, or simply wanting one config file to work across dev/staging/prod
by varying a bind address would each want the same treatment.

## Decision
One general-purpose YAML tag, `!env VAR_NAME`, usable as the value of any field on any component.
It's resolved by walking the parsed YAML tree (`serde_norway::Value`) *before* deserializing into
`Config` — the whole point of a YAML tag over, say, a `${VAR}`-style string convention: resolution
happens at a layer the config types never see, so no field's type changes and the published JSON
Schema needs no per-field widening to admit it.

```yaml
influx_out:
  type: influxdb_out
  url: !env INFLUXDB_URL
  token: !env INFLUXDB_TOKEN
```

`influxdb_out`'s `token_env: String` collapses back into a plain `token: String`
(`crates/logit-config/src/lib.rs`) — the last and only env-specific field in config, and no
component needs one again.

**The substituted value is re-parsed as a YAML scalar**, not always coerced to a string: `!env
PORT` with `PORT=8125` becomes the integer `8125`; `true`/`false` become a bool; anything else
(including a value that happens to parse as a mapping or sequence) stays a string. This is what
makes `!env` usable on *any* field — a numeric `throttle.limit`, a boolean flag, not only
string-typed ones like `token`. The cost: a secret that happens to look like a number or a bool
(`token: !env T` with `T=123456`) resolves to a non-string and fails deserialization with a type
error, rather than working. Accepted as a documented rough edge rather than solved by always
coercing to a string, because the always-a-string alternative fails silently in the opposite,
worse direction for numeric fields — see Alternatives.

**Strictness depends on the command, matching what each is for.** `logit run` and `logit validate`
error on an unset variable — `validate` is meant to be a real preflight, run before restarting a
service, and it should catch a missing secret exactly the way `run` would. `logit graph` (docs:
`docs/design/pipeline-graph.md`'s "`logit graph`" section) substitutes a `<unset:VAR>` placeholder
and warns on stderr instead: it renders a config's *shape*, never its values, and its documented
job is to render *something* useful even for a config that's otherwise broken — reading field
values was never part of that contract, so this costs it nothing, and a missing variable alone
never makes it exit non-zero.

**Any tag other than `!env` is a hard error.** `serde_norway` silently drops an unrecognized tag
on a field that isn't itself an enum, so a typo'd `!emv INFLUXDB_TOKEN` would otherwise
deserialize as the literal string `"INFLUXDB_TOKEN"` — a config that starts and authenticates with
garbage instead of failing to load. Rejecting every tag but `!env` turns that into a load-time
error naming the bad tag and its config path.

All of this lives in one new module, `crates/logit-cli/src/config.rs`, which becomes the *only*
place a config file is read and parsed — `logit run`, `logit validate`, and `logit graph` all
previously called `std::fs::read_to_string` + `serde_norway::from_str` independently
(`main.rs`, `pipeline.rs`); centralizing it is what lets `!env` (and the unknown-tag guard) apply
uniformly to all three rather than needing to be threaded through three call sites individually.

## Alternatives considered
- **Keep per-field `*_env` twins, add one for every field that might be secret.** The status quo.
  Rejected outright — it's the problem being solved, not a form of solving it: a new field on any
  component needs its own indirection field, its own schema entry, its own read site, forever.
- **`${VAR}` string interpolation inside string values.** Common in other tools (Docker Compose,
  many CI systems). Rejected: needs an escaping convention for a literal `${...}` in normal text,
  only ever produces a string (fields like `throttle.limit` or `sample.rate` couldn't use it), and
  is invisible to the JSON Schema in the same way `!env` is — no compensating advantage over a tag
  to offset those two real limitations.
- **An `{env: VAR}` object form** (`token: {env: INFLUXDB_TOKEN}`), no custom YAML tag needed.
  Rejected: it widens the *shape* of every field that wants to admit it — `token: String` would
  need to become `token: StringOrEnvRef`, an enum, on every field, everywhere. `!env` needs no
  schema change at all, at the cost of the schema not knowing about it either way (see
  Consequences).
- **Always coerce the substituted value to a string**, never re-parsed as a scalar. Simpler (no
  scalar re-parse, no risk of a numeric-looking secret misfiring) but far less general: it would
  only ever work on already-string-typed fields, which is most of config today but not all of it,
  and every future numeric or boolean field would be unable to use `!env` at all rather than
  merely needing its variable's value quoted. Rejected as solving today's problem in a way that
  reintroduces tomorrow's.

## Consequences
- `token_env: String` → `token: String` on `influxdb_out`, and `schema/logit.schema.json`
  regenerates accordingly — the only schema change this feature causes, and the last time an
  env-specific field will exist in config at all.
- `!env` is invisible to `schema/logit.schema.json`: resolution happens before serde, so the
  schema describes the *substituted* shape, not the tag. A schema-aware YAML editor will flag a
  `!env`-tagged value it can't resolve against the schema. Documented in `docs/known-gaps.md`.
- Config deserialization errors lose line/column information: `serde_norway::from_value` (needed
  to deserialize the already-resolved tree) has no source location the way
  `serde_norway::from_str` does. Partly offset by `!env`'s own errors carrying a config path
  (`components.influx_out.token`), and by the note appended when a substitution's resolved type
  likely caused the failure. Documented in `docs/known-gaps.md`.
- `crates/logit-pipeline/src/graph.rs` rule 7 (`is_implemented`) Debug-prints a whole
  `ComponentKind` on failure (`"kind {:?} is not implemented yet"`). Harmless today — no
  *unimplemented* kind carries a secret field — but with secrets inlined directly into fields
  rather than referenced by name, that becomes a real leak the moment one does. Documented in
  `docs/known-gaps.md` as a rough edge to fix before any unimplemented kind gains a secret field.
- `AGENTS.md`'s "Conventions to hold to" gains a rule: config is loaded through
  `logit_cli::config::load`, never a bare `serde_norway::from_str` — otherwise `!env` and the
  unknown-tag guard silently stop applying on whichever path skips it.
