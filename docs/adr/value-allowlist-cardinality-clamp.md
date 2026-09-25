---
created: 2026-09-15
updated: 2026-09-15
---

# `keep_values`: an attribute-value allowlist, and an optional normalize-before-compare step

## Status

Accepted

## Context

`keep` bounds metric-tag cardinality by attribute **key**: anything not named is dropped, so a new
field appearing in a log format later can never silently become a new tag dimension
(`crates/logit-transforms/src/keep.rs`'s module doc). Nothing bounds it by attribute **value**, and
that is the half that matters once a key is legitimately kept but the producer doesn't enforce what
values it carries.

[fixtures/nginx-to-influxdb.yaml](../../fixtures/nginx-to-influxdb.yaml) is the live instance of the
gap:

```yaml
  trimmed:
    type: keep
    sources: [nginx_metrics]
    fields: [host, request_method, status]
```

`request_method` and `status` are closed sets nginx itself constrains. `host` is nginx's `$host` —
the client's `Host` header, unbounded and attacker-controlled behind a public IP.
[fixtures/nginx/nginx.conf](../../fixtures/nginx/nginx.conf) serves exactly two vhosts
(`static.local`, `proxy.local`), so the intended cardinality of `host` is 2; traffic stuffing junk
into the header makes it effectively unbounded. That value lands directly in `aggregate`'s
`SeriesKey` (`crates/logit-transforms/src/aggregate.rs`, keyed on the whole of `event.attributes`),
so it explodes both series count and per-window memory, and then lands again as an InfluxDB tag —
the same fan-out `keep`'s own module doc warns about, one level further in.
[`docs/known-gaps.md`](../known-gaps.md) records the *truncation* risk from an oversized `Host`; the
cardinality risk from a merely-junk one was never recorded anywhere.

This generalizes past `Host` headers: any tag whose valid set the operator knows but the producer
doesn't enforce — vhost, tenant id, region, a route template, an upstream name.

**Why this is a bounded matcher, not `lua`.**
[ADR `routing-by-condition-is-lua`](routing-by-condition-is-lua.md) retired `rename`/`filter`/
`sample`/`throttle`/`dedup` because each was already expressible in Lua and a native version bought
ergonomics, not new function. That ADR's revisit trigger — sustained central-collector throughput
pressure — has since fired three times, each answered the same way:
[`has_attributes`/`drop_attributes`](attribute-filtering-components.md),
[`has_provenance`/`drop_provenance`](provenance-filtering-components.md), and
[`target`/`route`](target-components.md) are each a **bounded set-membership test**: no operators,
no boolean algebra, no predicate language, config no wider than `set`'s. This is the same shape —
"is this value one of N configured values" — applied to mutation instead of filtering or routing,
and it earns being native for the same two reasons those did: it sits in the hot path of every event
in the central-collector role where the ~3x Lua constant is real (that ADR's own cost table), and
cardinality defence is exactly the job `keep` is already native for.

**Why a normalize step, and why now.** An exact-match allow-list is only as good as its producer's
consistency. Two failure modes fall out of that: a genuinely valid value split across casing
variants becomes two series instead of one (a cardinality cost with no attacker involved at all),
and an operator writing `allow: [static.local]` against a field they've never seen mixed-case gets a
silent `other` the first time they do. Both are closed by the same one step — lowercase before
comparing — so it is included from the start rather than deferred to a follow-up, on the same
reasoning `scale` gives for handling unit conversion in one general transform rather than waiting
for a second caller to justify it.

## Decision

**`keep_values` is a new `ComponentKind`/`Transform`.** Per field, in both `resource:` and
`attributes:` maps (mirroring `set`'s two-map shape field for field):

```yaml
attributes:
  host:
    normalize: [lower]   # optional, ordered, applied before the allow test and written back
    allow: [static.local, proxy.local]
    other: other         # optional; absent removes the attribute instead
```

**Matching is exact equality only**, via the existing `logit_transforms::value_matches` — the same
total, allocation-free, numeric-coercing comparison `has_attributes` and `route` already use. No
globs, no suffix/prefix matching, no operators: crossing that line is exactly the predicate-language
creep `routing-by-condition-is-lua` retired, and an operator needing one still writes `lua`.

**A value outside `allow` becomes `other` if configured, or is removed if not.** Removal, not a
dropped event: `keep_values` never drops an event, matching `keep`/`remove`/`set`/`scale`'s posture
that a per-field operation shouldn't fail the whole event over one field.

**An attribute the event doesn't carry is a silent no-op for that field, never a stamp.** This
clamps a value that's already there; inventing one that wasn't is `set`'s job. Same rule `scale`
already has for a missing field.

**`normalize:` is an optional, ordered list of rewrite steps, applied per field before the `allow`
test, and its result — not just the comparison — is what's written back to the event.** One step
exists today, `lower`. The list shape (rather than a single enum) costs nothing now and leaves room
for `trim`/`strip_port`/`strip_trailing_dot` later with no config break. Write-back, not
compare-only, is deliberate: `HOST.example.com` and `host.example.com` are the same legitimate host
and should collapse into one series whether or not either is on the allow-list's mind — folding them
is a cardinality win independent of clamping, and `keep_values` is where the field is already being
looked at.

**`lower` is ASCII-only, applied bytewise to `Str`/`Bytes` only.** Every other `Value` variant passes
through untouched by any step. Not Unicode case folding, for three reasons: hostnames and the
motivating examples (vhosts, route templates, region codes) are ASCII — IDN is punycode, not raw
Unicode, at the wire; full case folding is locale-dependent (Turkish dotless ı is the standard
example) and would make the same byte sequence normalize differently depending on where `logit`
runs; and folding can change a string's byte length, while a bytewise lowercase cannot, keeping the
common no-op path — a value already lowercase — a linear scan with zero allocation. Bytewise also
makes it total on non-UTF-8 `Bytes` with no validation step, matching `value_matches`'s own
`Str | Bytes` treatment.

**A `SetValue::Str` in `allow` or `other` that isn't already ASCII-lowercase under
`normalize: [lower]` is rejected at `logit validate` time, not silently rewritten.** In `allow` it
could never match anything a `lower` step could ever produce — dead config, the same instinct as
`scale`'s non-finite-factor rejection. In `other` it would break the field's own declared invariant
("normalized to lowercase") the moment it's substituted in. Config validation names the field and
the offending literal rather than silently lowercasing what the operator wrote, on the same posture
`env-yaml-tag` and `scale` take toward a config author's likely typo: catching it at validate time is
strictly better than a mismatch nobody notices.

**Stateless**, like `scale`/`keep`/`set`: only `process`/`map_resource` are overridden;
`flush_interval`/`flush` keep the `Transform` trait's defaults. No `Diagnostics` builder — a clamp
is documented behavior, not a failure, matching `Keep`/`Remove`/`Set`/`Scale`'s posture exactly.

**Graph validation** (rule 54) rejects: both maps empty (certain no-op, `set`'s own rule); an empty
field name in either map (`trace_context`'s rule, extended); an empty `allow` list (a config that
could only ever clamp everything — that's `set` with `other:` or `remove` without it, both of which
already exist, and the error names them); a non-finite `SetValue::F64` in `allow`/`other`
(`value_matches` makes it match nothing, so it's dead config, `has_attributes`' rule 36 reasoning);
a non-lowercase `Str` literal under `normalize: [lower]` (above); and a duplicate step within one
field's `normalize:` list (certain no-op). An empty `normalize:` list is legal — it's the default,
meaning no normalization.

## Alternatives considered

- **`clamp` or `allow_values` as the name.** Rejected: `clamp` names the intent but not the
  mechanism, and neither continues the `keep`/`has_`/`drop_` prefix families the catalog already
  uses. `keep_values` reads as `keep`'s value-side sibling on sight.
- **Glob or suffix patterns in `allow`** (`*.example.com`). Genuinely more expressive, but a glob is
  an operator — exactly the line `routing-by-condition-is-lua` draws around this whole family of
  components. Crossing it here for convenience would invite the same creep that ADR spent real
  effort naming and closing three times already.
- **A compare-only `case_insensitive: true` flag instead of a write-back `normalize:` list.** Fixes
  the error-proneness half of the motivation but not the cardinality half: `HOST.example.com` and
  `host.example.com` would still match the same allow-list entry but land on the event, and in
  `aggregate`'s `SeriesKey`, as two different strings — two series, not one. Rejected because the
  cardinality tool that doesn't fold casing into one series is only half doing its job.
- **Unicode case folding (`str::to_lowercase`) instead of ASCII-only.** Rejected for the three
  reasons in the Decision section: hostnames are ASCII at the wire, full folding is
  locale-dependent, and folding can change byte length where a bytewise lowercase cannot — the
  latter is also what keeps the already-lowercase path allocation-free.
- **Silently lowercasing `allow`/`other` config entries instead of rejecting a non-lowercase one.**
  Friendlier on the surface, but it hides a typo (`allow: [Static.Local]`) rather than naming it,
  and it would mean the schema's stated field invariant ("normalized values only") doesn't actually
  hold for what the operator typed, only for what `logit` silently turned it into.
- **A dynamic `max_values: N` cardinality cap** (admit the first N distinct values seen per
  interval, clamp the rest). Genuinely useful for a tag whose valid set the operator *doesn't* know
  up front, which a static allow-list can't help with at all — but admission is arrival-order
  dependent: an abusive burst arriving first claims the N slots and evicts values that would
  otherwise have been legitimate, an operational surprise a static allow-list never has. It also
  needs real state (a seen-set, a reset interval), unlike this component, which stays stateless.
  Deferred rather than folded in here; if built, it's a separate stateful `ComponentKind` with its
  own ADR, not a mode of this one.
- **A normalize-only component, with no `allow:`.** Rejected: that's a value-side `scale` (rewrite
  in place, no clamping), a different job with a different name. Folding it into `keep_values` by
  making `allow:` optional would turn a one-job allowlist into a two-job component that sometimes
  keeps everything, which is exactly the kind of flag-driven double duty `scale`'s own ADR rejected
  when it turned down putting unit conversion on `kv_metrics`.
- **Extending `keep`'s own `fields:` list** with a per-field value constraint. Rejected: `keep`'s
  config is a flat `Vec<String>` specifically because it's a pure key-side allowlist with nothing
  else to configure per field; bolting a value constraint on would mean two shapes for `fields:`
  depending on whether a value constraint is present, worse than a second component.
- **A component-wide `other:` instead of per-field.** Terser for the single-field case, but forces a
  second `keep_values` the moment two fields want different fallbacks (drop a junk `host` outright,
  bucket a junk `tenant` into `"other"`) — the same reasoning `kv_metrics`' per-`MetricSpec` shape
  already follows.
- **A `host: [a, b]` list shorthand for the struct**, i.e. an untagged enum alongside
  `ValueAllowList`. Rejected: `component-graph-configuration` is on record disliking two ways to
  spell the same config, and the shorthand would need its own defaulting rules (no `normalize`, no
  `other`) duplicated against the struct's.

## Consequences

- **`routing-by-condition-is-lua`'s revisit trigger fires a fourth time**, for the same reason as
  its three prior firings: a bounded set-membership test in the hot path of the central-collector
  role. That ADR's core holding is unchanged by this one — `logit` still ships no native predicate
  language, no `filter`/`where`, no operators; anything needing one still means writing `lua`.
- `event.attributes` (and, via `map_resource`, `Resource.attributes`) can now be rewritten by four
  different transforms with four different postures: `set` (unconditional overwrite from
  constants), `trace_context` (lift-then-remove), `scale` (read-modify-write in place, skip on
  failure), and `keep_values` (read, optionally normalize, clamp-or-remove). There is still no
  unifying "attribute-mutation" trait, for the same reason `scale`'s own ADR gave: each one's
  failure/skip semantics differ enough that a shared abstraction would need per-variant escape
  hatches anyway.
- **Ordering is the operator's responsibility**, exactly as it already is for `scale` and `keep`. A
  `keep_values` placed after `aggregate`, or after the tag has already reached a sink, gets no
  error — `logit validate` checks the graph's shape, not the semantic effect of node order.
- **`normalize:`'s write-back is visible to every downstream consumer**, not just the allow-list
  test — a log sink rendering the same field verbatim will now show the lowercased form too. That's
  the intended cardinality win, but an operator wanting the original preserved for some other
  consumer has to place the clamp after whatever needs the untouched value, the same caveat
  `scale`'s ADR already states about its own in-place rewrite.
- `fixtures/nginx-to-influxdb.yaml` gains a `bounded: keep_values` component between `trimmed`
  (`keep`) and `windowed` (`aggregate`), closing the unbounded-`host` gap
  [`docs/known-gaps.md`](../known-gaps.md) already flags the truncation half of. `docs/known-gaps.md`
  is updated to note the cardinality half is now closed by example, without removing the truncation
  entry (a different failure mode, still real, still gated by nginx's own defaults rather than
  anything `logit` does).
- A `keep_values` clamping everything on a field the config author no longer needs (every legitimate
  value already covered, and the field is now dead weight) gets no warning — the same non-goal
  `has_attributes`/`drop_attributes`'s ADR already accepts for its own bounded matcher.
