---
created: 2026-09-02
updated: 2026-09-03
---

# Enabling plan: operator-declared resource identity, and a Loki-direct log leg

## Context

While closing out PR #60 (`service.name` on `internal`'s telemetry, and a dashboard panel-type
fix), a follow-on question came up: now that `logit` speaks OTLP, can the demo drop `alloy` and
ship logs straight to Loki over OTLP instead of `syslog_out` → Alloy → Loki?

Investigating that surfaced a cluster of real, interconnected findings well past PR #60's scope.
This plan exists to write them down before they're lost, in a form a separate session can pick up,
research, prioritize, and act on. **It is documentation only** — no code, config, or demo file
changes. Everything below is already verified: file:line from the source tree, live probes against
the running demo stack, and one claim checked against Grafana's own OTLP-ingestion docs (noted
where it's the one thing not verified by direct observation).

## Decisions already settled

- **`service.name` names the producer of telemetry, not the source of ingested data.** `internal`
  may name itself `logit`; `syslog_in`/`statsd_in` must not, because that data belongs to other
  services; `otlp_in` inherits the sender's own resource verbatim. Landed in PR #60; the rule lives
  in `docs/design/internal-telemetry.md`'s "Resource identity" section.
- **OTLP logs export is built and works.** `crates/logit-proto/src/otlp/logs.rs:99-125`
  `encode_log_record`, dispatched by `crates/logit-proto/src/otlp/mod.rs:78-146` `encode_signals`,
  one HTTP/gRPC request per non-empty signal (`crates/logit-outputs/src/otlp.rs:245-254`). This
  isn't new work — it's already shipped, just never exercised by the demo.
- **Loki 3.7.7 (the demo's pinned version) accepts OTLP logs today.** Verified live: `GET
  http://loki:3100/otlp/v1/logs` returns `405 Method Not Allowed` (route exists, POST-only), and
  `auth_enabled: false` in `demo/loki/loki.yaml` means no tenant header is required.
- **The path is already compatible.** `otlp_out`'s per-signal path is hardcoded
  (`crates/logit-proto/src/lib.rs:85-91`, `/v1/logs` for the logs signal), and
  `endpoint: http://loki:3100/otlp` resolves to exactly `/otlp/v1/logs`.

## Gaps this plan exists to schedule

### A. Operator-declared resource attributes — done

**Status: landed.** What was originally a finding-plus-sketch is now a shipped `set` transform,
`Transform::map_resource`, and a read/write Lua `resource` global — see
[ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md) for
the design and rationale, [lua-api.md](../design/lua-api.md)'s "Reading and writing `resource`"
section for the script-facing contract, and [data-model.md](../design/data-model.md) for the
one-line model update (a batch's `Resource` was already dynamic — `Arc`-swappable, not
immutable — the gap was a *mutation surface*, not the data model). Kept here, in past tense, as the
record of what workstream A originally found and how it was resolved; the finding itself is no
longer filed in `docs/known-gaps.md`.

An operator now writes, for example:

```yaml
web_identity:
  type: set
  sources: [web_logs]
  resource:
    service.name: nginx
    service.namespace: demo
```

**Why a `set` graph component, not the per-input `resource:` field this plan originally
sketched:** one mechanism composes anywhere in the graph (several inputs feeding one `set`, or one
input feeding several differently-configured `set`s), covers event attributes the same way it
covers the resource (a second thing this plan's original sketch would have needed its own
mechanism for regardless), and needs no per-input schema surface — `otlp_in` in particular needs
no special-casing, since an operator simply never puts a `set` after it. See the ADR's
"Alternatives considered" for the full comparison.

**The `BatchAccumulator` interaction this plan's original sketch worried about doesn't arise.**
`BatchAccumulator` (`crates/logit-pipeline/src/accumulator.rs`) is listener-side batching, upstream
of every transform — `set`'s `map_resource` runs downstream of it, on an already-accumulated
batch, so its `Arc::ptr_eq` resource-change flush logic never sees or interacts with `set` at all.

Value types settled as scalars (string/int/float/bool via `SetValue`, an untagged enum) rather than
strings-only — a per-event setter that couldn't write a number would have been a wart from day
one. No config-root default was added; per-component configuration (insert a `set` wherever it's
needed) covers the same ground without a second inheritance mechanism to reason about.

### B. A Loki-direct log leg, dropping `alloy` — done

**Status: landed.** `demo/logit.yaml`'s `log_out` is now `otlp_out` (protocol `http`, endpoint
`http://loki:3100/otlp`), fed by a new `access_identity` (`set`) component that stamps
`service.name: demo-hello`/`service.namespace: demo` onto the batch's resource right after
`access_in`. `demo/alloy/` and the `alloy` service/volume in `demo/compose.yaml` are gone
entirely; `logit`'s `depends_on` gained `loki: {condition: service_healthy}` so `log_out` doesn't
spend its first several seconds retrying into a cold Loki. The three Loki dashboard panels
(`demo/grafana/dashboards/logit-internal.json`) were rewritten from `{job="demo"}`/`by (host)` to
`{service_name="demo-hello"}`/`by (service_name)` — Loki's OTLP resource-attribute label naming
(dots become underscores).

**No `demo/loki/loki.yaml` change was needed**, confirming the analysis below: `service.name`/
`service.namespace` are already in Loki's default index-label set.

The analysis that motivated this, kept for the record:

**The correction that killed the obvious shortcut:** Loki's `otlp_config` allows the `index_label`
action *only* for resource attributes, never for log-record attributes (confirmed against Grafana's
own docs: "It additionally allows index_label action for Resource Attributes" — log/scope
attributes are limited to `structured_metadata` or `drop`). `syslog.hostname`/`syslog.tag` were
encoded as **log**-record attributes by `logit` (`crates/logit-proto/src/otlp/logs.rs:99-104` pushes
`event.attributes` onto `LogRecord.attributes`), not resource attributes. So no Loki-side
`otlp_config` change could have promoted them to index labels — full stop. This is what made
workstream A a hard prerequisite for a demo that doesn't look broken, not an optional enhancement.

**Loki's live default index-label set**, captured verbatim from the running demo stack's `/config`
endpoint at the time: `service.name`, `service.namespace`, `service.instance.id`,
`deployment.environment`, `deployment.environment.name`, `cloud.region`,
`cloud.availability_zone`, and a run of `k8s.*` / `container.name` keys. Everything else on the
resource → structured metadata, never a label, unless explicitly configured otherwise.

**Where the old labels came from — entirely Alloy, nothing else** (now removed):
`demo/alloy/config.alloy` set static `job="demo"`, `protocol="udp"`, and relabeled
`__syslog_message_hostname`→`host`, `__syslog_message_app_name`→`app`.

### C. What replacing Alloy cost: `syslog_out`'s only demo exercise — accepted

**Decision: accept the loss, document it, no loop-back.** `syslog_out` is no longer exercised
end-to-end by anything in the demo — it stays fully covered by its own unit/integration tests and
by `docs/adr/syslog-output.md`'s decisions, but the demo no longer demonstrates third-party syslog
receiver interop. This was an explicit choice, not an oversight: the demo was never going to stay
exhaustive over every `ComponentKind` as more land, and keeping Alloy solely to exercise one
component, or looping `syslog_out` back into a second `syslog_in` to prove `logit` talks to itself
(not real third-party interop, Alloy's actual evidentiary value), were both rejected as not worth
what they'd cost or not actually preserving what mattered.

Confirmed: removing Alloy does **not** invalidate `docs/adr/syslog-output.md`'s decisions (RFC 6587
octet-counting auto-detect, `idle_timeout` handling, `syslog.*` attribute round-tripping). Alloy was
the *evidence* those choices interoperate with a real receiver, not the *reason* for them — those
passages still cite Alloy v1.19.2 as the receiver they were verified against; only the ADR's
now-stale claim that `demo/compose.yaml` has a live `alloy` service was corrected.

### D. Log↔trace correlation has no native OTLP path yet — model/codec/Lua/transform half done

**Status: `LogRecord` now carries a native application trace/span reference.** `logit_core::
LogRecord::trace: Option<TraceRef>` — [ADR `log-record-trace-context`](../adr/log-record-trace-context.md)
has the full design. `otlp_out` encodes it (falling back to the same `Event`'s `span` when the log
has none of its own); `otlp_in` decodes it, leniently, per OTLP's own contract for an invalid log
trace id; a new `trace_context` native transform lifts one off an already-JSON-decoded attribute
without writing Lua; `event.log.trace_id`/`span_id`/`trace_flags` are read+write from Lua. So an
OTLP log sink — Loki included — can now get native trace correlation from `logit`, wherever the
trace context comes from (the wire, an attribute, or a script).

**What's still open, deliberately:** no mode stamps `logit`'s *own* pipeline trace context onto a
log automatically — a script can do it by hand
(`event.log.trace_id = trace.trace_id`), but that's an application-identity decision an operator
must opt into, never a default (`docs/known-gaps.md`). And the demo-stack half of this workstream
— wiring the demo's log leg to actually carry a trace context, and the Grafana-side upgrade this
paragraph originally described (a second `derivedFields` entry keyed on Loki's `trace_id`
structured metadata instead of the existing body regex) — is explicitly deferred to the demo app's
own tracing rework, a separate, already-planned piece of work. The demo's existing correlation (a
Grafana `derivedFields` regex over the log body,
`demo/grafana/provisioning/datasources/datasources.yaml`) is untouched and still works identically
either way.

This also still connects to a previously-noted future direction: converting logs into span/trace
events (e.g. nginx as a trace root arriving only via logs). Whichever component does that
conversion is also the natural place to assign `service.name` for that data — same principle as
workstream A, one level up the stack.

### E. `otlp_out` config gaps found along the way

Surfaced while checking Loki compatibility; none block the demo, all block a real deployment:

- No custom headers — no way to send `X-Scope-OrgID`, which rules out any multi-tenant Loki, Mimir,
  or Grafana Cloud target.
- No compression support (`crates/logit-outputs/src/otlp.rs:479-482` — the frame's compressed flag
  is always `0`).
- No TLS configuration for gRPC — `reject_insecure_grpc_endpoint`
  (`crates/logit-outputs/src/otlp.rs:359-370`) hard-rejects an `https://` endpoint under
  `protocol: grpc` at construction time, rather than supporting it.
- No signal filter — a sink sends whatever signal types the events in its batch happen to carry;
  there's no way to say "logs only" at the sink.
- Hardcoded per-signal paths (fine for the standard OTLP layout, blocks any backend using a
  different mount point).
- `observed_time_unix_nano` always `0` on encode (`logs.rs:114`).

`docs/known-gaps.md:662-673` already files the compression gap for `otlp_in`; `otlp_out`'s half of
the same gap is currently unfiled.

### Related, already filed — don't duplicate here

The Tempo service-graph gap (needs `metrics_generator` + a remote-write target +
`serviceMap.datasourceUid`, and today's demo has no cross-service trace to graph regardless) is
already recorded in `docs/known-gaps.md` via PR #60. See that entry rather than restating it here.

## Verification

Documentation-only; verification here means consistency, not tests.

1. `docs/plans/README.md`'s new row matches this file's frontmatter dates and title exactly.
2. Spot-checked file:line references resolve: `logs.rs:99-125`, `logs.rs:121-122`,
   `otlp.rs:245-254`, `lib.rs:85-91`, `syslog.rs:99-104`, `syslog.rs:155,220`,
   `config.alloy:13-16,33-41`, `logit-internal.json:130,145,161`.
3. The Loki `index_label`-is-resource-only claim is checked against Grafana's own OTLP-ingestion
   docs (done above), not left as an inference from behavior.
4. `docs/known-gaps.md` bullets cross-reference this plan rather than restating its prose.
5. No code, config, or demo file touched — `git status` shows only `docs/` changes for this plan.
