---
created: 2026-09-02
updated: 2026-09-02
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

### A. Operator-declared resource attributes — the actual blocker

**Finding:** there is no mechanism anywhere in `logit` to attach static attributes to a batch's
resource. Not config (`crates/logit-config/src/lib.rs`, `schema/logit.schema.json` — grepped for
`attributes`/`labels`/`tags`/`resource` on every input variant, no hits); not any transform
(`keep`/`json`/`kv_metrics`/`aggregate` in `crates/logit-transforms/src/` only filter or derive,
never insert a constant); and Lua can mutate only *event* attributes
(`crates/logit-script/src/proxy.rs:194-238` `AttrsProxy`), never a resource.

This matters beyond the demo: every OTLP-native backend keys behavior off resource attributes.
Every future OTLP sink hits this same wall.

**Design sketch, for whoever picks this up:**
- Cheaper than it looks. `SyslogDecoder` already *holds* an `Arc<Resource>` field
  (`crates/logit-inputs/src/syslog.rs:155`, returned unchanged on every decode at `:220`); only
  `SyslogInput::new` (`:99-104`) hardcodes `Resource::default()`. The plumbing exists; the config
  surface doesn't.
- Touch points: a config field (e.g. `resource: BTreeMap<String, String>` on `syslog_in`),
  regenerated `schema/logit.schema.json` (`script/schema`; `script/cibuild` fails the build on
  drift), a builder on the input, threading through `crates/logit-cli/src/pipeline.rs`, tests, docs.
- **Needs an ADR.** Written down badly, this reads as a contradiction of the rule PR #60 just
  landed. Written down correctly: *`logit`'s code may not invent a resource identity for data it
  didn't produce; an operator configuring the pipeline may declare one.* An operator writing
  `resource: {service.name: demo-hello}` on a listener is declaring what that specific socket
  receives — the same category of knowledge as `syslog_out`'s existing `hostname`/`app_name` config
  fields, not a violation of "code doesn't guess." Suggested slug:
  `operator-declared-resource-attributes`.
- **Open questions to research before implementing:**
  - Which inputs get the field first. `syslog_in` is the immediate motivator; `otlp_in` arguably
    should never get one, since it already receives a real resource on the wire.
  - Value types: strings only (simplest, matches what backends actually key on), or the full
    `logit_core::Value` set (more expressive, more schema surface).
  - Per-input only, or also a config-root default inherited by every input that doesn't override it.
  - Interaction with `BatchAccumulator`'s flush-on-resource-change behavior
    (`crates/logit-pipeline/src/accumulator.rs:88-95`) — a per-input constant resource should be a
    non-issue (one static value, no mid-stream changes), but worth confirming before relying on it.

### B. A Loki-direct log leg, dropping `alloy`

Feasible, and blocked on A for label quality — not blocked technically.

**The correction that kills the obvious shortcut:** Loki's `otlp_config` allows the `index_label`
action *only* for resource attributes, never for log-record attributes (confirmed against Grafana's
own docs: "It additionally allows index_label action for Resource Attributes" — log/scope
attributes are limited to `structured_metadata` or `drop`). `syslog.hostname`/`syslog.tag` are
encoded as **log**-record attributes by `logit` (`crates/logit-proto/src/otlp/logs.rs:99-104` pushes
`event.attributes` onto `LogRecord.attributes`), not resource attributes. So no Loki-side
`otlp_config` change can promote them to index labels — full stop. This makes workstream A a hard
prerequisite for a demo that doesn't look broken, not an optional enhancement.

**Loki's live default index-label set**, captured verbatim from the running demo stack's `/config`
endpoint: `service.name`, `service.namespace`, `service.instance.id`, `deployment.environment`,
`deployment.environment.name`, `cloud.region`, `cloud.availability_zone`, and a run of `k8s.*` /
`container.name` keys. Everything else on the resource → structured metadata, never a label, unless
explicitly configured otherwise.

**Consequence:** without A, every OTLP log line lands in Loki as `{service_name="unknown_service"}`
with no `host`/`app`/`job`-equivalent label at all — a demo that looks broken. With A, setting
`service.name`/`service.namespace` in `syslog_in`'s new `resource:` config is enough on its own —
both are already in Loki's default index-label set, so **no `demo/loki/loki.yaml` change would be
needed**.

**Where today's labels actually come from — entirely Alloy, nothing else:**
`demo/alloy/config.alloy:13-16` sets static `job="demo"`, `protocol="udp"`; `:33-41` relabels
`__syslog_message_hostname`→`host` and `__syslog_message_app_name`→`app`. All three Loki dashboard
panels select on `{job="demo"}`
(`demo/grafana/dashboards/logit-internal.json:130,145,161`) and would need new LogQL once Alloy's
labels no longer exist.

**Removal surface**, if this is executed: `demo/compose.yaml:87-103` (the `alloy` service) plus the
`alloy_data` volume at `:229`; `demo/alloy/config.alloy` (whole file, plus the now-empty
`demo/alloy/` directory); `demo/logit.yaml:44-57` (`log_out`) and the topology comment at `:16`; and
doc references treating Alloy as live in `docs/plans/demo-stack.md`, `demo/README.md`,
`docs/adr/syslog-output.md`, `docs/adr/demo-stack-separate-from-dev-stack.md`,
`docs/plans/otlp-end-to-end.md:36,329-330`.

One operational detail worth remembering: add `loki: {condition: service_healthy}` to `logit`'s
`depends_on` in `demo/compose.yaml` when this lands. Loki already has a healthcheck (Alloy currently
depends on it); without it, `log_out` spends its first several seconds retrying into a cold Loki.

### C. What replacing Alloy costs: `syslog_out`'s only demo exercise

`syslog_out` is exercised end-to-end **only** by the Alloy leg — nothing else in the stack speaks
syslog, which is the entire reason Alloy is there (Loki has no syslog receiver; promtail is EOL).
Dropping Alloy drops that coverage. This is a decision to make, not one already made. Options:

- **Accept the loss, document it.** Unit/integration tests still cover `syslog_out`; only
  third-party-receiver interop goes undemoed.
- **Keep Alloy solely for `syslog_out`.** Defeats the point of this workstream.
- **Loop `syslog_out` back into a second `syslog_in`** on another port, feeding a `stdio_out`.
  Exercises the encode path against `logit`'s own decoder, but proves `logit` talks to itself — not
  that it interoperates with a real third-party syslog receiver, which was Alloy's actual
  evidentiary value.

Separately, confirm for whoever picks this up: removing Alloy does **not** invalidate
`docs/adr/syslog-output.md`'s decisions (RFC 6587 octet-counting auto-detect at `:65-68`,
`idle_timeout` handling at `:80-82`, `syslog.*` attribute round-tripping at `:105-107`). Alloy was
the *evidence* those choices interoperate with a real receiver, not the *reason* for them. Those
passages need rewording to cite Alloy as "one receiver, verified against v1.19.2" rather than "the
demo's receiver" — not rescinding.

### D. Log↔trace correlation has no native OTLP path yet

`crates/logit-proto/src/otlp/logs.rs:121-122` always emits empty `trace_id`/`span_id` on every
exported `LogRecord` — deliberate, per the doc comment at `:22-26`: `logit_core::LogRecord` has no
field to carry them. So no OTLP log sink, Loki included, can ever get native trace correlation from
`logit` today. The demo's existing correlation is a Grafana `derivedFields` regex over the log
*body* (`demo/grafana/provisioning/datasources/datasources.yaml:30-39`), which is
transport-independent and works identically whether the log leg goes through Alloy or straight OTLP
— unaffected by workstream B either way.

This connects to a previously-noted future direction: converting logs into span/trace events (e.g.
nginx as a trace root arriving only via logs). Whichever component does that conversion is also the
natural place to assign `service.name` for that data — same principle as workstream A, one level
up the stack. Once `LogRecord` carries real trace context, the Grafana-side upgrade is a second
derived field keyed on `trace_id` structured metadata instead of a body regex — strictly better,
and worth doing at the same time as whatever adds trace-context fields to `LogRecord`.

### E. `otlp_out` config gaps found along the way

Surfaced while checking Loki compatibility; none block the demo, all block a real deployment:

- No custom headers — no way to send `X-Scope-OrgID`, which rules out any multi-tenant Loki, Mimir,
  or Grafana Cloud target. (Landed: `headers:` on `otlp_out`, applied on both transports, validated
  against a reserved protocol-owned set — see
  `docs/plans/signal-filtering-and-otlp-out-config-gaps.md`'s workstream 2.)
- No compression support (`crates/logit-outputs/src/otlp.rs:479-482` — the frame's compressed flag
  is always `0`).
- No TLS configuration for gRPC — `reject_insecure_grpc_endpoint`
  (`crates/logit-outputs/src/otlp.rs:359-370`) hard-rejects an `https://` endpoint under
  `protocol: grpc` at construction time, rather than supporting it.
- No signal filter — a sink sends whatever signal types the events in its batch happen to carry;
  there's no way to say "logs only" at the sink. (Landed: `has_signal`/`keep_signals`/
  `drop_signals`, three insertable transform components rather than a sink field — see ADR
  `signal-filtering-components` and `docs/plans/signal-filtering-and-otlp-out-config-gaps.md`.)
- Hardcoded per-signal paths (fine for the standard OTLP layout, blocks any backend using a
  different mount point). (Landed: `paths:` on `otlp_out`, HTTP-only — see
  `docs/plans/signal-filtering-and-otlp-out-config-gaps.md`'s workstream 3.)
- `observed_time_unix_nano` always `0` on encode (`logs.rs:114`). (Landed: stamped with
  `logit_proto::now_nanos()` at encode — same workstream.)

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
