# Enabling plan: a real nginx workload

`logit` is at v0.1 — one working path, `statsd_in → aggregate → lua → influxdb_out`. Every other
`ComponentKind` deserializes but is rejected at validation. This plan works backward from a real
workload — a low-volume nginx reverse proxy — to find out what has to change before someone can
plug `logit` into their environment. nginx is the forcing function; the changes it forces are the
deliverable.

**Scope.** This plan covers the work *inside this repo* that enables the integration: new
components, the data-model change they need, production packaging, and a working reference example
plus the docs to use it. It stops at "logit can do this, here is a proven example and how to point
your nginx at it." Deploying it against any particular server is out of scope and happens
elsewhere.

Each workstream below is independently reviewable (its own PR, `AGENTS.md`'s branch-and-PR
workflow); later ones depend on earlier ones per the graph below. Read the design docs a workstream
references before starting it — this plan sequences work, it doesn't re-derive design that belongs
in an ADR or a design doc.

## The target, generically

Properties that actually drive requirements here — deliberately not tied to any specific host:

- nginx reverse-proxying several small sites; low volume (a handful of requests/second at peak).
- Access logs already emitted as JSON via a custom `log_format` with `escape=json`. Error logs are
  nginx's own non-JSON format.
- Logs go to the container's stdout/stderr — the base `nginx` image's `/var/log/nginx/*.log`
  symlinks. `/var/log/nginx` is not a mounted volume, and there's no log rotation of its own.
- InfluxDB 2.x and Grafana already run as sibling containers on the same compose network, reachable
  by service name. Nothing collects logs or metrics today.
- Containers run with an unattended restart policy behind a public IP.
- The whole nginx config directory is a read-only bind mount, so config edits need no image rebuild.

## Decisions already settled

| Question | Decision |
|---|---|
| Transport | nginx's native `access_log syslog:server=…` → `syslog_in` (already a declared `ComponentKind`) |
| Log sink | New `stdio_out`; real log storage (e.g. a dedicated log backend) is deferred |
| Log → metric | New `kv_metrics` transform, reading attributes, configured as per-kind lists |
| Event model | One event can carry a log, metrics, *and* a span simultaneously; sinks self-select what they emit |
| Metric tags | Event attributes, pruned by a new `keep` transform before `aggregate` |
| Image delivery | A production Dockerfile lives in this repo; consumers build or pull it |

## Gaps this plan exists to schedule

| Gap | Consequence |
|---|---|
| No log-producing input | `json` (`crates/logit-transforms/src/json.rs`) has never run in a live pipeline |
| No log sink | `influxdb_out` silently skips `Payload::Log` (`crates/logit-outputs/src/influxdb.rs:108`) |
| Nothing derives metrics from logs | Lua has no event constructor (deliberate, `docs/design/lua-api.md`); `aggregate` only merges existing metrics |
| `Event.payload` is one-of | An access log line is *both* a log and a source of several metrics — unrepresentable today |
| No release Dockerfile | `Dockerfile.dev` says outright it is "not (yet) a production runtime image" |
| No SIGTERM handler | An unattended restart loses the in-flight aggregation window |
| No output retry | One InfluxDB 5xx ends `logit run`; an unattended restart policy turns that into a lossy crash loop |
| `eprintln!` diagnostics | Unattributed, unrated — a malformed-line source makes stderr unusable |

## Reference topology

What workstreams C–F build toward:

```
nginx  ──syslog/UDP──▶  nginx_in (syslog_in)
                            │
                            ▼
                        nginx_json (json)          ← merges the JSON body into attributes
                            │
                            ▼
                        nginx_metrics (kv_metrics) ← adds metrics to the same event
                            │
                ┌───────────┴───────────┐
                ▼                       ▼
           stdio_out              trimmed (keep)   ← drops high-cardinality attrs
                                        │
                                        ▼
                                  windowed (aggregate)
                                        │
                                        ▼
                                   influx_out
```

## Workstream dependency graph

```
A ──┬── C ──┐
    ├── D ──┼── F ── G
    └── E ──┘       │
B ──────────────────┘
```

A (event model) and B (packaging/lifecycle) are independent and can start in parallel. C, D, E each
depend only on A. F needs C, D, and E all landed. G (docs) needs B and F.

---

## A. Multi-payload event model

**Goal:** let one `Event` carry a log, metrics, and a span at once, so a native transform can add
metrics to an event that already has a log body, and every sink can emit whatever it finds.

**Depends on:** nothing — this is the foundation.

**Decisions to record:** new **ADR 0012** (multi-payload events); rewrite `docs/design/data-model.md`'s
"Top-level shape" and "Payload types" sections to match.

**The change.** `crates/logit-core/src/event.rs` — `Payload` goes away:

```rust
pub struct Event {
    pub timestamp: i64,
    pub attributes: AttrMap,
    pub log: Option<LogRecord>,
    pub metrics: SmallVec<[MetricRecord; 1]>,
    pub span: Option<SpanRecord>,
}
```

Named fields, not `SmallVec<[Payload; 1]>`: "does this event have a log?" becomes a typed, free
check — exactly what every sink needs — and two-logs-on-one-event is unrepresentable by
construction. `smallvec` is already a workspace dependency (`AttrMap` uses it).

**`Transform::process`'s `Option<Event>` signature does not need widening.** `kv_metrics` (E) adds
metrics to an event it already has — one in, one out. That falling out of the model cleanly is a
sign it's the right one, and is worth confirming in the ADR rather than assuming.

**Files:**

- `crates/logit-inputs/src/statsd.rs` — one metric per event, `log: None, span: None`.
- `crates/logit-transforms/src/json.rs` — `let Payload::Log(log) = …` becomes `let Some(log) = &event.log`. ADR 0010 gains a sentence: an event with no log passes through untouched, unchanged in effect from today.
- `crates/logit-transforms/src/aggregate.rs` — the real semantic change, amending **ADR 0008**: absorb every mergeable metric off the event; forward the remainder if a log or span is still on it; return `None` only when nothing is left. Today's pass-through for `Set`/`Histogram`/`Summary`/kind-conflict is preserved by leaving those metrics on the event rather than absorbing them.
- `crates/logit-outputs/src/influxdb.rs` — iterate `event.metrics` rather than matching one `Payload::Metric`. `allocate_timestamp`'s per-series 1ns nudge now also disambiguates several metrics sharing one event timestamp; it already handles that shape, but it earns a dedicated test.
- `crates/logit-script/src/proxy.rs` — `event.type` is ill-defined once an event is several things. Keep it, redefined as the first present of `"log"`/`"metric"`/`"span"` (`"empty"` if none), and add `event.has_log` / `event.has_metrics` / `event.has_span` plus matching `to_table()` keys. Document in `docs/design/lua-api.md`. No constructor API — still deliberately deferred there.
- `crates/logit-pipeline/src/runtime.rs` test helpers.

**Test list:** every existing test across the ~80 call sites above that constructs a `Payload`
needs a mechanical update — expect this to be the bulk of the workstream's effort, not a footnote.
New tests: an event with both a log and a metric surviving `json` then `aggregate` unchanged in its
log half; `aggregate` absorbing only the metrics off a mixed event and forwarding the rest;
`allocate_timestamp` disambiguating multiple metrics on one event; each new `event.has_*` proxy
accessor; `event.type`'s multi-payload precedence.

**Done when:** `script/cibuild` passes with `Payload` fully removed from the tree, and a Lua script
can read `event.has_log`/`has_metrics` and see both true on the same event.

## B. Production packaging and service lifecycle

**Goal:** make `logit run` survivable as an unattended, restart-on-failure service.

**Depends on:** nothing — independent of A, startable in parallel.

**Decisions to record:** amend `docs/known-gaps.md`'s "no graceful shutdown" and "no real diagnostics
facility" entries to reflect what lands here; note explicitly that output *buffering* (the `Buffer`
trait) is still out of scope even after retry/backoff lands.

**Files and changes:**

- **`Dockerfile`** (release, beside `Dockerfile.dev`): multi-stage — `rust:1-bookworm` builder with clang/cmake/pkg-config for the vendored LuaJIT build, then `debian:bookworm-slim` with `ca-certificates`. `reqwest` is already rustls-only, so no OpenSSL is needed at runtime. Non-root user, `ENTRYPOINT ["logit"]`.
- **SIGTERM/SIGINT handling** — `crates/logit-cli/src/main.rs` + `logit-pipeline::run`: on signal, stop the listeners so their inboxes close *normally*, triggering the existing per-node close-time flush (`crates/logit-pipeline/src/runtime.rs:176-185`). The drain logic already exists and is tested; the only missing piece is the handler that closes a channel normally instead of the process just dying.
- **Retry/backoff in `InfluxDbOutput::send`** (`crates/logit-outputs/src/influxdb.rs`): bounded exponential backoff on a connection error or 5xx response, still failing the process after a capped number of attempts. A 4xx stays a hard, immediate failure — it's a config error, not a transient one, and retrying it would just hide a real problem.
- **Diagnostics**: prefix every `eprintln!` (statsd, influxdb, aggregate, json, per-event script errors) with its component id; rate-limit `json`'s parse-failure reporting specifically. Scoped deliberately — a full `tracing` migration is separate work; the concrete hazard here is one malformed line per request filling the container's log ring unbounded.

**Test list:** a fake `Output` that fails N times then succeeds, confirming `InfluxDbOutput` retries and eventually delivers; a fake `Output` that always fails, confirming the process still exits after the cap; a signal-triggered shutdown test confirming an in-flight `aggregate` window flushes before exit (extending the existing `run_returns_once_the_only_input_finishes_instead_of_hanging` pattern in `runtime.rs`'s tests).

**Done when:** a running `logit` process, sent SIGTERM mid-window, flushes and exits; a transient
InfluxDB outage no longer ends the process.

## C. `syslog_in`

**Goal:** implement the already-declared `syslog_in` listener kind, since nginx's native syslog
writer is the transport decision.

**Depends on:** A (the `Event` it constructs needs `log: Option<LogRecord>`).

**Decisions to record:** none — this implements a declared kind, it doesn't decide anything new.
Add a `docs/known-gaps.md` entry for the one deliberate limitation below.

**Files:** `crates/logit-inputs/src/syslog.rs`, following `crates/logit-inputs/src/statsd.rs`'s
shape exactly — a pure `SyslogDecoder` (`logit_proto::Decoder`) split from a `SyslogInput`
(`logit_pipeline::Input`), so the parser is testable with no socket.

**Behavior:**

- **UDP only.** nginx's `syslog:` access-log writer is UDP-only, so TCP buys nothing for this
  integration. Record it as a follow-up in `docs/known-gaps.md` rather than half-building it now.
- RFC 3164 and RFC 5424 both, disambiguated per message by whether a version digit follows the
  priority tag.
- Emits `log: Some(LogRecord { message, severity, body_format: Raw })`, with `facility`,
  `severity`, `hostname`, `tag`/`appname`, `pid` set as event attributes. `Resource::default()`,
  same as `statsd_in` today.
- A malformed line is reported and skipped; the rest of the datagram still emits — matching
  `StatsdDecoder`'s existing behavior on a bad line within a multi-line UDP packet.

**Test list:** decode an RFC 3164 line, decode an RFC 5424 line, malformed-priority is a clear
skip-and-continue, facility/severity/hostname/tag land as attributes, a multi-line datagram with one
bad line still emits the good ones.

**Done when:** `SyslogIn` moves from `is_implemented`'s rejected set to its accepted one, with a
real listener behind it.

## D. `stdio_out`

**Goal:** a sink that makes the whole pipeline's output visible without standing up InfluxDB —
useful for this integration's dev loop and generally for anyone getting started with `logit`.

**Depends on:** A (needs to render `log`/`metrics`/`span` from the new `Event` shape).

**Files:** `crates/logit-outputs/src/stdio.rs` — writes to stdout (default), stderr, or a file path
(`target:`), rendering whatever the event carries (log body, each metric, span) together. Follow
`InfluxDbOutput`'s split: a pure encoder, unit-tested without touching a real file descriptor.

> **Superseded below:** the "one line of JSON per event" sketch in this section (and the matching
> line in the test list) predates workstream A landing and was revisited once `Event` actually
> carried `log`/`metrics`/`span` independently (ADR 0012). The settled shape — kept current here
> rather than only in the implementing PR — is a readable, human-facing text block per event
> (stdout/stderr/file target, structured around a `Format` enum so a future `format:` template
> string is a new variant, not a restructuring), not JSON: this is explicitly a debugging/dev-loop
> sink for a person reading a terminal, not a machine-parseable export format (`logit-outputs`
> already has one purpose-built machine format, InfluxDB line protocol, and NDJSON export is a
> reasonable future `Format` variant if a real need for one shows up — it doesn't need to be the
> *only* format `stdio_out` ever writes). See `crates/logit-outputs/src/stdio.rs`'s module doc
> comment and `docs/known-gaps.md` for the accepted consequences.

**Registration.** This is the first new component kind this plan adds, so the four-touchpoint
pattern is worth stating once here — workstream E's `kv_metrics`/`keep` follow the same list:

1. `crates/logit-config/src/lib.rs` — new `ComponentKind` variant (derives on the enum give it `Serialize`/`Deserialize`/`JsonSchema` for free).
2. `crates/logit-pipeline/src/graph.rs` — add it to `role()` (the match is exhaustive, so the compiler enforces this) and to `is_implemented()`.
3. `crates/logit-cli/src/pipeline.rs::build_spec` — the kind → implementation registry.
4. `script/schema`, then commit the regenerated `schema/logit.schema.json` (`script/cibuild` fails on drift).

**Test list:** encoding a log-only event, a metrics-only event, and a mixed event each produce the
expected readable block, including the batch's resource attributes and every part of a span (ids,
kind, status, duration, and any events/links it carries); an event with none of the three (should
not occur in practice, but the encoder shouldn't panic on it).

**Done when:** `type: stdio_out` is a valid, working sink in a config.

## E. `kv_metrics` and `keep`

**Goal:** turn attributes already present on an event (typically from `json`) into metrics on that
same event, and give operators a way to control which attributes ever reach a metrics sink as tags.

**Depends on:** A (adds to `event.metrics: SmallVec<[MetricRecord; 1]>`).

**Decisions to record:** new **ADR 0013** for `kv_metrics`'s semantics.

**`kv_metrics`** — `crates/logit-transforms/src/kv_metrics.rs`:

```yaml
nginx_metrics:
  type: kv_metrics
  sources: [nginx_json]
  counters:
    - name: nginx.requests            # no `field` -> +1 per event
    - name: nginx.bytes_sent
      field: body_bytes_sent
  distributions:
    - name: nginx.request_time
      field: request_time
      unit: s
  gauges: []
```

No `tags:` field on this component: metrics ride on the event, and sinks already read the event's
attributes, so tag selection is `keep`'s job, not something restated on every metrics producer.

Rules to pin down in the ADR and test:

- An entry with no `field` means "+1 per event" (counter) or "set to 1" (gauge); with a `field`, the
  named attribute's numeric value.
- A field that is **missing, non-numeric, or non-finite** skips *that metric for that event* — no
  metric emitted, no error, no dropped event. nginx's `$upstream_response_time` is `-` on a
  non-proxied request and a comma-separated list on a retry, so this is the common path this
  component will hit, not an edge case to handle grudgingly.
- Numeric coercion accepts `I64`/`U64`/`F64` and a `Str` that parses cleanly, so it works whether
  the source JSON quoted the value or not.
- Distributions produce a single-sample `DdSketch`, exactly as `crates/logit-inputs/src/statsd.rs`
  does for `ms`/`h`/`d` today.

**`keep`** — `crates/logit-transforms/src/keep.rs`: a new `Keep { fields: Vec<String> }` kind
retaining only the named attributes, dropping everything else. An allowlist rather than `remove`'s
denylist, deliberately: a new field appearing in a log format later must not be able to silently
become a new InfluxDB tag dimension.

While this file is open: widen the declared-but-unimplemented `Remove { field: String }` to
`Remove { fields: Vec<String> }` and implement it too — a natural sibling to `keep`, and cheap since
the same attribute-filtering machinery serves both.

**Placement note to carry into F's example and G's docs:** `keep` must sit *before* `aggregate` in
the graph, or `aggregate` keys a series per request on attributes like client address and user
agent, and both cardinality and per-window memory explode.

**Test list:** counter with no field increments by exactly one per event; counter/gauge/distribution
each reading a present numeric field; a missing field, a non-numeric string field, and a non-finite
value (`NaN`/`inf`) each skip only that metric without affecting the others or the event's log half;
a quoted-numeric JSON string field still coerces; `keep` drops everything not named, preserves order
of what remains, and is a no-op on an event with no attributes; `remove` with multiple fields.

**Done when:** a `json` → `kv_metrics` → `keep` → `aggregate` chain, fed a synthetic log-shaped
event, produces correctly-tagged counter/gauge/distribution metrics and nothing else.

## F. Reference example, end to end in the dev stack

**Goal:** prove A–E together against a real nginx, entirely inside this repo's own dev stack — no
external environment involved.

**Depends on:** C, D, E (all land into this example's config).

**Files:**

- `examples/nginx/` — a Dockerfile (`FROM nginx:1`) and an nginx.conf representative of the target
  properties above: a JSON `log_format` with `escape=json`, `access_log` directed to *both* stdout
  and `syslog:server=logit:5140,tag=nginx_access,nohostname` (two `access_log` directives are legal
  in nginx), and a couple of vhosts — one proxied, one static — so `upstream_response_time` is
  sometimes present and sometimes `-`.
- `compose.yaml` — add an `nginx` service on the existing `logit` network.
- `examples/nginx-to-influxdb.yaml` — the reference topology above, `token: !env INFLUXDB_TOKEN`,
  alongside today's `examples/statsd-to-influxdb.yaml`.
- `script/server examples/nginx-to-influxdb.yaml`, then drive it with a `curl` loop.

**First task, before anything else in this workstream:** measure whether nginx's syslog messages
truncate. nginx caps a syslog message at `NGX_SYSLOG_MAX_STR` (1024 bytes total, including the
`<PRI>` header, timestamp, and tag — `ngx_syslog.c`), and a JSON access log line with a long user
agent, a long URL, and a forwarded-for chain can approach that; a truncated line is invalid JSON and
`json` will reject it. Measure it here, where it's cheap, with representative request shapes.

If it truncates: the fix to document (in G) is a second, leaner `log_format` used only for the
syslog destination, carrying just what `kv_metrics` actually reads, while the full format stays on
stdout. `file_tail` is the other possible answer, but it means implementing rotation- and
checkpoint-aware tailing, so it's the larger fallback, not the first one to reach for.

**Test list (manual, this is an integration proof, not unit tests):** `curl` loop produces visible
`stdio_out` lines carrying both the log body and its derived metrics; Grafana query against the
InfluxDB bucket the example writes to shows correctly-tagged series; `logit graph
examples/nginx-to-influxdb.yaml | dot -Tsvg` renders the fan-out after `nginx_metrics` and shows
`keep` sitting between it and `aggregate`; an oversized request (long query string, long user agent)
settles the truncation question definitively.

**Done when:** the example runs via `script/server`, and the truncation question has a documented
answer either way.

## G. Operator-facing documentation

**Goal:** make the rest of this plan usable by someone who didn't build it.

**Depends on:** B, F.

**Files and changes:**

- A deployment section (in `README.md`, or a new `docs/deploying.md` if it outgrows one) covering:
  building or pulling the release image (B), mounting a config, supplying secrets via `!env`, what
  `logit validate` is for as a preflight before restarting a running service, and the signal/restart
  behavior B lands.
- The nginx-side recipe: which `access_log`/`error_log` directives to add, why to keep the existing
  stdout destination during cutover (a safety net, not a permanent duplicate), the syslog
  message-size limit and its symptom if hit, and the ordering rule — start `logit` and confirm it's
  listening before pointing nginx at it, so no line is ever sent into a closed UDP port.
- Update `README.md`'s status paragraph and `AGENTS.md`'s "Current state" section — both currently
  say `aggregate` and `json` are the only implemented transforms and statsd/InfluxDB the only
  implemented protocols; that becomes false partway through this plan.
- Amend `docs/known-gaps.md`: close what B closes, add the syslog-TCP gap from C, leave the
  output-buffering gap explicitly unchanged.
- Fold or discard `docs/components.md` (currently an untracked stub listing protocols "to be
  explored") — don't leave it silently contradicting this plan once `syslog_in` is real.

**Done when:** someone unfamiliar with this repo can follow the docs alone to point a real nginx at
a `logit` instance and see metrics land in Grafana.

---

## Verification, across the whole plan

- Per workstream: `script/cibuild` — the exact sequence CI runs, so a clean local run means a clean
  CI run.
- `stdio_out` (D) showing each access log line *with* its derived metrics (E) attached on the same
  event — A's model change proving itself in one screenful.
- Grafana query: `nginx.requests` tagged with host/status/method and nothing else. A client-address
  or user-agent tag showing up means `keep` is in the wrong place in the graph.
- `logit graph examples/nginx-to-influxdb.yaml | dot -Tsvg` — the fan-out after `nginx_metrics`
  visible, `keep` sitting between it and `aggregate`.
- An oversized request settling F's truncation question.
- SIGTERM mid-window (B) — the partial aggregation window flushes rather than being lost.
- Stopping InfluxDB, keeping traffic flowing, restarting it (B) — `logit` backs off and recovers
  rather than exiting.
