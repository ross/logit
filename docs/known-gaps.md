# Known gaps

Deliberate, already-identified gaps that don't block whatever they were found alongside, tracked
here so they don't only live in an untracked scratch file or someone's memory. Each one either has
a `todo!()`/doc-comment pointer at its actual location too, or is cheap enough to describe fully
here. Not a roadmap — see [OVERVIEW.md](OVERVIEW.md) for planned scope; this is specifically things
already built that have a known, accepted rough edge.

- **`HyperLogLog` is a stub** (`crates/logit-core/src/metric.rs`) — no methods, just a placeholder
  pending a real crate (`cardinality-estimator` is the candidate). Consequences: statsd's `s` (set)
  metric type is a clear decode error rather than silently losing data
  (`crates/logit-inputs/src/statsd.rs`); `logit-transforms::Aggregator` passes `MetricKind::Set`
  through unaggregated rather than fake-merging it
  ([ADR 0008](adr/0008-aggregation-window-semantics.md)); `logit-outputs::influxdb` errors on it
  rather than writing a wrong encoding.
- **Native wire protocol** (`crates/logit-proto/src/frame.rs`) — the frame header type exists; no
  actual encode/decode, no connection/handshake, no dictionary encoding. The `rkyv`-vs-hand-rolled
  encoding choice is an explicit open, benchmark-gated decision
  ([wire-protocol.md](design/wire-protocol.md)).
- **Output buffering** (`crates/logit-proto/src/buffer.rs`) — the `Buffer` trait exists (deliberately
  defined ahead of any implementation, since retrofitting it onto call sites that assumed an
  in-memory queue is the expensive direction); no in-memory implementation yet, no ack/retry hooks,
  no at-least-once delivery. Bounded retry now exists in `InfluxDbOutput::send`
  ([ADR 0013](adr/0013-service-lifecycle-and-output-retry.md)) — a single transient failure no
  longer ends `logit run` outright — but that's a narrower fix than this gap: it rides out a blip or
  one bad response within a tight (~5s) budget, not a real outage, and a persistent failure still
  ends the process today exactly as before. `Buffer` (this entry) is what closes the rest of the
  gap, still unimplemented.
- **Delivery I/O is not decoupled from event processing within a node** — each component is its own
  tokio task, but *within* one node, I/O and processing share a single sequential path: `run_output`
  (`crates/logit-pipeline/src/runtime.rs`) awaits `Output::send` inline in its drain loop, so a slow
  or retrying sink stops draining its own inbox for as long as `send` takes; `StatsdInput::run`
  (`crates/logit-inputs/src/statsd.rs`) likewise interleaves `recv_from`, decode, and `Fanout::send`
  in one loop, so downstream backpressure stops it reading its socket, and the kernel silently drops
  datagrams with no signal anywhere in `logit` that it happened. This is why `InfluxDbOutput`'s
  retry budget above is tight rather than generous ([ADR 0013](adr/0013-service-lifecycle-and-output-retry.md)):
  without decoupling, every second spent retrying is a second of dropped intake. Worth exploring: a
  bounded ring buffer (or similar) between a sink's drain loop and its actual delivery, so a sink
  keeps accepting while a write is in flight or backing off, with retry moving behind that boundary
  and an explicit overflow policy (`logit_proto::buffer::OverflowPolicy` already names
  `DropOldest`/`DropNewest`/`Block`). Related to, but distinct from, the output-buffering entry
  above: that one is about *what* to buffer and delivery guarantees; this one is about the threading
  shape that would make buffering actually useful.
- **Relative gauge adjustment (`+`/`-`) and sample-rate extrapolation for distributions** — the
  aggregator that would hold the needed state now exists (`crates/logit-transforms`), but the statsd
  decoder still has no representation for "this is a delta, not an absolute value" to hand it, and
  any leading sign is ambiguous with a plain negative number at the wire level regardless. Relative
  gauges are explicitly rejected with a clear decode error rather than silently miscoded
  (`crates/logit-inputs/src/statsd.rs`).
- **`eprintln!` instead of a real diagnostics facility** — every component's diagnostic now goes
  through `logit_core::diag::Diagnostics` ([ADR 0013](adr/0013-service-lifecycle-and-output-retry.md)),
  which closes the two concrete hazards this entry used to name: every message is prefixed with its
  component's id (statsd input, InfluxDB output and its encoder, the aggregator's kind-conflict
  reports, and the `json` transform's parse-failure reports all identify which running instance
  spoke), and a message that can fire once per event under normal operation (a malformed line, a
  parse failure) is throttled by occurrence count rather than printed unbounded. What's still
  missing is the real thing: severity levels, structured fields, filtering — a full `tracing`
  migration, deliberately kept as separate, later work rather than folded into this narrower fix.
- **Closed for SIGTERM/SIGINT** ([ADR 0013](adr/0013-service-lifecycle-and-output-retry.md)) — a
  signal handler now closes every listener's inbox normally
  (`logit_pipeline::run_with_shutdown`, `crates/logit-pipeline/src/runtime.rs`), triggering the
  same close-time flush a listener's own natural completion always has. Two residual gaps left
  open by that fix, not oversights:
  - **A datagram in flight when the signal lands is lost.** Cancelling a listener's `run` future
    drops whatever it was mid-`recv_from`/decode on. Accepted: UDP is lossy by contract already;
    the aggregation window (which this fix does protect) is not.
  - **`Output` still has no close/flush hook of its own** — only `Transform`/Lua nodes with a
    configured `flush_interval` get a close-time flush; a sink has nothing analogous to flush on
    shutdown, because none needs one yet (`InfluxDbOutput::send` writes synchronously, nothing
    buffered internally to lose).
- **Fan-out/fan-in is unbuffered/uncoordinated** — the component graph (ADR 0009,
  [pipeline-graph.md](design/pipeline-graph.md)) makes arbitrary fan-out/fan-in the normal case (a
  sink shared by two branches, one listener feeding several filters), but a stalled sink backs up
  every branch sharing an upstream with it, not just its own, and each extra consumer of a node
  costs a full `EventBatch` clone. `Arc<EventBatch>` with copy-on-write is the identified future
  fix; a per-edge `on_full: block | drop` policy for the backpressure question is an open one, not
  yet designed.

  Now measured ([memory.md](design/memory.md)): that clone is 4 allocations and a 792-byte memcpy
  per event per extra branch, 272 ns — about 10% of the 2.61 µs it takes to ingest a line, and
  roughly 1/16th of what encoding one event for InfluxDB costs. So the copy-on-write change is
  still worth making (it's strictly no worse anywhere, and frees every read-only sink branch
  entirely), but it is *not* the pipeline's main cost, which is what this entry previously implied.
  The encoder is.
- **A Lua component's `flush()` has no resource of its own at a timer tick** — unlike an `aggregate`
  component, which tracks its own per-resource windows, a Lua component's flushed events are
  stamped with whichever resource it most recently saw on a real batch
  (`crates/logit-pipeline/src/runtime.rs`, see [ADR 0008](adr/0008-aggregation-window-semantics.md)).
  Fine for every config today (one listener, one resource); would need a real answer once a
  component has more than one upstream resource.
- ~~**A benchmark of the event proxy against plain table conversion is still outstanding**~~ —
  **closed.** Measured in `crates/logit-bench/benches/pipeline.rs` (`lua::proxy` vs
  `lua::to_table`): 1.51 µs/event through the proxy against 2.63 µs for `to_table`, ~1.7× in the
  proxy's favour, widening for scripts that read few attributes. The design commitment in
  [lua-api.md](design/lua-api.md) stands, now with a number behind it. What the same measurement
  did turn up is that the boundary costs 21 allocations per event (a `_G` lookup of `process` per
  event, a fresh `AttrsProxy` userdata per attribute access, a Rust `String` per metamethod key) —
  recorded in [memory.md](design/memory.md)'s recommendations, not yet fixed.
- **The attribute/metric-name interner never frees** (`crates/logit-core/src/interner.rs`) —
  `lasso::ThreadedRodeo` has no eviction, so every distinct string ever interned is retained for the
  life of the process, at a measured ~94-124 bytes each.

  **Accepted, not planned work.** The bounds, measured: re-interning a string the table already
  holds allocates *nothing*, so a fixed schema reaches steady state and stays flat; and only keys
  and metric names are interned, never values — so the usual telemetry cardinality explosion (host,
  request id, user agent, path) never touches it. What's left is a metric name that is real and
  never repeats, which means a user who embedded an id in a metric name. `logit`'s listeners are
  private by deployment shape ([OVERVIEW.md](OVERVIEW.md)), so that namespace is user-controlled
  rather than attacker-controlled; the anti-pattern is well known; and `logit` isn't what breaks
  first. The metric store goes well before (a million distinct measurement names is a million-plus
  series, against 94 MB here), and even inside `logit`, `aggregate`'s window costs ~600 bytes per
  series *per window* against the interner's ~94 bytes once — ~6× harder, sooner, and already
  mitigated by putting `keep` in front of it.

  **The premise is the thing to re-check, not the conclusion:** if a listener ever stops being
  private — a public or multi-tenant ingest endpoint, a hosted aggregator — revisit this. The
  retrofit is expensive: `Symbol` is `Copy` and `resolve` *panics* on an unknown symbol, so
  `AttrMap`, `MetricRecord`, `SeriesKey`, the Lua proxy, and the planned wire dictionary all assume
  symbols are eternal. If the `tracing` migration lands anyway, an `interner::len()` gauge is nearly
  free at that point and would make this observable rather than silent. See
  [memory.md](design/memory.md)'s interner section.

  Separately and unrelated to growth: **`AttrMap::get` interns rather than probing** (`attrs.rs`).
  All three production call sites are keyed by config strings or Lua literals — a bounded set — so
  this is a wasted hash plus concurrent-map probe on the hot path, an efficiency cleanup rather than
  a leak. `lasso` has a non-interning `get()`.
- **`statsd_in` copies tag values instead of slicing them** (`crates/logit-inputs/src/statsd.rs`) —
  it builds attribute values with `attributes.insert(k, v)` on a `&str`, which routes through
  `Value::str` → `Bytes::from(String)`, copying bytes already present in the datagram buffer; the
  subsequent `attributes.clone()` in `build_event` then promotes each to a shared `Bytes`,
  allocating again. Two allocations per tag, where `syslog_in`'s `slice_of` achieves zero. Measured
  at 8 allocations per statsd line against 1 per syslog line
  ([memory.md](design/memory.md)); `crates/logit-bench`'s
  `statsd_tag_values_are_copied_not_sliced` pins the current behavior so it can't be assumed away.
  The fix is to give `statsd.rs` the same pointer-arithmetic slicing `syslog.rs` already has.
- **`influxdb_out`'s line encoder allocates ~180 times per event**
  (`crates/logit-outputs/src/influxdb.rs`) — the largest single cost in the pipeline, roughly twice
  what ingesting an event costs end to end. `escape_tag`/`escape_measurement` each build four
  intermediate `String`s via chained `.replace()` whether or not anything needs escaping;
  `metric_fields` returns a `Vec<(String, String)>` with a `format!` per name and a `to_string()`
  per value; the series map is keyed by a fresh `line.clone()` on every call rather than only on
  insert. All fixable within that one file (`Cow<str>` escaping, a reused line buffer, `raw_entry`)
  with no change to the event model. See [memory.md](design/memory.md).
- **Channel depth is bounded in batches, not bytes or events**
  (`CHANNEL_CAPACITY`, `crates/logit-pipeline/src/runtime.rs`) — 64 batches per edge, with
  unbounded batch size. A 65 KB syslog datagram can decode to hundreds of events, so one edge can
  hold tens of megabytes with nothing in the config saying so, and total in-flight memory scales
  with edge count. Bounded in practice today only because datagram size caps batch size; becomes
  real with a TCP or file-tail input, where nothing caps how many events one read produces.
- **`!env` is invisible to `schema/logit.schema.json`** ([ADR 0011](adr/0011-env-yaml-tag.md)) —
  resolution happens on the parsed YAML tree before serde ever sees it
  (`crates/logit-cli/src/config.rs`), so the schema describes the substituted shape, never the tag
  itself. A schema-aware YAML editor will flag a `!env`-tagged value it can't resolve against the
  schema.
- **Config deserialization errors lose line/column information** once `!env` is in the picture
  (`crates/logit-cli/src/config.rs`) — resolving the tag requires parsing to
  `serde_norway::Value` first and deserializing from that, and `serde_norway::from_value` carries
  no source location the way `serde_norway::from_str` does directly on the raw file. Partly offset
  by `!env`'s own errors naming a config path (`components.influx_out.token`) and by the note
  appended when a substitution's resolved type likely caused the failure.
- **`graph::is_implemented`'s error Debug-prints a whole `ComponentKind`**
  (`"kind {:?} is not implemented yet"`, `crates/logit-pipeline/src/graph.rs`) — harmless today,
  since no *unimplemented* kind carries a secret field, but with `!env` now used to inline secrets
  directly into fields (ADR 0011) rather than referencing them by name, this becomes a real leak
  the moment an unimplemented kind gains one. Fix before that happens: redact or field-list instead
  of a blanket `{:?}`.
- **`logit graph` can't render a config with any secret left unset** — every `!env` reference must
  resolve for all three commands (ADR 0011), including `graph`, even though it only ever reads a
  component's `sources`/`type` to render topology and style nodes by role. A lenient mode that
  substituted a placeholder for a missing variable was tried and reverted (ADR 0011's
  Alternatives) — visualizing a config's shape without its production secrets set needs a copy of
  the config with dummy values filled in, not a feature of `logit graph` itself.
- **syslog TCP and structured data** — `syslog_in` is UDP-only (nginx's `syslog:` writer is
  UDP-only, so TCP buys the driving integration nothing) and skips RFC 5424 STRUCTURED-DATA rather
  than merging it into attributes (no producer needs it yet, and a naming scheme for
  `[id@32473 k="v"]` invented without a consumer would be guesswork). Both are additive later.
- **A non-UTF-8 syslog MSG is a rejected line, not a `Value::Bytes` event** — RFC 5424's `MSG-ANY`
  permits arbitrary octets, and `logit-core::Value` already has a `Bytes` variant for exactly this.
  `syslog_in` isolates UTF-8 validation to one line at a time (so one bad line no longer takes its
  datagram siblings down with it — see the fixed panic/data-loss bugs this gap replaced), but a line
  whose header parses cleanly while its MSG bytes aren't valid UTF-8 still fails as a malformed
  line rather than being decoded with a `Value::Bytes` message. Doing better means parsing the
  ASCII header fields directly off the line's raw bytes instead of a validated `&str`, deferring
  UTF-8 validation to the MSG slice alone — a real change, not a one-line fix, and nginx's
  `escape=json` access-log writer never emits invalid UTF-8 in practice, so there's no production
  producer forcing the issue yet.
- **A syslog event's `timestamp` is receipt time, not the sender's** — `syslog_in` stamps every
  event with `now_nanos()` at decode and preserves the sender's own timestamp separately, as the
  `syslog.timestamp` attribute (a `Value::Timestamp` for RFC 5424's RFC 3339 form, a raw
  `Value::Str` for RFC 3164's). The two can diverge: by network and queueing delay always, and by an
  arbitrary amount when the sender's clock is skewed or when messages are replayed or forwarded
  through a relay. Everything downstream keyed on time — `aggregate`'s tumbling window, the point
  timestamp `influxdb_out` writes — uses `event.timestamp`, so today a delayed or replayed message
  lands in the window it *arrived* in, not the one it *happened* in.

  Deriving `event.timestamp` from the sender instead was considered and deliberately not done here:
  RFC 3164's timestamp carries no year and no timezone, so resolving it to an instant means guessing
  both, and doing it only for RFC 5424 would give two senders on one listener different timestamp
  semantics with nothing in the config saying so.

  Worth exploring: an **optional `syslog_timestamp` transform** — a component an operator adds to a
  flow explicitly, which replaces `event.timestamp` with a resolved `syslog.timestamp` and makes the
  guesswork configurable rather than implicit. The pieces it would need:

  - RFC 5424: parse the RFC 3339 timestamp directly; no inference needed.
  - RFC 3164: fill in the missing year and timezone. A reasonable default is "the year that puts the
    message closest to receipt time" (which handles a New Year's Eve rollover in both directions)
    plus an explicit `timezone:` field defaulting to UTC — never the host's local zone, which would
    make behavior depend on an environment variable.
  - A bounded **sanity window** (`max_skew:`, say): a resolved timestamp further from receipt time
    than the window is rejected and receipt time kept, with a throttled diagnostic. Without it, one
    sender with a badly wrong clock can write points years away and quietly poison a dashboard.
  - A skip rule matching every other transform here: no `syslog.timestamp`, or one that doesn't
    resolve, means the event passes through with `event.timestamp` untouched — never dropped.

  Being a separate, opt-in component (rather than a flag on `syslog_in`) is the point: it keeps the
  listener's contract simple and honest, and makes "we trust our senders' clocks" a visible line in
  the config graph rather than a default nobody remembers choosing.
- **`stdio_out` has no rotation, no reopen, and no user-controlled format** — a file target is
  opened once, in append mode, and held for the process's lifetime: an external log rotator that
  moves the file leaves `logit` writing to the unlinked inode until restart (there is no
  SIGHUP-reopen). The output format is fixed; a user-supplied `format:` template is designed for
  (the encoder is built around a `Format` enum) but not implemented. Both are acceptable for a
  debugging/dev-loop sink, which is what this is for.
- **A pathological Host header can truncate the syslog-bound JSON line -- but nginx's own header-size
  limit turns out to make that hard to actually trigger.** The example's lean `access_json_syslog`
  `log_format` (`examples/nginx/nginx.conf`) sizes its fixed fields well under nginx's syslog
  message cap, but `$host` itself is unbounded and attacker-controlled behind a public IP.
  Measuring this for real (workstream F, `docs/plans/0002-nginx-integration.md`) against nginx
  1.31.4 turned up a more reassuring result than expected: an oversized `Host` header never reaches
  nginx's syslog writer at all under default settings. `large_client_header_buffers` (4 8k by
  default) rejects any request whose request line plus headers exceed ~8180 bytes with a 400
  *before* nginx builds a log line for it -- every `Host` value nginx will actually log (measured up
  to 8180 bytes) produced a complete, untruncated syslog datagram and parsed cleanly, contradicting
  the assumption (based on older nginx source naming a 1024-byte `NGX_SYSLOG_MAX_STR`) that a
  several-KB `Host` header would truncate the line. Whatever nginx's current per-datagram syslog
  cap actually is, it sits above the request-header limit that already gates this specific vector.

  That doesn't mean the failure mode isn't real, just that this particular door is closed by
  nginx's own defaults, not by anything `logit` does. A syslog line *can* still end up truncated --
  a larger `large_client_header_buffers`, a different unbounded field, or a different syslog client
  entirely could all produce one -- so the pipeline's degradation was verified directly: a
  hand-crafted, deliberately truncated syslog datagram sent straight to `syslog_in` (bypassing
  nginx) confirmed the *documented* consequence exactly: `syslog_in` accepts the truncated-but-valid-
  UTF-8 datagram without error; `json` fails to parse the truncated body and reports a throttled
  `parse_failure` diagnostic (`crates/logit-transforms/src/json.rs`) while passing the event through
  with `attributes` unchanged (only `syslog.*` metadata survives); `nginx_metrics` derives nothing
  field-based for that event (only the fieldless `nginx.requests` counter, which always fires
  regardless of attributes, still increments); sibling requests before and after are unaffected. No
  nginx-side mitigation (e.g. capping `$host`'s logged length) is added here: the pipeline already
  degrades gracefully on a truncated line by whatever means it happens, and capping a field nginx
  itself allows up to 8KB would be solving a problem the design doesn't actually have.
