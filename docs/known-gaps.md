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
- **Output buffering: closed for the sink side, in-memory only.** `crates/logit-proto/src/buffer.rs`'s
  `Buffer`/`InMemoryBuffer` are implemented (`push`/`peek`/`commit`, `DropOldest`/`DropNewest`), and
  every sink now sits behind a bounded, byte-aware `SinkQueue`
  (`crates/logit-pipeline/src/queue.rs`) that keeps accepting while a delivery attempt is in
  flight or backing off, with retry (`RetryConfig`, up to 60s by default) and fault-classification-
  driven duplicate-safety (`Fault`/`DeliveryPosture`, `crates/logit-pipeline/src/output.rs`) moved
  behind that boundary ([ADR 0021](adr/0021-buffered-sink-delivery.md)). A persistent failure no
  longer ends `logit run` by default — it degrades to dropping the offending batch and continuing,
  exiting only after a sustained ~60s window of nothing but configuration-error (`Fault::Permanent`)
  failures. What's left, genuinely open:
  - **No durable (disk-backed) buffering** — both the sink's `SinkQueue` and (since
    [ADR 0022](adr/0022-decoupled-listener-io.md)) a UDP listener's `ReceiveQueue` are in-memory
    only; a process restart, SIGKILL, or a shutdown grace that expires mid-drain loses whatever
    either was holding. Plausibly config-optional even once it lands, since not every deployment
    needs cross-restart durability; blocked on the `rkyv`-vs-hand-rolled wire encoding decision
    ([wire-protocol.md](design/wire-protocol.md)), which this deliberately does not settle in
    passing.
  - **No end-to-end acknowledgement** — delivery is confirmed only as far as the immediate
    destination accepting the write; nothing tracks whether the data survives past that point. The
    receive-side loss this used to also name (a UDP listener losing datagrams before anything
    reaches a buffer) narrowed with ADR 0022: a listener now counts every datagram it drops itself
    (`logit.component.datagrams.dropped`); what remains uncounted is the kernel's own drop, before
    `logit` ever sees the datagram — see the new kernel-drop-visibility entry below.
  - **No out-of-order/credit-based acknowledgement** — `SinkQueue` is deliberately in-order and
    single-in-flight (one queue, one writer, `peek`-then-`commit`-the-head only). Several in-flight
    batches acknowledged out of order is real future scope for the native wire protocol's
    credit-based flow control, not built or designed yet.
- **No visibility into the kernel's own UDP receive-buffer drops.** A listener's `ReceiveQueue`
  ([ADR 0022](adr/0022-decoupled-listener-io.md), directly above) counts every datagram *it* drops,
  but a datagram the kernel discards before `recv_from` ever returns it is invisible to `logit`
  entirely. Linux exposes this per-socket in `/proc/net/udp[6]`'s `drops` column; sampling it (on a
  timer, keyed by the listener's own bound address) as `logit.input.kernel.drops` would close the
  last uncounted loss path, and is Linux-only with no new dependency. Worth noting almost nothing in
  the field does this in-process — syslog-ng, rsyslog, Telegraf, and gostatsd all tell operators to
  run `netstat -su`/`ss -u` themselves — so building it would put `logit` ahead of the field, not
  merely at parity.
- **A UDP listener reads one datagram per syscall.** `read_loop` (`logit-inputs::udp`,
  [ADR 0022](adr/0022-decoupled-listener-io.md)) calls `recv_from` once per datagram. `recvmmsg(2)`
  amortizes that across a batch — rsyslog's own high-throughput reference config sets `batchSize`
  to 128, gostatsd's `--receive-batch-size` defaults to 50 — and syscall overhead is the read half's
  dominant remaining cost now that a stalled downstream no longer stops it running. Not built:
  `tokio::net::UdpSocket` doesn't expose `recvmmsg`, so this needs raw-fd work via `try_io` plus a
  `libc` binding.
- **One reader per UDP listener.** A single `recv_from` loop is one core's worth of read capacity.
  `SO_REUSEPORT` lets multiple sockets share one port with the kernel load-balancing datagrams
  across them — gostatsd's `--max-readers` (default `min(8, NumCPU)`), rsyslog's per-listener
  thread count (capped at 32). Not built, partly because it interacts with the previous entry (a
  batched read raises the single-reader ceiling before more readers are worth adding) and partly
  because N readers each holding their own `Fanout` clone would need its own answer to the
  cancel-by-drop shutdown cascade ([ADR 0013](adr/0013-service-lifecycle-and-output-retry.md)) that
  today assumes exactly one `Fanout` per listener.
- **A `ReceiveQueue`'s depth/bytes/utilization gauges update on every datagram, not every batch.**
  `BoundedQueue::push`/`pop` (`crates/logit-pipeline/src/queue.rs`) call `update_gauges` — three
  `Telemetry::gauge` calls, each locking `ComponentBuffer`'s `Mutex<HashMap>`
  (`crates/logit-core/src/telemetry.rs`) — unconditionally on every accepted item. On a `SinkQueue`
  that's once per *batch*, an already-accepted cost; on a `ReceiveQueue` it's once per *datagram*,
  and the same listener's `read_loop` (pushing) and `decode_loop` (popping) run concurrently against
  the identical lock, so this is genuine cross-task contention on the receive side's two hottest
  loops, not just added per-call overhead. Deliberately not changed here: `BoundedQueue` is one
  implementation serving both queues by design ([ADR 0022](adr/0022-decoupled-listener-io.md),
  workstream A), and coalescing or sampling the receive side's gauge updates without also touching
  the sink side would split that implementation's behavior back apart along exactly the seam it was
  built to erase. If this ever shows up as a measured bottleneck (`script/bench`, the same evidence
  bar `docs/design/memory.md`'s "Costing internal spans" section sets for a similar hot-path
  tradeoff), the fix belongs in `BoundedQueue` itself — e.g. gauging on a sampled/coalesced cadence
  for every caller — not as a receive-only special case.
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
  `Diagnostics` now also mirrors every `warn_throttled` occurrence (not just the throttled subset
  that reaches stderr) into a `logit.component.diagnostics{key}` counter when telemetry is live
  ([internal-telemetry.md](design/internal-telemetry.md)) — a partial, additive answer to "where do
  these actually go," not a substitute for the `tracing` migration itself.
- **Closed for SIGTERM/SIGINT** ([ADR 0013](adr/0013-service-lifecycle-and-output-retry.md)) — a
  signal handler now closes every listener's inbox normally
  (`logit_pipeline::run_with_shutdown`, `crates/logit-pipeline/src/runtime.rs`), triggering the
  same close-time flush a listener's own natural completion always has. One residual gap left open
  by that fix, not an oversight (the other, `Output`'s missing close/flush hook, is now closed too
  — see below):
  - **A datagram in flight when the signal lands is lost.** Cancelling a listener's `run` future
    drops whatever it was mid-`recv_from`/decode on. Accepted: UDP is lossy by contract already;
    the aggregation window (which this fix does protect) is not.

  ~~`Output` still has no close/flush hook of its own.~~ **Closed**
  ([ADR 0021](adr/0021-buffered-sink-delivery.md)): `Output` gains `async fn flush(&mut self)`
  (default no-op, so no existing sink needed to change), called once `write_loop`
  (`crates/logit-pipeline/src/runtime.rs`) stops delivering — either because its queue drained to
  closed-and-empty, or because a bounded shutdown grace (default 5s) expired first with batches
  still undelivered. Now load-bearing rather than purely aspirational, since a sink can genuinely
  hold unwritten data at shutdown once buffering exists (the entry above).
- **Fan-out/fan-in is unbuffered/uncoordinated** — the component graph (ADR 0009,
  [pipeline-graph.md](design/pipeline-graph.md)) makes arbitrary fan-out/fan-in the normal case (a
  sink shared by two branches, one listener feeding several filters), but a stalled sink backs up
  every branch sharing an upstream with it, not just its own. A per-edge `on_full: block | drop`
  policy for the backpressure question is an open one, not yet designed — unaffected by the
  allocation work below, which is about the clone cost, not the backpressure semantics.

  ~~Each extra consumer of a node costs a full `EventBatch` clone.~~ **Closed, with a real residual
  gap.** `Arc<EventBatch>` copy-on-write landed (`docs/adr/0016-arc-eventbatch-copy-on-write.md`,
  three rounds, each correcting an overclaim the last one made — worth reading for that alone). A
  single-consumer edge (most edges in the shipped config) and an all-`Output` fan-out are both now
  unconditionally free or near-free (0 and 1 allocations). What's left, exactly as measured, not as
  originally hoped: a fan-out mixing one `Output` branch with one mutating branch costs 1 *or* 6
  allocations depending on real scheduling, never a fixed number; a fan-out with no `Output` branch
  at all still costs a full clone (6, one worse than the original code), with no path to
  improvement under the current design. See [memory.md](design/memory.md) §3 for the complete,
  shape-by-shape account — there is no single number for "what fan-out costs now."
- **A Lua component's `flush()` has no resource of its own at a timer tick** — unlike an `aggregate`
  component, which tracks its own per-resource windows, a Lua component's flushed events are
  stamped with whichever resource it most recently saw on a real batch
  (`crates/logit-pipeline/src/runtime.rs`, see [ADR 0008](adr/0008-aggregation-window-semantics.md)).
  Fine for every config today (one listener, one resource); would need a real answer once a
  component has more than one upstream resource.
- **A Lua component's `flush()` sees a stale trace context, for the same reason.** `trace.trace_id`/
  `trace.span_id` (`docs/design/lua-api.md`'s "Reading trace context") reflect whichever batch
  `process()` most recently saw, not any single batch a flush-driven emission could correctly
  attribute itself to — the same *n*-to-1 problem `Transform::flush`'s linking solves for native
  transforms (below), deliberately not solved the same way here: a Lua component has no
  accumulator `logit` can inspect, so there's no state to track contributing contexts *into*. A
  script that wants better than "stale" can read `trace.trace_id`/`trace.span_id` inside its own
  `process()` and do its own bookkeeping — the values are genuinely there to use, just not
  aggregated by `logit` on the script's behalf.
- ~~**A benchmark of the event proxy against plain table conversion is still outstanding**~~ —
  **closed.** Measured in `crates/logit-bench/benches/pipeline.rs` (`lua::proxy` vs
  `lua::to_table`): the proxy is faster, widening in its favour for scripts that read few
  attributes, since `to_table` converts everything regardless. The design commitment in
  [lua-api.md](design/lua-api.md) stands, now with a number behind it.

  What the same measurement turned up — the boundary costing 21 allocations per event (a `_G`
  lookup of `process` per event, a fresh `AttrsProxy` userdata per attribute access, a Rust
  `String` per metamethod key) — is **also now closed**: 21 → 9, via caching `process`/`flush` as
  an `mlua::RegistryKey` (resolved once at load, not looked up from `_G` per call), caching the
  `AttrsProxy` userdata per event instead of rebuilding it per access, and taking `mlua::String`
  instead of an owned `String` in both metamethods. Two edge cases the caching opened were closed
  rather than left as caveats: a script that stashes `event.attributes` past the point its event is
  returned now fails loudly in this crate's own voice (not mlua's raw error), and a `flush` global
  that exists but isn't a function is now a load-time error, matching `process`'s existing
  `MissingProcess` treatment, instead of silently behaving as "no `flush()`" forever. Both are
  documented in [lua-api.md](design/lua-api.md); see [memory.md](design/memory.md)'s recommendations
  for the full write-up.
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
  symbols are eternal. See [memory.md](design/memory.md)'s interner section.

  ~~If the `tracing` migration lands anyway, an `interner::len()` gauge is nearly free at that
  point and would make this observable rather than silent.~~ **Closed, ahead of that migration.**
  `internal`'s process-level gauges (`logit.process.interner.strings`,
  [internal-telemetry.md](design/internal-telemetry.md)) sample `interner::len()` on every drain
  tick — growth is now observable by attaching any sink to `internal`, no `tracing` migration
  required first.

  Separately and unrelated to growth: **`AttrMap::get` used to intern rather than probe**
  (`attrs.rs`) — fixed. All three production call sites were keyed by config strings or Lua
  literals (a bounded set), so this was a wasted hash plus concurrent-map probe on the hot path,
  not a leak, but it's gone now regardless: `get`/`remove` use `interner::lookup`, a non-interning
  probe, falling through to the existing search only on a hit.
- ~~**`statsd_in` copies tag values instead of slicing them**~~ — **closed.** It used to build
  attribute values with `attributes.insert(k, v)` on a `&str`, routing through
  `Value::str` → `Bytes::from(String)` (copying bytes already in the datagram buffer), then
  `build_event`'s `attributes.clone()` promoted each to a shared `Bytes`, copying a second time.
  Now uses the same pointer-arithmetic `slice_of` reconstruction `syslog.rs` already had: 8
  allocations per line down to 2, the same irreducible pair `syslog_in` pays (one `Vec<Event>` per
  line, one for the batch, split across two `Vec`s here due to statsd's multi-value grammar).
  `crates/logit-bench`'s `statsd_tag_values_share_the_datagram_allocation` (formerly
  `statsd_tag_values_are_copied_not_sliced`, inverted exactly as that test's own doc comment said
  it would be) asserts the zero-copy property structurally now. See [memory.md](design/memory.md).
- ~~**`influxdb_out`'s line encoder allocates ~180 times per event**~~ — **closed.** Was the largest
  single cost in the pipeline, roughly twice what ingesting an event cost end to end. Now 30
  allocations per 100-event batch (from 18,024) and 2.6× faster, by escaping and formatting
  straight into buffers reused on the encoder, merge-joining the resource and event attribute maps
  instead of cloning and re-inserting, borrowing the series key for its lookup and allocating it
  only on a miss, and reusing `allocate_timestamp`'s path-compression scratch. Output is
  byte-for-byte unchanged, which the existing format tests pin. What remains is per-batch rather
  than per-event; `crates/logit-bench`'s `influx_encode_100_events` guards that.

  **`stdio_out` got the same treatment shortly after** (`crates/logit-outputs/src/stdio.rs`):
  ~18 allocations per event down to ~1 (1801 → 101 per 100 events), via the identical merge-join
  and reused-buffer mechanism. It had briefly become the more wasteful of the two encoders once
  `influxdb_out` was fixed first; both are now in the same range. See
  [memory.md](design/memory.md)'s recommendations.
- **Channel depth is bounded in batches, not bytes or events**
  (`CHANNEL_CAPACITY`, `crates/logit-pipeline/src/runtime.rs`) — 64 batches per edge, with
  unbounded batch size. Narrowed by [ADR 0022](adr/0022-decoupled-listener-io.md) for a UDP
  listener's own outbound edge specifically: `BatchAccumulator` now merges many datagrams into one
  batch under an explicit, config-visible byte bound (`receive.batch_max_bytes`, default 1MiB), so
  what a `statsd_in`/`syslog_in` edge can hold is bounded by config, not just by datagram size. What
  remains open is every *other* edge — a transform's outbound batch size is still unbounded, so a
  65 KB syslog datagram parsed into hundreds of events and then re-batched by a downstream transform
  can still produce an oversized batch with nothing in the config saying so, and total in-flight
  memory still scales with edge count. Becomes real with a TCP or file-tail input feeding a
  transform directly, where nothing caps how many events one read produces.
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
- **A syslog event's `timestamp` is receipt time, not the sender's** — every event is stamped with
  the instant its datagram came off the socket (`received_at`, captured by the read half and
  threaded through to `Decoder::decode_into` explicitly since
  [ADR 0022](adr/0022-decoupled-listener-io.md) decoupled decode from the read loop — not a fresh
  clock read at decode time, which could otherwise run arbitrarily behind arrival under backlog)
  and preserves the sender's own timestamp separately, as the
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
- **Internal telemetry ([internal-telemetry.md](design/internal-telemetry.md),
  [ADR 0018](adr/0018-internal-telemetry-as-pipeline-events.md)) covers metrics only** — the
  framework (the `internal` component, the per-component buffer, the emit API) is built to extend,
  but three extensions are deliberately not part of this first cut:
  - **Internal spans — the propagation substrate is built; nothing emits a `SpanRecord` yet.**
    `internal`'s name (not `internal_metrics`) deliberately leaves room for this without a rename.
    History: `crates/logit-bench/tests/allocations.rs`/`benches/pipeline.rs`'s node-runtime
    coverage closed the gap where nothing measured what `run_transform`/`run_output`/`Fanout::send`
    themselves cost (including the *first* call after every `internal` drain,
    `ComponentBuffer::drain`'s `mem::take` re-populating the buffer rather than updating it — a
    real, recurring cost the first pass at this measurement missed); a throwaway `TraceContext`
    prototype on `Delivered` was built, measured against that coverage per
    [ADR 0017](adr/0017-minimize-allocations-over-event-size.md)'s gate, and reverted — zero
    allocation change, `size_of::<Delivered>()` 32 → 56, no attributable throughput regression.
    See `docs/design/memory.md`'s "Runtime" and "Costing internal spans" sections for that account.

    **On that evidence, [ADR 0020](adr/0020-trace-context-propagation-on-delivered.md) built it for
    real** — `Delivered` permanently carries a `TraceContext`, and the two node kinds with an
    unambiguous parent propagate a real one: `Transform::process`/`ScriptWorker::process` (the
    non-flush path, one incoming batch per emission) via `Fanout::send_with_context`, and
    `run_output` (already borrows the incoming `Delivered` without unwrapping, so nothing further
    to wire — see `docs/design/pipeline-graph.md`'s "Trace context propagation" section for the
    full per-node-kind table). Every listener and every flush-driven emission still mints a fresh
    root, unchanged from the prototype's behavior — not an oversight, see below.

    **Item 1, `Transform::flush`/Lua's `flush()`, is now built for both native transforms and
    Lua** — an *n*-to-1 relationship (however many batches were absorbed since the last tick), with
    no single correct parent. `Aggregator` (`crates/logit-transforms/src/aggregate.rs`) tracks a
    bounded, best-effort `ContributingContexts` set per series (`MAX_CONTRIBUTING_CONTEXTS_PER_SERIES`,
    8 — dropped and counted past the cap, `logit.transform.links.dropped{reason="cardinality"}`,
    the same shape `MAX_KEYS_PER_COMPONENT` below uses) and `Transform::flush`'s return type now
    pairs each emitted `Event` with the `SpanLink`s that set produced. Lua's `flush()` gets no
    equivalent — no accumulator `logit` can inspect — and is instead documented as an accepted
    stale-context limitation (above, next to the identical `Resource`-staleness gap), with
    `trace.trace_id`/`trace.span_id` (`docs/design/lua-api.md`) exposed to a script's own
    `process()` so it can do its own bookkeeping if it wants better than that. Picking an arbitrary
    contributing batch as "the" parent, for either case, was considered and rejected in ADR 0020
    (silently wrong is worse than visibly incomplete).

    **What's still open, in order:**
    1. **Nothing turns a (context, node, batch) tuple into a real `SpanRecord`-carrying `Event`
       yet.** `Transform::flush`'s `Vec<SpanLink>` is real and tested
       (`crates/logit-transforms/src/aggregate.rs`'s own test module), but nothing downstream reads
       it — `run_flush` (`crates/logit-pipeline/src/runtime.rs`) discards it, and propagation
       through `Delivered` is still unobservable outside `logit-pipeline` (`Output::send` takes
       `&EventBatch`, not `&Delivered`). This is the piece analogous to how `internal`'s
       `ComponentBuffer`/drain mechanism turns counters into events
       (`docs/design/internal-telemetry.md`); routing through the same `internal` component is the
       expected shape, per ADR 0018.
    2. **Sampling** — span volume would be a different shape than metric volume once emission
       exists (potentially one span per node-visit per batch); `internal` will likely need its own
       knob for this, separate from its drain `interval`.
  - **Internal logs** — routing `Diagnostics`' stderr output into the graph as `LogRecord` events
    is the natural next layer, and what the still-deferred `tracing` migration (above) should build
    on rather than duplicate.
  - **`host_metrics`** — facts about the machine itself (CPU, disks, NICs) are a different kind of
    source than `internal`: read from the OS rather than from `logit`'s own counters, need their
    own config, and can fail in ways an in-process atomic read never does. A separate component
    kind when it lands, not a field on `internal`.
- **A component's internal-telemetry buffer caps distinct `(name, tags)` keys at 1024**
  (`MAX_KEYS_PER_COMPONENT`, `crates/logit-core/src/telemetry.rs`) — bounds a component that
  ignores the tag-cardinality convention (`&'static str` values only) rather than letting it grow
  the process-wide interner unbounded. A dropped key is counted
  (`logit.internal.points.dropped{reason="cardinality"}`), never silent, but the cap itself is a
  fixed constant, not configurable — revisit if a legitimate component ever needs more than 1024
  distinct points between drains.
- **Lua-authored telemetry (`crates/logit-script/src/telemetry.rs`,
  [ADR 0019](adr/0019-lua-authored-telemetry-cardinality.md)) trades the type-system cardinality
  guarantee the rest of `internal-telemetry.md` relies on for a convention-enforced one** — a
  script's metric name/tag value is round-tripped through the process interner rather than
  required to be a Rust `&'static str`, so nothing stops a script from building one out of per-event
  data and leaking the interner one entry at a time. Accepted for the same reason the interner's
  own never-evicting design is accepted (above): bounded in the intended, documented use (a fixed
  literal in the script's own source), and the fix if it ever isn't (a bounded per-`ScriptWorker`
  cache instead of the process-wide interner) is recorded as a considered-and-deferred alternative
  in the ADR, not undesigned.
- **The internal-telemetry component survey (`docs/design/internal-telemetry.md`'s worked-examples
  list) found several more candidates not built yet** — real, but each needs more than a
  `telemetry.count(...)` call:
  - **Process-level facts beyond what `internal` already samples** — `logit.process.memory.*` via
    jemalloc heap stats needs a new `tikv-jemalloc-ctl` dependency, and cross-crate plumbing since
    `crates/logit-inputs` (where `internal` lives) doesn't depend on `crates/logit-cli` (where the
    `jemalloc` feature is, `docs/adr/0015-jemalloc-global-allocator.md`). `logit.process.threads`/
    `.fds`/`.cpu.seconds` would need Linux-specific `/proc` parsing. Candidate names:
    `logit.process.memory.allocated`/`.resident`, `.threads`, `.fds`, `.cpu.seconds`.
  - **`json`'s parse-outcome counts** — its two real failure modes (`no_brace`, `parse_failure`)
    already ride the `Diagnostics` bridge for free (`logit.component.diagnostics{key=...}`), so a
    dedicated metric would mostly restate what's already visible.
  - **`logit-proto`'s `frame.rs` metrics** — still a stub (see the entries above), nothing to
    instrument until an implementation exists. Candidate names, pre-committed so whoever builds it
    doesn't have to re-derive them: `logit.proto.frames{direction,codec,compression}`,
    `logit.proto.frame.bytes`, `logit.proto.errors{reason="magic"|"version"|"crc"|"truncated"}`.
    (`buffer.rs`'s own metrics are no longer on this list — implemented at the `SinkQueue` layer,
    `docs/adr/0021-buffered-sink-delivery.md`, as `logit.component.buffer.batches`/`.bytes`/
    `.utilization`/`.push.blocked.duration` and new `reason` values on `batches.dropped`/
    `events.dropped`, per `docs/design/internal-telemetry.md`'s catalog.)
  - **Lua per-call latency, error classification, flush-tick-empty tracking** — a per-event
    `ScriptWorker::process` timing distribution would isolate one pathological event from a big
    batch (today's `logit.component.process.duration` is whole-batch), but costs a clock read per
    event; `ScriptError`'s `MissingProcess`/`Lua(...)`/malformed-return cases collapse into one
    `errors{reason="process"}` today, when a script-bug class (a malformed `flush()` return) is a
    different signal than a runtime error. Each is real; none was a default yes.
- **Every `Output::send` call allocates a boxed future, on every batch, for every sink, unrelated
  to telemetry or anything else in this file's other entries.** `Output` is `#[async_trait]`
  (`crates/logit-pipeline/src/output.rs`); the macro desugars `async fn send` into a fn returning
  `Pin<Box<dyn Future<...>>>`, so calling it — through `&mut dyn Output`, the shape `run_output`
  actually has, or even on a concrete type directly — heap-allocates its future every time.
  Measured at 1 allocation (16 bytes) per call, confirmed identical whether the call goes through a
  trait object or not (`crates/logit-bench/tests/allocations.rs`'s
  `send_batch_through_a_noop_output_disabled_telemetry`, found while adding `run_output` allocation
  coverage for the internal-spans costing exercise above — a coincidental discovery, not something
  that exercise was looking for). `Input::run` and `Transform`'s Lua-adjacent paths don't have this
  problem the same way (`Input::run` is called once per process; `Transform`/`ScriptWorker` aren't
  `#[async_trait]` at all), so this is specific to the output side, on the hottest possible
  schedule (once per batch, every sink, every pipeline). Not fixed here — and **a hand-written
  method returning `Pin<Box<dyn Future<...>>>` would not fix it either**, an earlier version of
  this entry's own suggestion, corrected in review: that return type requires exactly the same
  heap allocation to construct, whether a macro or a person wrote the method, because the box
  *is* the mechanism a `dyn Trait` object uses to return a future of unknown, implementer-varying
  size — not an artifact of `async_trait`'s codegen specifically. A real fix means giving up `dyn
  Output` for this call: either enum dispatch over the small, closed set of concrete `Output`
  kinds this project ships (`StdioOutput`/`InfluxDbOutput`/...), matched rather than boxed, so
  each variant's `async fn` compiles to its own real, unboxed future; or making the runtime
  generic per node over a concrete `Output` type, which loses the config-driven dynamic
  construction (`Box<dyn Output + Send>` built from a running config, `crates/logit-cli/src/pipeline.rs`)
  the pipeline currently relies on. Real work either way, with no forcing function yet — this
  entry is that forcing function, for whenever the output path's allocation cost becomes worth
  chasing.
