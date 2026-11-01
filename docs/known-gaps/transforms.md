# Known gaps: Transforms

Entry format and the other areas: [the known-gaps index](README.md).

## Transforms: predicates and sampling

- **Predicate-shaped work (throttling, event-level dedup, and anything needing a real operator:
  `contains`, cross-attribute comparison, boolean combinations) costs a Lua VM, an OS thread, and
  roughly 9× the per-event allocations of a native transform, because `logit` has no native
  predicate language.** The rest of predicate-shaped work has native answers:
  - Equality and key-presence filtering, such as splitting a `logit_in` fan-out back apart by an
    attribute an upstream `set` stamped, or keeping events that carry a key: `has_attributes`/
    `drop_attributes`
    ([ADR `attribute-filtering-components`](../adr/attribute-filtering-components.md)).
  - Numeric ranges, such as splitting out slow requests: `bucket` writes a range's class into an
    attribute, and an equality component gates on it
    ([ADR `bucket-transform`](../adr/bucket-transform.md)).
  - Destination selection (splitting one flow into named streams): `route` (equality on
    provenance/attribute/resource, to one target or a list), `route_signals` (by the signals an
    event carries), and `target` components (ADR
    [`target-components`](../adr/target-components.md)); `lua` can also route with `event:to`.
  - Sampling: `sample`
    ([ADR `consistent-sampling-component`](../adr/consistent-sampling-component.md)), because a
    `lua` component can't express keyed, cross-process-consistent sampling.
  - Collapsing one series' metric readings at one instant before a sink that can't hold both:
    `dedup_timestamps`
    ([ADR `dedup-timestamps-transform`](../adr/dedup-timestamps-transform.md)), because a `lua`
    component can't keep the later reading of a batch without adding latency. Suppressing a
    repeated event stays `lua` work.

  **Consequence:** [ADR `routing-by-condition-is-lua`](../adr/routing-by-condition-is-lua.md)
  measured **9** allocations / **1.61 µs** per event through a `lua` component versus **1**
  allocation / **525 ns** through a native `Transform` (`docs/design/memory.md` has current
  timings). Each `lua` node also takes one dedicated OS thread and one LuaJIT VM
  (`crates/logit-pipeline/src/runtime.rs`'s `run_with_telemetry`), where a native transform is an
  ordinary tokio task. That's noise below roughly tens of thousands of events/sec
  (sidecar/host-agent volume). At central-collector volume it's real: a multi-branch routing
  diamond can cost a measurable fraction of a core for what a native transform answers in a third
  of that.

  **Revisit trigger:** sustained, *measured* central-collector throughput pressure against a real
  config. The ADR records a substantially designed native predicate grammar
  (total-by-construction, so it can't fail at runtime) as where to resume.
- **`sample` has no OTEP 235 / W3C `tracestate` `th:` interop.** An OTel SDK or collector
  sampling by threshold compares the trace id's *low 56 bits* against a `th:` value it also writes
  into `tracestate`; `logit` hashes the id's hex text with XXH64 and compares the hash's *top 53
  bits* ([ADR `consistent-sampling-component`](../adr/consistent-sampling-component.md)).
  - **Consequence:** `logit`'s `sample` and an upstream OTel sampler at the same rate keep
    *different* traces. Because `logit` neither reads nor writes `th:`, downstream can't recover
    the sampling probability from a `logit`-sampled trace.
  - **To close:** adopting OTEP 235 means a second bit convention for `key: trace_id` only (no
    other key has a `tracestate`) and, because the hash is a frozen cross-version contract, its
    own ADR.
- **`remove` matches whole attribute names only, with no prefix form.** It can't drop every
  attribute under `foo.`, such as the flat keys a `flatten` produced or a `json` parse wrote, so
  each name has to be listed, and a key that appears later isn't covered. `keep` is an allowlist of
  whole names too, so it has no prefix form either.
  - **Workaround:** list the names you know in `remove`; or use `keep` to name the attributes you
    want and drop the rest; or loop over `event.attributes` with `pairs` in a `lua` component and
    assign `nil` to each key with the prefix, at the cost the first entry above describes.
  - **To close:** a prefix form on `remove`, if a measured pipeline needs one.

## `shape` observer

- **`shape`'s cumulative gauges are since process start, not windowed.**
  `logit.shape.distinct_keys`, `.distinct_keysets`, `.keyset_share.top1`/`.top5`, and
  `.tracking_overflow` (`crates/logit-transforms/src/shape.rs`,
  [ADR `shape-observer-component`](../adr/shape-observer-component.md)) accumulate from startup,
  are re-reported unchanged on every flush, and never reset. That suits a survey capture (a whole
  key-set population, not the last ten seconds).
  - **Consequence:** a long-lived tap's distinct-key count only rises, so it can't show that a
    producer *stopped* emitting a key, and `tracking_overflow` latches at `1` for the life of the
    process once either cap is hit.
  - **Workaround:** restart the process; that's the only reset.
  - **To close:** a windowed variant (a second set of gauges reset per flush, or a decaying
    table).
- **Nothing bounds a single `shape` measurement event.** `logit.shape.key_bytes` and
  `.value_bytes` carry one value per top-level key and per string leaf, so an event with ten
  thousand attributes yields a ten-thousand-value `Samples`, bounded only by whatever bounded the
  source event. Every *table* in the component is capped and counted (`max_tracked_keys`,
  `max_tracked_keysets`, the per-window batch cap). The per-event vectors are exempt because
  truncating a width measurement at the widths worth knowing about defeats the instrument.
  - **To close,** if a tap meets a pathological producer: a per-event value cap with a drop
    counter.
- **`shape` measures `Resource`/`Scope` width as a count only.**
  `logit.shape.batch.resource_attributes` and `.scope_attributes` are attribute counts per batch,
  with no resource-side equivalent of `key_bytes`/`value_bytes`/`nested_maps`.
  - **Consequence:** the per-batch cost `docs/design/memory.md` cares about is only half visible:
    a 20-attribute resource of short enums reads the same as one of long ARNs and a nested label
    map.

## `aggregate`

- **A `keep` ahead of `aggregate` turns Datadog APM stats into ordinary metrics.** `aggregate`
  passes a stats group through whole (`passed_through{reason="bucketed"}`), because `datadog_in`
  and `datadog_trace_in` flag it `_logit.metrics.closed`. `keep` drops `_logit.*` attributes unless
  `private: keep` is set, so the flag goes, the groups merge across buckets, and no stats route
  sends them.
  - **Workaround:** set `private: keep` on that `keep`.
- **`max_series` admits series first come, first served within a window.** Once every series the
  cap holds was updated this window, each new series is refused until the next flush frees slots.
  Under churn that fills the cap early in each window, a stable delta series whose first record
  arrives late is dropped every window, while one-off series that arrived first take its slot.
  - **Signal:** `logit.transform.metrics.dropped{reason="series_limit"}` counts every refused
    record, with a throttled `series_limit` diagnostic naming the metric.
  - **Workaround:** bound cardinality upstream so the cap isn't reached. A `keep` ahead of
    `aggregate` limits which attributes reach it, and `keep_values` limits the values one
    attribute can take. `logit.transform.series.active` shows the peak each window.
  - **`max_series_per_metric` has the same shape per name:** once a name holds its limit's
    series, a series of it whose first record arrives later folds into the overflow series
    every window, while the series that arrived first keep their slots.
    `logit.transform.metrics.overflowed` counts the folds, and the same workaround applies.
- **A lookup that misses `find_group`'s memo walks every `(resource, scope)` group.** `max_series`
  bounds the group count at twice its value: every group but one emptied by an eviction holds a
  series, and an emptied group stays until the flush, so a window holds at most `max_series` groups
  plus one per eviction. The memo answers every event of a batch after the first, and the walk
  compares one stored 64-bit hash per group, about half a nanosecond each, before any full compare.
  `logit.transform.resource.groups` shows the count.
  - **Consequence:** absorb cost still grows with the group count once it reaches tens of
    thousands and batches are small. `docs/design/performance.md` §1's `aggregate-groups` row
    measures the 1000-group case on the perf VM (0.290 CPU µs/event), and
    [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s "The groups
    bound" section has the mechanism and the measurements.
  - **To close,** if that shape shows up in a profile: a map from hash to group index.
- **A folded record costs more than an absorbed one.** Each record of a series past
  `max_series_per_metric` misses its own key, then builds the overflow key and looks that up, on
  every arrival. On a laptop, with half of the `aggregate` scenario's series folding, that's about
  0.17 µs per folded record (0.297 against 0.211 CPU µs/event). The cost lasts only while a name
  is over its limit.
  - **To close,** if it shows up in a profile: memoize the overflow key per name and group.
- **A `GaugeDelta` whose series holds another kind is forwarded unresolved, and can reach a
  sink.** `aggregate` forwards any kind conflict untouched, counted
  `logit.transform.metrics.passed_through{reason="kind_conflict"}`.
  - **Consequence:** a `GaugeDelta` sharing a name, unit, and attribute set with a counter series
    reaches a sink as a delta no sink can encode, and every sink skips and counts it (the
    `GaugeDelta` rows in [Cross-protocol mappings](mappings.md)).
  - **Workaround:** give the gauge its own name or tags upstream.
- **A clamped sample rate held raw under `distributions: samples` is reported only if the series
  falls back to a sketch.** `logit.transform.samples.weight_clamped` counts a record when its
  weight is applied. A raw series that stays under `max_samples_per_series` with one rate is
  emitted as `Samples`, and the encoder that later sketches it clamps the weight without a report.
  - **Workaround:** to see every clamp, run `distributions: sketch`, the default, where each
    record's weight is applied on absorb.
  - **Revisit trigger:** an encoder gains its own `sample_rate_clamped` report.
- **`series_retention` counts flushes, not wall time.** The runtime coalesces missed flush ticks,
  so a stalled or overloaded stage that flushes late stretches retention in wall time: a series
  idle across one late flush loses one flush of retention, however long the gap. The same holds
  for a cumulative series' lifetime between restarts.
## `cumulative_to_delta`

- **A collectd COUNTER that wraps reads as a reset, and the increase up to the wrap is lost.**
  collectd reads a COUNTER that went down as a wrap at 2^32 or 2^64 and adds that width to the
  rate. `cumulative_to_delta` can't tell a wrap from a restart, so it reads every decrease of a
  monotonic `Sum` as a reset and emits the new value: the distance from the previous value to
  the wrap point never reaches the destination. `non_monotonic: reset_on_decrease` does the same
  to a DERIVE, which collectd never treats as a wrap.
  - **Consequence:** one undercount per wrap, of less than one wrap width. A 64-bit counter
    wraps rarely; a 32-bit one, such as an old SNMP interface counter, can wrap in minutes on a
    busy link.

## HTTP access logs: nginx, HAProxy, and `http_access`

- **`http_access` has no per-server presets.** It never learns a server's native variable names:
  the operator maps them onto the canonical names in the server's own log-format language, using
  `docs/http-access-logs.md` as the reference
  ([ADR `http-access-normalization`](../adr/http-access-normalization.md) rejected `preset: nginx`).
  - **Consequence:** Caddy and Traefik are the two servers whose JSON key names can't be chosen at
    all. For them the doc's recipe is a `lua` rename stage (Caddy with a `flatten` ahead of it),
    which costs a Lua VM per worker and a few allocations per event, because `logit` has no native
    rename component (`set` only stamps constants).
  - **Revisit trigger:** either server turns out to matter; then build a native rename, or a
    preset.
- **`http_access`'s route rules are regex-only, and matched O(rules) per event with no
  prefilter.** Each `routes:` entry is a regex (or a built-in set, itself one regex) tried in list
  order until one matches: no path-template syntax (`/users/:id`), no prefix trie, no `RegexSet`
  prefilter.
  - **Consequence:** for a real deployment's handful of rules this is noise next to `json`'s own
    parse. For dozens of rules on a busy tier it's linear in the rule count on every unmatched
    path, which is the scanner traffic that falls through to `route_other`. No `script/perf`
    scenario yet finds where it starts to matter.
- **`http_access`'s user-agent table is a heuristic bucket classifier, not a parser.** It writes
  one of `scanner`/`tool`/`crawler`/`browser`/`other`/`none` (or a configured class), never
  `user_agent.name`/`user_agent.version`. `user_agent_rules:` (tried first) or an upstream
  `user_agent.class` (trusted as-is) can pre-empt the built-in table, but it can't be turned off.
  - **Consequence:** it sees only what the client sent. nikto 2.6.1+ defaults to a browser UA from
    its own bundled list, nuclei randomizes a real browser UA for ordinary HTTP requests, and
    Nessus uses a real Chrome UA matching the scan host's platform. Their unconfigured traffic
    classifies `browser`, not `scanner`, however the table is tuned; the `scanner` row catches
    only a scanner that identifies itself.
- **`http_access` never percent-decodes a path.** `http.route` is matched against the capped
  `url.path` as it arrived, so a rule matches the encoded form only.
  - **Consequence:** `/api/x` and `/%61pi/x` are two different paths, and the second may land in
    `route_other`.
  - **Why not decode:** decoding safely (overlong sequences, encoded `/`, invalid UTF-8 after
    decoding) is real code, and would also change the recommended `url.original` contract of "the
    raw request target".
- **A non-UTF-8 value reaching `http_access` is capped in bytes and never classified.** A field
  arriving as `Value::Bytes` (from any stage that keeps raw bytes, including a `lua` stage writing
  a non-UTF-8 string) has no text to run a regex over. It's still capped (by byte length, not
  characters) and control-byte cleaned, but a bytes `user_agent.original` classifies `other` and a
  bytes `url.path` gets `route_other` (or no route). Like any other field it's best-effort,
  visible, and counted; it's never matched.
- **The built-in `crawler` pattern trades `\bbot\b`'s precision for bare `bot/`'s recall.**
  `\bbot\b` alone misses every crawler whose token runs the word into `Bot/` (`DotBot/1.2`,
  `Discordbot/2.0`, `YandexMobileBot/3.0`), which is what most of the unlisted long tail looks
  like. So bare `bot/` sits beside it (`\bbot/` would add nothing, because a `/` is always a word
  boundary).
  - **Consequence:** anything with `bot/` mid-word classifies as a crawler. `UptimeRobot/2.0` was
    the corpus's one false positive, and `tool` catches it first. A non-crawler product whose name
    ends in `bot` is called a crawler.
  - **Workaround:** a `user_agent_rules:` entry pre-empts it.
- **A pathological Host header can truncate the syslog-bound JSON line, but nginx's own
  header-size limit makes that hard to trigger.** `$host` is unbounded and attacker-controlled
  behind a public IP, while the lean `access_semconv` format (`fixtures/nginx/nginx.conf`) sizes
  its fixed fields well under nginx's syslog message cap.

  Measured against nginx 1.31.4: under default settings an
  oversized `Host` never reaches nginx's syslog writer. `large_client_header_buffers` (4 8k by
  default) rejects any request whose request line plus headers exceed ~8180 bytes with a 400
  *before* nginx builds a log line, and every `Host` value nginx will log (measured up to 8180
  bytes) produced a complete, untruncated datagram that parsed cleanly. Older nginx source named a
  1024-byte `NGX_SYSLOG_MAX_STR`; whatever the current per-datagram cap is, it sits above the
  request-header limit that gates this vector.

  nginx's defaults close this door, not `logit`. A line can still arrive truncated (a larger
  `large_client_header_buffers`, a different unbounded field, a different syslog client). A
  hand-crafted truncated datagram sent straight to `syslog_in` verified what happens then:
  - `syslog_in` accepts it.
  - `json` fails to parse the body, reports a throttled `parse_failure` diagnostic
    (`crates/logit-transforms/src/json.rs`), and passes the event through with `attributes`
    unchanged (only `syslog.*` metadata survives).
  - `nginx_metrics` derives nothing field-based (only the fieldless `http.server.requests` counter
    increments).
  - Sibling requests are unaffected.

  `http_access`'s 253-character cap on `server.address` doesn't help here, because it runs after
  `json` parses the line. There's no nginx-side mitigation (such as capping `$host`'s logged
  length): the pipeline degrades gracefully, and capping a field nginx allows up to 8KB solves a
  problem the design doesn't have.
- **HAProxy's native CBOR log output (`%{+cbor}o` / `%{+cbor,+bin}o`) is a closed door, not a "not
  now"; don't reopen it without new evidence.** It would cost a second decoder to build and
  maintain, for a ~15% wire saving and no parse-time win. Measured
  against real HAProxy 3.0.27 output with a throwaway decoder:
  - **Binary CBOR is reachable on both transports, once the flags are spelled right.** HAProxy's
    log-format options are comma-separated: `%{+cbor,+bin}o`. `%{+cbor+bin}o` applies only the
    last flag (`bin` alone, plain unencoded output) and `%{+bin+cbor}o` only `cbor` (the hex form):
    `parse_logformat_node_args` in HAProxy's `src/log.c` resets its start pointer at every `+`, and
    the manual never says so. With the comma form, HAProxy emitted raw binary CBOR in the syslog
    MSG both to a plain UDP `log` target and to a `ring` with `server ... log-proto octet-count`.
    Over TCP octet-counting that MSG reaches `logit` intact, because `syslog_in` decodes a
    non-UTF-8 MSG as a `Value::Bytes`; over UDP it needs `framing: datagram` (one message per
    datagram), which `syslog_in` has ([syslog known gaps](syslog.md)'s binary-MSG entry).
  - **Wire shape**, should anything decode it: an indefinite-length map (`BF … FF`) with definite
    text keys; *untyped* string items such as `%HM` come out as indefinite-length *chunked* text
    strings (`7F 63 'GET' FF`), `:str`-typed ones as definite strings; `:sint` non-negatives are
    major type 0; `:bool` is simple true/false; no tags.
  - **Size: 13–19% smaller than JSON, not more.** A 20-item HAProxy access line, same items from
    one HAProxy run: 588 bytes as `%{+json}o` (HAProxy pads after `:` and `,`), 549 bytes as
    compact hand-written JSON, 476 bytes as `%{+cbor,+bin}o`. The hex form is 2× the binary, so
    larger than JSON.
  - **Parse speed: no faster.** A hand-rolled CBOR-to-attributes prototype (a twin of `json`:
    zero-copy `Bytes` slices for definite strings, the same `KeyCache`, indefinite maps/strings, an
    explicit depth bound) benched against `JsonParser::process` on those two payloads, pinned to
    one core, three runs: JSON 881–921 ns/event, CBOR 1030–1049 ns/event, 1.1–1.2× *slower*.
    Allocations were equal (one: the `AttrMap` spilling past its 8-entry inline capacity at 20
    attributes) once the chunked `%HM` string was typed `:str`; as HAProxy emits it, CBOR costs two
    more for the chunk concatenation. What both formats share dominates the per-entry budget
    (key-cache lookup, `Value` construction, sorted `insert_sym`, UTF-8 validation, refcount
    bumps). The prototype isn't in tree.
  - **Key names:** HAProxy rejects a literal `.` in a custom item name under any encoding, so a
    CBOR-sourced tier would log dashed names, which `http_access`'s dashed-alias table already
    renames (as it does for the demo's `%{+json}o`).

  A `cbor_in`/`cbor_out` listener/sink pair is rejected outright: nothing in the telemetry
  landscape speaks CBOR over a socket (Fluent forward is msgpack, Vector's native wire is protobuf,
  syslog is text), so it would be a second native wire beside `logit_in`/`logit_out` with no
  producer or consumer.

  If new evidence reopens this, a decoder must handle:
  - `Value::as_str` **panics** on an invalid-UTF-8 `Value::Str` (`crates/logit-core/src/value.rs`),
    so CBOR's only-nominally-UTF-8 text strings need validation first.
  - A hand-rolled reader needs an explicit recursion bound, which `json`'s `serde_json`-based one
    inherits for free.
  - A length header must never size an allocation directly.
  - Tag 1 (epoch time), decodable straight into `Value::Timestamp`, is the one thing the format
    offers that JSON doesn't. HAProxy doesn't emit it.
- **`json-parse-x3` and `logfmt-parse` move 5–9% with code layout, with no source-level
  handle.** At #457 (`d47a99b6`) `json-parse-x3` rose 2.264 → 2.444 µs/event on the perf VM and
  `logfmt-parse` fell 7.9%. A disassembly finds `JsonParser::process`, `parse_object`, the
  `deserialize_map` visitor, and `insert_sym` identical instruction for instruction across that
  merge; only code addresses moved. At #516 (`cd734386`), which changes nothing on the parse path,
  `json-parse-x3` fell back to 2.247, unattributed, and `main` reads 2.276; `logfmt-parse` didn't
  move back (0.930 → 0.953).
  - Aligning `insert_sym`'s loop to a 64-byte line (`-C llvm-args=-align-loops=64`, +4.6%
    `.text`) didn't recover `json-parse-x3` at #457.
  - Also open: `json-parse`'s unattributed +2.3% step at #298 (`c860842c`), whose diff has no
    parse-path change and whose flamegraph pair moves no json frame by more than 0.4 points, so
    most likely LTO layout.
  - **Consequence:** a single before/after pair can't attribute a move of this size on these
    scenarios. Measure a change against two or three neighbouring builds, as
    [performance.md](../design/performance.md) §1's "`native-relay` is code-layout-sensitive"
    describes.

  Measurements are in [performance.md](../design/performance.md) §1, "What moved since
  2026-10-02". The #457 flamegraph and disassembly evidence is in its "What moved since 2026-09-28"
  section as of `201bb5a7`.
  **Revisit trigger:** a later change moves either by more than its neighbouring builds do.

## Lua

- **A script can't mutate `event.span` in place.** `event.span` is a read-only proxy over every
  `SpanRecord` field (`docs/design/lua-api.md`'s "Reading `event.span`"). To change one, a script
  calls `Event.new(event:to_table())` with the table edited
  ([ADR `lua-event-constructor`](../adr/lua-event-constructor.md)), paying a full table
  conversion.
  - **To close:** an in-place write path would share the constructor's parsers, as the
    `event.log` field writes do. It's a small follow-up, not designed yet.
- **A script can't write `Value::Null`.** A `nil` assigned to `event.attributes`, `resource`, or
  `scope.attributes` deletes the key ([ADR `lua-in-place-edits`](../adr/lua-in-place-edits.md)).
  Two sinks render a `Null` differently from an absent key: `syslog_out` writes a `Null`
  `syslog.timestamp` as RFC 5424's nil `-`, and the JSON formats write `null`. No shipped config
  or example needs either.
  - **To close:** a separate way to write `Null`, such as a global function (a proxy method would
    shadow an attribute of its name), if a need shows up.
- **A `Null` attribute doesn't survive a read-back write.** A `Null` attribute reads as `nil`, so
  Lua can't tell writing back what it read from deleting. `a.x = a.x`, or
  `for k, v in pairs(a) do a[k] = f(v) end` where `f(nil)` is `nil`, deletes an attribute a decoder
  stored as `Null` (a JSON `null`, a syslog nil timestamp). The unmodified-write no-op in [ADR
  `lua-value-identity-preservation`](../adr/lua-value-identity-preservation.md) no longer covers
  it.
  - **To avoid it:** skip a key whose read value is `nil` before writing it back.
- **A Lua `flush()` emission is never attributed to the batches that fed it.** `flush()` runs in
  a root context ([ADR `lua-flush-root-context`](../adr/lua-flush-root-context.md)), and there's no
  accumulator for the runtime to inspect, unlike `Transform::flush`'s linking (see "Internal
  spans" under [Internal telemetry and
  self-logging](telemetry.md#internal-telemetry-and-self-logging)).
  - **Workaround:** a script that wants that relationship tracks contributing contexts itself
    inside `process()`.
- **A batch sent into a revoked Lua inbox after `REVOKE_DRAIN_TIMEOUT` is lost uncounted.**
  `revoke_lua_io`, run when the watcher revokes a wedged node or when the Lua thread's loop fails,
  closes the inbox and counts each batch it still receives as `batches.dropped{reason="shutdown"}`
  under the node's id. It receives for `REVOKE_DRAIN_TIMEOUT` (250 ms) only.
  - **Consequence:** a producer that reserved its permit before the close and is still blocked on
    another consumer then sends into a channel nobody reads: counted `sent` upstream and nothing
    at the Lua node. It's a named exception in [ADR
    `shutdown-accounting-and-cancellation-safety`](../adr/shutdown-accounting-and-cancellation-safety.md),
    decision 1.
  - **Revisit trigger:** a revoked node's `dropped` count falls short of its producers' `sent` in
    practice.
- **A caught error from a Rust callback other than `Event.new` costs a walk of the script's
  global tables.** mlua 0.9.9 builds a traceback for every error a Rust callback returns, and
  mlua-sys 0.6.8's compat53 `luaL_traceback`, which it binds for LuaJIT, names an anonymous C
  frame by searching every global and every field of every global table.
  - **Consequence:** each failed call to a proxy metamethod (`__index`/`__newindex`),
    `event:to`, `telemetry.*`, `random.*`, or `print` that a script catches with `pcall` costs time in
    proportion to its global state, and an error that escapes `process()`/`flush()` pays the same
    once for its traceback. The cost is per failed call, not per loop iteration of a successful
    one, and none of these is a call a script retries until it succeeds. `Event.new` raises its
    refusals from Lua instead
    ([ADR `lua-refusals-raised-from-lua`](../adr/lua-refusals-raised-from-lua.md)).
  - **To close:** mlua-sys binds LuaJIT's own `luaL_traceback`, which prints a C function's
    address and searches nothing, or each callback moves onto the same Lua-shim pattern.
- **A nonzero float under 2^-52 in magnitude reads back `0` through `Event.new`.** mlua 0.9.9's
  LuaJIT number read truncates toward zero and keeps that integer when the difference is under
  `f64::EPSILON`. So a metric value, bound, or float attribute of, say, `1e-20`, read through
  `Event.new(event:to_table())` or written back through a proxy
  (`event.attributes.x = event.attributes.x`), comes back `I64(0)`.
  - **To close:** a number read that bypasses mlua's `Value` conversion. See [ADR
    `lua-event-constructor`](../adr/lua-event-constructor.md)'s "Amendment: counts round-trip at
    every magnitude".
