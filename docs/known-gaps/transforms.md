# Known gaps: Transforms

Entry format and the other areas: [the known-gaps index](README.md).

## Transforms: predicates and sampling

- **Predicate-shaped work (throttling, dedup, and anything needing a real operator: `>=`,
  `contains`, cross-attribute comparison) costs a Lua VM, an OS thread, and roughly 9× the
  per-event allocations of a native transform, because `logit` has no native predicate
  language.** The rest of predicate-shaped work has native answers:
  - Equality-only filtering, such as splitting a `logit_in` fan-out back apart by an attribute an
    upstream `set` stamped: `has_attributes`/`drop_attributes`
    ([ADR `attribute-filtering-components`](../adr/attribute-filtering-components.md)).
  - Destination selection (splitting one flow into named streams): `route` (equality on
    provenance/attribute/resource) and `target` components (ADR
    [`target-components`](../adr/target-components.md)); `lua` can also route with `event:to`.
  - Sampling: `sample`
    ([ADR `consistent-sampling-component`](../adr/consistent-sampling-component.md)), because a
    `lua` component can't express keyed, cross-process-consistent sampling.

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
- **`sample`'s consistency is `logit`'s own, not OTel's, and stops at the key.** It has four edges
  ([ADR `consistent-sampling-component`](../adr/consistent-sampling-component.md)):
  1. **No OTEP 235 / W3C `tracestate` `th:` interop.** An OTel SDK or collector sampling by
     threshold compares the trace id's *low 56 bits* against a `th:` value it also writes into
     `tracestate`; `logit` hashes the id's hex text with XXH64 and compares the hash's *top 53
     bits*. So `logit`'s `sample` and an upstream OTel sampler at the same rate keep *different*
     traces. Because `logit` neither reads nor writes `th:`, downstream can't recover the sampling
     probability from a `logit`-sampled trace. Adopting OTEP 235 means a second bit convention for
     `key: trace_id` only (no other key has a `tracestate`) and, because the hash is a frozen
     cross-version contract, its own ADR.
  2. **`always_keep` is per leg.** Nothing about an override hit propagates: a flagged event kept
     by one sampler is unflagged at the next sampler unless that one has the same `always_keep`.
     The reasoning is the rate's own: no propagated bit.
  3. **Two spellings of one trace id that aren't lowercase hex don't agree.** `key: trace_id`
     hashes a lifted id as its 32 lowercase hex characters, and `{attribute: ..}` hashes a `Str`
     as-is. So an *uppercase*-hex attribute (off-spec: W3C mandates lowercase), or a trace id
     carried as a 16-byte `Value::Bytes`, gets a different verdict than the same id lifted by
     `trace_context`. Case isn't folded and raw bytes aren't hex-encoded, so the canonicalization
     table (`logit_core::sampling`) stays one rule per `Value` variant.
  4. **A resource key is all-or-nothing per resource.** `key: {resource: ..}` gives every event of
     one resource the same verdict, which is the point of keying on one. At a low resource count
     the kept fraction is lumpy, not `rate`: two services at `rate: 0.5` keep zero, one, or both.

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
- **`shape` tracks top-level attribute keys only.** A nested `Value::Map`'s keys count toward that
  map's width (`logit.shape.nested_map_width`) and its values toward the per-type counters, but
  they never enter the distinct-key set or the key-set hash. Two events with matching top-level
  keys and entirely different nested maps are one key-set to `logit.shape.distinct_keysets`.
  `AttrMap`'s sorted `Symbol` sequence gives this key-set identity for free, and the sizing
  questions the survey feeds (`docs/design/memory.md` §8) are about the top-level map.
  - **Consequence:** a shop whose width lives under a `k8s`/`labels` map reads as narrow on the
    distinct-key gauges and wide only on the nested ones. Read the two together.
- **A batch's `Scope` passes through `shape` untouched, unlike its `Resource`.** `resource: drop`
  substitutes an empty `Resource` (through `Transform::map_resource`) so no resource attribute
  value leaves the tap. `Scope` has no equivalent: `Transform` has no scope-substitution hook, and
  [ADR `shape-observer-component`](../adr/shape-observer-component.md) judges one that every
  implementer must carry, for this one component, not worth it. A scope names an
  instrumentation library rather than carrying payload, so this is a narrow exception to the
  counts-only property, not a hole in it.
  - **Consequence:** an `otlp_in` whose senders put identifying information in
    `Scope.attributes` passes it through the tap. `shape`'s *flush* output carries no scope at all
    (a window spans many batches, so there's no single one to keep).
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

- **Nothing bounds how many series one window holds.** `max_retained_series` caps only what
  survives a flush. Within a window every distinct `(name, unit, attribute set)` opens a series,
  and memory grows with that count until the flush drains it.
  - **Workaround:** bound it upstream. A `keep` ahead of `aggregate` limits which attributes reach
    it, and `keep_values` limits the values one attribute can take.
    `logit.transform.series.active` shows the peak each window.
- **Nothing bounds how many `(resource, scope)` groups one window holds, and a lookup that misses
  `group_for`'s memo walks all of them.** The memo answers every event of a batch after the first,
  and the walk compares one stored 64-bit hash per group, about half a nanosecond each, before any
  full compare. `logit.transform.resource.groups` shows the count.
  - **Consequence:** absorb cost still grows with the group count once it reaches tens of
    thousands and batches are small. `docs/design/performance.md` §1's `aggregate-groups` row
    measures the 1000-group case on the perf VM (0.290 CPU µs/event), and
    [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s "The groups
    bound" section has the mechanism and the measurements.
  - **To close,** if that shape shows up in a profile: a map from hash to group index.
- **`aggregate` keeps `U64(200)`, `I64(200)`, and `F64(200.0)` as three series, and text sinks
  render all three as `200`.** Series identity is the typed value. A non-negative `json` integer
  arrives `U64`, an OTLP or Lua integer `I64`, and a `scale`d one `F64`.
  - **Consequence:** a mixed pipeline can send `prometheus_out`, `influxdb_out`, or
    `graphite_out` two or three samples under one label set in one window. `-0.0` and `0.0` are
    two series as well, but those sinks render them `-0` and `0`, so they stay apart downstream.
  - **Workaround:** to merge the variants, convert the tag to one type in a `lua` stage ahead of
    `aggregate`. See
    [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s "Amendment:
    series identity, merge laws, and accounting as a stated contract (2026-09-26)", "Series
    identity".
- **`aggregate` drops every exemplar on the records it absorbs.** An exemplar is one observation,
  and a summarized window has no per-observation data to attach it to. `aggregate` is the stage
  that summarizes by stated purpose, so the loss falls under
  [ADR `lossless-transit`](../adr/lossless-transit.md)'s "summarization is opt-in and named" rule.
  - **Workaround:** to keep exemplars, route the records around `aggregate`. See
    [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s "Amendment:
    series identity, merge laws, and accounting as a stated contract (2026-09-26)".
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
- **`DdSketch::merge` treats a sketch whose count and zero count are both 0 as empty, even when it
  carries bins.** Only a decoded sketch can be in that state (`DdSketch::from_parts` with summary
  stats claiming a zero count over non-empty bins), and merging it into a series drops those bins
  without a counter. No encoder `logit` ships writes that shape, and the threat model treats a
  crafted one as a non-goal.
  - **Revisit trigger:** a real producer's sketch carrying bins under a zero count.
- **`series_retention` counts flushes, not wall time.** The runtime coalesces missed flush ticks,
  so a stalled or overloaded stage that flushes late stretches retention in wall time: a series
  idle across one late flush loses one flush of retention, however long the gap. The same holds
  for a cumulative series' lifetime between restarts.
- **`aggregate`'s flush clock is `SystemTime`, which can step backwards.** A retained series' start
  time is clamped between the previous flush's clock and this one's (ADR
  `aggregation-window-semantics`'s "Start time after a cap eviction").
  - **Consequence:** after a backwards step the new start can precede the previous point, and
    emitted points stop being monotonic in time. A consumer still sees a changed start and
    re-bases.
  - **Why not a monotonic clock:** it would need its own mapping to wall time on every emitted
    point, which nothing else in the pipeline does.

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
- **`forwarded:` trusts the header it names.** The five HTTP listeners (`otlp_in`, `datadog_in`,
  `datadog_trace_in`, `splunk_hec_in`, and `prometheus_in`'s remote-write receiver) and
  `http_access` each read the client from the leftmost entry of the one forwarding header
  `forwarded:` names, with no trusted-proxy list and no hop count
  ([ADR `forwarded-header-parsing`](../adr/forwarded-header-parsing.md)). That's right behind one
  proxy you control that overwrites the header.
  - **Consequence:** a client that reaches the listener or web server directly, or through a proxy
    that appends to a header the client sent, can name any address as `client.address`. Picking
    the right hop needs to know which proxies are yours (the `set_real_ip_from`/`real_ip_recursive`
    shape of nginx's realip module), config none of these components carry. That's crafted input
    whose defense isn't free, so it's a non-goal under
    [ADR `deployment-threat-model`](../adr/deployment-threat-model.md).
  - **Workaround:** make the port reachable only through the proxy, and have the proxy overwrite
    the header rather than append to it.
  - **Revisit trigger:** a deployment where clients can reach the component past the proxy, or a
    proxy chain whose first hop isn't the operator's. The fix is a list of trusted proxy
    addresses, with the rightmost untrusted entry taken as the client.
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
- **Not a gap, a consequence: an nginx line with no `$http_user_agent` gets no
  `user_agent.class`.** By `http_access`'s absent rule, an absent header writes no class: absence
  is silence, not `none` (which a logged-but-empty header gets). `demo/nginx/nginx.conf`'s format
  doesn't log the user agent, so the demo's nginx events carry no class while its HAProxy events
  do.
  - **Workaround:** to get one, log `"user_agent.original":"$http_user_agent"`.
- **A pathological Host header can truncate the syslog-bound JSON line, but nginx's own
  header-size limit makes that hard to trigger.** `$host` is unbounded and attacker-controlled
  behind a public IP, while the lean `access_semconv` format (`fixtures/nginx/nginx.conf`) sizes
  its fixed fields well under nginx's syslog message cap.

  Measured against nginx 1.31.4 (`docs/plans/nginx-integration.md`): under default settings an
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
  - `nginx_metrics` derives nothing field-based (only the fieldless `nginx.requests` counter
    increments).
  - Sibling requests are unaffected.

  `http_access`'s 253-character cap on `server.address` doesn't help here, because it runs after
  `json` parses the line. There's no nginx-side mitigation (such as capping `$host`'s logged
  length): the pipeline degrades gracefully, and capping a field nginx allows up to 8KB solves a
  problem the design doesn't have.
- **HAProxy's native CBOR log output (`%{+cbor}o` / `%{+cbor,+bin}o`) is a closed door, not a "not
  now"; don't reopen it without new evidence.** It would cost a second decoder to build and
  maintain and a `syslog_in` framing knob, for a ~15% wire saving and no parse-time win. Measured
  against real HAProxy 3.0.27 output with a throwaway decoder:
  - **Binary CBOR is reachable on both transports, once the flags are spelled right.** HAProxy's
    log-format options are comma-separated: `%{+cbor,+bin}o`. `%{+cbor+bin}o` applies only the
    last flag (`bin` alone, plain unencoded output) and `%{+bin+cbor}o` only `cbor` (the hex form):
    `parse_logformat_node_args` in HAProxy's `src/log.c` resets its start pointer at every `+`, and
    the manual never says so. With the comma form, HAProxy emitted raw binary CBOR in the syslog
    MSG both to a plain UDP `log` target and to a `ring` with `server ... log-proto octet-count`.
    Over TCP octet-counting that MSG reaches `logit` intact, because `syslog_in` decodes a
    non-UTF-8 MSG as a `Value::Bytes`; over UDP it would need a `syslog_in` opt-out of newline
    splitting.
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
- **`json`'s `invalid_utf8: replace` is opt-in and lossy.** nginx's `escape=json` escapes `"`,
  `\`, and control bytes but passes bytes `>= 0x80` raw, so a Latin-1 `User-Agent`, or a
  percent-*decoded* path logged via `$uri`, puts invalid UTF-8 into an otherwise valid-looking JSON
  line.
  - **Consequence:** under the default `invalid_utf8: reject`, `json` fails the line (one
    throttled `parse_failure`, the event passed through with none of its fields), the one failure
    `http_access` can't reach from behind `json`. `invalid_utf8: replace`
    ([ADR `http-access-normalization`](../adr/http-access-normalization.md)) retries on a copy
    with every invalid sequence replaced by U+FFFD and reports `logit.component.diagnostics
    {key="invalid_utf8"}`, but the original bytes don't survive.
  - **Workaround:** `docs/http-access-logs.md` tells nginx users to log `$request_uri`, never
    `$uri`, which removes the decoded-path source but not a client's own non-ASCII header bytes.
- **`json-parse-x3` runs about 8% slower than its best, from code layout, with no source-level
  fix.** At #457 (`d47a99b6`) it rose 2.264 → 2.444 µs/event on the perf VM, and `main` reads
  2.502. A disassembly finds `JsonParser::process`, `parse_object`, the `deserialize_map` visitor,
  and `insert_sym` identical instruction for instruction across that merge; only code addresses
  moved.
  - Aligning `insert_sym`'s loop to a 64-byte line (`-C llvm-args=-align-loops=64`, +4.6%
    `.text`) didn't recover it. The same layout move makes `logfmt-parse` −7.9% faster, so the two
    trade against each other.
  - Single-parser `json-parse` moves less (+1.8%), because contention on the shared interner
    dominates `x3`'s stage cost.
  - Also open: `json-parse`'s unattributed +2.3% step at #298 (`c860842c`), whose diff has no
    parse-path change and whose flamegraph pair moves no json frame by more than 0.4 points, so
    most likely LTO layout.

  Measurements and flamegraph numbers are in [performance.md](../design/performance.md) §1, "What
  moved since 2026-09-28". **Revisit trigger:** a later change moves either.

## Lua

- **A script can't mutate `event.span` in place.** `event.span` is a read-only proxy over every
  `SpanRecord` field (`docs/design/lua-api.md`'s "Reading `event.span`"). To change one, a script
  calls `Event.new(event:to_table())` with the table edited
  ([ADR `lua-event-constructor`](../adr/lua-event-constructor.md)), paying a full table
  conversion.
  - **To close:** an in-place write path would share the constructor's parsers. It's a small
    follow-up, not designed yet, the same posture as in-place
    `event.log.message`/`severity`/`body_format` writes.
- **A Lua `flush()` emission is never attributed to the batches that fed it.** `flush()` runs in
  a root context ([ADR `lua-flush-root-context`](../adr/lua-flush-root-context.md)), and there's no
  accumulator for the runtime to inspect, unlike `Transform::flush`'s linking (see "Internal
  spans" under [Internal telemetry and
  self-logging](telemetry.md#internal-telemetry-and-self-logging)).
  - **Workaround:** a script that wants that relationship tracks contributing contexts itself
    inside `process()`.
- **A script has no time bound, and its memory bound is opt-in.** No instruction or time hook is
  used, by design: LuaJIT's compiled traces skip a count hook unless the runtime is built with
  `LUAJIT_ENABLE_CHECKHOOK`, so a hook would be both slow and unreliable ([ADR
  `lua-runaway-script-bounds`](../adr/lua-runaway-script-bounds.md)).

  What bounds a script instead:
  - The table-depth cap (`MAX_TABLE_DEPTH`, 128) and raw table reads.
  - `newproxy`'s removal.
  - The stall heartbeat with progress-based wedge detection: a script inside one call with no
    progress for 10 s reads `stalled`, and one still stuck 2 s into shutdown has its channels
    revoked and fails the run.
  - `max_memory`, only when the component sets it: a VM still over the cap after full garbage
    collections fails the node, exit code 2.
  - An 8 MiB stack per Lua thread (virtual, committed on touch), so pure-Lua recursion through C
    frames (`string.gsub` callbacks a few hundred deep) doesn't abort the process.

  Standing residuals, under [ADR `deployment-threat-model`](../adr/deployment-threat-model.md):
  - Interner growth from script-derived strings (`Event.new`'s and the proxy setters'
    `name`/`unit`/`description`/`event_name` fields, and nested attribute keys); the interner entry
    under [Event model and interner](runtime.md#event-model-and-interner) lists every site.
  - A shared-table DAG still converts at 2^k nodes, because the depth cap bounds nesting, not
    size.
  - A 128-deep value a script builds doesn't survive a relay through `otlp_out -> otlp_in`
    (OTLP's own JSON and protobuf nesting limits, 41 and 49 levels, are both under the cap).
  - Pure-Lua recursion through Rust/C frames can still abort the process past the larger stack.
  - A loop that keeps constructing events with `Event.new` advances the stall heartbeat and is
    never caught as a stall, because telling it from a large `flush()` would need a time limit.
    `max_memory` bounds its memory-retaining form, and a refused call isn't progress, so a `pcall`
    loop of refusals reads as a stall
    ([ADR `lua-refusals-raised-from-lua`](../adr/lua-refusals-raised-from-lua.md)). What's left is
    a loop that discards what it builds, or one with no `max_memory` set.
  - `max_memory` bounds the Lua VM heap only: a retained event costs the VM about 150 bytes while
    its payload stays in the Rust heap (10k retained 1 KiB events: 1.5 MB of VM, about 10 MB of
    Rust), so a script that hoards events shows in process RSS long before it trips the cap. A cap
    under about twice the working set forces a full collection on most batches; the verdict is
    rate-limited to one a second, so that costs latency, not a failure.
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
    `event:to`, `telemetry.*`, or `print` that a script catches with `pcall` costs time in
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
