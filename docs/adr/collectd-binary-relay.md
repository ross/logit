---
created: 2026-09-12
updated: 2026-10-23
---

# collectd binary-protocol relay: identity as attributes, value types as `Sum`/`Gauge`, and a packing encoder

## Status
Accepted

## Context

[`docs/OVERVIEW.md`](../OVERVIEW.md) names collectd in `logit`'s ingest scope, and
[`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md)'s "collectd binary/network
protocol" section surveys the wire format, but no `collectd_in`/`collectd_out` `ComponentKind` and no
`logit_proto::collectd` codec exist yet. [ADR `lossless-transit`](lossless-transit.md) requires every
`P_in -> P_out` pair to be a lossless relay modulo a named list of permitted normalizations. Unlike
Prometheus ([ADR `prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md)), collectd
needs no core-model change to satisfy that: `Sum{value, temporality, monotonic}` already covers
COUNTER/DERIVE/ABSOLUTE, `Gauge` covers GAUGE, `MetricList` (a `SmallVec` inlining one) already
carries an N-data-source value list in wire order on one `Event`, and `FLAG_NO_RECORDED_VALUE`
already gives a NaN gauge somewhere to live. So `crates/logit-core/tests/type_sizes.rs` is untouched
by this pair, and no new crate dependency is needed.

Decisions settled with Ross (2026-09-11/12): notifications
(`Message`/`Severity`) are in scope but land as a trailing workstream (W5); `types.db` DS naming is
operator-opt-in via a `types_db:` config field, never embedded (collectd's own file is GPL); the
shared UDP listener grows multicast support as part of `collectd_in`, with no new config field, and
that support benefits `statsd_in`/`syslog_in` for free; signing and encryption are deferred, tracked
as known gaps rather than built now.

## Decision

### Transport

`collectd_in` and `collectd_out` speak collectd's binary network protocol over **UDP only** — no TCP,
since collectd's own daemon and `network` plugin never offer one. `collectd_in`'s `bind:` address is
handled by the same shared UDP listener every other datagram input uses; when the bound IP is
multicast, the listener sets `SO_REUSEADDR`, binds the unspecified address for that address family on
the configured port, and joins the group (`join_multicast_v4`/`join_multicast_v6`) instead of binding
the multicast address directly — collectd's own default group addresses are `239.192.74.66` (v4) and
`ff18::efc0:4a42` (v6), port `25826`. `collectd_out` sends to a single configured `endpoint:`, unicast
or multicast, with no special-casing on the send side (multicast send is just a send to a multicast
destination address).

Packets are capped by `max_packet_bytes`, defaulting to **1452** (collectd's own default, chosen to
fit inside a typical Ethernet MTU after IP/UDP headers on a slightly-tunneled path) and bounded to
**1024..65535** by config validation — the same range collectd's own `MaxPacketSize` documentation
gives. A value list that doesn't fit in what remains of the current packet flushes it and starts a
new one; a value list that doesn't fit in an empty packet is dropped whole (see the encode mapping
table below).

**Amendment (2026-09-29): the upper bound is 65507, not 65535.** collectd's documented range tops
out at the IP length field's maximum, but a UDP payload over IPv4 can't exceed 65507 bytes (65535
minus the 20-byte IPv4 and 8-byte UDP headers), and the kernel refuses a longer datagram with
`EMSGSIZE`, which `collectd_out` counts as a per-datagram drop under an `ok` request. Config
validation bounds `max_packet_bytes` to **1024..=65507**, the ceiling `statsd_out` and
`graphite_out` get over UDP too
([ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md),
decision 9). IPv6 allows 20 more bytes, but an endpoint can be a hostname whose family validation
can't know, so the bound is the IPv4 figure. `collectd_out` also sends through the datagram path
the UDP sinks share and counts `logit.output.requests` under the four fault classes, where it
counted `ok|error`.

### Model mapping

**Decode (wire → model).** One `Values` part decodes to one `Event` carrying one `MetricRecord` per
value, in wire order.

| Wire | Model | Counter / diag |
|---|---|---|
| Values part, N values | one `Event`, `metrics` = N records in wire order | — |
| COUNTER u64 | `Sum{U64(v), Cumulative, monotonic:true}` | none |
| DERIVE i64 | `Sum{I64(v), Cumulative, monotonic:false}` | none |
| ABSOLUTE u64 | `Sum{U64(v), Delta, monotonic:true}` | none |
| GAUGE finite/±inf | `Gauge(F64(v))` | none |
| GAUGE NaN | `Gauge(F64(0.0))` + `FLAG_NO_RECORDED_VALUE` (NaN *is* collectd's "no value"; keeps `PartialEq` fixed points honest) | none |
| unknown DS type; `count==0`/`>64`; `len != 6+9*count` | packet tail rejected | `bad_part` / `bad_datagram` |
| Host/Plugin/PluginInstance/Type/TypeInstance | sticky → identity attrs (Attribute conventions below); empty string → attribute absent | — |
| Values with empty host/plugin/type | list skipped | `incomplete_identity` |
| TimeHR / Time / none | `timestamp` = cdtime→ns / s·1e9 / `received_at` | — |
| IntervalHR / Interval | `_logit.collectd.interval: F64` = cdtime/2³⁰ s (exact); 0 → absent | — |
| non-NUL-terminated / short / overlong part | tail rejected | `bad_part` |
| 0x0200 Signature | skipped, unverified | — (known-gaps) |
| 0x0210 Encryption | tail dropped | `encrypted_packet_dropped` |
| 0x0100/0x0101 | skip until W5; W5: `Event::log` | W5 `notification_dropped` |
| other part types | skipped by length | — |

**Encode (model → wire)** is the inverse, plus rules for model kinds collectd's wire can't carry
natively:

| Model | Wire | Counter (`logit.output.metrics.skipped{reason=…}` unless noted; kind drops use `{metric_kind=…}` — the Prometheus codec's convention) |
|---|---|---|
| event with `_logit.collectd.type` | one Values part, `event.metrics` in order, identity from its identity attrs | — |
| event without `_logit.collectd.type` | one 1-DS list per record: plugin = name up to first `.` (whole if none); type = `gauge`/`counter`/`derive`/`absolute` by kind (all 1-DS in stock types.db); type_instance = remainder after first `.`; no plugin_instance | — |
| `Sum{Cumulative, mono}` integral, u64 range | COUNTER | — |
| `Sum{Cumulative, !mono}` integral, i64 range | DERIVE | — |
| `Sum{Delta, mono}` integral, u64 range | ABSOLUTE | — |
| `Sum{Delta, !mono}` | dropped | `{metric_kind="non_monotonic_delta_sum"}` |
| any `Sum` non-integral / out of range / non-finite | dropped (statsd `2\|c\|@0.3` → 6.67 lands here; rounding would fabricate) | `{reason="unencodable_value"}` |
| `Gauge` finite/±inf; flagged `Gauge` | GAUGE; GAUGE NaN | — |
| non-`Gauge` with `FLAG_NO_RECORDED_VALUE` | dropped | `{reason="no_recorded_value"}` |
| `GaugeDelta` | dropped | `{metric_kind="gauge_delta"}` + diag `gauge_delta_unresolved` (shared key) |
| `Samples`/`Distribution`/`SetMembers`/`Set`/`Histogram`/`ExponentialHistogram`/`Summary` | dropped; one exhaustive arm each, no wildcard | `{metric_kind="samples"\|…}` |
| like-relay list where any record fails | whole list dropped (receiver's `ds_num` check would reject a partial one anyway) | the failing reason, once |
| `timestamp > 0` / `<= 0` | TimeHR / dropped | `unencodable_timestamp` |
| `_logit.collectd.interval` finite >0 / absent or bad | IntervalHR round(v·2³⁰) / IntervalHR 0 | bad non-absent: `logit.output.tags.dropped{reason="unrepresentable"}` |
| host | `host.name` (event, then resource) → configured `hostname:`; none resolved → **list dropped** | `{reason="no_host"}` |
| identity attr `Str`/`Bytes` | verbatim after sanitizing (`Bytes` byte-verbatim) | `logit.output.identity.sanitized{reason="substituted"\|"truncated"}` |
| identity attr of another `Value` type | treated as absent | `tags.dropped{unrepresentable}` |
| every other attribute | dropped (collectd has no tags) | `tags.dropped{reason="no_wire_form"}` |
| `unit`/`description`/`start_timestamp`/`exemplars`/`Scope`/`schema_url` | dropped | none; known-gaps rows |
| empty plugin after sanitizing | dropped | `empty_name` |
| list alone > `max_packet_bytes` | dropped whole | `{reason="oversize_value_list"}` + diag |
| datagram `EMSGSIZE` at send (sink) | its lists dropped, no `Fault` | `logit.output.messages.dropped{reason="oversize_datagram"}` |

**Amendment (2026-10-13): typed values.** The model's scalar is a `MetricValue`
([ADR `typed-metric-scalar`](typed-metric-scalar.md)), so decode keeps each data source's wire
type: COUNTER and ABSOLUTE decode to `U64`, DERIVE to `I64`, and GAUGE to `F64`. Encode writes a
typed `Sum` straight to its wire type when the value is in range: a `U64` or `I64` `Sum` needs no
integral check, and an `F64` `Sum` still must be integral and in range. The precision loss above
2⁵³ that the decode table recorded is closed, and `collectd_in -> collectd_out` is exact over the
whole `u64`/`i64` range. A `Gauge` holding an `I64` or `U64` is written as GAUGE
(an `f64`), counted `logit.output.metrics.normalized{reason="inexact"}` when the conversion loses
bits; a `Bool` value is written as `1` or `0`, counted `{reason="bool_value"}`. None of the ten
permitted normalizations below says an integer becomes a double, and none changes.

### Attribute conventions

Rule (b) of [ADR `lossless-transit`](lossless-transit.md): the raw five-tuple identity rides on
event attributes and wins on collectd egress. Consumed by `collectd_out`. The host and the two
instances are public, and every other sink renders them as tags; the rest are private, and every
other sink ignores them (see the private-namespace amendment at the end of this record).

| Attribute | Value | Meaning |
|---|---|---|
| `host.name` | `Str` (`Bytes` if not UTF-8) | Host, under the standard host key; absent when empty on the wire |
| `_logit.collectd.plugin` | `Str`/`Bytes` | required for like-relay |
| `collectd.plugin_instance` | `Str`/`Bytes`, only when non-empty | |
| `_logit.collectd.type` | `Str`/`Bytes` | its presence selects like-relay encoding |
| `collectd.type_instance` | `Str`/`Bytes`, only when non-empty | |
| `_logit.collectd.interval` | `F64` seconds (cdtime/2³⁰, exact) | absent when 0 / no part |
| `_logit.collectd.severity` (W5) | `U64` ∈ {1,2,4} | raw severity; marks a notification; outranks `log.severity` on egress |

### Value-list shape: one `Event` per `Values` part

Each `Values` part decodes to exactly one `Event`, whose `metrics` field holds N `MetricRecord`s in
wire order — never N separate `Event`s, and never merged across parts. `MetricList`'s inline
capacity of one means a single-DS list (collectd's overwhelmingly common case) costs no heap
allocation; the multi-DS case spills, same as everywhere else `MetricList` is used.

Identity — host, plugin, plugin instance, type, type instance — lives on the `Event`'s own
**attributes**, never on a per-host `Resource`. Two reasons, both load-bearing:

- `crates/logit-pipeline/src/accumulator.rs`'s `BatchAccumulator::absorb` decides whether to keep
  accumulating into the same in-flight batch or flush and start a new one by comparing the batch's
  resource with `Arc::ptr_eq`, not by value. A per-datagram or per-host `Resource` would make every
  distinct host (or, worse, every distinct list within one datagram) its own pointer, defeating the
  accumulator's batching for exactly the traffic shape — one shared listener, many hosts and plugins
  — collectd relays generate the most.
  [`docs/design/pipeline-graph.md`](../design/pipeline-graph.md) and the accumulator's own module
  doc describe the batching contract this depends on.
- `syslog_in` already sets this precedent: RFC 3164/5424's HOSTNAME is stamped as an event attribute
  (`host.name`), not folded into a per-message `Resource`, for the identical reason — a shared
  listener receiving from many hosts keeps one `Resource` and lets identity ride on the event.
  `collectd_in` follows the same shape rather than inventing a second one for a structurally
  identical situation.

`collectd_in` therefore always returns the same shared `Resource::default()` for the life of the
component (never a per-host one), and `collectd_out` reads identity from the event attributes
above, falling back to `host.name` (event, then resource) only for host, per the encode table
above.

### Record naming and types.db

A decoded record's name is chosen without ever needing collectd's own `types.db` for fidelity — the
encoder rebuilds the wire shape from the `collectd.*` and `_logit.collectd.*` attributes,
`MetricList` order, and each record's kind, so like-relay round-tripping never depends on how a name
was chosen. Naming exists purely for display and for cross-protocol legibility (what a non-collectd
sink downstream sees as the metric name):

- **Single-DS list**: `<plugin>.<type>` — the lone data source (conventionally named `value` in
  collectd's own types) is omitted from the name, matching `collectd`'s own `write_graphite` plugin's
  default naming.
- **Multi-DS list, with `types_db` configured and the type resolves** with a matching DS count and
  kinds: `<plugin>.<type>.<ds_name>` — one record per name.
- **Multi-DS list, `types_db` configured and the type resolves but with a mismatched DS count or
  kinds**: `<plugin>.<type>.<i>` (0-based index), plus a throttled `types_db_mismatch` diag. The
  diagnostic is specifically about *disagreement*: the configured file describes this type
  differently from the way the sender is sending it, so naming from it would attach the wrong label
  to a real measurement.
- **The type is not in `types_db` at all, or no `types_db` is configured**: index naming
  (`<plugin>.<type>.<i>` for multi-DS, `<plugin>.<type>` for single-DS), **no diag** — a type simply
  not being resolved is routine (a custom plugin, a newer collectd, or no database supplied), not a
  misconfiguration, and reporting it would fire forever on every interval.

`collectd_in` accepts an optional `types_db: [paths]` field (operator-supplied, e.g.
`/usr/share/collectd/types.db`) read once at startup and merged (later files override earlier
entries on a type conflict, matching collectd's own `TypesDB` directive semantics). collectd's own
`types.db` file is GPL-licensed and is **never embedded or shipped** in this repo; a test fixture is
a short, hand-written file in the same line format, never a copy of the real one.

**Amendment (2026-10-09): a record name carries at most 127 bytes of its plugin and its type.**
Decode cuts each of a record name's `<plugin>` and `<type>` segments to its first
`DATA_MAX_NAME_LEN` − 1 (127) wire bytes, in every naming rule above. The `_logit.collectd.plugin`
and `_logit.collectd.type` attributes keep every byte, and `collectd_out` never reads the name, so
the like-protocol relay is unchanged.

- **Why.** A Values part interns one name per data source, and the interner never evicts. Uncut,
  a 60 KB Plugin part ahead of a 64-value list put 64 times its length into the interner for one
  datagram. collectd's own receiver rejects a string past 127 bytes, so only a misconfigured
  sender that isn't collectd reaches the cut.
- **Consequence.** Two Plugins or two Types that share their first 127 bytes get the same record
  name. Every sink but `collectd_out` ignores the private `_logit.collectd.plugin` and
  `_logit.collectd.type`, so each writes the two as one series. `docs/known-gaps/mappings.md` has
  the `decode (collectd)` row.
- **Observable.** A list whose name is cut reports a throttled `record_name_cut` diagnostic. It
  costs one length comparison per list, and the merge it can cause is otherwise silent. There is no
  counter, because the decoder holds no `Telemetry`, and nothing is dropped.

`crates/logit-proto/src/collectd/mod.rs`'s decode table is the canonical statement of the rule.

### Time: cdtime ↔ nanoseconds

collectd v5+'s high-resolution time (`TimeHR`/`IntervalHR`) is a 64-bit fixed-point value in
2⁻³⁰-second units ("cdtime"), not a float, specifically to avoid floating-point time arithmetic.
`logit` converts using collectd's own split-arithmetic formulas from `utils_time.h`, not a floating
or `u128` approximation, so the conversion agrees bit-for-bit with what collectd itself computes:

```
CDTIME_T_TO_NS(t) = (t>>30)*1e9 + (((t & 0x3fffffff)*1e9 + (1<<29)) >> 30)
NS_TO_CDTIME_T(ns) = ((ns/1e9)<<30) | ((((ns%1e9)<<30) + 500_000_000)/1e9)
```

`ns -> cdtime -> ns` is exact. `cdtime -> ns -> cdtime` can move by at most one tick (2⁻³⁰ s) on the
*first* hop through the conversion, and is stable (a fixed point) on every hop after that — this
one-tick movement is a **permitted normalization** (below), not loss: a value 2⁻³⁰ seconds (about
0.93 nanoseconds) different from the original is not a value any consumer of either protocol can act
on differently. There is no raw-`cdtime` attribute (no `_logit.collectd.time`): unlike identity
fields, which have a genuine raw-encoding/normalized-field split under rule (b) of
`lossless-transit`, time has exactly one normalized field (`Event::timestamp`, nanoseconds) and the
≤1-tick drift is small enough, and universal enough across *every* cdtime round-trip regardless of
value, that carrying the original bits alongside would be state kept only to avoid a normalization
already declared permitted — not a fact about the record any consumer needs back.

Legacy `Time`/`Interval` (whole seconds) always **re-emit as `TimeHR`/`IntervalHR`** on egress — the
model has no place to remember "this specific list arrived using the legacy, low-resolution part
type," and every collectd version new enough to matter (v5+) accepts the HR parts, so re-emitting HR
loses no information a legacy sender could not itself have chosen to send more precisely. A `Values`
part with no `Time` part at all decodes with `timestamp = received_at` (lenient — collectd's own
receiver rejects a list with `time == 0` outright; `logit` observes and stamps rather than rejecting)
and re-emits `TimeHR = received_at`, since there is no better time to hand back.

### Host is never empty on the wire

collectd's own receiver (`network_dispatch_values`) rejects any `Values` list with an empty host (or
plugin, or type) with `-EINVAL`. `collectd_out` treats that as a hard invariant on its own egress,
but resolves it by **omission, not fabrication**: a `host.name` attribute (event, then resource)
wins when present and non-empty, falling back to the sink's own optional `hostname:` config
field — config-supplied only, exactly like `syslog_out`'s own `hostname:` field (`crates/logit-config/src/lib.rs`'s `SyslogOut::hostname` doc: omitted entirely
rather than a literal `logit` default, "so a relayed line's origin is never silently overwritten
with something that looks like a config mistake"). There is deliberately **no OS-hostname read and
no placeholder default** in this sink. (Amended 2026-10-14: the process now resolves a hostname
([ADR `process-hostname`](process-hostname.md)) and `internal` stamps it as `host.name`, but
`collectd_out` still doesn't fall back to it, because one made-up host would merge every
unlabeled sender into a single host's metrics.)

When neither resolves — no `host.name` anywhere on event or resource, and no configured
`hostname:` — the value list is **dropped whole**, counted
`logit.output.metrics.skipped{reason="no_host"}`, with a throttled `no_host` diagnostic telling the
operator to set `hostname:` or stamp `host.name` with a `set` transform. This is the one case in the
encode table above where host resolution doesn't always succeed — but every like-relay list (one
whose event carries `_logit.collectd.type`) already carries `host.name` as part of the
same attribute set the like-relay encoding requires, so `no_host` only ever bites cross-protocol
ingress (an event from some other input with no host anywhere on it), never a genuine
`collectd_in -> collectd_out` relay.

### NaN is a flagged point, not a dropped one

A GAUGE value of NaN is collectd's own idiom for "no value collected this interval" (many collectd
plugins report NaN rather than omitting the data source entirely, so a fixed-size multi-DS list stays
fixed-size even when one source has nothing to say). `logit` decodes GAUGE NaN as `Gauge(0.0)` plus
`FLAG_NO_RECORDED_VALUE`, the same flag OTLP's own explicit "no recorded value" marker uses, rather
than as a `Gauge(f64::NAN)`: `PartialEq`-based fixed-point tests need a value that compares equal to
itself round-trip after round-trip, which a raw NaN payload does not reliably do bit-for-bit across
languages and float implementations, while the flag does. Encoding reverses this exactly: a flagged
`Gauge` (any value, canonically 0.0) re-emits as GAUGE NaN; the specific NaN bit pattern is not
preserved (a permitted normalization, below), and a *non*-`Gauge` record carrying the flag has no
GAUGE-NaN equivalent on the wire and is dropped (`{reason="no_recorded_value"}`).

This makes `collectd_out` the **second** sink, after `otlp_out`, whose wire has a genuine no-value
concept. `crates/logit-core/src/metric.rs`'s `MetricRecord::flags` doc and
`docs/known-gaps/mappings.md`'s `NO_RECORDED_VALUE` entry both currently state the rule as "every
non-OTLP sink... must instead treat a flagged record as carrying no genuine reading" / "[o]nly
`otlp_out` can keep a flagged point on the wire... [n]o other sink or transform has a wire/model
concept of 'no value here'." Both are narrowed by this ADR from "every non-OTLP sink" / "no other
sink" to **"every sink whose wire has no no-value concept"** — `collectd_out`'s GAUGE-NaN re-encode
is the second, narrower exception the existing wording didn't anticipate, not a violation of the
rule once restated. See Consequences for where those two edits land.

### Sanitization

Applies only on encode, over the identity string/byte attributes and over the
plugin/type_instance names the fallback path derives from a non-collectd event's own name: NUL and
`/` both become `_` (the second matching collectd's own `escape_slashes`, since a bare `/` inside an
identity field would otherwise be indistinguishable from a path separator to tooling that treats
these fields as filesystem-adjacent, which several collectd write plugins do); truncate to 127 bytes
(collectd's `DATA_MAX_NAME_LEN` is 128 including the trailing NUL) on a UTF-8 char boundary for
`Str`, a plain byte boundary for `Bytes`. Every substitution or truncation is counted
(`logit.output.identity.sanitized{reason="substituted"|"truncated"}`). No whitespace or other
control-byte substitution: collectd's own wire format carries those untouched in a NUL-terminated
string, and the fixed point needs them to survive a round-trip unchanged, unlike the injection-unsafe
delimiter characters statsd and syslog have to escape.

### Cross-protocol fallback

An event with no `_logit.collectd.type` attribute (i.e., not itself decoded from collectd, or from a
protocol whose identity `logit` has no reason to map onto collectd's five-tuple) still encodes,
record by record, as one single-DS `Values` list per `MetricRecord`: plugin is the record's name up
to its first `.` (the whole name if there is none), type is `gauge`/`counter`/`derive`/`absolute`
chosen by the record's `MetricKind` (all four are single-DS types in collectd's own stock
`types.db`), type_instance is whatever remains of the name after that first `.`, and plugin_instance
is left empty. This gives every other protocol's metrics a legible collectd rendering (a statsd
counter `hits` becomes plugin `hits`, type `absolute`) without inventing collectd-specific structure
that isn't already present on the source event.

### Permitted normalizations for this pair

Per [ADR `lossless-transit`](lossless-transit.md), the following count as normalization, not loss,
for `collectd_in -> collectd_out`, and are what the round-trip fixed-point test asserts equality
modulo:

1. Legacy `Time`/`Interval` always re-emit as `TimeHR`/`IntervalHR`.
2. `TimeHR` may move by at most one tick (2⁻³⁰ s) on the first hop through the cdtime conversion,
   and is stable (a fixed point) after that.
3. String-part elision is recomputed per output datagram, independent of how the input happened to
   elide strings — the encoder's own `last`-identity tracking, not the sender's.
4. Datagram boundaries are re-chosen by `max_packet_bytes`, independent of how the input happened to
   pack lists into datagrams.
5. List reordering within a batch.
6. Unknown part types and Signature parts are dropped.
7. NaN payload bits canonicalize (the specific NaN bit pattern is not preserved; see "NaN is a
   flagged point" above).
8. A `Values` list with no `Time` part re-emits `TimeHR = received_at`.
9. `/` and NUL substituted with `_`; names truncated to 127 bytes (both counted).
10. `IntervalHR = 0` stands in for an absent `Interval`/`IntervalHR` part on the way in.

**Amendment (2026-09-12): an eleventh normalization — elided time and interval parts are
restored.** The list above covers *string*-part elision (3) but says nothing about the two numeric
parts, and the encoder treats them differently: it writes `TimeHR` **and** `IntervalHR` for every
value list and never elides an unchanged one
(`crates/logit-proto/src/collectd/encode.rs`'s `write_list`). A real collectd sender does elide
them — the committed capture `testdata/interop/collectd/collectd-000.raw` carries 26 value lists
behind 17 `TimeHR` parts and a single `IntervalHR` — so:

11. `TimeHR`/`IntervalHR` are written for every value list on egress; a sender's elided (unchanged)
    time or interval part is restored, so a packed collectd datagram grows and may split across
    `max_packet_bytes`.

This is information-preserving in both directions — a restored part carries exactly the value the
receiver's sticky state already held — but it is not free, and an operator sizing a relay needs to
know it: that same 1296-byte capture re-encodes to 1717 bytes in two datagrams at the default
1452-byte cap, about 30% more bytes and one more packet than the fleet emitted. Eliding them on
egress would be a legitimate future optimization (it costs one comparison against the last written
value, exactly as the five identity strings already get); it is not done today, and until it is this
entry is what makes "fixed point modulo the named list" true.
`crates/logit-cli/tests/collectd_round_trip.rs`'s `elided-time-interval` fixture pins the
byte-level shape, and its `a_real_collectd_capture_grows_when_its_elided_time_parts_are_restored`
test pins the numbers quoted above against the real capture.

Note the numbering: `crates/logit-proto/src/collectd/mod.rs`'s module doc carries the same list with
(3) and (4) above folded into a single entry, so this amendment is **item 10** there and item 11
here. Both files name the same eleven facts.

### Signing and encryption: deferred

Signature parts (0x0200) are skipped on decode without verification — the payload following one is
still plaintext and decodes normally. Encryption parts (0x0210) cannot be decoded without the shared
key collectd's own `network` plugin negotiates out of band, which this pair does not implement yet;
the packet tail from that point on is dropped, counted `encrypted_packet_dropped`. Neither
`collectd_in` nor `collectd_out` emits a Signature or Encryption part on egress. This is a known-gap,
not a silent omission — see Consequences.

### Notifications (W5 shape)

collectd's `Message`(0x0100)/`Severity`(0x0101) notification parts are in scope but land in a
trailing workstream after the metrics path is solid. The shape decided now, built later: a
notification decodes to an `Event::log` — a `LogRecord` whose `message` and `severity` (1 FAILURE →
`Error`, 2 WARNING → `Warn`, 4 OKAY → `Info`) come from the wire, plus a `_logit.collectd.severity:
U64` attribute carrying the raw value, which outranks the normalized `LogRecord.severity` on
`collectd_out` egress — the raw-encoding-outranks-normalized-field shape rule (b) of
`lossless-transit` already gives `_logit.syslog.severity`/`_logit.otel.severity_number`. Until W5
lands, both parts are simply skipped on decode (a routine, uncounted skip — not every collectd
deployment sends notifications) and never emitted on encode.

**Amendment (2026-09-12): notifications landed.** W5 built exactly this shape. Decode: `0x0101`
Severity sets sticky state (reset per datagram, like every other sticky field); `0x0100` Message
dispatches a notification against the current sticky host/plugin/plugin_instance/type/
type_instance/time — collectd's own `notification_t` has no interval field, so a notification never
carries `_logit.collectd.interval` even when an earlier value list in the same datagram set one.
Dropped (`notification_dropped`) when severity is outside `{1, 2, 4}`, the message is empty, or no
host is set; time `0` is lenient, exactly as for a value list (`received_at`). Encode: a `log`-only
event (no metrics) carrying a `_logit.collectd.severity` attribute is a notification; **metrics
win** on an event carrying both, which drops and counts the attribute
(`tags.dropped{reason="unrepresentable"}`) exactly as any other unrepresentable carrier
is. A notification is **always its own datagram** — confirmed by the recorded capture below,
not merely assumed: real collectd flushes whatever value-list packet is in progress, writes the
notification alone, and the next value list restates its identity in full. `CollectdEncoder` does
the same and shares no elision state with a notification in either direction — it neither elides
against a preceding list's identity nor leaves any behind for a following one, since a real
receiver's sticky state resets at exactly that boundary too. (An earlier revision of this amendment
shared elision state with a notification on the assumption that a receiver's sticky state might do
the same; the recorded capture settled it, and a review of this workstream's own PR caught the
resulting bug before merge — a packed notification's `last.clone_from(cur)` could make a *following*
list wrongly elide a field the notification never actually set, mislabeling it on the wire.) A
message is sanitized on a narrower rule than an identity field: NUL becomes `_` (uncounted), `/`
rides through untouched, and it is truncated to `NOTIF_MAX_MSG_LEN - 1` (255) bytes, counted
`logit.output.messages.truncated`. Severity absent-despite-being-attempted or out of `{1, 2, 4}`, an
empty message, and an oversize notification are each their own counted drop
(`notification_dropped`/`empty_message`/`oversize_notification`); a `log`-only event with no
`_logit.collectd.severity` attribute at all is not a notification attempt and still falls through to
the ordinary `skipped_no_metrics` path. The recorded interop corpus gained a fourth, separate
fixture (`testdata/interop/collectd/collectd-notification-000.raw`, from the `threshold` plugin, its
own `collectd-threshold.conf` so the `threshold` plugin never loads for the plain value-list capture
too) rather than touching the original three.

### No `logit_proto::Encoder`

Same reasoning as [ADR `statsd-output`](statsd-output.md)'s "No `logit_proto::Encoder`" section and
[ADR `prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md)'s identical section:
that trait returns one opaque `Bytes` per batch with no framing metadata, which cannot express the
per-datagram boundaries a UDP sink genuinely needs — packing several value lists into one datagram up
to `max_packet_bytes`, and flushing at a boundary, is exactly the kind of decision a single
`encode(&mut self, &EventBatch) -> Result<Bytes, _>` call has no way to make. `CollectdEncoder`/
`CollectdDecoder` instead expose plain, directly-unit-testable methods with
`with_telemetry`/`with_diagnostics` builders, the same shape `PrometheusEncoder`/`PrometheusDecoder`
already established.

**Amendment (2026-09-12): `CollectdEncoder` implements `FramedEncoder`.** `encode_into` is the
implementation of [ADR `framed-encoder`](framed-encoder.md)'s `logit_proto::FramedEncoder` (`type
Meta = usize; type Stats = EncodeStats;`), over `logit_proto::MessageBuf<usize>` — the per-datagram
`usize` meta is the value-list count that datagram carries, exactly what `collectd_out` needs to
attribute an `EMSGSIZE` drop to the right number of metrics (what a bespoke `Packets` type existed
to carry before this landed). One signature change, the same shape `statsd_output`'s amendment
made: the per-call `max_packet_bytes` argument became encoder state
(`CollectdEncoder::with_max_packet_bytes`, default uncapped), and `CollectdOutput` applies its own
`max_packet_bytes:` config value once at build time rather than on every `send` — collectd has no
TCP transport to leave uncapped, so unlike `StatsdOutput` there is no per-transport branch here at
all. The cap in effect, the packing/elision logic, the wire bytes, and `EncodeStats` are all
unchanged.

## Alternatives considered

- **A per-host `Resource` instead of event attributes for identity.** Rejected: `BatchAccumulator`
  keys its in-flight-batch decision on `Arc::ptr_eq` of the resource, not on its value, so a
  per-host (or, worse, per-list) `Resource` would silently defeat batch accumulation for a shared
  listener receiving from many hosts — exactly the traffic shape collectd relays produce the most
  of. `syslog_in`'s per-event HOSTNAME precedent already answers this the same way for a structurally
  identical situation.
- **A raw `_logit.collectd.time` cdtime attribute**, carrying the original bits alongside the
  normalized `Event::timestamp`, the way `host.name`/`_logit.collectd.plugin`/etc. carry
  the raw identity. Rejected: identity fields have a genuine two-sided split (a raw wire spelling
  *and* a normalized field that can each independently be right or wrong — e.g. sanitization changes
  the raw spelling but not what it means). Time has exactly one normalized field and the only
  difference cdtime round-tripping can ever introduce is the ≤1-tick drift already declared a
  permitted normalization; carrying the raw bits would be state kept only to avoid a normalization
  already accepted, not a fact anything downstream needs back.
- **Embedding collectd's own `types.db`.** Rejected outright: the file is GPL-licensed and this repo
  cannot ship it. `types_db:` is an operator-supplied path, and fixtures use a short hand-written file
  in the same line format.
- **Building signing/encryption support now, rather than deferring it.** Rejected for this pair's v1:
  collectd's signing/encryption scheme requires a shared-secret negotiation this repo has no existing
  primitive for (unlike TLS, which every other encrypted transport in this repo already uses), and no
  workstream in this plan needs it to ship a lossless, useful relay for the overwhelmingly common
  unauthenticated-UDP collectd deployment. Tracked as a known gap instead of built speculatively.
- **Encoder-side use of `types_db` to regroup fallback (non-collectd) records back into
  multi-DS lists**, the inverse of the decoder's naming lookup. Rejected: the fallback path (events
  with no `_logit.collectd.type`) only ever emits stock single-DS types (`gauge`/`counter`/`derive`/
  `absolute`), so there is never a multi-DS type to regroup into — building the lookup would add
  complexity with nothing behind it to use it.
- **N separate `Event`s per `Values` part instead of one `Event` with N `MetricRecord`s.** Rejected:
  the wire's own `Values` part is one list — one identity, one timestamp, N co-reported data sources
  — and `MetricList` exists precisely to carry that shape on one `Event` without forcing an N-way
  split that would then need to be re-merged on the way back out to reconstruct the original list.

## Consequences

- `collectd_in`/`collectd_out` become the fifth like-protocol pair under
  [ADR `lossless-transit`](lossless-transit.md), alongside `statsd`, `otlp`, `syslog`, and
  `prometheus`.
- Known-gaps rows, added by W1 (`docs/known-gaps/mappings.md`): non-integral or
  out-of-range `Sum` values dropped rather than rounded on encode;
  `Samples`/`Distribution`/`SetMembers`/`Set`/ `Histogram`/`ExponentialHistogram`/`Summary`
  unsupported on collectd egress; `unit`/`description`/
  `start_timestamp`/exemplars/`Scope`/`schema_url` dropped; `MAX_VALUES_PER_LIST` (64) as a hard cap
  on data sources per list; no signing or encryption support in either direction.
- Two existing documents get amended, not just added to, by W1: `crates/logit-core/src/metric.rs`'s
  `MetricRecord::flags` doc and `docs/known-gaps/mappings.md`'s `NO_RECORDED_VALUE` entry both
  narrow from "every non-OTLP sink"/"no other sink" to "every sink whose wire has no no-value
  concept," per "NaN is a flagged point, not a dropped one" above — `collectd_out` becomes the
  second sink (after `otlp_out`) that can keep a flagged point on the wire.
- Multicast support lands inside the shared UDP listener (`bind_one`), not `collectd_in`-specific
  code, so `statsd_in` and `syslog_in` gain the ability to join a multicast group for free the moment
  `collectd_in`'s workstream lands, with no config or code change of their own required.
- `crates/logit-core/tests/type_sizes.rs` is untouched by this pair: nothing in the core model
  changes shape to support it.
- No new crate dependency: `bytes`, `socket2`, and `proptest` are already present in the workspace.

## Amendment (2026-10-13): loss vocabulary

Under [ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md), `collectd_out`'s `output.tags.dropped` becomes `output.tags.omitted` with the same
reasons, an unresolved `GaugeDelta` becomes `output.metrics.dropped{reason="unresolved_gauge_delta"}`,
a notification that fails validation becomes `output.metrics.dropped{reason="invalid_notification"}`
(its diagnostic stays `notification_dropped`), and every fault reason moves from `metrics.skipped`
to `metrics.dropped`. `no_recorded_value` and a kind collectd has no data-source type for stay
`metrics.skipped`. `collectd_in` gains counters for the losses it only diagnosed:
`input.frames.dropped{reason="undecodable"|"malformed_part"|"encrypted"}`,
`input.metrics.dropped{reason="incomplete_identity"}`, and
`input.events.dropped{reason="invalid_notification"}`. `crates/logit-proto/src/collectd/mod.rs`'s
module doc is the canonical table.

## Amendment (2026-10-13): the carriers are private, the identity is public

Under [ADR `private-attribute-namespace`](private-attribute-namespace.md), the attributes this pair
carries split in two:

| Private, renamed | Public, unchanged |
|---|---|
| `_logit.collectd.plugin`, `_logit.collectd.type`, `_logit.collectd.interval`, `_logit.collectd.severity` | `collectd.host`, `collectd.plugin_instance`, `collectd.type_instance` |

`crates/logit-proto/src/collectd/mod.rs`'s `ATTR_PREFIX` becomes `_logit.collectd.` and covers
the private four. There are no aliases and no decode path for the old private names.

- **Why the split.** The plugin and type are already in the record name, `<plugin>.<type>[.<ds>]`,
  modulo the 127-byte cut the carriers undo; the interval and severity restate the wire form of
  something the model holds. The host and the two instances are identity nothing else carries:
  the record name leaves them out, and the host stays off the resource by design. Hiding them
  would merge different hosts' and instances' lists into one series at every other sink.
- **The relay is unchanged.** `collectd_out` reads the private four under its own prefix and the
  public three by exact name, so `collectd_in -> collectd_out` stays a fixed point.
- **Every other sink renders the public three as tags and ignores the private four.**
- **`collectd_out` skips other protocols' carriers uncounted.** Any other `_logit.` key reaching it
  is skipped without counting `tags.omitted{reason="no_wire_form"}`.

## Amendment (2026-10-23): the host is `host.name`

`collectd_in` writes a value list's or notification's Host part as the event attribute
`host.name`, the standard host key, instead of `collectd.host`, and `collectd_out` reads
`host.name` as the Host part, then its `hostname:`
([ADR `process-hostname`](process-hostname.md)'s "Amendment: `host.name` is the standard host key
(2026-10-23)"). The encode and attribute tables above state the current form; the 2026-10-13
amendment's public `collectd.host` is now `host.name`.

- **The relay is unchanged.** `collectd_in -> collectd_out` stays a fixed point: the Host part
  decodes to `host.name` and encodes from it.
- **The host stays on the event**, for the `Arc::ptr_eq` reason above.
- **Every other sink reads the host under one name.** `prometheus_out` labels it `host_name`,
  `influxdb_out` and `graphite_out` tag it `host.name`, `datadog_out` takes it as the host,
  `splunk_hec_out` writes it as each object's `host`, and `statsd_out` under `format: dogstatsd`
  writes it as the `host:` tag.
- **`collectd_out` no longer counts `host.name` as an omitted tag**: it is the Host part. There is
  no alias and no decode path for `collectd.host`; an attribute of that name is an ordinary
  attribute, counted `tags.omitted{reason="no_wire_form"}` like any other.
