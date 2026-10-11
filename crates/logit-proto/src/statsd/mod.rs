//! The statsd and DogStatsD decoder: [`StatsdDecoder`] turns plain statsd text, or statsd with the
//! DogStatsD extensions, into events. `statsd_in` (`crates/logit-inputs/src/statsd.rs`) wraps it in
//! a UDP, TCP, or Unix-socket listener; `statsd_out`'s encoder
//! (`crates/logit-outputs/src/statsd.rs`) is its mirror.
//!
//! **This module doc is the decoder's canonical grammar and mapping table**, which docs, tests, and
//! examples point at. The decoder is the input half of the `statsd_in -> statsd_out` lossless-relay
//! pair ([ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md); the mirror is
//! [ADR `statsd-output`](../../../../docs/adr/statsd-output.md)). It never sees a peer: the
//! sender attributes `statsd_in` can add are stamped after decode.
//!
//! ## Grammar
//!
//! A superset covering plain statsd and the DogStatsD tag/container-id/timestamp extensions:
//!
//! ```text
//! <name>:<value>[:<value>...]|<type>[|@<sample-rate>][|#<tag>[:<value>],...][|c:<container-id>][|e:<external-data>][|card:<cardinality>][|T<unix-seconds>][|<ignored>]
//! ```
//!
//! `<type>` is one of:
//!
//! - `c` (counter): one [`Event`](logit_core::Event) per value, extrapolated
//!   (`value / sample_rate`) into [`logit_core::MetricKind::Sum`].
//! - `g` (gauge): one `Event` per value. Unsigned is a [`logit_core::MetricKind::Gauge`]; a leading
//!   `+`/`-` is an unresolved [`logit_core::MetricKind::GaugeDelta`]
//!   (`docs/adr/relative-gauge-adjustments.md`). Sample rate is ignored: a gauge value is not a
//!   count to extrapolate.
//! - `ms`/`h`/`d` (timing/histogram/distribution): **one `Event` per line**, every value in one raw
//!   [`logit_core::MetricKind::Samples`] with `sample_rate` carried verbatim. No extrapolation and
//!   no sketching here: under `docs/adr/lossless-transit.md`'s "summarization is opt-in and named"
//!   rule only `aggregate` sketches. The weighting bound lives there too: `Samples::weight` clamps
//!   `round(1 / sample_rate)` to `Samples::MAX_WEIGHT` (1000), and `aggregate` counts a clamp as
//!   `logit.transform.samples.weight_clamped` with a `sample_rate_clamped` diagnostic. The wire
//!   type letter survives as the `statsd.type` attribute (rule (b) of
//!   `docs/adr/lossless-transit.md`), since all three land on the same `Samples` shape.
//! - `s` (set): **one `Event` per line**, every value in one
//!   [`logit_core::MetricKind::SetMembers`], each member a zero-copy `Bytes` slice of the datagram,
//!   in wire order. Only `aggregate` turns these into a [`logit_core::HyperLogLog`]
//!   ([`logit_core::MetricKind::Set`]). Sample rate is ignored, as for `g`.
//!
//! Multiple values on a `c`/`g` line become independent events sharing type, sample rate and tags
//! (gauge sign is per value, so one event would lose which value had which sign). A
//! `ms`/`h`/`d`/`s` line's values stay together on one event. A datagram may hold many
//! newline-separated lines.
//!
//! **`|c:<container-id>` and `|T<unix-seconds>` apply to every metric type**, not only the `c`/`g`
//! the DogStatsD spec restricts them to (`docs/design/telemetry-landscape.md`): a
//! forward-compatible superset. `|c:<id>` (v1.2+; v1.4+'s `ci-`/`in-`-prefixed forms land verbatim)
//! stamps `statsd.container_id: Value::Str`, a zero-copy datagram slice. `|T<secs>` sets
//! [`Event::timestamp`](logit_core::Event::timestamp) to `secs * 1_000_000_000` in place of the
//! receipt time `decode_into`'s `received_at` supplies, and stamps
//! `statsd.timestamp: Value::U64(secs)`, the raw wire value. The carrier is what survives a stage
//! that rebuilds `Event::timestamp` (`aggregate`'s flush), and it tells a wire timestamp from a
//! receipt-time one. A `|T` that isn't an unsigned integer (the form is under the rejection
//! rules below), or whose nanoseconds overflow `i64`, rejects only that line as `bad_line`. Both
//! attributes are protocol-namespaced carriers (`docs/adr/lossless-transit.md`). Every other
//! unrecognized `|` segment is accepted and ignored, for forward compatibility.
//!
//! **`|e:<external-data>` (v1.5, Agent 7.57+) and `|card:<cardinality>` (v1.6, Agent 7.64+)** stamp
//! `statsd.external_data` and `statsd.cardinality`, both `Value::Str` zero-copy slices, on every
//! metric type and on events and service checks (as `e:`/`card:` fields). Both are carried
//! verbatim: `card:` isn't checked against the Agent's `none`/`low`/`orchestrator`/`high`, since
//! a relay carries what the client sent, and an empty `|e:` stamps an empty string, as an empty
//! `|c:` does.
//!
//! A line is rejected as `bad_line` when it has no `:` or an empty name, no `|<type>`, an unknown
//! type, a `c`/`g`/`ms`/`h`/`d` value that doesn't parse or isn't finite (`NaN`/`inf` parse as
//! `f64`; `s` members are opaque and never parsed), a `c` value whose extrapolation overflows to
//! infinity (`1e308|c|@0.1`), or an `@rate` that doesn't parse, isn't finite, or is outside
//! `(0, 1]` (checked even on a `g`/`s` line, which ignores the rate).
//!
//! The integer fields, `|T`, an event's `d:` and `{TITLE_LEN,TEXT_LEN}`, and a service check's
//! `STATUS`, parse as Rust's unsigned integers do: decimal digits, with an optional leading `+`
//! and any number of leading zeros. No recorded client writes either, and both name the same
//! number as the bare digits, so accepting them loses nothing. `statsd_out` writes the bare
//! digits.
//!
//! ## DogStatsD tags
//!
//! A `|#` segment is a **list** of `key[:value]` tokens, not a map. The Datadog agent keeps every
//! token and dedupes only exact duplicates, so `#team:a,team:b` is two live tags (a query grouping
//! by `team` places the point in both groups) while `#team:a,team:a` is one. `insert_tags` does
//! the same: **a repeated tag key folds into a [`logit_core::Value::Array`] in wire order**
//! (`#team:a,team:b` -> `team: Array[Str("a"), Str("b")]`, three occurrences -> three elements),
//! and **an exact duplicate token is deduped** (`#team:a,team:a` -> `Str("a")`, `#urgent,urgent`
//! -> `Bool(true)`). **A one-element `Array` is never produced**, so a non-repeated tag decodes to
//! a scalar. A valueless tag is `Bool(true)`. `syslog_in`'s `insert_param` applies the same
//! fold to a repeated RFC 5424 PARAM-NAME (`docs/adr/syslog-structured-data-convention.md`): a
//! plain `AttrMap::insert` per token would let the last token win, which is loss under
//! `docs/adr/lossless-transit.md`.
//!
//! A bare token and a valued one that share a key are not duplicates; **both survive, in order**:
//! `#urgent,urgent:1` -> `urgent: Array[Bool(true), Str("1")]` (re-emitted by `statsd_out` as
//! `urgent,urgent:1`) and `#urgent:1,urgent` -> `Array[Str("1"), Bool(true)]` ->
//! `urgent:1,urgent`. Array order is wire order; the attribute map stays sorted by `Symbol`, so tag
//! key order doesn't change. Element values are zero-copy datagram slices, like a scalar tag
//! value.
//!
//! **The fold applies to the `#` payload only.** Every `#` segment on a line unions into the same
//! attribute map (`|#a:1|#a:2` folds as `|#a:1,a:2` would), while `@`, `|c:` and `|T` are
//! last-segment-wins. A repeated `|T`/`|c:`/type therefore never reaches `insert_tags`. A tag
//! **literally named** `statsd.type` (or any `statsd.*` carrier key) inside `#` does, and can
//! decode to an `Array`. On egress it matches no `statsd_out` carrier arm (each expects a
//! `Value::Str`/`Value::U64`) and is filtered out of the tag segment uncounted, as any wrong-typed
//! carrier is. On a `ms`/`h`/`d` line the decoder's own `statsd.type` stamp runs after the tags
//! and overwrites it.
//!
//! ## DogStatsD events and service checks
//!
//! Two more line shapes, picked out by their leading sigil before the grammar above applies: `_e{`
//! (an **event**) and `_sc|` (a **service check**). Nothing else about a leading `_` is special:
//! `_total.count:1|c` falls through to the metric grammar, since `_` is a legal name byte and
//! Datadog's own parser reserves only these two sigils.
//!
//! **Trailing whitespace is payload on both shapes, so `decode_into` never trims it off them.**
//! Every line has `\r` and leading whitespace trimmed; trailing whitespace is trimmed too, except
//! on a line starting `_e{` or `_sc|`. `_e{TITLE_LEN,TEXT_LEN}`'s lengths are authoritative, so a
//! trim would either shrink the line under a correct length (rejecting a legal event) or change
//! `TEXT`. `_sc|`'s `m:` is often the last field, and its trailing whitespace is message bytes a
//! trim would drop with no error. `event_text_ending_in_whitespace_is_kept` and
//! `service_check_message_trailing_whitespace_is_kept` pin this.
//!
//! **Event**: `_e{<TITLE_LEN>,<TEXT_LEN>}:<TITLE>|<TEXT>|d:<secs>|h:<hostname>|p:<normal|low>|
//! t:<info|success|warning|error>|k:<aggregation_key>|s:<source_type_name>|#<tags>|
//! c:<container_id>|e:<external_data>|card:<cardinality>`. `TITLE_LEN`/`TEXT_LEN` are byte lengths as on the wire and decide the split,
//! since `TEXT` may contain `|` and `:`. The line is rejected when a length runs past the line,
//! lands mid-UTF-8-char (checked via `str::get`, so never a panic), or no `|` follows the title,
//! when the `{a,b}` header is malformed, or when a `t:`/`p:` value is unrecognized. It decodes to
//! one [`Event::log`](logit_core::Event::log):
//!
//! - `message` is `TEXT` with its `\n` (backslash, `n`) escape unescaped to a newline; zero-copy
//!   when there is nothing to unescape. The title is never unescaped.
//! - `severity` maps `t:error`/`t:warning`/`t:success`/`t:info` to `Error`/`Warn`/`Info`/`Info`,
//!   and is `None` when `t:` is absent.
//! - `event_name` stays `None`: a title is free text, and interning it would grow the global
//!   interner without bound.
//! - Attributes (all `Value::Str`, zero-copy where possible): `statsd.event.title` (always),
//!   `statsd.event.priority` (`p:`, raw), `statsd.event.alert_type` (`t:`, raw),
//!   `statsd.event.aggregation_key` (`k:`), `statsd.event.source_type` (`s:`),
//!   `statsd.event.host` (`h:`), each only if present, plus `statsd.timestamp`,
//!   `statsd.container_id`, `statsd.external_data`, `statsd.cardinality` and `#tags` as on a
//!   metric line. `d:<secs>` plays `|T`'s role (same
//!   checked parse, same event timestamp and carrier); `|T` itself is an unrecognized field here
//!   and is ignored.
//!
//! **Service check**: `_sc|<NAME>|<STATUS>|d:<secs>|h:<hostname>|#<tags>|c:<container_id>|
//! e:<external_data>|card:<cardinality>|m:<message>`. `NAME` must be non-empty and `STATUS` an integer `0..=3`
//! (OK/WARNING/CRITICAL/UNKNOWN), or the line is rejected. The fields come in any order, and each,
//! `m:` included, ends at the next `|`: the DogStatsD reference puts `m:` last, but the `datadog`
//! Python client writes `c:` and `card:` after it, and the Agent reads `m:` up to the next `|`
//! (both recorded, `testdata/interop/datadog/README.md`). So a message can't contain `|`. It
//! decodes to one [`Event::metric`](logit_core::Event::metric), `MetricKind::Gauge(status as f64)`
//! under the check's name (interned, like a metric name), with attributes
//! `statsd.service_check.name` (always, `Value::Str`: `MetricRecord` has nowhere else to carry it),
//! `statsd.service_check.status` (always, `Value::U64`), `statsd.service_check.message` (`m:`,
//! verbatim) and `statsd.service_check.host` (`h:`), plus `statsd.timestamp`,
//! `statsd.container_id`, `statsd.external_data`, `statsd.cardinality` and `#tags` as above.
//!
//! **Tag values, `|c:<id>`, and set members are zero-copy slices of the datagram**, like every
//! field `syslog_in`'s decoder extracts: [`logit_core::subslice::share`] rebuilds each `Bytes` as a
//! slice of the datagram passed to [`decode_into`](crate::Decoder::decode_into), rather than
//! copying through `impl From<&str> for Value`. Tag keys and the metric name don't need this: both
//! only reach [`logit_core::interner::intern`], which copies into its own table regardless.
//!
//! ## Malformed input
//!
//! **A malformed line is skipped and reported as a `bad_line` diagnostic, and the rest of its
//! datagram still decodes**: clients pack independent metrics into one datagram.
//! [`decode_into`](crate::Decoder::decode_into) fails the whole input only when it isn't valid
//! UTF-8.

pub mod decode;

pub use decode::StatsdDecoder;
