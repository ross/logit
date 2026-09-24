//! The Graphite/Carbon codec, both directions and both wire protocols: [`GraphiteDecoder`] turns
//! carbon plaintext lines or one pickle batch payload into events, [`GraphiteEncoder`] packs a
//! batch of events back into lines or pickle frames. The pickle writer and the restricted pickle
//! reader live in [`pickle`]; nothing in [`decode`] or [`encode`] knows a pickle opcode.
//!
//! **This module doc is the codec's canonical mapping table and normalization list**, which docs,
//! tests, and examples point at.
//! [ADR `graphite-carbon-relay`](../../../../docs/adr/graphite-carbon-relay.md) is the decision
//! record. It and
//! [`docs/plans/graphite-carbon-relay.md`](../../../../docs/plans/graphite-carbon-relay.md) carry
//! the normalization list with identical numbering; keep all three in step.
//!
//! ## Wire shape, in one paragraph
//!
//! Carbon speaks two protocols for the same four facts: a dotted path, an optional tag set, one
//! number, one whole second. **Plaintext** (TCP or UDP, port [`DEFAULT_PLAINTEXT_PORT`]) is
//! `path[;k=v...] value timestamp\n`, three whitespace-separated fields per line. **Pickle** (TCP
//! only, port [`DEFAULT_PICKLE_PORT`]) is a 4-byte **big-endian** length prefix (Twisted's
//! `Int32StringReceiver`, whose `MAX_LENGTH` is [`DEFAULT_MAX_FRAME_BYTES`]) followed by a pickled
//! `[(path, (timestamp, value)), ...]`. Neither wire has a type, unit, temporality, or
//! description, so the decoded model is [`logit_core::MetricKind::Gauge`] only.
//!
//! ## No `graphite.*` namespace, and no well-known attributes
//!
//! Unlike [`crate::collectd`], [`crate::syslog`], or [`crate::prometheus`], **this pair defines no
//! `ATTR_*` constants and no `graphite.*` attribute namespace**. Each of the four wire facts is
//! stored once, with no lossy normalization: the path *is* [`logit_core::MetricRecord::name`], the
//! tags *are* event attributes, the number *is* the `Gauge` payload, the second *is*
//! [`logit_core::Event::timestamp`]. Rule (b) of
//! [ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md) (a raw carrier outranks a
//! normalized field) has no conflict to resolve here.
//!
//! So a `lua`/`set` stage that renames [`logit_core::MetricRecord::name`] changes the wire path.
//! That is the intended way to rename a series (neither component has a `prefix:`/`template:`
//! field), and `docs/deploying.md` says so.
//!
//! ## Decode: wire → model
//!
//! One [`logit_core::Event`] carrying exactly one [`logit_core::MetricRecord`] per line (plaintext)
//! or per datapoint (pickle). Tags are **event** attributes, never resource ones: the accumulator
//! keys batches on `Arc::ptr_eq` of the resource ([`crate::collectd`]'s module doc).
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | one line / one pickle datapoint | one `Event`, one `MetricRecord` | -- |
//! | `path` (everything before the first `;`) | `name = intern(path)`, `Gauge(v)` | -- |
//! | `;name=value` | event attribute, `Value::Str` over a zero-copy [`bytes::Bytes`] slice of the input | -- |
//! | repeated tag key | last occurrence wins | `logit.input.tags.normalized{reason="duplicate_key"}` + diag `duplicate_tag_key` |
//! | finite value (`3`, `-1.5`, `1e5`, or a pickle `"3.14"` string) | `Gauge(v)` | -- |
//! | NaN / ±inf | line skipped | `logit.input.metrics.skipped{reason="non_finite_value"}` + diag `non_finite_value` |
//! | `timestamp == -1` | `received_at` (carbon's own rule) | -- |
//! | `timestamp > 0`, integral or fractional | `(ts * 1e9) as i64` | -- |
//! | any other `timestamp` (`<= 0`, non-finite, unparseable) | skipped | `logit.input.metrics.skipped{reason="bad_timestamp"}` + diag `bad_timestamp` |
//! | not exactly 3 whitespace-separated fields; an unparseable value; non-UTF-8 bytes; an empty path | skipped | `logit.input.metrics.skipped{reason="bad_line"}` + diag `bad_line` |
//! | malformed tag (a `;` segment with no `=`, an empty name, or an empty value) | the **whole line** is skipped -- carbon's own `TaggedSeries.parse` raises rather than dropping the one tag | `logit.input.metrics.skipped{reason="bad_tag"}` + diag `bad_tag` |
//! | empty / whitespace-only line | skipped, **uncounted** (packet padding, a trailing `\n`) | -- |
//! | line longer than `max_line_bytes` (TCP) | the framer drops it and resynchronizes at the next `\n`; the line after it still decodes, and the connection stays up | `logit.input.frames.dropped{reason="oversize"}` + diag `framing_error` |
//! | pickle frame longer than `max_frame_bytes` | the connection is closed -- there is no resync point in a length-framed stream | `logit.input.frames.dropped{reason="oversize"}` + diag `framing_error` |
//! | a disallowed pickle opcode, or the depth/item caps | `CodecError::Malformed`, the whole frame is dropped | diag `bad_pickle` |
//! | a pickle item that is not `(str, (num, num))` | **that datapoint** is skipped; the rest of the frame still decodes | `logit.input.metrics.skipped{reason="bad_shape"}` + diag `bad_shape` |
//! | `Resource` / `Scope` | the decoder's own shared default / `None` | -- |
//!
//! Framing is the listener's job: the two oversize rows are counted and diagnosed by the shared TCP
//! driver's `Framer` (`crates/logit-inputs/src/tcp.rs`), not by [`GraphiteDecoder`]. Every other
//! row is emitted here.
//!
//! ## Encode: model → wire
//!
//! Counter prefix `logit.output.metrics.skipped{reason=…}` unless noted; a whole-kind drop is
//! tagged `{metric_kind=…}` instead of `{reason=…}`, the shape every other sink uses.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | event with N metrics | N datapoints, in `MetricList` order | -- |
//! | `MetricRecord::name`, sanitized | the path, verbatim | -- |
//! | `Sum{Delta\|Cumulative, monotonic\|!monotonic}` | the bare value | -- (normalization 12) |
//! | `Gauge(v)`, finite | `path v ts` | -- |
//! | `Gauge` NaN / ±inf, or any non-finite value | dropped | `{reason="unencodable_value"}` + diag `unencodable_value` |
//! | any kind flagged [`logit_core::MetricRecord::FLAG_NO_RECORDED_VALUE`] | dropped | `{reason="no_recorded_value"}` + diag `no_recorded_value` |
//! | `GaugeDelta` | dropped | `{metric_kind="gauge_delta"}` + diag `gauge_delta_unresolved` |
//! | `Samples`/`Distribution`/`Histogram`/`ExponentialHistogram`/`Summary`/`Set`/`SetMembers` under [`MultiValue::Skip`] (the default) | dropped, one exhaustive `match` arm each, no wildcard | `{metric_kind="samples"\|"distribution"\|"histogram"\|"exponential_histogram"\|"summary"\|"set"\|"set_members"}` |
//! | the same seven kinds under [`MultiValue::Expand`] | the sub-path table below | `logit.output.metrics.degraded{metric_kind=…}`, **once per record** however many sub-paths it produced |
//! | `event.timestamp.div_euclid(1_000_000_000) <= 0` | dropped | `{reason="unencodable_timestamp"}` + diag `unencodable_timestamp` |
//! | attributes under [`Tags::Carbon`] (the default) | `;name=value`, ascending **rendered** name | -- |
//! | attributes under [`Tags::Drop`] | no tag segment at all | `logit.output.tags.dropped{reason="dialect"}`, once per attribute |
//! | `Value::Str/I64/U64/F64/Bool` | stringified | -- |
//! | `Value::Array` | the **last** representable element | `logit.output.tags.normalized{reason="multi_value"}`, once per attribute |
//! | `Value::Null/Bytes/Timestamp/Map`, or an `Array` with no representable element | the tag is dropped | `logit.output.tags.dropped{reason="unrepresentable"}` |
//! | a forbidden byte in the path / a tag name / a tag value | `_` | `logit.output.metrics.normalized{reason="path_sanitized"\|"tag_sanitized"}`, once per record per reason |
//! | a tag whose name or value is empty after sanitizing | the tag is dropped | `logit.output.tags.dropped{reason="empty"}` |
//! | two tags colliding after rendering | the one whose **original** name sorts first is kept | `logit.output.tags.dropped{reason="collision"}` |
//! | a path empty after sanitizing | dropped | `{reason="empty_name"}` |
//! | a plaintext line longer than `max_packet_bytes` | dropped whole, never split | `{reason="oversize_line"}` + diag `oversize_line` |
//! | a single pickle datapoint larger than `max_frame_bytes` | dropped whole, never split | `{reason="oversize_datapoint"}` + diag `oversize_datapoint` |
//! | `MetricRecord`'s `unit`, `description`, `start_timestamp`, `exemplars`; `EventBatch::scope`; `Resource::schema_url` | dropped | none; `docs/known-gaps.md` rows -- carbon has no field for any of them |
//! | an event with no metrics at all | skipped | [`EncodeStats::skipped_no_metrics`], no counter (nothing was lost) |
//!
//! Attributes in the `statsd.`/`collectd.` namespaces are **skipped uncounted**, as `influxdb_out`
//! skips `statsd.`: they are another protocol's consumed carriers. Resource attributes *do* become
//! tags (as at `influxdb_out`/`statsd_out`); a `graphite_in` resource is empty, so the pair stays
//! a fixed point, and cross-protocol it is a `docs/known-gaps.md` row.
//!
//! ## Sanitization
//!
//! Substitute `_`, **never delete**: deletion would merge `a b` and `ab` into one series.
//! Remaining collisions are resolved on the **rendered** names, never on interner order (ADR
//! `prometheus-scrape-and-exposition`'s "Names and sanitization" section).
//!
//! | Field | Forbidden → `_` |
//! |---|---|
//! | path | whitespace ([`char::is_whitespace`]), [`char::is_control`], `;`, `/`, `\` |
//! | tag name | `;`, `!`, `^`, `=`, whitespace, control |
//! | tag value | `;`, whitespace, control; and a **leading** `~` only |
//!
//! `/` and `\` are whisper's directory separators: one in a path would create a nested directory,
//! not a series segment. There is **no path truncation**: carbon has no length bound, and whisper's
//! 255-byte filesystem component limit is a `docs/known-gaps.md` row. A leading `~` is reserved by
//! carbon's tag grammar; a `~` elsewhere in a tag value rides through.
//!
//! Whitespace is [`char::is_whitespace`] (Unicode): carbon splits a *decoded* `str` with Python's
//! `str.split()`, and [`decode`] mirrors that with [`str::split_whitespace`]. Sanitizing only ASCII
//! whitespace would let a U+00A0 in a path decode as a four-field line.
//!
//! ## `MultiValue::Expand` sub-paths
//!
//! Every expanded kind adds **at least one** dotted suffix, so an expanded sub-path can never
//! collide with the path the same record would have produced under [`MultiValue::Skip`], nor with
//! a scalar record's path unless that record was already named `x.count` by its producer.
//!
//! | Kind | Sub-paths |
//! |---|---|
//! | `Samples` (via [`logit_core::Samples::sketch`]) / `Distribution` | `.count`, `.sum`, `.q0_5`, `.q0_75`, `.q0_9`, `.q0_95`, `.q0_99` |
//! | `Histogram` | `.count` (Σ bucket counts), `.sum`/`.min`/`.max` when `Some`, `.bucket_<b>` per bucket (its **own** count, not a cumulative running total -- `logit_core::Histogram`'s doc) |
//! | `ExponentialHistogram` | `.count`, `.sum`/`.min`/`.max` when `Some`, `.zero_count`; **no buckets** |
//! | `Summary` | `.count`, `.sum`, `.q<q>` per its own quantiles |
//! | `Set` | `.count` = [`logit_core::HyperLogLog::estimate`] |
//! | `SetMembers` | `.count` = the distinct member count |
//!
//! `.sum` **is** emitted for a sketch: [`logit_core::DdSketch::sum`] is exact (a plain `f64`
//! accumulated alongside the bins and added on `merge`), not an estimate like a quantile.
//!
//! The quantiles are [`crate::otlp::metrics::DISTRIBUTION_QUANTILES`], the five every
//! sketch-to-quantiles degradation in this crate reports, so a metric describes itself identically
//! at `otlp_out`, `prometheus_out`, and `graphite_out`. `influxdb_out` keeps its narrower
//! `[0.5, 0.9, 0.99]`.
//!
//! **Number tokens are injective.** A quantile or bucket bound is formatted with `f64`'s `Display`
//! and every `.` substituted with `_`: `0.99 → q0_99`, `1.5 → bucket_1_5`,
//! `-0.5 → bucket_-0_5`, `f64::INFINITY → bucket_inf`. `Display` emits only `-`, digits, at most
//! one `.`, and `inf`/`-inf`/`NaN`, so two distinct bounds never render to one token (the
//! collision argument in `crates/logit-outputs/src/influxdb.rs`'s `render_fields`, which rejects
//! a *rounded* percentile). Graphite needs the substitution because `.` is its hierarchy separator.
//!
//! ## `Protocol` and `Meta`
//!
//! [`GraphiteEncoder`] implements [`crate::FramedEncoder`], not [`crate::Encoder`], because both
//! carbon protocols need per-message boundaries, which `Encoder`'s one blob per batch lacks: a UDP
//! datagram packs whole lines up to `max_packet_bytes`, and a pickle stream writes one
//! length-prefixed frame at a time.
//!
//! `type Meta = usize` is each message's **datapoint count**, so a sink that fails a send
//! attributes the loss to the right number of metrics. Under [`Protocol::Plaintext`] every entry is
//! one line, meta `1`. Under [`Protocol::Pickle`] every entry is one **complete, length-prefixed
//! frame** (the encoder writes the 4-byte big-endian prefix, so the sink does one `write_all` per
//! entry), meta its datapoint count. A new frame opens when the next datapoint would push the
//! payload past `max_frame_bytes`.
//!
//! `Protocol`/`Tags`/`MultiValue` and both size caps are **encoder state**, set by the `with_*`
//! builders, since [`crate::FramedEncoder`] has one signature. The codec never sees a transport,
//! so pickle over UDP is rejected by a graph rule (`crates/logit-pipeline/src/graph.rs`).
//!
//! ## Permitted normalizations
//!
//! `graphite_in -> graphite_out` is a fixed point modulo exactly this list (the round-trip tests in
//! `crates/logit-proto/tests/graphite_fixed_point.rs` pin it). The ADR uses the same numbering:
//!
//! 1. Re-framing: lines are repacked into datagrams or stream writes, and datapoints into pickle
//!    frames of at most `max_frame_bytes`, so one input frame may leave as several and vice versa.
//! 2. An operator-chosen dialect change: plaintext ↔ pickle, in either direction.
//! 3. Datapoint reordering within a batch (everything downstream of a batch boundary is
//!    order-insensitive by design).
//! 4. Tag order is canonicalized to ascending rendered name, as carbon's `TaggedSeries.format`
//!    sorts.
//! 5. A repeated tag key collapses to its **last** occurrence at decode, counted, as carbon's
//!    `TaggedSeries.parse` `dict` does. Not `statsd_in`'s fold into a `Value::Array`: keeping
//!    arrays out of the pair keeps the encoder's array→last-element tag rule from firing in it.
//! 6. Timestamps floor to whole seconds on egress (`div_euclid`, so a negative instant floors
//!    downward rather than toward zero).
//! 7. A `-1` timestamp becomes receipt time on ingress, and leaves as that absolute second.
//! 8. Number formatting becomes the shortest round-trip `f64` rendering: `1.50`, `1.5e0` and
//!    `+1.5` all leave as `1.5`, and `3.0` leaves as `3`.
//! 9. Field separators collapse to a single space, `\r\n` to `\n`; a TCP write terminates every
//!    line including the last, a UDP datagram terminates none.
//! 10. Sanitizer substitutions, empty-tag drops and rendered-name collision drops (all counted).
//! 11. [`Tags::Drop`] drops the tag set entirely (counted).
//! 12. A `Sum`'s temporality and monotonicity are dropped; the value goes on the wire bare. A
//!     **named normalization, not a skip**: unlike `prometheus_out`, which skips a delta `Sum`
//!     because exposition has a competing cumulative meaning, carbon's wire has no opinion on
//!     either, so the number is carried faithfully and only the model's extra facts are lost.
//!
//! Everything else is an error or a counted drop, never a silent reinterpretation.

pub mod decode;
pub mod encode;
pub mod pickle;

pub use decode::GraphiteDecoder;
pub use encode::{EncodeStats, GraphiteEncoder};

/// Carbon's own plaintext listener port (`LINE_RECEIVER_PORT`), for both `graphite_in` and
/// `graphite_out`.
pub const DEFAULT_PLAINTEXT_PORT: u16 = 2003;

/// Carbon's own pickle listener port (`PICKLE_RECEIVER_PORT`). TCP only -- the length-prefixed
/// framing has no meaning in a datagram, and a graph rule rejects the combination.
pub const DEFAULT_PICKLE_PORT: u16 = 2004;

/// The longest single UDP datagram `graphite_out` packs plaintext lines into: a 1500-byte Ethernet
/// MTU minus the IPv4 and UDP headers and headroom, as `statsd_out` uses. Bounds a whole datagram
/// (several `\n`-joined lines); a line that alone exceeds it is dropped whole, since half a line is
/// a corrupt series.
pub const DEFAULT_MAX_PACKET_BYTES: usize = 1432;

/// The longest plaintext line `graphite_in` assembles before draining to the next `\n`. (Twisted's
/// `LineReceiver` defaults to 16384, which carbon keeps.) 8 KiB is past any real tagged path and
/// keeps one hostile connection from growing an unbounded read buffer.
pub const DEFAULT_MAX_LINE_BYTES: usize = 8192;

/// Twisted's `Int32StringReceiver.MAX_LENGTH`, which carbon's pickle receiver inherits: a frame
/// declaring more is refused and the connection dropped. Both the decode and the pack bound, so a
/// relay never writes a frame the far end would refuse.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1 << 20;

/// The deepest nesting the restricted pickle reader follows, one open `MARK` per level. A real
/// carbon payload reaches 2 (the list, then a datapoint tuple).
pub const MAX_PICKLE_DEPTH: usize = 16;

/// The most values the restricted pickle reader holds at once, bounding the stack, each of the
/// two arenas, and the memo independently. 500k is more than [`DEFAULT_MAX_FRAME_BYTES`] can
/// encode; the cap stops a crafted frame from making the reader's own `Vec`s the denial of service.
pub const MAX_PICKLE_ITEMS: usize = 500_000;

/// Which carbon wire protocol a decoder reads or an encoder writes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// `path[;k=v...] value timestamp\n`, TCP or UDP.
    #[default]
    Plaintext,
    /// A 4-byte big-endian length prefix then a pickled `[(path, (timestamp, value)), ...]`. TCP
    /// only.
    Pickle,
}

/// Whether `graphite_out` renders attributes as carbon tags at all.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Tags {
    /// Render `;name=value` per attribute, ascending rendered name -- carbon 1.1+'s tag syntax.
    #[default]
    Carbon,
    /// Emit no tag segment, for a pre-1.1 Graphite, whose whisper backend would take the `;` into
    /// a directory name. Each dropped tag is counted `logit.output.tags.dropped{reason="dialect"}`.
    Drop,
}

/// What `graphite_out` does with a metric kind carbon's one-number-per-datapoint wire cannot carry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MultiValue {
    /// Drop the record, counted `logit.output.metrics.skipped{metric_kind=…}`. The default, since
    /// a sub-path naming convention is one the receiver knows nothing about.
    #[default]
    Skip,
    /// Expand into the dotted sub-paths this module doc's table lists, counted
    /// `logit.output.metrics.degraded{metric_kind=…}` once per record.
    Expand,
}
