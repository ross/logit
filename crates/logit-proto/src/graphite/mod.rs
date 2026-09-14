//! The Graphite/Carbon codec, both directions and both wire protocols: [`GraphiteDecoder`] turns
//! carbon plaintext lines or one pickle batch payload into events, [`GraphiteEncoder`] packs a
//! batch of events back into lines or pickle frames. The pickle writer and the restricted pickle
//! reader live next door in [`pickle`]; nothing in [`decode`] or [`encode`] knows a pickle opcode.
//!
//! **This module doc is the mapping table** (house convention, see [`crate::collectd`]'s,
//! [`crate::prometheus`]'s and [`crate::otlp`]'s module docs).
//! [ADR `graphite-carbon-relay`](../../../../docs/adr/graphite-carbon-relay.md) is the decision
//! record; [`docs/plans/graphite-carbon-relay.md`](../../../../docs/plans/graphite-carbon-relay.md)
//! is the workstream plan. Both carry the same numbered normalization list this doc ends with --
//! identical numbering, on purpose (the collectd lesson: three copies that drift are worse than
//! one).
//!
//! ## Wire shape, in one paragraph
//!
//! Carbon speaks two protocols for the same four facts -- a dotted path, an optional tag set, one
//! number, one whole second. **Plaintext** (TCP or UDP, port [`DEFAULT_PLAINTEXT_PORT`]) is
//! `path[;k=v...] value timestamp\n`, three whitespace-separated fields per line. **Pickle** (TCP
//! only, port [`DEFAULT_PICKLE_PORT`]) is a 4-byte **big-endian** length prefix -- Twisted's
//! `Int32StringReceiver`, whose `MAX_LENGTH` is [`DEFAULT_MAX_FRAME_BYTES`] -- followed by a
//! pickled `[(path, (timestamp, value)), ...]`. There is no type, no unit, no temporality and no
//! description anywhere on either wire: a datapoint is a number at a second, which is why the model
//! side of this codec is [`logit_core::MetricKind::Gauge`] and nothing else.
//!
//! ## No `graphite.*` namespace, and no well-known attributes
//!
//! Unlike [`crate::collectd`] (five sticky identity parts plus an interval), [`crate::syslog`]-era
//! headers or [`crate::prometheus`]'s family types, **this pair adds no `pub const ATTR_*` and
//! defines no `graphite.*` attribute namespace at all**. The wire carries exactly four facts and
//! each is represented once in the model with no lossy normalization: the path *is*
//! [`logit_core::MetricRecord::name`], the tags *are* event attributes, the number *is* the
//! `Gauge` payload, the second *is* [`logit_core::Event::timestamp`]. Rule (b) of
//! [ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md) -- "the raw, protocol-native
//! fact rides alongside the normalized model field and wins on the way back out" -- exists to
//! resolve a conflict between a wire fact and a normalized model field, and here there is no such
//! conflict to outrank. A carrier would be a second spelling of something already stored exactly
//! once.
//!
//! The consequence is worth stating plainly because it is the one place this pair is *less*
//! forgiving than collectd or syslog: since there is no `graphite.path` carrier, a `lua`/`set`
//! stage that renames [`logit_core::MetricRecord::name`] silently changes the wire path. That is
//! the intended way to rename a series (there is no `prefix:`/`template:` field on either
//! component either), and `docs/deploying.md` says so.
//!
//! ## Decode: wire → model
//!
//! One [`logit_core::Event`] carrying exactly one [`logit_core::MetricRecord`] per line (plaintext)
//! or per datapoint (pickle). Tags are **event** attributes, never resource ones -- the
//! `Arc::ptr_eq` accumulator keying [`crate::collectd`]'s decoder documents, and
//! `syslog.hostname`'s precedent.
//!
//! | Wire | Model | Counter / diag |
//! |---|---|---|
//! | one line / one pickle datapoint | one `Event`, one `MetricRecord` | -- |
//! | `path` (everything before the first `;`) | `name = intern(path)`, `Gauge(v)` | -- |
//! | `;name=value` | event attribute, `Value::Str` over a zero-copy [`bytes::Bytes`] slice of the input | -- |
//! | repeated tag key | last occurrence wins | `logit.input.tags.normalized{reason="duplicate_key"}` |
//! | finite value (`3`, `-1.5`, `1e5`, or a pickle `"3.14"` string) | `Gauge(v)` | -- |
//! | NaN / ±inf | line skipped | `logit.input.metrics.skipped{reason="non_finite_value"}` + diag `non_finite_value` |
//! | `timestamp == -1` | `received_at` (carbon's own rule) | -- |
//! | `timestamp > 0`, integral or fractional | `(ts * 1e9) as i64` | -- |
//! | any other `timestamp` (`<= 0`, non-finite, unparseable) | skipped | `logit.input.metrics.skipped{reason="bad_timestamp"}` + diag `bad_timestamp` |
//! | not exactly 3 whitespace-separated fields; an unparseable value; non-UTF-8 bytes; an empty path | skipped | `logit.input.metrics.skipped{reason="bad_line"}` + diag `bad_line` |
//! | malformed tag (a `;` segment with no `=`, an empty name, or an empty value) | the **whole line** is skipped -- carbon's own `TaggedSeries.parse` raises rather than dropping the one tag | `logit.input.metrics.skipped{reason="bad_tag"}` + diag `bad_tag` |
//! | empty / whitespace-only line | skipped, **uncounted** (packet padding, a trailing `\n`) | -- |
//! | line longer than `max_line_bytes` (TCP) | the reader drains to the next `\n`; the line after it still decodes | `logit.input.metrics.skipped{reason="oversize_line"}` + diag `oversize_line` |
//! | pickle frame longer than `max_frame_bytes` | the connection is closed -- there is no resync point in a length-framed stream | diag `oversize_frame` |
//! | a disallowed pickle opcode, or the depth/item caps | `CodecError::Malformed`, the whole frame is dropped | diag `bad_pickle` |
//! | a pickle item that is not `(str, (num, num))` | **that datapoint** is skipped; the rest of the frame still decodes | `logit.input.metrics.skipped{reason="bad_shape"}` |
//! | `Resource` / `Scope` | the decoder's own shared default / `None` | -- |
//!
//! The last three rows plus the two oversize rows are *shared* with `graphite_in`
//! (`crates/logit-inputs/src/graphite/`): framing is the listener's job, so it -- not
//! [`GraphiteDecoder`] -- counts `oversize_line`/`oversize_frame` and closes the connection. Every
//! other row is emitted here.
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
//! Attributes in the `statsd.`/`collectd.` namespaces are **skipped uncounted**, exactly as
//! `crates/logit-outputs/src/influxdb.rs`'s `render_tag_suffix` skips `statsd.`: those are another
//! protocol's consumed carriers, not tags anybody asked to see on this wire. Resource attributes
//! *do* become tags (the `influxdb_out`/`statsd_out` rule) -- a bare `graphite_in` resource is
//! empty, so the pair stays a fixed point; cross-protocol it is a `docs/known-gaps.md` row.
//!
//! ## Sanitization
//!
//! Substitute `_`, **never delete** -- `crates/logit-outputs/src/statsd.rs`'s `sanitize_into`, for
//! its reason: distinct inputs stay distinct in the common case, where deletion would silently
//! merge `a b` and `ab` into one series. Collisions are the residue, and are resolved on the
//! **rendered** names, never on interner order (ADR `prometheus-scrape-and-exposition`'s
//! `:220-224`).
//!
//! | Field | Forbidden → `_` |
//! |---|---|
//! | path | whitespace ([`char::is_whitespace`]), [`char::is_control`], `;`, `/`, `\` |
//! | tag name | `;`, `!`, `^`, `=`, whitespace, control |
//! | tag value | `;`, whitespace, control; and a **leading** `~` only |
//!
//! `/` and `\` are whisper's directory separators -- a path component containing one would create a
//! nested directory rather than a series segment. There is **no path truncation**: carbon has no
//! length bound, and whisper's 255-byte filesystem component limit is a `docs/known-gaps.md` row
//! rather than something this codec silently enforces. A leading `~` is reserved by carbon's tag
//! grammar; a `~` anywhere else in a tag value is legal and rides through untouched.
//!
//! Whitespace is [`char::is_whitespace`] (Unicode), not just ASCII: carbon's plaintext receiver
//! splits a *decoded* `str` with Python's `str.split()`, which splits on Unicode whitespace, and
//! [`decode`] mirrors that with [`str::split_whitespace`]. Sanitizing only ASCII whitespace would
//! let a U+00A0 in a path encode fine and then decode as a four-field line -- a broken fixed point.
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
//! `.sum` **is** emitted for a sketch: [`logit_core::DdSketch::sum`] is exact (the inner crate
//! accumulates it as a plain `f64` alongside the bins, and adds the two sums on `merge`), not an
//! estimate like a quantile. `crate::prometheus`'s module doc claims the opposite; that claim is
//! stale and is tracked as a follow-up rather than fixed here.
//!
//! The quantiles are [`crate::otlp::metrics::DISTRIBUTION_QUANTILES`] -- the same five every
//! sketch-to-quantiles degradation in this crate reports, so one metric describes itself
//! identically at `otlp_out`, `prometheus_out` and `graphite_out`. `influxdb_out`'s own narrower
//! `[0.5, 0.9, 0.99]` set is deliberately left alone.
//!
//! **Number tokens are injective.** A quantile or a bucket bound is formatted with Rust's `{}`
//! (`Display` for `f64`) and then has every `.` substituted with `_`: `0.99 → q0_99`,
//! `1.5 → bucket_1_5`, `-0.5 → bucket_-0_5`, `f64::INFINITY → bucket_inf`. `Display` emits only
//! `-`, decimal digits, at most one `.`, and the literals `inf`/`-inf`/`NaN`, so the substitution
//! is a bijection on the strings it can produce -- two distinct bounds can never render to one
//! token. That is what satisfies `crates/logit-outputs/src/influxdb.rs`'s collision argument
//! (`render_fields`' doc comment: a *rounded* percentile is rejected precisely because `0.991` and
//! `0.994` would both become `p99`). Graphite forces the substitution that InfluxDB does not need,
//! because `.` is the hierarchy separator here.
//!
//! ## `Protocol` and `Meta`
//!
//! [`GraphiteEncoder`] implements [`crate::FramedEncoder`], not [`crate::Encoder`] -- for
//! [`crate::collectd`]'s reason, restated: `Encoder` hands back one opaque `Bytes` per batch with
//! no framing metadata, and both carbon protocols genuinely need per-message boundaries the
//! transport re-chooses (a UDP datagram packs whole lines up to `max_packet_bytes`; a pickle
//! stream writes one length-prefixed frame at a time).
//!
//! `type Meta = usize` is the **datapoint count** of each message, for the reason collectd's
//! `MessageBuf<usize>` carries a value-list count: a sink that fails to send one message has to
//! attribute the loss to the right number of metrics, and a message is not one metric here. Under
//! [`Protocol::Plaintext`] every entry is one line and its meta is always `1`; under
//! [`Protocol::Pickle`] every entry is one **complete, already length-prefixed frame** (the 4-byte
//! big-endian prefix is written by the encoder, so the sink's send path is a single `write_all`
//! per entry) and its meta is however many datapoints that frame carries. A new frame opens when
//! the next datapoint would push the pickle payload past `max_frame_bytes`.
//!
//! `Protocol`/`Tags`/`MultiValue` and both size caps are **encoder state**, set once through the
//! `with_*` builders at construction, never per-call arguments -- [`crate::FramedEncoder`] has one
//! signature for every implementor. The one combination the codec does not police is pickle over
//! UDP: that is rejected by a graph rule (`crates/logit-pipeline/src/graph.rs`), not here, because
//! the codec never sees a transport.
//!
//! ## Permitted normalizations
//!
//! `graphite_in -> graphite_out` is a fixed point modulo exactly this list (the round-trip tests in
//! `crates/logit-proto/tests/graphite_fixed_point.rs` pin it). The numbering is identical in the
//! ADR and the plan:
//!
//! 1. Re-framing: lines are repacked into datagrams or stream writes, and datapoints into pickle
//!    frames of at most `max_frame_bytes`, so one input frame may leave as several and vice versa.
//! 2. An operator-chosen dialect change: plaintext ↔ pickle, in either direction.
//! 3. Datapoint reordering within a batch (everything downstream of a batch boundary is
//!    order-insensitive by design).
//! 4. Tag order is canonicalized to ascending rendered name -- carbon's own `TaggedSeries.format`
//!    sorts too, so this is the wire's canonical order, not an invention.
//! 5. A repeated tag key collapses to its **last** occurrence at decode, counted. Carbon's
//!    `TaggedSeries.parse` builds a `dict`, which is the same rule. Deliberately *not*
//!    `statsd_in`'s fold into a `Value::Array`: keeping arrays out of the pair is what stops
//!    normalization 10's array→last-element rule from ever firing inside it.
//! 6. Timestamps floor to whole seconds on egress (`div_euclid`, so a negative instant floors
//!    downward rather than toward zero).
//! 7. A `-1` timestamp becomes receipt time on ingress, and leaves as that absolute second.
//! 8. Number formatting becomes the shortest round-trip `f64` rendering: `1.50`, `1.5e0` and
//!    `+1.5` all leave as `1.5`, and `3.0` leaves as `3`.
//! 9. Field separators collapse to a single space, `\r\n` to `\n`; a TCP write terminates every
//!    line including the last, a UDP datagram terminates none.
//! 10. Sanitizer substitutions, empty-tag drops and rendered-name collision drops (all counted).
//! 11. [`Tags::Drop`] drops the tag set entirely (counted).
//! 12. A `Sum`'s temporality and monotonicity are dropped -- the value goes on the wire bare. A
//!     **named normalization, not a skip**: unlike `prometheus_out`, which skips a delta `Sum`
//!     because exposition genuinely has a competing cumulative meaning, carbon's wire has no
//!     opinion about either, so the number is carried faithfully and only the model's extra facts
//!     are lost.
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

/// The longest single UDP datagram `graphite_out` will pack plaintext lines into: a 1500-byte
/// Ethernet MTU minus the IPv4 and UDP headers minus headroom, the same figure `statsd_out` uses
/// (`crates/logit-outputs/src/statsd.rs`). Bounds a whole datagram (several `\n`-joined lines), and
/// a line that alone exceeds it is dropped whole rather than split -- half a line is a corrupt
/// series, not a partial one.
pub const DEFAULT_MAX_PACKET_BYTES: usize = 1432;

/// The longest plaintext line `graphite_in` will assemble before draining to the next `\n`. Carbon
/// itself has no such bound (Twisted's `LineReceiver` defaults to 16384 and carbon does not raise
/// it); 8 KiB is comfortably past any real tagged path and keeps one hostile connection from
/// growing an unbounded read buffer.
pub const DEFAULT_MAX_LINE_BYTES: usize = 8192;

/// Twisted's `Int32StringReceiver.MAX_LENGTH`, which is what carbon's pickle receiver inherits: a
/// frame declaring more than this is refused and the connection dropped. Used as both the decode
/// bound (`graphite_in`) and the pack bound (`graphite_out`), so a relay never writes a frame the
/// far end would refuse.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1 << 20;

/// The deepest nesting the restricted pickle reader will follow -- one open `MARK` per level. A
/// real carbon payload reaches 2 (the list, then a datapoint tuple); everything past a handful is
/// an attempt to make the reader recurse or allocate, not a sender.
pub const MAX_PICKLE_DEPTH: usize = 16;

/// The most values the restricted pickle reader will hold at once -- bounds the stack, each of the
/// two arenas, and the memo independently. 500k datapoints is ~7 MB of `f64`s and far past
/// [`DEFAULT_MAX_FRAME_BYTES`] can encode anyway; the cap exists so a crafted frame cannot make the
/// reader's own `Vec`s the denial of service rather than the frame's declared lengths.
pub const MAX_PICKLE_ITEMS: usize = 500_000;

/// Which carbon wire protocol a decoder reads or an encoder writes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// `path[;k=v...] value timestamp\n`, TCP or UDP. Carbon's own default listener.
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
    /// Emit no tag segment. The escape hatch for a pre-1.1 Graphite, whose whisper backend would
    /// otherwise take the `;` into a directory name silently. Every dropped tag is counted
    /// `logit.output.tags.dropped{reason="dialect"}`.
    Drop,
}

/// What `graphite_out` does with a metric kind carbon's one-number-per-datapoint wire cannot carry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MultiValue {
    /// Drop the record, counted `logit.output.metrics.skipped{metric_kind=…}`. The default: a
    /// carbon datapoint is one number, and inventing a naming convention a receiver knows nothing
    /// about is the kind of silent reinterpretation this codec does not do unasked.
    #[default]
    Skip,
    /// Expand into the dotted sub-paths this module doc's table lists, counted
    /// `logit.output.metrics.degraded{metric_kind=…}` once per record. Opt-in, and named: the
    /// operator is choosing a convention, not receiving one.
    Expand,
}
