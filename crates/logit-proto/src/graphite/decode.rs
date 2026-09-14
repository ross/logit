//! Decoding carbon plaintext lines or one pickle batch payload into events -- the
//! `| Wire | Model |` half of [`super`]'s module doc, which is the spec for everything here.
//!
//! Both protocols converge on one function: whatever produced a `(path-field, value, timestamp)`
//! triple, [`push_datapoint`] is what turns it into an [`Event`]. The two decode paths differ only
//! in how they get there -- splitting a line on whitespace, or walking a restricted pickle stack
//! ([`super::pickle`]).
//!
//! **Framing is not this decoder's job.** `graphite_in` owns the read buffer, the `max_line_bytes`
//! drain-to-newline state and the 4-byte pickle length prefix; [`GraphiteDecoder::decode_into`] is
//! handed either a datagram/complete-lines slice or one already-unframed pickle payload. That is
//! why the `oversize_line`/`oversize_frame` rows of [`super`]'s decode table are counted there
//! rather than here.
//!
//! Every tag value is a zero-copy [`Bytes::slice`] of the input, so an event's attributes share the
//! receive buffer's allocation instead of copying out of it (`docs/design/memory.md` §2) -- the
//! same trick `crates/logit-inputs/src/statsd.rs`'s `slice_of` plays, and it works for the pickle
//! path too because [`super::pickle::PickleReader`] yields `&str`s borrowed from that same buffer.

use super::pickle::PickleReader;
use super::Protocol;
use crate::{CodecError, Decoder};
use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, Diagnostics, Event, MetricKind, MetricRecord, Resource, Scope, Telemetry, Value,
};
use std::fmt::Display;
use std::sync::Arc;

/// Nanoseconds per second -- the scale an ingress timestamp is widened by.
const NANOS_PER_SECOND: f64 = 1e9;

/// Carbon's "stamp this with receipt time" sentinel. Its own `MetricLineReceiver` treats a `-1`
/// timestamp as "now", which is why this is a documented model mapping rather than a bad timestamp.
const RECEIPT_TIME_SENTINEL: f64 = -1.0;

/// Decodes carbon plaintext lines or pickle batch payloads. Split out from `graphite_in` (W2) so
/// every grammar, tag and malformed-input test runs against this with no socket involved -- the
/// same split [`crate::collectd`] and `crates/logit-outputs/src/statsd.rs` already use.
#[derive(Debug)]
pub struct GraphiteDecoder {
    protocol: Protocol,
    /// One shared resource for every batch this decoder ever produces. **Not** one per sender or
    /// per path prefix: `logit_pipeline::BatchAccumulator::absorb` keys accumulation on
    /// `Arc::ptr_eq`, so minting a resource per datagram would split every batch
    /// ([`crate::collectd`]'s decoder documents the same constraint).
    resource: Arc<Resource>,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// Reusable pickle machine -- stack, arenas and memo cleared per frame, never reallocated, so a
    /// warm pickle decode allocates only the caller's `Vec<Event>`.
    pickle: PickleReader,
}

/// Hand-written, not derived: [`PickleReader`] is a reusable *scratch* machine, and a clone must
/// get a fresh one rather than a copy of whatever the original last left in it.
///
/// `Clone` at all because `graphite_in` runs on the shared TCP driver
/// (`crates/logit-inputs/src/tcp.rs`'s "`D: Clone` is load-bearing" doc section), which hands
/// every accepted connection its own decoder -- the pickle stack, arenas and memo are per-stream
/// state and must not be shared between connections. Deriving would be *correct* today only by
/// accident: every one of those five `Vec`s is cleared at the start of each frame
/// (`PickleReader::parse`'s five `clear()`s), so no cross-frame state survives to copy -- but the
/// derive would
/// also copy each one's spare capacity into every new connection, which is the opposite of the
/// point, and would silently start carrying real state the day the reader keeps anything across
/// frames. [`PickleReader::new`] instead: a connection warms its own arenas on its first frame.
///
/// `resource` stays one shared [`Arc`], deliberately: `logit_pipeline::BatchAccumulator::absorb`
/// keys accumulation on `Arc::ptr_eq`, so a resource per connection would stop two connections'
/// events ever sharing a batch downstream (the `resource` field's own doc). `diag` and
/// `telemetry` are shared handles too -- a `Diagnostics` clone shares its original's throttle
/// counts (`logit_core::Diagnostics`' type doc), which is what makes `bad_line` throttle per
/// listener rather than per connection.
impl Clone for GraphiteDecoder {
    fn clone(&self) -> Self {
        Self {
            protocol: self.protocol,
            resource: Arc::clone(&self.resource),
            diag: self.diag.clone(),
            telemetry: self.telemetry.clone(),
            pickle: PickleReader::new(),
        }
    }
}

impl GraphiteDecoder {
    /// A plaintext decoder over `resource`. Use [`GraphiteDecoder::with_protocol`] for pickle.
    pub fn new(resource: Arc<Resource>) -> Self {
        Self {
            protocol: Protocol::default(),
            resource,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            pickle: PickleReader::new(),
        }
    }

    /// Which carbon wire protocol this decoder reads. Decoder state rather than a per-call
    /// argument, because [`Decoder::decode_into`] has one signature for every implementor and a
    /// listener's protocol never changes mid-connection.
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// This decoder's own diagnostics handle. Public for [`crate::collectd::CollectdDecoder::diag`]'s
    /// reason: `graphite_in` lives in `logit-inputs` while this decoder lives here, so the
    /// regression test that `with_diagnostics` actually *reached* the decoder cannot use a
    /// crate-private accessor.
    pub fn diag(&self) -> &Diagnostics {
        &self.diag
    }

    pub fn protocol(&self) -> Protocol {
        self.protocol
    }
}

impl Decoder for GraphiteDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<Scope>>), CodecError> {
        // Destructured rather than reached through `self`: the pickle path hands a closure to
        // `PickleReader`, which already holds `&mut self.pickle`, so the closure cannot also
        // borrow `self`. Splitting the fields once keeps both paths reading the same way.
        let Self { protocol, diag, telemetry, pickle, .. } = self;
        let mut ctx = Ctx { telemetry, diag };

        match protocol {
            Protocol::Plaintext => decode_plaintext(&bytes, received_at, out, &mut ctx),
            Protocol::Pickle => decode_pickle(pickle, &bytes, received_at, out, &mut ctx)?,
        }

        // Carbon carries no OTLP instrumentation-scope concept -- `None`, always; and the resource
        // is this decoder's own shared one (see the `resource` field's doc).
        Ok((self.resource.clone(), None))
    }
}

/// Splits `bytes` into lines and decodes each independently.
///
/// **Per-line isolation**, the rule every line-oriented decoder in this workspace follows
/// (`crates/logit-inputs/src/statsd.rs`'s `decode_into`): a datagram routinely packs several
/// unrelated metrics, so one malformed line is counted and skipped rather than failing the whole
/// input. Splitting on the raw **bytes** rather than validating the datagram as UTF-8 first is what
/// makes that true of a non-UTF-8 line too: it costs that line, not its neighbours.
fn decode_plaintext(bytes: &Bytes, received_at: i64, out: &mut Vec<Event>, ctx: &mut Ctx) {
    for raw in bytes.as_ref().split(|b| *b == b'\n') {
        // `\r\n` framing: carbon's own Twisted `LineReceiver` delimits on either, so a `\r` here is
        // framing, not payload (normalization 9).
        let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
        if raw.iter().all(|b| b.is_ascii_whitespace()) {
            // Padding, a trailing newline, a keepalive. Nothing was lost, so nothing is counted.
            continue;
        }
        let Ok(line) = std::str::from_utf8(raw) else {
            ctx.skip("bad_line", format_args!("graphite: line is not valid utf-8; skipping it"));
            continue;
        };
        decode_line(bytes, line, received_at, out, ctx);
    }
}

/// One plaintext line: `path[;k=v...] value timestamp`.
///
/// Fields are checked in wire order -- path, value, timestamp -- so a line with more than one
/// problem reports the leftmost. [`str::split_whitespace`] collapses runs of spaces and tabs alike
/// (normalization 9) and splits on Unicode whitespace, matching what carbon's own
/// `line.strip().split()` does to a decoded `str`.
fn decode_line(bytes: &Bytes, line: &str, received_at: i64, out: &mut Vec<Event>, ctx: &mut Ctx) {
    let mut fields = line.split_whitespace();
    let (Some(path_field), Some(value_field), Some(timestamp_field), None) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        ctx.skip(
            "bad_line",
            format_args!("graphite: {line:?} is not three whitespace-separated fields; skipping"),
        );
        return;
    };

    let Ok(value) = value_field.parse::<f64>() else {
        ctx.skip(
            "bad_line",
            format_args!("graphite: {value_field:?} is not a number (line {line:?}); skipping"),
        );
        return;
    };

    let Some(timestamp) = parse_timestamp(timestamp_field, received_at, ctx) else { return };

    push_datapoint(bytes, path_field, value, timestamp, out, ctx);
}

/// Decodes one complete, already **unframed** pickle payload.
///
/// Unlike the plaintext path, a bad payload fails the whole call: there is no resync point inside a
/// pickle stack machine, and the caller (`graphite_in`) responds by dropping the frame. A
/// *wrong-shaped item* inside an otherwise good payload is the isolated case, and is counted
/// `logit.input.metrics.skipped{reason="bad_shape"}` without touching its neighbours.
fn decode_pickle(
    pickle: &mut PickleReader,
    bytes: &Bytes,
    received_at: i64,
    out: &mut Vec<Event>,
    ctx: &mut Ctx,
) -> Result<(), CodecError> {
    let result = pickle.read_datapoints(bytes.as_ref(), |path_field, timestamp, value| {
        let Some(timestamp) = resolve_timestamp(timestamp, received_at, ctx) else { return };
        push_datapoint(bytes, path_field, value, timestamp, out, ctx);
    });
    match result {
        Ok(skipped) => {
            if skipped > 0 {
                ctx.count_skipped("bad_shape", skipped);
            }
            Ok(())
        }
        Err(err) => {
            ctx.diag.warn_throttled(
                "bad_pickle",
                format_args!("graphite: {err}; dropping the whole frame"),
            );
            Err(err)
        }
    }
}

/// Splits `path_field` into its path and `;k=v` tags, and pushes one event carrying one
/// `Gauge` record -- the last step both protocols share.
///
/// A non-finite value and a malformed tag both skip the **whole** datapoint: carbon drops a NaN on
/// receipt itself, and its own `TaggedSeries.parse` raises on a malformed tag rather than dropping
/// the one tag (which would silently change the series identity a receiver keys on).
fn push_datapoint(
    bytes: &Bytes,
    path_field: &str,
    value: f64,
    timestamp: i64,
    out: &mut Vec<Event>,
    ctx: &mut Ctx,
) {
    if !value.is_finite() {
        ctx.skip(
            "non_finite_value",
            format_args!(
                "graphite: {path_field:?} carries {value}, which has no carbon wire form \
                 (carbon drops a NaN on receipt too); skipping"
            ),
        );
        return;
    }

    let mut attributes = AttrMap::new();
    let Some(path) = parse_tags(bytes, path_field, &mut attributes, ctx) else { return };
    if path.is_empty() {
        ctx.skip("bad_line", format_args!("graphite: {path_field:?} has an empty path; skipping"));
        return;
    }

    out.push(Event::metric(
        timestamp,
        attributes,
        MetricRecord::new(intern(path), MetricKind::Gauge(value)),
    ));
}

/// Fills `attributes` from `field`'s `;name=value` segments and returns the path before the first
/// `;`, or `None` when a segment is malformed (which skips the whole line).
///
/// A repeated key collapses to its **last** value, counted
/// `logit.input.tags.normalized{reason="duplicate_key"}` -- carbon's own `TaggedSeries.parse`
/// builds a `dict`, so this reproduces the wire's semantics rather than inventing one.
/// Deliberately *not* `statsd_in`'s fold into a [`Value::Array`]: keeping arrays out of this pair
/// is what stops the encode side's array→last-element rule from ever firing inside it
/// (normalization 5).
fn parse_tags<'a>(
    bytes: &Bytes,
    field: &'a str,
    attributes: &mut AttrMap,
    ctx: &mut Ctx,
) -> Option<&'a str> {
    let Some((path, tags)) = field.split_once(';') else { return Some(field) };
    for segment in tags.split(';') {
        let Some((name, value)) = segment.split_once('=') else {
            ctx.skip(
                "bad_tag",
                format_args!("graphite: tag segment {segment:?} has no '='; skipping the line"),
            );
            return None;
        };
        if name.is_empty() || value.is_empty() {
            ctx.skip(
                "bad_tag",
                format_args!(
                    "graphite: tag segment {segment:?} has an empty name or value; skipping \
                     the line"
                ),
            );
            return None;
        }
        let key = intern(name);
        if attributes.get_sym(key).is_some() {
            ctx.tag_normalized_duplicate(name);
        }
        attributes.insert_sym(key, Value::Str(slice_of(bytes, value)));
    }
    Some(path)
}

/// The wire timestamp field, parsed and widened to nanoseconds.
fn parse_timestamp(field: &str, received_at: i64, ctx: &mut Ctx) -> Option<i64> {
    let Ok(seconds) = field.parse::<f64>() else {
        ctx.skip(
            "bad_timestamp",
            format_args!("graphite: {field:?} is not a timestamp; skipping the line"),
        );
        return None;
    };
    resolve_timestamp(seconds, received_at, ctx)
}

/// Carbon's timestamp rules, shared by both protocols: `-1` means receipt time, any other
/// non-positive (or non-finite) value is a bad timestamp, and a positive one -- integral or
/// fractional -- widens to nanoseconds.
///
/// **Split arithmetic**, whole seconds and the sub-second remainder scaled separately, rather than
/// the obvious `seconds * 1e9`: `1700000000.25 * 1e9` is `1.70000000025e18`, past `f64`'s 2⁵³ of
/// integer precision, so the one-step product lands a few hundred nanoseconds off and a
/// `graphite_in -> graphite_out` fixed point would drift on every hop. [`crate::collectd`]'s
/// `cdtime_to_nanos` splits for the same reason. Both casts saturate (Rust's `as`), so a timestamp
/// past the year 2262 clamps rather than wrapping into the past.
fn resolve_timestamp(seconds: f64, received_at: i64, ctx: &mut Ctx) -> Option<i64> {
    if seconds == RECEIPT_TIME_SENTINEL {
        return Some(received_at);
    }
    if !seconds.is_finite() || seconds <= 0.0 {
        ctx.skip(
            "bad_timestamp",
            format_args!(
                "graphite: {seconds} is not a positive instant and is not carbon's -1 \
                 receipt-time sentinel; skipping the line"
            ),
        );
        return None;
    }
    let whole = seconds.trunc();
    let sub_nanos = ((seconds - whole) * NANOS_PER_SECOND).round() as i64;
    Some((whole as i64).saturating_mul(NANOS_PER_SECOND as i64).saturating_add(sub_nanos))
}

/// Reconstructs a [`Bytes`] sharing `bytes`'s allocation for `sub`, a substring derived from it by
/// ordinary `&str` slicing. The identical trick `crates/logit-inputs/src/statsd.rs`'s `slice_of`
/// and `syslog.rs`'s play, and it holds here for the same reason: `sub` is always obtained by
/// slicing a `&str` that was itself validated out of `bytes`, never copied or rebuilt, so the
/// pointer round trip always lands inside `bytes`'s own allocation.
fn slice_of(bytes: &Bytes, sub: &str) -> Bytes {
    let base = bytes.as_ref().as_ptr() as usize;
    let start = sub.as_ptr() as usize - base;
    bytes.slice(start..start + sub.len())
}

/// The telemetry/diagnostics pair every skip site needs, carried together so a skipped line is
/// counted and reported at once and can never be one but not the other
/// ([`crate::collectd::encode`]'s `Ctx` is the same idea on the egress side).
struct Ctx<'a> {
    telemetry: &'a Telemetry,
    diag: &'a mut Diagnostics,
}

impl Ctx<'_> {
    /// One skipped line/datapoint: `logit.input.metrics.skipped{reason}` plus a throttled
    /// diagnostic under the same key, which is what makes an operator's counter and their log line
    /// greppable by the same word.
    fn skip(&mut self, reason: &'static str, message: impl Display) {
        self.telemetry.count("logit.input.metrics.skipped", 1.0, &[("reason", reason)]);
        self.diag.warn_throttled(reason, message);
    }

    /// Several skips at once, with no diagnostic of its own -- the pickle `bad_shape` case, where
    /// the reader already knows the count and the individual items carry nothing worth logging.
    fn count_skipped(&mut self, reason: &'static str, n: usize) {
        self.telemetry.count("logit.input.metrics.skipped", n as f64, &[("reason", reason)]);
        self.diag.warn_throttled(
            reason,
            format_args!(
                "graphite: {n} pickle item(s) were not (path, (timestamp, value)); skipping them"
            ),
        );
    }

    fn tag_normalized_duplicate(&mut self, name: &str) {
        self.telemetry.count("logit.input.tags.normalized", 1.0, &[("reason", "duplicate_key")]);
        self.diag.warn_throttled(
            "duplicate_tag_key",
            format_args!("graphite: tag {name:?} repeats on one series; keeping the last value"),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{MetricKind, Registry};

    const RECEIVED_AT: i64 = 1_699_000_000_000_000_000;

    fn decode(input: &str) -> Vec<Event> {
        decode_counted(input).0
    }

    /// Decodes `input` through a plaintext decoder wired to a fresh [`Registry`], so a test can
    /// assert on both the events and the counters the same call produced.
    fn decode_counted(input: &str) -> (Vec<Event>, Arc<Registry>) {
        let registry = Registry::new();
        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()))
            .with_telemetry(registry.telemetry_for("graphite_in", "graphite_in", "listener"));
        let mut events = Vec::new();
        decoder
            .decode_into(Bytes::from(input.to_string()), RECEIVED_AT, &mut events)
            .expect("plaintext never fails as a whole");
        (events, registry)
    }

    /// The **total** recorded on `logit.input.metrics.skipped{reason}`. Drains, so call once.
    fn skipped_total(registry: &Registry, reason: &str) -> f64 {
        counter_total(registry, "logit.input.metrics.skipped", "reason", reason)
    }

    fn counter_total(registry: &Registry, metric: &str, tag: &str, tag_value: &str) -> f64 {
        let mut total = 0.0;
        for event in registry.drain(0) {
            if event.attributes.get(tag).and_then(|v| v.as_str()) != Some(tag_value) {
                continue;
            }
            for m in &event.metrics {
                if logit_core::interner::resolve(m.name) != metric {
                    continue;
                }
                match &m.kind {
                    MetricKind::Sum(sum) => total += sum.value,
                    other => panic!("expected a Sum counter, got {other:?}"),
                }
            }
        }
        total
    }

    fn gauge(event: &Event) -> f64 {
        match &event.metrics[0].kind {
            MetricKind::Gauge(v) => *v,
            other => panic!("expected a Gauge, got {other:?}"),
        }
    }

    fn name(event: &Event) -> &'static str {
        logit_core::interner::resolve(event.metrics[0].name)
    }

    fn attr(event: &Event, key: &str) -> Option<String> {
        event.attributes.get(key).and_then(|v| v.as_str()).map(str::to_string)
    }

    #[test]
    fn a_plain_line_becomes_one_gauge_event() {
        let events = decode("sys.cpu.user 0.5 1700000000\n");
        assert_eq!(events.len(), 1);
        assert_eq!(name(&events[0]), "sys.cpu.user");
        assert_eq!(gauge(&events[0]), 0.5);
        assert_eq!(events[0].timestamp, 1_700_000_000_000_000_000);
        assert_eq!(events[0].metrics.len(), 1, "one datapoint is one record, never several");
        assert!(events[0].log.is_none() && events[0].span.is_none());
    }

    #[test]
    fn a_tagged_line_becomes_event_attributes() {
        let events = decode("sys.cpu;env=prod;host=web-1 0.5 1700000000\n");
        assert_eq!(events.len(), 1);
        assert_eq!(name(&events[0]), "sys.cpu", "the path stops at the first ';'");
        assert_eq!(attr(&events[0], "env").as_deref(), Some("prod"));
        assert_eq!(attr(&events[0], "host").as_deref(), Some("web-1"));
    }

    /// Carbon's own `TaggedSeries.parse` builds a `dict`, so the last occurrence wins -- and the
    /// collapse is counted, because it is a real loss (normalization 5).
    #[test]
    fn a_repeated_tag_key_keeps_the_last_value_and_is_counted() {
        let (events, registry) = decode_counted("a.b;team=a;team=b 1 1700000000\n");
        assert_eq!(attr(&events[0], "team").as_deref(), Some("b"));
        assert_eq!(
            counter_total(&registry, "logit.input.tags.normalized", "reason", "duplicate_key"),
            1.0
        );
    }

    /// Carbon's own rule, not an invention: its `MetricLineReceiver` reads `-1` as "now".
    #[test]
    fn a_minus_one_timestamp_becomes_receipt_time() {
        let events = decode("a.b 1 -1\n");
        assert_eq!(events[0].timestamp, RECEIVED_AT);
    }

    #[test]
    fn a_fractional_timestamp_keeps_its_sub_second_part() {
        let events = decode("a.b 1 1700000000.25\n");
        assert_eq!(events[0].timestamp, 1_700_000_000_250_000_000);
    }

    #[test]
    fn a_non_finite_value_is_skipped_and_counted() {
        for line in ["a.b NaN 1700000000", "a.b inf 1700000000", "a.b -inf 1700000000"] {
            let (events, registry) = decode_counted(&format!("{line}\n"));
            assert!(events.is_empty(), "{line}");
            assert_eq!(skipped_total(&registry, "non_finite_value"), 1.0, "{line}");
        }
    }

    #[test]
    fn a_non_positive_timestamp_other_than_minus_one_is_rejected() {
        for line in ["a.b 1 0", "a.b 1 -2", "a.b 1 -0.5"] {
            let (events, registry) = decode_counted(&format!("{line}\n"));
            assert!(events.is_empty(), "{line}");
            assert_eq!(skipped_total(&registry, "bad_timestamp"), 1.0, "{line}");
        }
    }

    #[test]
    fn an_unparseable_timestamp_is_rejected_as_a_bad_timestamp() {
        let (events, registry) = decode_counted("a.b 1 yesterday\n");
        assert!(events.is_empty());
        assert_eq!(skipped_total(&registry, "bad_timestamp"), 1.0);
    }

    #[test]
    fn two_and_four_field_lines_are_rejected() {
        for line in ["a.b 1", "a.b 1 1700000000 extra", "a.b"] {
            let (events, registry) = decode_counted(&format!("{line}\n"));
            assert!(events.is_empty(), "{line}");
            assert_eq!(skipped_total(&registry, "bad_line"), 1.0, "{line}");
        }
    }

    #[test]
    fn an_unparseable_value_is_rejected_as_a_bad_line() {
        let (events, registry) = decode_counted("a.b twelve 1700000000\n");
        assert!(events.is_empty());
        assert_eq!(skipped_total(&registry, "bad_line"), 1.0);
    }

    /// Normalization 9: a run of spaces, a tab, or a mix of both separates fields exactly like one
    /// space, and leading/trailing whitespace is framing rather than payload.
    #[test]
    fn tabs_and_runs_of_spaces_separate_fields() {
        for line in ["a.b\t1\t1700000000", "a.b   1  1700000000", "  a.b 1 1700000000  "] {
            let events = decode(&format!("{line}\n"));
            assert_eq!(events.len(), 1, "{line}");
            assert_eq!(name(&events[0]), "a.b", "{line}");
            assert_eq!(gauge(&events[0]), 1.0, "{line}");
        }
    }

    #[test]
    fn a_crlf_line_ending_is_trimmed() {
        let events = decode("a.b 1 1700000000\r\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].timestamp, 1_700_000_000_000_000_000);
    }

    /// Padding and a trailing newline are not losses, so they are skipped **uncounted** -- an
    /// operator watching `metrics.skipped` must not see traffic that was never a metric.
    #[test]
    fn empty_and_whitespace_only_lines_are_skipped_uncounted() {
        let (events, registry) = decode_counted("\n\n   \n\t\na.b 1 1700000000\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(skipped_total(&registry, "bad_line"), 0.0);
    }

    #[test]
    fn a_malformed_tag_rejects_the_whole_line() {
        for line in [
            "a.b;novalue 1 1700000000",
            "a.b;=novalue 1 1700000000",
            "a.b;name= 1 1700000000",
            "a.b;env=prod;bad 1 1700000000",
        ] {
            let (events, registry) = decode_counted(&format!("{line}\n"));
            assert!(events.is_empty(), "{line}");
            assert_eq!(skipped_total(&registry, "bad_tag"), 1.0, "{line}");
        }
    }

    /// Per-line isolation over raw bytes: a non-UTF-8 line costs itself, not its neighbours.
    #[test]
    fn a_non_utf8_line_is_rejected_without_taking_its_neighbours_down() {
        let registry = Registry::new();
        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()))
            .with_telemetry(registry.telemetry_for("graphite_in", "graphite_in", "listener"));
        let mut input = Vec::new();
        input.extend_from_slice(b"good.one 1 1700000000\n");
        input.extend_from_slice(b"bad.\xff.path 2 1700000000\n");
        input.extend_from_slice(b"good.two 3 1700000000\n");
        let mut events = Vec::new();
        decoder.decode_into(Bytes::from(input), RECEIVED_AT, &mut events).unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(name(&events[0]), "good.one");
        assert_eq!(name(&events[1]), "good.two");
        assert_eq!(skipped_total(&registry, "bad_line"), 1.0);
    }

    #[test]
    fn many_lines_become_many_events_in_wire_order() {
        let events = decode("a.b 1 1700000000\nc.d 2 1700000001\ne.f 3 1700000002\n");
        assert_eq!(events.len(), 3);
        assert_eq!(
            events.iter().map(name).collect::<Vec<_>>(),
            vec!["a.b", "c.d", "e.f"],
            "datapoint order within a datagram is preserved"
        );
    }

    /// One shared `Arc<Resource>` per decoder, not one per datagram -- what keeps
    /// `BatchAccumulator`'s `Arc::ptr_eq` keying from splitting a batch per packet.
    #[test]
    fn every_datagram_shares_one_resource() {
        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        let (first, _) = decoder
            .decode_into(Bytes::from_static(b"a.b 1 1\n"), RECEIVED_AT, &mut events)
            .unwrap();
        let (second, scope) = decoder
            .decode_into(Bytes::from_static(b"c.d 2 2\n"), RECEIVED_AT, &mut events)
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(scope.is_none(), "carbon carries no instrumentation scope");
    }

    /// The zero-copy claim, pinned: a tag value must *point into* the datagram, not copy out of it.
    #[test]
    fn a_tag_value_slices_the_input_rather_than_copying_it() {
        let bytes = Bytes::from_static(b"a.b;host=web-1 1 1700000000\n");
        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        decoder.decode_into(bytes.clone(), RECEIVED_AT, &mut events).unwrap();

        let Some(Value::Str(value)) = events[0].attributes.get("host") else {
            panic!("expected a Str tag value");
        };
        let base = bytes.as_ref().as_ptr() as usize;
        let at = value.as_ptr() as usize;
        assert!(
            at >= base && at + value.len() <= base + bytes.len(),
            "the tag value must be a slice of the datagram, not a copy"
        );
    }

    // -- pickle ---------------------------------------------------------------------------------

    fn decode_pickle_payload(payload: &[u8]) -> (Vec<Event>, Arc<Registry>) {
        let registry = Registry::new();
        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()))
            .with_protocol(Protocol::Pickle)
            .with_telemetry(registry.telemetry_for("graphite_in", "graphite_in", "listener"));
        let mut events = Vec::new();
        decoder
            .decode_into(Bytes::copy_from_slice(payload), RECEIVED_AT, &mut events)
            .expect("the payload must decode");
        (events, registry)
    }

    #[test]
    fn a_pickle_frame_becomes_one_event_per_datapoint() {
        let mut payload = Vec::new();
        super::super::pickle::write_datapoints(
            &mut payload,
            [("sys.cpu;host=web-1", 1_700_000_000i64, 0.5f64), ("sys.mem", 1_700_000_001, 2.0)],
        );
        let (events, _) = decode_pickle_payload(&payload);
        assert_eq!(events.len(), 2);
        assert_eq!(name(&events[0]), "sys.cpu");
        assert_eq!(attr(&events[0], "host").as_deref(), Some("web-1"));
        assert_eq!(gauge(&events[0]), 0.5);
        assert_eq!(events[0].timestamp, 1_700_000_000_000_000_000);
        assert_eq!(name(&events[1]), "sys.mem");
        assert_eq!(events[1].timestamp, 1_700_000_001_000_000_000);
    }

    /// A pickle path is a `&str` borrowed out of the frame, so the same zero-copy tag rule holds
    /// on this side too.
    #[test]
    fn a_pickle_tag_value_slices_the_frame() {
        let mut payload = Vec::new();
        super::super::pickle::write_datapoints(
            &mut payload,
            [("sys.cpu;host=web-1", 1_700_000_000i64, 0.5f64)],
        );
        let frame = Bytes::from(payload);
        let mut decoder =
            GraphiteDecoder::new(Arc::new(Resource::default())).with_protocol(Protocol::Pickle);
        let mut events = Vec::new();
        decoder.decode_into(frame.clone(), RECEIVED_AT, &mut events).unwrap();

        let Some(Value::Str(value)) = events[0].attributes.get("host") else {
            panic!("expected a Str tag value");
        };
        let base = frame.as_ref().as_ptr() as usize;
        assert!((value.as_ptr() as usize) >= base);
        assert!(value.as_ptr() as usize + value.len() <= base + frame.len());
    }

    /// `pickle.dumps([('a.b', ('1700000000', '2.5'))], protocol=2)`, generated on the host with
    /// CPython -- a producer that read its numbers out of text and never coerced them, which carbon
    /// accepts because it applies `float()` itself.
    #[test]
    fn a_pickle_numeric_string_value_decodes() {
        const PAYLOAD: &[u8] = &[
            0x80, 0x02, 0x5d, 0x71, 0x00, 0x58, 0x03, 0x00, 0x00, 0x00, 0x61, 0x2e, 0x62, 0x71,
            0x01, 0x58, 0x0a, 0x00, 0x00, 0x00, 0x31, 0x37, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
            0x30, 0x30, 0x71, 0x02, 0x58, 0x03, 0x00, 0x00, 0x00, 0x32, 0x2e, 0x35, 0x71, 0x03,
            0x86, 0x71, 0x04, 0x86, 0x71, 0x05, 0x61, 0x2e,
        ];
        let (events, _) = decode_pickle_payload(PAYLOAD);
        assert_eq!(events.len(), 1);
        assert_eq!(gauge(&events[0]), 2.5);
        assert_eq!(events[0].timestamp, 1_700_000_000_000_000_000);
    }

    /// `pickle.dumps([('a.b', (1, 1.0)), None, ('c.d', (2, 2.0))], protocol=2)` -- a stray `None`
    /// costs that datapoint and nothing else.
    #[test]
    fn a_wrong_shaped_pickle_item_is_skipped_and_counted_while_the_rest_decodes() {
        const PAYLOAD: &[u8] = &[
            0x80, 0x02, 0x5d, 0x71, 0x00, 0x28, 0x58, 0x03, 0x00, 0x00, 0x00, 0x61, 0x2e, 0x62,
            0x71, 0x01, 0x4b, 0x01, 0x47, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86,
            0x71, 0x02, 0x86, 0x71, 0x03, 0x4e, 0x58, 0x03, 0x00, 0x00, 0x00, 0x63, 0x2e, 0x64,
            0x71, 0x04, 0x4b, 0x02, 0x47, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86,
            0x71, 0x05, 0x86, 0x71, 0x06, 0x65, 0x2e,
        ];
        let (events, registry) = decode_pickle_payload(PAYLOAD);
        assert_eq!(events.len(), 2);
        assert_eq!(name(&events[0]), "a.b");
        assert_eq!(name(&events[1]), "c.d");
        assert_eq!(skipped_total(&registry, "bad_shape"), 1.0);
    }

    /// A forbidden opcode fails the whole frame -- there is no resync point in a pickle stack
    /// machine -- and the failure is reported under the greppable `bad_pickle` key.
    #[test]
    fn a_forbidden_pickle_opcode_fails_the_whole_frame() {
        const GLOBAL: &[u8] = &[
            0x80, 0x02, 0x63, 0x5f, 0x5f, 0x6d, 0x61, 0x69, 0x6e, 0x5f, 0x5f, 0x0a, 0x54, 0x68,
            0x69, 0x6e, 0x67, 0x0a, 0x71, 0x00, 0x29, 0x81, 0x71, 0x01, 0x2e,
        ];
        let mut decoder =
            GraphiteDecoder::new(Arc::new(Resource::default())).with_protocol(Protocol::Pickle);
        let mut events = Vec::new();
        let err = decoder
            .decode_into(Bytes::from_static(GLOBAL), RECEIVED_AT, &mut events)
            .expect_err("GLOBAL must fail the frame");
        assert!(err.to_string().contains("is not permitted"), "{err}");
        assert!(events.is_empty());
    }

    #[test]
    fn a_pickle_minus_one_timestamp_becomes_receipt_time() {
        let mut payload = Vec::new();
        super::super::pickle::write_datapoints(&mut payload, [("a.b", -1i64, 1.0f64)]);
        let (events, _) = decode_pickle_payload(&payload);
        assert_eq!(events[0].timestamp, RECEIVED_AT);
    }

    #[test]
    fn a_pickle_non_finite_value_is_skipped_and_counted() {
        let mut payload = Vec::new();
        super::super::pickle::write_datapoints(&mut payload, [("a.b", 1_700_000_000i64, f64::NAN)]);
        let (events, registry) = decode_pickle_payload(&payload);
        assert!(events.is_empty());
        assert_eq!(skipped_total(&registry, "non_finite_value"), 1.0);
    }
}
