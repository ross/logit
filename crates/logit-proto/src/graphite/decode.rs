//! Decoding carbon plaintext lines or one pickle batch payload into events: the decode half of
//! [`super`]'s module doc, which is the spec for everything here.
//!
//! Both protocols converge on [`push_datapoint`], which turns a `(path-field, value, timestamp)`
//! triple into an [`Event`]; they differ only in splitting a line or walking a restricted pickle
//! stack ([`super::pickle`]).
//!
//! **Framing is not this decoder's job.** `graphite_in`'s listener, through the shared TCP driver's
//! `Framer` (`crates/logit-inputs/src/tcp.rs`), owns the read buffer, the `max_line_bytes` drain,
//! and the 4-byte pickle length prefix. [`GraphiteDecoder::decode_into`] gets a whole UDP datagram
//! (possibly several lines) or one delimited message: a plaintext line or an unframed pickle
//! payload. Line splitting still runs, since a UDP datagram may carry several lines.
//!
//! Every tag value is a zero-copy [`Bytes::slice`] of the input (`docs/design/memory.md` §2),
//! including on the pickle path, because [`super::pickle::PickleReader`] yields `&str`s borrowed
//! from the same buffer.

use super::pickle::PickleReader;
use super::Protocol;
use crate::{CodecError, Decoder};
use bytes::Bytes;
use logit_core::interner::{intern, KeyCache};
use logit_core::{
    AttrMap, Diagnostics, Event, MetricKind, MetricRecord, Resource, Scope, Telemetry, Value,
};
use std::fmt::Display;
use std::sync::Arc;

/// Nanoseconds per second -- the scale an ingress timestamp is widened by.
const NANOS_PER_SECOND: f64 = 1e9;

/// Carbon's receipt-time sentinel: its `MetricLineReceiver` treats a `-1` timestamp as "now".
const RECEIPT_TIME_SENTINEL: f64 = -1.0;

/// Decodes carbon plaintext lines or pickle batch payloads, with no socket, so grammar, tag, and
/// malformed-input tests run against it directly.
#[derive(Debug)]
pub struct GraphiteDecoder {
    protocol: Protocol,
    /// One shared resource for every batch, **not** one per sender:
    /// `logit_pipeline::BatchAccumulator::absorb` keys accumulation on `Arc::ptr_eq`, so a
    /// resource per datagram would split every batch.
    resource: Arc<Resource>,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// Reusable pickle machine, cleared per frame, so a warm pickle decode allocates only the
    /// caller's `Vec<Event>`.
    pickle: PickleReader,
    /// Tag *keys* seen so far, memoised `&str -> Symbol`: a tagged stream repeats a handful of
    /// tag names, so each is one `memcmp` instead of an interner probe. Paths still use `intern`:
    /// a stream carries thousands of distinct ones, which would fill the cache's cap and then pay
    /// its scan on every line.
    keys: KeyCache,
}

/// Hand-written, not derived: a clone gets a fresh [`PickleReader`] and `KeyCache`, not a copy.
///
/// The shared TCP driver clones one decoder per accepted connection
/// (`crates/logit-inputs/src/tcp.rs`'s "`D: Clone`" section), and the pickle scratch is
/// per-stream state. A derive would copy spare capacity into every connection, and would carry
/// real state the day the reader keeps anything across frames.
///
/// `resource` stays one shared [`Arc`] (the field's doc), and `diag`/`telemetry` are shared
/// handles: a `Diagnostics` clone shares its throttle counts, so `bad_line` throttles per listener,
/// not per connection.
impl Clone for GraphiteDecoder {
    fn clone(&self) -> Self {
        Self {
            protocol: self.protocol,
            resource: Arc::clone(&self.resource),
            diag: self.diag.clone(),
            telemetry: self.telemetry.clone(),
            pickle: PickleReader::new(),
            // Fresh, like `pickle`: per-stream state.
            keys: KeyCache::new(),
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
            keys: KeyCache::new(),
        }
    }

    /// Which carbon wire protocol this decoder reads. Decoder state, since
    /// [`Decoder::decode_into`] has one signature and a listener's protocol never changes.
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

    /// This decoder's diagnostics handle, public for
    /// [`crate::collectd::CollectdDecoder::diag`]'s reason.
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
        // Destructured: the pickle path's closure can't borrow `self` while `PickleReader` holds
        // `&mut self.pickle`.
        let Self { protocol, diag, telemetry, pickle, keys, .. } = self;
        let mut ctx = Ctx { telemetry, diag, keys };

        match protocol {
            Protocol::Plaintext => decode_plaintext(&bytes, received_at, out, &mut ctx),
            Protocol::Pickle => decode_pickle(pickle, &bytes, received_at, out, &mut ctx)?,
        }

        // Carbon has no instrumentation-scope concept.
        Ok((self.resource.clone(), None))
    }
}

/// Splits `bytes` into lines and decodes each independently.
///
/// **Per-line isolation**, as in every line-oriented decoder here: one malformed line is counted
/// and skipped. Splitting raw **bytes** before UTF-8 validation means a non-UTF-8 line costs only
/// itself.
fn decode_plaintext(bytes: &Bytes, received_at: i64, out: &mut Vec<Event>, ctx: &mut Ctx) {
    for raw in bytes.as_ref().split(|b| *b == b'\n') {
        // Carbon's `LineReceiver` accepts `\r\n`, so a `\r` is framing (normalization 9).
        let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
        if raw.iter().all(|b| b.is_ascii_whitespace()) {
            // Padding, a trailing newline, a keepalive: nothing lost, nothing counted.
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
/// [`str::split_whitespace`] collapses runs (normalization 9) and splits on Unicode whitespace, as
/// carbon's `line.strip().split()` does to a decoded `str`.
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
/// A bad payload fails the whole call (a pickle stack machine has no resync point), and the caller
/// drops the frame. A *wrong-shaped item* in a good payload costs only itself, counted
/// `logit.input.metrics.skipped{reason="bad_shape"}`.
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

/// Splits `path_field` into its path and `;k=v` tags, and pushes one event carrying one `Gauge`
/// record.
///
/// A non-finite value or a malformed tag skips the **whole** datapoint: carbon drops a NaN on
/// receipt, and `TaggedSeries.parse` raises on a malformed tag rather than drop it (which would
/// change the series identity).
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
/// `logit.input.tags.normalized{reason="duplicate_key"}` (normalization 5).
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
        let key = ctx.keys.get_or_intern(name);
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

/// Carbon's timestamp rules, for both protocols: `-1` means receipt time, any other non-positive
/// or non-finite value is a bad timestamp, and a positive one (integral or fractional) widens to
/// nanoseconds.
///
/// **Split arithmetic**, whole seconds and remainder scaled separately: `1700000000.25 * 1e9` is
/// past `f64`'s 2⁵³ integer precision, so the one-step product lands a few hundred nanoseconds off
/// and the fixed point would drift every hop. Both casts saturate, so a timestamp past 2262 clamps
/// rather than wrapping into the past.
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

/// A [`Bytes`] sharing `bytes`'s allocation for `sub`.
///
/// Sound only because every caller's `sub` is sliced from a `&str` validated out of `bytes`, never
/// copied, so the pointer arithmetic lands inside `bytes`. The statsd and syslog decoders have
/// their own copies of this.
fn slice_of(bytes: &Bytes, sub: &str) -> Bytes {
    let base = bytes.as_ref().as_ptr() as usize;
    let start = sub.as_ptr() as usize - base;
    bytes.slice(start..start + sub.len())
}

/// The telemetry/diagnostics pair every skip site needs, carried together so a skip is never
/// counted but not reported, or the reverse.
struct Ctx<'a> {
    telemetry: &'a Telemetry,
    diag: &'a mut Diagnostics,
    /// The decoder's tag-key cache (`GraphiteDecoder::keys`).
    keys: &'a mut KeyCache,
}

impl Ctx<'_> {
    /// One skipped line/datapoint: `logit.input.metrics.skipped{reason}` plus a throttled
    /// diagnostic under the same key, so the counter and the log line grep by one word.
    fn skip(&mut self, reason: &'static str, message: impl Display) {
        self.telemetry.count("logit.input.metrics.skipped", 1.0, &[("reason", reason)]);
        self.diag.warn_throttled(reason, message);
    }

    /// Several skips at once, with one summary diagnostic: the pickle `bad_shape` case, where the
    /// reader returns only a count.
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

    /// Decodes `input` through a plaintext decoder wired to a fresh [`Registry`], returning both.
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

    /// Tag keys hit the decoder's `KeyCache` on a repeat line; the path never enters it.
    /// (`nextest` runs each test in its own process, so `interner::len()` is this test's alone.)
    #[test]
    fn repeat_tag_keys_are_cache_hits_and_paths_are_not_cached() {
        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        let line = |s: &str| Bytes::from(s.to_string());
        decoder
            .decode_into(
                line("gr.cache.a;gtag_env=prod;gtag_host=web-1 1 1700000000\n"),
                RECEIVED_AT,
                &mut events,
            )
            .expect("plaintext never fails as a whole");
        assert_eq!(decoder.keys.len(), 2, "two tag keys, no path");

        let before = logit_core::interner::len();
        decoder
            .decode_into(
                line("gr.cache.a;gtag_host=web-2;gtag_env=dev 2 1700000000\n"),
                RECEIVED_AT,
                &mut events,
            )
            .expect("plaintext never fails as a whole");
        assert_eq!(logit_core::interner::len(), before, "same path, same tag keys: nothing new");
        assert_eq!(decoder.keys.len(), 2);
        assert_eq!(attr(&events[1], "gtag_env").as_deref(), Some("dev"));
        assert_eq!(attr(&events[1], "gtag_host").as_deref(), Some("web-2"));

        decoder
            .decode_into(line("gr.cache.b;gtag_env=prod 3 1700000000\n"), RECEIVED_AT, &mut events)
            .expect("plaintext never fails as a whole");
        assert_eq!(logit_core::interner::len(), before + 1, "a new path is interned ...");
        assert_eq!(decoder.keys.len(), 2, "... but never cached");
    }

    /// Normalization 5: the last occurrence of a repeated key wins, counted.
    #[test]
    fn a_repeated_tag_key_keeps_the_last_value_and_is_counted() {
        let (events, registry) = decode_counted("a.b;team=a;team=b 1 1700000000\n");
        assert_eq!(attr(&events[0], "team").as_deref(), Some("b"));
        assert_eq!(
            counter_total(&registry, "logit.input.tags.normalized", "reason", "duplicate_key"),
            1.0
        );
    }

    /// A `-1` timestamp is receipt time, as carbon's `MetricLineReceiver` reads it.
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

    /// Normalization 9: any whitespace run separates fields, and leading/trailing whitespace is
    /// framing.
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

    /// Padding and a trailing newline are skipped **uncounted**: they were never metrics.
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

    /// One shared `Arc<Resource>` per decoder, not one per datagram.
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

    /// A tag value *points into* the datagram rather than copying out of it.
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

    /// Pickle tag values are zero-copy too.
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

    /// `pickle.dumps([('a.b', ('1700000000', '2.5'))], protocol=2)`, generated with CPython:
    /// numeric strings, which carbon accepts because it applies `float()` itself.
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

    /// `pickle.dumps([('a.b', (1, 1.0)), None, ('c.d', (2, 2.0))], protocol=2)`: a stray `None`
    /// costs only that datapoint.
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

    /// A forbidden opcode fails the whole frame, reported under `bad_pickle`.
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
