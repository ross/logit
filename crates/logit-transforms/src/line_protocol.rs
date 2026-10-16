//! `line_protocol`: parses an event's log message as InfluxDB line protocol and emits one event
//! per point through [`Transform::process_into`], for a producer that wraps line protocol in
//! another protocol instead of writing it to InfluxDB's API
//! (`docs/adr/line-protocol-transform.md`).
//!
//! The mapping from a point to an event is `influxdb_in`'s, built by the same code
//! ([`InfluxDecoder::decode_skipping`]); `crates/logit-proto/src/influxdb/mod.rs`'s module doc is
//! its table. This module doc is the canonical copy of what the transform adds around it.
//!
//! **Input.** The log message, when it's a `Value::Str` or `Value::Bytes`. An event with no log,
//! or with a message of another type, is forwarded untouched and uncounted.
//!
//! **Per line.**
//!
//! - Each point is one event, pushed onto `emitted` in line order. It starts from a copy of the
//!   parent's attributes; the point's tags, its measurement carrier, and its string fields are
//!   written over that copy, so the point wins a key collision. The parent's log, metrics, and
//!   span aren't copied, and the batch's resource, scope, trace context, and provenance are the
//!   parent's.
//! - A point's timestamp is scaled to nanoseconds by `precision`. A point with no timestamp takes
//!   the parent's `timestamp` as is, not truncated to `precision`: it's a recorded time, not an
//!   arrival.
//! - Points of one message with the same measurement, tag set, and timestamp merge into one
//!   event, as in one `influxdb_in` body. Points of different messages never merge.
//! - A line is bad when it fails the grammar or its scaled timestamp is outside the range
//!   `influxdb_in` accepts. A bad line is skipped, and the rest of the message is still parsed.
//!   Blank and comment lines are neither points nor bad.
//!
//! **The parent's fate.**
//!
//! - A message with no point (every line bad, blank, or a comment) forwards the parent untouched,
//!   whatever `keep_source` says.
//! - Under `keep_source: true`, the parent is forwarded unchanged beside its points.
//! - Under `keep_source: false`, the default, the parent's log is removed. A parent with no
//!   metrics or span left is discarded (the runtime counts `logit.component.events.discarded`);
//!   one that still carries either is forwarded without its log, so a payload this transform
//!   didn't read is never lost.
//!
//! **Counters**, emitted once per batch from `end_batch`:
//!
//! | Counter | When |
//! |---|---|
//! | `logit.transform.lines.dropped{reason="malformed"}` | a bad line in a message whose log doesn't go on: its text has left the pipeline, so it's loss |
//! | `logit.transform.lines.skipped{reason="malformed"}` | a bad line in a message whose log goes on (`keep_source: true`, or no point) |
//! | `logit.transform.tags.omitted{reason="reserved_prefix"}` | a tag key under `_logit.` or `influxdb.field.`, left off its point |
//!
//! Each bad line also reports the throttled diagnostic `parse_error`, with its 1-based line number
//! and the parser's reason. It never quotes the line: a diagnostic is the process's own log, and
//! the line is producer data.

use logit_core::{Diagnostics, Event, Resource, Telemetry, Value};
use logit_pipeline::Transform;
use logit_proto::influxdb::line::Reason;
use logit_proto::influxdb::{InfluxDecoder, Precision};
use std::sync::Arc;

pub struct LineProtocol {
    precision: Precision,
    keep_source: bool,
    decoder: InfluxDecoder,
    /// One message's bad lines, kept until the message's point count decides which counter they
    /// go to.
    bad: Vec<(usize, Reason)>,
    tally: Tally,
    diag: Diagnostics,
    telemetry: Telemetry,
}

/// One batch's counts, emitted and reset by `end_batch`, so a message costs no telemetry lock.
#[derive(Default)]
struct Tally {
    lines_dropped: usize,
    lines_skipped: usize,
    tags_omitted: usize,
}

impl LineProtocol {
    pub fn new(precision: Precision, keep_source: bool) -> Self {
        Self {
            precision,
            keep_source,
            decoder: InfluxDecoder::new(),
            bad: Vec::new(),
            tally: Tally::default(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }
}

impl Transform for LineProtocol {
    /// The runtime calls only [`Transform::process_into`]; this answers for a caller with nowhere
    /// to put the points, and forwards the event untouched.
    fn process(&mut self, _resource: &Arc<Resource>, _event: &mut Event) -> bool {
        true
    }

    fn process_into(
        &mut self,
        _resource: &Arc<Resource>,
        event: &mut Event,
        emitted: &mut Vec<Event>,
    ) -> bool {
        let Some(log) = &event.log else { return true };
        let body = match &log.message {
            Value::Str(b) | Value::Bytes(b) => b.clone(),
            _ => return true,
        };

        let bad = &mut self.bad;
        bad.clear();
        self.decoder.decode_skipping(
            &body,
            self.precision,
            event.timestamp,
            &event.attributes,
            emitted,
            |error| bad.push((error.line, error.reason)),
        );
        let stats = self.decoder.stats();
        self.tally.tags_omitted += stats.tags_omitted_reserved_prefix;

        let has_points = stats.points > 0;
        let log_goes_on = self.keep_source || !has_points;
        for (line, reason) in self.bad.drain(..) {
            if log_goes_on {
                self.tally.lines_skipped += 1;
            } else {
                self.tally.lines_dropped += 1;
            }
            self.diag.warn_throttled(
                "parse_error",
                format_args!("line {line} of a message isn't line protocol, skipped: {reason}"),
            );
        }

        if log_goes_on {
            return true;
        }
        event.log = None;
        !event.metrics.is_empty() || event.span.is_some()
    }

    fn end_batch(&mut self) {
        let tally = std::mem::take(&mut self.tally);
        let counts = [
            ("logit.transform.lines.dropped", "malformed", tally.lines_dropped),
            ("logit.transform.lines.skipped", "malformed", tally.lines_skipped),
            ("logit.transform.tags.omitted", "reserved_prefix", tally.tags_omitted),
        ];
        for (name, reason, count) in counts {
            if count > 0 {
                self.telemetry.count(name, count as f64, &[("reason", reason)]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use logit_core::interner::{intern, resolve};
    use logit_core::{
        AttrMap, BodyFormat, EventBatch, LogRecord, MetricKind, MetricRecord, MetricValue,
        SpanKind, SpanRecord, SpanStatus,
    };
    use logit_pipeline::runtime::process_batch;
    use logit_pipeline::test_util::TelemetryProbe;
    use logit_proto::influxdb::ATTR_MEASUREMENT;

    /// A firmware's metrics datagram after a script rewrote its timestamps to absolute
    /// nanoseconds: five points, one with two fields, one with a string field.
    const BODY: &str = "ttemp_noz,n=0,a=1 value=0i 1792167827767000000\n\
                        stp_stall v=1i 1792167827806000000\n\
                        heap free=61088i,total=79012i 1792167827896000000\n\
                        fan,fan=print state=0,pwm=0,measured=0 1792167828270000000\n\
                        fw_version v=\"7.0.0+16903\" 1792167828270000001\n";

    const PARENT_TS: i64 = 1_792_167_830_000_000_123;

    fn log_record(message: Value) -> LogRecord {
        LogRecord {
            message,
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        }
    }

    /// A log event carrying `message`, with `host` and `n` attributes for children to inherit.
    fn parent(message: &str) -> Event {
        let mut attributes = AttrMap::new();
        attributes.insert("host", Value::str("printer-1"));
        attributes.insert("n", Value::str("parent"));
        Event::log(PARENT_TS, attributes, log_record(Value::str(message)))
    }

    struct Run {
        events: Vec<Event>,
        probe: TelemetryProbe,
    }

    impl Run {
        fn sum(&mut self, name: &str, reason: &str) -> f64 {
            self.probe.sum(name, &[("reason", reason)])
        }

        fn discarded(&mut self) -> f64 {
            self.probe.sum("logit.component.events.discarded", &[])
        }

        fn diagnostics(&mut self, key: &str) -> f64 {
            self.probe.sum("logit.component.diagnostics", &[("key", key)])
        }
    }

    /// Runs `events` as one batch through a `line_protocol` node, as the runtime does.
    fn drive(precision: Precision, keep_source: bool, events: Vec<Event>) -> Run {
        let probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("points", "line_protocol", "transform");
        let mut transform = LineProtocol::new(precision, keep_source)
            .with_diagnostics(Diagnostics::new("points").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry.clone());
        let batch = EventBatch { resource: Arc::new(Resource::default()), scope: None, events };
        let mut scratch = Vec::new();
        let out = process_batch(&mut transform, batch, &mut scratch, &telemetry);
        assert!(scratch.is_empty(), "the scratch is appended to the batch");
        Run { events: out.map(|b| b.events).unwrap_or_default(), probe }
    }

    fn metrics(event: &Event) -> Vec<(&'static str, MetricKind)> {
        event.metrics.iter().map(|m| (resolve(m.name), m.kind.clone())).collect()
    }

    fn str_attr<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
        event.attributes.get(key).and_then(Value::as_str)
    }

    fn g(v: MetricValue) -> MetricKind {
        MetricKind::Gauge(v)
    }

    fn measurement(event: &Event) -> Option<&str> {
        str_attr(event, ATTR_MEASUREMENT)
    }

    #[test]
    fn a_message_becomes_one_event_per_point_in_line_order() {
        let mut run = drive(Precision::Ns, false, vec![parent(BODY)]);
        let events = &run.events;
        assert_eq!(
            events.iter().map(measurement).collect::<Vec<_>>(),
            [Some("ttemp_noz"), Some("stp_stall"), Some("heap"), Some("fan"), Some("fw_version")],
            "the parent is discarded and each point is a child, in order"
        );
        assert_eq!(
            events.iter().map(|e| e.timestamp).collect::<Vec<_>>(),
            [
                1_792_167_827_767_000_000,
                1_792_167_827_806_000_000,
                1_792_167_827_896_000_000,
                1_792_167_828_270_000_000,
                1_792_167_828_270_000_001
            ]
        );
        assert!(events.iter().all(|e| e.log.is_none() && e.span.is_none()));

        let ttemp = &events[0];
        assert_eq!(metrics(ttemp), [("ttemp_noz.value", g(MetricValue::I64(0)))]);
        assert_eq!(str_attr(ttemp, "a"), Some("1"), "a tag is an attribute");
        assert_eq!(str_attr(ttemp, "n"), Some("0"), "the point's tag wins over the parent's");
        assert_eq!(str_attr(ttemp, "host"), Some("printer-1"), "the parent's attribute rides");

        assert_eq!(
            metrics(&events[2]),
            [("heap.free", g(MetricValue::I64(61088))), ("heap.total", g(MetricValue::I64(79012)))],
            "a multi-field point is one event with a gauge per field, in wire order"
        );
        assert_eq!(
            metrics(&events[3]),
            [
                ("fan.state", g(MetricValue::F64(0.0))),
                ("fan.pwm", g(MetricValue::F64(0.0))),
                ("fan.measured", g(MetricValue::F64(0.0)))
            ]
        );
        assert_eq!(str_attr(&events[3], "fan"), Some("print"));
        assert_eq!(str_attr(&events[3], "n"), Some("parent"), "no tag here, so the parent's");

        let version = &events[4];
        assert!(version.metrics.is_empty(), "a string field isn't a metric");
        assert_eq!(str_attr(version, "influxdb.field.v"), Some("7.0.0+16903"));

        assert_eq!(run.discarded(), 1.0, "the parent, with nothing left, is discarded");
        assert_eq!(run.sum("logit.transform.lines.dropped", "malformed"), 0.0);
    }

    #[test]
    fn a_bytes_message_parses_as_a_str_one_does() {
        let mut event = parent("");
        event.log.as_mut().unwrap().message = Value::Bytes(Bytes::from_static(b"m f=1i 5"));
        let run = drive(Precision::Ns, false, vec![event]);
        assert_eq!(run.events.len(), 1);
        assert_eq!(metrics(&run.events[0]), [("m.f", g(MetricValue::I64(1)))]);
    }

    #[test]
    fn points_of_one_series_and_time_merge_within_a_message_only() {
        let run = drive(
            Precision::Ns,
            false,
            vec![parent("m,t=1 x=1i 5\nm,t=1 y=2i 5\nm,t=1 x=9i 5"), parent("m,t=1 x=3i 5")],
        );
        assert_eq!(run.events.len(), 2, "one event per message");
        assert_eq!(
            metrics(&run.events[0]),
            [("m.x", g(MetricValue::I64(9))), ("m.y", g(MetricValue::I64(2)))]
        );
        assert_eq!(metrics(&run.events[1]), [("m.x", g(MetricValue::I64(3)))]);
    }

    #[test]
    fn precision_scales_a_timestamp_to_nanoseconds() {
        let run = drive(Precision::Us, false, vec![parent("m f=1i 1792167827767000")]);
        assert_eq!(run.events[0].timestamp, 1_792_167_827_767_000_000);
        let run = drive(Precision::S, false, vec![parent("m f=1i 1792167827")]);
        assert_eq!(run.events[0].timestamp, 1_792_167_827_000_000_000);
    }

    #[test]
    fn a_point_with_no_timestamp_takes_the_parents_untruncated() {
        let run = drive(Precision::S, false, vec![parent("m f=1i\nn f=2i 7")]);
        assert_eq!(run.events[0].timestamp, PARENT_TS, "not truncated to seconds");
        assert_eq!(run.events[1].timestamp, 7_000_000_000);
    }

    #[test]
    fn a_bad_line_is_skipped_and_dropped_when_the_log_does_not_go_on() {
        let mut run = drive(Precision::Ns, false, vec![parent("a f=1i 1\nb f=\nc f=3i 3\n")]);
        assert_eq!(
            run.events.iter().map(measurement).collect::<Vec<_>>(),
            [Some("a"), Some("c")],
            "the lines around the bad one still parse"
        );
        assert_eq!(run.sum("logit.transform.lines.dropped", "malformed"), 1.0);
        assert_eq!(run.sum("logit.transform.lines.skipped", "malformed"), 0.0);
        assert_eq!(run.diagnostics("parse_error"), 1.0);
    }

    #[test]
    fn a_timestamp_out_of_range_is_a_bad_line() {
        let mut run = drive(Precision::S, false, vec![parent("a f=1i 9223372037\nb f=1i 1")]);
        assert_eq!(run.events.iter().map(measurement).collect::<Vec<_>>(), [Some("b")]);
        assert_eq!(run.sum("logit.transform.lines.dropped", "malformed"), 1.0);
    }

    #[test]
    fn under_keep_source_a_bad_line_is_skipped_and_the_parent_goes_on_unchanged() {
        let message = "a f=1i 1\nb f=\nc f=3i 3\n";
        let original = parent(message);
        let mut run = drive(Precision::Ns, true, vec![original.clone()]);
        assert_eq!(run.events.len(), 3, "the parent and two points");
        assert_eq!(run.events[0], original, "the parent is forwarded unchanged, first");
        assert_eq!(
            run.events[1..].iter().map(measurement).collect::<Vec<_>>(),
            [Some("a"), Some("c")]
        );
        assert_eq!(run.sum("logit.transform.lines.skipped", "malformed"), 1.0);
        assert_eq!(run.sum("logit.transform.lines.dropped", "malformed"), 0.0);
        assert_eq!(run.discarded(), 0.0);
    }

    #[test]
    fn a_message_with_no_point_forwards_the_parent_untouched_either_way() {
        for keep_source in [false, true] {
            let original = parent("not line protocol\n# a comment\n\nb f=\n");
            let mut run = drive(Precision::Ns, keep_source, vec![original.clone()]);
            assert_eq!(run.events, [original], "keep_source: {keep_source}");
            assert_eq!(
                run.sum("logit.transform.lines.skipped", "malformed"),
                2.0,
                "the log goes on, so its bad lines are skipped, not dropped"
            );
            assert_eq!(run.sum("logit.transform.lines.dropped", "malformed"), 0.0);
            assert_eq!(run.discarded(), 0.0);
        }
    }

    #[test]
    fn a_parent_still_carrying_a_metric_or_span_is_forwarded_without_its_log() {
        let mut with_metric = parent("m f=1i 1");
        with_metric.metrics.push(MetricRecord::new(intern("other"), MetricKind::counter(1.0)));
        let mut with_span = parent("m f=1i 1");
        with_span.span = Some(SpanRecord {
            trace_id: [1; 16],
            span_id: [1; 8],
            parent_span_id: None,
            name: Value::str("span"),
            kind: SpanKind::Internal,
            status: SpanStatus::Unset,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 0,
            flags: 0,
            ext: None,
        });
        let mut run = drive(Precision::Ns, false, vec![with_metric, with_span]);
        assert_eq!(run.events.len(), 4, "both parents, then a point from each");
        assert!(run.events[0].log.is_none());
        assert_eq!(metrics(&run.events[0]), [("other", MetricKind::counter(1.0))]);
        assert!(run.events[1].log.is_none());
        assert!(run.events[1].span.is_some());
        assert_eq!(run.discarded(), 0.0);
    }

    #[test]
    fn an_event_with_no_text_log_passes_through_untouched_and_uncounted() {
        let metric = Event::metric(
            1,
            AttrMap::new(),
            MetricRecord::new(intern("m"), MetricKind::counter(1.0)),
        );
        let mut numeric = parent("");
        numeric.log.as_mut().unwrap().message = Value::I64(5);
        let input = vec![metric, numeric];
        let mut run = drive(Precision::Ns, false, input.clone());
        assert_eq!(run.events, input);
        assert_eq!(run.discarded(), 0.0);
        assert!(!run.probe.poll().has("logit.transform.lines.skipped", &[]));
        assert!(!run.probe.poll().has("logit.transform.lines.dropped", &[]));
    }

    #[test]
    fn a_reserved_prefix_tag_is_omitted_and_counted() {
        let mut run =
            drive(Precision::Ns, false, vec![parent("m,_logit.x=1,influxdb.field.y=2,t=3 f=1i 1")]);
        let event = &run.events[0];
        assert_eq!(event.attributes.get("_logit.x"), None);
        assert_eq!(event.attributes.get("influxdb.field.y"), None);
        assert_eq!(str_attr(event, "t"), Some("3"));
        assert_eq!(run.sum("logit.transform.tags.omitted", "reserved_prefix"), 2.0);
    }
}
