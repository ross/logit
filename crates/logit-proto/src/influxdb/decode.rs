//! Decoding one `/api/v2/write` body into one [`EventBatch`]: the decode half of [`super`]'s
//! module doc, which is the spec for everything here.
//!
//! **All or nothing.** Every line is parsed before any event is built, and the first bad line
//! fails the body, as influxd rejects a write with a parse error (probe row 13.1). Nothing is
//! counted for a failed body: none of it was taken.
//!
//! **Merging.** Points sharing a measurement, tag set, and timestamp are one point at influxd, so
//! they merge into one event, field by field, the later value winning and a new field appended.
//! The index is a reused hash map from a series-and-time hash to the event, with a collision
//! chain checked against the lines themselves, so a warm decoder allocates no index.
//!
//! [`InfluxDecoder::decode_body`] isn't [`crate::Decoder::decode_into`]: it needs the request's
//! `precision`, as `SplunkDecoder::decode_raw` needs its envelope, and it can fail as a whole.

use super::line::{
    lines, parse_line, FieldValue, Line, LineError, MAX_TIMESTAMP_NS, MIN_TIMESTAMP_NS,
};
use super::{Precision, ATTR_MEASUREMENT, FIELD_ATTR_PREFIX};
use bytes::Bytes;
use logit_core::attrs::is_private;
use logit_core::interner::{intern, KeyCache, Symbol};
use logit_core::subslice;
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, MetricValue, Resource, Telemetry, Value,
};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

/// No earlier event shares the hash.
const END_OF_CHAIN: u32 = u32::MAX;

/// What one successful [`InfluxDecoder::decode_body`] did, for tests and the listener's
/// accounting. Reset at the start of every call.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DecodeStats {
    /// Points parsed: lines that weren't blank or a comment.
    pub points: usize,
    /// Points merged into an earlier event of the same series and timestamp.
    pub merged: usize,
    /// Tags omitted for a key under the private `_logit.` prefix or the string-field prefix
    /// `influxdb.field.`, counted `logit.input.tags.omitted{reason="reserved_prefix"}`.
    pub tags_omitted_reserved_prefix: usize,
}

/// Decodes line-protocol write bodies, with no socket, so grammar and model tests run against it
/// directly.
#[derive(Debug)]
pub struct InfluxDecoder {
    telemetry: Telemetry,
    /// Tag and string-field keys, memoized as other decoders memoize theirs.
    keys: KeyCache,
    /// `_logit.influxdb.measurement`, interned once.
    measurement_key: Symbol,
    /// Scratch for `<measurement>.<field>` and `influxdb.field.<key>`.
    name: String,
    hasher: RandomState,
    /// Series-and-time hash to the latest event with that hash.
    index: HashMap<u64, u32>,
    /// Per event, the previous event with the same hash, or [`END_OF_CHAIN`].
    chain: Vec<u32>,
    stats: DecodeStats,
}

impl Default for InfluxDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Hand-written: a clone starts with empty scratch, as one decoder per connection should.
impl Clone for InfluxDecoder {
    fn clone(&self) -> Self {
        Self { telemetry: self.telemetry.clone(), ..Self::new() }
    }
}

impl InfluxDecoder {
    pub fn new() -> Self {
        Self {
            telemetry: Telemetry::default(),
            keys: KeyCache::new(),
            measurement_key: intern(ATTR_MEASUREMENT),
            name: String::new(),
            hasher: RandomState::new(),
            index: HashMap::new(),
            chain: Vec::new(),
            stats: DecodeStats::default(),
        }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// The last successful decode's counts.
    pub fn stats(&self) -> DecodeStats {
        self.stats
    }

    /// Decodes a whole write body into one batch under `resource`, or fails on the first line
    /// that doesn't parse, with nothing decoded.
    ///
    /// `received_at` is the request's arrival in Unix nanoseconds; a point with no timestamp gets
    /// it truncated to `precision`, one instant for every such point in the body (probe row 10.5).
    pub fn decode_body(
        &mut self,
        body: Bytes,
        precision: Precision,
        received_at: i64,
        resource: Arc<Resource>,
    ) -> Result<EventBatch, LineError> {
        self.stats = DecodeStats::default();
        let arrival = precision.truncate(received_at);

        let mut parsed: Vec<(Line<'_>, i64)> = Vec::new();
        for (number, raw) in lines(&body) {
            let fail = |reason| {
                let start = raw.iter().position(|b| !matches!(b, b' ' | b'\t' | 0)).unwrap_or(0);
                LineError { line: number, text: subslice::share(&body, &raw[start..]), reason }
            };
            let Some(line) = parse_line(raw).map_err(fail)? else { continue };
            let timestamp = match line.timestamp {
                None => arrival,
                Some(t) => match precision.scale_to_ns(t) {
                    Some(ns) if (MIN_TIMESTAMP_NS..=MAX_TIMESTAMP_NS).contains(&ns) => ns,
                    _ => {
                        return Err(fail(
                            "time outside range -9223372036854775806 - 9223372036854775806".into(),
                        ))
                    }
                },
            };
            parsed.push((line, timestamp));
        }
        self.stats.points = parsed.len();

        self.index.clear();
        self.chain.clear();
        let mut events: Vec<Event> = Vec::with_capacity(parsed.len());
        // Per event, the line that opened it: the series key the chain compares against.
        let mut opened_by: Vec<u32> = Vec::with_capacity(parsed.len());
        for (at, (line, timestamp)) in parsed.iter().enumerate() {
            let hash = self.hasher.hash_one((&line.measurement, &line.tags, timestamp));
            let mut cursor = self.index.get(&hash).copied().unwrap_or(END_OF_CHAIN);
            while cursor != END_OF_CHAIN {
                let (first, first_ts) = &parsed[opened_by[cursor as usize] as usize];
                if first_ts == timestamp
                    && first.measurement == line.measurement
                    && first.tags == line.tags
                {
                    break;
                }
                cursor = self.chain[cursor as usize];
            }
            if cursor == END_OF_CHAIN {
                let event = self.open_event(&body, line, *timestamp);
                let id = events.len() as u32;
                events.push(event);
                opened_by.push(at as u32);
                self.chain.push(self.index.insert(hash, id).unwrap_or(END_OF_CHAIN));
            } else {
                self.stats.merged += 1;
                self.merge_into(&body, &mut events[cursor as usize], line);
            }
        }

        if self.stats.tags_omitted_reserved_prefix > 0 {
            self.telemetry.count(
                "logit.input.tags.omitted",
                self.stats.tags_omitted_reserved_prefix as f64,
                &[("reason", "reserved_prefix")],
            );
        }
        // Line protocol has no instrumentation scope.
        Ok(EventBatch { resource, scope: None, events })
    }

    /// The first point of a series and timestamp: tags, the measurement carrier, string fields,
    /// and one `Gauge` per other field in wire order.
    fn open_event(&mut self, body: &Bytes, line: &Line<'_>, timestamp: i64) -> Event {
        let mut attributes = AttrMap::new();
        for (key, value) in &line.tags {
            if key.starts_with(FIELD_ATTR_PREFIX) || is_private(key) {
                self.stats.tags_omitted_reserved_prefix += 1;
                continue;
            }
            attributes.insert_sym(self.keys.get_or_intern(key), Value::Str(share(body, value)));
        }
        attributes.insert_sym(self.measurement_key, Value::Str(share(body, &line.measurement)));
        let mut event = Event::empty(timestamp, attributes);
        for (key, value) in &line.fields {
            match value {
                FieldValue::Str(s) => {
                    let sym = self.field_attr(key);
                    event.attributes.insert_sym(sym, Value::Str(share(body, s)));
                }
                _ => {
                    let name = self.metric_name(&line.measurement, key);
                    event.metrics.push(MetricRecord::new(name, gauge(value)));
                }
            }
        }
        event
    }

    /// A later point of the same series and timestamp: each field overwrites its namesake, even
    /// across types, or is appended (normalizations 2 and 4).
    fn merge_into(&mut self, body: &Bytes, event: &mut Event, line: &Line<'_>) {
        for (key, value) in &line.fields {
            let name = self.metric_name(&line.measurement, key);
            let existing = event.metrics.iter().position(|m| m.name == name);
            match value {
                FieldValue::Str(s) => {
                    if let Some(i) = existing {
                        event.metrics.remove(i);
                    }
                    let sym = self.field_attr(key);
                    event.attributes.insert_sym(sym, Value::Str(share(body, s)));
                }
                _ => {
                    let sym = self.field_attr(key);
                    event.attributes.remove_sym(sym);
                    match existing {
                        Some(i) => event.metrics[i].kind = gauge(value),
                        None => event.metrics.push(MetricRecord::new(name, gauge(value))),
                    }
                }
            }
        }
    }

    /// `<measurement>.<field>`, interned.
    fn metric_name(&mut self, measurement: &str, field: &str) -> Symbol {
        self.name.clear();
        self.name.push_str(measurement);
        self.name.push('.');
        self.name.push_str(field);
        intern(&self.name)
    }

    /// `influxdb.field.<key>`, interned.
    fn field_attr(&mut self, key: &str) -> Symbol {
        self.name.clear();
        self.name.push_str(FIELD_ATTR_PREFIX);
        self.name.push_str(key);
        self.keys.get_or_intern(&self.name)
    }
}

/// A slice of the body when `s` borrows it, else a copy of the unescaped text: an owned string
/// lies outside the body, which `subslice::share` detects.
fn share(body: &Bytes, s: &str) -> Bytes {
    subslice::share(body, s.as_bytes())
}

/// A numeric or boolean field as a `Gauge`.
fn gauge(value: &FieldValue<'_>) -> MetricKind {
    MetricKind::Gauge(match value {
        FieldValue::F64(v) => MetricValue::F64(*v),
        FieldValue::I64(v) => MetricValue::I64(*v),
        FieldValue::U64(v) => MetricValue::U64(*v),
        FieldValue::Bool(v) => MetricValue::Bool(*v),
        FieldValue::Str(_) => unreachable!("a string field is an attribute"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::Registry;

    const RECEIVED_AT: i64 = 1_700_000_000_123_456_789;

    fn decode_at(body: &str, precision: Precision) -> Result<EventBatch, LineError> {
        InfluxDecoder::new().decode_body(
            Bytes::from(body.to_string()),
            precision,
            RECEIVED_AT,
            Arc::new(Resource::default()),
        )
    }

    fn decode(body: &str) -> Vec<Event> {
        decode_at(body, Precision::Ns).expect("the body must decode").events
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

    #[test]
    fn a_point_is_one_event_with_typed_gauges_in_wire_order() {
        let events = decode(
            "cpu,host=a,dc=iad usage=0.5,count=3i,big=7u,up=true,note=\"hi\" 1700000000000000000",
        );
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.timestamp, 1_700_000_000_000_000_000);
        assert_eq!(
            metrics(e),
            [
                ("cpu.usage", g(MetricValue::F64(0.5))),
                ("cpu.count", g(MetricValue::I64(3))),
                ("cpu.big", g(MetricValue::U64(7))),
                ("cpu.up", g(MetricValue::Bool(true))),
            ]
        );
        assert_eq!(str_attr(e, "host"), Some("a"));
        assert_eq!(str_attr(e, "dc"), Some("iad"));
        assert_eq!(str_attr(e, ATTR_MEASUREMENT), Some("cpu"));
        assert_eq!(str_attr(e, "influxdb.field.note"), Some("hi"));
        assert_eq!(e.attributes.len(), 4);
    }

    #[test]
    fn a_string_only_point_has_no_metrics() {
        let events = decode("log msg=\"started\" 1");
        assert!(events[0].metrics.is_empty());
        assert_eq!(str_attr(&events[0], "influxdb.field.msg"), Some("started"));
    }

    // probe: influxd 2.9.1, rows 18.1 to 18.3 and 6.2
    #[test]
    fn duplicate_points_merge_last_write_wins_in_first_appearance_order() {
        let events = decode(
            "m,t=1 x=1i,s=\"a\" 5\n\
             m,t=1 y=2i 5\n\
             m,t=1 x=9i 5\n\
             m,t=2 x=1i 5\n\
             m,t=1 x=3i 6\n",
        );
        assert_eq!(events.len(), 3);
        assert_eq!(
            metrics(&events[0]),
            [("m.x", g(MetricValue::I64(9))), ("m.y", g(MetricValue::I64(2)))]
        );
        assert_eq!(str_attr(&events[0], "influxdb.field.s"), Some("a"));
        assert_eq!(str_attr(&events[1], "t"), Some("2"));
        assert_eq!(events[2].timestamp, 6);
    }

    // probe: influxd 2.9.1, row 18.3: tag order is irrelevant to the series key
    #[test]
    fn tag_order_does_not_split_a_series() {
        let events = decode("m,b=2,a=1 x=1i 5\nm,a=1,b=2 y=1i 5");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].metrics.len(), 2);
    }

    #[test]
    fn a_merge_follows_a_type_change_across_lines() {
        let events = decode("m f=1i,g=\"s\" 5\nm f=\"now a string\",g=2.5 5");
        assert_eq!(events.len(), 1);
        assert_eq!(metrics(&events[0]), [("m.g", g(MetricValue::F64(2.5)))]);
        assert_eq!(str_attr(&events[0], "influxdb.field.f"), Some("now a string"));
        assert_eq!(events[0].attributes.get("influxdb.field.g"), None);
    }

    // probe: influxd 2.9.1, rows 10.2 to 10.4
    #[test]
    fn precision_scales_timestamps_to_nanoseconds() {
        for (precision, ts) in [
            (Precision::Ns, "1700000000123456789"),
            (Precision::Us, "1700000000123456"),
            (Precision::Ms, "1700000000123"),
            (Precision::S, "1700000000"),
        ] {
            let batch = decode_at(&format!("m f=1i {ts}"), precision).unwrap();
            let want = ts.parse::<i64>().unwrap()
                * match precision {
                    Precision::Ns => 1,
                    Precision::Us => 1_000,
                    Precision::Ms => 1_000_000,
                    Precision::S => 1_000_000_000,
                };
            assert_eq!(batch.events[0].timestamp, want, "{precision:?}");
        }
    }

    // probe: influxd 2.9.1, rows 10.5 and 18.5: one truncated arrival instant per body
    #[test]
    fn an_absent_timestamp_is_the_arrival_truncated_to_precision() {
        let batch = decode_at("a f=1i\nb f=1i\na,t=1 f=2i", Precision::Ms).unwrap();
        assert!(batch.events.iter().all(|e| e.timestamp == 1_700_000_000_123_000_000));
        assert_eq!(batch.events.len(), 3);
        let batch = decode_at("a f=1i", Precision::S).unwrap();
        assert_eq!(batch.events[0].timestamp, 1_700_000_000_000_000_000);
        // Two timestamp-less points of one series merge (row 18.5).
        let batch = decode_at("p18_g x=1i\np18_g,t=1 x=1i\np18_g x=2i", Precision::Ns).unwrap();
        assert_eq!(batch.events.len(), 2);
        assert_eq!(metrics(&batch.events[0]), [("p18_g.x", g(MetricValue::I64(2)))]);
    }

    // probe: influxd 2.9.1, rows 10.7, 10.9 to 10.15, 10.17
    #[test]
    fn the_timestamp_range_is_checked_after_scaling() {
        let outside = "time outside range -9223372036854775806 - 9223372036854775806";
        for (body, precision) in [
            ("m f=1i 9223372036854775807", Precision::Ns),
            ("m f=1i 9223372037", Precision::S),
            ("m f=1i 9223372036855", Precision::Ms),
            ("m f=1i 10413792000", Precision::S),
            ("m f=1i -9223372036854775807", Precision::Ns),
        ] {
            assert_eq!(decode_at(body, precision).unwrap_err().reason, outside, "{body}");
        }
        for (body, precision, want) in [
            ("m f=1i 9223372036", Precision::S, 9_223_372_036_000_000_000),
            ("m f=1i -1", Precision::Ns, -1),
            ("m f=1i -1000", Precision::S, -1_000_000_000_000),
            ("m f=1i 0", Precision::Ns, 0),
            ("m f=1i 9223372036854775806", Precision::Ns, MAX_TIMESTAMP_NS),
        ] {
            assert_eq!(decode_at(body, precision).unwrap().events[0].timestamp, want, "{body}");
        }
    }

    // probe: influxd 2.9.1, rows 13.1 and 13.4
    #[test]
    fn one_bad_line_fails_the_whole_body_naming_the_first_bad_line() {
        let err =
            decode_at("p13_a f=1i\np13_b f=\np13_c f=3i\np13_d g", Precision::Ns).unwrap_err();
        assert_eq!(err.line, 2);
        assert_eq!(err.text, Bytes::from_static(b"p13_b f="));
        assert_eq!(err.reason, "missing field value");

        let err = decode_at("\n# c\np13_y f=1i\n  p13_y2 f=oops", Precision::Ns).unwrap_err();
        assert_eq!(err.line, 4, "blank and comment lines count");
        assert_eq!(err.text, Bytes::from_static(b"p13_y2 f=oops"), "leading spaces trimmed");
        assert_eq!(err.reason, "invalid boolean");
    }

    #[test]
    fn a_failed_body_counts_nothing() {
        let registry = Registry::new();
        let mut decoder = InfluxDecoder::new().with_telemetry(registry.telemetry_for(
            "influxdb_in",
            "influxdb_in",
            "listener",
        ));
        let body = Bytes::from_static(b"m,_logit.x=1 f=1i\nbad");
        assert!(decoder
            .decode_body(body, Precision::Ns, 0, Arc::new(Resource::default()))
            .is_err());
        assert!(registry.drain(0).is_empty());
    }

    #[test]
    fn a_reserved_prefix_tag_is_omitted_and_counted() {
        let registry = Registry::new();
        let mut decoder = InfluxDecoder::new().with_telemetry(registry.telemetry_for(
            "influxdb_in",
            "influxdb_in",
            "listener",
        ));
        let body = Bytes::from_static(
            b"m,_logit.influxdb.measurement=x,influxdb.field.f=y,host=a,_logit.influxdb.bucket=b \
              f=1i 1\n\
              n,influxdb.z=1,logit.x=c,_logitx=d,influxdb.field.x=e,influxdb.measurement=v f=1i 1",
        );
        let batch =
            decoder.decode_body(body, Precision::Ns, 0, Arc::new(Resource::default())).unwrap();
        assert_eq!(str_attr(&batch.events[0], ATTR_MEASUREMENT), Some("m"));
        assert_eq!(batch.events[0].attributes.get("influxdb.field.f"), None);
        assert_eq!(str_attr(&batch.events[0], "host"), Some("a"));
        assert_eq!(batch.events[0].attributes.get("_logit.influxdb.bucket"), None);
        let second = &batch.events[1];
        assert_eq!(str_attr(second, "logit.x"), Some("c"), "only `_logit.` is private");
        assert_eq!(str_attr(second, "_logitx"), Some("d"), "the prefix ends in a dot");
        assert_eq!(second.attributes.get("influxdb.field.x"), None, "a string field's prefix");
        assert_eq!(str_attr(second, "influxdb.z"), Some("1"), "the rest of `influxdb.` is public");
        assert_eq!(str_attr(second, "influxdb.measurement"), Some("v"), "not the carrier");
        assert_eq!(str_attr(second, ATTR_MEASUREMENT), Some("n"));
        assert_eq!(decoder.stats().tags_omitted_reserved_prefix, 4);

        let mut total = 0.0;
        for event in registry.drain(0) {
            assert!(
                event.metrics.iter().all(|m| resolve(m.name) != "logit.input.tags.dropped"),
                "a tag left off a delivered point is omitted, not dropped"
            );
            if event.attributes.get("reason").and_then(Value::as_str) != Some("reserved_prefix") {
                continue;
            }
            for m in &event.metrics {
                if resolve(m.name) == "logit.input.tags.omitted" {
                    total += match m.kind {
                        MetricKind::Sum(s) => s.value.as_f64(),
                        MetricKind::Gauge(v) => v.as_f64(),
                        _ => 0.0,
                    };
                }
            }
        }
        assert_eq!(total, 4.0);
    }

    #[test]
    fn unescaped_tags_strings_and_the_measurement_slice_the_body() {
        let body = Bytes::from_static(b"cpu,host=web-1,esc=a\\ b f=1i,s=\"text\",t=\"q\\\"\" 1");
        let batch = InfluxDecoder::new()
            .decode_body(body.clone(), Precision::Ns, 0, Arc::new(Resource::default()))
            .unwrap();
        let attr = |key: &str| match batch.events[0].attributes.get(key) {
            Some(Value::Str(b)) => b.clone(),
            other => panic!("{key}: {other:?}"),
        };
        let inside = |b: &Bytes| {
            let start = body.as_ptr() as usize;
            let at = b.as_ptr() as usize;
            at >= start && at + b.len() <= start + body.len()
        };
        assert!(inside(&attr("host")));
        assert!(inside(&attr(ATTR_MEASUREMENT)));
        assert!(inside(&attr("influxdb.field.s")));
        assert!(!inside(&attr("esc")), "an unescaped value is a copy");
        assert_eq!(attr("esc"), Bytes::from_static(b"a b"));
        assert!(!inside(&attr("influxdb.field.t")));
        assert_eq!(attr("influxdb.field.t"), Bytes::from_static(b"q\""));
    }

    #[test]
    fn the_batch_carries_the_callers_resource_and_no_scope() {
        let resource = Arc::new(Resource::default());
        let batch = InfluxDecoder::new()
            .decode_body(Bytes::from_static(b"m f=1i"), Precision::Ns, 0, resource.clone())
            .unwrap();
        assert!(Arc::ptr_eq(&batch.resource, &resource));
        assert!(batch.scope.is_none());
    }

    #[test]
    fn an_empty_body_is_an_empty_batch() {
        for body in ["", "\n\n", "# only\n"] {
            assert!(decode(body).is_empty(), "{body:?}");
        }
    }

    #[test]
    fn a_reused_decoder_forgets_the_previous_body() {
        let mut decoder = InfluxDecoder::new();
        let resource = Arc::new(Resource::default());
        let first = decoder
            .decode_body(Bytes::from_static(b"m f=1i 1"), Precision::Ns, 0, resource.clone())
            .unwrap();
        let second = decoder
            .decode_body(Bytes::from_static(b"m g=2i 1"), Precision::Ns, 0, resource)
            .unwrap();
        assert_eq!(first.events.len(), 1);
        assert_eq!(metrics(&second.events[0]), [("m.g", g(MetricValue::I64(2)))]);
        assert_eq!(
            decoder.stats(),
            DecodeStats { points: 1, merged: 0, tags_omitted_reserved_prefix: 0 }
        );
    }

    #[test]
    fn stats_count_points_and_merges() {
        let mut decoder = InfluxDecoder::new();
        decoder
            .decode_body(
                Bytes::from_static(b"m f=1i 1\n# c\nm g=1i 1\nn f=1i 1"),
                Precision::Ns,
                0,
                Arc::new(Resource::default()),
            )
            .unwrap();
        assert_eq!(
            decoder.stats(),
            DecodeStats { points: 3, merged: 1, tags_omitted_reserved_prefix: 0 }
        );
    }
}
