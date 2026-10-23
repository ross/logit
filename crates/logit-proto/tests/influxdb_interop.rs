//! Recorded interop fixtures: what Telegraf 1.40.2's `outputs.influxdb_v2` sent a `/api/v2/write`
//! endpoint, replayed through `logit_proto::influxdb`'s decoder and encoder.
//!
//! **Real bytes from a real producer**, not this codec's own encoder. See
//! `testdata/interop/influxdb/README.md` for the provenance table and `testdata/interop/README.md`
//! for why the corpus exists; regenerate with `script/record-fixtures influxdb`.
//!
//! Each capture is replayed at the codec entry point, gunzipped per its `Content-Encoding`
//! sidecar line, at `precision=ns` (Telegraf's default, with no `precision` in the query). A body
//! must decode, carry the measurements the run configured, and sit on the codec's fixed point:
//! the encoder's output for a decode decodes to the same events. Assertions are on decoded,
//! identifiable values, never on whole-file bytes.

use bytes::Bytes;
use logit_core::attrs::FIELD_PREFIX;
use logit_core::interner::resolve;
use logit_core::{Event, EventBatch, MetricKind, MetricValue, Resource, Value};
use logit_proto::influxdb::encode::InfluxLineEncoder;
use logit_proto::influxdb::{InfluxDecoder, Precision, ATTR_MEASUREMENT};
use logit_proto::Encoder;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const RECEIVED_AT: i64 = 1_791_914_000_000_000_000;

fn interop_dir() -> PathBuf {
    // `crates/logit-proto/` -> repository root.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/influxdb")
}

/// One recorded request: its sidecar headers and its body, gunzipped per `Content-Encoding`.
struct Capture {
    name: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn every_capture() -> Vec<Capture> {
    let dir = interop_dir();
    let mut stems: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".bin"))
        .map(|name| name.trim_end_matches(".bin").to_string())
        .collect();
    stems.sort();
    assert!(!stems.is_empty(), "no captures under {}", dir.display());
    stems
        .into_iter()
        .map(|name| {
            let raw = std::fs::read(dir.join(format!("{name}.bin"))).unwrap();
            let sidecar = std::fs::read_to_string(dir.join(format!("{name}.headers"))).unwrap();
            let headers: BTreeMap<String, String> = sidecar
                .lines()
                .filter_map(|line| line.split_once(": "))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let body = match headers.get("content-encoding").map(String::as_str) {
                None | Some("identity") => raw,
                Some("gzip") => {
                    let mut out = Vec::new();
                    flate2::read::GzDecoder::new(&raw[..])
                        .read_to_end(&mut out)
                        .unwrap_or_else(|e| panic!("{name}: gzip: {e}"));
                    out
                }
                Some(other) => panic!("{name}: Content-Encoding {other:?}"),
            };
            Capture { name, headers, body }
        })
        .collect()
}

fn decode(body: Bytes, resource: &Arc<Resource>) -> EventBatch {
    InfluxDecoder::new()
        .decode_body(body, Precision::Ns, RECEIVED_AT, resource.clone())
        .unwrap_or_else(|e| panic!("a recorded body must decode: {e}"))
}

fn measurement(event: &Event) -> &str {
    event.attributes.get(ATTR_MEASUREMENT).and_then(Value::as_str).expect("a measurement carrier")
}

/// The value of the metric `<measurement>.<field>` on `event`.
fn field(event: &Event, field: &str) -> Option<MetricValue> {
    let want = format!("{}.{field}", measurement(event));
    event.metrics.iter().find(|m| resolve(m.name) == want).map(|m| match m.kind {
        MetricKind::Gauge(v) => v,
        ref other => panic!("{want}: an InfluxDB field decodes to a Gauge, not {other:?}"),
    })
}

fn events_of<'a>(batch: &'a EventBatch, name: &str) -> Vec<&'a Event> {
    batch.events.iter().filter(|e| measurement(e) == name).collect()
}

/// The first event of `name` that carries `field`: `system`'s fields arrive on lines of their own.
fn with_field<'a>(batch: &'a EventBatch, name: &str, f: &str) -> &'a Event {
    events_of(batch, name)
        .into_iter()
        .find(|e| {
            field(e, f).is_some() || e.attributes.get(&format!("{FIELD_PREFIX}{f}")).is_some()
        })
        .unwrap_or_else(|| panic!("no {name} event with field {f}"))
}

fn string_field<'a>(event: &'a Event, f: &str) -> Option<&'a str> {
    event.attributes.get(&format!("{FIELD_PREFIX}{f}")).and_then(Value::as_str)
}

fn each_batch(mut check: impl FnMut(&Capture, &EventBatch)) {
    let resource = Arc::new(Resource::default());
    for capture in every_capture() {
        let batch = decode(Bytes::from(capture.body.clone()), &resource);
        check(&capture, &batch);
    }
}

#[test]
fn interop_fixture_telegraf_every_capture_decodes_with_the_configured_measurements() {
    each_batch(|capture, batch| {
        let names: BTreeSet<&str> = batch.events.iter().map(measurement).collect();
        // The first flush of each output is the one that may predate `cpu`'s first interval; the
        // inputs that need no prior sample are in every body.
        for want in ["mem", "system", "interop_types", "interop_multi", "interop_escapes"] {
            assert!(names.contains(want), "{}: no {want} in {names:?}", capture.name);
        }
        assert!(
            capture.headers["path"].starts_with("/api/v2/write?"),
            "{}: path {:?}",
            capture.name,
            capture.headers["path"]
        );
    });
}

#[test]
fn interop_fixture_telegraf_covers_both_content_encodings() {
    let encodings: BTreeSet<String> = every_capture()
        .iter()
        .map(|c| c.headers.get("content-encoding").cloned().unwrap_or_default())
        .collect();
    assert_eq!(encodings, BTreeSet::from(["".to_string(), "gzip".to_string()]));
}

#[test]
fn interop_fixture_telegraf_system_fields_are_typed() {
    each_batch(|capture, batch| {
        let uptime = field(with_field(batch, "system", "uptime"), "uptime");
        assert!(
            matches!(uptime, Some(MetricValue::U64(_))),
            "{}: Telegraf writes uptime as a uint under influx_uint_support: {uptime:?}",
            capture.name
        );
        let format = string_field(with_field(batch, "system", "uptime_format"), "uptime_format");
        assert!(
            format.is_some_and(|s| s.contains("days") || s.contains(':')),
            "{}: {format:?}",
            capture.name
        );
        let load = field(with_field(batch, "system", "load1"), "load1");
        assert!(matches!(load, Some(MetricValue::F64(_))), "{}: {load:?}", capture.name);
        let n_cpus = field(with_field(batch, "system", "n_cpus"), "n_cpus");
        assert!(
            matches!(n_cpus, Some(MetricValue::I64(n)) if n > 0),
            "{}: {n_cpus:?}",
            capture.name
        );
    });
}

#[test]
fn interop_fixture_telegraf_cpu_usage_is_f64_when_present() {
    let mut seen = 0;
    each_batch(|capture, batch| {
        for event in events_of(batch, "cpu") {
            let idle = field(event, "usage_idle");
            assert!(matches!(idle, Some(MetricValue::F64(_))), "{}: {idle:?}", capture.name);
            assert!(
                event.attributes.get("cpu").is_some() && event.attributes.get("host").is_some()
            );
            seen += 1;
        }
    });
    assert!(seen > 0, "no capture holds a cpu point");
}

#[test]
fn interop_fixture_telegraf_file_input_types_and_escapes_survive() {
    each_batch(|capture, batch| {
        let name = &capture.name;
        let types = events_of(batch, "interop_types");
        assert_eq!(types.len(), 1, "{name}");
        let t = types[0];
        assert_eq!(field(t, "b_true"), Some(MetricValue::Bool(true)), "{name}");
        assert_eq!(field(t, "b_false"), Some(MetricValue::Bool(false)), "{name}");
        assert_eq!(field(t, "i_neg"), Some(MetricValue::I64(-7)), "{name}");
        assert_eq!(field(t, "i_max"), Some(MetricValue::I64(i64::MAX)), "{name}");
        assert_eq!(field(t, "u_big"), Some(MetricValue::U64(u64::MAX)), "{name}");
        assert_eq!(field(t, "f_plain"), Some(MetricValue::F64(1.5)), "{name}");
        assert_eq!(string_field(t, "s_plain"), Some("hello"), "{name}");

        // Escapes in a measurement, tag key, tag value, and field key.
        let esc = events_of(batch, "interop_escapes");
        assert_eq!(esc.len(), 1, "{name}");
        for key in ["tag space", "tag,comma", "tag=eq"] {
            assert!(esc[0].attributes.get(key).is_some(), "{name}: tag {key:?}");
        }
        assert_eq!(esc[0].attributes.get("tag,comma").and_then(Value::as_str), Some("c,d"));
        for f in ["f s", "f,c", "f=e"] {
            assert!(field(esc[0], f).is_some(), "{name}: field {f:?}");
        }
        assert_eq!(events_of(batch, "interop_escapes and,more").len(), 1, "{name}");

        // Strings with `\"` and `\\`, and a quoted newline.
        let strings = events_of(batch, "interop_strings");
        let by_kind = |kind: &str| {
            *strings
                .iter()
                .find(|e| e.attributes.get("kind").and_then(Value::as_str) == Some(kind))
                .unwrap_or_else(|| panic!("{name}: no kind {kind}"))
        };
        assert_eq!(string_field(by_kind("quotes"), "s_quote"), Some("say \"hi\""), "{name}");
        assert_eq!(string_field(by_kind("quotes"), "s_slash"), Some("back\\slash"), "{name}");
        assert_eq!(string_field(by_kind("newline"), "s_nl"), Some("line1\nline2"), "{name}");

        // Three points of one series and instant merge: v=1 then v=2 overwrite, w=3 appends.
        let dup = events_of(batch, "interop_dup");
        assert_eq!(dup.len(), 1, "{name}");
        assert_eq!(field(dup[0], "v"), Some(MetricValue::I64(2)), "{name}");
        assert_eq!(field(dup[0], "w"), Some(MetricValue::I64(3)), "{name}");

        // Two multi-field points on distinct series.
        let multi = events_of(batch, "interop_multi");
        assert_eq!(multi.len(), 2, "{name}");
        assert_eq!(multi[0].metrics.len(), 4, "{name}: a, b, c, e; d is a string field");
        assert_eq!(string_field(multi[0], "d"), Some("four"), "{name}");
    });
}

#[test]
fn interop_fixture_telegraf_is_a_fixed_point() {
    each_batch(|capture, first| {
        let encoded = InfluxLineEncoder::default().encode(first).expect("encode");
        let second = decode(encoded.clone(), &first.resource);
        assert_eq!(second.events, first.events, "{}: decode(encode(decode(b)))", capture.name);
        let again = InfluxLineEncoder::default().encode(&second).expect("encode");
        assert_eq!(again, encoded, "{}: encode is stable on the second hop", capture.name);
    });
}
