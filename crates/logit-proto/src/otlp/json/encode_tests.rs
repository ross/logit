use super::*;
use crate::otlp::json::{logs_data, metrics_data, traces_data};
use common_pb::any_value::Value as Any;
use metrics_pb::metric::Data;
use proptest::prelude::*;

fn logs_json(data: &logs_pb::LogsData) -> String {
    let mut out = Vec::new();
    write_logs(data, &mut out);
    String::from_utf8(out).expect("the encoder writes UTF-8")
}

fn traces_json(data: &trace_pb::TracesData) -> String {
    let mut out = Vec::new();
    write_traces(data, &mut out);
    String::from_utf8(out).expect("the encoder writes UTF-8")
}

fn metrics_json(data: &metrics_pb::MetricsData) -> String {
    let mut out = Vec::new();
    write_metrics(data, &mut out);
    String::from_utf8(out).expect("the encoder writes UTF-8")
}

fn kv(key: &str, value: Any) -> common_pb::KeyValue {
    common_pb::KeyValue {
        key: key.to_string(),
        value: Some(common_pb::AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

fn s(v: &str) -> Any {
    Any::StringValue(v.to_string())
}

fn one_record(record: logs_pb::LogRecord) -> logs_pb::LogsData {
    logs_pb::LogsData {
        resource_logs: vec![logs_pb::ResourceLogs {
            resource: None,
            scope_logs: vec![logs_pb::ScopeLogs {
                scope: None,
                log_records: vec![record],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn one_metric(metric: metrics_pb::Metric) -> metrics_pb::MetricsData {
    metrics_pb::MetricsData {
        resource_metrics: vec![metrics_pb::ResourceMetrics {
            resource: None,
            scope_metrics: vec![metrics_pb::ScopeMetrics {
                scope: None,
                metrics: vec![metric],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn double_text(v: f64) -> String {
    let mut out = Vec::new();
    double(&mut out, v);
    String::from_utf8(out).unwrap()
}

// -- byte-exact forms ----------------------------------------------------------------------------

#[test]
fn an_empty_request_is_an_empty_object() {
    assert_eq!(logs_json(&logs_pb::LogsData::default()), "{}");
    assert_eq!(traces_json(&trace_pb::TracesData::default()), "{}");
    assert_eq!(metrics_json(&metrics_pb::MetricsData::default()), "{}");
}

#[test]
fn unset_resource_scope_and_body_are_written_as_empty_objects() {
    let data = one_record(logs_pb::LogRecord::default());
    assert_eq!(
        logs_json(&data),
        concat!(
            r#"{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"#,
            r#""logRecords":[{"body":{}}]}]}]}"#,
        )
    );
}

#[test]
fn sixty_four_bit_integers_are_strings_and_thirty_two_bit_integers_are_numbers() {
    let data = one_record(logs_pb::LogRecord {
        time_unix_nano: u64::MAX,
        observed_time_unix_nano: 1,
        severity_number: 9,
        dropped_attributes_count: 4_000_000_000,
        flags: 1,
        attributes: vec![kv("min", Any::IntValue(i64::MIN)), kv("zero", Any::IntValue(0))],
        ..Default::default()
    });
    assert_eq!(
        logs_json(&data),
        concat!(
            r#"{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"#,
            r#""timeUnixNano":"18446744073709551615","observedTimeUnixNano":"1","#,
            r#""severityNumber":9,"body":{},"attributes":["#,
            r#"{"key":"min","value":{"intValue":"-9223372036854775808"}},"#,
            r#"{"key":"zero","value":{"intValue":"0"}}],"#,
            r#""droppedAttributesCount":4000000000,"flags":1}]}]}]}"#,
        )
    );
}

#[test]
fn ids_are_lowercase_hex_and_an_all_zero_id_is_omitted() {
    let data = one_record(logs_pb::LogRecord {
        trace_id: vec![0xAB; 16],
        span_id: vec![0; 8],
        ..Default::default()
    });
    let json = logs_json(&data);
    assert!(
        json.contains(r#""traceId":"abababababababababababababababab""#),
        "trace id must be lowercase hex: {json}"
    );
    assert!(!json.contains("spanId"), "an all-zero span id must be omitted: {json}");
}

#[test]
fn any_value_arms_write_their_proto_json_forms() {
    let data = one_record(logs_pb::LogRecord {
        body: Some(common_pb::AnyValue {
            value: Some(Any::KvlistValue(common_pb::KeyValueList {
                values: vec![
                    kv("bytes", Any::BytesValue(vec![0xff, 0x00, 0x10])),
                    kv("empty_bytes", Any::BytesValue(Vec::new())),
                    kv("empty_kvlist", Any::KvlistValue(common_pb::KeyValueList::default())),
                    kv("empty_array", Any::ArrayValue(common_pb::ArrayValue::default())),
                    kv("bool", Any::BoolValue(false)),
                    kv("double", Any::DoubleValue(f64::NAN)),
                    kv("strindex", Any::StringValueStrindex(3)),
                    common_pb::KeyValue { key: "unset".into(), value: None, key_strindex: 0 },
                    common_pb::KeyValue {
                        key: String::new(),
                        value: Some(common_pb::AnyValue { value: Some(s("v")) }),
                        key_strindex: 7,
                    },
                ],
            })),
        }),
        ..Default::default()
    });
    let json = logs_json(&data);
    let body = json
        .split_once(r#""body":"#)
        .and_then(|(_, rest)| rest.strip_suffix("}]}]}]}"))
        .unwrap_or_else(|| panic!("no body in {json}"));
    assert_eq!(
        body,
        concat!(
            r#"{"kvlistValue":{"values":["#,
            r#"{"key":"bytes","value":{"bytesValue":"/wAQ"}},"#,
            r#"{"key":"empty_bytes","value":{"bytesValue":""}},"#,
            r#"{"key":"empty_kvlist","value":{"kvlistValue":{}}},"#,
            r#"{"key":"empty_array","value":{"arrayValue":{}}},"#,
            r#"{"key":"bool","value":{"boolValue":false}},"#,
            r#"{"key":"double","value":{"doubleValue":"NaN"}},"#,
            r#"{"key":"strindex","value":{"stringValueStrindex":3}},"#,
            r#"{"key":"unset","value":{}},"#,
            r#"{"value":{"stringValue":"v"},"keyStrindex":7}]}}"#,
        )
    );
}

#[test]
fn doubles_follow_json_iterator_write_float64() {
    // Each right-hand side is Go's strconv.AppendFloat(v, 'f' or 'e', -1, 64), which
    // json-iterator 1.1.12 writes unchanged. Its unreleased `master` drops an exponent's leading
    // zero (`1e-7`); pdata pins 1.1.12.
    let cases: &[(f64, &str)] = &[
        (0.0, "0"),
        (-0.0, "-0"),
        (1.0, "1"),
        (-2.5, "-2.5"),
        (0.1 + 0.2, "0.30000000000000004"),
        (1e-6, "0.000001"),
        (1e20, "100000000000000000000"),
        (1e21, "1e+21"),
        (1.5e300, "1.5e+300"),
        (1e-7, "1e-07"),
        (2.5e-9, "2.5e-09"),
        (1.25e-10, "1.25e-10"),
        (5e-324, "5e-324"),
        (f64::MAX, "1.7976931348623157e+308"),
        (f64::NAN, r#""NaN""#),
        (f64::INFINITY, r#""Infinity""#),
        (f64::NEG_INFINITY, r#""-Infinity""#),
    ];
    for (v, want) in cases {
        assert_eq!(double_text(*v), *want, "{v:e}");
    }
}

#[test]
fn strings_escape_quotes_backslashes_and_control_characters_only() {
    let mut out = Vec::new();
    string(&mut out, "a\"b\\c\nd\re\tf\u{1}g\u{1f}h\u{7f}<&>\u{2028}é");
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "\"a\\\"b\\\\c\\nd\\re\\tf\\u0001g\\u001fh\u{7f}<&>\u{2028}é\""
    );
}

#[test]
fn oneof_and_optional_fields_are_written_at_zero_and_plain_zeros_are_omitted() {
    let data = one_metric(metrics_pb::Metric {
        name: "m".into(),
        data: Some(Data::Histogram(metrics_pb::Histogram {
            data_points: vec![metrics_pb::HistogramDataPoint {
                sum: Some(0.0),
                min: Some(-0.0),
                max: None,
                count: 0,
                ..Default::default()
            }],
            aggregation_temporality: 0,
        })),
        ..Default::default()
    });
    let json = metrics_json(&data);
    assert!(json.contains(r#""histogram":{"dataPoints":[{"sum":0,"min":-0}]}"#), "{json}");

    let data = one_metric(metrics_pb::Metric {
        name: "g".into(),
        data: Some(Data::Gauge(metrics_pb::Gauge {
            data_points: vec![metrics_pb::NumberDataPoint {
                value: Some(metrics_pb::number_data_point::Value::AsInt(0)),
                ..Default::default()
            }],
        })),
        ..Default::default()
    });
    let json = metrics_json(&data);
    assert!(json.contains(r#""gauge":{"dataPoints":[{"asInt":"0"}]}"#), "{json}");
}

#[test]
fn an_exponential_histogram_point_always_writes_both_bucket_ranges() {
    let data = one_metric(metrics_pb::Metric {
        name: "e".into(),
        data: Some(Data::ExponentialHistogram(metrics_pb::ExponentialHistogram {
            data_points: vec![metrics_pb::ExponentialHistogramDataPoint {
                positive: Some(metrics_pb::exponential_histogram_data_point::Buckets {
                    offset: -2,
                    bucket_counts: vec![0, 3],
                }),
                ..Default::default()
            }],
            aggregation_temporality: 2,
        })),
        ..Default::default()
    });
    let json = metrics_json(&data);
    assert!(
        json.contains(concat!(
            r#""exponentialHistogram":{"dataPoints":[{"positive":{"offset":-2,"#,
            r#""bucketCounts":["0","3"]},"negative":{}}],"aggregationTemporality":2}"#,
        )),
        "{json}"
    );
}

#[test]
fn entity_refs_and_metric_metadata_are_written() {
    let mut data = one_metric(metrics_pb::Metric {
        name: "m".into(),
        metadata: vec![kv("k", s("v"))],
        ..Default::default()
    });
    data.resource_metrics[0].resource = Some(resource_pb::Resource {
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        entity_refs: vec![common_pb::EntityRef {
            schema_url: "u".into(),
            r#type: "service".into(),
            id_keys: vec!["service.name".into()],
            description_keys: vec!["a".into(), "b".into()],
        }],
    });
    assert_eq!(
        metrics_json(&data),
        concat!(
            r#"{"resourceMetrics":[{"resource":{"entityRefs":[{"schemaUrl":"u","type":"service","#,
            r#""idKeys":["service.name"],"descriptionKeys":["a","b"]}]},"#,
            r#""scopeMetrics":[{"scope":{},"metrics":[{"name":"m","#,
            r#""metadata":[{"key":"k","value":{"stringValue":"v"}}]}]}]}]}"#,
        )
    );
}

// -- round trip through the decoder --------------------------------------------------------------

fn attrs(prefix: &str) -> Vec<common_pb::KeyValue> {
    vec![
        kv(&format!("{prefix}.s"), s("text \"quoted\"\n")),
        kv(&format!("{prefix}.b"), Any::BoolValue(true)),
        kv(&format!("{prefix}.i"), Any::IntValue(-42)),
        kv(&format!("{prefix}.d"), Any::DoubleValue(2.5e-9)),
        kv(&format!("{prefix}.bytes"), Any::BytesValue(vec![1, 2, 3, 255])),
        kv(
            &format!("{prefix}.a"),
            Any::ArrayValue(common_pb::ArrayValue {
                values: vec![
                    common_pb::AnyValue { value: Some(s("x")) },
                    common_pb::AnyValue { value: None },
                    common_pb::AnyValue {
                        value: Some(Any::ArrayValue(common_pb::ArrayValue::default())),
                    },
                ],
            }),
        ),
        kv(
            &format!("{prefix}.kv"),
            Any::KvlistValue(common_pb::KeyValueList {
                values: vec![
                    kv("nested", Any::IntValue(i64::MAX)),
                    kv("empty", Any::KvlistValue(common_pb::KeyValueList::default())),
                ],
            }),
        ),
    ]
}

fn full_resource() -> resource_pb::Resource {
    resource_pb::Resource {
        attributes: attrs("res"),
        dropped_attributes_count: 3,
        entity_refs: Vec::new(),
    }
}

fn full_scope() -> common_pb::InstrumentationScope {
    common_pb::InstrumentationScope {
        name: "scope".into(),
        version: "1.2.3".into(),
        attributes: attrs("scope"),
        dropped_attributes_count: 5,
    }
}

fn full_logs() -> logs_pb::LogsData {
    let record = logs_pb::LogRecord {
        time_unix_nano: 1_789_075_337_985_247_299,
        observed_time_unix_nano: u64::MAX,
        severity_number: 17,
        severity_text: "ERROR".into(),
        body: Some(common_pb::AnyValue { value: Some(s("body")) }),
        attributes: attrs("log"),
        dropped_attributes_count: 2,
        flags: 1,
        trace_id: (1..=16).collect(),
        span_id: (1..=8).collect(),
        event_name: "event".into(),
    };
    logs_pb::LogsData {
        resource_logs: vec![
            logs_pb::ResourceLogs {
                resource: Some(full_resource()),
                scope_logs: vec![
                    logs_pb::ScopeLogs {
                        scope: Some(full_scope()),
                        log_records: vec![record, logs_pb::LogRecord::default()],
                        schema_url: "https://scope".into(),
                    },
                    logs_pb::ScopeLogs::default(),
                ],
                schema_url: "https://resource".into(),
            },
            logs_pb::ResourceLogs::default(),
        ],
    }
}

fn full_traces() -> trace_pb::TracesData {
    let span = trace_pb::Span {
        trace_id: (16..32).collect(),
        span_id: (8..16).collect(),
        trace_state: "a=b".into(),
        parent_span_id: (24..32).collect(),
        flags: 257,
        name: "span".into(),
        kind: 3,
        start_time_unix_nano: 1,
        end_time_unix_nano: u64::MAX,
        attributes: attrs("span"),
        dropped_attributes_count: 1,
        events: vec![
            trace_pb::span::Event {
                time_unix_nano: 7,
                name: "ev".into(),
                attributes: attrs("event"),
                dropped_attributes_count: 2,
            },
            trace_pb::span::Event::default(),
        ],
        dropped_events_count: 3,
        links: vec![
            trace_pb::span::Link {
                trace_id: vec![0xfe; 16],
                span_id: vec![0x0f; 8],
                trace_state: "c=d".into(),
                attributes: attrs("link"),
                dropped_attributes_count: 4,
                flags: 768,
            },
            trace_pb::span::Link::default(),
        ],
        dropped_links_count: 5,
        status: Some(trace_pb::Status { message: "boom".into(), code: 2 }),
    };
    trace_pb::TracesData {
        resource_spans: vec![trace_pb::ResourceSpans {
            resource: Some(full_resource()),
            scope_spans: vec![trace_pb::ScopeSpans {
                scope: Some(full_scope()),
                spans: vec![span, trace_pb::Span::default()],
                schema_url: "https://scope".into(),
            }],
            schema_url: "https://resource".into(),
        }],
    }
}

fn exemplars_for_test() -> Vec<metrics_pb::Exemplar> {
    vec![
        metrics_pb::Exemplar {
            filtered_attributes: attrs("ex"),
            time_unix_nano: 9,
            trace_id: vec![0x11; 16],
            span_id: vec![0x22; 8],
            value: Some(metrics_pb::exemplar::Value::AsDouble(0.25)),
        },
        metrics_pb::Exemplar {
            value: Some(metrics_pb::exemplar::Value::AsInt(i64::MIN)),
            ..Default::default()
        },
        metrics_pb::Exemplar::default(),
    ]
}

fn full_metrics() -> metrics_pb::MetricsData {
    use metrics_pb::number_data_point::Value as Nv;
    let number = |value| metrics_pb::NumberDataPoint {
        attributes: attrs("point"),
        start_time_unix_nano: 10,
        time_unix_nano: 20,
        exemplars: exemplars_for_test(),
        flags: 1,
        value,
    };
    let metric = |name: &str, data| metrics_pb::Metric {
        name: name.into(),
        description: format!("{name} description"),
        unit: "ms".into(),
        metadata: Vec::new(),
        data,
    };
    let metrics = vec![
        metric(
            "gauge",
            Some(Data::Gauge(metrics_pb::Gauge {
                data_points: vec![
                    number(Some(Nv::AsDouble(-1.5e300))),
                    number(Some(Nv::AsInt(i64::MAX))),
                    number(None),
                ],
            })),
        ),
        metric(
            "sum",
            Some(Data::Sum(metrics_pb::Sum {
                data_points: vec![number(Some(Nv::AsInt(-7)))],
                aggregation_temporality: 2,
                is_monotonic: true,
            })),
        ),
        metric(
            "histogram",
            Some(Data::Histogram(metrics_pb::Histogram {
                data_points: vec![
                    metrics_pb::HistogramDataPoint {
                        attributes: attrs("h"),
                        start_time_unix_nano: 1,
                        time_unix_nano: 2,
                        count: 6,
                        sum: Some(12.5),
                        bucket_counts: vec![1, 0, 5],
                        explicit_bounds: vec![0.5, 1e21],
                        exemplars: exemplars_for_test(),
                        flags: 1,
                        min: Some(0.0),
                        max: Some(9.75),
                    },
                    metrics_pb::HistogramDataPoint::default(),
                ],
                aggregation_temporality: 1,
            })),
        ),
        metric(
            "exponential",
            Some(Data::ExponentialHistogram(metrics_pb::ExponentialHistogram {
                data_points: vec![
                    metrics_pb::ExponentialHistogramDataPoint {
                        attributes: attrs("e"),
                        start_time_unix_nano: 1,
                        time_unix_nano: 2,
                        count: 9,
                        sum: Some(-3.0),
                        scale: -3,
                        zero_count: 2,
                        positive: Some(metrics_pb::exponential_histogram_data_point::Buckets {
                            offset: -1,
                            bucket_counts: vec![1, 2],
                        }),
                        negative: Some(metrics_pb::exponential_histogram_data_point::Buckets {
                            offset: 4,
                            bucket_counts: vec![u64::MAX],
                        }),
                        flags: 1,
                        exemplars: exemplars_for_test(),
                        min: Some(-8.0),
                        max: Some(1e-300),
                        zero_threshold: 1e-9,
                    },
                    metrics_pb::ExponentialHistogramDataPoint::default(),
                ],
                aggregation_temporality: 2,
            })),
        ),
        metric(
            "summary",
            Some(Data::Summary(metrics_pb::Summary {
                data_points: vec![
                    metrics_pb::SummaryDataPoint {
                        attributes: attrs("s"),
                        start_time_unix_nano: 1,
                        time_unix_nano: 2,
                        count: 4,
                        sum: 10.0,
                        quantile_values: vec![
                            metrics_pb::summary_data_point::ValueAtQuantile {
                                quantile: 0.0,
                                value: 0.0,
                            },
                            metrics_pb::summary_data_point::ValueAtQuantile {
                                quantile: 0.99,
                                value: 123.0,
                            },
                        ],
                        flags: 1,
                    },
                    metrics_pb::SummaryDataPoint::default(),
                ],
            })),
        ),
        metric("no_data", None),
    ];
    metrics_pb::MetricsData {
        resource_metrics: vec![metrics_pb::ResourceMetrics {
            resource: Some(full_resource()),
            scope_metrics: vec![metrics_pb::ScopeMetrics {
                scope: Some(full_scope()),
                metrics,
                schema_url: "https://scope".into(),
            }],
            schema_url: "https://resource".into(),
        }],
    }
}

/// The encoder writes an unset `resource`, `scope`, `body`, `value`, `status`, `positive`, and
/// `negative` as `{}`, which the decoder reads as set and empty.
fn present<T: Default>(v: &mut Option<T>) {
    v.get_or_insert_with(T::default);
}

fn normalize_kvs(kvs: &mut [common_pb::KeyValue]) {
    for kv in kvs {
        present(&mut kv.value);
        if let Some(v) = &mut kv.value {
            normalize_any(v);
        }
    }
}

fn normalize_any(v: &mut common_pb::AnyValue) {
    match &mut v.value {
        Some(Any::ArrayValue(a)) => a.values.iter_mut().for_each(normalize_any),
        Some(Any::KvlistValue(kv)) => normalize_kvs(&mut kv.values),
        _ => {}
    }
}

fn normalize_resource(r: &mut Option<resource_pb::Resource>) {
    present(r);
    if let Some(r) = r {
        normalize_kvs(&mut r.attributes);
    }
}

fn normalize_scope(s: &mut Option<common_pb::InstrumentationScope>) {
    present(s);
    if let Some(s) = s {
        normalize_kvs(&mut s.attributes);
    }
}

fn normalize_logs(mut data: logs_pb::LogsData) -> logs_pb::LogsData {
    for rl in &mut data.resource_logs {
        normalize_resource(&mut rl.resource);
        for sl in &mut rl.scope_logs {
            normalize_scope(&mut sl.scope);
            for r in &mut sl.log_records {
                present(&mut r.body);
                if let Some(b) = &mut r.body {
                    normalize_any(b);
                }
                normalize_kvs(&mut r.attributes);
            }
        }
    }
    data
}

fn normalize_traces(mut data: trace_pb::TracesData) -> trace_pb::TracesData {
    for rs in &mut data.resource_spans {
        normalize_resource(&mut rs.resource);
        for ss in &mut rs.scope_spans {
            normalize_scope(&mut ss.scope);
            for span in &mut ss.spans {
                present(&mut span.status);
                normalize_kvs(&mut span.attributes);
                span.events.iter_mut().for_each(|e| normalize_kvs(&mut e.attributes));
                span.links.iter_mut().for_each(|l| normalize_kvs(&mut l.attributes));
            }
        }
    }
    data
}

fn normalize_metrics(mut data: metrics_pb::MetricsData) -> metrics_pb::MetricsData {
    fn exemplars(v: &mut [metrics_pb::Exemplar]) {
        v.iter_mut().for_each(|e| normalize_kvs(&mut e.filtered_attributes));
    }
    for rm in &mut data.resource_metrics {
        normalize_resource(&mut rm.resource);
        for sm in &mut rm.scope_metrics {
            normalize_scope(&mut sm.scope);
            for m in &mut sm.metrics {
                match &mut m.data {
                    Some(Data::Gauge(g)) => g.data_points.iter_mut().for_each(|p| {
                        normalize_kvs(&mut p.attributes);
                        exemplars(&mut p.exemplars);
                    }),
                    Some(Data::Sum(s)) => s.data_points.iter_mut().for_each(|p| {
                        normalize_kvs(&mut p.attributes);
                        exemplars(&mut p.exemplars);
                    }),
                    Some(Data::Histogram(h)) => h.data_points.iter_mut().for_each(|p| {
                        normalize_kvs(&mut p.attributes);
                        exemplars(&mut p.exemplars);
                    }),
                    Some(Data::ExponentialHistogram(h)) => h.data_points.iter_mut().for_each(|p| {
                        normalize_kvs(&mut p.attributes);
                        exemplars(&mut p.exemplars);
                        present(&mut p.positive);
                        present(&mut p.negative);
                    }),
                    Some(Data::Summary(s)) => {
                        s.data_points.iter_mut().for_each(|p| normalize_kvs(&mut p.attributes))
                    }
                    None => {}
                }
            }
        }
    }
    data
}

#[test]
fn every_logs_field_round_trips_through_the_decoder() {
    let data = full_logs();
    let json = logs_json(&data);
    let decoded = logs_data(json.as_bytes()).unwrap_or_else(|e| panic!("{e}: {json}"));
    assert_eq!(decoded, normalize_logs(data));
}

#[test]
fn every_traces_field_round_trips_through_the_decoder() {
    let data = full_traces();
    let json = traces_json(&data);
    let decoded = traces_data(json.as_bytes()).unwrap_or_else(|e| panic!("{e}: {json}"));
    assert_eq!(decoded, normalize_traces(data));
}

#[test]
fn every_metrics_field_round_trips_through_the_decoder() {
    let data = full_metrics();
    let json = metrics_json(&data);
    let decoded = metrics_data(json.as_bytes()).unwrap_or_else(|e| panic!("{e}: {json}"));
    assert_eq!(decoded, normalize_metrics(data));
}

#[test]
fn a_decoded_body_re_encodes_to_the_same_bytes() {
    let json = logs_json(&full_logs());
    assert_eq!(logs_json(&logs_data(json.as_bytes()).unwrap()), json);
    let json = traces_json(&full_traces());
    assert_eq!(traces_json(&traces_data(json.as_bytes()).unwrap()), json);
    let json = metrics_json(&full_metrics());
    assert_eq!(metrics_json(&metrics_data(json.as_bytes()).unwrap()), json);
}

fn any_value_strategy() -> impl Strategy<Value = common_pb::AnyValue> {
    let leaf = prop_oneof![
        Just(None),
        any::<String>().prop_map(|v| Some(Any::StringValue(v))),
        any::<bool>().prop_map(|v| Some(Any::BoolValue(v))),
        any::<i64>().prop_map(|v| Some(Any::IntValue(v))),
        any::<f64>()
            .prop_filter("NaN never compares equal", |v| !v.is_nan())
            .prop_map(|v| Some(Any::DoubleValue(v))),
        proptest::collection::vec(any::<u8>(), 0..8).prop_map(|v| Some(Any::BytesValue(v))),
    ]
    .prop_map(|value| common_pb::AnyValue { value });
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(|values| {
                common_pb::AnyValue {
                    value: Some(Any::ArrayValue(common_pb::ArrayValue { values })),
                }
            }),
            proptest::collection::vec((any::<String>(), inner), 0..4).prop_map(|pairs| {
                let values = pairs
                    .into_iter()
                    .map(|(key, v)| common_pb::KeyValue { key, value: Some(v), key_strindex: 0 })
                    .collect();
                common_pb::AnyValue {
                    value: Some(Any::KvlistValue(common_pb::KeyValueList { values })),
                }
            }),
        ]
    })
}

proptest! {
    /// Arbitrary strings, 64-bit integers, doubles, and nested values survive encode then decode.
    #[test]
    fn a_generated_log_record_round_trips(
        body in any_value_strategy(),
        attr in any_value_strategy(),
        key in any::<String>(),
        time in any::<u64>(),
        severity in any::<i32>(),
        text in any::<String>(),
        trace_id in proptest::array::uniform16(any::<u8>()),
    ) {
        let record = logs_pb::LogRecord {
            time_unix_nano: time,
            severity_number: severity,
            severity_text: text,
            body: Some(body),
            attributes: vec![common_pb::KeyValue { key, value: Some(attr), key_strindex: 0 }],
            trace_id: if trace_id == [0; 16] { Vec::new() } else { trace_id.to_vec() },
            ..Default::default()
        };
        let data = one_record(record);
        let json = logs_json(&data);
        let decoded = logs_data(json.as_bytes())
            .map_err(|e| TestCaseError::fail(format!("{e}: {json}")))?;
        prop_assert_eq!(decoded, normalize_logs(data));
    }

    /// Every finite double the decoder reads back is bit-identical, and every non-finite one
    /// keeps its class.
    #[test]
    fn a_generated_double_round_trips(v in any::<f64>()) {
        let data = one_metric(metrics_pb::Metric {
            name: "d".into(),
            data: Some(Data::Gauge(metrics_pb::Gauge {
                data_points: vec![metrics_pb::NumberDataPoint {
                    value: Some(metrics_pb::number_data_point::Value::AsDouble(v)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        });
        let json = metrics_json(&data);
        let decoded = metrics_data(json.as_bytes())
            .map_err(|e| TestCaseError::fail(format!("{e}: {json}")))?;
        let metric = &decoded.resource_metrics[0].scope_metrics[0].metrics[0];
        let Some(Data::Gauge(g)) = &metric.data else {
            return Err(TestCaseError::fail(format!("no gauge in {json}")));
        };
        use metrics_pb::number_data_point::Value as Nv;
        let Some(Nv::AsDouble(got)) = g.data_points[0].value else {
            return Err(TestCaseError::fail(format!("no asDouble in {json}")));
        };
        if v.is_nan() {
            prop_assert!(got.is_nan());
        } else {
            prop_assert_eq!(got.to_bits(), v.to_bits());
        }
    }
}

// -- recorded Collector output -------------------------------------------------------------------

/// `testdata/interop/otlp/` is the Collector's `file` exporter output, which is pdata's
/// `JSONMarshaler` output, one request per line. Decoding a line and encoding it again must give
/// back that line byte for byte.
#[test]
fn recorded_collector_output_re_encodes_byte_for_byte() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/otlp");
    type Reencode = fn(&[u8]) -> String;
    let files: [(&str, Reencode); 3] = [
        ("logs.json", |b| logs_json(&logs_data(b).unwrap())),
        ("traces.json", |b| traces_json(&traces_data(b).unwrap())),
        ("metrics.json", |b| metrics_json(&metrics_data(b).unwrap())),
    ];
    let mut lines = 0;
    for (name, reencode) in files {
        let text = std::fs::read_to_string(dir.join(name)).unwrap();
        for line in text.lines().filter(|l| !l.is_empty()) {
            assert_eq!(reencode(line.as_bytes()), line, "{name}");
            lines += 1;
        }
    }
    assert_eq!(lines, 5, "the corpus has five requests");
}
