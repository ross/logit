//! `MetricsData` from OTLP/JSON. See `super`'s module doc for the dialect rules this leans on, and
//! for why `exemplars` is parsed nowhere below.

use super::{
    array_field, bool_field, enum_field, f64_field, f64_field_opt, get, i32_field,
    instrumentation_scope, key_values, malformed, object_field, parse_f64, parse_i64, parse_u64,
    require_object, resource, str_field, u32_field, u64_field, JsonMap, JsonValue,
};
use crate::otlp::generated::opentelemetry::proto::metrics::v1 as pb;
use crate::CodecError;

pub(crate) fn metrics_data(bytes: &[u8]) -> Result<pb::MetricsData, CodecError> {
    let root: JsonValue = serde_json::from_slice(bytes).map_err(|e| malformed(e.to_string()))?;
    let obj = require_object(&root, "the request body")?;
    let resource_metrics = array_field(obj, "resourceMetrics", "resource_metrics")?
        .iter()
        .map(resource_metrics)
        .collect::<Result<_, _>>()?;
    Ok(pb::MetricsData { resource_metrics })
}

fn resource_metrics(v: &JsonValue) -> Result<pb::ResourceMetrics, CodecError> {
    let obj = require_object(v, "a resourceMetrics entry")?;
    let resource = match object_field(obj, "resource", "resource")? {
        Some(r) => Some(resource(r)?),
        None => None,
    };
    let scope_metrics = array_field(obj, "scopeMetrics", "scope_metrics")?
        .iter()
        .map(scope_metrics)
        .collect::<Result<_, _>>()?;
    Ok(pb::ResourceMetrics {
        resource,
        scope_metrics,
        schema_url: str_field(obj, "schemaUrl", "schema_url")?,
    })
}

fn scope_metrics(v: &JsonValue) -> Result<pb::ScopeMetrics, CodecError> {
    let obj = require_object(v, "a scopeMetrics entry")?;
    let scope = instrumentation_scope(get(obj, "scope", "scope"))?;
    let metrics =
        array_field(obj, "metrics", "metrics")?.iter().map(metric).collect::<Result<_, _>>()?;
    Ok(pb::ScopeMetrics { scope, metrics, schema_url: str_field(obj, "schemaUrl", "schema_url")? })
}

fn metric(v: &JsonValue) -> Result<pb::Metric, CodecError> {
    let obj = require_object(v, "a metric")?;
    // `metadata` (proto field 12) is parsed nowhere here -- nothing in `../metrics.rs`'s
    // `decode_metric` reads it, the same reason `exemplars` is skipped (module doc).
    let data = if let Some(g) = object_field(obj, "gauge", "gauge")? {
        Some(pb::metric::Data::Gauge(gauge(g)?))
    } else if let Some(s) = object_field(obj, "sum", "sum")? {
        Some(pb::metric::Data::Sum(sum(s)?))
    } else if let Some(h) = object_field(obj, "histogram", "histogram")? {
        Some(pb::metric::Data::Histogram(histogram(h)?))
    } else if let Some(eh) = object_field(obj, "exponentialHistogram", "exponential_histogram")? {
        Some(pb::metric::Data::ExponentialHistogram(exponential_histogram(eh)?))
    } else if let Some(s) = object_field(obj, "summary", "summary")? {
        Some(pb::metric::Data::Summary(summary(s)?))
    } else {
        None
    };
    Ok(pb::Metric {
        name: str_field(obj, "name", "name")?,
        description: str_field(obj, "description", "description")?,
        unit: str_field(obj, "unit", "unit")?,
        metadata: Vec::new(),
        data,
    })
}

fn temporality(obj: &JsonMap) -> Result<i32, CodecError> {
    enum_field(obj, "aggregationTemporality", "aggregation_temporality", |s| {
        pb::AggregationTemporality::from_str_name(s).map(|t| t as i32)
    })
}

fn gauge(obj: &JsonMap) -> Result<pb::Gauge, CodecError> {
    let data_points = array_field(obj, "dataPoints", "data_points")?
        .iter()
        .map(number_data_point)
        .collect::<Result<_, _>>()?;
    Ok(pb::Gauge { data_points })
}

fn sum(obj: &JsonMap) -> Result<pb::Sum, CodecError> {
    let data_points = array_field(obj, "dataPoints", "data_points")?
        .iter()
        .map(number_data_point)
        .collect::<Result<_, _>>()?;
    Ok(pb::Sum {
        data_points,
        aggregation_temporality: temporality(obj)?,
        is_monotonic: bool_field(obj, "isMonotonic", "is_monotonic")?,
    })
}

fn histogram(obj: &JsonMap) -> Result<pb::Histogram, CodecError> {
    let data_points = array_field(obj, "dataPoints", "data_points")?
        .iter()
        .map(histogram_data_point)
        .collect::<Result<_, _>>()?;
    Ok(pb::Histogram { data_points, aggregation_temporality: temporality(obj)? })
}

fn exponential_histogram(obj: &JsonMap) -> Result<pb::ExponentialHistogram, CodecError> {
    let data_points = array_field(obj, "dataPoints", "data_points")?
        .iter()
        .map(exponential_histogram_data_point)
        .collect::<Result<_, _>>()?;
    Ok(pb::ExponentialHistogram { data_points, aggregation_temporality: temporality(obj)? })
}

fn summary(obj: &JsonMap) -> Result<pb::Summary, CodecError> {
    let data_points = array_field(obj, "dataPoints", "data_points")?
        .iter()
        .map(summary_data_point)
        .collect::<Result<_, _>>()?;
    Ok(pb::Summary { data_points })
}

fn number_data_point(v: &JsonValue) -> Result<pb::NumberDataPoint, CodecError> {
    let obj = require_object(v, "a numberDataPoint")?;
    let value = if let Some(x) = get(obj, "asDouble", "as_double") {
        Some(pb::number_data_point::Value::AsDouble(parse_f64(x, "asDouble")?))
    } else if let Some(x) = get(obj, "asInt", "as_int") {
        Some(pb::number_data_point::Value::AsInt(parse_i64(x, "asInt")?))
    } else {
        None
    };
    Ok(pb::NumberDataPoint {
        attributes: key_values(obj, "attributes", "attributes")?,
        start_time_unix_nano: u64_field(obj, "startTimeUnixNano", "start_time_unix_nano")?,
        time_unix_nano: u64_field(obj, "timeUnixNano", "time_unix_nano")?,
        // Deliberately empty -- see the module doc.
        exemplars: Vec::new(),
        flags: u32_field(obj, "flags", "flags")?,
        value,
    })
}

// A `null` *element* is deliberately still an error, unlike a `null` field (see `super`'s module
// doc): `bucketCounts`/`explicitBounds` are positional and length-coupled
// (`bucket_counts.len() == explicit_bounds.len() + 1`, which `../metrics.rs` relies on to
// reconstruct boundaries), so there is no "this element is absent" reading -- coercing a null to 0
// would invent a real bucket observation out of a producer bug instead of surfacing it.
fn u64_array(obj: &JsonMap, camel: &str, snake: &str, field: &str) -> Result<Vec<u64>, CodecError> {
    array_field(obj, camel, snake)?.iter().map(|x| parse_u64(x, field)).collect()
}

fn f64_array(obj: &JsonMap, camel: &str, snake: &str, field: &str) -> Result<Vec<f64>, CodecError> {
    array_field(obj, camel, snake)?.iter().map(|x| parse_f64(x, field)).collect()
}

fn histogram_data_point(v: &JsonValue) -> Result<pb::HistogramDataPoint, CodecError> {
    let obj = require_object(v, "a histogramDataPoint")?;
    Ok(pb::HistogramDataPoint {
        attributes: key_values(obj, "attributes", "attributes")?,
        start_time_unix_nano: u64_field(obj, "startTimeUnixNano", "start_time_unix_nano")?,
        time_unix_nano: u64_field(obj, "timeUnixNano", "time_unix_nano")?,
        count: u64_field(obj, "count", "count")?,
        // `Option<f64>` -- an absent key must stay `None`, not become `Some(0.0)` (module doc).
        sum: f64_field_opt(obj, "sum", "sum")?,
        bucket_counts: u64_array(obj, "bucketCounts", "bucket_counts", "bucketCounts[]")?,
        explicit_bounds: f64_array(obj, "explicitBounds", "explicit_bounds", "explicitBounds[]")?,
        exemplars: Vec::new(),
        flags: u32_field(obj, "flags", "flags")?,
        min: f64_field_opt(obj, "min", "min")?,
        max: f64_field_opt(obj, "max", "max")?,
    })
}

fn exponential_histogram_data_point(
    v: &JsonValue,
) -> Result<pb::ExponentialHistogramDataPoint, CodecError> {
    let obj = require_object(v, "an exponentialHistogramDataPoint")?;
    let positive = match object_field(obj, "positive", "positive")? {
        Some(o) => Some(buckets(o)?),
        None => None,
    };
    let negative = match object_field(obj, "negative", "negative")? {
        Some(o) => Some(buckets(o)?),
        None => None,
    };
    Ok(pb::ExponentialHistogramDataPoint {
        attributes: key_values(obj, "attributes", "attributes")?,
        start_time_unix_nano: u64_field(obj, "startTimeUnixNano", "start_time_unix_nano")?,
        time_unix_nano: u64_field(obj, "timeUnixNano", "time_unix_nano")?,
        count: u64_field(obj, "count", "count")?,
        sum: f64_field_opt(obj, "sum", "sum")?,
        // Signed -- `scale`/`Buckets.offset` may be negative (module: "the negative range").
        scale: i32_field(obj, "scale", "scale")?,
        zero_count: u64_field(obj, "zeroCount", "zero_count")?,
        positive,
        negative,
        flags: u32_field(obj, "flags", "flags")?,
        exemplars: Vec::new(),
        min: f64_field_opt(obj, "min", "min")?,
        max: f64_field_opt(obj, "max", "max")?,
        zero_threshold: f64_field(obj, "zeroThreshold", "zero_threshold")?,
    })
}

fn buckets(obj: &JsonMap) -> Result<pb::exponential_histogram_data_point::Buckets, CodecError> {
    Ok(pb::exponential_histogram_data_point::Buckets {
        offset: i32_field(obj, "offset", "offset")?,
        bucket_counts: u64_array(obj, "bucketCounts", "bucket_counts", "bucketCounts[]")?,
    })
}

fn summary_data_point(v: &JsonValue) -> Result<pb::SummaryDataPoint, CodecError> {
    let obj = require_object(v, "a summaryDataPoint")?;
    let quantile_values = array_field(obj, "quantileValues", "quantile_values")?
        .iter()
        .map(value_at_quantile)
        .collect::<Result<_, _>>()?;
    Ok(pb::SummaryDataPoint {
        attributes: key_values(obj, "attributes", "attributes")?,
        start_time_unix_nano: u64_field(obj, "startTimeUnixNano", "start_time_unix_nano")?,
        time_unix_nano: u64_field(obj, "timeUnixNano", "time_unix_nano")?,
        count: u64_field(obj, "count", "count")?,
        // Required (plain `f64`, not `Option`) -- unlike Histogram's `sum`, a Summary's `sum` has
        // no "unset" state in the proto (`SummaryDataPoint.sum` is a bare `double`, field 5).
        sum: f64_field(obj, "sum", "sum")?,
        quantile_values,
        flags: u32_field(obj, "flags", "flags")?,
    })
}

fn value_at_quantile(v: &JsonValue) -> Result<pb::summary_data_point::ValueAtQuantile, CodecError> {
    let obj = require_object(v, "a valueAtQuantile")?;
    Ok(pb::summary_data_point::ValueAtQuantile {
        quantile: f64_field(obj, "quantile", "quantile")?,
        value: f64_field(obj, "value", "value")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gauge_data_point_decodes() {
        let json = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "m", "gauge": {"dataPoints": [{"timeUnixNano": "1", "asDouble": 3.5}]}
        }]}]}]}"#;
        let data = metrics_data(json).expect("should decode");
        match &data.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(pb::metric::Data::Gauge(g)) => {
                assert_eq!(
                    g.data_points[0].value,
                    Some(pb::number_data_point::Value::AsDouble(3.5))
                );
            }
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn an_absent_histogram_sum_stays_none_not_zero() {
        let json = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "m", "histogram": {"dataPoints": [{"timeUnixNano": "1", "count": "0"}]}
        }]}]}]}"#;
        let data = metrics_data(json).expect("should decode");
        match &data.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(pb::metric::Data::Histogram(h)) => {
                assert_eq!(h.data_points[0].sum, None, "an absent sum must be None, not Some(0.0)");
            }
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    #[test]
    fn a_negative_bucket_offset_is_accepted() {
        let json = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "m", "exponentialHistogram": {"dataPoints": [{
                "timeUnixNano": "1", "count": "1", "scale": -2,
                "positive": {"offset": -5, "bucketCounts": ["1"]}
            }]}
        }]}]}]}"#;
        let data = metrics_data(json).expect("should decode");
        match &data.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(pb::metric::Data::ExponentialHistogram(eh)) => {
                assert_eq!(eh.data_points[0].scale, -2);
                assert_eq!(eh.data_points[0].positive.as_ref().unwrap().offset, -5);
            }
            other => panic!("expected ExponentialHistogram, got {other:?}"),
        }
    }

    #[test]
    fn exemplars_are_ignored_without_erroring() {
        let json = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "m", "gauge": {"dataPoints": [{
                "timeUnixNano": "1", "asDouble": 1.0,
                "exemplars": [{"timeUnixNano": "1", "asDouble": 1.0, "spanId": "0102030405060708"}]
            }]}
        }]}]}]}"#;
        let data = metrics_data(json).expect("an exemplar must not fail decoding");
        match &data.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(pb::metric::Data::Gauge(g)) => assert!(g.data_points[0].exemplars.is_empty()),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn a_cumulative_sum_decodes_with_the_temporality_field_set() {
        let json = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "m", "sum": {
                "dataPoints": [{"timeUnixNano": "1", "asInt": "5"}],
                "aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE",
                "isMonotonic": true
            }
        }]}]}]}"#;
        let data = metrics_data(json).expect("should decode");
        match &data.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(pb::metric::Data::Sum(s)) => {
                assert_eq!(
                    s.aggregation_temporality,
                    pb::AggregationTemporality::Cumulative as i32
                );
                assert!(s.is_monotonic);
            }
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn a_null_data_point_value_decodes_as_no_value() {
        for body in [r#""asInt": null"#, r#""asDouble": null"#] {
            let json = format!(
                r#"{{"resourceMetrics": [{{"scopeMetrics": [{{"metrics": [{{
                    "name": "m", "gauge": {{"dataPoints": [{{"timeUnixNano": "1", {body}}}]}}
                }}]}}]}}]}}"#
            );
            let data = metrics_data(json.as_bytes())
                .unwrap_or_else(|e| panic!("{body} should decode, got {e}"));
            match &data.resource_metrics[0].scope_metrics[0].metrics[0].data {
                Some(pb::metric::Data::Gauge(g)) => {
                    assert_eq!(g.data_points[0].value, None, "{body} should leave value unset");
                }
                other => panic!("expected Gauge, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_null_bucket_count_is_rejected_with_a_message_naming_the_element() {
        let json = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "m", "histogram": {"dataPoints": [{
                "timeUnixNano": "1", "count": "3", "bucketCounts": ["1", null], "explicitBounds": [1.0]
            }]}
        }]}]}]}"#;
        let err = metrics_data(json).unwrap_err().to_string();
        assert!(err.contains("bucketCounts[]"), "error should name the element, got: {err}");
        assert!(err.contains("null"), "error should show the offending value, got: {err}");
    }

    #[test]
    fn a_null_bucket_counts_array_is_the_empty_list_not_an_error() {
        let json = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "m", "histogram": {"dataPoints": [{
                "timeUnixNano": "1", "count": "0", "bucketCounts": null, "explicitBounds": null
            }]}
        }]}]}]}"#;
        let data = metrics_data(json).expect("a null bucketCounts field must decode as empty");
        match &data.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(pb::metric::Data::Histogram(h)) => {
                assert!(h.data_points[0].bucket_counts.is_empty());
                assert!(h.data_points[0].explicit_bounds.is_empty());
            }
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    #[test]
    fn a_bucket_count_may_be_a_json_string() {
        let json = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "m", "histogram": {"dataPoints": [{
                "timeUnixNano": "1", "count": "3",
                "bucketCounts": ["1", "2"], "explicitBounds": [1.0]
            }]}
        }]}]}]}"#;
        let data = metrics_data(json).expect("should decode");
        match &data.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(pb::metric::Data::Histogram(h)) => {
                assert_eq!(h.data_points[0].bucket_counts, vec![1, 2]);
            }
            other => panic!("expected Histogram, got {other:?}"),
        }
    }
}
