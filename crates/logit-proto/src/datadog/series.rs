//! Datadog metric series: `/api/v2/series` (protobuf `MetricPayload` and JSON), `/api/v1/series`
//! (JSON), and `/api/v1/distribution_points` (JSON). The mapping tables are in [`super`]'s module
//! doc, under "Metrics"; this file holds the shared series carriers [`super::sketches`] reuses.
//!
//! Every decoded point becomes one `Event` carrying one `MetricRecord`: a series with N points is
//! N events sharing their attributes. Point timestamps are whole seconds on every route, so a
//! decoded `Event::timestamp` is always a whole second too, including a `received_at` stand-in.

use super::generated::agentpayload::{metric_payload, Metadata, MetricPayload, Origin};
use super::outcomes::{MetricIn, MetricOut};
use super::service_checks::{
    ATTR_SERVICE_CHECK_HOST, ATTR_SERVICE_CHECK_MESSAGE, ATTR_SERVICE_CHECK_NAME,
    ATTR_SERVICE_CHECK_STATUS,
};
use super::tags::{count_tags, insert_tags, render_tags};
use super::time::{nanos_to_seconds, seconds_to_nanos};
use super::{
    host_tag, is_datadog_stats, is_service_check, DatadogDecoder, DatadogEncoder, HistogramMode,
    HistogramOptions, ATTR_DEVICE, ATTR_HOSTLESS, ATTR_HOST_NAME, ATTR_HOST_TAG, ATTR_INTERVAL,
    ATTR_ORIGIN_CATEGORY, ATTR_ORIGIN_METRIC_TYPE, ATTR_ORIGIN_PRODUCT, ATTR_ORIGIN_SERVICE,
    ATTR_RESOURCES, ATTR_SOURCE_TYPE_NAME, ATTR_TYPE, MAX_METRIC_TAGS, RESOURCE_ATTR_AGENT_EPOCH,
    RESOURCE_ATTR_AGENT_INTERNAL_IP, RESOURCE_ATTR_AGENT_PUBLIC_IP, RESOURCE_ATTR_AGENT_TIMEZONE,
    RESOURCE_ATTR_AGENT_VERSION,
};
use crate::CodecError;
use bytes::Bytes;
use logit_core::attrs::merged;
use logit_core::interner::{intern, resolve};
use logit_core::sketch::from_otlp::{exp_histogram_oversized, otlp_buckets};
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, MetricValue, Resource, Samples, Symbol,
    Temporality, Value,
};
use prost::Message;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

type JsonMap = Map<String, JsonValue>;

/// `_logit.datadog.type`'s two values; `count` and `gauge` are said by the model kind itself.
const TYPE_RATE: &str = "rate";
const TYPE_UNSPECIFIED: &str = "unspecified";

/// The resource `type` the Agent's v2 serializer gives a series' `device`.
const RESOURCE_HOST: &str = "host";
const RESOURCE_DEVICE: &str = "device";

/// A Datadog metric type, as every series route spells it one way or another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WireType {
    Unspecified,
    Count,
    Rate,
    Gauge,
}

impl WireType {
    fn from_proto(code: i64) -> Option<Self> {
        Some(match code {
            0 => WireType::Unspecified,
            1 => WireType::Count,
            2 => WireType::Rate,
            3 => WireType::Gauge,
            _ => return None,
        })
    }

    fn proto(self) -> i32 {
        match self {
            WireType::Unspecified => metric_payload::MetricType::Unspecified as i32,
            WireType::Count => metric_payload::MetricType::Count as i32,
            WireType::Rate => metric_payload::MetricType::Rate as i32,
            WireType::Gauge => metric_payload::MetricType::Gauge as i32,
        }
    }

    fn from_v1(name: &str) -> Option<Self> {
        Some(match name {
            "" => WireType::Unspecified,
            "count" => WireType::Count,
            "rate" => WireType::Rate,
            "gauge" => WireType::Gauge,
            _ => return None,
        })
    }

    fn v1(self) -> &'static str {
        match self {
            WireType::Unspecified => "",
            WireType::Count => "count",
            WireType::Rate => "rate",
            WireType::Gauge => "gauge",
        }
    }

    fn kind(self, value: f64) -> MetricKind {
        match self {
            WireType::Count => MetricKind::counter(value),
            WireType::Gauge | WireType::Rate | WireType::Unspecified => MetricKind::gauge(value),
        }
    }

    fn type_attr(self) -> Option<&'static str> {
        match self {
            WireType::Rate => Some(TYPE_RATE),
            WireType::Unspecified => Some(TYPE_UNSPECIFIED),
            WireType::Count | WireType::Gauge => None,
        }
    }
}

// -- decode ---------------------------------------------------------------------------------------

/// One series' request-level fields, whichever route they came from, folded into the attributes
/// every one of its points shares. Also what [`super::sketches`] builds a `Sketch`'s attributes
/// with.
#[derive(Default)]
pub(super) struct WireSeries<'a> {
    pub tags: Vec<&'a str>,
    pub host: Option<&'a str>,
    pub device: Option<&'a str>,
    /// Every resource that isn't the first non-empty `host` or `device`, in wire order.
    pub resources: Vec<(&'a str, &'a str)>,
    pub source_type_name: Option<&'a str>,
    pub interval: i64,
    pub wire_type: Option<WireType>,
    pub origin: OriginCodes,
}

/// `_logit.datadog.origin.*`: the protobuf `Origin`'s three codes plus the JSON API's
/// `metric_type`. A zero code is absent, the protobuf default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct OriginCodes {
    pub product: u64,
    pub category: u64,
    pub service: u64,
    pub metric_type: u64,
}

impl OriginCodes {
    pub(super) fn from_proto(metadata: Option<&Metadata>) -> Self {
        match metadata.and_then(|m| m.origin.as_ref()) {
            None => Self::default(),
            Some(o) => OriginCodes {
                product: o.origin_product.into(),
                category: o.origin_category.into(),
                service: o.origin_service.into(),
                metric_type: 0,
            },
        }
    }
}

impl<'a> WireSeries<'a> {
    /// Sorts `(type, name)` resources into `host`, `device`, and the rest.
    fn push_resource(&mut self, kind: &'a str, name: &'a str) {
        if kind == RESOURCE_HOST && self.host.is_none() && !name.is_empty() {
            self.host = Some(name);
        } else if kind == RESOURCE_DEVICE && self.device.is_none() && !name.is_empty() {
            self.device = Some(name);
        } else {
            self.resources.push((kind, name));
        }
    }

    /// Tags first, then the wire's own fields, so a tag spelled like a carrier (`host.name:x`)
    /// loses to the field it names. Also returns how many tags were skipped for a private key,
    /// for the caller to count once the series delivers a point.
    pub(super) fn attributes(&self) -> (AttrMap, usize) {
        let mut attrs = AttrMap::new();
        let reserved = insert_tags(&mut attrs, self.tags.iter().copied());
        match self.host {
            Some("") => attrs.insert(ATTR_HOSTLESS, Value::Bool(true)),
            Some(host) => attrs.insert(ATTR_HOST_NAME, Value::str(host)),
            None => {}
        }
        if let Some(device) = self.device.filter(|d| !d.is_empty()) {
            attrs.insert(ATTR_DEVICE, Value::str(device));
        }
        if !self.resources.is_empty() {
            let items = self
                .resources
                .iter()
                .map(|(kind, name)| {
                    let mut m = AttrMap::new();
                    m.insert("type", Value::str(*kind));
                    m.insert("name", Value::str(*name));
                    Value::Map(Box::new(m))
                })
                .collect();
            attrs.insert(ATTR_RESOURCES, Value::Array(items));
        }
        if let Some(name) = self.source_type_name.filter(|s| !s.is_empty()) {
            attrs.insert(ATTR_SOURCE_TYPE_NAME, Value::str(name));
        }
        if self.interval != 0 {
            attrs.insert(ATTR_INTERVAL, Value::I64(self.interval));
        }
        // A route that carries a type always decides `_logit.datadog.type`, from the wire type
        // alone.
        if let Some(wire_type) = self.wire_type {
            match wire_type.type_attr() {
                Some(t) => attrs.insert(ATTR_TYPE, Value::str(t)),
                None => {
                    attrs.remove(ATTR_TYPE);
                }
            }
        }
        for (key, code) in [
            (ATTR_ORIGIN_PRODUCT, self.origin.product),
            (ATTR_ORIGIN_CATEGORY, self.origin.category),
            (ATTR_ORIGIN_SERVICE, self.origin.service),
            (ATTR_ORIGIN_METRIC_TYPE, self.origin.metric_type),
        ] {
            if code != 0 {
                attrs.insert(key, Value::U64(code));
            }
        }
        (attrs, reserved)
    }
}

/// A point's wire timestamp in whole seconds to `Event::timestamp`: absent or non-positive is
/// `received_at`, truncated to its second so every metric timestamp is a whole second.
fn point_timestamp(seconds: Option<i64>, received_at: i64) -> (i64, bool) {
    match seconds {
        Some(s) if s > 0 => (seconds_to_nanos(s), false),
        _ => (seconds_to_nanos(nanos_to_seconds(received_at)), true),
    }
}

/// A JSON timestamp (an integer, or the v1 public API's double) to whole seconds, truncated.
fn json_seconds(v: &JsonValue) -> Option<i64> {
    if let Some(i) = v.as_i64() {
        return Some(i);
    }
    if v.as_u64().is_some() {
        return Some(i64::MAX);
    }
    let f = v.as_f64()?;
    // `as` saturates, and JSON has no non-finite literal.
    Some(f.floor() as i64)
}

/// A JSON integer field that must be integral: an integer, or a double with no fraction.
fn json_integer(v: &JsonValue) -> Option<i64> {
    if let Some(i) = v.as_i64() {
        return Some(i);
    }
    let f = v.as_f64()?;
    (f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64).then_some(f as i64)
}

/// `null` is the same as absent, as in `crate::otlp::json`.
fn get<'a>(obj: &'a JsonMap, key: &str) -> Option<&'a JsonValue> {
    obj.get(key).filter(|v| !v.is_null())
}

fn opt_str<'a>(obj: &'a JsonMap, key: &str) -> Result<Option<&'a str>, String> {
    match get(obj, key) {
        None => Ok(None),
        Some(JsonValue::String(s)) => Ok(Some(s)),
        Some(_) => Err(format!("`{key}` is not a string")),
    }
}

fn json_tags(obj: &JsonMap) -> Result<Vec<&str>, String> {
    match get(obj, "tags") {
        None => Ok(Vec::new()),
        Some(JsonValue::Array(items)) => items
            .iter()
            .map(|t| t.as_str().ok_or_else(|| "a tag is not a string".to_string()))
            .collect(),
        Some(_) => Err("`tags` is not an array".to_string()),
    }
}

fn json_metric(obj: &JsonMap) -> Result<&str, String> {
    match opt_str(obj, "metric")? {
        Some(m) if !m.is_empty() => Ok(m),
        _ => Err("no `metric` name".to_string()),
    }
}

fn json_points(obj: &JsonMap) -> Result<&[JsonValue], String> {
    match get(obj, "points") {
        Some(JsonValue::Array(points)) => Ok(points),
        _ => Err("no `points` array".to_string()),
    }
}

fn json_interval(obj: &JsonMap) -> Result<i64, String> {
    match get(obj, "interval") {
        None => Ok(0),
        Some(v) => json_integer(v).ok_or_else(|| "`interval` is not an integer".to_string()),
    }
}

fn json_u64(obj: &JsonMap, key: &str) -> Result<u64, String> {
    match get(obj, key) {
        None => Ok(0),
        Some(v) => v.as_u64().ok_or_else(|| format!("`{key}` is not an unsigned integer")),
    }
}

fn parse_json(body: &[u8]) -> Result<JsonValue, CodecError> {
    serde_json::from_slice(body)
        .map_err(|e| CodecError::Malformed(format!("datadog metrics body is not JSON: {e}")))
}

/// The body's `series` array. An object with no `series` member is an empty request: the Agent
/// probes each route with `{}` when it starts, and Datadog's intake answers that `202`.
fn series_array(root: &JsonValue) -> Result<&[JsonValue], CodecError> {
    let malformed =
        || CodecError::Malformed("datadog metrics body has no `series` array".to_string());
    let object = root.as_object().ok_or_else(malformed)?;
    match object.get("series") {
        None => Ok(&[]),
        Some(series) => series.as_array().map(Vec::as_slice).ok_or_else(malformed),
    }
}

fn batch(events: Vec<Event>) -> EventBatch {
    EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
}

impl DatadogDecoder {
    pub(super) fn metric_not_decoded(&self, outcome: MetricIn, n: usize) {
        if n > 0 {
            let reason = outcome.reason();
            self.telemetry.count(outcome.metric(), n as f64, &[("reason", reason)]);
        }
    }

    fn no_timestamp(&self) {
        self.telemetry.count("logit.input.metrics.degraded", 1.0, &[("reason", "no_timestamp")]);
    }

    /// Drops one series (or sketch) whose shape is wrong, keeping the rest of the request.
    pub(super) fn bad_series(&mut self, outcome: MetricIn, why: impl std::fmt::Display) {
        self.metric_not_decoded(outcome, 1);
        let key = outcome.reason();
        self.diagnostics.warn_throttled(key, format!("datadog: {key} dropped: {why}"));
    }

    /// Pushes one `Event` per point of a decoded series.
    fn push_points(
        &mut self,
        out: &mut Vec<Event>,
        metric: &str,
        unit: Option<&str>,
        series: &WireSeries<'_>,
        points: impl IntoIterator<Item = (Option<i64>, f64)>,
        received_at: i64,
    ) {
        let name = intern(metric);
        let unit = unit.filter(|u| !u.is_empty()).map(intern);
        let wire_type = series.wire_type.unwrap_or(WireType::Gauge);
        let (attrs, reserved) = series.attributes();
        let first = out.len();
        for (seconds, value) in points {
            if !value.is_finite() {
                self.metric_not_decoded(MetricIn::NonFiniteValue, 1);
                continue;
            }
            let (timestamp, defaulted) = point_timestamp(seconds, received_at);
            if defaulted {
                self.no_timestamp();
            }
            let mut record = MetricRecord::new(name, wire_type.kind(value));
            record.unit = unit;
            out.push(Event::metric(timestamp, attrs.clone(), record));
        }
        if out.len() > first {
            self.reserved_tags(reserved);
        }
    }

    /// `/api/v2/series` as the Agent sends it: a protobuf `MetricPayload`, already decompressed.
    pub fn decode_series_v2_protobuf(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let payload = MetricPayload::decode(body).map_err(|e| {
            CodecError::Malformed(format!("datadog MetricPayload does not decode: {e}"))
        })?;
        let mut out = Vec::new();
        for s in &payload.series {
            if s.metric.is_empty() {
                self.bad_series(MetricIn::BadSeries, "a series has no metric name");
                continue;
            }
            let Some(wire_type) = WireType::from_proto(s.r#type.into()) else {
                self.bad_series(
                    MetricIn::BadSeries,
                    format_args!("unknown metric type {}", s.r#type),
                );
                continue;
            };
            let mut series = WireSeries {
                tags: s.tags.iter().map(String::as_str).collect(),
                source_type_name: Some(&s.source_type_name),
                interval: s.interval,
                wire_type: Some(wire_type),
                origin: OriginCodes::from_proto(s.metadata.as_ref()),
                ..WireSeries::default()
            };
            for r in &s.resources {
                series.push_resource(&r.r#type, &r.name);
            }
            let points = s.points.iter().map(|p| (Some(p.timestamp), p.value));
            self.push_points(&mut out, &s.metric, Some(&s.unit), &series, points, received_at);
        }
        Ok(batch(out))
    }

    /// `/api/v2/series` as the public API takes it: `{"series":[{"metric","type":<int>,
    /// "points":[{"timestamp","value"}],"resources":[{"type","name"}],...}]}`.
    pub fn decode_series_v2_json(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let root = parse_json(body)?;
        let mut out = Vec::new();
        for item in series_array(&root)? {
            match v2_json_series(item) {
                Ok((metric, unit, series, points)) => {
                    let (points, bad) = points;
                    self.metric_not_decoded(MetricIn::BadPoint, bad);
                    self.push_points(&mut out, metric, unit, &series, points, received_at);
                }
                Err(why) => self.bad_series(MetricIn::BadSeries, why),
            }
        }
        Ok(batch(out))
    }

    /// `/api/v1/series`: `{"series":[{"metric","points":[[ts, value]],"tags","host","device",
    /// "type":"gauge"|"count"|"rate"|"","interval","source_type_name","unit"}]}`.
    pub fn decode_series_v1(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let root = parse_json(body)?;
        let mut out = Vec::new();
        for item in series_array(&root)? {
            match v1_json_series(item) {
                Ok((metric, unit, series, points)) => {
                    let mut kept = Vec::with_capacity(points.len());
                    for point in points {
                        match point {
                            V1Point::Ok(ts, v) => kept.push((ts, v)),
                            V1Point::Null => self.metric_not_decoded(MetricIn::NullValue, 1),
                            V1Point::Bad => self.metric_not_decoded(MetricIn::BadPoint, 1),
                        }
                    }
                    self.push_points(&mut out, metric, unit, &series, kept, received_at);
                }
                Err(why) => self.bad_series(MetricIn::BadSeries, why),
            }
        }
        Ok(batch(out))
    }

    /// `/api/v1/distribution_points`: `{"series":[{"metric","points":[[ts,[v, ...]]],"host",
    /// "tags","type":"distribution"}]}`, one `Samples` event per point.
    pub fn decode_distribution_points(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let root = parse_json(body)?;
        let mut out = Vec::new();
        for item in series_array(&root)? {
            let parsed = (|| -> Result<_, String> {
                let obj = item.as_object().ok_or("a series is not an object")?;
                let metric = json_metric(obj)?;
                let series = WireSeries {
                    tags: json_tags(obj)?,
                    host: opt_str(obj, "host")?,
                    ..WireSeries::default()
                };
                Ok((metric, series, json_points(obj)?))
            })();
            let (metric, series, points) = match parsed {
                Ok(p) => p,
                Err(why) => {
                    self.bad_series(MetricIn::BadSeries, why);
                    continue;
                }
            };
            let name = intern(metric);
            let (attrs, reserved) = series.attributes();
            let first = out.len();
            for point in points {
                let Some((seconds, values)) = distribution_point(point) else {
                    self.metric_not_decoded(MetricIn::BadPoint, 1);
                    continue;
                };
                if values.is_empty() {
                    self.metric_not_decoded(MetricIn::EmptyDistribution, 1);
                    continue;
                }
                let (timestamp, defaulted) = point_timestamp(seconds, received_at);
                if defaulted {
                    self.no_timestamp();
                }
                let record = MetricRecord::new(name, MetricKind::Samples(Samples::new(values)));
                out.push(Event::metric(timestamp, attrs.clone(), record));
            }
            if out.len() > first {
                self.reserved_tags(reserved);
            }
        }
        Ok(batch(out))
    }
}

type ParsedSeries<'a, P> = (&'a str, Option<&'a str>, WireSeries<'a>, P);
/// A v2 JSON series' good points, and how many malformed ones it had.
type V2Points = (Vec<(Option<i64>, f64)>, usize);

/// A v2 JSON series and its points; a malformed point is dropped and counted, not the series.
fn v2_json_series(item: &JsonValue) -> Result<ParsedSeries<'_, V2Points>, String> {
    let obj = item.as_object().ok_or("a series is not an object")?;
    let metric = json_metric(obj)?;
    let wire_type = match get(obj, "type") {
        None => WireType::Unspecified,
        Some(v) => v
            .as_i64()
            .and_then(WireType::from_proto)
            .ok_or_else(|| format!("unknown metric type {v}"))?,
    };
    let origin = match get(obj, "metadata").map(|m| m.as_object().and_then(|m| get(m, "origin"))) {
        None => OriginCodes::default(),
        Some(Some(JsonValue::Object(o))) => OriginCodes {
            product: json_u64(o, "product")?,
            category: 0,
            service: json_u64(o, "service")?,
            metric_type: json_u64(o, "metric_type")?,
        },
        Some(None) => OriginCodes::default(),
        Some(Some(_)) => return Err("`metadata.origin` is not an object".to_string()),
    };
    let mut series = WireSeries {
        tags: json_tags(obj)?,
        source_type_name: opt_str(obj, "source_type_name")?,
        interval: json_interval(obj)?,
        wire_type: Some(wire_type),
        origin,
        ..WireSeries::default()
    };
    match get(obj, "resources") {
        None => {}
        Some(JsonValue::Array(resources)) => {
            for r in resources {
                let r = r.as_object().ok_or("a resource is not an object")?;
                let kind = opt_str(r, "type")?.unwrap_or("");
                let name = opt_str(r, "name")?.unwrap_or("");
                series.push_resource(kind, name);
            }
        }
        Some(_) => return Err("`resources` is not an array".to_string()),
    }
    let mut points = Vec::new();
    let mut bad = 0;
    for p in json_points(obj)? {
        let parsed = p.as_object().and_then(|p| {
            let value = get(p, "value")?.as_f64()?;
            let seconds = match get(p, "timestamp") {
                None => None,
                Some(ts) => Some(json_seconds(ts)?),
            };
            Some((seconds, value))
        });
        match parsed {
            Some(point) => points.push(point),
            None => bad += 1,
        }
    }
    Ok((metric, opt_str(obj, "unit")?, series, (points, bad)))
}

enum V1Point {
    Ok(Option<i64>, f64),
    Null,
    Bad,
}

fn v1_json_series(item: &JsonValue) -> Result<ParsedSeries<'_, Vec<V1Point>>, String> {
    let obj = item.as_object().ok_or("a series is not an object")?;
    let metric = json_metric(obj)?;
    let wire_type = match opt_str(obj, "type")? {
        None => WireType::Gauge,
        Some(t) => WireType::from_v1(t).ok_or_else(|| format!("unknown metric type {t:?}"))?,
    };
    let series = WireSeries {
        tags: json_tags(obj)?,
        host: opt_str(obj, "host")?,
        device: opt_str(obj, "device")?,
        source_type_name: opt_str(obj, "source_type_name")?,
        interval: json_interval(obj)?,
        wire_type: Some(wire_type),
        ..WireSeries::default()
    };
    let points = json_points(obj)?
        .iter()
        .map(|p| match p.as_array().map(Vec::as_slice) {
            Some([ts, value]) => {
                let seconds = if ts.is_null() {
                    None
                } else {
                    match json_seconds(ts) {
                        Some(s) => Some(s),
                        None => return V1Point::Bad,
                    }
                };
                match value {
                    JsonValue::Null => V1Point::Null,
                    v => v.as_f64().map_or(V1Point::Bad, |v| V1Point::Ok(seconds, v)),
                }
            }
            _ => V1Point::Bad,
        })
        .collect();
    Ok((metric, opt_str(obj, "unit")?, series, points))
}

fn distribution_point(point: &JsonValue) -> Option<(Option<i64>, Vec<f64>)> {
    let [ts, values] = point.as_array()?.as_slice() else { return None };
    let seconds = if ts.is_null() { None } else { Some(json_seconds(ts)?) };
    let values = values.as_array()?.iter().map(JsonValue::as_f64).collect::<Option<Vec<_>>>()?;
    Some((seconds, values))
}

// -- encode ---------------------------------------------------------------------------------------

/// Every attribute a metrics route reads into a wire field of its own; never rendered as a tag.
static CONSUMED: LazyLock<[Symbol; 16]> = LazyLock::new(|| {
    [
        intern(ATTR_HOST_NAME),
        intern(ATTR_HOSTLESS),
        intern(ATTR_TYPE),
        intern(ATTR_INTERVAL),
        intern(ATTR_SOURCE_TYPE_NAME),
        intern(ATTR_DEVICE),
        intern(ATTR_RESOURCES),
        intern(ATTR_ORIGIN_PRODUCT),
        intern(ATTR_ORIGIN_CATEGORY),
        intern(ATTR_ORIGIN_SERVICE),
        intern(ATTR_ORIGIN_METRIC_TYPE),
        intern(RESOURCE_ATTR_AGENT_VERSION),
        intern(RESOURCE_ATTR_AGENT_TIMEZONE),
        intern(RESOURCE_ATTR_AGENT_EPOCH),
        intern(RESOURCE_ATTR_AGENT_INTERNAL_IP),
        intern(RESOURCE_ATTR_AGENT_PUBLIC_IP),
    ]
});

/// [`CONSUMED`] plus a service check's own carriers: on a check's later records (its first is
/// the service-checks route's), those belong to the check and are never rendered as tags. The
/// private-key skip already keeps them off the wire; this list is belt and braces.
static CONSUMED_WITH_CHECK: LazyLock<Vec<Symbol>> = LazyLock::new(|| {
    let mut keys = CONSUMED.to_vec();
    keys.extend(
        [
            ATTR_SERVICE_CHECK_NAME,
            ATTR_SERVICE_CHECK_STATUS,
            ATTR_SERVICE_CHECK_MESSAGE,
            ATTR_SERVICE_CHECK_HOST,
        ]
        .map(intern),
    );
    keys
});

/// The carriers of one event (resource merged with event attributes), read back into wire fields,
/// plus the remaining attributes rendered as tags.
#[derive(Default)]
pub(super) struct Carriers {
    pub tags: Vec<String>,
    pub host: Option<String>,
    /// The sender marked the sample hostless: [`ATTR_HOSTLESS`] is `true`, the DogStatsD `#host:`
    /// tag is empty, or [`ATTR_RESOURCES`] holds a `host` entry. Never filled with the default
    /// host.
    pub hostless: bool,
    pub device: Option<String>,
    pub resources: Vec<(String, String)>,
    pub source_type_name: Option<String>,
    pub interval: i64,
    /// `_logit.datadog.type`'s value, when it is one this codec knows.
    pub wire_type: Option<WireType>,
    pub origin: OriginCodes,
    /// How many `_logit.datadog.agent.*` carriers (a sketch payload's `CommonMetadata`) are
    /// present; only the sketches route writes them, from the batch resource.
    pub agent_metadata: usize,
}

impl Carriers {
    /// Which carriers are present that `route` has no field for, for a `no_wire_form` count.
    fn unwritable(&self, route: Route) -> usize {
        let origin = |set: bool| usize::from(set);
        match route {
            Route::V1 => {
                self.agent_metadata
                    + usize::from(!self.resources.is_empty())
                    + origin(self.origin.product != 0)
                    + origin(self.origin.category != 0)
                    + origin(self.origin.service != 0)
                    + origin(self.origin.metric_type != 0)
            }
            // v2 carries `device` as a resource; the JSON origin has no `category`.
            Route::V2Json => self.agent_metadata + origin(self.origin.category != 0),
            Route::V2Protobuf => self.agent_metadata + origin(self.origin.metric_type != 0),
            Route::Distribution => {
                self.agent_metadata
                    + usize::from(self.device.is_some())
                    + usize::from(!self.resources.is_empty())
                    + usize::from(self.source_type_name.is_some())
                    + usize::from(self.interval != 0)
                    + usize::from(self.wire_type.is_some())
                    + origin(self.origin.product != 0)
                    + origin(self.origin.category != 0)
                    + origin(self.origin.service != 0)
                    + origin(self.origin.metric_type != 0)
            }
            Route::Sketches => {
                usize::from(self.device.is_some())
                    + usize::from(!self.resources.is_empty())
                    + usize::from(self.source_type_name.is_some())
                    + usize::from(self.interval != 0)
                    + usize::from(self.wire_type.is_some())
                    + origin(self.origin.metric_type != 0)
            }
        }
    }

    /// The v1 and distribution-point `host`: the event's host, `""` for a hostless sample, and
    /// `None`, the field omitted, for an event with neither, so an absent `host` stays absent.
    fn json_host(&self) -> Option<&str> {
        self.host.as_deref().or(self.hostless.then_some(""))
    }

    /// The v2 `resources` list: `host` first, then `device`, then `_logit.datadog.resources` in
    /// order.
    fn resource_list(&self) -> Vec<(&str, &str)> {
        let mut out = Vec::with_capacity(self.resources.len() + 2);
        if let Some(host) = &self.host {
            out.push((RESOURCE_HOST, host.as_str()));
        } else if self.hostless && !self.resources.iter().any(|(kind, _)| kind == RESOURCE_HOST) {
            // The Agent's v2 spelling of a hostless sample, which a later hop keeps unfilled.
            out.push((RESOURCE_HOST, ""));
        }
        if let Some(device) = &self.device {
            out.push((RESOURCE_DEVICE, device.as_str()));
        }
        out.extend(self.resources.iter().map(|(t, n)| (t.as_str(), n.as_str())));
        out
    }

    pub(super) fn proto_metadata(&self) -> Option<Metadata> {
        let o = &self.origin;
        (o.product != 0 || o.category != 0 || o.service != 0).then(|| Metadata {
            origin: Some(Origin {
                origin_product: saturate_u32(o.product),
                origin_category: saturate_u32(o.category),
                origin_service: saturate_u32(o.service),
            }),
        })
    }
}

fn saturate_u32(v: u64) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Route {
    V1,
    V2Json,
    V2Protobuf,
    Distribution,
    Sketches,
}

fn non_empty_str(v: &Value) -> Option<Option<String>> {
    match v {
        Value::Str(_) => {
            let s = v.as_str().unwrap_or_default();
            Some((!s.is_empty()).then(|| s.to_string()))
        }
        _ => None,
    }
}

fn unsigned(v: &Value) -> Option<u64> {
    match v {
        Value::U64(n) => Some(*n),
        Value::I64(n) => u64::try_from(*n).ok(),
        _ => None,
    }
}

fn resource_entry(v: &Value) -> Option<(String, String)> {
    let Value::Map(m) = v else { return None };
    let kind = m.get("type")?.as_str()?;
    let name = m.get("name")?.as_str()?;
    Some((kind.to_string(), name.to_string()))
}

impl DatadogEncoder {
    pub(super) fn metric_not_sent(&self, outcome: MetricOut) {
        let (metric, tag) = outcome.counter();
        self.telemetry.count(metric, 1.0, &[tag]);
    }

    pub(super) fn out_degraded(&self, reason: &'static str) {
        self.telemetry.count("logit.output.metrics.degraded", 1.0, &[("reason", reason)]);
    }

    pub(super) fn tags_omitted(&self, reason: &'static str, n: usize) {
        if n > 0 {
            self.telemetry.count("logit.output.tags.omitted", n as f64, &[("reason", reason)]);
        }
    }

    /// Reads one event's carriers and renders the rest of its attributes as tags, counting
    /// whatever `route` can't write.
    pub(super) fn carriers(&self, resource: &Resource, event: &Event, route: Route) -> Carriers {
        let consumed = &*CONSUMED;
        let skip = tag_skip(resource, event);
        let mut c = Carriers::default();
        let dropped = render_tags(merged(resource, event), skip, &mut c.tags);
        let mut unrepresentable = dropped.unrepresentable;
        // A `host.name` that isn't a `Str` is the event's own host, unwritable: counted, and
        // never replaced by the default host.
        let mut own_host_unwritable = false;
        for (key, value) in merged(resource, event) {
            if !consumed.contains(&key) {
                continue;
            }
            let ok = match resolve(key) {
                ATTR_HOST_NAME => {
                    let ok = non_empty_str(value).map(|s| c.host = s).is_some();
                    own_host_unwritable |= !ok;
                    ok
                }
                ATTR_HOSTLESS => match value {
                    Value::Bool(b) => {
                        c.hostless = *b;
                        true
                    }
                    _ => false,
                },
                ATTR_DEVICE => non_empty_str(value).map(|s| c.device = s).is_some(),
                ATTR_SOURCE_TYPE_NAME => {
                    non_empty_str(value).map(|s| c.source_type_name = s).is_some()
                }
                ATTR_TYPE => match value.as_str() {
                    Some(TYPE_RATE) => {
                        c.wire_type = Some(WireType::Rate);
                        true
                    }
                    Some(TYPE_UNSPECIFIED) => {
                        c.wire_type = Some(WireType::Unspecified);
                        true
                    }
                    _ => false,
                },
                ATTR_INTERVAL => match value {
                    Value::I64(n) => {
                        c.interval = *n;
                        true
                    }
                    Value::U64(n) => i64::try_from(*n).map(|n| c.interval = n).is_ok(),
                    _ => false,
                },
                ATTR_RESOURCES => match value {
                    Value::Array(items) => {
                        let parsed: Option<Vec<_>> = items.iter().map(resource_entry).collect();
                        parsed.map(|r| c.resources = r).is_some()
                    }
                    _ => false,
                },
                ATTR_ORIGIN_PRODUCT => unsigned(value).map(|n| c.origin.product = n).is_some(),
                ATTR_ORIGIN_CATEGORY => unsigned(value).map(|n| c.origin.category = n).is_some(),
                ATTR_ORIGIN_SERVICE => unsigned(value).map(|n| c.origin.service = n).is_some(),
                ATTR_ORIGIN_METRIC_TYPE => {
                    unsigned(value).map(|n| c.origin.metric_type = n).is_some()
                }
                RESOURCE_ATTR_AGENT_EPOCH => {
                    let ok = matches!(value, Value::F64(_));
                    c.agent_metadata += usize::from(ok);
                    ok
                }
                RESOURCE_ATTR_AGENT_VERSION
                | RESOURCE_ATTR_AGENT_TIMEZONE
                | RESOURCE_ATTR_AGENT_INTERNAL_IP
                | RESOURCE_ATTR_AGENT_PUBLIC_IP => {
                    let ok = matches!(value, Value::Str(_));
                    c.agent_metadata += usize::from(ok);
                    ok
                }
                _ => true,
            };
            if !ok {
                unrepresentable += 1;
            }
        }
        self.tags_omitted("unrepresentable", unrepresentable);
        self.tags_omitted("no_wire_form", c.unwritable(route));
        if c.host.is_none() && !own_host_unwritable {
            match host_tag(resource, event) {
                Some(tag) if !tag.is_empty() => c.host = Some(tag.to_string()),
                // An empty `#host:` tag: marked hostless, as the Agent reads it.
                Some(_) => c.hostless = true,
                // The Agent's v2 spelling of a hostless series: a `host` resource with no name.
                None if c.resources.iter().any(|(kind, _)| *kind == RESOURCE_HOST) => {
                    c.hostless = true;
                }
                // The v1, distribution, and sketch spelling: `"host":""`.
                None if c.hostless => {}
                None => c.host = self.default_host.as_deref().map(str::to_string),
            }
        }
        c
    }

    /// Counts the notes [`plan_series`] left for one record. A note about how a written series
    /// looks (a degrade, a normalization, a summary's left-off count and sum) counts only when
    /// `written`: none of a record whose every series the tag limit leaves off. A missing sum
    /// counts when the series route writes the record, or when `sketched`: the sketches route
    /// takes it under the tag limit. That route can still skip or drop it (a conversion error,
    /// `empty_sketch`, `oversized_sketch`, or the sink's `oversize`), and the missing sum counts
    /// all the same, since telling those apart here would run the sketch conversion twice.
    fn count_notes(&mut self, notes: &[Note], written: bool, sketched: bool) {
        for &note in notes {
            match note {
                Note::NotSent(outcome) => self.metric_not_sent(outcome),
                Note::Cumulative(kind) => self.unresolved_cumulative(kind),
                Note::Degraded(_)
                | Note::DegradedReason(_)
                | Note::BoolValue
                | Note::SummaryCumulative
                    if !written => {}
                Note::NoSum if !(written || sketched) => {}
                Note::Degraded(kind) => self.degraded_kind(kind),
                Note::DegradedReason(reason) => self.out_degraded(reason),
                Note::BoolValue => self.telemetry.count(
                    "logit.output.metrics.normalized",
                    1.0,
                    &[("reason", "bool_value")],
                ),
                Note::NoSum => self.telemetry.count(
                    "logit.output.fields.omitted",
                    1.0,
                    &[("reason", "no_sum")],
                ),
                Note::SummaryCumulative => {
                    self.telemetry.count(
                        "logit.output.fields.dropped",
                        1.0,
                        &[("reason", "unresolved_cumulative")],
                    );
                    self.diagnostics.warn_throttled(
                        "unresolved_cumulative",
                        "datadog: a cumulative summary's .count and .sum aren't sent: they're \
                         running totals, which Datadog's exporter differences first -- place a \
                         `cumulative_to_delta` component ahead of this output",
                    );
                }
            }
        }
    }

    /// `logit.output.metrics.degraded{metric_kind}`: a record written in a lesser form, once.
    pub(super) fn degraded_kind(&self, kind: &'static str) {
        self.telemetry.count("logit.output.metrics.degraded", 1.0, &[("metric_kind", kind)]);
    }

    /// A cumulative kind the translator would difference, dropped: a `cumulative_to_delta`
    /// upstream converts it (ADR `datadog-otlp-metric-mapping`'s "Per kind").
    pub(super) fn unresolved_cumulative(&mut self, kind: &str) {
        self.metric_not_sent(MetricOut::UnresolvedCumulative);
        self.diagnostics.warn_throttled(
            "unresolved_cumulative",
            format_args!(
                "datadog: a cumulative {kind} isn't sent: Datadog's exporter differences it into \
                 a delta first -- place a `cumulative_to_delta` component ahead of this output"
            ),
        );
    }

    /// Calls `f` once per `(event, record, series)` a series route sends, in batch order. A
    /// series whose tags, the event's and its own together, pass [`MAX_METRIC_TAGS`] is left off,
    /// uncounted: the sink counts it before encoding ([`series_over_tag_limit`]).
    ///
    /// Of several points of one series in one second, only the latest is sent, the later in batch
    /// order on a tie; each other is left off and counted
    /// `dropped{reason="duplicate_timestamp"}`. Datadog keeps the higher of two such points in
    /// one request, not the later, and doesn't sum two counts, so sending both loses one of them
    /// uncounted. [`DatadogEncoder::series_duplicates`] reports how many this call left off.
    fn each_series(
        &mut self,
        batch: &EventBatch,
        route: Route,
        mut f: impl FnMut(&Event, &MetricRecord, &Carriers, &SeriesPoint),
    ) {
        self.series_duplicates = 0;
        // Each event's carriers, read only for an event with a series to write.
        let mut carriers: Vec<Option<Carriers>> = Vec::with_capacity(batch.events.len());
        // Every series point to write: (event, record, point), in batch order.
        let mut plans: Vec<(usize, &MetricRecord, Vec<SeriesPoint>)> = Vec::new();
        for (index, event) in batch.events.iter().enumerate() {
            carriers.push(None);
            // The tags `carriers` renders, counted without counting what it omits.
            let base = metric_tag_count(&batch.resource, event);
            for record in &event.metrics[first_metric(&batch.resource, event)..] {
                if !on_series_route(record) {
                    continue;
                }
                let plan = plan_series(record, self.histograms);
                let points: Vec<SeriesPoint> = plan
                    .points
                    .into_iter()
                    .filter(|p| base + p.extra_tags() <= MAX_METRIC_TAGS)
                    .collect();
                // The sketches route drops an event over the limit whole.
                let sketched = on_sketch_route(record, self.histograms) && base <= MAX_METRIC_TAGS;
                self.count_notes(&plan.notes, !points.is_empty(), sketched);
                if points.is_empty() {
                    continue;
                }
                // Read lazily: an event with nothing written on this route counts nothing.
                if carriers[index].is_none() {
                    carriers[index] = Some(self.carriers(&batch.resource, event, route));
                }
                plans.push((index, record, points));
            }
        }

        // Which points survive: the latest of each (series, second).
        let flat: Vec<(usize, &MetricRecord, &SeriesPoint)> = plans
            .iter()
            .flat_map(|(index, record, points)| points.iter().map(move |p| (*index, *record, p)))
            .collect();
        let mut keep = vec![true; flat.len()];
        if flat.len() > 1 {
            // Each point's group, a group being one (series, second), and each group's latest
            // point, the later in batch order on a tie.
            let mut groups: HashMap<SeriesKey<'_>, usize> = HashMap::with_capacity(flat.len());
            let mut group_of = Vec::with_capacity(flat.len());
            let mut latest: Vec<usize> = Vec::new();
            for (n, &(index, record, p)) in flat.iter().enumerate() {
                let timestamp = batch.events[index].timestamp;
                let c = carriers[index].as_ref().expect("read for every planned event");
                let key = SeriesKey::new(record, p, c, nanos_to_seconds(timestamp));
                let next = latest.len();
                let group = *groups.entry(key).or_insert(next);
                if group == next {
                    latest.push(n);
                } else if timestamp >= batch.events[flat[latest[group]].0].timestamp {
                    latest[group] = n;
                }
                group_of.push(group);
            }
            for (n, &group) in group_of.iter().enumerate() {
                keep[n] = latest[group] == n;
            }
        }
        let duplicates = keep.iter().filter(|kept| !**kept).count();
        if duplicates > 0 {
            self.series_duplicates = duplicates;
            for _ in 0..duplicates {
                self.metric_not_sent(MetricOut::DuplicateTimestamp);
            }
            self.diagnostics.warn_throttled(
                "duplicate_timestamp",
                format_args!(
                    "datadog: dropped {duplicates} series point(s) that shared a series and a \
                     second with a later one, since Datadog keeps only the higher of the two -- \
                     add a `dedup_timestamps` component with `precision: 1s` before this \
                     output, or an `aggregate` for a live delta flow"
                ),
            );
        }
        for (&(index, record, p), kept) in flat.iter().zip(keep) {
            if kept {
                let c = carriers[index].as_ref().expect("read for every planned event");
                f(&batch.events[index], record, c, p);
            }
        }
    }

    /// How many series points the last series encode left off as `duplicate_timestamp`: the
    /// entries a request holds fewer than its records weigh.
    pub fn series_duplicates(&self) -> usize {
        self.series_duplicates
    }

    /// `/api/v2/series` as a protobuf `MetricPayload`, one series per point a record writes (one,
    /// or a histogram's or summary's expansion); `None` when nothing in the batch is a series.
    pub fn encode_series_v2_protobuf(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let mut series = Vec::new();
        self.each_series(batch, Route::V2Protobuf, |event, record, c, p| {
            series.push(metric_payload::MetricSeries {
                resources: c
                    .resource_list()
                    .into_iter()
                    .map(|(t, n)| metric_payload::Resource {
                        r#type: t.to_string(),
                        name: n.to_string(),
                    })
                    .collect(),
                metric: p.name(record),
                tags: p.tags(c),
                points: vec![metric_payload::MetricPoint {
                    value: p.value,
                    timestamp: nanos_to_seconds(event.timestamp),
                }],
                r#type: p.wire_type(c).proto(),
                unit: p.unit(record).map(|u| resolve(u).to_string()).unwrap_or_default(),
                source_type_name: c.source_type_name.clone().unwrap_or_default(),
                interval: c.interval,
                metadata: c.proto_metadata(),
            });
        });
        (!series.is_empty()).then(|| Bytes::from(MetricPayload { series }.encode_to_vec()))
    }

    /// `/api/v2/series` as JSON, the public API's form.
    pub fn encode_series_v2_json(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let mut series = Vec::new();
        self.each_series(batch, Route::V2Json, |event, record, c, p| {
            let mut s = JsonMap::new();
            s.insert("metric".into(), p.name(record).into());
            s.insert("type".into(), p.wire_type(c).proto().into());
            s.insert(
                "points".into(),
                json!([{ "timestamp": nanos_to_seconds(event.timestamp), "value": p.value }]),
            );
            let resources = c.resource_list();
            if !resources.is_empty() {
                let list = resources.iter().map(|(t, n)| json!({ "type": t, "name": n }));
                s.insert("resources".into(), JsonValue::Array(list.collect()));
            }
            s.insert("tags".into(), p.tags(c).into());
            if let Some(unit) = p.unit(record) {
                s.insert("unit".into(), resolve(unit).into());
            }
            if let Some(name) = &c.source_type_name {
                s.insert("source_type_name".into(), name.as_str().into());
            }
            if c.interval != 0 {
                s.insert("interval".into(), c.interval.into());
            }
            let o = &c.origin;
            if o.product != 0 || o.service != 0 || o.metric_type != 0 {
                let mut origin = JsonMap::new();
                for (key, code) in
                    [("metric_type", o.metric_type), ("product", o.product), ("service", o.service)]
                {
                    if code != 0 {
                        origin.insert(key.into(), code.into());
                    }
                }
                s.insert("metadata".into(), json!({ "origin": origin }));
            }
            series.push(JsonValue::Object(s));
        });
        json_body(series)
    }

    /// `/api/v1/series`, with the Agent's own field set: `tags`, `type`, and `interval` always;
    /// `host`, `device`, `source_type_name`, and `unit` when present.
    pub fn encode_series_v1(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let mut series = Vec::new();
        self.each_series(batch, Route::V1, |event, record, c, p| {
            let mut s = JsonMap::new();
            s.insert("metric".into(), p.name(record).into());
            s.insert("points".into(), json!([[nanos_to_seconds(event.timestamp), p.value]]));
            s.insert("tags".into(), p.tags(c).into());
            if let Some(host) = c.json_host() {
                s.insert("host".into(), host.into());
            }
            if let Some(device) = &c.device {
                s.insert("device".into(), device.as_str().into());
            }
            s.insert("type".into(), p.wire_type(c).v1().into());
            s.insert("interval".into(), c.interval.into());
            if let Some(name) = &c.source_type_name {
                s.insert("source_type_name".into(), name.as_str().into());
            }
            if let Some(unit) = p.unit(record) {
                s.insert("unit".into(), resolve(unit).into());
            }
            series.push(JsonValue::Object(s));
        });
        json_body(series)
    }

    /// `/api/v1/distribution_points`: every `Samples` record as one point. Every other kind is
    /// left to its own route, uncounted here.
    pub fn encode_distribution_points(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let mut series = Vec::new();
        for event in &batch.events {
            let mut carriers = None;
            for record in &event.metrics[first_metric(&batch.resource, event)..] {
                let MetricKind::Samples(samples) = &record.kind else { continue };
                if record.is_no_recorded_value() {
                    self.metric_not_sent(MetricOut::NoRecordedValue);
                    continue;
                }
                let finite: Vec<f64> =
                    samples.values.iter().copied().filter(|v| v.is_finite()).collect();
                if finite.len() != samples.values.len() {
                    self.metric_not_sent(MetricOut::NonFiniteValue);
                }
                if finite.is_empty() {
                    continue;
                }
                let weight = samples.weight();
                let values: Vec<f64> = if weight > 1 {
                    // The route has no sample rate: each value is repeated as the observations it
                    // stands for, what a sketch built from it would count.
                    self.out_degraded("sample_rate_expanded");
                    finite.iter().flat_map(|v| std::iter::repeat_n(*v, weight as usize)).collect()
                } else {
                    finite
                };
                let c = carriers.get_or_insert_with(|| {
                    self.carriers(&batch.resource, event, Route::Distribution)
                });
                let mut s = JsonMap::new();
                s.insert("metric".into(), resolve(record.name).into());
                s.insert("points".into(), json!([[nanos_to_seconds(event.timestamp), values]]));
                if let Some(host) = c.json_host() {
                    s.insert("host".into(), host.into());
                }
                s.insert("tags".into(), c.tags.clone().into());
                s.insert("type".into(), "distribution".into());
                series.push(JsonValue::Object(s));
            }
        }
        json_body(series)
    }
}

/// What makes two series points one Datadog series in one second: the metric name, the tag set
/// (order and repeats aside), and the host, device, and resources. The wire type, unit, interval,
/// and origin are the series' metadata, not its identity.
#[derive(PartialEq, Eq, Hash)]
struct SeriesKey<'a> {
    name: Symbol,
    suffix: &'static str,
    tags: Vec<&'a str>,
    host: Option<&'a str>,
    hostless: bool,
    device: Option<&'a str>,
    resources: &'a [(String, String)],
    second: i64,
}

impl<'a> SeriesKey<'a> {
    fn new(record: &MetricRecord, p: &'a SeriesPoint, c: &'a Carriers, second: i64) -> Self {
        let mut tags: Vec<&str> = c.tags.iter().map(String::as_str).collect();
        tags.extend(p.tag.iter().flatten().map(String::as_str));
        tags.sort_unstable();
        tags.dedup();
        SeriesKey {
            name: record.name,
            suffix: p.suffix,
            tags,
            host: c.host.as_deref(),
            hostless: c.hostless,
            device: c.device.as_deref(),
            resources: &c.resources,
            second,
        }
    }
}

/// One series a record writes: the record's own point, or one the translator derives from it (a
/// histogram's `.bucket`, `.count`, `.sum`, `.min`, `.max`, a summary's `.quantile`).
pub(super) struct SeriesPoint {
    /// Appended to the record's name after a `.`; empty for the record's own series.
    suffix: &'static str,
    /// Tags after the event's own.
    tag: Option<Vec<String>>,
    /// `None` takes the `_logit.datadog.type` carrier's type, else a gauge.
    wire_type: Option<WireType>,
    value: f64,
    /// Whether the series carries the record's unit; a count of observations doesn't.
    unit: bool,
}

impl SeriesPoint {
    fn name(&self, record: &MetricRecord) -> String {
        let name = resolve(record.name);
        if self.suffix.is_empty() {
            name.to_string()
        } else {
            format!("{name}.{}", self.suffix)
        }
    }

    fn tags(&self, c: &Carriers) -> Vec<String> {
        let mut tags = c.tags.clone();
        if let Some(extra) = &self.tag {
            tags.extend(extra.iter().cloned());
        }
        tags
    }

    fn wire_type(&self, c: &Carriers) -> WireType {
        self.wire_type.unwrap_or(c.wire_type.unwrap_or(WireType::Gauge))
    }

    fn unit(&self, record: &MetricRecord) -> Option<Symbol> {
        record.unit.filter(|_| self.unit)
    }

    /// The tags the series carries after the event's own.
    fn extra_tags(&self) -> usize {
        self.tag.as_ref().map_or(0, Vec::len)
    }
}

/// The routes one record takes under `histograms`: how many series it writes on the series route,
/// whether the series route reads it at all (it counts a skip there even when it writes nothing),
/// and whether it goes to the distribution-points or sketches route. The sink sizes its requests
/// and counts its deliveries and drops by it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordRoutes {
    /// The series [`DatadogEncoder`]'s series routes write for the record, from the same
    /// [`plan_series`] count.
    pub series: usize,
    /// The records' skips and drops the series route counts for it, from the same plan: what a
    /// stale copy, which never reaches the encoder, counts in their place.
    pub series_skips: usize,
    pub series_route: bool,
    pub distribution_points: bool,
    pub sketch: bool,
}

/// Whether the series route reads `record`: every kind but those with routes of their own.
fn on_series_route(record: &MetricRecord) -> bool {
    !matches!(record.kind, MetricKind::Samples(_) | MetricKind::Distribution(_))
}

/// [`RecordRoutes`] for `record`.
pub fn record_routes(record: &MetricRecord, histograms: HistogramOptions) -> RecordRoutes {
    let series_route = on_series_route(record);
    let (series, series_skips) = if series_route {
        let plan = plan_series(record, histograms);
        let skips = plan
            .notes
            .iter()
            .filter(|n| matches!(n, Note::NotSent(_) | Note::Cumulative(..)))
            .count();
        (plan.points.len(), skips)
    } else {
        (0, 0)
    };
    RecordRoutes {
        series,
        series_skips,
        series_route,
        distribution_points: matches!(record.kind, MetricKind::Samples(_)),
        sketch: on_sketch_route(record, histograms),
    }
}

/// Whether the sketches route writes `record`: a `Distribution`, a delta `Histogram` under
/// `histograms.mode: distributions`, and every delta `ExponentialHistogram`.
fn on_sketch_route(record: &MetricRecord, histograms: HistogramOptions) -> bool {
    match &record.kind {
        MetricKind::Distribution(_) => true,
        MetricKind::Histogram(h) => {
            h.temporality == Temporality::Delta && histograms.mode == HistogramMode::Distributions
        }
        MetricKind::ExponentialHistogram(e) => e.temporality == Temporality::Delta,
        _ => false,
    }
}

/// How many of `record`'s series carry more than [`MAX_METRIC_TAGS`] tags with `base_tags` (the
/// event's, [`metric_tag_count`]) ahead of their own, and the most tags such a series carries.
/// The encoder leaves each one off; the sink counts them.
pub fn series_over_tag_limit(
    record: &MetricRecord,
    histograms: HistogramOptions,
    base_tags: usize,
) -> (usize, usize) {
    if !on_series_route(record) {
        return (0, 0);
    }
    plan_series(record, histograms)
        .points
        .iter()
        .map(|p| base_tags + p.extra_tags())
        .filter(|tags| *tags > MAX_METRIC_TAGS)
        .fold((0, 0), |(n, most), tags| (n + 1, most.max(tags)))
}

/// What [`plan_series`] counts for a record, which the encoder turns into telemetry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Note {
    NotSent(MetricOut),
    /// A cumulative kind, named for the diagnostic.
    Cumulative(&'static str),
    Degraded(&'static str),
    DegradedReason(&'static str),
    BoolValue,
    NoSum,
    SummaryCumulative,
}

/// The series one record writes on a series route and what writing them counts. It's the one
/// count both the encoder's series and the sink's request weight ([`RecordRoutes::series`]) come
/// from, so the weight is what the encoder writes. A kind another route carries plans nothing.
struct SeriesPlan {
    points: Vec<SeriesPoint>,
    notes: Vec<Note>,
}

/// [`SeriesPlan`] for `record` under `histograms`; pure, so the sink can weigh a record by it.
fn plan_series(record: &MetricRecord, histograms: HistogramOptions) -> SeriesPlan {
    let mut out = Vec::new();
    let mut notes = Vec::new();
    let point = |wire_type, value| SeriesPoint {
        suffix: "",
        tag: None,
        wire_type: Some(wire_type),
        value,
        unit: true,
    };
    // A scalar's wire double: Datadog stores every point as a double, so an integer leaves as its
    // nearest `f64` and a `Bool` as `1` or `0`, counted.
    let scalar = |value: MetricValue, notes: &mut Vec<Note>| {
        if let MetricValue::Bool(_) = value {
            notes.push(Note::BoolValue);
        }
        value.as_f64()
    };
    let done = |points, notes| SeriesPlan { points, notes };
    // The kind a record leaves in a lesser form as, counted once if anything is written.
    let mut degraded = None;
    match &record.kind {
        MetricKind::Sum(sum) => {
            // The translator's `mapToDDFormat`: any delta sum is a `count`, a cumulative
            // non-monotonic one a `gauge`.
            if !sum.monotonic {
                degraded = Some("non_monotonic_sum");
            }
            let wire_type = match (sum.temporality, sum.monotonic) {
                (Temporality::Delta, _) => WireType::Count,
                (Temporality::Cumulative, false) => WireType::Gauge,
                (Temporality::Cumulative, true) => {
                    notes.push(Note::Cumulative("sum"));
                    return done(out, notes);
                }
            };
            out.push(point(wire_type, scalar(sum.value, &mut notes)));
        }
        MetricKind::Gauge(v) => {
            // `None`: the `_logit.datadog.type` carrier's type, else a gauge.
            out.push(SeriesPoint {
                wire_type: None,
                ..point(WireType::Gauge, scalar(*v, &mut notes))
            });
        }
        MetricKind::Set(hll) => {
            notes.push(Note::DegradedReason("set_estimate"));
            out.push(point(WireType::Gauge, hll.estimate() as f64));
        }
        // Their own routes: `encode_distribution_points` and `encode_sketches`.
        MetricKind::Samples(_) | MetricKind::Distribution(_) => return done(out, notes),
        MetricKind::GaugeDelta(_) => {
            notes.push(Note::NotSent(MetricOut::GaugeDelta));
            return done(out, notes);
        }
        MetricKind::SetMembers(_) => {
            notes.push(Note::NotSent(MetricOut::Kind(record.kind.name())));
            return done(out, notes);
        }
        MetricKind::Histogram(h) => {
            if h.temporality == Temporality::Cumulative {
                notes.push(Note::Cumulative("histogram"));
                return done(out, notes);
            }
            if record.is_no_recorded_value() {
                // The sketch route counts the record it would have sketched.
                if histograms.mode != HistogramMode::Distributions {
                    notes.push(Note::NotSent(MetricOut::NoRecordedValue));
                }
                return done(out, notes);
            }
            // Under `distributions`, the sketch route counts the record it sketches.
            if histograms.mode != HistogramMode::Distributions {
                degraded = Some("histogram");
            }
            let count = h.buckets.iter().fold(0u64, |a, b| a.saturating_add(b.1));
            aggregation_points(histograms, count as f64, h.sum, h.min, h.max, &mut out, &mut notes);
            if histograms.mode == HistogramMode::Counters {
                // `getLegacyBuckets`: every bucket, an empty one included, with no unit.
                for (lower, upper, n) in otlp_buckets(h) {
                    out.push(SeriesPoint {
                        suffix: "bucket",
                        tag: Some(vec![
                            format!("lower_bound:{}", format_float(lower)),
                            format!("upper_bound:{}", format_float(upper)),
                        ]),
                        unit: false,
                        ..point(WireType::Count, n as f64)
                    });
                }
            }
        }
        MetricKind::ExponentialHistogram(e) => {
            if e.temporality == Temporality::Cumulative {
                notes.push(Note::Cumulative("exponential histogram"));
                return done(out, notes);
            }
            // The sketch route counts a flagged or oversized record.
            if !record.is_no_recorded_value() && !exp_histogram_oversized(e) {
                let (count, sum) = (e.count as f64, e.sum);
                aggregation_points(histograms, count, sum, e.min, e.max, &mut out, &mut notes);
            }
        }
        MetricKind::Summary(summary) => {
            if record.is_no_recorded_value() {
                notes.push(Note::NotSent(MetricOut::NoRecordedValue));
                return done(out, notes);
            }
            // Its cell names the dropped count and sum, so the quantiles' form isn't counted.
            for (q, v) in &summary.quantiles {
                out.push(SeriesPoint {
                    suffix: "quantile",
                    tag: Some(vec![format!("quantile:{}", format_float(*q))]),
                    ..point(WireType::Gauge, *v)
                });
            }
            if summary.temporality == Temporality::Delta {
                // `MapSummaryMetrics`: `.count` with no unit, `.sum` with the record's.
                out.push(SeriesPoint {
                    suffix: "count",
                    unit: false,
                    ..point(WireType::Count, summary.count as f64)
                });
                out.push(SeriesPoint { suffix: "sum", ..point(WireType::Count, summary.sum) });
            } else {
                // A running total: its count and sum need differencing first.
                notes.push(Note::SummaryCumulative);
            }
        }
    }
    if matches!(record.kind, MetricKind::Sum(_) | MetricKind::Gauge(_) | MetricKind::Set(_))
        && record.is_no_recorded_value()
    {
        out.clear();
        notes.push(Note::NotSent(MetricOut::NoRecordedValue));
        return done(out, notes);
    }
    let before = out.len();
    out.retain(|p| p.value.is_finite());
    for _ in out.len()..before {
        notes.push(Note::NotSent(MetricOut::NonFiniteValue));
    }
    if let (Some(kind), false) = (degraded, out.is_empty()) {
        notes.push(Note::Degraded(kind));
    }
    done(out, notes)
}

/// `send_aggregation_metrics`'s series for a delta histogram: `.count` (no unit) and `.sum` as
/// counts, `.min` and `.max` as gauges where present. All of them need a finite sum, as the
/// translator's `histInfo.ok` does; an absent one counts as unusable too.
fn aggregation_points(
    histograms: HistogramOptions,
    count: f64,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    out: &mut Vec<SeriesPoint>,
    notes: &mut Vec<Note>,
) {
    if !histograms.send_aggregation_metrics {
        return;
    }
    let Some(sum) = sum.filter(|s| s.is_finite()) else {
        notes.push(Note::NoSum);
        return;
    };
    let point = |suffix, wire_type, value, unit| SeriesPoint {
        suffix,
        tag: None,
        wire_type: Some(wire_type),
        value,
        unit,
    };
    out.push(point("count", WireType::Count, count, false));
    out.push(point("sum", WireType::Count, sum, true));
    if let Some(min) = min {
        out.push(point("min", WireType::Gauge, min, true));
    }
    if let Some(max) = max {
        out.push(point("max", WireType::Gauge, max, true));
    }
}

/// The translator's `formatFloat`, its form for a bound or quantile in a tag: `inf`, `-inf`,
/// `nan`, `0`, else Go's shortest `'g'` form with `.0` after a whole number (`5.0`, `0.5`,
/// `1e+06.0`).
pub fn format_float(f: f64) -> String {
    if f == f64::INFINITY {
        return "inf".into();
    }
    if f == f64::NEG_INFINITY {
        return "-inf".into();
    }
    if f.is_nan() {
        return "nan".into();
    }
    if f == 0.0 {
        return "0".into();
    }
    let mut s = go_shortest_g(f);
    if f == f.floor() {
        s.push_str(".0");
    }
    s
}

/// `strconv.FormatFloat(f, 'g', -1, 64)` for a finite, non-zero `f`: the shortest decimal form
/// that round-trips, with an exponent when the decimal exponent is below -4 or at least 6.
fn go_shortest_g(f: f64) -> String {
    // Rust's `{:e}` writes the same shortest round-tripping figures, as `d.ddde<exp>`.
    let e = format!("{f:e}");
    let (mantissa, exp) = e.split_once('e').expect("`{:e}` always writes an exponent");
    let exp: i32 = exp.parse().expect("`{:e}`'s exponent is an integer");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(m) => ("-", m),
        None => ("", mantissa),
    };
    let figures: String = mantissa.chars().filter(|c| *c != '.').collect();
    if !(-4..6).contains(&exp) {
        let rest = &figures[1..];
        let point = if rest.is_empty() { String::new() } else { format!(".{rest}") };
        let exp_sign = if exp < 0 { '-' } else { '+' };
        return format!("{sign}{}{point}e{exp_sign}{:02}", &figures[..1], exp.abs());
    }
    if exp < 0 {
        let zeros = "0".repeat((-exp - 1) as usize);
        return format!("{sign}0.{zeros}{figures}");
    }
    let int_len = exp as usize + 1;
    if figures.len() <= int_len {
        format!("{sign}{figures}{}", "0".repeat(int_len - figures.len()))
    } else {
        format!("{sign}{}.{}", &figures[..int_len], &figures[int_len..])
    }
}

/// [`CONSUMED`] and [`CONSUMED_WITH_CHECK`], each plus the `host` attribute, for an event whose
/// `host` is its host ([`host_tag`]).
static CONSUMED_WITH_HOST_TAG: LazyLock<[Vec<Symbol>; 2]> = LazyLock::new(|| {
    let with_host = |keys: &[Symbol]| {
        let mut keys = keys.to_vec();
        keys.push(intern(ATTR_HOST_TAG));
        keys
    };
    [with_host(&*CONSUMED), with_host(&CONSUMED_WITH_CHECK)]
});

/// The attributes a metrics route never renders as a tag for `event`: the carriers it reads into
/// wire fields of their own, a service check's `_logit.statsd.service_check.*` too, and a `host` read as
/// the host.
fn tag_skip(resource: &Resource, event: &Event) -> &'static [Symbol] {
    let check = is_service_check(resource, event);
    match (host_tag(resource, event).is_some(), check) {
        (false, true) => &CONSUMED_WITH_CHECK,
        (false, false) => &*CONSUMED,
        (true, check) => &CONSUMED_WITH_HOST_TAG[usize::from(check)],
    }
}

/// The length of the `tags` list every metrics route (series, distribution points, sketches)
/// writes for each of `event`'s records, counting nothing. Host, device, and the other carriers
/// aren't in it.
pub fn metric_tag_count(resource: &Resource, event: &Event) -> usize {
    count_tags(merged(resource, event), tag_skip(resource, event))
}

/// The index of an event's first record a metrics route may send: `1` for a service check, whose
/// record 0 is [`DatadogEncoder::encode_service_checks`]'s alone (skipped silently, as fan-out),
/// else `0`. For APM stats ([`is_datadog_stats`]), which belong wholly to the stats routes, it is
/// `metrics.len()`: `each_series`, `encode_distribution_points`, and `encode_sketches` then skip
/// the event silently.
pub(super) fn first_metric(resource: &Resource, event: &Event) -> usize {
    if is_datadog_stats(resource, event) {
        return event.metrics.len();
    }
    usize::from(is_service_check(resource, event))
}

fn json_body(series: Vec<JsonValue>) -> Option<Bytes> {
    if series.is_empty() {
        return None;
    }
    let body = json!({ "series": series });
    Some(Bytes::from(serde_json::to_vec(&body).expect("a JSON value always serializes")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{Diagnostics, ExpHistogram, Histogram, HyperLogLog, Registry, Sum, Summary};

    const RECEIVED_AT: i64 = 1_699_999_999_500_000_000;
    const TS: i64 = 1_700_000_000_000_000_000;

    fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> f64 {
        let mut total = 0.0;
        for event in registry.drain(0) {
            if event.attributes.get(tag.0).and_then(|v| v.as_str()) != Some(tag.1) {
                continue;
            }
            for m in &event.metrics {
                if resolve(m.name) == metric {
                    if let MetricKind::Sum(s) = &m.kind {
                        total += s.value.as_f64();
                    }
                }
            }
        }
        total
    }

    fn decoder() -> (DatadogDecoder, Arc<Registry>) {
        let registry = Registry::new();
        let d = DatadogDecoder::new().with_telemetry(registry.telemetry_for(
            "datadog_in",
            "datadog_in",
            "listener",
        ));
        (d, registry)
    }

    fn encoder() -> (DatadogEncoder, Arc<Registry>) {
        let registry = Registry::new();
        let e = DatadogEncoder::new().with_telemetry(registry.telemetry_for(
            "datadog_out",
            "datadog_out",
            "sink",
        ));
        (e, registry)
    }

    fn one_event_batch(attrs: AttrMap, kind: MetricKind) -> EventBatch {
        batch(vec![Event::metric(TS, attrs, MetricRecord::new(intern("m"), kind))])
    }

    /// `metric_tag_count` is the length of the `tags` list the series encoder writes: carriers
    /// (host, device, a service check's own fields) and private keys excluded, resource tags and
    /// repeats included. A private key is never counted `tags.omitted`, even one no tag could
    /// hold.
    #[test]
    fn metric_tag_count_is_the_wire_tag_count() {
        let mut resource = Resource::default();
        resource.attributes.insert("region", Value::str("eu"));
        resource.attributes.insert("_logit.y", Value::str("r"));
        let mut plain = AttrMap::new();
        plain.insert("_logit.x", Value::str("e"));
        plain.insert("_logit.m", Value::Map(Box::new(AttrMap::new())));
        plain.insert(ATTR_HOST_NAME, Value::str("h"));
        plain.insert(ATTR_DEVICE, Value::str("d"));
        plain.insert("team", Value::Array(vec![Value::str("a"), Value::str("a")]));
        let mut check = plain.clone();
        check.insert(ATTR_SERVICE_CHECK_NAME, Value::str("app.ok"));
        check.insert(ATTR_SERVICE_CHECK_MESSAGE, Value::str("fine"));
        let mut check_event =
            Event::metric(TS, check, MetricRecord::new(intern("app.ok"), MetricKind::gauge(0.0)));
        check_event.metrics.push(MetricRecord::new(intern("m"), MetricKind::gauge(1.0)));
        let plain_event =
            Event::metric(TS, plain, MetricRecord::new(intern("m"), MetricKind::gauge(1.0)));
        for event in [plain_event, check_event] {
            let batch = EventBatch {
                resource: Arc::new(resource.clone()),
                scope: None,
                events: vec![event],
            };
            let (mut e, registry) = encoder();
            let body = e.encode_series_v2_protobuf(&batch).unwrap();
            let payload = MetricPayload::decode(body).unwrap();
            assert_eq!(payload.series[0].tags.len(), 3, "region:eu, team:a twice");
            assert!(payload.series[0].tags.iter().all(|t| !t.contains("_logit")));
            assert_eq!(metric_tag_count(&batch.resource, &batch.events[0]), 3);
            assert!(
                !registry
                    .drain(0)
                    .iter()
                    .flat_map(|ev| &ev.metrics)
                    .any(|m| resolve(m.name) == "logit.output.tags.omitted"),
                "a private key is never counted"
            );
        }
    }

    #[test]
    fn v1_agent_series_maps_every_field() {
        let body = br#"{"series":[{"metric":"my.metric","points":[[1700000000,1.5]],"tags":["env:prod","urgent"],"host":"myhost","device":"/dev/sda1","type":"rate","interval":20,"source_type_name":"my_check","unit":"byte"}]}"#;
        let (mut d, _) = decoder();
        let batch = d.decode_series_v1(body, RECEIVED_AT).unwrap();
        assert_eq!(batch.events.len(), 1);
        let e = &batch.events[0];
        assert_eq!(e.timestamp, TS);
        let m = &e.metrics[0];
        assert_eq!(resolve(m.name), "my.metric");
        assert_eq!(m.unit.map(resolve), Some("byte"));
        assert_eq!(m.kind, MetricKind::gauge(1.5));
        let a = &e.attributes;
        assert_eq!(a.get("env"), Some(&Value::str("prod")));
        assert_eq!(a.get("urgent"), Some(&Value::Bool(true)));
        assert_eq!(a.get(ATTR_HOST_NAME), Some(&Value::str("myhost")));
        assert_eq!(a.get(ATTR_DEVICE), Some(&Value::str("/dev/sda1")));
        assert_eq!(a.get(ATTR_TYPE), Some(&Value::str("rate")));
        assert_eq!(a.get(ATTR_INTERVAL), Some(&Value::I64(20)));
        assert_eq!(a.get(ATTR_SOURCE_TYPE_NAME), Some(&Value::str("my_check")));
    }

    /// A private tag on a series, an event, or a service check is left off what's delivered and
    /// counted, once per series however many points it has; a series that delivers no point
    /// counts nothing. `logit.x` and `_logitx` aren't private.
    #[test]
    fn a_private_tag_is_omitted_and_counted_and_lookalikes_are_not() {
        let (mut d, registry) = decoder();
        let series = br#"{"series":[
            {"metric":"m","points":[[1700000000,1],[1700000001,2]],
             "tags":["env:prod","_logit.datadog.interval:5","logit.x:1","_logitx"]},
            {"metric":"m","points":[[1700000000,null]],"tags":["_logit.y:1"]}]}"#;
        let batch = d.decode_series_v1(series, RECEIVED_AT).unwrap();
        assert_eq!(batch.events.len(), 2);
        for event in &batch.events {
            let a = &event.attributes;
            assert_eq!(a.get("env"), Some(&Value::str("prod")));
            assert_eq!(a.get("logit.x"), Some(&Value::str("1")));
            assert_eq!(a.get("_logitx"), Some(&Value::Bool(true)));
            assert_eq!(a.len(), 3, "no private key survives: {a:?}");
        }
        let event = br#"{"title":"t","text":"x","tags":["_logit.z:1","env:prod"]}"#;
        let batch = d.decode_events(event, RECEIVED_AT).unwrap();
        assert_eq!(batch.events[0].attributes.get("_logit.z"), None);
        let check = br#"[{"check":"c","status":0,"tags":["_logit.z:1","env:prod"]}]"#;
        let batch = d.decode_service_checks(check, RECEIVED_AT).unwrap();
        assert_eq!(batch.events[0].attributes.get("_logit.z"), None);
        assert_eq!(
            counted(&registry, "logit.input.tags.omitted", ("reason", "reserved_prefix")),
            3.0
        );
    }

    #[test]
    fn v1_types_map_to_kinds() {
        let (mut d, _) = decoder();
        for (t, kind, attr) in [
            (r#""count""#, MetricKind::counter(2.0), None),
            (r#""gauge""#, MetricKind::gauge(2.0), None),
            (r#""""#, MetricKind::gauge(2.0), Some("unspecified")),
            ("null", MetricKind::gauge(2.0), None),
        ] {
            let body =
                format!(r#"{{"series":[{{"metric":"m","points":[[1700000000,2]],"type":{t}}}]}}"#);
            let e = &d.decode_series_v1(body.as_bytes(), RECEIVED_AT).unwrap().events[0];
            assert_eq!(e.metrics[0].kind, kind, "{t}");
            assert_eq!(e.attributes.get(ATTR_TYPE).and_then(Value::as_str), attr, "{t}");
        }
    }

    #[test]
    fn v1_null_values_are_skipped_and_bad_series_dropped() {
        let body = br#"{"series":[
            {"metric":"a","points":[[1700000000,null],[1700000000.7,3]]},
            {"metric":"b","points":[[1700000000,1]],"type":"histogram"},
            {"points":[[1700000000,1]]},
            {"metric":"c","points":[[1700000000]]}
        ]}"#;
        let (mut d, registry) = decoder();
        let batch = d.decode_series_v1(body, RECEIVED_AT).unwrap();
        assert_eq!(batch.events.len(), 1);
        // A fractional v1 timestamp truncates to its second.
        assert_eq!(batch.events[0].timestamp, TS);
        let events = registry.drain(0);
        let total = |metric: &str, reason: &str| -> f64 {
            events
                .iter()
                .filter(|e| e.attributes.get("reason").and_then(Value::as_str) == Some(reason))
                .flat_map(|e| e.metrics.iter())
                .filter(|m| resolve(m.name) == metric)
                .map(|m| match m.kind {
                    MetricKind::Sum(Sum { value, .. }) => value.as_f64(),
                    _ => 0.0,
                })
                .sum()
        };
        let (skipped, dropped) = ("logit.input.metrics.skipped", "logit.input.metrics.dropped");
        assert_eq!(total(skipped, "null_value"), 1.0);
        assert_eq!(total(dropped, "bad_series"), 2.0);
        assert_eq!(total(dropped, "bad_point"), 1.0);
        assert_eq!(total(skipped, "bad_series") + total(skipped, "bad_point"), 0.0);
    }

    #[test]
    fn unparseable_bodies_are_malformed() {
        let (mut d, _) = decoder();
        assert!(matches!(d.decode_series_v1(b"nope", 0), Err(CodecError::Malformed(_))));
        assert!(matches!(
            d.decode_series_v2_json(br#"{"series":{}}"#, 0),
            Err(CodecError::Malformed(_))
        ));
        assert!(matches!(
            d.decode_series_v2_protobuf(b"\xff\xff\xff", 0),
            Err(CodecError::Malformed(_))
        ));
        assert!(matches!(d.decode_distribution_points(b"[]", 0), Err(CodecError::Malformed(_))));
    }

    /// The Agent's startup connectivity probe: `{}` on every JSON metrics route.
    #[test]
    fn an_object_without_series_is_an_empty_request() {
        let (mut d, _) = decoder();
        assert!(d.decode_series_v1(b"{}", 0).unwrap().events.is_empty());
        assert!(d.decode_series_v2_json(b"{}", 0).unwrap().events.is_empty());
        assert!(d.decode_distribution_points(b"{}", 0).unwrap().events.is_empty());
    }

    #[test]
    fn a_missing_timestamp_is_received_at_on_its_second_and_counted() {
        let body = br#"{"series":[{"metric":"system.load.1","points":[{"value":0.7}],"resources":[{"name":"dummyhost","type":"host"}]}]}"#;
        let (mut d, registry) = decoder();
        let e = &d.decode_series_v2_json(body, RECEIVED_AT).unwrap().events[0];
        assert_eq!(e.timestamp, 1_699_999_999_000_000_000);
        assert_eq!(e.attributes.get(ATTR_HOST_NAME), Some(&Value::str("dummyhost")));
        assert_eq!(e.attributes.get(ATTR_TYPE), Some(&Value::str("unspecified")));
        assert_eq!(
            counted(&registry, "logit.input.metrics.degraded", ("reason", "no_timestamp")),
            1.0
        );
    }

    #[test]
    fn v2_json_resources_origin_and_types() {
        let body = br#"{"series":[{"metric":"m","type":1,"interval":10,"unit":"request",
            "points":[{"timestamp":1700000000,"value":4}],
            "resources":[{"type":"db","name":"users"},{"type":"host","name":"h"},{"type":"device","name":"sda"}],
            "metadata":{"origin":{"product":10,"service":7,"metric_type":2}},
            "source_type_name":"chk","tags":["team:a","team:b"]}]}"#;
        let (mut d, _) = decoder();
        let e = &d.decode_series_v2_json(body, RECEIVED_AT).unwrap().events[0];
        assert_eq!(e.metrics[0].kind, MetricKind::counter(4.0));
        let a = &e.attributes;
        assert_eq!(a.get(ATTR_HOST_NAME), Some(&Value::str("h")));
        assert_eq!(a.get(ATTR_DEVICE), Some(&Value::str("sda")));
        let mut db = AttrMap::new();
        db.insert("type", Value::str("db"));
        db.insert("name", Value::str("users"));
        assert_eq!(a.get(ATTR_RESOURCES), Some(&Value::Array(vec![Value::Map(Box::new(db))])));
        assert_eq!(a.get(ATTR_ORIGIN_PRODUCT), Some(&Value::U64(10)));
        assert_eq!(a.get(ATTR_ORIGIN_SERVICE), Some(&Value::U64(7)));
        assert_eq!(a.get(ATTR_ORIGIN_METRIC_TYPE), Some(&Value::U64(2)));
        assert_eq!(a.get(ATTR_ORIGIN_CATEGORY), None);
        assert_eq!(a.get(ATTR_INTERVAL), Some(&Value::I64(10)));
        assert_eq!(a.get("team"), Some(&Value::Array(vec![Value::str("a"), Value::str("b")])));
    }

    #[test]
    fn v2_protobuf_decodes_origin_and_skips_unknown_types() {
        let payload = MetricPayload {
            series: vec![
                metric_payload::MetricSeries {
                    metric: "m".into(),
                    r#type: 3,
                    points: vec![metric_payload::MetricPoint {
                        value: 1.0,
                        timestamp: 1_700_000_000,
                    }],
                    metadata: Some(Metadata {
                        origin: Some(Origin {
                            origin_product: 1,
                            origin_category: 2,
                            origin_service: 3,
                        }),
                    }),
                    ..Default::default()
                },
                metric_payload::MetricSeries {
                    metric: "bad".into(),
                    r#type: 9,
                    ..Default::default()
                },
            ],
        };
        let (mut d, registry) = decoder();
        let batch = d.decode_series_v2_protobuf(&payload.encode_to_vec(), RECEIVED_AT).unwrap();
        assert_eq!(batch.events.len(), 1);
        let a = &batch.events[0].attributes;
        assert_eq!(a.get(ATTR_ORIGIN_CATEGORY), Some(&Value::U64(2)));
        assert_eq!(batch.events[0].metrics[0].kind, MetricKind::gauge(1.0));
        assert_eq!(
            counted(&registry, "logit.input.metrics.dropped", ("reason", "bad_series")),
            1.0
        );
    }

    #[test]
    fn distribution_points_decode_to_samples() {
        let body = br#"{"series":[{"metric":"lat","points":[[1700000000,[1.0,2.5]],[1700000000,[]]],"host":"h","tags":["env:a"],"type":"distribution"}]}"#;
        let (mut d, registry) = decoder();
        let batch = d.decode_distribution_points(body, RECEIVED_AT).unwrap();
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].metrics[0].kind, MetricKind::Samples(Samples::new([1.0, 2.5])));
        assert_eq!(batch.events[0].attributes.get(ATTR_HOST_NAME), Some(&Value::str("h")));
        assert_eq!(
            counted(&registry, "logit.input.metrics.skipped", ("reason", "empty_distribution")),
            1.0
        );
    }

    /// Of two points of one series in one second, the later is sent and the earlier counted, for
    /// a count as for a gauge and whether or not the values differ; a point of another series
    /// (another tag set) or another second is no duplicate.
    #[test]
    fn a_series_point_sharing_its_second_with_a_later_one_is_dropped_and_counted() {
        let (mut e, registry) = encoder();
        let at = |offset_ms: i64, name: &str, kind: MetricKind, env: &str| {
            let mut attrs = AttrMap::new();
            attrs.insert("env", Value::str(env));
            Event::metric(TS + offset_ms * 1_000_000, attrs, MetricRecord::new(intern(name), kind))
        };
        let events = vec![
            at(0, "c", MetricKind::counter(2.0), "prod"),
            at(400, "c", MetricKind::counter(2.0), "prod"),
            at(0, "g", MetricKind::gauge(5.0), "prod"),
            at(0, "g", MetricKind::gauge(5.0), "prod"),
            at(0, "g", MetricKind::gauge(7.0), "dev"),
            at(900, "h", MetricKind::gauge(9.0), "prod"),
            at(100, "h", MetricKind::gauge(1.0), "prod"),
            at(1_000, "h", MetricKind::gauge(4.0), "prod"),
        ];
        let body = e.encode_series_v1(&batch(events)).unwrap();
        let json: JsonValue = serde_json::from_slice(&body).unwrap();
        let sent: Vec<(String, f64)> = json["series"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                (s["metric"].as_str().unwrap().to_string(), s["points"][0][1].as_f64().unwrap())
            })
            .collect();
        let want = [("c", 2.0), ("g", 5.0), ("g", 7.0), ("h", 9.0), ("h", 4.0)];
        let want: Vec<(String, f64)> = want.iter().map(|(n, v)| (n.to_string(), *v)).collect();
        assert_eq!(sent, want, "the later point of each pair is sent, in batch order");
        assert_eq!(e.series_duplicates(), 3);
        assert_eq!(
            counted(&registry, "logit.output.metrics.dropped", ("reason", "duplicate_timestamp")),
            3.0
        );
    }

    #[test]
    fn encode_maps_kinds_and_counts_the_rest() {
        let (mut e, registry) = encoder();
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_HOST_NAME, Value::str("h"));
        attrs.insert("env", Value::str("prod"));
        let mut events = Vec::new();
        let mut hll = HyperLogLog::new();
        hll.insert(b"a");
        hll.insert(b"b");
        // A second apart, so no two are one series in one second.
        for (second, kind) in [
            MetricKind::counter(3.0),
            MetricKind::Set(hll),
            MetricKind::Sum(Sum {
                value: MetricValue::F64(1.0),
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
            MetricKind::GaugeDelta(MetricValue::F64(1.0)),
            MetricKind::Samples(Samples::new([1.0])),
            MetricKind::gauge(f64::NAN),
        ]
        .into_iter()
        .enumerate()
        {
            let ts = TS + second as i64 * 1_000_000_000;
            events.push(Event::metric(ts, attrs.clone(), MetricRecord::new(intern("m"), kind)));
        }
        let body = e.encode_series_v1(&batch(events)).unwrap();
        let json: JsonValue = serde_json::from_slice(&body).unwrap();
        let series = json["series"].as_array().unwrap();
        assert_eq!(series.len(), 2);
        assert_eq!(series[0]["type"], "count");
        assert_eq!(series[0]["host"], "h");
        assert_eq!(series[0]["tags"], json!(["env:prod"]));
        assert_eq!(series[0]["interval"], 0);
        assert_eq!(series[1]["type"], "gauge");
        assert_eq!(series[1]["points"][0][1], 2.0);
        let drained = registry.drain(0);
        let has = |metric: &str, k: &str, v: &str| {
            drained.iter().any(|ev| {
                ev.attributes.get(k).and_then(Value::as_str) == Some(v)
                    && ev.metrics.iter().any(|m| resolve(m.name) == metric)
            })
        };
        assert!(has("logit.output.metrics.degraded", "reason", "set_estimate"));
        assert!(has("logit.output.metrics.dropped", "reason", "unresolved_cumulative"));
        assert!(has("logit.output.metrics.dropped", "reason", "unresolved_gauge_delta"));
        assert!(!has("logit.output.metrics.skipped", "metric_kind", "gauge_delta"));
        assert!(has("logit.output.metrics.dropped", "reason", "non_finite_value"));
        assert!(!has("logit.output.metrics.skipped", "reason", "non_finite_value"));
        assert!(!has("logit.output.metrics.skipped", "metric_kind", "samples"));
    }

    /// Every series route writes the default host for a hostless event, and an event's own
    /// `host.name` wins over it.
    #[test]
    fn every_series_route_fills_only_a_missing_host() {
        let mut e = DatadogEncoder::new().with_default_host(Some(Arc::from("relay")));
        let mut own = AttrMap::new();
        own.insert(ATTR_HOST_NAME, Value::str("h"));
        for (attrs, want) in [(AttrMap::new(), "relay"), (own, "h")] {
            let gauge = one_event_batch(attrs.clone(), MetricKind::gauge(1.0));
            let v1: JsonValue =
                serde_json::from_slice(&e.encode_series_v1(&gauge).unwrap()).unwrap();
            assert_eq!(v1["series"][0]["host"], want);
            let v2: JsonValue =
                serde_json::from_slice(&e.encode_series_v2_json(&gauge).unwrap()).unwrap();
            assert_eq!(v2["series"][0]["resources"], json!([{ "type": "host", "name": want }]));
            let proto =
                MetricPayload::decode(e.encode_series_v2_protobuf(&gauge).unwrap()).unwrap();
            assert_eq!(
                proto.series[0].resources,
                vec![metric_payload::Resource { r#type: "host".into(), name: want.into() }]
            );
            let samples = one_event_batch(attrs, MetricKind::Samples(Samples::new([1.0])));
            let points: JsonValue =
                serde_json::from_slice(&e.encode_distribution_points(&samples).unwrap()).unwrap();
            assert_eq!(points["series"][0]["host"], want);
        }
    }

    /// A DogStatsD `#host:` tag (`statsd_in`'s `host` attribute) with no `host.name`: used as the
    /// host and not rendered as a tag, as the Agent's `extractTagsMetadata` does; an empty
    /// `host.name` counts as none. Empty, `host` is a hostless sample: no fill and no tag, a
    /// `host` of `""`, and on v2 the Agent's `host` resource with no name. With a non-empty
    /// `host.name`, `host` stays a tag.
    #[test]
    fn a_host_tag_is_the_host_only_without_host_name() {
        let mut e = DatadogEncoder::new().with_default_host(Some(Arc::from("relay")));
        let attrs = |pairs: &[(&str, &str)]| {
            let mut attrs = AttrMap::new();
            for (k, v) in pairs {
                attrs.insert(k, Value::str(*v));
            }
            attrs
        };
        for (pairs, host, v2_resources, tags) in [
            (
                &[("host", "web1"), ("env", "prod")][..],
                "web1",
                json!([{ "type": "host", "name": "web1" }]),
                json!(["env:prod"]),
            ),
            (
                &[("host", ""), ("env", "prod")][..],
                "",
                json!([{ "type": "host", "name": "" }]),
                json!(["env:prod"]),
            ),
            (
                &[("host", "web1"), (ATTR_HOST_NAME, "")][..],
                "web1",
                json!([{ "type": "host", "name": "web1" }]),
                json!([]),
            ),
            (
                &[("host", "web1"), (ATTR_HOST_NAME, "h")][..],
                "h",
                json!([{ "type": "host", "name": "h" }]),
                json!(["host:web1"]),
            ),
        ] {
            let b = one_event_batch(attrs(pairs), MetricKind::gauge(1.0));
            let v1: JsonValue = serde_json::from_slice(&e.encode_series_v1(&b).unwrap()).unwrap();
            assert_eq!(v1["series"][0]["host"], host, "{pairs:?}");
            assert_eq!(v1["series"][0]["tags"], tags, "{pairs:?}");
            assert_eq!(
                metric_tag_count(&b.resource, &b.events[0]),
                tags.as_array().unwrap().len(),
                "{pairs:?}"
            );
            let v2: JsonValue =
                serde_json::from_slice(&e.encode_series_v2_json(&b).unwrap()).unwrap();
            assert_eq!(v2["series"][0]["resources"], v2_resources, "{pairs:?}");
            let samples = one_event_batch(attrs(pairs), MetricKind::Samples(Samples::new([1.0])));
            let points: JsonValue =
                serde_json::from_slice(&e.encode_distribution_points(&samples).unwrap()).unwrap();
            assert_eq!(points["series"][0]["host"], host, "{pairs:?}");
        }
    }

    /// `_logit.datadog.hostless` keeps every series route unfilled: `""` on v1 and distribution
    /// points, the Agent's `host` resource with no name on v2, and never a tag. A `host.name`
    /// still wins over it.
    #[test]
    fn a_hostless_carrier_is_never_filled() {
        let mut e = DatadogEncoder::new().with_default_host(Some(Arc::from("relay")));
        let mut hostless = AttrMap::new();
        hostless.insert(ATTR_HOSTLESS, Value::Bool(true));
        let mut named = hostless.clone();
        named.insert(ATTR_HOST_NAME, Value::str("h"));
        for (attrs, want) in [(hostless, ""), (named, "h")] {
            let gauge = one_event_batch(attrs.clone(), MetricKind::gauge(1.0));
            let v1: JsonValue =
                serde_json::from_slice(&e.encode_series_v1(&gauge).unwrap()).unwrap();
            assert_eq!(v1["series"][0]["host"], want);
            assert_eq!(v1["series"][0]["tags"], json!([]));
            let v2: JsonValue =
                serde_json::from_slice(&e.encode_series_v2_json(&gauge).unwrap()).unwrap();
            assert_eq!(v2["series"][0]["resources"], json!([{ "type": "host", "name": want }]));
            let proto =
                MetricPayload::decode(e.encode_series_v2_protobuf(&gauge).unwrap()).unwrap();
            assert_eq!(
                proto.series[0].resources,
                vec![metric_payload::Resource { r#type: "host".into(), name: want.into() }]
            );
            let samples = one_event_batch(attrs, MetricKind::Samples(Samples::new([1.0])));
            let points: JsonValue =
                serde_json::from_slice(&e.encode_distribution_points(&samples).unwrap()).unwrap();
            assert_eq!(points["series"][0]["host"], want);
        }
    }

    /// A v1 or distribution-point `"host":""` decodes to `_logit.datadog.hostless`; an absent
    /// `host` decodes to nothing.
    #[test]
    fn an_empty_wire_host_decodes_as_hostless() {
        let mut d = DatadogDecoder::new();
        let v1 = |host: &str| {
            format!(r#"{{"series":[{{"metric":"m","points":[[1700000000,1]]{host}}}]}}"#)
        };
        let dist = |host: &str| {
            format!(r#"{{"series":[{{"metric":"m","points":[[1700000000,[1]]]{host}}}]}}"#)
        };
        for (host, hostless) in [(r#","host":"""#, Some(&Value::Bool(true))), ("", None)] {
            let b = d.decode_series_v1(v1(host).as_bytes(), 0).unwrap();
            assert_eq!(b.events[0].attributes.get(ATTR_HOSTLESS), hostless, "{host}");
            assert_eq!(b.events[0].attributes.get(ATTR_HOST_NAME), None, "{host}");
            let b = d.decode_distribution_points(dist(host).as_bytes(), 0).unwrap();
            assert_eq!(b.events[0].attributes.get(ATTR_HOSTLESS), hostless, "{host}");
        }
    }

    /// With no default host, a v1 series or distribution point with no host omits `host`, so
    /// it never decodes as hostless; a hostless one keeps the Agent's `""`.
    #[test]
    fn with_no_default_host_only_a_hostless_series_sends_an_empty_host() {
        let (mut e, _) = encoder();
        let mut hostless = AttrMap::new();
        hostless.insert(ATTR_HOSTLESS, Value::Bool(true));
        for (attrs, want) in [(AttrMap::new(), JsonValue::Null), (hostless, json!(""))] {
            let b = one_event_batch(attrs.clone(), MetricKind::gauge(1.0));
            let v1: JsonValue = serde_json::from_slice(&e.encode_series_v1(&b).unwrap()).unwrap();
            assert_eq!(v1["series"][0]["host"], want);
            let b = one_event_batch(attrs, MetricKind::Samples(Samples::new([1.0])));
            let points: JsonValue =
                serde_json::from_slice(&e.encode_distribution_points(&b).unwrap()).unwrap();
            assert_eq!(points["series"][0]["host"], want);
        }
    }

    /// A `host.name` that isn't a string is the event's own host: omitted and counted, never
    /// replaced by the default host.
    #[test]
    fn an_unrepresentable_host_is_not_replaced_by_the_default_host() {
        let registry = Registry::new();
        let mut e = DatadogEncoder::new()
            .with_telemetry(registry.telemetry_for("datadog_out", "datadog_out", "sink"))
            .with_default_host(Some(Arc::from("relay")));
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_HOST_NAME, Value::I64(7));
        let b = one_event_batch(attrs, MetricKind::gauge(1.0));
        let v1: JsonValue = serde_json::from_slice(&e.encode_series_v1(&b).unwrap()).unwrap();
        assert_eq!(v1["series"][0]["host"], JsonValue::Null);
        assert_eq!(
            counted(&registry, "logit.output.tags.omitted", ("reason", "unrepresentable")),
            1.0
        );
    }

    #[test]
    fn nothing_to_send_is_none() {
        let (mut e, _) = encoder();
        let b = one_event_batch(AttrMap::new(), MetricKind::Samples(Samples::new([1.0])));
        assert!(e.encode_series_v1(&b).is_none());
        assert!(e.encode_series_v2_protobuf(&b).is_none());
        let b = one_event_batch(AttrMap::new(), MetricKind::gauge(1.0));
        assert!(e.encode_distribution_points(&b).is_none());
    }

    #[test]
    fn v2_protobuf_encode_writes_resources_type_and_origin() {
        let (mut e, _) = encoder();
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_HOST_NAME, Value::str("h"));
        attrs.insert(ATTR_TYPE, Value::str("rate"));
        attrs.insert(ATTR_INTERVAL, Value::I64(10));
        attrs.insert(ATTR_ORIGIN_SERVICE, Value::U64(5));
        let mut record = MetricRecord::new(intern("r"), MetricKind::gauge(0.5));
        record.unit = Some(intern("second"));
        let b = batch(vec![Event::metric(TS, attrs, record)]);
        let payload = MetricPayload::decode(e.encode_series_v2_protobuf(&b).unwrap()).unwrap();
        let s = &payload.series[0];
        assert_eq!(s.r#type, 2);
        assert_eq!(
            s.resources,
            vec![metric_payload::Resource { r#type: "host".into(), name: "h".into() }]
        );
        assert_eq!(s.interval, 10);
        assert_eq!(s.unit, "second");
        assert!(s.tags.is_empty());
        assert_eq!(s.points[0].timestamp, 1_700_000_000);
        assert_eq!(s.metadata.unwrap().origin.unwrap().origin_service, 5);
    }

    #[test]
    fn distribution_points_encode_expands_a_sample_rate() {
        let (mut e, registry) = encoder();
        let samples = Samples { values: [1.0, 2.0].into_iter().collect(), sample_rate: 0.5 };
        let b = one_event_batch(AttrMap::new(), MetricKind::Samples(samples));
        let json: JsonValue =
            serde_json::from_slice(&e.encode_distribution_points(&b).unwrap()).unwrap();
        assert_eq!(json["series"][0]["points"][0][1], json!([1.0, 1.0, 2.0, 2.0]));
        assert_eq!(json["series"][0]["type"], "distribution");
        assert_eq!(
            counted(&registry, "logit.output.metrics.degraded", ("reason", "sample_rate_expanded")),
            1.0
        );
    }

    #[test]
    fn carriers_with_no_wire_form_or_wrong_types_are_counted() {
        let (mut e, registry) = encoder();
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_ORIGIN_PRODUCT, Value::U64(1));
        attrs.insert(ATTR_INTERVAL, Value::str("ten"));
        let b = one_event_batch(attrs, MetricKind::gauge(1.0));
        e.encode_series_v1(&b).unwrap();
        let drained = registry.drain(0);
        let reason = |r: &str| {
            drained.iter().any(|ev| ev.attributes.get("reason").and_then(Value::as_str) == Some(r))
        };
        assert!(reason("no_wire_form"));
        assert!(reason("unrepresentable"));
    }

    /// An encoder counting into `registry`, its diagnostics too, under `histograms`.
    fn otlp_encoder(histograms: HistogramOptions) -> (DatadogEncoder, Arc<Registry>) {
        let registry = Registry::new();
        let t = registry.telemetry_for("datadog_out", "datadog_out", "sink");
        let e = DatadogEncoder::new()
            .with_telemetry(t.clone())
            .with_diagnostics(Diagnostics::new("datadog_out").with_telemetry(t))
            .with_histograms(histograms);
        (e, registry)
    }

    /// Every counter `registry` holds, as `(metric, tag value, total)`.
    fn counters(registry: &Registry) -> Vec<(String, String, f64)> {
        let mut out = Vec::new();
        for event in registry.drain(0) {
            let tag = ["reason", "metric_kind", "key"]
                .iter()
                .find_map(|k| event.attributes.get(k).and_then(Value::as_str))
                .unwrap_or_default()
                .to_string();
            for m in &event.metrics {
                if let MetricKind::Sum(s) = &m.kind {
                    out.push((resolve(m.name).to_string(), tag.clone(), s.value.as_f64()));
                }
            }
        }
        out
    }

    fn total(counters: &[(String, String, f64)], metric: &str, tag: &str) -> f64 {
        counters.iter().filter(|(m, t, _)| m == metric && t == tag).map(|c| c.2).sum()
    }

    fn sum(value: f64, temporality: Temporality, monotonic: bool) -> MetricKind {
        MetricKind::Sum(Sum { value: MetricValue::F64(value), temporality, monotonic })
    }

    fn delta_histogram(sum: Option<f64>) -> Histogram {
        Histogram {
            buckets: vec![(1.0, 2), (10.0, 5), (f64::INFINITY, 1)],
            temporality: Temporality::Delta,
            sum,
            min: Some(0.5),
            max: Some(12.0),
        }
    }

    fn v2_series(e: &mut DatadogEncoder, batch: &EventBatch) -> Vec<JsonValue> {
        match e.encode_series_v2_json(batch) {
            Some(body) => {
                let json: JsonValue = serde_json::from_slice(&body).unwrap();
                json["series"].as_array().unwrap().clone()
            }
            None => Vec::new(),
        }
    }

    /// As the translator's `mapToDDFormat`: any delta sum is a `count` (1), a cumulative
    /// non-monotonic one a `gauge` (3), and a cumulative monotonic one is dropped with the
    /// diagnostic naming the stage it needs.
    #[test]
    fn sums_map_as_datadogs_exporter_maps_them() {
        let (mut e, registry) = otlp_encoder(HistogramOptions::default());
        let events = [
            sum(2.0, Temporality::Delta, false),
            sum(3.0, Temporality::Cumulative, false),
            sum(4.0, Temporality::Cumulative, true),
        ]
        .map(|k| Event::metric(TS, AttrMap::new(), MetricRecord::new(intern("m"), k)));
        // A second apart, so the delta and the cumulative sum aren't one series in one second.
        let events: Vec<Event> = events
            .into_iter()
            .enumerate()
            .map(|(second, mut e)| {
                e.timestamp += second as i64 * 1_000_000_000;
                e
            })
            .collect();
        let series = v2_series(&mut e, &batch(events));
        let wire: Vec<(i64, f64)> = series
            .iter()
            .map(|s| (s["type"].as_i64().unwrap(), s["points"][0]["value"].as_f64().unwrap()))
            .collect();
        assert_eq!(wire, [(1, 2.0), (3, 3.0)]);
        let c = counters(&registry);
        assert_eq!(total(&c, "logit.output.metrics.dropped", "unresolved_cumulative"), 1.0);
        assert_eq!(total(&c, "logit.output.metrics.skipped", "cumulative_sum"), 0.0);
        assert_eq!(total(&c, "logit.output.metrics.skipped", "non_monotonic_delta_sum"), 0.0);
        assert_eq!(total(&c, "logit.component.diagnostics", "unresolved_cumulative"), 1.0);
    }

    /// A cumulative histogram or exponential histogram goes to no route, dropped once with the
    /// diagnostic, whatever the histogram mode.
    #[test]
    fn a_cumulative_histogram_kind_is_dropped_once_with_the_diagnostic() {
        for mode in [HistogramMode::Distributions, HistogramMode::Counters] {
            let (mut e, registry) =
                otlp_encoder(HistogramOptions { mode, send_aggregation_metrics: true });
            let h =
                Histogram { temporality: Temporality::Cumulative, ..delta_histogram(Some(9.0)) };
            let x = ExpHistogram {
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: (0, vec![1, 2]),
                negative: (0, vec![]),
                temporality: Temporality::Cumulative,
                count: 3,
                sum: Some(5.0),
                min: None,
                max: None,
            };
            let b = batch(vec![
                Event::metric(
                    TS,
                    AttrMap::new(),
                    MetricRecord::new(intern("h"), MetricKind::Histogram(h)),
                ),
                Event::metric(
                    TS,
                    AttrMap::new(),
                    MetricRecord::new(intern("x"), MetricKind::ExponentialHistogram(x)),
                ),
            ]);
            assert!(v2_series(&mut e, &b).is_empty());
            assert!(e.encode_sketches(&b).is_none());
            let c = counters(&registry);
            assert_eq!(
                total(&c, "logit.output.metrics.dropped", "unresolved_cumulative"),
                2.0,
                "{mode:?}"
            );
            assert_eq!(total(&c, "logit.output.metrics.skipped", "histogram"), 0.0, "{mode:?}");
            // One per record: the counter counts every diagnostic, a throttled one included.
            assert_eq!(total(&c, "logit.component.diagnostics", "unresolved_cumulative"), 2.0);
        }
    }

    /// A cumulative summary's quantiles go out as `.quantile` gauges, a non-finite one dropped
    /// and counted, and its `.count` and `.sum` are left off, counted once per summary.
    #[test]
    fn a_cumulative_summary_writes_its_quantiles_and_counts_its_count_and_sum_dropped() {
        let (mut e, registry) = otlp_encoder(HistogramOptions::default());
        let s = Summary {
            quantiles: vec![(0.5, 2.0), (0.99, f64::NAN), (1.0, 9.0)],
            count: 4,
            sum: 15.0,
            temporality: Temporality::Cumulative,
        };
        let b = one_event_batch(AttrMap::new(), MetricKind::Summary(s));
        let series = v2_series(&mut e, &b);
        let got: Vec<(&str, &JsonValue)> =
            series.iter().map(|s| (s["metric"].as_str().unwrap(), &s["tags"])).collect();
        assert_eq!(
            got,
            [("m.quantile", &json!(["quantile:0.5"])), ("m.quantile", &json!(["quantile:1.0"]))]
        );
        let c = counters(&registry);
        assert_eq!(total(&c, "logit.output.fields.dropped", "unresolved_cumulative"), 1.0);
        assert_eq!(total(&c, "logit.output.fields.omitted", "unresolved_cumulative"), 0.0);
        assert_eq!(total(&c, "logit.output.metrics.dropped", "non_finite_value"), 1.0);
        assert_eq!(total(&c, "logit.component.diagnostics", "unresolved_cumulative"), 1.0);
    }

    /// A delta summary adds `.count` (no unit) and `.sum` (the record's unit) as `count` points
    /// after its quantiles, as the translator's `MapSummaryMetrics` writes them, and counts
    /// nothing.
    #[test]
    fn a_delta_summary_writes_its_count_and_sum_as_counts() {
        let (mut e, registry) = otlp_encoder(HistogramOptions::default());
        let s = Summary {
            quantiles: vec![(0.5, 2.0)],
            count: 4,
            sum: 15.0,
            temporality: Temporality::Delta,
        };
        let rec = MetricRecord {
            unit: Some(intern("ms")),
            ..MetricRecord::new(intern("m"), MetricKind::Summary(s))
        };
        let b = batch(vec![Event::metric(TS, AttrMap::new(), rec)]);
        let series = v2_series(&mut e, &b);
        let got: Vec<(&str, i64, f64, Option<&str>)> = series
            .iter()
            .map(|s| {
                (
                    s["metric"].as_str().unwrap(),
                    s["type"].as_i64().unwrap(),
                    s["points"][0]["value"].as_f64().unwrap(),
                    s["unit"].as_str(),
                )
            })
            .collect();
        assert_eq!(
            got,
            [
                ("m.quantile", 3, 2.0, Some("ms")),
                ("m.count", 1, 4.0, None),
                ("m.sum", 1, 15.0, Some("ms")),
            ]
        );
        let c = counters(&registry);
        assert_eq!(total(&c, "logit.output.fields.dropped", "unresolved_cumulative"), 0.0);
        assert_eq!(total(&c, "logit.component.diagnostics", "unresolved_cumulative"), 0.0);
    }

    /// A flagged delta histogram is counted `no_recorded_value` once, on whichever route it
    /// would have used, and sends nothing on either.
    #[test]
    fn a_flagged_histogram_counts_once_across_routes() {
        for mode in [HistogramMode::Distributions, HistogramMode::Counters] {
            let (mut e, registry) =
                otlp_encoder(HistogramOptions { mode, send_aggregation_metrics: true });
            let mut record =
                MetricRecord::new(intern("h"), MetricKind::Histogram(delta_histogram(Some(9.0))));
            record.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
            let b = batch(vec![Event::metric(TS, AttrMap::new(), record)]);
            assert!(v2_series(&mut e, &b).is_empty());
            assert!(e.encode_sketches(&b).is_none());
            let c = counters(&registry);
            assert_eq!(
                total(&c, "logit.output.metrics.skipped", "no_recorded_value"),
                1.0,
                "{mode:?}"
            );
        }
    }

    /// With the aggregation metrics on, a histogram whose sum is missing or non-finite writes
    /// none of them, counted, and still sends its sketch.
    #[test]
    fn aggregations_need_a_finite_sum() {
        let (mut e, registry) = otlp_encoder(HistogramOptions {
            mode: HistogramMode::Distributions,
            send_aggregation_metrics: true,
        });
        let events = [None, Some(f64::INFINITY), Some(7.5)].map(|s| {
            Event::metric(
                TS,
                AttrMap::new(),
                MetricRecord::new(intern("h"), MetricKind::Histogram(delta_histogram(s))),
            )
        });
        let b = batch(events.to_vec());
        let names: Vec<String> = v2_series(&mut e, &b)
            .iter()
            .map(|s| s["metric"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(names, ["h.count", "h.sum", "h.min", "h.max"]);
        assert!(e.encode_sketches(&b).is_some());
        let c = counters(&registry);
        assert_eq!(total(&c, "logit.output.fields.omitted", "no_sum"), 2.0);
    }

    /// A record's series weight, which the sink sizes its requests and counts its deliveries by,
    /// is what the series routes write for it: each row's expected count, checked against the
    /// series the encoder writes for that record alone.
    #[test]
    fn record_routes_weigh_what_the_encoder_writes() {
        let counters =
            HistogramOptions { mode: HistogramMode::Counters, send_aggregation_metrics: true };
        let plain = HistogramOptions::default();
        let histogram = |sum, min, max| Histogram {
            buckets: vec![(1.0, 2), (10.0, 5), (f64::INFINITY, 1)],
            temporality: Temporality::Delta,
            sum,
            min,
            max,
        };
        let flagged = |kind| {
            let mut r = MetricRecord::new(intern("m"), kind);
            r.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
            r
        };
        let record = |kind| MetricRecord::new(intern("m"), kind);
        let oversized = ExpHistogram {
            scale: 0,
            zero_count: 0,
            zero_threshold: 0.0,
            positive: (0, vec![logit_core::sketch::from_otlp::MAX_OBSERVATIONS + 1]),
            negative: (0, vec![]),
            temporality: Temporality::Delta,
            count: 1,
            sum: Some(1.0),
            min: Some(1.0),
            max: Some(2.0),
        };
        let summary = |quantiles: Vec<(f64, f64)>| {
            MetricKind::Summary(Summary {
                quantiles,
                count: 3,
                sum: 6.0,
                temporality: Temporality::Cumulative,
            })
        };
        let rows: Vec<(&str, MetricRecord, HistogramOptions, usize)> = vec![
            // Three buckets, `.count`, and `.sum`; no `.min` or `.max` to write.
            (
                "counters, sum only",
                record(MetricKind::Histogram(histogram(Some(30.0), None, None))),
                counters,
                5,
            ),
            (
                "counters, no sum",
                record(MetricKind::Histogram(histogram(None, None, None))),
                counters,
                3,
            ),
            (
                "counters, non-finite min",
                record(MetricKind::Histogram(histogram(Some(30.0), Some(f64::NAN), Some(12.0)))),
                counters,
                6,
            ),
            (
                "counters, flagged",
                flagged(MetricKind::Histogram(histogram(Some(30.0), None, None))),
                counters,
                0,
            ),
            (
                "distributions, no aggregations",
                record(MetricKind::Histogram(histogram(Some(30.0), None, None))),
                plain,
                0,
            ),
            (
                "oversized exponential histogram",
                record(MetricKind::ExponentialHistogram(oversized)),
                counters,
                0,
            ),
            (
                "summary, a non-finite quantile",
                record(summary(vec![(0.5, 1.0), (0.9, f64::INFINITY), (1.0, 3.0)])),
                plain,
                2,
            ),
            ("summary, no quantiles", record(summary(vec![])), plain, 0),
            (
                "delta summary",
                record(MetricKind::Summary(Summary {
                    quantiles: vec![(0.5, 1.0)],
                    count: 3,
                    sum: 6.0,
                    temporality: Temporality::Delta,
                })),
                plain,
                3,
            ),
            (
                "delta summary, a non-finite sum",
                record(MetricKind::Summary(Summary {
                    quantiles: vec![(0.5, 1.0)],
                    count: 3,
                    sum: f64::NAN,
                    temporality: Temporality::Delta,
                })),
                plain,
                2,
            ),
            ("cumulative monotonic sum", record(sum(1.0, Temporality::Cumulative, true)), plain, 0),
            ("gauge delta", record(MetricKind::GaugeDelta(MetricValue::F64(1.0))), plain, 0),
            ("flagged gauge", flagged(MetricKind::gauge(1.0)), plain, 0),
            ("non-finite gauge", record(MetricKind::gauge(f64::NAN)), plain, 0),
            ("gauge", record(MetricKind::gauge(1.0)), plain, 1),
        ];
        // Each row's skips too: what a fresh copy's series route counts skipped or dropped, which
        // a stale copy counts in their place.
        let mut wrong = Vec::new();
        for (name, record, options, want) in rows {
            let routes = record_routes(&record, options);
            let (mut e, registry) = otlp_encoder(options);
            let written =
                v2_series(&mut e, &batch(vec![Event::metric(TS, AttrMap::new(), record)]));
            let c = self::counters(&registry);
            let skips: f64 = c
                .iter()
                .filter(|(m, ..)| {
                    m == "logit.output.metrics.skipped" || m == "logit.output.metrics.dropped"
                })
                .map(|c| c.2)
                .sum();
            if (routes.series, written.len(), routes.series_route) != (want, want, true)
                || routes.series_skips as f64 != skips
            {
                wrong.push(format!(
                    "{name}: weighs {}, writes {}, want {want}; skips {} vs {skips} counted; \
                     series route {}",
                    routes.series,
                    written.len(),
                    routes.series_skips,
                    routes.series_route
                ));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
        let h = record(MetricKind::Histogram(histogram(Some(1.0), None, None)));
        assert!(record_routes(&h, plain).sketch);
        assert!(!record_routes(&h, counters).sketch);
        let samples = record(MetricKind::Samples(Samples::new([1.0])));
        assert!(!record_routes(&samples, plain).series_route);
    }

    /// On an event over the tag limit nothing is written, so nothing counts how a series would
    /// have looked: no degrade, no normalization, no omitted tag. A cumulative kind's drop still
    /// counts.
    #[test]
    fn a_record_the_tag_limit_leaves_off_counts_no_degrade() {
        let (mut e, registry) = otlp_encoder(HistogramOptions::default());
        let mut attrs = AttrMap::new();
        for i in 0..=MAX_METRIC_TAGS {
            attrs.insert(format!("t{i}").as_str(), Value::str("v"));
        }
        attrs.insert("blob", Value::Bytes(bytes::Bytes::from_static(b"x")));
        let mut hll = HyperLogLog::new();
        hll.insert(b"a");
        let kinds = [
            sum(2.0, Temporality::Delta, false),
            MetricKind::Gauge(MetricValue::Bool(true)),
            MetricKind::Set(hll),
            sum(4.0, Temporality::Cumulative, true),
        ];
        let events =
            kinds.map(|k| Event::metric(TS, attrs.clone(), MetricRecord::new(intern("m"), k)));
        assert!(v2_series(&mut e, &batch(events.to_vec())).is_empty());
        let c = counters(&registry);
        assert_eq!(total(&c, "logit.output.metrics.degraded", "non_monotonic_sum"), 0.0);
        assert_eq!(total(&c, "logit.output.metrics.degraded", "set_estimate"), 0.0);
        assert_eq!(total(&c, "logit.output.metrics.normalized", "bool_value"), 0.0);
        assert_eq!(total(&c, "logit.output.tags.omitted", "unrepresentable"), 0.0);
        assert_eq!(total(&c, "logit.output.metrics.dropped", "unresolved_cumulative"), 1.0);
    }

    /// A summary the tag limit leaves off whole counts no left-off count and sum, and a histogram
    /// with no sum counts its missing aggregations only while some route writes it: the sketches
    /// route under 100 tags, none over it.
    #[test]
    fn notes_about_written_fields_need_a_written_record() {
        let options =
            HistogramOptions { mode: HistogramMode::Distributions, send_aggregation_metrics: true };
        let tagged = |n: usize, kind| {
            let mut attrs = AttrMap::new();
            for i in 0..n {
                attrs.insert(format!("t{i}").as_str(), Value::str("v"));
            }
            Event::metric(TS, attrs, MetricRecord::new(intern("m"), kind))
        };
        let summary = || {
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 1.0)],
                count: 1,
                sum: 1.0,
                temporality: Temporality::Cumulative,
            })
        };
        let no_sum = || MetricKind::Histogram(delta_histogram(None));
        let run = |events| {
            let (mut e, registry) = otlp_encoder(options);
            v2_series(&mut e, &batch(events));
            counters(&registry)
        };
        let over =
            run(vec![tagged(MAX_METRIC_TAGS, summary()), tagged(MAX_METRIC_TAGS + 1, no_sum())]);
        assert_eq!(total(&over, "logit.output.fields.dropped", "unresolved_cumulative"), 0.0);
        assert_eq!(total(&over, "logit.output.fields.omitted", "no_sum"), 0.0);
        let under = run(vec![tagged(1, summary()), tagged(1, no_sum())]);
        assert_eq!(total(&under, "logit.output.fields.dropped", "unresolved_cumulative"), 1.0);
        assert_eq!(total(&under, "logit.output.fields.omitted", "no_sum"), 1.0);
    }

    /// A `.bucket` or `.quantile` series whose own tags take the event's past the limit is left
    /// off, and counted by `series_over_tag_limit` for the sink; the record's other series go out.
    #[test]
    fn a_series_its_own_tags_take_past_the_limit_is_left_off() {
        let counters =
            HistogramOptions { mode: HistogramMode::Counters, send_aggregation_metrics: true };
        let mut attrs = AttrMap::new();
        for i in 0..MAX_METRIC_TAGS - 1 {
            attrs.insert(format!("t{i}").as_str(), Value::str("v"));
        }
        let record = MetricRecord::new(
            intern("h"),
            MetricKind::Histogram(Histogram {
                buckets: vec![(1.0, 2), (f64::INFINITY, 1)],
                temporality: Temporality::Delta,
                sum: Some(3.0),
                min: None,
                max: None,
            }),
        );
        assert_eq!(
            series_over_tag_limit(&record, counters, MAX_METRIC_TAGS - 1),
            (2, MAX_METRIC_TAGS + 1)
        );
        let mut e = DatadogEncoder::new().with_histograms(counters);
        let written = v2_series(&mut e, &batch(vec![Event::metric(TS, attrs, record)]));
        let names: Vec<&str> = written.iter().map(|s| s["metric"].as_str().unwrap()).collect();
        assert_eq!(names, ["h.count", "h.sum"]);
    }

    /// Go's `strconv.FormatFloat(f, 'g', -1, 64)` plus the translator's `.0`; the corpus test
    /// `datadog_otlp_series_differential` checks these forms against Go itself.
    #[test]
    fn format_float_writes_the_translators_forms() {
        for (f, want) in [
            (5.0, "5.0"),
            (0.5, "0.5"),
            (1e6, "1e+06.0"),
            (123456.0, "123456.0"),
            (1234567.0, "1.234567e+06.0"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (-2.5, "-2.5"),
            (0.0, "0"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (f64::NAN, "nan"),
        ] {
            assert_eq!(format_float(f), want, "{f}");
        }
    }
}
