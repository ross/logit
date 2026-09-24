//! Datadog metric series: `/api/v2/series` (protobuf `MetricPayload` and JSON), `/api/v1/series`
//! (JSON), and `/api/v1/distribution_points` (JSON). The mapping tables are in [`super`]'s module
//! doc, under "Metrics"; this file holds the shared series carriers [`super::sketches`] reuses.
//!
//! Every decoded point becomes one `Event` carrying one `MetricRecord`: a series with N points is
//! N events sharing their attributes. Point timestamps are whole seconds on every route, so a
//! decoded `Event::timestamp` is always a whole second too, including a `received_at` stand-in.

use super::generated::agentpayload::{metric_payload, Metadata, MetricPayload, Origin};
use super::tags::{insert_tags, render_tags};
use super::time::{nanos_to_seconds, seconds_to_nanos};
use super::{
    DatadogDecoder, DatadogEncoder, ATTR_DEVICE, ATTR_HOST_NAME, ATTR_INTERVAL,
    ATTR_ORIGIN_CATEGORY, ATTR_ORIGIN_METRIC_TYPE, ATTR_ORIGIN_PRODUCT, ATTR_ORIGIN_SERVICE,
    ATTR_RESOURCES, ATTR_SOURCE_TYPE_NAME, ATTR_TYPE,
};
use crate::CodecError;
use bytes::Bytes;
use logit_core::attrs::merged;
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Samples, Symbol, Temporality,
    Value,
};
use prost::Message;
use serde_json::{json, Map, Value as JsonValue};
use std::sync::{Arc, LazyLock};

type JsonMap = Map<String, JsonValue>;

/// `datadog.type`'s two values; `count` and `gauge` are said by the model kind itself.
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
            WireType::Gauge | WireType::Rate | WireType::Unspecified => MetricKind::Gauge(value),
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

/// `datadog.origin.*`: the protobuf `Origin`'s three codes plus the JSON API's `metric_type`.
/// A zero code is absent, the protobuf default.
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
    /// loses to the field it names.
    pub(super) fn attributes(&self) -> AttrMap {
        let mut attrs = AttrMap::new();
        insert_tags(&mut attrs, self.tags.iter().copied());
        if let Some(host) = self.host.filter(|h| !h.is_empty()) {
            attrs.insert(ATTR_HOST_NAME, Value::str(host));
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
        if let Some(t) = self.wire_type.and_then(WireType::type_attr) {
            attrs.insert(ATTR_TYPE, Value::str(t));
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
        attrs
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

fn series_array(root: &JsonValue) -> Result<&[JsonValue], CodecError> {
    root.as_object()
        .and_then(|o| o.get("series"))
        .and_then(JsonValue::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| {
            CodecError::Malformed("datadog metrics body has no `series` array".to_string())
        })
}

fn batch(events: Vec<Event>) -> EventBatch {
    EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
}

impl DatadogDecoder {
    pub(super) fn skipped(&self, reason: &'static str, n: usize) {
        if n > 0 {
            self.telemetry.count("logit.input.metrics.skipped", n as f64, &[("reason", reason)]);
        }
    }

    fn no_timestamp(&self) {
        self.telemetry.count("logit.input.metrics.degraded", 1.0, &[("reason", "no_timestamp")]);
    }

    /// Drops one series (or sketch) whose shape is wrong, keeping the rest of the request.
    pub(super) fn bad_series(&mut self, key: &'static str, why: impl std::fmt::Display) {
        self.skipped(key, 1);
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
        let attrs = series.attributes();
        for (seconds, value) in points {
            if !value.is_finite() {
                self.skipped("non_finite_value", 1);
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
                self.bad_series("bad_series", "a series has no metric name");
                continue;
            }
            let Some(wire_type) = WireType::from_proto(s.r#type.into()) else {
                self.bad_series("bad_series", format_args!("unknown metric type {}", s.r#type));
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
                    self.skipped("bad_point", bad);
                    self.push_points(&mut out, metric, unit, &series, points, received_at);
                }
                Err(why) => self.bad_series("bad_series", why),
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
                            V1Point::Null => self.skipped("null_value", 1),
                            V1Point::Bad => self.skipped("bad_point", 1),
                        }
                    }
                    self.push_points(&mut out, metric, unit, &series, kept, received_at);
                }
                Err(why) => self.bad_series("bad_series", why),
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
                    self.bad_series("bad_series", why);
                    continue;
                }
            };
            let name = intern(metric);
            let attrs = series.attributes();
            for point in points {
                let Some((seconds, values)) = distribution_point(point) else {
                    self.skipped("bad_point", 1);
                    continue;
                };
                if values.is_empty() {
                    self.skipped("empty_distribution", 1);
                    continue;
                }
                let (timestamp, defaulted) = point_timestamp(seconds, received_at);
                if defaulted {
                    self.no_timestamp();
                }
                let record = MetricRecord::new(name, MetricKind::Samples(Samples::new(values)));
                out.push(Event::metric(timestamp, attrs.clone(), record));
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
static CONSUMED: LazyLock<[Symbol; 10]> = LazyLock::new(|| {
    [
        intern(ATTR_HOST_NAME),
        intern(ATTR_TYPE),
        intern(ATTR_INTERVAL),
        intern(ATTR_SOURCE_TYPE_NAME),
        intern(ATTR_DEVICE),
        intern(ATTR_RESOURCES),
        intern(ATTR_ORIGIN_PRODUCT),
        intern(ATTR_ORIGIN_CATEGORY),
        intern(ATTR_ORIGIN_SERVICE),
        intern(ATTR_ORIGIN_METRIC_TYPE),
    ]
});

/// The carriers of one event (resource merged with event attributes), read back into wire fields,
/// plus the remaining attributes rendered as tags.
#[derive(Default)]
pub(super) struct Carriers {
    pub tags: Vec<String>,
    pub host: Option<String>,
    pub device: Option<String>,
    pub resources: Vec<(String, String)>,
    pub source_type_name: Option<String>,
    pub interval: i64,
    /// `datadog.type`'s value, when it is one this codec knows.
    pub wire_type: Option<WireType>,
    pub origin: OriginCodes,
}

impl Carriers {
    /// Which carriers are present that `route` has no field for, for a `no_wire_form` count.
    fn unwritable(&self, route: Route) -> usize {
        let origin = |set: bool| usize::from(set);
        match route {
            Route::V1 => {
                usize::from(!self.resources.is_empty())
                    + origin(self.origin.product != 0)
                    + origin(self.origin.category != 0)
                    + origin(self.origin.service != 0)
                    + origin(self.origin.metric_type != 0)
            }
            // v2 carries `device` as a resource; the JSON origin has no `category`.
            Route::V2Json => origin(self.origin.category != 0),
            Route::V2Protobuf => origin(self.origin.metric_type != 0),
            Route::Distribution => {
                usize::from(self.device.is_some())
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

    /// The v2 `resources` list: `host` first, then `device`, then `datadog.resources` in order.
    fn resource_list(&self) -> Vec<(&str, &str)> {
        let mut out = Vec::with_capacity(self.resources.len() + 2);
        if let Some(host) = &self.host {
            out.push((RESOURCE_HOST, host.as_str()));
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
    pub(super) fn out_skipped(&self, tag: (&'static str, &'static str)) {
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[tag]);
    }

    pub(super) fn out_degraded(&self, reason: &'static str) {
        self.telemetry.count("logit.output.metrics.degraded", 1.0, &[("reason", reason)]);
    }

    pub(super) fn tags_dropped(&self, reason: &'static str, n: usize) {
        if n > 0 {
            self.telemetry.count("logit.output.tags.dropped", n as f64, &[("reason", reason)]);
        }
    }

    /// Reads one event's carriers and renders the rest of its attributes as tags, counting
    /// whatever `route` can't write.
    pub(super) fn carriers(&self, resource: &Resource, event: &Event, route: Route) -> Carriers {
        let consumed = &*CONSUMED;
        let mut c = Carriers::default();
        let dropped = render_tags(merged(resource, event), consumed, &mut c.tags);
        let mut unrepresentable = dropped.unrepresentable;
        for (key, value) in merged(resource, event) {
            if !consumed.contains(&key) {
                continue;
            }
            let ok = match resolve(key) {
                ATTR_HOST_NAME => non_empty_str(value).map(|s| c.host = s).is_some(),
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
                _ => true,
            };
            if !ok {
                unrepresentable += 1;
            }
        }
        self.tags_dropped("unrepresentable", unrepresentable);
        self.tags_dropped("no_wire_form", c.unwritable(route));
        c
    }

    /// The Datadog type and value a series route sends for one record, or `None` when the record
    /// isn't one it carries (counted only when no metrics route carries it either).
    fn series_value(&self, record: &MetricRecord, carriers: &Carriers) -> Option<(WireType, f64)> {
        let (wire_type, value) = match &record.kind {
            MetricKind::Sum(sum) => match (sum.temporality, sum.monotonic) {
                (Temporality::Delta, true) => (WireType::Count, sum.value),
                (Temporality::Delta, false) => {
                    self.out_skipped(("metric_kind", "non_monotonic_delta_sum"));
                    return None;
                }
                (Temporality::Cumulative, _) => {
                    self.out_skipped(("metric_kind", "cumulative_sum"));
                    return None;
                }
            },
            MetricKind::Gauge(v) => (carriers.wire_type.unwrap_or(WireType::Gauge), *v),
            MetricKind::Set(hll) => {
                self.out_degraded("set_estimate");
                (WireType::Gauge, hll.estimate() as f64)
            }
            // Their own routes: `encode_distribution_points` and `encode_sketches`.
            MetricKind::Samples(_) | MetricKind::Distribution(_) => return None,
            MetricKind::GaugeDelta(_)
            | MetricKind::SetMembers(_)
            | MetricKind::Histogram(_)
            | MetricKind::ExponentialHistogram(_)
            | MetricKind::Summary(_) => {
                self.out_skipped(("metric_kind", record.kind.name()));
                return None;
            }
        };
        if record.is_no_recorded_value() {
            self.out_skipped(("reason", "no_recorded_value"));
            return None;
        }
        if !value.is_finite() {
            self.out_skipped(("reason", "non_finite_value"));
            return None;
        }
        Some((wire_type, value))
    }

    /// Calls `f` once per `(event, record, type, value)` a series route sends.
    fn each_series(
        &self,
        batch: &EventBatch,
        route: Route,
        mut f: impl FnMut(&Event, &MetricRecord, &Carriers, WireType, f64),
    ) {
        for event in &batch.events {
            let mut carriers = None;
            for record in &event.metrics {
                let c = match &carriers {
                    Some(c) => c,
                    None => {
                        // Read lazily: an event with nothing for this route counts nothing.
                        if !matches!(
                            record.kind,
                            MetricKind::Sum(_) | MetricKind::Gauge(_) | MetricKind::Set(_)
                        ) {
                            self.series_value(record, &Carriers::default());
                            continue;
                        }
                        carriers.insert(self.carriers(&batch.resource, event, route))
                    }
                };
                if let Some((wire_type, value)) = self.series_value(record, c) {
                    f(event, record, c, wire_type, value);
                }
            }
        }
    }

    /// `/api/v2/series` as a protobuf `MetricPayload`, one series per record; `None` when nothing
    /// in the batch is a series.
    pub fn encode_series_v2_protobuf(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let mut series = Vec::new();
        self.each_series(batch, Route::V2Protobuf, |event, record, c, wire_type, value| {
            series.push(metric_payload::MetricSeries {
                resources: c
                    .resource_list()
                    .into_iter()
                    .map(|(t, n)| metric_payload::Resource {
                        r#type: t.to_string(),
                        name: n.to_string(),
                    })
                    .collect(),
                metric: resolve(record.name).to_string(),
                tags: c.tags.clone(),
                points: vec![metric_payload::MetricPoint {
                    value,
                    timestamp: nanos_to_seconds(event.timestamp),
                }],
                r#type: wire_type.proto(),
                unit: record.unit.map(|u| resolve(u).to_string()).unwrap_or_default(),
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
        self.each_series(batch, Route::V2Json, |event, record, c, wire_type, value| {
            let mut s = JsonMap::new();
            s.insert("metric".into(), resolve(record.name).into());
            s.insert("type".into(), wire_type.proto().into());
            s.insert(
                "points".into(),
                json!([{ "timestamp": nanos_to_seconds(event.timestamp), "value": value }]),
            );
            let resources = c.resource_list();
            if !resources.is_empty() {
                let list = resources.iter().map(|(t, n)| json!({ "type": t, "name": n }));
                s.insert("resources".into(), JsonValue::Array(list.collect()));
            }
            s.insert("tags".into(), c.tags.clone().into());
            if let Some(unit) = record.unit {
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

    /// `/api/v1/series`, with the Agent's own field set: `tags`, `host`, `type`, and `interval`
    /// always; `device`, `source_type_name`, and `unit` when present.
    pub fn encode_series_v1(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let mut series = Vec::new();
        self.each_series(batch, Route::V1, |event, record, c, wire_type, value| {
            let mut s = JsonMap::new();
            s.insert("metric".into(), resolve(record.name).into());
            s.insert("points".into(), json!([[nanos_to_seconds(event.timestamp), value]]));
            s.insert("tags".into(), c.tags.clone().into());
            s.insert("host".into(), c.host.clone().unwrap_or_default().into());
            if let Some(device) = &c.device {
                s.insert("device".into(), device.as_str().into());
            }
            s.insert("type".into(), wire_type.v1().into());
            s.insert("interval".into(), c.interval.into());
            if let Some(name) = &c.source_type_name {
                s.insert("source_type_name".into(), name.as_str().into());
            }
            if let Some(unit) = record.unit {
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
            for record in &event.metrics {
                let MetricKind::Samples(samples) = &record.kind else { continue };
                if record.is_no_recorded_value() {
                    self.out_skipped(("reason", "no_recorded_value"));
                    continue;
                }
                let finite: Vec<f64> =
                    samples.values.iter().copied().filter(|v| v.is_finite()).collect();
                if finite.len() != samples.values.len() {
                    self.out_skipped(("reason", "non_finite_value"));
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
                series.push(json!({
                    "metric": resolve(record.name),
                    "points": [[nanos_to_seconds(event.timestamp), values]],
                    "host": c.host.clone().unwrap_or_default(),
                    "tags": c.tags,
                    "type": "distribution",
                }));
            }
        }
        json_body(series)
    }
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
    use logit_core::{HyperLogLog, Registry, Sum};

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
                        total += s.value;
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
        assert_eq!(m.kind, MetricKind::Gauge(1.5));
        let a = &e.attributes;
        assert_eq!(a.get("env"), Some(&Value::str("prod")));
        assert_eq!(a.get("urgent"), Some(&Value::Bool(true)));
        assert_eq!(a.get(ATTR_HOST_NAME), Some(&Value::str("myhost")));
        assert_eq!(a.get(ATTR_DEVICE), Some(&Value::str("/dev/sda1")));
        assert_eq!(a.get(ATTR_TYPE), Some(&Value::str("rate")));
        assert_eq!(a.get(ATTR_INTERVAL), Some(&Value::I64(20)));
        assert_eq!(a.get(ATTR_SOURCE_TYPE_NAME), Some(&Value::str("my_check")));
    }

    #[test]
    fn v1_types_map_to_kinds() {
        let (mut d, _) = decoder();
        for (t, kind, attr) in [
            (r#""count""#, MetricKind::counter(2.0), None),
            (r#""gauge""#, MetricKind::Gauge(2.0), None),
            (r#""""#, MetricKind::Gauge(2.0), Some("unspecified")),
            ("null", MetricKind::Gauge(2.0), None),
        ] {
            let body =
                format!(r#"{{"series":[{{"metric":"m","points":[[1700000000,2]],"type":{t}}}]}}"#);
            let e = &d.decode_series_v1(body.as_bytes(), RECEIVED_AT).unwrap().events[0];
            assert_eq!(e.metrics[0].kind, kind, "{t}");
            assert_eq!(e.attributes.get(ATTR_TYPE).and_then(Value::as_str), attr, "{t}");
        }
    }

    #[test]
    fn v1_null_values_and_bad_series_are_skipped_and_counted() {
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
        let total = |reason: &str| -> f64 {
            events
                .iter()
                .filter(|e| e.attributes.get("reason").and_then(Value::as_str) == Some(reason))
                .flat_map(|e| e.metrics.iter())
                .filter(|m| resolve(m.name) == "logit.input.metrics.skipped")
                .map(|m| match m.kind {
                    MetricKind::Sum(Sum { value, .. }) => value,
                    _ => 0.0,
                })
                .sum()
        };
        assert_eq!(total("null_value"), 1.0);
        assert_eq!(total("bad_series"), 2.0);
        assert_eq!(total("bad_point"), 1.0);
    }

    #[test]
    fn unparseable_bodies_are_malformed() {
        let (mut d, _) = decoder();
        assert!(matches!(d.decode_series_v1(b"nope", 0), Err(CodecError::Malformed(_))));
        assert!(matches!(d.decode_series_v2_json(b"{}", 0), Err(CodecError::Malformed(_))));
        assert!(matches!(
            d.decode_series_v2_protobuf(b"\xff\xff\xff", 0),
            Err(CodecError::Malformed(_))
        ));
        assert!(matches!(d.decode_distribution_points(b"[]", 0), Err(CodecError::Malformed(_))));
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
        assert_eq!(batch.events[0].metrics[0].kind, MetricKind::Gauge(1.0));
        assert_eq!(
            counted(&registry, "logit.input.metrics.skipped", ("reason", "bad_series")),
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
        for kind in [
            MetricKind::counter(3.0),
            MetricKind::Set(hll),
            MetricKind::Sum(Sum {
                value: 1.0,
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
            MetricKind::GaugeDelta(1.0),
            MetricKind::Samples(Samples::new([1.0])),
            MetricKind::Gauge(f64::NAN),
        ] {
            events.push(Event::metric(TS, attrs.clone(), MetricRecord::new(intern("m"), kind)));
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
        assert!(has("logit.output.metrics.skipped", "metric_kind", "cumulative_sum"));
        assert!(has("logit.output.metrics.skipped", "metric_kind", "gauge_delta"));
        assert!(has("logit.output.metrics.skipped", "reason", "non_finite_value"));
        assert!(!has("logit.output.metrics.skipped", "metric_kind", "samples"));
    }

    #[test]
    fn nothing_to_send_is_none() {
        let (mut e, _) = encoder();
        let b = one_event_batch(AttrMap::new(), MetricKind::Samples(Samples::new([1.0])));
        assert!(e.encode_series_v1(&b).is_none());
        assert!(e.encode_series_v2_protobuf(&b).is_none());
        let b = one_event_batch(AttrMap::new(), MetricKind::Gauge(1.0));
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
        let mut record = MetricRecord::new(intern("r"), MetricKind::Gauge(0.5));
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
        let b = one_event_batch(attrs, MetricKind::Gauge(1.0));
        e.encode_series_v1(&b).unwrap();
        let drained = registry.drain(0);
        let reason = |r: &str| {
            drained.iter().any(|ev| ev.attributes.get("reason").and_then(Value::as_str) == Some(r))
        };
        assert!(reason("no_wire_form"));
        assert!(reason("unrepresentable"));
    }
}
