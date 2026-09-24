//! Datadog Agent sketches: `/api/beta/sketches`, a protobuf `SketchPayload` of `Dogsketch`es, to
//! and from `MetricKind::Distribution` under [`Mapping::agent`]. The mapping tables are in
//! [`super`]'s module doc, under "Metrics".
//!
//! The `Dogsketch` wire form is the Agent's sparse store verbatim: `k` (ascending `sint32` keys,
//! negative for negative values, `0` for the zero bin) paired 1:1 with `n` (`uint32` counts), a
//! count above 65535 split across repeated entries of one key. It carries no mapping parameters,
//! so every decoded sketch is [`Mapping::agent`], and every encoded one has to be.

use super::generated::agentpayload::sketch_payload::{sketch::Dogsketch, Sketch};
use super::generated::agentpayload::SketchPayload;
use super::series::{OriginCodes, Route, WireSeries};
use super::time::{nanos_to_seconds, seconds_to_nanos};
use super::{DatadogDecoder, DatadogEncoder};
use crate::CodecError;
use bytes::Bytes;
use logit_core::interner::{intern, resolve};
use logit_core::{
    Bin, DdSketch, Event, EventBatch, Mapping, MappingKind, MetricKind, MetricRecord, Resource,
    SketchStats,
};
use prost::Message;
use std::sync::Arc;

/// The largest count one `n` entry carries; the Agent's bins are `uint16`.
pub const MAX_BIN_COUNT: u32 = 65535;

impl DatadogDecoder {
    /// `/api/beta/sketches`: one `Event` per `Dogsketch`, its `MetricRecord` a
    /// `Distribution` under [`Mapping::agent`], bin-for-bin.
    pub fn decode_sketches(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let payload = SketchPayload::decode(body).map_err(|e| {
            CodecError::Malformed(format!("datadog SketchPayload does not decode: {e}"))
        })?;
        let mut out = Vec::new();
        for s in &payload.sketches {
            self.skipped("legacy_distribution", s.distributions.len());
            if s.metric.is_empty() {
                self.bad_series("bad_sketch", "a sketch has no metric name");
                continue;
            }
            let name = intern(&s.metric);
            let attrs = WireSeries {
                tags: s.tags.iter().map(String::as_str).collect(),
                host: Some(&s.host),
                origin: OriginCodes::from_proto(s.metadata.as_ref()),
                ..WireSeries::default()
            }
            .attributes();
            for dog in &s.dogsketches {
                let sketch = match dogsketch_to_sketch(dog) {
                    Ok(Some(sketch)) => sketch,
                    Ok(None) => {
                        self.skipped("empty_sketch", 1);
                        continue;
                    }
                    Err(why) => {
                        self.bad_series("bad_sketch", why);
                        continue;
                    }
                };
                let timestamp = if dog.ts > 0 {
                    seconds_to_nanos(dog.ts)
                } else {
                    self.telemetry.count(
                        "logit.input.metrics.degraded",
                        1.0,
                        &[("reason", "no_timestamp")],
                    );
                    seconds_to_nanos(nanos_to_seconds(received_at))
                };
                let record = MetricRecord::new(name, MetricKind::Distribution(sketch));
                out.push(Event::metric(timestamp, attrs.clone(), record));
            }
        }
        Ok(EventBatch { resource: Arc::new(Resource::default()), scope: None, events: out })
    }
}

/// `Ok(None)` for an empty sketch (no bins, zero count), which the encoder never sends either.
fn dogsketch_to_sketch(dog: &Dogsketch) -> Result<Option<DdSketch>, String> {
    if dog.k.len() != dog.n.len() {
        return Err(format!("{} keys but {} counts", dog.k.len(), dog.n.len()));
    }
    let mut positive = Vec::new();
    let mut negative = Vec::new();
    let mut zero = 0.0;
    for (&k, &n) in dog.k.iter().zip(&dog.n) {
        if !(-Mapping::AGENT_INF_KEY..=Mapping::AGENT_INF_KEY).contains(&k) {
            return Err(format!("key {k} is outside the Agent's int16 key space"));
        }
        let count = f64::from(n);
        match k {
            0 => zero += count,
            k if k > 0 => positive.push(Bin { key: k, count }),
            k => negative.push(Bin { key: -k, count }),
        }
    }
    let populated = zero > 0.0 || dog.n.iter().any(|&n| n > 0);
    if !populated && dog.cnt == 0 {
        return Ok(None);
    }
    if dog.cnt <= 0 {
        return Err(format!("populated bins with a count of {}", dog.cnt));
    }
    if !(dog.min.is_finite() && dog.max.is_finite() && dog.sum.is_finite()) {
        return Err("a non-finite min, max, or sum".to_string());
    }
    let stats = SketchStats { count: dog.cnt as f64, min: dog.min, max: dog.max, sum: dog.sum };
    Ok(Some(DdSketch::from_parts(Mapping::agent(), positive, negative, zero, Some(stats))))
}

impl DatadogEncoder {
    /// `/api/beta/sketches`: every `Distribution` record as one `Sketch` with one `Dogsketch`.
    /// Every other kind is left to its own route, uncounted here; `None` when nothing is sent.
    pub fn encode_sketches(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let mut sketches = Vec::new();
        for event in &batch.events {
            let mut carriers = None;
            for record in &event.metrics {
                let MetricKind::Distribution(sketch) = &record.kind else { continue };
                if record.is_no_recorded_value() {
                    self.out_skipped(("reason", "no_recorded_value"));
                    continue;
                }
                let Some(dog) = self.dogsketch(sketch, event.timestamp) else {
                    self.out_skipped(("reason", "empty_sketch"));
                    continue;
                };
                let c = carriers
                    .get_or_insert_with(|| self.carriers(&batch.resource, event, Route::Sketches));
                sketches.push(Sketch {
                    metric: resolve(record.name).to_string(),
                    host: c.host.clone().unwrap_or_default(),
                    distributions: Vec::new(),
                    tags: c.tags.clone(),
                    dogsketches: vec![dog],
                    metadata: c.proto_metadata(),
                });
            }
        }
        (!sketches.is_empty())
            .then(|| Bytes::from(SketchPayload { sketches, metadata: None }.encode_to_vec()))
    }

    /// One sketch as a `Dogsketch`, re-binned first when it isn't under the Agent mapping; `None`
    /// for an empty one.
    fn dogsketch(&self, sketch: &DdSketch, timestamp: i64) -> Option<Dogsketch> {
        let rebinned;
        let sketch = if *sketch.mapping() == Mapping::agent() {
            sketch
        } else {
            self.out_degraded("rebinned");
            rebinned = rebin_to_agent(sketch);
            &rebinned
        };
        let mut k = Vec::new();
        let mut n = Vec::new();
        let mut fractional = false;
        let mut push = |key: i32, count: f64| {
            let rounded = count.round();
            fractional |= rounded != count;
            let mut left = if rounded >= f64::from(u32::MAX) { u32::MAX } else { rounded as u32 };
            while left > 0 {
                let chunk = left.min(MAX_BIN_COUNT);
                k.push(key);
                n.push(chunk);
                left -= chunk;
            }
        };
        for bin in sketch.negative_bins().iter().rev() {
            push(-bin.key, bin.count);
        }
        if sketch.zero_count() > 0.0 {
            push(0, sketch.zero_count());
        }
        for bin in sketch.positive_bins() {
            push(bin.key, bin.count);
        }
        if fractional {
            self.out_degraded("fractional_count");
        }
        let cnt = sketch.count() as i64;
        if k.is_empty() && cnt == 0 {
            return None;
        }
        let sum = sketch.sum();
        Some(Dogsketch {
            ts: nanos_to_seconds(timestamp),
            cnt,
            min: sketch.min().unwrap_or(0.0),
            max: sketch.max().unwrap_or(0.0),
            avg: if cnt > 0 { sum / cnt as f64 } else { 0.0 },
            sum,
            k,
            n,
        })
    }
}

/// Re-keys a sketch under another mapping into [`Mapping::agent`], each bin at its
/// representative value (the `DdSketch::merge` rule across mappings; `merge` itself can't be used,
/// since merging into an empty sketch adopts the incoming mapping). The summary is kept as is.
fn rebin_to_agent(sketch: &DdSketch) -> DdSketch {
    let mapping = sketch.mapping();
    let mut agent = DdSketch::new();
    let representative = |key: i32| -> f64 {
        let lower = match mapping.kind() {
            MappingKind::Agent => mapping.gamma().powf(f64::from(key) - mapping.index_offset()),
            MappingKind::Logarithmic => {
                ((f64::from(key) - mapping.index_offset()) * mapping.gamma().ln()).exp()
            }
        };
        let v = match mapping.kind() {
            MappingKind::Agent => lower,
            MappingKind::Logarithmic => lower * (2.0 * mapping.gamma() / (1.0 + mapping.gamma())),
        };
        if v.is_finite() {
            v
        } else {
            f64::MAX
        }
    };
    for bin in sketch.positive_bins() {
        agent.add_count(representative(bin.key), bin.count);
    }
    for bin in sketch.negative_bins() {
        agent.add_count(-representative(bin.key), bin.count);
    }
    agent.add_count(0.0, sketch.zero_count());
    let stats = SketchStats {
        count: sketch.count() as f64,
        min: sketch.min().unwrap_or(0.0),
        max: sketch.max().unwrap_or(0.0),
        sum: sketch.sum(),
    };
    DdSketch::from_parts(
        Mapping::agent(),
        agent.positive_bins().to_vec(),
        agent.negative_bins().to_vec(),
        agent.zero_count(),
        Some(stats),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datadog::generated::agentpayload::{sketch_payload::sketch::Distribution, Metadata};
    use crate::datadog::{ATTR_HOST_NAME, ATTR_ORIGIN_PRODUCT};
    use logit_core::{AttrMap, Registry, Value};

    const TS_S: i64 = 1_700_000_000;

    fn dog(k: Vec<i32>, n: Vec<u32>, cnt: i64) -> Dogsketch {
        Dogsketch { ts: TS_S, cnt, min: -3.0, max: 9.0, avg: 0.0, sum: 12.0, k, n }
    }

    fn payload(dogs: Vec<Dogsketch>) -> Vec<u8> {
        SketchPayload {
            sketches: vec![Sketch {
                metric: "lat".into(),
                host: "h".into(),
                distributions: vec![Distribution::default()],
                tags: vec!["env:prod".into()],
                dogsketches: dogs,
                metadata: Some(Metadata { origin: None }),
            }],
            metadata: None,
        }
        .encode_to_vec()
    }

    fn with_registry() -> (DatadogDecoder, DatadogEncoder, Arc<Registry>) {
        let registry = Registry::new();
        let t = registry.telemetry_for("dd", "datadog", "listener");
        (
            DatadogDecoder::new().with_telemetry(t.clone()),
            DatadogEncoder::new().with_telemetry(t),
            registry,
        )
    }

    fn reasons(registry: &Registry) -> Vec<String> {
        registry
            .drain(0)
            .iter()
            .filter_map(|e| e.attributes.get("reason").and_then(Value::as_str).map(String::from))
            .collect()
    }

    #[test]
    fn a_dogsketch_decodes_bin_for_bin() {
        let (mut d, _, registry) = with_registry();
        let body =
            payload(vec![dog(vec![-5, -5, -2, 0, 3, 3], vec![1, 2, 3, 4, 65535, 10], 65555)]);
        let batch = d.decode_sketches(&body, 0).unwrap();
        let e = &batch.events[0];
        assert_eq!(e.timestamp, seconds_to_nanos(TS_S));
        assert_eq!(e.attributes.get(ATTR_HOST_NAME), Some(&Value::str("h")));
        assert_eq!(e.attributes.get("env"), Some(&Value::str("prod")));
        let MetricKind::Distribution(s) = &e.metrics[0].kind else { panic!() };
        assert_eq!(*s.mapping(), Mapping::agent());
        assert_eq!(s.negative_bins(), &[Bin { key: 2, count: 3.0 }, Bin { key: 5, count: 3.0 }]);
        assert_eq!(s.positive_bins(), &[Bin { key: 3, count: 65545.0 }]);
        assert_eq!(s.zero_count(), 4.0);
        assert_eq!(s.count(), 65555);
        assert_eq!((s.min(), s.max(), s.sum()), (Some(-3.0), Some(9.0), 12.0));
        assert!(reasons(&registry).contains(&"legacy_distribution".to_string()));
    }

    #[test]
    fn malformed_and_empty_dogsketches_are_skipped() {
        let (mut d, _, registry) = with_registry();
        let body = payload(vec![
            dog(vec![1, 2], vec![1], 1),
            dog(vec![40000], vec![1], 1),
            dog(vec![], vec![], 0),
            dog(vec![1], vec![1], 1),
        ]);
        assert_eq!(d.decode_sketches(&body, 0).unwrap().events.len(), 1);
        let r = reasons(&registry);
        assert!(r.contains(&"bad_sketch".to_string()));
        assert!(r.contains(&"empty_sketch".to_string()));
    }

    #[test]
    fn encode_orders_keys_and_splits_large_counts() {
        let (_, mut e, _) = with_registry();
        let sketch = DdSketch::from_parts(
            Mapping::agent(),
            vec![Bin { key: 3, count: 70000.0 }, Bin { key: 1, count: 1.0 }],
            vec![Bin { key: 2, count: 3.0 }, Bin { key: 5, count: 1.0 }],
            2.0,
            Some(SketchStats { count: 70007.0, min: -1.0, max: 5.0, sum: 10.0 }),
        );
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_HOST_NAME, Value::str("h"));
        attrs.insert(ATTR_ORIGIN_PRODUCT, Value::U64(4));
        let record = MetricRecord::new(intern("lat"), MetricKind::Distribution(sketch));
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::metric(seconds_to_nanos(TS_S), attrs, record)],
        };
        let payload = SketchPayload::decode(e.encode_sketches(&batch).unwrap()).unwrap();
        let s = &payload.sketches[0];
        assert_eq!(s.host, "h");
        assert!(s.tags.is_empty());
        assert_eq!(s.metadata.unwrap().origin.unwrap().origin_product, 4);
        let dog = &s.dogsketches[0];
        assert_eq!(dog.k, vec![-5, -2, 0, 1, 3, 3]);
        assert_eq!(dog.n, vec![1, 3, 2, 1, 65535, 4465]);
        assert_eq!((dog.cnt, dog.ts), (70007, TS_S));
        assert_eq!(dog.avg, 10.0 / 70007.0);
    }

    #[test]
    fn a_logarithmic_sketch_is_rebinned_and_fractional_counts_rounded() {
        let (_, mut e, registry) = with_registry();
        let mut log = DdSketch::with_mapping(Mapping::logarithmic(1.02, 0.0, 2048));
        log.add_count(10.0, 1.4);
        log.add_count(-2.0, 2.0);
        let record = MetricRecord::new(intern("s"), MetricKind::Distribution(log));
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::metric(seconds_to_nanos(TS_S), AttrMap::new(), record)],
        };
        let payload = SketchPayload::decode(e.encode_sketches(&batch).unwrap()).unwrap();
        let dog = &payload.sketches[0].dogsketches[0];
        assert_eq!(dog.n, vec![2, 1]);
        assert!(dog.k[0] < 0 && dog.k[1] > 0);
        let r = reasons(&registry);
        assert!(r.contains(&"rebinned".to_string()));
        assert!(r.contains(&"fractional_count".to_string()));
    }

    #[test]
    fn non_distributions_are_left_alone() {
        let (_, mut e, registry) = with_registry();
        let record = MetricRecord::new(intern("g"), MetricKind::Gauge(1.0));
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::metric(1, AttrMap::new(), record)],
        };
        assert!(e.encode_sketches(&batch).is_none());
        assert!(reasons(&registry).is_empty());
    }
}
