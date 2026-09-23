//! `sample`: keeps a fraction of events, consistently -- see
//! `docs/adr/consistent-sampling-component.md`.
//!
//! Per event, in order: an `always_keep` hit is kept outright; otherwise, with a `key` configured,
//! the key's value is hashed through `logit_core::sampling`'s frozen contract and kept iff
//! `sampling::keep(hash, rate)`; with no key, a per-instance counter mixed with a random seed
//! stands in for a draw. A configured key the event doesn't carry falls to `missing`. Every event
//! sharing a key gets the same verdict -- in this node, in every other `sample` node configured
//! with the same rate, and in every other `logit` process -- with nothing propagated. That
//! property is the whole reason this is a native kind rather than `math.random()` in `lua`.
//!
//! Never mutates an event: it only ever decides whether to forward one.
//!
//! **`always_keep` compares under `crate::value_matches`**, so it inherits `has_attributes`'
//! rules exactly -- including that a `Bool` never matches a string: `value: true` does not match
//! a logfmt line's `sampling.keep=true`, which arrives as `Str("true")`. Write `value: "true"`
//! for that producer, or omit `value:` to match any value.
//!
//! **Telemetry** is tallied in plain integers per event and emitted once per batch from
//! `end_batch` (`kv_metrics`' `Tally` pattern): `logit.transform.events.filtered` (the filter
//! family's counter -- the dropped count, emitted even when `0` so the series registers) and
//! `logit.transform.sample.decisions{outcome, by}` for each non-zero cell. `by` is `override` for
//! an `always_keep` hit, `key` for a hashed verdict, `random` for a keyless sampler's draw, and
//! `missing` for an event whose configured key was absent -- whatever `missing:` then did with it,
//! a random draw included, so the cell counts exactly the events the key didn't cover. The node
//! runtime also counts every drop as `logit.component.events.dropped{reason="absorbed"}`
//! (`crates/logit-pipeline/src/runtime.rs`'s `process_batch`), as it does for any transform that
//! returns `false`.
//!
//! Zero allocations per event, pinned by `crates/logit-bench/tests/allocations.rs`: keys and the
//! override field are interned once at construction, lookups are `AttrMap::get_sym`, and the hash
//! formats numbers straight into the hasher.

use crate::value_matches;
use logit_core::interner::intern;
use logit_core::sampling;
use logit_core::{Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// What to hash. Mirrors `logit_config::SampleKey` -- `logit-transforms` doesn't depend on
/// `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), so the CLI converts, the
/// `keep_values`/`flatten` pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SampleKey {
    /// `span.trace_id`, else `log.trace.trace_id`, hashed as 32 lowercase hex characters.
    TraceId,
    /// A top-level event attribute, named literally.
    Attribute(String),
    /// A resource attribute.
    Resource(String),
}

/// What to do with an event the configured key isn't on. Mirrors `logit_config::SampleMissing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SampleMissing {
    /// Draw as if no key were configured.
    #[default]
    Random,
    Keep,
    Drop,
}

/// Where an `always_keep` override looks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SampleField {
    Attribute(String),
    Resource(String),
}

/// `always_keep`: the field to look for and, optionally, the value it must carry. Mirrors
/// `logit_config::SampleOverride`, with graph rule 61's "exactly one of attribute/resource"
/// already folded into [`SampleField`].
#[derive(Debug, Clone, PartialEq)]
pub struct SampleOverride {
    pub field: SampleField,
    /// `None` matches any value.
    pub value: Option<Value>,
}

/// [`SampleKey`]/[`SampleField`] with their names interned once, at construction.
#[derive(Clone, Copy)]
enum Compiled {
    TraceId,
    Attribute(Symbol),
    Resource(Symbol),
}

impl Compiled {
    fn key(key: &SampleKey) -> Self {
        match key {
            SampleKey::TraceId => Compiled::TraceId,
            SampleKey::Attribute(name) => Compiled::Attribute(intern(name)),
            SampleKey::Resource(name) => Compiled::Resource(intern(name)),
        }
    }

    fn field(field: &SampleField) -> Self {
        match field {
            SampleField::Attribute(name) => Compiled::Attribute(intern(name)),
            SampleField::Resource(name) => Compiled::Resource(intern(name)),
        }
    }

    /// The attribute/resource value this names on `event`, if any. `TraceId` is never looked up
    /// through here -- it isn't a `Value`.
    fn lookup<'a>(self, resource: &'a Resource, event: &'a Event) -> Option<&'a Value> {
        match self {
            Compiled::TraceId => None,
            Compiled::Attribute(sym) => event.attributes.get_sym(sym),
            Compiled::Resource(sym) => resource.attributes.get_sym(sym),
        }
    }

    /// The contract's hash of this key on `event`, or `None` if the event doesn't carry it.
    fn hash(self, resource: &Resource, event: &Event) -> Option<u64> {
        match self {
            Compiled::TraceId => event
                .span
                .as_ref()
                .map(|span| &span.trace_id)
                .or_else(|| event.log.as_ref()?.trace.as_ref().map(|t| &t.trace_id))
                .map(sampling::hash_trace_id),
            _ => self.lookup(resource, event).and_then(sampling::hash_value),
        }
    }
}

/// Why an event got its verdict -- the `by` tag, and an index into [`Tally`].
#[derive(Clone, Copy)]
enum By {
    Key = 0,
    Random = 1,
    Override = 2,
    Missing = 3,
}

impl By {
    const ALL: [By; 4] = [By::Key, By::Random, By::Override, By::Missing];

    fn tag(self) -> &'static str {
        match self {
            By::Key => "key",
            By::Random => "random",
            By::Override => "override",
            By::Missing => "missing",
        }
    }
}

/// Per-batch decision counts, emitted and reset by [`Sample::end_batch`].
#[derive(Default)]
struct Tally {
    kept: [u64; 4],
    dropped: [u64; 4],
}

impl Tally {
    fn record(&mut self, by: By, kept: bool) {
        if kept {
            self.kept[by as usize] += 1;
        } else {
            self.dropped[by as usize] += 1;
        }
    }

    fn is_empty(&self) -> bool {
        self.kept.iter().chain(&self.dropped).all(|n| *n == 0)
    }

    fn flush(&mut self, telemetry: &Telemetry) {
        telemetry.count(
            "logit.transform.events.filtered",
            self.dropped.iter().sum::<u64>() as f64,
            &[],
        );
        for by in By::ALL {
            for (outcome, n) in
                [("kept", self.kept[by as usize]), ("dropped", self.dropped[by as usize])]
            {
                if n > 0 {
                    telemetry.count(
                        "logit.transform.sample.decisions",
                        n as f64,
                        &[("outcome", outcome), ("by", by.tag())],
                    );
                }
            }
        }
        *self = Tally::default();
    }
}

pub struct Sample {
    rate: f64,
    key: Option<Compiled>,
    missing: SampleMissing,
    always_keep: Option<(Compiled, Option<Value>)>,
    /// Mixed with `counter` for a draw (`sampling::mix`). Random per instance so two keyless
    /// samplers never march in lockstep; [`Sample::with_seed`] fixes it for a reproducible test.
    seed: u64,
    counter: u64,
    telemetry: Telemetry,
    tally: Tally,
}

impl Sample {
    /// `rate` is the fraction kept; graph rule 61 has already checked it (and everything else
    /// here), so this never fails.
    pub fn new(
        rate: f64,
        key: Option<SampleKey>,
        missing: SampleMissing,
        always_keep: Option<SampleOverride>,
    ) -> Self {
        Self {
            rate,
            key: key.as_ref().map(Compiled::key),
            missing,
            always_keep: always_keep.map(|o| (Compiled::field(&o.field), o.value)),
            seed: u64::from_le_bytes(logit_core::random_id_bytes::<8>()),
            counter: 0,
            telemetry: Telemetry::default(),
            tally: Tally::default(),
        }
    }

    /// Fixes the keyless draw's seed, so a test's sequence of verdicts is reproducible.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// See [`crate::Keep::with_telemetry`]. No `Diagnostics`: nothing here can fail.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    fn draw(&mut self) -> bool {
        self.counter = self.counter.wrapping_add(1);
        sampling::keep(sampling::mix(self.counter, self.seed), self.rate)
    }

    fn decide(&mut self, resource: &Resource, event: &Event) -> (By, bool) {
        if let Some((field, value)) = &self.always_keep {
            let hit = field.lookup(resource, event).is_some_and(|actual| match value {
                Some(configured) => value_matches(configured, actual),
                None => true,
            });
            if hit {
                return (By::Override, true);
            }
        }
        let Some(key) = self.key else {
            return (By::Random, self.draw());
        };
        match key.hash(resource, event) {
            Some(hash) => (By::Key, sampling::keep(hash, self.rate)),
            None => match self.missing {
                SampleMissing::Random => (By::Missing, self.draw()),
                SampleMissing::Keep => (By::Missing, true),
                SampleMissing::Drop => (By::Missing, false),
            },
        }
    }
}

impl Transform for Sample {
    fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool {
        let (by, keep) = self.decide(resource, event);
        self.tally.record(by, keep);
        keep
    }

    fn end_batch(&mut self) {
        if !self.tally.is_empty() {
            self.tally.flush(&self.telemetry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::{
        AttrMap, BodyFormat, LogRecord, MetricKind, Registry, SpanKind, SpanRecord, SpanStatus,
        TraceRef,
    };

    fn log_event(attrs: &[(&str, Value)]) -> Event {
        let mut map = AttrMap::new();
        for (k, v) in attrs {
            map.insert(k, v.clone());
        }
        Event::log(
            0,
            map,
            LogRecord {
                message: Value::str("msg"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn span_event(trace_id: [u8; 16]) -> Event {
        Event::span(
            0,
            AttrMap::new(),
            SpanRecord {
                trace_id,
                span_id: [1; 8],
                parent_span_id: None,
                name: Value::str("op"),
                kind: SpanKind::Internal,
                status: SpanStatus::Unset,
                events: vec![],
                links: vec![],
                end_timestamp: 0,
                flags: 0,
                ext: None,
            },
        )
    }

    fn log_with_trace(trace_id: [u8; 16]) -> Event {
        let mut event = log_event(&[]);
        event.log.as_mut().unwrap().trace = Some(TraceRef { trace_id, span_id: None, flags: 0 });
        event
    }

    fn resource(attrs: &[(&str, Value)]) -> Arc<Resource> {
        let mut map = AttrMap::new();
        for (k, v) in attrs {
            map.insert(k, v.clone());
        }
        Arc::new(Resource { attributes: map, ..Default::default() })
    }

    fn no_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn id(n: u64) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&n.to_be_bytes());
        id[8..].copy_from_slice(&n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
        id
    }

    fn hex(id: &[u8; 16]) -> String {
        id.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn keyed(rate: f64, key: SampleKey) -> Sample {
        Sample::new(rate, Some(key), SampleMissing::Random, None).with_seed(7)
    }

    fn flag(field: SampleField, value: Option<Value>) -> Option<SampleOverride> {
        Some(SampleOverride { field, value })
    }

    #[test]
    fn rate_zero_with_an_override_keeps_only_flagged_events() {
        let mut s = Sample::new(
            0.0,
            None,
            SampleMissing::Random,
            flag(SampleField::Attribute("sampling.keep".into()), None),
        );
        let r = no_resource();
        for n in 0..200 {
            assert!(!s.process(&r, &mut log_event(&[("n", Value::I64(n))])));
            assert!(s.process(&r, &mut log_event(&[("sampling.keep", Value::Bool(true))])));
        }
    }

    #[test]
    fn rate_one_keeps_everything() {
        // Rule 61 rejects `rate: 1` in config; the transform itself still keeps everything.
        let mut s = keyed(1.0, SampleKey::Attribute("k".into()));
        let r = no_resource();
        for n in 0..200 {
            assert!(s.process(&r, &mut log_event(&[("k", Value::I64(n))])));
        }
    }

    #[test]
    fn the_same_key_gets_the_same_verdict_every_time_and_in_every_instance() {
        let r = no_resource();
        for n in 0..500u64 {
            let mut a = keyed(0.3, SampleKey::TraceId);
            let mut b = keyed(0.3, SampleKey::TraceId).with_seed(99);
            let first = a.process(&r, &mut span_event(id(n)));
            for _ in 0..3 {
                assert_eq!(a.process(&r, &mut span_event(id(n))), first);
                assert_eq!(b.process(&r, &mut span_event(id(n))), first);
            }
        }
    }

    #[test]
    fn a_hashed_key_keeps_roughly_the_configured_fraction() {
        let mut s = keyed(0.25, SampleKey::Attribute("request_id".into()));
        let r = no_resource();
        let kept = (0..10_000)
            .filter(|n| s.process(&r, &mut log_event(&[("request_id", Value::str(n.to_string()))])))
            .count();
        let fraction = kept as f64 / 10_000.0;
        assert!((fraction - 0.25).abs() <= 0.03, "kept {fraction}");
    }

    #[test]
    fn a_lifted_trace_id_agrees_with_its_unlifted_hex_attribute() {
        let r = no_resource();
        let mut by_trace = keyed(0.5, SampleKey::TraceId);
        let mut by_attr = keyed(0.5, SampleKey::Attribute("trace_id".into()));
        let (mut kept, mut dropped) = (0, 0);
        for n in 0..300 {
            let verdict = by_trace.process(&r, &mut span_event(id(n)));
            let attr = log_event(&[("trace_id", Value::str(hex(&id(n))))]);
            assert_eq!(by_attr.process(&r, &mut attr.clone()), verdict, "trace {n}");
            if verdict {
                kept += 1;
            } else {
                dropped += 1;
            }
        }
        assert!(kept > 0 && dropped > 0, "a meaningful comparison needs both verdicts");
    }

    #[test]
    fn trace_id_falls_back_to_the_log_records_trace_reference() {
        let r = no_resource();
        let mut s = keyed(0.5, SampleKey::TraceId);
        for n in 0..300 {
            assert_eq!(
                s.process(&r, &mut log_with_trace(id(n))),
                s.process(&r, &mut span_event(id(n))),
                "trace {n}"
            );
        }
    }

    #[test]
    fn numeric_keys_agree_across_decoded_types() {
        let r = no_resource();
        let mut s = keyed(0.5, SampleKey::Attribute("k".into()));
        for n in 0..300i64 {
            let expected = s.process(&r, &mut log_event(&[("k", Value::str(n.to_string()))]));
            assert_eq!(s.process(&r, &mut log_event(&[("k", Value::I64(n))])), expected);
            assert_eq!(s.process(&r, &mut log_event(&[("k", Value::U64(n as u64))])), expected);
            assert_eq!(s.process(&r, &mut log_event(&[("k", Value::F64(n as f64))])), expected);
        }
    }

    #[test]
    fn missing_keep_forwards_and_missing_drop_drops() {
        let r = no_resource();
        let mut keep = Sample::new(0.01, Some(SampleKey::TraceId), SampleMissing::Keep, None);
        let mut drop = Sample::new(0.99, Some(SampleKey::TraceId), SampleMissing::Drop, None);
        for _ in 0..100 {
            assert!(keep.process(&r, &mut log_event(&[])));
            assert!(!drop.process(&r, &mut log_event(&[])));
        }
        // A `Null`/`Array`/`Map` value counts as missing too.
        let mut drop =
            Sample::new(0.99, Some(SampleKey::Attribute("k".into())), SampleMissing::Drop, None);
        assert!(!drop.process(&r, &mut log_event(&[("k", Value::Null)])));
        assert!(!drop.process(&r, &mut log_event(&[("k", Value::Array(vec![]))])));
    }

    #[test]
    fn missing_random_draws_near_the_rate() {
        let r = no_resource();
        let mut s =
            Sample::new(0.5, Some(SampleKey::TraceId), SampleMissing::Random, None).with_seed(1234);
        let kept = (0..10_000).filter(|_| s.process(&r, &mut log_event(&[]))).count();
        assert!((kept as f64 / 10_000.0 - 0.5).abs() <= 0.03, "kept {kept}");
    }

    #[test]
    fn override_without_a_value_matches_any_value() {
        let r = no_resource();
        let mut s = Sample::new(
            0.0,
            None,
            SampleMissing::Random,
            flag(SampleField::Attribute("debug".into()), None),
        );
        assert!(s.process(&r, &mut log_event(&[("debug", Value::str("anything"))])));
        assert!(s.process(&r, &mut log_event(&[("debug", Value::I64(0))])));
        assert!(!s.process(&r, &mut log_event(&[("other", Value::Bool(true))])));
    }

    #[test]
    fn override_with_a_value_matches_only_that_value() {
        let r = no_resource();
        let mut s = Sample::new(
            0.0,
            None,
            SampleMissing::Random,
            flag(SampleField::Attribute("sampling.keep".into()), Some(Value::Bool(true))),
        );
        assert!(s.process(&r, &mut log_event(&[("sampling.keep", Value::Bool(true))])));
        assert!(!s.process(&r, &mut log_event(&[("sampling.keep", Value::Bool(false))])));
        // `value_matches` never coerces a `Bool` -- the documented logfmt caveat.
        assert!(!s.process(&r, &mut log_event(&[("sampling.keep", Value::str("true"))])));
    }

    #[test]
    fn override_on_the_resource() {
        let mut s = Sample::new(
            0.0,
            None,
            SampleMissing::Random,
            flag(SampleField::Resource("env".into()), Some(Value::str("staging"))),
        );
        let staging = resource(&[("env", Value::str("staging"))]);
        let prod = resource(&[("env", Value::str("prod"))]);
        assert!(s.process(&staging, &mut log_event(&[])));
        assert!(!s.process(&prod, &mut log_event(&[])));
        assert!(!s.process(&no_resource(), &mut log_event(&[])));
    }

    #[test]
    fn a_resource_key_keeps_or_drops_every_event_of_a_resource_together() {
        let mut s = keyed(0.5, SampleKey::Resource("service.name".into()));
        let (mut kept, mut dropped) = (0, 0);
        for n in 0..100 {
            let r = resource(&[("service.name", Value::str(format!("svc-{n}")))]);
            let first = s.process(&r, &mut log_event(&[]));
            for m in 0..20 {
                assert_eq!(s.process(&r, &mut log_event(&[("m", Value::I64(m))])), first);
            }
            if first {
                kept += 1;
            } else {
                dropped += 1;
            }
        }
        assert!(kept > 0 && dropped > 0);
    }

    #[test]
    fn a_seeded_keyless_sampler_is_reproducible_and_near_the_rate() {
        let r = no_resource();
        let run = |seed| {
            let mut s = Sample::new(0.3, None, SampleMissing::Random, None).with_seed(seed);
            (0..10_000).map(|_| s.process(&r, &mut log_event(&[]))).collect::<Vec<_>>()
        };
        let a = run(42);
        assert_eq!(a, run(42));
        assert_ne!(a, run(43));
        let kept = a.iter().filter(|k| **k).count();
        assert!((kept as f64 / 10_000.0 - 0.3).abs() <= 0.03, "kept {kept}");
    }

    #[test]
    fn a_kept_event_is_never_mutated() {
        let r = no_resource();
        let mut s = Sample::new(
            0.5,
            Some(SampleKey::Attribute("k".into())),
            SampleMissing::Keep,
            flag(SampleField::Attribute("f".into()), None),
        );
        for event in [
            log_event(&[("k", Value::I64(1)), ("f", Value::Bool(true))]),
            log_event(&[("k", Value::str("x"))]),
            log_event(&[]),
        ] {
            let mut processed = event.clone();
            s.process(&r, &mut processed);
            assert_eq!(processed, event);
        }
    }

    fn decisions(events: &[Event], outcome: &str, by: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            let tag = |k| e.attributes.get(k).and_then(|v| v.as_str());
            if tag("outcome") != Some(outcome) || tag("by") != Some(by) {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if resolve(m.name) == "logit.transform.sample.decisions" => {
                    Some(sum.value)
                }
                _ => None,
            })
        })
    }

    fn filtered(events: &[Event]) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if resolve(m.name) == "logit.transform.events.filtered" => {
                    Some(sum.value)
                }
                _ => None,
            })
        })
    }

    #[test]
    fn decisions_are_counted_once_per_batch_by_outcome_and_reason() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("sampled", "sample", "transform");
        let mut s = Sample::new(
            0.0,
            Some(SampleKey::Attribute("k".into())),
            SampleMissing::Keep,
            flag(SampleField::Attribute("f".into()), None),
        )
        .with_telemetry(telemetry);
        let r = no_resource();
        assert!(s.process(&r, &mut log_event(&[("f", Value::Bool(true))])));
        assert!(!s.process(&r, &mut log_event(&[("k", Value::I64(1))])));
        assert!(!s.process(&r, &mut log_event(&[("k", Value::I64(2))])));
        assert!(s.process(&r, &mut log_event(&[])));
        // Nothing is emitted per event.
        assert!(registry.drain(0).iter().all(|e| e.metrics.is_empty()));

        s.end_batch();
        let events = registry.drain(0);
        assert_eq!(filtered(&events), Some(2.0));
        assert_eq!(decisions(&events, "kept", "override"), Some(1.0));
        assert_eq!(decisions(&events, "dropped", "key"), Some(2.0));
        assert_eq!(decisions(&events, "kept", "missing"), Some(1.0));
        assert_eq!(decisions(&events, "kept", "key"), None, "zero cells are not emitted");
    }

    #[test]
    fn filtered_is_emitted_at_zero_for_a_batch_that_dropped_nothing() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("sampled", "sample", "transform");
        let mut s = Sample::new(0.5, Some(SampleKey::TraceId), SampleMissing::Keep, None)
            .with_telemetry(telemetry);
        assert!(s.process(&no_resource(), &mut log_event(&[])));
        s.end_batch();
        let events = registry.drain(0);
        assert_eq!(filtered(&events), Some(0.0));
        assert_eq!(decisions(&events, "kept", "missing"), Some(1.0));
    }

    #[test]
    fn an_empty_batch_emits_nothing() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("sampled", "sample", "transform");
        let mut s = Sample::new(0.5, None, SampleMissing::Random, None).with_telemetry(telemetry);
        s.end_batch();
        assert_eq!(filtered(&registry.drain(0)), None);
    }
}
