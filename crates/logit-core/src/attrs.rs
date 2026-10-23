//! `AttrMap`: the small, sorted, interned-key map behind event attributes, plus [`merged`], the
//! resource-then-event merge-join shared by every codec that renders both onto one wire form, and
//! [`is_private`], the test for the private `_logit.` namespace.
//!
//! Most events carry under a dozen attributes, where a sorted `SmallVec` beats a `HashMap` on
//! lookup and iteration, and its deterministic order is what the wire format's dictionary encoding
//! and reproducible tests depend on. See `docs/design/data-model.md`.

use crate::interner::{intern, lookup, Symbol};
use crate::value::Value;
use crate::{Event, MetricKind, Resource};
use smallvec::SmallVec;
use std::cmp::Ordering;

const INLINE_CAPACITY: usize = 8;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AttrMap(SmallVec<[(Symbol, Value); INLINE_CAPACITY]>);

impl AttrMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        // `lookup`, not `intern`: a never-interned key can't be here, and a miss mustn't grow
        // the interner (`docs/design/memory.md` §4).
        let key = lookup(key)?;
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| &self.0[i].1)
    }

    /// [`AttrMap::get`] by an already-interned [`Symbol`], skipping the interner probe: for a
    /// component that interns its configured keys once at construction.
    pub fn get_sym(&self, key: Symbol) -> Option<&Value> {
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| &self.0[i].1)
    }

    pub fn insert(&mut self, key: &str, value: impl Into<Value>) {
        let key = intern(key);
        self.insert_sym(key, value);
    }

    /// [`AttrMap::insert`] by an already-interned [`Symbol`], skipping the interner probe.
    /// Overwrites an existing key.
    pub fn insert_sym(&mut self, key: Symbol, value: impl Into<Value>) {
        match self.0.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(i) => self.0[i].1 = value.into(),
            Err(i) => self.0.insert(i, (key, value.into())),
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        // As in `get`: a never-interned key was never inserted.
        let key = lookup(key)?;
        self.remove_sym(key)
    }

    /// [`AttrMap::remove`] by an already-held [`Symbol`], skipping the interner probe.
    pub fn remove_sym(&mut self, key: Symbol) -> Option<Value> {
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| self.0.remove(i).1)
    }

    /// Empties the map but keeps its backing storage, so refilling a spilled map per event
    /// doesn't allocate.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates in `Symbol` order (interning order, not alphabetical or insertion order).
    pub fn iter(&self) -> impl Iterator<Item = (Symbol, &Value)> {
        self.0.iter().map(|(k, v)| (*k, v))
    }

    /// The first entry whose key sorts after `after` in [`AttrMap::iter`]'s order, or the first
    /// entry for `None`. `after` needn't be present, so a caller stepping through the map by its
    /// last key keeps its place when that key is removed between steps.
    pub fn entry_after(&self, after: Option<Symbol>) -> Option<(Symbol, &Value)> {
        let i = match after {
            None => 0,
            Some(after) => self.0.partition_point(|(k, _)| *k <= after),
        };
        self.0.get(i).map(|(k, v)| (*k, v))
    }

    /// The key that sorts last in [`AttrMap::iter`]'s order, or `None` for an empty map.
    pub fn last_key(&self) -> Option<Symbol> {
        self.0.last().map(|(k, _)| *k)
    }

    /// Consumes the map, yielding owned pairs in [`AttrMap::iter`]'s order, so values move out
    /// rather than clone. A method rather than `IntoIterator`, which would expose the backing
    /// `SmallVec` as `IntoIter`.
    pub fn into_pairs(self) -> impl Iterator<Item = (Symbol, Value)> {
        self.0.into_iter()
    }
}

impl FromIterator<(&'static str, Value)> for AttrMap {
    fn from_iter<T: IntoIterator<Item = (&'static str, Value)>>(iter: T) -> Self {
        let mut map = AttrMap::new();
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}

/// Iterates `resource.attributes` merged with `event.attributes` in [`Symbol`] order, the event's
/// value winning on an equal key: the same sequence as clone-and-insert, without copying a map
/// per event.
///
/// Lives here because sinks in `logit-outputs` and the Prometheus codec in `logit-proto` both need
/// it, and `logit-proto` can't depend on `logit-outputs`.
pub fn merged<'a>(
    resource: &'a Resource,
    event: &'a Event,
) -> impl Iterator<Item = (Symbol, &'a Value)> {
    let mut resource_attrs = resource.attributes.iter().peekable();
    let mut event_attrs = event.attributes.iter().peekable();
    std::iter::from_fn(move || {
        match (resource_attrs.peek().map(|(k, _)| *k), event_attrs.peek().map(|(k, _)| *k)) {
            (Some(r), Some(e)) => match r.cmp(&e) {
                Ordering::Less => resource_attrs.next(),
                Ordering::Greater => event_attrs.next(),
                // Same key on both: the event's value wins, and the resource's is discarded.
                Ordering::Equal => {
                    resource_attrs.next();
                    event_attrs.next()
                }
            },
            (Some(_), None) => resource_attrs.next(),
            (None, Some(_)) => event_attrs.next(),
            (None, None) => None,
        }
    })
}

/// The prefix of the private attribute namespace, which protocol codecs reserve for their own use.
///
/// See ADR `private-attribute-namespace` (`docs/adr/private-attribute-namespace.md`) and
/// `docs/design/data-model.md`'s "Well-known attribute names".
pub const PRIVATE_PREFIX: &str = "_logit.";

/// Whether `key` is in the private namespace, [`PRIVATE_PREFIX`].
///
/// ADR `private-attribute-namespace` has the rule sinks follow for these keys. Don't filter inside
/// [`merged`]: under that rule, a sink reads its own private keys through it.
pub fn is_private(key: &str) -> bool {
    key.starts_with(PRIVATE_PREFIX)
}

/// The prefix of the field namespace: an attribute `_logit.field.<key>` is data about its point,
/// never part of the identity of the series a point belongs to.
///
/// `influxdb_out` writes a `Str` under it as the string field `<key>`, and `aggregate` leaves it
/// out of a series' identity on an event flagged [`METRICS_FIELDS`]. It is a private key ([`is_private`]), so every other protocol sink
/// ignores it. See ADR `metric-flags` (`docs/adr/metric-flags.md`) and
/// `docs/design/data-model.md`'s "Well-known attribute names".
pub const FIELD_PREFIX: &str = "_logit.field.";

/// Whether `key` is in the field namespace, [`FIELD_PREFIX`].
pub fn is_field(key: &str) -> bool {
    key.starts_with(FIELD_PREFIX)
}

/// The flag that says every metric record on its event summarizes a time span its source closed,
/// so `aggregate` forwards the event whole instead of windowing it. Its value is one of
/// [`CLOSED_BUCKETED`] or [`CLOSED_TIMESTAMPED`]; any other `Str`, or a `Bool(true)`, means the
/// same and counts as [`CLOSED_FLAGGED`].
///
/// Read off the event, else the batch resource. See ADR `metric-flags`
/// (`docs/adr/metric-flags.md`), "The flags".
pub const METRICS_CLOSED: &str = "_logit.metrics.closed";

/// The flag that says its event carries attributes under [`FIELD_PREFIX`]; a reader scans for them
/// only when it is `Bool(true)`. Read off the event, else the batch resource. See ADR
/// `metric-flags` (`docs/adr/metric-flags.md`), "The flags".
pub const METRICS_FIELDS: &str = "_logit.metrics.fields";

/// [`METRICS_CLOSED`]'s value for an event whose records each summarize a time bucket the source
/// closed, such as a Datadog APM stats group. See ADR `metric-flags`, "The flags".
pub const CLOSED_BUCKETED: &str = "bucketed";

/// [`METRICS_CLOSED`]'s value for an event whose timestamp is its source's own, such as a
/// DogStatsD `|T` line. See ADR `metric-flags`, "The flags".
pub const CLOSED_TIMESTAMPED: &str = "timestamped";

/// The reason a reader counts for a [`METRICS_CLOSED`] value outside the closed set, so free text
/// never becomes a telemetry tag. Not a value a producer writes. See ADR `metric-flags`, "The
/// flags".
pub const CLOSED_FLAGGED: &str = "flagged";

/// Whether `event` is flagged [`METRICS_CLOSED`] with the value [`CLOSED_TIMESTAMPED`], read off
/// the event, else the batch resource: its timestamp is its source's own, so a sink that can write
/// a point's time (`statsd_out`'s `|T`, `prometheus_out`'s exposition timestamp) writes
/// `Event::timestamp`. [`CLOSED_BUCKETED`], any other `Str`, and `Bool(true)` don't count: each
/// closes a span for `aggregate`, but none says the timestamp is the source's. An event value
/// shadows the resource's, as [`merged`] does.
pub fn is_closed_timestamped(resource: &Resource, event: &Event) -> bool {
    let value =
        event.attributes.get(METRICS_CLOSED).or_else(|| resource.attributes.get(METRICS_CLOSED));
    value.and_then(Value::as_str) == Some(CLOSED_TIMESTAMPED)
}

/// The flag that names what kind of reading its event's records are, where the model kind alone
/// doesn't say. Its values are [`TYPE_RATE`], [`TYPE_INFO`], [`TYPE_STATESET`], and
/// [`TYPE_GAUGE_HISTOGRAM`], each for one model kind; a reader ignores a value it has no rule for,
/// and a value on a kind it doesn't name. Read off the event, else the batch resource, through
/// [`metric_type`]. See ADR `metric-flags` (`docs/adr/metric-flags.md`), "The flags".
pub const METRICS_TYPE: &str = "_logit.metrics.type";

/// [`METRICS_TYPE`]'s value for an event whose `Gauge` records each hold a per-second rate over
/// [`METRICS_INTERVAL`] seconds, such as a Datadog `rate` series point. A rate is an increment
/// over its interval, not a state. See ADR `metric-flags`, "The flags".
pub const TYPE_RATE: &str = "rate";

/// [`METRICS_TYPE`]'s value for an event whose `Gauge` records are each an info metric: a `1`
/// whose labels are the payload, such as an OpenMetrics `info` family. See ADR `metric-flags`,
/// "The flags".
pub const TYPE_INFO: &str = "info";

/// [`METRICS_TYPE`]'s value for an event whose `Gauge` records are each one state of a state set,
/// `1` when the state holds and `0` when it doesn't, such as an OpenMetrics `stateset` family. See
/// ADR `metric-flags`, "The flags".
pub const TYPE_STATESET: &str = "stateset";

/// [`METRICS_TYPE`]'s value for an event whose `Histogram` records each count a quantity that can
/// go down, so no bucket is a running count, such as an OpenMetrics `gaugehistogram` family. See
/// ADR `metric-flags`, "The flags".
pub const TYPE_GAUGE_HISTOGRAM: &str = "gaugehistogram";

/// The flag that holds the span, in whole seconds (`I64`), its event's readings cover: a rate's
/// divisor, a delta count's window. Written only when nonzero. Read off the event, else the batch
/// resource. See ADR `metric-flags` (`docs/adr/metric-flags.md`), "The flags".
pub const METRICS_INTERVAL: &str = "_logit.metrics.interval";

/// [`METRICS_TYPE`]'s value as a `Str`, read off the event, else the batch resource. An event
/// value shadows the resource's, as [`merged`] does. The caller applies the value's kind rule.
pub fn metric_type<'a>(resource: &'a Resource, event: &'a Event) -> Option<&'a str> {
    event
        .attributes
        .get(METRICS_TYPE)
        .or_else(|| resource.attributes.get(METRICS_TYPE))
        .and_then(Value::as_str)
}

/// Whether `kind` on `event` is a rate: a `Gauge` whose event is flagged [`METRICS_TYPE`]
/// [`TYPE_RATE`], read off the event, else the batch resource. A stage that collapses readings of
/// one series at one instant sums a rate as an increment rather than keeping the later one. The
/// rate value applies to `Gauge` records only, so a rate flag on any other kind is ignored.
pub fn is_rate(resource: &Resource, event: &Event, kind: &MetricKind) -> bool {
    matches!(kind, MetricKind::Gauge(_)) && metric_type(resource, event) == Some(TYPE_RATE)
}

/// [`METRICS_INTERVAL`]'s value, read off the event, else the batch resource.
pub fn interval<'a>(resource: &'a Resource, event: &'a Event) -> Option<&'a Value> {
    event.attributes.get(METRICS_INTERVAL).or_else(|| resource.attributes.get(METRICS_INTERVAL))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interner;

    #[test]
    fn is_closed_timestamped_reads_the_event_else_the_resource_and_only_timestamped() {
        let flagged = |value: Value| {
            let mut attributes = AttrMap::new();
            attributes.insert(METRICS_CLOSED, value);
            Event::empty(0, attributes)
        };
        let bare = Resource::default();
        assert!(is_closed_timestamped(&bare, &flagged(Value::from(CLOSED_TIMESTAMPED))));
        for other in [Value::from(CLOSED_BUCKETED), Value::from("other"), Value::Bool(true)] {
            assert!(!is_closed_timestamped(&bare, &flagged(other.clone())), "{other:?}");
        }
        assert!(!is_closed_timestamped(&bare, &Event::empty(0, AttrMap::new())));

        let mut stamped = Resource::default();
        stamped.attributes.insert(METRICS_CLOSED, CLOSED_TIMESTAMPED);
        assert!(
            is_closed_timestamped(&stamped, &Event::empty(0, AttrMap::new())),
            "the resource's flag"
        );
        assert!(
            !is_closed_timestamped(&stamped, &flagged(Value::from(CLOSED_BUCKETED))),
            "an event value shadows the resource's"
        );
    }

    #[test]
    fn is_rate_reads_a_gauge_flagged_rate_on_the_event_else_the_resource() {
        let gauge = MetricKind::gauge(1.0);
        let flagged = |value: Value| {
            let mut attributes = AttrMap::new();
            attributes.insert(METRICS_TYPE, value);
            attributes.insert(METRICS_INTERVAL, Value::I64(10));
            Event::empty(0, attributes)
        };
        let bare = Resource::default();
        let rate = flagged(Value::from(TYPE_RATE));
        assert!(is_rate(&bare, &rate, &gauge));
        assert_eq!(interval(&bare, &rate), Some(&Value::I64(10)));
        assert!(!is_rate(&bare, &rate, &MetricKind::counter(1.0)), "a rate flag on a Sum");
        assert!(!is_rate(&bare, &flagged(Value::from("other")), &gauge), "an unknown type");
        assert!(!is_rate(&bare, &Event::empty(0, AttrMap::new()), &gauge));

        let mut stamped = Resource::default();
        stamped.attributes.insert(METRICS_TYPE, TYPE_RATE);
        stamped.attributes.insert(METRICS_INTERVAL, Value::I64(20));
        let bare_event = Event::empty(0, AttrMap::new());
        assert!(is_rate(&stamped, &bare_event, &gauge), "the resource's flag");
        assert_eq!(interval(&stamped, &bare_event), Some(&Value::I64(20)));
        assert!(
            !is_rate(&stamped, &flagged(Value::from("other")), &gauge),
            "an event value shadows"
        );
        assert_eq!(interval(&stamped, &rate), Some(&Value::I64(10)), "an event value shadows");
    }

    #[test]
    fn metric_type_reads_the_event_else_the_resource() {
        let flagged = |value: Value| {
            let mut attributes = AttrMap::new();
            attributes.insert(METRICS_TYPE, value);
            Event::empty(0, attributes)
        };
        let bare = Resource::default();
        assert_eq!(metric_type(&bare, &flagged(Value::from(TYPE_INFO))), Some(TYPE_INFO));
        assert_eq!(metric_type(&bare, &flagged(Value::Bool(true))), None, "not a Str");
        assert_eq!(metric_type(&bare, &Event::empty(0, AttrMap::new())), None);

        let mut stamped = Resource::default();
        stamped.attributes.insert(METRICS_TYPE, TYPE_STATESET);
        let bare_event = Event::empty(0, AttrMap::new());
        assert_eq!(metric_type(&stamped, &bare_event), Some(TYPE_STATESET), "the resource's");
        assert_eq!(
            metric_type(&stamped, &flagged(Value::from(TYPE_GAUGE_HISTOGRAM))),
            Some(TYPE_GAUGE_HISTOGRAM),
            "an event value shadows the resource's"
        );
    }

    #[test]
    fn is_field_matches_only_keys_under_the_field_prefix() {
        assert!(is_field("_logit.field.note"));
        assert!(is_field("_logit.field."));
        assert!(!is_field("_logit.field"));
        assert!(!is_field("_logit.influxdb.measurement"));
        assert!(!is_field("field.note"));
        assert!(is_private("_logit.field.note"));
    }

    #[test]
    fn get_present_key_returns_the_value() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        assert_eq!(map.get("host"), Some(&Value::from("web-1")));
    }

    #[test]
    fn get_absent_key_returns_none() {
        let map = AttrMap::new();
        assert_eq!(map.get("does-not-exist"), None);
    }

    #[test]
    fn remove_present_key_returns_the_value_and_removes_it() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        assert_eq!(map.remove("host"), Some(Value::from("web-1")));
        assert_eq!(map.get("host"), None);
    }

    #[test]
    fn remove_absent_key_returns_none() {
        let mut map = AttrMap::new();
        assert_eq!(map.remove("does-not-exist"), None);
    }

    /// A missed `get`/`remove` doesn't intern the key (nextest isolates `interner::len()`).
    #[test]
    fn getting_an_absent_key_does_not_grow_the_interner() {
        let map = AttrMap::new();
        let never_interned_elsewhere = "attrmap_absent_key_probe_xyzzy";

        let before = interner::len();
        assert_eq!(map.get(never_interned_elsewhere), None);
        assert_eq!(interner::len(), before, "a missed `get` must not intern the key");

        let mut map = map;
        assert_eq!(map.remove(never_interned_elsewhere), None);
        assert_eq!(interner::len(), before, "a missed `remove` must not intern the key");
    }

    #[test]
    fn into_pairs_yields_every_entry_in_sorted_order() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        map.insert("env", "prod");
        map.insert("retries", 3_i64);

        let expected: Vec<(Symbol, Value)> = map.iter().map(|(k, v)| (k, v.clone())).collect();
        let actual: Vec<(Symbol, Value)> = map.into_pairs().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn stepping_by_entry_after_yields_iter_order() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        map.insert("env", "prod");
        map.insert("retries", 3_i64);

        let mut stepped = Vec::new();
        let mut last = None;
        while let Some((k, v)) = map.entry_after(last) {
            stepped.push((k, v.clone()));
            last = Some(k);
        }
        let expected: Vec<(Symbol, Value)> = map.iter().map(|(k, v)| (k, v.clone())).collect();
        assert_eq!(stepped, expected);
        assert_eq!(AttrMap::new().entry_after(None), None);
    }

    /// `last_key` is the greatest key, and `interner::index` orders keys as `Symbol` does.
    #[test]
    fn last_key_is_the_greatest_and_index_follows_symbol_order() {
        let mut map = AttrMap::new();
        assert_eq!(map.last_key(), None);
        map.insert("host", "web-1");
        map.insert("env", "prod");
        map.insert("retries", 3_i64);
        let keys: Vec<Symbol> = map.iter().map(|(k, _)| k).collect();
        assert_eq!(map.last_key(), keys.last().copied());
        for pair in keys.windows(2) {
            assert!(crate::interner::index(pair[0]) < crate::interner::index(pair[1]));
        }
    }

    #[test]
    fn entry_after_a_removed_key_seeks_the_next_one() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        map.insert("env", "prod");
        map.insert("retries", 3_i64);
        let keys: Vec<Symbol> = map.iter().map(|(k, _)| k).collect();

        map.remove_sym(keys[1]);
        assert_eq!(map.entry_after(Some(keys[1])).map(|(k, _)| k), Some(keys[2]));
        map.remove_sym(keys[2]);
        assert_eq!(map.entry_after(Some(keys[2])), None);
        assert_eq!(map.entry_after(Some(keys[0])), None);
    }

    #[test]
    fn into_pairs_on_an_empty_map_yields_nothing() {
        let map = AttrMap::new();
        assert_eq!(map.into_pairs().count(), 0);
    }

    #[test]
    fn get_sym_present_key_returns_the_value() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        let sym = intern("host");
        assert_eq!(map.get_sym(sym), Some(&Value::from("web-1")));
    }

    #[test]
    fn get_sym_absent_key_returns_none() {
        let map = AttrMap::new();
        let sym = intern("attrmap_get_sym_absent_probe");
        assert_eq!(map.get_sym(sym), None);
    }

    #[test]
    fn get_sym_agrees_with_get_for_every_key() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        map.insert("env", "prod");
        map.insert("retries", 3_i64);

        for key in ["host", "env", "retries", "does-not-exist"] {
            let sym = intern(key);
            assert_eq!(map.get_sym(sym), map.get(key), "mismatch for key {key:?}");
        }
    }

    /// `get_sym` itself never grows the interner.
    #[test]
    fn get_sym_never_touches_the_interner() {
        let map = AttrMap::new();
        let sym = intern("attrmap_get_sym_no_growth_probe_xyzzy");

        let before = interner::len();
        assert_eq!(map.get_sym(sym), None);
        assert_eq!(interner::len(), before, "`get_sym` must not intern anything");
    }

    fn resource_with(attrs: &[(&str, &str)]) -> Resource {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, *v);
        }
        Resource { attributes, ..Default::default() }
    }

    fn event_with(attrs: &[(&str, &str)]) -> Event {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, *v);
        }
        Event::empty(0, attributes)
    }

    fn collect(resource: &Resource, event: &Event) -> Vec<(String, String)> {
        merged(resource, event)
            .map(|(k, v)| (interner::resolve(k).to_string(), v.as_str().unwrap().to_string()))
            .collect()
    }

    #[test]
    fn an_event_attribute_overrides_a_resource_attribute_of_the_same_name() {
        let resource = resource_with(&[("env", "staging")]);
        let event = event_with(&[("env", "prod")]);
        assert_eq!(collect(&resource, &event), vec![("env".to_string(), "prod".to_string())]);
    }

    #[test]
    fn a_resource_attribute_with_no_event_counterpart_still_appears() {
        let resource = resource_with(&[("region", "us-east")]);
        let event = event_with(&[]);
        assert_eq!(collect(&resource, &event), vec![("region".to_string(), "us-east".to_string())]);
    }

    /// The merge-join matches a single combined map's order (interning order, not alphabetical).
    #[test]
    fn merged_order_matches_a_single_combined_attrmap() {
        let resource = resource_with(&[("zzz", "resource-only"), ("shared", "from-resource")]);
        let event = event_with(&[("aaa", "event-only"), ("shared", "from-event")]);

        let mut combined = AttrMap::new();
        combined.insert("zzz", "resource-only");
        combined.insert("shared", "from-event"); // event wins
        combined.insert("aaa", "event-only");

        let expected: Vec<(String, String)> = combined
            .iter()
            .map(|(k, v)| (interner::resolve(k).to_string(), v.as_str().unwrap().to_string()))
            .collect();
        assert_eq!(collect(&resource, &event), expected);
    }

    #[test]
    fn a_key_under_the_private_prefix_is_private() {
        assert!(is_private("_logit.x"));
        assert!(is_private("_logit.statsd.type"));
    }

    #[test]
    fn the_bare_prefix_without_its_dot_is_not_private() {
        assert!(!is_private("_logit"));
    }

    #[test]
    fn a_public_logit_key_is_not_private() {
        assert!(!is_private("logit.x"));
        assert!(!is_private("logit.body_format"));
    }

    #[test]
    fn the_prefix_matches_only_at_the_start_of_a_key() {
        assert!(!is_private("x._logit.y"));
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;
        use std::collections::BTreeMap;

        /// Unique to this test, and interned in reverse name order by the first case, so `Symbol`
        /// order is the reverse of name order.
        const KEYS: [&str; 6] = [
            "attrs_prop_a",
            "attrs_prop_b",
            "attrs_prop_c",
            "attrs_prop_d",
            "attrs_prop_e",
            "attrs_prop_f",
        ];

        #[derive(Debug, Clone)]
        enum Op {
            Insert(usize, i64),
            InsertSym(usize, i64),
            Remove(usize),
            RemoveSym(usize),
            Clear,
        }

        fn op() -> impl Strategy<Value = Op> {
            let key = 0..KEYS.len();
            prop_oneof![
                4 => (key.clone(), any::<i64>()).prop_map(|(k, v)| Op::Insert(k, v)),
                4 => (key.clone(), any::<i64>()).prop_map(|(k, v)| Op::InsertSym(k, v)),
                2 => key.clone().prop_map(Op::Remove),
                2 => key.prop_map(Op::RemoveSym),
                1 => Just(Op::Clear),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Every mutation keeps `iter()` strictly ascending by `Symbol` and agrees with a
            /// `BTreeMap` keyed by `Symbol` on `len`, `iter`, `get`, `get_sym`, and what `remove`
            /// returns. `aggregate`'s `SeriesKey` relies on that order for a zipped walk.
            #[test]
            fn random_edits_keep_symbol_order_and_match_a_btreemap(
                ops in prop::collection::vec(op(), 0..64),
            ) {
                let mut symbols: Vec<Symbol> = KEYS.iter().rev().map(|k| intern(k)).collect();
                symbols.reverse();
                let mut map = AttrMap::new();
                let mut model: BTreeMap<Symbol, i64> = BTreeMap::new();

                for op in ops {
                    match op {
                        Op::Insert(k, v) => {
                            map.insert(KEYS[k], v);
                            model.insert(symbols[k], v);
                        }
                        Op::InsertSym(k, v) => {
                            map.insert_sym(symbols[k], v);
                            model.insert(symbols[k], v);
                        }
                        Op::Remove(k) => {
                            let expected = model.remove(&symbols[k]).map(Value::I64);
                            prop_assert_eq!(map.remove(KEYS[k]), expected);
                        }
                        Op::RemoveSym(k) => {
                            let expected = model.remove(&symbols[k]).map(Value::I64);
                            prop_assert_eq!(map.remove_sym(symbols[k]), expected);
                        }
                        Op::Clear => {
                            map.clear();
                            model.clear();
                        }
                    }

                    let keys: Vec<Symbol> = map.iter().map(|(k, _)| k).collect();
                    prop_assert!(keys.windows(2).all(|w| w[0] < w[1]), "not sorted: {:?}", keys);
                    let actual: Vec<(Symbol, Value)> =
                        map.iter().map(|(k, v)| (k, v.clone())).collect();
                    let expected: Vec<(Symbol, Value)> =
                        model.iter().map(|(k, v)| (*k, Value::I64(*v))).collect();
                    prop_assert_eq!(actual, expected);
                    prop_assert_eq!(map.len(), model.len());
                    for (k, name) in KEYS.iter().enumerate() {
                        let expected = model.get(&symbols[k]).map(|v| Value::I64(*v));
                        prop_assert_eq!(map.get(name), expected.as_ref());
                        prop_assert_eq!(map.get_sym(symbols[k]), expected.as_ref());
                    }
                }
            }
        }
    }
}
