//! `AttrMap`: the small, sorted, interned-key map that backs event attributes, plus the
//! resource-attributes-overridden-by-event-attributes merge-join ([`merged`]) every codec and sink
//! that renders both onto one wire representation shares.
//!
//! Most events carry well under a dozen attributes, so a sorted `SmallVec` beats a `HashMap` on
//! both lookup and iteration at this size, and gives deterministic ordering for free -- which the
//! wire format's dictionary encoding and reproducible tests both depend on. See
//! `docs/design/data-model.md`.

use crate::interner::{intern, lookup, Symbol};
use crate::value::Value;
use crate::{Event, Resource};
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
        // `lookup`, not `intern`: a key that was never interned can't be in this map either
        // (interning is monotonic and global), so a miss here returns `None` without growing the
        // process-wide interner table for an attribute this event doesn't carry. See
        // `docs/design/memory.md` §4.
        let key = lookup(key)?;
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| &self.0[i].1)
    }

    /// Same as [`AttrMap::get`], but for a caller that already holds an interned [`Symbol`] --
    /// skips both the `lookup` hash probe and the `resolve` such a caller would otherwise need to
    /// reconstruct the `&str`. The [`AttrMap::insert_sym`] reasoning applied to the read side:
    /// a matcher that interns its configured keys once, at construction, then probes those same
    /// `Symbol`s on every event pays a plain `binary_search_by_key` instead of a hash + resolve
    /// round trip. The interner-growth guarantee `get` documents is trivially preserved here --
    /// the `Symbol` already exists, so there is nothing left to intern.
    pub fn get_sym(&self, key: Symbol) -> Option<&Value> {
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| &self.0[i].1)
    }

    pub fn insert(&mut self, key: &str, value: impl Into<Value>) {
        let key = intern(key);
        self.insert_sym(key, value);
    }

    /// Same as [`AttrMap::insert`], but for a caller that already holds an interned [`Symbol`] --
    /// skips the interner lookup `insert` would otherwise redo on every call. `logit-transforms`'
    /// `set` transform is the first caller: it interns its configured keys once, at construction,
    /// then inserts the same `Symbol`s into every event's/resource's map on the per-event hot
    /// path.
    pub fn insert_sym(&mut self, key: Symbol, value: impl Into<Value>) {
        match self.0.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(i) => self.0[i].1 = value.into(),
            Err(i) => self.0.insert(i, (key, value.into())),
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        // Same reasoning as `get`: a key never interned was never inserted, so it can't be present.
        let key = lookup(key)?;
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| self.0.remove(i).1)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates in sorted-symbol order -- stable and deterministic, not insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (Symbol, &Value)> {
        self.0.iter().map(|(k, v)| (*k, v))
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

/// Iterates `resource.attributes` merged with `event.attributes`, in sorted-[`Symbol`] order, the
/// event's value winning on an equal key. Both maps already iterate in that order
/// ([`AttrMap::iter`]), so walking them in lockstep and preferring the event's value on a tie
/// produces exactly the same sequence a clone-and-insert would -- without copying an `AttrMap` per
/// event, and without the `resolve` -> `intern` round trip re-inserting every key would cost.
///
/// Lives here rather than in one sink: `influxdb_out`'s tags, `statsd_out`'s tags
/// (`crates/logit-outputs/src/attrs.rs` re-exports this) and the Prometheus codec's labels
/// (`crates/logit-proto/src/prometheus/`) all need the identical merge with a different emission
/// format on top, and `logit-proto` cannot reach into `logit-outputs` (the dependency runs the other
/// way).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interner;

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

    /// Pins the fix this module exists for: `get`/`remove` on a key that was never interned must
    /// not intern it just to find out it's absent. `nextest` runs each test in its own process
    /// (see `docs/design/memory.md` §7), so `interner::len()` here reflects only this test.
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

    /// `get_sym` takes an already-interned `Symbol`, so there is nothing left for it to intern --
    /// interning the probe key happens explicitly, before the snapshot, so this only pins
    /// `get_sym` itself against growing the interner.
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

    /// [`AttrMap::iter`] orders by `Symbol` (interning order), not alphabetically -- so the property
    /// worth pinning is that the merge-join matches what a single combined map would produce, not
    /// any particular string ordering.
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
}
