//! `AttrMap`: the small, sorted, interned-key map behind event attributes, plus [`merged`], the
//! resource-then-event merge-join shared by every codec that renders both onto one wire form.
//!
//! Most events carry under a dozen attributes, where a sorted `SmallVec` beats a `HashMap` on
//! lookup and iteration, and its deterministic order is what the wire format's dictionary encoding
//! and reproducible tests depend on. See `docs/design/data-model.md`.

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
}
