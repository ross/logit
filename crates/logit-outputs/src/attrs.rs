//! The resource-attributes-overridden-by-event-attributes merge-join shared by every sink that
//! renders both onto one wire representation (`influxdb_out`'s tags, `statsd_out`'s tags).
//!
//! Originally `influxdb_out::render_tag_suffix`'s own inline walk; lifted out once `statsd_out`
//! needed the identical merge, with a different emission format on top.

use logit_core::interner::Symbol;
use logit_core::{Event, Resource, Value};
use std::cmp::Ordering;

/// Iterates `resource.attributes` merged with `event.attributes`, in sorted-[`Symbol`] order, the
/// event's value winning on an equal key. Both maps already iterate in that order
/// (`AttrMap::iter`), so walking them in lockstep and preferring the event's value on a tie
/// produces exactly the same sequence a clone-and-insert would -- without copying an `AttrMap` per
/// event, and without the `resolve` -> `intern` round trip re-inserting every key would cost.
pub(crate) fn merged<'a>(
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
    use logit_core::AttrMap;

    fn resource_with(attrs: &[(&str, &str)]) -> Resource {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, *v);
        }
        Resource { attributes }
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
            .map(|(k, v)| {
                (logit_core::interner::resolve(k).to_string(), v.as_str().unwrap().to_string())
            })
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

    /// `AttrMap::iter` orders by `Symbol` (interning order), not alphabetically -- so the property
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
            .map(|(k, v)| {
                (logit_core::interner::resolve(k).to_string(), v.as_str().unwrap().to_string())
            })
            .collect();
        assert_eq!(collect(&resource, &event), expected);
    }
}
