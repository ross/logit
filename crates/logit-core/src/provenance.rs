//! Batch-level graph identity: which component created a batch, and which component the current
//! node received it from. Pipeline metadata, not application data -- never stamped onto
//! `Event::attributes` or `Resource` (`docs/design/pipeline-graph.md`'s "Provenance propagation"
//! section, `docs/adr/batch-provenance-on-delivered.md`).
//!
//! Lives here, unlike `TraceContext`, because `logit-proto` must encode it and can't depend on
//! `logit-pipeline`, and `logit-script` depends only on `logit-core`. It's inert data; the
//! stamping policy lives in `logit-pipeline::Fanout`.

use crate::interner::{resolve, Symbol};

/// A batch's graph identity. Both fields are `None` only before anything stamps them (a `Fanout`
/// built directly by a test or bench, with no component).
///
/// `origin` is the component that created the batch, set once. `previous` is the component the
/// current node received it from, rewritten at every hop.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Provenance {
    pub origin: Option<Symbol>,
    pub previous: Option<Symbol>,
}

impl Provenance {
    /// `origin` as a string. Panics as [`crate::interner::resolve`] does.
    pub fn origin_str(&self) -> Option<&'static str> {
        self.origin.map(resolve)
    }

    /// `previous` as a string. Panics as [`crate::interner::resolve`] does.
    pub fn previous_str(&self) -> Option<&'static str> {
        self.previous.map(resolve)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_both_fields_absent() {
        let p = Provenance::default();
        assert_eq!(p.origin, None);
        assert_eq!(p.previous, None);
    }

    #[test]
    fn str_accessors_resolve_through_the_interner() {
        let p = Provenance {
            origin: Some(crate::interner::intern("nginx_in")),
            previous: Some(crate::interner::intern("parse_json")),
        };
        assert_eq!(p.origin_str(), Some("nginx_in"));
        assert_eq!(p.previous_str(), Some("parse_json"));
    }

    #[test]
    fn str_accessors_are_none_when_the_field_is_none() {
        let p = Provenance::default();
        assert_eq!(p.origin_str(), None);
        assert_eq!(p.previous_str(), None);
    }
}
