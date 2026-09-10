//! Batch-level graph identity: which component created a batch, and which component the current
//! node received it from. Pipeline metadata, not application data -- never stamped onto
//! `Event::attributes` or `Resource` (`docs/design/pipeline-graph.md`'s "Provenance propagation"
//! section, `docs/adr/batch-provenance-on-delivered.md`).
//!
//! Lives here rather than in `logit-pipeline` (where the closest precedent, `TraceContext`,
//! lives) for two reasons: `logit-proto` has to name this type to encode/decode it, and cannot
//! depend on `logit-pipeline` (the dependency runs the other way); and `logit-script` depends on
//! `logit-core` only, so a type it can pass around directly here avoids the raw-byte-array
//! workaround `crate::trace`/`logit_script::trace` needs for `TraceContext`. `TraceContext` stays
//! in `logit-pipeline` because it carries real behavior (minting roots, deriving children);
//! `Provenance` is inert data, the same character as `Symbol`/the interner it's built from -- all
//! stamping *policy* (when to set which field) lives in `logit-pipeline::Fanout`, not here.

use crate::interner::{resolve, Symbol};

/// `origin`: the component that created the batch, set once and never mutated again.
/// `previous`: the component the current node received the batch from, rewritten at every hop.
/// Both `None` only when nothing in the graph has stamped a value yet (a `Fanout` with no
/// component attached -- tests/benches that construct one directly).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Provenance {
    pub origin: Option<Symbol>,
    pub previous: Option<Symbol>,
}

impl Provenance {
    /// `origin`, resolved back to its string. Panics if `origin` is `Some` but wasn't produced by
    /// `crate::interner::intern` -- the same invariant `interner::resolve` itself documents.
    pub fn origin_str(&self) -> Option<&'static str> {
        self.origin.map(resolve)
    }

    /// `previous`, resolved back to its string. See [`Provenance::origin_str`].
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
