use crate::AttrMap;

/// Origin metadata (host, service, container id, ...) shared across every event in a batch.
/// `Arc`-wrapped at the batch level (see [`crate::EventBatch`]) rather than copied per event.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Resource {
    pub attributes: AttrMap,
}
