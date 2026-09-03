use crate::AttrMap;

/// Origin metadata (host, service, container id, ...) shared across every event in a batch.
/// `Arc`-wrapped at the batch level (see [`crate::EventBatch`]) rather than copied per event.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Resource {
    pub attributes: AttrMap,
}

impl Resource {
    /// This resource's own contribution to [`crate::EventBatch::estimated_heap_bytes`] -- counted
    /// once per batch there (a resource is `Arc`-shared across every event, not copied per event),
    /// so exposed here as its own method rather than inlined, for a caller tracking that total
    /// incrementally (`logit_pipeline::BatchAccumulator`) to add exactly once per held resource,
    /// alongside [`crate::Event::estimated_heap_bytes`]'s per-event half of the same formula.
    pub fn estimated_heap_bytes(&self) -> u64 {
        crate::event::attr_map_heap_bytes(&self.attributes)
    }
}
