use crate::AttrMap;
use bytes::Bytes;

/// Origin metadata (host, service, container id, ...) shared across every event in a batch.
/// `Arc`-wrapped at the batch level (see [`crate::EventBatch`]) rather than copied per event.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Resource {
    pub attributes: AttrMap,
    pub dropped_attributes_count: u32,
    pub schema_url: Option<Bytes>,
}

impl Resource {
    /// This resource's own contribution to [`crate::EventBatch::estimated_heap_bytes`] -- counted
    /// once per batch there (a resource is `Arc`-shared across every event, not copied per event),
    /// so exposed here as its own method rather than inlined, for a caller tracking that total
    /// incrementally (`logit_pipeline::BatchAccumulator`) to add exactly once per held resource,
    /// alongside [`crate::Event::estimated_heap_bytes`]'s per-event half of the same formula.
    pub fn estimated_heap_bytes(&self) -> u64 {
        crate::event::attr_map_heap_bytes(&self.attributes)
            + self.schema_url.as_ref().map(|s| s.len() as u64).unwrap_or(0)
    }
}

/// The instrumentation scope a batch's events were reported through -- OTLP's `InstrumentationScope`
/// (a name/version pair, e.g. `"nginx-otel-module"`/`"1.0.0"`), carried at the batch level rather
/// than duplicated onto every event's own attributes the way an earlier, since-retired convention
/// did (`docs/adr/lossless-transit.md`). `None` means no scope was reported or carried -- most
/// non-OTLP producers.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Scope {
    pub name: Bytes,
    pub version: Bytes,
    pub attributes: AttrMap,
    pub dropped_attributes_count: u32,
    pub schema_url: Option<Bytes>,
}

impl Scope {
    /// This scope's own contribution to [`crate::EventBatch::estimated_heap_bytes`] -- counted
    /// once per batch (a scope is `Arc`-shared across every event, same reasoning as
    /// [`Resource::estimated_heap_bytes`]).
    pub fn estimated_heap_bytes(&self) -> u64 {
        crate::event::attr_map_heap_bytes(&self.attributes)
            + self.name.len() as u64
            + self.version.len() as u64
            + self.schema_url.as_ref().map(|s| s.len() as u64).unwrap_or(0)
    }
}
