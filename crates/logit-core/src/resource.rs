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
    /// This resource's share of [`crate::EventBatch::estimated_heap_bytes`], counted once per
    /// batch; an incremental caller adds it once alongside [`crate::Event::estimated_heap_bytes`].
    pub fn estimated_heap_bytes(&self) -> u64 {
        crate::event::attr_map_heap_bytes(&self.attributes)
            + self.schema_url.as_ref().map(|s| s.len() as u64).unwrap_or(0)
    }
}

/// OTLP's `InstrumentationScope` (e.g. `"nginx-otel-module"`/`"1.0.0"`), carried once per batch
/// rather than on every event (`docs/adr/lossless-transit.md`). Most non-OTLP producers have none
/// (`EventBatch::scope` is `None`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Scope {
    pub name: Bytes,
    pub version: Bytes,
    pub attributes: AttrMap,
    pub dropped_attributes_count: u32,
    pub schema_url: Option<Bytes>,
}

impl Scope {
    /// This scope's share of [`crate::EventBatch::estimated_heap_bytes`], counted once per batch.
    pub fn estimated_heap_bytes(&self) -> u64 {
        crate::event::attr_map_heap_bytes(&self.attributes)
            + self.name.len() as u64
            + self.version.len() as u64
            + self.schema_url.as_ref().map(|s| s.len() as u64).unwrap_or(0)
    }
}
