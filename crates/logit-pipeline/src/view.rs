//! [`BatchView`]: a destination's share of a routed batch, as an index list into one shared
//! `Arc<EventBatch>` rather than an owned copy. A measurement prototype of an index-partition
//! router: a router sends each destination the incoming batch's `Arc`, the indices of the events
//! that destination gets, and, for `route_signals`, the signals it may see, so routing never
//! touches event bytes. A consumer that can read a view does; anything else calls
//! [`BatchView::materialize`], which clones only the indexed events.

use logit_core::{Event, EventBatch, MetricList};
use std::sync::Arc;

/// A view of some or all of one shared batch's events, and of some or all of their payloads.
#[derive(Debug, Clone)]
pub struct BatchView {
    pub batch: Arc<EventBatch>,
    /// The events in view, as ascending indices into `batch.events`; `None` is every event.
    pub indices: Option<Arc<[u32]>>,
    /// The payloads in view, as [`crate::signal_index`] bits (logs `1`, metrics `2`, traces `4`);
    /// `None` is every payload. An event in view always shows its timestamp and attributes.
    pub signals: Option<u8>,
}

impl From<Arc<EventBatch>> for BatchView {
    fn from(batch: Arc<EventBatch>) -> Self {
        BatchView::whole(batch)
    }
}

impl From<EventBatch> for BatchView {
    fn from(batch: EventBatch) -> Self {
        BatchView::whole(Arc::new(batch))
    }
}

impl BatchView {
    /// Every event and every payload of `batch`.
    pub fn whole(batch: Arc<EventBatch>) -> Self {
        BatchView { batch, indices: None, signals: None }
    }

    /// Whether this view is the whole batch, so `batch` can be read as is.
    pub fn is_whole(&self) -> bool {
        self.indices.is_none() && self.signals.is_none()
    }

    /// Events in view.
    pub fn len(&self) -> usize {
        match &self.indices {
            Some(indices) => indices.len(),
            None => self.batch.events.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The events in view, whole: a reader honoring `signals` skips the payloads it leaves out.
    pub fn events(&self) -> ViewEvents<'_> {
        match &self.indices {
            Some(indices) => ViewEvents::Indexed(&self.batch.events, indices.iter()),
            None => ViewEvents::All(self.batch.events.iter()),
        }
    }

    /// An owned batch holding what this view shows: the resource and scope `Arc`s shared, each
    /// event in view cloned, and, under `signals`, only the payloads in view (the way
    /// `route_signals` splits a part off an event).
    pub fn materialize(&self) -> EventBatch {
        if self.is_whole() {
            return (*self.batch).clone();
        }
        let events = match self.signals {
            None => self.events().cloned().collect(),
            Some(signals) => self.events().map(|event| part_of(event, signals)).collect(),
        };
        EventBatch {
            resource: self.batch.resource.clone(),
            scope: self.batch.scope.clone(),
            events,
        }
    }

    /// The batch as an `Arc`: `batch` itself for a whole view, else a materialized copy.
    pub fn into_batch_arc(self) -> Arc<EventBatch> {
        if self.is_whole() {
            self.batch
        } else {
            Arc::new(self.materialize())
        }
    }

    /// The batch owned: the `Arc` unwrapped when this is its last holder, else a copy.
    pub fn into_batch(self) -> EventBatch {
        if self.is_whole() {
            Arc::try_unwrap(self.batch).unwrap_or_else(|shared| (*shared).clone())
        } else {
            self.materialize()
        }
    }

    /// [`EventBatch::estimated_heap_bytes`] over the events in view; O(view).
    pub fn estimated_heap_bytes(&self) -> u64 {
        if self.is_whole() {
            return self.batch.estimated_heap_bytes();
        }
        let mut total = self.batch.resource.estimated_heap_bytes()
            + self.batch.scope.as_ref().map(|s| s.estimated_heap_bytes()).unwrap_or(0)
            + (self.len() * std::mem::size_of::<Event>()) as u64;
        for event in self.events() {
            total += event.estimated_heap_bytes();
        }
        total
    }
}

/// Test-only: reads a whole view as its batch, so tests written against the store's former
/// `Arc<EventBatch>` item read on. Never in a production build, where every reader goes through
/// [`BatchView::events`] or [`BatchView::materialize`].
#[cfg(any(test, feature = "test-util"))]
impl std::ops::Deref for BatchView {
    type Target = EventBatch;

    fn deref(&self) -> &EventBatch {
        assert!(self.is_whole(), "a partial BatchView read as its whole batch");
        &self.batch
    }
}

/// The part of `event` showing the payloads `signals` names.
fn part_of(event: &Event, signals: u8) -> Event {
    Event {
        timestamp: event.timestamp,
        attributes: event.attributes.clone(),
        log: if signals & 1 != 0 { event.log.clone() } else { None },
        metrics: if signals & 2 != 0 { event.metrics.clone() } else { MetricList::new() },
        span: if signals & 4 != 0 { event.span.clone() } else { None },
    }
}

/// [`BatchView::events`]' iterator.
pub enum ViewEvents<'a> {
    All(std::slice::Iter<'a, Event>),
    Indexed(&'a [Event], std::slice::Iter<'a, u32>),
}

impl<'a> Iterator for ViewEvents<'a> {
    type Item = &'a Event;

    fn next(&mut self) -> Option<&'a Event> {
        match self {
            ViewEvents::All(iter) => iter.next(),
            ViewEvents::Indexed(events, indices) => indices.next().map(|&i| &events[i as usize]),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            ViewEvents::All(iter) => iter.size_hint(),
            ViewEvents::Indexed(_, indices) => indices.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, LogRecord, MetricKind, MetricRecord, Resource, Value};

    fn event(id: i64) -> Event {
        let mut attributes = AttrMap::new();
        attributes.insert("id", Value::I64(id));
        Event {
            timestamp: id,
            attributes,
            log: Some(LogRecord {
                message: Value::str("m"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            }),
            metrics: MetricList::from_iter([MetricRecord::new(
                logit_core::interner::intern("c"),
                MetricKind::counter(1.0),
            )]),
            span: None,
        }
    }

    fn batch() -> Arc<EventBatch> {
        Arc::new(EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: (0..5).map(event).collect(),
        })
    }

    #[test]
    fn an_indexed_view_materializes_its_events_in_order() {
        let view =
            BatchView { batch: batch(), indices: Some(Arc::from(vec![1u32, 3, 4])), signals: None };
        assert_eq!(view.len(), 3);
        let out = view.materialize();
        assert_eq!(out.events.iter().map(|e| e.timestamp).collect::<Vec<_>>(), vec![1, 3, 4]);
        assert!(out.events.iter().all(|e| e.log.is_some() && !e.metrics.is_empty()));
    }

    #[test]
    fn a_signal_view_materializes_only_its_payloads() {
        let view = BatchView { batch: batch(), indices: None, signals: Some(2) };
        let out = view.materialize();
        assert_eq!(out.events.len(), 5);
        assert!(out.events.iter().all(|e| e.log.is_none() && !e.metrics.is_empty()));
        assert!(out.events.iter().all(|e| e.attributes.get("id").is_some()));
    }
}
