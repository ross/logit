//! The output buffering trait. See `docs/design/wire-protocol.md`: this boundary is cheap to add
//! now and expensive to retrofit onto call sites that assumed an in-memory queue, so it's defined
//! even though only an in-memory implementation ships initially.

use logit_core::EventBatch;

/// What to do when a bounded buffer is full and another batch arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    DropOldest,
    DropNewest,
    Block,
}

pub trait Buffer {
    fn push(&mut self, batch: EventBatch) -> Result<(), EventBatch>;
    fn pop(&mut self) -> Option<EventBatch>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // TODO: ack/retry hooks land here once at-least-once delivery is implemented
    // (`docs/design/wire-protocol.md`'s credit-based flow control section).
}
