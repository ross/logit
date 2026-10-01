//! One `logit_in` component's table of sender high-water marks: the frames it recognizes as
//! resends (`docs/adr/native-hop-identity-and-sequence.md`, decisions 5 and 6, and the parent
//! module doc's "Deduplication").
//!
//! The lock is a `std::sync::Mutex` held only inside [`SenderTable::is_resend`] and
//! [`SenderTable::raise`], never across an `.await`: a connection task's future must stay `Send`,
//! and no lock spans a forward (decision 7 accepts the race that leaves).

use logit_core::Telemetry;
use logit_proto::native::SeqId;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

/// Identities in the table, published whenever the count changes.
const SENDERS: &str = "logit.input.senders";
/// Identities evicted from a full table.
const SENDERS_EVICTED: &str = "logit.input.senders.evicted";
/// Frames at or below their identity's mark: acknowledged, not forwarded.
pub(super) const RESENDS: &str = "logit.input.batches.resends";

pub(crate) struct SenderTable {
    capacity: usize,
    state: Mutex<TableState>,
    telemetry: Telemetry,
}

struct TableState {
    entries: HashMap<[u8; 16], Entry>,
    /// A logical clock, advanced on every touch. Unique per touch, so the eviction scan never
    /// meets a tie and needs no `Instant`.
    tick: u64,
}

struct Entry {
    /// The highest sequence a consumer took under this identity.
    mark: u64,
    last_seen: u64,
}

impl SenderTable {
    /// Room for `max_connections + max_connections / 4` identities: the headroom covers senders
    /// that reconnect or restart while their old identity is still held.
    pub(crate) fn new(max_connections: usize, telemetry: Telemetry) -> Self {
        let capacity = (max_connections + max_connections / 4).max(1);
        Self {
            capacity,
            state: Mutex::new(TableState { entries: HashMap::new(), tick: 0 }),
            telemetry,
        }
    }

    /// Whether `seq` is at or below its identity's mark. Refreshes a held identity's recency; an
    /// identity the table doesn't hold has a mark of 0 and isn't inserted, since only a taken
    /// forward ([`SenderTable::raise`]) makes an identity worth a slot.
    pub(crate) fn is_resend(&self, seq: SeqId) -> bool {
        let mut state = self.lock();
        let tick = state.next_tick();
        match state.entries.get_mut(&seq.id) {
            Some(entry) => {
                entry.last_seen = tick;
                seq.seq <= entry.mark
            }
            None => false,
        }
    }

    /// Raises `seq`'s identity's mark to `seq.seq` after a consumer took the batch; a mark never
    /// moves down. A new identity at a full table evicts the least recently seen one first, with
    /// one scan over the table.
    pub(crate) fn raise(&self, seq: SeqId) {
        let mut state = self.lock();
        let tick = state.next_tick();
        if let Some(entry) = state.entries.get_mut(&seq.id) {
            entry.mark = entry.mark.max(seq.seq);
            entry.last_seen = tick;
            return;
        }
        let full = state.entries.len() >= self.capacity;
        if full {
            let oldest = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_seen)
                .map(|(id, _)| *id)
                .expect("a full table of capacity >= 1 holds an entry");
            state.entries.remove(&oldest);
            self.telemetry.count(SENDERS_EVICTED, 1.0, &[]);
        }
        state.entries.insert(seq.id, Entry { mark: seq.seq, last_seen: tick });
        // An eviction and an insert leave the size where it was. Published under the lock, so
        // two connections' updates publish in the order they changed the table and the last
        // gauge point is the table's size.
        if !full {
            self.telemetry.gauge(SENDERS, state.entries.len() as f64, &[]);
        }
    }

    fn lock(&self) -> MutexGuard<'_, TableState> {
        // The state stays consistent at every point a panic could leave it.
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl TableState {
    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_pipeline::test_util::TelemetryProbe;

    fn seq(id: u8, seq: u64) -> SeqId {
        SeqId { id: [id; 16], seq }
    }

    #[test]
    fn the_capacity_follows_the_connection_cap() {
        assert_eq!(SenderTable::new(1024, Telemetry::default()).capacity, 1280);
        assert_eq!(SenderTable::new(1, Telemetry::default()).capacity, 1);
        assert_eq!(SenderTable::new(0, Telemetry::default()).capacity, 1);
    }

    #[test]
    fn a_number_at_or_below_the_mark_is_a_resend_and_one_above_is_not() {
        let table = SenderTable::new(4, Telemetry::default());
        assert!(!table.is_resend(seq(1, 1)), "an unknown identity has a mark of 0");
        table.raise(seq(1, 5));
        assert!(table.is_resend(seq(1, 5)));
        assert!(table.is_resend(seq(1, 3)));
        assert!(!table.is_resend(seq(1, 6)));
        table.raise(seq(1, 2));
        assert!(table.is_resend(seq(1, 5)), "a mark never moves down");
    }

    #[test]
    fn a_full_table_evicts_the_least_recently_seen_identity() {
        let mut probe = TelemetryProbe::new();
        let table = SenderTable::new(2, probe.telemetry("in", "logit_in", "listener"));
        table.raise(seq(1, 1));
        table.raise(seq(2, 1));
        // Touching 1 makes 2 the oldest.
        assert!(table.is_resend(seq(1, 1)));
        table.raise(seq(3, 1));
        assert!(table.is_resend(seq(1, 1)), "the recently seen identity stays");
        assert!(!table.is_resend(seq(2, 1)), "the least recently seen identity went");
        assert_eq!(probe.sum(SENDERS_EVICTED, &[]), 1.0);
        assert_eq!(probe.gauge(SENDERS, &[]), Some(2.0));
    }
}
