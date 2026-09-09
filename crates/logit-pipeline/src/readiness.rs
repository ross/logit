//! Readiness/liveness state (docs/plans/operator-surface.md, workstream B): a live snapshot of
//! where the pipeline is in its lifecycle and what every component is doing, written by
//! [`crate::runtime::run_with_telemetry`] and read by `logit-cli`'s admin server (workstream C).
//!
//! The constraint this whole module is designed around: readiness must be true only when the
//! pipeline can actually accept and forward, and false as soon as it can't -- never a static
//! "the process is up." See `docs/plans/operator-surface.md`'s "The constraint everything is
//! designed around".

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::watch;

/// Where the process as a whole is in its lifecycle.
///
/// Transitions are monotonic in one direction only: `Starting -> Ready -> Draining`, with
/// `Failed` reachable from any of them and terminal once reached. That monotonicity is enforced
/// inside [`Readiness`]'s own update methods, not by the callers -- the join loop (on a task
/// error) and the shutdown driver (on a real signal) genuinely race to update this, and neither
/// needs to know which one the other already did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Binding listeners and spawning tasks. Nothing has failed yet, but not everything exists.
    Starting,
    /// Every listener bound, every node task (or Lua thread) spawned, nothing has failed.
    Ready,
    /// A shutdown signal arrived; nodes are draining. Never entered from `Failed`.
    Draining,
    /// At least one node exited with an error. Terminal -- once here, always here.
    Failed,
}

impl Phase {
    /// This module's own vocabulary, not the wire words `/readyz` returns -- workstream C's
    /// `admin.rs` maps `Phase` to `ok`/`starting`/`draining`/`degraded` itself, deliberately using
    /// different strings: that mapping is an HTTP-response concern, not a runtime one.
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Starting => "starting",
            Phase::Ready => "ready",
            Phase::Draining => "draining",
            Phase::Failed => "failed",
        }
    }
}

/// One component's coarse lifecycle, as seen from the outside -- not the same granularity as
/// `Diagnostics`/`Telemetry`'s per-component instrumentation, just enough for a readiness probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// In the graph; not yet bound or spawned.
    Pending,
    /// [`crate::Input::bind`] returned `Ok`. Listeners only -- a transform, sink, or Lua node is
    /// never `Bound`, it goes straight from `Pending` to `Running`.
    Bound,
    /// Its task (or, for a Lua node, its thread) exists and hasn't returned.
    Running,
    /// It returned `Ok(())` on its own -- a finite listener, or any node whose inbox closed
    /// during a graceful drain.
    Finished,
    /// It returned `Err`, or its task panicked.
    Failed,
}

impl NodeState {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeState::Pending => "pending",
            NodeState::Bound => "bound",
            NodeState::Running => "running",
            NodeState::Finished => "finished",
            NodeState::Failed => "failed",
        }
    }
}

/// The whole readiness snapshot, as one `watch` value -- cheap to clone (a `HashMap` per update,
/// bounded by the graph's own component count) and cheap to read (`watch::Receiver::borrow`).
#[derive(Debug, Clone, PartialEq)]
pub struct PipelineState {
    pub phase: Phase,
    /// Every id in the graph, populated (all [`NodeState::Pending`]) by [`Readiness::begin`]
    /// before the first bind -- so a probe arriving during startup already sees the complete
    /// component list, not one that grows as nodes are bound and spawned.
    pub components: HashMap<String, NodeState>,
    /// When `phase` last *changed*. A per-node update alone does not move it.
    pub since: SystemTime,
}

impl Default for PipelineState {
    fn default() -> Self {
        Self { phase: Phase::Starting, components: HashMap::new(), since: SystemTime::now() }
    }
}

/// The runtime's write end of the readiness signal. Cloned freely -- every clone shares the same
/// underlying `watch::Sender`.
///
/// Every update goes through `watch::Sender::send_modify`, never `send`: `send` returns `Err`
/// once the last receiver is dropped, and [`Readiness::disabled`] is a channel with no receivers
/// *by construction* -- every update on a disabled signal would otherwise have to be `let _ =
/// ...`'d at the call site, or every call site would have to know to check first. `send_modify`
/// is infallible, mutates the shared value in place, and holds the watch's internal write lock
/// for the whole closure -- which is what makes the monotonicity rules above atomic against a
/// second, concurrent writer.
#[derive(Clone)]
pub struct Readiness(Arc<watch::Sender<PipelineState>>);

impl Readiness {
    /// A live signal, plus the receiver `logit-cli::admin::serve` (workstream C) watches.
    pub fn channel() -> (Self, watch::Receiver<PipelineState>) {
        let (tx, rx) = watch::channel(PipelineState::default());
        (Self(Arc::new(tx)), rx)
    }

    /// A signal nobody is watching -- what `run`/`run_with_shutdown` and every test that doesn't
    /// care about readiness passes. Still a real channel (see the type's own doc comment on why
    /// that matters); the receiver this constructor drops is simply never handed to anyone.
    pub fn disabled() -> Self {
        let (tx, _rx) = watch::channel(PipelineState::default());
        Self(Arc::new(tx))
    }

    /// A fresh receiver on this same signal.
    pub fn subscribe(&self) -> watch::Receiver<PipelineState> {
        self.0.subscribe()
    }

    /// A cloned snapshot -- for tests. The server (workstream C) holds a `Receiver` directly
    /// rather than polling this repeatedly.
    pub fn snapshot(&self) -> PipelineState {
        self.0.borrow().clone()
    }

    /// Every id in `ids` set to [`NodeState::Pending`]. Called once, before the first bind --
    /// `run_with_telemetry` calls it before it spawns anything that could write here.
    ///
    /// It does **not** set `phase`: `phase` is already [`Phase::Starting`] by construction
    /// ([`PipelineState::default`], which both constructors above start from), so the only thing
    /// assigning it could ever do is move an *already advanced* phase backwards -- erasing a
    /// drain (and its `since`) that a concurrent writer had just recorded, and letting
    /// [`Readiness::ready`] then promote the run to `Ready` as if nothing had happened. Like
    /// every other method here, the rule lives in this method rather than in its caller's
    /// ordering (see this module's own doc comment); `since` moves only while the phase is still
    /// the `Starting` this seeding belongs to.
    pub fn begin(&self, ids: &[String]) {
        self.0.send_modify(|state| {
            state.components = ids.iter().map(|id| (id.clone(), NodeState::Pending)).collect();
            if state.phase == Phase::Starting {
                state.since = SystemTime::now();
            }
        });
    }

    /// Sets one component's state, leaving `phase` untouched -- callers move `phase` itself via
    /// [`Readiness::ready`]/[`Readiness::draining`]/[`Readiness::failed`].
    pub fn set_node(&self, id: &str, node_state: NodeState) {
        self.0.send_modify(|state| {
            if let Some(existing) = state.components.get_mut(id) {
                *existing = node_state;
            }
        });
    }

    /// `Starting -> Ready`. A no-op from `Draining` or `Failed` -- once draining has begun (or
    /// failed), nothing moves the process back to accepting traffic.
    pub fn ready(&self) {
        self.0.send_modify(|state| {
            if state.phase == Phase::Starting {
                state.phase = Phase::Ready;
                state.since = SystemTime::now();
            }
        });
    }

    /// `Starting | Ready -> Draining`. A no-op from `Failed` -- a real shutdown signal arriving
    /// after a node has already failed must not paper over the failure with "draining".
    pub fn draining(&self) {
        self.0.send_modify(|state| {
            if state.phase != Phase::Failed && state.phase != Phase::Draining {
                state.phase = Phase::Draining;
                state.since = SystemTime::now();
            }
        });
    }

    /// Any phase -> `Failed`. Terminal, and idempotent: a second failing node after the first
    /// leaves `phase` at `Failed` and `since` at the *first* failure's timestamp.
    pub fn failed(&self) {
        self.0.send_modify(|state| {
            if state.phase != Phase::Failed {
                state.phase = Phase::Failed;
                state.since = SystemTime::now();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_seeds_every_id_as_pending_and_sets_the_starting_phase() {
        let (readiness, rx) = Readiness::channel();
        readiness.begin(&["a".to_string(), "b".to_string()]);
        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.phase, Phase::Starting);
        assert_eq!(snapshot.components.get("a"), Some(&NodeState::Pending));
        assert_eq!(snapshot.components.get("b"), Some(&NodeState::Pending));
    }

    #[test]
    fn ready_does_not_regress_from_draining_or_failed() {
        let (readiness, _rx) = Readiness::channel();
        readiness.begin(&[]);
        readiness.draining();
        readiness.ready();
        assert_eq!(readiness.snapshot().phase, Phase::Draining, "ready() must not undo draining");

        let (readiness, _rx) = Readiness::channel();
        readiness.begin(&[]);
        readiness.failed();
        readiness.ready();
        assert_eq!(readiness.snapshot().phase, Phase::Failed, "ready() must not undo failed");
    }

    #[test]
    fn draining_does_not_regress_from_failed() {
        let (readiness, _rx) = Readiness::channel();
        readiness.begin(&[]);
        readiness.failed();
        readiness.draining();
        assert_eq!(readiness.snapshot().phase, Phase::Failed, "draining() must not undo failed");
    }

    /// `begin` is the one update method that isn't a transition -- it seeds the component list --
    /// so it must leave `phase` alone rather than resetting it to `Starting`. `run_with_telemetry`
    /// calls it before it spawns anything that could write here, but the rule lives here, in the
    /// method, not in that caller's statement order (this module's own doc comment): a caller that
    /// got the order wrong would otherwise erase a drain, `since` and all, and `ready()` would
    /// then promote the run to `Ready` as though shutdown had never begun.
    #[test]
    fn begin_seeds_components_without_moving_the_phase_backwards() {
        let (readiness, _rx) = Readiness::channel();
        readiness.draining();
        let drained_at = readiness.snapshot().since;
        std::thread::sleep(std::time::Duration::from_millis(5));
        readiness.begin(&["a".to_string()]);
        let snapshot = readiness.snapshot();
        assert_eq!(snapshot.phase, Phase::Draining, "begin() must not undo draining");
        assert_eq!(snapshot.since, drained_at, "begin() must not move a drain's `since`");
        assert_eq!(
            snapshot.components.get("a"),
            Some(&NodeState::Pending),
            "begin() must still seed the component list whatever the phase"
        );
        readiness.ready();
        assert_eq!(
            readiness.snapshot().phase,
            Phase::Draining,
            "a phase begin() left alone must still be one ready() refuses to promote"
        );

        let (readiness, _rx) = Readiness::channel();
        readiness.failed();
        readiness.begin(&[]);
        assert_eq!(readiness.snapshot().phase, Phase::Failed, "begin() must not undo failed");
    }

    #[test]
    fn failed_is_terminal_and_keeps_the_first_failures_timestamp() {
        let (readiness, _rx) = Readiness::channel();
        readiness.begin(&[]);
        readiness.failed();
        let first = readiness.snapshot().since;
        std::thread::sleep(std::time::Duration::from_millis(5));
        readiness.failed();
        assert_eq!(readiness.snapshot().since, first, "a second failure must not move `since`");
    }

    #[test]
    fn set_node_on_an_unknown_id_is_a_silent_no_op() {
        let (readiness, _rx) = Readiness::channel();
        readiness.begin(&["a".to_string()]);
        readiness.set_node("not-in-the-graph", NodeState::Running);
        assert_eq!(readiness.snapshot().components.len(), 1);
    }

    /// The whole reason `Readiness` uses `send_modify` and never `watch::Sender::send`: `send`
    /// returns `Err` once every receiver is dropped, which is exactly `disabled()`'s shape by
    /// construction. If any update method regressed to `send`, this test would panic (an
    /// unhandled `Err`) or -- if it were `let _ = ... .send(...)` -- would still be silently
    /// swallowing exactly the bug this test exists to catch.
    #[test]
    fn every_update_method_is_infallible_with_no_receiver() {
        let readiness = Readiness::disabled();
        readiness.begin(&["a".to_string()]);
        readiness.set_node("a", NodeState::Running);
        readiness.ready();
        readiness.draining();
        readiness.failed();
        assert_eq!(readiness.snapshot().phase, Phase::Failed);
    }
}
