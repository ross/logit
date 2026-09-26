//! Readiness/liveness state: a live snapshot of where the pipeline is in its lifecycle and what
//! every component is doing, written by [`crate::runtime::run_with_telemetry`] and read by
//! `logit-cli`'s admin server (`docs/adr/admin-readiness-endpoint.md`).
//!
//! Readiness is true only while the pipeline can accept and forward, and false as soon as it
//! can't; never a static "the process is up." See `docs/plans/operator-surface.md`'s "The
//! constraint everything is designed around".

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::watch;

/// Where the process as a whole is in its lifecycle.
///
/// Transitions only move forward: `Starting -> Ready -> Draining`, with `Failed` reachable from
/// any of them and terminal. [`Readiness`]'s update methods enforce this, not their callers: the
/// join loop (on a task error) and the shutdown driver (on a signal) race to update it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Binding listeners and spawning tasks. Nothing has failed yet, but not everything exists.
    Starting,
    /// Every listener bound, every node task (or Lua thread) spawned, nothing has failed.
    Ready,
    /// A shutdown signal arrived; nodes are draining. Never entered from `Failed`.
    Draining,
    /// At least one node exited with an error. Terminal.
    Failed,
}

impl Phase {
    /// This module's vocabulary, not the words `/readyz` returns: `logit-cli`'s `admin.rs` maps
    /// `Phase` to `ok`/`starting`/`draining`/`degraded` (and a stalled node to `stalled`) itself,
    /// as an HTTP-response concern.
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Starting => "starting",
            Phase::Ready => "ready",
            Phase::Draining => "draining",
            Phase::Failed => "failed",
        }
    }
}

/// One component's coarse lifecycle, as seen from the outside: enough for a readiness probe, far
/// coarser than `Diagnostics`/`Telemetry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// In the graph; not yet bound or spawned.
    Pending,
    /// [`crate::Input::bind`] or [`crate::Output::bind`] returned `Ok` in `run_with_telemetry`'s
    /// pre-spawn bind pass. A sink with the default no-op `bind` still reaches `Bound`: the state
    /// records that the pass cleared the node, not that it opened a socket. A transform or Lua
    /// node goes straight from `Pending` to `Running`.
    Bound,
    /// Its task (or, for a Lua node, its thread) exists and hasn't returned.
    Running,
    /// A Lua node whose thread has been inside one `process()`/`flush()` call with no progress
    /// for its `stall_after` (`docs/adr/lua-runaway-script-bounds.md`). Set and cleared only by
    /// its watcher, which moves it back to `Running` on the next sign of progress. `/readyz`
    /// reports `503 stalled` while any node is here and the phase is `Ready`, without moving
    /// [`Phase`].
    Stalled,
    /// It returned `Ok(())` on its own: a finite listener, or any node whose inbox closed
    /// during a graceful drain.
    Finished,
    /// It returned `Err`, or its task (or, for a Lua node, its thread) panicked.
    Failed,
    /// A `target` ([`crate::graph::Role::Target`], `docs/adr/target-components.md`): a name for
    /// its routers' outbound edges, with no task or inbox, so its liveness is its routers'. Set
    /// once, in the pre-spawn pass that builds each target's `Fanout`, and never moved: the
    /// other transitions come from a `JoinSet` entry a target doesn't have.
    Alias,
}

impl NodeState {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeState::Pending => "pending",
            NodeState::Bound => "bound",
            NodeState::Running => "running",
            NodeState::Stalled => "stalled",
            NodeState::Finished => "finished",
            NodeState::Failed => "failed",
            NodeState::Alias => "alias",
        }
    }
}

/// The whole readiness snapshot, as one `watch` value. A clone copies a `HashMap` bounded by the
/// graph's component count.
#[derive(Debug, Clone, PartialEq)]
pub struct PipelineState {
    pub phase: Phase,
    /// Every id in the graph, seeded as [`NodeState::Pending`] by [`Readiness::begin`] before the
    /// first bind, so a probe during startup sees the complete component list.
    pub components: HashMap<String, NodeState>,
    /// When `phase` last *changed*. A per-node update alone does not move it.
    pub since: SystemTime,
}

impl PipelineState {
    /// Whether any component reads [`NodeState::Stalled`].
    pub fn has_stalled_node(&self) -> bool {
        self.components.values().any(|state| *state == NodeState::Stalled)
    }
}

impl Default for PipelineState {
    fn default() -> Self {
        Self { phase: Phase::Starting, components: HashMap::new(), since: SystemTime::now() }
    }
}

/// The runtime's write end of the readiness signal. Every clone shares one `watch::Sender`.
///
/// Every update goes through `watch::Sender::send_modify`, never `send`: `send` returns `Err`
/// once the last receiver is dropped, and [`Readiness::disabled`] has no receivers by
/// construction. `send_modify` is infallible and holds the watch's write lock for the whole
/// closure, which makes each forward-only transition check atomic against a concurrent writer.
#[derive(Clone)]
pub struct Readiness(Arc<watch::Sender<PipelineState>>);

impl Readiness {
    /// A live signal, plus the receiver `logit-cli::admin::serve` watches.
    pub fn channel() -> (Self, watch::Receiver<PipelineState>) {
        let (tx, rx) = watch::channel(PipelineState::default());
        (Self(Arc::new(tx)), rx)
    }

    /// A signal nobody watches, for `run`/`run_with_shutdown` and tests that don't care about
    /// readiness. Still a real channel whose receiver is dropped here (see [`Readiness`]).
    pub fn disabled() -> Self {
        let (tx, _rx) = watch::channel(PipelineState::default());
        Self(Arc::new(tx))
    }

    /// A fresh receiver on this same signal.
    pub fn subscribe(&self) -> watch::Receiver<PipelineState> {
        self.0.subscribe()
    }

    /// A cloned snapshot, for tests. The admin server holds a `Receiver` instead.
    pub fn snapshot(&self) -> PipelineState {
        self.0.borrow().clone()
    }

    /// Sets every id in `ids` to [`NodeState::Pending`]. Called once, before the first bind.
    ///
    /// It does not set `phase`, which starts as [`Phase::Starting`] by construction: assigning it
    /// could only move an advanced phase backwards, erasing a concurrent writer's drain (and its
    /// `since`) and letting [`Readiness::ready`] promote the run to `Ready`. `since` moves only
    /// while the phase is still `Starting`.
    pub fn begin(&self, ids: &[String]) {
        self.0.send_modify(|state| {
            state.components = ids.iter().map(|id| (id.clone(), NodeState::Pending)).collect();
            if state.phase == Phase::Starting {
                state.since = SystemTime::now();
            }
        });
    }

    /// Sets one component's state, leaving `phase` untouched. An id not seeded by `begin` is
    /// ignored.
    pub fn set_node(&self, id: &str, node_state: NodeState) {
        self.0.send_modify(|state| {
            if let Some(existing) = state.components.get_mut(id) {
                *existing = node_state;
            }
        });
    }

    /// `Starting -> Ready`. A no-op from `Draining` or `Failed`: nothing moves the process back
    /// to accepting traffic.
    pub fn ready(&self) {
        self.0.send_modify(|state| {
            if state.phase == Phase::Starting {
                state.phase = Phase::Ready;
                state.since = SystemTime::now();
            }
        });
    }

    /// `Starting | Ready -> Draining`. A no-op from `Failed`: a shutdown signal after a node has
    /// failed must not mask the failure as "draining".
    pub fn draining(&self) {
        self.0.send_modify(|state| {
            if state.phase != Phase::Failed && state.phase != Phase::Draining {
                state.phase = Phase::Draining;
                state.since = SystemTime::now();
            }
        });
    }

    /// Any phase -> `Failed`. Terminal and idempotent: `since` keeps the first failure's
    /// timestamp.
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

    /// `begin` seeds components without moving an advanced phase (or its `since`) backwards.
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

    #[test]
    fn has_stalled_node_reflects_any_stalled_component() {
        let (readiness, _rx) = Readiness::channel();
        readiness.begin(&["a".to_string(), "b".to_string()]);
        readiness.set_node("a", NodeState::Running);
        readiness.set_node("b", NodeState::Running);
        assert!(!readiness.snapshot().has_stalled_node());

        readiness.set_node("b", NodeState::Stalled);
        assert!(readiness.snapshot().has_stalled_node(), "one stalled component is enough");

        readiness.set_node("b", NodeState::Running);
        assert!(!readiness.snapshot().has_stalled_node(), "a resumed component clears it");
    }

    /// Every update method works on `disabled()`, which has no receiver (see [`Readiness`]).
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
