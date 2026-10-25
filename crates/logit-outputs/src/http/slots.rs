//! [`Slots`]: the window an HTTP sink keeps under `concurrency:` above 1, one slot per batch in
//! flight, keyed by the batch's [`SeqId`] (`docs/adr/sink-request-concurrency.md`, decision 3).
//!
//! The sink's `Output::window`, `submit`, and `await_ack` go through it, and its `send` stays the
//! path at a concurrency of 1, which `write_loop` takes whenever the window is 1. What the slot
//! helper does on each call:
//!
//! - **`observe`** (`Output::observe_batch`) adds a slot at the back with a fresh
//!   [`BatchAccounting`] over the sink's one [`CountGate`]. Every encode runs on the loop task
//!   with nothing awaited inside it, so one gate serves every slot.
//! - **`submit`** finds the batch's slot, runs the sink's encode through that slot's accounting
//!   on the loop task, and spawns the transport as a task holding the request. A resubmitted head
//!   finds its slot again, so a unit an earlier attempt counted runs muted.
//! - **`ack`** (`Output::await_ack`) awaits the front slot's task and hands its outcome to the
//!   sink on the loop task, where the transport counters and the throttled diagnostics live, then
//!   finishes the slot's accounting. `Ok` pops the slot. Every `Err` is marked [`HeadOnly`]: a
//!   final one (`Rejected`, or `Ambiguous` under at-most-once) pops the slot, and a retryable one
//!   keeps it for the head's resubmit. A slot behind the head whose request already failed keeps
//!   its outcome in its task until it reaches the front; no task retries.
//! - **Cancellation.** Dropping a slot aborts its task, so dropping the helper or [`Slots::cancel`]
//!   aborts every request in flight. A cancelled `ack` cancels every slot and keeps each one's
//!   accounting, so a batch submitted again counts nothing twice.
//!
//! Each request keeps its own timeout; the slots add no time bound of their own.
//! `logit.output.in_flight` reads the slots whose request is in flight or answered and not yet
//! acknowledged.

use crate::accounting::BatchAccounting;
use logit_core::{CountGate, Telemetry};
use logit_pipeline::{DeliveryPosture, Fault, HeadOnly, SeqId};
use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;

/// The gauge of slots in flight, the name `logit_out` reports its frames under.
const IN_FLIGHT: &str = "logit.output.in_flight";

/// A spawned transport task, aborted when dropped. It returns the sink's outcome and how long the
/// request took.
struct Task<O>(tokio::task::JoinHandle<(O, Duration)>);

impl<O> Drop for Task<O> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum State<O> {
    /// Observed and not submitted, or waiting for the head's resubmit.
    Idle,
    /// The request is in flight, or answered and waiting for `ack`.
    Running(Task<O>),
    /// The encode left nothing to send: acknowledged with no request.
    Empty,
}

struct Slot<O> {
    seq: SeqId,
    accounting: BatchAccounting,
    state: State<O>,
}

/// The window of an HTTP sink's batches in flight (the module doc). `O` is what the sink's
/// transport task returns for its batch.
pub(crate) struct Slots<O> {
    concurrency: usize,
    gate: CountGate,
    posture: DeliveryPosture,
    telemetry: Telemetry,
    slots: VecDeque<Slot<O>>,
    /// How long the request behind the last `ack` took, `None` when it sent none.
    answered: Option<Duration>,
}

impl<O: Send + 'static> Slots<O> {
    /// A window of one, over the sink's `gate`, the one its encoder's handles are gated by.
    pub(crate) fn new(gate: CountGate) -> Self {
        Self {
            concurrency: 1,
            gate,
            posture: DeliveryPosture::AtLeastOnce,
            telemetry: Telemetry::default(),
            slots: VecDeque::new(),
            answered: None,
        }
    }

    /// Sets the window (`concurrency:`), at least 1.
    pub(crate) fn set_concurrency(&mut self, concurrency: u32) {
        self.concurrency = usize::try_from(concurrency).unwrap_or(usize::MAX).max(1);
    }

    /// The sink's ungated handle, for the in-flight gauge.
    pub(crate) fn set_telemetry(&mut self, telemetry: Telemetry) {
        self.telemetry = telemetry;
    }

    /// `Output::window`: the configured concurrency.
    pub(crate) fn window(&self) -> usize {
        self.concurrency
    }

    /// `Output::observe_posture`: decides which failures are final for every slot.
    pub(crate) fn observe_posture(&mut self, posture: DeliveryPosture) {
        self.posture = posture;
        for slot in &mut self.slots {
            slot.accounting.observe_posture(posture);
        }
    }

    /// `Output::observe_batch`: a slot for `seq` at the back, armed with nothing counted.
    pub(crate) fn observe(&mut self, seq: SeqId) {
        let index = match self.position(seq) {
            Some(index) => index,
            None => {
                self.slots.push_back(self.slot(seq));
                self.slots.len() - 1
            }
        };
        self.slots[index].accounting.observe();
    }

    /// `Output::submit`: runs `prepare` on this task with the batch's accounting, then spawns the
    /// request it returns. `prepare` returning `None` sends nothing, and the batch is
    /// acknowledged with no request. A slot is made, unarmed, for a batch never observed.
    ///
    /// A failed `prepare` is marked [`HeadOnly`]. At the front it finishes the slot's
    /// accounting, and a final failure leaves the window; behind the head the slot waits, as
    /// `write_loop` submits it again once it's the head.
    pub(crate) fn submit<F>(
        &mut self,
        seq: SeqId,
        prepare: impl FnOnce(&mut BatchAccounting) -> anyhow::Result<Option<F>>,
    ) -> anyhow::Result<()>
    where
        F: Future<Output = O> + Send + 'static,
    {
        let index = match self.position(seq) {
            Some(index) => index,
            None => {
                self.slots.push_back(self.slot(seq));
                self.slots.len() - 1
            }
        };
        let slot = &mut self.slots[index];
        slot.state = State::Idle;
        let result = match prepare(&mut slot.accounting) {
            Ok(Some(request)) => {
                let started = tokio::time::Instant::now();
                let timed = async move {
                    let outcome = request.await;
                    (outcome, started.elapsed())
                };
                slot.state = State::Running(Task(tokio::spawn(timed)));
                Ok(())
            }
            Ok(None) => {
                slot.state = State::Empty;
                Ok(())
            }
            Err(err) if index == 0 => {
                let result = slot.accounting.finish(Err(err));
                if slot.accounting.is_final(&result) {
                    self.slots.pop_front();
                }
                result.context_head_only()
            }
            Err(err) => Err(err).context_head_only(),
        };
        self.report();
        result
    }

    /// `Output::await_ack`: awaits the front slot's outcome and passes it to `apply`, whose
    /// result is the batch's, then finishes the slot's accounting. `Ok` pops the slot; an `Err`
    /// is marked [`HeadOnly`] and pops it only when final. A panic in the task resumes here, as
    /// one in a `send` would.
    pub(crate) async fn ack(
        &mut self,
        apply: impl FnOnce(O) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.answered = None;
        let Some(front) = self.slots.front_mut() else {
            return Ok(());
        };
        let result = match std::mem::replace(&mut front.state, State::Idle) {
            State::Empty => Ok(()),
            State::Idle => Err(anyhow::anyhow!("no request in flight for the oldest batch"))
                .context_fault(Fault::Clean),
            State::Running(task) => {
                // Cancels every slot unless the wait completes, so a cancelled `ack` leaves
                // nothing in flight.
                struct OnCancel<'a, O> {
                    slots: &'a mut Slots<O>,
                    task: Task<O>,
                    done: bool,
                }
                impl<O> Drop for OnCancel<'_, O> {
                    fn drop(&mut self) {
                        if !self.done {
                            self.slots.cancel_all();
                        }
                    }
                }
                let mut guard = OnCancel { slots: self, task, done: false };
                let joined = (&mut guard.task.0).await;
                guard.done = true;
                drop(guard);
                match joined {
                    Ok((outcome, took)) => {
                        self.answered = Some(took);
                        apply(outcome)
                    }
                    Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
                    Err(err) => Err(anyhow::Error::new(err)).context_fault(Fault::Ambiguous),
                }
            }
        };
        let front = self.slots.front_mut().expect("the front slot is still there");
        let result = front.accounting.finish(result);
        let result = match result {
            Ok(()) => {
                self.slots.pop_front();
                Ok(())
            }
            Err(err) => {
                let err = Err(err);
                if front.accounting.is_final(&err) {
                    self.slots.pop_front();
                }
                err.context_head_only()
            }
        };
        self.report();
        result
    }

    /// `Output::answered_send_duration`: how long the request behind the last `ack` took, from
    /// its spawn to its response.
    pub(crate) fn answered(&self) -> Option<Duration> {
        self.answered
    }

    /// Aborts every request in flight, keeping each slot's accounting: `Output::flush` at
    /// shutdown, or a cancelled `ack`.
    pub(crate) fn cancel(&mut self) {
        self.cancel_all();
    }

    fn position(&self, seq: SeqId) -> Option<usize> {
        self.slots.iter().position(|slot| slot.seq == seq)
    }

    fn slot(&self, seq: SeqId) -> Slot<O> {
        let mut accounting = BatchAccounting::sharing(&self.gate);
        accounting.observe_posture(self.posture);
        Slot { seq, accounting, state: State::Idle }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }
}

impl<O> Slots<O> {
    fn cancel_all(&mut self) {
        for slot in &mut self.slots {
            slot.state = State::Idle;
        }
        self.report();
    }

    fn report(&self) {
        let in_flight = self.slots.iter().filter(|slot| !matches!(slot.state, State::Idle)).count();
        self.telemetry.gauge(IN_FLIGHT, in_flight as f64, &[]);
    }
}

/// The marks a slot's error carries.
trait Marks {
    fn context_head_only(self) -> anyhow::Result<()>;
    fn context_fault(self, fault: Fault) -> anyhow::Result<()>;
}

impl Marks for anyhow::Result<()> {
    fn context_head_only(self) -> anyhow::Result<()> {
        self.map_err(|err| err.context(HeadOnly))
    }

    fn context_fault(self, fault: Fault) -> anyhow::Result<()> {
        self.map_err(|err| err.context(fault))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_pipeline::{classify, is_head_only};

    fn seq(n: u64) -> SeqId {
        SeqId { id: [7; 16], seq: n }
    }

    fn failed(fault: Fault) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("request failed").context(fault))
    }

    /// The task's outcome is the batch's result: `apply` turns it into `Ok` or a fault.
    async fn ack(slots: &mut Slots<anyhow::Result<()>>) -> anyhow::Result<()> {
        slots.ack(|outcome| outcome).await
    }

    fn submit(slots: &mut Slots<anyhow::Result<()>>, n: u64, outcome: Option<Fault>) {
        slots.submit(seq(n), |_| Ok(Some(async move { outcome.map_or(Ok(()), failed) }))).unwrap();
    }

    /// A retryable failure keeps the head's slot for its resubmit; a final one pops it. Both are
    /// marked `HeadOnly`, and a slot behind the head is answered in turn.
    #[tokio::test]
    async fn a_failed_head_keeps_its_slot_until_final_and_every_error_is_head_only() {
        let mut slots = Slots::new(CountGate::new());
        slots.set_concurrency(4);
        for n in 1..=3 {
            slots.observe(seq(n));
        }
        submit(&mut slots, 1, Some(Fault::Ambiguous));
        submit(&mut slots, 2, Some(Fault::Rejected));
        submit(&mut slots, 3, None);

        let err = ack(&mut slots).await.unwrap_err();
        assert!(is_head_only(&err) && classify(&err) == Fault::Ambiguous, "{err:#}");
        assert_eq!(slots.len(), 3, "a retryable fault keeps the head's slot");

        submit(&mut slots, 1, None);
        assert_eq!(slots.len(), 3, "the resubmit found the head's slot");
        ack(&mut slots).await.expect("the resubmitted head delivers");

        let err = ack(&mut slots).await.unwrap_err();
        assert!(is_head_only(&err) && classify(&err) == Fault::Rejected, "{err:#}");
        assert_eq!(slots.len(), 1, "a rejected slot leaves the window");
        ack(&mut slots).await.expect("the last slot delivers");
        assert_eq!(slots.len(), 0);
    }

    /// A unit the first attempt counted runs muted on the resubmit.
    #[tokio::test]
    async fn a_resubmitted_head_re_encodes_muted() {
        let gate = CountGate::new();
        let mut slots: Slots<anyhow::Result<()>> = Slots::new(gate.clone());
        slots.set_concurrency(2);
        slots.observe(seq(1));
        let mut seen = Vec::new();
        for outcome in [Some(Fault::Clean), None] {
            slots
                .submit(seq(1), |accounting| {
                    accounting.encode(0, || seen.push(gate.is_muted()));
                    Ok(Some(async move { outcome.map_or(Ok(()), failed) }))
                })
                .unwrap();
            let _ = ack(&mut slots).await;
        }
        assert_eq!(seen, [false, true]);
    }

    /// Under at-most-once an ambiguous head is final, so its slot leaves the window.
    #[tokio::test]
    async fn an_ambiguous_head_under_at_most_once_leaves_the_window() {
        let mut slots = Slots::new(CountGate::new());
        slots.set_concurrency(2);
        slots.observe_posture(DeliveryPosture::AtMostOnce);
        slots.observe(seq(1));
        slots.observe(seq(2));
        submit(&mut slots, 1, Some(Fault::Ambiguous));
        submit(&mut slots, 2, None);
        assert!(is_head_only(&ack(&mut slots).await.unwrap_err()));
        assert_eq!(slots.len(), 1);
        ack(&mut slots).await.unwrap();
    }

    /// A failed encode at the head is marked `HeadOnly`, and a final one leaves the window; behind
    /// the head the slot waits for its turn.
    #[tokio::test]
    async fn a_failed_prepare_is_head_only_and_leaves_only_at_the_front() {
        let mut slots: Slots<anyhow::Result<()>> = Slots::new(CountGate::new());
        slots.set_concurrency(2);
        slots.observe(seq(1));
        slots.observe(seq(2));
        submit(&mut slots, 1, None);
        let err = slots
            .submit(seq(2), |_| Err::<Option<std::future::Ready<_>>, _>(anyhow::anyhow!("x")))
            .unwrap_err();
        assert!(is_head_only(&err));
        assert_eq!(slots.len(), 2, "behind the head, the slot waits");
        ack(&mut slots).await.unwrap();
        let err = slots
            .submit(seq(2), |_| Err::<Option<std::future::Ready<_>>, _>(anyhow::anyhow!("x")))
            .unwrap_err();
        assert!(is_head_only(&err) && classify(&err) == Fault::Rejected);
        assert_eq!(slots.len(), 0, "at the front, a final failure leaves");
    }

    /// A cancelled `ack` aborts every task in flight.
    #[tokio::test]
    async fn a_cancelled_ack_aborts_every_task() {
        let mut slots: Slots<anyhow::Result<()>> = Slots::new(CountGate::new());
        slots.set_concurrency(2);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(2);
        for n in 1..=2 {
            slots.observe(seq(n));
            let tx = tx.clone();
            slots
                .submit(seq(n), |_| {
                    Ok(Some(async move {
                        let _held = tx;
                        std::future::pending::<()>().await;
                        Ok(())
                    }))
                })
                .unwrap();
        }
        drop(tx);
        let cancelled = tokio::time::timeout(std::time::Duration::from_millis(20), ack(&mut slots));
        assert!(cancelled.await.is_err(), "the tasks never answer");
        // Each task holds a sender; once both are aborted the channel closes.
        tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, rx.recv())
            .await
            .expect("every task was aborted");
        assert_eq!(slots.len(), 2, "the slots keep their accounting");
    }
}
