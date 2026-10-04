//! The `Output` trait, plus [`Fault`]/[`DeliveryPosture`]/[`is_retryable`]: the classification
//! and policy the generic writer (`crate::runtime::write_loop`) uses to decide whether a failed
//! `send` is worth retrying. See `docs/adr/buffered-sink-delivery.md` and
//! `docs/adr/sink-fault-classes.md`.

use crate::fanout::BatchContext;
use logit_core::EventBatch;
use logit_proto::native::SeqId;

/// A sink component: takes batches and delivers them somewhere. It has at least one source and is
/// never a source itself (`docs/design/pipeline-graph.md`'s arity table).
///
/// Buffering is the runtime's job: `run_output` splits into a drain half and a writer half joined
/// around a [`crate::SinkQueue`], so the inbox keeps draining while a slow or backing-off delivery
/// is in flight (`docs/adr/buffered-sink-delivery.md`). `send` sees one batch at a time. A sink
/// whose [`Output::window`] is above 1 has several batches in flight at once through
/// [`Output::submit`] and [`Output::await_ack`] (`docs/adr/native-hop-send-window.md`, decision
/// 4).
///
/// `send` takes `&EventBatch` so `run_output` can hand a `Delivered::Shared` branch through by
/// reference, with no `Arc::try_unwrap` or clone however many sibling branches share it
/// (`docs/adr/arc-eventbatch-copy-on-write.md`).
///
/// Retry is the runtime's job too. `send` is a single attempt that reports what a failure means
/// via [`Fault`] (`.context(fault)` on the returned error); `write_loop` owns retry timing and
/// the retry-or-drop decision, from [`is_retryable`] and the resolved
/// [`DeliveryPosture`]: `buffer.delivery` when the operator set it, else
/// [`Output::default_posture`]. A sink runs no retry loop of its own. Inside one attempt it may
/// resend, bounded, on a verdict that proves the resend safe, or poll, bounded, for a verdict:
/// - the pooled-stream driver's one reconnect after a plaintext first write that accepted nothing
///   (`PooledStream::send` in `logit-outputs`);
/// - `statsd_out`'s Unix datagram socket, which reconnects and resends once when a batch's first
///   datagram finds an inherited socket's receiver gone (`UnixDest::send` in `logit-outputs`);
/// - `splunk_hec_out`'s resend of the rest of a body after a code 6 dropped one of its objects
///   (`SplunkHecOutput::send_once`);
/// - `splunk_hec_out`'s one split of a body Splunk Cloud answered as over its cap, each half sent
///   once (`SplunkHecOutput::send_body`);
/// - `otlp_out`, `datadog_out`, and `datadog_trace_out` go on to the next request when one is
///   answered with a [`Fault::Rejected`] verdict, count its records
///   `records.dropped{reason="rejected"}`, and return `Ok` if any request was accepted. A
///   `Refused`, a `Clean`, or an `Ambiguous` verdict stops the send
///   (`docs/adr/delivery-semantics.md`, "Amendment: per-request verdicts (2026-10-04)");
/// - `splunk_hec_out`'s `/ack` poll under `ack: true`, until every id is acknowledged or
///   `ack_timeout` passes (`SplunkHecOutput::await_acks`).
#[async_trait::async_trait]
pub trait Output {
    /// Opens whatever this sink must open before it can serve anything: a listening socket, in
    /// practice. `crate::runtime::run_with_telemetry` calls it for every output, in sorted id
    /// order, in the pre-spawn pass that binds every input ([`crate::Input::bind`]), so an address
    /// in use fails startup with nothing else running.
    ///
    /// The default is a no-op, right for a sink that only connects outward: `write_loop`'s retry
    /// owns the "destination isn't up yet" case. A listening sink (`prometheus_out`) overrides it
    /// (`docs/adr/prometheus-scrape-and-exposition.md`, "`Output::bind`").
    ///
    /// Two obligations on an override, the same as [`crate::Input::bind`]'s:
    /// - **Idempotent.** A second call must return `Ok(())` without re-opening.
    /// - **`send`/`flush` must still work if nobody called this first.** `run_output` calls
    ///   `bind` itself before opening the sink's store, so a caller outside the node runtime (a
    ///   direct unit test) needs only one call.
    async fn bind(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// One delivery attempt at `batch`. `write_loop` calls it once per attempt and owns the retry
    /// (the trait doc). The contract:
    ///
    /// - **Cancellable at every await.** Each attempt races the shutdown grace, so the future can
    ///   be dropped at any await; the sink must stay usable for the next call (`docs/design/pipeline-graph.md`'s "Cancellation points").
    /// - **The encode is synchronous**, with no await inside it, so a sink's per-batch accounting
    ///   holds across it (`docs/adr/sink-send-path-and-attempt-accounting.md`, decision 2).
    /// - **A failure carries a [`Fault`]**; one with none classifies `Fault::Rejected`
    ///   ([`classify`]).
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()>;

    /// Called once per batch by `write_loop`, before the batch's first attempt or submission,
    /// never between its retries or resubmissions: the place a sink resets per-batch state. A
    /// sink that saves the value for `send` sees the same one on every attempt at the batch. `BatchContext` is the trace/span id plus
    /// which component created and last handled the batch
    /// (`docs/adr/batch-provenance-on-delivered.md`); `logit_out` threads it across the wire, and
    /// a sink with encode-side counters arms its once-per-batch accounting here
    /// (`docs/adr/sink-send-path-and-attempt-accounting.md`, decision 2). A caller outside the
    /// runtime that never calls it gets every `send` counted. Default no-op, so a type that
    /// implements `Output` by delegating to another must forward this too, or the inner sink's
    /// accounting never arms (`prometheus_out`'s `PrometheusOutput`).
    ///
    /// `seq` is the batch's native-hop sender identity and number from the sink's store, which
    /// numbers every batch; only `logit_out` reads it, and it never rides on `BatchContext`
    /// (`docs/adr/native-hop-identity-and-sequence.md`). A caller outside the runtime must
    /// observe again before each new batch, or the next batch goes out under the last one's
    /// number and reads as a resend.
    fn observe_batch(&mut self, ctx: BatchContext, seq: SeqId) {
        let _ = (ctx, seq);
    }

    /// How many batches `write_loop` may have submitted and unacknowledged at once. The value
    /// may change after a connection is made, so `write_loop` reads it before every submission.
    /// The default of 1 keeps a sink on `send`, one batch per attempt: `write_loop` calls
    /// `submit`/`await_ack` only while this reads above 1, or while batches it observed are
    /// still unacknowledged. A type that implements `Output` by delegating to another must
    /// forward this, [`Output::submit`], and [`Output::await_ack`].
    ///
    /// **A sink that reports a window above 1 bounds itself.** `write_loop` applies the head's
    /// remaining retry budget only to a submit with nothing in flight. Every `submit` past the
    /// head and every `await_ack` runs under no time limit from the loop, so the sink must bound
    /// each one on its own (a progress bound on a write, a request timeout on an ack wait), or a
    /// stalled peer holds the sink until shutdown.
    fn window(&self) -> usize {
        1
    }

    /// Writes `batch` without waiting for its delivery, under a window above 1
    /// (`docs/adr/native-hop-send-window.md`, decision 4). `ctx` and `seq` are the batch's, as
    /// [`Output::observe_batch`] last saw them for it; a resubmitted batch isn't observed again.
    ///
    /// A failure with nothing in flight is this batch's own, classified as a `send` failure is.
    /// A failure with batches in flight is never classified: `write_loop` stops submitting and
    /// reads the acknowledgments already owed through [`Output::await_ack`]. Cancellable at every
    /// await, as `send` is; a cancelled `submit` means the sink dropped its connection and nothing
    /// is outstanding. The default calls `send`.
    async fn submit(
        &mut self,
        batch: &EventBatch,
        ctx: BatchContext,
        seq: SeqId,
    ) -> anyhow::Result<()> {
        let _ = (ctx, seq);
        self.send(batch).await
    }

    /// Waits for the oldest outstanding submission's acknowledgment: `Ok` means that batch was
    /// delivered. Every `Err`, and every cancelled `await_ack`, means the sink dropped its
    /// connection and nothing is outstanding, so `write_loop` submits again from the oldest
    /// unacknowledged batch. The `Err` carries a [`Fault`], as a `send` failure does. The default
    /// returns `Ok`, matching the default `submit`, which has already delivered.
    async fn await_ack(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called once, when the sink's input has closed and `write_loop` has stopped, for a sink
    /// that holds unwritten data at shutdown (`docs/adr/buffered-sink-delivery.md`'s
    /// shutdown-grace section). That isn't always after every batch was delivered or dropped: on a
    /// shutdown-grace expiry, a disk-buffered sink keeps its undelivered batches spooled for the
    /// next run. Default no-op, for a sink with nothing buffered internally.
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// The posture `write_loop` uses when the operator's `buffer.delivery` is unset: whether an
    /// attempt whose outcome is unknown ([`Fault::Ambiguous`]) is retried. The default is
    /// `AtLeastOnce` for every sink (`docs/adr/delivery-semantics.md`, item 5). A sink whose wire
    /// gives a resend no identity at its destination declares `AtMostOnce` (`statsd_out`). A type
    /// that implements `Output` by delegating to another must forward this too.
    fn default_posture(&self) -> DeliveryPosture {
        DeliveryPosture::AtLeastOnce
    }
}

/// What a `send` failure says about the batch and the destination. Only the sink can tell, so it
/// travels out of `send` as `anyhow` context (`.context(Fault::Ambiguous)`), read back by
/// [`classify`]. [`is_retryable`] turns a class and a posture into the retry decision. See
/// `docs/adr/sink-fault-classes.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// The destination never saw the batch (connect refused, DNS failure). Retried under either
    /// posture.
    Clean,
    /// The batch may have been applied before the response was lost (timeout, `5xx`, `429`).
    /// Retried only under `DeliveryPosture::AtLeastOnce`.
    Ambiguous,
    /// The destination refused this batch for its own content (a malformed body, an oversize
    /// payload); a resend can't succeed. Never retried: `write_loop` drops it at once, so
    /// nothing behind it waits.
    Rejected,
    /// The destination refuses every batch for now (credentials, an unknown tenant or bucket, a
    /// protocol mismatch), and nothing was applied. Retried under either posture until it
    /// succeeds or shutdown cuts it, so the head holds while the operator or the destination
    /// fixes the cause.
    Refused,
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Fault::Clean => "clean",
            Fault::Ambiguous => "ambiguous",
            Fault::Rejected => "rejected",
            Fault::Refused => "refused",
        };
        f.write_str(s)
    }
}

/// Reads `err` for an attached [`Fault`] marker (a sink's `.context(fault)`), defaulting to
/// [`Fault::Rejected`] when none is found: a failure the sink didn't recognize is never retried.
/// The default is a retry decision, not a claim about the destination.
///
/// Not `err.chain().find_map(|e| e.downcast_ref::<Fault>())`: each link's concrete type is
/// anyhow's internal `ContextError<Fault, _>`, so `dyn Error::downcast_ref` never matches and
/// every error would classify `Rejected`. The inherent `anyhow::Error::downcast_ref` looks
/// inside its context wrappers, through any further `.context(...)` layers stacked on top (such
/// as `write_loop`'s `component '{id}'`).
pub fn classify(err: &anyhow::Error) -> Fault {
    err.downcast_ref::<Fault>().copied().unwrap_or(Fault::Rejected)
}

/// Whether a sink retries an attempt whose outcome is unknown, accepting that the destination may
/// receive the batch twice. Decides which [`Fault`]s are retried (see [`is_retryable`]). The
/// runtime's default is `AtLeastOnce`; a sink may declare another through
/// [`Output::default_posture`], and `buffer.delivery` overrides either per component
/// (`logit_config::BufferConfig::delivery`, resolved into `WriteLoopConfig::delivery_override` by
/// `logit-cli::pipeline::write_config`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPosture {
    AtLeastOnce,
    AtMostOnce,
}

/// Whether `fault` is worth retrying under `posture` (`docs/adr/sink-fault-classes.md`, "Four
/// classes"):
///
/// | `Fault` | `AtMostOnce` | `AtLeastOnce` |
/// |---|---|---|
/// | `Clean` | retry | retry |
/// | `Ambiguous` | no retry | retry |
/// | `Rejected` | no retry | no retry |
/// | `Refused` | retry | retry |
///
/// `Clean` and `Refused` applied nothing, so a resend can't duplicate. `Ambiguous` retries only
/// once duplicates are tolerable (`AtLeastOnce`). `Rejected` would get the same answer again.
pub fn is_retryable(fault: Fault, posture: DeliveryPosture) -> bool {
    match (fault, posture) {
        (Fault::Clean | Fault::Refused, _) => true,
        (Fault::Ambiguous, DeliveryPosture::AtLeastOnce) => true,
        (Fault::Ambiguous, DeliveryPosture::AtMostOnce) => false,
        (Fault::Rejected, _) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins all 8 `(Fault, DeliveryPosture)` combinations of the ADR's table.
    #[test]
    fn is_retryable_matches_the_adr_table_exhaustively() {
        use DeliveryPosture::*;
        use Fault::*;

        let table = [
            (Clean, AtMostOnce, true),
            (Clean, AtLeastOnce, true),
            (Ambiguous, AtMostOnce, false),
            (Ambiguous, AtLeastOnce, true),
            (Rejected, AtMostOnce, false),
            (Rejected, AtLeastOnce, false),
            (Refused, AtMostOnce, true),
            (Refused, AtLeastOnce, true),
        ];
        for (fault, posture, retry) in table {
            assert_eq!(is_retryable(fault, posture), retry, "{fault} under {posture:?}");
        }
    }

    #[test]
    fn the_class_strings_are_the_telemetry_tag_values() {
        assert_eq!(Fault::Rejected.to_string(), "rejected");
        assert_eq!(Fault::Refused.to_string(), "refused");
    }

    #[test]
    fn a_sink_that_declares_nothing_defaults_to_at_least_once() {
        struct Bare;

        #[async_trait::async_trait]
        impl Output for Bare {
            async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
                Ok(())
            }
        }

        assert_eq!(Bare.default_posture(), DeliveryPosture::AtLeastOnce);
    }

    #[test]
    fn classify_reads_back_a_fault_attached_via_context() {
        let err = anyhow::anyhow!("boom").context(Fault::Ambiguous);
        assert_eq!(classify(&err), Fault::Ambiguous);
    }

    #[test]
    fn classify_defaults_to_rejected_for_an_unclassified_error() {
        let err = anyhow::anyhow!("boom, no fault attached");
        assert_eq!(classify(&err), Fault::Rejected);
    }

    #[test]
    fn classify_finds_a_fault_attached_underneath_further_context() {
        // write_loop layers more context on top of the sink's `Fault`.
        let err = anyhow::anyhow!("boom").context(Fault::Clean).context("component 'out'");
        assert_eq!(classify(&err), Fault::Clean);
    }
}
