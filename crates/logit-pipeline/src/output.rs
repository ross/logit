//! The `Output` trait, plus [`Fault`]/[`DeliveryPosture`]/[`is_retryable`]: the classification
//! and policy the generic writer (`crate::runtime::write_loop`) uses to decide whether a failed
//! `send` is worth retrying. See `docs/adr/buffered-sink-delivery.md`.

use crate::fanout::BatchContext;
use logit_core::EventBatch;
use logit_proto::native::SeqId;

/// A sink component: takes batches and delivers them somewhere. It has at least one source and is
/// never a source itself (`docs/design/pipeline-graph.md`'s arity table).
///
/// Buffering is the runtime's job: `run_output` splits into a drain half and a writer half joined
/// around a [`crate::SinkQueue`], so the inbox keeps draining while a slow or backing-off delivery
/// is in flight (`docs/adr/buffered-sink-delivery.md`). `send` sees one batch at a time.
///
/// `send` takes `&EventBatch` so `run_output` can hand a `Delivered::Shared` branch through by
/// reference, with no `Arc::try_unwrap` or clone however many sibling branches share it
/// (`docs/adr/arc-eventbatch-copy-on-write.md`).
///
/// Retry is the runtime's job too. `send` is a single attempt that reports what a failure means
/// via [`Fault`] (`.context(fault)` on the returned error); `write_loop` owns retry timing,
/// budget, and the retryable/permanent decision, from [`is_retryable`] and the resolved
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
    /// - **Cancellable at every await.** Each attempt races the rest of the retry budget and the
    ///   shutdown grace, so the future can be dropped at any await; the sink must stay usable for
    ///   the next call (`docs/design/pipeline-graph.md`'s "Cancellation points").
    /// - **The encode is synchronous**, with no await inside it, so a sink's per-batch accounting
    ///   holds across it (`docs/adr/sink-send-path-and-attempt-accounting.md`, decision 2).
    /// - **A failure carries a [`Fault`]**; one with none classifies `Fault::Permanent`
    ///   ([`classify`]).
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()>;

    /// Called once per batch by `write_loop`, before the batch's first attempt, never between its
    /// retries: the place a sink resets per-batch state. A sink that saves the value for `send`
    /// sees the same one on every attempt at the batch. `BatchContext` is the trace/span id plus
    /// which component created and last handled the batch
    /// (`docs/adr/batch-provenance-on-delivered.md`); `logit_out` threads it across the wire, and
    /// a sink with encode-side counters arms its once-per-batch accounting here
    /// (`docs/adr/sink-send-path-and-attempt-accounting.md`, decision 2). A caller outside the
    /// runtime that never calls it gets every `send` counted. Default no-op, so a type that
    /// implements `Output` by delegating to another must forward this too, or the inner sink's
    /// accounting never arms (`prometheus_out`'s `PrometheusOutput`).
    ///
    /// `seq` is the batch's native-hop sender identity and number from the sink's store; only
    /// `logit_out` reads it, and it never rides on `BatchContext`
    /// (`docs/adr/native-hop-identity-and-sequence.md`). A caller outside the runtime that
    /// passes `Some` must observe again before each new batch, or the next batch goes out under
    /// the last one's number and reads as a resend.
    fn observe_batch(&mut self, ctx: BatchContext, seq: Option<SeqId>) {
        let _ = (ctx, seq);
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

/// What a `send` failure says about whether the destination received the batch. Only the sink
/// can tell, so it travels out of `send` as `anyhow` context (`.context(Fault::Ambiguous)`), read
/// back by [`classify`]. See `docs/adr/buffered-sink-delivery.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// The destination provably never saw the batch (connect refused, DNS failure). Safe to retry
    /// under any delivery posture.
    Clean,
    /// The batch may have been committed before the response was lost (timeout, 5xx, 429).
    /// Retried only under `DeliveryPosture::AtLeastOnce`.
    Ambiguous,
    /// A configuration error (a 4xx other than 429). Never retried.
    Permanent,
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Fault::Clean => "clean",
            Fault::Ambiguous => "ambiguous",
            Fault::Permanent => "permanent",
        };
        f.write_str(s)
    }
}

/// Reads `err` for an attached [`Fault`] marker (a sink's `.context(fault)`), defaulting to
/// [`Fault::Permanent`] when none is found: never retry a failure the sink didn't recognize.
///
/// Not `err.chain().find_map(|e| e.downcast_ref::<Fault>())`: each link's concrete type is
/// anyhow's internal `ContextError<Fault, _>`, so `dyn Error::downcast_ref` never matches and
/// every error would classify `Permanent`. The inherent `anyhow::Error::downcast_ref` looks
/// inside its context wrappers, through any further `.context(...)` layers stacked on top (such
/// as `write_loop`'s `component '{id}'`).
pub fn classify(err: &anyhow::Error) -> Fault {
    err.downcast_ref::<Fault>().copied().unwrap_or(Fault::Permanent)
}

/// Whether `err` carries an explicit `Fault::Permanent` marker from the sink, as opposed to
/// [`classify`]'s default when no `Fault` is attached. Only an explicit marker counts toward
/// `write_loop`'s sustained-permanent-failure exit window: an unclassified error (`StreamOutput`'s
/// bare I/O errors, say a full disk) is non-retryable but is not a positively identified
/// configuration error (a bad token) that should end the process.
pub fn is_explicitly_permanent(err: &anyhow::Error) -> bool {
    matches!(err.downcast_ref::<Fault>(), Some(Fault::Permanent))
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

/// Whether `fault` is worth retrying under `posture` (`docs/adr/buffered-sink-delivery.md`'s
/// table):
///
/// | `Fault` | `AtMostOnce` | `AtLeastOnce` |
/// |---|---|---|
/// | `Clean` | retry | retry |
/// | `Ambiguous` | no retry | retry |
/// | `Permanent` | no retry | no retry |
///
/// `Clean` never reached the destination, so there is nothing to duplicate. `Ambiguous` retries
/// only once duplicates are tolerable (`AtLeastOnce`). `Permanent` is a configuration error, not
/// a transient condition.
pub fn is_retryable(fault: Fault, posture: DeliveryPosture) -> bool {
    match (fault, posture) {
        (Fault::Clean, _) => true,
        (Fault::Ambiguous, DeliveryPosture::AtLeastOnce) => true,
        (Fault::Ambiguous, DeliveryPosture::AtMostOnce) => false,
        (Fault::Permanent, _) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins all 6 `(Fault, DeliveryPosture)` combinations of the ADR's table.
    #[test]
    fn is_retryable_matches_the_adr_table_exhaustively() {
        use DeliveryPosture::*;
        use Fault::*;

        assert!(is_retryable(Clean, AtMostOnce), "Clean should retry under AtMostOnce");
        assert!(is_retryable(Clean, AtLeastOnce), "Clean should retry under AtLeastOnce");
        assert!(
            !is_retryable(Ambiguous, AtMostOnce),
            "Ambiguous should NOT retry under AtMostOnce"
        );
        assert!(is_retryable(Ambiguous, AtLeastOnce), "Ambiguous SHOULD retry under AtLeastOnce");
        assert!(
            !is_retryable(Permanent, AtMostOnce),
            "Permanent should never retry under AtMostOnce"
        );
        assert!(
            !is_retryable(Permanent, AtLeastOnce),
            "Permanent should never retry under AtLeastOnce"
        );
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
    fn classify_defaults_to_permanent_for_an_unclassified_error() {
        let err = anyhow::anyhow!("boom, no fault attached");
        assert_eq!(classify(&err), Fault::Permanent);
    }

    #[test]
    fn an_unclassified_error_is_never_explicitly_permanent() {
        // classify()'s default is a retry decision, not a positively identified configuration
        // error, so it must not count toward write_loop's permanent-failure exit window.
        let err = anyhow::anyhow!("boom, no fault attached");
        assert_eq!(classify(&err), Fault::Permanent, "still non-retryable by default");
        assert!(!is_explicitly_permanent(&err), "but not an explicit classification");
    }

    #[test]
    fn a_sink_that_explicitly_classifies_permanent_is_explicitly_permanent() {
        let err = anyhow::anyhow!("bad token").context(Fault::Permanent);
        assert!(is_explicitly_permanent(&err));
    }

    #[test]
    fn clean_and_ambiguous_are_never_explicitly_permanent() {
        assert!(!is_explicitly_permanent(&anyhow::anyhow!("x").context(Fault::Clean)));
        assert!(!is_explicitly_permanent(&anyhow::anyhow!("x").context(Fault::Ambiguous)));
    }

    #[test]
    fn classify_finds_a_fault_attached_underneath_further_context() {
        // write_loop layers more context on top of the sink's `Fault`.
        let err = anyhow::anyhow!("boom").context(Fault::Clean).context("component 'out'");
        assert_eq!(classify(&err), Fault::Clean);
    }
}
