//! The `Output` trait, plus [`Fault`]/[`DeliveryPosture`]/[`is_retryable`]: the classification
//! and policy the generic writer (`crate::runtime::write_loop`) uses to decide whether a failed
//! `send` is worth retrying. See `docs/adr/buffered-sink-delivery.md`.

use crate::fanout::BatchContext;
use logit_core::EventBatch;

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
/// budget, and the retryable/permanent decision, from [`is_retryable`] and
/// [`Output::duplicate_safe`]. A sink never runs its own retry loop.
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

    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()>;

    /// Called immediately before each delivery attempt in `write_loop`, retries included, so a
    /// sink that saves the value for `send` sees the same one on every attempt at one batch.
    /// `BatchContext` is the trace/span id plus which component created and last handled the
    /// batch (`docs/adr/batch-provenance-on-delivered.md`); `logit_out` threads it across the
    /// wire. Default no-op.
    fn observe_batch(&mut self, ctx: BatchContext) {
        let _ = ctx;
    }

    /// Called once after the last batch has been delivered or dropped and no more will follow,
    /// for a sink that holds unwritten data at shutdown (`docs/adr/buffered-sink-delivery.md`'s
    /// shutdown-grace section). Default no-op, for a sink with nothing buffered internally.
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Whether re-delivering an already-delivered batch is safe for this destination. Drives the
    /// default [`DeliveryPosture`]; config can override it per component
    /// (`logit_config::BufferConfig::delivery`). Defaults to `false`, the safe choice for a sink
    /// that hasn't opted in.
    fn duplicate_safe(&self) -> bool {
        false
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

/// Whether re-delivering an already-delivered batch is an acceptable risk for a sink's
/// destination. Decides which [`Fault`]s are retried (see [`is_retryable`]). Config can override
/// the derived default per component (`logit_config::BufferConfig::delivery`, resolved into
/// `WriteLoopConfig::delivery_override` by `logit-cli::pipeline::write_config`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPosture {
    AtLeastOnce,
    AtMostOnce,
}

impl DeliveryPosture {
    /// The default posture from [`Output::duplicate_safe`]: `true` gives `AtLeastOnce`, `false`
    /// gives `AtMostOnce`.
    pub fn from_duplicate_safe(duplicate_safe: bool) -> Self {
        if duplicate_safe {
            DeliveryPosture::AtLeastOnce
        } else {
            DeliveryPosture::AtMostOnce
        }
    }
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
    fn delivery_posture_from_duplicate_safe_maps_true_to_at_least_once() {
        assert_eq!(DeliveryPosture::from_duplicate_safe(true), DeliveryPosture::AtLeastOnce);
        assert_eq!(DeliveryPosture::from_duplicate_safe(false), DeliveryPosture::AtMostOnce);
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
