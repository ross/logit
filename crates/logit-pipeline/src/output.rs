//! The `Output` trait -- moved here from `logit-outputs`, same reasoning as [`crate::input`]. Also
//! home to [`Fault`]/[`DeliveryPosture`]/[`is_retryable`], the classification and policy pieces the
//! generic writer (`crate::runtime::write_loop`) needs to decide whether a failed `send` is worth
//! retrying. See `docs/adr/buffered-sink-delivery.md`.

use logit_core::EventBatch;

/// A sink component: takes batches and delivers them somewhere. Buffering between the pipeline
/// and delivery is the **runtime's** responsibility, not this trait's -- `run_output`
/// (`runtime.rs`) splits into a drain half and a writer half joined around a `SinkQueue`
/// (`queue.rs`), so a sink's own inbox keeps draining while a slow or backing-off delivery
/// attempt is in flight. See `docs/adr/buffered-sink-delivery.md`. `Output::send` itself
/// only ever sees one batch at a time, exactly as before this existed. A sink has at least one
/// source and is never itself a source of anything else (`docs/design/pipeline-graph.md`'s arity
/// table).
///
/// Takes `&EventBatch`, not an owned one -- a sink only ever reads a batch to encode/write it, and
/// this is the half of `docs/adr/arc-eventbatch-copy-on-write.md`'s copy-on-write design that
/// actually realizes the fan-out saving: `run_output` (`runtime.rs`) can hand a `Delivered::Shared`
/// branch straight through as a reference, with no `Arc::try_unwrap`/clone ever needed for a
/// read-only `Output` consumer, regardless of how many sibling branches still hold their own
/// handle to the same batch.
///
/// **Retry is not this trait's job either.** A sink implements `send` as a single attempt and
/// reports what a failure means via [`Fault`] (`.context(fault)` on the returned error); the
/// generic writer (`crate::runtime::write_loop`) owns retry timing, budget, and the
/// retryable/permanent decision, driven by [`is_retryable`] and this sink's [`Output::duplicate_safe`].
/// This is what every sink gets retry for free, rather than reimplementing its own loop --
/// `InfluxDbOutput` used to have one; it doesn't any more.
#[async_trait::async_trait]
pub trait Output {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()>;

    /// Called once after the last batch has been delivered or dropped and no more will follow.
    /// Default no-op -- most sinks (e.g. `InfluxDbOutput`, which writes synchronously with nothing
    /// buffered internally) need nothing here; this exists for a sink that does. Closes ADR `service-lifecycle-and-output-retry`'s
    /// residual "no `Output` close hook" gap, now load-bearing since a sink can hold unwritten data
    /// at shutdown (`docs/adr/buffered-sink-delivery.md`'s shutdown-grace section).
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Whether re-delivering an already-delivered batch is safe for this destination. Drives the
    /// default delivery posture (see [`DeliveryPosture`]); config can still override it per
    /// component (`logit_config::BufferConfig::delivery`, resolved in `write_loop`). Default
    /// `false` -- the safer default for a sink that hasn't opted in.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

/// What a `send` failure means about whether the destination actually received the batch -- only
/// the sink can tell, so it travels back out of `send` as `anyhow` context rather than a signature
/// change (`.context(Fault::Ambiguous)`, read back via [`classify`] -- see that function's doc
/// comment for exactly how). See `docs/adr/buffered-sink-delivery.md`.
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
/// [`Fault::Permanent`] when none is found -- never retry a failure the sink didn't recognize
/// (`docs/adr/buffered-sink-delivery.md`).
///
/// **Not** `err.chain().find_map(|e| e.downcast_ref::<Fault>())`, even though that's the more
/// obvious-looking spelling: each link `chain()` yields is a `&dyn std::error::Error` whose
/// *concrete* type is anyhow's own internal `ContextError<Fault, _>` wrapper, not `Fault` itself,
/// so the standard `dyn Error::downcast_ref::<Fault>()` never matches at any link and this would
/// silently always fall through to `Permanent`. `anyhow::Error::downcast_ref` is a different,
/// anyhow-specific inherent method (not the `std::error::Error` trait method) that knows how to
/// look inside its own context wrapper -- and recurses through further `.context(...)` layers
/// stacked on top, so this still finds `Fault` even if a caller later adds more context (e.g.
/// `write_loop`'s own `.with_context(|| format!("component '{id}'"))`).
pub fn classify(err: &anyhow::Error) -> Fault {
    err.downcast_ref::<Fault>().copied().unwrap_or(Fault::Permanent)
}

/// Whether `err` carries an **explicit** `Fault::Permanent` marker from the sink itself, as
/// opposed to [`classify`]'s conservative default when no `Fault` was found at all. Only an
/// explicit classification should ever count toward `write_loop`'s sustained-permanent-failure
/// exit window -- an unclassified error (a sink that hasn't opted into `Fault` at all, e.g.
/// `StdioOutput`'s bare I/O errors) is correctly treated as non-retryable by [`classify`]'s
/// default, but must never be mistaken for a *positively identified* configuration error that
/// should eventually end the process. A `StdioOutput` hitting a transient disk-full condition
/// forever is a very different situation from `InfluxDbOutput` hitting a bad token forever, and
/// only the latter should ever trip that window.
pub fn is_explicitly_permanent(err: &anyhow::Error) -> bool {
    matches!(err.downcast_ref::<Fault>(), Some(Fault::Permanent))
}

/// Whether re-delivering an already-delivered batch is an acceptable risk for a sink's
/// destination. Drives which [`Fault`]s are worth retrying (see [`is_retryable`]); config can
/// override the derived default per component (`logit_config::BufferConfig::delivery`, resolved
/// into `WriteLoopConfig::delivery_override` by `logit-cli::pipeline::write_config`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPosture {
    AtLeastOnce,
    AtMostOnce,
}

impl DeliveryPosture {
    /// The default posture derived purely from [`Output::duplicate_safe`]: `true` means
    /// re-delivery is an acceptable risk, so retry as aggressively as fault classification allows
    /// (`AtLeastOnce`); `false` is the conservative default (`AtMostOnce`).
    pub fn from_duplicate_safe(duplicate_safe: bool) -> Self {
        if duplicate_safe {
            DeliveryPosture::AtLeastOnce
        } else {
            DeliveryPosture::AtMostOnce
        }
    }
}

/// Whether `fault` is worth retrying under `posture` -- the crux of the whole duplicate-safety
/// argument (`docs/adr/buffered-sink-delivery.md`'s table, reproduced here):
///
/// | `Fault` | `AtMostOnce` | `AtLeastOnce` |
/// |---|---|---|
/// | `Clean` | retry | retry |
/// | `Ambiguous` | no retry | retry |
/// | `Permanent` | no retry | no retry |
///
/// `Clean` is always safe to retry under either posture, since the destination never actually saw
/// the batch in the first place -- there is nothing to duplicate. `Ambiguous` is only safe once
/// the sink itself has said duplicates are tolerable (`AtLeastOnce`). `Permanent` is a
/// configuration error, not a transient condition, so it is never retried regardless of posture.
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

    /// Exhaustive, all 6 `(Fault, DeliveryPosture)` combinations -- this table is the crux of the
    /// whole duplicate-safety argument (`docs/adr/buffered-sink-delivery.md`), so it's pinned
    /// directly rather than trusted to a handful of spot checks.
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
        // classify()'s default-to-Permanent is a retry decision, not a claim that the sink
        // positively identified a configuration error -- an unclassified error must not count
        // toward write_loop's sustained-permanent-failure exit window.
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
        // A real call site may layer more context on top (e.g. `with_context(|| "component
        // 'out'")`) after the sink itself attaches its `Fault` -- `classify` has to walk the whole
        // chain, not just look at the outermost layer.
        let err = anyhow::anyhow!("boom").context(Fault::Clean).context("component 'out'");
        assert_eq!(classify(&err), Fault::Clean);
    }
}
