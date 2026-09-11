//! Codec traits every input/output implements against, plus the native logit-to-logit wire
//! format. See `docs/design/wire-protocol.md` for the framing and payload design.

pub mod buffer;
pub mod frame;
pub mod native;
pub mod otlp;

use logit_core::{Event, EventBatch, Resource};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("malformed input: {0}")]
    Malformed(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// Something this codec cannot represent at all, as opposed to [`CodecError::Malformed`]'s
    /// "this input violates the format." OTLP's own service RPCs (PR3) are the first caller: an
    /// `application/json` content type, or an unknown gRPC method name, is well-formed on the
    /// wire, just not something this codec speaks.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The input is a *valid prefix* of something this codec could decode -- the buffer simply
    /// ends before a complete frame does, and it doesn't have `needed` more bytes yet. Distinct
    /// from [`CodecError::Malformed`], which means the bytes present are provably wrong and more
    /// of them won't help.
    ///
    /// A stream or file reader needs that difference to decide what to do next: `Truncated` means
    /// "come back once more bytes exist" (a live socket, a `logit_in` connection) or "this is where
    /// a torn write ends, truncate here" (a durable buffer's segment file, a reader resuming
    /// mid-file), where `Malformed` means the reader should give up on this frame and resync past
    /// it instead (`crate::frame::resync`). `needed` is the additional byte count that would make
    /// the read succeed, when known. `frame::read_frame`/`FrameHeader::read` are the first callers,
    /// on a short header or a short body.
    #[error("truncated input: need {needed} more byte(s)")]
    Truncated { needed: usize },
}

/// Turns wire bytes into events sharing one [`Resource`]. Every input (statsd, syslog, OTLP, the
/// native protocol, ...) implements this against the same internal model -- see
/// `docs/design/data-model.md`.
pub trait Decoder {
    /// Decodes one datagram, appending its events to `out` rather than returning a fresh `Vec` --
    /// a caller accumulating across many datagrams (`logit_pipeline::BatchAccumulator`,
    /// `docs/adr/decoupled-listener-io.md`) can then reuse one buffer via `Vec::drain`
    /// instead of allocating and immediately discarding one per datagram
    /// (`docs/design/memory.md` §2).
    ///
    /// `received_at` is when the datagram was taken off the socket, not when this runs -- once a
    /// listener's own I/O is decoupled from its decode loop (ADR `decoupled-listener-io`), the two can diverge by the
    /// receive queue's own latency under backlog, and every emitted event's `timestamp` must be
    /// the former: this is what keeps a syslog/statsd event's `timestamp` meaning *receipt* time
    /// regardless of how far behind decode is running.
    fn decode_into(
        &mut self,
        bytes: bytes::Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<Arc<Resource>, CodecError>;

    /// Convenience wrapper over [`Decoder::decode_into`], stamping every event with the current
    /// time -- for a caller (a test, a benchmark) with no real "receipt" instant of its own to
    /// thread through. Production code always calls `decode_into` directly with the read half's
    /// own captured `received_at`; nothing in this crate or its callers uses this method on the
    /// hot path.
    fn decode(&mut self, bytes: bytes::Bytes) -> Result<EventBatch, CodecError> {
        let received_at = now_nanos();
        let mut events = Vec::new();
        let resource = self.decode_into(bytes, received_at, &mut events)?;
        Ok(EventBatch { resource, events })
    }
}

pub(crate) fn now_nanos() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64
}

/// Turns an [`EventBatch`] into wire bytes. The mirror of [`Decoder`]; every output implements
/// this.
pub trait Encoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<bytes::Bytes, CodecError>;
}

/// Which OTLP service a payload belongs to. `logit`'s `Event` doesn't split by signal -- one event
/// may carry a log, metrics, and a span at once (ADR `multi-payload-events`) -- but OTLP's wire protocol does: logs,
/// metrics, and traces are three separate RPCs/URLs with three separate message types. A `Signal`
/// names which one a given payload of bytes belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Logs,
    Metrics,
    Traces,
}

impl Signal {
    /// The OTLP/HTTP path this signal is POSTed to, e.g. `/v1/traces`. Doubles as the `signal` tag
    /// value on any telemetry a caller attaches to a per-signal operation.
    pub fn path(self) -> &'static str {
        match self {
            Signal::Logs => "/v1/logs",
            Signal::Metrics => "/v1/metrics",
            Signal::Traces => "/v1/traces",
        }
    }

    /// The fully qualified gRPC method name for this signal's `Export` RPC.
    pub fn grpc_method(self) -> &'static str {
        match self {
            Signal::Logs => "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            Signal::Metrics => "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
            Signal::Traces => "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Metrics => "metrics",
            Signal::Traces => "traces",
        }
    }
}

/// An encoder whose wire format splits one [`EventBatch`] across several payloads, one per
/// [`Signal`] -- [`Encoder`] doesn't fit here since OTLP has no single message type an
/// [`EventBatch`] maps onto. Additive: [`Decoder`]/[`Encoder`] are unchanged, and every codec that
/// already implements them (the native format, statsd, syslog) is untouched.
pub trait SignalEncoder {
    /// Encodes `batch` into zero or more `(Signal, bytes)` payloads. Only non-empty signals
    /// appear -- an event batch with no metrics produces no `Signal::Metrics` payload -- and an
    /// entirely empty batch yields none at all, never an empty OTLP request.
    fn encode_signals(
        &mut self,
        batch: &EventBatch,
    ) -> Result<Vec<(Signal, bytes::Bytes)>, CodecError>;
}

/// The mirror of [`SignalEncoder`]. Returns several batches, not one: a single OTLP request can
/// carry data from N distinct `Resource*` entries, and an [`EventBatch`] holds exactly one
/// `Arc<Resource>` -- collapsing every entry under the first would silently mislabel the rest.
///
/// **OTLP/JSON decoding (`otlp::OtlpDecoder::decode_signal_json`) is deliberately not on this
/// trait.** `OtlpDecoder` is this trait's only implementor, and nothing in the crate is generic
/// over `SignalDecoder` -- every call site already holds a concrete `OtlpDecoder`. A trait method
/// would need either a default body (silently giving any future implementor "JSON unsupported"
/// with no compile error to catch it) or forcing every implementor to answer a question that's
/// only meaningful for OTLP in the first place. An inherent method costs nothing today and adds
/// friction only if a second `SignalDecoder` ever needs the same asymmetry solved for real.
pub trait SignalDecoder {
    fn decode_signal(
        &mut self,
        signal: Signal,
        bytes: bytes::Bytes,
    ) -> Result<Vec<EventBatch>, CodecError>;
}
