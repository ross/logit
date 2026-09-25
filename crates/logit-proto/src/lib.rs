//! The codec traits and every wire codec, including the native logit-to-logit format.
//!
//! A new protocol implements [`Decoder`] on the listener side and one of three encoder shapes on
//! the sink side ([ADR `framed-encoder`](../../../docs/adr/framed-encoder.md) says why three):
//! [`Encoder`] (one blob per batch), [`FramedEncoder`] (one framed message per record into a
//! [`MessageBuf`], with per-message drop accounting), or [`SignalEncoder`] (one payload per OTLP
//! signal). `docs/design/wire-protocol.md` has the native format's framing and payload.

pub mod buffer;
pub mod collectd;
pub mod datadog;
pub mod frame;
pub mod graphite;
pub mod json;
pub mod msgbuf;
pub mod msgpack;
pub mod native;
pub mod otlp;
pub mod prometheus;
pub mod splunk;

pub use msgbuf::MessageBuf;

use logit_core::{Event, EventBatch, Resource, Scope};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("malformed input: {0}")]
    Malformed(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// Well-formed input this codec doesn't speak, as opposed to [`CodecError::Malformed`]'s
    /// "this input violates the format": a frame version or codec byte this reader doesn't know,
    /// or reserved `zstd` compression.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The input is a valid prefix of a frame and needs `needed` more bytes; unlike
    /// [`CodecError::Malformed`], more bytes could make it decode.
    ///
    /// A stream or file reader acts on the difference: `Truncated` means "read more" (a live
    /// socket) or "a torn write ends here" (a spool segment), where `Malformed` means give up on
    /// this frame and [`frame::resync`] past it. Returned by [`frame::read_frame`] and
    /// [`frame::FrameHeader::read`] on a short header or body.
    #[error("truncated input: need {needed} more byte(s)")]
    Truncated { needed: usize },
    /// A well-formed native payload that would decode into more heap than its
    /// `native::DecodeBudget` allows: a batch too large for the frame cap it arrived under, not
    /// corrupt bytes.
    #[error("payload decodes past its {limit}-byte decode budget")]
    BudgetExceeded { limit: u64 },
}

/// Turns wire bytes into events sharing one [`Resource`] (`docs/design/data-model.md`).
///
/// `prometheus_in` doesn't implement it: a scrape body it fetched isn't a datagram it received,
/// so it calls `prometheus`'s plain functions (ADR `prometheus-scrape-and-exposition`). OTLP
/// decodes through [`SignalDecoder`].
pub trait Decoder {
    /// Decodes one datagram, appending its events to `out` so an accumulating caller
    /// (`logit_pipeline::BatchAccumulator`) reuses one buffer (`docs/design/memory.md` §2).
    ///
    /// `received_at` is when the datagram left the socket, not when this runs; the two diverge by
    /// the receive queue's latency under backlog (ADR `decoupled-listener-io`). A receipt-time
    /// `timestamp` (every syslog and statsd event's) comes from it, never from the decode clock.
    ///
    /// Returns the batch's [`Scope`] beside its [`Resource`]: a scope is part of the batch's
    /// identity (ADR `lossless-transit`), and `BatchAccumulator::absorb` keys on the pair.
    fn decode_into(
        &mut self,
        bytes: bytes::Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<Scope>>), CodecError>;

    /// [`Decoder::decode_into`] with `received_at` set to now, for tests and benchmarks. A
    /// listener calls `decode_into` with the instant its read half captured.
    fn decode(&mut self, bytes: bytes::Bytes) -> Result<EventBatch, CodecError> {
        let received_at = now_nanos();
        let mut events = Vec::new();
        let (resource, scope) = self.decode_into(bytes, received_at, &mut events)?;
        Ok(EventBatch { resource, scope, events })
    }
}

pub(crate) fn now_nanos() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64
}

/// Turns an [`EventBatch`] into one opaque blob of wire bytes.
///
/// For a codec whose output is one blob per batch: the native format, InfluxDB line protocol (one
/// HTTP body), and `stdio_out`/`file_out`'s dump. An error means the whole batch didn't encode.
/// `prometheus_out` is a registry rendered on scrape and implements none of the three encoder
/// shapes (ADR `prometheus-scrape-and-exposition`).
pub trait Encoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<bytes::Bytes, CodecError>;
}

/// Turns an [`EventBatch`] into N framed messages with per-message drop accounting.
///
/// For a transport that needs message boundaries one opaque `Bytes` can't carry: one datagram or
/// framed record per message (syslog), or lines or packets a transport packs into datagrams
/// (statsd, collectd, graphite). See ADR `framed-encoder`.
///
/// **Never fails.** A per-message problem (an oversize line, an unencodable value, a record the
/// wire can't represent) is a counted outcome in `Stats`, since a caller can do nothing about one
/// bad message but count it. `Stats` is per sink; each sink's `send` maps it onto
/// `logit.output.*` counters by hand.
///
/// `Meta` is what a message carries beyond its bytes: `()` for syslog and statsd, whose messages
/// are self-describing; a per-packet count for collectd (messages) and graphite (datapoints).
pub trait FramedEncoder {
    type Meta;
    type Stats: Default + std::fmt::Debug + PartialEq;

    /// Encodes every event in `batch` into `out` (cleared first), one entry per framed message.
    /// Anything else the encoder needs (a dialect, a size cap) is state set at construction,
    /// never a per-call argument.
    fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf<Self::Meta>) -> Self::Stats;
}

/// What a sink does with a metric kind its one-number-per-point wire can't carry natively
/// (`graphite_out`, `splunk_hec_out`). Each codec's module doc lists what `Expand` renders.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MultiValue {
    /// Drop the record, counted `logit.output.metrics.skipped{metric_kind=…}`. The default, since
    /// an expansion's naming convention is one the receiver may know nothing about.
    #[default]
    Skip,
    /// Expand into the per-codec series its module doc lists, counted
    /// `logit.output.metrics.degraded{metric_kind=…}` once per record.
    Expand,
}

/// Which OTLP service a payload belongs to.
///
/// One `Event` can carry a log, metrics, and a span at once (ADR `multi-payload-events`), but OTLP
/// sends logs, metrics, and traces as three RPCs with three message types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Logs,
    Metrics,
    Traces,
}

impl Signal {
    /// The default OTLP/HTTP path this signal is POSTed to, e.g. `/v1/traces`.
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

    /// The `signal` tag value on per-signal telemetry.
    pub fn as_str(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Metrics => "metrics",
            Signal::Traces => "traces",
        }
    }
}

/// Splits one [`EventBatch`] into one payload per [`Signal`].
///
/// OTLP has no single message type a batch maps onto, and each payload needs a per-signal
/// destination rather than a message boundary.
pub trait SignalEncoder {
    /// Encodes `batch` into zero or more `(Signal, bytes)` payloads, one per non-empty signal. An
    /// empty batch yields none, never an empty OTLP request.
    fn encode_signals(
        &mut self,
        batch: &EventBatch,
    ) -> Result<Vec<(Signal, bytes::Bytes)>, CodecError>;
}

/// The mirror of [`SignalEncoder`].
///
/// Returns one batch per `Resource*` entry: one OTLP request can carry N resources, and an
/// [`EventBatch`] holds one.
///
/// OTLP/JSON decoding ([`otlp::OtlpDecoder::decode_signal_json`]) is an inherent method, not part
/// of this trait: `OtlpDecoder` is the only implementor and nothing is generic over the trait, so
/// a trait method would only force a default "JSON unsupported" body on a future implementor.
pub trait SignalDecoder {
    fn decode_signal(
        &mut self,
        signal: Signal,
        bytes: bytes::Bytes,
    ) -> Result<Vec<EventBatch>, CodecError>;
}
