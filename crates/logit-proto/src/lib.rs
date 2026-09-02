//! Codec traits every input/output implements against, plus the native logit-to-logit wire
//! format. See `docs/design/wire-protocol.md` for the framing and payload design.

pub mod buffer;
pub mod frame;

use logit_core::{Event, EventBatch, Resource};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("malformed input: {0}")]
    Malformed(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

/// Turns wire bytes into events sharing one [`Resource`]. Every input (statsd, syslog, OTLP, the
/// native protocol, ...) implements this against the same internal model -- see
/// `docs/design/data-model.md`.
pub trait Decoder {
    /// Decodes one datagram, appending its events to `out` rather than returning a fresh `Vec` --
    /// a caller accumulating across many datagrams (`logit_pipeline::BatchAccumulator`,
    /// `docs/adr/0022-decoupled-listener-io.md`) can then reuse one buffer via `Vec::drain`
    /// instead of allocating and immediately discarding one per datagram
    /// (`docs/design/memory.md` §2).
    ///
    /// `received_at` is when the datagram was taken off the socket, not when this runs -- once a
    /// listener's own I/O is decoupled from its decode loop (ADR 0022), the two can diverge by the
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

fn now_nanos() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64
}

/// Turns an [`EventBatch`] into wire bytes. The mirror of [`Decoder`]; every output implements
/// this.
pub trait Encoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<bytes::Bytes, CodecError>;
}
