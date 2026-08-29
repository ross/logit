//! Codec traits every input/output implements against, plus the native logit-to-logit wire
//! format. See `docs/design/wire-protocol.md` for the framing and payload design.

pub mod buffer;
pub mod frame;

use logit_core::EventBatch;

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("malformed input: {0}")]
    Malformed(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

/// Turns wire bytes into an [`EventBatch`]. Every input (statsd, syslog, OTLP, the native
/// protocol, ...) implements this against the same internal model -- see
/// `docs/design/data-model.md`.
pub trait Decoder {
    fn decode(&mut self, bytes: bytes::Bytes) -> Result<EventBatch, CodecError>;
}

/// Turns an [`EventBatch`] into wire bytes. The mirror of [`Decoder`]; every output implements
/// this.
pub trait Encoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<bytes::Bytes, CodecError>;
}
