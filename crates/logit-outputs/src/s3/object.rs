//! The open object: each staged batch encoded, compressed, and hashed into one body as it arrives
//! (`docs/adr/s3-object-archive.md`, decisions 2 and 4). Each step runs per batch because
//! `run_output` drains the inbox on the same task as the write loop, so compressing a whole object
//! at its close would stall the inbox that long.
//!
//! - **Encoding.** `format: json` writes `stdio_out`'s `format: json` lines
//!   ([`crate::ndjson::render_ndjson`]) into a reused buffer; `format: native` writes one bare
//!   native frame per batch, uncompressed, as `file_out` does under `format: native`.
//! - **Compression.** `gzip` is one gzip member per object through `flate2`, whose output lags
//!   its input by what the deflate stream holds back. `zstd` is one frame per staged batch through
//!   `ruzstd`, which has no streaming encoder; a zstd reader reads concatenated frames as one
//!   stream. `none` appends the encoding as it is.
//! - **Hashing.** The body's SHA-256 is updated with the compressed bytes each stage produced, so
//!   the close hashes only what the compressor's finish adds.

use super::{ObjectCompression, ObjectFormat};
use flate2::write::GzEncoder;
use logit_core::EventBatch;
use logit_proto::frame::Compression as FrameCompression;
use logit_proto::native::NativeEncoder;
use logit_proto::{CodecError, Encoder};
use ring::digest;
use std::io::Write;

/// The compressed body so far.
enum Body {
    Gzip(GzEncoder<Vec<u8>>),
    /// `zstd`'s frames, or `none`'s encoding, appended as each batch is staged.
    Plain(Vec<u8>),
}

impl Body {
    fn new(compression: ObjectCompression) -> Self {
        match compression {
            ObjectCompression::Gzip => {
                Body::Gzip(GzEncoder::new(Vec::new(), flate2::Compression::default()))
            }
            ObjectCompression::Zstd | ObjectCompression::None => Body::Plain(Vec::new()),
        }
    }

    fn written(&self) -> &[u8] {
        match self {
            Body::Gzip(encoder) => encoder.get_ref(),
            Body::Plain(body) => body,
        }
    }
}

/// An object closed for upload: its body and the body's SHA-256 in lowercase hex.
pub struct FinishedObject {
    pub body: Vec<u8>,
    pub sha256: String,
}

/// The object being staged. Reused across objects: the encode buffer keeps its capacity.
pub struct StagedObject {
    format: ObjectFormat,
    compression: ObjectCompression,
    /// `format: json`'s lines for the batch being staged.
    scratch: String,
    body: Body,
    hash: digest::Context,
    /// How much of the body `hash` has taken.
    hashed: usize,
    batches: usize,
    /// The staged batches' encoded bytes, before compression: what `max_bytes` bounds.
    encoded_bytes: u64,
}

impl StagedObject {
    pub fn new(format: ObjectFormat, compression: ObjectCompression) -> Self {
        Self {
            format,
            compression,
            scratch: String::new(),
            body: Body::new(compression),
            hash: digest::Context::new(&digest::SHA256),
            hashed: 0,
            batches: 0,
            encoded_bytes: 0,
        }
    }

    /// Encodes `batch`, compresses it into the body, and hashes what that produced. A failed
    /// encode leaves the object as it was.
    pub fn stage(&mut self, batch: &EventBatch) -> Result<(), CodecError> {
        let frame;
        let encoded: &[u8] = match self.format {
            ObjectFormat::Json => {
                self.scratch.clear();
                crate::ndjson::render_ndjson(&mut self.scratch, batch);
                self.scratch.as_bytes()
            }
            ObjectFormat::Native => {
                frame = NativeEncoder::new(FrameCompression::None).encode(batch)?;
                &frame
            }
        };
        match &mut self.body {
            Body::Gzip(encoder) => {
                encoder.write_all(encoded).expect("a gzip encoder writing to a Vec doesn't fail")
            }
            Body::Plain(body) if self.compression == ObjectCompression::Zstd => {
                ruzstd::encoding::compress(
                    encoded,
                    &mut *body,
                    ruzstd::encoding::CompressionLevel::Fastest,
                );
            }
            Body::Plain(body) => body.extend_from_slice(encoded),
        }
        self.encoded_bytes += encoded.len() as u64;
        self.batches += 1;
        let written = self.body.written();
        self.hash.update(&written[self.hashed..]);
        self.hashed = written.len();
        Ok(())
    }

    /// The batches staged.
    pub fn batches(&self) -> usize {
        self.batches
    }

    /// The staged batches' encoded bytes, before compression.
    pub fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    /// The compressed bytes written so far. Under `gzip` it trails the input by what the deflate
    /// stream holds back until the next block or the finish.
    pub fn compressed_bytes(&self) -> usize {
        self.body.written().len()
    }

    /// Closes the object: finishes the compressed stream and the hash, and leaves an empty
    /// object behind.
    pub fn finish(&mut self) -> FinishedObject {
        let body = match std::mem::replace(&mut self.body, Body::new(self.compression)) {
            Body::Gzip(encoder) => {
                encoder.finish().expect("a gzip encoder writing to a Vec doesn't fail")
            }
            Body::Plain(body) => body,
        };
        let mut hash = std::mem::replace(&mut self.hash, digest::Context::new(&digest::SHA256));
        hash.update(&body[self.hashed..]);
        let sha256 = hex(hash.finish().as_ref());
        self.hashed = 0;
        self.batches = 0;
        self.encoded_bytes = 0;
        FinishedObject { body, sha256 }
    }

    /// Drops everything staged, as for an object a fault abandoned.
    pub fn reset(&mut self) {
        if self.batches > 0 || self.hashed > 0 {
            self.body = Body::new(self.compression);
            self.hash = digest::Context::new(&digest::SHA256);
        }
        self.hashed = 0;
        self.batches = 0;
        self.encoded_bytes = 0;
    }
}

/// `bytes` in lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("writing to a String");
    }
    out
}
