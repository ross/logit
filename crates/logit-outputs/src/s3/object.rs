//! The open object: each staged batch encoded, compressed, and cut into parts as it arrives
//! (`docs/adr/s3-object-archive.md`, decisions 1, 2, and 4). Each step runs per batch because
//! `run_output` drains the inbox on the same task as the write loop, so compressing a whole object
//! at its close would stall the inbox that long.
//!
//! - **Encoding.** `format: json` writes `stdio_out`'s `format: json` lines
//!   ([`crate::ndjson::render_ndjson`]) into a reused buffer; `format: native` writes one bare
//!   native frame per batch, uncompressed, as `file_out` does under `format: native`.
//! - **Compression.** `gzip` is one gzip member per object through `flate2`, whose output lags
//!   its input by what the deflate stream holds back. `zstd` is one frame per
//!   [`ZSTD_CHUNK_BYTES`] of encoded input through `ruzstd`, which has no streaming encoder: the
//!   encoding collects in a reused buffer, a frame is compressed each time the buffer reaches the
//!   chunk size, and the close compresses the rest. A zstd reader reads concatenated frames as
//!   one stream. `none` appends the encoding as it is.
//! - **Parts.** The compressor writes into the current part, which is cut once it holds
//!   `part_size` bytes, the rest carried into the next one, so every part but the last is
//!   `part_size` bytes: Cloudflare R2 requires every non-final part to be the same size. A part
//!   never grows past `part_size`, so the object holds about one part of compressed body, plus the
//!   parts one batch's overshoot cut before the sink uploads them.
//! - **Hashing.** Each part's SHA-256 is updated with the bytes as the compressor writes them, so
//!   a cut part's hash is ready with it.

use super::{ObjectCompression, ObjectFormat};
use flate2::write::GzEncoder;
use logit_core::EventBatch;
use logit_proto::frame::Compression as FrameCompression;
use logit_proto::native::NativeEncoder;
use logit_proto::{CodecError, Encoder};
use ring::digest;
use std::collections::VecDeque;
use std::io::Write;

/// How much encoded input one zstd frame takes. A frame finds no match in another frame, so a
/// smaller chunk costs ratio, and the chunk is held uncompressed until it fills, so a larger one
/// costs memory and lags the part cut (`docs/adr/s3-object-archive.md`'s
/// "Implementation notes (2026-10-30)" has the measurement).
pub const ZSTD_CHUNK_BYTES: usize = 1024 * 1024;

/// One part of an object's compressed body, and its SHA-256 in lowercase hex.
pub struct Part {
    pub body: Vec<u8>,
    pub sha256: String,
}

/// The compressed body, cut into parts: what each compressor writes into.
struct Parts {
    part_size: usize,
    /// The part being filled.
    current: Vec<u8>,
    hash: digest::Context,
    /// Parts cut and not yet taken.
    ready: VecDeque<Part>,
    /// Every compressed byte written.
    written: u64,
    /// Parts cut so far.
    cut: usize,
}

impl Parts {
    fn new(part_size: usize) -> Self {
        Self {
            part_size: part_size.max(1),
            current: Vec::new(),
            hash: digest::Context::new(&digest::SHA256),
            ready: VecDeque::new(),
            written: 0,
            cut: 0,
        }
    }

    fn cut(&mut self) {
        let body = std::mem::take(&mut self.current);
        let hash = std::mem::replace(&mut self.hash, digest::Context::new(&digest::SHA256));
        self.ready.push_back(Part { body, sha256: hex(hash.finish().as_ref()) });
        self.cut += 1;
    }

    /// The parts not yet taken, the current one last. An object that wrote nothing is one empty
    /// part, and an object whose last part was cut full has no shorter one after it.
    fn finish(mut self) -> VecDeque<Part> {
        if !self.current.is_empty() || self.cut == 0 {
            self.cut();
        }
        self.ready
    }
}

impl Write for Parts {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut rest = buf;
        while !rest.is_empty() {
            let take = (self.part_size - self.current.len()).min(rest.len());
            if self.current.capacity() - self.current.len() < take {
                // Doubling, as a `Vec` grows, but never past `part_size`.
                let want = (self.current.capacity() * 2)
                    .max(self.current.len() + take)
                    .min(self.part_size);
                self.current.reserve_exact(want - self.current.len());
            }
            self.current.extend_from_slice(&rest[..take]);
            self.hash.update(&rest[..take]);
            rest = &rest[take..];
            if self.current.len() == self.part_size {
                self.cut();
            }
        }
        self.written += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The compressor.
enum Body {
    Gzip(GzEncoder<Parts>),
    /// `zstd`'s frames so far, and the encoded input not yet compressed into one.
    Zstd {
        parts: Parts,
        chunk: Vec<u8>,
    },
    /// `none`'s encoding, appended as each batch is staged.
    Plain(Parts),
}

impl Body {
    fn new(compression: ObjectCompression, part_size: usize) -> Self {
        let parts = Parts::new(part_size);
        match compression {
            ObjectCompression::Gzip => {
                Body::Gzip(GzEncoder::new(parts, flate2::Compression::default()))
            }
            ObjectCompression::Zstd => Body::Zstd { parts, chunk: Vec::new() },
            ObjectCompression::None => Body::Plain(parts),
        }
    }

    fn parts(&self) -> &Parts {
        match self {
            Body::Gzip(encoder) => encoder.get_ref(),
            Body::Zstd { parts, .. } | Body::Plain(parts) => parts,
        }
    }

    fn parts_mut(&mut self) -> &mut Parts {
        match self {
            // Taking cut parts out of the encoder's writer writes nothing to it.
            Body::Gzip(encoder) => encoder.get_mut(),
            Body::Zstd { parts, .. } | Body::Plain(parts) => parts,
        }
    }
}

/// Compresses `chunk` into one zstd frame written to `parts`, and empties it.
fn compress_chunk(parts: &mut Parts, chunk: &mut Vec<u8>) {
    if !chunk.is_empty() {
        ruzstd::encoding::compress(
            chunk.as_slice(),
            &mut *parts,
            ruzstd::encoding::CompressionLevel::Fastest,
        );
        chunk.clear();
    }
}

/// The object being staged. Reused across objects: the encode buffer keeps its capacity.
pub struct StagedObject {
    format: ObjectFormat,
    compression: ObjectCompression,
    part_size: usize,
    /// `format: json`'s lines for the batch being staged.
    scratch: String,
    body: Body,
    batches: usize,
    /// The staged batches' encoded bytes, before compression: what `max_bytes` bounds.
    encoded_bytes: u64,
}

impl StagedObject {
    /// An empty object whose body is cut into parts of `part_size` bytes.
    pub fn new(format: ObjectFormat, compression: ObjectCompression, part_size: usize) -> Self {
        Self {
            format,
            compression,
            part_size,
            scratch: String::new(),
            body: Body::new(compression, part_size),
            batches: 0,
            encoded_bytes: 0,
        }
    }

    /// Encodes `batch`, and compresses and hashes it into the body, cutting each part it fills.
    /// A failed encode leaves the object as it was.
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
                encoder.write_all(encoded).expect("a gzip encoder writing to memory doesn't fail")
            }
            Body::Zstd { parts, chunk } => {
                let mut rest = encoded;
                while !rest.is_empty() {
                    let take = rest.len().min(ZSTD_CHUNK_BYTES - chunk.len());
                    chunk.extend_from_slice(&rest[..take]);
                    rest = &rest[take..];
                    if chunk.len() == ZSTD_CHUNK_BYTES {
                        compress_chunk(parts, chunk);
                    }
                }
            }
            Body::Plain(parts) => {
                parts.write_all(encoded).expect("writing to memory doesn't fail");
            }
        }
        self.encoded_bytes += encoded.len() as u64;
        self.batches += 1;
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

    /// The compressed bytes written so far, cut parts included. Under `gzip` it trails the input
    /// by what the deflate stream holds back until the next block or the finish, and under `zstd`
    /// by the chunk not yet compressed.
    pub fn compressed_bytes(&self) -> u64 {
        self.body.parts().written
    }

    /// The full parts the body cut that nothing took yet.
    pub fn cut_parts(&self) -> usize {
        self.body.parts().ready.len()
    }

    /// The oldest full part the body cut and nothing took yet: every one is `part_size` bytes.
    pub fn take_part(&mut self) -> Option<Part> {
        self.body.parts_mut().ready.pop_front()
    }

    /// Closes the object: finishes the compressed stream, and returns every part not yet taken,
    /// the last one at most `part_size` bytes and every other one `part_size`. Leaves an empty
    /// object behind.
    pub fn finish(&mut self) -> VecDeque<Part> {
        let parts = match &mut self.body {
            // The chunk buffer stays, with its capacity, for the next object.
            Body::Zstd { parts, chunk } => {
                compress_chunk(parts, chunk);
                std::mem::replace(parts, Parts::new(self.part_size))
            }
            _ => {
                match std::mem::replace(&mut self.body, Body::new(self.compression, self.part_size))
                {
                    Body::Gzip(encoder) => {
                        encoder.finish().expect("a gzip encoder writing to memory doesn't fail")
                    }
                    Body::Plain(parts) | Body::Zstd { parts, .. } => parts,
                }
            }
        };
        self.batches = 0;
        self.encoded_bytes = 0;
        parts.finish()
    }

    /// Drops everything staged, as for an object a fault abandoned.
    pub fn reset(&mut self) {
        if self.batches > 0 || self.body.parts().written > 0 {
            match &mut self.body {
                Body::Zstd { parts, chunk } => {
                    *parts = Parts::new(self.part_size);
                    chunk.clear();
                }
                body => *body = Body::new(self.compression, self.part_size),
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sha256(bytes: &[u8]) -> String {
        hex(digest::digest(&digest::SHA256, bytes).as_ref())
    }

    /// Every part but the last is `part_size` bytes, each carries its own body's hash, and the
    /// parts concatenate to the body a single write would have made.
    #[test]
    fn parts_are_cut_at_part_size_and_hashed_each() {
        let mut parts = Parts::new(10);
        let input: Vec<u8> = (0..47u8).collect();
        parts.write_all(&input[..3]).unwrap();
        parts.write_all(&input[3..25]).unwrap();
        parts.write_all(&input[25..]).unwrap();
        assert_eq!(parts.written, 47);
        let all: Vec<Part> = parts.finish().into_iter().collect();
        let sizes: Vec<usize> = all.iter().map(|p| p.body.len()).collect();
        assert_eq!(sizes, [10, 10, 10, 10, 7]);
        for part in &all {
            assert_eq!(part.sha256, sha256(&part.body));
            assert!(part.body.capacity() <= 10, "a part never grows past part_size");
        }
        assert_eq!(all.iter().flat_map(|p| p.body.clone()).collect::<Vec<u8>>(), input);

        let mut parts = Parts::new(10);
        parts.write_all(&input[..20]).unwrap();
        let sizes: Vec<usize> = parts.finish().iter().map(|p| p.body.len()).collect();
        assert_eq!(sizes, [10, 10], "no empty part after a full last one");
        let empty: Vec<Part> = Parts::new(10).finish().into_iter().collect();
        assert_eq!(empty.len(), 1, "an empty body is one empty part");
        assert_eq!(empty[0].sha256, sha256(b""));
    }
}
