---
created: 2026-09-03
updated: 2026-09-03
---

# `otlp_out`/`otlp_in` gzip: client never accepts a compressed response, server bounds decompressed size

## Status
Accepted

## Context
`docs/plans/otlp-logs-and-resource-identity.md`'s workstream E found `otlp_out` never sets gzip's
compressed flag on either transport (`crates/logit-outputs/src/otlp.rs`'s frame encoder). The
matching gap on the receiving side was already filed and explained why it hadn't been closed:
`otlp_in` rejects any `Content-Encoding`/`grpc-encoding` other than `identity` outright, because
decompressing untrusted input unboundedly is real, security-relevant surface — a small,
highly-compressible request ("a compression bomb") can inflate to far more memory than the sender
ever transmitted, and `flate2` wasn't a dependency yet.

Both sides land together here, deliberately: `otlp_out`'s compression has no way to be exercised
end-to-end without `otlp_in` learning to decode it, and `crates/logit-cli/tests/otlp_round_trip.rs`
— the test that proves the two halves actually interoperate, not just that each side's own unit
tests pass in isolation — needs both. `docs/known-gaps.md`'s existing `otlp_in` entry filed the
`flate2`-as-a-dependency question as the reason compression was deferred; that reason no longer
applies once this lands, so this ADR is also where that call gets made.

## Decision

**`flate2`, default features, both crates.** Pure-Rust `miniz_oxide` backend, not a C zlib —
keeps "no host toolchain needed" intact ([ADR `containerized-development`](containerized-development.md)).
Not `async-compression`: both `otlp_out`'s outbound payload and `otlp_in`'s collected request body
are already fully buffered into one contiguous `Bytes`/`Vec` before any codec runs (`Limited::collect`
on the input side; an in-memory encoded protobuf on the output side), so there is no stream to
adapt — `async-compression` would only add a tokio-io adapter layer over the same `flate2` call.
A few MiB of protobuf compresses or decompresses in well under a millisecond, so both run inline
on the async task rather than via `spawn_blocking`.

**`otlp_out` defaults to `compression: none`.** Flipping the default would change an existing
pipeline's wire behavior in a patch, and would break `otlp_out → otlp_in` across two `logit`
versions where only one side has learned gzip. An operator opts in per `otlp_out` component.

**`otlp_out` never advertises accepting a compressed response, regardless of `compression`.**
`grpc_roundtrip`'s `grpc-accept-encoding` header stays `identity` unconditionally — compressing a
request and being willing to receive a compressed response are independent questions, and this
answers only the first. A compliant server therefore never compresses its response, so
`grpc_unframe` (the client's own response parser) is unchanged: it still rejects a set compressed
flag exactly as it did before this ADR (`grpc_unframe_rejects_a_set_compressed_flag`, the pinned
regression test). OTLP responses are `partial_success` messages of single-digit-byte size — there
is nothing worth saving by accepting a compressed one, only an inbound untrusted-decompression path
to gain on the client side for no benefit. `otlp_in`'s response side is symmetric: it never
compresses what it sends back, only what it accepts, and now advertises
`grpc-accept-encoding: identity, gzip` accordingly (accurate, not aspirational — it now really does
accept both).

**`otlp_in` bounds decompressed size to the same `MAX_REQUEST_BYTES` already enforced on the
compressed body.** `Limited::new(req.into_body(), MAX_REQUEST_BYTES)` already caps what a client
can transmit; a new `inflate` helper wraps `flate2::read::GzDecoder` in `Read::take(MAX_REQUEST_BYTES
+ 1)` and rejects anything that decompresses past the cap, distinguishing that case
(`InflateError::TooLarge` → `413`/`grpc-status: 8` `RESOURCE_EXHAUSTED`) from an outright-malformed
gzip stream (`InflateError::Malformed` → `400`/`grpc-status: 3` `INVALID_ARGUMENT`) — two different
client mistakes, two different responses, not one message that overclaims either way. `Read::take`
reading `MAX_REQUEST_BYTES + 1` bytes rather than exactly `MAX_REQUEST_BYTES` is what lets an input
that would inflate to exactly one byte over the cap be caught rather than silently truncated to fit.

**gRPC's per-message compressed flag is what actually drives decompression on `otlp_in`, not the
`grpc-encoding` header.** The header is checked first and rejects an encoding this input can't
decode (anything but `identity`/`gzip`) with a clear message, but the frame's own flag byte —
gRPC's actual self-describing framing — decides whether `inflate` runs. `grpc_unframe` on the input
side returns `(bool, &[u8])`, the compressed flag alongside the payload slice, rather than silently
assuming the header and the frame always agree.

## Alternatives considered
- **`async-compression`** instead of `flate2` directly. Rejected: no stream exists to adapt on
  either side of either transport (see above) — it would add a dependency and an adapter layer to
  wrap a call that's already synchronous and sub-millisecond.
- **Advertise `grpc-accept-encoding: gzip` on `otlp_out`'s requests**, symmetric with sending
  compressed. Rejected: OTLP response bodies are too small for compression to matter, and accepting
  one adds an inbound decompression path to the client with a bomb-guard of its own to design, for
  zero measured benefit.
- **A single decompression error variant** (`Result<Bytes, ()>`) instead of distinguishing
  malformed-vs-too-large. Rejected during implementation: a `413` for a client's genuinely malformed
  gzip stream (not oversized at all) is misleading, and the two cases are already trivially
  distinguishable from `flate2`'s own read error vs. the post-hoc length check.
- **Default `compression: gzip`.** Rejected: see the default-`none` reasoning above — the
  cross-version compatibility break alone rules it out for what's meant to be a config default, not
  a decision an operator is forced to discover by breakage.

## Consequences
- `docs/known-gaps.md`'s `otlp_in` compression clause and `otlp_out`'s workstream-E compression
  clause both close; `otlp_in`'s `partial_success`-accounting gap (a separate, unrelated limitation
  of the same module) is unaffected and stays filed.
- `otlp_out → otlp_in` interoperates with any real OTLP collector's default gzip-sending exporter
  for the first time — previously any real collector pointed at `otlp_in` failed on day one.
- `flate2` is now a genuine, non-transitive dependency of both `logit-inputs` and `logit-outputs`;
  `cargo deny`'s license allow-list already covers it and its own dependencies
  (`crc32fast`/`miniz_oxide`/`adler2`, all MIT-compatible) without a `deny.toml` change.
- gRPC TLS remains unaddressed — a separate, larger workstream (a `tokio-rustls` layer in the
  hand-rolled client) that this ADR deliberately does not attempt.
