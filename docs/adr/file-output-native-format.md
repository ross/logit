---
created: 2026-09-09
updated: 2026-09-09
---

# `stdio_out`/`file_out` gain a `native` wire-format option

## Status
Accepted

## Context

[ADR `rotating-file-output`](rotating-file-output.md) built `stdio_out`/`file_out` on
`StreamOutput<E: logit_proto::Encoder>` specifically so a second encoder could plug in later "with
no change to the destination half" — but deferred building one, since the native wire format was
at that point still an open, benchmark-gated decision (`AGENTS.md`'s design constraints).

[ADR `native-wire-format-encoding`](native-wire-format-encoding.md) settled that decision:
`logit_proto::native::{NativeEncoder, NativeDecoder}` is a real, tested `Encoder`/`Decoder`
implementation, with no change to either trait's signature. The fit for `file_out` turns out to be
closer than "a seam exists to fill": `NativeEncoder` is `Copy` with no state carried across calls
(its own "dictionary-first" framing is rebuilt fresh inside every `encode()`), and every frame it
writes is independently decodable — that ADR's own tests
(`concatenated_frames_written_to_a_buffer_are_each_independently_decodable`) exist specifically to
prove a file of concatenated frames survives being read starting mid-stream. That is exactly
`file_out`'s rotation model: each rotated file is already meant to stand alone. Two designs, landed
in separate PRs by different work, converge on the same shape without either one having been built
with the other in mind — worth stating plainly, since it's the whole reason this follow-up is
cheap rather than a redesign.

## Decision

### `StreamEncoder`: an enum, not `Box<dyn Encoder>`

```rust
pub enum StreamEncoder {
    Human(EventDump),
    Native(logit_proto::native::NativeEncoder),
}
```

Both implementors are `Copy` with no cross-call state, so delegating through a `match` costs
nothing a trait object wouldn't also cost, and it keeps `StreamOutput<StreamEncoder>` the one
concrete type `build_spec` ever constructs — the same shape `syslog_out`'s `Conn` and `otlp_out`'s
`OtlpTransport` already use for a runtime choice between a small, closed set of implementations.
`StreamOutput<EventDump>`'s constructors (`stdout`/`stderr`/`open_path`/`rotating`) become
`StreamOutput<StreamEncoder>`'s, each still defaulting to `StreamEncoder::human()`; a new
`with_format` builder overrides it, mirroring `with_telemetry`/`with_diagnostics`. No call site
outside `stdio.rs` changes — the field was already private, reached only through `Encoder`/builder
methods.

### The empty-batch short-circuit moves from output to input

`send` used to check `bytes.is_empty()` on the *encoded* output, which happened to coincide with
`batch.events.is_empty()` only because `EventDump` renders an empty batch to `""`.
`NativeEncoder::encode` always produces a real, non-empty frame — a 24-byte header plus a
dictionary and resource, even for zero events — so that check would never fire under
`format: native`, and every empty batch would still write a small real frame to disk. Checking
`batch.events.is_empty()` before encoding at all is correct for either encoder and behavior-
preserving for `EventDump`; it also means never asking an encoder to do work for nothing. This is
encoder-agnostic correctness, not a `native`-specific workaround, so it landed as part of the same
change rather than being scoped only to the new format.

### Config: `format: human | native`, `compression: none | lz4`, on both kinds

```yaml
- id: disk_out
  type: file_out
  sources: [enrich]
  path: /var/log/logit/events.log
  rotate: { interval: daily }
  format: native
  compression: lz4
```

Both fields land on **both** `stdio_out` and `file_out` — the mechanism is shared, so restricting
`format: native` to one kind would cost extra plumbing (a graph rule rejecting it on the other) for
no benefit. `format: native` on `stdio_out` (binary to a terminal) is a niche case, but harmless,
and still useful piped to a file or another process.

`compression` is `logit_config::Compression { None, Lz4 }` — a local mirror of
`logit_proto::frame::Compression`, converted in `crates/logit-cli/src/pipeline.rs`'s
`to_native_compression`, the same crate-layout reason `RotatePolicy`/`RotateInterval` are mirrored
rather than shared directly (`logit-config` must not depend on `logit-proto`,
`docs/design/pipeline-graph.md`). `Zstd` is deliberately not a variant: `logit_proto::native`
rejects it on both encode and decode (the real `zstd` crate needs a C build via `zstd-sys`,
breaking ADR `containerized-development`), so there is nothing valid for a config to select.

Graph rule 33 rejects `compression` set to anything but `none` while `format` stays `human` (the
default) — the same "would silently do nothing" reasoning rule 29 already applies to `rotate:`'s
own triggers.

## Alternatives considered

- **`Box<dyn Encoder + Send>` instead of an enum.** Rejected: both implementors are `Copy` with no
  state to erase, so a trait object buys nothing but an extra indirection and a lost `Copy` bound,
  for a choice between exactly two things known at compile time.
- **Restricting `format: native` to `file_out` only.** Considered and rejected — see "Config"
  above. The mechanism is shared regardless; excluding `stdio_out` would be a config-surface
  restriction for its own sake, enforced by an extra validation rule rather than saved by one.
- **Deferring `compression` to a later pass.** `NativeEncoder` already takes a `Compression`
  parameter, so exposing it costs one more config field and one more converter, not a new
  mechanism — folding it in now avoids a second, near-identical follow-up PR.
- **A decode-side reader/verifier, or wiring `NativeDecoder` into `tail_in`.** Out of scope here,
  same as `logit_proto::native`'s own ADR leaves `logit_in`/`logit_out` unbuilt: reading a
  `file_out`-written native file back is real, unblocked follow-up work, not designed yet.

## Consequences

- `crates/logit-outputs/src/stdio.rs`: `StreamEncoder` (new), `StreamOutput<EventDump>` →
  `StreamOutput<StreamEncoder>`, `with_format`, and the empty-batch check moved before `encode`.
- `crates/logit-config/src/lib.rs`: `StreamFormat`, `Compression`; `format`/`compression` fields on
  `ComponentKind::StdioOut`/`FileOut`.
- `crates/logit-pipeline/src/graph.rs`: rule 33.
- `crates/logit-cli/src/pipeline.rs`: `to_stream_encoder`/`to_native_compression`; a new direct
  `logit-proto` dependency (previously only transitive via `logit-outputs`) since `build_spec`
  names `logit_proto::frame::Compression` directly.
- `schema/logit.schema.json` regenerated.
- `docs/known-gaps.md`'s `file_out`/`stdio_out` entries narrow: `format:` is no longer fixed to
  human-readable text, though reading a native-formatted file back remains unbuilt.
