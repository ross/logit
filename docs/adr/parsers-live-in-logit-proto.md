---
created: 2026-10-09
updated: 2026-10-09
---

# Parsers of producer bytes live in `logit-proto`, and listeners and transforms wrap them

## Status
Accepted

## Context

[ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md) lets a fuzz target call only `logit-core` and
`logit-proto`. `logit-inputs` and `logit-transforms` both depend on `logit-pipeline`, which pulls
in `logit-script` (vendored LuaJIT), tokio, and rustls, and building all of that under
AddressSanitizer buys no target anything. That ADR's answer is a seam: code a target needs moves
into `logit-proto` first. The gRPC framing and bounded inflate moved to `logit_proto::otlp::grpc`
that way, the remote-write Snappy gate to `logit_proto::prometheus::compression`, and the PROXY
and forwarding-header parsers were written in `logit_proto::{proxy,forwarded}` from the start.

AGENTS.md's Inputs section already states the intent: "Listeners live in `crates/logit-inputs`,
codecs in `crates/logit-proto`." Four parsers of bytes a producer controls don't follow it, so
none of them can be fuzzed:

- the TCP stream `Framer` in `crates/logit-inputs/src/tcp.rs`, which every TCP listener but
  `logit_in` frames with;
- `StatsdDecoder` in `crates/logit-inputs/src/statsd.rs`;
- `SyslogDecoder` in `crates/logit-inputs/src/syslog.rs`;
- the json, csv, logfmt, and kv tokenizers in `crates/logit-transforms/src/{json,csv,logfmt}.rs`,
  which parse an event's log message: text a producer wrote, as untrusted as a wire frame.

Each decoder is already pure. `StatsdDecoder` and `SyslogDecoder` import only `bytes`,
`logit-core`, and `logit_proto::{CodecError, Decoder}`, and `Framer` uses only `Bytes`, `BytesMut`,
and `&[u8]`.

Three decoders each rebuild a zero-copy `Bytes` sub-slice from a raw-pointer subtraction in their
own `slice_of`, with no check that the sub-slice lies inside its base: `StatsdDecoder`,
`SyslogDecoder`, and `logit_proto::graphite`'s decoder (`graphite/decode.rs`). `json.rs`'s
`borrowed_str_bytes` does the same arithmetic with a range check and a copy fallback.

`docs/plans/critical-sections-inventory.md`'s cluster 9, "Untrusted-input parsers", asks for one
fuzz target per parser, which needs these parsers reachable from the fuzz crate.

## Decision

Every parser of producer bytes, wire protocol or log-message text, lives in `logit-proto`. A
listener in `logit-inputs` or a transform in `logit-transforms` holds only:

- its I/O: sockets, TLS, files, and the read loop;
- its policy: which framing to use, `json`'s `invalid_utf8` retry, and what a failure costs;
- its diagnostics and telemetry;
- its `Input` or `Transform` impl.

It calls the `logit-proto` parser for everything else.

The moves that bring today's code in line:

| From | To | What moves |
|---|---|---|
| `crates/logit-inputs/src/tcp.rs` | `logit_proto::framing` | `Framer`, `FramingMode`, `Oversize`, `Framing`, `FrameError`, `MAX_FRAME_BYTES`, and `READ_BUFFER_BYTES` |
| `crates/logit-inputs/src/statsd.rs` | `logit_proto::statsd` | `StatsdDecoder` and its line, event, and service-check parsers |
| `crates/logit-inputs/src/syslog.rs` | `logit_proto::syslog` | `SyslogDecoder` and its RFC 3164, RFC 5424, and STRUCTURED-DATA parsers |
| `crates/logit-transforms/src/json.rs` | `logit_proto::message::json` | `parse_object` and `parse_object_prefix` |
| `crates/logit-transforms/src/csv.rs` | `logit_proto::message::csv` | `split_row` and `unescape` |
| `crates/logit-transforms/src/logfmt.rs` | `logit_proto::message::logfmt` | `parse_logfmt` and `parse_kv` |

- `logit_proto::statsd` and `logit_proto::syslog` follow `graphite/` and `collectd/`: a `mod.rs`
  whose doc is the grammar and mapping table, and a `decode.rs` with the decoder.
- `logit_proto::framing` isn't named `stream`, which already names the sinks' pooled-stream
  driver in `logit-outputs`.
- `logit_proto::message` holds parsers of an event's log message. It doesn't collide with the
  existing `logit_proto::json`, which stays.
- Only the parse cores of `json`, `csv`, `logfmt`, and `kv` move. Their `Transform` impls can't:
  `logit-proto` sits below `logit-pipeline` and can't name the `Transform` trait, and the orphan
  rule doesn't let `logit-transforms` implement that trait for a type `logit-proto` defines. So
  the transforms keep their all-or-nothing scratch merge, diagnostics, telemetry, and
  `invalid_utf8` handling.
- The transport-facing parts of each listener stay in `logit-inputs`: `serve_connection` and the
  framing-error reporters in `tcp.rs`, and `StatsdInput` and `SyslogInput` with their `Input`
  impls.

One helper replaces the four sub-slice rebuilds: `logit_core::subslice::share(base, sub)` returns
a `Bytes` sharing `base`'s allocation when `sub` lies inside `base`, and a copy of `sub` when it
doesn't. `logit_core::subslice::within(base, sub)` is the containment test, for test assertions.
`share` never calls `Bytes::slice_ref`, which panics when `sub` lies outside `base`. A parser that
builds a sub-slice from an unescaped copy would then panic on ordinary input.

## Alternatives considered

- **Add `logit-inputs` and `logit-transforms` to the fuzz crate.** Rejected. Every target would
  build LuaJIT, tokio, and rustls under AddressSanitizer, and no target calls any of them. It also
  contradicts ADR `out-of-ci-fuzzing`, which keeps those crates out for that reason.
- **Stable-toolchain robustness tests without cargo-fuzz.** Rejected as the whole answer. A seeded
  suite such as `crates/logit-proto/tests/robustness.rs` explores only the mutations its author
  named. The moved parsers get those tests too, but coverage-guided fuzzing is what finds the
  input nobody named.
- **A new `logit-parsers` crate.** Rejected. `logit-proto` is already the codec crate, and its
  `graphite`, `collectd`, `prometheus`, `proxy`, and `forwarded` modules are parsers of the same
  kind. A new crate adds a crate edge and a second home for one kind of code, and
  [ADR `crate-layout-and-build-speed`](crate-layout-and-build-speed.md) measured that splitting
  crates doesn't shorten the build, so build speed doesn't argue for one either.

## Consequences

- Each moved parser gets a fuzz target and stable robustness tests
  ([ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md)'s "untrusted-input parsers" amendment). That
  closes the inventory's cluster 9.
- A move changes no behavior. The workspace's test count and the exact allocation pins in
  `crates/logit-bench/tests/allocations.rs` stay the same across each one. Release and bench
  builds use fat LTO, so a parser still inlines across the new crate boundary.
- `logit-proto` gains a `serde` dependency for the `serde::de` traits `message::json` drives.
  OTLP/JSON decoding still uses no derives
  ([ADR `otlp-json-decoding`](otlp-json-decoding.md)).
- Each move updates AGENTS.md's Inputs table `Code` column and "Where things live" for the code it
  moves.
- A new parser of producer bytes starts in `logit-proto`, so it can get a fuzz target without a
  move.
