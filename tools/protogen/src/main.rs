//! One-shot generator for `crates/logit-proto/src/otlp/generated/`. Not a workspace member and
//! not a build-time dependency (see `docs/adr/0023-committed-pregenerated-otlp-protobuf.md`) --
//! run via `script/protogen`, inside a throwaway image with `protoc` installed, then review the
//! diff and commit the result by hand. Messages only, no service stubs: PR3 hand-rolls the RPCs.

use std::fs;
use std::path::Path;

const PROTO_ROOT: &str = "crates/logit-proto/proto";
const DEST: &str = "crates/logit-proto/src/otlp/generated";
// `#![rustfmt::skip]` (inner form) is nightly-only (rust-lang/rust#54726) -- `generated/mod.rs`
// carries the stable outer `#[rustfmt::skip]` on each file's `pub mod v1;` declaration instead.
const HEADER: &str = "#![allow(clippy::all)]\n#![allow(rustdoc::all)]\n\n";

// (source .proto, generated package file name) -- prost-build names each output file after the
// proto `package` statement it came from, dots and all.
const FILES: &[(&str, &str)] = &[
    ("opentelemetry/proto/common/v1/common.proto", "opentelemetry.proto.common.v1.rs"),
    ("opentelemetry/proto/resource/v1/resource.proto", "opentelemetry.proto.resource.v1.rs"),
    ("opentelemetry/proto/logs/v1/logs.proto", "opentelemetry.proto.logs.v1.rs"),
    ("opentelemetry/proto/metrics/v1/metrics.proto", "opentelemetry.proto.metrics.v1.rs"),
    ("opentelemetry/proto/trace/v1/trace.proto", "opentelemetry.proto.trace.v1.rs"),
];

fn main() {
    let out_dir = std::env::temp_dir().join("logit-protogen-out");
    fs::create_dir_all(&out_dir).expect("create scratch out dir");

    let inputs: Vec<_> = FILES.iter().map(|(src, _)| Path::new(PROTO_ROOT).join(src)).collect();
    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(&inputs, &[Path::new(PROTO_ROOT)])
        .expect("compile OTLP protos -- is protoc on PATH?");

    for (_, generated) in FILES {
        let body = fs::read_to_string(out_dir.join(generated))
            .unwrap_or_else(|e| panic!("read generated {generated}: {e}"));
        // Renamed short: opentelemetry.proto.common.v1.rs -> common.v1.rs -- generated/mod.rs
        // nests each `include!` in a hand-written module tree matching the proto package path
        // (`opentelemetry::proto::common::v1`, ...), so the short on-disk name loses no
        // information the module path doesn't already carry.
        let short = generated.strip_prefix("opentelemetry.proto.").unwrap_or(generated);
        fs::write(Path::new(DEST).join(short), format!("{HEADER}{body}"))
            .unwrap_or_else(|e| panic!("write {short}: {e}"));
        println!("wrote {DEST}/{short}");
    }
}
