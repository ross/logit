//! One-shot generator for `crates/logit-proto/src/{otlp,prometheus}/generated/`, run by
//! `script/protogen` (see `docs/adr/committed-pregenerated-otlp-protobuf.md`). Messages only, no
//! service stubs: the hand-rolled transports send and receive these bytes directly.
//!
//! Each proto family is generated from its own vendored sources (pinned in
//! `crates/logit-proto/proto/README.md`) into its own `generated/` directory. Both regenerate on
//! every run, so a diff should show changes only under the family whose sources moved.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

// No `#![rustfmt::skip]`: the inner form is nightly-only (rust-lang/rust#54726), so each
// family's hand-written `generated/mod.rs` puts the outer `#[rustfmt::skip]` on its `pub mod`s.
const HEADER: &str = "#![allow(clippy::all)]\n#![allow(rustdoc::all)]\n\n";

/// One proto family: include roots (repo-relative) for resolving `import`s, `(source .proto,
/// generated package file name)` pairs -- prost-build names each output file after the proto
/// `package` statement it came from, dots and all, so two source files sharing a package (as
/// Prometheus's `remote.proto`/`types.proto` do) collapse into one output file -- a destination
/// directory, and an on-disk filename rewrite for the committed short name.
struct Family {
    includes: &'static [&'static str],
    files: &'static [(&'static str, &'static str)],
    dest: &'static str,
    rename: fn(&str) -> &str,
}

fn identity(name: &str) -> &str {
    name
}

// Renamed short: opentelemetry.proto.common.v1.rs -> common.v1.rs -- generated/mod.rs nests each
// `include!` in a hand-written module tree matching the proto package path
// (`opentelemetry::proto::common::v1`, ...), so the short on-disk name loses no information the
// module path doesn't already carry.
fn strip_otlp_prefix(name: &str) -> &str {
    name.strip_prefix("opentelemetry.proto.").unwrap_or(name)
}

const FAMILIES: &[Family] = &[
    Family {
        includes: &["crates/logit-proto/proto"],
        files: &[
            (
                "crates/logit-proto/proto/opentelemetry/proto/common/v1/common.proto",
                "opentelemetry.proto.common.v1.rs",
            ),
            (
                "crates/logit-proto/proto/opentelemetry/proto/resource/v1/resource.proto",
                "opentelemetry.proto.resource.v1.rs",
            ),
            (
                "crates/logit-proto/proto/opentelemetry/proto/logs/v1/logs.proto",
                "opentelemetry.proto.logs.v1.rs",
            ),
            (
                "crates/logit-proto/proto/opentelemetry/proto/metrics/v1/metrics.proto",
                "opentelemetry.proto.metrics.v1.rs",
            ),
            (
                "crates/logit-proto/proto/opentelemetry/proto/trace/v1/trace.proto",
                "opentelemetry.proto.trace.v1.rs",
            ),
        ],
        dest: "crates/logit-proto/src/otlp/generated",
        rename: strip_otlp_prefix,
    },
    Family {
        // `remote.proto`'s `import "types.proto"` and the nested v2 file resolve against the
        // prompb root; `import "gogoproto/gogo.proto"` resolves against proto/. Neither prompb
        // package imports the other, so the output has no cross-package `super::` field types.
        // gogo.proto's `import "google/protobuf/descriptor.proto"` needs `/usr/include`, which
        // `libprotobuf-dev` populates (see tools/protogen/Dockerfile).
        includes: &[
            "crates/logit-proto/proto/prometheus/prompb",
            "crates/logit-proto/proto",
            "/usr/include",
        ],
        files: &[
            ("crates/logit-proto/proto/prometheus/prompb/remote.proto", "prometheus.rs"),
            ("crates/logit-proto/proto/prometheus/prompb/types.proto", "prometheus.rs"),
            (
                "crates/logit-proto/proto/prometheus/prompb/io/prometheus/write/v2/types.proto",
                "io.prometheus.write.v2.rs",
            ),
        ],
        dest: "crates/logit-proto/src/prometheus/generated",
        rename: identity,
    },
];

fn main() {
    for family in FAMILIES {
        let out_dir = std::env::temp_dir().join("logit-protogen-out").join(family.dest.replace('/', "_"));
        fs::create_dir_all(&out_dir).expect("create scratch out dir");
        fs::create_dir_all(family.dest).expect("create dest dir");

        let inputs: Vec<_> = family.files.iter().map(|(src, _)| Path::new(src)).collect();
        let includes: Vec<_> = family.includes.iter().map(Path::new).collect();
        prost_build::Config::new()
            .out_dir(&out_dir)
            .compile_protos(&inputs, &includes)
            .unwrap_or_else(|e| panic!("compile protos for {}: {e} -- is protoc on PATH?", family.dest));

        // Two source files can share a package (Prometheus's remote.proto/types.proto both
        // declare `package prometheus;`) and so share one generated output file -- write it once.
        let mut written = HashSet::new();
        for (_, generated) in family.files {
            if !written.insert(*generated) {
                continue;
            }
            let body = fs::read_to_string(out_dir.join(generated))
                .unwrap_or_else(|e| panic!("read generated {generated}: {e}"));
            let short = (family.rename)(generated);
            fs::write(Path::new(family.dest).join(short), format!("{HEADER}{body}"))
                .unwrap_or_else(|e| panic!("write {short}: {e}"));
            println!("wrote {}/{short}", family.dest);
        }
    }
}
