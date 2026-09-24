//! One-shot generator for `crates/logit-proto/src/{otlp,prometheus,datadog}/generated/`. Not a
//! workspace member and not a build-time dependency (see
//! `docs/adr/committed-pregenerated-otlp-protobuf.md`) -- run via `script/protogen`, inside a
//! throwaway image with `protoc` installed, then review the diff and commit the result by hand.
//! Messages only, no service stubs: hand-rolled transports (OTLP's gRPC/HTTP, Prometheus
//! remote-write's HTTP, the Datadog Agent intake's HTTP) send/receive these bytes directly.
//!
//! Three independent proto families, each generated from its own vendored `.proto` sources
//! (`crates/logit-proto/proto/README.md` has the pinned tag/commit for all three) into its own
//! `generated/` directory. Regenerating one family never touches another's committed output --
//! `script/protogen` regenerates all three every run, and `git diff --stat` after a run should
//! show changes only under the family whose `.proto` sources actually moved.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

// `#![rustfmt::skip]` (inner form) is nightly-only (rust-lang/rust#54726) -- each family's
// hand-written `generated/mod.rs` carries the stable outer `#[rustfmt::skip]` on every file's
// `pub mod ...;` declaration instead.
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
        // `remote.proto`'s `import "types.proto"` and the nested v2 file both resolve against the
        // prompb root; `import "gogoproto/gogo.proto"` (all three files) resolves against the
        // proto/ root. Neither prompb file imports the other's package, so no cross-package
        // `super::`-relative field types appear in the output (checked against the vendored
        // sources -- see crates/logit-proto/proto/README.md). `gogoproto/gogo.proto`'s own
        // `import "google/protobuf/descriptor.proto"` needs `/usr/include` on the include path --
        // Debian's `protobuf-compiler` package alone doesn't ship the well-known-types `.proto`
        // sources, `libprotobuf-dev` does (tools/protogen/Dockerfile installs both).
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
    Family {
        // `agent_payload.proto`'s `import
        // "github.com/gogo/protobuf/gogoproto/gogo.proto"` resolves against the `datadog/include`
        // root, which holds a relative symlink to the one vendored `gogoproto/gogo.proto` at that
        // Go-style import path (rather than a second copy) -- see
        // `crates/logit-proto/proto/README.md`'s Datadog section. `/usr/include` is still needed
        // for `gogo.proto`'s own `import "google/protobuf/descriptor.proto"`, same as the
        // Prometheus family above.
        includes: &[
            "crates/logit-proto/proto/datadog/agent-payload/proto",
            "crates/logit-proto/proto/datadog/include",
            "/usr/include",
        ],
        files: &[(
            "crates/logit-proto/proto/datadog/agent-payload/proto/metrics/agent_payload.proto",
            "datadog.agentpayload.rs",
        )],
        dest: "crates/logit-proto/src/datadog/generated",
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
