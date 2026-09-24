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
    // `(parent generated file, child module name, child generated file)`: after the parent file
    // is written, append `#[path = "<child, renamed>"] pub mod <name>;` to it as an extra
    // top-level item -- see the Datadog family's entry for why a *generation-side* fixup, rather
    // than another hand-written nesting level in `generated/mod.rs`, is what a genuine
    // parent-package/child-package pair (as opposed to sibling packages, which OTLP's five are)
    // needs.
    nest: &'static [(&'static str, &'static str, &'static str)],
}

fn identity(name: &str) -> &str {
    name
}

// sketches-go's ddsketch.proto declares `package test;` (upstream's own placeholder package
// name, never renamed there) -- rewritten to the name the rest of this repo actually uses.
// Every other Datadog output file keeps prost-build's package-derived name as-is.
fn rename_datadog(name: &str) -> &str {
    match name {
        "test.rs" => "ddsketch.rs",
        other => other,
    }
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
        nest: &[],
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
        nest: &[],
    },
    Family {
        // `agent_payload.proto`'s (the agent-payload one, package `datadog.agentpayload`) `import
        // "github.com/gogo/protobuf/gogoproto/gogo.proto"` resolves against the `datadog/include`
        // root, which holds a relative symlink to the one vendored `gogoproto/gogo.proto` at that
        // Go-style import path (rather than a second copy) -- see
        // `crates/logit-proto/proto/README.md`'s Datadog section. `/usr/include` is still needed
        // for `gogo.proto`'s own `import "google/protobuf/descriptor.proto"`, same as the
        // Prometheus family above.
        //
        // The datadog-agent trace/stats files (package `datadog.trace`, plus `datadog.trace.idx`
        // for the `idx/` pair vendored only to satisfy `agent_payload.proto`'s -- the trace one's,
        // not the agent-payload one's -- import) resolve their `import "datadog/trace/..."` lines
        // against the `datadog-agent/pkg/proto` root, matching upstream's own include layout.
        // sketches-go's `ddsketch.proto` has no imports of its own but still needs its root on the
        // include path -- protoc requires every input file to sit under some declared include
        // directory.
        //
        // `agent_payload.proto` (the trace one)'s `idxTracerPayloads` field is typed
        // `idx.TracerPayload` -- a genuine *child* package (`datadog.trace.idx` sits directly
        // under `datadog.trace`), not a sibling like OTLP's five. prost-build emits that
        // cross-package reference as the bare, unqualified `idx::TracerPayload` (checked against
        // the actual generated output, not assumed) -- which only resolves if `idx` is declared as
        // a real child item inside whatever module holds `datadog.trace.rs`'s own top-level items.
        // A `#[path] mod trace;` file-module's content is fixed to exactly its file; nothing
        // outside it (`generated/mod.rs` included) can add a sibling `pub mod idx` inside that
        // scope. So `nest` below appends `idx`'s own `#[path] pub mod idx;` declaration straight
        // into the written `datadog.trace.rs`, as an ordinary extra item (item order doesn't
        // matter) -- both files stay genuine, independent file modules, each free to keep the
        // shared `HEADER`'s inner attributes at its own true start (see `generated/mod.rs`'s own
        // comment on why `mod trace { include!(..); pub mod idx { include!(..); } }` can't be used
        // instead: those inner attributes wouldn't be at the true start of a file module anymore).
        includes: &[
            "crates/logit-proto/proto/datadog/agent-payload/proto",
            "crates/logit-proto/proto/datadog/include",
            "crates/logit-proto/proto/datadog/datadog-agent/pkg/proto",
            "crates/logit-proto/proto/datadog/sketches-go",
            "/usr/include",
        ],
        files: &[
            (
                "crates/logit-proto/proto/datadog/agent-payload/proto/metrics/agent_payload.proto",
                "datadog.agentpayload.rs",
            ),
            (
                "crates/logit-proto/proto/datadog/datadog-agent/pkg/proto/datadog/trace/span.proto",
                "datadog.trace.rs",
            ),
            (
                "crates/logit-proto/proto/datadog/datadog-agent/pkg/proto/datadog/trace/tracer_payload.proto",
                "datadog.trace.rs",
            ),
            (
                "crates/logit-proto/proto/datadog/datadog-agent/pkg/proto/datadog/trace/agent_payload.proto",
                "datadog.trace.rs",
            ),
            (
                "crates/logit-proto/proto/datadog/datadog-agent/pkg/proto/datadog/trace/stats.proto",
                "datadog.trace.rs",
            ),
            (
                "crates/logit-proto/proto/datadog/datadog-agent/pkg/proto/datadog/trace/idx/span.proto",
                "datadog.trace.idx.rs",
            ),
            (
                "crates/logit-proto/proto/datadog/datadog-agent/pkg/proto/datadog/trace/idx/tracer_payload.proto",
                "datadog.trace.idx.rs",
            ),
            (
                "crates/logit-proto/proto/datadog/sketches-go/ddsketch/pb/ddsketch.proto",
                "test.rs",
            ),
        ],
        dest: "crates/logit-proto/src/datadog/generated",
        rename: rename_datadog,
        nest: &[("datadog.trace.rs", "idx", "datadog.trace.idx.rs")],
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
            let mut out = format!("{HEADER}{body}");
            for (parent, child_mod, child_generated) in family.nest {
                if *parent == *generated {
                    let child_short = (family.rename)(child_generated);
                    out.push_str(&format!(
                        "\n#[rustfmt::skip]\n#[path = \"{child_short}\"]\npub mod {child_mod};\n"
                    ));
                }
            }
            fs::write(Path::new(family.dest).join(short), out)
                .unwrap_or_else(|e| panic!("write {short}: {e}"));
            println!("wrote {}/{short}", family.dest);
        }
    }
}
