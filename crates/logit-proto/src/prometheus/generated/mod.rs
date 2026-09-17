//! Hand-written module tree wrapping the `prost-build` output in the two sibling files
//! (`prometheus.rs`, `io.prometheus.write.v2.rs`) -- never regenerated itself, `script/protogen`
//! only ever (re)writes those two files. See
//! [ADR `committed-pregenerated-otlp-protobuf`](../../../../../docs/adr/committed-pregenerated-otlp-protobuf.md)
//! (this crate's OTLP family is where that ADR is written up; this second family follows the same
//! scheme) and `crates/logit-proto/proto/README.md`'s "Vendored Prometheus prompb" section for the
//! pinned tag/commit.
//!
//! Unlike OTLP's five interdependent packages, the two files here share no cross-package field
//! types (`remote.proto`/`types.proto` -- one merged `prometheus.rs` output, since both declare
//! `package prometheus;` -- and the 2.0 `io.prometheus.write.v2` file are checked, at vendoring
//! time, to reference no type outside their own package), so there is no `super::`-relative path
//! to preserve by nesting. The `io::prometheus::write::v2` nesting below exists only to mirror the
//! wire's own package name; flattening `prometheus.rs` would not break anything, but the naming
//! symmetry with `crate::otlp::generated`'s scheme is kept anyway.
//!
//! `#[path = ...] mod ...;` (a real file-module), not `mod ... { include!(...); }` -- inner
//! attributes (`#![allow(clippy::all)]` etc., at the top of every generated file) are only valid
//! at the true start of a file module; `include!`'s textual splice doesn't count as one, and rustc
//! rejects them there.
//!
//! `#[rustfmt::skip]` on each generated file's module declaration -- not `#![rustfmt::skip]`
//! inside the generated files themselves -- is what keeps `script/format`/`script/format --check`
//! off this generated code: rustfmt honors `#[rustfmt::skip]` on a module *declaration* by
//! skipping the file it names entirely, and the outer form is stable (the inner
//! `#![rustfmt::skip]` form each generated file would otherwise want at its own top is
//! nightly-only, rust-lang/rust#54726).

/// The 1.0 remote-write / remote-read types (`prometheus.WriteRequest`, `TimeSeries`, `Label`,
/// `Sample`, `Exemplar`, `Histogram`, `MetricMetadata`, ...), from `remote.proto` + `types.proto`.
#[rustfmt::skip]
#[path = "prometheus.rs"]
pub mod prometheus;

// `#[path = "."]` on each inline intermediate module resets its children's base directory back to
// this file's own directory instead of descending further (without it, an inline module's
// file-module children resolve against a *virtual* nested directory that accumulates one path
// segment per enclosing inline `mod` -- see `crate::otlp::generated`'s module doc for the fuller
// explanation of why).
#[path = "."]
pub mod io {
    #[path = "."]
    pub mod prometheus {
        #[path = "."]
        pub mod write {
            /// The 2.0 remote-write types (`io.prometheus.write.v2.Request`, `TimeSeries`,
            /// `Metadata`, ...), from `prompb/io/prometheus/write/v2/types.proto`.
            #[rustfmt::skip]
            #[path = "io.prometheus.write.v2.rs"]
            pub mod v2;
        }
    }
}

#[cfg(test)]
mod tests {
    // Remote-write's body is Snappy **block** (not framed) compressed protobuf
    // (docs/plans/prometheus-remote-write.md) -- `snap` isn't wired into a codec yet (that's W2's
    // `remote_write.rs`), so this sanity-checks the dependency round-trips before anything depends
    // on it.
    #[test]
    fn snap_block_round_trip() {
        use prost::Message;

        let series = super::prometheus::TimeSeries {
            labels: vec![super::prometheus::Label {
                name: "__name__".to_string(),
                value: "up".to_string(),
            }],
            samples: vec![super::prometheus::Sample { value: 1.0, timestamp: 1_000 }],
            ..Default::default()
        };
        let body = series.encode_to_vec();

        let compressed = snap::raw::Encoder::new().compress_vec(&body).expect("snappy compress");
        let decompressed =
            snap::raw::Decoder::new().decompress_vec(&compressed).expect("snappy decompress");
        assert_eq!(decompressed, body);

        let round_tripped =
            super::prometheus::TimeSeries::decode(decompressed.as_slice()).expect("prost decode");
        assert_eq!(round_tripped, series);
    }
}
