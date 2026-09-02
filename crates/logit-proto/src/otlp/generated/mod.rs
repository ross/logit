//! Hand-written module tree wrapping the `prost-build` output in `generated/*.v1.rs`. Never
//! regenerated itself -- `script/protogen` only ever (re)writes the five sibling `.v1.rs` files.
//!
//! The nesting below exists for one reason: `prost-build` emits cross-package field types as
//! `super`-relative paths matching the `.proto` package hierarchy (e.g.
//! `crates/logit-proto/proto/opentelemetry/proto/resource/v1/resource.proto`'s `Resource.attributes`
//! comes out typed `super::super::common::v1::KeyValue`, since `opentelemetry.proto.resource.v1`
//! sits two levels below `opentelemetry.proto`, the same place `opentelemetry.proto.common.v1`
//! does). Those paths only resolve if this tree mirrors the proto package path exactly --
//! `opentelemetry::proto::{common,resource,logs,metrics,trace}::v1` -- so don't flatten or rename
//! it without re-running `script/protogen` and checking the diff compiles.

// `#[path = ...] mod v1;` (a real file-module), not `mod v1 { include!(...); }` -- inner
// attributes (`#![allow(clippy::all)]` etc., at the top of every generated file) are only valid
// at the true start of a file module; `include!`'s textual splice doesn't count as one, and
// rustc rejects them there.
//
// `#[rustfmt::skip]` on each `pub mod v1;` below -- not `#![rustfmt::skip]` inside the generated
// files themselves -- is what keeps `script/format`/`script/format --check` off this generated
// code: rustfmt honors `#[rustfmt::skip]` on a module *declaration* by skipping the file it names
// entirely, and the outer form is stable (the inner `#![rustfmt::skip]` form each generated file
// would otherwise want at its own top is nightly-only, rust-lang/rust#54726).
// Without help, an inline module's file-module children resolve against a *virtual* nested
// directory that accumulates one path segment per enclosing inline `mod` (e.g. `common`'s child
// `v1` would default to looking in `generated/opentelemetry/proto/common/`, not `generated/` --
// and that virtual directory doesn't exist, so even a `../../../` escape can't reach the real
// file: the OS has to actually open each component left-to-right, `opentelemetry` included).
// `#[path = "."]` on each intermediate inline module resets its children's base directory back to
// this file's own directory instead of descending further, so the leaf `#[path]`s below can stay
// simple flat filenames.
#[path = "."]
pub mod opentelemetry {
    #[path = "."]
    pub mod proto {
        #[path = "."]
        pub mod common {
            #[rustfmt::skip]
            #[path = "common.v1.rs"]
            pub mod v1;
        }
        #[path = "."]
        pub mod resource {
            #[rustfmt::skip]
            #[path = "resource.v1.rs"]
            pub mod v1;
        }
        #[path = "."]
        pub mod logs {
            #[rustfmt::skip]
            #[path = "logs.v1.rs"]
            pub mod v1;
        }
        #[path = "."]
        pub mod metrics {
            #[rustfmt::skip]
            #[path = "metrics.v1.rs"]
            pub mod v1;
        }
        #[path = "."]
        pub mod trace {
            #[rustfmt::skip]
            #[path = "trace.v1.rs"]
            pub mod v1;
        }
    }
}
