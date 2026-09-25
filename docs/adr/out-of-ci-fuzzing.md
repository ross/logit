---
created: 2026-09-25
updated: 2026-09-25
---

# Out-of-CI fuzzing: a `cargo-fuzz` workspace in the unsafe-check image, with every crash landed as a stable test

## Status
Accepted

## Context

Every decoder that reads a peer's or the disk spool's bytes is covered today by a
stable-toolchain mutation suite
([ADR `deployment-threat-model`](deployment-threat-model.md) says why that input is accidental,
not hostile): `crates/logit-proto/tests/robustness.rs` truncates, flips bits, inflates declared lengths,
and nests past the depth cap, from a fixed seed. That suite finds what its author thought to
mutate. A coverage-guided fuzzer finds the rest, and `docs/known-gaps.md` has deferred one twice:
first because `cargo-fuzz` needs nightly and `Dockerfile.dev` is stable-only
([ADR `containerized-development`](containerized-development.md)), then again in
[ADR `out-of-ci-unsafe-verification`](out-of-ci-unsafe-verification.md)'s "Alternatives
considered", which built a nightly image but had no fuzz target to justify the scope.

The remote-reachable crash/DoS cluster of `docs/plans/critical-sections-inventory.md` (CORE-05,
CORE-06, WIRE-01..03, WIRE-06, WIRE-10/11/15, CODEC-16, CODEC-17) supplies those targets. Its
suggested approach is "mostly fuzz targets plus size and depth caps", and two of its decoders have
no robustness harness at all: `logit_core::sketch::DdSketch::from_bytes` and
`HyperLogLog::from_bytes`, both fed from `logit_in` peers and the disk spool.
[ADR `untrusted-input-bounds`](untrusted-input-bounds.md) records the caps. This ADR records the
fuzzer.

## Decision

Fuzz with `cargo-fuzz` (libFuzzer plus AddressSanitizer), from a `fuzz/` crate that lives outside
the root workspace, built and run in the existing `tools/unsafe-check` nightly image, by hand and
never in CI. Every crash it finds becomes a stable-toolchain regression test in the crate that owns
the decoder.

### The `fuzz/` crate

- `fuzz/Cargo.toml` declares `logit-fuzz` (`publish = false`, `[package.metadata] cargo-fuzz =
  true`) with its own `[workspace]` whose members are `.` and `seedgen`. This is the
  `tools/protogen` pattern: the root `Cargo.toml` needs no `exclude`, and `libfuzzer-sys` never
  enters the root `Cargo.lock`. `fuzz/Cargo.lock` is committed, seeded from the root lock so the
  shared dependencies resolve to the versions production runs.
- It has path dependencies on `logit-core` and `logit-proto` only. `logit-inputs` is out: it pulls
  in vendored LuaJIT, tokio, and rustls, which would all build under the sanitizer for no target
  that needs them. Where a target needs code that lives in `logit-inputs`, that code moves into
  `logit-proto` behind a seam first. The gRPC framing and bounded gzip inflate
  (`grpc_unframe`/`inflate`) move to `logit_proto::otlp::grpc` for this reason.
- Each target is one `[[bin]]` with `test = false` and `doc = false`. `[profile.release]` sets
  `debug = 1` so a crash's stack trace names lines.

### Targets

The canonical target table lives only in `script/unsafe-check`'s `FUZZ_TARGETS` array, one
`<target>|<max_len>|<malloc_limit_mb>` entry per target. Nothing else lists them. In prose, the
targets cover:

- the native frame envelope, read in a loop with `resync` the way the disk spool reads it;
- native batch decode, for both batch codecs (v1 and v2 with its provenance trailer);
- native control messages (`Hello`, `HelloAck`, `Ack`, `Reject`);
- `DdSketch` bytes (decode, then a `to_bytes` fixed-point oracle) and `DdSketch` merge (two decoded
  halves merged, then `quantile` and `to_bytes`);
- `HyperLogLog` bytes (decode, `estimate`, `insert`, `merge`, `to_bytes`, and drop, since the
  drop is where a wrong allocation `Layout` would surface);
- OTLP protobuf, OTLP/JSON, and gRPC framing followed by protobuf decode, with the first input
  byte choosing the signal;
- Prometheus remote-write decompression (oracle: output never exceeds the cap) and remote-write
  decode, with the first byte choosing 1.0 or 2.0.

### Seeds, corpus, and artifacts

- `fuzz/seeds/<target>/` is committed. `fuzz/seedgen`, a member of the fuzz workspace, writes it
  deterministically from `testdata/interop/{otlp,prometheus}/` captures and from encoder round
  trips of constructed events and sketches.
- `fuzz/corpus/`, `fuzz/artifacts/`, `fuzz/coverage/`, and `fuzz/target/` are gitignored. The
  driver passes `fuzz/corpus/<target>` first and `fuzz/seeds/<target>` second, because libFuzzer
  writes new inputs only to the first directory. A campaign never rewrites committed seeds.

### The driver

`script/unsafe-check` gains four subcommands:

- `fuzz <target> [-- <libFuzzer args>]` runs one target.
- `fuzz-all [seconds]` runs every target for the given time (default 120 seconds each), keeps
  going past a failing target, and prints a per-target summary with a `fuzz-tmin` line for each
  crash. Each target's output lands in `perf/results/fuzz/<timestamp>/<target>.log`.
- `fuzz-tmin <target> <crash file>` runs `cargo fuzz tmin`. An `oom-*` file gets the target's
  limits, because an allocation-limit crash reproduces only under `-malloc_limit_mb`. Any other
  crash gets none, because under the limit libFuzzer can minimize a panic down to an empty file
  that doesn't reproduce.
- `fuzz-seed` runs `seedgen` to regenerate `fuzz/seeds/`.

Every run passes the target's `-max_len` and `-malloc_limit_mb` from `FUZZ_TARGETS`, plus
`-rss_limit_mb=4096`, `-timeout=10`, and `-max_total_time=${FUZZ_SECONDS:-600}`. Caller arguments
come last, so they override any default. The per-target malloc limit is how the fuzzer checks
[ADR `untrusted-input-bounds`](untrusted-input-bounds.md)'s per-frame decode budget: a single
allocation past the limit is a crash. The limit is set slightly above
the largest allocation a valid input of `max_len` bytes can need. For the native frame that is 65
MiB, because a legitimate 64 MiB lz4 frame allocates its full `uncompressed_len`.

A fourth `FUZZ_TARGETS` column holds extra libFuzzer arguments for one target, passed by `fuzz`
and never by `fuzz-tmin`. The two native batch targets use it to run in fork mode (`-fork=1
-ignore_ooms=0`). The process-wide interner never evicts, so a long-lived fuzz process
accumulates every dictionary string it decodes until the arena outgrows the malloc limit, and
fork mode restarts the process. `-ignore_ooms=0` keeps an out-of-memory in a child fatal. Fork
mode was checked in the image: it stops at `-max_total_time`, merges new inputs into
`fuzz/corpus/<target>`, and writes a child's crash to `fuzz/artifacts/<target>/`.

### The image

`tools/unsafe-check/Dockerfile` installs a pinned `cargo-fuzz` with `cargo install --locked` next
to `cargo-careful` and `cargo-nextest`. The image already carries `build-essential` and `clang`,
and nightly's `rust-std` ships the ASan runtime. The one package added is `llvm`, for
`llvm-symbolizer`: without it, ASan prints a crash's stack as bare addresses. Fuzz builds use a new
named volume, `logit_fuzz_target`, because sanitizer `RUSTFLAGS` invalidate every artifact in the
miri and careful target volume and the reverse. The cargo-home volume is shared.

### Crash to regression test

A crash is not fixed in the fuzz crate. It is turned into a stable test:

1. Minimize it with `cargo fuzz tmin`.
2. Commit the minimized bytes as a `const` in the owning crate's robustness test file
   (`crates/logit-proto/tests/robustness.rs` or `crates/logit-core/tests/robustness.rs`), with a
   provenance comment that says what the input exercises, following `docs/design/memory.md`'s
   "Fixtures" section. The test asserts the exact error variant the fixed decoder returns, not the
   absence of a panic.
3. Save the same minimized file as `fuzz/seeds/<target>/regress-<short>`, so every later campaign
   starts from it.

### `HyperLogLog` needs two tools

`HyperLogLog::from_bytes` exists to avoid undefined behavior in `cardinality-estimator` 1.0.3,
which frees a member vector with a `Layout` whose size can differ from the one it was allocated
with. AddressSanitizer can't see that bug: Rust's `System` allocator drops the size when it calls
`free`, so a wrong-size `dealloc` looks correct to ASan. Miri checks the `Layout` on every
deallocation and does see it. An `HyperLogLog` fuzz regression therefore lands as a `logit-core`
lib test and joins `script/unsafe-check`'s `MIRI_TARGETS`, so Miri runs it.

### What `script/cibuild` doesn't cover

`fuzz/` is outside the root workspace, so `script/lint`, `script/audit` (`cargo-deny` and
`cargo-audit`), and `script/test` never build it. Nothing in `fuzz/` ships, and its only
dependency outside the root lock's set is `libfuzzer-sys`. `script/format` does reach it, with
`cargo fmt --manifest-path fuzz/Cargo.toml`, so `rustfmt.toml` holds there too. A `fuzz/` crate
that no longer compiles against a changed decoder API is found the next time someone runs the
harness, not by CI.

## Alternatives considered

- **Extend `crates/logit-proto/tests/robustness.rs` on stable and stop there.** Rejected as the
  whole answer. The seeded suite stays and keeps running in CI, but it explores only the mutations
  it names. It can't reach the input shapes a coverage-guided fuzzer finds by following branches,
  which is where a two-decoder interaction, such as a sketch blob inside a batch inside a frame,
  goes wrong.
- **A second nightly image for fuzzing.** Rejected. The `tools/unsafe-check` image already pins a
  nightly, carries `clang`, and has a driver script. A second image would repeat all of that and
  drift from it. Only the target volume needs to be separate.
- **Structured targets through `arbitrary`.** Rejected for these targets. Every decoder here takes
  bytes from a peer, so raw bytes are the input space an attacker controls. `arbitrary`-derived
  inputs start from well-typed values and reach the decoders only through an encoder, which skips
  the malformed inputs the fuzzer exists to find. A later target for an encoder-side invariant can
  use it.
- **Fuzzing in CI.** Rejected. A short CI run finds little, a long one makes CI slow and flaky, and
  either one ties CI to a moving nightly toolchain, which ADR `containerized-development` rules
  out. The harness runs by hand, like `script/perf` and the rest of `script/unsafe-check`.

## Consequences

- `docs/known-gaps.md`'s "`cargo-fuzz` targets over the decoders" entry closes when the harness
  lands. ADR `out-of-ci-unsafe-verification`'s deferral of `cargo-fuzz` is superseded by its
  amendment of the same date.
- A decoder change that breaks the `fuzz/` build isn't caught by CI. Run
  `script/unsafe-check fuzz-seed` and a short `fuzz-all` after changing a decoder a target calls.
- A new decoder of untrusted input should get a target: one `[[bin]]`, one `FUZZ_TARGETS` entry,
  and seeds from `seedgen`.
- A target can only call code in `logit-core` or `logit-proto`. Parsing logic that lives in a
  listener has to move behind a `logit-proto` seam before it can be fuzzed.

## Running it

### First campaign (2026-09-25)

`script/unsafe-check fuzz-seed`, then `fuzz native_frame -- -max_total_time=30` as a smoke test,
then `fuzz-all 120`, on the dev box, one target at a time, against `dos/w1`. Image
`logit-unsafe-check:local` with `nightly-2026-09-01` and `cargo-fuzz` 0.13.2. The smoke test ran
448,354 inputs in 31 seconds with no crash.

The executions per second and corpus size come from libFuzzer's last stats line. A failing target
stopped at its first crash, so its line is the last one before the crash, and its time is how long
the crash took to find. The two native batch rows are a rerun in fork mode, after the first run
of each stopped at about 40 seconds on the interner growth described below. Fork mode's stats
line gives no corpus size, and its exec/s is the executions divided by the seconds.

| Target | Seconds | Executions | exec/s | Corpus (inputs/size) | Result |
|---|---|---|---|---|---|
| `native_frame` | 121 | 1,346,503 | 11,128 | 263 / 577 KB | clean |
| `native_batch_v1` | 121 | 6,642,834 | 54,900 | 606 / n/a | clean (fork mode) |
| `native_batch_v2` | 122 | 2,423,419 | 19,864 | 510 / n/a | clean (fork mode) |
| `native_control` | 121 | 6,495,351 | 53,680 | 309 / 47 KB | clean |
| `sketch_bytes` | 121 | 4,502,624 | 37,211 | 520 / 929 KB | clean |
| `sketch_merge` | 121 | 3,231,840 | 26,709 | 591 / 1,754 KB | clean |
| `hll_bytes` | <1 | 38,579 | n/a | 88 / 22 KB | crash: `estimate` overflow |
| `otlp_proto` | 121 | 3,667,327 | 30,308 | 1,857 / 399 KB | clean |
| `otlp_json` | 121 | 2,006,625 | 16,583 | 1,540 / 405 KB | clean |
| `otlp_grpc` | 121 | 3,481,103 | 28,769 | 1,752 / 407 KB | clean |
| `prom_decompress` | 121 | 911,925 | 7,536 | 754 / 205 KB | clean |
| `prom_remote_write` | 121 | 1,739,336 | 14,374 | 1,544 / 1,126 KB | clean |

One finding for the workstream that owns the decoder, and one harness change:

- **`hll_bytes`: `HyperLogLog::estimate` overflows on a decoded estimator.**
  `cardinality-estimator` 1.0.3's `hyperloglog.rs` computes `M - zeros` from the estimator's
  stored zero-register count, and `HyperLogLog::from_bytes` accepts a count larger than `M`
  (4096). Under the fuzz build's debug assertions that panics with "attempt to subtract with
  overflow". A release build wraps and returns a meaningless estimate. The 3,097-byte reproducer
  doesn't minimize further, because the HLL representation's register array fills most of it. It
  is committed as `fuzz/seeds/hll_bytes/regress-9a8fd57a`, so `hll_bytes` fails at startup until
  the decoder rejects it (CORE-06).
- **`native_batch_v1`/`v2`: an `oom` that no single input reproduces.** The failing allocation is
  16 MiB (`malloc(16777240)`), from `lasso`'s arena growing a bucket under
  `logit_core::interner::intern`, called from `Dict::read`. The process-wide interner never
  evicts, so after about two million decoded dictionaries it outgrows the target's 16 MiB malloc
  limit. Rerunning the saved input alone doesn't crash, so `fuzz-tmin` has nothing to minimize.
  This is the native dictionary's cross-frame interner growth (WIRE-02), a documented non-goal
  under ADR `deployment-threat-model`, not a decoder bug. The two targets now run in fork mode
  (see "The driver"), and the rerun in the table is clean.
