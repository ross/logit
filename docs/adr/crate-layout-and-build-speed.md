---
created: 2026-10-03
updated: 2026-10-03
---

# Crate layout: don't split crates to speed up the dev build

## Status
Accepted

## Context
The dev profile is `debug = "line-tables-only"`, with no debug info for dependencies. With it, a
cold `cargo build --workspace --all-targets` takes about 70 s on a 24-core machine. Without it,
the build took 124 s.

For the last 12–17 s, only one or two units are compiling. That pattern suggested splitting the
large crates, `logit-inputs`, `logit-outputs`, and `logit-proto`, so their compiles could run
side by side.

### What ends the build
The last units are the unit-test targets (`lib (test)`) of `logit-inputs` and `logit-outputs`:
- Each recompiles its whole crate under `cfg(test)`. Well over half of each crate's source lines
  are in-file tests.
- On `main`, each takes 23–30 s in one rustc process: a serial front end, then LLVM codegen
  across several threads.
- Neither can start until `logit-pipeline` is fully built, because a test target links its
  dependencies.

Three edges put more in front of them:
- **`logit-pipeline` → `logit-script`.** `logit-pipeline` depends on `logit-script` for the Lua
  node's `ScriptWorker`. That gates it behind the vendored LuaJIT build script, which runs 24–32 s.
- **`logit-outputs` → `logit-inputs` and `logit-script` (dev-dependencies).**
  - `logit-inputs` is used for about 23 encoder-against-decoder round trips through
    `StatsdDecoder` and `SyslogDecoder`, and about 13 transport tests against a real `LogitInput`.
  - `logit-script` is used for one Lua test in `influxdb.rs`.
- **`logit-inputs` → `logit-transforms` (dev-dependency),** for one integration test.

### What was measured
Three throwaway prototypes and an unchanged baseline were built from `main` at `201bb5a7`. Each
arm was built cold twice:
- in the dev container, run with a plain `docker run` (not `docker compose`), on 24 cores;
- with a fresh `CARGO_TARGET_DIR` for each arm, and dependencies fetched beforehand;
- with `cargo build --workspace --all-targets --timings`.

The incremental figure appends a comment line to `crates/logit-outputs/src/splunk.rs` and
rebuilds. That's the cheapest possible edit, so treat the figure as a floor.

| Arm | Change | Cold (s) | Edit a sink (s) |
|---|---|---|---:|
| A | none | 67.3, 72.4 | 6.0 |
| B | All three dev-dependency edges cut. 37 `logit-outputs` tests disabled. | 71.3, 74.6 | 5.4 |
| C | B, plus `tail/`, `docker`, and `docker_verification` (about 8,500 lines of tests) moved into a `logit-inputs-tail` crate | 73.1, 72.0 | 5.9 |
| D | B, plus `logit-pipeline` no longer depending on `logit-script` (Lua node stubbed) | 69.1, 68.4 | 5.6 |

Two cold runs of the same arm differ by up to 5 s, which is as much as any difference between arms.
Arm B's disabled tests make its `logit-outputs` test target smaller, which favors B. B still
didn't gain.

What each arm did:
- **B** let the `logit-outputs` test target start beside the `logit-inputs` one instead of after
  it. The `logit-inputs` test target, 34–35 s, became the last unit.
- **C** didn't shorten the `logit-inputs` test target: 31 s against 30 s on `main`. That's
  unexplained. The `logit-outputs` test target became the last unit again.
- **D** took LuaJIT off the critical path. Both test targets started at 26–30 s, against 37–40 s in
  B. They then took 35–42 s instead of B's 29–35 s, and ended where they had before.
- **A 2026-10-02 run gives independent evidence for D's result.** LuaJIT's `make` was parallelized
  there and ran in 7–8.5 s, which also took it off the critical path. The build still took
  68–74 s.

The box isn't saturated for most of the build. Across the eight cold runs, cargo's
`--timings` CPU graph averages 62–74% of the 24 cores. It reaches 93–99% only in an 8–12 s burst
when about 24 units start together. During the one-or-two-unit tail it reads 29–57%, which fits
the two test targets' codegen threads.

So the build's end is set by the duration of the largest single rustc invocation. That duration
grows when the invocation overlaps other work: in D, summed unit time rose from A's 507–592 s to
656–714 s. Moving a large test target earlier, or trimming part of one, didn't change when the
build ended. Where that time goes inside rustc wasn't profiled.

The timing reports and the scripts that drove and analyzed them are kept outside the repo, with
the rest of the 2026-10-02 and 2026-10-03 build-timing sessions.

## Decision
Don't split `logit-inputs`, `logit-outputs`, or `logit-proto`, or reshape crate edges, to speed up
the dev build. The layout in [`docs/design/pipeline-graph.md`](../design/pipeline-graph.md)'s
"Crate layout" section stays.

## Alternatives considered
- **Cut the dev-dependency edges (arm B).** No measurable gain, cold or incremental. Cutting them
  would also move tests:
  - the statsd and syslog round trips into `logit-proto`, with the decoders;
  - the `LogitInput` transport tests into `logit-cli/tests`, because they need a real
    `logit_in`.

  Make that move only on its own merits.
- **Split `logit-inputs` by driver family and `logit-outputs` by transport.** Arm C was the first
  step, and it didn't shorten the build. A full split would need every large test target well
  under today's length, so a three- or four-way split of both crates. Arm D suggests overlapping
  work would stretch the pieces.

  The cost is concrete:
  - a crate per family;
  - `pub` versions of today's `pub(crate)` driver items;
  - test helpers shared across crates;
  - new imports in `logit-cli`'s registry.

  Nothing measured pays for it.
- **Take `logit-script` off `logit-pipeline` (arm D),** with `logit-cli` registering the Lua node.
  It removed LuaJIT from the critical path but didn't shorten the build.
- **Split `logit-proto` into a trait-and-native-frame crate plus codec crates.** `logit-pipeline`
  needs only `native`, `frame`, `buffer`, `CodecError`, and two constants from it. `logit-proto`
  ended before the test targets in every arm, so this wasn't prototyped.
- **Parallelize LuaJIT's `make` with `MAKEFLAGS=-jN`.** Measured on 2026-10-02: 75 s against 79 s,
  inside the noise.

## Consequences
- The dev build stays about 70 s cold and about 6 s after a small edit to one sink.
- If cold builds become a bottleneck, measure these first:
  - **Profile the two large test targets.** Use `-Z self-profile` or `-Z time-passes`. They
    should show how the time divides between front end and codegen, and why moving about 8,500
    test lines out of `logit-inputs` didn't shorten its test target.
  - **Move unit tests out of the two large crates.** Each in-file `#[cfg(test)]` module makes
    cargo compile its crate a second time as one `lib (test)` invocation. Tests under `tests/`
    would compile as separate binaries against the built library, but they can only reach `pub`
    items. Profile first: if the time is codegen of dependencies' generic code, moving tests
    won't help. Merging integration-test binaries into one per crate was measured on 2026-10-02:
    it saved CPU time but no wall time.
  - **Build the tests without jemalloc.** `tikv-jemalloc-sys`'s build script runs 26–43 s
    alongside the test targets. It gates `logit-cli` and `logit-bench`. The `jemalloc` feature
    already has an off switch, `--no-default-features`
    ([ADR `jemalloc-global-allocator`](jemalloc-global-allocator.md)).
- Cutting the dev-dependency edges for test-organization reasons is still allowed. Don't expect it
  to shorten the build.
