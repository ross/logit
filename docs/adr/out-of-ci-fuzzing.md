---
created: 2026-09-25
updated: 2026-10-30
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
mutate. A coverage-guided fuzzer finds the rest, and `docs/known-gaps/` has deferred one twice:
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
`<target>|<max_len>|<malloc_limit_mb>|<extra args>` entry per target, where the fourth column
holds extra libFuzzer arguments (see "The driver"). Nothing else lists them. What each target
covers is described in prose under the "untrusted-input parsers" amendment below.

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
  crash. Each target's output lands in `perf/results/fuzz/<timestamp>/<target>.log`. It exits 1
  if any target failed.
- `fuzz-tmin <target> <crash file>` runs `cargo fuzz tmin`. An `oom-*` file gets the target's
  limits, because an allocation-limit crash reproduces only under `-malloc_limit_mb`. Any other
  crash gets none, because under the limit libFuzzer can minimize a panic down to an empty file
  that doesn't reproduce.
- `fuzz-seed` runs `seedgen` to regenerate `fuzz/seeds/`. `script/lint` runs `seedgen --check`, which writes nothing and fails on any added, removed, or changed generated seed, so an encoding change can't land with stale seeds.

Every run passes the target's `-max_len` and `-malloc_limit_mb` from `FUZZ_TARGETS`, plus
`-rss_limit_mb=4096`, `-timeout=10`, and `-max_total_time=${FUZZ_SECONDS:-600}`. Caller arguments
come last, so they override any default. The per-target malloc limit is how the fuzzer checks
[ADR `untrusted-input-bounds`](untrusted-input-bounds.md)'s per-frame decode budget: a single
allocation past the limit is a crash. The limit is set slightly above
the largest allocation a valid input of `max_len` bytes can need. For the native frame that is 65
MiB, because a legitimate 64 MiB lz4 frame allocates its full `uncompressed_len`.

The fourth `FUZZ_TARGETS` column holds extra libFuzzer arguments for one target, passed by `fuzz`
and never by `fuzz-tmin`. The two native batch targets, `statsd`, `syslog`, the two graphite
targets, `collectd`, `influxdb_line`, the two Prometheus decode targets, `message_json`,
`message_logfmt`, `message_kv`, and `msgpack_event` use it to run in fork mode (`-fork=1 -ignore_ooms=0`). The process-wide
interner never evicts, so a long-lived fuzz process accumulates every dictionary string, metric
name, tag key, SD-ID, PARAM-NAME, carbon path, Prometheus exemplar label name and family name,
log-message key, and MessagePack event attribute key it decodes until the arena outgrows the malloc limit, and fork mode restarts
the process. A key in `message_logfmt` or `message_kv` can run the whole 64 KiB message, and
long distinct keys grow the never-evicting arena past the 16 MiB limit within one fork child, so
those two targets skip an input that could hold a key over 32 bytes; a key's length changes
nothing in either scan.
`-ignore_ooms=0` keeps an out-of-memory in a child fatal. Fork mode was checked in the image: it
stops at `-max_total_time`, merges new inputs into `fuzz/corpus/<target>`, and writes a child's
crash to `fuzz/artifacts/<target>/`. Its first step merges the corpus and the seeds, and that
merge drops an input that crashes rather than reporting it. With `MAX_VALUES_PER_LIST`'s check
widened by one, `collectd` ran 120 seconds clean in fork mode, while the same binary outside fork
mode crashed on the committed seed that crosses the cap. So a fork-mode
target's `regress-*` seed is checked by its `robustness.rs` test or a run without `-fork`, not by
`fuzz`.

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

`fuzz/` is outside the root workspace, so clippy, `script/audit` (`cargo-deny`), and
`script/test` never reach it. Nothing in `fuzz/` ships, and its only dependency outside the root lock's set is `libfuzzer-sys`. `script/format` does reach it, with
`cargo fmt --manifest-path fuzz/Cargo.toml`, so `rustfmt.toml` holds there too, and `script/lint`
type-checks it on the stable toolchain, so a `fuzz/` crate that no longer compiles against a
changed decoder API fails CI (the 2026-10-11 amendment). The sanitizer builds and the runs stay
manual and nightly.

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
  bytes from a peer, so raw bytes are the input space a peer writes. `arbitrary`-derived
  inputs start from well-typed values and reach the decoders only through an encoder, which skips
  the malformed inputs the fuzzer exists to find. A later target for an encoder-side invariant can
  use it.
- **Fuzzing in CI.** Rejected. A short CI run finds little, a long one makes CI slow and flaky, and
  either one ties CI to a moving nightly toolchain, which ADR `containerized-development` rules
  out. The harness runs by hand, like `script/perf` and the rest of `script/unsafe-check`.

## Consequences

- `docs/known-gaps/`'s "`cargo-fuzz` targets over the decoders" entry closes when the harness
  lands. ADR `out-of-ci-unsafe-verification`'s deferral of `cargo-fuzz` is superseded by its
  amendment of the same date.
- CI type-checks `fuzz/` but never runs a target, so a decoder change that breaks a target's
  oracle passes CI. Run a short `fuzz-all` after changing a
  decoder a target calls, and `script/unsafe-check fuzz-seed` after a change `script/lint` reports as stale seeds.
- A new decoder of untrusted input should get a target: one `[[bin]]`, one `FUZZ_TARGETS` entry,
  and seeds from `seedgen`.
- A target can only call code in `logit-core` or `logit-proto`. Parsing logic that lives in a
  listener has to move behind a `logit-proto` seam before it can be fuzzed.

## Running it

### First campaign (2026-09-25)

`script/unsafe-check fuzz-seed`, then `fuzz native_frame -- -max_total_time=30` as a smoke test,
then `fuzz-all 120`, on the dev box, one target at a time, against the harness's PR (#371). Image
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

One finding for the workstream that owns the decoder, and one harness change, from the campaign
itself:

- **`hll_bytes`: `HyperLogLog::estimate` overflows on a decoded estimator.**
  `cardinality-estimator` 1.0.3's `hyperloglog.rs` computes `M - zeros` from the estimator's
  stored zero-register count, and `HyperLogLog::from_bytes` accepts a count larger than `M`
  (4096). Under the fuzz build's debug assertions that panics with "attempt to subtract with
  overflow". A release build wraps and returns a meaningless estimate. The 3,097-byte reproducer
  doesn't minimize further, because the HLL representation's register array fills most of it. It
  is committed as `fuzz/seeds/hll_bytes/regress-9a8fd57a`. The decoder fix is #369
  (CORE-06), and the seed decodes cleanly in the 1800 s campaign below.
- **`native_batch_v1`/`v2`: an `oom` that no single input reproduces.** The failing allocation is
  16 MiB (`malloc(16777240)`), from `lasso`'s arena growing a bucket under
  `logit_core::interner::intern`, called from `Dict::read`. The process-wide interner never
  evicts, so after about two million decoded dictionaries it outgrows the target's 16 MiB malloc
  limit. Rerunning the saved input alone doesn't crash, so `fuzz-tmin` has nothing to minimize.
  This is the native dictionary's cross-frame interner growth (WIRE-02), a documented non-goal
  under ADR `deployment-threat-model`, not a decoder bug. The two targets now run in fork mode
  (see "The driver"), and the rerun in the table is clean.

Review of this harness found two more, each with its reproducer committed as a `regress-*` seed:

- **`native_control`: a decoded `Reject` didn't re-encode within its own cap.** `Reject`'s decode
  checked the message's wire bytes against the 1,024-byte cap, then converted them with lossy
  UTF-8, which turns each invalid byte into a 3-byte U+FFFD. 342 bytes of `0xFF` decoded to a
  1,026-byte message that a second decode refused, and the target's round-trip oracle fired.
  Decode now truncates the converted message to the cap on a char boundary. The input is
  `fuzz/seeds/native_control/regress-reject-lossy`.
- **`sketch_bytes`: two finite bin counts can fold to `+inf`.** A version-1 Agent sketch with two
  positive-store bins at the same key, each with a count of `f64::MAX`, decodes. The bins fold
  into one infinite count after the non-finite filter has run, so `to_bytes` is not a fixed
  point and the oracle fires. It is committed as
  `fuzz/seeds/sketch_bytes/regress-bins-fold-to-inf`. The fix to the decoder's bin
  normalization is #369 (CORE-05), and the seed decodes cleanly in the 1800 s campaign below.

### Campaign at the stack tip (2026-09-25)

Every target ran clean for 1,800 seconds at the tip of the remote-reachable crash/DoS stack
(`d04a494`, plus the native batch targets' fix below), with the image rebuilt there. The three
`regress-*` seeds under `hll_bytes`, `sketch_bytes`, and `native_control` decode cleanly; before
#369, the first two made their targets fail at startup. `script/unsafe-check fuzz-seed` ran
first and rewrote no committed seed.

The targets ran four at a time, each with `script/unsafe-check fuzz <target> --
-max_total_time=1800`, sharing one fuzz target volume, on a host that other work was also
using. So the executions per second are lower than a quiet host gives, and they aren't
comparable with the 120-second table. The columns mean what they do in that table, and the two
fork-mode rows compute exec/s the same way.

| Target | Seconds | Executions | exec/s | Corpus (inputs/size) | Result |
|---|---|---|---|---|---|
| `native_frame` | 1,801 | 15,134,144 | 8,403 | 331 / 1,559 KB | clean |
| `native_batch_v1` | 1,840 | 63,473,912 | 34,497 | 996 / n/a | clean (fork mode) |
| `native_batch_v2` | 1,841 | 52,680,656 | 28,615 | 904 / n/a | clean (fork mode) |
| `native_control` | 1,801 | 65,362,641 | 36,292 | 295 / 38 KB | clean |
| `sketch_bytes` | 1,801 | 57,217,550 | 31,769 | 539 / 951 KB | clean |
| `sketch_merge` | 1,801 | 31,361,682 | 17,413 | 599 / 1,604 KB | clean |
| `hll_bytes` | 1,801 | 231,861,975 | 128,740 | 106 / 24 KB | clean |
| `otlp_proto` | 1,801 | 62,675,242 | 34,800 | 3,027 / 1,067 KB | clean |
| `otlp_json` | 1,801 | 20,681,285 | 11,483 | 3,249 / 8,867 KB | clean |
| `otlp_grpc` | 1,801 | 92,265,558 | 51,230 | 2,986 / 648 KB | clean |
| `prom_decompress` | 1,801 | 11,262,266 | 6,253 | 1,100 / 285 KB | clean |
| `prom_remote_write` | 1,801 | 24,410,001 | 13,553 | 2,677 / 2,187 KB | clean |

Two things the campaign turned up about the harness, not the decoders:

- **The native batch targets didn't build.** The decode budget (#370) added a `DecodeBudget`
  argument to `decode_batch` and `decode_batch_v2`, and the two targets still passed one
  argument. `fuzz/` is outside the cargo workspace, and no check built it then; `script/lint`
  now type-checks it (the 2026-10-11 amendment).
  Both targets now decode under `DecodeBudget::default()`, the budget `NativeDecoder` gives a
  frame under the default cap. At the time, "Consequences" said to run `fuzz-seed` and a short
  `fuzz-all` after changing a decoder a target calls. The rule now lives in "Consequences", and
  `script/lint` runs `seedgen --check` to catch stale seeds.
- **A host suspend reads as a timeout.** The host suspended for about 2.8 hours during the last
  four targets. On resume, libFuzzer counted the whole suspend against the input in flight and
  reported `timeout-*` artifacts in `prom_decompress` and both batch targets, and a
  10,315-second `slow-unit-*` in `prom_remote_write`. Each artifact runs in 1 ms on its own. The
  four targets were rerun from their seeds, and the table has the rerun. The driver now detects
  a suspend and reruns the target itself (the 2026-10-18 amendment).

After the campaign, both `native_batch_*` targets' `malloc_limit_mb` rose from 16 to 64, because
a valid batch of more than 16,384 empty events regrows `decode_batch_body`'s events `Vec` past
16 MiB (about 56 MiB at `max_len`), and none of the campaign's inputs reached that count.

### Campaign at the untrusted-input stack tip (2026-10-11)

Every target ran clean at the tip of the untrusted-input parser stack: the 11 cluster 9 targets
for 1,800 seconds each and the 14 earlier ones for 600. `stream_framing`, `statsd`, `syslog`, and
`graphite_plaintext` ran binaries built at `dc38374d`, the head of `untrusted/w13` (#621). The
other 21 ran binaries rebuilt from that tree plus a doc-comment-only change to `logit-core`, so
every decoder they ran is the same code, and the result stands for `dc38374d`. `script/unsafe-check fuzz-seed` ran first and rewrote no committed seed, so every
`regress-*` and `diff-*` seed is in the starting corpus. Image `logit-unsafe-check:local` with
`nightly-2026-09-01` (`rustc 1.100.0-nightly (0dfb098f3 2026-08-31)`) and `cargo-fuzz` 0.13.2.

All 25 targets were built once with `cargo fuzz build` into a fuzz target volume of their own,
then run four at a time, longest first, each with `FUZZ_SECONDS=<n> script/unsafe-check fuzz
<target>` and the extra arguments its `FUZZ_TARGETS` row gives, under `systemd-inhibit
--what=sleep:idle`, on a 32-core host. During the first four targets (`stream_framing`, `statsd`,
`syslog`, and `graphite_plaintext`) the host also ran the workspace tests and both differential
checks. The columns mean what they do in the first table: fork-mode rows compute exec/s from
executions and seconds and give no corpus size, and a fork-mode run's seconds include libFuzzer
wrapping up after `-max_total_time`.

| Target | Seconds | Executions | exec/s | Corpus (inputs/size) | Result |
|---|---|---|---|---|---|
| `native_frame` | 601 | 7,100,111 | 11,814 | 396 / 1,345 KB | clean |
| `native_batch` | 633 | 47,849,101 | 75,591 | 852 / n/a | clean (fork mode) |
| `native_hop_batch` | 633 | 52,643,559 | 83,165 | 863 / n/a | clean (fork mode) |
| `native_control` | 601 | 162,414,570 | 270,241 | 245 / 7 KB | clean |
| `sketch_bytes` | 601 | 45,633,443 | 75,929 | 552 / 944 KB | clean |
| `sketch_merge` | 601 | 27,308,137 | 45,438 | 605 / 1,929 KB | clean |
| `hll_bytes` | 601 | 155,884,819 | 259,376 | 118 / 23 KB | clean |
| `otlp_proto` | 601 | 27,745,848 | 46,166 | 2,708 / 1,423 KB | clean |
| `otlp_json` | 601 | 12,590,211 | 20,949 | 2,713 / 4,171 KB | clean |
| `otlp_grpc` | 601 | 37,675,556 | 62,688 | 2,839 / 617 KB | clean |
| `prom_decompress` | 601 | 10,722,781 | 17,842 | 1,035 / 494 KB | clean |
| `prom_remote_write` | 603 | 5,726,035 | 9,496 | 2,351 / n/a | clean (fork mode) |
| `prom_text` | 1,845 | 2,928,234 | 1,587 | 2,724 / n/a | clean (fork mode) |
| `proxy_header` | 601 | 507,045,221 | 843,669 | 162 / 5 KB | clean |
| `forwarded` | 601 | 313,586,245 | 521,774 | 580 / 17 KB | clean |
| `stream_framing` | 1,801 | 11,047,865 | 6,134 | 404 / 1,062 KB | clean |
| `statsd` | 1,838 | 33,723,292 | 18,348 | 1,310 / n/a | clean (fork mode) |
| `syslog` | 1,838 | 25,667,262 | 13,965 | 1,742 / n/a | clean (fork mode) |
| `graphite_plaintext` | 1,839 | 13,370,775 | 7,271 | 984 / n/a | clean (fork mode) |
| `graphite_pickle` | 1,838 | 1,604,989 | 873 | 1,066 / n/a | clean (fork mode) |
| `collectd` | 1,838 | 16,888,680 | 9,189 | 704 / n/a | clean (fork mode) |
| `message_json` | 1,843 | 77,079,815 | 41,823 | 3,285 / n/a | clean (fork mode) |
| `message_csv` | 1,801 | 33,224,847 | 18,448 | 204 / 32 KB | clean |
| `message_logfmt` | 1,838 | 31,992,832 | 17,406 | 470 / n/a | clean (fork mode) |
| `message_kv` | 1,837 | 53,008,188 | 28,856 | 576 / n/a | clean (fork mode) |

No target wrote an artifact: no crash, no out-of-memory against its `malloc_limit_mb`, no
timeout, and no slow unit. Three things the campaign turned up about the harness, none about the
decoders:

- **A campaign needs its own target volume and a checkout nobody edits while it runs.** Each
  `fuzz` call runs `cargo fuzz run`, which rebuilds when a source in the checkout changed. A
  change mid-campaign makes whichever run starts next rebuild, cargo's build-directory lock makes
  every other starting run wait for it, and each later target relinks its binary against the
  changed tree. Set `UNSAFE_CHECK_FUZZ_TARGET_VOLUME` so another checkout's build can't replace
  the binaries either.
- **A fork-mode run outlasts `-max_total_time` by up to 45 seconds**, the time its last
  child takes to finish and its corpus to merge, so a schedule should budget for it.
- **`graphite_pickle` and `prom_text` are the slow targets**, at about 900 and 1,600 exec/s:
  each input runs a second-generation fixed point (the pickle target in both protocols, the text
  target parsing and writing three times), so their 1,800 seconds reach fewer executions than a
  600-second run of most other targets.

## Amendment (2026-10-09): untrusted-input parsers

The inventory's cluster 9, "Untrusted-input parsers", gives every hand-rolled parser of producer
bytes a target. Most of those parsers live outside `logit-core` and `logit-proto` today, so
[ADR `parsers-live-in-logit-proto`](parsers-live-in-logit-proto.md) moves them behind the seam
this ADR's "The `fuzz/` crate" requires first. This amendment records what changes in the
harness. It holds the prose description of every target, which "Targets" points at; the
campaign tables under "Running it" keep the names each campaign ran under.

### Targets

`script/unsafe-check`'s `FUZZ_TARGETS` stays the one canonical table. In prose, the existing
targets cover:

- `native_frame`: the native frame envelope, read in a loop with `resync` the way the disk spool
  reads it;
- `native_batch` and `native_hop_batch`: the two native payload shapes, a bare batch and a hop
  batch with its sender pair and provenance trailer, both under the default decode budget;
- `native_control`: the native control messages;
- `sketch_bytes`, `sketch_merge`, and `hll_bytes`: `DdSketch` bytes (decode, then a `to_bytes`
  fixed-point oracle), `DdSketch` merge (two decoded halves merged, then `quantile` and
  `to_bytes`), and `HyperLogLog` bytes (decode, `estimate`, `insert`, `merge`, `to_bytes`, and
  drop, where a wrong allocation `Layout` would surface);
- `otlp_proto`, `otlp_json`, and `otlp_grpc`: OTLP protobuf, OTLP/JSON, and gRPC framing followed
  by protobuf decode, with the first input byte choosing the signal;
- `prom_decompress` and `prom_remote_write`: remote-write decompression (oracle: output never
  exceeds the cap) and decode, with the first byte choosing 1.0 or 2.0;
- `proxy_header`: the start of a stream through `logit_proto::proxy::parse`;
- `forwarded`: one forwarding header's value through `logit_proto::forwarded::parse`.

Cluster 9 adds one target per moved or already-movable parser:

- `stream_framing`: the TCP stream `Framer`, in every framing mode;
- `statsd` and `syslog`: the two line decoders;
- `graphite_plaintext`, `graphite_pickle`, and `collectd`: the carbon and collectd decoders, each
  with a second-generation encode/decode fixed point;
- `prom_text`: the Prometheus text and OpenMetrics decoder;
- `message_json`, `message_csv`, `message_logfmt`, and `message_kv`: the log-message tokenizers.

The InfluxDB line-protocol codec adds one more:

- `influxdb_line`: a whole write body through `InfluxDecoder::decode_body`, the first byte
  choosing the `precision`, with an all-or-nothing oracle against the body's lines parsed one at
  a time and a second-generation encode/decode fixed point. The same body also goes through
  `InfluxDecoder::decode_skipping`, the `line_protocol` transform's path, with an oracle that it
  skips each bad line in order, decodes the rest, and on a good body matches `decode_body`. Its
  seeds are the Telegraf-shaped body in `crates/logit-proto/tests/support/influxdb_lines.rs` and
  constructed lines.

Listener authentication adds one more:

- `http_auth`: one `Authorization` header value through `logit_proto::auth::Credentials::check`,
  against a bearer token and basic pair taken from the input, with oracles that an accepted value
  survives a flipped scheme case and surrounding whitespace and that a client's own spelling of
  each configured credential is accepted.

The CRI container log reader adds one more:

- `cri`: one container log line through `logit_proto::cri::parse`, with oracles that the content
  is the input's tail, that the three fields before it are separated by single spaces and name
  the stream and the first tag (`P` is partial, anything else full), and that the timestamp reads
  the same through `parse_rfc3339_to_nanos`. Its seeds are constructed lines in both runtimes'
  shapes and each malformed form.

The `kafka_in` consumer adds two more:

- `kafka_batch`: a Fetch response's records blob through `logit_proto::kafka::decode`, every
  record batch in it, with each records section decompressing to at most 64 KiB, then the walk
  `kafka_in` makes over each record's key, value, and header names. The bound sits far under
  `kafka_in`'s 64 MiB `max_batch_bytes`, so a section that expands past it is an `Oversize`
  error, not a buffer. Its seeds are the ten recorded segments under
  `testdata/interop/kafka/batches/`, those segments back to back and cut inside the last one, and
  `logit_proto::kafka::batch::encode`'s batch under each of the five codecs, plus an uncompressed
  batch with a repeated header name and one with a header name that isn't UTF-8, rewritten in
  place with the CRC-32C recomputed because the encoder can't write either.
- `msgpack_event`: a `format: msgpack` record value, one or more event maps back to back,
  through `MsgpackEventDecoder::decode_batches_with_budget` under a 1 MiB decode budget, which an
  input under its 64 KiB `max_len` can exhaust. Its seeds are `format: json` lines (logs,
  gauges, a sum, a histogram, a summary, and a span) written as MessagePack in `kafka_out`'s
  value forms, each alone and all back to back.

`prom_remote_write` also gains a structured mode, which builds a request from its input so the
symbol-table rules are reachable, and a fixed-point oracle on every input.

### Seeds and differential corpora

`seedgen` also reads `testdata/interop/{statsd,syslog,graphite,collectd,datadog,prometheus-scrape,kafka}/`,
the `graphite-pickle` and `prometheus-text` corpora under `testdata/differential/` (the
`datadog-sketch` corpus holds no decoder input), every `*.in` exposition body under
`crates/logit-cli/tests/fixtures/prometheus/`, and
`tools/record-fixtures/prometheus-metadata-target.prom`. A differential corpus differs from
`testdata/interop/`, which holds captured bytes that don't reproduce: it's generated,
deterministic, and carries a reference implementation's reading of each case beside the case's
bytes. `script/differential <corpus>`
regenerates one, and `script/differential <corpus> --check` regenerates it into a temporary
directory and fails if it differs from the committed files. No test needs the reference
implementation installed; tests read the committed reading.

### The `fuzz/` crate

- The seam list under "The `fuzz/` crate" grows by the moves ADR `parsers-live-in-logit-proto`
  names: `logit_proto::framing`, `logit_proto::statsd`, `logit_proto::syslog`, and
  `logit_proto::message::{json,csv,logfmt}`.
- The fuzz crate gains two third-party dependencies, `serde_json` (the reference reading for
  `message_json`) and `prost` (to build a structured remote-write request). Both are already in
  `fuzz/Cargo.lock` through the path dependencies, so neither adds a crate version.

### Crash to regression test

A crash's regression test lands in the `tests/robustness.rs` of the crate that owns the decoder,
whichever crate that is. After the moves that is `logit-proto` for every cluster 9 parser, but
the rule follows the code, not a list of two files.

### Debug assertions under fuzzing

The driver runs `cargo fuzz run` without `-O`. Without it, the pinned cargo-fuzz 0.13.2 adds
`-Cdebug-assertions` to the build's `RUSTFLAGS` (`src/project.rs`), and its `-a` help reads
"Build artifacts with debug assertions and overflow checks enabled (default if not -O)". rustc's
overflow checks follow debug assertions unless set separately. So every `debug_assert!` in a
parser fires under fuzzing, and integer overflow panics, in dependencies too: the `hll_bytes`
finding under "First campaign" is an overflow panic inside `cardinality-estimator`. Don't add
`-O` to the driver, because it turns both off.

### The `Framer`'s drain state gets an observable

Under `Oversize::DrainToNextLine`, a line that crosses the bound with no `LF` buffered is counted
once, as `logit.input.frames.dropped{reason="oversize"}`, and the `Framer` then discards every
byte until the next `LF`. A connection that never sends one, such as a binary protocol pointed at
a line port, keeps sending bytes into that discard for its whole life, and nothing distinguishes
it from a single long line that ended. That is reachable by accident, so it gets a free
observable rather than a `docs/known-gaps/` entry:

- A line that enters the drain state counts `logit.input.frames.dropped{reason="drained"}` in
  place of `reason="oversize"`, so each dropped line still counts once. A line whose `LF` is
  already buffered, and every other framing mode's oversize, keeps `reason="oversize"`.
- The throttled `framing_error` diagnostic for that line says the connection discards input until
  its next `LF`.

`docs/design/internal-telemetry.md`'s "Layer 3: what only a component knows" table holds the
canonical `frames.dropped` reasons. `untrusted/w6`, the branch that adds the `stream_framing`
target, adds `drained` there, to the reason lists in that doc's `statsd_in`, `graphite_in`, and
`lines_in` sections (the listeners that frame with `DrainToNextLine`), and to those listeners'
module docs.

## Amendment (2026-10-11): `script/lint` type-checks `fuzz/`

After clippy, `script/lint` runs `cargo check --manifest-path fuzz/Cargo.toml --workspace
--all-targets` on the stable toolchain the dev and CI images carry. `libfuzzer-sys`'s build script
compiles libFuzzer with the image's C++ compiler, and only `cargo fuzz build` and `cargo fuzz run`
need nightly, so the check fits the stable build: it took about 7 seconds from an empty target
directory. A decoder signature change that breaks a target or `seedgen` now fails `script/check`
and CI, where before it showed only at the next campaign's build.

The check doesn't build under the sanitizer, run a target, or lint `fuzz/`. A change to what a
decoder accepts can still break a target's oracle with every check green, so the
"Consequences" advice stands: run a short `fuzz-all` after changing a decoder a target calls.
`script/lint` also runs `seedgen --check`, which fails on a stale seed until
`script/unsafe-check fuzz-seed` regenerates it.

## Amendment (2026-10-18): a run the host slept through is rerun, not kept awake

The host that fuzzes may be a laptop that suspends, closed in a sleeve, where holding it awake
heats it. So nothing in the harness inhibits sleep. The driver detects a run that spanned a
suspend and reruns it once the host is awake, as `script/soak` does for a stalled run
([ADR `soak-harness`](soak-harness.md)'s 2026-10-18 amendment).

### What libFuzzer does across a suspend

libFuzzer, as `libfuzzer-sys` 0.4.13 bundles it, reads every duration off
`std::chrono::system_clock`, which is `CLOCK_REALTIME`, and a suspend advances it:

- **`-timeout` fires on the input in flight.** `SetSignalHandler` arms `setitimer(ITIMER_REAL)`
  every `timeout / 2 + 1` seconds (`FuzzerUtilPosix.cpp`), and `Fuzzer::AlarmCallback`
  (`FuzzerLoop.cpp`) measures the running unit as `system_clock::now() - UnitStartTime`. The
  first alarm after a resume counts the whole suspend against that unit, writes `timeout-<sha1>`,
  and exits with `-timeout_exitcode`, 70 by default. Outside fork mode that ends the run as a
  failure.
- **In fork mode the false timeout is quieter.** `-ignore_timeouts` defaults to 1, so
  `FuzzWithFork` (`FuzzerFork.cpp`) counts a child's exit 70 and goes on, but the child has
  already written its `timeout-*` artifact into `fuzz/artifacts/<target>/`.
- **A unit that finishes across a suspend is a `slow-unit-*`.**
  `PrintPulseAndReportSlowInput` writes one when the unit took over `-report_slow_units` seconds
  (default 10) by the same clock; it doesn't fail the run.
- **`-max_total_time` counts the suspend.** `secondsSinceProcessStartUp()` (`FuzzerInternal.h`,
  and its fork-mode copy in `FuzzerFork.cpp`) is also `system_clock`, so the run ends early, with
  fewer executions than its budget asked for.

The 2026-09-25 campaign's suspend left `timeout-*` artifacts in three targets and a
10,315-second `slow-unit-*` in a fourth, each of which ran in 1 ms on its own.

### Decision

- **Detection is on the host, around each target's `docker run`.** `CLOCK_BOOTTIME` counts a
  suspend and `CLOCK_MONOTONIC` doesn't, so their difference grows by the suspend's length. The
  driver reads it before and after the run, and growth of 5 seconds or more means the run slept.
  A suspend has to last about as long as the 10-second `-timeout` the driver passes to fake a
  timeout, so half that catches every suspend that can. Bash has no monotonic clock (`date` and `$EPOCHREALTIME` read `CLOCK_REALTIME`), so
  the driver reads both clocks with `python3`'s `time.clock_gettime`, a new host dependency of
  `fuzz` and `fuzz-all`.
- **A slept run is neither a pass, a crash, nor a timeout finding.** Its new `timeout-*` and
  `slow-unit-*` artifacts move to `fuzz/artifacts/<target>/slept/`, out of the directory a crash
  is listed from, and are kept so a doubtful one can still be run by hand. A slept run that
  failed and left any other new artifact (`crash-*`, `oom-*`, `leak-*`) keeps its own exit
  status and its `fuzz-tmin` line, because none of those depends on time. `fuzz-all`'s
  `fuzz-tmin` line names the last `crash-*`, `oom-*`, `leak-*`, or `timeout-*` file the log
  names that's still in the target's artifacts directory, so it never names `slept/` or a
  timeout moved there.
- **A slept run is rerun.** `FUZZ_RETRY_SLEPT` (default 1) reruns it right away, since the
  driver only runs while the host is awake. The rerun starts from the corpus the slept run
  grew: coverage doesn't depend on time, so those inputs stand. That's why the default is 1
  where `script/soak`'s `--retry-stalled` defaults to 0: a fuzz rerun costs one target's time
  and loses nothing, while a soak rerun replays a whole scenario.
- **A target whose last attempt slept exits 3**, as a stalled soak run does. `fuzz-all` lists it
  as `slept`, never `ok` or `FAILED`, and exits 1 if any target failed, else 3 if any slept.
  That reverses soak's order, where 3 outranks 1, because a stall leaves nothing in a soak run
  to judge, while each `fuzz-all` target is its own run and another target's crash still needs
  reading.
- **`UNSAFE_CHECK_FAKE_SUSPEND=N` makes each target's first `N` attempts read as slept**, the
  seam that tests the retry path without suspending the host. `fuzz-self-test` checks the
  detection against simulated clock readings and the `fuzz-tmin` line's artifact against
  simulated slept-run logs.

### Limits

- A `CLOCK_REALTIME` step, such as an NTP correction, moves libFuzzer's clock without moving
  `CLOCK_BOOTTIME` against `CLOCK_MONOTONIC`, so the driver doesn't see it. The development
  host's chrony steps the clock only at boot.
- A suspend shorter than 5 seconds isn't detected. It can't fake a timeout, and it shortens the
  run by under 5 seconds.
