---
created: 2026-09-21
updated: 2026-09-21
---

# Out-of-CI verification of raw-`libc` `unsafe`: a throwaway nightly image, not a Dockerfile.dev change

## Status
Accepted

## Context

Production `unsafe` in this codebase lives in exactly three files, one shared `libc` surface:
`crates/logit-inputs/src/udp.rs` (`recvmmsg(2)`, batched UDP receive), `crates/logit-pipeline/
src/sockstat.rs` (`getsockopt(SO_MEMINFO)`/`getsockopt(TCP_INFO)`), and `crates/logit-inputs/src/
tail/watch.rs` (a hand-rolled `inotify` backend: `inotify_init1`, `inotify_add_watch`,
`inotify_rm_watch`, a raw `read(2)` off the fd, and an unaligned read of `libc::inotify_event` out
of the kernel's own buffer). `docs/plans/critical-sections-inventory.md` names these NET-01,
NET-11, NET-12, and TAIL-07 and flags all four as `unsafe`/syscall entries worth a dedicated
verification session — every `unsafe` block already carries a `SAFETY:` comment (`AGENTS.md`'s
"stub code says so" discipline extends to this: an invariant claim next to the code, not just in a
plan doc), but a comment is an assertion, not a check. `libc/w1..w3` are the follow-up PRs that
factor the *pure* halves of these four entries into standalone, testable helpers (`parse_events`
in `watch.rs` already is one) and add miri/fault-injection-shaped tests against them; this PR
(`w0`) is the tooling those PRs run against, plus the lint that keeps every future `unsafe` block
in this surface carrying a truthful comment.

The obstacle is [ADR `containerized-development`](containerized-development.md): `Dockerfile.dev`
is deliberately stable-only, pinned by `rust-toolchain.toml`, because the ordinary edit/check/test
loop has no business depending on a moving nightly channel. But the tools that can actually say
something about raw pointer arithmetic, alignment, and syscall fault paths — miri and
`cargo-careful` — are nightly-only by their own nature (`cargo-careful` drives `-Zbuild-std` to get
a debug-assertion std; miri isn't shipped on stable at all). [ADR
`load-test-harness`](load-test-harness.md) already answered a structurally identical question for
`perf`/`inferno` (nightly is not required there, but the same principle — "a tool `Dockerfile.dev`
has no business carrying" — applies): a second, throwaway image (`crates/logit-perf/Dockerfile`),
built and run by its own script, never touched by `script/cibuild`. `tools/protogen/Dockerfile`
is the same shape again, one crate lighter, for `protoc`. This ADR takes that precedent and applies
it to nightly tooling specifically.

## Decision

`tools/unsafe-check/Dockerfile` builds a `debian:bookworm-slim`-based image carrying a
**date-pinned** nightly toolchain (`nightly-2026-09-01` at the time of writing, with `miri`,
`rust-src`, and `clippy` components), `cargo-careful` 0.4.10, `cargo-nextest` 0.9.144, `strace`,
and the same native build dependencies `Dockerfile.dev` installs (`build-essential`, `clang`,
`cmake`, `pkg-config`, `libssl-dev`, `git`, `ca-certificates` — the workspace's vendored-LuaJIT
`mlua` build needs a real `make`/`cc`, which `debian:bookworm-slim` doesn't carry the way `rust:*-
bookworm`'s own base image does). `RUSTUP_TOOLCHAIN` is set as an image-wide `ENV`, not left to
`rustup default`: the workspace's own `rust-toolchain.toml` pins stable and resolves ahead of
rustup's default in every directory under `/work`, so without this override every `cargo`
invocation inside the bind-mounted checkout would silently fall back to stable. `script/unsafe-
check` (bash, `script/common.sh`-sourced like every other `script/*`) builds this image and runs
it via a plain `docker run` — never `docker compose`, since this is not a `compose.yaml` service —
binding this checkout at `/work` explicitly, with its own named volumes for the cargo registry
cache and build artifacts (`logit_unsafe_check_cargo_home`, `logit_unsafe_check_target`),
**never** `logit_cargo_home`/`logit_target_cache`: nightly build artifacts must never land in the
stable toolchain's incremental cache, and the reverse (the stable dev container picking up a
nightly-built artifact) would be just as wrong. Subcommands:

- **`miri [cargo-miri-test args]`** — with no arguments, runs every `(package, libtest filter)`
  pair in the script's `MIRI_TARGETS` array, today just `logit-inputs|parse_events`
  (`watch.rs`'s `mod inotify::tests`, the pure event-buffer parser). `libc/w1..w3` extend this one
  array as their own pure helpers land; nothing else in the script changes.
- **`careful [args]`** — defaults to `cargo careful test -p logit-pipeline -p logit-inputs`, the
  two crates holding this cluster's `unsafe`, exercised through their *ordinary* real-socket/
  real-fd test suites (not a special subset) since `cargo careful`'s whole point is real syscalls
  under a debug-assertion std.
- **`inject <strace-inject-spec> [-- <cargo test args>]`** — builds the named test binary via
  `cargo test --no-run`, takes the compiled artifact's path from the `Executable ... (path)` line
  cargo prints for it (cargo's human status output, deliberately: it keeps the script free of a
  JSON parser on the host, and a change to that line breaks the run loudly rather than subtly),
  and runs it under `strace -f -e trace=<syscall> -e inject=<spec>`, where `<syscall>` is derived
  from the spec's own leading `syscallname:...` shape. This is the one subcommand that needs
  `--cap-add SYS_PTRACE` (Docker's default profile drops `CAP_SYS_PTRACE` — see "What each tool
  verifies" below) and, on some hosts, `--security-opt seccomp=unconfined` on top
  (`UNSAFE_CHECK_SECCOMP_UNCONFINED=1` in the environment opts into it; see "Running it" for which
  this repo's own dev box needed).
- **`all`** — `miri` then `careful`, both at their defaults.
- **`shell`** — an interactive shell in the image, for anything the three subcommands above don't
  cover directly.

### What each tool verifies, and its limits

**miri** has no shims for `recvmsg`/`recvmmsg`/`sendmmsg`/any `inotify*` syscall, and its
`getsockopt` shim's default implementation is unsupported for a real socket — the tool's own README
states plainly, "Miri currently does not support networking." Concretely: NET-01's `recvmmsg` call
itself, NET-11's two `getsockopt` calls, TAIL-07's `inotify_init1`/`inotify_add_watch`/
`inotify_rm_watch`/the raw `read(2)` off the inotify fd, and NET-12's kernel samplers (which are
built on NET-11) are **all unreachable under miri, full stop** — not "slow under miri," genuinely
outside what the tool can execute at all. What miri *can* reach is the surrounding pure logic: the
`mmsghdr`/`iovec` array construction in `BatchReader::read_batch` up to (but not including) the
`recvmmsg` call itself, the post-call `(*hdrs.add(i))` reads over kernel-filled memory *if* that
memory can be constructed without a real syscall, and — already true today — `parse_events` in
`watch.rs`, which decodes a raw `inotify_event` buffer with no fd of its own. This is exactly why
`libc/w1..w3` factor a pure decode/construct/harvest helper out of each entry rather than trying to
run the syscall-bearing function itself under miri: a function miri cannot execute is not a
function this tool can say anything about, no matter how the test around it is written.

**`cargo-careful`** runs the *real* test suite against std built with debug assertions (bounds
checks and validity checks that a release/ordinary-dev build elides) at close to native speed —
the opposite trade-off from miri: real syscalls, real sockets, real inotify fds, but a narrower
class of bugs caught (misaligned/null pointer operations, `get_unchecked` out-of-bounds, and
similar std-internal assertions — not the same soundness guarantees miri's abstract machine gives
for, say, provenance or data races). This is what actually exercises NET-01/NET-11/NET-12/TAIL-07
as they really run.

**`strace -e inject=SYSCALL:error=ERRNO:when=N[+M]`** forces a chosen call of a chosen syscall to
fail with a chosen errno — the only tool of the three that can put `EINTR`/`EAGAIN`/`ENOSYS`/
`EINVAL` in front of `recvmmsg`, `getsockopt`, or the inotify calls *on demand*, rather than hoping
a real kernel condition happens to occur during a test run. In Docker this needs `--cap-add
SYS_PTRACE` — strace's fundamental mechanism is a `ptrace(2)` attach, and Docker's default
capability set drops `CAP_SYS_PTRACE`. Older docker/libseccomp/kernel combinations went further
and blocked the syscall itself in the default seccomp profile regardless of capabilities (the
well-documented gap in Julia Evans's "Why strace doesn't work in Docker" and `moby#21051`, closed
in Docker 19.03 for kernels ≥ 4.8, where the profile allows `ptrace` once the capability is
granted); on such a host `--security-opt seccomp=unconfined` is needed on top of the capability,
which is what the `UNSAFE_CHECK_SECCOMP_UNCONFINED=1` escape hatch is for. This repo's own dev box
needed the capability alone.

## Alternatives considered

- **Nightly in `Dockerfile.dev`.** Rejected outright — this is precisely what ADR
  `containerized-development` already ruled out, and it would mean every contributor's ordinary
  `script/check` loop tracking a moving nightly channel for a need three files have.
- **Ad hoc, unrecorded local runs** (install nightly by hand on a dev box, run miri once, throw the
  setup away). Rejected: not repeatable the next time `libc`/`tokio` bump, and "did this pass under
  miri" becomes a claim nobody can re-check.
- **`cargo-fuzz` over these same call sites.** Deferred, not rejected — `cargo-fuzz` also needs
  nightly, and could in principle live in this same image. It solves a different problem (corpus-
  driven exploration of decoder input space) from what this ADR's three tools solve (soundness
  under miri's abstract machine, debug-assertion std, and forced syscall faults), and
  `docs/known-gaps.md`'s existing `cargo-fuzz` entry (over `crates/logit-proto`'s decoders, not
  this `libc` surface) already tracks it as future work in its own right. Folding it into this
  image without a concrete fuzz target to build first would be scope this PR doesn't need.
- **LD_PRELOAD/seccomp interposition** (a shim library that intercepts `recvmmsg`/`getsockopt`/
  `inotify_*` and injects failures without `ptrace`). Noted as a real alternative to `strace -e
  inject=` — lower per-call overhead, no `SYS_PTRACE` needed — but more machinery to build and
  maintain for this scope; `strace` is already present in every Linux distribution's package
  repository and needs no code of its own.
- **`FROM logit-dev:local`**, the way `crates/logit-perf/Dockerfile` builds atop the dev image.
  Rejected specifically for this image: the dev image's toolchain is `rustup`-managed but pinned to
  stable via `rust-toolchain.toml` baked into the bind-mounted workspace, not the image itself, so
  building atop it buys nothing over starting from a plain Debian base — and starting fresh makes
  the "this image is nightly, full stop" property visible in the Dockerfile itself rather than
  layered on top of a stable base image's own assumptions.

## Running it

`script/unsafe-check miri` and `script/unsafe-check careful` were run end to end against this
worktree while writing this ADR (image `logit-unsafe-check:local`, ~2.1 GB). One `inject` example
(`recvmmsg:error=EINTR:when=2+3` against `logit-inputs`' UDP burst test) was also run end to end;
this repo's own dev box needed `--cap-add SYS_PTRACE` alone, with no `seccomp=unconfined` fallback.
Real output, wall-clock times, and any workarounds are recorded in the `libc/w0` PR description
rather than duplicated here — this ADR is the decision record, not the run log.

## Consequences

- A new, throwaway, non-default image (`tools/unsafe-check/Dockerfile`) and script
  (`script/unsafe-check`) join `crates/logit-perf/Dockerfile`/`script/perf flamegraph` and
  `tools/protogen/Dockerfile`/`script/protogen` as the pattern for "a tool `Dockerfile.dev` has no
  business carrying." `Dockerfile.dev`, `rust-toolchain.toml`, `script/cibuild`, and CI are
  untouched by this decision.
- `[lints] workspace = true` was tried and reverted (see `libc/w0`'s commits): a workspace-wide
  `unsafe_op_in_unsafe_fn`/`clippy::undocumented_unsafe_blocks` deny has real fallout in
  `logit-bench`/`logit-perf`, dev-only crates with no stake in this cluster's review bar. The lint
  lands instead as a crate-level `#![deny(...)]` in `logit-inputs/src/lib.rs` and
  `logit-pipeline/src/lib.rs` only — the two crates that actually hold NET-01/NET-11/NET-12/
  TAIL-07 — so every `unsafe` block in this surface is required to carry a `SAFETY:` comment
  immediately preceding it, checked by `script/lint` on every ordinary run, not just when someone
  remembers to run this out-of-CI harness.
- Running this harness is not wired into `script/cibuild`, a pre-merge gate, or a schedule — same
  posture as `script/bench`/`script/perf`. It should be run by hand on any change to the three
  files this ADR names, and on any `libc`/`tokio` version bump (a version bump can silently change
  struct layout, syscall wrapper behavior, or an `AsyncFd`-adjacent assumption none of the three
  tools re-verify on their own schedule).
- `libc/w1..w3` are expected to extend `script/unsafe-check`'s `MIRI_TARGETS` array as they factor
  more pure helpers out of NET-01/NET-11/NET-12/TAIL-07, and to add their own fault-injection
  scenarios via `script/unsafe-check inject`. Nothing about this image or script should need to
  change shape to accommodate that — only the target list and the specs passed to `inject` grow.
- The nightly date (`NIGHTLY_DATE` build arg, `2026-09-01` at the time of writing) is a deliberate,
  reviewed value to bump, the same way `docs/adr/committed-pregenerated-otlp-protobuf.md` treats
  regenerating the OTLP bindings — not something to float to `nightly` for convenience, which would
  make "miri passed" stop meaning the same thing run to run.
