# syntax=docker/dockerfile:1
# Throwaway image for `script/perf flamegraph` only -- never built by script/cibuild, never the
# image any other script/* command runs in. Carries `perf` and `inferno`, which ADR
# `containerized-development` and docs/adr/load-test-harness.md deliberately keep out of
# Dockerfile.dev: nothing about the ordinary edit/check/test loop should pull in a profiler or the
# elevated container capabilities (CAP_SYS_ADMIN, unconfined seccomp) `perf record` needs.
# Precedent: tools/protogen/Dockerfile, which does the same for `protoc`.
#
# Built from the dev image, not from `rust:` directly, so the profiled binary is compiled by the
# exact toolchain every other build in this repo uses -- a flamegraph of a *differently* built
# binary would be measuring something nobody else runs.
FROM logit-dev:local

USER root

# `linux-perf` is Debian's kernel-version-agnostic wrapper package: /usr/bin/perf dispatches to
# perf_<uname -r> when one matching the running kernel is installed, and otherwise falls back to
# the newest it has, with a warning on stderr. That warning is expected here and harmless -- this
# container runs on whatever kernel the host has (Fedora, in this repo's case), not Debian's.
RUN apt-get update && apt-get install -y --no-install-recommends \
        linux-perf \
    && rm -rf /var/lib/apt/lists/*

# Installed with `--root /usr/local` rather than into the dev user's CARGO_HOME. compose.yaml
# mounts a named volume over /home/dev/.cargo, and that volume was initialized from the dev image
# long before this one existed -- anything installed into ~/.cargo/bin here would be shadowed the
# moment the container starts. /usr/local/bin is on PATH, on no volume, and owned by root, so the
# three `inferno-*` binaries are actually there at runtime.
ARG INFERNO_VERSION=0.12.3
RUN --mount=type=cache,id=logit-perf-cargo-registry,target=/tmp/cargo-install/registry,sharing=locked \
    --mount=type=cache,id=logit-perf-cargo-target,target=/tmp/cargo-tools-target,sharing=locked \
    CARGO_HOME=/tmp/cargo-install CARGO_TARGET_DIR=/tmp/cargo-tools-target \
    cargo install inferno --version ${INFERNO_VERSION} --locked --root /usr/local

USER dev
