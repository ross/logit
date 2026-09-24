# syntax=docker/dockerfile:1
# Throwaway image for `script/perf flamegraph` only; script/cibuild never builds it and no other
# script/* command runs in it. It carries `perf` and `inferno`, which docs/adr/load-test-harness.md
# keeps out of Dockerfile.dev: the edit/check/test loop shouldn't pull in a profiler or the
# elevated capabilities (CAP_SYS_ADMIN, unconfined seccomp) `perf record` needs.
#
# Built from the dev image, not `rust:`, so the profiled binary comes from the toolchain every
# other build uses; a flamegraph of a differently built binary measures something nobody runs.
FROM logit-dev:local

USER root

# `linux-perf` is Debian's kernel-agnostic wrapper: /usr/bin/perf runs perf_<uname -r> if
# installed, else the newest it has, with a warning on stderr. Expect that warning: the container
# runs on the host's kernel, not Debian's.
RUN apt-get update && apt-get install -y --no-install-recommends \
        linux-perf \
    && rm -rf /var/lib/apt/lists/*

# Installed under `--root /usr/local`, not the dev user's CARGO_HOME: compose.yaml mounts a named
# volume over /home/dev/.cargo, initialized from the dev image, which would shadow anything
# installed into ~/.cargo/bin here. /usr/local/bin is on PATH and on no volume.
ARG INFERNO_VERSION=0.12.3
RUN --mount=type=cache,id=logit-perf-cargo-registry,target=/tmp/cargo-install/registry,sharing=locked \
    --mount=type=cache,id=logit-perf-cargo-target,target=/tmp/cargo-tools-target,sharing=locked \
    CARGO_HOME=/tmp/cargo-install CARGO_TARGET_DIR=/tmp/cargo-tools-target \
    cargo install inferno --version ${INFERNO_VERSION} --locked --root /usr/local

USER dev
