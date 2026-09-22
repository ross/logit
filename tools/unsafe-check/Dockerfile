# syntax=docker/dockerfile:1
# Throwaway image for `script/unsafe-check` only -- never built as part of `script/cibuild`, never
# the image any other script/* command runs in, and never `FROM logit-dev:local`. `Dockerfile.dev`
# is stable-only (ADR `containerized-development`); this image exists specifically to carry the
# nightly-only tools (miri, `cargo-careful`) that ADR verifies the codebase's raw-`libc` `unsafe`
# with, precedent for a purpose-built non-default image already set by
# `crates/logit-perf/Dockerfile` (perf/inferno) and `tools/protogen/Dockerfile` (protoc). See
# `docs/adr/out-of-ci-unsafe-verification.md`.
FROM debian:bookworm-slim

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000

# Same native build deps `Dockerfile.dev` installs -- this image builds the same workspace
# (mlua's vendored LuaJIT, the rest of the dependency graph), so it needs the same toolchain to
# get past `cargo build` at all. `Dockerfile.dev`'s own base image (`rust:*-bookworm`) gets a C
# compiler and `make` for free via its own base (`buildpack-deps:bookworm-scm`, which carries
# `build-essential`) -- this image starts from plain `debian:bookworm-slim`, so `build-essential`
# is explicit here (LuaJIT's vendored build is a real `make` invocation, `crates/logit-script/
# Cargo.toml`'s own `mlua = { features = ["vendored"] }`). Plus `curl`/`ca-certificates`/
# `xz-utils` to fetch rustup and a nightly toolchain (`rust:*-bookworm` isn't a usable base here --
# its rustup default toolchain is stable and pinned by `rust-toolchain.toml`'s presence in the
# bind-mounted workspace the moment any `cargo`/`rustup` command runs inside /work, so installing
# nightly *alongside* has to happen from a base image that doesn't already carry one) and `strace`
# for the `inject` subcommand.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        pkg-config \
        libssl-dev \
        git \
        ca-certificates \
        curl \
        xz-utils \
        strace \
    && rm -rf /var/lib/apt/lists/*

# Match the host user (see `compose.yaml`'s `dev` service and `Dockerfile.dev`'s own comment) so
# bind-mounted files written from inside the container come back owned by the host account, not
# root.
RUN groupadd --gid ${USER_GID} ${USERNAME} \
    && useradd --uid ${USER_UID} --gid ${USER_GID} --create-home --shell /bin/bash ${USERNAME}

ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/home/${USERNAME}/.cargo
# `cargo install`ed tools go under `TOOLS_ROOT`, not `${CARGO_HOME}/bin`: `script/unsafe-check`
# mounts a named volume over `/home/dev/.cargo` at runtime, and Docker seeds that volume from the
# image only the first time it is created -- after that the volume wins, so a tool installed into
# `~/.cargo/bin` would keep running at whatever version the volume was first seeded with, no
# matter how many times the pin below is bumped and the image rebuilt (the same trap
# `crates/logit-perf/Dockerfile` documents for `inferno`). This prefix is baked into the image and
# on no volume, so a rebuilt image is what runs.
ENV TOOLS_ROOT=/opt/unsafe-check
ENV PATH=${TOOLS_ROOT}/bin:${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:${PATH}

# Date-pinned nightly, not a floating `nightly` -- the whole point of `docs/adr/out-of-ci-
# unsafe-verification.md` is a *repeatable* check; a floating channel would mean "miri passed" or
# "miri failed" stops meaning the same thing between two runs a week apart. Bumping this date is a
# deliberate, reviewed act, the same way `docs/adr/committed-pregenerated-otlp-protobuf.md` treats
# regenerating the OTLP bindings.
#
# `rust-src` is miri's own dependency (it builds a good chunk of std under Miri's own instrumented
# target) and is not installed by `rustup component add miri` on its own.
ARG NIGHTLY_DATE=2026-09-01
# `RUSTUP_TOOLCHAIN`, not just `rustup default`: the workspace this image builds carries its own
# `rust-toolchain.toml` pinning the *stable* release (`Dockerfile.dev`'s own toolchain) -- rustup
# resolves that file from the current directory before falling back to the `default` set here, so
# every `cargo`/`rustc` invocation inside the bind-mounted `/work` would silently pick stable right
# back up without this override. `RUSTUP_TOOLCHAIN` sits above both `rust-toolchain.toml` and
# `rustup default` in rustup's own override precedence, which is exactly what's needed here.
ENV RUSTUP_TOOLCHAIN=nightly-${NIGHTLY_DATE}
RUN mkdir -p ${RUSTUP_HOME} ${CARGO_HOME} ${TOOLS_ROOT} \
    && chown -R ${USER_UID}:${USER_GID} ${RUSTUP_HOME} ${CARGO_HOME} ${TOOLS_ROOT}
USER ${USERNAME}
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
        --default-toolchain "nightly-${NIGHTLY_DATE}" \
        --profile minimal \
        --component miri \
        --component rust-src \
        --component clippy \
    && rustup default "nightly-${NIGHTLY_DATE}"

# Pinned, like every other `cargo install` in this repo's Dockerfiles (`Dockerfile.dev`,
# `crates/logit-perf/Dockerfile`). Real syscalls under a debug-assertion std
# (`docs/adr/out-of-ci-unsafe-verification.md`'s "what each tool is for" section) -- installed
# with the nightly toolchain above since building std from source (`-Zbuild-std`, which `cargo
# careful` drives under the hood) needs one.
ARG CARGO_CAREFUL_VERSION=0.4.10
RUN --mount=type=cache,id=logit-unsafe-check-cargo-registry,target=/home/dev/.cargo/registry,sharing=locked,uid=1000,gid=1000 \
    cargo install cargo-careful --version ${CARGO_CAREFUL_VERSION} --locked --root ${TOOLS_ROOT}

# `cargo-nextest`, matching `Dockerfile.dev`'s pinned version -- optional per the CLI's own help
# (nothing in `script/unsafe-check` requires it), carried along so a `nextest`-flavored invocation
# someone hands to `careful`/`inject` works the same way it does in the ordinary dev container.
ARG CARGO_NEXTEST_VERSION=0.9.144
RUN --mount=type=cache,id=logit-unsafe-check-cargo-registry,target=/home/dev/.cargo/registry,sharing=locked,uid=1000,gid=1000 \
    cargo install cargo-nextest --version ${CARGO_NEXTEST_VERSION} --locked --root ${TOOLS_ROOT}

# Pre-create the mount point for `logit_unsafe_check_target`, owned by the dev user -- Docker
# initializes a fresh named volume from whatever already exists at its mount point in the image,
# ownership included (`Dockerfile.dev`'s own comment on its matching `/work/target`); without
# this, Docker creates it root-owned on first use and every `cargo`/`cargo miri` build here fails
# to write to it. Back to root briefly since `/work` itself doesn't exist yet.
USER root
WORKDIR /work
RUN mkdir -p /work/target && chown ${USER_UID}:${USER_GID} /work /work/target

USER ${USERNAME}

CMD ["bash"]
