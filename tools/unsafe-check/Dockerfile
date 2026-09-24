# syntax=docker/dockerfile:1
# Nightly-only image for script/unsafe-check (miri, cargo-careful, strace). Dockerfile.dev stays
# stable-only. See docs/adr/out-of-ci-unsafe-verification.md.
FROM debian:bookworm-slim

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000

# Dockerfile.dev's native build deps, plus `build-essential`, which its `rust:*-bookworm` base
# already carries: mlua's vendored LuaJIT build runs `make`. `curl`, `ca-certificates`, and
# `xz-utils` fetch rustup; `strace` serves `inject`.
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

# Match the host user, so bind-mounted files come back owned by the host account, not root.
RUN groupadd --gid ${USER_GID} ${USERNAME} \
    && useradd --uid ${USER_UID} --gid ${USER_GID} --create-home --shell /bin/bash ${USERNAME}

ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/home/${USERNAME}/.cargo
# `cargo install`ed tools go under TOOLS_ROOT, not ${CARGO_HOME}/bin: script/unsafe-check mounts a
# named volume over /home/dev/.cargo, and Docker seeds a volume from the image only when creating
# it. A tool there would stay at its first-seeded version however often the pin changes.
ENV TOOLS_ROOT=/opt/unsafe-check
ENV PATH=${TOOLS_ROOT}/bin:${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:${PATH}

# Date-pinned, not a floating `nightly`, so a miri verdict means the same thing from run to run.
# Bump it in its own reviewed change.
#
# miri needs `rust-src`, which `rustup component add miri` doesn't install.
ARG NIGHTLY_DATE=2026-09-01
# RUSTUP_TOOLCHAIN, not only `rustup default`: the workspace's rust-toolchain.toml pins stable
# and outranks the default inside /work. RUSTUP_TOOLCHAIN outranks both.
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

# cargo-careful rebuilds std with debug assertions (`-Zbuild-std`), which needs the nightly above.
ARG CARGO_CAREFUL_VERSION=0.4.10
RUN --mount=type=cache,id=logit-unsafe-check-cargo-registry,target=/home/dev/.cargo/registry,sharing=locked,uid=1000,gid=1000 \
    cargo install cargo-careful --version ${CARGO_CAREFUL_VERSION} --locked --root ${TOOLS_ROOT}

# Matches Dockerfile.dev's version. script/unsafe-check doesn't use it; it's here so a nextest
# invocation in `shell` works as it does in the dev container.
ARG CARGO_NEXTEST_VERSION=0.9.144
RUN --mount=type=cache,id=logit-unsafe-check-cargo-registry,target=/home/dev/.cargo/registry,sharing=locked,uid=1000,gid=1000 \
    cargo install cargo-nextest --version ${CARGO_NEXTEST_VERSION} --locked --root ${TOOLS_ROOT}

# Docker copies a mount point's ownership into a new named volume, so without this the target
# volume is root-owned and every build fails to write to it. Root only to create /work.
USER root
WORKDIR /work
RUN mkdir -p /work/target && chown ${USER_UID}:${USER_GID} /work /work/target

USER ${USERNAME}

CMD ["bash"]
