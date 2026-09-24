# Production runtime image for logit, built with `script/image`. Dockerfile.dev is the
# contributor build-and-test container.

FROM rust:1.98.1-bookworm AS builder

# Build deps for the vendored LuaJIT build (mlua "luajit, vendored" features,
# crates/logit-script/Cargo.toml). No libssl-dev here: reqwest is pinned to rustls-tls
# (workspace Cargo.toml), so there's no OpenSSL to build or link against.
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work
# No dependency-layer caching (cargo-chef): this image is built rarely, so the caching isn't worth
# the extra tooling.
COPY . .
RUN cargo build --release -p logit-cli

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --shell /usr/sbin/nologin logit

COPY --from=builder /work/target/release/logit /usr/local/bin/logit

# GHCR links the package to the repo through image.source
# (docs/adr/publish-release-image-to-ghcr.md). No image.version: the workspace version is a
# pre-release placeholder, and stamping it would imply a release process that doesn't exist.
LABEL org.opencontainers.image.title="logit" \
      org.opencontainers.image.description="A logging/metrics/tracing multiplexer" \
      org.opencontainers.image.source="https://github.com/ross/logit" \
      org.opencontainers.image.licenses="MIT"

# Only effective when the config sets `admin.bind` (docs/deploying.md); otherwise `logit ready`
# finds nothing listening and exits 1, as for an unready process. Names the binary because
# HEALTHCHECK's exec form bypasses ENTRYPOINT.
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s CMD ["logit", "ready"]

USER logit
ENTRYPOINT ["logit"]
CMD ["--help"]
