# Production runtime image for logit (docs/plans/nginx-integration.md, workstream B).
#
# Unlike Dockerfile.dev (the container contributors build and test in), this is what a consumer
# builds or pulls to actually run `logit`. Built with `script/image`.

FROM rust:1-bookworm AS builder

# Build deps for the vendored LuaJIT build (mlua "luajit, vendored" features,
# crates/logit-script/Cargo.toml). No libssl-dev here: reqwest is pinned to rustls-tls
# (workspace Cargo.toml), so there's no OpenSSL to build or link against.
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work
# No dependency-layer caching (e.g. cargo-chef) here -- this image is built rarely (a release, or
# a local `script/image`), not on every save, so the extra tooling and build-graph complexity
# aren't worth it for the caching they'd buy.
COPY . .
RUN cargo build --release -p logit-cli

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --shell /usr/sbin/nologin logit

COPY --from=builder /work/target/release/logit /usr/local/bin/logit

USER logit
ENTRYPOINT ["logit"]
CMD ["--help"]
