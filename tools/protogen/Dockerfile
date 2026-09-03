# Throwaway image for `script/protogen` only -- never built as part of `script/cibuild` or the
# dev image (Dockerfile.dev). Carries `protoc`, which ADR `containerized-development` deliberately keeps out of both.
FROM rust:1-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
