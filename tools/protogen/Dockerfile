# Throwaway image for `script/protogen` only -- never built as part of `script/cibuild` or the
# dev image (Dockerfile.dev). Carries `protoc`, which ADR `containerized-development` deliberately keeps out of both.
FROM rust:1-bookworm

# `protobuf-compiler` alone doesn't ship the well-known-types `.proto` sources (no
# `google/protobuf/*.proto` under /usr/include on Debian) -- `libprotobuf-dev` is the package that
# owns them. The gogoproto family's `gogoproto/gogo.proto` imports
# `google/protobuf/descriptor.proto`, so both are needed for `protoc` to resolve it.
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
