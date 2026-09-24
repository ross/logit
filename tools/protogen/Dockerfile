# Throwaway image for `script/protogen` only. Carries `protoc`, which ADR
# `committed-pregenerated-otlp-protobuf` keeps out of the dev and CI images.
FROM rust:1-bookworm

# On Debian, `libprotobuf-dev`, not `protobuf-compiler`, ships the well-known-types `.proto`
# sources. `gogoproto/gogo.proto` imports `google/protobuf/descriptor.proto`, so both are needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
