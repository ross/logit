# 0005 — Containerized development environment

## Status
Accepted

## Context
No Rust, LuaJIT, or related build tooling is installed on the primary dev machine, and the project
should be easy for others to pick up without host setup. The dev machine has Docker Engine 29.7.2 /
Compose v5.4.0 reachable via passwordless sudo, and SELinux Enforcing; rootless Podman 5.8.4 is also
available and works as a drop-in.

## Decision
Docker is the primary local runtime, driven through a `Makefile` wrapping `docker compose`
(`DOCKER ?= sudo docker`, overridable to `docker` or `podman`). Repo artifacts (`Dockerfile.dev`,
`compose.yaml`) are kept plain and runtime-agnostic — no Docker- or Podman-specific extensions — so
either runtime works unmodified. CI runs the same image.

Two environment-specific details are handled explicitly rather than left to be discovered by
whoever hits them first:
- Bind mounts use the `:z` SELinux relabel suffix.
- The `dev` service runs as `user: "1000:1000"` against a matching UID/GID baked into the image, and
  the `target/` named-volume mount point is pre-created and chowned in the image so Docker
  initializes the volume already owned by that user rather than by root.

## Alternatives considered
- **Rootless Podman as the primary target.** Avoids `sudo` entirely and was the first plan, before
  it turned out Docker is available here via passwordless sudo. Still fully supported as a drop-in
  (`make DOCKER=podman ...`) since the compose file has no Docker-specific features.
- **Host toolchain install (rustup, apt packages) documented instead of containerized.** Rejected:
  reintroduces "works on my machine" drift and a setup step for every contributor, which is exactly
  what this decision exists to avoid.

## Consequences
- `sudo` is required for every `make` target on this machine unless `ross` is added to the `docker`
  group (noted in the README as the removal path).
- Anyone in a `docker` group, or using rootless Podman, overrides one Makefile variable and needs no
  other changes.
