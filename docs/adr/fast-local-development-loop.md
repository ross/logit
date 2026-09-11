---
created: 2026-09-11
updated: 2026-09-11
---

# A shared build cache and one-container check execution for the local development loop

## Status
Accepted

## Context
The containerized workflow is reproducible, but its original isolation boundaries made small
changes increasingly expensive. Each worktree received separate Cargo and `target` volumes, and
each phase of `script/cibuild` launched a disposable Compose container. The toolchain file named
the moving `stable` channel while the base image carried a versioned toolchain, causing `rustup`
to download another toolchain into each new container layer.

Measurements on a 24-core development machine showed the impact:

| Operation | First measured run | Subsequent measured run |
|---|---:|---:|
| dev image build | 254 s cold | 1.0 s cached |
| format check | 17.2 s | 16.8 s |
| lint | 51.8 s | 16.2 s |
| workspace tests | 57.2 s | 83.7 s |
| shipped-config validation | 180.7 s | — |
| full safe preflight inside one settled container | — | 4.7 s |

The actual warm Cargo work was generally below two seconds; container startup and repeated
toolchain discovery dominated. Docker volumes left by 34 worktrees also held about 247.5 GB of
duplicated target artifacts and 8.8 GB of duplicated Cargo state.

The demo stack has different economics: service readiness was roughly 10 seconds and a release
image rebuild after a source change was roughly 126 seconds. That path is intentionally outside
this decision because it tests the production image and real service integration.

## Decision

- Pin Rust to one exact version in `rust-toolchain.toml`, development and production Dockerfiles,
  and CI. Install `rustfmt` and `clippy` into the development image explicitly.
- Pin `cargo-nextest`, `cargo-deny`, and `cargo-audit`. Give their image-build compilation stable
  BuildKit caches and separate layers.
- Give the Compose Cargo-home and target volumes fixed project-wide names. All worktrees share
  them. Cargo's own locking provides coordination for concurrent commands.
- Mark entry into the development container. Aggregate scripts re-enter themselves once, then
  call phase scripts directly rather than launching a container per phase.
- Provide `script/check` for the routine loop: format-check, workspace clippy, and all workspace
  tests. Keep `script/cibuild` as the CI-equivalent preflight by adding dependency audits.
- Enforce shipped-config validity and committed-schema freshness in ordinary tests. Keep
  `script/validate` and `script/schema` as explicit manual operations.
- Do not delete legacy worktree volumes automatically and do not optimize the demo's release
  image build as part of this work.

## Alternatives considered

- **Per-worktree caches.** They avoid cross-worktree Cargo lock contention, but their repeated
  compilation, downloads, and disk consumption overwhelm that benefit for normal development.
- **A long-running development container.** This saves startup too, but introduces lifecycle and
  stale-environment state that a single disposable container per top-level command avoids.
- **Skip config or schema checks in the fast path.** Rejected because those checks are cheap once
  expressed as tests and are precisely the regressions developers should see while iterating.
- **Optimize the demo release image at the same time.** Rejected because its production-build
  fidelity and service readiness are a separate workflow with different tradeoffs.

## Consequences

- A warm worktree benefits immediately from builds performed by any other worktree, and a new
  worktree no longer starts from an empty target directory.
- `script/check` and `script/cibuild` each pay Compose startup once. Standalone scripts retain the
  same interface.
- Concurrent builds may wait on Cargo locks, while incompatible branches may invalidate some
  shared artifacts. Exact toolchain pinning bounds the largest source of churn.
- Rust and cargo-tool upgrades are explicit maintenance changes across the pinned locations.
- Old Compose volumes remain until a developer inspects and removes each explicitly named legacy
  volume; routine scripts never destroy them.
