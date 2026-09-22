---
created: 2026-09-22
updated: 2026-09-22
---

# Publish the release image to GHCR, `latest` only, on manual dispatch

## Status
Accepted

## Context
The production runtime image (`Dockerfile`, built by `script/image`) has nowhere to be pulled
from — `docs/deploying.md`'s "Getting the image" says so outright: no registry push step exists in
this repo, so running `logit` means cloning the repo and doing a release build first, a real
barrier for anyone who isn't a contributor. The repo is pre-release (workspace version a
placeholder `0.1.0`, zero git tags, no CHANGELOG — `Cargo.toml`), so this isn't the moment to build
a versioned release process; it's the moment to make `docker pull` work at all.

## Decision
Push the image to **GHCR** (`ghcr.io/ross/logit`), not Docker Hub: the repo's own `GITHUB_TOKEN`
already authenticates to it, so there's no second account and no registry secret to create and
rotate. GHCR also links a package to its source repo via the
`org.opencontainers.image.source` label, which the Dockerfile now sets.

Publishing is **manual only** — a new `.github/workflows/publish.yml`, triggered by
`workflow_dispatch` alone. Nothing pushes on a merge to `main`. There's exactly one tag,
`latest`, and exactly one platform, `amd64` — matching the Dockerfile's own `FROM` images, which
are not multi-arch built today.

The workflow builds through `script/image` itself, given `IMAGE_REPO=ghcr.io/ross/logit` and
`IMAGE_PUSH=1` (both new, and both no-ops when unset — a bare `script/image` still builds
`logit:local` and pushes nothing), rather than reimplementing the build in the workflow — one
build definition, per [ADR `scripts-to-rule-them-all`](scripts-to-rule-them-all.md). The publish
job runs without a `container:` directive, unlike `ci.yml`'s single job
([ADR `containerized-development`](containerized-development.md)): that job runs *inside* the
`rust:1.98.1-bookworm` image and has no Docker daemon of its own to build against, and GitHub-hosted
runners don't support docker-in-docker out of the box.

## Alternatives considered
- **Docker Hub.** Would need a second account and a stored username/token secret, for no benefit
  over a registry the repo's own token already authenticates to.
- **Push on every merge to `main`.** Moves a mutable `latest` under people without a human
  deciding it should, on a pre-release codebase with no release process to gate it — a
  `workflow_dispatch` keeps that a deliberate action.
- **`docker buildx` with a GitHub Actions layer cache (`type=gha`).** Would make repeat publishes
  fast, but routes around `script/image` (a second build definition to keep in sync) for a job
  that runs by hand and rarely. Worth revisiting if publishing becomes frequent.
- **Multi-arch via QEMU cross-build.** A real CI-time cost for an arm64 audience that doesn't exist
  yet; amd64-only matches the Dockerfile's current base images.

## Consequences
- `latest` is mutable and carries no version — it is explicitly not something to pin a deployment
  to. Adding real version tags is a named follow-up, not a promise this ADR makes.
- The package inherited this repo's own public visibility on its first push — GHCR packages linked
  to a public repo via `org.opencontainers.image.source` are public from the start, no manual
  visibility flip needed. (Confirmed against the actual first publish, 2026-09-22 — a private repo
  would need that step instead.)
- amd64-only means an Apple Silicon (or other arm64) user gets emulation or has to build locally
  via `script/image`.
- Nothing in CI builds or smoke-tests the production `Dockerfile` outside of this manual publish —
  `script/cibuild` still doesn't touch it (`docs/known-gaps.md`).
