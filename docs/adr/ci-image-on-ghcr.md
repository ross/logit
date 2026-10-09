---
created: 2026-10-09
updated: 2026-10-09
---

# Run CI in an image prebaked from `Dockerfile.dev` and published to GHCR

## Status
Accepted

## Context
On 2026-10-09 four consecutive CI runs on PR #606 failed before any build step, because the runner
couldn't pull `docker.io/library/rust:1.98.1-bookworm`. Docker Hub's auth endpoint timed out, and
a later run hit `toomanyrequests: You have reached your unauthenticated pull rate limit`, a quota
shared by every GitHub-hosted runner behind the same egress address. Each job runs on a fresh VM
with no image cache, and `actions/cache` can't help because the runner pulls a `container:` image
before any step runs. Every run also repeats the apt, `rustup`, and `taiki-e/install-action`
downloads that the same toolchain needs.

## Decision
CI runs in an image published to `ghcr.io/ross/logit-ci`, built from the `ci` stage of
`Dockerfile.dev`. Contributors and CI share one build definition: the `dev` stage adds the
host-matched user on top of `ci`.

- **Publishing is manual.** `.github/workflows/publish-ci-image.yml` is `workflow_dispatch` only
  and calls `script/ci-image`, per [ADR `scripts-to-rule-them-all`](scripts-to-rule-them-all.md). It
  runs without `container:`, for the reason in
  [ADR `publish-release-image-to-ghcr`](publish-release-image-to-ghcr.md).
- **The tag is content-addressed.** It is the first 12 hex characters of `Dockerfile.dev`'s
  sha256. A tag that already exists is never overwritten, so a tag always names one toolchain.
- **`ci.yml` names the tag literally**, because a `container.image` expression can't call
  `hashFiles`. Its first step runs `script/ci-image check`, which compares the hash recorded in
  the image (`/etc/logit-ci-image`) with the checked-out `Dockerfile.dev` and fails the job on a
  mismatch. CI therefore never runs a toolchain the repo no longer declares.
- **The package links to the repo** through the `org.opencontainers.image.source` label, as the
  release image does, so it inherits the repo's visibility.
- **Changing `Dockerfile.dev` takes one PR.** The edit changes the hash, so the PR's own CI fails
  `check` until the new image exists. Dispatch the workflow on the PR's branch
  (`gh workflow run "Publish CI image" --ref <branch>`), which works once the workflow file is on
  `main`, then bump the tag in `ci.yml` in the same PR. Only the first image needs two PRs,
  because the workflow file isn't on `main` until its PR merges.

## Alternatives considered
- **Run on the runner without `container:`, with `dtolnay/rust-toolchain`.** Stops CI running on
  the Debian image contributors use, against
  [ADR `containerized-development`](containerized-development.md).
- **Authenticate the Docker Hub pull with a token secret.** Moves the quota to an account but
  doesn't help when the auth endpoint times out, and adds a secret to rotate.
- **A separate, trimmed `Dockerfile.ci`.** A second definition of the toolchain that drifts from
  `Dockerfile.dev`.
- **Tag `latest` or by date.** A toolchain bump wouldn't be an explicit `ci.yml` edit, and `latest`
  moves under every run.
- **A self-hosted runner.** A standing cost to avoid a pull problem.

## Consequences
- Every edit to `Dockerfile.dev`, comments included, changes the tag and needs a republish and a
  `ci.yml` bump.
- The base image still comes from Docker Hub, but only in the manual publish job, not on every CI
  run.
- The image holds no cargo registry, because the tool installs use build cache mounts. `ci.yml`'s
  `actions/cache` still covers the registry and `target`.
