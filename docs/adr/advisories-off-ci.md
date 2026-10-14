---
created: 2026-10-14
updated: 2026-10-14
---

# CI runs no advisory check: Dependabot files the bump PRs, and `script/audit` checks locally

## Status
Accepted

## Context
`script/cibuild` ran `cargo deny check` and `cargo audit`, and both read the live RustSec
database. A new advisory against a locked dependency failed every open PR and `main` with no
change in the tree. #652 and #664 were hand-written bump PRs that existed only to clear such a
failure.

## Decision
CI runs the deterministic `cargo-deny` checks only: `bans`, `licenses`, and `sources`. No CI job
checks advisories, on PRs or on `main`. Dependabot security updates, a repository setting, open
the bump PR when an advisory has a fixed version. `script/audit` still runs every `cargo-deny`
check, advisories included, for local use, and no longer runs `cargo audit`, which reads the
same RustSec database as `cargo-deny`'s advisories check with a separate ignore list.

## Alternatives considered
- **Gate only PRs that touch `Cargo.lock`, plus a scheduled fix-up workflow.** Rejected: the
  workflow needs a personal access token to open PRs that trigger CI, a long-lived credential,
  and the gate adds a check that is red or green for reasons outside the PR.
- **Pin the advisory database snapshot.** Rejected: it needs plumbing to refresh the pin, and
  buys little once CI doesn't gate on advisories.

## Consequences
- A PR can merge while a locked dependency has an open advisory.
- Dependabot reads GitHub's Advisory Database, which imports RustSec after review, often hours
  to days after RustSec publishes.
- An advisory with no fixed version (an unmaintained crate, an unpatched flaw) produces a
  Security-tab alert and no PR. Nothing fails.
- `deny.toml`'s `[advisories] ignore` list matters only to local runs.
- `cargo-audit` stays installed in `Dockerfile.dev` until the next CI image republish.
