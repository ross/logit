---
created: 2026-10-14
updated: 2026-10-14
---

# CI saves the cargo cache from `main` only, and every run restores it with a prefix fallback

## Status
Accepted

## Context
`ci.yml` cached the cargo registry and `target` in one `actions/cache` step keyed on the
lockfile hash, so any run that missed saved a new entry. Each entry is about 2.6 GB, and the
repository's cache cap is 10 GB, so about four entries survive and GitHub evicts the least
recently used. On 2026-10-14 the four live entries all belonged to PR refs, and `main`'s had been
evicted.

A PR run reads caches from its own ref, its base branch, and `main` only. A stacked PR's base is
a branch that never runs CI, so it has no cache. When the ten-PR `priv/*` stack pushed at once
with one lockfile hash, most runs missed, each downloaded the registry from crates.io, and each
saved another entry that evicted a sibling's. On a miss a run takes about 11 minutes, against
about 3.5 on a hit: 252 crates downloaded and 292 compiled instead of 45, plus a 2.6 GB upload.
One miss on PR #685 also met a crates.io outage and failed, and a failed run saves nothing, so
its rerun missed again.

## Decision
`ci.yml` restores the cache with `actions/cache/restore` on every run and saves it with
`actions/cache/save` only on a push to `main` that didn't hit the exact key. The key is a version
(`cargo-v3`), the UTC month, and the lockfile hash. On `main`, `restore-keys` is the prefix
through the current month; on a PR, it's the prefix through the version, so it matches any month.
Bumping the version starts the cache over.

Cargo never removes build output from `target`, so a `main` entry restored through a prefix
carries every crate version built since the last clean build. Holding `main` to its own month
forces a clean build once a month.

## Alternatives considered
- **`Swatinem/rust-cache`.** It saves the same way and also prunes workspace artifacts from
  `target`, making entries smaller and restores faster. Rejected for now: it adds a third-party
  action to fix a problem the first-party split already fixes.
- **Cache the registry only.** Entries stay small and crates.io is still avoided, but every run
  compiles all 292 crates, about 6 minutes against 1m40s. Rejected.
- **A key with no time component, reset by bumping the version.** Nothing prompts the bump, so
  `target` grows until someone notices a slow restore. Rejected for the monthly reset.
- **Fetch the crates into the CI image.** No crates.io at all, but every Dependabot lockfile bump
  ([`advisories-off-ci`](advisories-off-ci.md)) would need an image republish
  ([`ci-image-on-ghcr`](ci-image-on-ghcr.md)). Rejected.

## Consequences
- One `main` entry serves every PR and every stacked branch. Every run reads it, so it's never
  the eviction victim while CI is active.
- A PR that changes `Cargo.lock` or `Cargo.toml` restores `main`'s newest entry through the
  prefix. Cargo downloads only the crates it lacks and rebuilds only the changed crates and their
  dependents, on every push to that PR until the change reaches `main`.
- Within a month, a `main` run that restores through the prefix saves a new entry that includes
  the old one's `target`, so artifacts from replaced crate versions accumulate across lockfile
  bumps. The first `main` push each month finds no entry, builds from scratch, about 11 minutes,
  and saves a clean one. Earlier months' entries go unread and are evicted after 7 days.
- Until that first `main` push, PRs restore the previous month's entry, so the reset costs one
  slow `main` run and no slow PR runs. If entries still grow enough to slow the restore, change
  the month to an ISO week (`date -u +%G-W%V`).
- A `main` run that fails saves nothing, so the next `main` run restores the older entry again.
- After a version bump, every PR runs cold until a push to `main` saves under the new version.
