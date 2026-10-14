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
`ci.yml` restores the cache with `actions/cache/restore` on every run, under the exact lockfile
key with `restore-keys` set to the key's prefix, and saves it with `actions/cache/save` only on a
push to `main` that didn't hit the exact key. The key carries a version (`cargo-v3`); bumping it
starts the cache over.

## Alternatives considered
- **`Swatinem/rust-cache`.** It saves the same way and also prunes workspace artifacts from
  `target`, making entries smaller and restores faster. Rejected for now: it adds a third-party
  action to fix a problem the first-party split already fixes.
- **Cache the registry only.** Entries stay small and crates.io is still avoided, but every run
  compiles all 292 crates, about 6 minutes against 1m40s. Rejected.
- **Fetch the crates into the CI image.** No crates.io at all, but every Dependabot lockfile bump
  ([`advisories-off-ci`](advisories-off-ci.md)) would need an image republish
  ([`ci-image-on-ghcr`](ci-image-on-ghcr.md)). Rejected.

## Consequences
- One `main` entry serves every PR and every stacked branch. Every run reads it, so it's never
  the eviction victim while CI is active.
- A PR that changes `Cargo.lock` or `Cargo.toml` restores `main`'s newest entry through the
  prefix. Cargo downloads only the crates it lacks and rebuilds only the changed crates and their
  dependents, on every push to that PR until the change reaches `main`.
- A `main` run that restores through the prefix saves a new entry that includes the old one's
  `target`, so stale artifacts from replaced crate versions accumulate across lockfile bumps.
  Bump the key's version when entries grow enough to slow the restore.
- A `main` run that fails saves nothing, so the next `main` run restores the older entry again.
- The PR that introduces a new key version runs cold, and so does every PR until a push to
  `main` saves under it.
