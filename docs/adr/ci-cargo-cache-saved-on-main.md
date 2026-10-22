---
created: 2026-10-14
updated: 2026-10-21
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
(`cargo-v4`; see the amendment), the UTC month, and the lockfile hash. On `main`, `restore-keys` is the prefix
through the current month; on a PR, it's the prefix through the version, so it matches any month.
Bumping the version starts the cache over.

Cargo never removes build output from `target`, so a `main` entry restored through a prefix
carries every crate version built since the last clean build. Holding `main` to its own month
forces a clean build once a month. The amendment adds a prune step that keeps workspace
artifacts out of the entry.

## Alternatives considered
- **`Swatinem/rust-cache`.** It saves the same way and also prunes workspace artifacts from
  `target`, making entries smaller and restores faster. Rejected: it adds a third-party action
  to fix a problem the first-party split already fixes. The amendment's prune step does the same
  pruning in a few lines of shell. Revisit it if that step needs more than `cargo clean -p`.
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
  the old one's third-party artifacts, so artifacts from replaced crate versions accumulate
  across lockfile bumps. Workspace artifacts don't, because the prune step removes them before
  the save. The first `main` push each month finds no entry, builds from scratch, about 11 minutes,
  and saves a clean one. Earlier months' entries go unread and are evicted after 7 days.
- Until that first `main` push, PRs restore the previous month's entry, so the reset costs one
  slow `main` run and no slow PR runs.
- A `main` run that fails saves nothing, so the next `main` run restores the month's older
  entry again, or builds from scratch again if the failed run was the month's first.
- After a version bump, every PR runs cold until a push to `main` saves under the new version.

## Amendment (2026-10-21): prune workspace artifacts before the save

### Context
The decision above left `target` growing within a month, and it grew much faster than the
2.6 GB the record assumed. On 2026-10-21, `main`'s entry was 8.3 GB compressed, up from 2.6 GB
seven days earlier, and restored to 26 GB:

| Path | Size | Contents |
|---|---|---|
| `target/debug/incremental` | 17 GB | Six incremental directories per workspace target (the lib and every integration-test binary), all stale |
| `target/debug/deps` | 8.7 GB | Two to three hash copies of every workspace test binary and lib, 60 to 230 MB each |
| `target/debug/build` | 351 MB | Build script output |
| `registry` | 61 MB | Index and crate archives |

The restore took about 4 minutes of an 8.5-minute run. The restored workspace artifacts were no
help: a PR restores the entry from the last `main` commit that changed the lockfile, so cargo
rebuilds the workspace crates anyway, and the test compile takes about 2 minutes 20 seconds
either way. Separately, `script/lint` type-checks `fuzz/`, its own workspace, and its `target`
wasn't cached, which cost about 25 seconds cold on every run.

### Decision
- Set `CARGO_INCREMENTAL=0` for the job. CI never reuses incremental state, and it was most of
  the entry.
- After `./script/cibuild` and before the save, run `cargo clean -p` for every workspace member,
  listed from `cargo metadata --no-deps`. Do the same for `fuzz/`'s members and the two
  workspace crates it builds, `logit-core` and `logit-proto`. Third-party artifacts stay, and the
  monthly reset bounds their growth. The step prints `du -sh` of both `target` directories so
  the entry size shows in the log. `cargo clean -p` removes a package's integration-test
  binaries and incremental directories along with its lib.
- Cache `fuzz/target` in the restore and the save.
- Bump the key version to `cargo-v4`, so the next `main` push starts a clean entry instead of
  restoring the 26 GB one.

### Alternatives considered
- **An ISO-week key** (`date -u +%G-W%V`) in place of the month. The entry grew from 2.6 to
  8.3 GB in the seven days from 2026-10-14 to 2026-10-21, so a weekly reset still ends each week
  near 8 GB and adds a cold `main` build every week. Rejected: it shortens the growth and
  doesn't remove its cause.

### Consequences
- The entry holds third-party artifacts only, so the restore should take about a minute and the
  upload should shrink with it.
- A PR compiles the workspace crates on every run. It did before, and the change makes that
  explicit.
- Lint's `fuzz/` check no longer compiles third-party crates cold, saving about 25 seconds.
- The first `main` push after the version bump builds cold once, and PRs run cold until it saves.
