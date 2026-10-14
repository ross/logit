---
created: 2026-10-14
updated: 2026-10-14
---

# RustSec advisories run off the PR gate, with a daily fix-up PR

## Status
Accepted

## Context
`script/cibuild` ran `script/audit`, which ran `cargo deny check` and `cargo audit`. Both fetch
the RustSec advisory database when they run, so their verdict depends on the day as well as the
checkout. A new advisory against any locked crate failed CI on every open PR, none of which had
changed a dependency. Each time, someone wrote a one-line `Cargo.lock` PR by hand
(`fix(deps): bump ruzstd to 0.9.1 for RUSTSEC-2026-0344`, and a `quinn-proto` bump the day
before) and every other PR waited on it.

`cargo deny check` runs four checks. `bans`, `licenses`, and `sources` read only the checkout and
`deny.toml`. `advisories` also reads the database. `cargo audit` reads the same database as
`advisories`, with its own ignore list that isn't `deny.toml`'s.

## Decision
Checks that depend only on the checkout gate every PR. The advisory check gates only the changes
that can cause or fix a finding, and otherwise runs where a finding is news.

- **`script/audit` runs `cargo deny check "$@"`.** With no arguments it runs all four checks.
  `cargo audit` is gone from it.
- **`script/cibuild`, and so `ci.yml`, runs `script/audit bans licenses sources`.**
- **`.github/workflows/advisories.yml` runs `script/audit advisories`** in the CI image
  ([ADR `ci-image-on-ghcr`](ci-image-on-ghcr.md)):
  - On a PR, the job always starts, so it can be a required check. It runs the check only when the
    PR changes `Cargo.lock` or `deny.toml`, and passes otherwise. A workflow-level `paths:` filter
    would leave a required check pending forever on every other PR.
  - On a push to `main`, daily at 09:00 UTC, and on manual dispatch, it always runs the check.
- **A failed daily or manual run on `main` starts a `fixup` job.** It runs
  `.github/scripts/advisory_fixup.py`, which runs `cargo update -p NAME@VERSION` for each crate
  flagged as vulnerable, unsound, or yanked, and reruns the check. `Cargo.toml` never changes, so
  only a semver-compatible release can clear a finding.
  - When the bump clears an advisory, the job opens a PR from `fix/rustsec-advisories`, or updates
    the open one, with the hand-written PRs' title shape and a body listing each advisory, crate,
    and version change.
  - When a finding remains (an unmaintained crate, no patched release, or a fix past a major
    version), the job opens an issue labeled `rustsec`, or comments on the open one when its list
    changed. The fix is a hand edit or an `ignore` entry in `deny.toml`.
- **The PR uses `ADVISORY_PR_TOKEN`, a fine-grained PAT** with contents and pull-requests write,
  because GitHub doesn't run workflows on a PR that the job's `GITHUB_TOKEN` opens, and the fix-up
  PR needs CI. The secret lives in an `advisories` environment restricted to `main`, and only the
  step that pushes and opens the PR receives it. The issue path uses `GITHUB_TOKEN` with
  `issues: write`, and covers the bumpable findings too when the secret is unset.
- **An unmerged PR can't reach the token.** The workflow has no `pull_request_target` or
  `workflow_run` trigger, and the `fixup` job runs only on `schedule` and `workflow_dispatch`
  against `main`, so a `pull_request` run, a fork's included, never enters the environment. The job
  compiles nothing: `cargo update` and `cargo deny` don't run build scripts, so a dependency's
  code doesn't run beside the token. Third-party actions are pinned to a commit SHA.

## Alternatives considered
- **Dependabot security updates alone.** Dependabot reads GitHub's Advisory Database, which
  imports RustSec advisories after a review, often hours to days later, while `cargo deny` fails
  as soon as RustSec publishes. It also opens nothing for an unmaintained or yanked crate, or for
  an advisory with no patched release. It stays an optional backstop, turned on in
  Settings → Code security → Dependabot security updates.
- **A grace period on new advisories.** Neither `cargo-deny` nor `cargo-audit` has a setting that
  ignores advisories younger than some age.
- **Pin the advisory database.** Check out a fixed `advisory-db` commit, point `deny.toml`'s
  `db-path` at it, and run `cargo deny --disable-fetch`. The check becomes reproducible from the
  checkout and could gate every PR again, at the cost of a pinned commit to bump and the plumbing
  to fetch it into CI and the dev container. Still possible later, on top of this workflow.
- **Keep `cargo audit` beside `cargo deny`.** It reads the same database as the `advisories`
  check, so it adds no finding, and its ignore list can drift from `deny.toml`'s.

## Consequences
- A PR that doesn't touch `Cargo.lock` or `deny.toml` can merge while `main` has an open advisory.
  The daily run, the run on each push to `main`, and the fix-up PR or issue are what surface it.
- `script/cibuild` no longer reproduces every check CI runs. `script/audit advisories` runs the
  rest locally, against the database as it is that day.
- The `advisories` environment and its `ADVISORY_PR_TOKEN` secret are set up by hand in the
  repository settings. The PAT expires and needs rotating; until it's set, findings arrive as
  issues only.
- `Dockerfile.dev` still installs `cargo-audit`, which nothing runs. Removing it changes the CI
  image tag, so it waits for the next image republish.
