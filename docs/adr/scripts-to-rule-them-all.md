---
created: 2026-08-28
updated: 2026-08-28
---

# Developer workflow: Scripts to Rule Them All, and PR-based development

## Status
Accepted

## Context
[ADR `containerized-development`](containerized-development.md) got the dev environment itself containerized, but
the interface to it was still a hand-maintained `Makefile` whose targets duplicated the exact
command sequence `.github/workflows/ci.yml` ran — two places to keep in sync, and "does this pass
CI?" had no single local answer. The project also needs a settled convention for how changes land,
now that it has more than one contributor (human and AI).

## Decision
Adopt the [Scripts to Rule Them All](https://github.blog/engineering/scripts-to-rule-them-all/)
pattern: every common task is a `script/<name>` executable with a consistent name across projects
(`bootstrap`, `setup`, `update`, `server`, `test`, `cibuild`, plus project-specific ones —
`lint`, `format`, `schema`, `audit`, `console`). `script/common.sh` holds the shared logic: it
resolves the `DOCKER` override from [ADR `containerized-development`](containerized-development.md), and a `run()`
helper that runs a command in the dev container locally, or directly when `$CI` is set (GitHub
Actions' job `container:` directive already provides the equivalent image, so wrapping in another
`docker compose run` there would mean unsupported docker-in-docker).

`.github/workflows/ci.yml` calls `script/cibuild` directly rather than repeating its steps —
one definition of "the CI sequence," runnable identically by a human, by CI, or by an agent.
`Makefile` becomes a thin wrapper: `make test` just execs `script/test`, kept only because some
tools and habits expect `make` to work.

Alongside this: **development happens on branches, landed via pull request.** No more direct
pushes to `main`.

## Alternatives considered
- **Keep the Makefile as the primary interface**, hand-syncing it with `ci.yml`. Rejected: this is
  the exact duplication that prompted the change, and `make` is a weaker fit than a plain
  executable for the "just run this" case script/bootstrap and friends are for — no PHONY
  bookkeeping, no make-flavored quoting surprises, directly invocable and directly readable as a
  shell script.
- **Direct pushes to `main`, reviewed after the fact.** Workable for one contributor early on, but
  doesn't scale as soon as more than one person (or agent) touches the repo concurrently, and
  loses the natural point to run `script/cibuild` and get a second look before something lands.

## Consequences
- Adding a new check or setup step means adding/editing one `script/*` file; CI picks it up for
  free by virtue of calling `script/cibuild`.
- `script/*` assumes execution from a checkout with `script/common.sh` present — they're not
  meant to be curl'd and run standalone.
- Every change now needs a branch and a PR, including small ones — a minor cost for the review
  point and CI gate it buys.
