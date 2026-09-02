# 0020 — A separate demo stack, not an extension of the dev stack

## Status
Accepted

## Context
`docs/plans/0003-demo-stack.md` builds a "clone and `docker compose up`" path for someone with no
Rust toolchain and no familiarity with this repo to see `logit` work. The existing root
`compose.yaml` already runs InfluxDB and Grafana, seeded and provisioned — extending it to also
run Loki, Tempo, a synthetic traffic source, and `logit` itself was the obvious-looking option.

That stack's actual job, though, is the contributor dev loop (ADR 0005, ADR 0006): a bind-mounted
source tree, a long-lived `dev` service `script/server` execs `cargo run` into, InfluxDB and
Grafana as the two backends v0.1's slice targets. Its `nginx` service and the example that drives
it are load-bearing for `crates/logit-bench`'s fixtures (`docs/design/memory.md`'s "Fixtures"
section — a benchmark input must never depend on a running service, but its *provenance* is allowed
to, and does). Growing that same file to also be "the thing a stranger runs" means every change to
either purpose risks the other: a demo-only healthcheck timeout tuned for a fresh clone now also
throttles every contributor's `script/server`, and a dev-stack convenience (the `dev` service's
`sleep infinity`, its network alias trick for `nginx`'s `syslog:` writer) has no meaning to someone
who only wants to see Grafana light up.

## Decision
`demo/` is a new, self-contained directory with its own `compose.yaml`. It runs the release image
(`Dockerfile`, built with `context: ..`), not the dev container — a demo proves what a consumer
actually gets, not what compiles inside this repo's own build environment. It has its own InfluxDB
and Grafana service definitions (deliberately duplicated, not shared via YAML anchors or a second
compose file layered on the first — see Alternatives), plus Loki, Tempo, and Alloy, none of which
the dev stack needs, and a hello-world app (`hello`) that doubles as the demo's landing page and
its traffic source, fed by a small `logit graph`-to-SVG render chain (`graph-dot`/`graph-svg`, two
one-shot containers) that has no dev-stack analogue at all. Root `compose.yaml`, `script/server`,
and every `script/*` entrypoint that targets the dev stack are unchanged.

`script/demo` is a thin wrapper (`${DOCKER} compose -f demo/compose.yaml "$@"`) for
`$DOCKER`/podman/sudo parity with the rest of `script/*` (ADR 0006) — but `demo/README.md`'s
headline instruction is plain `docker compose up` in `demo/`, since the whole point is that running
it requires nothing this repo provides beyond the compose file itself.

## Alternatives considered
- **Extend root `compose.yaml` with profiles** (`demo`/`dev` on each service). Rejected: it makes
  every service definition carry two audiences' worth of caveats in one block, and a `docker
  compose --profile demo up` invocation is a worse first instruction than a plain `docker compose
  up` in a directory that only contains the demo.
- **`demo/compose.yaml` extending root `compose.yaml` via `include:` or multiple `-f` files.**
  Rejected for the same reason as sharing service blocks: the InfluxDB/Grafana seeding the two
  stacks want is close but not identical (a distinct token, distinct volumes, so a demo run can
  never collide with a contributor's dev-stack data), and `include:` would make that divergence a
  layered override to trace rather than a single file to read.
- **Make the demo the default and move the dev container to `compose.dev.yaml`.** Rejected as
  out of scope for this plan specifically — it touches every `script/*` entrypoint's assumption
  about what `compose.yaml` means, which is a larger, separately-reviewable change if wanted later.

## Consequences
- Two compose files now describe two overlapping backends (InfluxDB, Grafana) with intentionally
  different seeding values. A change to one's provisioning shape (a new InfluxDB bucket, a Grafana
  datasource field) has to be considered for the other by a human, not caught by sharing a
  definition — accepted, since the two stacks' seeding is deliberately allowed to diverge (see
  Decision).
- The demo always builds `logit` from source on first run (no published image exists yet,
  `docs/deploying.md`) — several minutes before anything appears. Publishing an image to a
  registry would fix this and is a natural follow-up, out of scope here.
- `examples/nginx/` remains real and runnable for the dev stack's sake even though it is no longer
  anyone's onboarding path — see `docs/plans/0003-demo-stack.md` workstream E for why deleting it
  isn't free.
- Reusing the release image for a second purpose (`logit graph`, in `graph-dot`) works only
  because the `logit` service is given an explicit `image: logit-demo:latest` tag rather than
  Compose's derived `<project>-logit` default — a second `build:` block for the identical image
  was the alternative, rejected as pure duplication for no benefit.
