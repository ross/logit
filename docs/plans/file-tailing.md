---
created: 2026-09-06
updated: 2026-09-06
---

# Enabling plan: file tailing and Docker json-file container logs

## Context

[ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md) is the
design decision; this is the workstream plan that lands it and carries it through to the demo. See
that ADR for the full reasoning — this file tracks what's built and what's left, PR by PR.

## Workstreams

### A. Config, graph, registry, driver (poll only), `tail_in` — **landed**

- `logit_config::ComponentKind::TailIn { paths, tail: TailOptions }` (renamed from the
  unimplemented `FileTail`) and `ComponentKind::DockerIn { root, containers, discover, labels,
  tail: TailOptions }` (declared, not yet implemented — see workstream C). New shared
  `TailOptions`, `ReadFrom`, `WatchMode`.
- `logit_pipeline::graph`: `is_tail_listener`; rule 17 extended (a tail listener's `receive:` may
  only set the four batch-assembly fields, a queue-bounding field is rejected by name); rule 18
  extended (a tail listener's `batch_max_events`/`batch_max_bytes` of `0` rejected); new rule 26
  (`tail_in`'s `paths` non-empty, no empty entry, `*` only in the final path component); rule 27
  written but its body is a no-op until `docker_in` is implemented (rule 8 rejects it first
  regardless); new rule 28 (`tail_in`'s `poll_interval`/`checkpoint_interval`/`max_line_bytes`
  nonzero).
- `logit_pipeline::accumulator`: new `FlushReason::Closed`.
- New `crates/logit-inputs/src/tail/` module: `driver::Tailer<D, F>` (the shared read/rotate/
  truncate/checkpoint/shutdown loop), `line::{LineSplitter, TailDecoder, LineDecoder}`,
  `pattern::PathPattern`, `checkpoint::{FileId, CheckpointStore}`, `watch::Watcher` (`Poll` only —
  every `WatchMode` resolves to it for now; see workstream B), and the `tail_in`-facing `mod.rs`
  (`TailConfig`, `TailBatching`, `TailInput`).
- `crates/logit-cli/src/pipeline.rs`: `TailIn` arm in `build_spec`; `tail_config` converts
  `TailOptions` + `ReceiveConfig` into `logit_inputs::tail::TailConfig`, resolving
  `checkpoint_path` against the config file's own directory when relative.
- Fixed along the way: shutdown previously flushed every tracked file's *accumulated* batch
  (`FlushReason::Shutdown`) but never gave an active file's decoder a chance to close — an
  unterminated last line (already counted in the checkpoint offset, since `read_one` advances
  `offset` per chunk read, not per decoded line) would sit in `LineSplitter`'s partial buffer and
  be silently lost on shutdown, with no future restart ever re-reading those bytes. Fixed by a
  shared `close_decoder` helper (`driver.rs`) called for every still-tracked file on shutdown, not
  only draining ones — caught by `an_unterminated_last_line_is_held_until_its_newline_arrives_and_
  emitted_on_close`'s own shutdown assertion during this workstream's own test-writing, not filed
  as a separate gap.
- Tests: `pattern`/`line`/`checkpoint` unit tests; `driver` tests covering emit shape, `read_from`,
  new-file discovery, rotation, truncation, removal, checkpoint interval/resume/overshoot/pruning,
  shutdown flush, the unterminated-last-line fix above, downstream backpressure, invalid UTF-8, and
  round-robin fairness between a busy and a quick file; `graph` tests per rule (accept + reject);
  `pipeline::build_spec` tests for the `TailIn` arm and `tail_config`'s conversions (checkpoint
  path resolution, batching wiring, `read_from`/`watch` conversion).
- Docs: this ADR, this plan, and the stale `file_tail`/`FileTail` references in
  `docs/design/pipeline-graph.md` (including its own numbered rule list, extended through 28),
  `docs/adr/component-graph-configuration.md`, `docs/adr/syslog-output.md`.
- `script/cibuild` green (format, clippy `-D warnings`, nextest, `script/validate`, schema drift,
  audit).

### B. `inotify` wake source — next

- `crates/logit-inputs/src/tail/watch.rs` grows a `#[cfg(target_os = "linux")]` `inotify` backend
  (hand-rolled via `libc` + `tokio::io::unix::AsyncFd` — no `notify` crate, license-blocked by
  `deny.toml`) implementing the `Wake::Changed`/`Wake::Overflow` variants `driver.rs` already
  matches on today. `Auto` tries it, falls back to `Poll` (diagnosed `watch_error`) on setup
  failure; `Inotify` fails startup instead of falling back; non-Linux `Inotify` is a startup error.
- `Tailer` watches every pattern's parent directory (`docker_in`: the root plus each container
  directory as it appears); a `Wake::Changed` for a matching name triggers an immediate `scan`
  rather than waiting for the next `poll_interval` tick, which stays on as reconciliation.
- Cargo: `libc.workspace = true` under `[target.'cfg(target_os = "linux")'.dependencies]`; confirm
  `script/audit` stays clean.
- Tests: `inotify` event parsing (`IN_CREATE`/`IN_MODIFY`/`IN_MOVED_TO`/`IN_MOVED_FROM`/
  `IN_DELETE`/`IN_DELETE_SELF`/`IN_CLOSE_WRITE`, `IN_Q_OVERFLOW`), a real wake on a child file
  write, latency comparison against `Poll` at a large `poll_interval`, `Auto`'s fallback when
  `inotify_init1` fails, and an overflow forcing a full rescan.

### C. `docker_in` — after B

- New `crates/logit-inputs/src/docker.rs`: `DockerInput`, `ContainerFilter` (explicit
  name/id-prefix matching, or `discover: true`), `ContainerMeta` (reads `config.v2.json`: `Name`,
  `Config.Image`, `Config.Labels`), `DockerDecoder` (Docker's json-file envelope, partial-line
  reassembly up to `max_line_bytes`, `time` as the event timestamp with a `bad_time` fallback to
  read time).
- `logit_pipeline::graph`: add `DockerIn` to `is_implemented`; rule 27's body actually runs now
  (`containers` non-empty or `discover: true`, no empty/duplicate entries, non-empty `root`).
- `crates/logit-cli/src/pipeline.rs`: `DockerIn` arm in `build_spec`.
- Tests: envelope decoding (time, stream, `attrs`), partial-line reassembly and its own
  `max_line_bytes` bound, a dangling partial emitted on close rather than lost, `ContainerMeta`
  parsing (leading-slash strip, image tag split rules), resource attribute shape (`container.*`,
  opt-in labels), filter matching (name, id-prefix, `discover`), a recreated container followed by
  name under a new id, rotated `-json.log.1` never opened, missing `config.v2.json` degraded to
  `container.id`-only rather than fatal, and confirmation that `docker_in` never parses the inner
  application line.

### D. Demo rework + remaining docs — after C

- `demo/compose.yaml`: nginx gains `container_name: logit-demo-nginx` and
  `NGINX_ENTRYPOINT_QUIET_LOGS: "1"`; drops its `depends_on: logit` (checkpoint + `read_from:
  beginning` means nothing is lost if nginx starts first). `logit` service: `user: "0:0"` (root,
  read-only bind mount — see the ADR's "Root privileges" section), `/var/lib/docker/containers:
  ro`, **no `:z`** (would relabel the daemon's own state — `security_opt: ["label=disable"]`
  instead), a new `logit_state` volume for the checkpoint.
- `demo/nginx/nginx.conf`: drop the `syslog:` `access_log` destination — stdout only.
- `demo/logit.yaml`: `nginx_in` becomes `docker_in` (`containers: [logit-demo-nginx]`,
  `read_from: beginning`, a `checkpoint_path`); a new inline `lua` stage (`nginx_stdout`) between
  `nginx_in` and `nginx_identity` dropping any event whose `log.iostream` isn't `stdout` (nginx's
  `error_log` on stderr would otherwise count as a request) — the ADR's "Alternatives considered"
  covers why this is a Lua stage today, not a `streams:` field or named output ports.
- `demo/README.md`, `demo/architecture.dot`: the nginx leg now reads "docker logs," a paragraph on
  running as root / native-Linux-Docker-only / what `docker compose down -v` wipes.
- Docs: `README.md`, `AGENTS.md`, `docs/OVERVIEW.md` status lines; `docs/design/data-model.md`
  (`log.file.path`, `log.iostream`, `container.*`); `docs/design/internal-telemetry.md` (new
  metrics/diagnostics catalog entries, `receive.flushed` gains `closed`); `docs/deploying.md` (new
  "tailing files and Docker logs" section); `docs/known-gaps.md` (every gap named in the ADR's
  Alternatives/Consequences sections, plus narrowing the existing channel-depth entry to TCP).

## Verification

`script/cibuild` after every workstream. Workstream A: confirmed green (format, clippy
`-D warnings`, 1053 nextest tests including the new `tail`/`graph`/`pipeline` coverage,
`script/validate` on `demo/logit.yaml` and every `examples/*.yaml`, schema regenerated and
committed, `script/audit` clean). B/C/D verification (inotify latency, a live `docker_in` smoke
test, and the demo's end-to-end proof) follows the same `script/cibuild` bar per PR, plus the
manual demo checks the ADR's own Consequences section implies: `docker compose logs logit` shows
`files.open` ≥ 1 with no `open_error`/`metadata_error`; `web.requests` matches the traffic
generator's actual request count (stderr excluded by the Lua stage); `docker compose restart
nginx` is followed under the new container id without restarting `logit`; `docker compose restart
logit` resumes from the checkpoint with no duplicate lines visible in Loki.
