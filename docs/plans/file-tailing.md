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

### B. `inotify` wake source — **landed**

- `crates/logit-inputs/src/tail/watch.rs` grows a `#[cfg(target_os = "linux")]` `inotify` backend
  (hand-rolled via `libc` + `tokio::io::unix::AsyncFd` — no `notify` crate, license-blocked by
  `deny.toml`) implementing the `Wake::Changed`/`Wake::Overflow` variants `driver.rs` already
  matched on since workstream A. `Auto` tries it, falls back to `Poll` (diagnosed `watch_error`) on
  setup failure; `Inotify` fails startup instead of falling back; non-Linux `Inotify` is a startup
  error (via an uninhabited `PlatformInotify` stand-in on non-Linux builds, so the same `Watcher`
  code compiles everywhere without a parallel non-Linux implementation).
- Watches whole directories (matching `PathPattern`'s own "scan a directory, match names" shape),
  not individual files: `IN_MODIFY | IN_CREATE | IN_MOVED_TO | IN_MOVED_FROM | IN_DELETE |
  IN_DELETE_SELF | IN_CLOSE_WRITE`. `Tailer` watches every pattern's parent directory (already
  wired in workstream A); any `Wake` — `Changed` or `Overflow` — triggers the same `scan(false)`
  the poll tick already used, so a burst of events or an `IN_Q_OVERFLOW` both just cause one full
  rescan rather than needing per-event bookkeeping.
- **Finding during implementation:** `poll_interval`/`inotify` only govern *discovering* a path
  (a new file, a rotation, a truncation) via `scan` — reading more bytes off an *already-tracked*
  file is not gated by either at all, since `drain`'s round-robin read runs after every loop
  iteration regardless of what woke it (an open file handle just sees new bytes on its next
  `read()`). The latency-comparison tests were designed around new-file discovery accordingly, not
  around appending to an already-open file — an earlier draft of
  `under_poll_a_write_is_delivered_only_after_the_poll_interval` assumed the latter and failed for
  exactly this reason before being corrected.
- Cargo: `libc.workspace = true` (workspace `Cargo.toml`, already pinned at `0.2.189` — matches
  the transitive version, confirmed via `script/audit`) under
  `crates/logit-inputs/Cargo.toml`'s `[target.'cfg(target_os = "linux")'.dependencies]`.
- Tests: `inotify` event parsing (`IN_CREATE`/`IN_MODIFY`, `IN_Q_OVERFLOW`, an unknown watch
  descriptor ignored, a nameless `IN_DELETE_SELF` reported as the directory itself), a real wake on
  a child file write (against a live `inotify` fd in the dev container), `Watcher`-level coverage
  of `Poll`/`Auto`-success/`Auto`-fallback/`Inotify`-hard-failure (via an injectable-constructor
  test seam, not a real exhausted OS limit), and driver-level latency comparisons: a new file
  discovered well within seconds under `inotify` against a 30s `poll_interval`, versus only after a
  300ms `poll_interval` tick under `poll`.

### C. `docker_in` — **landed**

- New `crates/logit-inputs/src/docker.rs`: `DockerInput`, `ContainerFilter` (explicit
  name/id-prefix matching, or `discover: true`), `ContainerMeta` (reads `config.v2.json`: `Name`,
  `Config.Image`, `Config.Labels`), `DockerDecoder` (Docker's json-file envelope, partial-line
  reassembly up to `max_line_bytes`, `time` as the event timestamp with a `bad_time` fallback to
  read time).
- `crate::tail::PathPattern` grows `docker_containers(root)`: not a wildcard glob (this driver's
  matcher is still exactly the minimal subset from workstream A) but a dedicated two-level walk
  over Docker's own deterministic naming (`<root>/<id>/<id>-json.log`, the id appearing both as
  the directory name and the log file's own prefix) -- `Tailer`/`DecoderFactory` (`driver.rs`) are
  re-exported `pub(crate)` from `tail/mod.rs` so `crate::docker` can build on them from outside
  the `tail` module tree.
- `logit_pipeline::graph`: `DockerIn` added to `is_implemented`; rule 27's body now actually runs
  (`containers` non-empty or `discover: true`, no empty/duplicate entries, non-empty `root`); the
  stale `docker_in_is_rejected_as_not_yet_implemented` test replaced with real accept/reject
  coverage per rule.
- `crates/logit-cli/src/pipeline.rs`: `DockerIn` arm in `build_spec`, reusing `tail_config` as-is.
- Tests: envelope decoding (time, stream, `attrs`, an unknown stream or malformed JSON rejected as
  a bad line), partial-line reassembly and its own `max_line_bytes` bound (including the drop
  actually resuming cleanly afterward), a dangling partial emitted on close rather than lost,
  confirmation the inner application line is never parsed, `ContainerMeta` parsing (leading-slash
  strip, image tag split rules including a registry-port false positive and a digest reference),
  resource attribute shape (`container.*`, opt-in labels, a stable `Arc` across one container's
  lines), filter matching (name, id-prefix, `discover`, a too-short/non-hex entry never matching
  by prefix), and full `DockerInput` driver tests: explicit vs. `discover` selection, a recreated
  container followed by name under a new id, rotated `-json.log.1` never opened, and a missing
  `config.v2.json` degraded to `container.id`-only rather than refusing to tail. **Test-fixture
  pitfall found while writing these**: a raw json-file log line needs an actual trailing newline
  *byte* terminating the file-level line, on top of (and separate from) the `\n` *inside* the
  JSON string's own `log` field -- a fixture with only the latter sits forever in the outer
  `LineSplitter`'s partial buffer, never reaching `DockerDecoder::decode_line` at all; several of
  these tests hit exactly that before being corrected.

### D. Demo rework + remaining docs — **landed**

- `demo/compose.yaml`: nginx gains `container_name: logit-demo-nginx` and
  `NGINX_ENTRYPOINT_QUIET_LOGS: "1"`; drops its `depends_on: logit` (checkpoint + `read_from:
  beginning` means nothing is lost if nginx starts first). `logit` service: `user: "0:0"` (root,
  read-only bind mount — see the ADR's "Root privileges" section), `/var/lib/docker/containers:ro`,
  **no `:z`** (would relabel the daemon's own state — `security_opt: ["label=disable"]` instead), a
  new `logit_state` volume for the checkpoint.
- `demo/nginx/nginx.conf`: dropped the `syslog:` `access_log` destination — stdout only now
  (`access_log /dev/stdout access_json;`, renamed from `access_json_syslog` since it no longer is
  one); header comment explains the switch and why `NGINX_ENTRYPOINT_QUIET_LOGS` matters now.
- `demo/logit.yaml`: `nginx_in` is now `docker_in` (`containers: [logit-demo-nginx]`,
  `read_from: beginning`, `checkpoint_path: /var/lib/logit/nginx.checkpoint`); a new inline `lua`
  stage (`nginx_stdout`) between `nginx_in` and `nginx_identity` dropping any event whose
  `log.iostream` isn't `stdout` (nginx's `error_log` on stderr would otherwise count as a request)
  — the ADR's "Alternatives considered" covers why this is a Lua stage today, not a `streams:`
  field or named output ports. Topology comment and header updated.
- `demo/README.md`, `demo/architecture.dot`: the nginx leg now reads "docker logs" (still one
  "logging" edge in the architecture diagram — the transport distinction is a detail that diagram
  doesn't otherwise draw); a new paragraph on running as root / native-Linux-Docker-only / what
  `docker compose down -v` wipes; `container.*` resource attributes noted alongside the existing
  `service.name`/`service.namespace` mention; `tail_in` added to "what isn't exercised yet"
  (`docker_in` is the one that gets a live workout, not plain file tailing).
- Docs: `README.md`, `AGENTS.md`, `docs/OVERVIEW.md` status lines; `docs/design/data-model.md`
  (`log.file.path`, `log.iostream`, a `container.*` resource sub-table); `docs/design/
  internal-telemetry.md` (a `tail_in`/`docker_in` Layer-3 worked example, `receive.flushed` gains
  `closed`, `docker_in`'s `container.*` named as a third resource-identity category alongside "no
  claim" and "genuine self-claim"); `docs/deploying.md` (new "Tailing files and Docker logs"
  section: root/bind-mount, `read_from`/checkpoint, `watch` modes, what to watch); `docs/
  known-gaps.md` and `docs/design/memory.md` (every gap named in the ADR's Alternatives/
  Consequences sections; narrowed the existing channel-depth entry to TCP now that `tail_in`/
  `docker_in` turned out to already be batch-bounded the same way a UDP listener is, not the
  unbounded case that entry used to speculate about).

**Live end-to-end verification** (`script/demo up --build`, a real run against native Linux Docker
Engine): `nginx_in` discovered and tailed `logit-demo-nginx`'s json-file log immediately, every
event correctly carrying `container.id`/`container.name`/`container.image.name`/
`container.image.tag`/`log.iostream="stdout"`; zero `stderr` lines leaked past `nginx_stdout`
(`log.iostream="stderr"` never appeared downstream) and zero diagnostics fired
(`open_error`/`metadata_error`/`bad_line` all absent from `docker compose logs logit`); InfluxDB's
`web.requests` summed to **exactly** the same count for `nginx` and `haproxy` (38 each) over the
same window — proof the stderr filter neither inflated nor lost real access lines; the landing
page's live-rendered pipeline graph shows `nginx_in`/`nginx_stdout` in the actual running topology;
a full `docker restart` of the `logit` container resumed from the checkpoint with **zero**
duplicate `trace.id`s across the restart boundary, confirming the checkpoint write/resume path end
to end, not just in unit tests.

## Verification

All four workstreams landed; `script/cibuild` green on every PR (format, clippy `-D warnings`,
nextest, `script/validate` on `demo/logit.yaml` and every `examples/*.yaml`, schema regenerated and
committed, `script/audit` clean) — workstream A alone landed 1053 nextest tests including the new
`tail`/`graph`/`pipeline` coverage, B added `inotify` unit and latency-comparison tests, C added
the full `docker.rs` suite. Workstream D's own bar was higher than a config-only PR would need,
since it's this whole plan's actual proof: a live `script/demo up --build` run (see workstream D's
own entry above for the full account) confirmed every claim the ADR's Consequences section made in
the abstract — discovery, container identity, the stderr filter neither losing nor double-counting
real lines, and the checkpoint surviving a real container restart with no duplicates — against a
running stack, not just unit tests asserting the same properties in isolation.
