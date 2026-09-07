---
created: 2026-09-06
updated: 2026-09-07
---

# `tail_in`: generic file tailing, and `docker_in` on top of it for Docker's json-file logs

## Status
Accepted

## Context

`logit` ingests only over the network (`statsd_in`, `syslog_in`, `otlp_in`) or from itself
(`internal`) today. The mainstream way to collect container logs on a Docker host is a per-host
collector tailing the daemon's json-file logs at
`/var/lib/docker/containers/<id>/<id>-json.log` and enriching each line with container metadata
read locally — not a docker socket/API subscription, which is heavier, needs an API version
negotiated against whatever daemon is running, and normally requires broader privilege than
reading a directory the daemon already owns. `docs/OVERVIEW.md` already promises "file tailing for
logs (rotation- and checkpoint-aware)," and a `FileTail` config variant existed but was
unimplemented and misnamed against the `_in`/`_out` kind-naming rule
([ADR `component-graph-configuration`](component-graph-configuration.md)).

This record covers two listener kinds landing together, since the second is a thin decoder swapped
onto the first's driver, not an independent design:

- **`tail_in`** — a generic file tailer: one line becomes one raw log event, rotation/truncation
  aware, optionally checkpointed.
- **`docker_in`** — built on the same driver; decodes Docker's json-file envelope, reassembles
  Docker's own 16 KiB partial-line splits, and stamps per-container resource attributes read from
  the sibling `config.v2.json`.

And reworks `demo/` so nginx logs to stdout only (the container-native default) and is consumed via
`docker_in` instead of `syslog_in`, as the end-to-end proof.

## Decision

### One driver, two decoders — the `Decoder`/`UdpListener<D>` shape, adapted for a durable source

`crate::udp::UdpListener<D: Decoder>` is the existing precedent for "one generic read/decode/batch
driver, parameterized by what turns raw bytes into events." `tail/driver.rs::Tailer<D: TailDecoder,
F: DecoderFactory<D>>` follows the same shape: `TailInput` (`tail_in`) wraps a `Tailer<LineDecoder,
_>`, and `docker_in`'s `DockerInput` wraps a `Tailer<DockerDecoder, _>` with a decoder factory that
also applies the container filter and reads `config.v2.json`. Everything about discovery, rotation,
truncation, checkpointing, and the read/batch/emit loop is shared; only "what does one line become"
and "which paths does this listener want" differ per kind.

### No receive queue — the file itself is the durable buffer

Every other listener in `logit-inputs` decouples its socket read from its decode/send half through
a `ReceiveQueue` ([ADR `decoupled-listener-io`](decoupled-listener-io.md)), because a kernel socket
buffer can't be asked to wait — once it's full, the kernel starts discarding datagrams the process
never even sees. A tailed file has no such constraint: it's already a durable, arbitrarily large
buffer sitting on disk. There is nothing to protect against overflowing by dropping. `Tailer`'s
read loop simply stops advancing a file's offset when `Fanout::send` is slow (an `Output`, a full
inbox downstream), and resumes exactly where it left off once there's room — no drop policy, no
queue depth, no `receive.max_bytes`/`max_datagrams` to configure. Graph rule 17 enforces this in
config: a tail listener's `receive:` block may only set `batch_max_events`, `batch_max_bytes`,
`batch_flush_interval`, and `shutdown_grace` — the four fields that govern decoded-events-to-batch
assembly, which a tailed file still has exactly like a datagram listener does. A queue-bounding
field is rejected by name, not silently ignored.

### Wake source: poll always, `inotify` as a lower-latency addition

`watch: auto | inotify | poll` (config), backing `tail/watch.rs::Watcher`. `poll_interval`
(default 1s) is never disabled — it's both the sole cadence under `watch: poll` and, under
`inotify`/`auto`, a reconciliation pass that catches whatever a wake event might have missed (a
`IN_Q_OVERFLOW`, a network/FUSE mount where `inotify` doesn't reliably fire). `auto` tries
`inotify`, falls back to `poll` on any setup failure (e.g. an exhausted
`fs.inotify.max_user_instances`) with a `watch_error` diagnostic; `inotify` is a hard startup error
on the same failure, or on a non-Linux build, for an operator who wants to know immediately if the
low-latency path stopped working rather than silently degrading. The hand-rolled `inotify` backend
(via `libc` + `tokio::io::unix::AsyncFd`, confined to `tail/watch.rs`'s own `inotify` submodule)
watches whole directories, not individual files — matching `PathPattern`'s own "scan a directory,
match names within it" shape — and only ever triggers the same full `scan` the poll tick already
performs, for either a specific change or an `IN_Q_OVERFLOW`. **Note this only speeds up
*discovery*** (a new file, a rotation, a truncation) — reading more bytes off an already-tracked
file is never gated by `poll_interval`/`inotify` at all, since the driver's own read loop
(`Tailer::drain`) runs after every loop iteration regardless of what woke it, and an already-open
file handle simply sees new bytes on its next `read()`. The set of watched directories is itself
reconciled on every `scan`, not fixed once at startup (`PathPattern::watch_dirs`,
`Tailer::reconcile_watches`): `tail_in`'s patterns always reconcile to the same single directory
they always watched, but `docker_in`'s reaches `root` plus every container subdirectory that
currently exists, so a container's own subdirectory — where its log file actually appears, a
moment after the directory itself does — is watched as soon as it exists, and unwatched again once
it's gone.

### Rotation and truncation: identity by `(dev, ino)`

A file's identity across a `scan` is its `(st_dev, st_ino)` pair (`tail/checkpoint.rs::FileId`),
not its path — the only thing that survives both a rotation (the path keeps its name, the inode
doesn't) and a checkpoint resume (the inode is what's persisted). Each `scan`: a path whose inode
changed since the last scan is a rotation — the old handle drains to EOF, flushes, and closes; the
new one opens at its own beginning, regardless of `read_from`. A path whose length is now less than
the tracked offset is a truncation — seek to `0`, diagnosed (`truncated`), same inode. The line
splitter is reset along with the offset, so an unterminated fragment held from the pre-truncation
generation is dropped rather than spliced onto the first line of the new one — and so is each
decoder's own cross-line state (`TailDecoder::reset`), for the same reason: a no-op for `tail_in`'s
stateless `LineDecoder`, but real for `docker_in`'s `DockerDecoder`, whose own reassembly state
(`partial`, `dropping`) would otherwise either splice a stale fragment onto the new generation's
first entry, or silently swallow it clearing a stale `dropping` flag. A
previously-tracked path no longer matched by any pattern is a removal — drain and close. A rotated
`.1`-suffixed file is never matched in the first place, though for different reasons per kind:
`tail_in`'s wildcard is anchored (prefix/suffix), so `access.log.1` never satisfies a `*.log`
pattern; `docker_in`'s own two-position discovery (`PathPattern::docker_containers`) isn't a glob at
all and never looks for anything but the exact `<id>-json.log` name a container's own directory
implies, so `<id>-json.log.1` is simply never a name it looks for in the first place. A pattern
that matches a file both before and after a rename (`app.log*` matching both `app.log` and
`app.log.1`) rebinds the existing tracked entry to the new path rather than re-opening the inode,
so no duplicate re-emission occurs.

### Checkpoints: optional, written on an interval, only when dirty

`checkpoint_path` (default `None`, meaning no checkpoint at all — every restart re-applies
`read_from` to every file as if newly discovered). When set: a JSON document
(`{version, files: [{dev, ino, path, offset}]}`), written atomically (tmp file + rename) every
`checkpoint_interval` (default 5s) **only if something changed since the last write**, plus
unconditionally on a file's own close and on shutdown. Resume is by `(dev, ino)`, not path; an
offset past the file's current size (truncated between the checkpoint write and this restart)
restarts at `0` rather than seeking past EOF. A checkpoint write only persists the tailer's
*currently tracked* files, so a removed or rotated-away file's entry simply isn't reproduced on the
next write — pruning falls out of the write contract, with no separate GC pass.

**This is a deliberate at-least-once boundary, not per-line durability.** A checkpoint written on
an interval means a crash between two writes can replay up to `checkpoint_interval` worth of
already-emitted lines on restart — the same trade-off `buffer:`'s sink-side retry already makes on
the delivery half of this same pipeline, and the same reasoning: a checkpoint written on every line
would dominate the cost of tailing an active file for no correctness benefit past "bounds how much
a crash can replay," and replay is always safe (downstream is expected to tolerate a duplicate the
same way any at-least-once pipeline stage does). An operator wanting less window trades it directly
against write volume via `checkpoint_interval`. The interval checkpoint flushes every accumulator
before it writes and persists only the line-complete, already-emitted offset (excluding bytes still
held as an incomplete line), so the accepted failure mode is strictly duplicates on restart, never
loss of a line nothing downstream has seen.

### Start position: `read_from` only governs what was there before startup

`read_from: end` (default) or `beginning`. This only applies to a file present at the very first
`scan` with no checkpoint entry naming it — a file discovered afterward (created, or rotated in)
always starts at its own beginning, since it has no "before `logit` started" content to skip in the
first place. A checkpoint entry, when present, always wins over `read_from` for the file it names.

### Metadata: `config.v2.json` beside the log, not the docker socket/API

`docker_in` reads `Name` (leading `/` stripped), `Config.Image`, and `Config.Labels` from the
sibling `config.v2.json` the daemon already writes next to each container's log file — no socket
connection, no HTTP client, no API version to negotiate. This is a documented internal Docker
format, not a public API (see Alternatives/Consequences below), but it costs nothing beyond a
`read_dir`+`read`+`serde_json::from_slice` already paid for by walking the log directory itself,
and needs no additional privilege beyond the read access already required for the logs.

### Selection: explicit by default, opt-in `discover`

`docker_in`'s `containers: [name | id-prefix (≥12 hex)]` selects which containers to follow;
`discover: true` follows every container under `root`, including ones that appear after startup.
`containers` empty and `discover` unset is rejected (rule 27) — the same "would silently do
nothing" reasoning rule 7 already applies to a listener with no consumers. Explicit is the default
because a container log stream, unlike a metric or a datagram source, is a *named, deliberate*
selection in most operational setups — an operator names what they want observed. Even in explicit
mode, `docker_in` still watches the whole root directory (not just the named containers'
subdirectories): a recreated container gets a fresh id, and matching by name means noticing the
directory that now carries the old name.

### `docker_in` timestamps: the envelope's own `time`, not read time

`tail_in` stamps every event with read time — the same receipt-time rule `syslog_in` follows.
`docker_in` is a deliberate exception: it parses the envelope's own `time` field (the daemon's
same-host clock) and uses that as the event timestamp, falling back to read time (plus a `bad_time`
diagnostic) only on a parse failure. Replaying a backlog (`read_from: beginning`, or a fresh
container's already-written history) as "now" would misrepresent when those lines actually
happened; the daemon's local clock is a source `logit` can trust for a same-host log. This is the
one place a tailing decoder's timestamp policy differs between the two kinds — documented here so a
future kind added to this driver has to make the same choice deliberately, not by copying whichever
of the two happened to be closer.

### Event and resource shape

`tail_in` (`LineDecoder`): `message` is a zero-copy `Value::Str` slice of the read chunk (never
copied — the same zero-copy discipline `syslog_in` established), `severity: None`,
`body_format: Raw`, one attribute `log.file.path`. Resource is `Resource::default()` — a bare file
has no identity of its own to stamp; an operator names one via `set` downstream exactly as they
would for any other source, per
[ADR `operator-declared-resource-attributes`](operator-declared-resource-attributes.md).

`docker_in` (`DockerDecoder`): `message` is the envelope's `log` field minus its trailing `\n` — an
**owned** `String`, not a slice (`log` always contains escapes when it was written as valid JSON,
e.g. embedded newlines as `\n` two-character sequences, so `serde_json` cannot borrow a slice of
the original buffer for it; see Consequences). `severity: None`, `body_format: Raw`, an event
attribute `log.iostream` (`stdout`/`stderr`) plus every entry of the envelope's own `attrs` object
copied verbatim. Resource carries `container.id`, `container.name`, `container.image.name`,
`container.image.tag` (split on the image reference's last `:`, only when there's no `@` digest and
no `/` after that colon — a registry port like `registry:5000/app` must not be misread as a tag),
and `container.label.<key>` for every key named in `labels:` (default empty — a label's value is
operator data, not `logit`'s to expose unasked, and every key becomes a permanent entry in the
process-wide attribute interner; see [`docs/design/memory.md`](../design/memory.md) §4). The inner
application line (whatever the containerized process actually logged) stays the `json`
transform's job downstream, exactly as it already is for any other raw log line — `docker_in` never
looks inside `log` past reassembling Docker's own partial-line splits.

**`docker_in` has no per-stream (`stdout`/`stderr`) filter of its own.** An operator who wants only
one stream uses `log.iostream` from a downstream stage — the demo does this with an inline `lua`
component (see Workstream D, and "Alternatives considered" below for why this isn't a `streams:`
config field or a new named-output-ports mechanism).

### `receive:` reuse, not a parallel batching mechanism

A tail listener's `receive:` block reuses exactly `ReceiveConfig`'s batch-assembly fields
(`batch_max_events`, `batch_max_bytes`, `batch_flush_interval`, `shutdown_grace`) — the same
`BatchAccumulator` `crate::udp` already uses, one instance per tracked file. No new batching
concept, no tail-specific config vocabulary for something that already exists.

### Long lines are dropped whole, never truncated

A line exceeding `max_line_bytes` (default 1 MiB) is dropped in its entirety — not truncated to the
limit and emitted anyway. A truncated line would silently hand a downstream JSON parser
(`docker_in`'s own envelope, or a `json` transform an operator chains after `tail_in`) a value that
*looks* well-formed but isn't the real line — worse than an honest drop, which is at least visible
via the `long_line` diagnostic and the dropped-line counter.

### Root privileges and the bind mount

Docker's per-container state directories are `root:root 0710`, and the log files inside them are
`root:root 0640` on stock installs — reading them requires running as root (or a group grant this
ADR doesn't assume). `logit`'s runtime image runs as an unprivileged `logit` user by default; the
demo's `logit` service overrides that (`user: "0:0"`) specifically to read the host's
`/var/lib/docker/containers`, bind-mounted read-only. This is a real, unavoidable cost of reading
Docker's log files directly rather than through the socket API (which brokers access via group
membership on the socket instead) — stated plainly here since it's a meaningful operational
trade-off, not hidden in a compose file comment alone.

## Alternatives considered

- **Docker socket/API (`GET /containers/{id}/logs`) instead of the json-file driver.** Rejected for
  this pass: needs a client, API version negotiation against whatever daemon is running, and
  broader effective privilege (socket access, not just a read-only directory mount) for the same
  outcome. A documented follow-up (`docs/known-gaps.md`), not ruled out permanently — it would also
  enable metadata that isn't in `config.v2.json` and survives a `docker rename`, which this design
  doesn't (see Consequences).
- **A filesystem-watch crate (`notify`) instead of hand-rolled `inotify`.** `notify`'s dependency
  tree fails `deny.toml`'s license allowlist. `libc` (already present transitively at the exact
  pinned version) is enough to call `inotify_init1`/`inotify_add_watch`/`read` directly, confined
  to one small module.
- **A glob crate instead of a hand-rolled pattern.** `tail_in`'s own config-validated `paths` only
  ever need "a literal path" or "a `*` in exactly the final path component" — `**`, `?`, `[...]`,
  and escaping are all unused surface a real glob crate would carry for nothing. `docker_in` isn't
  in that shape at all, and a glob crate wouldn't have served it either: its discovery is a
  correlation between two path positions, not a pattern against one — the log file's name has to be
  derived from its own containing directory's name (`<root>/<id>/<id>-json.log`), which no
  positional glob expresses. A naive `<root>/*/*-json.log` would come closer but still be wrong: it
  would also accept a mismatched pair like `<root>/foo/bar-json.log`, which the real two-position
  walk (`PathPattern::docker_containers`) correctly rejects.
- **Per-line checkpoint writes.** Rejected as the busy-loop-adjacent cost this whole design exists
  to avoid — see "Checkpoints" above.
- **`tail_in` using sender/embedded time like `docker_in`.** A plain text line carries no
  timestamp of its own to trust; read time is the only honest choice, matching `syslog_in`'s own
  receipt-time precedent for a source with no reliable embedded clock.
- **Truncating an oversized line instead of dropping it.** Rejected — see "Long lines" above.
- **A per-input resource field (`resource: {...}` on `tail_in`/`docker_in` directly) instead of
  `set` downstream for `tail_in`, and instead of `container.*` for `docker_in`.** Consistent with
  the standing decision in
  [ADR `operator-declared-resource-attributes`](operator-declared-resource-attributes.md): resource
  identity an operator wants to assert is a `set` transform's job, not a per-kind config surface.
  `docker_in`'s `container.*` attributes are different in kind — they're *discovered facts* about
  what's actually running, the same way `syslog_in`'s parsed hostname is a discovered fact rather
  than an operator assertion, and don't replace an operator's own `service.name` choice via `set`.
- **A per-input stream filter (a `streams:` config field selecting `stdout`/`stderr`) or named
  output ports (`docker_in` publishing separate `nginx_stdout`/`nginx_stderr` sources another
  component subscribes to individually, with a future `splitter`-style component doing the general
  version of the same thing for logs/metrics/traces).** Explored and set aside for this pass: named
  output ports touch the component graph's core arity/wiring model (`docs/adr/
  component-graph-configuration.md`'s per-kind arity table, `Fanout`'s one-inbox-per-consumer
  shape) broadly enough to be its own design, not a `docker_in`-sized increment. The demo's actual
  need — "only stdout counts as a web request" — is fully solved today by an inline `lua` component
  reading `log.iostream`, so nothing is blocked waiting on the larger change. Worth revisiting as a
  named future idea (`docs/known-gaps.md`) if a second, unrelated need for the same shape shows up.
- **A `docker rename` handled by watching for it explicitly.** Out of scope — `container.name` is
  read once, when a container's log file is first opened, and never re-read for the life of that
  handle. A rename after that point is invisible until the container restarts (a new inode, a fresh
  `open`). Documented as a known gap, not silently promised.

## Consequences

- New `ComponentKind::TailIn`/`ComponentKind::DockerIn` (`crates/logit-config/src/lib.rs`),
  replacing the unimplemented `FileTail` (renamed, not additive — `file_tail` never shipped, so
  there's no compatibility surface to preserve). New shared `TailOptions`, `ReadFrom`, `WatchMode`.
- `crates/logit-pipeline/src/graph.rs`: `is_tail_listener`, rules 17/18 extended, new rules 26
  (`tail_in` path validation), 27 (`docker_in` selection validation — unreachable until `docker_in`
  itself is added to `is_implemented`), 28 (nonzero interval/size bounds).
- `crates/logit-pipeline/src/accumulator.rs`: new `FlushReason::Closed` — a per-file close is a
  distinct reason from `Shutdown` (the whole component stopping) or `Interval`/`Bound` (a batch
  reaching a limit while still running).
- New `crates/logit-inputs/src/tail/` module (`driver`, `line`, `pattern`, `checkpoint`, `watch`,
  and the `tail_in`-facing `mod.rs`) plus (workstream C) `crates/logit-inputs/src/docker.rs`.
- `crates/logit-inputs/Cargo.toml` gains direct `serde`/`serde_json` dependencies (both already
  present transitively at these exact versions — no new dependency-tree entry) for the checkpoint
  file and (in `docker_in`) `config.v2.json`; a Linux-only `libc` dependency lands with the
  `inotify` follow-up.
- `crates/logit-cli/src/pipeline.rs`: `tail_config` converts `logit_config::TailOptions` +
  `ReceiveConfig` into `logit_inputs::tail::TailConfig`, resolving `checkpoint_path` against the
  config file's own directory when relative (the same pattern `stdio_out`'s `path` target and
  `lua_file` already follow).
- `docs/OVERVIEW.md`, `README.md`, `AGENTS.md`, `docs/design/data-model.md` (new well-known
  attributes), `docs/design/internal-telemetry.md` (new metrics/diagnostics), `docs/deploying.md`
  (a new "tailing files and Docker logs" section covering the root-privilege/read-only-mount
  requirement), and `docs/known-gaps.md` (the new gaps named throughout this record: `docker
  rename`, json-file-driver-only, `(dev, ino)` checkpoint identity, `inotify` on network/FUSE
  mounts, non-Linux poll-only, `docker_in`'s single deviation from read-time timestamps, per-input
  stream filtering, `config.v2.json`'s undocumented-format risk, named output ports as a deferred
  future idea) all need updating — tracked PR by PR in
  [`docs/plans/file-tailing.md`](../plans/file-tailing.md).
- The demo (`demo/`) reworks nginx to log to stdout only
  (`NGINX_ENTRYPOINT_QUIET_LOGS: "1"`, dropping its `syslog:` `access_log` destination) and adds a
  `docker_in` leg reading it, replacing the `syslog_in` leg that previously carried it — the
  concrete end-to-end proof this design is built to support.
