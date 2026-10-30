---
created: 2026-10-29
updated: 2026-10-30
---

# `cri_in`: Kubernetes container logs from `/var/log/pods`, on `tail_in`'s driver

## Status
Accepted. Partly built: `cri_in` reads CRI-format files under `root:` as sections 1 to 7, 10, and
11 describe. `container.id` (section 8) and cri-dockerd's Docker-format files (section 9) are the
[`cri` plan](../plans/cri-container-logs.md)'s next workstreams, so those two sections describe
`cri_in` as it will behave.

## Context

On a Kubernetes node, the kubelet asks the container runtime to write each container's stdout and
stderr to a file it names, and a per-node collector tails those files. `logit` tails Docker's
json-file logs (`docker_in`) and plain files (`tail_in`), but nothing reads the format containerd
and CRI-O write, so a node running either had no container-log input. The `cri` plan builds one in
six workstreams; this record holds the decisions they share.

**Paths.** The kubelet builds the layout (`pkg/kubelet/kuberuntime/helpers.go`,
`BuildPodLogsDirectory` and `buildContainerLogsPath`):

```
/var/log/pods/<namespace>_<pod name>_<pod uid>/<container name>/<restart count>.log
```

A namespace, a pod name, and a container name can't hold `_`, so a pod directory name's first two
`_` end its namespace and its pod name, and the rest is its uid. A uid the API server or the kubelet
generates is hex and `-`, but a static pod's manifest can set its own uid, and that uid may hold `_`
(section 2). The kubelet also writes a symlink per container attempt,
`/var/log/containers/<pod>_<namespace>_<container>-<container id>.log`, pointing at that file
(`pkg/kubelet/kuberuntime/legacy.go`, `logSymlink`). It cuts the link's name to 251 bytes before
appending `.log`, so a long name loses its container id.

**Line grammar.** One record per line, from `k8s.io/cri-api`'s `constants.go` and the kubelet's
reader (`staging/src/k8s.io/cri-client/pkg/logs/logs.go`, `parseCRILog`):

```
<timestamp> SP <stdout|stderr> SP <tag>[:<tag>...] SP <content> LF
```

The runtimes write a first tag of `P` (a piece of a longer line) or `F` (a full line, or the last
piece of one). The kubelet reads only the first tag: `P` is partial, and any other value, empty
included, is a full line ("Keep this forward compatible"). It ignores further tags. Content holds no
LF, may hold `\r` and bytes that aren't UTF-8, and may be empty.

**The two runtimes differ in ways a reader sees:**

| | containerd (`internal/cri/io/logger.go`) | CRI-O (conmon, `src/ctr_logging.c`) |
|---|---|---|
| Timestamp | RFC 3339 with 0 to 9 fraction digits (trailing zeros trimmed), node-local zone, `Z` whenever the local offset is zero (Go's `time.RFC3339Nano` writes `Z` for a zero offset, not only for UTC) | 9 fraction digits, node-local zone, always a numeric offset (`+00:00`, never `Z`) |
| When it writes `P` | a line over `max_container_log_line_size` (16 KiB by default; `-1` or `0` means no limit) in pieces of that size; the unterminated rest of a stream at EOF | each `read(2)` of at most 8 KiB that didn't end in a newline, so a short line can arrive as `P` then `F` |
| A cut inside a UTF-8 sequence | yes: the size cut counts bytes | yes: a `read(2)` boundary |
| CRLF | strips the `\r` before the `\n` | keeps it in content |
| stdout and stderr | one goroutine per stream through one writer, so a `P` of one stream can be followed by records of the other before its `F` | one loop over both pipes; runs of the two streams interleave the same way |

A third runtime path, cri-dockerd, makes `/var/log/pods/.../<n>.log` a symlink to Docker's own
json-file log, so the file under that name holds Docker JSON lines, not CRI lines (cri-dockerd's
`createContainerLogSymlink`, defined in `core/logs.go` and called from `core/container_start.go`).

**Rotation.** The kubelet's `ContainerLogManager` (`pkg/kubelet/logs/container_log_manager.go`)
checks each running container's file every `containerLogMonitorInterval` (10 s by default). Over
`containerLogMaxSize` (10 MiB), `rotateLog` removes stale `.tmp` files and plain files with a `.gz`
twin, keeps at most `containerLogMaxFiles - 2` older rotated files (`containerLogMaxFiles` is 5 by
default), gzips every remaining uncompressed rotated file, then renames the live file to
`<n>.log.<YYYYMMDD-HHMMSS>` (the kubelet's local time) and asks the runtime to reopen. A failed
compression returns before the rename, so the newest rotated file is the only uncompressed one.
Between the rename and the reopen the runtime keeps writing to the renamed inode. containerd writes
a stream's unterminated bytes into the renamed file as a `P` when it closes the old logger; conmon
opens `<n>.log.tmp` and renames it onto `<n>.log`.

**What other collectors do.** Each reads the path for pod identity and reassembles `P` runs, but
they differ in the details this record decides:

| Collector | Reassembly key | Dangling `P` | `k8s.container.restart_count` | `container.id` |
|---|---|---|---|---|
| Vector `kubernetes_logs` | file path | emitted after 30 s | not from the path | from the API server, `<runtime>://` prefix kept |
| OpenTelemetry Collector `filelog` `container` operator | file path | flushed after 5 s | a string | not set |
| Fluent Bit `cri` multiline parser | stream | flushed after 4 s | not from the path | from the `/var/log/containers` name, else the API server |

Keying on the path alone joins a stdout piece to an interleaved stderr record. Fluent Bit's
per-stream grouping is the one that matches both runtimes.

Each fact above was read in the source file it names, on that project's default branch, on
2026-10-29.

## Decision

`cri_in` is an input kind: a third thin wrapper over `tail_in`'s driver, beside `tail_in`
and `docker_in`, that reads every container's log under `/var/log/pods`, reassembles `P` runs per
file and stream, and stamps each line's pod and container identity from its path.

### 1. A kind of its own on `Tailer`

`cri_in` wraps `Tailer<CriDecoder, CriDecoderFactory>` with a `Matcher::PodLogs` arm for the pods
layout, as `docker_in` wraps it with `Matcher::DockerContainers`. Rotation, truncation, checkpoints,
content claims, the compressed skip, back-pressure, and batching are the driver's, unchanged
([ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md),
[ADR `tail-discovery-failure-and-resume-identity`](tail-discovery-failure-and-resume-identity.md),
[ADR `tail-content-identity`](tail-content-identity.md)).

It isn't a mode of `docker_in` or `tail_in`, for the reasons that made `docker_in` a kind: its
discovery is a layout no glob expresses, its resource is facts read from the path, its timestamp
comes from the line, and it has config the others don't.

### 2. Discovery: walk `root:`, match two file names, watch only `root`

`cri_in` lists `root:` (default `/var/log/pods`), then each `<namespace>_<pod>_<uid>` directory,
then each container directory, and match two names in it:

- `<n>.log`, where `<n>` is decimal digits that fit an `i64`: the live file.
- `<n>.log.<YYYYMMDD-HHMMSS>`: a rotated file the kubelet hasn't compressed yet.

A pod directory name is split at its first two `_`: the namespace, the pod name, and the rest
as the uid, so a static pod whose manifest-set uid holds `_` is read with its whole uid. The
kubelet's own reader of this name, `parsePodUIDFromLogsDirectory`
(`pkg/kubelet/kuberuntime/helpers.go`, for its log garbage collection), also splits on `_`, but
takes the last part as the uid, which such a uid would cut short; splitting at the first two `_` is
the form `BuildPodLogsDirectory` writes. A pod directory name with fewer than two `_` or an empty
part, a container directory name holding `_` or `.`, and every other file name isn't matched: no
`.gz`, no `.tmp`, and no conmon `.new` or `.1`. An unmatched name is skipped with no counter and no
diagnostic, as a name outside a `tail_in` pattern is. A symlinked directory isn't followed, as
`docker_in`'s walk doesn't follow one. A file name is matched whether it's a regular file or a
symlink to one (section 9). Failed listings follow the driver's rule: a listing that failed
retires nothing, and a pod or container directory whose listing failed retires no file beneath it
([ADR `tail-discovery-failure-and-resume-identity`](tail-discovery-failure-and-resume-identity.md),
decision 1).

**The rotated name is matched so that a restart resumes the file the kubelet renamed.** Within
a run the driver doesn't need it: a rename changes the inode at `<n>.log`, and the old handle drains
to its end before it closes. Across a restart it does. A checkpoint entry for an inode no pattern
names counts its unread bytes `logit.input.bytes.dropped{reason="gone"}`, so a `logit` stopped
between the kubelet's rename and its next rotation would lose the rotated file's tail. With both
names matched, the renamed inode rebinds to its new path and stay open (no re-read, no
duplicate) until the kubelet compresses it and removes the plain file, one rotation later. Every
uncompressed rotated name is matched, not only the newest: a failed compression aborts the
rotation, so the kubelet leaves at most one, and choosing the newest would need a per-directory
comparison to exclude a file that still holds data.

A `.gz` file is never matched. When `logit` read the plain file before the kubelet compressed
it, nothing is lost. When it didn't, because `logit` was down past the next rotation, the plain
file's checkpoint entry counts `bytes.dropped{reason="gone"}`, the driver's existing path. The
driver never decompresses ([ADR `tail-content-identity`](tail-content-identity.md), decision 2).

**The `inotify` watch set is `root` plus each open file**, as for `docker_in`
([ADR `docker-container-identity-and-minimal-watches`](docker-container-identity-and-minimal-watches.md)).
A new pod directory wakes a scan at once. A new container directory, a restart's `<n+1>.log`, and a
rotation are two levels below `root`, so they ride the `poll_interval` tick. A rename rotation has
no loss window for the tick to widen.

**Discovery doesn't read `/var/log/containers`.** Its names put the pod before the namespace, carry no
uid, and lose their container id past 251 bytes, and a symlink is made after the container starts,
so lines can exist before the link does. Section 8 will read it for `container.id` alone.

### 3. The line parser lives in `logit-proto`, with a fuzz target

`logit_proto::cri` parses one record into a timestamp, a stream, a `P` or `F` flag, and the
content. It reads raw bytes from a file any container's output reaches, so it sits with the other
decoders of untrusted bytes and has a fuzz target, a seed set, and a `FUZZ_TARGETS` row
([ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md)).

- **Timestamp:** `logit_core::parse_rfc3339_to_nanos`, which takes 0 to 9 fraction digits and `Z` or
  a numeric offset, both runtimes' forms.
- **Stream:** `stdout` or `stderr`; anything else is malformed, as the kubelet rejects it.
- **Tag:** the field up to the next space, split on `:`. A first tag of `P` is partial, and any
  other first tag, empty included, is a full line, the kubelet's rule; further tags are ignored. A
  record is malformed only when its fields aren't there or its stream is unknown, never for its tag,
  so `cri_in` delivers every record `kubectl logs` shows.
- **Content:** every byte after the tag's space. The space must be there, even for empty content.

**A trailing `\r` stays in the content.** For an `F` record it's a CRLF line's `\r`, which CRI-O
writes and the kubelet hands to `kubectl logs` unchanged, and `docker_in` keeps it inside the
envelope's `log` the same way. For a `P` record it's a byte in the middle of a line, cut at a read
or size boundary, and removing it would change the reassembled line. So `cri_in` turns off the
line splitter's one-`\r` strip, which every other tail kind keeps. The same CRLF output then reads
differently per runtime: containerd removes the `\r`, CRI-O keeps it. `cri_in` relays
what the runtime wrote; a downstream transform trims it where an operator wants one form.

**UTF-8 is checked once, over the reassembled line.** Both runtimes cut a line by bytes, so a
`P` piece can end inside a multi-byte character. For the other tail kinds, the driver lossily converts each physical line
before the decoder sees it, which would turn such a character into two `U+FFFD`s. `cri_in` takes
the record's raw bytes, reassemble them, and convert the finished line, with the driver's
`invalid_utf8` diagnostic when the conversion replaced anything.

### 4. Reassembly per file and stream

A `P` record opens or extends a held run for its stream, and the next `F` on that stream closes
it into one event. Each file holds one run per stream, because both runtimes interleave the two
streams inside a run. The state and its rules follow `DockerDecoder`'s:

- **Bound:** `max_line_bytes` (1 MiB by default) bounds the reassembled content, checked before each
  append, so a held run never exceeds it. A line over it is dropped whole, counted
  `logit.input.frames.dropped{reason="oversize"}` once with a `long_line` diagnostic, and its later
  pieces are discarded until its `F`.
- **The splitter's bound and its drops:** the line splitter is bounded by
  `envelope_cap(max_line_bytes)`, the bound `docker_in` uses (`6 × max(max_line_bytes, 16 KiB)` plus
  64 KiB), so a Docker-format file (section 9) gets `docker_in`'s bound, and every containerd piece
  at its default 16 KiB and every CRI-O piece reaches the decoder whatever `max_line_bytes` is. A
  record over that bound (a containerd piece under a raised or unlimited
  `max_container_log_line_size`) is dropped by the splitter before the decoder sees it, so the
  splitter hands the decoder the dropped record's leading bytes through a `TailDecoder` hook
  (Implementation notes for W2). From them the decoder reads the record's stream and tag, drops that
  stream's held run, discards every later piece of the stream through its `F` (none when the dropped
  record is itself the `F`), and counts the whole line once as `frames.dropped{reason="oversize"}`,
  with one `long_line`. A stream already dropping its line isn't counted again, and a dropped `F`
  ends its stream's drop, whether the drop began with it or earlier. When the leading bytes don't
  parse, the decoder flushes both streams' held runs, as before a malformed record, and the driver
  counts the drop as it does for the other kinds. Without the hook, the tail of a line whose middle the splitter
  dropped would leave as a line of its own, and each dropped piece would count once.
- **Checkpoint:** while a run is held, `holds_entry` pins the checkpoint at the run's first record,
  so a crash replays the whole line. As for `docker_in`, the pin is the oldest held record across
  both streams.
- **Clean stop:** `hold_at_shutdown` is true, so an `Active` file's unterminated last record is
  never decoded at a clean stop; the checkpoint, when there is one, stays at its start. Its held
  runs are kept unemitted only when a checkpoint is configured, so the restart reads them whole;
  with no checkpoint, nothing would re-read them, so they're emitted as they stand (the file-tailing
  ADR's "Amendment: `docker_in` holds its unterminated tail and held fragments at a clean stop
  (2026-10-08)", and `close_all_for_shutdown` in `tail/driver.rs`).
- **Flush:** a held run is emitted as it stands when its file closes, when its idle timer fires
  (section 5), and before a malformed record is reported.
- **Truncation:** `reset` drops held runs, counted `frames.dropped{reason="truncated"}` once per
  stream holding one, as `docker_in` counts.
- **Malformed record:** both streams' held runs are flushed, stdout first, then the record is
  reported. It's counted `logit.input.frames.dropped{reason="malformed"}` beside the driver's
  `bad_line` diagnostic, since a record's loss reported only by a diagnostic needs a counter
  ([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)). `docker_in` counts its rejected
  json-file entry nowhere today; section 9 closes that for both kinds.

A run split across a rotation leaves as two lines: containerd writes the head as a `P` at the
end of the renamed file, and the continuation starts the new file, a different inode with its own
decoder. Joining them would mean pairing two files' state by stream, across a rename the driver
treats as two files, for a case that occurs once per rotation per stream with a line in flight.

### 5. A dangling run flushes after an idle `partial_timeout`

A held run is emitted as it stands once its stream has had no new record for `partial_timeout`
(default `5s`) and the reader has caught up with its file. A `P` with no `F` is normal: containerd
writes one when a container exits mid-line and at a reopen, and CRI-O writes one whenever a write
was split across reads. Holding it until the file closes would hold it until the kubelet's container
garbage collection removes the file, minutes or never, with the checkpoint pinned the whole time.

- **The file must be at its end.** A file is a candidate only once a read of it has reached its end
  with a run held, and only while no read since has returned bytes. On the `poll_interval` tick, the
  driver also confirms through the open handle's length that the file has no unread bytes, and reads
  it instead if it has. So a reader that is behind (a backlog after a restart, or a send blocked by
  back-pressure) never flushes a run whose `F` is already in the file.
- **Each stream expires by its own last record.** For a file at its end, the decoder flushes each
  stream whose last record is at least `partial_timeout` old, and leaves the other stream's run
  held. So a stdout prompt with no `F` is flushed on time while the container's stderr keeps
  writing, instead of pinning the checkpoint until stderr goes quiet. A dangling run waits between
  `partial_timeout` and `partial_timeout` plus one `poll_interval` from its last record, once the
  reader has caught up.
- Ages are measured on the process's monotonic clock (tokio's `Instant`, so a test can advance
  paused time), not the CRI timestamps, which are the node's
  local clock and are written per record or per read.
- A flush also ends an oversize drop in progress, so the stream's next record starts a new line.
- `partial_timeout: 0s` turns the timer off: a run waits for its `F` or for the file to close.
- A timer flush counts `logit.input.frames.truncated{reason="partial_timeout"}`. It isn't loss, so
  it isn't `*.dropped`: the run is delivered, and when its continuation arrives later, that is
  delivered too, as a second event. `*.truncated` is the vocabulary's word for delivered but
  shortened, and the count is what tells an operator whether `partial_timeout` is too short for
  their writers. A flush at a file's close or before a malformed record isn't counted, as
  `docker_in` counts neither.

The one case left that splits a line is a writer that stalls mid-line for longer than
`partial_timeout`: its line leaves as two events, split at the stall. That's the price of not
holding every exited container's last line until its file is removed.

### 6. Event time is the first record's timestamp

An event's timestamp is its line's first record's CRI timestamp, the moment the runtime started
writing the line. A record whose timestamp doesn't parse is still decoded, with the read time
in its place and a `bad_time` diagnostic, as `docker_in` does. Read time would stamp a replayed
backlog as now (the file-tailing ADR's "`docker_in` timestamps").

### 7. Resource and event attributes from the path

Each file's resource holds the semantic-convention keys its path names:

| Key | Type | From |
|---|---|---|
| `k8s.namespace.name` | string | the pod directory's first part |
| `k8s.pod.name` | string | its second part |
| `k8s.pod.uid` | string | the rest, after its second `_` |
| `k8s.container.name` | string | the container directory |
| `k8s.container.restart_count` | `i64` | the file name's `<n>` |
| `container.id` | string | section 8, when known (not yet built) |

Each event carries `log.iostream` (`stdout` or `stderr`) and its line as an unparsed message
(`body_format: Raw`, no severity), as `docker_in`'s does. A rotated file has its live file's
resource.

**The path layout is this protocol's wire for the common-keys rule.** The kubelet defines it, and
every value above is a field of it, so `cri_in` writes the keys as a source writes the keys its
protocol defines ([ADR `process-hostname`](process-hostname.md)'s "Amendment: a source fills the
common keys (2026-10-23)"). `k8s.container.restart_count` is an integer, the semantic convention's
type, although the OpenTelemetry Collector's `container` operator writes the directory text as a
string; `i64` rather than `u64` keeps it an OTLP integer at `otlp_out`.

**A static pod's `k8s.pod.uid` isn't its API uid.** For a static pod whose manifest sets no uid, the
kubelet sets one: an FNV-128a hash of the decoded pod object, then the node name and the manifest's
file path for a file source, or its URL for a URL source (`pkg/kubelet/config/common.go`,
`applyDefaults`). The API server's mirror pod gets a uid of its own. The path's uid is the one
`cri_in` writes. A consumer joining to the API's pod objects matches a static pod by namespace
and name.

### 8. `container.id` from the `/var/log/containers` symlinks, best effort

`cri_in` will list `containers_dir:` (default `/var/log/containers`), read each symlink's target,
and index it by the target's last three path components (`<pod dir>/<container>/<n>.log`) to the id,
when the link name's last `-`-separated part before `.log` is 64 lowercase hex characters, the form
Fluent Bit's `kubernetes` filter reads. Keying on the last three components rather than the whole
target lets a `root:` mounted at another path still match.

- The value is the bare hex id, never a `containerd://` prefix: semantic conventions' form, and the
  one OpenTelemetry's `k8sattributes` processor and SDK resource detectors write, so a log joins to
  a trace or metric from the same container.
- It's cached per file identity, so the kubelet removing the link after the container exits doesn't
  remove the id from that file's later lines, and a rotated file has its live file's id.
- The listing runs on the `poll_interval` tick and on a miss, since the kubelet makes the link after
  the container starts. An id found later replaces the file's resource, which starts a new batch, as
  `docker_in`'s identity refresh does.
- A missing link, or a name cut short of a whole id, writes no `container.id`. A failed listing
  keeps the cache and is diagnosed (`container_id_error`), with no counter: no data is lost.

### 9. cri-dockerd's files are recognized and decoded as Docker JSON

`cri_in` will latch each file identity's format from its first byte: `{` is Docker json-file, and
anything else is CRI. A Docker-format file will go through `docker_in`'s envelope decoder and its 16
KiB partial-line reassembly, under `cri_in`'s path resource, and the splitter bound
`envelope_cap(max_line_bytes)` (section 4) is the one `docker_in` gives it. The file under the pods
name is a symlink there, and the driver stats and opens through it, so the identity is the target's
inode, and a Docker rotation (a rename of the target) reads as a rotation. An empty file stays
undecided until its first byte.

The Docker decoder can't use the splitter-drop hook the way `CriDecoder` does: json-file writes
`stream` after `log`, so a dropped envelope's leading bytes can't name its stream. W2 leaves it on
the hook's default, so the driver keeps counting the drop, and `docs/known-gaps/tailing.md`'s "An
envelope over the cap is dropped by the splitter without the decoder seeing it" stays open. W4
decides and builds a remedy that doesn't need the stream, under `cri_in` and `docker_in` alike.
Under `envelope_cap`, only an `attrs` object past the bound's slack or a writer other than dockerd
produces such an envelope.

The shared decoder will count a rejected json-file entry as
`logit.input.frames.dropped{reason="malformed"}`, as section 4 counts a CRI record, so `docker_in`
will gain the counter its `bad_line` diagnostic lacked.

### 10. What `cri_in` doesn't read

- **No API server, no CRI socket, no root-only runtime state** (`/run/containerd`,
  `/var/lib/containers`). Each would need a client or more privilege than reading the log directory,
  as `docker_in`'s ADR found for the docker socket.
- **No pod labels or annotations**, which only those sources hold.
- **No `host.name`**: the path names no node. A sink's process-hostname fallback names the node
  `logit` runs on, which for a per-node collector is the node, and `set` or the top-level
  `hostname:` names another ([ADR `process-hostname`](process-hostname.md)).
- **No selection config.** `cri_in` follows every container under `root:`. A node collector
  usually wants all of them, and the path's keys are on every event, so `has_attributes` or
  `drop_attributes` on `k8s.namespace.name` or `k8s.pod.name` narrows the flow downstream
  ([ADR `attribute-filtering-components`](attribute-filtering-components.md)). `docker_in`'s
  explicit default exists because a Docker host's containers are named one by one; a Kubernetes
  node's are scheduled onto it.
- **No stream filter**, as `docker_in` has none; a downstream stage reads `log.iostream`.

### 11. Configuration

`cri_in`'s own keys are:

| Key | Default | Meaning |
|---|---|---|
| `root` | `/var/log/pods` | the kubelet's pod log directory; an empty value is rejected |
| `containers_dir` | `/var/log/containers` | where the kubelet's symlinks are, for `container.id`; an empty value is rejected, and a missing directory is an empty listing (not yet built) |
| `partial_timeout` | `5s` | section 5; `0s` turns the timer off |

It flattens `TailOptions` as `tail_in` and `docker_in` do: `checkpoint_path`, `read_from`,
`watch`, `poll_interval`, `checkpoint_interval`, and `max_line_bytes`, the last bounding the
reassembled line as under `docker_in`. Its `receive:` block takes the tail listeners' three
batch-assembly fields and `shutdown_grace`.

`containers_dir` arrives with section 8's lookup, not before it: a key that configures nothing would
pass `logit validate` while reading nothing, and the config's `deny_unknown_fields` rejects it until
then. An empty `root` is rejected under the graph's rule 27, beside the other empty-path checks,
rather than under a rule number of its own.

## Alternatives considered

- **A mode of `tail_in` or `docker_in`.** Rejected for the reasons in section 1. A `tail_in` glob
  can't name the three-level layout or derive a resource from it, and `docker_in`'s `containers:`,
  `labels:`, and `host_name:` mean nothing here.
- **Discovery from `/var/log/containers`**, as Fluent Bit's manifests do. Rejected in section 2: no
  uid, names cut at 251 bytes, a link made after the container starts, and no rotated names.
- **Reassembly keyed by file**, as Vector and the OpenTelemetry Collector key it. Rejected: an
  interleaved record of the other stream would be joined into the line.
- **Holding a dangling `P` until the file closes.** Rejected in section 5: the file can outlive the
  container by minutes, or stay until the pod is deleted, with the checkpoint pinned.
- **Truncating an over-long line instead of dropping it.** Rejected for the reason the file-tailing
  ADR gives: a truncated line looks whole to a parser downstream.
- **`container.id` from the API server or the runtime's state.** Rejected in section 10.
- **Matching only the live `<n>.log`.** Rejected: a restart between a rotation and the next one
  would count the renamed file's tail `gone` (section 2).
- **Stripping a trailing `\r`**, as the driver does for every other kind. Rejected: it changes a `P`
  record's content (section 3).
- **Deciding the runtime from the timestamp's form**, as the OpenTelemetry Collector does. Not
  needed: one parser takes both forms, and conmon's `+00:00` and containerd on a non-UTC node look
  alike anyway.

## Consequences

- `ComponentKind::CriIn` has `root`, `partial_timeout`, and flattened `TailOptions`, with
  `containers_dir` to come (section 11), and a graph shape rule beside `docker_in`'s. `cri_in` joins `is_tail_listener`,
  which feeds the `receive:` rules (17 and 18), and joins rule 28 (positive `poll_interval`,
  `checkpoint_interval`, and `max_line_bytes`) and rule 62 (a unique `checkpoint_path`) beside
  `tail_in` and `docker_in`, which those two rules name by kind. Without rule 62, a `cri_in` sharing
  a `checkpoint_path` with another tail listener would overwrite its offsets.
- The driver has the hooks and choices in "Implementation notes for W2". `tail_in` and
  `docker_in` keep their behavior. The splitter reports its drops in sequence with its lines, so the
  driver counts a drop the decoder didn't take where it decodes, not in bulk after the read: a drop
  that falls after a batch the closed consumer didn't take goes uncounted, as a line there goes
  undelivered.
- A file's splitter can hold up to `envelope_cap(max_line_bytes)` of an unterminated record, about 6
  MiB at the default, as `docker_in`'s can.
- `DockerDecoder`'s envelope decoding and reassembly will become reachable from `cri_in`, and the
  shared decoder will count a rejected entry (section 9, not yet built).
- New counter uses: `logit.input.frames.truncated{reason="partial_timeout"}`, and
  `frames.dropped{reason="malformed"}` on `cri_in` (and on `docker_in` with section 9). New
  diagnostic key, with section 8: `container_id_error`.
- Reading needs what `docker_in` needs: the runtimes create each log file `0640` as root, so
  `logit` runs as root or with a group grant, with `/var/log/pods` (and, with section 8,
  `/var/log/containers`) mounted read-only. Under cri-dockerd, the symlinks' targets, Docker's container directory, must be
  mounted at their host path too.
- One open descriptor and one file watch per rotated file the kubelet hasn't compressed, at most one
  per container in the kubelet's steady state.

**Documented limits**, each decided here, with the operator's workaround in
[`docs/deploying.md`](../deploying.md)'s "Kubernetes container logs":

- **A line split across a rotation leaves as two events** (section 4).
- **A line whose writer stalls mid-line longer than `partial_timeout` leaves as two events**
  (section 5). A reader that is behind doesn't split one: each stream expires by its own last
  record, and only while the reader is at the file's end with no unread bytes.
- **A static pod's `k8s.pod.uid` is the kubelet's hash, not the mirror pod's uid**, unless its
  manifest sets a uid (section 7).
- **A symlink name the kubelet cut short writes no `container.id`**, and neither does a container
  whose link isn't there yet or was removed before `cri_in` listed it (section 8).
- **No pod labels or annotations, and no node name** (section 10). An enrichment stage or `set` adds
  what a deployment needs.
- **conmon's own size rotation isn't followed across a restart.** CRI-O's `log_size_max`, off by
  default, makes conmon replace the file (or, with its rotate option, rename it to `<n>.log.1`)
  outside the kubelet's naming. Within a run the replaced inode drains; across a restart its unread
  tail counts `gone`.
- **Under cri-dockerd, a Docker-rotated file (`-json.log.1`) is outside `root:`**, so its unread
  tail at a restart counts `gone`, the same as `docker_in` counts it.

## Implementation notes for W2

The driver changes `cri_in` needs, as W2 built them:

- **A splitter-drop hook on `TailDecoder`:** `fn line_dropped(&mut self, head: &[u8], start: u64,
  out: &mut Vec<Event>) -> bool`, with a default that returns `false`. `LineSplitter` keeps the
  first bytes of each line it drops (enough for a record's timestamp, stream, and first tag; 128
  bytes) and reports each drop in sequence with the line's start offset, between the lines it emits.
  The driver calls the hook for each drop. It returns `true` when the decoder took the drop (counted
  it with its `long_line`, or absorbed it into a line it already counted), so the driver counts
  neither, and `false` when the head doesn't parse, so the driver counts as it does for the other
  kinds. The driver then updates `held_from` from `holds_entry`, as after `decode_line`, so a
  checkpoint during the drop stays at the line's first record. `CriDecoder` implements it (section 4); `DockerDecoder`
  and `LineDecoder` keep the default (section 9).
- **The head parse** reads the timestamp, the stream, and the first byte or bytes of the tag, enough
  to tell `P` from any other first tag. It doesn't require the space after the tag within the
  128-byte head, since a long tag field can push that space past it.
- **An idle-flush hook on `TailDecoder`:** `fn flush_idle(&mut self, now: Instant, out: &mut
  Vec<Event>)`, with tokio's `Instant` and a no-op default, which the driver calls on each poll
  tick for every file at its end (section 5): a read of it has reached its end while `holds_entry` was true, no read since has
  returned bytes, and the open handle's length shows no unread bytes. The decoder keeps each
  stream's last-record `Instant`, taken when a record leaves the stream holding a run, and its
  `partial_timeout`, and flushes each stream whose last record is at least that old.
- **Per-decoder choices:** whether the splitter strips one trailing `\r`, whether the driver
  lossily converts each line to UTF-8 before `decode_line` (section 3), whether the splitter keeps
  each dropped line's head, and whether the driver tracks the at-end state and calls `flush_idle`.
  Each is an associated `const` on `TailDecoder` whose default is the other kinds' behavior, so
  `tail_in` and `docker_in` skip the at-end tracking and the idle flush entirely.
- **The splitter bound** comes from the kind, as `docker_in`'s does: `envelope_cap(max_line_bytes)`
  for `cri_in`.
