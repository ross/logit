---
created: 2026-10-29
updated: 2026-10-30
---

# Enabling plan: `cri_in` — Kubernetes container logs from `/var/log/pods`

## Context

A Kubernetes node running containerd or CRI-O writes each container's output to
`/var/log/pods/<namespace>_<pod>_<uid>/<container>/<restart count>.log` in the CRI log format, and
`logit` has no input that reads it. This plan built `cri_in`, a third kind on `tail_in`'s
driver, under stream key `cri`. Every decision the workstreams share is in
[ADR `cri-container-logs`](../adr/cri-container-logs.md), cited below by its section numbers; this
plan says which workstream landed each one and how it's tested.

Goals:

- `cri_in` reads every container's log under `root:`, reassembles `P` runs per file and stream,
  and stamps the path's pod and container identity, with `container.id` where the kubelet's
  symlink names it.
- A node using cri-dockerd works with the same config.
- The parser is fuzzed, the decoder is checked against files a real kubelet and runtime wrote, and
  a soak run rotates files the way the kubelet does while lines are in flight.

Non-goals:

- Pod labels, annotations, node name, or anything else the path doesn't hold (ADR section 10).
- Joining a line split across a rotation (ADR section 4).
- Decompressing a rotated `.gz` file.
- Windows nodes.

## Workstreams

Each workstream is one PR on branch `cri/w<N>`, stacked on the one before it.

| WS | Branch | Scope | Depends on | State |
|---|---|---|---|---|
| W0 | `cri/w0`, #928 | the ADR and this plan; `docs/design/data-model.md`'s stale line on `docker_in`'s metadata refresh | — | Built |
| W1 | `cri/w1`, #925 | `logit_proto::cri`: the record parser and its timestamp (ADR section 3), the `cri` fuzz target, its seeds, and a `FUZZ_TARGETS` row in `script/unsafe-check` | W0 | Built |
| W2 | `cri/w2`, #930 | the `cri_in` kind: config and schema (section 11), the `Matcher` arm (section 2), `CriDecoder` and its factory with per-stream reassembly (section 4), the idle flush (section 5), event time (section 6), the path resource (section 7); the driver changes in the ADR's "Implementation notes for W2" (the splitter-drop and idle-flush hooks, the per-decoder `\r` and UTF-8 choices, and the `envelope_cap` bound); graph rules (`is_tail_listener`, rules 28 and 62, and a shape rule) and the registry arm; `AGENTS.md`, `docs/deploying.md` (mounts, privileges, and the ADR's documented limits with their workarounds), `docs/design/internal-telemetry.md`, `docs/design/data-model.md`; an example config | W1 | Built |
| W3 | `cri/w3`, #936 | `container.id` from `containers_dir:` (section 8): the key, the id test, the per-scan listing, the cache, and the `refresh` that installs a later-found id; the config key, its schema, and rule 27's empty check; `deploying.md`, `data-model.md`, the DaemonSet example | W2 | Built |
| W4 | `cri/w4`, #937 | cri-dockerd: the per-file format latch and the shared Docker decoder (section 9), with its `malformed` counter on `docker_in` too; decide and implement docker's splitter-drop remedy (known-gaps tailing entry) | W2 | Built |
| W5 | `cri/w5`, #935 | recorded fixtures from a real `kind` node (containerd) and a `minikube` node (CRI-O), through a new `script/record-fixtures cri` recipe, committed under `testdata/interop/cri/` and replayed by a test | W2 | Built |
| W6 | `cri/w6`, #940 | soak scenario `cri-rotate`: kubelet-style rename, reopen, and gzip rotation, with interleaved `P` and `F` runs on both streams, from a stdlib writer (`tools/soak/soaklib/criwriter.py`) the ledger reads through a `cri` wire | W2 | Built |

W3 to W6 depend only on W2, but they are stacked in order, each on one parent.

## Test coverage

Each workstream's tests wait on observables through `logit_pipeline::test_util`, never a sleep
([ADR `test-timing-and-observables`](../adr/test-timing-and-observables.md)), and none needs a
running service. Each regression test was run once with its fix reverted, and the PR body says so.

- **W1, the parser.** Unit tests over committed literal records: containerd's variable-width and `Z`
  timestamps and CRI-O's 9-digit `+00:00` form; `P`, `F`, and `F:x` tags; empty content with its
  trailing space; content ending in `\r`; a piece ending mid-UTF-8; an empty or unknown first tag
  read as a full line, as the kubelet reads it; and each malformed shape (a missing field, an
  unknown stream, no space after the tag). The fuzz target (`cri`) feeds arbitrary bytes and
  checks that the parser never panics, that a parsed record's content is a suffix of its input, and
  that its stream, tag, and timestamp fields read back as the input spelled them.
  Seeds come from the unit literals and from the recorded files W5 committed, through
  `fuzz/seedgen`.
- **W2, the kind.** Decoder unit tests: per-stream reassembly with interleaved runs; the bound
  checked before each append, with exact-at-cap kept and one-over dropped; a record over the
  splitter's bound dropping its stream's line through its `F`, counted once, under a small
  `max_line_bytes` and under an unlimited containerd piece size; a malformed record flushing both
  streams; `reset` counting per held stream; the idle flush firing on the tick only after a read
  reached the file's end, never while unread bytes remain, and off under `0s`; one stream flushed
  on time while the other keeps writing, its run still held; event time from the
  first record and the `bad_time` fallback. Driver-level tests on a scratch pods tree: discovery of
  both names and rejection of `.gz`, `.tmp`, and malformed directory names; a rename rotation
  rebinding with no duplicate; a restart resuming the rotated file from its checkpoint; the
  clean-stop hold. A proptest drives the line splitter, the splitter-drop hook, and `CriDecoder`
  together against a per-stream model, where the reassembly differs from `docker_in`'s.
- **W3, `container.id`.** A scratch `containers_dir` of symlinks: a whole id, a cut name, a link
  made after the file is found, and a link removed while the file is still read, with the id kept.
- **W4, cri-dockerd.** A pods-tree symlink into a scratch Docker container directory: the format
  latch, Docker's 16 KiB reassembly under the path resource, and a Docker rename rotation read
  through the symlink.
- **W5, recorded fixtures.** `script/record-fixtures cri` runs a `kind` node (containerd) and a
  `minikube` node (CRI-O), each with a kubelet configured to rotate small files quickly, and
  runs pods whose containers write partial lines, lines over 16 KiB, interleaved stdout and stderr,
  CRLF, empty lines, invalid UTF-8, and an exit mid-line, plus a static pod. It copies
  `/var/log/pods` and `/var/log/containers` out of each node into `testdata/interop/cri/`. Its
  README holds the provenance table, as for every `testdata/interop/` directory.
  `crates/logit-inputs/tests/cri_interop.rs` replays each tree through `cri_in` and checks the
  reassembled lines against what the pods wrote (`tools/record-fixtures/cri/pods.yaml`).
- **W6, soak.** `cri-rotate` runs on the `cri` wire, which reads like the `file` wire but from a CRI
  writer: the writer produces CRI records into a pods tree shared with the SUT, and rotates the
  live file the kubelet's way on its own timer, while the scenario's steps pause, stop, restart,
  and kill the SUT. The ledger judges `cri_in`'s lines against the writer's, with a dangling run
  flushed by `partial_timeout` and a run split at a rotation each counted as the ADR says. The
  writer is a small one of its own, `tools/soak/soaklib/criwriter.py`, since `file_out` can't
  write CRI records, split a line into pieces, or rotate the kubelet's way. It reports its
  finished lines in `internal`'s NDJSON grammar, and the soak harness's `cri` wire judges
  `cri_in`'s events sent, less its `partial_timeout` flushes, against them
  (`tools/soak/README.md`).

## Mechanics

- The stack is managed with `gh stack` from W0 on, and Ross merges it.
- The reviewer peer session reviews each PR, and its findings are addressed before the next
  workstream builds on it.
- Commit messages carry a conventional type (`feat`, `fix`, `docs`, `test`, `chore`, `perf`);
  PR titles are `cri/w<N>: <summary>`.
- A decision a workstream makes that the ADR doesn't hold goes into the ADR in that PR.
