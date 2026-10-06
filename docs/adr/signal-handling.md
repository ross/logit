---
created: 2026-10-05
updated: 2026-10-05
---

# Signal handling: SIGHUP reopens file targets and never exits, and every handler is installed before config load

## Status
Accepted

## Context
`logit run` installs handlers for SIGTERM and SIGINT only
([ADR `service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md)'s "Shutdown"
section; `shutdown_signal` and `run_pipelines` in `crates/logit-cli/src/pipeline.rs`). Every other
signal gets the kernel's default disposition, except SIGPIPE, which Rust's runtime ignores. Two
gaps follow:

- **SIGHUP kills the process undrained.** The default action for SIGHUP is to terminate: no
  drain, any open `aggregate` window lost, and no `exiting` lifecycle line. Operators send SIGHUP
  routinely, expecting a daemon to survive it: logrotate's `postrotate` script
  (`kill -HUP`), systemd's `ExecReload=`, a Kubernetes config-reloader sidecar, and an `ssh`
  session hanging up on a foreground `logit run`.
- **Startup has no handler at all.** The SIGTERM/SIGINT handlers exist only once `config::load`
  and `prepare` have returned. A SIGTERM during startup (a TLS handshake against a slow CA path, a
  large `lua_file`, a supervisor stopping a unit it started a moment ago) kills the process with no
  drain and no log line.

An external log rotator also can't work with `stdio_out` or `file_out` today. Each holds its file
handle for the process's lifetime, so after logrotate renames the file, `logit` keeps writing into
the renamed inode until it restarts
([ADR `rotating-file-output`](rotating-file-output.md)'s "Alternatives considered"). rsyslog,
nginx, and most daemons a rotator manages answer SIGHUP by reopening their log files, and that's
the signal a stock logrotate stanza sends.

### PID 1 changes what the default disposition does
The release image runs `logit` as its entrypoint (`ENTRYPOINT ["logit"]` in `Dockerfile`, no
`--init`), so in a container it's PID 1 of its PID namespace. Per pid_namespaces(7), the kernel
discards a signal sent to a namespace's init process when that signal's disposition is the
default. So today the same SIGHUP has two outcomes:

- `logit` is PID 1 (the image as shipped): SIGHUP, and a SIGTERM during startup, are silently
  ignored.
- `logit` isn't PID 1 (`docker run --init`, tini, a Kubernetes pod with `shareProcessNamespace`
  where the pause container is PID 1, or any host install): SIGHUP and a startup SIGTERM kill it
  undrained.

## Decision
Every signal `logit run` reacts to has an installed handler, installed before the config is
read, and SIGHUP reopens file targets instead of ending the process.

1. **SIGTERM and SIGINT drain, and a second one exits.** Unchanged from
   [ADR `service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md): the first
   SIGTERM or SIGINT starts a graceful drain, and a second one before the drain finishes exits
   with code `130`. The count starts when the handlers are installed (decision 3), so a second
   SIGTERM or SIGINT during startup exits `130` at once, and a wedged startup stays killable.
2. **SIGHUP never ends the process.** It reopens the file targets of `stdio_out` and `file_out`,
   rsyslog's semantic, so an external rotator in rename mode with a `postrotate` `kill -HUP` works.
   Each SIGHUP logs a stable lifecycle line, `reopen signal received`, at `info` on the `logit`
   target, with a field saying the config isn't reloaded. A SIGHUP doesn't count toward the
   second-signal exit in decision 1: SIGTERM then SIGHUP doesn't exit `130`, and SIGHUP then
   SIGTERM starts a drain.
3. **Handlers are installed before `config::load`.** A signal that arrives during startup is held
   and acted on once the pipeline runs. A SIGTERM or SIGINT during startup starts the drain as
   soon as the pipeline has started. A SIGHUP during startup still increments the reopen
   generation (decision 4), so a file target that opened its file during startup reopens it
   before its first write. A startup that fails still exits `1`, whatever signal arrived first.
4. **A reopen is lazy, per target, and ordered against writes.** A process-wide generation counter
   (a `tokio::sync::watch`) increments on each SIGHUP. The counter is created before
   `config::load`, and each file target takes its snapshot of the generation before it opens its
   file, so a SIGHUP between the two is never marked as seen. The check runs first in the
   file-target arm of `StreamOutput::send`, before the rotation decision, and on a change the
   target reopens its path, so the re-seeded state decides that batch's rotation. The reopen:
   - opens in append mode and never truncates, so a file a rotator copied but didn't move keeps
     its contents;
   - re-seeds `file_out`'s rotation state (bytes written, current period) from the newly opened
     file, as a fresh `FileTarget::open` does;
   - counts one `logit.output.file.reopens`.

   Nothing written after the signal lands in the old inode, except a batch already mid-write when
   the signal arrives. That's why a logrotate stanza for a `logit` file needs `delaycompress`. A
   sink that receives no batch keeps the old inode open until its next one. A failed reopen leaves
   the target with no open handle, and the target's existing self-healing re-open retries on the
   next write, with the batch's fault classified as it is after a failed post-rotation re-open
   ([ADR `rotating-file-output`](rotating-file-output.md)). A `stdout` or `stderr` target ignores
   the generation.
5. **SIGHUP is a Unix signal only.** Non-Unix builds keep `tokio::signal::ctrl_c()` alone, as
   decision 1's ADR records.

### Containers and Kubernetes
Orchestrators stop a container with SIGTERM, then SIGKILL after a grace period. None of Docker,
containerd, or the kubelet sends SIGHUP, so decision 2 doesn't conflict with a stop. With the
handler installed, `logit` behaves the same as PID 1, under `--init`, and on a host:

- `docker kill -s HUP` reopens.
- A config-reloader sidecar's SIGHUP reopens and logs that the config wasn't reloaded. A config
  change in Kubernetes means rolling the pod.
- A reopen matters in a container only when `file_out` writes to a volume that a rotator able to
  signal `logit` manages. `stdout`, or a network sink, stays the recommended container output.

### Not covered
A SIGHUP doesn't reload the config, TLS certificates and keys, `collectd_in`'s `types_db:`, or a
`lua_file` script. Each is read once at startup, and a change to any of them still needs a
restart. Certificate rotation stays the gap `docs/known-gaps/intake.md` records.

## Alternatives considered
- **SIGHUP as a graceful shutdown.** Rejected. systemd's `ExecReload=/bin/kill -HUP $MAINPID` and
  logrotate's `postrotate` would each stop the service, and systemd's `Restart=on-failure` doesn't
  restart a clean exit `0`, so a nightly rotation would leave `logit` down.
- **Ignore SIGHUP, logging it.** Rejected. It fixes the undrained death but leaves an external
  rotator with no way to make `logit` let go of a renamed file, so every logrotate setup still
  needs `copytruncate` and its window of lost lines.
- **SIGHUP reloads the config.** Rejected for now. Hot reload needs its own design: diffing the old
  and new resolved graphs, and deciding which components to keep and which to rebuild
  (`docs/known-gaps/runtime.md`'s config hot reload entry). A future reload would subsume the
  reopen, because rebuilding a file target reopens its file.
- **Reopen eagerly from the signal task.** Rejected. Each target's handle lives inside its sink's
  task, and a reopen from outside would race a write in progress. The lazy check costs one atomic
  load per write and keeps every reopen on the task that owns the handle.

## Consequences
- `crates/logit-cli/src/pipeline.rs` installs the SIGTERM, SIGINT, and SIGHUP handlers before
  `config::load` and owns the generation counter. The second-signal exit counts only SIGTERM and
  SIGINT, starting from the handlers' installation, so a second one during startup exits `130`.
- `crates/logit-outputs/src/file.rs`'s `FileTarget` and `crates/logit-outputs/src/stdio.rs`'s sink
  take a receiver for the generation, check it first in the file-target arm of `send`, before
  `should_rotate` and `rotate`, and count
  `logit.output.file.reopens`. `docs/design/internal-telemetry.md` gains the counter.
- An operator running logrotate against a `logit` file uses rename mode with `postrotate`
  `kill -HUP` and `delaycompress`. `copytruncate` keeps working, and still loses the lines written
  between its copy and its truncate. `file_out`'s own `rotate:` policy and an external rotator
  shouldn't manage the same path: each renames files the other counts on.
- `docs/known-gaps/runtime.md`'s config hot reload entry stays open, with SIGHUP now defined as a
  reopen. `docs/known-gaps/sinks.md`'s two missing-reopen entries and `docs/deploying.md` change
  when the behavior lands.
- A sink that receives no traffic holds the old inode until its next batch, so disk space for a
  rotated file isn't freed until then.
- A foreground `logit run` whose terminal hangs up keeps running. It holds its ports, and a
  `stdio_out` writing to `stdout` fails each write with `EIO`, counted as a `Rejected` drop. nginx,
  rsyslog, and other daemons that handle SIGHUP behave the same way. Run `logit` under systemd or
  a container runtime, not a bare terminal session.
