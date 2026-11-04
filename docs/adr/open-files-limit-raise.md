---
created: 2026-11-04
updated: 2026-11-04
---

# `logit run` raises its soft open-files limit to the hard limit

## Status
Accepted

## Context
Every stream listener defaults to `max_connections: 1024`. A stock systemd service has a soft
`RLIMIT_NOFILE` of 1024, and recent Docker and containerd start a container at soft 1024, hard
524288. One listener at its default therefore runs out of descriptors before its cap binds, and
the connections, sink sockets, tailed files, and spools draw on the same limit. Until now `logit
run` only warned about it (`fd_limit_warning` in `crates/logit-cli/src/pipeline.rs`), and on a
default box the warning fired on every start.

The usual reason to leave the soft limit low is `select(2)`, which can't watch a descriptor at or
above `FD_SETSIZE` (1024). `logit` doesn't call `select`: its I/O runs on tokio's epoll, so the
reason doesn't apply. The Go runtime has raised its soft limit to the hard limit at startup for
every Go program since Go 1.19, for the same reason.

## Decision
`logit run`, before it prepares the graph, binds a listener, or opens a file, raises the soft
`RLIMIT_NOFILE` to the hard limit (`crates/logit-cli/src/fdlimit.rs`).

- If the soft limit is already at the hard limit, or is unlimited, nothing changes and nothing is
  logged.
- If the hard limit is `RLIM_INFINITY`, the target is the kernel's `/proc/sys/fs/nr_open`, because
  `setrlimit` fails with `EPERM` for a soft limit above it. If that file can't be read, the soft
  limit stays and the failure is logged.
- A raise logs one `info` line with the old and new soft limits. A failure logs one `warn` line and
  startup continues; the raise never fails the start.
- There is no config field to turn it off.
- `logit validate`, `graph`, and `schema` don't raise it: the limit belongs to the process, not to
  the config.
- Linux only, like the startup warning, which reads `/proc/self/limits` after the raise. The
  warning stays and now means the hard limit is too low; its text points at systemd's
  `LimitNOFILE=` and `docker run --ulimit nofile=`.

## Alternatives considered
- **Warn only, as before.** Rejected: the warning fired on every default deployment, so it carried
  no information, and the fix is a setting the operator can't always reach (a managed Docker
  host).
- **A config field.** Rejected: no deployment wants the soft limit lower than the hard limit that
  the operator already granted, and a field adds a validation surface for nothing.
- **Restore the soft limit before spawning children.** Not needed: `logit` starts no child process,
  and a Lua script's sandbox has no `os` or `io` library.
- **Raise past the hard limit.** Not possible without `CAP_SYS_RESOURCE`, and a limit the operator
  set is the operator's call.

## Consequences
- A default listener's 1024 connections fit under the soft limit on a stock Docker or systemd host.
- A box where the operator relied on a low soft limit to bound the process now sees the hard limit
  as the bound; they lower the hard limit instead.
- A descriptor leak takes longer to surface as `EMFILE`; `logit.process.fds` and
  `logit.process.fds.limit` (see [internal-telemetry.md](../design/internal-telemetry.md)) are how
  it shows.
