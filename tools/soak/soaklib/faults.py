"""The fault actions a scenario step names: how each is applied and reverted, and which legs of
the data path it affects.

`ACTIONS` maps an action name to an `Action`. `apply` and `revert` take the driver's `Context`
and the `scenario.Step`, and return a dict for `timeline.jsonl`: `rc`, `stderr`, and, for netem,
`netem_show` (the target's root qdisc after the change). The two predicates take the step and
the run's role map (`{service: role}`), so they judge a service by its role, not its name.

Facts a maintainer needs here (docs/plans/soak-harness.md, "Netem scope"):

- A netem change is a one-shot container in the target's network namespace. The qdisc outlives
  it, and a root qdisc on the default-route interface shapes the target's egress only.
- `limit 100000` is appended unless the step names a `limit`: headroom so netem's own queue
  never drops. A delay plus a post-outage burst can queue more than netem's default 1,000
  packets, and a drop there would read as a fault the step didn't ask for.
- `docker network connect` drops a container's aliases, so a partition records them before the
  disconnect and passes each back with `--alias`. It also records the container's IP address on
  each network before the disconnect (`ips`) and after the reconnect (`ips_after`): a TCP peer's
  retransmits after a partition reach the container only at the address it had, and the
  reconnect may assign another.
- A `reset` is a one-shot netem container in the target's network namespace running
  `netem.sh reset`, which closes its established TCP connections, or those on the step's `port`,
  with `ss -K`: the kernel sends each peer a reset, and the closing side reads `ECONNABORTED`.
  `ss -K` needs CAP_NET_ADMIN, which the container gets, a kernel built with
  `CONFIG_INET_DIAG_DESTROY`, without which it closes nothing, and, under SELinux, an
  unconfined container (`--security-opt label=disable`, which a `reset` run passes). A reset that closed no
  connection is `rc` 1, so `timeline` FAILs it and it gets no window. Its revert does nothing:
  the client dials again on its own.
- A `prlimit` is a one-shot netem container in the target's PID namespace
  (`--pid container:<id>`) with CAP_SYS_RESOURCE, running util-linux's `prlimit` against PID 1,
  which is `logit`: compose.yaml's entrypoint runs it with no init. The apply reads the soft and
  hard `RLIMIT_NOFILE` (`found`), then sets both to the step's `nofile`; the revert sets
  `found` back. Neither needs `label=disable`, and `docker update` can't change a running
  container's ulimits. Lowering the limit closes nothing already open. A process started
  inside the window starts with the container's own limit, so the fault is gone: the revert
  then sets nothing and records `started_in_window`, as a pause's revert records `unpaused`.
- A `cgroup` writes `cpu.max` or `memory.high` in the target's own cgroup for its window,
  through a one-shot netem container with the host's `/sys/fs/cgroup` bound read-write at
  `CGROUP_MOUNT` and `--security-opt label=disable`, without which SELinux denies the write. The
  cgroup's path is the cgroup v2 (`0::`) line of `/proc/<pid>/cgroup` on the host, for the PID
  `docker inspect` reports, and must name the container's full id; under the systemd driver
  it's `system.slice/docker-<id>.scope`. The apply reads the file (`found`, `max` or
  `max 100000` on a container started with no limit), writes the step's value, and reads it
  back, and a value read back other than the one written is `rc` 1; the revert writes `found`
  back. On a `memory.high` step the apply and the revert each read `memory.events`, and the
  revert records the difference in its `high` count (`high_events`), the times the kernel
  throttled the cgroup in the window. `docker update` is the wrong tool: `--memory` can't
  return to unlimited, and `--cpus` stays in the container's config and comes back on its next
  start. A start inside the window
  makes the scope again with the container's own settings, so the revert then writes nothing
  and records `started_in_window`; a container not running at the revert lost its scope with
  its process, and the revert writes nothing and records `exited_in_window`.
- A `cgroup` io step writes `<major>:<minor> wbps=<n>` and/or `wiops=<n>` into the scope's
  `io.max`, for the disk behind the step's `io_path` in the target (`io_device`): the mount
  holding that path in the process's `/proc/<pid>/mountinfo`, and for an overlay, the host
  mount holding its `upperdir` in `/proc/1/mountinfo`; a partition's whole disk from
  `/sys/dev/block/<major>:<minor>/../dev`, since `io.max` takes no partition. A device with
  major 0 (btrfs, tmpfs, an overlay with no host mount found) has no block device, and the step
  fails before any write. `found` is the device's `io.max` line, `max` when it has none, and
  the revert writes each limited key's found value back, `max` included. The apply and the
  revert each read `io.stat`'s `wbytes` and `wios` for the device and `io.pressure`'s `some`
  total, and the revert records the window's differences (`io_wbytes`, `io_wios`,
  `io_stall_us`) and its length (`io_window_s`). `io.stat` has no count of throttled writes,
  so the stalled time is the engagement's observable.
- A `slowclient` runs slowclient.py's source in a detached `FILEOPS_IMAGE` container on the
  first network of the service it's `on`, so the step's `address` resolves as that service's
  peers resolve it, and records the client's `ready` line. Under `half_open` the apply then
  disconnects the client from that network, the `partition` mechanism on a container of its
  own: the client's sockets stay established with no interface under them, so the listener's
  end sees a peer that sends nothing and answers nothing, and no FIN or RST ever leaves. The
  revert stops the client with SIGTERM, records its `done` line, and removes it; its
  connections close with it, a half-open one silently. Compose doesn't remove a container it
  has no `config-hash` label for, so `--remove-orphans` leaves a crashed run's client; the
  driver's teardown force-removes the project's `<project>-slowclient-` containers before
  `down`.
- A `kill` is `docker kill -s KILL`: the process gets no shutdown signal, so it writes no final
  `internal` drain, no shutdown drops, and no `exiting` or `drain complete` line, and the container
  exits with `KILL_EXIT_CODE`. Its revert is a `docker start`, as a `stop`'s is. The container is
  never recreated, so its filesystem, a disk spool under `/tmp` included, survives the kill.
- A `kill` may land inside a `pause` of its container and end with it (a stalled kill). Docker
  signals a paused container and then thaws it, and the process dies on the pending SIGKILL
  before it runs again, so it never reads what its sockets hold. The pause's revert then finds
  the container exited or started again and unpauses nothing.
- A `respond` sets a stand-in's answer: its apply POSTs the step's cue to the stand-in's
  control port and its revert POSTs an empty cue, which restores the default. A refused or
  failed POST is the record's `rc` 1, so `timeline` FAILs it and the step gets no window.
- A rolling overlap's holder (the role `netns`) owns the network namespace its instances join,
  so a fault on it is a fault on every instance's network: netem on it shapes their egress, and
  a partition of it cuts their ingress too. Every instance is an ingress SUT, since the
  generators send to the address they share. With no overlap, a holder's namespace holds
  `logit` and an `idp`, a token endpoint the SUT's sink reaches on loopback: a fault on the
  holder is one on `logit`'s network, and a lifecycle fault on the `idp` affects egress only,
  since the sink holds its queue while it can't authenticate.
- A `signal` is `docker kill -s <signal>` through the same CLI path as a `kill`, and a `rename`
  is an `os.rename` on the host inside `<run>/shared`, which the containers mount. Neither can
  lose data on any leg, so every `affects_*` predicate is false, and neither revert does
  anything. A `rename` refuses to replace a file already at its `to`.
- A `copytruncate`, `gzip`, or `replace` runs `fileops.py`'s source in a one-shot
  `FILEOPS_IMAGE` container, as root and with no network, with `<run>/shared` mounted at
  `SHARED_MOUNT`: the writer's files belong to the `logit` user in its container, which the host
  user can't truncate. Each records what fileops.py printed, the lines it copied, compressed, or
  wrote, and for a `copytruncate` the lines written after the copy and before the truncate
  (`lost_lines`). Each opens no window and affects no leg: `ledger.wire` accounts for a
  `copytruncate`'s lines from its record instead.
- A `rotate`, a `trust`, and a `probe` run `certs.sh` (tools/soak/netem/certs.sh) in the netem
  image, as the host user. A `rotate` issues the certificate's next generation into `<run>/tls`,
  signed by its `ca`, CA 1 by default, and repoints its `live` symlink there in one rename;
  generations count from the driver's first issue, 1, and a failed issue still uses its number
  up. A `trust` writes the next trust bundle, its `cas` concatenated, making each CA it doesn't
  find, and repoints `trust/live` the same way; bundles count from the driver's first, CA 1
  alone. A `probe` joins the target's network namespace and handshakes with the listener on its
  port, presenting the probe's own client certificate from the newest CA in the bundle in
  force, so a listener that requires one completes the handshake. A rotate and a probe record
  the `serial`, `not_after`, and `issuer` its script reports, and a probe whether the served
  chain verified against the bundle in force for the certificate's first DNS name
  (`verified`); a trust records its bundle's CAs and their subjects. None can lose data, and no
  revert does anything.
- A `mark` and an `exit` name an event the SUT's own config causes, so neither apply makes a
  Docker call. An `exit`'s revert starts the container only when it has exited, and its record
  says whether it did (`started`): a revert that found the process still running starts no new
  life, and the checks then FAIL the exit that never happened.
"""

import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from . import fileops, slowclient
from .scenario import (BACKEND_ROLE, BROKER_ROLE, GENERATOR_ROLE, IDP_ROLE, NETNS_ROLE,
                       SHARED_MOUNT, SLOWCLIENT_INTERVAL_S, STANDIN_ROLE, STOP_TIMEOUT_S,
                       SUT_ROLE, SUT_SERVICE, TLS_MOUNT, io_bytes, memory_bytes,
                       slowclient_send)

NETEM_LIMIT = "100000"
# The timeline key recording, per `[[ledger.source]]` wire, whether a step can affect it.
INGRESS_KEYS = {"udp": "affects_udp_ingress", "tcp": "affects_tcp_ingress",
                "file": "affects_file_ingress", "http": "affects_http_ingress",
                "kafka": "affects_kafka_ingress", "cri": "affects_file_ingress"}
# 128 + SIGKILL: the exit code Docker records for a container a `kill` ended.
KILL_EXIT_CODE = 137
CUE_TIMEOUT_S = 10
# The netem image's certificate script (tools/soak/netem/certs.sh), and the `Context.state` key
# holding each certificate's last generation.
CERTS_ENTRYPOINT = "/certs.sh"
TLS_GEN_KEY = "tls_gen"
# The `Context.state` keys holding the last trust bundle's generation and its CAs, and the
# bundle the driver's first issue makes: CA 1 alone.
TLS_TRUST_GEN_KEY = "tls_trust_gen"
TLS_TRUST_KEY = "tls_trust"
TLS_FIRST_BUNDLE = [1]
# The image a file operation runs in: the stand-in's (compose.yaml), so a run pulls one Python
# image, and the source it runs there. The driver pulls the image before the schedule starts
# (`ensure_fileops_image`): a pull inside a step could hold the tick past the driver's stall
# bound, which ends the run. An operation's own timeout then covers a container start and a
# copy of the writer's file.
FILEOPS_IMAGE = "python:3.13.16-alpine3.24"
FILEOPS_SOURCE = Path(fileops.__file__).read_text()
FILEOPS_TIMEOUT_S = 60
FILEOPS_PULL_TIMEOUT_S = 600
# util-linux's `prlimit` in the netem image, and the PID it acts on in the target's namespace.
PRLIMIT_ENTRYPOINT = "/usr/bin/prlimit"
PRLIMIT_PID = "1"
# Where a `cgroup` one-shot mounts the host's cgroup v2 hierarchy, the period it writes into
# `cpu.max` (the kernel's default), and the script it runs: with a value, write it, then print
# the file.
CGROUP_HOST_ROOT = "/sys/fs/cgroup"
CGROUP_MOUNT = "/cg"
CPU_PERIOD_US = 100000
CGROUP_SCRIPT = 'if [ $# -eq 2 ]; then printf "%s\\n" "$2" > "$1" || exit 1; fi; cat "$1"'
# A `slowclient`'s source, run in `FILEOPS_IMAGE`; how long its apply waits for the client's
# `ready` line, polling its log every `SLOWCLIENT_POLL_S`; and how long its revert's
# `docker stop` waits on SIGTERM before it kills the client.
SLOWCLIENT_SOURCE = Path(slowclient.__file__).read_text()
# Where a `churn` client's apply reads its namespace's `net.ipv4.tcp_migrate_req`.
MIGRATE_REQ_PATH = "/proc/sys/net/ipv4/tcp_migrate_req"
SLOWCLIENT_READY_S = 30.0
SLOWCLIENT_POLL_S = 0.25
SLOWCLIENT_STOP_S = 5
# The compose labels a client container carries. They put it in the project's network and
# `ps` filter, but `compose down --remove-orphans` doesn't remove it: Compose skips a container
# with no `config-hash` label, so the driver's teardown removes it itself.
COMPOSE_PROJECT_LABEL = "com.docker.compose.project"
COMPOSE_SERVICE_LABEL = "com.docker.compose.service"
# Where an io step reads the host's mounts, and the key order of an `io.max` line.
SYS_ROOT = "/sys"
HOST_MOUNTINFO_PID = "1"
IO_MAX_KEYS = (("io_wbps", "wbps"), ("io_wiops", "wiops"))


@dataclass
class Context:
    """What an action needs from the driver: the Docker handle, each service's container id,
    the netem image, each stand-in's control URL, and per-step state an apply leaves for its
    revert."""

    docker: object
    ids: dict
    netem_image: str
    state: dict = field(default_factory=dict)
    standin_base: dict = field(default_factory=dict)
    # `<run>/shared` on the host, for a `rename`; None when the run has none.
    shared_dir: object = None
    # `<run>/tls` on the host, the `[tls]` certificates (`{name: {san, usage}}`), and the
    # `uid:gid` certs.sh runs as, for a `rotate` and a `probe`; None when the run has no [tls].
    tls_dir: object = None
    tls_certs: dict = field(default_factory=dict)
    user: object = None
    # Where a `cgroup` reads a process's `/proc/<pid>/cgroup` and `mountinfo` (the host's
    # procfs), and a block device's `/sys/dev/block` entry; and the clock an io step's window
    # is measured on.
    proc_root: str = "/proc"
    sys_root: str = SYS_ROOT
    clock: object = time.monotonic


@dataclass
class Action:
    apply: object
    revert: object
    # Whether the fault can lose UDP between a generator and the SUT, which the ledger's wire
    # row judges only outside such windows. A fault on a generator affects only that generator's
    # own source; checks.py splits it per `[[ledger.source]]` by the step's `on`. Every source's
    # listener is on `SUT_SERVICE`, so a fault on a further SUT service affects none; under a
    # rolling overlap every instance holds them.
    affects_udp_ingress: object
    # Whether the fault can delay or fail a SUT sink's delivery to a backend or a broker.
    affects_egress: object
    # The same as `affects_udp_ingress` for a source whose `wire` is `tcp` or `file`
    # (`INGRESS_KEYS`). Netem on a TCP generator affects nothing: TCP retransmits what netem
    # drops, so the wire row judges that loss at 0. Netem on the SUT does affect it, because the
    # SUT's egress carries the ACKs. On `http` netem on either end can hold a request or its
    # answer past the client's timeout, which the client resends.
    affects_tcp_ingress: object
    affects_file_ingress: object
    affects_http_ingress: object
    # On `kafka` a fault on the generator, the broker, or the ingress SUT can delay records on
    # their way, and one on the SUT can replay them; the wire row judges the run against its
    # replay band and reports these windows.
    affects_kafka_ingress: object = None

    def affects(self, wire, step, roles):
        """Whether the fault can lose lines on a `wire` source's way into the SUT."""
        predicate = getattr(self, INGRESS_KEYS[wire])
        return bool(predicate and predicate(step, roles))


def _record(result, **extra):
    record = {"rc": result.rc, "stderr": result.stderr.strip()[-2000:]}
    record.update(extra)
    return record


def netem_args(args):
    words = args.split()
    if "limit" not in words:
        words += ["limit", NETEM_LIMIT]
    return words


def _netem(ctx, step, *argv):
    container = ctx.ids[step.on]
    # `ss -K` opens a sock_diag netlink socket, which SELinux denies a container_t container.
    labels = ["--security-opt", "label=disable"] if argv[0] == "reset" else []
    return ctx.docker.run_oneshot(
        ["--network", f"container:{container}", "--cap-add", "NET_ADMIN", *labels,
         ctx.netem_image, *argv],
    )


def netem_apply(ctx, step):
    result = _netem(ctx, step, "set", *netem_args(step.args))
    return _record(result, netem_show=result.stdout.strip(), netem_args=netem_args(step.args))


def netem_revert(ctx, step):
    result = _netem(ctx, step, "clear")
    return _record(result, netem_show=result.stdout.strip())


def reset_apply(ctx, step):
    """Closes the target's established TCP connections, or those on `step.port` (netem.sh's
    `reset`), recording how many it closed (`killed`)."""
    result = _netem(ctx, step, "reset", *([str(step.port)] if step.port is not None else []))
    lines = result.stdout.strip().splitlines()
    try:
        killed = json.loads(lines[-1]).get("killed") if lines else None
    except (ValueError, AttributeError):
        killed = None
    record = _record(result, killed=killed, closed=lines[:-1][-20:])
    if result.rc == 0 and not isinstance(killed, int):
        record.update(rc=1, stderr="netem.sh reset printed no count")
    elif result.rc == 0 and killed == 0:
        where = f" on port {step.port}" if step.port is not None else ""
        record.update(rc=1, stderr=(f"closed no established TCP connection{where}; ss -K closes "
                                    "nothing on a kernel without CONFIG_INET_DIAG_DESTROY or "
                                    "under an LSM (SELinux) denial of the sock_diag socket"))
    return record


def prlimit_argv(ctx, step, *args):
    """`docker run --rm` arguments for one `prlimit` call on PID 1 in the target's PID
    namespace."""
    return ["--pid", f"container:{ctx.ids[step.on]}", "--cap-add", "SYS_RESOURCE",
            "--entrypoint", PRLIMIT_ENTRYPOINT, ctx.netem_image, "--pid", PRLIMIT_PID, *args]


def nofile_limits(result):
    """`[soft, hard]` from `prlimit --nofile --raw --noheadings --output SOFT,HARD`, each an int
    or `"unlimited"`, or None when the output isn't two such words."""
    words = result.stdout.split()
    if len(words) != 2:
        return None
    try:
        return [word if word == "unlimited" else int(word) for word in words]
    except ValueError:
        return None


def prlimit_apply(ctx, step):
    """Reads the target's open-files limit (`found`), sets it to `step.nofile`, soft and hard,
    and leaves `found` and the process's start time for the revert. A read that printed no
    limit is `rc` 1 and sets nothing."""
    started = _started_at(ctx, step.on)
    read = ctx.docker.run_oneshot(prlimit_argv(
        ctx, step, "--nofile", "--raw", "--noheadings", "--output", "SOFT,HARD"))
    found = nofile_limits(read)
    if read.rc != 0 or found is None:
        record = _record(read)
        record.update(rc=read.rc or 1, stderr=record["stderr"] or
                      f"prlimit printed no limit: {read.stdout.strip()[-200:]!r}")
        return record
    ctx.state[step.id] = {"found": found, "started_at": started}
    limit = f"--nofile={step.nofile}:{step.nofile}"
    result = ctx.docker.run_oneshot(prlimit_argv(ctx, step, limit))
    return _record(result, found=found, nofile=step.nofile)


def prlimit_revert(ctx, step):
    """Sets the limit the apply found back, unless the process ended or started again inside
    the window. A running container with a new `StartedAt` has the container's own limit
    (`started_in_window`); one that isn't running has no process to restore
    (`exited_in_window`, with its status). Either sets nothing. An apply that failed left
    nothing to restore."""
    saved = ctx.state.pop(step.id, None)
    if saved is None:
        return {"rc": 0, "stderr": "", "restored": False}
    state = (ctx.docker.inspect(ctx.ids[step.on]) or {}).get("State") or {}
    if state.get("Status") != "running":
        return {"rc": 0, "stderr": "", "restored": False, "exited_in_window": True,
                "status": state.get("Status")}
    if state.get("StartedAt") != saved["started_at"]:
        return {"rc": 0, "stderr": "", "restored": False, "started_in_window": True}
    soft, hard = saved["found"]
    result = ctx.docker.run_oneshot(prlimit_argv(ctx, step, f"--nofile={soft}:{hard}"))
    return _record(result, restored=result.rc == 0, nofile=saved["found"])


def cgroup_setting(step):
    """`(file, value)`: the cgroup file a `cgroup` step writes and what it writes there; an io
    step's value is its `io.max` keys, `{key: n}`, which `io_max_line` puts on a device."""
    if step.cpu is not None:
        return "cpu.max", f"{round(step.cpu * CPU_PERIOD_US)} {CPU_PERIOD_US}"
    if step.memory_high is not None:
        return "memory.high", str(memory_bytes(step.memory_high))
    values = {"io_wbps": io_bytes(step.io_wbps) if step.io_wbps is not None else None,
              "io_wiops": step.io_wiops}
    return "io.max", {key: str(values[name]) for name, key in IO_MAX_KEYS
                      if values[name] is not None}


def io_max_line(device, values):
    """The `io.max` line that sets `values` (`{key: n}`) on `device` and leaves its other keys
    as they are."""
    return f"{device} " + " ".join(f"{key}={values[key]}" for _, key in IO_MAX_KEYS
                                    if key in values)


def io_max_values(text, device):
    """`{key: value}` of `device`'s line in a read of `io.max`: every key at `max` when the
    file has no line for it, as the kernel prints none for a device with no limit."""
    for line in text.splitlines():
        words = line.split()
        if words and words[0] == device:
            return dict(word.split("=", 1) for word in words[1:] if "=" in word)
    return {"rbps": "max", "wbps": "max", "riops": "max", "wiops": "max"}


def _unescape_mount(text):
    """A `mountinfo` path with its octal escapes (`\\040` for a space) decoded."""
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), text)


def _mountinfo(text):
    """`[(mount_point, "major:minor", fstype, super_options)]` from a `mountinfo` read; the
    fields after the ` - ` separator are the filesystem type, source, and super options."""
    mounts = []
    for line in text.splitlines():
        head, sep, tail = line.partition(" - ")
        fields, rest = head.split(), tail.split()
        if not sep or len(fields) < 5 or not rest:
            continue
        mounts.append((_unescape_mount(fields[4]), fields[2], rest[0],
                       rest[2] if len(rest) > 2 else ""))
    return mounts


def _covering_mount(mounts, path):
    """The mount whose mount point is the longest whole-component prefix of `path`, the last
    of equals (a later mount over the same point hides the earlier), or None."""
    best = None
    for mount in mounts:
        point = mount[0].rstrip("/") or "/"
        if point == "/" or path == point or path.startswith(point + "/"):
            if best is None or len(point) >= len(best[0].rstrip("/") or "/"):
                best = mount
    return best


def io_device(ctx, pid, path):
    """`(device, detail, None)`: the whole disk behind `path` in process `pid`'s mount
    namespace as `major:minor`, and how it was found (`{mount, fstype, dev, partition}`); or
    `(None, detail, why)` when no block device backs it."""
    def read(where):
        try:
            return Path(where).read_text(), None
        except OSError as err:
            return None, f"can't read {where}: {err}"

    where = Path(ctx.proc_root, str(pid), "mountinfo")
    text, why = read(where)
    if text is None:
        return None, {}, why
    mount = _covering_mount(_mountinfo(text), path)
    if mount is None:
        return None, {}, f"{where} has no mount holding {path}"
    point, dev, fstype, options = mount
    detail = {"mount": point, "fstype": fstype, "dev": dev}
    if fstype == "overlay":
        upper = next((opt[len("upperdir="):] for opt in options.split(",")
                      if opt.startswith("upperdir=")), None)
        if upper is None:
            return None, detail, f"no block device backs {path}: its overlay has no upperdir"
        host = Path(ctx.proc_root, HOST_MOUNTINFO_PID, "mountinfo")
        text, why = read(host)
        if text is None:
            return None, detail, why
        under = _covering_mount(_mountinfo(text), _unescape_mount(upper))
        if under is None:
            return None, detail, f"{host} has no mount holding {upper}"
        dev, fstype = under[1], under[2]
        detail.update(upper=upper, upper_mount=under[0], upper_fstype=fstype, dev=dev)
    if dev.split(":")[0] == "0":
        return None, detail, (f"no block device backs {path}: it is on {fstype} with the "
                              f"anonymous device {dev}")
    block = Path(ctx.sys_root, "dev", "block", dev)
    if not block.exists():
        return None, detail, f"no block device backs {path}: {block} doesn't exist"
    if (block / "partition").exists():
        text, why = read(block / ".." / "dev")
        if text is None:
            return None, detail, why
        detail["partition"] = dev
        dev = text.strip()
    return dev, detail, None


def cgroup_dir(ctx, service):
    """`(path, started_at, pid, None)`: the container's cgroup relative to the hierarchy's root,
    from the cgroup v2 line of its process's `/proc/<pid>/cgroup` on the host, and the start
    time and PID of that process from the same inspect; or `(None, None, None, why)` when the
    container isn't running or the line doesn't name its full id."""
    info = ctx.docker.inspect(ctx.ids[service]) or {}
    state = info.get("State") or {}
    pid, started = state.get("Pid"), state.get("StartedAt")
    full = info.get("Id")
    if not pid or not full:
        return None, None, None, f"{service} has no running process to find its cgroup by"
    where = Path(ctx.proc_root, str(pid), "cgroup")
    try:
        text = where.read_text()
    except OSError as err:
        return None, None, None, f"can't read {where}: {err}"
    for line in text.splitlines():
        if line.startswith("0::/"):
            path = line[len("0::/"):].strip()
            if full in path.split("/")[-1] and ".." not in path.split("/"):
                return path, started, pid, None
    return None, None, None, f"{where} names no cgroup v2 path for container {full[:12]}"


def cgroup_argv(ctx, path, *value):
    """`docker run --rm` arguments that print the cgroup file at `path` under the mount, after
    writing `value` into it when one is given."""
    return ["-v", f"{CGROUP_HOST_ROOT}:{CGROUP_MOUNT}:rw", "--security-opt", "label=disable",
            "--entrypoint", "/bin/sh", ctx.netem_image, "-c", CGROUP_SCRIPT, "sh",
            f"{CGROUP_MOUNT}/{path}", *value]


def _cgroup_write(ctx, path, value, io_disk=None, **extra):
    """Writes `value` and reads it back; a read-back that differs is `rc` 1. On an `io.max`
    write (`io_disk` set) the read-back is the file's every line, so it compares the keys
    `value` sets on `io_disk`'s line, where a line the kernel dropped reads `max`."""
    result = ctx.docker.run_oneshot(cgroup_argv(ctx, path, value))
    record = _record(result, path=path, value=value, **extra)
    if io_disk is not None:
        wanted = dict(word.split("=", 1) for word in value.split()[1:])
        back = io_max_values(result.stdout, io_disk)
        differs = any(back.get(key) != want for key, want in wanted.items())
    else:
        differs = result.stdout.strip() != value
    if result.rc == 0 and differs:
        record.update(rc=1, read_back=result.stdout.strip()[-200:],
                      stderr=f"wrote {value!r} to {path} and read back "
                             f"{result.stdout.strip()[-200:]!r}")
    return record


def cgroup_apply(ctx, step):
    """Finds the target's cgroup, reads the step's file there (`found`), writes the step's value,
    and leaves the path, `found`, and the process's start time for the revert. A `memory.high`
    step also reads `memory.events`' `high` count first (`high_events_before`), so the revert
    can count the window's; an io step finds the device (`io_device`), puts its keys on that
    device's line, and reads `io_counts` first (`io_before`). A cgroup or device it can't find,
    or a read that printed nothing or no count, is `rc` 1 and writes nothing. A failed write leaves nothing for the revert, and a read-back
    that differs writes `found` back at once. A process that started again between the inspect
    that found the cgroup and the write has a new scope, which the write may have reached: the
    apply writes `found` back there and fails the step (`started_during_apply`)."""
    name, value = cgroup_setting(step)
    found_dir, started, pid, why = cgroup_dir(ctx, step.on)
    if found_dir is None:
        return {"rc": 1, "stderr": why, "file": name}
    path = f"{found_dir}/{name}"
    device, extra = None, {}
    if name == "io.max":
        device, detail, why = io_device(ctx, pid, step.io_path)
        extra["io_device"] = detail
        if device is None:
            return {"rc": 1, "stderr": why, "file": name, "path": path, **extra}
        extra["device"] = device
    read = ctx.docker.run_oneshot(cgroup_argv(ctx, path))
    found = read.stdout.strip()
    if name == "io.max" and read.rc == 0:
        values = io_max_values(read.stdout, device)
        found = next((line.strip() for line in read.stdout.splitlines()
                      if line.split()[:1] == [device]), "max")
        restore = io_max_line(device, {key: values.get(key, "max") for key in value})
        value = io_max_line(device, value)
    else:
        restore = found
    if read.rc != 0 or not found:
        record = _record(read, file=name, path=path, **extra)
        record.update(rc=read.rc or 1, stderr=record["stderr"] or f"{path} read empty")
        return record
    saved = {"path": path, "found": restore, "started_at": started, "device": device}
    if name == "memory.high":
        before, why = memory_high_events(ctx, found_dir)
        if before is None:
            return {"rc": 1, "stderr": why, "file": name, "path": path, "found": found}
        saved.update(events_dir=found_dir, high_before=before)
        extra["high_events_before"] = before
    if name == "io.max":
        before, why = io_counts(ctx, found_dir, device)
        if before is None:
            return {"rc": 1, "stderr": why, "file": name, "path": path, "found": found,
                    **extra}
        saved.update(io_dir=found_dir, io_before=before, io_started=ctx.clock())
        extra["io_before"] = before
    record = _cgroup_write(ctx, path, value, io_disk=device, file=name, found=found, **extra)
    if record["rc"] != 0:
        if record.get("read_back") is not None:
            ctx.docker.run_oneshot(cgroup_argv(ctx, path, restore))
        return record
    if _started_at(ctx, step.on) != started:
        back = ctx.docker.run_oneshot(cgroup_argv(ctx, path, restore))
        record.update(rc=1, started_during_apply=True, restored=back.rc == 0,
                      stderr="the process started again during the apply; wrote "
                             f"{restore!r} back to its new scope")
        return record
    ctx.state[step.id] = saved
    return record


def memory_high_events(ctx, found_dir):
    """`(count, None)`: the `high` line of the cgroup's `memory.events`, the times the kernel
    throttled it over `memory.high`; or `(None, why)` when the read failed or printed no such
    line."""
    path = f"{found_dir}/memory.events"
    read = ctx.docker.run_oneshot(cgroup_argv(ctx, path))
    if read.rc != 0:
        return None, f"{path}: rc {read.rc}: {read.stderr.strip()[-200:]}"
    for line in read.stdout.splitlines():
        words = line.split()
        if len(words) == 2 and words[0] == "high" and words[1].isdigit():
            return int(words[1]), None
    return None, f"{path} printed no high count: {read.stdout.strip()[-200:]!r}"


def io_counts(ctx, found_dir, device):
    """`({wbytes, wios, stall_us}, None)`: `device`'s `wbytes` and `wios` in the cgroup's
    `io.stat`, 0 when it has no line yet (the kernel prints one only once the cgroup has done
    I/O there), and `io.pressure`'s `some` total, the microseconds some task in the cgroup
    waited on I/O; or `(None, why)` when a read failed or printed no `some` total."""
    counts = {}
    stat = f"{found_dir}/io.stat"
    read = ctx.docker.run_oneshot(cgroup_argv(ctx, stat))
    if read.rc != 0:
        return None, f"{stat}: rc {read.rc}: {read.stderr.strip()[-200:]}"
    fields = {}
    for line in read.stdout.splitlines():
        words = line.split()
        if words and words[0] == device:
            fields = dict(word.split("=", 1) for word in words[1:] if "=" in word)
    for key in ("wbytes", "wios"):
        value = fields.get(key, "0")
        if not value.isdigit():
            return None, f"{stat} printed {key}={value!r} for {device}"
        counts[key] = int(value)
    pressure = f"{found_dir}/io.pressure"
    read = ctx.docker.run_oneshot(cgroup_argv(ctx, pressure))
    if read.rc != 0:
        return None, f"{pressure}: rc {read.rc}: {read.stderr.strip()[-200:]}"
    for line in read.stdout.splitlines():
        words = line.split()
        if words[:1] == ["some"]:
            total = next((word[len("total="):] for word in words
                          if word.startswith("total=")), "")
            if total.isdigit():
                counts["stall_us"] = int(total)
                return counts, None
    return None, f"{pressure} printed no some total: {read.stdout.strip()[-200:]!r}"


def cgroup_revert(ctx, step):
    """Writes what the apply found back, unless the process exited inside the window, whose
    scope went with it (`exited_in_window`), or started again there, whose new scope has the
    container's own settings (`started_in_window`): the record then says which and writes
    nothing. On a `memory.high` step it first reads `memory.events` again and records how many
    times the kernel throttled the cgroup in the window (`high_events`), or why it couldn't
    (`events_error`); on an io step it reads `io.stat` and `io.pressure` again and records the
    window's written bytes and writes and stalled time (`io_wbytes`, `io_wios`, `io_stall_us`)
    over its length (`io_window_s`), or why it couldn't (`io_error`). An apply that failed
    left nothing to restore."""
    saved = ctx.state.pop(step.id, None)
    if saved is None:
        return {"rc": 0, "stderr": "", "restored": False}
    state = (ctx.docker.inspect(ctx.ids[step.on]) or {}).get("State") or {}
    if state.get("Status") != "running":
        return {"rc": 0, "stderr": "", "restored": False, "exited_in_window": True,
                "status": state.get("Status")}
    if state.get("StartedAt") != saved["started_at"]:
        return {"rc": 0, "stderr": "", "restored": False, "started_in_window": True,
                "status": state.get("Status")}
    events = {}
    if "events_dir" in saved:
        after, why = memory_high_events(ctx, saved["events_dir"])
        events = ({"events_error": why} if after is None
                  else {"high_events": after - saved["high_before"]})
    if "io_dir" in saved:
        after, why = io_counts(ctx, saved["io_dir"], saved["device"])
        before = saved["io_before"]
        events = ({"io_error": why} if after is None else {
            "io_wbytes": after["wbytes"] - before["wbytes"],
            "io_wios": after["wios"] - before["wios"],
            "io_stall_us": after["stall_us"] - before["stall_us"],
            "io_window_s": round(ctx.clock() - saved["io_started"], 3)})
        events["device"] = saved["device"]
    record = _cgroup_write(ctx, saved["path"], saved["found"], io_disk=saved.get("device"),
                           **events)
    record["restored"] = record["rc"] == 0
    return record


def _started_at(ctx, service):
    return ((ctx.docker.inspect(ctx.ids[service]) or {}).get("State") or {}).get("StartedAt")


def pause_apply(ctx, step):
    ctx.state[step.id] = _started_at(ctx, step.on)
    return _record(ctx.docker.run(["pause", ctx.ids[step.on]]))


def pause_revert(ctx, step):
    """Unpauses the container, unless a stalled kill inside the pause ended it
    (scenario.py's `stalled_kills()`): a container no longer paused that has exited or started
    again since the pause's apply has nothing to unpause, and the record says so (`unpaused`
    false). Any other container that isn't paused fails the unpause."""
    paused_at = ctx.state.pop(step.id, None)
    state = (ctx.docker.inspect(ctx.ids[step.on]) or {}).get("State") or {}
    if state and not state.get("Paused") and (
            state.get("Status") == "exited" or state.get("StartedAt") != paused_at):
        return {"rc": 0, "stderr": "", "unpaused": False, "status": state.get("Status")}
    return _record(ctx.docker.run(["unpause", ctx.ids[step.on]]))


def stop_apply(ctx, step):
    return _record(ctx.docker.run(
        ["stop", "-t", str(STOP_TIMEOUT_S), ctx.ids[step.on]], timeout=STOP_TIMEOUT_S + 15,
    ))


def stop_revert(ctx, step):
    return _record(ctx.docker.run(["start", ctx.ids[step.on]]))


def kill_apply(ctx, step):
    return _record(ctx.docker.run(["kill", "-s", "KILL", ctx.ids[step.on]]))


def signal_apply(ctx, step):
    return _record(ctx.docker.run(["kill", "-s", step.signal, ctx.ids[step.on]]),
                   signal=step.signal)


def rename_apply(ctx, step):
    if ctx.shared_dir is None:
        return {"rc": 1, "stderr": "the run has no shared directory"}
    source = os.path.join(ctx.shared_dir, step.path)
    target = os.path.join(ctx.shared_dir, step.to)
    if os.path.lexists(target):
        return {"rc": 1, "stderr": f"{step.to} already exists in the shared directory"}
    try:
        os.rename(source, target)
    except OSError as err:
        return {"rc": 1, "stderr": str(err)}
    return {"rc": 0, "stderr": "", "path": step.path, "to": step.to}


def ensure_fileops_image(docker):
    """`None` once `FILEOPS_IMAGE` is on the daemon, pulling it if it isn't, else why not."""
    if docker.run(["image", "inspect", FILEOPS_IMAGE]).rc == 0:
        return None
    result = docker.run(["pull", FILEOPS_IMAGE], timeout=FILEOPS_PULL_TIMEOUT_S)
    if result.rc != 0:
        return f"pulling {FILEOPS_IMAGE} failed: {result.stderr.strip()[-500:]}"
    return None


def file_op_apply(ctx, step):
    """A `copytruncate`, `gzip`, or `replace` of `step.path` in `<run>/shared` (fileops.py),
    its record carrying what the operation printed. A failed operation, or one that printed no
    JSON object, is `rc` 1."""
    if ctx.shared_dir is None:
        return {"rc": 1, "stderr": "the run has no shared directory"}
    names = [step.path] + ([step.to] if step.to is not None else [])
    result = ctx.docker.run_oneshot(
        ["--network", "none", "--volume", f"{ctx.shared_dir}:{SHARED_MOUNT}:z", FILEOPS_IMAGE,
         "python", "-I", "-c", FILEOPS_SOURCE, step.action, SHARED_MOUNT, *names],
        timeout=FILEOPS_TIMEOUT_S,
    )
    record = _record(result, path=step.path, **({"to": step.to} if step.to is not None else {}))
    if result.rc == 0:
        try:
            done = json.loads(result.stdout.strip().splitlines()[-1])
        except (IndexError, ValueError):
            done = None
        if isinstance(done, dict):
            record.update(done)
        else:
            record.update(rc=1, stderr=f"no JSON object in {result.stdout.strip()[-200:]!r}")
    return record


def slowclient_name(ctx, step):
    """The client container's name: the compose project's, then the step's id."""
    return f"{getattr(ctx.docker, 'project', None) or 'soak'}-slowclient-{step.id}"


def slowclient_argv(ctx, step, name, network):
    """`docker run -d` arguments for a step's client (slowclient.py) on `network`, labeled into
    the compose project."""
    host, port = step.address.rsplit(":", 1)
    project = getattr(ctx.docker, "project", None) or "soak"
    interval = step.interval if step.interval is not None else SLOWCLIENT_INTERVAL_S
    return ["run", "-d", "--name", name, "--label", f"{COMPOSE_PROJECT_LABEL}={project}",
            "--label", f"{COMPOSE_SERVICE_LABEL}={name}", "--network", network, FILEOPS_IMAGE,
            "python", "-I", "-u", "-c", SLOWCLIENT_SOURCE, step.mode, host, port,
            str(step.connections), f"{interval:g}", slowclient_send(step)]


def client_lines(text):
    """The JSON objects a client printed, in order; any other line is skipped."""
    found = []
    for line in text.splitlines():
        try:
            one = json.loads(line)
        except ValueError:
            continue
        if isinstance(one, dict):
            found.append(one)
    return found


def _client_event(result, event, last=False):
    lines = [one for one in client_lines(result.stdout) if one.get("event") == event]
    return (lines[-1] if last else lines[0]) if lines else None


def slowclient_apply(ctx, step):
    """Starts the step's client on the first network of the service it's `on`, or of the
    container whose namespace that service shares, waits up to
    `SLOWCLIENT_READY_S` for its `ready` line (`ready`), and under `half_open` disconnects it
    from that network. A client that printed no `ready` line, opened no connection, or, under
    `half_open`, left bytes unacknowledged, or a disconnect that failed, is `rc` 1, and the
    client is removed."""
    info = ctx.docker.inspect(ctx.ids[step.on]) or {}
    mode = (info.get("HostConfig") or {}).get("NetworkMode") or ""
    holder = mode.split(":", 1)[1] if mode.startswith("container:") else None
    if holder is not None:
        # An `[overlap]` instance has no network of its own: it's in its holder's namespace.
        info = ctx.docker.inspect(holder) or {}
    networks = sorted((info.get("NetworkSettings") or {}).get("Networks") or {})
    if not networks:
        return {"rc": 1, "stderr": f"{step.on} is on no network for a client to join"}
    name, network = slowclient_name(ctx, step), networks[0]
    started = ctx.docker.run(slowclient_argv(ctx, step, name, network))
    record = _record(started, container=name, network=network)
    if step.mode == "churn" and holder is not None:
        # The namespace's value in force, which `overlap.migrate` reads the resets against.
        read = ctx.docker.run(["exec", holder, "cat", MIGRATE_REQ_PATH])
        value = read.stdout.strip()
        record["migrate_req"] = int(value) if read.rc == 0 and value.isdigit() else None
    if started.rc != 0:
        ctx.docker.run(["rm", "-f", name])
        return record
    deadline = time.monotonic() + SLOWCLIENT_READY_S
    while True:
        logs = ctx.docker.run(["logs", name])
        ready = _client_event(logs, "ready")
        if ready is not None or time.monotonic() >= deadline:
            break
        time.sleep(SLOWCLIENT_POLL_S)
    record["ready"] = ready
    if ready is None:
        record.update(rc=1, stderr=f"the client printed no ready line in "
                                   f"{SLOWCLIENT_READY_S:g}s: {logs.stderr.strip()[-500:]}")
    elif not ready.get("dialed"):
        record.update(rc=1, stderr=f"the client opened no connection to {step.address}")
    elif step.mode == "half_open" and ready.get("unacked_bytes"):
        record.update(rc=1, stderr=f"{step.address} left {ready['unacked_bytes']} byte(s) "
                                   "unacknowledged before the disconnect")
    elif step.mode == "half_open":
        cut = ctx.docker.run(["network", "disconnect", network, name])
        record["disconnected"] = cut.rc == 0
        if cut.rc != 0:
            record.update(rc=cut.rc, stderr=cut.stderr.strip()[-2000:])
    if record["rc"] != 0:
        ctx.docker.run(["rm", "-f", name])
        return record
    ctx.state[step.id] = name
    return record


def slowclient_revert(ctx, step):
    """Stops the client with SIGTERM, records the counts its `done` line printed (`done`), and
    removes it. A client no longer running at the revert ended its connections early, and one
    that printed no `done` line can't say what it held: either is `rc` 1. An apply that failed
    left no client."""
    name = ctx.state.pop(step.id, None)
    if name is None:
        return {"rc": 0, "stderr": "", "stopped": False}
    state = (ctx.docker.inspect(name) or {}).get("State") or {}
    stop = ctx.docker.run(["stop", "-t", str(SLOWCLIENT_STOP_S), name],
                          timeout=SLOWCLIENT_STOP_S + 15)
    logs = ctx.docker.run(["logs", name])
    removed = ctx.docker.run(["rm", "-f", name])
    done = _client_event(logs, "done", last=True)
    record = {"rc": 0, "stderr": "", "container": name, "stopped": True, "done": done}
    if state.get("Status") != "running":
        record.update(rc=1, stderr=f"the client wasn't running at the revert (status "
                                   f"{state.get('Status')}, exit code {state.get('ExitCode')}), "
                                   f"so its connections ended early: "
                                   f"{logs.stderr.strip()[-500:]}")
    elif stop.rc != 0 or removed.rc != 0:
        failed = stop if stop.rc != 0 else removed
        record.update(rc=failed.rc, stderr=failed.stderr.strip()[-2000:])
    elif done is None:
        record.update(rc=1, stderr=f"the client printed no done line: "
                                   f"{logs.stderr.strip()[-500:]}")
    return record


def certs_argv(ctx, command, *args, network=None, read_only=False):
    """`docker run --rm` arguments for one `certs.sh` command in the netem image, as the host
    user so the files it writes in `<run>/tls` are the user's, with `<run>/tls` at
    `TLS_MOUNT`, inside `network`'s namespace when given."""
    argv = []
    if network is not None:
        argv += ["--network", f"container:{network}"]
    mode = "ro,z" if read_only else "z"
    argv += ["--user", ctx.user, "-v", f"{ctx.tls_dir}:{TLS_MOUNT}:{mode}",
             "--entrypoint", CERTS_ENTRYPOINT, ctx.netem_image, command, *args]
    return argv


def issue_args(name, gen, days, cert, broken=False, ca=None):
    """`certs.sh issue`'s arguments: the certificate's name, its generation, its days, its
    usage, and its DNS names, then `--broken` for a key that doesn't match it and `--ca` for a
    CA generation other than 1."""
    return ([name, str(gen), str(days), cert["usage"], *cert["san"]]
            + (["--broken"] if broken else [])
            + (["--ca", str(ca)] if ca not in (None, 1) else []))


def cert_report(result):
    """`{serial, not_after, verified?}` from the JSON line `certs.sh` prints last, or None."""
    lines = result.stdout.strip().splitlines()
    try:
        report = json.loads(lines[-1]) if lines else None
    except ValueError:
        return None
    return report if isinstance(report, dict) and "serial" in report else None


def _certs_record(result):
    """A timeline record for one `certs.sh` call: rc 1 when it printed no report."""
    record = _record(result)
    report = cert_report(result)
    if report is None:
        record["rc"] = record["rc"] or 1
        record["stderr"] = (record["stderr"] or "certs.sh printed no report")
        return record
    record.update(report)
    return record


def rotate_apply(ctx, step):
    if ctx.tls_dir is None or step.cert not in ctx.tls_certs:
        return {"rc": 1, "stderr": f"the run has no [tls] certificate {step.cert}"}
    # A failed issue can leave a partial directory, so a generation is never reused.
    gen = ctx.state.get(TLS_GEN_KEY, {}).get(step.cert, 1) + 1
    ctx.state.setdefault(TLS_GEN_KEY, {})[step.cert] = gen
    result = ctx.docker.run_oneshot(certs_argv(
        ctx, "issue", *issue_args(step.cert, gen, step.days, ctx.tls_certs[step.cert],
                                  bool(step.broken), step.ca)))
    record = _certs_record(result)
    record["gen"] = gen
    return record


def trust_apply(ctx, step):
    """Writes the next trust bundle, `step.cas`, and records it: `bundle`, its generation,
    and `cas` and `subjects` as certs.sh printed them. rc 1 when it printed no report, which
    leaves the bundle in force as it was for a later probe."""
    if ctx.tls_dir is None:
        return {"rc": 1, "stderr": "the run has no [tls] table"}
    gen = ctx.state.get(TLS_TRUST_GEN_KEY, 1) + 1
    ctx.state[TLS_TRUST_GEN_KEY] = gen
    result = ctx.docker.run_oneshot(certs_argv(ctx, "trust", str(gen),
                                               *(str(ca) for ca in step.cas)))
    record = _record(result)
    lines = result.stdout.strip().splitlines()
    try:
        report = json.loads(lines[-1]) if lines else None
    except ValueError:
        report = None
    if not isinstance(report, dict) or report.get("cas") != list(step.cas):
        record["rc"] = record["rc"] or 1
        record["stderr"] = record["stderr"] or f"certs.sh printed no bundle of {step.cas}"
        return record
    record.update(bundle=report.get("bundle"), cas=report["cas"],
                  subjects=report.get("subjects"))
    ctx.state[TLS_TRUST_KEY] = list(step.cas)
    return record


def probe_apply(ctx, step):
    if ctx.tls_dir is None or step.cert not in ctx.tls_certs:
        return {"rc": 1, "stderr": f"the run has no [tls] certificate {step.cert}"}
    server_name = ctx.tls_certs[step.cert]["san"][0]
    client = max(ctx.state.get(TLS_TRUST_KEY) or TLS_FIRST_BUNDLE)
    record = _certs_record(ctx.docker.run_oneshot(certs_argv(
        ctx, "probe", str(step.port), server_name, *([str(client)] if client != 1 else []),
        network=ctx.ids[step.on], read_only=True)))
    record["client_ca"] = client
    return record


def restart_apply(ctx, step):
    return _record(ctx.docker.run(
        ["restart", "-t", str(STOP_TIMEOUT_S), ctx.ids[step.on]], timeout=STOP_TIMEOUT_S + 15,
    ))


def restart_revert(ctx, step):
    # A restart is over once its apply returns; the revert only closes the fault window.
    return {"rc": 0, "stderr": ""}


def post_cue(base, cue, timeout=CUE_TIMEOUT_S):
    """POSTs `cue` to a stand-in's control URL `base`: `(rc, stderr)`, rc 0 on a 200."""
    if not base:
        return 1, "the stand-in has no published control port"
    request = urllib.request.Request(f"{base}/cue", data=json.dumps(cue).encode(),
                                     headers={"Content-Type": "application/json"},
                                     method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return (0, "") if response.status == 200 else (1, f"status {response.status}")
    except urllib.error.HTTPError as err:
        return 1, f"status {err.code}: {err.read().decode(errors='replace')[:500]}"
    except (urllib.error.URLError, OSError, ValueError) as err:
        return 1, str(err)


def respond_apply(ctx, step):
    rc, stderr = post_cue(ctx.standin_base.get(step.on), step.cue())
    return {"rc": rc, "stderr": stderr}


def respond_revert(ctx, step):
    rc, stderr = post_cue(ctx.standin_base.get(step.on), {})
    return {"rc": rc, "stderr": stderr}


def no_op(ctx, step):
    return {"rc": 0, "stderr": ""}


def exit_revert(ctx, step):
    info = ctx.docker.inspect(ctx.ids[step.on]) or {}
    status = (info.get("State") or {}).get("Status")
    if status != "exited":
        return {"rc": 0, "stderr": "", "started": False, "status": status}
    return _record(ctx.docker.run(["start", ctx.ids[step.on]]), started=True, status=status)


def _ips(info):
    """`{network: IP address}` from a container's `docker inspect`."""
    networks = ((info or {}).get("NetworkSettings") or {}).get("Networks") or {}
    return {name: net.get("IPAddress") or "" for name, net in networks.items()}


def partition_apply(ctx, step):
    container = ctx.ids[step.on]
    info = ctx.docker.inspect(container) or {}
    networks = (info.get("NetworkSettings") or {}).get("Networks") or {}
    saved = {name: list(net.get("Aliases") or []) for name, net in networks.items()}
    ctx.state[step.id] = saved
    ips = _ips(info)
    rcs, errors = [], []
    for name in saved:
        result = ctx.docker.run(["network", "disconnect", name, container])
        rcs.append(result.rc)
        if result.stderr.strip():
            errors.append(result.stderr.strip())
    rc = 0 if saved and all(rc == 0 for rc in rcs) else (max(rcs) if rcs else 1)
    stderr = "\n".join(errors) if saved else "no networks to disconnect"
    return {"rc": rc, "stderr": stderr, "networks": saved, "ips": ips}


def partition_revert(ctx, step):
    container = ctx.ids[step.on]
    saved = ctx.state.pop(step.id, {})
    rcs, errors = [], []
    for name, aliases in saved.items():
        argv = ["network", "connect"]
        for alias in aliases:
            argv += ["--alias", alias]
        result = ctx.docker.run(argv + [name, container])
        rcs.append(result.rc)
        if result.stderr.strip():
            errors.append(result.stderr.strip())
    rc = 0 if saved and all(rc == 0 for rc in rcs) else (max(rcs) if rcs else 1)
    return {"rc": rc, "stderr": "\n".join(errors), "networks": saved,
            "ips_after": _ips(ctx.docker.inspect(container))}


def _on_sut(step, roles):
    return roles.get(step.on) == SUT_ROLE


def _on_holder(step, roles):
    return roles.get(step.on) == NETNS_ROLE


def _on_ingress_sut(step, roles):
    """On a SUT service holding every source's listener: `logit`, or under a rolling overlap
    any instance. A stack with a `netns` holder runs an overlap's instances or `logit` alone
    beside an `idp`, so every SUT in it holds the listeners."""
    return _on_sut(step, roles) and (step.on == SUT_SERVICE or NETNS_ROLE in roles.values())


def _on_ingress_side(step, roles):
    """On an ingress SUT or the holder of its namespace."""
    return _on_ingress_sut(step, roles) or _on_holder(step, roles)


def _on_generator(step, roles):
    return roles.get(step.on) == GENERATOR_ROLE


def _on_sut_or_destination(step, roles):
    return roles.get(step.on) in (SUT_ROLE, BACKEND_ROLE, BROKER_ROLE, STANDIN_ROLE, NETNS_ROLE,
                                  IDP_ROLE)


def _lifecycle_on_ingress_sut_or_generator_partition(step, roles):
    return (_on_ingress_side(step, roles)
            or (_on_generator(step, roles) and step.action == "partition"))


def _on_ingress_sut_or_generator(step, roles):
    return _on_ingress_side(step, roles) or _on_generator(step, roles)


def _on_kafka_path(step, roles):
    """On an ingress SUT, its namespace's holder, a generator, or the broker between them."""
    return (_on_ingress_side(step, roles) or _on_generator(step, roles)
            or roles.get(step.on) == BROKER_ROLE)


def _always(step, roles):
    return True


def _never(step, roles):
    return False


ACTIONS = {
    "netem": Action(
        apply=netem_apply,
        revert=netem_revert,
        affects_udp_ingress=_on_generator,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_side,
        affects_file_ingress=_never,
        affects_http_ingress=_on_ingress_sut_or_generator,
        affects_kafka_ingress=_on_kafka_path,
    ),
    "pause": Action(
        apply=pause_apply,
        revert=pause_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut_or_generator,
        affects_kafka_ingress=_on_kafka_path,
    ),
    "stop": Action(
        apply=stop_apply,
        revert=stop_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut_or_generator,
        affects_kafka_ingress=_on_kafka_path,
    ),
    "kill": Action(
        apply=kill_apply,
        revert=stop_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut_or_generator,
        affects_kafka_ingress=_on_kafka_path,
    ),
    "restart": Action(
        apply=restart_apply,
        revert=restart_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut_or_generator,
        affects_kafka_ingress=_on_kafka_path,
    ),
    "partition": Action(
        apply=partition_apply,
        revert=partition_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_never,
        affects_http_ingress=_on_ingress_sut_or_generator,
        affects_kafka_ingress=_on_kafka_path,
    ),
    # A reset can lose what sat in either side's socket buffers on any TCP path through the
    # target: ingress to an ingress SUT or from a generator, a sink's egress, a Kafka fetch.
    "reset": Action(
        apply=reset_apply,
        revert=no_op,
        affects_udp_ingress=_never,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_never,
        affects_http_ingress=_on_ingress_sut_or_generator,
        affects_kafka_ingress=_on_kafka_path,
    ),
    # A limit on the SUT fails its accepts, opens, and dials once its descriptors reach it: a
    # TCP or HTTP sender's connection waits in the accept queue, a tailer's new file waits for
    # the next scan, and a sink's redial is retried. It loses nothing on a TCP or HTTP wire, so
    # those judge its window; a file, cri, or Kafka wire reports it like any window there and
    # judges the run against the replay band. It never touches a bound UDP socket.
    "prlimit": Action(
        apply=prlimit_apply,
        revert=prlimit_revert,
        affects_udp_ingress=_never,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut,
        affects_file_ingress=_on_ingress_sut,
        affects_http_ingress=_on_ingress_sut,
        affects_kafka_ingress=_on_ingress_sut,
    ),
    # A CPU quota or memory pressure on the SUT slows every task in it, as a pause stops them:
    # the receive queue and then the kernel drop UDP, a TCP or HTTP sender waits, and a sink's
    # deliveries fall behind.
    "cgroup": Action(
        apply=cgroup_apply,
        revert=cgroup_revert,
        affects_udp_ingress=_on_ingress_sut,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut,
        affects_file_ingress=_on_ingress_sut,
        affects_http_ingress=_on_ingress_sut,
        affects_kafka_ingress=_on_ingress_sut,
    ),
    # A slow client holds the SUT's connection slots and sends nothing a listener frames or
    # decodes, so it loses nothing on any leg; a generator's connection opened before the
    # slots fill goes on as before, and its window is judged.
    "slowclient": Action(
        apply=slowclient_apply,
        revert=slowclient_revert,
        affects_udp_ingress=_never,
        affects_egress=_never,
        affects_tcp_ingress=_never,
        affects_file_ingress=_never,
        affects_http_ingress=_never,
        affects_kafka_ingress=_never,
    ),
    # A mark on the SUT holds what it marks, such as a stalled script backing up into the UDP
    # receive queue.
    "mark": Action(
        apply=no_op,
        revert=no_op,
        affects_udp_ingress=_on_ingress_sut,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut,
        affects_file_ingress=_never,
        affects_http_ingress=_on_ingress_sut,
        affects_kafka_ingress=_on_ingress_sut,
    ),
    "exit": Action(
        apply=no_op,
        revert=exit_revert,
        affects_udp_ingress=_on_ingress_sut,
        affects_egress=_always,
        affects_tcp_ingress=_on_ingress_sut,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut,
        affects_kafka_ingress=_on_ingress_sut,
    ),
    "respond": Action(
        apply=respond_apply,
        revert=respond_revert,
        affects_udp_ingress=_never,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_never,
        affects_file_ingress=_never,
        affects_http_ingress=_never,
    ),
    "signal": Action(
        apply=signal_apply,
        revert=no_op,
        affects_udp_ingress=_never,
        affects_egress=_never,
        affects_tcp_ingress=_never,
        affects_file_ingress=_never,
        affects_http_ingress=_never,
    ),
    "rename": Action(
        apply=rename_apply,
        revert=no_op,
        affects_udp_ingress=_never,
        affects_egress=_never,
        affects_tcp_ingress=_never,
        affects_file_ingress=_never,
        affects_http_ingress=_never,
    ),
    **{name: Action(
        apply=file_op_apply,
        revert=no_op,
        affects_udp_ingress=_never,
        affects_egress=_never,
        affects_tcp_ingress=_never,
        affects_file_ingress=_never,
        affects_http_ingress=_never,
    ) for name in ("copytruncate", "gzip", "replace")},
    "rotate": Action(
        apply=rotate_apply,
        revert=no_op,
        affects_udp_ingress=_never,
        affects_egress=_never,
        affects_tcp_ingress=_never,
        affects_file_ingress=_never,
        affects_http_ingress=_never,
    ),
    "trust": Action(
        apply=trust_apply,
        revert=no_op,
        affects_udp_ingress=_never,
        affects_egress=_never,
        affects_tcp_ingress=_never,
        affects_file_ingress=_never,
        affects_http_ingress=_never,
    ),
    "probe": Action(
        apply=probe_apply,
        revert=no_op,
        affects_udp_ingress=_never,
        affects_egress=_never,
        affects_tcp_ingress=_never,
        affects_file_ingress=_never,
        affects_http_ingress=_never,
    ),
}

# Actions after whose apply or revert a `logit` service has started again and must report ready.
STARTS_ON_APPLY = ("restart",)
STARTS_ON_REVERT = ("stop", "kill", "exit")
RESUMES_ON_REVERT = ("pause",)
# Actions whose apply ends a container's process, each with the exit code Docker records for it.
# An `exit` step names its own code.
EXIT_CODES = {"stop": 0, "restart": 0, "kill": KILL_EXIT_CODE}
