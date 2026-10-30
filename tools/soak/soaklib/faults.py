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
- A `kill` is `docker kill -s KILL`: the process gets no shutdown signal, so it writes no final
  `internal` drain, no shutdown drops, and no `exiting` or `drain complete` line, and the container
  exits with `KILL_EXIT_CODE`. Its revert is a `docker start`, as a `stop`'s is. The container is
  never recreated, so its filesystem, a disk spool under `/tmp` included, survives the kill.
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
- A `rotate` and a `probe` run `certs.sh` (tools/soak/netem/certs.sh) in the netem image, as
  the host user. A `rotate` issues the certificate's next generation into `<run>/tls` and
  repoints its `live` symlink there in one rename; generations count from the driver's first
  issue, 1, and a failed issue still uses its number up. A `probe` joins the target's network
  namespace and handshakes with the listener on its port, presenting the probe's own client
  certificate, so a listener that requires one completes the handshake. Each records the
  `serial` and `not_after` its script reports, and a probe whether the served chain verified
  against the CA for the certificate's first DNS name (`verified`). Neither can lose data, and
  neither revert does anything.
- A `mark` and an `exit` name an event the SUT's own config causes, so neither apply makes a
  Docker call. An `exit`'s revert starts the container only when it has exited, and its record
  says whether it did (`started`): a revert that found the process still running starts no new
  life, and the checks then FAIL the exit that never happened.
"""

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from . import fileops
from .scenario import (BACKEND_ROLE, BROKER_ROLE, GENERATOR_ROLE, IDP_ROLE, NETNS_ROLE,
                       SHARED_MOUNT, STANDIN_ROLE, STOP_TIMEOUT_S, SUT_ROLE, SUT_SERVICE,
                       TLS_MOUNT)

NETEM_LIMIT = "100000"
# The timeline key recording, per `[[ledger.source]]` wire, whether a step can affect it.
INGRESS_KEYS = {"udp": "affects_udp_ingress", "tcp": "affects_tcp_ingress",
                "file": "affects_file_ingress", "http": "affects_http_ingress",
                "cri": "affects_file_ingress"}
# 128 + SIGKILL: the exit code Docker records for a container a `kill` ended.
KILL_EXIT_CODE = 137
CUE_TIMEOUT_S = 10
# The netem image's certificate script (tools/soak/netem/certs.sh), and the `Context.state` key
# holding each certificate's last generation.
CERTS_ENTRYPOINT = "/certs.sh"
TLS_GEN_KEY = "tls_gen"
# The image a file operation runs in: the stand-in's (compose.yaml), so a run pulls one Python
# image, and the source it runs there. The driver pulls the image before the schedule starts
# (`ensure_fileops_image`): a pull inside a step could hold the tick past the driver's stall
# bound, which ends the run. An operation's own timeout then covers a container start and a
# copy of the writer's file.
FILEOPS_IMAGE = "python:3.13.16-alpine3.24"
FILEOPS_SOURCE = Path(fileops.__file__).read_text()
FILEOPS_TIMEOUT_S = 60
FILEOPS_PULL_TIMEOUT_S = 600


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

    def affects(self, wire, step, roles):
        """Whether the fault can lose lines on a `wire` source's way into the SUT."""
        return bool(getattr(self, INGRESS_KEYS[wire])(step, roles))


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
    return ctx.docker.run_oneshot(
        ["--network", f"container:{container}", "--cap-add", "NET_ADMIN", ctx.netem_image,
         *argv],
    )


def netem_apply(ctx, step):
    result = _netem(ctx, step, "set", *netem_args(step.args))
    return _record(result, netem_show=result.stdout.strip(), netem_args=netem_args(step.args))


def netem_revert(ctx, step):
    result = _netem(ctx, step, "clear")
    return _record(result, netem_show=result.stdout.strip())


def pause_apply(ctx, step):
    return _record(ctx.docker.run(["pause", ctx.ids[step.on]]))


def pause_revert(ctx, step):
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


def issue_args(name, gen, days, cert, broken=False):
    """`certs.sh issue`'s arguments: the certificate's name, its generation, its days, its
    usage, and its DNS names, then `--broken` for a key that doesn't match it."""
    return ([name, str(gen), str(days), cert["usage"], *cert["san"]]
            + (["--broken"] if broken else []))


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
                                  bool(step.broken))))
    record = _certs_record(result)
    record["gen"] = gen
    return record


def probe_apply(ctx, step):
    if ctx.tls_dir is None or step.cert not in ctx.tls_certs:
        return {"rc": 1, "stderr": f"the run has no [tls] certificate {step.cert}"}
    server_name = ctx.tls_certs[step.cert]["san"][0]
    return _certs_record(ctx.docker.run_oneshot(certs_argv(
        ctx, "probe", str(step.port), server_name, network=ctx.ids[step.on], read_only=True)))


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
    ),
    "pause": Action(
        apply=pause_apply,
        revert=pause_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut_or_generator,
    ),
    "stop": Action(
        apply=stop_apply,
        revert=stop_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut_or_generator,
    ),
    "kill": Action(
        apply=kill_apply,
        revert=stop_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut_or_generator,
    ),
    "restart": Action(
        apply=restart_apply,
        revert=restart_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut_or_generator,
    ),
    "partition": Action(
        apply=partition_apply,
        revert=partition_revert,
        affects_udp_ingress=_lifecycle_on_ingress_sut_or_generator_partition,
        affects_egress=_on_sut_or_destination,
        affects_tcp_ingress=_on_ingress_sut_or_generator,
        affects_file_ingress=_never,
        affects_http_ingress=_on_ingress_sut_or_generator,
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
    ),
    "exit": Action(
        apply=no_op,
        revert=exit_revert,
        affects_udp_ingress=_on_ingress_sut,
        affects_egress=_always,
        affects_tcp_ingress=_on_ingress_sut,
        affects_file_ingress=_on_ingress_side,
        affects_http_ingress=_on_ingress_sut,
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
