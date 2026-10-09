"""The fault actions a scenario step names: how each is applied and reverted, and which legs of
the data path it affects.

`ACTIONS` maps an action name to an `Action`. `apply` and `revert` take the driver's `Context`
and the `scenario.Step`, and return a dict for `timeline.jsonl`: `rc`, `stderr`, and, for netem,
`netem_show` (the target's root qdisc after the change).

Facts a maintainer needs here (docs/plans/soak-harness.md, "Netem scope"):

- A netem change is a one-shot container in the target's network namespace. The qdisc outlives
  it, and a root qdisc on the default-route interface shapes the target's egress only.
- `limit 100000` is appended unless the step names a `limit`: headroom so netem's own queue
  never drops. A delay plus a post-outage burst can queue more than netem's default 1,000
  packets, and a drop there would read as a fault the step didn't ask for.
- `docker network connect` drops a container's aliases, so a partition records them before the
  disconnect and passes each back with `--alias`.
- A `kill` is `docker kill -s KILL`: the process gets no shutdown signal, so it writes no final
  `internal` drain, no shutdown drops, and no `exiting` or `drain complete` line, and the container
  exits with `KILL_EXIT_CODE`. Its revert is a `docker start`, as a `stop`'s is. The container is
  never recreated, so its filesystem, a disk spool under `/tmp` included, survives the kill.
"""

from dataclasses import dataclass, field

NETEM_LIMIT = "100000"
STOP_TIMEOUT_S = 30
# 128 + SIGKILL: the exit code Docker records for a container a `kill` ended.
KILL_EXIT_CODE = 137


@dataclass
class Context:
    """What an action needs from the driver: the Docker handle, each service's container id,
    the netem image, and per-step state an apply leaves for its revert."""

    docker: object
    ids: dict
    netem_image: str
    state: dict = field(default_factory=dict)


@dataclass
class Action:
    apply: object
    revert: object
    # Whether the fault can lose UDP between the generator and the SUT, which the ledger's wire
    # row judges only outside such windows.
    affects_udp_ingress: object
    # Whether the fault can delay or fail the SUT's remote-write.
    affects_egress: object


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


def restart_apply(ctx, step):
    return _record(ctx.docker.run(
        ["restart", "-t", str(STOP_TIMEOUT_S), ctx.ids[step.on]], timeout=STOP_TIMEOUT_S + 15,
    ))


def restart_revert(ctx, step):
    # A restart is over once its apply returns; the revert only closes the fault window.
    return {"rc": 0, "stderr": ""}


def partition_apply(ctx, step):
    container = ctx.ids[step.on]
    info = ctx.docker.inspect(container) or {}
    networks = (info.get("NetworkSettings") or {}).get("Networks") or {}
    saved = {name: list(net.get("Aliases") or []) for name, net in networks.items()}
    ctx.state[step.id] = saved
    rcs, errors = [], []
    for name in saved:
        result = ctx.docker.run(["network", "disconnect", name, container])
        rcs.append(result.rc)
        if result.stderr.strip():
            errors.append(result.stderr.strip())
    rc = 0 if saved and all(rc == 0 for rc in rcs) else (max(rcs) if rcs else 1)
    stderr = "\n".join(errors) if saved else "no networks to disconnect"
    return {"rc": rc, "stderr": stderr, "networks": saved}


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
    return {"rc": rc, "stderr": "\n".join(errors), "networks": saved}


def _lifecycle_on_sut_or_generator_partition(step):
    return step.on == "logit" or (step.on == "generator" and step.action == "partition")


ACTIONS = {
    "netem": Action(
        apply=netem_apply,
        revert=netem_revert,
        affects_udp_ingress=lambda step: step.on == "generator",
        affects_egress=lambda step: step.on in ("logit", "victoria-metrics"),
    ),
    "pause": Action(
        apply=pause_apply,
        revert=pause_revert,
        affects_udp_ingress=_lifecycle_on_sut_or_generator_partition,
        affects_egress=lambda step: step.on in ("logit", "victoria-metrics"),
    ),
    "stop": Action(
        apply=stop_apply,
        revert=stop_revert,
        affects_udp_ingress=_lifecycle_on_sut_or_generator_partition,
        affects_egress=lambda step: step.on in ("logit", "victoria-metrics"),
    ),
    "kill": Action(
        apply=kill_apply,
        revert=stop_revert,
        affects_udp_ingress=_lifecycle_on_sut_or_generator_partition,
        affects_egress=lambda step: step.on in ("logit", "victoria-metrics"),
    ),
    "restart": Action(
        apply=restart_apply,
        revert=restart_revert,
        affects_udp_ingress=_lifecycle_on_sut_or_generator_partition,
        affects_egress=lambda step: step.on in ("logit", "victoria-metrics"),
    ),
    "partition": Action(
        apply=partition_apply,
        revert=partition_revert,
        affects_udp_ingress=_lifecycle_on_sut_or_generator_partition,
        affects_egress=lambda step: step.on in ("logit", "victoria-metrics"),
    ),
}

# Actions after whose apply or revert a `logit` service has started again and must report ready.
STARTS_ON_APPLY = ("restart",)
STARTS_ON_REVERT = ("stop", "kill")
RESUMES_ON_REVERT = ("pause",)
# Actions whose apply ends a container's process, each with the exit code Docker records for it.
EXIT_CODES = {"stop": 0, "restart": 0, "kill": KILL_EXIT_CODE}
