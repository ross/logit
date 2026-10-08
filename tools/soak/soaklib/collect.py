"""Writes a run directory's evidence: provenance, the append-only JSON-lines records the driver
keeps during a run, and each service's logs and final `docker inspect` at the end.

Everything `check <run-dir>` reads is written here or by the driver through `Jsonl`, so a run
can be re-scored offline from its directory alone.
"""

import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path


class Jsonl:
    """An append-only JSON-lines file, flushed per record so a killed driver leaves every record
    it wrote."""

    def __init__(self, path):
        self.path = Path(path)
        self.handle = open(self.path, "a")

    def write(self, record):
        self.handle.write(json.dumps(record, sort_keys=True) + "\n")
        self.handle.flush()

    def close(self):
        self.handle.close()


def read_jsonl(path):
    records = []
    try:
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except ValueError:
                        continue
    except OSError:
        pass
    return records


def _command(argv, timeout=20):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=timeout,
                              stdin=subprocess.DEVNULL).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


def _read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return ""


def _meminfo_total():
    for line in _read("/proc/meminfo").splitlines():
        if line.startswith("MemTotal:"):
            return line.split(":", 1)[1].strip()
    return "unknown"


def provenance(run_dir, root, docker, images, scenario_name, duration, seed, argv):
    """Writes `provenance.txt`: what ran, on what, from which tree."""
    sha = _command(["git", "-C", str(root), "rev-parse", "HEAD"]) or "unknown"
    dirty = _command(["git", "-C", str(root), "status", "--porcelain"])
    dirty_count = len(dirty.splitlines()) if dirty else 0
    server = docker.run(["version", "--format", "{{.Server.Version}}"]).stdout.strip()
    lines = [
        "soak provenance",
        f"captured: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}",
        f"scenario: {scenario_name}",
        f"duration: {duration:g}s",
        f"seed: {seed if seed is not None else 'none'}",
        f"argv: {' '.join(argv)}",
        f"repo: {sha} ({dirty_count} modified path(s))",
        f"docker: {server or 'unknown'}",
    ]
    for image in images:
        result = docker.run(["image", "inspect", "--format",
                             "{{.Id}} {{join .RepoDigests \",\"}}", image])
        lines.append(f"image {image}: {result.stdout.strip() or 'not present'}")
    lines += [
        f"uname: {' '.join(platform.uname())}",
        f"nproc: {os.cpu_count()}",
        f"MemTotal: {_meminfo_total()}",
        f"net.core.rmem_max: {_read('/proc/sys/net/core/rmem_max') or 'unknown'}",
        f"python: {sys.version.split()[0]}",
    ]
    (Path(run_dir) / "provenance.txt").write_text("\n".join(lines) + "\n")


def service_logs(run_dir, docker, ids):
    """`logs/<svc>.stdout` and `.stderr`, kept apart, for every service with a container."""
    logs = Path(run_dir) / "logs"
    logs.mkdir(exist_ok=True)
    results = {}
    for service, container in ids.items():
        result = docker.logs_to(container, logs / f"{service}.stdout", logs / f"{service}.stderr")
        results[service] = result.rc
    return results


def service_inspect(run_dir, docker, ids):
    """`inspect/<svc>.json`: each container's final `docker inspect`."""
    out = Path(run_dir) / "inspect"
    out.mkdir(exist_ok=True)
    for service, container in ids.items():
        info = docker.inspect(container)
        (out / f"{service}.json").write_text(json.dumps(info, indent=2, sort_keys=True) + "\n")
