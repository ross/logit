"""Writes a run directory's evidence: provenance, the append-only JSON-lines records the driver
keeps during a run, and each service's logs and final `docker inspect` at the end.

Everything `check <run-dir>` reads is written here or by the driver through `Jsonl`, so a run
can be re-scored offline from its directory alone.
"""

import json
import os
import platform
import shutil
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


def provenance(run_dir, root, docker, images, scenario_name, duration, seed, argv,
               seed_source=None):
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
        f"seed: {seed} (from {seed_source})" if seed is not None
        else "seed: none (a fixed schedule)",
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


# A chunk's `--until` trails the wall clock by this much, so a line the daemon timestamped
# before it has been written to the log file by the time the chunk reads it.
LOG_LAG_NS = 5 * 1_000_000_000
LOG_CHUNK_TIMEOUT_S = 60.0


def rfc3339_ns(ns):
    """Epoch nanoseconds -> RFC 3339 UTC with nine fractional digits, which `docker logs
    --since`/`--until` parse without rounding."""
    seconds, frac = divmod(int(ns), 1_000_000_000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{frac:09d}Z"


class LogCapture:
    """Appends each service's `docker logs` to `logs/<svc>.stdout` and `.stderr` in chunks
    during a run, so an hours-long run never reads a whole log at once and the daemon's
    rotated `json-file` logs (compose.yaml's `logging` options) never drop a line before it is
    captured.

    Boundaries, from the daemon's `json-file` reader (moby's `loggerutils` forwarder): `--since`
    skips lines timestamped before it only until the first line at or after it, then passes
    every later line in file order; `--until` stops at the first line timestamped after it. A
    chunk's next `--since` is its `--until` plus 1 ns, so the next chunk starts at the line the
    previous one stopped at, and consecutive chunks partition the file by position: no line is
    read twice or skipped, even where stdout's and stderr's timestamps interleave out of order.
    The one loss is a line written to the file more than `LOG_LAG_NS` after its timestamp,
    behind the next chunk's start; the daemon writes a line as it reads it from the container.

    One cursor per container id: a container stopped and started keeps its id and one log
    across lives. A chunk goes to temporary files and is appended only when `docker logs`
    returned 0, so a failed or timed-out chunk is retried whole from the same cursor next time.
    Each chunk is recorded in `log-chunks.jsonl`."""

    def __init__(self, run_dir, docker):
        self.logs = Path(run_dir) / "logs"
        self.logs.mkdir(exist_ok=True)
        self.docker = docker
        self.cursors = {}
        self.record = Jsonl(Path(run_dir) / "log-chunks.jsonl")

    def chunk(self, ids, final=False, now_ns=None, timeout=LOG_CHUNK_TIMEOUT_S):
        """One chunk per service in `ids`, up to `LOG_LAG_NS` before `now_ns`, or everything
        left when `final`. Stops the round at the first failed call and returns False, so a
        hung daemon costs one timeout per round."""
        now_ns = time.time_ns() if now_ns is None else now_ns
        for service, container in ids.items():
            since = self.cursors.get(container)
            until = None if final else now_ns - LOG_LAG_NS
            if until is not None and since is not None and until < since:
                continue
            out = self.logs / f".{service}.stdout.chunk"
            err = self.logs / f".{service}.stderr.chunk"
            started = time.time()
            result = self.docker.logs_to(
                container, out, err,
                since=None if since is None else rfc3339_ns(since),
                until=None if until is None else rfc3339_ns(until),
                timeout=timeout)
            entry = {"svc": service, "id": container, "since_ns": since, "until_ns": until,
                     "final": final, "rc": result.rc, "t": started,
                     "elapsed_s": round(time.time() - started, 3)}
            if result.ok:
                sizes = {}
                for chunk_path, stream in ((out, "stdout"), (err, "stderr")):
                    with open(chunk_path, "rb") as src, \
                            open(self.logs / f"{service}.{stream}", "ab") as dst:
                        shutil.copyfileobj(src, dst)
                    sizes[stream] = chunk_path.stat().st_size
                entry.update(stdout_bytes=sizes["stdout"], stderr_bytes=sizes["stderr"])
                if until is not None:
                    self.cursors[container] = until + 1
            else:
                # `logs_to` sends the CLI's stderr to `err`, so the reason is there.
                try:
                    said = err.read_text(errors="replace").strip()
                except OSError:
                    said = ""
                entry["stderr"] = (said or result.stderr.strip())[-500:]
            for chunk_path in (out, err):
                chunk_path.unlink(missing_ok=True)
            self.record.write(entry)
            if not result.ok:
                return False
        return True

    def close(self):
        self.record.close()


def service_logs(run_dir, docker, ids, capture=None):
    """`logs/<svc>.stdout` and `.stderr`, kept apart, for every service with a container: the
    tail after `capture`'s last chunk, or the whole log with no capture."""
    own = capture is None
    capture = capture or LogCapture(run_dir, docker)
    results = {}
    for service, container in ids.items():
        ok = capture.chunk({service: container}, final=True, timeout=300.0)
        results[service] = 0 if ok else 1
    if own:
        capture.close()
    return results


def service_inspect(run_dir, docker, ids):
    """`inspect/<svc>.json`: each container's final `docker inspect`."""
    out = Path(run_dir) / "inspect"
    out.mkdir(exist_ok=True)
    for service, container in ids.items():
        info = docker.inspect(container)
        (out / f"{service}.json").write_text(json.dumps(info, indent=2, sort_keys=True) + "\n")
