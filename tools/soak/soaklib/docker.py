"""Runs `$DOCKER` commands for the driver, each with a timeout, never raising on failure.

`$DOCKER` (`sudo docker` by default, as in script/common.sh) is split with `shlex`, and `-n` is
inserted after `sudo` so an expired ticket fails the command instead of prompting on a terminal
the driver doesn't read. `script/soak` primes the ticket before the driver starts.

Every call returns a `Result` with `rc`, `stdout`, and `stderr`; a timeout is `rc` 124 with the
timeout named in `stderr`, as coreutils `timeout` reports it.
"""

import json
import os
import shlex
import subprocess
from dataclasses import dataclass

DEFAULT_TIMEOUT = 60.0


@dataclass
class Result:
    rc: int
    stdout: str
    stderr: str

    @property
    def ok(self):
        return self.rc == 0


def docker_argv(env=None):
    """The `$DOCKER` prefix as an argv list, with `sudo -n`."""
    argv = shlex.split((env or os.environ).get("DOCKER", "sudo docker"))
    if argv and os.path.basename(argv[0]) == "sudo" and "-n" not in argv[1:]:
        argv.insert(1, "-n")
    return argv


class Docker:
    """One compose project's view of the daemon. `project`, `compose_file`, and `env_file` are
    passed to every `compose` call."""

    def __init__(self, project, compose_file, env_file, prefix=None):
        self.prefix = prefix if prefix is not None else docker_argv()
        self.project = project
        self.compose_file = str(compose_file)
        self.env_file = str(env_file)

    def run(self, args, timeout=DEFAULT_TIMEOUT, stdout=None, stderr=None):
        """Runs `$DOCKER <args>`. With `stdout`/`stderr` file objects the output goes there
        instead of into the result."""
        argv = self.prefix + list(args)
        try:
            proc = subprocess.run(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=stdout if stdout is not None else subprocess.PIPE,
                stderr=stderr if stderr is not None else subprocess.PIPE,
                timeout=timeout,
                text=True,
            )
        except subprocess.TimeoutExpired:
            return Result(124, "", f"timed out after {timeout:g}s: {shlex.join(argv)}")
        except OSError as err:
            return Result(127, "", f"{shlex.join(argv)}: {err}")
        return Result(proc.returncode, proc.stdout or "", proc.stderr or "")

    def compose(self, *args, timeout=DEFAULT_TIMEOUT):
        return self.run(
            ["compose", "--progress", "quiet", "-p", self.project, "-f", self.compose_file,
             "--env-file", self.env_file, *args],
            timeout=timeout,
        )

    def container_id(self, service):
        """The service's container id, running or not, or None."""
        result = self.compose("ps", "-a", "-q", service)
        ids = result.stdout.split()
        return ids[0] if result.ok and ids else None

    def project_containers(self):
        result = self.compose("ps", "-a", "-q")
        return result.stdout.split() if result.ok else []

    def inspect(self, container):
        """`docker inspect` of one container as a dict, or None."""
        result = self.run(["inspect", container])
        if not result.ok:
            return None
        try:
            return json.loads(result.stdout)[0]
        except (ValueError, IndexError):
            return None

    def logs_to(self, container, stdout_path, stderr_path, timeout=300.0):
        """`docker logs` with the container's stdout and stderr kept apart. They span every life
        of a container that was stopped and started but never recreated."""
        with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
            return self.run(["logs", container], timeout=timeout, stdout=out, stderr=err)

    def exec_(self, container, argv, timeout=DEFAULT_TIMEOUT):
        return self.run(["exec", container, *argv], timeout=timeout)

    def run_oneshot(self, args, timeout=DEFAULT_TIMEOUT):
        """`docker run --rm <args>`."""
        return self.run(["run", "--rm", *args], timeout=timeout)

    def port(self, service, private_port):
        """The host `address:port` a published port maps to, or None."""
        result = self.compose("port", service, str(private_port))
        value = result.stdout.strip()
        return value if result.ok and value else None

    def stats(self, containers):
        """One `docker stats --no-stream` sample per container, as dicts."""
        if not containers:
            return []
        result = self.run(["stats", "--no-stream", "--no-trunc", "--format", "{{json .}}",
                           *containers])
        samples = []
        for line in result.stdout.splitlines():
            try:
                samples.append(json.loads(line))
            except ValueError:
                continue
        return samples
