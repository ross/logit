"""The writer in `script/soak`'s `cri` file wire: one container's log in the CRI format
containerd writes, under a kubelet pod log tree, rotated, compressed, and restarted as the
kubelet does it. Stdlib only, so it runs in a stock Python image.

Usage: `python -u criwriter.py CONFIG`, where CONFIG is a JSON object (`validate()` lists its
keys). It writes `<root>/<namespace>_<pod>_<uid>/<container>/<restart>.log` until SIGTERM.

What it writes, per containerd and the kubelet (`testdata/interop/cri/README.md` records both):

- Records are `<RFC 3339 time> <stream> <tag> <content>\\n`, the time in containerd's form
  (`Z`, trailing zeros of the fraction trimmed), stdout and stderr interleaved.
- A line longer than 16 KiB leaves as 16,384-byte `P` pieces and a closing `F`, with a whole
  stderr line between two pieces, as containerd interleaves the streams.
- Every `dangle_every_s`, a stdout line's head leaves as a `P` and its `F` follows
  `dangle_s` later, while only stderr writes in between: a stall longer than the reader's
  `partial_timeout` splits the line into two events.
- Every `rotate_every_s`, the kubelet's rotation, in its order: remove the oldest rotated files
  past `max_files - 2`, gzip every uncompressed rotated file (to `.gz.tmp`, renamed to `.gz`,
  the original removed), rename the live file to `<n>.log.<YYYYMMDD-HHMMSS>`, then reopen. The
  runtime's write between the rename and the reopen is a stdout line's head as a `P` in the
  renamed file, and its `F` starts the new one, so the line is split at the rotation.
- After `restart_after_s`, the container restarts: the writer finishes its lines, closes
  `0.log`, and writes `1.log` from then on. Only the live attempt's log rotates.

At SIGTERM it finishes every open line with its `F`, reports, and exits 0.

It reports on stdout in the NDJSON grammar of `logit`'s `internal` telemetry
(`crates/logit-outputs/src/ndjson.rs`), which `soaklib/telemetry.py` reads, so the soak ledger
reads it as it reads a `logit` writer: every `interval_s`, `logit.process.uptime`, its RSS and
open descriptors, its input's `logit.component.events.sent` and `batches.sent`, and its sink's
`logit.component.events.delivered`, the logical lines it finished (a line counts once, at its
`F`, however many records it took): the ledger's G. Three counters of its own on the sink:
`soak.writer.rotations`, `soak.writer.lines.removed` (finished lines in the files rotation
removed, so the lines left on disk plus these equal G), and `soak.writer.lines.split` (lines
it split at a stall or a rotation).
"""

import gzip
import json
import os
import random
import shutil
import signal
import sys
import time

# containerd's split size: a longer line leaves as pieces of this many content bytes.
PIECE_BYTES = 16 * 1024
# The kubelet's rotated name suffix, `time.Format("20060102-150405")`.
ROTATED_FORMAT = "%Y%m%d-%H%M%S"

KEYS = {
    "root": str, "namespace": str, "pod": str, "uid": str, "container": str,
    "input": str, "sink": str, "rate": int, "batch": int, "interval_s": (int, float),
    "stderr_share": (int, float), "long_every": int, "long_bytes": list,
    "dangle_every_s": (int, float), "dangle_s": (int, float),
    "rotate_every_s": (int, float), "max_files": int, "restart_after_s": (int, float),
    "seed": int,
}


def validate(config):
    """Problems with a writer config: every key of `KEYS` with its type, and the bounds the
    writer relies on. `soaklib/scenario.py` checks a scenario's writer config with this."""
    if not isinstance(config, dict):
        return ["the writer config must be a JSON object"]
    problems = [f"missing `{key}`" for key in KEYS if key not in config]
    problems += [f"unknown key `{key}`" for key in config if key not in KEYS]
    for key, kind in KEYS.items():
        value = config.get(key)
        if key in config and (not isinstance(value, kind) or isinstance(value, bool)):
            problems.append(f"`{key}` has the wrong type: {value!r}")
    if problems:
        return problems
    if config["rate"] <= 0 or config["batch"] <= 0:
        problems.append("`rate` and `batch` must be positive")
    if config["max_files"] < 2:
        problems.append("`max_files` must be at least 2, as the kubelet requires")
    low_high = config["long_bytes"]
    if (len(low_high) != 2 or not all(isinstance(n, int) for n in low_high)
            or not PIECE_BYTES < low_high[0] <= low_high[1]):
        problems.append(f"`long_bytes` must be [low, high] with {PIECE_BYTES} < low <= high")
    if not 0 <= config["stderr_share"] < 1:
        problems.append("`stderr_share` must be in [0, 1)")
    if config["dangle_s"] >= config["dangle_every_s"]:
        problems.append("`dangle_s` must be below `dangle_every_s`")
    for key in ("interval_s", "dangle_every_s", "dangle_s", "rotate_every_s",
                "restart_after_s"):
        if config[key] <= 0:
            problems.append(f"`{key}` must be positive")
    return problems


def cri_time(ns):
    """containerd's record time: RFC 3339 UTC with the fraction's trailing zeros trimmed."""
    seconds, nanos = divmod(ns, 1_000_000_000)
    text = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds))
    fraction = f"{nanos:09d}".rstrip("0")
    return f"{text}.{fraction}Z" if fraction else f"{text}Z"


def stamp(ns):
    """The NDJSON grammar's timestamp: RFC 3339 UTC with nine fractional digits."""
    seconds, nanos = divmod(ns, 1_000_000_000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{nanos:09d}Z"


class Writer:
    def __init__(self, config):
        self.cfg = config
        self.rng = random.Random(config["seed"])
        self.dir = os.path.join(config["root"], "_".join(
            (config["namespace"], config["pod"], config["uid"])), config["container"])
        os.makedirs(self.dir, exist_ok=True)
        self.restart = 0
        self.fd = None
        # Finished lines per file, by its current name, so a removal counts what it took.
        self.lines_in = {}
        self.pending = []
        self.seq = 0
        # The stream whose line is held open as a `P`, and when its `F` is due.
        self.dangling = None
        self.started = time.monotonic()
        self.counts = {"delivered": 0, "sent": 0, "batches": 0, "rotations": 0,
                       "removed": 0, "split": 0}
        self.reported = dict(self.counts)
        self.open_live()

    # ---- files ---------------------------------------------------------------------------------

    def live(self):
        return os.path.join(self.dir, f"{self.restart}.log")

    def open_live(self):
        self.fd = os.open(self.live(), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666)
        self.lines_in.setdefault(self.live(), 0)
        self.current = self.live()

    def flush(self):
        """Writes the pending records to the open file in one write."""
        if self.pending:
            data = b"".join(self.pending)
            view = memoryview(data)
            while view:
                view = view[os.write(self.fd, view):]
            self.pending.clear()

    def record(self, stream, tag, content):
        line = f"{cri_time(time.time_ns())} {stream} {tag} ".encode() + content + b"\n"
        self.pending.append(line)
        if tag == "F":
            self.lines_in[self.current] = self.lines_in.get(self.current, 0) + 1
            self.counts["delivered"] += 1

    def rotated(self):
        """This log's rotated files, oldest first: the kubelet's names sort by time."""
        prefix = os.path.basename(self.live()) + "."
        return sorted(os.path.join(self.dir, name) for name in os.listdir(self.dir)
                      if name.startswith(prefix) and not name.endswith(".tmp"))

    def rotate(self):
        """The kubelet's `rotateLog`: remove the excess, compress, rename, reopen."""
        name = f"{self.live()}.{time.strftime(ROTATED_FORMAT)}"
        if os.path.exists(name) or os.path.exists(name + ".gz"):
            return  # a second rotation inside one second: the kubelet's next check retries
        self.flush()
        files = self.rotated()
        keep = self.cfg["max_files"] - 2
        for path in files[:max(0, len(files) - keep)]:
            os.remove(path)
            self.counts["removed"] += self.lines_in.pop(path, 0)
        for path in files[max(0, len(files) - keep):]:
            if path.endswith(".gz"):
                continue
            with open(path, "rb") as src, gzip.open(path + ".gz.tmp", "wb") as dst:
                shutil.copyfileobj(src, dst)
            os.rename(path + ".gz.tmp", path + ".gz")
            os.remove(path)
            self.lines_in[path + ".gz"] = self.lines_in.pop(path, 0)
        os.rename(self.live(), name)
        self.lines_in[name] = self.lines_in.pop(self.live(), 0)
        self.current = name
        split = self.dangling is None
        if split:
            # The runtime writes on through the old descriptor until the reopen.
            head = self.next_content()
            self.record("stdout", "P", head + b" head")
            self.flush()
        os.close(self.fd)
        self.open_live()
        if split:
            self.record("stdout", "F", b" rest")
            self.counts["split"] += 1
        self.counts["rotations"] += 1

    def restart_container(self):
        """A container restart: its lines finished, the next attempt writes `<n + 1>.log`."""
        self.finish_dangle()
        self.flush()
        os.close(self.fd)
        self.restart += 1
        self.open_live()

    # ---- lines ---------------------------------------------------------------------------------

    def next_content(self):
        self.seq += 1
        return f"soak line {self.seq}".encode()

    def stream(self):
        if self.dangling is not None:
            return "stderr"
        return "stderr" if self.rng.random() < self.cfg["stderr_share"] else "stdout"

    def line(self):
        """One logical line, a long one in pieces with a stderr line between two of them."""
        stream = self.stream()
        content = self.next_content()
        if self.cfg["long_every"] and self.seq % self.cfg["long_every"] == 0:
            low, high = self.cfg["long_bytes"]
            content += b" " + b"x" * (self.rng.randint(low, high) - len(content) - 1)
        pieces = [content[i:i + PIECE_BYTES] for i in range(0, len(content), PIECE_BYTES)]
        for index, piece in enumerate(pieces[:-1]):
            self.record(stream, "P", piece)
            if index == 0 and stream == "stdout":
                self.record("stderr", "F", self.next_content() + b" between pieces")
        self.record(stream, "F", pieces[-1])

    def start_dangle(self, now):
        self.record("stdout", "P", self.next_content() + b" dangling head")
        self.dangling = now + self.cfg["dangle_s"]

    def finish_dangle(self):
        if self.dangling is not None:
            self.record("stdout", "F", b" dangling rest")
            self.counts["split"] += 1
            self.dangling = None

    # ---- telemetry -----------------------------------------------------------------------------

    def report(self):
        ns = time.time_ns()
        ts = stamp(ns)
        uptime = time.monotonic() - self.started

        def emit(component, kind, role, metrics):
            if metrics:
                print(json.dumps({"timestamp": ts, "metrics": metrics, "attributes": {
                    "component": component, "kind": kind, "role": role}},
                    separators=(",", ":")), flush=True)

        def delta(name, key):
            value = self.counts[key] - self.reported[key]
            return ([{"name": name, "kind": "sum", "value": float(value),
                      "temporality": "delta", "monotonic": True}] if value else [])

        emit(self.cfg["input"], "cri_writer", "listener",
             delta("logit.component.events.sent", "sent")
             + delta("logit.component.batches.sent", "batches"))
        emit(self.cfg["sink"], "cri_writer", "sink",
             delta("logit.component.events.delivered", "delivered")
             + delta("soak.writer.rotations", "rotations")
             + delta("soak.writer.lines.removed", "removed")
             + delta("soak.writer.lines.split", "split"))
        try:
            with open("/proc/self/statm") as statm:
                rss = int(statm.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
            fds = len(os.listdir("/proc/self/fd"))
        except OSError:
            rss = fds = None
        process = [{"name": "logit.process.uptime", "kind": "gauge", "value": uptime}]
        if rss is not None:
            process += [{"name": "logit.process.memory.resident.bytes", "kind": "gauge",
                         "value": float(rss)},
                        {"name": "logit.process.fds", "kind": "gauge", "value": float(fds)}]
        emit("self", "internal", "listener", process)
        self.reported = dict(self.counts)

    # ---- loop ----------------------------------------------------------------------------------

    def run(self, stopping):
        cfg = self.cfg
        tick = cfg["batch"] / cfg["rate"]
        start = time.monotonic()
        next_tick = next_report = start
        next_rotate = start + cfg["rotate_every_s"]
        next_dangle = start + cfg["dangle_every_s"]
        restart_at = start + cfg["restart_after_s"]
        while not stopping():
            now = time.monotonic()
            if self.dangling is not None and now >= self.dangling:
                self.finish_dangle()
            if self.restart == 0 and now >= restart_at:
                self.restart_container()
            if now >= next_rotate:
                self.rotate()
                next_rotate += cfg["rotate_every_s"]
            if now >= next_dangle:
                if self.dangling is None:
                    self.start_dangle(now)
                next_dangle += cfg["dangle_every_s"]
            if now >= next_tick:
                before = self.counts["delivered"]
                for _ in range(cfg["batch"]):
                    self.line()
                self.counts["sent"] += self.counts["delivered"] - before
                self.counts["batches"] += 1
                next_tick += tick
            self.flush()
            if now >= next_report:
                self.report()
                next_report += cfg["interval_s"]
            time.sleep(max(0.0, min(next_tick, next_report) - time.monotonic()))
        self.finish_dangle()
        self.flush()
        os.close(self.fd)
        self.report()


def main(argv):
    if len(argv) != 2:
        print("usage: criwriter.py CONFIG", file=sys.stderr)
        return 2
    with open(argv[1]) as handle:
        config = json.load(handle)
    problems = validate(config)
    if problems:
        for problem in problems:
            print(f"criwriter: {problem}", file=sys.stderr)
        return 2
    stop = []
    # PID 1 in a container ignores SIGTERM without a handler, and `docker stop` sends it.
    signal.signal(signal.SIGTERM, lambda *_: stop.append(True))
    signal.signal(signal.SIGINT, lambda *_: stop.append(True))
    # The host user that removes a run directory isn't the writer's, so everything it creates
    # is writable by all, as `<run>/shared` itself is.
    os.umask(0)
    Writer(config).run(lambda: bool(stop))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
