"""The client a `slowclient` step runs against a listener: connections that hold its connection
slots while sending nothing the ledger counts.

faults.py runs this file's source in a detached `python` container on the target's network, as
`python -I -u -c <source> <mode> <host> <port> <connections> <interval> <send>`, stops it with
SIGTERM at the step's revert, and reads the JSON lines it prints. It's stdlib only and imports
nothing from the harness for that reason.

- `silent` opens `connections` connections and sends nothing.
- `trickle` opens them and sends each one byte every `interval`.
- `half_open` opens them, sends each its first byte, and waits until the peer has acknowledged
  every byte (`TIOCOUTQ` reads 0), then holds them. The apply then disconnects the container
  from its networks, so from then on no packet passes either way and no FIN or RST is sent.

`send` is what goes on the wire, a byte at a time (`SENDS`): `lf` is LF bytes, which a stream
listener's LF framer skips with no frame counted; `http_head` is an HTTP/1.1 request head that
never ends; `http_body` is a whole head declaring a `BODY_BYTES` body to `HTTP_TARGET`,
`influxdb_in`'s write route, so the listener reads the body, sent at connect, then the body's
LF bytes, which never reach its length. `silent` sends nothing and takes `none`.

Under `silent` and `trickle` a connection the peer closes or that fails is counted and dialed
again at the next tick, `interval` later, as a client sent away comes back: a listener at its
connection cap closes each one past it at once. `half_open` dials once, since its network goes.

It prints `{"event": "ready", ...}` once its first round of dials is done, and
`{"event": "done", ...}` at SIGTERM, each with the counts `counts()` names.
"""

import fcntl
import json
import selectors
import signal
import socket
import struct
import sys
import termios
import time

MODES = ("silent", "trickle", "half_open")
SENDS = ("none", "lf", "http_head", "http_body")
HTTP_TARGET = "/api/v2/write?org=soak&bucket=soak"
BODY_BYTES = 1 << 20
DIAL_TIMEOUT_S = 2.0
ACK_TIMEOUT_S = 10.0


class Stop(Exception):
    """SIGTERM: print the counts and exit."""


def _stop(signum, frame):
    raise Stop()


def head(host, send):
    """The bytes a connection sends whole at connect: an `http_body` request's head."""
    if send != "http_body":
        return b""
    return (f"POST {HTTP_TARGET} HTTP/1.1\r\nHost: {host}\r\n"
            f"Content-Type: text/plain; charset=utf-8\r\nContent-Length: {BODY_BYTES}\r\n"
            "\r\n").encode()


def trickled(host, send):
    """The byte stream a connection trickles, one byte per tick: endless, and under
    `http_head` a request line and a header whose value never ends."""
    if send == "http_head":
        yield from f"POST {HTTP_TARGET} HTTP/1.1\r\nHost: {host}\r\nX-Soak-Trickle: ".encode()
        while True:
            yield ord("a")
    while True:
        yield ord("\n")


class Client:
    def __init__(self, mode, host, port, connections, interval, send):
        self.mode, self.host, self.port = mode, host, port
        self.connections, self.interval, self.send = connections, interval, send
        self.selector = selectors.DefaultSelector()
        self.open = {}
        self.dialed = self.dial_errors = self.peer_closed = 0
        self.sent_bytes = self.received_bytes = self.max_open = 0
        self.unacked = None

    def counts(self, event):
        record = {"event": event, "mode": self.mode, "open": len(self.open),
                  "max_open": self.max_open, "dialed": self.dialed,
                  "dial_errors": self.dial_errors, "peer_closed": self.peer_closed,
                  "sent_bytes": self.sent_bytes, "received_bytes": self.received_bytes}
        if self.unacked is not None:
            record["unacked_bytes"] = self.unacked
        return record

    def dial(self):
        try:
            sock = socket.create_connection((self.host, self.port), timeout=DIAL_TIMEOUT_S)
        except OSError:
            self.dial_errors += 1
            return
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.dialed += 1
        self.open[sock] = trickled(self.host, self.send)
        self.max_open = max(self.max_open, len(self.open))
        self.selector.register(sock, selectors.EVENT_READ)
        if not self.write(sock, head(self.host, self.send)):
            return
        if self.mode == "half_open" and self.send != "none":
            self.write(sock, bytes([next(self.open[sock])]))

    def write(self, sock, data):
        """Sends `data` whole, closing the connection when the peer has gone."""
        try:
            sock.settimeout(DIAL_TIMEOUT_S)
            sock.sendall(data)
        except OSError:
            self.drop(sock)
            return False
        self.sent_bytes += len(data)
        return True

    def drop(self, sock):
        if sock in self.open:
            del self.open[sock]
            self.selector.unregister(sock)
            sock.close()
            self.peer_closed += 1

    def fill(self):
        for _ in range(self.connections - len(self.open)):
            self.dial()

    def tick(self):
        if self.mode == "trickle":
            for sock in list(self.open):
                self.write(sock, bytes([next(self.open[sock])]))
        self.fill()

    def read(self, timeout):
        for key, _ in self.selector.select(timeout):
            sock = key.fileobj
            try:
                sock.settimeout(0)
                data = sock.recv(4096)
            except BlockingIOError:
                continue
            except OSError:
                data = b""
            if data:
                self.received_bytes += len(data)
            else:
                self.drop(sock)

    def wait_acked(self):
        """Waits up to `ACK_TIMEOUT_S` for the peer to acknowledge every byte sent, and records
        what's still unacknowledged."""
        deadline = time.monotonic() + ACK_TIMEOUT_S
        while True:
            self.unacked = sum(
                struct.unpack("i", fcntl.ioctl(sock.fileno(), termios.TIOCOUTQ, b"\0" * 4))[0]
                for sock in self.open)
            if self.unacked == 0 or time.monotonic() >= deadline:
                return
            time.sleep(0.05)

    def run(self):
        self.fill()
        if self.mode == "half_open":
            self.wait_acked()
            print(json.dumps(self.counts("ready")), flush=True)
            while True:
                time.sleep(3600)
        print(json.dumps(self.counts("ready")), flush=True)
        due = time.monotonic() + self.interval
        while True:
            self.read(max(0.0, due - time.monotonic()))
            if time.monotonic() >= due:
                due += self.interval
                self.tick()


def main(argv):
    """`<mode> <host> <port> <connections> <interval_s> <send>`: runs until SIGTERM, then prints
    the counts and returns 0; a bad argument prints usage and returns 2."""
    try:
        mode, host, port, connections, interval, send = argv
        port, connections, interval = int(port), int(connections), float(interval)
        valid = (mode in MODES and send in SENDS and (send == "none") == (mode == "silent")
                 and connections >= 1 and interval > 0)
    except ValueError:
        valid = False
    if not valid:
        print(f"usage: {'|'.join(MODES)} <host> <port> <connections> <interval_s> "
              f"{'|'.join(SENDS)}, got {argv}", file=sys.stderr)
        return 2
    client = Client(mode, host, port, connections, interval, send)
    signal.signal(signal.SIGTERM, _stop)
    try:
        client.run()
    except (Stop, KeyboardInterrupt):
        pass
    print(json.dumps(client.counts("done")), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
