"""Holds connections open against httpd so a `mod_status` page reads busy workers and async
connection states: three keepalive connections that finish one request and then idle (`K` on the
scoreboard, or `ConnsAsyncKeepAlive` under `event`), one that sends half a request line and
stops (`R`), and one that never sends a byte. With a TLS port, it first makes three TLS requests
there so the session cache holds entries. Prints `ready` once every connection is open, then holds
them until killed.

Under `--waves`, for the `event` MPM, the keepalive connections come in two waves of 12, three
seconds apart, and `ready` comes 2.5 seconds after the second. `event` refreshes the `Conns*`
counts only when one of its timeout queues expires an entry, so the first wave's expiry, at the
default 5 s `KeepAliveTimeout`, is what makes the page show the second wave.

Usage: python3 connections.py HOST PORT [TLS_PORT] [--waves]
"""

import socket
import ssl
import sys
import time

args = [a for a in sys.argv[1:] if a != "--waves"]
waves = "--waves" in sys.argv
host, port = args[0], int(args[1])
tls_port = int(args[2]) if len(args) > 2 else None
REQUEST = b"GET /server-status?auto HTTP/1.1\r\nHost: status-server\r\n\r\n"


def connect(p):
    for _ in range(60):
        try:
            return socket.create_connection((host, p), timeout=5)
        except OSError:
            time.sleep(1)
    sys.exit("could not connect")


def read_response(sock):
    """Reads one response whose head carries a Content-Length."""
    data = b""
    while b"\r\n\r\n" not in data:
        data += sock.recv(65536)
    head, body = data.split(b"\r\n\r\n", 1)
    length = next(
        int(line.split(b":", 1)[1])
        for line in head.split(b"\r\n")
        if line.lower().startswith(b"content-length:")
    )
    while len(body) < length:
        body += sock.recv(65536)


if tls_port is not None:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    for _ in range(3):
        with context.wrap_socket(connect(tls_port), server_hostname="status-server") as tls:
            tls.sendall(REQUEST.replace(b"\r\n\r\n", b"\r\nConnection: close\r\n\r\n"))
            while tls.recv(65536):
                pass



def keepalive(count):
    """`count` connections that each finish one request and stay open."""
    socks = []
    for _ in range(count):
        sock = connect(port)
        sock.sendall(REQUEST)
        read_response(sock)
        socks.append(sock)
    return socks


held = []
if waves:
    held += keepalive(12)
    time.sleep(3)
    held += keepalive(12)
else:
    held += keepalive(3)
half = connect(port)
half.sendall(b"GET /server-status HTTP/1.1\r\nHost: status-server\r\n")
held.append(half)
held.append(connect(port))
if waves:
    time.sleep(2.5)
print("ready", flush=True)
while True:
    time.sleep(60)
