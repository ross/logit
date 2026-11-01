"""Holds connections open against an nginx-family server so a `stub_status` page reads nonzero
`Reading` and `Waiting` gauges: three idle keepalive connections that never send a byte (nginx
counts each as Waiting) and one that sends half a request line and stops (Reading). Prints
`ready` once every connection is open, then holds them until killed.

Usage: python3 connections.py HOST PORT
"""

import socket
import sys
import time

host, port = sys.argv[1], int(sys.argv[2])
held = []
for _ in range(60):
    try:
        held = [socket.create_connection((host, port), timeout=5) for _ in range(4)]
        break
    except OSError:
        time.sleep(1)
else:
    sys.exit("could not connect")
held[3].sendall(b"GET /nginx_status HTTP/1.1\r\nHost: x\r\n")
print("ready", flush=True)
while True:
    time.sleep(60)
