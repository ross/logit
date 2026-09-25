#!/usr/bin/env python3
"""Sends one of every DogStatsD construct from Datadog's `datadog` package, for
`script/record-fixtures datadog-*` to capture over UDP, a Unix datagram socket, or a Unix stream
socket.

    --url udp://capture:8125                        UDP, one datagram per call
    --url unix:///var/run/datadog/dsd.socket        a Unix datagram socket, one datagram per call
    --url unixstream:///var/run/datadog/dsd.socket  a Unix stream socket: one connection per mode,
                                                    `--modes unbuffered,buffered` for two

The nine calls are a counter, a gauge, a histogram, a distribution, a set, a timer, a gauge with
an explicit timestamp (`|T`), an event, and a service check, each tagged. Two of them pass a
per-call cardinality (`|card:`); the client's own `cardinality=` covers the rest. `DD_EXTERNAL_ENV`,
which `script/record-fixtures` sets, becomes `|e:` on every line, and the client adds `|c:` on its
own when it can see its container. So every line carries the optional segments whose order the
capture records.

Buffered mode packs the nine into as few packets as the client's buffer allows: under a stream
socket that is one length-prefixed frame holding several newline-separated lines.

Needs only the stdlib and `datadog`, which `script/record-fixtures` `pip install`s at record time.
The resolved version prints first, for `testdata/interop/datadog/README.md`'s provenance table.
"""

import argparse
import sys
import time

TAGS = ["env:record", "service:record-fixtures", "team:obs"]
#: A fixed wire timestamp for the `|T` gauge, so the capture carries a known value.
FIXED_TIMESTAMP = 1_790_000_000


def version_of(package):
    """The installed distribution version, printed for the provenance table."""
    from importlib.metadata import version

    return version(package)


def client_for(url, buffered):
    from datadog import DogStatsd

    common = {"disable_buffering": not buffered, "cardinality": "low"}
    if url.startswith("udp://"):
        host, _, port = url[len("udp://"):].rpartition(":")
        return DogStatsd(host=host, port=int(port), **common)
    # The client reads the scheme prefix off `socket_path` itself (`unix://`, `unixgram://`,
    # `unixstream://`) and picks the socket kind from it.
    return DogStatsd(socket_path=url, **common)


def workload(client):
    client.increment("record.requests.count", 3, tags=TAGS + ["endpoint:/checkout"])
    client.gauge("record.queue.depth", 17, tags=TAGS + ["queue:email"], cardinality="high")
    client.histogram("record.request.duration", 42.5, tags=TAGS)
    client.distribution("record.response.size", 1830, tags=TAGS)
    client.set("record.users.active", "user-4711", tags=TAGS)
    client.timing("record.db.query.duration", 7.25, tags=TAGS + ["db:primary"])
    client.gauge_with_timestamp("record.batch.lag", 2.5, FIXED_TIMESTAMP, tags=TAGS)
    client.event(
        "Deploy finished",
        "record-fixtures 1.2.3 is live",
        alert_type="success",
        aggregation_key="deploy-123",
        source_type_name="record",
        priority="low",
        tags=TAGS,
        hostname="record-host",
        cardinality="orchestrator",
    )
    client.service_check(
        "record.can_connect", 1, tags=TAGS, hostname="record-host", message="slow upstream"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--modes", default="unbuffered", help="comma-separated: unbuffered, buffered")
    args = parser.parse_args()

    print("datadog=={}".format(version_of("datadog")))
    for mode in args.modes.split(","):
        client = client_for(args.url, buffered=mode == "buffered")
        workload(client)
        client.flush()
        # A stream client's connection is the capture's file boundary, so close it before the next
        # mode opens its own.
        client.close_socket()
        print("sent 9 calls to {} ({})".format(args.url, mode))
        time.sleep(0.2)
    sys.stdout.flush()


if __name__ == "__main__":
    main()
