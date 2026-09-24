#!/usr/bin/env python3
"""Emits an app-like statsd/DogStatsD workload from a real third-party client, for
`script/record-fixtures statsd` to capture through `raw_capture.py --proto udp`.

The capture records how a real client packs an application's metrics into datagrams; hand-written
pairs under `crates/logit-cli/tests/fixtures/statsd/` cover the line grammar:

    --client dogstatsd --mode unbuffered   `datadog`'s DogStatsd, one datagram per call
    --client dogstatsd --mode buffered     the same client with its own send buffer on
    --client statsd    --mode plain        `statsd`'s StatsClient, one datagram per call
    --client statsd    --mode pipeline     the same client through `client.pipeline()`

In the buffered modes each client decides where to cut a datagram, which is the distribution
`perf/load/README.md` measures the load model's `datagram_mix:` weights from. The unbuffered modes
are its other end: one metric per datagram, the syscall-bound worst case (ADR
`udp-intake-batching-and-socket-visibility`).

The workload is a small web service: request counters and latency timings per endpoint and status,
a response-size distribution, worker-queue gauges, an active-user set, and sampled counters. It
isn't one repeated metric, because name length, tag count, and value width are what a decoder pays
for. Values come from a seeded `random.Random`, so a re-record has the same shape but not the same
bytes.

Needs only the stdlib and the client under test, which `script/record-fixtures` `pip install`s at
record time. The resolved version prints first, for `testdata/interop/statsd/README.md`'s
provenance table.
"""

import argparse
import random
import sys

# A small, fixed service topology, sized like a real app's naming rather than to hit a byte target.
ENDPOINTS = [
    "/api/v1/users",
    "/api/v1/users/:id",
    "/api/v1/orders",
    "/api/v1/orders/:id/items",
    "/api/v1/checkout",
    "/healthz",
]
STATUSES = ["200", "200", "200", "200", "201", "400", "404", "500"]
QUEUES = ["email", "webhooks", "reindex"]
BASE_TAGS = ["env:prod", "service:checkout-api", "region:us-east-1", "host:web-07"]


def latency_ms(rng, mean, stddev):
    """A plausible latency: a normal draw floored just above zero.

    An unclamped `gauss(42, 18)` goes negative a few percent of the time, and a fixture of negative
    durations would record a bug rather than an application's traffic.
    """
    return max(0.05, rng.gauss(mean, stddev))


def version_of(package):
    """The installed distribution version, printed for the provenance table."""
    try:
        from importlib.metadata import version

        return version(package)
    except Exception as err:  # pragma: no cover - diagnostic path only
        return "unknown ({})".format(err)


def dogstatsd_client(host, port, buffered):
    """A real `datadog.DogStatsd`, with buffering explicitly on or off.

    The argument is `disable_buffering=` in 0.44+ and `max_buffer_size=` before it, so this tries
    the current one and falls back rather than pinning a version.
    """
    from datadog import DogStatsd

    try:
        return DogStatsd(host=host, port=port, disable_buffering=not buffered)
    except TypeError:
        # Pre-0.44: buffering is on iff a buffer size is configured.
        return DogStatsd(host=host, port=port, max_buffer_size=50 if buffered else 1)


def dogstatsd_workload(client, rng, rounds):
    """One "request" per round, plus the periodic gauges/set a real app reports alongside them."""
    for i in range(rounds):
        endpoint = rng.choice(ENDPOINTS)
        status = rng.choice(STATUSES)
        tags = BASE_TAGS + ["endpoint:{}".format(endpoint), "status:{}".format(status)]

        client.increment("app.http.requests.count", tags=tags)
        client.histogram("app.http.request.duration_ms", latency_ms(rng, 42.0, 18.0), tags=tags)
        client.distribution("app.http.response.size_bytes", rng.randint(180, 64000), tags=tags)
        # Two sampled counters at typical hot-path rates. Both clients sample client-side, skipping
        # the send at `1 - sample_rate`, so these reach the wire far less often than the lines
        # above; the capture measures that too.
        client.increment("app.cache.lookups.count", sample_rate=0.1, tags=BASE_TAGS)
        client.increment("app.render.calls.count", sample_rate=0.5, tags=tags)
        if i % 3 == 0:
            client.timing(
                "app.db.query.duration_ms",
                latency_ms(rng, 7.5, 3.0),
                tags=BASE_TAGS + ["query:select_user", "db:primary"],
            )
        if i % 5 == 0:
            for queue in QUEUES:
                client.gauge(
                    "app.worker.queue.depth",
                    rng.randint(0, 250),
                    tags=BASE_TAGS + ["queue:{}".format(queue)],
                )
            client.set("app.users.active", rng.randint(1, 5000), tags=BASE_TAGS)


def statsd_client(host, port):
    """A real `statsd.StatsClient` -- the plain-statsd dialect: no tags, cardinality in the name."""
    import statsd

    return statsd.StatsClient(host=host, port=port)


def statsd_workload(client, rng, rounds):
    """The same service, expressed the way a tagless client has to express it: in the metric name."""
    for i in range(rounds):
        endpoint = rng.choice(ENDPOINTS).strip("/").replace("/", ".").replace(":", "")
        status = rng.choice(STATUSES)
        prefix = "app.checkout_api.web_07.us_east_1"

        client.incr("{}.http.{}.{}.count".format(prefix, endpoint, status))
        client.timing("{}.http.{}.duration".format(prefix, endpoint), latency_ms(rng, 42.0, 18.0))
        client.incr("{}.cache.lookups".format(prefix), rate=0.1)
        client.incr("{}.render.calls".format(prefix), rate=0.5)
        if i % 3 == 0:
            client.timing("{}.db.select_user.duration".format(prefix), latency_ms(rng, 7.5, 3.0))
        if i % 5 == 0:
            for queue in QUEUES:
                client.gauge("{}.worker.{}.depth".format(prefix, queue), rng.randint(0, 250))
            client.set("{}.users.active".format(prefix), rng.randint(1, 5000))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="the capture container's hostname")
    parser.add_argument("--port", type=int, default=8125)
    parser.add_argument("--client", choices=["dogstatsd", "statsd"], required=True)
    parser.add_argument(
        "--mode",
        choices=["unbuffered", "buffered", "plain", "pipeline"],
        required=True,
        help="unbuffered/buffered for dogstatsd, plain/pipeline for statsd",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=40,
        help="simulated requests; each is several metrics, so a buffered mode needs far fewer "
        "rounds than the datagrams it produces",
    )
    parser.add_argument("--seed", type=int, default=20260918)
    args = parser.parse_args()

    rng = random.Random(args.seed)

    if args.client == "dogstatsd":
        if args.mode not in ("unbuffered", "buffered"):
            parser.error("--client dogstatsd takes --mode unbuffered|buffered")
        print("datadog=={}".format(version_of("datadog")))
        client = dogstatsd_client(args.host, args.port, buffered=args.mode == "buffered")
        if args.mode == "buffered":
            # The context manager is the documented way to batch: it flushes what's left on exit,
            # so nothing is lost when the workload ends mid-datagram.
            with client:
                dogstatsd_workload(client, rng, args.rounds)
        else:
            dogstatsd_workload(client, rng, args.rounds)
        # `flush` exists on the buffered paths across versions and is a no-op when unbuffered.
        getattr(client, "flush", lambda: None)()
    else:
        if args.mode not in ("plain", "pipeline"):
            parser.error("--client statsd takes --mode plain|pipeline")
        print("statsd=={}".format(version_of("statsd")))
        client = statsd_client(args.host, args.port)
        if args.mode == "pipeline":
            with client.pipeline() as pipe:
                statsd_workload(pipe, rng, args.rounds)
        else:
            statsd_workload(client, rng, args.rounds)

    print("sent {} rounds as {}/{}".format(args.rounds, args.client, args.mode))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
