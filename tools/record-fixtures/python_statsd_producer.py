#!/usr/bin/env python3
"""Emits an app-like statsd/DogStatsD workload from a *real* third-party client, for
`script/record-fixtures statsd` to capture through `raw_capture.py --proto udp`.

Two clients, four modes, because the thing worth capturing is not "a statsd line" -- this repo
already has 44 hand-written ones under `crates/logit-cli/tests/fixtures/statsd/` -- but **how a
real client packs an application's metrics into datagrams**:

    --client dogstatsd --mode unbuffered   `datadog`'s DogStatsd, one datagram per call
    --client dogstatsd --mode buffered     the same client with its own send buffer on
    --client statsd    --mode plain        `statsd`'s StatsClient, one datagram per call
    --client statsd    --mode pipeline     the same client through `client.pipeline()`

The two buffered modes are the interesting ones: each client decides for itself how many lines fit
in a datagram and where to cut, which is exactly the distribution `perf/load/README.md` measures
the load model's `datagram_mix:` weights out of. The two unbuffered modes are the other end of that
same distribution -- one metric per datagram, the syscall-bound shape
[ADR `udp-intake-batching-and-socket-visibility`](../../docs/adr/udp-intake-batching-and-socket-visibility.md)
calls the worst case.

The workload itself is a small, plausible web service: request counters and latency timings per
endpoint and status, a response-size distribution, worker-queue gauges, an active-user set, and a
sampled cache counter. Deliberately not a loop over one metric -- name length, tag count and value
width are what a decoder actually pays for, and a single repeated line would misreport all three.
Values come from a seeded `random.Random`, so a re-record produces the same *shape* (it cannot
produce the same bytes: see `testdata/interop/README.md` on why this corpus is real captures rather
than golden files).

Stdlib plus the client under test only. `script/record-fixtures` `pip install`s the client into a
stock `python:3.12-slim` at record time, the same "install the real third-party software fresh"
shape `rsyslog`/`collectd` already use with `apt-get`; the resolved version is printed here so
`testdata/interop/statsd/README.md`'s provenance table records what actually ran.
"""

import argparse
import random
import sys

# A small, fixed service topology. Sizes chosen to look like a real app's naming rather than to hit
# a byte target: hierarchical dotted names, a handful of endpoints, a few status codes, one host.
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

    The floor is not cosmetic. An unclamped `gauss(42, 18)` emits negative timings a few percent of
    the time, and a capture full of negative durations would be a fixture of a bug rather than of a
    real application's traffic -- worth getting right in a corpus whose whole point is being real.
    """
    return max(0.05, rng.gauss(mean, stddev))


def version_of(package):
    """The installed distribution version, for the provenance table -- printed, never asserted on."""
    try:
        from importlib.metadata import version

        return version(package)
    except Exception as err:  # pragma: no cover - diagnostic path only
        return "unknown ({})".format(err)


def dogstatsd_client(host, port, buffered):
    """A real `datadog.DogStatsd`, with buffering explicitly on or off.

    The buffering knob has been spelled two ways across the package's life (`disable_buffering=`
    in 0.44+, `max_buffer_size=` before it), so this tries the current one and falls back rather
    than pinning a version this repo has no other reason to pin.
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
        # Two sampled counters, at the two rates a hot path typically picks. Both clients sample
        # *client-side* -- the call returns without sending at `1 - sample_rate` -- so these
        # deliberately appear on the wire far less often than the unsampled lines above, which is
        # itself part of what the capture measures.
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
            # The context manager is the documented way to batch: it opens a buffer on entry and
            # flushes whatever is left on exit, so nothing is lost when the workload ends
            # mid-datagram.
            with client:
                dogstatsd_workload(client, rng, args.rounds)
        else:
            dogstatsd_workload(client, rng, args.rounds)
        # Belt and braces across versions: `flush` exists on the buffered paths, and calling it on
        # an unbuffered client is a no-op.
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
