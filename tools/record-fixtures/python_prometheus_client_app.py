"""A `prometheus_client` app for the `prometheus-scrape` producer (script/record-fixtures).

Exposes one of every type the library writes on `:8000/metrics`, through the library's own HTTP
handler, which picks the dialect from the request's `Accept` header: counters with `_created` and
exemplars, a gauge with a unit and the non-finite values, a histogram with bucket exemplars, a
summary, an `info`, an `Enum` (OpenMetrics `stateset`), and, from a custom collector, a
`gaugehistogram` and an `unknown`. Label values carry every escape the format defines.

The values are fixed so a re-record differs only in the `_created` instants, the process and GC
collectors' readings, and exemplar timestamps.
"""

import importlib.metadata
import sys
import time

from prometheus_client import Counter, Enum, Gauge, Histogram, Info, Summary, start_http_server
from prometheus_client.core import REGISTRY, GaugeHistogramMetricFamily, UnknownMetricFamily

TRACE = {"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "span_id": "00f067aa0ba902b7"}


class Custom:
    """The two types `prometheus_client` has no instrument for, from metric families."""

    def collect(self):
        yield GaugeHistogramMetricFamily(
            "app_queue_wait_seconds",
            "Time jobs now queued have waited.",
            buckets=[("0.5", 2), ("1.0", 5), ("+Inf", 6)],
            gsum_value=4.25,
        )
        yield UnknownMetricFamily("app_legacy_ratio", "A value with no declared type.", value=0.75)


def main():
    version = importlib.metadata.version("prometheus_client")
    print(f"prometheus_client {version} on Python {sys.version.split()[0]}", flush=True)

    requests = Counter("app_requests", "Requests served.", ["method", "path"])
    requests.labels("GET", "/").inc(3, exemplar=TRACE)
    requests.labels("POST", 'a "quoted" \\path\nwith a newline').inc()
    requests.labels("GET", "/caf\u00e9/\u65e5\u672c").inc(2.5)

    temperature = Gauge("app_temperature", "Sensor reading.", ["sensor"], unit="celsius")
    for sensor, value in [
        ("nan", float("nan")),
        ("pos_inf", float("inf")),
        ("neg_inf", float("-inf")),
        ("neg_zero", -0.0),
        ("max", 1.7976931348623157e308),
        ("min_subnormal", 5e-324),
        ("plain", 21.5),
    ]:
        temperature.labels(sensor).set(value)

    latency = Histogram("app_latency_seconds", "Request latency.", buckets=(0.005, 0.1, 1, 2.5))
    for value in (0.003, 0.05, 0.05, 0.7, 4.0):
        latency.observe(value)
    latency.observe(0.08, exemplar=TRACE)

    payload = Summary("app_payload_bytes", "Request payload size.")
    for value in (120, 4096, 512):
        payload.observe(value)

    Info("app_build", "Build information.").info({"version": "1.2.3", "revision": "abc123"})
    Enum("app_state", "Lifecycle state.", states=["starting", "running", "stopped"]).state(
        "running"
    )

    REGISTRY.register(Custom())
    start_http_server(8000)
    print("serving on :8000", flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
