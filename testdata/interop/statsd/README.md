# statsd / DogStatsD producer fixtures

Real UDP datagrams from two real statsd clients, captured by
[`script/record-fixtures statsd`](../../../script/record-fixtures) through
`tools/record-fixtures/raw_capture.py --proto udp` (one file per datagram, byte for byte, no
re-encoding). These close the "statsd/DogStatsD producer fixtures" gap
[`docs/plans/recorded-interop-fixtures.md`](../../../docs/plans/recorded-interop-fixtures.md) has
carried open since that corpus started, and they are the ground truth
[`perf/load/README.md`](../../../perf/load/README.md)'s traffic model is calibrated against.

`crates/logit-cli/tests/fixtures/statsd/` already holds 40 hand-written line/expectation pairs for
`statsd_in` → `statsd_out` round-tripping. Those cover the *grammar*. What they can't cover, because
they were written by this team, is **how a real client packs an application's metrics into
datagrams** — where it cuts a buffer, how long its names get, how many tags it hangs off a line.
That is what these four captures are for.

## What's here

Four captures of the **same workload** (see
[`tools/record-fixtures/python_statsd_producer.py`](../../../tools/record-fixtures/python_statsd_producer.py):
a small web service reporting request counters and latencies per endpoint and status, a
response-size distribution, worker-queue gauges, an active-user set, and two sampled counters), so
the only difference between the files is the client and its buffering.

| Files | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `statsd-dogstatsd-unbuffered-0{00..23}.raw` (24 files, 3,283 B) | `datadog` 0.53.0 (`DogStatsd`), pip-installed fresh into `python:3.12-slim` (Python 3.12.14) at record time | `python3 run.py --host capture --port 8125 --client dogstatsd --mode unbuffered` | 2026-09-18 | **One metric per datagram** — the unbuffered DogStatsD shape, 95–165 B per datagram (median 142). DogStatsD tag syntax (`\|#k:v,...`, 4–6 tags), all six metric type letters across the set (`c`/`g`/`ms`/`h`/`d`/`s`), sampled lines (`\|@0.1`, `\|@0.5`), and — unprompted, because the client detected its own container — the `\|c:in-<id>` container-id segment. Short hierarchical names (16–28 B): a tagged client puts its cardinality in tags, not in the name. |
| `statsd-dogstatsd-buffered-00{0..3}.raw` (4 files, 5,587 B) | same | `... --client dogstatsd --mode buffered` | 2026-09-18 | **The same lines, packed by the client itself**: 1,341–1,418 B per datagram, 10–11 metrics each. The cut points are DogStatsD's own — its UDP buffer is 1,432 B by default, and it never splits a line across datagrams. This is the fixture that says what "a packed statsd datagram" actually looks like. |
| `statsd-plain-unbuffered-0{00..23}.raw` (24 files, 1,544 B) | `statsd` 4.0.1 (`StatsClient`), same image | `... --client statsd --mode plain` | 2026-09-18 | **Plain statsd, no tags**: one metric per datagram, 53–76 B. The same information the tagged client carries in tags has to live in the metric name instead, so names are 46–64 B and there are twice as many distinct ones. Sampled lines here are sampled *client-side* — the call simply doesn't send at `1 - rate` — which is why a `\|@0.1` line is rare on the wire. |
| `statsd-plain-pipeline-00{0..3}.raw` (4 files, 1,975 B) | same | `... --client statsd --mode pipeline` | 2026-09-18 | **`StatsClient.pipeline()` batching**: 467–507 B per datagram, 7–8 metrics each. A second, independent client's answer to the same packing question, and a visibly different one — the `statsd` package cuts at its own 512 B `_maxudpsize`, not at an MTU. |

Whole corpus: 56 files, 12,389 bytes, within
[`testdata/interop/README.md`](../README.md)'s size discipline.

## What was measured out of them

For the load model in `perf/load/`, and recorded here so the two documents can't drift:

| | DogStatsD (tagged) | plain statsd (tagless) |
|---|---|---|
| Datagram size, buffered | 1,341–1,418 B (client ceiling 1,432) | 467–507 B (client ceiling 512) |
| Datagram size, unbuffered | 95–165 B (median 142) | 53–76 B (median 66) |
| Lines per buffered datagram | 10–11 | 7–8 |
| Line length | 94–164 B (median 139) | 53–76 B (median 61) |
| Tags per line | 4–6 (median 6) | 0 |
| Metric name length | 16–28 B (median 23) | 46–64 B (median 56) |
| Distinct metric names in this workload | 8 | 18 |

Every figure above is over the whole corpus — all 65 tagged and all 55 tagless lines — not over any
one capture. The per-file medians differ (the buffered DogStatsD capture alone medians at 137 B,
the pipelined plain-statsd one at 60 B), which is what an earlier revision of this table quoted by
mistake. The eight distinct DogStatsD names are `app.cache.lookups.count`,
`app.db.query.duration_ms`, `app.http.request.duration_ms`, `app.http.requests.count`,
`app.http.response.size_bytes`, `app.render.calls.count`, `app.users.active` and
`app.worker.queue.depth`.

Metric-type mix across all 120 captured lines: `c` 43, `ms` 22, `g` 21, `h` 14, `d` 13, `s` 7.
**Read that as a property of the producer script, not of production traffic** — it is what this
synthetic app happens to emit, and `perf/load/README.md` says so explicitly where it chooses its own
type weights instead.

## Re-recording

```
script/record-fixtures statsd
```

Not run by CI, for the reason the top-level [`README.md`](../README.md) gives for every producer
here: recording from live third-party software is a deliberate, reviewed act. The run pulls
`python:3.12-slim` and `pip install`s the client fresh, so **a re-record will pick up whatever
version of `datadog`/`statsd` is current** — the producer prints the resolved version as its first
line of output, and the table above should be updated from that rather than assumed.

A re-record does **not** reproduce these bytes. The workload's own values come from a seeded
`random.Random`, so the *shape* is stable, but the DogStatsD container id changes with the
container, and which lines land in which buffered datagram depends on flush timing. That is the
same "real capture, not a golden file" property every other producer in this corpus has: consuming
tests assert on decoded values, never on raw bytes.

## What isn't covered here (yet)

- **DogStatsD events (`_e{...}`) and service checks (`_sc|...`).** Neither client's high-level API
  emits them in an ordinary application workload, and `crates/logit-cli/tests/fixtures/statsd/`
  already has hand-written cases for both. A capture would need a producer written specifically to
  call `client.event()`/`client.service_check()`, which is a different kind of fixture from these.
- **`|T<timestamp>` point timestamps.** DogStatsD v1.3+ only, and `datadog` 0.53.0's Python API
  doesn't expose them on the metric helpers used here.
- **Unix-domain-socket transport.** DogStatsD's other transport; `logit`'s `statsd_in` is UDP and
  TCP only, so there is nothing here to check a UDS capture against.
- **A statsd server's own relay output.** These are *client* captures. What another statsd server
  (gostatsd, statsite, the Datadog Agent) forwards is a separate producer worth capturing later.
