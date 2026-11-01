---
created: 2026-10-31
updated: 2026-10-31
---

# Server status inputs: `nginx_stub_status_in` and `apache_status_in` on a scrape driver shared with `prometheus_in`

## Status
Accepted. Not yet built: the [`status` plan](../plans/server-status-inputs.md) lists the
workstreams.

## Context

nginx and Apache httpd each serve their own server-wide counters on an HTTP page in a plain-text
layout of their own, and `logit` reads neither. An operator who wants them today runs an exporter
sidecar per server and scrapes it with `prometheus_in`.

**nginx's `stub_status`** (`ngx_http_stub_status_module`, in the official `nginx` image and the
distribution packages) is turned on with a `location { stub_status; }` block and serves four lines
as `text/plain` (`src/http/modules/ngx_http_stub_status_module.c`, unchanged since 2021; a capture
from nginx 1.31.8):

```
Active connections: 1 
server accepts handled requests
 10 10 10 
Reading: 0 Writing: 1 Waiting: 0 
```

Lines 1, 3, and 4 end in a space. `accepts`, `handled`, and `requests` are counters; the other four
are gauges, and `active` covers the other three. The counters live in shared memory, so they survive
`nginx -s reload` and reset when the master process restarts (both tested). The request that reads
the page counts toward `requests` and `Writing`. freenginx and Angie print the same format string.
Tengine prints it too unless built with `T_NGX_HTTP_STUB_STATUS`, which adds a fourth column,
`request_time`, to the header and number lines. Open-source nginx has no other format and no
Prometheus exposition; NGINX Plus's JSON API isn't open source.

The page carries three things the access log can't: connection state, including idle keepalives and
handshakes that never produce a log line (a gap between `accepts` and `handled` means the server hit
a resource limit such as `worker_connections`); a request total that doesn't depend on the log
pipeline, so it cross-checks log loss; and liveness, since a failed scrape means the server is down
or wedged.

**Apache's `mod_status`** serves `/server-status`, an HTML page, and under the `auto` query key a
`Key: value` text form as `text/plain; charset=ISO-8859-1` (`modules/generators/mod_status.c`,
2.4.x). The first line is the bare server name with no key. Which keys appear depends on three
things:

- `ExtendedStatus` (on by default whenever `mod_status` is loaded) adds the request, traffic,
  duration, and CPU totals and the derived averages.
- An async MPM (`event`) adds `Processes`, `Stopping`, and the `Conns*` lines; `prefork` and
  `worker` omit them.
- `mod_ssl` with a session cache appends its own block after `Scoreboard`, keyed by the cache
  provider.

Captures from httpd 2.4.69 under `event`, `prefork`, `ExtendedStatus Off`, and `mod_ssl` with
`shmcb` show each variant. Numbers use C's `%g`, so `.0689655` has no leading zero. A graceful
restart keeps the scoreboard and every total. A full restart zeroes the request, traffic, and
duration totals and the uptime, but not the CPU totals, which come from per-process times (both
tested). Apache has no Prometheus exposition: `modules/` holds no such module.

**The OpenTelemetry Collector** has a receiver for each, `nginxreceiver` and `apachereceiver`
(contrib, beta; `metadata.yaml` at commits `d5fbc3aa` and `6ac8ee46`). `apachereceiver` is
mid-migration: two feature gates, `receiver.apache.enableNewFormatMetrics` and
`receiver.apache.disableOldFormatMetrics` (alpha, from v0.159.0), move it to a set of new metric and
attribute names. By default it emits only the old names. Its new-format `apache.cpu.time` renames
the `level` and `mode` attributes to `apache.process.level` and `cpu.mode` in `scraper.go`'s
`applyCPUTimeNewFormat`, not in `metadata.yaml`.

**HAProxy** ships a Prometheus exporter as a built-in service (`http-request use-service
prometheus-exporter`), so `prometheus_in` already reads it.

**`prometheus_in`'s scrape mode** already does what both pages need around the parse: a target list,
an interval, concurrent fetches with a timeout and a body cap, client TLS with reload, status
classes, synthetic `up`, and per-target batches ([ADR
`prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md)). It lives inside
`crates/logit-inputs/src/prometheus.rs`, wired to the Prometheus text decoder.

**A prototype without a new input** fetched the nginx page, sent it to `syslog_in` as one datagram
under `framing: datagram`, and parsed it with a 25-line `lua_file` that minted the seven metrics
through `Event.new`. It worked end to end against the release image and stock `nginx:1`, and it
showed what any input carrying the page must do: deliver the whole page as one event (a
line-splitting input turns it into four unrelated events), put it in `log.message`, and tolerate the
trailing spaces.

## Decision

`logit` will gain two input kinds, `nginx_stub_status_in` and `apache_status_in`, each a scrape
client for one server's status page on a scrape driver extracted from `prometheus_in`, emitting the
Collector's metric names.

### 1. One kind per server, on a shared driver

Each server will get its own kind, named for the page it reads. Neither will be a mode of
`prometheus_in` or a `format:` of one generic kind, because each has its own grammar, its own body
cap, and its own mapping, and a kind's name is what an operator searches for.

Neither will be a lossless pair: there's no `_out` and nothing to relay back onto the wire. Neither
will be a transit-spec source, because gauges and cumulative sums from other sources already cover
every sink's column ([ADR `transit-spec`](transit-spec.md)).

HAProxy will get no kind; section 14 points an operator at `prometheus_in`.

### 2. The scrape driver and its seam

A new module, `crates/logit-inputs/src/scrape.rs`, will hold a generic `ScrapeInput<D:
ScrapeDecoder>`, shaped like `UdpListener<D>` and `TcpListener<D>`. `prometheus_in`'s scrape mode
will move onto it with no change in behavior apart from two validation changes in section 3: the
same attributes, synthetics, telemetry, and diagnostic text, with its existing tests passing
unchanged apart from paths, type names, and those two changes.

The driver will own everything that isn't the page's format:

- the target list, each with its URL, its redacted form (`redact_url`), and its `Resource`, built
  once at construction;
- the interval ticker, under `MissedTickBehavior::Delay`, with its first tick swallowed;
- one fetch per target per tick, concurrently on a `JoinSet`, each under the per-request timeout;
- the `reqwest` client, with client TLS from `logit_pipeline::tls` (reload included) and ALPN;
- the operator's `headers:`, with the `Accept` and `User-Agent` the driver inserts last;
- the incremental body read under the decoder's cap;
- status classification and the `logit.input.scrapes{class}`, `logit.input.scrape.duration`, and
  `logit.input.samples` counters;
- the throttled `scrape_failed` diagnostic and the errors `headers:` and `scrape_tls:` raise at
  construction, each message prefixed with the kind's name;
- the three synthetic series (section 5);
- one `EventBatch` per target per tick on that target's `Resource`, sent through the `Fanout`.

The decoder will supply its kind's name for messages, its `Accept` value, its body cap, the
per-target resource builder (section 4), and one decode call from a body, the response's
`Content-Type`, and the tick's timestamp to the events it produced or an error. A decoder will be
kept across ticks, so its throttled diagnostics accumulate. Prometheus's dialect choice from the
`Content-Type` will move behind its decoder. The trait's final shape is the implementation's, set in
review. Both new decoders will read the body as bytes, not UTF-8: Apache's page is ISO-8859-1, and
every byte either grammar interprets is ASCII.

Shutdown won't change: `Input::run_until_shutdown`'s default will drop a scrape in flight and any
target's batch not yet sent, as it does for `prometheus_in` today (section 15).

### 3. Config keys and validation, shared by the three kinds

Both new kinds will take `prometheus_in`'s scrape-mode keys, with the same names, defaults, and
checks, so one validation rule covers all three kinds and a block copied between them works:

| Key | Default | Meaning |
|---|---|---|
| `scrape_targets` | required, non-empty | absolute `http://` or `https://` URLs (`targets` is `Component.targets`) |
| `interval` | `15s` | scrape cadence; `0s` is rejected (graph rule 9's table will gain both kinds) |
| `timeout` | `10s` | per-request timeout; `0s` is rejected |
| `headers` | `{}` | extra request headers; a name the driver reserves is rejected |
| `scrape_tls` | `{}` | client TLS for `https://` targets; accepted, with no effect, when no target is `https://` |

`scrape_tls` will keep its prefix although these kinds have one mode, so that no scrape kind has a
bare `tls:` key. Each kind's config struct will carry `deny_unknown_fields`.

Graph rule 40's scrape-target checks will become shared across the three kinds, and the
reserved-header list will become the driver's: `accept`, `user-agent`, and the connection-management
names rule 40 lists today, plus `accept-encoding`, so the driver alone decides which content
encodings a target may answer in. Rule 40 will also stop rejecting a non-empty `scrape_tls:` with no
`https://` target: the block has no effect there, and validation doesn't reject a config for being
pointless. Those two are the only changes to `prometheus_in`'s validation. Rule 55 will stay
`prometheus_in`'s, since only it has two modes.

### 4. Resource identity

Each target's resource will carry OTel semantic-convention `server.address` (the URL's host, an IPv6
address without brackets) and `server.port` (an integer, defaulted to 80 or 443 from the scheme),
and `url.path` (the URL's path, without the query string, which may hold a credential), built from
the URL. `url.path` keeps two targets reached through one proxy at one host and port, such as
`/a/nginx_status` and `/b/nginx_status`, from merging their series. A URL that `reqwest` can't
parse, which rule 40's approximation lets through, will get `prometheus_in`'s fallback,
`<unparseable target #N>`, as `server.address` and no `server.port` or `url.path`.

The new kinds will carry neither `instance` nor `_logit.prometheus.target`, which are Prometheus's
vocabulary and family carrier ([ADR
`protocol-knowledge-stays-in-family`](protocol-knowledge-stays-in-family.md)). The Collector's
`apache.server.name` and `apache.server.port` (strings) will give way to the semantic convention
pair, a named deviation. The page's own server-name line won't be used: it's the name the request's
virtual host answers to, which repeats the URL's host, not the machine's name.

### 5. Synthetic scrape metrics

Every target on every tick, failures included, will get `prometheus_in`'s three synthetic series on
its resource, under the same names and kinds: `up` (`Gauge`, `1` or `0`), `scrape_duration_seconds`
(`Gauge`), and `scrape_samples_scraped` (`Gauge`, the number of events the decoder produced).
They're `logit`'s convention for a scrape kind, kept identical to `prometheus_in`'s so one `up == 0`
alert and one dashboard cover every scrape kind. The names come from Prometheus, and neither
Collector receiver emits them; they describe the scrape, not the server, which is what liveness
needs, and liveness was one of the three reasons to read these pages.

`up` will be `1` only for a `2xx` response the decoder accepted. A `parse_error` will set `up` and
`scrape_samples_scraped` to `0` and emit no decoded events, as `prometheus_in` does today
(`PrometheusInput::tick`). Section 9's out-of-range fields and section 10's skipped values and
discarded line will leave `up` at `1`.

### 6. Timestamps and start time

Every point will carry the tick's timestamp, taken once before the tick's fetches start, as
`prometheus_in`'s points do. No cumulative `Sum` will carry a start time: `start_timestamp` stays
`0`, OTLP's "unknown". A downstream reset check reads a server restart as a decrease, which both
servers' full restarts produce.

Both Collector receivers differ here. Their generated `MetricsBuilder` stamps every point, gauges
included, with the time the builder was created, which is when the receiver started, and neither
scraper calls the builder's `Reset` (`internal/metadata/generated_metrics.go` in each receiver, on
`main` at `504480882aa7`). That start is the collector's, not the server's: it doesn't move when the
server restarts, and it does move when the collector restarts while the server's counters run on.
`cumulative_to_delta` reads a start moved past the previous point as a reset and emits the running
total whole as a delta ([ADR `cumulative-to-delta-transform`](cumulative-to-delta-transform.md)), so
stamping `logit`'s own start would overcount once per `logit` restart. The alternatives below
include the two start times from the page that this also rejects.

### 7. A value that can decrease is a `Gauge`

A counter that only resets on a restart will be a cumulative monotonic `Sum`. Every other number
will be a `Gauge`, including those the Collector models as a non-monotonic cumulative `Sum` (an
UpDownCounter): `nginx.connections_current`, and Apache's connection, worker, scoreboard, and
worker-limit counts. This deviation is named in each mapping table below.

A non-monotonic cumulative `Sum` adds fidelity only at OTLP. The transit spec's `sum` row for
`cumulative_nonmonotonic` (`spec/transit/mapping.yaml`) has it `exact` only at `otlp_out` and
`kafka_out`'s OTLP formats; `statsd_out`, `prometheus_out`, `influxdb_out`, `datadog_out`, and
`graphite_out` each write a gauge and count `logit.output.metrics.degraded` once per record. A
`Gauge` reads back as a gauge at every one of them, with at most the integer-as-float normalization
a `Sum` also gets, and `cumulative_to_delta` passes a non-monotonic `Sum` through by default
(`NonMonotonic::Pass` in `crates/logit-transforms/src/cumulative_to_delta.rs`). So everywhere but
OTLP the `Sum` would arrive as the same gauge, plus a degraded count on every scrape for an operator
to chase.

The monotonic `Sum`s this design keeps have their own outcomes, from the same file's
`cumulative_monotonic` row: `exact` at `otlp_out` and `kafka_out`; `normalized` at `prometheus_out`,
a `counter` family whose sample gains `_total`; `degraded` at `statsd_out` (a `|g` gauge of the
running total), `influxdb_out` (a `value` field), and `graphite_out` (a bare value), each counted
`logit.output.metrics.degraded{metric_kind="cumulative_sum"}`; and `dropped` at `datadog_out`,
counted `logit.output.metrics.dropped{reason="unresolved_cumulative"}`, unless a
`cumulative_to_delta` sits upstream. They stay `Sum`s because they're counters, and
`cumulative_to_delta` turns a monotonic `Sum` into deltas for a delta-only destination, which it
won't do for a `Gauge`. Section 14 says what to do at each sink.

`apache.cpu.time` will be a `Gauge` for a second reason: its value can drop while the server runs.
Each scoreboard slot's CPU times are zeroed when its process exits (`update_child_status_internal`
in `server/scoreboard.c`, on `SERVER_DEAD`), and `mod_status` sums the slots, so a child exiting
under `MaxConnectionsPerChild` or a graceful restart lowers the total.

### 8. Value types and units

A count will be an `I64` `MetricValue` ([ADR `typed-metric-scalar`](typed-metric-scalar.md)),
matching the Collector's `int` points and written exact by `otlp_out`. Seconds of CPU, percentages,
and load averages will be `F64`. A value that doesn't fit its type, such as a negative count or one
past `i64::MAX`, will be skipped and counted as sections 9 and 10 say. Each record will carry its
unit in `MetricRecord.unit` and no description.

Units will follow the Collector's `metadata.yaml`, with three deviations named in section 10's
table: `apache.cpu.time` will be in `s`, not `{jiff}`, because the page reports seconds
(`mod_status.c` divides clock ticks by the tick rate); `apache.load.*` will be `1`, not `%`, because
a load average is a run-queue length, not a percentage; and `apache.connection.status` and
`apache.worker.status` will be `{connection}` and `{worker}`, not `1`, because each value counts
connections or workers in a state, where the semantic-convention status-metric pattern the Collector
cites for `1` is a `0` or `1` per state.

The units cost something at one sink. `prometheus_out`'s OpenMetrics exposition writes `# UNIT` only
when the unit is `[a-zA-Z0-9_]+` and `_<unit>` ends the family name (`unit_suffixes` in
`crates/logit-proto/src/prometheus/text.rs`), and otherwise leaves it off and counts
`logit.output.metrics.degraded{reason="unit_not_suffix"}`, once per family per write. Every unit
here fails that test, the braced ones and `%` on the character set and the rest on the suffix,
except `apache.load.1`'s `1`, whose sanitized name `apache_load_1` ends in `_1`. So an OpenMetrics
exposition of these series counts that degradation on every write; text 0.0.4 writes no unit and
counts nothing. The units stay set because OTLP carries them to destinations that read them, and
section 14 tells the operator to expect the count.

### 9. `nginx_stub_status_in`: grammar and mapping

**Grammar.** The decoder will accept a body of four lines in order, each ending in `\n`:

```
Active connections: <n>
server accepts handled requests
 <accepts> <handled> <requests>
Reading: <n> Writing: <n> Waiting: <n>
```

The grammar is over bytes. Words are separated by single spaces and `<n>` is ASCII decimal digits.
Trailing spaces or a `\r` before a line's `\n` will be ignored. The final `\n` is required:
`ngx_http_stub_status_handler` ends every line it writes with one, so a body without it was cut
short. The header line may instead read `server accepts handled requests request_time`, Tengine's
form, and then the number line must hold four numbers; the fourth will be read, checked as a number,
and not emitted, since the Collector has no name for it and its unit isn't documented. Any other
body, including a missing final `\n`, a number count that doesn't match the header, a missing line,
an extra non-empty line, or a non-digit, will be a `parse_error`: the page is fixed-format, so a
mismatch means the target isn't a `stub_status` page or the body was cut, and no partial reading
will be emitted.

A field whose digits don't fit an `I64` will be skipped and counted
`logit.input.metrics.dropped{reason="out_of_range"}`, and the rest of the page still emitted, with
`up` at `1`. `%uA` prints an unsigned machine word, so a connection gauge that is transiently below
zero prints as a number near 2^64. On a 32-bit build `ngx_atomic_uint_t` is 32 bits, so a counter
wraps at 2^32, which a downstream reads as a reset.

**Mapping** (Collector's `nginxreceiver`, all four names checked against its `metadata.yaml`):

| Page field | Metric | Kind | Value | Unit | Attributes | Deviation |
|---|---|---|---|---|---|---|
| `accepts` | `nginx.connections_accepted` | cumulative monotonic `Sum` | `I64` | `{connections}` | — | — |
| `handled` | `nginx.connections_handled` | cumulative monotonic `Sum` | `I64` | `{connections}` | — | — |
| `requests` | `nginx.requests` | cumulative monotonic `Sum` | `I64` | `{requests}` | — | — |
| `Active connections` | `nginx.connections_current` | `Gauge` | `I64` | `{connections}` | `state` = `active` | the Collector's non-monotonic `Sum` (section 7) |
| `Reading` | `nginx.connections_current` | `Gauge` | `I64` | `{connections}` | `state` = `reading` | same |
| `Writing` | `nginx.connections_current` | `Gauge` | `I64` | `{connections}` | `state` = `writing` | same |
| `Waiting` | `nginx.connections_current` | `Gauge` | `I64` | `{connections}` | `state` = `waiting` | same |
| Tengine `request_time` | not mapped | | | | | no Collector name |

Each row will be one event. `state` = `active` covers the other three states, so summing
`nginx.connections_current` across `state` double-counts.

### 10. `apache_status_in`: grammar, validation, and mapping

**Target URL.** A target needs `auto` in its query string (`?auto`); without it Apache serves the
HTML page, every scrape is a `parse_error`, and that diagnostic will say to add `?auto` to the URL.
No validation rule checks it: `status_handler` matches `auto` anywhere in the query string
(`ap_strstr_c`), and a proxy in front of the server may add it.

**Grammar.** The grammar is over bytes, since the page is ISO-8859-1 and only its keys and numbers,
which are ASCII, are interpreted. The body is lines ending in `\n`; the decoder will ignore a
trailing `\r`. Every line will be split at its first `": "` into a key and a value. A line with no
`": "` (the server name 2.4 prints first and 2.2 doesn't, and `TLSSessionCacheStatus`) and a key the
table below doesn't list will be ignored, uncounted, since the key set varies by version, MPM,
`ExtendedStatus`, and loaded modules. A listed key's value will be parsed as its type, `%g` forms
such as `.266667` included; one that doesn't parse will be skipped, counted
`logit.input.metrics.dropped{reason="malformed_value"}` with a throttled diagnostic naming the key,
and the rest of the page still read. A listed key that repeats will keep its last value, as the
Collector's `parseStats` map does, counted `logit.input.metrics.normalized{reason="duplicate_key"}`,
the analog of `graphite_in`'s `logit.input.tags.normalized{reason="duplicate_key"}` for a repeated
tag. A last line with no `\n` will be discarded and counted
`logit.input.lines.dropped{reason="truncated"}`, and the lines before it still read: every line
`status_handler` and the status hooks write ends in `\n` (`mod_status.c`, `ssl_scache.c`,
`mod_socache_shmcb.c`), so an unterminated one was cut short. A body with no listed key, such as the
HTML page, will be a `parse_error`.

A listed key that's absent will emit nothing and count nothing: `ExtendedStatus Off` drops every
`Total*` and `CPU*` key, a non-async MPM drops the `Conns*` keys, and `mod_status` omits `CPULoad`
when CPU time is zero. `scrape_samples_scraped` will show how many points a page gave.

**Mapping** (the Collector's new-format names only, never the old duplicates; every name, unit, and
attribute checked against `apachereceiver`'s `metadata.yaml`, and the `apache.cpu.time` attribute
keys against `scraper.go`):

| `?auto` key | Metric | Kind | Value | Unit | Attributes | Deviation or reason |
|---|---|---|---|---|---|---|
| (first line, server name, printed by 2.4 and not 2.2) | not mapped | | | | | a line with no `": "`, ignored; the virtual host's name, which repeats the URL's host (section 4) |
| `ServerVersion`, `ServerMPM`, `Server Built` | not mapped | | | | | strings; the Collector maps none of them |
| `CurrentTime`, `RestartTime` | not mapped | | | | | strings in a `%Z` zone abbreviation; a point's time is the tick's (section 6) |
| `ParentServerConfigGeneration`, `ParentServerMPMGeneration` | not mapped | | | | | no Collector name; a restart already reads as `apache.uptime` decreasing |
| `ServerUptimeSeconds` | `apache.uptime` | cumulative monotonic `Sum` | `I64` | `s` | — | — |
| `ServerUptime` | not mapped | | | | | `ServerUptimeSeconds` as human text |
| `Load1`, `Load5`, `Load15` | `apache.load.1`, `apache.load.5`, `apache.load.15` | `Gauge` | `F64` | `1` | — | unit, not `%` (section 8); the host's load average, not Apache's |
| `Total Accesses` | `apache.request.count` | cumulative monotonic `Sum` | `I64` | `{request}` | — | — |
| `Total kBytes` | `apache.traffic` | cumulative monotonic `Sum` | `I64` | `By` | — | the value × 1024, as the Collector does; the page truncates to whole KiB |
| `Total Duration` | `apache.request.time` | cumulative monotonic `Sum` | `I64` | `ms` | — | — |
| `CPUUser` | `apache.cpu.time` | `Gauge` | `F64` | `s` | `apache.process.level` = `self`, `cpu.mode` = `user` | `Gauge`, not a monotonic `Sum` (section 7); unit, not `{jiff}` (section 8) |
| `CPUSystem` | `apache.cpu.time` | `Gauge` | `F64` | `s` | `apache.process.level` = `self`, `cpu.mode` = `system` | same |
| `CPUChildrenUser` | `apache.cpu.time` | `Gauge` | `F64` | `s` | `apache.process.level` = `children`, `cpu.mode` = `user` | same |
| `CPUChildrenSystem` | `apache.cpu.time` | `Gauge` | `F64` | `s` | `apache.process.level` = `children`, `cpu.mode` = `system` | same |
| `CPULoad` | `apache.cpu.load` | `Gauge` | `F64` | `%` | — | — (a lifetime average since the last restart, as `mod_status` computes it) |
| `Uptime` | not mapped | | | | | a second copy of `ServerUptimeSeconds` |
| `ReqPerSec`, `BytesPerSec`, `BytesPerReq`, `DurationPerReq` | not mapped | | | | | lifetime averages since the last restart, not rates over an interval; a rate comes from the counters downstream. The Collector's `apache.request.rate` and `apache.traffic.rate` are off by default |
| `BusyWorkers` | `apache.worker.active` | `Gauge` | `I64` | `{worker}` | — | `Gauge` (section 7) |
| `GracefulWorkers` | not mapped | | | | | no Collector name; slots finishing a graceful restart are `apache.worker.status`'s `finishing` |
| `IdleWorkers` | `apache.worker.idle` | `Gauge` | `I64` | `{worker}` | — | `Gauge` (section 7) |
| `Processes`, `Stopping` | not mapped | | | | | no Collector name |
| `ConnsTotal` | `apache.connection.active` | `Gauge` | `I64` | `{connection}` | — | `Gauge` (section 7) |
| `ConnsAsyncWaitIO` | not mapped | | | | | the Collector doesn't read it, and `apache.connection.state` has no value for it |
| `ConnsAsyncWriting` | `apache.connection.status` | `Gauge` | `I64` | `{connection}` | `apache.connection.state` = `writing` | `Gauge` (section 7); unit, not `1` (section 8) |
| `ConnsAsyncKeepAlive` | `apache.connection.status` | `Gauge` | `I64` | `{connection}` | `apache.connection.state` = `keepalive` | same |
| `ConnsAsyncClosing` | `apache.connection.status` | `Gauge` | `I64` | `{connection}` | `apache.connection.state` = `closing` | same |
| `Scoreboard` | `apache.worker.status` | `Gauge` | `I64` | `{worker}` | `apache.worker.state` (below) | `Gauge` (section 7); unit, not `1` (section 8) |
| `Scoreboard` | `apache.worker.limit` | `Gauge` | `I64` | `{worker}` | — | `Gauge` (section 7); the scoreboard's length |
| the `mod_ssl` session-cache block (`CacheType`, `CacheSharedMemory`, `Cache*Count`, and the rest) | not mapped | | | | | each cache provider prints its own keys; the Collector maps none |

`Scoreboard` will map each character to an `apache.worker.state` as the Collector's
`parseScoreboard` does: `_` `waiting`, `S` `starting`, `R` `reading`, `W` `sending`, `K`
`keepalive`, `D` `dnslookup`, `C` `closing`, `L` `logging`, `G` `finishing`, `I` `idle_cleanup`, `.`
`open`, and any other character `unknown`. Every state but `unknown` will be emitted each scrape,
`0` included; `unknown` only when nonzero. A slot the configuration disables isn't printed, so the
length is the configured worker limit.

### 11. Body caps

Each decoder will set its cap: 64 KiB for `nginx_stub_status_in`, whose page is about 100 bytes, and
1 MiB for `apache_status_in`, whose `Scoreboard` holds one character per worker slot.
`prometheus_in` keeps its 32 MiB. A body past the cap will be the existing `oversize` class, with
`up` at `0`.

### 12. Decoders, fuzz targets, and the recorded corpus

The parsers will live in `logit-proto` as `nginx_status` and `apache_status`, each with its module
doc holding the grammar and mapping table it implements and pointing here. Each reads a peer's
bytes, so each will get a fuzz target (`nginx_status`, `apache_status`), committed seeds, and a row
in `script/unsafe-check`'s `FUZZ_TARGETS` ([ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md)).

`script/record-fixtures` will gain `nginx-status` and `apache-status` producers that run real
servers from images pinned by digest and commit what they serve under
`testdata/interop/nginx-status/` and `testdata/interop/apache-status/`, each body beside its headers
and a README with the provenance table:

- nginx stable and mainline, freenginx, and Angie;
- httpd under `event`, `prefork`, and `worker`, under `ExtendedStatus Off`, and with `mod_ssl` and
  `shmcb`, plus the HTML page as a `parse_error` case.

Tengine has no official image; its four-column form will be tested from a literal built from its
source format string, with that provenance, unless the workstream finds an official image to record.
The decoder tests will read the corpus, and `fuzz/seedgen` will turn it into seeds.

### 13. Telemetry and diagnostics

The new kinds will report the driver's counters and diagnostic with no new names:
`logit.input.scrapes` with the same classes, `logit.input.scrape.duration`, `logit.input.samples`,
and `scrape_failed`. `nginx_stub_status_in`'s decoder will add
`logit.input.metrics.dropped{reason="out_of_range"}` (section 9), and `apache_status_in`'s will add
`logit.input.metrics.dropped{reason="malformed_value"}`,
`logit.input.lines.dropped{reason="truncated"}`, and
`logit.input.metrics.normalized{reason="duplicate_key"}` (section 10), under the loss vocabulary
([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)). Neither decoder will write a key,
value, or URL into a tag. Every message names the kind, the construction errors from `headers:` and
`scrape_tls:` included.

### 14. Operator guidance

`docs/deploying.md` will get a section for the two kinds that says:

- **Serve the status location on an internal interface or network only.** Both pages disclose
  connection counts, and Apache's discloses the server version and MPM. Bind it to a separate
  `listen` or `<VirtualHost>` on a loopback or internal address, or restrict it with `allow`/`deny`
  or `Require ip`.
- Apache's target URL needs `?auto`; without it the scrape reads the HTML page and fails as
  `parse_error`, and the diagnostic says to add it.
- Both servers count `logit`'s scrapes in their request totals; nginx's `requests` includes the
  scrape that reads it.
- What each sink does with the counters (section 7): `datadog_out` drops them as
  `unresolved_cumulative` unless a `cumulative_to_delta` sits upstream ([ADR
  `datadog-otlp-metric-mapping`](datadog-otlp-metric-mapping.md)); `statsd_out` writes the running
  total as a gauge, and a `cumulative_to_delta` upstream makes them `|c` deltas instead;
  `influxdb_out` and `graphite_out` store the running total, so take a rate in the query; and
  `prometheus_out` names each with `_total` (`nginx_requests_total`).
- `prometheus_out`'s OpenMetrics exposition leaves off every unit but `apache.load.1`'s and counts
  `logit.output.metrics.degraded{reason="unit_not_suffix"}` per family on every write (section 8).
  For these series that count is expected.
- Neither kind sets `host.name`, so `datadog_out`, `splunk_hec_out`, and `syslog_out` send `logit`'s
  own hostname as each event's host ([ADR `process-hostname`](process-hostname.md)). To send the
  server's, put a `set` of `host.name` on the resource after each component.
- **HAProxy:** turn on its built-in exporter with `http-request use-service prometheus-exporter` on
  a frontend (`haproxy -vv` lists `prometheus-exporter` under its services when the build has it)
  and scrape it with `prometheus_in`.

### 15. Gaps the new kinds inherit

`docs/known-gaps/runtime.md`'s entry on a batch parked in `Fanout::send` when a listener's future is
dropped names `prometheus_in`'s scrape mode, and `docs/known-gaps/prometheus.md`'s entry on the
scrape client following redirects names its `reqwest::Client`. Both are the driver's, so both will
extend to the new kinds, and the workstream that adds each kind will name it in those entries.

## Alternatives considered

- **A generic `http_fetch` input and a Lua parser.** The prototype in the context showed it works.
  It moves the parse into every operator's script: each writes and maintains its own reading of a
  format with variants (Tengine's column, Apache's MPM- and module-dependent keys, `%g` numbers),
  and its own `up`, timeout, TLS, and failure classes, unless the generic input grows the driver
  anyway. A scrape with no parse also can't say `parse_error`. Deferred until a JSON status endpoint
  gives a generic fetch a use case of its own.
- **The exporter sidecar** (`nginx-prometheus-exporter`, `apache_exporter`) **into
  `prometheus_in`.** It works today. It adds a container per server to deploy, version, and watch, a
  second hop whose failure reads like the server's, and Prometheus names rather than the
  Collector's. A small parser on a driver `logit` already has is cheaper.
- **nginx's `$connections_*` variables in the access log, through `kv_metrics`.** No new input, but
  the gauges are sampled only when a request finishes, so an idle server reports nothing, and
  `accepts`, `handled`, and `requests` aren't available, which loses the connection-limit and
  log-loss checks, two of the three reasons to read the page.
- **One kind with a `format:` key.** One kind would carry per-format grammars, caps, and mappings
  behind one name, and an operator searches by the server's name. Rejected for two kinds.
- **The Collector's old-format Apache names** (`apache.requests`, `apache.workers`,
  `apache.scoreboard`, `apache.current_connections`, `apache.connections.async`, and the bare
  `level`/`mode` and `state` attributes). They're its default today, but the Collector has said
  they'll change; new users would migrate twice. Emitting both, as the Collector does under its
  gate, doubles every series.
- **Non-monotonic cumulative `Sum`s for values that go down, as the Collector models them.** Section
  7: every sink but OTLP degrades them to gauges and counts the degradation on every point.
- **`instance` and `_logit.prometheus.target` as identity.** Those are Prometheus's label and
  `prometheus_in`'s carrier; a component outside that family doesn't write them. `server.address`
  and `server.port` are the semantic-convention names for the server a client talked to.
- **A start time from the page.** `CurrentTime - ServerUptimeSeconds` changes by a second from one
  scrape to the next as the two round independently, which a downstream reads as a start change.
  Parsing `RestartTime` means parsing a `%Z` zone abbreviation, which is ambiguous. nginx gives
  neither. Section 6 leaves the start unknown, as the Collector does.
- **The Collector's `apache.server.name` and `apache.server.port`.** They're strings and specific to
  one receiver, and the port as a string disagrees with `server.port`'s integer. The semantic
  convention pair names the same facts for both kinds.

## Consequences

- `crates/logit-inputs/src/prometheus.rs` will lose its scrape client to `scrape.rs`, and its module
  doc will point at the driver for the shared parts. A later scrape kind will be a decoder and a
  config struct.
- Two kinds will join `ComponentKind`, the schema, the registry, `AGENTS.md`'s inputs table, and
  graph rules 9 and 40. Section 3's two validation changes reach `prometheus_in` too.
- Two decoders will join `logit-proto` with fuzz targets, seeds, and recorded corpora, which
  `script/record-fixtures` can re-record.
- Section 15's known gaps will grow to name the new kinds. Closing the redirect gap in the driver
  will close it for all three kinds at once.
- The Collector may rename again: `apachereceiver`'s new names sit behind alpha feature gates, and
  every metric in both receivers is marked `development`. A rename there is a choice for `logit`,
  not a break; this ADR records the names as of the commits in the context.
- Follow-ons not planned: a generic HTTP fetch for JSON status endpoints (Angie's `/status/`, NGINX
  Plus's API), and `GracefulWorkers` and the other unmapped keys if a user asks for them.
