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
restart keeps the scoreboard and every total; a full restart zeroes them (both tested). Apache has
no Prometheus exposition: `modules/` holds no such module.

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
`prometheus_in` or a `format:` of one generic kind, because each has its own validation (section
10's `auto` rule), its own body cap, and its own mapping, and a kind's name is what an operator
searches for.

Neither will be a lossless pair: there's no `_out` and nothing to relay back onto the wire. Neither
will be a transit-spec source, because gauges and cumulative sums from other sources already cover
every sink's column ([ADR `transit-spec`](transit-spec.md)).

HAProxy will get no kind; section 14 points an operator at `prometheus_in`.

### 2. The scrape driver and its seam

A new module, `crates/logit-inputs/src/scrape.rs`, will hold a generic `ScrapeInput<D:
ScrapeDecoder>`, shaped like `UdpListener<D>` and `TcpListener<D>`. `prometheus_in`'s scrape mode
will move onto it with no change in behavior: the same attributes, synthetics, telemetry, and
diagnostic text, with its existing tests passing unchanged apart from paths and type names.

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
- the throttled `scrape_failed` diagnostic, its text prefixed with the kind's name;
- the three synthetic series (section 5);
- one `EventBatch` per target per tick on that target's `Resource`, sent through the `Fanout`.

The decoder will supply its kind's name for messages, its `Accept` value, its body cap, the
per-target resource builder (section 4), and one decode call from a body, the response's
`Content-Type`, and the tick's timestamp to the events it produced or an error. A decoder will be
kept across ticks, so its throttled diagnostics accumulate. Prometheus's dialect choice from the
`Content-Type` will move behind its decoder. The trait's final shape is the implementation's, set in
review.

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
| `scrape_tls` | `{}` | client TLS for `https://` targets; rejected with no `https://` target |

`scrape_tls` will keep its prefix although these kinds have one mode, so that no scrape kind has a
bare `tls:` key. Each kind's config struct will carry `deny_unknown_fields`.

Graph rule 40's scrape-target checks will become shared across the three kinds, and the
reserved-header list will become the driver's (`accept`, `user-agent`, and the connection-management
names rule 40 lists today). Rule 55 will stay `prometheus_in`'s, since only it has two modes.

A new rule will reject two `scrape_targets` entries of one new-kind component that resolve to the
same host and port. Both pages are server-wide, so two such entries scrape one server twice, and
section 4's resource can't tell their series apart. `prometheus_in` doesn't need the rule: its
resource carries the URL.

### 4. Resource identity

Each target's resource will carry OTel semantic-convention `server.address` (the URL's host, an IPv6
address without brackets) and `server.port` (an integer, defaulted to 80 or 443 from the scheme),
built from the URL. A URL that `reqwest` can't parse, which rule 40's approximation lets through,
will get `prometheus_in`'s fallback, `<unparseable target #N>`, as `server.address` and no
`server.port`.

The new kinds will carry neither `instance` nor `_logit.prometheus.target`, which are Prometheus's
vocabulary and family carrier ([ADR
`protocol-knowledge-stays-in-family`](protocol-knowledge-stays-in-family.md)). The Collector's
`apache.server.name` and `apache.server.port` (strings) will give way to the semantic convention
pair, a named deviation. The page's own server-name line won't be used: it's the name the request's
virtual host answers to, which repeats the URL's host, not the machine's name.

### 5. Synthetic scrape metrics

Every target on every tick, failures included, will get `prometheus_in`'s three synthetic series on
its resource, under the same names and kinds: `up` (`Gauge`, `1` or `0`), `scrape_duration_seconds`
(`Gauge`), and `scrape_samples_scraped` (`Gauge`, the number of events the decoder produced). They
describe the scrape, not the server, and `up` is the name liveness alerting already keys on.
Liveness was one of the three reasons to read these pages.

`up` will be `1` only for a `2xx` response the decoder accepted. A `parse_error` will set `up` and
`scrape_samples_scraped` to `0` and emit no decoded events, as `prometheus_in` does today
(`PrometheusInput::tick`). Section 10's skipped values will leave `up` at `1`.

### 6. Timestamps and start time

Every point will carry the tick's timestamp, taken once before the tick's fetches start, as
`prometheus_in`'s points do. No cumulative `Sum` will carry a start time: `start_timestamp` stays
`0`, OTLP's "unknown", as in both Collector receivers. A downstream reset check reads a restart as a
decrease, which both servers' full restarts produce. The alternatives below include the two start
times this rejects.

### 7. A value that can decrease is a `Gauge`

A counter that only resets on a restart will be a cumulative monotonic `Sum`. Every other number
will be a `Gauge`, including those the Collector models as a non-monotonic cumulative `Sum` (an
UpDownCounter): `nginx.connections_current`, and Apache's connection, worker, scoreboard, and
worker-limit counts. This deviation is named in each mapping table below.

The reason is what `logit`'s sinks do with each. The transit spec's `sum` rows for
`cumulative_nonmonotonic` (`spec/transit/mapping.yaml`) class a non-monotonic cumulative `Sum` as
`degraded` at `statsd_out`, `prometheus_out`, `influxdb_out`, `datadog_out`, and `graphite_out`,
each writing a gauge and counting `logit.output.metrics.degraded` once per record, and `exact` only
at `otlp_out` and `kafka_out`'s OTLP formats. The same spec reads a `Gauge` back as a gauge at every
one of them, with at most the integer-as-float normalization a `Sum` also gets.
`cumulative_to_delta` passes a non-monotonic `Sum` through by default (`NonMonotonic::Pass` in
`crates/logit-transforms/src/cumulative_to_delta.rs`). So the `Sum` would arrive everywhere but OTLP
as the same gauge, with a degraded count on every scrape that an operator would have to chase.

`apache.cpu.time` will be a `Gauge` for a second reason: its value can drop while the server runs.
Each scoreboard slot's CPU times are zeroed when its process exits (`update_child_status_internal`
in `server/scoreboard.c`, on `SERVER_DEAD`), and `mod_status` sums the slots, so a child exiting
under `MaxConnectionsPerChild` or a graceful restart lowers the total.

### 8. Value types and units

A count will be an `I64` `MetricValue` ([ADR `typed-metric-scalar`](typed-metric-scalar.md)),
matching the Collector's `int` points and written exact by `otlp_out`. Seconds of CPU, percentages,
and load averages will be `F64`. A value that doesn't fit its type, such as a negative count or one
past `i64::MAX`, will be malformed (sections 9 and 10). Each record will carry its unit in
`MetricRecord.unit` and no description.

Units will follow the Collector's `metadata.yaml`, with three deviations named in section 10's
table: `apache.cpu.time` will be in `s`, not `{jiff}`, because the page reports seconds
(`mod_status.c` divides clock ticks by the tick rate); `apache.load.*` will be `1`, not `%`, because
a load average is a run-queue length, not a percentage; and `apache.connection.status` and
`apache.worker.status` will be `{connection}` and `{worker}`, not `1`, because each value counts
connections or workers in a state, where the semantic-convention status-metric pattern the Collector
cites for `1` is a `0` or `1` per state.

### 9. `nginx_stub_status_in`: grammar and mapping

**Grammar.** The decoder will accept a body of four lines in order, each ending in `\n`:

```
Active connections: <n>
server accepts handled requests
 <accepts> <handled> <requests>
Reading: <n> Writing: <n> Waiting: <n>
```

Words are separated by single spaces and `<n>` is ASCII decimal digits. Trailing spaces or a `\r`
before a line's `\n` will be ignored, and a final line without its `\n` accepted. The header line
may instead read `server accepts handled requests request_time`, Tengine's form, and then the number
line must hold four numbers; the fourth will be read, checked as a number, and not emitted, since
the Collector has no name for it and its unit isn't documented. Any other body, including a number
count that doesn't match the header, a missing line, an extra non-empty line, or a value that
doesn't fit an `I64`, will be a `parse_error`: the page is fixed-format, so a mismatch means the
target isn't a `stub_status` page, and no partial reading will be emitted.

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

**Validation.** A new graph rule will reject a target whose query has no `auto` key (`?auto`,
`?auto&notable`, `?refresh=5&auto`, with or without a value). Without it Apache serves the HTML
page, so every scrape would fail.

**Grammar.** The body is lines ending in `\n`; the decoder will ignore a trailing `\r`. The first
line is the server name and will be skipped. Every other line will be split at its first `": "` into
a key and a value. A line with no `": "` (`TLSSessionCacheStatus`) and a key the table below doesn't
list will be ignored, uncounted, since the key set varies by version, MPM, `ExtendedStatus`, and
loaded modules. A listed key's value will be parsed as its type, `%g` forms such as `.266667`
included; one that doesn't parse will be skipped, counted
`logit.input.metrics.dropped{reason="malformed_value"}` with a throttled diagnostic naming the key,
and the rest of the page still read. A listed key that repeats will keep its first value, the later
one counted `reason="duplicate_key"`: `mod_status`'s own lines come before any other module's. A
body with no listed key, such as the HTML page behind a rewrite, will be a `parse_error`.

A listed key that's absent will emit nothing and count nothing: `ExtendedStatus Off` drops every
`Total*` and `CPU*` key, a non-async MPM drops the `Conns*` keys, and `mod_status` omits `CPULoad`
when CPU time is zero. `scrape_samples_scraped` will show how many points a page gave.

**Mapping** (the Collector's new-format names only, never the old duplicates; every name, unit, and
attribute checked against `apachereceiver`'s `metadata.yaml`, and the `apache.cpu.time` attribute
keys against `scraper.go`):

| `?auto` key | Metric | Kind | Value | Unit | Attributes | Deviation or reason |
|---|---|---|---|---|---|---|
| (first line, server name) | not mapped | | | | | the virtual host's name, which repeats the URL's host (section 4) |
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
and `scrape_failed`. `apache_status_in`'s decoder will add `logit.input.metrics.dropped{reason}` for
section 10's skipped values, under the loss vocabulary ([ADR
`telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)). Neither decoder will write a key,
value, or URL into a tag.

### 14. Operator guidance

`docs/deploying.md` will get a section for the two kinds that says:

- **Serve the status location on an internal interface or network only.** Both pages disclose
  connection counts, and Apache's discloses the server version and MPM. Bind it to a separate
  `listen` or `<VirtualHost>` on a loopback or internal address, or restrict it with `allow`/`deny`
  or `Require ip`.
- Apache's target URL needs `?auto`, which `logit validate` checks.
- Both servers count `logit`'s scrapes in their request totals; nginx's `requests` includes the
  scrape that reads it.
- A cumulative `Sum` reaching `datadog_out` is dropped as `unresolved_cumulative` unless a
  `cumulative_to_delta` sits upstream ([ADR
  `datadog-otlp-metric-mapping`](datadog-otlp-metric-mapping.md)).
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
- **One kind with a `format:` key.** One kind would carry per-format validation (Apache's `auto`
  rule), per-format caps, and per-format mappings behind one name, and an operator searches by the
  server's name. Rejected for two kinds.
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
  graph rules 9 and 40, plus two new rules: section 3's duplicate-server check and section 10's
  `auto` check.
- Two decoders will join `logit-proto` with fuzz targets, seeds, and recorded corpora, which
  `script/record-fixtures` can re-record.
- Section 15's known gaps will grow to name the new kinds. Closing the redirect gap in the driver
  will close it for all three kinds at once.
- The Collector may rename again: `apachereceiver`'s new names sit behind alpha feature gates, and
  every metric in both receivers is marked `development`. A rename there is a choice for `logit`,
  not a break; this ADR records the names as of the commits in the context.
- Follow-ons not planned: a generic HTTP fetch for JSON status endpoints (Angie's `/status/`, NGINX
  Plus's API), and `GracefulWorkers` and the other unmapped keys if a user asks for them.
