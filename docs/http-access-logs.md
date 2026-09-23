# HTTP access logs into `logit`

This is the schema reference for the `http_access` transform: what a web server's access log
must emit, a snippet per server, and what `http_access` derives from it. Configured as below, one
access line becomes a real trace span, a bounded metric set, and a log record that follow the
OpenTelemetry (OTel) HTTP semantic conventions, with no `map`/`log-format` normalization layer on
the server.

Related docs:

- [ADR `http-access-normalization`](adr/http-access-normalization.md): why the work lives in the
  pipeline rather than on the server.
- [`docs/plans/http-access-normalization.md`](plans/http-access-normalization.md): the build-out.
- [`docs/deploying.md`](deploying.md)'s "The nginx-side recipe": the operational side of running
  `logit` against a real server (keeping the old log line during cutover, the syslog
  message-size limit, the startup-ordering rule). This doc is the schema half that recipe points
  at.

## The contract

**The server logs the raw, untouched value under the standard OTel semconv attribute name, and
nothing else**: no capping, no coercion, no classification, and no escaping beyond making the line
valid JSON. One `http_access` component, placed once in the pipeline, does the rest: coercion,
unit merging, method/version normalization, query redaction, length capping, control-byte
cleaning, user-agent and route classification, and span derivation.

Without `http_access`, each deployment writes that normalization by hand in the server's own
config language: rename each variable to its semconv name, cap every free-text field, turn
`HTTP/1.1` into `1.1`, turn a `000` client-abort status into a real integer, classify the user
agent into `browser`/`crawler`/`tool`/`scanner`/`other`, classify the request path into a handful
of low-cardinality routes for a span name, decide when a status becomes `span.status: error`, and
redact sensitive query parameters. In nginx that's roughly a hundred lines of `map` blocks, and
every HAProxy, Varnish, or Squid user rediscovers it in a different language with different
escaping rules. It also fails quietly: an unquoted `"status":$status` produces invalid JSON
(`{"status":000}`) on every client-aborted request, and nothing tests that a hand-written `map`
table still agrees with semconv six months later.

Raw semconv names, rather than a `logit`-specific namespace, mean that a pipeline without
`http_access` still carries standard attribute names (just unhardened), that anyone reading the
config recognizes every key, and that the mapping from a server's own variables to the schema is
this doc, not code. There is deliberately no `preset: nginx`, because the component would then own
every server's variable vocabulary and its drift.

nginx, Apache, HAProxy, Varnish, Squid, and Envoy let you choose the key each value is logged
under, so they write the schema directly. Caddy and Traefik have fixed JSON key names, so for those
two the pipeline renames first (see their snippets below).

## The pipeline

`http_access` sits between whatever decoded the line into attributes and `trace_context`, which
lifts the span it describes:

```
syslog_in / docker_in / tail_in -> json -> http_access -> trace_context -> kv_metrics -> keep -> keep_values -> aggregate -> sink
```

```yaml
components:
  access_in:
    type: syslog_in # or docker_in / tail_in, whichever matches how this server ships lines
    bind: 0.0.0.0:5140

  # nginx's escape=json passes bytes >= 0x80 through raw, so one Latin-1 User-Agent makes the
  # whole line invalid UTF-8 and json's strict parse would fail it. `replace` retries a failed
  # parse on a lossy copy, on the failure path only. See "Quoting rules" below.
  access_json:
    type: json
    sources: [access_in]
    invalid_utf8: replace

  access_http:
    type: http_access
    sources: [access_json]
    routes:
      - builtin: probes
      - builtin: well_known
      - builtin: assets
    route_other: /{other}

  # `mint_id: true` is right only because this tier is the edge and doesn't forward a traceparent
  # of its own. A tier that proxies onward and sends its own `traceparent` must log the span id it
  # actually sent and leave `mint_id` off -- minting a different one here would fork the trace.
  access_trace:
    type: trace_context
    sources: [access_http]
    span:
      kind: server
      mint_id: true

  access_metrics:
    type: kv_metrics
    sources: [access_trace]
    distributions:
      - name: http.server.request.duration
        field: http.request.duration_s
        unit: s

  # keep before aggregate is what bounds SeriesKey cardinality -- see "Bounding cardinality".
  access_keep:
    type: keep
    sources: [access_metrics]
    fields:
      - http.request.method
      - http.response.status_code
      - http.route
      - url.scheme
      - network.protocol.version
      - server.address

  access_bounded:
    type: keep_values
    sources: [access_keep]
    attributes:
      server.address:
        normalize: [lower]
        allow: [app1.internal, app2.internal]
        other: other

  access_window:
    type: aggregate
    sources: [access_bounded]
    interval: 10s

  access_out:
    type: influxdb_out
    sources: [access_window]
    url: http://influxdb:8086
    org: logit
    bucket: metrics
    token: !env INFLUXDB_TOKEN
```

Every `http_access` config field is optional: a bare `type: http_access` still coerces, caps,
classifies user agents with the built-in table, and derives the span fields. It rewrites
attributes only on an event that carries a log record; a metric or span event passes through
untouched. It never touches `event.log`, `event.metrics`, `event.span`, `event.timestamp`, or the
batch `Resource`. It never drops an event: a value that doesn't parse is left exactly as it arrived
and counted, and every other field is still normalized.

Working, shipped examples: [`examples/nginx/nginx.conf`](../examples/nginx/nginx.conf) with
[`examples/nginx-to-influxdb.yaml`](../examples/nginx-to-influxdb.yaml), and the demo's
[`demo/nginx/nginx.conf`](../demo/nginx/nginx.conf) and
[`demo/haproxy/haproxy.cfg`](../demo/haproxy/haproxy.cfg) with
[`demo/logit.yaml`](../demo/logit.yaml).

## The canonical fields

Send exactly these names, with exactly these raw values. `http_access` does everything in the
"What `http_access` does" column, so you don't need to do any of it server-side.

**The absent rule applies to every field:** `""`, `"-"`, and a JSON `null` all count as absent, so
an unset HAProxy `txn` var, an unpopulated nginx `$upstream_*` variable, and a JSON `null` read the
same. This is the rule [`docs/design/data-model.md`](design/data-model.md)'s well-known-attribute
table and `trace_context` already use. An absent field produces nothing: no default `url.scheme`,
no `user_agent.class` for a producer that doesn't log the header, and no fabricated route.

### Request identity

| Attribute | Send | What `http_access` does | Notes |
|---|---|---|---|
| `http.request.method` | string, raw | Kept if it is one of `CONNECT DELETE GET HEAD OPTIONS PATCH POST PUT QUERY TRACE`, compared case-sensitively (semconv's rule: `get` is not `GET`); otherwise rewritten to `_OTHER`, the raw value preserved as `http.request.method_original`. | The one pre-normalization value kept, because semconv asks for it. |
| `url.path` | string, raw request path | Capped (256 chars) and control-byte cleaned; classified into `http.route`. | Usually not sent directly — see Composites. |
| `url.query` | string, raw query string, with or without a leading `?` | Leading `?` stripped; the values of semconv's sensitive keys redacted to `REDACTED` *before* capping; capped (256 chars). | Redaction before capping so a secret can't survive by being cut mid-value. |
| `url.scheme` | string, `http`/`https` | Capped (16 chars). | |
| `network.protocol.version` | string, raw (`HTTP/1.1`, `2.0`, …) | `HTTP/` prefix stripped; `2.0`→`2`, `3.0`→`3` (semconv's spelling); anything else left as-is. Capped (8 chars). | |

### Response and sizes

Each of these is coerced to an integer from an integer, an integral float, or a quoted numeric
string. A value that doesn't parse is left exactly as it arrived and counted `invalid{field}`.

| Attribute | Notes |
|---|---|
| `http.response.status_code` | `"000"` becomes `0` — nginx's client-abort sentinel. Unparseable also raises a throttled `bad_status` diagnostic. |
| `http.response.body.size` | |
| `http.response.size` | Headers + body, when the server can give you that. |
| `http.request.body.size` | |
| `http.request.size` | |

### Durations and unit suffixes

The unit is always in the *name*, never a separate field: `_s` (decimal seconds), `_ms`, `_us`, or
**unsuffixed = integer nanoseconds** (Traefik's `Duration`, `data-model.md`'s unsuffixed
`span.duration`). Send one form per quantity. `http_access` converts whichever it finds into the
`_s` form (an `F64`) and removes the others. If you send several, `_s` wins, then `_ms`, `_us`,
and the unsuffixed form. The unsuffixed form must be an integer, bare or quoted: a fractional
nanosecond count is a producer bug and is counted invalid, not rounded.

| Quantity | Forms accepted | Written as |
|---|---|---|
| request duration | `http.request.duration_s` / `_ms` / `_us` / `http.request.duration` | `http.request.duration_s` |
| upstream total | `upstream.duration_s` / `_ms` / `_us` / `upstream.duration` | `upstream.duration_s` |
| upstream connect | `upstream.connect_s` / `_ms` / `_us` / `upstream.connect` | `upstream.connect_s` |
| upstream header | `upstream.header_s` / `_ms` / `_us` / `upstream.header` | `upstream.header_s` |

A value that doesn't parse in its named unit is left in place (with any other forms of that
quantity), counted `invalid{field}`, and raises a throttled `bad_duration` diagnostic. nginx's
per-attempt list (`0.004, 0.012` after a retry) is left verbatim and never counted invalid.

### Addresses and ports

| Attribute | Send | What `http_access` does |
|---|---|---|
| `server.address` | string | Capped (253 chars). |
| `server.port` | integer or numeric string | Coerced to an integer. |
| `client.address` | string | Capped (128 chars). Overwritten from the first hop of `http.request.header.x-forwarded-for` only under `forwarded: {trust: true}`. |
| `client.port` | integer or numeric string | Coerced to an integer. |
| `network.peer.address` | string | Capped (128 chars). |
| `network.peer.port` | integer or numeric string | Coerced to an integer. |

### Headers and `user.name`

| Attribute | Send | What `http_access` does |
|---|---|---|
| `user_agent.original` | string, raw header value | Classified into `user_agent.class` on the **uncapped** value (a spoofed user agent's identifying token is often at its tail), then capped (256 chars). Absent: no class. Present but `""`/`"-"`: class `none`. |
| `http.request.header.referer` | string, raw | Capped (256 chars). |
| `http.request.header.x-forwarded-for` | string, raw | Capped (128 chars). Its first comma-separated hop feeds `client.address` under `forwarded: {trust: true}` (off by default — the header is client-supplied). |
| `user.name` | string | Capped (128 chars). The authenticated principal, when the server exposes one. |

### Proxy fields

| Attribute | Send | What `http_access` does |
|---|---|---|
| `upstream.address` | string | Capped (128 chars). |
| `upstream.status` | integer, or nginx's attempt list (`502, 200` / `502 : 200`) | A single value is coerced to an integer. An attempt list is left verbatim and never counted invalid — it is normal traffic across a retry or internal redirect. |
| `upstream.{duration,connect,header}_*` | duration, above | Merged into the `_s` form. |
| `cache.status` | string | Capped (32 chars). |
| `network.connection.id` | integer or numeric string | Coerced to an integer. Omit it rather than fake one where the server has no per-connection id. |
| `network.connection.requests` | integer or numeric string | Coerced to an integer. |
| `http.termination_state` | string | Capped (8 chars). |
| `http.response.compression_ratio` | number or numeric string | Coerced to a float. Only log it when the response was actually compressed. |

### Composites

A server often logs one field where semconv wants several, so `logit` defines two composite names
of its own. `http_access` decomposes each composite into whichever atomic fields are **absent**,
then removes the composite. A composite never overrides an atomic field you also sent.

| Attribute | Send | What `http_access` does |
|---|---|---|
| `http.request.line` | `METHOD TARGET PROTOCOL` (nginx's `$request`, Apache's `%r`) | Split into `http.request.method`, `url.path`/`url.query` (target split at the first `?`), and `network.protocol.version`. A line that isn't exactly three space-separated tokens is left in place, counted `invalid{field}`, and raises a throttled `bad_request_line` diagnostic. |
| `url.original` | `/path?query` (nginx's `$request_uri`) | Split into `url.path`/`url.query` at the first `?`. **Always the raw request target, never a decoded path** — nginx: `$request_uri`, never `$uri`, which is normalized and can change mid-request (internal redirects, index files). |

### Derived fields, filled in only when absent

To tune how these are derived, see
[What `http_access` derives, and how to steer it](#what-http_access-derives-and-how-to-steer-it).

`http_access` writes these only when the line doesn't already carry them. If you send one
yourself (because your server computed it for its own reasons, or because you prefer your
router's real `http.route` to a regex bucket), it's honoured as-is. Leave it out and `http_access`
derives it. A derived value comes from config, a built-in table, or a closed numeric range, never
from a capture of the input, so it's bounded. A value you send is only as bounded as you made it;
put `keep_values` after this component if that matters. See "Doing some of it server-side" below.

| Attribute | Where it comes from |
|---|---|
| `http.route` | Your `routes:` rules and built-in sets, matched against the capped `url.path`; else `route_other`; else nothing. |
| `user_agent.class` | Your `user_agent_rules:`, then the built-in table, matched against the uncapped `user_agent.original`; else `other`. |
| `user_agent.synthetic.type` | `bot`, alongside a `crawler` or `scanner` class only. |
| `error.type` | The status as a decimal string, 5xx only. |
| `span.name` | `{method} {route}`, or the method alone when no route was written; `HTTP` stands in for `_OTHER` (`HTTP {route}`, `HTTP`). |
| `span.status` | `error` for a 5xx or a `0` status, `unset` otherwise — **never `ok`**, which semconv reserves for an explicit override. |
| `http.request.method_original` | The raw method, only when it fell outside the known set. |
| `span.duration_s` | Mirrored from `http.request.duration_s` **unless** the line already states a span duration (any `span.duration*`) or states both a start and an end. A lone end (nginx's `$msec`) or a lone start (HAProxy's `request_date(us)`) is exactly what the mirror is for: without it, `trace_context` would borrow the event's receipt time for the missing bound. |

Every row above is fill-only. An existing value is never recomputed, whether the producer logged
it or an upstream `set`/`lua` stage wrote it, and a present `user_agent.class` also suppresses the
`user_agent.synthetic.type` derivation. No option makes `http_access` overwrite them. The one
field it does replace is `client.address`, and only under `forwarded: {trust: true}` (see
[`forwarded:`](#forwarded)). Normalization of a value you *did* send (status to an integer, the
cap, the clean, the redaction) applies regardless: that's hardening, not overriding.

### Server variables at a glance

The per-server snippets below are complete; this is the lookup for the fields most pipelines need.
Caddy and Traefik columns are their fixed native keys, renamed in the pipeline.

| Attribute | nginx | Apache | HAProxy | Varnish | Squid | Envoy | Caddy | Traefik |
|---|---|---|---|---|---|---|---|---|
| `http.request.method` | `$request_method` | `%m` | `%HM` | `%m` | `%rm` | `%REQ(:METHOD)%` | `request.method` | `RequestMethod` |
| `url.original` | `$request_uri` | `%U%q` | `%HU` | `%U%q` | `%ru` | `%REQ(:PATH)%` | `request.uri` | `RequestPath` |
| `network.protocol.version` | `$server_protocol` | `%H` | `%HV` | `%H` | `HTTP/%rv` | `%PROTOCOL%` | `request.proto` | `RequestProtocol` |
| `http.response.status_code` | `$status` | `%>s` | `%ST` | `%s` | `%03>Hs` | `%RESPONSE_CODE%` | `status` | `DownstreamStatus` |
| request duration | `$request_time` → `_s` | `%D` → `_us` | `%Ta` → `_ms` | `%D` → `_us` | `%tr` → `_ms` | `%DURATION%` → `_ms` | `duration` → `_s` | `Duration` → unsuffixed |
| `server.address` | `$host` | `%{Host}i` | `req.hdr(host)` via var | `%{Host}i` | `%la` | `%REQ(:AUTHORITY)%` | `request.host` | `RequestHost` |
| `client.address` | `$remote_addr` | `%{c}a` | `%ci` | `%h` | `%>a` | `%DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT%` | `request.remote_ip` | `ClientHost` |
| `user_agent.original` | `$http_user_agent` | `%{User-Agent}i` | `req.hdr(user-agent)` via var | `%{User-agent}i` | `%{User-Agent}>h` | `%REQ(USER-AGENT)%` | `request.headers.User-Agent` | `request_User-Agent` |
| `traceparent` | `$http_traceparent` | `%{traceparent}i` | `req.hdr(traceparent)` via var | `%{traceparent}i` | `%{traceparent}>h` | `%REQ(TRACEPARENT)%` | `request.headers.Traceparent` | `request_Traceparent` |

## Trace and timing

`http_access` never touches `traceparent`, `trace.*`, `span.id`, `span.parent_id`, `span.kind`, or
any `span.start*`/`span.end*` attribute. Those belong to
[`data-model.md`](design/data-model.md)'s well-known table, and `trace_context` reads them exactly
as documented there, including its rule that only one form of a timing quantity may be present.

What you need to get right here is which end of the request each server's timestamp marks.
Sending an end as a start silently produces a span shifted by its own duration.

| Server | Start or end? | Timestamp | Log it as |
|---|---|---|---|
| nginx | **End** — `$msec` is taken at the log-write instant | `$msec` | `span.end_s` (quoted — see "Quoting rules") |
| Apache httpd | **Start** — `%{...}t` is "time the request was received" | `%{usec}t` | `span.start_us` |
| HAProxy | **Start** — `request_date(us)` is "the exact date when the first byte of the HTTP request was received" | `request_date(us)` | `span.start_us` — never `accept_date` (the TCP accept, not the request) |
| Varnish | **Start**, client mode — "time when the request was received" | `%{usec}t` | `span.start_us` |
| Squid | **End** — "the time when Squid started to log the transaction, which normally happens at the end of a transaction lifecycle" | `%ts.%03tu` | `span.end_s` |
| Caddy | **End** — source-verified: the access log is written after the response is handled | `ts` (fractional seconds) | `span.end_s` |
| Envoy | **Start** — `%START_TIME%` is request start, UTC by default | `%START_TIME(%Y-%m-%dT%H:%M:%S.%9fZ)%` | `span.start_rfc3339` |
| Traefik | **Start** — `StartUTC`, captured at the top of the access-log middleware | `StartUTC` | `span.start_rfc3339` |

Paired with the request duration, every row resolves to a full span through the
`span.duration_s` mirror, without logging a span duration separately: `(end − duration, end)` for
an end-stamped server, `(start, start + duration)` for a start-stamped one.

Two HAProxy rules matter more than the table:

- **Never send `accept_date`** as a request start — it's the TCP accept, not the request, and reads
  as an inflated duration once paired with `%Ta`. Use `request_date(us)`.
- **Leave `option logasap` off** on any frontend feeding this pipeline. It prefixes the duration
  and byte counts with `+` and emits them early; under `%{+json}o` the `+`-prefixed value is
  written un-encoded even under `:sint`, an invalid JSON token (`+123`) that breaks the line.

### Trace ids

**`logit` never mints a trace id.** A trace id has to come from the first hop that saw the
request:

- If a `traceparent` header arrived, log it verbatim under `traceparent`. `trace_context` takes the
  trace id, this line's parent span id, and the flags from it.
- If none arrived, this server is the edge: mint a trace id here and log it as `trace.id`.

nginx already mints a per-request id (`$request_id`, 32 hex characters). This `map` uses it as a
fallback while still reusing an inbound trace id:

```nginx
map $http_traceparent $trace_id {
    default $request_id;
    "~^00-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$" $1;
}
```

Log `$trace_id` under `trace.id` alongside the raw `traceparent`. The regex arm matters because
`trace_context` always prefers an explicit `trace.id` over the header's trace id: with only
`default $request_id`, every request would start a new trace even when an inbound one arrived.

If the tier also forwards a `traceparent` to its backend, it has to log the span id it put in that
header, as [`demo/nginx/nginx.conf`](../demo/nginx/nginx.conf)'s maps do.

## Quoting rules for nginx `escape=json`

`escape=json` escapes `"`, `\`, and control bytes correctly, but you decide which variables are
wrapped in `"..."`. Getting it wrong either breaks the line's JSON or loses precision:

- **Quote every variable that can be unset, empty, or non-numeric on some code path.** An
  `$upstream_*` variable renders `-` or `""` on a non-proxied vhost, and a bare `-` is not valid
  JSON.
- **Always quote `$status`.** nginx can log a literal `000` on an abnormal termination
  ([trac #1415](https://trac.nginx.org/nginx/ticket/1415),
  [#2623](https://trac.nginx.org/nginx/ticket/2623)). An unquoted `000` is a leading-zero token,
  **not valid JSON**, and the whole line is lost. `http_access` turns the quoted `"000"` into `0`.
- **Always quote `$msec`.** Unquoted, it round-trips through an `f64` and loses nginx's exact
  millisecond digits at epoch magnitude. Quoted, `trace_context` parses it digit-exact.
- **Leave bare only fields that are always well-formed numbers** — `$body_bytes_sent`,
  `$bytes_sent`, `$request_length`, `$request_time`, `$connection`, `$connection_requests`,
  `$server_port`.
- **Send `$request_uri` for `url.original`, never `$uri`.** `$uri` is normalized and can change
  mid-request; `$request_uri` is the untouched request target with its query string.
- **Set `invalid_utf8: replace` on the `json` component ahead of `http_access`.** `escape=json`
  passes bytes `>= 0x80` through raw, so a client sending a Latin-1 header puts invalid UTF-8 into
  an otherwise valid-looking line. `syslog_in` still accepts it (its message becomes raw bytes),
  but `json`'s strict parse fails the whole line. `http_access` can't recover from that failure,
  because no attributes are left for it to normalize. Under `replace`, a parse that failed on
  invalid UTF-8 is retried on a copy with every invalid sequence replaced by U+FFFD, and a
  well-formed line's cost is unchanged. The default is `reject`.

## Emitters that can't write dotted keys

**Every canonical name in this doc is also accepted with each `.` replaced by `-`, and only the
`.`.** Underscores stay where they are:

- `http-request-method`, `url-path`, `http-response-status_code`, `user_agent-original`,
  `span-start_us`, and `http-request-duration_ms`.
- `http-request-header-x-forwarded-for`: the header key keeps its own dashes.

This exists for HAProxy's own JSON encoder (`%{+json}o`, HAProxy 3.0+), which names each item with
`%(name)`. That name grammar accepts only `[A-Za-z0-9_-]` and rejects a literal `.` (verified
against HAProxy's `src/log.c` name parser). The alias uses `-`, not `_`, because canonical names
already contain `_` (`user_agent.original`, `span.start_us`): an underscore spelling couldn't be
decoded back to the dotted name by rule, and the dashed one can, by eye.

How the aliases behave:

- **The alias table is fixed.** It covers every name `http_access` reads or writes, plus the
  trace/timing names (`trace-id`, `trace-flags`, `span-id`, `span-parent_id`, `span-kind`,
  `span-end_s`, …), because `http_access` runs before `trace_context` and `trace_context` only
  reads dotted names. It's never a blanket `-`→`.` rewrite, which would corrupt a legitimately
  dashed key.
- **Renaming happens first.** `http_access` renames each dashed key to its dotted spelling before
  anything else runs. If both spellings arrive, the dotted one wins, and the dashed key is removed
  either way.
- **A name with no dots needs no alias**, for example `traceparent`.
- **Your own extra fields have no alias entry.** Give a non-canonical field an underscore name
  (`haproxy_timer_queue_ms`), never a dash standing in for a dot, so it can't collide with a
  future canonical name.

Worked example, trimmed from [`demo/haproxy/haproxy.cfg`](../demo/haproxy/haproxy.cfg):

```haproxy
frontend fe_http
    bind :8080
    mode http
    option httplog
    # Do NOT set `option logasap` -- see "Trace and timing".

    http-request set-var(txn.host) req.hdr(host)
    http-request set-var(txn.path) path
    http-request set-var(txn.query) query
    http-request set-var(txn.ua) req.hdr(user-agent)
    acl has_tp req.hdr(traceparent) -m found
    http-request set-var(txn.inbound_tp) req.hdr(traceparent) if has_tp

    log-format '%{+json}o %(server-address)[var(txn.host)] %(url-path)[var(txn.path)] %(url-query)[var(txn.query)] %(user_agent-original)[var(txn.ua)] %(http-request-method)HM %(http-response-status_code:sint)ST %(http-request-duration_ms:sint)Ta %(client-address)ci %(traceparent)[var(txn.inbound_tp)] %(span-start_us:sint)[request_date(us)]'
```

`%(http-request-method)HM` names the item `http-request-method` and fills it from the `%HM`
alias. `http_access` renames it to `http.request.method` before doing anything else, so everything
downstream, `trace_context` included, only ever sees dotted names.

## Per-server snippets

Each directive below was checked against the vendor's own docs or source, and a few
lower-confidence items are called out inline. The nginx and HAProxy formats are the ones this repo
ships: they were checked with `nginx -t`/`haproxy -c` against the real images and verified with
live lines. The others haven't been run against a live instance, so treat "Cannot provide" as
researched, not measured, and check anything that matters to you against your deployed version.

Each server's section ends with the same three notes: what it **cannot provide**, which end of the
request its **timestamp** marks (see [Trace and timing](#trace-and-timing)), and its **gotchas**.

### nginx

```nginx
log_format access_semconv escape=json
    '{"http.request.method":"$request_method",'
    '"url.original":"$request_uri",'
    '"url.scheme":"$scheme",'
    '"network.protocol.version":"$server_protocol",'
    '"http.response.status_code":"$status",'
    '"http.response.body.size":$body_bytes_sent,'
    '"http.response.size":$bytes_sent,'
    '"http.request.size":$request_length,'
    '"http.request.duration_s":$request_time,'
    '"server.address":"$host",'
    '"server.port":$server_port,'
    '"client.address":"$remote_addr",'
    '"network.peer.port":"$remote_port",'
    '"user_agent.original":"$http_user_agent",'
    '"http.request.header.referer":"$http_referer",'
    '"http.request.header.x-forwarded-for":"$http_x_forwarded_for",'
    '"user.name":"$remote_user",'
    '"network.connection.id":$connection,'
    '"network.connection.requests":$connection_requests,'
    '"upstream.address":"$upstream_addr",'
    '"upstream.status":"$upstream_status",'
    '"upstream.duration_s":"$upstream_response_time",'
    '"upstream.connect_s":"$upstream_connect_time",'
    '"upstream.header_s":"$upstream_header_time",'
    '"cache.status":"$upstream_cache_status",'
    '"traceparent":"$http_traceparent",'
    '"span.end_s":"$msec"}';

access_log syslog:server=logit:5140,tag=nginx_access,nohostname access_semconv;
```

Optionally add `"http.response.compression_ratio":"$gzip_ratio"`, quoted because it's empty unless
that response was actually gzip-compressed. `$remote_port` is quoted because it's empty for a
UNIX-socket listener.

**Cannot provide:** `http.termination_state` (no analog).

**Timestamp:** end — `$msec`, paired with `$request_time`.

**Gotchas:** quote `$status` (`000`); `$request_uri`, never `$uri`; `$upstream_*` render `-`/`""`
on a non-proxied vhost and become an attempt list on a retry; `$http_x_forwarded_for` is the raw
header, because nginx does no `X-Forwarded-For` trust logic of its own.

### Apache httpd 2.4

`mod_log_config` has no JSON formatter, so the JSON is assembled by hand:

```apache
# mod_log_config is compiled in by default; mod_logio is NOT (needed for %I/%O, omitted below).
LogFormat "{\"http.request.method\":\"%m\",\"url.original\":\"%U%q\",\"url.scheme\":\"%{REQUEST_SCHEME}e\",\"network.protocol.version\":\"%H\",\"http.response.status_code\":%>s,\"http.response.body.size\":%B,\"http.request.duration_us\":%D,\"server.address\":\"%{Host}i\",\"server.port\":%{local}p,\"client.address\":\"%{c}a\",\"network.peer.port\":%{remote}p,\"user_agent.original\":\"%{User-Agent}i\",\"http.request.header.referer\":\"%{Referer}i\",\"http.request.header.x-forwarded-for\":\"%{X-Forwarded-For}i\",\"user.name\":\"%u\",\"network.connection.requests\":%k,\"traceparent\":\"%{traceparent}i\",\"span.start_us\":\"%{usec}t\"}" access_semconv

CustomLog "logs/access_semconv.log" access_semconv
```

Use `%>s` (final status after internal redirects), not `%s`. `%U` honours `MergeSlashes` (on by
default since 2.4.6). If the path must be byte-exact, send `%r` as `http.request.line` instead of
`%U%q` as `url.original`, because `%r` is exempt. `%{REQUEST_SCHEME}e` is lower-confidence
(verified indirectly, not on `mod_log_config`'s own page).

**Cannot provide (without non-default modules):** `http.response.size`/`http.request.size` (need
`mod_logio`'s `%O`/`%I`); `network.connection.id`; `upstream.*`/`cache.status`;
`http.termination_state`; `http.response.compression_ratio` (only via `mod_deflate`'s
`DeflateFilterNote`). **No syslog transport for access logs:** `CustomLog` takes a file or a
pipe, so point `tail_in` at the file, or pipe to `logger`.

**Timestamp:** start — `%{usec}t`.

**Gotchas:** JSON validity is **not** guaranteed: Apache escapes non-printable bytes as `\xhh`,
which is not a JSON escape, so a header or URL byte outside printable ASCII can produce an
invalid line. A header the client didn't send generally logs `-`, which the absent rule already
covers.

### HAProxy 3.x

Checked against HAProxy 3.4's `configuration.txt` and `src/log.c`; `%{+json}o` needs 3.0 or later.

```haproxy
frontend fe_http
    bind :8080
    mode http
    option httplog
    # Do NOT set `option logasap`.

    http-request set-var(txn.host) req.hdr(host)
    http-request set-var(txn.scheme) ssl_fc,iif(https,http)
    http-request set-var(txn.ua) req.hdr(user-agent)
    http-request set-var(txn.referer) req.hdr(referer)
    http-request set-var(txn.xff) req.hdr(x-forwarded-for)
    acl has_tp req.hdr(traceparent) -m found
    http-request set-var(txn.tp) req.hdr(traceparent) if has_tp

    log-format '%{+json}o %(http-request-method)HM %(url-original)HU %(url-scheme)[var(txn.scheme)] %(network-protocol-version)HV %(http-response-status_code:sint)ST %(http-response-size:sint)B %(http-request-size:sint)U %(http-request-duration_ms:sint)Ta %(server-address)[var(txn.host)] %(server-port:sint)fp %(client-address)ci %(network-peer-port:sint)cp %(user_agent-original)[var(txn.ua)] %(http-request-header-referer)[var(txn.referer)] %(http-request-header-x-forwarded-for)[var(txn.xff)] %(upstream-address)si %(upstream-connect_ms:sint)Tc %(upstream-header_ms:sint)Tr %(http-termination_state)tsc %(traceparent)[var(txn.tp)] %(span-start_us:sint)[request_date(us)]'
```

Log-format aliases (`%HM`, `%HU`, `%ST`, `%B`, `%Ta`, `%ci`, `%si`, `%tsc`, …) work directly inside
a named item. A `log-format` rejects a bracketed `[expr]` fetch of a request header ("may not be
reliably used here"), so Host, User-Agent, Referer, `X-Forwarded-For` (XFF), `traceparent`, and
the scheme are captured into `txn` vars first. `%si` is the backend IP HAProxy connected to; `%s`
would be the configured server *name*.

**Cannot provide:** `http.response.body.size` (`%B` is total bytes, headers included);
`upstream.status` (indistinguishable from `%ST`, which may be HAProxy's own status);
`upstream.duration_*` as one field (only `%Tc` connect and `%Tr` header exist); `cache.status`;
`network.connection.id`/`.requests` (`%ID` is per-transaction); `http.response.compression_ratio`;
`user.name` (no dedicated fetch).

**Timestamp:** start — `request_date(us)`.

**Gotchas:** `option logasap` must stay off; every item must be *named* for `+json` to include it;
`%ST` has no `000` sentinel and is safe as `:sint`.

### Varnish

Checked against `varnishncsa`'s reference and source.

```sh
varnishncsa -g request -j -F '{"http.request.method":"%m","url.original":"%U%q","network.protocol.version":"%H","http.response.status_code":%s,"http.response.size":%O,"http.response.body.size":%b,"http.request.size":%I,"http.request.duration_us":%D,"client.address":"%h","network.peer.port":%{VSL:ReqStart[2]}x,"server.address":"%{Host}i","user_agent.original":"%{User-agent}i","http.request.header.referer":"%{Referer}i","http.request.header.x-forwarded-for":"%{X-Forwarded-For}i","cache.status":"%{Varnish:handling}x","upstream.address":"%{VSL:BackendOpen[2]}x","upstream.status":%{VSL:BerespStatus}x,"traceparent":"%{traceparent}i","span.start_us":%{usec}t}'
```

Real upstream timing needs a second, backend-mode instance (Varnish's own documented pattern),
because client-mode `Varnish:time_firstbyte` is end-to-end time to first byte and is populated on
cache hits too:

```sh
varnishncsa -b -g request -j -F '{"upstream.header_us":%{Varnish:time_firstbyte}x,"upstream.duration_us":%D}'
```

`-j` makes missing values JSON-safe placeholders, which keeps the bare numeric fields valid.
`%{usec}t` is microseconds since the epoch, despite the Varnish docs' prose calling it milliseconds
(checked in `format_time()`'s source).

**Cannot provide:** `upstream.connect_*`; `server.port`; `network.connection.id`/`.requests`
(`Varnish:vxid` is per-transaction); `http.response.compression_ratio`.

**Timestamp:** start, client mode — `%{usec}t`.

**Gotchas:** a VSL buffer overrun under sustained load silently drops a *gap* of lines, with no
backpressure and no retry. Pass `-g request` explicitly, because the compiled-in default is `vxid`.

### Squid

```
logformat logit_semconv {"http.request.method":"%rm","url.original":"%\"ru","network.protocol.version":"HTTP/%rv","http.response.status_code":"%\"03>Hs","http.response.size":%<st,"http.request.size":%>st,"http.request.duration_ms":%tr,"span.end_s":"%ts.%03tu","server.address":"%la","server.port":%lp,"client.address":"%\">a","network.peer.port":%>p,"user_agent.original":"%\"{User-Agent}>h","http.request.header.referer":"%\"{Referer}>h","http.request.header.x-forwarded-for":"%\"{X-Forwarded-For}>h","user.name":"%\"un","traceparent":"%\"{traceparent}>h","cache.status":"%\"Ss","upstream.address":"%\"<A","upstream.status":"%\"<Hs","upstream.duration_ms":"%\"<pt"}

access_log daemon:/var/log/squid/access.log logformat=logit_semconv
```

`%\"` is Squid's quoted-string encoding, **not** a full JSON escaper. Declare `logformat` before
the `access_log` line that uses it.

**Cannot provide:** `url.scheme` (no dedicated code); `http.response.body.size` (only `%<st`,
headers + body); separate `upstream.connect_*`/`upstream.header_*` (only one peer timer, `%<pt`);
`http.termination_state`; `http.response.compression_ratio`; `network.connection.id`/`.requests`.

**Timestamp:** end — `%ts.%03tu`, quoted for the same reason as nginx's `$msec`.

**Gotchas:** no JSON escaping mode, so an unescaped control byte in a header or URL can still break
the line; `%ru` is sanitized, not byte-identical to the wire; the status is zero-padded (`000` for
"no response"), so keep it quoted; `%rv` has no `HTTP/` prefix, so type it literally; many codes log
`-` for "not applicable".

### Envoy

```yaml
access_log:
  - name: envoy.access_loggers.file
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog
      path: "/dev/stdout"
      log_format:
        typed_json_format:
          "http.request.method": "%REQ(:METHOD)%"
          "url.original": "%REQ(:PATH)%"
          "url.scheme": "%REQ(:SCHEME)%"
          "network.protocol.version": "%PROTOCOL%"
          "server.address": "%REQ(:AUTHORITY)%"
          "server.port": "%DOWNSTREAM_LOCAL_PORT%"
          "client.address": "%DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT%"
          "network.peer.port": "%DOWNSTREAM_REMOTE_PORT%"
          "user_agent.original": "%REQ(USER-AGENT)%"
          "http.request.header.referer": "%REQ(REFERER)%"
          "http.request.header.x-forwarded-for": "%REQ(X-FORWARDED-FOR)%"
          "traceparent": "%REQ(TRACEPARENT)%"
          "http.response.status_code": "%RESPONSE_CODE%"
          "http.request.body.size": "%BYTES_RECEIVED%"
          "http.response.body.size": "%BYTES_SENT%"
          "http.request.duration_ms": "%DURATION%"
          "span.start_rfc3339": "%START_TIME(%Y-%m-%dT%H:%M:%S.%9fZ)%"
          "upstream.address": "%UPSTREAM_HOST%"
          "upstream.duration_ms": "%RESP(X-ENVOY-UPSTREAM-SERVICE-TIME)%"
          "http.termination_state": "%RESPONSE_FLAGS%"
          "network.connection.id": "%CONNECTION_ID%"
        omit_empty_values: true
```

Ports, `%CONNECTION_ID%`, and every `%REQ`/`%RESP` header stay JSON strings even under
`typed_json_format`; `http_access` coerces the numeric ones.

**Cannot provide:** `user.name`; `network.connection.requests`; `cache.status`;
`http.response.compression_ratio`; `upstream.status` distinct from `%RESPONSE_CODE%`;
`http.response.size` as one field (body and headers are separate operators).

**Timestamp:** start — `%START_TIME%`, UTC by default.

**Gotchas:** `%DURATION%` runs to the last byte out, response body included; `%RESPONSE_FLAGS%`
is a short code (`UH`, `UF`, `NR`, …) and needs its own table if you want it decoded.

### Caddy

Caddy's JSON access log has a **fixed, nested schema**: no directive renames or flattens its keys
(checked against `caddyserver.com/docs/logging` and Caddy's `server.go`/`marshalers.go`). The
server config only turns the JSON log on:

```
example.com {
    log {
        output file /var/log/caddy/access.log
        format json
    }
}
```

It writes lines like this:

```json
{
  "ts": 1646861401.5241024,
  "request": {
    "remote_ip": "127.0.0.1", "remote_port": "41342", "proto": "HTTP/2.0",
    "method": "GET", "host": "example.com", "uri": "/",
    "headers": { "User-Agent": ["curl/7.82.0"] }
  },
  "bytes_read": 0, "duration": 0.000929675, "size": 10900, "status": 200
}
```

So the pipeline renames instead:

1. `json` decodes the object.
2. `flatten` dot-joins it: `request.method`, and `request.headers.User-Agent.0`, since every header
   value is an array.
3. A `lua` stage moves each field onto its canonical name. Writing `nil` stores a null, which
   `http_access` treats as absent and a later `keep` removes.

A `lua` stage is needed because `logit` has no native rename component: `set` only stamps
constants, and the old `rename` kind was retired in favour of Lua
([ADR `routing-by-condition-is-lua`](adr/routing-by-condition-is-lua.md)).

```yaml
components:
  caddy_in:
    type: tail_in
    paths: ["/var/log/caddy/access.log"]

  caddy_json:
    type: json
    sources: [caddy_in]
    invalid_utf8: replace

  caddy_flat:
    type: flatten
    sources: [caddy_json]

  caddy_names:
    type: lua
    sources: [caddy_flat]
    script: |
      local names = {
        ["request.method"] = "http.request.method",
        ["request.uri"] = "url.original",
        ["request.proto"] = "network.protocol.version",
        ["request.host"] = "server.address",
        ["request.remote_ip"] = "client.address",
        ["request.remote_port"] = "network.peer.port",
        ["request.headers.User-Agent.0"] = "user_agent.original",
        ["request.headers.Referer.0"] = "http.request.header.referer",
        ["request.headers.Traceparent.0"] = "traceparent",
        ["status"] = "http.response.status_code",
        ["size"] = "http.response.body.size",
        ["duration"] = "http.request.duration_s",
        ["ts"] = "span.end_s",
      }
      function process(event)
        for from, to in pairs(names) do
          local value = event.attributes[from]
          if value ~= nil then
            event.attributes[to] = value
            event.attributes[from] = nil
          end
        end
        return event
      end

  caddy_http:
    type: http_access
    sources: [caddy_names]
    routes:
      - builtin: probes
      - builtin: assets
    route_other: /{other}

  caddy_trace:
    type: trace_context
    sources: [caddy_http]
    span:
      kind: server
      mint_id: true

  caddy_out:
    type: stdio_out
    sources: [caddy_trace]
```

`duration` is already seconds, so it maps straight onto `http.request.duration_s`. `request.uri`
is Go's raw `RequestURI`, query included. A Lua stage costs a VM per worker and a few allocations
per event (`docs/design/memory.md`): the price of a server whose key names can't be chosen.

**Cannot provide:** `cache.status` (no built-in cache); `server.address`/`server.port` as the bind
address (`request.host` is the client's `Host` header); upstream fields at all, unless each route
adds them with `log_append` (`{http.reverse_proxy.upstream.address}`,
`{http.reverse_proxy.upstream.duration_ms}`, `{http.reverse_proxy.upstream.latency_ms}` →
`upstream.address`, `upstream.duration_ms`, `upstream.header_ms`). No connect-only timer or
upstream-status placeholder was found.

**Timestamp:** end — source-verified; `ts` is written after the response is handled.

**Gotchas:** every request header is logged by default, not an allow-list — beyond Caddy's small
`Cookie`/`Authorization` redaction list, that's real over-logging. The schema has changed across
v2 releases; check it against your version.

### Traefik (v3.x)

Traefik has fixed key names like Caddy, but its JSON is flat, so the pipeline is `json` plus a
`lua` rename with no `flatten`:

```yaml
accessLog:
  format: json
  fields:
    defaultMode: keep
    headers:
      defaultMode: drop
      names:
        User-Agent: keep
        Referer: keep
        X-Forwarded-For: keep
        Traceparent: keep
```

A kept header is logged as `request_<Canonical-Name>` (checked in `pkg/middlewares/accesslog`).
The rename stage is the Caddy one with a different table and no `flatten`:

```lua
local names = {
  RequestMethod = "http.request.method",
  RequestPath = "url.original",            -- path AND query, despite the name
  RequestScheme = "url.scheme",
  RequestProtocol = "network.protocol.version",
  RequestHost = "server.address",
  ClientHost = "client.address",
  ClientPort = "network.peer.port",
  DownstreamStatus = "http.response.status_code",
  RequestContentSize = "http.request.body.size",
  DownstreamContentSize = "http.response.body.size",
  ServiceAddr = "upstream.address",
  Duration = "http.request.duration",      -- unsuffixed: integer nanoseconds
  OriginDuration = "upstream.duration",
  StartUTC = "span.start_rfc3339",
  GzipRatio = "http.response.compression_ratio",
  ["request_User-Agent"] = "user_agent.original",
  ["request_Referer"] = "http.request.header.referer",
  ["request_X-Forwarded-For"] = "http.request.header.x-forwarded-for",
  ["request_Traceparent"] = "traceparent",
}
```

`Duration`/`OriginDuration` are raw int64 nanoseconds (Go's `time.Duration` marshalled as a
number, not a `"1.2ms"` string), so they go under the unsuffixed names. They stay integers across
the Lua hop: any value below 2^53 is an exact Lua integer, and a request would have to take more
than 100 days to exceed it.

**Cannot provide:** `network.connection.id`/`.requests` (`RequestCount` is a global counter);
separate `upstream.connect_*`/`upstream.header_*` (only `OriginDuration`); `cache.status`.
`traceparent`, the user agent, referer, and XFF are opt-in via `headers.names`, not gaps.

**Timestamp:** start — `StartUTC`.

**Gotchas:** headers are off by default; `RequestHost`/`RequestPort` come from the `Host` header,
not the entrypoint's bind address.

## What `http_access` derives, and how to steer it

### `routes:`

An ordered list of rules over the *capped* `url.path`; the first match wins. Each entry is exactly
one of:

- a named built-in set (`builtin:`), or
- a `match` regex with a literal `route` value. The route is never a capture, so every `http.route`
  comes from config or the table below. A `match` is unanchored unless it anchors itself.

```yaml
routes:
  - builtin: probes
  - builtin: well_known
  - match: '^/api/v[0-9]+/'
    route: /api
  - builtin: assets
```

| Built-in set | Route value | Case | Pattern |
|---|---|---|---|
| `probes` | `/{probe}` | sensitive | `^/(-/(healthy\|ready)\|health\|healthz\|healthcheck\|livez\|readyz\|ready\|ping\|status\|_status\|up\|metrics\|_metrics\|stats\|nginx_status\|server-status\|haproxy_status\|version\|_version)/?$` |
| `well_known` | `/{well-known}` | sensitive | `^/(\.well-known/.*\|robots\.txt\|favicon\.ico\|sitemap[^/]*\.xml(\.gz)?\|humans\.txt\|security\.txt\|apple-touch-icon[^/]*\.png\|browserconfig\.xml\|manifest\.json\|manifest\.webmanifest\|crossdomain\.xml\|ads\.txt\|app-ads\.txt)$` |
| `assets` | `/{asset}` | insensitive (`(?i)`) | `\.(css\|js\|mjs\|cjs\|map\|png\|jpe?g\|gif\|webp\|avif\|svg\|ico\|bmp\|tiff?\|woff2?\|ttf\|otf\|eot\|heic\|heif\|docx\|xlsx\|pptx\|apk\|ipa\|mp4\|m4v\|webm\|mov\|mp3\|m4a\|ogg\|oga\|opus\|wav\|flac\|pdf\|zip\|gz\|tgz\|bz2\|xz\|7z\|rar\|wasm)$` |

- **Order sets priority.** Built-ins expand in place, so a rule's position in the list sets its
  priority. Each `builtin:` may appear once.
- **Case.** `probes` and `well_known` are case-sensitive because those paths are protocol- or
  convention-mandated literals (`robots.txt` is lowercase by spec, `/healthz` by Kubernetes
  convention), so `/ROBOTS.TXT` is a different, usually 404, route. A file extension's case carries
  no meaning, so `assets` ignores it.
- **`assets` deliberately excludes `json`/`xml`/`txt`/`csv`.** Those are routinely API responses,
  and routing one to `/{asset}` would hide it.

`route_other: /{other}` (or any literal) is what a path gets when nothing matches. Without it, the
path gets no route and `span.name` is the method alone.

### `user_agent_rules:`

Extra classes, tried in order **before** the built-in table. You can pre-empt the built-in table,
but you can't disable it:

```yaml
user_agent_rules:
  - match: '(?i)internal-healthcheck'
    class: internal
```

A `class` of `crawler` or `scanner` also writes `user_agent.synthetic.type: bot`, as the built-ins
do. The built-in table is scanned in priority order, first match wins, all case-insensitive:

| Class | Pattern |
|---|---|
| `scanner` | `nmap\|masscan\|zgrab\|nikto\|sqlmap\|dirbuster\|gobuster\|ffuf\|fuzz faster u fool\|feroxbuster\|wpscan\|nuclei\|acunetix\|nessus\|qualys\|openvas\|censysinspect\|internetmeasurement\|expanse\|paloaltonetworks\|leakix\|shodan` |
| `tool` | `curl/\|wget/\|libwww-perl\|python-requests\|python-urllib\|aiohttp\|httpie\|go-http-client\|okhttp\|apache-httpclient\|^java/\|axios/\|node-fetch\|guzzlehttp\|postmanruntime\|insomnia\|reqwest/\|k6/\|wrk/\|jmeter\|kube-probe\|prometheus/\|blackbox-exporter\|elb-healthchecker\|googlehc\|telegraf/\|vector/\|chrome-lighthouse\|uptimerobot` |
| `crawler` | `\bbot\b\|bot/\|spider\|crawler\|slurp\|scrapy\|googlebot\|bingbot\|yandex\|baiduspider\|duckduckbot\|facebookexternalhit\|twitterbot\|linkedinbot\|slackbot\|applebot\|petalbot\|semrushbot\|ahrefsbot\|mj12bot\|ccbot\|gptbot\|chatgpt-user\|claudebot\|perplexitybot\|amazonbot\|bytespider\|feedfetcher` |
| `browser` | `mozilla/\|opera/\|dalvik/\|safari/\|msie \|trident/` |

`browser` is last because crawlers routinely claim `Mozilla/`. A user agent (UA) matching nothing
is `other`.

**Scanners that spoof a browser by default classify `browser`.** nikto 2.6.1+ defaults to a
browser UA, and nuclei and Nessus do the same, so the `scanner` row catches only a scanner that
identifies itself.

The tables were checked against a corpus of 62 real, sourced user-agent strings and 18 real paths.
Choices worth knowing:

- `\bbot\b` rather than `bot\b`, which would match phone models like `CUBOT`.
- Bare `bot/` alongside it, to catch `DotBot/1.2`, `Discordbot/2.0`, and other `…Bot/` tokens with
  no word boundary before them (see `docs/known-gaps.md` for the trade-off).
- `uptimerobot` in `tool`, both because it's a synthetic monitor and because `bot/` would otherwise
  catch `UptimeRobot/2.0`.
- `yandex` rather than `yandexbot`, since most of Yandex's robots carry no `bot` token.
- `^java/` anchored, because Java's `HttpURLConnection` sends exactly `Java/<version>`.
- `fuzz faster u fool`, because ffuf's default UA contains no "ffuf".

### `max_length:`

Per-field character caps, overriding these defaults (`logit-config`'s `CAPPED_FIELDS`):

| Field | Default cap (characters) |
|---|---|
| `url.path` | 256 |
| `url.query` | 256 |
| `user_agent.original` | 256 |
| `http.request.header.referer` | 256 |
| `server.address` | 253 |
| `client.address` | 128 |
| `network.peer.address` | 128 |
| `http.request.header.x-forwarded-for` | 128 |
| `upstream.address` | 128 |
| `user.name` | 128 |
| `http.request.method_original` | 32 |
| `cache.status` | 32 |
| `url.scheme` | 16 |
| `network.protocol.version` | 8 |
| `http.termination_state` | 8 |

```yaml
max_length:
  url.path: 512
  user_agent.original: 512
```

A key not in this table, or a limit of `0`, is a config error (graph rule 60). A cap cuts at a
character boundary, so a capped value stays valid UTF-8. After capping, any control byte (`< 0x20`,
`0x7F`) left in the value becomes `_`. A value that arrived as raw, non-UTF-8 bytes is capped in
bytes instead.

### `redact_query:`

Extra `url.query` keys, beyond semconv's seven (`AWSAccessKeyId`, `Signature`, `sig`,
`X-Goog-Signature`, `X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`), whose values
become `REDACTED`. Matched ASCII-case-insensitively:

```yaml
redact_query: [session_token, api_key]
```

### `forwarded:`

```yaml
forwarded: {trust: true}
```

Off by default. When present, `client.address` is overwritten with the first comma-separated hop
of `http.request.header.x-forwarded-for`. That header is client-supplied, so only turn this on
when every request reaches this server through a proxy you control that sets it. There is no
trusted-proxy list or hop count (see [What it does not do](#what-it-does-not-do)).

`{trust: false}` is rejected. To turn trust off, omit the block, so there's one spelling of "off".

## Bounding cardinality

`http_access` bounds what it *derives*: `http.route`, `user_agent.class`, `span.name`,
`span.status`, and `error.type` only ever take values from your config, a built-in table, or a
closed range, so they can go straight into an `aggregate` series key or a span name.

It doesn't bound what it only caps. `server.address`, `client.address`, `upstream.address`,
`cache.status`, and the rest are bounded in *length*, not in *value*. A `Host` header is
attacker-controlled; left unclamped, a stuffed one becomes its own series. Clamping a value set is
`keep_values`' job
([ADR `value-allowlist-cardinality-clamp`](adr/value-allowlist-cardinality-clamp.md)), exactly as
in [The pipeline](#the-pipeline) above:

```yaml
access_bounded:
  type: keep_values
  sources: [access_keep]
  attributes:
    server.address:
      normalize: [lower]
      allow: [app1.internal, app2.internal]
      other: other
```

`keep` still goes immediately before `aggregate`, because `aggregate`'s `SeriesKey` includes every
attribute: whatever survives `keep` sets both series cardinality and per-window memory. Keep
`url.path`, `client.address`, and every `user_agent.original`-style free-text field out of it.

## Metrics

`http_access` emits no metrics itself. It guarantees the attribute names that semconv's
`http.server.request.duration` and its low-cardinality attribute set expect, and a stock
`kv_metrics` + `keep` pair produces the metric:

```yaml
access_metrics:
  type: kv_metrics
  sources: [access_trace]
  distributions:
    - name: http.server.request.duration
      field: http.request.duration_s
      unit: s
    - name: http.server.request.body.size
      field: http.request.body.size
      unit: By
    - name: http.server.response.body.size
      field: http.response.body.size
      unit: By

access_keep:
  type: keep
  sources: [access_metrics]
  fields:
    - http.request.method
    - http.response.status_code
    - http.route
    - url.scheme
    - network.protocol.version
    - server.address
    - user_agent.class
```

`user_agent.class` isn't in semconv's set, but when `http_access` derives it, its values come from
the table, so adding it costs at most a handful of series per route. A class you send yourself is
only as bounded as you made it. [`examples/nginx-to-influxdb.yaml`](../examples/nginx-to-influxdb.yaml)
runs this shape end to end.

## Cutover

Add the new line alongside the old one and keep the old one until the new path is trusted:

```nginx
access_log syslog:server=logit:5140,tag=nginx_access,nohostname access_semconv;
access_log /var/log/nginx/access.log combined;   # unchanged, kept during cutover
```

Start `logit` and wait until it's listening before pointing the server at it. UDP is
fire-and-forget, so a line sent before the listener is bound is lost with no error anywhere:

```sh
until logit ready --admin http://logit-host:9600; do sleep 0.5; done
```

If `admin:` isn't configured, `docs/deploying.md`'s "The ordering rule" and "Probes and exit
codes" sections cover the fallback.

Once traffic is flowing, watch these:

- **`logit.transform.http_access.routed{outcome}`**: `rule`, `builtin`, `other` (fell through to
  `route_other`), `none` (no rule and no `route_other`), or `kept` (the line already carried its
  own `http.route`, so no rule ran; not a gap). A large or climbing `other`/`none`
  share means your `routes:` list is missing a real route. Aim for a trickle of genuinely
  unclassifiable paths.
- **`logit.transform.http_access.invalid{field}`**: a field that arrived but didn't parse. Beyond
  an occasional blip, a steady count on one `field` means that key is misnamed or mistyped in the
  log format. The throttled diagnostics `bad_request_line`, `bad_status`, and `bad_duration`
  (`logit.component.diagnostics{key}`) point at the same problems.
- **`logit.component.diagnostics{key="invalid_utf8"}` on the `json` component**: how often
  `invalid_utf8: replace` actually rescued a line. `key="parse_failure"` is a line lost anyway.

The full surface (`.normalized{field}`, `.derived{field}`, `.truncated{field}`, `.cleaned{field}`,
`.invalid{field}`, `.redacted`, `.routed{outcome}`) is in
[`docs/design/internal-telemetry.md`](design/internal-telemetry.md). Every tag comes from a closed
table, never from a value. An absent field, an unknown method, an unclassifiable user agent, and
an unrouted path are normal traffic: they get counters only, never a diagnostic.

Drop the old line once the new one lands where you expect and these counters look sane.

## Doing some of it server-side

**Rule of thumb: compute anything you want to *act on* at the server, and log it under the
standard name. Leave anything you only want in the data to the pipeline.**

A web server that classifies a request itself can act on the result before the line is ever
logged: rate-limit by `user_agent.class`, route probes to a cheaper upstream, or refuse a path
bucket outright. Do whichever parts of the work you want on the server, log the result under the
standard name, and `http_access` fills in the rest and hardens what you sent.

The list below is the shape of that work, in the order the component applies it, so you can pick
the pieces worth doing at the edge. An exact match is rarely the goal, but if you want one, the
exact rules live in `crates/logit-transforms/src/http_access.rs` and its tests. The built-in tables
and the corpus of real user agents and paths they were checked against are `const`s there.

1. **Standard names.** Log the raw value under the semconv name from
   [The canonical fields](#the-canonical-fields). This is the one part that isn't optional, and it
   costs nothing: every server here lets you choose the key (Caddy and Traefik via the pipeline
   rename).
2. **Types.** A number as a bare JSON number where the server always writes one; quoted where it
   may be unset. nginx's `escape=json` and `$status`'s `000` are the traps; see
   [Quoting rules for nginx `escape=json`](#quoting-rules-for-nginx-escapejson). `http_access` coerces either form, so this is about your own downstream readers.
3. **Method and version.** semconv's known set (`CONNECT DELETE GET HEAD OPTIONS PATCH POST PUT
   QUERY TRACE`, case-sensitive) with anything else as `_OTHER` plus the raw value in
   `http.request.method_original`; `HTTP/1.1` → `1.1`, `HTTP/2.0` → `2`. Cheap in any config
   language, and only worth doing server-side if something there keys off it.
4. **Length caps and control bytes.** A cap per free-text field (`url.path`, `url.query`,
   `user_agent.original`, and the referer at 256; most addresses at 128; the
   [`max_length:`](#max_length) table has the full list) and control bytes replaced. Server-side, this is where regex dialects bite: an nginx
   `map` is PCRE matching *bytes*, so a `.{0,256}` cap can split a multi-byte character and a
   `[[:print:]]` cap silently drops everything from the first non-ASCII byte; `http_access` caps by
   characters. If you cap at the edge, prefer `[[:print:]]` and accept the truncation, or leave
   capping to the pipeline.
5. **Query redaction.** Replace the value of any sensitive key (semconv's `AWSAccessKeyId`,
   `Signature`, `sig`, `X-Goog-Signature`, `X-Amz-Signature`, `X-Amz-Credential`,
   `X-Amz-Security-Token`, plus your own) with `REDACTED` *before* any cap. Worth doing
   server-side if the raw line is also written to a local file.
6. **User-agent class.** An ordered table, first match wins, scanner → tool → crawler → browser,
   with `none` for an empty header and `other` for no match; `crawler`/`scanner` also mean
   `user_agent.synthetic.type: bot`. This is the derivation most often worth having at the edge,
   because a rate limit or a block wants it there. Log it as `user_agent.class` and
   `http_access` keeps yours; the order of the built-in table matters more than its members
   (crawlers spoof `Mozilla/`, so `browser` must be last), and a `map` with `~*` arms in that order
   is the nginx shape.
7. **Route.** An ordered list of path patterns to literal route values, first match wins, with a
   catch-all — the same three built-in ideas (`/{probe}`, `/{well-known}`, `/{asset}`) and your
   own application routes. Log it as `http.route`. If your server *is* the router (an application
   server, or a proxy with a route table), its real route template is better than any regex
   bucket, and this is the single most valuable field to send yourself.
8. **Span fields.** `span.status` (`error` for 5xx or a `0` status, otherwise `unset`, never
   `ok`), `span.name` (`{method} {http.route}`, or the method alone), `error.type` (the status,
   5xx only). Rarely worth doing server-side: nothing at the edge acts on them, and `http_access`
   derives them from steps 3 and 7.

## What it does not do

[`docs/known-gaps.md`](known-gaps.md) tracks each of these:

- **No per-server presets.** `http_access` never learns a server's native variable names; you
  write the mapping in the server's own log-format language, or, for Caddy and Traefik, in a Lua
  rename stage.
- **`forwarded: {trust: true}` is all-or-nothing.** No trusted-proxy list and no hop count: it
  trusts the first hop of the whole `X-Forwarded-For` chain, including any a client injected before
  reaching your first proxy.
- **Route rules are regex-only**, matched in order, one at a time — O(rules) per event, no
  prefilter, no path-template syntax (`/users/:id`), no prefix trie. Fine for the handful of rules
  a real deployment needs.
- **The user-agent table is a heuristic bucket classifier, not a parser.** No
  `user_agent.name`/`user_agent.version`; the built-in table can be pre-empted but not turned off;
  and it trusts whatever the client sent, so a scanner using a browser UA classifies `browser`.
- **Paths are never percent-decoded.** Route rules see the request target exactly as it arrived
  on the wire, so two encodings of the same path classify separately.
- **A raw-bytes value is capped in bytes and never matched.** A field that arrived as non-UTF-8
  bytes (a `lua` stage writing a non-UTF-8 string produces one) has no text to run a
  pattern over: a bytes user agent is `other`, a bytes path takes `route_other` (or no route).
- **No `user_agent.class` without a user agent.** A format that doesn't log the header gets no
  class at all, by the absent rule; [`demo/nginx/nginx.conf`](../demo/nginx/nginx.conf) is one.
