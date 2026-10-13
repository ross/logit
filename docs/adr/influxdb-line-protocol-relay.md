---
created: 2026-10-13
updated: 2026-10-13
---

# InfluxDB line-protocol relay: fields as typed `Gauge` records under a measurement carrier, strings as attributes, and an all-or-nothing write

## Status
Accepted

## Context

`logit` has `influxdb_out`, which writes InfluxDB 2.x line protocol to `POST /api/v2/write`, but
no `influxdb_in`. [`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md)'s
"InfluxDB line protocol" section surveys the wire. Telegraf, the InfluxDB client libraries, and
every InfluxDB-compatible store speak it, so a listener that accepts it lets `logit` stand in for
InfluxDB's write API, and an `influxdb_in -> influxdb_out` pair becomes the tenth fixed point under
[ADR `lossless-transit`](lossless-transit.md).

Two things in today's model and sink stand in the way:

1. **Typed fields.** A field is `1.0`, `1i`, `1u`, `"text"`, or `true`, and every metric scalar in
   the model is an `f64`. [ADR `typed-metric-scalar`](typed-metric-scalar.md) adds
   `MetricValue { F64, I64, U64, Bool }` to the model for every protocol, and this ADR builds on it.
   Strings aren't metrics and need a home of their own.
2. **Multi-field points.** A point is a measurement, a tag set, N named fields, and one timestamp.
   `influxdb_out` writes one line per `MetricRecord`, with the record name as the measurement and a
   fixed `value=` field, so `cpu,host=a usage_user=1,usage_system=2` can't round-trip.

Scope is InfluxDB 2.x line protocol over the v2 write API. The v1 `/write` endpoint, the query and
delete APIs, and InfluxDB 3's own write endpoints are out of scope. The decisions below were settled
with Ross on 2026-10-13 and are recorded in full, with the workstreams that build them, in
[`docs/plans/influxdb-relay.md`](../plans/influxdb-relay.md).

## Decision

### Transport

`influxdb_in` is an HTTP listener that serves `POST /api/v2/write`, and `influxdb_out` keeps
writing to the same route. The listener:

- honors `precision=ns|us|ms|s`, nanoseconds when absent, and stamps a point with no timestamp with
  the request's receipt time;
- reads `org`, `orgID`, and `bucket` from the query string (see "One request, one batch");
- accepts `Content-Encoding: gzip` and identity, and answers `415` to any other encoding;
- takes an optional `tokens:` allowlist, matched against `Authorization: Token <t>` or
  `Authorization: Bearer <t>`;
- takes `tls:`, the field every single-mode HTTP listener uses. `bind_tls:` is `prometheus_in`'s
  alone, because only that listener has two modes;
- takes `peer:`, `proxy_protocol:`, and `forwarded:` as the other HTTP listeners do
  ([ADR `listener-peer-address`](listener-peer-address.md),
  [ADR `forwarded-header-parsing`](forwarded-header-parsing.md));
- answers `503` with InfluxDB's error JSON when the pipeline stays busy, as `splunk_hec_in` does.

Whether to serve `GET /ping` and `GET /health`, and the default `max_request_bytes`, wait on the
probe of a real `influxd` in `influx/w2` (see the plan's "Open risks").

### Model mapping

One point decodes to one `Event`:

| Wire | Model |
|---|---|
| measurement | event attribute `influxdb.measurement`, `Value::Str` |
| tag `k=v` | event attribute `k`, `Value::Str`, never a resource attribute |
| float field `f=1.5` | `MetricRecord` named `<measurement>.f`, `Gauge(F64(1.5))` |
| integer field `f=1i` | `<measurement>.f`, `Gauge(I64(1))` |
| unsigned field `f=1u` | `<measurement>.f`, `Gauge(U64(1))` |
| boolean field `f=true` | `<measurement>.f`, `Gauge(Bool(true))` |
| string field `f="x"` | event attribute `influxdb.field.f`, `Value::Str("x")` |
| N numeric or boolean fields | N records on the one `Event`, in wire order |
| timestamp | `event.timestamp`, scaled to nanoseconds by `precision` |
| no timestamp | `event.timestamp` = the request's receipt time |
| `org`, `orgID`, `bucket` | resource attributes `influxdb.org`, `influxdb.org_id`, `influxdb.bucket` |

Tags stay event attributes, as `graphite_in` and `statsd_in` keep theirs: a per-tag-set resource
would defeat batch accumulation, which keys on the resource's pointer. N fields as N records on one
`Event` is the shape `collectd_in` uses for a value list's data sources.

Every field is a `Gauge`. Line protocol has no metric kind: whether a field is a counter is a
convention between writer and reader, so the decoder doesn't guess one.

`crates/logit-proto/src/influxdb/mod.rs`'s module doc holds the canonical decode and encode tables
once `influx/w2` lands, with every skip and its counter. This ADR doesn't repeat them.

### The `influxdb.*` carrier

Rule (b) of [ADR `lossless-transit`](lossless-transit.md): the protocol's own spelling rides on
protocol-namespaced attributes that `influxdb_out` reads and every other sink treats as ordinary
attributes.

| Attribute | Scope | Value | Meaning |
|---|---|---|---|
| `influxdb.measurement` | event | `Str` | the point's measurement; its presence selects the carrier path on `influxdb_out` |
| `influxdb.field.<key>` | event | `Str` | a string field named `<key>` |
| `influxdb.org` | resource | `Str` | the request's `org` query parameter |
| `influxdb.org_id` | resource | `Str` | the request's `orgID` query parameter |
| `influxdb.bucket` | resource | `Str` | the request's `bucket` query parameter |

A tag key that starts with `influxdb.` would collide with the carrier, so the decoder drops it,
counted `logit.input.tags.dropped{reason="reserved_prefix"}` (normalization 12).

### Egress, with and without the carrier

**With `influxdb.measurement`**, `influxdb_out` writes one line per event. The measurement comes
from the carrier. Each record whose name starts with `<measurement>.` becomes a field whose key is
the rest of the name, in record order, with its typed suffix
([ADR `typed-metric-scalar`](typed-metric-scalar.md)). Each `influxdb.field.<key>` attribute
becomes a string field `<key>`, never a tag. Every `influxdb.*` attribute is skipped as a tag, as
`statsd.*` is today. An event with string fields and no metrics still writes a line. A record that
doesn't match the prefix, or isn't a scalar kind, takes the path below, counted
`logit.output.metrics.normalized{reason="carrier_mismatch"}`.

**Without the carrier**, `influxdb_out` keeps today's behavior: one line per `MetricRecord`, the
record name as the measurement, and a `value=` field, now typed.

`influxdb_out` keeps its configured `org` and `bucket`. It doesn't read `influxdb.org` or
`influxdb.bucket` to choose a destination.

Other sinks see the decoded shape. A record named `cpu.usage_user` reaches `prometheus_out` as
`cpu_usage_user` after sanitizing, the name Telegraf's own Prometheus output uses. A string-only
point has no metrics, so metric sinks skip it, and `influxdb.field.*` and `influxdb.measurement`
reach them as ordinary tags or labels unless a `remove` drops them. Both are cross-protocol rows in
`docs/known-gaps/mappings.md`.

### Strings are attributes, not metrics

A string field decodes to the event attribute `influxdb.field.<key>` and leaves `influxdb_out` as a
string field. No other sink's metric wire can carry a string value, and `aggregate` can't merge one,
so a string metric variant would cost every metric sink and transform a skip arm. See
[ADR `typed-metric-scalar`](typed-metric-scalar.md)'s alternatives.

### One request, one batch

`org`, `orgID`, and `bucket` decode to resource attributes. One request names one org and one
bucket, so a request decodes into one `EventBatch` under one resource, and the listener never
regroups points across requests. The listener caches a bounded number of `Arc<Resource>`s keyed by
the pair, so requests that name the same pair share one resource.

### Parse the whole body, then deliver all or nothing

The listener parses the whole body before delivering any of it. One unparseable line answers `400`
with InfluxDB's error JSON (`code`, `message`, and `line`), and nothing in the body is forwarded.
That's what InfluxDB does: a parse error rejects the whole write. It also means a producer that
retries a body gets the same answer, and never a partial write `logit` can't describe.

### Duplicate points merge, last write wins

Two points in one body with the same measurement, tag set, and timestamp merge into one event,
field by field, the later value winning, with a new field appended after the existing ones. InfluxDB
stores that series the same way, so a relay that kept both points would write a different series
than a direct write (normalization 2).

`influxdb_out`'s `allocate_timestamp` stays. It nudges a same-series point from a different source
in one sink batch forward by 1 ns, so neither is lost (normalization 9). Within the pair, the
listener's merge means the nudge never fires on a single body.

### Destination faults never reach the producer

`influxdb_in` answers `204` once a batch is in every downstream inbox. That's the acknowledgment
[ADR `delivery-semantics`](delivery-semantics.md) gives every input: accepted into the pipeline,
never delivered. A status the real InfluxDB gives `influxdb_out` later is a fault at the sink, and
no status the producer sees:

- a `422` for a field type conflict or a point outside retention is `Rejected` at `influxdb_out`;
- a `404` for a missing bucket or org is `Refused`;
- a `401` or `403` for the sink's token is `Refused`.

A producer writing through `logit` therefore never learns that its bucket doesn't exist or that a
field changed type. An operator watches `influxdb_out`'s `batches.dropped{reason="rejected"}` and
`logit.component.retrying` instead. `influx/w3` records this in `docs/known-gaps/influxdb.md` and in
[`docs/deploying.md`](../deploying.md). Forwarding the destination's verdict would mean holding the
producer's request open across the whole pipeline and every sink, which no `logit` input does.

### Listener responses

Each status is chosen so that a producer's retry behaves as it would against InfluxDB, and so that
`influxdb_out` pointed at `influxdb_in` reads it as the fault class its own response table assigns
(`crates/logit-outputs/src/influxdb.rs`'s "Response classes"):

| Situation | Status | Reaches `influxdb_out` as |
|---|---|---|
| written | `204` | `Ok` |
| a parse error, a missing `bucket` or org, or a bad `precision` | `400` `invalid`, with `line` for a parse error | `Rejected` |
| `tokens:` set, and the token is missing or unknown | `401` `unauthorized` | `Refused` |
| over `max_request_bytes`, by `Content-Length` or after decompression | `413` | `Rejected` |
| busy past the listener's bound, or a closed downstream consumer | `503` `unavailable`, `Retry-After: 1` | `Ambiguous` |
| an unknown route, `/write` included | `404` | `Refused`: a misconfigured URL |
| a wrong method | `405` | `Refused`; `influxdb_out` always sends `POST` |
| a stalled body | `408` | `Rejected` |
| an unsupported `Content-Encoding` | `415` | `Rejected`; `influxdb_out` sends identity |

The listener never sends `403`, a bucket `404`, `422`, or `429`: it has no permissions per bucket,
no buckets, no stored schema, and no rate limit. The module doc of
`crates/logit-inputs/src/influxdb.rs` is the canonical copy once `influx/w3` lands.

### The codec lives in `logit-proto`

The encoder moves from `crates/logit-outputs/src/influxdb.rs` to `crates/logit-proto/src/influxdb/`,
beside the new decoder, and `influxdb_out` keeps only the sink. The fixed-point suite needs both
directions in one crate, and a fuzz target can depend only on `logit-core` and `logit-proto`
([ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md)). It's also where
[ADR `parsers-live-in-logit-proto`](parsers-live-in-logit-proto.md) puts every parser of producer
bytes.

### Duplicate safety

InfluxDB's storage, not either component, is what makes most resends harmless:

- **`influxdb_out`** keeps its `at_least_once` posture. A retry re-encodes the same body byte for
  byte, and InfluxDB treats an identical `(measurement, tag set, timestamp)` write as an overwrite.
- **`influxdb_in`** acknowledges with `204`. A producer that resends after losing that answer
  delivers the body twice. A point with an explicit timestamp overwrites itself at InfluxDB. A
  point with no timestamp gets a new receipt time on the second delivery and lands as a second
  point, the same caveat a direct write to InfluxDB has.

### Permitted normalizations

Per [ADR `lossless-transit`](lossless-transit.md), the following count as normalization, not loss,
for `influxdb_in -> influxdb_out`. `crates/logit-proto/src/influxdb/mod.rs`'s module doc will
carry the same list with the same numbers when `influx/w2` lands, and the two lists must stay
numbered alike:

1. Batching: the writes `influxdb_out` sends needn't match the requests `influxdb_in` received.
2. Duplicate points in one body (same measurement, tag set, and timestamp) merge, last write wins
   per field.
3. Tag order is canonicalized.
4. Field order is first appearance: a field a later duplicate adds goes after the existing ones.
5. Number spelling: a float leaves as the shortest string that round-trips its `f64`, an integer
   keeps its `i` or `u` suffix, and a boolean leaves as `true` or `false` whatever spelling
   arrived.
6. Escaping: the encoder writes only the escapes the grammar requires.
7. Timestamps: every point leaves with a timestamp in nanoseconds, so `precision` isn't carried,
   and a point that arrived with none leaves with its receipt time.
8. Comments, blank lines, and runs of whitespace are dropped.
9. A same-series point from another source in one sink batch is nudged forward by 1 ns.
10. A `Sum`'s temporality and monotonicity are dropped on egress. A point from `influxdb_in` is
    always a `Gauge`, so this applies only to an event from another protocol.
11. A listener's sender attributes (`network.peer.*`, `client.*`) are added under `peer:`,
    `proxy_protocol:`, or `forwarded:`.
12. A tag key under the reserved `influxdb.` prefix is dropped, counted.

Which escapes the grammar requires, including whether `influxd` reads `\\` as one backslash in a
measurement, tag, or field key, is settled by the `influx/w2` probe, not by this ADR.

## Alternatives considered

- **Strings as metrics**, a `Str` variant on `MetricValue`. Rejected: no metric sink has a wire for
  it and `aggregate` can't merge it. See
  [ADR `typed-metric-scalar`](typed-metric-scalar.md)'s alternatives.
- **Keep the encoder in `logit-outputs`** and put only the decoder in `logit-proto`. Rejected: the
  fixed-point suite (`decode(encode(b)) == b`) and the fuzz target need both directions in one
  codec, and a fuzz target can't depend on `logit-outputs`.
- **Regroup points by org and bucket** across requests, or route `influxdb_out` by the carrier.
  Rejected: one request already names one pair, so one request is one batch with nothing to
  regroup. Routing a sink by a resource attribute is a `route` and `target` job
  ([ADR `target-components`](target-components.md)), not a sink field.
- **Skip and count a bad line, forward the rest.** Rejected: InfluxDB rejects the whole write on a
  parse error, so a relay that forwarded the good lines would store data a direct write wouldn't,
  and a producer that retried the `400` would duplicate whatever `logit` had already forwarded.
- **Keep duplicate points in one body as separate events.** Rejected: InfluxDB stores one point per
  series and timestamp, the later field values winning. Two events would reach `influxdb_out`,
  whose nudge would write the second 1 ns later as a distinct point that a direct write never
  stored.
- **A per-field event**, one `Event` per field with the measurement as the record name. Rejected:
  it loses which fields shared a point, so the encoder couldn't write `m f1=1,f2=2` back as one
  line without regrouping on timestamp and tags, which is the shape collectd's ADR rejected for a
  value list.

## Consequences

- `influxdb_in -> influxdb_out` becomes the tenth like-protocol pair under
  [ADR `lossless-transit`](lossless-transit.md), by its amendment of 2026-10-13. It depends on
  [ADR `typed-metric-scalar`](typed-metric-scalar.md), which lands first.
- A new `ComponentKind::InfluxDbIn`, a new graph rule for its fields, and a new module
  `logit_proto::influxdb` holding the lexer, decoder, encoder, and error-body writer. A fuzz target
  joins `fuzz/`.
- `influxdb_out` gains the carrier path and typed suffixes, and counts what it drops silently today:
  a non-finite value and a line with no fields.
- AGENTS.md's "Nine like-protocol pairs" becomes ten, and "the five HTTP listeners" becomes six,
  when `influx/w3` lands.
- Known-gaps rows, added with the codec and the listener: what `influxdb_out` drops or reshapes
  from other protocols (`SetMembers`, `ExponentialHistogram`, `unit`, `description`, exemplars, a
  sketch leaving as `count`/`sum`/`p50`/`p90`/`p99`, and the rest), the carrier leaking as tags at
  other sinks, a string-only point skipped at other sinks, the v1 `/write` route, no query or
  delete API, org and bucket not routed, and destination faults not reaching the producer.
- The pair's evidence is a recorded Telegraf corpus and a probe of a real `influxd`, listed in the
  plan's "Verification".
