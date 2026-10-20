---
created: 2026-10-13
updated: 2026-10-19
---

# InfluxDB line-protocol relay: fields as typed `Gauge` records under a measurement carrier, strings as attributes, and an all-or-nothing write

## Status
Accepted. Superseded in part on 2026-10-15 by "Amendment: `influxdb_out` follows the carried
destination (2026-10-15)", below: `influxdb_out` writes to the org and bucket a batch carries
unless its config fixes them, and reads a `403` or `404` about a carried bucket or org as
`Rejected`.
Amended the same day by "Amendment: `influxdb_out`'s `default_org:` (2026-10-15)", which gives a
batch that carries no org a configured fallback.
Amended on 2026-10-19 by "Amendment: string fields are private (2026-10-19)": a string field
rides as `_logit.influxdb.field.<key>`, so `influxdb_out` is the only protocol sink that writes
it, and `aggregate` leaves it out of a series. The sections above that amendment describe the
amended form.

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
with Ross on 2026-10-13.

On 2026-10-13 a probe checked this ADR's wire claims against `influxd` 2.9.1.
[Verified `influxd` behavior](#verified-influxd-behavior) lists the answers, and this ADR states
each as verified: the escaping rules, the error JSON without a `line` field, and the listener's
responses. "Deviations from `influxd`" lists where `influxdb_in` chooses differently.

## Decision

### Transport

`influxdb_in` is an HTTP listener that serves `POST /api/v2/write`, and `influxdb_out` keeps
writing to the same route. The listener:

- honors `precision=ns|us|ms|s`, nanoseconds when absent, and stamps a point with no timestamp with
  the request's receipt time;
- reads `org`, `orgID`, and `bucket` from the query string (see "One request, one batch");
- accepts `Content-Encoding: gzip` (lowercase, as `influxd` matches it) and identity, and answers
  `415` to any other encoding;
- takes an optional `tokens:` allowlist, matched against `Authorization: Token <t>` or
  `Authorization: Bearer <t>`;
- serves `GET` and `HEAD` on `/ping` and `/health`, which need no token, as `influxd` does;
- takes `tls:`, the field every single-mode HTTP listener uses. `bind_tls:` is `prometheus_in`'s
  alone, because only that listener has two modes;
- takes `peer:`, `proxy_protocol:`, and `forwarded:` as the other HTTP listeners do
  ([ADR `listener-peer-address`](listener-peer-address.md),
  [ADR `forwarded-header-parsing`](forwarded-header-parsing.md));
- answers `503` with InfluxDB's error JSON when the pipeline stays busy, as `splunk_hec_in` does.

`influxd` enforces no request-size limit, so `max_request_bytes` and its default are a `logit`
choice, set in `influx/w3`.

### Model mapping

One point decodes to one `Event`:

| Wire | Model |
|---|---|
| measurement | event attribute `_logit.influxdb.measurement`, `Value::Str` |
| tag `k=v` | event attribute `k`, `Value::Str`, never a resource attribute |
| float field `f=1.5` | `MetricRecord` named `<measurement>.f`, `Gauge(F64(1.5))` |
| integer field `f=1i` | `<measurement>.f`, `Gauge(I64(1))` |
| unsigned field `f=1u` | `<measurement>.f`, `Gauge(U64(1))` |
| boolean field `f=true` | `<measurement>.f`, `Gauge(Bool(true))` |
| string field `f="x"` | event attribute `_logit.influxdb.field.f`, `Value::Str("x")` |
| N numeric or boolean fields | N records on the one `Event`, in wire order |
| timestamp | `event.timestamp`, scaled to nanoseconds by `precision` |
| no timestamp | `event.timestamp` = the request's receipt time |
| `org`, `orgID`, `bucket` | resource attributes `_logit.influxdb.org`, `_logit.influxdb.org_id`, `_logit.influxdb.bucket` |

Tags stay event attributes, as `graphite_in` and `statsd_in` keep theirs: a per-tag-set resource
would defeat batch accumulation, which keys on the resource's pointer. N fields as N records on one
`Event` is the shape `collectd_in` uses for a value list's data sources.

Every field is a `Gauge`. Line protocol has no metric kind: whether a field is a counter is a
convention between writer and reader, so the decoder doesn't guess one.

`crates/logit-proto/src/influxdb/mod.rs`'s module doc holds the canonical decode table, and its
encode table is generated from [`spec/transit/mapping.yaml`](../../spec/transit/mapping.yaml)'s
`influxdb_out` column, the canonical copy, with every skip and its counter. This ADR doesn't
repeat them.

**Note:** The table above recorded the decision when it was made.
[`spec/transit/mapping.yaml`](../../spec/transit/mapping.yaml)'s `influxdb_out` column is the
current statement of what the sink does with each kind and feature, checked against the code by
the transit spec's flows and rendered into the codec's module doc and
`docs/known-gaps/mappings.md` ([ADR `transit-spec`](transit-spec.md)).

### The `_logit.influxdb.*` carrier

Rule (b) of [ADR `lossless-transit`](lossless-transit.md): the protocol's own spelling rides on
attributes that `influxdb_out` reads. The measurement, the string fields, the org, and the bucket
are private carriers under `_logit.influxdb.`, which every other sink leaves off its wire
([ADR `private-attribute-namespace`](private-attribute-namespace.md)).

| Attribute | Scope | Value | Meaning |
|---|---|---|---|
| `_logit.influxdb.measurement` | event | `Str` | the point's measurement; its presence selects the carrier path on `influxdb_out` |
| `_logit.influxdb.field.<key>` | event | `Str` | a string field named `<key>` |
| `_logit.influxdb.org` | resource | `Str` | the request's `org` query parameter |
| `_logit.influxdb.org_id` | resource | `Str` | the request's `orgID` query parameter |
| `_logit.influxdb.bucket` | resource | `Str` | the request's `bucket` query parameter |

A tag key under the private `_logit.` prefix is omitted, counted
`logit.input.tags.omitted{reason="reserved_prefix"}` (normalization 12), as on each of the five
tag-bearing metric wires (the private namespace ADR's inbound guard). That guard covers the
string-field prefix too. Any other tag key, one under `influxdb.` or `influxdb.field.` included,
is an ordinary tag.

### Egress, with and without the carrier

**With `_logit.influxdb.measurement`**, `influxdb_out` writes one line per event. The measurement
comes from the carrier. Each record whose name starts with `<measurement>.` becomes a field whose
key is the rest of the name, in record order, with its typed suffix
([ADR `typed-metric-scalar`](typed-metric-scalar.md)). Each `_logit.influxdb.field.<key>`
attribute becomes a string field `<key>`, never a tag. Every private `_logit.` attribute is
skipped as a tag, uncounted. An event with string fields and no
metrics still writes a line. A record that
doesn't match the prefix, or isn't a scalar kind, takes the path below, counted
`logit.output.metrics.normalized{reason="carrier_mismatch"}`.

**Without the carrier**, `influxdb_out` keeps today's behavior: one line per `MetricRecord`, the
record name as the measurement, and a `value=` field, now typed.

`influxdb_out` keeps its configured `org` and `bucket`. It doesn't read `_logit.influxdb.org` or
`_logit.influxdb.bucket` to choose a destination. [Superseded by "Amendment: `influxdb_out`
follows the carried destination (2026-10-15)": with no `bucket:` configured, `influxdb_out`
writes each batch to the org and bucket it carries.]

Other sinks see the decoded shape. A record named `cpu.usage_user` reaches `prometheus_out` as
`cpu_usage_user` after sanitizing, the name Telegraf's own Prometheus output uses. The measurement
and the org and bucket never reach another sink's wire, because they're private, and neither do
string fields. A string-only point has no metrics, so metric sinks skip it.

### Strings are attributes, not metrics

A string field decodes to the event attribute `_logit.influxdb.field.<key>` and leaves
`influxdb_out` as a string field. No other sink's metric wire can carry a string value, and
`aggregate` can't merge one, so a string metric variant would cost every metric sink and
transform a skip arm. See
[ADR `typed-metric-scalar`](typed-metric-scalar.md)'s alternatives.

### One request, one batch

`org`, `orgID`, and `bucket` decode to resource attributes. One request names one org and one
bucket, so a request decodes into one `EventBatch` under one resource, and the listener never
regroups points across requests. The listener caches a bounded number of `Arc<Resource>`s keyed by
the pair, so requests that name the same pair share one resource.

### Parse the whole body, then deliver all or nothing

The listener parses the whole body before delivering any of it. One unparseable line answers `400`
with InfluxDB's error JSON (`code` and `message`), and nothing in the body is forwarded. The
message is `unable to parse '<line>': <reason>`, naming the offending line by its text. InfluxDB
has no `line` field, and the decoder's 1-based line index goes to logs and telemetry only. That's
what InfluxDB does: a parse error rejects the whole write. It also means a producer that
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

[Superseded in part by "Amendment: `influxdb_out` follows the carried destination
(2026-10-15)": a `403` or `404` about a bucket or org that came from the batch is `Rejected` and
drops that batch.]

A producer writing through `logit` therefore never learns that its bucket doesn't exist or that a
field changed type. An operator watches `influxdb_out`'s `batches.dropped{reason="rejected"}` and
`logit.component.retrying` instead. `influx/w3` records this in `docs/known-gaps/influxdb.md` and in
[`docs/deploying.md`](../deploying.md). Forwarding the destination's verdict would mean holding the
producer's request open across the whole pipeline and every sink, which no `logit` input does.

### Listener responses

Each status is chosen so that a producer's retry behaves as it would against InfluxDB, and so that
`influxdb_out` pointed at `influxdb_in` reads it as the fault class its own response table assigns
(`crates/logit-outputs/src/influxdb.rs`'s "Response classes"). The module doc of
`crates/logit-inputs/src/influxdb.rs` is the one response table, with every status and the check
order; this table keeps the rows whose fault class is the decision:

| Situation | Status | Reaches `influxdb_out` as |
|---|---|---|
| written, or an empty body | `204` | `Ok` |
| a line that fails to parse | `400` `invalid` | `Rejected` |
| a missing `bucket` | `404` `not found` | `Refused`: a misconfigured URL |
| `tokens:` set, and the token is missing or unknown | `401` `unauthorized` | `Refused` |
| over `max_request_bytes` | `413` | `Rejected` |
| busy past the listener's bound, or a closed downstream consumer | `503` `unavailable`, `Retry-After: 1` | `Ambiguous` |

[Superseded in part by "Amendment: `influxdb_out` follows the carried destination (2026-10-15)":
an `influxdb_out` reads the `404` row as `Rejected` when the bucket or org came from the
batch.]

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
for `influxdb_in -> influxdb_out`. `crates/logit-proto/src/influxdb/mod.rs`'s module doc carries
the same list with the same numbers, and the two lists must stay numbered alike:

1. Batching: the writes `influxdb_out` sends needn't match the requests `influxdb_in` received.
2. Duplicate points in one body (same measurement, tag set, and timestamp) merge, last write wins
   per field.
3. Tag order isn't preserved: tags leave in the order the process interned their keys.
   influxd sorts the series key itself, so what it stores is unaffected; only the bytes on the
   wire differ between processes.
4. Field order: numeric and boolean fields leave in wire order (a field a later duplicate adds
   goes after the existing ones), and string fields leave after them, in an order the encoder
   doesn't preserve. A field whose key starts with a tab or NUL is never written first. A field's
   identity is its key, so order carries no meaning at the receiver.
5. Number spelling: a float leaves as the shortest string that round-trips its `f64`, an integer
   keeps its `i` or `u` suffix, and a boolean leaves as `true` or `false` whatever spelling
   arrived.
6. Escaping: the encoder writes only the escapes the grammar requires, so a backslash a sender
   wrote doubled leaves as it was read, and a key the grammar can't spell is left out, counted:
   a measurement starting with a tab or NUL, a field key starting with one on a line whose every
   field key does, and a measurement or field key ending in `\` or with `\` before a delimiter
   are `metrics.dropped{reason="unencodable_key"}`, and a tag key or value ending in `\` or with
   `\` before a space is `tags.omitted{reason="unencodable"}`.
7. Timestamps: every point leaves with a timestamp in nanoseconds, so `precision` isn't carried,
   and a point that arrived with none leaves with its receipt time (see item 14).
8. Comments, blank lines, and runs of spaces are dropped. A CRLF line ending, a tab separator, and
   a trailing comment after the fields are parse errors, not normalized.
9. A same-series point from another source in one sink batch is nudged forward by 1 ns.
10. A `Sum`'s temporality and monotonicity are dropped on egress. A point from `influxdb_in` is
    always a `Gauge`, so this applies only to an event from another protocol.
11. A listener's sender attributes (`network.peer.*`, `client.*`) are added under `peer:`,
    `proxy_protocol:`, or `forwarded:`.
12. A tag key under the private `_logit.` prefix is omitted, counted.
13. A duplicate field key within one line: the last value wins.
14. An absent timestamp: the request's arrival time truncated to `precision`, one instant for every
    such point in the request.

### Escaping

The probe settled the grammar `influxdb_in` decodes and `influxdb_out` encodes. In a measurement,
tag key, tag value, and field key:

- A backslash before `,` or space is dropped and the character is literal. `=` is special in a
  tag key, tag value, and field key, and a backslash before it is dropped. `=` isn't special in a
  measurement, so the encoder writes it bare there. `\=` in a measurement is accepted by `influxd`
  with `204` and the data is unreadable afterward, so the encoder never writes it.
- A field key also unescapes `\"` to a quote. A tag value, tag key, and
  measurement keep a backslash before `"` literal.
- A backslash before anything else, another backslash included, stays a literal backslash. `\\`
  is not an escape: it decodes to two backslashes. The encoder writes a backslash as itself and
  escapes only `,`, space, and (outside a measurement) `=`, each with one backslash.
- A key that ends in a backslash has no spelling: the backslash would escape the delimiter. A
  measurement or field key with a backslash before a delimiter has none either. A measurement
  starting with a tab or NUL has none: it starts where `influxd` skips whitespace, at a line's
  start, and loses that byte. The first field key starts where `influxd` skips whitespace too, in
  the run after the space before the fields, but nothing is skipped after the `,` between fields,
  so the encoder writes a field whose key starts with a tab or NUL after one whose key doesn't,
  and drops it only when every field key on the line starts with one. A tag key or value has none with a
  backslash before a space, written as a `\\` pair that leaves the space bare and ends the tag
  section early. A backslash before `,` or `=` in a tag is written as `\\,` or `\\=`
  and reads back as written, because `influxd`'s line splitter counts `,` and `=` only after a bare
  space has ended the tag section. The encoder drops the point (a measurement, a
  field key) or omits the tag (a tag key or value), counting a point or field
  `metrics.dropped{reason="unencodable_key"}` and a tag `tags.omitted{reason="unencodable"}`. The decoder reads a trailing backslash before a delimiter
  as escaping it, which fails as it does at `influxd`.
- Inside a quoted string field value, `\"` is a quote and `\\` is one backslash, and any other
  `\x` is two literal characters. A raw newline inside the quotes is part of the value, so the
  decoder splits a body into lines with quote tracking.

### Deviations from `influxd`

`crates/logit-proto/src/influxdb/mod.rs`'s module doc ("Deviations from influxd") is the canonical
list; this section summarizes it. Each deviation is a choice or a consequence of the model:

- A `time` field key and a same-line field type conflict (`f=1i,f=2.5`) are parse errors (`400`),
  where `influxd` answers `422`. The conflict text is `influxd`'s. The same type repeated on one
  line takes the last value at the first position (normalization 13). Across lines the decoder
  keeps no type table: a later value wins even across types, and a conflict is the destination's
  `422`.
- Neither the 1 MiB field-value limit nor the 65,535-byte series-key limit is enforced.
  `max_request_bytes` bounds the body; the destination applies its own limits.
- A name or string value that isn't UTF-8 is a `400`, because the model's `Value::Str` is UTF-8.
- The error names the first bad line only. `influxd` lists every bad line, joined by `\n`.
- The line splitter keeps `influxd`'s quirks: a comment line containing `="` swallows the lines
  after it until a quote closes, and a backslash before a newline continues the line.
- Reason strings follow the probe, except that `invalid field format` replaces `influxd`'s
  `invalid value: field-key=...`.
- A `Content-Encoding` other than `gzip` or identity answers `415`, where `influxd` ignores the
  header and parses the raw bytes. A body labeled `gzip` that doesn't decompress answers `400`,
  where `influxd` answers `500`, and a route with a trailing `/` answers `404`, where `influxd`
  redirects with `308`.
- A field type conflict across lines or requests is the destination's `422`, which `influxdb_out`
  reads as `Rejected` (decision 10), not a response from the listener.
- The encoder's 1 ns nudge (normalization 9) can push a point at the timestamp ceiling out of
  range. The encoder then drops it, counted `metrics.dropped{reason="timestamp_overflow"}`.

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
  ([ADR `target-components`](target-components.md)), not a sink field. [The second half is
  superseded by "Amendment: `influxdb_out` follows the carried destination (2026-10-15)": a
  `route` needs every bucket listed in advance.]
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
  a non-finite value and a line with no fields. Its counters, and `influxdb_in`'s, follow
  [ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md): a point or field lost is
  `metrics.dropped`, an unresolved `GaugeDelta` among them, a tag left off a delivered point is
  `tags.omitted`, and only a record with no value and a kind line protocol has no form for are
  `metrics.skipped`.
- AGENTS.md's "Nine like-protocol pairs" becomes ten, and "the five HTTP listeners" becomes six,
  when `influx/w3` lands.
- Known-gaps rows, added with the codec and the listener: what `influxdb_out` drops or reshapes
  from other protocols (`SetMembers`, `ExponentialHistogram`, `unit`, `description`, exemplars, a
  sketch leaving as `count`/`sum`/`p50`/`p90`/`p99`, and the rest), string fields leaking as tags
  at other sinks [lost there instead since "Amendment: string fields are private (2026-10-19)"],
  a string-only point skipped at other sinks, the v1 `/write` route, no query or
  delete API, org and bucket not routed, and destination faults not reaching the producer. [The
  org and bucket row is replaced by the gaps that "Amendment: `influxdb_out` follows the carried
  destination (2026-10-15)" lists.]
- The pair's evidence is a recorded Telegraf corpus and a probe of a real `influxd`, listed in
  [Verified `influxd` behavior](#verified-influxd-behavior).

## Verified `influxd` behavior

On 2026-10-13 a probe wrote each case to `docker.io/library/influxdb:2` (the tag `compose.yaml`
uses), digest `sha256:f75e48af0598e8aec7986e991a848d19a119101a7d563a2e5db1dfaac9c45daa`, which
reports `version: v2.9.1` (commit `d4fa1941fd`), and read it back with Flux or InfluxQL. These
facts bind the codec and the listener:

1. **Escaping outside a string field value.** In a measurement, tag key, tag value, and field key,
   a backslash before `,`, space, or `=` is dropped and the character is literal. A backslash
   before anything else, another backslash included, stays a literal backslash. `\\` is not an
   escape: `p\\x` is stored with two backslashes. So the encoder writes a backslash as itself and
   escapes only `,`, space, and (outside a measurement) `=`, each with one backslash.
2. **`=` in a measurement.** A bare `=` is stored as written (`p=e`). `\=` is accepted with `204`,
   the name appears in the index, and no row is readable, so the data is lost without an error. The
   encoder never writes `\=` in a measurement.
3. **A trailing backslash is unwritable.** A key or tag value that ends in a backslash, or has one
   before a delimiter, makes that delimiter escaped, and `influxd` answers `400` (`invalid tag
   format`, `missing fields`, or `invalid value: field-key=...`). The grammar has no spelling for
   it.
4. **String field values.** Inside the quotes, `\"` is a quote and `\\` is one backslash. Any other
   `\x` is two literal characters. A raw newline inside the quotes is part of the value, so a body
   splits into lines only with quote tracking. `,`, space, and `=` need no escape inside quotes.
   An unterminated quote is `400` `unbalanced quotes`.
5. **Numbers and booleans.** A bare number is a float, including `1`, `5.`, `.5`, `1.5e+3`, and
   `-0`. `Ni` is an `i64` and `Nu` a `u64`; `007i` and `0u` are fine. These are `400`: `+1`, `+1i`,
   `inf`, `NaN`, `0x10`, `1_000i`, `-1u`, `1.0i`, `1e3i`, `1e`, `1e309`, and an `i64` or `u64`
   overflow. Booleans are only `t T true True TRUE f F false False FALSE`; `tRUE` and `TrUe`
   are `400`.
6. **Whitespace, line endings, and comments.** A line that starts with `#` after optional spaces is
   a comment, and blank lines, leading spaces, trailing spaces, and runs of spaces between
   sections are accepted. CRLF is `400`, because the `\r` stays on the last token (`invalid
   number`). A tab is not a separator (`missing fields`). A `# comment` after the fields is `400`
   (`bad timestamp`). A space after a field comma, or a trailing comma, is `400`
   (`invalid field format`). An empty, whitespace-only, or comment-only body is `204`.
7. **`precision` and an absent timestamp.** `ns`, `us`, `ms`, and `s` are valid and an empty value
   means `ns`; anything else (`h`, `n`, `NS`, `millisecond`) is `400` with `invalid precision; valid
   precision units are ns, us, ms, and s`. A point with no timestamp gets the request's arrival
   time truncated to `precision`, and every such point in one request shares that instant.
8. **Timestamp range.** After scaling to nanoseconds, a value must lie in -9223372036854775806 to
   9223372036854775806; outside it is `400` `time outside range ...`. Negative values and `0` are
   fine. A decimal point, a leading `+`, a non-number, or a second trailing token is `400`
   (`bad timestamp`, `point is invalid`).
9. **Duplicates.** Points with the same measurement, tag set, and timestamp merge into one point,
   and the last write wins per field, within one body and across requests. Tag order doesn't matter
   because `influxd` sorts the series key. A duplicate tag key in one line is `400`
   (`duplicate tags`). A duplicate field key in one line (`f=1i,f=2i`) keeps the last value.
10. **Reserved names.** A tag key of `_measurement`, `_field`, or `time` is `400`. A field key of
    `time` is `422` (`invalid field name`). A measurement may start with `_`. A tag key of `_time`
    or `_value` is accepted at write but breaks every Flux read that includes it, and a full-bucket
    scan panicked, so those arrive from a sender as ordinary tags.
11. **Error JSON.** The body is `{"code":"invalid","message":"unable to parse '<line>': <reason>"}`
    for `400` and `{"code":"unprocessable entity","message":"..."}` for `422`. There is no `line`
    field. When several lines fail to parse, one message joins their `unable to parse` clauses
    with `\n`. The message repeats the whole offending line, including any raw bytes.
12. **Parse error against type conflict.** A parse error is `400` and rejects the whole request;
    nothing from the body is stored. A field type conflict (`is type float, already exists as type
    integer`) is a `422` partial write: the other points are stored, the conflicting point is
    dropped whole (`dropped=1`), and the message names the first conflict. The check spans lines in
    one body and spans requests, because the type lives in the bucket.
13. **Limits.** No body-size limit exists up to the 256 MiB tried, and v2.9.1 has no flag to set
    one. A string field value over 1,048,576 bytes is a `422` partial write. A series key over
    65,535 bytes (measurement, tag keys and values, escapes counted) is `400` `max key length
    exceeded: <n> > 65535`.
14. **`Content-Encoding`.** Only lowercase `gzip` is decompressed. `deflate`, `zstd`, `br`, and
    `GZIP` pass through undecoded and the raw bytes are parsed as line protocol, so a compressed
    body fails as `missing fields` and a plain-text body with `br` succeeds. `influxd` never
    answers `415`. A body labeled `gzip` that isn't gzip is `500`. `Content-Type` is ignored.
    `Transfer-Encoding: chunked` with no `Content-Length` is accepted.
15. **Authentication.** `Authorization: Token <t>` and `Bearer <t>` both work, and the scheme
    matches case-insensitively. A missing token, a wrong token, HTTP Basic, a `u`/`p` query pair,
    and a bare token are all `401` `{"code":"unauthorized","message":"unauthorized access"}`.
16. **Query parameters.** A missing or empty `bucket` is `404` `bucket not found`. A missing `org`
    and `orgID` is `400` `Please provide either orgID or org`. `orgID` wins when both are given.
    An unknown bucket or org is `404`. A malformed `orgID` is `400`. `rp` and `consistency` are
    ignored. A trailing slash on the route is a `308` redirect.
17. **`/ping`, `/health`, and v1.** `GET /ping` and `HEAD /ping` need no token and answer `204`
    with `X-Influxdb-Version` and `X-Influxdb-Build` headers. `GET /health` needs no token and
    answers `200` with a JSON body (`name`, `message`, `status`, `checks`, `version`, `commit`).
    Every response carries `X-Influxdb-Version`. A wrong method on the write route is `405` with
    `Allow: OPTIONS, POST`. The v1 `POST /write?db=` route works only when a DBRP mapping for `db`
    exists; otherwise it is `404` `no dbrp mapping found`. It stays out of scope here.

## Amendment: the carriers move under `_logit.influxdb.` (2026-10-13)

The measurement and the org, org ID, and bucket carriers are private under `_logit.influxdb.`, as
[ADR `private-attribute-namespace`](private-attribute-namespace.md) decides, so they no longer
leak as tags or labels at other sinks and no `remove` is needed ahead of one. `influxdb.field.<key>`
stays public. The decoder's reserved tag prefix narrows from `influxdb.` to `influxdb.field.`,
beside the private `_logit.` guard, so a wire tag such as `influxdb.host` now decodes as an
ordinary tag. The sections above describe the amended form. [Amended by "Amendment: string
fields are private (2026-10-19)": string fields move under `_logit.influxdb.field.`, and the
`influxdb.field.` guard is gone.]

## Amendment: `influxdb_out` follows the carried destination (2026-10-15)

`influxdb_out` writes each batch to the org and bucket `influxdb_in` carried on its resource,
unless its own config fixes them. This reverses "Egress, with and without the carrier"'s last
paragraph and the second half of the "Regroup points by org and bucket" alternative. A `route` on
`_logit.influxdb.bucket` into one `influxdb_out` per bucket works, but only for the buckets the
operator lists in advance. In practice that list is error-prone: a bucket a client starts using
after the config was written lands wherever the router sends unrouted events.

`crates/logit-outputs/src/influxdb.rs`'s module doc is the canonical statement of how the sink
resolves a destination ("Destination resolution") and of its response classes ("Response
classes"). This amendment records the decisions and the evidence behind them.

### Destination

- **Bucket.** With `bucket:` set, the sink runs in fixed mode: every batch goes to that bucket and
  the carrier is ignored, as before. With `bucket:` absent, it runs in carried mode: a batch goes
  to its `_logit.influxdb.bucket`, else to `default_bucket:`. Setting both is a validation error.
  In carried mode, a batch with no carrier and no `default_bucket:` is dropped, counted under
  reason `no_bucket`.
- **Org.** A configured `org:` is sent as `org=` alone, never beside a carried `orgID`. InfluxDB
  2.7 lets `orgID` win when a request sends both (rows 5a to 5d below), so forwarding the client's
  `orgID` would defeat the operator's override. With no `org:`, the sink forwards what the client
  sent: `org`, `orgID`, or both. With neither, it sends no org parameter. InfluxDB 3 Core ignores
  the org, and InfluxDB 2.x answers `400`.

### Fault classes

A `403` or `404` is `Rejected` only when a value it could concern came from the batch's carrier:
the batch drops, counted `batches.dropped{reason="rejected"}`, and batches for other buckets keep
flowing.

- A `404` that names a bucket is `Rejected` when the bucket or the org was carried, because
  InfluxDB 2.x looks a bucket name up inside the org the request names: a configured bucket
  missing from a carried org is that org's doing. A `404` that names an org is `Rejected` when
  the org was carried.
- A `403` (`insufficient permissions for write`) names neither, and on 2.7.12 both a bucket outside
  the token's scope and an org the token doesn't reach produce it (rows 8 and 10a below). So a
  `403`, like a `404` whose body names neither, is `Rejected` when the bucket or the org was
  carried.
- Otherwise the answer is `Refused` and holds the queue: the bucket came from `bucket:` or
  `default_bucket:` and the org from `org:`, so it's the operator's config, and every batch that
  uses it gets the same answer. That includes no org at all, which InfluxDB 2.x answers `400`
  first.
- A `401` is always `Refused`, because it's about the sink's own token.

`org:` overrides independently of `bucket:`. With `bucket:` set and no `org:`, the client's org is
still forwarded, and a `403` or any `404` is `Rejected`. Only with both `bucket:` and
`org:` configured does the sink keep the original table, where `401`, `403`, and `404` are all
`Refused`. The module doc's "Response classes" has the row for each body.

The reason is the queue. `write_loop` holds one head at a time in FIFO order, and a `Refused` head
retries with no budget ([ADR `sink-fault-classes`](sink-fault-classes.md)). When a client picks
the bucket, a bucket that doesn't exist, or that the relay's token can't write, would hold every
other bucket's batches behind it indefinitely. One misconfigured producer would stop the relay for
every producer. `splunk_hec_out` already makes this call for HEC code 7, an index carried on the
batch as `com.splunk.index`, and `otlp_out` reads a `401`, `403`, or `404` as `Rejected` for one
signal. Both are rows in that ADR's list of sinks that depart from the status-only default.

### Alternative: hold only the failing bucket

Holding only the failing bucket's batches, and letting the others pass, would keep a transient
`403` or `404` lossless, such as a write to a bucket that's being created. It needs a keyed queue
per destination in a sink whose `write_loop` is a single-head FIFO, and the "Order is kept"
section of [ADR `sink-fault-classes`](sink-fault-classes.md) rules out letting later batches pass
a held one. That's out of scope here; `docs/known-gaps/influxdb.md` records it as a gap.

### Trust

Any client `influxdb_in` accepts can write to any bucket the relay's token can reach. `tokens:`
admits a client but doesn't bind its token to a bucket, so scope the relay's token to the buckets
its producers may use. On InfluxDB 2.7.12 a bucket ID crosses orgs: a token for org A writing with
`org=A` and the ID of a bucket in org B got `204`, and the point landed in org B (row x6 below),
so even an org-scoped token doesn't confine a relay that forwards a client's bucket ID.

### Verified responses

Checked on 2026-10-15 against `influxdb:2.7.12`
(`sha256:b8d940ca9376f85118260f5b6bd236b8a00b1749c3350c5578d4cde8e27f31f2`) and
`influxdb:3.8.0-core`
(`sha256:13a654390eccd4b238895b458e1b4a796ada2365123a034df9037cf7a4f4af5d`), with a write-only
token scoped to one bucket, an all-access token for one org, and the operator token:

| Row | Server | Request | Answer | Carried mode |
|---|---|---|---|---|
| 2 | 2.7.12 | unknown bucket | `404` `bucket "<name>" not found` | `Rejected` if the bucket or the org was carried |
| 3, 4b | 2.7.12 | unknown org name, or unknown `orgID` | `404` `organization name "<name>" not found`, or `organization not found` | `Rejected` if the org was carried |
| 5a–5d | 2.7.12 | `org` and `orgID` naming different orgs | no error; `orgID` wins | — |
| 7a–7c | 2.7.12 | neither `org` nor `orgID`, any token | `400` `Please provide either orgID or org`; the token never implies an org | `Rejected` |
| 8, 10a, 11 | 2.7.12 | a bucket that exists but the token can't write: another org's, out of a scoped token's reach, or read-only | `403` `insufficient permissions for write` | `Rejected` if the bucket or the org was carried |
| 10b | 2.7.12 | a scoped token, an unknown bucket | `404`, not `403`, so a bucket's existence is visible | `Rejected` if the bucket or the org was carried |
| 12a, 12b | 2.7.12 | an invalid or missing token | `401` `unauthorized access` | `Refused` |
| x6 | 2.7.12 | an all-access token for org A, `org=A`, the ID of a bucket in org B | `204`; the point lands in org B | — |
| x7 | 2.7.12 | a bucket name in the wrong case | `404`; names are case-sensitive | `Rejected` if the bucket or the org was carried |
| a1, a2 | 3.8.0 Core | unknown bucket | `204`; a database of that name is created | — |
| b1, b3 | 3.8.0 Core | any `org` or `orgID`, bogus included | ignored | — |
| c1–c3 | 3.8.0 Core, auth on | an invalid or missing token | `401` `{"error": "the request was not authenticated"}` | `Refused` |

Because InfluxDB 3 Core creates a database for any bucket it doesn't know, a client's typo there
becomes a new database rather than a counted drop.

### Consequences of the amendment

- `influxdb_out`'s `org` and `bucket` become optional, beside a new optional `default_bucket`. An
  existing config with `bucket:` set behaves as before.
- An operator's own mistake still holds in carried mode, and `logit.component.retrying` reads
  `1`: a missing `default_bucket:` is `Refused` unless the org was carried, and a wrong `org:` is
  `Refused` on its `404`, and on a `403` unless the bucket was carried. Configure both `bucket:` and `org:` to hold on every `403` and `404`.
- In carried mode with no `org:` against InfluxDB 2.x, a batch that didn't come from `influxdb_in`
  carries no org and draws a `400`, which is `Rejected`. Set `org:` when such a source feeds the
  sink.
- `docs/known-gaps/influxdb.md` replaces "`org` and `bucket` are carried, not routed" with the gaps
  above: a transient `403` or `404` drops rather than holds, the trust boundary, and InfluxDB 3
  Core's auto-created databases.

## Amendment: `influxdb_out`'s `default_org:` (2026-10-15)

`influxdb_out` gains `default_org:`, the org-side twin of `default_bucket:`. The amendment above
left a batch that carries no org, such as an `internal` component's metrics or any path with no
`influxdb_in` upstream, two choices against InfluxDB 2.x: draw a `400`, or set `org:`, which also
overrides every carried org on the same sink. With `default_org:`, those batches and relayed ones
share one sink, each relayed batch keeping its client's org.

The sink resolves a batch's org in this order:

1. `org:`, sent as `org=` alone, as before.
2. The carried `org` and `orgID`, forwarded as before, both when both were carried. A carried
   `orgID` alone wins over `default_org:` and is sent without an `org=` beside it, because
   InfluxDB 2.7 reads `orgID` over `org` when a request has both.
3. `default_org:`, sent as `org=`.
4. Nothing, as before: InfluxDB 3 Core ignores the absence, and InfluxDB 2.x answers `400`, which
   is `Rejected`.

Setting `org:` and `default_org:` together is a validation error, as `bucket:` with
`default_bucket:` is: `org:` would win for every batch, so the default would never be read.

A `default_org:` value is the operator's config, like `org:` and `default_bucket:`. A `403` or
`404` that could concern it is `Refused` and holds the queue; only a carried org makes the
org-related rows `Rejected`. The module doc's "Response classes" has the rows.

The amendment above advised setting `org:` when a source that carries no org feeds the sink in
carried mode; `default_org:` is now the setting for that case, and `org:` stays the override.

## Amendment: string fields are private (2026-10-19)

A string field decodes to the private attribute `_logit.influxdb.field.<key>`, so `influxdb_out`
is the only protocol sink that writes it. Every other protocol sink leaves it off its wire,
uncounted, under [ADR `private-attribute-namespace`](private-attribute-namespace.md)'s ignore
rule, as it does the measurement, org, and bucket carriers. The renders of a whole event carry it,
as they carry every private key: `stdio_out` and `file_out` in every format, `kafka_out` under `json`, `msgpack`, and `native`, `logit_out`, and a `buffer.disk:` spool.

### Why

A string field's value is free text: a version, a status message, a hostname, a log line. As a
public attribute, `statsd_out` wrote it as an `influxdb.field.f:x` tag, `prometheus_out` and
`graphite_out` as a label or tag, and `otlp_out` as an attribute. At every metric sink but
`influxdb_out`, each distinct value starts a new series, so one Telegraf input that reports a
string per sample can grow a destination's series count without bound. The operator's remedy was
a `remove` of each string field ahead of every non-InfluxDB sink, which needs the field names in
advance, the same burden the private namespace removed for the carriers.

InfluxDB is the only destination where a string field is a field rather than series identity.
Any other sink that wants one needs an explicit choice, so the default is to leave it off.

### What changes

- The decoder writes `_logit.influxdb.field.<key>`, and `influxdb_out` reads that prefix. The
  `line_protocol` transform shares the decoder and changes with it.
- `influxdb_in -> influxdb_out` stays a fixed point: the encoder still writes each string field
  as a field.
- Lua and transforms still read a string field by its full key, as they read any private key. A
  `lua` stage that copies one to a public attribute sends it to other sinks.
- A `keep` under its default `private: drop` strips string fields along with the measurement
  carrier. A `keep` ahead of `influxdb_out` needs `private: keep` to keep them.
- `aggregate` leaves string fields out of a series' key and out of what it flushes. Keyed on
  them, two readings of one series that differ only in a string, such as Telegraf's
  `uptime_format`, would flush as two series that every other protocol sink writes with the same
  labels, so one overwrites the other. A string field isn't series identity at InfluxDB, and an
  aggregate summarizes numbers. Only this prefix is left out; other private keys, the measurement
  carrier included, are identity a sink reads after `aggregate`
  ([ADR `aggregation-window-semantics`](aggregation-window-semantics.md)'s 2026-10-19
  amendment).
- There's no alias and no decode path for the old public key, because `logit` is pre-release.

### The reserved tag prefix

The decoder's separate guard on a wire tag under `influxdb.field.` is gone. It existed so a
sender's tag couldn't read back as a string field. With string fields under
`_logit.influxdb.field.`, the private `_logit.` guard covers that, and nothing public lives under
`influxdb.field.`. A wire tag such as `influxdb.field.f=x` now decodes as an ordinary tag, and
`influxdb_out` writes it back as a tag, so the pair stays a fixed point for it too. The encoder's
matching skip of a public `influxdb.field.` attribute is gone for the same reason. Normalization
12 narrows to the `_logit.` prefix alone, the same guard the other four tag-bearing wires have.

Keeping the guard would have omitted and counted a tag that no longer collides with anything,
which loses a sender's data for no reason.
