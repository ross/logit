---
created: 2026-10-13
updated: 2026-10-15
---

# Enabling plan: `influxdb_in`/`influxdb_out` — a lossless InfluxDB line-protocol relay

## Status

W0 to W2 are done: the ADRs, `MetricValue`, and the codec with its `influxd` probe. W3 adds the
`influxdb_in` listener, its config and graph rule, the operator docs, and the closing assessment
below.

## Context

`logit` has `influxdb_out` (InfluxDB 2.x line protocol, `POST /api/v2/write`) and no
`influxdb_in`. This plan builds the listener and makes `influxdb_in -> influxdb_out` the tenth
lossless pair under [ADR `lossless-transit`](../adr/lossless-transit.md). Scope is 2.x line
protocol over the v2 write API; the v1 `/write` route is out.

The model can't carry line protocol today, for two reasons:

1. Every metric scalar is an `f64`, and a line-protocol field is typed (`1.0`, `1i`, `1u`,
   `"text"`, `true`). The same collapse already loses OTLP `as_int` and collectd
   `COUNTER`/`DERIVE`/`ABSOLUTE` precision above 2^53. [ADR
   `typed-metric-scalar`](../adr/typed-metric-scalar.md) fixes it in the model, for every protocol.
2. A point is a measurement, tags, N named fields, and a timestamp. `influxdb_out` writes one line
   per `MetricRecord` with a fixed `value=` field, so `m,t=a f1=1,f2=2` can't round-trip. [ADR
   `influxdb-line-protocol-relay`](../adr/influxdb-line-protocol-relay.md) settles the mapping.

The stream key is `influx`.

## Decisions already settled

Settled with Ross (2026-10-13), recorded in full in the two ADRs:

| # | Decision | ADR |
|---|---|---|
| 1 | `MetricValue { F64, I64, U64, Bool }` (`Copy`, strict `PartialEq`) replaces the `f64` in `Gauge`, `GaugeDelta`, and `Sum.value`. OTLP and collectd decoders produce typed values too. | `typed-metric-scalar` |
| 2 | A string field decodes to the event attribute `influxdb.field.<key>` and leaves `influxdb_out` as a string field, never a tag. | `influxdb-line-protocol-relay` |
| 3 | Measurement carrier `influxdb.measurement`. Each numeric or boolean field is one `Gauge` record named `<measurement>.<field>`, all on one `Event` in wire order. With the carrier, `influxdb_out` writes one line per event and skips every `influxdb.*` attribute as a tag; without it, today's one-line-per-record path. | `influxdb-line-protocol-relay` |
| 4 | Tags become event attributes, `Value::Str`, never resource. | `influxdb-line-protocol-relay` |
| 5 | One unparseable line answers `400` with InfluxDB's error JSON and forwards nothing from the body. | `influxdb-line-protocol-relay` |
| 6 | Duplicate `(measurement, tags, timestamp)` in one body merges, last write wins per field. `influxdb_out`'s 1 ns nudge stays for points from different sources. | `influxdb-line-protocol-relay` |
| 7 | `org`, `orgID`, and `bucket` become resource attributes `influxdb.org`, `influxdb.org_id`, `influxdb.bucket`. One request is one batch. `influxdb_out` keeps its fixed config (superseded on 2026-10-15 by the ADR's "Amendment: `influxdb_out` follows the carried destination": with no `bucket:` configured, the sink writes to the org and bucket a batch carries). | `influxdb-line-protocol-relay` |
| 8 | `precision=` honored, receipt time for an absent timestamp, gzip, an optional `tokens:` allowlist (`Token` or `Bearer`), `tls:` (not `bind_tls:`), `peer`/`proxy_protocol`/`forwarded`, `503` when busy. | `influxdb-line-protocol-relay` |
| 9 | Egress is always typed: `influxdb_out` writes `Ni`, `Nu`, and `true`/`false`, with or without the carrier. | `typed-metric-scalar` |
| 10 | Destination faults never reach the producer: `influxdb_in` answers `204` once the batch is in every downstream inbox. | `influxdb-line-protocol-relay` |
| 11 | Lua reads a metric value as a double, counting an inexact read; a write equal to the current `as_f64()` keeps the variant; `Event.new` takes an optional `type`. | `typed-metric-scalar` |
| 12 | `aggregate` widens mixed types toward `F64` and counts `normalized{reason="widened"}`. | `typed-metric-scalar` |
| 13 | Numeric-only sinks write ints in their wire's integer form where it has one, `Bool` as `1`/`0` (counted), and a `U64` past `i64::MAX` at OTLP as a double (counted). | `typed-metric-scalar` |
| 14 | Native wire: a typed scalar is `[type u8][8 bytes LE]`, with no compatibility path for old frames. | `typed-metric-scalar` |
| 15 | The encoder moves to `crates/logit-proto/src/influxdb/`, beside the decoder. | `influxdb-line-protocol-relay` |
| 16 | Four stacked PRs: `influx/w0` docs, `influx/w1` `MetricValue`, `influx/w2` codec, `influx/w3` listener. | this plan |

Also settled in design: `Exemplar.value` stays `f64` (typed later; see [ADR `typed-metric-scalar`](../adr/typed-metric-scalar.md)'s exemplar amendment). `stdio_out`'s `format: json` prints `I64`
and `U64` as JSON integers. A string-only point is an event with no metrics that `influxdb_out`
writes under the carrier and every other sink skips. `influxdb.field.*` reaches other sinks as a
tag or label unless a `remove` drops it.

## Design

### `influx/w1`: `MetricValue`

- **Core** (`crates/logit-core/src/metric.rs`): the enum, with `as_f64`, `as_f64_exact` (returning
  whether the conversion was exact), `is_finite`, `type_name`, `From` for each scalar type, and a
  `checked_add` that implements the widening table. `MetricKind::gauge(f64)` beside
  `MetricKind::counter(f64)` keeps the sweep mechanical.
- **Sizes**: exact pins in `crates/logit-core/tests/type_sizes.rs` for `MetricValue` (expected 16)
  and `Sum` (expected 24); `MetricKind`, `MetricRecord`, and `Event` re-measured. Updated together
  with `docs/design/memory.md` and `docs/design/data-model.md`.
- **Typed decoders**: `crates/logit-proto/src/otlp/metrics.rs` (`as_int` to `I64`) and
  `crates/logit-proto/src/collectd/decode.rs` (`COUNTER` and `ABSOLUTE` to `U64`, `DERIVE` to
  `I64`). Every other decoder wraps `F64`.
- **Encoders**: every metric encoder, the native record codec, and the `stdio_out` renders, per the
  typed-egress and numeric-only rules.
- **Transforms**: `aggregate`'s accumulators hold a `MetricValue` and add with `checked_add`;
  `kv_metrics` keeps an attribute's numeric type.
- **Lua**: the metric proxy's read, write, and `to_table`, and `Event.new`'s typed form, with
  `docs/design/lua-api.md` and an amendment to [ADR
  `lua-event-constructor`](../adr/lua-event-constructor.md).
- **Allocation pins**: `crates/logit-bench/tests/allocations.rs` is expected unchanged, since the
  type is `Copy`. Any move gets a new constant and a `memory.md` row.

### `influx/w2`: the codec, `crates/logit-proto/src/influxdb/`

**Step 0 probed a real `influxd`** (`compose.yaml`'s `influxdb` service) through
`POST /api/v2/write`, reading each result back with Flux. ["Settled by probe"](#settled-by-probe-influxd-291)
records the image and the answers, as [`splunk-relay.md`](splunk-relay.md) records its own. The
lexer, decoder, and encoder follow those answers.

Files:

- `mod.rs`: the canonical decode table, encode table, and numbered normalization list; the carrier
  constants; a `Precision` enum parsed from the query string.
- `line.rs`: a zero-copy lexer returning one parsed line, or nothing for a comment or blank line,
  with the grammar the probe settles. Floats are grammar-checked before `str::parse`, so `inf`,
  `NaN`, and hex are rejected; timestamps scale with `checked_mul`.
- `decode.rs`: `InfluxDecoder::decode_body(body, precision, received_at, resource)`, returning an
  `EventBatch` or the first error with its line number. It isn't the `Decoder` trait, because it
  needs the precision, as `SplunkDecoder::decode_raw` needs its envelope. It parses every line
  before building output and merges duplicates in place.
- `encode.rs`: `InfluxLineEncoder` and its helpers, moved from `crates/logit-outputs/src/influxdb.rs`,
  plus the carrier path and typed suffixes. It counts the two drops that are silent today: a
  non-finite value and a line left with no fields.
- `response.rs`: InfluxDB's `{"code","message"}` error body and its code constants. The message
  carries the offending line's text, not a line number.

`crates/logit-outputs/src/influxdb.rs` keeps only the sink. `statsd_out` and the benches import the
shared helpers from the proto module.

### `influx/w3`: listener, config, fixtures, and docs

- `crates/logit-inputs/src/influxdb.rs`, modeled on `splunk_hec_in` and reusing `crate::http`,
  `crate::listener`, and `crate::peer`. A small, capped cache maps an `(org, bucket)` pair to its
  `Arc<Resource>`. Routes: `POST /api/v2/write`; `GET /ping` and `GET /health`, which
  need no token ("Settled by probe"); `404` for everything else, `/write` included. The response table is [ADR
  `influxdb-line-protocol-relay`](../adr/influxdb-line-protocol-relay.md)'s "Listener responses".
- `InfluxDbIn` in `crates/logit-config/src/lib.rs`, with `bind`, `tls`, `tokens`,
  `max_request_bytes`, `handshake_timeout`, `idle_timeout`, `max_connections`, `peer`,
  `proxy_protocol`, `forwarded`, and `reuse_port`; `script/schema` regenerated.
- Graph rules, the registry arm, and `docs/design/pipeline-graph.md`.
- `influxdb_out`: carrier tests through `send`, and the encoder's new counters in its telemetry.
- `fixtures/influxdb-relay.yaml`; a recorded Telegraf corpus under `testdata/interop/influxdb/`
  from `script/record-fixtures influxdb`; round-trip tests in `crates/logit-cli/tests/`; a
  `script/victoria-interop` leg against VictoriaMetrics' own `/api/v2/write`.
- Docs: `docs/deploying.md`, `docs/design/internal-telemetry.md`, a new
  `docs/known-gaps/influxdb.md`, `docs/OVERVIEW.md`, and AGENTS.md ("Nine" pairs to ten, "five
  HTTP listeners" to six, the Inputs row).

### Permitted normalizations

The numbered list is [ADR `influxdb-line-protocol-relay`](../adr/influxdb-line-protocol-relay.md)'s
"Permitted normalizations". `crates/logit-proto/src/influxdb/mod.rs` carries the same numbers.

## Workstreams

| # | Branch | PR | Depends |
|---|---|---|---|
| W0 | `influx/w0` | **Docs.** [ADR `typed-metric-scalar`](../adr/typed-metric-scalar.md), [ADR `influxdb-line-protocol-relay`](../adr/influxdb-line-protocol-relay.md), rows in `docs/adr/README.md`; this plan and its row in `docs/plans/README.md`; the tenth-pair amendment to `lossless-transit`; the "InfluxDB line protocol" section of `telemetry-landscape.md` | — |
| W1 | `influx/w1` | **`MetricValue`.** The core type, the size pins, typed OTLP and collectd decoders, every encoder, `aggregate`, Lua, the native wire, `memory.md`, `wire-protocol.md`, and the known-gaps and ADR amendments `typed-metric-scalar` lists | W0 |
| W2 | `influx/w2` | **Codec.** The `influxd` probe; `logit_proto::influxdb` (lexer, decoder, moved encoder with the carrier path, error body); the fixed-point suite, robustness tests, and a fuzz target; bench fixture and allocation rows; known-gaps rows for the codec | W1 |
| W3 | `influx/w3` | **Listener.** `influxdb_in`, `InfluxDbIn` config, graph rules, registry, schema; `influxdb_out` carrier tests; fixture config, recorded Telegraf corpus, round-trip tests, victoria-interop leg; operator docs, known gaps, AGENTS.md; this plan's closing assessment | W2 |

Landing order: W0 → W1 → W2 → W3, linear. Each branch is cut from its parent's branch, its PR
targets that branch, and it retargets to `main` when the parent merges. A branch is brought up to
date with `git merge`, never a rebase.

## Verification

Every PR: `script/cibuild` passes. W1 and W3 regenerate and commit `script/schema` if a config type
changes. Allocation constants and `memory.md` rows change together and are never relaxed to an
inequality.

- **W0**: every relative link resolves (`script/test -p logit-cli doc_links`); ADR headings match
  `TEMPLATE.md`; both READMEs gain rows.
- **W1**: regression tests, each run once with its fix reverted: an OTLP `as_int` point relays as
  `as_int`; a collectd `COUNTER` of 2^53 + 1 round-trips; `aggregate`'s widening cases; a Lua
  inexact read is counted, a no-op write keeps `I64`, and `Event.new{type = "u64"}` builds a `U64`;
  every variant round-trips the native codec. `script/unsafe-check fuzz-seed` and a short
  `fuzz-all`.
- **W2**: lexer unit tests per probe row, each carrying its provenance; decoder tests for merging,
  typed fields, the string carrier, precision, receipt time, all-or-nothing with a message that
  names the offending line, and zero copy; encoder tests for the carrier path and its fallback;
  `crates/logit-proto/tests/influxdb_fixed_point.rs` (`decode(encode(b)) == b` and
  `encode(decode(encode(b))) == encode(b)`, plus a proptest over normalized generators); truncation
  and bit-flip robustness; `script/unsafe-check fuzz influxdb_line` for 600 seconds.
- **W3**: listener socket tests on `logit_pipeline::test_util` with no sleeps, covering every row
  of the response table, gzip, tokens, the all-or-nothing `400`, merged duplicates, precision,
  receipt time, a resource per org and bucket, busy and closed `503`s, sender attributes, and
  shutdown; `script/validate`; `script/record-fixtures influxdb`; `script/victoria-interop`.
- **Manual real-stack run** (W3): one Telegraf with two `outputs.influxdb_v2`, one writing direct
  to bucket `direct` and one through `logit run fixtures/influxdb-relay.yaml` to bucket `relayed`.
  A Flux `from(bucket:)` over both returns identical tables, field types included.

A wire feature the fixture corpus must cover is listed in
[`telemetry-landscape.md`](../design/telemetry-landscape.md)'s "InfluxDB line protocol" section.

## Settled by probe (influxd 2.9.1)

The W2 probe wrote each case to `docker.io/library/influxdb:2` (the tag `compose.yaml` uses),
digest `sha256:f75e48af0598e8aec7986e991a848d19a119101a7d563a2e5db1dfaac9c45daa`, which reports
`version: v2.9.1` (commit `d4fa1941fd`), and read it back with Flux or InfluxQL. ADR
`influxdb-line-protocol-relay` records what each answer changed. These facts bind the codec and
the listener:

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
    for `400` and `{"code":"unprocessable entity","message":"..."}` for `422`. There is **no `line`
    field**. When several lines fail to parse, one message joins their `unable to parse` clauses
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

## Closing assessment

`influxdb_in -> influxdb_out` is a fixed point modulo the numbered normalizations in
`crates/logit-proto/src/influxdb/mod.rs`'s module doc. The residual debt is listed once, in
[`docs/known-gaps/influxdb.md`](../known-gaps/influxdb.md) (the unsupported v1 route, no query or
delete API, `org` and `bucket` carried but not routed (superseded on 2026-10-15 by the ADR's
"Amendment: `influxdb_out` follows the carried destination", whose own gaps replace that entry),
destination faults the producer never sees,
and the places the listener's answers differ from `influxd`) and in the `encode (InfluxDB)` and
`decode (InfluxDB)` rows of [`docs/known-gaps/mappings.md`](../known-gaps/mappings.md) (what
`influxdb_out` drops or reshapes from other protocols, and the carrier at other sinks). A
Telegraf-to-`logit`-to-InfluxDB run with a Flux comparison is the one verification this plan lists
that needs a real stack.

## Open risks

- **Telegraf chunked bodies.** `influxd` accepts a chunked body, but
  `tools/record-fixtures/raw_capture.py` answers `411` to a request with no `Content-Length`. If
  Telegraf's `outputs.influxdb_v2` sends chunked bodies, the recorder needs an `--accept-chunked`
  mode before W3 can record. W3's recording verifies it.
- **The `max_request_bytes` default.** `influxd` enforces no limit, so there is no default to
  follow. The value is a `logit` choice, set in W3 and documented as a protection that `influxd`
  lacks.
- **Whether clients probe `/ping` or `/health`.** `influxd` answers both without a token, and the
  listener serves both. W3's recording shows which of Telegraf and the client libraries call them.
