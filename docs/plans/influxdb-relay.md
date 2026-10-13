---
created: 2026-10-13
updated: 2026-10-13
---

# Enabling plan: `influxdb_in`/`influxdb_out` — a lossless InfluxDB line-protocol relay

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
| 7 | `org`, `orgID`, and `bucket` become resource attributes `influxdb.org`, `influxdb.org_id`, `influxdb.bucket`. One request is one batch. `influxdb_out` keeps its fixed config. | `influxdb-line-protocol-relay` |
| 8 | `precision=` honored, receipt time for an absent timestamp, gzip, an optional `tokens:` allowlist (`Token` or `Bearer`), `tls:` (not `bind_tls:`), `peer`/`proxy_protocol`/`forwarded`, `503` when busy. | `influxdb-line-protocol-relay` |
| 9 | Egress is always typed: `influxdb_out` writes `Ni`, `Nu`, and `true`/`false`, with or without the carrier. | `typed-metric-scalar` |
| 10 | Destination faults never reach the producer: `influxdb_in` answers `204` once the batch is in every downstream inbox. | `influxdb-line-protocol-relay` |
| 11 | Lua reads a metric value as a double, counting an inexact read; a write equal to the current `as_f64()` keeps the variant; `Event.new` takes an optional `type`. | `typed-metric-scalar` |
| 12 | `aggregate` widens mixed types toward `F64` and counts `normalized{reason="widened"}`. | `typed-metric-scalar` |
| 13 | Numeric-only sinks write ints in their wire's integer form where it has one, `Bool` as `1`/`0` (counted), and a `U64` past `i64::MAX` at OTLP as a double (counted). | `typed-metric-scalar` |
| 14 | Native wire: a typed scalar is `[type u8][8 bytes LE]`, with no compatibility path for old frames. | `typed-metric-scalar` |
| 15 | The encoder moves to `crates/logit-proto/src/influxdb/`, beside the decoder. | `influxdb-line-protocol-relay` |
| 16 | Four stacked PRs: `influx/w0` docs, `influx/w1` `MetricValue`, `influx/w2` codec, `influx/w3` listener. | this plan |

Also settled in design: `Exemplar.value` stays `f64`. `stdio_out`'s `format: json` prints `I64`
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

**Step 0 probes a real `influxd`** (`compose.yaml`'s `influxdb` service, its image pinned by digest
in this plan) through `POST /api/v2/write`, reading each result back through `/api/v2/query`. The
answers land in this plan as "Settled by probe", as [`splunk-relay.md`](splunk-relay.md) records
its own. The probe covers escaping in every position, comments and blank lines, CRLF and tabs, an
empty body, a duplicate tag or field key in one line, reserved names (`_measurement`, `_field`,
`time`), boolean spellings, number forms and overflow, timestamp range and precision overflow, a
long string field, a type conflict within one body, the error JSON's shape and line numbering,
`Token` against `Bearer`, missing `bucket`/`org`/`orgID` and a bad `precision`,
`Content-Encoding: deflate`, `/ping` and `/health`, and the default `max-body-size`.

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
- `response.rs`: InfluxDB's `{"code","message"[,"line"]}` error body and its code constants.

`crates/logit-outputs/src/influxdb.rs` keeps only the sink. `statsd_out` and the benches import the
shared helpers from the proto module.

### `influx/w3`: listener, config, fixtures, and docs

- `crates/logit-inputs/src/influxdb.rs`, modeled on `splunk_hec_in` and reusing `crate::http`,
  `crate::listener`, and `crate::peer`. A small, capped cache maps an `(org, bucket)` pair to its
  `Arc<Resource>`. Routes: `POST /api/v2/write`; `/ping` and `/health` as the probe decides;
  `404` for everything else, `/write` included. The response table is [ADR
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
  typed fields, the string carrier, precision, receipt time, all-or-nothing with `line`, and zero
  copy; encoder tests for the carrier path and its fallback;
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

## Open risks

Each of these is to be verified by W2's probe or W3's recording, and none is settled here:

- **Backslash escaping.** On a reading of its source, the v1 `models` parser's unescape handles
  only `\,`, `\ `, `\=`, and `\"`. If `influxd` doesn't read `\\` as one backslash in a
  measurement, tag, or field key, today's encoder, which writes `\\`, doubles a backslash on every
  hop. If the probe confirms it, W2 fixes the encoder with a regression test.
- **Telegraf chunked bodies.** `tools/record-fixtures/raw_capture.py` answers `411` to a request
  with no `Content-Length`. If Telegraf's `outputs.influxdb_v2` sends chunked bodies, the recorder
  needs an `--accept-chunked` mode before W3 can record.
- **`/ping` and `/health`.** Whether Telegraf and the client libraries probe either route before
  writing, and what a real `influxd` answers, decides whether `influxdb_in` serves them.
- **The `max-body-size` default.** `influxdb_in`'s `max_request_bytes` default follows what the
  probe finds `influxd` enforcing by default.
