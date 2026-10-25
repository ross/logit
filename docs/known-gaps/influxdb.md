# Known gaps: InfluxDB

Entry format and the other areas: [the known-gaps index](README.md). The codec's per-field
mapping rows are in [mappings.md](mappings.md) (`encode (InfluxDB)`, `decode (InfluxDB)`), and
`crates/logit-proto/src/influxdb/mod.rs`'s "Deviations from influxd" is the full list of places
the decoder differs from `influxd`.

- **The v1 `/write` route is unsupported.** `influxdb_in` serves `POST /api/v2/write` only; it
  answers `404` for `/write`, and for every other path but `/ping` and `/health`.
  - **Consequence:** a v1 client (`db=` and `rp=` parameters, no org) can't write to `logit`.
  - **Workaround:** point the client at the v2 API, or run Telegraf's `outputs.influxdb_v2`.
  - **Revisit trigger:** a producer that can only speak v1.
- **A carried bucket's or org's `403` or `404` drops the batch rather than holding it.** With no
  `bucket:` configured, `influxdb_out` writes each batch to the bucket `influxdb_in` received it
  for, and with no `org:`, to the client's org, else to `default_org:`. It reads a `403` or `404`
  as `Rejected` when the bucket or org it could concern was carried, so a bucket a client names
  that doesn't exist or that the relay's token can't write doesn't hold every other bucket's
  batches behind it
  ([ADR `influxdb-line-protocol-relay`](../adr/influxdb-line-protocol-relay.md)'s "Amendment:
  `influxdb_out` follows the carried destination (2026-10-15)").
  - **Consequence:** a transient `403` or `404`, such as a write to a bucket that's still being
    created or a token whose grant is being widened, loses those batches, counted
    `batches.dropped{reason="rejected"}`. `bucket:` alone doesn't prevent this: the client's org
    is still forwarded, and a `403` or any `404` drops, a missing `bucket:` included, because
    InfluxDB 2.x looks the bucket up inside that org. Nor does `default_org:`, because a carried
    org wins over it; only a batch that carries no org is held.
  - **Workaround:** create buckets and grant the relay's token before producers write to them, or
    configure both `bucket:` and `org:`, where a `403` or `404` holds and retries.
  - **Revisit trigger:** a deployment that needs a missing bucket held. That needs a queue per
    destination in place of `write_loop`'s single-head FIFO.
- **A relay's token decides which buckets a client reaches.** With no `bucket:` or no `org:`
  configured, any client `influxdb_in` accepts can write to any bucket the relay's token can
  write: a forwarded org picks among same-named buckets. `tokens:`
  admits a client without binding its token to a bucket. On InfluxDB 2.x a bucket ID isn't
  confined by the org sent beside it: 2.7.12 wrote a point to org B's bucket for a token scoped to
  org A, sent with `org=A` and that bucket's ID.
  - **Consequence:** a relay with an all-access token lets every accepted client write to every
    bucket, in any org a forwarded bucket ID names.
  - **Workaround:** give the relay a token scoped to the buckets its producers may use, or
    configure both `bucket:` and `org:`.
- **InfluxDB 3 Core creates a database for any bucket it doesn't know.** 3.8.0 Core answers `204`
  to a write naming an unknown bucket and creates a database of that name, and it ignores `org`
  and `orgID`.
  - **Consequence:** in carried mode, a client's typo in its bucket becomes a new database at the
    destination rather than a counted drop at `influxdb_out`.
  - **Workaround:** set `bucket:`, which is enough here because InfluxDB 3 ignores the org, or
    check the destination's database list.
- **`influxdb_in` answers some requests differently from `influxd`.** `influxd` decompresses only
  lowercase `gzip`, passes every other `Content-Encoding` through undecoded, and parses the raw
  bytes; `influxdb_in` answers `415` for any value but lowercase `gzip` or identity. A body labeled
  `gzip` that doesn't decompress gets `400` here and `500` at `influxd`. A route with a trailing
  `/` gets `404` here, where `influxd` answers `308` with a redirect.
  - **Consequence:** a client that labels a plain body `br` or `deflate`, which `influxd`
    accepts, gets `415` from `logit`. A client that follows redirects, or that relies on `500`
    for a corrupt gzip body, sees `404` or `400` instead.
  - **Workaround:** drop the `Content-Encoding` header or send `gzip`, and write to the route
    without a trailing `/`.
- **The pair hasn't been compared end to end against a real InfluxDB.** The evidence is a
  recorded Telegraf corpus, the fixed-point suite, and a probe of `influxd` 2.9.1. No run has
  sent one Telegraf through two `outputs.influxdb_v2`, one writing direct to a bucket `direct` and
  one through `logit run fixtures/influxdb-relay.yaml` to a bucket `relayed`, and checked that a
  Flux `from(bucket:)` over both returns identical tables, field types included.
  - **Consequence:** a difference that only a real Telegraf and `influxd` together expose, such
    as a field type the corpus doesn't hold, would go unnoticed.
  - **Revisit trigger:** a change to the codec's typed fields or escaping, or a user report of a
    relayed table that differs from a direct one.
- **`flatten` ahead of `influxdb_out` hasn't run against a real InfluxDB.**
  [fixtures/nested-json-to-influxdb.yaml](../../fixtures/nested-json-to-influxdb.yaml) runs
  `tail_in -> json -> flatten -> keep -> kv_metrics -> aggregate -> influxdb_out`, but
  `script/server` and `compose.yaml` don't run it, and `script/validate` only loads it. That
  dotted tags such as `req.method` reach InfluxDB is checked by unit and integration tests
  against the encoder, not by a write to `influxd` and a query back.
  - **Consequence:** a difference in how `influxd` stores or returns a dotted tag key, which the
    encoder's tests can't see, would go unnoticed.
  - **Revisit trigger:** a change to `flatten`'s key syntax or to `influxdb_out`'s tag escaping, or
    a user report of a dotted tag missing at the destination.
- **`influxdb_out` writes no start time.** `otel2influx`, the OTel Collector's InfluxDB exporter
  and Telegraf's `inputs.opentelemetry`, adds a `start_time_unix_nano=<n>i` field to a point whose
  start time is set. `influxdb_out` leaves a record's `start_timestamp` off, a structural loss
  with no counter ([ADR `influxdb-histogram-summary-shape`](../adr/influxdb-histogram-summary-shape.md)).
  - **Consequence:** a delta or cumulative series' window start doesn't reach InfluxDB, so a
    reader can't tell a reset from a new series.
  - **Revisit trigger:** a reader that needs the start time; `influx2otel` v0.5.12 ignores the
    field.
- **`influxdb_out` writes no exemplars.** `otel2influx` writes each exemplar of a sum, gauge, or
  histogram as a point of its own, measurement `<name>_exemplar`, field `gauge`, tagged
  `trace_id` and `span_id`. `influxdb_out` leaves exemplars off, a structural loss with no
  counter.
  - **Consequence:** the trace behind an observation doesn't reach InfluxDB.
  - **Revisit trigger:** a user who links InfluxDB data to traces.
- **A gauge or a sum writes `value`, where upstream writes `gauge` or `counter`.** `otel2influx`'s
  `telegraf-prometheus-v1` schema writes a gauge's field as `gauge` and a sum's as `counter`, and
  `influx2otel` reads those names to choose the kind. `influxdb_out` writes `value` for both,
  though its histogram and summary fields follow that schema.
  - **Consequence:** `influx2otel` doesn't recognize a `logit` gauge or sum as one, and a query
    written for Collector data reads a different field name.
  - **Revisit trigger:** a reader that needs the upstream names. The change renames every
    gauge and sum series at the destination.
