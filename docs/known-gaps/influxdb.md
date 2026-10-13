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
- **No query or delete API.** `logit` is a multiplexer and stores nothing, so `/api/v2/query`,
  `/api/v2/delete`, and the v1 `/query` have no handler.
  - **Consequence:** a dashboard or client that reads back through `logit` gets `404`.
  - **Workaround:** query the destination directly.
- **`org` and `bucket` are carried, not routed.** `influxdb_in` records `org`, `orgID`, and
  `bucket` as the resource attributes `influxdb.org`, `influxdb.org_id`, and `influxdb.bucket`.
  `influxdb_out` writes to the one org and bucket in its own config and ignores them.
  - **Consequence:** two producers writing to different buckets through one `influxdb_in` land in
    the one bucket `influxdb_out` names.
  - **Workaround:** a `route` on `influxdb.bucket` into one `influxdb_out` per destination.
- **A destination fault never reaches the producer.** `influxdb_in` answers `204` once the batch
  is in every downstream inbox, before `influxdb_out` writes it. A `422` for a field type
  conflict (`Rejected`, the batch drops), a `404` for a missing bucket, and a `401` for the sink's
  token (both `Refused`, the sink retries and holds its queue) are faults at the sink, so the
  producer sees none of them
  ([ADR `influxdb-line-protocol-relay`](../adr/influxdb-line-protocol-relay.md)'s "Destination
  faults never reach the producer").
  - **Consequence:** a producer with a wrong bucket or a changed field type believes its writes
    succeed.
  - **Workaround:** watch `influxdb_out`'s `batches.dropped{reason="rejected"}` and
    `logit.component.retrying`.
  - **Revisit trigger:** none planned. Forwarding the verdict would hold the producer's request
    open across the whole pipeline, which no `logit` input does.
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
- **`max_request_bytes` has no `influxd` counterpart.** `influxd` v2.9.1 enforces no body limit.
  `influxdb_in` bounds the body with `max_request_bytes`, a `logit` choice.
  - **Consequence:** a body larger than the limit that `influxd` would take is refused.
  - **Workaround:** raise `max_request_bytes`, or split the batch at the producer.
