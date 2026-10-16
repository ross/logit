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
- **A destination fault never reaches the producer.** `influxdb_in` answers `204` once the batch
  is in every downstream inbox, before `influxdb_out` writes it. A `422` for a field type
  conflict (`Rejected`, the batch drops), a `404` for a missing bucket (`Refused`, the sink
  retries and holds its queue, or `Rejected` when the bucket or org came from the client), and a `401` for
  the sink's token (`Refused`) are faults at the sink, so the producer sees none of them
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
