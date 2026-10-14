# InfluxDB interop fixtures

Real `/api/v2/write` traffic from Telegraf's `outputs.influxdb_v2`, captured verbatim by
`tools/record-fixtures/raw_capture.py` with no decompression, parsing, or re-encoding.
`logit`'s line-protocol codec (`crates/logit-proto/src/influxdb/`) and `influxdb_in` were written
from InfluxDB's documentation and probes of an influxd; these captures check that reading against
what a real, widely deployed writer puts on the wire. Nothing here came from, or went to, an
InfluxDB: the capture sink stood where `/api/v2/write` would, answering `204` to every write. The
sink's `--accept-chunked` removes the chunk framing from a `Transfer-Encoding: chunked` request
(Telegraf streams every body chunked, whatever its encoding); the payload is untouched and the sidecar keeps
`transfer-encoding: chunked`.

Every request carried the dummy token `00000000-0000-0000-0000-000000000000`. It never authorized
anything. The data is cpu, memory, and load of the recording host, plus the committed
`tools/record-fixtures/telegraf-influxdb-lines.txt`.

To regenerate, run `script/record-fixtures influxdb`. See `../README.md` for the corpus-wide rules.

## Producer

Telegraf 1.40.2 (`docker.io/library/telegraf:1.40.2`, digest
`sha256:aedc74ac64c623b6e6760545a8ff56a63b99a798bba3d361279a2887642798cf`), configured by
`tools/record-fixtures/telegraf-influxdb-v2.conf`: `inputs.cpu`, `inputs.mem`, `inputs.system`,
and `inputs.file` (`data_format = "influx"`) over `telegraf-influxdb-lines.txt`; a 5 s interval
and flush; hostname `telegraf-fixture`; two `outputs.influxdb_v2` sections with
`influx_uint_support = true`, one to bucket `fixture-gzip` (the default gzip) and one to
`fixture-identity` (`content_encoding = "identity"`). No `precision` is sent, so the bodies are
nanoseconds.

## Fixtures

Each request is a body (`.bin`) and a `.headers` sidecar holding the method, the path with its
query string, and every request header, lowercased. Files are numbered in arrival order
(`raw_capture.py --name-by-path`); the bucket in the sidecar's `path:` and its
`content-encoding` line say which output sent it. A run keeps six writes, three flushes of both
outputs; which output arrives first within a flush varies between recordings.

| Files | Output | Captured | Construct exercised |
|---|---|---|---|
| `telegraf-api-v2-write-000.*` (2,161 B) | `fixture-identity`, identity, chunked | 2026-10-13 | The first flush: `mem` (a `u` field per `influx_uint_support`), `inputs.file`'s points, and `system` as three lines (`n_cpus`/loads, `uptime`, and the string `uptime_format`) at one instant. No `cpu`: it needs a prior sample |
| `telegraf-api-v2-write-001.*` (937 B) | `fixture-gzip`, gzip, chunked | 2026-10-13 | The same first flush, gzipped and streamed |
| `telegraf-api-v2-write-00{2,5}.*` (3,204 B, 3,715 B) | `fixture-gzip`, gzip, chunked | 2026-10-13 | Later flushes: 32 `cpu` series (per-CPU and `cpu-total`) of `F64` usage fields |
| `telegraf-api-v2-write-00{3,4}.*` (11,866 B, 12,663 B) | `fixture-identity`, identity, chunked | 2026-10-13 | The same later flushes, uncompressed |

The `inputs.file` points repeat in every body at their fixed 2023 timestamps. They hold: every
field type (`true`/`false`, `i`, `u` up to `u64::MAX`, floats, a string); two multi-field points
on distinct series; three points of one series and instant (`interop_dup`), which Telegraf writes
as three lines and the decoder merges; escapes in a measurement, tag key, tag value, and field key
(Telegraf's serializer rewrites them in its own order, tags sorted); a string with `\"` and `\\`;
a string with a raw newline; and non-ASCII names and values. Telegraf adds `host=telegraf-fixture`
to every point and writes `1.5e-7` as `0.00000015`.
