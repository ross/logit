# Apache httpd `mod_status` interop fixtures

`mod_status` pages real httpd servers served, captured verbatim by curl with no parsing and no
trimming. Each `.body` is the bytes the server wrote, and its `.headers` sidecar holds the
response's status line and headers as received, CRLFs included.

`crates/logit-proto/tests/apache_status_interop.rs` reads every body through
`logit_proto::apache_status` and checks each event against the mapping table in that module's doc
and each variant's omitted keys, and `fuzz/seedgen` turns each body into an `apache_status` seed.

To regenerate, run `script/record-fixtures apache-status`. See `../README.md` and the header
comment above `record_apache_status` in `script/record-fixtures`.

## Fixtures

Every server was httpd 2.4.69 from
`httpd:2.4.69@sha256:c6b7745aa371570cc8460a9f08c39989583d4aa7bc8e3ed8bd77a492803ea704`, running
`tools/record-fixtures/apache-status/status.conf`, which serves nothing but `/server-status` on port
8080, with the variant chosen by an `httpd -D` name. curl
(`curlimages/curl:8.22.0@sha256:58adaa4e8dca9c988bae2aba4ab3434a0bb2da16bbe3f92dec39ec7785166777`)
fetched `/server-status?auto` twice per variant: `-0` is the first request after start, and `-1`
was fetched while `tools/record-fixtures/apache-status/connections.py`, on
`python:3.12.15-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1`,
held keepalive connections, a half-sent request, and a silent connection open. All were captured
on 2026-11-01.

| Files | Variant | `httpd -D` |
|---|---|---|
| `event-0`, `event-1` | the `event` MPM, the image's default | — |
| `worker-0`, `worker-1` | the `worker` MPM | `WORKER` |
| `prefork-0`, `prefork-1` | the `prefork` MPM | `PREFORK` |
| `ext-off-0`, `ext-off-1` | `event` under `ExtendedStatus Off` | `EXT_OFF` |
| `ssl-0`, `ssl-1` | `event` with `mod_ssl` on port 8443 and an `shmcb` session cache; `-1` after three TLS requests | `SSL` |
| `html` | the `event` server's `/server-status` without `?auto`, the HTML page | — |

## What the recording shows

- Every `?auto` page is `Content-Type: text/plain; charset=ISO-8859-1`, starts with the server
  name on a line with no `": "`, and ends every line in `\n`. The HTML page is
  `text/html; charset=ISO-8859-1`.
- `prefork` and `worker` pages have no `Processes`, `Stopping`, or `Conns*` lines.
- `ExtendedStatus Off` drops every `Total*` and `CPU*` line and the derived averages.
- `mod_ssl` adds `OpenSSL/<version>` to `ServerVersion` and appends a `TLSSessionCacheStatus`
  line and the `shmcb` cache's `Cache*` block.
- The scrape counts itself: every page's scoreboard holds a `W`.
- The `event` MPM refreshes its `Conns*` counts only when one of its timeout queues expires an
  entry, so `connections.py --waves` opens its keepalive connections in two waves, and each `event`
  `-1` page reads the second wave as `ConnsAsyncKeepAlive`, with the first wave closing.
  Under `prefork` and `worker`, each keepalive connection holds a slot, read as `K`.
- Numbers use APR's `%g`, so a fraction has no leading zero (`CPUUser: .01`).

## What isn't covered here (yet)

- **httpd 2.2**, end of life since 2017, whose page has no server-name line. The decoder's unit
  tests build a page with none instead.
- **Other session-cache providers** (`dbm`, `memcache`, `redis`), each of which prints its own
  keys after `TLSSessionCacheStatus`. All of them are unmapped, so `shmcb` stands in for the
  shape.
- **A graceful restart in progress** (`G` on the scoreboard, nonzero `GracefulWorkers` and
  `Stopping`), and **a dead slot past the worker limit** that the scoreboard keeps printing while
  its process runs.
- **Distribution builds** (Debian's `apache2`, RHEL's `httpd`), which serve the same
  `mod_status` code under their own `ServerVersion` strings.
- **A server under real load**, with a scoreboard tens of thousands of slots long or `L`, `D`, and
  `C` states.
