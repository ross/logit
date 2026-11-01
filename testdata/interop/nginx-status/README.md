# nginx `stub_status` interop fixtures

`stub_status` pages four nginx-family servers served, captured verbatim by curl with no parsing
and no trimming. Each `.body` is the bytes the server wrote, and its `.headers` sidecar holds the
response's status line and headers as received, CRLFs included.

`crates/logit-proto/tests/nginx_status_interop.rs` reads every body through
`logit_proto::nginx_status` and checks each event against the mapping table in that module's doc,
and `fuzz/seedgen` turns each body into an `nginx_status` seed.

To regenerate, run `script/record-fixtures nginx-status`. See `../README.md` and the header
comment above `record_nginx_status` in `script/record-fixtures`.

## Fixtures

Every server ran `tools/record-fixtures/nginx-status/status.conf`, which serves nothing but a
`stub_status` location on port 8080. curl (`curlimages/curl:8.22.0@sha256:58adaa4e8dca9c988bae2aba4ab3434a0bb2da16bbe3f92dec39ec7785166777`)
fetched each page twice: `-0` is the first request after start, and `-1` was fetched while
`tools/record-fixtures/nginx-status/connections.py`, on
`python:3.12.15-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1`,
held three idle connections and one half-sent request open. All were captured on 2026-10-31.

| Files | Server | Image |
|---|---|---|
| `nginx-stable-0`, `nginx-stable-1` | nginx 1.30.6 | `nginx:1.30.6@sha256:66b50d24288dba67f3d000b31223daefc4868c58d29aaacb6a4ff9771597b5eb` |
| `nginx-mainline-0`, `nginx-mainline-1` | nginx 1.31.8 | `nginx:1.31.8@sha256:5ab5b0b01097420873a3f3330f44248e34a3c2a3e8d5ee30658aeae8cb7435da` |
| `angie-0`, `angie-1` | Angie 1.12.4 (`nginx/1.31.2` core) | `docker.angie.software/angie:1.12.4@sha256:5e0cffc7863bdf9562db301c114f0a8cba21c3727854aa22c822d0fc7bf6dea4` |
| `freenginx-0`, `freenginx-1` | freenginx 1.30.2 | built from `https://freenginx.org/download/freenginx-1.30.2.tar.gz` (SHA-256 `3166632fe3c74892e3fb216ed36d36e1bcaafa46a54d28c00d575998bc7ea927`) with `--with-http_stub_status_module` on `debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3`, by `tools/record-fixtures/nginx-status/build-freenginx.sh`; freenginx publishes no image |

## What the recording shows

- All four servers write the same 97-byte page for the same state: lines 1, 3, and 4 end in a
  space, and the number line starts with one.
- The scrape counts itself: a `-0` page reads `Active connections: 1`, `Writing: 1`, and
  `requests` 1.
- An idle keepalive connection that hasn't sent a byte is `Waiting`, and a half-sent request is
  `Reading` and already counted in `requests`, so each `-1` page reads `requests` 3 after two
  scrapes.
- Every server answers `Content-Type: text/plain`.

Tengine has no official image, so its four-column form isn't recorded here. The decoder's unit
tests build it from Tengine's handler format strings instead.
