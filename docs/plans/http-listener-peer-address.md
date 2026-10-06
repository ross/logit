---
created: 2026-10-05
updated: 2026-10-05
---

# Enabling plan: sender addresses on the HTTP listeners — `peer:`, `proxy_protocol:`, and `forwarded:`

## Status

Complete. Every PR in the table under "PRs" landed, and "Closing assessment" records what each
one did, the residual debt, and the end-to-end run. The decisions are in amendments to [ADR
`listener-peer-address`](../adr/listener-peer-address.md) and [ADR
`http-access-normalization`](../adr/http-access-normalization.md), and in [ADR
`forwarded-header-parsing`](../adr/forwarded-header-parsing.md).

## Goal

Every HTTP listener can record who sent each event, opt-in, with the attribute names and semantics
ADR `listener-peer-address` gave the shared-driver listeners. Stream key `hpeer`.

- `peer: true` stamps `network.peer.address` and `network.peer.port`: the socket peer.
- `proxy_protocol: true` reads a PROXY v1 or v2 header and stamps the origin it names as
  `client.address` and `client.port`.
- `forwarded: <header>` reads an HTTP forwarding header on each request and stamps `client.*`
  from it.

The listeners in scope are `otlp_in` (HTTP and gRPC), `datadog_in`, `datadog_trace_in`,
`splunk_hec_in`, and `prometheus_in`'s remote-write receiver.

## Non-goals

- **Unifying the five HTTP accept loops.** They're separate by design:
  `crates/logit-inputs/src/http.rs`'s module doc keeps `serve_connection` per listener. This stream
  adds shared helpers and calls them from each loop.
- **A trusted-proxy list or a hop count for forwarding headers.** [ADR
  `deployment-threat-model`](../adr/deployment-threat-model.md) puts the trust boundary with the
  operator and rejects a per-listener untrusted mode. A client that spoofs a forwarding header is
  the crafted-input case. The `forwarded` entry in [transform gaps](../known-gaps/transforms.md)
  becomes the one non-goal entry for it, covering the listeners and `http_access` alike, and
  [intake gaps](../known-gaps/intake.md)' `proxy_protocol:` entry points at it. An allowlist of
  trusted PROXY sources is separate: it stays deferred work in ADR `listener-peer-address`'s
  "Consequences", and the intake entry keeps its revisit trigger.
- **A mutual-TLS client's identity, `SO_PEERCRED`, and `network.connection.id`.** These stay
  deferred, as in ADR `listener-peer-address`'s "Consequences".
- **Reverse DNS.** Rejected for the reasons in ADR `listener-peer-address`'s "Peer attributes".
- **`logit_in`.** See decision 1.

## Decisions settled before W0

Agreed on 2026-10-05. The two ADRs record the reasoning.

1. **`logit_in` is out of scope.** It belongs with planned work on the native protocol. It stays in
   [runtime gaps](../known-gaps/runtime.md)'s peer entry, which `w5` narrows to `logit_in` alone.
2. **A forwarding header beats a PROXY origin.** An L7 proxy behind an L4 one (an NLB in front of
   nginx) puts the L7 proxy in the PROXY header and the client in `X-Forwarded-For`. When the
   header is absent or unparseable, the PROXY origin stands.
3. **One forwarding header per listener:** `forwarded: x_forwarded_for | forwarded | x_real_ip`.
   Only the named header is read, and any other is ignored even when present. It's always clear
   which header produced `client.address`, and a proxy change means a config change.
4. **`http_access` moves to the shared parser** in the forwarding-header PR, and its config takes
   the listeners' shape: `forwarded: x_forwarded_for | forwarded | x_real_ip`, reading
   `http.request.header.<name>`, in place of `forwarded: {trust: true}`. Its `first_hop` stamps
   `203.0.113.7:5678` or `[2001:db8::1]:443` into `client.address` as written. The shared parser
   strips the port and brackets. A parsed header replaces `client.address` and `client.port` as a
   pair, as on the listeners: the header's port when it carries one, and otherwise no
   `client.port`, removing the one the web server logged, which is the proxy's ephemeral port
   once a forwarding header is in play. Both are changes to `http_access` output, which the
   pre-release no-compatibility rule allows.

## Design

### What the survey found

- The five accept loops (`otlp.rs`, `datadog.rs`, `datadog_trace.rs`'s TCP side, `splunk.rs`, and
  `prometheus.rs`'s receiver) share one order:
  1. Accept.
  2. Take a permit without waiting. Over the cap, the stream is dropped and counted.
  3. Spawn a task.
  4. In the task, run the TLS accept, or a one-byte `peek`, under `handshake_timeout`.
  5. Hand the stream to hyper.
- `otlp_in` discards the peer address. `datadog_in`, `datadog_trace_in`, `splunk_hec_in`, and the
  remote-write receiver keep it for diagnostic text only, never a tag.
- Every HTTP listener has one point per request where all its batches exist, before delivery:

  | Listener | Stamp point | Batches per request |
  |---|---|---|
  | `otlp_in` HTTP | `handle_http`, after decode | One per resource |
  | `otlp_in` gRPC | `handle_grpc`, after decode | One per resource |
  | `datadog_in` | `respond`, before `deliver_with_deadline` | One, or one per `TracerPayload` or stats payload |
  | `datadog_trace_in` | `respond`, beside `apply_tracer_headers` | One |
  | `splunk_hec_in` | `respond`, before the split into a deadline-bound first batch and detached rest | One per envelope |
  | Remote-write receiver | `write_response`, before building the batch | One |

  Two things constrain the order of work:
  - Every handler but `datadog_trace_in`'s `respond` consumes the request with `req.into_body()`
    before it decodes: `handle_http`, `handle_grpc`, `datadog_in`'s `respond`, `splunk_hec_in`'s
    `respond`, and `write_response`. Each of those five reads the forwarding header, or splits the
    request with `into_parts`, before it collects the body, and the stamp itself stays after
    decode. `datadog_trace_in` already splits the request with `req.into_parts()` for
    `apply_tracer_headers`.
  - `splunk_hec_in` stamps before its split, because a code 6 can follow a delivered prefix.
- `datadog_trace_in` also accepts on a Unix socket, which has no `SocketAddr`.

### Shared pieces

All in `logit-inputs`, beside the `peer` stack's code, except the parser:

- **`read_proxy_header`** moves out of `tcp.rs` into a module the HTTP loops can call on a raw
  `TcpStream`. Its behavior doesn't change: it peeks, then consumes, never past the header, under
  `handshake_timeout`.
- **A per-connection origin**, built once per connection from the socket peer and the PROXY
  result. It extends `ConnectionAttrs` from `peer.rs` and is captured in each `service_fn` closure.
  It replaces `Shared.peer`, `datadog_trace_in`'s `Peer`, and the remote-write receiver's `peer`
  parameter where those only feed diagnostics, and the diagnostics read it in their place.
- **A stamp helper over `&mut [EventBatch]`**, so a multi-batch request stamps every batch. The
  per-event cost stays a reference-count clone, as on the shared drivers.
- **A forwarding-header parser in `logit-proto`**, beside `proxy.rs`, so it joins the fuzz targets
  and `http_access` can use it (decision 4). One function takes the configured header's name and
  its value's bytes, and returns an `IpAddr` and an optional port, or the reason it stamps nothing.

### Part 1: `peer:`

- An opt-in `peer: bool` on all five listeners, stamped at the points in the survey's table.
- `datadog_trace_in`'s Unix socket follows the shared drivers' `unix_stream` rule: a bound
  client's path is the address, with no port. An unbound client, the usual tracer, gets nothing.
- Each listener's permitted-normalization list names the attributes as an opt-in addition, in the
  codec module doc that's canonical for it (`crates/logit-proto/src/{otlp,datadog,splunk,prometheus}`)
  and in [ADR `lossless-transit`](../adr/lossless-transit.md).

### Part 2: `proxy_protocol:`

- An opt-in `proxy_protocol: bool` on all five listeners, on TCP only. Graph rule 79, which
  rejects `proxy_protocol: true` off TCP, extends to them. On `datadog_trace_in` it applies to the
  `bind:` listener, and a `socket:`-only `datadog_trace_in` is rejected.
- The header is read in the spawned task, after the permit and before the TLS accept or the
  one-byte `peek`, under the same `handshake_timeout`. The plaintext `peek` comes after the header,
  because otherwise it reads the header's first byte as the request's.
- Health checks: the HTTP loops treat a `peek` of `Ok(0)` as a health check. A PROXY-aware check
  sends a header and then an RST, as HAProxy 3.2 does. On a plaintext listener, a complete
  header followed by an RST or a FIN ends the connection quietly at the first-byte `peek`. On a TLS
  listener the check reaches `connection_error` from the TLS accept, as on the shared stream driver
  in `tcp.rs`. Each listener gets a header-then-RST test.
- A rejected header counts as `logit.input.connections.rejected{reason="proxy_header"}` with the
  `proxy_header` diagnostic, as on the shared stream driver.

### Part 3: `forwarded:`

ADR `forwarded-header-parsing` is the canonical account of the parsing rules, the precedence, and
the trust stance. In brief:

- An opt-in `forwarded: x_forwarded_for | forwarded | x_real_ip` on the five HTTP listeners, off by
  default. gRPC metadata is HTTP/2 headers, so `otlp_in` gRPC needs nothing extra.
- Each request reads the first instance of the configured header, and no other header (decision
  3).
- A parsed header replaces a PROXY-derived `client.*` for that request (decision 2), as a pair:
  `client.address`, and `client.port` when the header carries a port. A header with no port
  leaves no `client.port`, as a PROXY `AF_UNIX` origin does. `network.peer.*` always stays the
  socket peer.
- The field's doc states the trust assertion in the same words `proxy_protocol:` uses for
  reachability: the operator asserts their proxy sets the header.

## PRs

Stacked, as the stacking rules below the table describe.

| Branch | Content |
|---|---|
| `hpeer/w0` | This plan, an amendment to ADR `listener-peer-address` extending it to these listeners (its deferred list narrows to `logit_in`), ADR `forwarded-header-parsing` for part 3, and an amendment to ADR `http-access-normalization` superseding its `forwarded: {trust: true}`. |
| `hpeer/w1` | The shared pieces: `read_proxy_header` moved, the per-connection origin, and the batch stamp helper. No behavior change. |
| `hpeer/w2` | `otlp_in` (HTTP and gRPC): `peer:` and `proxy_protocol:`, with the header-then-RST test. Sets the pattern. |
| `hpeer/w3a` | `datadog_in` and `datadog_trace_in`, including the Unix socket. |
| `hpeer/w3b` | `splunk_hec_in`. |
| `hpeer/w3c` | The remote-write receiver. |
| `hpeer/w4a` | The forwarding-header parser in `logit-proto` with a fuzz target and seeds, and `http_access` moved onto it with the listeners' config shape (decision 4), including dashed aliases for `forwarded` and `x-real-ip`. It updates the docs it contradicts: [`docs/http-access-logs.md`](../http-access-logs.md), [the `http_access` plan](http-access-normalization.md), the all-or-nothing `forwarded` entry in transform gaps, rewritten as the shared spoofed-header non-goal for the listeners and `http_access`, and the telemetry, graph-rule, and allocation tables `http_access`'s `forwarded` path appears in. |
| `hpeer/w4b` | `forwarded:` on the five HTTP listeners, over `w4a`'s parser. |
| `hpeer/w5` | Operator docs ([`docs/deploying.md`](../deploying.md)'s "Recording the sender" section grows to cover these listeners), the runtime-gaps peer entry narrowed to `logit_in`, a pointer from intake gaps' `proxy_protocol:` entry to the spoofed-header non-goal, and an end-to-end run. |

- `w1` stacks on `w0`, and `w2` on `w1`.
- `w3a`, `w3b`, and `w3c` are siblings off `w2` and can be built in parallel.
- `w4a` stacks on `w0` alone, whose ADR its docs link, and not on `w1`–`w3c`, so the parser is
  reviewed once before five call sites depend on it.
- `w4b` stacks on `w4a` and on whichever of `w3a`–`w3c` lands last.
- `w5` stacks on `w4b`.

## Verification

- **Every PR:** `script/cibuild`, and real-socket tests of each listener with each option on and
  off, using the shared `logit_pipeline::test_util` helpers.
- **Allocation pins:** the off path moves no existing pin in
  `crates/logit-bench/tests/allocations.rs`. A multi-batch stamp gets a pin of its own if its cost
  differs from the shared drivers' stamp.
- **`w4a`:** the forwarding-header parser fuzzed for at least 600 s; unit vectors from RFC 7239's
  examples, including a quoted IPv6 address with a port, a bracketed IPv6 address with no port
  (`for="[2001:db8::cafe]"`), and `unknown`; an unbracketed IPv6 address (`2001:db8::1`), which
  keeps its last group; and `http_access`'s existing `X-Forwarded-For` tests rewritten for the
  stripped form and the `client.port` pair.
- **`w5` end to end:**
  - HAProxy with `send-proxy-v2` in front of `otlp_in` (HTTP and gRPC).
  - nginx setting `X-Forwarded-For` in front of `otlp_in` HTTP and `splunk_hec_in`.
  - Envoy setting `X-Forwarded-For` in front of `otlp_in` gRPC.
  - An L4-then-L7 chain (HAProxy in TCP mode into nginx), to confirm decision 2's precedence.
  - For each: the expected `client.*` and `network.peer.*` on the JSON event, health checks quiet,
    and a direct connection to a `proxy_protocol:` port rejected and counted.

## Closing assessment

### What landed

| PR | Branch | What it did |
|---|---|---|
| #546 | `hpeer/w0` | This plan, ADR `forwarded-header-parsing`, and the amendments to ADR `listener-peer-address` and ADR `http-access-normalization`. |
| #547 | `hpeer/w1` | The shared pieces in `crates/logit-inputs/src/peer.rs`: `read_proxy_header` moved out of `tcp.rs`, `ConnectionPeer` built once per connection, and the multi-batch stamp. No behavior change. |
| #548 | `hpeer/w2` | `peer:` and `proxy_protocol:` on `otlp_in` over HTTP and gRPC, with the header-then-RST health-check test. |
| #550 | `hpeer/w3a` | The same on `datadog_in` and `datadog_trace_in`, including the Unix socket's bound-path rule and a `socket:`-only `proxy_protocol:` rejected. |
| #551 | `hpeer/w3b` | The same on `splunk_hec_in`, stamped before the split a code 6 can follow. |
| #552 | `hpeer/w3c` | The same on `prometheus_in`'s remote-write receiver, receiver mode only. |
| #555 | `hpeer/w4a` | `logit_proto::forwarded`, its fuzz target and seeds, and `http_access` moved onto it with `forwarded: x_forwarded_for \| forwarded \| x_real_ip`. |
| #558 | `hpeer/w4b` | `forwarded:` on the five HTTP listeners, replacing a PROXY origin's `client.*` per request. |
| This PR | `hpeer/w5` | [`docs/deploying.md`](../deploying.md)'s "Recording the sender" rewritten to cover every listener; the runtime-gaps peer entry narrowed to `logit_in`; a pointer from intake gaps' `proxy_protocol:` entry to the spoofed-header non-goal; the sender-attribute pointer in the `statsd`, `syslog`, `graphite`, and `collectd` codec docs; and the end-to-end run below. |

### Residual debt

- **`logit_in`** records no sender ([runtime gaps](../known-gaps/runtime.md)), per decision 1.
- **An allowlist of trusted PROXY sources** stays deferred ([intake gaps](../known-gaps/intake.md)).
- **A mutual-TLS client's identity, `SO_PEERCRED`, and `network.connection.id`** stay deferred, as
  ADR `listener-peer-address`'s "Consequences" lists them.
- **A spoofed forwarding header** is a non-goal, not debt ([transform gaps](../known-gaps/transforms.md)).

### End-to-end run

Run on 2026-10-05 against a release build of `hpeer/w4b`'s head (`b2f43cce`), each leg in a
throwaway compose project on its own `172.31.<leg>.0/24` network: clients at `.10` to `.14`,
HAProxy at `.20`, nginx at `.30`, `logit` at `.40`, and Envoy at `.50`. Images:
`haproxy:3.2-alpine`, `nginx:1.27-alpine` (nginx 1.27.5), `envoyproxy/envoy:v1.33-latest`,
`fullstorydev/grpcurl:latest`, and `curlimages/curl:8.11.1`. Events went to a `stdio_out` with
`format: json`, and an `internal` input on a 2 s interval went to a second one. OTLP/HTTP requests
were a JSON `POST /v1/logs` from curl, and gRPC requests a `LogsService/Export` from grpcurl with
the repo's OTLP protos. Every leg passed.

1. **HAProxy `mode tcp` with `send-proxy-v2` in front of `otlp_in` HTTP and gRPC**, both
   listeners `peer: true` and `proxy_protocol: true`, and HAProxy's `server` lines
   `send-proxy-v2 check inter 500ms` (`check-send-proxy` added on the gRPC one):

   ```json
   {"network.peer.address":"172.31.1.20","network.peer.port":50374,"client.address":"172.31.1.10","client.port":49970}
   {"network.peer.address":"172.31.1.20","network.peer.port":45286,"client.address":"172.31.1.11","client.port":33270}
   ```

   HAProxy's log named the same client and port. Each health check, with or without
   `check-send-proxy`, was a 16-byte version 2 `LOCAL` header and then an RST (captured with
   tcpdump). Over about 100 seconds of checks every 500 ms on both backends, both servers stayed
   `UP` with `chkfail=0`, and `logit` counted no `connections.rejected` and logged no diagnostic.
2. **nginx with `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for`** in front of
   `otlp_in` HTTP and `splunk_hec_in` (`/services/collector/event`), both `peer: true` and
   `forwarded: x_forwarded_for`:

   ```json
   {"network.peer.address":"172.31.2.30","network.peer.port":52030,"client.address":"172.31.2.10"}
   {"network.peer.address":"172.31.2.30","network.peer.port":46994,"client.address":"172.31.2.11"}
   ```

   No `client.port`. A client sending `X-Forwarded-For: unknown` got no `client.*` and one
   `forwarded` diagnostic that names the socket peer and not the value. A client sending
   `X-Forwarded-For: 192.0.2.1` got `client.address` `192.0.2.1`, because `$proxy_add_x_forwarded_for`
   appends: the spoofed-header non-goal, now called out in "Recording the sender".
3. **Envoy with `use_remote_address: true`**, HTTP/2 upstream, in front of `otlp_in` gRPC with
   `peer: true` and `forwarded: x_forwarded_for`:

   ```json
   {"network.peer.address":"172.31.3.50","network.peer.port":41248,"client.address":"172.31.3.10"}
   ```

   Envoy also appends to a client's own `X-Forwarded-For`, with the same result as leg 2.
4. **Decision 2's precedence**: client, then nginx setting `X-Forwarded-For`, then HAProxy
   `mode tcp` with `send-proxy-v2`, then `otlp_in` HTTP with `peer: true`, `proxy_protocol: true`,
   and `forwarded: x_forwarded_for`. The PROXY header names nginx, and the forwarding header names
   the client. HAProxy in TCP mode into nginx, the arrangement this plan's "Verification" names,
   sends `logit` no PROXY header, so it can't test precedence; leg 6 runs it instead.

   | Route | `client.*` | `network.peer.*` |
   |---|---|---|
   | nginx sets the header | `172.31.4.10`, no port (the PROXY origin was `172.31.4.30:57170`) | HAProxy |
   | nginx clears the header | `172.31.4.30:45182`, the PROXY origin | HAProxy |
   | The client sends `unknown` | `172.31.4.30:45196`, the PROXY origin, and one `forwarded` diagnostic | HAProxy |

5. **A direct connection to a `proxy_protocol: true` port**: curl to leg 1's `otlp_in` HTTP port
   got `Recv failure: Connection reset by peer`, and `logit` counted
   `logit.input.connections.rejected{reason="proxy_header"}` once, with one `proxy_header`
   diagnostic. A plain `nc -z` to the gRPC port, a TCP health check with no header, counted the
   same.
6. **The "Recording the sender" recipe**: client, then HAProxy `mode tcp` with
   `send-proxy-v2 check`, then nginx on `listen 8080 proxy_protocol` with
   `proxy_set_header X-Forwarded-For $proxy_protocol_addr`, then `otlp_in` HTTP with `peer: true`
   and `forwarded: x_forwarded_for`:

   ```json
   {"network.peer.address":"172.31.6.30","network.peer.port":38818,"client.address":"172.31.6.10"}
   ```

   A client sending `X-Forwarded-For: 192.0.2.1` got its own address, `172.31.6.11`, because nginx
   overwrites the header. HAProxy's check against nginx stayed `UP`.

The run found one wording fault: the `forwarded` diagnostic said the request kept "its
connection's `client.address` and `client.port`" on a listener with no `proxy_protocol:`, where
there are none. This PR rewords it.
