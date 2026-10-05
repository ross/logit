---
created: 2026-10-05
updated: 2026-10-05
---

# Enabling plan: sender addresses on the HTTP listeners — `peer:`, `proxy_protocol:`, and `forwarded:`

## Status

Planned. `hpeer/w0` is this plan, an amendment to [ADR
`listener-peer-address`](../adr/listener-peer-address.md), and [ADR
`forwarded-header-parsing`](../adr/forwarded-header-parsing.md), and changes no code.

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
- **A trusted-proxy allowlist or a hop count.** [ADR
  `deployment-threat-model`](../adr/deployment-threat-model.md) puts the trust boundary with the
  operator and rejects a per-listener untrusted mode. A client that spoofs a forwarding header, or
  reaches a `proxy_protocol:` port directly, is the crafted-input case. It's recorded as a
  known-gaps non-goal citing that ADR, beside the existing `proxy_protocol:` entry in
  [intake gaps](../known-gaps/intake.md).
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
   strips the port and brackets, and when the port parses, `http_access` writes it as
   `client.port`, as the listeners do. Both are changes to `http_access` output, which the
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
- Every HTTP listener has one point per request where all its batches exist, before delivery,
  with the request headers still readable:

  | Listener | Stamp point | Batches per request |
  |---|---|---|
  | `otlp_in` HTTP | `handle_http`, after decode | One per resource |
  | `otlp_in` gRPC | `handle_grpc`, after decode | One per resource |
  | `datadog_in` | `respond`, before `deliver_with_deadline` | One, or one per `TracerPayload` or stats payload |
  | `datadog_trace_in` | `respond`, beside `apply_tracer_headers` | One |
  | `splunk_hec_in` | `respond`, before the split into a deadline-bound first batch and detached rest | One per envelope |
  | Remote-write receiver | `write_response`, before building the batch | One |

  Two of these constrain the order of work. The remote-write receiver consumes the request into
  its body partway through `write_response`, so it reads the forwarding header first.
  `splunk_hec_in` stamps before its split, because a code 6 can follow a delivered prefix.
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
  and in [ADR `lossless-transit`](../adr/lossless-transit.md). The remote-write receiver's "never a
  tag" sentence becomes "never a tag unless `peer:` is on".

### Part 2: `proxy_protocol:`

- An opt-in `proxy_protocol: bool` on all five listeners, on TCP only. Graph rule 79, which
  rejects `proxy_protocol: true` off TCP, extends to them. On `datadog_trace_in` it applies to the
  `bind:` listener, and a `socket:`-only `datadog_trace_in` is rejected.
- The header is read in the spawned task, after the permit and before the TLS accept or the
  one-byte `peek`, under the same `handshake_timeout`. The plaintext `peek` comes after the header,
  because otherwise it reads the header's first byte as the request's.
- Health checks: the HTTP loops treat a `peek` of `Ok(0)` as a health check. A PROXY-aware check
  sends a header and then an RST, as HAProxy 3.2 does. Each loop ends that connection quietly,
  matching the shared stream driver in `tcp.rs`, and each listener gets a header-then-RST test.
- A rejected header counts as `logit.input.connections.rejected{reason="proxy_header"}` with the
  `proxy_header` diagnostic, as on the shared stream driver.

### Part 3: `forwarded:`

ADR `forwarded-header-parsing` is the canonical account of the parsing rules, the precedence, and
the trust stance. In brief:

- An opt-in `forwarded: x_forwarded_for | forwarded | x_real_ip` on the five HTTP listeners, off by
  default. gRPC metadata is HTTP/2 headers, so `otlp_in` gRPC needs nothing extra.
- Each request reads the first instance of the configured header, and no other header (decision
  3).
- A parsed header replaces a PROXY-derived `client.*` for that request (decision 2).
  `network.peer.*` always stays the socket peer.
- The field's doc states the trust assertion in the same words `proxy_protocol:` uses for
  reachability: the operator asserts their proxy sets the header.

## PRs

Stacked, as the stacking rules below the table describe.

| Branch | Content |
|---|---|
| `hpeer/w0` | This plan, an amendment to ADR `listener-peer-address` extending it to these listeners (its deferred list narrows to `logit_in`), and ADR `forwarded-header-parsing` for part 3. |
| `hpeer/w1` | The shared pieces: `read_proxy_header` moved, the per-connection origin, and the batch stamp helper. No behavior change. |
| `hpeer/w2` | `otlp_in` (HTTP and gRPC): `peer:` and `proxy_protocol:`, with the header-then-RST test. Sets the pattern. |
| `hpeer/w3a` | `datadog_in` and `datadog_trace_in`, including the Unix socket. |
| `hpeer/w3b` | `splunk_hec_in`. |
| `hpeer/w3c` | The remote-write receiver. |
| `hpeer/w4a` | The forwarding-header parser in `logit-proto` with a fuzz target and seeds, and `http_access` moved onto it with the listeners' config shape (decision 4). It updates the docs it contradicts: [`docs/http-access-logs.md`](../http-access-logs.md), [ADR `http-access-normalization`](../adr/http-access-normalization.md), [the `http_access` plan](http-access-normalization.md), and the all-or-nothing `forwarded` entry in [transform gaps](../known-gaps/transforms.md), which becomes the shared spoofed-header non-goal. |
| `hpeer/w4b` | `forwarded:` on the five HTTP listeners, over `w4a`'s parser. |
| `hpeer/w5` | Operator docs ([`docs/deploying.md`](../deploying.md)'s "Recording the sender" section grows to cover these listeners), the runtime-gaps peer entry narrowed to `logit_in`, the spoofed-header non-goal, and an end-to-end run. |

- `w1` stacks on `w0`, and `w2` on `w1`.
- `w3a`, `w3b`, and `w3c` are siblings off `w2` and can be built in parallel.
- `w4a` branches from `main` and stacks on nothing, so the parser is reviewed once before five call
  sites depend on it.
- `w4b` stacks on `w4a` and on whichever of `w3a`–`w3c` lands last.
- `w5` stacks on `w4b`.

## Verification

- **Every PR:** `script/cibuild`, and real-socket tests of each listener with each option on and
  off, using the shared `logit_pipeline::test_util` helpers.
- **Allocation pins:** the off path moves no existing pin in
  `crates/logit-bench/tests/allocations.rs`. A multi-batch stamp gets a pin of its own if its cost
  differs from the shared drivers' stamp.
- **`w4a`:** the forwarding-header parser fuzzed for at least 600 s; unit vectors from RFC 7239's
  examples, including a quoted IPv6 address with a port and `unknown`; and `http_access`'s existing
  `X-Forwarded-For` tests rewritten for the stripped form and `client.port`.
- **`w5` end to end:**
  - HAProxy with `send-proxy-v2` in front of `otlp_in` (HTTP and gRPC).
  - nginx setting `X-Forwarded-For` in front of `otlp_in` HTTP and `splunk_hec_in`.
  - Envoy setting `X-Forwarded-For` in front of `otlp_in` gRPC.
  - An L4-then-L7 chain (HAProxy in TCP mode into nginx), to confirm decision 2's precedence.
  - For each: the expected `client.*` and `network.peer.*` on the JSON event, health checks quiet,
    and a direct connection to a `proxy_protocol:` port rejected and counted.
