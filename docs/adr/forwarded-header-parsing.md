---
created: 2026-10-05
updated: 2026-10-05
---

# Forwarded-header parsing: one named header per component, the leftmost client, and a shared parser for the HTTP listeners and `http_access`

## Status
Accepted

## Context
An HTTP listener behind an L7 proxy (nginx, Envoy, HAProxy in HTTP mode, a cloud load balancer)
sees the proxy, not the client. The proxy is the socket peer, and when an L4 proxy sits in front of
it, the L7 proxy is also the origin a PROXY protocol header names. The client is in a forwarding
header the L7 proxy sets: `X-Forwarded-For`, RFC 7239's `Forwarded`, or `X-Real-IP`. So `peer:`
and `proxy_protocol:` ([ADR `listener-peer-address`](listener-peer-address.md) and its 2026-10-05
amendment) can't name the client of a request that came through an L7 proxy.

`http_access` already reads one of these headers. Under `forwarded: {trust: true}`, it replaces
`client.address` with the first comma-separated entry of `http.request.header.x-forwarded-for`
([ADR `http-access-normalization`](http-access-normalization.md)). Its `first_hop` trims the entry
and writes it as it stands, so a port or IPv6 brackets stay in the address: `203.0.113.7:5678`, or
`[2001:db8::1]:443`. It reads no other header.

[ADR `deployment-threat-model`](deployment-threat-model.md) puts the trust boundary with the
operator: the proxy in front of a listener is the operator's tool, and crafted input is defended
only when the defense is free.

## Decision
The five HTTP listeners (`otlp_in` over HTTP and gRPC, `datadog_in`, `datadog_trace_in`,
`splunk_hec_in`, and `prometheus_in`'s remote-write receiver) and `http_access` each take an opt-in
`forwarded: x_forwarded_for | forwarded | x_real_ip`, off by default. It names the one forwarding
header the component reads. One parser in `logit-proto` serves all six.

### Which header, and which instance
- **Only the named header is read.** Any other forwarding header is ignored, even when present.
  It's always clear which header produced `client.address`, and a proxy change is a config change.
- **The first instance.** When a request carries the named header more than once, the first is
  read and the rest are ignored.
- **The listeners read the request header.** gRPC metadata is HTTP/2 headers, so `otlp_in`'s gRPC
  side reads it as its HTTP side does.
- **`http_access` reads an attribute.** It reads `http.request.header.<name>`, the name being the
  lowercase header name (`x-forwarded-for`, `forwarded`, `x-real-ip`), or its dashed alias
  (`http-request-header-x-forwarded-for`), which the fixed alias table carries for each of the
  three. This replaces `forwarded: {trust: true}`.

### Parsing
- **`X-Forwarded-For`:** the leftmost comma-separated entry, trimmed of whitespace.
- **`Forwarded`:** the `for=` parameter of the first comma-separated element, as a token or a
  quoted string (`for="[2001:db8::1]:443"`).
- **`X-Real-IP`:** the whole value, trimmed of whitespace.
- **The address.** For all three, IPv6 brackets are stripped, and so is a trailing `:port`. The
  remaining text must parse as an IP address, and it's stamped as `client.address` in the text form
  ADR `listener-peer-address` uses. A port that parses is stamped as `client.port`.
- **Nothing usable stamps nothing.** `unknown`, an obfuscated identifier (`_hidden`), an empty
  value, or anything else that isn't an IP address stamps no `client.*` and counts a throttled
  `forwarded` diagnostic. The request and its events go through unchanged.

### Precedence
- A parsed header replaces a PROXY-derived `client.*` for that request. Behind an L4 proxy in front
  of an L7 one (an NLB in front of nginx), the PROXY header names nginx and the forwarding header
  names the client.
- An absent or unparseable header leaves the PROXY origin standing.
- `network.peer.*` always stays the socket peer.
- In `http_access`, a parsed header replaces the `client.address` the web server logged, as
  `{trust: true}` does today, and writes `client.port` when the header carries one.

### Trust
The operator asserts that the named header is set by their proxy. `logit` keeps no allowlist of
trusted proxy addresses and no hop count. A client that reaches the listener directly, or through a
proxy that appends to a header the client sent, can name any address. Under ADR
`deployment-threat-model`, that's crafted input whose defense isn't free (it needs the operator's
proxy topology as config), so it's a documented non-goal in `docs/known-gaps/`. Each
component's `forwarded:` field doc states the assertion, in the same words `proxy_protocol:`'s doc
uses for reachability.

### One parser
The parser lives in `logit-proto`, beside the PROXY header parser in `proxy.rs`. It takes the
configured header and the value's bytes, and returns an address and an optional port, or the
reason it stamps nothing. It joins the fuzz targets ([ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md)),
whose workspace depends on `logit-core` and `logit-proto` only, and `http_access`'s `first_hop`
goes away.

## Alternatives considered
- **Reading several headers in a precedence order** (`Forwarded`, then `X-Forwarded-For`, then
  `X-Real-IP`). Rejected: a client can add whichever header the proxy doesn't set, and it wins
  whenever it ranks above the proxy's. Naming one header leaves the proxy's as the only one read.
- **Rightmost-minus-N hop selection, or a trusted-proxy list** (nginx's `set_real_ip_from` and
  `real_ip_recursive`). Rejected under ADR `deployment-threat-model`: it defends against a spoofing
  client at the cost of the operator's proxy topology as config on every component, for a
  deployment shape the project doesn't target. The leftmost entry is right behind one proxy the
  operator controls that overwrites the header, which is the shape a private listener has.
- **Keeping `http_access`'s own parser.** Rejected: two parsers would disagree on a port, brackets,
  and the other two headers, so one access line would carry a different `client.address`
  depending on which component read it.
- **Auto-detecting the header.** Rejected for the reason it's rejected for PROXY headers in ADR
  `listener-peer-address`: a listener that takes whatever header is present lets any client name
  its own origin by sending the one the proxy doesn't set.

## Consequences
- `http_access`'s output changes: `client.address` loses a port and IPv6 brackets, a parsed port
  appears as `client.port`, and the config changes from `forwarded: {trust: true}` to
  `forwarded: x_forwarded_for`. The pre-release no-compatibility rule allows both.
- Changing the proxy in front of a listener, or the header it sets, is a config change.
- The all-or-nothing `forwarded` entry in `docs/known-gaps/transforms.md` becomes the shared
  spoofed-header non-goal for the listeners and `http_access`.
- A request through an L7 proxy with `forwarded:` off carries the proxy's address as
  `client.address` when a PROXY header names it, and no `client.*` otherwise.
