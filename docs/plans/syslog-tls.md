---
created: 2026-09-13
updated: 2026-09-14
---

# Enabling plan: TLS and TCP ingress for `syslog_in`/`syslog_out`

## Context

[ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md) decides the shape of this
work: `syslog_in` gains `transport: tcp` (it has been UDP-only, `docs/known-gaps.md`'s
"`syslog_in` is UDP-only" entry), and both `syslog_in` and `syslog_out` gain a `tls:` block, closing
`docs/known-gaps.md`'s "`syslog_out` has no TLS" entry (narrowed to DTLS, which stays out of scope).
This plan is the concrete build-out of that decision: what lands in which order, in which files,
and how each piece is verified. It does not repeat the ADR's reasoning -- read that first for *why*
each of these choices was made; this plan only orders the work that implements them.

Everything this plan needs besides the framer and the TCP driver already exists in-tree:

- Shared TLS builders: `crates/logit-inputs/src/tls.rs::build_server_config` and
  `crates/logit-outputs/src/tls.rs::build_client_config` (both `pub(crate)`, already used by
  `otlp_*` and `logit_*`). No new dependency anywhere -- `rustls`/`tokio-rustls` are already direct
  dependencies of both crates.
- Raw-TCP(+TLS) templates: `crates/logit-inputs/src/logit.rs` (accept loop: semaphore cap,
  handshake timeout, TLS accept inside the per-connection task) and
  `crates/logit-outputs/src/logit.rs` (connect: TCP connect, SNI via `host_only`, `TlsConnector`,
  `Fault::Clean`).
- Committed certs `testdata/tls/` (a CA, a second "wrong" CA, a server cert with `localhost`/
  `127.0.0.1` SANs, a client cert for mutual TLS).
- `logit_pipeline::BatchAccumulator` (`pub`) and `udp.rs`'s batch/flush loop shape;
  `tools/record-fixtures/raw_capture.py` already has an unused `--proto tcp` mode.

## Decisions already settled

| Question | Decision |
|---|---|
| TCP framing on `syslog_in` | Auto-detect per connection, no config field: first byte an ASCII digit → RFC 6587 §3.4.1 octet-counting (`MSG-LEN SP MSG`); else non-transparent LF-delimited (a well-formed message always starts `<`). Latched for the connection's life. |
| Scope | syslog only, but the TCP+TLS listener is a generic driver (`crates/logit-inputs/src/tcp.rs`) mirroring `udp.rs`'s `UdpListener<D>` so `statsd_in` can adopt it later. `statsd_in` is not wired to it here. |
| Frame bound | `MAX_FRAME_BYTES = 64 KiB`, a constant, matching the UDP path's 65507-byte datagram ceiling -- not a config field, and not `syslog_out`'s configurable `max_message_bytes`. |
| Receive queue | None on TCP -- a connection's own flow control is the backpressure. `receive:`'s queue-only fields are rejected on a TCP `syslog_in`; batch-assembly fields + `shutdown_grace` still apply, per connection. |
| TLS semantics | A `tls:` block's presence turns TLS on and makes it required -- no plaintext fallback -- on both `syslog_in` (`TlsServerConfig`) and `syslog_out` (`TlsClientConfig`); the `logit_in`/`logit_out` shape, since `bind`/`endpoint` are bare `host:port`. `tls:` under `transport: udp` is a config error on both sides (DTLS out of scope). No ALPN. |
| Interop fixture | A recorded `record_rsyslog_tcp` capture (`omfwd protocol="tcp"`, rsyslog's default LF framing) → `testdata/interop/syslog/rsyslog-tcp-000.raw` + a framer test. TLS interop is proven in-process against `testdata/tls/`, not via a second recorded fixture. |
| Demo | `demo/` stays plaintext (the same call [`otlp-tls`](otlp-tls.md) made). |
| Compat | Pre-release: schema/type breaks are free, no compat shims. |
| Config surface | Minimal -- no peer-address event attributes; `tls:` under `transport: udp` is a hard graph error on both sides, not a silent no-op. |

## Design

See [ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md) for the reasoning
behind every choice below; this section only locates where each piece lands.

### Config (`crates/logit-config/src/lib.rs`)

```rust
SyslogIn  { bind: String, #[serde(default)] transport: SyslogTransport, #[serde(default)] tls: Option<TlsServerConfig> }
SyslogOut { ...existing..., #[serde(default)] tls: Option<TlsClientConfig> }
```

`SyslogTransport` (already published for `syslog_out`) is reused on the input side rather than
duplicated -- `StatsdTransport` staying its own enum is a cross-protocol-only concern (schemars
publishes a type's own name into `$defs`), not a precedent against reuse *within* one protocol.
`SyslogIn`'s doc comment (currently asserting UDP-only is deliberate) and `SyslogTransport`'s
(currently `syslog_out`-only) are rewritten to describe both directions. `tls:` presence turning TLS
on follows the `logit_out`/`otlp_in` shape already documented on `LogitOut`/`OtlpIn`'s own `tls`
fields.

### Graph rules (`crates/logit-pipeline/src/graph.rs`; next free numbers **43, 44**)

- **Rule 43** (`syslog_in`): `tls:` with `transport: udp` rejected.
- **Rule 44** (`syslog_out`): the twin of rule 34's `logit_out` checks -- `cert_file`/`key_file`
  paired, `insecure_skip_verify` + `ca_file` contradictory -- plus the same tls-requires-tcp check
  as rule 43.
- **Rules 17/18 + `receive:`**: narrow `is_datagram_listener` to `SyslogIn { transport: Udp, .. }`;
  add an `is_stream_listener` predicate for `SyslogIn { transport: Tcp, .. }` and handle it in rule
  17 exactly like the existing tail-listener branch (queue-only fields `max_datagrams`/`max_bytes`/
  `overflow`/`receive_buffer_bytes` rejected by name; batch + `shutdown_grace` fields allowed) and
  in rule 18's second loop. TCP flow control *is* the queue; no `ReceiveQueue` on this path.
- Document rules 43/44 and the 17/18 amendment in `docs/design/pipeline-graph.md`.

### TCP driver (`crates/logit-inputs/src/tcp.rs`, new; `pub mod tcp;`)

- `pub struct TcpListener<D: Decoder + Clone + Send + 'static>` (alias tokio's as
  `TokioTcpListener`), `TcpListenerConfig { batch_max_events, batch_max_bytes,
  batch_flush_interval, shutdown_grace }` (`UdpListenerConfig` minus the queue-only fields).
  Builders copied from `udp.rs`'s shape: `new`, `with_diagnostics`, `with_telemetry`,
  `map_decoder`, `config`, `local_addr`, plus `with_tls(&TlsServerSettings, base_dir)` →
  `build_server_config(.., &[])` (no ALPN), and `#[cfg(test)] with_max_connections`/
  `with_handshake_timeout`.
- **`Input::bind` pre-pass + `local_addr`**, copied from `otlp.rs` (not `logit.rs`, which binds
  inside `run_until_shutdown` and has no `local_addr`).
- Accept loop copied from `logit.rs`: `Semaphore` + `try_acquire_owned` (reject, never queue),
  `MAX_CONCURRENT_CONNECTIONS = 1024`, `HANDSHAKE_TIMEOUT = 5s`, `TlsAcceptor` built once, TLS
  accept inside the spawned task under the timeout, `diag.warn_throttled("connection_error", ..)`.
  Simplification vs. `logit_in`: syslog has no in-band reject message, so a past-the-cap connection
  is closed **before** any TLS handshake, counting
  `logit.input.connections.rejected{reason="limit"}`; the `logit.input.connections` gauge only
  counts permit holders.
- `run_until_shutdown` override so every connection task's `Fanout` clone drops on shutdown; copy
  `logit.rs`'s `changed()`-not-`wait_for()` discipline (Send-ness inside `tokio::spawn`).
- `serve_connection<S: AsyncRead + AsyncWrite + Unpin + Send>`: `Framer` → per frame
  `decoder.decode_into(frame, received_at, &mut scratch)` → `BatchAccumulator::absorb` → `emit`
  (copy `udp.rs`'s shape: deadline race via `next_deadline`, `scratch.clear()`, fresh
  `TraceContext::new_root` per batch, `logit.component.receive.flushed{reason}`). Flush on bound,
  interval, shutdown, and clean close. Batching is **per connection** -- document that on the
  config field.

### Framer (in `tcp.rs`)

- `enum Framing { OctetCounting, NonTransparent }`, latched on the first byte.
- Octet-counting: ≤9 digits, parsed len ≤ `MAX_FRAME_BYTES`, one SP, then exactly len bytes
  verbatim (embedded `\n` is payload). Non-transparent: up to `\n`.
- `MAX_FRAME_BYTES = 64 KiB`, a constant, not config (see Decisions table). Oversize/malformed →
  count `logit.input.frames.dropped{reason="oversize"|"malformed"}`, throttled diag, **close the
  connection** (octet-counting can't resync).
- Trailing partial at close: non-transparent → emit it; octet-counting → drop,
  `reason="truncated"`.
- **Decoder must not re-split**: `SyslogDecoder::decode_into` (`syslog.rs`) splits on `\n`, which
  would shred a multiline octet-counted MSG. Add `SyslogDecoder::with_line_splitting(bool)`
  (default `true`); `SyslogInput::tcp` sets it `false` for both framings. `SyslogDecoder` gains
  `#[derive(Clone)]` (needed once per connection).

### Sink (`crates/logit-outputs/src/syslog.rs`)

- `Conn::Tcp { stream: Option<TcpStream>, .. }` → `Option<Box<dyn AsyncStream>>`. Hoist
  `trait AsyncStream` and `host_only` (both currently private in `logit.rs`) into
  `crates/logit-outputs/src/tls.rs` as `pub(crate)`; `logit.rs` uses them from there (own,
  behavior-preserving commit).
- `SyslogOutput` gains `tls: Option<Arc<rustls::ClientConfig>>` + `has_connected_once: bool`;
  `with_tls(&TlsClientSettings, base_dir)` -- no `is_empty()` early return (presence is decided by
  the `Option<TlsClientConfig>` at the call site, the same as `logit_out`'s own `with_tls`, so an
  empty `tls: {}` block still means TLS with the bundled Mozilla roots) -- plus the
  `insecure_skip_verify` warning `otlp_out` already logs (`logit_out` omits it today; don't copy
  that omission here).
- `send_tcp`: only the `None =>` connect branch changes, to `logit.rs`'s path (TCP connect, then
  `ServerName::try_from(host_only(endpoint))` + `TlsConnector::connect`, both under
  `connect_timeout`, both `Fault::Clean`). The take-before-write and single-first-`write()`
  invariants stay byte-for-byte. `flush` → `(&mut **stream).flush()`.
- Add `logit.output.reconnects` (mirroring `logit.rs`'s own counter; counted on every connect after
  the first).

### Wiring (`crates/logit-cli/src/pipeline.rs`)

- `SyslogIn` arm: branch `SyslogInput::new(bind)` / `SyslogInput::tcp(bind)`;
  `.with_tls(&to_tls_server_settings(tls), base_dir)?` (template: the `OtlpIn` arm). Add a
  `tcp_receive_config` sibling of the existing `receive_config`.
- `SyslogOut` arm: under `Tcp`, `.with_tls(&to_tls_client_settings(tls), base_dir)?`.
- `script/schema` → commit `schema/logit.schema.json` (W2 and W3 both touch it; after merging the
  sibling branch, re-run `script/schema` rather than hand-merging the JSON).

### Telemetry

No TLS-specific metric -- `docs/deploying.md`'s existing TLS section position holds: a handshake
failure surfaces through the same connection-error diagnostics and counters any other transport
failure would.

| Metric | Precedent |
|---|---|
| `logit.input.connections` (gauge), `logit.input.connections.rejected{reason="limit"}` | `logit_in`, verbatim |
| `logit.input.frames`, `logit.input.frame.bytes` | stream twin of `logit.input.datagrams`/`.datagram.bytes` |
| `logit.input.frames.dropped{reason="oversize"\|"malformed"\|"truncated"}` | `logit.proto.errors{reason}` shape |
| `logit.output.reconnects` (`syslog_out`) | `logit_out`, verbatim |

Catalog rows land in `docs/design/internal-telemetry.md` (beside `syslog_in`'s and `syslog_out`'s
existing entries).

## Workstreams

Stacked branches `feat/syslog-tls-w<N>`, one PR each, opened against the parent branch and
retargeted to `main` when it merges. `git merge origin/main` to update, never rebase.

| # | PR | Files | Depends |
|---|---|---|---|
| W0 | Docs: ADR + plan + index rows | `docs/adr/syslog-tcp-ingress-and-tls.md`, `docs/adr/README.md`, `docs/plans/syslog-tls.md`, `docs/plans/README.md` | — |
| W1 | Generic TCP+TLS driver + framer | `crates/logit-inputs/src/tcp.rs` (new), `src/lib.rs` | W0 |
| W2 | `syslog_in` over TCP/TLS | `logit-inputs/src/syslog.rs`, `logit-config/src/lib.rs`, `logit-pipeline/src/graph.rs`, `logit-cli/src/pipeline.rs`, `schema/logit.schema.json`, `docs/design/pipeline-graph.md`, `docs/design/internal-telemetry.md` | W1 |
| W3 | `syslog_out` TLS | `logit-outputs/src/{syslog,tls,logit}.rs`, `logit-config/src/lib.rs`, `graph.rs`, `pipeline.rs`, `schema/logit.schema.json`, both design docs | W0 (∥ W1/W2) |
| W4 | Recorded rsyslog-over-TCP fixture | `script/record-fixtures`, `tools/record-fixtures/rsyslog-tcp.conf` (new), `testdata/interop/syslog/rsyslog-tcp-000.raw` + `README.md`, a framer test in `tcp.rs`, `docs/plans/recorded-interop-fixtures.md` | W1 (∥ W2/W3) |
| W5 | Round-trip tests, example, closeout | `crates/logit-cli/tests/syslog_round_trip.rs`, `fixtures/syslog-relay.yaml` (new), `docs/known-gaps.md`, `docs/adr/syslog-output.md`, `docs/deploying.md`, `docs/design/internal-telemetry.md`, `AGENTS.md` | W2 + W3 (+ W4) |
| W6 | Operator-configurable `handshake_timeout` on all three TCP listeners | `logit-config/src/lib.rs`, `logit-pipeline/src/graph.rs`, `logit-inputs/src/{tcp,logit,syslog,otlp}.rs`, `logit-cli/src/pipeline.rs`, `schema/logit.schema.json`, `docs/design/pipeline-graph.md`, `docs/deploying.md`, `docs/known-gaps.md`, both syslog/native ADRs, `fixtures/{syslog-relay,forwarder-central}.yaml` | W5 |

Landing order: **W0 → (W1 ∥ W3) → (W2 ∥ W4) → W5 → W6.**

### Status (2026-09-13)

PR numbers, per this plan's landing order: W1 (the generic TCP+TLS driver + framer) is #157, W3
(`syslog_out` TLS) is #159, W4 (the recorded rsyslog-over-TCP fixture) is #161, and W2 (`syslog_in`
over TCP/TLS, stacked on W1) is #163. This document's own workstream, W5 (round-trip tests, the new
`fixtures/syslog-relay.yaml`, and the closeout doc edits below), is built as a branch on top of all
four but does not yet have a PR open. Both W2 and W3 have since picked up review fixes on their own
branches (W2/#163: a first-byte deadline on both accept arms, a shared throttle for framing
diagnostics, truncated-frame accounting on an abrupt close; W3/#159: `syslog_out` now flushes
before reporting a TLS batch delivered, the zero-byte reconnect-and-retry is plaintext-only, a TLS
write failure is always `Fault::Ambiguous`, and `connect_timeout` bounds the TCP connect and TLS
handshake as separate phases — see both syslog ADRs' 2026-09-13 amendments), which this workstream
has merged in. **Nothing in this stack is merged** — landing order and timing are Ross's call, per
this plan's "Execution instruction" above, not something this document or any workstream branch
decides for itself. **W6** is a follow-on stacked on W5: the 5s pre-message timeout this plan's
driver inherited from `logit_in` becomes an operator-facing `handshake_timeout:` field on
`syslog_in`, `logit_in`, and `otlp_in` alike (graph rule 45; both syslog and native ADRs carry a
2026-09-13 amendment), and `otlp_in`'s previously-unbounded TLS accept is wrapped at the same time,
closing `docs/known-gaps.md`'s row for it. It deliberately adds no idle timeout -- that row is
amended instead with the three design questions that make it its own effort.

### Per-workstream detail

**W0** — Done when: every relative link resolves, the ADR's headings match `docs/adr/TEMPLATE.md`
exactly, both `docs/adr/README.md`/`docs/plans/README.md` gained a top row with
`created`/`updated: 2026-09-13`. Exempt from `cibuild`.

**W1** — Test list: framer detection per first byte; frame split across reads / one byte per read /
several per read; octet-counted MSG containing `\n` stays one frame; `\r\n`; oversize per framing;
trailing partial at close per framing; non-digit before SP and a 10-digit count → malformed. Driver:
`bind()` + `local_addr()` before `run`; plaintext round trip via a trivial test `Decoder`; flush on
count / interval / close; a cap of `with_max_connections(1)` counts the reject; TLS round trip on
`testdata/tls/server.*`; a client trusting `other-ca.pem` is refused without killing the listener;
TCP-connect-then-silence releases the permit after `with_handshake_timeout(50ms)`; shutdown returns
with an idle connection open. Done: `cibuild` clean, no new deps, `allocations.rs`/`type_sizes.rs`
untouched.

**W2** — Test list: config round-trip (`transport`/`tls` defaults + set); rule 43 both ways; rule 17
rejects `receive.max_datagrams` on a TCP `syslog_in` and accepts `receive.batch_max_events`; rule 18
still rejects `batch_max_events: 0`; `build_spec` builds both a TCP and a TLS `syslog_in`; a
loopback e2e per framing asserting tag/severity/message; a multiline octet-counted MSG → exactly one
event. Done: `cibuild` clean, `script/schema` no diff after commit, `script/validate` passes.

**W3** — Test list: `tls_tcp_collector` (a `TlsAcceptor` over `testdata/tls/server.*`); server-TLS
send delivers the octet-counted frame; mTLS with `client.*` vs. `client_ca_file`; a sink trusting
`other-ca.pem` fails `Fault::Clean`; `insecure_skip_verify` connects and warns; extend the existing
`tcp_reconnects_after_the_peer_resets_an_inherited_connection` test to assert
`logit.output.reconnects == 1`; existing fault-classification tests unchanged over the boxed stream;
config round-trip + rule 44's three cases. Done: `cibuild` clean, `send_tcp`'s diff outside the
connect branch is zero lines, schema no diff.

**W4** — Copy `record_rsyslog()`'s shape; reuse `rsyslog-entrypoint.sh` unchanged (mount
`rsyslog-tcp.conf` at `/etc/rsyslog-fixture.conf`); `omfwd protocol="tcp"` without `TCP_Framing`;
use `raw_capture.py --proto tcp`. Test: the fixture's bytes through `Framer` + `SyslogDecoder` →
non-transparent detected, one event, tag/severity/message match. Done: `cibuild` clean; the README
row names the rsyslog version and invocation; the "TCP framing not covered" bullet is replaced;
`recorded-interop-fixtures.md`'s follow-on closes.

**W5** — `syslog_round_trip.rs` gains `mod tcp` (`syslog_out` TCP → `syslog_in` TCP over the existing
corpus; a raw LF-framed client; the multiline octet case) and `mod tls` (server TLS, mTLS, wrong-CA
refused + `Fault::Clean`), modelled on `logit_round_trip.rs` but using `bind()`+`local_addr()`, not
`ephemeral_addr()`+sleep. `fixtures/syslog-relay.yaml` in `statsd-relay.yaml` style, `tls:` blocks
commented as in `fixtures/forwarder-*.yaml`. Done: `cibuild` + `script/validate` clean; no remaining
"`syslog_in` is UDP-only" claim outside a closed `known-gaps.md` entry.

**W6** -- Test list: config round-trip (default + set, all three kinds); rule 45's zero case per
kind, the non-default-under-UDP rejection, and both accepting cases (a *deserialized* defaulted UDP
`syslog_in`, so `graph.rs`'s hand-mirrored `DEFAULT_HANDSHAKE_TIMEOUT` can't drift unnoticed); a new
`otlp_in` test that a TLS listener with a 50ms budget closes a silent connection inside 1s and still
serves a real request after; `build_spec` tests per kind that the configured value reaches the built
listener (asserted behaviourally -- `NodeSpec::Input` is a `Box<dyn Input>`, so there is nothing to
read the field back off). Done: `cibuild` clean, `script/schema` no diff after commit,
`script/validate` passes, no idle timeout added anywhere.

## Verification

- `script/cibuild` at every PR; `script/schema` no diff (W2/W3); `script/validate` (W5);
  `script/audit` unaffected (no new dependency anywhere in this plan).
- `allocations.rs`/`type_sizes.rs` must not move; if either does, that's a new TCP-framer case to
  add, not a pin to relax, and `docs/design/memory.md` gets updated in the same commit.
- Manual smoke (W8, 2026-09-14): a real `rsyslogd` 8.2302.0 (Debian bookworm's packaged build,
  `rsyslog-gnutls` for the `gtls` netstream driver) forwarding to a TLS `syslog_in`
  (`transport: tcp`, `tls: { cert_file: testdata/tls/server.pem, key_file:
  testdata/tls/server.key }`) feeding `stdio_out`, both containers on the dev compose network,
  reached by alias. Three runs, two `logger`-generated messages each:
  - **TLS, rsyslog's default (non-transparent/LF) framing:**
    `action(type="omfwd" target="logit-w8" port="6514" protocol="tcp" StreamDriver="gtls"
    StreamDriverMode="1" StreamDriverAuthMode="x509/certvalid")`, `DefaultNetstreamDriverCAFile`
    set to `testdata/tls/ca.pem` (the leaf's `DNS:localhost`/`IP:127.0.0.1` SANs don't cover the
    container's own hostname, so `x509/name` was not reachable; `x509/certvalid` still validates
    the chain, unlike `anon`). TLS handshake completed (`decided upon suite
    TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384`), both messages arrived intact with correct
    `syslog.*` attributes, and the listener's `connection_error` diagnostic fired exactly once
    and correctly at teardown (`peer closed connection without sending TLS close_notify`, `logit`
    warns and drops the connection rather than hanging).
  - **TLS, `TCP_Framing="octet-counted"`:** same action plus `TCP_Framing="octet-counted"`. Same
    handshake, both messages arrived byte-correct in order with no `framing_error`/`bad_frame`
    diagnostic, which is decisive: a mis-parsed octet count would either desync the stream (empty
    `<` never lands as the frame's first byte once garbled) or trip `framing_error`, neither of
    which happened.
  - **Plaintext TCP control (no `tls:` on `syslog_in`, plain `protocol="tcp"` `omfwd`, no
    `StreamDriver`):** both messages arrived, `bound` logged, no diagnostics at all -- the
    baseline the two TLS runs are compared against.
  All three ran against the release `logit-cli` binary (`cargo build --release -p logit-cli`) via
  throwaway scratchpad configs/scripts, not committed. `logger`'s own `imuxsock` escapes an
  embedded newline in the message text (`\n` -> `#012`) before it ever reaches `omfwd`, so an
  embedded-newline probe can't distinguish the two TCP framings this way; the byte-correct
  two-message-per-connection result above is the framing evidence instead.

## Open risks

- Framing auto-detection is a heuristic (leading whitespace or a stray newline before the first
  message would mis-latch); mitigated by loud per-connection failure rather than silent
  misinterpretation. The ADR cites the `go-syslog`/Alloy precedent for the same detection.
- ~~No idle-connection timeout: a handshaken-then-silent connection holds a concurrency-cap permit
  forever, the same gap `otlp_in` already has -- one shared `known-gaps.md` row for both rather
  than two.~~ Closed 2026-09-14 by an opt-in `idle_timeout:` field on `syslog_in` and every other
  TCP-capable listener kind ([ADR `idle-connection-timeout`](../adr/idle-connection-timeout.md)).
- Per-connection batching means N connections × `batch_max_events` events can be in flight at once;
  documented directly on the config field so an operator sizing the bound accounts for connection
  count, not just a single stream's rate.
- `D: Clone` is load-bearing for a future decoder with real per-connection scratch state, not just a
  convenience for this one; noted in `tcp.rs`'s own module doc.
- `send_tcp`'s invariants are subtle enough that W3's reviewer should diff the function body outside
  the connect branch directly, rather than trusting a description of "only the connect arm changed."
