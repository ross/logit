---
created: 2026-09-02
updated: 2026-09-02
---

# Syslog egress: format, transport, and header-field precedence

## Status
Accepted

## Context

`syslog_in` (`crates/logit-inputs/src/syslog.rs`) can ingest RFC 3164/5424 syslog over UDP, but
`logit` has had no way to emit it: `syslog_out` was not implemented, and was not even a declared
`ComponentKind` (`docs/plans/demo-stack.md`'s gap table). That gap is the specific reason the
demo stack's log leg (`demo/compose.yaml`'s `loki` service) has been standing up
provisioned and unfed since `docs/plans/demo-stack.md` landed — `AGENTS.md` names
`syslog_out`/`otlp_out` as the demo's forcing function, and this ADR is the `syslog_out` half.

Three questions had to be settled before implementation: which syslog dialect to emit, which
transport(s) to support, and where a relayed message's header fields (facility, severity,
hostname, tag, pid) should come from.

Note: `docs/adr/` already has two files numbered `0020`
(`demo-stack-separate-from-dev-stack.md` and
`trace-context-propagation-on-delivered.md`) from an earlier merge. `0022` is simply the next
free number after the highest existing one (`0021`); resolving that collision is a separate,
unrelated change, not this one's job.

## Decision

### Format: configurable, default RFC 5424

`syslog_out` emits either dialect (`format: rfc3164 | rfc5424`), defaulting to **RFC 5424**. RFC
3164's TIMESTAMP carries no year and no timezone, so a receiver has to guess both — Grafana Alloy's
`loki.source.syslog` (the demo's receiver) defaults `rfc3164_default_to_current_year` to `false`
for exactly this reason. RFC 5424's TIMESTAMP is unambiguous RFC 3339. `syslog_in` already parses
both dialects on the way in, so emitting both on the way out is parity, not new scope; `rfc3164`
stays available as an explicit compatibility mode for a receiver that only understands it.

RFC 5424's TIME-SECFRAC is capped at 6 digits (microseconds); `logit_core::time::format_rfc3339_utc`
always produces 9 (nanoseconds), so the encoder trims the last 3 fractional digits rather than
reimplementing RFC 3339 formatting.

**A relayed message's TIMESTAMP is `event.timestamp`, never the `syslog.timestamp` attribute.**
`syslog_in`'s own module doc explains why that attribute can't be resolved to an instant for RFC
3164 without guessing a year and a timezone; re-emitting it here would reintroduce exactly that
guess on the way out. Consequence: a `syslog_in -> syslog_out` relay re-stamps with `logit`'s
receipt time rather than preserving the origin's own clock — recorded in `docs/known-gaps.md`. The
opt-in `syslog_timestamp` transform that gap already proposes is the right place to resolve
`syslog.timestamp` onto `event.timestamp` explicitly, for either direction.

### Transport: both UDP and TCP

`transport: udp | tcp`, defaulting to **UDP** — it mirrors `syslog_in`'s own listener and needs no
ordering guarantee against the receiver's startup (a fire-and-forget `send_to` before the receiver
is up just loses that line, the same honest limit `syslog_in`'s own UDP intake already accepts).

TCP is what makes `Fault` classification (`docs/adr/buffered-sink-delivery.md`) meaningful for
this sink: a connect failure is unambiguously `Fault::Clean` (the destination never saw the
batch), where UDP's `send_to` failures are almost always `Clean` too, but for the less interesting
reason that the kernel rejected the datagram locally before it ever reached the wire.

**TCP framing is RFC 6587 §3.4.1 octet-counting** (`MSG-LEN SP SYSLOG-MSG` per message), not
non-transparent (LF-delimited) framing. Two reasons: Alloy/Loki's syslog receiver (built on
`go-syslog`) auto-detects octet-counting from a message's leading digit, so no receiver-side
configuration is needed; and octet-counting is transparent to a literal newline inside MSG, which
is defense-in-depth alongside (not a replacement for) the encoder's own control-character escaping
below.

**No `Permanent` fault is ever emitted by this sink.** There is no syslog equivalent of a bad
token or a 4xx — a connect or write failure always means "can't reach the destination right now,"
never "this destination will never accept this data." A persistent failure degrades to dropping
with counters and throttled diagnostics; it does not eventually trip
`docs/adr/buffered-sink-delivery.md`'s sustained-permanent-failure exit window.

**A bounded reconnect exception to "one attempt per `send`."** ADR `buffered-sink-delivery` makes `Output::send` a
single attempt, with retry timing owned entirely by the generic writer. A persistent TCP
connection can still go stale between `send` calls (the receiver restarted, a NAT dropped the
flow) — Alloy's own `idle_timeout` (120s default) guarantees this eventually happens even in the
healthy case. The first `write_all` after that fails with a reset-class error. Rather than drop
the whole batch and wait for the writer's next retry (which, under the default `at_most_once`
posture, wouldn't happen at all for an `Ambiguous` fault), `send` reconnects once and retries the
write **only when** the failing connection was inherited from a previous `send` call and no byte
of the current write has succeeded yet. This is connection re-establishment, not delivery retry:
a reset before any byte of this attempt lands means the peer discarded its prior connection state,
so the duplicate risk is exactly a `Clean` retry's (zero) — the batch has provably not been
partially delivered twice.

### `duplicate_safe()` is `false` for both transports

Unlike `influxdb_out` (idempotent-overwrite semantics on the destination), syslog has no
destination-side idempotency to lean on — a redelivered message is a duplicated log line at Loki.
This yields the conservative `AtMostOnce` default posture, under which `Fault::Clean` still
retries (ADR `buffered-sink-delivery`'s table) — covering the common outage shape (the receiver restarting) with zero
duplicate risk. An operator who prefers the other trade sets `buffer.delivery: at_least_once` per
component.

### Header-field precedence: round-trip `syslog.*` attributes, config as fallback

Per event, per field, the event's own `syslog.*` attribute wins when present (exactly what
`SyslogDecoder` writes on the way in); a configured default is the fallback; a format-appropriate
absence (`-` for RFC 5424's NILVALUE, an omitted token for RFC 3164) is the last resort. This makes
`syslog_in -> syslog_out` a real relay rather than a lossy re-encoding, and gives a receiver
meaningful `host`/`app` stream labels (via Alloy's `__syslog_message_hostname`/`__syslog_message_
app_name` relabeling) from the very first line, with no config beyond `endpoint`.

**`syslog.severity` deliberately outranks `log.severity`.** `syslog_in`'s PRI-to-`Severity` mapping
is lossy by construction — `logit_core::Severity` has six variants where syslog has eight, so
`map_severity` collapses `emerg`/`alert`/`crit` (0/1/2) onto `Fatal` and `notice`/`info` (5/6) onto
`Info`. Preferring the raw attribute is what makes a relay byte-faithful for the severities that
survive that collapse; the inverse mapping (`syslog_severity_of`) is only the fallback for an
event whose log record came from somewhere other than `syslog_in` — e.g. a Lua-authored log line,
or (in the future) `file_tail`. That inverse is itself necessarily lossy in the other direction:
`Fatal` maps to `2` (crit), not `0` (emerg), since `Fatal` never claims "system unusable."

### Injection safety

`syslog_in` splits a datagram into lines on `\n`, and Alloy's syslog listener does the same — an
embedded newline in a relayed message would otherwise forge a second, fully attacker-controlled
message at the receiver. The encoder escapes `\n`/`\r`/NUL and every other C0 control character
(plus DEL) in the rendered message, on both transports, so the wire bytes don't depend on which
transport is configured. A literal backslash is deliberately **not** escaped: the demo's message
body is a JSON document, where a real newline inside a JSON string is already the two characters
`\` `n` on the wire, and escaping a literal backslash would double every one of them and break
Loki's `| json` LogQL parsing on every line. Consequence, recorded in `docs/known-gaps.md`: a
message that already contained the literal two characters `\` `n` is indistinguishable on the wire
from one that contained a real newline.

RFC 5424's HOSTNAME/APP-NAME/PROCID/MSGID are `PRINTUSASCII` with length caps; every
non-conforming byte (including a raw space) becomes `_`. RFC 3164's HOSTNAME/TAG additionally
forbid `:`/`[`/`]`, matching `syslog_in`'s own two-token header rule (a `:` in HOSTNAME would make
that parser misread the token as TAG) and the same "must not end in `:`" constraint
`demo/hello/app.py` already documents by construction.

**No RFC 5424 §6.4 BOM before MSG, despite the RFC allowing one.** A first version emitted one —
the symmetric choice to `syslog_in` stripping a leading BOM on the way in — on the untested
assumption that Alloy's receiver would tolerate it. Verified against the real demo stack that it
does not: Loki's `| json` LogQL stage uses Go's `encoding/json`, which does not skip a leading
BOM, so every relayed line silently failed to parse as JSON — `count_over_time({job="demo"} |
json | status >= 500 [...])` returned zero matches despite the underlying lines genuinely being in
Loki (confirmed via a direct `query_range` against Loki's own API, both with and without the BOM).
Dropped entirely rather than made configurable — there is no receiver in this repo's own stack
that benefits from it, and "off by default, on for a receiver we've never tested against" would be
speculative.

### Sizing

`max_message_bytes` (default **8192**) bounds one whole encoded message. The default matches
Grafana Alloy's own `loki.source.syslog` `max_message_length` default — the receiver the demo
stack points this at — rather than RFC 3164 §4.1's traditional 1024, which would truncate a
JSON-bodied message on every modern relay chain. An oversize MSG is truncated on a UTF-8 character
boundary (never the header); an oversize header (only reachable with an absurdly small
`max_message_bytes`) drops the whole message instead of emitting a malformed one.

### No `logit_proto::Encoder`

That trait returns one opaque `Bytes` per batch with no framing metadata, which cannot express
per-message boundaries (one UDP datagram per message, one octet-counted frame per message on TCP).
`SyslogEncoder::encode_into` is a bespoke, still-pure, still-directly-unit-testable method instead
— `EventDump` (`stdio.rs`) is the in-tree precedent for a sink whose encoder sidesteps the trait
for the same class of reason.

## Alternatives considered

- **RFC 3164 as the default format.** Rejected: its ambiguous timestamp is a real cost for a demo
  whose whole point is a first-time visitor seeing correct data land in Loki, and `syslog_in`
  already pays the cost of supporting both dialects, so emitting only the lesser one would be an
  arbitrary narrowing.
- **UDP only.** Rejected: it leaves ADR `buffered-sink-delivery`'s `Fault` classification almost vacuous for this sink
  (a UDP `send_to` failure is essentially always `Clean`), and gives up the one transport where a
  connect failure is unambiguous.
- **Non-transparent (LF-delimited) TCP framing.** Rejected: it depends on the sender never emitting
  a raw newline, which is exactly the invariant this ADR's injection-safety section exists to
  guarantee independently of transport; octet-counting doesn't need that guarantee to also hold at
  the framing layer, and needs no corresponding Alloy configuration either.
- **`duplicate_safe() == true`, reasoning that a UDP `send_to` failure means the datagram never
  left the host.** Rejected: that's an argument about `Fault` classification, not about whether
  re-delivery of an *already-sent* message is safe. Conflating them would silently widen
  `Ambiguous` retries on a sink that has no way to absorb the resulting duplicates.
- **`log.severity` outranking `syslog.severity`.** Rejected: it would silently rewrite, on every
  relay hop, a `crit` line's severity to `emerg` and a `notice` line's to `info` — the opposite of
  a relay's job.
- **A single fixed delivery guarantee, ignoring transport.** Not applicable here beyond ADR `buffered-sink-delivery`'s
  own decision; `duplicate_safe` is a per-sink fact regardless of transport, and both transports
  answer it the same way for the same reason (no destination-side idempotency).
- **Always exit on a sustained TCP failure, mirroring a config-error sink.** Rejected: nothing
  about a persistently-unreachable syslog destination is a configuration error the way a bad
  InfluxDB token is; degrading to dropping, loudly, is the honest behavior.

## Consequences

- `crates/logit-config/src/lib.rs`: new `ComponentKind::SyslogOut`, `SyslogTransport`,
  `SyslogFormat`, `SyslogFacility`; `schema/logit.schema.json` regenerated. `SyslogIn`'s doc
  comment corrected from "UDP or TCP" to "UDP" (`syslog_in` has always been UDP-only; the
  egress/ingress asymmetry this ADR creates is deliberate, not a sign the input needs fixing to
  match).
- `crates/logit-pipeline/src/graph.rs`: `role`/`kind_name`/`is_implemented` all gain the variant.
- `crates/logit-outputs/src/syslog.rs` (new): `SyslogEncoder` + `SyslogOutput`, per this ADR.
- `crates/logit-cli/src/pipeline.rs`: `build_spec`'s `SyslogOut` arm; the sole place a
  `logit_config::SyslogFormat`/`SyslogTransport`/`SyslogFacility` value crosses into
  `logit_outputs::syslog`'s own tiny mirror types, mirroring `overflow_policy`/`delivery_posture`.
- `demo/logit.yaml`, `demo/alloy/config.alloy`: the demo's log leg goes live.
- `docs/known-gaps.md`: new entries for no TLS, no RFC 5424 STRUCTURED-DATA emission, the
  backslash-escaping ambiguity, and the receipt-time-not-origin-time timestamp; the existing
  "syslog TCP and structured data" entry narrows to `syslog_in` staying UDP-only.
