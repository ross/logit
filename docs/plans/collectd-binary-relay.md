---
created: 2026-09-12
updated: 2026-09-12
---

# Enabling plan: `collectd_in`/`collectd_out` — a lossless collectd binary-protocol relay

## Context

[ADR `collectd-binary-relay`](../adr/collectd-binary-relay.md) decides the shape of `collectd_in`
and `collectd_out`: transport (UDP, with multicast join), the model mapping in both directions, the
`collectd.*` attribute convention, why identity rides on event attributes rather than a per-host
`Resource`, record naming and the optional `types_db:` lookup, cdtime time handling, why host is
never empty, why NaN is a flagged point, sanitization, the cross-protocol fallback shape, the
permitted normalizations this pair's fixed-point tests assert modulo, and why signing/encryption and
notifications are deferred/trailing. This plan is the concrete build-out of that decision: what
lands in which order, in which files, and how each piece is verified.

`docs/OVERVIEW.md` already lists collectd in `logit`'s ingest scope, and
[`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md) surveys its wire format, but
no `collectd_in`/`collectd_out` kinds or codec exist yet. The metrics-model-v2 work
([`docs/plans/lossless-transit.md`](lossless-transit.md)'s W1 and W4) already landed everything
collectd needs from the model: `Sum{value, temporality, monotonic}` covers COUNTER/DERIVE/ABSOLUTE,
`Gauge` covers GAUGE, `MetricList` (inline capacity one) carries an N-data-source value list in wire
order on one `Event`, and `FLAG_NO_RECORDED_VALUE` gives a NaN gauge a home. No core-model change, so
`crates/logit-core/tests/type_sizes.rs` is untouched and no new crate dependency is needed.

## Decisions already settled

Settled with Ross (2026-09-11/12), recorded in full in the ADR:

- **Notifications** (`Message` 0x0100 / `Severity` 0x0101): in scope, trailing workstream (W5).
- **types.db DS naming**: a single-DS list is named `<plugin>.<type>` (the lone data source,
  conventionally `value`, is omitted — collectd's own `write_graphite` default); an N-DS list is
  `<plugin>.<type>.<ds_name>` when an operator-supplied `types_db:` resolves the type with a matching
  DS count and kinds, else `<plugin>.<type>.<i>` (0-based) with a throttled `types_db_mismatch` diag.
  With no `types_db` configured, index naming applies throughout. Names are display/cross-protocol
  only — the encoder rebuilds the wire shape from attributes, `MetricList` order, and record kinds,
  so like-relay fidelity never depends on `types_db`. collectd's own `types.db` is GPL and is never
  embedded; `collectd_in` takes it as an optional, operator-supplied `types_db: [paths]` field.
- **Multicast**: lands inside `collectd_in`'s workstream, but as a change to the shared UDP listener,
  not `collectd_in`-specific code. When `bind`'s IP is multicast, the listener sets `SO_REUSEADDR`,
  binds the unspecified address on that port, and joins the group. No new config field on any
  component. Benefits `statsd_in`/`syslog_in` for free.
- **Signing/encryption**: deferred. Signature parts are skipped on decode (the payload after one is
  still plaintext); Encryption parts drop the packet tail with diag `encrypted_packet_dropped`.
  Neither is emitted on encode. A known-gaps row, not a silent omission.
- Pre-release, no compat concerns: minimal config shape —
  `collectd_in { bind, types_db: [] }` + shared `receive:`; `collectd_out { endpoint,
  max_packet_bytes: "1452", hostname: Option<String> }` + shared `buffer:`.

## Design

### Codec: `crates/logit-proto/src/collectd/`

A new module beside `crates/logit-proto/src/prometheus/` (the pattern to mirror for a codec living
in `logit-proto`, per the OTLP/Prometheus precedent): module doc *is* the mapping table, plain
functions plus `CollectdDecoder`/`CollectdEncoder` types with `with_telemetry`/`with_diagnostics`
builders, counters emitted inside the codec, `pub const ATTR_*` attribute-name constants. The full
decode/encode mapping tables, the `collectd.*` attribute convention, the permitted-normalizations
list, and the "no `logit_proto::Encoder`" reasoning are in the ADR and are not repeated here — this
plan only orders the work that implements them.

```
mod.rs     module doc IS the mapping table; DEFAULT_PORT=25826, DEFAULT_MAX_PACKET_BYTES=1452,
           DATA_MAX_NAME_LEN=128, NOTIF_MAX_MSG_LEN=256, MAX_VALUES_PER_LIST=64;
           pub const ATTR_HOST/PLUGIN/.../INTERVAL/SEVERITY; cdtime_to_nanos/nanos_to_cdtime
part.rs    TYPE_*/DS_* consts, PartHeader, read_part, write_string_part/write_number_part/
           write_values_part
decode.rs  CollectdDecoder { resource: Arc<Resource>, diag, name scratch }, impl
           logit_proto::Decoder
encode.rs  CollectdEncoder, EncodeStats; sanitization; packing; implements FramedEncoder
```

As landed (W3, post [ADR `framed-encoder`](../adr/framed-encoder.md)): `CollectdEncoder` implements
`logit_proto::FramedEncoder` over `logit_proto::MessageBuf<usize>` rather than the bespoke `Packets`
type this plan originally called for -- the per-datagram `usize` meta is exactly the value-list
count `oversize_datagram` accounting needs, and `MessageBuf` moved into `logit-proto` (from its
original home private to `logit-outputs`) as part of that same ADR, so there was no longer a reuse
barrier to work around.

**Decoder state machine.** State resets per datagram: sticky `{host, plugin, plugin_instance, type,
type_instance, time_ns, interval_cdtime}`. String parts decode zero-copy (byte-offset slices of the
input buffer, not owned `String`s). `Time` → `s * 1e9`; `TimeHR` → `cdtime_to_nanos`; `Interval` →
`s << 30`; `IntervalHR` raw. A `Values` part validates `len` and `count` before any allocation; an
empty host/plugin/type skips the list with diag `incomplete_identity`; otherwise one
`Event::empty(ts, attrs)` is built and one record pushed per value in wire order. `ts` is `time_ns`
when non-zero, else `received_at` — lenient, where collectd's own receiver would reject outright.
Signature parts are skipped by length; Encryption parts stop decoding the rest of the datagram with
diag `encrypted_packet_dropped`; Message/Severity are skipped until W5; unknown part types are
skipped by length. On a malformed part: if any event was already pushed from this datagram, that's a
throttled `bad_part` diag and the decode call still returns `Ok` with what was decoded so far;
otherwise it's `Err(CodecError::Malformed)` (the listener's own `bad_datagram` counter fires). The
decoder always returns the same shared `Resource::default()` — never a per-host one — because
`BatchAccumulator::absorb` keys its batching decision on `Arc::ptr_eq` of the resource (the ADR's
"Value-list shape" section has the full reasoning, including the `syslog.hostname` precedent for
identity-as-attributes). The record name is written into a reused scratch `String` then interned, to
avoid a per-list allocation.

**`types_db.rs`.** `TypesDb::parse(&str) -> Result<TypesDb, TypesDbError>` over collectd's own line
format (`<type> <name>:<KIND>:<min>:<max>, ...`, `#` comments, `U` for unbounded, blank lines; a
later file's entries override an earlier file's on conflict) into a map from type name to its
ordered list of `(name, kind)` data sources; `TypesDb::load(paths)` reads and merges a list of files
(a missing or unparseable file is a config-time error, not a runtime one). `CollectdDecoder` takes an
optional `Arc<TypesDb>`; at decode time, one lookup per `Values` part resolves the naming path
described in "Decisions already settled" above. Tests use a short, hand-written fixture in the same
line format — never a copy of collectd's own file.

**Encoder packing.** Per event: if it has `collectd.type`, encode one like-relay `Values` list with
its records in `MetricList` order; otherwise, one single-DS fallback list per record (the ADR's
cross-protocol fallback naming). Each list resolves its identity/time/interval/values fields; any
resolution failure drops the *whole* list once, counted by the failing reason (matching collectd's
own receiver, which would reject a partial list via its `ds_num` check anyway). Each list then
encodes against the encoder's own `last`-identity elision state (which string parts were most
recently written, so they're omitted when unchanged) and against the running packet buffer: if
adding the list would overflow `max_packet_bytes` and the packet already holds at least one list,
the packet flushes first and the list is *re-encoded* from scratch (nothing elided) — a single pass
would otherwise emit an already-elided list at what becomes a packet boundary, which the receiver
would fail to parse. A list that overflows even an empty packet is dropped whole
(`{reason="oversize_value_list"}`). The remaining packet flushes at the end of the batch if
non-empty.

### Multicast

Inside the shared UDP listener's `bind_one`: if the bind address's IP is multicast, set
`SO_REUSEADDR`, bind the unspecified address for that address family on the configured port, and
join the group (`join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)` /
`join_multicast_v6(&group, 0)`) instead of binding the group address directly. The listener's
"bound" info log line notes the group joined. A unit test joins collectd's own default group
(`239.192.74.66`) on an ephemeral port and skips (rather than fails) when the test environment has no
multicast route (`ENODEV`/`ENETUNREACH`/`EADDRNOTAVAIL`) — production code still fails loudly on those
errors. Because this lives in the shared listener, `statsd_in` and `syslog_in` gain multicast for
free with no changes of their own.

### Components and configuration

**`collectd_in`**: modeled closely on the existing `statsd_in` input over a `CollectdDecoder` —
`with_diagnostics` propagating so both `bad_datagram` and `bad_part` carry the component's id,
`with_telemetry`, `with_receive`, plus a new `with_types_db(Arc<TypesDb>)` builder. Config:
`bind` (required) and an optional `types_db: Vec<PathBuf>` (paths resolved against the config's
`base_dir`, loaded once at build time), beside the shared `receive:` block every datagram input
already has.

**`collectd_out`**: modeled on `statsd_out` — encode via `CollectdEncoder`, counters from the
returned `EncodeStats`, one `send_to` per resulting packet, `EMSGSIZE` counted but not treated as a
delivery fault, `duplicate_safe() -> false` (ABSOLUTE's reset-on-read semantics would double-count on
redelivery). Config: `endpoint` (required), `max_packet_bytes` (human-readable byte size, default
`"1452"`), `hostname: Option<String>` (config-supplied only, no OS read and no placeholder default —
exactly like `syslog_out`'s own `hostname:` field. Host resolution is `collectd.host` → `host.name`
(event, then resource) → configured `hostname:`; when none resolves, the value list is dropped
whole, counted `logit.output.metrics.skipped{reason="no_host"}`, with a throttled `no_host` diag —
see the ADR's "Host is never empty on the wire" section), beside the shared `buffer:` block every
sink already has.

Both kinds get the usual graph wiring (`role`, `kind_name`, `is_implemented`, `is_datagram_listener`
for `collectd_in` to unlock `receive:`, a zero-bound validation rule for `collectd_out`) and
registry arms in the CLI's `build_spec`, following the existing `statsd_in`/`statsd_out` arms as the
template — see the Workstreams table below for exactly which files each piece touches.

### Verification shape (all workstreams)

Each workstream's own tests are described in its row below; in outline, the suite this plan builds
toward is: codec unit tests (endianness, sticky identity, malformed-shape handling, cdtime table
edge cases, encoder elision/boundary/cap/fallback-naming behavior); a pure `decode(encode(b)) == b`
(exact, over already-normalized inputs only) / `encode(decode(encode(b))) == encode(b)` (over
unconstrained wire input) fixed-point test plus a `proptest` packet-grammar generator generating
only already-normalized inputs — see the Verification section below for what that means concretely;
robustness tests (truncation, bit-flips, an inflated `count` field) asserting no panics
and bounded peak memory; allocation-count pins alongside `docs/design/memory.md` rows; a real-socket
round-trip test with committed `.in`/`.expected` fixtures generated by the real codec and reviewed
against the normalization list; and a recorded-interop capture against a real `collectd-core`
container, following the shape
[`docs/plans/recorded-interop-fixtures.md`](recorded-interop-fixtures.md) already established for
`rsyslogd`.

## Workstreams

One PR each, stacked branches `feat/collectd-w<N>`.

| # | PR | Files | Depends |
|---|---|---|---|
| W0 | **Landed** (#136). **Docs** | `docs/adr/collectd-binary-relay.md` (this ADR); row atop `docs/adr/README.md`; `docs/plans/collectd-binary-relay.md` (this plan) + row atop `docs/plans/README.md`; `docs/adr/lossless-transit.md` gains "Amendment: a fifth like pair (2026-09-12)" beside the Prometheus one; `docs/design/telemetry-landscape.md`'s collectd section and metrics matrix | — |
| W1 | **Landed** (#137). **Codec** | `crates/logit-proto/src/lib.rs` (`pub mod collectd;` after `prometheus`), `src/collectd/{mod,part,decode,encode}.rs`, `tests/collectd_fixed_point.rs`, `tests/robustness.rs` additions; `docs/design/data-model.md` `collectd.*` attribute table (after the `statsd.*` rows); `docs/known-gaps.md` cross-protocol entry rows (signing/encryption; >2⁵³ precision; non-integral sums; unsupported kinds; unit/description/start/exemplars/scope; `MAX_VALUES_PER_LIST`) **and** an amendment to its existing `NO_RECORDED_VALUE` entry (narrowing "every non-OTLP sink"/"no other sink" to "every sink whose wire has no no-value concept," now that `collectd_out` is a second such sink); `crates/logit-core/src/metric.rs`'s `MetricRecord::flags` doc amended the same way | W0 |
| W2 | **Landed** (#139). **`collectd_in` + multicast + types.db** | `crates/logit-inputs/src/collectd.rs`, `lib.rs`; the shared UDP listener's multicast bind path + test; `crates/logit-proto/src/collectd/types_db.rs` (parser, `load`, tests with a hand-written fixture) + decoder `with_types_db` naming path + unit tests (match, count/kind mismatch, unresolved, single-DS omission) + a fixed-point case proving names don't affect the fixed point; `logit-config`'s `CollectdIn { bind, types_db }` + config tests; graph-rule additions + `is_datagram_listener` + tests; CLI `build_spec` arm (loads `types_db`, `base_dir`-relative) + test; `schema/logit.schema.json`; bench fixtures + decode allocation cases (+ one with `types_db` resolving, expecting the same allocation count) + `docs/design/memory.md`; `docs/design/pipeline-graph.md` arity/rule text; `docs/design/internal-telemetry.md` entry for `types_db_mismatch`; `docs/design/data-model.md` naming rule; `docs/deploying.md` listener/multicast/types.db mention | W1 |
| W3 | **Landed** (#142). **`collectd_out`** | `crates/logit-outputs/src/collectd.rs`, `lib.rs`; `logit-config`'s `CollectdOut` + its default-max-packet-bytes function; graph-rule + zero-bound test additions; CLI `build_spec` arm + host resolution (`collectd.host` → `host.name` → configured `hostname:` → drop-whole-list `no_host`) + test; schema; encode allocation case + `docs/design/memory.md`; `docs/design/internal-telemetry.md` entry (`no_host`); `docs/design/pipeline-graph.md` arity/rule text | W1 (parallel with W2) |
| W4 | **Landed**, in two halves (#141 the recorded-interop half, #144 this one). **Integration + closeout** | `crates/logit-cli/tests/collectd_round_trip.rs` + `tests/fixtures/collectd/*`; `script/record-fixtures`'s `record_collectd()`, `tools/record-fixtures/collectd.conf`, `testdata/interop/collectd/{README.md,*.raw}`, `testdata/interop/README.md`; `examples/collectd-relay.yaml`, `examples/collectd-to-influxdb.yaml` (mirroring `statsd-relay.yaml`/`statsd-to-influxdb.yaml`, every default commented); `docs/OVERVIEW.md`'s ingest-scope paragraph; `AGENTS.md`'s current-state paragraph and lossless-pairs bullet; `docs/deploying.md`; `docs/plans/recorded-interop-fixtures.md` follow-on list | W2, W3 |
| W5 | **Notifications** | `decode.rs` Message/Severity → `Event::log` (`LogRecord{message, severity}` + `collectd.severity` + identity attrs); `encode.rs` (an event with a `log` payload and `collectd.severity` encodes as TimeHR, Severity, identity, Message; 255-byte message truncation; a severity outside `{1,2,4}` or an empty message is dropped, counted `notification_dropped`); unit/fixed-point/robustness cases; corpus fixtures; a `threshold`-plugin recorded capture; `docs/design/data-model.md` row; `docs/known-gaps.md` update; ADR amendment | W4 |

**Status (2026-09-12): W0–W4 have landed; W5 (notifications) has not started.** W4 went in as two
independent PRs rather than one, since its two halves share no files: #141 carried the recorded
`collectd` interop captures, `record_collectd()` and `examples/collectd-to-influxdb.yaml` (all of
which need only `collectd_in`, so it could start as soon as W2 merged), and #144 carried
everything that needs `collectd_out` — `crates/logit-cli/tests/collectd_round_trip.rs` with its
binary fixture corpus, `examples/collectd-relay.yaml`, and the closeout docs. The pair is therefore
usable and documented end to end today; what W5 adds is collectd's *other* payload kind, not a
missing piece of this one.

Landing order: **W0 → W1 → (W2, W3) → W4 → W5.** Each PR is opened against its parent workstream's
branch (`feat/collectd-w0` from `origin/main`; `w1` from `w0`; `w2` and `w3` both from `w1`; `w4` from
`w2` merged with `w3`; `w5` from `w4`) and retargeted to `main` once its parent merges, since this
lets work on the later workstreams proceed without waiting on every earlier PR to land first. Not
built, and noted in the ADR's Consequences rather than as a TODO here: an encoder-side use of
`types_db` to regroup fallback (non-`collectd.*`) records back into multi-DS lists — the fallback
path only ever emits stock single-DS types, so nothing needs it.

## Verification

- Every workstream's PR: `script/check` (fmt --check + clippy -D warnings + nextest) and
  `script/cibuild` pass; `script/schema` regenerated and committed for any workstream that changes a
  config type (W2, W3); `script/validate` passes the two new example configs (W4); `script/audit`
  unchanged (no new dependency anywhere in this plan).
- **W0** (this PR): docs only, no code — `script/cibuild` doesn't apply. Verified instead by: every
  relative link in the touched files resolving, the ADR's headings matching
  `docs/adr/TEMPLATE.md` exactly, and both `docs/adr/README.md`/`docs/plans/README.md` gaining a row
  ordered by `created`.
- **W1**: the hand-written corpus and the `proptest` packet grammar both generate only
  already-normalized inputs — identity bytes drawn from the sanitizer's fixed set
  (`[A-Za-z0-9._-]`, ≤127 bytes, no `/` or NUL) and times that survive a cdtime→ns→cdtime round trip
  unchanged — so `decode(encode(b)) == b` is exact `EventBatch` equality over that corpus, and
  `encode(decode(encode(b))) == encode(b)` holds over unconstrained wire input (per the ADR's
  permitted-normalizations list, not exact equality against the original bytes). A `/`-bearing
  identity is covered separately by an explicit sanitizer unit test, not by the generator. The
  robustness suite never panics over the truncation and bit-flip corpus.
- **W2/W3**: `build_spec` wiring tests pass; allocation-count constants and their
  `docs/design/memory.md` rows are updated together in the same commit, never relaxed to an
  inequality; `crates/logit-core/tests/type_sizes.rs` stays untouched by this pair.
- **W4**: the socket round-trip corpus passes byte-exact modulo the ADR's permitted normalizations;
  recorded interop captures decode to the expected plugin/type shapes with the fixture's configured
  hostname. A manual smoke test (not automated): a real `collectd` daemon pointed at
  `examples/collectd-to-influxdb.yaml` shows its metrics landing in InfluxDB; `collectd-relay.yaml`
  forwards into a second collectd instance's own listener.
- **W5**: notification fixtures round-trip; the `threshold`-plugin capture decodes to a `LogRecord`
  with the expected severity.

## Open risks

- `>2⁵³`-magnitude counters lose precision once stored in `Sum.value: f64` — documented as a
  known gap, not fixed; matches OTLP's own int/double collapse, and a typed-integer value is a
  core-model question outside this effort's scope.
- Record names are wire-chosen (`<plugin>.<type>[.i]`), which grows the global string interner the
  same way statsd metric names already do — bounded in practice by `MAX_VALUES_PER_LIST` per list,
  since attribute keys themselves are fixed and never wire-chosen.
- `influxdb_out` renders an N-value event as N measurements sharing one tag set; giving each a
  distinct `.<ds_name>`/`.<i>` name avoids the same-series-same-timestamp collision that would
  otherwise nudge them apart.
- Multicast join can't be exercised in CI without an actual multicast route on the runner; the unit
  test skips rather than fails on the known errnos for that case, so this path's confidence rests
  more on the manual smoke test than the automated suite.
- No new crate dependencies anywhere in this plan (`bytes`, `socket2`, `proptest` are already
  present); `script/audit` is expected to be unaffected.
