---
created: 2026-09-17
updated: 2026-09-17
---

# Enabling plan: Prometheus remote-write — receive on `prometheus_in`, send on `prometheus_out`

## Context

[ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md) decides the shape: an optional
`bind:` on `prometheus_in` is a remote-write **receiver**, an optional `endpoint:` on
`prometheus_out` is a remote-write **sender**, both versions 1.0 and 2.0 are mapped through a
`remote_write.rs` beside `text.rs` onto the `MetricFamily` seam
[ADR `prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md) already fixed,
and the flat-sample assembler `text.rs` owns today is hoisted into a shared `assemble.rs` first.
This plan is the build-out: what lands in which order, in which files, and how it is verified. Read
the ADR first — this document doesn't repeat its reasoning, only what implements it.

Stream key **`rw`**: branches `rw/w0`…`rw/w6`, a strictly linear stack, each PR based on and
targeting its parent's branch, brought up to date with `git merge origin/main` (never rebase). W4
(the sender) is developed in parallel with W3 (the receiver) — they share only `ComponentKind` and
the regenerated schema — but is **stacked after** it so the stack stays linear. PR stack only:
nothing is merged by this workstream; Ross directs merging.

## Decisions already settled

| Question | Decision |
|---|---|
| Sender wire version | Explicit `version: 1 \| 2`, default `1` (`prometheus.WriteRequest`). No auto-negotiation, no 415 fallback — the operator picks, as they pick an exposition dialect |
| Receiver version | Accepts both on one `bind:`, selected per request from `Content-Type`; the 2.0 spec requires a 2.0 receiver to keep accepting 1.0 |
| Receiver state | **Stateless first.** Typing comes only from metadata in the request being decoded (2.0 inline `Metadata`, 1.0 same-request `metadata[]`). The bounded metadata cache is W5, not part of W3 |
| Receiver TLS | Server TLS now, `bind_tls: Option<TlsServerConfig>` |
| TLS key names | `prometheus_in`'s existing `tls:` → **`scrape_tls:`** (client, scrape mode); receiver's is `bind_tls:`; `prometheus_out`'s sender client TLS is `endpoint_tls:`. Pre-release, no compat shim, no alias |
| Exposition `bind:` | Unchanged — no TLS, no auth, known-gaps row as-is |
| Resource identity | Labels stay labels. `instance`/`job` are ordinary attributes; no `prometheus.target`, no lifting to `Resource`. One `EventBatch` per request, empty `Resource` |
| Multi-sample series | Decode partitions a request's samples by timestamp, one assembler per distinct timestamp, ascending; encode partitions the batch by `Event::timestamp` and merges identical label sets into one `TimeSeries` with ordered samples |
| Timestamps | Receiver sets `Event::timestamp` **without** the `prometheus.timestamp` marker; sender always emits one (ms) |
| Stale markers | `Point::Stale` ↔ the stale NaN `0x7ff0000000000002` ↔ `FLAG_NO_RECORDED_VALUE`, for single-series kinds only. `Histogram`/`Summary`/sketch kinds with the flag stay skipped+counted |
| 2.0 created timestamp | `created_timestamp` ↔ `Series.created` ↔ `MetricRecord::start_timestamp`, the existing `_created` path (which message carries the field is settled against the vendored proto in W1). 1.0 has none |
| Exemplars | Both ways via the existing `Exemplar` mapping; sender emits on counter and `_bucket` series only, as the text writer does |
| Body cap | `MAX_REQUEST_BYTES = 4 MiB` constant on the **decompressed** body, matching `otlp_in`; Snappy's `decompress_len` checked before decompressing. Not a config field |
| Wrong method | `405 + Allow: POST` — a deliberate divergence from `otlp_in`'s `404`, matching `prometheus_out`'s exposition routes; documented in the module doc |
| Sender retry | None in the sink. One request per `send`; 429/5xx → `Fault::Ambiguous`, other 4xx permanent; `write_loop` retries. `duplicate_safe()` stays `true` — it is what selects `AtLeastOnce` |
| Native histograms | Deferred to a follow-up plan. Skipped+counted on send (`ExponentialHistogram`) and on receive (`TimeSeries.histograms`); the known-gaps row is reworded to point at the follow-up |
| Landing | PR stack only. Nothing merged by this workstream; Ross directs merging |

## Design

### 1. Protos and dependency (W1)

- Vendor `prompb/remote.proto`, `prompb/types.proto` and
  `prompb/io/prometheus/write/v2/types.proto` verbatim at a pinned `prometheus/prometheus` tag
  under `crates/logit-proto/proto/prometheus/prompb/…`, plus `gogoproto/gogo.proto` under
  `crates/logit-proto/proto/gogoproto/` so the upstream `import` lines resolve unchanged. Extend
  [`crates/logit-proto/proto/README.md`](../../crates/logit-proto/proto/README.md) with the tag,
  commit and file list, as it already does for OTLP.
- The only gogoproto feature in use is `(gogoproto.nullable) = false` — a no-op for prost — but
  both `import "gogoproto/gogo.proto"` and the intra-package `import "types.proto"` must resolve in
  `tools/protogen`. 1.0's `MetricMetadata.type` enum is exactly `FamilyType` (`UNKNOWN` ↔
  `Unknown`).
- [`tools/protogen/src/main.rs`](../../tools/protogen): today one `DEST` const (`:11`), one
  OTLP-specific filename rewrite (`strip_prefix("opentelemetry.proto.")`, `:43`), a single include
  dir `PROTO_ROOT` (`:31`), and no prost-build customizations to mirror. Turn `FILES`/`DEST` into a
  per-family table `(include_dirs, files, dest_dir, filename_rewrite)`; add the prompb family
  writing to `crates/logit-proto/src/prometheus/generated/{prometheus.rs,
  io.prometheus.write.v2.rs}`; pass both include roots. Hand-write `generated/mod.rs` mirroring
  `crates/logit-proto/src/otlp/generated/mod.rs`'s `#[path]`/`#[rustfmt::skip]`/allow-attribute
  nesting (prost's cross-package types are `super::`-relative), exposing `generated::prometheus` and
  `generated::io::prometheus::write::v2`. Run `script/protogen`, commit the output.
- Add `snap = "1"` to `[workspace.dependencies]` and to `crates/logit-proto/Cargo.toml`. BSD-3-Clause
  is already allowed in [`deny.toml`](../../deny.toml).
- Verification: `cargo build -p logit-proto`, `cargo deny check`, `script/cibuild`.

### 2. Codec: `logit_proto::prometheus::remote_write` (W2)

**The assembler hoist comes first, in its own commit, before any remote-write code.** The flat-sample
assembler (`Role`, `SUFFIXES`, `suffix_applies`, `bare_name_role`, `FamilyAccum`, `SeriesAccum`,
`Parser::route`, `finish_series`, `text.rs:200-660`) is what remote-write's flat series need and is
private to `text.rs`. It moves to `crates/logit-proto/src/prometheus/assemble.rs` as a `pub(super)`
`Assembler`:

- `Assembler::new(implicit: FamilyType)` — the kind an undeclared family gets (`Untyped`/`Unknown`
  from the text dialect, `Unknown` for remote-write).
- `declare(base_name, FamilyType, help, unit)`.
- `push(sample_name, labels, value: f64, timestamp_nanos, exemplar, decoder)`.
- `push_created(sample_name, labels, created_nanos, decoder)` — the nanos-typed path both
  `Role::Created` and 2.0's `created_timestamp` need.
- `finish(decoder) -> Vec<MetricFamily>`.

`text::Parser` keeps what is genuinely text syntax and delegates the rest: line parsing, `dialect`,
`saw_eof`, `parse_sample(line, dialect)`, `unescape_help`'s dialect arg, `Parser::untyped()`
(`Unknown` vs `Untyped`, `text.rs:399-426`), and `Role::Created`'s `parse_created`
(`text.rs:470-476`, `:990`), which parses the value *text* to avoid float rounding. **Every skip and
degrade reason string stays byte-identical**, so `crates/logit-proto/tests/prometheus_fixed_point.rs`
and `text.rs`'s own tests pass with no expectation changes.

**Seam additions in `crates/logit-proto/src/prometheus/mod.rs`:**

- `Point::Stale` — a variant, not a `Series` field, so exhaustive matches find every site and no
  struct literal in the existing tests churns.
- `families_to_events` maps a `Stale` series to the family type's zero-shaped kind with
  `FLAG_NO_RECORDED_VALUE` set, replacing today's hardcoded `flags: 0` (`mod.rs:467`).
- `PrometheusEncoder::with_stale_markers(bool)`, default `false`: a flagged `Gauge`/`Sum`/
  marker-untyped record becomes a `Point::Stale` series, bypassing the early `is_no_recorded_value`
  return at `mod.rs:560`. Flagged `Histogram`/`Summary`/sketch kinds stay skipped+counted (their
  derived `_bucket`/`_sum`/`_count` series can't be reconstructed from one flag) — a known-gaps row.
  `text::write` skips a `Stale` point, counted.
- `PrometheusEncoder::with_timestamps_always(bool)`, default `false`: fill `Series.timestamp` from
  `Event::timestamp` even without the `prometheus.timestamp` marker.
- `prometheus_fixed_point.rs`'s `series()` generator (`:416`) stays free of `Stale`, so property 1
  (whole-value `PartialEq` round trip) holds under the default encoder; `Stale` gets a dedicated test
  with the switch on.

**`remote_write.rs` public surface:**

- `pub enum Version { V1, V2 }` with `content_type()`, `header_version()` (`0.1.0` / `2.0.0`), and
  `from_content_type(&str) -> Option<Version>`: bare `application/x-protobuf` and
  `;proto=prometheus.WriteRequest` → `V1`; `;proto=io.prometheus.write.v2.Request` → `V2`; anything
  else `None`, which is the receiver's `415`. Serde on the config side maps `1`/`2` onto it.
- `pub fn decode(body: &[u8], version: Version, decoder: &mut PrometheusDecoder) -> Result<Decoded,
  CodecError>`, over the **decompressed** protobuf — the caller does Snappy and the size check.

  ```rust
  pub struct Decoded {
      /// One family list per distinct sample timestamp, ascending.
      pub groups: Vec<Vec<MetricFamily>>,
      pub samples: u64,
      pub exemplars: u64,
      pub histograms_skipped: u64,
  }
  ```

  The counts feed the 2.0 `-Written` response headers. **1.0:** `declare` each `MetricMetadata`
  first (family name as sent — the assembler's `_total` handling covers both spellings), then bucket
  every `TimeSeries × Sample` by timestamp and `push` into that group's assembler with `__name__` as
  the sample name. A series lacking `__name__`, carrying an empty or duplicate label name, or with
  an unsorted label set is skipped and counted (`skipped{reason="invalid_labels"}`), **not** a `400`
  — one bad series does not lose the request. **2.0:** resolve `labels_refs`/`help_ref`/`unit_ref`
  through `symbols`; a bad ref, an odd-length `labels_refs`, or `symbols[0] != ""` is
  `CodecError::Malformed` → `400`, since those are structural, not per-series. `declare` from inline
  `Metadata` (`UNSPECIFIED` → `Unknown`); `created_timestamp != 0` → `push_created`. The stale NaN
  (`0x7ff0000000000002`) on any sample → `Point::Stale`. `histograms` entries →
  `histograms_skipped`, `skipped{reason="native_histogram"}`. Exemplar labels through the existing
  OpenMetrics exemplar mapping.
- `pub fn encode(groups: &[Vec<MetricFamily>], version: Version, encoder: &mut PrometheusEncoder)
  -> Vec<u8>`, uncompressed — the caller Snappy-compresses. Flatten each family (counter → one
  series; histogram → `_bucket{le}` + `_sum` when `Some` + `_count`; summary → `{quantile}` +
  `_sum` + `_count`; info → `name_info` = 1; stateset → one series per state; unknown → one;
  `Stale` → one stale-NaN sample). Label set is `__name__` plus the labels, **sorted by byte order
  after** adding `__name__`/`le`/`quantile` (`__name__` does *not* always sort first —
  uppercase-initial names precede `_`). Series with identical label sets are merged across groups
  into one `TimeSeries` with samples in timestamp order; timestamp is `Series.timestamp` in ms,
  always `Some` on this path. **1.0:** one `MetricMetadata` per family in `metadata[]`. **2.0:**
  symbol table with `""` at index 0, inline `Metadata` per series, `created_timestamp` from
  `Series.created`. Exemplars on `_total`/`_bucket` series only.

**Tests.** `crates/logit-proto/tests/prometheus_remote_write_fixed_point.rs`, both versions: groups
→ `encode` → `decode` → identical groups (exemplars, created, stale, multi-timestamp series); the
text corpus → parse → rw encode → decode → `text::write` equal to the direct text output modulo the
permitted normalizations; 1.0 without metadata → `Unknown` families with `_bucket`/`_sum` left as
plain series; 2.0 symbol-table edge cases; a label-sort property test. Bench fixtures in
`crates/logit-bench/src/fixtures.rs` beside the existing `prometheus_decoder` (`:1330`),
`prometheus_encoder` (`:1334`) and `prometheus_gauge_events` (`:1342`), with
`remote_write_decode_one_request` / `remote_write_encode_100_series` rows in
`crates/logit-bench/tests/allocations.rs` and `docs/design/memory.md` (the existing Prometheus rows
are at `:234` and `:278`) — pins and doc rows in the same commit, per `AGENTS.md`.

### 3. Receiver: `prometheus_in` `bind:` (W3)

**Config** (`ComponentKind::PrometheusIn`, `crates/logit-config/src/lib.rs:1560`):
`scrape_targets` becomes `#[serde(default)]` (empty = unset); new `bind: Option<String>`,
`path: String` default `/api/v1/write`, `bind_tls: Option<TlsServerConfig>` (`:284`, matching every
other listener's `Option<TlsServerConfig>` at `:427, 534, 635, 690, 790`), and
`idle_timeout: Option<Duration>` (`with = "humantime_serde_duration"`, `:2828`, plus the
`#[schemars(with = "String")]` hint). `tls` → `scrape_tls` in the config, in
`crates/logit-cli/src/pipeline.rs`'s `PrometheusIn` arm (`:444-452`), in the input's module doc, and
in this pair's ADR/plan/known-gaps prose.

**Rule 55** (`crates/logit-pipeline/src/graph.rs` module doc `:6-260` plus the inline check in
`resolve()`; 54 is the highest rule in use, 55 and 56 are the next free): exactly one of
`scrape_targets`/`bind`; a non-default scrape-only field (`interval`, `timeout`, `headers`,
`scrape_tls`) with `bind:` is an error, and a non-default bind-only field (`path`, `bind_tls`,
`idle_timeout`) with `scrape_targets` is an error — rule 45's (`:182-186`) and rule 53's
(`:243-252`) shape. `interval` keeps its default so rule 9's `interval: 0s` rejection (`:531-540`)
stays satisfied in bind mode. Update the kind role/name tables (`graph.rs:329/388`, `:363/422`) and
run `script/schema`, committing `schema/logit.schema.json` — `script/cibuild` fails when it is
stale.

**Input** (`crates/logit-inputs/src/prometheus.rs`): `PrometheusInput` becomes a mode enum (or two
structs behind one `Input`); `tick` is untouched. In receiver mode `Input::bind` opens the
`TcpListener`, idempotently, per `crates/logit-pipeline/src/input.rs:24-29`'s obligations, and `run`
is an accept loop copied from `otlp_in`'s (`crates/logit-inputs/src/otlp.rs:369-500`): auto h1/h2
builder, optional TLS via `crate::tls::TlsServerSettings`, connection permits, first-byte peek,
handshake timeout. That loop's otlp-specific tags mean it is **copied, not shared**; what does hoist
cleanly, in its own commit with zero `otlp_in` behaviour change, is `drive_with_idle`
(`otlp.rs:688-700`) and `Activity`/`InFlight` (`:594-680`) into a shared `logit_inputs::http`
module. `serve_connection` stays otlp-specific. There is no listener-level graceful shutdown
anywhere in the repo; "graceful then bounded drop" is per-connection, as `otlp_in` does it.

Routes:

| Request | Response |
|---|---|
| `POST path`, `Content-Encoding: snappy`, recognised `Content-Type` | decode; `204` (+ `X-Prometheus-Remote-Write-{Samples,Histograms,Exemplars}-Written` when the request was 2.0) |
| other path | `404` |
| other method on `path` | `405` + `Allow: POST` (divergence from `otlp_in`'s `404` at `otlp.rs:838`, documented in the module doc) |
| missing/other `Content-Encoding`, unrecognised `Content-Type` | `415` |
| compressed body or Snappy `decompress_len` > `MAX_REQUEST_BYTES` (4 MiB) | `413` |
| Snappy/protobuf failure, 2.0 symbol errors | `400`, `text/plain` reason |

One `EventBatch` per accepted request (empty `Resource`, `received_at` = now), built by
concatenating `families_to_events` over `Decoded.groups`, sent on the `Fanout` **before** the
response is built — the ordering `otlp_in` uses (`otlp.rs:904-925`) — so channel backpressure delays
the `204` and the sender's queue throttles, which is remote-write's own flow-control model.
Shutdown is per-connection, like `otlp_in`'s.

Counters match `otlp_in`'s request-counter spelling:
`logit.input.requests{class="ok"|"not_found"|"method"|"unsupported"|"oversize"|"bad_request"}`,
`logit.input.request.duration`, `logit.input.samples` (reused), plus the decoder's own
`skipped{reason}`. `warn_throttled("write_rejected", ..)` on `400`/`413`/`415`, with the peer
address in the message text only, never as a tag.

**The module doc is the spec**, per house convention: config table, routes table, "no `up` or
scrape synthetics in bind mode", labels-stay-labels, timestamp handling, and the timestamp-group
semantics.

**Tests:** an in-process hyper client per routes-table row, both versions; a multi-timestamp request
→ one batch with N events per series in order; a 1.0 request without metadata → untyped families;
backpressure (a full channel delays the response); TLS using `otlp_in`'s cert fixtures; idle-timeout
in `otlp_in`'s test shape; rule 55 config cases.

### 4. Sender: `prometheus_out` `endpoint:` (W4)

**Config** (`ComponentKind::PrometheusOut`, `crates/logit-config/src/lib.rs:1649`): `bind` becomes
`Option<String>`; new `endpoint: Option<String>` (an absolute `http(s)` URL including the path,
typically `/api/v1/write`), `version` (`1 | 2`, default `1`), `timeout: Duration` default 10s,
`headers: HashMap<String, String>` validated the way rules 22 and 40 already validate header maps
(rule 40 is at `graph.rs:1815-1887`; note its `tls` check is a *scheme* check, not a mode check)
against a `RESERVED_REMOTE_WRITE_HEADERS` list — `content-type`, `content-encoding`,
`x-prometheus-remote-write-version`, `user-agent`, `content-length` — mirroring
`RESERVED_PROMETHEUS_HEADERS` (`lib.rs:605`), and `endpoint_tls: TlsClientConfig` (`:246`).

**Rule 56:** exactly one of `bind`/`endpoint`; a non-default registry-only field (`path`,
`expire_after`, `max_series`) with `endpoint:` is an error, and a non-default sender-only field
(`version`, `timeout`, `headers`, `endpoint_tls`) with `bind:` is an error. `script/schema` again.

**Output** (`crates/logit-outputs/src/prometheus.rs`): a mode enum. Registry mode is unchanged —
`bind` spawns `serve` (`:531-541`), `flush`/`Drop` abort that task (`:578-586`). Endpoint mode's
`bind` is a no-op and its `flush` is `Ok`. The client is built like `otlp_out`'s HTTP path:
`build_client(timeout, tls)` (`crates/logit-outputs/src/otlp.rs:462`) with
`logit_outputs::tls::TlsClientSettings`, `User-Agent: logit/<ver>`.

`send`: partition events by `Event::timestamp`, `events_to_families` per partition with
`with_stale_markers(true)` and `with_timestamps_always(true)`; an empty result is `Ok(())` with **no
request**; otherwise `remote_write::encode` → `snap::raw::Encoder` → `POST` with the four protocol
headers `insert`ed over a clone of the operator's `headers:` map, so protocol-owned names win —
`otlp_out`'s own per-request merge (`otlp.rs:292-311`).

Response → `Fault` exactly as `otlp_out` does it, attached with `.context(fault)` (`otlp.rs:345`,
`:354`): 2xx ok; 429 and 5xx → `Fault::Ambiguous` via `is_retryable_http_status` (`:541`, `:336-340`);
transport errors via `classify_reqwest_error` (`:548`), which reserves `Fault::Clean` for connect
failures; any other 4xx is permanent, with a throttled warning carrying the first ~256 bytes of the
body (Prometheus's `400` text names the offending series). One attempt per `send`; `write_loop`
retries. `duplicate_safe()` stays `true` (`prometheus.rs:588`, already `true` today): one request per
batch, a sample's identity is `(label set, timestamp)` so a replayed identical request is an
idempotent overwrite — and `true` is what selects `DeliveryPosture::AtLeastOnce`
(`crates/logit-pipeline/src/output.rs:96-105,159-166`), without which `Fault::Ambiguous` is never
retried at all.

Counters use `otlp_out`'s spelling:
`logit.output.requests{class="2xx"|"4xx"|"429"|"5xx"|"network_error"|"timeout"}`,
`logit.output.request.duration`, `logit.output.samples`, plus the encoder's existing
`skipped`/`degraded` counters — delta temporality is still skipped and counted, with
`aggregate { temporality: cumulative }` as the named fix, unchanged from the exposition path.

**Module-doc caveat:** samples are sent in batch order and the sink does not reorder across batches,
so two upstreams writing one series can draw out-of-order `400`s from a receiver with no
out-of-order window. That is topology, not a sink bug, and the doc says so where an operator looks.

**Tests:** a canned hyper receiver asserting method, path, headers, Snappy framing and protobuf
contents for both versions; a multi-timestamp batch → one `TimeSeries` with ordered samples; stale
marker emission; an empty batch sending no request; 4xx/5xx/connect → the correct `Fault`; the
`headers:` merge and reserved-name rejection; TLS via the client cert fixtures; rule 56 config cases.

### 5. Receiver metadata cache (W5)

`metadata_cache: { max_families: 10000, ttl: 10m }` on `ComponentKind::PrometheusIn`, bind-mode only
(rule 55's wrong-mode check covers it); `max_families: 0` disables it. A
`HashMap<String, (FamilyType, help, unit, last_seen)>` keyed by family name, fed by every 1.0
`metadata[]` entry and every 2.0 inline `Metadata`, consulted by the receiver, which pre-`declare`s
cached entries into each group's assembler for series whose own request carried none. LRU eviction
over the cap, a TTL sweep per request. Counters: `logit.input.metadata_cache.size` and
`.evicted{reason="cardinality"|"expired"}`. The assembler's "exact family-name match beats suffix"
rule is what makes this work: a cached `foo` histogram claims `foo_bucket` exactly as
`# TYPE foo histogram` does in the text path. `script/schema`.

**Tests:** a metadata-only request followed by a samples-only request → typed families; TTL expiry →
untyped again; cap eviction.

### 6. Integration and closeout (W6)

- `crates/logit-cli/tests/prometheus_remote_write_round_trip.rs`: a real `prometheus_out(endpoint)`
  into a real `prometheus_in(bind)`, in process, both versions, over the same 10-fixture text corpus
  `crates/logit-cli/tests/prometheus_round_trip.rs` uses
  (`crates/logit-cli/tests/fixtures/prometheus/`) — text scrape → remote-write → exposition equal to
  the direct round trip's expected output — plus `statsd_in → aggregate(cumulative) →
  prometheus_out(endpoint)`, and stale-marker and exemplar cases.
- **Recorded interop fixtures.** Add a `capture_http` mode to
  [`tools/record-fixtures/raw_capture.py`](../../tools/record-fixtures/raw_capture.py), whose
  `--proto udp|tcp` modes today explicitly say HTTP is unimplemented (`:25-29`) and whose
  `capture_tcp` never responds: bind, accept N `POST`s, answer `204`, write each body verbatim.
  Record real Prometheus 3.x remote-write requests for both `protobuf_message` settings into
  `testdata/interop/prometheus/` with the per-producer provenance README and the
  low-single-digit-KB size budget `testdata/interop/README.md` mandates, and replay them through
  `remote_write::decode`.
- **Real-store check**, ad hoc rather than in the demo stack (which has no Prometheus service):
  `docker run prom/prometheus --web.enable-remote-write-receiver` as a target for
  `prometheus_out(endpoint)` in both versions, and a Prometheus `remote_write`-ing into
  `prometheus_in(bind)`. Commands and outcome go in the PR body.
- Examples: `examples/prometheus-remote-write-receive.yaml`,
  `examples/prometheus-remote-write-send.yaml`.
- Docs: [`docs/known-gaps.md`](../known-gaps.md) — delete the "Prometheus remote-write is not built"
  row; reword the `ExponentialHistogram` row to point at the native-histogram follow-up; add rows for
  the receiver having no auth, 1.0 typing depending on the cache, stale markers for
  histogram/summary kinds being skipped, and no out-of-order handling.
  [`AGENTS.md`](../../AGENTS.md)'s current-state paragraph;
  [`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md)'s existing
  `Prom. remote-write` matrix column at `:218` (update the cells, don't add a column) and its §112
  prose; the status rows in this plan and the ADR.

## Workstreams

| # | PR | Depends on |
|---|---|---|
| W0 | This plan; ADR [`prometheus-remote-write`](../adr/prometheus-remote-write.md); the "landed" pointer in [`prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md)'s forward-compat section and its plan's §5; index rows in both `docs/adr/README.md` and `docs/plans/README.md`. | — |
| W1 | Vendored prompb + gogoproto protos (README pin), `tools/protogen` per-family table, committed generated code, `snap` dependency. | W0 |
| W2 | Assembler hoist (own commit); `Point::Stale`, `with_stale_markers`, `with_timestamps_always`; `remote_write.rs` decode/encode for both versions with timestamp groups; fixed-point and bench tests. | W1 |
| W3 | `otlp_in` idle-helper hoist (own commit); `prometheus_in` `bind:` receiver, `tls` → `scrape_tls`, `bind_tls`, rule 55, regenerated schema, module-doc spec, tests. | W2 |
| W4 | `prometheus_out` `endpoint:` sender, `version`/`timeout`/`headers`/`endpoint_tls`, rule 56, regenerated schema, `Fault` mapping, tests. | W2 (developed in parallel with W3, stacked after it) |
| W5 | Receiver metadata cache. | W3 |
| W6 | Round-trip test, `capture_http` plus recorded Prometheus fixtures, real-store check, examples, docs closeout. | W3, W4, W5 |

Landing order: **W0 → W1 → W2 → W3 → W4 → W5 → W6**, strictly linear, each PR based on and
targeting its parent's branch and brought up to date with `git merge origin/main` (never rebase).
W4 touches no file W3 owns except `ComponentKind` and the regenerated schema, so it can be built
while W3 is in review, but it stacks after W3 rather than branching beside it so the schema
regeneration has one owner per PR.

## Verification

- Per PR: `script/cibuild` green in the dev container — fmt, clippy with warnings denied, the
  workspace test suite, `cargo deny`, and schema freshness. `script/schema` regenerated and
  `schema/logit.schema.json` committed by any workstream that changes a config type;
  `script/validate` over `demo/` and `examples/` for any workstream that adds one.
- W2: both fixed-point suites green, and `prometheus_fixed_point.rs` plus `text.rs`'s own tests pass
  with **no expectation changes** after the assembler hoist — that is the whole point of doing the
  hoist as a pure refactor in its own commit.
- W3/W4: the unit tests above; a manual `curl` with a hand-built Snappy body returns each
  routes-table code; a config setting both mode fields, or neither, fails with the rule 55/56
  message.
- W6: the round-trip test green; the recorded real-Prometheus fixtures decode; a real Prometheus
  receiver accepts `logit`'s 1.0 and 2.0 writes with the series visible in its UI; and a real
  Prometheus `remote_write`-ing into `prometheus_in(bind)` → `prometheus_out(bind)` exposes the same
  series it scraped, modulo the documented normalizations.
- W0 (this PR) is documentation only: verification is that every relative link resolves, the ADR's
  headings match [`docs/adr/TEMPLATE.md`](../adr/TEMPLATE.md), and both README indexes gained a row.
