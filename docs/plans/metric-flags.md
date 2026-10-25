---
created: 2026-10-22
updated: 2026-10-23
---

# Enabling plan: metric flags — producers set `_logit.metrics.*`, and `aggregate` reads flags, not protocols

## Context

`aggregate` probes every event for four protocol carriers to decide two kinds of special
handling. [ADR `metric-flags`](../adr/metric-flags.md) replaces the probes with flags the
event's creator sets, and its "Decision" section is the canonical table of flags, values,
producers, and readers. This plan is the work that realizes it.

Stream key: `flags`.

Goals:

- `crates/logit-transforms/src/aggregate.rs` imports nothing from `logit_proto`, and every
  source-specific decision moves to the decoder that made the event.
- A Lua script or a `set` can flag an event out of windowing, or add a string field to an
  InfluxDB point, through the same flags.
- `aggregate`'s per-event cost on the perf VM returns to where it was before the probes
  (`72191ead`: 0.334 µs on `aggregate`, 0.293 on `aggregate-groups`), or the gap is explained.

Non-goals:

- Honoring the flags in `dedup_timestamps` or `cumulative_to_delta`. The names leave room for
  it; nothing here builds it.
- A flags byte on `Event`. It's the ADR's fallback, not taken: W2 measured the attribute lookup
  inside the spread (the Outcome paragraph under "Measurement").
- Changing which carriers the protocol sinks read. `statsd_out`, `prometheus_out`,
  `datadog_out`, and `influxdb_out` keep their own carriers; `influxdb_out` reads the renamed
  `_logit.field.` as well.

## Design

The ADR holds the vocabulary. The pieces this plan adds to it:

- **The namespace constant lives in `logit_core::attrs`**, as `FIELD_PREFIX` and `is_field`
  beside `PRIVATE_PREFIX` and `is_private`, because the rule is a data-model rule, not an
  InfluxDB one. `logit_proto::influxdb` uses it rather than defining its own.
- **The resource half is read in `begin_batch`.** `Transform::begin_batch` receives the batch's
  `&Arc<Resource>` once per batch (`dedup_timestamps` already uses it), so `aggregate` reads
  `_logit.metrics.closed` off the resource there and off the event in `process`. A direct caller
  of `Aggregator::process` that skips `begin_batch` loses the resource half; the existing test
  that stamps the stats mark on the resource is the one that catches it.
- **The regression test** feeds `aggregate` an event carrying every protocol carrier
  (`_logit.datadog.stats.name`, `_logit.statsd.timestamp`, `_logit.prometheus.timestamp`, a
  `_logit.field.*` key; the two timestamp carriers were later retired, ADR
  `protocol-knowledge-stays-in-family`'s "Amendment: move, don't mirror (2026-10-23)") and no flag, and asserts it is windowed with the field key in its
  series identity. It fails on main, which still reads the carriers.
- **Transit.** `script/transit gen` rewrites the InfluxDB cases and every flow's expectations
  for the rename, and the aggregate flows' ledgers for the flags. A flow's class doesn't change;
  only key names and counters do.

## Workstreams

| # | Branch | PR title | Size | Depends on | Status |
|---|---|---|---|---|---|
| W0 | `flags/w0` | `flags/w0: ADR and plan for producer-set metric flags` | S | — | merged 2026-10-23 |
| W1a | `flags/w1a` | `flags/w1a: string fields move to the _logit.field. namespace` | M | W0 | merged 2026-10-23 |
| W1b | `flags/w1b` | `flags/w1b: decoders set _logit.metrics.* flags, and aggregate reads flags only` | L | W1a | merged 2026-10-23 |
| W2 | `flags/w2` | `flags/w2: VM measurement of the flag reads and the series-key copy` | S | W1b | merged 2026-10-23 |
| W3 | `flags/w3` | `flags/w3: ADR and plan for keeping protocol knowledge in family` | S | W1b | merged 2026-10-23 |

- **W1a.** `FIELD_PREFIX`/`is_field` in `logit_core::attrs`; the InfluxDB decoder and encoder,
  `line_protocol`, and `aggregate`'s scan under the new name (no behavior change);
  `logit-config` field docs and `script/schema`; the soak `http-ingress` generator config;
  `script/transit gen`; the amendments to
  [ADR `influxdb-line-protocol-relay`](../adr/influxdb-line-protocol-relay.md) and
  [ADR `private-attribute-namespace`](../adr/private-attribute-namespace.md); every doc that
  names the old prefix (`data-model.md`, `deploying.md`, `AGENTS.md`, `known-gaps/mappings.md`
  through `mapping.yaml`).
  `fuzz/seedgen` stays as is: its one use of the old prefix is a wire-level tag key that the
  reserved-prefix guard omits under either name, and regenerating committed seeds needs the
  nightly image.
- **W1b.** The decoders set both flags beside the carriers they already stamp; `aggregate` reads
  flags only, through `begin_batch` for the resource half, and drops its `logit_proto` imports;
  the `passed_through` reasons, `flagged` included; the regression test; `script/transit gen`
  for the counters; the amendments to
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md) and
  `private-attribute-namespace`; the `data-model.md` rows; the Lua capability in
  [lua-api.md](../design/lua-api.md) with an example.
- **W2.** A docs PR. The protocol is in "Measurement" below; the numbers are in
  [performance.md](../design/performance.md)'s "`aggregate`: the flag reads and the series-key
  copy (2026-10-23)".
- **W3.** [ADR `protocol-knowledge-stays-in-family`](../adr/protocol-knowledge-stays-in-family.md)
  generalizes this stream's rule to every component, and
  [its plan](protocol-knowledge-stays-in-family.md) lists the four cross-family reads left after
  W1b, each a future workstream outside this stream.

## Measurement

On the perf VM, bisect-style through each binary's own harness, 3 rounds of 5 repeats, on
`aggregate`, `aggregate-groups`, `json-parse`, and `passthrough`:

| Arm | Purpose |
|---|---|
| `72191ead` | before both probes |
| main at W1b's merge base | today |
| W1b | the flag read |
| floor | W1b with no flag read at all (a throwaway via `script/vm build`, not a PR) |

Read W1b against main and `72191ead`, and the floor against `72191ead`. If W1b sits above
`72191ead` by more than the runs' spread and the floor closes the gap, open the `Event` flags
byte as its own measured change; otherwise record the cost. Don't decide on desktop numbers:
#826's desktop A/B read −3.4% / −4.8% where the VM read −0.65% / −0.55%.

**Outcome.** Against the floor, the flag reads cost less than the spread on all four scenarios.
W1b sat +4.7% / +4.1% above `72191ead` on `aggregate` / `aggregate-groups` until it removed the
series-key copy `1f30eb3c` introduced (`1ceca584`), after which it reads at `72191ead`'s level. No
`Event` flags byte. [performance.md](../design/performance.md)'s "`aggregate`: the flag reads and
the series-key copy (2026-10-23)" has the numbers.

## Review mechanics

- W1a and W1b get `pr-review --comment` after opening, with the lead assessing each finding, the
  implementer fixing in-branch, and a re-review of the fix delta. W0 and the W2 docs PR get a
  lead read.
- Each code PR's body says which regression test was run with its change reverted, and what it
  printed.
- Before each PR: the stale-prose sweep for the old prefix and the old probe descriptions, the
  banned-word grep over added comments, and `script/cibuild`.

## Findings

Found while reading `aggregate` for this plan, out of scope here:

- `SeriesKey` clones the event's attributes per record to look up the series
  (`event.attributes.clone()` in `Aggregator::process`). W1b removed the second copy that the
  string-field pruning had added, a clone into a local map then moved into the key (+4.8% /
  +4.7% on the VM). The per-record clone remains: for `aggregate`'s `host: h{seq%N}` that's a
  `Bytes` refcount pair per event, more than any probe. A borrowed-key lookup would remove it;
  nothing measures that yet.
- `datadog_out` and `datadog/series.rs` call `is_datadog_stats` per event, which does an
  `interner::lookup` per call. The symbol form `aggregate` used is free for them to take once
  `aggregate` stops calling it.
