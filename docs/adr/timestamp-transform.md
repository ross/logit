---
created: 2026-10-04
updated: 2026-10-04
---

# `timestamp`: resolving `event.timestamp` from an attribute, with jiff for calendar time

## Status
Accepted. Supersedes the `syslog_timestamp` transform sketched in `docs/known-gaps/syslog.md`.

## Context

`syslog_in` and `tail_in` stamp `event.timestamp` with receipt time
([ADR `decoupled-listener-io`](decoupled-listener-io.md); the `tail_in` rule in
[ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md), where `docker_in`
is the one exception and uses the envelope's `time`). `syslog_in` keeps the sender's stamp as the
`syslog.timestamp` attribute: a `Value::Timestamp` for RFC 5424, the raw 15-byte `Value::Str` for
RFC 3164, and `Value::Null` for a nil stamp.

A backlog replayed after an outage, such as an rsyslog disk queue draining or a re-read file,
therefore reaches every sink with the wrong record time: the `influxdb_out` point time, the
`otlp_out` `time_unix_nano` that Loki and Tempo index, the `splunk_hec_out` `time`, `datadog_out`'s
timestamps, `prometheus_out`'s remote-write samples, and, inside `aggregate`, the gauge last-write
ordering and `start_timestamp`. `aggregate`'s windows are wall-clock
([ADR `aggregation-window-semantics`](aggregation-window-semantics.md)), so a resolved timestamp
never moves a metric between windows; it changes the record times and the two `aggregate` fields
above.

`docs/known-gaps/syslog.md` sketched an opt-in `syslog_timestamp` transform for this. The need is
wider than syslog: `tail_in` followed by `json`, `regex`, or `logfmt` produces an attribute holding
the application's own stamp, and a plain text line carries nothing a listener could trust.
`trace_context`'s `span:` block already rewrites `event.timestamp` to a span's start and rejects an
instant further than `max_skew` from receipt
([ADR `trace-context-span-lifting`](trace-context-span-lifting.md)), citing the syslog sketch. This
ADR is the general form of both.

The sandboxed Lua stdlib exposes no clock, no calendar, and no time zone, so
[ADR `routing-by-condition-is-lua`](routing-by-condition-is-lua.md)'s premise, that a `lua`
component already does the job, is false for this kind, as it was for
[`sample`](consistent-sampling-component.md).

## Decision

**`timestamp` is a native transform kind in `crates/logit-transforms`, opt-in and placed by the
operator.** It resolves `event.timestamp` from one attribute:

```yaml
resolved:
  type: timestamp
  sources: [syslog]
  from: syslog.timestamp   # required, a literal attribute name
  format: rfc3164          # required, see below
  timezone: Europe/Berlin  # optional: IANA name, UTC, or ±HH:MM; default UTC
  max_skew: 24h            # optional; 0s is rejected
  keep_source: false       # optional
```

`format` is one of `rfc3339`, `rfc3164`, `unix_seconds`, `unix_millis`, `unix_micros`,
`unix_nanos`, or `{pattern: "<strftime-style>"}`. `timezone` defaults to `UTC`, never the host's
zone. Only `rfc3164` and a pattern with no `%z`, `%:z`, or `%s` read it; setting it under any other
format is a config error. A `Value::Timestamp` attribute is used as-is under every format, so one
listener's RFC 5424 and RFC 3164 senders both resolve under `format: rfc3164`.

A separate component, not a `syslog_in` flag, keeps "we trust our senders' clocks" a visible line
in the config graph, the composition argument
[ADR `operator-declared-resource-attributes`](operator-declared-resource-attributes.md) makes for
`set`. A general one, not a syslog-only one, follows [`scale`](scale-transform.md): the general
primitive is smaller and more reusable.

**An event the component can't resolve is forwarded untouched, never dropped.** Each skip counts
one reason:

| Reason | When |
|---|---|
| `missing` | The attribute is absent, `Null`, `""`, or `"-"`. |
| `invalid` | The value doesn't parse under `format`. |
| `skew_past`, `skew_future` | The resolved instant is further than `max_skew` before, or after, the event's current timestamp. |
| `span` | The event carries a span; its timestamp is the span's start, which `trace_context` owns. |
| `start` | A `MetricRecord.start_timestamp` is non-zero and later than the resolved instant. |

Skew is measured against whatever `event.timestamp` holds on arrival: receipt time, `docker_in`'s
envelope time, or an upstream stage's result. The two skew reasons are split so a backlog older
than the window reads differently from a sender with a wrong clock. A metric-only event is
resolved unless `start` applies.

When the component applies to a log whose `observed_timestamp` is 0, the previous
`event.timestamp` becomes `observed_timestamp`. That is OTel's meaning of the field, the time the
collection system observed the record; without it, `otlp_out` substitutes encode time.

Counters are `logit.transform.timestamp.resolved` and `logit.transform.timestamp.skipped{reason}`,
tallied per batch. There is no throttled `Diagnostics` warning: skew rejections are expected
traffic during a drain, not a fault.

**`max_skew` defaults to `24h`.** `trace_context`'s one hour would reject the overnight backlog this
component exists for, and a required field would make every operator copy the same number.
`datadog_out`'s 18-hour log window is the same order. Closest-year inference for RFC 3164 keeps a
result within about 183 days of receipt, so any `max_skew` under that also catches a wrong-year
guess. `datadog_out` drops metrics older than one hour as `stale`, which is narrower than this
default, so a metric drain through it shows as counted drops there. The default isn't tightened
for that one sink.

**RFC 3164 resolution.** The value is the 15-byte `Mmm dd hh:mm:ss`. The year is whichever of the
receipt instant's year in `timezone`, the year before, or the year after puts the stamp closest to
receipt; a Feb 29 candidate in a non-leap year is invalid for that candidate. Civil time is read in
`timezone`. A DST fold picks the occurrence closest to receipt, and a DST gap takes the offset after
the gap. A pattern with no year infers the year the same way.

**`format: rfc3339` uses jiff's RFC 3339 and Temporal parser.** It accepts a space separator, a
lowercase `z`, and an RFC 9557 `[zone]` suffix, and it clamps a `:60` leap second to `:59`, because
this format reads application logs. The syslog codec's strict RFC 5424 parser in `logit_core::time`
(uppercase `T` and `Z` required, leap second rejected) is a different contract, and this ADR leaves
it unchanged. The hand-rolled calendar code in `logit_core::time` is slated to move onto jiff in its
own record.

**Pattern rules.** A `{pattern: ...}` uses strftime-style directives as jiff's `fmt::strtime`
defines them and must match the whole value. `%z`, `%:z`, or `%s` makes the result an instant, and
`timezone` isn't read; otherwise the result is civil time in `timezone`. A pattern with no year
infers it as RFC 3164 does. Two examples:

| Source | Pattern |
|---|---|
| nginx and Apache `$time_local` | `%d/%b/%Y:%H:%M:%S %z` |
| Postgres jsonlog with `log_timezone = UTC` | `%Y-%m-%d %H:%M:%S.%f UTC` |

The Postgres pattern matches `UTC` as a literal, because zone abbreviations are ambiguous and `%Z`
is rejected.

**`keep_source` defaults to `false`, following `trace_context`.** A resolved source attribute is
removed. This interacts with `syslog_out`: a kept `syslog.timestamp` is written verbatim on an RFC
3164 output and renders the resolved instant on RFC 5424, while a removed one is re-rendered from
`event.timestamp` in UTC. An operator with a non-UTC `timezone:` in front of `syslog_out` sets
`keep_source: true`.

**jiff 0.2 does the calendar math and the pattern parsing.** jiff is pure Rust and licensed
`Unlicense OR MIT`. It's a dependency of `logit-core` only, exposed through a small
`logit_core::zoned` module that both graph validation and the transform call, so one function
decides what a valid zone or pattern is.

**The time zone database is the system's.** With jiff's default features, a named zone reads
`/usr/share/zoneinfo`, or `TZDIR` when set, on Linux; the database isn't bundled into the binary.
`debian:bookworm-slim` (the release image base) and `rust:1.98.1-bookworm` (dev and CI) ship
`tzdata` as a `required`-priority package; `Dockerfile`, `Dockerfile.dev`, and CI install it
explicitly anyway so the dependency is declared. A scratch or distroless image has to add it. `UTC`
and fixed offsets need no database. A named zone that fails to load, or any named zone when no
database is found, is a `logit validate` and startup error, never a per-event skip: nothing per
event looks a zone up by name, because `%Q` is rejected in patterns.

**Graph rule 76** rejects an empty `from`; `max_skew: 0s`; a `timezone` that doesn't resolve; a
`timezone` set under a format that never reads it; and a pattern that is empty, has no time of day,
uses `%Z`, `%Q`, or `%:Q`, or fails to parse its own rendering of a reference instant.

## Alternatives considered

- **A `syslog_timestamp` transform, or a flag on `syslog_in`.** Rejected: `tail_in` with `json`,
  `regex`, or `logfmt` has the same need, and a listener flag hides the trust decision inside the
  listener's config instead of the graph.
- **A `lua` component.** Rejected: the sandboxed stdlib has no clock, calendar, or time zone.
- **Hand-rolled calendar code.** Rejected: it would support fixed offsets only, so a local-time RFC
  3164 sender in a DST zone would be wrong half the year. That's the class of silently wrong
  timestamp this component exists to remove, and it would be a fourth hand-rolled date module to
  keep correct.
- **`chrono`.** Rejected: named zones come only through `chrono-tz`, which compiles the whole tz
  database as Rust, and its parser ignores `%Z`.
- **`time`.** Rejected: fixed offsets only, and its own `[year]-[month]` format syntax rather than
  strftime.
- **jiff's `tzdb-bundle-always` feature.** Rejected: it adds a few hundred KB to every binary and
  freezes the database at build time, where the OS package receives updates.
- **A required `max_skew`, or `trace_context`'s one-hour default.** Rejected: see the `24h`
  decision above.
- **A throttled diagnostic on a skew rejection**, as the syslog sketch proposed. Rejected: during a
  drain every old event is one, and the `skipped{reason}` counter already says how many.

## Consequences

- jiff enters the dependency tree, through `logit-core` only.
- `tzdata` is a runtime requirement for a named zone. An image without it fails validation on a
  config that names one, and works for `UTC` and fixed offsets.
- `logit.transform.timestamp.*` is a new `logit.transform.*` metric family.
- A local-time RFC 3164 sender left on the default `timezone: UTC` shifts every event by its offset,
  silently, because the shift stays inside the 24-hour `max_skew`. The field docs and the fixture
  tell operators to set `timezone:`.
- Follow-on work points the `syslog_timestamp` sketch, and the receipt-time gap entries that cite
  it, at this component:
  `docs/known-gaps/syslog.md`, `docs/known-gaps/tailing.md`, `docs/known-gaps/statsd.md`,
  [ADR `syslog-output`](syslog-output.md),
  [ADR `syslog-structured-data-convention`](syslog-structured-data-convention.md),
  [ADR `trace-context-span-lifting`](trace-context-span-lifting.md),
  [ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md),
  `docs/plans/lossless-transit.md`, and the module docs of `crates/logit-inputs/src/syslog.rs` and
  `crates/logit-outputs/src/syslog.rs`.
