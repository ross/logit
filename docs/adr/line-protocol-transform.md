---
created: 2026-10-16
updated: 2026-10-23
---

# `line_protocol`: parse a log message as InfluxDB line protocol into one event per point, skipping bad lines

## Status
Accepted. String fields follow [ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)'s
"Amendment: string fields are private (2026-10-19)", and the decode mapping below is amended to
match.

## Context
Some producers put InfluxDB line protocol inside another protocol rather than POSTing it to a write
API. Prusa's Buddy firmware sends one RFC 5424 syslog message per UDP datagram whose MSG is a
header line and about 37 points ([capture details](../../examples/prusa/README.md)). With
`framing: datagram` ([ADR `syslog-udp-datagram-framing`](syslog-udp-datagram-framing.md)),
`syslog_in` delivers that MSG as one log event. Nothing turns its body into metrics:

- `influxdb_in` parses line protocol only from a `POST /api/v2/write` body
  ([ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)).
- The parser transforms (`json`, `csv`, `logfmt`, `kv`, `regex`, `http_access`) write attributes
  onto the event they're given. None makes metrics, and until
  [ADR `transform-fan-out`](transform-fan-out.md) no native transform could make several events.
- A `lua` stage can do it with `Event.new`, but it re-implements line-protocol escaping and field
  typing in Lua patterns and has to rebuild the measurement-carrier convention by hand.

`logit_proto::influxdb` already has the grammar, settled against a real `influxd`, and the mapping
from a point to an event. Its module doc (`crates/logit-proto/src/influxdb/mod.rs`) is the
canonical mapping table. [ADR `parsers-live-in-logit-proto`](parsers-live-in-logit-proto.md) says
a transform that parses producer bytes wraps a parser in `logit-proto` rather than holding its own.

## Decision
Add the transform kind `line_protocol`. It parses an event's log message as line protocol and
emits one event per point, through `Transform::process_into`.

```yaml
points:
  type: line_protocol
  sources: [prusa]
  precision: ns        # ns | us | ms | s; the unit of every timestamp in the message
  keep_source: false   # true also forwards the parent with its log
```

**Input.** The transform reads `event.log.message` when it's a `Value::Str` or `Value::Bytes`. An
event with no log, or with a message of any other type, passes through untouched and uncounted,
as `json` treats one.

**Each point is one event in `influxdb_in`'s shape.** The mapping is the decode table in
`crates/logit-proto/src/influxdb/mod.rs`, unchanged:

- The measurement becomes the private attribute `_logit.influxdb.measurement`.
- Tags become event attributes, `Value::Str`. A tag key under `_logit.` is omitted and counted
  `logit.transform.tags.omitted{reason="reserved_prefix"}`.
- Each numeric or boolean field becomes a typed `Gauge` named `<measurement>.<field>`, in wire
  order.
- Each string field becomes the private attribute `_logit.field.<key>`.
- Points in one message with the same measurement, tag set, and timestamp merge into one event,
  field by field, as `influxdb_in` merges them within one body (that ADR's normalization 2).
  Points in different messages never merge.

**Timestamps.** A point's timestamp is scaled to nanoseconds by `precision`, `ns` by default as in
the write API. A point with no timestamp takes the parent event's `timestamp` as it is, without
truncating it to `precision`: `influxdb_in` truncates a request's arrival time because `influxd`
does, but the parent's timestamp is a recorded time, not an arrival.

**Inherited attributes.** Each child starts from a copy of every parent attribute, so
`syslog_in`'s `host.name` rides along. The point's own tags, its measurement carrier, and its string fields
are written over that copy, so the point wins on a key collision. The parent's log, metrics, and
span aren't copied. The resource, scope, trace context, and provenance are the parent batch's
([ADR `transform-fan-out`](transform-fan-out.md)).

**The parent's fate.**

- With `keep_source: false`, the default, the transform removes the parent's log once its points
  are emitted. A parent with no metrics or span left is discarded, counted
  `logit.component.events.discarded`; one that still carries metrics or a span is forwarded
  without its log, so a payload the transform didn't read is never lost.
- With `keep_source: true`, the parent is forwarded unchanged beside its points.
- A message that yields no point (every line bad, or only blank and comment lines) forwards the
  parent untouched, whichever `keep_source` is set.

**A bad line is skipped and the rest of the message is parsed.** A line is bad when it fails the
grammar or its timestamp falls outside the range `influxdb_in` accepts. Each one is counted once,
by what happened to its text:

- `logit.transform.lines.dropped{reason="malformed"}` when the parent's log doesn't go on, the
  default for a message with at least one good point. The line's text has left the pipeline, so
  it's loss ([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)).
- `logit.transform.lines.skipped{reason="malformed"}` when the parent's log goes on, under
  `keep_source: true` or for a message with no good point. The text is still in the pipeline, as
  an empty row `csv` forwards untouched is counted `rows.skipped`.

Either way, a throttled diagnostic `parse_error` names the 1-based line number and the parser's
reason. It doesn't quote the whole line, because a diagnostic is the process's own log and the
line is producer data, but a reason can name the line's measurement, a key, or a number token, as
influxd's own error text does.

**One construction path, shared with `influxdb_in`.** `logit_proto::influxdb` factors the event
construction out of `InfluxDecoder::decode_body` so that both callers use it: building an event
from a parsed `Line`, merging a later point of the same series and timestamp into it, and counting
reserved-prefix tags. The caller supplies the base attributes (empty for `influxdb_in`, the
parent's for the transform), the timestamp for a point with none, and what a bad line does:

- `decode_body` stays all or nothing. It parses every line first and builds nothing if one fails,
  because an HTTP client can't resend half a request. It emits its own counts under
  `logit.input.`.
- `decode_skipping`, the transform's path, skips a bad line and keeps going, because it has no
  client to answer. It emits nothing: it reports each bad line to the caller, which decides
  between `lines.dropped` and `lines.skipped` and counts under `logit.transform.`, along with the
  reserved-prefix tags the decoder's per-call stats report.

A second copy of the mapping in `logit-transforms` is not
allowed: the two would drift, and the module doc would describe only one of them.

**Not `influxdb_in`.** The transform carries no `org`, `orgID`, or `bucket`, so `influxdb_out`
writes its points to its configured `bucket:` and `org:`, else its `default_bucket:` and
`default_org:`. `syslog_in` with `line_protocol` isn't a like-protocol relay pair, and nothing in
[ADR `lossless-transit`](lossless-transit.md) changes.

## Alternatives considered
- **All or nothing per message, as `decode_body` does.** It fits a write API, where the client
  sees the `400` and fixes the body. A transform has no one to tell, so one bad line would cost
  every good point beside it. Prusa's own collector skips a bad line and keeps the rest
  (`LineProtocolParser.parse` in `utils/metrics/collect.py`).
- **Forward the parent untouched when any line is bad.** It keeps the bad line's text, but loses
  every good point of that message as metrics.
- **A `format: line_protocol` option on `syslog_in` or `lines_in`.** Every listener that can carry
  line protocol would need the option, and it couldn't sit after a Lua stage that rewrites the
  body, which the Prusa pipeline needs.
- **A Lua recipe only.** It works today, but each user re-implements escaping and typing, and the
  measurement carrier is easy to get wrong.
- **A Prusa-specific input.** Everything Prusa-specific (the header, the boot-relative time, the
  format versions) fits in a Lua stage before this transform, and the transform serves any other
  producer that wraps line protocol.

## Consequences
- Each child costs a copy of the parent's attributes. Tag values and string fields that need no
  unescaping are zero-copy slices of the parent's message, as in `influxdb_in`.
- Every inherited parent attribute reaches `influxdb_out` as a tag: `syslog.facility` and
  `syslog.tag` from `syslog_in`, for example, and a nil RFC 5424 TIMESTAMP's `Value::Null`
  `syslog.timestamp`, which `influxdb_out` omits and counts. An operator places `keep`, `remove`,
  or a Lua stage to choose the tags.
- A child's attribute under `_logit.field.` inherited from the parent leaves
  `influxdb_out` as a string field.
- The existing `influxdb_line` fuzz target covers the shared construction path, and the skip mode
  joins it.
- The canonical copy of the per-line rules, the counters, and the parent's fate is the
  transform's module doc; `docs/design/internal-telemetry.md` gains the two line counters.
