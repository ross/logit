# 0012 — `Event` carries a log, metrics, and a span at once, not one of the three

## Status
Accepted

## Context
`logit_core::Event` has always been a one-of: `payload: Payload`, where `Payload` is `Log(LogRecord)
| Metric(MetricRecord) | Span(SpanRecord)`. That models every input built so far correctly — statsd
produces metric-only events, and no log- or span-producing input exists yet — but it stops being
correct the moment a real log-shaped input does exist. A single nginx access-log line is a log
record *and*, once a transform derives request counts, byte counts, and latency from its fields, a
source of several metrics. Under the one-of model those two things can't live on the same event:
either a transform has to fabricate a second, unrelated event to carry the derived metrics (losing
the "these came from the same access-log line" relation a downstream sink or correlated-tagging use
case needs), or the model has to change. This ADR is the change, driven by planning the concrete
`kv_metrics` transform (`docs/plans/0002-nginx-integration.md`'s workstream E) that needs to add
metrics to an event that already has a log body.

## Decision
`Event` gains three independent fields in place of one `Payload`:

```rust
pub struct Event {
    pub timestamp: i64,
    pub attributes: AttrMap,
    pub log: Option<LogRecord>,
    pub metrics: MetricList,        // SmallVec<[MetricRecord; 1]>
    pub span: Option<SpanRecord>,
}
```

`Payload` is deleted. An event is *whatever it carries* — a log, some metrics, a span, several of
those at once, or (now legally) none at all — not a tagged one-of. A sink emits whatever it finds:
`influxdb_out` writes every metric on an event and ignores its log/span; a future `stdio_out` writes
the log line and every metric together, on one line.

**Named fields, not `SmallVec<[Payload; 1]>` or `Vec<Payload>`.** The alternative of keeping one
polymorphic payload list was considered and rejected: it admits a state this model has no use for
(two logs on one event) with no way to express "does this event have a log?" except a linear scan
and a match — exactly the kind of check every sink and native transform needs to make constantly.
Named fields make that check typed and free (`event.log.is_some()`), and make two-logs-on-one-event
unrepresentable by construction rather than merely undocumented.

`MetricList = SmallVec<[MetricRecord; 1]>`, inline capacity 1: the overwhelmingly common shape is a
single metric (statsd, unchanged) or none at all (a bare log line); a transform that derives several
metrics from one log event (`kv_metrics`) spills to the heap only past the first.

**`Event` gained four constructors** (`Event::metric`, `::log`, `::span`, `::empty`) so that
building the ordinary one-payload shape stays a one-line call at every input/transform's
construction site, and so `smallvec` stays an implementation detail of `logit-core` — no other crate
in the workspace takes a direct `smallvec` dependency just to write `smallvec![record]`. `AttrMap`
already hides `smallvec` behind its own API the same way (`crates/logit-core/src/attrs.rs`); this
follows that precedent rather than inventing a new one.

**An empty event (no log, no metrics, no span) is now legal and representable**, where it was
previously impossible to construct. Nothing currently produces one deliberately, but nothing needs
to reject it either — `Fanout`, `Transform::process`, and every sink already handle "nothing to do
with this event" as a no-op, not an error.

**`Transform::process`'s `Option<Event>` signature did not need widening.** This was worth
confirming explicitly rather than assuming: `kv_metrics` (and `aggregate`'s own `process`, amended
below) *add* metrics to an event they already hold, or *absorb* metrics off one — both are still
exactly one event in, at most one event out. Nothing in this model requires a transform to split one
event into several from `process` (that already exists, via Lua's `return {a, b}` fan-out and
`EmitMany`, and remains untouched).

**`event.type` is removed outright, not given a precedence rule.** The first-pass design considered
keeping a single classification string (`"log"`/`"metric"`/`"span"`, resolved by a precedence order
when an event carries more than one) but rejected it: once an event can carry several payloads at
once, a summary string is strictly lossy versus checking the specific thing a consumer cares about,
and worse, it's an active footgun — a Lua script or native component branching on
`event.type == "metric"` would silently skip the metrics on a log-carrying event, exactly the shape
`kv_metrics` exists to produce. `crates/logit-script/src/proxy.rs`'s `EventProxy` instead exposes
`event.has_log` / `event.has_metrics` / `event.has_span` (booleans, read-only, and present in
`to_table()`) as the only introspection surface. There's no back-compat cost to removing a field
outright here: pre-1.0, no external config or script users exist yet (the same reasoning ADR 0009
used to justify its own breaking config-format change).

**Branch isolation already held, and still holds — nothing new needed here.** A correctness property
worth stating explicitly given the model change: a mutation one branch of a fan-out makes to an
event must not be visible to a sibling branch reading the same upstream event. This was already true
before this ADR and remains true unchanged: `Fanout::send`/`send_blocking`
(`crates/logit-pipeline/src/fanout.rs`) deep-clone the whole `EventBatch` — via `Event`'s derived
`Clone` — for every consumer but the last, *before* any downstream node can touch it, so two branches
never share the same `Event` value. `Clone` still deep-copies whatever `log`/`metrics`/`span` an
event happens to carry, so this holds structurally regardless of how many payloads an event carries.
Proven directly, not just asserted, by
`a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch`
(`crates/logit-pipeline/src/runtime.rs`), which drives a real two-branch fan-out and asserts a
mutating transform's change on one branch never appears on the other's copy.

**What does change: the average per-branch clone cost goes up**, since an event can now carry a log
and several metrics at once where before it carried exactly one payload — more bytes moved per extra
fan-out consumer. This is not a new problem; `docs/design/pipeline-graph.md`'s "Backpressure:
diamonds are the normal shape now" section already identifies `Arc<EventBatch>` + copy-on-write as
the future fix and explicitly defers it ("a known cost, not designed now"). That section gains one
sentence noting the cost profile shifted; copy-on-write itself stays out of scope here, same as
before this ADR.

## Amended by this change
- **ADR 0008** (aggregation window semantics): pass-through is now per-*metric*, not per-*event* —
  see that ADR's amendment section.
- **ADR 0010** (JSON parsing into attributes): "only `Payload::Log` events... are candidates" no
  longer parses (the type is gone) — see that ADR's amendment section.
- `docs/design/data-model.md`'s "Top-level shape" and "Payload types" (retitled "Record types")
  sections.
- `docs/design/lua-api.md`'s `event.type` description, replaced by the `has_log`/`has_metrics`/
  `has_span` surface.

## Alternatives considered
- **`Vec<Payload>` or `SmallVec<[Payload; 1]>`**, keeping one polymorphic list instead of three
  named fields. Rejected: admits an unwanted state (two logs on one event) and makes "does this
  event have a log?" an O(n) scan-and-match instead of a typed field access every sink and transform
  needs to do constantly.
- **Keep `Payload` as a one-of, and have a transform like `kv_metrics` emit a second, separate
  metric-only event alongside the original log event** (fan-out via `EmitMany`, effectively).
  Rejected: it works mechanically, but loses the "these values came from this exact log line"
  relation — a sink that wants to emit both together (a `stdio_out` writing "here's the request,
  here's what it cost") has no way to recover that relation once the events have gone their separate
  ways down the graph, short of re-correlating by timestamp, which is fragile and unnecessary when
  the relation could simply not have been thrown away.
- **Keep `event.type` as a precedence-ordered string** (log > metric > span, plus a new `"empty"`
  case) instead of removing it. Considered first, then rejected in review: it is strictly less
  informative than `has_log`/`has_metrics`/`has_span` for a fully lossless surface that already had
  to exist anyway, and it is an active footgun for exactly the mixed-event shape this ADR exists to
  enable.

## Consequences
- ~80 call sites across `logit-core`, `logit-script`, `logit-pipeline`, `logit-inputs`,
  `logit-transforms`, and `logit-outputs` that constructed or matched on `Payload` were mechanically
  updated; two — `aggregate::process` and `influxdb`'s line encoder — needed real structural changes
  (see the ADR 0008 amendment and `crates/logit-outputs/src/influxdb.rs`'s `render_tag_suffix`
  split, respectively).
- `logit-inputs::statsd` is deliberately **unchanged** in observable behavior: it still emits one
  event per `:`-separated value in a multi-value line, each carrying exactly one metric, rather than
  folding them into one multi-metric event. Folding would save a clone per extra value but is an
  opportunistic behavior change unrelated to what this ADR exists to enable; `statsd.rs`'s
  `only_metric` test helper now asserts this shape explicitly so a future attempt at folding fails
  loudly there instead of silently changing seventeen tests' meaning.
- `crates/logit-outputs/src/influxdb.rs`'s `allocate_timestamp` (the per-series "smallest free
  timestamp slot" union-find allocator) needed **no algorithmic change**: distinct metric names on
  one event produce distinct series keys and never collide; a repeated metric name on one event
  takes the existing same-series collision path, producing byte-identical output to the equivalent
  N-separate-events shape. Tag rendering was hoisted out of the per-metric loop into
  `render_tag_suffix` (computed once per event, since tags depend only on `resource.attributes` +
  `event.attributes`, never on which metric is being encoded) purely for cost, not correctness — a
  regression test (`several_metrics_on_one_event_share_its_tags_and_each_get_a_line`) pins down that
  the hoist changed nothing observable.
- Error handling in the InfluxDB encoder moved from the per-*event* loop to the per-*metric* loop: a
  `Set` metric or a `#`-prefixed measurement name sharing an event with an otherwise-good metric no
  longer takes that good metric down with it.
