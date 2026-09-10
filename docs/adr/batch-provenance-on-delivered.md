---
created: 2026-09-10
updated: 2026-09-10
---

# Batch provenance (`origin`/`previous`) carried on `Delivered`, stamped by `Fanout`

## Status

Accepted

## Context

A batch flowing through the component graph carries no record of where it entered `logit` or
which node handed it to the current one. The only per-batch metadata that already travels the
graph is `TraceContext` (which trace, which span — [ADR
`trace-context-propagation-on-delivered`](trace-context-propagation-on-delivered.md)) and
`Arc<Resource>` (host/service identity of the *data*, `docs/design/data-model.md`). Neither
answers "which listener admitted this batch" or "which component did the current node just
receive it from" — questions an operator debugging a multi-branch graph, or a Lua script
conditioning behavior on where a batch came from, needs answered without threading a manual
attribute through every component in a config.

Two pieces of metadata close that gap:

- **`origin`** — the component that created the batch. A listener, normally; the `internal`
  component in the one non-listener case; and, per the Decision below, a flush-bearing
  transform when it mints a batch from accumulated state. Set once, never mutated afterwards.
- **`previous`** — the component that most recently handled the batch, i.e. the node the current
  component received it from. Rewritten at every hop, as the last action before handoff.

Both must be readable by components — native transforms, sinks, Lua scripts — and writable by
none: a transform that could set its own `origin` would defeat the property that makes them worth
trusting downstream. Neither belongs on `Event::attributes` or `Resource`: they are pipeline-graph
identities, not data the source ever produced, and stamping them onto every event would blur
exactly the boundary `crate::trace`'s own doc comment already draws between the application's
trace context and `logit`'s own pipeline one.

`logit_out` → `logit_in` is a deliberate special case. When collection and processing are split
across two `logit` processes ([overview](../OVERVIEW.md)), the wire hop between them must be
invisible to `origin`/`previous`: the node after `logit_in` should see `previous` naming the node
that fed `logit_out` on the *far* side, not `logit_in` itself — otherwise every split-collection
deployment would show a graph shape that isn't the one actually configured.

## Decision

**Provenance lives on the graph edge (`Delivered`, alongside `TraceContext`), not inside
`EventBatch`.** The alternative — a field on `EventBatch` — was rejected on one decisive point,
not on churn: `EventBatch` has public fields and is rebuilt by every transform and Lua hop
(`process_batch`, `run_lua`'s post-`process` batch), so a field there is either forgeable (any
`Transform`/`Input` impl can write whatever it likes) or silently droppable (a rebuild site that
forgets to carry it forward resets it to `None`, with no compiler error to catch the omission).
Keeping it on the edge, where the only construction site is `Fanout`, makes both failure modes
structurally impossible: a component never constructs a `Delivered` and never chooses what
provenance it carries.

**`Provenance { origin: Option<Symbol>, previous: Option<Symbol> }` lives in `logit-core`,
bundled with `TraceContext` into a new `BatchContext` in `logit-pipeline`.** Not in
`logit-pipeline` alongside `TraceContext` itself, despite that being the closest precedent:
`logit-proto` has to name the type to encode/decode it and cannot depend on `logit-pipeline` (the
dependency runs the other way), and `logit-script` depends on `logit-core` only — exactly why
`crate::trace` (in `logit-script`) passes raw `[u8; 16]`/`[u8; 8]` arrays across that boundary
instead of `TraceContext` itself; `Provenance` avoids repeating that workaround. `TraceContext`
stays in `logit-pipeline` because it carries real behavior (`new_root`, `child`, random-id
minting); `Provenance` is inert data, the same character as `Symbol`/the interner it's built from,
both already in `logit-core`. Bundling into `BatchContext` rather than adding a third `Delivered`
element matters because provenance has to reach every place `TraceContext` already reaches
(`Delivered`, `SinkQueue`, `Transform`'s per-batch hooks, the disk-queue record) — one struct means
each of those is widened once, not twice.

**`Fanout` is the only writer.** It already sits at the single choke point every producing node
sends through, and the node runtime already builds one per component with that component's own id
in scope. `Fanout` gains an `Option<Symbol>` component field, set via a `with_component(&str)`
builder (mirroring the existing `with_telemetry`, not a `Fanout::new` parameter — `new` has
roughly twenty test call sites that would otherwise all need updating for a value most of them
don't care about). Immediately before constructing a `Delivered`, both `send_with_own_context` and
`send_blocking_with_own_context` apply one rule:

```
origin   = origin.or(Some(self.component))   // get_or_insert: set once
previous = Some(self.component)              // always rewritten
```

This one rule, applied uniformly with no per-node-kind special-casing, produces every behavior the
Context section asks for:

- A listener's first send has empty incoming provenance, so it sets *both* fields — `origin` and
  `previous` both name the listener. `internal` needs no special case: it's a listener by role.
- A later hop finds `origin` already set and only rewrites `previous`.
- A flush emission (`run_flush`, Lua's `flush_now`) mints a fresh `BatchContext` with empty
  provenance, exactly as it already mints a fresh `TraceContext` root — so the flushing component
  becomes *both* `origin` and `previous` on what it emits, with the same justification the
  existing `TraceContext::new_root()` call already has: a flushed batch is a new artifact (merged
  sketches/counters that were never any one input event), not a re-emission of something that
  passed through unchanged, so naming the flush as the origin is the honest statement, not an
  approximation. `Provenance`'s own n-to-1 concern is structurally identical to `TraceContext`'s;
  the resolution is identical too.
- A real fan-out gives every branch the identical provenance — one emission, not several, the same
  property `Fanout` already guarantees for `TraceContext`.
- A transform can neither forge nor drop provenance: it never constructs a `Delivered`, and
  `process_batch` rebuilds only the `EventBatch`, which does not carry it.

**`logit_in`'s relay is a separate rule, `Fanout::stamp_relayed`, not the default one.** It
back-fills only whatever the wire didn't carry:

```
origin   = origin.or(Some(self.component))     // same as stamp()
previous = previous.or(Some(self.component))   // or, not overwrite
```

A v2 peer that sent its own `origin`/`previous` gets both relayed completely untouched — the
property this whole special case exists for. A v1 peer (whose frame carries no provenance at all)
or a v2 peer that genuinely had none gets `logit_in`'s own id backfilled into both fields, so a
downstream reader never sees them empty for no operator-visible reason.

**Wire format: a new codec (`CODEC_NATIVE_V2`), not an in-place change to v1.** The native v1
payload is positional with no forward-compatibility seam at the batch level, and
`crates/logit-proto/tests/robustness.rs`'s `assert_every_truncation_fails_cleanly` pins the
invariant that no proper prefix of a valid v1 encoding is itself valid — every field is
length-checked, nothing optional. An *optional* trailing section on the existing format would
break that invariant silently: a payload truncated exactly at the trailer boundary would decode as
"no provenance," a valid-looking result, not an error. So v1 stays byte-for-byte unchanged
(`encode_batch`/`decode_batch`, untouched), and provenance ships as `encode_batch_v2`/
`decode_batch_v2`, which call the v1 functions as subroutines and add a **mandatory**
length-prefixed trailer:

```
payload_v2 := dict | resource attrs | uvarint(event_count) | events...
            | uvarint(trailer_len) | trailer_bytes[trailer_len]
trailer_bytes := (tag: u8, len: uvarint, value: [u8; len])*   -- tag 1 = origin, tag 2 = previous
```

Because the trailer's own length prefix is mandatory — always at least one byte, even when both
fields are absent — `decode_batch_v2` holds the identical truncation invariant `decode_batch`
does: cutting anywhere in the v1-shaped prefix already fails via `decode_batch`'s own logic;
cutting right at the boundary means the `trailer_len` read finds nothing and fails; cutting inside
the trailer body fails the declared-vs-remaining check. A plain v1 payload fed to `decode_batch_v2`
also fails, by the same mechanism — v2 is a distinct codec, not a superset of v1's, so `logit_in`'s
codec dispatch (not silent fallback inside the decoder) is what decides which to call. Each
trailer field holds its string inline, not dictionary-indexed: `origin`/`previous` are at most two
scalar strings written once per batch, so there's no repetition within one payload for a
dictionary to pay off on, unlike attribute/metric keys.

**Negotiation needs no new machinery.** `logit_out`'s `Hello.codecs` becomes `[CODEC_NATIVE_V2,
CODEC_NATIVE_V1]`; `logit_in`'s handshake already validates `Hello.codecs` against what it
supports and acks the best shared choice. An unmodified old `logit_in` sees `[2, 1]`, doesn't
recognize `2`, and acks `1` today with no code changes on that side; an unmodified old `logit_out`
only ever offers `[1]` and gets `1` back. Either direction talks; provenance is simply absent
whenever either side is on v1.

**The disk queue's 24-byte trace-context prefix (`CONTEXT_LEN`) is not widened.** Records on disk
carry no version of their own, so a wider fixed prefix would silently misparse every already
-spooled record on upgrade (`walk_segment`'s resync arithmetic assumes the old, narrower prefix).
Provenance rides inside the v2 frame payload instead, where the codec byte `parse_record` already
reads makes it self-describing: `CODEC_NATIVE_V1` records decode with `Provenance::default()`,
`CODEC_NATIVE_V2` records decode their trailer, and an already-spooled v1 record keeps replaying
correctly forever.

**Two new hooks, not widened existing ones, for reading it.** `Transform::observe_provenance`
(default no-op) sits alongside `observe_batch_context`, not folded into it: `Aggregator` exposes
`observe_batch_context` as an inherent method its own tests call directly with a bare
`TraceContext`, and no transform today has a use for provenance, so a second hook costs nothing
rather than forcing an unrelated signature change on the one type that would break.
`Output::observe_batch(BatchContext)` (default no-op) is new on the `Output` trait, called by
`write_loop` immediately before each delivery attempt (retries included, so the same value is used
across all of them) — `logit_out` is the one implementer, threading provenance into
`encode_batch_v2`. This also hands sinks the trace context they didn't have direct access to
before, a small bonus from the same seam.

**Lua exposure is a read-only `provenance` global, built as `UserData`, not a plain table like
`trace`.** `trace`'s own module doc concedes a script's write to it is silently accepted and only
clobbered on the *next* batch — that fails "never modifiable" outright. `provenance` mirrors
`crate::resource`'s `__index`/`__newindex` proxy shape instead, but rejects every write in
`__newindex` rather than accepting one: `provenance.origin`/`.previous` (mutated per batch, mirrors
`resource`/`trace`'s staleness during a `flush()` call — same documented gap, not new) and
`provenance.component` (this worker's own id, set once via `ScriptWorker::with_component`, a
builder rather than a `new()` parameter for the same "don't touch every existing call site" reason
`with_telemetry` already is one). Installed unconditionally, before the script's own source runs,
same reasoning as `trace`/`resource`: a top-level alias (`local p = provenance`) captures the
*userdata reference*, not a snapshot, so a later mutation through `set`/`with_component` is still
visible through it — installing first only has to guarantee the global exists by the time any such
alias could be taken.

## Alternatives considered

- **A field on `EventBatch`.** Rejected — see Decision: forgeable and silently droppable, the
  opposite of the trust property this feature exists to provide.
- **An optional trailing section on the existing v1 payload**, keeping one codec. Rejected: breaks
  `robustness.rs`'s pinned truncation invariant, the concrete failure mode being a truncated
  payload decoding as a *valid*, wrong result ("no provenance") instead of an error.
- **Dictionary-indexing the trailer's two strings**, matching how attribute/metric keys cross the
  wire. Rejected as unnecessary complexity: `origin`/`previous` are two scalar strings written
  once per batch, with no repetition within one payload for a dictionary to amortize, and
  dictionary-indexing them would require the trailer to share `encode_batch`'s internal
  `DictBuilder`, coupling a widely-tested, unmodified function to a new caller for no measured
  benefit.
- **Widening `TraceContext`/`CONTEXT_LEN` in the disk-queue record** instead of adding a v2 wire
  codec. Rejected: no per-record versioning exists on disk, so a wider fixed prefix would silently
  corrupt every already-spooled segment on upgrade, discovered only as mass "corrupt, resyncing
  past" log noise rather than a clean codec-dispatch decision.
- **Overwriting `previous` unconditionally on `logit_in`'s relay too** (the same rule every other
  hop uses), rather than `stamp_relayed`'s back-fill-only rule. Rejected: that would make
  `logit_in` indistinguishable from an ordinary transform hop, destroying exactly the
  cross-wire-transparency property the special case exists for — the node after `logit_in` would
  always see `logit_in` itself as `previous`, never the real upstream node.

## Consequences

- `size_of::<Delivered>()` is now 64 (was 56) — `BatchContext` (32 bytes: `TraceContext`'s 24 plus
  `Provenance`'s 8, no padding) replacing `TraceContext` as the second element. Pinned by
  `crates/logit-pipeline/src/fanout.rs`'s own test; `docs/design/memory.md` §2 carries the
  channel-capacity-times-8-bytes cost, not an allocation-count change.
- `crates/logit-bench/tests/allocations.rs`'s `disk_queue_push_one_batch` constant moved 25 → 27:
  `encode_batch_v2` builds v1's payload as its own `Bytes` then copies it into a fresh, larger
  `BytesMut` alongside the trailer, rather than extending in place. Every other allocation-count
  assertion in that suite held exactly — provenance is two `Copy` `Option<Symbol>`s on the hot
  path, with interning happening once per node at graph build, never per batch.
- `logit_out`/`logit_in` on different builds still talk: negotiation degrades to v1 automatically,
  with provenance simply absent, not a breaking change to the wire protocol or a forced upgrade.
- Existing on-disk sink-buffer segments remain fully readable after upgrade — `CONTEXT_LEN` never
  changed, and `parse_record`'s codec dispatch handles both v1 and v2 records in the same segment.
- `Fanout` gains `with_component`, `stamp`/`stamp_relayed` (private), `send_relayed`/
  `send_relayed_blocking`. `Transform` gains `observe_provenance`. `Output` gains `observe_batch`.
  All additive — no existing public signature changed except `Delivered`'s and `SinkQueue`'s
  element type (`TraceContext` → `BatchContext`, via `impl From<TraceContext> for BatchContext`
  absorbing most call-site churn).
- `crates/logit-pipeline/src/fanout.rs`'s test module verifies the stamping contract directly (a
  listener's first send sets both fields; an interior hop rewrites only `previous`; a fan-out gives
  every branch identical provenance; `send_relayed` backfills only what's missing).
  `crates/logit-proto/src/native/mod.rs` and `crates/logit-proto/tests/robustness.rs` verify the
  v2 codec's truncation/bit-flip robustness and that it rejects a plain v1 payload rather than
  silently decoding it. `crates/logit-pipeline/src/disk_queue.rs` verifies a spool round trip and
  that a hand-written pre-existing v1 record still replays after this change.
  `crates/logit-cli/tests/logit_round_trip.rs` verifies the end-to-end property across a real
  `logit_out` → `logit_in` hop: `origin` names the remote listener, `previous` names the remote
  node that fed `logit_out`, not `logit_in` itself.
