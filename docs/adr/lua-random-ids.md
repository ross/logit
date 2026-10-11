---
created: 2026-10-11
updated: 2026-10-11
---

# Lua `random`: a global that mints trace ids, span ids, UUIDs, and ULIDs, with the time passed in

## Status
Accepted

## Context

A script can build a span with `Event.new` ([ADR `lua-event-constructor`](lua-event-constructor.md))
and can write `event.log.trace_id`, but it has no way to produce a fresh id for either. `math.random`
returns a double, so 128 random bits take several calls, string formatting, and an all-zero check
the script has to know to make. A script minting a span for a line that carries no trace, or
stamping a request id on a log it constructs in `flush()`, had to hand-roll that.

Three constraints shape the answer:

- **A script has no clock.** The sandbox exposes no time source
  ([ADR `lua-event-constructor`](lua-event-constructor.md)'s context;
  [ADR `timestamp-transform`](timestamp-transform.md) moves an event's time onto the event itself).
  `flush(now)` and `event.timestamp` are the only times a script sees, each a decimal-nanos
  string. UUID v7 and ULID encode a millisecond time.
- **`logit` already has a minter.** `logit_core::trace::random_id_bytes::<N>()` is a per-thread
  SplitMix64 seeded from `RandomState`. Its doc says an id from it "needs only not to repeat, not
  to resist prediction", and
  [ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md) records its 64 bits
  of entropy per process. `splunk_hec_out` already built a v4 UUID from it by hand for its channel.
- **LuaJIT seeds `math.random` with one constant.** Two VMs created in one process draw the same
  `math.random()` sequence, so every worker of a `lua` component, and every `lua` component,
  produced the same "random" values in the same order.

## Decision

1. **A `random` global, a table of six Rust functions**, installed on every worker's VM beside
   `trace` and `Event`:

   | Call | Returns |
   |---|---|
   | `random.trace_id()` | 32 lowercase hex characters, never all-zero |
   | `random.span_id()` | 16 lowercase hex characters, never all-zero |
   | `random.uuid()` | a UUID v4, `8-4-4-4-12` lowercase hex |
   | `random.uuid7(ts)` | a UUID v7 (RFC 9562) whose 48-bit millisecond field is `ts / 1_000_000` |
   | `random.ulid(ts)` | a 26-character ULID, Crockford base32, uppercase, with the same millisecond field |
   | `random.hex(n)` | `n` random bytes, 1 to 256, as `2n` lowercase hex characters |

   `docs/design/lua-api.md`'s "Minting ids" section is the reference, errors included.
2. **The time is an argument.** `uuid7` and `ulid` take `ts` as a decimal-nanos string, the form
   `event.timestamp` and `flush(now)` give, and reject a Lua number by the same rule an
   `event.timestamp` write does. The caller picks the time from the event or the tick; nothing
   reads an ambient clock, so the no-clock rule stands.
3. **One minter.** The byte-level generators live in `logit_core::ids`, built on
   `random_id_bytes`, with no `uuid`, `rand`, or `getrandom` dependency. `splunk_hec_out`'s
   channel uses the shared v4 and formatter. The ids carry this minter's stance: unique, not
   unpredictable. None is a secret or a capability.
4. **Each VM seeds `math.random` once at creation**, from the same minter, so two VMs draw
   different sequences. A script that calls `math.randomseed` itself still gets the sequence it
   asked for.

## Alternatives considered

- **`trace.new_id()` on the existing `trace` table.** `trace` is the node's pipeline identity
  ([ADR `trace-context-propagation-on-delivered`](trace-context-propagation-on-delivered.md)), not
  an application's. Hanging application-id minting off it would blur the line
  `docs/design/lua-api.md`'s "Reading trace context" draws between the two.
- **`random.uuid7()` reading the wall clock.** It would give a script a clock through the back
  door, and the time it stamped wouldn't be the event's own: a replayed backlog would mint ids
  sorted by replay time.
- **A Datadog decimal `u64` form.** A LuaJIT number is a double, exact only to 2^53, so the id
  would have to be a decimal string, and `trace_context`'s `format: datadog` already reads 128-bit
  hex ids (`dd.trace_id`'s 32-character form). `random.trace_id()` serves both.
- **A deterministic id hashed from a key.** A different feature: same input, same id, in every
  process. [ADR `consistent-sampling-component`](consistent-sampling-component.md) notes that a
  Lua-side hash may be worth adding later; this doesn't preclude it.
- **The `uuid` and `rand` crates.** Nothing in the runtime graph already pays for either, and v4,
  v7, and ULID are a few lines each over bytes `logit` already mints.

## Consequences

- [ADR `trace-context-span-lifting`](trace-context-span-lifting.md)'s holding that `logit` never
  mints a trace id still binds every transform. A script calling `random.trace_id()` is the
  operator's explicit choice, the posture
  [ADR `log-record-trace-context`](log-record-trace-context.md) already allows for a script's
  `event.log.trace_id` write, and it's now the sanctioned way to mint one.
- Two ids minted in one millisecond by `uuid7` or `ulid` aren't ordered between themselves: this
  isn't the ULID spec's monotonic variant.
- Any `i64` nanosecond count fits the 48-bit millisecond field, so `logit_core::ids`'s range error
  can't follow a `ts` the Lua side accepts.
- A failed `random` call is an mlua callback error, so a script that catches one with `pcall` pays
  the traceback cost `docs/known-gaps/transforms.md` records for every callback but `Event.new`.
- One allocation pin, `lua_process_one_event_minting_log_trace`
  (`docs/design/memory.md` §2), covers a `random.trace_id()` written to `event.log.trace_id`.
