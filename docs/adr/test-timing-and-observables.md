---
created: 2026-09-28
updated: 2026-09-28
---

# Test timing: wait on an observable, size every sleep, and share one set of helpers

## Status
Accepted

## Context

Six fixes for flaky tests landed in three weeks. Each one had one of three causes:

- **A sleep stood in for an event.** PR #111's `durable_buffer_restart` kill test expected 40
  batches spooled after 500 ms, and a loaded disk had 15 to 30. Commit b0417540's `docker_in`
  identity tests appended a line before the poll that refreshes identity had run. PR #358's
  `tail_in` checkpoint test slept 70 ms against a 30 ms tick, and then waited for the checkpoint
  file to exist when a tick can write offset 0 before any read.
- **A margin was too thin.** Commit 47142b7b's `otlp_in` grace-window test had 10 ms of slack on
  one side and 40 ms on the other. PR #418's Lua stall test bounded wall time over work that
  scales with CPU, and a heartbeat gap stretched it past 200 ms under load.
- **An exact assertion had no exact guarantee.** PR #111's allocation pin counted a tokio
  blocking-thread spawn. PR #413's HyperLogLog merge proptest compared an estimate whose `f32`
  sum rounds by update order.

A scan of the workspace found about 15 more tests with the same causes. Two have failed CI with
no fix. The causes recur because every test module writes its own helpers. There is one real
`wait_until` (`crates/logit-inputs/src/tail/driver.rs`) and about 12 inline deadline loops. There
are at least 12 telemetry readers with different signatures, and most drain the registry, so a
read inside a poll loop loses the points from every earlier poll. There are about 14 channel
receives with timeouts from 500 ms to 5 s. Six test modules bind a port, drop it, and hand the
number to a listener that binds it again later.

## Decision

Tests follow these rules. A reviewer can check each one on a diff.

1. **No sleep for a positive assertion.** A test waits for the observable that says the step
   happened. A sleep only sizes a negative window ("nothing arrives in this time"). Its comment
   names the interval it covers, and it is at least 3x that interval.
2. **Bind before spawn.** A test calls `bind().await`, reads `local_addr()`, then spawns the
   input. It never binds a port, drops it, and binds it again. An exception (a boxed `dyn Input`
   with no `local_addr`, a port handed to a child process) says so in a comment.
3. **A timing constant states its interval, its margin, and what each side of the margin
   protects.** Commit 47142b7b's comment is the model. A wall-clock bound over work that scales
   with CPU is at least 10x a gap measured under load, or it bounds progress instead of time.
4. **An exact assertion names the layer that makes it exact.** Otherwise it asserts the
   order-free part with equality and the rest within its real bound.
5. **Telemetry read in a poll loop goes through `TelemetryProbe`.** A test waits on the
   post-state (an offset, a count reaching N), not on an artifact existing.
6. **One positive-wait ceiling: `RECV_TIMEOUT`, 5 s.** Every wait for a batch, a close, or a
   telemetry post-state uses it.
7. **`proptest-regressions/` files are committed.** A proptest failure seen only in CI becomes
   its `cc` line in that file plus a named deterministic test built from the minimal input.
8. **Reproduce before fixing.** Run the test 20 times pinned to one core beside CPU hogs
   (`taskset` and `yes`), and record the mechanism in the fix.

The helpers live in `logit_pipeline::test_util` (`crates/logit-pipeline/src/test_util.rs`). The
module compiles for `logit-pipeline`'s own tests and, elsewhere, through a `test-util` cargo
feature that only dev-dependencies enable. `logit-inputs`, `logit-outputs`, and `logit-cli` enable
it. `script/lint` fails if `logit-cli`'s normal dependency graph enables it, the same guard as the
`fault-injection` feature
([ADR `durable-checkpoint-writes-and-fault-injection`](durable-checkpoint-writes-and-fault-injection.md)).
The module holds `wait_until`, `TelemetryProbe` and `Totals`, `fanout_channel`, `recv_batch`,
`recv_events`, `assert_no_batch`, `expect_closed`, `expect_still_open`, `spawn_input`, and
`scratch_dir`. Its module doc is the canonical account of how each behaves.

Fixtures specific to one crate stay in that crate. The TCP, TLS, and UDP collectors that sink
tests read from need `tokio-rustls` and a server TLS config, so they belong in `logit-outputs`.

## Alternatives considered

- **A copy of the helpers in each crate.** That is the current state, and the copies have
  drifted: different timeouts, different signatures, and readers that do and don't accumulate
  across drains.
- **A feature on `logit-core`.** `logit-core` has no tokio dependency, and `recv_batch` and
  `spawn_input` need `Delivered`, `Fanout`, and `Input` from `logit-pipeline`.
- **A separate dev-only crate.** It would depend on `logit-pipeline`, so `logit-pipeline`'s own
  tests would link a second copy of the crate and see a second, incompatible `Fanout`.

## Consequences

- A test that uses `TelemetryProbe` reads that registry only through the probe. A direct
  `registry.drain(0)` in the same test takes points the probe never sees, so a reviewer greps a
  converted test for it.
- Under `start_paused`, a wait on work done by a real OS thread reaches its deadline before the
  work finishes, because the paused clock jumps forward when the runtime is idle. Those tests
  keep real time.
- Existing tests move to the helpers in later changes, one crate or one cause at a time; this
  record doesn't change any of them.
- A new test module reaches for `test_util` before writing its own wait or reader.
