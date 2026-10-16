---
created: 2026-10-16
updated: 2026-10-16
---

# Enabling plan: Prusa Buddy printer metrics over syslog, through generic pieces

## Status

W0 records the decisions: three ADRs and this plan. W1 to W4 build them; W4 ships the example,
and "Outcome" below says what its test proves.

## Goal

Receive a Prusa printer's metrics datagrams on UDP and deliver every point to InfluxDB with an
absolute timestamp, its measurement, tags, and fields as Prusa's own collector writes them, and the
printer's MAC address as a tag. The first deployment is whale's `logit`, forwarding through
`logit_out` to xormedia, whose `influxdb_out` writes a `prusa` bucket.

Non-goals:

- A Prusa-specific input kind. Every Prusa-specific rule lives in a Lua script that ships as an
  example.
- Session anchoring, Prusa's `udp_datagram` and `metric_error` points, and the session tags its
  collector adds (`hostname`, `port`, `session_start_time`). The script can add the first two
  later; the third is per-run cardinality nobody needs.
- A lossless relay pair. `syslog_in` with `line_protocol` changes nothing in
  [ADR `lossless-transit`](../adr/lossless-transit.md).

The stream key is `prusa`.

## Evidence

The capture was taken on 2026-10-16 from one printer running firmware `7.0.0+16903`: 40 datagrams
in 25 seconds, each 962–989 bytes, holding 1,470 points between them, about 58 points per second.
One datagram becomes the W4 fixture under `crates/logit-cli/tests/fixtures/`. Prusa's reference
collector is `utils/metrics/collect.py` in the
[Prusa-Firmware-Buddy repository](https://github.com/prusa3d/Prusa-Firmware-Buddy).

Each datagram is one RFC 5424 message:

```text
<14>1 - 10:9c:70:2d:d5:c4 buddy - - - msg=920,tm=1730850936,v=4 ttemp_noz,n=0,a=1 value=0i -158
stp_stall v=1i 38950
heap free=61088i,total=79012i 128900
fan,fan=print state=0,pwm=0,measured=0 502646
...
esp_out sent=4171237i 676222
```

- **Syslog header.** TIMESTAMP is nil, because the printer has no wall clock. HOSTNAME is the MAC
  address and APP-NAME is `buddy`.
- **Message header.** The MSG opens with `msg=N,tm=T,v=V` and a space, then the first point on the
  same line. `msg` is a datagram sequence number, `tm` is the time since boot, and `v` is the
  format version.
- **Points.** One InfluxDB line-protocol point per line, the datagram ending in `\n`. Fields are
  floats, `i` integers, or strings (`fw_version v="7.0.0+16903"`), and some lines carry several.
- **Point timestamps.** An offset from `tm` in the same unit, possibly negative. The unit is
  microseconds in `v=4` and milliseconds in `v=2` and `v=3`. In `v=2` each offset is relative to
  the previous point instead.
- **Prusa's collector** renames a lone `v` field to `value`, skips a NaN `value`, skips a `v=3`
  point tagged `_seq`, tags each point `mac_address`, and skips a line it can't parse.

## The three gaps

1. **`syslog_in` over UDP splits a datagram on `\n`** (`SyslogDecoder::decode_into`,
   `crates/logit-proto/src/syslog/decode.rs`). Only the first line starts with `<PRI>`, so about
   36 of every 37 points are counted `logit.input.messages.dropped{reason="malformed"}`.
2. **Nothing parses line protocol out of a log message,** and no native transform can turn one
   event into several.
3. **Point times are offsets from a boot-relative `tm`,** and the syslog TIMESTAMP is nil, so
   there's no absolute time in the datagram.

## Decisions settled

| # | Decision | Record |
|---|---|---|
| 1 | `syslog_in` gains `framing: lines \| datagram` for UDP, default `lines`; any value under `transport: tcp` is a config error. `datagram` reuses the TCP arm's decoder, `with_line_splitting(false)`. | [ADR `syslog-udp-datagram-framing`](../adr/syslog-udp-datagram-framing.md) |
| 2 | `Transform::process_into` lets a native transform emit extra events, appended to the same batch after its surviving in-place events, in emission order. No new counter, no cap, no route mark, and no allocation on the common path. | [ADR `transform-fan-out`](../adr/transform-fan-out.md) |
| 3 | A `line_protocol` transform parses `event.log.message` into one event per point in `influxdb_in`'s shape, through a construction path shared with `influxdb_in`. `precision:` and `keep_source:` options. A bad line is skipped and counted; a message with no good point forwards its parent untouched. | [ADR `line-protocol-transform`](../adr/line-protocol-transform.md) |
| 4 | Everything Prusa-specific lives in `examples/prusa/prusa.lua`, placed before `line_protocol`, with per-datagram anchoring. | this plan |

The framing decision is its own ADR rather than a section of the `line_protocol` one, because it
stands alone: any sender of a multi-line or binary MSG over UDP needs it, the syslog known gaps
cite it, and AGENTS.md's `syslog_in` row points at it.

## Pipeline

```yaml
components:
  printer_in:
    type: syslog_in
    bind: 0.0.0.0:5141
    framing: datagram       # one event per datagram, body = header + ~37 lines
  prusa:
    type: lua_file
    sources: [printer_in]
    lua_file: prusa.lua     # header, anchoring, units, v -> value, NaN, tags
  points:
    type: line_protocol
    sources: [prusa]        # one event per point, in influxdb_in's shape
  xormedia:
    type: logit_out
    sources: [points]
    endpoint: xormedia:5150
```

The script runs before `line_protocol` so it sees the whole datagram as one body. It rewrites that
body and returns one event; it constructs no events.

## The Lua anchoring rule

**Per datagram.** The datagram's newest point maps to the event's `timestamp`, which `syslog_in`
sets to the receipt time. Every other point keeps its offset from the newest:

```text
point_ns = event.timestamp - (newest_offset - offset) * unit_ns
```

`newest_offset` is the largest offset in the datagram, or the last running sum under `v=2`. `tm`
cancels out, so the script reads it only to validate the header. A point lands within network
latency and the firmware's send delay of its true time.

Prusa's collector anchors per session instead: on the first datagram it records `now - tm` and
adds `tm + offset` to it for every later point, re-anchoring when `msg` jumps by 10 or more, which a
reboot causes. That keeps points evenly spaced, but it drifts as the printer's clock wanders,
needs state per printer, and needs the jump heuristic to survive a reboot. Per-datagram anchoring
is stateless, doesn't drift, and survives a reboot with no special case.

The script also:

- Parses and strips the `msg=N,tm=T,v=V` header. A datagram with an unknown `v` is dropped and
  counted `prusa.datagrams.dropped{reason="version"}`, because its offsets have no known unit and
  `line_protocol` would parse its header as a point. A message with no header, or from an app other
  than `buddy`, passes through unchanged.
- Rewrites each line's trailing offset to absolute nanoseconds, in `v=4` (µs), `v=3` (ms), and
  `v=2` (ms, cumulative).
- Renames a lone `v` field to `value`, as Prusa's collector does.
- Drops a line whose `value` is NaN, counted through `telemetry`. Line protocol has no NaN, so
  `line_protocol` would otherwise count the line as loss.
- Drops a `v=3` line tagged `_seq`, as Prusa's collector does.
- Sets `mac_address` from `syslog.hostname` and removes the other `syslog.*` attributes, so the
  points carry the tags Prusa's collector writes.
- Counts gaps in `msg` per printer through `telemetry`, tagged `mac_address`, from a Lua table
  keyed by hostname. That table is the script's only state, and only the gap count depends on it.
  The tag's cardinality is the number of printers.

## Workstreams

| # | Branch | PR | Depends |
|---|---|---|---|
| W0 | `prusa/w0` | **Docs.** [ADR `syslog-udp-datagram-framing`](../adr/syslog-udp-datagram-framing.md), [ADR `transform-fan-out`](../adr/transform-fan-out.md), [ADR `line-protocol-transform`](../adr/line-protocol-transform.md), and their rows in `docs/adr/README.md`; this plan and its row in `docs/plans/README.md`; a pointer amendment to [ADR `in-place-transform-process`](../adr/in-place-transform-process.md) | — |
| W1 | `prusa/w1` | **`syslog_in` framing.** `framing:` in `ComponentKind::SyslogIn`, the graph rule, the UDP arm's decoder choice, `script/schema`; tests for a multi-line MSG, a binary MSG, the default split, and the TCP rejection; `docs/known-gaps/syslog.md`'s binary-MSG entry gains the workaround; AGENTS.md's `syslog_in` row | W0 |
| W2 | `prusa/w2` | **Transform emit path.** `Transform::process_into`, the scratch in `run_transform` and `process_batch`; tests for order, children-only batches, discard counting, and unchanged allocation pins; `transform.rs`'s module doc and `pipeline-graph.md` | W1 |
| W3 | `prusa/w3` | **`line_protocol`.** The shared construction path in `logit_proto::influxdb`, with `influxdb_in`'s tests unchanged; the transform, its config, registry arm, and schema; the fuzz target's skip mode; `internal-telemetry.md`'s counters; AGENTS.md's transforms table | W2 |
| W4 | `prusa/w4` | **Prusa example.** `examples/prusa/` (config, `prusa.lua`, rendered graph, its README as the operator docs, `examples/README.md` row); two captured datagrams as fixtures; `crates/logit-cli/tests/prusa_example.rs`, which runs them through `syslog_in`'s decoder with `framing: datagram`, the script, and `line_protocol` and asserts the points; the whale deployment config in the PR body | W3 |

W2 doesn't need W1's code, but the stream lands linearly so each PR reviews against the one before
it. Each branch is cut from its parent's branch, its PR targets that branch, and it retargets to
`main` when the parent merges. A branch is brought up to date with `git merge`, never a rebase.

## Mechanics

- Each PR gets `pr-review N --comment` once it's open, and its findings are addressed before the
  next workstream stacks on it. A re-review checks each fix as a delta against that review.
- Before opening each PR, run the sweep in AGENTS.md's "Workflow" section: prose the diff
  contradicts, every row of any list the change touches, a test that can't fail, banned words in
  comments, and a decision recorded only in a module doc.
- Each regression test is run once with its fix reverted, and the PR body says so.

## Verification

Every PR runs `script/cibuild`. W1 and W3 regenerate and commit `script/schema`.

- **W0**: every relative link resolves (`script/test -p logit-cli doc_links`).
- **W1**: decoder tests for `framing: datagram` (a multi-line MSG, a `0x0A` in a binary MSG, one
  trailing `\n` and `\r\n` stripped) and the unchanged default; a socket test through
  `logit_pipeline::test_util`; the graph rule's accept and reject cases.
- **W2**: `process_batch` with a test transform that emits children, discards parents, and
  forwards some, asserting order and counters; a fully emitting batch; every allocation pin
  unchanged; `script/bench` for `process_batch` through `keep` before and after, numbers in the PR.
- **W3**: transform tests for every row of the decode table, inheritance and collision, precision,
  a timestamp-less point, merging, each bad-line counter and the parent's fate under both
  `keep_source` values, a non-log event, and a body with no point; `influxdb_in`'s existing suite
  passing unchanged on the shared path; `script/unsafe-check fuzz influxdb_line`.
- **W4**: the end-to-end test asserts every point of the fixture datagram: measurement, tags
  including `mac_address`, typed fields, `v` renamed to `value`, and timestamps anchored to the
  receipt time. `script/validate` covers the example config.

## Open questions

- **Where the whale config lives.** W4 assumes it's outside this repository and ships only the
  example; the PR body carries the deployment snippet.
- **Firmware lines that don't parse.** Prusa's collector skips `value too long` lines without a
  warning, which suggests the firmware emits them. The capture has none, and none with a NaN. W4
  leaves such lines to `line_protocol`'s `lines.dropped`. If a longer run shows them, the script
  drops and counts them itself, so `*.dropped` keeps meaning loss someone should look into.

## Outcome

W4's `crates/logit-cli/tests/prusa_example.rs` runs the first captured datagram (`msg=920`, 984
bytes) through `SyslogDecoder::with_line_splitting(false)`, `examples/prusa/prusa.lua` in a real
`ScriptWorker`, and `LineProtocol` through `process_batch`, and checks every point against the
fixture's own lines:

- 37 points in line order and no parent, none skipped or counted as a bad line.
- Each point's measurement, `mac_address` set, and no `syslog.*` attribute left.
- Each point's timestamp is the receipt time minus its distance from the newest offset, so
  `esp_out` takes the receipt time and `ttemp_noz` (offset `-158`) lands 676,380 µs before it.
- `v` renamed to `value` (`stp_stall.value` is the integer `1`), tags (`n`, `a`, `fan`), and typed
  multi-field points (`heap.free`, `heap.total`, three `fan.*` gauges per fan).
- The batch resource carries `_logit.influxdb.bucket = "prusa"` past `line_protocol`.

The same file checks that consecutive datagrams `msg=920` and `msg=921` count no gap, that a jump
to `msg=925` counts four missed datagrams, that a datagram in an unsupported format version is
discarded and counted `prusa.datagrams.dropped`, and that a non-Buddy syslog event passes through unchanged. With the anchoring
subtraction removed from the script, the first test fails on its first point.
