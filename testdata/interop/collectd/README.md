# collectd interop fixtures

Raw UDP datagrams from a real collectd's own `network` plugin, captured verbatim by
`tools/record-fixtures/raw_capture.py` -- **no parsing, no re-encoding.** Each file is exactly the
bytes collectd put on the wire; `logit`'s own encoder never touches these, which is the whole point:
`crates/logit-proto/src/collectd/` was written from the protocol documentation and collectd's
`network.c`, and these fixtures are what checks that reading against the real sender. Regenerate
with `script/record-fixtures collectd` (see `../README.md` and `script/record-fixtures`'s own header
comment).

All three came out of a single `script/record-fixtures collectd` run against one
`tools/record-fixtures/collectd.conf`, so the table rows differ only in what the sender happened to
pack into each datagram -- which is itself the thing worth having three of.

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `collectd-000.raw` (1296 bytes) | collectd 5.12.0-14 (Debian 12 "bookworm" `collectd-core` package, installed fresh into `debian:bookworm-slim` at record time -- the collectd project's own Docker Hub namespace holds only `collectd/ci`, a build-environment image, not a runnable daemon) | `collectd -C /etc/collectd-fixture.conf -f` with `tools/record-fixtures/collectd.conf` (`Hostname "logit-fixture"`, `FQDNLookup false`, `Interval 1`, plugins `load`/`memory`/`interface`/`network`, `<Plugin network> Server "capture" "25826"`) | 2026-09-12 | A packed send buffer, flushed short of the 1452-byte `MaxPacketSize` (collectd flushes once the *next* list would not fit, so a datagram is always under the cap): 26 value lists from three plugins in one datagram, with the sender's own identity elision (one Host part for all 26 lists, 8 Plugin parts, TimeHR written only when it changes). Carries all three data-source shapes at once -- `load`/`load` as 3 GAUGEs, `memory`/`memory` as single-GAUGE lists differing only in `type_instance`, `interface`/`if_octets`\|`if_packets`\|`if_errors`\|`if_dropped` as 2-DERIVE lists with the interface name in `plugin_instance` |
| `collectd-001.raw` (1318 bytes) | Same as above | Same | 2026-09-12 | The same shape one buffer later: 26 lists, and the **continuation** case -- a read cycle's lists split across a datagram boundary, so this packet opens mid-cycle with `interface` rather than with the `load` list that starts a cycle. Identity is re-stated in full at the packet boundary, never carried across a datagram (`logit_proto::collectd`'s sticky state resets per datagram for exactly this reason) |
| `collectd-002.raw` (1299 bytes) | Same as above | Same | 2026-09-12 | 23 lists -- the same again, showing the per-datagram list count is a consequence of packing, not a fixed number. Between them the three files carry several read cycles, so a consuming test can see the same series' DERIVE counters advance across datagrams |

The collectd version is what `dpkg-query -W -f='${Version}' collectd-core` reported inside the
recording container; `tools/record-fixtures/collectd-entrypoint.sh` prints it on every run, so a
re-record's diff to this table is visible in the script's own output.

`crates/logit-inputs/src/collectd.rs`'s `interop_fixture_*` tests consume these: the host, the three
plugins, `collectd.interval == 1.0`, the wire timestamps, the index-vs-`types.db` naming of the
`load` list, the DERIVE kinds of `if_octets`, the one-Host-part elision, and -- the assertion that
covers everything not named individually -- that all three datagrams decode with **no** `bad_part`,
`incomplete_identity`, `types_db_mismatch` or `encrypted_packet_dropped` diagnostic.

## What isn't covered here (yet)

- **Notifications** (`Message` 0x0100 / `Severity` 0x0101 parts). No fixture: the `network` plugin
  only emits them for a notification-generating plugin such as `threshold`, which this config does
  not load, and the decoder skips both part types until W5 of
  `docs/plans/collectd-binary-relay.md`. That workstream adds a `threshold`-plugin capture here.
- **Signed and encrypted traffic** (`SecurityLevel Sign`/`Encrypt`, the 0x0200/0x0210 parts). Not
  captured, and not implementable against as a fixture either: `logit` verifies no signature and
  holds no keys (`docs/known-gaps.md`), so a signed capture would only exercise "skip a part by its
  length" and an encrypted one "drop the rest of the datagram". Both are covered by hand-built unit
  tests in `crates/logit-proto/src/collectd/decode.rs`, where the key material can be absent on
  purpose rather than by accident.
- **Multi-host forwarding** -- a collectd configured with both `Listen` and `Server`, re-emitting
  another host's lists, i.e. several distinct `collectd.host` values in one datagram. Every fixture
  here is a single collectd reporting its own metrics, so `Host` appears exactly once per datagram.
  The sticky-identity decoder handles a mid-datagram `Host` change (there are unit tests for it),
  but no *recorded* fixture proves a real forwarder does it the way this codec expects.
- **Legacy second-resolution `Time`/`Interval` parts** (0x0001/0x0007). collectd has written the
  high-resolution 0x0008/0x0009 parts since 5.0, so a modern sender never emits the legacy pair --
  capturing one would mean building a collectd 4.x, which is not worth it for a path
  `crates/logit-proto/src/collectd/decode.rs`'s own tests already cover byte for byte.
- **Multicast delivery.** The capture uses a unicast `Server "capture" "25826"`; the datagram
  payload is identical either way, so a multicast capture would record nothing new about the
  protocol. `collectd_in`'s multicast join is tested in `crates/logit-inputs/src/udp.rs` instead.
- **COUNTER and ABSOLUTE data sources.** These three plugins emit only GAUGE and DERIVE. A capture
  carrying the other two kinds would need a plugin that uses them (`processes` uses ABSOLUTE for
  some types); worth adding if one of those plugins is ever loaded here for another reason.
