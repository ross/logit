# collectd interop fixtures

Raw UDP datagrams from a real collectd's own `network` plugin, captured verbatim by
`tools/record-fixtures/raw_capture.py`, with **no parsing and no re-encoding.** Each file is exactly
the bytes collectd put on the wire, and `logit`'s own encoder never touches them. That's the point:
`crates/logit-proto/src/collectd/` was written from the protocol documentation and collectd's
`network.c`, and these fixtures check that reading against the real sender.

To regenerate, run `script/record-fixtures collectd`. See `../README.md` and the header comment in
`script/record-fixtures`.

## Fixtures

One `script/record-fixtures collectd` run makes two separate captures, one after the other:

1. **Value lists.** The first capture, against `tools/record-fixtures/collectd.conf`, produces
   `collectd-000.raw` through `collectd-002.raw`. The three differ only in what the sender packed
   into each datagram, and that packing is what makes three worth having.
2. **A notification.** The second capture has its own listener and its own `docker run`, against
   `tools/record-fixtures/collectd-threshold.conf`, and produces `collectd-notification-000.raw`.
   That config is a copy of `collectd.conf` plus the `threshold` plugin, kept as its own file so the
   first capture never loads `threshold`. Otherwise a notification would land among the three
   value-list datagrams and perturb them. The row below and `record_collectd`'s comment in
   `script/record-fixtures` explain why.

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `collectd-000.raw` (1296 bytes) | collectd 5.12.0-14 (Debian 12 "bookworm" `collectd-core` package, installed fresh into `debian:bookworm-slim` at record time; the collectd project's own Docker Hub namespace holds only `collectd/ci`, a build-environment image, not a runnable daemon) | `collectd -C /etc/collectd-fixture.conf -f` with `tools/record-fixtures/collectd.conf` (`Hostname "logit-fixture"`, `FQDNLookup false`, `Interval 1`, plugins `load`/`memory`/`interface`/`network`, `<Plugin network> Server "capture" "25826"`) | 2026-09-12 | A packed send buffer, flushed short of the 1452-byte `MaxPacketSize`. collectd flushes once the *next* list wouldn't fit, so a datagram is always under the cap. Holds 26 value lists from three plugins in one datagram, with the sender's own identity elision: one Host part for all 26 lists, 8 Plugin parts, and TimeHR written only when it changes (17 TimeHR parts). Carries all three data-source shapes at once: `load`/`load` as 3 GAUGEs, `memory`/`memory` as single-GAUGE lists differing only in `type_instance`, and `interface`/`if_octets`\|`if_packets`\|`if_errors`\|`if_dropped` as 2-DERIVE lists with the interface name in `plugin_instance` |
| `collectd-001.raw` (1318 bytes) | Same as above | Same | 2026-09-12 | The same shape one buffer later, with 26 lists. This is the **continuation** case: a read cycle's lists split across a datagram boundary, so this packet opens mid-cycle with `interface` rather than with the `load` list that starts a cycle. Identity is re-stated in full at the packet boundary and never carried across a datagram, which is why `logit_proto::collectd`'s sticky state resets per datagram |
| `collectd-002.raw` (1299 bytes) | Same as above | Same | 2026-09-12 | 23 lists, showing that the per-datagram list count is a consequence of packing, not a fixed number. Between them, the three files carry several read cycles, so a consuming test can see the same series' DERIVE counters advance across datagrams |
| `collectd-notification-000.raw` (203 bytes) | Same collectd and Debian version, from a **second, separate** `record_collectd` capture (its own listener, its own `docker run`) | `collectd -C /etc/collectd-fixture.conf -f` against `collectd-threshold.conf`: `collectd.conf` plus `threshold` (`<Plugin threshold> <Plugin "load"> <Type "load"> DataSource "shortterm" WarningMax 0.0 FailureMax 0.0`), its own file so the first capture never loads `threshold` too. W5 of `docs/plans/collectd-binary-relay.md` | 2026-09-12 | One datagram with one notification and no value lists: TimeHR, Severity, Host, Plugin, Type, and Message. It has no PluginInstance or TypeInstance, because `load` has neither. A real load average is essentially never exactly zero, so the first read breaches both `WarningMax` and `FailureMax`. collectd reports the more severe of the two, so this fixture's severity is `1` (FAILURE), not `2` (WARNING). `crates/logit-inputs/src/collectd.rs`'s `interop_fixture_notification_decodes_to_a_log_record` asserts on the decoded `LogRecord` (`Severity::Error`), `collectd.severity == 1`, `collectd.host == "logit-fixture"`, `collectd.plugin == "load"`, and the real message text collectd generated, which names the breached data source. Captured under a distinct fixture prefix so this addition never re-records or perturbs the three value-list fixtures above |

The collectd version is what `dpkg-query -W -f='${Version}' collectd-core` reported inside the
recording container. `tools/record-fixtures/collectd-entrypoint.sh` prints it on every run, so a
re-record's change to this table shows up in the script's own output.

## Tests that consume these fixtures

`crates/logit-inputs/src/collectd.rs`'s `interop_fixture_*` tests assert on the following:

- The host, the three plugins, and `collectd.interval == 1.0`.
- The wire timestamps.
- The index-vs-`types.db` naming of the `load` list.
- The DERIVE kinds of `if_octets`.
- The one-Host-part elision.
- That all three datagrams decode with **no** `bad_part`, `incomplete_identity`,
  `types_db_mismatch`, or `encrypted_packet_dropped` diagnostic. This assertion covers everything
  not named individually.

A fourth test, `interop_fixture_notification_decodes_to_a_log_record`, does the same for
`collectd-notification-000.raw`.

## What isn't covered here (yet)

- **Signed and encrypted traffic** (`SecurityLevel Sign`/`Encrypt`, the 0x0200/0x0210 parts). These
  aren't captured, and a fixture couldn't exercise much: `logit` verifies no signature and holds no
  keys (`docs/known-gaps.md`), so a signed capture would only exercise "skip a part by its length",
  and an encrypted one "drop the rest of the datagram". Hand-built unit tests in
  `crates/logit-proto/src/collectd/decode.rs` cover both, where the key material can be absent on
  purpose rather than by accident.
- **Multi-host forwarding.** This is a collectd configured with both `Listen` and `Server`,
  re-emitting another host's lists, so that one datagram holds several distinct `collectd.host`
  values. Every fixture here is a single collectd reporting its own metrics, so `Host` appears
  exactly once per datagram. The sticky-identity decoder handles a mid-datagram `Host` change, and
  unit tests cover it, but no *recorded* fixture proves that a real forwarder does it the way this
  codec expects.
- **Legacy second-resolution `Time`/`Interval` parts** (0x0001/0x0007). collectd has written the
  high-resolution 0x0008/0x0009 parts since 5.0, so a modern sender never emits the legacy pair.
  Capturing one would mean building a collectd 4.x, which isn't worth it for a path that
  `crates/logit-proto/src/collectd/decode.rs`'s own tests already cover byte for byte.
- **Multicast delivery.** The capture uses a unicast `Server "capture" "25826"`. The datagram
  payload is identical either way, so a multicast capture would record nothing new about the
  protocol. `collectd_in`'s multicast join is tested in `crates/logit-inputs/src/udp.rs` instead.
- **COUNTER and ABSOLUTE data sources.** The three plugins used here emit only GAUGE and DERIVE. A
  capture carrying the other two kinds needs a plugin that uses them (`processes` uses ABSOLUTE for
  some types). It's worth adding if one of those plugins is ever loaded here for another reason.
