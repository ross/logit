# Prusa printer metrics

This example writes the metrics a Prusa printer on Buddy firmware (MK4, XL, MINI, CORE One)
sends over syslog into InfluxDB 2.x, one point per metric, each tagged with the printer it came
from. It's [`logit.yaml`](logit.yaml) and the script it loads, [`prusa.lua`](prusa.lua).

## What the printer sends

The printer sends one RFC 5424 syslog datagram over UDP about every half second. Its hostname is
the printer's MAC address, its app name is `buddy`, and it has no timestamp, because the printer
has no wall clock. The message opens with a header, `msg=N,tm=T,v=V`: a datagram sequence number,
the time since boot, and the format version. After the header come about 37 InfluxDB
line-protocol points, one per line, each timed as an offset from the time since boot:

```text
<14>1 - 10:9c:70:2d:d5:c4 buddy - - - msg=920,tm=1730850936,v=4 ttemp_noz,n=0,a=1 value=0i -158
stp_stall v=1i 38950
heap free=61088i,total=79012i 128900
esp_out sent=4171237i 676222
```

The format comes from a capture of one printer running firmware `7.0.0+16903` on 2026-10-16: 40
datagrams in 25 seconds, each 962 to 989 bytes, holding 1,470 points, about 58 points per second.
Prusa's reference collector is `utils/metrics/collect.py` in the
[Prusa-Firmware-Buddy repository](https://github.com/prusa3d/Prusa-Firmware-Buddy). The capture
holds no point with a NaN `value` and no line that fails to parse. Prusa's collector skips
`value too long` lines without a warning, which suggests some firmware emits them; this example
leaves any such line to `line_protocol`, which skips and counts it.

## The pipeline

| Node | What it does |
|---|---|
| `printer_in` | `syslog_in` on UDP port 5141 with `framing: datagram`, so one datagram is one event instead of one event per line. |
| `prusa` | `prusa.lua`: strips the header, gives each point an absolute timestamp, tags it `printer`, and names the bucket. |
| `points` | `line_protocol`: one event per point. |
| `influx_out` | `influxdb_out`: writes the points to InfluxDB. |

The printer's clock isn't synchronized, so the script anchors each datagram on its own: the point
with the newest offset gets the time `logit` received the datagram, and every other point keeps
its distance from that one. Points land within network latency of their true time, and a reboot
needs no special handling.

The script also follows Prusa's own collector: a lone field named `v` becomes `value`, a point
whose `value` is NaN is skipped, and a format-3 point tagged `_seq` is skipped. Formats 2, 3, and
4 are supported. A datagram in any other format is dropped and counted
`prusa.datagrams.dropped{reason="version"}`, because its offsets have no known unit.

## Run it

1. Create a bucket named `prusa` in InfluxDB, or the name you set as `vars.bucket` on the `prusa`
   component. InfluxDB refuses a write to a bucket that doesn't exist, and `logit` drops and
   counts that batch.
2. In `logit.yaml`, set `influx_out`'s `url` and `org` to your InfluxDB's.
3. In `logit.yaml`, list each printer's MAC address and the name to tag it with under the `prusa`
   component's `vars.names`, or remove `names` to tag every printer with its MAC address. The
   printer shows its MAC address in its network settings.
4. Start `logit` with a token that can write the bucket:

   ```sh
   INFLUXDB_TOKEN=... logit run logit.yaml
   ```

5. In the printer's metrics settings, set the metrics host to the address of the machine running
   `logit`, set the port to 5141, and allow metrics to be sent. The menu differs between printer
   models and firmware versions; Prusa's documentation for your printer has the path.

## What lands in InfluxDB

- **Measurements** are the firmware's metric names: `temp_noz`, `fan`, `heap`, `cpu_usage`, and
  so on.
- **Tags** are `printer`, plus each point's own tags, such as `n` and `a` on `temp_noz` or
  `fan` on `fan`.
- **Fields** keep their names, except that a lone `v` becomes `value`. Numbers keep their type, so
  `v=1i` is an integer `value` and `v=19.5` a float. String fields such as `fw_version`'s `value`
  are written as strings.

## Name the printers

The `printer` tag is the name `vars.names` gives the printer's MAC address, which is the
datagram's syslog hostname, read as `host.name`. The script removes `host.name` and the other
syslog attributes, so no point carries the MAC address as a tag of its own. A printer missing from `vars.names` is tagged with its MAC address,
so a new printer shows up at once, under its MAC address until you name it. Naming a printer
starts a new series for it: points written before the change keep the MAC address.

## Send to a different bucket

Change `vars.bucket` on the `prusa` component in `logit.yaml`. `influx_out` sets no `bucket:`, so
it writes each batch to the bucket the script names. The name rides with the points, so it
survives a forwarding hop too.

## Forward instead of writing InfluxDB directly

To receive on one host and write InfluxDB from another, replace `influx_out` with a `logit_out`
to a central `logit` whose `logit_in` feeds an `influxdb_out` with no `bucket:`, as
[`../forwarding/`](../forwarding) does for its own signals.

## Telemetry

The script counts through `logit`'s internal telemetry, which an `internal` component exposes:

| Counter | Meaning |
|---|---|
| `prusa.datagrams.missed{printer}` | datagrams lost between two that arrived, from a step of 2 to 9 in the sequence number |
| `prusa.datagrams.resync{printer}` | a jump of 10 or more, or a step back, in the sequence number: a reboot or firmware restart |
| `prusa.datagrams.dropped{reason="version"}` | a datagram in a format the script doesn't support, dropped with its points; nonzero means the firmware sends a format this script needs to learn |
| `prusa.points.skipped{reason}` | a point skipped as Prusa's collector skips it: `nan` or `seq` |
