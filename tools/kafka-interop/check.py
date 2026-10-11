#!/usr/bin/env python3
"""Judges each leg of `script/kafka-interop` from the files the run left behind.

Runs as `python3 -I /tools/check.py` in a throwaway `python:3.12-slim` container, stdlib only,
after the legs have exited and the script has read every topic back. It queries nothing: the
script has already written, under /out (the run directory):

    readback/<broker>/<topic>.java.tsv  kafka-console-consumer.sh with the timestamp, partition,
                                        offset, and key printed: tab-separated
                                        `CreateTime:<ms>`, `Partition:<p>`, `Offset:<o>`, key,
                                        value
    readback/<broker>/<topic>.kcat.tsv  kcat -f '%p\\t%o\\t%T\\t%k\\t%s\\n'
    readback/<broker>/<topic>.offsets   kafka-get-offsets.sh: `<topic>:<partition>:<end offset>`
    <leg>-<broker>-telemetry.log        each leg's `internal` telemetry, `stdio_out` `format: json`
    exits.txt                           `<service> <exit code>` per leg

Prints one row per leg and broker (docs/plans/kafka-output.md, "Interop plan (W4)"). A row is
PASS (the leg did what the plan expects), GAP (the broker behaves differently from Apache Kafka in
a way docs/known-gaps/kafka.md records), or FAIL. Exits 1 on any FAIL.
"""

import json
import pathlib
import sys

OUT = pathlib.Path("/out")
BROKERS = ("kafka", "redpanda")
CODECS = ("none", "gzip", "snappy", "lz4", "zstd")

PARITY_EVENTS = 200
PARITY_KEYS = 50
CODEC_EVENTS = 100
DEDUP_EVENTS = 800
TLS_EVENTS = 50
TIMESTAMP_EVENTS = 100
# `logit-timestamp-*.yaml` stamps seq 3, 13, ..., 93 at 2100-01-01.
FUTURE_SEQS = {seq for seq in range(TIMESTAMP_EVENTS) if seq % 10 == 3}


# ---- read-back ---------------------------------------------------------------------------------


class Record:
    def __init__(self, partition, offset, timestamp_ms, key, value):
        self.partition = partition
        self.offset = offset
        self.timestamp_ms = timestamp_ms
        self.key = key
        self.value = value

    def event(self):
        """The record's value as the NDJSON object `kafka_out`'s `format: json` writes."""
        return json.loads(self.value)

    def seq(self):
        return int(self.event()["attributes"]["seq"])


def read_java(broker, topic):
    path = OUT / "readback" / broker / f"{topic}.java.tsv"
    records = []
    if not path.exists():
        return records
    for line in path.read_text(errors="replace").splitlines():
        if not line.strip():
            continue
        fields = line.split("\t")
        meta = {}
        while fields and ":" in fields[0] and fields[0].split(":", 1)[0] in (
                "CreateTime", "LogAppendTime", "NoTimestampType", "Partition", "Offset"):
            name, value = fields.pop(0).split(":", 1)
            meta[name] = value
        # The key and the value, which may itself hold a tab: kafka_out's JSON escapes one.
        key, value = (fields[0], "\t".join(fields[1:])) if len(fields) >= 2 else ("", "")
        records.append(Record(int(meta.get("Partition", -1)), int(meta.get("Offset", -1)),
                              int(meta.get("CreateTime", meta.get("LogAppendTime", -1))),
                              key, value))
    return records


def read_kcat(broker, topic):
    path = OUT / "readback" / broker / f"{topic}.kcat.tsv"
    records = []
    if not path.exists():
        return records
    for line in path.read_text(errors="replace").splitlines():
        if not line.strip():
            continue
        partition, offset, timestamp, key, value = line.split("\t", 4)
        records.append(Record(int(partition), int(offset), int(timestamp), key, value))
    return records


def end_offsets(broker, topic):
    """The sum of the topic's end offsets: how many records the broker holds, control records
    included (`kafka_out` writes none)."""
    path = OUT / "readback" / broker / f"{topic}.offsets"
    if not path.exists():
        return None
    total = 0
    for line in path.read_text().splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].isdigit():
            total += int(parts[2])
    return total


# ---- telemetry ---------------------------------------------------------------------------------


def telemetry(leg, broker):
    path = OUT / f"{leg}-{broker}-telemetry.log"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(errors="replace").splitlines()
            if line.strip()]


def telemetry_sum(events, metric, **attrs):
    total = 0.0
    for event in events:
        have = {**event.get("resource", {}).get("attributes", {}), **event.get("attributes", {})}
        if any(have.get(k) != v for k, v in attrs.items()):
            continue
        for item in event.get("metrics", []):
            if item.get("name") == metric and item.get("kind") == "sum":
                total += float(item["value"])
    return int(total)


def requests_by_class(events, component):
    classes = {}
    for cls in ("ok", "clean", "ambiguous", "rejected", "refused"):
        n = telemetry_sum(events, "logit.output.requests", component=component, **{"class": cls})
        if n:
            classes[cls] = n
    return classes


def shown(classes):
    return ", ".join(f"{cls} {n}" for cls, n in classes.items()) or "none"


def exits():
    path = OUT / "exits.txt"
    if not path.exists():
        return {}
    return dict(line.split() for line in path.read_text().splitlines() if line.strip())


# ---- legs --------------------------------------------------------------------------------------


def count_and_seqs(records):
    seqs = [r.seq() for r in records]
    return len(seqs), set(seqs)


def leg_parity(broker):
    logit = read_java(broker, "parity-logit")
    java = read_java(broker, "parity-java")
    if len(logit) != PARITY_EVENTS:
        return "FAIL", f"parity-logit holds {len(logit)} records, want {PARITY_EVENTS}"
    if len(java) != PARITY_KEYS:
        return "FAIL", f"parity-java holds {len(java)} records, want {PARITY_KEYS}"
    logit_parts = {}
    for record in logit:
        logit_parts.setdefault(record.key, set()).add(record.partition)
    java_parts = {record.key: record.partition for record in java}
    split = sorted(key for key, parts in logit_parts.items() if len(parts) != 1)
    if split:
        return "FAIL", f"kafka_out put one key on several partitions: {split[:5]}"
    differ = sorted(key for key in java_parts
                    if logit_parts.get(key) != {java_parts[key]})
    if differ:
        detail = ", ".join(f"{k}: logit {sorted(logit_parts.get(k, []))} java {java_parts[k]}"
                           for k in differ[:5])
        return "FAIL", f"{len(differ)} of {PARITY_KEYS} keys on a different partition ({detail})"
    used = len(set(java_parts.values()))
    return "PASS", (f"all {PARITY_KEYS} keys on the Java producer's partition, {used} of 6 "
                    f"partitions used; {len(logit)} kafka_out records")


def leg_codec(broker, codec):
    topic = f"codec-{codec}"
    held = end_offsets(broker, topic)
    results = []
    for reader, records in (("Java", read_java(broker, topic)), ("kcat", read_kcat(broker, topic))):
        try:
            n, seqs = count_and_seqs(records)
        except (ValueError, KeyError, json.JSONDecodeError) as err:
            return "FAIL", f"{reader} read a record that isn't kafka_out's JSON: {err}"
        if n != CODEC_EVENTS or seqs != set(range(CODEC_EVENTS)):
            return "FAIL", (f"{reader} read {n} records ({len(seqs)} distinct), want "
                            f"{CODEC_EVENTS}; end offsets {held}")
        results.append(f"{reader} {n}")
    return "PASS", f"{', '.join(results)} records, each event once; end offsets {held}"


def leg_dedup(broker):
    events = telemetry("dedup", broker)
    idem = requests_by_class(events, "idempotent")
    control = requests_by_class(events, "control")
    records = read_java(broker, "dedup")
    try:
        n, seqs = count_and_seqs(records)
        control_n, control_seqs = count_and_seqs(read_java(broker, "dedup-control"))
    except (ValueError, KeyError, json.JSONDecodeError) as err:
        return "FAIL", f"a dedup record isn't kafka_out's JSON: {err}"
    control_text = (f"control (idempotent: false) stored {control_n} records for "
                    f"{len(control_seqs)} events, {control_n - len(control_seqs)} duplicate(s), "
                    f"requests {shown(control)}")
    detail = (f"dedup holds {n} records for {len(seqs)} events of {DEDUP_EVENTS}; idempotent "
              f"requests {shown(idem)}; {control_text}")
    if not idem.get("ambiguous"):
        return "FAIL", f"no Ambiguous attempt, so nothing was resent and the leg proves nothing; {detail}"
    if seqs != set(range(DEDUP_EVENTS)):
        return "FAIL", f"{DEDUP_EVENTS - len(seqs)} event(s) missing; {detail}"
    if n != DEDUP_EVENTS:
        return "FAIL", f"{n - DEDUP_EVENTS} duplicate(s) despite idempotence; {detail}"
    return "PASS", detail


def leg_tls(broker, topic, component):
    records = read_java(broker, topic)
    classes = requests_by_class(telemetry("tls", broker), component)
    if len(records) != TLS_EVENTS:
        return "FAIL", f"{topic} holds {len(records)} records, want {TLS_EVENTS}; requests {shown(classes)}"
    return "PASS", f"{len(records)} records over TLS; requests {shown(classes)}"


def leg_mtls_required(broker):
    records = read_java(broker, "mtls-nocert")
    classes = requests_by_class(telemetry("tls", broker), "mtls_nocert")
    if records or classes.get("ok"):
        return "FAIL", (f"the mutual-TLS listener took a client without a certificate: "
                        f"{len(records)} records, requests {shown(classes)}")
    if not classes.get("clean"):
        return "FAIL", f"no attempt was made, so the refusal is unproven; requests {shown(classes)}"
    return "PASS", f"refused without a client certificate: 0 records, requests {shown(classes)}"


def leg_timestamp(broker):
    events = telemetry("timestamp", broker)
    dropped = telemetry_sum(events, "logit.output.records.dropped", component="timestamp",
                            reason="invalid_timestamp")
    classes = requests_by_class(events, "timestamp")
    try:
        n, seqs = count_and_seqs(read_java(broker, "timestamp"))
    except (ValueError, KeyError, json.JSONDecodeError) as err:
        return "FAIL", f"a timestamp record isn't kafka_out's JSON: {err}"
    want = set(range(TIMESTAMP_EVENTS)) - FUTURE_SEQS
    detail = (f"{n} records landed, {len(FUTURE_SEQS)} stamped at 2100-01-01, "
              f"records.dropped{{reason=invalid_timestamp}} {dropped}; requests {shown(classes)}")
    if seqs == want and n == len(want) and dropped == len(FUTURE_SEQS):
        return "PASS", detail
    rejected = telemetry_sum(events, "logit.output.records.dropped", component="timestamp",
                             reason="rejected")
    if n == 0 and dropped == 0 and rejected == TIMESTAMP_EVENTS:
        # docs/known-gaps/kafka.md: a broker that names no record in `record_errors` gets the
        # whole partition batch rejected, counted, under ADR `kafka-output` item 5.
        return "GAP", (f"the broker answered INVALID_TIMESTAMP naming no record, so every "
                       f"batch was rejected whole: records.dropped{{reason=rejected}} {rejected}; "
                       f"{detail}")
    if seqs == set(range(TIMESTAMP_EVENTS)) and dropped == 0:
        return "GAP", f"the broker stored the future-stamped records; {detail}"
    return "FAIL", detail


LEGS = [("parity", "partition parity with kafka-console-producer.sh", leg_parity)]
LEGS += [(f"codec-{c}", f"compression: {c}, read by Java and kcat",
          lambda b, c=c: leg_codec(b, c)) for c in CODECS]
LEGS += [
    ("dedup", "idempotent resend under netem response loss", leg_dedup),
    ("tls", "server TLS, private CA", lambda b: leg_tls(b, "tls", "tls")),
    ("mtls", "mutual TLS, client certificate", lambda b: leg_tls(b, "mtls", "mtls")),
    ("mtls-required", "mutual TLS refuses a client without a certificate", leg_mtls_required),
    ("timestamp", "a record over an hour ahead dropped and counted", leg_timestamp),
]


# ---- main --------------------------------------------------------------------------------------


def main():
    codes = exits()
    results = []
    for broker in BROKERS:
        for name, title, check in LEGS:
            try:
                result, detail = check(broker)
            except Exception as err:  # A malformed read-back is a failed leg, not a crashed check.
                result, detail = "FAIL", f"{type(err).__name__}: {err}"
            results.append({"leg": name, "broker": broker, "title": title, "result": result,
                            "detail": detail})
    bad_exits = {leg: code for leg, code in codes.items() if code != "0"}
    if bad_exits:
        results.append({"leg": "exits", "broker": "-", "title": "every leg's process exited 0",
                        "result": "FAIL", "detail": str(bad_exits)})

    lines = ["| Leg | Broker | What | Result | Detail |", "|---|---|---|---|---|"]
    for row in results:
        lines.append(f"| {row['leg']} | {row['broker']} | {row['title']} | {row['result']} | "
                     f"{row['detail']} |")
    table = "\n".join(lines)
    print(table)
    (OUT / "results.md").write_text(table + "\n")
    (OUT / "results.json").write_text(json.dumps({"legs": results}, indent=2) + "\n")
    return 1 if any(row["result"] == "FAIL" for row in results) else 0


if __name__ == "__main__":
    sys.exit(main())
