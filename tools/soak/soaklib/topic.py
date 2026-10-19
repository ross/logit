"""Reads a Kafka target's topic back, and turns it into the ledger's V.

The driver runs the broker image's own tools inside the `kafka` container after the SUT stops:
`kafka-get-offsets.sh` for each partition's end offset, then `kafka-console-consumer.sh` from
the beginning for that many records. Their sum is T, the records the topic holds; a topic with no
deletes and no transactions holds one record per offset. The consumer prints one record value per
line, and `kafka_out`'s `format: json` writes each value as one object in
`crates/logit-outputs/src/ndjson.rs`'s grammar, so a value with no newline in it is one line.

V is the sum of every distinct record's `sum` metric values under the scenario's `aggregate`,
which must run `temporality: delta`: each record then carries one window's increments, and V
counts what reached the topic with no reset logic. Records are deduplicated on the value's raw
bytes before they're summed, because a resend re-encodes the same event to the same bytes, and
two distinct windows of one series never share a timestamp. Summing the copies instead would let
a duplicate cancel a loss of the same size in V. Each record, and each extra copy, is credited to
the SUT life its event `timestamp` (the window's flush time) falls in.

Under `format: otlp_proto` each record holds one `ExportMetricsServiceRequest` for every series
of a batch, so the consumer reads values through Kafka's `BytesDeserializer`, which prints a
value's bytes in an escaped form with no newline in it, and `otlp.py` walks each one. T and the
duplicate copies are counted on records' raw bytes as for JSON, because `logit.output.records`
counts records. V is the sum of the `Sum` data points' values under `prefix`, each counted once
per series and `time_unix_nano`, and each record is credited to the SUT life its latest point
time falls in. A resend whose bytes differ, a batch split another way, repeats points a distinct
record already holds: those points add nothing to V and count as point copies, credited by
their own time.
"""

import datetime
import json
import re
from typing import NamedTuple

from . import otlp, vm

TOPIC_FILE = "kafka-topic.ndjson"
OFFSETS_FILE = "kafka-offsets.txt"
BOOTSTRAP = "localhost:9092"
BIN = "/opt/kafka/bin"
# The `kafka_out` formats the ledger reads a topic in: one event per record as a JSON object, or
# one OTLP metrics export request per record in protobuf.
JSON, OTLP_PROTO = "json", "otlp_proto"
FORMATS = (JSON, OTLP_PROTO)
# What the consumer reads an `otlp_proto` value through: `Bytes.toString` escapes every byte
# outside printable ASCII, so a binary value prints on one line (`otlp.unescape`).
BYTES_DESERIALIZER = "org.apache.kafka.common.serialization.BytesDeserializer"
# The consumer's idle bound once it has read what the offsets promised, and its whole bound.
CONSUMER_TIMEOUT_MS = 30000

_PROCESSED = re.compile(r"Processed a total of (\d+) messages")
_FRACTION = re.compile(r"\.(\d+)")


def offsets_argv(topic):
    return [f"{BIN}/kafka-get-offsets.sh", "--bootstrap-server", BOOTSTRAP, "--topic", topic]


def consumer_argv(topic, max_messages=None, fmt=JSON):
    argv = [f"{BIN}/kafka-console-consumer.sh", "--bootstrap-server", BOOTSTRAP, "--topic",
            topic, "--from-beginning", "--timeout-ms", str(CONSUMER_TIMEOUT_MS)]
    if fmt == OTLP_PROTO:
        argv += ["--value-deserializer", BYTES_DESERIALIZER]
    if max_messages is not None:
        argv += ["--max-messages", str(max_messages)]
    return argv


def end_offsets(text):
    """`{partition: end offset}` from `kafka-get-offsets.sh`'s `topic:partition:offset` lines,
    or None when no line parses."""
    offsets = {}
    for line in text.splitlines():
        parts = line.strip().rsplit(":", 2)
        if len(parts) == 3 and parts[1].isdigit() and parts[2].lstrip("-").isdigit():
            offsets[int(parts[1])] = int(parts[2])
    return offsets or None


def processed_count(stderr):
    """The record count the console consumer reports on exit, or None."""
    match = _PROCESSED.search(stderr or "")
    return int(match.group(1)) if match else None


def record_lines(text):
    return [line for line in (text or "").splitlines() if line.strip()]


def parse_time(text):
    """An RFC 3339 UTC timestamp with up to nine fractional digits, as epoch seconds, or None."""
    if not isinstance(text, str):
        return None
    fraction = 0.0
    match = _FRACTION.search(text)
    if match:
        fraction = float("0." + match.group(1))
        text = text[:match.start()] + text[match.end():]
    try:
        stamp = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp.timestamp() + fraction


class TopicRead(NamedTuple):
    """The topic as the ledger reads it. `records` is one `(epoch seconds, amount)` per distinct
    record value; `unparsed` counts the values that don't decode; `duplicates` is one
    epoch-seconds timestamp per extra copy of a value already seen; `point_copies` one per data
    point a distinct `otlp_proto` record repeats."""
    records: list
    unparsed: int
    duplicates: list
    point_copies: list


def parse_records(text, prefix="", fmt=JSON):
    """The topic `text`, one record value per line, read in `fmt`: under `json`, each value
    that parses as a JSON object is a record whose amount is the sum of its `sum` metrics whose
    name starts with `prefix`; under `otlp_proto`, see `parse_otlp_records`."""
    if fmt == OTLP_PROTO:
        return parse_otlp_records(text, prefix)
    records = []
    duplicates = []
    seen = set()
    unparsed = 0
    for line in record_lines(text):
        try:
            event = json.loads(line)
        except ValueError:
            unparsed += 1
            continue
        if not isinstance(event, dict):
            unparsed += 1
            continue
        t = parse_time(event.get("timestamp"))
        if line in seen:
            duplicates.append(t)
            continue
        seen.add(line)
        amount = 0
        for metric in event.get("metrics") or []:
            if (isinstance(metric, dict) and metric.get("kind") == "sum"
                    and str(metric.get("name", "")).startswith(prefix)
                    and isinstance(metric.get("value"), (int, float))):
                amount += metric["value"]
        records.append((t, amount))
    return TopicRead(records, unparsed, duplicates, [])


def parse_otlp_records(text, prefix=""):
    """The topic read under `otlp_proto`: each line unescaped (`otlp.unescape`) and walked as an
    `ExportMetricsServiceRequest`. A record's amount is the sum of its `Sum` points under
    `prefix` not already counted for their series and `time_unix_nano`, and its time is its
    latest point's, or None with no point. A line that doesn't unescape or decode is unparsed."""
    records = []
    duplicates = []
    point_copies = []
    seen = set()
    counted = set()
    unparsed = 0
    for line in record_lines(text):
        try:
            points = otlp.metric_points(otlp.unescape(line))
        except otlp.ProtoError:
            unparsed += 1
            continue
        times = [p.time_unix_nano for p in points if p.time_unix_nano]
        t = max(times) / 1e9 if times else None
        if line in seen:
            duplicates.append(t)
            continue
        seen.add(line)
        amount = 0
        for point in points:
            if point.kind != otlp.SUM or not point.name.startswith(prefix):
                continue
            key = (point.series, point.time_unix_nano)
            if key in counted:
                point_copies.append(point.time_unix_nano / 1e9 if point.time_unix_nano
                                    else None)
                continue
            counted.add(key)
            amount += point.value
        records.append((t, amount))
    return TopicRead(records, unparsed, duplicates, point_copies)


def duplicates_by_life(duplicates, life_starts):
    """`{life: extra copies}`, each copy credited as `totals_by_life` credits a record."""
    by_life = {}
    for t in duplicates:
        life = 0 if t is None else vm.life_at(life_starts, t)
        by_life[life] = by_life.get(life, 0) + 1
    return by_life


def totals_by_life(records, life_starts):
    """`({life: amount}, total)`: each record's amount credited to the SUT life its timestamp
    falls in, a record with no timestamp to the first."""
    by_life = {}
    total = 0
    for t, amount in records:
        life = 0 if t is None else vm.life_at(life_starts, t)
        by_life[life] = by_life.get(life, 0) + amount
        total += amount
    return by_life, total
