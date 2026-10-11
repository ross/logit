"""Reads a Kafka target's topic back, and turns it into the ledger's V.

The driver runs the broker image's own tools inside the `kafka` container after the SUT stops:
`kafka-get-offsets.sh` for each partition's end offset, then `kafka-console-consumer.sh` from
the beginning for that many records. Their sum is T, the records the topic holds; a topic with no
deletes and no transactions holds one record per offset. The consumer prints one record value per
line, and `kafka_out`'s `format: json` writes each value as one object in
`crates/logit-outputs/src/ndjson.rs`'s grammar, so a value with no newline in it is one line.

V is the sum of every record's `sum` metric values under the scenario's `aggregate`, which must
run `temporality: delta`: each record then carries one window's increments, and V counts what
reached the topic with no reset logic. A duplicated record adds its increments twice, so V above
Ab is a duplicate, not a reset. Each record is credited to the SUT life its event `timestamp`
(the window's flush time) falls in.
"""

import datetime
import json
import re

from . import vm

TOPIC_FILE = "kafka-topic.ndjson"
OFFSETS_FILE = "kafka-offsets.txt"
BOOTSTRAP = "localhost:9092"
BIN = "/opt/kafka/bin"
# The consumer's idle bound once it has read what the offsets promised, and its whole bound.
CONSUMER_TIMEOUT_MS = 30000

_PROCESSED = re.compile(r"Processed a total of (\d+) messages")
_FRACTION = re.compile(r"\.(\d+)")


def offsets_argv(topic):
    return [f"{BIN}/kafka-get-offsets.sh", "--bootstrap-server", BOOTSTRAP, "--topic", topic]


def consumer_argv(topic, max_messages=None):
    argv = [f"{BIN}/kafka-console-consumer.sh", "--bootstrap-server", BOOTSTRAP, "--topic",
            topic, "--from-beginning", "--timeout-ms", str(CONSUMER_TIMEOUT_MS)]
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


def parse_records(text, prefix=""):
    """The topic read as `(records, unparsed)`: `records` is one `(epoch seconds, amount)` per
    record value that parses as a JSON object, `amount` the sum of its `sum` metrics whose name
    starts with `prefix`; `unparsed` counts the lines that don't."""
    records = []
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
        amount = 0
        for metric in event.get("metrics") or []:
            if (isinstance(metric, dict) and metric.get("kind") == "sum"
                    and str(metric.get("name", "")).startswith(prefix)
                    and isinstance(metric.get("value"), (int, float))):
                amount += metric["value"]
        records.append((parse_time(event.get("timestamp")), amount))
    return records, unparsed


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
