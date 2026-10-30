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
    readback/kafka/codec-<c>.batches    kafka-dump-log.sh over the topic's first segment: one
                                        `baseOffset: ... compresscodec: <c> ...` line per batch
    readback/redpanda/codec-<c>.batches rpk topic consume -f 'offset: %o compression:
                                        %a{compression}\n': each record's batch codec
    <leg>-<broker>-telemetry.log        each leg's `internal` telemetry, `stdio_out` `format: json`
    readback/<broker>/<topic>.{java,kcat}.tsv  for the OTLP topics, no value: the Java consumer
                                        prints the key last, and kcat prints the value's size
                                        and key length (`%S\t%K`, -1 for a null key)
    collector/<broker>/<name>.jsonl     what the Collector decoded, one OTLP/JSON request a line
    consume-<leg>-<broker>.jsonl        what a consumer leg's `kafka_in` read, `stdio_out`
                                        `format: json`, `_logit.kafka.*` carriers included;
                                        `relay-<broker>-sent.jsonl` and `-text-sent.jsonl` what
                                        the relay leg's generators wrote
    readback/<broker>/consume.offsets   kafka-get-offsets.sh for the consumer legs' topics, and
    readback/<broker>/restart.offsets   for the broker-restart leg's
    readback/<broker>/group-mixed-console.tsv  the group-mixed leg's console consumer's records,
                                        in the `.java.tsv` form
    readback/<broker>/group-mixed.describe  kafka-consumer-groups.sh --describe --state and
                                        --members, read while both members were in the group
    exits.txt                           `<service> <exit code>` per leg and Collector
    redpanda-oidc.txt                   `ok` once Redpanda took OAUTHBEARER, else `failed`
    logs/<service>.log                  each service's log

Prints one row per leg and broker (tools/kafka-interop/README.md, "What it runs"). A row is
PASS (the leg did what that table says it checks), GAP (the broker behaves differently from Apache Kafka in
a way docs/known-gaps/kafka.md records), or FAIL. Exits 1 on any FAIL.
"""

import json
import pathlib
import re
import sys

OUT = pathlib.Path("/out")
BROKERS = ("kafka", "redpanda")
CODECS = ("none", "gzip", "snappy", "lz4", "zstd")

PARITY_EVENTS = 200
PARITY_KEYS = 50
CODEC_EVENTS = 100
DEDUP_EVENTS = 800
TLS_EVENTS = 50
SASL_EVENTS = 50
OAUTH_CC_EVENTS = 60
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


def read_java(broker, topic, path=None):
    path = path or OUT / "readback" / broker / f"{topic}.java.tsv"
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


_DUMP_CODEC = re.compile(r"\bcompresscodec: (\S+)")
_RPK_CODEC = re.compile(r"^offset: \d+ compression: (\S+)$")


def stored_codecs(broker, topic):
    """The codec of each stored batch (Kafka) or of each record's batch (Redpanda), in order."""
    path = OUT / "readback" / broker / f"{topic}.batches"
    if not path.exists():
        return []
    codecs = []
    for line in path.read_text(errors="replace").splitlines():
        if broker == "kafka":
            if line.startswith("baseOffset:"):
                match = _DUMP_CODEC.search(line)
                codecs.append(match.group(1) if match else "?")
        else:
            match = _RPK_CODEC.match(line.strip())
            if match:
                codecs.append(match.group(1))
    return codecs


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


def telemetry_max(events, metric, **attrs):
    """The highest value any point of a gauge took: `logit.component.retrying` reads 0 again once
    the write loop exits, so the last point can't show a hold."""
    best = 0.0
    for event in events:
        have = {**event.get("resource", {}).get("attributes", {}), **event.get("attributes", {})}
        if any(have.get(k) != v for k, v in attrs.items()):
            continue
        for item in event.get("metrics", []):
            if item.get("name") == metric and item.get("kind") == "gauge":
                best = max(best, float(item["value"]))
    return int(best)


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
    stored = stored_codecs(broker, topic)
    unit = "batch(es)" if broker == "kafka" else "record(s)"
    wrong = sorted({c for c in stored if c != codec})
    if not stored:
        return "FAIL", f"no stored batch codec read for {topic}, so the codec is unproven"
    if wrong:
        return "FAIL", (f"{sum(c != codec for c in stored)} of {len(stored)} stored {unit} "
                        f"carry {', '.join(wrong)}, not {codec}")
    return "PASS", (f"{', '.join(results)} records, each event once; all {len(stored)} stored "
                    f"{unit} carry {codec}; end offsets {held}")


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
    # The control's duplicate is the proof that the fault hit a batch the broker had appended:
    # without one, the idempotent sink's resends may never have reached the broker's dedup check.
    if control_seqs != set(range(DEDUP_EVENTS)):
        return "FAIL", f"the control lost {DEDUP_EVENTS - len(control_seqs)} event(s); {detail}"
    if control_n <= len(control_seqs):
        return "FAIL", (f"the control stored no duplicate, so the fault never hit an appended "
                        f"batch and the leg proves nothing; {detail}")
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


def leg_sasl(broker, topic, component, how):
    records = read_java(broker, topic)
    classes = requests_by_class(telemetry("sasl", broker), component)
    if len(records) != SASL_EVENTS or not classes.get("ok"):
        return "FAIL", (f"{topic} holds {len(records)} records, want {SASL_EVENTS}; "
                        f"requests {shown(classes)}")
    seqs = {r.seq() for r in records}
    if seqs != set(range(SASL_EVENTS)):
        return "FAIL", f"{topic} holds {len(seqs)} distinct events, want {SASL_EVENTS}"
    return "PASS", f"{len(records)} records, {how}; requests {shown(classes)}"


def leg_sasl_wrong(broker):
    events = telemetry("sasl", broker)
    records = read_java(broker, "sasl-wrong")
    classes = requests_by_class(events, "sasl_wrong")
    retrying = telemetry_max(events, "logit.component.retrying", component="sasl_wrong")
    rejected = telemetry_sum(events, "logit.component.batches.dropped", component="sasl_wrong",
                             reason="rejected")
    detail = (f"{len(records)} records, requests {shown(classes)}, retrying peaked at "
              f"{retrying}, batches.dropped{{reason=rejected}} {rejected}")
    if records or classes.get("ok"):
        return "FAIL", f"the broker took a login with the wrong password; {detail}"
    if not classes.get("refused"):
        return "FAIL", f"no Refused attempt, so the refusal is unproven; {detail}"
    if retrying != 1:
        return "FAIL", f"the sink wasn't holding and retrying; {detail}"
    if rejected:
        return "FAIL", f"a refused login dropped a batch; {detail}"
    return "PASS", f"refused, held, and retried; {detail}"


def redpanda_oidc_gap(broker):
    """The GAP row for an OAUTHBEARER leg on a Redpanda that wouldn't turn OAUTHBEARER on, an
    Enterprise feature; `None` when the leg can run."""
    path = OUT / "redpanda-oidc.txt"
    if broker == "redpanda" and (not path.exists() or path.read_text().strip() != "ok"):
        return "GAP", ("Redpanda refused OAUTHBEARER, an Enterprise feature "
                       "(logs/redpanda-oidc-setup.log)")
    return None


def token_requests(scope):
    """How many client-credentials requests the `oauth` service logged with `scope`; its
    `DEBUG` log prints each request's body."""
    path = OUT / "logs" / "oauth.log"
    log = path.read_text(errors="replace") if path.exists() else ""
    return len(re.findall(rf"body=grant_type=client_credentials&scope={re.escape(scope)}\)", log))


def leg_oauth(broker, topic, component, events, how, min_tokens=0):
    gap = redpanda_oidc_gap(broker)
    if gap:
        return gap
    records = read_java(broker, topic)
    classes = requests_by_class(telemetry("oauth", broker), component)
    if len(records) != events or not classes.get("ok"):
        return "FAIL", (f"{topic} holds {len(records)} records, want {events}; "
                        f"requests {shown(classes)}")
    if {r.seq() for r in records} != set(range(events)):
        return "FAIL", f"{topic} doesn't hold each of the {events} events once"
    detail = f"{len(records)} records, {how}; requests {shown(classes)}"
    if min_tokens:
        # The sink's `scope` names its broker (logit-oauth-*.yaml).
        tokens = token_requests(f"{broker}-oauth-cc")
        detail += f"; tokens fetched {tokens}"
        if tokens < min_tokens:
            return "FAIL", (f"want at least {min_tokens} tokens fetched, so a redial took a "
                            f"new one; {detail}")
    return "PASS", detail


def leg_oauth_refused(broker):
    gap = redpanda_oidc_gap(broker)
    if gap:
        return gap
    events = telemetry("oauth", broker)
    records = read_java(broker, "oauth-refused")
    classes = requests_by_class(events, "oauth_refused")
    retrying = telemetry_max(events, "logit.component.retrying", component="oauth_refused")
    rejected = telemetry_sum(events, "logit.component.batches.dropped", component="oauth_refused",
                             reason="rejected")
    log_path = OUT / "logs" / f"logit-oauth-{broker}.log"
    log = log_path.read_text(errors="replace") if log_path.exists() else ""
    status = re.search(r'the broker refused the token: status "([^"]*)"', log)
    detail = (f"{len(records)} records, requests {shown(classes)}, retrying peaked at "
              f"{retrying}, batches.dropped{{reason=rejected}} {rejected}, error challenge status "
              f"{status.group(1) if status else 'not logged'}")
    if records or classes.get("ok"):
        return "FAIL", f"the broker took a token that isn't a JWT; {detail}"
    if not classes.get("refused"):
        return "FAIL", f"no Refused attempt, so the refusal is unproven; {detail}"
    if retrying != 1:
        return "FAIL", f"the sink wasn't holding and retrying; {detail}"
    if rejected:
        return "FAIL", f"a refused token dropped a batch; {detail}"
    # Redpanda refuses with `SASL_AUTHENTICATION_FAILED` at once, sending no RFC 7628 error
    # challenge, so only Kafka's refusal carries a status.
    if broker == "kafka" and not status:
        return "FAIL", f"no refusal in the leg's log carries the error challenge's status; {detail}"
    return "PASS", f"refused, held, and retried; {detail}"


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
    if broker == "redpanda" and n == 0 and dropped == 0 and rejected == TIMESTAMP_EVENTS:
        # docs/known-gaps/kafka.md: a broker that names no record in `record_errors` gets the
        # whole partition batch rejected, counted, under ADR `kafka-output` item 5.
        return "GAP", (f"the broker answered INVALID_TIMESTAMP naming no record, so every "
                       f"batch was rejected whole: records.dropped{{reason=rejected}} {rejected}; "
                       f"{detail}")
    return "FAIL", detail


# ---- OTLP legs ---------------------------------------------------------------------------------
#
# `logit-otlp-*.yaml` mints OTLP_TICKS ticks, OTLP_BATCH to a batch, each a metrics event, a log,
# and a span derived from the tick number `seq`, so every expected value below is a function of
# `seq`. The Collector's `kafka` receivers decoded the topics, and its `file` exporters wrote each
# decoded request as one OTLP/JSON line.

OTLP_TICKS = 160
OTLP_BATCH = 20
OTLP_BATCHES = OTLP_TICKS // OTLP_BATCH
OTLP_TRACES = 16
OTLP_BIG_BYTES = 2048
OTLP_METRICS = ("otlp_delta_sum", "otlp_cumulative_sum", "otlp_gauge", "otlp_hist", "otlp_exphist")
HEX32 = re.compile(r"^[0-9a-f]{32}$")


def otlp_trace_id(seq):
    return "a1" + format(seq % OTLP_TRACES, "030x")


def otlp_span_id(seq):
    return format(seq + 1, "016x")


def otlp_log_has_trace(seq):
    return seq % 4 != 3


def murmur2(data):
    """Kafka's `Utils.murmur2`, which the Java producer's default partitioner hashes keys with."""
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(0, length - length % 4, 4):
        k = int.from_bytes(data[i:i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> 24
        k = (k * m) & 0xFFFFFFFF
        h = ((h * m) & 0xFFFFFFFF) ^ k
    tail = data[length - length % 4:]
    if len(tail) == 3:
        h ^= tail[2] << 16
    if len(tail) >= 2:
        h ^= tail[1] << 8
    if len(tail) >= 1:
        h ^= tail[0]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def java_partition(key, partitions):
    return (murmur2(key.encode()) & 0x7FFFFFFF) % partitions


def collector_lines(broker, name):
    path = OUT / "collector" / broker / f"{name}.jsonl"
    if not path.exists():
        return None
    return [json.loads(line) for line in path.read_text(errors="replace").splitlines()
            if line.strip()]


def attr_map(attributes):
    """An OTLP/JSON attribute list as `{key: value}`, a 64-bit integer read back as an int."""
    result = {}
    for item in attributes or []:
        (kind, value), = item["value"].items()
        result[item["key"]] = int(value) if kind == "intValue" else value
    return result


def number(value):
    return 0 if value is None else float(value)


def point_value(point):
    """A number data point's value, whichever of `asDouble` and `asInt` carries it."""
    return number(point["asDouble"] if "asDouble" in point else point.get("asInt"))


def collector_logs(broker, prefix):
    requests = collector_lines(broker, f"{prefix}-logs")
    if requests is None:
        return None
    items = []
    for request in requests:
        for resource in request.get("resourceLogs", []):
            service = attr_map(resource.get("resource", {}).get("attributes")).get("service.name")
            for scope in resource.get("scopeLogs", []):
                for record in scope.get("logRecords", []):
                    items.append((service, record))
    return items


def collector_spans(broker, prefix):
    requests = collector_lines(broker, f"{prefix}-traces")
    if requests is None:
        return None
    items = []
    for request in requests:
        for resource in request.get("resourceSpans", []):
            service = attr_map(resource.get("resource", {}).get("attributes")).get("service.name")
            for scope in resource.get("scopeSpans", []):
                for span in scope.get("spans", []):
                    items.append((service, span))
    return items


def collector_points(broker, prefix):
    """Every data point the Collector decoded: `(metric, kind, point)` per point."""
    requests = collector_lines(broker, f"{prefix}-metrics")
    if requests is None:
        return None
    items = []
    for request in requests:
        for resource in request.get("resourceMetrics", []):
            service = attr_map(resource.get("resource", {}).get("attributes")).get("service.name")
            for scope in resource.get("scopeMetrics", []):
                for metric in scope.get("metrics", []):
                    for kind in ("gauge", "sum", "histogram", "exponentialHistogram"):
                        if kind in metric:
                            for point in metric[kind].get("dataPoints", []):
                                items.append((service, metric, kind, point))
    return items


def check_logs(items):
    """The first problem in the decoded logs, else `None`."""
    if len(items) != OTLP_TICKS:
        return f"{len(items)} log records, want {OTLP_TICKS}"
    seen = set()
    for service, record in items:
        attrs = attr_map(record.get("attributes"))
        seq = attrs.get("seq")
        if not isinstance(seq, int) or not 0 <= seq < OTLP_TICKS or seq in seen:
            return f"a log record's seq is {seq!r}, a repeat or out of range"
        seen.add(seq)
        want_trace = otlp_trace_id(seq) if otlp_log_has_trace(seq) else ""
        want_span = otlp_span_id(seq) if otlp_log_has_trace(seq) else ""
        problems = []
        if service != "kafka-interop-otlp":
            problems.append(f"service.name {service!r}")
        if record.get("body", {}).get("stringValue") != f"otlp log {seq}":
            problems.append(f"body {record.get('body')!r}")
        if record.get("severityNumber") != 9:
            problems.append(f"severityNumber {record.get('severityNumber')!r}")
        if record.get("traceId", "") != want_trace:
            problems.append(f"traceId {record.get('traceId', '')!r}, want {want_trace!r}")
        if record.get("spanId", "") != want_span:
            problems.append(f"spanId {record.get('spanId', '')!r}, want {want_span!r}")
        if attrs.get("leg") != "otlp":
            problems.append(f"leg {attrs.get('leg')!r}")
        if problems:
            return f"log seq {seq}: " + "; ".join(problems)
    return None


def check_spans(items):
    if len(items) != OTLP_TICKS:
        return f"{len(items)} spans, want {OTLP_TICKS}"
    seen = set()
    for service, span in items:
        attrs = attr_map(span.get("attributes"))
        seq = attrs.get("seq")
        if not isinstance(seq, int) or not 0 <= seq < OTLP_TICKS or seq in seen:
            return f"a span's seq is {seq!r}, a repeat or out of range"
        seen.add(seq)
        error = seq % 5 == 0
        status = span.get("status", {})
        problems = []
        if service != "kafka-interop-otlp":
            problems.append(f"service.name {service!r}")
        if span.get("name") != f"otlp-span-{seq}":
            problems.append(f"name {span.get('name')!r}")
        if span.get("traceId") != otlp_trace_id(seq):
            problems.append(f"traceId {span.get('traceId')!r}")
        if span.get("spanId") != otlp_span_id(seq):
            problems.append(f"spanId {span.get('spanId')!r}")
        if span.get("kind") != 2:
            problems.append(f"kind {span.get('kind')!r}, want 2 (server)")
        if status.get("code", 0) != (2 if error else 0):
            problems.append(f"status.code {status.get('code', 0)!r}")
        if status.get("message", "") != ("boom" if error else ""):
            problems.append(f"status.message {status.get('message', '')!r}")
        if [event.get("name") for event in span.get("events", [])] != ["retry"]:
            problems.append(f"events {span.get('events')!r}")
        if problems:
            return f"span seq {seq}: " + "; ".join(problems)
    return None


def check_metrics(items):
    if len(items) != OTLP_TICKS * len(OTLP_METRICS):
        return f"{len(items)} data points, want {OTLP_TICKS * len(OTLP_METRICS)}"
    seen = set()
    for service, metric, kind, point in items:
        name = metric.get("name")
        attrs = attr_map(point.get("attributes"))
        seq = attrs.get("seq")
        if name not in OTLP_METRICS or not isinstance(seq, int) or (name, seq) in seen:
            return f"an unexpected or repeated point: {name!r} seq {seq!r}"
        seen.add((name, seq))
        problems = []
        if service != "kafka-interop-otlp":
            problems.append(f"service.name {service!r}")
        if name == "otlp_delta_sum":
            if kind != "sum" or point_value(point) != 1 \
                    or metric["sum"].get("aggregationTemporality") != 1 \
                    or metric["sum"].get("isMonotonic") is not True:
                problems.append(f"delta sum {kind} {point_value(point)} {metric.get('sum')}")
        elif name == "otlp_cumulative_sum":
            if kind != "sum" or point_value(point) != seq + 1 \
                    or metric["sum"].get("aggregationTemporality") != 2:
                problems.append(f"cumulative sum {kind} {point_value(point)}")
        elif name == "otlp_gauge":
            if kind != "gauge" or point_value(point) != seq * 0.5 \
                    or metric.get("unit") != "By" or metric.get("description") != "a gauge":
                problems.append(f"gauge {kind} {point_value(point)} {metric.get('unit')!r}")
        elif name == "otlp_hist":
            counts = [int(c) for c in point.get("bucketCounts", [])]
            if kind != "histogram" or point.get("explicitBounds") != [1, 5] \
                    or counts != [1, seq % 3, 2] or int(point.get("count", 0)) != 3 + seq % 3 \
                    or number(point.get("sum")) != 10 + seq:
                problems.append(f"histogram {point}")
        elif name == "otlp_exphist":
            positive = point.get("positive", {})
            if kind != "exponentialHistogram" or point.get("scale", 0) != 0 \
                    or int(point.get("zeroCount", 0)) != 1 or int(point.get("count", 0)) != 4 \
                    or number(point.get("sum")) != 7 or positive.get("offset", 0) != 0 \
                    or [int(c) for c in positive.get("bucketCounts", [])] != [1, 2]:
                problems.append(f"exponential histogram {point}")
        if problems:
            return f"{name} seq {seq}: " + "; ".join(problems)
    return None


def collector_problem(broker, prefix):
    """The first mismatch between what the Collector decoded under `prefix` and what was sent."""
    for signal, read, check in (("logs", collector_logs, check_logs),
                                ("traces", collector_spans, check_spans),
                                ("metrics", collector_points, check_metrics)):
        items = read(broker, prefix)
        if items is None:
            return f"the Collector wrote no {prefix}-{signal}.jsonl"
        problem = check(items)
        if problem:
            return f"{signal}: {problem}"
    return None


def sink_summary(broker, component):
    events = telemetry("otlp", broker)
    classes = requests_by_class(events, component)
    dropped = telemetry_sum(events, "logit.output.records.dropped", component=component)
    return classes, dropped


def read_record_meta(broker, topic):
    """`(partition, key, size, key_length)` per record of a topic read back without values.
    kcat prints a null key and an empty one alike as the empty string, so `key_length`
    (kcat's `%K`, -1 for a null key) is what tells them apart."""
    path = OUT / "readback" / broker / f"{topic}.kcat.tsv"
    if not path.exists():
        return None
    records = []
    for line in path.read_text(errors="replace").splitlines():
        if line.strip():
            partition, _offset, _timestamp, key, size, key_length = line.split("\t")
            records.append((int(partition), key, int(size), int(key_length)))
    return records


def java_record_keys(broker, topic):
    """`(partition, key)` per record from the Java consumer, which prints a null key as `null`."""
    path = OUT / "readback" / broker / f"{topic}.java.tsv"
    if not path.exists():
        return None
    records = []
    for line in path.read_text(errors="replace").splitlines():
        fields = line.split("\t")
        meta = {}
        while fields and fields[0].split(":", 1)[0] in ("CreateTime", "LogAppendTime",
                                                        "NoTimestampType", "Partition", "Offset"):
            name, value = fields.pop(0).split(":", 1)
            meta[name] = value
        key = "\t".join(fields)
        records.append((int(meta["Partition"]), "" if key == "null" else key))
    return records


def leg_otlp_unkeyed(broker, prefix, topics):
    """The sink `prefix` wrote one record per signal per batch, and the Collector decoded every
    field. `topics` names the logs, metrics, and spans topics."""
    problem = collector_problem(broker, prefix)
    classes, dropped = sink_summary(broker, prefix)
    counts = {}
    for signal, topic in zip(("logs", "metrics", "spans"), topics):
        records = read_record_meta(broker, topic)
        counts[signal] = None if records is None else len(records)
    detail = (f"records per default topic {counts}, requests {shown(classes)}, "
              f"records.dropped {dropped}")
    if problem:
        return "FAIL", f"{problem}; {detail}"
    if set(counts.values()) != {OTLP_BATCHES}:
        return "FAIL", f"want one record per signal per batch ({OTLP_BATCHES}); {detail}"
    if dropped or not classes.get("ok"):
        return "FAIL", detail
    return "PASS", (f"the Collector decoded {OTLP_TICKS} logs, {OTLP_TICKS} spans, and "
                    f"{OTLP_TICKS * len(OTLP_METRICS)} data points, each field as sent; {detail}")


def leg_otlp_proto(broker):
    return leg_otlp_unkeyed(broker, "proto", ("otlp_logs", "otlp_metrics", "otlp_spans"))


def leg_otlp_json(broker):
    return leg_otlp_unkeyed(broker, "json", ("otlpjson-logs", "otlpjson-metrics",
                                             "otlpjson-spans"))


def leg_otlp_keyed(broker, prefix="keyed", cap=None):
    """`cap` is the sink's `max_request_bytes`, which no record may exceed and which splits
    the metrics requests into more records than batches."""
    problem = collector_problem(broker, prefix)
    classes, dropped = sink_summary(broker, prefix)
    if problem:
        return "FAIL", problem
    if dropped or not classes.get("ok"):
        return "FAIL", f"requests {shown(classes)}, records.dropped {dropped}"
    wanted = {otlp_trace_id(seq) for seq in range(OTLP_TICKS)}
    summary = []
    for signal, topic, traces in (("spans", f"{prefix}-spans", wanted),
                                  ("logs", f"{prefix}-logs",
                                   {otlp_trace_id(s) for s in range(OTLP_TICKS)
                                    if otlp_log_has_trace(s)})):
        kcat = read_record_meta(broker, topic)
        java = java_record_keys(broker, topic)
        if kcat is None or java is None or not kcat:
            return "FAIL", f"{topic} wasn't read back"
        if sorted((p, k) for p, k, _, _ in kcat) != sorted(java):
            return "FAIL", f"{topic}: kcat and the Java consumer disagree on keys or partitions"
        placed = {}
        unkeyed = 0
        for partition, key, size, key_length in kcat:
            if cap is not None and size > cap:
                return "FAIL", f"{topic}: a record of {size} bytes exceeds max_request_bytes {cap}"
            if key == "":
                if key_length != -1:
                    return "FAIL", (f"{topic}: a record has an empty key (length {key_length}), "
                                    "not a null one")
                unkeyed += 1
                continue
            if not HEX32.match(key):
                return "FAIL", f"{topic}: key {key!r} isn't 32 lowercase hex characters"
            placed.setdefault(key, set()).add(partition)
        if set(placed) != traces:
            return "FAIL", f"{topic}: keys {len(placed)} distinct, want {len(traces)}"
        split = sorted(key for key, parts in placed.items() if len(parts) != 1)
        if split:
            return "FAIL", f"{topic}: a trace id on several partitions: {split[:3]}"
        wrong = sorted(key for key, parts in placed.items()
                       if parts != {java_partition(key, 6)})
        if wrong:
            return "FAIL", f"{topic}: {len(wrong)} trace ids off the Java partitioner's partition"
        summary.append(f"{topic} {len(kcat)} records, {len(placed)} trace ids each on one "
                       f"partition, {unkeyed} unkeyed")
    metrics = read_record_meta(broker, f"{prefix}-metrics") or []
    if any(size > cap for _, _, size, _ in metrics if cap is not None):
        return "FAIL", f"{prefix}-metrics holds a record over max_request_bytes {cap}"
    if any(key or key_length != -1 for _, key, _, key_length in metrics):
        return "FAIL", f"{prefix}-metrics holds a keyed record"
    if len(metrics) < (OTLP_BATCHES if cap is None else OTLP_BATCHES + 1):
        return "FAIL", (f"{prefix}-metrics holds {len(metrics)} records for {OTLP_BATCHES} "
                        "batches" + ("" if cap is None else ", so no request split"))
    return "PASS", ("keys are the trace id in 32 lowercase hex characters, on the Java "
                    f"partitioner's partition; {'; '.join(summary)}; {prefix}-metrics "
                    f"{len(metrics)} unkeyed")


def exporter_placement(broker, topic):
    records = read_record_meta(broker, topic)
    if not records:
        return None
    placed = {}
    for partition, key, _size, _key_length in records:
        placed.setdefault(key, set()).add(partition)
    return placed


def leg_otlp_exporter_parity(broker):
    mine = exporter_placement(broker, "keyed-spans")
    default = exporter_placement(broker, "exporter-default")
    murmur = exporter_placement(broker, "exporter-murmur2")
    if not mine or not default or not murmur:
        return "FAIL", "a topic holds no records: the Collector's exporter or kafka_out wrote none"
    if set(default) != set(mine) or set(murmur) != set(mine):
        return "FAIL", "the exporter and kafka_out keyed different trace ids"
    for name, placed in (("kafka_out", mine), ("exporter default", default),
                         ("exporter murmur2", murmur)):
        if any(len(parts) != 1 for parts in placed.values()):
            return "FAIL", f"{name} put a trace id on several partitions"
    differ_default = sorted(key for key in mine if default[key] != mine[key])
    differ_murmur = sorted(key for key in mine if murmur[key] != mine[key])
    detail = (f"of {len(mine)} trace ids, the exporter with its default partitioner "
              f"(sticky_key, Sarama-compatible FNV-1a) put {len(differ_default)} on a different "
              f"partition than kafka_out, and the exporter with the murmur2 hasher put "
              f"{len(differ_murmur)}")
    if differ_murmur:
        return "FAIL", detail
    return "PASS", detail


def big_item_batches(broker, name, resources, scopes, items):
    """For each request line the Collector wrote, the set of batches (`seq // OTLP_BATCH`) its
    items belong to."""
    lines = collector_lines(broker, name) or []
    result = []
    for request in lines:
        batches = set()
        for resource in request.get(resources, []):
            for scope in resource.get(scopes, []):
                for item in scope.get(items, []):
                    if items == "metrics":
                        points = [point for kind in ("gauge", "sum", "histogram",
                                                     "exponentialHistogram")
                                  for point in item.get(kind, {}).get("dataPoints", [])]
                    else:
                        points = [item]
                    for point in points:
                        seq = attr_map(point.get("attributes")).get("seq")
                        if isinstance(seq, int):
                            batches.add(seq // OTLP_BATCH)
        result.append(tuple(sorted(batches)) if batches else ())
    return result


def leg_otlp_oversize(broker):
    problem = collector_problem(broker, "big")
    classes, dropped = sink_summary(broker, "big")
    if problem:
        return "FAIL", problem
    if dropped or not classes.get("ok"):
        return "FAIL", f"requests {shown(classes)}, records.dropped {dropped}"
    counts = {}
    largest = 0
    for signal, topic in (("logs", "big-logs"), ("metrics", "big-metrics"), ("spans", "big-spans")):
        records = read_record_meta(broker, topic) or []
        counts[signal] = len(records)
        largest = max([largest] + [size for _, _, size, _ in records])
    # The receiver hands the file exporter one request per record, so a file line is a record.
    # Each batch's items must arrive in at least two lines, and no line may span two batches.
    for signal, lines in (("logs", big_item_batches(broker, "big-logs", "resourceLogs",
                                                    "scopeLogs", "logRecords")),
                          ("spans", big_item_batches(broker, "big-traces", "resourceSpans",
                                                     "scopeSpans", "spans")),
                          ("metrics", big_item_batches(broker, "big-metrics", "resourceMetrics",
                                                       "scopeMetrics", "metrics"))):
        if any(len(batches) != 1 for batches in lines):
            return "FAIL", f"a big-{signal} record holds items of several batches"
        records_per_batch = {}
        for (batch,) in lines:
            records_per_batch[batch] = records_per_batch.get(batch, 0) + 1
        unsplit = sorted(b for b in range(OTLP_BATCHES) if records_per_batch.get(b, 0) < 2)
        if unsplit:
            return "FAIL", (f"big-{signal}: batches {unsplit} arrived in fewer than two "
                            f"records, so their requests weren't split: {counts}")
    if largest > OTLP_BIG_BYTES:
        return "FAIL", f"a record of {largest} bytes exceeds max_request_bytes {OTLP_BIG_BYTES}"
    return "PASS", (f"{OTLP_BATCHES} batches split into {counts} records, the largest {largest} "
                    f"bytes under the {OTLP_BIG_BYTES} cap; the Collector decoded every item")


LEGS = [("parity", "partition parity with kafka-console-producer.sh", leg_parity)]
LEGS += [(f"codec-{c}", f"compression: {c} stored, read by Java and kcat",
          lambda b, c=c: leg_codec(b, c)) for c in CODECS]
LEGS += [
    ("dedup", "idempotent resend under netem response loss", leg_dedup),
    ("tls", "server TLS, private CA", lambda b: leg_tls(b, "tls", "tls")),
    ("mtls", "mutual TLS, client certificate", lambda b: leg_tls(b, "mtls", "mtls")),
    ("mtls-required", "mutual TLS refuses a client without a certificate", leg_mtls_required),
    ("sasl-plain", "SASL PLAIN over SASL_SSL",
     lambda b: leg_sasl(b, "sasl-plain", "sasl_plain", "PLAIN over SASL_SSL")),
    ("sasl-scram512", "SASL SCRAM-SHA-512 over SASL_SSL",
     lambda b: leg_sasl(b, "sasl-scram512", "sasl_scram512", "SCRAM-SHA-512 over SASL_SSL")),
    ("sasl-scram256", "SASL SCRAM-SHA-256 over SASL_PLAINTEXT",
     lambda b: leg_sasl(b, "sasl-scram256", "sasl_scram256", "SCRAM-SHA-256 over SASL_PLAINTEXT")),
    ("sasl-wrong", "a wrong password is refused and retried", leg_sasl_wrong),
    ("timestamp", "a record over an hour ahead dropped and counted", leg_timestamp),
    ("oauth-cc", "OAUTHBEARER, client credentials, past the first token's expiry",
     lambda b: leg_oauth(b, "oauth-cc", "oauth_cc", OAUTH_CC_EVENTS,
                         "over 60 s of 45 s tokens", min_tokens=2)),
    ("oauth-static", "OAUTHBEARER, a fixed token (with two SASL extensions on Kafka)",
     lambda b: leg_oauth(b, "oauth-static", "oauth_static", SASL_EVENTS,
                         "a fixed token and two extensions" if b == "kafka" else
                         "a fixed token; Redpanda takes no extensions")),
    ("oauth-refused", "a token the broker won't take is refused, with its status, and retried",
     leg_oauth_refused),
    ("otlp-proto", "otlp_proto on the default topics, decoded by a Collector kafkareceiver",
     leg_otlp_proto),
    ("otlp-keyed", "key: trace_id puts each trace id's records on one partition, as Java would",
     leg_otlp_keyed),
    ("otlp-exporter-parity", "trace-keyed placement beside the Collector's kafkaexporter",
     leg_otlp_exporter_parity),
    ("otlp-oversize", "a request over max_request_bytes splits and the receiver reads every item",
     leg_otlp_oversize),
    ("otlp-json", "otlp_json on its own topics, decoded by a Collector kafkareceiver",
     leg_otlp_json),
    ("otlp-json-keyed", "otlp_json with key: trace_id and a size cap, read by the receiver",
     lambda b: leg_otlp_keyed(b, "jsonbig", cap=4096)),
]



# ---- consumer legs -----------------------------------------------------------------------------
#
# Each `logit-in-*` leg's `kafka_in` writes what it read to /out as `stdio_out` `format: json`
# lines (crates/logit-outputs/src/ndjson.rs's module doc is the grammar), each event carrying its
# record's `_logit.kafka.*` carriers. `readback/<broker>/consume.offsets` and `restart.offsets`
# hold the end offsets of the topics only consumer legs write.

KAFKA_CARRIER = "_logit.kafka."
CONSOLE_LINES = 50
GROUP_RECORDS = 400
GROUP_RATE = 10
RESTART_RECORDS = 300
RESTART_RATE = 5
RESTART_PARTITIONS = 3
# kafka_in's default `commit_interval`, in seconds.
COMMIT_INTERVAL = 5
RELAY_TICKS = 40
RELAY_FORMATS = ("json", "msgpack", "native", "text", "otlp_proto", "otlp_json")
RELAY_RECORDS = ("event", "batch")
OTLP_SIGNALS = {"logs": "log", "metrics": "metrics", "spans": "span"}
TXN_RECORDS = 100


def consumed(name):
    """The events a consumer leg wrote to /out/<name>.jsonl, or `None` without the file."""
    path = OUT / f"{name}.jsonl"
    if not path.exists():
        return None
    return [json.loads(line) for line in path.read_text(errors="replace").splitlines()
            if line.strip()]


def carrier(event, name):
    return event.get("attributes", {}).get(KAFKA_CARRIER + name)


def place(event):
    """The `(topic, partition, offset)` of the record an event arrived in."""
    return carrier(event, "topic"), carrier(event, "partition"), carrier(event, "offset")


def without_carriers(event):
    """The event with its `_logit.kafka.*` attributes and their `types` entries removed: what
    the producing `logit` wrote, under ADR `kafka-input` item 13."""
    event = json.loads(json.dumps(event))
    attributes = {k: v for k, v in event.get("attributes", {}).items()
                  if not k.startswith(KAFKA_CARRIER)}
    types = {k: v for k, v in event.get("types", {}).items()
             if not k.startswith("/attributes/" + KAFKA_CARRIER)}
    event.pop("attributes", None)
    event.pop("types", None)
    if attributes:
        event["attributes"] = attributes
    if types:
        event["types"] = types
    return event


def consume_offsets(broker, name="consume"):
    """`{(topic, partition): end offset}` from `readback/<broker>/<name>.offsets`."""
    path = OUT / "readback" / broker / f"{name}.offsets"
    ends = {}
    if path.exists():
        for line in path.read_text().splitlines():
            parts = line.strip().rsplit(":", 2)
            if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
                ends[(parts[0], int(parts[1]))] = int(parts[2])
    return ends


def ms_floor(stamp):
    """An RFC 3339 timestamp with nine fractional digits, cut to milliseconds."""
    return stamp[:23] + "000000Z"


def leg_consume_console(broker):
    events = consumed(f"consume-console-{broker}")
    if events is None:
        return "FAIL", "the leg wrote no events"
    ends = {p: end for (topic, p), end in consume_offsets(broker).items() if topic == "console"}
    seen = {}
    for event in events:
        topic, partition, offset = place(event)
        message = event.get("log", {}).get("message", "")
        match = re.fullmatch(r"console line (\d+) é", message)
        if topic != "console" or not match:
            return "FAIL", f"an unexpected event: {json.dumps(event)[:300]}"
        n = int(match.group(1))
        want_headers = {"seq": str(n), "rep": ["a", "b"], "nil": None}
        problems = []
        if carrier(event, "key") != f"key-{n}":
            problems.append(f"key {carrier(event, 'key')!r}")
        if carrier(event, "headers") != want_headers:
            problems.append(f"headers {carrier(event, 'headers')!r}")
        if carrier(event, "timestamp_type") != "create_time":
            problems.append(f"timestamp_type {carrier(event, 'timestamp_type')!r}")
        if event.get("timestamp") != carrier(event, "timestamp"):
            problems.append(f"timestamp {event.get('timestamp')} against the record's "
                            f"{carrier(event, 'timestamp')}")
        if event.get("types", {}).get("/attributes/_logit.kafka.timestamp") != "timestamp":
            problems.append("the timestamp carrier isn't typed `timestamp`")
        if event.get("log", {}).get("format") != "raw" or "severity" in event.get("log", {}) \
                or "resource" in event:
            problems.append(f"log {event.get('log')!r}, resource {event.get('resource')!r}")
        if problems:
            return "FAIL", f"line {n}: " + "; ".join(problems)
        if n in seen:
            return "FAIL", f"line {n} read twice, at {seen[n]} and {(partition, offset)}"
        seen[n] = (partition, offset)
    if set(seen) != set(range(CONSOLE_LINES)):
        return "FAIL", f"read {len(seen)} of {CONSOLE_LINES} lines"
    read = {}
    for partition, offset in seen.values():
        read.setdefault(partition, set()).add(offset)
    gaps = {p: end for p, end in ends.items() if read.get(p, set()) != set(range(end))}
    if not ends or gaps:
        return "FAIL", f"offsets read don't match the end offsets {ends}: {sorted(read.items())}"
    return "PASS", (f"{CONSOLE_LINES} lines on {len(ends)} partitions, each read once with its "
                    "key, its `seq` and null `nil` headers, `rep`'s two values in order, and "
                    "the record timestamp as the event's")


def relay_received(broker):
    events = consumed(f"consume-relay-{broker}")
    by_topic = {}
    for event in events or []:
        by_topic.setdefault(carrier(event, "topic"), []).append(event)
    return events is not None, by_topic


def canonical(event):
    return json.dumps(event, sort_keys=True)


def multiset_problem(want, got):
    """The first difference between two event lists compared as multisets, else `None`."""
    counts = {}
    for event in want:
        counts[canonical(event)] = counts.get(canonical(event), 0) + 1
    for event in got:
        counts[canonical(event)] = counts.get(canonical(event), 0) - 1
    missing = [k for k, n in counts.items() if n > 0]
    extra = [k for k, n in counts.items() if n < 0]
    if not missing and not extra:
        return None
    return (f"{sum(n for n in counts.values() if n > 0)} sent event(s) not read, "
            f"{-sum(n for n in counts.values() if n < 0)} read event(s) not sent; "
            f"first missing {missing[0][:400] if missing else '-'}; "
            f"first extra {extra[0][:400] if extra else '-'}")


def otlp_normalized(event, sent_log):
    """A read OTLP event under the OTLP formats' own normalizations (crates/logit-proto/src/otlp's
    mapping table): `kafka_out` stamps a log with no observed time with the time it encodes the
    request, and the decoder writes the raw severity number and text into
    `_logit.otel.severity_*` beside the band's severity."""
    log = event.get("log")
    if log is not None and sent_log is not None and "observed_timestamp" not in sent_log:
        log.pop("observed_timestamp", None)
    attributes = event.get("attributes", {})
    for key in ("_logit.otel.severity_number", "_logit.otel.severity_text"):
        attributes.pop(key, None)
    if "attributes" in event and not attributes:
        event.pop("attributes")
    return event


def leg_consume_relay(broker, fmt, codec):
    ok, by_topic = relay_received(broker)
    if not ok:
        return "FAIL", "the leg wrote no events"
    sent = consumed(f"relay-{broker}-sent")
    sent_text = consumed(f"relay-{broker}-text-sent")
    if not sent or not sent_text:
        return "FAIL", "the generators wrote no events"
    if len(sent) != 3 * RELAY_TICKS or len(sent_text) != RELAY_TICKS:
        return "FAIL", f"the generators wrote {len(sent)} and {len(sent_text)} events"
    summary = []
    if fmt.startswith("otlp"):
        # One read event per data point: the OTLP decoder makes an event of each.
        points = []
        for event in sent:
            for metric in event.get("metrics", []):
                points.append({**event, "metrics": [metric]})
        for signal, field in OTLP_SIGNALS.items():
            topic = f"relay-{fmt}-{codec}-{signal}"
            want = points if signal == "metrics" else [e for e in sent if field in e]
            sent_logs = {e["log"].get("message"): e["log"] for e in want if "log" in e}
            got = []
            for event in by_topic.get(topic, []):
                bare = without_carriers(event)
                message = bare.get("log", {}).get("message")
                got.append(otlp_normalized(bare, sent_logs.get(message)))
            problem = multiset_problem(want, got)
            if problem:
                return "FAIL", f"{topic}: {problem}"
            summary.append(f"{signal} {len(got)}")
        return "PASS", (f"{', '.join(summary)} events, each equal to what was sent, field for "
                        "field, but for the OTLP formats' observed time and severity carriers")
    for record in RELAY_RECORDS:
        topic = f"relay-{fmt}-{record}-{codec}"
        read = by_topic.get(topic, [])
        if fmt != "text":
            problem = multiset_problem(sent, [without_carriers(e) for e in read])
            if problem:
                return "FAIL", f"{topic}: {problem}"
            records = len({place(e) for e in read})
            summary.append(f"{record}: {len(read)} events in {records} records")
            continue
        # `text` carries the body alone, at the record's timestamp: the event's own, cut to
        # milliseconds, under `record: event`, and the record's first event's under `batch`.
        stamps = {e["log"]["message"]: e["timestamp"] for e in sent_text}
        bodies = [e.get("log", {}).get("message") for e in read]
        problem = multiset_problem([{"m": m} for m in stamps], [{"m": m} for m in bodies])
        if problem:
            return "FAIL", f"{topic}: {problem}"
        first = {}
        for event in read:
            first.setdefault(place(event), event["log"]["message"])
        for event in read:
            bare = without_carriers(event)
            record_stamp = ms_floor(stamps[first[place(event)]])
            extra = {k: v for k, v in bare.items() if k not in ("timestamp", "log")}
            if bare["timestamp"] != record_stamp or carrier(event, "timestamp") != record_stamp \
                    or extra or bare["log"] != {"format": "raw",
                                                "message": bare["log"]["message"]}:
                return "FAIL", (f"{topic}: {json.dumps(event)[:400]} isn't the body alone at "
                                f"{record_stamp}")
        summary.append(f"{record}: {len(read)} logs in {len(first)} records")
    return "PASS", "; ".join(summary) + (", each equal to what was sent, field for field" if
                                          fmt != "text" else ", each body as sent at its "
                                          "record's timestamp")


def collector_read(broker, fmt):
    events = consumed(f"consume-collector-{broker}")
    if events is None:
        return None
    prefix = f"collector-{fmt}-"
    return [e for e in events if (carrier(e, "topic") or "").startswith(prefix)]


def leg_consume_collector(broker, fmt):
    events = collector_read(broker, fmt)
    if events is None:
        return "FAIL", "the leg wrote no events"
    logs, spans, points = {}, {}, {}
    for event in events:
        attributes = event.get("attributes", {})
        seq = attributes.get("seq")
        signal = carrier(event, "topic").rsplit("-", 1)[1]
        service = event.get("resource", {}).get("attributes", {}).get("service.name")
        if service != "kafka-interop-otlp" or attributes.get("leg") != "otlp" \
                or not isinstance(seq, int):
            return "FAIL", f"an event without the leg's resource or attributes: {json.dumps(event)[:300]}"
        if signal == "logs":
            log = event.get("log", {})
            trace = otlp_trace_id(seq) if otlp_log_has_trace(seq) else None
            if log.get("message") != f"otlp log {seq}" or log.get("severity") != "info" \
                    or log.get("trace_id") != trace \
                    or log.get("span_id") != (otlp_span_id(seq) if trace else None):
                return "FAIL", f"log seq {seq}: {log}"
            logs[seq] = logs.get(seq, 0) + 1
        elif signal == "spans":
            span = event.get("span", {})
            error = seq % 5 == 0
            if span.get("name") != f"otlp-span-{seq}" or span.get("trace_id") != otlp_trace_id(seq) \
                    or span.get("span_id") != otlp_span_id(seq) or span.get("kind") != "server" \
                    or span.get("status") != ("error" if error else "unset") \
                    or span.get("status_message") != ("boom" if error else None) \
                    or [e.get("name") for e in span.get("events", [])] != ["retry"]:
                return "FAIL", f"span seq {seq}: {span}"
            spans[seq] = spans.get(seq, 0) + 1
        else:
            for metric in event.get("metrics", []):
                name = metric.get("name")
                want = {
                    "otlp_delta_sum": {"kind": "sum", "value": 1.0, "temporality": "delta",
                                       "monotonic": True},
                    "otlp_cumulative_sum": {"kind": "sum", "value": float(seq + 1),
                                            "temporality": "cumulative"},
                    "otlp_gauge": {"kind": "gauge", "value": seq * 0.5, "unit": "By",
                                   "description": "a gauge"},
                    "otlp_hist": {"kind": "histogram", "temporality": "delta",
                                  "buckets": [{"bound": 1.0, "count": 1},
                                              {"bound": 5.0, "count": seq % 3},
                                              {"bound": "inf", "count": 2}],
                                  "sum": float(10 + seq)},
                    "otlp_exphist": {"kind": "exponential_histogram", "scale": 0,
                                     "temporality": "delta", "count": 4, "zero_count": 1,
                                     "positive": {"offset": 0, "counts": [1, 2]}, "sum": 7.0},
                }.get(name)
                if want is None or any(metric.get(k) != v for k, v in want.items()):
                    return "FAIL", f"{name} seq {seq}: {metric}"
                points[(name, seq)] = points.get((name, seq), 0) + 1
    detail = (f"{len(logs)} logs, {len(spans)} spans, and {len(points)} data points read; "
              f"{sum(logs.values()) + sum(spans.values()) + sum(points.values()) - len(logs) - len(spans) - len(points)} "
              "read more than once")
    if set(logs) != set(range(OTLP_TICKS)) or set(spans) != set(range(OTLP_TICKS)) \
            or len(points) != OTLP_TICKS * len(OTLP_METRICS):
        return "FAIL", f"want {OTLP_TICKS} of each signal and {OTLP_TICKS * len(OTLP_METRICS)} points; {detail}"
    return "PASS", detail + ", each field as the tick number implies"


def ownership(members, topic, ends):
    """Judges a group's reads of `topic` from each member's events: `(missing, duplicates,
    switches, per_member)`. `missing` are offsets below a partition's end no member read;
    `duplicates` the reads beyond the first of an offset; `switches` per partition, how often its
    reads change member in offset order, skipping an offset two members read."""
    readers = {}
    reads = 0
    per_member = {}
    for member, events in members.items():
        for event in events:
            t, partition, offset = place(event)
            if t != topic:
                continue
            readers.setdefault((partition, offset), []).append(member)
            per_member[member] = per_member.get(member, 0) + 1
            reads += 1
    missing = sum(1 for (t, p), end in ends.items() if t == topic
                  for o in range(end) if (p, o) not in readers)
    switches = {}
    for (t, partition), end in ends.items():
        if t != topic:
            continue
        owners = [readers[(partition, o)][0] for o in range(end)
                  if len(set(readers.get((partition, o), []))) == 1]
        switches[partition] = sum(1 for a, b in zip(owners, owners[1:]) if a != b)
    return missing, reads - len(readers), switches, per_member


def seqs_read(members):
    seqs = set()
    for events in members.values():
        for event in events:
            seq = event.get("attributes", {}).get("seq")
            if seq is not None:
                seqs.add(int(seq))
    return seqs


def judge_group(broker, topic, members, max_switches, max_duplicates, extra=""):
    if any(events is None for events in members.values()):
        return "FAIL", f"a member wrote no events: {sorted(m for m, e in members.items() if e is None)}"
    ends = {k: v for k, v in consume_offsets(broker).items() if k[0] == topic}
    if not ends:
        return "FAIL", f"no end offsets read for {topic}"
    missing, duplicates, switches, per_member = ownership(members, topic, ends)
    seqs = seqs_read(members)
    detail = (f"reads per member {per_member}, {duplicates} offset(s) read twice, member changes "
              f"per partition {dict(sorted(switches.items()))}, {len(seqs)} of {GROUP_RECORDS} "
              f"records{extra}")
    if missing or seqs != set(range(GROUP_RECORDS)):
        return "FAIL", f"{missing} offset(s) below the end offsets read by no member; {detail}"
    if any(n > max_switches for n in switches.values()):
        return "FAIL", (f"a partition changed member more than {max_switches} time(s), so two "
                        f"members read it in one generation; {detail}")
    if duplicates > max_duplicates:
        return "FAIL", f"over {max_duplicates} offset(s) read twice; {detail}"
    if len([m for m, n in per_member.items() if n]) != len(members):
        return "FAIL", f"a member read nothing; {detail}"
    return "PASS", detail


def leg_consume_group_join(broker):
    members = {m: consumed(f"consume-group-join-{m}-{broker}") for m in ("a", "b")}
    # Every rebalance commits the revoked partitions' positions before the rejoin, so the member
    # that joins later reads on from there: no offset is read twice.
    return judge_group(broker, "group-join", members, 1, 0)


def leg_consume_group_kill(broker):
    members = {m: consumed(f"consume-group-kill-{m}-{broker}") for m in ("a", "b")}
    # `b` dies between commits, so `a` reads again what `b` read after its last commit: up to one
    # commit interval of records, plus one second of slack for the fetch in flight. `b`'s output
    # file can lose its last lines at the kill, which only shrinks the count.
    bound = GROUP_RATE * (COMMIT_INTERVAL + 1)
    # A partition can move twice: to `b` when it joins beside `a`, and back once `b` is gone.
    status, detail = judge_group(broker, "group-kill", members, 2, bound)
    events = telemetry("in-group-kill-a", broker)
    lost = telemetry_sum(events, "logit.input.rebalances", component="member")
    return status, f"{detail}; member a's rebalances {lost}"


def group_strategy(broker):
    path = OUT / "readback" / broker / "group-mixed.describe"
    text = path.read_text(errors="replace") if path.exists() else ""
    # `--state` prints GROUP, COORDINATOR (ID), ASSIGNMENT-STRATEGY, STATE, and #MEMBERS; the
    # coordinator column holds a space, so the state is found by name.
    states = ("Stable", "PreparingRebalance", "CompletingRebalance", "Empty", "Dead")
    for line in text.splitlines():
        fields = line.split()
        if fields and fields[0] == "consume-group-mixed":
            for i, field in enumerate(fields[2:], 2):
                if field in states and i + 1 < len(fields):
                    return fields[i - 1], field, fields[i + 1], text
    return None, None, None, text


def console_reads(broker):
    """The console consumer's records of `group-mixed` as events with carriers."""
    path = OUT / "readback" / broker / "group-mixed-console.tsv"
    if not path.exists():
        return None
    events = []
    for record in read_java(broker, "group-mixed", path):
        event = json.loads(record.value)
        event.setdefault("attributes", {}).update({
            KAFKA_CARRIER + "topic": "group-mixed", KAFKA_CARRIER + "partition": record.partition,
            KAFKA_CARRIER + "offset": record.offset})
        events.append(event)
    return events


def leg_consume_group_mixed(broker):
    strategy, state, size, text = group_strategy(broker)
    if strategy != "range" or size != "2":
        return "FAIL", (f"the group's assignment strategy is {strategy!r} with {size} members "
                        f"(state {state!r}), want range with 2: {text.strip()[-300:]!r}")
    members = {"kafka_in": consumed(f"consume-group-mixed-{broker}"),
               "console": console_reads(broker)}
    # Partitions move to the console consumer when it joins and back when it leaves.
    bound = GROUP_RATE * (COMMIT_INTERVAL + 1)
    return judge_group(broker, "group-mixed", members, 2, bound,
                       extra=f"; the group's strategy {strategy} while both were members")


def leg_consume_restart(broker):
    events = consumed(f"consume-restart-{broker}")
    if events is None:
        return "FAIL", "the leg wrote no events"
    ends = {k: v for k, v in consume_offsets(broker, "restart").items() if k[0] == "restart"}
    tele = telemetry("in-restart", broker)
    resets = telemetry_sum(tele, "logit.input.offsets.reset", component="member",
                           reason="no_commit")
    rebalances = {reason: telemetry_sum(tele, "logit.input.rebalances", component="member",
                                        reason=reason)
                  for reason in ("start", "rebalance", "membership_lost", "metadata")}
    rebalances = {k: v for k, v in rebalances.items() if v}
    commits = {cls: telemetry_sum(tele, "logit.input.commits", component="member",
                                  **{"class": cls}) for cls in ("ok", "error", "fenced")}
    errors = telemetry_sum(tele, "logit.input.broker.errors", component="member")
    missing, duplicates, _switches, _ = ownership({"member": events}, "restart", ends)
    seqs = seqs_read({"member": events})
    detail = (f"{len(events)} events for {len(seqs)} of {RESTART_RECORDS} records, {duplicates} "
              f"offset(s) read twice, offsets.reset{{reason=no_commit}} {resets}, rebalances "
              f"{rebalances}, commits {commits}, broker.errors {errors}")
    if not ends:
        return "FAIL", "no end offsets read for restart"
    if missing or seqs != set(range(RESTART_RECORDS)):
        return "FAIL", f"{missing} offset(s) below the end offsets never read; {detail}"
    if resets != RESTART_PARTITIONS:
        return "FAIL", (f"want start: used once per partition ({RESTART_PARTITIONS}), so the "
                        f"second life resumed from the commits; {detail}")
    if rebalances.get("start") != 2:
        return "FAIL", f"want two joins counted `start`, one per process life; {detail}"
    if not errors:
        return "FAIL", f"no broker error counted, so the restart never reached the member; {detail}"
    bound = RESTART_RATE * (COMMIT_INTERVAL + 1)
    if duplicates > bound:
        return "FAIL", f"over {bound} offset(s) read twice, more than a commit interval; {detail}"
    return "PASS", detail


def leg_consume_auth(broker, component, topic, events_wanted=SASL_EVENTS):
    events = consumed(f"consume-auth-{broker}")
    if events is None:
        return "FAIL", "the leg wrote no events"
    read = [e for e in events if carrier(e, "topic") == topic]
    seqs = [int(e["attributes"]["seq"]) for e in read]
    if sorted(seqs) != list(range(events_wanted)):
        return "FAIL", (f"read {len(seqs)} events for {len(set(seqs))} of {events_wanted} "
                        f"records of {topic}")
    tele = telemetry("in-auth", broker)
    errors = telemetry_sum(tele, "logit.input.broker.errors", component=component)
    return "PASS", f"{len(read)} events, each record once; broker.errors {errors}"


def leg_consume_oauth(broker):
    gap = redpanda_oidc_gap(broker)
    if gap:
        return gap
    events = consumed(f"consume-oauth-{broker}")
    if events is None:
        return "FAIL", "the leg wrote no events"
    seqs = [int(e["attributes"]["seq"]) for e in events if carrier(e, "topic") == "oauth-cc"]
    tokens = token_requests(f"{broker}-consume-oauth-cc")
    detail = f"{len(seqs)} events for {len(set(seqs))} of {OAUTH_CC_EVENTS} records; tokens fetched {tokens}"
    if sorted(seqs) != list(range(OAUTH_CC_EVENTS)):
        return "FAIL", detail
    if tokens < 2:
        return "FAIL", f"want at least 2 tokens fetched, so a redial took a new one; {detail}"
    return "PASS", detail


def leg_consume_read_committed(broker):
    committed = consumed(f"consume-txn-committed-{broker}")
    uncommitted = consumed(f"consume-txn-uncommitted-{broker}")
    if committed is None or uncommitted is None:
        return "FAIL", "a member wrote no events"
    want = {f"{'committed' if t % 2 == 0 else 'aborted'} {t} {r}"
            for t in range(10) for r in range(10)}
    want_committed = {m for m in want if m.startswith("committed")}
    read_c = [e.get("log", {}).get("message") for e in committed]
    read_u = [e.get("log", {}).get("message") for e in uncommitted]
    tele = telemetry("in-txn", broker)
    aborted = telemetry_sum(tele, "logit.input.records.skipped", component="committed",
                            reason="aborted")
    detail = (f"read_committed read {len(read_c)}, read_uncommitted {len(read_u)}; "
              f"records.skipped{{reason=aborted}} {aborted}")
    if sorted(read_c) != sorted(want_committed):
        return "FAIL", f"read_committed didn't read the 50 committed records once each; {detail}"
    if sorted(read_u) != sorted(want):
        return "FAIL", f"read_uncommitted didn't read all {TXN_RECORDS} records once each; {detail}"
    if aborted != TXN_RECORDS // 2:
        return "FAIL", f"want the 50 aborted records counted skipped; {detail}"
    return "PASS", detail


LEGS += [
    ("consume-console", "kafka-console-producer.sh keys and headers read as format: text",
     leg_consume_console),
]
LEGS += [(f"consume-relay-{f}-{c}", f"kafka_out to kafka_in, format: {f}, compression: {c}",
          lambda b, f=f, c=c: leg_consume_relay(b, f, c))
         for f in RELAY_FORMATS for c in CODECS]
LEGS += [
    ("consume-collector-proto", "the Collector's kafkaexporter otlp_proto read by kafka_in",
     lambda b: leg_consume_collector(b, "proto")),
    ("consume-collector-json", "the Collector's kafkaexporter otlp_json read by kafka_in",
     lambda b: leg_consume_collector(b, "json")),
    ("consume-group-join", "a second member joins: every record read, no partition shared",
     leg_consume_group_join),
    ("consume-group-kill", "a member killed: the other reads on from its last commit",
     leg_consume_group_kill),
    ("consume-group-mixed", "kafka_in and kafka-console-consumer.sh in one group pick range",
     leg_consume_group_mixed),
    ("consume-broker-restart", "broker restarts: the member reads through one and resumes from "
     "its commit after another",
     leg_consume_restart),
    ("consume-tls", "kafka_in over server TLS", lambda b: leg_consume_auth(b, "tls", "tls", TLS_EVENTS)),
    ("consume-mtls", "kafka_in over mutual TLS", lambda b: leg_consume_auth(b, "mtls", "mtls", TLS_EVENTS)),
    ("consume-sasl-plain", "kafka_in with SASL PLAIN over SASL_SSL",
     lambda b: leg_consume_auth(b, "sasl_plain", "sasl-plain")),
    ("consume-sasl-scram512", "kafka_in with SASL SCRAM-SHA-512 over SASL_SSL",
     lambda b: leg_consume_auth(b, "sasl_scram512", "sasl-scram512")),
    ("consume-oauth-cc", "kafka_in with OAUTHBEARER client credentials, past a token's expiry",
     leg_consume_oauth),
    ("consume-read-committed", "read_committed skips aborted transactions, read_uncommitted doesn't",
     leg_consume_read_committed),
]

# The member the group-kill leg kills with SIGKILL, which exits 137.
KILLED = {"logit-in-group-kill-b-kafka", "logit-in-group-kill-b-redpanda"}

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
    bad_exits = {leg: code for leg, code in codes.items()
                 if code != ("137" if leg in KILLED else "0")}
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
