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
    collector/<broker>/<name>.jsonl     what the Collector decoded, one OTLP/JSON request a line
    exits.txt                           `<service> <exit code>` per leg and Collector
    redpanda-oidc.txt                   `ok` once Redpanda took OAUTHBEARER, else `failed`
    logs/<service>.log                  each service's log

Prints one row per leg and broker (docs/plans/kafka-output.md, "Interop plan (W4)"). A row is
PASS (the leg did what the plan expects), GAP (the broker behaves differently from Apache Kafka in
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
    """`(partition, key, size)` per record of a topic read back without values: size is kcat's,
    and the key is kcat's too, so a null key reads as the empty string."""
    path = OUT / "readback" / broker / f"{topic}.kcat.tsv"
    if not path.exists():
        return None
    records = []
    for line in path.read_text(errors="replace").splitlines():
        if line.strip():
            partition, _offset, _timestamp, key, size = line.split("\t")
            records.append((int(partition), key, int(size)))
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


def leg_otlp_proto(broker):
    problem = collector_problem(broker, "proto")
    classes, dropped = sink_summary(broker, "proto")
    counts = {}
    for signal, topic in (("logs", "otlp_logs"), ("metrics", "otlp_metrics"),
                          ("spans", "otlp_spans")):
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


def leg_otlp_keyed(broker):
    problem = collector_problem(broker, "keyed")
    classes, dropped = sink_summary(broker, "keyed")
    if problem:
        return "FAIL", problem
    if dropped or not classes.get("ok"):
        return "FAIL", f"requests {shown(classes)}, records.dropped {dropped}"
    wanted = {otlp_trace_id(seq) for seq in range(OTLP_TICKS)}
    summary = []
    for signal, topic, traces in (("spans", "keyed-spans", wanted),
                                  ("logs", "keyed-logs",
                                   {otlp_trace_id(s) for s in range(OTLP_TICKS)
                                    if otlp_log_has_trace(s)})):
        kcat = read_record_meta(broker, topic)
        java = java_record_keys(broker, topic)
        if kcat is None or java is None or not kcat:
            return "FAIL", f"{topic} wasn't read back"
        if sorted((p, k) for p, k, _ in kcat) != sorted(java):
            return "FAIL", f"{topic}: kcat and the Java consumer disagree on keys or partitions"
        placed = {}
        unkeyed = 0
        for partition, key, _size in kcat:
            if key == "":
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
    metrics = read_record_meta(broker, "keyed-metrics") or []
    if len(metrics) != OTLP_BATCHES or any(key for _, key, _ in metrics):
        return "FAIL", f"keyed-metrics holds {len(metrics)} records, want {OTLP_BATCHES} unkeyed"
    return "PASS", ("keys are the trace id in 32 lowercase hex characters, on the Java "
                    f"partitioner's partition; {'; '.join(summary)}; keyed-metrics "
                    f"{len(metrics)} unkeyed")


def exporter_placement(broker, topic):
    records = read_record_meta(broker, topic)
    if not records:
        return None
    placed = {}
    for partition, key, _size in records:
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
        largest = max([largest] + [size for _, _, size in records])
    if any(n <= OTLP_BATCHES for n in counts.values()):
        return "FAIL", (f"a batch's request wasn't split into several records: {counts} for "
                        f"{OTLP_BATCHES} batches")
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
