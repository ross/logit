#!/usr/bin/env python3
"""Judges each leg of `script/s3-interop` from the files the run left behind.

Runs as `python3 -I /tools/check.py` in the tools image (tools/s3-interop/Dockerfile.tools),
stdlib only apart from the stock `gzip` and `zstd` binaries it shells out to, after the legs have
exited and the script has fetched every object. It queries nothing. Under /out (the run
directory) the script has written:

    meta.txt                              `target=`, `run_prefix=`, and `stores=` lines
    fetch/<store>/listing.json            `list-objects-v2` over the run prefix
    fetch/<store>/late-listing.json       the same in the late bucket
    fetch/<store>/objects/<key>           every object, fetched with the AWS CLI
    fetch/<store>/late-objects/<key>      the late bucket's objects
    fetch/<store>/head/<leg>.{json,err}   `head-object` of the leg's first object
    fetch/<store>/sse-probe.status        `ok` or `failed`: the AWS CLI's PutObject with the sse
                                          leg's header, as the reference for the sse row
    fetch/<store>/sse-probe.err           that request's status line, the error body's
                                          `<Code>`, and the CLI's error line, from its `--debug`
                                          log
    replay/<store>/out/replay-<n>.ndjson  the `native` objects, decompressed and read back by
                                          `replay_in` into `stdio_out` `format: json`
    <leg>-<store>-telemetry.log           the leg's `internal` telemetry and warnings, one JSON
                                          object a line
    {creds,bucket,sse}-<store>-phase1-telemetry.log  a copy taken 20 s in, while a refused leg
                                          still holds and before the fault was cleared
    creds2-<store>-telemetry.log          the second `creds` run
    late-created-<store>                  present when the script created the late bucket
    exits.txt                             `<container> <exit code>` per leg
    logs/<container>.log                  each container's log

Prints one row per leg and store (tools/s3-interop/README.md, "What it runs"): PASS (the leg did
what that table says it checks), GAP (the store behaves differently from AWS S3 in a way the row
records), or FAIL. Writes the same rows to /out/results.md. Exits 1 on any FAIL.
"""

import json
import pathlib
import re
import subprocess
import sys

OUT = pathlib.Path("/out")

EVENTS = {"json-gzip": 1000, "zstd": 1000, "none": 1000, "native": 1000, "vhost": 500,
          "sse": 200, "creds": 200, "bucket": 200}
EXTENSIONS = {"json-gzip": "ndjson.gz", "zstd": "ndjson.zst", "none": "ndjson",
              "native": "logit.gz", "vhost": "ndjson.gz", "sse": "ndjson.gz",
              "creds": "ndjson.gz", "bucket": "ndjson.gz"}
CONTENT_TYPES = {"ndjson.gz": "application/gzip", "ndjson.zst": "application/zstd",
                 "ndjson": "application/x-ndjson", "logit.gz": "application/gzip"}


def meta():
    values = dict(line.split("=", 1) for line in (OUT / "meta.txt").read_text().splitlines()
                  if "=" in line)
    return values["target"], values["run_prefix"], values["stores"].split()


# ---- telemetry ---------------------------------------------------------------------------------


def telemetry(name):
    path = OUT / f"{name}-telemetry.log"
    if not path.exists():
        return []
    events = []
    for line in path.read_text(errors="replace").splitlines():
        if line.strip():
            try:
                events.append(json.loads(line))
            except ValueError:
                pass
    return events


def _matches(event, attrs):
    have = {**event.get("resource", {}).get("attributes", {}), **event.get("attributes", {})}
    return all(have.get(k) == v for k, v in attrs.items())


def telemetry_sum(events, metric, **attrs):
    total = 0.0
    for event in events:
        if not _matches(event, attrs):
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
        if not _matches(event, attrs):
            continue
        for item in event.get("metrics", []):
            if item.get("name") == metric and item.get("kind") == "gauge":
                best = max(best, float(item["value"]))
    return int(best)


def dropped(events):
    """Every nonzero `*.dropped` counter in the telemetry, as `name{reason=r}`: n."""
    found = {}
    for event in events:
        attrs = event.get("attributes", {})
        for item in event.get("metrics", []):
            name = item.get("name", "")
            if name.endswith(".dropped") and item.get("kind") == "sum" and float(item["value"]):
                key = f"{name}{{reason={attrs.get('reason', '?')}}}"
                found[key] = found.get(key, 0) + int(float(item["value"]))
    return found


def requests(events):
    out = {}
    for cls in ("2xx", "3xx", "4xx", "5xx", "connect_error", "network_error"):
        n = telemetry_sum(events, "logit.output.requests", component="archive", op="put_object",
                          **{"class": cls})
        if n:
            out[cls] = n
    return out


def shown(counts):
    return ", ".join(f"{k} {v}" for k, v in counts.items()) or "none"


def refusals(events):
    """The store's answers the sink quoted in its warn-level self-logs, as `<status> <Code>:
    <Message>`, each once."""
    found = []
    for event in events:
        log = event.get("log")
        text = str(log.get("message", "")) if isinstance(log, dict) else ""
        match = re.search(r"failed \((\d{3})[^)]*\) (\w+): ?(.*?)(?:; retrying|$)", text)
        if match:
            answer = f"{match.group(1)} {match.group(2)}: {match.group(3)}".rstrip(": ")
            if answer not in found:
                found.append(answer)
    return found


# ---- objects -----------------------------------------------------------------------------------


def listing(store, name="listing.json"):
    path = OUT / "fetch" / store / name
    try:
        return json.loads(path.read_text()).get("Contents", [])
    except (OSError, ValueError):
        return []


def head(store, leg):
    path = OUT / "fetch" / store / "head" / f"{leg}.json"
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        err = OUT / "fetch" / store / "head" / f"{leg}.err"
        return {"error": err.read_text(errors="replace").strip() if err.exists() else "none"}


def keys_for(items, prefix):
    return sorted(i["Key"] for i in items if i["Key"].startswith(prefix))


def key_pattern(prefix, extension):
    return re.compile(
        re.escape(prefix) + r"dt=\d{4}-\d{2}-\d{2}/hour=\d{2}/"
        r"\d{8}T\d{6}\.\d{3}Z-[0-9a-f]+-\d{20}\." + re.escape(extension) + "$")


def tool(args, path):
    result = subprocess.run(args + [str(path)], capture_output=True)
    return result.returncode, result.stdout, result.stderr.decode(errors="replace").strip()


def read_events(store, key, extension, objects="objects"):
    """The decoded NDJSON events of an object, after the reference decompressor's integrity test.
    Returns (events, error)."""
    path = OUT / "fetch" / store / objects / key
    if not path.exists():
        return [], f"{key} was listed but not fetched"
    if extension.endswith(".gz"):
        code, _, err = tool(["gzip", "-t"], path)
        if code:
            return [], f"gzip -t {key}: {err}"
        _, data, _ = tool(["gzip", "-dc"], path)
    elif extension.endswith(".zst"):
        code, _, err = tool(["zstd", "-t", "-q"], path)
        if code:
            return [], f"zstd -t {key}: {err}"
        _, data, _ = tool(["zstd", "-dc", "-q"], path)
    else:
        data = path.read_bytes()
    events = []
    for line in data.decode(errors="replace").splitlines():
        if line.strip():
            try:
                events.append(json.loads(line))
            except ValueError:
                return [], f"{key} holds a line that isn't JSON"
    return events, None


def seq(event):
    return int(event["attributes"]["seq"])


def judge_events(events, want, label):
    """None when `events` hold seq 0..want-1 once each, else the failure's text."""
    seqs = [seq(e) for e in events]
    missing = set(range(want)) - set(seqs)
    extra = len(seqs) - len(set(seqs))
    if len(seqs) != want or missing or extra:
        return (f"{label} hold {len(seqs)} events, want {want}: {len(missing)} missing, "
                f"{extra} duplicated")
    return None


# ---- legs --------------------------------------------------------------------------------------


def exits():
    path = OUT / "exits.txt"
    if not path.exists():
        return {}
    return dict(line.split() for line in path.read_text().splitlines() if line.strip())


def leg_objects(store, leg, run_prefix):
    """The checks every object-writing leg shares: key form, content type, integrity, and no
    `*.dropped`. Returns (verdict, detail, events)."""
    extension = EXTENSIONS[leg]
    prefix = f"{run_prefix}{leg}/"
    keys = keys_for(listing(store), prefix)
    events = telemetry(f"{leg}-{store}")
    tail = f"requests {shown(requests(events))}"
    if not keys:
        return "FAIL", f"no object under {prefix}; {tail}", []
    pattern = key_pattern(prefix, extension)
    bad = [k for k in keys if not pattern.match(k)]
    if bad:
        return "FAIL", f"key {bad[0]} isn't the documented form; {tail}", []
    decoded = []
    for key in keys:
        got, error = read_events(store, key, extension)
        if error:
            return "FAIL", error, []
        decoded.extend(got)
    info = head(store, leg)
    if "error" in info:
        return "FAIL", f"head-object failed: {info['error'][:160]}", decoded
    if info.get("ContentType") != CONTENT_TYPES[extension]:
        return "FAIL", f"Content-Type is {info.get('ContentType')}, want {CONTENT_TYPES[extension]}", decoded
    if info.get("ContentEncoding"):
        return "FAIL", f"Content-Encoding is {info['ContentEncoding']}, want none", decoded
    lost = dropped(events)
    if lost:
        return "FAIL", f"nonzero {shown(lost)}", decoded
    return "PASS", f"{len(keys)} object(s), {len(decoded)} events, .{extension}; {tail}", decoded


def leg_plain(store, leg, run_prefix):
    verdict, detail, decoded = leg_objects(store, leg, run_prefix)
    if verdict != "PASS":
        return verdict, detail
    problem = judge_events(decoded, EVENTS[leg], "the objects")
    if problem:
        return "FAIL", f"{problem}; {detail}"
    return "PASS", f"{detail}, each seq once"


def leg_native(store, run_prefix):
    prefix = f"{run_prefix}native/"
    keys = keys_for(listing(store), prefix)
    events = telemetry(f"native-{store}")
    if not keys:
        return "FAIL", f"no object under {prefix}; requests {shown(requests(events))}"
    pattern = key_pattern(prefix, "logit.gz")
    if [k for k in keys if not pattern.match(k)]:
        return "FAIL", "a key isn't the documented form"
    for key in keys:
        code, _, err = tool(["gzip", "-t"], OUT / "fetch" / store / "objects" / key)
        if code:
            return "FAIL", f"gzip -t {key}: {err}"
    replays = sorted((OUT / "replay" / store / "out").glob("replay-*.ndjson"))
    if len(replays) != len(keys):
        return "FAIL", f"{len(keys)} object(s) but {len(replays)} replay output(s)"
    replayed = []
    for path in replays:
        for line in path.read_text(errors="replace").splitlines():
            if line.strip():
                replayed.append(json.loads(line))
    problem = judge_events(replayed, EVENTS["native"], "the replayed objects")
    if problem:
        return "FAIL", problem
    lost = dropped(events)
    if lost:
        return "FAIL", f"nonzero {shown(lost)}"
    return "PASS", (f"{len(keys)} object(s) decompressed with gzip, {len(replayed)} events read "
                    f"back through replay_in, each seq once; requests {shown(requests(events))}")


def probe_answer(store):
    """The SSE probe's outcome: (`ok`, `failed`, or `missing`, `(status, code)` or None). The
    status comes from the request's urllib3 line, the code from the response body's `<Code>`, else
    the CLI's error line: the CLI can fail to parse an error body it then logs."""
    base = OUT / "fetch" / store
    status_file = base / "sse-probe.status"
    outcome = status_file.read_text().strip() if status_file.exists() else "missing"
    err_file = base / "sse-probe.err"
    text = err_file.read_text(errors="replace") if err_file.exists() else ""
    statuses = re.findall(r'"PUT [^"]*" (\d{3}) ', text)
    code = (re.search(r"<Code>(\w+)</Code>", text)
            or re.search(r"An error occurred \((\w+)\)", text))
    if statuses and code:
        return outcome, (statuses[-1], code.group(1))
    return outcome, None


def leg_sse(store, run_prefix, target):
    keys = keys_for(listing(store), f"{run_prefix}sse/")
    if keys:
        info = head(store, "sse")
        if info.get("ServerSideEncryption") != "AES256":
            return "FAIL", (f"stored, but head-object reports ServerSideEncryption "
                            f"{info.get('ServerSideEncryption')}")
        verdict, detail = leg_plain(store, "sse", run_prefix)
        if verdict != "PASS":
            return verdict, detail
        # S3 encrypts every new object with SSE-S3 by default, so AES256 on the sse leg's object
        # proves the header only where an object written without it reads otherwise.
        if target == "aws" and head(store, "json-gzip").get("ServerSideEncryption") == "AES256":
            return "GAP", (f"{detail}; head-object reports AES256, but so does the json-gzip "
                           f"leg's object, written with no SSE header: the bucket encrypts by "
                           f"default, so this row can't tell whether the sink sent the header")
        return "PASS", f"{detail}; head-object reports ServerSideEncryption AES256"
    # Nothing stored. That is the store's limit only when the AWS CLI's own PutObject with the
    # header got the same refusal, and the sink was refused, never accepted, and dropped nothing.
    # The verdict reads the copy taken while the sink held: at SIGTERM a held queue with no
    # `buffer.disk:` is dropped, counted `reason=shutdown`, which says nothing about the refusal.
    events = telemetry(f"sse-{store}-phase1")
    classes = requests(events)
    answers = refusals(events)
    lost = dropped(events)
    outcome, probe = probe_answer(store)
    sink = f"requests {shown(classes)}; the sink's answers: {' | '.join(answers) or 'none'}"
    if outcome != "failed" or probe is None:
        read = f" ({' '.join(probe) if probe else 'no status and code read'})"
        return "FAIL", (f"the sink stored nothing under sse: aes256, but the AWS CLI's own "
                        f"PutObject with the header gave '{outcome}'"
                        f"{read if outcome == 'failed' else ''}; {sink}")
    refused = {tuple(a.split(":", 1)[0].split(" ", 1)) for a in answers}
    if probe not in refused:
        return "FAIL", (f"the AWS CLI's PutObject was refused {' '.join(probe)}, but the sink "
                        f"wasn't refused the same way; {sink}")
    if classes.get("2xx") or not classes.get("4xx"):
        return "FAIL", f"the sink's requests aren't all refusals; {sink}"
    if lost:
        return "FAIL", f"the refusal dropped data: {shown(lost)}; {sink}"
    return "GAP", (f"nothing stored under sse: aes256 and nothing dropped while held; phase 1 "
                   f"{sink}; the AWS CLI's PutObject with the same header is refused "
                   f"{' '.join(probe)} too")


def leg_vhost(store, run_prefix, target):
    verdict, detail = leg_plain(store, "vhost", run_prefix)
    if verdict == "PASS" and target == "aws":
        # Every AWS leg runs virtual-hosted, and S3's regional endpoints still answer path-style
        # requests, so a sink that sent one would pass here too.
        return "GAP", (f"{detail}; AWS accepts path-style requests, so only the local run checks "
                       f"the addressing")
    return verdict, detail


def refusal_phase(name, store):
    """The shared first half of the refused-credentials and missing-bucket legs: the sink held,
    retried, and dropped nothing. Returns (failure or None, detail)."""
    phase1 = telemetry(f"{name}-{store}-phase1")
    classes = requests(phase1)
    retrying = telemetry_max(phase1, "logit.component.retrying", component="archive")
    lost = dropped(phase1)
    detail = (f"phase 1 requests {shown(classes)}, retrying peaked at {retrying}, "
              f"dropped {shown(lost)}")
    if classes.get("2xx"):
        return f"the store took the write; {detail}", detail
    if not classes.get("4xx"):
        return f"no refused attempt, so the refusal is unproven; {detail}", detail
    if retrying != 1:
        return f"the sink wasn't holding and retrying; {detail}", detail
    if lost:
        return f"the refusal dropped data; {detail}", detail
    return None, detail


def leg_creds(store, run_prefix):
    failure, detail = refusal_phase("creds", store)
    if failure:
        return "FAIL", failure
    keys = keys_for(listing(store), f"{run_prefix}creds/")
    if not keys:
        return "FAIL", f"nothing was delivered after the credentials were fixed; {detail}"
    decoded = []
    for key in keys:
        got, error = read_events(store, key, "ndjson.gz")
        if error:
            return "FAIL", error
        decoded.extend(got)
    for run in ("run1", "run2"):
        part = [e for e in decoded if str(e["attributes"].get("run")) == run]
        problem = judge_events(part, EVENTS["creds"], f"run {run}'s events")
        if problem:
            return "FAIL", f"{problem}; {detail}"
    lost = dropped(telemetry(f"creds2-{store}"))
    if lost:
        return "FAIL", f"nonzero {shown(lost)} after the restart"
    return "PASS", (f"refused, held, and retried with no drop; after the restart {len(decoded)} "
                    f"events in {len(keys)} object(s): the spool's run1 and the new run2, each "
                    f"seq once; {detail}")


def leg_bucket(store, run_prefix):
    if not (OUT / f"late-created-{store}").exists():
        return "GAP", "the script couldn't create the late bucket on this store"
    failure, detail = refusal_phase("bucket", store)
    if failure:
        return "FAIL", failure
    keys = keys_for(listing(store, "late-listing.json"), f"{run_prefix}bucket/")
    if not keys:
        return "FAIL", f"nothing drained after the bucket was created; {detail}"
    decoded = []
    for key in keys:
        got, error = read_events(store, key, "ndjson.gz", "late-objects")
        if error:
            return "FAIL", error
        decoded.extend(got)
    problem = judge_events(decoded, EVENTS["bucket"], "the late bucket's objects")
    if problem:
        return "FAIL", f"{problem}; {detail}"
    lost = dropped(telemetry(f"bucket-{store}"))
    if lost:
        return "FAIL", f"nonzero {shown(lost)}"
    return "PASS", (f"held and retried with no drop; once the bucket existed {len(decoded)} "
                    f"events in {len(keys)} object(s), each seq once; {detail}")


def main():
    target, run_prefix, stores = meta()
    exit_codes = exits()
    legs = [
        ("json + gzip (default)", lambda s: leg_plain(s, "json-gzip", run_prefix)),
        ("json + zstd", lambda s: leg_plain(s, "zstd", run_prefix)),
        ("json, no compression", lambda s: leg_plain(s, "none", run_prefix)),
        ("native format, replayed", lambda s: leg_native(s, run_prefix)),
        ("virtual-hosted addressing", lambda s: leg_vhost(s, run_prefix, target)),
        ("SSE-S3 header", lambda s: leg_sse(s, run_prefix, target)),
        ("refused credentials", lambda s: leg_creds(s, run_prefix)),
        ("missing bucket", lambda s: leg_bucket(s, run_prefix)),
    ]
    rows = []
    for store in stores:
        for title, judge in legs:
            try:
                verdict, detail = judge(store)
            except Exception as err:  # a malformed artifact fails its leg instead of the run
                verdict, detail = "FAIL", f"check.py could not read the leg's files: {err!r}"
            rows.append((title, store, verdict, detail))
        bad = {n: c for n, c in exit_codes.items() if n.endswith(f"-{store}") and c != "0"}
        if bad:
            rows.append(("process exits", store, "FAIL", f"nonzero exit: {bad}"))
        else:
            rows.append(("process exits", store, "PASS", "every leg exited 0 on SIGTERM"))
    lines = [f"# s3-interop results ({target})", "", "| Leg | Store | Result | Detail |",
             "|---|---|---|---|"]
    for title, store, verdict, detail in rows:
        lines.append(f"| {title} | {store} | `{verdict}` | {detail} |")
    text = "\n".join(lines) + "\n"
    (OUT / "results.md").write_text(text)
    print(text)
    return 1 if any(r[2] == "FAIL" for r in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
