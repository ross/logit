# The `hostagents` producer: collectd and Telegraf, each from its own official distribution in
# DEFAULT configuration, driving `logit` over every wire this repo has a listener for -- collectd's
# binary protocol, carbon plaintext (twice, once per agent), a Prometheus scrape, and OTLP/gRPC.
# See tools/shape-survey/configs/hostagents.yaml's header for the wire map and why the collectd leg
# is this survey's only live measurement of metrics-per-event, and
# tools/shape-survey/hostagents/{collectd,telegraf}/ for the agent images/config.
#
# Sourced by script/shape-survey, which discovers this file by glob -- everything specific to this
# producer lives here, under tools/shape-survey/hostagents/, and in
# tools/shape-survey/configs/hostagents*.yaml. See tools/shape-survey/README.md's "Adding a
# producer".
#
# TWO RUNS, ONE PAIR OF LONG-LIVED AGENTS
#
# collectd and Telegraf start once and run for the whole producer; `logit` restarts once in the
# middle, wire.yaml/main.yaml only differing in which side of the accumulator a batch is measured
# from (hostagents.yaml's header has the full reasoning):
#
#   start collectd, telegraf (pushing/exposing against a not-yet-up "logit" -- their first flush
#   may be lost, same as any agent started before its collector)
#   start_logit hostagents.yaml             (default batching)
#   capture SHAPE_SURVEY_DURATION (default 600s)
#   stop_logit; summarize
#   start_logit hostagents-wire-grouped.yaml (batch_max_events: 1 on the accumulator-fronted legs)
#   capture SHAPE_SURVEY_HOSTAGENTS_WIRE_DURATION (default 180s -- this run exists to look at
#     batch shape, not to accumulate a second full-length capture)
#   stop_logit; summarize

SHAPE_SURVEY_HOSTAGENTS_DURATION_DEFAULT=600
SHAPE_SURVEY_HOSTAGENTS_WIRE_DURATION_DEFAULT=180

# Builds the collectd image (tools/shape-survey/hostagents/collectd/) and echoes its tag. Not
# tracked by survey_image/SHAPE_SURVEY_SKIP_IMAGE -- that machinery is for the one shared
# `logit:shape-survey` tag two concurrent producers race on; this is a small, producer-local image
# nobody else touches, so it just rebuilds (fast once Docker's own layer cache is warm).
survey_hostagents_collectd_image() {
    local img="shape-survey-hostagents-collectd:local"
    ${DOCKER} build -t "${img}" "${ROOT}/tools/shape-survey/hostagents/collectd" >/dev/null
    echo "${img}"
}

# Starts collectd and Telegraf as long-lived services under this producer's network, and appends
# both agents' versions and image identities to the CURRENT ${SURVEY_RUN_DIR}/provenance.txt
# (survey_start_service already does the image half; this adds the version numbers the
# representativeness line quotes). Called once; both configs' pipelines read from the same two
# containers.
survey_hostagents_services() {
    local collectd_img
    collectd_img="$(survey_hostagents_collectd_image)"

    # No readiness probe the image has a binary for (no procps in Debian's collectd install) --
    # collectd runs as PID 1 in the foreground (`collectd -f`, the Dockerfile's CMD), so
    # /proc/1/comm is the cheapest positive proof it is that process and not a crash loop.
    survey_start_service collectd --ready-cmd 'test "$(cat /proc/1/comm)" = collectd' -- \
        "${collectd_img}"

    # The official image's own /etc/telegraf/telegraf.conf (default [agent] + default-enabled
    # inputs) plus this producer's outputs-only drop-in, merged via --config-directory rather than
    # a custom image -- see tools/shape-survey/hostagents/telegraf/outputs.conf's header.
    survey_start_service telegraf --ready-http http://telegraf:9273/metrics -- \
        -v "${ROOT}/tools/shape-survey/hostagents/telegraf/outputs.conf:/etc/telegraf/telegraf.d/outputs.conf:ro,z" \
        telegraf:1.32-alpine \
        telegraf --config /etc/telegraf/telegraf.conf --config-directory /etc/telegraf/telegraf.d

    {
        echo "collectd package version: $(${DOCKER} exec "$(survey_container_name collectd)" \
            sh -c "dpkg -s collectd | grep '^Version:'")"
        echo "telegraf version: $(${DOCKER} exec "$(survey_container_name telegraf)" telegraf --version)"
        echo "telegraf default-enabled inputs (confirmed against the image's own shipped" \
            "/etc/telegraf/telegraf.conf, not assumed): $(${DOCKER} exec "$(survey_container_name telegraf)" \
            sh -c "grep -E '^\[\[inputs\.' /etc/telegraf/telegraf.conf" | tr -d '[]' | tr '\n' ' ')"
    } >>"${SURVEY_RUN_DIR}/provenance.txt"
}

# Both agents' version/image identity, re-appended to whichever run directory is current --
# survey_start_service/the block above only wrote them into the FIRST run directory
# (survey_hostagents_services runs once; survey_out_dir runs twice). Called again before the
# second (wire-grouped) run so its provenance.txt is self-contained too.
survey_hostagents_reprovenance() {
    {
        echo "collectd package version: $(${DOCKER} exec "$(survey_container_name collectd)" \
            sh -c "dpkg -s collectd | grep '^Version:'")"
        echo "telegraf version: $(${DOCKER} exec "$(survey_container_name telegraf)" telegraf --version)"
        echo "(same collectd/telegraf processes as the default-batching run in this producer's" \
            "other run directory -- they were not restarted between the two logit runs)"
    } >>"${SURVEY_RUN_DIR}/provenance.txt"
}

# This producer's own section of summary.md (tools/shape-survey/README.md's "Your own summary
# section"), shared by both runs -- reads only /out/summary.json, never shape.log.
survey_hostagents_section_py() {
    cat <<'PYEOF'
#!/usr/bin/env python3
"""Renders the `hostagents` producer's section of summary.md from /out/summary.json.

The headline: `logit.shape.metrics` on the collectd leg is this survey's only live measurement of
metrics-per-event, because every other input here (and every metrics input in this repo except
collectd_in) emits one metric per event.
"""

import json
import pathlib

SUMMARY = json.loads(pathlib.Path("/out/summary.json").read_text())

SOURCES = [
    ("collectd_network", "collectd's `network` plugin -- collectd binary, UDP 25826"),
    ("graphite_collectd", "collectd's `write_graphite` plugin -- carbon plaintext, TCP"),
    ("graphite_telegraf", "Telegraf's `outputs.graphite` -- carbon plaintext, TCP"),
    ("prometheus_telegraf", "Telegraf's `outputs.prometheus_client`, scraped"),
    ("otlp_telegraf", "Telegraf's `outputs.opentelemetry` -- OTLP/gRPC"),
]


def rows(section, metric=None):
    for row in SUMMARY.get(section) or []:
        if metric is None or row.get("metric") == metric:
            yield row


def by_source(section, metric=None):
    return {row["source"]: row for row in rows(section, metric) if row.get("source")}


def hist(stats):
    return {int(k): v for k, v in (stats.get("histogram") or {}).items()}


def fraction_above(stats, n):
    h = hist(stats)
    total = sum(h.values())
    return sum(c for v, c in h.items() if v > n) / total if total else None


def num(value):
    if value is None:
        return "n/a"
    return f"{value:.0f}" if float(value).is_integer() else f"{value:.2f}"


def pct(value):
    return "n/a" if value is None else f"{value:.1%}"


def table(histogram, limit=12):
    items = sorted(histogram.items())
    head = items[:limit]
    cells = ", ".join(f"{v}&nbsp;x&nbsp;{c}" for v, c in head)
    if len(items) > limit:
        rest = sum(c for _, c in items[limit:])
        cells += f", (>{head[-1][0]})&nbsp;x&nbsp;{rest}"
    return cells or "-"


metrics = by_source("distributions", "logit.shape.metrics")
attrs = by_source("distributions", "logit.shape.attributes")
batches = by_source("distributions", "logit.shape.batch.events")
keys = by_source("distributions", "logit.shape.key_bytes")
values = by_source("distributions", "logit.shape.value_bytes")
gauges = {row["tap"]: row for row in rows("gauges")}

out = []

out.append("## `logit.shape.metrics`: collectd is the only live measurement of metrics-per-event")
out.append("")
out.append(
    "Every other leg below emits one metric per event by construction --"
    " `logit.shape.metrics` is a constant `1`. `collectd_network` is the exception: it decodes one"
    " event per collectd *value list*, N metric records in wire order for an N-data-source type"
    " (`load` 3, `if_octets`/`disk_octets` 2, most types 1 -- docs/adr/collectd-binary-relay.md)."
    " The table below is this producer's headline number."
)
out.append("")
out.append("| source | events | value -> count (metrics per event) | events with >1 metric |")
out.append("|---|--:|---|--:|")
for source, label in SOURCES:
    row = metrics.get(source)
    if not row:
        continue
    s = row["stats"]
    h = hist(s)
    total = sum(h.values())
    gt1 = sum(c for v, c in h.items() if v > 1)
    out.append(
        f"| `{source}` ({label}) | {s['count']} | {table(h)} |"
        f" {pct(gt1 / total) if total else 'n/a'} |"
    )
out.append("")

out.append("## Attributes per event")
out.append("")
out.append(
    "collectd's identity fields (`collectd.host`, `collectd.plugin`, `collectd.plugin_instance`,"
    " `collectd.type`, `collectd.type_instance`) and Telegraf's own tags land as ordinary event"
    " attributes -- this is the width a `keep`/interner sizing decision on this leg would see."
)
out.append("")
out.append("| source | events | p50 | p90 | max | >4 | >8 | >12 | >16 | value -> count |")
out.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|---|")
for source, label in SOURCES:
    row = attrs.get(source)
    if not row:
        continue
    s = row["stats"]
    out.append(
        f"| `{source}` | {s['count']} | {num(s.get('p50'))} | {num(s.get('p90'))} |"
        f" {num(s.get('max'))} | {pct(fraction_above(s, 4))} | {pct(fraction_above(s, 8))} |"
        f" {pct(fraction_above(s, 12))} | {pct(fraction_above(s, 16))} | {table(hist(s))} |"
    )
out.append("")

out.append("## Key/value bytes")
out.append("")
out.append("| source | key p50 | p90 | max | value p50 | p90 | max |")
out.append("|---|--:|--:|--:|--:|--:|--:|")
for source, label in SOURCES:
    k, v = keys.get(source), values.get(source)
    if not k and not v:
        continue
    ks = (k or {}).get("stats", {})
    vs = (v or {}).get("stats", {})
    out.append(
        f"| `{source}` | {num(ks.get('p50'))} | {num(ks.get('p90'))} | {num(ks.get('max'))} |"
        f" {num(vs.get('p50'))} | {num(vs.get('p90'))} | {num(vs.get('max'))} |"
    )
out.append("")

out.append("## Batch events, and batch resource/scope attributes")
out.append("")
out.append(
    "`prometheus_telegraf` and `otlp_telegraf` bypass the accumulator in every run, so their"
    " batches are always the wire's own (one scrape response; one OTLP export request -- and for"
    " `otlp_telegraf`, this is also \"events per Telegraf flush per output\", since one flush is"
    " one export request). The other three read as the accumulator's regrouping in"
    " hostagents.yaml and as the wire's own grouping (one datagram, one carbon line) in"
    " hostagents-wire-grouped.yaml -- compare the two run directories' tables here rather than"
    " reading either alone."
)
out.append("")
out.append("| source | batches | events/batch p50 | p90 | max | value -> count |")
out.append("|---|--:|--:|--:|--:|---|")
for source, label in SOURCES:
    row = batches.get(source)
    if not row:
        continue
    s = row["stats"]
    out.append(
        f"| `{source}` | {s['count']} | {num(s.get('p50'))} | {num(s.get('p90'))} |"
        f" {num(s.get('max'))} | {table(hist(s), limit=8)} |"
    )
out.append("")

out.append("## Distinct keys and key-sets, per source")
out.append("")
out.append(
    "Cumulative since process start, per **component** (one `shape` per source, so this stays"
    " separable) -- `keyset_share.top1`/`top5` is the share of that source's events carried by its"
    " most common one/five attribute-key sets."
)
out.append("")
out.append("| source | distinct keys | distinct key-sets | top1 share | top5 share | overflow |")
out.append("|---|--:|--:|--:|--:|--:|")
for source, label in SOURCES:
    tap = f"tap_{source}"
    if tap not in gauges:
        continue

    def gauge(metric, tap=tap):
        for g in rows("gauges"):
            if g["tap"] == tap and g["metric"] == metric:
                return g["value"]
        return None

    out.append(
        f"| `{source}` | {num(gauge('logit.shape.distinct_keys'))} |"
        f" {num(gauge('logit.shape.distinct_keysets'))} |"
        f" {pct(gauge('logit.shape.keyset_share.top1'))} |"
        f" {pct(gauge('logit.shape.keyset_share.top5'))} |"
        f" {num(gauge('logit.shape.tracking_overflow'))} |"
    )
out.append("")

out.append(
    "**Series/device counts here are a floor, not a typical.** One idle container is one host's"
    " worth of block devices, filesystems and network interfaces (fewer than a real host, same"
    " caveat exporters.sh records for node_exporter) -- the *label/attribute structure* each agent"
    " produces is not a floor, it is a property of that agent's own default plugin/input set."
)
out.append("")

pathlib.Path("/out/section.md").write_text("\n".join(out) + "\n")
print("hostagents: wrote /out/section.md")
PYEOF
}

# Runs one logit config against the already-running collectd/telegraf pair for `duration` seconds,
# into a freshly created run directory, and writes that directory's provenance + representativeness
# + summary (with this producer's section appended). `first` (1/0) selects whether
# survey_hostagents_services (which also appends the agents' versions to provenance.txt) has
# already run -- the wire-grouped run instead calls survey_hostagents_reprovenance, since the
# agents are not restarted between the two.
survey_hostagents_run() {
    local config="$1" duration="$2" representativeness="$3" note="$4" first="$5"
    local run_dir

    survey_out_dir hostagents
    run_dir="${SURVEY_RUN_DIR}"

    survey_provenance hostagents "${representativeness}"
    echo "${note}" >>"${run_dir}/provenance.txt"

    if [ "${first}" -eq 1 ]; then
        start_logit "${config}"
        survey_hostagents_services
    else
        survey_hostagents_reprovenance
        start_logit "${config}"
    fi

    survey_capture_for "${duration}"
    stop_logit

    survey_summarize >/dev/null
    survey_hostagents_section_py >"${run_dir}/section.py"
    survey_python section -- python3 /out/section.py
    survey_summarize --append section.md
}

survey_hostagents() {
    local main_duration wire_duration
    main_duration="${SHAPE_SURVEY_DURATION:-${SHAPE_SURVEY_HOSTAGENTS_DURATION_DEFAULT}}"
    wire_duration="${SHAPE_SURVEY_HOSTAGENTS_WIRE_DURATION:-${SHAPE_SURVEY_HOSTAGENTS_WIRE_DURATION_DEFAULT}}"

    survey_hostagents_run \
        "${ROOT}/tools/shape-survey/configs/hostagents.yaml" \
        "${main_duration}" \
        "collectd and Telegraf with their distro/default plugin sets inside containers on an idle host -- agent packing and identity structure; device/filesystem counts, and so series counts, are a floor (default-batching run: logit.shape.batch.events is the accumulator's regrouping, not the wire's)" \
        "variant: default batching (accumulator regroups across datagrams/lines; see the sibling hostagents-wire-grouped run directory for the wire's own grouping)" \
        1

    # Snapshot both agents' logs from the default-batching phase before the second logit run
    # overwrites SURVEY_RUN_DIR -- survey_cleanup's automatic capture at the very end only reaches
    # whichever run directory is current then (the wire-grouped one).
    survey_service_logs collectd
    survey_service_logs telegraf

    survey_hostagents_run \
        "${ROOT}/tools/shape-survey/configs/hostagents-wire-grouped.yaml" \
        "${wire_duration}" \
        "collectd and Telegraf with their distro/default plugin sets inside containers on an idle host -- agent packing and identity structure; device/filesystem counts, and so series counts, are a floor (wire-grouping run: logit.shape.batch.events is the wire's own grouping -- one datagram, or for graphite_telegraf's TCP leg, one line)" \
        "variant: wire grouping (receive.batch_max_events: 1 on collectd_network/graphite_collectd/graphite_telegraf; prometheus_telegraf and otlp_telegraf are always wire-grouped, in both runs, since they bypass the accumulator)" \
        0
}
