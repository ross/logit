# The `hostagents` producer: collectd and Telegraf, each from its official distribution in default
# configuration, over five wires at once: collectd binary, carbon plaintext from each agent, a
# Telegraf Prometheus scrape, and Telegraf OTLP/gRPC. configs/hostagents.yaml's header has the
# wire map; tools/shape-survey/hostagents/ has the agent image and config.
#
# Representativeness and caveats: README "Producers" and "Caveats each author recorded".
#
# Two runs against one pair of long-lived agents; only `logit` restarts in between, and the two
# configs differ only in which side of the accumulator a batch is measured from:
#
#   start_logit hostagents.yaml, then start collectd and telegraf     (default batching)
#   capture SHAPE_SURVEY_DURATION (default 600s); stop_logit; summarize
#   start_logit hostagents-wire-grouped.yaml   (batch_max_events: 1 on accumulator-fronted legs)
#   capture SHAPE_SURVEY_HOSTAGENTS_WIRE_DURATION (default 180s: it measures batch shape only)
#   stop_logit; summarize
#
# Between the two runs no `logit` container exists, so whatever the agents push then is lost and
# outside either capture.

SHAPE_SURVEY_HOSTAGENTS_DURATION_DEFAULT=600
SHAPE_SURVEY_HOSTAGENTS_WIRE_DURATION_DEFAULT=180

# Builds the collectd image and echoes its tag. SHAPE_SURVEY_SKIP_IMAGE doesn't apply: this image
# is producer-local, so no concurrent run races on it, and it rebuilds from the layer cache.
survey_hostagents_collectd_image() {
    local img="shape-survey-hostagents-collectd:local"
    ${DOCKER} build -t "${img}" "${ROOT}/tools/shape-survey/hostagents/collectd" >/dev/null
    echo "${img}"
}

# Starts collectd and Telegraf once for both runs, and appends their versions to the current
# run's provenance.txt (`survey_start_service` already appends the images).
survey_hostagents_services() {
    local collectd_img
    collectd_img="$(survey_hostagents_collectd_image)"

    # The image has no procps; collectd runs as PID 1 in the foreground (the Dockerfile's CMD), so
    # /proc/1/comm proves it is running and not crash-looping.
    survey_start_service collectd --ready-cmd 'test "$(cat /proc/1/comm)" = collectd' -- \
        "${collectd_img}"

    # The official image's own telegraf.conf plus this producer's outputs-only drop-in, merged via
    # --config-directory rather than a custom image (see hostagents/telegraf/outputs.conf).
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

# Re-appends both agents' versions for the second run, whose provenance.txt would otherwise lack
# them: the agents start once, but `survey_out_dir` runs twice.
survey_hostagents_reprovenance() {
    {
        echo "collectd package version: $(${DOCKER} exec "$(survey_container_name collectd)" \
            sh -c "dpkg -s collectd | grep '^Version:'")"
        echo "telegraf version: $(${DOCKER} exec "$(survey_container_name telegraf)" telegraf --version)"
        echo "(same collectd/telegraf processes as the default-batching run in this producer's" \
            "other run directory -- they were not restarted between the two logit runs)"
    } >>"${SURVEY_RUN_DIR}/provenance.txt"
}

# This producer's own summary.md section, shared by both runs, computed from summary.json alone.
survey_hostagents_section_py() {
    cat <<'PYEOF'
#!/usr/bin/env python3
"""Renders the `hostagents` producer's section of summary.md from /out/summary.json.

`logit.shape.metrics` on the collectd leg is the survey's only live measurement of
metrics-per-event: every other metrics input in this repo emits one metric per event.
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

# Runs one logit config for `duration` seconds into a new run directory, with its provenance and
# summary. `first` is 1 to start the agents after `logit`, or 0 when they're already running and
# only their versions need re-appending.
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

    # Snapshot the agents' logs into the first run directory now: cleanup's automatic capture
    # reaches only the run directory current at the end.
    survey_service_logs collectd
    survey_service_logs telegraf

    survey_hostagents_run \
        "${ROOT}/tools/shape-survey/configs/hostagents-wire-grouped.yaml" \
        "${wire_duration}" \
        "collectd and Telegraf with their distro/default plugin sets inside containers on an idle host -- agent packing and identity structure; device/filesystem counts, and so series counts, are a floor (wire-grouping run: logit.shape.batch.events is the wire's own grouping -- one datagram, or for graphite_telegraf's TCP leg, one line)" \
        "variant: wire grouping (receive.batch_max_events: 1 on collectd_network/graphite_collectd/graphite_telegraf; prometheus_telegraf and otlp_telegraf are always wire-grouped, in both runs, since they bypass the accumulator)" \
        0
}
