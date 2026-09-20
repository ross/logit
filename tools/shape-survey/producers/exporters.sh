# The `exporters` producer: official Prometheus exporters, in their **default** configuration,
# scraped by `prometheus_in` and measured by `shape`.
#
# Sourced by script/shape-survey, which discovers this file by glob -- everything specific to this
# producer lives here and in tools/shape-survey/configs/exporters.yaml (read that file's header
# first: it is where the mapping from `logit.shape.*` to "labels per series" and "series per
# scrape" is written down).
#
# ---------------------------------------------------------------------------------------------
# WHAT THESE NUMBERS ARE WORTH
#
# Good evidence: the **label and series structure** each exporter produces -- how many labels a
# series carries, how long a label name and a label value are, how many distinct key-sets one
# exporter's exposition contains, how many series arrive in one scrape response. That structure is
# a property of the exporter's own metric definitions, and it does not change with load. Nobody
# publishes it: of the exporters here, only node_exporter commits an exposition fixture at all.
#
# Not evidence: **series counts in a real estate.** Every service below is a single idle instance
# with no data in it -- one empty Postgres database, a Redis with no keyspaces, an nginx serving
# one location, a node_exporter seeing a container's view of the host rather than a host's. Every
# per-object family (per database, per table, per keyspace, per device, per filesystem, per vhost)
# is therefore at its smallest possible value. Series per scrape here is a **floor**.
#
# Nor is it evidence about how an application's own instrumentation looks: `go_runtime` below is a
# stock `client_golang` default registry, which is what a Go service exposes *before* anybody adds
# an application metric. It is labelled as exactly that.
# ---------------------------------------------------------------------------------------------

#: How long the scrape capture runs, in seconds. The config scrapes every 5s and `prometheus_in`
#: swallows its first immediate tick, so 70s is >=13 scrapes per target -- comfortably past the
#: plan's "10 scrapes" box. `SHAPE_SURVEY_DURATION=180 script/shape-survey exporters` for longer.
SHAPE_SURVEY_EXPORTERS_DURATION_DEFAULT=70

#: `nginx` (open source) exposes nothing to Prometheus by itself: `stub_status` is a module you
#: turn on, and nginx-prometheus-exporter reads it. That makes this the one target whose subject is
#: configured rather than default -- but `stub_status` *is* the open-source exporter's only input,
#: so the exporter itself is still in its default configuration, and its output shape is what any
#: open-source nginx yields. (NGINX Plus's API would be a bigger, different shape; noted in
#: provenance.)
survey_exporters_nginx_conf() {
    cat <<'EOF'
server {
    listen 80;
    server_name _;
    location / {
        return 200 "shape-survey\n";
    }
    location /stub_status {
        stub_status;
    }
}
EOF
}

# Every service this producer starts, and the readiness check that proves it is up. `<suffix>` is
# also the container's network alias, which is the hostname configs/exporters.yaml scrapes.
survey_exporters_services() {
    local run_dir="${SURVEY_RUN_DIR}"

    # The subjects: something real for each exporter to export.
    survey_start_service postgres --ready-cmd 'pg_isready -U postgres -q' -- \
        -e POSTGRES_PASSWORD=shape-survey -e POSTGRES_USER=postgres \
        postgres:17-alpine

    survey_start_service redis --ready-cmd 'redis-cli ping' -- redis:8-alpine

    survey_start_service nginx --ready-http http://nginx/stub_status -- \
        -v "${run_dir}/nginx.conf:/etc/nginx/conf.d/default.conf:ro,z" \
        nginx:1.29-alpine

    # The exporters, each with default flags. Only the pointer at what to watch is configured --
    # that is the one thing an exporter cannot default.
    survey_start_service node-exporter --ready-http http://node-exporter:9100/metrics -- \
        prom/node-exporter:latest

    survey_start_service postgres-exporter --ready-http http://postgres-exporter:9187/metrics -- \
        -e "DATA_SOURCE_NAME=postgresql://postgres:shape-survey@postgres:5432/postgres?sslmode=disable" \
        quay.io/prometheuscommunity/postgres-exporter:latest

    survey_start_service redis-exporter --ready-http http://redis-exporter:9121/metrics -- \
        -e "REDIS_ADDR=redis://redis:6379" \
        oliver006/redis_exporter:latest

    survey_start_service nginx-exporter --ready-http http://nginx-exporter:9113/metrics -- \
        nginx/nginx-prometheus-exporter:1.4.2 --nginx.scrape-uri=http://nginx:80/stub_status

    survey_start_service blackbox-exporter --ready-http http://blackbox-exporter:9115/metrics -- \
        prom/blackbox-exporter:latest
}

# The producer's own section of summary.md, generated into the run directory and run there rather
# than committed beside this file: it is this producer's reading of its own numbers, it reads only
# `summary.json` (never shape.log -- summarize.py's value->count tables are exact), and keeping it
# here keeps the producer one file, the way demo.sh generates its config.
survey_exporters_section_py() {
    cat <<'PYEOF'
#!/usr/bin/env python3
"""Renders the `exporters` producer's section of summary.md from /out/summary.json.

Everything here is a statement about *Prometheus exposition*, which is why it is not in
summarize.py: at a scrape-mode `prometheus_in` tap, `logit.shape.attributes` is labels per series
and `logit.shape.batch.events` is series per scrape.
"""

import json
import pathlib

SUMMARY = json.loads(pathlib.Path("/out/summary.json").read_text())

# What each `prometheus_in` component was actually pointed at, for the table's first column.
SUBJECTS = {
    "node_exporter": "prom/node-exporter, container's view of the host",
    "postgres_exporter": "postgres-exporter -> one idle, empty postgres",
    "redis_exporter": "redis_exporter -> one idle redis, no keyspaces",
    "nginx_exporter": "nginx-prometheus-exporter -> open-source stub_status",
    "blackbox_probe": "blackbox_exporter /probe?module=http_2xx (one HTTP probe of nginx)",
    "go_runtime": "blackbox_exporter's own /metrics: a stock client_golang registry",
}


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


def median(values):
    ordered = sorted(values)
    return ordered[(len(ordered) - 1) // 2] if ordered else None


def num(value):
    if value is None:
        return "n/a"
    return f"{value:.0f}" if float(value).is_integer() else f"{value:.2f}"


def pct(value):
    return "n/a" if value is None else f"{value:.1%}"


def table(histogram, limit=12):
    """A value->count table as `n x count` cells, longest tail folded into a final cell."""
    items = sorted(histogram.items())
    head = items[:limit]
    cells = ", ".join(f"{v}&nbsp;x&nbsp;{c}" for v, c in head)
    if len(items) > limit:
        rest = sum(c for _, c in items[limit:])
        cells += f", (>{head[-1][0]})&nbsp;x&nbsp;{rest}"
    return cells or "-"


attrs = by_source("distributions", "logit.shape.attributes")
batches = by_source("distributions", "logit.shape.batch.events")
keys = by_source("distributions", "logit.shape.key_bytes")
values = by_source("distributions", "logit.shape.value_bytes")
keysets = by_source("distributions", "logit.shape.batch.keysets")
gauges = {row["tap"]: row for row in rows("gauges")}

out = []
out.append("## Per exporter: labels per series, series per scrape")
out.append("")
out.append(
    "At a scrape-mode `prometheus_in` tap these series read as Prometheus, not as `logit`:"
    " **`logit.shape.attributes` is labels per series** and **`logit.shape.batch.events` is series"
    " per scrape** (one exporter's whole `/metrics` response is one batch). `instance` and"
    " `prometheus.target` are on the *resource*, which this tap drops, so a row's attribute count"
    " is the exposition line's own labels plus `prometheus.type` where the family's wire type has"
    " no distinct model kind (`unknown`/`info`/`stateset`/`gaugehistogram`) -- read it as **wire"
    " labels + 0 or 1**."
)
out.append("")
out.append("| exporter | what it watches | scrapes | series/scrape min | median | max |")
out.append("|---|---|--:|--:|--:|--:|")
for source in SUBJECTS:
    batch = batches.get(source)
    if not batch:
        continue
    h = hist(batch["stats"])
    samples = [v for v, c in h.items() for _ in range(c)]
    out.append(
        f"| `{source}` | {SUBJECTS[source]} | {batch['stats']['count']} |"
        f" {num(batch['stats']['min'])} | {num(median(samples))} | {num(batch['stats']['max'])} |"
    )
out.append("")

out.append("### Labels per series")
out.append("")
out.append("| exporter | series | p50 | p90 | max | >4 labels | >8 labels | value->count |")
out.append("|---|--:|--:|--:|--:|--:|--:|---|")
for source in SUBJECTS:
    row = attrs.get(source)
    if not row:
        continue
    s = row["stats"]
    out.append(
        f"| `{source}` | {s['count']} | {num(s['p50'])} | {num(s['p90'])} | {num(s['max'])} |"
        f" {pct(fraction_above(s, 4))} | {pct(fraction_above(s, 8))} | {table(hist(s))} |"
    )
out.append("")

out.append("### Label names and label values, in bytes")
out.append("")
out.append(
    "`key_bytes` is a label *name*'s length and `value_bytes` a label *value*'s -- the two numbers"
    " an interned-key or inline-string decision turns on."
)
out.append("")
out.append("| exporter | names p50 | p90 | max | values p50 | p90 | max |")
out.append("|---|--:|--:|--:|--:|--:|--:|")
for source in SUBJECTS:
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

out.append("### Distinct label names and label-name sets, per exporter")
out.append("")
out.append(
    "Cumulative over the whole capture, and per `shape` **component** -- which is why this config"
    " runs one tap per exporter (see configs/exporters.yaml). `keyset_share.top1`/`top5` are the"
    " share of that exporter's series carried by its most common one and five label-name sets:"
    " a high top5 means an exposition dominated by a few repeated label shapes, which is what a"
    " shared-key layout would exploit. `batch.keysets` is distinct label-name sets *within a"
    " single scrape response*."
)
out.append("")
out.append("| exporter | distinct names | distinct name-sets | top1 share | top5 share | keysets/scrape p50 | max | overflow |")
out.append("|---|--:|--:|--:|--:|--:|--:|--:|")
for source in SUBJECTS:
    tap = f"tap_{source}"
    row = gauges.get(tap)
    if not row:
        continue

    def gauge(metric, tap=tap):
        for g in rows("gauges"):
            if g["tap"] == tap and g["metric"] == metric:
                return g["value"]
        return None

    ks = keysets.get(source, {}).get("stats", {})
    out.append(
        f"| `{source}` | {num(gauge('logit.shape.distinct_keys'))} |"
        f" {num(gauge('logit.shape.distinct_keysets'))} |"
        f" {pct(gauge('logit.shape.keyset_share.top1'))} |"
        f" {pct(gauge('logit.shape.keyset_share.top5'))} |"
        f" {num(ks.get('p50'))} | {num(ks.get('max'))} |"
        f" {num(gauge('logit.shape.tracking_overflow'))} |"
    )
out.append("")
out.append(
    "**Series counts above are a floor.** Every subject is a single idle instance with nothing in"
    " it, so every per-object family (per database, per table, per keyspace, per device, per"
    " filesystem, per vhost) is at its smallest possible value. The *label structure* is not a"
    " floor: it is a property of the exporter's own metric definitions and does not move with load."
)
out.append("")

pathlib.Path("/out/section.md").write_text("\n".join(out) + "\n")
print("exporters: wrote /out/section.md")
PYEOF
}

survey_exporters() {
    local run_dir config duration

    survey_out_dir exporters
    run_dir="${SURVEY_RUN_DIR}"
    config="${ROOT}/tools/shape-survey/configs/exporters.yaml"
    duration="${SHAPE_SURVEY_DURATION:-${SHAPE_SURVEY_EXPORTERS_DURATION_DEFAULT}}"

    survey_provenance exporters \
        "official exporter images in default configuration against idle single-instance services -- label/series structure per exporter; series counts scale with real object counts and are a floor"
    {
        echo "duration: ${duration}s at a 5s scrape interval (>=10 scrapes per target)"
        echo "config: tools/shape-survey/configs/exporters.yaml (one prometheus_in per target)"
        echo "subjects: one idle instance each -- an empty postgres, a redis with no keyspaces,"
        echo "  an nginx serving one location. node-exporter sees a CONTAINER's view of the host"
        echo "  (fewer block devices, filesystems and interfaces than a real host), so its series"
        echo "  count is a floor even by this capture's standards; its label structure is not."
        echo "nginx: stub_status is enabled by a generated nginx.conf in this directory -- the"
        echo "  open-source exporter's only possible input. NGINX Plus's API would be a larger,"
        echo "  different shape, and is not measured here."
        echo "cadvisor: NOT captured. It was tried with read-only /, /sys and /var/lib/docker"
        echo "  mounts and dies at startup on this daemon:"
        echo "    F cadvisor.go:173] Failed to start manager: inotify_add_watch /sys/fs/cgroup:"
        echo "    permission denied"
        echo "  Getting past that needs --privileged (or an SELinux label / seccomp exemption),"
        echo "  which is more than this harness should ask of a shared daemon, so the target is"
        echo "  skipped rather than run in a configuration nobody would call default."
    } >>"${run_dir}/provenance.txt"

    survey_exporters_nginx_conf >"${run_dir}/nginx.conf"
    chmod 644 "${run_dir}/nginx.conf"
    survey_exporters_services

    start_logit "${config}"
    survey_capture_for "${duration}"
    stop_logit

    # summary.json first; then this producer's own reading of it, appended to summary.md.
    survey_summarize >/dev/null
    survey_exporters_section_py >"${run_dir}/section.py"
    survey_python section -- python3 /out/section.py
    survey_summarize --append section.md
}
