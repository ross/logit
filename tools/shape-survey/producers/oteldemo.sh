# The `oteldemo` producer: the **OpenTelemetry Demo** ("astronomy shop",
# github.com/open-telemetry/opentelemetry-demo), run at a pinned release tag under its own Locust
# load generator, exporting through **its own OpenTelemetry Collector** into a `logit` `otlp_in`.
#
# Sourced by script/shape-survey, which discovers this file by glob. Everything specific to this
# producer lives here, in tools/shape-survey/configs/oteldemo.yaml (read that file's header: it is
# where the mapping from `logit.shape.*` to OTLP's own vocabulary is written down) and in
# tools/shape-survey/oteldemo/ (two small override files, below).
#
# **The demo is fetched at run time, never vendored.** `survey_oteldemo_fetch` shallow-clones the
# pinned tag into the run directory, and the commit it lands on is asserted against the SHA pinned
# below -- a tag is a movable ref, and a survey that silently measured a different tree than its
# provenance claims is worse than one that fails. Nothing of the demo's enters this repository;
# what is committed here is two files totalling a few dozen lines.
#
# ---------------------------------------------------------------------------------------------
# WHAT THESE NUMBERS ARE WORTH
#
# Good evidence, and the best in this harness for OTLP: **what real OpenTelemetry SDK
# auto-instrumentation emits, across nine languages at once, after a real collector**. Every
# attribute on every span, metric point and log record below was put there by an OTel SDK, an
# instrumentation library, or a collector processor -- none of it by this repository. That is the
# one thing `demo` (whose formats we partly authored) cannot say, and no recorded fixture corpus
# can say at this breadth.
#
# Not evidence: **traffic mix, or what an operator's own pipeline looks like**. This is a demo
# application. Every instrumentation it ships is enabled at once, on a service graph built to show
# OpenTelemetry off rather than to serve anybody's customers; the load is one synthetic Locust
# generator walking a fixed set of user journeys; and there is no operator-added context at all --
# no team/owner/tier/environment attributes, no per-tenant labels, none of the enrichment a real
# estate's collector adds before a gateway sees the data. Read the attribute widths as **"what the
# SDKs and the demo's own collector produce"**, which is a floor an operator adds to, not a
# typical.
#
# The post-collector position is the other half of the caveat, and it is deliberate: a `logit`
# receiving OTLP is receiving it from something, and in practice that something is a collector. The
# producer's appended section of summary.md names every processor in the demo's pipelines at this
# tag, so a reader knows exactly whose attributes are in the resource-width column.
# ---------------------------------------------------------------------------------------------

#: The released tag measured, and the commit that tag pointed at when this producer was written.
#: Both go in provenance.txt; the clone is asserted against the SHA, so a moved tag fails the run
#: rather than quietly measuring something else.
SHAPE_SURVEY_OTELDEMO_TAG=3.1.0
SHAPE_SURVEY_OTELDEMO_SHA=dedc0178918e260823323b8d95005a8cb924b007

#: How long the capture runs, in seconds, after the stack reports healthy. 20 minutes by default.
#: `SHAPE_SURVEY_DURATION=300 script/shape-survey oteldemo` for a verification run -- the load
#: generator is steady from its first cycle, so a short run is a smaller sample of the same thing.
SHAPE_SURVEY_OTELDEMO_DURATION_DEFAULT=1200

#: The demo's own `.env` value for `DEMO_VERSION` is `latest`, which is a moving tag. The survey
#: pins it to the release being measured instead -- the same images, named so they cannot drift
#: mid-capture or between runs. This and `OTEL_COLLECTOR_CONFIG_EXTRAS` are the **only** two
#: values of the demo's `.env` this producer overrides.
SHAPE_SURVEY_OTELDEMO_IMAGE_TAG="${SHAPE_SURVEY_OTELDEMO_TAG}"

# Shallow-clones the pinned tag into the run directory and checks the commit. `--depth 1
# --branch <tag>` is one tag's tree and nothing else (~40 MB), which is what keeps "fetch it at run
# time" cheaper than vendoring it would have been.
survey_oteldemo_fetch() {
    local dir="$1" sha
    echo "shape-survey: cloning opentelemetry-demo ${SHAPE_SURVEY_OTELDEMO_TAG} into ${dir}"
    git clone --quiet --depth 1 --branch "${SHAPE_SURVEY_OTELDEMO_TAG}" \
        https://github.com/open-telemetry/opentelemetry-demo.git "${dir}" ||
        survey_fail "could not clone opentelemetry-demo ${SHAPE_SURVEY_OTELDEMO_TAG}"
    sha="$(git -C "${dir}" rev-parse HEAD)"
    [ "${sha}" = "${SHAPE_SURVEY_OTELDEMO_SHA}" ] ||
        survey_fail "opentelemetry-demo ${SHAPE_SURVEY_OTELDEMO_TAG} is now ${sha}, not the pinned" \
            "${SHAPE_SURVEY_OTELDEMO_SHA}. A release tag moved: re-read the diff and update" \
            "SHAPE_SURVEY_OTELDEMO_SHA (and this producer's notes) rather than measuring a tree" \
            "the provenance misdescribes."

    # SELinux: this daemon runs with the `selinux` security option, and the demo's compose files
    # bind-mount their config with no `:z` (nothing upstream would -- they are not written for a
    # labelled host). Without a relabel every one of those mounts is `Permission denied` inside the
    # container and the collector never starts. `chcon` on our own freshly-cloned copy is the
    # narrowest fix available: it touches nothing outside this run directory, needs no privilege,
    # and changes no file's content. A host without SELinux no-ops here.
    if command -v chcon >/dev/null 2>&1 && [ "$(getenforce 2>/dev/null || echo Disabled)" != "Disabled" ]; then
        chcon -R -t container_file_t "${dir}" ||
            echo "shape-survey: WARNING -- chcon failed on ${dir}; expect the demo's bind mounts" \
                "to be denied inside their containers" >&2
    fi
}

# The demo's `.env`, plus this survey's two overrides, as a second `--env-file`. Compose applies
# multiple `--env-file`s in order with the later winning -- the same layering the demo's own
# Makefile uses for `.env.override`. Passing `--env-file` at all disables compose's automatic `.env`
# pickup, which is why the demo's own file is named explicitly first.
survey_oteldemo_env() {
    cat <<EOF
# Generated by tools/shape-survey/producers/oteldemo.sh -- layered over the demo's own .env.

# Pin the image tag: the demo ships DEMO_VERSION=latest, which can move between two runs of this
# survey (and, for a long capture, underneath one).
DEMO_VERSION=${SHAPE_SURVEY_OTELDEMO_IMAGE_TAG}

# The demo's own documented collector extension point, pointed at this survey's extras layer. The
# upstream file at this path is an empty stub whose header says to override it; the core compose
# file already loads it last. See tools/shape-survey/oteldemo/otelcol-config-extras.yml.
OTEL_COLLECTOR_CONFIG_EXTRAS=${ROOT}/tools/shape-survey/oteldemo/otelcol-config-extras.yml

# Read by tools/shape-survey/oteldemo/compose-overlay.yaml, so the overlay never hard-codes
# lib.sh's naming scheme.
SHAPE_SURVEY_NET=${SURVEY_NET}
EOF
}

# The stack, under this run's own compose project. `survey_compose` does the namespacing, the
# "somebody else's demo stack is already up" guard (the demo gives every service a fixed
# `container_name` and its network a fixed name, so only one can exist on a host) and the teardown
# registration -- see lib.sh.
#
# **`compose.yaml` alone -- the demo's own "core/minimal" layer**, per its header: "Core/minimal
# demo services. Run alone for the smallest footprint." It is the right one here on both counts the
# brief cares about. It has every application service and the load generator, so the polyglot SDK
# coverage this producer exists for is complete (Java, .NET, Go, C++, Ruby, TypeScript, JavaScript,
# Python, PHP, Rust, plus Envoy and flagd). What the other layers add is not producers:
# `compose.full.yaml` adds the Kafka group (accounting, fraud-detection) and
# `compose.observability.yaml` adds the demo's own *backends* -- Jaeger, Prometheus, Grafana,
# OpenSearch, OpAMP -- which store the telemetry rather than emit it, and whose OpenSearch alone is
# a JVM heavyweight. Running them would cost several GB of RAM to measure nothing extra.
survey_oteldemo_compose() {
    survey_compose stack \
        -f "${SURVEY_RUN_DIR}/opentelemetry-demo/compose.yaml" \
        -f "${ROOT}/tools/shape-survey/oteldemo/compose-overlay.yaml" \
        --env-file "${SURVEY_RUN_DIR}/opentelemetry-demo/.env" \
        --env-file "${SURVEY_RUN_DIR}/survey.env" -- "$@"
}

# The readiness condition `survey_capture_until` polls, in two parts because one of them is not
# enough. The load generator healthy means the application chain behind it is up (its own
# healthcheck waits on `frontend`, which waits on the rest) -- but the load generator does not
# depend on the collector at all, so it goes healthy just as happily while the collector is
# crash-looping, and the capture would then run its full window against a dead exporter and only
# discover it at `stop_logit`'s empty-shape.log check. So the collector's own container state is
# checked too: `running`, not `restarting`.
survey_oteldemo_ready() {
    survey_oteldemo_service_state load-generator health = healthy &&
        survey_oteldemo_service_state otel-collector state = running
}

# `<service> health|state = <expected>`: one compose service's container state, or the empty string
# if compose has not created it yet. A `{{.State.Health.Status}}` on a container with no
# healthcheck renders empty, which is why the two are separate lookups rather than one.
survey_oteldemo_service_state() {
    local service="$1" what="$2" _eq="$3" expected="$4" cid format
    case "${what}" in
    health) format='{{.State.Health.Status}}' ;;
    state) format='{{.State.Status}}' ;;
    *) survey_fail "survey_oteldemo_service_state: unknown field '${what}'" ;;
    esac
    cid="$(survey_oteldemo_compose ps -q "${service}" 2>/dev/null || true)"
    [ -n "${cid}" ] || return 1
    [ "$(${DOCKER} inspect --format "${format}" "${cid}" 2>/dev/null || true)" = "${expected}" ]
}

# The producer's own section of summary.md, generated into the run directory and run there rather
# than committed beside this file -- this producer's reading of its own numbers, computed from
# `summary.json` alone (never re-parsing shape.log: summarize.py's value->count tables are exact).
survey_oteldemo_section_py() {
    cat <<'PYEOF'
#!/usr/bin/env python3
"""Renders the `oteldemo` producer's section of summary.md from /out/summary.json.

Everything here is a statement about **OTLP after a collector**, which is why it is not in
summarize.py. The per-event series carry `shape`'s `signal` tag, so traces, metrics and logs
separate inside the one `otlp_in` component; the per-batch series carry no `signal` at all (a
batch is a (Resource, Scope) group and can hold any mix), so they are reported once.
"""

import json
import pathlib

SUMMARY = json.loads(pathlib.Path("/out/summary.json").read_text())

#: Which collector processors ran, per pipeline, at the measured tag -- read off
#: src/otel-collector/otelcol-config.yml's `service.pipelines`. This is the list that says whose
#: attributes are in the resource-width row below, and it is written out rather than inferred so a
#: reader never has to go and look.
PROCESSORS = {
    "traces": [
        "resource_detection",
        "memory_limiter",
        "transform/sanitize_spans",
        "gen_ai_normalizer",
        "transform/redact_sensitive_data",
        "redaction",
    ],
    "metrics": ["resource_detection", "memory_limiter"],
    "logs": ["resource_detection", "memory_limiter", "transform/sanitize_logs"],
}

#: Which receivers fed each pipeline. The metrics one matters: only a minority of the metric points
#: measured here came from an application SDK.
RECEIVERS = {
    "traces": ["otlp"],
    "metrics": [
        "http_check/frontend-proxy",
        "host_metrics",
        "nginx",
        "otlp",
        "redis",
        "postgresql",
        "prometheus/ad",
        "span_metrics (connector)",
        "~~docker_stats~~ (removed, see below)",
    ],
    "logs": ["otlp"],
}


def rows(section, metric=None):
    for row in SUMMARY.get(section) or []:
        if metric is None or row.get("metric") == metric:
            yield row


def by_signal(metric, section="distributions"):
    return {r["signal"]: r for r in rows(section, metric)}


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


def table(histogram, limit=14):
    """A value->count table as `n x count` cells, longest tail folded into a final cell."""
    items = sorted(histogram.items())
    head = items[:limit]
    cells = ", ".join(f"{v}&nbsp;x&nbsp;{c}" for v, c in head)
    if len(items) > limit:
        rest = sum(c for _, c in items[limit:])
        cells += f", (&gt;{head[-1][0]})&nbsp;x&nbsp;{rest}"
    return cells or "-"


def dist_row(label, row):
    if not row:
        return f"| {label} | 0 | n/a | n/a | n/a | n/a |"
    s = row["stats"]
    return (
        f"| {label} | {s['count']} | {num(s['p50'])} | {num(s['p90'])} |"
        f" {num(s['p99'])} | {num(s['max'])} |"
    )


out = []

# ---- where in the chain this sits ----------------------------------------------------------------
out.append("## OTLP after a collector: what these numbers include")
out.append("")
out.append(
    "Data reaches `logit` **after** the demo's own OpenTelemetry Collector, not straight from the"
    " SDKs. The chain is `SDK -> (OTLP/gRPC) -> otel-collector -> (OTLP/HTTP) -> otlp_in -> shape`,"
    " and the collector's processors have already run. At the measured tag those are, per pipeline:"
)
out.append("")
out.append("| pipeline | receivers | processors (in order) |")
out.append("|---|---|---|")
for pipeline in ("traces", "metrics", "logs"):
    out.append(
        f"| `{pipeline}` | {', '.join('`' + r + '`' for r in RECEIVERS[pipeline])} |"
        f" {', '.join('`' + p + '`' for p in PROCESSORS[pipeline])} |"
    )
out.append("")
out.append(
    "**`resource_detection` is the one that moves a number in this summary.** It runs on all three"
    " pipelines with `detectors: [env, docker, system]` and a long `resource_attributes` allow-list"
    " (`host.arch`, `host.cpu.vendor.id`, `host.cpu.family`, `host.cpu.model.id`,"
    " `host.cpu.model.name`, `host.cpu.stepping`, `host.cpu.cache.l2.size`, `os.description`, plus"
    " the detectors' defaults), so **every** resource-attribute count reported below is the SDK's"
    " own resource *plus* that block. An operator whose collector does not run"
    " `resource_detection`, or runs it with fewer attributes enabled, will see a narrower resource;"
    " one who also runs `k8sattributes` (not present here -- this is compose, not Kubernetes) will"
    " see a wider one."
)
out.append("")
out.append(
    "The other processors are near-neutral for shape: `memory_limiter` changes nothing,"
    " `transform/sanitize_spans` rewrites span *names*, `transform/sanitize_logs` renames one scope"
    " attribute, `gen_ai_normalizer` only touches GenAI spans (the agentic layer is not running"
    " here), and `transform/redact_sensitive_data` + `redaction` delete `demo.payment.card_cvv` and"
    " swap `user.email` for `user.hash` on the few spans that carry them -- a one-for-one swap and"
    " one deletion, not a width change worth a caveat."
)
out.append("")
out.append(
    "**`docker_stats` is the one receiver removed**, and it is forced rather than chosen: on an"
    " SELinux-enforcing host it cannot connect to `/var/run/docker.sock` at all, and a receiver"
    " that fails to start takes the whole collector down with it. Everything else in the demo's"
    " collector -- every other receiver, every processor, every exporter, all three pipelines --"
    " is exactly as shipped, with one `otlphttp` exporter added alongside. What the removal costs"
    " these numbers is `container.*` metrics about the demo's own containers: collector"
    " infrastructure telemetry, not application SDK output."
)
out.append("")
out.append(
    "**A minority of the metric points here came from an application SDK.** The metrics pipeline's"
    " receiver list above is mostly collector-side scrapers (`host_metrics` especially, which"
    " produces a large, wide-ish block of `system.*` points every interval) plus the `span_metrics`"
    " connector's derived series. Only the `otlp` receiver's share is SDK output. The traces and"
    " logs pipelines have no such dilution -- their only receiver is `otlp`."
)
out.append("")
out.append(
    "**There is no `batch` processor in any pipeline at this tag** (the core config's pipelines are"
    " listed above in full). So `logit.shape.batch.events` is not a collector re-batching window:"
    " it is **one `(Resource, Scope)` group of one collector export request**. `otlp_in` bypasses"
    " the `BatchAccumulator` and the OTLP codec never collapses groups -- every"
    " `(ResourceSpans/Metrics/Logs, Scope*)` pair inside one HTTP POST becomes its own `EventBatch`"
    " (`crates/logit-proto/src/otlp/mod.rs`). Read a batch here as *one resource's one"
    " instrumentation scope's share of one export request*, which is upstream of any `logit`-side"
    " grouping and downstream of whatever the SDK's own batch span processor decided."
)
out.append("")
out.append(
    "**No per-service breakdown.** `service.name` is a resource attribute and this tap runs"
    " `resource: drop`, so nothing below can be split by service. That is the deliberate trade:"
    " `resource: drop` is what lets a summary of this run leave the environment the traffic could"
    " not, and the count that matters for OTLP -- how *wide* a resource is -- survives it."
)
out.append("")

# ---- per signal ------------------------------------------------------------------------------------
counters = {(r["metric"], r["signal"]): r["total"] for r in rows("counters")}
events = {sig: total for (metric, sig), total in counters.items() if metric == "logit.shape.events"}
widths = {r["signal"]: r for r in rows("attribute_widths")}
signals = sorted(events, key=lambda s: -events[s])

out.append("## Per signal: attributes per event")
out.append("")
out.append(
    "`signal` is `shape`'s tag for what the observed event carried, `+`-joined when an event"
    " carried more than one. Attributes here are the **event's own** -- a span's span attributes, a"
    " metric point's point attributes, a log record's log attributes -- never the resource's."
)
out.append("")
out.append("| signal | events | p50 | p90 | p99 | max | >4 | >8 | >12 | >16 |")
out.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
for sig in signals:
    row = widths.get(sig)
    if not row:
        continue
    s, f = row["stats"], row["fractions"]
    out.append(
        f"| `{sig}` | {s['count']} | {num(s['p50'])} | {num(s['p90'])} | {num(s['p99'])} |"
        f" {num(s['max'])} | {pct(f['>4'])} | {pct(f['>8'])} | {pct(f['>12'])} | {pct(f['>16'])} |"
    )
out.append("")
out.append("Exact value&rarr;count tables (the survey's real answer -- every N, not three percentiles):")
out.append("")
out.append("| signal | attributes per event |")
out.append("|---|---|")
for sig in signals:
    row = widths.get(sig)
    if row:
        out.append(f"| `{sig}` | {table(hist(row['stats']))} |")
out.append("")

# ---- spans -------------------------------------------------------------------------------------
out.append("## Spans: events, links, and span-event attributes")
out.append("")
out.append(
    "Per span, from the spans in the capture. A span event's own attributes are a separate"
    " population from the span's -- one value per span event, not per span."
)
out.append("")
out.append("| series | values | p50 | p90 | p99 | max |")
out.append("|---|--:|--:|--:|--:|--:|")
for metric, label in (
    ("logit.shape.span_events", "events per span"),
    ("logit.shape.span_links", "links per span"),
    ("logit.shape.span_event_attributes", "attributes per span event"),
):
    for sig, row in sorted(by_signal(metric).items()):
        out.append(dist_row(f"{label} (`{sig}`)", row))
for metric, label in (
    ("logit.shape.span_events", "events per span"),
    ("logit.shape.span_links", "links per span"),
    ("logit.shape.span_event_attributes", "attributes per span event"),
):
    for sig, row in sorted(by_signal(metric).items()):
        h = hist(row["stats"])
        if h:
            out.append("")
            out.append(f"`{metric}` (`{sig}`): {table(h)}")
out.append("")

# ---- nesting -----------------------------------------------------------------------------------
out.append("## Nesting: maps inside attribute values")
out.append("")
out.append(
    "`nested_maps` is how many of an event's attribute values are maps, `nested_map_width` how many"
    " entries each such map has, and `value_depth` the deepest nesting reached. OTLP's `AnyValue`"
    " can carry a `KvlistValue` at any depth, so this is the one wire format in the survey where"
    " deep nesting is cheap to produce -- what the SDKs actually do with that is the measurement."
)
out.append("")
out.append("| series | signal | values | p50 | p90 | p99 | max | value&rarr;count |")
out.append("|---|---|--:|--:|--:|--:|--:|---|")
for metric in (
    "logit.shape.nested_maps",
    "logit.shape.nested_map_width",
    "logit.shape.value_depth",
):
    for sig, row in sorted(by_signal(metric).items()):
        s = row["stats"]
        out.append(
            f"| `{metric.split('.')[-1]}` | `{sig}` | {s['count']} | {num(s['p50'])} |"
            f" {num(s['p90'])} | {num(s['p99'])} | {num(s['max'])} | {table(hist(s), limit=8)} |"
        )
out.append("")

# ---- bytes -------------------------------------------------------------------------------------
out.append("## Key and value byte lengths, and log bodies")
out.append("")
out.append(
    "`key_bytes` is an attribute *key*'s length and `value_bytes` a leaf string/bytes value's --"
    " the two numbers an interned-key or inline-string decision turns on. `body_bytes` is a log"
    " record's body."
)
out.append("")
out.append("| series | signal | values | p50 | p90 | p99 | max |")
out.append("|---|---|--:|--:|--:|--:|--:|")
for metric in ("logit.shape.key_bytes", "logit.shape.value_bytes", "logit.shape.body_bytes"):
    for sig, row in sorted(by_signal(metric).items()):
        # `dist_row`'s label is two cells here -- metric and signal -- so it lines up with the
        # seven-column header above.
        out.append(dist_row(f"`{metric.split('.')[-1]}` | `{sig}`", row))
out.append("")

# ---- value type mix ------------------------------------------------------------------------------
out.append("## Value-type mix")
out.append("")
out.append(
    "Counts of leaf values by model type, as a share of that signal's leaves -- `logit.shape.values.*`"
    " are counters, not distributions. OTLP's `AnyValue` maps onto `Value` directly, so this is as"
    " close as the survey gets to 'what types does real OTLP carry'."
)
out.append("")
TYPES = ["string", "int", "float", "bool", "bytes", "array", "map", "null", "timestamp"]
out.append("| signal | leaves | " + " | ".join(TYPES) + " |")
out.append("|---|--:|" + "--:|" * len(TYPES))
for sig in signals:
    counts = {t: counters.get((f"logit.shape.values.{t}", sig), 0) or 0 for t in TYPES}
    total = sum(counts.values())
    if not total:
        continue
    cells = " | ".join(pct(counts[t] / total) for t in TYPES)
    out.append(f"| `{sig}` | {total:.0f} | {cells} |")
out.append("")

# ---- per batch -----------------------------------------------------------------------------------
out.append("## Per batch: a (Resource, Scope) group of one collector export request")
out.append("")
out.append(
    "These carry no `signal` tag -- a batch is a `(Resource, Scope)` group and `shape` emits its"
    " per-batch measurements at `flush`, where no single signal applies. `batch.keysets` is how many"
    " distinct top-level attribute key-sets the events in one batch had between them."
)
out.append("")
out.append("| series | batches | p50 | p90 | p99 | max | value&rarr;count |")
out.append("|---|--:|--:|--:|--:|--:|---|")
for metric, label in (
    ("logit.shape.batch.events", "events per batch"),
    ("logit.shape.batch.resource_attributes", "resource attributes per batch"),
    ("logit.shape.batch.scope_attributes", "scope attributes per batch"),
    ("logit.shape.batch.keysets", "key-sets per batch"),
):
    for row in rows("distributions", metric):
        s = row["stats"]
        out.append(
            f"| {label} | {s['count']} | {num(s['p50'])} | {num(s['p90'])} | {num(s['p99'])} |"
            f" {num(s['max'])} | {table(hist(s), limit=10)} |"
        )
out.append("")

# ---- cumulative ------------------------------------------------------------------------------------
out.append("## Cumulative, since process start")
out.append("")
out.append(
    "One tap, so one set of tracking tables, covering all three signals together: `distinct_keys`"
    " is distinct **top-level attribute keys** and `distinct_keysets` distinct key-*sets*, with"
    " `top1`/`top5` the share of events carried by the most common one and five. `tracking_overflow`"
    " at `1` means a cap was hit and the two counts are floors."
)
out.append("")
gauges = {(g["tap"], g["metric"]): g["value"] for g in rows("gauges")}
taps = sorted({g["tap"] for g in rows("gauges")})
out.append("| tap | distinct keys | distinct key-sets | top1 share | top5 share | overflow |")
out.append("|---|--:|--:|--:|--:|--:|")
for tap in taps:
    out.append(
        f"| `{tap}` | {num(gauges.get((tap, 'logit.shape.distinct_keys')))} |"
        f" {num(gauges.get((tap, 'logit.shape.distinct_keysets')))} |"
        f" {pct(gauges.get((tap, 'logit.shape.keyset_share.top1')))} |"
        f" {pct(gauges.get((tap, 'logit.shape.keyset_share.top5')))} |"
        f" {num(gauges.get((tap, 'logit.shape.tracking_overflow')))} |"
    )
out.append("")
out.append(
    "**These are a demo application's numbers.** Every instrumentation the demo ships is on at"
    " once, the workload is one synthetic Locust generator walking fixed journeys, and no operator"
    " context (team, environment, tenant, owner) has been added anywhere -- which is exactly what a"
    " real deployment adds and this one does not. Treat the widths as a floor over SDK output, not"
    " as a typical."
)
out.append("")

pathlib.Path("/out/section.md").write_text("\n".join(out) + "\n")
print("oteldemo: wrote /out/section.md")
PYEOF
}

# ---- the optional `resource: keep` second run ----------------------------------------------------
#
# Off unless `SHAPE_SURVEY_OTELDEMO_KEEP=1`. A short second capture against the *same* demo stack,
# through configs/oteldemo-keep.yaml, whose tap forwards the observed `Resource` so `aggregate`
# keys every per-event series per resource and a **per-service** breakdown becomes possible.
#
# It answers the one question the main run structurally cannot: does attribute width differ by
# service -- that is, by SDK and instrumentation stack? Nine languages instrumented nine different
# ways is exactly the case a single pooled distribution hides.
#
# **Its output is identity-bearing and stays in the run directory.** Keeping the resource means
# `service.name`, `host.name`, `container.id` and everything else `resource_detection` stamped
# reach the capture, which is precisely the property `resource: drop` exists to deny. It therefore
# writes to its own `resource-keep/` subdirectory, with its own provenance saying so, and nothing
# derived from it is folded into the main summary. `perf/results/` is gitignored.
#
# Mechanically it retargets `SURVEY_RUN_DIR` for the duration -- lib.sh's `start_logit`/
# `stop_logit`/`survey_summarize` all read that one variable, so a second capture into a second
# directory needs no change there and no second copy of any of them.
#
# **It holds the whole 20-container stack up for its own window**, on top of whatever else shares
# the daemon, so it is deliberately short. On one observed run its `survey_capture_for 300` took
# 54 minutes of wall clock to spend 300 seconds of `sleep` -- three surveys and ~50 containers
# were live on the workstation at the time, and `logit` itself logged `otlp_in` handshake timeouts
# ("no first byte received within 5s") through the same stretch, so the host was genuinely starved
# rather than this loop being wrong; the identical `survey_capture_for` had run 1200s in 20m54s an
# hour earlier in the same invocation. Nothing here can prevent that, but a short window bounds
# how long it lasts when it happens.
SHAPE_SURVEY_OTELDEMO_KEEP_DURATION_DEFAULT=180

# Per-service medians, which `summarize.py` cannot produce: its series key is the metric name plus
# `shape`'s own `signal`/`source`/`tap` tags, so two services' samples collapse into one series
# however many resource attributes rode along. This walks `shape.log` itself, reusing
# summarize.py's own `parse_attrs` (its render parser is the thing under self-test; a second hand-
# rolled one would be a second thing to get wrong) and grouping on `service.name` instead.
#
# `parse_attrs` handles a `resource: keep` line's array values -- `process.command_args`, on the
# resource of every OTel SDK that detects a process, renders bare with spaces, commas and an `=`
# inside it. It did not always: that was a real parser bug, and fixing it is what let this run go
# back through `survey_summarize` at all. summarize.py's own `--self-test` now carries a
# structurally-verbatim `resource: keep` line (with neutral values) so it cannot regress.
survey_oteldemo_keep_section_py() {
    cat <<'PYEOF'
#!/usr/bin/env python3
"""Per-service attribute-count medians from a `resource: keep` capture.

Reads /out/shape.log directly, which the main run's section script deliberately does not: with
`resource: keep`, `service.name` is on each record's `attrs` line but is not one of summarize.py's
series-key tags, so summary.json has already pooled every service together by the time it is
written. The parsing itself is summarize.py's (`parse_attrs`), imported rather than re-implemented
-- its render parser is the thing under `--self-test`, including a `resource: keep` line's array
values; a second hand-rolled one here would be a second thing to get wrong.

Its output NAMES SERVICES and stays in this run directory.
"""

import importlib.util
import pathlib
import statistics
from collections import defaultdict

spec = importlib.util.spec_from_file_location("summarize", "/tools/summarize.py")
summarize = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summarize)

# (metric, signal, service) -> samples
buckets: defaultdict[tuple[str, str, str], list[float]] = defaultdict(list)
tags: dict[str, str] = {}
for raw in pathlib.Path("/out/shape.log").read_text().splitlines():
    if not raw or not raw.startswith(" "):
        tags = {}
        continue
    line = raw.strip()
    if line.startswith("attrs "):
        tags = summarize.parse_attrs(line[len("attrs ") :].strip())
        continue
    if not line.startswith("metric "):
        continue
    name, _, rendered = line[len("metric ") :].strip().partition(" ")
    if not name.startswith("logit.shape.") or not rendered.startswith("samples=["):
        continue
    values = rendered[len("samples=[") : rendered.index("]")]
    if not values:
        continue
    key = (name, tags.get("signal", ""), tags.get("service.name", "(no service.name)"))
    buckets[key].extend(float(v) for v in values.split(","))


def rows(metric):
    return sorted(
        ((sig, svc, vals) for (m, sig, svc), vals in buckets.items() if m == metric),
        key=lambda r: (r[0], -len(r[2])),
    )


out = ["# `oteldemo`, `resource: keep`: per-service attribute counts", ""]
out.append(
    "**This file names services, and is the output of a capture that deliberately let resource"
    " identity through.** It is not part of the main run's summary and does not travel with it."
    " See tools/shape-survey/configs/oteldemo-keep.yaml's header."
)
out.append("")
out.append(
    "`logit.shape.attributes` is the count of an event's **own** attributes -- a span's span"
    " attributes, a metric point's point attributes, a log record's log attributes. The resource's"
    " own width is not in this number; it is `logit.shape.batch.resource_attributes`, which a"
    " `flush`-time series cannot carry a resource on and so stays pooled."
)
out.append("")
out.append("| signal | service | events | median | p90 | max |")
out.append("|---|---|--:|--:|--:|--:|")
for sig, svc, vals in rows("logit.shape.attributes"):
    ordered = sorted(vals)
    p90 = ordered[max(0, round(0.9 * len(ordered)) - 1)] if len(ordered) >= 10 else None
    out.append(
        f"| `{sig}` | `{svc}` | {len(vals)} | {statistics.median(ordered):.0f} |"
        f" {'n/a' if p90 is None else f'{p90:.0f}'} | {max(ordered):.0f} |"
    )
out.append("")

pathlib.Path("/out/per-service.md").write_text("\n".join(out) + "\n")
print("oteldemo: wrote /out/per-service.md")
PYEOF
}

survey_oteldemo_keep_run() {
    local main_dir="${SURVEY_RUN_DIR}" keep_dir duration
    keep_dir="${main_dir}/resource-keep"
    duration="${SHAPE_SURVEY_OTELDEMO_KEEP_DURATION:-${SHAPE_SURVEY_OTELDEMO_KEEP_DURATION_DEFAULT}}"
    mkdir -p "${keep_dir}"
    # The same accommodation `survey_out_dir` makes: the logit container runs as an unprivileged
    # user whose uid has no relationship to whoever owns this checkout.
    chmod 777 "${keep_dir}"

    echo "shape-survey: second capture, resource: keep, into ${keep_dir} (${duration}s)"
    SURVEY_RUN_DIR="${keep_dir}"
    survey_provenance oteldemo \
        "OpenTelemetry Demo ${SHAPE_SURVEY_OTELDEMO_TAG}, resource: keep -- IDENTITY-BEARING per-service run, stays in the run directory; see the main capture one level up for the survey's own numbers"
    {
        echo "second run: configs/oteldemo-keep.yaml (resource: keep), same demo stack, ${duration}s"
        echo "purpose: per-service attribute-count medians, which resource: drop cannot produce"
        echo "WARNING: this capture carries service.name, host.name, container.id and everything"
        echo "  else the collector's resource_detection stamped. It does not leave this directory."
    } >>"${keep_dir}/provenance.txt"

    start_logit "${ROOT}/tools/shape-survey/configs/oteldemo-keep.yaml"
    survey_capture_for "${duration}"
    stop_logit

    # The ordinary path, like every other capture here: summary.json/summary.md for this run's own
    # pooled numbers (the series key has no room for a resource, so they are pooled across
    # services exactly as the main capture's are), and then the per-service table below, which is
    # the one thing only a `resource: keep` capture can produce.
    survey_summarize >/dev/null
    survey_oteldemo_keep_section_py >"${keep_dir}/per-service.py"
    survey_python keep-section -- python3 /out/per-service.py

    SURVEY_RUN_DIR="${main_dir}"
}

survey_oteldemo() {
    local run_dir config duration demo

    survey_out_dir oteldemo
    run_dir="${SURVEY_RUN_DIR}"
    config="${ROOT}/tools/shape-survey/configs/oteldemo.yaml"
    duration="${SHAPE_SURVEY_DURATION:-${SHAPE_SURVEY_OTELDEMO_DURATION_DEFAULT}}"
    demo="${run_dir}/opentelemetry-demo"

    survey_provenance oteldemo \
        "OpenTelemetry Demo ${SHAPE_SURVEY_OTELDEMO_TAG}: real SDK auto-instrumentation in ~10 languages under a synthetic load generator, via the demo's own collector -- a demo app: every instrumentation enabled at once, no operator-added context, no production enrichment"

    survey_oteldemo_fetch "${demo}"
    survey_oteldemo_env >"${run_dir}/survey.env"

    {
        echo "opentelemetry-demo tag: ${SHAPE_SURVEY_OTELDEMO_TAG}"
        echo "opentelemetry-demo commit: $(git -C "${demo}" rev-parse HEAD)"
        echo "collector image: $(grep -E '^COLLECTOR_CONTRIB_IMAGE=' "${demo}/.env" | cut -d= -f2-)"
        echo "demo image tag: ${SHAPE_SURVEY_OTELDEMO_IMAGE_TAG} (.env ships DEMO_VERSION=latest; pinned here)"
        echo "compose files: compose.yaml (the demo's own core/minimal layer) +"
        echo "  tools/shape-survey/oteldemo/compose-overlay.yaml"
        echo "  NOT compose.full.yaml (adds only the Kafka group) and NOT compose.observability.yaml"
        echo "  (adds Jaeger/Prometheus/Grafana/OpenSearch/OpAMP -- backends that store telemetry"
        echo "  rather than emit it, and several GB of RAM to measure nothing extra)."
        echo "collector config: the demo's own src/otel-collector/otelcol-config.yml, UNCHANGED."
        echo "  One otlphttp exporter is added through the demo's own documented extension point"
        echo "  (OTEL_COLLECTOR_CONFIG_EXTRAS -> tools/shape-survey/oteldemo/otelcol-config-extras.yml),"
        echo "  alongside the demo's own exporters on all three of its traces/metrics/logs"
        echo "  pipelines. No receiver, processor or pipeline is otherwise altered."
        echo "collector pipelines at this tag (what 'after the collector' means for these numbers):"
        echo "  traces:  otlp -> resource_detection, memory_limiter, transform/sanitize_spans,"
        echo "           gen_ai_normalizer, transform/redact_sensitive_data, redaction"
        echo "  metrics: docker_stats, http_check/frontend-proxy, host_metrics, nginx, otlp, redis,"
        echo "           postgresql, prometheus/ad, span_metrics -> resource_detection, memory_limiter"
        echo "  logs:    otlp -> resource_detection, memory_limiter, transform/sanitize_logs"
        echo "  There is NO \`batch\` processor in any of them at this tag."
        echo "docker_stats: NOT captured, and the ONE deviation from the demo's own pipelines."
        echo "  The receiver cannot start on an SELinux-enforcing host -- 'permission denied while"
        echo "  trying to connect to the docker API at unix:///var/run/docker.sock' -- and a failed"
        echo "  receiver start takes the whole collector down, so this is the difference between a"
        echo "  capture and no capture, not one missing metric family. It is removed from the"
        echo "  metrics pipeline's receiver list in the extras layer; every other receiver,"
        echo "  processor and exporter is exactly as the demo ships them. What is lost is"
        echo "  \`container.*\` metrics about the demo's own containers -- collector infrastructure"
        echo "  telemetry, not application SDK output. Making it work would mean relabelling the"
        echo "  host's /var/run/docker.sock or running the collector privileged, which is more"
        echo "  than a survey should ask of a shared daemon."
        echo "languages instrumented in the core layer: Java (ad), .NET (cart), Go (checkout,"
        echo "  product-catalog, flagd), C++ (currency), Ruby (email), TypeScript (frontend,"
        echo "  flagd-ui), JavaScript (payment), Python (recommendation, load-generator), PHP"
        echo "  (quote), Rust (shipping), plus Envoy (frontend-proxy) and nginx (image-provider)."
        echo "duration: ${duration}s of capture after the load generator reported healthy"
        echo "config: tools/shape-survey/configs/oteldemo.yaml (one otlp_in, one shape tap)"
    } >>"${run_dir}/provenance.txt"

    # logit first, so the collector's very first export has somewhere to land -- an `otlphttp`
    # exporter whose endpoint refuses connections retries, but the queue is finite and the point of
    # starting in this order is that nothing is lost while ~20 containers come up.
    start_logit "${config}"

    echo "shape-survey: bringing up the demo's core stack (this pulls ~20 images on a cold daemon)"
    survey_oteldemo_compose up -d --no-build --pull missing ||
        survey_fail "the opentelemetry-demo core stack did not come up"

    # 15 minutes is generous for a cold start with ~20 image pulls behind it.
    survey_capture_until survey_oteldemo_ready 900

    survey_capture_for "${duration}"

    # The collector's log is worth keeping whatever happens -- an export failure to `logit` shows
    # up there and nowhere else -- and it is taken *before* the check below, so the check has
    # something to point the reader at when it fails.
    survey_oteldemo_compose logs --no-color --tail 2000 otel-collector \
        >"${run_dir}/service-otel-collector.log" 2>&1 || true

    # The collector is the single point through which every measurement arrives, and it is the one
    # container in this stack that a bad config or a blocked mount takes down *after* readiness. A
    # restart mid-window means a gap the summary would not otherwise show, so it is checked rather
    # than assumed -- loudly, before anything is summarized.
    survey_oteldemo_service_state otel-collector state = running ||
        survey_fail "the demo's otel-collector is not running at the end of the capture --" \
            "see service-otel-collector.log in the run directory; every measurement in this" \
            "survey arrives through it, so a summary built on this window would be a gap"

    stop_logit

    # summary.json first; then this producer's own reading of it, appended to summary.md.
    survey_summarize >/dev/null
    survey_oteldemo_section_py >"${run_dir}/section.py"
    survey_python section -- python3 /out/section.py
    survey_summarize --append section.md

    # Opt-in, and after the main capture is fully summarized, so nothing here can cost the run its
    # own result. The stack is still up (`survey_compose`'s teardown runs at cleanup).
    #
    # `|| { ...; true; }` rather than a bare call: this is a supplementary capture, the main
    # summary is already on disk, and the survey's own rule is that a run fails loudly *about the
    # thing it was measuring*. A failure in the identity-bearing extra should be a loud warning
    # that leaves `${run_dir}` intact, not a non-zero exit that makes a completed 20-minute
    # capture look like a failed one. `SURVEY_RUN_DIR` is restored here too, because the keep run
    # retargets it and an early exit would otherwise leave it pointing at the subdirectory.
    if [ -n "${SHAPE_SURVEY_OTELDEMO_KEEP:-}" ]; then
        survey_oteldemo_keep_run || {
            SURVEY_RUN_DIR="${run_dir}"
            echo "shape-survey: WARNING -- the resource: keep second capture failed." \
                "The main capture in ${run_dir} is complete and unaffected; see" \
                "${run_dir}/resource-keep/ for how far the second one got." >&2
            true
        }
    fi
}
