# The `applogs` producer: real structured-logging libraries, at pinned versions, in their own
# documented production configuration inside tiny HTTP apps -- plus one Django app under
# OpenTelemetry auto-instrumentation, exporting OTLP straight to `logit` with no Collector in
# between.
#
# Sourced by script/shape-survey, which discovers this file by glob. Everything specific to this
# producer lives here, in tools/shape-survey/configs/applogs.yaml (read its header: it is where the
# two-tap arrangement and the per-app `shape` components are justified) and under
# tools/shape-survey/applogs/ (one directory per app: source, Dockerfile, pinned versions).
#
# ---------------------------------------------------------------------------------------------
# WHAT THESE NUMBERS ARE WORTH
#
# Good evidence: the **record structure each logging library produces** -- how many keys its own
# envelope contributes, how it nests (pino-http's `req`/`res`, semantic_logger's `payload`), how
# long its keys and values are, how many distinct key-sets one library emits across a mixed
# request stream, and what an exception does to all of that. Every library is at a pinned version
# in the configuration its own documentation prescribes, cited in each app's header comment.
#
# Not evidence: **application field counts.** structlog, python-json-logger, log/slog and zap have
# no request serializer of their own, so the *app* supplies the fields. Every app here supplies the
# same eight canonical access-log fields and nothing else -- no tenant id, no user id, no feature
# flags, no `service.version`, none of the ten or twenty context fields a real service accumulates.
# Widths here are therefore a FLOOR, and the library envelope is the part that is not.
#
# Nor is the Django leg evidence about a tuned deployment: it is SDK defaults, auto-instrumentation
# only, sqlite rather than Postgres, and Django's own development server. What it measures well is
# exactly what it claims to -- what the OpenTelemetry Python auto-instrumentation emits when nobody
# has configured anything.
# ---------------------------------------------------------------------------------------------

#: The capture window, in seconds. At the per-app rates below this is >=5k lines per app; the two
#: default-configuration streams are sampled at one request in four, so they land near 1.8k.
#: `SHAPE_SURVEY_DURATION=900 script/shape-survey applogs` for longer.
SHAPE_SURVEY_APPLOGS_DURATION_DEFAULT=300

#: Requests per second the driver aims at each logging-library app, and at Django. The apps are
#: single small containers and Django is on `runserver`; these rates keep every one of them far
#: from saturation, because a survey that measured a saturated app would be measuring the host.
SHAPE_SURVEY_APPLOGS_RATE=25
SHAPE_SURVEY_APPLOGS_DJANGO_RATE=8

# The five images, built from tools/shape-survey/applogs/<dir>/. Built here rather than pulled:
# they are this survey's own apps, pinned inside their Dockerfiles, and a rebuild is seconds once
# the base layers are cached. Named `shape-survey-applogs-*` like everything else this harness
# creates, and deliberately NOT removed at cleanup -- an image is not run state, and re-running the
# survey should not pay for five rebuilds.
survey_applogs_images() {
    local dir name
    for name in python node go ruby django; do
        dir="${ROOT}/tools/shape-survey/applogs/${name}"
        echo "shape-survey: building shape-survey-applogs-${name} from ${dir}"
        ${DOCKER} build -q -t "shape-survey-applogs-${name}:latest" "${dir}" >/dev/null ||
            survey_fail "could not build shape-survey-applogs-${name}"
    done
}

# Every app, on this producer's network under its own alias, each writing its JSON lines into the
# shared log directory the `tail_in`s follow. `--ready-http` against each app's own `/healthz`
# (`/` for Django, which has no such route) -- never a blind sleep.
survey_applogs_services() {
    local logs="${SURVEY_RUN_DIR}/applogs"

    survey_start_service structlog --ready-http http://structlog:8000/healthz -- \
        -v "${logs}:/logs:z" shape-survey-applogs-python:latest \
        python /app/app_structlog.py /logs/structlog-prod.log 8000

    survey_start_service pyjson --ready-http http://pyjson:8000/healthz -- \
        -v "${logs}:/logs:z" shape-survey-applogs-python:latest \
        python /app/app_pyjson.py /logs/pyjson-prod.log /logs/pyjson-default.log 8000

    survey_start_service pino --ready-http http://pino:8000/healthz -- \
        -v "${logs}:/logs:z" shape-survey-applogs-node:latest \
        node /app/app.js /logs/pino-http.log /logs/pino-default.log 8000

    survey_start_service goapp --ready-http http://goapp:8000/healthz -- \
        -v "${logs}:/logs:z" shape-survey-applogs-go:latest \
        /app /logs/slog-default.log /logs/zap-prod.log 8000

    survey_start_service semlog --ready-http http://semlog:8000/healthz -- \
        -v "${logs}:/logs:z" shape-survey-applogs-ruby:latest \
        ruby /app/app.rb /logs/semlog-prod.log 8000
}

# Django, under `opentelemetry-instrument`, exporting to `logit`'s own otlp_in. Started AFTER
# `start_logit` (see survey_applogs below) so its first export has somewhere to go.
survey_applogs_django() {
    #
    # The ONLY OpenTelemetry settings given are the endpoint, the service name, and the logging
    # opt-in. `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED` is the documented switch for the
    # logging instrumentation and defaults to false -- without it this leg would carry traces and
    # metrics but no logs at all, and the brief asks for all three. Everything else (protocol,
    # sampler, batch sizes, export intervals, resource detectors, semantic-convention opt-in) is
    # left at the SDK's own default, which is the whole point of this leg.
    #
    # `--ready-timeout 180` covers the migrate/seed step in entrypoint.sh.
    survey_start_service django --ready-http http://django:8000/ --ready-timeout 180 -- \
        -e SELF_BASE_URL=http://django:8000 \
        -e OTEL_SERVICE_NAME=shape-survey-django \
        -e OTEL_EXPORTER_OTLP_ENDPOINT=http://logit:4317 \
        -e OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true \
        shape-survey-applogs-django:latest
}

# The producer's own section of summary.md, generated into the run directory and run there rather
# than committed beside this file -- this producer's reading of its own numbers, computed from
# `summary.json`'s exact value->count tables and never from shape.log.
survey_applogs_section_py() {
    cat <<'PYEOF'
#!/usr/bin/env python3
"""Renders the `applogs` producer's section of summary.md from /out/summary.json."""

import json
import pathlib

SUMMARY = json.loads(pathlib.Path("/out/summary.json").read_text())

# source component -> (label, which configuration it is). The `source` tag is the ORIGIN
# component, so a post-`json` tap still reports the `tail_in`'s name.
APPS = {
    "structlog_prod_in": ("structlog 26.1.0", "documented production JSON", "tap_structlog_prod"),
    "pyjson_prod_in": ("python-json-logger 4.2.0", "documented format-string config", "tap_pyjson_prod"),
    "pyjson_default_in": ("python-json-logger 4.2.0", "BARE DEFAULT (JsonFormatter())", "tap_pyjson_default"),
    "pino_http_in": ("pino 10.3.1 + pino-http 11.0.0", "pino-http defaults (own serializers)", "tap_pino_http"),
    "pino_default_in": ("pino 10.3.1", "BARE DEFAULT (pino())", "tap_pino_default"),
    "slog_default_in": ("Go log/slog (go1.23)", "DEFAULT JSONHandler(w, nil)", "tap_slog_default"),
    "zap_prod_in": ("zap 1.28.0", "NewProductionConfig()", "tap_zap_prod"),
    "semlog_prod_in": ("semantic_logger 5.1.0", "formatter: :json", "tap_semlog_prod"),
}

DESK = {
    "django server span": "7 / 10 / 14",
    "dbapi client span": "3 / 6 / 7",
    "requests client span": "3 / 4 / 6",
    "SDK resource": "5",
}


def rows(section, **match):
    for row in SUMMARY.get(section) or []:
        if all(row.get(k) == v for k, v in match.items()):
            yield row


def one(section, **match):
    return next(iter(rows(section, **match)), None)


def hist(stats):
    return {int(k): v for k, v in (stats.get("histogram") or {}).items()}


def num(value):
    if value is None:
        return "n/a"
    return f"{value:.0f}" if float(value).is_integer() else f"{value:.2f}"


def pct(value):
    return "n/a" if value is None else f"{value:.1%}"


def table(histogram, limit=10):
    items = sorted(histogram.items())
    head = items[:limit]
    cells = ", ".join(f"{v}&nbsp;x&nbsp;{c}" for v, c in head)
    if len(items) > limit:
        rest = sum(c for _, c in items[limit:])
        cells += f", (>{head[-1][0]})&nbsp;x&nbsp;{rest}"
    return cells or "-"


def dist(metric, source, tap, signal=None):
    match = {"metric": metric, "source": source, "tap": tap}
    if signal is not None:
        match["signal"] = signal
    row = one("distributions", **match)
    return (row or {}).get("stats", {})


def stats_cells(stats):
    return f"{num(stats.get('p50'))} | {num(stats.get('p90'))} | {num(stats.get('max'))}"


out = []
out.append("## `applogs`: real logging libraries, and one auto-instrumented Django")
out.append("")
out.append(
    "Every row below is one library at a pinned version in the configuration its own"
    " documentation prescribes (cited in each app's source header), logging one JSON line per"
    " request from a tiny HTTP app under the same synthetic request mix. **The eight"
    " access-log fields the app supplies are identical across structlog, python-json-logger,"
    " log/slog, zap and semantic_logger** -- so the width difference between those rows is the"
    " library's own envelope, and the absolute width is a floor (no application would stop at"
    " eight fields). `pino-http` is the exception and the interesting one: its *library* decides"
    " the fields."
)
out.append("")

out.append("### Attributes per event, at both taps")
out.append("")
out.append(
    "`tap_input` is the raw line straight off `tail_in` (a `LogRecord` with a long body and the"
    " tailer's own file attributes); the second tap is after a `json` stage has merged the line's"
    " top-level fields into attributes. The difference between the two columns is what parsing"
    " that library's line costs an operator in attribute width."
)
out.append("")
out.append("| library | configuration | events | p50 | p90 | max | >4 | >8 | >12 | >16 | value->count |")
out.append("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|---|")
for source, (label, config, tap) in APPS.items():
    row = one("attribute_widths", source=source, tap=tap)
    if not row:
        continue
    s, f = row["stats"], row["fractions"]
    out.append(
        f"| {label} | {config} | {s['count']} | {num(s['p50'])} | {num(s['p90'])} |"
        f" {num(s['max'])} | {pct(f['>4'])} | {pct(f['>8'])} | {pct(f['>12'])} | {pct(f['>16'])} |"
        f" {table(hist(s))} |"
    )
raw = one("attribute_widths", tap="tap_input")
if raw:
    out.append("")
    out.append(
        f"At `tap_input` every one of these sources reports the same width"
        f" ({num(raw['stats']['p50'])} attributes: `tail_in`'s own file provenance), which is why"
        " the raw tap is one shared `shape` component rather than eight."
    )
out.append("")

out.append("### Nesting, depth, and bytes (after `json`)")
out.append("")
out.append(
    "`json` merges only the **top level**, so a nested JSON object arrives as one attribute whose"
    " value is a map. `nested_maps` is how many of an event's attributes are maps,"
    " `nested_map_width` how many entries each holds, `value_depth` the deepest value on the"
    " event (0 = flat)."
)
out.append("")
out.append("| library | nested maps p50/p90/max | map width p50/p90/max | depth p50/p90/max | key bytes p50/p90/max | value bytes p50/p90/max |")
out.append("|---|---|---|---|---|---|")
for source, (label, config, tap) in APPS.items():
    if not one("distributions", metric="logit.shape.attributes", source=source, tap=tap):
        continue
    out.append(
        f"| {label} ({config}) |"
        f" {stats_cells(dist('logit.shape.nested_maps', source, tap))} |"
        f" {stats_cells(dist('logit.shape.nested_map_width', source, tap))} |"
        f" {stats_cells(dist('logit.shape.value_depth', source, tap))} |"
        f" {stats_cells(dist('logit.shape.key_bytes', source, tap))} |"
        f" {stats_cells(dist('logit.shape.value_bytes', source, tap))} |"
    )
out.append("")
out.append("| library | raw line bytes (body at `tap_input`) p50 | p90 | max |")
out.append("|---|--:|--:|--:|")
for source, (label, config, _tap) in APPS.items():
    s = dist("logit.shape.body_bytes", source, "tap_input")
    if not s:
        continue
    out.append(f"| {label} ({config}) | {num(s.get('p50'))} | {num(s.get('p90'))} | {num(s.get('max'))} |")
out.append("")

out.append("### Value types, distinct keys and key-sets (after `json`)")
out.append("")
out.append(
    "The type mix is over an event's attribute *values*; `distinct_keys`/`distinct_keysets` are"
    " that tap's own cumulative tables (one `shape` component per library, which is what makes"
    " them a per-library statement), and `keyset_share.top1` is the share of the library's events"
    " carried by its single most common key-set -- a library whose records are one fixed shape"
    " reads near 100%, one whose error path is a different shape does not."
)
out.append("")
out.append("| library | string | int | float | bool | map | array | other | distinct keys | key-sets | top1 | top5 | overflow |")
out.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
TYPES = ["string", "int", "float", "bool", "map", "array"]
for source, (label, config, tap) in APPS.items():
    totals = {}
    for t in TYPES + ["bytes", "null"]:
        row = one("counters", metric=f"logit.shape.values.{t}", source=source, tap=tap)
        if row:
            totals[t] = row["total"]
    if not totals:
        continue
    grand = sum(totals.values())
    cells = " | ".join(pct(totals.get(t, 0) / grand) for t in TYPES)
    other = pct((totals.get("bytes", 0) + totals.get("null", 0)) / grand)

    def gauge(metric, tap=tap):
        row = one("gauges", metric=metric, tap=tap)
        return None if row is None else row["value"]

    out.append(
        f"| {label} ({config}) | {cells} | {other} |"
        f" {num(gauge('logit.shape.distinct_keys'))} | {num(gauge('logit.shape.distinct_keysets'))} |"
        f" {pct(gauge('logit.shape.keyset_share.top1'))} | {pct(gauge('logit.shape.keyset_share.top5'))} |"
        f" {num(gauge('logit.shape.tracking_overflow'))} |"
    )
out.append("")

out.append("### Default configuration versus documented production configuration")
out.append("")
out.append(
    "Only two libraries here have a *bare default that is already JSON*, and so a real pair to"
    " compare. structlog's default renderer is `ConsoleRenderer` (text) and semantic_logger's"
    " default appender formatter is `:color` (text) -- neither has a default JSON shape to"
    " measure, which is itself the finding. log/slog's `JSONHandler(w, nil)` *is* its default,"
    " and zap ships no second JSON preset beyond `NewProduction` (its `NewExample` is documented"
    " as a testing convenience), so Go's two rows are one default and one production."
)
out.append("")
out.append("| pair | default p50 attrs | production p50 attrs | delta |")
out.append("|---|--:|--:|--:|")
for default_source, prod_source, name in (
    ("pyjson_default_in", "pyjson_prod_in", "python-json-logger"),
    ("pino_default_in", "pino_http_in", "pino / pino-http"),
):
    d = dist("logit.shape.attributes", default_source, APPS[default_source][2])
    p = dist("logit.shape.attributes", prod_source, APPS[prod_source][2])
    if not d or not p:
        continue
    delta = (p.get("p50") or 0) - (d.get("p50") or 0)
    out.append(f"| {name} | {num(d.get('p50'))} | {num(p.get('p50'))} | {delta:+.0f} |")
out.append("")

# ---- the Django / OpenTelemetry leg -------------------------------------------------------------
out.append("### Django under `opentelemetry-instrument`, straight into `otlp_in`")
out.append("")
out.append(
    "No Collector in between, so the resource width below is the Python SDK's own and nothing"
    " else's. One tap, carrying all three signals; `shape`'s `signal` tag is what separates them."
)
out.append("")
out.append("| signal | events | attrs p50 | p90 | max | >4 | >8 | >12 | >16 | value->count |")
out.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|---|")
for row in rows("attribute_widths", tap="tap_django"):
    s, f = row["stats"], row["fractions"]
    out.append(
        f"| `{row['signal']}` | {s['count']} | {num(s['p50'])} | {num(s['p90'])} | {num(s['max'])} |"
        f" {pct(f['>4'])} | {pct(f['>8'])} | {pct(f['>12'])} | {pct(f['>16'])} | {table(hist(s), 14)} |"
    )
out.append("")
out.append("| desk count (static, from the instrumentation source) | min / typical / max |")
out.append("|---|---|")
for what, triple in DESK.items():
    out.append(f"| {what} | {triple} |")
out.append("")
out.append(
    "**Read the span row as a pooled distribution.** `shape` emits counts and lengths only -- no"
    " span name, no span kind -- so one row covers the Django SERVER spans, the sqlite3 dbapi"
    " CLIENT spans and the `requests` CLIENT spans together, in whatever proportion the request"
    " mix produced. The value->count table is multi-modal for exactly that reason, and the modes"
    " are what to compare against the three desk triples above; the p50 is a property of the mix"
    " as much as of any one span."
)
out.append("")
for metric, label in (
    ("logit.shape.batch.events", "events per OTLP export (the SDK's own batch processors)"),
    ("logit.shape.batch.resource_attributes", "resource attributes per batch"),
    ("logit.shape.batch.scope_attributes", "scope attributes per batch"),
    ("logit.shape.metrics", "metric records per event"),
    ("logit.shape.samples_per_metric", "data points per metric record"),
    ("logit.shape.span_events", "events per span"),
    ("logit.shape.span_links", "links per span"),
):
    for row in rows("distributions", metric=metric, tap="tap_django"):
        s = row["stats"]
        if not out or not out[-1].startswith("| measure"):
            pass
        out.append(
            f"- **{label}** (`signal={row['signal'] or 'batch'}`): n={s['count']},"
            f" p50 {num(s['p50'])}, p90 {num(s['p90'])}, max {num(s['max'])}"
            f" -- {table(hist(s), 10)}"
        )
out.append("")
out.append("The same per-batch numbers for the file legs, for contrast:")
out.append("")
out.append("| source | events per tailer batch p50 | p90 | max | resource attrs | scope attrs |")
out.append("|---|--:|--:|--:|--:|--:|")
for source, (label, config, _tap) in APPS.items():
    s = dist("logit.shape.batch.events", source, "tap_input")
    if not s:
        continue
    res = dist("logit.shape.batch.resource_attributes", source, "tap_input")
    scope = dist("logit.shape.batch.scope_attributes", source, "tap_input")
    out.append(
        f"| {label} ({config}) | {num(s.get('p50'))} | {num(s.get('p90'))} | {num(s.get('max'))} |"
        f" {num(res.get('p50'))} | {num(scope.get('p50'))} |"
    )
out.append("")

pathlib.Path("/out/section.md").write_text("\n".join(out) + "\n")
print("applogs: wrote /out/section.md")
PYEOF
}

survey_applogs() {
    local run_dir config duration

    survey_out_dir applogs
    run_dir="${SURVEY_RUN_DIR}"
    config="${ROOT}/tools/shape-survey/configs/applogs.yaml"
    duration="${SHAPE_SURVEY_DURATION:-${SHAPE_SURVEY_APPLOGS_DURATION_DEFAULT}}"

    survey_provenance applogs \
        "real logging libraries at pinned versions in their documented production configuration, in minimal apps under a synthetic request mix -- library record structure; no application-specific fields an operator would add, so widths are a floor. Django leg: SDK defaults with auto-instrumentation, no collector enrichment"

    mkdir -p "${run_dir}/applogs"
    chmod 777 "${run_dir}/applogs"

    survey_applogs_images
    {
        echo "duration: ${duration}s at ${SHAPE_SURVEY_APPLOGS_RATE} req/s per logging app and"
        echo "  ${SHAPE_SURVEY_APPLOGS_DJANGO_RATE} req/s at Django (tools/shape-survey/applogs/drive.py's mix:"
        echo "  ~20% /, ~30% a listing with a query string, ~25% an item fetch, ~15% a search,"
        echo "  ~7% a 404, ~3% a 500 with a real traceback; seven real user-agent strings)"
        echo "config: tools/shape-survey/configs/applogs.yaml"
        echo "libraries (pinned in each app's Dockerfile):"
        echo "  structlog 26.1.0            documented production JSON config (WriteLoggerFactory,"
        echo "                              dict_tracebacks, ISO TimeStamper). NO default-config"
        echo "                              stream: structlog's default renderer is ConsoleRenderer,"
        echo "                              which is text, not JSON."
        echo "  python-json-logger 4.2.0    both configurations: the quickstart's bare"
        echo "                              JsonFormatter() and its format-string form."
        echo "  pino 10.3.1 / pino-http 11.0.0  pino-http at its defaults (pino-std-serializers'"
        echo "                              nested req/res), and a bare pino() logger beside it."
        echo "  Go log/slog (go1.23)        JSONHandler(w, nil) -- the default, and slog's only"
        echo "                              JSON configuration."
        echo "  zap 1.28.0                  NewProductionConfig() with only OutputPaths changed"
        echo "                              (stderr -> the tailed file); encoder untouched."
        echo "  semantic_logger 5.1.0       formatter: :json. NO default-config stream: the default"
        echo "                              appender formatter is :color, which is text."
        echo "LOGRAGE AND RAILS WERE DROPPED, deliberately: lograge is a Rails railtie with no"
        echo "  supported use outside Rails, and a 'gem install rails' + 'rails new' inside an image"
        echo "  build is minutes of build for four routes. The brief's own fallback --"
        echo "  semantic_logger's JSON formatter -- was taken instead. Ruby's row is NOT a Rails row."
        echo "transport: each app writes JSON lines to a file on a shared volume; one tail_in per"
        echo "  file follows it. Chosen over piping stdout at a syslog listener because it is the"
        echo "  honest shape of the thing measured -- the library's own line, byte for byte, with"
        echo "  no syslog header wrapped around it and no framing decision in between."
        echo "django leg:"
        echo "  Django 6.1.1, opentelemetry-distro 0.65b0 / SDK 1.44.0, instrumentations selected"
        echo "  by 'opentelemetry-bootstrap -a install' (full pip freeze below), OTLP/gRPC straight"
        echo "  to otlp_in with NO collector in between."
        echo "  sqlite, not Postgres -- the dbapi span comes from the same"
        echo "  opentelemetry-instrumentation-dbapi either way, but sqlite's carries no network"
        echo "  peer, so its attribute count sits at the low end of the desk count's range."
        echo "  Django's own 'runserver --noreload', not gunicorn: the SERVER span is minted by"
        echo "  opentelemetry-instrumentation-django's middleware, which is identical under either."
        echo "  Only three OTel settings are given (endpoint, service name, and"
        echo "  OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED, which defaults off and without"
        echo "  which there would be no log signal at all). Everything else is the SDK default,"
        echo "  including the semantic-convention opt-in -- so whichever HTTP semconv this SDK"
        echo "  version defaults to is what was measured."
    } >>"${run_dir}/provenance.txt"
    {
        echo "django image pip freeze:"
        ${DOCKER} run --rm --entrypoint cat shape-survey-applogs-django:latest /app/pip-freeze.txt |
            sed 's/^/  /'
    } >>"${run_dir}/provenance.txt" || true

    survey_applogs_services
    start_logit "${config}"
    survey_applogs_django

    # The driver *is* the capture window: it runs in the foreground for `duration` seconds,
    # printing a progress line every 15s.
    survey_python driver --network "${SURVEY_NET}" -- \
        python3 /tools/applogs/drive.py "${duration}" \
        "app=http://structlog:8000,${SHAPE_SURVEY_APPLOGS_RATE}" \
        "app=http://pyjson:8000,${SHAPE_SURVEY_APPLOGS_RATE}" \
        "app=http://pino:8000,${SHAPE_SURVEY_APPLOGS_RATE}" \
        "app=http://goapp:8000,${SHAPE_SURVEY_APPLOGS_RATE}" \
        "app=http://semlog:8000,${SHAPE_SURVEY_APPLOGS_RATE}" \
        "django=http://django:8000,${SHAPE_SURVEY_APPLOGS_DJANGO_RATE}"

    # Let the tailers drain the last writes and the SDK's batch processors fire once more before
    # the final flush. The OTLP BatchSpanProcessor's default schedule is 5s, so 20s is comfortably
    # more than one of them.
    survey_capture_for 20

    { echo "lines written per app log file:"; wc -l "${run_dir}"/applogs/*.log | sed 's/^/  /'; } \
        >>"${run_dir}/provenance.txt"

    stop_logit

    survey_summarize >/dev/null
    survey_applogs_section_py >"${run_dir}/section.py"
    survey_python section -- python3 /out/section.py
    survey_summarize --append section.md
}
