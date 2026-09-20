# The `demo` producer: run `demo/`'s own stack, tapped, for a quarter of an hour.
#
# Sourced by script/shape-survey, which discovers this file by glob. Everything specific to this
# producer lives here -- there is no `configs/demo*.yaml`, because the config is *generated* from
# demo/logit.yaml at run time (see `survey_demo_config`) and lives in the run directory as an
# artifact. A committed second copy would have to be kept in step with demo/logit.yaml by hand,
# and would be wrong the first time either changed.
#
# **`demo/` itself is never modified.** The taps go into a generated copy of its config, mounted
# over `/config.yaml` by tools/shape-survey/demo-overlay.yaml, which changes nothing else about
# the stack.
#
# ---------------------------------------------------------------------------------------------
# WHAT THESE NUMBERS ARE WORTH, AND WHY THE CAVEAT IS STRUCTURAL
#
# This is the harness's best end-to-end exercise: six tiers, three signals, real software, real
# rotation, real Docker json-file logs, a real transform chain per tier. It is **not** evidence
# about production shape, and it must not be quoted as though it were:
#
#   * The stack exists to demonstrate `logit`. Its tiers were chosen to show components off, not
#     because that is how anybody's estate is built, and several of them are configured for
#     visibility rather than the way an operator would run them (Postgres at
#     `log_min_duration_statement=0`, rotation every few minutes, one request generator).
#   * Some of the formats being measured **were written by this project**. nginx's JSON
#     `log_format`, the Django app's logging config and the Celery worker's are ours; measuring
#     the width of an event whose field list we chose is circular, and says what we picked, not
#     what the world sends. `survey_demo_source_labels` below labels every tier with where its
#     format came from, and summarize.py prints that table above the numbers.
#
# So this producer states its representativeness in provenance.txt, summarize.py turns that into
# the banner at the top of summary.md, and the per-tier format-origin table sits above the first
# distribution. None of that is a footnote a reader can skip past on the way to a percentile --
# which is the point. In docs/plans/data-shape-survey.md's grading these rows are Measured/Demo:
# highest fidelity, lowest representativeness.
# ---------------------------------------------------------------------------------------------

#: How long the stack runs before SIGTERM, in seconds. 15 minutes by default (the plan's box);
#: `SHAPE_SURVEY_DURATION=180 script/shape-survey demo` for a quick verification run. demo/'s
#: `traffic` service is steady from its first ~16s cycle, so a short run is a smaller sample of
#: the same thing rather than a different thing.
SHAPE_SURVEY_DEMO_DURATION_DEFAULT=900

# The tiers, as `<input component>|<post-parse component>|<tier>|<where its format came from>`.
# The first two are what the two taps attach to; the last two are what
# `survey_demo_source_labels` writes for the summary.
#
# "software's own default" versus "authored in this repo" is the distinction that decides whether
# a row is evidence about anything outside this repository. Postgres's jsonlog, Redis's log line,
# HAProxy's `option httplog` and Docker's json-file envelope are those projects' own formats,
# configured on but not designed here. nginx's `log_format` and the Django/Celery logging configs
# are ours.
survey_demo_tiers() {
    cat <<'EOF'
haproxy_in|haproxy_trace|haproxy|haproxy's own `option httplog` line (software default, configured on here)
nginx_in|nginx_trace|nginx|JSON `log_format` AUTHORED IN THIS REPO (demo/nginx/) -- circular as evidence
app_in|app_trace|demo-app (Django)|Django logging config AUTHORED IN THIS REPO (demo/app/)
worker_in|worker_trace|demo-worker (Celery)|Celery logging config AUTHORED IN THIS REPO (demo/app/)
postgres_in|postgres_trace|postgres|Postgres `log_destination=jsonlog` (software default format)
redis_in|redis_parse|redis|Redis's own plain-text server log line (software default)
EOF
}

# The JSON map summarize.py reads with --source-labels: `source` component -> tier + format origin.
survey_demo_source_labels() {
    local input landed tier format first=1
    echo "{"
    while IFS='|' read -r input landed tier format; do
        [ -n "${input}" ] || continue
        [ "${first}" -eq 1 ] || echo ","
        first=0
        printf '  "%s": {"tier": "%s", "format": "%s"}' "${input}" "${tier}" "${format}"
    done < <(survey_demo_tiers)
    echo ""
    echo "}"
}

# Generates the tapped config from demo/logit.yaml. Appended, not rewritten: `components:` is the
# last top-level key in that file, so more two-space-indented entries at the end of it join the
# same mapping -- which keeps this a purely additive transformation with nothing to re-derive if
# demo/logit.yaml changes shape.
survey_demo_config() {
    local out="$1" inputs=() landed=() input landed_name tier format
    while IFS='|' read -r input landed_name tier format; do
        [ -n "${input}" ] || continue
        inputs+=("${input}")
        landed+=("${landed_name}")
    done < <(survey_demo_tiers)

    cp "${ROOT}/demo/logit.yaml" "${out}"
    cat >>"${out}" <<EOF

  # ---- appended by tools/shape-survey/producers/demo.sh; NOT part of demo/logit.yaml ----------
  # Two taps per the survey's design (docs/plans/data-shape-survey.md): one straight off each
  # tier's listener, one after that tier's own parse chain, so the widening a chain does is a
  # measured difference rather than an assumption. \`shape\` tags every measurement with the
  # batch's provenance \`source\`, so all six tiers share one component per tap and stay
  # distinguishable downstream.
  #
  # \`tap_input\` reads nginx's listener directly, which means it also sees the container's
  # stderr (error_log) lines that \`nginx_stdout\` filters out before \`nginx_trace\`. That is the
  # honest reading of "straight off the input" -- and it is why the two nginx rows in the summary
  # are not a like-for-like pair the way the other five tiers' are.
  tap_input:
    type: shape
    sources: [$(IFS=,; echo "${inputs[*]}")]
    interval: 10s
    resource: drop

  tap_landed:
    type: shape
    sources: [$(IFS=,; echo "${landed[*]}")]
    interval: 10s
    resource: drop

  # \`distributions: samples\` keeps \`shape\`'s raw observations raw through the window;
  # \`max_samples_per_series\` is far above what 15 minutes of this stack can produce, and
  # summarize.py fails loudly on any sketched \`logit.shape.*\` series rather than reporting an
  # approximation as a measurement.
  shape_rollup:
    type: aggregate
    sources: [tap_input, tap_landed]
    interval: 10s
    distributions: samples
    max_samples_per_series: 5000000
    max_retained_series: 1000000

  # Into the run directory, mounted by tools/shape-survey/demo-overlay.yaml. A huge
  # \`rotate.max_bytes\`: \`file_out\` requires a rotation trigger, and a survey that rotated would
  # silently lose the start of its own capture.
  shape_out:
    type: file_out
    sources: [shape_rollup]
    path: /shape-survey/shape.log
    rotate:
      max_bytes: "64GiB"
EOF
}

survey_demo_compose() {
    ${DOCKER} compose -f "${ROOT}/demo/compose.yaml" -f "${ROOT}/tools/shape-survey/demo-overlay.yaml" \
        --env-file "${SURVEY_RUN_DIR}/compose.env" "$@"
}

survey_demo() {
    local run_dir config duration

    # demo/compose.yaml gives `nginx`/`redis` fixed container names (docker_in follows them by
    # name), so only one demo stack can exist on a host at a time. If one is already up it is
    # somebody else's -- this daemon is shared -- and it is not ours to stop.
    if [ -n "$(${DOCKER} compose -f "${ROOT}/demo/compose.yaml" ps -q 2>/dev/null)" ]; then
        survey_fail "a demo stack is already running on this daemon. It is not this run's to stop" \
            "-- bring it down yourself (script/demo down -v) if it is yours, or wait."
    fi

    survey_out_dir demo
    run_dir="${SURVEY_RUN_DIR}"
    duration="${SHAPE_SURVEY_DURATION:-${SHAPE_SURVEY_DEMO_DURATION_DEFAULT}}"

    survey_provenance demo \
        "own demo stack -- harness exercise; not evidence of production shape"
    {
        echo "duration: ${duration}s under demo/'s own \`traffic\` generator"
        echo "config: generated from demo/logit.yaml at run time -> logit.yaml in this directory"
        echo "tiers and where each one's format came from:"
        survey_demo_tiers | sed 's/^/  /'
        echo "caveat: several of the formats above were authored in this repository (see the table)."
        echo "  Measuring the width of an event whose field list we chose is circular, and says"
        echo "  what this project picked rather than what real producers send. Parts of the stack"
        echo "  are also configured for visibility (log_min_duration_statement=0, minute-scale"
        echo "  rotation, one synthetic request generator) rather than the way an operator would"
        echo "  run them."
    } >>"${run_dir}/provenance.txt"

    survey_demo_source_labels >"${run_dir}/source-labels.json"

    config="${run_dir}/logit.yaml"
    survey_demo_config "${config}"
    # `INFLUXDB_TOKEN` because demo/logit.yaml's `influx_out` resolves its token through `!env`
    # (ADR `env-yaml-tag`), and `validate` resolves those exactly as `run` does. The same default
    # demo/compose.yaml itself uses, so validating here and running there see the same config.
    echo "shape-survey: validating the generated ${config}"
    ${DOCKER} run --rm -e "INFLUXDB_TOKEN=${INFLUXDB_TOKEN:-logit-demo-token}" \
        -v "${config}:/config.yaml:ro,z" "${SURVEY_IMAGE}" validate /config.yaml ||
        survey_fail "the generated demo config did not validate -- the tap block or demo/logit.yaml changed"

    {
        echo "SHAPE_SURVEY_CONFIG=${config}"
        echo "SHAPE_SURVEY_RUN_DIR=${run_dir}"
    } >"${run_dir}/compose.env"

    # Registered before `up`, so a failure anywhere below still tears the stack down. `down -v`
    # rather than `down`: the checkpoints and the Postgres log volume are this run's, and leaving
    # them would make the next run resume a tail mid-file instead of reading from the beginning.
    survey_on_cleanup "${DOCKER} compose -f '${ROOT}/demo/compose.yaml' -f '${ROOT}/tools/shape-survey/demo-overlay.yaml' --env-file '${run_dir}/compose.env' down -v --remove-orphans >/dev/null 2>&1"

    echo "shape-survey: bringing up the demo stack (this builds demo images on a first run)"
    survey_demo_compose up -d --build

    # `logit`'s own healthcheck is `logit ready` against demo/logit.yaml's `admin:` block, so
    # compose's health state is the readiness signal here -- no blind sleep, same as start_logit.
    # Compose names the container after the project, so it is asked for the id rather than
    # guessed at: `docker inspect` on a name that does not exist prints an empty *stdout* line
    # before failing, which quietly turned a later "healthy" into "\nhealthy" and never matched.
    local i cid state=""
    for i in $(seq 1 180); do
        cid="$(survey_demo_compose ps -q logit 2>/dev/null || true)"
        if [ -n "${cid}" ]; then
            state="$(${DOCKER} inspect --format '{{.State.Health.Status}}' "${cid}" 2>/dev/null || true)"
        fi
        [ "${state}" = "healthy" ] && break
        sleep 1
    done
    [ "${state}" = "healthy" ] ||
        survey_fail "the demo stack's logit never became healthy (last state: ${state:-unknown})"
    echo "shape-survey: stack healthy; capturing for ${duration}s"

    sleep "${duration}"

    echo "shape-survey: stopping logit (SIGTERM, up to 60s for the final flush)"
    survey_demo_compose stop -t 60 logit
    survey_demo_compose logs --no-color logit >"${run_dir}/logit.log" 2>&1 || true
    survey_demo_compose down -v --remove-orphans

    local shape_log="${run_dir}/shape.log"
    [ -f "${shape_log}" ] || survey_fail "no ${shape_log} -- the file_out never wrote anything"
    [ -s "${shape_log}" ] || survey_fail "${shape_log} is empty -- nothing reached the shape taps"
    echo "shape-survey: shape.log is $(wc -c <"${shape_log}") bytes"

    survey_summarize
}
