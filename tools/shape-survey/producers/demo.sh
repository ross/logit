# The `demo` producer: `demo/`'s own stack, tapped, for 15 minutes by default.
#
# There is no configs/demo*.yaml: `survey_demo_config` generates the tapped config from
# demo/logit.yaml at run time, so no committed copy has to be kept in step. demo/ itself is never
# modified; tools/shape-survey/demo-overlay.yaml mounts the generated config and changes nothing
# else.
#
# Representativeness: the weakest evidence in the harness, and some tiers' formats were authored
# in this repo. See README "Representativeness is structural". `survey_demo_tiers` labels each
# tier's format origin, and the summary prints that table above the numbers.
#
# Environment: SHAPE_SURVEY_DURATION (default 900s); INFLUXDB_TOKEN (read by both the validation
# step and demo/compose.yaml, each defaulting to `logit-demo-token`).

#: Seconds the stack runs before SIGTERM. demo/'s `traffic` service is steady from its first ~16s
#: cycle, so a short `SHAPE_SURVEY_DURATION` run is a smaller sample of the same thing.
SHAPE_SURVEY_DEMO_DURATION_DEFAULT=900

# The tiers, as `<input component>|<post-parse component>|<tier>|<where its format came from>`.
# The taps attach to the first two; `survey_demo_source_labels` writes the last two. "Software
# default" versus "authored in this repo" decides whether a row is evidence about anything outside
# this repository.
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

# Generates the tapped config from demo/logit.yaml by appending. This relies on `components:`
# being that file's last top-level key, so two-space-indented entries at the end join its mapping.
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
  # Two taps: one off each tier's listener, one after its parse chain. \`shape\` tags each
  # measurement with the batch's \`source\`, so six tiers share one component per tap.
  #
  # \`tap_input\` also sees nginx's stderr (error_log) lines, which \`nginx_stdout\` filters out
  # before \`nginx_trace\`, so the two nginx rows aren't a like-for-like pair.
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

  # \`distributions: samples\` with a cap far above what this stack produces keeps observations
  # raw; summarize.py fails on any sketched \`logit.shape.*\` series.
  shape_rollup:
    type: aggregate
    sources: [tap_input, tap_landed]
    interval: 10s
    distributions: samples
    max_samples_per_series: 5000000
    max_retained_series: 1000000

  # A huge \`rotate.max_bytes\`: \`file_out\` requires a rotation trigger, and a survey that rotated
  # would lose the start of its capture.
  shape_out:
    type: file_out
    sources: [shape_rollup]
    path: /shape-survey/shape.log
    rotate:
      max_bytes: "64GiB"
EOF
}

# The stack, through `survey_compose`, whose already-running guard matters here: demo/compose.yaml
# fixes the `nginx` and `redis` container names for `docker_in`, so only one can exist on a host.
survey_demo_compose() {
    survey_compose stack \
        -f "${ROOT}/demo/compose.yaml" -f "${ROOT}/tools/shape-survey/demo-overlay.yaml" \
        --env-file "${SURVEY_RUN_DIR}/compose.env" -- "$@"
}

# Readiness: compose's health state for `logit`, whose healthcheck is `logit ready`. Ask compose
# for the container id rather than guess the name: `docker inspect` on a missing name prints an
# empty stdout line before failing, turning "healthy" into "\nhealthy", which never matches.
survey_demo_healthy() {
    local cid
    cid="$(survey_demo_compose ps -q logit 2>/dev/null || true)"
    [ -n "${cid}" ] || return 1
    [ "$(${DOCKER} inspect --format '{{.State.Health.Status}}' "${cid}" 2>/dev/null || true)" = "healthy" ]
}

survey_demo() {
    local run_dir config duration

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
    # `validate` resolves `!env` like `run` does, and `influx_out`'s token is one. The default
    # matches demo/compose.yaml's, so both see the same config.
    echo "shape-survey: validating the generated ${config}"
    ${DOCKER} run --rm -e "INFLUXDB_TOKEN=${INFLUXDB_TOKEN:-logit-demo-token}" \
        -v "${config}:/config.yaml:ro,z" "${SURVEY_IMAGE}" validate /config.yaml ||
        survey_fail "the generated demo config did not validate -- the tap block or demo/logit.yaml changed"

    {
        echo "SHAPE_SURVEY_CONFIG=${config}"
        echo "SHAPE_SURVEY_RUN_DIR=${run_dir}"
    } >"${run_dir}/compose.env"

    # The first `survey_compose` call registers the teardown hook, so a failure below still brings
    # the stack down.
    echo "shape-survey: bringing up the demo stack (this builds demo images on a first run)"
    survey_demo_compose up -d --build

    survey_capture_until survey_demo_healthy 180
    echo "shape-survey: stack healthy"

    survey_capture_for "${duration}"

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
