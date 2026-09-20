# Shared plumbing for `script/shape-survey`, sourced by the dispatcher. Not meant to be run
# directly, and deliberately holding **nothing producer-specific**: a new producer is one file,
# `tools/shape-survey/producers/<name>.sh`, plus its config(s) under `configs/`, and it must never
# need an edit here or in the dispatcher. See tools/shape-survey/README.md.
#
# Everything this file creates -- containers, the network, images -- is named with a
# `shape-survey-` prefix (the image is `logit:shape-survey`) so cleanup can enumerate exactly what
# this run started. Nothing here ever prunes, and nothing here removes a docker resource it did
# not create itself: this daemon is shared.

# Docker network every container in a run joins. `logit` is reachable on it by the alias `logit`.
SURVEY_NET="shape-survey-net"

# The release image built from the current tree, once per invocation (see `survey_image`).
SURVEY_IMAGE="logit:shape-survey"

# Python containers run this, matching tools/record-fixtures/raw_capture.py's own runtime -- the
# three scripts here are stdlib-only for exactly that reason (no `pip install` step).
SURVEY_PYTHON_IMAGE="python:3.12-slim"

# Every container name this run has started, in order, for `survey_cleanup`. Appended to by
# `survey_run_container`/`start_logit`; never read for anything else.
SURVEY_CONTAINERS=()

# Set by `survey_out_dir`: the current producer's run directory on the host. `start_logit` mounts
# it read-write at /out, and `stop_logit` writes the container log into it.
SURVEY_RUN_DIR=""

# Set by `start_logit`, cleared by `stop_logit` -- the logit container's name, so a cleanup after
# a mid-run failure still captures its log.
SURVEY_LOGIT_CONTAINER=""

# ---- naming and bookkeeping ---------------------------------------------------------------------

# survey_container_name <suffix>: every container this harness starts, named so cleanup can only
# ever touch its own. Never build a container name any other way.
survey_container_name() {
    echo "shape-survey-$1"
}

# survey_track <name>: remember a container for cleanup. Called by the two starters below; a
# producer needing its own one-off container should call this right after `docker run -d`.
survey_track() {
    SURVEY_CONTAINERS+=("$1")
}

survey_fail() {
    echo "shape-survey: $*" >&2
    return 1
}

# ---- network and cleanup ------------------------------------------------------------------------

# survey_network: create the run's docker network. Never reuses an existing one silently -- a
# leftover `shape-survey-net` from a crashed run is fine to reuse (it is ours, by name), but a
# failure for any *other* reason (notably "all predefined address pools have been fully
# subnetted", which means the daemon is out of subnets and other sessions' networks are holding
# them) stops the run rather than deleting anything.
survey_network() {
    if ${DOCKER} network inspect "${SURVEY_NET}" >/dev/null 2>&1; then
        echo "shape-survey: reusing existing ${SURVEY_NET} (left by an earlier shape-survey run)"
        return 0
    fi
    if ! ${DOCKER} network create "${SURVEY_NET}" >/dev/null; then
        survey_fail "could not create ${SURVEY_NET}." \
            "If this said 'all predefined address pools have been fully subnetted', the daemon is" \
            "out of subnets -- other sessions share it, so do NOT delete their networks. Stop here" \
            "and report it."
    fi
}

# survey_cleanup: removes every container this run started and the network, in that order. Safe to
# call twice, and safe to call after a partial run. Only ever names containers from
# SURVEY_CONTAINERS, so it cannot touch another session's.
survey_cleanup() {
    local name
    for name in "${SURVEY_CONTAINERS[@]-}"; do
        [ -n "${name}" ] || continue
        ${DOCKER} rm -f "${name}" >/dev/null 2>&1 || true
    done
    SURVEY_CONTAINERS=()
    ${DOCKER} network rm "${SURVEY_NET}" >/dev/null 2>&1 || true
    return 0
}

# ---- the image ----------------------------------------------------------------------------------

# survey_image: build the release image from the current tree, once per invocation. A survey
# measures *this* tree's `shape`, so the image is rebuilt rather than pulled or assumed --
# `SHAPE_SURVEY_SKIP_IMAGE=1` skips it when the caller knows it is already current (a second
# producer in the same sitting, or an iteration that only changed a config).
survey_image() {
    if [ -n "${SHAPE_SURVEY_SKIP_IMAGE:-}" ]; then
        echo "shape-survey: SHAPE_SURVEY_SKIP_IMAGE set -- using the existing ${SURVEY_IMAGE}"
        ${DOCKER} image inspect "${SURVEY_IMAGE}" >/dev/null 2>&1 ||
            survey_fail "SHAPE_SURVEY_SKIP_IMAGE is set but ${SURVEY_IMAGE} does not exist"
        return 0
    fi
    if [ -n "${SURVEY_IMAGE_BUILT:-}" ]; then
        return 0
    fi
    echo "shape-survey: building ${SURVEY_IMAGE} from the current tree (Dockerfile)"
    ${DOCKER} build -f "${ROOT}/Dockerfile" -t "${SURVEY_IMAGE}" "${ROOT}"
    SURVEY_IMAGE_BUILT=1
}

# ---- run directories and provenance -------------------------------------------------------------

# survey_out_dir <producer>: creates and echoes this run's output directory, and sets
# SURVEY_RUN_DIR to it. Under perf/results/ by default, which .gitignore already covers -- raw
# captures and survey output never enter the repo, and nothing here ever writes under testdata/.
survey_out_dir() {
    local producer="$1" stamp
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    SURVEY_RUN_DIR="${SHAPE_SURVEY_OUT:-${ROOT}/perf/results/shape-survey}/${producer}/${stamp}"
    mkdir -p "${SURVEY_RUN_DIR}"
    # The logit container runs as the unprivileged `logit` user (Dockerfile), whose uid has no
    # relationship to whoever owns this checkout -- world-writable is what lets `file_out` create
    # its log here, the same accommodation record_otlp makes for the Collector's file exporter.
    chmod 777 "${SURVEY_RUN_DIR}"
    echo "${SURVEY_RUN_DIR}"
}

# survey_provenance <producer> <representativeness>: writes the common half of provenance.txt --
# date, this repo's git SHA and dirty state, the docker and image identities. A producer appends
# its own software versions to the same file (that is the half only it knows), e.g.
#   { echo "producer software:"; echo "  python: $(...)"; } >>"${SURVEY_RUN_DIR}/provenance.txt"
#
# **`<representativeness>` is required, and it is not decoration.** It is one line saying what kind
# of traffic this producer's numbers are -- what docs/plans/data-shape-survey.md's grading calls
# the representativeness axis (Demo / Default / Configured / Production). summarize.py reads it
# straight out of provenance.txt and prints it as a banner at the top of summary.md, so a run's
# numbers cannot be read, quoted or pasted without the caveat attached: a measurement of a stack
# this project built to demonstrate itself is a harness exercise, not evidence about production
# shape, and the difference has to travel with the number.
survey_provenance() {
    local producer="$1" representativeness="$2" file="${SURVEY_RUN_DIR}/provenance.txt"
    [ -n "${representativeness}" ] ||
        survey_fail "survey_provenance: ${producer} passed no representativeness line"
    {
        echo "shape-survey provenance"
        echo "producer: ${producer}"
        echo "representativeness: ${representativeness}"
        echo "captured: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "host: $(uname -srm)"
        echo "repo: $(git -C "${ROOT}" rev-parse HEAD)"
        echo "repo dirty: $(git -C "${ROOT}" status --porcelain | wc -l) modified path(s)"
        echo "docker: $(${DOCKER} version --format '{{.Server.Version}}' 2>/dev/null || echo unknown)"
        echo "image ${SURVEY_IMAGE}: $(${DOCKER} image inspect --format '{{.Id}}' "${SURVEY_IMAGE}" 2>/dev/null || echo unknown)"
        echo "image ${SURVEY_PYTHON_IMAGE}: $(${DOCKER} image inspect --format '{{.Id}}' "${SURVEY_PYTHON_IMAGE}" 2>/dev/null || echo 'not pulled yet')"
    } >"${file}"
}

# ---- running logit ------------------------------------------------------------------------------

# start_logit <producer> <config>: validates <config> in the image (loudly -- a config that does
# not validate is a broken survey, not a warning), then runs it detached on the run's network under
# the alias `logit`, with the config mounted read-only and SURVEY_RUN_DIR mounted read-write at
# /out. Waits for readiness via `logit ready` against the config's own `admin:` block rather than
# sleeping.
start_logit() {
    local producer="$1" config="$2" name
    name="$(survey_container_name "logit-${producer}")"

    echo "shape-survey: validating ${config}"
    ${DOCKER} run --rm -v "${config}:/config.yaml:ro,z" "${SURVEY_IMAGE}" validate /config.yaml ||
        survey_fail "${config} failed 'logit validate' -- fix the config, not this check"

    ${DOCKER} rm -f "${name}" >/dev/null 2>&1 || true
    ${DOCKER} run -d --name "${name}" --network "${SURVEY_NET}" --network-alias logit \
        -v "${config}:/config.yaml:ro,z" \
        -v "${SURVEY_RUN_DIR}:/out:z" \
        "${SURVEY_IMAGE}" run /config.yaml >/dev/null
    survey_track "${name}"
    SURVEY_LOGIT_CONTAINER="${name}"

    # `logit ready` against the config's own admin endpoint, polled -- never a blind sleep. Every
    # config under configs/ sets `admin: { bind: 0.0.0.0:9600 }` precisely so this works; a config
    # without one would hang here rather than silently racing its own listeners.
    local i
    for i in $(seq 1 60); do
        if ${DOCKER} exec "${name}" logit ready --admin http://127.0.0.1:9600 >/dev/null 2>&1; then
            echo "shape-survey: ${name} ready after ${i}s"
            return 0
        fi
        if [ -z "$(${DOCKER} ps -q --filter "name=^${name}$")" ]; then
            ${DOCKER} logs "${name}" 2>&1 | sed 's/^/  [logit] /' >&2
            survey_fail "${name} exited before becoming ready"
        fi
        sleep 1
    done
    ${DOCKER} logs "${name}" 2>&1 | sed 's/^/  [logit] /' >&2
    survey_fail "${name} never reported ready"
}

# stop_logit: SIGTERM (`docker stop -t 60`, so the final `aggregate`/`shape` flush lands in the
# capture rather than being killed mid-window), then the container log into the run dir, then a
# loud failure if the shape output is missing or empty -- an empty shape.log is the one outcome
# that would otherwise produce a clean-looking, contentless summary.
stop_logit() {
    local name="${SURVEY_LOGIT_CONTAINER}"
    [ -n "${name}" ] || survey_fail "stop_logit called with no logit container running"

    echo "shape-survey: stopping ${name} (SIGTERM, up to 60s for the final flush)"
    ${DOCKER} stop -t 60 "${name}" >/dev/null
    ${DOCKER} logs "${name}" >"${SURVEY_RUN_DIR}/logit.log" 2>&1 || true
    ${DOCKER} rm -f "${name}" >/dev/null 2>&1 || true
    SURVEY_LOGIT_CONTAINER=""

    local shape_log="${SURVEY_RUN_DIR}/shape.log"
    [ -f "${shape_log}" ] || survey_fail "no ${shape_log} -- the file_out never wrote anything"
    [ -s "${shape_log}" ] || survey_fail "${shape_log} is empty -- nothing reached the shape taps"
    echo "shape-survey: shape.log is $(wc -c <"${shape_log}") bytes"
}

# ---- python helpers -----------------------------------------------------------------------------

# survey_python <container-suffix> <docker run args...> -- <script> [script args...]: runs one of
# this directory's stdlib-only scripts in a throwaway python container. The tools directory is
# always mounted read-only at /tools and the run dir read-write at /out; anything else (a network,
# a corpus mount) is passed through by the caller before the `--`.
survey_python() {
    local suffix="$1"
    shift
    local name
    name="$(survey_container_name "${suffix}")"
    local args=()
    while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
        args+=("$1")
        shift
    done
    [ "$1" = "--" ] || survey_fail "survey_python: missing -- separating docker args from the script"
    shift

    ${DOCKER} rm -f "${name}" >/dev/null 2>&1 || true
    survey_track "${name}"
    ${DOCKER} run --rm --name "${name}" \
        -v "${ROOT}/tools/shape-survey:/tools:ro,z" \
        -v "${SURVEY_RUN_DIR}:/out:z" \
        "${args[@]}" \
        "${SURVEY_PYTHON_IMAGE}" "$@"
}

# survey_self_test: runs summarize.py's parser self-test. The dispatcher calls this before every
# survey, so a change to `stdio_out`'s human render (the format summarize.py parses) fails here,
# loudly, instead of producing an empty summary from a capture that cost 15 minutes to collect.
survey_self_test() {
    echo "shape-survey: summarize.py --self-test"
    ${DOCKER} run --rm -v "${ROOT}/tools/shape-survey:/tools:ro,z" "${SURVEY_PYTHON_IMAGE}" \
        python3 /tools/summarize.py --self-test
}

# survey_summarize: parses SURVEY_RUN_DIR/shape.log into summary.json + summary.md, and prints the
# markdown. Fails if the parser saw a sketched `logit.shape.*` series (see summarize.py).
#
# provenance.txt is always passed, for the representativeness banner. `source-labels.json` is
# optional and producer-written: a `{"<source component>": {"tier": ..., "format": ...}}` map that
# adds a per-source column to the summary, for a producer whose sources differ in where their
# *format* came from (see producers/demo.sh, where some tiers log in their own software's default
# shape and others in a format this repo authored -- measuring the latter is partly circular, and
# the summary has to say which is which rather than leave it to a reader's memory).
survey_summarize() {
    echo "shape-survey: summarizing ${SURVEY_RUN_DIR}/shape.log"
    local labels=()
    [ -f "${SURVEY_RUN_DIR}/source-labels.json" ] && labels=(--source-labels /out/source-labels.json)
    survey_python summarize -- \
        python3 /tools/summarize.py --shape-log /out/shape.log --out-dir /out \
        --provenance /out/provenance.txt "${labels[@]}"
}
