# Shared plumbing for `script/shape-survey`, sourced by the dispatcher. It holds nothing
# producer-specific: a new producer must never need an edit here or in the dispatcher. See
# tools/shape-survey/README.md, "Adding a producer".
#
# The docker daemon is shared, so everything here follows README "Two surveys at once": every
# resource is namespaced `shape-survey-<producer>-*`, cleanup names only what this invocation's
# own bookkeeping arrays recorded, and nothing ever prunes or removes a resource it didn't create.
# The network alias `logit`, and each service's alias, stay unscoped: they live inside one
# producer's network, where they can't collide.

# The producer whose survey is running, set by `survey_begin`. Every namespaced name reads it.
SURVEY_PRODUCER=""

# The network every container in a run joins, set by `survey_begin`.
SURVEY_NET=""

# The one resource concurrent invocations share (see `survey_image`).
SURVEY_IMAGE="logit:shape-survey"

# The scripts here are stdlib-only so this image needs no `pip install` step.
SURVEY_PYTHON_IMAGE="python:3.12-slim"

# Every container this run started, appended by `survey_track`, for `survey_cleanup`.
SURVEY_CONTAINERS=()

# The current run directory on the host, set by `survey_out_dir` and mounted at /out.
SURVEY_RUN_DIR=""

# Set by `start_logit`, cleared by `stop_logit`.
SURVEY_LOGIT_CONTAINER=""

# Compose project suffixes this run has brought up, so `survey_compose` registers each teardown
# once.
SURVEY_COMPOSE_PROJECTS=()

# ---- naming and bookkeeping ---------------------------------------------------------------------

# survey_container_name <suffix>: the namespaced container name. Never build one any other way,
# or two concurrent producers can collide and cleanup can touch another run's container.
survey_container_name() {
    echo "shape-survey-${SURVEY_PRODUCER}-$1"
}

# survey_project_name <suffix>: the same namespacing for a compose project (`survey_compose`).
survey_project_name() {
    echo "shape-survey-${SURVEY_PRODUCER}-$1"
}

# survey_track <name>: remember a container for cleanup. A producer that starts its own container
# calls this right after `docker run -d`.
survey_track() {
    SURVEY_CONTAINERS+=("$1")
}

survey_fail() {
    echo "shape-survey: $*" >&2
    return 1
}

# ---- per-producer setup, network and cleanup ------------------------------------------------------

# survey_begin <producer>: sets the namespace, empties the bookkeeping, and creates the network.
# The dispatcher calls it before each producer and `survey_cleanup` after it.
survey_begin() {
    SURVEY_PRODUCER="$1"
    SURVEY_NET="shape-survey-${SURVEY_PRODUCER}-net"
    SURVEY_CONTAINERS=()
    SURVEY_CLEANUP_HOOKS=()
    SURVEY_SERVICES=()
    SURVEY_COMPOSE_PROJECTS=()
    SURVEY_LOGIT_CONTAINER=""
    survey_network
}

# survey_network: create the producer's network. A leftover one from a crashed run is ours by name
# and is reused. Any other failure, notably "all predefined address pools have been fully
# subnetted", stops the run: the fix is someone else releasing a network, never deleting one here.
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

# Snippets registered with `survey_on_cleanup`, which `survey_cleanup` runs before removing
# containers.
SURVEY_CLEANUP_HOOKS=()

# Services `survey_start_service` started, so cleanup captures each log before removing it.
SURVEY_SERVICES=()

# survey_on_cleanup <shell snippet>: run this at cleanup, before the containers and network go,
# for a teardown that isn't "remove these containers". A hook must be idempotent: cleanup can run
# after a producer already tore itself down.
survey_on_cleanup() {
    SURVEY_CLEANUP_HOOKS+=("$1")
}

# survey_cleanup: captures service logs, runs hooks, then removes this run's containers and the
# network. Safe to call twice and after a partial run. It names only containers from
# SURVEY_CONTAINERS, so it can't touch another session's.
survey_cleanup() {
    local hook name suffix
    # Service logs first: `docker logs` needs the container to still exist, and a service that
    # died mid-capture is the one whose log matters.
    for suffix in "${SURVEY_SERVICES[@]-}"; do
        [ -n "${suffix}" ] || continue
        survey_service_logs "${suffix}" || true
    done
    SURVEY_SERVICES=()
    for hook in "${SURVEY_CLEANUP_HOOKS[@]-}"; do
        [ -n "${hook}" ] || continue
        eval "${hook}" || true
    done
    SURVEY_CLEANUP_HOOKS=()
    for name in "${SURVEY_CONTAINERS[@]-}"; do
        [ -n "${name}" ] || continue
        ${DOCKER} rm -f "${name}" >/dev/null 2>&1 || true
    done
    SURVEY_CONTAINERS=()
    SURVEY_COMPOSE_PROJECTS=()
    # `|| true`, not an `&&` tail: cleanup runs twice on a normal run (the dispatcher's loop, then
    # the EXIT trap), so the second `network rm` always fails, and a failing last command of an
    # AND-OR list trips `set -e`, making a successful survey exit 1 from inside its own trap.
    if [ -n "${SURVEY_NET}" ]; then
        ${DOCKER} network rm "${SURVEY_NET}" >/dev/null 2>&1 || true
    fi
    return 0
}

# ---- the image ----------------------------------------------------------------------------------

# survey_image: build the release image from the current tree, once per invocation, because a
# survey measures this tree's `shape`. `SHAPE_SURVEY_SKIP_IMAGE=1` reuses the existing image; a
# second concurrent invocation must set it rather than re-tag the image under a running survey.
#
# The build holds an flock so two invocations can't race the same tag. Without `flock` it runs
# unlocked, since a lone invocation needs no lock.
survey_image() {
    if [ -n "${SHAPE_SURVEY_SKIP_IMAGE:-}" ]; then
        echo "shape-survey: SHAPE_SURVEY_SKIP_IMAGE set -- using the existing ${SURVEY_IMAGE}"
        ${DOCKER} image inspect "${SURVEY_IMAGE}" >/dev/null 2>&1 ||
            survey_fail "SHAPE_SURVEY_SKIP_IMAGE is set but ${SURVEY_IMAGE} does not exist." \
                "Run one survey without it first (it builds the image), or unset it."
        return 0
    fi
    if [ -n "${SURVEY_IMAGE_BUILT:-}" ]; then
        return 0
    fi
    echo "shape-survey: building ${SURVEY_IMAGE} from the current tree (Dockerfile)"
    local lock="${TMPDIR:-/tmp}/shape-survey-image.lock"
    if command -v flock >/dev/null 2>&1; then
        ( flock 9 && ${DOCKER} build -f "${ROOT}/Dockerfile" -t "${SURVEY_IMAGE}" "${ROOT}" ) 9>"${lock}"
    else
        ${DOCKER} build -f "${ROOT}/Dockerfile" -t "${SURVEY_IMAGE}" "${ROOT}"
    fi
    SURVEY_IMAGE_BUILT=1
}

# ---- run directories and provenance -------------------------------------------------------------

# survey_out_dir <producer>: creates this run's directory (under the gitignored perf/results/ by
# default) and sets SURVEY_RUN_DIR to it.
#
# Call it as a plain command, never as `$(survey_out_dir x)`: a command substitution runs in a
# subshell, so the assignment is lost and everything downstream writes to `/`. It prints the path
# for the log, not for capture.
survey_out_dir() {
    local producer="$1" stamp
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    SURVEY_RUN_DIR="${SHAPE_SURVEY_OUT:-${ROOT}/perf/results/shape-survey}/${producer}/${stamp}"
    mkdir -p "${SURVEY_RUN_DIR}"
    # The container's unprivileged `logit` user has an unrelated uid; world-writable lets
    # `file_out` create its log here.
    chmod 777 "${SURVEY_RUN_DIR}"
    echo "shape-survey: run directory ${SURVEY_RUN_DIR}"
}

# survey_provenance <producer> <representativeness>: writes the common half of provenance.txt
# (date, repo SHA and dirty state, docker and image identities). The producer appends its own
# software versions to the same file.
#
# `<representativeness>` is required: summarize.py prints it as summary.md's banner. See README
# "Representativeness is structural".
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

# start_logit <config> [docker run args...]: fails the run if <config> doesn't validate, then runs
# it detached under the alias `logit`, with the config read-only and SURVEY_RUN_DIR read-write at
# /out, and waits on `logit ready`. Extra docker run args (an `-e` a config resolves with `!env`)
# are passed through.
start_logit() {
    local config="$1" name
    shift
    name="$(survey_container_name logit)"

    echo "shape-survey: validating ${config}"
    ${DOCKER} run --rm "$@" -v "${config}:/config.yaml:ro,z" "${SURVEY_IMAGE}" validate /config.yaml ||
        survey_fail "${config} failed 'logit validate' -- fix the config, not this check"

    ${DOCKER} rm -f "${name}" >/dev/null 2>&1 || true
    ${DOCKER} run -d --name "${name}" --network "${SURVEY_NET}" --network-alias logit \
        -v "${config}:/config.yaml:ro,z" \
        -v "${SURVEY_RUN_DIR}:/out:z" \
        "$@" \
        "${SURVEY_IMAGE}" run /config.yaml >/dev/null
    survey_track "${name}"
    SURVEY_LOGIT_CONTAINER="${name}"

    # Every config under configs/ sets `admin: { bind: 0.0.0.0:9600 }` for this probe; a config
    # without one fails here after 60 s rather than racing its own listeners.
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

# stop_logit: `docker stop -t 60` so the final `aggregate` flush lands, captures logit.log, and
# fails if shape.log is missing or empty, which would otherwise summarize to a clean-looking
# nothing.
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

# ---- services under test --------------------------------------------------------------------------

# survey_start_service <suffix> [readiness option] -- <docker run args...>
#
# Starts a long-lived service under test, tracked for cleanup, with its image identity appended
# to provenance.txt and its log captured at teardown. It joins the network under the alias
# `<suffix>`, so a config names it (`http://redis:6379`) without the namespaced container name.
#
# Readiness is bounded and observed. One of:
#
#   --ready-cmd '<cmd>'     polled via `docker exec <container> sh -c '<cmd>'` (a shell in the image)
#   --ready-log '<regex>'   polled via `docker logs | grep -qE` (an image with no shell)
#   --ready-http <url>      polled from a throwaway container inside the network (a distroless
#                           exporter: no shell to exec, nothing on the log to match)
#   --ready-timeout <s>     how long to wait, default 60
#
# With none of the three, the bar is "still running two seconds later", and the log says so.
survey_start_service() {
    local suffix="$1"
    shift
    local kind="" arg="" timeout=60
    while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
        case "$1" in
        --ready-cmd | --ready-log | --ready-http)
            kind="${1#--ready-}"
            arg="$2"
            shift 2
            ;;
        --ready-timeout)
            timeout="$2"
            shift 2
            ;;
        *) survey_fail "survey_start_service: unknown option '$1'" ;;
        esac
    done
    [ "$1" = "--" ] || survey_fail "survey_start_service: missing -- before the docker run args"
    shift

    local name
    name="$(survey_container_name "${suffix}")"
    ${DOCKER} rm -f "${name}" >/dev/null 2>&1 || true
    survey_track "${name}"
    SURVEY_SERVICES+=("${suffix}")
    echo "shape-survey: starting service ${suffix} (${name})"
    ${DOCKER} run -d --name "${name}" --network "${SURVEY_NET}" --network-alias "${suffix}" \
        "$@" >/dev/null || survey_fail "could not start service ${suffix}"

    # Read the image from the started container, not the caller's args: what ran is what
    # `docker inspect` says ran.
    {
        local image digest
        image="$(${DOCKER} inspect --format '{{.Config.Image}}' "${name}" 2>/dev/null || echo unknown)"
        digest="$(${DOCKER} inspect --format '{{index .RepoDigests 0}}' "${image}" 2>/dev/null || true)"
        [ -n "${digest}" ] || digest="$(${DOCKER} inspect --format '{{.Image}}' "${name}" 2>/dev/null || echo unknown)"
        echo "service ${suffix}: ${image} (${digest})"
    } >>"${SURVEY_RUN_DIR}/provenance.txt"

    local waited=0 ok=0
    while [ "${waited}" -lt "${timeout}" ]; do
        if [ -z "$(${DOCKER} ps -q --filter "name=^${name}$")" ]; then
            ${DOCKER} logs "${name}" 2>&1 | tail -40 | sed "s/^/  [${suffix}] /" >&2
            survey_fail "service ${suffix} exited before becoming ready"
        fi
        case "${kind}" in
        cmd) ${DOCKER} exec "${name}" sh -c "${arg}" >/dev/null 2>&1 && ok=1 ;;
        log) ${DOCKER} logs "${name}" 2>&1 | grep -qE "${arg}" && ok=1 ;;
        http) survey_http_ready "${suffix}" "${arg}" 5 && ok=1 ;;
        *)
            [ "${waited}" -ge 2 ] && ok=1
            ;;
        esac
        [ "${ok}" -eq 1 ] && break
        sleep 1
        waited=$((waited + 1))
    done
    if [ "${ok}" -ne 1 ]; then
        ${DOCKER} logs "${name}" 2>&1 | tail -40 | sed "s/^/  [${suffix}] /" >&2
        survey_fail "service ${suffix} never became ready within ${timeout}s (${kind:-liveness} check)"
    fi
    if [ -z "${kind}" ]; then
        echo "shape-survey: ${suffix} is up (NO readiness check was given -- liveness only)"
    else
        echo "shape-survey: ${suffix} ready after ${waited}s (${kind})"
    fi
}

# survey_http_ready <suffix> <url> <seconds>: one bounded HTTP probe from inside the producer's
# network; any status below 400 counts. `--ready-http` calls it.
survey_http_ready() {
    local suffix="$1" url="$2" seconds="$3"
    ${DOCKER} run --rm --network "${SURVEY_NET}" "${SURVEY_PYTHON_IMAGE}" python3 -c '
import sys, time, urllib.request
url, deadline = sys.argv[1], time.monotonic() + float(sys.argv[2])
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            if r.status < 400:
                sys.exit(0)
    except Exception:
        pass
    time.sleep(0.5)
sys.exit(1)
' "${url}" "${seconds}" >/dev/null 2>&1
}

# survey_service_logs <suffix>: that service's container log into the run directory. Called for
# every started service at cleanup; call it directly to snapshot one mid-run.
survey_service_logs() {
    local suffix="$1" name
    name="$(survey_container_name "${suffix}")"
    [ -n "${SURVEY_RUN_DIR}" ] || return 0
    ${DOCKER} logs "${name}" >"${SURVEY_RUN_DIR}/service-${suffix}.log" 2>&1 || true
}

# ---- capture windows ------------------------------------------------------------------------------

# survey_capture_for <seconds>: hold the capture open for a fixed window, with a progress line
# every 30s.
survey_capture_for() {
    local seconds="$1" elapsed=0 step
    echo "shape-survey: capturing for ${seconds}s"
    while [ "${elapsed}" -lt "${seconds}" ]; do
        step=$((seconds - elapsed))
        [ "${step}" -gt 30 ] && step=30
        sleep "${step}"
        elapsed=$((elapsed + step))
        [ "${elapsed}" -lt "${seconds}" ] && echo "shape-survey:   ${elapsed}s / ${seconds}s"
    done
    echo "shape-survey: capture window closed after ${seconds}s"
}

# survey_capture_until '<cmd>' <timeout-seconds>: poll <cmd> (through `eval`, so a shell function
# name works) every 5s until it succeeds: to hold a capture open until the measured thing has
# happened, or to wait on a readiness condition no other probe covers. The timeout is mandatory
# and failing it fails the run, because a half capture looks like data.
survey_capture_until() {
    local cmd="$1" timeout="$2" waited=0
    echo "shape-survey: waiting for: ${cmd} (max ${timeout}s)"
    while ! eval "${cmd}" >/dev/null 2>&1; do
        sleep 5
        waited=$((waited + 5))
        [ $((waited % 30)) -eq 0 ] && echo "shape-survey:   ${waited}s / ${timeout}s, still waiting"
        [ "${waited}" -ge "${timeout}" ] &&
            survey_fail "condition never held within ${timeout}s: ${cmd}"
    done
    echo "shape-survey: condition held after ${waited}s"
}

# ---- compose stacks -------------------------------------------------------------------------------

# survey_compose <project-suffix> <compose global args...> -- <compose args...>
#
# `docker compose` under project `shape-survey-<producer>-<suffix>`. Arguments before the `--` are
# compose global options (`-f`, `--env-file`); arguments after it are the command.
#
# On first use of a suffix it does two things:
#
#   * Fails if the files' own default project already has containers up. Compose files may fix
#     `container_name`s (demo/compose.yaml does, for `docker_in`), so a second stack can't come up
#     beside it, and that stack belongs to somebody else on this shared daemon.
#   * Registers `down -v --remove-orphans` as a cleanup hook. `-v` because a leftover volume would
#     make the next run resume a tail mid-file.
survey_compose() {
    local suffix="$1"
    shift
    local globals=()
    while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
        globals+=("$1")
        shift
    done
    [ "$1" = "--" ] || survey_fail "survey_compose: missing -- before the compose command"
    shift

    local project seen=0 known
    project="$(survey_project_name "${suffix}")"
    for known in "${SURVEY_COMPOSE_PROJECTS[@]-}"; do
        [ "${known}" = "${suffix}" ] && seen=1
    done
    if [ "${seen}" -eq 0 ]; then
        if [ -n "$(${DOCKER} compose "${globals[@]}" ps -q 2>/dev/null)" ]; then
            survey_fail "a stack from these compose files is already running on this daemon, under" \
                "their own default project. It is not this run's to stop -- bring it down yourself" \
                "if it is yours, or wait for whoever is using it."
        fi
        SURVEY_COMPOSE_PROJECTS+=("${suffix}")
        survey_on_cleanup "${DOCKER} compose -p '${project}' $(printf "'%s' " "${globals[@]}")down -v --remove-orphans >/dev/null 2>&1"
    fi
    ${DOCKER} compose -p "${project}" "${globals[@]}" "$@"
}

# ---- python helpers -----------------------------------------------------------------------------

# survey_python <container-suffix> <docker run args...> -- <cmd> [args...]: runs a command in a
# throwaway python container with this directory read-only at /tools and the run directory at
# /out. Anything else (a network, a corpus mount) goes before the `--`.
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

# survey_self_test: runs `summarize.py --self-test`, so a change to `stdio_out`'s human render
# fails before any capture rather than after one.
survey_self_test() {
    echo "shape-survey: summarize.py --self-test"
    ${DOCKER} run --rm -v "${ROOT}/tools/shape-survey:/tools:ro,z" "${SURVEY_PYTHON_IMAGE}" \
        python3 /tools/summarize.py --self-test
}

# survey_summarize [--append <file>]: parses shape.log into summary.json and summary.md, with
# provenance.txt's representativeness banner. Fails on a sketched `logit.shape.*` series.
#
# An optional producer-written `source-labels.json`, a `{"<source component>": {"tier": ...,
# "format": ...}}` map, adds a per-source table (see producers/demo.sh). `--append <file>`, a path
# relative to the run directory, appends the producer's own markdown section, computed from a
# first call's summary.json (README "Your own summary section").
survey_summarize() {
    local append=()
    if [ "${1:-}" = "--append" ]; then
        [ -f "${SURVEY_RUN_DIR}/$2" ] || survey_fail "survey_summarize --append: no ${SURVEY_RUN_DIR}/$2"
        append=(--append "/out/$2")
    fi
    echo "shape-survey: summarizing ${SURVEY_RUN_DIR}/shape.log"
    local labels=()
    [ -f "${SURVEY_RUN_DIR}/source-labels.json" ] && labels=(--source-labels /out/source-labels.json)
    survey_python summarize -- \
        python3 /tools/summarize.py --shape-log /out/shape.log --out-dir /out \
        --provenance /out/provenance.txt "${labels[@]}" "${append[@]}"
}
