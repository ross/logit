# Shared plumbing for `script/shape-survey`, sourced by the dispatcher. Not meant to be run
# directly, and deliberately holding **nothing producer-specific**: a new producer is one file,
# `tools/shape-survey/producers/<name>.sh`, plus its config(s) under `configs/`, and it must never
# need an edit here or in the dispatcher. See tools/shape-survey/README.md.
#
# Everything this file creates -- containers, networks, compose projects, images -- is named with a
# `shape-survey-` prefix (the image is `logit:shape-survey`) so cleanup can enumerate exactly what
# this run started. Nothing here ever prunes, and nothing here removes a docker resource it did
# not create itself: this daemon is shared.
#
# ---------------------------------------------------------------------------------------------
# CONCURRENCY: two `script/shape-survey <producer>` invocations may run at the same time
#
# Producers are written in parallel by different people and captures take minutes to a quarter of
# an hour, so two surveys sharing one docker daemon is the normal case, not an edge one.
# Everything a run creates is therefore namespaced by **producer**, not just by the harness:
#
#   network          shape-survey-<producer>-net
#   containers       shape-survey-<producer>-<suffix>   (`survey_container_name`)
#   compose project  shape-survey-<producer>-<suffix>   (`survey_compose`)
#   run directory    perf/results/shape-survey/<producer>/<UTC timestamp>/
#
# and `survey_cleanup` only ever names things out of this invocation's own bookkeeping arrays. The
# one shared resource is the image `logit:shape-survey`, which is content-addressed by the tree it
# is built from: `survey_image` takes an flock so two invocations cannot build it at once, and
# `SHAPE_SURVEY_SKIP_IMAGE=1` skips the build entirely for a second producer in the same sitting.
#
# The network alias `logit` (and a service's own alias, see `survey_start_service`) stays unscoped
# on purpose: it lives *inside* one producer's network, where it cannot collide with anything.
# ---------------------------------------------------------------------------------------------

# The producer whose survey is currently running, set by `survey_begin` (the dispatcher calls it
# once per producer). Everything namespaced below reads it.
SURVEY_PRODUCER=""

# Docker network every container in a run joins, set by `survey_begin` to
# `shape-survey-<producer>-net`. `logit` is reachable on it by the alias `logit`.
SURVEY_NET=""

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

# Compose project suffixes this run has brought up, for `survey_compose`'s one-time-per-project
# teardown registration.
SURVEY_COMPOSE_PROJECTS=()

# ---- naming and bookkeeping ---------------------------------------------------------------------

# survey_container_name <suffix>: every container this harness starts, named so cleanup can only
# ever touch its own -- and so two producers running at once cannot collide on a name. Never build
# a container name any other way.
survey_container_name() {
    echo "shape-survey-${SURVEY_PRODUCER}-$1"
}

# survey_project_name <suffix>: the same namespacing for a compose project (`survey_compose`).
survey_project_name() {
    echo "shape-survey-${SURVEY_PRODUCER}-$1"
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

# ---- per-producer setup, network and cleanup ------------------------------------------------------

# survey_begin <producer>: everything one producer's survey needs before its own function runs --
# the namespacing above, empty bookkeeping, and its own docker network. The dispatcher calls this
# once per producer and `survey_cleanup` after it, so a run of several producers holds one
# producer's resources at a time and two concurrent invocations share none.
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

# survey_network: create the producer's docker network. Never reuses an existing one silently -- a
# leftover `shape-survey-<producer>-net` from a crashed run is fine to reuse (it is ours, by name),
# but a failure for any *other* reason (notably "all predefined address pools have been fully
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

# Shell snippets a producer registers with `survey_on_cleanup` and `survey_cleanup` runs first --
# for a producer whose teardown is not "remove these containers" (a compose stack, say). Kept
# general rather than special-casing any one producer here, per this file's own no-producer-
# specifics rule.
SURVEY_CLEANUP_HOOKS=()

# Suffixes of the long-lived services `survey_start_service` started, so cleanup can write each
# one's log into the run directory before the container is removed.
SURVEY_SERVICES=()

# survey_on_cleanup <shell snippet>: run this at cleanup, before the containers and network go.
# A hook must be idempotent -- cleanup can run after a producer already tore itself down.
survey_on_cleanup() {
    SURVEY_CLEANUP_HOOKS+=("$1")
}

# survey_cleanup: runs any registered hooks, then removes every container this run started and the
# network, in that order. Safe to call twice, and safe to call after a partial run. Only ever names
# containers from SURVEY_CONTAINERS, so it cannot touch another session's.
survey_cleanup() {
    local hook name suffix
    # Service logs first: `docker logs` needs the container to still exist, and a service that
    # died mid-capture is exactly the case whose log is worth keeping.
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
    [ -n "${SURVEY_NET}" ] && ${DOCKER} network rm "${SURVEY_NET}" >/dev/null 2>&1
    return 0
}

# ---- the image ----------------------------------------------------------------------------------

# survey_image: build the release image from the current tree, once per invocation. A survey
# measures *this* tree's `shape`, so the image is rebuilt rather than pulled or assumed --
# `SHAPE_SURVEY_SKIP_IMAGE=1` skips it when the caller knows it is already current (a second
# producer in the same sitting, an iteration that only changed a config, or a second *concurrent*
# invocation, where rebuilding the same tag underneath a running survey is worse than wasteful).
#
# The image tag is the one resource two concurrent invocations share, so the build takes an flock
# on a lockfile beside it: the second invocation waits for the first's build instead of racing it
# through the same layer cache and re-tagging `logit:shape-survey` mid-run. `flock` is coreutils-
# adjacent and present everywhere this harness runs; if it is genuinely missing the build simply
# runs unlocked rather than failing, since a lone invocation needs no lock at all.
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

# survey_out_dir <producer>: creates this run's output directory and sets SURVEY_RUN_DIR to it.
# Under perf/results/ by default, which .gitignore already covers -- raw captures and survey
# output never enter the repo, and nothing here ever writes under testdata/.
#
# **Call it as a plain command and then read `${SURVEY_RUN_DIR}`**, never as `$(survey_out_dir x)`:
# a command substitution runs in a subshell, so the assignment would be thrown away and everything
# downstream would try to write to `/`. It prints the path for the log, not for capture.
survey_out_dir() {
    local producer="$1" stamp
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    SURVEY_RUN_DIR="${SHAPE_SURVEY_OUT:-${ROOT}/perf/results/shape-survey}/${producer}/${stamp}"
    mkdir -p "${SURVEY_RUN_DIR}"
    # The logit container runs as the unprivileged `logit` user (Dockerfile), whose uid has no
    # relationship to whoever owns this checkout -- world-writable is what lets `file_out` create
    # its log here, the same accommodation record_otlp makes for the Collector's file exporter.
    chmod 777 "${SURVEY_RUN_DIR}"
    echo "shape-survey: run directory ${SURVEY_RUN_DIR}"
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

# start_logit <config> [docker run args...]: validates <config> in the image (loudly -- a config
# that does not validate is a broken survey, not a warning), then runs it detached on the
# producer's network under the alias `logit`, with the config mounted read-only and SURVEY_RUN_DIR
# mounted read-write at /out. Waits for readiness via `logit ready` against the config's own
# `admin:` block rather than sleeping. Extra docker run args (an `-e` a config resolves with
# `!env`, say) are passed through.
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

# ---- services under test --------------------------------------------------------------------------

# survey_start_service <suffix> [readiness option] -- <docker run args...>
#
# Starts a long-lived container -- the software a producer is measuring, rather than logit or a
# throwaway script -- on this producer's network, named and tracked like everything else, with its
# image identity appended to provenance.txt and its log captured at teardown.
#
# It joins the network under the **alias `<suffix>`**, so a config can name it (`http://redis:6379`,
# `http://node-exporter:9100/metrics`) without knowing the namespaced container name. That alias is
# scoped to this producer's own network, so two producers can both have a `postgres`.
#
# Readiness is bounded and observed, never a blind sleep. One of:
#
#   --ready-cmd '<cmd>'     polled via `docker exec <container> sh -c '<cmd>'` (a shell in the image)
#   --ready-log '<regex>'   polled via `docker logs | grep -qE` (an image with no shell)
#   --ready-http <url>      polled from a throwaway container *inside the network* (a distroless
#                           exporter: no shell to exec, nothing on the log to match)
#   --ready-timeout <s>     how long to wait, default 60
#
# With none of the three, "started and still running two seconds later" is the bar, and it says so
# -- an unchecked service is a race waiting to be blamed on the measurement.
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

    # Image identity from the started container rather than from the caller's argument list: what
    # ran is what `docker inspect` says ran, digest included where the registry gave one.
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

# survey_http_ready <suffix> <url> <seconds>: one bounded attempt at an HTTP readiness probe from
# *inside* the producer's network, for a service with neither a shell to exec nor a log line to
# match. Any 2xx/3xx counts as ready. Not usually called directly -- `--ready-http` is the door.
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
# every 30s so a 15-minute run is visibly alive rather than an unexplained silence.
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
# name is fine) until it succeeds. Two uses, one primitive: holding a capture open until the thing
# being measured has happened -- "ten scrapes have landed", "the replay finished" -- rather than
# until a guessed clock runs out, and waiting on a readiness condition no `start_logit`/
# `survey_start_service` probe covers (a compose service's health state, say).
#
# A timeout is mandatory and failing it fails the run: a survey that quietly captured half of what
# it meant to is a thin summary that looks like data.
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
# `docker compose` under a namespaced project (`shape-survey-<producer>-<suffix>`), so two
# producers -- or two invocations -- never share a project, a default network or a volume set.
# Everything before the `--` is passed to compose as global options (`-f`, `--env-file`);
# everything after is the command.
#
# On first use of a project suffix it does two things once:
#
#   * **the already-running guard.** The same compose files may name fixed `container_name`s (as
#     demo/compose.yaml does, because `docker_in` follows containers by name), so a second stack
#     cannot come up beside a first whatever the project is called. If those files' *own* default
#     project already has containers up, it belongs to somebody else on this shared daemon and is
#     not this run's to stop -- the run fails and says so.
#   * **teardown registration**, via `survey_on_cleanup`, so a failure anywhere below still brings
#     the stack down. `down -v`: a survey's stack state is this run's (checkpoints, log volumes),
#     and leaving it would make the next run resume a tail mid-file rather than read from the start.
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
# `--append <file>` (a path *relative to the run directory*) appends that markdown to the end of
# summary.md, for a producer whose numbers want a reading the general engine cannot give -- "series
# per scrape, per exporter" is a fact about `prometheus_in` and exporters, not about surveys. The
# producer computes it from summary.json (which carries every series' full value->count table, so
# nothing has to re-parse shape.log) and hands the rendered markdown back here. Typical shape:
#
#     survey_summarize                       # summary.json first
#     ...producer writes ${SURVEY_RUN_DIR}/section.md from it...
#     survey_summarize --append section.md   # and again, with its section on the end
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
