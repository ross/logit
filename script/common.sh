# Shared helpers sourced by every other script/* file. Not meant to be run directly.
#
# The project's environment is a container (Dockerfile.dev / compose.yaml, ADR `containerized-development`) -- nothing
# here assumes Rust, LuaJIT, or any other toolchain is installed on the host.

set -e

ROOT="$(cd "$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")/.." && pwd)"
cd "${ROOT}"

# Override with `DOCKER=docker script/...` (in the `docker` group) or `DOCKER=podman script/...`
# (rootless) -- see ADR `containerized-development`. Defaults to sudo because that's what works out of the box here.
DOCKER="${DOCKER:-sudo docker}"
COMPOSE="${DOCKER} compose"

# run <cmd...>: execute a command in the project's environment.
#
# In CI (`$CI`, set by GitHub Actions) the job already runs inside the equivalent image via the
# workflow's `container:` directive, so commands run directly -- wrapping them in another
# `docker compose run` would mean docker-in-docker, which GitHub-hosted runners don't support out
# of the box. Locally, commands run inside the dev container built from Dockerfile.dev.
run() {
    if [ -n "${CI:-}" ]; then
        "$@"
    else
        ${COMPOSE} run --rm dev "$@"
    fi
}

# Build (or rebuild) the dev container image. No-op in CI -- see `run` above.
build_image() {
    if [ -z "${CI:-}" ]; then
        ${COMPOSE} build dev
    fi
}
