# The demo's app tier (docs/plans/demo-tracing-stack.md's workstream B) -- a real Django app, not
# a stdlib bind-mount like the `hello` service it replaces (demo/README.md calls that trade-off
# out: this tier is now something to actually build, not just edit and reload).
#
# **This tier's `bundle` stage below is the demo's first client-side build step** -- everything
# else in this stack is either a stock image or a `pip install` against PyPI.
# `docker compose up --build` now needs npm registry access too, not just PyPI/crates.io/apt:
# real, worth-flagging new build requirement for this stack, not a footnote
# (docs/plans/browser-tracing.md's Workstream C).

# Bundles browser/telemetry.js (the real @opentelemetry/sdk-trace-web SDK, not a hand-rolled
# script) into one minified file, via esbuild -- a single static Go binary pulled from npm with no
# further transitive tooling, chosen over a bigger bundler (webpack/rollup/vite) specifically to
# keep this new build step's own footprint small. `node:22-alpine`, not the full `node:22`: this
# stage exists only to run `npm install` + `esbuild`, and its output is one file copied into the
# final stage below -- Alpine's smaller pull cost isn't paid by anything downstream.
FROM docker.io/library/node:22-alpine AS bundle
WORKDIR /bundle
COPY browser/package.json .
# No `package-lock.json` to `npm ci` against -- this repo doesn't lock Python's transitive
# dependencies either (requirements.txt's own header comment); `package.json`'s exact pins on
# every *direct* dependency are the same discipline applied here, and esbuild's output only ever
# depends on what actually got resolved into node_modules at build time, not on that resolution
# being byte-for-byte reproducible run to run.
RUN npm install
COPY browser/telemetry.js .
RUN npm run build

FROM docker.io/library/python:3.13-slim

# Runs as a non-root user, same posture as the release `logit` image itself (../../Dockerfile).
# Created before the app files are copied in, so `COPY --chown` below needs no separate `chown -R`
# pass over an already-populated directory.
RUN useradd --system --create-home --home-dir /app demo

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=demo:demo manage.py .
COPY --chown=demo:demo demoproj/ demoproj/
COPY --chown=demo:demo pages/ pages/
COPY --chown=demo:demo gunicorn.conf.py .
# The bundle stage's only output -- read by pages/views.py's `browser_telemetry_js` view, the
# same "open fresh per request" pattern `_serve_svg` already uses for graph.svg/architecture.svg.
COPY --chown=demo:demo --from=bundle /bundle/telemetry.bundle.js browser/telemetry.bundle.js

USER demo

EXPOSE 8080
ENTRYPOINT ["gunicorn", "-c", "gunicorn.conf.py", "demoproj.wsgi:application"]
