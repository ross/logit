# The demo's app tier (docs/plans/demo-tracing-stack.md): a real Django app, served by gunicorn,
# that also runs as the Celery `worker` and the one-shot `app-migrate` (demo/compose.yaml).
#
# **The `bundle` stage below is the demo's only client-side build step.** Everything else in this
# stack is either a stock image or a `pip install` against PyPI, so `docker compose up --build`
# needs npm registry access for this stage, on top of PyPI/crates.io/apt
# (docs/plans/browser-tracing.md; demo/README.md's "Quick start").

# Bundles browser/telemetry.js (the real @opentelemetry/sdk-trace-web SDK, not a hand-rolled
# script) into one minified file with esbuild: a single static Go binary pulled from npm with no
# further transitive tooling, chosen over a bigger bundler (webpack/rollup/vite) to keep this
# build step's footprint small. `node:22-alpine`, not the full `node:22`, because this stage only
# runs `npm install` + `esbuild`, and its output is one file copied into the final stage below.
FROM docker.io/library/node:22-alpine AS bundle
WORKDIR /bundle
COPY browser/package.json .
# No `package-lock.json` to `npm ci` against, just as this repo doesn't lock Python's transitive
# dependencies (requirements.txt's header comment). `package.json` pins every *direct* dependency
# exactly, and esbuild's output depends only on what got resolved into node_modules at build
# time, not on that resolution being byte-for-byte reproducible run to run.
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
# The bundle stage's only output, read by pages/views.py's `browser_telemetry_js` view with the
# same "open fresh per request" pattern `_serve_svg` uses for graph.svg/architecture.svg.
COPY --chown=demo:demo --from=bundle /bundle/telemetry.bundle.js browser/telemetry.bundle.js

USER demo

EXPOSE 8080
ENTRYPOINT ["gunicorn", "-c", "gunicorn.conf.py", "demoproj.wsgi:application"]
