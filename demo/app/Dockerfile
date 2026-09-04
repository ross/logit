# The demo's app tier (docs/plans/demo-tracing-stack.md's workstream B) -- a real Django app, not
# a stdlib bind-mount like the `hello` service it replaces (demo/README.md calls that trade-off
# out: this tier is now something to actually build, not just edit and reload).
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

USER demo

EXPOSE 8080
ENTRYPOINT ["gunicorn", "-c", "gunicorn.conf.py", "demoproj.wsgi:application"]
