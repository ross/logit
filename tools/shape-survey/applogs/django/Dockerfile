FROM python:3.12-slim

# Pinned. The instrumentation packages aren't listed: `opentelemetry-bootstrap -a install` is the
# documented way to select them, and a hand-written list would replace what the survey measures
# with this repo's choice. What landed is frozen into /app/pip-freeze.txt for provenance.txt.
RUN pip install --no-cache-dir \
    Django==6.1.1 \
    requests==2.34.2 \
    opentelemetry-distro==0.65b0 \
    opentelemetry-exporter-otlp==1.44.0
RUN opentelemetry-bootstrap -a install && pip freeze >/app-pip-freeze.txt

WORKDIR /app
COPY . /app/
RUN chmod +x /app/entrypoint.sh && cp /app-pip-freeze.txt /app/pip-freeze.txt
ENV PYTHONUNBUFFERED=1 DJANGO_SETTINGS_MODULE=project.settings
ENTRYPOINT ["/app/entrypoint.sh"]
