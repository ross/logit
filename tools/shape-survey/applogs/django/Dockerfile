FROM python:3.12-slim

# Pinned, resolved against PyPI on 2026-09-20. The instrumentation packages themselves are
# **not** listed here on purpose: `opentelemetry-bootstrap -a install` is the documented way an
# operator selects them (it inspects the installed distributions and installs the matching
# instrumentations), and hand-listing them here would replace the thing the survey is measuring
# with a list this repo chose. What actually landed is frozen into /app/pip-freeze.txt below and
# copied into the run's provenance.txt.
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
