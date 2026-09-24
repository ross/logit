# One image, two apps (app_structlog.py, app_pyjson.py), which the producer runs as two services.
FROM python:3.12-slim

# Pinned: a survey's numbers describe these versions.
RUN pip install --no-cache-dir structlog==26.1.0 python-json-logger==4.2.0

WORKDIR /app
COPY appbase.py app_structlog.py app_pyjson.py /app/
ENV PYTHONUNBUFFERED=1
