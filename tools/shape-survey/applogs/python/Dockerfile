# One image, two apps (app_structlog.py, app_pyjson.py) -- same runtime, same HTTP skeleton, two
# logging libraries. The producer runs it twice with different commands and different aliases.
FROM python:3.12-slim

# Pinned: a survey's numbers are a statement about *these* versions of these libraries.
RUN pip install --no-cache-dir structlog==26.1.0 python-json-logger==4.2.0

WORKDIR /app
COPY appbase.py app_structlog.py app_pyjson.py /app/
ENV PYTHONUNBUFFERED=1
