"""One trivial model, existing only to give `/work` (pages/views.py) a real write and a real
count (`docs/plans/demo-richer-traces.md`).

The schema doesn't matter. What matters is that `psycopg` issues a genuine `INSERT`/`SELECT`
against Postgres, each producing its own driver-level CLIENT span
(opentelemetry-instrumentation-psycopg, demo/app/demoproj/telemetry.py) and a real
sqlcommenter-tagged statement in Postgres's jsonlog, which demo/logit.yaml's `postgres_trace`
stage lifts a trace id back out of.
"""

from django.db import models


class WorkRecord(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
