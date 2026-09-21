#!/bin/sh
# Migrate, seed 25 rows, then hand the process to `opentelemetry-instrument`.
#
# The migration and seed steps run UNinstrumented on purpose: they are setup, not traffic, and
# instrumenting them would put a handful of management-command spans into a capture whose whole
# point is the per-request distribution.
set -e

python manage.py makemigrations pages --noinput
python manage.py migrate --noinput
python manage.py shell -c "from pages.models import Item; Item.objects.exists() or Item.objects.bulk_create([Item(name='item-%d' % i, price=i * 7) for i in range(1, 26)])"

# `runserver --noreload`: Django's own WSGI server, which is what the Django instrumentation
# hooks. `--noreload` because the autoreloader forks a second process, and only one of the two
# would be the instrumented one. Not a production server -- said out loud in provenance.txt --
# but the SERVER span the survey counts attributes on is produced by
# `opentelemetry-instrumentation-django`'s middleware, which is identical under gunicorn.
exec opentelemetry-instrument python manage.py runserver 0.0.0.0:8000 --noreload
