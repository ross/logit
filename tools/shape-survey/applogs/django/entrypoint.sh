#!/bin/sh
# Migrate, seed 25 rows, then hand the process to `opentelemetry-instrument`.
#
# Setup runs uninstrumented, so management-command spans stay out of the per-request
# distribution.
set -e

python manage.py makemigrations pages --noinput
python manage.py migrate --noinput
python manage.py shell -c "from pages.models import Item; Item.objects.exists() or Item.objects.bulk_create([Item(name='item-%d' % i, price=i * 7) for i in range(1, 26)])"

# `--noreload` because the autoreloader forks a second process, and only one would be
# instrumented. `runserver` isn't a production server (provenance.txt says so), but the SERVER span
# comes from the instrumentation's middleware, which is identical under gunicorn.
exec opentelemetry-instrument python manage.py runserver 0.0.0.0:8000 --noreload
