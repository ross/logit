#!/bin/bash
# Entrypoint for script/record-fixtures's collectd and graphite producers (debian:bookworm-slim):
# installs collectd-core, prints its package version for
# testdata/interop/{collectd,graphite}/README.md's provenance tables, then runs collectd against
# the mounted config long enough for its output plugin to flush several times.
#
# RUN_SECONDS (default 10) is how long collectd runs. record_graphite lowers it because
# `write_graphite` holds one connection open for the whole run, so fixture size scales with it.
set -e
RUN_SECONDS="${RUN_SECONDS:-10}"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq collectd-core

# The provenance line: dpkg's record of what got installed.
echo "collectd-core version: $(dpkg-query -W -f='${Version}' collectd-core)"

# `-f` keeps collectd in the foreground so its own errors reach `docker logs`; `-C` points it at
# the bind-mounted config rather than the package's /etc/collectd/collectd.conf.
collectd -C /etc/collectd-fixture.conf -f &
collectd_pid=$!

# record_collectd's captures stop on their `--count` well before this elapses. The SIGTERM lets
# collectd, and so the container, exit cleanly; its shutdown flush lands after the capture has
# stopped, so it isn't part of the corpus.
sleep "${RUN_SECONDS}"
kill "${collectd_pid}" 2>/dev/null || true
wait "${collectd_pid}" 2>/dev/null || true
