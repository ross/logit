#!/bin/bash
# Entrypoint for script/record-fixtures's `collectd`/`graphite` producer containers
# (debian:bookworm-slim, with collectd-core installed fresh at record time -- see
# record_collectd's comment in script/record-fixtures for why there's no pre-baked image).
# Installs collectd-core, prints the exact package version (which is what
# testdata/interop/{collectd,graphite}/README.md's provenance tables record), then runs collectd
# in the foreground against the mounted config long enough for its output plugin to flush several
# times to the "capture" container.
#
# `RUN_SECONDS` (default 10, `record_collectd`'s original figure) is how long collectd stays up
# before this script kills it -- overridable so `record_graphite`'s `write_graphite` capture,
# which (unlike `network`'s UDP datagrams) keeps one TCP connection open for as long as collectd
# runs, doesn't have to commit ten seconds' worth of `Interval 1` read cycles just to get a few:
# testdata/interop/README.md's size discipline applies here too.
set -e
RUN_SECONDS="${RUN_SECONDS:-10}"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq collectd-core

# The provenance line: dpkg's own record of what actually got installed, not a guess.
echo "collectd-core version: $(dpkg-query -W -f='${Version}' collectd-core)"

# `-f` keeps collectd in the foreground so its own errors reach `docker logs`; `-C` points it at
# the bind-mounted config rather than the package's /etc/collectd/collectd.conf.
collectd -C /etc/collectd-fixture.conf -f &
collectd_pid=$!

# Long enough for several read cycles at `Interval 1` to fill and flush an output plugin's buffer
# (or, for `write_graphite`'s TCP connection, to just write several times) more than once over.
# `record_collectd`'s two captures stop on their own `--count`, well before the default 10s
# elapses; the SIGTERM below is only so collectd (and therefore this container) exits cleanly
# instead of being killed with the container. Whatever collectd flushes on its way out lands after
# the capture has already gone, so it is *not* part of the corpus.
sleep "${RUN_SECONDS}"
kill "${collectd_pid}" 2>/dev/null || true
wait "${collectd_pid}" 2>/dev/null || true
