#!/bin/bash
# Entrypoint for script/record-fixtures's `collectd` producer container (debian:bookworm-slim, with
# collectd-core installed fresh at record time -- see record_collectd's comment in
# script/record-fixtures for why there's no pre-baked image). Installs collectd-core, prints the
# exact package version (which is what testdata/interop/collectd/README.md's provenance table
# records), then runs collectd in the foreground against the mounted config long enough for its
# `network` plugin to fill and flush several datagrams to the "capture" container.
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq collectd-core

# The provenance line: dpkg's own record of what actually got installed, not a guess.
echo "collectd-core version: $(dpkg-query -W -f='${Version}' collectd-core)"

# `-f` keeps collectd in the foreground so its own errors reach `docker logs`; `-C` points it at
# the bind-mounted config rather than the package's /etc/collectd/collectd.conf.
collectd -C /etc/collectd-fixture.conf -f &
collectd_pid=$!

# Long enough for several read cycles at `Interval 1` to fill the network plugin's 1452-byte send
# buffer more than three times over; the capture container stops on its own --count, so overshooting
# here costs nothing but a few seconds. SIGTERM at the end makes collectd flush whatever is left in
# the buffer on the way out, which is a real sender behaviour worth having in the corpus.
sleep 30
kill "${collectd_pid}" 2>/dev/null || true
wait "${collectd_pid}" 2>/dev/null || true
