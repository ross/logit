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

# Long enough for several read cycles at `Interval 1` to pack and flush the network plugin's send
# buffer more than three times over -- which takes about four seconds, so this is generous on
# purpose rather than tuned. The capture container stops on its own `--count`, well before this
# sleep ends; the SIGTERM below is only so collectd (and therefore this container) exits cleanly
# instead of being killed with the container. Whatever collectd flushes on its way out lands after
# the capture has already gone, so it is *not* part of the corpus.
sleep 10
kill "${collectd_pid}" 2>/dev/null || true
wait "${collectd_pid}" 2>/dev/null || true
