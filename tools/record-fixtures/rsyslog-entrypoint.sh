#!/bin/bash
# Entrypoint for script/record-fixtures's `rsyslog` producer container (debian:bookworm-slim,
# with rsyslog installed fresh at record time -- see record_rsyslog's comment in
# script/record-fixtures for why there's no pre-baked image). Installs rsyslog, starts it against
# the mounted rsyslog.conf (which forwards everything to the "capture" container), gives it a
# moment to bind its Unix domain socket, then logs one message through the system `logger(1)`.
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq rsyslog
mkdir -p /tmp/rsyslog-work
rsyslogd -f /etc/rsyslog-fixture.conf -n -iNONE &
sleep 5
logger -t logit-fixture "hello from rsyslog, captured for logit interop fixtures"
sleep 2
