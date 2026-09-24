#!/bin/bash
# Entrypoint for script/record-fixtures's rsyslog producers (debian:bookworm-slim): installs
# rsyslog, runs it against the config mounted at /etc/rsyslog-fixture.conf, which forwards to the
# "capture" container, waits for it to bind its Unix socket, then logs one message via logger(1).
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq rsyslog
mkdir -p /tmp/rsyslog-work
rsyslogd -f /etc/rsyslog-fixture.conf -n -iNONE &
sleep 5
logger -t logit-fixture "hello from rsyslog, captured for logit interop fixtures"
sleep 2
