#!/bin/sh
# Builds freenginx from its release tarball inside a pinned Debian image and runs it in the
# foreground on status.conf: freenginx publishes no container image. The tarball's SHA-256 was
# recorded when the producer was written, and a mismatch fails the run.
# Usage: build-freenginx.sh VERSION SHA256
set -eu
version="$1" sha="$2"
apt-get update -qq
apt-get install -y -qq --no-install-recommends gcc make libc6-dev libpcre2-dev zlib1g-dev \
    curl ca-certificates >/dev/null
cd /tmp
curl -sSfLO "https://freenginx.org/download/freenginx-${version}.tar.gz"
echo "${sha}  freenginx-${version}.tar.gz" | sha256sum -c -
tar xzf "freenginx-${version}.tar.gz"
cd "freenginx-${version}"
./configure --with-http_stub_status_module >/dev/null
make -j"$(nproc)" >/dev/null
make install >/dev/null
/usr/local/nginx/sbin/nginx -v
exec /usr/local/nginx/sbin/nginx -c /status.conf -g 'daemon off;'
