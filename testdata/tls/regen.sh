#!/bin/bash
# Regenerates this directory's test-only TLS fixtures. Run from inside the dev container
# (script/console) or any host with openssl 3.4 or later -- these are never used at runtime, only by
# logit-pipeline/logit-outputs/logit-inputs/logit-cli test suites, so there's no "no host toolchain
# needed" concern (ADR containerized-development) in running this by hand when the fixtures need
# to change.
#
# Usage: ./regen.sh [GROUP...]
#   base      ca, other-ca, server, client: the original set every TLS test uses
#   rotation  server-b, server-other, client-other, utctime: the reload tests' second set, signed
#             by the existing ca and other-ca, so regenerating it leaves `base` untouched
# With no GROUP, regenerates both. Regenerating `base` invalidates `rotation`, which it signs.
#
# 100-year validity, PKCS#8 keys, no passphrase -- same "generated once, committed" precedent as
# docs/adr/committed-pregenerated-otlp-protobuf.md. Never rotate these for "expiry"; only
# regenerate if the shape of what a test needs changes (a new SAN, a new mTLS case, ...).
set -euo pipefail
cd "$(dirname "$0")"

DAYS=36500
SERVER_SAN="subjectAltName=DNS:localhost,IP:127.0.0.1"

new_key() {
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 \
        -pkeyopt ec_param_enc:named_curve -out "$1"
}

# leaf NAME CA SUBJECT [EXTENSIONS]: a leaf signed by CA.pem/CA.key.
leaf() {
    local name=$1 ca=$2 subject=$3 ext=${4:-}
    new_key "$name.key"
    openssl req -new -key "$name.key" -out "$name.csr" -subj "$subject"
    if [ -n "$ext" ]; then
        openssl x509 -req -in "$name.csr" -CA "$ca.pem" -CAkey "$ca.key" -CAcreateserial \
            -days "$DAYS" -out "$name.pem" -extfile <(printf '%s' "$ext")
    else
        openssl x509 -req -in "$name.csr" -CA "$ca.pem" -CAkey "$ca.key" -CAcreateserial \
            -days "$DAYS" -out "$name.pem"
    fi
    rm -f "$name.csr"
}

base() {
    # Test CA
    new_key ca.key
    openssl req -x509 -new -key ca.key -days "$DAYS" -out ca.pem \
        -subj "/O=logit test fixtures/CN=logit-test-ca"

    # An unrelated second CA, for "wrong CA is rejected" tests. In `base` it signs nothing;
    # `rotation` signs leaves with it.
    new_key other-ca.key
    openssl req -x509 -new -key other-ca.key -days "$DAYS" -out other-ca.pem \
        -subj "/O=logit test fixtures/CN=logit-other-test-ca"

    # Server leaf: SANs cover both localhost and 127.0.0.1, since tests connect by IP.
    leaf server ca "/O=logit test fixtures/CN=localhost" "$SERVER_SAN"

    # Client leaf, for mTLS tests.
    leaf client ca "/O=logit test fixtures/CN=logit-test-client"
}

rotation() {
    # A second server leaf from the same CA: what a renewal produces.
    leaf server-b ca "/O=logit test fixtures/CN=localhost" "$SERVER_SAN"
    # A server leaf from other-ca, for a client whose trusted CA rotates.
    leaf server-other other-ca "/O=logit test fixtures/CN=localhost" "$SERVER_SAN"
    # A client leaf from other-ca, for a listener whose client_ca_file rotates.
    leaf client-other other-ca "/O=logit test fixtures/CN=logit-other-test-client"

    # A self-signed certificate whose notAfter is before 2050, so it's encoded as UTCTime rather
    # than GeneralizedTime. Fixed dates keep the expected value stable across regenerations.
    new_key utctime.key
    openssl req -x509 -new -key utctime.key -out utctime.pem \
        -not_before 20260101000000Z -not_after 20491231235959Z \
        -subj "/O=logit test fixtures/CN=logit-utctime"
    rm -f utctime.key
}

groups=("$@")
[ ${#groups[@]} -eq 0 ] && groups=(base rotation)
for group in "${groups[@]}"; do
    case "$group" in
        base | rotation) "$group" ;;
        *) echo "unknown group '$group' (expected base or rotation)" >&2; exit 2 ;;
    esac
done

rm -f ca.srl other-ca.srl
echo "Regenerated testdata/tls fixtures: ${groups[*]}"
