#!/bin/bash
# Regenerates this directory's test-only TLS fixtures. Run from inside the dev container
# (script/console) or any host with openssl 3.x -- these are never used at runtime, only by
# logit-outputs/logit-inputs test suites, so there's no "no host toolchain needed" concern (ADR
# containerized-development) in running this by hand when the fixtures need to change.
#
# 100-year validity, PKCS#8 keys, no passphrase -- same "generated once, committed" precedent as
# docs/adr/committed-pregenerated-otlp-protobuf.md. Never rotate these for "expiry"; only
# regenerate if the shape of what a test needs changes (a new SAN, a new mTLS case, ...).
set -euo pipefail
cd "$(dirname "$0")"

DAYS=36500

# Test CA
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 -pkeyopt ec_param_enc:named_curve -out ca.key
openssl req -x509 -new -key ca.key -days "$DAYS" -out ca.pem \
    -subj "/O=logit test fixtures/CN=logit-test-ca"

# An unrelated second CA, for "wrong CA is rejected" tests -- signs nothing else here.
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 -pkeyopt ec_param_enc:named_curve -out other-ca.key
openssl req -x509 -new -key other-ca.key -days "$DAYS" -out other-ca.pem \
    -subj "/O=logit test fixtures/CN=logit-other-test-ca"

# Server leaf: SANs cover both localhost and 127.0.0.1, since tests connect by IP.
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 -pkeyopt ec_param_enc:named_curve -out server.key
openssl req -new -key server.key -out server.csr -subj "/O=logit test fixtures/CN=localhost"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -out server.pem \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")
rm -f server.csr

# Client leaf, for mTLS tests.
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 -pkeyopt ec_param_enc:named_curve -out client.key
openssl req -new -key client.key -out client.csr -subj "/O=logit test fixtures/CN=logit-test-client"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -out client.pem
rm -f client.csr

rm -f ca.srl other-ca.srl
echo "Regenerated testdata/tls/*.pem, *.key"
