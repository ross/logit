#!/bin/bash
# Usage: certs.sh init
#        certs.sh issue NAME GEN DAYS USAGE DNS_NAME... [--broken]
#        certs.sh probe PORT SERVER_NAME
#
# A run's private CA, in /tls, the run directory's `tls/` mounted by `script/soak`'s driver.
# Each command prints one JSON line last, `{"serial": ..., "not_after": ...}`, which the driver
# records. Keys are EC P-256, and every certificate has v3 extensions, which rustls requires.
# These are throwaway keys, so every file is 0644 and readable by the image's logit user.
#
#   init   the CA (`ca.key`, `ca.pem`) and the probe's client certificate (`probe/`); prints
#          the probe certificate's report
#   issue  NAME's generation GEN into NAME/gen-GEN/{cert.pem,key.pem}, valid DAYS days, for
#          USAGE (server or client) and the DNS names given, then points the relative symlink
#          NAME/live at it in one rename(2), so each file read through NAME/live/ is one
#          generation's or the other's. A reader that reads cert.pem and key.pem one at a
#          time can still pair the two generations. `--broken` writes a key.pem that doesn't match
#          cert.pem, which a TLS reload refuses
#   probe  handshakes with 127.0.0.1:PORT in the namespace this container joined, presenting
#          the probe's client certificate, and reports the certificate the listener served,
#          with `verified`, whether it chains to the CA for SERVER_NAME

set -euo pipefail
# The container runs as the host's uid, which has no home directory in the image.
export HOME=/tmp
cd /tls

key() {
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null
}

# report CERT [VERIFIED]: the JSON line the driver reads.
report() {
    local serial end verified=""
    serial="$(openssl x509 -noout -serial -in "$1")"
    end="$(openssl x509 -noout -enddate -in "$1")"
    if [ -n "${2:-}" ]; then
        verified=", \"verified\": $2"
    fi
    printf '{"serial": "%s", "not_after": %s%s}\n' "${serial#serial=}" \
        "$(date -u -d "${end#notAfter=}" +%s)" "${verified}"
}

# sign DIR CN USAGE DAYS DNS_NAME...: a key and a CA-signed certificate in DIR.
sign() {
    local dir="$1" cn="$2" usage="$3" days="$4"
    shift 4
    local eku san="" dns
    case "${usage}" in
        server) eku=serverAuth ;;
        client) eku=clientAuth ;;
        *) echo "certs.sh: usage must be server or client, got ${usage}" >&2; exit 2 ;;
    esac
    for dns in "$@"; do
        san="${san:+${san},}DNS:${dns}"
    done
    key "${dir}/key.pem"
    openssl req -new -key "${dir}/key.pem" -out "${dir}/req.csr" -subj "/CN=${cn}"
    printf '%s\n' "subjectAltName=${san}" "extendedKeyUsage=${eku}" \
        "basicConstraints=critical,CA:FALSE" "keyUsage=critical,digitalSignature" \
        >"${dir}/ext.cnf"
    openssl x509 -req -in "${dir}/req.csr" -CA ca.pem -CAkey ca.key \
        -set_serial "0x$(openssl rand -hex 8)" -days "${days}" -extfile "${dir}/ext.cnf" \
        -out "${dir}/cert.pem" 2>/dev/null
    rm -f "${dir}/req.csr" "${dir}/ext.cnf"
}

case "${1:-}" in
    init)
        key ca.key
        openssl req -x509 -new -key ca.key -out ca.pem -days 3650 -subj "/CN=logit soak CA" \
            -addext "basicConstraints=critical,CA:TRUE" \
            -addext "keyUsage=critical,keyCertSign,cRLSign"
        mkdir -p probe
        sign probe soak-probe client 3650 soak-probe
        chmod 0644 ca.key ca.pem probe/*
        report probe/cert.pem
        ;;
    issue)
        [ "$#" -ge 6 ] || { echo "usage: certs.sh issue NAME GEN DAYS USAGE DNS_NAME... [--broken]" >&2; exit 2; }
        name="$2" gen="$3" days="$4" usage="$5"
        shift 5
        broken=""
        names=()
        for arg in "$@"; do
            if [ "${arg}" = "--broken" ]; then broken=1; else names+=("${arg}"); fi
        done
        dir="${name}/gen-${gen}"
        mkdir -p "${dir}"
        sign "${dir}" "${name}" "${usage}" "${days}" "${names[@]}"
        if [ -n "${broken}" ]; then
            key "${dir}/key.pem"
        fi
        chmod 0644 "${dir}"/*
        # Relative, so the link resolves in any container that mounts the directory anywhere.
        ln -sfn "gen-${gen}" "${name}/live.next"
        mv -T "${name}/live.next" "${name}/live"
        report "${dir}/cert.pem"
        ;;
    probe)
        [ "$#" -eq 3 ] || { echo "usage: certs.sh probe PORT SERVER_NAME" >&2; exit 2; }
        # s_client's own exit status says nothing the served certificate doesn't: its stdin is
        # empty, so it ends the session as soon as the handshake is done.
        out="$(openssl s_client -connect "127.0.0.1:$2" -servername "$3" \
            -verify_hostname "$3" -CAfile ca.pem -cert probe/cert.pem -key probe/key.pem \
            </dev/null 2>&1 || true)"
        printf '%s\n' "${out}" | openssl x509 -out /tmp/served.pem 2>/dev/null || {
            echo "certs.sh: 127.0.0.1:$2 served no certificate" >&2
            printf '%s\n' "${out}" | tail -n 20 >&2
            exit 1
        }
        verified=false
        if printf '%s\n' "${out}" | grep -q "Verify return code: 0 (ok)"; then
            verified=true
        fi
        report /tmp/served.pem "${verified}"
        ;;
    *)
        echo "usage: certs.sh init | issue NAME GEN DAYS USAGE DNS_NAME... [--broken] | probe PORT SERVER_NAME" >&2
        exit 2
        ;;
esac
