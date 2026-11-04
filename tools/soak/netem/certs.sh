#!/bin/bash
# Usage: certs.sh init
#        certs.sh issue NAME GEN DAYS USAGE DNS_NAME... [--broken] [--ca CA]
#        certs.sh trust GEN CA...
#        certs.sh probe PORT SERVER_NAME [CLIENT_CA]
#
# A run's private CA, in /tls, the run directory's `tls/` mounted by `script/soak`'s driver.
# Each command but `trust` prints one JSON line last, `{"serial": ..., "not_after": ...,
# "issuer": ...}`, which the driver records. Keys are EC P-256, and every certificate has v3 extensions, which rustls requires.
# These are throwaway keys, so every file is 0644 and readable by the image's logit user.
#
#   init   CA 1 (`ca.key`, `ca.pem`), the probe's client certificate from it (`probe/`), and
#          trust bundle 1, CA 1 alone, behind the relative symlink `trust/live`; prints the
#          probe certificate's report
#   issue  NAME's generation GEN into NAME/gen-GEN/{cert.pem,key.pem}, valid DAYS days, for
#          USAGE (server or client) and the DNS names given, then points the relative symlink
#          NAME/live at it in one rename(2), so each file read through NAME/live/ is one
#          generation's or the other's. A reader that reads cert.pem and key.pem one at a
#          time can still pair the two generations. `--broken` writes a key.pem that doesn't match
#          cert.pem, which a TLS reload refuses. `--ca CA` signs it with CA generation CA
#          instead of CA 1
#   trust  trust bundle GEN, `trust/gen-GEN/ca.pem`, the CAs given concatenated in order, each
#          CA after 1 made first if it doesn't exist (`ca-N/ca.key`, `ca-N/ca.pem`, and the
#          probe's client certificate from it, `probe-ca-N/`), then points `trust/live` at it
#          in one rename(2); prints `{"bundle": GEN, "cas": [...], "subjects": [...]}`
#   probe  handshakes with 127.0.0.1:PORT in the namespace this container joined, presenting
#          the probe's client certificate from CA CLIENT_CA (1 by default), and reports the
#          certificate the listener served, with `verified`, whether it chains to the trust
#          bundle in force (`trust/live/ca.pem`) for SERVER_NAME

set -euo pipefail
# The container runs as the host's uid, which has no home directory in the image.
export HOME=/tmp
cd /tls

key() {
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null
}

# report CERT [VERIFIED]: the JSON line the driver reads. `issuer` is the issuing CA's common
# name.
report() {
    local serial end issuer verified=""
    serial="$(openssl x509 -noout -serial -in "$1")"
    end="$(openssl x509 -noout -enddate -in "$1")"
    issuer="$(openssl x509 -noout -issuer -nameopt multiline -in "$1" \
        | sed -n 's/^ *commonName *= *//p')"
    if [ -n "${2:-}" ]; then
        verified=", \"verified\": $2"
    fi
    printf '{"serial": "%s", "not_after": %s, "issuer": "%s"%s}\n' "${serial#serial=}" \
        "$(date -u -d "${end#notAfter=}" +%s)" "${issuer}" "${verified}"
}

# ca_dir CA: the directory holding CA generation CA's `ca.pem` and `ca.key`; CA 1's is the top.
ca_dir() {
    if [ "$1" = 1 ]; then echo .; else echo "ca-$1"; fi
}

# make_ca CA: CA generation CA, its common name numbered after 1, and the probe's client
# certificate from it, unless it exists.
make_ca() {
    local dir cn="logit soak CA"
    dir="$(ca_dir "$1")"
    [ -f "${dir}/ca.pem" ] && return 0
    [ "$1" = 1 ] || cn="logit soak CA $1"
    mkdir -p "${dir}"
    key "${dir}/ca.key"
    openssl req -x509 -new -key "${dir}/ca.key" -out "${dir}/ca.pem" -days 3650 -subj "/CN=${cn}" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign"
    chmod 0644 "${dir}/ca.key" "${dir}/ca.pem"
    local probe=probe
    [ "$1" = 1 ] || probe="probe-ca-$1"
    mkdir -p "${probe}"
    sign "${probe}" soak-probe client 3650 "$1" soak-probe
    chmod 0644 "${probe}"/*
}

# link NAME TARGET: points the relative symlink NAME/live at NAME/TARGET in one rename(2), so
# each file read through NAME/live/ is one generation's or the other's.
link() {
    ln -sfn "$2" "$1/live.next"
    mv -T "$1/live.next" "$1/live"
}

# sign DIR CN USAGE DAYS CA DNS_NAME...: a key and a certificate CA generation CA signed, in
# DIR.
sign() {
    local dir="$1" cn="$2" usage="$3" days="$4" ca
    ca="$(ca_dir "$5")"
    shift 5
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
    openssl x509 -req -in "${dir}/req.csr" -CA "${ca}/ca.pem" -CAkey "${ca}/ca.key" \
        -set_serial "0x$(openssl rand -hex 8)" -days "${days}" -extfile "${dir}/ext.cnf" \
        -out "${dir}/cert.pem" 2>/dev/null
    rm -f "${dir}/req.csr" "${dir}/ext.cnf"
}

case "${1:-}" in
    init)
        make_ca 1
        mkdir -p trust/gen-1
        cp ca.pem trust/gen-1/ca.pem
        chmod 0644 trust/gen-1/ca.pem
        link trust gen-1
        report probe/cert.pem
        ;;
    issue)
        [ "$#" -ge 6 ] || { echo "usage: certs.sh issue NAME GEN DAYS USAGE DNS_NAME... [--broken] [--ca CA]" >&2; exit 2; }
        name="$2" gen="$3" days="$4" usage="$5"
        shift 5
        broken="" ca=1
        names=()
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --broken) broken=1 ;;
                --ca) ca="$2"; shift ;;
                *) names+=("$1") ;;
            esac
            shift
        done
        [ -f "$(ca_dir "${ca}")/ca.pem" ] || { echo "certs.sh: no CA ${ca}; a trust bundle makes it" >&2; exit 1; }
        dir="${name}/gen-${gen}"
        mkdir -p "${dir}"
        sign "${dir}" "${name}" "${usage}" "${days}" "${ca}" "${names[@]}"
        if [ -n "${broken}" ]; then
            key "${dir}/key.pem"
        fi
        chmod 0644 "${dir}"/*
        # Relative, so the link resolves in any container that mounts the directory anywhere.
        link "${name}" "gen-${gen}"
        report "${dir}/cert.pem"
        ;;
    trust)
        [ "$#" -ge 3 ] || { echo "usage: certs.sh trust GEN CA..." >&2; exit 2; }
        gen="$2"
        shift 2
        dir="trust/gen-${gen}"
        mkdir -p "${dir}"
        : >"${dir}/ca.pem.next"
        cas="" subjects=""
        for ca in "$@"; do
            make_ca "${ca}"
            cat "$(ca_dir "${ca}")/ca.pem" >>"${dir}/ca.pem.next"
            subject="$(openssl x509 -noout -subject -nameopt multiline -in "$(ca_dir "${ca}")/ca.pem" \
                | sed -n 's/^ *commonName *= *//p')"
            cas="${cas:+${cas}, }${ca}"
            subjects="${subjects:+${subjects}, }\"${subject}\""
        done
        mv "${dir}/ca.pem.next" "${dir}/ca.pem"
        chmod 0644 "${dir}/ca.pem"
        link trust "gen-${gen}"
        printf '{"bundle": %s, "cas": [%s], "subjects": [%s]}\n' "${gen}" "${cas}" "${subjects}"
        ;;
    probe)
        [ "$#" -eq 3 ] || [ "$#" -eq 4 ] || { echo "usage: certs.sh probe PORT SERVER_NAME [CLIENT_CA]" >&2; exit 2; }
        client=probe
        [ "${4:-1}" = 1 ] || client="probe-ca-$4"
        # s_client's own exit status says nothing the served certificate doesn't: its stdin is
        # empty, so it ends the session as soon as the handshake is done.
        out="$(openssl s_client -connect "127.0.0.1:$2" -servername "$3" \
            -verify_hostname "$3" -CAfile trust/live/ca.pem -cert "${client}/cert.pem" \
            -key "${client}/key.pem" </dev/null 2>&1 || true)"
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
        echo "usage: certs.sh init | issue NAME GEN DAYS USAGE DNS_NAME... [--broken] [--ca CA] | trust GEN CA... | probe PORT SERVER_NAME [CLIENT_CA]" >&2
        exit 2
        ;;
esac
