#!/bin/bash
# Usage: netem.sh set <netem args...> | clear | show | reset [port]
#
# Runs inside the target container's network namespace. A root qdisc on the default-route
# interface shapes that container's egress only.
#
#   set <args>    replace the root qdisc with `netem <args>`, then print it
#   clear         delete the root qdisc, then print what's left; clearing an absent one succeeds
#   show          print the root qdisc
#   reset [port]  close every established TCP connection in the namespace, or each whose local
#                 or remote port is `port`, with `ss -K`, which sends the peer a reset; print
#                 each socket it closed, then `{"killed": N}`. `ss -K` needs CAP_NET_ADMIN and a
#                 kernel built with CONFIG_INET_DIAG_DESTROY; without it, it closes and prints
#                 nothing, and N is 0. An `ss` that exits non-zero or reports a netlink or
#                 SOCK_DESTROY error (an LSM denial, say) makes `reset` exit 1 with its stderr.

set -euo pipefail

if [ "${1:-}" = "reset" ]; then
    filter=()
    if [ -n "${2:-}" ]; then
        filter=("(" sport = ":${2}" or dport = ":${2}" ")")
    fi
    errs="$(mktemp)"
    status=0
    closed="$(ss -K -H -t -n state established "${filter[@]}" 2>"${errs}")" || status=$?
    cat "${errs}" >&2
    if [ "${status}" -ne 0 ] || grep -q -e 'Cannot talk to rtnetlink' -e 'SOCK_DESTROY answers' "${errs}"; then
        echo "netem: ss -K failed; a denied socket destroy closes nothing" >&2
        exit 1
    fi
    printf '%s\n' "${closed}"
    printf '{"killed": %d}\n' "$(printf '%s' "${closed}" | grep -c . || true)"
    exit 0
fi

dev="$(ip route show default | awk '{for (i = 1; i < NF; i++) if ($i == "dev") { print $(i + 1); exit }}')"
if [ -z "${dev}" ]; then
    echo "netem: no default route in this namespace" >&2
    exit 1
fi

case "${1:-}" in
    set)
        shift
        [ "$#" -gt 0 ] || { echo "netem: set needs netem arguments" >&2; exit 2; }
        tc qdisc replace dev "${dev}" root netem "$@"
        ;;
    clear)
        # `tc qdisc del` fails with "No such file or directory" when only the default qdisc is
        # left, which is the state `clear` asks for.
        tc qdisc del dev "${dev}" root 2>/dev/null || true
        ;;
    show)
        ;;
    *)
        echo "usage: netem.sh set <netem args...> | clear | show | reset [port]" >&2
        exit 2
        ;;
esac
tc qdisc show dev "${dev}" root
