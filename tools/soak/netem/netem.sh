#!/bin/bash
# Usage: netem.sh set <netem args...> | clear | show
#
# Runs inside the target container's network namespace. A root qdisc on the default-route
# interface shapes that container's egress only.
#
#   set <args>  replace the root qdisc with `netem <args>`, then print it
#   clear       delete the root qdisc, then print what's left; clearing an absent one succeeds
#   show        print the root qdisc

set -euo pipefail

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
        echo "usage: netem.sh set <netem args...> | clear | show" >&2
        exit 2
        ;;
esac
tc qdisc show dev "${dev}" root
