#!/bin/bash
# Usage: netem-drop.sh set <ip> | clear | show
#
# Runs inside a broker container's network namespace, in tools/soak/netem's image (iproute2):
#
#   docker run --rm --network container:<broker> --cap-add NET_ADMIN \
#       -v .../netem-drop.sh:/netem-drop.sh:ro --entrypoint bash logit-soak-netem:local \
#       /netem-drop.sh set 172.20.0.7
#
# `set` drops every packet the broker sends to <ip>, and nothing else: a root `prio` qdisc whose
# priomap sends all traffic to band 1, a `netem loss 100%` on band 4, and a u32 filter steering
# packets for <ip> into band 4. A root netem alone, as tools/soak/netem/netem.sh sets, would cut
# the broker off from every other leg and from the read-back. The broker still receives what
# <ip> sends, so a produce request is appended while its response is lost.
#
#   set <ip>  replace the root qdisc as above, then print the qdiscs and filters
#   clear     delete the root qdisc; clearing an absent one succeeds
#   show      print the qdiscs and filters

set -euo pipefail

dev="$(ip route show default | awk '{for (i = 1; i < NF; i++) if ($i == "dev") { print $(i + 1); exit }}')"
if [ -z "${dev}" ]; then
    echo "netem-drop: no default route in this namespace" >&2
    exit 1
fi

case "${1:-}" in
    set)
        ip="${2:?netem-drop: set needs the destination address}"
        tc qdisc del dev "${dev}" root 2>/dev/null || true
        tc qdisc add dev "${dev}" root handle 1: prio bands 4 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
        tc qdisc add dev "${dev}" parent 1:4 handle 40: netem loss 100% limit 100000
        tc filter add dev "${dev}" parent 1: protocol ip prio 1 u32 match ip dst "${ip}/32" flowid 1:4
        ;;
    clear)
        # `tc qdisc del` fails with "No such file or directory" when only the default qdisc is
        # left, which is the state `clear` asks for.
        tc qdisc del dev "${dev}" root 2>/dev/null || true
        ;;
    show)
        ;;
    *)
        echo "usage: netem-drop.sh set <ip> | clear | show" >&2
        exit 2
        ;;
esac
tc qdisc show dev "${dev}"
tc filter show dev "${dev}" parent 1: 2>/dev/null || true
