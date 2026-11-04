#!/bin/bash
# Usage: tools/soak/vm-io-probe.sh
#
# Checks that `io.max` throttles a container's writes on this host before `slow-spool` runs
# here: a throwaway alpine container under a 1 MiB/s `wbps` limit writes 4 MiB with `fsync`,
# which takes about 4 s when the limit holds. PASS at 3 s or more, exit 0; FAIL otherwise, or
# when no block device backs the container's filesystem, exit 1. It finds the device as the
# soak `cgroup` action does (tools/soak/soaklib/faults.py, `io_device`): the mount holding `/`
# in the container's mountinfo, an overlay's upperdir through the host's mounts, and a
# partition's whole disk. It changes nothing on the host but the container, whose cgroup goes
# with it. DOCKER overrides the docker command (default `sudo docker`).

set -euo pipefail
DOCKER="${DOCKER:-sudo docker}"
IMAGE="alpine:3.20"
LIMIT=1048576
MIN_SECONDS=3

id=""
cleanup() { [ -n "${id}" ] && ${DOCKER} rm -f "${id}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# The major:minor and fields of the mount whose mount point is the longest whole-component
# prefix of $2 in mountinfo file $1: "<major:minor> <fstype> <super options>".
covering_mount() {
    awk -v path="$2" '
        {
            point = $5; gsub(/\\040/, " ", point)
            for (i = 7; $i != "-"; i++) {}
            if (point == "/" || path == point || index(path, point "/") == 1) {
                if (length(point) >= best) { best = length(point); hit = $3 " " $(i + 1) " " $(i + 3) }
            }
        }
        END { print hit }' "$1"
}

id="$(${DOCKER} run -d --rm "${IMAGE}" sleep 300)"
pid="$(${DOCKER} inspect -f '{{.State.Pid}}' "${id}")"
cgroup="$(sed -n 's|^0::/||p' "/proc/${pid}/cgroup")"
read -r dev fstype options <<<"$(covering_mount "/proc/${pid}/mountinfo" /)"
if [ "${fstype}" = overlay ]; then
    upper="$(tr ',' '\n' <<<"${options}" | sed -n 's/^upperdir=//p')"
    read -r dev fstype _ <<<"$(covering_mount /proc/1/mountinfo "${upper}")"
    echo "overlay upperdir ${upper} is on ${fstype} ${dev}"
fi
if [ "${dev%%:*}" = 0 ] || [ ! -e "/sys/dev/block/${dev}" ]; then
    echo "FAIL: no block device backs the container's root (${fstype} ${dev})"
    exit 1
fi
if [ -e "/sys/dev/block/${dev}/partition" ]; then
    dev="$(cat "/sys/dev/block/${dev}/../dev")"
fi
echo "cgroup ${cgroup}, device ${dev}"
echo "${dev} wbps=${LIMIT}" | sudo tee "/sys/fs/cgroup/${cgroup}/io.max" >/dev/null
echo "io.max: $(cat "/sys/fs/cgroup/${cgroup}/io.max")"

start="$(date +%s.%N)"
${DOCKER} exec "${id}" dd if=/dev/zero of=/f bs=1M count=4 conv=fsync 2>&1 | tail -1
end="$(date +%s.%N)"
elapsed="$(awk -v a="${start}" -v b="${end}" 'BEGIN { printf "%.2f", b - a }')"
echo "io.stat: $(cat "/sys/fs/cgroup/${cgroup}/io.stat")"
if awk -v e="${elapsed}" -v m="${MIN_SECONDS}" 'BEGIN { exit !(e >= m) }'; then
    echo "PASS: 4 MiB under ${LIMIT} B/s took ${elapsed}s"
else
    echo "FAIL: 4 MiB under ${LIMIT} B/s took ${elapsed}s, under ${MIN_SECONDS}s"
    exit 1
fi
