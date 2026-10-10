# The one-shot fault image `script/soak` runs as
# `docker run --rm --network container:<id> --cap-add NET_ADMIN logit-soak-netem:local set|clear|show`.
# It joins the target container's network namespace, changes or prints the root qdisc on that
# namespace's default-route interface, and exits; the qdisc outlives it.

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY netem.sh /netem.sh
ENTRYPOINT ["/netem.sh"]
