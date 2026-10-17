# The one-shot fault image `script/soak` runs as
# `docker run --rm --network container:<id> --cap-add NET_ADMIN logit-soak-netem:local set|clear|show`.
# It joins the target container's network namespace, changes or prints the root qdisc on that
# namespace's default-route interface, and exits; the qdisc outlives it.
#
# A scenario with a [tls] table also runs it with `--entrypoint /certs.sh`, as the host user, to
# make the run's private CA, issue and rotate its certificates, and probe what a listener serves.

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends iproute2 openssl \
    && rm -rf /var/lib/apt/lists/*

COPY netem.sh /netem.sh
COPY certs.sh /certs.sh
ENTRYPOINT ["/netem.sh"]
