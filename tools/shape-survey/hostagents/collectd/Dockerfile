# Debian's own `collectd` package, in its DEFAULT `collectd.conf` as shipped, with exactly two
# plugins additively enabled through /etc/collectd/collectd.conf.d/ -- the distro's own
# collectd.conf already ends in an <Include "/etc/collectd/collectd.conf.d"> globbing "*.conf", so
# dropping a file there turns plugins on without editing a single line the package installed.
# Nothing else about the default plugin set changes: whatever Debian's collectd.conf enables by
# default (battery, cpu, df, disk, entropy, interface, irq, load, memory, processes, rrdtool,
# swap, syslog, users, as of the collectd version this built -- see provenance.txt for what a run
# actually saw and record the version there) stays exactly as shipped.
#
# The two plugins this drop-in turns on are the pointers this survey needs and nothing collectd
# could default on its own: `network` (collectd's own binary wire protocol, to logit's
# `collectd_in`) and `write_graphite` (carbon plaintext, to logit's `graphite_in`) -- see
# shape-survey.conf and tools/shape-survey/producers/hostagents.sh.
FROM debian:bookworm-slim

# No --no-install-recommends: the `processes` plugin (default-enabled) dlopen()s libmnl.so.0 at
# runtime, which is a Recommends rather than a Depends -- dropping recommends here would silently
# turn a plugin apt's own default install runs into a startup error, an answer this survey chose
# to change rather than found.
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y collectd \
    && rm -rf /var/lib/apt/lists/*

COPY shape-survey.conf /etc/collectd/collectd.conf.d/shape-survey.conf

CMD ["collectd", "-f"]
