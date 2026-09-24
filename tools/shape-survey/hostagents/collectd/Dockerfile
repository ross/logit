# Debian's `collectd` package with its default `collectd.conf` as shipped. That file already
# includes /etc/collectd/collectd.conf.d/*.conf, so shape-survey.conf there adds two plugins
# without editing anything the package installed: `network` (to `collectd_in`) and
# `write_graphite` (to `graphite_in`), the destinations collectd can't default. The default plugin
# set is untouched; provenance.txt records the package version a run saw.
FROM debian:bookworm-slim

# No --no-install-recommends: the default-enabled `processes` plugin dlopen()s libmnl.so.0, a
# Recommends rather than a Depends, so dropping recommends turns it into a startup error.
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y collectd \
    && rm -rf /var/lib/apt/lists/*

COPY shape-survey.conf /etc/collectd/collectd.conf.d/shape-survey.conf

CMD ["collectd", "-f"]
