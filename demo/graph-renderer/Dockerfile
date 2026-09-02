# Graphviz in a container, for one job: turning `logit graph`'s DOT into the SVG the demo's
# hello-world page shows (demo/compose.yaml's `graph-dot` -> `graph-svg` -> `hello` chain).
#
# A tiny purpose-built image rather than a stock one -- no official image ships `dot`. Same
# Debian version the rest of the demo's Debian-based services use.
FROM docker.io/library/debian:13-slim

# `graphviz` is the command-line tools package; it provides /usr/bin/dot. `fonts-liberation` is
# only a Recommends, so --no-install-recommends alone would leave the image with no fonts at
# all -- `-Tsvg` still emits valid SVG (text stays as <text> elements, nothing is rasterized),
# but graphviz measures labels to size node boxes, and with no font installed it falls back to
# estimated metrics: a "no hard-coded metrics for 'monospace'" warning, and labels that can
# overflow their boxes. crates/logit-cli/src/dot.rs sets `node [fontname="monospace"]`, so pull
# the font in explicitly rather than rely on the fallback.
RUN apt-get update && apt-get install -y --no-install-recommends \
        graphviz \
        fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/bin/sh", "-c"]
