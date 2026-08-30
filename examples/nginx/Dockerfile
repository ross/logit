# Reference nginx image for docs/plans/0002-nginx-integration.md's workstream F -- a
# self-contained proof of syslog_in -> json -> kv_metrics -> keep -> aggregate -> influxdb_out
# against a real nginx, inside this repo's own dev stack. Not a deployment guide: see
# docs/deploying.md for the real-world recipe (a read-only bind mount over a config directory,
# not a build-time COPY).
FROM nginx:1

# nginx-module-njs: the optional njs module, used by the internal upstream block (nginx.conf) to
# add a randomized response delay so nginx.request_time/nginx.upstream_response_time show a real
# distribution rather than near-uniform sub-millisecond values.
RUN apt-get update && apt-get install -y --no-install-recommends nginx-module-njs \
    && rm -rf /var/lib/apt/lists/*

COPY nginx.conf /etc/nginx/nginx.conf
COPY delay.js /etc/nginx/njs/delay.js
COPY html/ /usr/share/nginx/html/
