# Data shapes: the desk-survey rows

The appendix to [`data-shapes.md`](data-shapes.md): every desk-survey row that document draws on,
grouped by signal, with its citation. Nothing here was measured by `logit` — the measured rows are
in `data-shapes.md` §5. These were counted from pinned sources, specifications and documentation on
2026-09-20 across eight research tracks, and then roughly a third of them (106 rows) were
independently re-derived by a second pass that was told not to trust the first.

Reading a row:

- **Fid.** is fidelity — *Counted* (from a pinned commit, release or spec), *Reported* (somebody
  else's published figure), *Estimated* (our inference; the row says why). **Repr.** is
  representativeness — *Demo*, library/vendor *Default*, typical operator-*Configured*, or
  *Production*-derived. `data-shapes.md` §1 has the full definitions.
- **✓** means the verification pass independently re-derived the row and confirmed it; **✓c** means
  it found an error and the value shown is the **corrected** one (the first table below lists every
  correction, so the audit trail stays visible); blank means the row was not sampled.
- Triples are `min · typical · max` for a library under default configuration, or OpenTelemetry's
  `required · +conditionally required · +recommended` for a specification row. A static count is a
  range, not a distribution.
- The **id** column (`T5a#12`, `T2a-py#1`, "merge" for combined near-duplicates) points into the
  working notes the rows were consolidated from, which are kept outside the repository. The
  **citation** column is the checkable reference.

## Corrections applied

| # | Row | Was | Corrected to |
|---|---|---|---|
| 1 | T3#1 | Datadog max 3,638 metrics = `envoy` | = **`clickhouse`** (envoy is 1,042); the distribution itself reproduces exactly |
| 2 | T3#10 | Datadog k8s pod tags ~44 (orchestrator 4) | **43** (38 pod-level: High 2 / Orchestrator **3** / Low 33, + 5 host-level) |
| 3 | T3#23 | dd-trace-py writer buffer/payload 8 MiB (docs) | **20 MiB** at pinned SHA `d6ab482`; the 8 MiB is stale released-docs drift |
| 4 | T3#7 | `kafka_actions` has no `metadata.csv` | It **has** one (header only, 0 data rows); effect (0 metrics) unchanged |
| 5 | T2a-py#1 | wsgi server span max 13 | **14** (misses `user_agent.synthetic.type`, set unconditionally on UA match) |
| 6 | T2a-js#1 | JS http server span max 15 | **14** under Default — the 15th needs `enableSyntheticSourceDetection`, default `false` |
| 7 | T4#4 | structlog docs stdlib+JSON recipe 6 keys | **7** (adds `filename`; the recipe's callsite set includes FILENAME) |
| 8 | T4#27 | semantic_logger 9 always-present JSON keys | **7** always; `file`/`line` are backtrace-gated (default `:error`), same Error+-only pattern as zap's `stacktrace`. `host`/`application` are de-facto always-on, `environment` is not |
| 9 | T5a#9 | blackbox_exporter HTTP prober 11 families | **8 always-registered → 15 max**; a typical HTTPS probe exposes **13–15** |
| 10 | T2b#6 | Java agent bare-host resource 11 · 12 · 14 | **12 · 13 · 15** (`emitCommandAttributes()` double-negates two `false` flags → command attrs **are** captured by default) |
| 11 | T2b#9 | JVM experimental instruments "≥6" | experimental total is **8** (adds `SystemCpu`=2); bound was true, list incomplete |
| 12 | T5b#D7 | `resourcedetection` gcp: 16 default of 18 | **17 of 19** (the table has 19 rows; only the 2 named gce ones are off) |
| 13 | T5b#D2 | k8sattributes fully configured "25 fixed names" | **30** (6 + 19 + 5 *new* container-level), plus unbounded label/annotation extraction |
| 14 | T5b#A10 | Caddy `request{}` nests ~8 subfields | **9**, including a whole extra nested `tls{5}` the row omitted; depth-3 claim stands |
| 15 | T6#27 | Prometheus PR #16069: ~0 at 5 labels | **−19.94%** at 5 and −39.48% at 30 on `middle_label/get`; "~0" is the `first_label/get` bench |
| 16 | T6#38 | Zhu et al. Table III: Proxifier 98 templates | **9** (the adjacent 2k-sample column's 8 looks concatenated); other 15 systems exact |
| 17 | T6#23 | otel-arrow metrics compression ×2.17–2.45 | **×2.19–2.45** reading the OTel-Arrow ZSTD+Stream column consistently; 2.17 is the OTLP-Dict column |
| 18 | T1#38 | DogStatsD "1432 B for lower-MTU/tunneled paths" | **dropped** — absent from the cited page; 1472 B UDP / 8192 B UDS / 8 KB buffer stand |
| 19 | T2b#36, T2b#31 | Rust `tracing` hard 32-field-per-callsite cap | **No cap since 2023** (tokio-rs/tracing#2508, const generics). `ValueSet` is a plain slice; only a stale doc line survives. T6#31 is right; T2b#31's "max 32 user fields" becomes unbounded |
| 20 | T1#1 vs T6#33 | HTTP server span 27 vs 29 | **Both right, different pins.** 3 required · +7 conditionally required · +6 recommended at both; opt-in **11 @ semconv `e10a930` (v1.44.0, 2026-08-04) → 27**, **13 @ `d0472f4a` (2026-09-16) → 29** (`http.request.body.content`/`http.response.body.content` added between) |

---

## §A — Logs

| Source | Dimension | Value / range | Fid. | Repr. | ✓ | Citation | id |
|---|---|---|---|---|---|---|---|
| **Library / framework records** | | | | | | | |
| structlog 26.1.0 | fields, default vs docs' JSON recipe | **0 structured** by default (human console text, not JSON) → **7** flat with the docs' stdlib+JSON recipe, + kwargs | Counted | Default / Configured | ✓ / ✓c | pypi 26.1.0 (ran it); `docs/standard-library.md:190-219` | T4#3-4 |
| loguru 0.7.3 `serialize=True` | fields, nesting, depth | 2 top (`text`,`record`); `record` = **13 keys of which 6 are nested objects** (2-3 sub-fields each); depth 3 | Counted | Default | ✓ | pypi 0.7.3, ran it | T4#6 |
| Go `log/slog` / zap / zerolog / logrus | flat fields, default JSON line | slog **3** (+1 nested `source{3}`, depth 2, with `AddSource`) · zap **4** (`stacktrace` Error+ only) · zerolog **3** · logrus **3** (+2 with `ReportCaller`) | Counted | Default/Configured | ✓ (zap) | golang/go@9834516 `slog/handler.go`; zap@v1.28.0; zerolog@v1.35.1; logrus@v1.10.2 | T4#11-14 merge |
| JS pino | fields, default line | **5** flat (`level,time,msg,pid,hostname`) | Counted | Default | ✓ | pino@v10.3.1 `README.md:69` | T4#15 |
| JS pino-http completion record | width, nesting | **9 top-level** (pino's 5 + `reqId,req{},res{},responseTime`); `req{}` = 7 flat + a nested `headers{}` of ~10-15 | Counted | Configured | ✓ | pino-http@v11.0.0; pino-std-serializers@v7.1.0 `lib/req.js` | T4#16,18 merge |
| JS winston / bunyan | flat fields | winston bare **2** (`level`,`message`) — **no timestamp by default**; bunyan **7** always; `src:true` adds a nested `src{3}` | Counted | Default/Configured | | winston@v3.19.0 README:158-164; bunyan@2.0.5 README:141,522 | T4#19,21-22 merge |
| logstash-logback-encoder | fields; MDC handling | **7** flat always; **MDC flattened onto the root, unprefixed** | Counted | Default | | @9.0 `LogstashFormatter.java:112-131` | T4#29 |
| log4j2 JsonTemplateLayout, 3 shipped templates | fields; key style | `EcsLayout` **11** top-level, **dotted-namespace literal keys** · `LogstashJsonEventLayoutV1` **13** with genuine nested `exception{3}`/`mdc{}` · `GelfLayout` **7** base + conditional `full_message`, **MDC flattened with a `_` prefix** | Counted | Default | ✓ (Ecs, Gelf) | logging-log4j2@rel/2.26.1 `log4j-layout-template-json/**` | T4#30-32 merge |
| Serilog CLEF | fields | **2** always (`@t`,`@mt`); `@l` **omitted at Information**; every structured property flattened to top level, no wrapper | Counted | Default | | serilog-formatting-compact@v3.0.0 | T4#33 |
| MS `JsonConsoleFormatter` | fields; nesting | **4** flat (`EventId,LogLevel,Category,Message`), **Timestamp omitted by default**; message templates add a nested `State{}` (depth 2); `IncludeScopes` adds a `Scopes[]` of objects (**depth 3**) | Counted | Default / Configured | ✓ | dotnet/runtime@v9.0.20 `JsonConsoleFormatter.cs:64-91,155-178` | T4#34-35 merge |
| Rust `tracing-subscriber` `fmt().json()` | fields; nesting | **4** flat + a nested `fields{}` holding `message`, depth 2; **inside any span** +`span{}` +`spans[]` → depth 3 | Counted | Default/Configured | | tracing@tracing-subscriber-0.3.23 `format/json.rs:146-304` | T4#36-37 merge |
| Rust `env_logger` / PHP monolog | fields | env_logger **4** tokens `[ts LEVEL target] message` · monolog `JsonFormatter` **7** flat, `context`/`extra` nested but **empty by default**; `WebProcessor` fills `extra` with 5 | Counted | Default/Configured | ✓ (env_logger) | env_logger@v0.11.6 `fmt/mod.rs:262-274`; monolog@3.10.0 | T4#38-40 merge |
| semantic_logger / rails_semantic_logger | fields; nesting | **7** always + `host`,`application` de-facto → ~12 typical; nested `payload{}`/`exception{3 + recursive cause}`. `process_action` = **4 top-level**, of which `payload{}` carries ~9 always + 2-4 conditional | Counted | Default | ✓c / ✓ | semantic_logger@v5.1.0 `formatters/raw.rb`; rails_semantic_logger@v5.2.0 | T4#27-28 merge |
| lograge / Rails default production log | flat fields; tokens | lograge **8 · 10 · 12**, flat, no nesting; stock Rails **~10 · ~13 · ~16** tokens across its 3 unstructured lines | Counted | Default | | lograge@v0.15.0 `log_subscribers/*.rb`; rails@v8.1.3.1 | T4#24-25 merge |
| **Access-log formats** | | | | | | | |
| nginx | fields | predefined `combined` **8** `$variables`; 3 widely-copied community JSON `log_format`s: **11 · 12 · 28** | Counted | Default / Configured | | nginx.org ngx_http_log_module; logdy.dev; blog.tyk.nu | T5b#A1-2 merge |
| ingress-nginx | fields in its own default `log-format-upstream` | **17** `$variables` | Counted | Default | ✓ | kubernetes.github.io/ingress-nginx log-format | T5b#A3 |
| Apache `mod_log_config` | fields | `common` (CLF) **7** · `combined` **9** | Counted | Default | ✓ | httpd.apache.org/docs/2.4/logs.html | T5b#A4-5 |
| HAProxy | field slots (atomic values) | `httplog` **16 (~28)** · `tcplog` **10 (~19)** | Counted | Default | | haproxy@main `doc/configuration.txt` §8.2.2-3 | T5b#A6-7 |
| Envoy | default access-log operators | **15** `%COMMAND%` | Counted | Default | ✓ | envoyproxy.io access_log/usage | T5b#A8 |
| Caddy `http.log.access` | top-level; nesting; depth | **11** top-level; `request{}` nests **9** subfields incl. `headers{}` and `tls{5}`; depth 3 | Reported | Default | ✓c | caddyserver.com/docs/logging | T5b#A10 |
| gunicorn / uvicorn / JS morgan | tokens | gunicorn **9** · uvicorn **5** · morgan `combined` 10 / `common`,`short` 8 / `tiny` 5 / `dev` 4 | Counted | Default | ✓ (gunicorn) | gunicorn@afc7d2f `config.py:1518`; uvicorn@master; morgan@1.12.1 | T4#9-10,23 merge |
| AWS ALB / Amazon CloudFront | positional fields | ALB **34** · CloudFront **33** | Counted | Default | ✓ (ALB) | docs.aws.amazon.com load-balancer-access-logs; standard-logging-legacy-s3 | T5b#A11-12 merge |
| Cloudflare Logpush `http_requests` | available vs default | **174 available; no default at all** — every job enumerates fields (tutorial starter set: 9) | Reported | Default/Configured | | developers.cloudflare.com log-fields/zone/http_requests | T5b#A13 |
| **System and service logs** | | | | | | | |
| systemd-journald, one developer workstation | fields per entry; key length; vocabulary | min 16 · **median 27** · p90 33 · max 40 (trusted `_`-fields median **23**; user fields median **4**); key length min 3 / median 11 / p90 20 / max 27 B — against a documented vocabulary of **72** names (18 user · 28 trusted · 5 kernel · 16 coredump · 5 address) | Counted | **Production** (one host, aggregates only) / Default (spec) | | `journalctl -o json -n 200`, 2026-09-20; `man systemd.journal-fields` | T5b#C1-2 merge |
| RFC 5424 | header fields | **7** HEADER fields + 0..N SD-ELEMENTs + MSG | Counted | Default (spec) | | rfc-editor.org/rfc/rfc5424 §6 | T5b#C3 |
| PostgreSQL | columns / keys | `csvlog` **26** columns · `jsonlog` **29** keys | Counted | Configured (both opt-in) | ✓ | postgresql.org runtime-config-logging §19.8.4-5 | T5b#C4 |
| Docker `json-file` / K8s CRI | fields per line | **3** (`log,stream,time`, JSON) / **4** (ts, stream, P\|F tag, message; plain text) | Counted | Default | | docs.docker.com json-file; kubelet-cri-logging.md | T5b#C5 |
| **Cloud audit / flow logs, and log schemas** | | | | | | | |
| AWS CloudTrail | top-level fields; nesting; depth | **31** top-level (~10 near-always-present); `userIdentity` 13 sub-fields; `sessionContext.sessionIssuer{5}`/`webIdFederationData{2}` → **depth 5** | Counted | Default (schema) | ✓ (31 exact; nesting spot-checked) | docs.aws.amazon.com cloudtrail-event-reference-* | T5b#B1 |
| AWS VPC Flow Logs | default vs available | **14 default (v2) of 54** | Counted | Default/Configured | ✓ | docs.aws.amazon.com flow-log-records | T5b#B2 |
| Kubernetes `audit.k8s.io/v1` | top-level fields | **19**; `requestObject`/`responseObject` gated by audit-policy `level` | Counted | Default (schema) / Configured | | kubernetes.io apiserver-audit.v1 | T5b#B3 |
| ECS v9.5.0 | catalog size; depth; key length | **2,614** fields (347 core / 2,267 extended) across 47 fieldsets (`threat` 437, `process` 391); dotted-name depth median 4 / max 7; full name median 31 B / max 66; leaf segment median 6 B / max 30 | Counted | Default | | elastic/ecs@401807e `ecs_flat.yml` | T1#11-14 merge |
| ECS v9.5.0 | its own worked Apache-access example | **30 distinct fields across 9 fieldsets** | Counted | Configured | | same, `ecs-getting-started.md` | T1#15 |
| OCSF v1.10.0-dev | resolved attrs by requirement level | Authentication 8·+21·+21 = **50** · HTTP Activity 7·+16·+24 = **47** · Process Activity 10·+15·+14 = **39** | Counted | Default | | ocsf-schema@dd542bd1, `extends` chain resolved | T1#19-21 merge |
| Splunk CIM | fields per data model | Web **38** · Authentication **37** | Counted | Default | ✓ (Auth) | docs.splunk.com CIM/latest/User/{Web,Authentication} | T1#22-23 merge |
| Datadog | standard-attribute catalog; reserved log attrs | 231 rows / **208 unique names** / 39 domains; **6** reserved log attributes named on the naming page, **7** in the standard-attributes "Reserved" domain (adds `device`) | Counted / Reported | Default | ✓ (reserved) | docs.datadoghq.com/standard-attributes/; attributes_naming_convention | T1#16,18 merge |

---

## §B — Metrics

One metric = one event for `otlp_in`/`prometheus_in`/`statsd_in`/`graphite_in`, so "labels per
series" below **is** per-event attribute width.

| Source | Dimension | Value / range | Fid. | Repr. | ✓ | Citation | id |
|---|---|---|---|---|---|---|---|
| **Labels / tags per series or data point** | | | | | | | |
| node_exporter (one Linux host) | labels per series | **min 0 · median 2 · p90 4 · max 19** (n=3,034) | Counted | Default | ✓ | node_exporter@17ddd77 `collector/fixtures/e2e-output.txt` | T5a#2 |
| node_exporter | families / series / type mix; series per family | **1,227 families** by declared `# TYPE` (**1,180** counting only families that emit), 3,034 series; counter 475 / gauge 417 / untyped 334 / summary 1 / **histogram 0**. Series per family min 1 · median 1 · p90 4 · **max 112** (`node_interrupts_total`, one per IRQ line) | Counted | Default | ✓ | same fixture | T5a#1,3 merge |
| cAdvisor (one container) | labels per series; series per family | labels min 0 · **median 7** · p90 8 · max 10; series/family max 99 | Counted | Default | | cadvisor@5bf5d43 `lib/metrics/testdata/*` | T5a#4-5 merge |
| cAdvisor | container label-set composition | 3 fixed (`id,name,image`) + **N dynamic** (one per Docker/OCI label, one per env var) + 0-3 metric-specific | Counted | Default | ✓ | cadvisor@5bf5d43 `lib/metrics/prometheus.go` | T5a#18 |
| Spring Boot Micrometer, **one real pod scrape** | families/series; labels; value length | 63 families / 109 series; labels min 0 · **median 1** · p90 2 · max 4; label-value length p90 21 B / max 32 B | Counted | **Production** (third-party sample) | | mosip/k8s-infra@83e3b10 `pod-jvm-scraped-metrics-sample.txt` | T5a#13 |
| kube-state-metrics | labels per metric | pod family (n=60) min 3 · **median 4** · max 10; container 4·4·8; deployment 2·2·5; node 1·2·10 | Counted | Default | | KSM@ca59026 `docs/metrics/**` | T5a#14-17 merge |
| kube-apiserver / kubelet | label width; histogram fan-out | `apiserver_request_total` carries **9** labels; `kubelet_image_pull_duration_seconds` is **1 family = 126 series** (6 bucket-label values × (19 `le` + sum + count)) | Counted | Default | | k8s@400031d `metrics_test.go:401`; `kubelet/metrics/testdata/` | T5a#11-12 merge |
| nginx-exporter / blackbox_exporter | families per target | nginx: 8 core `stub_status` families, **all 0-label**, + `build_info` (7 labels). blackbox HTTP prober: **8 always-registered → 15 max**; typical HTTPS probe **13-15** | Reported / Counted | Default | ✓ / ✓c | nginx-prom-exporter@9956da7 README:197-435; blackbox@feaab45 `prober/http.go` | T5a#9 |
| Prometheus | labels added at scrape time | `job`,`instance` always, before relabeling; a typical `kubernetes_sd` job adds `namespace`,`pod` (or `service`) **plus one label per pod/service label** via `labelmap` | Reported / Counted | Default / Configured | | prometheus.io jobs_instances; prometheus@main `examples/prometheus-kubernetes.yml` | T5a#20-21 merge |
| Datadog integrations-core (n=273) | metrics per integration | min 0 · **median 45** · p90 323 · **max 3,638 (`clickhouse`)**; envoy 1,042, vault 679 | Counted | Default | ✓c | integrations-core@99606e5 `*/metadata.csv` | T3#1 |
| Datadog integrations-core (n=37,191) | metric-name length; type mix | length min 9 · **median 38** · p90 56 · max 100 B. Types: gauge 63.3% · count 35.8% · rate 0.9% — **no histogram type exists** (one histogram → ~5-8 sibling rows) | Counted | Default | ✓ | same, `metric_name`/`metric_type` cols | T3#2-3 merge |
| Datadog checks (nginx, mysql, mongo, +) | tags per metric | **1-3 built-in identity tags always** + 0..N operator `tags:` + 0-3 conditional metric-specific (replica role, cluster, per-schema/table/vhost) | Counted (3 read in full) · Estimated (rest) | Default | ✓ | integrations-core@99606e5 `nginx.py`, `mysql.py`, `mongo.py` | T3#8-9 merge |
| OTel semconv v1.44.0 | attrs per metric data point | `http.server.request.duration` 2·+4·+1 (7) +3 opt-in → 10 · http client → 9 · `db.client.operation.duration` 1·+6·+5 (12) +1 → 13 · rpc server/client → 6 | Counted | Default (spec) | | semconv@e10a930 `model/{http,db,rpc}/metrics.yaml` | T1#7 |
| Go otelhttp / otelgrpc / runtime | attrs per data point | http server 3 + up to 5 (max 8) · http client max 7 · grpc max 6 · **Go runtime: 8 instruments, 7 of them zero-attribute** | Counted | Default | | go-contrib@c4c6248 `internal/semconv/*`, `runtime.go` | T2b#13,15,17 merge |
| Java JMX runtime-telemetry | instruments; attrs per point | **12** default instruments (+8 experimental); attrs/point **max 2** (`jvm.memory.*`), 1 (`thread.daemon`), 0 on class/cpu | Counted | Default | ✓ | otel-java-instr@8ad06a0 `JmxRuntimeMetricsFactory.java` | T2b#9-10 merge |
| JS otel instrumentation | attrs per data point | `http.server.request.duration` 2·4·6 · `http.client.request.duration` 3·5·6 · pg `db.client.operation.duration` 4·5·6 | Counted | Default | ✓ | js-contrib@590f154 `http.ts`, `utils.ts`, pg `instrumentation.ts` | T2a-js#5-6,11 merge |
| **Series per target; how it scales; key/value lengths** | | | | | | | |
| KSM / cAdvisor; Robust Perception | scaling; series per application | Series scale with **object count**: KSM ~5-15 series/pod × pods, cAdvisor ~100-300 series/container × containers — no row above is a cluster total. Practitioner guidance: ~100 series (simple app) · ~1,000 (complex) · **10,000 = "an indication you may have a cardinality issue"** | Estimated / Reported (explicitly personal experience) | — / Configured | | inference from T5a#4-5,14-17; robustperception.io/how-many-metrics-should-an-application-return/ | T5a#40, T6#42 merge |
| OTel semconv registry (all signals, n=932) | attribute **key** length; value-type mix | min 5 · **median 20** · p90 32 · max 50 B; string 58% · enum 18% · int 10% · rest template/array/bool/double | Counted | Default | | semconv@e10a930 `model/*/registry.yaml` | T1#10 |
| **Values per emission** | | | | | | | |
| collectd `types.db` | data sources per type, full histogram | 391 types: **1 ds → 349 · 2 ds → 35 · 3 ds → 1 · 4 ds → 5 · 5 ds → 1**. Nothing wider than 5; **42 of 391 (10.7%) are multi-value**, mostly rx/tx or read/write pairs | Counted | Default | ✓ | collectd@10d8891 `src/types.db` | T5a#31-33 merge |
| collectd plugins | values per read cycle | `load` = one 3-ds type; `interface` = 3 types × 2 ds per iface; but `df`/`memory`/`cpu` **repeat a 1-ds type** per mount / per state / per (cpu,state) instead of widening | Counted+Reported | Default | | collectd@10d8891 `src/{load,interface,df,memory,cpu}.c` | T5a#34-35 merge |
| Telegraf, 8 classic system inputs | tags / fields per point | tags 0-5 (**median 0**); fields 5-34 base (**median 9**), up to 21/35/40 with optional sub-features | Counted | Default | ✓ | telegraf@ae4a0da `plugins/inputs/*/README.md` | T5a#23-24 merge |
| Telegraf `redis` | fields on one point | **64 fields**, 3 tags on the main measurement — the widest single point in the survey; 5 sibling measurements 1-5 fields each | Counted | Default | ✓ | telegraf@ae4a0da `plugins/inputs/redis/README.md` | T5a#27 |
| Telegraf `docker` / `kubernetes` | fan-out instead of width | docker: **13 measurement names**, 2-34 fields / 3-9 tags each; kubernetes: 4 measurements (18f/1t, 13f/4t, 3f/4t, 4f/3t) | Counted | Default | ✓ (k8s) | same repo, those READMEs | T5a#28-29 merge |
| **statsd-family client batching and datagram defaults** | | | | | | | |
| pystatsd / statsd-ruby | batching defaults | pystatsd `maxudpsize` **512 B**; statsd-ruby `batch_size` **10** (count-based), no byte budget, no auto-flush | Reported | Default | | pystatsd@08d0456 `docs/reference.rst`; statsd-ruby@ab57784 `lib/statsd.rb:273-303` | T5a#36-37 merge |
| node-statsd / hot-shots | batching defaults | node-statsd: **no batching at all**, one datagram per call. hot-shots: `maxBufferSize` **0** (udp/tcp) / 8192 (uds); `bufferFlushInterval` 1000 ms | Counted / Reported | Default | ✓ | node-statsd@f9fac42 `lib/statsd.js`; hot-shots@8645c9c README | T5a#38 |
| Etsy/community statsd server | packet guidance; flush | MTU tiers **512 / 1432 / 8932 B** (advisory prose, not enforced); server `flushInterval` **10000 ms** | Reported+Counted | Default | | statsd/statsd@f7157b8 `docs/metric_types.md:110-128`, `stats.js:452` | T5a#39 |
| DogStatsD (docs + datadogpy) | datagram sizes | docs: UDP **≤1472 B**, UDS **8192 B** = the Agent's `dogstatsd_buffer_size` default. Client constants: `UDP_OPTIMAL_PAYLOAD_LENGTH` **1432 B**, UDS 8192 B, min send buffer 32 KiB | Counted | Default | ✓c | docs.datadoghq.com dogstatsd/high_throughput; datadogpy@fac2d5e `dogstatsd/base.py` | T1#38, T3#14 merge |
| DogStatsD wire | out-of-band identity | container `\|c:`, external data `\|e:`, cardinality `\|card:` **packet suffixes** + `dd.internal.entity_id`/`env`/`service`/`version` auto-injected — a raw payload **undercounts** a metric's eventual tag count | Counted | Default | ✓ | datadogpy@fac2d5e same file | T3#15 |

---

## §C — Traces

Spec rows use OTel's triple `required · +conditionally required · +recommended` with opt-in stated
separately; library rows are min · typical · max under **default** config.

| Source | Span / dimension | Value / range | Fid. | Repr. | ✓ | Citation | id |
|---|---|---|---|---|---|---|---|
| **Attributes per span, by instrumentation family** | | | | | | | |
| OTel semconv | HTTP **server** | **3 · +7 · +6 = 16** on-path, + opt-in **11 @ `e10a930` (2026-08-04) → 27** or **13 @ `d0472f4a` (2026-09-16) → 29** | Counted | Default (spec) | ✓ (both pins) | semconv `docs/http/http-spans.md` at each SHA | T1#1, T6#33 |
| OTel semconv v1.44.0 | HTTP client; DB client | client 4·+4·+4 = 12 (+11 opt-in → 23); DB generic 1·+6·+7 = 14 (+2 → 16), technology overlays **8 (MongoDB) to 20 (Cassandra)** | Counted | Default (spec) | | semconv@e10a930 `model/{http,db}/spans.yaml` | T1#2-3 merge |
| OTel semconv v1.44.0 | RPC; gen_ai; messaging | RPC 1·+6·+2 = **9**, symmetric, no opt-in (**smallest**) · gen_ai inference 2·+8·+17 = 27 (+4 → **31**, largest in the registry) · Kafka send 8 / process 9 | Counted | Default (spec) | | semconv@e10a930 `model/{rpc,gen-ai,messaging}/*.yaml` | T1#4-6 merge |
| Java otel-instrumentation | HTTP server / client; DB | server 2 required + 14 conditional default-on = **16 max with zero extra config**; client 12; DB old (default) semconv 12-13. Opt-in adds 5 fixed + **N allow-listed headers** | Counted | Default / Configured | ✓ (server) | otel-java-instr@8ad06a0 `Http*AttributesExtractor.java`, `DbClientAttributesExtractor.java` | T2b#1-5 merge |
| Go otel-contrib | HTTP / gRPC / SQL | otelhttp server 3 · +13 → **16 max**, client 3 · +6 → 9; otelgrpc 3 · +1-2 → 5; XSAM/otelsql **0 automatic** + 1 default-on (`db.query.text`) | Counted | Default | ✓ (http server) | go-contrib@c4c6248; XSAM/otelsql@5c3d0aa | T2b#11-12,14,16 merge |
| .NET otel-contrib | AspNetCore / HttpClient / SqlClient | AspNetCore 5 required · +5 → **10 typical** (+7 opt-in); HttpClient .NET ≤8 6+1 but **.NET 9+ 0 base** (BCL emits them); SqlClient 2 · 6 typical | Counted | Default | | dotnet-contrib@3ea290c those listeners | T2b#22,24-25 merge |
| Rust `tracing-opentelemetry` / tower-http | non-user span attrs | **6 default-on** before any user field (code location ×3, thread ×2, target) · +2 on close · +1 opt-in. User fields 0 · 2-5 · **unbounded** (no 32-cap since 2023). tower-http's own span: 3 (+1 opt-in) | Counted / Estimated (user) | Default | ✓ / ✓c | tracing-opentelemetry@1d5422f `layer.rs`; tower-http@c941451 | T2b#30-31,35 merge |
| Python otel-contrib | wsgi / asgi / frameworks | wsgi **7 · 10 · 14** (old semconv, Python's default; `http/dup` ~13·~19·~24); asgi 6·9·12; django/flask/fastapi add only **0·1·1** (`http.route`) | Counted | Default / Configured | ✓c | py-contrib@4c93bd9 `wsgi/`, `asgi/`, frameworks | T2a-py#1-2,4,5,7,8 merge |
| Python otel-contrib | celery / dbapi / redis / requests | celery task-run **2 · 8 · 14** (19-entry conditional list); dbapi 3·6·7 with `db.statement` = **raw unsanitized SQL by default**; redis 6·7·9 (sanitized, 1000-char cap); requests 3·4·6 | Counted | Default | ✓ | py-contrib@4c93bd9 those packages | T2a-py#9,12-13,16 merge |
| JS otel-contrib | http / express / pg / ioredis / nestjs | http server **7 · 11 · 14** (default config), client 5·9·11, + unbounded hook surface; express layer 2·3·3; pg 4·5·7; ioredis 5 fixed; nestjs 4/8/4 over 2 spans | Counted | Default | ✓c (http) | js-contrib@590f154 `utils.ts:776-863` + those packages | T2a-js#1-2,7,10,12,14 merge |
| Ruby otel-contrib | rack / net_http / sidekiq / pg | rack **5 · 7 · 11+**; net_http 6·7·8; sidekiq server 6·6·7 and client 5 (+1 opt-in); pg 6·9·13 | Counted | Default | ✓ (sidekiq client) | ruby-contrib@141076e those gems | T2a-ruby#1,4,6,8,9 merge |
| Ruby otel-contrib | Rails internals | `action_pack` **adds 2·3·4 to the Rack span** and opens none of its own; `active_record` spans carry **0 attributes on 18 methods**, 1 on `transaction`; `action_view` forwards the whole notification payload | Counted | Default | ✓ | ruby-contrib@141076e `action_controller.rb`, `persistence.rb` | T2a-ruby#12,14-15 merge |
| dd-trace-py / dd-trace-java | vendor HTTP & DB spans | shared `set_http_meta` 4 · 8-9 · 13+N, Django combined ~8 · ~14-16 · **~25+**; Java `HttpServerDecorator` 3 · 8-9 · **15+** (5 `X-Forwarded-*` variants each a tag); psycopg 3·9·10; JDBC 3·5·6. 11 span fields sit outside `meta`/`metrics` | Counted / Estimated (Django combined) | Default | ✓ (java http, psycopg) | dd-trace-py@d6ab482; dd-trace-java@7b903a5 | T3#16,18-22 merge |
| **Span events, links, and spans per request** | | | | | | | |
| OTel semconv | exception event attrs | 0 · +2 · +2 = **4**, no opt-in | Counted | Default (spec) | | semconv@e10a930 `model/exceptions/events.yaml` | T1#8 |
| JS / Python / Ruby instrumentation | span events per span | JS http: **1 exception event, only on socket-level error — never on 4xx/5xx**; django 1 per unhandled view exception; express 1 per erroring layer; ruby rack **0·1·1** (queue-time header only); sidekiq 0-2 | Counted | Default | ✓ (js, rack) | js-contrib `http.ts`; py-contrib `otel_middleware.py`; ruby-contrib `tracer_middleware.rb:82` | T2a-js#4,9; T2a-py#6; T2a-ruby#3,7 merge |
| Rust `tracing-opentelemetry`; OTel testbed / telemetrygen | attrs per span event; synthetic shapes | tracing span event: 2 always (level, target) · +3 default-on (code file/module/line) · +N user. OTel testbed 2 attrs/span, 2 attrs/datapoint × 7 datapoints, 6 attrs/log; telemetrygen **2 attrs/span, 1 attr/log, 0 attrs/metric point**, batch 100, 1 child span, **0 links** | Counted | Default / Demo | | tracing-opentelemetry@1d5422f `layer.rs:1345-1435`; otel-collector-contrib@7d24eb8 `testbed/data_providers.go`, `cmd/telemetrygen` | T2b#32, T6#21 merge |
| JS express / Ruby Rails | spans per request | express **2 · 5 · 12+** (one narrow span per matched middleware/router/handler layer); a Rails request is likewise many narrow spans (0-9 attrs each), not one wide one | **Estimated** (mechanism read; range inferred) | Default | | js-contrib `instrumentation-express`; ruby-contrib Rails gems | T2a-js#8, T2a-ruby notes |
| Python / JS / Ruby / Java | **HTTP semconv mode defaults** | **Python still defaults to OLD semconv** (`http/dup` roughly doubles span width); JS has **no dup/old mode at all** at its pin; Ruby stable is default; Java's DB extractor default-emits `network.transport`/`network.type` as a side effect of its old-DB branch | Counted | Default | ✓ (py, js) | `_semconv.py:196-201`; js `src/semconv.ts`; `SemconvSelectionResolver.java` | T2a-py#1, T2a-js#3, T2b#4 |

---

## §D — Resource and enrichment (cuts across all three signals)

| Source | Dimension | Value / range | Fid. | Repr. | ✓ | Citation | id |
|---|---|---|---|---|---|---|---|
| **SDK default resource width, per language** | | | | | | | |
| OTel Go / .NET / Rust SDKs | default resource | **4 each** — `service.name` + 3 `telemetry.sdk.*`. Env detector adds 0 unless `OTEL_RESOURCE_ATTRIBUTES` set; Go adds 1 more behind experimental `OTEL_GO_X_RESOURCE` | Counted | Default | ✓✓✓ (all three) | otel-go@58db4c8 `resource.go`; otel-dotnet@7d1bd16 `ResourceBuilder.cs`; otel-rust@1983384 `resource/mod.rs` | T2b#19,26,39 merge |
| OTel Python / JS SDKs | default resource | **5 each** (Python adds `service.instance.id`; JS adds `process.runtime.name`); `sdk-node` turns on **3 of 5** detectors; Python's ProcessResourceDetector is opt-in at 7·8·9 | Counted | Default | | otel-python@5321c60 `sdk/resources/`; otel-js@66c0403 `ResourceImpl.ts`, `sdk.ts` | T2a-py#20-21, T2a-js#20,22 merge |
| OTel Ruby SDK | default resource | **9** (`service.name` + 3 `telemetry.sdk.*` + 5 `process.*`); host/os/container/cloud detectors are separate opt-in gems | Counted | Default | | otel-ruby@681075f `sdk/resources/resource.rb:36-75` | T2a-ruby#16-17 |
| **Java javaagent** | default resource providers | **12 · 13 · 15** (bare host/VM 12 · +1 `container.id` · +2 MANIFEST fallback) — the outlier, because the agent ships host/os/process/container providers the bare SDKs don't auto-attach | Counted | Default | ✓c | otel-java-instr@8ad06a0 the 7 resource-provider classes | T2b#6 |
| **The semconv resource budget** | | | | | | | |
| OTel semconv v1.44.0 | attrs per identity ("entity") group | service 3 · host 9 · os 5 · process 12 · process.runtime 3 · container 7 · k8s.pod 7 · cloud 6 · telemetry.sdk 3 · deployment 1 → **sum of non-opt-in totals = 38**, +18 more with every opt-in enabled | Counted | Default | ✓ | semconv@e10a930 `model/*/entities.yaml` | T1#9 |
| **Collector- and agent-side enrichment** | | | | | | | |
| OTel `k8sattributesprocessor` | resource attrs added | **6 with no config** (`k8s.namespace.name`, `k8s.pod.{name,uid,start_time}`, `k8s.deployment.name`, `k8s.node.name`) → **30 fixed names** with everything enabled, **plus unbounded pod/namespace/node label & annotation extraction** | Counted | Default / Configured (max) | ✓ / ✓c | otel-collector-contrib@7d24eb8 `k8sattributesprocessor/README.md:68-129` | T5b#D1-2 merge |
| OTel `resourcedetectionprocessor` | enabled-by-default of total, per detector | system **2 of 17** · docker 2 of 4 · **ec2 9 of 9** · eks 2 of 10 · **gcp 17 of 19** · azure 10 of 11 · aks 2 of 3 | Counted | Default/Configured | ✓ (system, ec2, azure, aks); ✓c (gcp) | same repo `resourcedetectionprocessor/internal/*/documentation.md` | T5b#D3-9 merge |
| Fluent Bit / Fluentd `kubernetes` filters | fields under `kubernetes.*` | Fluent Bit **11 default** (9 fixed + `labels{}` + `annotations{}`, both On) → **14** with owner_references/namespace_labels/namespace_annotations; Fluentd ~10-12 default, annotations off unless `annotation_match` set | Counted / Estimated (Fluentd) | Default/Configured | ✓ (Fluent Bit) | docs.fluentbit.io filters/kubernetes; fluent-plugin-kubernetes_metadata_filter README | T5b#D10-11 merge |
| Vector sources | fields in the docs' own sample event | `kubernetes_logs` **16 total** = 5 top-level + 11 under `kubernetes.*` (3 of them maps/arrays); `file` **5** top-level | Counted | Default | | vector.dev sources/{kubernetes_logs,file} | T5b#D12-13 merge |
| Filebeat processors | fields added | `add_host_metadata` **12** (+7 geo opt-in) · `add_cloud_metadata` 6 common (4-7 per provider) · `add_docker_metadata` 4 · `add_kubernetes_metadata` 4+ (page doesn't enumerate — a floor) | Reported | Default/Configured | | elastic.co/guide beats/filebeat those pages | T5b#D14-17 merge |
| Logstash / Promtail | automatic fields; Loki labels | Logstash core **4** (`@timestamp,@version,host,message`); Promtail's widely-copied helm default applies **9** Loki labels per pod | Counted | Default / Configured | ✓ (Logstash) | elastic.co/guide logstash/8.19; grafana/helm-charts `charts/promtail/values.yaml` | T5b#D18-19 merge |
| Datadog Agent | out-of-the-box tags | **k8s pod: 43** distinct names (38 pod-level — High 2 / Orchestrator 3 / Low 33 — + 5 host-level); a single point realistically carries ~10-20. Docker container: 23 documented, **~11 apply to a plain container**. EC2 host ~8. Unified Service Tagging: **3** (`env`,`service`,`version`) | Counted / Reported (EC2) | Default | ✓c (pod) | docs.datadoghq.com containers/kubernetes/tag, agent/docker/tag, unified_service_tagging | T3#10-13 merge |

---

## §E — SDK and vendor limits, and batch defaults

"Who" distinguishes a limit the software **enforces** from one it only **advises**.

| Source | Limit | Value | Who | Fid. | Repr. | ✓ | Citation | id |
|---|---|---|---|---|---|---|---|---|
| OTel SDK spec | attribute **count** limits | **128** — generic, per-span, per-log-record, per-event, per-link, and span events/links themselves | enforces | Counted | Default | ✓ (at two SHAs) | otel-spec@cda6778 `sdk-environment-variables.md:179-204` | T1#24-25, T6#20 |
| OTel SDK spec | attribute **value length** limit | **no default** (unset) — OTel bounds record *shape*, not record *size* | — | Counted | Default | ✓ | same file | T1#24 |
| OTel spec + 6 SDKs | batch processor | BSP queue **2048**, batch **512**, delay **5000 ms**, timeout **30000 ms**; **BLRP delay is 1000 ms**, rest identical. Independently reproduced in Go, .NET, Rust, Python, JS, Ruby | enforces | Counted | Default | ✓ (Go, .NET, Py, JS) | otel-spec@cda6778 L156-170 + the six SDK sources | T1#26, T2b#20,27,38, T2a-py#22-23, T2a-js#23-24, T2a-ruby#18 |
| OTel OTLP exporter spec | timeout; message sizes | timeout **10 s**; max request **64 MiB**; max response **4 MiB** (the size text was added 2026-08-05) | enforces | Counted | Default | ✓ | otel-spec@34a2483 `protocol/exporter.md:66-76` | T1#27 |
| OTel Collector | `batchprocessor` | `send_batch_size` **8192**, timeout **200 ms** | enforces | Counted | Default | ✓ | otel-collector `processor/batchprocessor/factory.go` | T6#20 |
| Grafana Loki | label vs per-record limits | `max_label_names_per_series` **15** (indexed stream labels) but **128 structured-metadata entries per line**, 64 KB each; name 1024 B / value 2048 B; `max_line_size` 256 KB | enforces | Counted | Default | ✓ (two SHAs) | loki@443c975 `pkg/validation/limits.go` | T1#29, T6#29 |
| Grafana Mimir | label limits | `max_label_names_per_series` **30**; `..._info_series` **80**; name 1024 / value 2048 | enforces | Counted | Default | | mimir@149c8f7 `pkg/util/validation/limits.go:407-412` | T6#28 |
| Prometheus | scrape guardrails | `label_limit`, `label_name_length_limit`, `label_value_length_limit`, `sample_limit` **all default 0 = unlimited** | advises | Counted | Default | | prometheus.io scrape_config; practices/naming | T1#28 |
| Honeycomb | fields / sizes | **2,000 distinct fields per event**; string value ≤64 KB; event <1 MB | enforces | Counted/Reported | Default | | docs.honeycomb.io organizing-data; api/events | T1#32, T6#35 |
| New Relic | custom event limits | **254 attrs/event** via Event API (**64** via APM agent API; 48,000 per event type); key ≤255 chars; value ≤4096 (≤255 via agents) | enforces | Counted | Default | | docs.newrelic.com limits-custom-event-data | T1#31 |
| AWS CloudWatch / Google Cloud | dimension & label caps | CloudWatch **30 dimensions/metric**, ≤1000 metrics/call, ≤1 MB; EMF ≤30 keys / ≤100 definitions. GCP Logging **64 labels/entry** (key 512 B, value 64 KiB); GCP Monitoring **30 labels/descriptor** (200 Prometheus-sourced). **Both clouds land on 30** | enforces | Counted | Default | | docs.aws.amazon.com PutMetricData + EMF spec; docs.cloud.google.com quotas | T1#33-34,39 merge |
| Elasticsearch | mapping limits | `index.mapping.total_fields.limit` **1000**; `index.mapping.depth.limit` **20** | enforces | Counted | Default | | elastic.co mapping-limit settings | T1#36 |
| Datadog / InfluxDB | cardinality guidance | Datadog: tag length **≤200 chars**, **no discoverable hard tag-count cap**. InfluxDB OSS: **no published per-point tag limit**; Cloud enforces an account-specific series quota with no published default | advises | Reported | Default | | docs.datadoghq.com/getting_started/tagging/; docs.influxdata.com high-cardinality | T1#30,37 merge |
| Rust `tracing` | fields per callsite | **No cap** since tokio-rs/tracing#2508 (2023); only a stale doc line "up to 32 key-value fields" survives | — | Counted | Default | ✓c | tracing@74fc079 `tracing-core/src/field.rs` | T6#31, T2b#36 |

---

## §F — Peers: how each represents attributes, and on what evidence

| Peer | Representation / optimization (N) | Evidence behind it | Fid. | Repr. | ✓ | Citation | id |
|---|---|---|---|---|---|---|---|
| Vector / VRL | `ObjectMap = BTreeMap<KeyString, Value>`; **no small-map optimization, no `smallvec` anywhere in the event path**. `KeyString(String)` is a **plain heap String, no inline capacity** | **No stated evidence** for either: the KeyString PR gives no benchmark, only "lays the groundwork for future changes" | Counted (incl. the absence) | Default | | vrl `value/value.rs:34,38-66`, `value/keystring.rs:6-11`; vectordotdev/vrl#530 | T6#1-2 merge |
| Vector | `MetricTags`: `TagValueSet::{Empty, Single, Set}` — **inline optimization, N = 1** | Rationale in the doc comment ("avoid allocating a hash table for the common case of a single value") — **asserted, unmeasured** | Counted | Default | | vector@c2d37ef `event/metric/tags.rs:97-112,460` | T6#3 |
| Vector / VRL (open PR) | proposed flat `ObjectMap` + **"inline storage for maps below a threshold (e.g. 8-16 entries)"** | Attacks a stated "5-6 pointer indirections" chain (`LogEvent → Arc<Inner> → Value::Object(BTreeMap) → node → Value::Bytes → heap`) | Reported (proposal, not built) | — | ✓ | lukesteensen/vrl@90933ef `objectmap-optimization-proposal.md` | T6#5 |
| Vector / VRL | **measured flat-vs-BTree crossover** | ≈**128 fields** for isolated miss-lookups, but ≈**16** once the bench **clones the event**; read-only favours flat at every width. Raw per-width numbers **unpublished** | Reported (measured) | Demo | ✓ | vectordotdev/vrl#1826 §Benchmark takeaways | T6#6 |
| Vector / VRL | flat `ObjectMap` end to end | **+29%** on `datadog_agent_remap_blackhole` — "from contiguous memory layout and cheaper structural clones, not from KeyString changes" | Reported (measured) | Configured | | same PR, `BENCH_PLAN.md` §1 | T6#7 |
| Vector / VRL | **inline-string × linear-scan trap** | `EcoString` (16 B, 15-byte inline) **hurts the flat map ~10%** — its `as_str()` double-branch is paid per key per lookup; `CompactString` (single discriminant check) is neutral | Reported (measured) | Demo | | same, `BENCH_PLAN.md` §2-3 | T6#8 |
| Vector / VRL | bench widths, key lengths, "realistic" event | sweep **[4,8,16,32,64,128,256,512,1024]** (comment expects crossover 32-256); Short keys ~6-8 B, "Realistic" ~24-28 B; the designed-against event is **15 fields**; Vector's own in-repo `benches/event.rs` uses **3 fields**, 4 B keys, depth 3 | Counted (code) | Demo | ✓ | `benches/objectmap_cliff.rs:60-93`; `BENCH_PLAN.md`; vector `benches/event.rs` | T6#9-10,13 merge |
| Vector / VRL (open PR) | SSO key proposal | `SmolStr`: **22 B inline** — "all common event key names fit"; +12% (regex_parsing) / +8% (real_world_1) | Reported (measured) | Configured | | vectordotdev/vrl#1825 (OPEN) | T6#12 |
| OTel Collector | `pcommon.Map` = **unsorted `[]internal.KeyValue`**; `Get` is a linear scan and **every `Put*` calls `Get` first**, so insert is O(n) too | `Sort` was removed (#6688) to keep an open-addressing hash map possible later; `xpdata/map_builder.go` calls the dedup check "a linear time operation" | Counted (code) / Reported (rationale) | Default | ✓ | otel-collector@9637d3e `pdata/pcommon/map.go`, issue #6688 | T6#18 |
| OTel Collector | attributes-per-record evidence | **NONE FOUND** — exhaustive issue/PR search across collector + contrib returned no statement of a real-world count | Counted (of the absence) | — | | multiple `gh search issues/prs` | T6#19 |
| Prometheus | **default** `stringlabels`: one packed flat string `[len][name][len][value]…`, 1 B per length under 255, names in order, linear scan. Opt-in `dedupelabels`: per-set `SymbolTable`, varint symbol indexes, **alphabetical** — the same shape `logit` uses, **still behind a build tag** | Measured: "24 bytes of overhead for the slice and 16 for each name and value" → "16 bytes overhead plus 1 for each"; LoadWAL heap **481 → 388 MB (−19.5%)** | Counted (code) / Reported (measured) | Default / Configured | | prometheus@64c05e8 `model/labels/labels_{stringlabels,dedupelabels}.go`; PRs #10991, #12304 | T6#25-26 merge |
| Prometheus | label-count sweep its own benches use (**5, 10, 30**) | Synthetic names 5-9 B, empty values. PR #16069: middle-label `get` **−19.94% at 5 labels, −39.48% at 30** — roughly linear with position scanned | Counted / Reported | Demo | ✓c | `model/labels/labels_test.go:667-698`; PR #16069 | T6#27 |
| Datadog Agent | tag accumulator pre-allocation, **N = 128** | "128 tags should be enough for most metrics" — **stated rationale, no evidence**; their own tagstore benchmark uses `nMaxTags = 5` | Counted | Default | | datadog-agent@751c4de `pkg/tagset/*.go` | T6#30 |
| Fluent Bit | **no attribute map at all** — `flb_log_event` holds `msgpack_object*` pointers into the chunk (group_attributes / group_metadata / metadata / body / root); records stay in msgpack | The *group* level is a resource-attribute analogue. Chunks: 256 KB hint, FS max 2 MB | Counted | Default | | fluent-bit@8c71dfd `include/fluent-bit/flb_log_event.h:51-60` | T6#34 |
| lading (Datadog's load generator) | `dogstatsd`: **tags_per_msg 2-50**, contexts 5,000-10,000, name 1-200 B, tag 3-100 B. `opentelemetry`: **attributes_per_resource 1-20**, per_scope **0**, per_log/per_metric **0-10** | Cites Datadog's own tag-naming guidance for lengths; ranges chosen to **stress a receiver**, not a measured distribution, and no doc says where they came from | Counted | Default | ✓ (otel) | lading@558d2d5 `lading_payload/src/{dogstatsd,opentelemetry}` | T6#16-17 merge |
| otel-arrow | dictionary encoding; compression; its one committed dataset | Rationale "attributes … are often repeated and limited in cardinality" — **no citation, no number**. Compression vs OTLP+zstd: metrics **×2.19-2.45**, multivariate ×2.60-7.97, logs ×1.03-2.01, traces ×1.60-2.83, but the "Prod (anonymized)" set is one unattributed sentence and its raw files are **git-ignored and absent**. The committed dataset: **9 tags + 8 fields**, constant over 10,000 records, transparently synthetic ids | Reported (measured, unreproducible) / Counted (dataset) | Demo + claimed-Production | ✓c | otel-arrow@5588c3e `docs/otap_basics.md`, `docs/benchmarks-phase1.md`, `data/multivariate-metrics.json` | T6#22-24 merge |

**The VRL benchmarking trap.** VRL's sweep put the flat-vs-BTree crossover at ≈128 fields for
isolated miss-lookups but ≈16 once the bench **clones the event** — what a real pipeline does — with
read-only favouring flat at every width and +29% end to end. A container microbenchmark omitting the
clone systematically understates flat layouts. Paired trap: inline keys can *lose* ~10% in a
scan-based map when the key type's `as_str()` costs a double branch, paid per key per lookup (T6#6-8).

---

## §G — Published production-derived numbers and adoption

| Source (+date) | Dimension | Value | Fid. | Repr. | ✓ | Citation | id |
|---|---|---|---|---|---|---|---|
| Luo et al., SoCC 2021 (Alibaba, >10 bn traces / 7 d, ~20,000 microservices) | call-graph depth & breadth | **avg depth 4.27, sd 3.25**; >**4%** of graphs exceed depth 10; >**10%** span >**40 unique microservices**; >10% of stateless microservices have out-degree ≥5, most have in-degree 1 | Reported (measured) | **Production** | ✓ | SOCC21-Alibaba.pdf §3.1, Fig. 3(a)/4 | T6#40 |
| Charity Majors, charity.wtf, 2022-08-15 | fields per event, observed; events per request | "maturely instrumented datasets that we see are often **200-500 dimensions wide**"; a worked example yields **19 events** for one request (a simpler one, 8) | Reported | Production (vendor-observed; the author sells the practice) / Demo | ✓ | charity.wtf/2022/08/15/live-your-best-life-with-structured-events/ | T6#36-37 merge |
| Cloudflare, 2023-03-03 | Prometheus at scale | **916 instances, ~4.9 bn series**; ~5 M series/instance average, biggest ~30 M; **64 labels** allowed per series (a cap they impose, **not an observed average**); default `sample_limit` **200** | Reported | Production | ✓ | blog.cloudflare.com/how-cloudflare-runs-prometheus-at-scale/ | T6#41 |
| Zhu et al., ICSE-SEIP 2019 | distinct log templates per system | HDFS 30 · Apache 44 · OpenStack 51 · OpenSSH 62 · **Proxifier 9** · ZooKeeper 95 · HPC 104 · HealthApp 220 · Hadoop 298 · Spark 456 · Linux 488 · BGL 619 · Mac 2,214 · Thunderbird 4,040 · Windows 4,833 · **Android 76,923** | Reported | Production (real systems) | ✓c | arXiv:1811.03509 Table III | T6#38 |
| LogHub, arXiv:2008.06448 | bytes per log line | 19 datasets, >77 GB. Derived: HDFS_v1 ≈**141 B/line** · Thunderbird ≈150 · BGL ≈157 · Windows ≈245 | Reported (counts) / **Estimated** (the division is the track's; the paper asserts no average) | Production | | arXiv:2008.06448 Table I | T6#39 |
| Pinterest Eng., 2017-09-29 | calls per trace | "tens of services and **hundreds of network calls per-trace**" | Reported (order of magnitude) | Production | ✓ | medium.com/pinterest-engineering/analyzing-distributed-trace-data-6aae58919949 | T6#46 |
| Splunk (AppDynamics on-prem) sizing docs | vendor "average event size" | Log Analytics **350 B/event**; business-transaction event **1 KB**; DB-visibility/EUM raw events ~2 KB | Reported | Default (vendor sizing) | | help.splunk.com events-service-requirements — **AppDynamics-scoped; do not generalise** | T6#43 |
| Cribl reference architecture | vendor sizing assumption | "average event size of **500 bytes** is assumed" | Reported | Default (vendor sizing) | | docs.cribl.io reference-arch-syslog — **LOW CONFIDENCE: live page 403'd, quote from a search cache** | T6#44 |
| Grafana Observability Survey 2026 (n=1,363) / 2025 (n=1,255) | signal mix; tool sprawl | 2026: Prometheus 77% · OTel 76% · both 65%; OTel use **metrics 57% · traces 50% · logs 48%** · profiles 9%. 2025, any technology: **metrics 95% · logs 87% · traces 57%** · profiles 16%; 101 technologies cited, avg **8 tools / 16 data sources** per org (24 at 5,000+ employees) | Reported (vendor survey) | Production (self-reported) | | grafana.com/observability-survey/ and /2025/ | T6#47-49 merge |

*Grading note carried from T6:* Grafana sells observability and surveys its own audience, so the
Prometheus/OTel shares are near-certainly inflated against the wider market, and the "signal mix" is
a **respondent** mix, not a **byte** mix; these were read off landing pages, not the raw PDFs.

---

## Cross-track conflicts and resolutions

1. **HTTP server span 27 (T1#1) vs 29 (T6#33)** — *SHA drift, both correct*: same file six weeks apart, requirement tiers identical, only opt-in grew 11→13. State both pins.
2. **Rust `tracing` 32-field cap live (T2b#36, and T2b#31 built on it) vs removed (T6#31)** — *T2b is wrong*: #2508 (2023) dropped it; `ValueSet` is a plain slice at both pins.
3. **DogStatsD 1432 B (T3#14, client source) vs 1472 B (T1#38, docs)** — *different definitions, both right*: 1472 is the docs' MTU ceiling, 1432 the client's `UDP_OPTIMAL_PAYLOAD_LENGTH` pack size (which the Etsy server's advisory tiers also use). T1#38's separate "1432 for lower-MTU paths" clause is dropped as unverifiable.
4. **Prometheus at 5 labels "~0" (T6#27) vs −19.94%** — *T6 is wrong*: it quoted the `first_label/get` row while describing `middle_label/get`.
5. **Datadog max-metric integration envoy (T3#1) vs clickhouse** — *T3 wrong on attribution only*; the distribution reproduces exactly. Its note "envoy and vault drive the max" is right for p90, not max.
6. **k8sattributes "25 fixed names" (T5b#D2 inline) vs "up to 30" (same track's summary)** — a track disagreeing with itself; 25 is the non-container subtotal, **30** the grand total.
7. **dd-trace-py writer 8 MiB (docs) vs 20 MiB (pinned source)** — *real docs-vs-source drift*; the pinned SHA is newer, so 20 MiB.
8. **node_exporter "1,227 families"** — a definitional fork, not a conflict: 1,227 declared `# TYPE` headers vs 1,180 name-roots that actually emit. Both stated in the row.
9. **Telegraf `postgresql`: README table says 18 fields, its own example shows 24+10** — unresolved *within* the source (the table is stale); left Estimated, not carried as a row.

## UNVERIFIED (kept out of the tables)

- New Relic `Transaction`/`Span`/`Log` attribute counts — client-rendered SPA, two fetches disagreed (T3#24-25).
- Datadog's hard per-metric **tag-count** cap and InfluxDB OSS's per-point tag limit — neither published (T1#30,37).
- redis_exporter / mysqld_exporter exposition samples — neither repo commits one (T5a#8). Telegraf's default-enabled input list (no `etc/telegraf.conf` at the pin, T5a#22), `postgresql_extensible` fields (query-defined, T5a#26), `net`'s `/proc/net/snmp` extension (T5a#30).
- cAdvisor's kubelet-injected `container`/`namespace`/`pod` labels — absent from `google/cadvisor`; added by a kubelet wrapper in an uncloned repo (T5a#19).
- .NET `IncludeScopes` real scope-attribute count `M` (T2b#29); Rust SDK attribute *value-length* limit and opentelemetry-rust log batch defaults; whether log4j2's `JsonTemplateLayout` writes `"log.level"` literally or expands it (T4 notes).
- ingress-nginx's claim that "the source page's own count of 16 undercounts" — the 17 is confirmed, the stated "16" was not locatable. CloudTrail's full 13-sub-field `userIdentity` list and 5-level depth — spot-checked only.
- Exact SHAs for `encode/uvicorn` and structlog (GitHub API rate-limited); `sharding_cluster_role` cited to `config.py` but living in `common.py`.
- Whether the JS SDK drops `undefined`-valued attributes at span creation; JS `instrumentation-redis`/koa, Python urllib3, Ruby `action_view` payload keys — not read.

## Selection notes

Merged rows carry "merge" in the id column: the six SDK batch-processor rows into one, the five OTel
count-limit rows into one, and likewise Apache common/combined, HAProxy http/tcp, the four Go
loggers, KSM's four families, OCSF's three classes, Splunk CIM's two models, ECS's four schema rows,
the per-framework `http.route`-only rows, Prometheus's two label builds, lading's two generators and
otel-arrow's three. Dropped as low-yield or redundant: python-json-logger, django-structlog,
Django/Celery logger extras, Traefik, Sentry/Elastic APM document widths, Envoy's synthetic `/stats`
fixture, Splunk HEC size, brandur's 200 B line, Telegraf `nginx`, KSM's 339-family count, Python
wsgi metric labels, Datadog's per-integration metric-count lists, Vector's regression payload mix,
lading's JSON generator, and T5a's already-trimmed `client_golang`/django-prometheus rows.

