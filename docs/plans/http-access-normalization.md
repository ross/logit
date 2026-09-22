---
created: 2026-09-22
updated: 2026-09-22
---

# Enabling plan: `http_access` — access-log normalization from raw semconv fields

## Context

[ADR `http-access-normalization`](../adr/http-access-normalization.md) decides the shape: a new
`http_access` transform that takes the raw, unnormalized attributes a web server logged under
OTel semconv names and rewrites them, once and natively, into their conformant form plus a
bounded set of derived attributes — replacing the per-server `map`/`log-format` logic every
operator otherwise writes by hand. This plan is the build-out: what lands in which order, in
which files, and how each piece is verified. Read the ADR first; this document repeats its
consequences, not its reasoning.

Stream key **`hacc`**: branches `hacc/w0`…`hacc/w7`, a strictly linear stack, each PR based on
and targeting its parent's branch, brought up to date with `git merge origin/main` (never rebase).

## Decisions already settled

| Question | Decision |
|---|---|
| Input names | OTel semconv names, sent raw; `logit`-named composites (`http.request.line`, `url.original`), durations (`http.request.duration_{s,ms,us}`), and proxy fields (`upstream.*`, `cache.status`, `network.connection.*`, `http.termination_state`, `http.response.compression_ratio`); trace/timing names are `data-model.md`'s existing table, unchanged |
| Dashed aliases | Every canonical name is also accepted with each `.` replaced by `-`; fixed pre-interned table, dotted wins, dashed key removed on rename; includes the trace/timing names since `http_access` runs before `trace_context` |
| Placement | `json -> http_access -> trace_context`; the component only *emits* `span.name`/`span.status`/`span.duration_s`, `trace_context` lifts; never mints a trace id |
| Failure model | Best-effort per field; `process` always returns `true`; a value that doesn't parse is left in place and counted `invalid{field}` (plus a throttled diagnostic for status/duration only) |
| Derived set (v1) | `user_agent.class` (+ `user_agent.synthetic.type: bot`), `http.route`, `error.type`, `span.name`, `span.status` (never `ok`), `span.duration_s` mirror, `http.request.method_original`, and `client.address` from XFF under `forwarded: {trust: true}` |
| Bounded outputs | Every `http.route`/`user_agent.class` value comes from config or the built-in table — never a capture |
| Metrics | Not emitted; the doc ships the `kv_metrics` + `keep` block |
| Caps | Per-field character limits from `CAPPED_FIELDS` (in `logit-config`), overridable via `max_length:`; control bytes in the capped prefix become `_` |
| `json` | Gains `invalid_utf8: reject \| replace` (default `reject`); `replace` retries a failed parse on a `from_utf8_lossy` copy, failure path only |
| Route rule shape | One flat struct `{builtin?, match?, route?}`, validated by graph rule 60 to be exactly `builtin` xor `match`+`route` — not an untagged enum |
| UA matching | Ordered `Vec<(Value, Regex)>` scanned with `is_match` (config rules first, then scanner > tool > crawler > browser), not `RegexSet` |
| Repo configs | `examples/nginx/nginx.conf`, `demo/nginx/nginx.conf`, `demo/haproxy/haproxy.cfg`, `examples/nginx-to-influxdb.yaml`, `demo/logit.yaml` all move onto it; `$status` quoted; demo's `scale` stage removed |
| Landing | PR stack only. Nothing merged by this workstream; Ross directs merging |

## Design

### `crates/logit-config/src/lib.rs` (W1, W2)

```rust
// W1
#[serde(rename_all = "snake_case")]
pub enum JsonInvalidUtf8 { Reject, Replace }   // default Reject
Json { skip_to_brace: bool, /* existing */ #[serde(default)] invalid_utf8: JsonInvalidUtf8 }

// W2 — beside ValueAllowList
#[serde(deny_unknown_fields)]
pub struct HttpRouteRule {
    #[serde(default)] pub builtin: Option<HttpRouteSet>,
    #[serde(default, rename = "match")] pub pattern: Option<String>,
    #[serde(default)] pub route: Option<String>,
}
#[serde(rename_all = "snake_case")]
pub enum HttpRouteSet { Assets, WellKnown, Probes }
#[serde(deny_unknown_fields)]
pub struct UserAgentRule { #[serde(rename = "match")] pub pattern: String, pub class: String }
#[serde(deny_unknown_fields)]
pub struct ForwardedConfig { #[serde(default = "default_true")] pub trust: bool }

/// Every field `http_access` caps, with its default character limit — the one source of truth
/// graph rule 60 and the transform both read.
pub const CAPPED_FIELDS: &[(&str, usize)] = &[
    ("url.path", 256), ("url.query", 256), ("user_agent.original", 256),
    ("http.request.header.referer", 256), ("server.address", 253),
    ("client.address", 128), ("network.peer.address", 128),
    ("http.request.header.x-forwarded-for", 128), ("upstream.address", 128), ("user.name", 128),
    ("http.request.method_original", 32), ("cache.status", 32), ("url.scheme", 16),
    ("network.protocol.version", 8), ("http.termination_state", 8),
];

HttpAccess {
    #[serde(default)] routes: Vec<HttpRouteRule>,
    #[serde(default)] route_other: Option<String>,
    #[serde(default)] user_agent_rules: Vec<UserAgentRule>,
    #[serde(default)] max_length: BTreeMap<String, usize>,
    #[serde(default)] redact_query: Vec<String>,
    #[serde(default)] forwarded: Option<ForwardedConfig>,
},
```

Every type derives `Serialize + Deserialize + JsonSchema`; every doc comment names the graph rule
that backstops it and the ADR. `script/schema` after each of W1 and W2.

### `crates/logit-pipeline/src/graph.rs` (W2) — rule 60

Module-doc entry and a body block after rule 59. Clauses: (a) compile every `routes[].match` and
`user_agent_rules[].match` with `::regex::Regex::new`, failing with the pattern and the error
(rule 31's shape); (b) reject an empty `match`, `route`, `class`, `route_other`, or
`redact_query` entry; (c) each route rule is exactly `builtin` xor (`match` + `route`) — the
error names which half is missing or which extra key is present; (d) a repeated `builtin:`;
(e) a `max_length` key not in `CAPPED_FIELDS`, listing the valid keys; (f) `max_length` value 0;
(g) `forwarded: {trust: false}` — "omit the block instead". No no-op clause. Plus `HttpAccess` in
`is_implemented`/`role` (Transform)/`kind_name`, and `docs/design/pipeline-graph.md`'s list,
kind sketch, and roles table.

### `crates/logit-transforms/src/json.rs` (W1)

On the existing `Err(err)` arm only: if `invalid_utf8 == Replace` and `std::str::from_utf8(&body)`
fails, build `Bytes::from(String::from_utf8_lossy(&body).into_owned())`, re-run the same
`parse_object`/`parse_object_prefix` against it (the zero-copy `Str` slices then borrow that
owned buffer, which is fine — it lives as long as they do), count
`logit.transform.json.utf8_replaced`, and `warn_throttled("invalid_utf8", …)`. A second failure
falls through to the existing `parse_failure` path. The happy path is untouched, so the existing
allocation pins for `json` do not move.

### `crates/logit-transforms/src/http_access.rs` (W2, W3)

```rust
pub enum RouteSet { Assets, WellKnown, Probes }         // mirrors logit_config::HttpRouteSet
pub enum RouteRule { Builtin(RouteSet), Pattern { pattern: String, route: String } }
pub struct UaRule { pub pattern: String, pub class: String }
pub struct HttpAccessConfig {
    pub routes: Vec<RouteRule>, pub route_other: Option<String>,
    pub user_agent_rules: Vec<UaRule>, pub max_length: Vec<(String, usize)>,
    pub redact_query: Vec<String>, pub trust_forwarded: bool,
}

struct Keys { /* every Symbol this component reads or writes, interned once in `new` */ }
struct Aliases(Vec<(Symbol, Symbol)>);                    // (dashed, dotted), fixed table
const KNOWN_METHODS: [&str; 10];
const BUILTIN_UA_RULES: [(&str, &str); 4];                // (class, pattern), in priority order
const BUILTIN_ROUTE_SETS: [(RouteSet, &str, &str); 3];    // (set, pattern, route value)
const SENSITIVE_QUERY_KEYS: [&str; 7];

struct Classifier { rules: Vec<(Value, Regex)> }
struct Router { rules: Vec<(Value, Regex, Origin)>, other: Option<Value> }   // Origin: Rule|Builtin
struct SpanNames { cells: Vec<Option<Value>> }            // 11 * (routes + 2), lazily filled
struct ErrorTypes { cells: Box<[Option<Value>; 100]> }    // 500..=599, lazily filled

pub struct HttpAccess {
    keys: Keys, aliases: Aliases,
    caps: Vec<(Symbol, usize, &'static str)>,             // (field, cap, static name for the tag)
    ua: Classifier, router: Router, span_names: SpanNames, error_types: ErrorTypes,
    redact: Vec<String>, trust_forwarded: bool,
    scratch: Vec<u8>,                                     // reused for cleaning/redaction
    telemetry: Telemetry, diag: Diagnostics,
}
impl HttpAccess {
    pub fn new(config: HttpAccessConfig) -> Result<Self, ::regex::Error>;  // unreachable Err after rule 60
    pub fn with_telemetry(self, t: Telemetry) -> Self;
    pub fn with_diagnostics(self, d: Diagnostics) -> Self;
}
impl Transform for HttpAccess { fn process(&mut self, _: &Arc<Resource>, e: &mut Event) -> bool }
```

`process` runs these steps in order, each a free function over `&mut AttrMap` + `&Keys` with its
own tests:

0. **De-alias.** For each `(dashed, dotted)`: if `dashed` is present, remove it and insert under
   `dotted` only if `dotted` is absent. Counted `normalized{field=<dotted>}`.
1. **Decompose** `http.request.line` (three space-separated tokens; a bad shape is left in place
   and diagnosed `bad_request_line`) and `url.original` (split at the first `?`), each writing
   only the atomic fields that are absent, then removed. `Bytes::slice` throughout.
2. **Coerce numerics** (`status_code`, sizes, ports, connection id/requests, compression ratio) from
   an `I64`/`U64`/`F64` or a quoted numeric string; `"000"` → `I64(0)`. Unparseable: left in
   place, `invalid{field}`; `bad_status` diagnostic for the status. `upstream.status` is coerced
   only when it is a single integer; a comma-separated per-attempt list is left verbatim and
   never counted invalid.
3. **Durations**: `http.request.duration_{us,ms}` → `http.request.duration_s` (`F64`), source
   removed; `_s` wins if both are present; same for `upstream.{duration,connect,header}_*`.
   Unparseable: `bad_duration`.
4. **Method**: case-sensitive membership in `KNOWN_METHODS`; on a miss write
   `http.request.method_original` (the raw `Bytes`, refcount bump) and overwrite with `_OTHER`.
5. **Version**: strip `HTTP/` (`Bytes::slice(5..)`); `2.0` → `2`, `3.0` → `3`; anything else
   untouched.
6. **Query**: strip a leading `?`; then redact — for each `key=value` pair whose key matches the
   sensitive list ASCII-case-insensitively, replace the value with `REDACTED` (one allocation,
   only when something was actually redacted); counted `redacted`.
7. **Cap and clean** every `CAPPED_FIELDS` entry: walk `char_indices()` to at most `cap + 1`
   chars; if a `cap + 1`-th exists, `Bytes::slice(..offset)` and count `truncated{field}`; then
   on the capped prefix replace any byte `< 0x20` or `== 0x7F` with `_` (length-preserving, so
   `Value::Str`'s UTF-8 invariant is kept for free — `keep_values::lower`'s reasoning), allocating
   only when a byte actually changed; count `cleaned{field}`.
8. **Classify.** `user_agent.class` on the *uncapped* value (the identifying token is often at the
   tail of a spoofed UA): absent → nothing written; present but empty (`""`/`"-"`) → `none`;
   else first matching rule, else `other`; `crawler`/`scanner` also write
   `user_agent.synthetic.type = bot`. `http.route` on the *capped* `url.path`: first matching rule
   (config rules and built-ins in config order), else `route_other`, else nothing; counted
   `routed{outcome=rule|builtin|other|none}` once per event that has a path.
9. **Derive.** `error.type` = the status as a decimal string, 5xx only. `span.status` = `error`
   for 5xx or 0, else `unset`. `span.name` from the lazily-filled table: `{M} {route}` or `{M}`,
   `M` = `HTTP` when the method is `_OTHER`. `span.duration_s` = `http.request.duration_s` only
   when no `span.duration*` and no `span.start*` is present. Under `trust_forwarded`, when
   `http.request.header.x-forwarded-for` is present and non-empty, `client.address` = its first
   comma-separated hop, ASCII-trimmed, `Bytes::slice`. Each counted `derived{field}`.

Never touched: `traceparent`, `trace.*`, `span.id`, `span.parent_id`, `span.kind`,
`span.start*`, `span.end*`, `event.log`, `event.metrics`, `event.span`, `event.timestamp`, the
batch `Resource` (no `map_resource`). No `keep_source`.

Built-in tables (finalized in W3 against a 62-UA/18-path corpus of real, sourced strings — the
member lists below are what shipped, not the placeholder W2 carried; see that PR's tests and doc
comments for the full hand-traced justification of each token):

| Table | Order / value | Pattern (`is_match`) |
|---|---|---|
| UA `scanner` | 1, `(?i)` | `nmap\|masscan\|zgrab\|nikto\|sqlmap\|dirbuster\|gobuster\|ffuf\|fuzz faster u fool\|feroxbuster\|wpscan\|nuclei\|acunetix\|nessus\|qualys\|openvas\|censysinspect\|internetmeasurement\|expanse\|paloaltonetworks\|leakix\|shodan` |
| UA `tool` | 2, `(?i)` | `curl/\|wget/\|libwww-perl\|python-requests\|python-urllib\|aiohttp\|httpie\|go-http-client\|okhttp\|apache-httpclient\|^java/\|axios/\|node-fetch\|guzzlehttp\|postmanruntime\|insomnia\|reqwest/\|k6/\|wrk/\|jmeter\|kube-probe\|prometheus/\|blackbox-exporter\|elb-healthchecker\|googlehc\|telegraf/\|vector/\|chrome-lighthouse\|uptimerobot` |
| UA `crawler` | 3, `(?i)` | `\bbot\b\|bot/\|spider\|crawler\|slurp\|scrapy\|googlebot\|bingbot\|yandex\|baiduspider\|duckduckbot\|facebookexternalhit\|twitterbot\|linkedinbot\|slackbot\|applebot\|petalbot\|semrushbot\|ahrefsbot\|mj12bot\|ccbot\|gptbot\|chatgpt-user\|claudebot\|perplexitybot\|amazonbot\|bytespider\|feedfetcher` |
| UA `browser` | 4 (last: crawlers spoof `Mozilla/`), `(?i)` | `mozilla/\|opera/\|dalvik/\|safari/\|msie \|trident/` |
| route `probes` → `/{probe}` | case-sensitive | `^/(-/(healthy\|ready)\|health\|healthz\|healthcheck\|livez\|readyz\|ready\|ping\|status\|_status\|up\|metrics\|_metrics\|stats\|nginx_status\|server-status\|haproxy_status\|version\|_version)/?$` |
| route `well_known` → `/{well-known}` | case-sensitive | `^/(\.well-known/.*\|robots\.txt\|favicon\.ico\|sitemap[^/]*\.xml(\.gz)?\|humans\.txt\|security\.txt\|apple-touch-icon[^/]*\.png\|browserconfig\.xml\|manifest\.json\|manifest\.webmanifest\|crossdomain\.xml\|ads\.txt\|app-ads\.txt)$` |
| route `assets` → `/{asset}` | no `json`/`xml`/`txt`/`csv` — routinely API responses; `(?i)` | `\.(css\|js\|mjs\|cjs\|map\|png\|jpe?g\|gif\|webp\|avif\|svg\|ico\|bmp\|tiff?\|woff2?\|ttf\|otf\|eot\|heic\|heif\|docx\|xlsx\|pptx\|apk\|ipa\|mp4\|m4v\|webm\|mov\|mp3\|m4a\|ogg\|oga\|opus\|wav\|flac\|pdf\|zip\|gz\|tgz\|bz2\|xz\|7z\|rar\|wasm)$` |

`\bbot\b` rather than `bot\b` because `bot\b` matches phone models like `CUBOT`; bare `bot/` is kept
alongside it, not `\bbot/` (a `/` is always a word boundary, so `\bbot/` adds nothing), because it
is what catches a `...Bot/<version>` token with no boundary before it (`DotBot/1.2`,
`Discordbot/2.0`, `YandexMobileBot/3.0`) -- `CUBOT` still falls through, with no `/` after it. Its
one false positive, `UptimeRobot/2.0`, is a synthetic monitor, so `uptimerobot` sits in `tool`,
which is checked first. `yandex`, not `yandexbot`, because most of Yandex's robots
(`YandexImages`, `YandexMetrika`, `YandexFavicons`, ...) carry no `bot` token. `^java/` is
anchored (Java's `HttpURLConnection` sends exactly `Java/<version>` as the whole string);
`blackbox-exporter` is hyphenated (the exporter's real format since v0.28.0); `chrome-lighthouse`
moved from `crawler` to `tool` (a synthetic audit tool, not a content crawler); `fuzz faster u
fool` was added because ffuf's actual default UA contains no "ffuf" substring at all. `probes` and
`well_known` are matched case-sensitively, not `(?i)`, because a probe/well-known path is a
protocol- or convention-mandated literal (unlike a file extension, whose casing is conventionally
meaningless); `probes` gained `-/(healthy|ready)` for Prometheus's/Alertmanager's Management API,
`well_known` gained `manifest.webmanifest`, and `assets` gained `.heic`/`.heif`,
`.docx`/`.xlsx`/`.pptx`, and `.apk`/`.ipa`. **Known limitation, not fixable by regex**: nikto
(2.6.1+), nuclei, and Nessus all spoof a real browser `User-Agent` by default, so their
un-configured traffic classifies `browser`, not `scanner`, regardless of table tuning.

### `crates/logit-cli/src/pipeline.rs` (W2)

`to_http_access_config` beside `to_allow_lists`/`to_flatten_fields`, for their reason
(`logit-transforms` does not depend on `logit-config`). The arm chains `.with_telemetry(..)` and
`.with_diagnostics(Diagnostics::new(id).with_telemetry(..))`; its `?` is unreachable after rule 60.

### Producer configs (W5)

`examples/nginx/nginx.conf`: all three `map`s deleted; `access_json_syslog` becomes
`access_semconv` — raw semconv keys, `url.original: $request_uri` (never `$uri`), quoted
`$status`, quoted `$msec` as `span.end_s`, bare `$request_time` as `http.request.duration_s`,
`$upstream_*` quoted, `traceparent` raw. `access_json_full` unchanged. `demo/nginx/nginx.conf`:
propagation maps kept (this tier forwards the `traceparent` it logs), `$span_status` deleted,
semconv keys, quoted `$status`. `demo/haproxy/haproxy.cfg`: `%{+json}o` with dashed keys
(`%(url-path)[var(txn.path)]`, `%(http-response-status-code:sint)ST`,
`%(http-request-duration_ms:sint)Ta`, `%(span-start_us:sint)[request_date(us)]`,
`upstream-connect_ms` ← `%Tc`, `upstream-header_ms` ← `%Tr`, extras as `haproxy_timer_*_ms`); new
`txn.query`/`txn.ua` vars; the `txn.span_status` rules deleted. Every spelling verified with
`nginx -t`/`haproxy -c` in the real images, and one live line from each pasted into the PR.

### Consumer configs (W6)

`examples/nginx-to-influxdb.yaml`: `nginx_http` between `nginx_json` and `nginx_trace`
(`routes: [probes, assets, '^/$' -> /]`, `route_other: /{other}`); `nginx_trace` gains
`mint_id: true` (its `name:` is now a fallback — `http_access`'s `span.name` wins);
`kv_metrics` reads `http.response.body.size`, `http.request.duration_s` (s),
`upstream.duration_s`; `keep` keeps semconv's low-cardinality metric set
(`server.address`, `http.request.method`, `http.response.status_code`, `http.route`,
`network.protocol.version`, `url.scheme`) plus `user_agent.class`; `keep_values` clamps
`server.address`. `demo/logit.yaml`: `haproxy_http`/`nginx_http` sharing a `routes:` block for
the traffic generator's paths (`/health`, `/graph.svg`, `/work`, `/boom`, `/`); `*_trace`
re-sourced; `nginx_scale` deleted; metrics and `keep` on the semconv names. Both header
diagrams updated. `logit-transforms`' `chained_pipeline_test` extended to
`json -> http_access -> trace_context -> kv_metrics -> keep -> aggregate`.

### Docs (W7)

New `docs/http-access-logs.md`: what/who → the one-component pipeline → the canonical field
table (with the action per field and the variable per server) → start-versus-end timestamp
semantics per server → unit suffixes → nginx `escape=json` quoting rules → "Emitters that can't
write dotted keys" with the HAProxy `%{+json}o` worked example → one snippet per server (nginx,
Apache, HAProxy, Varnish, Squid, Caddy, Envoy, Traefik), each naming what that server cannot
provide → what `http_access` derives → bounding cardinality (`routes`, `route_other`,
`max_length`, `keep_values`) → the `kv_metrics` + `keep` block for `http.server.request.duration`
→ cutover (two `access_log` lines side by side; watch `routed{outcome="none"}` fall) → what it
does not do. Pointers: `docs/deploying.md`'s nginx recipe, `docs/design/data-model.md`'s
well-known table, `docs/design/internal-telemetry.md`, `docs/known-gaps.md` (the gaps the ADR
lists, plus the corrected `$uri`/invalid-UTF-8 entry and a narrowing of the `$host` entry),
`AGENTS.md`, `README.md`.

## Workstreams

| | Branch: PR title | Scope | Proof |
|---|---|---|---|
| W0 | `hacc/w0: ADR and plan for native HTTP access-log normalization` | This plan, the ADR, both index rows | links resolve |
| W1 | `hacc/w1: json — opt-in invalid_utf8: replace` | `json.rs`, `JsonInvalidUtf8`, registry, schema, tests, a paragraph in `json-parsing-into-attributes.md` | `script/check`; `json`'s allocation pins unchanged; a Latin-1 byte parses under `replace` and fails under `reject` |
| W2 | `hacc/w2: http_access — config, graph rule 60, and the transform` | config types + `CAPPED_FIELDS`; rule 60; `http_access.rs` with every step and the alias table (placeholder minimal UA/route patterns); `lib.rs`; registry + converter; schema; `pipeline-graph.md` | `script/check`; `script/schema` no diff; one unit test per contract (composites; `000`; durations and `_s`-wins; `_OTHER` + original; version; `?` strip; each sensitive key; multi-byte char-boundary cap; control byte → `_` at unchanged length; absent-vs-empty UA; first-match routes and config-beats-builtin; no `route_other` → no route, method-only name; `HTTP …` for `_OTHER`; status 500/0/200/404 → `span.status`; `error.type` 5xx only; duration mirror on and off; XFF ignored by default, first hop when trusted; every dashed alias round-trips and dotted wins; metrics-only event untouched; `log`/`metrics`/`span`/`timestamp`/`Resource` untouched; idempotent; every counter fires); config round-trips; one graph test per rule-60 clause plus "a bare `http_access` validates"; `build_spec_builds_a_working_http_access_transform` |
| W3 | `hacc/w3: http_access — the built-in UA and route tables` | The full regex tables; corpus tests over 62 real UA strings and 18 real paths with provenance comments; negatives (`CUBOT`, a spoofed `Mozilla/…Googlebot`, `/orders.json`) | tests |
| W4 | `hacc/w4: http_access — allocation pins and the memory table` | `fixtures.rs` (`const HTTP_ACCESS_SEMCONV_LINE` with provenance, `http_access_event()`, `http_access()`), `allocations.rs` (non-HTTP event; conforming line warm; conforming line cold; a line needing a clean), `docs/design/memory.md` rows | numbers pinned from a measurement |
| W5 | `hacc/w5: the nginx and haproxy log formats become raw semconv fields` | the three producer configs | `nginx -t`/`haproxy -c`; a captured live line per producer in the PR |
| W6 | `hacc/w6: the example and demo pipelines run http_access` | the two `logit.yaml`s; `chained_pipeline_test` | `script/validate`; `every_shipped_config_loads_and_validates`; `logit graph`; the end-to-end runs below |
| W7 | `hacc/w7: docs/http-access-logs.md and the doc pointers` | the new doc and every pointer | links resolve; every snippet pasteable |

## Verification

- `script/check` in the loop; `script/cibuild` before each PR; `script/schema` after W1 and W2;
  `script/validate` after W6.
- Negative configs by hand after W2: an uncompilable `match`; empty `match`/`route`; `builtin`
  and `match` together; two `builtin: assets`; `max_length: {url.paths: 10}`;
  `max_length: {url.path: 0}`; `forwarded: {trust: false}`; `redact_query: ['']`.
- `logit graph examples/nginx-to-influxdb.yaml` renders `nginx_http` between `nginx_json` and
  `nginx_trace`.
- `script/server` against `examples/`: `curl` at `static.local`/`proxy.local` for
  `/favicon.ico`, a 500, a `curl` user agent, a `?X-Amz-Signature=…` query, and a Latin-1
  `User-Agent` byte; `stdio_out` shows an integer `http.response.status_code`,
  `http.route: /{asset}`, `user_agent.class: tool`, `REDACTED`, `span.name: GET /{asset}`,
  `span.status: error` on the 500, and the Latin-1 line parsed under `invalid_utf8: replace`;
  `nginx.requests`/`nginx.request_time` land in InfluxDB under the renamed fields.
- `script/demo`: Tempo shows spans `GET /work`, `GET /{probe}`, `GET /{asset}` on both the
  `haproxy` and `nginx` services; `/boom` (503) carries `status = error`, `/missing` (404)
  carries unset; the HAProxy line arrives through `%{+json}o` with dashed keys and comes out
  dotted.
- Allocation tests run, and their numbers read, before anything is written into
  `docs/design/memory.md`.

## Risks

- **The `Bytes` promotion trick.** A zero-allocation steady state for a classified event depends
  on a warm-up clone in `new()` promoting each config-derived `Value::Str` to its shared
  representation; W4's tripwire decides, and the fallback is a pinned `+1`.
- **HAProxy `%(name)` item spellings** — `upstream.address` as `host:port` in particular — are
  settled in shape, not in spelling, until `haproxy -c` in W5.
- **`span.status` no longer `ok`** changes Tempo's colouring in the demo.
- **Route matching is O(rules) with no prefilter.** A `script/perf` scenario to find the knee is a
  follow-up, not part of this stream.
