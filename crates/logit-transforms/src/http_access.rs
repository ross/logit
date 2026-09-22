//! `http_access`: normalizes a web server's access line -- logged under raw OTel semconv attribute
//! names, the standard name with the untouched value -- into its conformant form, plus a small,
//! bounded set of derived attributes. See `docs/adr/http-access-normalization.md` for why the
//! work lives here rather than in a hundred lines of per-server `map` blocks, and
//! `docs/plans/http-access-normalization.md` for the step list this module implements.
//!
//! Placed by the operator between `json` (or whatever parsed the line) and `trace_context`:
//! this component only *emits* `span.name`/`span.status`/`span.duration_s`; `trace_context`
//! lifts them, and nothing here ever mints a trace id.
//!
//! Best-effort per field, never all-or-nothing and never a dropped event -- the deliberate
//! opposite of `trace_context`'s contract, because that component writes *identity* (a half-lifted
//! trace id is a corrupt trace) while this one writes *descriptions*, where a half-normalized line
//! is strictly more useful than an untouched one. A value that doesn't parse is left exactly as
//! it arrived and counted `invalid{field}`; a status, duration, or request line that doesn't
//! parse also gets one throttled diagnostic, since those are genuine producer malformation. An
//! absent field produces nothing: no default `url.scheme`, no `user_agent.class` for a producer
//! that doesn't log the header, no fabricated route -- ADR
//! `operator-declared-resource-attributes`' rule applied one component over.
//!
//! An existing `user_agent.class` is trusted and never recomputed: the user agent is classified
//! on its uncapped, uncleaned value, which only the first pass ever sees, so a second pass over
//! the same event keeps the first verdict rather than re-reading the capped/cleaned value. It
//! also means a class set upstream (a `set` or Lua stage, or a producer that logs one itself)
//! overrides the built-in and configured tables -- an operator override, by design.
//!
//! **Every classification value is bounded by construction**: `http.route`, `user_agent.class`,
//! `span.name`, `span.status`, and `error.type` values all come from config, a built-in table, or
//! a closed numeric range -- never from a capture of the input. That is the review question for
//! any change here, the same one `shape`'s ADR asks: does any output value derive from the input?
//!
//! Allocation posture (`docs/design/memory.md`, pinned from a measurement in W4, not here): every
//! constant output is `Bytes::from_static`, every substring a `Bytes::slice` of the value it came
//! from, and every config-derived output a `Value` built once in [`HttpAccess::new`] and cloned by
//! refcount. What allocates is what has to: a control-byte clean, a redaction, and the first use of
//! each lazily-built `span.name`/`error.type` cell.
//!
//! Stateless apart from those caches -- only `process` is overridden; `flush_interval`/`flush`
//! keep the `Transform` trait's defaults, and there is no `map_resource`: the batch `Resource` is
//! never touched.

use ::regex::Regex;
use bytes::Bytes;
use logit_core::interner::{intern, resolve};
use logit_core::{AttrMap, Diagnostics, Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

// -- Names ------------------------------------------------------------------------------------
//
// Every attribute name this component reads or writes. Each is interned exactly once, in
// `HttpAccess::new`, paired with its `&'static str` spelling so a telemetry tag never has to
// `resolve` a `Symbol` (a shard-locked interner probe) on the per-event path.

/// `logit`'s own composite: `GET /p?q HTTP/1.1`, nginx's `$request`.
const REQUEST_LINE: &str = "http.request.line";
/// `logit`'s own composite: `/p?q`, nginx's `$request_uri`.
const URL_ORIGINAL: &str = "url.original";
const METHOD: &str = "http.request.method";
const METHOD_ORIGINAL: &str = "http.request.method_original";
const URL_PATH: &str = "url.path";
const URL_QUERY: &str = "url.query";
const PROTOCOL_VERSION: &str = "network.protocol.version";
const STATUS: &str = "http.response.status_code";
/// A single integer, or nginx's per-attempt list (`502, 200` / `502 : 200`), left verbatim.
const UPSTREAM_STATUS: &str = "upstream.status";
/// A ratio (nginx's `$gzip_ratio`, `2.50`), so coerced to `F64`, not `I64`.
const COMPRESSION_RATIO: &str = "http.response.compression_ratio";
const USER_AGENT: &str = "user_agent.original";
const UA_CLASS: &str = "user_agent.class";
const UA_SYNTHETIC: &str = "user_agent.synthetic.type";
const ROUTE: &str = "http.route";
const ERROR_TYPE: &str = "error.type";
const SPAN_NAME: &str = "span.name";
const SPAN_STATUS: &str = "span.status";
const SPAN_DURATION_S: &str = "span.duration_s";
const XFF: &str = "http.request.header.x-forwarded-for";
const CLIENT_ADDRESS: &str = "client.address";

/// Coerced to `I64` from an integer, an integral float, or a quoted numeric string.
const INTEGER_FIELDS: [&str; 9] = [
    "http.request.body.size",
    "http.response.body.size",
    "http.request.size",
    "http.response.size",
    "server.port",
    "client.port",
    "network.peer.port",
    "network.connection.id",
    "network.connection.requests",
];

/// Each duration quantity in its four spellings, each with its divisor to seconds: `_s` first --
/// the one this component writes (`F64` seconds), and the one that wins when more than one is
/// present -- then `_ms`, `_us`, and the unsuffixed form, which is integer nanoseconds exactly as
/// `docs/design/data-model.md`'s unsuffixed `span.duration` is (Traefik's `Duration`/
/// `OriginDuration` are raw int64 nanoseconds). The unit is only ever in the name.
const DURATIONS: [[(&str, f64); 4]; 4] = [
    [
        ("http.request.duration_s", 1.0),
        ("http.request.duration_ms", 1e3),
        ("http.request.duration_us", 1e6),
        ("http.request.duration", 1e9),
    ],
    [
        ("upstream.duration_s", 1.0),
        ("upstream.duration_ms", 1e3),
        ("upstream.duration_us", 1e6),
        ("upstream.duration", 1e9),
    ],
    [
        ("upstream.connect_s", 1.0),
        ("upstream.connect_ms", 1e3),
        ("upstream.connect_us", 1e6),
        ("upstream.connect", 1e9),
    ],
    [
        ("upstream.header_s", 1.0),
        ("upstream.header_ms", 1e3),
        ("upstream.header_us", 1e6),
        ("upstream.header", 1e9),
    ],
];
/// The index of the unsuffixed, integer-nanosecond form within each `DURATIONS` row.
const NANOS_FORM: usize = 3;

/// The span timing a line may carry itself -- any one of these present means `trace_context`
/// already has what it needs, and `span.duration_s` is not mirrored from the request duration.
/// `span.end*` is deliberately absent: an nginx line's lone `span.end_s` is exactly the case the
/// mirror exists for, turning it into a resolvable `(end - duration, end)` pair.
const SPAN_TIMING: [&str; 9] = [
    "span.duration",
    "span.duration_us",
    "span.duration_ms",
    "span.duration_s",
    "span.start",
    "span.start_us",
    "span.start_ms",
    "span.start_s",
    "span.start_rfc3339",
];

/// Names this component only caps or passes through, listed so they get a dashed alias too.
const PASSTHROUGH: [&str; 8] = [
    "url.scheme",
    "server.address",
    "network.peer.address",
    "upstream.address",
    "user.name",
    "cache.status",
    "http.termination_state",
    "http.request.header.referer",
];

/// `docs/design/data-model.md`'s trace/timing names that `trace_context` reads and this
/// component never touches -- aliased anyway, because `http_access` runs *before*
/// `trace_context`, which then stays dotted-only (the ADR's dashed-alias decision).
/// (`span.name`/`span.status`/`span.duration_s` and the `span.start*`/`span.duration*` forms are
/// listed above, since this component reads or writes them.)
const TRACE_NAMES: [&str; 10] = [
    "trace.id",
    "trace.flags",
    "span.id",
    "span.parent_id",
    "span.kind",
    "span.end",
    "span.end_us",
    "span.end_ms",
    "span.end_s",
    "span.end_rfc3339",
];

/// Semconv's known methods, compared case-sensitively (semconv's own rule: `get` is not `GET`).
/// Their order is the row order of the `span.name` table.
const KNOWN_METHODS: [&str; 10] =
    ["CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "QUERY", "TRACE"];
const OTHER_METHOD: &str = "_OTHER";
/// Row `KNOWN_METHODS.len()` of the `span.name` table: semconv names a span `HTTP {route}`, not
/// `_OTHER {route}`, when the method is unknown.
const SPAN_METHOD_OTHER: &str = "HTTP";

/// `(class, pattern)`, in priority order -- a UA claiming both `Mozilla/` and `bot` is a crawler,
/// which is why `browser` comes last and why this is an ordered scan, not a `RegexSet` (the ADR's
/// Alternatives). The member lists are placeholders W3 finalizes against a corpus; the classes and
/// their order are fixed. `\bbot\b` rather than `bot\b` because the latter matches phone models
/// like `CUBOT`.
const BUILTIN_UA_RULES: [(&str, &str); 4] = [
    (
        "scanner",
        r"(?i)nmap|masscan|zgrab|nikto|sqlmap|dirbuster|gobuster|ffuf|feroxbuster|wpscan|nuclei|acunetix|nessus|qualys|openvas|censysinspect|internetmeasurement|expanse|leakix|shodan|paloaltonetworks",
    ),
    (
        "tool",
        r"(?i)curl/|wget/|libwww-perl|python-requests|python-urllib|aiohttp|httpie|go-http-client|okhttp|apache-httpclient|java/|axios/|node-fetch|guzzlehttp|postmanruntime|insomnia|reqwest/|k6/|wrk/|jmeter|kube-probe|prometheus/|blackbox_exporter|elb-healthchecker|googlehc|telegraf|vector/",
    ),
    (
        "crawler",
        r"(?i)\bbot\b|bot/|spider|crawler|slurp|scrapy|googlebot|bingbot|yandex|baiduspider|duckduckbot|facebookexternalhit|twitterbot|linkedinbot|slackbot|applebot|petalbot|semrushbot|ahrefsbot|mj12bot|ccbot|gptbot|chatgpt-user|claudebot|perplexitybot|amazonbot|bytespider|feedfetcher|lighthouse",
    ),
    ("browser", r"(?i)mozilla/|opera/|dalvik/|safari/|msie |trident/"),
];

/// A UA that matches no rule -- still a real, bounded class.
const UA_OTHER: &str = "other";
/// A UA the producer logged but that carried nothing (`""`/`"-"`). Distinct from an *absent*
/// header, which writes no class at all.
const UA_NONE: &str = "none";

/// `(set, pattern, route value)`. The three route values are fixed by the plan; the member lists
/// are W3's to finalize. `assets` carries no `json`/`xml`/`txt`/`csv` -- those are routinely API
/// responses, and routing an API endpoint to `/{asset}` would hide it.
const BUILTIN_ROUTE_SETS: [(RouteSet, &str, &str); 3] = [
    (
        RouteSet::Probes,
        r"(?i)^/(health|healthz|healthcheck|livez|readyz|ready|ping|status|_status|up|metrics|_metrics|stats|nginx_status|server-status|haproxy_status|version|_version)/?$",
        "/{probe}",
    ),
    (
        RouteSet::WellKnown,
        r"(?i)^/(\.well-known/.*|robots\.txt|favicon\.ico|sitemap[^/]*\.xml(\.gz)?|humans\.txt|security\.txt|apple-touch-icon[^/]*\.png|browserconfig\.xml|manifest\.json|crossdomain\.xml|ads\.txt|app-ads\.txt)$",
        "/{well-known}",
    ),
    (
        RouteSet::Assets,
        r"(?i)\.(css|js|mjs|cjs|map|png|jpe?g|gif|webp|avif|svg|ico|bmp|tiff?|woff2?|ttf|otf|eot|mp4|m4v|webm|mov|mp3|m4a|ogg|oga|opus|wav|flac|pdf|zip|gz|tgz|bz2|xz|7z|rar|wasm)$",
        "/{asset}",
    ),
];

/// Semconv's sensitive query keys (`url.query`'s own note), extensible via `redact_query`.
const SENSITIVE_QUERY_KEYS: [&str; 7] = [
    "AWSAccessKeyId",
    "Signature",
    "sig",
    "X-Goog-Signature",
    "X-Amz-Signature",
    "X-Amz-Credential",
    "X-Amz-Security-Token",
];
const REDACTED: &[u8] = b"REDACTED";

// -- Telemetry names --------------------------------------------------------------------------

const NORMALIZED: &str = "logit.transform.http_access.normalized";
const DERIVED: &str = "logit.transform.http_access.derived";
const TRUNCATED: &str = "logit.transform.http_access.truncated";
const CLEANED: &str = "logit.transform.http_access.cleaned";
const INVALID: &str = "logit.transform.http_access.invalid";
const REDACTED_METRIC: &str = "logit.transform.http_access.redacted";
const ROUTED: &str = "logit.transform.http_access.routed";

// -- Config -----------------------------------------------------------------------------------

/// Mirrors `logit_config::HttpRouteSet` -- `logit-transforms` deliberately doesn't depend on
/// `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), so the CLI converts, the
/// pattern `Normalize`/`MatchMode` already follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteSet {
    Assets,
    WellKnown,
    Probes,
}

/// One `routes:` entry, already shape-checked by graph rule 60 -- so an enum here, where the
/// config side is one flat struct only so that rule can name the offending key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteRule {
    Builtin(RouteSet),
    Pattern { pattern: String, route: String },
}

/// One `user_agent_rules:` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UaRule {
    pub pattern: String,
    pub class: String,
}

/// Everything [`HttpAccess::new`] takes. `max_length` is the **fully resolved** cap list --
/// `logit_config::CAPPED_FIELDS`' defaults with the config's `max_length:` overrides applied, as
/// `logit-cli` builds it -- so this crate never needs to know the defaults, and a field absent
/// from it is simply never capped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpAccessConfig {
    pub routes: Vec<RouteRule>,
    pub route_other: Option<String>,
    pub user_agent_rules: Vec<UaRule>,
    pub max_length: Vec<(String, usize)>,
    pub redact_query: Vec<String>,
    pub trust_forwarded: bool,
}

// -- Pre-built state --------------------------------------------------------------------------

/// An interned attribute name with its `&'static` spelling, for telemetry tags.
#[derive(Debug, Clone, Copy)]
struct Key {
    sym: Symbol,
    name: &'static str,
}

impl Key {
    fn new(name: &'static str) -> Self {
        Self { sym: intern(name), name }
    }

    /// For a config-supplied name: `resolve` hands back the interner's own `&'static str`.
    fn owned(name: &str) -> Self {
        let sym = intern(name);
        Self { sym, name: resolve(sym) }
    }
}

/// Every fixed name, interned once in [`HttpAccess::new`].
struct Keys {
    request_line: Key,
    url_original: Key,
    method: Key,
    method_original: Key,
    url_path: Key,
    url_query: Key,
    protocol_version: Key,
    status: Key,
    upstream_status: Key,
    compression_ratio: Key,
    user_agent: Key,
    ua_class: Key,
    ua_synthetic: Key,
    route: Key,
    error_type: Key,
    span_name: Key,
    span_status: Key,
    span_duration_s: Key,
    xff: Key,
    client_address: Key,
    integers: [Key; INTEGER_FIELDS.len()],
    /// `[_s, _ms, _us, ns]` per quantity, each with its divisor to seconds.
    durations: [[(Key, f64); 4]; DURATIONS.len()],
    span_timing: [Key; SPAN_TIMING.len()],
}

impl Keys {
    fn new() -> Self {
        Self {
            request_line: Key::new(REQUEST_LINE),
            url_original: Key::new(URL_ORIGINAL),
            method: Key::new(METHOD),
            method_original: Key::new(METHOD_ORIGINAL),
            url_path: Key::new(URL_PATH),
            url_query: Key::new(URL_QUERY),
            protocol_version: Key::new(PROTOCOL_VERSION),
            status: Key::new(STATUS),
            upstream_status: Key::new(UPSTREAM_STATUS),
            compression_ratio: Key::new(COMPRESSION_RATIO),
            user_agent: Key::new(USER_AGENT),
            ua_class: Key::new(UA_CLASS),
            ua_synthetic: Key::new(UA_SYNTHETIC),
            route: Key::new(ROUTE),
            error_type: Key::new(ERROR_TYPE),
            span_name: Key::new(SPAN_NAME),
            span_status: Key::new(SPAN_STATUS),
            span_duration_s: Key::new(SPAN_DURATION_S),
            xff: Key::new(XFF),
            client_address: Key::new(CLIENT_ADDRESS),
            integers: INTEGER_FIELDS.map(Key::new),
            durations: DURATIONS.map(|forms| forms.map(|(name, div)| (Key::new(name), div))),
            span_timing: SPAN_TIMING.map(Key::new),
        }
    }
}

/// Every canonical name this component knows, in no particular order, duplicates allowed --
/// the source of the dashed-alias table.
fn canonical_names() -> impl Iterator<Item = &'static str> {
    [
        REQUEST_LINE,
        URL_ORIGINAL,
        METHOD,
        METHOD_ORIGINAL,
        URL_PATH,
        URL_QUERY,
        PROTOCOL_VERSION,
        STATUS,
        UPSTREAM_STATUS,
        COMPRESSION_RATIO,
        USER_AGENT,
        UA_CLASS,
        UA_SYNTHETIC,
        ROUTE,
        ERROR_TYPE,
        SPAN_NAME,
        SPAN_STATUS,
        SPAN_DURATION_S,
        XFF,
        CLIENT_ADDRESS,
    ]
    .into_iter()
    .chain(INTEGER_FIELDS)
    .chain(DURATIONS.iter().flatten().map(|(name, _)| *name))
    .chain(SPAN_TIMING)
    .chain(PASSTHROUGH)
    .chain(TRACE_NAMES)
}

/// Builds the fixed `(dashed, dotted)` alias table: every canonical name with at least one `.`,
/// spelled with each `.` as `-`. A **fixed table** of pre-interned pairs, never a blanket
/// `-`-to-`.` rewrite, which would corrupt a legitimately dashed key -- and `-`, not `_`, because
/// canonical names already contain `_`, so only the dashed spelling is decodable from the dotted
/// table by rule alone (the ADR's dashed-alias decision). `extra` adds the configured cap fields,
/// which rule 60 already restricts to names this table carries anyway.
fn alias_table(extra: impl Iterator<Item = &'static str>) -> Vec<(Symbol, Key)> {
    let mut seen: Vec<&'static str> = Vec::new();
    canonical_names()
        .chain(extra)
        .filter(|name| name.contains('.'))
        .filter(|name| {
            let fresh = !seen.contains(name);
            seen.push(name);
            fresh
        })
        .map(|name| (intern(&name.replace('.', "-")), Key::new(name)))
        .collect()
}

/// Clones `value` once and drops the clone, so the one kept is already in `bytes`' shared,
/// refcounted representation. A `Bytes` built from an owned `String` starts out "promotable" and
/// only allocates its shared header on the *first* clone (`bytes-1.x`'s `shallow_clone_vec`,
/// which swaps the original's own data pointer in place -- `crates/logit-bench/src/fixtures.rs`'s
/// `cached_message` has the long form). Paying that here, at construction or on a lazy cell's
/// first fill, is what makes every later per-event clone a refcount bump.
fn shared(value: Value) -> Value {
    drop(value.clone());
    value
}

fn static_str(s: &'static str) -> Value {
    Value::Str(Bytes::from_static(s.as_bytes()))
}

/// Whether a matched class also marks the request synthetic -- semconv's
/// `user_agent.synthetic.type: bot`, for the two classes that are bots by definition.
fn is_bot_class(class: &str) -> bool {
    class == "crawler" || class == "scanner"
}

/// The ordered user-agent table: config rules first, then the built-ins. Scanned with `is_match`,
/// which allocates nothing -- `RegexSet::matches` allocates a `Vec<bool>` per call and cannot
/// express priority (the ADR's Alternatives).
struct Classifier {
    rules: Vec<UaEntry>,
    other: Value,
    none: Value,
    bot: Value,
}

struct UaEntry {
    class: Value,
    pattern: Regex,
    bot: bool,
}

impl Classifier {
    fn new(config: Vec<UaRule>) -> Result<Self, ::regex::Error> {
        let mut rules = Vec::with_capacity(config.len() + BUILTIN_UA_RULES.len());
        for rule in config {
            rules.push(UaEntry {
                bot: is_bot_class(&rule.class),
                pattern: Regex::new(&rule.pattern)?,
                class: shared(Value::str(rule.class)),
            });
        }
        for (class, pattern) in BUILTIN_UA_RULES {
            rules.push(UaEntry {
                class: static_str(class),
                pattern: Regex::new(pattern)?,
                bot: is_bot_class(class),
            });
        }
        Ok(Self {
            rules,
            other: static_str(UA_OTHER),
            none: static_str(UA_NONE),
            bot: static_str("bot"),
        })
    }

    /// The first matching rule's class (and whether it is a bot), else `other`.
    fn classify(&self, ua: &str) -> (&Value, bool) {
        self.rules
            .iter()
            .find(|entry| entry.pattern.is_match(ua))
            .map_or((&self.other, false), |entry| (&entry.class, entry.bot))
    }
}

/// Where a matched route came from -- the `routed{outcome}` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Rule,
    Builtin,
}

impl Origin {
    fn outcome(self) -> &'static str {
        match self {
            Origin::Rule => "rule",
            Origin::Builtin => "builtin",
        }
    }
}

/// The ordered route table, config order with each `builtin:` expanded in place.
struct Router {
    rules: Vec<(Value, Regex, Origin)>,
    other: Option<Value>,
}

impl Router {
    fn new(config: Vec<RouteRule>, other: Option<String>) -> Result<Self, ::regex::Error> {
        let mut rules = Vec::with_capacity(config.len());
        for rule in config {
            rules.push(match rule {
                RouteRule::Builtin(set) => {
                    let (_, pattern, route) = BUILTIN_ROUTE_SETS
                        .iter()
                        .find(|(candidate, _, _)| *candidate == set)
                        .expect("every RouteSet has a built-in entry");
                    (static_str(route), Regex::new(pattern)?, Origin::Builtin)
                }
                RouteRule::Pattern { pattern, route } => {
                    (shared(Value::str(route)), Regex::new(&pattern)?, Origin::Rule)
                }
            });
        }
        Ok(Self { rules, other: other.map(|route| shared(Value::str(route))) })
    }

    /// The route value for a column of the `span.name` table: a rule's, `route_other`'s (column
    /// `rules.len()`), or none (the last column).
    fn route_at(&self, column: usize) -> Option<&Value> {
        match self.rules.get(column) {
            Some((route, _, _)) => Some(route),
            None if column == self.rules.len() => self.other.as_ref(),
            None => None,
        }
    }
}

/// `span.name` values, one cell per `(method row, route column)`, built on first use. Rows are
/// `KNOWN_METHODS` then `HTTP`; columns are each route rule, then `route_other`, then "no route"
/// (the method alone). Bounded by construction: `11 * (rules + 2)` cells, every one a product of
/// config and a closed table.
struct SpanNames {
    width: usize,
    cells: Vec<Option<Value>>,
}

impl SpanNames {
    fn new(route_rules: usize) -> Self {
        let width = route_rules + 2;
        Self { width, cells: vec![None; (KNOWN_METHODS.len() + 1) * width] }
    }

    fn get(&mut self, router: &Router, method: usize, route: Option<usize>) -> Value {
        let column = route.unwrap_or(self.width - 1);
        let cell = &mut self.cells[method * self.width + column];
        cell.get_or_insert_with(|| {
            let method = KNOWN_METHODS.get(method).copied().unwrap_or(SPAN_METHOD_OTHER);
            match router.route_at(column).and_then(Value::as_str) {
                Some(route) => shared(Value::str(format!("{method} {route}"))),
                None => static_str(method),
            }
        })
        .clone()
    }
}

/// `error.type` values for `500..=599`, the decimal status, built on first use.
struct ErrorTypes {
    cells: Box<[Option<Value>; 100]>,
}

impl ErrorTypes {
    fn new() -> Self {
        Self { cells: Box::new(std::array::from_fn(|_| None)) }
    }

    /// `status` must be in `500..=599`.
    fn get(&mut self, status: i64) -> Value {
        let index = usize::try_from(status - 500).expect("a 5xx status");
        self.cells[index].get_or_insert_with(|| shared(Value::str(status.to_string()))).clone()
    }
}

// -- The transform ----------------------------------------------------------------------------

pub struct HttpAccess {
    keys: Keys,
    /// `(dashed, dotted)` -- see [`alias_table`].
    aliases: Vec<(Symbol, Key)>,
    /// `(field, character limit)`, resolved by the CLI from `CAPPED_FIELDS` plus overrides.
    caps: Vec<(Key, usize)>,
    ua: Classifier,
    router: Router,
    span_names: SpanNames,
    error_types: ErrorTypes,
    span_error: Value,
    span_unset: Value,
    other_method: Value,
    /// Semconv's seven plus the config's `redact_query`, matched ASCII-case-insensitively.
    redact: Vec<String>,
    trust_forwarded: bool,
    /// Reused across events for a control-byte clean or a redaction rewrite, so neither pays for
    /// a growing buffer -- only for the one exact-size copy that becomes the new `Bytes`.
    scratch: Vec<u8>,
    telemetry: Telemetry,
    diag: Diagnostics,
}

impl HttpAccess {
    /// Compiles every pattern and pre-builds every output value. The `Err` is unreachable after
    /// graph rule 60, which compiled the same config patterns at validate time; the built-in
    /// patterns are constants covered by this module's tests.
    pub fn new(config: HttpAccessConfig) -> Result<Self, ::regex::Error> {
        let caps: Vec<(Key, usize)> =
            config.max_length.iter().map(|(field, cap)| (Key::owned(field), *cap)).collect();
        let aliases = alias_table(caps.iter().map(|(key, _)| key.name));
        let router = Router::new(config.routes, config.route_other)?;
        let span_names = SpanNames::new(router.rules.len());
        let redact = SENSITIVE_QUERY_KEYS
            .iter()
            .map(|key| key.to_string())
            .chain(config.redact_query)
            .collect();
        Ok(Self {
            keys: Keys::new(),
            aliases,
            caps,
            ua: Classifier::new(config.user_agent_rules)?,
            router,
            span_names,
            error_types: ErrorTypes::new(),
            span_error: static_str("error"),
            span_unset: static_str("unset"),
            other_method: static_str(OTHER_METHOD),
            redact,
            trust_forwarded: config.trust_forwarded,
            scratch: Vec::new(),
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
        })
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Attaches the throttled-diagnostic sink for the three genuine producer malformations:
    /// `bad_request_line`, `bad_status`, `bad_duration`. An absent field, an unknown method, an
    /// unclassifiable UA, or an unrouted path is normal traffic and gets counters only.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }
}

fn count(telemetry: &Telemetry, metric: &'static str, field: &'static str) {
    telemetry.count(metric, 1.0, &[("field", field)]);
}

fn is_blank(bytes: &[u8]) -> bool {
    bytes.is_empty() || bytes == b"-"
}

/// An attribute counts as present only if it carries a value: `Null`, `""`, and `"-"` are how
/// nginx and haproxy spell "nothing here" -- `trace_context`'s `present` rule exactly, so the two
/// components agree on what a blank field is.
fn present(attrs: &AttrMap, key: Symbol) -> Option<&Value> {
    match attrs.get_sym(key)? {
        Value::Null => None,
        Value::Str(bytes) if is_blank(bytes) => None,
        value => Some(value),
    }
}

/// Writes `value` under `key` only if `key` is absent (by [`present`]'s rule) -- a composite
/// never overrides an atomic field the producer logged directly.
fn write_absent(attrs: &mut AttrMap, key: Symbol, value: Bytes) {
    if present(attrs, key).is_none() {
        attrs.insert_sym(key, Value::Str(value));
    }
}

// -- Step 0: de-alias -------------------------------------------------------------------------

/// Renames every dashed spelling present to its dotted one. The dotted spelling wins when both
/// are present, and the dashed key is removed either way, so nothing downstream ever sees two
/// spellings of one field.
fn dealias(attrs: &mut AttrMap, aliases: &[(Symbol, Key)], telemetry: &Telemetry) {
    for (dashed, dotted) in aliases {
        let Some(value) = attrs.remove_sym(*dashed) else { continue };
        if attrs.get_sym(dotted.sym).is_none() {
            attrs.insert_sym(dotted.sym, value);
            count(telemetry, NORMALIZED, dotted.name);
        }
    }
}

// -- Step 1: decompose the composites -----------------------------------------------------------

/// `METHOD TARGET PROTOCOL`: exactly two spaces, three non-empty tokens, each a zero-copy slice.
fn split_request_line(line: &Bytes) -> Option<(Bytes, Bytes, Bytes)> {
    let first = line.iter().position(|&b| b == b' ')?;
    let last = line.iter().rposition(|&b| b == b' ')?;
    if first == 0 || last + 1 == line.len() || first + 1 >= last {
        return None;
    }
    if line[first + 1..last].contains(&b' ') {
        return None;
    }
    Some((line.slice(..first), line.slice(first + 1..last), line.slice(last + 1..)))
}

/// Writes a request target's path and (non-empty) query into whichever of `url.path`/`url.query`
/// are absent. Split at the *first* `?`, so a `?` inside the query stays part of it.
fn write_target(attrs: &mut AttrMap, keys: &Keys, target: &Bytes) {
    let (path, query) = match target.iter().position(|&b| b == b'?') {
        Some(at) => (target.slice(..at), Some(target.slice(at + 1..))),
        None => (target.clone(), None),
    };
    if !path.is_empty() {
        write_absent(attrs, keys.url_path.sym, path);
    }
    if let Some(query) = query.filter(|query| !query.is_empty()) {
        write_absent(attrs, keys.url_query.sym, query);
    }
}

/// Step 1: `http.request.line` and `url.original` into their atomic fields, each writing only the
/// fields that are absent, then removed -- the atomic parts are strictly more useful, and keeping
/// both would give every consumer two sources for one fact. A request line that isn't three
/// tokens is left in place, counted, and diagnosed: it's a producer bug, not traffic.
fn decompose(attrs: &mut AttrMap, keys: &Keys, telemetry: &Telemetry, diag: &mut Diagnostics) {
    if let Some(line) = present(attrs, keys.request_line.sym) {
        let parts = match line {
            Value::Str(bytes) => split_request_line(bytes),
            _ => None,
        };
        match parts {
            Some((method, target, protocol)) => {
                write_absent(attrs, keys.method.sym, method);
                write_target(attrs, keys, &target);
                write_absent(attrs, keys.protocol_version.sym, protocol);
                attrs.remove_sym(keys.request_line.sym);
                count(telemetry, NORMALIZED, keys.request_line.name);
            }
            None => {
                count(telemetry, INVALID, keys.request_line.name);
                diag.warn_throttled(
                    "bad_request_line",
                    "http_access: http.request.line is not 'METHOD TARGET PROTOCOL'; left in place",
                );
            }
        }
    }
    if let Some(original) = present(attrs, keys.url_original.sym) {
        match original {
            Value::Str(bytes) => {
                let target = bytes.clone();
                write_target(attrs, keys, &target);
                attrs.remove_sym(keys.url_original.sym);
                count(telemetry, NORMALIZED, keys.url_original.name);
            }
            _ => count(telemetry, INVALID, keys.url_original.name),
        }
    }
}

// -- Step 2: numerics ---------------------------------------------------------------------------

/// What a coercion decided about one present value.
enum Coerced {
    /// Already in the target representation -- nothing to write, nothing to count.
    Keep,
    Write(Value),
    Invalid,
}

/// To `I64`, from `I64`/`U64`/an integral `F64`/a quoted decimal string (`"000"` is `0`, which is
/// what keeps nginx's client-abort status from breaking anything downstream). A `U64` past
/// `i64::MAX` is a real number, just not one this can write as `I64`, so it's left alone rather
/// than called invalid.
fn to_i64(value: &Value) -> Coerced {
    match value {
        Value::I64(_) => Coerced::Keep,
        Value::U64(n) => i64::try_from(*n).map_or(Coerced::Keep, |n| Coerced::Write(Value::I64(n))),
        Value::F64(f) if f.is_finite() && f.fract() == 0.0 && f.abs() < 9.0e18 => {
            Coerced::Write(Value::I64(*f as i64))
        }
        Value::Str(_) => value
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .map_or(Coerced::Invalid, |n| Coerced::Write(Value::I64(n))),
        _ => Coerced::Invalid,
    }
}

/// To `F64`, from any finite numeric or numeric string -- the compression ratio.
fn to_f64(value: &Value) -> Coerced {
    match value {
        Value::F64(f) if f.is_finite() => Coerced::Keep,
        _ => crate::numeric(value).map_or(Coerced::Invalid, |f| Coerced::Write(Value::F64(f))),
    }
}

/// nginx's per-attempt spelling for `$upstream_status`/`$upstream_response_time` and friends --
/// `502, 200`, or `502 : 200` across an internal redirect. Verbatim is the only faithful thing to
/// do with it, and it is normal traffic, never counted invalid.
fn is_attempt_list(value: &Value) -> bool {
    matches!(value, Value::Str(bytes) if bytes.iter().any(|&b| b == b',' || b == b':'))
}

/// Coerces one present field in place. Returns `true` if it was present and unparseable.
fn coerce(attrs: &mut AttrMap, key: Key, to: fn(&Value) -> Coerced, telemetry: &Telemetry) -> bool {
    let Some(value) = present(attrs, key.sym) else { return false };
    match to(value) {
        Coerced::Keep => false,
        Coerced::Write(value) => {
            attrs.insert_sym(key.sym, value);
            count(telemetry, NORMALIZED, key.name);
            false
        }
        Coerced::Invalid => {
            count(telemetry, INVALID, key.name);
            true
        }
    }
}

/// Step 2: status, sizes, ports, connection id/requests to `I64`; the compression ratio to
/// `F64`; `upstream.status` only when it is a single value.
fn coerce_numerics(
    attrs: &mut AttrMap,
    keys: &Keys,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) {
    if coerce(attrs, keys.status, to_i64, telemetry) {
        diag.warn_throttled(
            "bad_status",
            "http_access: http.response.status_code is not an integer; left in place",
        );
    }
    for key in keys.integers {
        coerce(attrs, key, to_i64, telemetry);
    }
    if !present(attrs, keys.upstream_status.sym).is_some_and(is_attempt_list) {
        coerce(attrs, keys.upstream_status, to_i64, telemetry);
    }
    coerce(attrs, keys.compression_ratio, to_f64, telemetry);
}

// -- Step 3: durations --------------------------------------------------------------------------

/// An unsuffixed duration is integer nanoseconds: `I64`/`U64` or a quoted integer. A float is
/// rejected outright rather than rounded -- `trace_context`'s rule for unsuffixed `span.duration`,
/// since a fractional nanosecond count is a producer bug, not a value to guess at.
fn integer_nanos(value: &Value) -> Option<f64> {
    match value {
        Value::I64(n) => Some(*n as f64),
        Value::U64(n) => Some(*n as f64),
        Value::Str(_) => value.as_str()?.parse::<i64>().ok().map(|n| n as f64),
        _ => None,
    }
}

/// Step 3: each duration quantity to `<quantity>_s` (`F64` seconds), the source removed --
/// keeping two spellings of one quantity is the contradiction `trace_context` already refuses.
/// `_s` wins when present, then `_ms`, `_us`, and the unsuffixed nanoseconds; the losers are
/// removed once the winner parses. An unparseable winner is left in place with everything else,
/// counted, and diagnosed.
fn convert_durations(
    attrs: &mut AttrMap,
    keys: &Keys,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) {
    for forms in &keys.durations {
        let Some((winner, (key, divisor))) = forms
            .iter()
            .enumerate()
            .find(|(_, (key, _))| present(attrs, key.sym).is_some())
            .map(|(index, form)| (index, *form))
        else {
            continue;
        };
        let value = present(attrs, key.sym).expect("found above");
        if is_attempt_list(value) {
            continue;
        }
        let parsed =
            if winner == NANOS_FORM { integer_nanos(value) } else { crate::numeric(value) };
        // `None` inside: the winner is `_s` and already an `F64` -- nothing to write, but the
        // losers still go.
        let seconds = match (value, parsed) {
            (Value::F64(f), _) if winner == 0 && f.is_finite() => None,
            (_, Some(f)) => Some(f / divisor),
            (_, None) => {
                count(telemetry, INVALID, key.name);
                diag.warn_throttled(
                    "bad_duration",
                    "http_access: a duration attribute is not a number in its named unit; left in place",
                );
                continue;
            }
        };
        let (target, _) = forms[0];
        if let Some(seconds) = seconds {
            attrs.insert_sym(target.sym, Value::F64(seconds));
            count(telemetry, NORMALIZED, target.name);
        }
        for (loser, _) in &forms[1..] {
            attrs.remove_sym(loser.sym);
        }
    }
}

// -- Step 4: method -----------------------------------------------------------------------------

/// Step 4: a method outside semconv's known set becomes `_OTHER`, the raw value preserved as
/// `http.request.method_original` -- the one pre-normalization value kept, because semconv itself
/// asks for it. Returns the method's `span.name` row, or `None` with no method at all.
fn normalize_method(
    attrs: &mut AttrMap,
    keys: &Keys,
    other: &Value,
    telemetry: &Telemetry,
) -> Option<usize> {
    let value = present(attrs, keys.method.sym)?;
    if let Value::Str(bytes) = value {
        if let Some(row) = KNOWN_METHODS.iter().position(|m| m.as_bytes() == bytes.as_ref()) {
            return Some(row);
        }
        if bytes.as_ref() == OTHER_METHOD.as_bytes() {
            return Some(KNOWN_METHODS.len());
        }
    }
    let original = value.clone();
    attrs.insert_sym(keys.method_original.sym, original);
    attrs.insert_sym(keys.method.sym, other.clone());
    count(telemetry, NORMALIZED, keys.method.name);
    count(telemetry, DERIVED, keys.method_original.name);
    Some(KNOWN_METHODS.len())
}

// -- Step 5: protocol version -------------------------------------------------------------------

/// Step 5: `HTTP/1.1` to `1.1` (a zero-copy slice), and semconv's `2`/`3` for `2.0`/`3.0`.
/// Anything else is left exactly as it arrived.
fn normalize_version(attrs: &mut AttrMap, keys: &Keys, telemetry: &Telemetry) {
    let Some(Value::Str(bytes)) = present(attrs, keys.protocol_version.sym) else { return };
    let stripped = bytes.starts_with(b"HTTP/").then(|| bytes.slice(5..));
    let version = stripped.as_ref().unwrap_or(bytes);
    let normalized = match version.as_ref() {
        b"2.0" => Some(Bytes::from_static(b"2")),
        b"3.0" => Some(Bytes::from_static(b"3")),
        _ => stripped,
    };
    if let Some(version) = normalized {
        attrs.insert_sym(keys.protocol_version.sym, Value::Str(version));
        count(telemetry, NORMALIZED, keys.protocol_version.name);
    }
}

// -- Step 6: query ------------------------------------------------------------------------------

/// Rewrites `query` into `scratch` with every sensitive key's value replaced by `REDACTED`,
/// returning how many values were replaced -- `0` means `scratch` is untouched and nothing needs
/// writing back. Keys match ASCII-case-insensitively; an empty value, or one already `REDACTED`,
/// is left alone, which is what makes a second pass a no-op. Only ASCII delimiters are ever cut
/// at and only ASCII is written, so a `Value::Str` stays valid UTF-8.
fn redact_query(query: &[u8], sensitive: &[String], scratch: &mut Vec<u8>) -> usize {
    let is_sensitive = |pair: &[u8]| -> Option<usize> {
        let eq = pair.iter().position(|&b| b == b'=')?;
        let (key, value) = (&pair[..eq], &pair[eq + 1..]);
        let hit = !value.is_empty()
            && value != REDACTED
            && sensitive.iter().any(|s| s.as_bytes().eq_ignore_ascii_case(key));
        hit.then_some(eq)
    };
    if !query.split(|&b| b == b'&').any(|pair| is_sensitive(pair).is_some()) {
        return 0;
    }
    scratch.clear();
    let mut redacted = 0;
    for (index, pair) in query.split(|&b| b == b'&').enumerate() {
        if index > 0 {
            scratch.push(b'&');
        }
        match is_sensitive(pair) {
            Some(eq) => {
                scratch.extend_from_slice(&pair[..=eq]);
                scratch.extend_from_slice(REDACTED);
                redacted += 1;
            }
            None => scratch.extend_from_slice(pair),
        }
    }
    redacted
}

/// Step 6: a leading `?` stripped (a zero-copy slice -- nginx's `$is_args$args` logs one), then
/// semconv's sensitive values redacted. Before step 7's cap on purpose: a secret must not survive
/// by being cut mid-value, where the key would no longer be followed by a complete value to find.
fn normalize_query(
    attrs: &mut AttrMap,
    keys: &Keys,
    sensitive: &[String],
    scratch: &mut Vec<u8>,
    telemetry: &Telemetry,
) {
    let Some(Value::Str(bytes)) = attrs.get_sym(keys.url_query.sym) else { return };
    let stripped = bytes.first() == Some(&b'?');
    let mut query = if stripped { bytes.slice(1..) } else { bytes.clone() };
    let redacted = redact_query(&query, sensitive, scratch);
    if redacted > 0 {
        query = Bytes::copy_from_slice(scratch);
        telemetry.count(REDACTED_METRIC, redacted as f64, &[]);
    }
    if stripped || redacted > 0 {
        attrs.insert_sym(keys.url_query.sym, Value::Str(query));
    }
    if stripped {
        count(telemetry, NORMALIZED, keys.url_query.name);
    }
}

// -- Step 7: cap and clean ----------------------------------------------------------------------

/// Step 7: every capped field to at most its limit in characters (a `Bytes::slice` at a char
/// boundary, so `Value::Str` stays valid UTF-8), then every control byte (`< 0x20`, `0x7F`) in
/// what's left becomes `_`. Bytewise and length-preserving, `keep_values::lower`'s reasoning: a
/// control byte is never part of a multi-byte UTF-8 sequence, so replacing it can't break
/// validity, and the clean allocates only when a byte actually changed. A `Value::Bytes` (a
/// non-UTF-8 value) is capped in bytes instead, since it has no characters to count.
fn cap_and_clean(
    attrs: &mut AttrMap,
    caps: &[(Key, usize)],
    scratch: &mut Vec<u8>,
    telemetry: &Telemetry,
) {
    for (key, cap) in caps {
        let (value, bytes) = match attrs.get_sym(key.sym) {
            Some(value @ (Value::Str(bytes) | Value::Bytes(bytes))) => (value, bytes),
            _ => continue,
        };
        // A value no longer than `cap` bytes can't be longer than `cap` characters, so the common
        // case never walks (or validates) anything.
        let cut = if bytes.len() <= *cap {
            None
        } else {
            match value.as_str() {
                Some(s) => s.char_indices().nth(*cap).map(|(offset, _)| offset),
                None => Some(*cap),
            }
        };
        let end = cut.unwrap_or(bytes.len());
        let dirty = bytes[..end].iter().any(|&b| b < 0x20 || b == 0x7F);
        if cut.is_none() && !dirty {
            continue;
        }
        let rewritten = if dirty {
            scratch.clear();
            scratch
                .extend(bytes[..end].iter().map(|&b| if b < 0x20 || b == 0x7F { b'_' } else { b }));
            Bytes::copy_from_slice(scratch)
        } else {
            bytes.slice(..end)
        };
        let rewritten = match value {
            Value::Str(_) => Value::Str(rewritten),
            _ => Value::Bytes(rewritten),
        };
        attrs.insert_sym(key.sym, rewritten);
        if cut.is_some() {
            count(telemetry, TRUNCATED, key.name);
        }
        if dirty {
            count(telemetry, CLEANED, key.name);
        }
    }
}

// -- Step 8: classify ---------------------------------------------------------------------------

/// Step 8, user-agent half: `user_agent.class` from the *uncapped* value, which is why `process`
/// runs this before step 7 -- the identifying token of a spoofed UA is often at its tail. Absent
/// writes nothing (an absent header is silence); present but blank is `none`; a `Value::Bytes`
/// UA (not UTF-8, so no regex can read it) is `other`.
///
/// An existing `user_agent.class` (by [`present`]'s rule) is trusted and never recomputed. The
/// first pass is the only one that ever sees the uncapped, uncleaned value -- step 7 then caps
/// and cleans it in place -- so its verdict is the one to keep: re-classifying on a second pass
/// would read the rewritten value and could flip the class (a tail token cut off by the cap, a
/// control byte cleaned to `_`) while leaving the first pass's `user_agent.synthetic.type`
/// behind. The same rule makes a class written upstream -- by a `set` or Lua stage, or logged by
/// the producer itself -- an operator override of the table: it wins, and neither the class nor
/// `user_agent.synthetic.type` is derived for that event.
fn classify_user_agent(
    attrs: &mut AttrMap,
    keys: &Keys,
    classifier: &Classifier,
    telemetry: &Telemetry,
) {
    if present(attrs, keys.ua_class.sym).is_some() {
        return;
    }
    let (class, bot) = match attrs.get_sym(keys.user_agent.sym) {
        None | Some(Value::Null) => return,
        Some(Value::Str(bytes)) if is_blank(bytes) => (&classifier.none, false),
        Some(value @ Value::Str(_)) => classifier.classify(value.as_str().expect("a Str")),
        Some(_) => (&classifier.other, false),
    };
    attrs.insert_sym(keys.ua_class.sym, class.clone());
    count(telemetry, DERIVED, keys.ua_class.name);
    if bot {
        attrs.insert_sym(keys.ua_synthetic.sym, classifier.bot.clone());
        count(telemetry, DERIVED, keys.ua_synthetic.name);
    }
}

/// Step 8, route half: `http.route` from the *capped* `url.path` -- first matching rule, else
/// `route_other`, else nothing. Returns the `span.name` column: the rule's index,
/// `rules.len()` for `route_other`, `None` for no route. Counted `routed{outcome}` once per event
/// with a path; the tag is the outcome, never the route value, which is operator-declared and
/// unbounded in number.
fn classify_route(
    attrs: &mut AttrMap,
    keys: &Keys,
    router: &Router,
    telemetry: &Telemetry,
) -> Option<usize> {
    let path = present(attrs, keys.url_path.sym)?;
    let hit = path
        .as_str()
        .and_then(|path| router.rules.iter().position(|(_, pattern, _)| pattern.is_match(path)));
    let (route, column, outcome) = match (hit, &router.other) {
        (Some(index), _) => {
            let (route, _, origin) = &router.rules[index];
            (route, index, origin.outcome())
        }
        (None, Some(other)) => (other, router.rules.len(), "other"),
        (None, None) => {
            telemetry.count(ROUTED, 1.0, &[("outcome", "none")]);
            return None;
        }
    };
    attrs.insert_sym(keys.route.sym, route.clone());
    telemetry.count(ROUTED, 1.0, &[("outcome", outcome)]);
    count(telemetry, DERIVED, keys.route.name);
    Some(column)
}

// -- Step 9: derive -----------------------------------------------------------------------------

/// The first comma-separated hop of an `X-Forwarded-For` value, ASCII-trimmed, as a slice.
fn first_hop(xff: &Bytes) -> Bytes {
    let end = xff.iter().position(|&b| b == b',').unwrap_or(xff.len());
    let start = xff[..end].iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(end);
    let end = xff[..end].iter().rposition(|b| !b.is_ascii_whitespace()).map_or(start, |i| i + 1);
    xff.slice(start..end)
}

impl HttpAccess {
    /// Step 9: `error.type` (the status, 5xx only), `span.status` (`error` for 5xx or `0`, else
    /// `unset` -- never `ok`, which semconv reserves for an explicit override), `span.name`
    /// (`{method} {route}`, or the method alone), the `span.duration_s` mirror, and -- only under
    /// `forwarded: {trust: true}`, since the header is client-supplied -- `client.address` from
    /// the first XFF hop. Every value comes from a pre-built cell, a constant, or a slice.
    fn derive(&mut self, attrs: &mut AttrMap, method: Option<usize>, route: Option<usize>) {
        let keys = &self.keys;
        let telemetry = &self.telemetry;
        if let Some(&Value::I64(status)) = attrs.get_sym(keys.status.sym) {
            let server_error = (500..=599).contains(&status);
            if server_error {
                attrs.insert_sym(keys.error_type.sym, self.error_types.get(status));
                count(telemetry, DERIVED, keys.error_type.name);
            }
            let span_status =
                if server_error || status == 0 { &self.span_error } else { &self.span_unset };
            attrs.insert_sym(keys.span_status.sym, span_status.clone());
            count(telemetry, DERIVED, keys.span_status.name);
        }
        if let Some(method) = method {
            attrs.insert_sym(keys.span_name.sym, self.span_names.get(&self.router, method, route));
            count(telemetry, DERIVED, keys.span_name.name);
        }
        let (duration_s, _) = keys.durations[0][0];
        if let Some(duration @ Value::F64(_)) = attrs.get_sym(duration_s.sym) {
            if !keys.span_timing.iter().any(|key| present(attrs, key.sym).is_some()) {
                let duration = duration.clone();
                attrs.insert_sym(keys.span_duration_s.sym, duration);
                count(telemetry, DERIVED, keys.span_duration_s.name);
            }
        }
        if self.trust_forwarded {
            if let Some(Value::Str(xff)) = present(attrs, keys.xff.sym) {
                let hop = first_hop(xff);
                if !hop.is_empty() {
                    attrs.insert_sym(keys.client_address.sym, Value::Str(hop));
                    count(telemetry, DERIVED, keys.client_address.name);
                }
            }
        }
    }
}

impl Transform for HttpAccess {
    /// Runs the plan's steps in order, each best-effort and independent of whether any other
    /// succeeded. An event with no log passes through untouched: an access line is always a log
    /// record, and a metric or span event carrying `server.port`-shaped attributes is not this
    /// component's to rewrite. Never touches `event.log`/`metrics`/`span`/`timestamp` or the
    /// batch `Resource`; always returns `true`.
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        if event.log.is_none() || event.attributes.is_empty() {
            return true;
        }
        let attrs = &mut event.attributes;
        dealias(attrs, &self.aliases, &self.telemetry);
        decompose(attrs, &self.keys, &self.telemetry, &mut self.diag);
        coerce_numerics(attrs, &self.keys, &self.telemetry, &mut self.diag);
        convert_durations(attrs, &self.keys, &self.telemetry, &mut self.diag);
        let method = normalize_method(attrs, &self.keys, &self.other_method, &self.telemetry);
        normalize_version(attrs, &self.keys, &self.telemetry);
        normalize_query(attrs, &self.keys, &self.redact, &mut self.scratch, &self.telemetry);
        // Step 8's user-agent half runs here, ahead of step 7, because it classifies the
        // uncapped value; the route half below classifies the capped path, as the plan says.
        classify_user_agent(attrs, &self.keys, &self.ua, &self.telemetry);
        cap_and_clean(attrs, &self.caps, &mut self.scratch, &self.telemetry);
        let route = classify_route(attrs, &self.keys, &self.router, &self.telemetry);
        self.derive(attrs, method, route);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::{
        BodyFormat, LogRecord, MetricKind, MetricRecord, Registry, SpanKind, SpanRecord, SpanStatus,
    };

    /// A handful of `logit_config::CAPPED_FIELDS`' defaults -- this crate can't see that list
    /// (`logit-cli` resolves it), so the tests carry the subset they exercise.
    fn test_caps() -> Vec<(String, usize)> {
        [
            ("url.path", 256),
            ("url.query", 256),
            ("user_agent.original", 256),
            ("http.request.header.referer", 256),
            ("client.address", 128),
            ("http.request.header.x-forwarded-for", 128),
            ("user.name", 128),
            ("http.request.method_original", 32),
            ("network.protocol.version", 8),
        ]
        .into_iter()
        .map(|(field, cap)| (field.to_string(), cap))
        .collect()
    }

    fn config() -> HttpAccessConfig {
        HttpAccessConfig { max_length: test_caps(), ..HttpAccessConfig::default() }
    }

    fn http(config: HttpAccessConfig) -> HttpAccess {
        HttpAccess::new(config).expect("valid patterns")
    }

    fn bare() -> HttpAccess {
        http(config())
    }

    fn log_event(pairs: &[(&str, Value)]) -> Event {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, v.clone());
        }
        Event::log(
            1_000,
            attrs,
            LogRecord {
                message: Value::str("GET / HTTP/1.1"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn run(t: &mut HttpAccess, pairs: &[(&str, Value)]) -> Event {
        let mut event = log_event(pairs);
        assert!(t.process(&Arc::new(Resource::default()), &mut event), "never drops");
        event
    }

    fn s(v: &str) -> Value {
        Value::str(v)
    }

    fn get<'a>(event: &'a Event, key: &str) -> Option<&'a Value> {
        event.attributes.get(key)
    }

    fn instrumented(config: HttpAccessConfig) -> (HttpAccess, Arc<Registry>, Diagnostics) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("http", "http_access", "transform");
        let diag = Diagnostics::new("http");
        let t = http(config).with_telemetry(telemetry).with_diagnostics(diag.clone());
        (t, registry, diag)
    }

    /// Sums one counter across a single drain, optionally filtered to one tag. Callers drain
    /// once -- `Registry::drain` empties the buffer.
    fn counter(events: &[Event], name: &str, tag: Option<(&str, &str)>) -> f64 {
        events
            .iter()
            .flat_map(|e| e.metrics.iter().map(move |m| (e, m)))
            .filter(|(e, m)| {
                resolve(m.name) == name
                    && tag.is_none_or(|(k, want)| {
                        e.attributes.get(k).and_then(Value::as_str) == Some(want)
                    })
            })
            .map(|(_, m)| match &m.kind {
                MetricKind::Sum(sum) => sum.value,
                other => panic!("{name} is not a counter: {other:?}"),
            })
            .sum()
    }

    // -- Step 1: composites -----------------------------------------------------------------------

    #[test]
    fn a_request_line_decomposes_into_its_absent_atomic_fields_and_is_removed() {
        let event = run(&mut bare(), &[("http.request.line", s("GET /p/q?a=1&b=2 HTTP/1.1"))]);
        assert_eq!(get(&event, "http.request.method"), Some(&s("GET")));
        assert_eq!(get(&event, "url.path"), Some(&s("/p/q")));
        assert_eq!(get(&event, "url.query"), Some(&s("a=1&b=2")));
        assert_eq!(get(&event, "network.protocol.version"), Some(&s("1.1")), "then step 5");
        assert_eq!(get(&event, "http.request.line"), None);
    }

    #[test]
    fn a_composite_never_overrides_an_atomic_field_the_producer_logged() {
        let event = run(
            &mut bare(),
            &[("http.request.line", s("GET /from-line HTTP/1.1")), ("url.path", s("/atomic"))],
        );
        assert_eq!(get(&event, "url.path"), Some(&s("/atomic")));
        assert_eq!(get(&event, "url.query"), None, "no query in the line, none invented");
    }

    #[test]
    fn url_original_splits_at_the_first_question_mark_and_is_removed() {
        let event = run(&mut bare(), &[("url.original", s("/search?q=a?b"))]);
        assert_eq!(get(&event, "url.path"), Some(&s("/search")));
        assert_eq!(get(&event, "url.query"), Some(&s("q=a?b")));
        assert_eq!(get(&event, "url.original"), None);
    }

    #[test]
    fn a_malformed_request_line_is_left_in_place_counted_and_diagnosed() {
        let (mut t, registry, diag) = instrumented(config());
        for line in ["GET /only-two", "GET  /double HTTP/1.1", "garbage", " / HTTP/1.1"] {
            let event = run(&mut t, &[("http.request.line", s(line))]);
            assert_eq!(get(&event, "http.request.line"), Some(&s(line)), "{line:?}");
            assert_eq!(get(&event, "url.path"), None, "{line:?}");
        }
        let events = registry.drain(0);
        assert_eq!(counter(&events, INVALID, Some(("field", "http.request.line"))), 4.0);
        assert_eq!(diag.occurrences("bad_request_line"), 4);
    }

    // -- Step 2: numerics -------------------------------------------------------------------------

    #[test]
    fn numerics_coerce_to_i64_from_every_accepted_representation() {
        let event = run(
            &mut bare(),
            &[
                ("http.response.status_code", s("200")),
                ("http.response.body.size", Value::U64(512)),
                ("server.port", Value::F64(443.0)),
                ("network.connection.requests", Value::I64(3)),
                ("http.response.compression_ratio", s("2.50")),
            ],
        );
        assert_eq!(get(&event, "http.response.status_code"), Some(&Value::I64(200)));
        assert_eq!(get(&event, "http.response.body.size"), Some(&Value::I64(512)));
        assert_eq!(get(&event, "server.port"), Some(&Value::I64(443)));
        assert_eq!(get(&event, "network.connection.requests"), Some(&Value::I64(3)));
        assert_eq!(get(&event, "http.response.compression_ratio"), Some(&Value::F64(2.5)));
    }

    #[test]
    fn nginx_000_status_becomes_zero() {
        let event = run(&mut bare(), &[("http.response.status_code", s("000"))]);
        assert_eq!(get(&event, "http.response.status_code"), Some(&Value::I64(0)));
    }

    #[test]
    fn an_unparseable_status_is_left_in_place_counted_and_diagnosed() {
        let (mut t, registry, diag) = instrumented(config());
        let event = run(&mut t, &[("http.response.status_code", s("OK")), ("server.port", s("x"))]);
        assert_eq!(get(&event, "http.response.status_code"), Some(&s("OK")));
        assert_eq!(get(&event, "server.port"), Some(&s("x")));
        assert_eq!(get(&event, "span.status"), None, "no status, no span.status");
        let events = registry.drain(0);
        assert_eq!(counter(&events, INVALID, Some(("field", STATUS))), 1.0);
        assert_eq!(counter(&events, INVALID, Some(("field", "server.port"))), 1.0);
        assert_eq!(diag.occurrences("bad_status"), 1);
        assert_eq!(diag.occurrences("bad_duration"), 0, "a port is not a diagnosed field");
    }

    #[test]
    fn an_upstream_attempt_list_is_left_verbatim_and_never_invalid() {
        let (mut t, registry, _) = instrumented(config());
        let event = run(
            &mut t,
            &[("upstream.status", s("502, 200")), ("upstream.duration_s", s("0.010, 0.020"))],
        );
        assert_eq!(get(&event, "upstream.status"), Some(&s("502, 200")));
        assert_eq!(get(&event, "upstream.duration_s"), Some(&s("0.010, 0.020")));
        assert_eq!(counter(&registry.drain(0), INVALID, None), 0.0);

        let event = run(&mut bare(), &[("upstream.status", s("504"))]);
        assert_eq!(get(&event, "upstream.status"), Some(&Value::I64(504)), "a single value is");
    }

    #[test]
    fn blank_values_are_absent_not_invalid() {
        let (mut t, registry, _) = instrumented(config());
        let event = run(
            &mut t,
            &[("server.port", s("")), ("upstream.status", s("-")), ("client.port", Value::Null)],
        );
        assert_eq!(get(&event, "server.port"), Some(&s("")), "left exactly as it arrived");
        assert_eq!(get(&event, "upstream.status"), Some(&s("-")));
        assert_eq!(counter(&registry.drain(0), INVALID, None), 0.0);
    }

    // -- Step 3: durations ------------------------------------------------------------------------

    #[test]
    fn suffixed_durations_become_seconds_and_the_source_is_removed() {
        let event = run(
            &mut bare(),
            &[
                ("http.request.duration_ms", Value::I64(125)),
                ("upstream.connect_us", s("2500")),
                ("upstream.header_ms", Value::F64(40.0)),
            ],
        );
        assert_eq!(get(&event, "http.request.duration_s"), Some(&Value::F64(0.125)));
        assert_eq!(get(&event, "http.request.duration_ms"), None);
        assert_eq!(get(&event, "upstream.connect_s"), Some(&Value::F64(0.0025)));
        assert_eq!(get(&event, "upstream.connect_us"), None);
        assert_eq!(get(&event, "upstream.header_s"), Some(&Value::F64(0.04)));
    }

    #[test]
    fn an_unsuffixed_duration_is_integer_nanoseconds() {
        let event = run(
            &mut bare(),
            &[
                ("http.request.duration", Value::I64(1_500_000_000)),
                ("upstream.duration", s("250000000")),
                ("upstream.header", Value::U64(40_000_000)),
            ],
        );
        assert_eq!(get(&event, "http.request.duration_s"), Some(&Value::F64(1.5)));
        assert_eq!(get(&event, "http.request.duration"), None, "the source is removed");
        assert_eq!(get(&event, "upstream.duration_s"), Some(&Value::F64(0.25)));
        assert_eq!(get(&event, "upstream.header_s"), Some(&Value::F64(0.04)));

        let event = run(
            &mut bare(),
            &[("http.request.duration_s", Value::F64(0.5)), ("http.request.duration", s("9"))],
        );
        assert_eq!(get(&event, "http.request.duration_s"), Some(&Value::F64(0.5)), "_s wins");
        assert_eq!(get(&event, "http.request.duration"), None);
    }

    #[test]
    fn a_float_in_the_unsuffixed_nanosecond_form_is_invalid() {
        let (mut t, registry, diag) = instrumented(config());
        for value in [Value::F64(1.5e9), s("1.5")] {
            let event = run(&mut t, &[("upstream.connect", value.clone())]);
            assert_eq!(get(&event, "upstream.connect"), Some(&value), "left in place");
            assert_eq!(get(&event, "upstream.connect_s"), None);
        }
        let events = registry.drain(0);
        assert_eq!(counter(&events, INVALID, Some(("field", "upstream.connect"))), 2.0);
        assert_eq!(diag.occurrences("bad_duration"), 2);
    }

    #[test]
    fn a_quoted_seconds_value_becomes_f64() {
        let event = run(&mut bare(), &[("upstream.duration_s", s("0.004"))]);
        assert_eq!(get(&event, "upstream.duration_s"), Some(&Value::F64(0.004)));
    }

    #[test]
    fn the_seconds_form_wins_and_the_others_are_removed() {
        let event = run(
            &mut bare(),
            &[
                ("http.request.duration_s", Value::F64(0.5)),
                ("http.request.duration_ms", Value::I64(999)),
                ("http.request.duration_us", Value::I64(1)),
            ],
        );
        assert_eq!(get(&event, "http.request.duration_s"), Some(&Value::F64(0.5)));
        assert_eq!(get(&event, "http.request.duration_ms"), None);
        assert_eq!(get(&event, "http.request.duration_us"), None);
    }

    #[test]
    fn an_unparseable_duration_is_left_in_place_counted_and_diagnosed() {
        let (mut t, registry, diag) = instrumented(config());
        let event = run(&mut t, &[("http.request.duration_ms", s("fast"))]);
        assert_eq!(get(&event, "http.request.duration_ms"), Some(&s("fast")));
        assert_eq!(get(&event, "http.request.duration_s"), None);
        let events = registry.drain(0);
        assert_eq!(counter(&events, INVALID, Some(("field", "http.request.duration_ms"))), 1.0);
        assert_eq!(diag.occurrences("bad_duration"), 1);
    }

    // -- Step 4: method ---------------------------------------------------------------------------

    #[test]
    fn an_unknown_method_becomes_other_with_the_original_preserved() {
        for raw in ["PROPFIND", "get"] {
            let event = run(&mut bare(), &[("http.request.method", s(raw))]);
            assert_eq!(get(&event, "http.request.method"), Some(&s("_OTHER")), "{raw}");
            assert_eq!(get(&event, "http.request.method_original"), Some(&s(raw)), "{raw}");
        }
    }

    #[test]
    fn a_known_method_is_untouched() {
        let event = run(&mut bare(), &[("http.request.method", s("QUERY"))]);
        assert_eq!(get(&event, "http.request.method"), Some(&s("QUERY")));
        assert_eq!(get(&event, "http.request.method_original"), None);
    }

    // -- Step 5: version --------------------------------------------------------------------------

    #[test]
    fn the_protocol_version_loses_its_prefix_and_two_and_three_lose_their_point_zero() {
        for (raw, want) in [
            ("HTTP/1.1", "1.1"),
            ("HTTP/1.0", "1.0"),
            ("HTTP/2.0", "2"),
            ("2.0", "2"),
            ("HTTP/3.0", "3"),
            ("2", "2"),
            ("SPDY/3", "SPDY/3"),
        ] {
            let event = run(&mut bare(), &[("network.protocol.version", s(raw))]);
            assert_eq!(get(&event, "network.protocol.version"), Some(&s(want)), "{raw}");
        }
    }

    // -- Step 6: query ----------------------------------------------------------------------------

    #[test]
    fn a_leading_question_mark_is_stripped() {
        let event = run(&mut bare(), &[("url.query", s("?a=1"))]);
        assert_eq!(get(&event, "url.query"), Some(&s("a=1")));
    }

    #[test]
    fn every_sensitive_key_is_redacted_case_insensitively() {
        for key in SENSITIVE_QUERY_KEYS {
            for spelling in [key.to_string(), key.to_ascii_lowercase(), key.to_ascii_uppercase()] {
                let query = format!("a=1&{spelling}=s3cr3t&b=2");
                let event = run(&mut bare(), &[("url.query", s(&query))]);
                assert_eq!(
                    get(&event, "url.query"),
                    Some(&s(&format!("a=1&{spelling}=REDACTED&b=2"))),
                    "{spelling}"
                );
            }
        }
    }

    #[test]
    fn a_configured_redact_key_is_redacted_and_others_are_not() {
        let mut t = http(HttpAccessConfig { redact_query: vec!["token".to_string()], ..config() });
        let event = run(&mut t, &[("url.query", s("?Token=abc&tokenish=def&sig="))]);
        assert_eq!(
            get(&event, "url.query"),
            Some(&s("Token=REDACTED&tokenish=def&sig=")),
            "an empty value has nothing to redact"
        );
    }

    #[test]
    fn redaction_happens_before_capping() {
        // The secret straddles the 256-char cap: capped first, the key would survive with part of
        // its value; redacted first, the value is gone before the cap ever sees it.
        let query = format!("{}&X-Amz-Signature={}", "a".repeat(230), "f".repeat(64));
        let event = run(&mut bare(), &[("url.query", s(&query))]);
        let got = get(&event, "url.query").and_then(Value::as_str).unwrap();
        assert!(!got.contains('f'), "no byte of the secret may survive: {got}");
        assert!(got.ends_with("X-Amz-Signature=REDACTED"), "{got}");
    }

    // -- Step 7: cap and clean --------------------------------------------------------------------

    #[test]
    fn a_cap_cuts_at_a_char_boundary_counting_characters_not_bytes() {
        let path = format!("/{}", "é".repeat(300));
        let event = run(&mut bare(), &[("url.path", s(&path))]);
        let got = get(&event, "url.path").and_then(Value::as_str).expect("still a valid Str");
        assert_eq!(got.chars().count(), 256);
        assert_eq!(got.len(), 1 + 255 * 2, "'/' then 255 two-byte chars");
    }

    #[test]
    fn a_value_at_the_cap_is_untouched() {
        let path = "é".repeat(256);
        let event = run(&mut bare(), &[("url.path", s(&path))]);
        assert_eq!(get(&event, "url.path"), Some(&s(&path)));
    }

    #[test]
    fn a_max_length_override_applies() {
        let mut t = http(HttpAccessConfig {
            max_length: vec![("user.name".to_string(), 4)],
            ..HttpAccessConfig::default()
        });
        let event = run(&mut t, &[("user.name", s("alexandra"))]);
        assert_eq!(get(&event, "user.name"), Some(&s("alex")));
    }

    #[test]
    fn control_bytes_become_underscores_at_unchanged_length() {
        let ua = "curl/8.0\u{1}\t\u{7f}é";
        let event = run(&mut bare(), &[("user_agent.original", s(ua))]);
        let got = get(&event, "user_agent.original").and_then(Value::as_str).unwrap();
        assert_eq!(got, "curl/8.0___é");
        assert_eq!(got.len(), ua.len());
    }

    #[test]
    fn a_non_utf8_bytes_value_is_capped_in_bytes_and_cleaned() {
        let mut t = http(HttpAccessConfig {
            max_length: vec![("user.name".to_string(), 3)],
            ..HttpAccessConfig::default()
        });
        let raw = Value::Bytes(Bytes::from_static(b"\xff\n\xfeabc"));
        let event = run(&mut t, &[("user.name", raw)]);
        assert_eq!(get(&event, "user.name"), Some(&Value::Bytes(Bytes::from_static(b"\xff_\xfe"))));
    }

    // -- Step 8: classify -------------------------------------------------------------------------

    #[test]
    fn an_absent_user_agent_writes_nothing_and_a_blank_one_is_none() {
        let event = run(&mut bare(), &[("url.path", s("/"))]);
        assert_eq!(get(&event, "user_agent.class"), None, "absent is silence");
        for blank in ["", "-"] {
            let event = run(&mut bare(), &[("user_agent.original", s(blank))]);
            assert_eq!(get(&event, "user_agent.class"), Some(&s("none")), "{blank:?}");
        }
    }

    #[test]
    fn the_built_in_table_classifies_in_priority_order() {
        for (ua, class, bot) in [
            ("curl/8.4.0", "tool", false),
            ("Mozilla/5.0 (X11; Linux x86_64) Firefox/120.0", "browser", false),
            (
                "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
                "crawler",
                true,
            ),
            ("Mozilla/5.0 zgrab/0.x", "scanner", true),
            ("kube-probe/1.29", "tool", false),
            ("something unheard of", "other", false),
            ("Mozilla/5.0 (Linux; Android 10; CUBOT X30) Safari/537.36", "browser", false),
        ] {
            let event = run(&mut bare(), &[("user_agent.original", s(ua))]);
            assert_eq!(get(&event, "user_agent.class"), Some(&s(class)), "{ua}");
            let synthetic = bot.then(|| s("bot"));
            assert_eq!(get(&event, "user_agent.synthetic.type"), synthetic.as_ref(), "{ua}");
        }
    }

    #[test]
    fn a_configured_user_agent_rule_beats_the_built_in_table() {
        let mut t = http(HttpAccessConfig {
            user_agent_rules: vec![UaRule {
                pattern: "(?i)curl/8".to_string(),
                class: "internal".to_string(),
            }],
            ..config()
        });
        let event = run(&mut t, &[("user_agent.original", s("curl/8.4.0"))]);
        assert_eq!(get(&event, "user_agent.class"), Some(&s("internal")));
        let event = run(&mut t, &[("user_agent.original", s("curl/7.0"))]);
        assert_eq!(get(&event, "user_agent.class"), Some(&s("tool")));
    }

    #[test]
    fn the_user_agent_is_classified_on_its_uncapped_value() {
        let ua = format!("{} Googlebot/2.1", "x".repeat(400));
        let event = run(&mut bare(), &[("user_agent.original", s(&ua))]);
        assert_eq!(get(&event, "user_agent.class"), Some(&s("crawler")));
        let capped = get(&event, "user_agent.original").and_then(Value::as_str).unwrap();
        assert_eq!(capped.len(), 256, "and still capped afterwards");
    }

    fn routed(routes: Vec<RouteRule>, other: Option<&str>) -> HttpAccess {
        http(HttpAccessConfig { routes, route_other: other.map(str::to_string), ..config() })
    }

    fn pattern(pattern: &str, route: &str) -> RouteRule {
        RouteRule::Pattern { pattern: pattern.to_string(), route: route.to_string() }
    }

    #[test]
    fn routes_are_first_match_in_config_order() {
        let mut t = routed(
            vec![
                pattern("^/api/", "/api/{rest}"),
                pattern("^/api/users$", "/api/users"),
                RouteRule::Builtin(RouteSet::Probes),
            ],
            None,
        );
        let event = run(&mut t, &[("url.path", s("/api/users"))]);
        assert_eq!(get(&event, "http.route"), Some(&s("/api/{rest}")), "the earlier rule wins");
        let event = run(&mut t, &[("url.path", s("/healthz"))]);
        assert_eq!(get(&event, "http.route"), Some(&s("/{probe}")));
    }

    #[test]
    fn a_config_rule_placed_before_a_builtin_beats_it_and_after_it_does_not() {
        let favicon = || pattern(r"^/favicon\.ico$", "/fav");
        let before = vec![favicon(), RouteRule::Builtin(RouteSet::WellKnown)];
        let event = run(&mut routed(before, None), &[("url.path", s("/favicon.ico"))]);
        assert_eq!(get(&event, "http.route"), Some(&s("/fav")));
        let after = vec![RouteRule::Builtin(RouteSet::WellKnown), favicon()];
        let event = run(&mut routed(after, None), &[("url.path", s("/favicon.ico"))]);
        assert_eq!(get(&event, "http.route"), Some(&s("/{well-known}")));
    }

    #[test]
    fn each_built_in_route_set_routes_its_own_members() {
        let mut t = routed(
            vec![
                RouteRule::Builtin(RouteSet::Assets),
                RouteRule::Builtin(RouteSet::WellKnown),
                RouteRule::Builtin(RouteSet::Probes),
            ],
            Some("/{other}"),
        );
        for (path, want) in [
            ("/static/app.min.js", "/{asset}"),
            ("/img/logo.PNG", "/{asset}"),
            ("/robots.txt", "/{well-known}"),
            ("/.well-known/acme-challenge/x", "/{well-known}"),
            ("/readyz", "/{probe}"),
            ("/nginx_status/", "/{probe}"),
            ("/orders.json", "/{other}"),
            ("/", "/{other}"),
        ] {
            let event = run(&mut t, &[("url.path", s(path))]);
            assert_eq!(get(&event, "http.route"), Some(&s(want)), "{path}");
        }
    }

    #[test]
    fn with_no_route_other_an_unmatched_path_gets_no_route_and_a_method_only_name() {
        let mut t = routed(vec![pattern("^/api/", "/api")], None);
        let event = run(&mut t, &[("url.path", s("/home")), ("http.request.method", s("GET"))]);
        assert_eq!(get(&event, "http.route"), None);
        assert_eq!(get(&event, "span.name"), Some(&s("GET")));
    }

    #[test]
    fn a_route_is_matched_against_the_capped_path() {
        // The suffix that would match sits past the 256-char cap.
        let mut t = routed(vec![pattern(r"/secret$", "/secret")], Some("/{other}"));
        let path = format!("/{}/secret", "p".repeat(300));
        let event = run(&mut t, &[("url.path", s(&path))]);
        assert_eq!(get(&event, "http.route"), Some(&s("/{other}")));
    }

    // -- Step 9: derive ---------------------------------------------------------------------------

    #[test]
    fn span_name_is_method_and_route_and_http_for_an_unknown_method() {
        let mut t = routed(vec![RouteRule::Builtin(RouteSet::Assets)], Some("/{other}"));
        let event = run(&mut t, &[("http.request.method", s("GET")), ("url.path", s("/a.css"))]);
        assert_eq!(get(&event, "span.name"), Some(&s("GET /{asset}")));
        let event = run(&mut t, &[("http.request.method", s("BREW")), ("url.path", s("/pot"))]);
        assert_eq!(get(&event, "span.name"), Some(&s("HTTP /{other}")));
        let event = run(&mut bare(), &[("http.request.method", s("BREW"))]);
        assert_eq!(get(&event, "span.name"), Some(&s("HTTP")));
        let event = run(&mut t, &[("url.path", s("/a.css"))]);
        assert_eq!(get(&event, "span.name"), None, "no method, no name");
    }

    #[test]
    fn span_status_is_error_for_5xx_and_0_and_otherwise_unset_never_ok() {
        for (status, want) in
            [(500, "error"), (0, "error"), (599, "error"), (200, "unset"), (404, "unset")]
        {
            let event = run(&mut bare(), &[("http.response.status_code", Value::I64(status))]);
            assert_eq!(get(&event, "span.status"), Some(&s(want)), "{status}");
        }
    }

    #[test]
    fn error_type_is_the_status_for_5xx_only() {
        let mut t = bare();
        for (status, want) in [(503, Some("503")), (500, Some("500")), (404, None), (0, None)] {
            let event = run(&mut t, &[("http.response.status_code", Value::I64(status))]);
            assert_eq!(get(&event, "error.type"), want.map(s).as_ref(), "{status}");
        }
        let event = run(&mut t, &[("http.response.status_code", s("503"))]);
        assert_eq!(get(&event, "error.type"), Some(&s("503")), "the cached cell again");
    }

    #[test]
    fn the_request_duration_is_mirrored_only_when_the_line_carries_no_span_timing() {
        let event = run(
            &mut bare(),
            &[("http.request.duration_s", Value::F64(0.25)), ("span.end_s", s("1700000000.5"))],
        );
        assert_eq!(get(&event, "span.duration_s"), Some(&Value::F64(0.25)), "a lone end: mirrored");

        for timing in ["span.start_us", "span.duration_ms", "span.duration_s"] {
            let event = run(
                &mut bare(),
                &[("http.request.duration_ms", Value::I64(250)), (timing, Value::I64(7))],
            );
            assert_eq!(
                get(&event, "span.duration_s"),
                (timing == "span.duration_s").then_some(&Value::I64(7)),
                "{timing} present: not mirrored"
            );
        }
    }

    #[test]
    fn xff_is_ignored_by_default_and_its_first_hop_wins_when_trusted() {
        let pairs =
            [("client.address", s("10.0.0.1")), (XFF, s("  203.0.113.7 , 10.0.0.9, 10.0.0.1"))];
        let event = run(&mut bare(), &pairs);
        assert_eq!(get(&event, "client.address"), Some(&s("10.0.0.1")));

        let mut trusted = http(HttpAccessConfig { trust_forwarded: true, ..config() });
        let event = run(&mut trusted, &pairs);
        assert_eq!(get(&event, "client.address"), Some(&s("203.0.113.7")));
        let event = run(&mut trusted, &[("client.address", s("10.0.0.1")), (XFF, s("-"))]);
        assert_eq!(get(&event, "client.address"), Some(&s("10.0.0.1")), "a blank XFF is absent");
    }

    // -- Step 0: aliases --------------------------------------------------------------------------

    /// Every name the alias table covers, as its dotted spelling.
    fn aliased_names() -> Vec<&'static str> {
        bare().aliases.iter().map(|(_, dotted)| dotted.name).collect()
    }

    #[test]
    fn the_alias_table_covers_every_canonical_dotted_name_and_the_trace_names() {
        let names = aliased_names();
        for name in canonical_names().filter(|name| name.contains('.')) {
            assert!(names.contains(&name), "{name} has no alias");
        }
        for name in ["trace.id", "span.parent_id", "span.start_rfc3339", "span.duration_us"] {
            assert!(names.contains(&name), "{name}");
        }
        assert!(!names.contains(&"traceparent"), "no dot, no alias");
        let mut deduped = names.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(deduped.len(), names.len(), "no duplicate pairs");
    }

    #[test]
    fn every_dashed_alias_round_trips_to_its_dotted_output_and_dotted_wins_when_both_present() {
        // One instance throughout: the component is stateless per event (its lazy cells only
        // memoize constants), and compiling the built-in tables per case is slow in debug.
        let mut t = bare();
        for name in aliased_names() {
            let dashed_key = name.replace('.', "-");
            for value in [s("7"), s("GET /x HTTP/1.1")] {
                let dotted = run(&mut t, &[(name, value.clone())]);
                let dashed = run(&mut t, &[(dashed_key.as_str(), value.clone())]);
                assert_eq!(dashed.attributes, dotted.attributes, "{name} = {value:?}");
                assert_eq!(get(&dashed, &dashed_key), None, "{dashed_key} removed");

                let both = run(&mut t, &[(name, value.clone()), (&dashed_key, s("loser"))]);
                assert_eq!(both.attributes, dotted.attributes, "{name}: dotted wins");
            }
        }
    }

    #[test]
    fn a_dashed_header_key_keeps_its_own_dashes_after_the_prefix() {
        let mut trusted = http(HttpAccessConfig { trust_forwarded: true, ..config() });
        let event =
            run(&mut trusted, &[("http-request-header-x-forwarded-for", s("198.51.100.2"))]);
        assert_eq!(get(&event, XFF), Some(&s("198.51.100.2")));
        assert_eq!(get(&event, "client.address"), Some(&s("198.51.100.2")));
    }

    #[test]
    fn an_unlisted_dashed_key_is_left_alone() {
        let event = run(&mut bare(), &[("haproxy-timer-queue_ms", Value::I64(3))]);
        assert_eq!(get(&event, "haproxy-timer-queue_ms"), Some(&Value::I64(3)));
    }

    // -- Whole-component contracts ----------------------------------------------------------------

    fn full_line() -> Vec<(&'static str, Value)> {
        vec![
            ("http.request.line", s("PROPFIND /dav/a.css?sig=abc HTTP/2.0")),
            ("http.response.status_code", s("502")),
            ("http.request.duration_ms", Value::I64(12)),
            ("user_agent.original", s("Mozilla/5.0 bingbot/2.0\u{1}")),
            (XFF, s("198.51.100.2, 10.0.0.1")),
            ("span.end_s", s("1700000000.123")),
            ("http-response-body-size", s("1024")),
            ("traceparent", s("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")),
        ]
    }

    fn full_config() -> HttpAccessConfig {
        HttpAccessConfig {
            routes: vec![RouteRule::Builtin(RouteSet::Assets)],
            route_other: Some("/{other}".to_string()),
            trust_forwarded: true,
            ..config()
        }
    }

    #[test]
    fn a_full_line_normalizes_end_to_end() {
        let event = run(&mut http(full_config()), &full_line());
        let want: &[(&str, Value)] = &[
            ("http.request.method", s("_OTHER")),
            ("http.request.method_original", s("PROPFIND")),
            ("url.path", s("/dav/a.css")),
            ("url.query", s("sig=REDACTED")),
            ("network.protocol.version", s("2")),
            ("http.response.status_code", Value::I64(502)),
            ("http.response.body.size", Value::I64(1024)),
            ("http.request.duration_s", Value::F64(0.012)),
            ("user_agent.original", s("Mozilla/5.0 bingbot/2.0_")),
            ("user_agent.class", s("crawler")),
            ("user_agent.synthetic.type", s("bot")),
            ("http.route", s("/{asset}")),
            ("error.type", s("502")),
            ("span.status", s("error")),
            ("span.name", s("HTTP /{asset}")),
            ("span.duration_s", Value::F64(0.012)),
            ("client.address", s("198.51.100.2")),
            ("span.end_s", s("1700000000.123")),
            (XFF, s("198.51.100.2, 10.0.0.1")),
            ("traceparent", s("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")),
        ];
        for (key, value) in want {
            assert_eq!(get(&event, key), Some(value), "{key}");
        }
        assert_eq!(event.attributes.len(), want.len(), "{:?}", event.attributes);
    }

    #[test]
    fn processing_is_idempotent() {
        let mut t = http(full_config());
        let once = run(&mut t, &full_line());
        let mut twice = once.clone();
        assert!(t.process(&Arc::new(Resource::default()), &mut twice));
        assert_eq!(twice, once);
    }

    /// The UA is classified before step 7 caps and cleans it, so a second pass sees a rewritten
    /// value. These two shapes classify differently before and after the rewrite; the existing
    /// class is trusted, so the second pass changes neither it nor `user_agent.synthetic.type`.
    #[test]
    fn a_second_pass_keeps_the_first_user_agent_verdict() {
        let long = format!("Mozilla/5.0 {} sqlmap/1.7", "x".repeat(300));
        for (ua, class) in [
            // The tab is the `\b` boundary `\bbot\b` needs; cleaned to `_` it is a word byte.
            ("Foo\tbot", "crawler"),
            // The identifying token sits past the 256-char cap; capped, only `Mozilla/` is left.
            (long.as_str(), "scanner"),
        ] {
            let mut t = bare();
            let once = run(&mut t, &[("user_agent.original", s(ua))]);
            assert_eq!(get(&once, "user_agent.class"), Some(&s(class)), "{ua:?}");
            assert_eq!(get(&once, "user_agent.synthetic.type"), Some(&s("bot")), "{ua:?}");
            assert_ne!(
                get(&once, "user_agent.original"),
                Some(&s(ua)),
                "step 7 rewrote the value: {ua:?}"
            );
            let mut twice = once.clone();
            assert!(t.process(&Arc::new(Resource::default()), &mut twice));
            assert_eq!(get(&twice, "user_agent.class"), get(&once, "user_agent.class"), "{ua:?}");
            assert_eq!(
                get(&twice, "user_agent.synthetic.type"),
                get(&once, "user_agent.synthetic.type"),
                "{ua:?}"
            );
            assert_eq!(twice, once, "{ua:?}");
        }
    }

    /// A class already on the event -- from an upstream `set`/Lua stage or the producer itself --
    /// is an operator override: kept as-is, with no `user_agent.synthetic.type` derived.
    #[test]
    fn an_existing_user_agent_class_overrides_the_table() {
        let event = run(
            &mut bare(),
            &[
                ("user_agent.original", s("Mozilla/5.0 (compatible; Googlebot/2.1)")),
                ("user_agent.class", s("internal")),
            ],
        );
        assert_eq!(get(&event, "user_agent.class"), Some(&s("internal")));
        assert_eq!(get(&event, "user_agent.synthetic.type"), None);
    }

    #[test]
    fn a_metrics_only_event_is_untouched() {
        let mut attrs = AttrMap::new();
        attrs.insert("http.response.status_code", s("200"));
        attrs.insert("url-path", s("/x"));
        let mut event =
            Event::metric(0, attrs, MetricRecord::new(intern("m"), MetricKind::counter(1.0)));
        let before = event.clone();
        assert!(bare().process(&Arc::new(Resource::default()), &mut event));
        assert_eq!(event, before);
    }

    #[test]
    fn log_metrics_span_timestamp_and_resource_are_untouched() {
        let mut event = log_event(&full_line());
        event.metrics.push(MetricRecord::new(intern("m"), MetricKind::counter(1.0)));
        event.span = Some(SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: s("existing"),
            kind: SpanKind::Server,
            status: SpanStatus::Unset,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 5,
            flags: 0,
            ext: None,
        });
        let before = event.clone();
        let resource = Arc::new(Resource::default());
        let mut t = http(full_config());
        assert!(t.process(&resource, &mut event));
        assert_eq!(event.log, before.log);
        assert_eq!(event.metrics, before.metrics);
        assert_eq!(event.span, before.span);
        assert_eq!(event.timestamp, before.timestamp);
        assert!(t.map_resource(&resource).is_none(), "never maps the batch Resource");
        assert_ne!(event.attributes, before.attributes, "while the attributes did change");
    }

    #[test]
    fn an_event_with_no_http_attributes_is_untouched() {
        let pairs = [("service", s("api")), ("level", s("info"))];
        let event = run(&mut http(full_config()), &pairs);
        assert_eq!(event.attributes, log_event(&pairs).attributes);
    }

    #[test]
    fn every_counter_fires() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("http", "http_access", "transform");
        let build = |config: HttpAccessConfig| http(config).with_telemetry(telemetry.clone());

        let mut full = full_config();
        full.max_length.push(("url.scheme".to_string(), 2));
        let mut t = build(full);
        run(&mut t, &full_line());
        run(&mut t, &[("url.path", s("/nothing-matches")), ("url.scheme", s("https"))]);
        run(&mut t, &[("http.response.status_code", s("nope"))]);
        let mut unrouted = build(HttpAccessConfig {
            routes: vec![RouteRule::Builtin(RouteSet::Probes)],
            ..config()
        });
        run(&mut unrouted, &[("url.path", s("/x"))]);
        let mut by_rule =
            build(HttpAccessConfig { routes: vec![pattern("^/x$", "/x")], ..config() });
        run(&mut by_rule, &[("url.path", s("/x"))]);

        let events = registry.drain(0);
        let field = |name| Some(("field", name));
        assert_eq!(counter(&events, NORMALIZED, field("http.response.body.size")), 2.0);
        assert_eq!(counter(&events, NORMALIZED, field(REQUEST_LINE)), 1.0);
        assert_eq!(counter(&events, NORMALIZED, field(STATUS)), 1.0);
        assert_eq!(counter(&events, NORMALIZED, field("http.request.duration_s")), 1.0);
        assert_eq!(counter(&events, NORMALIZED, field(METHOD)), 1.0);
        assert_eq!(counter(&events, NORMALIZED, field(PROTOCOL_VERSION)), 1.0);
        for derived in [
            METHOD_ORIGINAL,
            UA_CLASS,
            UA_SYNTHETIC,
            ERROR_TYPE,
            SPAN_STATUS,
            SPAN_NAME,
            SPAN_DURATION_S,
            CLIENT_ADDRESS,
        ] {
            assert_eq!(counter(&events, DERIVED, field(derived)), 1.0, "{derived}");
        }
        assert_eq!(counter(&events, DERIVED, field(ROUTE)), 3.0, "asset, other, rule");
        assert_eq!(counter(&events, TRUNCATED, field("url.scheme")), 1.0);
        assert_eq!(counter(&events, CLEANED, field(USER_AGENT)), 1.0);
        assert_eq!(counter(&events, INVALID, field(STATUS)), 1.0);
        assert_eq!(counter(&events, REDACTED_METRIC, None), 1.0);
        for outcome in ["builtin", "other", "none", "rule"] {
            assert_eq!(counter(&events, ROUTED, Some(("outcome", outcome))), 1.0, "{outcome}");
        }
    }

    #[test]
    fn every_built_in_pattern_compiles() {
        for (_, pattern) in BUILTIN_UA_RULES {
            Regex::new(pattern).expect(pattern);
        }
        for (_, pattern, _) in BUILTIN_ROUTE_SETS {
            Regex::new(pattern).expect(pattern);
        }
    }

    #[test]
    fn an_invalid_configured_pattern_is_an_error_not_a_panic() {
        let result = HttpAccess::new(HttpAccessConfig {
            routes: vec![pattern("(", "/x")],
            ..HttpAccessConfig::default()
        });
        assert!(result.is_err());
    }
}
