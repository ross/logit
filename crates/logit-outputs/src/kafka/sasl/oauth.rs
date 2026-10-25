//! SASL `OAUTHBEARER` (RFC 7628, KIP-255, KIP-342, KIP-768): one client message carrying an
//! OAuth 2.0 access token and the configured SASL extensions.
//!
//! The message is Kafka's `OAuthBearerClientInitialResponse`: the GS2 header `n,,`, then
//! `auth=Bearer <token>` and each extension as `key=value`, each preceded by `\x01`, and a closing
//! `\x01\x01`. The broker answers an accepted token with an empty message. It answers a refused
//! one with an RFC 7628 JSON error (`{"status":"invalid_token"}`) and no error code; the client
//! sends a lone `\x01` to end the exchange, and the broker then fails it with
//! `SASL_AUTHENTICATION_FAILED`. The session keeps the JSON's `status` for that error's message.
//!
//! The token is either fixed, or fetched from a token endpoint with the client-credentials grant
//! (RFC 6749 §4.4) and cached in the authenticator for every connection of the sink:
//!
//! - The request is a form `POST` of `grant_type=client_credentials` and the optional `scope`,
//!   with the client id and secret in HTTP Basic authentication as they are, as the Java client
//!   sends them by default (`sasl.oauthbearer.header.urlencode: false`), not form-urlencoded as
//!   RFC 6749 §2.3.1 asks.
//! - It's bounded by the sink's `timeout`, its connection by a fixed 5 s
//!   (`crate::http::DEFAULT_CONNECT_TIMEOUT`), and follows no redirect. The endpoint's
//!   certificate is checked against the bundled Mozilla roots, never the sink's broker `tls:`
//!   block.
//! - A cached token is reused until 80% of its `expires_in` has passed, measured from before the
//!   request, the Java client's `sasl.login.refresh.window.factor` default. Without
//!   `expires_in`, a token that is a JWT is kept until 80% of the time to its `exp` claim, and
//!   any other token is fetched again for every connection.
//! - A failed refresh leaves the cached token in use until it expires, as the Java client and
//!   librdkafka do, with one `token_refresh_failed` warning per failure (throttled). No dial
//!   fetches for `RETRY_DELAY` after it, so dials during an endpoint outage don't each wait on its
//!   timeout; the first dial after that fetches again. Nothing fetches on a timer. Unlike
//!   both, the sink never sends a token past its expiry: the endpoint's error is reported instead
//!   of a broker's refusal of a token known to have expired.
//! - Concurrent dials wait on one fetch. A broker's refusal of a fetched token drops it from the
//!   cache, whether it's an error challenge (Kafka) or a bare `SASL_AUTHENTICATION_FAILED`
//!   (Redpanda), so the next dial fetches a new one rather than resending a token the broker
//!   refused.
//!
//! An endpoint fault that fails the dial takes its class from `super::super`'s response-class
//! table: unreachable, timed out, `429`, or `5xx` is `Clean`, and so is a `2xx` whose body is cut
//! off by the timeout or a closed connection; any other answer but a `2xx` is `Refused`, and so
//! is a `2xx` whose body, read up to `MAX_RESPONSE_BYTES`, holds no usable `access_token`. No message carries the token
//! or the client secret, and the endpoint is printed through `logit_core::redact::url`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use logit_core::{redact, Diagnostics};
use logit_pipeline::Fault;
use tokio::time::Instant;

use super::{Authenticator, Session, Step};

/// RFC 7628's `kvsep`.
const KVSEP: u8 = 0x01;

/// The share of a token's lifetime it's reused for.
const REFRESH_FACTOR: f64 = 0.8;

/// How long after a failed refresh no dial fetches while the cached token is still valid: the
/// Java client's `DELAY_SECONDS_BEFORE_NEXT_RETRY_WHEN_RELOGIN_FAILS` and librdkafka's
/// delay after `rd_kafka_oauthbearer_set_token_failure`.
const RETRY_DELAY: Duration = Duration::from_secs(10);

/// How much of a token endpoint's answer is read. A token response is a few kilobytes at most.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// How much of an endpoint's `error_description` or a broker's error status a message quotes.
const MAX_QUOTED: usize = 256;

pub(crate) struct OAuthBearer {
    source: Source,
    extensions: Vec<(String, String)>,
}

enum Source {
    Fixed(String),
    Endpoint(Arc<Endpoint>),
}

impl OAuthBearer {
    pub(crate) fn fixed(token: String, extensions: Vec<(String, String)>) -> Self {
        Self { source: Source::Fixed(token), extensions }
    }

    pub(crate) fn client_credentials(
        url: String,
        client_id: &str,
        client_secret: &str,
        scope: Option<&str>,
        extensions: Vec<(String, String)>,
        timeout: Duration,
    ) -> Self {
        let endpoint = Endpoint {
            client: crate::http::build_client(
                timeout,
                crate::http::DEFAULT_CONNECT_TIMEOUT,
                crate::http::DEFAULT_IDLE_TIMEOUT,
                None,
            ),
            url,
            client_id: client_id.to_string(),
            authorization: basic_authorization(client_id, client_secret),
            body: token_request_body(scope),
            timeout,
            cache: Mutex::new(None),
            fetching: tokio::sync::Mutex::new(()),
            diag: Mutex::new(Diagnostics::default()),
            #[cfg(test)]
            skew: Mutex::new(Duration::ZERO),
        };
        Self { source: Source::Endpoint(Arc::new(endpoint)), extensions }
    }
}

#[async_trait::async_trait]
impl Authenticator for OAuthBearer {
    fn mechanism(&self) -> &'static str {
        "OAUTHBEARER"
    }

    fn principal(&self) -> String {
        match &self.source {
            Source::Fixed(_) => "a fixed token".to_string(),
            Source::Endpoint(endpoint) => format!(
                "client {:?} of token endpoint {}",
                endpoint.client_id,
                redact::url(&endpoint.url)
            ),
        }
    }

    fn set_diagnostics(&self, diag: &Diagnostics) {
        if let Source::Endpoint(endpoint) = &self.source {
            *endpoint.diag.lock().unwrap() = diag.clone();
        }
    }

    async fn start(&self, _host: &str) -> anyhow::Result<Box<dyn Session>> {
        let (token, endpoint) = match &self.source {
            Source::Fixed(token) => (token.clone(), None),
            Source::Endpoint(endpoint) => (endpoint.token().await?, Some(endpoint.clone())),
        };
        Ok(Box::new(OAuthSession {
            message: Some(client_message(&token, &self.extensions)),
            token,
            endpoint,
            status: None,
        }))
    }
}

/// Kafka's `OAuthBearerClientInitialResponse::toBytes` with no authorization id.
pub(crate) fn client_message(token: &str, extensions: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + token.len());
    out.extend_from_slice(b"n,,");
    out.push(KVSEP);
    out.extend_from_slice(b"auth=Bearer ");
    out.extend_from_slice(token.as_bytes());
    for (key, value) in extensions {
        out.push(KVSEP);
        out.extend_from_slice(key.as_bytes());
        out.push(b'=');
        out.extend_from_slice(value.as_bytes());
    }
    out.extend_from_slice(&[KVSEP, KVSEP]);
    out
}

struct OAuthSession {
    message: Option<Vec<u8>>,
    token: String,
    endpoint: Option<Arc<Endpoint>>,
    /// The broker's error challenge's `status`, once it has sent one.
    status: Option<String>,
}

impl Session for OAuthSession {
    fn first(&mut self) -> anyhow::Result<Vec<u8>> {
        self.message.take().ok_or_else(|| anyhow!("OAUTHBEARER sends one message"))
    }

    fn next(&mut self, server: &[u8]) -> anyhow::Result<Step> {
        if self.status.is_some() {
            anyhow::bail!(
                "the broker accepted the exchange after refusing the token ({})",
                self.failure().unwrap_or_default()
            );
        }
        if server.is_empty() {
            return Ok(Step::Done);
        }
        self.status = Some(error_status(server));
        self.refused();
        Ok(Step::Send(vec![KVSEP]))
    }

    fn failure(&self) -> Option<String> {
        self.status.as_ref().map(|status| format!("the broker refused the token: {status}"))
    }

    /// Redpanda fails the exchange with no error challenge, so `next` never sees its refusal.
    fn refused(&mut self) {
        if let Some(endpoint) = &self.endpoint {
            endpoint.forget(&self.token);
        }
    }
}

/// The `status` (and `scope`, when present) of an RFC 7628 §3.2.2 error challenge.
fn error_status(server: &[u8]) -> String {
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(server) else {
        return "an error challenge that isn't JSON".to_string();
    };
    let field = |name: &str| json.get(name).and_then(|v| v.as_str()).map(|v| quoted(v, MAX_QUOTED));
    match (field("status"), field("scope")) {
        (Some(status), Some(scope)) => format!("status {status}, scope {scope}"),
        (Some(status), None) => format!("status {status}"),
        (None, _) => "an error challenge without a status".to_string(),
    }
}

/// `value`, cut to `max` bytes on a character boundary, in quotes.
fn quoted(value: &str, max: usize) -> String {
    let mut end = value.len().min(max);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    let ellipsis = if end < value.len() { "..." } else { "" };
    format!("{:?}{ellipsis}", &value[..end])
}

struct Endpoint {
    client: reqwest::Client,
    url: String,
    client_id: String,
    /// `Basic <credentials>`: holds the client secret.
    authorization: String,
    body: String,
    timeout: Duration,
    cache: Mutex<Option<Cached>>,
    /// Held across a fetch, so concurrent dials wait on one request.
    fetching: tokio::sync::Mutex<()>,
    /// The sink's, once attached, for the failed-refresh warning.
    diag: Mutex<Diagnostics>,
    /// Added to the clock, so a test can move past a refresh or an expiry without waiting.
    #[cfg(test)]
    skew: Mutex<Duration>,
}

struct Cached {
    token: String,
    refresh_at: Instant,
    expires_at: Instant,
    /// Set by a failed refresh: no fetch before it while the token is still valid.
    retry_at: Option<Instant>,
}

impl Endpoint {
    /// The cached token, or a fresh one once the cached one is due for refresh. A failed
    /// refresh returns the cached token while it's valid, and its error once it isn't.
    async fn token(&self) -> anyhow::Result<String> {
        let _fetching = self.fetching.lock().await;
        if let Some(cached) = self.cache.lock().unwrap().as_ref() {
            let now = self.now();
            let retrying = cached.retry_at.is_some_and(|at| now < at);
            if now < cached.refresh_at || (retrying && now < cached.expires_at) {
                return Ok(cached.token.clone());
            }
        }
        let asked = self.now();
        let err = match self.fetch().await {
            Ok((token, lifetime)) => {
                *self.cache.lock().unwrap() = lifetime.map(|lifetime| Cached {
                    token: token.clone(),
                    refresh_at: asked + lifetime.mul_f64(REFRESH_FACTOR),
                    expires_at: asked + lifetime,
                    retry_at: None,
                });
                return Ok(token);
            }
            Err(err) => err,
        };
        // Read again after the fetch, so a token `forget` dropped meanwhile isn't brought back.
        let mut cache = self.cache.lock().unwrap();
        let now = self.now();
        let Some(cached) = cache.as_mut().filter(|cached| now < cached.expires_at) else {
            return Err(err);
        };
        cached.retry_at = Some(now + RETRY_DELAY);
        let remaining = Duration::from_millis(
            u64::try_from((cached.expires_at - now).as_millis()).unwrap_or(u64::MAX),
        );
        let quiet = RETRY_DELAY.min(remaining);
        self.diag.lock().unwrap().clone().warn_throttled(
            "token_refresh_failed",
            format_args!(
                "refreshing the OAUTHBEARER token from {} failed; the cached token, valid for \
                 another {remaining:?}, stays in use, with no fetch for {quiet:?}: \
                 {err:#}",
                redact::url(&self.url)
            ),
        );
        Ok(cached.token.clone())
    }

    fn now(&self) -> Instant {
        #[cfg(test)]
        return Instant::now() + *self.skew.lock().unwrap();
        #[cfg(not(test))]
        Instant::now()
    }

    /// Drops `token` from the cache, unless another connection has replaced it already.
    fn forget(&self, token: &str) {
        let mut cache = self.cache.lock().unwrap();
        if cache.as_ref().is_some_and(|cached| cached.token == token) {
            *cache = None;
        }
    }

    /// One client-credentials request: the token and how long it's valid.
    async fn fetch(&self) -> anyhow::Result<(String, Option<Duration>)> {
        let shown = redact::url(&self.url);
        let response = self
            .client
            .post(&self.url)
            .header(reqwest::header::AUTHORIZATION, &self.authorization)
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(reqwest::header::ACCEPT, "application/json")
            .body(self.body.clone())
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|err| {
                // reqwest's connect timeout also sets `is_timeout`, so the dial is checked first.
                let connect_timed_out = err.is_connect() && err.is_timeout();
                let timed_out = err.is_timeout();
                let err = anyhow::Error::new(err.without_url());
                if connect_timed_out {
                    err.context(format!(
                        "token endpoint {shown} didn't connect within {:?}",
                        crate::http::DEFAULT_CONNECT_TIMEOUT
                    ))
                } else if timed_out {
                    err.context(format!(
                        "token endpoint {shown} timed out after {:?}",
                        self.timeout
                    ))
                } else {
                    err.context(format!("requesting a token from {shown}"))
                }
            })
            .context(Fault::Clean)?;
        let status = response.status();
        if !status.is_success() {
            let body = crate::http::read_body_bytes(response, MAX_RESPONSE_BYTES).await;
            let class = if status.as_u16() == 429 || status.is_server_error() {
                Fault::Clean
            } else {
                Fault::Refused
            };
            return Err(anyhow!("token endpoint {shown} answered {status}{}", oauth_error(&body)))
                .context(class);
        }
        // A body cut off by the timeout or a closed connection is a transport fault, not an
        // answer that fails to parse.
        let body = crate::http::try_read_body_bytes(response, MAX_RESPONSE_BYTES)
            .await
            .map_err(|err| {
                let timed_out = err.is_timeout();
                let err = anyhow::Error::new(err.without_url());
                if timed_out {
                    err.context(format!(
                        "token endpoint {shown} timed out after {:?} reading its {status} answer",
                        self.timeout
                    ))
                } else {
                    err.context(format!("reading token endpoint {shown}'s {status} answer"))
                }
            })
            .context(Fault::Clean)?;
        parse_token_response(&body, SystemTime::now())
            .with_context(|| format!("token endpoint {shown} answered {status}"))
            .context(Fault::Refused)
    }
}

/// `: <error> (<error_description>)` from an RFC 6749 §5.2 error body, or nothing when the body
/// isn't one. Nothing else of the body is quoted, since it may echo the request.
fn oauth_error(body: &[u8]) -> String {
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(body) else {
        return String::new();
    };
    let Some(error) = json.get("error").and_then(|v| v.as_str()) else {
        return String::new();
    };
    match json.get("error_description").and_then(|v| v.as_str()) {
        Some(description) => {
            format!(": {} ({})", quoted(error, MAX_QUOTED), quoted(description, MAX_QUOTED))
        }
        None => format!(": {}", quoted(error, MAX_QUOTED)),
    }
}

/// The access token of an RFC 6749 §5.1 response, and how long it's valid, given the wall clock
/// `now` for a JWT's `exp`.
pub(crate) fn parse_token_response(
    body: &[u8],
    now: SystemTime,
) -> anyhow::Result<(String, Option<Duration>)> {
    let json: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| anyhow!("a token response that isn't JSON"))?;
    let token = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("a token response without an 'access_token' string"))?;
    anyhow::ensure!(
        is_bearer_token(token),
        "an 'access_token' that is empty or holds a character a bearer token can't carry"
    );
    // Some providers send `expires_in` as a string.
    let expires_in = json.get("expires_in").and_then(|v| match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    });
    let lifetime = match expires_in {
        Some(seconds) => Some(seconds),
        None => jwt_exp(token).and_then(|exp| {
            let now = now.duration_since(UNIX_EPOCH).ok()?.as_secs_f64();
            Some(exp - now)
        }),
    };
    let lifetime = lifetime
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
        .map(|seconds| Duration::from_secs_f64(seconds.min(1e9)));
    Ok((token.to_string(), lifetime))
}

/// The `exp` claim of a JWT, in seconds since the epoch; `None` for a token that isn't a JWT.
fn jwt_exp(token: &str) -> Option<f64> {
    let mut parts = token.split('.');
    let (_header, payload) = (parts.next()?, parts.next()?);
    let payload = URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    claims.get("exp")?.as_f64()
}

/// RFC 6750's `b64token`, what RFC 7628 puts after `auth=Bearer `. A fetched token outside it
/// could carry a `\x01` into the message's framing.
fn is_bearer_token(token: &str) -> bool {
    let body = token.trim_end_matches('=');
    !body.is_empty()
        && body.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
        })
}

/// `Basic base64(id ":" secret)`, as Kafka's `ClientCredentialsRequestFormatter` writes it with
/// `sasl.oauthbearer.header.urlencode` at its default, `false`.
pub(crate) fn basic_authorization(client_id: &str, client_secret: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("{client_id}:{client_secret}")))
}

fn token_request_body(scope: Option<&str>) -> String {
    match scope {
        Some(scope) => format!("grant_type=client_credentials&scope={}", form_urlencode(scope)),
        None => "grant_type=client_credentials".to_string(),
    }
}

/// The `application/x-www-form-urlencoded` byte serializer (RFC 6749 Appendix B): ASCII
/// letters, digits, and `*-._` as they are, space as `+`, every other byte `%XX`.
pub(crate) fn form_urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                out.push(char::from(b))
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::fake_broker::TokenEndpoint;
    use logit_pipeline::test_util::RECV_TIMEOUT;

    /// Kafka's `OAuthBearerClientInitialResponse::toBytes` writes
    /// `String.format("n,%s,%sauth=Bearer %s%s%s%s", authzid, SEPARATOR, tokenValue, extensions,
    /// SEPARATOR, SEPARATOR)`, with `SEPARATOR` `"\u0001"` and the extensions, when present,
    /// prefixed by one `SEPARATOR` and joined by another as `key=value`
    /// (clients/src/main/java/org/apache/kafka/common/security/oauthbearer/internals/
    /// OAuthBearerClientInitialResponse.java, Apache Kafka 4.2.0).
    #[test]
    fn the_message_is_kafkas_client_initial_response() {
        assert_eq!(client_message("eyJ.a.b", &[]), b"n,,\x01auth=Bearer eyJ.a.b\x01\x01");
        let extensions = vec![
            ("identityPoolId".to_string(), "pool-1".to_string()),
            ("logicalCluster".to_string(), "lkc-1".to_string()),
        ];
        assert_eq!(
            client_message("t0k", &extensions),
            b"n,,\x01auth=Bearer t0k\x01identityPoolId=pool-1\x01logicalCluster=lkc-1\x01\x01"
        );
    }

    #[tokio::test]
    async fn a_fixed_token_finishes_on_an_empty_answer() {
        let auth = OAuthBearer::fixed("tok".into(), Vec::new());
        assert_eq!(auth.principal(), "a fixed token");
        let mut session = auth.start("broker").await.unwrap();
        assert_eq!(session.first().unwrap(), b"n,,\x01auth=Bearer tok\x01\x01");
        assert_eq!(session.next(b"").unwrap(), Step::Done);
        assert_eq!(session.failure(), None);
    }

    /// RFC 7628 §3.2.3: the client answers an error challenge with a lone `kvsep`, and keeps
    /// the status for the broker's failure that follows.
    #[tokio::test]
    async fn an_error_challenge_is_answered_with_kvsep_and_its_status_kept() {
        let auth = OAuthBearer::fixed("tok".into(), Vec::new());
        let mut session = auth.start("broker").await.unwrap();
        session.first().unwrap();
        let challenge = br#"{"status":"invalid_token","scope":"kafka","openid-configuration":"x"}"#;
        assert_eq!(session.next(challenge).unwrap(), Step::Send(vec![0x01]));
        assert_eq!(
            session.failure().unwrap(),
            r#"the broker refused the token: status "invalid_token", scope "kafka""#
        );
        assert!(session.next(b"").is_err(), "an accept after a refusal is broken");
    }

    /// Kafka 4.2.0's `ClientCredentialsRequestFormatter` URL-encodes the id and secret only when
    /// `sasl.oauthbearer.header.urlencode` is `true`, and `SaslConfigs` defaults it to `false`.
    #[test]
    fn the_client_credentials_are_basic_encoded_as_they_are() {
        assert_eq!(form_urlencode("a b+c/d%é*-._"), "a+b%2Bc%2Fd%25%C3%A9*-._");
        let header = basic_authorization("my client", "s3:cr+t%é");
        let decoded = STANDARD.decode(header.strip_prefix("Basic ").unwrap()).unwrap();
        assert_eq!(decoded, "my client:s3:cr+t%é".as_bytes());
        assert_eq!(token_request_body(None), "grant_type=client_credentials");
        assert_eq!(
            token_request_body(Some("kafka write")),
            "grant_type=client_credentials&scope=kafka+write"
        );
    }

    #[test]
    fn a_token_response_carries_its_lifetime() {
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let parse = |body: &str| parse_token_response(body.as_bytes(), now);
        assert_eq!(
            parse(r#"{"access_token":"t","token_type":"Bearer","expires_in":100}"#).unwrap(),
            ("t".to_string(), Some(Duration::from_secs(100)))
        );
        assert_eq!(
            parse(r#"{"access_token":"t","expires_in":"100"}"#).unwrap().1,
            Some(Duration::from_secs(100))
        );
        // No `expires_in`: a JWT's `exp`, 50 s from `now`.
        let claims = URL_SAFE_NO_PAD.encode(br#"{"sub":"logit","exp":1800000050}"#);
        let jwt = format!("eyJhbGciOiJub25lIn0.{claims}.sig");
        assert_eq!(
            parse(&format!(r#"{{"access_token":"{jwt}"}}"#)).unwrap(),
            (jwt, Some(Duration::from_secs(50)))
        );
        // Neither: not reused.
        assert_eq!(parse(r#"{"access_token":"opaque"}"#).unwrap().1, None);
        assert_eq!(parse(r#"{"access_token":"t","expires_in":0}"#).unwrap().1, None);
    }

    #[test]
    fn a_token_response_without_a_usable_token_is_an_error() {
        let now = SystemTime::now();
        for body in [
            "<html>login</html>",
            r#"{"token":"t"}"#,
            r#"{"access_token":7}"#,
            r#"{"access_token":""}"#,
            "{\"access_token\":\"a\\u0001b\"}",
            r#"{"access_token":"a b"}"#,
        ] {
            assert!(parse_token_response(body.as_bytes(), now).is_err(), "{body}");
        }
    }

    #[test]
    fn an_oauth_error_body_quotes_its_error_and_description_only() {
        assert_eq!(
            oauth_error(br#"{"error":"invalid_client","error_description":"bad secret"}"#),
            r#": "invalid_client" ("bad secret")"#
        );
        assert_eq!(
            oauth_error(br#"{"error":"invalid_scope","echo":"s3cr3t"}"#),
            r#": "invalid_scope""#
        );
        assert_eq!(oauth_error(b"Unauthorized s3cr3t"), "");
    }

    #[test]
    fn a_long_quote_is_cut_on_a_character_boundary() {
        assert_eq!(quoted("é".repeat(200).as_str(), 5), format!("{:?}...", "éé"));
    }

    /// The fake endpoint's default token lives an hour: due for refresh at 48 minutes.
    const DUE: Duration = Duration::from_secs(2880);
    const LIFETIME: Duration = Duration::from_secs(3600);

    /// An authenticator on `endpoint`, its token cache, and the diagnostics it reports to.
    fn client(endpoint: &TokenEndpoint) -> (OAuthBearer, Arc<Endpoint>, Diagnostics) {
        let auth = OAuthBearer::client_credentials(
            endpoint.url.clone(),
            "logit",
            "s3cr3t",
            None,
            Vec::new(),
            RECV_TIMEOUT,
        );
        let diag = Diagnostics::new("out");
        auth.set_diagnostics(&diag);
        let Source::Endpoint(cache) = &auth.source else { unreachable!() };
        let cache = cache.clone();
        (auth, cache, diag)
    }

    fn advance(cache: &Endpoint, by: Duration) {
        *cache.skew.lock().unwrap() += by;
    }

    fn fail(endpoint: &TokenEndpoint) {
        endpoint.state().answer = Box::new(|_| (503, String::new()));
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_the_valid_cached_token() {
        let endpoint = TokenEndpoint::start().await;
        let (auth, cache, diag) = client(&endpoint);
        assert_eq!(cache.token().await.unwrap(), "token-0");
        advance(&cache, DUE);
        fail(&endpoint);

        let mut session = auth.start("broker").await.unwrap();
        assert_eq!(session.first().unwrap(), client_message("token-0", &[]));
        assert_eq!(endpoint.fetches(), 2, "the refresh was tried");
        assert_eq!(diag.occurrences("token_refresh_failed"), 1);
    }

    /// No fetch for `RETRY_DELAY` after a failed one, including for dials that waited on it.
    #[tokio::test]
    async fn a_failed_refresh_is_retried_after_ten_seconds() {
        let endpoint = TokenEndpoint::start().await;
        let (_auth, cache, diag) = client(&endpoint);
        cache.token().await.unwrap();
        advance(&cache, DUE);
        fail(&endpoint);
        assert_eq!(cache.token().await.unwrap(), "token-0");
        assert_eq!(endpoint.fetches(), 2);

        assert_eq!(cache.token().await.unwrap(), "token-0");
        advance(&cache, RETRY_DELAY - Duration::from_millis(100));
        assert_eq!(cache.token().await.unwrap(), "token-0");
        assert_eq!(endpoint.fetches(), 2, "inside the retry window");

        advance(&cache, Duration::from_millis(100));
        let mut dials = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let cache = cache.clone();
            dials.spawn(async move { cache.token().await.unwrap() });
        }
        while let Some(token) = dials.join_next().await {
            assert_eq!(token.unwrap(), "token-0");
        }
        assert_eq!(endpoint.fetches(), 3, "one retry for every dial waiting on it");
        assert_eq!(diag.occurrences("token_refresh_failed"), 2);
    }

    #[tokio::test]
    async fn a_failed_refresh_past_expiry_is_an_error() {
        let endpoint = TokenEndpoint::start().await;
        let (_auth, cache, diag) = client(&endpoint);
        cache.token().await.unwrap();
        advance(&cache, LIFETIME);
        fail(&endpoint);

        let err = cache.token().await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
        assert!(format!("{err:#}").contains("503"), "{err:#}");
        assert_eq!(diag.occurrences("token_refresh_failed"), 0);

        // A token kept by a failed refresh is dropped once it expires.
        let endpoint = TokenEndpoint::start().await;
        let (_auth, cache, _diag) = client(&endpoint);
        cache.token().await.unwrap();
        advance(&cache, DUE);
        fail(&endpoint);
        assert_eq!(cache.token().await.unwrap(), "token-0");
        advance(&cache, LIFETIME - DUE);
        assert!(cache.token().await.is_err());
    }

    #[tokio::test]
    async fn a_refresh_after_a_failed_one_replaces_the_token() {
        let endpoint = TokenEndpoint::start().await;
        let (_auth, cache, _diag) = client(&endpoint);
        cache.token().await.unwrap();
        advance(&cache, DUE);
        fail(&endpoint);
        assert_eq!(cache.token().await.unwrap(), "token-0");

        advance(&cache, RETRY_DELAY);
        endpoint.state().answer =
            Box::new(|n| (200, format!(r#"{{"access_token":"token-{n}","expires_in":3600}}"#)));
        assert_eq!(cache.token().await.unwrap(), "token-2");
        assert_eq!(cache.token().await.unwrap(), "token-2");
        assert_eq!(endpoint.fetches(), 3);
    }

    /// A token the broker refused stays forgotten when the refresh after it fails, whether it
    /// was forgotten before the fetch or while the fetch was in flight.
    #[tokio::test]
    async fn a_failed_refresh_doesnt_bring_back_a_forgotten_token() {
        let endpoint = TokenEndpoint::start().await;
        let (_auth, cache, _diag) = client(&endpoint);
        cache.token().await.unwrap();
        cache.forget("token-0");
        fail(&endpoint);
        assert!(cache.token().await.is_err());

        let endpoint = TokenEndpoint::start().await;
        let (_auth, cache, diag) = client(&endpoint);
        cache.token().await.unwrap();
        advance(&cache, DUE);
        let during = cache.clone();
        endpoint.state().answer = Box::new(move |_| {
            during.forget("token-0");
            (503, String::new())
        });
        assert!(cache.token().await.is_err());
        assert_eq!(diag.occurrences("token_refresh_failed"), 0);
    }
}
