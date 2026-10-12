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
//! - It's bounded by the sink's `timeout` and follows no redirect. The endpoint's certificate is
//!   checked against the bundled Mozilla roots, never the sink's broker `tls:` block.
//! - A cached token is reused until 80% of its `expires_in` has passed, measured from before the
//!   request, the Java client's `sasl.login.refresh.window.factor` default. Without
//!   `expires_in`, a token that is a JWT is kept until 80% of the time to its `exp` claim, and
//!   any other token is fetched again for every connection.
//! - Concurrent dials wait on one fetch. A broker's refusal of a fetched token drops it from the
//!   cache, whether it's an error challenge (Kafka) or a bare `SASL_AUTHENTICATION_FAILED`
//!   (Redpanda), so the next dial fetches a new one rather than resending a token the broker
//!   refused.
//!
//! An endpoint fault's class follows `super::super`'s response-class table: unreachable, timed
//! out, `429`, or `5xx` is `Clean`, and so is a `2xx` whose body is cut off by the timeout or a
//! closed connection; any other answer but a `2xx` is `Refused`, and so is a `2xx` whose body,
//! read up to `MAX_RESPONSE_BYTES`, holds no usable `access_token`. No message carries the token
//! or the client secret, and the endpoint is printed through `logit_core::redact::url`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use logit_core::redact;
use logit_pipeline::Fault;
use tokio::time::Instant;

use super::{Authenticator, Session, Step};

/// RFC 7628's `kvsep`.
const KVSEP: u8 = 0x01;

/// The share of a token's lifetime it's reused for.
const REFRESH_FACTOR: f64 = 0.8;

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
            client: crate::http::build_client(timeout, None),
            url,
            client_id: client_id.to_string(),
            authorization: basic_authorization(client_id, client_secret),
            body: token_request_body(scope),
            timeout,
            cache: Mutex::new(None),
            fetching: tokio::sync::Mutex::new(()),
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
}

struct Cached {
    token: String,
    refresh_at: Instant,
}

impl Endpoint {
    /// The cached token, or a fresh one once the cached one is due for refresh.
    async fn token(&self) -> anyhow::Result<String> {
        let _fetching = self.fetching.lock().await;
        if let Some(cached) = self.cache.lock().unwrap().as_ref() {
            if Instant::now() < cached.refresh_at {
                return Ok(cached.token.clone());
            }
        }
        let asked = Instant::now();
        let (token, lifetime) = self.fetch().await?;
        *self.cache.lock().unwrap() =
            lifetime.map(|lifetime| Cached { token: token.clone(), refresh_at: asked + lifetime });
        Ok(token)
    }

    /// Drops `token` from the cache, unless another connection has replaced it already.
    fn forget(&self, token: &str) {
        let mut cache = self.cache.lock().unwrap();
        if cache.as_ref().is_some_and(|cached| cached.token == token) {
            *cache = None;
        }
    }

    /// One client-credentials request: the token and how long to reuse it.
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
                let timed_out = err.is_timeout();
                let err = anyhow::Error::new(err.without_url());
                if timed_out {
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

/// The access token of an RFC 6749 §5.1 response, and how long to reuse it, given the wall
/// clock `now` for a JWT's `exp`.
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
    let reuse = lifetime
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
        .map(|seconds| Duration::from_secs_f64((seconds * REFRESH_FACTOR).min(1e9)));
    Ok((token.to_string(), reuse))
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
    fn a_token_response_is_reused_for_80_percent_of_its_lifetime() {
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let parse = |body: &str| parse_token_response(body.as_bytes(), now);
        assert_eq!(
            parse(r#"{"access_token":"t","token_type":"Bearer","expires_in":100}"#).unwrap(),
            ("t".to_string(), Some(Duration::from_secs(80)))
        );
        assert_eq!(
            parse(r#"{"access_token":"t","expires_in":"100"}"#).unwrap().1,
            Some(Duration::from_secs(80))
        );
        // No `expires_in`: a JWT's `exp`, 50 s from `now`.
        let claims = URL_SAFE_NO_PAD.encode(br#"{"sub":"logit","exp":1800000050}"#);
        let jwt = format!("eyJhbGciOiJub25lIn0.{claims}.sig");
        assert_eq!(
            parse(&format!(r#"{{"access_token":"{jwt}"}}"#)).unwrap(),
            (jwt, Some(Duration::from_secs(40)))
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
}
