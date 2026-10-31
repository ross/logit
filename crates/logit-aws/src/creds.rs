//! AWS credentials: a hand-rolled subset of the AWS SDK for Java v2's default
//! chain, in the order `aws-msk-iam-auth`'s `MSKCredentialProvider` builds it
//! (`docs/adr/kafka-sasl.md`, "Amendment: what W3 settled"). `kafka_out`'s `AWS_MSK_IAM` is
//! the first caller:
//!
//! 1. The environment: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`.
//! 2. Web identity: `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`, exchanged through an
//!    unsigned STS `AssumeRoleWithWebIdentity` at the broker's region (EKS IRSA). A region
//!    outside the `aws`, `aws-cn`, and `aws-us-gov` partitions is `Refused`
//!    ([`partition_domain`]).
//! 3. The shared files: `AWS_SHARED_CREDENTIALS_FILE` and `AWS_CONFIG_FILE`, or
//!    `~/.aws/credentials` and `~/.aws/config`, profile `AWS_PROFILE` or `default`, static keys
//!    only. The credentials file wins a key both files set. A profile `AWS_PROFILE` names must
//!    hold keys; the implicit `default` without them, such as the `region`-only one
//!    `aws configure` writes, counts as absent.
//! 4. The container endpoint: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` at `169.254.170.2`, else
//!    `AWS_CONTAINER_CREDENTIALS_FULL_URI`, with `AWS_CONTAINER_AUTHORIZATION_TOKEN` or the file
//!    `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` names (ECS, EKS Pod Identity).
//! 5. IMDSv2, unless `AWS_EC2_METADATA_DISABLED` is `true`.
//!
//! A source whose variables or profile aren't there, or the implicit `default` profile without
//! keys, is skipped. Unlike the Java chain, a source that is there and fails ends the lookup
//! with its own error instead of falling through, so a broken IRSA setup on EKS never
//! authenticates as the node's instance role. With no source there, the error lists what was
//! looked for.
//!
//! Credentials that carry an expiry are reused by every connection until 5 minutes before it,
//! and concurrent dials wait on one lookup. Credentials without one (the environment, the files)
//! are read again on every dial, which costs a variable lookup or a file read.
//!
//! A fault's class follows the calling sink's response-class table (`kafka_out`'s is in
//! `logit-outputs`' `kafka` module doc): an endpoint unreachable, timed
//! out, or answering `429` or `5xx`, and STS's `Throttling` and `IDPCommunicationError`, are
//! `Clean`; any other answer from an endpoint, and a source misconfigured on this host, are
//! `Refused`. Every HTTP request is bounded by the sink's `timeout`, and its connection by a fixed
//! 5 s (`crate::http::DEFAULT_CONNECT_TIMEOUT`). STS's request honors
//! `HTTPS_PROXY` and the other proxy variables; the container endpoint's and IMDS's never go
//! through a proxy, as the Java SDK's don't, because a proxy would see the container
//! authorization token and the credentials. No message or `Debug` output
//! carries a secret key, a session token, or a web identity token.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use logit_core::redact;
use logit_pipeline::Fault;
use tokio::time::Instant;

/// How long before their expiry cached credentials are replaced.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

/// How much of an endpoint's answer, or of a token file, is read.
const MAX_BODY_BYTES: usize = 64 * 1024;

/// How much of an endpoint's error code or message a message quotes.
const MAX_QUOTED: usize = 256;

/// The IMDSv2 session token's lifetime the token request asks for, the Java SDK's default.
const IMDS_TOKEN_TTL_SECONDS: &str = "21600";

/// The keys that make a profile take its credentials from somewhere this chain doesn't support.
const UNSUPPORTED_PROFILE_KEYS: &[&str] = &[
    "role_arn",
    "source_profile",
    "credential_source",
    "web_identity_token_file",
    "credential_process",
    "sso_session",
    "sso_start_url",
    "sso_account_id",
    "sso_role_name",
    "sso_region",
    "login_session",
];

/// Reads an environment variable. The process environment in production; a map in tests, so
/// no test changes the environment another test reads.
pub type Env = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// The process environment.
pub(crate) fn process_env() -> Env {
    Arc::new(|name| std::env::var(name).ok())
}

/// Where the chain's HTTP sources are. Only tests point them anywhere else; no variable moves
/// them.
#[derive(Debug, Clone)]
pub struct Endpoints {
    /// IMDS, `http://169.254.169.254`.
    pub imds: String,
    /// What `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` is relative to, `http://169.254.170.2`.
    pub container: String,
    /// STS's base URL; `None` for the region's own, `https://sts.<region>.amazonaws.com`.
    pub sts: Option<String>,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            imds: "http://169.254.169.254".to_string(),
            container: "http://169.254.170.2".to_string(),
            sts: None,
        }
    }
}

/// One set of AWS credentials and where they came from. Its `Debug` shows the source only.
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expires: Option<SystemTime>,
    /// Where they came from, for messages, such as `the environment`.
    pub source: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("source", &self.source)
            .field("expires", &self.expires)
            .finish_non_exhaustive()
    }
}

/// The credential chain, shared by every connection of one sink.
pub struct Chain {
    env: Env,
    endpoints: Endpoints,
    /// STS's client, which honors `HTTPS_PROXY` and the other proxy variables.
    client: reqwest::Client,
    /// The container endpoint's and IMDS's client, which ignores every proxy setting.
    direct: reqwest::Client,
    timeout: Duration,
    cache: Mutex<Option<Cached>>,
    /// Held across a lookup, so concurrent dials wait on one.
    fetching: tokio::sync::Mutex<()>,
}

struct Cached {
    credentials: Arc<Credentials>,
    refresh_at: Instant,
}

impl Chain {
    /// The chain over the process environment and AWS's own endpoints, with every request
    /// bounded by `timeout`.
    pub fn new(timeout: Duration) -> Self {
        Self::with(process_env(), Endpoints::default(), timeout)
    }

    pub fn with(env: Env, endpoints: Endpoints, timeout: Duration) -> Self {
        Self::with_clients(env, endpoints, timeout, || {
            crate::http::client_builder(
                timeout,
                crate::http::DEFAULT_CONNECT_TIMEOUT,
                crate::http::DEFAULT_IDLE_TIMEOUT,
            )
        })
    }

    /// The chain with its HTTP clients built from `base`. The container endpoint's and IMDS's
    /// drop every proxy `base` sets or the environment names, as the Java SDK opens them with
    /// `Proxy.NO_PROXY`: a proxy would see the container authorization token and the
    /// credentials, and couldn't reach a link-local address anyway.
    fn with_clients(
        env: Env,
        endpoints: Endpoints,
        timeout: Duration,
        base: impl Fn() -> reqwest::ClientBuilder,
    ) -> Self {
        let build = |builder: reqwest::ClientBuilder| {
            builder.build().expect("reqwest client should build with default TLS settings")
        };
        Self {
            env,
            endpoints,
            client: build(base()),
            direct: build(base().no_proxy()),
            timeout,
            cache: Mutex::new(None),
            fetching: tokio::sync::Mutex::new(()),
        }
    }

    /// The variable `name`, unless it's unset or blank.
    pub fn var(&self, name: &str) -> Option<String> {
        (self.env)(name).filter(|value| !value.trim().is_empty())
    }

    /// Cached credentials, or the chain's answer once they're due for refresh. `region` is the
    /// broker's, where STS is asked.
    pub async fn credentials(&self, region: &str) -> anyhow::Result<Arc<Credentials>> {
        let _fetching = self.fetching.lock().await;
        if let Some(cached) = self.cache.lock().unwrap().as_ref() {
            if Instant::now() < cached.refresh_at {
                return Ok(cached.credentials.clone());
            }
        }
        let credentials = Arc::new(self.resolve(region).await?);
        let reuse = credentials.expires.and_then(|expires| {
            expires.duration_since(SystemTime::now()).ok()?.checked_sub(REFRESH_MARGIN)
        });
        *self.cache.lock().unwrap() = reuse.map(|reuse| Cached {
            credentials: credentials.clone(),
            refresh_at: Instant::now() + reuse,
        });
        Ok(credentials)
    }

    /// Drops `credentials` from the cache, unless another connection has replaced them already.
    pub fn forget(&self, credentials: &Arc<Credentials>) {
        let mut cache = self.cache.lock().unwrap();
        if cache.as_ref().is_some_and(|cached| Arc::ptr_eq(&cached.credentials, credentials)) {
            *cache = None;
        }
    }

    /// The first source that is there, and its answer.
    async fn resolve(&self, region: &str) -> anyhow::Result<Credentials> {
        let mut absent = Vec::new();
        if let Some(found) = self.environment(&mut absent)? {
            return Ok(found);
        }
        if let Some(found) = self.web_identity(region, &mut absent).await? {
            return Ok(found);
        }
        if let Some(found) = self.profile(&mut absent).await? {
            return Ok(found);
        }
        if let Some(found) = self.container(&mut absent).await? {
            return Ok(found);
        }
        let looked = absent.join(", ");
        if self
            .var("AWS_EC2_METADATA_DISABLED")
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
        {
            return Err(anyhow!(
                "no AWS credentials: {looked}, and IMDS is off (AWS_EC2_METADATA_DISABLED)"
            ))
            .context(Fault::Refused);
        }
        self.imds().await.with_context(|| format!("no AWS credentials in {looked}; asking IMDS"))
    }

    fn environment(&self, absent: &mut Vec<String>) -> anyhow::Result<Option<Credentials>> {
        let id = self.var("AWS_ACCESS_KEY_ID");
        let secret = self.var("AWS_SECRET_ACCESS_KEY");
        match (id, secret) {
            (Some(access_key_id), Some(secret_access_key)) => Ok(Some(Credentials {
                access_key_id: access_key_id.trim().to_string(),
                secret_access_key: secret_access_key.trim().to_string(),
                session_token: self.var("AWS_SESSION_TOKEN").map(|t| t.trim().to_string()),
                expires: None,
                source: "the environment".to_string(),
            })),
            (None, None) => {
                absent.push("the environment (no AWS_ACCESS_KEY_ID)".to_string());
                Ok(None)
            }
            (Some(_), None) => Err(anyhow!(
                "AWS_ACCESS_KEY_ID is set without AWS_SECRET_ACCESS_KEY -- set both, or neither"
            ))
            .context(Fault::Refused),
            (None, Some(_)) => Err(anyhow!(
                "AWS_SECRET_ACCESS_KEY is set without AWS_ACCESS_KEY_ID -- set both, or neither"
            ))
            .context(Fault::Refused),
        }
    }

    async fn web_identity(
        &self,
        region: &str,
        absent: &mut Vec<String>,
    ) -> anyhow::Result<Option<Credentials>> {
        let (file, role) = match (self.var("AWS_WEB_IDENTITY_TOKEN_FILE"), self.var("AWS_ROLE_ARN"))
        {
            (Some(file), Some(role)) => (file, role),
            (None, None) => {
                absent.push("web identity (no AWS_WEB_IDENTITY_TOKEN_FILE)".to_string());
                return Ok(None);
            }
            (Some(_), None) => {
                return Err(anyhow!("AWS_WEB_IDENTITY_TOKEN_FILE is set without AWS_ROLE_ARN"))
                    .context(Fault::Refused)
            }
            (None, Some(_)) => {
                return Err(anyhow!("AWS_ROLE_ARN is set without AWS_WEB_IDENTITY_TOKEN_FILE"))
                    .context(Fault::Refused)
            }
        };
        let token = read_token_file(Path::new(&file), "AWS_WEB_IDENTITY_TOKEN_FILE").await?;
        let session = self.var("AWS_ROLE_SESSION_NAME").unwrap_or_else(|| {
            let millis = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis());
            format!("logit-{millis}")
        });
        let base = match &self.endpoints.sts {
            Some(base) => base.clone(),
            None => format!("https://sts.{region}.{}", partition_domain(region)?),
        };
        let url = format!("{}/", base.trim_end_matches('/'));
        let form = |k: &str, v: &str| format!("{k}={}", crate::http::form_urlencode(v));
        let body = [
            form("Action", "AssumeRoleWithWebIdentity"),
            form("Version", "2011-06-15"),
            form("RoleArn", &role),
            form("RoleSessionName", &session),
            form("WebIdentityToken", &token),
        ]
        .join("&");
        let what =
            format!("STS AssumeRoleWithWebIdentity at {} for role {role:?}", redact::url(&url));
        let request = self
            .client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body);
        let (status, body) = self.send(request, &what).await?;
        if !status.is_success() {
            let code = xml_text(&body, "Code");
            let class = match code.as_deref() {
                Some("Throttling" | "IDPCommunicationError") => Fault::Clean,
                _ => status_class(status),
            };
            let detail = match (code, xml_text(&body, "Message")) {
                (Some(code), Some(message)) => {
                    format!(": {} ({})", quoted(&code), quoted(&message))
                }
                (Some(code), None) => format!(": {}", quoted(&code)),
                _ => String::new(),
            };
            return Err(anyhow!("{what} answered {status}{detail}")).context(class);
        }
        let field = |name: &str| {
            xml_text(&body, name)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| anyhow!("{what} answered {status} without {name}"))
                .context(Fault::Refused)
        };
        Ok(Some(Credentials {
            access_key_id: field("AccessKeyId")?,
            secret_access_key: field("SecretAccessKey")?,
            session_token: Some(field("SessionToken")?),
            expires: xml_text(&body, "Expiration").as_deref().and_then(parse_expiration),
            source: format!("web identity for role {role:?}"),
        }))
    }

    async fn profile(&self, absent: &mut Vec<String>) -> anyhow::Result<Option<Credentials>> {
        let home = self.var("HOME").map(PathBuf::from);
        let path = |var: &str, file: &str| {
            self.var(var)
                .map(PathBuf::from)
                .or_else(|| home.as_ref().map(|h| h.join(".aws").join(file)))
        };
        let credentials_file = path("AWS_SHARED_CREDENTIALS_FILE", "credentials");
        let config_file = path("AWS_CONFIG_FILE", "config");
        let explicit = self.var("AWS_PROFILE");
        let name = explicit.clone().unwrap_or_else(|| "default".to_string());

        let mut found = false;
        let mut keys: HashMap<String, String> = HashMap::new();
        // The config file first, so the credentials file wins a key both set.
        for (file, config) in [(&config_file, true), (&credentials_file, false)] {
            let Some(file) = file else { continue };
            let Some(text) = read_optional(file).await? else { continue };
            if let Some(profile) = profile_keys(&text, config, &name) {
                found = true;
                keys.extend(profile);
            }
        }
        let files = [&credentials_file, &config_file]
            .iter()
            .filter_map(|f| f.as_ref().map(|f| f.display().to_string()))
            .collect::<Vec<_>>()
            .join(" or ");
        let files = if files.is_empty() { "no file (HOME is unset)".to_string() } else { files };
        if !found {
            if explicit.is_some() {
                return Err(anyhow!("AWS_PROFILE names profile {name:?}, which isn't in {files}"))
                    .context(Fault::Refused);
            }
            absent.push(format!("the shared files (no profile {name:?} in {files})"));
            return Ok(None);
        }
        if let Some(key) = UNSUPPORTED_PROFILE_KEYS.iter().find(|k| keys.contains_key(**k)) {
            return Err(anyhow!(
                "AWS profile {name:?} in {files} sets '{key}', which kafka_out doesn't support -- \
                 give the profile 'aws_access_key_id' and 'aws_secret_access_key', or use another \
                 credential source"
            ))
            .context(Fault::Refused);
        }
        let value = |key: &str| keys.get(key).filter(|v| !v.is_empty()).cloned();
        match (value("aws_access_key_id"), value("aws_secret_access_key")) {
            (Some(access_key_id), Some(secret_access_key)) => Ok(Some(Credentials {
                access_key_id,
                secret_access_key,
                session_token: value("aws_session_token"),
                expires: None,
                source: format!("profile {name:?} in {files}"),
            })),
            // `aws configure` writes a `[default]` holding only `region`, so only an implicit
            // `default` without keys counts as absent.
            (None, None) if explicit.is_some() => Err(anyhow!(
                "AWS_PROFILE names profile {name:?}, which has no 'aws_access_key_id' in {files}"
            ))
            .context(Fault::Refused),
            (None, None) => {
                absent.push(format!("the shared files (profile {name:?} has no keys)"));
                Ok(None)
            }
            _ => Err(anyhow!(
                "AWS profile {name:?} in {files} sets one of 'aws_access_key_id' and \
                 'aws_secret_access_key' without the other"
            ))
            .context(Fault::Refused),
        }
    }

    async fn container(&self, absent: &mut Vec<String>) -> anyhow::Result<Option<Credentials>> {
        let url = if let Some(relative) = self.var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
            format!("{}{}", self.endpoints.container.trim_end_matches('/'), relative.trim())
        } else if let Some(full) = self.var("AWS_CONTAINER_CREDENTIALS_FULL_URI") {
            let full = full.trim().to_string();
            if !is_allowed_container_uri(&full) {
                return Err(anyhow!(
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI ({}) must be 'https://', or 'http://' to \
                     a loopback address, 169.254.170.2, 169.254.170.23, or fd00:ec2::23",
                    redact::url(&full)
                ))
                .context(Fault::Refused);
            }
            full
        } else {
            absent.push(
                "the container endpoint (no AWS_CONTAINER_CREDENTIALS_RELATIVE_URI or \
                         AWS_CONTAINER_CREDENTIALS_FULL_URI)"
                    .to_string(),
            );
            return Ok(None);
        };
        // The Java SDK takes the variable over the file when both are set.
        let token = match self.var("AWS_CONTAINER_AUTHORIZATION_TOKEN") {
            Some(token) => Some(token.trim().to_string()),
            None => match self.var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
                Some(file) => Some(
                    read_token_file(Path::new(&file), "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
                        .await?,
                ),
                None => None,
            },
        };
        let what = format!("the container credential endpoint {}", redact::url(&url));
        let mut request = self.direct.get(&url);
        if let Some(token) = token {
            request = request.header(reqwest::header::AUTHORIZATION, token);
        }
        let (status, body) = self.send(request, &what).await?;
        if !status.is_success() {
            return Err(anyhow!("{what} answered {status}")).context(status_class(status));
        }
        credentials_json(&body, &format!("{what} answered {status}"), "the container endpoint")
            .map(Some)
    }

    async fn imds(&self) -> anyhow::Result<Credentials> {
        let base = self.endpoints.imds.trim_end_matches('/');
        let what = format!("IMDS at {base}");
        let request = self
            .direct
            .put(format!("{base}/latest/api/token"))
            .header("x-aws-ec2-metadata-token-ttl-seconds", IMDS_TOKEN_TTL_SECONDS);
        let (status, token) = self.send(request, &what).await?;
        if !status.is_success() {
            return Err(anyhow!("{what} answered {status} to the session token request"))
                .context(status_class(status));
        }
        let token = String::from_utf8_lossy(&token).trim().to_string();
        let roles_url = format!("{base}/latest/meta-data/iam/security-credentials/");
        let (status, roles) = self
            .send(self.direct.get(&roles_url).header("x-aws-ec2-metadata-token", &token), &what)
            .await?;
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(anyhow!("{what} has no IAM role for this instance (404)"))
                .context(Fault::Refused);
        }
        if !status.is_success() {
            return Err(anyhow!("{what} answered {status} to the role request"))
                .context(status_class(status));
        }
        let roles = String::from_utf8_lossy(&roles);
        let Some(role) = roles.lines().map(str::trim).find(|line| !line.is_empty()) else {
            return Err(anyhow!("{what} lists no IAM role for this instance"))
                .context(Fault::Refused);
        };
        let request = self
            .direct
            .get(format!("{roles_url}{role}"))
            .header("x-aws-ec2-metadata-token", &token);
        let (status, body) = self.send(request, &what).await?;
        if !status.is_success() {
            return Err(anyhow!("{what} answered {status} for role {role:?}"))
                .context(status_class(status));
        }
        credentials_json(
            &body,
            &format!("{what} answered {status} for role {role:?}"),
            &format!("IMDS role {role:?}"),
        )
    }

    /// Sends `request` and reads its answer, up to [`MAX_BODY_BYTES`]. A transport failure or a
    /// body cut off is `Clean`.
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        what: &str,
    ) -> anyhow::Result<(reqwest::StatusCode, Vec<u8>)> {
        let transport = |err: reqwest::Error| {
            // reqwest's connect timeout also sets `is_timeout`, so the dial is checked first.
            let connect_timed_out = err.is_connect() && err.is_timeout();
            let timed_out = err.is_timeout();
            let err = anyhow::Error::new(err.without_url());
            if connect_timed_out {
                err.context(format!(
                    "{what} didn't connect within {:?}",
                    crate::http::DEFAULT_CONNECT_TIMEOUT
                ))
            } else if timed_out {
                err.context(format!("{what} timed out after {:?}", self.timeout))
            } else {
                err.context(format!("asking {what}"))
            }
            .context(Fault::Clean)
        };
        let response = request.timeout(self.timeout).send().await.map_err(transport)?;
        let status = response.status();
        let body =
            crate::http::try_read_body_bytes(response, MAX_BODY_BYTES).await.map_err(transport)?;
        Ok((status, body))
    }
}

/// `429` and `5xx` are `Clean`; any other status is `Refused`.
fn status_class(status: reqwest::StatusCode) -> Fault {
    if status.as_u16() == 429 || status.is_server_error() {
        Fault::Clean
    } else {
        Fault::Refused
    }
}

/// The commercial `aws` partition's region prefixes, from its `regionRegex` in the AWS SDKs'
/// partition metadata.
const COMMERCIAL_PREFIXES: &[&str] = &["us", "eu", "ap", "sa", "ca", "me", "af", "il", "mx"];

/// The domain a region's endpoints are under, for the three partitions the chain supports:
/// `amazonaws.com.cn` for `aws-cn` (`cn-`), `amazonaws.com` for `aws-us-gov` (`us-gov-`) and
/// `aws`. Any other region, such as an ISO partition's (`us-iso-east-1`) or the European
/// Sovereign Cloud's (`eusc-de-east-1`, under `amazonaws.eu`), is `Refused`.
pub(crate) fn partition_domain(region: &str) -> anyhow::Result<&'static str> {
    // ISO regions share the `us` and `eu` prefixes.
    let iso = region.split('-').any(|label| label.starts_with("iso"));
    match region.split('-').next().unwrap_or_default() {
        "cn" => Ok("amazonaws.com.cn"),
        prefix if !iso && COMMERCIAL_PREFIXES.contains(&prefix) => Ok("amazonaws.com"),
        _ => Err(anyhow!(
            "region {region:?} isn't in a partition kafka_out supports for STS: aws, aws-cn, or \
             aws-us-gov"
        ))
        .context(Fault::Refused),
    }
}

/// A full container credentials URI the Java SDK accepts: `https` to any host, or `http` to a
/// loopback address, ECS's `169.254.170.2`, or EKS Pod Identity's `169.254.170.23` and
/// `fd00:ec2::23`. A host name other than `localhost` isn't resolved.
fn is_allowed_container_uri(uri: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(uri) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    match url.scheme() {
        "https" => true,
        "http" => {
            let host = host.trim_start_matches('[').trim_end_matches(']');
            host.eq_ignore_ascii_case("localhost")
                || host.parse::<std::net::IpAddr>().is_ok_and(|ip| {
                    ip.is_loopback()
                        || ip == std::net::Ipv4Addr::new(169, 254, 170, 2)
                        || ip == std::net::Ipv4Addr::new(169, 254, 170, 23)
                        || ip == std::net::Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x23)
                })
        }
        _ => false,
    }
}

/// The container endpoint's and IMDS's JSON: `AccessKeyId` and `SecretAccessKey`, with
/// `Token` and `Expiration` optional, as the Java SDK's `HttpCredentialsLoader` reads it.
fn credentials_json(body: &[u8], what: &str, source: &str) -> anyhow::Result<Credentials> {
    let json: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| anyhow!("{what} with a body that isn't JSON"))
        .context(Fault::Refused)?;
    let field = |name: &str| json.get(name).and_then(|v| v.as_str()).filter(|v| !v.is_empty());
    let (Some(access_key_id), Some(secret_access_key)) =
        (field("AccessKeyId"), field("SecretAccessKey"))
    else {
        return Err(anyhow!("{what} without an AccessKeyId and a SecretAccessKey"))
            .context(Fault::Refused);
    };
    Ok(Credentials {
        access_key_id: access_key_id.to_string(),
        secret_access_key: secret_access_key.to_string(),
        session_token: field("Token").map(String::from),
        expires: field("Expiration").and_then(parse_expiration),
        source: source.to_string(),
    })
}

/// An RFC 3339 expiry, with the `+0000` some endpoints write read as `Z`.
fn parse_expiration(text: &str) -> Option<SystemTime> {
    let text = text.trim();
    let text = match text.strip_suffix("+0000") {
        Some(rest) => format!("{rest}Z"),
        None => text.to_string(),
    };
    let nanos = logit_core::time::parse_rfc3339_to_nanos(&text).ok()?;
    let nanos = u64::try_from(nanos).ok()?;
    Some(UNIX_EPOCH + Duration::from_nanos(nanos))
}

/// A token from the file `var` names, trimmed. A file that can't be read, is empty, or is over
/// [`MAX_BODY_BYTES`] is `Refused`.
async fn read_token_file(path: &Path, var: &str) -> anyhow::Result<String> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("reading {var} ({})", path.display()))
        .context(Fault::Refused)?;
    let token = String::from_utf8_lossy(&bytes).trim().to_string();
    if token.is_empty() || bytes.len() > MAX_BODY_BYTES {
        return Err(anyhow!(
            "{var} ({}) is empty or over {} KiB",
            path.display(),
            MAX_BODY_BYTES / 1024
        ))
        .context(Fault::Refused);
    }
    Ok(token)
}

/// A shared file's text, or `None` when it doesn't exist. Any other read error is `Refused`.
async fn read_optional(path: &Path) -> anyhow::Result<Option<String>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(anyhow::Error::new(err))
            .with_context(|| format!("reading {}", path.display()))
            .context(Fault::Refused),
    }
}

/// The keys of profile `name` in a shared file's `text`, or `None` when it has no such section.
/// In the config file (`config`) a profile's section is `[profile <name>]`, or `[default]` for
/// the default profile, which `[profile default]` overrides; in the credentials file it's
/// `[<name>]`. The parser follows the Java SDK's: a line starting with `#` or `;` is a comment,
/// a property's ` #` or ` ;` starts one, an indented line continues a property and is skipped
/// here, a later key wins, and keys are case-sensitive.
pub(crate) fn profile_keys(
    text: &str,
    config: bool,
    name: &str,
) -> Option<HashMap<String, String>> {
    // Each matching section's keys by rank: 1 for `[default]` in the config file, 2 for every
    // other match. The highest rank present wins, and its sections merge in file order.
    let mut sections: Vec<(u8, HashMap<String, String>)> = Vec::new();
    let mut current: Option<usize> = None;
    for raw in text.lines() {
        if raw.starts_with([' ', '\t']) {
            continue;
        }
        let line = raw.trim();
        if line.is_empty() || line.starts_with(['#', ';']) {
            continue;
        }
        if let Some(rest) = line.strip_prefix('[') {
            current = None;
            let Some((section, _)) = rest.split_once(']') else { continue };
            let section = section.trim();
            let rank = if config {
                match section.strip_prefix("profile ") {
                    Some(profile) if profile.trim() == name => Some(2),
                    None if section == "default" && name == "default" => Some(1),
                    _ => None,
                }
            } else {
                (section == name).then_some(2)
            };
            if let Some(rank) = rank {
                sections.push((rank, HashMap::new()));
                current = Some(sections.len() - 1);
            }
            continue;
        }
        let Some(at) = current else { continue };
        let Some((key, value)) = line.split_once('=') else { continue };
        let mut value = value;
        for marker in [" #", " ;", "\t#", "\t;"] {
            if let Some(cut) = value.find(marker) {
                value = &value[..cut];
            }
        }
        sections[at].1.insert(key.trim().to_string(), value.trim().to_string());
    }
    let top = sections.iter().map(|(rank, _)| *rank).max()?;
    let mut keys = HashMap::new();
    for (rank, section) in sections {
        if rank == top {
            keys.extend(section);
        }
    }
    Some(keys)
}

/// The text of the first `<tag>` element in an XML `body`, entity references decoded. Enough
/// for STS's answers, whose fields are leaf elements with no attributes.
pub(crate) fn xml_text(body: &[u8], tag: &str) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let open = format!("<{tag}>");
    let start = text.find(&open)? + open.len();
    let end = start + text[start..].find(&format!("</{tag}>"))?;
    let raw = &text[start..end];
    if raw.contains('<') {
        return None;
    }
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        let semi = rest[at..].find(';')?;
        let entity = &rest[at + 1..at + semi];
        let ch = match entity {
            "amp" => '&',
            "lt" => '<',
            "gt" => '>',
            "quot" => '"',
            "apos" => '\'',
            _ => {
                let code = match entity.strip_prefix("#x").or_else(|| entity.strip_prefix("#X")) {
                    Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                    None => entity.strip_prefix('#')?.parse().ok()?,
                };
                char::from_u32(code)?
            }
        };
        out.push(ch);
        rest = &rest[at + semi + 1..];
    }
    out.push_str(rest);
    Some(out.trim().to_string())
}

/// `value`, cut to [`MAX_QUOTED`] bytes on a character boundary, in quotes.
fn quoted(value: &str) -> String {
    let mut end = value.len().min(MAX_QUOTED);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    let ellipsis = if end < value.len() { "..." } else { "" };
    format!("{:?}{ellipsis}", &value[..end])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use logit_pipeline::test_util::{scratch_dir, RECV_TIMEOUT};

    use super::*;
    use crate::test_stub::{HttpRequest, HttpStub};

    const SECRET: &str = "secret-key-never-printed";
    const SESSION: &str = "session-token-never-printed";
    const WEB_TOKEN: &str = "web-identity-token-never-printed";

    /// An [`Env`] over `vars`.
    fn env(vars: &[(&str, &str)]) -> Env {
        let vars: BTreeMap<String, String> =
            vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Arc::new(move |name| vars.get(name).cloned())
    }

    /// Endpoints that answer nothing: IMDS and the container base are a closed port.
    fn closed() -> Endpoints {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let base = format!("http://127.0.0.1:{port}");
        Endpoints { imds: base.clone(), container: base.clone(), sts: Some(base) }
    }

    fn chain(vars: &[(&str, &str)], endpoints: Endpoints) -> Chain {
        Chain::with(env(vars), endpoints, RECV_TIMEOUT)
    }

    /// The error a lookup fails with, after asserting its class and that it carries no secret.
    async fn failure(chain: &Chain, want: Fault) -> String {
        let err = chain.credentials("us-east-1").await.unwrap_err();
        let message = format!("{err:#} {err:?}");
        assert_eq!(logit_pipeline::classify(&err), want, "{message}");
        for secret in [SECRET, SESSION, WEB_TOKEN] {
            assert!(!message.contains(secret), "{message}");
        }
        message
    }

    /// The container and IMDS credential JSON, expiring `expires_in` from now.
    fn credential_json(n: usize, expires_in: Duration) -> String {
        let expires = SystemTime::now() + expires_in;
        let nanos = expires.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
        let expiration = logit_core::time::format_rfc3339_utc(nanos);
        format!(
            r#"{{"Code":"Success","AccessKeyId":"AKID{n}","SecretAccessKey":"{SECRET}",
                "Token":"{SESSION}-{n}","Expiration":"{}Z"}}"#,
            &expiration[..19]
        )
    }

    #[tokio::test]
    async fn the_environment_comes_first() {
        let chain = chain(
            &[
                ("AWS_ACCESS_KEY_ID", "AKIDENV"),
                ("AWS_SECRET_ACCESS_KEY", SECRET),
                ("AWS_SESSION_TOKEN", SESSION),
                ("AWS_WEB_IDENTITY_TOKEN_FILE", "/nonexistent"),
                ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
                ("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://10.0.0.1/creds"),
            ],
            closed(),
        );
        let found = chain.credentials("us-east-1").await.unwrap();
        assert_eq!(
            (found.access_key_id.as_str(), found.secret_access_key.as_str()),
            ("AKIDENV", SECRET)
        );
        assert_eq!(found.session_token.as_deref(), Some(SESSION));
        assert_eq!(found.source, "the environment");
        let shown = format!("{found:?}");
        assert!(!shown.contains(SECRET) && !shown.contains(SESSION), "{shown}");
    }

    #[tokio::test]
    async fn half_a_configured_source_is_refused() {
        for (vars, detail) in [
            (vec![("AWS_ACCESS_KEY_ID", "AKID")], "without AWS_SECRET_ACCESS_KEY"),
            (vec![("AWS_SECRET_ACCESS_KEY", SECRET)], "without AWS_ACCESS_KEY_ID"),
            (
                vec![("AWS_ROLE_ARN", "arn:aws:iam::1:role/r")],
                "without AWS_WEB_IDENTITY_TOKEN_FILE",
            ),
            (vec![("AWS_WEB_IDENTITY_TOKEN_FILE", "/t")], "without AWS_ROLE_ARN"),
            (
                vec![("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://10.1.2.3/creds")],
                "must be 'https://', or 'http://' to a loopback address",
            ),
        ] {
            let chain = chain(&vars, closed());
            let message = failure(&chain, Fault::Refused).await;
            assert!(message.contains(detail), "{message}");
        }
    }

    /// Web identity: the token file's token and the role go to STS's `AssumeRoleWithWebIdentity`
    /// as a form, and its XML answer's credentials come back.
    #[tokio::test]
    async fn web_identity_asks_sts_with_the_token_file() {
        let dir = scratch_dir("aws-web-identity");
        let token_file = dir.join("token");
        std::fs::write(&token_file, format!("{WEB_TOKEN}\n")).unwrap();
        let sts = HttpStub::start().await;
        sts.answer(|_, _| {
            (
                200,
                format!(
                    "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/\
                     2011-06-15/\"><AssumeRoleWithWebIdentityResult><Credentials>\
                     <SessionToken>{SESSION}</SessionToken><SecretAccessKey>{SECRET}\
                     </SecretAccessKey><Expiration>2099-10-24T23:00:23Z</Expiration>\
                     <AccessKeyId>ASIASTS</AccessKeyId></Credentials></AssumeRoleWithWebIdentity\
                     Result></AssumeRoleWithWebIdentityResponse>"
                ),
            )
        });
        let endpoints = Endpoints { sts: Some(sts.url.clone()), ..closed() };
        let chain = chain(
            &[
                ("AWS_WEB_IDENTITY_TOKEN_FILE", token_file.to_str().unwrap()),
                ("AWS_ROLE_ARN", "arn:aws:iam::123:role/logit"),
                ("AWS_ROLE_SESSION_NAME", "logit session"),
            ],
            endpoints,
        );
        let found = chain.credentials("eu-west-1").await.unwrap();
        assert_eq!(found.access_key_id, "ASIASTS");
        assert_eq!(found.secret_access_key, SECRET);
        assert_eq!(found.session_token.as_deref(), Some(SESSION));
        assert!(found.expires.is_some());
        assert_eq!(found.source, "web identity for role \"arn:aws:iam::123:role/logit\"");
        let request = sts.requests().remove(0);
        assert_eq!((request.method.as_str(), request.path.as_str()), ("POST", "/"));
        assert_eq!(
            request.body,
            format!(
                "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A\
                 123%3Arole%2Flogit&RoleSessionName=logit+session&WebIdentityToken={WEB_TOKEN}"
            )
        );
        assert_eq!(request.headers["content-type"], "application/x-www-form-urlencoded");
        assert!(!request.headers.contains_key("authorization"), "STS's call is unsigned");

        // Reused: they expire in 2099.
        chain.credentials("eu-west-1").await.unwrap();
        assert_eq!(sts.requests().len(), 1);
    }

    /// STS's error codes: throttling and an identity provider it couldn't reach are `Clean`, as
    /// are `5xx`; any other `4xx` is `Refused`. The error quotes STS's code and message.
    #[tokio::test]
    async fn sts_faults_are_clean_when_transient_and_refused_otherwise() {
        let dir = scratch_dir("aws-sts-faults");
        let token_file = dir.join("token");
        std::fs::write(&token_file, WEB_TOKEN).unwrap();
        let error = |code: &str, message: &str| {
            format!(
                "<ErrorResponse><Error><Type>Sender</Type><Code>{code}</Code><Message>{message}\
                 </Message></Error><RequestId>r</RequestId></ErrorResponse>"
            )
        };
        let cases = [
            (
                403,
                error("AccessDenied", "Not authorized to perform sts:AssumeRoleWithWebIdentity"),
                Fault::Refused,
                "\"AccessDenied\" (\"Not authorized",
            ),
            (
                400,
                error("InvalidIdentityToken", "Incorrect token audience &amp; issuer"),
                Fault::Refused,
                "(\"Incorrect token audience & issuer\")",
            ),
            (400, error("Throttling", "Rate exceeded"), Fault::Clean, "\"Throttling\""),
            (
                400,
                error("IDPCommunicationError", "unreachable"),
                Fault::Clean,
                "\"IDPCommunicationError\"",
            ),
            (503, String::new(), Fault::Clean, "503 Service Unavailable"),
            (200, "<html/>".to_string(), Fault::Refused, "without AccessKeyId"),
        ];
        for (status, body, want, detail) in cases {
            let sts = HttpStub::start().await;
            sts.answer(move |_, _| (status, body.clone()));
            let endpoints = Endpoints { sts: Some(sts.url.clone()), ..closed() };
            let chain = chain(
                &[
                    ("AWS_WEB_IDENTITY_TOKEN_FILE", token_file.to_str().unwrap()),
                    ("AWS_ROLE_ARN", "arn:aws:iam::123:role/logit"),
                ],
                endpoints,
            );
            let message = failure(&chain, want).await;
            assert!(message.contains(detail), "{status}: {message}");
            assert!(message.contains("STS AssumeRoleWithWebIdentity"), "{message}");
        }

        // A token file that isn't there is the host's misconfiguration.
        let chain = chain(
            &[
                ("AWS_WEB_IDENTITY_TOKEN_FILE", dir.join("missing").to_str().unwrap()),
                ("AWS_ROLE_ARN", "arn:aws:iam::123:role/logit"),
            ],
            closed(),
        );
        let message = failure(&chain, Fault::Refused).await;
        assert!(message.contains("reading AWS_WEB_IDENTITY_TOKEN_FILE"), "{message}");

        // STS unreachable.
        let chain = self::chain(
            &[
                ("AWS_WEB_IDENTITY_TOKEN_FILE", token_file.to_str().unwrap()),
                ("AWS_ROLE_ARN", "arn:aws:iam::123:role/logit"),
            ],
            closed(),
        );
        failure(&chain, Fault::Clean).await;
    }

    /// The shared files: `[profile x]` in the config file, `[x]` in the credentials file, the
    /// credentials file winning a key both set, and comments.
    #[tokio::test]
    async fn the_shared_files_give_a_profiles_static_keys() {
        let dir = scratch_dir("aws-profiles");
        std::fs::create_dir_all(dir.join(".aws")).unwrap();
        std::fs::write(
            dir.join(".aws/config"),
            "# comment\n[default]\nregion = us-east-1\n\n[profile logit]\n\
             aws_access_key_id = AKIDCONFIG\naws_secret_access_key = config-secret\n\
             aws_session_token = config-token ; trailing comment\ns3 =\n  max_concurrent = 1\n",
        )
        .unwrap();
        std::fs::write(
            dir.join(".aws/credentials"),
            format!(
                "[default]\n; comment\naws_access_key_id=AKIDDEFAULT\naws_secret_access_key={SECRET}\n\
                 [logit]\naws_secret_access_key = {SECRET} # comment\n"
            ),
        )
        .unwrap();
        let home = dir.to_str().unwrap();

        let found = chain(&[("HOME", home)], closed()).credentials("us-east-1").await.unwrap();
        assert_eq!(found.access_key_id, "AKIDDEFAULT");
        assert_eq!(found.secret_access_key, SECRET);
        assert_eq!(found.session_token, None);
        assert!(found.source.starts_with("profile \"default\" in "), "{}", found.source);

        let found = chain(&[("HOME", home), ("AWS_PROFILE", "logit")], closed())
            .credentials("us-east-1")
            .await
            .unwrap();
        assert_eq!(found.access_key_id, "AKIDCONFIG");
        assert_eq!(found.secret_access_key, SECRET, "the credentials file wins");
        assert_eq!(found.session_token.as_deref(), Some("config-token"));

        // AWS_SHARED_CREDENTIALS_FILE and AWS_CONFIG_FILE replace the defaults.
        std::fs::write(
            dir.join("other"),
            format!("[default]\naws_access_key_id=AKIDOTHER\naws_secret_access_key={SECRET}\n"),
        )
        .unwrap();
        let found = chain(
            &[
                ("HOME", home),
                ("AWS_SHARED_CREDENTIALS_FILE", dir.join("other").to_str().unwrap()),
                ("AWS_CONFIG_FILE", dir.join("none").to_str().unwrap()),
            ],
            closed(),
        )
        .credentials("us-east-1")
        .await
        .unwrap();
        assert_eq!(found.access_key_id, "AKIDOTHER");

        let message =
            failure(&chain(&[("HOME", home), ("AWS_PROFILE", "absent")], closed()), Fault::Refused)
                .await;
        assert!(
            message.contains("AWS_PROFILE names profile \"absent\", which isn't in"),
            "{message}"
        );
    }

    /// A keyless profile counts as absent when it's the implicit `default`, as `aws configure`
    /// writes one holding only `region`; one `AWS_PROFILE` names is refused.
    #[tokio::test]
    async fn a_keyless_profile_is_skipped_only_when_implicit() {
        let dir = scratch_dir("aws-keyless-profile");
        std::fs::create_dir_all(dir.join(".aws")).unwrap();
        std::fs::write(
            dir.join(".aws/config"),
            "[default]\nregion = us-east-1\n[profile prod]\nregion = us-west-2\n",
        )
        .unwrap();
        let home = dir.to_str().unwrap();
        let imds = HttpStub::start().await;
        imds.answer(imds_answer);

        let found = chain(&[("HOME", home)], Endpoints { imds: imds.url.clone(), ..closed() })
            .credentials("us-east-1")
            .await
            .unwrap();
        assert_eq!(found.source, "IMDS role \"logit-role\"");

        let requests = imds.requests().len();
        let chain = chain(
            &[("HOME", home), ("AWS_PROFILE", "prod")],
            Endpoints { imds: imds.url.clone(), ..closed() },
        );
        let message = failure(&chain, Fault::Refused).await;
        assert!(
            message
                .contains("AWS_PROFILE names profile \"prod\", which has no 'aws_access_key_id'"),
            "{message}"
        );
        assert_eq!(imds.requests().len(), requests, "IMDS isn't asked");
    }

    #[tokio::test]
    async fn a_profile_with_an_unsupported_source_is_refused_naming_the_key() {
        for (key, line) in [
            ("role_arn", "role_arn = arn:aws:iam::1:role/r\nsource_profile = default"),
            ("credential_process", "credential_process = /bin/creds"),
            ("sso_session", "sso_session = corp"),
            ("sso_start_url", "sso_start_url = https://corp.awsapps.com/start"),
        ] {
            let dir = scratch_dir("aws-unsupported-profile");
            std::fs::create_dir_all(dir.join(".aws")).unwrap();
            std::fs::write(
                dir.join(".aws/config"),
                format!("[profile p]\naws_access_key_id = AKID\naws_secret_access_key = {SECRET}\n{line}\n"),
            )
            .unwrap();
            let chain = chain(&[("HOME", dir.to_str().unwrap()), ("AWS_PROFILE", "p")], closed());
            let message = failure(&chain, Fault::Refused).await;
            assert!(
                message.contains(&format!("sets '{key}', which kafka_out doesn't support")),
                "{message}"
            );
        }
    }

    /// The container endpoint: the relative URI under its base, or a full URI to a loopback
    /// host, with the authorization token from the variable or its file.
    #[tokio::test]
    async fn the_container_endpoint_is_asked_with_its_token() {
        let stub = HttpStub::start().await;
        stub.answer(|_, n| (200, credential_json(n, Duration::from_secs(3600))));
        let endpoints = Endpoints { container: stub.url.clone(), ..closed() };
        let found =
            chain(&[("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/abc")], endpoints)
                .credentials("us-east-1")
                .await
                .unwrap();
        assert_eq!(found.access_key_id, "AKID0");
        assert_eq!(found.session_token.as_deref(), Some(&*format!("{SESSION}-0")));
        assert_eq!(found.source, "the container endpoint");
        let request = stub.requests().remove(0);
        assert_eq!(
            (request.method.as_str(), request.path.as_str()),
            ("GET", "/v2/credentials/abc")
        );
        assert!(!request.headers.contains_key("authorization"));

        let dir = scratch_dir("aws-container-token");
        std::fs::write(dir.join("token"), "file-token\n").unwrap();
        let full = format!("{}/v1/credentials", stub.url);
        let file = dir.join("token");
        for (vars, want) in [
            (
                vec![("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", file.to_str().unwrap())],
                "file-token",
            ),
            (
                vec![
                    ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "variable-token"),
                    ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", file.to_str().unwrap()),
                ],
                "variable-token",
            ),
        ] {
            let mut vars = vars;
            vars.push(("AWS_CONTAINER_CREDENTIALS_FULL_URI", &full));
            chain(&vars, closed()).credentials("us-east-1").await.unwrap();
            let request = stub.requests().pop().unwrap();
            assert_eq!(request.path, "/v1/credentials");
            assert_eq!(request.headers["authorization"], want);
        }
    }

    #[tokio::test]
    async fn container_endpoint_faults_are_clean_when_transient_and_refused_otherwise() {
        for (status, body, want) in [
            (500, "", Fault::Clean),
            (429, "", Fault::Clean),
            (403, "forbidden", Fault::Refused),
            (200, "not json", Fault::Refused),
            (200, r#"{"AccessKeyId":"AKID"}"#, Fault::Refused),
        ] {
            let stub = HttpStub::start().await;
            stub.answer(move |_, _| (status, body.to_string()));
            let full = format!("{}/creds", stub.url);
            let chain = chain(&[("AWS_CONTAINER_CREDENTIALS_FULL_URI", &full)], closed());
            let message = failure(&chain, want).await;
            assert!(message.contains("the container credential endpoint"), "{message}");
        }
        let chain = chain(&[("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/creds")], closed());
        failure(&chain, Fault::Clean).await;
    }

    /// The container endpoint and IMDS are reached directly with a proxy configured, so the
    /// proxy sees neither the container authorization token nor the credentials. STS's request
    /// goes through it, which shows the proxy is in effect.
    #[tokio::test]
    async fn the_container_endpoint_and_imds_bypass_a_proxy() {
        let proxy = HttpStub::start().await;
        let proxied = |vars: &[(&str, &str)], endpoints: Endpoints| {
            let url = proxy.url.clone();
            Chain::with_clients(env(vars), endpoints, RECV_TIMEOUT, move || {
                crate::http::client_builder(
                    RECV_TIMEOUT,
                    crate::http::DEFAULT_CONNECT_TIMEOUT,
                    crate::http::DEFAULT_IDLE_TIMEOUT,
                )
                .proxy(reqwest::Proxy::all(&url).unwrap())
            })
        };

        let imds = HttpStub::start().await;
        imds.answer(imds_answer);
        let found = proxied(&[], Endpoints { imds: imds.url.clone(), ..closed() })
            .credentials("us-east-1")
            .await
            .unwrap();
        assert_eq!(found.source, "IMDS role \"logit-role\"");
        assert_eq!(imds.requests().len(), 3);

        let container = HttpStub::start().await;
        container.answer(|_, n| (200, credential_json(n, Duration::from_secs(3600))));
        let vars = [
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/creds"),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "container-token"),
        ];
        let found = proxied(&vars, Endpoints { container: container.url.clone(), ..closed() })
            .credentials("us-east-1")
            .await
            .unwrap();
        assert_eq!(found.source, "the container endpoint");
        assert_eq!(container.requests()[0].headers["authorization"], "container-token");
        assert!(proxy.requests().is_empty(), "{:?}", proxy.requests());

        let dir = scratch_dir("aws-proxy");
        let token_file = dir.join("token");
        std::fs::write(&token_file, WEB_TOKEN).unwrap();
        let vars = [
            ("AWS_WEB_IDENTITY_TOKEN_FILE", token_file.to_str().unwrap()),
            ("AWS_ROLE_ARN", "arn:aws:iam::123:role/logit"),
        ];
        let endpoints = Endpoints { sts: Some("http://sts.example".to_string()), ..closed() };
        failure(&proxied(&vars, endpoints), Fault::Refused).await;
        assert_eq!(proxy.requests()[0].headers["host"], "sts.example");
    }

    /// Web identity outside the supported partitions is refused before STS is asked, rather
    /// than asking an `amazonaws.com` host that doesn't exist.
    #[tokio::test]
    async fn web_identity_outside_the_supported_partitions_is_refused() {
        let dir = scratch_dir("aws-partition");
        let token_file = dir.join("token");
        std::fs::write(&token_file, WEB_TOKEN).unwrap();
        let chain = chain(
            &[
                ("AWS_WEB_IDENTITY_TOKEN_FILE", token_file.to_str().unwrap()),
                ("AWS_ROLE_ARN", "arn:aws:iam::123:role/logit"),
            ],
            Endpoints { sts: None, ..closed() },
        );
        let err = chain.credentials("eusc-de-east-1").await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{err:#}");
        assert!(format!("{err:#}").contains("\"eusc-de-east-1\" isn't in a partition"), "{err:#}");
    }

    #[test]
    fn a_full_container_uri_is_https_or_a_known_local_host() {
        for uri in [
            "https://creds.example/x",
            "http://127.0.0.1:8080/x",
            "http://localhost/x",
            "http://[::1]/x",
            "http://169.254.170.2/x",
            "http://169.254.170.23/v1/credentials",
            "http://[fd00:ec2::23]/v1/credentials",
        ] {
            assert!(is_allowed_container_uri(uri), "{uri}");
        }
        for uri in ["http://10.0.0.1/x", "http://creds.example/x", "ftp://127.0.0.1/x", "nonsense"]
        {
            assert!(!is_allowed_container_uri(uri), "{uri}");
        }
    }

    /// IMDSv2: a session token from `PUT`, then the role and its credentials with the token.
    fn imds_answer(request: &HttpRequest, _: usize) -> (u16, String) {
        match (request.method.as_str(), request.path.as_str()) {
            ("PUT", "/latest/api/token") => {
                assert_eq!(request.headers["x-aws-ec2-metadata-token-ttl-seconds"], "21600");
                (200, "imds-session".to_string())
            }
            (_, path)
                if request.headers.get("x-aws-ec2-metadata-token").map(String::as_str)
                    != Some("imds-session") =>
            {
                (401, format!("no token for {path}"))
            }
            ("GET", "/latest/meta-data/iam/security-credentials/") => {
                (200, "logit-role\n".to_string())
            }
            ("GET", "/latest/meta-data/iam/security-credentials/logit-role") => {
                (200, credential_json(0, Duration::from_secs(3600)))
            }
            _ => (404, String::new()),
        }
    }

    #[tokio::test]
    async fn imds_gives_the_instance_roles_credentials() {
        let imds = HttpStub::start().await;
        imds.answer(imds_answer);
        let chain = chain(&[], Endpoints { imds: imds.url.clone(), ..closed() });
        let found = chain.credentials("us-east-1").await.unwrap();
        assert_eq!(found.access_key_id, "AKID0");
        assert_eq!(found.source, "IMDS role \"logit-role\"");
        let paths: Vec<String> =
            imds.requests().iter().map(|r| format!("{} {}", r.method, r.path)).collect();
        assert_eq!(
            paths,
            [
                "PUT /latest/api/token",
                "GET /latest/meta-data/iam/security-credentials/",
                "GET /latest/meta-data/iam/security-credentials/logit-role",
            ]
        );
    }

    #[tokio::test]
    async fn imds_faults_and_no_source_at_all() {
        // No role on the instance.
        let imds = HttpStub::start().await;
        imds.answer(|r, _| match r.method.as_str() {
            "PUT" => (200, "imds-session".to_string()),
            _ => (404, String::new()),
        });
        let message =
            failure(&chain(&[], Endpoints { imds: imds.url.clone(), ..closed() }), Fault::Refused)
                .await;
        assert!(message.contains("has no IAM role"), "{message}");

        let imds = HttpStub::start().await;
        imds.answer(|_, _| (503, String::new()));
        failure(&chain(&[], Endpoints { imds: imds.url.clone(), ..closed() }), Fault::Clean).await;

        // IMDS unreachable: `Clean`, and the message lists every source looked for.
        let message = failure(&chain(&[], closed()), Fault::Clean).await;
        for looked in [
            "the environment (no AWS_ACCESS_KEY_ID)",
            "web identity (no AWS_WEB_IDENTITY_TOKEN_FILE)",
            "the shared files (no profile \"default\" in no file (HOME is unset))",
            "the container endpoint (no AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "asking IMDS",
        ] {
            assert!(message.contains(looked), "{looked}: {message}");
        }

        // IMDS off, and nothing else: `Refused`.
        let message =
            failure(&chain(&[("AWS_EC2_METADATA_DISABLED", "TRUE")], closed()), Fault::Refused)
                .await;
        assert!(message.contains("no AWS credentials: the environment"), "{message}");
        assert!(message.contains("IMDS is off (AWS_EC2_METADATA_DISABLED)"), "{message}");
    }

    /// Credentials with an expiry are reused until 5 minutes before it, by every dial at once
    /// too; ones inside that margin are fetched again; forgotten ones are fetched again.
    #[tokio::test]
    async fn expiring_credentials_are_cached_until_five_minutes_before_expiry() {
        let stub = HttpStub::start().await;
        stub.answer(|_, n| (200, credential_json(n, Duration::from_secs(3600))));
        let full = format!("{}/creds", stub.url);
        let chain = Arc::new(chain(&[("AWS_CONTAINER_CREDENTIALS_FULL_URI", &full)], closed()));
        let mut lookups = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let chain = chain.clone();
            lookups.spawn(async move { chain.credentials("us-east-1").await.unwrap() });
        }
        let mut found = Vec::new();
        while let Some(done) = lookups.join_next().await {
            found.push(done.unwrap());
        }
        assert_eq!(stub.requests().len(), 1, "concurrent dials wait on one lookup");
        assert!(found.iter().all(|c| Arc::ptr_eq(c, &found[0])));

        chain.forget(&found[0]);
        assert_eq!(chain.credentials("us-east-1").await.unwrap().access_key_id, "AKID1");
        assert_eq!(stub.requests().len(), 2, "forgotten credentials are fetched again");

        // Inside the margin: used, not kept.
        let stub = HttpStub::start().await;
        stub.answer(|_, n| (200, credential_json(n, Duration::from_secs(240))));
        let full = format!("{}/creds", stub.url);
        let chain = self::chain(&[("AWS_CONTAINER_CREDENTIALS_FULL_URI", &full)], closed());
        chain.credentials("us-east-1").await.unwrap();
        chain.credentials("us-east-1").await.unwrap();
        assert_eq!(stub.requests().len(), 2);
    }

    #[test]
    fn profile_sections_follow_the_java_parser() {
        let config =
            "[profile default]\na = 2\n[default]\na = 1\n[profile x] # c\nb = 3\n[other]\nc = 4\n";
        assert_eq!(profile_keys(config, true, "default").unwrap()["a"], "2");
        assert_eq!(profile_keys("[default]\na = 1\n", true, "default").unwrap()["a"], "1");
        assert_eq!(profile_keys(config, true, "x").unwrap()["b"], "3");
        assert!(profile_keys(config, true, "other").is_none(), "no 'profile ' prefix");
        assert_eq!(profile_keys(config, false, "other").unwrap()["c"], "4");
        assert_eq!(profile_keys("[p]\nk = a#b ;c\n", false, "p").unwrap()["k"], "a#b");
    }

    #[test]
    fn xml_text_reads_a_leaf_element() {
        let body = b"<R><Code>A&amp;B&#65;&#x42;</Code><Message> m </Message><N><X>1</X></N></R>";
        assert_eq!(xml_text(body, "Code").as_deref(), Some("A&BAB"));
        assert_eq!(xml_text(body, "Message").as_deref(), Some("m"));
        assert_eq!(xml_text(body, "N"), None);
        assert_eq!(xml_text(body, "Missing"), None);
        assert_eq!(xml_text(b"<Code>a &bogus; b</Code>", "Code"), None);
    }

    #[test]
    fn an_expiration_reads_rfc3339_and_a_plus_zero_offset() {
        let want = UNIX_EPOCH + Duration::from_secs(1_791_766_923);
        assert_eq!(parse_expiration("2026-10-12T01:02:03Z"), Some(want));
        assert_eq!(parse_expiration("2026-10-12T01:02:03+0000"), Some(want));
        assert_eq!(parse_expiration("2026-10-12T01:02:03.000Z"), Some(want));
        assert_eq!(parse_expiration("tomorrow"), None);
    }

    #[test]
    fn a_regions_partition_gives_its_domain_or_is_refused() {
        for (region, want) in [
            ("us-east-1", "amazonaws.com"),
            ("ap-southeast-7", "amazonaws.com"),
            ("mx-central-1", "amazonaws.com"),
            ("cn-northwest-1", "amazonaws.com.cn"),
            ("us-gov-west-1", "amazonaws.com"),
        ] {
            assert_eq!(partition_domain(region).unwrap(), want, "{region}");
        }
        for region in
            ["eusc-de-east-1", "us-iso-east-1", "us-isob-east-1", "eu-isoe-west-1", "xx-east-1"]
        {
            let err = partition_domain(region).unwrap_err();
            assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{region}");
            let message = format!("{err:#}");
            assert!(message.contains(&format!("{region:?}")), "{message}");
            assert!(message.contains("aws, aws-cn, or aws-us-gov"), "{message}");
        }
    }
}
