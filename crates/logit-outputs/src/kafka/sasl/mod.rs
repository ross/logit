//! SASL authentication for `kafka_out`'s broker connections (`docs/adr/kafka-sasl.md`).
//!
//! One [`Authenticator`] is built per sink from its `sasl:` block and shared by every
//! connection. After `ApiVersions`, `super::conn::Dialer::connect` runs [`authenticate`]:
//! `SaslHandshake` v1 naming the mechanism, then [`Authenticator::start`] for a [`Session`], then
//! one `SaslAuthenticate` per client message the session writes until it reports
//! [`Step::Done`]. `start` is async and takes the broker's host so a mechanism can fetch a token
//! or credentials, cache them in the authenticator across connections, or read a region from
//! the host; a session only computes.
//!
//! A failure's class follows the module doc's table (`super`, "Response classes"): a transport
//! failure during the exchange is `Clean`, because no produce byte has been written on the
//! connection, and a broker's refusal or a server message the mechanism rejects is `Refused`. A
//! session's own error is `Refused` unless it carries a class. A broker's error code on any
//! `SaslAuthenticate` reaches the session through [`Session::refused`]. Every message names the
//! mechanism and [`Authenticator::principal`], never a secret, and adds [`Session::failure`] to a
//! broker's refusal.

mod msk_iam;
mod oauth;
mod plain;
mod scram;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use bytes::Bytes;
use kafka_protocol::messages::{SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_protocol::protocol::StrBytes;
use logit_pipeline::Fault;
use tokio::time::Instant;

use super::conn::{Conn, SASL_HANDSHAKE_VERSION};
use super::fault;

#[cfg(test)]
pub(crate) use logit_aws::creds::{Endpoints, Env};
#[cfg(test)]
pub(crate) use logit_aws::sigv4::{AmzDate, Signer};
#[cfg(test)]
pub(crate) use scram::{escape_username, ScramHash};

/// A `kafka_out` sink's SASL credentials. Mirrors `logit_config::KafkaSasl`; its `Debug` leaves
/// the password, the token, and the client secret out. `AwsMskIam` holds no secret: its
/// credentials come from the AWS credential chain when a connection is dialed.
#[derive(Clone, PartialEq, Eq)]
pub enum KafkaSasl {
    Plain {
        username: String,
        password: String,
    },
    ScramSha256 {
        username: String,
        password: String,
    },
    ScramSha512 {
        username: String,
        password: String,
    },
    OAuthBearer {
        token: OAuthToken,
        /// Sent in this order.
        extensions: Vec<(String, String)>,
    },
    AwsMskIam {
        /// The region to sign for; `None` reads it from the broker's host or the environment.
        region: Option<String>,
    },
}

/// Where an `OAUTHBEARER` token comes from.
#[derive(Clone, PartialEq, Eq)]
pub enum OAuthToken {
    /// Sent as given.
    Fixed(String),
    /// Fetched with the client-credentials grant and cached across connections.
    ClientCredentials {
        endpoint: String,
        client_id: String,
        client_secret: String,
        scope: Option<String>,
    },
}

impl std::fmt::Debug for KafkaSasl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (name, username) = match self {
            KafkaSasl::Plain { username, .. } => ("Plain", username),
            KafkaSasl::ScramSha256 { username, .. } => ("ScramSha256", username),
            KafkaSasl::ScramSha512 { username, .. } => ("ScramSha512", username),
            KafkaSasl::OAuthBearer { token, extensions } => {
                let keys: Vec<&str> = extensions.iter().map(|(k, _)| k.as_str()).collect();
                return f
                    .debug_struct("OAuthBearer")
                    .field("token", token)
                    .field("extensions", &keys)
                    .finish();
            }
            KafkaSasl::AwsMskIam { region } => {
                return f.debug_struct("AwsMskIam").field("region", region).finish();
            }
        };
        f.debug_struct(name).field("username", username).field("password", &"<redacted>").finish()
    }
}

impl std::fmt::Debug for OAuthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OAuthToken::Fixed(_) => f.write_str("Fixed(<redacted>)"),
            OAuthToken::ClientCredentials { endpoint, client_id, scope, .. } => f
                .debug_struct("ClientCredentials")
                .field("endpoint", &logit_core::redact::url(endpoint))
                .field("client_id", client_id)
                .field("client_secret", &"<redacted>")
                .field("scope", scope)
                .finish(),
        }
    }
}

/// The [`Authenticator`] for `settings`. `timeout` bounds each token or credential request.
pub(crate) fn authenticator(settings: &KafkaSasl, timeout: Duration) -> Arc<dyn Authenticator> {
    match settings.clone() {
        KafkaSasl::Plain { username, password } => Arc::new(plain::Plain { username, password }),
        KafkaSasl::ScramSha256 { username, password } => {
            Arc::new(scram::Scram { hash: scram::ScramHash::Sha256, username, password })
        }
        KafkaSasl::ScramSha512 { username, password } => {
            Arc::new(scram::Scram { hash: scram::ScramHash::Sha512, username, password })
        }
        KafkaSasl::OAuthBearer { token: OAuthToken::Fixed(token), extensions } => {
            Arc::new(oauth::OAuthBearer::fixed(token, extensions))
        }
        KafkaSasl::OAuthBearer {
            token: OAuthToken::ClientCredentials { endpoint, client_id, client_secret, scope },
            extensions,
        } => Arc::new(oauth::OAuthBearer::client_credentials(
            endpoint,
            &client_id,
            &client_secret,
            scope.as_deref(),
            extensions,
            timeout,
        )),
        KafkaSasl::AwsMskIam { region } => {
            Arc::new(msk_iam::MskIam::new(region, logit_aws::creds::Chain::new(timeout)))
        }
    }
}

/// An `AWS_MSK_IAM` [`Authenticator`] whose credential chain reads `env` and asks `endpoints`.
#[cfg(test)]
pub(crate) fn msk_iam_with(
    region: Option<&str>,
    env: Env,
    endpoints: Endpoints,
    timeout: Duration,
) -> Arc<dyn Authenticator> {
    Arc::new(msk_iam::MskIam::new(
        region.map(String::from),
        logit_aws::creds::Chain::with(env, endpoints, timeout),
    ))
}

/// What a [`Session`] asks for after reading the broker's answer.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// Send this client message in the next `SaslAuthenticate`.
    Send(Vec<u8>),
    /// The mechanism has finished and the connection is authenticated.
    Done,
}

/// A SASL mechanism, shared by every connection of one sink.
#[async_trait::async_trait]
pub(crate) trait Authenticator: Send + Sync {
    /// The name `SaslHandshake` carries, such as `SCRAM-SHA-256`.
    fn mechanism(&self) -> &'static str;

    /// Who this authenticates as, for error messages, such as `user "alice"`. Never a secret.
    fn principal(&self) -> String;

    /// Attaches the sink's diagnostics, for a mechanism that reports something it recovers
    /// from, such as `OAUTHBEARER`'s failed token refresh.
    fn set_diagnostics(&self, _diag: &logit_core::Diagnostics) {}

    /// Starts one connection's exchange with the broker at `host`. An error without a fault
    /// class of its own is `Clean`.
    async fn start(&self, host: &str) -> anyhow::Result<Box<dyn Session>>;
}

/// One connection's exchange: the client's side of a mechanism's messages.
pub(crate) trait Session: Send {
    /// The first client message.
    fn first(&mut self) -> anyhow::Result<Vec<u8>>;

    /// Reads the broker's answer to the last client message.
    fn next(&mut self, server: &[u8]) -> anyhow::Result<Step>;

    /// What the session learned of a refusal before the broker failed the exchange, such as an
    /// `OAUTHBEARER` error challenge's status. Never a secret.
    fn failure(&self) -> Option<String> {
        None
    }

    /// Called when the broker fails a `SaslAuthenticate` with an error code, whether or not it
    /// sent a challenge first, so a mechanism can drop a credential the broker refused.
    fn refused(&mut self) {}
}

/// The most `SaslAuthenticate` round trips one exchange may take. SCRAM takes two; a session
/// that hasn't finished by then is broken, and the cap keeps it from looping.
const MAX_ROUNDS: usize = 8;

/// `err` with `default` as its fault class unless it already carries one.
fn classed(err: anyhow::Error, default: Fault) -> anyhow::Error {
    if err.downcast_ref::<Fault>().is_some() {
        err
    } else {
        err.context(default)
    }
}

/// Authenticates `conn` to the broker at `host:port` with `auth` at `SaslAuthenticate`
/// `version`. Returns when the session ends and its lifetime, if the broker gave it one,
/// measured from the moment before the last `SaslAuthenticate` was written: no later than the
/// broker's own start of the session.
pub(crate) async fn authenticate(
    conn: &mut Conn,
    auth: &dyn Authenticator,
    version: i16,
    host: &str,
    port: u16,
) -> anyhow::Result<Option<(Instant, Duration)>> {
    let mut who = format!("SASL {} as {}", auth.mechanism(), auth.principal());
    let broker = format!("Kafka broker {host}:{port}");
    let request =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str(auth.mechanism()));
    let handshake = conn
        .request(&request, SASL_HANDSHAKE_VERSION)
        .await
        .map_err(|err| err.error)
        .with_context(|| format!("{who}: SaslHandshake with {broker}"))
        .context(Fault::Clean)?;
    if handshake.error_code != fault::code::NONE {
        let offered: Vec<&str> = handshake.mechanisms.iter().map(|m| m.as_str()).collect();
        return Err(anyhow!(
            "{who}: {broker} answered SaslHandshake {}; it offers [{}]",
            fault::name(handshake.error_code),
            offered.join(", ")
        ))
        .context(fault::classify_sasl(handshake.error_code));
    }

    let mut session = auth
        .start(host)
        .await
        .with_context(|| format!("{who}: starting the exchange with {broker}"))
        .map_err(|err| classed(err, Fault::Clean))?;
    // `AWS_MSK_IAM` learns where its credentials came from in `start`.
    who = format!("SASL {} as {}", auth.mechanism(), auth.principal());
    let refused = |err: anyhow::Error| {
        classed(err.context(format!("{who}: the exchange with {broker}")), Fault::Refused)
    };
    let mut message = session.first().map_err(refused)?;
    for _ in 0..MAX_ROUNDS {
        let sent_at = Instant::now();
        let request = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(message));
        let response = conn
            .request(&request, version)
            .await
            .map_err(|err| err.error)
            .with_context(|| format!("{who}: SaslAuthenticate with {broker}"))
            .context(Fault::Clean)?;
        if response.error_code != fault::code::NONE {
            session.refused();
            let mut detail =
                response.error_message.as_ref().map(|m| format!(": {m}")).unwrap_or_default();
            if let Some(failure) = session.failure() {
                detail.push_str(&format!("; {failure}"));
            }
            return Err(anyhow!(
                "{who}: {broker} answered SaslAuthenticate {}{detail}",
                fault::name(response.error_code)
            ))
            .context(fault::classify_sasl(response.error_code));
        }
        match session.next(&response.auth_bytes).map_err(refused)? {
            Step::Send(next) => message = next,
            Step::Done => {
                // A lifetime of 0, or a negative one, means the broker set none.
                let lifetime = u64::try_from(response.session_lifetime_ms)
                    .ok()
                    .filter(|&ms| ms > 0)
                    .map(Duration::from_millis);
                return Ok(lifetime.map(|lifetime| (sent_at + lifetime, lifetime)));
            }
        }
    }
    Err(anyhow!("{who}: the exchange with {broker} didn't finish in {MAX_ROUNDS} round trips"))
        .context(Fault::Refused)
}
