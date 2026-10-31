//! The SASL server side of a fake broker listener: `SaslHandshake` and `SaslAuthenticate` for
//! `PLAIN`, SCRAM (RFC 5802), `OAUTHBEARER` (RFC 7628), and `AWS_MSK_IAM`.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use bytes::Bytes;
use kafka_protocol::messages::SaslAuthenticateResponse;
use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use super::State;
use crate::error::code;
use crate::sasl::{escape_username, AmzDate, ScramHash, Signer};

/// The SASL a broker requires before any request but `ApiVersions`.
#[derive(Debug, Clone)]
pub struct Sasl {
    /// The mechanisms `SaslHandshake` accepts.
    pub mechanisms: Vec<String>,
    pub username: String,
    pub password: String,
    /// What a successful `SaslAuthenticate` (v1 and later) answers as `session_lifetime_ms`; 0
    /// for no limit.
    pub session_lifetime_ms: i64,
    /// The iteration count a SCRAM server-first message asks for.
    pub iterations: u32,
    /// Answers a valid SCRAM proof with a server signature from another password, as a broker
    /// that doesn't hold the user's credential would.
    pub forge_signature: bool,
    /// The one `OAUTHBEARER` token accepted; `None` accepts any.
    pub oauth_token: Option<String>,
    /// Refuses an `OAUTHBEARER` token as Redpanda v26.2.3 does: `SASL_AUTHENTICATION_FAILED` on
    /// the first message, with no error challenge.
    pub oauth_no_challenge: bool,
    /// The one `AWS_MSK_IAM` session token accepted; `None` accepts a payload with any or none.
    /// `AWS_MSK_IAM` takes `username` as the access key id and `password` as the secret key.
    pub msk_session_token: Option<String>,
}

impl Sasl {
    /// Every mechanism `logit` speaks, for `username` and `password`, with no lifetime.
    pub fn new(username: &str, password: &str) -> Self {
        Self {
            mechanisms: ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "AWS_MSK_IAM"]
                .map(String::from)
                .to_vec(),
            username: username.to_string(),
            password: password.to_string(),
            session_lifetime_ms: 0,
            iterations: 4096,
            forge_signature: false,
            oauth_token: None,
            oauth_no_challenge: false,
            msk_session_token: None,
        }
    }
}

/// Where one connection is in its SASL exchange.
#[derive(Default)]
pub(super) enum Auth {
    #[default]
    None,
    Handshaken(String),
    /// A SCRAM server-first message is sent.
    ScramFirst {
        hash: ScramHash,
        nonce: String,
        auth_prefix: String,
    },
    /// An `OAUTHBEARER` error challenge is sent.
    OAuthRefused,
    Done {
        expires: Option<Instant>,
    },
}

/// `OAuthBearerSaslServer`'s error challenge for a token its validator refuses, as Kafka 4.2.0
/// sends it (`script/kafka-interop`'s `oauth-refused` leg).
const OAUTH_CHALLENGE: &str = r#"{"status":"invalid_token"}"#;

/// Redpanda v26.2.3's code-58 message for a refused credential (`script/kafka-interop`'s
/// `oauth-refused` and `sasl-wrong` legs).
const REDPANDA_REFUSAL: &str = "SASL authentication failed: security: Invalid credentials";

/// MSK's answer to an accepted `AWS_MSK_IAM` payload.
const MSK_ACCEPTED: &str = r#"{"version":"2020_10_22","request-id":"fake-request-id"}"#;

/// The salt every SCRAM credential here uses.
const SCRAM_SALT: &[u8] = b"fake-broker-salt";

/// The token of an `OAUTHBEARER` client message in the form Kafka's
/// `OAuthBearerClientInitialResponse` parses: `n,` an optional `a=` authzid `,`, then `\x01`-led
/// `key=value` pairs, one of them `auth=Bearer <token>`, and a closing `\x01\x01`.
fn oauth_token(message: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(message).ok()?;
    let rest = text.strip_prefix("n,")?;
    let (_authzid, rest) = rest.split_once(',')?;
    let pairs = rest.strip_prefix('\x01')?.strip_suffix("\x01\x01")?;
    pairs.split('\x01').find_map(|pair| pair.strip_prefix("auth=Bearer ")).map(String::from)
}

/// Checks an `AWS_MSK_IAM` payload's fields as `aws-msk-iam-auth`'s `AWS4SignedPayloadGenerator`
/// writes them, and its signature, recomputed for `sasl`'s access key id and secret key. The
/// reason a payload fails names no secret.
fn msk_iam_check(
    sasl: &Sasl,
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let field = |name: &str| payload.get(name).and_then(|v| v.as_str());
    let mut keys: Vec<&str> = payload.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let token = field("x-amz-security-token");
    let mut want = vec![
        "action",
        "host",
        "user-agent",
        "version",
        "x-amz-algorithm",
        "x-amz-credential",
        "x-amz-date",
        "x-amz-expires",
        "x-amz-signature",
        "x-amz-signedheaders",
    ];
    if token.is_some() {
        want.push("x-amz-security-token");
    }
    want.sort_unstable();
    if keys != want {
        return Err(format!("keys {keys:?}"));
    }
    let fixed = [
        ("version", "2020_10_22"),
        ("action", "kafka-cluster:Connect"),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-signedheaders", "host"),
        ("x-amz-expires", "900"),
    ];
    for (name, value) in fixed {
        if field(name) != Some(value) {
            return Err(format!("{name} {:?}", field(name)));
        }
    }
    if sasl.msk_session_token.is_some() && token != sasl.msk_session_token.as_deref() {
        return Err("a session token the broker didn't issue".to_string());
    }
    let credential: Vec<&str> = field("x-amz-credential").unwrap_or_default().split('/').collect();
    let [access_key_id, date, region, "kafka-cluster", "aws4_request"] = credential[..] else {
        return Err(format!("credential scope {credential:?}"));
    };
    if access_key_id != sasl.username {
        return Err("an unknown access key id".to_string());
    }
    let datetime = field("x-amz-date").unwrap_or_default();
    if !datetime.starts_with(date) || datetime.len() != 16 {
        return Err(format!("x-amz-date {datetime:?} outside scope date {date:?}"));
    }
    let signer = Signer {
        access_key_id,
        secret_access_key: &sasl.password,
        region,
        service: "kafka-cluster",
    };
    let date = AmzDate { datetime: datetime.to_string() };
    let params = signer.presign(
        field("host").unwrap_or_default(),
        &[("Action", "kafka-cluster:Connect")],
        token,
        &date,
        900,
    );
    if params.last().map(|(_, signature)| signature.as_str()) != field("x-amz-signature") {
        return Err("a signature that doesn't match".to_string());
    }
    Ok(())
}

/// One `SaslAuthenticate` step of the exchange `auth` is in.
pub(super) fn sasl_authenticate(
    state: &mut State,
    auth: &mut Auth,
    message: &[u8],
) -> SaslAuthenticateResponse {
    let fail = |code: i16, why: &str| {
        SaslAuthenticateResponse::default()
            .with_error_code(code)
            .with_error_message(Some(StrBytes::from_string(why.to_string())))
    };
    let Some(sasl) = state.sasl.clone() else {
        return fail(code::ILLEGAL_SASL_STATE, "no SASL on this listener");
    };
    let done = |state: &mut State, auth: &mut Auth, server: Vec<u8>| {
        let lifetime = u64::try_from(sasl.session_lifetime_ms).unwrap_or(0);
        let expires =
            (lifetime > 0).then(|| Instant::now() + std::time::Duration::from_millis(lifetime));
        state.authentications += 1;
        state.session_ends.extend(expires);
        *auth = Auth::Done { expires };
        SaslAuthenticateResponse::default()
            .with_auth_bytes(Bytes::from(server))
            .with_session_lifetime_ms(sasl.session_lifetime_ms)
    };
    // Kafka's own refusal text for SCRAM.
    let scram_failed = |hash: ScramHash| {
        fail(
            code::SASL_AUTHENTICATION_FAILED,
            &format!(
                "Authentication failed during authentication due to invalid credentials with \
                 SASL mechanism {}",
                hash.mechanism()
            ),
        )
    };
    match std::mem::take(auth) {
        Auth::Handshaken(mechanism) if mechanism == "OAUTHBEARER" => {
            state.oauth_messages.push(message.to_vec());
            let token = oauth_token(message);
            let accepted = token.is_some_and(|token| {
                sasl.oauth_token.as_ref().is_none_or(|expected| *expected == token)
            });
            if accepted {
                return done(state, auth, Vec::new());
            }
            if sasl.oauth_no_challenge {
                return fail(code::SASL_AUTHENTICATION_FAILED, REDPANDA_REFUSAL);
            }
            *auth = Auth::OAuthRefused;
            SaslAuthenticateResponse::default().with_auth_bytes(Bytes::from(OAUTH_CHALLENGE))
        }
        Auth::OAuthRefused if message != [0x01] => {
            fail(code::ILLEGAL_SASL_STATE, "expected a kvsep after the error challenge")
        }
        // Kafka 4.2.0 fails the exchange with the challenge as its message.
        Auth::OAuthRefused => fail(code::SASL_AUTHENTICATION_FAILED, OAUTH_CHALLENGE),
        Auth::Handshaken(mechanism) if mechanism == "AWS_MSK_IAM" => {
            let parsed = serde_json::from_slice::<serde_json::Value>(message).ok();
            let Some(serde_json::Value::Object(payload)) = parsed else {
                return fail(code::SASL_AUTHENTICATION_FAILED, "Invalid authentication payload");
            };
            state.msk_payloads.push(payload.clone());
            match msk_iam_check(&sasl, &payload) {
                Ok(()) => done(state, auth, MSK_ACCEPTED.as_bytes().to_vec()),
                Err(why) => fail(
                    code::SASL_AUTHENTICATION_FAILED,
                    &format!("[fake-request-id]: Access denied ({why})"),
                ),
            }
        }
        Auth::Handshaken(mechanism) if mechanism == "PLAIN" => {
            let parts: Vec<&[u8]> = message.split(|&b| b == 0).collect();
            if parts.len() == 3
                && parts[1] == sasl.username.as_bytes()
                && parts[2] == sasl.password.as_bytes()
            {
                done(state, auth, Vec::new())
            } else {
                fail(
                    code::SASL_AUTHENTICATION_FAILED,
                    "Authentication failed: Invalid username or password",
                )
            }
        }
        Auth::Handshaken(mechanism) => {
            let hash = match mechanism.as_str() {
                "SCRAM-SHA-256" => ScramHash::Sha256,
                "SCRAM-SHA-512" => ScramHash::Sha512,
                _ => return fail(code::ILLEGAL_SASL_STATE, "an unknown mechanism"),
            };
            let first = String::from_utf8_lossy(message).to_string();
            let Some(bare) = first.strip_prefix("n,,") else {
                return scram_failed(hash);
            };
            let (user, client_nonce) = match bare.split_once(",r=") {
                Some((user, nonce)) => (user.strip_prefix("n=").unwrap_or_default(), nonce),
                None => return scram_failed(hash),
            };
            if user != escape_username(&sasl.username) {
                return scram_failed(hash);
            }
            let nonce = format!("{client_nonce}fake-server-nonce");
            let server_first =
                format!("r={nonce},s={},i={}", BASE64.encode(SCRAM_SALT), sasl.iterations);
            *auth = Auth::ScramFirst { hash, nonce, auth_prefix: format!("{bare},{server_first}") };
            SaslAuthenticateResponse::default().with_auth_bytes(Bytes::from(server_first))
        }
        Auth::ScramFirst { hash, nonce, auth_prefix } => {
            let last = String::from_utf8_lossy(message).to_string();
            let Some((without_proof, proof)) = last.rsplit_once(",p=") else {
                return scram_failed(hash);
            };
            if without_proof != format!("c=biws,r={nonce}") {
                return scram_failed(hash);
            }
            let Ok(proof) = BASE64.decode(proof) else {
                return scram_failed(hash);
            };
            let auth_message = format!("{auth_prefix},{without_proof}");
            let salted = hash.salted_password(&sasl.password, SCRAM_SALT, sasl.iterations);
            let stored_key = hash.hash(&hash.hmac(&salted, b"Client Key"));
            let signature = hash.hmac(&stored_key, auth_message.as_bytes());
            let client_key: Vec<u8> = proof.iter().zip(&signature).map(|(p, s)| p ^ s).collect();
            if proof.len() != signature.len() || hash.hash(&client_key) != stored_key {
                return scram_failed(hash);
            }
            let signing = if sasl.forge_signature {
                hash.salted_password(
                    &format!("{}-forged", sasl.password),
                    SCRAM_SALT,
                    sasl.iterations,
                )
            } else {
                salted
            };
            let server_key = hash.hmac(&signing, b"Server Key");
            let verifier = BASE64.encode(hash.hmac(&server_key, auth_message.as_bytes()));
            done(state, auth, format!("v={verifier}").into_bytes())
        }
        Auth::None | Auth::Done { .. } => {
            fail(code::ILLEGAL_SASL_STATE, "SaslAuthenticate outside an exchange")
        }
    }
}
