//! SASL `SCRAM-SHA-256` and `SCRAM-SHA-512` (RFC 5802, RFC 7677), the client side, as the Java
//! client speaks it:
//!
//! - The client nonce is 24 bytes from `SystemRandom`, base64-encoded, so it holds no `,`.
//! - The username escapes `=` as `=3D` and `,` as `=2C`. Neither the username nor the password
//!   goes through SASLprep: Java sends both as UTF-8 unmodified, and a broker derives its stored
//!   credential the same way (`docs/adr/kafka-sasl.md`, item 5).
//! - No channel binding: the GS2 header is `n,,`, and `c=biws` is its base64.
//! - The server-first message's nonce must extend the client's, its iteration count must be from
//!   [`MIN_ITERATIONS`] to [`MAX_ITERATIONS`], and a mandatory extension (`m=`) is refused.
//! - The server-final message's `v=` must be the server signature, compared in constant time; an
//!   `e=` is the broker's refusal.
//!
//! Every check that fails here fails the same way on every connection, so its error is
//! `Refused` (the parent module's default for a session).

use anyhow::{anyhow, bail, ensure, Context};
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use base64::Engine;
use ring::rand::SecureRandom;
use ring::{digest, hmac, pbkdf2};

use super::{Authenticator, Session, Step};

/// The lowest iteration count accepted: RFC 7677's floor, and Kafka's own minimum for both
/// mechanisms (`ScramMechanism`).
const MIN_ITERATIONS: u32 = 4096;

/// The highest iteration count accepted. It only bounds the PBKDF2 work a garbled server message
/// can ask of the connecting task, and sits far above any real broker: Kafka caps a credential at
/// 16384, and Redpanda sets none (it creates credentials at 4096).
const MAX_ITERATIONS: u32 = 1 << 20;

/// The GS2 header for no channel binding and no authorization identity, and its base64 for the
/// client-final message's `c=`.
const GS2_HEADER: &str = "n,,";
const GS2_HEADER_B64: &str = "biws";

/// The hash a SCRAM mechanism is built on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramHash {
    Sha256,
    Sha512,
}

impl ScramHash {
    pub fn mechanism(self) -> &'static str {
        match self {
            ScramHash::Sha256 => "SCRAM-SHA-256",
            ScramHash::Sha512 => "SCRAM-SHA-512",
        }
    }

    fn hmac_algorithm(self) -> hmac::Algorithm {
        match self {
            ScramHash::Sha256 => hmac::HMAC_SHA256,
            ScramHash::Sha512 => hmac::HMAC_SHA512,
        }
    }

    /// RFC 5802's `Hi(password, salt, iterations)`: PBKDF2 with this hash's HMAC.
    pub fn salted_password(self, password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
        let (algorithm, len) = match self {
            ScramHash::Sha256 => (pbkdf2::PBKDF2_HMAC_SHA256, digest::SHA256_OUTPUT_LEN),
            ScramHash::Sha512 => (pbkdf2::PBKDF2_HMAC_SHA512, digest::SHA512_OUTPUT_LEN),
        };
        let iterations = std::num::NonZeroU32::new(iterations).expect("checked against the floor");
        let mut out = vec![0; len];
        pbkdf2::derive(algorithm, iterations, salt, password.as_bytes(), &mut out);
        out
    }

    pub fn key(self, key: &[u8]) -> hmac::Key {
        hmac::Key::new(self.hmac_algorithm(), key)
    }

    pub fn hmac(self, key: &[u8], data: &[u8]) -> Vec<u8> {
        hmac::sign(&self.key(key), data).as_ref().to_vec()
    }

    pub fn hash(self, data: &[u8]) -> Vec<u8> {
        let algorithm = match self {
            ScramHash::Sha256 => &digest::SHA256,
            ScramHash::Sha512 => &digest::SHA512,
        };
        digest::digest(algorithm, data).as_ref().to_vec()
    }
}

/// RFC 5802's `saslname`: `=` as `=3D`, `,` as `=2C`.
pub fn escape_username(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

pub struct Scram {
    pub hash: ScramHash,
    pub username: String,
    pub password: String,
}

#[async_trait::async_trait]
impl Authenticator for Scram {
    fn mechanism(&self) -> &'static str {
        self.hash.mechanism()
    }

    fn principal(&self) -> String {
        format!("user {:?}", self.username)
    }

    async fn start(&self, _host: &str) -> anyhow::Result<Box<dyn Session>> {
        let mut nonce = [0u8; 24];
        ring::rand::SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| anyhow!("the system random source failed to give a SCRAM nonce"))?;
        Ok(Box::new(ScramSession::new(
            self.hash,
            &self.username,
            &self.password,
            STANDARD_NO_PAD.encode(nonce),
        )))
    }
}

enum State {
    Start,
    /// The client-first message is sent; `bare` is it without the GS2 header.
    SentFirst {
        bare: String,
    },
    /// The client-final message is sent; the server signature is checked with `server_key` over
    /// `auth_message`.
    SentFinal {
        server_key: hmac::Key,
        auth_message: String,
    },
    Done,
}

/// One connection's SCRAM exchange.
pub struct ScramSession {
    hash: ScramHash,
    username: String,
    password: String,
    nonce: String,
    state: State,
}

impl ScramSession {
    /// A session whose client nonce is `nonce`; a test pins it to reproduce a published exchange.
    pub fn new(hash: ScramHash, username: &str, password: &str, nonce: String) -> Self {
        Self {
            hash,
            username: escape_username(username),
            password: password.to_string(),
            nonce,
            state: State::Start,
        }
    }

    /// Reads the server-first message and returns the client-final one.
    fn client_final(&mut self, bare: &str, server_first: &[u8]) -> anyhow::Result<Vec<u8>> {
        let server_first =
            std::str::from_utf8(server_first).context("the server-first message isn't UTF-8")?;
        if let Some(error) = server_first.strip_prefix("e=") {
            bail!("the broker refused the client-first message: {error}");
        }
        let mut attributes = server_first.split(',');
        let nonce = attribute(&mut attributes, "r")?;
        let salt = attribute(&mut attributes, "s")?;
        let iterations = attribute(&mut attributes, "i")?;
        ensure!(
            nonce.len() > self.nonce.len() && nonce.starts_with(&self.nonce),
            "the server-first message's nonce doesn't extend the client's"
        );
        let salt = STANDARD.decode(salt).context("the server-first message's salt isn't base64")?;
        ensure!(!salt.is_empty(), "the server-first message's salt is empty");
        let iterations: u32 = iterations.parse().with_context(|| {
            format!("the server-first message's iteration count {iterations:?}")
        })?;
        ensure!(
            (MIN_ITERATIONS..=MAX_ITERATIONS).contains(&iterations),
            "the server-first message asks for {iterations} iterations; SCRAM here takes \
             {MIN_ITERATIONS} to {MAX_ITERATIONS}"
        );

        let salted = self.hash.salted_password(&self.password, &salt, iterations);
        let client_key = self.hash.hmac(&salted, b"Client Key");
        let stored_key = self.hash.hash(&client_key);
        let without_proof = format!("c={GS2_HEADER_B64},r={nonce}");
        let auth_message = format!("{bare},{server_first},{without_proof}");
        let signature = self.hash.hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(&signature).map(|(k, s)| k ^ s).collect();
        let server_key = self.hash.key(&self.hash.hmac(&salted, b"Server Key"));
        self.state = State::SentFinal { server_key, auth_message };
        Ok(format!("{without_proof},p={}", STANDARD.encode(proof)).into_bytes())
    }
}

/// The value of the server-first message's next attribute, which must be `name`.
fn attribute<'a>(
    attributes: &mut std::str::Split<'a, char>,
    name: &str,
) -> anyhow::Result<&'a str> {
    let attribute = attributes.next().unwrap_or_default();
    ensure!(
        !attribute.starts_with("m="),
        "the server-first message carries a mandatory extension, which SCRAM here can't honor"
    );
    attribute
        .strip_prefix(name)
        .and_then(|a| a.strip_prefix('='))
        .ok_or_else(|| anyhow!("the server-first message has no '{name}=' where RFC 5802 puts it"))
}

impl Session for ScramSession {
    fn first(&mut self) -> anyhow::Result<Vec<u8>> {
        ensure!(matches!(self.state, State::Start), "SCRAM's client-first message is sent once");
        let bare = format!("n={},r={}", self.username, self.nonce);
        let first = format!("{GS2_HEADER}{bare}").into_bytes();
        self.state = State::SentFirst { bare };
        Ok(first)
    }

    fn next(&mut self, server: &[u8]) -> anyhow::Result<Step> {
        match std::mem::replace(&mut self.state, State::Done) {
            State::SentFirst { bare } => self.client_final(&bare, server).map(Step::Send),
            State::SentFinal { server_key, auth_message } => {
                let server_final =
                    std::str::from_utf8(server).context("the server-final message isn't UTF-8")?;
                if let Some(error) = server_final.strip_prefix("e=") {
                    bail!("the broker refused the client-final message: {error}");
                }
                let verifier = server_final
                    .split(',')
                    .next()
                    .and_then(|v| v.strip_prefix("v="))
                    .ok_or_else(|| anyhow!("the server-final message has no 'v='"))?;
                let verifier = STANDARD
                    .decode(verifier)
                    .context("the server-final message's 'v=' isn't base64")?;
                // A mismatch means the broker doesn't hold this user's credential, so it isn't
                // the broker the credential was set up with.
                hmac::verify(&server_key, auth_message.as_bytes(), &verifier).map_err(|_| {
                    anyhow!(
                        "the broker's server signature doesn't match: it doesn't hold this \
                         user's credential"
                    )
                })?;
                Ok(Step::Done)
            }
            State::Start | State::Done => bail!("a SCRAM message arrived outside the exchange"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677's section 3 example, and the RFC 5802 section 5 example's inputs.
    const CLIENT_NONCE: &str = "rOprNGfwEbeRWgbNEkqO";
    const SERVER_FIRST: &str =
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const WITHOUT_PROOF: &str = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";

    fn session(hash: ScramHash) -> ScramSession {
        ScramSession::new(hash, "user", "pencil", CLIENT_NONCE.to_string())
    }

    /// Runs the exchange to the client-final message.
    fn to_final(hash: ScramHash, server_first: &str) -> anyhow::Result<(ScramSession, String)> {
        let mut s = session(hash);
        assert_eq!(s.first().unwrap(), format!("n,,n=user,r={CLIENT_NONCE}").into_bytes());
        match s.next(server_first.as_bytes())? {
            Step::Send(message) => Ok((s, String::from_utf8(message).unwrap())),
            Step::Done => panic!("SCRAM sends a client-final message"),
        }
    }

    fn assert_refused<T>(result: anyhow::Result<T>, want: &str) {
        let Err(err) = result else { panic!("want an error with {want:?}") };
        assert!(format!("{err:#}").contains(want), "want {want:?}, got: {err:#}");
        assert!(!format!("{err:#}").contains("pencil"), "{err:#}");
    }

    /// RFC 7677, section 3, byte for byte.
    #[test]
    fn rfc_7677_scram_sha_256_example() {
        let (mut s, client_final) = to_final(ScramHash::Sha256, SERVER_FIRST).unwrap();
        assert_eq!(
            client_final,
            format!("{WITHOUT_PROOF},p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
        );
        assert_eq!(s.next(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=").unwrap(), Step::Done);
    }

    /// RFC 7677's inputs under SHA-512. Python 3's `hashlib.pbkdf2_hmac`, `hmac`, and `base64`
    /// computed the expected messages from RFC 5802's formulas; the same script reproduces RFC
    /// 7677's SHA-256 values above.
    #[test]
    fn scram_sha_512_with_rfc_7677_inputs() {
        let (mut s, client_final) = to_final(ScramHash::Sha512, SERVER_FIRST).unwrap();
        assert_eq!(
            client_final,
            format!(
                "{WITHOUT_PROOF},p=gMGXRcevScNtxZ6/8lQYpGtnsNAc3mGcmNomv+xnoOMw+3R2xNJdMNnzMlTN8PP\
                 C6wdp6dybEmDYXYTxwnYPJQ=="
            )
        );
        let verifier =
            "v=ZQnYEgWQMFmmsM8aQMF0nDDCy/AgCzkwk8CmMZYcMg0vSVlKDanekLtifDSeVGT4+5ZxXnJq19\
                        9RVG2rR7N7Zw==";
        assert_eq!(s.next(verifier.as_bytes()).unwrap(), Step::Done);
    }

    #[test]
    fn a_username_escapes_equals_and_comma_and_skips_saslprep() {
        assert_eq!(escape_username("a=b,c"), "a=3Db=2Cc");
        let mut s = ScramSession::new(ScramHash::Sha256, "ｕ=,", "p", "n0".into());
        assert_eq!(s.first().unwrap(), "n,,n=ｕ=3D=2C,r=n0".as_bytes());
    }

    #[test]
    fn a_bad_server_signature_is_refused() {
        let (mut s, _) = to_final(ScramHash::Sha256, SERVER_FIRST).unwrap();
        // RFC 7677's verifier with its first byte changed.
        assert_refused(
            s.next(b"v=7rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="),
            "server signature doesn't match",
        );
        let (mut s, _) = to_final(ScramHash::Sha512, SERVER_FIRST).unwrap();
        assert_refused(
            s.next(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="),
            "server signature doesn't match",
        );
    }

    #[test]
    fn an_iteration_count_outside_the_bounds_is_refused() {
        for i in ["4095", "1", "1048577"] {
            let first = SERVER_FIRST.replace("i=4096", &format!("i={i}"));
            assert_refused(to_final(ScramHash::Sha256, &first), "SCRAM here takes 4096 to 1048576");
        }
        for i in ["16384", "16385", "1048576"] {
            let first = SERVER_FIRST.replace("i=4096", &format!("i={i}"));
            to_final(ScramHash::Sha256, &first).expect("a count in range is accepted");
        }
    }

    #[test]
    fn a_malformed_server_first_message_is_refused() {
        let cases = [
            ("r=someoneelse,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", "doesn't extend the client's"),
            (
                "r=rOprNGfwEbeRWgbNEkqO,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
                "doesn't extend the client's",
            ),
            ("r=rOprNGfwEbeRWgbNEkqOx,s=!!,i=4096", "salt isn't base64"),
            ("r=rOprNGfwEbeRWgbNEkqOx,s=,i=4096", "salt is empty"),
            ("r=rOprNGfwEbeRWgbNEkqOx,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=lots", "iteration count"),
            ("r=rOprNGfwEbeRWgbNEkqOx,s=W22ZaJ0SNY7soEsUEjb6gQ==", "no 'i='"),
            ("s=W22ZaJ0SNY7soEsUEjb6gQ==,r=rOprNGfwEbeRWgbNEkqOx,i=4096", "no 'r='"),
            (
                "m=ext,r=rOprNGfwEbeRWgbNEkqOx,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
                "mandatory extension",
            ),
            ("e=unknown-user", "refused the client-first message: unknown-user"),
            ("", "no 'r='"),
        ];
        for (first, want) in cases {
            assert_refused(to_final(ScramHash::Sha256, first), want);
        }
        let mut s = session(ScramHash::Sha256);
        s.first().unwrap();
        assert_refused(s.next(b"r=\xff"), "isn't UTF-8");
    }

    #[test]
    fn a_malformed_or_refusing_server_final_message_is_refused() {
        for (last, want) in [
            (&b"e=invalid-proof"[..], "refused the client-final message: invalid-proof"),
            (b"", "no 'v='"),
            (b"v=!!", "isn't base64"),
            (b"\xff", "isn't UTF-8"),
        ] {
            let (mut s, _) = to_final(ScramHash::Sha256, SERVER_FIRST).unwrap();
            assert_refused(s.next(last), want);
        }
    }

    #[test]
    fn a_message_after_the_exchange_is_refused() {
        let (mut s, _) = to_final(ScramHash::Sha256, SERVER_FIRST).unwrap();
        s.next(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=").unwrap();
        assert_refused(s.next(b"v="), "outside the exchange");
    }

    #[tokio::test]
    async fn each_connection_gets_a_fresh_nonce() {
        let scram = Scram { hash: ScramHash::Sha512, username: "u".into(), password: "p".into() };
        assert_eq!(scram.mechanism(), "SCRAM-SHA-512");
        let a = scram.start("h").await.unwrap().first().unwrap();
        let b = scram.start("h").await.unwrap().first().unwrap();
        assert_ne!(a, b);
        let nonce = std::str::from_utf8(&a).unwrap().strip_prefix("n,,n=u,r=").unwrap();
        assert_eq!(nonce.len(), 32, "24 bytes, base64 without padding");
        assert!(!nonce.contains(','));
    }
}
