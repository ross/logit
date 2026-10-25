//! The `Authorization` check behind an HTTP listener's `auth:` (`otlp_in`) or `bind_auth:`
//! (`prometheus_in`'s receiver, `prometheus_out`'s exposition), and the constant-time key
//! comparison every listener's protocol token check shares. One copy, so every listener agrees on
//! a scheme's case, whitespace, and what counts as a match.
//!
//! ADR `listener-authentication` records which credentials a listener accepts and why. What a
//! maintainer needs at the code:
//!
//! - The scheme is matched case-insensitively (RFC 9110 §11.1) and the value is trimmed of
//!   surrounding whitespace. The credential is everything after the first space or tab, trimmed.
//! - `Bearer` compares the credential with each configured token. `Basic` compares it with each
//!   configured `base64(username:password)`, encoded once at build time with standard padding, so
//!   a password holding `:` needs no split and nothing is decoded per request. A client that
//!   sends unpadded base64 doesn't match; every standard client pads.
//! - Every configured value of the scheme is compared, each over its whole length
//!   ([`matches_any_key`]), so the time taken doesn't reveal which matched or how much of a guess
//!   did. Only the scheme, which isn't secret, picks the list.
//! - A credential never leaves this module: [`Denied`] names why a request failed without
//!   carrying any of it, and [`Credentials`]'s `Debug` prints counts only. A caller never logs,
//!   counts, or stamps a credential, username included.

use base64::Engine;
use std::fmt;

/// The `WWW-Authenticate` challenge for `Basic`.
const BASIC_CHALLENGE: &str = "Basic realm=\"logit\"";

/// The `WWW-Authenticate` challenge for `Bearer`.
const BEARER_CHALLENGE: &str = "Bearer";

/// The bearer tokens and basic credentials one listener accepts. Empty accepts every request.
#[derive(Clone, Default)]
pub struct Credentials {
    bearer: Vec<Box<[u8]>>,
    /// Each `base64(username:password)`, as a client writes it after `Basic `.
    basic: Vec<Box<[u8]>>,
}

/// Why a request's `Authorization` header admitted it to no listener with credentials set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Denied {
    /// No `Authorization` header, or one with no credential after its scheme.
    Missing,
    /// A scheme none of the configured credentials use.
    Scheme,
    /// A credential that matches none configured.
    Unknown,
}

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Denied::Missing => "no credential",
            Denied::Scheme => "an authorization scheme this listener isn't configured for",
            Denied::Unknown => "a credential that matches none configured",
        })
    }
}

impl Credentials {
    /// The tokens `Bearer` accepts and the `(username, password)` pairs `Basic` accepts.
    /// Validation rejects an empty token, username, or password before it gets here.
    pub fn new<'a>(
        bearer_tokens: impl IntoIterator<Item = &'a str>,
        basic: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        Self {
            bearer: bearer_tokens.into_iter().map(|token| Box::from(token.as_bytes())).collect(),
            basic: basic
                .into_iter()
                .map(|(username, password)| {
                    basic_credential(username, password).into_bytes().into_boxed_slice()
                })
                .collect(),
        }
    }

    /// `true` when nothing is configured, so every request is accepted.
    pub fn is_empty(&self) -> bool {
        self.bearer.is_empty() && self.basic.is_empty()
    }

    /// Checks a request's first `Authorization` header value, `None` when it carries none. Always
    /// `Ok` when nothing is configured.
    pub fn check(&self, authorization: Option<&[u8]>) -> Result<(), Denied> {
        if self.is_empty() {
            return Ok(());
        }
        let value = authorization.ok_or(Denied::Missing)?.trim_ascii();
        let (scheme, credential) = match value.iter().position(|&b| b == b' ' || b == b'\t') {
            Some(at) => (&value[..at], value[at + 1..].trim_ascii()),
            None => (value, &[][..]),
        };
        let keys = if scheme.eq_ignore_ascii_case(b"bearer") && !self.bearer.is_empty() {
            &self.bearer
        } else if scheme.eq_ignore_ascii_case(b"basic") && !self.basic.is_empty() {
            &self.basic
        } else if value.is_empty() {
            return Err(Denied::Missing);
        } else {
            return Err(Denied::Scheme);
        };
        if credential.is_empty() {
            return Err(Denied::Missing);
        }
        if matches_any_key(keys, credential) {
            Ok(())
        } else {
            Err(Denied::Unknown)
        }
    }

    /// The `WWW-Authenticate` challenges a `401` carries, one per configured scheme, each its own
    /// header value: `Basic realm="logit"`, then `Bearer`.
    pub fn challenges(&self) -> impl Iterator<Item = &'static str> {
        let basic = (!self.basic.is_empty()).then_some(BASIC_CHALLENGE);
        let bearer = (!self.bearer.is_empty()).then_some(BEARER_CHALLENGE);
        basic.into_iter().chain(bearer)
    }
}

/// Counts only: a credential must never reach a log line through a derived `Debug`.
impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("bearer_tokens", &self.bearer.len())
            .field("basic", &self.basic.len())
            .finish()
    }
}

/// The credential a client writes after `Basic `: `username:password` in standard, padded base64.
pub fn basic_credential(username: &str, password: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"))
}

/// Whether `sent` equals one of `keys`. Every key is compared, and each comparison runs over the
/// whole key, so the time taken doesn't reveal which key matched or how much of a guess did.
pub fn matches_any_key(keys: &[Box<[u8]>], sent: &[u8]) -> bool {
    keys.iter().fold(false, |matched, key| matched | constant_time_eq(key, sent))
}

/// Byte equality whose time depends on the two lengths only, never on where the bytes differ.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn both() -> Credentials {
        Credentials::new(["token-a", "token-b"], [("alice", "pa:ss"), ("bob", "hunter2")])
    }

    fn check(credentials: &Credentials, value: &str) -> Result<(), Denied> {
        credentials.check(Some(value.as_bytes()))
    }

    #[test]
    fn constant_time_eq_compares_whole_keys() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }

    #[test]
    fn matches_any_key_finds_any_entry_and_nothing_else() {
        let keys: Vec<Box<[u8]>> = vec![Box::from(&b"one"[..]), Box::from(&b"two"[..])];
        assert!(matches_any_key(&keys, b"one"));
        assert!(matches_any_key(&keys, b"two"));
        assert!(!matches_any_key(&keys, b"three"));
        assert!(!matches_any_key(&[], b"one"));
    }

    #[test]
    fn nothing_configured_accepts_every_request() {
        let open = Credentials::default();
        assert!(open.is_empty());
        assert_eq!(open.check(None), Ok(()));
        assert_eq!(check(&open, "Bearer anything"), Ok(()));
        assert_eq!(open.challenges().count(), 0);
    }

    #[test]
    fn bearer_accepts_each_configured_token() {
        let credentials = both();
        assert_eq!(check(&credentials, "Bearer token-a"), Ok(()));
        assert_eq!(check(&credentials, "Bearer token-b"), Ok(()));
        assert_eq!(check(&credentials, "Bearer token-c"), Err(Denied::Unknown));
        assert_eq!(check(&credentials, "Bearer token-"), Err(Denied::Unknown));
    }

    #[test]
    fn basic_accepts_each_configured_pair_and_a_password_holding_a_colon() {
        let credentials = both();
        let alice = format!("Basic {}", basic_credential("alice", "pa:ss"));
        let bob = format!("Basic {}", basic_credential("bob", "hunter2"));
        assert_eq!(check(&credentials, &alice), Ok(()));
        assert_eq!(check(&credentials, &bob), Ok(()));
        // The same bytes split at the colon a server reads first: a different username.
        let swapped = format!("Basic {}", basic_credential("alice:pa", "ss"));
        assert_eq!(check(&credentials, &swapped), Ok(()), "one encoding, however it's split");
        let wrong = format!("Basic {}", basic_credential("alice", "pa"));
        assert_eq!(check(&credentials, &wrong), Err(Denied::Unknown));
        let crossed = format!("Basic {}", basic_credential("alice", "hunter2"));
        assert_eq!(check(&credentials, &crossed), Err(Denied::Unknown));
    }

    #[test]
    fn the_scheme_is_case_insensitive_and_whitespace_is_trimmed() {
        let credentials = both();
        for value in
            ["bearer token-a", "BEARER token-a", "  Bearer   token-a \t", "Bearer\ttoken-a"]
        {
            assert_eq!(check(&credentials, value), Ok(()), "{value:?}");
        }
        let basic = format!("bAsIc {}", basic_credential("bob", "hunter2"));
        assert_eq!(check(&credentials, &basic), Ok(()));
    }

    #[test]
    fn the_credential_itself_is_case_sensitive() {
        assert_eq!(check(&both(), "Bearer TOKEN-A"), Err(Denied::Unknown));
    }

    #[test]
    fn a_missing_header_or_credential_is_missing() {
        let credentials = both();
        assert_eq!(credentials.check(None), Err(Denied::Missing));
        assert_eq!(check(&credentials, ""), Err(Denied::Missing));
        assert_eq!(check(&credentials, "   "), Err(Denied::Missing));
        assert_eq!(check(&credentials, "Bearer"), Err(Denied::Missing));
        assert_eq!(check(&credentials, "Bearer   "), Err(Denied::Missing));
    }

    #[test]
    fn a_scheme_with_nothing_configured_is_refused() {
        let bearer_only = Credentials::new(["token-a"], []);
        let basic = format!("Basic {}", basic_credential("alice", "pa:ss"));
        assert_eq!(check(&bearer_only, &basic), Err(Denied::Scheme));
        assert_eq!(check(&bearer_only, "Token token-a"), Err(Denied::Scheme));
        assert_eq!(check(&bearer_only, "token-a"), Err(Denied::Scheme));
        let basic_only = Credentials::new([], [("alice", "pa:ss")]);
        assert_eq!(check(&basic_only, "Bearer token-a"), Err(Denied::Scheme));
    }

    #[test]
    fn challenges_name_each_configured_scheme() {
        assert_eq!(both().challenges().collect::<Vec<_>>(), ["Basic realm=\"logit\"", "Bearer"]);
        let bearer_only = Credentials::new(["t"], []);
        assert_eq!(bearer_only.challenges().collect::<Vec<_>>(), ["Bearer"]);
        let basic_only = Credentials::new([], [("u", "p")]);
        assert_eq!(basic_only.challenges().collect::<Vec<_>>(), ["Basic realm=\"logit\""]);
    }

    #[test]
    fn debug_prints_counts_and_no_credential() {
        let printed = format!("{:?}", both());
        assert_eq!(printed, "Credentials { bearer_tokens: 2, basic: 2 }");
        for secret in ["token-a", "alice", "pa:ss", &basic_credential("alice", "pa:ss")] {
            assert!(!printed.contains(secret), "{printed} contains {secret}");
        }
    }
}
