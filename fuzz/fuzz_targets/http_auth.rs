//! An `Authorization` header value through `auth::Credentials::check`, as an HTTP listener's
//! `auth:` reads it. The first byte is a length: that many following bytes, when UTF-8, are both a
//! bearer token and, split at their first `:`, a basic username and password; the rest is the
//! header value. Oracles: nothing configured accepts any value; an accepted value stays accepted
//! with its scheme's case flipped and whitespace around it; the header a client writes for the
//! configured token and for the configured pair is accepted.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_proto::auth::{basic_credential, Credentials};

fuzz_target!(|data: &[u8]| {
    let Some((&split, rest)) = data.split_first() else { return };
    let (secret, value) = rest.split_at(usize::from(split).min(rest.len()));
    let Ok(secret) = std::str::from_utf8(secret) else { return };
    let (username, password) = secret.split_once(':').unwrap_or((secret, ""));
    let credentials = Credentials::new([secret], [(username, password)]);

    assert_eq!(Credentials::default().check(Some(value)), Ok(()), "nothing configured");
    if credentials.check(Some(value)).is_ok() {
        let trimmed = value.trim_ascii();
        let at = trimmed.iter().position(|&b| b == b' ' || b == b'\t').unwrap_or(trimmed.len());
        let mut flipped = trimmed.to_vec();
        for byte in &mut flipped[..at] {
            *byte = if byte.is_ascii_lowercase() {
                byte.to_ascii_uppercase()
            } else {
                byte.to_ascii_lowercase()
            };
        }
        let padded = [&b" \t"[..], &flipped, b" "].concat();
        assert_eq!(credentials.check(Some(&padded)), Ok(()), "{padded:?}");
    }
    // A secret with surrounding whitespace never matches a trimmed header, and validation
    // rejects one, as it does an empty one.
    if !secret.is_empty() && secret.trim_ascii() == secret {
        let bearer = format!("Bearer {secret}");
        assert_eq!(credentials.check(Some(bearer.as_bytes())), Ok(()), "{bearer:?}");
    }
    let basic = format!("Basic {}", basic_credential(username, password));
    assert_eq!(credentials.check(Some(basic.as_bytes())), Ok(()), "{basic:?}");
});
