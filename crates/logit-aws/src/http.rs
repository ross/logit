//! The HTTP client settings and body read the credential chain uses. These mirror
//! `logit-outputs`' `http` module (the client builder with no TLS override, the connect and idle
//! defaults, and the bounded read); a change to a default there doesn't move these.

use std::time::Duration;

/// The connection timeout of every request the chain makes.
pub(crate) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the chain's clients keep an idle connection.
pub(crate) const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// A client builder with redirects off, the whole-request `timeout`, and the pool's idle
/// timeout.
pub(crate) fn client_builder(
    timeout: Duration,
    connect_timeout: Duration,
    idle_timeout: Duration,
) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout)
        .redirect(reqwest::redirect::Policy::none());
    if idle_timeout.is_zero() {
        builder.pool_max_idle_per_host(0)
    } else {
        builder.pool_idle_timeout(idle_timeout)
    }
}

/// At most `limit` bytes of `response`'s body. A read error, such as a connection reset
/// mid-body, is returned rather than ending the body, so a caller that parses the body never
/// reads a cut-off one as complete.
pub(crate) async fn try_read_body_bytes(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, reqwest::Error> {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < limit {
        match response.chunk().await? {
            Some(chunk) => buf.extend_from_slice(&chunk),
            None => break,
        }
    }
    buf.truncate(limit);
    Ok(buf)
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
