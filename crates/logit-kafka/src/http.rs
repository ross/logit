//! The HTTP client settings and bounded body reads `sasl::oauth`'s token endpoint uses: a
//! TLS-less copy of the subset of `logit-outputs`' `http` module it needs, since the endpoint
//! trusts the bundled roots and that module's `build_client` takes the sinks' TLS config. A
//! change to a default there doesn't move these. `logit-aws` keeps its own copy; neither
//! `logit-core` nor `logit-pipeline` depends on `reqwest`, so a shared home is a small crate of
//! its own, the fix when a fourth user appears (`docs/adr/kafka-input.md`, item 1).

use std::time::Duration;

/// The connection timeout of every token request, `logit-outputs`' `connect_timeout:` default.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a token endpoint's client keeps an idle connection, `logit-outputs`'
/// `idle_timeout:` default.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// A client with redirects off, the whole-request `timeout`, and the pool's idle timeout, trusting
/// `reqwest`'s bundled roots. `Duration::ZERO` as `idle_timeout` keeps no idle connection.
pub fn build_client(
    timeout: Duration,
    connect_timeout: Duration,
    idle_timeout: Duration,
) -> reqwest::Client {
    let builder = reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout)
        .redirect(reqwest::redirect::Policy::none());
    let builder = if idle_timeout.is_zero() {
        builder.pool_max_idle_per_host(0)
    } else {
        builder.pool_idle_timeout(idle_timeout)
    };
    builder.build().expect("a reqwest client with no TLS override should build")
}

/// At most `limit` bytes of `response`'s body. A read error ends the body where it stopped.
pub async fn read_body_bytes(mut response: reqwest::Response, limit: usize) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < limit {
        match response.chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    buf.truncate(limit);
    buf
}

/// [`read_body_bytes`], but a read error, such as the request's timeout or a connection closed
/// mid-body, is returned rather than ending the body, for a caller that parses the body and
/// must not read a cut-off one as complete.
pub async fn try_read_body_bytes(
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
