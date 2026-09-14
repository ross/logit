//! Shared client-side TLS construction for every sink that dials out over TLS -- `otlp_out`
//! (`crates/logit-outputs/src/otlp.rs`) and `logit_out` (`crates/logit-outputs/src/logit.rs`)
//! both build a `rustls::ClientConfig` from the same operator-facing settings via
//! [`build_client_config`]. Extracted from `otlp.rs` (`docs/plans/native-transport.md` workstream
//! B) -- a pure refactor, no behaviour change for `otlp_out`.
//!
//! Also home to the TLS-adjacent pieces every raw-TCP sink shares regardless of whether TLS is
//! actually on: [`AsyncStream`], [`host_only`], and [`poll_pending_close`], the one-poll probe
//! each pooled sink runs on a reused connection before writing to it.

use std::future::poll_fn;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use anyhow::Context;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A plain `TcpStream` or a TLS-wrapped one, behind one object-safe trait so a sink's connection
/// field doesn't need to be generic (a sink field can't be, without making the whole sink type
/// generic in a way `logit-cli::pipeline::build_spec` would have to know about). Lives here
/// rather than in either sink: `logit_out` (`crates/logit-outputs/src/logit.rs`) and `syslog_out`
/// (`crates/logit-outputs/src/syslog.rs`) both dial raw TCP that may or may not be TLS-wrapped,
/// and both want the identical erasure.
pub(crate) trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// The host part of a bare `host:port` endpoint -- the SNI/`ServerName` to hand `rustls` when the
/// endpoint carries no scheme to parse (`logit_out`'s and `syslog_out`'s shape; `otlp_out`'s
/// URL-shaped endpoint has `reqwest`/`hyper` do this instead). `rsplit_once` so a bracketed IPv6
/// literal's own colons don't confuse this (an IPv6 endpoint here would need brackets,
/// `[::1]:1234`, the same convention every other bare `host:port` field in this codebase leaves
/// to the operator to write correctly; this only avoids splitting on the wrong colon, not
/// validating the address itself).
pub(crate) fn host_only(endpoint: &str) -> &str {
    endpoint
        .rsplit_once(':')
        .map(|(host, _port)| host)
        .unwrap_or(endpoint)
        .trim_start_matches('[')
        .trim_end_matches(']')
}

/// What one poll of a pooled stream found -- [`poll_pending_close`]'s answer.
pub(crate) enum PendingClose {
    /// Nothing readable at this instant. On every protocol these sinks speak the peer is silent
    /// unless it is answering something, so this is the healthy case: the connection is still
    /// there and the write can go ahead.
    Open,
    /// The peer closed its end (an immediate end-of-file), or the poll failed outright -- the two
    /// are the same thing to a caller about to write: this connection is finished.
    Eof,
    /// The peer sent something unprompted. On `logit_in` that is a `Reject{GOING_AWAY}` -- the
    /// close-is-coming signal, from a graceful shutdown or an idle timeout
    /// (`docs/adr/idle-connection-timeout.md`); on the line-oriented sinks there is nothing a
    /// receiver ever sends at all. Either way the pooled connection is not one to write a batch
    /// into.
    Bytes(usize),
}

impl std::fmt::Display for PendingClose {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PendingClose::Open => f.write_str("still open"),
            PendingClose::Eof => f.write_str("closed by the peer"),
            PendingClose::Bytes(n) => write!(f, "carrying {n} unsolicited byte(s) from the peer"),
        }
    }
}

/// Polls `stream` for readability **exactly once** and reports what it found, without ever
/// waiting: the check every pooled sink runs on a *reused* connection before the first write of a
/// send attempt, so a batch is not written into a socket whose peer already closed it
/// (`docs/adr/idle-connection-timeout.md`'s "The client-side probe" decision).
///
/// **Why one `poll_read` and not `tokio::time::timeout(stream.read(..))`.** A timeout around a
/// real read is a *cancellable* read: when the timer wins, the read future is dropped, and on a
/// TLS stream that can discard a partially-received record that `tokio_rustls` had already taken
/// off the socket -- bytes gone from the kernel and from the session both. The same hazard
/// applies through the `Box<dyn AsyncStream>` these sinks hold, where the caller cannot even tell
/// which kind of stream it has. One `poll_read` that returns `Poll::Pending` has, by contrast,
/// consumed nothing: `Pending` is precisely "no bytes were available," so the [`PendingClose::Open`]
/// answer -- the one where the connection is kept and written to -- is the one answer that
/// provably takes nothing off the stream. The two answers that *may* consume something
/// ([`PendingClose::Eof`], [`PendingClose::Bytes`]) both end with the connection dropped, so
/// there is nothing left to have corrupted.
///
/// `?Sized` so `&mut *boxed_stream` (a `&mut dyn AsyncStream`) works as directly as a `&mut
/// TcpStream` does -- the sinks hold both shapes.
///
/// This is inherently point-in-time: a FIN arriving between this poll and the write that follows
/// is unchanged from today (`Fault::Ambiguous` for `logit_out`, silent for the line-oriented
/// sinks). What it closes is the common case -- a peer that closed some time ago and whose FIN is
/// already sitting in this host's receive queue.
pub(crate) async fn poll_pending_close<S: AsyncRead + Unpin + ?Sized>(
    stream: &mut S,
    buf: &mut [u8],
) -> PendingClose {
    poll_fn(|cx| {
        let mut read_buf = ReadBuf::new(buf);
        match Pin::new(&mut *stream).poll_read(cx, &mut read_buf) {
            Poll::Pending => Poll::Ready(PendingClose::Open),
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => Poll::Ready(PendingClose::Eof),
            Poll::Ready(Ok(())) => Poll::Ready(PendingClose::Bytes(read_buf.filled().len())),
            Poll::Ready(Err(_)) => Poll::Ready(PendingClose::Eof),
        }
    })
    .await
}

/// Client-side TLS tuning for a sink's `tls:` config block. Mirrors `logit_config::
/// TlsClientConfig` -- this crate doesn't depend on `logit-config` (`docs/design/
/// pipeline-graph.md`'s crate layout); `logit-cli::pipeline::build_spec` converts one into the
/// other at construction time.
#[derive(Debug, Clone, Default)]
pub struct TlsClientSettings {
    /// PEM bundle of CA certificates to trust *instead of* the bundled Mozilla root set.
    pub ca_file: Option<String>,
    /// Client certificate chain (PEM) presented for mutual TLS. Requires `key_file`.
    pub cert_file: Option<String>,
    /// Private key (PEM, PKCS#8/PKCS#1/SEC1) for `cert_file`. Requires `cert_file`.
    pub key_file: Option<String>,
    /// Disables server-certificate verification entirely -- the connection is still encrypted,
    /// but accepts any certificate the peer presents, self-signed or otherwise.
    pub insecure_skip_verify: bool,
}

impl TlsClientSettings {
    /// `true` if every field is at its default -- a "was a `tls:` block actually set" check,
    /// mirroring `logit_config::TlsClientConfig::is_empty`.
    pub fn is_empty(&self) -> bool {
        self.ca_file.is_none()
            && self.cert_file.is_none()
            && self.key_file.is_none()
            && !self.insecure_skip_verify
    }
}

/// Builds a `rustls::ClientConfig` from `settings`. Every path is resolved against `base_dir`,
/// same as `logit_inputs::tls::build_server_config`'s server-side counterpart.
pub(crate) fn build_client_config(
    settings: &TlsClientSettings,
    base_dir: &Path,
) -> anyhow::Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("the ring crypto provider always supports TLS 1.2/1.3");

    // Both arms land in the same `WantsClientCert` builder state -- `with_root_certificates` and
    // `dangerous().with_custom_certificate_verifier` are just two different ways to supply a
    // verifier -- so client-cert material (below) is layered on identically either way. A caller
    // whose own graph validation rejects `insecure_skip_verify` together with `ca_file` (rule 24
    // for `otlp_out`, the mirrored rule for `logit_out`) never reaches this function with both
    // set; `insecure_skip_verify` together with a client certificate is legal (mTLS with no
    // server verification) and reaches the `with_client_auth_cert` branch below like any other
    // case.
    let builder = if settings.insecure_skip_verify {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert((*provider).clone())))
    } else {
        let mut roots = rustls::RootCertStore::empty();
        match &settings.ca_file {
            Some(ca_file) => {
                let path = base_dir.join(ca_file);
                let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&path)
                    .with_context(|| format!("reading tls.ca_file {}", path.display()))?
                    .collect::<Result<_, _>>()
                    .with_context(|| format!("parsing tls.ca_file {}", path.display()))?;
                roots.add_parsable_certificates(certs);
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        builder.with_root_certificates(roots)
    };

    match (&settings.cert_file, &settings.key_file) {
        (Some(cert_file), Some(key_file)) => {
            let cert_path = base_dir.join(cert_file);
            let key_path = base_dir.join(key_file);
            let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
                .with_context(|| format!("reading tls.cert_file {}", cert_path.display()))?
                .collect::<Result<_, _>>()
                .with_context(|| format!("parsing tls.cert_file {}", cert_path.display()))?;
            let key = PrivateKeyDer::from_pem_file(&key_path)
                .with_context(|| format!("reading tls.key_file {}", key_path.display()))?;
            Ok(builder.with_client_auth_cert(chain, key)?)
        }
        _ => Ok(builder.with_no_client_auth()),
    }
}

/// A [`rustls::client::danger::ServerCertVerifier`] that accepts any certificate the peer
/// presents -- `tls.insecure_skip_verify`'s implementation. The connection is still encrypted;
/// only the "is this actually who I meant to talk to" check is skipped. Still verifies the
/// handshake *signature* itself via `provider`'s own algorithms (`verify_tls12_signature`/
/// `verify_tls13_signature`) -- only certificate-chain and hostname validation are skipped, not
/// cryptographic signature verification.
#[derive(Debug)]
struct AcceptAnyServerCert(rustls::crypto::CryptoProvider);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
