//! Client-side TLS shared by every sink that dials out over TLS: [`build_client_config`] turns
//! the operator-facing [`TlsClientSettings`] into a `rustls::ClientConfig`.
//!
//! Also the pieces raw-TCP sinks share whether or not TLS is on: [`AsyncStream`], [`host_only`],
//! and [`poll_pending_close`], the one-poll probe a pooled sink runs on a reused connection before
//! writing to it.

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

/// A plain `TcpStream` or a TLS-wrapped one, behind one object-safe trait, so a sink's connection
/// field isn't generic (a generic field would make the sink type generic, which
/// `logit-cli::pipeline::build_spec` would have to know about). Shared by every sink that dials a
/// raw TCP connection that may be TLS-wrapped.
pub(crate) trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// The host part of a bare `host:port` endpoint: the SNI `ServerName` for an endpoint with no
/// scheme to parse (a URL endpoint gets this from `reqwest`/`hyper`). `rsplit_once`, so a
/// bracketed IPv6 literal (`[::1]:1234`) splits on the port's colon. The brackets are the
/// operator's to write; this doesn't validate the address.
pub(crate) fn host_only(endpoint: &str) -> &str {
    endpoint
        .rsplit_once(':')
        .map(|(host, _port)| host)
        .unwrap_or(endpoint)
        .trim_start_matches('[')
        .trim_end_matches(']')
}

/// What one poll of a pooled stream found ([`poll_pending_close`]'s answer).
pub(crate) enum PendingClose {
    /// Nothing readable now. Every protocol these sinks speak has a peer that is silent unless
    /// answering, so this is the healthy case: write.
    Open,
    /// The peer closed its end (an immediate EOF), or the poll failed; either way the connection
    /// is finished.
    Eof,
    /// The peer sent something unprompted: from `logit_in`, a `Reject{GOING_AWAY}` before a
    /// graceful shutdown or idle close; a line-oriented receiver never sends anything. Either
    /// way, don't write a batch into it.
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

/// Polls `stream` for readability **once**, never waiting: the check a pooled sink runs on a
/// *reused* connection before a send attempt's first write, so a batch isn't written into a socket
/// the peer already closed (`docs/adr/idle-connection-timeout.md`'s "The client-side probe"
/// section).
///
/// **Why one `poll_read` and not `tokio::time::timeout(stream.read(..))`.** A timed-out read is
/// cancelled, and on a TLS stream dropping the read future can discard a partial record
/// `tokio_rustls` already took off the socket, gone from kernel and session both. Behind a
/// `Box<dyn AsyncStream>` the caller can't tell which kind of stream it has. A `poll_read` that
/// returns `Pending` has consumed nothing, so [`PendingClose::Open`], the one answer that keeps
/// the connection, takes nothing off the stream. The answers that may consume bytes
/// ([`PendingClose::Eof`], [`PendingClose::Bytes`]) both drop the connection.
///
/// `?Sized` so `&mut *boxed_stream` (a `&mut dyn AsyncStream`) works like a `&mut TcpStream`.
///
/// Point-in-time only: a FIN arriving between this poll and the write is still missed
/// (`Fault::Ambiguous` for `logit_out`, undetected for the line-oriented sinks). It catches the
/// common case, a FIN already in this host's receive queue.
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

/// Client-side TLS settings for a sink's `tls:` block. Mirrors `logit_config::TlsClientConfig`,
/// since this crate doesn't depend on `logit-config` (`docs/design/pipeline-graph.md`'s "Crate
/// layout" section); `logit-cli::pipeline::build_spec` converts between them.
#[derive(Debug, Clone, Default)]
pub struct TlsClientSettings {
    /// PEM bundle of CA certificates to trust *instead of* the bundled Mozilla root set.
    pub ca_file: Option<String>,
    /// Client certificate chain (PEM) presented for mutual TLS. Requires `key_file`.
    pub cert_file: Option<String>,
    /// Private key (PEM, PKCS#8/PKCS#1/SEC1) for `cert_file`. Requires `cert_file`.
    pub key_file: Option<String>,
    /// Skips server-certificate verification: still encrypted, but any certificate is accepted.
    pub insecure_skip_verify: bool,
}

impl TlsClientSettings {
    /// `true` if every field is at its default (`logit_config::TlsClientConfig::is_empty`). Not a
    /// "TLS is off" test for a sink where a `tls:` block's presence alone turns TLS on.
    pub fn is_empty(&self) -> bool {
        self.ca_file.is_none()
            && self.cert_file.is_none()
            && self.key_file.is_none()
            && !self.insecure_skip_verify
    }
}

/// Builds a `rustls::ClientConfig` from `settings`, resolving every path against `base_dir` (as
/// `logit_inputs::tls::build_server_config` does).
pub(crate) fn build_client_config(
    settings: &TlsClientSettings,
    base_dir: &Path,
) -> anyhow::Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("the ring crypto provider always supports TLS 1.2/1.3");

    // Both arms reach the same `WantsClientCert` state, so the client certificate below layers on
    // either way. Graph rules 24, 34, 44, 52, and 56 (`endpoint_tls`) reject
    // `insecure_skip_verify` with `ca_file` for their sinks; with a client certificate it is legal
    // (mTLS, no server verification).
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

/// `tls.insecure_skip_verify`'s [`rustls::client::danger::ServerCertVerifier`]: skips chain and
/// hostname validation, but still verifies the handshake signature with the provider's algorithms.
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
