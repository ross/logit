//! Shared TLS construction for this crate's listeners and its one HTTP client.
//!
//! [`build_server_config`] builds the `rustls::ServerConfig` every TLS-terminating listener uses:
//! `otlp_in`, `logit_in`, `prometheus_in`'s remote-write receiver, and the stream listeners in
//! `crate::tcp`.
//!
//! [`TlsClientSettings`]/[`apply_client_tls`] are the client direction, for `prometheus_in`'s
//! scrape client. They use `reqwest`'s own `Certificate`/`Identity` loaders rather than a
//! hand-built `rustls::ClientConfig` like `logit_outputs::tls::build_client_config`: those sinks
//! swap a whole `hyper-rustls` connector, where a `ClientConfig` is the natural seam, but a plain
//! `reqwest::Client` is smaller to configure through `reqwest` and keeps `rustls` types out of
//! this crate's HTTP-client path.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Server-side TLS for a listener's `tls:` config block, mirroring
/// `logit_config::TlsServerConfig` (this crate doesn't depend on `logit-config`;
/// `logit-cli::pipeline::build_spec` converts). Its presence turns TLS on; there's no separate
/// flag.
#[derive(Debug, Clone)]
pub struct TlsServerSettings {
    /// Certificate chain (PEM) this listener presents to every client.
    pub cert_file: String,
    /// Private key (PEM, PKCS#8/PKCS#1/SEC1) for `cert_file`.
    pub key_file: String,
    /// PEM bundle of CAs. When set, every client must present a certificate chaining to one of
    /// them (mutual TLS); when absent, any client that completes the handshake is accepted.
    pub client_ca_file: Option<String>,
}

/// Builds a `rustls::ServerConfig` from `settings`, with every path resolved against `base_dir`.
///
/// `alpn` is the advertised protocol list. An HTTP listener passes `[b"h2", b"http/1.1"]` (so the
/// client's negotiation picks what `hyper_util::server::conn::auto` would otherwise sniff from
/// plaintext) or `[b"h2"]` for gRPC; a non-HTTP protocol passes `&[]`.
pub(crate) fn build_server_config(
    settings: &TlsServerSettings,
    base_dir: &Path,
    alpn: &[&[u8]],
) -> anyhow::Result<rustls::ServerConfig> {
    let cert_path = base_dir.join(&settings.cert_file);
    let key_path = base_dir.join(&settings.key_file);
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
        .map_err(|e| anyhow::anyhow!("reading tls.cert_file {}: {e}", cert_path.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("parsing tls.cert_file {}: {e}", cert_path.display()))?;
    let key = PrivateKeyDer::from_pem_file(&key_path)
        .map_err(|e| anyhow::anyhow!("reading tls.key_file {}: {e}", key_path.display()))?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("the ring crypto provider always supports TLS 1.2/1.3");

    let mut cfg = match &settings.client_ca_file {
        Some(client_ca_file) => {
            let ca_path = base_dir.join(client_ca_file);
            let ca_certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&ca_path)
                .map_err(|e| {
                    anyhow::anyhow!("reading tls.client_ca_file {}: {e}", ca_path.display())
                })?
                .collect::<Result<_, _>>()
                .map_err(|e| {
                    anyhow::anyhow!("parsing tls.client_ca_file {}: {e}", ca_path.display())
                })?;
            let mut roots = rustls::RootCertStore::empty();
            roots.add_parsable_certificates(ca_certs);
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| anyhow::anyhow!("building client-cert verifier: {e}"))?;
            builder.with_client_cert_verifier(verifier).with_single_cert(chain, key)?
        }
        None => builder.with_no_client_auth().with_single_cert(chain, key)?,
    };
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(cfg)
}

/// Client-side TLS for `prometheus_in`'s scrape client. Mirrors
/// `logit_outputs::tls::TlsClientSettings` field for field (`logit-cli::pipeline::build_spec`
/// converts from config), but is applied to a `reqwest::ClientBuilder`; the module doc says why.
#[derive(Debug, Clone, Default)]
pub struct TlsClientSettings {
    /// PEM bundle of CA certificates to trust *instead of* the bundled Mozilla root set.
    pub ca_file: Option<String>,
    /// Client certificate chain (PEM) presented for mutual TLS. Requires `key_file`.
    pub cert_file: Option<String>,
    /// Private key (PEM, PKCS#8/PKCS#1/SEC1) for `cert_file`. Requires `cert_file`.
    pub key_file: Option<String>,
    /// Disables server-certificate verification: still encrypted, but any certificate is
    /// accepted.
    pub insecure_skip_verify: bool,
}

impl TlsClientSettings {
    /// `true` if every field is at its default, meaning no `tls:` block was set.
    pub fn is_empty(&self) -> bool {
        self.ca_file.is_none()
            && self.cert_file.is_none()
            && self.key_file.is_none()
            && !self.insecure_skip_verify
    }
}

/// Applies `settings` to `builder`, resolving every path against `base_dir`. Returns `builder`
/// unchanged when `settings.is_empty()`: `reqwest` trusts the bundled Mozilla roots by default.
pub(crate) fn apply_client_tls(
    mut builder: reqwest::ClientBuilder,
    settings: &TlsClientSettings,
    base_dir: &Path,
) -> anyhow::Result<reqwest::ClientBuilder> {
    if settings.is_empty() {
        return Ok(builder);
    }
    if settings.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_file) = &settings.ca_file {
        let path = base_dir.join(ca_file);
        let pem = std::fs::read(&path)
            .with_context(|| format!("reading tls.ca_file {}", path.display()))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .with_context(|| format!("parsing tls.ca_file {}", path.display()))?;
        // `add_root_certificate` alone is additive: the Mozilla roots would stay trusted, and a
        // server chaining to any public root would verify. Disabling the built-in roots makes
        // `ca_file` a replacement, as `logit_outputs::tls::build_client_config`'s
        // `RootCertStore::empty()` does.
        builder = builder.tls_built_in_root_certs(false).add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&settings.cert_file, &settings.key_file) {
        let cert_path = base_dir.join(cert_file);
        let key_path = base_dir.join(key_file);
        // `reqwest::Identity::from_pem` wants the chain and key in one PEM blob; concatenated
        // here so `cert_file`/`key_file` stay two fields, as everywhere else.
        let mut pem = std::fs::read(&cert_path)
            .with_context(|| format!("reading tls.cert_file {}", cert_path.display()))?;
        let key_pem = std::fs::read(&key_path)
            .with_context(|| format!("reading tls.key_file {}", key_path.display()))?;
        pem.push(b'\n');
        pem.extend_from_slice(&key_pem);
        let identity = reqwest::Identity::from_pem(&pem).with_context(|| {
            format!(
                "building a client identity from tls.cert_file {} + tls.key_file {}",
                cert_path.display(),
                key_path.display()
            )
        })?;
        builder = builder.identity(identity);
    }
    Ok(builder)
}
