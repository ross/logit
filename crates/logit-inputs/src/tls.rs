//! Shared server-side TLS construction for every listener that terminates TLS -- `otlp_in`
//! (`crates/logit-inputs/src/otlp.rs`) and `logit_in` (`crates/logit-inputs/src/logit.rs`) both
//! build a `rustls::ServerConfig` from the same operator-facing settings via
//! [`build_server_config`]. Extracted from `otlp.rs` (`docs/plans/native-transport.md` workstream
//! B) -- a pure refactor, no behaviour change for `otlp_in`.
//!
//! [`TlsClientSettings`]/[`apply_client_tls`] are the opposite direction: `prometheus_in`
//! (`crates/logit-inputs/src/prometheus.rs`) is a client, not a listener, so it needs client-side
//! TLS tuning instead of a `ServerConfig` to terminate on. Deliberately built on `reqwest`'s own
//! `Certificate`/`Identity`/`danger_accept_invalid_certs` rather than hand-rolling a
//! `rustls::ClientConfig` the way `crates/logit-outputs/src/tls.rs`'s `build_client_config` does
//! for `otlp_out`/`logit_out`: those two sinks already build and swap a whole
//! `hyper_util`/`hyper-rustls` gRPC connector, where a raw `rustls::ClientConfig` is the natural
//! seam, but `prometheus_in`'s only client is a plain `reqwest::Client` -- `reqwest`'s own PEM
//! loaders are the smaller, equally-correct way to get there, and keep every line of `rustls` type
//! plumbing out of this crate's HTTP-client path entirely.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Server-side TLS for a listener's `tls:` config block. Mirrors `logit_config::TlsServerConfig`
/// -- this crate doesn't depend on `logit-config` (`docs/design/pipeline-graph.md`'s crate
/// layout); `logit-cli::pipeline::build_spec` converts one into the other at construction time.
/// Its mere presence on a listener turns TLS on -- there is no separate on/off flag.
#[derive(Debug, Clone)]
pub struct TlsServerSettings {
    /// Certificate chain (PEM) this listener presents to every client.
    pub cert_file: String,
    /// Private key (PEM, PKCS#8/PKCS#1/SEC1) for `cert_file`.
    pub key_file: String,
    /// PEM bundle of CAs. When set, every connecting client must present a certificate chaining
    /// to one of them (mutual TLS) -- absent, any client is accepted once the TLS handshake
    /// itself completes.
    pub client_ca_file: Option<String>,
}

/// Builds a `rustls::ServerConfig` from `settings`, advertising `alpn` as this listener's ALPN
/// protocol list -- `otlp_in` passes `[b"h2", b"http/1.1"]` under `protocol: http` (so a TLS
/// client's own negotiation picks the same protocol `hyper_util::server::conn::auto` would
/// otherwise have to sniff from plaintext bytes) or `[b"h2"]` under `protocol: grpc`; `logit_in`
/// passes `&[]` -- it isn't an HTTP-shaped protocol and has nothing for a client to negotiate down
/// to (the same "no ALPN" shape `logit_outputs::tls::build_client_config`'s client side already
/// uses). Every path in `settings` is resolved against `base_dir`, same as that client-side
/// counterpart.
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

/// Client-side TLS tuning for `prometheus_in`'s `tls:` config block. Mirrors
/// `logit_outputs::tls::TlsClientSettings` field-for-field (this crate doesn't depend on
/// `logit-config`, `docs/design/pipeline-graph.md`'s crate layout; `logit-cli::pipeline::
/// build_spec` converts one into the other at construction time) but is built into a
/// `reqwest::ClientBuilder` directly rather than a `rustls::ClientConfig` -- see this module's own
/// doc comment for why.
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
    /// mirroring `logit_outputs::tls::TlsClientSettings::is_empty`.
    pub fn is_empty(&self) -> bool {
        self.ca_file.is_none()
            && self.cert_file.is_none()
            && self.key_file.is_none()
            && !self.insecure_skip_verify
    }
}

/// Applies `settings` to `builder`, resolving every path against `base_dir` (same rule as
/// [`build_server_config`]'s). A no-op (`builder` returned unchanged) when `settings.is_empty()`
/// -- `reqwest` already trusts the bundled Mozilla root set for an `https://` target without this
/// ever being called.
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
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_file), Some(key_file)) = (&settings.cert_file, &settings.key_file) {
        let cert_path = base_dir.join(cert_file);
        let key_path = base_dir.join(key_file);
        // `reqwest::Identity::from_pem` wants one PEM blob carrying both the certificate chain and
        // its private key -- concatenated here rather than asking the operator to pre-combine the
        // two files themselves, matching `cert_file`/`key_file` staying two separate config fields
        // everywhere else in this project (`TlsServerSettings`, `logit_outputs::tls::
        // TlsClientSettings`).
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
