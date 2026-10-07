//! Client-side TLS for `prometheus_in`'s scrape client.
//!
//! [`TlsClientSettings`]/[`apply_client_tls`] use `reqwest`'s own `Certificate`/`Identity` loaders
//! rather than a hand-built `rustls::ClientConfig` like `logit_outputs::tls::build_client_config`:
//! those sinks swap a whole `hyper-rustls` connector, where a `ClientConfig` is the natural seam,
//! but a plain `reqwest::Client` is smaller to configure through `reqwest` and keeps `rustls` types
//! out of this crate's HTTP-client path. Every listener's server side is in
//! `logit_pipeline::tls`.

use std::path::Path;

use anyhow::Context;

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
