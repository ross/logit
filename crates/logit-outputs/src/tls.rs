//! Shared client-side TLS construction for every sink that dials out over TLS -- `otlp_out`
//! (`crates/logit-outputs/src/otlp.rs`) and `logit_out` (`crates/logit-outputs/src/logit.rs`)
//! both build a `rustls::ClientConfig` from the same operator-facing settings via
//! [`build_client_config`]. Extracted from `otlp.rs` (`docs/plans/native-transport.md` workstream
//! B) -- a pure refactor, no behaviour change for `otlp_out`.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;

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
