//! Shared server-side TLS construction for every listener that terminates TLS -- `otlp_in`
//! (`crates/logit-inputs/src/otlp.rs`) and `logit_in` (`crates/logit-inputs/src/logit.rs`) both
//! build a `rustls::ServerConfig` from the same operator-facing settings via
//! [`build_server_config`]. Extracted from `otlp.rs` (`docs/plans/native-transport.md` workstream
//! B) -- a pure refactor, no behaviour change for `otlp_in`.

use std::path::Path;
use std::sync::Arc;

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
