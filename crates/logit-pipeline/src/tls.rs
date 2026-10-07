//! Shared TLS construction, and the reloader that swaps rotated certificates in without a restart.
//!
//! [`build_server_config`] builds the `rustls::ServerConfig` every TLS-terminating listener uses,
//! and [`build_client_config`] the `rustls::ClientConfig` every TLS client uses: each sink, and
//! `prometheus_in`'s scrape client. Each registers the files it read with a [`TlsReloader`]. A
//! config is built once around swappable pieces, [`ReloadingCert`], [`ReloadingClientVerifier`],
//! and [`ReloadingServerVerifier`], so a reload reaches the next full handshake with no rebuilt
//! config or client, and an open connection keeps the certificate it handshook with.
//! `docs/adr/tls-certificate-reload.md` has the design. The facts a maintainer needs at the code:
//!
//! - **A change is a change in content.** [`TlsReloader::check_now`] reads every file of a set and
//!   compares the bytes with what it last *attempted*, not with what loaded. Opening the configured
//!   path follows whatever symlinks exist at that moment, so a Kubernetes `..data` swap or a
//!   certbot `live/` relink is a change, and the same bad content fails once rather than once per
//!   check.
//! - **A set loads whole or not at all.** A changed set is parsed in full before anything swaps,
//!   and a failure keeps the old material. A certificate and key that don't match fail the load:
//!   `CertifiedKey::from_der` runs `keys_match`.
//! - **A cloned `ClientConfig` shares its swappable pieces.** They sit behind `Arc`s, so a
//!   `reqwest` client built with `use_preconfigured_tls(cfg.clone())` and a `hyper-rustls`
//!   connector built with `with_tls_config(cfg.clone())` both see every reload.
//! - **Errors name a file's config key and path, never a value from the config.**

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use logit_core::{Diagnostics, Telemetry};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use rustls::crypto::CryptoProvider;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio::sync::watch;

/// Server-side TLS for a listener's `tls:` config block, mirroring
/// `logit_config::TlsServerConfig` (`logit-cli::pipeline::build_spec` converts). Its presence
/// turns TLS on; there's no separate flag.
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

/// Builds a `rustls::ServerConfig` from `settings`, with every path resolved against `base_dir`,
/// and registers its files with `reloader` under the owning component's `diag` and `telemetry`.
///
/// `alpn` is the advertised protocol list. An HTTP listener passes `[b"h2", b"http/1.1"]` (so the
/// client's negotiation picks what `hyper_util::server::conn::auto` would otherwise sniff from
/// plaintext) or `[b"h2"]` for gRPC; a non-HTTP protocol passes `&[]`.
///
/// Fails on a file that's missing or doesn't parse, and on a key that doesn't match its
/// certificate: at startup a bad file stops the process, where a reload keeps the old material.
pub fn build_server_config(
    settings: &TlsServerSettings,
    base_dir: &Path,
    alpn: &[&[u8]],
    reloader: &TlsReloader,
    diag: &Diagnostics,
    telemetry: &Telemetry,
) -> anyhow::Result<rustls::ServerConfig> {
    let mut files = vec![
        WatchedFile::new("tls.cert_file", base_dir.join(&settings.cert_file)),
        WatchedFile::new("tls.key_file", base_dir.join(&settings.key_file)),
    ];
    if let Some(client_ca_file) = &settings.client_ca_file {
        files.push(WatchedFile::new("tls.client_ca_file", base_dir.join(client_ca_file)));
    }
    let contents = files.iter().map(WatchedFile::read).collect::<anyhow::Result<Vec<_>>>()?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let material = ServerMaterial::parse(&files, &contents, &provider)?;
    let not_after = material.not_after;
    let cert = Arc::new(ReloadingCert::new(material.key));
    let verifier = material.verifier.map(|v| Arc::new(ReloadingClientVerifier::new(v)));

    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("the ring crypto provider always supports TLS 1.2/1.3");
    let mut cfg = match &verifier {
        Some(verifier) => builder
            .with_client_cert_verifier(verifier.clone() as Arc<dyn ClientCertVerifier>)
            .with_cert_resolver(cert.clone()),
        None => builder.with_no_client_auth().with_cert_resolver(cert.clone()),
    };
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();

    let load_files = files.clone();
    reloader.register(Registration {
        files,
        contents,
        side: "server",
        not_after,
        load: Box::new(move |contents| {
            let material = ServerMaterial::parse(&load_files, contents, &provider)?;
            cert.swap(material.key);
            if let (Some(verifier), Some(next)) = (&verifier, material.verifier) {
                verifier.swap(next);
            }
            Ok(material.not_after)
        }),
        diag: diag.clone(),
        telemetry: telemetry.clone(),
    });
    Ok(cfg)
}

/// One parsed server file set: everything a swap needs, built before anything swaps.
struct ServerMaterial {
    key: Arc<CertifiedKey>,
    verifier: Option<Arc<dyn ClientCertVerifier>>,
    not_after: Option<i64>,
}

impl ServerMaterial {
    /// `files` and `contents` are parallel: certificate, key, and an optional client CA bundle.
    fn parse(
        files: &[WatchedFile],
        contents: &[Vec<u8>],
        provider: &Arc<CryptoProvider>,
    ) -> anyhow::Result<Self> {
        let (key, not_after) = certified_key(&files[..2], &contents[..2], provider)?;
        let verifier = match (files.get(2), contents.get(2)) {
            (Some(file), Some(contents)) => {
                let mut roots = rustls::RootCertStore::empty();
                roots.add_parsable_certificates(parse_certs(file, contents)?);
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                        .build()
                        .map_err(|e| file.error(format_args!("building a verifier: {e}")))?;
                Some(verifier)
            }
            _ => None,
        };
        Ok(Self { key, verifier, not_after })
    }
}

/// A certificate chain and its key, from parallel `[cert_file, key_file]` slices, with the leaf's
/// `notAfter`. Fails on a key that doesn't match the leaf.
fn certified_key(
    files: &[WatchedFile],
    contents: &[Vec<u8>],
    provider: &Arc<CryptoProvider>,
) -> anyhow::Result<(Arc<CertifiedKey>, Option<i64>)> {
    let chain = parse_certs(&files[0], &contents[0])?;
    let key = PrivateKeyDer::from_pem_slice(&contents[1])
        .map_err(|e| files[1].error(format_args!("parsing: {e}")))?;
    let not_after = not_after(&chain[0]);
    let key = CertifiedKey::from_der(chain, key, provider).map_err(|e| {
        anyhow::anyhow!(
            "loading {} {} with {} {}: {e}",
            files[0].label,
            files[0].path.display(),
            files[1].label,
            files[1].path.display()
        )
    })?;
    Ok((Arc::new(key), not_after))
}

/// Client-side TLS for a sink's `tls:` block, or `prometheus_in`'s `scrape_tls:`, mirroring
/// `logit_config::TlsClientConfig` (`logit-cli::pipeline::build_spec` converts).
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

/// Builds a `rustls::ClientConfig` from `settings`, with every path resolved against `base_dir`,
/// and registers its files with `reloader` under the owning component's `diag` and `telemetry`.
///
/// Registers `ca_file` and the `cert_file`/`key_file` pair, whichever are set, and nothing when
/// neither is. `insecure_skip_verify` ignores `ca_file`, and the default Mozilla roots are compiled
/// in, so neither has anything to reload. ALPN stays empty: `otlp_out`'s gRPC connector panics on
/// a config that sets it.
///
/// Fails on a file that's missing or doesn't parse, and on a key that doesn't match its
/// certificate: at startup a bad file stops the process, where a reload keeps the old material.
pub fn build_client_config(
    settings: &TlsClientSettings,
    base_dir: &Path,
    reloader: &TlsReloader,
    diag: &Diagnostics,
    telemetry: &Telemetry,
) -> anyhow::Result<rustls::ClientConfig> {
    let mut files = Vec::new();
    if let Some(ca_file) = settings.ca_file.as_ref().filter(|_| !settings.insecure_skip_verify) {
        files.push(WatchedFile::new("tls.ca_file", base_dir.join(ca_file)));
    }
    let has_ca = !files.is_empty();
    if let (Some(cert_file), Some(key_file)) = (&settings.cert_file, &settings.key_file) {
        files.push(WatchedFile::new("tls.cert_file", base_dir.join(cert_file)));
        files.push(WatchedFile::new("tls.key_file", base_dir.join(key_file)));
    }
    let contents = files.iter().map(WatchedFile::read).collect::<anyhow::Result<Vec<_>>>()?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let material = ClientMaterial::parse(&files, &contents, has_ca, &provider)?;
    let not_after = material.not_after;
    let verifier = material.verifier.map(|v| Arc::new(ReloadingServerVerifier::new(v)));
    let cert = material.key.map(|key| Arc::new(ReloadingCert::new(key)));

    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("the ring crypto provider always supports TLS 1.2/1.3");
    // The graph rejects `insecure_skip_verify` with `ca_file` for every client; with a client
    // certificate it's legal (mutual TLS, no server verification).
    let builder = match &verifier {
        _ if settings.insecure_skip_verify => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert((*provider).clone()))),
        Some(verifier) => builder
            .dangerous()
            .with_custom_certificate_verifier(verifier.clone() as Arc<dyn ServerCertVerifier>),
        None => {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            builder.with_root_certificates(roots)
        }
    };
    let cfg = match &cert {
        Some(cert) => builder.with_client_cert_resolver(cert.clone()),
        None => builder.with_no_client_auth(),
    };

    if !files.is_empty() {
        let load_files = files.clone();
        reloader.register(Registration {
            files,
            contents,
            side: "client",
            not_after,
            load: Box::new(move |contents| {
                let material = ClientMaterial::parse(&load_files, contents, has_ca, &provider)?;
                if let (Some(verifier), Some(next)) = (&verifier, material.verifier) {
                    verifier.swap(next);
                }
                if let (Some(cert), Some(next)) = (&cert, material.key) {
                    cert.swap(next);
                }
                Ok(material.not_after)
            }),
            diag: diag.clone(),
            telemetry: telemetry.clone(),
        });
    }
    Ok(cfg)
}

/// One parsed client file set: everything a swap needs, built before anything swaps.
struct ClientMaterial {
    verifier: Option<Arc<dyn ServerCertVerifier>>,
    key: Option<Arc<CertifiedKey>>,
    not_after: Option<i64>,
}

impl ClientMaterial {
    /// `files` and `contents` are parallel: a CA bundle first when `has_ca`, then a certificate
    /// and its key when two files follow.
    fn parse(
        files: &[WatchedFile],
        contents: &[Vec<u8>],
        has_ca: bool,
        provider: &Arc<CryptoProvider>,
    ) -> anyhow::Result<Self> {
        let verifier = if has_ca {
            let mut roots = rustls::RootCertStore::empty();
            roots.add_parsable_certificates(parse_certs(&files[0], &contents[0])?);
            let verifier =
                WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                    .build()
                    .map_err(|e| files[0].error(format_args!("building a verifier: {e}")))?;
            Some(verifier as Arc<dyn ServerCertVerifier>)
        } else {
            None
        };
        let pair = usize::from(has_ca);
        let (key, not_after) = if files.len() == pair + 2 {
            let (key, not_after) = certified_key(&files[pair..], &contents[pair..], provider)?;
            (Some(key), not_after)
        } else {
            (None, None)
        };
        Ok(Self { verifier, key, not_after })
    }
}

/// Every certificate in one PEM file, failing on a file with none.
fn parse_certs(
    file: &WatchedFile,
    contents: &[u8],
) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_slice_iter(contents)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| file.error(format_args!("parsing: {e}")))?;
    if certs.is_empty() {
        return Err(file.error("has no PEM certificate"));
    }
    Ok(certs)
}

/// A certificate resolver whose certificate and key can be swapped while the config holding it is
/// in use: a listener's server certificate, or a client's certificate for mutual TLS. Each full
/// handshake resolves the current pair once.
pub struct ReloadingCert {
    current: RwLock<Arc<CertifiedKey>>,
}

impl ReloadingCert {
    pub fn new(key: Arc<CertifiedKey>) -> Self {
        Self { current: RwLock::new(key) }
    }

    /// The pair the next handshake gets.
    pub fn current(&self) -> Arc<CertifiedKey> {
        self.current.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }

    /// Replaces the pair for every later handshake. An open connection keeps its own.
    pub fn swap(&self, key: Arc<CertifiedKey>) {
        *self.current.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = key;
    }
}

/// Not derived: `CertifiedKey`'s `Debug` would print its signing key's.
impl fmt::Debug for ReloadingCert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReloadingCert")
    }
}

impl ResolvesServerCert for ReloadingCert {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }
}

/// Sends the pair whatever CAs the server hints, as rustls's own single-certificate resolver does.
impl ResolvesClientCert for ReloadingCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// A client-certificate verifier that delegates to a swappable inner verifier, for a listener's
/// `client_ca_file`.
///
/// `root_hint_subjects` returns a slice borrowed from `self`, which a lock guard can't hand out.
/// So every distinct hint list this verifier has held stays in an append-only chain for its
/// lifetime, and the call returns the newest. The chain grows by one node per reload that changes
/// the CA subjects, a few hundred bytes each.
pub struct ReloadingClientVerifier {
    current: RwLock<Arc<dyn ClientCertVerifier>>,
    hints: HintNode,
    /// Serializes appends to `hints`, so the newest node is the one `current` came from.
    swapping: Mutex<()>,
}

/// One hint list in [`ReloadingClientVerifier`]'s chain. `OnceLock` makes `next` settable through
/// `&self` and borrowable for as long as `self` lives, with no `unsafe`.
struct HintNode {
    hints: Vec<DistinguishedName>,
    next: OnceLock<Box<HintNode>>,
}

impl HintNode {
    fn new(hints: Vec<DistinguishedName>) -> Self {
        Self { hints, next: OnceLock::new() }
    }

    fn newest(&self) -> &HintNode {
        let mut node = self;
        while let Some(next) = node.next.get() {
            node = next;
        }
        node
    }
}

impl ReloadingClientVerifier {
    pub fn new(inner: Arc<dyn ClientCertVerifier>) -> Self {
        let hints = HintNode::new(inner.root_hint_subjects().to_vec());
        Self { current: RwLock::new(inner), hints, swapping: Mutex::new(()) }
    }

    fn current(&self) -> Arc<dyn ClientCertVerifier> {
        self.current.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }

    /// Replaces the inner verifier for every later handshake.
    pub fn swap(&self, next: Arc<dyn ClientCertVerifier>) {
        let _swapping = self.swapping.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let newest = self.hints.newest();
        if !same_hints(&newest.hints, next.root_hint_subjects()) {
            // Can't fail: `swapping` is held, so nothing else sets `newest.next`.
            let _ = newest.next.set(Box::new(HintNode::new(next.root_hint_subjects().to_vec())));
        }
        *self.current.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
    }

    /// How many hint lists the chain holds, for the bound's tests.
    #[cfg(test)]
    fn hint_lists(&self) -> usize {
        let mut count = 1;
        let mut node = &self.hints;
        while let Some(next) = node.next.get() {
            count += 1;
            node = next;
        }
        count
    }
}

/// `DistinguishedName` has no `PartialEq`; its DER bytes do.
fn same_hints(a: &[DistinguishedName], b: &[DistinguishedName]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.as_ref() == b.as_ref())
}

impl fmt::Debug for ReloadingClientVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReloadingClientVerifier")
    }
}

impl ClientCertVerifier for ReloadingClientVerifier {
    fn offer_client_auth(&self) -> bool {
        self.current().offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.current().client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &self.hints.newest().hints
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.current().verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.current().supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.current().requires_raw_public_keys()
    }
}

/// A server-certificate verifier that delegates to a swappable inner verifier, for a client's
/// `ca_file`.
pub struct ReloadingServerVerifier {
    current: RwLock<Arc<dyn ServerCertVerifier>>,
}

impl ReloadingServerVerifier {
    pub fn new(inner: Arc<dyn ServerCertVerifier>) -> Self {
        Self { current: RwLock::new(inner) }
    }

    fn current(&self) -> Arc<dyn ServerCertVerifier> {
        self.current.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }

    /// Replaces the inner verifier for every later full handshake.
    pub fn swap(&self, next: Arc<dyn ServerCertVerifier>) {
        *self.current.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
    }
}

impl fmt::Debug for ReloadingServerVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReloadingServerVerifier")
    }
}

/// `root_hint_subjects` keeps the trait's default, `None`, which is what `WebPkiServerVerifier`
/// returns too; a delegated borrow couldn't outlive the lock guard.
impl ServerCertVerifier for ReloadingServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.current().verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.current().supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.current().requires_raw_public_keys()
    }
}

/// `tls.insecure_skip_verify`'s [`ServerCertVerifier`]: skips chain and hostname validation, but
/// still verifies the handshake signature with the provider's algorithms.
#[derive(Debug)]
pub struct AcceptAnyServerCert(CryptoProvider);

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
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
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// One file of a watched set: the config key it came from, for messages, and its resolved path.
#[derive(Debug, Clone)]
pub struct WatchedFile {
    pub label: &'static str,
    pub path: PathBuf,
}

impl WatchedFile {
    pub fn new(label: &'static str, path: PathBuf) -> Self {
        Self { label, path }
    }

    fn read(&self) -> anyhow::Result<Vec<u8>> {
        std::fs::read(&self.path).map_err(|e| self.error(format_args!("reading: {e}")))
    }

    fn error(&self, what: impl fmt::Display) -> anyhow::Error {
        anyhow::anyhow!("{} {}: {what}", self.label, self.path.display())
    }
}

/// Parses a changed set's contents and swaps them in, returning the new leaf certificate's
/// `notAfter` when there is one. Called with the files' contents in registration order, and only
/// after every file read.
pub type Loader = Box<dyn FnMut(&[Vec<u8>]) -> anyhow::Result<Option<i64>> + Send>;

/// What a component hands [`TlsReloader::register`]: its files, the contents its startup build
/// loaded, and how to load new contents.
pub struct Registration {
    pub files: Vec<WatchedFile>,
    /// What the startup build loaded, parallel to `files`: the baseline a check compares with.
    pub contents: Vec<Vec<u8>>,
    /// `server` or `client`, the `side` tag on `logit.tls.certificate.not_after`.
    pub side: &'static str,
    /// The startup leaf certificate's `notAfter`, in unix seconds.
    pub not_after: Option<i64>,
    pub load: Loader,
    /// The owning component's, so a reload's report and points carry its identity.
    pub diag: Diagnostics,
    pub telemetry: Telemetry,
}

/// How often [`TlsReloader::run`] re-emits each set's cached `logit.tls.certificate.not_after`.
/// The telemetry drain empties the point map, so a gauge written only at registration or on a
/// reload would leave the series after one `internal` interval; the kernel samplers in
/// `logit-inputs` re-emit their constants on the same one-second tick.
pub const GAUGE_INTERVAL: Duration = Duration::from_secs(1);

/// `NotAfter`'s value for "no certificate expiry to report".
const NO_NOT_AFTER: i64 = i64::MIN;

/// One set's cached expiry and where to report it. Shared between the set's [`Watched`], which
/// updates it on a reload, and the gauge tick, which reads it without touching the set's lock,
/// so a check reading files never delays the tick.
struct NotAfter {
    value: AtomicI64,
    side: &'static str,
    telemetry: Telemetry,
}

impl NotAfter {
    fn set(&self, not_after: Option<i64>) {
        self.value.store(not_after.unwrap_or(NO_NOT_AFTER), Ordering::Relaxed);
    }

    fn get(&self) -> Option<i64> {
        Some(self.value.load(Ordering::Relaxed)).filter(|&v| v != NO_NOT_AFTER)
    }

    fn emit(&self) {
        if let Some(not_after) = self.get() {
            self.telemetry.gauge(
                "logit.tls.certificate.not_after",
                not_after as f64,
                &[("side", self.side)],
            );
        }
    }
}

/// A registered set and its check state.
struct Watched {
    files: Vec<WatchedFile>,
    /// The contents of the last load attempted, successful or not; `None` for a file that
    /// couldn't be read.
    attempted: Vec<Option<Vec<u8>>>,
    not_after: Arc<NotAfter>,
    load: Loader,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl Watched {
    fn check(&mut self) {
        let read: Vec<Result<Vec<u8>, anyhow::Error>> =
            self.files.iter().map(WatchedFile::read).collect();
        let unchanged = read
            .iter()
            .zip(&self.attempted)
            .all(|(now, before)| now.as_ref().ok() == before.as_ref());
        if !unchanged {
            self.attempted = read.iter().map(|r| r.as_ref().ok().cloned()).collect();
            let loaded = read
                .into_iter()
                .collect::<anyhow::Result<Vec<_>>>()
                .and_then(|contents| (self.load)(&contents));
            match loaded {
                Ok(not_after) => {
                    self.not_after.set(not_after);
                    self.not_after.emit();
                    self.telemetry.count("logit.tls.reloads", 1.0, &[("outcome", "reloaded")]);
                    self.diag.info("tls_reloaded", self.reloaded_message());
                }
                Err(err) => {
                    self.telemetry.count("logit.tls.reloads", 1.0, &[("outcome", "failed")]);
                    self.diag.warn_throttled(
                        "tls_reload_failed",
                        format_args!("TLS reload failed, still using the previous files: {err:#}"),
                    );
                }
            }
        }
    }

    fn reloaded_message(&self) -> String {
        let files: Vec<String> =
            self.files.iter().map(|f| format!("{} {}", f.label, f.path.display())).collect();
        let expiry =
            match self.not_after.get().and_then(|secs| jiff::Timestamp::from_second(secs).ok()) {
                Some(not_after) => format!("; certificate valid until {not_after}"),
                None => String::new(),
            };
        format!("reloaded TLS files {}{expiry}", files.join(", "))
    }
}

/// The process's registry of watched TLS file sets, and the task that checks them. `Clone` shares
/// one registry.
///
/// Every TLS component registers at build time ([`build_server_config`], [`build_client_config`]);
/// `logit run` spawns [`TlsReloader::run`] once. Tests call [`TlsReloader::check_now`] instead of
/// waiting out an interval.
#[derive(Clone, Default)]
pub struct TlsReloader {
    /// A `std::sync::Mutex`: held across file reads only on a blocking thread, never across an
    /// `.await`.
    sets: Arc<Mutex<Vec<Watched>>>,
    /// Every set's expiry, for the gauge tick. Never held across a file read.
    not_after: Arc<Mutex<Vec<Arc<NotAfter>>>>,
}

impl fmt::Debug for TlsReloader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsReloader").field("sets", &self.len()).finish()
    }
}

impl TlsReloader {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Watched>> {
        self.sets.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Adds a set, and emits its `logit.tls.certificate.not_after` gauge.
    pub fn register(&self, registration: Registration) {
        let not_after = Arc::new(NotAfter {
            value: AtomicI64::new(NO_NOT_AFTER),
            side: registration.side,
            telemetry: registration.telemetry.clone(),
        });
        not_after.set(registration.not_after);
        not_after.emit();
        self.not_after
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(not_after.clone());
        self.lock().push(Watched {
            files: registration.files,
            attempted: registration.contents.into_iter().map(Some).collect(),
            not_after,
            load: registration.load,
            diag: registration.diag,
            telemetry: registration.telemetry,
        });
    }

    /// Re-emits every set's cached `logit.tls.certificate.not_after`. Reads no file.
    pub fn emit_gauges(&self) {
        for not_after in self.not_after.lock().unwrap_or_else(|p| p.into_inner()).iter() {
            not_after.emit();
        }
    }

    /// How many sets are registered.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Checks every set once, loading any whose contents changed. Blocking: it reads files.
    pub fn check_now(&self) {
        for watched in self.lock().iter_mut() {
            watched.check();
        }
    }

    /// Checks every `interval`, and at once on each change of `hangup` (the SIGHUP reopen
    /// generation); a zero `interval` turns the timer off and leaves SIGHUP. Re-emits the expiry
    /// gauges every [`GAUGE_INTERVAL`] whatever `interval` is. Runs until aborted.
    pub async fn run(self, interval: Duration, mut hangup: watch::Receiver<u64>) {
        let delayed = |period: Duration| {
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker
        };
        let mut gauges = delayed(GAUGE_INTERVAL);
        let mut checks = (!interval.is_zero()).then(|| delayed(interval));
        let mut hangup_open = true;
        loop {
            tokio::select! {
                _ = gauges.tick() => {
                    self.emit_gauges();
                    continue;
                }
                _ = async {
                    match &mut checks {
                        Some(checks) => {
                            checks.tick().await;
                        }
                        None => std::future::pending::<()>().await,
                    }
                } => {}
                changed = hangup.changed(), if hangup_open => {
                    if changed.is_err() {
                        hangup_open = false;
                        continue;
                    }
                }
            }
            let reloader = self.clone();
            // A loader that panics leaves every lock here usable (each tolerates poison), and the
            // next change to the files is tried.
            let _ = tokio::task::spawn_blocking(move || reloader.check_now()).await;
        }
    }
}

/// The `notAfter` of a DER-encoded X.509 certificate, in unix seconds, or `None` for anything
/// this reader doesn't expect.
///
/// A narrow walk, not a parser: Certificate, TBSCertificate, past the optional `[0]` version, the
/// serial, the signature algorithm, and the issuer, to Validity, then its second time. rustls
/// parses the same certificate before it's served, so the walk checks only what it needs to stay
/// in bounds, and `None` leaves the expiry gauge out without affecting the load.
pub fn not_after(der: &[u8]) -> Option<i64> {
    const SEQUENCE: u8 = 0x30;
    const INTEGER: u8 = 0x02;
    const VERSION: u8 = 0xa0;

    let (certificate, _) = der_expect(der, SEQUENCE)?;
    let (tbs, _) = der_expect(certificate, SEQUENCE)?;
    let mut rest = tbs;
    if rest.first() == Some(&VERSION) {
        rest = der_next(rest)?.2;
    }
    let (_serial, rest) = der_expect(rest, INTEGER)?;
    let (_signature, rest) = der_expect(rest, SEQUENCE)?;
    let (_issuer, rest) = der_expect(rest, SEQUENCE)?;
    let (validity, _) = der_expect(rest, SEQUENCE)?;
    let (not_before_tag, _, rest) = der_next(validity)?;
    if !is_der_time(not_before_tag) {
        return None;
    }
    let (tag, not_after, _) = der_next(rest)?;
    der_time(tag, not_after)
}

const UTC_TIME: u8 = 0x17;
const GENERALIZED_TIME: u8 = 0x18;

fn is_der_time(tag: u8) -> bool {
    tag == UTC_TIME || tag == GENERALIZED_TIME
}

/// The next element's tag, value, and the bytes after it. `None` on a high-tag-number form, an
/// indefinite length, a length past four bytes, or a value past the end of `input`.
fn der_next(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    if tag & 0x1f == 0x1f {
        return None;
    }
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 4 {
            return None;
        }
        let (bytes, rest) = rest.split_at_checked(count)?;
        let len = bytes
            .iter()
            .try_fold(0usize, |len, &b| len.checked_mul(256)?.checked_add(usize::from(b)))?;
        (len, rest)
    };
    let (value, rest) = rest.split_at_checked(len)?;
    Some((tag, value, rest))
}

/// [`der_next`], requiring `tag`.
fn der_expect(input: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    let (found, value, rest) = der_next(input)?;
    (found == tag).then_some((value, rest))
}

/// A `UTCTime` (`YYMMDDHHMMSSZ`, a year below 50 in the 2000s per RFC 5280) or `GeneralizedTime`
/// (`YYYYMMDDHHMMSSZ`) as unix seconds. RFC 5280 requires both forms to carry seconds and `Z`,
/// so nothing else parses.
fn der_time(tag: u8, value: &[u8]) -> Option<i64> {
    let (zone, digits) = value.split_last()?;
    if *zone != b'Z' || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let number = |range: std::ops::Range<usize>| -> Option<i16> {
        digits.get(range)?.iter().try_fold(0i16, |n, &d| Some(n * 10 + i16::from(d - b'0')))
    };
    let (year, rest) = match (tag, digits.len()) {
        (UTC_TIME, 12) => {
            let yy = number(0..2)?;
            (if yy < 50 { 2000 + yy } else { 1900 + yy }, 2)
        }
        (GENERALIZED_TIME, 14) => (number(0..4)?, 4),
        _ => return None,
    };
    let field = |at: usize| -> Option<i8> { i8::try_from(number(rest + at..rest + at + 2)?).ok() };
    let datetime =
        jiff::civil::DateTime::new(year, field(0)?, field(2)?, field(4)?, field(6)?, field(8)?, 0)
            .ok()?;
    // Civil arithmetic, not `jiff::Timestamp`: a timestamp's range stops short of
    // 9999-12-31T23:59:59Z, the notAfter RFC 5280 gives a certificate with no expiry.
    Some(datetime.duration_since(jiff::civil::datetime(1970, 1, 1, 0, 0, 0, 0)).as_secs())
}

#[cfg(test)]
mod tests;
