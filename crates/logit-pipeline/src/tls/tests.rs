use super::*;
use crate::test_util::{scratch_dir, TelemetryProbe, RECV_TIMEOUT};
use proptest::prelude::*;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{client::TlsStream, TlsAcceptor, TlsConnector};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls").join(name)
}

fn fixture_der(name: &str) -> Vec<u8> {
    CertificateDer::from_pem_file(fixture(name)).expect("fixture certificate").to_vec()
}

/// `ca.pem` and `server.pem`'s notAfter, 2126-08-10T23:25:13Z (`openssl x509 -enddate`).
const BASE_NOT_AFTER: i64 = 4_942_077_913;
/// `server-b.pem`'s, 2126-09-13T20:27:39Z.
const SERVER_B_NOT_AFTER: i64 = 4_945_004_859;

const NOT_AFTER: &str = "logit.tls.certificate.not_after";
const RELOADS: &str = "logit.tls.reloads";

// --- not_after ---

#[test]
fn not_after_reads_a_generalized_time_from_each_fixture() {
    assert_eq!(not_after(&fixture_der("ca.pem")), Some(BASE_NOT_AFTER));
    assert_eq!(not_after(&fixture_der("server.pem")), Some(BASE_NOT_AFTER));
    assert_eq!(not_after(&fixture_der("client.pem")), Some(BASE_NOT_AFTER));
    assert_eq!(not_after(&fixture_der("server-b.pem")), Some(SERVER_B_NOT_AFTER));
}

#[test]
fn not_after_reads_a_utc_time() {
    // 2049-12-31T23:59:59Z: the last instant a UTCTime can name.
    assert_eq!(not_after(&fixture_der("utctime.pem")), Some(2_524_607_999));
}

/// A DER element with `tag` around `value`, in short or long form as its length needs.
fn tlv(tag: u8, value: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    match value.len() {
        len @ 0..=0x7f => out.push(len as u8),
        len @ 0x80..=0xff => out.extend([0x81, len as u8]),
        len => out.extend([0x82, (len >> 8) as u8, len as u8]),
    }
    out.extend_from_slice(value);
    out
}

/// A certificate shaped as far as `not_after` reads: no `[0]` version, an empty signature
/// algorithm and issuer, and the two times given.
fn synthetic(not_before: (u8, &[u8]), not_after: (u8, &[u8])) -> Vec<u8> {
    let validity = [tlv(not_before.0, not_before.1), tlv(not_after.0, not_after.1)].concat();
    let tbs = [tlv(0x02, &[1]), tlv(0x30, &[]), tlv(0x30, &[]), tlv(0x30, &validity)].concat();
    tlv(0x30, &tlv(0x30, &tbs))
}

#[test]
fn not_after_maps_a_utc_time_year_of_50_or_more_into_the_1900s() {
    let der = synthetic((0x17, b"990101000000Z"), (0x17, b"991231235959Z"));
    assert_eq!(not_after(&der), Some(946_684_799));
    let der = synthetic((0x17, b"490101000000Z"), (0x17, b"500101000000Z"));
    assert_eq!(not_after(&der), Some(-631_152_000));
}

#[test]
fn not_after_reads_the_generalized_time_rfc_5280_uses_for_no_expiry() {
    let der = synthetic((0x18, b"20260101000000Z"), (0x18, b"99991231235959Z"));
    assert_eq!(not_after(&der), Some(253_402_300_799));
}

#[test]
fn not_after_rejects_a_time_rfc_5280_doesnt_allow() {
    for bad in [
        (0x18, &b"20260101000000.5Z"[..]), // fractional seconds
        (0x18, b"20260101000000+0000"),    // an offset instead of Z
        (0x17, b"2601010000Z"),            // no seconds
        (0x18, b"260101000000Z"),          // a UTCTime's length under GeneralizedTime's tag
        (0x18, b"20260230000000Z"),        // February 30th
        (0x18, b"20261301000000Z"),        // month 13
        (0x18, b"20260101250000Z"),        // hour 25
        (0x18, b"2026010100000AZ"),        // a non-digit
        (0x13, b"20260101000000Z"),        // not a time tag at all
    ] {
        let der = synthetic((0x18, b"20260101000000Z"), bad);
        assert_eq!(not_after(&der), None, "{:?}", String::from_utf8_lossy(bad.1));
    }
}

#[test]
fn not_after_rejects_a_not_before_that_isnt_a_time() {
    let der = synthetic((0x04, b"20260101000000Z"), (0x18, b"21260101000000Z"));
    assert_eq!(not_after(&der), None);
}

#[test]
fn not_after_rejects_every_truncation_of_a_real_certificate() {
    let der = fixture_der("server.pem");
    for len in 0..der.len() {
        assert_eq!(not_after(&der[..len]), None, "truncated to {len} bytes");
    }
}

#[test]
fn not_after_rejects_lengths_der_never_uses() {
    // Indefinite length (BER only).
    assert_eq!(not_after(&[0x30, 0x80, 0x00, 0x00]), None);
    // A five-byte length.
    assert_eq!(not_after(&[0x30, 0x85, 0, 0, 0, 0, 1, 0]), None);
    // A four-byte length past the input.
    assert_eq!(not_after(&[0x30, 0x84, 0xff, 0xff, 0xff, 0xff]), None);
    // High-tag-number form.
    assert_eq!(not_after(&[0x1f, 0x01, 0x00]), None);
}

#[test]
fn not_after_survives_every_single_byte_corruption_of_a_real_certificate() {
    let der = fixture_der("server.pem");
    for at in 0..der.len() {
        for flip in [0x01, 0x80, 0xff] {
            let mut corrupt = der.clone();
            corrupt[at] ^= flip;
            let _ = not_after(&corrupt);
        }
    }
}

proptest! {
    #[test]
    fn not_after_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = not_after(&bytes);
    }
}

// --- reload ---

/// A listener's file set in a scratch directory, starting as `server.pem`/`server.key`.
struct Set {
    dir: PathBuf,
    settings: TlsServerSettings,
}

impl Set {
    fn new(label: &str, client_ca: Option<&str>) -> Self {
        let dir = scratch_dir(label);
        std::fs::copy(fixture("server.pem"), dir.join("cert.pem")).unwrap();
        std::fs::copy(fixture("server.key"), dir.join("key.pem")).unwrap();
        if let Some(ca) = client_ca {
            std::fs::copy(fixture(ca), dir.join("client-ca.pem")).unwrap();
        }
        let settings = TlsServerSettings {
            cert_file: "cert.pem".into(),
            key_file: "key.pem".into(),
            client_ca_file: client_ca.map(|_| "client-ca.pem".into()),
        };
        Self { dir, settings }
    }

    /// Overwrites `name` in place with fixture `from`.
    fn write(&self, name: &str, from: &str) {
        std::fs::write(self.dir.join(name), std::fs::read(fixture(from)).unwrap()).unwrap();
    }
}

struct Listener {
    addr: SocketAddr,
    reloader: TlsReloader,
    probe: TelemetryProbe,
}

impl Listener {
    async fn start(set: &Set) -> Self {
        let probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("in", "otlp_in", "listener");
        let diag = Diagnostics::new("in").with_telemetry(telemetry.clone());
        let reloader = TlsReloader::new();
        let cfg = build_server_config(&set.settings, &set.dir, &[], &reloader, &diag, &telemetry)
            .expect("building the server config");
        let addr = serve_echo(cfg).await;
        Self { addr, reloader, probe }
    }
}

/// Accepts TLS on loopback and echoes every byte back on each connection.
async fn serve_echo(cfg: rustls::ServerConfig) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(cfg));
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(tcp).await else { return };
                let mut buf = [0u8; 64];
                while let Ok(n @ 1..) = tls.read(&mut buf).await {
                    if tls.write_all(&buf[..n]).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    addr
}

/// A client trusting `ca.pem`, presenting fixture `client` (`<client>.pem`/`<client>.key`) if
/// given.
fn connector(client: Option<&str>) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from_pem_file(fixture("ca.pem")).unwrap()).unwrap();
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots);
    let cfg = match client {
        Some(name) => builder
            .with_client_auth_cert(
                vec![CertificateDer::from_pem_file(fixture(&format!("{name}.pem"))).unwrap()],
                PrivateKeyDer::from_pem_file(fixture(&format!("{name}.key"))).unwrap(),
            )
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    TlsConnector::from(Arc::new(cfg))
}

async fn connect(addr: SocketAddr, connector: &TlsConnector) -> TlsStream<TcpStream> {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
    tokio::time::timeout(RECV_TIMEOUT, connector.connect(name, tcp))
        .await
        .expect("handshake timed out")
        .expect("handshake")
}

/// The leaf certificate the server presented.
fn presented(stream: &TlsStream<TcpStream>) -> Vec<u8> {
    stream.get_ref().1.peer_certificates().expect("a server certificate")[0].to_vec()
}

/// Whether `stream` echoes a frame. A server that refused the client's certificate under
/// TLS 1.3 does so after the client's side of the handshake completes, so this, not
/// [`connect`], is where a refusal shows.
async fn round_trips(stream: &mut TlsStream<TcpStream>) -> bool {
    tokio::time::timeout(RECV_TIMEOUT, async {
        if stream.write_all(b"ping").await.is_err() {
            return false;
        }
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.is_ok() && &buf == b"ping"
    })
    .await
    .expect("the echo neither answered nor closed")
}

#[tokio::test]
async fn a_rewritten_certificate_reaches_the_next_handshake_and_an_open_connection_keeps_its_own() {
    let set = Set::new("tls-rotate", None);
    let mut listener = Listener::start(&set).await;
    let client = connector(None);
    let mut before = connect(listener.addr, &client).await;
    assert_eq!(presented(&before), fixture_der("server.pem"));
    assert_eq!(listener.probe.gauge(NOT_AFTER, &[("side", "server")]), Some(BASE_NOT_AFTER as f64));

    set.write("cert.pem", "server-b.pem");
    set.write("key.pem", "server-b.key");
    listener.reloader.check_now();

    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "reloaded")]), 1.0);
    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "failed")]), 0.0);
    assert_eq!(
        listener.probe.gauge(NOT_AFTER, &[("side", "server")]),
        Some(SERVER_B_NOT_AFTER as f64)
    );
    let after = connect(listener.addr, &client).await;
    assert_eq!(presented(&after), fixture_der("server-b.pem"));
    assert!(round_trips(&mut before).await, "the connection opened before the reload broke");
}

/// The layout a mounted Kubernetes Secret has: each file is a symlink through `..data`, which
/// the kubelet repoints at a new directory with one rename.
#[cfg(unix)]
#[tokio::test]
async fn a_symlink_swap_like_a_kubernetes_secret_update_is_a_change() {
    use std::os::unix::fs::symlink;

    let dir = scratch_dir("tls-symlink-swap");
    for (generation, cert, key) in
        [("..gen_a", "server.pem", "server.key"), ("..gen_b", "server-b.pem", "server-b.key")]
    {
        std::fs::create_dir(dir.join(generation)).unwrap();
        std::fs::copy(fixture(cert), dir.join(generation).join("tls.crt")).unwrap();
        std::fs::copy(fixture(key), dir.join(generation).join("tls.key")).unwrap();
    }
    symlink("..gen_a", dir.join("..data")).unwrap();
    symlink("..data/tls.crt", dir.join("tls.crt")).unwrap();
    symlink("..data/tls.key", dir.join("tls.key")).unwrap();
    let set = Set {
        dir: dir.clone(),
        settings: TlsServerSettings {
            cert_file: "tls.crt".into(),
            key_file: "tls.key".into(),
            client_ca_file: None,
        },
    };
    let mut listener = Listener::start(&set).await;
    let client = connector(None);
    assert_eq!(presented(&connect(listener.addr, &client).await), fixture_der("server.pem"));

    symlink("..gen_b", dir.join("..data_tmp")).unwrap();
    std::fs::rename(dir.join("..data_tmp"), dir.join("..data")).unwrap();
    listener.reloader.check_now();

    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "reloaded")]), 1.0);
    assert_eq!(presented(&connect(listener.addr, &client).await), fixture_der("server-b.pem"));
}

#[tokio::test]
async fn a_key_that_doesnt_match_keeps_the_old_certificate_and_counts_one_failure() {
    let set = Set::new("tls-mismatch", None);
    let mut listener = Listener::start(&set).await;
    let client = connector(None);

    // The certificate lands before its key, as a tool writing them one at a time leaves them.
    set.write("cert.pem", "server-b.pem");
    listener.reloader.check_now();
    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "failed")]), 1.0);
    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "reloaded")]), 0.0);
    assert_eq!(presented(&connect(listener.addr, &client).await), fixture_der("server.pem"));
    assert_eq!(listener.probe.gauge(NOT_AFTER, &[("side", "server")]), Some(BASE_NOT_AFTER as f64));

    // The same bytes again: already attempted, so not counted again.
    listener.reloader.check_now();
    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "failed")]), 1.0);
    assert_eq!(listener.probe.gauge(NOT_AFTER, &[("side", "server")]), Some(BASE_NOT_AFTER as f64));

    // The key lands: new content, so the set loads.
    set.write("key.pem", "server-b.key");
    listener.reloader.check_now();
    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "reloaded")]), 1.0);
    assert_eq!(presented(&connect(listener.addr, &client).await), fixture_der("server-b.pem"));
}

#[tokio::test]
async fn a_missing_file_fails_the_check_and_keeps_serving() {
    let set = Set::new("tls-missing", None);
    let mut listener = Listener::start(&set).await;
    std::fs::remove_file(set.dir.join("key.pem")).unwrap();
    listener.reloader.check_now();
    listener.reloader.check_now();
    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "failed")]), 1.0);
    assert_eq!(
        presented(&connect(listener.addr, &connector(None)).await),
        fixture_der("server.pem")
    );
}

#[tokio::test]
async fn an_unchanged_set_doesnt_reload() {
    let set = Set::new("tls-unchanged", None);
    let mut listener = Listener::start(&set).await;
    listener.reloader.check_now();
    listener.reloader.check_now();
    let totals = listener.probe.poll();
    assert!(!totals.has(RELOADS, &[]), "an unchanged set counted a reload");
    assert_eq!(totals.gauge(NOT_AFTER, &[("side", "server")]), Some(BASE_NOT_AFTER as f64));
}

/// Each connection gets a new client config and so a new session cache: a resumed session skips
/// client-certificate verification, and a reload is for rotation, not revoking a session.
#[tokio::test]
async fn a_rotated_client_ca_admits_its_clients_and_refuses_the_old_ones() {
    let set = Set::new("tls-client-ca", Some("ca.pem"));
    let mut listener = Listener::start(&set).await;
    assert!(round_trips(&mut connect(listener.addr, &connector(Some("client"))).await).await);
    assert!(
        !round_trips(&mut connect(listener.addr, &connector(Some("client-other"))).await).await
    );

    set.write("client-ca.pem", "other-ca.pem");
    listener.reloader.check_now();

    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "reloaded")]), 1.0);
    assert!(round_trips(&mut connect(listener.addr, &connector(Some("client-other"))).await).await);
    assert!(!round_trips(&mut connect(listener.addr, &connector(Some("client"))).await).await);
}

#[test]
fn the_hint_chain_grows_only_when_the_ca_subjects_change() {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier_for = |ca: &str| {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from_pem_file(fixture(ca)).unwrap()).unwrap();
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
            .build()
            .unwrap()
    };
    let reloading = ReloadingClientVerifier::new(verifier_for("ca.pem"));
    let ca_hints = verifier_for("ca.pem").root_hint_subjects().to_vec();
    assert!(same_hints(reloading.root_hint_subjects(), &ca_hints));

    reloading.swap(verifier_for("ca.pem"));
    assert_eq!(reloading.hint_lists(), 1);

    reloading.swap(verifier_for("other-ca.pem"));
    assert_eq!(reloading.hint_lists(), 2);
    assert!(same_hints(
        reloading.root_hint_subjects(),
        verifier_for("other-ca.pem").root_hint_subjects()
    ));
    assert!(!same_hints(reloading.root_hint_subjects(), &ca_hints));
}

#[test]
fn startup_fails_on_a_key_that_doesnt_match() {
    let set = Set::new("tls-startup-mismatch", None);
    set.write("key.pem", "server-b.key");
    let reloader = TlsReloader::new();
    let err = build_server_config(
        &set.settings,
        &set.dir,
        &[],
        &reloader,
        &Diagnostics::default(),
        &Telemetry::default(),
    )
    .expect_err("a mismatched key built a config");
    assert!(err.to_string().contains("tls.cert_file"), "{err}");
    assert!(reloader.is_empty(), "a failed build registered its files");
}

#[test]
fn startup_names_the_file_it_couldnt_read() {
    let set = Set::new("tls-startup-missing", None);
    std::fs::remove_file(set.dir.join("key.pem")).unwrap();
    let err = build_server_config(
        &set.settings,
        &set.dir,
        &[],
        &TlsReloader::new(),
        &Diagnostics::default(),
        &Telemetry::default(),
    )
    .expect_err("a missing key built a config");
    let message = err.to_string();
    assert!(message.contains("tls.key_file") && message.contains("key.pem"), "{message}");
}

/// `run` checks on each bump of the hangup generation even with the timer off.
#[tokio::test]
async fn run_checks_on_a_hangup_with_the_interval_off() {
    let set = Set::new("tls-run-hangup", None);
    let mut listener = Listener::start(&set).await;
    let (hangup_tx, hangup) = watch::channel(0u64);
    let task = tokio::spawn(listener.reloader.clone().run(Duration::ZERO, hangup));

    set.write("cert.pem", "server-b.pem");
    set.write("key.pem", "server-b.key");
    hangup_tx.send_modify(|g| *g += 1);
    listener
        .probe
        .wait_for("a reload after the hangup", |t| {
            t.sum(RELOADS, &[("outcome", "reloaded")]) == 1.0
        })
        .await;
    task.abort();
}

/// Real time with a short interval, not a paused clock: `check_now` runs on a blocking thread
/// the paused clock can't wait for.
#[tokio::test]
async fn run_checks_on_its_interval() {
    let set = Set::new("tls-run-interval", None);
    let mut listener = Listener::start(&set).await;
    let (_hangup_tx, hangup) = watch::channel(0u64);
    let task = tokio::spawn(listener.reloader.clone().run(Duration::from_millis(20), hangup));

    set.write("cert.pem", "server-b.pem");
    set.write("key.pem", "server-b.key");
    listener
        .probe
        .wait_for("a reload on the interval", |t| t.sum(RELOADS, &[("outcome", "reloaded")]) == 1.0)
        .await;
    task.abort();
}

/// How many `logit.tls.certificate.not_after` points the probe has drained, across every window.
fn not_after_points(totals: &crate::test_util::Totals) -> usize {
    totals
        .events
        .iter()
        .flat_map(|event| event.metrics.iter())
        .filter(|metric| logit_core::interner::resolve(metric.name) == NOT_AFTER)
        .count()
}

/// The drain empties the point map, so the gauge has to be written again for a later window to
/// carry it. `run` does that on its own tick, with polling off and no reload.
#[tokio::test]
async fn run_re_emits_the_expiry_gauge_into_a_later_drain_with_polling_off() {
    let set = Set::new("tls-gauge-tick", None);
    let mut listener = Listener::start(&set).await;
    assert_eq!(not_after_points(listener.probe.poll()), 1, "the registration emit");

    let (_hangup_tx, hangup) = watch::channel(0u64);
    let task = tokio::spawn(listener.reloader.clone().run(Duration::ZERO, hangup));
    listener.probe.wait_for("the gauge in a later window", |t| not_after_points(t) >= 2).await;
    assert_eq!(listener.probe.gauge(NOT_AFTER, &[("side", "server")]), Some(BASE_NOT_AFTER as f64));
    assert!(!listener.probe.poll().has(RELOADS, &[]), "the tick reloaded something");
    task.abort();
}

/// After a failed reload the tick keeps reporting the certificate still being served.
#[tokio::test]
async fn the_gauge_tick_reports_the_old_certificate_after_a_failed_reload() {
    let set = Set::new("tls-gauge-failed", None);
    let mut listener = Listener::start(&set).await;
    set.write("cert.pem", "server-b.pem");
    listener.reloader.check_now();
    assert_eq!(listener.probe.sum(RELOADS, &[("outcome", "failed")]), 1.0);
    let before = not_after_points(listener.probe.poll());

    listener.reloader.emit_gauges();
    let totals = listener.probe.poll();
    assert_eq!(not_after_points(totals), before + 1);
    assert_eq!(totals.gauge(NOT_AFTER, &[("side", "server")]), Some(BASE_NOT_AFTER as f64));
}
