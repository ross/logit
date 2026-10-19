//! `logit ready` and `/readyz` against a real `logit run` with `admin.bind` set (ADR
//! `admin-readiness-endpoint`), spawning the binary twice: once as the service, once as the
//! probe. `crates/logit-cli/src/admin.rs`'s in-module tests drive `serve_on`/`handle` directly;
//! `logit-cli` is a binary crate, so an integration test reaches the rest only by spawning it.

mod support;

use std::process::Stdio;
use std::time::Duration;

use support::{
    logit_ready, logit_ready_env, logit_ready_url, spawn_run, wait_until_probe_succeeds,
    wait_until_ready, KillOnDrop, TempConfig, ADMIN, ANY_PORT, PROCESS_DEADLINE,
};

/// Port `0` in the config, for the admin endpoint and a TCP listener alike, reads back as the
/// port the child bound, and both answer on it.
#[tokio::test(flavor = "multi_thread")]
async fn a_port_0_bind_reports_the_port_the_child_bound() {
    let config = TempConfig::write(
        "admin-any-port",
        format!(
            "admin:\n  bind: \"{ANY_PORT}\"\ncomponents:\n  in:\n    type: statsd_in\n    \
             transport: tcp\n    bind: \"{ANY_PORT}\"\n  out:\n    type: null_out\n    \
             sources: [in]\n"
        ),
    );
    let (_child, stderr) = spawn_run(&config, Stdio::null());
    let [admin_addr, statsd_addr] =
        <[String; 2]>::try_from(stderr.wait_for_bound(&[ADMIN, "in"])).expect("two addresses");
    for addr in [&admin_addr, &statsd_addr] {
        let parsed: std::net::SocketAddr = addr.parse().expect("an IP address and port");
        assert_ne!(parsed.port(), 0, "{addr} names the requested port, not the bound one");
    }
    assert_eq!(wait_until_ready(&admin_addr).await, "ok");
    std::net::TcpStream::connect(&statsd_addr).expect("statsd_in listens on the reported address");
}

#[tokio::test(flavor = "multi_thread")]
async fn logit_ready_reflects_a_real_runs_readiness_then_fails_once_it_exits() {
    let config = TempConfig::write(
        "admin-ready-e2e",
        format!(
            "admin:\n  bind: \"{ANY_PORT}\"\ncomponents:\n  in:\n    type: statsd_in\n    bind: \"{ANY_PORT}\"\n  out:\n    type: stdio_out\n    sources: [in]\n"
        )
        .as_bytes(),
    );

    let (mut child, stderr) = spawn_run(&config, Stdio::null());
    let admin_addr = stderr.wait_for_bound(&[ADMIN]).remove(0);

    // Retry the probe under test as its own readiness signal rather than a fixed sleep.
    let word = wait_until_ready(&admin_addr).await;
    assert_eq!(word, "ok");

    // Kill the pipeline, then confirm `logit ready` fails against the closed port: it reflects
    // live state, not a cached first success.
    child.0.kill().expect("killing the running logit process");
    child.0.wait().expect("waiting for logit to exit");

    let deadline = std::time::Instant::now() + PROCESS_DEADLINE;
    loop {
        let output = logit_ready(&admin_addr);
        if !output.status.success() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "logit ready should have started failing once the process died"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Between SIGTERM and exit, `/readyz` answers `503 draining` for the whole drain: an
/// orchestrator needs a definite "stop routing here, still finishing", not a refused connection
/// it can't tell from a crash. So the admin server must keep its port open until the drain ends.
///
/// The drain is held open by a sink request that never completes: `internal` emits its own
/// process gauges every 100ms with no traffic needed, `influxdb_out` sends them to a listener this
/// test accepts on and never answers, and `buffer.shutdown_grace` bounds the drain. `influxdb_out`'s
/// HTTP client connects on its first send, not at startup, so the accept is the observable that a
/// batch is in flight.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn readyz_answers_draining_for_the_whole_drain_after_a_sigterm() {
    /// The drain's bound, and the window the draining poll runs over. It is under
    /// `influxdb_out`'s 10s default request timeout, so the held request is still pending when the
    /// grace cuts it and the drain can't end early on a request timeout.
    const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

    let influx = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let influx_addr = influx.local_addr().unwrap();
    let grace_secs = SHUTDOWN_GRACE.as_secs();
    let config = TempConfig::write(
        "admin-draining-e2e",
        format!(
            "admin:\n  bind: \"{ANY_PORT}\"\ncomponents:\n  self:\n    type: internal\n    \
             interval: 100ms\n  out:\n    type: influxdb_out\n    sources: [self]\n    url: \
             \"http://{influx_addr}\"\n    org: o\n    bucket: b\n    token: t\n    buffer:\n      \
             delivery: at_least_once\n      shutdown_grace: {grace_secs}s\n"
        )
        .as_bytes(),
    );

    let (child, stderr) = spawn_run(&config, Stdio::null());
    let admin_addr = stderr.wait_for_bound(&[ADMIN]).remove(0);
    wait_until_ready(&admin_addr).await;

    // The sink's first request, held unanswered for the rest of the test: the drain SIGTERM
    // starts waits on it until the grace cuts it.
    let (_held, _) = tokio::time::timeout(PROCESS_DEADLINE, influx.accept())
        .await
        .expect("influxdb_out never connected to send a batch")
        .expect("accepting influxdb_out's connection");

    // A real SIGTERM, not `Child::kill` (SIGKILL) -- only SIGTERM starts the graceful drain this
    // test exists to probe.
    support::send_signal(&child.0, libc::SIGTERM);

    let deadline = std::time::Instant::now() + SHUTDOWN_GRACE;
    let mut saw_draining = false;
    let mut last_status = String::new();
    while std::time::Instant::now() < deadline {
        let output = logit_ready(&admin_addr);
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        last_status = if stderr.is_empty() { stdout.clone() } else { stderr.clone() };
        if stderr.contains("draining") {
            saw_draining = true;
            break;
        }
        // The process may already have exited; keep polling to the deadline, and a connection
        // failure still lands in the failure message below.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        saw_draining,
        "logit ready never reported 'draining' during the drain window; last observed status: \
         {last_status}"
    );
}

#[test]
fn logit_ready_against_nothing_listening_exits_1() {
    // Port 1: nothing listens there, so the connection fails, the "no admin server reachable"
    // case.
    let output = logit_ready("127.0.0.1:1");
    assert_eq!(output.status.code(), Some(1));
}

// -- `admin.socket` (`docs/adr/listener-port-sharing-and-shutdown-delay.md`) --

/// Writes a config with `admin` as the YAML of its `admin:` block (indented two spaces) and a
/// `statsd_in` into a `stdio_out`, and spawns `logit run` on it. `shutdown.delay` keeps the
/// process draining after a SIGTERM for longer than any test waits.
#[cfg(unix)]
async fn spawn_with_admin(name: &str, admin: &str) -> (TempConfig, KillOnDrop, support::Lines) {
    let config = TempConfig::write(
        name,
        format!(
            "admin:\n{admin}shutdown:\n  delay: 60s\ncomponents:\n  in:\n    type: statsd_in\n    \
             bind: \"{ANY_PORT}\"\n  out:\n    type: stdio_out\n    sources: [in]\n"
        ),
    );
    let (child, stderr) = spawn_run(&config, Stdio::null());
    (config, child, stderr)
}

/// Polls `probe` until its stderr says `draining`, or panics after [`PROCESS_DEADLINE`].
#[cfg(unix)]
async fn wait_until_draining(probe: impl Fn() -> std::process::Output) {
    logit_pipeline::test_util::wait_until_within(
        "logit ready to report draining",
        PROCESS_DEADLINE,
        || String::from_utf8_lossy(&probe().stderr).contains("draining"),
    )
    .await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn logit_ready_over_a_unix_socket_reflects_readiness_and_draining() {
    use std::os::unix::fs::PermissionsExt;
    let dir = logit_pipeline::test_util::scratch_dir("admin-socket");
    let path = dir.join("admin.sock");
    let url = format!("unix:{}", path.display());
    let (_config, child, _stderr) =
        spawn_with_admin("admin-socket", &format!("  socket: \"{}\"\n", path.display())).await;

    let word = wait_until_probe_succeeds(|| logit_ready_url(&url)).await;
    assert_eq!(word, "ok");
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o722, "the socket file's mode is {mode:o}");
    // The URL form names the same socket.
    assert!(logit_ready_url(&format!("unix://{}", path.display())).status.success());

    support::send_signal(&child.0, libc::SIGTERM);
    wait_until_draining(|| logit_ready_url(&url)).await;
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn logit_ready_reads_a_unix_socket_endpoint_from_logit_admin() {
    let dir = logit_pipeline::test_util::scratch_dir("admin-socket-env");
    let path = dir.join("admin.sock");
    let url = format!("unix:{}", path.display());
    let (_config, child, _stderr) =
        spawn_with_admin("admin-socket-env", &format!("  socket: \"{}\"\n", path.display())).await;

    let word = wait_until_probe_succeeds(|| logit_ready_env(&url)).await;
    assert_eq!(word, "ok");

    support::send_signal(&child.0, libc::SIGTERM);
    wait_until_draining(|| logit_ready_env(&url)).await;
    std::fs::remove_dir_all(&dir).ok();
}

/// A socket file left at the path by an earlier run is replaced, not a startup failure.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_admin_socket_file_is_replaced() {
    let dir = logit_pipeline::test_util::scratch_dir("admin-socket-stale");
    let path = dir.join("admin.sock");
    // Dropping the listener leaves its socket file behind, as a crashed process would.
    drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
    assert!(path.exists());
    let url = format!("unix:{}", path.display());
    let (_config, _child, _stderr) =
        spawn_with_admin("admin-socket-stale", &format!("  socket: \"{}\"\n", path.display()))
            .await;

    assert_eq!(wait_until_probe_succeeds(|| logit_ready_url(&url)).await, "ok");
    std::fs::remove_dir_all(&dir).ok();
}

/// With `admin.bind` and `admin.socket` both set, the child logs `admin bound` twice, the TCP
/// address first. [`support::Lines::wait_for_bound`] keeps that first address for [`ADMIN`] while
/// it reads on for `in`'s line, which follows both.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn wait_for_bound_keeps_the_admin_tcp_address_beside_a_socket() {
    let dir = logit_pipeline::test_util::scratch_dir("admin-socket-first-address");
    let path = dir.join("admin.sock");
    let (_config, _child, stderr) = spawn_with_admin(
        "admin-socket-first-address",
        &format!("  bind: \"{ANY_PORT}\"\n  socket: \"{}\"\n", path.display()),
    )
    .await;
    let addrs = stderr.wait_for_bound(&[ADMIN, "in"]);
    let admin: std::net::SocketAddr =
        addrs[0].parse().expect("the TCP address, not the socket path");
    assert_ne!(admin.port(), 0, "{admin}");
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn admin_bind_and_socket_together_both_serve() {
    let dir = logit_pipeline::test_util::scratch_dir("admin-socket-both");
    let path = dir.join("admin.sock");
    let url = format!("unix:{}", path.display());
    let (_config, child, stderr) = spawn_with_admin(
        "admin-socket-both",
        &format!("  bind: \"{ANY_PORT}\"\n  socket: \"{}\"\n", path.display()),
    )
    .await;
    let admin_addr = stderr.wait_for_bound(&[ADMIN]).remove(0);

    assert_eq!(wait_until_ready(&admin_addr).await, "ok");
    assert_eq!(wait_until_probe_succeeds(|| logit_ready_url(&url)).await, "ok");

    support::send_signal(&child.0, libc::SIGTERM);
    wait_until_draining(|| logit_ready(&admin_addr)).await;
    wait_until_draining(|| logit_ready_url(&url)).await;
    std::fs::remove_dir_all(&dir).ok();
}
