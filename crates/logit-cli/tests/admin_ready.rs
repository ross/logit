//! `logit ready` and `/readyz` against a real `logit run` with `admin.bind` set (ADR
//! `admin-readiness-endpoint`), spawning the binary twice: once as the service, once as the
//! probe. `crates/logit-cli/src/admin.rs`'s in-module tests drive `serve_on`/`handle` directly;
//! `logit-cli` is a binary crate, so an integration test reaches the rest only by spawning it.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct TempConfig(PathBuf);

impl TempConfig {
    fn write(name: &str, contents: &[u8]) -> Self {
        let path = std::env::temp_dir()
            .join(format!("logit-admin-ready-test-{name}-{}.yaml", std::process::id()));
        std::fs::File::create(&path)
            .and_then(|mut f| f.write_all(contents))
            .expect("writing the temp config");
        Self(path)
    }
}

impl Drop for TempConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Kills the child on drop, however the test exits -- a panicking assertion must not leave a
/// `logit run` process behind holding its port.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// How long a poll that spawns `logit ready` once per attempt waits for the state it expects: a
/// `logit run` child reporting ready, or exiting. Each attempt is a process spawn, so this is
/// wider than `logit_pipeline::test_util::RECV_TIMEOUT`.
const PROCESS_POLL_DEADLINE: Duration = Duration::from_secs(10);

/// A free loopback port, bound and released. This is the child-process exception to
/// `docs/adr/test-timing-and-observables.md`'s bind-before-spawn rule: the port is handed to a
/// `logit run` child through its config, and the child binds it.
async fn ephemeral_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

fn logit_ready(admin: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_logit"))
        .args(["ready", "--admin", &format!("http://{admin}")])
        .output()
        .expect("spawning the logit binary")
}

#[tokio::test(flavor = "multi_thread")]
async fn logit_ready_reflects_a_real_runs_readiness_then_fails_once_it_exits() {
    let admin_addr = ephemeral_addr().await;
    let statsd_addr = ephemeral_addr().await;
    let config = TempConfig::write(
        "ready-e2e",
        format!(
            "admin:\n  bind: \"{admin_addr}\"\ncomponents:\n  in:\n    type: statsd_in\n    bind: \"{statsd_addr}\"\n  out:\n    type: stdio_out\n    sources: [in]\n"
        )
        .as_bytes(),
    );

    let child = Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("run")
        .arg(&config.0)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning logit run");
    let mut child = KillOnDrop(child);

    // Retry the probe under test as its own readiness signal rather than a fixed sleep.
    let word = wait_until_ready(&admin_addr).await;
    assert_eq!(word, "ok");

    // Kill the pipeline, then confirm `logit ready` fails against the closed port: it reflects
    // live state, not a cached first success.
    child.0.kill().expect("killing the running logit process");
    child.0.wait().expect("waiting for logit to exit");

    let deadline = std::time::Instant::now() + PROCESS_POLL_DEADLINE;
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
    let admin_addr = ephemeral_addr().await;
    let grace_secs = SHUTDOWN_GRACE.as_secs();
    let config = TempConfig::write(
        "draining-e2e",
        format!(
            "admin:\n  bind: \"{admin_addr}\"\ncomponents:\n  self:\n    type: internal\n    \
             interval: 100ms\n  out:\n    type: influxdb_out\n    sources: [self]\n    url: \
             \"http://{influx_addr}\"\n    org: o\n    bucket: b\n    token: t\n    buffer:\n      \
             delivery: at_least_once\n      shutdown_grace: {grace_secs}s\n"
        )
        .as_bytes(),
    );

    let child = Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("run")
        .arg(&config.0)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning logit run");
    let pid = child.id();
    let _child = KillOnDrop(child);

    wait_until_ready(&admin_addr).await;

    // The sink's first request, held unanswered for the rest of the test: the drain SIGTERM
    // starts waits on it until the grace cuts it.
    let (_held, _) = tokio::time::timeout(PROCESS_POLL_DEADLINE, influx.accept())
        .await
        .expect("influxdb_out never connected to send a batch")
        .expect("accepting influxdb_out's connection");

    // A real SIGTERM, not `Child::kill` (SIGKILL) -- only SIGTERM starts the graceful drain this
    // test exists to probe.
    // SAFETY: `pid` names a real child process this test spawned and still holds (`child` has not
    // been waited on or dropped yet), so it is a valid target for `kill(2)`.
    let kill_result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    assert_eq!(
        kill_result,
        0,
        "sending SIGTERM to the child failed: {:?}",
        std::io::Error::last_os_error()
    );

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

/// Polls `logit ready` until it succeeds, returning its stdout, or panics after
/// [`PROCESS_POLL_DEADLINE`].
async fn wait_until_ready(admin_addr: &str) -> String {
    let deadline = std::time::Instant::now() + PROCESS_POLL_DEADLINE;
    loop {
        let output = logit_ready(admin_addr);
        if output.status.success() {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "logit ready never reported success within {PROCESS_POLL_DEADLINE:?}; last attempt \
             exited {:?} with stderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test]
fn logit_ready_against_nothing_listening_exits_1() {
    // Port 1: nothing listens there, so the connection fails, the "no admin server reachable"
    // case.
    let output = logit_ready("127.0.0.1:1");
    assert_eq!(output.status.code(), Some(1));
}
