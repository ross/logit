//! `shutdown.delay` against the real binary
//! (`docs/adr/listener-port-sharing-and-shutdown-delay.md`): after the first SIGTERM, `/readyz`
//! reports draining while every listener keeps reading, the drain starts once the delay ends, and
//! a second SIGTERM still exits 130 at once. Each test reads the child's self-log on stderr and
//! its events on stdout rather than sleeping.

#![cfg(unix)]

mod support;

use std::process::{Command, Stdio};

use logit_pipeline::test_util::wait_until_within;
use support::{
    ephemeral_addr, logit_ready, send_signal, wait_for_exit, wait_until_ready, KillOnDrop, Lines,
    TempConfig, PROCESS_DEADLINE,
};

/// A free loopback UDP port for `statsd_in`, bound and released like
/// [`support::ephemeral_addr`]'s TCP one.
fn ephemeral_udp_addr() -> String {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().to_string()
}

/// `admin:`, `shutdown.delay`, and `statsd_in` into a `stdio_out` writing JSON lines to stdout.
fn config(name: &str, admin_addr: &str, statsd_addr: &str, delay: &str) -> TempConfig {
    TempConfig::write(
        name,
        format!(
            "admin:\n  bind: \"{admin_addr}\"\nshutdown:\n  delay: {delay}\ncomponents:\n  in:\n    \
             type: statsd_in\n    bind: \"{statsd_addr}\"\n  out:\n    type: stdio_out\n    \
             sources: [in]\n    format: json\n"
        ),
    )
}

/// Spawns `logit run` with stdout and stderr piped, returning the child and both line readers.
fn spawn_run(config: &TempConfig) -> (KillOnDrop, Lines, Lines) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_logit"))
        .args(["--log-format", "json", "run"])
        .arg(&config.0)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawning logit run");
    let stdout = Lines::spawn(child.stdout.take().expect("piped stdout"));
    let stderr = Lines::spawn(child.stderr.take().expect("piped stderr"));
    (KillOnDrop(child), stdout, stderr)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_datagram_sent_after_sigterm_is_delivered_during_the_delay_and_a_second_sigterm_exits_130(
) {
    let admin_addr = ephemeral_addr().await;
    let statsd_addr = ephemeral_udp_addr();
    // Long enough that the test never reaches its end: the second SIGTERM ends the run.
    let config = config("shutdown-delay-serves", &admin_addr, &statsd_addr, "60s");
    let (mut child, stdout, stderr) = spawn_run(&config);
    wait_until_ready(&admin_addr).await;
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();

    sender.send_to(b"shutdown_delay_before:1|c", &statsd_addr).unwrap();
    stdout.wait_for("shutdown_delay_before");

    send_signal(&child.0, libc::SIGTERM);
    stderr.wait_for("shutdown signal received");
    wait_until_within("logit ready to report draining", PROCESS_DEADLINE, || {
        String::from_utf8_lossy(&logit_ready(&admin_addr).stderr).contains("draining")
    })
    .await;

    sender.send_to(b"shutdown_delay_after:1|c", &statsd_addr).unwrap();
    stdout.wait_for("shutdown_delay_after");
    assert!(child.0.try_wait().unwrap().is_none(), "the process exited during the delay");

    send_signal(&child.0, libc::SIGTERM);
    let status = wait_for_exit(&mut child.0).await;
    assert_eq!(status.code(), Some(130), "{status:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_drain_starts_once_the_shutdown_delay_elapses_and_exits_0() {
    let admin_addr = ephemeral_addr().await;
    let statsd_addr = ephemeral_udp_addr();
    // The 1 s is the delay under test, not a wait: the test reads the lines it produces.
    let config = config("shutdown-delay-drains", &admin_addr, &statsd_addr, "1s");
    let (mut child, _stdout, stderr) = spawn_run(&config);
    wait_until_ready(&admin_addr).await;

    let signalled_at = std::time::Instant::now();
    send_signal(&child.0, libc::SIGTERM);
    stderr.wait_for("shutdown signal received");
    let line = stderr.wait_for("shutdown delay elapsed");
    // A lower bound only: the line is written a second after the signal lands, and read later.
    assert!(signalled_at.elapsed() >= std::time::Duration::from_secs(1), "{line}");
    let event: serde_json::Value = serde_json::from_str(&line).expect("a JSON log line");
    assert_eq!(event["level"], "INFO", "{line}");
    assert_eq!(event["target"], "logit", "{line}");
    stderr.wait_for("drain complete");

    let status = wait_for_exit(&mut child.0).await;
    assert_eq!(status.code(), Some(0), "{status:?}");
}
