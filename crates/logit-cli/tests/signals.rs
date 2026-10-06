//! `logit run`'s signal contract against the real binary (`docs/adr/signal-handling.md`): SIGHUP
//! never ends the process and never counts toward the second-signal exit, and a SIGTERM during
//! startup drains once the pipeline starts. Each test reads the child's self-log on stderr to know
//! a signal was handled, rather than sleeping.

#![cfg(unix)]

mod support;

use std::process::{Command, Stdio};

use support::{
    ephemeral_addr, logit_ready, send_signal, wait_for_exit, wait_until_ready, KillOnDrop, Lines,
    TempConfig,
};

fn spawn_run(config: &TempConfig) -> (KillOnDrop, Lines) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_logit"))
        .args(["--log-format", "json", "run"])
        .arg(&config.0)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawning logit run");
    let stderr = Lines::spawn(child.stderr.take().expect("piped stderr"));
    (KillOnDrop(child), stderr)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sighup_keeps_the_process_running_and_doesnt_count_toward_the_second_signal_exit() {
    let admin_addr = ephemeral_addr().await;
    let statsd_addr = ephemeral_addr().await;
    let config = TempConfig::write(
        "hup-then-term",
        format!(
            "admin:\n  bind: \"{admin_addr}\"\ncomponents:\n  in:\n    type: statsd_in\n    \
             bind: \"{statsd_addr}\"\n  out:\n    type: stdio_out\n    sources: [in]\n"
        ),
    );
    let (mut child, stderr) = spawn_run(&config);
    wait_until_ready(&admin_addr).await;

    send_signal(&child.0, libc::SIGHUP);
    let line = stderr.wait_for("reopen signal received");
    let event: serde_json::Value = serde_json::from_str(&line).expect("a JSON log line");
    assert_eq!(event["level"], "INFO", "{line}");
    assert_eq!(event["target"], "logit", "{line}");
    assert_eq!(event["config_reloaded"], false, "{line}");
    assert!(child.0.try_wait().unwrap().is_none(), "SIGHUP ended the process");
    assert!(logit_ready(&admin_addr).status.success(), "not ready after SIGHUP");

    // One SIGTERM after the SIGHUP drains and exits 0. Had the SIGHUP counted as a first shutdown
    // signal, this would be the second, and the exit would be 130.
    send_signal(&child.0, libc::SIGTERM);
    let status = wait_for_exit(&mut child.0).await;
    assert_eq!(status.code(), Some(0), "{status:?}");
}

/// A SIGHUP that lands during the drain only reopens: the drain runs to its end and exits 0, not
/// 130. The drain is held open as in `admin_ready.rs`'s draining test: `internal` feeds an
/// `influxdb_out` whose one request this test accepts and never answers, so the drain lasts until
/// `shutdown_grace` cuts it.
#[tokio::test(flavor = "multi_thread")]
async fn a_sighup_during_the_drain_doesnt_exit_130() {
    let influx = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let influx_addr = influx.local_addr().unwrap();
    let config = TempConfig::write(
        "term-then-hup",
        format!(
            "components:\n  self:\n    type: internal\n    interval: 100ms\n  out:\n    type: \
             influxdb_out\n    sources: [self]\n    url: \"http://{influx_addr}\"\n    org: o\n    \
             bucket: b\n    token: t\n    buffer:\n      shutdown_grace: 2s\n"
        ),
    );
    let (mut child, stderr) = spawn_run(&config);
    stderr.wait_for("ready");
    let (_held, _) = tokio::time::timeout(support::PROCESS_DEADLINE, influx.accept())
        .await
        .expect("influxdb_out never connected to send a batch")
        .expect("accepting influxdb_out's connection");

    send_signal(&child.0, libc::SIGTERM);
    stderr.wait_for("shutdown signal received");
    send_signal(&child.0, libc::SIGHUP);
    stderr.wait_for("reopen signal received");

    let status = wait_for_exit(&mut child.0).await;
    assert_eq!(status.code(), Some(0), "{status:?}");
}

/// A SIGTERM sent while the config is still being resolved is held, not defaulted: the process
/// drains and exits 0 rather than dying to the signal. `starting` is logged after the config
/// loads and before the graph is built, so the signal lands during startup or, on a fast run,
/// after it; both must exit 0.
#[tokio::test(flavor = "multi_thread")]
async fn a_sigterm_during_startup_drains_and_exits_0() {
    let statsd_addr = ephemeral_addr().await;
    let config = TempConfig::write(
        "term-at-startup",
        format!(
            "components:\n  in:\n    type: statsd_in\n    bind: \"{statsd_addr}\"\n  out:\n    \
             type: stdio_out\n    sources: [in]\n"
        ),
    );
    let (mut child, stderr) = spawn_run(&config);
    stderr.wait_for("starting");
    send_signal(&child.0, libc::SIGTERM);

    let status = wait_for_exit(&mut child.0).await;
    assert_eq!(status.code(), Some(0), "{status:?}");
    stderr.wait_for("exiting");
}

/// A startup that fails exits 1 even with a SIGTERM already held: the held signal never turns a
/// startup failure into a clean exit.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_startup_exits_1_with_a_sigterm_held() {
    // A port already in use fails the startup bind pass.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken_addr = taken.local_addr().unwrap();
    let config = TempConfig::write(
        "term-then-failed-startup",
        format!(
            "components:\n  in:\n    type: statsd_in\n    transport: tcp\n    bind: \
             \"{taken_addr}\"\n  out:\n    type: stdio_out\n    sources: [in]\n"
        ),
    );
    let (mut child, stderr) = spawn_run(&config);
    stderr.wait_for("starting");
    send_signal(&child.0, libc::SIGTERM);

    let status = wait_for_exit(&mut child.0).await;
    assert_eq!(status.code(), Some(1), "{status:?}");
    drop(taken);
}
