//! `logit run`'s signal contract against the real binary (`docs/adr/signal-handling.md`): SIGHUP
//! never ends the process, never counts toward the second-signal exit, reopens a `file_out`
//! target, and checks every TLS component's files for new content, and a SIGTERM during startup
//! drains once the pipeline starts. Each test reads the child's self-log on stderr to know a
//! signal was handled, rather than sleeping.

#![cfg(unix)]

mod support;

use std::io::Write;
use std::process::{Command, Stdio};

use logit_pipeline::test_util::{scratch_dir, wait_until_within};
use support::{
    ephemeral_addr, logit_ready, send_signal, wait_for_exit, wait_until_ready, KillOnDrop, Lines,
    TempConfig, PROCESS_DEADLINE,
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

/// A SIGTERM sent while `config::load` is still reading the config is held, not defaulted: the
/// process starts, drains, and exits 0. The config path is a FIFO, so the signal lands while the
/// load is blocked on it.
#[tokio::test(flavor = "multi_thread")]
async fn a_sigterm_during_config_load_drains_and_exits_0() {
    let statsd_addr = ephemeral_addr().await;
    let config = TempConfig::fifo("term-during-load");
    let (mut child, stderr) = spawn_run(&config);
    let mut writer = config.wait_for_reader().await;
    send_signal(&child.0, libc::SIGTERM);
    write!(
        writer,
        "components:\n  in:\n    type: statsd_in\n    bind: \"{statsd_addr}\"\n  out:\n    type: \
         stdio_out\n    sources: [in]\n"
    )
    .unwrap();
    drop(writer);

    let status = wait_for_exit(&mut child.0).await;
    assert_eq!(status.code(), Some(0), "{status:?}");
    stderr.wait_for("exiting");
}

/// A startup that fails exits 1 even with a SIGTERM held from `config::load`: the held signal
/// never turns a startup failure into a clean exit.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_startup_exits_1_with_a_sigterm_held() {
    // A port already in use fails the startup bind pass.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken_addr = taken.local_addr().unwrap();
    let config = TempConfig::fifo("term-then-failed-startup");
    let (mut child, _stderr) = spawn_run(&config);
    let mut writer = config.wait_for_reader().await;
    send_signal(&child.0, libc::SIGTERM);
    write!(
        writer,
        "components:\n  in:\n    type: statsd_in\n    transport: tcp\n    bind: \
         \"{taken_addr}\"\n  out:\n    type: stdio_out\n    sources: [in]\n"
    )
    .unwrap();
    drop(writer);

    let status = wait_for_exit(&mut child.0).await;
    assert_eq!(status.code(), Some(1), "{status:?}");
    drop(taken);
}

/// A rename then a SIGHUP reopens a `file_out` target, the sink with the more involved reopen
/// (its rotation state is re-seeded): lines sent after the signal land in a new file at `path`,
/// and the renamed file keeps only the lines sent before it.
#[tokio::test(flavor = "multi_thread")]
async fn a_sighup_after_an_external_rename_reopens_a_file_out_target() {
    let dir = scratch_dir("hup-reopen");
    let path = dir.join("events.log");
    let renamed = dir.join("events.log.1");
    let udp_addr = {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.local_addr().unwrap().to_string()
    };
    let config = TempConfig::write(
        "hup-reopen",
        format!(
            "components:\n  in:\n    type: lines_in\n    transport: udp\n    bind: \
             \"{udp_addr}\"\n  out:\n    type: file_out\n    sources: [in]\n    format: json\n    \
             path: \"{}\"\n    rotate:\n      max_bytes: 1MiB\n",
            path.display()
        ),
    );
    let (child, stderr) = spawn_run(&config);
    // `ready` means every listener is bound, so the first datagram can't hit an unbound port.
    stderr.wait_for("ready");
    let read = |p: &std::path::Path| std::fs::read_to_string(p).unwrap_or_default();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();

    sender.send_to(b"before the rotation\n", &udp_addr).unwrap();
    wait_until_within("the first line in the file", PROCESS_DEADLINE, || {
        read(&path).contains("before the rotation")
    })
    .await;

    std::fs::rename(&path, &renamed).unwrap();
    send_signal(&child.0, libc::SIGHUP);
    stderr.wait_for("reopen signal received");
    sender.send_to(b"after the rotation\n", &udp_addr).unwrap();
    wait_until_within("the second line in a new file at the path", PROCESS_DEADLINE, || {
        read(&path).contains("after the rotation")
    })
    .await;

    assert!(!read(&path).contains("before the rotation"), "{}", read(&path));
    assert!(!read(&renamed).contains("after the rotation"), "{}", read(&renamed));
    std::fs::remove_dir_all(&dir).ok();
}

/// With `tls_reload_interval: 0s` nothing polls, so a SIGHUP is what finds a renewed certificate:
/// it checks every TLS component's files, and the listener reports the reload.
#[tokio::test(flavor = "multi_thread")]
async fn a_sighup_checks_tls_files_with_polling_off() {
    let dir = scratch_dir("hup-tls");
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls");
    std::fs::copy(fixtures.join("server.pem"), dir.join("cert.pem")).unwrap();
    std::fs::copy(fixtures.join("server.key"), dir.join("key.pem")).unwrap();
    let admin_addr = ephemeral_addr().await;
    let lines_addr = ephemeral_addr().await;
    let config = TempConfig::write(
        "hup-tls",
        format!(
            "admin:\n  bind: \"{admin_addr}\"\ntls_reload_interval: 0s\ncomponents:\n  in:\n    \
             type: lines_in\n    transport: tcp\n    bind: \"{lines_addr}\"\n    tls:\n      \
             cert_file: \"{cert}\"\n      key_file: \"{key}\"\n  out:\n    type: null_out\n    \
             sources: [in]\n",
            cert = dir.join("cert.pem").display(),
            key = dir.join("key.pem").display(),
        ),
    );
    let (child, stderr) = spawn_run(&config);
    wait_until_ready(&admin_addr).await;

    std::fs::copy(fixtures.join("server-b.pem"), dir.join("cert.pem")).unwrap();
    std::fs::copy(fixtures.join("server-b.key"), dir.join("key.pem")).unwrap();
    send_signal(&child.0, libc::SIGHUP);
    let line = stderr.wait_for("tls_reloaded");
    let event: serde_json::Value = serde_json::from_str(&line).expect("a JSON log line");
    assert_eq!(event["level"], "INFO", "{line}");
    assert_eq!(event["component"], "in", "{line}");
    assert!(event["message"].as_str().unwrap().contains("2126-09-13T20:27:39Z"), "{line}");
    std::fs::remove_dir_all(&dir).ok();
}
