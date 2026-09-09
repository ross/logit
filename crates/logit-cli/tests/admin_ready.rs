//! `logit ready` against a real running `logit run` with `admin.bind` set
//! (docs/plans/operator-surface.md, workstream C) -- blackbox, spawning the actual binary twice
//! (once as the long-running service, once as the probe), unlike `crates/logit-cli/src/admin.rs`'s
//! own in-module tests, which exercise `serve_on`/`handle` directly and can't reach `logit ready`
//! (a private module of this binary crate, per `logging_flags.rs`'s own note on why every test
//! here spawns the real binary rather than calling into `crate::*`).

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

    // Retry `logit ready` rather than a fixed sleep: it's the exact probe under test, so using
    // it as its own readiness signal needs no separate synchronization primitive.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let output = logit_ready(&admin_addr);
        if output.status.success() {
            let word = String::from_utf8_lossy(&output.stdout).trim().to_string();
            assert_eq!(word, "ok");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "logit ready never reported success within 10s; last attempt exited {:?} with \
             stderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Kill the running pipeline, then confirm `logit ready` now fails against the closed port --
    // proving it actually reflects live state, not a cached "yes" from the first success above.
    child.0.kill().expect("killing the running logit process");
    child.0.wait().expect("waiting for logit to exit");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
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

/// The drain window is exactly what `/readyz` exists to report. Between the signal arriving and
/// the process exiting, an orchestrator must get `503 draining` -- a definite "stop routing here,
/// I am still finishing" -- not a refused connection, which it cannot tell from a crash. A
/// regression test for the admin server closing its own port on the shutdown signal, which made
/// `draining` unreachable in a real process no matter how long the drain took.
///
/// The drain is made long enough to probe by giving a sink something it can never deliver:
/// `internal` emits its own process gauges every 100ms with no traffic needed, `influxdb_out`
/// points at a port nothing listens on, and `buffer.shutdown_grace` bounds the drain at 2s.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn readyz_answers_draining_for_the_whole_drain_after_a_sigterm() {
    let admin_addr = ephemeral_addr().await;
    let config = TempConfig::write(
        "draining-e2e",
        format!(
            "admin:\n  bind: \"{admin_addr}\"\ncomponents:\n  self:\n    type: internal\n    \
             interval: 100ms\n  out:\n    type: influxdb_out\n    sources: [self]\n    url: \
             \"http://127.0.0.1:1\"\n    org: o\n    bucket: b\n    token: t\n    buffer:\n      \
             delivery: at_least_once\n      shutdown_grace: 2s\n"
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

    // Poll `logit ready` until the pipeline reports itself ready, same as the happy-path test
    // above.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let output = logit_ready(&admin_addr);
        if output.status.success() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "logit ready never reported success within 10s; last attempt exited {:?} with \
             stderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Give the undeliverable sink time to accumulate a batch it cannot flush, so the drain the
    // signal below starts is guaranteed to still be running when we start probing it.
    tokio::time::sleep(Duration::from_millis(400)).await;

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

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
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
        // The process may have already exited (drain finished, or admin port closed) -- either
        // way, keep polling until the deadline; a `logit ready` connection failure is itself
        // useful information in the failure message below.
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
    // Port 1 -- nothing is listening there, and no real server (this test's own admin server
    // included) ever binds it, so the connection simply fails, exactly the "no admin server
    // reachable" case.
    let output = logit_ready("127.0.0.1:1");
    assert_eq!(output.status.code(), Some(1));
}
