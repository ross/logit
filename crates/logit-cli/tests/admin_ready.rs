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

#[test]
fn logit_ready_against_nothing_listening_exits_1() {
    // Port 1 -- nothing is listening there, and no real server (this test's own admin server
    // included) ever binds it, so the connection simply fails, exactly the "no admin server
    // reachable" case.
    let output = logit_ready("127.0.0.1:1");
    assert_eq!(output.status.code(), Some(1));
}
