//! `docs/deploying.md`'s exit-code table, exercised against the real binary
//! (`docs/plans/operator-surface.md`, workstream B) -- `env!("CARGO_BIN_EXE_logit")` is the same
//! "spawn the real binary" idiom `logging_flags.rs` in this same crate uses. Exit 2 (a runtime
//! failure after ready) needs 60 real seconds of `PERMANENT_FAILURE_WINDOW` and is covered
//! instead by `logit-pipeline`'s own paused-clock tests
//! (`a_sustained_permanent_sink_failure_returns_runtime_not_startup`) plus the unit test on
//! `RunError::exit_code` -- this file only covers what's cheap to prove end to end: exit 1 and
//! exit 0.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

/// Removed on drop -- no `tempfile` dependency for one throwaway file, same as
/// `logging_flags.rs`'s `BrokenConfig` in this same crate.
struct TempConfig(PathBuf);

impl TempConfig {
    fn write(name: &str, contents: &[u8]) -> Self {
        let path = std::env::temp_dir()
            .join(format!("logit-exit-codes-test-{name}-{}.yaml", std::process::id()));
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

#[test]
fn an_empty_config_exits_1() {
    let config = TempConfig::write("empty", b"components: {}\n");
    let status = Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("run")
        .arg(&config.0)
        .status()
        .expect("spawning the logit binary");
    assert_eq!(status.code(), Some(1));
}

#[test]
fn a_port_already_in_use_exits_1() {
    // Held for the test's own duration so `logit run`'s bind (workstream B's pre-pass) fails --
    // exactly the "startup failure with nothing else running" case `RunError::Startup` names.
    let held = std::net::UdpSocket::bind("127.0.0.1:0").expect("binding a probe socket");
    let addr = held.local_addr().expect("probe socket should have a local address");

    let config = TempConfig::write(
        "port-in-use",
        format!(
            "components:\n  in:\n    type: statsd_in\n    bind: \"{addr}\"\n  out:\n    type: stdio_out\n    sources: [in]\n"
        )
        .as_bytes(),
    );
    let status = Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("run")
        .arg(&config.0)
        .status()
        .expect("spawning the logit binary");
    assert_eq!(status.code(), Some(1));
}

#[test]
fn logit_validate_a_good_config_exits_0() {
    let config = TempConfig::write(
        "valid",
        b"components:\n  in:\n    type: statsd_in\n    bind: \"127.0.0.1:0\"\n  out:\n    type: stdio_out\n    sources: [in]\n",
    );
    let status = Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("validate")
        .arg(&config.0)
        .status()
        .expect("spawning the logit binary");
    assert_eq!(status.code(), Some(0));
}
