//! `docs/deploying.md`'s exit-code table against the real binary (ADR
//! `service-lifecycle-and-output-retry`): exit 1 for a startup failure, 0 for a clean finish.
//! Exit 2, a runtime failure after ready, needs 60 real seconds of `PERMANENT_FAILURE_WINDOW`, so
//! `logit-pipeline`'s paused-clock `a_sustained_permanent_sink_failure_returns_runtime_not_startup`
//! and the unit test on `RunError::exit_code` cover it instead.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

/// Removed on drop; no `tempfile` dependency for one throwaway file.
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
    // Held for the whole test so `logit run`'s bind pre-pass fails: the "startup failure with
    // nothing else running" case `RunError::Startup` names.
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

/// `generate_in` returns after `count` events, its senders drop, the listener-exit cascade
/// flushes downstream, and the process exits 0 with no signal or timeout involved: the contract a
/// perf scenario depends on (`docs/plans/load-test-harness.md`;
/// `crates/logit-pipeline/src/runtime.rs`'s
/// `run_returns_once_the_only_input_finishes_instead_of_hanging`).
///
/// The sink is `stdio_out` to `/dev/null`, not `null_out`: a sink that opens, writes, and flushes
/// a file on close is evidence the cascade flushed downstream, and one whose `send` does nothing
/// isn't. `examples/generate-to-null.yaml` has the canonical `generate_in -> null_out` shape.
#[test]
fn a_finite_generate_in_config_exits_0() {
    let config = TempConfig::write(
        "finite-generate",
        b"components:\n  gen:\n    type: generate_in\n    count: 1000\n    batch: 100\n    event:\n      log: \"n={seq}\"\n  sink:\n    type: stdio_out\n    sources: [gen]\n    target: /dev/null\n",
    );
    let output = Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("--log-format")
        .arg("json")
        .arg("run")
        .arg(&config.0)
        .output()
        .expect("spawning the logit binary");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "stderr was: {stderr}");

    // The harness reads this line to derive events/s, so its message and count are part of the
    // contract.
    let completion = stderr
        .lines()
        .find(|line| line.contains(r#""message":"generation complete""#))
        .unwrap_or_else(|| panic!("no 'generation complete' line in stderr: {stderr}"));
    assert!(completion.contains(r#""events":1000"#), "got: {completion}");

    // `run_with_telemetry` logs `drain complete` only when a shutdown signal or a node failure
    // started a drain. A generator finishing its `count` is neither, so this pins the clean
    // self-exit path.
    assert!(!stderr.contains("drain complete"), "stderr was: {stderr}");
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
