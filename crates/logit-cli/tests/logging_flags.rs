//! `--log-level`/`--log-format` against the real binary (ADR `tracing-for-self-logging`). The
//! subscriber is process-global and `tracing_subscriber::registry().init()` panics if called
//! twice in one process, so these can't be in-crate unit tests alongside anything else that
//! installs one. Only `logit run` installs a subscriber, so every test here runs it.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

/// A config that fails `logit run` before any socket is bound (`components: {}` fails graph
/// resolution), so each test exits fast whatever the flags do to the output. Removed on drop; the
/// pid in the name keeps concurrent test binaries apart.
struct BrokenConfig(PathBuf);

impl BrokenConfig {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir()
            .join(format!("logit-logging-flags-test-{name}-{}.yaml", std::process::id()));
        std::fs::File::create(&path)
            .and_then(|mut f| f.write_all(b"components: {}\n"))
            .expect("writing the temp config");
        Self(path)
    }
}

impl Drop for BrokenConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn a_bad_log_level_directive_exits_1_with_a_clear_message() {
    let config = BrokenConfig::new("bad-level");
    let output = Command::new(env!("CARGO_BIN_EXE_logit"))
        // `EnvFilter`'s grammar is `[target][span{field=value}]=level`, so `notalevel` parses as
        // a level and fails cleanly. A directive with stray punctuation instead is accepted as a
        // target/span pattern and wouldn't fail.
        .args(["--log-level", "foo=notalevel", "run"])
        .arg(&config.0)
        .output()
        .expect("spawning the logit binary");

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--log-level") && stderr.contains("foo=notalevel"),
        "stderr should name the flag and the bad value, got: {stderr}"
    );
}

#[test]
fn log_format_json_emits_one_parseable_object_per_line() {
    let config = BrokenConfig::new("json-format");
    let output = Command::new(env!("CARGO_BIN_EXE_logit"))
        .args(["--log-format", "json", "run"])
        .arg(&config.0)
        .output()
        .expect("spawning the logit binary");

    // Self-logging goes to stderr: stdout is the pipeline's (`stdio_out` defaults to
    // `target: stdout`), so `logit run c.yaml > events.log` stays a clean event stream.
    // `components: {}` fails graph resolution with exit 1, but only after `run_pipelines` has
    // logged `starting`.
    assert!(
        output.stdout.is_empty(),
        "stdout must carry no self-logging, got: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut saw_starting = false;
    for line in stderr.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else { continue };
        assert!(value.get("timestamp").is_some(), "line missing 'timestamp': {line}");
        assert!(value.get("level").is_some(), "line missing 'level': {line}");
        if value.get("message").and_then(serde_json::Value::as_str) == Some("starting") {
            saw_starting = true;
            assert_eq!(
                value.get("target").and_then(serde_json::Value::as_str),
                Some("logit"),
                "'starting' line missing target=logit: {line}"
            );
        }
    }
    assert!(
        saw_starting,
        "expected a top-level message=\"starting\" JSON line on stderr, got: {stderr}"
    );
}

#[test]
fn a_strict_log_level_still_silences_stderr_self_logging() {
    // The `EnvFilter` is scoped to the stderr `fmt` layer so it doesn't gate internal-log capture
    // (`TelemetryLayer::capture_filter`), but it must still filter stderr: `--log-level error`
    // has to swallow the `info`-level `starting`.
    let config = BrokenConfig::new("strict-level");
    let output = Command::new(env!("CARGO_BIN_EXE_logit"))
        .args(["--log-level", "error", "run"])
        .arg(&config.0)
        .output()
        .expect("spawning the logit binary");

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("starting"),
        "--log-level error must still silence the info-level 'starting' line, got: {stderr}"
    );
}
