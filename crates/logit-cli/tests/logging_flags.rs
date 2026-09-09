//! `--log-level`/`--log-format` (docs/plans/operator-surface.md, workstream A) only take effect
//! on `logit run`, and only fail loudly on a bad directive -- both need a real process (a
//! subscriber is process-global, `tracing_subscriber::registry().init()` panics if called twice
//! in one process, so these can't run as in-crate unit tests alongside anything else that might
//! install one). `env!("CARGO_BIN_EXE_logit")` is the standard Cargo mechanism for an
//! integration test to find its own crate's freshly-built binary.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

/// A config broken enough to fail `logit run` immediately (before ever binding a socket) --
/// deliberately not "a working pipeline," so these tests exit fast regardless of what
/// `--log-level`/`--log-format` do to the output. Removed on drop -- no `tempfile` dependency
/// for one throwaway file; a unique name (pid + this process's own address-space randomness via
/// `std::process::id`) is enough for these tests, which never run two copies against the same
/// path concurrently.
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
        // `EnvFilter`'s directive grammar is `[target][span{field=value}]=level` -- "notalevel"
        // parses as a level filter and fails cleanly (verified by hand: a directive with stray
        // punctuation instead gets silently accepted as a target/span pattern, which would make
        // this test assert on the wrong thing).
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

    // `tracing_subscriber::fmt`'s default writer is stdout, not stderr -- verified by hand
    // (`anyhow`'s own `Error: {err:?}` line, from `Result`'s `Termination` impl, is the one that
    // goes to stderr). An empty `components: {}` config fails graph resolution (rule: at least
    // one component) -- exit 1, but only *after* `run_pipelines` has already logged `starting`.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut saw_a_line = false;
    for line in stdout.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else { continue };
        saw_a_line = true;
        assert!(value.get("timestamp").is_some(), "line missing 'timestamp': {line}");
        assert!(value.get("level").is_some(), "line missing 'level': {line}");
    }
    // Not asserting `saw_a_line` unconditionally would let this test pass even if JSON formatting
    // were silently broken and nothing ever parsed -- but `run_pipelines` always logs `starting`
    // before config resolution can fail, so at least one line is guaranteed.
    assert!(saw_a_line, "expected at least one JSON log line on stdout, got: {stdout}");
}
