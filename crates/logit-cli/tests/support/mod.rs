//! Helpers for the integration tests that spawn the real `logit` binary. Each test file that
//! needs them declares `mod support;`; a file uses only some, hence the `dead_code` allowance.

#![allow(dead_code)]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use logit_pipeline::test_util::wait_until_within;

/// How long a poll against a `logit run` child waits for the state it expects: ready, exited, or
/// an event on its output. A process spawn is slower than an in-process bind, and a poll that
/// spawns `logit ready` once per attempt slower still, so this is wider than
/// `logit_pipeline::test_util::RECV_TIMEOUT`.
pub const PROCESS_DEADLINE: Duration = Duration::from_secs(10);

/// A config file in the temp dir, removed on drop; no `tempfile` dependency for one throwaway
/// file. `name` must be unique within the test binary.
pub struct TempConfig(pub PathBuf);

impl TempConfig {
    fn path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("logit-cli-test-{name}-{}.yaml", std::process::id()))
    }

    pub fn write(name: &str, contents: impl AsRef<[u8]>) -> Self {
        let path = Self::path(name);
        std::fs::File::create(&path)
            .and_then(|mut f| f.write_all(contents.as_ref()))
            .expect("writing the temp config");
        Self(path)
    }

    /// A FIFO in place of the file, so a test can hold `logit run` inside `config::load`: the
    /// child's read blocks until [`TempConfig::wait_for_reader`]'s write end is written and closed.
    #[cfg(unix)]
    pub fn fifo(name: &str) -> Self {
        use std::os::unix::ffi::OsStrExt;
        let path = Self::path(name);
        let _ = std::fs::remove_file(&path);
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `c_path` is a valid NUL-terminated string that outlives the call.
        let result = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(result, 0, "mkfifo failed: {:?}", std::io::Error::last_os_error());
        Self(path)
    }

    /// Waits until a reader has opened this FIFO, returning its write end. A non-blocking
    /// write-only open fails with `ENXIO` until a reader exists, so its success is the observable
    /// that `logit run` is inside `config::load`.
    #[cfg(unix)]
    pub async fn wait_for_reader(&self) -> std::fs::File {
        use std::os::unix::fs::OpenOptionsExt;
        let mut writer = None;
        wait_until_within("logit run to open its config", PROCESS_DEADLINE, || {
            writer = std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&self.0)
                .ok();
            writer.is_some()
        })
        .await;
        writer.unwrap()
    }
}

impl Drop for TempConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Kills the child on drop, however the test exits, so a panicking assertion never leaves a
/// `logit run` process behind holding its ports.
pub struct KillOnDrop(pub Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A free loopback port, bound and released. This is the child-process exception to
/// `docs/adr/test-timing-and-observables.md`'s bind-before-spawn rule: the port is handed to a
/// `logit run` child through its config, and the child binds it.
pub async fn ephemeral_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

/// Runs `logit ready` against the TCP admin address `admin` once.
pub fn logit_ready(admin: &str) -> std::process::Output {
    logit_ready_url(&format!("http://{admin}"))
}

/// Runs `logit ready --admin <url>` once: `url` is `http://…` or `unix:<path>`.
pub fn logit_ready_url(url: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_logit"))
        .args(["ready", "--admin", url])
        .env_remove("LOGIT_ADMIN")
        .output()
        .expect("spawning the logit binary")
}

/// Runs `logit ready` once with no `--admin`, the endpoint given as `LOGIT_ADMIN=<url>`.
pub fn logit_ready_env(url: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("ready")
        .env("LOGIT_ADMIN", url)
        .output()
        .expect("spawning the logit binary")
}

/// Polls `logit ready` until it succeeds, returning its stdout, or panics after
/// [`PROCESS_DEADLINE`].
pub async fn wait_until_ready(admin_addr: &str) -> String {
    wait_until_probe_succeeds(|| logit_ready(admin_addr)).await
}

/// Polls `probe`, a `logit ready` run, until it succeeds, returning its stdout, or panics after
/// [`PROCESS_DEADLINE`].
pub async fn wait_until_probe_succeeds(probe: impl Fn() -> std::process::Output) -> String {
    let deadline = std::time::Instant::now() + PROCESS_DEADLINE;
    loop {
        let output = probe();
        if output.status.success() {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "logit ready never reported success within {PROCESS_DEADLINE:?}; last attempt \
             exited {:?} with stderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A child's piped output, read line by line on a thread so a test can wait on one line with a
/// deadline.
pub struct Lines(std::sync::mpsc::Receiver<String>);

impl Lines {
    pub fn spawn(output: impl std::io::Read + Send + 'static) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(output).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self(rx)
    }

    /// Reads until a line containing `needle`, returning it, or panics after
    /// [`PROCESS_DEADLINE`] or once the output closes.
    pub fn wait_for(&self, needle: &str) -> String {
        let deadline = std::time::Instant::now() + PROCESS_DEADLINE;
        let mut skipped = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            match self.0.recv_timeout(left) {
                Ok(line) if line.contains(needle) => return line,
                Ok(line) => skipped.push(line),
                Err(err) => panic!("no line containing {needle:?} ({err}); read: {skipped:#?}"),
            }
        }
    }
}

/// Waits up to [`PROCESS_DEADLINE`] for `child` to exit, returning its status.
pub async fn wait_for_exit(child: &mut Child) -> std::process::ExitStatus {
    let mut status = None;
    wait_until_within("the child to exit", PROCESS_DEADLINE, || {
        status = child.try_wait().expect("polling the child");
        status.is_some()
    })
    .await;
    status.unwrap()
}

/// Sends `signal` to `child`, a real signal rather than `Child::kill`'s SIGKILL.
#[cfg(unix)]
pub fn send_signal(child: &Child, signal: libc::c_int) {
    // SAFETY: `child` is a process this test spawned and still holds unwaited, so its pid can't
    // have been reused and is a valid target for `kill(2)`.
    let result = unsafe { libc::kill(child.id() as libc::pid_t, signal) };
    assert_eq!(
        result,
        0,
        "sending signal {signal} to the child failed: {:?}",
        std::io::Error::last_os_error()
    );
}
