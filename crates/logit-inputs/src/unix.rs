//! Binding a listener on a Unix socket path, shared by `datadog_trace_in`'s `socket:` and
//! `statsd_in`'s `transport: unix`/`unix_stream`.
//!
//! Every bind prepares the path the same way, which is what the Datadog Agent does at startup for
//! its own sockets:
//!
//! - **The parent directory must exist.** It is never created: its ownership and mode are the
//!   access control a socket file's own mode can't give, so choosing them is the operator's call.
//! - **A stale socket file is replaced.** A file left by an earlier run (a crash, or no cleanup on
//!   shutdown) would otherwise fail the bind with `EADDRINUSE`.
//! - **Anything else at the path is refused**, so a typo can't delete a regular file.
//! - **The mode is set after the bind**, since `bind(2)` creates the file with the process umask
//!   applied.
//!
//! The socket file isn't removed on shutdown; the next bind replaces it.

use anyhow::Context as _;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
use tokio::net::{UnixDatagram, UnixListener};

/// Binds a `SOCK_STREAM` Unix socket at `path` and sets its mode. `kind` names the component in
/// the error text.
pub(crate) fn bind_listener(kind: &str, path: &Path, mode: u32) -> anyhow::Result<UnixListener> {
    prepare_path(kind, path)?;
    let listener = UnixListener::bind(path)
        .with_context(|| format!("binding the Unix socket {}", path.display()))?;
    set_mode(path, mode)?;
    Ok(listener)
}

/// Binds a `SOCK_DGRAM` Unix socket at `path` and sets its mode. `kind` names the component in the
/// error text.
pub(crate) fn bind_datagram(kind: &str, path: &Path, mode: u32) -> anyhow::Result<UnixDatagram> {
    prepare_path(kind, path)?;
    let socket = UnixDatagram::bind(path)
        .with_context(|| format!("binding the Unix datagram socket {}", path.display()))?;
    set_mode(path, mode)?;
    Ok(socket)
}

/// This module's first three rules: the parent must exist, a stale socket is removed, and
/// anything else at `path` is refused.
fn prepare_path(kind: &str, path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        if !parent.is_dir() {
            anyhow::bail!(
                "{kind}: can't bind the Unix socket {}: its directory {} does not exist (create \
                 it first; the Agent's is /var/run/datadog)",
                path.display(),
                parent.display()
            );
        }
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(path)
            .with_context(|| format!("removing the stale socket file {}", path.display())),
        Ok(_) => anyhow::bail!(
            "{kind}: {} exists and is not a socket; refusing to replace it",
            path.display()
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("inspecting {}", path.display())),
    }
}

fn set_mode(path: &Path, mode: u32) -> anyhow::Result<()> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("setting {}'s mode to {mode:o}", path.display()))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::path::PathBuf;

    /// A per-test directory under the system temp dir, removed on drop (`crate::datadog_trace`'s
    /// tests use the same shape; the crate has no `tempfile` dependency).
    pub(crate) struct TempDir(pub(crate) PathBuf);

    impl TempDir {
        pub(crate) fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("liu-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        pub(crate) fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn mode_of(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[tokio::test]
    async fn a_datagram_socket_gets_the_requested_mode_and_replaces_a_stale_one() {
        let dir = TempDir::new("dgram");
        let path = dir.path().join("dsd.socket");
        let first = bind_datagram("statsd_in", &path, 0o722).unwrap();
        assert_eq!(mode_of(&path), 0o722);
        drop(first);
        // The file outlives the socket; the second bind replaces it rather than failing.
        let _second = bind_datagram("statsd_in", &path, 0o722).unwrap();
        assert_eq!(mode_of(&path), 0o722);
    }

    #[tokio::test]
    async fn a_stream_socket_gets_the_requested_mode() {
        let dir = TempDir::new("stream");
        let path = dir.path().join("dsd-stream.socket");
        let _listener = bind_listener("statsd_in", &path, 0o722).unwrap();
        assert_eq!(mode_of(&path), 0o722);
    }

    #[tokio::test]
    async fn a_regular_file_at_the_path_is_refused_and_left_alone() {
        let dir = TempDir::new("refuse");
        let path = dir.path().join("not-a-socket");
        std::fs::write(&path, b"keep me").unwrap();
        let err = bind_datagram("statsd_in", &path, 0o722).unwrap_err().to_string();
        assert!(err.contains("statsd_in:") && err.contains("is not a socket"), "{err}");
        assert_eq!(std::fs::read(&path).unwrap(), b"keep me");
    }

    #[tokio::test]
    async fn a_missing_directory_is_refused() {
        let dir = TempDir::new("missing");
        let path = dir.path().join("missing").join("dsd.socket");
        let err = bind_listener("statsd_in", &path, 0o722).unwrap_err().to_string();
        assert!(err.contains("does not exist"), "{err}");
    }
}
