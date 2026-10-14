//! The process hostname: one value, resolved once at `logit run` startup, that `logit` stamps as
//! `host.name` on its own `internal` telemetry (`docs/adr/process-hostname.md`). `build_spec` takes
//! it as a parameter, so other components can use it.
//!
//! The order is the top-level `hostname:` if set, then the kernel hostname (`uname(2)`'s
//! nodename, the value `gethostname(2)` returns). `$HOSTNAME` is not read and no DNS lookup is
//! made. A kernel hostname that isn't UTF-8 is converted lossily, and an empty result is an error,
//! because an empty `host.name` would be worse than a refusal to start.

use anyhow::bail;
use std::sync::Arc;

/// Resolves the process hostname from the config's `hostname:` and the kernel.
///
/// `logit validate` never calls this; only `logit run` does, so validation needs no OS access.
pub fn resolve(configured: Option<&str>) -> anyhow::Result<Arc<str>> {
    let nodename = rustix::system::uname();
    resolve_from(configured, nodename.nodename().to_bytes())
}

/// [`resolve`] with the OS nodename passed in, so tests can supply one.
fn resolve_from(configured: Option<&str>, nodename: &[u8]) -> anyhow::Result<Arc<str>> {
    if let Some(configured) = configured {
        return Ok(Arc::from(configured));
    }
    let name = String::from_utf8_lossy(nodename);
    if name.is_empty() {
        bail!(
            "the kernel hostname is empty -- set 'hostname:' in the config, for example \
             'hostname: !env NODE_NAME'"
        );
    }
    Ok(Arc::from(name.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configured_hostname_wins_over_the_kernel_one() {
        assert_eq!(&*resolve_from(Some("configured"), b"kernel").unwrap(), "configured");
    }

    #[test]
    fn the_kernel_hostname_is_the_default() {
        assert_eq!(&*resolve_from(None, b"kernel").unwrap(), "kernel");
    }

    #[test]
    fn a_non_utf8_kernel_hostname_converts_lossily() {
        assert_eq!(&*resolve_from(None, b"node-\xff").unwrap(), "node-\u{fffd}");
    }

    #[test]
    fn an_empty_kernel_hostname_is_an_error_that_names_the_config_field() {
        let err = resolve_from(None, b"").unwrap_err().to_string();
        assert!(err.contains("hostname:"), "got: {err}");
    }

    #[test]
    fn a_configured_hostname_rescues_an_empty_kernel_one() {
        assert_eq!(&*resolve_from(Some("pod-a"), b"").unwrap(), "pod-a");
    }

    #[test]
    fn the_real_kernel_hostname_resolves() {
        assert!(!resolve(None).unwrap().is_empty());
    }
}
