//! Display forms of configured values that can carry a credential.
//!
//! An operator can put `user:pass@` in a URL's authority, often through `!env`, so an error, log
//! line, diagnostic, or telemetry tag that names a configured URL or endpoint prints it through
//! [`url`] rather than raw. The request itself still uses the raw string.

use std::borrow::Cow;

/// What replaces a URL's userinfo in [`url`]'s output.
pub const USERINFO_MASK: &str = "***";

/// `s` with the userinfo of a `scheme://` authority replaced by [`USERINFO_MASK`]:
/// `https://user:pass@host:8086/path` renders as `https://***@host:8086/path`.
///
/// The mask, rather than dropping the userinfo, tells an operator chasing a `401` that the URL
/// carries credentials, without showing them.
///
/// Works on the string, not a parsed URL, so it never fails and never normalizes: a value that
/// isn't a URL (`host:514`, `/run/agent.sock`, an empty string) comes back unchanged and borrowed,
/// as does a URL with no userinfo. The authority runs from `://` to the first `/`, `?`, or `#`,
/// as WHATWG URL parsing reads it, so an `@` in a path or query is left alone; within it, the
/// last `@` ends the userinfo, so an unescaped `@` in a password is masked too. A scheme-less
/// `user:pass@host:port` is not a URL and is left unchanged.
pub fn url(s: &str) -> Cow<'_, str> {
    let Some(scheme_end) = s.find("://") else {
        return Cow::Borrowed(s);
    };
    if !is_scheme(&s[..scheme_end]) {
        return Cow::Borrowed(s);
    }
    let authority_start = scheme_end + "://".len();
    let rest = &s[authority_start..];
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let Some(at) = rest[..authority_len].rfind('@') else {
        return Cow::Borrowed(s);
    };
    let mut out = String::with_capacity(s.len());
    out.push_str(&s[..authority_start]);
    out.push_str(USERINFO_MASK);
    out.push_str(&rest[at..]);
    Cow::Owned(out)
}

/// RFC 3986's `scheme`: a letter, then letters, digits, `+`, `-`, or `.`.
fn is_scheme(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_user_and_password() {
        assert_eq!(url("https://user:pass@host:8086/path"), "https://***@host:8086/path");
    }

    #[test]
    fn masks_a_user_with_no_password() {
        assert_eq!(url("http://token@host/api"), "http://***@host/api");
    }

    #[test]
    fn masks_an_empty_userinfo() {
        assert_eq!(url("http://@host/"), "http://***@host/");
    }

    #[test]
    fn masks_userinfo_with_no_path() {
        assert_eq!(url("http://u:p@host"), "http://***@host");
        assert_eq!(url("http://u:p@host?q=1"), "http://***@host?q=1");
        assert_eq!(url("http://u:p@host#frag"), "http://***@host#frag");
    }

    #[test]
    fn masks_through_the_last_at_in_the_authority() {
        assert_eq!(url("https://user:p@ss@host/x"), "https://***@host/x");
    }

    #[test]
    fn masks_before_an_ipv6_host() {
        assert_eq!(url("http://u:p@[::1]:9090/metrics"), "http://***@[::1]:9090/metrics");
    }

    #[test]
    fn leaves_a_url_with_no_userinfo_borrowed() {
        for s in ["https://host:8086/path", "http://[::1]:9090/metrics", "https://host"] {
            assert!(matches!(url(s), Cow::Borrowed(b) if b == s), "{s}");
        }
    }

    #[test]
    fn leaves_an_at_in_the_path_query_or_fragment() {
        for s in [
            "https://host/users/a@b.com",
            "https://host/x?email=a@b.com",
            "https://host#a@b",
            "https://host:8086/a@b?c@d",
        ] {
            assert_eq!(url(s), s);
        }
    }

    #[test]
    fn leaves_non_urls_unchanged() {
        for s in [
            "",
            "host:514",
            "127.0.0.1:8125",
            "[::1]:514",
            "/var/run/datadog/apm.socket",
            "user:pass@host:514",
            "://u:p@host",
            "1http://u:p@host",
            "ht tp://u:p@host",
            "not a url @ all",
        ] {
            assert!(matches!(url(s), Cow::Borrowed(b) if b == s), "{s:?}");
        }
    }

    #[test]
    fn handles_multibyte_text_without_panicking() {
        assert_eq!(url("https://ü:ß@höst/päth"), "https://***@höst/päth");
        assert_eq!(url("é://x"), "é://x");
    }
}
