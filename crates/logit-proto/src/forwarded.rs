//! The client address in an L7 proxy's forwarding header: `X-Forwarded-For`, RFC 7239's
//! `Forwarded`, or `X-Real-IP`. One parser for every component with a `forwarded:` field, so they
//! agree on a port, brackets, and what's unusable.
//!
//! ADR `forwarded-header-parsing` is the canonical account of the rules, in its "Parsing"
//! section. What a maintainer needs at the code:
//!
//! - [`parse`] reads one header value. A request that carries the header more than once is the
//!   caller's concern: the caller passes the first instance.
//! - Each header yields one candidate: `X-Forwarded-For`'s leftmost comma-separated entry,
//!   `Forwarded`'s first element's `for=` value, or `X-Real-IP`'s whole value, each trimmed of
//!   whitespace. The address rule in the ADR then decides it, and an IPv4-mapped IPv6 address is
//!   returned as given; the caller canonicalizes it when it writes the text form.
//! - `Forwarded` is read with RFC 7239 §4's grammar, a `;`-separated list of pairs per
//!   comma-separated element, with case-insensitive parameter names:
//!
//!   ```text
//!   Forwarded         = 1#forwarded-element
//!   forwarded-element = [ forwarded-pair ] *( ";" [ forwarded-pair ] )
//!   forwarded-pair    = token "=" value
//!   value             = token / quoted-string
//!   ```
//!
//!   A quoted string may hold a `,` or `;`, and a `\` escapes the byte after it. An unquoted
//!   value runs to the next whitespace, `;`, or `,`, so `for=[2001:db8::1]:443` without the
//!   quotes the grammar requires still reads. When an element names `for` twice, the first wins.
//!
//! The success path allocates nothing: candidates are slices of the value, and a quoted string's
//! escapes are undone into a stack buffer no longer than the longest bracketed IPv6 address with
//! a port.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// The forwarding header a component reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForwardedHeader {
    XForwardedFor,
    Forwarded,
    XRealIp,
}

impl ForwardedHeader {
    /// The header's lowercase wire name.
    pub fn name(&self) -> &'static str {
        match self {
            ForwardedHeader::XForwardedFor => "x-forwarded-for",
            ForwardedHeader::Forwarded => "forwarded",
            ForwardedHeader::XRealIp => "x-real-ip",
        }
    }
}

/// The client a header names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Client {
    pub address: IpAddr,
    pub port: Option<u16>,
}

/// Why a header value names no usable client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unusable {
    /// The candidate is empty or whitespace.
    Empty,
    /// RFC 7239's `unknown` node, which `X-Forwarded-For` writers use too.
    Unknown,
    /// An RFC 7239 obfuscated identifier, which starts with `_`.
    Obfuscated,
    /// A `Forwarded` value whose first element has no `for=` parameter.
    NoFor,
    /// A `Forwarded` value that doesn't follow RFC 7239's grammar.
    Malformed,
    /// Anything else that isn't an IP address, with or without a port.
    NotAnAddress,
}

impl fmt::Display for Unusable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Unusable::Empty => "the client entry is empty",
            Unusable::Unknown => "the client is 'unknown'",
            Unusable::Obfuscated => "the client is an obfuscated identifier",
            Unusable::NoFor => "the first element has no 'for' parameter",
            Unusable::Malformed => "the value doesn't follow RFC 7239's grammar",
            Unusable::NotAnAddress => "the client entry isn't an IP address",
        })
    }
}

/// `[` + the longest IPv6 text form (45 bytes, IPv4-embedded) + `]:65535`.
const MAX_QUOTED: usize = 53;

/// Reads the client from one instance of `header`'s value.
pub fn parse(header: ForwardedHeader, value: &[u8]) -> Result<Client, Unusable> {
    match header {
        ForwardedHeader::XForwardedFor => {
            let end = value.iter().position(|&b| b == b',').unwrap_or(value.len());
            address(trim(&value[..end]))
        }
        ForwardedHeader::XRealIp => address(trim(value)),
        ForwardedHeader::Forwarded => {
            let mut buf = [0u8; MAX_QUOTED];
            let node = forwarded_for(value, &mut buf)?;
            address(node)
        }
    }
}

fn is_ows(b: u8) -> bool {
    b == b' ' || b == b'\t'
}

fn trim(bytes: &[u8]) -> &[u8] {
    let start = bytes.iter().position(|&b| !b.is_ascii_whitespace()).unwrap_or(bytes.len());
    let end = bytes.iter().rposition(|&b| !b.is_ascii_whitespace()).map_or(start, |i| i + 1);
    &bytes[start..end]
}

/// RFC 9110's `tchar`.
fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// The first element's `for=` value: a slice of `value`, or of `buf` when a quoted string held
/// an escape.
fn forwarded_for<'a>(value: &'a [u8], buf: &'a mut [u8; MAX_QUOTED]) -> Result<&'a [u8], Unusable> {
    let mut at = 0;
    let skip_ows = |at: &mut usize| {
        while value.get(*at).copied().is_some_and(is_ows) {
            *at += 1;
        }
    };
    // The span of the first `for=` value, and whether it's a quoted string with escapes.
    let mut found: Option<(usize, usize, bool)> = None;
    loop {
        skip_ows(&mut at);
        match value.get(at) {
            None | Some(b',') => break,
            Some(b';') => {
                at += 1;
                continue;
            }
            Some(_) => {}
        }
        let name_start = at;
        while value.get(at).copied().is_some_and(is_tchar) {
            at += 1;
        }
        let name = &value[name_start..at];
        if name.is_empty() || value.get(at) != Some(&b'=') {
            return Err(Unusable::Malformed);
        }
        at += 1;
        let span = if value.get(at) == Some(&b'"') {
            at += 1;
            let start = at;
            let mut escaped = false;
            loop {
                match value.get(at) {
                    None => return Err(Unusable::Malformed),
                    Some(b'"') => break,
                    Some(b'\\') => {
                        escaped = true;
                        at += 2;
                    }
                    Some(_) => at += 1,
                }
            }
            let span = (start, at, escaped);
            at += 1;
            span
        } else {
            let start = at;
            while value.get(at).is_some_and(|&b| !is_ows(b) && !b"\";,".contains(&b)) {
                at += 1;
            }
            (start, at, false)
        };
        if found.is_none() && name.eq_ignore_ascii_case(b"for") {
            found = Some(span);
        }
        skip_ows(&mut at);
        match value.get(at) {
            None | Some(b',') => break,
            Some(b';') => at += 1,
            Some(_) => return Err(Unusable::Malformed),
        }
    }
    let (start, end, escaped) = found.ok_or(Unusable::NoFor)?;
    if !escaped {
        return Ok(&value[start..end]);
    }
    let mut len = 0;
    let mut bytes = value[start..end].iter();
    while let Some(&b) = bytes.next() {
        let b = if b == b'\\' { *bytes.next().ok_or(Unusable::Malformed)? } else { b };
        *buf.get_mut(len).ok_or(Unusable::NotAnAddress)? = b;
        len += 1;
    }
    Ok(&buf[..len])
}

/// The address rule: the whole candidate as an IP address with no port; else `[v6]`,
/// `[v6]:port`, or `v4:port`.
fn address(node: &[u8]) -> Result<Client, Unusable> {
    if node.is_empty() {
        return Err(Unusable::Empty);
    }
    if node.eq_ignore_ascii_case(b"unknown")
        || node.get(..8).is_some_and(|head| head.eq_ignore_ascii_case(b"unknown:"))
    {
        return Err(Unusable::Unknown);
    }
    if node[0] == b'_' {
        return Err(Unusable::Obfuscated);
    }
    let text = std::str::from_utf8(node).map_err(|_| Unusable::NotAnAddress)?;
    if let Ok(address) = text.parse::<IpAddr>() {
        return Ok(Client { address, port: None });
    }
    if let Some(rest) = text.strip_prefix('[') {
        let (inner, tail) = rest.split_once(']').ok_or(Unusable::NotAnAddress)?;
        let address = inner.parse::<Ipv6Addr>().map_err(|_| Unusable::NotAnAddress)?;
        let port = match tail {
            "" => None,
            _ => Some(port(tail.strip_prefix(':').ok_or(Unusable::NotAnAddress)?)?),
        };
        return Ok(Client { address: IpAddr::V6(address), port });
    }
    let (host, tail) = text.split_once(':').ok_or(Unusable::NotAnAddress)?;
    let address = host.parse::<Ipv4Addr>().map_err(|_| Unusable::NotAnAddress)?;
    Ok(Client { address: IpAddr::V4(address), port: Some(port(tail)?) })
}

/// One to five decimal digits within `u16`.
fn port(text: &str) -> Result<u16, Unusable> {
    if text.is_empty() || text.len() > 5 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Unusable::NotAnAddress);
    }
    text.parse().map_err(|_| Unusable::NotAnAddress)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ForwardedHeader::{Forwarded, XForwardedFor, XRealIp};

    fn client(address: &str, port: Option<u16>) -> Result<Client, Unusable> {
        Ok(Client { address: address.parse().unwrap(), port })
    }

    fn fwd(value: &str) -> Result<Client, Unusable> {
        parse(Forwarded, value.as_bytes())
    }

    fn xff(value: &str) -> Result<Client, Unusable> {
        parse(XForwardedFor, value.as_bytes())
    }

    #[test]
    fn each_header_has_its_lowercase_wire_name() {
        assert_eq!(XForwardedFor.name(), "x-forwarded-for");
        assert_eq!(Forwarded.name(), "forwarded");
        assert_eq!(XRealIp.name(), "x-real-ip");
    }

    /// RFC 7239 section 7's examples.
    #[test]
    fn rfc_7239_examples() {
        assert_eq!(fwd(r#"for="_gazonk""#), Err(Unusable::Obfuscated));
        assert_eq!(
            fwd(r#"For="[2001:db8:cafe::17]:4711""#),
            client("2001:db8:cafe::17", Some(4711))
        );
        assert_eq!(fwd("for=192.0.2.60;proto=http;by=203.0.113.43"), client("192.0.2.60", None));
        assert_eq!(fwd("for=192.0.2.43, for=198.51.100.17"), client("192.0.2.43", None));
        assert_eq!(
            fwd("for=192.0.2.43,for=198.51.100.17;by=203.0.113.60;proto=http;host=example.com"),
            client("192.0.2.43", None)
        );
    }

    #[test]
    fn a_bracketed_ipv6_address_with_no_port_has_no_port() {
        assert_eq!(fwd(r#"for="[2001:db8::cafe]""#), client("2001:db8::cafe", None));
        assert_eq!(xff("[2001:db8::cafe]"), client("2001:db8::cafe", None));
    }

    #[test]
    fn an_unbracketed_ipv6_address_parses_whole_and_keeps_its_last_group() {
        assert_eq!(xff("2001:db8::1"), client("2001:db8::1", None));
        assert_eq!(xff("2001:db8::5:1"), client("2001:db8::5:1", None));
        assert_eq!(fwd(r#"for="2001:db8::5:1""#), client("2001:db8::5:1", None));
    }

    #[test]
    fn an_ipv4_or_bracketed_ipv6_address_with_a_port_is_split() {
        assert_eq!(xff("203.0.113.7:5678"), client("203.0.113.7", Some(5678)));
        assert_eq!(xff("[2001:db8::1]:443"), client("2001:db8::1", Some(443)));
        assert_eq!(parse(XRealIp, b" 203.0.113.7:0 "), client("203.0.113.7", Some(0)));
    }

    #[test]
    fn the_leftmost_xff_entry_wins_trimmed() {
        assert_eq!(xff("  203.0.113.7 , 10.0.0.9, 10.0.0.1"), client("203.0.113.7", None));
        assert_eq!(xff("\t198.51.100.2"), client("198.51.100.2", None));
        assert_eq!(xff(", 10.0.0.1"), Err(Unusable::Empty), "an empty leftmost entry");
    }

    #[test]
    fn x_real_ip_is_the_whole_value_trimmed() {
        assert_eq!(parse(XRealIp, b"  192.0.2.1\t"), client("192.0.2.1", None));
        assert_eq!(parse(XRealIp, b"192.0.2.1, 10.0.0.1"), Err(Unusable::NotAnAddress));
    }

    #[test]
    fn unknown_and_obfuscated_nodes_are_unusable_in_every_header() {
        for header in [XForwardedFor, Forwarded, XRealIp] {
            let value: &[u8] = if header == Forwarded { b"for=unknown" } else { b"unknown" };
            assert_eq!(parse(header, value), Err(Unusable::Unknown), "{header:?}");
            let value: &[u8] = if header == Forwarded { b"for=_hidden" } else { b"_hidden" };
            assert_eq!(parse(header, value), Err(Unusable::Obfuscated), "{header:?}");
        }
        assert_eq!(fwd(r#"for="unknown:4711""#), Err(Unusable::Unknown));
        assert_eq!(fwd("for=UNKNOWN"), Err(Unusable::Unknown));
        assert_eq!(fwd(r#"for="[2001:db8::1]:_port""#), Err(Unusable::NotAnAddress));
    }

    #[test]
    fn an_empty_or_whitespace_value_is_empty() {
        for header in [XForwardedFor, XRealIp] {
            assert_eq!(parse(header, b""), Err(Unusable::Empty), "{header:?}");
            assert_eq!(parse(header, b" \t "), Err(Unusable::Empty), "{header:?}");
        }
        assert_eq!(fwd(""), Err(Unusable::NoFor));
        assert_eq!(fwd("for="), Err(Unusable::Empty));
        assert_eq!(fwd(r#"for="""#), Err(Unusable::Empty));
    }

    #[test]
    fn an_ipv4_mapped_ipv6_address_is_returned_as_given() {
        let mapped = client("::ffff:192.0.2.1", None);
        assert_eq!(xff("::ffff:192.0.2.1"), mapped);
        assert!(matches!(mapped, Ok(Client { address: IpAddr::V6(_), .. })));
    }

    #[test]
    fn forwarded_reads_only_the_first_elements_for() {
        assert_eq!(fwd("proto=https;for=192.0.2.1"), client("192.0.2.1", None));
        assert_eq!(fwd("FOR=192.0.2.1"), client("192.0.2.1", None), "case-insensitive name");
        assert_eq!(fwd("by=10.0.0.1, for=192.0.2.1"), Err(Unusable::NoFor));
        assert_eq!(fwd("for=192.0.2.1;for=192.0.2.2"), client("192.0.2.1", None));
        assert_eq!(fwd(" ; ;for=192.0.2.1 ;"), client("192.0.2.1", None), "empty pairs");
        assert_eq!(fwd("for = 192.0.2.1"), Err(Unusable::Malformed));
    }

    #[test]
    fn forwarded_quoted_strings_hold_separators_and_escapes() {
        assert_eq!(fwd(r#"host="a,b;c";for=192.0.2.1"#), client("192.0.2.1", None));
        assert_eq!(fwd(r#"for="192.0.2.\1:80""#), client("192.0.2.1", Some(80)));
        assert_eq!(fwd(r#"for="192.0.2.1"#), Err(Unusable::Malformed), "unterminated");
        assert_eq!(fwd(r#"for="192.0.2.1\"#), Err(Unusable::Malformed), "a trailing escape");
        assert_eq!(fwd(r#"for="192.0.2.1"x"#), Err(Unusable::Malformed));
        let long = format!(r#"for="\[{}]""#, "0".repeat(60));
        assert_eq!(fwd(&long), Err(Unusable::NotAnAddress));
    }

    #[test]
    fn an_unquoted_bracketed_value_still_reads() {
        assert_eq!(fwd("for=[2001:db8::1]:443"), client("2001:db8::1", Some(443)));
    }

    #[test]
    fn anything_else_is_not_an_address() {
        for value in [
            "-",
            "example.com",
            "example.com:80",
            "[192.0.2.1]",
            "[2001:db8::1]443",
            "[2001:db8::1",
            "203.0.113.7:",
            "203.0.113.7:65536",
            "203.0.113.7:+80",
            "203.0.113.7:000080",
            "203.0.113.007",
            "fe80::1%eth0",
            "2001:db8::1:443:x",
        ] {
            assert_eq!(xff(value), Err(Unusable::NotAnAddress), "{value}");
        }
        assert_eq!(parse(XRealIp, b"\xff192.0.2.1"), Err(Unusable::NotAnAddress));
    }
}
