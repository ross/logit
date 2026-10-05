//! HAProxy's PROXY protocol header, versions 1 (text) and 2 (binary): the origin address a load
//! balancer writes ahead of a relayed TCP stream.
//!
//! The format is HAProxy's `proxy-protocol.txt`
//! (<https://www.haproxy.org/download/2.9/doc/proxy-protocol.txt>). ADR `listener-peer-address`
//! says why this is hand-rolled and what a listener does with the result.
//!
//! [`parse`] reads the start of a stream and returns [`Parse::Complete`] with the header's length,
//! [`Parse::Incomplete`] when more bytes are needed, or an error. A reader that must not consume
//! past the header relies on [`Parse::Incomplete`]'s guarantee that every byte it was given
//! belongs to the header, so it can consume all of them and read more.
//!
//! What a valid header is:
//!
//! - **Version 1** is `PROXY` SP family SP source SP destination SP source-port SP
//!   destination-port CRLF, at most [`V1_MAX_LEN`] bytes with the CRLF. The family is `TCP4` or
//!   `TCP6`, and both addresses must be of it. A port is decimal in `0..=65535` with no leading
//!   zero. `PROXY UNKNOWN` may be followed by anything up to the CRLF, which is ignored. A lone CR
//!   or LF never ends the line.
//! - **Version 2** is the 12-byte [`V2_SIGNATURE`], a version-and-command byte (high nibble `2`;
//!   command `0` `LOCAL` or `1` `PROXY`), a family-and-transport byte, and a big-endian `u16`
//!   length of what follows: the address block (12 bytes for IPv4, 36 for IPv6, 216 for
//!   `AF_UNIX`) and then type-length-value extensions (TLVs). The TLVs are skipped unread.
//!   `LOCAL` ignores the family byte and any address bytes, as the spec requires. A `PROXY`
//!   header's family must be `0` to `3` and its transport `0` to `2`, and its length must cover
//!   the family's address block.
//!
//! [`Origin::None`] is a header with no usable origin: v2 `LOCAL`, v2 `AF_UNSPEC` or an
//! unspecified transport, v1 `UNKNOWN`, or a v2 `AF_UNIX` source path that is empty (an unnamed or
//! abstract socket). The connection is still valid.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// The longest version 1 header, CRLF included: `PROXY UNKNOWN` with two full IPv6 addresses and
/// two five-digit ports.
pub const V1_MAX_LEN: usize = 107;

/// The fixed part of a version 2 header: signature, version and command, family and transport,
/// and the length of the rest.
pub const V2_FIXED_LEN: usize = 16;

/// The 12 bytes a version 2 header starts with.
pub const V2_SIGNATURE: [u8; 12] = *b"\r\n\r\n\0\r\nQUIT\n";

const V1_PREFIX: &[u8] = b"PROXY ";
/// The shortest valid header of either version: `PROXY UNKNOWN\r\n`.
pub const MIN_LEN: usize = 15;

const UNIX_PATH_LEN: usize = 108;

/// Where the relayed connection came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// No usable origin; the module doc lists the cases.
    None,
    /// The relayed connection's source address and port.
    Ip(SocketAddr),
    /// A v2 `AF_UNIX` source path, its bytes up to the first NUL.
    Unix(Vec<u8>),
}

/// What [`parse`] made of the start of a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parse {
    /// A whole header: its origin, and its length, the number of bytes to consume before the
    /// payload.
    Complete { origin: Origin, len: usize },
    /// A valid header start that needs more bytes. Every byte given to [`parse`] belongs to the
    /// header, so a reader can consume all of them. `len` is the header's total length once it's
    /// known, which is from the 16th byte of a version 2 header; a version 1 header's length is
    /// known only at its CRLF.
    Incomplete { len: Option<usize> },
}

/// Why a stream doesn't start with a valid PROXY header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProxyError {
    #[error("the stream doesn't start with a PROXY protocol signature")]
    NoSignature,
    #[error("PROXY v2 version {0} isn't 2")]
    Version(u8),
    #[error("PROXY v2 command {0} is neither LOCAL nor PROXY")]
    Command(u8),
    #[error("PROXY v2 address family {0} isn't one the spec defines")]
    Family(u8),
    #[error("PROXY v2 transport {0} isn't one the spec defines")]
    Transport(u8),
    #[error("PROXY v2 length {len} is shorter than the {needed}-byte address block")]
    ShortAddressBlock { len: usize, needed: usize },
    #[error("PROXY v1 line has no CRLF within its first 107 bytes")]
    V1TooLong,
    #[error("PROXY v1 line has a CR or LF that isn't its CRLF")]
    V1StrayLineEnd,
    #[error("PROXY v1 line is malformed: {0}")]
    V1Malformed(&'static str),
}

/// Parses a PROXY header at the start of `buf`, which may hold payload bytes after it. Returns as
/// soon as the bytes seen can't start a valid header, so a reader can close the connection without
/// waiting for the rest.
pub fn parse(buf: &[u8]) -> Result<Parse, ProxyError> {
    match buf.first() {
        None => Ok(Parse::Incomplete { len: None }),
        Some(b'\r') => parse_v2(buf),
        Some(_) => parse_v1(buf),
    }
}

fn parse_v2(buf: &[u8]) -> Result<Parse, ProxyError> {
    let signature = &buf[..buf.len().min(V2_SIGNATURE.len())];
    if signature != &V2_SIGNATURE[..signature.len()] {
        return Err(ProxyError::NoSignature);
    }
    if let Some(&ver_cmd) = buf.get(12) {
        let version = ver_cmd >> 4;
        if version != 2 {
            return Err(ProxyError::Version(version));
        }
        let command = ver_cmd & 0x0F;
        if command > 1 {
            return Err(ProxyError::Command(command));
        }
    }
    if buf.len() < V2_FIXED_LEN {
        return Ok(Parse::Incomplete { len: None });
    }
    let local = buf[12] & 0x0F == 0;
    let family = buf[13] >> 4;
    let transport = buf[13] & 0x0F;
    let rest = usize::from(u16::from_be_bytes([buf[14], buf[15]]));
    let len = V2_FIXED_LEN + rest;
    if !local {
        if family > 3 {
            return Err(ProxyError::Family(family));
        }
        if transport > 2 {
            return Err(ProxyError::Transport(transport));
        }
        let needed = match family {
            1 => 12,
            2 => 36,
            3 => 2 * UNIX_PATH_LEN,
            _ => 0,
        };
        if rest < needed {
            return Err(ProxyError::ShortAddressBlock { len: rest, needed });
        }
    }
    if buf.len() < len {
        return Ok(Parse::Incomplete { len: Some(len) });
    }
    let block = &buf[V2_FIXED_LEN..len];
    let origin = if local || transport == 0 {
        Origin::None
    } else {
        match family {
            1 => {
                let ip = Ipv4Addr::new(block[0], block[1], block[2], block[3]);
                Origin::Ip(SocketAddr::new(
                    IpAddr::V4(ip),
                    u16::from_be_bytes([block[8], block[9]]),
                ))
            }
            2 => {
                let mut octets = [0; 16];
                octets.copy_from_slice(&block[..16]);
                let port = u16::from_be_bytes([block[32], block[33]]);
                Origin::Ip(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
            }
            3 => {
                let source = &block[..UNIX_PATH_LEN];
                let path = &source[..source.iter().position(|&b| b == 0).unwrap_or(UNIX_PATH_LEN)];
                if path.is_empty() {
                    Origin::None
                } else {
                    Origin::Unix(path.to_vec())
                }
            }
            _ => Origin::None,
        }
    };
    Ok(Parse::Complete { origin, len })
}

fn parse_v1(buf: &[u8]) -> Result<Parse, ProxyError> {
    let prefix = &buf[..buf.len().min(V1_PREFIX.len())];
    if prefix != &V1_PREFIX[..prefix.len()] {
        return Err(ProxyError::NoSignature);
    }
    let window = &buf[..buf.len().min(V1_MAX_LEN)];
    let Some(end) = window.iter().position(|&b| b == b'\r' || b == b'\n') else {
        if buf.len() >= V1_MAX_LEN {
            return Err(ProxyError::V1TooLong);
        }
        return Ok(Parse::Incomplete { len: None });
    };
    match window.get(end..end + 2) {
        Some(b"\r\n") => {}
        // A CR that is the last byte seen may yet be followed by its LF.
        None if window[end] == b'\r' && buf.len() < V1_MAX_LEN => {
            return Ok(Parse::Incomplete { len: None });
        }
        None if window[end] == b'\r' => return Err(ProxyError::V1TooLong),
        _ => return Err(ProxyError::V1StrayLineEnd),
    }
    let origin = parse_v1_line(&buf[V1_PREFIX.len()..end])?;
    Ok(Parse::Complete { origin, len: end + 2 })
}

/// The fields after `PROXY `, CRLF excluded.
fn parse_v1_line(line: &[u8]) -> Result<Origin, ProxyError> {
    if let Some(rest) = line.strip_prefix(b"UNKNOWN") {
        if rest.is_empty() || rest[0] == b' ' {
            return Ok(Origin::None);
        }
        return Err(ProxyError::V1Malformed("unknown family"));
    }
    let mut fields = line.split(|&b| b == b' ');
    let mut next = |what: &'static str| fields.next().ok_or(ProxyError::V1Malformed(what));
    let family = next("missing family")?;
    let source = next("missing source address")?;
    let destination = next("missing destination address")?;
    let source_port = next("missing source port")?;
    let destination_port = next("missing destination port")?;
    if fields.next().is_some() {
        return Err(ProxyError::V1Malformed("a field after the destination port"));
    }
    let v6 = match family {
        b"TCP4" => false,
        b"TCP6" => true,
        _ => return Err(ProxyError::V1Malformed("unknown family")),
    };
    let source = v1_address(source, v6)?;
    v1_address(destination, v6)?;
    let port = v1_port(source_port)?;
    v1_port(destination_port)?;
    Ok(Origin::Ip(SocketAddr::new(source, port)))
}

/// An address in `family`'s text form. Rust's `Ipv4Addr` parser rejects a leading zero, as the
/// spec requires.
fn v1_address(field: &[u8], v6: bool) -> Result<IpAddr, ProxyError> {
    let text = std::str::from_utf8(field).map_err(|_| ProxyError::V1Malformed("bad address"))?;
    let parsed = if v6 {
        text.parse::<Ipv6Addr>().map(IpAddr::V6)
    } else {
        text.parse::<Ipv4Addr>().map(IpAddr::V4)
    };
    parsed.map_err(|_| ProxyError::V1Malformed("bad address"))
}

fn v1_port(field: &[u8]) -> Result<u16, ProxyError> {
    let malformed = ProxyError::V1Malformed("bad port");
    if field.is_empty() || field.len() > 5 || !field.iter().all(u8::is_ascii_digit) {
        return Err(malformed);
    }
    if field.len() > 1 && field[0] == b'0' {
        return Err(malformed);
    }
    let value = field.iter().fold(0u32, |n, &d| n * 10 + u32::from(d - b'0'));
    u16::try_from(value).map_err(|_| malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(buf: &[u8]) -> (Origin, usize) {
        match parse(buf) {
            Ok(Parse::Complete { origin, len }) => (origin, len),
            other => panic!("expected a complete header from {buf:?}, got {other:?}"),
        }
    }

    fn ip(text: &str) -> Origin {
        Origin::Ip(text.parse().unwrap())
    }

    /// A v2 header: `ver_cmd`, `fam`, then `body` (address block and TLVs) with its length.
    fn v2(ver_cmd: u8, fam: u8, body: &[u8]) -> Vec<u8> {
        let mut out = V2_SIGNATURE.to_vec();
        out.push(ver_cmd);
        out.push(fam);
        out.extend_from_slice(&u16::try_from(body.len()).unwrap().to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    fn v4_block() -> Vec<u8> {
        let mut block = vec![192, 168, 0, 1, 192, 168, 0, 11];
        block.extend_from_slice(&56324u16.to_be_bytes());
        block.extend_from_slice(&443u16.to_be_bytes());
        block
    }

    // ---- version 1 ------------------------------------------------------------------------

    /// The spec's example line, with the HTTP request after it left unconsumed.
    #[test]
    fn the_spec_v1_example_parses_and_stops_at_its_crlf() {
        let header = b"PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n";
        let mut stream = header.to_vec();
        stream.extend_from_slice(b"GET / HTTP/1.1\r\n");
        assert_eq!(complete(&stream), (ip("192.168.0.1:56324"), header.len()));
    }

    #[test]
    fn a_v1_tcp6_line_parses() {
        let line = b"PROXY TCP6 2001:db8::1 2001:db8::2 65535 0\r\n";
        assert_eq!(complete(line), (ip("[2001:db8::1]:65535"), line.len()));
    }

    #[test]
    fn the_longest_v4_and_v6_lines_parse() {
        let v4 = b"PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n";
        assert_eq!(v4.len(), 56);
        assert_eq!(complete(v4).0, ip("255.255.255.255:65535"));
        let full = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff";
        let v6 = format!("PROXY TCP6 {full} {full} 65535 65535\r\n");
        assert_eq!(v6.len(), 104);
        assert_eq!(complete(v6.as_bytes()).0, ip(&format!("[{full}]:65535")));
    }

    #[test]
    fn v1_unknown_has_no_origin_in_short_and_long_form() {
        assert_eq!(complete(b"PROXY UNKNOWN\r\n"), (Origin::None, MIN_LEN));
        let full = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff";
        let long = format!("PROXY UNKNOWN {full} {full} 65535 65535\r\n");
        assert_eq!(long.len(), V1_MAX_LEN);
        assert_eq!(complete(long.as_bytes()), (Origin::None, V1_MAX_LEN));
        // The receiver ignores whatever follows UNKNOWN.
        assert_eq!(complete(b"PROXY UNKNOWN garbage\r\n").0, Origin::None);
    }

    /// Every proper prefix of a valid line is `Incomplete`, so a reader can consume it all.
    #[test]
    fn every_prefix_of_a_v1_line_is_incomplete() {
        let line = b"PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n";
        for end in 0..line.len() {
            assert_eq!(parse(&line[..end]), Ok(Parse::Incomplete { len: None }), "at {end}");
        }
    }

    #[test]
    fn v1_rejections() {
        let cases: &[(&[u8], ProxyError)] = &[
            (b"GET / HTTP/1.1\r\n", ProxyError::NoSignature),
            (b"PROXY\r\n", ProxyError::NoSignature),
            (b"PROXYTCP4", ProxyError::NoSignature),
            (b"PROXY TCP4 1.2.3.4 5.6.7.8 1 2\n", ProxyError::V1StrayLineEnd),
            (b"PROXY TCP4 1.2.3.4 5.6.7.8 1 2\rX", ProxyError::V1StrayLineEnd),
            (b"PROXY TCP4 1.2.3.4\n5.6.7.8 1 2\r\n", ProxyError::V1StrayLineEnd),
            (b"PROXY UNKNOWNX\r\n", ProxyError::V1Malformed("unknown family")),
            (b"PROXY UDP4 1.2.3.4 5.6.7.8 1 2\r\n", ProxyError::V1Malformed("unknown family")),
            (b"PROXY tcp4 1.2.3.4 5.6.7.8 1 2\r\n", ProxyError::V1Malformed("unknown family")),
            (
                b"PROXY TCP4 1.2.3.4 5.6.7.8 1\r\n",
                ProxyError::V1Malformed("missing destination port"),
            ),
            (
                b"PROXY TCP4 1.2.3.4 5.6.7.8 1 2 3\r\n",
                ProxyError::V1Malformed("a field after the destination port"),
            ),
            (
                b"PROXY TCP4  1.2.3.4 5.6.7.8 1 2\r\n",
                ProxyError::V1Malformed("a field after the destination port"),
            ),
            (
                b"PROXY TCP4 1.2.3.4 5.6.7.8 1 2 \r\n",
                ProxyError::V1Malformed("a field after the destination port"),
            ),
            (b"PROXY TCP4 01.2.3.4 5.6.7.8 1 2\r\n", ProxyError::V1Malformed("bad address")),
            (b"PROXY TCP4 1.2.3.256 5.6.7.8 1 2\r\n", ProxyError::V1Malformed("bad address")),
            (b"PROXY TCP4 ::1 5.6.7.8 1 2\r\n", ProxyError::V1Malformed("bad address")),
            (b"PROXY TCP4 1.2.3.4 ::1 1 2\r\n", ProxyError::V1Malformed("bad address")),
            (b"PROXY TCP6 1.2.3.4 ::1 1 2\r\n", ProxyError::V1Malformed("bad address")),
            (b"PROXY TCP6 ::1 ::g 1 2\r\n", ProxyError::V1Malformed("bad address")),
            (b"PROXY TCP4 1.2.3.4 5.6.7.8 65536 2\r\n", ProxyError::V1Malformed("bad port")),
            (b"PROXY TCP4 1.2.3.4 5.6.7.8 1 99999\r\n", ProxyError::V1Malformed("bad port")),
            (b"PROXY TCP4 1.2.3.4 5.6.7.8 01 2\r\n", ProxyError::V1Malformed("bad port")),
            (b"PROXY TCP4 1.2.3.4 5.6.7.8 +1 2\r\n", ProxyError::V1Malformed("bad port")),
            (b"PROXY TCP4 1.2.3.4 5.6.7.8 1 \r\n", ProxyError::V1Malformed("bad port")),
            (b"PROXY TCP4 1.2.3.4 5.6.7.8 100000 2\r\n", ProxyError::V1Malformed("bad port")),
        ];
        for (input, expected) in cases {
            assert_eq!(parse(input), Err(*expected), "for {:?}", String::from_utf8_lossy(input));
        }
    }

    #[test]
    fn a_v1_line_with_no_crlf_in_107_bytes_is_rejected() {
        let mut long = b"PROXY UNKNOWN ".to_vec();
        long.resize(V1_MAX_LEN, b'a');
        assert_eq!(parse(&long), Err(ProxyError::V1TooLong));
        // A CR at byte 107 can't be completed within the cap.
        long[V1_MAX_LEN - 1] = b'\r';
        assert_eq!(parse(&long), Err(ProxyError::V1TooLong));
        // One byte shorter is still waiting.
        assert_eq!(parse(&long[..V1_MAX_LEN - 1]), Ok(Parse::Incomplete { len: None }));
    }

    #[test]
    fn port_zero_is_valid_and_a_leading_zero_is_not() {
        assert_eq!(complete(b"PROXY TCP4 1.2.3.4 5.6.7.8 0 0\r\n").0, ip("1.2.3.4:0"));
        assert_eq!(v1_port(b"00"), Err(ProxyError::V1Malformed("bad port")));
    }

    // ---- version 2 ------------------------------------------------------------------------

    #[test]
    fn a_v2_ipv4_proxy_header_parses() {
        let header = v2(0x21, 0x11, &v4_block());
        assert_eq!(complete(&header), (ip("192.168.0.1:56324"), 28));
    }

    #[test]
    fn a_v2_ipv6_proxy_header_parses() {
        let mut block = Vec::new();
        block.extend_from_slice(&"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets());
        block.extend_from_slice(&"2001:db8::2".parse::<Ipv6Addr>().unwrap().octets());
        block.extend_from_slice(&1234u16.to_be_bytes());
        block.extend_from_slice(&80u16.to_be_bytes());
        let header = v2(0x21, 0x21, &block);
        assert_eq!(complete(&header), (ip("[2001:db8::1]:1234"), 52));
    }

    /// TLVs after the address block count toward the length and are skipped; the payload after
    /// them is left unconsumed.
    #[test]
    fn v2_tlvs_are_skipped() {
        let mut body = v4_block();
        body.extend_from_slice(&[0x02, 0x00, 0x0B]); // PP2_TYPE_AUTHORITY
        body.extend_from_slice(b"example.com");
        body.extend_from_slice(&[0x04, 0x00, 0x00]); // PP2_TYPE_NOOP, empty
        let header = v2(0x21, 0x11, &body);
        let mut stream = header.clone();
        stream.extend_from_slice(b"payload\n");
        assert_eq!(complete(&stream), (ip("192.168.0.1:56324"), header.len()));
    }

    /// A UDP-relayed origin is still an origin.
    #[test]
    fn a_v2_dgram_origin_parses() {
        assert_eq!(complete(&v2(0x21, 0x12, &v4_block())).0, ip("192.168.0.1:56324"));
    }

    #[test]
    fn v2_local_with_length_zero_has_no_origin() {
        assert_eq!(complete(&v2(0x20, 0x00, &[])), (Origin::None, V2_FIXED_LEN));
    }

    /// `LOCAL` ignores the family and any address bytes, whatever they hold.
    #[test]
    fn v2_local_ignores_its_family_and_address_bytes() {
        assert_eq!(complete(&v2(0x20, 0x11, &v4_block())), (Origin::None, 28));
        assert_eq!(complete(&v2(0x20, 0xFF, &[1, 2, 3])), (Origin::None, 19));
    }

    #[test]
    fn v2_unspec_family_or_transport_has_no_origin() {
        assert_eq!(complete(&v2(0x21, 0x00, &[])).0, Origin::None);
        assert_eq!(complete(&v2(0x21, 0x00, &[9; 7])), (Origin::None, 23));
        assert_eq!(complete(&v2(0x21, 0x10, &v4_block())).0, Origin::None);
    }

    #[test]
    fn a_v2_unix_header_yields_the_source_path() {
        let mut block = vec![0u8; 216];
        block[..13].copy_from_slice(b"/run/app.sock");
        block[108..113].copy_from_slice(b"/dest");
        assert_eq!(
            complete(&v2(0x21, 0x31, &block)),
            (Origin::Unix(b"/run/app.sock".to_vec()), 232)
        );
        // A path that fills all 108 bytes has no NUL.
        let full = vec![b'p'; 216];
        assert_eq!(complete(&v2(0x21, 0x32, &full)).0, Origin::Unix(vec![b'p'; 108]));
        // An unnamed or abstract socket's path is empty.
        assert_eq!(complete(&v2(0x21, 0x31, &[0; 216])).0, Origin::None);
    }

    #[test]
    fn every_prefix_of_a_v2_header_is_incomplete_and_knows_its_length_from_byte_16() {
        let header = v2(0x21, 0x11, &v4_block());
        for end in 0..header.len() {
            let len = (end >= V2_FIXED_LEN).then_some(header.len());
            assert_eq!(parse(&header[..end]), Ok(Parse::Incomplete { len }), "at {end}");
        }
    }

    #[test]
    fn v2_rejections() {
        let mut bad_signature = v2(0x21, 0x11, &v4_block());
        bad_signature[11] = b'X';
        let cases: Vec<(Vec<u8>, ProxyError)> = vec![
            (bad_signature, ProxyError::NoSignature),
            (b"\r\nGET".to_vec(), ProxyError::NoSignature),
            (v2(0x11, 0x11, &v4_block()), ProxyError::Version(1)),
            (v2(0x31, 0x11, &v4_block()), ProxyError::Version(3)),
            (v2(0x22, 0x11, &v4_block()), ProxyError::Command(2)),
            (v2(0x2F, 0x11, &v4_block()), ProxyError::Command(15)),
            (v2(0x21, 0x41, &[0; 12]), ProxyError::Family(4)),
            (v2(0x21, 0x13, &v4_block()), ProxyError::Transport(3)),
            (v2(0x21, 0x11, &[0; 11]), ProxyError::ShortAddressBlock { len: 11, needed: 12 }),
            (v2(0x21, 0x21, &[0; 12]), ProxyError::ShortAddressBlock { len: 12, needed: 36 }),
            (v2(0x21, 0x31, &[0; 215]), ProxyError::ShortAddressBlock { len: 215, needed: 216 }),
            (v2(0x21, 0x11, &[]), ProxyError::ShortAddressBlock { len: 0, needed: 12 }),
        ];
        for (input, expected) in cases {
            assert_eq!(parse(&input), Err(expected), "for {input:?}");
        }
    }

    /// A bad version or command is refused at byte 13, before the length arrives.
    #[test]
    fn a_v2_bad_command_is_refused_before_the_fixed_part_completes() {
        let mut start = V2_SIGNATURE.to_vec();
        start.push(0x23);
        assert_eq!(parse(&start), Err(ProxyError::Command(3)));
    }

    /// The largest declared length is still bounded: 16 plus 65,535.
    #[test]
    fn the_largest_v2_length_is_incomplete_until_it_all_arrives() {
        let mut header = V2_SIGNATURE.to_vec();
        header.extend_from_slice(&[0x21, 0x11, 0xFF, 0xFF]);
        assert_eq!(parse(&header), Ok(Parse::Incomplete { len: Some(V2_FIXED_LEN + 65_535) }));
        header.resize(V2_FIXED_LEN + 65_535, 0);
        assert_eq!(complete(&header), (ip("0.0.0.0:0"), V2_FIXED_LEN + 65_535));
    }
}
