//! A forwarding header's value through `forwarded::parse`, as a `forwarded:` component reads it.
//! The first byte picks the header (`0` `X-Forwarded-For`, `1` `Forwarded`, `2` `X-Real-IP`,
//! modulo 3) and the rest is the value. Oracles: `X-Forwarded-For` reads as `X-Real-IP` over its
//! leftmost entry; a usable client, written back as a node (`v4`, `v6`, `v4:port`, or
//! `[v6]:port`), reads as the same client from `X-Real-IP` and from a quoted `Forwarded` `for=`.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_proto::forwarded::{parse, ForwardedHeader};
use std::net::IpAddr;

fuzz_target!(|data: &[u8]| {
    let Some((&selector, value)) = data.split_first() else { return };
    let header = match selector % 3 {
        0 => ForwardedHeader::XForwardedFor,
        1 => ForwardedHeader::Forwarded,
        _ => ForwardedHeader::XRealIp,
    };
    let result = parse(header, value);
    if header == ForwardedHeader::XForwardedFor {
        let end = value.iter().position(|&b| b == b',').unwrap_or(value.len());
        assert_eq!(result, parse(ForwardedHeader::XRealIp, &value[..end]), "leftmost entry");
    }
    if let Ok(client) = result {
        let node = match (client.address, client.port) {
            (address, None) => address.to_string(),
            (IpAddr::V4(address), Some(port)) => format!("{address}:{port}"),
            (IpAddr::V6(address), Some(port)) => format!("[{address}]:{port}"),
        };
        assert_eq!(parse(ForwardedHeader::XRealIp, node.as_bytes()), Ok(client), "{node}");
        let quoted = format!("for=\"{node}\"");
        assert_eq!(parse(ForwardedHeader::Forwarded, quoted.as_bytes()), Ok(client), "{quoted}");
    }
});
