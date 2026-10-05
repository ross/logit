//! The sender's address on listener events: the `network.peer.address` and `network.peer.port`
//! attributes a shared driver stamps, after decode, on a listener configured `peer: true`.
//!
//! ADR `listener-peer-address` settles the names, the text form, why these are event attributes
//! and not resource ones, and why the driver's value replaces a same-named attribute a decoder
//! produced. A driver builds one [`PeerAttrs`] per peer and calls [`PeerAttrs::stamp`] on every
//! event decoded from it.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{Event, Symbol, Value};
use std::net::SocketAddr;
use std::sync::OnceLock;

/// The attribute holding the immediate socket peer's address, in OpenTelemetry's name.
pub const PEER_ADDRESS: &str = "network.peer.address";
/// The attribute holding the immediate socket peer's port, in OpenTelemetry's name.
pub const PEER_PORT: &str = "network.peer.port";

/// [`PEER_ADDRESS`] and [`PEER_PORT`], interned once per process rather than per peer.
fn keys() -> (Symbol, Symbol) {
    static KEYS: OnceLock<(Symbol, Symbol)> = OnceLock::new();
    *KEYS.get_or_init(|| (intern(PEER_ADDRESS), intern(PEER_PORT)))
}

/// One peer's attribute values, formatted once and cloned onto each event by [`Self::stamp`].
#[derive(Debug, Clone, PartialEq)]
pub struct PeerAttrs {
    address: Value,
    /// `None` for a Unix socket peer, which has a path and no port.
    port: Option<Value>,
}

impl PeerAttrs {
    /// A TCP or UDP peer. An IPv4-mapped IPv6 address (`::ffff:192.0.2.1`, what a dual-stack
    /// socket reports for an IPv4 sender) is written as IPv4, so one sender reads the same on
    /// either kind of socket.
    pub fn from_socket(addr: SocketAddr) -> Self {
        Self {
            address: shared_str(addr.ip().to_canonical().to_string()),
            port: Some(Value::I64(i64::from(addr.port()))),
        }
    }

    /// A Unix socket peer: its path when it bound one, or `None` for an unbound client socket, the
    /// usual case, which gets no attribute. A path that isn't UTF-8 is written lossily, since a
    /// `Value::Str` must be UTF-8.
    pub fn from_unix(addr: &tokio::net::unix::SocketAddr) -> Option<Self> {
        let path = addr.as_pathname()?;
        Some(Self { address: shared_str(path.to_string_lossy().into_owned()), port: None })
    }

    /// Writes this peer onto every event in `events`, replacing a same-named attribute.
    pub fn stamp(&self, events: &mut [Event]) {
        let (address_key, port_key) = keys();
        for event in events {
            event.attributes.insert_sym(address_key, self.address.clone());
            if let Some(port) = &self.port {
                event.attributes.insert_sym(port_key, port.clone());
            }
        }
    }
}

/// A `Value::Str` whose clones are reference-count increments. `Bytes::from(String)` allocates
/// its shared header on the first clone, so that clone happens here, once per peer, and not on
/// the first stamped event.
fn shared_str(text: String) -> Value {
    let bytes = Bytes::from(text);
    drop(bytes.clone());
    Value::Str(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::AttrMap;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn event() -> Event {
        Event::empty(0, AttrMap::new())
    }

    fn str_attr<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
        event.attributes.get(key).and_then(Value::as_str)
    }

    #[test]
    fn an_ipv4_peer_stamps_its_address_and_port() {
        let peer =
            PeerAttrs::from_socket(SocketAddr::new(Ipv4Addr::new(192, 0, 2, 7).into(), 5140));
        let mut events = vec![event(), event()];
        peer.stamp(&mut events);
        for event in &events {
            assert_eq!(str_attr(event, PEER_ADDRESS), Some("192.0.2.7"));
            assert_eq!(event.attributes.get(PEER_PORT), Some(&Value::I64(5140)));
        }
    }

    #[test]
    fn an_ipv4_mapped_ipv6_peer_is_written_as_ipv4() {
        let mapped: IpAddr = Ipv4Addr::new(192, 0, 2, 7).to_ipv6_mapped().into();
        let peer = PeerAttrs::from_socket(SocketAddr::new(mapped, 1));
        let mut events = vec![event()];
        peer.stamp(&mut events);
        assert_eq!(str_attr(&events[0], PEER_ADDRESS), Some("192.0.2.7"));
    }

    #[test]
    fn an_ipv6_peer_is_written_in_its_standard_text_form() {
        let peer = PeerAttrs::from_socket(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 9));
        let mut events = vec![event()];
        peer.stamp(&mut events);
        assert_eq!(str_attr(&events[0], PEER_ADDRESS), Some("::1"));
        assert_eq!(events[0].attributes.get(PEER_PORT), Some(&Value::I64(9)));
    }

    /// The ADR's collision rule: the observed peer replaces what a decoder wrote.
    #[test]
    fn a_decoded_attribute_of_the_same_name_is_replaced() {
        let peer = PeerAttrs::from_socket(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 2));
        let mut decoded = event();
        decoded.attributes.insert(PEER_ADDRESS, Value::str("forged"));
        decoded.attributes.insert(PEER_PORT, Value::str("also forged"));
        decoded.attributes.insert("other", Value::I64(1));
        let mut events = vec![decoded];
        peer.stamp(&mut events);
        assert_eq!(str_attr(&events[0], PEER_ADDRESS), Some("127.0.0.1"));
        assert_eq!(events[0].attributes.get(PEER_PORT), Some(&Value::I64(2)));
        assert_eq!(events[0].attributes.get("other"), Some(&Value::I64(1)));
        assert_eq!(events[0].attributes.len(), 3);
    }

    /// Every stamped event shares the one buffer the address was formatted into.
    #[test]
    fn every_stamped_address_shares_one_buffer() {
        let peer = PeerAttrs::from_socket(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 2));
        let mut events = vec![event(), event()];
        peer.stamp(&mut events);
        let ptr = |event: &Event| match event.attributes.get(PEER_ADDRESS) {
            Some(Value::Str(bytes)) => bytes.as_ptr(),
            other => panic!("expected a string address, got {other:?}"),
        };
        assert_eq!(ptr(&events[0]), ptr(&events[1]));
    }

    #[test]
    fn a_bound_unix_peer_stamps_its_path_and_no_port() {
        let dir = logit_pipeline::test_util::scratch_dir("peer-bound");
        let listen = dir.join("listen.sock");
        let client = dir.join("client.sock");
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let listener = tokio::net::UnixListener::bind(&listen).unwrap();
            // A client binds a path before connecting only through a raw socket.
            let socket =
                socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None).unwrap();
            socket.bind(&socket2::SockAddr::unix(&client).unwrap()).unwrap();
            socket.connect(&socket2::SockAddr::unix(&listen).unwrap()).unwrap();
            let (_server_side, addr) = listener.accept().await.unwrap();
            let peer = PeerAttrs::from_unix(&addr).expect("a bound client has a path");
            let mut events = vec![event()];
            peer.stamp(&mut events);
            assert_eq!(str_attr(&events[0], PEER_ADDRESS), Some(client.to_str().unwrap()));
            assert_eq!(events[0].attributes.get(PEER_PORT), None);
            drop(socket);
        });
    }

    #[test]
    fn an_unbound_unix_peer_has_nothing_to_stamp() {
        let dir = logit_pipeline::test_util::scratch_dir("peer-unbound");
        let listen = dir.join("listen.sock");
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let listener = tokio::net::UnixListener::bind(&listen).unwrap();
            let _client = tokio::net::UnixStream::connect(&listen).await.unwrap();
            let (_server_side, addr) = listener.accept().await.unwrap();
            assert_eq!(PeerAttrs::from_unix(&addr), None);
        });
    }
}
