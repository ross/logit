//! The sender's address on listener events: the `network.peer.address` and `network.peer.port`
//! attributes a shared driver stamps, after decode, on a listener configured `peer: true`, and the
//! `client.address` and `client.port` attributes the stream driver stamps from a PROXY protocol
//! header under `proxy_protocol: true`.
//!
//! ADR `listener-peer-address` settles the names, the text form, why these are event attributes
//! and not resource ones, and why the driver's value replaces a same-named attribute a decoder
//! produced. A driver builds one [`PeerAttrs`] per peer and calls [`PeerAttrs::stamp`] on every
//! event decoded from it. The stream driver builds one per connection. The datagram driver reads a
//! [`Sender`] with every datagram and goes through a [`PeerCache`], which formats an address only
//! when the sender differs from the previous datagram's. A PROXY header's origin becomes a
//! [`PeerAttrs`] through [`PeerAttrs::client`], and [`ConnectionAttrs`] holds a connection's two.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{Event, Symbol, Value};
use logit_proto::proxy::Origin;
use std::net::SocketAddr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::OnceLock;

/// The attribute holding the immediate socket peer's address, in OpenTelemetry's name.
pub const PEER_ADDRESS: &str = "network.peer.address";
/// The attribute holding the immediate socket peer's port, in OpenTelemetry's name.
pub const PEER_PORT: &str = "network.peer.port";

/// The attribute holding the origin a PROXY protocol header names, in OpenTelemetry's name.
pub const CLIENT_ADDRESS: &str = "client.address";
/// The attribute holding the port of the origin a PROXY protocol header names.
pub const CLIENT_PORT: &str = "client.port";

/// [`PEER_ADDRESS`] and [`PEER_PORT`], interned once per process rather than per peer.
fn peer_keys() -> (Symbol, Symbol) {
    static KEYS: OnceLock<(Symbol, Symbol)> = OnceLock::new();
    *KEYS.get_or_init(|| (intern(PEER_ADDRESS), intern(PEER_PORT)))
}

/// [`CLIENT_ADDRESS`] and [`CLIENT_PORT`], interned once per process.
fn client_keys() -> (Symbol, Symbol) {
    static KEYS: OnceLock<(Symbol, Symbol)> = OnceLock::new();
    *KEYS.get_or_init(|| (intern(CLIENT_ADDRESS), intern(CLIENT_PORT)))
}

/// One address's attribute values, formatted once and cloned onto each event by [`Self::stamp`]:
/// the socket peer's under `network.peer.*`, or a PROXY header's origin under `client.*`.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerAttrs {
    /// The address and port attribute names.
    keys: (Symbol, Symbol),
    address: Value,
    /// `None` for a Unix socket peer, which has a path and no port.
    port: Option<Value>,
}

impl PeerAttrs {
    /// A TCP or UDP peer. An IPv4-mapped IPv6 address (`::ffff:192.0.2.1`, what a dual-stack
    /// socket reports for an IPv4 sender) is written as IPv4, so one sender reads the same on
    /// either kind of socket.
    pub fn from_socket(addr: SocketAddr) -> Self {
        Self::ip(peer_keys(), addr)
    }

    fn ip(keys: (Symbol, Symbol), addr: SocketAddr) -> Self {
        Self {
            keys,
            address: shared_str(addr.ip().to_canonical().to_string()),
            port: Some(Value::I64(i64::from(addr.port()))),
        }
    }

    /// A Unix socket peer: its path when it bound one, or `None` for an unbound client socket, the
    /// usual case, which gets no attribute. A path that isn't UTF-8 is written lossily, since a
    /// `Value::Str` must be UTF-8.
    pub fn from_unix(addr: &tokio::net::unix::SocketAddr) -> Option<Self> {
        addr.as_pathname().map(Self::from_path)
    }

    /// A Unix socket peer that bound `path`. A path that isn't UTF-8 is written lossily.
    pub fn from_path(path: &Path) -> Self {
        Self {
            keys: peer_keys(),
            address: shared_str(path.to_string_lossy().into_owned()),
            port: None,
        }
    }

    /// A PROXY header's origin as `client.address` and `client.port`, the address written as
    /// [`Self::from_socket`] writes one. A Unix origin's path is `client.address` alone, written
    /// lossily when it isn't UTF-8. [`Origin::None`] has nothing to stamp.
    pub fn client(origin: &Origin) -> Option<Self> {
        match origin {
            Origin::None => None,
            Origin::Ip(addr) => Some(Self::ip(client_keys(), *addr)),
            Origin::Unix(path) => Some(Self {
                keys: client_keys(),
                address: shared_str(String::from_utf8_lossy(path).into_owned()),
                port: None,
            }),
        }
    }

    /// Writes this address onto every event in `events`, replacing a same-named attribute.
    pub fn stamp(&self, events: &mut [Event]) {
        let (address_key, port_key) = self.keys;
        for event in events {
            event.attributes.insert_sym(address_key, self.address.clone());
            if let Some(port) = &self.port {
                event.attributes.insert_sym(port_key, port.clone());
            }
        }
    }
}

/// What the stream driver stamps on every event of one connection: the socket peer under
/// `peer: true`, and a PROXY header's origin under `proxy_protocol: true`. Built once per
/// connection.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionAttrs {
    peer: Option<PeerAttrs>,
    client: Option<PeerAttrs>,
}

impl ConnectionAttrs {
    /// `None` when there's nothing to stamp, so a connection with neither costs one branch per
    /// decoded frame.
    pub fn new(peer: Option<PeerAttrs>, client: Option<PeerAttrs>) -> Option<Self> {
        (peer.is_some() || client.is_some()).then_some(Self { peer, client })
    }

    /// Writes both onto every event in `events`, each replacing a same-named attribute.
    pub fn stamp(&self, events: &mut [Event]) {
        for attrs in [&self.peer, &self.client].into_iter().flatten() {
            attrs.stamp(events);
        }
    }
}

/// A datagram's sender as the datagram driver reads it off the socket, before any formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sender {
    Ip(SocketAddr),
    /// A Unix datagram sender's bound path, as raw bytes. A sender that bound none has no
    /// `Sender`.
    Unix(Bytes),
}

impl Sender {
    fn attrs(&self) -> PeerAttrs {
        match self {
            Self::Ip(addr) => PeerAttrs::from_socket(*addr),
            Self::Unix(path) => PeerAttrs::from_path(Path::new(std::ffi::OsStr::from_bytes(path))),
        }
    }
}

/// The last [`Sender`] seen and its [`PeerAttrs`]. Consecutive datagrams from one sender, the
/// common case for a listener with few senders, share one formatted address; a change of sender
/// formats the new one.
#[derive(Debug, Default)]
pub struct PeerCache {
    last: Option<(Sender, PeerAttrs)>,
}

impl PeerCache {
    /// The attributes for `sender`, formatted only when it differs from the previous call's.
    pub fn attrs_for(&mut self, sender: &Sender) -> &PeerAttrs {
        let cached = matches!(&self.last, Some((last, _)) if last == sender);
        if !cached {
            self.last = Some((sender.clone(), sender.attrs()));
        }
        let (_, attrs) = self.last.as_ref().expect("filled above");
        attrs
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

    /// Two senders interleaved each get their own address: the cache never hands one sender's
    /// attributes to another.
    #[test]
    fn the_cache_follows_every_change_of_sender() {
        let a = Sender::Ip("192.0.2.1:1000".parse().unwrap());
        let b = Sender::Ip("192.0.2.2:2000".parse().unwrap());
        let unix = Sender::Unix(Bytes::from_static(b"/run/client.sock"));
        let mut cache = PeerCache::default();
        for (sender, address, port) in [
            (&a, "192.0.2.1", Some(1000)),
            (&a, "192.0.2.1", Some(1000)),
            (&b, "192.0.2.2", Some(2000)),
            (&a, "192.0.2.1", Some(1000)),
            (&unix, "/run/client.sock", None),
            (&b, "192.0.2.2", Some(2000)),
        ] {
            let mut events = vec![event()];
            cache.attrs_for(sender).stamp(&mut events);
            assert_eq!(str_attr(&events[0], PEER_ADDRESS), Some(address));
            assert_eq!(events[0].attributes.get(PEER_PORT), port.map(Value::I64).as_ref());
        }
    }

    /// Only the same address and port are the same sender: one host's two sockets are two.
    #[test]
    fn the_same_host_on_another_port_is_another_sender() {
        let mut cache = PeerCache::default();
        let mut events = vec![event()];
        cache.attrs_for(&Sender::Ip("192.0.2.1:1000".parse().unwrap())).stamp(&mut events);
        cache.attrs_for(&Sender::Ip("192.0.2.1:1001".parse().unwrap())).stamp(&mut events);
        assert_eq!(events[0].attributes.get(PEER_PORT), Some(&Value::I64(1001)));
    }

    /// A repeated sender reuses the buffer its address was first formatted into.
    #[test]
    fn a_repeated_sender_reuses_its_formatted_address() {
        let sender = Sender::Ip("192.0.2.1:1000".parse().unwrap());
        let mut cache = PeerCache::default();
        let first = cache.attrs_for(&sender).clone();
        let again = cache.attrs_for(&sender);
        match (&first.address, &again.address) {
            (Value::Str(a), Value::Str(b)) => assert_eq!(a.as_ptr(), b.as_ptr()),
            other => panic!("expected two string addresses, got {other:?}"),
        }
    }

    #[test]
    fn a_proxy_origin_stamps_client_attributes_written_as_a_peer_is() {
        let mapped: IpAddr = Ipv4Addr::new(198, 51, 100, 4).to_ipv6_mapped().into();
        let client = PeerAttrs::client(&Origin::Ip(SocketAddr::new(mapped, 40000))).unwrap();
        let mut events = vec![event()];
        client.stamp(&mut events);
        assert_eq!(str_attr(&events[0], CLIENT_ADDRESS), Some("198.51.100.4"));
        assert_eq!(events[0].attributes.get(CLIENT_PORT), Some(&Value::I64(40000)));
        assert_eq!(events[0].attributes.get(PEER_ADDRESS), None);
    }

    #[test]
    fn a_unix_proxy_origin_stamps_its_path_lossily_and_no_port() {
        let client = PeerAttrs::client(&Origin::Unix(b"/run/a\xFF.sock".to_vec())).unwrap();
        let mut events = vec![event()];
        client.stamp(&mut events);
        assert_eq!(str_attr(&events[0], CLIENT_ADDRESS), Some("/run/a\u{FFFD}.sock"));
        assert_eq!(events[0].attributes.get(CLIENT_PORT), None);
    }

    #[test]
    fn no_proxy_origin_has_nothing_to_stamp() {
        assert_eq!(PeerAttrs::client(&Origin::None), None);
        assert_eq!(ConnectionAttrs::new(None, None), None);
    }

    /// The proxy is the peer and the origin is the client, both on one event, and each replaces
    /// what a decoder wrote under its name.
    #[test]
    fn a_connection_stamps_its_peer_and_its_client_side_by_side() {
        let peer = PeerAttrs::from_socket("192.0.2.1:5000".parse().unwrap());
        let client = PeerAttrs::client(&Origin::Ip("203.0.113.9:6000".parse().unwrap()));
        let attrs = ConnectionAttrs::new(Some(peer), client).unwrap();
        let mut decoded = event();
        decoded.attributes.insert(CLIENT_ADDRESS, Value::str("forged"));
        let mut events = vec![decoded];
        attrs.stamp(&mut events);
        assert_eq!(str_attr(&events[0], PEER_ADDRESS), Some("192.0.2.1"));
        assert_eq!(str_attr(&events[0], CLIENT_ADDRESS), Some("203.0.113.9"));
        assert_eq!(events[0].attributes.get(CLIENT_PORT), Some(&Value::I64(6000)));
        assert_eq!(events[0].attributes.len(), 4);
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
