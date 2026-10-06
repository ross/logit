//! The sender's address on listener events: the `network.peer.address` and `network.peer.port`
//! attributes a shared driver stamps, after decode, on a listener configured `peer: true`, and the
//! `client.address` and `client.port` attributes stamped from a PROXY header's origin under
//! `proxy_protocol: true` or a forwarding header's client under `forwarded:`.
//!
//! ADR `listener-peer-address` settles the names, the text form, why these are event attributes
//! and not resource ones, and why the driver's value replaces a same-named attribute a decoder
//! produced. A driver builds one [`PeerAttrs`] per peer and calls [`PeerAttrs::stamp`] on every
//! event decoded from it. The stream driver builds one per connection. The datagram driver reads a
//! [`Sender`] with every datagram and goes through a [`PeerCache`], which formats an address only
//! when the sender differs from the previous datagram's. A PROXY header's origin becomes a
//! [`PeerAttrs`] through [`PeerAttrs::client`], and [`ConnectionAttrs`] holds a connection's two.
//!
//! The PROXY header is read here too, by [`read_proxy_origin`], because reading it and stamping its
//! origin are the two halves of `proxy_protocol:`, and the stream driver and the HTTP listeners'
//! accept loops both call it on a raw [`TcpStream`]. An HTTP listener builds a [`ConnectionPeer`]
//! per connection: the sender its diagnostics name, and the attributes
//! [`ConnectionAttrs::stamp_batches`] writes on every batch a request decodes into.
//!
//! Under `forwarded:`, [`ConnectionPeer::request`] reads one request's forwarding header, with
//! `logit_proto::forwarded`, into a [`RequestPeer`] whose `client.*` replaces the connection's
//! PROXY-derived pair for that request. It reads the request's headers, so a handler calls it
//! before `into_body()` consumes the request. ADR `forwarded-header-parsing`'s "Precedence"
//! section has the rules.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{Diagnostics, Event, EventBatch, Symbol, Value};
use logit_proto::forwarded::{self, ForwardedHeader};
use logit_proto::proxy::{self, Origin, Parse};
use std::fmt;
use std::net::SocketAddr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// The attribute holding the immediate socket peer's address, in OpenTelemetry's name.
pub const PEER_ADDRESS: &str = "network.peer.address";
/// The attribute holding the immediate socket peer's port, in OpenTelemetry's name.
pub const PEER_PORT: &str = "network.peer.port";

/// The attribute holding a PROXY header's origin or a forwarding header's client, in
/// OpenTelemetry's name.
pub const CLIENT_ADDRESS: &str = "client.address";
/// The attribute holding the port of a PROXY header's origin or a forwarding header's client.
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
/// the socket peer's under `network.peer.*`, or a PROXY header's origin or a forwarding header's
/// client under `client.*`.
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

    /// A forwarding header's client as `client.address` and `client.port`, the address written
    /// as [`Self::from_socket`] writes one. A header that names no port has no `client.port`.
    fn forwarded_client(client: forwarded::Client) -> Self {
        Self {
            keys: client_keys(),
            address: shared_str(client.address.to_canonical().to_string()),
            port: client.port.map(|port| Value::I64(i64::from(port))),
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

/// What a stream or HTTP listener stamps on every event of one connection: the socket peer under
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

    /// What a TCP connection stamps: `peer` as `network.peer.*` when `record_peer` is on, and a
    /// PROXY header's `origin` as `client.*`. `peer` is `None` for a listener whose socket has no
    /// `SocketAddr`. `None` when there's nothing to stamp, as for [`Self::new`].
    pub fn for_connection(
        peer: Option<SocketAddr>,
        record_peer: bool,
        origin: Option<&Origin>,
    ) -> Option<Self> {
        let peer = peer.filter(|_| record_peer).map(PeerAttrs::from_socket);
        Self::new(peer, origin.and_then(PeerAttrs::client))
    }

    /// Writes both onto every event in `events`, each replacing a same-named attribute.
    pub fn stamp(&self, events: &mut [Event]) {
        for attrs in [&self.peer, &self.client].into_iter().flatten() {
            attrs.stamp(events);
        }
    }

    /// [`Self::stamp`] on every event of every batch, for a request that decodes into several.
    pub fn stamp_batches(&self, batches: &mut [EventBatch]) {
        for batch in batches {
            self.stamp(&mut batch.events);
        }
    }
}

/// One HTTP connection's sender, built once per connection: the socket peer its diagnostics name,
/// stamped or not, the [`ConnectionAttrs`] its requests' events carry, and the forwarding header
/// each request is read for under `forwarded:`.
#[derive(Debug, Clone)]
pub struct ConnectionPeer {
    socket: SocketPeer,
    attrs: Option<ConnectionAttrs>,
    /// Behind an `Arc` because some listeners clone the peer per request, and the off path
    /// clones a `None`.
    forwarded: Option<Arc<Forwarding>>,
}

/// `forwarded:`'s header, and the diagnostics an unusable one is counted on.
#[derive(Debug)]
struct Forwarding {
    header: ForwardedHeader,
    diag: Diagnostics,
}

/// The socket a connection came from, as diagnostic text names it.
#[derive(Debug, Clone)]
enum SocketPeer {
    Tcp(SocketAddr),
    /// The listener's own path: an unbound Unix client, the usual case, has no address to name.
    Unix(Arc<Path>),
}

impl ConnectionPeer {
    /// A TCP connection from `peer`, stamped as [`ConnectionAttrs::for_connection`] describes.
    pub fn tcp(peer: SocketAddr, record_peer: bool, origin: Option<&Origin>) -> Self {
        Self {
            socket: SocketPeer::Tcp(peer),
            attrs: ConnectionAttrs::for_connection(Some(peer), record_peer, origin),
            forwarded: None,
        }
    }

    /// A connection accepted on the Unix socket at `listener`, stamped with `peer` when it's set.
    /// Diagnostics name the listener's path, written `unix:<path>`.
    pub fn unix(listener: Arc<Path>, peer: Option<PeerAttrs>) -> Self {
        Self {
            socket: SocketPeer::Unix(listener),
            attrs: ConnectionAttrs::new(peer, None),
            forwarded: None,
        }
    }

    /// Reads `header` on each of this connection's requests (`forwarded:` in config), counting
    /// an unusable one as the throttled `forwarded` diagnostic on `diag`. `None` reads nothing.
    pub fn with_forwarded(mut self, header: Option<ForwardedHeader>, diag: &Diagnostics) -> Self {
        self.forwarded = header.map(|header| Arc::new(Forwarding { header, diag: diag.clone() }));
        self
    }

    /// What one request's events carry. Without `forwarded:`, this connection's attributes.
    /// With it, the first instance of the named header in `headers` is parsed: a client it
    /// names replaces the connection's `client.*` for this request, and an unusable value
    /// leaves them standing and counts the `forwarded` diagnostic. An absent header leaves them
    /// standing and counts nothing.
    pub fn request(&self, headers: &http::HeaderMap) -> RequestPeer<'_> {
        let connection = self.attrs.as_ref();
        let Some(forwarding) = &self.forwarded else {
            return RequestPeer { connection, client: None };
        };
        // `get` returns the first instance of a repeated header.
        let Some(value) = headers.get(forwarding.header.name()) else {
            return RequestPeer { connection, client: None };
        };
        match forwarded::parse(forwarding.header, value.as_bytes()) {
            Ok(client) => {
                RequestPeer { connection, client: Some(PeerAttrs::forwarded_client(client)) }
            }
            Err(reason) => {
                // The value itself is never written: it's whatever the sender put there.
                forwarding.diag.clone().warn_throttled(
                    "forwarded",
                    format_args!(
                        "the {} header from {self} names no client: {reason}; the request keeps \
                         its connection's client.address and client.port",
                        forwarding.header.name()
                    ),
                );
                RequestPeer { connection, client: None }
            }
        }
    }

    /// The attributes this connection's events carry, or `None` when it stamps nothing.
    pub fn attrs(&self) -> Option<&ConnectionAttrs> {
        self.attrs.as_ref()
    }

    /// [`ConnectionAttrs::stamp_batches`] when there's anything to stamp, one branch when not.
    pub fn stamp_batches(&self, batches: &mut [EventBatch]) {
        if let Some(attrs) = &self.attrs {
            attrs.stamp_batches(batches);
        }
    }
}

/// What one request's events carry, from [`ConnectionPeer::request`].
#[derive(Debug)]
pub struct RequestPeer<'a> {
    connection: Option<&'a ConnectionAttrs>,
    /// A forwarding header's client, which replaces `connection`'s `client.*` as a pair.
    client: Option<PeerAttrs>,
}

impl RequestPeer<'_> {
    /// Stamps every event of every batch, each attribute replacing a same-named one. A forwarded
    /// client takes the place of the connection's PROXY-derived `client.*`, which isn't stamped,
    /// and one with no port removes a decoded `client.port`, so the address never sits beside
    /// another source's port.
    pub fn stamp_batches(&self, batches: &mut [EventBatch]) {
        let Some(client) = &self.client else {
            if let Some(attrs) = self.connection {
                attrs.stamp_batches(batches);
            }
            return;
        };
        let peer = self.connection.and_then(|attrs| attrs.peer.as_ref());
        let (_, port_key) = client.keys;
        for batch in batches {
            if let Some(peer) = peer {
                peer.stamp(&mut batch.events);
            }
            client.stamp(&mut batch.events);
            if client.port.is_none() {
                for event in &mut batch.events {
                    event.attributes.remove_sym(port_key);
                }
            }
        }
    }
}

/// The socket peer as `SocketAddr` writes it (`192.0.2.1:5000`, `[2001:db8::1]:443`), or
/// `unix:<listener path>`, whether or not `peer:` stamps it.
impl fmt::Display for ConnectionPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.socket {
            SocketPeer::Tcp(addr) => write!(f, "{addr}"),
            SocketPeer::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

/// Reads one PROXY protocol header off `stream` under `handshake_timeout` and returns the origin
/// it names, leaving every byte after the header in the socket, as [`read_proxy_header`] does.
pub(crate) async fn read_proxy_origin(
    stream: &mut TcpStream,
    handshake_timeout: Duration,
) -> anyhow::Result<Origin> {
    tokio::time::timeout(handshake_timeout, read_proxy_header(stream)).await.map_err(
        |_elapsed| anyhow::anyhow!("no complete PROXY header within {handshake_timeout:?}"),
    )?
}

/// Reads one PROXY protocol header off `stream` and nothing after it, and returns the origin it
/// names. [`read_proxy_origin`] bounds it with the listener's `handshake_timeout`.
///
/// A version 1 header's length is known only at its CRLF, so each step peeks at what the socket
/// holds and consumes only the bytes [`proxy::parse`] places in the header: all of them while it
/// answers [`Parse::Incomplete`], and the header's own length once it answers [`Parse::Complete`].
/// The payload after the header stays in the socket for whatever reads the connection next: a
/// TLS handshake, the stream driver's framer, or hyper. A version 2 header's length is known from
/// its 16th byte, and the rest is read in one go.
pub(crate) async fn read_proxy_header(stream: &mut TcpStream) -> anyhow::Result<Origin> {
    let mut header = Vec::with_capacity(proxy::V1_MAX_LEN);
    let mut chunk = [0u8; proxy::V1_MAX_LEN];
    loop {
        // Every byte peeked is consumed below unless the header completes, so the next peek waits
        // for new bytes rather than returning the same ones again.
        let peeked = stream.peek(&mut chunk).await?;
        if peeked == 0 {
            anyhow::bail!("the peer closed the connection before a complete PROXY header");
        }
        let held = header.len();
        header.extend_from_slice(&chunk[..peeked]);
        match proxy::parse(&header)? {
            Parse::Complete { origin, len } => {
                stream.read_exact(&mut chunk[..len - held]).await?;
                return Ok(origin);
            }
            Parse::Incomplete { len: Some(len) } => {
                stream.read_exact(&mut chunk[..peeked]).await?;
                header.resize(len, 0);
                stream.read_exact(&mut header[held + peeked..]).await?;
                return match proxy::parse(&header)? {
                    Parse::Complete { origin, .. } => Ok(origin),
                    Parse::Incomplete { .. } => unreachable!("the header holds its whole length"),
                };
            }
            Parse::Incomplete { len: None } => {
                stream.read_exact(&mut chunk[..peeked]).await?;
            }
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

    fn batch(events: usize) -> EventBatch {
        EventBatch {
            resource: Arc::new(logit_core::Resource::default()),
            scope: None,
            events: (0..events).map(|_| event()).collect(),
        }
    }

    /// A request that decodes into several batches carries its sender on every event of each,
    /// and an empty batch among them is no obstacle.
    #[test]
    fn stamp_batches_stamps_every_event_of_every_batch() {
        let attrs = ConnectionAttrs::for_connection(
            Some("192.0.2.1:5000".parse().unwrap()),
            true,
            Some(&Origin::Ip("203.0.113.9:6000".parse().unwrap())),
        )
        .unwrap();
        let mut batches = vec![batch(2), batch(0), batch(1)];
        attrs.stamp_batches(&mut batches);
        assert_eq!(batches.iter().map(|b| b.events.len()).collect::<Vec<_>>(), [2, 0, 1]);
        for event in batches.iter().flat_map(|b| &b.events) {
            assert_eq!(str_attr(event, PEER_ADDRESS), Some("192.0.2.1"));
            assert_eq!(event.attributes.get(PEER_PORT), Some(&Value::I64(5000)));
            assert_eq!(str_attr(event, CLIENT_ADDRESS), Some("203.0.113.9"));
            assert_eq!(event.attributes.get(CLIENT_PORT), Some(&Value::I64(6000)));
        }
    }

    /// Every combination of `peer:` and a PROXY origin: the socket peer only under
    /// `record_peer`, the origin's `client.*` whenever it names one, and `None` with neither.
    #[test]
    fn for_connection_builds_what_each_option_asks_for() {
        type Client = (Option<&'static str>, Option<i64>);
        type Case<'a> =
            (Option<SocketAddr>, bool, Option<&'a Origin>, Option<(Option<&'a str>, Client)>);
        let socket: SocketAddr = "192.0.2.1:5000".parse().unwrap();
        let ip = Origin::Ip("203.0.113.9:6000".parse().unwrap());
        let unix = Origin::Unix(b"/run/origin.sock".to_vec());
        let peer = Some("192.0.2.1");
        let ip_client = (Some("203.0.113.9"), Some(6000));
        let unix_client = (Some("/run/origin.sock"), None);
        let no_client = (None, None);
        let cases: [Case<'_>; 10] = [
            (Some(socket), false, None, None),
            (Some(socket), false, Some(&Origin::None), None),
            (Some(socket), false, Some(&ip), Some((None, ip_client))),
            (Some(socket), false, Some(&unix), Some((None, unix_client))),
            (Some(socket), true, None, Some((peer, no_client))),
            (Some(socket), true, Some(&Origin::None), Some((peer, no_client))),
            (Some(socket), true, Some(&ip), Some((peer, ip_client))),
            (Some(socket), true, Some(&unix), Some((peer, unix_client))),
            (None, true, None, None),
            (None, true, Some(&ip), Some((None, ip_client))),
        ];
        for (socket, record_peer, origin, expect) in cases {
            let case = format!("socket {socket:?}, record_peer {record_peer}, origin {origin:?}");
            let attrs = ConnectionAttrs::for_connection(socket, record_peer, origin);
            let Some((peer_address, (client_address, client_port))) = expect else {
                assert_eq!(attrs, None, "{case}");
                continue;
            };
            let mut events = vec![event()];
            attrs.expect(&case).stamp(&mut events);
            let stamped = &events[0];
            assert_eq!(str_attr(stamped, PEER_ADDRESS), peer_address, "{case}");
            let peer_port = peer_address.map(|_| Value::I64(5000));
            assert_eq!(stamped.attributes.get(PEER_PORT), peer_port.as_ref(), "{case}");
            assert_eq!(str_attr(stamped, CLIENT_ADDRESS), client_address, "{case}");
            let client_port = client_port.map(Value::I64);
            assert_eq!(stamped.attributes.get(CLIENT_PORT), client_port.as_ref(), "{case}");
        }
    }

    /// Diagnostics name the socket peer whether or not `peer:` stamps it.
    #[test]
    fn a_connection_peer_names_its_socket_with_or_without_a_stamp() {
        let v6: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        let quiet = ConnectionPeer::tcp(v6, false, None);
        assert_eq!(quiet.to_string(), "[2001:db8::1]:443");
        assert_eq!(quiet.attrs(), None);
        let mut batches = vec![batch(1)];
        quiet.stamp_batches(&mut batches);
        assert_eq!(batches[0].events[0].attributes.len(), 0);

        let stamped = ConnectionPeer::tcp("192.0.2.1:5000".parse().unwrap(), true, None);
        assert_eq!(stamped.to_string(), "192.0.2.1:5000");
        stamped.stamp_batches(&mut batches);
        assert_eq!(str_attr(&batches[0].events[0], PEER_ADDRESS), Some("192.0.2.1"));

        let unix = ConnectionPeer::unix(Arc::from(Path::new("/run/trace.sock")), None);
        assert_eq!(unix.to_string(), "unix:/run/trace.sock");
        assert_eq!(unix.attrs(), None);
    }

    // ---- `forwarded:` --------------------------------------------------------------------------

    /// A connection behind a PROXY header naming `198.51.100.7:40000`, with `peer:` on, reading
    /// `header` on its requests, and the diagnostics it counts on.
    fn forwarding_peer(header: Option<ForwardedHeader>) -> (ConnectionPeer, Diagnostics) {
        let diag = Diagnostics::new("http");
        let origin = Origin::Ip("198.51.100.7:40000".parse().unwrap());
        let peer = ConnectionPeer::tcp("192.0.2.1:5000".parse().unwrap(), true, Some(&origin))
            .with_forwarded(header, &diag);
        (peer, diag)
    }

    fn headers(pairs: &[(&'static str, &'static str)]) -> http::HeaderMap {
        let mut map = http::HeaderMap::new();
        for (name, value) in pairs {
            map.append(*name, http::HeaderValue::from_static(value));
        }
        map
    }

    /// Stamps one request's two batches and returns their events' `client.*` and
    /// `network.peer.address`, asserting every event agrees.
    fn stamped(request: &RequestPeer<'_>) -> (Option<String>, Option<Value>, Option<String>) {
        let mut decoded = event();
        decoded.attributes.insert(CLIENT_PORT, Value::I64(1));
        let mut batches = vec![batch(1), batch(0), batch(1)];
        batches[0].events[0] = decoded;
        request.stamp_batches(&mut batches);
        let mut seen = batches.iter().flat_map(|b| &b.events).map(|event| {
            (
                str_attr(event, CLIENT_ADDRESS).map(str::to_owned),
                event.attributes.get(CLIENT_PORT).cloned(),
                str_attr(event, PEER_ADDRESS).map(str::to_owned),
            )
        });
        let first = seen.next().expect("two stamped events");
        assert_eq!(seen.next().as_ref(), Some(&first), "every event carries the same values");
        first
    }

    #[test]
    fn a_forwarded_header_with_a_port_replaces_both_client_attributes() {
        let (peer, diag) = forwarding_peer(Some(ForwardedHeader::Forwarded));
        let request = peer.request(&headers(&[("forwarded", r#"for="[2001:db8::1]:443""#)]));
        let (address, port, socket) = stamped(&request);
        assert_eq!(address.as_deref(), Some("2001:db8::1"));
        assert_eq!(port, Some(Value::I64(443)));
        assert_eq!(socket.as_deref(), Some("192.0.2.1"), "network.peer.* stays the socket peer");
        assert_eq!(diag.occurrences("forwarded"), 0);
    }

    /// The pair rule: an address with no port never sits beside the PROXY origin's port, or one
    /// the decoder wrote.
    #[test]
    fn a_header_without_a_port_removes_the_proxy_origins_port() {
        let (peer, _diag) = forwarding_peer(Some(ForwardedHeader::XForwardedFor));
        let request = peer.request(&headers(&[("x-forwarded-for", "203.0.113.9, 10.0.0.1")]));
        let (address, port, socket) = stamped(&request);
        assert_eq!(address.as_deref(), Some("203.0.113.9"));
        assert_eq!(port, None);
        assert_eq!(socket.as_deref(), Some("192.0.2.1"));
    }

    #[test]
    fn an_unusable_header_leaves_the_proxy_origin_and_is_diagnosed() {
        let (peer, diag) = forwarding_peer(Some(ForwardedHeader::XRealIp));
        let request = peer.request(&headers(&[("x-real-ip", "unknown")]));
        let (address, port, _) = stamped(&request);
        assert_eq!(address.as_deref(), Some("198.51.100.7"));
        assert_eq!(port, Some(Value::I64(40000)));
        assert_eq!(diag.occurrences("forwarded"), 1);
    }

    #[test]
    fn an_absent_header_leaves_the_proxy_origin_and_is_not_diagnosed() {
        let (peer, diag) = forwarding_peer(Some(ForwardedHeader::XForwardedFor));
        let request = peer.request(&headers(&[("x-real-ip", "203.0.113.9")]));
        let (address, port, _) = stamped(&request);
        assert_eq!(address.as_deref(), Some("198.51.100.7"));
        assert_eq!(port, Some(Value::I64(40000)));
        assert_eq!(diag.occurrences("forwarded"), 0);
    }

    /// With `forwarded:` unset, no header is read, whatever the request carries.
    #[test]
    fn no_configured_header_reads_nothing() {
        let (peer, diag) = forwarding_peer(None);
        let request = peer.request(&headers(&[
            ("x-forwarded-for", "203.0.113.9"),
            ("forwarded", "for=203.0.113.9"),
            ("x-real-ip", "unknown"),
        ]));
        let (address, port, _) = stamped(&request);
        assert_eq!(address.as_deref(), Some("198.51.100.7"));
        assert_eq!(port, Some(Value::I64(40000)));
        assert_eq!(diag.occurrences("forwarded"), 0);
    }

    #[test]
    fn a_repeated_header_is_read_from_its_first_instance() {
        let (peer, diag) = forwarding_peer(Some(ForwardedHeader::XForwardedFor));
        let request = peer.request(&headers(&[
            ("x-forwarded-for", "203.0.113.9"),
            ("x-forwarded-for", "198.51.100.200"),
        ]));
        assert_eq!(stamped(&request).0.as_deref(), Some("203.0.113.9"));

        let request = peer.request(&headers(&[
            ("x-forwarded-for", "unknown"),
            ("x-forwarded-for", "198.51.100.200"),
        ]));
        assert_eq!(stamped(&request).0.as_deref(), Some("198.51.100.7"), "the second is ignored");
        assert_eq!(diag.occurrences("forwarded"), 1);
    }

    /// A connection with nothing of its own to stamp still stamps a forwarded client, and an
    /// IPv4-mapped address is written as IPv4, as a socket peer is.
    #[test]
    fn a_forwarded_client_is_stamped_without_peer_or_proxy_protocol() {
        let diag = Diagnostics::new("http");
        let peer = ConnectionPeer::tcp("192.0.2.1:5000".parse().unwrap(), false, None)
            .with_forwarded(Some(ForwardedHeader::XRealIp), &diag);
        assert_eq!(peer.attrs(), None);
        let request = peer.request(&headers(&[("x-real-ip", "::ffff:203.0.113.9")]));
        let (address, port, socket) = stamped(&request);
        assert_eq!(address.as_deref(), Some("203.0.113.9"));
        assert_eq!(port, None);
        assert_eq!(socket, None);
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
