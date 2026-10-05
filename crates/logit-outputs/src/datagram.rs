//! The datagram send path the four UDP sinks and `statsd_out`'s `transport: unix` share: one
//! packer, one destination type, one `EMSGSIZE` test, and the address-family choice (ADR
//! `sink-send-path-and-attempt-accounting`, decision 9).
//!
//! [`send_datagrams`] walks a sink's [`MessageBuf`] once. Under [`Framing::Packed`] (`statsd_out`,
//! `graphite_out`) it joins entries with `\n` into datagrams of at most `cap` bytes, never
//! splitting an entry, with no leading or trailing `\n`. Under [`Framing::OnePerEntry`]
//! (`syslog_out`, `collectd_out`, whose encoders chose every boundary) each entry is one datagram.
//! A datagram has no application response, so the outcome of each `send` decides, for
//! `syslog_out`, `statsd_out`, `graphite_out`, and `collectd_out` over UDP or a Unix datagram
//! socket alike. The Evidence column names the test that pins each row.
//!
//! | Response | Class | Why | Evidence |
//! |---|---|---|---|
//! | sent | its entries and their weight count as sent | -- | `one_per_entry_never_packs` |
//! | `EMSGSIZE` ([`is_message_too_large`]) | no fault: its weight counts `logit.output.messages.dropped{reason="oversize_datagram"}` with a throttled diagnostic, and the batch goes on | a per-datagram data condition the kernel decided, `Rejected` for that datagram alone | `only_emsgsize_is_a_message_too_large`, `the_reconnect_once_rule_survives_an_emsgsize_dropped_first_datagram` |
//! | an entry over `cap` (the packer's backstop) | no fault: dropped and counted as `EMSGSIZE` is, its neighbours sent | the same, decided before the kernel | `an_entry_over_the_cap_is_dropped_and_counted_and_its_neighbours_are_sent` |
//! | DNS failure, or an endpoint that resolves to no address | `Clean` | nothing of the batch was sent | `an_endpoint_that_does_not_resolve_is_clean` |
//! | any other send error, a Unix send past `send_timeout` included, before any datagram of the batch was sent | `Clean` | nothing of the batch was sent | `emsgsize_then_a_failure_with_nothing_sent_is_clean`, `a_timed_out_unix_send_drops_the_socket_and_the_next_send_reconnects` |
//! | the same after one was | `Ambiguous` | the receiver may hold part of the batch | `sent_then_emsgsize_then_a_failure_is_ambiguous`, `a_failure_after_two_datagrams_returns_what_the_two_carried` |
//!
//! A datagram the network loses after `send` returned is invisible here: UDP reports nothing back.
//!
//! An entry longer than `cap` never reaches the kernel. Every encoder caps its entries at the
//! value its sink passes here, so the branch that drops one, counted like `EMSGSIZE`, is a
//! backstop that costs one comparison per entry. It counts on every attempt that reaches it, as an
//! `EMSGSIZE` does, so a retried batch can count it again but never loses it: a backstop's count
//! doesn't depend on which attempt reached it.
//!
//! [`Sent`] comes back on every exit, so a sink counts what reached the wire before an error as
//! well as on success. A cancelled send returns nothing, and its counts are lost
//! (`docs/known-gaps/intake.md`).
//!
//! A UDP sink resolves its endpoint once per batch ([`UdpDest::resolve`]) and sends to the first
//! IPv4 address in the answer, else the first IPv6 one ([`pick_addr`]), over an IPv4 socket bound
//! at construction or an IPv6 one bound on first use.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use logit_core::{redact, Diagnostics, Telemetry};
use logit_pipeline::Fault;
use logit_proto::MessageBuf;
use tokio::net::{lookup_host, UdpSocket, UnixDatagram};

#[cfg(test)]
use crate::test_support::ScriptedDest;

/// Linux's `EMSGSIZE`. macOS and the BSDs use 40; `logit` builds and runs in Linux containers.
const EMSGSIZE: i32 = 90;

/// Whether `err` is the kernel refusing one datagram as too large to send. Only `EMSGSIZE`:
/// std's `InvalidInput` for a Unix path too long for `sockaddr_un` or holding a NUL, and the
/// kernel's `EINVAL` for a UDP port of 0, fail every datagram alike and are faults.
pub(crate) fn is_message_too_large(err: &io::Error) -> bool {
    err.raw_os_error() == Some(EMSGSIZE)
}

/// How [`send_datagrams`] turns entries into datagrams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Framing {
    /// Entries joined by `\n` into datagrams of at most `cap` bytes.
    Packed,
    /// One datagram per entry.
    OnePerEntry,
}

/// What one [`send_datagrams`] call handed the kernel.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Sent {
    /// Entries in datagrams the kernel took.
    pub(crate) entries: usize,
    /// The summed weight of those entries, in the sink's own unit.
    pub(crate) weight: usize,
    pub(crate) datagrams: usize,
}

/// A sink's encoded batch and how to send it.
pub(crate) struct Datagrams<'a, M> {
    pub(crate) entries: &'a MessageBuf<M>,
    /// An entry's weight: its datapoints for `graphite_out`, its value lists for `collectd_out`,
    /// and `1` for the sinks that count entries.
    pub(crate) weight: fn(&M) -> usize,
    /// The largest datagram, in bytes.
    pub(crate) cap: usize,
    pub(crate) framing: Framing,
}

/// Where a send's drops are reported, through the sink's ungated handles: both of its drops count
/// per attempt.
pub(crate) struct Report<'a> {
    pub(crate) sink: &'static str,
    pub(crate) diag: &'a mut Diagnostics,
    pub(crate) telemetry: &'a Telemetry,
}

impl Report<'_> {
    /// Counts `weight` dropped as `oversize_datagram`, with the throttled diagnostic.
    fn oversize(&mut self, weight: usize, what: std::fmt::Arguments<'_>) {
        self.telemetry.count(
            "logit.output.messages.dropped",
            weight as f64,
            &[("reason", "oversize_datagram")],
        );
        self.diag
            .warn_throttled("oversize_datagram", format_args!("{}: {what}; dropped", self.sink));
    }
}

/// Sends `batch` through `dest` as the module doc describes, building each packed datagram in
/// `packet_buf`, which it clears first. Returns what was sent whether or not the batch failed.
pub(crate) async fn send_datagrams<M>(
    batch: Datagrams<'_, M>,
    dest: &mut DatagramDest<'_>,
    packet_buf: &mut Vec<u8>,
    report: &mut Report<'_>,
) -> (Sent, anyhow::Result<()>) {
    let mut sent = Sent::default();
    let (mut open_entries, mut open_weight) = (0, 0);
    packet_buf.clear();
    for (entry, meta) in batch.entries.iter_with() {
        let weight = (batch.weight)(meta);
        if entry.len() > batch.cap {
            report.oversize(
                weight,
                format_args!(
                    "a {}-byte entry exceeds the {}-byte datagram cap",
                    entry.len(),
                    batch.cap
                ),
            );
            continue;
        }
        if batch.framing == Framing::OnePerEntry {
            if let Err(err) = send_one(dest, entry, 1, weight, &mut sent, report).await {
                return (sent, Err(err));
            }
            continue;
        }
        if !packet_buf.is_empty() && packet_buf.len() + 1 + entry.len() > batch.cap {
            let result =
                send_one(dest, packet_buf, open_entries, open_weight, &mut sent, report).await;
            packet_buf.clear();
            (open_entries, open_weight) = (0, 0);
            if let Err(err) = result {
                return (sent, Err(err));
            }
        }
        if !packet_buf.is_empty() {
            packet_buf.push(b'\n');
        }
        packet_buf.extend_from_slice(entry);
        open_entries += 1;
        open_weight += weight;
    }
    if !packet_buf.is_empty() {
        let result = send_one(dest, packet_buf, open_entries, open_weight, &mut sent, report).await;
        packet_buf.clear();
        if let Err(err) = result {
            return (sent, Err(err));
        }
    }
    (sent, Ok(()))
}

/// Sends one datagram of `entries` entries and `weight` weight, and applies its outcome to `sent`.
async fn send_one(
    dest: &mut DatagramDest<'_>,
    datagram: &[u8],
    entries: usize,
    weight: usize,
    sent: &mut Sent,
    report: &mut Report<'_>,
) -> anyhow::Result<()> {
    match dest.send(datagram, entries, sent.datagrams == 0).await {
        Ok(_) => {
            sent.entries += entries;
            sent.weight += weight;
            sent.datagrams += 1;
            Ok(())
        }
        Err(err) if is_message_too_large(&err) => {
            report.oversize(
                weight,
                format_args!("a {}-byte datagram was too large to send: {err}", datagram.len()),
            );
            Ok(())
        }
        Err(err) => {
            let fault = if sent.datagrams > 0 { Fault::Ambiguous } else { Fault::Clean };
            Err(anyhow::Error::new(err)
                .context(format!("{}: sending a datagram", report.sink))
                .context(fault))
        }
    }
}

/// The address a UDP sink sends to from a resolved endpoint: the first IPv4 one, else the first
/// IPv6 one. Taking the first of either would turn a loud failure into silent loss where a name
/// such as `localhost` resolves to `::1` first and the receiver listens on `127.0.0.1` only.
pub(crate) fn pick_addr(addrs: impl IntoIterator<Item = SocketAddr>) -> Option<SocketAddr> {
    let mut first_v6 = None;
    for addr in addrs {
        if addr.is_ipv4() {
            return Some(addr);
        }
        first_v6.get_or_insert(addr);
    }
    first_v6
}

/// A UDP sink's sockets: IPv4 bound at construction, so a bad local bind fails startup, and IPv6
/// bound by the first batch whose endpoint resolves only to IPv6 addresses.
pub(crate) enum UdpDest {
    Sockets {
        v4: UdpSocket,
        v6: Option<UdpSocket>,
    },
    /// Datagrams a test scripts and records.
    #[cfg(test)]
    Scripted(Arc<ScriptedDest>),
}

impl UdpDest {
    /// Binds the IPv4 socket.
    pub(crate) fn bind(sink: &str) -> anyhow::Result<Self> {
        Ok(Self::Sockets { v4: bind_udp("0.0.0.0:0", sink, "IPv4")?, v6: None })
    }

    /// Resolves `endpoint` once for the batch ([`pick_addr`]) and returns the destination on the
    /// socket of its family. Every failure is `Fault::Clean`, since nothing of the batch was sent.
    pub(crate) async fn resolve(
        &mut self,
        sink: &'static str,
        endpoint: &str,
    ) -> anyhow::Result<DatagramDest<'_>> {
        match self {
            UdpDest::Sockets { v4, v6 } => {
                let addrs = lookup_host(endpoint)
                    .await
                    .with_context(|| format!("resolving {sink} endpoint {}", redact::url(endpoint)))
                    .context(Fault::Clean)?;
                let addr = pick_addr(addrs)
                    .with_context(|| {
                        format!(
                            "{sink} endpoint {} resolved to no addresses",
                            redact::url(endpoint)
                        )
                    })
                    .context(Fault::Clean)?;
                let socket = socket_for(addr, v4, v6, sink)?;
                Ok(DatagramDest::Udp { socket, addr })
            }
            #[cfg(test)]
            UdpDest::Scripted(script) => Ok(DatagramDest::Scripted(script)),
        }
    }

    /// Resolves `endpoint` and sends `batch` ([`send_datagrams`]); a resolution failure sends
    /// nothing.
    pub(crate) async fn send<M>(
        &mut self,
        endpoint: &str,
        batch: Datagrams<'_, M>,
        packet_buf: &mut Vec<u8>,
        report: &mut Report<'_>,
    ) -> (Sent, anyhow::Result<()>) {
        match self.resolve(report.sink, endpoint).await {
            Ok(mut dest) => send_datagrams(batch, &mut dest, packet_buf, report).await,
            Err(err) => (Sent::default(), Err(err)),
        }
    }
}

/// The socket of `addr`'s family: `v4`, or `v6`, bound here on first use. A failed IPv6 bind is
/// `Fault::Clean`.
fn socket_for<'a>(
    addr: SocketAddr,
    v4: &'a UdpSocket,
    v6: &'a mut Option<UdpSocket>,
    sink: &str,
) -> anyhow::Result<&'a UdpSocket> {
    if addr.is_ipv4() {
        return Ok(v4);
    }
    match v6 {
        Some(socket) => Ok(socket),
        slot @ None => Ok(slot.insert(bind_udp("[::]:0", sink, "IPv6").context(Fault::Clean)?)),
    }
}

fn bind_udp(addr: &str, sink: &str, family: &str) -> anyhow::Result<UdpSocket> {
    let socket = std::net::UdpSocket::bind(addr)
        .with_context(|| format!("binding {sink}'s local {family} UDP socket"))?;
    socket
        .set_nonblocking(true)
        .with_context(|| format!("configuring {sink}'s local {family} UDP socket"))?;
    UdpSocket::from_std(socket)
        .with_context(|| format!("registering {sink}'s local {family} UDP socket"))
}

/// Where [`send_datagrams`] sends each datagram.
pub(crate) enum DatagramDest<'a> {
    Udp {
        socket: &'a UdpSocket,
        addr: SocketAddr,
    },
    Unix(UnixDest<'a>),
    #[cfg(test)]
    Scripted(&'a ScriptedDest),
}

impl DatagramDest<'_> {
    /// Sends one datagram holding `entries` entries; `first_of_batch` is whether nothing of this
    /// batch has been sent yet.
    #[cfg_attr(not(test), allow(unused_variables))]
    async fn send(
        &mut self,
        datagram: &[u8],
        entries: usize,
        first_of_batch: bool,
    ) -> io::Result<usize> {
        match self {
            DatagramDest::Udp { socket, addr } => socket.send_to(datagram, *addr).await,
            DatagramDest::Unix(dest) => dest.send(datagram, first_of_batch).await,
            #[cfg(test)]
            DatagramDest::Scripted(script) => script.send(datagram, Some(entries)).await,
        }
    }
}

/// A `transport: unix` socket, connected to the receiver's path.
pub(crate) enum UnixSocket {
    Real(UnixDatagram),
    #[cfg(test)]
    Scripted(Arc<ScriptedDest>),
}

impl UnixSocket {
    async fn send(&self, datagram: &[u8]) -> io::Result<usize> {
        match self {
            UnixSocket::Real(socket) => socket.send(datagram).await,
            #[cfg(test)]
            UnixSocket::Scripted(script) => script.send(datagram, None).await,
        }
    }
}

/// What a [`UnixDest`] connects to.
pub(crate) enum UnixTarget<'a> {
    Path(&'a Path),
    #[cfg(test)]
    Scripted(&'a Arc<ScriptedDest>),
}

impl UnixTarget<'_> {
    /// Connects a datagram socket, which doesn't block. A failure keeps its `ErrorKind` and names
    /// the path.
    fn connect(&self, sink: &str) -> io::Result<UnixSocket> {
        match self {
            UnixTarget::Path(path) => UnixDatagram::unbound()
                .and_then(|socket| socket.connect(path).map(|()| UnixSocket::Real(socket)))
                .map_err(|err| {
                    io::Error::new(
                        err.kind(),
                        format!("connecting to {sink} socket {}: {err}", path.display()),
                    )
                }),
            #[cfg(test)]
            UnixTarget::Scripted(script) => {
                script.connect();
                Ok(UnixSocket::Scripted(Arc::clone(script)))
            }
        }
    }

    fn describe(&self) -> String {
        match self {
            UnixTarget::Path(path) => path.display().to_string(),
            #[cfg(test)]
            UnixTarget::Scripted(_) => "the scripted socket".to_string(),
        }
    }
}

/// A `transport: unix` sender: a datagram socket connected to the receiver (`statsd_out`'s module
/// doc, "Packing and framing", has why it's connected and when it reconnects).
pub(crate) struct UnixDest<'a> {
    /// `None` until the first send, and again after a send that shows the receiver gone or stuck.
    pub(crate) socket: &'a mut Option<UnixSocket>,
    pub(crate) target: UnixTarget<'a>,
    /// Bounds each datagram's wait on a receiver whose queue is full.
    pub(crate) send_timeout: Duration,
    pub(crate) sink: &'static str,
    pub(crate) telemetry: &'a Telemetry,
    /// Gates `logit.output.reconnects`, as `crate::stream::PooledStream`'s flag does.
    pub(crate) has_connected_once: &'a mut bool,
}

impl UnixDest<'_> {
    /// Sends one datagram. When it's the batch's first and an inherited socket finds its receiver
    /// gone, reconnects and retries once: nothing of the batch has left, so the retry can't
    /// duplicate.
    async fn send(&mut self, datagram: &[u8], first_of_batch: bool) -> io::Result<usize> {
        let inherited = self.socket.is_some();
        match self.send_once(datagram).await {
            Err(err) if first_of_batch && inherited && is_receiver_gone(&err) => {
                self.send_once(datagram).await
            }
            result => result,
        }
    }

    /// Connects when there's no socket, then sends under `send_timeout`. Drops the socket on a
    /// timeout or a gone receiver, so the next send reconnects to whatever is at the path. A
    /// connect failure is a send error, `Fault::Clean` on a batch's first datagram.
    async fn send_once(&mut self, datagram: &[u8]) -> io::Result<usize> {
        let socket: &UnixSocket = match &mut *self.socket {
            Some(socket) => socket,
            slot @ None => {
                let socket = self.target.connect(self.sink)?;
                if *self.has_connected_once {
                    self.telemetry.count("logit.output.reconnects", 1.0, &[]);
                } else {
                    *self.has_connected_once = true;
                }
                slot.insert(socket)
            }
        };
        let result = match tokio::time::timeout(self.send_timeout, socket.send(datagram)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "the receiver at {} did not take a datagram within {:?}",
                    self.target.describe(),
                    self.send_timeout
                ),
            )),
        };
        if let Err(err) = &result {
            if err.kind() == io::ErrorKind::TimedOut || is_receiver_gone(err) {
                *self.socket = None;
            }
        }
        result
    }
}

/// `ECONNREFUSED` (the connected receiver's socket closed) or `ENOTCONN` (a later send on a socket
/// the kernel already disconnected): the path may now name a new receiver.
fn is_receiver_gone(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::ConnectionRefused | io::ErrorKind::NotConnected)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::pin::pin;
    use std::task::Poll;

    use logit_pipeline::test_util::TelemetryProbe;
    use proptest::prelude::*;

    use super::*;
    use crate::test_support::SendStep;

    /// `entries` with weight `weight` each, in a `MessageBuf<usize>` whose meta is the weight.
    fn entries(entries: &[&str], weight: usize) -> MessageBuf<usize> {
        let mut buf = MessageBuf::default();
        for entry in entries {
            buf.push_with(entry.as_bytes(), weight);
        }
        buf
    }

    fn packed(entries: &MessageBuf<usize>, cap: usize) -> Datagrams<'_, usize> {
        Datagrams { entries, weight: |w| *w, cap, framing: Framing::Packed }
    }

    /// Runs one [`send_datagrams`] over `dest`, reporting into `probe`.
    async fn send(
        batch: Datagrams<'_, usize>,
        dest: &mut DatagramDest<'_>,
        packet_buf: &mut Vec<u8>,
        probe: &TelemetryProbe,
    ) -> (Sent, anyhow::Result<()>) {
        let mut diag = Diagnostics::default();
        let telemetry = probe.telemetry("out", "test_out", "sink");
        let mut report = Report { sink: "test_out", diag: &mut diag, telemetry: &telemetry };
        send_datagrams(batch, dest, packet_buf, &mut report).await
    }

    const OVERSIZE: [(&str, &str); 1] = [("reason", "oversize_datagram")];

    #[test]
    fn only_emsgsize_is_a_message_too_large() {
        assert!(is_message_too_large(&io::Error::from_raw_os_error(90)));
        // std's own error for a Unix path too long for `sockaddr_un`.
        assert!(!is_message_too_large(&io::Error::new(io::ErrorKind::InvalidInput, "path")));
        // `EINVAL` (a UDP port of 0) and `EAFNOSUPPORT`.
        assert!(!is_message_too_large(&io::Error::from_raw_os_error(22)));
        assert!(!is_message_too_large(&io::Error::from_raw_os_error(97)));
    }

    #[test]
    fn pick_addr_takes_the_first_ipv4_address_else_the_first_ipv6_one() {
        let v4 = |last| SocketAddr::from((Ipv4Addr::new(127, 0, 0, last), 8125));
        let v6 = |last| SocketAddr::from((Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, last), 8125));
        assert_eq!(pick_addr([v6(1), v4(1)]), Some(v4(1)), "IPv4 wins over an earlier IPv6");
        assert_eq!(pick_addr([v4(2), v6(1), v4(1)]), Some(v4(2)), "the first IPv4 one");
        assert_eq!(pick_addr([v6(2), v6(1)]), Some(v6(2)), "the first IPv6 one");
        assert_eq!(pick_addr([]), None, "nothing to send to: resolution's error");
    }

    /// An endpoint that doesn't resolve is `Clean`: nothing of the batch was sent. One with no
    /// port fails inside `lookup_host` before any DNS query, so the test needs no resolver.
    #[tokio::test]
    async fn an_endpoint_that_does_not_resolve_is_clean() {
        let mut dest = UdpDest::bind("test_out").unwrap();
        let Err(err) = dest.resolve("test_out", "no-port-here").await else {
            panic!("an endpoint with no port can't resolve")
        };
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
    }

    /// The socket choice behind `UdpDest::resolve`, with no packet sent, so it runs where IPv6
    /// loopback isn't routable. An IPv4 answer uses the IPv4 socket and binds no IPv6 one; an
    /// IPv6-only answer binds the IPv6 slot, or fails `Clean` naming it where the host has no
    /// IPv6 at all. Never skipped.
    #[tokio::test]
    async fn resolution_selects_the_socket_of_the_chosen_address_family() {
        let UdpDest::Sockets { v4, mut v6 } = UdpDest::bind("test_out").unwrap() else {
            unreachable!("bind builds real sockets")
        };
        let v4_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 9));
        let v6_addr = SocketAddr::from((Ipv6Addr::LOCALHOST, 9));

        let chosen = pick_addr([v6_addr, v4_addr]).unwrap();
        let socket = socket_for(chosen, &v4, &mut v6, "test_out").unwrap();
        assert!(socket.local_addr().unwrap().is_ipv4());
        assert!(v6.is_none(), "an IPv4 answer binds no IPv6 socket");

        let chosen = pick_addr([v6_addr]).unwrap();
        match socket_for(chosen, &v4, &mut v6, "test_out") {
            Ok(socket) => {
                assert!(socket.local_addr().unwrap().is_ipv6());
                assert!(v6.is_some(), "the IPv6 slot keeps the socket for later batches");
            }
            Err(err) => {
                println!("this environment can't bind an IPv6 socket: {err:#}");
                assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
                assert!(format!("{err:#}").contains("IPv6 UDP socket"), "{err:#}");
            }
        }
    }

    /// Nothing reached the wire: an `EMSGSIZE` drop sends nothing, so the failure after it is
    /// `Clean`, and the dropped datagram's weight is counted.
    #[tokio::test]
    async fn emsgsize_then_a_failure_with_nothing_sent_is_clean() {
        let script = ScriptedDest::new([
            SendStep::TooLarge,
            SendStep::Fail(io::ErrorKind::ConnectionRefused),
        ]);
        let mut probe = TelemetryProbe::new();
        let buf = entries(&["aaaa", "bbbb", "cccc"], 3);
        let (sent, result) =
            send(packed(&buf, 4), &mut DatagramDest::Scripted(&script), &mut Vec::new(), &probe)
                .await;
        let err = result.expect_err("the second datagram fails");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert_eq!(sent, Sent::default());
        assert_eq!(probe.sum("logit.output.messages.dropped", &OVERSIZE), 3.0);
        assert_eq!(script.state().sends, 2, "the batch ends at the failure");
    }

    /// A datagram reached the wire before the failure, so it's `Ambiguous`, whatever `EMSGSIZE`
    /// drops sit between them.
    #[tokio::test]
    async fn sent_then_emsgsize_then_a_failure_is_ambiguous() {
        let script = ScriptedDest::new([
            SendStep::Accept,
            SendStep::TooLarge,
            SendStep::Fail(io::ErrorKind::ConnectionRefused),
        ]);
        let mut probe = TelemetryProbe::new();
        let buf = entries(&["aaaa", "bbbb", "cccc", "dddd"], 2);
        let (sent, result) =
            send(packed(&buf, 4), &mut DatagramDest::Scripted(&script), &mut Vec::new(), &probe)
                .await;
        let err = result.expect_err("the third datagram fails");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(sent, Sent { entries: 1, weight: 2, datagrams: 1 });
        assert_eq!(probe.sum("logit.output.messages.dropped", &OVERSIZE), 2.0);
    }

    /// What reached the wire before a failure comes back with the error.
    #[tokio::test]
    async fn a_failure_after_two_datagrams_returns_what_the_two_carried() {
        let script = ScriptedDest::new([
            SendStep::Accept,
            SendStep::Accept,
            SendStep::Fail(io::ErrorKind::ConnectionRefused),
        ]);
        let probe = TelemetryProbe::new();
        let buf = entries(&["a1", "a2", "b1", "b2", "c1"], 5);
        // Two entries and their separator fit a five-byte datagram.
        let (sent, result) =
            send(packed(&buf, 5), &mut DatagramDest::Scripted(&script), &mut Vec::new(), &probe)
                .await;
        assert_eq!(logit_pipeline::classify(&result.unwrap_err()), Fault::Ambiguous);
        assert_eq!(sent, Sent { entries: 4, weight: 20, datagrams: 2 });
        assert_eq!(script.datagrams(), [b"a1\na2".to_vec(), b"b1\nb2".to_vec()]);
    }

    /// One datagram per entry, whatever would fit: `syslog_out` and `collectd_out`.
    #[tokio::test]
    async fn one_per_entry_never_packs() {
        let script = ScriptedDest::new([]);
        let probe = TelemetryProbe::new();
        let buf = entries(&["a", "b", "c"], 4);
        let batch =
            Datagrams { entries: &buf, weight: |w| *w, cap: 100, framing: Framing::OnePerEntry };
        let (sent, result) =
            send(batch, &mut DatagramDest::Scripted(&script), &mut Vec::new(), &probe).await;
        result.expect("every datagram accepted");
        assert_eq!(sent, Sent { entries: 3, weight: 12, datagrams: 3 });
        assert_eq!(script.datagrams(), [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let counts: Vec<_> = script.state().accepted.iter().map(|(_, n)| *n).collect();
        assert_eq!(counts, [Some(1), Some(1), Some(1)]);
    }

    /// An entry over the cap never reaches the kernel: it's dropped, counted in the weight unit,
    /// and the entries around it are sent.
    #[tokio::test]
    async fn an_entry_over_the_cap_is_dropped_and_counted_and_its_neighbours_are_sent() {
        let script = ScriptedDest::new([]);
        let mut probe = TelemetryProbe::new();
        let buf = entries(&["aa", "bbbbbbbb", "cc"], 7);
        let (sent, result) =
            send(packed(&buf, 4), &mut DatagramDest::Scripted(&script), &mut Vec::new(), &probe)
                .await;
        result.expect("an over-cap entry is a drop, not a fault");
        assert_eq!(script.datagrams(), [b"aa".to_vec(), b"cc".to_vec()]);
        assert_eq!(sent, Sent { entries: 2, weight: 14, datagrams: 2 });
        assert_eq!(probe.sum("logit.output.messages.dropped", &OVERSIZE), 7.0);
    }

    /// Every attempt that reaches an over-cap entry counts it, as the kernel's `EMSGSIZE` counts
    /// every datagram it refuses: two attempts at one batch count both drops twice.
    #[tokio::test]
    async fn every_attempt_that_reaches_an_over_cap_entry_counts_it_as_emsgsize_does() {
        let script = ScriptedDest::new([
            SendStep::TooLarge,
            SendStep::Accept,
            SendStep::TooLarge,
            SendStep::Accept,
        ]);
        let mut probe = TelemetryProbe::new();
        let buf = entries(&["aa", "bbbbbbbb", "cc"], 7);
        for _ in 0..2 {
            let (_sent, result) = send(
                packed(&buf, 4),
                &mut DatagramDest::Scripted(&script),
                &mut Vec::new(),
                &probe,
            )
            .await;
            result.expect("neither drop is a fault");
        }
        assert_eq!(script.datagrams(), [b"cc".to_vec(), b"cc".to_vec()]);
        assert_eq!(
            probe.sum("logit.output.messages.dropped", &OVERSIZE),
            28.0,
            "the over-cap `bbbbbbbb` and the refused `aa`, on each of two attempts"
        );
    }

    /// A sink over [`send_datagrams`] as the datagram sinks build one, for entries no encoder
    /// produces: an entry over the cap.
    struct PackerSink {
        entries: MessageBuf<usize>,
        cap: usize,
        script: Arc<ScriptedDest>,
        diag: Diagnostics,
        telemetry: Telemetry,
    }

    #[async_trait::async_trait]
    impl logit_pipeline::Output for PackerSink {
        async fn send(&mut self, _batch: &logit_core::EventBatch) -> anyhow::Result<()> {
            let mut report =
                Report { sink: "test_out", diag: &mut self.diag, telemetry: &self.telemetry };
            let batch = packed(&self.entries, self.cap);
            let mut dest = DatagramDest::Scripted(&self.script);
            send_datagrams(batch, &mut dest, &mut Vec::new(), &mut report).await.1
        }
    }

    /// An over-cap entry the packer never reached on a failed attempt is counted on the retry that
    /// reaches it: attempt 1 fails sending `aa`, ahead of the over-cap `bbbbbbbb`.
    #[tokio::test]
    async fn an_over_cap_entry_first_reached_on_a_retry_is_counted() {
        let script = ScriptedDest::new([SendStep::Fail(io::ErrorKind::ConnectionRefused)]);
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "test_out", "sink");
        let mut sink = PackerSink {
            entries: entries(&["aa", "cc", "bbbbbbbb"], 7),
            cap: 4,
            script: Arc::clone(&script),
            diag: Diagnostics::default(),
            telemetry: telemetry.clone(),
        };
        let batch = logit_core::EventBatch {
            resource: Arc::new(logit_core::Resource::default()),
            scope: None,
            events: Vec::new(),
        };
        logit_pipeline::test_util::drive_write_loop(
            &mut sink,
            vec![batch],
            crate::test_support::fast_retry(),
            telemetry,
        )
        .await;
        assert_eq!(script.datagrams(), [b"aa".to_vec(), b"cc".to_vec()]);
        assert_eq!(probe.sum("logit.component.retries", &[]), 1.0);
        assert_eq!(
            probe.sum("logit.output.messages.dropped", &OVERSIZE),
            7.0,
            "the over-cap entry's weight, counted on the attempt that reached it"
        );
    }

    // -- `transport: unix` -----------------------------------------------------------------------

    /// The socket slot and flag a sink holds for a [`UnixDest`] over a script.
    struct UnixSlot {
        socket: Option<UnixSocket>,
        has_connected_once: bool,
        telemetry: Telemetry,
    }

    impl UnixSlot {
        /// A slot that already connected once, as after an earlier batch.
        fn inherited(script: &Arc<ScriptedDest>, probe: &TelemetryProbe) -> Self {
            Self {
                socket: Some(UnixSocket::Scripted(Arc::clone(script))),
                has_connected_once: true,
                telemetry: probe.telemetry("out", "test_out", "sink"),
            }
        }

        fn dest<'a>(&'a mut self, script: &'a Arc<ScriptedDest>) -> DatagramDest<'a> {
            DatagramDest::Unix(UnixDest {
                socket: &mut self.socket,
                target: UnixTarget::Scripted(script),
                send_timeout: Duration::from_secs(1),
                sink: "test_out",
                telemetry: &self.telemetry,
                has_connected_once: &mut self.has_connected_once,
            })
        }
    }

    /// The receiver restarted: the batch's first datagram is refused `EMSGSIZE` and dropped,
    /// and the next one, still the first to be sent, finds the receiver gone and gets the one
    /// reconnect-and-retry, since nothing of the batch has left.
    #[tokio::test]
    async fn the_reconnect_once_rule_survives_an_emsgsize_dropped_first_datagram() {
        let script = ScriptedDest::new([
            SendStep::TooLarge,
            SendStep::Fail(io::ErrorKind::ConnectionRefused),
        ]);
        let mut probe = TelemetryProbe::new();
        let mut slot = UnixSlot::inherited(&script, &probe);
        let buf = entries(&["aaaa", "bbbb"], 1);
        let (sent, result) =
            send(packed(&buf, 4), &mut slot.dest(&script), &mut Vec::new(), &probe).await;
        result.expect("the retry on a fresh connection delivers");
        assert_eq!(sent, Sent { entries: 1, weight: 1, datagrams: 1 });
        assert_eq!(script.datagrams(), [b"bbbb".to_vec()]);
        assert_eq!(script.state().connects, 1);
        assert_eq!(probe.sum("logit.output.reconnects", &[]), 1.0);
    }

    /// A send that timed out drops the socket, so the next send connects again rather than
    /// queue behind a receiver that stopped reading.
    #[tokio::test]
    async fn a_timed_out_unix_send_drops_the_socket_and_the_next_send_reconnects() {
        let script = ScriptedDest::new([SendStep::Fail(io::ErrorKind::TimedOut)]);
        let mut probe = TelemetryProbe::new();
        let mut slot = UnixSlot::inherited(&script, &probe);
        let buf = entries(&["aaaa"], 1);

        let (_, result) =
            send(packed(&buf, 4), &mut slot.dest(&script), &mut Vec::new(), &probe).await;
        assert_eq!(logit_pipeline::classify(&result.unwrap_err()), Fault::Clean);
        assert!(slot.socket.is_none(), "a timed-out socket is dropped");

        let (sent, result) =
            send(packed(&buf, 4), &mut slot.dest(&script), &mut Vec::new(), &probe).await;
        result.expect("the next batch connects again");
        assert_eq!(sent.datagrams, 1);
        assert_eq!(script.state().connects, 1);
        assert_eq!(probe.sum("logit.output.reconnects", &[]), 1.0);
    }

    /// The real `send_timeout`: a send parked on a receiver that never takes the datagram fails
    /// `TimedOut` and drops the socket.
    #[tokio::test(start_paused = true)]
    async fn a_unix_send_parked_past_send_timeout_times_out_and_drops_the_socket() {
        let script = ScriptedDest::new([SendStep::Park]);
        let probe = TelemetryProbe::new();
        let mut slot = UnixSlot::inherited(&script, &probe);
        let buf = entries(&["aaaa"], 1);
        let (_, result) =
            send(packed(&buf, 4), &mut slot.dest(&script), &mut Vec::new(), &probe).await;
        let err = result.expect_err("the parked send times out");
        assert!(format!("{err:#}").contains("did not take a datagram"), "{err:#}");
        assert!(slot.socket.is_none());
    }

    /// A send dropped while a datagram is parked leaves the packet buffer holding part of that
    /// batch and the Unix socket connected. The next send starts from a clean buffer, sends only
    /// its own entries, and reuses the connection.
    #[tokio::test]
    async fn a_send_dropped_mid_batch_leaves_a_clean_start_and_a_usable_unix_socket() {
        let script = ScriptedDest::new([SendStep::Accept, SendStep::Park]);
        let probe = TelemetryProbe::new();
        let mut slot = UnixSlot::inherited(&script, &probe);
        let mut packet_buf = Vec::new();

        let first = entries(&["a1", "a2", "a3"], 1);
        {
            let mut dest = slot.dest(&script);
            let mut dropped = pin!(send(packed(&first, 2), &mut dest, &mut packet_buf, &probe));
            let polled = std::future::poll_fn(|cx| Poll::Ready(dropped.as_mut().poll(cx))).await;
            assert!(polled.is_pending(), "parked on the second datagram");
        }
        assert_eq!(packet_buf, b"a2", "the parked datagram is still in the buffer");
        assert_eq!(script.state().sends, 2);

        let second = entries(&["b1", "b2"], 1);
        let (sent, result) =
            send(packed(&second, 2), &mut slot.dest(&script), &mut packet_buf, &probe).await;
        result.expect("the next batch sends");
        assert_eq!(sent, Sent { entries: 2, weight: 2, datagrams: 2 });
        assert_eq!(script.datagrams(), [b"a1".to_vec(), b"b1".to_vec(), b"b2".to_vec()]);
        assert_eq!(script.state().connects, 0, "the connected socket was reused");
    }

    // -- Packing properties ---------------------------------------------------------------------

    /// An entry as an encoder writes one: no leading or trailing `\n`, and sometimes an embedded
    /// one, as `statsd_out`'s negative-gauge pair has.
    fn arb_entry() -> impl Strategy<Value = Vec<u8>> {
        "[a-z]([a-z\n]{0,30}[a-z])?".prop_map(String::into_bytes)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Splitting a datagram on `\n` doesn't recover its entries (an entry can hold one), so
        /// the entry boundaries are checked against the counts the packer reports per datagram.
        /// The cap is drawn apart from the entries, so some cases hold entries over it, which
        /// must be dropped and counted, and the rest packed as if they weren't there.
        #[test]
        fn packing_never_exceeds_the_cap_never_splits_an_entry_and_reconciles(
            batch in prop::collection::vec((arb_entry(), 1usize..=5), 0..40),
            cap in 1usize..48,
        ) {
            let mut buf = MessageBuf::<usize>::default();
            for (entry, weight) in &batch {
                buf.push_with(entry, *weight);
            }
            let script = ScriptedDest::new([]);
            let mut probe = TelemetryProbe::new();
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let (sent, result) = runtime.block_on(send(
                packed(&buf, cap),
                &mut DatagramDest::Scripted(&script),
                &mut Vec::new(),
                &probe,
            ));
            prop_assert!(result.is_ok());
            let state = script.state();
            let datagrams: Vec<&[u8]> =
                state.accepted.iter().map(|(datagram, _)| datagram.as_slice()).collect();

            for datagram in &datagrams {
                prop_assert!(!datagram.is_empty());
                prop_assert!(datagram.len() <= cap, "{} bytes, cap {cap}", datagram.len());
                prop_assert!(datagram[0] != b'\n' && datagram[datagram.len() - 1] != b'\n');
            }
            // Over-cap entries are absent from every datagram; the rest appear in order.
            let (kept, over): (Vec<_>, Vec<_>) =
                batch.iter().partition(|(entry, _)| entry.len() <= cap);
            let entries: Vec<&[u8]> = kept.iter().map(|(entry, _)| entry.as_slice()).collect();
            prop_assert_eq!(datagrams.join(&b'\n'), entries.join(&b'\n'));
            let over_weight: usize = over.iter().map(|(_, weight)| weight).sum();
            let dropped = probe.sum("logit.output.messages.dropped", &OVERSIZE);
            prop_assert_eq!(dropped, over_weight as f64, "over-cap weight counted");

            // Each datagram is a run of whole entries, as many as the packer said it holds, and
            // the next entry would not have fit.
            let mut next = 0;
            for (datagram, held) in &state.accepted {
                let held = held.expect("the packer names each datagram's entry count");
                prop_assert!(held > 0);
                prop_assert!(next + held <= entries.len());
                prop_assert_eq!(datagram, &entries[next..next + held].join(&b'\n'));
                next += held;
                if let Some(following) = entries.get(next) {
                    prop_assert!(datagram.len() + 1 + following.len() > cap, "not greedy");
                }
            }
            prop_assert_eq!(next, entries.len());

            let weight: usize = kept.iter().map(|(_, weight)| weight).sum();
            let expected = Sent { entries: entries.len(), weight, datagrams: datagrams.len() };
            prop_assert_eq!(sent, expected);
        }
    }
}
