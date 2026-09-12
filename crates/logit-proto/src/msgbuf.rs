//! A reusable buffer of encoded messages: one contiguous byte buffer plus a range per message
//! (and one `M` per message), so encoding a batch allocates once (the backing `Vec`s grow as
//! needed and are never freed between calls) rather than once per message. The output half of
//! [`crate::FramedEncoder`] -- see ADR `framed-encoder`.
//!
//! Originally `syslog_out`'s own private type, lifted into `logit-outputs`'s `msgbuf` once
//! `statsd_out` needed the identical shape, and moved here once a codec living in *this* crate
//! needed it too: `logit-proto` can't depend on `logit-outputs`, so a buffer both a codec and a
//! sink name has to live at the codec layer.

use std::fmt;
use std::ops::Range;

/// One contiguous byte buffer, one range per message, one `M` per message.
///
/// `M` is whatever an encoder needs to say about a message beyond its bytes -- `()` for a sink
/// whose messages are self-describing (syslog's one datagram or frame per message, statsd's
/// lines packed by the transport), or, say, a per-datagram value count for a packer that has to
/// report how many records each message carries. `M = ()` costs nothing: a `Vec<()>` never
/// allocates, so the default instantiation is exactly the two-`Vec` buffer it always was.
///
/// The "allocate once per batch, never free between calls" contract is load-bearing: the exact
/// allocation counts `crates/logit-bench/tests/allocations.rs` pins for every framed encoder
/// (`docs/design/memory.md` §2) rely on `clear` keeping capacity.
pub struct MessageBuf<M = ()> {
    bytes: Vec<u8>,
    ranges: Vec<Range<usize>>,
    /// Pushed in lock-step with `ranges`: `meta[i]` describes the message `ranges[i]` bounds.
    meta: Vec<M>,
}

// Manual impls rather than derives: a derive would demand `M: Default`/`M: Debug` bounds on the
// *type*, and an empty buffer needs neither.
impl<M> Default for MessageBuf<M> {
    fn default() -> Self {
        Self { bytes: Vec::new(), ranges: Vec::new(), meta: Vec::new() }
    }
}

impl<M: fmt::Debug> fmt::Debug for MessageBuf<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageBuf")
            .field("bytes", &self.bytes)
            .field("ranges", &self.ranges)
            .field("meta", &self.meta)
            .finish()
    }
}

impl<M> MessageBuf<M> {
    /// Forgets every message but keeps every backing allocation -- the next batch of the same
    /// size fills the buffer without touching the allocator.
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.ranges.clear();
        self.meta.clear();
    }

    /// Appends one message and its `meta`.
    pub fn push_with(&mut self, msg: &[u8], meta: M) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(msg);
        self.ranges.push(start..self.bytes.len());
        self.meta.push(meta);
    }

    /// One slice per encoded message, in batch order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.ranges.iter().map(move |r| &self.bytes[r.clone()])
    }

    /// [`MessageBuf::iter`], paired with each message's `meta`.
    pub fn iter_with(&self) -> impl Iterator<Item = (&[u8], &M)> {
        self.ranges.iter().zip(&self.meta).map(move |(r, meta)| (&self.bytes[r.clone()], meta))
    }

    /// Message count.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Bytes across every message (no separators -- framing is the transport's job).
    pub fn total_bytes(&self) -> usize {
        self.bytes.len()
    }
}

impl MessageBuf<()> {
    /// Appends one text message.
    pub fn push(&mut self, msg: &str) {
        self.push_with(msg.as_bytes(), ());
    }

    /// Same as [`MessageBuf::push`], for a message that is already raw bytes (arbitrary, not
    /// assumed to be valid UTF-8) -- `syslog_out`'s `Value::Bytes` message path
    /// (`crates/logit-outputs/src/syslog.rs`'s module doc, "Message body" section).
    pub fn push_bytes(&mut self, msg: &[u8]) {
        self.push_with(msg, ());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_come_back_in_push_order() {
        let mut buf = MessageBuf::default();
        buf.push("hello");
        buf.push_bytes(b"\xffraw");
        buf.push("");
        buf.push("world");
        let msgs: Vec<&[u8]> = buf.iter().collect();
        assert_eq!(msgs, vec![&b"hello"[..], &b"\xffraw"[..], &b""[..], &b"world"[..]]);
        assert_eq!(buf.len(), 4);
        assert!(!buf.is_empty());
        assert_eq!(buf.total_bytes(), "hello".len() + b"\xffraw".len() + "world".len());
    }

    #[test]
    fn iter_with_pairs_each_message_with_its_own_meta() {
        let mut buf: MessageBuf<usize> = MessageBuf::default();
        buf.push_with(b"one", 1);
        buf.push_with(b"two three", 2);
        let pairs: Vec<(&[u8], &usize)> = buf.iter_with().collect();
        assert_eq!(pairs, vec![(&b"one"[..], &1), (&b"two three"[..], &2)]);
        // `iter` sees the same messages, meta-less.
        assert_eq!(buf.iter().count(), 2);
    }

    #[test]
    fn an_empty_buffer_reports_empty() {
        let buf: MessageBuf = MessageBuf::default();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.total_bytes(), 0);
        assert_eq!(buf.iter().count(), 0);
        assert_eq!(buf.iter_with().count(), 0);
    }

    #[test]
    fn clear_forgets_messages_but_keeps_capacity() {
        let mut buf: MessageBuf<u8> = MessageBuf::default();
        for i in 0..64u8 {
            buf.push_with(b"twelve bytes", i);
        }
        let caps = (buf.bytes.capacity(), buf.ranges.capacity(), buf.meta.capacity());
        assert!(caps.0 >= 64 * 12 && caps.1 >= 64 && caps.2 >= 64);

        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.total_bytes(), 0);
        assert_eq!((buf.bytes.capacity(), buf.ranges.capacity(), buf.meta.capacity()), caps);

        // A second fill of the same size reuses every backing allocation untouched.
        for i in 0..64u8 {
            buf.push_with(b"twelve bytes", i);
        }
        assert_eq!(buf.len(), 64);
        assert_eq!((buf.bytes.capacity(), buf.ranges.capacity(), buf.meta.capacity()), caps);
    }

    #[test]
    fn unit_meta_never_allocates() {
        let mut buf: MessageBuf = MessageBuf::default();
        for _ in 0..1000 {
            buf.push("m");
        }
        // `Vec<()>` is a zero-sized-element vector: its "capacity" is `usize::MAX` and it never
        // touches the allocator, which is what makes `MessageBuf<()>` cost exactly what the
        // pre-generic two-`Vec` buffer did.
        assert_eq!(buf.meta.capacity(), usize::MAX);
        assert_eq!(buf.meta.len(), 1000);
    }
}
