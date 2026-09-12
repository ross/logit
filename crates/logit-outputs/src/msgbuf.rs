//! A reusable buffer of encoded messages: one contiguous byte buffer plus a range per message, so
//! encoding a batch allocates once (the backing `Vec<u8>` grows as needed and is never freed
//! between calls) rather than once per message.
//!
//! Shared by every per-message/per-datagram sink -- originally `syslog_out`'s own private type,
//! lifted out here once `statsd_out` needed the identical shape (`syslog::MessageBuf` re-exports
//! this for source compatibility, since `crates/logit-bench` names it by that path).

#[derive(Debug, Default)]
pub struct MessageBuf {
    bytes: Vec<u8>,
    ranges: Vec<std::ops::Range<usize>>,
}

impl MessageBuf {
    pub(crate) fn clear(&mut self) {
        self.bytes.clear();
        self.ranges.clear();
    }

    pub(crate) fn push(&mut self, msg: &str) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(msg.as_bytes());
        self.ranges.push(start..self.bytes.len());
    }

    /// Same as [`MessageBuf::push`], for a message that is already raw bytes (arbitrary, not
    /// assumed to be valid UTF-8) -- `syslog_out`'s `Value::Bytes` message path
    /// (`crates/logit-outputs/src/syslog.rs`'s module doc, "Message body" section).
    pub(crate) fn push_bytes(&mut self, msg: &[u8]) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(msg);
        self.ranges.push(start..self.bytes.len());
    }

    /// One slice per encoded message, in batch order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.ranges.iter().map(move |r| &self.bytes[r.clone()])
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn total_bytes(&self) -> usize {
        self.bytes.len()
    }
}
