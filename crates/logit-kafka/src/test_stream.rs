//! [`FakeStream`], a scripted stand-in for the stream behind the `Box<dyn AsyncStream>` seam that
//! the broker connection and `logit-outputs`' sinks write to, for the tests of both crates.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A scripted in-memory stream standing in for the connection behind a
/// `Box<dyn AsyncStream>` seam, with shared state a test inspects after the call under test.
///
/// Writes are accepted whole unless a [`WriteStep`] is scripted for that call. Accepted bytes
/// land in [`FakeState::unflushed`]; a successful `flush` moves them to [`FakeState::flushed`], so
/// a test tells "the stream took it" from "the stream was told to deliver it". Reads follow one
/// [`ReadStep`], `Pending` by default.
///
/// Never wrap this in tokio-rustls. It returns `Ok(0)` or `Pending` without registering a waker,
/// and tokio-rustls treats an IO `Ok(0)` as would-block, so a TLS write over it can park forever
/// with nothing to wake it (`logit-outputs`' `stream_pins`). TLS behavior is tested against a
/// real tokio-rustls pair (`logit-outputs`' `test_support::tls_pair`) instead.
#[derive(Clone, Default)]
pub struct FakeStream(Arc<Mutex<FakeState>>);

/// What a [`FakeStream`] has seen, and its script.
#[derive(Default)]
pub struct FakeState {
    /// Accepted by `write` and not yet flushed.
    pub unflushed: Vec<u8>,
    /// Moved out of `unflushed` by a successful `flush`.
    pub flushed: Vec<u8>,
    /// `poll_write` calls, including scripted failures.
    pub writes: usize,
    /// `poll_flush` calls, including a scripted failure.
    pub flushes: usize,
    /// `poll_read` calls.
    pub reads: usize,
    /// Per 1-based write call number.
    write_steps: Vec<(usize, WriteStep)>,
    flush_error: Option<io::ErrorKind>,
    read: ReadStep,
}

/// A scripted outcome for one `write` call of a [`FakeStream`].
#[derive(Clone, Copy, Debug)]
pub enum WriteStep {
    /// Accepts at most this many bytes.
    Short(usize),
    /// Returns `Ok(0)`, accepting nothing.
    Zero,
    /// Fails with this kind, accepting nothing.
    Fail(io::ErrorKind),
}

/// How every `read` of a [`FakeStream`] answers.
#[derive(Clone, Debug, Default)]
pub enum ReadStep {
    /// `Pending`, as a live, quiet peer is. No waker is registered, so a test that awaits a read
    /// here hangs instead of passing.
    #[default]
    Pending,
    /// `Ready(Ok(()))` with nothing filled: the peer closed.
    Eof,
    /// `Ready(Err(kind))`.
    Fail(io::ErrorKind),
    /// Unsolicited bytes, handed out across reads; `Pending` once they run out.
    Bytes(Vec<u8>),
}

impl FakeStream {
    pub fn new() -> Self {
        Self::default()
    }

    /// Scripts the `call`th write (1-based).
    pub fn on_write(self, call: usize, step: WriteStep) -> Self {
        self.state().write_steps.push((call, step));
        self
    }

    /// Every `flush` fails with `kind` and moves nothing.
    pub fn failing_flush(self, kind: io::ErrorKind) -> Self {
        self.state().flush_error = Some(kind);
        self
    }

    pub fn reading(self, step: ReadStep) -> Self {
        self.state().read = step;
        self
    }

    pub fn state(&self) -> MutexGuard<'_, FakeState> {
        self.0.lock().unwrap()
    }
}

impl AsyncWrite for FakeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state();
        state.writes += 1;
        let call = state.writes;
        let step = state.write_steps.iter().find(|(n, _)| *n == call).map(|(_, step)| *step);
        let accepted = match step {
            None => buf.len(),
            Some(WriteStep::Short(n)) => n.min(buf.len()),
            Some(WriteStep::Zero) => 0,
            Some(WriteStep::Fail(kind)) => {
                return Poll::Ready(Err(io::Error::new(kind, "scripted write failure")));
            }
        };
        state.unflushed.extend_from_slice(&buf[..accepted]);
        Poll::Ready(Ok(accepted))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state();
        state.flushes += 1;
        if let Some(kind) = state.flush_error {
            return Poll::Ready(Err(io::Error::new(kind, "scripted flush failure")));
        }
        let unflushed = std::mem::take(&mut state.unflushed);
        state.flushed.extend_from_slice(&unflushed);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for FakeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.state();
        state.reads += 1;
        match &mut state.read {
            ReadStep::Pending => Poll::Pending,
            ReadStep::Eof => Poll::Ready(Ok(())),
            ReadStep::Fail(kind) => {
                Poll::Ready(Err(io::Error::new(*kind, "scripted read failure")))
            }
            ReadStep::Bytes(bytes) if bytes.is_empty() => Poll::Pending,
            ReadStep::Bytes(bytes) => {
                let n = bytes.len().min(buf.remaining());
                buf.put_slice(&bytes[..n]);
                bytes.drain(..n);
                Poll::Ready(Ok(()))
            }
        }
    }
}
