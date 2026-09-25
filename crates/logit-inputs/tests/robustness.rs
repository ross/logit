//! Peak-heap pins for `logit_in`, driven through its public API over a real socket.
//!
//! Its own test binary because it installs a counting `#[global_allocator]`, which would
//! otherwise tax every unit test in the crate (`crates/logit-proto/tests/robustness.rs` has the
//! same arrangement). The counters are thread-local, and `#[tokio::test]`'s default
//! `current_thread` runtime runs the listener, its connection task, and the test's client on the
//! test thread, so a test measures the whole listener and nothing from other tests.

use bytes::Bytes;
use logit_core::{AttrMap, Event, EventBatch, LogRecord, Resource, Severity, Value};
use logit_inputs::logit::LogitInput;
use logit_inputs::Input;
use logit_pipeline::Fanout;
use logit_proto::frame::{self, Compression};
use logit_proto::native::{self, control, NativeEncoder};
use logit_proto::Encoder;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

struct CountingAlloc;

thread_local! {
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

/// `try_with`: an allocation during thread-local teardown is served but not counted.
fn record(delta: i64) {
    let _ = LIVE.try_with(|live| {
        let now = live.get() + delta;
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

// SAFETY: every method forwards to `System` with the caller's arguments unchanged and only
// updates thread-local counters around the call.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as i64);
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract for `layout`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as i64);
        // SAFETY: as `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(-(layout.size() as i64));
        // SAFETY: `ptr` came from this allocator, which is `System`, with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size as i64 - layout.size() as i64);
        // SAFETY: as `dealloc`, and the caller upholds `realloc`'s size contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Starts a measurement window: live and peak bytes restart from zero.
fn reset() {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
}

/// The peak live bytes since [`reset`]. Memory allocated before the window and freed inside it
/// lowers the running total, so this is a close lower bound, not an exact count.
fn peak() -> i64 {
    PEAK.with(Cell::get)
}

/// A running `LogitInput` on an ephemeral port, forwarding into a channel.
async fn running_listener() -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
    let mut input = LogitInput::new("127.0.0.1:0");
    input.bind().await.expect("binding an ephemeral port should succeed");
    let addr = input.local_addr().expect("bind() leaves a real address behind").to_string();
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move { input.run(Fanout::new(vec![tx])).await });
    (addr, rx)
}

async fn write_control(stream: &mut TcpStream, payload: Bytes) {
    let framed =
        frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &payload).unwrap();
    stream.write_all(&framed).await.unwrap();
}

async fn read_control(stream: &mut TcpStream) -> control::ControlMessage {
    let mut header = [0u8; frame::HEADER_LEN];
    stream.read_exact(&mut header).await.unwrap();
    let parsed = frame::FrameHeader::read(&mut Bytes::copy_from_slice(&header)).unwrap();
    let mut full = vec![0u8; frame::HEADER_LEN + parsed.compressed_len as usize];
    full[..frame::HEADER_LEN].copy_from_slice(&header);
    stream.read_exact(&mut full[frame::HEADER_LEN..]).await.unwrap();
    let (_, mut payload) = frame::read_frame_with_header(&mut Bytes::from(full)).unwrap();
    control::ControlMessage::decode(&mut payload).unwrap()
}

async fn handshake(stream: &mut TcpStream) {
    let hello = control::Hello {
        version: control::PROTOCOL_VERSION,
        codecs: vec![native::CODEC_NATIVE_V1],
        compressions: vec![0],
        max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
        window: 1,
    };
    write_control(stream, hello.encode()).await;
    match read_control(stream).await {
        control::ControlMessage::HelloAck(_) => {}
        other => panic!("expected HelloAck, got {other:?}"),
    }
}

/// A frame's body is held once from the socket to the downstream inbox: `logit_in` reads it into
/// one buffer of `HEADER_LEN + compressed_len` and hands it to `frame::read_frame_with_header`,
/// and the decoder slices the event's `Value::Str` body out of that buffer. So the listener's
/// peak for an 8 MiB body is about one body, not two.
#[tokio::test]
async fn a_frame_body_is_held_once_at_peak() {
    const BODY: usize = 8 * 1024 * 1024;
    let (addr, mut rx) = running_listener().await;
    let mut client = TcpStream::connect(&addr).await.unwrap();
    handshake(&mut client).await;

    let event = Event::log(
        1,
        AttrMap::new(),
        LogRecord {
            message: Value::str("x".repeat(BODY)),
            severity: Some(Severity::Info),
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    let batch =
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![event] };
    let framed = NativeEncoder::new(Compression::None).encode(&batch).unwrap();
    drop(batch);

    reset();
    client.write_all(&framed).await.unwrap();
    match read_control(&mut client).await {
        control::ControlMessage::Ack(ack) => assert_eq!(ack.seq, 1),
        other => panic!("expected Ack, got {other:?}"),
    }
    let peak = peak();
    drop(framed);

    let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the batch is forwarded")
        .expect("the channel is open");
    let relayed = logit_pipeline::unwrap_batch(delivered);
    assert_eq!(relayed.events.len(), 1);

    let body = BODY as i64;
    assert!(
        (body..body + body / 8).contains(&peak),
        "peak live heap {peak} bytes for a {BODY}-byte body: the body is held more than once"
    );
}

/// A stray client (an HTTP request, a syslog line) fails the frame header's magic check before
/// the length bound and the body allocation, so the listener closes it having allocated nothing
/// sized from its bytes: only the connection's own fixed bookkeeping.
#[tokio::test]
async fn a_stray_client_allocates_nothing_sized_from_its_bytes() {
    const STRAYS: [(&str, &[u8]); 2] = [
        ("http", b"GET /metrics HTTP/1.1\r\nHost: logit\r\n\r\n"),
        ("syslog", b"<13>1 2026-09-25T00:00:00Z host app - - - hello world\n"),
    ];
    let (addr, _rx) = running_listener().await;

    for (name, bytes) in STRAYS {
        reset();
        let mut stray = TcpStream::connect(&addr).await.unwrap();
        stray.write_all(bytes).await.unwrap();
        let mut buf = [0u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(2), stray.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("{name}: expected a close within 2s"));
        assert!(matches!(read, Ok(0) | Err(_)), "{name}: expected a close, got {read:?}");
        let peak = peak();
        assert!(peak < 64 * 1024, "{name}: {peak} bytes allocated for a rejected connection");
    }
}
