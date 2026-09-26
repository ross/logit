//! Black-box checks on the five `hyper`-based listeners (`otlp_in`, `prometheus_in`'s
//! remote-write receiver, `datadog_in`, `datadog_trace_in`, `splunk_hec_in`), driven over real
//! sockets.
//!
//! This file installs a counting global allocator, so it is its own test binary: the heap
//! measurement below would see every other test's allocations if it shared a process with them.

use logit_inputs::datadog::DatadogInput;
use logit_inputs::datadog_trace::DatadogTraceInput;
use logit_inputs::otlp::{OtlpInput, OtlpTransport};
use logit_inputs::prometheus::PrometheusReceiver;
use logit_inputs::splunk::SplunkHecInput;
use logit_inputs::Input;
use logit_pipeline::Fanout;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Counts live heap bytes and forwards every call to [`System`] unchanged.
struct CountingAlloc;

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);

// SAFETY: every method forwards to `System` with the caller's own arguments, so `System`'s
// contract is the one upheld; the counter is a side effect only.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE_BYTES.fetch_add(layout.size() as isize, Ordering::Relaxed);
        // SAFETY: the caller's `layout`, as `GlobalAlloc::alloc` requires.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        // SAFETY: `ptr` came from `System` under `layout`, per the caller's contract.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE_BYTES.fetch_add(new_size as isize - layout.size() as isize, Ordering::Relaxed);
        // SAFETY: forwarded unchanged under the caller's contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn live_bytes() -> isize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

/// Runs `input` on its own task with a consumer nobody drains. The receiver is leaked so the
/// channel stays open for the test's lifetime.
async fn run<I: Input + Send + 'static>(mut input: I) {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    std::mem::forget(rx);
    tokio::spawn(async move { input.run(Fanout::new(vec![tx])).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// ---- the h2 SETTINGS each listener advertises --------------------------------------------------

/// RFC 9113 §3.4's client connection preface.
const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
/// RFC 9113 §6.5.2 setting identifiers.
const SETTINGS_MAX_CONCURRENT_STREAMS: u16 = 0x3;
const SETTINGS_MAX_HEADER_LIST_SIZE: u16 = 0x6;

/// Sends an h2c prior-knowledge preface and an empty `SETTINGS` frame to `addr`, and returns the
/// `(identifier, value)` pairs of the server's first non-ACK `SETTINGS` frame.
async fn server_settings(addr: &str) -> Vec<(u16, u32)> {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(H2_PREFACE).await.unwrap();
    // An empty SETTINGS frame: length 0, type 0x4, flags 0, stream 0.
    stream.write_all(&[0, 0, 0, 0x4, 0, 0, 0, 0, 0]).await.unwrap();
    loop {
        let mut head = [0u8; 9];
        tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut head))
            .await
            .expect("a frame within 3s")
            .expect("reading a frame header");
        let len = u32::from_be_bytes([0, head[0], head[1], head[2]]) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await.unwrap();
        let (frame_type, flags) = (head[3], head[4]);
        if frame_type == 0x4 && flags & 0x1 == 0 {
            let (settings, _) = payload.as_chunks::<6>();
            return settings
                .iter()
                .map(|c| {
                    (u16::from_be_bytes([c[0], c[1]]), u32::from_be_bytes([c[2], c[3], c[4], c[5]]))
                })
                .collect();
        }
    }
}

/// Every h2 entry point advertises `MAX_CONCURRENT_STREAMS = 200` and `MAX_HEADER_LIST_SIZE =
/// 16384`: hyper 1.11.1's own server defaults, pinned by the shared builders in `crate::http` so a
/// hyper upgrade that moves them fails here.
#[tokio::test]
async fn the_h2_settings_frame_advertises_the_pinned_stream_cap() {
    let mut listeners = Vec::new();

    let mut input = OtlpInput::new("127.0.0.1:0", OtlpTransport::Grpc);
    input.bind().await.unwrap();
    listeners.push(("otlp_in (grpc)", input.local_addr().unwrap().to_string()));
    run(input).await;

    let mut input = OtlpInput::new("127.0.0.1:0", OtlpTransport::Http);
    input.bind().await.unwrap();
    listeners.push(("otlp_in (http, h2c)", input.local_addr().unwrap().to_string()));
    run(input).await;

    let mut input = PrometheusReceiver::new("127.0.0.1:0", "/api/v1/write");
    input.bind().await.unwrap();
    listeners.push(("prometheus_in (bind, h2c)", input.local_addr().unwrap().to_string()));
    run(input).await;

    let mut input = DatadogInput::new("127.0.0.1:0");
    input.bind().await.unwrap();
    listeners.push(("datadog_in (h2c)", input.local_addr().unwrap().to_string()));
    run(input).await;

    let mut input = DatadogTraceInput::new().with_bind("127.0.0.1:0");
    input.bind().await.unwrap();
    listeners.push(("datadog_trace_in (tcp, h2c)", input.local_addr().unwrap().to_string()));
    run(input).await;

    let mut input = SplunkHecInput::new("127.0.0.1:0");
    input.bind().await.unwrap();
    listeners.push(("splunk_hec_in (h2c)", input.local_addr().unwrap().to_string()));
    run(input).await;

    for (who, addr) in listeners {
        let settings = server_settings(&addr).await;
        let value = |id: u16| settings.iter().find(|(i, _)| *i == id).map(|(_, v)| *v);
        assert_eq!(value(SETTINGS_MAX_CONCURRENT_STREAMS), Some(200), "{who}: {settings:?}");
        assert_eq!(value(SETTINGS_MAX_HEADER_LIST_SIZE), Some(16 * 1024), "{who}: {settings:?}");
    }
}

// ---- request-body collection -------------------------------------------------------------------

/// A body arriving as many small TCP segments is held in one growing buffer, not one buffer per
/// read: over h1, each read's frame shares hyper's read buffer, and hyper allocates a fresh one
/// behind it, so keeping every frame alive until the body ends keeps every one of those buffers.
///
/// 256 KiB arrives in 256-byte writes with `TCP_NODELAY`, so nearly every write is its own read.
/// The head declares 4 MiB (`otlp_in`'s cap) so the body is still being collected when the heap is
/// measured. One growing buffer's capacity stays under twice its length.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_arriving_in_small_writes_is_held_once() {
    const BODY: usize = 256 * 1024;
    const WRITE: usize = 256;

    let mut input = OtlpInput::new("127.0.0.1:0", OtlpTransport::Http);
    input.bind().await.unwrap();
    let addr = input.local_addr().unwrap().to_string();
    run(input).await;

    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let head = format!(
        "POST /v1/metrics HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/x-protobuf\r\n\
         Content-Length: {}\r\n\r\n",
        4 * 1024 * 1024
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let before = live_bytes();
    let chunk = vec![0x0au8; WRITE];
    for _ in 0..BODY / WRITE {
        stream.write_all(&chunk).await.unwrap();
        tokio::time::sleep(Duration::from_micros(300)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let held = live_bytes() - before;

    let ratio = held as f64 / BODY as f64;
    assert!(
        held < 2 * BODY as isize,
        "{BODY} body bytes in {WRITE}-byte writes left {held} bytes live ({ratio:.1}x the body)"
    );
    drop(stream);
}
