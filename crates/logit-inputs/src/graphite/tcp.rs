//! `graphite_in`'s TCP accept loop and per-connection reader -- carbon's plaintext line protocol
//! and its length-prefixed pickle batch protocol over a stream.
//!
//! Written here rather than in a shared driver because there is nothing to share it with yet:
//! `syslog_in` is UDP-only and `otlp_in`/`logit_in` own bespoke, protocol-specific loops (ADR
//! `graphite-carbon-relay`'s "No shared `logit_inputs::tcp` driver yet"; the extraction trigger it
//! names is a second line listener, e.g. a TCP `syslog_in`). The *shape* is borrowed rather than
//! invented, though: [`run_accept_loop`] is `otlp_in`'s bind/accept split
//! (`crates/logit-inputs/src/otlp.rs`) with `logit_in`'s connection cap and per-connection
//! shutdown racing (`crates/logit-inputs/src/logit.rs`), and `serve_connection`'s batch assembly is
//! `crate::udp`'s `decode_loop` with the receive queue taken out.
//!
//! **No [`ReceiveQueue`](crate::udp::ReceiveQueue).** ADR `decoupled-listener-io` decouples reading
//! from decoding because a UDP socket drops silently while the decoder is busy; a stream cannot do
//! that. Here, not reading *is* the backpressure, and it propagates all the way back to the
//! sender's own kernel buffer -- so a connection that stalls does so visibly, at the sender,
//! rather than by quietly discarding datapoints.

use bytes::{Buf, BytesMut};
use logit_core::{Diagnostics, Event, EventBatch, Resource, Scope, Telemetry};
use logit_pipeline::{BatchAccumulator, Fanout, FlushReason};
use logit_proto::graphite::pickle::LENGTH_PREFIX_BYTES;
use logit_proto::graphite::{GraphiteDecoder, Protocol};
use logit_proto::Decoder;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;

/// How much room to make available for one `read` call. Not a bound on anything -- the buffer
/// grows past it whenever a line or a frame spans reads, and `max_line_bytes`/`max_frame_bytes`
/// are what actually bound it. 8 KiB is one `logit_proto::graphite::DEFAULT_MAX_LINE_BYTES` line,
/// so an ordinary plaintext connection reads whole lines in one syscall.
const READ_CHUNK_BYTES: usize = 8192;

/// Everything one connection needs to know, gathered so [`run_accept_loop`] takes one value rather
/// than seven. Copied per connection -- it is a handful of words and is never mutated.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionConfig {
    pub protocol: Protocol,
    pub max_line_bytes: usize,
    pub max_frame_bytes: usize,
    pub batch_max_events: usize,
    pub batch_max_bytes: u64,
    pub batch_flush_interval: Duration,
    pub max_connections: usize,
}

/// Accepts connections until `shutdown` fires or `accept` itself fails, serving each in its own
/// task.
///
/// Three things it does deliberately, each with a named precedent:
///
/// - **The permit is taken after `accept`, non-blockingly** -- `logit_in`'s shape, not `otlp_in`'s:
///   a connection past the cap is closed at once and counted, because carbon's wire has no way to
///   say "try later" and a sender holding an accepted-but-never-read connection would look healthy
///   while delivering nothing.
/// - **One connection's error is never fatal** -- only `accept` failing is. A client disconnecting
///   mid-line, a hostile pickle frame, a reset: all `connection_error`, exactly as `otlp_in` and
///   `logit_in` treat theirs.
/// - **Teardown waits for the connections, and always reaches them.** Each serving task watches
///   the component's own shutdown signal *and* a second, local one this loop owns; whichever way
///   the loop ends it flips the local signal, then joins every task rather than returning
///   immediately, so each connection finishes its current read, flushes its accumulator and
///   returns. On the shutdown path the local signal is redundant (the task is already watching the
///   component's). On the `accept`-error path it is the only thing that ever tells them: nothing
///   flips the component's signal there, so without it a single idle client would park this drain
///   forever and the error would never reach `run_input` -- which only arms its `shutdown_grace`
///   backstop once shutdown has actually fired. `logit_in` propagates an accept error immediately
///   for the same reason; this is that property, kept while still letting each connection flush.
///   `InputRuntimeConfig::shutdown_grace` is what bounds the orderly drain -- the contract
///   `crate::Input::run_until_shutdown` states.
pub async fn run_accept_loop(
    listener: TcpListener,
    sink: Fanout,
    config: ConnectionConfig,
    resource: Arc<Resource>,
    diag: Diagnostics,
    telemetry: Telemetry,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let connection_limit = Arc::new(tokio::sync::Semaphore::new(config.max_connections));
    let live_connections = Arc::new(AtomicI64::new(0));
    let mut connections = JoinSet::new();
    // This loop's own teardown signal, distinct from the component's -- see the "Teardown" bullet
    // above. A local `watch` rather than a `CancellationToken` because `tokio-util` is not a
    // dependency of this crate and this effort adds none.
    let (cancel, cancel_rx) = watch::channel(false);

    let result = loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            // Reaps finished connections so the `JoinSet` doesn't grow for the process's lifetime
            // on a long-lived listener. Disabled while empty, or `join_next` would resolve
            // immediately with `None` and spin this loop.
            _ = connections.join_next(), if !connections.is_empty() => continue,
            _ = shutdown.wait_for(|&due| due) => break Ok(()),
        };
        let (stream, _peer) = match accepted {
            Ok(pair) => pair,
            Err(err) => break Err(err.into()),
        };

        let Ok(permit) = Arc::clone(&connection_limit).try_acquire_owned() else {
            telemetry.count("logit.input.connections.rejected", 1.0, &[("reason", "limit")]);
            // Dropped here, which closes the socket: carbon has no control message to reject with,
            // unlike `logit_in`'s `Reject{INTERNAL}`.
            drop(stream);
            continue;
        };

        let sink = sink.clone();
        let mut diag = diag.clone();
        let telemetry = telemetry.clone();
        // One decoder per connection: it carries the pickle machine's reusable stack/arenas/memo,
        // which are per-stream state and must not be shared. Its `Arc<Resource>` is the
        // component's one shared resource, so two connections' events can still land in one batch
        // downstream (`super`'s `resource` field).
        let decoder = GraphiteDecoder::new(Arc::clone(&resource))
            .with_protocol(config.protocol)
            .with_diagnostics(diag.clone())
            .with_telemetry(telemetry.clone());
        let live_connections = Arc::clone(&live_connections);
        let conn_shutdown = shutdown.clone();
        let conn_cancel = cancel_rx.clone();
        connections.spawn(async move {
            let _permit = permit; // held for the connection's lifetime; released on drop
            gauge_connections(&telemetry, &live_connections, 1);
            let result = serve_connection(
                stream,
                decoder,
                sink,
                config,
                &mut diag,
                telemetry.clone(),
                conn_shutdown,
                conn_cancel,
            )
            .await;
            gauge_connections(&telemetry, &live_connections, -1);
            if let Err(err) = result {
                diag.warn_throttled("connection_error", err);
            }
        });
    };

    // Whichever way the loop ended, this component is going away -- so tell the serving tasks so
    // themselves rather than assuming they already know. On the shutdown path they do (they watch
    // the component's signal too) and this is a no-op; on the `accept`-error path nothing else
    // ever would, and the drain below would never finish while any client stayed connected. Each
    // task still runs its final flush either way; `run_input`'s grace backstop bounds the wait.
    let _ = cancel.send(true);
    while connections.join_next().await.is_some() {}
    result
}

/// Adjusts the live-connection count and re-samples the gauge -- the identical
/// `fetch_add`/`gauge` ... `fetch_sub`/`gauge` pair `logit_in`'s `reject_or_serve` keeps around its
/// own serve call, factored out because both ends of it live in one `spawn` here.
fn gauge_connections(telemetry: &Telemetry, live: &AtomicI64, delta: i64) {
    let now = live.fetch_add(delta, Ordering::Relaxed) + delta;
    telemetry.gauge("logit.input.connections", now as f64, &[]);
}

/// Reads one connection to EOF, shutdown, or an error, decoding and batching as it goes.
///
/// The accumulator, the reused `scratch` `Vec<Event>` and the flush-deadline race are
/// `crate::udp::decode_loop`'s, reused rather than reinvented so a TCP connection assembles batches
/// by exactly the same rules a datagram listener does -- `batch_max_events`, `batch_max_bytes` and
/// `batch_flush_interval` mean the same thing on both. What differs is that the source is a socket
/// read rather than a queue pop, so there is no separate read loop and no queue between them.
///
/// Both stop signals -- the component's `shutdown` and [`run_accept_loop`]'s own `cancel` -- are
/// checked before each read and raced against it, never *during* the decode/send that follows: an
/// in-flight read's worth of lines is always finished and flushed, which is what makes "drains
/// within grace" true rather than best-effort.
///
/// **Every exit runs the final flush**, including a read error. Returning `?` straight out of the
/// read would silently drop everything this connection had decoded since its last flush -- up to
/// `batch_max_events` of it -- with no counter, and an abnormal close (a `SO_LINGER 0` RST from a
/// carbon client, say) is exactly when that is most likely. So the read's error is *stored*, the
/// loop breaks, the flush runs, and the stored outcome is what the caller sees and diagnoses as
/// `connection_error`.
#[allow(clippy::too_many_arguments)] // one helper is clearer than a params struct for 8 unrelated threaded-through values
async fn serve_connection(
    mut stream: TcpStream,
    mut decoder: GraphiteDecoder,
    sink: Fanout,
    config: ConnectionConfig,
    diag: &mut Diagnostics,
    telemetry: Telemetry,
    mut shutdown: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut accumulator = BatchAccumulator::new(config.batch_max_events, config.batch_max_bytes);
    // Reused across every `decode_into` call and cleared (not replaced) between them, so its
    // capacity survives from one read to the next -- `BatchAccumulator::absorb`'s own doc comment
    // explains why a `std::mem::take` anywhere in this loop would silently undo that.
    let mut scratch: Vec<Event> = Vec::new();
    let mut buf = BytesMut::with_capacity(READ_CHUNK_BYTES);
    // Set when a plaintext line passed `max_line_bytes` with no newline in it: everything up to and
    // including the next newline belongs to that abandoned line and is discarded uncounted (the
    // skip was counted once, when the bound was crossed).
    let mut draining = false;

    let has_interval = !config.batch_flush_interval.is_zero();
    let mut next_flush =
        has_interval.then(|| tokio::time::Instant::now() + config.batch_flush_interval);

    // Why this connection stopped, which is what the final flush is tagged with. `Closed` is the
    // default because it is what every *ordinary* exit is -- a clean FIN, a read error, an
    // oversize pickle frame -- and `FlushReason::Closed` means exactly that: one source among
    // several ended while the listener keeps running (`logit_pipeline::accumulator`'s own doc, and
    // the split `logit_inputs::tail` already uses). `Shutdown` is the whole component going away,
    // so only the stop-signal exits below claim it -- otherwise a healthy listener would report
    // `receive.flushed{reason="shutdown"}` every time a client hung up.
    let mut exit_reason = FlushReason::Closed;
    // A read error, held until after the flush -- see this function's doc comment.
    let mut outcome: anyhow::Result<()> = Ok(());

    loop {
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Interval).await;
                }
                next_flush = Some(BatchAccumulator::next_deadline(
                    deadline,
                    now_instant,
                    config.batch_flush_interval,
                ));
            }
        }

        // Checked explicitly rather than left to the `select!` below, for `logit_in`'s reason: a
        // `changed()` arm only fires on a transition this receiver hasn't observed yet, which
        // would miss "already stopping when this connection's loop started". Both `Ref`s are
        // dropped at the end of this statement, well before any `.await`.
        if *shutdown.borrow() || *cancel.borrow() {
            exit_reason = FlushReason::Shutdown;
            break;
        }

        buf.reserve(READ_CHUNK_BYTES);
        let read = match next_flush {
            None => tokio::select! {
                read = stream.read_buf(&mut buf) => read,
                _ = shutdown.changed() => { exit_reason = FlushReason::Shutdown; break }
                _ = cancel.changed() => { exit_reason = FlushReason::Shutdown; break }
            },
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
                tokio::select! {
                    read = tokio::time::timeout(wait, stream.read_buf(&mut buf)) => match read {
                        Ok(read) => read,
                        Err(_elapsed) => continue,
                    },
                    _ = shutdown.changed() => { exit_reason = FlushReason::Shutdown; break }
                    _ = cancel.changed() => { exit_reason = FlushReason::Shutdown; break }
                }
            }
        };
        match read {
            // Clean close -- the ordinary way a carbon sender ends a connection.
            Ok(0) => break,
            Ok(_) => {}
            // Stored rather than `?`d, so the flush below still runs -- see this function's doc.
            Err(err) => {
                outcome = Err(err.into());
                break;
            }
        }

        let received_at = now_nanos();
        let keep_open = match config.protocol {
            Protocol::Plaintext => {
                consume_lines(
                    Framing {
                        buf: &mut buf,
                        draining: &mut draining,
                        bound: config.max_line_bytes,
                    },
                    &mut decoder,
                    Assembly {
                        received_at,
                        scratch: &mut scratch,
                        accumulator: &mut accumulator,
                        sink: &sink,
                        telemetry: &telemetry,
                    },
                    diag,
                )
                .await;
                true
            }
            Protocol::Pickle => {
                consume_frames(
                    Framing {
                        buf: &mut buf,
                        draining: &mut draining,
                        bound: config.max_frame_bytes,
                    },
                    &mut decoder,
                    Assembly {
                        received_at,
                        scratch: &mut scratch,
                        accumulator: &mut accumulator,
                        sink: &sink,
                        telemetry: &telemetry,
                    },
                    diag,
                )
                .await
            }
        };
        if !keep_open {
            break;
        }
    }

    if let Some(batch) = accumulator.take() {
        emit(&sink, &telemetry, batch, exit_reason).await;
    }
    outcome
}

/// One connection's read buffer and the bound that applies to it -- `max_line_bytes` under
/// plaintext, `max_frame_bytes` under pickle. Grouped purely so the two framing functions below
/// take three arguments instead of nine (clippy's `too_many_arguments`, and a reader's patience).
struct Framing<'a> {
    buf: &'a mut BytesMut,
    draining: &'a mut bool,
    bound: usize,
}

/// The batch-assembly side of one read, grouped for [`Framing`]'s reason.
struct Assembly<'a> {
    received_at: i64,
    scratch: &'a mut Vec<Event>,
    accumulator: &'a mut BatchAccumulator,
    sink: &'a Fanout,
    telemetry: &'a Telemetry,
}

/// Hands the decoder everything in the buffer through its **last** newline, in one call, and
/// leaves the partial line after it behind.
///
/// One `decode_into` per read rather than per line is what keeps a busy connection's cost flat:
/// `GraphiteDecoder`'s plaintext path already splits a byte slice on `\n` and isolates each line's
/// failures (`logit_proto::graphite::decode`'s `decode_plaintext`), so re-splitting here would buy
/// nothing and cost a call per line. The slice is `split_to`n out of the read buffer rather than
/// copied, so the tag values the decoder slices out of it share this buffer's allocation
/// (`docs/design/memory.md` §2).
async fn consume_lines(
    framing: Framing<'_>,
    decoder: &mut GraphiteDecoder,
    assembly: Assembly<'_>,
    diag: &mut Diagnostics,
) {
    let Framing { buf, draining, bound } = framing;
    let Assembly { received_at, scratch, accumulator, sink, telemetry } = assembly;

    if *draining {
        match buf.iter().position(|b| *b == b'\n') {
            Some(at) => {
                buf.advance(at + 1);
                *draining = false;
            }
            None => {
                buf.clear();
                return;
            }
        }
    }

    if let Some(at) = buf.iter().rposition(|b| *b == b'\n') {
        let lines = buf[..=at].iter().filter(|b| **b == b'\n').count();
        let chunk = buf.split_to(at + 1).freeze();
        telemetry.count("logit.input.lines", lines as f64, &[]);
        telemetry.count("logit.input.line.bytes", chunk.len() as f64, &[]);
        // Infallible in practice: the plaintext path isolates every failure per line and counts it
        // itself, so there is nothing here to diagnose that the decoder hasn't already.
        scratch.clear();
        if let Ok((resource, scope)) = decoder.decode_into(chunk, received_at, scratch) {
            absorb(accumulator, resource, scope, scratch, sink, telemetry).await;
        }
    }

    // Whatever is left holds no newline at all, so it is one unterminated line. Past the bound it
    // can only grow, and nothing says how far away its end is -- so it is abandoned now, counted
    // once, and everything up to the next newline is discarded as it arrives.
    if buf.len() > bound {
        telemetry.count("logit.input.metrics.skipped", 1.0, &[("reason", "oversize_line")]);
        diag.warn_throttled(
            "oversize_line",
            format_args!(
                "graphite: a line passed max_line_bytes ({bound}) with no newline; skipping it \
                 and draining to the next one"
            ),
        );
        buf.clear();
        *draining = true;
    }
}

/// Reads as many complete length-prefixed pickle frames out of the buffer as it holds. Returns
/// `false` when the connection must be closed -- a frame declaring more than `max_frame_bytes`.
///
/// Carbon's framing is Twisted's `Int32StringReceiver`: a 4-byte **big-endian** payload length,
/// then that many bytes. The prefix is validated and stripped here so
/// [`GraphiteDecoder::decode_into`] is handed exactly one already-unframed payload, which is what
/// its pickle path expects (`logit_proto::graphite::decode`'s module doc).
///
/// A frame that is merely *malformed* (a disallowed opcode, a depth or item cap) drops that frame
/// and keeps the connection: its declared length already said where the next one starts. An
/// oversize length is the case with no such recovery -- nothing after it has been read, so there is
/// no resync point at all -- and is why this returns a bool.
async fn consume_frames(
    framing: Framing<'_>,
    decoder: &mut GraphiteDecoder,
    assembly: Assembly<'_>,
    diag: &mut Diagnostics,
) -> bool {
    let Framing { buf, bound, .. } = framing;
    let Assembly { received_at, scratch, accumulator, sink, telemetry } = assembly;

    while buf.len() >= LENGTH_PREFIX_BYTES {
        let mut prefix = [0u8; LENGTH_PREFIX_BYTES];
        prefix.copy_from_slice(&buf[..LENGTH_PREFIX_BYTES]);
        let payload_len = u32::from_be_bytes(prefix) as usize;
        if payload_len > bound {
            diag.warn_throttled(
                "oversize_frame",
                format_args!(
                    "graphite: a pickle frame declared {payload_len} bytes, past max_frame_bytes \
                     ({bound}); closing the connection, since a length-framed stream has no \
                     resync point"
                ),
            );
            return false;
        }
        if buf.len() < LENGTH_PREFIX_BYTES + payload_len {
            break; // the rest of this frame hasn't arrived yet
        }
        buf.advance(LENGTH_PREFIX_BYTES);
        let payload = buf.split_to(payload_len).freeze();
        telemetry.count("logit.input.frames", 1.0, &[]);
        telemetry.count("logit.input.frame.bytes", (LENGTH_PREFIX_BYTES + payload_len) as f64, &[]);
        // An `Err` is a frame the restricted pickle reader refused; it has already counted and
        // diagnosed it (`bad_pickle`), and whatever it salvaged before refusing stays in `scratch`
        // to be discarded by the next iteration's `clear` rather than delivered half-decoded.
        scratch.clear();
        if let Ok((resource, scope)) = decoder.decode_into(payload, received_at, scratch) {
            absorb(accumulator, resource, scope, scratch, sink, telemetry).await;
        }
    }
    true
}

/// Moves `scratch`'s events into the accumulator (`absorb` drains it) and sends whatever that
/// completes.
async fn absorb(
    accumulator: &mut BatchAccumulator,
    resource: Arc<Resource>,
    scope: Option<Arc<Scope>>,
    scratch: &mut Vec<Event>,
    sink: &Fanout,
    telemetry: &Telemetry,
) {
    if let Some((batch, reason)) = accumulator.absorb(resource, scope, scratch) {
        emit(sink, telemetry, batch, reason).await;
    }
}

/// The same one-line helper `crate::udp`'s `decode_loop` uses, for the same reason: every batch
/// this listener emits is counted under the reason that completed it, so
/// `logit.component.receive.flushed{reason}` reads identically for a TCP connection and a datagram
/// socket.
async fn emit(sink: &Fanout, telemetry: &Telemetry, batch: EventBatch, reason: FlushReason) {
    telemetry.count("logit.component.receive.flushed", 1.0, &[("reason", reason.as_str())]);
    sink.send(batch).await;
}

fn now_nanos() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64
}
