use super::*;
use crate::tail::test_support::scratch_dir;
use crate::tail::{LineSplitter, ReadFrom, Split, TailBatching, WatchMode};
use logit_pipeline::test_util::{
    assert_no_batch, fanout_channel, recv_events, spawn_input, wait_until, TelemetryProbe,
};
use std::io::Write;

/// 2026-09-21T13:33:20.123456789Z
const T0: i64 = 1_789_997_600_123_456_789;
const TS: &str = "2026-09-21T13:33:20.123456789Z";
/// One second after [`TS`], as CRI-O writes it.
const TS1: &str = "2026-09-21T13:33:21.123456789+00:00";

fn rec(stream: &str, tag: &str, content: &str) -> Bytes {
    Bytes::from(format!("{TS} {stream} {tag} {content}"))
}

fn decoder(max_line_bytes: usize) -> CriDecoder {
    CriDecoder::new(Arc::new(Resource::default()), max_line_bytes, Duration::from_secs(5))
}

fn probed(max_line_bytes: usize, probe: &mut TelemetryProbe) -> CriDecoder {
    let telemetry = probe.telemetry("pods", "cri_in", "listener");
    decoder(max_line_bytes)
        .with_telemetry(telemetry.clone())
        .with_diagnostics(Diagnostics::new("pods").with_telemetry(telemetry))
}

fn decode(d: &mut CriDecoder, line: Bytes, out: &mut Vec<Event>) {
    d.decode_line(line, 7, out).expect("a CRI record");
}

/// Each event's `(message, log.iostream)`.
fn lines(events: &[Event]) -> Vec<(String, String)> {
    events
        .iter()
        .map(|e| {
            let message = e.log.as_ref().unwrap().message.as_str().unwrap().to_string();
            let stream = e.attributes.get("log.iostream").and_then(|v| v.as_str()).unwrap();
            (message, stream.to_string())
        })
        .collect()
}

fn pairs(want: &[(&str, &str)]) -> Vec<(String, String)> {
    want.iter().map(|(m, s)| (m.to_string(), s.to_string())).collect()
}

fn dropped(probe: &mut TelemetryProbe, reason: &str) -> f64 {
    probe.sum("logit.input.frames.dropped", &[("reason", reason)])
}

fn diagnosed(probe: &mut TelemetryProbe, key: &str) -> f64 {
    probe.sum("logit.component.diagnostics", &[("key", key)])
}

// -- CriDecoder ------------------------------------------------------------------------------

#[test]
fn a_full_line_is_one_event_with_its_record_time_and_stream_and_no_copy() {
    let mut d = decoder(1024);
    let mut out = Vec::new();
    let line = rec("stderr", "F", "hello world");
    let content_at = line.as_ptr() as usize + line.len() - "hello world".len();
    decode(&mut d, line, &mut out);
    assert_eq!(lines(&out), pairs(&[("hello world", "stderr")]));
    assert_eq!(out[0].timestamp, T0);
    let Value::Str(message) = &out[0].log.as_ref().unwrap().message else { panic!() };
    assert_eq!(message.as_ptr() as usize, content_at, "a slice of the read");
    assert_eq!(out[0].log.as_ref().unwrap().body_format, BodyFormat::Raw);
    assert!(!d.holds_entry());
}

/// A run's event carries its first record's time, not its last's.
#[test]
fn a_p_run_is_reassembled_at_its_f_with_the_first_records_time() {
    let mut d = decoder(1024);
    let mut out = Vec::new();
    decode(&mut d, rec("stdout", "P", "one "), &mut out);
    assert!(out.is_empty() && d.holds_entry());
    decode(&mut d, Bytes::from(format!("{TS1} stdout P two ")), &mut out);
    decode(&mut d, Bytes::from(format!("{TS1} stdout F:x three")), &mut out);
    assert_eq!(lines(&out), pairs(&[("one two three", "stdout")]));
    assert_eq!(out[0].timestamp, T0);
    assert!(!d.holds_entry());
}

#[test]
fn interleaved_streams_reassemble_separately() {
    let mut d = decoder(1024);
    let mut out = Vec::new();
    decode(&mut d, rec("stdout", "P", "out-a "), &mut out);
    decode(&mut d, rec("stderr", "P", "err-a "), &mut out);
    decode(&mut d, rec("stderr", "F", "err-b"), &mut out);
    decode(&mut d, rec("stdout", "F", "out-b"), &mut out);
    assert_eq!(lines(&out), pairs(&[("err-a err-b", "stderr"), ("out-a out-b", "stdout")]));
}

#[test]
fn a_trailing_cr_and_empty_content_stay_as_written() {
    let mut d = decoder(1024);
    let mut out = Vec::new();
    decode(&mut d, rec("stdout", "F", "crlf\r"), &mut out);
    decode(&mut d, rec("stdout", "P", "mid\r"), &mut out);
    decode(&mut d, rec("stdout", "F", "dle"), &mut out);
    decode(&mut d, rec("stdout", "F", ""), &mut out);
    assert_eq!(lines(&out), pairs(&[("crlf\r", "stdout"), ("mid\rdle", "stdout"), ("", "stdout")]));
}

/// A character cut between two pieces is whole in the line, and an invalid byte is replaced once,
/// diagnosed.
#[test]
fn utf8_is_checked_once_over_the_reassembled_line() {
    let mut probe = TelemetryProbe::new();
    let mut d = probed(1024, &mut probe);
    let mut out = Vec::new();
    let mut first = format!("{TS} stdout P caf").into_bytes();
    first.push(0xC3);
    let mut second = format!("{TS} stdout F ").into_bytes();
    second.extend_from_slice(&[0xA9, b'!']);
    decode(&mut d, Bytes::from(first), &mut out);
    decode(&mut d, Bytes::from(second), &mut out);
    assert_eq!(lines(&out), pairs(&[("café!", "stdout")]));
    assert_eq!(diagnosed(&mut probe, "invalid_utf8"), 0.0);

    let mut bad = format!("{TS} stderr F x").into_bytes();
    bad.push(0xFF);
    decode(&mut d, Bytes::from(bad), &mut out);
    assert_eq!(lines(&out[1..]), pairs(&[("x\u{FFFD}", "stderr")]));
    assert_eq!(diagnosed(&mut probe, "invalid_utf8"), 1.0);
}

#[test]
fn an_unreadable_timestamp_falls_back_to_read_time_with_bad_time() {
    let mut probe = TelemetryProbe::new();
    let mut d = probed(1024, &mut probe);
    let mut out = Vec::new();
    decode(&mut d, Bytes::from_static(b"yesterday stdout P a"), &mut out);
    // A later piece's timestamp isn't read: the line's time is its first record's.
    decode(&mut d, Bytes::from_static(b"never stdout F b"), &mut out);
    assert_eq!(lines(&out), pairs(&[("ab", "stdout")]));
    assert_eq!(out[0].timestamp, 7);
    assert_eq!(diagnosed(&mut probe, "bad_time"), 1.0);
}

/// The bound is checked before each append: a run as long as the bound is kept, one byte over
/// drops the line, whose later pieces are discarded through its `F`, counted once.
#[test]
fn the_bound_is_checked_before_each_append_and_a_drop_counts_once() {
    let mut probe = TelemetryProbe::new();
    let mut d = probed(6, &mut probe);
    let mut out = Vec::new();
    decode(&mut d, rec("stdout", "P", "abc"), &mut out);
    decode(&mut d, rec("stdout", "F", "def"), &mut out);
    assert_eq!(lines(&out), pairs(&[("abcdef", "stdout")]), "at the bound");

    decode(&mut d, rec("stdout", "P", "abc"), &mut out);
    decode(&mut d, rec("stderr", "P", "e"), &mut out);
    decode(&mut d, rec("stdout", "P", "defg"), &mut out);
    assert!(d.holds_entry(), "dropping until the F");
    decode(&mut d, rec("stdout", "P", "more"), &mut out);
    decode(&mut d, rec("stdout", "F", "end"), &mut out);
    decode(&mut d, rec("stderr", "F", "rr"), &mut out);
    decode(&mut d, rec("stdout", "F", "ok"), &mut out);
    decode(&mut d, rec("stdout", "F", "1234567"), &mut out);
    assert_eq!(lines(&out[1..]), pairs(&[("err", "stderr"), ("ok", "stdout")]));
    assert_eq!(dropped(&mut probe, "oversize"), 2.0);
    assert_eq!(diagnosed(&mut probe, "long_line"), 2.0);
    assert!(!d.holds_entry());
}

/// A record the splitter dropped drops its stream's held run and the rest of the line through
/// its `F`, counted once. A `P` of a stream already dropping isn't counted again, and a dropped
/// `F` ends the drop, whether the drop began with it or earlier.
#[test]
fn a_splitter_drop_drops_the_streams_line_through_its_f_counted_once() {
    let mut probe = TelemetryProbe::new();
    let mut d = probed(1024, &mut probe);
    let mut out = Vec::new();
    let head = |stream: &str, tag: &str| format!("{TS} {stream} {tag} xxxx").into_bytes();

    decode(&mut d, rec("stdout", "P", "held "), &mut out);
    assert!(d.line_dropped(&head("stdout", "P"), 0, &mut out));
    assert!(d.line_dropped(&head("stdout", "P"), 0, &mut out), "already dropping: taken");
    decode(&mut d, rec("stderr", "F", "other"), &mut out);
    decode(&mut d, rec("stdout", "P", "discarded"), &mut out);
    assert!(d.holds_entry());
    assert!(d.line_dropped(&head("stdout", "F"), 0, &mut out), "a dropped F ends the drop");
    assert!(!d.holds_entry());
    decode(&mut d, rec("stdout", "F", "next"), &mut out);
    assert_eq!(lines(&out), pairs(&[("other", "stderr"), ("next", "stdout")]));
    assert_eq!(dropped(&mut probe, "oversize"), 1.0);
    assert_eq!(diagnosed(&mut probe, "long_line"), 1.0);

    // A dropped F with no drop in progress is a whole line, counted, with nothing to discard.
    decode(&mut d, rec("stdout", "P", "held "), &mut out);
    assert!(d.line_dropped(&head("stdout", "F"), 0, &mut out));
    assert!(!d.holds_entry());
    decode(&mut d, rec("stdout", "F", "after"), &mut out);
    assert_eq!(lines(&out[2..]), pairs(&[("after", "stdout")]));
    assert_eq!(dropped(&mut probe, "oversize"), 2.0);
}

/// A head that doesn't parse leaves the count to the driver and flushes both streams, as a
/// malformed record does.
#[test]
fn an_unparseable_splitter_drop_flushes_both_streams_and_is_left_to_the_driver() {
    let mut probe = TelemetryProbe::new();
    let mut d = probed(1024, &mut probe);
    let mut out = Vec::new();
    decode(&mut d, rec("stderr", "P", "e"), &mut out);
    decode(&mut d, rec("stdout", "P", "o"), &mut out);
    assert!(!d.line_dropped(b"garbage with no stream", 0, &mut out));
    assert_eq!(lines(&out), pairs(&[("o", "stdout"), ("e", "stderr")]));
    assert!(!d.holds_entry());
    assert_eq!(dropped(&mut probe, "oversize"), 0.0);
}

#[test]
fn the_head_parse_reads_only_the_tags_first_byte() {
    let h = |s: &str| parse_head(s.as_bytes());
    assert_eq!(h("ts stdout P"), Some((Stream::Stdout, true)), "cut right after the tag");
    assert_eq!(h("ts stderr P:x:y"), Some((Stream::Stderr, true)), "no space after the tag");
    assert_eq!(h("ts stdout P content"), Some((Stream::Stdout, true)));
    assert_eq!(h("ts stdout F"), Some((Stream::Stdout, false)));
    assert_eq!(h("ts stdout PX y"), Some((Stream::Stdout, false)), "only a bare P is partial");
    assert_eq!(h("ts stdout  y"), Some((Stream::Stdout, false)), "an empty tag is full");
    assert_eq!(h("ts stdout "), None, "no tag byte to read");
    assert_eq!(h("ts stdin P y"), None);
    assert_eq!(h("ts stdout"), None);
    assert_eq!(h("ts"), None);
}

#[test]
fn a_malformed_record_flushes_both_streams_stdout_first_and_counts_malformed() {
    let mut probe = TelemetryProbe::new();
    let mut d = probed(1024, &mut probe);
    let mut out = Vec::new();
    decode(&mut d, rec("stderr", "P", "e"), &mut out);
    decode(&mut d, rec("stdout", "P", "o"), &mut out);
    let err = d.decode_line(Bytes::from_static(b"{\"log\":\"x\"}"), 0, &mut out).unwrap_err();
    assert!(matches!(err, logit_proto::CodecError::Malformed(_)));
    assert_eq!(lines(&out), pairs(&[("o", "stdout"), ("e", "stderr")]));
    assert_eq!(dropped(&mut probe, "malformed"), 1.0);
    assert!(!d.holds_entry());
}

#[test]
fn reset_counts_each_held_run_as_one_truncated_frame() {
    let mut probe = TelemetryProbe::new();
    let mut d = probed(4, &mut probe);
    let mut out = Vec::new();
    d.reset();
    assert_eq!(dropped(&mut probe, "truncated"), 0.0);
    decode(&mut d, rec("stdout", "P", "a"), &mut out);
    decode(&mut d, rec("stdout", "P", "b"), &mut out);
    decode(&mut d, rec("stderr", "P", "c"), &mut out);
    d.reset();
    assert_eq!(dropped(&mut probe, "truncated"), 2.0);
    decode(&mut d, rec("stdout", "P", "too long"), &mut out);
    d.reset();
    assert_eq!(dropped(&mut probe, "truncated"), 2.0, "a stream mid-drop was counted oversize");
    assert!(!d.holds_entry());
    decode(&mut d, rec("stdout", "F", "ok"), &mut out);
    assert_eq!(lines(&out), pairs(&[("ok", "stdout")]));
}

#[test]
fn close_emits_held_runs_and_keeps_a_drop() {
    let mut d = decoder(4);
    let mut out = Vec::new();
    decode(&mut d, rec("stdout", "P", "too long"), &mut out);
    decode(&mut d, rec("stderr", "P", "e"), &mut out);
    d.close(&mut out);
    assert_eq!(lines(&out), pairs(&[("e", "stderr")]));
    assert!(d.holds_entry(), "the stdout drop outlives close");
}

/// Each stream expires by its own last record: a stdout run left at 0 s is flushed at 5 s while
/// stderr's, last extended at 3 s, stays held until 8 s.
#[tokio::test(start_paused = true)]
async fn the_idle_flush_expires_each_stream_by_its_own_last_record() {
    let mut probe = TelemetryProbe::new();
    let mut d = probed(1024, &mut probe);
    let mut out = Vec::new();
    decode(&mut d, rec("stdout", "P", "prompt> "), &mut out);
    decode(&mut d, rec("stderr", "P", "e1 "), &mut out);
    tokio::time::advance(Duration::from_secs(3)).await;
    decode(&mut d, rec("stderr", "P", "e2 "), &mut out);
    tokio::time::advance(Duration::from_millis(1999)).await;
    d.flush_idle(Instant::now(), &mut out);
    assert!(out.is_empty(), "nothing is 5 s old yet");

    tokio::time::advance(Duration::from_millis(1)).await;
    d.flush_idle(Instant::now(), &mut out);
    assert_eq!(lines(&out), pairs(&[("prompt> ", "stdout")]));
    assert!(d.holds_entry(), "stderr's run is 2 s old");
    let truncated = |p: &mut TelemetryProbe| {
        p.sum("logit.input.frames.truncated", &[("reason", "partial_timeout")])
    };
    assert_eq!(truncated(&mut probe), 1.0);

    tokio::time::advance(Duration::from_secs(3)).await;
    d.flush_idle(Instant::now(), &mut out);
    assert_eq!(lines(&out[1..]), pairs(&[("e1 e2 ", "stderr")]));
    assert!(!d.holds_entry());
    assert_eq!(truncated(&mut probe), 2.0);
    assert_eq!(probe.sum("logit.input.frames.dropped", &[]), 0.0, "a flush isn't loss");
}

/// The flush ends a drop in progress, so the stream's next record starts a new line, and a
/// `partial_timeout` of `0s` never flushes.
#[tokio::test(start_paused = true)]
async fn the_idle_flush_ends_a_drop_and_is_off_at_zero() {
    let mut d = decoder(4);
    let mut out = Vec::new();
    decode(&mut d, rec("stdout", "P", "too long"), &mut out);
    tokio::time::advance(Duration::from_secs(5)).await;
    d.flush_idle(Instant::now(), &mut out);
    assert!(out.is_empty() && !d.holds_entry());
    decode(&mut d, rec("stdout", "F", "new"), &mut out);
    assert_eq!(lines(&out), pairs(&[("new", "stdout")]));

    let mut d = CriDecoder::new(Arc::new(Resource::default()), 1024, Duration::ZERO);
    let mut out = Vec::new();
    decode(&mut d, rec("stdout", "P", "held"), &mut out);
    tokio::time::advance(Duration::from_secs(3600)).await;
    d.flush_idle(Instant::now(), &mut out);
    assert!(out.is_empty() && d.holds_entry());
}

#[test]
fn the_resource_comes_from_the_path() {
    let r =
        path_resource(Path::new("/var/log/pods/kube-system_etcd-0_9f1e-22/etcd/3.log")).unwrap();
    let get = |k: &str| r.attributes.get(k).cloned();
    assert_eq!(get("k8s.namespace.name"), Some(Value::str("kube-system")));
    assert_eq!(get("k8s.pod.name"), Some(Value::str("etcd-0")));
    assert_eq!(get("k8s.pod.uid"), Some(Value::str("9f1e-22")));
    assert_eq!(get("k8s.container.name"), Some(Value::str("etcd")));
    assert_eq!(get("k8s.container.restart_count"), Some(Value::I64(3)));
    assert_eq!(r.attributes.len(), 5);

    let rotated =
        path_resource(Path::new("/p/kube-system_etcd-0_9f1e-22/etcd/3.log.20261030-120000"))
            .unwrap();
    assert_eq!(rotated, r, "a rotated file has its live file's resource");
    let static_uid = path_resource(Path::new("/p/ns_pod_my_uid/c/0.log")).unwrap();
    assert_eq!(static_uid.attributes.get("k8s.pod.uid"), Some(&Value::str("my_uid")));
    assert!(path_resource(Path::new("/p/ns_pod/c/0.log")).is_none());
    assert!(path_resource(Path::new("/p/ns_pod_uid/c/0.log.gz")).is_none());
}

proptest::proptest! {
    /// The splitter and the decoder together, through the hook, against a per-stream model: a
    /// record over the splitter's bound and a run over the decoder's bound both drop their
    /// line through its `F`, counted once per line, whatever the read chunking.
    #[test]
    fn splitter_drops_and_appends_reassemble_as_the_per_stream_model_says(
        records in proptest::collection::vec((proptest::bool::ANY, proptest::bool::ANY, 0usize..48), 0..40),
        chunk in 1usize..97,
    ) {
        // `<TS> stdout P ` is 40 bytes, so a record over the splitter's 70 holds over 30.
        const SPLIT: usize = 70;
        const MAX: usize = 40;
        let mut file = Vec::new();
        let mut model = Vec::new();
        let mut state: [(Option<String>, bool); 2] = Default::default();
        let mut model_drops = 0;
        for (i, &(stderr, partial, len)) in records.iter().enumerate() {
            let stream = if stderr { "stderr" } else { "stdout" };
            let content: String = (0..len).map(|j| (b'a' + ((i + j) % 26) as u8) as char).collect();
            let line = format!("{TS} {stream} {} {content}", if partial { "P" } else { "F" });
            file.extend_from_slice(line.as_bytes());
            file.push(b'\n');
            let s = &mut state[usize::from(stderr)];
            if s.1 {
                s.1 = partial;
                continue;
            }
            let held = s.0.as_ref().map_or(0, String::len);
            if line.len() > SPLIT || held + len > MAX {
                s.0 = None;
                s.1 = partial;
                model_drops += 1;
                continue;
            }
            let run = s.0.get_or_insert_with(String::new);
            run.push_str(&content);
            if !partial {
                model.push((s.0.take().unwrap(), stream.to_string()));
            }
        }
        for (i, stream) in ["stdout", "stderr"].iter().enumerate() {
            if let Some(run) = state[i].0.take() {
                model.push((run, stream.to_string()));
            }
        }

        let mut probe = TelemetryProbe::new();
        let mut d = probed(MAX, &mut probe);
        let mut splitter = LineSplitter::with_options(SPLIT, false, true);
        let mut out = Vec::new();
        let mut driver_drops = 0;
        for piece in file.chunks(chunk) {
            let mut items = Vec::new();
            splitter.push_split(Bytes::copy_from_slice(piece), |item| items.push(item));
            for item in items {
                match item {
                    Split::Line(line, _) => decode(&mut d, line, &mut out),
                    Split::Dropped { head, .. } => {
                        if !d.line_dropped(&head, 0, &mut out) {
                            driver_drops += 1;
                        }
                    }
                }
            }
        }
        d.close(&mut out);
        proptest::prop_assert_eq!(lines(&out), model);
        proptest::prop_assert_eq!(driver_drops, 0);
        proptest::prop_assert_eq!(dropped(&mut probe, "oversize"), f64::from(model_drops));
    }
}

// -- CriInput on a scratch pods tree ---------------------------------------------------------

fn fast_config() -> TailConfig {
    TailConfig {
        checkpoint_path: None,
        read_from: ReadFrom::Beginning,
        watch: WatchMode::Poll,
        poll_interval: Duration::from_millis(15),
        checkpoint_interval: Duration::from_secs(5),
        max_line_bytes: 1024 * 1024,
        batching: TailBatching {
            max_events: 1,
            max_bytes: 1024 * 1024,
            flush_interval: Duration::from_millis(15),
        },
    }
}

/// `<root>/default_web-0_0123-abcd/nginx/`, created.
fn container_dir(root: &Path) -> PathBuf {
    let dir = root.join("default_web-0_0123-abcd").join("nginx");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn append(path: &Path, text: &str) {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap()
        .write_all(text.as_bytes())
        .unwrap();
}

fn record(stream: &str, tag: &str, content: &str) -> String {
    format!("{TS} {stream} {tag} {content}\n")
}

fn input(
    root: &Path,
    partial_timeout: Duration,
    config: TailConfig,
    probe: &mut TelemetryProbe,
) -> CriInput {
    let telemetry = probe.telemetry("pods", "cri_in", "listener");
    CriInput::new(root.to_path_buf(), root.join("no-links"), partial_timeout, config)
        .with_diagnostics(Diagnostics::new("pods").with_telemetry(telemetry.clone()))
        .with_telemetry(telemetry)
}

#[tokio::test]
async fn cri_in_reads_a_pods_tree_with_its_path_resource_and_keeps_a_cr() {
    let root = scratch_dir("cri-in-basic");
    let dir = container_dir(&root);
    append(&dir.join("2.log"), &record("stdout", "P", "a "));
    append(&dir.join("2.log"), &record("stderr", "F", "crlf\r"));
    append(&dir.join("2.log"), &record("stdout", "F", "b"));
    append(&dir.join("2.log.20261029-101010.gz"), &record("stdout", "F", "never read"));
    append(&dir.join("2.log.tmp"), &record("stdout", "F", "never read"));

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root, Duration::from_secs(5), fast_config(), &mut probe), fanout).await;
    let events = recv_events(&mut rx, 2).await;
    assert_eq!(lines(&events), pairs(&[("crlf\r", "stderr"), ("a b", "stdout")]));
    assert_eq!(events[0].timestamp, T0);
    assert_no_batch(&mut rx, Duration::from_millis(60), "no other file is read").await;
    running.stop().await;
    assert_eq!(probe.sum("logit.input.files.skipped", &[]), 0.0, "a .gz isn't even opened");

    // The resource: tested through `recv_batch` would need one batch per event, so read it from
    // a fresh run's batch instead.
    let (fanout, mut rx) = fanout_channel(8);
    let running =
        spawn_input(input(&root, Duration::from_secs(5), fast_config(), &mut probe), fanout).await;
    let batch = logit_pipeline::test_util::recv_batch(&mut rx).await;
    let attr = |k: &str| batch.resource.attributes.get(k).cloned();
    assert_eq!(attr("k8s.pod.name"), Some(Value::str("web-0")));
    assert_eq!(attr("k8s.container.restart_count"), Some(Value::I64(2)));
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
}

/// A `P` with no `F` is sent as it stands once the reader is at the file's end and the stream's
/// last record is `partial_timeout` old, counted `frames.truncated`, not `*.dropped`.
#[tokio::test]
async fn a_dangling_run_is_flushed_after_partial_timeout() {
    let root = scratch_dir("cri-in-idle");
    let log = container_dir(&root).join("0.log");
    append(&log, &record("stdout", "P", "exited mid-line"));
    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root, Duration::from_millis(30), fast_config(), &mut probe), fanout)
            .await;
    let events = recv_events(&mut rx, 1).await;
    assert_eq!(lines(&events), pairs(&[("exited mid-line", "stdout")]));
    assert_eq!(probe.sum("logit.input.frames.truncated", &[("reason", "partial_timeout")]), 1.0);
    assert_eq!(probe.sum("logit.input.frames.dropped", &[]), 0.0);

    // The continuation is a line of its own.
    append(&log, &record("stdout", "F", " rest"));
    let events = recv_events(&mut rx, 1).await;
    assert_eq!(lines(&events), pairs(&[(" rest", "stdout")]));
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
}

/// A reader behind its file never flushes a run whose `F` is already there: with a backlog, no
/// read reaches the end until the `F` is read, whatever the ticks in between.
#[tokio::test]
async fn a_reader_behind_its_file_never_flushes_a_run() {
    let root = scratch_dir("cri-in-behind");
    let log = container_dir(&root).join("0.log");
    let mut text = record("stdout", "P", "head ");
    let filler = record("stderr", "F", &"x".repeat(200));
    for _ in 0..20_000 {
        text.push_str(&filler);
    }
    text.push_str(&record("stdout", "F", "tail"));
    append(&log, &text);

    let config = TailConfig {
        poll_interval: Duration::from_millis(1),
        batching: TailBatching { max_events: 1_000, ..fast_config().batching },
        ..fast_config()
    };
    let (fanout, mut rx) = fanout_channel(1024);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root, Duration::from_nanos(1), config, &mut probe), fanout).await;
    let events = recv_events(&mut rx, 20_001).await;
    assert_eq!(lines(&events[20_000..]), pairs(&[("head tail", "stdout")]));
    assert_eq!(probe.sum("logit.input.frames.truncated", &[]), 0.0);
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
}

/// A run at the file's end whose `F` lands before the next tick: the tick sees the unread bytes
/// through the handle's length and leaves them to the read, even though the run's last record
/// is older than `partial_timeout`.
#[tokio::test]
async fn unread_bytes_at_the_tick_are_read_before_any_flush() {
    let root = scratch_dir("cri-in-unread");
    let log = container_dir(&root).join("0.log");
    append(&log, &record("stdout", "P", "head "));
    // Poll only, no flush timer: each tick runs the idle pass before the read, and nothing else
    // reads between ticks.
    let config = TailConfig {
        poll_interval: Duration::from_millis(500),
        batching: TailBatching { flush_interval: Duration::ZERO, ..fast_config().batching },
        ..fast_config()
    };
    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root, Duration::from_millis(1), config, &mut probe), fanout).await;
    probe.wait_for("the P to be read", |t| t.sum("logit.input.lines", &[]) >= 1.0).await;
    append(&log, &record("stdout", "F", "tail"));
    let events = recv_events(&mut rx, 1).await;
    assert_eq!(lines(&events), pairs(&[("head tail", "stdout")]));
    assert_eq!(probe.sum("logit.input.frames.truncated", &[]), 0.0);
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
}

/// A record over the splitter's bound (`envelope_cap(max_line_bytes)`) drops its stream's whole
/// line through its `F`, counted once, and the other stream's line is untouched.
#[tokio::test]
async fn a_record_over_the_splitter_bound_drops_its_line_once() {
    let root = scratch_dir("cri-in-splitter-drop");
    let log = container_dir(&root).join("0.log");
    let config = TailConfig { max_line_bytes: 1024, ..fast_config() };
    let huge = "y".repeat(envelope_cap(1024) + 1);
    let mut text = record("stdout", "P", "held ");
    text.push_str(&record("stderr", "P", "other "));
    text.push_str(&record("stdout", "P", &huge));
    text.push_str(&record("stdout", "P", "discarded"));
    text.push_str(&record("stderr", "F", "line"));
    text.push_str(&record("stdout", "F", "discarded too"));
    text.push_str(&record("stdout", "F", "next"));
    append(&log, &text);

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running = spawn_input(input(&root, Duration::ZERO, config, &mut probe), fanout).await;
    let events = recv_events(&mut rx, 2).await;
    assert_eq!(lines(&events), pairs(&[("other line", "stderr"), ("next", "stdout")]));
    assert_no_batch(&mut rx, Duration::from_millis(60), "the dropped line's tail").await;
    assert_eq!(probe.sum("logit.input.frames.dropped", &[("reason", "oversize")]), 1.0);
    assert_eq!(diagnosed(&mut probe, "long_line"), 1.0);
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
}

/// A run held at a clean stop stays unemitted when a checkpoint will replay it, and the restart
/// reads it whole. With no checkpoint it's emitted as it stands.
#[tokio::test]
async fn a_held_run_survives_a_clean_stop_with_a_checkpoint() {
    let root = scratch_dir("cri-in-shutdown");
    let log = container_dir(&root).join("0.log");
    append(&log, &record("stdout", "F", "before"));
    append(&log, &record("stdout", "P", "held "));
    let config = TailConfig { checkpoint_path: Some(root.join("ckpt.json")), ..fast_config() };

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root, Duration::ZERO, config.clone(), &mut probe), fanout).await;
    assert_eq!(lines(&recv_events(&mut rx, 1).await), pairs(&[("before", "stdout")]));
    probe.wait_for("the P to be read", |t| t.sum("logit.input.lines", &[]) >= 2.0).await;
    running.stop().await;
    // The stop dropped the sender, so this sees only what was sent before it.
    assert_no_batch(&mut rx, Duration::from_millis(1), "the held run at a clean stop").await;

    append(&log, &record("stdout", "F", "rest"));
    let (fanout, mut rx) = fanout_channel(8);
    let running = spawn_input(input(&root, Duration::ZERO, config, &mut probe), fanout).await;
    assert_eq!(lines(&recv_events(&mut rx, 1).await), pairs(&[("held rest", "stdout")]));
    running.stop().await;

    // No checkpoint: nothing would re-read it, so the stop emits it.
    let root2 = scratch_dir("cri-in-shutdown-nockpt");
    append(&container_dir(&root2).join("0.log"), &record("stdout", "P", "alone"));
    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root2, Duration::ZERO, fast_config(), &mut probe), fanout).await;
    probe.wait_for("the P to be read", |t| t.sum("logit.input.lines", &[]) >= 1.0).await;
    running.stop().await;
    let events = recv_events(&mut rx, 1).await;
    assert_eq!(lines(&events), pairs(&[("alone", "stdout")]));

    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&root2).ok();
}

/// The kubelet's rotation renames the live file while a run is held: the inode rebinds to the
/// rotated name with its decoder, the runtime's `F` into the renamed file finishes the line, and
/// the new live file is read from its start, with no line read twice.
#[tokio::test]
async fn a_rename_rotation_keeps_a_held_run() {
    let root = scratch_dir("cri-in-rotate");
    let dir = container_dir(&root);
    let live = dir.join("0.log");
    append(&live, &record("stdout", "P", "held "));
    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root, Duration::ZERO, fast_config(), &mut probe), fanout).await;
    probe.wait_for("the P to be read", |t| t.sum("logit.input.lines", &[]) >= 1.0).await;

    let rotated = dir.join("0.log.20261030-120000");
    std::fs::rename(&live, &rotated).unwrap();
    probe
        .wait_for("the rename to rebind", |t| {
            t.sum("logit.component.diagnostics", &[("key", "renamed")]) >= 1.0
        })
        .await;
    append(&rotated, &record("stdout", "F", "rest"));
    append(&live, &record("stdout", "F", "new file"));
    let mut events = recv_events(&mut rx, 2).await;
    events.sort_by_key(|e| e.log.as_ref().unwrap().message.as_str().unwrap().to_string());
    assert_eq!(lines(&events), pairs(&[("held rest", "stdout"), ("new file", "stdout")]));
    assert_no_batch(&mut rx, Duration::from_millis(60), "no line twice").await;
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
}

/// A restart after the kubelet renamed a file resumes it under its rotated name from the
/// checkpoint: nothing replayed and nothing counted gone.
#[tokio::test]
async fn a_restart_resumes_a_rotated_file_from_its_checkpoint() {
    let root = scratch_dir("cri-in-restart-rotated");
    let dir = container_dir(&root);
    let live = dir.join("0.log");
    append(&live, &record("stdout", "F", "one"));
    let config = TailConfig { checkpoint_path: Some(root.join("ckpt.json")), ..fast_config() };

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root, Duration::ZERO, config.clone(), &mut probe), fanout).await;
    assert_eq!(lines(&recv_events(&mut rx, 1).await), pairs(&[("one", "stdout")]));
    running.stop().await;

    let rotated = dir.join("0.log.20261030-120000");
    std::fs::rename(&live, &rotated).unwrap();
    append(&rotated, &record("stdout", "F", "two"));
    let (fanout, mut rx) = fanout_channel(8);
    let running = spawn_input(input(&root, Duration::ZERO, config, &mut probe), fanout).await;
    assert_eq!(lines(&recv_events(&mut rx, 1).await), pairs(&[("two", "stdout")]));
    assert_no_batch(&mut rx, Duration::from_millis(60), "no replay").await;
    running.stop().await;
    assert_eq!(probe.sum("logit.input.bytes.dropped", &[]), 0.0);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn the_splitter_bound_is_envelope_cap_of_max_line_bytes() {
    let input = CriInput::new(
        PathBuf::from("/var/log/pods"),
        PathBuf::from("/var/log/containers"),
        Duration::ZERO,
        TailConfig { max_line_bytes: 1024, ..TailConfig::default() },
    );
    assert_eq!(input.config().max_line_bytes, envelope_cap(1024));
}

#[tokio::test]
async fn a_malformed_record_is_counted_and_the_file_continues() {
    let root = scratch_dir("cri-in-malformed");
    let log = container_dir(&root).join("0.log");
    append(&log, &record("stdout", "P", "held"));
    append(&log, "not a cri record\n");
    append(&log, &record("stdout", "F", "after"));
    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root, Duration::ZERO, fast_config(), &mut probe), fanout).await;
    let events = recv_events(&mut rx, 2).await;
    assert_eq!(lines(&events), pairs(&[("held", "stdout"), ("after", "stdout")]));
    wait_until("the malformed count", || {
        probe.sum("logit.input.frames.dropped", &[("reason", "malformed")]) == 1.0
    })
    .await;
    assert_eq!(diagnosed(&mut probe, "bad_line"), 1.0);
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
}

/// The driver hands `cri_in` raw bytes, so a character cut between two pieces is whole in the
/// event, with no `invalid_utf8`.
#[tokio::test]
async fn a_character_cut_between_pieces_is_whole_through_the_driver() {
    let root = scratch_dir("cri-in-utf8");
    let log = container_dir(&root).join("0.log");
    let mut bytes = format!("{TS} stdout P caf").into_bytes();
    bytes.extend_from_slice(b"\xC3\n");
    bytes.extend_from_slice(format!("{TS} stdout F ").as_bytes());
    bytes.extend_from_slice(b"\xA9\n");
    std::fs::write(&log, bytes).unwrap();
    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running =
        spawn_input(input(&root, Duration::ZERO, fast_config(), &mut probe), fanout).await;
    assert_eq!(lines(&recv_events(&mut rx, 1).await), pairs(&[("café", "stdout")]));
    assert_eq!(diagnosed(&mut probe, "invalid_utf8"), 0.0);
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
}

// -- container.id ----------------------------------------------------------------------------

const ID_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ID_B: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// The kubelet's link name for `container`'s attempt with `id`.
fn link_name(container: &str, id: &str) -> String {
    format!("web-0_default_{container}-{id}.log")
}

/// Links `links/<name>` to `target`, as the kubelet does.
fn link(links: &Path, name: &str, target: &Path) {
    std::fs::create_dir_all(links).unwrap();
    std::os::unix::fs::symlink(target, links.join(name)).unwrap();
}

fn unlink(links: &Path, name: &str) {
    std::fs::remove_file(links.join(name)).unwrap();
}

fn linked_input(
    root: &Path,
    links: &Path,
    config: TailConfig,
    probe: &mut TelemetryProbe,
) -> CriInput {
    let telemetry = probe.telemetry("pods", "cri_in", "listener");
    CriInput::new(root.to_path_buf(), links.to_path_buf(), Duration::ZERO, config)
        .with_diagnostics(Diagnostics::new("pods").with_telemetry(telemetry.clone()))
        .with_telemetry(telemetry)
}

fn container_id(batch: &logit_core::EventBatch) -> Option<String> {
    batch.resource.attributes.get("container.id").and_then(|v| v.as_str()).map(String::from)
}

async fn next_batch(
    rx: &mut tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>,
) -> logit_core::EventBatch {
    logit_pipeline::test_util::recv_batch(rx).await
}

fn ids_with_diag(dir: PathBuf, probe: &mut TelemetryProbe) -> ContainerIds {
    let mut ids = ContainerIds::new(dir);
    ids.diag = Diagnostics::new("pods").with_telemetry(probe.telemetry("pods", "cri_in", "x"));
    ids
}

#[test]
fn a_link_name_yields_an_id_only_for_64_lowercase_hex_after_its_last_dash() {
    assert_eq!(link_id(&link_name("nginx", ID_A)), Some(ID_A));
    // A container name holds dashes of its own.
    assert_eq!(link_id(&format!("a-b_ns_my-app-{ID_B}.log")), Some(ID_B));
    // Truncated by the kubelet: the id is partial.
    assert_eq!(link_id(&format!("web-0_default_nginx-{}.log", &ID_A[..40])), None);
    // Truncated before the id began: the last segment is part of a name.
    assert_eq!(link_id("web-0_default_nginx-and-a-long-name.log"), None);
    // A non-hex character, an uppercase one, 63 and 65 characters, and no `.log`.
    assert_eq!(link_id(&format!("w_d_n-{}g.log", &ID_A[..63])), None);
    assert_eq!(link_id(&format!("w_d_n-{}.log", ID_A.to_uppercase())), None);
    assert_eq!(link_id(&format!("w_d_n-{}.log", &ID_A[..63])), None);
    assert_eq!(link_id(&format!("w_d_n-a{ID_A}.log")), None);
    assert_eq!(link_id(&format!("w_d_n-{ID_A}")), None);
}

#[test]
fn the_key_is_the_last_three_components_and_a_rotated_file_shares_its_live_files() {
    let live = id_key(Path::new("/var/log/pods/default_web-0_0123-abcd/nginx/1.log")).unwrap();
    assert_eq!(live, "default_web-0_0123-abcd/nginx/1");
    let elsewhere = Path::new("/mnt/host/pods/default_web-0_0123-abcd/nginx/1.log");
    assert_eq!(id_key(elsewhere), Some(live.clone()));
    let rotated = Path::new("/p/default_web-0_0123-abcd/nginx/1.log.20261030-120000");
    assert_eq!(id_key(rotated), Some(live));
    assert_eq!(id_key(Path::new("/p/default_web-0_0123-abcd/nginx/1.log.gz")), None);
    assert_eq!(id_key(Path::new("1.log")), None);
}

#[test]
fn ids_are_read_from_links_whose_targets_sit_under_another_mount() {
    let links = scratch_dir("cri-ids-links");
    let mut probe = TelemetryProbe::new();
    let mut ids = ids_with_diag(links.clone(), &mut probe);
    let elsewhere = Path::new("/host/var/log/pods/default_web-0_0123-abcd/nginx");
    link(&links, &link_name("nginx", ID_A), &elsewhere.join("0.log"));
    // Cut short of a whole id, and a non-hex suffix: neither is read.
    link(&links, &format!("web-0_default_nginx-{}.log", &ID_B[..40]), &elsewhere.join("1.log"));
    link(&links, &format!("web-0_default_nginx-{}g.log", &ID_B[..63]), &elsewhere.join("2.log"));

    let root = Path::new("/var/log/pods/default_web-0_0123-abcd/nginx");
    assert_eq!(ids.id_for(&root.join("0.log")).as_deref(), Some(ID_A));
    assert_eq!(ids.id_for(&root.join("0.log.20261030-120000")).as_deref(), Some(ID_A));
    assert_eq!(ids.id_for(&root.join("1.log")), None);
    assert_eq!(ids.id_for(&root.join("2.log")), None);
    assert_eq!(diagnosed(&mut probe, "container_id_error"), 0.0);
    std::fs::remove_dir_all(&links).ok();
}

#[test]
fn a_missing_directory_is_an_empty_listing_and_any_other_failure_is_diagnosed() {
    let scratch = scratch_dir("cri-ids-missing");
    let log = Path::new("/p/default_web-0_0123-abcd/nginx/0.log");
    let mut probe = TelemetryProbe::new();
    let mut missing = ids_with_diag(scratch.join("absent"), &mut probe);
    assert_eq!(missing.id_for(log), None);
    assert_eq!(diagnosed(&mut probe, "container_id_error"), 0.0);

    let file = scratch.join("a-file");
    std::fs::write(&file, b"").unwrap();
    let mut broken = ids_with_diag(file, &mut probe);
    assert_eq!(broken.id_for(log), None);
    assert_eq!(diagnosed(&mut probe, "container_id_error"), 1.0);
    assert_eq!(probe.sum("logit.input.scan.errors", &[]), 0.0, "not a discovery failure");
    std::fs::remove_dir_all(&scratch).ok();
}

/// A scan lists the directory once, so a link made after that listing waits for the next scan.
#[test]
fn the_directory_is_listed_once_per_scan_and_a_miss_relists_on_the_next() {
    let links = scratch_dir("cri-ids-once");
    let mut probe = TelemetryProbe::new();
    let mut ids = ids_with_diag(links.clone(), &mut probe);
    let dir = Path::new("/p/default_web-0_0123-abcd/nginx");
    link(&links, &link_name("nginx", ID_A), &dir.join("0.log"));
    assert_eq!(ids.id_for(&dir.join("0.log")).as_deref(), Some(ID_A));
    link(&links, &link_name("nginx", ID_B), &dir.join("1.log"));
    assert_eq!(ids.id_for(&dir.join("1.log")), None, "already listed this scan");
    ids.end_scan();
    assert_eq!(ids.id_for(&dir.join("1.log")).as_deref(), Some(ID_B));
    std::fs::remove_dir_all(&links).ok();
}

/// A removed link leaves the id with its file while the file is discovered, and the id goes when
/// no file maps to it.
#[test]
fn an_id_outlives_its_link_while_a_file_uses_it() {
    let links = scratch_dir("cri-ids-removed");
    let mut probe = TelemetryProbe::new();
    let mut ids = ids_with_diag(links.clone(), &mut probe);
    let path = Path::new("/p/default_web-0_0123-abcd/nginx/0.log");
    let name = link_name("nginx", ID_A);
    link(&links, &name, path);
    assert_eq!(ids.id_for(path).as_deref(), Some(ID_A));
    ids.end_scan();

    unlink(&links, &name);
    ids.touch(path);
    ids.end_scan();
    assert_eq!(ids.id_for(path).as_deref(), Some(ID_A), "kept past the link");

    ids.end_scan();
    ids.end_scan();
    assert_eq!(ids.id_for(path), None, "no file used it last scan");
    std::fs::remove_dir_all(&links).ok();
}

#[tokio::test]
async fn an_id_found_is_on_the_resource_beside_the_path_keys() {
    let root = scratch_dir("cri-id-found");
    let links = scratch_dir("cri-id-found-links");
    let log = container_dir(&root).join("0.log");
    append(&log, &record("stdout", "F", "one"));
    link(&links, &link_name("nginx", ID_A), &log);
    // Cut short, beside it: ignored.
    link(&links, &format!("web-0_default_nginx-{}.log", &ID_B[..40]), &log);

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running = spawn_input(linked_input(&root, &links, fast_config(), &mut probe), fanout).await;
    let batch = next_batch(&mut rx).await;
    assert_eq!(container_id(&batch).as_deref(), Some(ID_A));
    assert_eq!(batch.resource.attributes.len(), 6);
    assert_eq!(probe.sum("logit.input.files.identity_changed", &[]), 0.0, "known at open");
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&links).ok();
}

/// The kubelet makes the link after the container starts: lines read before it carry no id, the
/// id shows up on a later batch, and no batch holds both resources.
#[tokio::test]
async fn a_link_made_after_the_file_was_read_adds_the_id_to_later_batches() {
    let root = scratch_dir("cri-id-late");
    let links = scratch_dir("cri-id-late-links");
    let log = container_dir(&root).join("0.log");
    append(&log, &record("stdout", "F", "before"));

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running = spawn_input(linked_input(&root, &links, fast_config(), &mut probe), fanout).await;
    let first = next_batch(&mut rx).await;
    assert_eq!(lines(&first.events), pairs(&[("before", "stdout")]));
    assert_eq!(container_id(&first), None);

    link(&links, &link_name("nginx", ID_A), &log);
    probe
        .wait_for("a scan to install the id", |t| {
            t.sum("logit.input.files.identity_changed", &[]) >= 1.0
        })
        .await;
    append(&log, &record("stdout", "F", "after"));
    let second = next_batch(&mut rx).await;
    assert_eq!(lines(&second.events), pairs(&[("after", "stdout")]));
    assert_eq!(container_id(&second).as_deref(), Some(ID_A));
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&links).ok();
}

/// The kubelet removes the link when it collects the container, but the file may still be read.
#[tokio::test]
async fn a_removed_link_keeps_its_id_on_the_files_later_batches() {
    let root = scratch_dir("cri-id-removed");
    let links = scratch_dir("cri-id-removed-links");
    let log = container_dir(&root).join("0.log");
    append(&log, &record("stdout", "F", "one"));
    let name = link_name("nginx", ID_A);
    link(&links, &name, &log);

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running = spawn_input(linked_input(&root, &links, fast_config(), &mut probe), fanout).await;
    assert_eq!(container_id(&next_batch(&mut rx).await).as_deref(), Some(ID_A));

    unlink(&links, &name);
    append(&log, &record("stdout", "F", "two"));
    let batch = next_batch(&mut rx).await;
    assert_eq!(lines(&batch.events), pairs(&[("two", "stdout")]));
    assert_eq!(container_id(&batch).as_deref(), Some(ID_A));
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&links).ok();
}

/// A restart is a new container: `1.log` has its own link and id, and `0.log` keeps its own.
#[tokio::test]
async fn a_restarted_container_has_its_own_id() {
    let root = scratch_dir("cri-id-restart");
    let links = scratch_dir("cri-id-restart-links");
    let dir = container_dir(&root);
    append(&dir.join("0.log"), &record("stdout", "F", "first life"));
    append(&dir.join("1.log"), &record("stdout", "F", "second life"));
    link(&links, &link_name("nginx", ID_A), &dir.join("0.log"));
    link(&links, &link_name("nginx", ID_B), &dir.join("1.log"));

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running = spawn_input(linked_input(&root, &links, fast_config(), &mut probe), fanout).await;
    let mut seen = Vec::new();
    for _ in 0..2 {
        let batch = next_batch(&mut rx).await;
        seen.push((lines(&batch.events)[0].0.clone(), container_id(&batch)));
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("first life".to_string(), Some(ID_A.to_string())),
            ("second life".to_string(), Some(ID_B.to_string())),
        ]
    );
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&links).ok();
}

/// The kubelet renames a full file to `<n>.log.<stamp>` while the link still names `<n>.log`: the
/// renamed inode keeps its id, and so does a file first opened under the rotated name.
#[tokio::test]
async fn a_rotated_file_keeps_its_live_files_id() {
    let root = scratch_dir("cri-id-rotated");
    let links = scratch_dir("cri-id-rotated-links");
    let dir = container_dir(&root);
    let live = dir.join("0.log");
    append(&live, &record("stdout", "F", "one"));
    link(&links, &link_name("nginx", ID_A), &live);

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running = spawn_input(linked_input(&root, &links, fast_config(), &mut probe), fanout).await;
    assert_eq!(container_id(&next_batch(&mut rx).await).as_deref(), Some(ID_A));

    let rotated = dir.join("0.log.20261030-120000");
    std::fs::rename(&live, &rotated).unwrap();
    append(&rotated, &record("stdout", "F", "two"));
    let batch = next_batch(&mut rx).await;
    assert_eq!(lines(&batch.events), pairs(&[("two", "stdout")]));
    assert_eq!(container_id(&batch).as_deref(), Some(ID_A));
    running.stop().await;

    let (fanout, mut rx) = fanout_channel(8);
    let running = spawn_input(linked_input(&root, &links, fast_config(), &mut probe), fanout).await;
    assert_eq!(container_id(&next_batch(&mut rx).await).as_deref(), Some(ID_A));
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&links).ok();
}

/// No `containers_dir` at all (the mount is absent) means no ids: not an error, not loss.
#[tokio::test]
async fn a_missing_containers_dir_gives_no_id_and_no_error() {
    let root = scratch_dir("cri-id-nodir");
    let links = root.join("absent");
    append(&container_dir(&root).join("0.log"), &record("stdout", "F", "one"));

    let (fanout, mut rx) = fanout_channel(8);
    let mut probe = TelemetryProbe::new();
    let running = spawn_input(linked_input(&root, &links, fast_config(), &mut probe), fanout).await;
    let batch = next_batch(&mut rx).await;
    assert_eq!(lines(&batch.events), pairs(&[("one", "stdout")]));
    assert_eq!(container_id(&batch), None);
    running.stop().await;
    assert_eq!(diagnosed(&mut probe, "container_id_error"), 0.0);
    assert_eq!(probe.sum("logit.input.scan.errors", &[]), 0.0);
    assert_eq!(probe.sum("logit.input.bytes.dropped", &[]), 0.0);
    std::fs::remove_dir_all(&root).ok();
}
