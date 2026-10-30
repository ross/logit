//! Replays the CRI container-log files recorded under `testdata/interop/cri/` through `cri_in`.
//!
//! Each expectation is what the recorded pod wrote (`tools/record-fixtures/cri/pods.yaml`), not
//! what `cri_in` returns. The raw files are read here only for what the decoder must reproduce:
//! each line's first record time, and a check that the file holds the shape the test claims (a
//! cut inside a UTF-8 sequence, a run split across a rotation).

use flate2::read::GzDecoder;
use logit_core::{Diagnostics, Event, Value};
use logit_inputs::cri::CriInput;
use logit_inputs::tail::{ReadFrom, TailBatching, TailConfig, WatchMode};
use logit_pipeline::test_util::{
    assert_no_batch, fanout_channel, recv_batch, scratch_dir, spawn_input, TelemetryProbe,
};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

const NAMESPACE: &str = "cri-fixture";

fn recorded(runtime: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/cri").join(runtime)
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// One record of a recorded file, split on single spaces by hand.
struct Raw {
    time: String,
    stream: String,
    tag: String,
    content: Vec<u8>,
    /// The record begins a line: the previous record of its stream in the same file closed a
    /// line (an `F`), or there is none.
    starts: bool,
}

fn raw_records(bytes: &[u8]) -> Vec<Raw> {
    let mut records: Vec<Raw> = Vec::new();
    for line in bytes.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let mut fields = line.splitn(4, |&b| b == b' ');
        let mut next = || String::from_utf8(fields.next().unwrap().to_vec()).unwrap();
        let (time, stream, tag) = (next(), next(), next());
        let content = fields.next().unwrap_or_default().to_vec();
        let starts = records.iter().rfind(|r| r.stream == stream).is_none_or(|r| r.tag == "F");
        records.push(Raw { time, stream, tag, content, starts });
    }
    records
}

/// `2026-10-30T16:28:54.141366191Z` or `...+00:00` as Unix nanoseconds.
fn nanos(time: &str) -> i64 {
    let (date, rest) = time.split_once('T').unwrap();
    let mut d = date.split('-').map(|n| n.parse::<i64>().unwrap());
    let (y, m, day) = (d.next().unwrap(), d.next().unwrap(), d.next().unwrap());
    let split = rest.find(['Z', '+', '-']).unwrap();
    let (clock, zone) = rest.split_at(split);
    let (hms, frac) = clock.split_once('.').unwrap_or((clock, ""));
    let mut t = hms.split(':').map(|n| n.parse::<i64>().unwrap());
    let secs_of_day = t.next().unwrap() * 3600 + t.next().unwrap() * 60 + t.next().unwrap();
    let nanos: i64 = format!("{frac:0<9}").parse().unwrap();
    let offset = if zone == "Z" {
        0
    } else {
        let sign = if zone.starts_with('-') { -1 } else { 1 };
        let (h, mi) = zone[1..].split_once(':').unwrap();
        sign * (h.parse::<i64>().unwrap() * 3600 + mi.parse::<i64>().unwrap() * 60)
    };
    // Days from civil (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    ((days * 86_400 + secs_of_day - offset) * 1_000_000_000) + nanos
}

fn repeat(byte: u8, n: usize) -> Vec<u8> {
    vec![byte; n]
}

/// What one delivered event must show, as the pod wrote it.
struct Delivered {
    pod_dir: String,
    container: String,
    restart_count: i64,
    stream: String,
    message: String,
    timestamp: i64,
}

fn text(event: &Event) -> String {
    event.log.as_ref().unwrap().message.as_str().unwrap().to_string()
}

fn attr(map: &logit_core::AttrMap, key: &str) -> Option<Value> {
    map.get(key).cloned()
}

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

/// Runs `cri_in` over a scratch copy of `runtime`'s pods tree until `count` events have arrived,
/// then checks nothing else does.
async fn replay(runtime: &str, count: usize, probe: &mut TelemetryProbe) -> Vec<Delivered> {
    let root = scratch_dir(&format!("cri-interop-{runtime}"));
    copy_tree(&recorded(runtime).join("pods"), &root);
    let telemetry = probe.telemetry("pods", "cri_in", "listener");
    let input = CriInput::new(root.clone(), Duration::from_millis(100), fast_config())
        .with_diagnostics(Diagnostics::new("pods").with_telemetry(telemetry.clone()))
        .with_telemetry(telemetry);
    let (fanout, mut rx) = fanout_channel(64);
    let running = spawn_input(input, fanout).await;

    let mut delivered = Vec::new();
    while delivered.len() < count {
        let batch = recv_batch(&mut rx).await;
        let resource = &batch.resource.attributes;
        let string = |key: &str| {
            attr(resource, key)
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| panic!("resource lacks {key}"))
        };
        let restart_count = match attr(resource, "k8s.container.restart_count") {
            Some(Value::I64(n)) => n,
            other => panic!("restart_count must be an integer, got {other:?}"),
        };
        assert_eq!(string("k8s.namespace.name"), NAMESPACE);
        let pod_dir = format!(
            "{}_{}_{}",
            string("k8s.namespace.name"),
            string("k8s.pod.name"),
            string("k8s.pod.uid")
        );
        for event in batch.events {
            let stream = attr(&event.attributes, "log.iostream")
                .and_then(|v| v.as_str().map(str::to_string))
                .expect("every event carries log.iostream");
            delivered.push(Delivered {
                pod_dir: pod_dir.clone(),
                container: string("k8s.container.name"),
                restart_count,
                stream,
                message: text(&event),
                timestamp: event.timestamp,
            });
        }
    }
    assert_no_batch(&mut rx, Duration::from_millis(150), "the tree holds no other line").await;
    running.stop().await;
    std::fs::remove_dir_all(&root).ok();
    delivered
}

/// The pod directory in the recorded tree whose name starts with `NAMESPACE_<pod>_`.
fn pod_dir(runtime: &str, pod: &str) -> String {
    let prefix = format!("{NAMESPACE}_{pod}");
    let mut found: Vec<String> = std::fs::read_dir(recorded(runtime).join("pods"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with(&prefix))
        .collect();
    assert_eq!(found.len(), 1, "one directory for {pod}: {found:?}");
    found.pop().unwrap()
}

/// The delivered lines of one stream of one container run, in delivery order.
fn lines<'a>(
    delivered: &'a [Delivered],
    pod_dir: &str,
    container: &str,
    restart: i64,
    stream: &str,
) -> Vec<&'a Delivered> {
    delivered
        .iter()
        .filter(|d| {
            d.pod_dir == pod_dir
                && d.container == container
                && d.restart_count == restart
                && d.stream == stream
        })
        .collect()
}

/// Asserts `got` holds the lines of `segments`, each segment's lines in its own order.
/// A kubelet rotation leaves a run's lines in two files that `cri_in` reads concurrently, and a
/// file's last line, held until the idle timer, can come after the other file's lines.
fn assert_segments(got: &[&Delivered], segments: &[Vec<String>], what: &str) {
    let got: Vec<&str> = got.iter().map(|d| d.message.as_str()).collect();
    let mut want: Vec<&str> = segments.iter().flatten().map(String::as_str).collect();
    let mut have = got.clone();
    want.sort_unstable();
    have.sort_unstable();
    assert_eq!(have.len(), want.len(), "{what}: {} lines delivered", have.len());
    assert!(have == want, "{what}: the delivered lines differ from what the pod wrote");
    for segment in segments {
        let positions: Vec<usize> = segment
            .iter()
            .map(|line| got.iter().position(|g| g == line).expect("every line is delivered"))
            .collect();
        assert!(
            positions.windows(2).all(|w| w[0] < w[1]),
            "{what}: a file's lines keep their order"
        );
    }
}

/// The highest `<n>` of a container directory's `<n>.log` files.
fn restart_count(dir: &Path) -> i64 {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            e.unwrap().file_name().into_string().unwrap().strip_suffix(".log")?.parse().ok()
        })
        .max()
        .unwrap()
}

fn s(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn all_raw(dir: &Path, only: impl Fn(&str) -> bool) -> Vec<Raw> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| only(n))
        .collect();
    names.sort();
    names.iter().flat_map(|n| raw_records(&std::fs::read(dir.join(n)).unwrap())).collect()
}

/// Each delivered line's time is its first record's: the first raw record of the same stream that
/// begins a line and whose content starts with the line (the whole line when it is short).
fn assert_first_record_time(delivered: &[&Delivered], raw: &[Raw], expected_bytes: &[Vec<u8>]) {
    assert_eq!(delivered.len(), expected_bytes.len());
    for (d, want) in delivered.iter().zip(expected_bytes) {
        let head = &want[..want.len().min(64)];
        let what = s(head);
        let first = raw
            .iter()
            .find(|r| {
                r.starts
                    && r.stream == d.stream
                    && if want.len() <= 64 {
                        r.content == head
                    } else {
                        r.content.starts_with(head)
                    }
            })
            .unwrap_or_else(|| panic!("no record starts {what:?}"));
        assert_eq!(d.timestamp, nanos(&first.time), "time of {what:?}");
    }
}

/// Replays `runtime`'s recording and checks it against what `tools/record-fixtures/cri/pods.yaml`
/// wrote. `crlf` is the line the pod wrote as `crlf line` plus CRLF, as the runtime logged it.
async fn check_tree(runtime: &str, crlf: &[u8]) {
    let tree = recorded(runtime).join("pods");
    let edge = pod_dir(runtime, "edge");
    let rotator = pod_dir(runtime, "rotator");
    let static_pod = pod_dir(runtime, "static-hello");

    let x = repeat(b'x', 17_384);
    let y = repeat(b'y', 20_500);
    let mut straddle = repeat(b'a', 16_383);
    straddle.extend_from_slice("\u{e9}".as_bytes());
    straddle.extend(repeat(b'b', 100));
    let bad: &[u8] = b"bad \xff\xfe utf8";

    // The rotator wrote 850 numbered lines and one unterminated tail. The kubelet gzipped its
    // oldest rotated files, which `cri_in` never reads, so the lines it must deliver are what
    // the compressed files don't hold.
    let rotator_dir = tree.join(&rotator).join("spin");
    let pad = String::from_utf8(repeat(b'p', 70)).unwrap();
    let mut wrote: Vec<String> = (1..=850).map(|i| format!("rotate {i:04} {pad}")).collect();
    wrote.push("rotate done no newline".to_string());
    let mut gz_lines = Vec::new();
    let mut gz_files: Vec<_> = std::fs::read_dir(&rotator_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(".gz"))
        .collect();
    gz_files.sort();
    assert!(!gz_files.is_empty(), "the recording holds a kubelet-compressed file");
    for name in &gz_files {
        let mut text = Vec::new();
        GzDecoder::new(std::fs::File::open(rotator_dir.join(name)).unwrap())
            .read_to_end(&mut text)
            .unwrap();
        gz_lines.extend(raw_records(&text).into_iter().map(|r| s(&r.content)));
    }
    assert_eq!(gz_lines[..], wrote[..gz_lines.len()], "the .gz files hold the first lines");
    let rotated_name = std::fs::read_dir(&rotator_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .find(|n| n.starts_with("0.log.") && !n.ends_with(".gz"))
        .expect("the recording holds the newest, uncompressed rotated file");
    let rotated_lines = raw_records(&std::fs::read(rotator_dir.join(&rotated_name)).unwrap()).len();
    let unread = wrote[gz_lines.len()..].to_vec();
    let (rotator_a, rotator_b) = unread.split_at(rotated_lines);

    // Fixture shape the assertions below rely on, checked in the raw file.
    let edge_dir = tree.join(&edge).join("writer");
    let rotated_edge = std::fs::read_dir(&edge_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .find(|n| n.starts_with("0.log."))
        .expect("edge's first run was rotated");
    let before = raw_records(&std::fs::read(edge_dir.join(&rotated_edge)).unwrap());
    if runtime == "containerd" {
        let cut = before
            .iter()
            .position(|r| r.content.starts_with(b"aaaa") && r.tag == "P")
            .expect("the straddling line is split");
        assert_eq!(before[cut].content.len(), 16_384, "containerd cuts at 16384 bytes");
        assert_eq!(before[cut].content.last(), Some(&0xC3), "the cut falls inside the e-acute");
        assert_eq!(before[cut + 1].content.first(), Some(&0xA9));
        assert!(before.iter().all(|r| r.time.ends_with('Z')), "containerd writes UTC as Z");
    } else {
        assert!(before.iter().all(|r| r.time.ends_with("+00:00") && r.time.len() == 35));
    }
    let logged_crlf = before.iter().find(|r| r.content.starts_with(b"crlf line")).unwrap();
    assert_eq!(logged_crlf.content, crlf, "the runtime's CRLF handling");
    let y_p = before.iter().position(|r| r.content.starts_with(b"yyyy") && r.tag == "P").unwrap();
    let err_at = before.iter().position(|r| r.content == b"stderr between").unwrap();
    assert!(y_p < err_at, "stderr lands inside the stdout run");
    let closes = &before[err_at + 1];
    assert_eq!((closes.stream.as_str(), closes.tag.as_str()), ("stdout", "F"));
    assert_eq!(before.last().unwrap().tag, "P", "the rotated file ends on a dangling run");

    // The kubelet restarts the container until the recording ends, so the number of restarts is
    // whatever `<n>.log` files the edge directory holds past 0.log.
    let restarts = restart_count(&edge_dir);
    assert!(restarts >= 1, "the recording holds a restart");
    let count = 23 + 2 * restarts as usize + unread.len() + 2;
    let mut probe = TelemetryProbe::new();
    let delivered = replay(runtime, count, &mut probe).await;

    // edge, first run: its files are 0.log.<ts> (before the rotation) and 0.log (after).
    let out_a = vec![
        b"first run".to_vec(),
        y.clone(),
        crlf.to_vec(),
        b"cr mid\rline".to_vec(),
        b"".to_vec(),
        x,
        straddle,
        bad.to_vec(),
        b"held across rotation".to_vec(),
    ];
    let mut out_b = vec![b" rest".to_vec()];
    out_b.extend((1..=5).map(|i| format!("out {i}").into_bytes()));
    out_b.push(b"dangling no newline".to_vec());
    let err_a = vec![b"stderr line".to_vec(), b"stderr between".to_vec()];
    let err_b: Vec<Vec<u8>> = (1..=5).map(|i| format!("err {i}").into_bytes()).collect();
    let strings = |lines: &[Vec<u8>]| lines.iter().map(|l| s(l)).collect::<Vec<_>>();

    let stdout = lines(&delivered, &edge, "writer", 0, "stdout");
    assert_segments(&stdout, &[strings(&out_a), strings(&out_b)], "edge stdout");
    let stderr = lines(&delivered, &edge, "writer", 0, "stderr");
    assert_segments(&stderr, &[strings(&err_a), strings(&err_b)], "edge stderr");
    assert!(
        stdout.iter().any(|d| d.message.contains('\u{fffd}') && d.message.starts_with("bad ")),
        "invalid bytes are replaced"
    );
    assert!(stdout
        .iter()
        .all(|d| !d.message.contains("\u{fffd}") || d.message.starts_with("bad ")));

    // The mid-line stderr record lands before the stdout line it interrupts.
    let order: Vec<&str> = delivered
        .iter()
        .filter(|d| d.pod_dir == edge && d.restart_count == 0)
        .filter(|d| d.message == "stderr between" || d.message.starts_with("yyyy"))
        .map(|d| d.message.as_str())
        .collect();
    assert_eq!(order[0], "stderr between");

    // Event time is the first record's, for the split lines too.
    let raw = all_raw(&edge_dir, |n| n.starts_with("0.log"));
    let a_stdout = lines(&delivered, &edge, "writer", 0, "stdout");
    let by_content: BTreeMap<&str, &Delivered> =
        a_stdout.iter().map(|d| (d.message.as_str(), *d)).collect();
    for want in out_a.iter().chain(&out_b) {
        let d = by_content[s(want).as_str()];
        assert_first_record_time(&[d], &raw, std::slice::from_ref(want));
    }
    let a_stderr = lines(&delivered, &edge, "writer", 0, "stderr");
    for d in &a_stderr {
        assert_first_record_time(&[d], &raw, &[d.message.clone().into_bytes()]);
    }

    // Each restart ended on a dangling run.
    for restart in 1..=restarts {
        let stdout = lines(&delivered, &edge, "writer", restart, "stdout");
        assert_segments(
            &stdout,
            &[vec!["second run".to_string(), "second run dangling".to_string()]],
            "edge restart",
        );
        let raw = raw_records(&std::fs::read(edge_dir.join(format!("{restart}.log"))).unwrap());
        assert_eq!(stdout[0].timestamp, nanos(&raw[0].time));
        assert_eq!(stdout[1].timestamp, nanos(&raw[1].time));
    }

    // rotator: the live file and the uncompressed rotated file, in two orders.
    let spin = lines(&delivered, &rotator, "spin", 0, "stdout");
    assert_segments(&spin, &[rotator_a.to_vec(), rotator_b.to_vec()], "rotator");
    assert_eq!(lines(&delivered, &rotator, "spin", 0, "stderr").len(), 0);

    // The static pod's directory carries the kubelet's manifest hash.
    let uid = static_pod.rsplit('_').next().unwrap();
    assert_eq!(uid.len(), 32);
    assert!(uid.bytes().all(|b| b.is_ascii_hexdigit()), "a manifest hash, not an API uid");
    assert_segments(
        &lines(&delivered, &static_pod, "hello", 0, "stdout"),
        &[vec!["static hello".to_string()]],
        "static stdout",
    );
    assert_segments(
        &lines(&delivered, &static_pod, "hello", 0, "stderr"),
        &[vec!["static stderr".to_string()]],
        "static stderr",
    );

    // Dangling runs flushed on the idle timer: edge's held head and last line, every restart, and
    // the rotator's tail. Nothing was lost, and the one invalid line was diagnosed.
    assert_eq!(
        probe.sum("logit.input.frames.truncated", &[("reason", "partial_timeout")]),
        (3 + restarts) as f64
    );
    assert_eq!(probe.sum("logit.input.frames.dropped", &[]), 0.0);
    assert_eq!(probe.sum("logit.component.diagnostics", &[("key", "invalid_utf8")]), 1.0);
}

/// The kubelet's `/var/log/containers` symlinks, as the recording's manifest lists them.
fn check_symlinks(runtime: &str) {
    let root = recorded(runtime);
    let manifest = std::fs::read_to_string(root.join("symlinks.txt")).unwrap();
    let mut seen = 0;
    for entry in manifest.lines() {
        let (name, target) = entry.split_once(" -> ").unwrap();
        let stem = name.strip_suffix(".log").expect("a .log name");
        let (head, id) = stem.rsplit_once('-').unwrap();
        assert_eq!(id.len(), 64);
        assert!(id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        // <pod>_<namespace>_<container>
        let parts: Vec<&str> = head.split('_').collect();
        assert_eq!(parts.len(), 3, "{name}");
        assert_eq!(parts[1], NAMESPACE);
        // An absolute target in the kubelet's layout, whose container directory is in the tree.
        let rel = target.strip_prefix("/var/log/pods/").expect("an absolute pods path");
        let mut path = rel.split('/');
        let (dir, container, file) =
            (path.next().unwrap(), path.next().unwrap(), path.next().unwrap());
        assert_eq!(container, parts[2]);
        assert!(dir.starts_with(&format!("{NAMESPACE}_{}_", parts[0])), "{name}");
        assert!(root.join("pods").join(dir).join(container).join(file).exists(), "{name}");
        seen += 1;
    }
    let edge = pod_dir(runtime, "edge");
    let attempts = 1 + restart_count(&root.join("pods").join(edge).join("writer"));
    assert_eq!(seen, attempts + 2, "edge's attempts, the rotator, and the static pod");
}

#[tokio::test]
async fn containerd_pods_tree_replays_as_the_pods_wrote_it() {
    // containerd removes the CR before the LF.
    check_tree("containerd", b"crlf line").await;
}

#[tokio::test]
async fn crio_pods_tree_replays_as_the_pods_wrote_it() {
    // conmon keeps the CR in the content.
    check_tree("crio", b"crlf line\r").await;
}

#[test]
fn containerd_symlink_manifest_matches_the_tree() {
    check_symlinks("containerd");
}

#[test]
fn crio_symlink_manifest_matches_the_tree() {
    check_symlinks("crio");
}
