//! `replay_in` through a real `logit run`: the shipped `fixtures/replay-json.yaml` replays its
//! file to `stdio_out format: json` and exits 0 on its own, a `format: native` archive of the
//! same events replays to the same lines, and a missing file fails startup.

mod support;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use logit_proto::event_json::{EventJsonDecoder, Line};
use support::{wait_for_exit, KillOnDrop, TempConfig};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every non-blank line of `bytes`, decoded.
fn decode_lines(bytes: &[u8]) -> Vec<Line> {
    let mut decoder = EventJsonDecoder::new();
    bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| decoder.decode_line(line).expect("a format: json line"))
        .collect()
}

/// Runs `logit run <config>` to its own exit, returning its status and stdout.
async fn run_to_exit(config: &Path) -> (std::process::ExitStatus, Vec<u8>) {
    let child = Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("run")
        .arg(config)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning logit run");
    let mut child = KillOnDrop(child);
    let mut stdout = child.0.stdout.take().expect("piped stdout");
    // Read on a thread, so a full pipe can't hold the child back from exiting.
    let reader = std::thread::spawn(move || {
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut stdout, &mut out).expect("reading stdout");
        out
    });
    let status = wait_for_exit(&mut child.0).await;
    (status, reader.join().expect("the stdout reader"))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_replay_fixture_writes_every_event_and_exits_zero() {
    let fixture = repo_root().join("fixtures/replay/events.json");
    let (status, stdout) = run_to_exit(&repo_root().join("fixtures/replay-json.yaml")).await;
    assert!(status.success(), "logit run exits 0 at the end of the file: {status}");

    let written = decode_lines(&std::fs::read(&fixture).expect("the fixture file"));
    let replayed = decode_lines(&stdout);
    assert_eq!(written.len(), 9);
    assert_eq!(replayed, written, "every event, resource, and scope reads back as written");
}

/// The fixture through a `format: native` file and back through `replay_in format: native` gives
/// the lines the JSON replay gives.
#[tokio::test(flavor = "multi_thread")]
async fn a_native_archive_replays_to_the_same_lines() {
    let fixture = repo_root().join("fixtures/replay/events.json");
    let dir = std::env::temp_dir().join(format!("logit-cli-test-replay-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    let archive = dir.join("archive.native");
    let _ = std::fs::remove_file(&archive);

    let to_native = TempConfig::write(
        "replay-to-native",
        format!(
            "components:\n  in:\n    type: replay_in\n    path: \"{}\"\n  out:\n    type: \
             stdio_out\n    sources: [in]\n    format: native\n    compression: lz4\n    \
             target: \"{}\"\n",
            fixture.display(),
            archive.display()
        ),
    );
    let (status, _) = run_to_exit(&to_native.0).await;
    assert!(status.success(), "writing the archive exits 0: {status}");

    let from_native = TempConfig::write(
        "replay-from-native",
        format!(
            "components:\n  in:\n    type: replay_in\n    path: \"{}\"\n    format: native\n    \
             batch: 2\n  out:\n    type: stdio_out\n    sources: [in]\n    format: json\n",
            archive.display()
        ),
    );
    let (status, native_lines) = run_to_exit(&from_native.0).await;
    assert!(status.success(), "replaying the archive exits 0: {status}");

    let (_, json_lines) = run_to_exit(&repo_root().join("fixtures/replay-json.yaml")).await;
    assert_eq!(String::from_utf8(native_lines).unwrap(), String::from_utf8(json_lines).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_replay_file_fails_startup_with_exit_1() {
    let config = TempConfig::write(
        "replay-missing",
        "components:\n  in:\n    type: replay_in\n    path: no-such-file.json\n  out:\n    \
         type: null_out\n    sources: [in]\n",
    );
    let (status, _) = run_to_exit(&config.0).await;
    assert_eq!(status.code(), Some(1), "a file that can't be opened is a startup failure");
}
