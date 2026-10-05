//! `lines_in` end to end through a real `logit run`: lines sent over TCP and UDP reach a
//! `stdio_out` `format: json` sink as raw log events, and a `json` stage parses the JSON ones into
//! attributes. `crates/logit-inputs/src/lines.rs`'s own tests cover the decoder and each
//! transport; this pins the config-to-runtime wiring the binary does.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use logit_pipeline::test_util::wait_until_within;

/// How long the `logit run` child gets to bind its listeners, and each stdout line to arrive. A
/// process spawn is slower than an in-process bind, so this is wider than `RECV_TIMEOUT`.
const PROCESS_DEADLINE: Duration = Duration::from_secs(10);

struct TempConfig(PathBuf);

impl TempConfig {
    fn write(name: &str, contents: &str) -> Self {
        let path = std::env::temp_dir()
            .join(format!("logit-lines-round-trip-{name}-{}.yaml", std::process::id()));
        std::fs::File::create(&path)
            .and_then(|mut f| f.write_all(contents.as_bytes()))
            .expect("writing the temp config");
        Self(path)
    }
}

impl Drop for TempConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Kills the child on drop, so a failing assertion never leaves a `logit run` holding its ports.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A free loopback port, bound and released: the child-process exception to
/// `docs/adr/test-timing-and-observables.md`'s bind-before-spawn rule, since the child binds it.
async fn ephemeral_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn lines_over_tcp_and_udp_reach_stdout_as_log_events() {
    let tcp_addr = ephemeral_addr().await;
    let udp_addr = {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.local_addr().unwrap().to_string()
    };
    let config = TempConfig::write(
        "tcp-udp",
        &format!(
            "components:\n  tcp_in:\n    type: lines_in\n    bind: \"{tcp_addr}\"\n  udp_in:\n    \
             type: lines_in\n    bind: \"{udp_addr}\"\n    transport: udp\n  parse:\n    type: \
             json\n    sources: [tcp_in, udp_in]\n  out:\n    type: stdio_out\n    sources: \
             [parse]\n    format: json\n"
        ),
    );

    let child = Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("run")
        .arg(&config.0)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning logit run");
    let mut child = KillOnDrop(child);
    let stdout = child.0.stdout.take().expect("piped stdout");
    let (line_tx, line_rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line_tx.send(line).is_err() {
                break;
            }
        }
    });

    // Every listener binds before any runs, so an accepted TCP connection means the UDP socket
    // is bound too.
    let mut client = None;
    wait_until_within("the TCP lines_in to accept", PROCESS_DEADLINE, || {
        client = std::net::TcpStream::connect(&tcp_addr).ok();
        client.is_some()
    })
    .await;
    let mut client = client.unwrap();
    client.write_all(b"{\"user\":\"ada\",\"n\":3}\r\nplain text over tcp\n").unwrap();
    client.flush().unwrap();
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    udp.send_to(b"one datagram\nits unterminated tail", &udp_addr).unwrap();

    let mut events = Vec::new();
    while events.len() < 4 {
        let line = line_rx
            .recv_timeout(PROCESS_DEADLINE)
            .unwrap_or_else(|_| panic!("timed out with {} of 4 events: {events:?}", events.len()));
        let event: serde_json::Value =
            serde_json::from_str(&line).unwrap_or_else(|e| panic!("not JSON ({e}): {line}"));
        events.push(event);
    }

    let message = |event: &serde_json::Value| event["log"]["message"].as_str().map(str::to_string);
    let mut messages: Vec<String> = events.iter().filter_map(message).collect();
    messages.sort();
    assert_eq!(
        messages,
        [
            "its unterminated tail",
            "one datagram",
            "plain text over tcp",
            "{\"user\":\"ada\",\"n\":3}",
        ],
        "one event per line, CR stripped, a datagram's tail kept: {events:?}"
    );
    for event in &events {
        assert_eq!(event["log"]["format"], "raw", "{event}");
        assert!(event["log"].get("severity").is_none(), "no severity: {event}");
    }
    let parsed = events
        .iter()
        .find(|e| e["log"]["message"].as_str().is_some_and(|m| m.starts_with('{')))
        .unwrap();
    assert_eq!(parsed["attributes"]["user"], "ada", "json parsed the line: {parsed}");
    assert_eq!(parsed["attributes"]["n"], 3, "{parsed}");
    let plain = events.iter().find(|e| e["log"]["message"] == "plain text over tcp").unwrap();
    assert!(plain.get("attributes").is_none(), "nothing attached to a line: {plain}");
}
