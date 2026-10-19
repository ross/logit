//! `lines_in` end to end through a real `logit run`: lines sent over TCP and UDP reach a
//! `stdio_out` `format: json` sink as raw log events, and a `json` stage parses the JSON ones into
//! attributes. `crates/logit-inputs/src/lines.rs`'s own tests cover the decoder and each
//! transport; this pins the config-to-runtime wiring the binary does.

mod support;

use std::io::{BufRead, BufReader, Write};
use std::process::Stdio;
use std::sync::mpsc;

use support::{spawn_run, TempConfig, ANY_PORT, PROCESS_DEADLINE};

#[tokio::test(flavor = "multi_thread")]
async fn lines_over_tcp_and_udp_reach_stdout_as_log_events() {
    let config = TempConfig::write(
        "lines-tcp-udp",
        format!(
            "components:\n  tcp_in:\n    type: lines_in\n    bind: \"{ANY_PORT}\"\n  udp_in:\n    \
             type: lines_in\n    bind: \"{ANY_PORT}\"\n    transport: udp\n  parse:\n    type: \
             json\n    sources: [tcp_in, udp_in]\n  out:\n    type: stdio_out\n    sources: \
             [parse]\n    format: json\n"
        ),
    );

    let (mut child, stderr) = spawn_run(&config, Stdio::piped());
    let stdout = child.0.stdout.take().expect("piped stdout");
    let (line_tx, line_rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line_tx.send(line).is_err() {
                break;
            }
        }
    });

    let addrs = stderr.wait_for_bound(&["tcp_in", "udp_in"]);
    let (tcp_addr, udp_addr) = (&addrs[0], &addrs[1]);
    let mut client = std::net::TcpStream::connect(tcp_addr).expect("connecting to tcp_in");
    client.write_all(b"{\"user\":\"ada\",\"n\":3}\r\nplain text over tcp\n").unwrap();
    client.flush().unwrap();

    let mut events = Vec::new();
    let mut read_events = |want: usize| {
        while events.len() < want {
            let line = line_rx.recv_timeout(PROCESS_DEADLINE).unwrap_or_else(|_| {
                panic!("timed out with {} of {want} events: {events:?}", events.len())
            });
            let event: serde_json::Value =
                serde_json::from_str(&line).unwrap_or_else(|e| panic!("not JSON ({e}): {line}"));
            events.push(event);
        }
    };
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    udp.send_to(b"one datagram\nits unterminated tail", udp_addr).unwrap();
    read_events(4);

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
