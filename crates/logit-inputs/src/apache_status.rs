//! `apache_status_in`: the scrape driver ([`crate::scrape`]) over
//! [`logit_proto::apache_status`], which holds the `?auto` page's grammar and its mapping table.
//! Each target's batches carry [`crate::scrape::server_resource`]'s `server.address`,
//! `server.port`, and `url.path`.
//!
//! Beyond the driver's counters, the decoder counts what the parser reports, under the loss
//! vocabulary, and leaves `up` at `1` for each:
//! - a listed key whose value doesn't parse, `logit.input.metrics.dropped{reason="malformed_value"}`,
//!   with a throttled `malformed_value` diagnostic naming the key;
//! - a repeated listed key, `logit.input.metrics.normalized{reason="duplicate_key"}`, once per
//!   repeat;
//! - an unterminated last line, `logit.input.lines.dropped{reason="truncated"}`.
//!
//! A body with no listed key, such as the HTML page a URL without `?auto` gets, is the driver's
//! `parse_error` class, and its `scrape_failed` diagnostic says to add `?auto`.

use crate::scrape::{server_resource, ScrapeDecoder, ScrapeInput};
use logit_core::{Diagnostics, Event, Resource, Telemetry};
use logit_proto::apache_status::{parse, ParseError};

/// The largest page this input reads. `Scoreboard` holds one byte per worker slot, so a server
/// with tens of thousands of slots still fits.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;

/// `apache_status_in`'s half of the [`ScrapeDecoder`] seam.
#[derive(Default)]
pub struct ApacheStatusDecoder {
    telemetry: Telemetry,
    diag: Diagnostics,
}

impl ScrapeDecoder for ApacheStatusDecoder {
    type Error = ParseError;

    fn kind(&self) -> &'static str {
        "apache_status_in"
    }

    fn accept(&self) -> &'static str {
        "text/plain"
    }

    fn max_body_bytes(&self) -> usize {
        MAX_BODY_BYTES
    }

    fn resource(&self, index: usize, url: &str) -> Resource {
        server_resource(index, url)
    }

    fn set_diagnostics(&mut self, diag: Diagnostics) {
        self.diag = diag;
    }

    fn set_telemetry(&mut self, telemetry: Telemetry) {
        self.telemetry = telemetry;
    }

    fn decode(
        &mut self,
        body: &[u8],
        _content_type: Option<&str>,
        timestamp: i64,
    ) -> Result<Vec<Event>, Self::Error> {
        let status = parse(body)?;
        if !status.malformed.is_empty() {
            self.telemetry.count(
                "logit.input.metrics.dropped",
                status.malformed.len() as f64,
                &[("reason", "malformed_value")],
            );
            for (key, expected) in &status.malformed {
                self.diag.warn_throttled(
                    "malformed_value",
                    format_args!(
                        "apache_status_in: skipped the page's '{key}' value, which isn't \
                         {expected}"
                    ),
                );
            }
        }
        if status.duplicates > 0 {
            self.telemetry.count(
                "logit.input.metrics.normalized",
                status.duplicates as f64,
                &[("reason", "duplicate_key")],
            );
        }
        if status.truncated {
            self.telemetry.count("logit.input.lines.dropped", 1.0, &[("reason", "truncated")]);
        }
        Ok(status.to_events(timestamp))
    }
}

/// `apache_status_in`: the scrape driver over [`ApacheStatusDecoder`].
pub type ApacheStatusInput = ScrapeInput<ApacheStatusDecoder>;

/// An input over `targets`, scraped every `interval`.
pub fn input(targets: Vec<String>, interval: std::time::Duration) -> ApacheStatusInput {
    ScrapeInput::with_decoder(ApacheStatusDecoder::default(), targets, interval)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrape::tests::{canned_server, sent_batch};
    use crate::scrape::{SERVER_ADDRESS, SERVER_PORT, URL_PATH};
    use bytes::Bytes;
    use logit_core::interner::resolve;
    use logit_core::{EventBatch, MetricKind, Value};
    use logit_pipeline::test_util::TelemetryProbe;
    use logit_pipeline::Fanout;
    use std::time::Duration;
    use tokio::sync::mpsc;

    const PAGE: &[u8] = b"status-server\nServerVersion: Apache/2.4.69 (Unix)\n\
ServerUptimeSeconds: 30\nTotal Accesses: 5\nBusyWorkers: 1\nIdleWorkers: 74\nScoreboard: _W.\n";

    async fn tick_once(body: Bytes, probe: &TelemetryProbe) -> (EventBatch, u16) {
        let (addr, _) = canned_server(body).await;
        let url = format!("http://{addr}/server-status?auto");
        let telemetry = probe.telemetry("apache", "apache_status_in", "listener");
        let mut input = input(vec![url], Duration::MAX)
            .with_diagnostics(Diagnostics::new("apache").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        let (tx, mut rx) = mpsc::channel(4);
        input.tick(&Fanout::new(vec![tx])).await;
        (sent_batch(&mut rx), addr.port())
    }

    fn gauge(batch: &EventBatch, name: &str) -> Option<f64> {
        batch.events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match m.kind {
                MetricKind::Gauge(v) if resolve(m.name) == name => Some(v.as_f64()),
                _ => None,
            })
        })
    }

    fn names(batch: &EventBatch) -> Vec<&'static str> {
        batch.events.iter().map(|e| resolve(e.metrics[0].name)).collect()
    }

    #[tokio::test]
    async fn a_page_is_its_points_and_three_synthetics_on_the_targets_resource() {
        let mut probe = TelemetryProbe::new();
        let (batch, port) = tick_once(Bytes::from_static(PAGE), &probe).await;

        let mut want = vec!["apache.uptime", "apache.request.count"];
        want.extend(["apache.worker.active", "apache.worker.idle"]);
        want.extend(["apache.worker.status"; 11]);
        want.extend(["apache.worker.limit", "up", "scrape_duration_seconds"]);
        want.push("scrape_samples_scraped");
        assert_eq!(names(&batch), want);
        assert_eq!(gauge(&batch, "up"), Some(1.0));
        assert_eq!(gauge(&batch, "scrape_samples_scraped"), Some(16.0));
        let resource = &batch.resource.attributes;
        assert_eq!(resource.get(SERVER_ADDRESS), Some(&Value::str("127.0.0.1")));
        assert_eq!(resource.get(SERVER_PORT), Some(&Value::I64(i64::from(port))));
        assert_eq!(resource.get(URL_PATH), Some(&Value::str("/server-status")));
        assert_eq!(resource.len(), 3);
        assert_eq!(probe.sum("logit.input.scrapes", &[("class", "2xx")]), 1.0);
        assert_eq!(probe.sum("logit.input.samples", &[]), 16.0);
        for name in ["logit.input.metrics.dropped", "logit.input.metrics.normalized"] {
            assert_eq!(probe.sum(name, &[]), 0.0, "{name}");
        }
        assert_eq!(probe.sum("logit.input.lines.dropped", &[]), 0.0);
    }

    #[tokio::test]
    async fn the_html_page_is_a_parse_error_with_up_at_zero_and_a_hint_to_add_auto() {
        let mut probe = TelemetryProbe::new();
        let html = Bytes::from_static(
            b"<html><head><title>Apache Status</title></head><body>\n\
<dt>Server Version: Apache/2.4.69 (Unix)</dt>\n</body></html>\n",
        );
        let (batch, _) = tick_once(html, &probe).await;

        assert_eq!(names(&batch), ["up", "scrape_duration_seconds", "scrape_samples_scraped"]);
        assert_eq!(gauge(&batch, "up"), Some(0.0));
        assert_eq!(gauge(&batch, "scrape_samples_scraped"), Some(0.0));
        assert_eq!(probe.sum("logit.input.scrapes", &[("class", "parse_error")]), 1.0);
        assert!(ParseError::NoStatusKeys.to_string().contains("?auto"));
    }

    #[tokio::test]
    async fn a_malformed_value_a_repeated_key_and_a_cut_line_are_counted_with_up_at_one() {
        let mut probe = TelemetryProbe::new();
        let body =
            b"BusyWorkers: x\nIdleWorkers: 1\nIdleWorkers: 2\nIdleWorkers: 3\nScoreboard: _W";
        let (batch, _) = tick_once(Bytes::from_static(body), &probe).await;

        assert_eq!(gauge(&batch, "up"), Some(1.0));
        assert_eq!(gauge(&batch, "apache.worker.idle"), Some(3.0));
        assert_eq!(gauge(&batch, "scrape_samples_scraped"), Some(1.0));
        assert_eq!(probe.sum("logit.input.metrics.dropped", &[("reason", "malformed_value")]), 1.0);
        assert_eq!(
            probe.sum("logit.input.metrics.normalized", &[("reason", "duplicate_key")]),
            2.0
        );
        assert_eq!(probe.sum("logit.input.lines.dropped", &[("reason", "truncated")]), 1.0);
        assert_eq!(probe.sum("logit.component.diagnostics", &[("key", "malformed_value")]), 1.0);
    }

    #[tokio::test]
    async fn a_body_past_1_mib_is_oversize() {
        // A literal, not `MAX_BODY_BYTES`, so a changed cap fails here.
        const ONE_MIB: usize = 1024 * 1024;
        let mut probe = TelemetryProbe::new();
        let mut body = PAGE.to_vec();
        body.resize(ONE_MIB + 1, b'\n');
        let (batch, _) = tick_once(Bytes::from(body), &probe).await;

        assert_eq!(gauge(&batch, "up"), Some(0.0));
        assert_eq!(probe.sum("logit.input.scrapes", &[("class", "oversize")]), 1.0);

        let mut at_cap = PAGE.to_vec();
        at_cap.resize(ONE_MIB, b'\n');
        let (batch, _) = tick_once(Bytes::from(at_cap), &probe).await;
        assert_eq!(gauge(&batch, "up"), Some(1.0));
    }
}
