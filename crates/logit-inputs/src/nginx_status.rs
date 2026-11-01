//! `nginx_stub_status_in`: the scrape driver ([`crate::scrape`]) over
//! [`logit_proto::nginx_status`], which holds the page's grammar and its mapping table. Each
//! target's batches carry [`crate::scrape::server_resource`]'s `server.address`, `server.port`,
//! and `url.path`.
//!
//! Beyond the driver's counters, the decoder counts each field whose digits don't fit an `i64` as
//! `logit.input.metrics.dropped{reason="out_of_range"}`, and leaves `up` at `1` for that page.
//! A body that isn't a `stub_status` page is the driver's `parse_error` class.

use crate::scrape::{server_resource, ScrapeDecoder, ScrapeInput};
use logit_core::{Diagnostics, Event, Resource, Telemetry};
use logit_proto::nginx_status::{parse, ParseError};

/// The largest page this input reads. A `stub_status` page is about 100 bytes, so anything near
/// this is a different page.
pub const MAX_BODY_BYTES: usize = 64 * 1024;

/// `nginx_stub_status_in`'s half of the [`ScrapeDecoder`] seam.
pub struct NginxStatusDecoder {
    telemetry: Telemetry,
}

impl ScrapeDecoder for NginxStatusDecoder {
    type Error = ParseError;

    fn kind(&self) -> &'static str {
        "nginx_stub_status_in"
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

    fn set_diagnostics(&mut self, _diag: Diagnostics) {}

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
        let out_of_range = status.out_of_range();
        if out_of_range > 0 {
            self.telemetry.count(
                "logit.input.metrics.dropped",
                out_of_range as f64,
                &[("reason", "out_of_range")],
            );
        }
        Ok(status.to_events(timestamp))
    }
}

/// `nginx_stub_status_in`: the scrape driver over [`NginxStatusDecoder`].
pub type NginxStubStatusInput = ScrapeInput<NginxStatusDecoder>;

/// An input over `targets`, scraped every `interval`.
pub fn input(targets: Vec<String>, interval: std::time::Duration) -> NginxStubStatusInput {
    let decoder = NginxStatusDecoder { telemetry: Telemetry::default() };
    ScrapeInput::with_decoder(decoder, targets, interval)
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

    const PAGE: &[u8] = b"Active connections: 5 \nserver accepts handled requests\n 6 6 3 \n\
Reading: 1 Writing: 1 Waiting: 3 \n";

    async fn tick_once(body: Bytes, probe: &TelemetryProbe) -> (EventBatch, String) {
        let (addr, _) = canned_server(body).await;
        let url = format!("http://{addr}/nginx_status?k=v");
        let mut input = input(vec![url], Duration::MAX).with_telemetry(probe.telemetry(
            "nginx",
            "nginx_stub_status_in",
            "listener",
        ));
        let (tx, mut rx) = mpsc::channel(4);
        input.tick(&Fanout::new(vec![tx])).await;
        (sent_batch(&mut rx), addr.port().to_string())
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
    async fn a_page_is_seven_events_and_three_synthetics_on_the_targets_resource() {
        let mut probe = TelemetryProbe::new();
        let (batch, port) = tick_once(Bytes::from_static(PAGE), &probe).await;

        assert_eq!(
            names(&batch),
            [
                "nginx.connections_accepted",
                "nginx.connections_handled",
                "nginx.requests",
                "nginx.connections_current",
                "nginx.connections_current",
                "nginx.connections_current",
                "nginx.connections_current",
                "up",
                "scrape_duration_seconds",
                "scrape_samples_scraped",
            ]
        );
        assert_eq!(gauge(&batch, "up"), Some(1.0));
        assert_eq!(gauge(&batch, "scrape_samples_scraped"), Some(7.0));
        let resource = &batch.resource.attributes;
        assert_eq!(resource.get(SERVER_ADDRESS), Some(&Value::str("127.0.0.1")));
        assert_eq!(resource.get(SERVER_PORT), Some(&Value::I64(port.parse().unwrap())));
        assert_eq!(resource.get(URL_PATH), Some(&Value::str("/nginx_status")));
        assert_eq!(resource.len(), 3);
        assert_eq!(probe.sum("logit.input.scrapes", &[("class", "2xx")]), 1.0);
        assert_eq!(probe.sum("logit.input.samples", &[]), 7.0);
    }

    #[tokio::test]
    async fn a_body_that_isnt_the_page_is_a_parse_error_with_up_at_zero_and_no_decoded_events() {
        let mut probe = TelemetryProbe::new();
        let cut = Bytes::from_static(&PAGE[..PAGE.len() - 1]);
        let (batch, _) = tick_once(cut, &probe).await;

        assert_eq!(names(&batch), ["up", "scrape_duration_seconds", "scrape_samples_scraped"]);
        assert_eq!(gauge(&batch, "up"), Some(0.0));
        assert_eq!(gauge(&batch, "scrape_samples_scraped"), Some(0.0));
        assert_eq!(probe.sum("logit.input.scrapes", &[("class", "parse_error")]), 1.0);
    }

    #[tokio::test]
    async fn an_out_of_range_field_is_dropped_and_counted_with_up_at_one() {
        let mut probe = TelemetryProbe::new();
        let body = b"Active connections: 18446744073709551615 \nserver accepts handled requests\n \
6 6 3 \nReading: 1 Writing: 1 Waiting: 3 \n";
        let (batch, _) = tick_once(Bytes::from_static(body), &probe).await;

        assert_eq!(gauge(&batch, "up"), Some(1.0));
        assert_eq!(gauge(&batch, "scrape_samples_scraped"), Some(6.0));
        assert_eq!(batch.events.len(), 9);
        assert_eq!(probe.sum("logit.input.metrics.dropped", &[("reason", "out_of_range")]), 1.0);
    }

    #[tokio::test]
    async fn a_body_past_64_kib_is_oversize() {
        // A literal, not `MAX_BODY_BYTES`, so a changed cap fails here.
        const SIXTY_FOUR_KIB: usize = 65536;
        let mut probe = TelemetryProbe::new();
        let mut body = PAGE.to_vec();
        body.resize(SIXTY_FOUR_KIB + 1, b'\n');
        let (batch, _) = tick_once(Bytes::from(body), &probe).await;

        assert_eq!(gauge(&batch, "up"), Some(0.0));
        assert_eq!(probe.sum("logit.input.scrapes", &[("class", "oversize")]), 1.0);

        let mut at_cap = PAGE.to_vec();
        at_cap.resize(SIXTY_FOUR_KIB, b'\n');
        let (batch, _) = tick_once(Bytes::from(at_cap), &probe).await;
        assert_eq!(gauge(&batch, "up"), Some(1.0));
    }
}
