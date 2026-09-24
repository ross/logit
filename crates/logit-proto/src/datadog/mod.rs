//! Datadog's intake API and the Datadog Agent's own payloads: the codecs behind `datadog_in` and
//! `datadog_out` ([ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md),
//! [`docs/plans/datadog-relay.md`](../../../../docs/plans/datadog-relay.md)). One submodule per
//! payload family, each documenting its own wire ↔ model mapping table in the format
//! [`crate::collectd`]'s module doc set; this doc holds what they share.
//!
//! Like [`crate::prometheus`], this family implements none of [`crate::Encoder`],
//! [`crate::FramedEncoder`], or [`crate::SignalEncoder`]: Datadog has several endpoints per
//! signal (series, sketches, and distribution points are all "metrics"), each with its own body
//! shape, so the decoders are methods on [`DatadogDecoder`] taking one already-decompressed request
//! body, and the encoders are methods on [`DatadogEncoder`] producing one body per route. HTTP
//! concerns (routing, `Content-Type`, `Content-Encoding`, zstd/gzip/deflate) belong to the
//! listener and sink in `logit-inputs`/`logit-outputs`, never here.
//!
//! # Shared vocabulary
//!
//! Raw Datadog fields the model has no typed home for live in event attributes under `datadog.*`
//! ([ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md)'s carrier rule); the
//! constants below are the only spelling. `host.name` carries a series' `host` resource, a
//! sketch's `host`, or a log's `hostname` fallback. Events and service checks reuse
//! `crates/logit-inputs/src/statsd.rs`'s `statsd.event.*` / `statsd.service_check.*` names: they
//! are the same Datadog concepts DogStatsD carries. Tags fold into attributes by [`tags`]'s rule;
//! timestamps convert by [`time`]'s.
//!
//! # Metrics (`series`, `sketches`)
//!
//! *Filled by the metrics implementer: decode table, encode table, permitted normalizations.*
//!
//! # Logs, events, service checks (`logs`, `events`, `service_checks`)
//!
//! *Filled by the logs implementer: decode table, encode table, permitted normalizations.*

pub mod generated;
pub mod tags;
pub mod time;

pub mod series;
pub mod sketches;

pub mod events;
pub mod logs;
pub mod service_checks;

use logit_core::{Diagnostics, Telemetry};

/// `host.name`: the Datadog host of a series/sketch/log, as an event attribute.
pub const ATTR_HOST_NAME: &str = "host.name";
/// `datadog.type`: `rate` or `unspecified`; absent for `count` and `gauge`, whose model kinds say
/// so themselves.
pub const ATTR_TYPE: &str = "datadog.type";
/// `datadog.interval`: a count's or rate's interval in seconds (I64), present only when nonzero.
pub const ATTR_INTERVAL: &str = "datadog.interval";
/// `datadog.source_type_name`: the check or integration that produced a series.
pub const ATTR_SOURCE_TYPE_NAME: &str = "datadog.source_type_name";
/// `datadog.device`: the v1 series `device` field.
pub const ATTR_DEVICE: &str = "datadog.device";
/// `datadog.resources`: every series resource whose `type` isn't `host`, as an `Array` of
/// `Map{type, name}`.
pub const ATTR_RESOURCES: &str = "datadog.resources";
/// `datadog.origin.product` / `.category` / `.service`: the protobuf `Origin` codes (U64).
pub const ATTR_ORIGIN_PRODUCT: &str = "datadog.origin.product";
pub const ATTR_ORIGIN_CATEGORY: &str = "datadog.origin.category";
pub const ATTR_ORIGIN_SERVICE: &str = "datadog.origin.service";
/// `datadog.origin.metric_type`: the JSON v2 API's extra origin code, which the protobuf lacks.
pub const ATTR_ORIGIN_METRIC_TYPE: &str = "datadog.origin.metric_type";
/// `datadog.event_type`: the Agent-only event field.
pub const ATTR_EVENT_TYPE: &str = "datadog.event_type";
/// `datadog.event.device_name` / `.related_event_id`: the public events API's extra fields.
pub const ATTR_EVENT_DEVICE_NAME: &str = "datadog.event.device_name";
pub const ATTR_EVENT_RELATED_EVENT_ID: &str = "datadog.event.related_event_id";
/// `datadog.source`: a log's `ddsource` when it arrives from elsewhere than a Datadog log payload
/// (an upstream `set`); a decoded Datadog log keeps `ddsource` verbatim instead.
pub const ATTR_SOURCE: &str = "datadog.source";
/// `datadog.agent.hostname`: an events envelope's `internalHostname`, as a resource attribute.
pub const RESOURCE_ATTR_AGENT_HOSTNAME: &str = "datadog.agent.hostname";

/// Decodes Datadog intake bodies, one per route. Carries its [`Diagnostics`] and [`Telemetry`]
/// so a malformed item can be dropped and counted while the rest of a request decodes;
/// default-constructed handles make those no-ops for a codec used standalone.
#[derive(Default)]
pub struct DatadogDecoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
}

impl DatadogDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

/// Encodes an `EventBatch` into Datadog intake bodies, one per route. Every skip and degrade is
/// counted through [`Telemetry`]; a disabled handle costs nothing.
#[derive(Default)]
pub struct DatadogEncoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
    /// `ddsource` for a log that carries neither `ddsource` nor `datadog.source`; `None` omits it.
    default_source: Option<String>,
}

impl DatadogEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    pub fn with_default_source(mut self, source: impl Into<String>) -> Self {
        self.default_source = Some(source.into());
        self
    }
}
