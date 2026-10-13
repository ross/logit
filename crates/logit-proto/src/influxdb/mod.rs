//! The InfluxDB 2.x line-protocol codec, both directions: [`decode`] turns a `/api/v2/write`
//! body into events, [`encode`] packs a batch of events back into lines, [`line`] is the lexer
//! both share, and [`response`] writes the error JSON an InfluxDB client expects.
//!
//! **This module doc is the codec's canonical mapping table and normalization list**, which docs,
//! tests, and examples point at.
//! [ADR `influxdb-line-protocol-relay`](../../../../docs/adr/influxdb-line-protocol-relay.md) is
//! the decision record; it carries the normalization list with identical numbering.
//!
//! TODO(influx/w2): the decode table, encode table, and permitted normalizations.

pub mod decode;
pub mod encode;
pub mod line;
pub mod response;

/// The event attribute carrying a point's measurement, so the encoder can rebuild the line and
/// cross-protocol egress keeps the measurement in the record name (`<measurement>.<field>`).
pub const ATTR_MEASUREMENT: &str = "influxdb.measurement";
/// A string field rides as `influxdb.field.<key>` (`Value::Str`); strings are not metrics.
pub const FIELD_ATTR_PREFIX: &str = "influxdb.field.";
/// Every carrier attribute shares this prefix; the encoder never writes one as a tag.
pub const CARRIER_PREFIX: &str = "influxdb.";
pub const RESOURCE_ATTR_ORG: &str = "influxdb.org";
pub const RESOURCE_ATTR_ORG_ID: &str = "influxdb.org_id";
pub const RESOURCE_ATTR_BUCKET: &str = "influxdb.bucket";

/// The `precision` query parameter of a write: the unit of every timestamp in the body, and the
/// unit a timestamp-less point's arrival time is truncated to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precision {
    Ns,
    Us,
    Ms,
    S,
}

impl Precision {
    /// The four spellings influxd accepts; anything else is a `400`.
    pub fn from_query(s: &str) -> Option<Self> {
        match s {
            "ns" => Some(Self::Ns),
            "us" => Some(Self::Us),
            "ms" => Some(Self::Ms),
            "s" => Some(Self::S),
            _ => None,
        }
    }

    fn factor(self) -> i64 {
        match self {
            Self::Ns => 1,
            Self::Us => 1_000,
            Self::Ms => 1_000_000,
            Self::S => 1_000_000_000,
        }
    }

    /// Scales a wire timestamp to nanoseconds; `None` when it overflows.
    pub fn scale_to_ns(self, t: i64) -> Option<i64> {
        t.checked_mul(self.factor())
    }

    /// Truncates a nanosecond instant to this unit, as influxd stamps a timestamp-less point.
    pub fn truncate(self, ns: i64) -> i64 {
        ns - ns.rem_euclid(self.factor())
    }
}
