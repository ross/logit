//! The resource-attributes-overridden-by-event-attributes merge-join shared by every sink that
//! renders both onto one wire representation (`influxdb_out`'s tags, `statsd_out`'s tags).
//!
//! Originally `influxdb_out::render_tag_suffix`'s own inline walk; lifted out once `statsd_out`
//! needed the identical merge, with a different emission format on top -- and lifted again, into
//! `logit-core`, once the Prometheus codec needed the same merge for its labels
//! (`crates/logit-proto/src/prometheus/`): `logit-proto` cannot depend on `logit-outputs`, the
//! dependency runs the other way, so the one definition lives in the crate both share. This module
//! stays as the name every sink here already calls.

pub(crate) use logit_core::attrs::merged;
