//! The resource ⊕ event attribute merge-join (event wins) for sinks that render both onto one
//! wire representation, such as `influxdb_out`'s and `statsd_out`'s tags. It lives in
//! `logit-core` so `logit-proto`'s Prometheus codec can share it; this is the local name.

pub(crate) use logit_core::attrs::merged;
