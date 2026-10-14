//! The resource ⊕ event attribute merge-join (event wins) for sinks that render both onto one
//! wire representation, such as `influxdb_out`'s and `statsd_out`'s tags, and the private
//! namespace test those renders apply. Both live in `logit-core` so `logit-proto`'s codecs can
//! share them; these are the local names.

#[allow(unused_imports)]
pub(crate) use logit_core::attrs::is_private;
pub(crate) use logit_core::attrs::merged;
