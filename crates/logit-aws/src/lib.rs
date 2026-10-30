//! AWS request signing and credentials for the sinks that talk to AWS: SigV4 on `ring`
//! ([`sigv4`]) and a hand-rolled subset of the AWS SDK's default credential chain ([`creds`]).
//!
//! Faults carry `logit_pipeline::Fault` classes, so a caller's sink retries a transient lookup
//! failure and refuses a misconfigured host. Messages and `Debug` output never carry a secret.

pub mod creds;
mod http;
pub mod sigv4;

#[cfg(any(test, feature = "test-util"))]
pub mod test_stub;
