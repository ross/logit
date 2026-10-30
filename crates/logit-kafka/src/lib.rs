//! What `kafka_out` and `kafka_in` share: the broker connection ([`conn`]), its SASL mechanisms
//! ([`sasl`]), Kafka's error codes and the fault class of a SASL answer ([`error`]), and the
//! [`KafkaSasl`] settings `logit-cli` builds from config; and `kafka_in`'s consumer-group client
//! ([`consumer`]), which has no config of its own. Behind the `test-util` feature, the
//! in-process fake broker ([`fake_broker`]) and the scripted stream ([`test_stream`]) both
//! crates' tests drive.
//!
//! It sits between `logit-pipeline` and the two component crates, depending on neither
//! `logit-config` nor `logit-outputs`, so `logit-inputs` reaches the connection without a sink
//! crate in its graph. The record-batch codec is `logit_proto::kafka`'s, since it does no I/O,
//! and SigV4 and the AWS credential chain are `logit-aws`'s. `http` is a TLS-less copy of the
//! helpers the `OAUTHBEARER` token endpoint uses. [ADR
//! `kafka-input`](../../../docs/adr/kafka-input.md)'s "1. Code layout: `logit-kafka`, and the
//! codec in `logit-proto`" has the reasons for each placement.
//!
//! Faults carry `logit_pipeline::Fault` classes: a connect, TLS, or `ApiVersions` request failure
//! is `Clean`; an `ApiVersions` error code, a broker without a required API version, or a SASL
//! refusal is `Refused`; and a request failure is `Clean` or `Ambiguous` by whether a byte of it
//! could have left ([`conn::RequestError::fault`]).

pub mod conn;
pub mod consumer;
pub mod error;
mod http;
pub mod sasl;

#[cfg(any(test, feature = "test-util"))]
pub mod fake_broker;
#[cfg(any(test, feature = "test-util"))]
pub mod test_stream;

pub use sasl::{KafkaSasl, OAuthToken};
