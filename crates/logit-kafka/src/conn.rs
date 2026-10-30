//! One connection to a Kafka broker: a TCP connect, an optional TLS handshake, `ApiVersions`
//! negotiation, an optional SASL exchange, then length-prefixed requests matched to their
//! responses by correlation id.
//!
//! The caller's timeout bounds the connect, the handshake, each request's write, and each
//! response wait, each on its own. A failure says whether any byte of the request could have
//! left ([`RequestError::sent`]), which is what separates `Clean` from `Ambiguous`
//! (`docs/adr/kafka-output.md`, item 5's transport rows). A connection that failed a request is
//! finished: the caller drops it, because its framing position is unknown.
//!
//! Version negotiation (`docs/adr/kafka-output.md`, item 8): [`API_RANGES`] has a row per API
//! `logit` sends, and each API takes the highest version both sides support within its row.
//! The [`Dialer`]'s `required` list names what one client can't run without: the producer's
//! `Produce`, `Metadata`, and `InitProducerId`, the consumer's [`CONSUMER_APIS`].
//! `Produce` reads the broker's advertised minimum as no constraint, because a 4.x broker
//! advertises v0 and rejects anything below v3. A broker without a version of an API the
//! [`Dialer`] requires is `Refused`; any other API it lacks is `None` in [`Versions`]. With
//! `sasl:` set, the broker must offer `SaslHandshake` v1 and a `SaslAuthenticate` version.
//! `ApiVersions` itself is v3 alone.
//!
//! SASL (`docs/adr/kafka-sasl.md`): [`Dialer::connect`] authenticates the connection through
//! [`crate::sasl`] before returning it. A broker that sets `connections.max.reauth.ms` gives the
//! session a lifetime and closes the connection on the first request after it ends, so the
//! connection gets a deadline ([`reauth_deadline`]) and [`Conn::is_open`] reads it as closed
//! from then on, which makes the producer dial a new one instead of re-authenticating in band.

use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, Request, StrBytes};
use logit_pipeline::Fault;
use rustls_pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_rustls::TlsConnector;

use logit_pipeline::tls::{poll_pending_close, AsyncStream, PendingClose};

use crate::error;
use crate::sasl::{self, Authenticator};

/// The `SaslHandshake` version `logit` sends: v1, whose exchange runs in `SaslAuthenticate`
/// requests instead of raw frames.
pub const SASL_HANDSHAKE_VERSION: i16 = 1;

/// The versions of one API that `logit` sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRange {
    pub key: ApiKey,
    pub versions: RangeInclusive<i16>,
    /// Whether the broker's advertised minimum constrains the choice. `false` takes the highest
    /// version up to the broker's maximum even below its advertised minimum.
    pub broker_min_counts: bool,
}

/// Every API `logit` sends after `ApiVersions`, one row each. The consumer's rows are
/// `docs/adr/kafka-input.md`'s item 3: each minimum is Kafka 4.0's floor or the first version with
/// a field the consumer needs, each maximum the last that names a topic by name.
pub const API_RANGES: [ApiRange; 14] = [
    ApiRange { key: ApiKey::Produce, versions: 3..=12, broker_min_counts: false },
    // v4 is the first with `allow_auto_topic_creation`.
    ApiRange { key: ApiKey::Metadata, versions: 4..=12, broker_min_counts: true },
    ApiRange { key: ApiKey::InitProducerId, versions: 0..=5, broker_min_counts: true },
    ApiRange {
        key: ApiKey::SaslHandshake,
        versions: SASL_HANDSHAKE_VERSION..=SASL_HANDSHAKE_VERSION,
        broker_min_counts: true,
    },
    ApiRange { key: ApiKey::SaslAuthenticate, versions: 0..=2, broker_min_counts: true },
    // v4 adds `isolation_level`; v13 names topics by id.
    ApiRange { key: ApiKey::Fetch, versions: 4..=12, broker_min_counts: true },
    // v2 adds `isolation_level`.
    ApiRange { key: ApiKey::ListOffsets, versions: 2..=10, broker_min_counts: true },
    ApiRange { key: ApiKey::OffsetCommit, versions: 2..=9, broker_min_counts: true },
    // v7 adds `require_stable`; v8 moves the group into a `groups` array.
    ApiRange { key: ApiKey::OffsetFetch, versions: 1..=9, broker_min_counts: true },
    // v4 batches its keys into `coordinator_keys`.
    ApiRange { key: ApiKey::FindCoordinator, versions: 0..=6, broker_min_counts: true },
    // From v4 a first join is answered `MEMBER_ID_REQUIRED` with the id to join under.
    ApiRange { key: ApiKey::JoinGroup, versions: 2..=9, broker_min_counts: true },
    ApiRange { key: ApiKey::SyncGroup, versions: 0..=5, broker_min_counts: true },
    ApiRange { key: ApiKey::Heartbeat, versions: 0..=4, broker_min_counts: true },
    // v3 addresses members by list.
    ApiRange { key: ApiKey::LeaveGroup, versions: 0..=5, broker_min_counts: true },
];
/// The APIs a consumer's [`Dialer`] requires: every consumer API in [`API_RANGES`].
pub const CONSUMER_APIS: &[ApiKey] = &[
    ApiKey::Metadata,
    ApiKey::Fetch,
    ApiKey::ListOffsets,
    ApiKey::OffsetCommit,
    ApiKey::OffsetFetch,
    ApiKey::FindCoordinator,
    ApiKey::JoinGroup,
    ApiKey::SyncGroup,
    ApiKey::Heartbeat,
    ApiKey::LeaveGroup,
];
/// The first flexible `ApiVersions` version, which every 2.4 or later broker answers.
const API_VERSIONS_VERSION: i16 = 3;
/// The first `Produce` version that can carry zstd.
pub const PRODUCE_ZSTD_MIN: i16 = 7;
/// The first `Produce` version whose response names records in `record_errors`.
pub const PRODUCE_RECORD_ERRORS_MIN: i16 = 8;
/// A response size no broker answer to these four requests comes near; a larger length prefix
/// is a framing error, not an allocation to make.
const MAX_RESPONSE_BYTES: usize = 64 << 20;

/// What every connection of one client shares.
#[derive(Clone)]
pub struct Dialer {
    pub tls: Option<Arc<rustls::ClientConfig>>,
    pub timeout: Duration,
    pub client_id: StrBytes,
    pub sasl: Option<Arc<dyn Authenticator>>,
    /// The APIs a broker must offer in a version of their [`API_RANGES`] row; a broker that
    /// doesn't is `Refused`.
    pub required: &'static [ApiKey],
}

/// The versions one connection negotiated, one per [`API_RANGES`] row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Versions([Option<i16>; API_RANGES.len()]);

impl Versions {
    /// Nothing negotiated.
    const NONE: Versions = Versions([None; API_RANGES.len()]);

    /// The version to send `key` at, or `None` when the broker offers none in its
    /// [`API_RANGES`] row, or the table has no row for it. A key the [`Dialer`] requires always
    /// has one.
    pub fn for_key(&self, key: ApiKey) -> Option<i16> {
        API_RANGES.iter().position(|row| row.key == key).and_then(|i| self.0[i])
    }

    /// The `SaslAuthenticate` version, when the broker also offers `SaslHandshake` v1: what a
    /// connection with `sasl:` set needs.
    pub fn sasl_authenticate(&self) -> Option<i16> {
        self.for_key(ApiKey::SaslHandshake).and(self.for_key(ApiKey::SaslAuthenticate))
    }
}

/// A failed request: the error, and whether any byte of it could have reached the broker.
#[derive(Debug)]
pub struct RequestError {
    pub sent: bool,
    pub error: anyhow::Error,
}

impl RequestError {
    fn unsent(error: anyhow::Error) -> Self {
        Self { sent: false, error }
    }

    fn sent(error: anyhow::Error) -> Self {
        Self { sent: true, error }
    }

    /// `Clean` when nothing left, `Ambiguous` once anything may have.
    pub fn fault(&self) -> Fault {
        if self.sent {
            Fault::Ambiguous
        } else {
            Fault::Clean
        }
    }
}

/// One negotiated broker connection.
pub struct Conn {
    stream: Box<dyn AsyncStream>,
    tls: bool,
    versions: Versions,
    correlation: i32,
    client_id: StrBytes,
    timeout: Duration,
    /// When to stop reusing the connection, because its SASL session ends ([`reauth_deadline`]).
    deadline: Option<Instant>,
}

/// When to stop reusing a connection whose SASL session ends at `end`, `lifetime` after it began.
/// The margin is `timeout`, so a request written before the deadline is answered or failed
/// before the session ends. A lifetime of twice `timeout` or less gets half the lifetime as its
/// margin instead, so the connection carries some requests; a request the broker then drops at
/// the session's end is `Ambiguous`, and the idempotent resend is deduplicated.
pub fn reauth_deadline(end: Instant, lifetime: Duration, timeout: Duration) -> Instant {
    end - timeout.min(lifetime / 2)
}

impl Dialer {
    /// Connects to `host:port`, with TLS whose server name is `host`, negotiates versions, and
    /// authenticates when `sasl` is set. Every failure is `Clean`, since no request byte of the
    /// caller's goes out before this returns, except a broker without a version of a required API
    /// and a SASL refusal, which are `Refused` ([`crate::sasl`]).
    pub async fn connect(&self, host: &str, port: u16) -> anyhow::Result<Conn> {
        let tcp = timeout(self.timeout, TcpStream::connect((host, port)))
            .await
            .map_err(|_elapsed| {
                anyhow!(
                    "connecting to Kafka broker {host}:{port} timed out after {:?}",
                    self.timeout
                )
            })
            .and_then(|r| r.with_context(|| format!("connecting to Kafka broker {host}:{port}")))
            .context(Fault::Clean)?;
        // A produce request waits on its response; Nagle's algorithm would hold its tail.
        let _ = tcp.set_nodelay(true);
        let stream: Box<dyn AsyncStream> = match &self.tls {
            None => Box::new(tcp),
            Some(config) => {
                let name = ServerName::try_from(host.to_string())
                    .map_err(|err| {
                        anyhow!("Kafka broker host {host:?} isn't a TLS server name: {err}")
                    })
                    .context(Fault::Clean)?;
                let tls =
                    timeout(self.timeout, TlsConnector::from(config.clone()).connect(name, tcp))
                        .await
                        .map_err(|_elapsed| {
                            anyhow!(
                            "TLS handshake with Kafka broker {host}:{port} timed out after {:?}",
                            self.timeout
                        )
                        })
                        .and_then(|r| {
                            r.with_context(|| {
                                format!("TLS handshake with Kafka broker {host}:{port}")
                            })
                        })
                        .context(Fault::Clean)?;
                Box::new(tls)
            }
        };
        let mut conn = Conn {
            stream,
            tls: self.tls.is_some(),
            versions: Versions::NONE,
            correlation: 0,
            client_id: self.client_id.clone(),
            timeout: self.timeout,
            deadline: None,
        };
        let request = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str("logit"))
            .with_client_software_version(StrBytes::from_static_str(env!("CARGO_PKG_VERSION")));
        let response = conn
            .request(&request, API_VERSIONS_VERSION)
            .await
            .map_err(|err| err.error)
            .with_context(|| format!("ApiVersions with Kafka broker {host}:{port}"))
            .context(Fault::Clean)?;
        conn.versions = negotiate(&response, self.required)
            .with_context(|| format!("Kafka broker {host}:{port}"))
            .context(Fault::Refused)?;
        if let Some(auth) = &self.sasl {
            let Some(version) = conn.versions.sasl_authenticate() else {
                return Err(anyhow!(
                    "SASL {} as {}: Kafka broker {host}:{port} doesn't offer SaslHandshake v1 \
                     and SaslAuthenticate v0 to v2",
                    auth.mechanism(),
                    auth.principal()
                ))
                .context(Fault::Refused);
            };
            let end = sasl::authenticate(&mut conn, auth.as_ref(), version, host, port).await?;
            conn.deadline = end.map(|(end, lifetime)| reauth_deadline(end, lifetime, self.timeout));
        }
        Ok(conn)
    }
}

/// The versions to use with a broker that answered `response`. An API in `required` that the
/// broker offers in no version of its [`API_RANGES`] row is an error.
pub fn negotiate(response: &ApiVersionsResponse, required: &[ApiKey]) -> anyhow::Result<Versions> {
    anyhow::ensure!(
        response.error_code == error::code::NONE,
        "ApiVersions answered {}",
        error::name(response.error_code)
    );
    let pick = |row: &ApiRange| {
        let key = row.key;
        let theirs = response
            .api_keys
            .iter()
            .find(|v| v.api_key == key as i16)
            .map(|v| v.min_version..=v.max_version)
            .ok_or_else(|| anyhow!("the broker doesn't offer {key:?}"))?;
        let ours = &row.versions;
        let hi = (*theirs.end()).min(*ours.end());
        let lo = if row.broker_min_counts {
            (*theirs.start()).max(*ours.start())
        } else {
            *ours.start()
        };
        anyhow::ensure!(
            hi >= lo,
            "the broker offers {key:?} v{} to v{}; logit sends v{} to v{}",
            theirs.start(),
            theirs.end(),
            ours.start(),
            ours.end()
        );
        Ok(hi)
    };
    for &key in required {
        let row = API_RANGES.iter().find(|row| row.key == key);
        pick(row.ok_or_else(|| anyhow!("logit sends no version of {key:?}"))?)?;
    }
    let mut versions = Versions::NONE;
    for (slot, row) in versions.0.iter_mut().zip(&API_RANGES) {
        *slot = pick(row).ok();
    }
    Ok(versions)
}

impl Conn {
    /// A connection over `stream` with `versions` already negotiated, for tests that script
    /// the stream.
    #[cfg(test)]
    pub fn over(stream: Box<dyn AsyncStream>, tls: bool, timeout: Duration) -> Self {
        let mut versions = Versions::NONE;
        for (slot, row) in versions.0.iter_mut().zip(&API_RANGES) {
            *slot = Some(*row.versions.end());
        }
        Self {
            stream,
            tls,
            versions,
            correlation: 0,
            client_id: StrBytes::from_static_str("logit"),
            timeout,
            deadline: None,
        }
    }

    pub fn versions(&self) -> Versions {
        self.versions
    }

    /// Whether a reused connection is still worth writing to: one poll, no wait. A broker closes
    /// an idle connection (`connections.max.idle.ms`), and writing a request into one already
    /// closed would turn a `Clean` redial into an `Ambiguous` failure. A connection past its SASL
    /// deadline reads as closed, because the broker closes it on the next request.
    pub async fn is_open(&mut self) -> bool {
        if self.past_deadline() {
            return false;
        }
        matches!(poll_pending_close(&mut *self.stream, &mut [0u8; 1]).await, PendingClose::Open)
    }

    /// Whether the connection's SASL deadline has passed, with no socket poll: the check before
    /// each request after the first that one broker job writes.
    pub fn past_deadline(&self) -> bool {
        self.deadline.is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Sends `request` at `version` and waits for its response.
    pub async fn request<R: Request>(
        &mut self,
        request: &R,
        version: i16,
    ) -> Result<R::Response, RequestError> {
        self.request_within(request, version, self.timeout).await
    }

    /// [`Conn::request`] with `wait` bounding the response wait in place of the timeout: a
    /// `JoinGroup` the coordinator holds until the group rejoins, or a `Fetch` the broker holds
    /// up to its `max_wait_ms`.
    pub async fn request_within<R: Request>(
        &mut self,
        request: &R,
        version: i16,
        wait: Duration,
    ) -> Result<R::Response, RequestError> {
        let correlation = self.write(request, version).await?;
        self.read::<R>(version, correlation, wait).await
    }

    /// Sends `request` at `version` without waiting: a `Produce` with `acks: none`, which the
    /// broker never answers.
    pub async fn send_only<R: Request>(
        &mut self,
        request: &R,
        version: i16,
    ) -> Result<(), RequestError> {
        self.write(request, version).await.map(|_| ())
    }

    /// Frames and writes one request under the timeout; returns its correlation id.
    async fn write<R: Request>(&mut self, request: &R, version: i16) -> Result<i32, RequestError> {
        let key = ApiKey::try_from(R::KEY).expect("every request type has an API key");
        self.correlation = self.correlation.wrapping_add(1);
        let correlation = self.correlation;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(correlation)
            .with_client_id(Some(self.client_id.clone()));
        let mut frame = BytesMut::new();
        frame.put_i32(0);
        header
            .encode(&mut frame, key.request_header_version(version))
            .and_then(|()| request.encode(&mut frame, version))
            .with_context(|| format!("encoding {key:?} v{version}"))
            .map_err(RequestError::unsent)?;
        let len = i32::try_from(frame.len() - 4)
            .map_err(|_| RequestError::unsent(anyhow!("{key:?} request too large to frame")))?;
        frame[..4].copy_from_slice(&len.to_be_bytes());

        let deadline = Instant::now() + self.timeout;
        let stalled =
            |what: &str| anyhow!("{what} {key:?} request timed out after {:?}", self.timeout);
        // On plaintext, a first write that accepts nothing has sent nothing. On TLS, a write
        // error can follow ciphertext already on the wire, so any failure may have sent bytes.
        let first = match timeout_at(deadline, self.stream.write(&frame)).await {
            Ok(Ok(n)) if n > 0 => n,
            Ok(Ok(_)) => {
                let err = anyhow!("writing a {key:?} request: the connection accepted nothing");
                return Err(RequestError { sent: self.tls, error: err });
            }
            Ok(Err(err)) => {
                let err = anyhow::Error::from(err).context(format!("writing a {key:?} request"));
                return Err(RequestError { sent: self.tls, error: err });
            }
            Err(_elapsed) => {
                return Err(RequestError { sent: self.tls, error: stalled("writing a") });
            }
        };
        let rest = async {
            self.stream.write_all(&frame[first..]).await?;
            self.stream.flush().await
        };
        match timeout_at(deadline, rest).await {
            Ok(Ok(())) => Ok(correlation),
            Ok(Err(err)) => Err(RequestError::sent(
                anyhow::Error::from(err).context(format!("writing a {key:?} request")),
            )),
            Err(_elapsed) => Err(RequestError::sent(stalled("writing a"))),
        }
    }

    /// Reads the response to the request numbered `correlation`, waiting at most `wait`.
    async fn read<R: Request>(
        &mut self,
        version: i16,
        correlation: i32,
        wait: Duration,
    ) -> Result<R::Response, RequestError> {
        let key = ApiKey::try_from(R::KEY).expect("every request type has an API key");
        let body = async {
            let len = self.stream.read_i32().await?;
            let len = usize::try_from(len)
                .ok()
                .filter(|&n| n <= MAX_RESPONSE_BYTES)
                .ok_or_else(|| anyhow!("a {key:?} response declares {len} bytes"))?;
            let mut body = vec![0; len];
            self.stream.read_exact(&mut body).await?;
            anyhow::Ok(Bytes::from(body))
        };
        let mut body = timeout(wait, body)
            .await
            .map_err(|_elapsed| anyhow!("waiting for a {key:?} response timed out after {wait:?}"))
            .and_then(|r| r.with_context(|| format!("reading a {key:?} response")))
            .map_err(RequestError::sent)?;
        let header = ResponseHeader::decode(&mut body, key.response_header_version(version))
            .with_context(|| format!("decoding a {key:?} response header"))
            .map_err(RequestError::sent)?;
        if header.correlation_id != correlation {
            return Err(RequestError::sent(anyhow!(
                "a {key:?} response carries correlation id {}, expected {correlation}",
                header.correlation_id
            )));
        }
        R::Response::decode(&mut body, version)
            .with_context(|| format!("decoding a {key:?} v{version} response"))
            .map_err(RequestError::sent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io;

    use kafka_protocol::messages::api_versions_response::ApiVersion;

    use crate::test_stream::{FakeStream, ReadStep, WriteStep};

    /// What a producer's dialer requires.
    const PRODUCER: &[ApiKey] = &[ApiKey::Produce, ApiKey::Metadata, ApiKey::InitProducerId];

    /// The versions `negotiate` picked for `Produce`, `Metadata`, and `InitProducerId`, and
    /// `SaslAuthenticate` beside `SaslHandshake` v1.
    fn picked(versions: Versions) -> (Option<i16>, Option<i16>, Option<i16>, Option<i16>) {
        (
            versions.for_key(ApiKey::Produce),
            versions.for_key(ApiKey::Metadata),
            versions.for_key(ApiKey::InitProducerId),
            versions.sasl_authenticate(),
        )
    }

    fn offered(ranges: &[(ApiKey, i16, i16)]) -> ApiVersionsResponse {
        ApiVersionsResponse::default().with_api_keys(
            ranges
                .iter()
                .map(|&(key, min, max)| {
                    ApiVersion::default()
                        .with_api_key(key as i16)
                        .with_min_version(min)
                        .with_max_version(max)
                })
                .collect(),
        )
    }

    /// The ranges `apache/kafka:4.2.0` advertises (`testdata/interop/kafka/api-versions.txt`).
    #[test]
    fn a_kafka_4_2_broker_negotiates_produce_v12_and_ignores_its_advertised_minimum() {
        let versions = negotiate(
            &offered(&[
                (ApiKey::Produce, 0, 13),
                (ApiKey::Metadata, 0, 13),
                (ApiKey::ApiVersions, 0, 4),
                (ApiKey::InitProducerId, 0, 5),
            ]),
            PRODUCER,
        )
        .unwrap();
        assert_eq!(picked(versions), (Some(12), Some(12), Some(5), None));
    }

    #[test]
    fn an_older_broker_negotiates_its_own_maximum() {
        let versions = negotiate(
            &offered(&[
                (ApiKey::Produce, 0, 7),
                (ApiKey::Metadata, 0, 9),
                (ApiKey::InitProducerId, 0, 2),
            ]),
            PRODUCER,
        )
        .unwrap();
        assert_eq!(picked(versions), (Some(7), Some(9), Some(2), None));
    }

    #[test]
    fn a_broker_without_produce_v3_is_refused() {
        let err = negotiate(
            &offered(&[
                (ApiKey::Produce, 0, 2),
                (ApiKey::Metadata, 0, 9),
                (ApiKey::InitProducerId, 0, 2),
            ]),
            PRODUCER,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Produce v0 to v2"), "{err}");
    }

    fn metadata_request() -> kafka_protocol::messages::MetadataRequest {
        kafka_protocol::messages::MetadataRequest::default()
    }

    async fn request_over(fake: &FakeStream, tls: bool) -> RequestError {
        let mut conn = Conn::over(Box::new(fake.clone()), tls, Duration::from_secs(5));
        conn.request(&metadata_request(), 12).await.unwrap_err()
    }

    /// A plaintext first write that fails or accepts nothing has sent nothing.
    #[tokio::test]
    async fn a_plaintext_first_write_that_accepts_nothing_is_clean() {
        for step in [WriteStep::Fail(io::ErrorKind::BrokenPipe), WriteStep::Zero] {
            let fake = FakeStream::new().on_write(1, step);
            let err = request_over(&fake, false).await;
            assert_eq!(err.fault(), Fault::Clean, "{step:?}: {:#}", err.error);
            assert!(fake.state().flushed.is_empty());
        }
    }

    /// On TLS, a write error can follow ciphertext already sent.
    #[tokio::test]
    async fn a_tls_write_failure_is_ambiguous() {
        let fake = FakeStream::new().on_write(1, WriteStep::Fail(io::ErrorKind::BrokenPipe));
        assert_eq!(request_over(&fake, true).await.fault(), Fault::Ambiguous);
    }

    #[tokio::test]
    async fn a_failure_after_part_of_the_request_is_written_is_ambiguous() {
        let fake = FakeStream::new()
            .on_write(1, WriteStep::Short(3))
            .on_write(2, WriteStep::Fail(io::ErrorKind::ConnectionReset));
        assert_eq!(request_over(&fake, false).await.fault(), Fault::Ambiguous);
    }

    #[tokio::test]
    async fn a_close_while_waiting_for_the_response_is_ambiguous() {
        let fake = FakeStream::new().reading(ReadStep::Eof);
        let err = request_over(&fake, false).await;
        assert_eq!(err.fault(), Fault::Ambiguous);
        assert!(!fake.state().flushed.is_empty(), "the request was written whole");
    }

    /// A response for another request means the framing is lost.
    #[tokio::test]
    async fn a_response_with_the_wrong_correlation_id_is_ambiguous() {
        let mut frame = BytesMut::new();
        frame.put_i32(5);
        frame.put_i32(99);
        // The flexible header's empty tagged-field section.
        frame.put_u8(0);
        let fake = FakeStream::new().reading(ReadStep::Bytes(frame.to_vec()));
        let err = request_over(&fake, false).await;
        assert_eq!(err.fault(), Fault::Ambiguous);
        assert!(format!("{:#}", err.error).contains("correlation id 99"), "{:#}", err.error);
    }

    #[tokio::test]
    async fn a_response_length_past_the_cap_is_ambiguous() {
        let fake = FakeStream::new().reading(ReadStep::Bytes((-1i32).to_be_bytes().to_vec()));
        assert_eq!(request_over(&fake, false).await.fault(), Fault::Ambiguous);
    }

    /// An API outside `required` that the broker lacks is `None`, not a refusal, as is a key
    /// with no row.
    #[test]
    fn an_api_outside_the_required_set_is_none_when_the_broker_lacks_it() {
        let versions =
            negotiate(&offered(&[(ApiKey::Metadata, 0, 13)]), &[ApiKey::Metadata]).unwrap();
        assert_eq!(versions.for_key(ApiKey::Metadata), Some(12));
        assert_eq!(versions.for_key(ApiKey::Produce), None);
        assert_eq!(versions.for_key(ApiKey::Fetch), None);
    }

    /// `SaslAuthenticate` takes the highest version both sides offer up to v2, and only beside
    /// `SaslHandshake` v1.
    #[test]
    fn sasl_negotiates_saslauthenticate_up_to_v2_beside_saslhandshake_v1() {
        let base =
            [(ApiKey::Produce, 0, 13), (ApiKey::Metadata, 0, 13), (ApiKey::InitProducerId, 0, 5)];
        let with = |extra: &[(ApiKey, i16, i16)]| {
            let all: Vec<_> = base.iter().chain(extra).copied().collect();
            negotiate(&offered(&all), PRODUCER).unwrap().sasl_authenticate()
        };
        assert_eq!(
            with(&[(ApiKey::SaslHandshake, 0, 1), (ApiKey::SaslAuthenticate, 0, 2)]),
            Some(2)
        );
        assert_eq!(
            with(&[(ApiKey::SaslHandshake, 0, 1), (ApiKey::SaslAuthenticate, 0, 1)]),
            Some(1)
        );
        assert_eq!(
            with(&[(ApiKey::SaslHandshake, 0, 1), (ApiKey::SaslAuthenticate, 0, 9)]),
            Some(2)
        );
        assert_eq!(with(&[(ApiKey::SaslHandshake, 0, 0), (ApiKey::SaslAuthenticate, 0, 2)]), None);
        assert_eq!(with(&[(ApiKey::SaslHandshake, 0, 1)]), None);
        assert_eq!(with(&[]), None);
    }

    /// The margin is the timeout, or half of a lifetime no longer than twice the timeout.
    #[test]
    fn the_reauth_deadline_leaves_the_timeout_or_half_a_short_lifetime() {
        let end = Instant::now() + Duration::from_secs(3600);
        let s = Duration::from_secs;
        assert_eq!(reauth_deadline(end, s(3600), s(30)), end - s(30));
        assert_eq!(reauth_deadline(end, s(60), s(30)), end - s(30));
        assert_eq!(reauth_deadline(end, s(40), s(30)), end - s(20));
        assert_eq!(reauth_deadline(end, s(10), s(30)), end - s(5));
    }

    /// A connection past its deadline reads as closed with the socket still open.
    #[tokio::test]
    async fn a_connection_past_its_sasl_deadline_reads_as_closed() {
        let fake = FakeStream::new();
        let mut conn = Conn::over(Box::new(fake.clone()), false, Duration::from_secs(5));
        assert!(conn.is_open().await);
        conn.deadline = Some(Instant::now() + Duration::from_secs(3600));
        assert!(conn.is_open().await);
        conn.deadline = Some(Instant::now());
        assert!(!conn.is_open().await);
    }

    #[test]
    fn an_api_versions_error_fails_negotiation() {
        let response = offered(&[]).with_error_code(error::code::UNSUPPORTED_VERSION);
        assert!(negotiate(&response, &[]).is_err());
    }
}
