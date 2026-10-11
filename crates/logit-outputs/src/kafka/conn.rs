//! One connection to a Kafka broker: a TCP connect, an optional TLS handshake, `ApiVersions`
//! negotiation, then length-prefixed requests matched to their responses by correlation id.
//!
//! The caller's timeout bounds the connect, the handshake, each request's write, and each
//! response wait, each on its own. A failure says whether any byte of the request could have
//! left ([`RequestError::sent`]), which is what separates `Clean` from `Ambiguous`
//! (`docs/adr/kafka-output.md`, item 5's transport rows). A connection that failed a request is
//! finished: the caller drops it, because its framing position is unknown.
//!
//! Version negotiation (item 8): `Produce` takes the highest version both sides support within
//! v3 to v12, reading the broker's advertised minimum as no constraint, because a 4.x broker
//! advertises v0 and rejects anything below v3. `Metadata` takes the highest within v4 (the first
//! with `allow_auto_topic_creation`) to v12, and `InitProducerId` the highest within v0 to v5.

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

use super::fault;
use crate::tls::{poll_pending_close, AsyncStream, PendingClose};

/// The `Produce` versions `kafka_out` sends.
pub(crate) const PRODUCE_VERSIONS: RangeInclusive<i16> = 3..=12;
const METADATA_VERSIONS: RangeInclusive<i16> = 4..=12;
const INIT_PRODUCER_ID_VERSIONS: RangeInclusive<i16> = 0..=5;
/// The first flexible `ApiVersions` version, which every 2.4 or later broker answers.
const API_VERSIONS_VERSION: i16 = 3;
/// The first `Produce` version that can carry zstd.
pub(crate) const PRODUCE_ZSTD_MIN: i16 = 7;
/// The first `Produce` version whose response names records in `record_errors`.
pub(crate) const PRODUCE_RECORD_ERRORS_MIN: i16 = 8;
/// A response size no broker answer to these four requests comes near; a larger length prefix
/// is a framing error, not an allocation to make.
const MAX_RESPONSE_BYTES: usize = 64 << 20;

/// What every connection of one producer shares.
#[derive(Clone)]
pub(crate) struct Dialer {
    pub(crate) tls: Option<Arc<rustls::ClientConfig>>,
    pub(crate) timeout: Duration,
    pub(crate) client_id: StrBytes,
}

/// The versions one connection negotiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Versions {
    pub(crate) produce: i16,
    pub(crate) metadata: i16,
    pub(crate) init_producer_id: i16,
}

/// A failed request: the error, and whether any byte of it could have reached the broker.
#[derive(Debug)]
pub(crate) struct RequestError {
    pub(crate) sent: bool,
    pub(crate) error: anyhow::Error,
}

impl RequestError {
    fn unsent(error: anyhow::Error) -> Self {
        Self { sent: false, error }
    }

    fn sent(error: anyhow::Error) -> Self {
        Self { sent: true, error }
    }

    /// `Clean` when nothing left, `Ambiguous` once anything may have.
    pub(crate) fn fault(&self) -> Fault {
        if self.sent {
            Fault::Ambiguous
        } else {
            Fault::Clean
        }
    }
}

/// One negotiated broker connection.
pub(crate) struct Conn {
    stream: Box<dyn AsyncStream>,
    tls: bool,
    versions: Versions,
    correlation: i32,
    client_id: StrBytes,
    timeout: Duration,
}

impl Dialer {
    /// Connects to `host:port`, with TLS whose server name is `host`, and negotiates versions.
    /// Every failure is `Clean`, since no produce byte goes out before this returns, except a
    /// broker whose versions `kafka_out` can't use, which is `Refused`.
    pub(crate) async fn connect(&self, host: &str, port: u16) -> anyhow::Result<Conn> {
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
            versions: Versions {
                produce: *PRODUCE_VERSIONS.start(),
                metadata: *METADATA_VERSIONS.start(),
                init_producer_id: *INIT_PRODUCER_ID_VERSIONS.start(),
            },
            correlation: 0,
            client_id: self.client_id.clone(),
            timeout: self.timeout,
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
        conn.versions = negotiate(&response)
            .with_context(|| format!("Kafka broker {host}:{port}"))
            .context(Fault::Refused)?;
        Ok(conn)
    }
}

/// The versions to use with a broker that answered `response`.
pub(crate) fn negotiate(response: &ApiVersionsResponse) -> anyhow::Result<Versions> {
    anyhow::ensure!(
        response.error_code == fault::code::NONE,
        "ApiVersions answered {}",
        fault::name(response.error_code)
    );
    let range = |key: ApiKey| {
        response
            .api_keys
            .iter()
            .find(|v| v.api_key == key as i16)
            .map(|v| v.min_version..=v.max_version)
            .ok_or_else(|| anyhow!("the broker doesn't offer {key:?}"))
    };
    let pick = |key: ApiKey, ours: RangeInclusive<i16>, ignore_min: bool| {
        let theirs = range(key)?;
        let hi = (*theirs.end()).min(*ours.end());
        let lo = if ignore_min { *ours.start() } else { (*theirs.start()).max(*ours.start()) };
        anyhow::ensure!(
            hi >= lo,
            "the broker offers {key:?} v{} to v{}; kafka_out sends v{} to v{}",
            theirs.start(),
            theirs.end(),
            ours.start(),
            ours.end()
        );
        Ok(hi)
    };
    Ok(Versions {
        produce: pick(ApiKey::Produce, PRODUCE_VERSIONS, true)?,
        metadata: pick(ApiKey::Metadata, METADATA_VERSIONS, false)?,
        init_producer_id: pick(ApiKey::InitProducerId, INIT_PRODUCER_ID_VERSIONS, false)?,
    })
}

impl Conn {
    /// A connection over `stream` with `versions` already negotiated, for tests that script
    /// the stream.
    #[cfg(test)]
    pub(crate) fn over(stream: Box<dyn AsyncStream>, tls: bool, timeout: Duration) -> Self {
        let versions = Versions { produce: 12, metadata: 12, init_producer_id: 5 };
        Self {
            stream,
            tls,
            versions,
            correlation: 0,
            client_id: StrBytes::from_static_str("logit"),
            timeout,
        }
    }

    pub(crate) fn versions(&self) -> Versions {
        self.versions
    }

    /// Whether a reused connection is still worth writing to: one poll, no wait. A broker closes
    /// an idle connection (`connections.max.idle.ms`), and writing a request into one already
    /// closed would turn a `Clean` redial into an `Ambiguous` failure.
    pub(crate) async fn is_open(&mut self) -> bool {
        matches!(poll_pending_close(&mut *self.stream, &mut [0u8; 1]).await, PendingClose::Open)
    }

    /// Sends `request` at `version` and waits for its response.
    pub(crate) async fn request<R: Request>(
        &mut self,
        request: &R,
        version: i16,
    ) -> Result<R::Response, RequestError> {
        let correlation = self.write(request, version).await?;
        self.read::<R>(version, correlation).await
    }

    /// Sends `request` at `version` without waiting: a `Produce` with `acks: none`, which the
    /// broker never answers.
    pub(crate) async fn send_only<R: Request>(
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

    /// Reads the response to the request numbered `correlation`, under the timeout.
    async fn read<R: Request>(
        &mut self,
        version: i16,
        correlation: i32,
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
        let mut body = timeout(self.timeout, body)
            .await
            .map_err(|_elapsed| {
                anyhow!("waiting for a {key:?} response timed out after {:?}", self.timeout)
            })
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

    use crate::test_support::{FakeStream, ReadStep, WriteStep};

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
        let versions = negotiate(&offered(&[
            (ApiKey::Produce, 0, 13),
            (ApiKey::Metadata, 0, 13),
            (ApiKey::ApiVersions, 0, 4),
            (ApiKey::InitProducerId, 0, 5),
        ]))
        .unwrap();
        assert_eq!(versions, Versions { produce: 12, metadata: 12, init_producer_id: 5 });
    }

    #[test]
    fn an_older_broker_negotiates_its_own_maximum() {
        let versions = negotiate(&offered(&[
            (ApiKey::Produce, 0, 7),
            (ApiKey::Metadata, 0, 9),
            (ApiKey::InitProducerId, 0, 2),
        ]))
        .unwrap();
        assert_eq!(versions, Versions { produce: 7, metadata: 9, init_producer_id: 2 });
    }

    #[test]
    fn a_broker_without_produce_v3_is_refused() {
        let err = negotiate(&offered(&[
            (ApiKey::Produce, 0, 2),
            (ApiKey::Metadata, 0, 9),
            (ApiKey::InitProducerId, 0, 2),
        ]))
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

    #[test]
    fn an_api_versions_error_fails_negotiation() {
        let response = offered(&[]).with_error_code(fault::code::UNSUPPORTED_VERSION);
        assert!(negotiate(&response).is_err());
    }
}
