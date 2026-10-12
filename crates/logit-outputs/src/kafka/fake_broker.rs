//! An in-process Kafka cluster for the producer's tests: one TCP listener per broker node over
//! shared state, answering `ApiVersions`, `Metadata`, `InitProducerId`, and `Produce` through
//! `kafka-protocol`'s `broker` feature, optionally over TLS.
//!
//! It keeps the broker's rules a producer depends on, so a test exercises real semantics:
//!
//! - only a partition's leader appends to it; any other node answers `NOT_LEADER_OR_FOLLOWER`;
//! - a request carries one record batch per partition, decompressed with the producer's own
//!   codecs (`super::compression`);
//! - an idempotent batch is checked as `ProducerAppendInfo.checkSequence` checks it: an older
//!   epoch is `INVALID_PRODUCER_EPOCH`; a known producer at a new epoch must start at
//!   sequence 0; a producer the partition has no state for is accepted at any sequence; the
//!   same epoch must continue at the last appended sequence + 1, wrapping from
//!   `i32::MAX` to 0; anything else is `OUT_OF_ORDER_SEQUENCE_NUMBER`. A batch matching one of
//!   the producer's last five appended batches is answered `NONE` with its original offset and
//!   not appended again.
//!
//! With [`State::sasl`] set, a connection must authenticate as a broker's SASL listener
//! requires: `SaslHandshake` v1 naming a configured mechanism, then `SaslAuthenticate` with
//! `PLAIN`, the server side of SCRAM (RFC 5802), `OAUTHBEARER` (RFC 7628, refusing a token with
//! Kafka's error challenge), or `AWS_MSK_IAM` (the payload's fields and its SigV4 signature,
//! recomputed with `super::sasl`'s signer), checked against the configured credentials.
//! Any request but `ApiVersions` before that closes the connection, as does one after the
//! session's `session_lifetime_ms` has passed (KIP-368's expiry).
//!
//! A test scripts departures from that with [`Step`]s, each consumed by the first request it
//! matches: an error code, `record_errors`, an append whose answer is lost or held until the
//! SASL session ends, a close, or no answer at all.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{
    BatchIndexAndErrorMessage, PartitionProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, InitProducerIdRequest,
    InitProducerIdResponse, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse,
    ProducerId, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse, TopicName,
};
use kafka_protocol::protocol::{decode_request_header_from_buffer, Decodable, Encodable, StrBytes};
use kafka_protocol::records::{Compression, Record, RecordBatchDecoder};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;

use super::fault::code;
use super::producer::next_sequence;
use super::sasl::{escape_username, AmzDate, ScramHash, Signer};

/// How many appended batches per producer per partition the broker remembers for dedup
/// (`ProducerStateEntry.NUM_BATCHES_TO_RETAIN`).
const DEDUP_WINDOW: usize = 5;

/// What a scripted request gets instead of the broker's own answer.
#[derive(Debug, Clone)]
pub(crate) enum Action {
    /// Answer this error code without appending: a partition's code for `Produce`, the topic's
    /// for `Metadata`, the top-level code for `InitProducerId`.
    Error(i16),
    /// Append as usual, then answer this code (`REQUEST_TIMED_OUT` after the leader appended).
    AppendThenError(i16),
    /// Answer this code naming these record indexes in `record_errors`, without appending.
    RecordErrors(i16, Vec<i32>),
    /// Append every partition as usual, then close without answering: the answer is lost.
    AppendThenClose,
    /// Append as usual, then hold the answer until the connection's SASL session ends.
    AnswerAtSessionEnd,
    /// Close without appending or answering.
    Close,
    /// Read the request and never answer; the connection stays open until the client drops it.
    Hang,
}

/// One scripted departure: the first request of `api` (for `Produce`, naming `partition` when
/// set; for `Produce` and `Metadata`, naming `topic` when set) gets `action`.
#[derive(Debug, Clone)]
pub(crate) struct Step {
    pub(crate) api: ApiKey,
    pub(crate) partition: Option<i32>,
    pub(crate) topic: Option<String>,
    pub(crate) action: Action,
}

impl Step {
    pub(crate) fn produce(partition: Option<i32>, action: Action) -> Self {
        Self { api: ApiKey::Produce, partition, topic: None, action }
    }

    /// `action` for the first `Metadata` request asking about `topic`.
    pub(crate) fn metadata(topic: &str, action: Action) -> Self {
        Self { api: ApiKey::Metadata, partition: None, topic: Some(topic.to_string()), action }
    }

    /// The same step, matching only requests for `topic`.
    pub(crate) fn on_topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }
}

/// A record batch the broker received, appended or not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Received {
    pub(crate) node: i32,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) version: i16,
    pub(crate) acks: i16,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) base_sequence: i32,
    pub(crate) records: usize,
    pub(crate) compression: Compression,
    /// The code the broker answered, `NONE` for an append or a deduplicated resend.
    pub(crate) answered: i16,
}

/// A batch appended to a partition's log.
#[derive(Debug, Clone)]
pub(crate) struct Appended {
    pub(crate) records: Vec<Record>,
}

#[derive(Debug, Default)]
struct ProducerEntry {
    epoch: i16,
    last_sequence: i32,
    /// `(first sequence, last sequence, base offset)` of the latest appended batches.
    recent: VecDeque<(i32, i32, i64)>,
}

#[derive(Debug, Default)]
pub(crate) struct Partition {
    pub(crate) leader: i32,
    pub(crate) log: Vec<Appended>,
    next_offset: i64,
    producers: HashMap<i64, ProducerEntry>,
}

impl Partition {
    /// Every appended record, in offset order.
    pub(crate) fn records(&self) -> Vec<&Record> {
        self.log.iter().flat_map(|b| &b.records).collect()
    }
}

/// The SASL a broker requires before any request but `ApiVersions`.
#[derive(Debug, Clone)]
pub(crate) struct Sasl {
    /// The mechanisms `SaslHandshake` accepts.
    pub(crate) mechanisms: Vec<String>,
    pub(crate) username: String,
    pub(crate) password: String,
    /// What a successful `SaslAuthenticate` (v1 and later) answers as `session_lifetime_ms`; 0
    /// for no limit.
    pub(crate) session_lifetime_ms: i64,
    /// The iteration count a SCRAM server-first message asks for.
    pub(crate) iterations: u32,
    /// Answers a valid SCRAM proof with a server signature from another password, as a broker
    /// that doesn't hold the user's credential would.
    pub(crate) forge_signature: bool,
    /// The one `OAUTHBEARER` token accepted; `None` accepts any.
    pub(crate) oauth_token: Option<String>,
    /// Refuses an `OAUTHBEARER` token as Redpanda v26.2.3 does: `SASL_AUTHENTICATION_FAILED` on
    /// the first message, with no error challenge.
    pub(crate) oauth_no_challenge: bool,
    /// The one `AWS_MSK_IAM` session token accepted; `None` accepts a payload with any or none.
    /// `AWS_MSK_IAM` takes `username` as the access key id and `password` as the secret key.
    pub(crate) msk_session_token: Option<String>,
}

impl Sasl {
    /// Every mechanism `kafka_out` speaks, for `username` and `password`, with no lifetime.
    pub(crate) fn new(username: &str, password: &str) -> Self {
        Self {
            mechanisms: ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "AWS_MSK_IAM"]
                .map(String::from)
                .to_vec(),
            username: username.to_string(),
            password: password.to_string(),
            session_lifetime_ms: 0,
            iterations: 4096,
            forge_signature: false,
            oauth_token: None,
            oauth_no_challenge: false,
            msk_session_token: None,
        }
    }
}

/// Where one connection is in its SASL exchange.
#[derive(Default)]
enum Auth {
    #[default]
    None,
    Handshaken(String),
    /// A SCRAM server-first message is sent.
    ScramFirst {
        hash: ScramHash,
        nonce: String,
        auth_prefix: String,
    },
    /// An `OAUTHBEARER` error challenge is sent.
    OAuthRefused,
    Done {
        expires: Option<Instant>,
    },
}

/// `OAuthBearerSaslServer`'s error challenge for a token its validator refuses, as Kafka 4.2.0
/// sends it (`script/kafka-interop`'s `oauth-refused` leg).
const OAUTH_CHALLENGE: &str = r#"{"status":"invalid_token"}"#;

/// Redpanda v26.2.3's code-58 message for a refused credential (`script/kafka-interop`'s
/// `oauth-refused` and `sasl-wrong` legs).
const REDPANDA_REFUSAL: &str = "SASL authentication failed: security: Invalid credentials";

/// MSK's answer to an accepted `AWS_MSK_IAM` payload.
const MSK_ACCEPTED: &str = r#"{"version":"2020_10_22","request-id":"fake-request-id"}"#;

/// The salt every SCRAM credential here uses.
const SCRAM_SALT: &[u8] = b"fake-broker-salt";

/// The cluster's shared state; tests read and change it through [`FakeCluster::state`].
#[derive(Debug, Default)]
pub(crate) struct State {
    pub(crate) topics: BTreeMap<String, Vec<Partition>>,
    /// Partitions for a topic `Metadata` asks to create, `None` when auto-creation is off.
    pub(crate) auto_create: Option<usize>,
    /// The highest `Produce` version every node advertises; the minimum is always 0, as a 4.x
    /// broker advertises it.
    pub(crate) produce_max: i16,
    /// Required before any request but `ApiVersions` when set.
    pub(crate) sasl: Option<Sasl>,
    /// Leaves `SaslHandshake` and `SaslAuthenticate` out of `ApiVersions`.
    pub(crate) hide_sasl: bool,
    /// Every successful SASL exchange.
    pub(crate) authentications: usize,
    /// Every `OAUTHBEARER` client message, in arrival order.
    pub(crate) oauth_messages: Vec<Vec<u8>>,
    /// Every `AWS_MSK_IAM` payload that parsed as a JSON object, in arrival order.
    pub(crate) msk_payloads: Vec<serde_json::Map<String, serde_json::Value>>,
    /// When each session that has a lifetime ends, in order.
    pub(crate) session_ends: Vec<Instant>,
    /// Connections closed for a request that arrived after their session ended.
    pub(crate) expired_closes: usize,
    pub(crate) script: Vec<Step>,
    pub(crate) received: Vec<Received>,
    /// `(node, api, version)` of every request, in arrival order.
    pub(crate) requests: Vec<(i32, ApiKey, i16)>,
    /// Every topic a `Metadata` request asked about, in arrival order.
    pub(crate) metadata_topics: Vec<String>,
    pub(crate) accepted_connections: usize,
    /// Connections being served now.
    pub(crate) open_connections: usize,
    next_producer_id: i64,
    advertised_host: String,
    ports: Vec<u16>,
}

impl State {
    fn take_step(&mut self, api: ApiKey, partition: Option<i32>) -> Option<Action> {
        self.take_step_for(api, partition, None)
    }

    /// [`State::take_step`] for a request about `topic`; a step naming another topic doesn't
    /// match it.
    fn take_step_for(
        &mut self,
        api: ApiKey,
        partition: Option<i32>,
        topic: Option<&str>,
    ) -> Option<Action> {
        let at = self.script.iter().position(|s| {
            s.api == api
                && (s.partition.is_none() || partition.is_none() || s.partition == partition)
                && (s.topic.is_none() || topic.is_none() || s.topic.as_deref() == topic)
        })?;
        Some(self.script.remove(at).action)
    }

    pub(crate) fn partition(&self, topic: &str, partition: i32) -> &Partition {
        &self.topics[topic][partition as usize]
    }

    pub(crate) fn add_topic(&mut self, name: &str, leaders: &[i32]) {
        let partitions =
            leaders.iter().map(|&leader| Partition { leader, ..Partition::default() }).collect();
        self.topics.insert(name.to_string(), partitions);
    }
}

/// A running fake cluster. Dropping it stops every node and closes every connection.
pub(crate) struct FakeCluster {
    state: Arc<Mutex<State>>,
    addrs: Vec<SocketAddr>,
    tls: Option<TlsAcceptor>,
    tasks: Vec<Option<JoinHandle<()>>>,
}

impl Drop for FakeCluster {
    fn drop(&mut self) {
        for task in self.tasks.iter().flatten() {
            task.abort();
        }
    }
}

impl FakeCluster {
    /// Starts `nodes` brokers, ids 0 up, on loopback, advertising `127.0.0.1`.
    pub(crate) async fn start(nodes: usize) -> Self {
        Self::start_with(nodes, None, "127.0.0.1").await
    }

    /// Starts `nodes` brokers serving TLS from `tls`, advertising `host`.
    pub(crate) async fn start_with(
        nodes: usize,
        tls: Option<Arc<rustls::ServerConfig>>,
        host: &str,
    ) -> Self {
        let state = Arc::new(Mutex::new(State {
            produce_max: 13,
            next_producer_id: 1000,
            advertised_host: host.to_string(),
            ..State::default()
        }));
        let tls = tls.map(TlsAcceptor::from);
        let mut addrs = Vec::new();
        let mut tasks = Vec::new();
        for node in 0..nodes as i32 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            state.lock().unwrap().ports.push(addr.port());
            addrs.push(addr);
            tasks.push(Some(tokio::spawn(accept_loop(node, listener, tls.clone(), state.clone()))));
        }
        Self { state, addrs, tls, tasks }
    }

    pub(crate) fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Node `node`'s `host:port` on loopback.
    pub(crate) fn addr(&self, node: usize) -> String {
        self.addrs[node].to_string()
    }

    pub(crate) fn port(&self, node: usize) -> u16 {
        self.addrs[node].port()
    }

    /// Stops node `node` and waits until its listener is closed, so a dial to it is refused.
    /// Its connections close as their tasks are cancelled; [`State::open_connections`] counts
    /// them down.
    pub(crate) async fn stop(&mut self, node: usize) {
        if let Some(task) = self.tasks[node].take() {
            task.abort();
            let _ = task.await;
        }
    }

    /// Stops node `node` and starts it again on the same port, as a broker restart does.
    pub(crate) async fn restart(&mut self, node: usize) {
        self.stop(node).await;
        let listener = TcpListener::bind(self.addrs[node]).await.unwrap();
        let task =
            tokio::spawn(accept_loop(node as i32, listener, self.tls.clone(), self.state.clone()));
        self.tasks[node] = Some(task);
    }

    pub(crate) fn script(&self, step: Step) {
        self.state().script.push(step);
    }
}

async fn accept_loop(
    node: i32,
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    state: Arc<Mutex<State>>,
) {
    // Owned here so aborting this task closes every connection it accepted.
    let mut conns = JoinSet::new();
    loop {
        let Ok((tcp, _)) = listener.accept().await else { continue };
        state.lock().unwrap().accepted_connections += 1;
        let state = state.clone();
        match &tls {
            None => {
                conns.spawn(serve(node, tcp, state));
            }
            Some(acceptor) => {
                let acceptor = acceptor.clone();
                conns.spawn(async move {
                    if let Ok(stream) = acceptor.accept(tcp).await {
                        serve(node, stream, state).await;
                    }
                });
            }
        }
    }
}

/// What a request's handling asks of the connection after it.
enum Reply {
    Answer(Bytes),
    AnswerAt(Bytes, Instant),
    Silent,
    Close,
    Hang,
}

/// Counts a served connection down when its task ends or is cancelled.
struct Open(Arc<Mutex<State>>);

impl Drop for Open {
    fn drop(&mut self) {
        // A test that panicked while holding the lock poisoned it; the count no longer matters.
        if let Ok(mut state) = self.0.lock() {
            state.open_connections -= 1;
        }
    }
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    node: i32,
    mut stream: S,
    state: Arc<Mutex<State>>,
) {
    state.lock().unwrap().open_connections += 1;
    let _open = Open(state.clone());
    let mut auth = Auth::None;
    loop {
        let Ok(len) = stream.read_i32().await else { return };
        let mut body = vec![0; len as usize];
        if stream.read_exact(&mut body).await.is_err() {
            return;
        }
        let reply = handle(node, Bytes::from(body), &state, &mut auth);
        match reply {
            Reply::Answer(frame) => {
                if stream.write_all(&frame).await.is_err() {
                    return;
                }
            }
            Reply::AnswerAt(frame, at) => {
                tokio::time::sleep_until(at).await;
                if stream.write_all(&frame).await.is_err() {
                    return;
                }
            }
            Reply::Silent => {}
            Reply::Close => return,
            Reply::Hang => {
                // Drain until the client gives up and closes.
                let mut sink = [0u8; 1024];
                while matches!(stream.read(&mut sink).await, Ok(n) if n > 0) {}
                return;
            }
        }
    }
}

fn handle(node: i32, mut body: Bytes, state: &Arc<Mutex<State>>, auth: &mut Auth) -> Reply {
    let header = decode_request_header_from_buffer(&mut body).expect("a valid request header");
    let key = ApiKey::try_from(header.request_api_key).expect("a known API key");
    let version = header.request_api_version;
    let mut state = state.lock().unwrap();
    state.requests.push((node, key, version));
    let exchange =
        matches!(key, ApiKey::ApiVersions | ApiKey::SaslHandshake | ApiKey::SaslAuthenticate);
    if state.sasl.is_some() && !exchange {
        match auth {
            Auth::Done { expires } if expires.is_none_or(|end| Instant::now() < end) => {}
            Auth::Done { .. } => {
                state.expired_closes += 1;
                return Reply::Close;
            }
            _ => return Reply::Close,
        }
    }
    let frame = |body: &dyn Fn(&mut BytesMut)| {
        let mut out = BytesMut::new();
        out.put_i32(0);
        ResponseHeader::default()
            .with_correlation_id(header.correlation_id)
            .encode(&mut out, key.response_header_version(version))
            .unwrap();
        body(&mut out);
        let len = (out.len() - 4) as i32;
        out[..4].copy_from_slice(&len.to_be_bytes());
        Reply::Answer(out.freeze())
    };
    match key {
        ApiKey::ApiVersions => {
            ApiVersionsRequest::decode(&mut body, version).unwrap();
            let offered = |key: ApiKey, max: i16| {
                ApiVersion::default()
                    .with_api_key(key as i16)
                    .with_min_version(0)
                    .with_max_version(max)
            };
            let mut keys = vec![
                offered(ApiKey::Produce, state.produce_max),
                offered(ApiKey::Metadata, 13),
                offered(ApiKey::ApiVersions, 4),
                offered(ApiKey::InitProducerId, 5),
            ];
            if !state.hide_sasl {
                keys.push(offered(ApiKey::SaslHandshake, 1));
                keys.push(offered(ApiKey::SaslAuthenticate, 2));
            }
            let response = ApiVersionsResponse::default().with_api_keys(keys);
            frame(&|out| response.encode(out, version).unwrap())
        }
        ApiKey::SaslHandshake => {
            let request = SaslHandshakeRequest::decode(&mut body, version).unwrap();
            let offered: Vec<StrBytes> = state
                .sasl
                .iter()
                .flat_map(|s| s.mechanisms.iter().cloned().map(StrBytes::from_string))
                .collect();
            let mechanism = request.mechanism.to_string();
            let error = match state.take_step(key, None) {
                Some(Action::Error(code)) => code,
                Some(Action::Close) => return Reply::Close,
                Some(Action::Hang) => return Reply::Hang,
                // A listener without SASL answers a handshake this way.
                _ if state.sasl.is_none() || !matches!(auth, Auth::None) => {
                    code::ILLEGAL_SASL_STATE
                }
                _ if !offered.iter().any(|m| m.as_str() == mechanism) => {
                    code::UNSUPPORTED_SASL_MECHANISM
                }
                _ => {
                    *auth = Auth::Handshaken(mechanism);
                    code::NONE
                }
            };
            let response =
                SaslHandshakeResponse::default().with_error_code(error).with_mechanisms(offered);
            frame(&|out| response.encode(out, version).unwrap())
        }
        ApiKey::SaslAuthenticate => {
            let request = SaslAuthenticateRequest::decode(&mut body, version).unwrap();
            let response = match state.take_step(key, None) {
                Some(Action::Error(code)) => SaslAuthenticateResponse::default()
                    .with_error_code(code)
                    .with_error_message(Some(StrBytes::from_static_str("scripted refusal"))),
                Some(Action::Close) => return Reply::Close,
                Some(Action::Hang) => return Reply::Hang,
                _ => sasl_authenticate(&mut state, auth, &request.auth_bytes),
            };
            frame(&|out| response.encode(out, version).unwrap())
        }
        ApiKey::Metadata => {
            let request = MetadataRequest::decode(&mut body, version).unwrap();
            let asked: Vec<String> = request
                .topics
                .iter()
                .flatten()
                .filter_map(|t| t.name.as_ref().map(|n| n.0.to_string()))
                .collect();
            let scripted = state.take_step_for(key, None, asked.first().map(String::as_str));
            state.metadata_topics.extend(asked);
            match scripted {
                Some(Action::Close) => return Reply::Close,
                Some(Action::Hang) => return Reply::Hang,
                _ => {}
            }
            let response = metadata(&mut state, &request, scripted);
            frame(&|out| response.encode(out, version).unwrap())
        }
        ApiKey::InitProducerId => {
            InitProducerIdRequest::decode(&mut body, version).unwrap();
            let response = match state.take_step(key, None) {
                Some(Action::Error(code)) => InitProducerIdResponse::default()
                    .with_error_code(code)
                    .with_producer_id(ProducerId(-1))
                    .with_producer_epoch(-1),
                Some(Action::Close) => return Reply::Close,
                Some(Action::Hang) => return Reply::Hang,
                _ => {
                    let id = state.next_producer_id;
                    state.next_producer_id += 1;
                    InitProducerIdResponse::default()
                        .with_producer_id(ProducerId(id))
                        .with_producer_epoch(0)
                }
            };
            frame(&|out| response.encode(out, version).unwrap())
        }
        ApiKey::Produce => {
            let request = ProduceRequest::decode(&mut body, version).unwrap();
            let (response, connection) = produce(node, &mut state, &request, version);
            match connection {
                Some(Action::Close | Action::AppendThenClose) => Reply::Close,
                Some(Action::Hang) => Reply::Hang,
                _ if request.acks == 0 => Reply::Silent,
                Some(Action::AnswerAtSessionEnd) => {
                    let Auth::Done { expires: Some(end) } = auth else {
                        panic!("AnswerAtSessionEnd needs a SASL session with a lifetime")
                    };
                    match frame(&|out| response.encode(out, version).unwrap()) {
                        Reply::Answer(bytes) => Reply::AnswerAt(bytes, *end),
                        other => other,
                    }
                }
                _ => frame(&|out| response.encode(out, version).unwrap()),
            }
        }
        other => panic!("the fake broker doesn't serve {other:?}"),
    }
}

/// The token of an `OAUTHBEARER` client message in the form Kafka's
/// `OAuthBearerClientInitialResponse` parses: `n,` an optional `a=` authzid `,`, then `\x01`-led
/// `key=value` pairs, one of them `auth=Bearer <token>`, and a closing `\x01\x01`.
fn oauth_token(message: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(message).ok()?;
    let rest = text.strip_prefix("n,")?;
    let (_authzid, rest) = rest.split_once(',')?;
    let pairs = rest.strip_prefix('\x01')?.strip_suffix("\x01\x01")?;
    pairs.split('\x01').find_map(|pair| pair.strip_prefix("auth=Bearer ")).map(String::from)
}

/// Checks an `AWS_MSK_IAM` payload's fields as `aws-msk-iam-auth`'s `AWS4SignedPayloadGenerator`
/// writes them, and its signature, recomputed for `sasl`'s access key id and secret key. The
/// reason a payload fails names no secret.
fn msk_iam_check(
    sasl: &Sasl,
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let field = |name: &str| payload.get(name).and_then(|v| v.as_str());
    let mut keys: Vec<&str> = payload.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let token = field("x-amz-security-token");
    let mut want = vec![
        "action",
        "host",
        "user-agent",
        "version",
        "x-amz-algorithm",
        "x-amz-credential",
        "x-amz-date",
        "x-amz-expires",
        "x-amz-signature",
        "x-amz-signedheaders",
    ];
    if token.is_some() {
        want.push("x-amz-security-token");
    }
    want.sort_unstable();
    if keys != want {
        return Err(format!("keys {keys:?}"));
    }
    let fixed = [
        ("version", "2020_10_22"),
        ("action", "kafka-cluster:Connect"),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-signedheaders", "host"),
        ("x-amz-expires", "900"),
    ];
    for (name, value) in fixed {
        if field(name) != Some(value) {
            return Err(format!("{name} {:?}", field(name)));
        }
    }
    if sasl.msk_session_token.is_some() && token != sasl.msk_session_token.as_deref() {
        return Err("a session token the broker didn't issue".to_string());
    }
    let credential: Vec<&str> = field("x-amz-credential").unwrap_or_default().split('/').collect();
    let [access_key_id, date, region, "kafka-cluster", "aws4_request"] = credential[..] else {
        return Err(format!("credential scope {credential:?}"));
    };
    if access_key_id != sasl.username {
        return Err("an unknown access key id".to_string());
    }
    let datetime = field("x-amz-date").unwrap_or_default();
    if !datetime.starts_with(date) || datetime.len() != 16 {
        return Err(format!("x-amz-date {datetime:?} outside scope date {date:?}"));
    }
    let signer = Signer {
        access_key_id,
        secret_access_key: &sasl.password,
        region,
        service: "kafka-cluster",
    };
    let date = AmzDate { datetime: datetime.to_string() };
    let params = signer.presign(
        field("host").unwrap_or_default(),
        &[("Action", "kafka-cluster:Connect")],
        token,
        &date,
        900,
    );
    if params.last().map(|(_, signature)| signature.as_str()) != field("x-amz-signature") {
        return Err("a signature that doesn't match".to_string());
    }
    Ok(())
}

/// One `SaslAuthenticate` step of the exchange `auth` is in.
fn sasl_authenticate(
    state: &mut State,
    auth: &mut Auth,
    message: &[u8],
) -> SaslAuthenticateResponse {
    let fail = |code: i16, why: &str| {
        SaslAuthenticateResponse::default()
            .with_error_code(code)
            .with_error_message(Some(StrBytes::from_string(why.to_string())))
    };
    let Some(sasl) = state.sasl.clone() else {
        return fail(code::ILLEGAL_SASL_STATE, "no SASL on this listener");
    };
    let done = |state: &mut State, auth: &mut Auth, server: Vec<u8>| {
        let lifetime = u64::try_from(sasl.session_lifetime_ms).unwrap_or(0);
        let expires =
            (lifetime > 0).then(|| Instant::now() + std::time::Duration::from_millis(lifetime));
        state.authentications += 1;
        state.session_ends.extend(expires);
        *auth = Auth::Done { expires };
        SaslAuthenticateResponse::default()
            .with_auth_bytes(Bytes::from(server))
            .with_session_lifetime_ms(sasl.session_lifetime_ms)
    };
    // Kafka's own refusal text for SCRAM.
    let scram_failed = |hash: ScramHash| {
        fail(
            code::SASL_AUTHENTICATION_FAILED,
            &format!(
                "Authentication failed during authentication due to invalid credentials with \
                 SASL mechanism {}",
                hash.mechanism()
            ),
        )
    };
    match std::mem::take(auth) {
        Auth::Handshaken(mechanism) if mechanism == "OAUTHBEARER" => {
            state.oauth_messages.push(message.to_vec());
            let token = oauth_token(message);
            let accepted = token.is_some_and(|token| {
                sasl.oauth_token.as_ref().is_none_or(|expected| *expected == token)
            });
            if accepted {
                return done(state, auth, Vec::new());
            }
            if sasl.oauth_no_challenge {
                return fail(code::SASL_AUTHENTICATION_FAILED, REDPANDA_REFUSAL);
            }
            *auth = Auth::OAuthRefused;
            SaslAuthenticateResponse::default().with_auth_bytes(Bytes::from(OAUTH_CHALLENGE))
        }
        Auth::OAuthRefused if message != [0x01] => {
            fail(code::ILLEGAL_SASL_STATE, "expected a kvsep after the error challenge")
        }
        // Kafka 4.2.0 fails the exchange with the challenge as its message.
        Auth::OAuthRefused => fail(code::SASL_AUTHENTICATION_FAILED, OAUTH_CHALLENGE),
        Auth::Handshaken(mechanism) if mechanism == "AWS_MSK_IAM" => {
            let parsed = serde_json::from_slice::<serde_json::Value>(message).ok();
            let Some(serde_json::Value::Object(payload)) = parsed else {
                return fail(code::SASL_AUTHENTICATION_FAILED, "Invalid authentication payload");
            };
            state.msk_payloads.push(payload.clone());
            match msk_iam_check(&sasl, &payload) {
                Ok(()) => done(state, auth, MSK_ACCEPTED.as_bytes().to_vec()),
                Err(why) => fail(
                    code::SASL_AUTHENTICATION_FAILED,
                    &format!("[fake-request-id]: Access denied ({why})"),
                ),
            }
        }
        Auth::Handshaken(mechanism) if mechanism == "PLAIN" => {
            let parts: Vec<&[u8]> = message.split(|&b| b == 0).collect();
            if parts.len() == 3
                && parts[1] == sasl.username.as_bytes()
                && parts[2] == sasl.password.as_bytes()
            {
                done(state, auth, Vec::new())
            } else {
                fail(
                    code::SASL_AUTHENTICATION_FAILED,
                    "Authentication failed: Invalid username or password",
                )
            }
        }
        Auth::Handshaken(mechanism) => {
            let hash = match mechanism.as_str() {
                "SCRAM-SHA-256" => ScramHash::Sha256,
                "SCRAM-SHA-512" => ScramHash::Sha512,
                _ => return fail(code::ILLEGAL_SASL_STATE, "an unknown mechanism"),
            };
            let first = String::from_utf8_lossy(message).to_string();
            let Some(bare) = first.strip_prefix("n,,") else {
                return scram_failed(hash);
            };
            let (user, client_nonce) = match bare.split_once(",r=") {
                Some((user, nonce)) => (user.strip_prefix("n=").unwrap_or_default(), nonce),
                None => return scram_failed(hash),
            };
            if user != escape_username(&sasl.username) {
                return scram_failed(hash);
            }
            let nonce = format!("{client_nonce}fake-server-nonce");
            let server_first =
                format!("r={nonce},s={},i={}", BASE64.encode(SCRAM_SALT), sasl.iterations);
            *auth = Auth::ScramFirst { hash, nonce, auth_prefix: format!("{bare},{server_first}") };
            SaslAuthenticateResponse::default().with_auth_bytes(Bytes::from(server_first))
        }
        Auth::ScramFirst { hash, nonce, auth_prefix } => {
            let last = String::from_utf8_lossy(message).to_string();
            let Some((without_proof, proof)) = last.rsplit_once(",p=") else {
                return scram_failed(hash);
            };
            if without_proof != format!("c=biws,r={nonce}") {
                return scram_failed(hash);
            }
            let Ok(proof) = BASE64.decode(proof) else {
                return scram_failed(hash);
            };
            let auth_message = format!("{auth_prefix},{without_proof}");
            let salted = hash.salted_password(&sasl.password, SCRAM_SALT, sasl.iterations);
            let stored_key = hash.hash(&hash.hmac(&salted, b"Client Key"));
            let signature = hash.hmac(&stored_key, auth_message.as_bytes());
            let client_key: Vec<u8> = proof.iter().zip(&signature).map(|(p, s)| p ^ s).collect();
            if proof.len() != signature.len() || hash.hash(&client_key) != stored_key {
                return scram_failed(hash);
            }
            let signing = if sasl.forge_signature {
                hash.salted_password(
                    &format!("{}-forged", sasl.password),
                    SCRAM_SALT,
                    sasl.iterations,
                )
            } else {
                salted
            };
            let server_key = hash.hmac(&signing, b"Server Key");
            let verifier = BASE64.encode(hash.hmac(&server_key, auth_message.as_bytes()));
            done(state, auth, format!("v={verifier}").into_bytes())
        }
        Auth::None | Auth::Done { .. } => {
            fail(code::ILLEGAL_SASL_STATE, "SaslAuthenticate outside an exchange")
        }
    }
}

fn metadata(
    state: &mut State,
    request: &MetadataRequest,
    scripted: Option<Action>,
) -> MetadataResponse {
    let brokers = state
        .ports
        .iter()
        .enumerate()
        .map(|(id, &port)| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(id as i32))
                .with_host(StrBytes::from_string(state.advertised_host.clone()))
                .with_port(i32::from(port))
        })
        .collect();
    let mut topics = Vec::new();
    for asked in request.topics.iter().flatten() {
        let name = asked.name.as_ref().expect("a topic by name").0.to_string();
        // A Kafka 4.x broker creates the topic and answers the request that asked for it
        // `UNKNOWN_TOPIC_OR_PARTITION`; the next request finds it.
        let mut created = false;
        if !state.topics.contains_key(&name) && request.allow_auto_topic_creation {
            if let Some(n) = state.auto_create {
                state.add_topic(&name, &vec![0; n]);
                created = true;
            }
        }
        let topic = MetadataResponseTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(name.clone()))));
        let topic = match (&scripted, state.topics.get(&name)) {
            (Some(Action::Error(code)), _) => topic.with_error_code(*code),
            (_, None) => topic.with_error_code(code::UNKNOWN_TOPIC_OR_PARTITION),
            (_, Some(_)) if created => topic.with_error_code(code::UNKNOWN_TOPIC_OR_PARTITION),
            (_, Some(partitions)) => topic.with_partitions(
                partitions
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        let part = MetadataResponsePartition::default()
                            .with_partition_index(i as i32)
                            .with_leader_id(BrokerId(p.leader));
                        if p.leader < 0 {
                            part.with_error_code(code::LEADER_NOT_AVAILABLE)
                        } else {
                            part.with_replica_nodes(vec![BrokerId(p.leader)])
                                .with_isr_nodes(vec![BrokerId(p.leader)])
                        }
                    })
                    .collect(),
            ),
        };
        topics.push(topic);
    }
    MetadataResponse::default()
        .with_brokers(brokers)
        .with_cluster_id(Some(StrBytes::from_static_str("fake")))
        .with_controller_id(BrokerId(0))
        .with_topics(topics)
}

/// Handles one produce request; returns its response and any connection-level action a step
/// scripted for it.
fn produce(
    node: i32,
    state: &mut State,
    request: &ProduceRequest,
    version: i16,
) -> (ProduceResponse, Option<Action>) {
    let asked: Vec<i32> =
        request.topic_data.iter().flat_map(|t| t.partition_data.iter().map(|d| d.index)).collect();
    let topics: Vec<String> = request.topic_data.iter().map(|t| t.name.0.to_string()).collect();
    let refusal = state.script.iter().position(|s| {
        s.api == ApiKey::Produce
            && matches!(s.action, Action::Close | Action::Hang)
            && s.partition.is_none_or(|p| asked.contains(&p))
            && s.topic.as_ref().is_none_or(|t| topics.contains(t))
    });
    if let Some(at) = refusal {
        return (ProduceResponse::default(), Some(state.script.remove(at).action));
    }
    let mut connection = None;
    let mut responses = Vec::new();
    for topic in &request.topic_data {
        let name = topic.name.0.to_string();
        let mut partitions = Vec::new();
        for data in &topic.partition_data {
            let index = data.index;
            let step = state.take_step_for(ApiKey::Produce, Some(index), Some(&name));
            let mut answer = PartitionProduceResponse::default().with_index(index);
            let records = data.records.clone().expect("a produce request carries records");
            let mut info_buf = records.clone();
            let infos = RecordBatchDecoder::decode_batch_info(&mut info_buf).unwrap();
            assert_eq!(infos.len(), 1, "a producer sends one batch per partition per request");
            let info = &infos[0];
            let sets = super::batch::decode(records).expect("the batch decodes");
            let decoded: Vec<Record> = sets.into_iter().flat_map(|s| s.records).collect();

            let (code, base_offset, record_errors) = match &step {
                Some(Action::Error(code)) => (*code, -1, vec![]),
                Some(Action::RecordErrors(code, named)) => (*code, -1, named.clone()),
                _ => {
                    let appended = match state.topics.get_mut(&name) {
                        None => Err(code::UNKNOWN_TOPIC_OR_PARTITION),
                        Some(parts) => match parts.get_mut(index as usize) {
                            None => Err(code::UNKNOWN_TOPIC_OR_PARTITION),
                            Some(p) if p.leader != node => Err(code::NOT_LEADER_OR_FOLLOWER),
                            Some(p) => append(p, info, decoded),
                        },
                    };
                    match (appended, &step) {
                        (Ok(offset), Some(Action::AppendThenError(code))) => {
                            (*code, offset, vec![])
                        }
                        (
                            Ok(offset),
                            Some(Action::AppendThenClose | Action::AnswerAtSessionEnd),
                        ) => {
                            connection = step.clone();
                            (code::NONE, offset, vec![])
                        }
                        (Ok(offset), _) => (code::NONE, offset, vec![]),
                        (Err(code), _) => (code, -1, vec![]),
                    }
                }
            };
            state.received.push(Received {
                node,
                topic: name.clone(),
                partition: index,
                version,
                acks: request.acks,
                producer_id: info.producer_id,
                producer_epoch: info.producer_epoch,
                base_sequence: info.base_sequence,
                records: info.record_count as usize,
                compression: info.compression,
                answered: code,
            });
            answer = answer.with_error_code(code).with_base_offset(base_offset);
            if version >= 8 && !record_errors.is_empty() {
                answer = answer.with_record_errors(
                    record_errors
                        .into_iter()
                        .map(|i| BatchIndexAndErrorMessage::default().with_batch_index(i))
                        .collect(),
                );
            }
            partitions.push(answer);
        }
        responses.push(
            TopicProduceResponse::default()
                .with_name(TopicName(StrBytes::from_string(name)))
                .with_partition_responses(partitions),
        );
    }
    (ProduceResponse::default().with_responses(responses), connection)
}

/// Appends one batch under the broker's sequence rules; returns its base offset, or the error
/// code that refuses it.
fn append(
    p: &mut Partition,
    info: &kafka_protocol::records::BatchDecodeInfo,
    records: Vec<Record>,
) -> Result<i64, i16> {
    let count = records.len() as i32;
    let pid = info.producer_id;
    let first = info.base_sequence;
    let last = if pid >= 0 { next_sequence(first, count - 1) } else { first };
    if pid >= 0 {
        match p.producers.get(&pid) {
            Some(entry) if info.producer_epoch < entry.epoch => {
                return Err(code::INVALID_PRODUCER_EPOCH)
            }
            Some(entry) if info.producer_epoch == entry.epoch => {
                if let Some(&(_, _, offset)) =
                    entry.recent.iter().find(|&&(f, l, _)| f == first && l == last)
                {
                    return Ok(offset);
                }
                if first != next_sequence(entry.last_sequence, 1) {
                    return Err(code::OUT_OF_ORDER_SEQUENCE_NUMBER);
                }
            }
            Some(_) if first != 0 => return Err(code::OUT_OF_ORDER_SEQUENCE_NUMBER),
            _ => {}
        }
    }
    let base_offset = p.next_offset;
    p.next_offset += i64::from(count);
    p.log.push(Appended { records });
    if pid >= 0 {
        let entry = p.producers.entry(pid).or_default();
        if entry.epoch != info.producer_epoch {
            entry.recent.clear();
        }
        entry.epoch = info.producer_epoch;
        entry.last_sequence = last;
        entry.recent.push_back((first, last, base_offset));
        if entry.recent.len() > DEDUP_WINDOW {
            entry.recent.pop_front();
        }
    }
    Ok(base_offset)
}

/// One request a [`TokenEndpoint`] received.
#[derive(Debug, Clone)]
pub(crate) struct TokenRequest {
    pub(crate) authorization: String,
    pub(crate) content_type: String,
    pub(crate) body: String,
}

/// What a [`TokenEndpoint`] answers its `n`th request (from 0): a status and a body.
pub(crate) type TokenAnswer = Box<dyn Fn(usize) -> (u16, String) + Send>;

/// The shared state of a [`TokenEndpoint`].
pub(crate) struct TokenState {
    pub(crate) requests: Vec<TokenRequest>,
    pub(crate) answer: TokenAnswer,
}

/// An OAuth 2.0 token endpoint on `127.0.0.1` over HTTP/1.1, answering every `POST` with
/// [`TokenState::answer`]. By default it issues `token-<n>` with an hour's `expires_in`.
pub(crate) struct TokenEndpoint {
    pub(crate) url: String,
    state: Arc<Mutex<TokenState>>,
    task: JoinHandle<()>,
}

impl Drop for TokenEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TokenEndpoint {
    pub(crate) async fn start() -> Self {
        use http_body_util::{BodyExt, Full};
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/oauth2/token", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(TokenState {
            requests: Vec::new(),
            answer: Box::new(|n| {
                (
                    200,
                    format!(
                        r#"{{"access_token":"token-{n}","token_type":"Bearer","expires_in":3600}}"#
                    ),
                )
            }),
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let state = state.clone();
                        async move {
                            let header = |name: http::header::HeaderName| {
                                req.headers()
                                    .get(name)
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or_default()
                                    .to_string()
                            };
                            let request = TokenRequest {
                                authorization: header(http::header::AUTHORIZATION),
                                content_type: header(http::header::CONTENT_TYPE),
                                body: String::new(),
                            };
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let (status, text) = {
                                let mut state = state.lock().unwrap();
                                let n = state.requests.len();
                                state.requests.push(TokenRequest {
                                    body: String::from_utf8_lossy(&body).into_owned(),
                                    ..request
                                });
                                (state.answer)(n)
                            };
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .status(status)
                                    .header(http::header::CONTENT_TYPE, "application/json")
                                    .body(Full::new(Bytes::from(text)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self { url, state, task }
    }

    pub(crate) fn state(&self) -> MutexGuard<'_, TokenState> {
        self.state.lock().unwrap()
    }

    /// How many token requests have arrived.
    pub(crate) fn fetches(&self) -> usize {
        self.state().requests.len()
    }
}

/// One request an [`HttpStub`] received.
#[derive(Debug, Clone)]
pub(crate) struct HttpRequest {
    pub(crate) method: String,
    /// The path and query.
    pub(crate) path: String,
    /// Header names in lowercase.
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: String,
}

/// What an [`HttpStub`] answers a request, given it and how many came before it: a status and
/// a body.
pub(crate) type HttpAnswer = Box<dyn Fn(&HttpRequest, usize) -> (u16, String) + Send>;

/// The shared state of an [`HttpStub`].
pub(crate) struct HttpStubState {
    pub(crate) requests: Vec<HttpRequest>,
    pub(crate) answer: HttpAnswer,
}

/// An HTTP/1.1 server on `127.0.0.1` that records every request and answers it with
/// [`HttpStubState::answer`]: a stand-in for STS, a container credential endpoint, or IMDS. It
/// answers `404` until a test sets an answer.
pub(crate) struct HttpStub {
    /// `http://127.0.0.1:<port>`, no trailing `/`.
    pub(crate) url: String,
    state: Arc<Mutex<HttpStubState>>,
    task: JoinHandle<()>,
}

impl Drop for HttpStub {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl HttpStub {
    pub(crate) async fn start() -> Self {
        use http_body_util::{BodyExt, Full};
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(HttpStubState {
            requests: Vec::new(),
            answer: Box::new(|_, _| (404, String::new())),
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let state = state.clone();
                        async move {
                            let method = req.method().to_string();
                            let path = req
                                .uri()
                                .path_and_query()
                                .map(|p| p.as_str().to_string())
                                .unwrap_or_default();
                            let headers = req
                                .headers()
                                .iter()
                                .map(|(name, value)| {
                                    (
                                        name.as_str().to_string(),
                                        value.to_str().unwrap_or_default().to_string(),
                                    )
                                })
                                .collect();
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let request = HttpRequest {
                                method,
                                path,
                                headers,
                                body: String::from_utf8_lossy(&body).into_owned(),
                            };
                            let (status, text) = {
                                let mut state = state.lock().unwrap();
                                let n = state.requests.len();
                                let answer = (state.answer)(&request, n);
                                state.requests.push(request);
                                answer
                            };
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::from(text)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self { url, state, task }
    }

    pub(crate) fn state(&self) -> MutexGuard<'_, HttpStubState> {
        self.state.lock().unwrap()
    }

    /// Sets what every request is answered with.
    pub(crate) fn answer(
        &self,
        answer: impl Fn(&HttpRequest, usize) -> (u16, String) + Send + 'static,
    ) {
        self.state().answer = Box::new(answer);
    }

    /// Every request received so far.
    pub(crate) fn requests(&self) -> Vec<HttpRequest> {
        self.state().requests.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(producer_epoch: i16, base_sequence: i32) -> kafka_protocol::records::BatchDecodeInfo {
        use super::super::batch::{encode, ProducerRecord, Stamp};
        let record =
            ProducerRecord { key: None, value: None, headers: Default::default(), timestamp: 0 };
        let stamp = Stamp { producer_id: 7, producer_epoch, base_sequence };
        let mut bytes = encode(&[record], stamp, Compression::None).unwrap();
        RecordBatchDecoder::decode_batch_info(&mut bytes).unwrap().remove(0)
    }

    #[test]
    fn unknown_producer_is_accepted_at_any_sequence() {
        let mut p = Partition::default();
        assert_eq!(append(&mut p, &info(0, 5), vec![]), Ok(0));
    }

    #[test]
    fn known_producer_at_a_new_epoch_must_start_at_zero() {
        let mut p = Partition::default();
        append(&mut p, &info(0, 0), vec![]).unwrap();
        assert_eq!(append(&mut p, &info(1, 5), vec![]), Err(code::OUT_OF_ORDER_SEQUENCE_NUMBER));
        assert_eq!(append(&mut p, &info(1, 0), vec![]), Ok(0));
    }
}
