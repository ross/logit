//! An in-process Kafka cluster for the Kafka clients' tests: one TCP listener per broker node over
//! shared state, optionally over TLS, decoding and encoding every request through
//! `kafka-protocol`'s `broker` feature at whatever version the client sends.
//!
//! It keeps the broker rules a client depends on, so a test exercises real semantics:
//!
//! - **Produce** (this module): only a partition's leader appends to it, and any other node
//!   answers `NOT_LEADER_OR_FOLLOWER`. A request carries one record batch per partition,
//!   decompressed with the producer's own codecs (`logit_proto::kafka::compression`). An
//!   idempotent batch is checked as `ProducerAppendInfo.checkSequence` checks it: an older epoch
//!   is `INVALID_PRODUCER_EPOCH`; a known producer at a new epoch must start at sequence 0; a
//!   producer the partition has no state for is accepted at any sequence; the same epoch must
//!   continue at the last appended sequence + 1, wrapping from `i32::MAX` to 0; anything else is
//!   `OUT_OF_ORDER_SEQUENCE_NUMBER`. A batch matching one of the producer's last five appended
//!   batches is answered `NONE` with its original offset and not appended again.
//! - **The log** (`log`): a partition stores each batch as the producer wrote it, with its
//!   base offset rewritten as a broker assigns it, plus transactions, a log start a test raises
//!   to model retention, and blobs it never decodes. Fetch and ListOffsets read it with Kafka's
//!   offset, isolation, size, and wait rules.
//! - **The group coordinator** (`group`): Kafka's classic `GroupCoordinator`, eager, with
//!   generations, held JoinGroup and SyncGroup answers, session expiry on the tokio clock, and
//!   committed offsets per group.
//! - **SASL** (`sasl`): with [`State::sasl`] set, a connection must authenticate as a broker's
//!   SASL listener requires before any request but `ApiVersions`, and a request after the
//!   session's `session_lifetime_ms` closes the connection (KIP-368's expiry).
//!
//! A connection answers its requests in order, one at a time, as a broker mutes a connection
//! until its answer goes out, so a held JoinGroup or Fetch holds every request behind it.
//!
//! A test scripts departures with [`Step`]s, each consumed by the first request it matches.
//! [`Action`] lists what each one does per API.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{
    BatchIndexAndErrorMessage, PartitionProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest,
    FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse,
    InitProducerIdRequest, InitProducerIdResponse, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse, ProducerId,
    ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse, SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{decode_request_header_from_buffer, Decodable, Encodable, StrBytes};
use kafka_protocol::records::{Compression, Record, RecordBatchDecoder};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;

use crate::error::code;
use logit_proto::kafka::next_sequence;

mod group;
mod log;
mod sasl;
mod token;

#[cfg(test)]
mod tests;

pub use group::{CommitSeen, Group, GroupState, JoinSeen, Member};
pub use log::FetchSeen;
pub use logit_aws::test_stub::HttpStub;
pub use sasl::Sasl;
pub use token::{TokenAnswer, TokenEndpoint, TokenRequest, TokenState};

use sasl::{sasl_authenticate, Auth};

/// How many appended batches per producer per partition the broker remembers for dedup
/// (`ProducerStateEntry.NUM_BATCHES_TO_RETAIN`).
const DEDUP_WINDOW: usize = 5;

/// The bound on one batch's decompressed records section: far above any batch a test sends.
const MAX_RECORDS_BYTES: usize = 64 << 20;

/// What Kafka 2.4.0 advertises in `ApiVersions` for the APIs the Kafka clients send, from its
/// `clients/src/main/resources/common/message/*Request.json`, for [`State::api_versions`].
pub const KAFKA_2_4: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 0, 8),
    (ApiKey::Fetch, 0, 11),
    (ApiKey::ListOffsets, 0, 5),
    (ApiKey::Metadata, 0, 9),
    (ApiKey::OffsetCommit, 0, 8),
    (ApiKey::OffsetFetch, 0, 6),
    (ApiKey::FindCoordinator, 0, 3),
    (ApiKey::JoinGroup, 0, 6),
    (ApiKey::Heartbeat, 0, 4),
    (ApiKey::LeaveGroup, 0, 4),
    (ApiKey::SyncGroup, 0, 4),
    (ApiKey::SaslHandshake, 0, 1),
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::InitProducerId, 0, 2),
    (ApiKey::SaslAuthenticate, 0, 1),
];

/// The consumer APIs' default `ApiVersions` rows: Kafka 4.0's ranges, except `Fetch`, which stops
/// at v12 because v13 names topics by id and the fake assigns none.
const CONSUMER_RANGES: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 10),
    (ApiKey::OffsetCommit, 2, 9),
    (ApiKey::OffsetFetch, 1, 9),
    (ApiKey::FindCoordinator, 0, 6),
    (ApiKey::JoinGroup, 2, 9),
    (ApiKey::Heartbeat, 0, 4),
    (ApiKey::LeaveGroup, 0, 5),
    (ApiKey::SyncGroup, 0, 5),
];

/// What a scripted request gets instead of the broker's own answer. A variant an API has no use
/// for is answered as if no step matched.
#[derive(Debug, Clone)]
pub enum Action {
    /// Answer this error code without acting on the request:
    ///
    /// - `Produce`: the partition's code. `Metadata`: the topic's. `InitProducerId`,
    ///   `ApiVersions`, `SaslHandshake`, `SaslAuthenticate`: the top-level code.
    /// - `FindCoordinator`: the coordinator's code (from v4, in each key's entry).
    /// - `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`: the top-level code, the group
    ///   untouched.
    /// - `OffsetCommit`: the step's partition's code, or every partition's without one; nothing
    ///   is stored for a partition answered non-zero.
    /// - `OffsetFetch`: the step's partition's code; without one, the group's code (top-level
    ///   to v7, the group entry's from v8, each partition's at v1, which has neither).
    /// - `ListOffsets`, `Fetch`: the step's partition's code, or every partition's without one;
    ///   a `Fetch` partition answered with a code carries no records.
    Error(i16),
    /// Append as usual, then answer this code (`REQUEST_TIMED_OUT` after the leader appended).
    AppendThenError(i16),
    /// Answer this code naming these record indexes in `record_errors`, without appending.
    RecordErrors(i16, Vec<i32>),
    /// Append every partition as usual, then close without answering: the answer is lost.
    AppendThenClose,
    /// Append as usual, then hold the answer until the connection's SASL session ends.
    AnswerAtSessionEnd,
    /// Answer as usual with `throttle_time_ms` set to this, on every response that carries it.
    Throttle(i32),
    /// `Fetch` only: answer as usual with one bit of the CRC field flipped in the first batch
    /// served for the step's partition. The step stays scripted until a fetch serves a batch for
    /// that partition.
    CorruptCrc,
    /// Close without acting or answering.
    Close,
    /// Read the request and never answer; the connection stays open until the client drops it.
    Hang,
}

/// One scripted departure: the first request of `api` that names `topic` and `partition`, each
/// when set, gets `action`. A request that names no topic or partition matches a step that
/// names one.
#[derive(Debug, Clone)]
pub struct Step {
    pub api: ApiKey,
    pub partition: Option<i32>,
    pub topic: Option<String>,
    pub action: Action,
}

impl Step {
    /// `action` for any request of `api`.
    pub fn api(api: ApiKey, action: Action) -> Self {
        Self { api, partition: None, topic: None, action }
    }

    pub fn produce(partition: Option<i32>, action: Action) -> Self {
        Self { api: ApiKey::Produce, partition, topic: None, action }
    }

    /// `action` for the first `Metadata` request asking about `topic`.
    pub fn metadata(topic: &str, action: Action) -> Self {
        Self { api: ApiKey::Metadata, partition: None, topic: Some(topic.to_string()), action }
    }

    /// The same step, matching only requests for `topic`.
    pub fn on_topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }

    /// The same step, matching only requests for `partition`.
    pub fn on_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Whether a request naming `asked` matches; an empty `asked` matches any step.
    fn matches(&self, asked: &[(&str, i32)]) -> bool {
        asked.is_empty() || asked.iter().any(|&(topic, partition)| self.covers(topic, partition))
    }

    fn covers(&self, topic: &str, partition: i32) -> bool {
        self.topic.as_deref().is_none_or(|t| t == topic)
            && self.partition.is_none_or(|p| p == partition)
    }
}

/// A record batch the broker received through `Produce`, appended or not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    pub node: i32,
    pub topic: String,
    pub partition: i32,
    pub version: i16,
    pub acks: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: usize,
    pub compression: Compression,
    /// The code the broker answered, `NONE` for an append or a deduplicated resend.
    pub answered: i16,
}

/// A batch stored in a partition's log.
#[derive(Debug, Clone)]
pub struct Appended {
    /// The batch's records with their assigned offsets; empty for a blob from
    /// [`State::append_raw`], which is never decoded.
    pub records: Vec<Record>,
    /// The batch as stored and served: the bytes it arrived as, with bytes 0..8 rewritten to
    /// [`Appended::base_offset`]. The CRC doesn't cover them.
    pub raw: Bytes,
    pub base_offset: i64,
    pub last_offset: i64,
    /// Attributes bit 4.
    pub transactional: bool,
    /// Attributes bit 5: a control batch, such as a transaction marker.
    pub control: bool,
    /// -1 for a producer without idempotence.
    pub producer_id: i64,
}

#[derive(Debug, Default)]
struct ProducerEntry {
    epoch: i16,
    last_sequence: i32,
    /// `(first sequence, last sequence, base offset)` of the latest appended batches.
    recent: VecDeque<(i32, i32, i64)>,
}

#[derive(Debug, Default)]
pub struct Partition {
    pub leader: i32,
    pub log: Vec<Appended>,
    /// The first offset a fetch may read; [`State::truncate`] raises it.
    pub log_start: i64,
    /// `(producer id, first offset, abort marker's offset)` of every aborted transaction.
    pub aborted: Vec<(i64, i64, i64)>,
    next_offset: i64,
    producers: HashMap<i64, ProducerEntry>,
    /// Each producer's open transaction's first offset.
    open_transactions: BTreeMap<i64, i64>,
}

impl Partition {
    /// Every appended record, in offset order.
    pub fn records(&self) -> Vec<&Record> {
        self.log.iter().flat_map(|b| &b.records).collect()
    }

    /// The offset the next append gets.
    pub fn high_watermark(&self) -> i64 {
        self.next_offset
    }

    /// The first offset of the earliest open transaction, else the high watermark.
    pub fn last_stable_offset(&self) -> i64 {
        self.open_transactions.values().copied().min().unwrap_or(self.next_offset)
    }
}

/// The cluster's shared state; tests read and change it through [`FakeCluster::state`].
#[derive(Debug, Default)]
pub struct State {
    pub topics: BTreeMap<String, Vec<Partition>>,
    /// Partitions for a topic `Metadata` asks to create, `None` when auto-creation is off.
    pub auto_create: Option<usize>,
    /// The highest `Produce` version every node advertises; the minimum is always 0, as a 4.x
    /// broker advertises it.
    pub produce_max: i16,
    /// The `(key, min, max)` rows `ApiVersions` answers, in place of the defaults: `Produce` to
    /// [`State::produce_max`], `Metadata`, `ApiVersions`, `InitProducerId`, the SASL pair unless
    /// [`State::hide_sasl`], and Kafka 4.0's consumer ranges.
    pub api_versions: Option<Vec<(ApiKey, i16, i16)>>,
    /// Required before any request but `ApiVersions` when set.
    pub sasl: Option<Sasl>,
    /// Leaves `SaslHandshake` and `SaslAuthenticate` out of the default `ApiVersions` rows.
    pub hide_sasl: bool,
    /// Every successful SASL exchange.
    pub authentications: usize,
    /// Every `OAUTHBEARER` client message, in arrival order.
    pub oauth_messages: Vec<Vec<u8>>,
    /// Every `AWS_MSK_IAM` payload that parsed as a JSON object, in arrival order.
    pub msk_payloads: Vec<serde_json::Map<String, serde_json::Value>>,
    /// When each session that has a lifetime ends, in order.
    pub session_ends: Vec<Instant>,
    /// Connections closed for a request that arrived after their session ended.
    pub expired_closes: usize,
    pub script: Vec<Step>,
    pub received: Vec<Received>,
    /// `(node, api, version)` of every request, in arrival order.
    pub requests: Vec<(i32, ApiKey, i16)>,
    /// Every topic a `Metadata` request asked about, in arrival order.
    pub metadata_topics: Vec<String>,
    /// Every `Metadata` request's `allow_auto_topic_creation`, in arrival order.
    pub metadata_auto_create: Vec<bool>,
    pub accepted_connections: usize,
    /// Connections being served now.
    pub open_connections: usize,
    /// The node `FindCoordinator` names for every group; any other node answers a group request
    /// `NOT_COORDINATOR`.
    pub coordinator: i32,
    /// The session timeouts a JoinGroup may ask for (`group.min.session.timeout.ms` and
    /// `group.max.session.timeout.ms`); outside them it's `INVALID_SESSION_TIMEOUT`.
    pub group_session_bounds: (Duration, Duration),
    pub groups: BTreeMap<String, Group>,
    /// Every `OffsetCommit`, in arrival order.
    pub commits: Vec<CommitSeen>,
    /// Every `JoinGroup`, in arrival order.
    pub joins: Vec<JoinSeen>,
    /// Every `Fetch`, in arrival order.
    pub fetches: Vec<FetchSeen>,
    /// `(node, version, topic, partition, timestamp, isolation_level)` of every partition a
    /// `ListOffsets` asked about, in arrival order.
    pub list_offsets: Vec<(i32, i16, String, i32, i64, i8)>,
    next_producer_id: i64,
    advertised_host: String,
    ports: Vec<u16>,
    /// Woken by every append, for a fetch waiting on data.
    appended: Arc<Notify>,
    /// Woken by every group request, for the coordinator task to recompute its next deadline.
    group_wake: Arc<Notify>,
    next_member: u64,
    next_join: u64,
}

/// The step one request consumed.
#[derive(Debug, Clone)]
struct Taken {
    action: Action,
    topic: Option<String>,
    partition: Option<i32>,
}

impl Taken {
    fn throttle(taken: &Option<Taken>) -> i32 {
        match taken {
            Some(Taken { action: Action::Throttle(ms), .. }) => *ms,
            _ => 0,
        }
    }

    /// The scripted code for `topic`'s `partition`, if the step is an error covering it.
    fn error_for(taken: &Option<Taken>, topic: &str, partition: i32) -> Option<i16> {
        match taken {
            Some(t @ Taken { action: Action::Error(code), .. })
                if t.topic.as_deref().is_none_or(|n| n == topic)
                    && t.partition.is_none_or(|p| p == partition) =>
            {
                Some(*code)
            }
            _ => None,
        }
    }

    /// The scripted code when the step is an error that names no partition.
    fn whole_error(taken: &Option<Taken>) -> Option<i16> {
        match taken {
            Some(Taken { action: Action::Error(code), partition: None, .. }) => Some(*code),
            _ => None,
        }
    }

    /// The reply for a `Close` or `Hang` step.
    fn connection(taken: &Option<Taken>) -> Option<Reply> {
        match taken.as_ref().map(|t| &t.action) {
            Some(Action::Close) => Some(Reply::Close),
            Some(Action::Hang) => Some(Reply::Hang),
            _ => None,
        }
    }
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

    /// The first step for `api` matching a request that names the partitions in `asked`,
    /// leaving `CorruptCrc` steps for `log` to take when a batch is served.
    fn take_step_in(&mut self, api: ApiKey, asked: &[(&str, i32)]) -> Option<Taken> {
        let at = self.script.iter().position(|s| {
            s.api == api && !matches!(s.action, Action::CorruptCrc) && s.matches(asked)
        })?;
        let step = self.script.remove(at);
        Some(Taken { action: step.action, topic: step.topic, partition: step.partition })
    }

    pub fn partition(&self, topic: &str, partition: i32) -> &Partition {
        &self.topics[topic][partition as usize]
    }

    pub fn add_topic(&mut self, name: &str, leaders: &[i32]) {
        let partitions =
            leaders.iter().map(|&leader| Partition { leader, ..Partition::default() }).collect();
        self.topics.insert(name.to_string(), partitions);
    }

    fn partition_mut(&mut self, topic: &str, partition: i32) -> &mut Partition {
        self.topics
            .get_mut(topic)
            .and_then(|parts| parts.get_mut(partition as usize))
            .unwrap_or_else(|| panic!("no partition {topic}/{partition}"))
    }

    fn api_versions_rows(&self) -> Vec<(ApiKey, i16, i16)> {
        if let Some(rows) = &self.api_versions {
            return rows.clone();
        }
        let mut rows = vec![
            (ApiKey::Produce, 0, self.produce_max),
            (ApiKey::Metadata, 0, 13),
            (ApiKey::ApiVersions, 0, 4),
            (ApiKey::InitProducerId, 0, 5),
        ];
        if !self.hide_sasl {
            rows.push((ApiKey::SaslHandshake, 0, 1));
            rows.push((ApiKey::SaslAuthenticate, 0, 2));
        }
        rows.extend_from_slice(CONSUMER_RANGES);
        rows
    }
}

/// A running fake cluster. Dropping it stops every node and closes every connection.
pub struct FakeCluster {
    state: Arc<Mutex<State>>,
    addrs: Vec<SocketAddr>,
    tls: Option<TlsAcceptor>,
    tasks: Vec<Option<JoinHandle<()>>>,
    /// Expires group sessions and rebalances on the tokio clock (`group::coordinator_task`).
    coordinator: JoinHandle<()>,
}

impl Drop for FakeCluster {
    fn drop(&mut self) {
        for task in self.tasks.iter().flatten() {
            task.abort();
        }
        self.coordinator.abort();
    }
}

impl FakeCluster {
    /// Starts `nodes` brokers, ids 0 up, on loopback, advertising `127.0.0.1`.
    pub async fn start(nodes: usize) -> Self {
        Self::start_with(nodes, None, "127.0.0.1").await
    }

    /// Starts `nodes` brokers serving TLS from `tls`, advertising `host`.
    pub async fn start_with(
        nodes: usize,
        tls: Option<Arc<rustls::ServerConfig>>,
        host: &str,
    ) -> Self {
        let state = Arc::new(Mutex::new(State {
            produce_max: 13,
            next_producer_id: 1000,
            advertised_host: host.to_string(),
            group_session_bounds: (Duration::from_secs(6), Duration::from_secs(30 * 60)),
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
        let coordinator = tokio::spawn(group::coordinator_task(state.clone()));
        Self { state, addrs, tls, tasks, coordinator }
    }

    pub fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Node `node`'s `host:port` on loopback.
    pub fn addr(&self, node: usize) -> String {
        self.addrs[node].to_string()
    }

    pub fn port(&self, node: usize) -> u16 {
        self.addrs[node].port()
    }

    /// Stops node `node` and waits until its listener is closed, so a dial to it is refused.
    /// Its connections close as their tasks are cancelled; [`State::open_connections`] counts
    /// them down.
    pub async fn stop(&mut self, node: usize) {
        if let Some(task) = self.tasks[node].take() {
            task.abort();
            let _ = task.await;
        }
    }

    /// Stops node `node` and starts it again on the same port, as a broker restart does.
    pub async fn restart(&mut self, node: usize) {
        self.stop(node).await;
        let listener = TcpListener::bind(self.addrs[node]).await.unwrap();
        let task =
            tokio::spawn(accept_loop(node as i32, listener, self.tls.clone(), self.state.clone()));
        self.tasks[node] = Some(task);
    }

    pub fn script(&self, step: Step) {
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
    /// An answer decided later: a held JoinGroup, SyncGroup, or Fetch. The future never holds
    /// the state's lock across an await.
    Later(Pin<Box<dyn Future<Output = Reply> + Send>>),
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
        let mut reply = handle(node, Bytes::from(body), &state, &mut auth);
        loop {
            match reply {
                Reply::Later(later) => reply = later.await,
                Reply::Answer(frame) => {
                    if stream.write_all(&frame).await.is_err() {
                        return;
                    }
                    break;
                }
                Reply::AnswerAt(frame, at) => {
                    tokio::time::sleep_until(at).await;
                    if stream.write_all(&frame).await.is_err() {
                        return;
                    }
                    break;
                }
                Reply::Silent => break,
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
}

/// A response frame: the length prefix, the header, and `response` encoded at `version`.
fn frame<R: Encodable>(correlation: i32, key: ApiKey, version: i16, response: &R) -> Bytes {
    let mut out = BytesMut::new();
    out.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(correlation)
        .encode(&mut out, key.response_header_version(version))
        .unwrap();
    response
        .encode(&mut out, version)
        .unwrap_or_else(|err| panic!("encoding a {key:?} v{version} response: {err:#}"));
    let len = (out.len() - 4) as i32;
    out[..4].copy_from_slice(&len.to_be_bytes());
    out.freeze()
}

fn answer<R: Encodable>(correlation: i32, key: ApiKey, version: i16, response: &R) -> Reply {
    Reply::Answer(frame(correlation, key, version, response))
}

/// An answer that waits on `held`, encoded at `version` once it arrives; a dropped sender closes
/// the connection.
fn later<T: Send + 'static, R: Encodable>(
    correlation: i32,
    key: ApiKey,
    version: i16,
    held: tokio::sync::oneshot::Receiver<T>,
    respond: impl FnOnce(T) -> R + Send + 'static,
) -> Reply {
    Reply::Later(Box::pin(async move {
        match held.await {
            Ok(result) => answer(correlation, key, version, &respond(result)),
            Err(_) => Reply::Close,
        }
    }))
}

fn handle(node: i32, mut body: Bytes, shared: &Arc<Mutex<State>>, auth: &mut Auth) -> Reply {
    let header = decode_request_header_from_buffer(&mut body).expect("a valid request header");
    let key = ApiKey::try_from(header.request_api_key).expect("a known API key");
    let version = header.request_api_version;
    let correlation = header.correlation_id;
    let client_id = header.client_id.as_ref().map(|c| c.to_string()).unwrap_or_default();
    let mut state = shared.lock().unwrap();
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
    match key {
        ApiKey::ApiVersions => {
            ApiVersionsRequest::decode(&mut body, version).unwrap();
            let taken = state.take_step_in(key, &[]);
            if let Some(reply) = Taken::connection(&taken) {
                return reply;
            }
            let keys = state
                .api_versions_rows()
                .into_iter()
                .map(|(key, min, max)| {
                    ApiVersion::default()
                        .with_api_key(key as i16)
                        .with_min_version(min)
                        .with_max_version(max)
                })
                .collect();
            let response = ApiVersionsResponse::default()
                .with_api_keys(keys)
                .with_error_code(Taken::whole_error(&taken).unwrap_or(code::NONE))
                .with_throttle_time_ms(Taken::throttle(&taken));
            answer(correlation, key, version, &response)
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
            answer(correlation, key, version, &response)
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
            answer(correlation, key, version, &response)
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
            state.metadata_auto_create.push(request.allow_auto_topic_creation);
            match scripted {
                Some(Action::Close) => return Reply::Close,
                Some(Action::Hang) => return Reply::Hang,
                _ => {}
            }
            let throttle = match scripted {
                Some(Action::Throttle(ms)) => ms,
                _ => 0,
            };
            let response = metadata(&mut state, &request, scripted).with_throttle_time_ms(throttle);
            answer(correlation, key, version, &response)
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
                step => {
                    let id = state.next_producer_id;
                    state.next_producer_id += 1;
                    let throttle = match step {
                        Some(Action::Throttle(ms)) => ms,
                        _ => 0,
                    };
                    InitProducerIdResponse::default()
                        .with_producer_id(ProducerId(id))
                        .with_producer_epoch(0)
                        .with_throttle_time_ms(throttle)
                }
            };
            answer(correlation, key, version, &response)
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
                    Reply::AnswerAt(frame(correlation, key, version, &response), *end)
                }
                _ => answer(correlation, key, version, &response),
            }
        }
        ApiKey::Fetch => {
            let request = FetchRequest::decode(&mut body, version).unwrap();
            drop(state);
            log::fetch(shared, node, correlation, version, request)
        }
        ApiKey::ListOffsets => {
            let request = ListOffsetsRequest::decode(&mut body, version).unwrap();
            let asked: Vec<(String, i32)> = request
                .topics
                .iter()
                .flat_map(|t| {
                    t.partitions.iter().map(|p| (t.name.0.to_string(), p.partition_index))
                })
                .collect();
            let names: Vec<(&str, i32)> = asked.iter().map(|(t, p)| (t.as_str(), *p)).collect();
            let taken = state.take_step_in(key, &names);
            if let Some(reply) = Taken::connection(&taken) {
                return reply;
            }
            let response: ListOffsetsResponse =
                log::list_offsets(&mut state, node, version, &request, &taken);
            answer(correlation, key, version, &response)
        }
        ApiKey::FindCoordinator => {
            let request = FindCoordinatorRequest::decode(&mut body, version).unwrap();
            let taken = state.take_step_in(key, &[]);
            if let Some(reply) = Taken::connection(&taken) {
                return reply;
            }
            let response: FindCoordinatorResponse =
                group::find_coordinator(&state, version, &request, &taken);
            answer(correlation, key, version, &response)
        }
        ApiKey::JoinGroup => {
            let request = JoinGroupRequest::decode(&mut body, version).unwrap();
            let taken = state.take_step_in(key, &[]);
            if let Some(reply) = Taken::connection(&taken) {
                return reply;
            }
            let throttle = Taken::throttle(&taken);
            let held = state.join_group(node, version, &client_id, &request, &taken);
            state.group_wake.notify_one();
            let respond = move |result: group::JoinResult| -> JoinGroupResponse {
                result.response(version).with_throttle_time_ms(throttle)
            };
            match held {
                group::Held::Now(result) => answer(correlation, key, version, &respond(result)),
                group::Held::Later(rx) => later(correlation, key, version, rx, respond),
            }
        }
        ApiKey::SyncGroup => {
            let request = SyncGroupRequest::decode(&mut body, version).unwrap();
            let taken = state.take_step_in(key, &[]);
            if let Some(reply) = Taken::connection(&taken) {
                return reply;
            }
            let throttle = Taken::throttle(&taken);
            let held = state.sync_group(node, version, &request, &taken);
            state.group_wake.notify_one();
            let respond = move |result: group::SyncResult| -> SyncGroupResponse {
                result.response(version).with_throttle_time_ms(throttle)
            };
            match held {
                group::Held::Now(result) => answer(correlation, key, version, &respond(result)),
                group::Held::Later(rx) => later(correlation, key, version, rx, respond),
            }
        }
        ApiKey::Heartbeat => {
            let request = HeartbeatRequest::decode(&mut body, version).unwrap();
            let taken = state.take_step_in(key, &[]);
            if let Some(reply) = Taken::connection(&taken) {
                return reply;
            }
            let error = state.heartbeat(node, &request, &taken);
            state.group_wake.notify_one();
            let response = HeartbeatResponse::default()
                .with_error_code(error)
                .with_throttle_time_ms(Taken::throttle(&taken));
            answer(correlation, key, version, &response)
        }
        ApiKey::LeaveGroup => {
            let request = LeaveGroupRequest::decode(&mut body, version).unwrap();
            let taken = state.take_step_in(key, &[]);
            if let Some(reply) = Taken::connection(&taken) {
                return reply;
            }
            let response: LeaveGroupResponse = state.leave_group(node, version, &request, &taken);
            state.group_wake.notify_one();
            answer(correlation, key, version, &response)
        }
        ApiKey::OffsetCommit => {
            let request = OffsetCommitRequest::decode(&mut body, version).unwrap();
            let asked: Vec<(String, i32)> = request
                .topics
                .iter()
                .flat_map(|t| {
                    t.partitions.iter().map(|p| (t.name.0.to_string(), p.partition_index))
                })
                .collect();
            let names: Vec<(&str, i32)> = asked.iter().map(|(t, p)| (t.as_str(), *p)).collect();
            let taken = state.take_step_in(key, &names);
            if let Some(reply) = Taken::connection(&taken) {
                return reply;
            }
            let response: OffsetCommitResponse =
                state.offset_commit(node, version, &request, &taken);
            state.group_wake.notify_one();
            answer(correlation, key, version, &response)
        }
        ApiKey::OffsetFetch => {
            let request = OffsetFetchRequest::decode(&mut body, version).unwrap();
            let asked = group::offset_fetch_asked(version, &request);
            let names: Vec<(&str, i32)> = asked.iter().map(|(t, p)| (t.as_str(), *p)).collect();
            let taken = state.take_step_in(key, &names);
            if let Some(reply) = Taken::connection(&taken) {
                return reply;
            }
            let response: OffsetFetchResponse = state.offset_fetch(node, version, &request, &taken);
            answer(correlation, key, version, &response)
        }
        other => panic!("the fake broker doesn't serve {other:?}"),
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
    // A null topic list asks about every topic.
    let names: Vec<String> = match &request.topics {
        Some(topics) => {
            topics.iter().map(|t| t.name.as_ref().expect("a topic by name").0.to_string()).collect()
        }
        None => state.topics.keys().cloned().collect(),
    };
    let mut topics = Vec::new();
    for name in names {
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
    let mut throttle = 0;
    let mut responses = Vec::new();
    for topic in &request.topic_data {
        let name = topic.name.0.to_string();
        let mut partitions = Vec::new();
        for data in &topic.partition_data {
            let index = data.index;
            let step = state.take_step_for(ApiKey::Produce, Some(index), Some(&name));
            if let Some(Action::Throttle(ms)) = step {
                throttle = ms;
            }
            let mut answer = PartitionProduceResponse::default().with_index(index);
            let records = data.records.clone().expect("a produce request carries records");
            let mut info_buf = records.clone();
            let infos = RecordBatchDecoder::decode_batch_info(&mut info_buf).unwrap();
            assert_eq!(infos.len(), 1, "a producer sends one batch per partition per request");
            let info = &infos[0];

            let (code, base_offset, record_errors) = match &step {
                Some(Action::Error(code)) => (*code, -1, vec![]),
                Some(Action::RecordErrors(code, named)) => (*code, -1, named.clone()),
                _ => {
                    let appended = match state.topics.get_mut(&name) {
                        None => Err(code::UNKNOWN_TOPIC_OR_PARTITION),
                        Some(parts) => match parts.get_mut(index as usize) {
                            None => Err(code::UNKNOWN_TOPIC_OR_PARTITION),
                            Some(p) if p.leader != node => Err(code::NOT_LEADER_OR_FOLLOWER),
                            Some(p) => append(p, info, records),
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
    state.appended.notify_waiters();
    (
        ProduceResponse::default().with_responses(responses).with_throttle_time_ms(throttle),
        connection,
    )
}

/// Appends one producer batch under the broker's sequence rules; returns its base offset, or the
/// error code that refuses it.
fn append(
    p: &mut Partition,
    info: &kafka_protocol::records::BatchDecodeInfo,
    batch: Bytes,
) -> Result<i64, i16> {
    let count = info.record_count;
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
    let base_offset = p.push_batch(batch);
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
