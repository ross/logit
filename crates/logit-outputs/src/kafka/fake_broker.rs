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
//! A test scripts departures from that with [`Step`]s, each consumed by the first request it
//! matches: an error code, `record_errors`, an append whose answer is lost, a close, or no
//! answer at all.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

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
    ProducerId, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{decode_request_header_from_buffer, Decodable, Encodable, StrBytes};
use kafka_protocol::records::{Compression, Record, RecordBatchDecoder};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::TlsAcceptor;

use super::fault::code;
use super::producer::next_sequence;

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
    /// Close without appending or answering.
    Close,
    /// Read the request and never answer; the connection stays open until the client drops it.
    Hang,
}

/// One scripted departure: the first request of `api` (for `Produce`, naming `partition` when
/// set) gets `action`.
#[derive(Debug, Clone)]
pub(crate) struct Step {
    pub(crate) api: ApiKey,
    pub(crate) partition: Option<i32>,
    pub(crate) action: Action,
}

impl Step {
    pub(crate) fn produce(partition: Option<i32>, action: Action) -> Self {
        Self { api: ApiKey::Produce, partition, action }
    }
}

/// A record batch the broker received, appended or not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Received {
    pub(crate) node: i32,
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

/// The cluster's shared state; tests read and change it through [`FakeCluster::state`].
#[derive(Debug, Default)]
pub(crate) struct State {
    pub(crate) topics: BTreeMap<String, Vec<Partition>>,
    /// Partitions for a topic `Metadata` asks to create, `None` when auto-creation is off.
    pub(crate) auto_create: Option<usize>,
    /// The highest `Produce` version every node advertises; the minimum is always 0, as a 4.x
    /// broker advertises it.
    pub(crate) produce_max: i16,
    pub(crate) script: Vec<Step>,
    pub(crate) received: Vec<Received>,
    /// `(node, api, version)` of every request, in arrival order.
    pub(crate) requests: Vec<(i32, ApiKey, i16)>,
    pub(crate) accepted_connections: usize,
    /// Connections being served now.
    pub(crate) open_connections: usize,
    next_producer_id: i64,
    advertised_host: String,
    ports: Vec<u16>,
}

impl State {
    fn take_step(&mut self, api: ApiKey, partition: Option<i32>) -> Option<Action> {
        let at = self.script.iter().position(|s| {
            s.api == api
                && (s.partition.is_none() || partition.is_none() || s.partition == partition)
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
    loop {
        let Ok(len) = stream.read_i32().await else { return };
        let mut body = vec![0; len as usize];
        if stream.read_exact(&mut body).await.is_err() {
            return;
        }
        let reply = handle(node, Bytes::from(body), &state);
        match reply {
            Reply::Answer(frame) => {
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

fn handle(node: i32, mut body: Bytes, state: &Arc<Mutex<State>>) -> Reply {
    let header = decode_request_header_from_buffer(&mut body).expect("a valid request header");
    let key = ApiKey::try_from(header.request_api_key).expect("a known API key");
    let version = header.request_api_version;
    let mut state = state.lock().unwrap();
    state.requests.push((node, key, version));
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
            let response = ApiVersionsResponse::default().with_api_keys(vec![
                offered(ApiKey::Produce, state.produce_max),
                offered(ApiKey::Metadata, 13),
                offered(ApiKey::ApiVersions, 4),
                offered(ApiKey::InitProducerId, 5),
            ]);
            frame(&|out| response.encode(out, version).unwrap())
        }
        ApiKey::Metadata => {
            let request = MetadataRequest::decode(&mut body, version).unwrap();
            let scripted = state.take_step(key, None);
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
                _ => frame(&|out| response.encode(out, version).unwrap()),
            }
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
    let refusal = state.script.iter().position(|s| {
        s.api == ApiKey::Produce
            && matches!(s.action, Action::Close | Action::Hang)
            && s.partition.is_none_or(|p| asked.contains(&p))
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
            let step = state.take_step(ApiKey::Produce, Some(index));
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
                        (Ok(offset), Some(Action::AppendThenClose)) => {
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
