//! The consumer's connections: one [`Link`] per role (metadata, the coordinator, heartbeats,
//! each leader's fetch loop), each dialing its broker on demand through the shared [`Pool`].
//!
//! - A link that fails a request drops its connection, since the framing position is lost, and
//!   dials again on the next call; a redial of a slot dialed before counts
//!   `logit.input.reconnects`.
//! - A response's `throttle_time_ms` mutes the link: its next request waits until the throttle
//!   passes, as the Java client mutes the channel (`KafkaChannel.mute`). Each link has its own
//!   connection, so muting one leaves the others' requests to the same broker going.
//! - Every failure counts `logit.input.broker.errors{api, error}` here, with `error` `transport`,
//!   `tls`, `sasl`, or `unsupported_version` for a dial and `transport` for a request.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context};
use kafka_protocol::messages::{
    ApiKey, FetchResponse, FindCoordinatorResponse, HeartbeatResponse, JoinGroupResponse,
    LeaveGroupResponse, ListOffsetsResponse, MetadataResponse, OffsetCommitResponse,
    OffsetFetchResponse, SyncGroupResponse,
};
use kafka_protocol::protocol::{Request, StrBytes};
use tokio::time::Instant;

use super::{ConsumerSettings, Counters};
use crate::conn::{Conn, Dialer, CONSUMER_APIS};

/// Which connection of the consumer's a [`Link`] is, for counting redials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Slot {
    Control,
    Coordinator,
    Heartbeat,
    /// The heartbeat task's FindCoordinator.
    Locate,
    Fetch(i32),
}

/// Where a [`Link`] dials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    /// A bootstrap entry, else any broker the metadata named, in turn.
    Any,
    /// A broker by node id, through the metadata's address for it.
    Node(i32),
    /// The group coordinator, by the address FindCoordinator gave.
    Addr(String, u16),
}

/// Why a call failed before an answer came back.
#[derive(Debug)]
pub(crate) enum CallError {
    /// No connection: connect, TLS, ApiVersions, a missing required version, or SASL.
    Dial(anyhow::Error),
    /// The request failed on an open connection, which is dropped.
    Transport(anyhow::Error),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Dial(err) | CallError::Transport(err) => write!(f, "{err:#}"),
        }
    }
}

/// Splits a bootstrap `host:port`; a bracketed IPv6 literal loses its brackets.
pub(crate) fn host_port(entry: &str) -> anyhow::Result<(String, u16)> {
    let (host, port) =
        entry.rsplit_once(':').ok_or_else(|| anyhow!("Kafka broker {entry:?} has no port"))?;
    let port = port.parse().with_context(|| format!("Kafka broker {entry:?} has no valid port"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    anyhow::ensure!(!host.is_empty(), "Kafka broker {entry:?} has no host");
    Ok((host.to_string(), port))
}

/// What every link of one consumer shares: the dialer, the bootstrap list, the brokers the last
/// metadata named, and which slots have dialed.
#[derive(Clone)]
pub(crate) struct Pool {
    dialer: Dialer,
    bootstrap: Arc<Vec<(String, u16)>>,
    brokers: Arc<Mutex<HashMap<i32, (String, u16)>>>,
    dialed: Arc<Mutex<HashSet<Slot>>>,
    pub(crate) counters: Counters,
}

impl Pool {
    pub(crate) fn new(settings: &ConsumerSettings, counters: Counters) -> anyhow::Result<Self> {
        anyhow::ensure!(!settings.brokers.is_empty(), "no Kafka broker to bootstrap from");
        let bootstrap = settings.brokers.iter().map(|b| host_port(b)).collect::<Result<_, _>>()?;
        Ok(Self {
            dialer: Dialer {
                tls: settings.tls.clone(),
                timeout: settings.timeout,
                client_id: StrBytes::from_string(settings.client_id.clone()),
                sasl: settings.sasl.clone(),
                required: CONSUMER_APIS,
            },
            bootstrap: Arc::new(bootstrap),
            brokers: Default::default(),
            dialed: Default::default(),
            counters,
        })
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.dialer.timeout
    }

    pub(crate) fn set_brokers(&self, brokers: HashMap<i32, (String, u16)>) {
        *self.brokers.lock().unwrap() = brokers;
    }

    fn broker(&self, node: i32) -> Option<(String, u16)> {
        self.brokers.lock().unwrap().get(&node).cloned()
    }

    async fn dial(&self, slot: Slot, target: &Target, api: ApiKey) -> anyhow::Result<Conn> {
        let result = match target {
            Target::Addr(host, port) => self.dialer.connect(host, *port).await,
            Target::Node(node) => match self.broker(*node) {
                Some((host, port)) => self.dialer.connect(&host, port).await,
                None => Err(anyhow!("no address for Kafka broker {node}")),
            },
            Target::Any => self.dial_any().await,
        };
        match &result {
            Ok(_) => {
                if !self.dialed.lock().unwrap().insert(slot) {
                    self.counters.count("logit.input.reconnects", 1.0, &[]);
                }
            }
            Err(err) => self.counters.dial_failed(api, err),
        }
        result
    }

    async fn dial_any(&self) -> anyhow::Result<Conn> {
        let mut targets: Vec<(String, u16)> = self.bootstrap.to_vec();
        let mut known: Vec<_> = self.brokers.lock().unwrap().clone().into_iter().collect();
        known.sort();
        targets.extend(known.into_iter().map(|(_, addr)| addr));
        let mut last = None;
        for (host, port) in targets {
            match self.dialer.connect(&host, port).await {
                Ok(conn) => return Ok(conn),
                // Every broker gives the same versions and SASL verdict.
                Err(err) if logit_pipeline::classify(&err) == logit_pipeline::Fault::Refused => {
                    return Err(err)
                }
                Err(err) => last = Some(err),
            }
        }
        Err(last.expect("at least one bootstrap entry"))
    }
}

/// A response that carries `throttle_time_ms`.
pub(crate) trait Throttled {
    fn throttle_ms(&self) -> i32;
}

macro_rules! throttled {
    ($($t:ty),*) => {$(
        impl Throttled for $t {
            fn throttle_ms(&self) -> i32 {
                self.throttle_time_ms
            }
        }
    )*};
}

throttled!(
    FetchResponse,
    FindCoordinatorResponse,
    HeartbeatResponse,
    JoinGroupResponse,
    LeaveGroupResponse,
    ListOffsetsResponse,
    MetadataResponse,
    OffsetCommitResponse,
    OffsetFetchResponse,
    SyncGroupResponse
);

/// One role's connection: dialed on demand, dropped on a failed request, muted by a throttle.
pub(crate) struct Link {
    slot: Slot,
    pub(crate) target: Target,
    conn: Option<Conn>,
    muted_until: Option<Instant>,
}

impl Link {
    pub(crate) fn new(slot: Slot, target: Target) -> Self {
        Self { slot, target, conn: None, muted_until: None }
    }

    /// Points the link at `target`, dropping a connection to anywhere else.
    pub(crate) fn retarget(&mut self, target: Target) {
        if self.target != target {
            self.target = target;
            self.conn = None;
        }
    }

    /// Sends the request `build` makes for the negotiated version of `api`, after any throttle,
    /// and waits `wait` for the answer (the timeout when `None`).
    pub(crate) async fn call<R, F>(
        &mut self,
        pool: &Pool,
        api: ApiKey,
        wait: Option<Duration>,
        build: F,
    ) -> Result<(i16, R::Response), CallError>
    where
        R: Request,
        R::Response: Throttled,
        F: FnOnce(i16) -> R,
    {
        if let Some(at) = self.muted_until.take() {
            tokio::time::sleep_until(at).await;
        }
        if self.conn.is_some() && !self.conn.as_mut().expect("checked").is_open().await {
            self.conn = None;
        }
        let conn = match &mut self.conn {
            Some(conn) => conn,
            None => {
                let conn =
                    pool.dial(self.slot, &self.target, api).await.map_err(CallError::Dial)?;
                self.conn.insert(conn)
            }
        };
        let version = conn
            .versions()
            .for_key(api)
            .expect("the dialer requires a version of every consumer API");
        let request = build(version);
        match conn.request_within(&request, version, wait.unwrap_or(pool.timeout())).await {
            Ok(response) => {
                let throttle = response.throttle_ms();
                if throttle > 0 {
                    self.muted_until =
                        Some(Instant::now() + Duration::from_millis(throttle as u64));
                }
                Ok((version, response))
            }
            Err(err) => {
                self.conn = None;
                pool.counters.broker_error(api, "transport");
                Err(CallError::Transport(err.error.context(format!("{api:?}"))))
            }
        }
    }
}
