//! `kafka_in`'s consumer-group client: the classic group protocol, the fetch path, and offset
//! commits, driven by [`Consumer`] (`docs/adr/kafka-input.md`, items 2 to 9). It reads no
//! config: `kafka_in` fills [`ConsumerSettings`] and runs the call sequence in
//! [`Consumer`]'s module doc, `client`.
//!
//! - `assignor`: the `range` and `roundrobin` assignors and Java's `ConsumerProtocol`
//!   subscription and assignment bytes.
//! - `group` and `heartbeat`: FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup; the
//!   heartbeat runs on its own task, so a caller slow to accept keeps its session.
//! - `offsets`: OffsetFetch, OffsetCommit, ListOffsets, and the contiguous [`Tracker`].
//! - `fetch`: one full-fetch loop per leader, and the reading of a records blob.
//! - `metadata` and `pool`: Metadata with auto-creation off, and one connection per role with
//!   throttle muting. Every connection requires [`crate::conn::CONSUMER_APIS`].
//!
//! # Outcomes
//!
//! This table is the canonical copy of the ADR's item 9: every broker outcome is one of five
//! actions, **rejoin**, **refresh** metadata and retry, **rediscover** the coordinator and
//! retry, **back off** and retry with no end, or **skip** and count. A backoff starts at 100 ms
//! and doubles to 10 s, reset by a success ([`Backoff`]). No outcome ends the consumer. Each
//! row counts `logit.input.broker.errors{api, error}`, `error` the snake_case Kafka error name or
//! `transport`, `tls`, `sasl`, `unsupported_version`, `response_too_large` (an answer over its
//! request's cap: Fetch's below, 64 MiB for any other API), or `malformed` (a Metadata answer
//! that doesn't hold together), unless it says otherwise, and each names the test in `tests`
//! that pins it.
//!
//! | Outcome | Code | Action | Also counted | Test |
//! |---|---|---|---|---|
//! | Connect, TLS, or timeout failure; a disconnect | — | Back off; a fetch loop's asks for a refresh, a coordinator link's rediscovers | `logit.input.reconnects` on the redial | `a_broker_restart_redials_and_counts_a_reconnect`, `an_expired_session_rejoins_with_an_empty_id`, `a_failed_commit_keeps_the_positions_for_the_next_one` |
//! | SASL refused, or a token endpoint's `4xx` | 33, 34, 58 | Back off; no other mechanism, no fallback to none | Diagnostic `auth_failed` | `a_refused_sasl_login_backs_off_with_no_fallback` |
//! | `OFFSET_OUT_OF_RANGE` | 1 | ListOffsets earliest and latest; below the log start moves to it, above the log end moves back to it | `offsets.reset{reason="out_of_range"}`; below the start, `records` and `records.dropped{reason="retention"}` for the offsets between, and diagnostic `records_dropped` | `a_position_below_the_log_start_moves_to_it_and_counts_the_gap_as_retention`, `a_position_above_the_log_end_moves_back_to_it` |
//! | A batch whose CRC-32C doesn't match | — | Refetch once from the same offset; a second mismatch at that batch skips it: the skip ends at the base offset of the next batch in the answer whose CRC passes; else at the base offset in the first 12 bytes of a batch the answer cut short, when it's above the failed batch's base and at or below the high watermark; else, after one refetch widened to `max_batch_bytes` (the row for a lone failed batch), at the high watermark (the last stable offset under `read_committed`). It never passes the high watermark, and reads nothing from the failed batch's header but its base offset, since neither its length nor its offset delta can be trusted; every entry decoded between it and the landing batch is skipped with it. A batch whose length reaches past the answer's end at the fetch position is corrupt the same way and skips to the high watermark | `records` and `records.dropped{reason="corrupt"}`, the offsets skipped; diagnostic `records_dropped`; no broker error | `a_crc_mismatch_refetches_once_and_reads_the_batch`, `a_second_crc_mismatch_skips_the_batch_as_corrupt`, `a_corrupt_batch_with_a_far_delta_skips_to_the_next_batch`, `a_corrupt_batch_before_a_cut_one_skips_to_the_cut_batchs_base`, `a_corrupt_last_batch_skips_to_the_high_watermark` |
//! | `CORRUPT_MESSAGE` on a Fetch partition | 2 | Back off and refetch from the same offset; an answer with no batch names nothing to skip | — | `corrupt_message_on_fetch_refetches_after_a_backoff` |
//! | `UNKNOWN_TOPIC_OR_PARTITION` on Metadata | 3 | Not assigned until it exists; a refresh (`metadata_max_age`) that finds it, or a subscribed topic's partition count changed, revokes and rejoins | Diagnostic `topic_missing`; `rebalances{reason="metadata"}` | `a_missing_topic_is_subscribed_once_it_appears` |
//! | `NOT_LEADER_OR_FOLLOWER` on Fetch | 6 | Refresh, then fetch from the leader after a backoff | — | `a_leader_move_refreshes_metadata_and_fetches_from_the_new_leader` |
//! | `UNKNOWN_TOPIC_OR_PARTITION` on Fetch, `LEADER_NOT_AVAILABLE`, `REPLICA_NOT_AVAILABLE`, `KAFKA_STORAGE_ERROR`, `FENCED_LEADER_EPOCH`, `UNKNOWN_LEADER_EPOCH`, `OFFSET_NOT_AVAILABLE` | 3, 5, 9, 56, 74, 75, 78 | As 6 | — | `every_refresh_code_on_fetch_refreshes_and_refetches` |
//! | `COORDINATOR_LOAD_IN_PROGRESS` | 14 | Back off, same coordinator | — | `coordinator_load_in_progress_backs_off_on_the_same_coordinator` |
//! | `COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR` | 15, 16 | Rediscover | — | `not_coordinator_and_coordinator_not_available_rediscover` |
//! | `REBALANCE_IN_PROGRESS` | 27 | Revoke, commit, rejoin | `rebalances{reason="rebalance"}` | `rebalance_in_progress_on_a_heartbeat_revokes_commits_and_rejoins`, `two_members_split_the_partitions_through_a_late_join_and_a_leave` |
//! | `ILLEGAL_GENERATION` | 22 | Revoke without a commit, rejoin under the same member id | `rebalances{reason="membership_lost"}` | `illegal_generation_revokes_without_a_commit_and_rejoins_under_the_same_id` |
//! | `UNKNOWN_MEMBER_ID`, `FENCED_INSTANCE_ID`, or a session that expired | 25, 82 | Revoke without a commit, rejoin with an empty member id | `rebalances{reason="membership_lost"}` | `unknown_member_id_and_fenced_instance_id_rejoin_with_an_empty_id`, `an_expired_session_rejoins_with_an_empty_id` |
//! | `MEMBER_ID_REQUIRED` | 79 | JoinGroup again under the id it carries, up to three rounds | Not counted | `a_first_join_follows_member_id_required_and_reads_from_the_log_start` |
//! | `INCONSISTENT_GROUP_PROTOCOL`, `INVALID_SESSION_TIMEOUT`, `GROUP_MAX_SIZE_REACHED` | 23, 26, 81 | Back off and rejoin | Diagnostic `group_refused` | `group_refusals_back_off_and_rejoin_with_group_refused` |
//! | `TOPIC_AUTHORIZATION_FAILED` | 29 | Back off; the partition stays assigned and unread | Diagnostic `not_authorized` | `topic_authorization_failed_on_fetch_backs_off_with_not_authorized` |
//! | `GROUP_AUTHORIZATION_FAILED` | 30 | Back off on the group request that got it | Diagnostic `not_authorized` | `group_authorization_failed_backs_off_with_not_authorized` |
//! | `UNSUPPORTED_VERSION` | 35 | Back off | Diagnostic `unsupported_version` | `unsupported_version_from_a_group_request_backs_off` |
//! | A broker with no version of a required API in range | — | Back off; a rolling upgrade can end it | Diagnostic `unsupported_version`, error `unsupported_version` | `a_broker_without_a_required_api_backs_off_as_unsupported_version` |
//! | `UNSTABLE_OFFSET_COMMIT` on OffsetFetch | 88 | Back off | — | `unstable_offset_commit_on_offset_fetch_backs_off` |
//! | An OffsetCommit with a transport error or a retriable code | — | The positions stay pending for the next commit | `commits{class="error"}` | `a_failed_commit_keeps_the_positions_for_the_next_one` |
//! | An OffsetCommit refused for generation or membership | 22, 25, 82 | As the code's row; the positions are dropped | `commits{class="fenced"}` | `a_commit_refused_for_membership_never_lands` |
//! | `throttle_time_ms` above 0 in any answer | — | No request on that connection before it passes | Not counted | `a_throttled_answer_mutes_the_connection_until_it_passes` |
//! | A batch whose records decompress past `max_batch_bytes` | — | Skip it | `records` and `records.dropped{reason="oversize"}`; diagnostic `records_dropped`; no broker error | `a_batch_over_max_batch_bytes_is_skipped_as_oversize` |
//! | A batch that doesn't decompress, or whose records don't parse | — | As a CRC mismatch: refetch once, then skip; the skip ends at the base offset of the next batch in the answer whose CRC passes; else at the base offset in the first 12 bytes of a batch the answer cut short, when it's above the failed batch's base and at or below the high watermark; else, after one refetch widened to `max_batch_bytes` (the row for a lone failed batch), at the high watermark (the last stable offset under `read_committed`). It never passes the high watermark, and reads nothing from the failed batch's header but its base offset, since neither its length nor its offset delta can be trusted; every entry decoded between it and the landing batch is skipped with it. A batch whose length reaches past the answer's end at the fetch position is corrupt the same way and skips to the high watermark | `records.dropped{reason="corrupt"}`, the offsets skipped; no broker error | `a_batch_that_does_not_decompress_is_skipped_as_corrupt` |
//! | A batch that failed twice with no CRC-verified batch after it in the answer and no cut batch's base within the high watermark, and offsets past its base before the high watermark | — | Refetch the partition once with `max_batch_bytes` of room (`fetch.max_bytes` raised by as much for that request), so the next batch arrives whole beside it and its base is the landing; if nothing verified follows even then, skip to the high watermark | As a CRC mismatch | `a_lone_corrupt_batch_is_refetched_widened_and_skips_to_the_next_batch`, `fetch::tests::a_lone_failed_batch_is_refetched_widened_before_skipping_to_end` |
//! | A magic 0 or 1 message set | — | Skip it | `records.dropped{reason="legacy_format"}`, one per offset it covers; no broker error | `a_legacy_message_set_is_skipped_as_legacy_format` |
//! | A Fetch answer whose length is over `fetch.max_bytes` plus `max_batch_bytes` plus 1 MiB | — | Back off and refetch; with no offsets read, nothing can be skipped, and every partition that broker's fetch loop asks for holds with it, since one request covers them all (`docs/known-gaps/kafka.md`) | Error `response_too_large`; diagnostic `response_too_large`, naming `max_batch_bytes` | `an_answer_over_the_fetch_cap_backs_off_as_response_too_large` |
//! | A Metadata answer with a partition index outside its partition entries | — | The topic is left out of that answer, as one with an error code | Error `malformed`; diagnostic `broker_error` | `metadata::tests::an_index_outside_the_entries_is_malformed` |
//!
//! `OFFSET_METADATA_TOO_LARGE` (12) can't occur: no commit carries metadata. Any other code backs
//! off with diagnostic `broker_error` and counts under its own name. A record whose value
//! doesn't decode, a tombstone, an empty value, an omitted header, and a deferred or dropped
//! batch are the input's to count, not this client's.
//!
//! Under `read_committed`, a record of an aborted transaction counts
//! `records.skipped{reason="aborted"}` and a control record isn't counted
//! (`read_committed_skips_aborted_transactions_and_control_records`).

mod assignor;
mod client;
mod fetch;
mod group;
mod heartbeat;
mod metadata;
mod offsets;
mod pool;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use kafka_protocol::messages::ApiKey;
use kafka_protocol::ResponseError;
use logit_core::{Diagnostics, Telemetry};

use crate::conn::{DialError, DialStage};
use crate::error::code;
use crate::sasl::Authenticator;

pub use assignor::{
    decode_assignment, decode_subscription, encode_assignment, encode_subscription, range,
    round_robin, Assignor,
};
pub use client::{CommitOutcome, Consumer, Next, RevokeReason};
pub use fetch::Fetched;
pub use offsets::Tracker;

/// A partition of a topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicPartition {
    pub topic: Arc<str>,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: &str, partition: i32) -> Self {
        Self { topic: Arc::from(topic), partition }
    }
}

impl std::fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

/// Where a partition with no committed offset starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPosition {
    Latest,
    Earliest,
}

impl StartPosition {
    /// The `ListOffsets` timestamp that asks for it.
    fn timestamp(self) -> i64 {
        match self {
            StartPosition::Latest => offsets::LATEST,
            StartPosition::Earliest => offsets::EARLIEST,
        }
    }
}

/// Which records a fetch reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    ReadUncommitted,
    /// Only committed transactions' records, up to the last stable offset.
    ReadCommitted,
}

impl Isolation {
    fn wire(self) -> i8 {
        match self {
            Isolation::ReadUncommitted => 0,
            Isolation::ReadCommitted => 1,
        }
    }
}

/// A Fetch request's bounds (`fetch:` in config).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchBounds {
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub max_partition_bytes: i32,
    pub max_wait: Duration,
}

impl Default for FetchBounds {
    /// Java's `fetch.min.bytes`, `fetch.max.bytes`, `max.partition.fetch.bytes`, and
    /// `fetch.max.wait.ms`.
    fn default() -> Self {
        Self {
            min_bytes: 1,
            max_bytes: 50 << 20,
            max_partition_bytes: 1 << 20,
            max_wait: Duration::from_millis(500),
        }
    }
}

/// Everything a [`Consumer`] is built from; `kafka_in` fills it from its config.
#[derive(Clone)]
pub struct ConsumerSettings {
    /// `host:port` entries to bootstrap from.
    pub brokers: Vec<String>,
    pub group: String,
    pub topics: Vec<String>,
    pub client_id: String,
    pub assignment: Assignor,
    pub start: StartPosition,
    pub isolation: Isolation,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    /// JoinGroup's `rebalance_timeout_ms`: how long the coordinator waits for every member.
    pub rebalance_timeout: Duration,
    pub commit_interval: Duration,
    /// Bounds every connect, write, and response wait but a JoinGroup's and a Fetch's.
    pub timeout: Duration,
    pub fetch: FetchBounds,
    /// The most one record batch may decompress to.
    pub max_batch_bytes: usize,
    /// How often cluster metadata is refreshed with nothing asking for it sooner.
    pub metadata_max_age: Duration,
    pub tls: Option<Arc<rustls::ClientConfig>>,
    pub sasl: Option<Arc<dyn Authenticator>>,
}

impl ConsumerSettings {
    /// The ADR's defaults (`docs/adr/kafka-input.md`, item 2) for `group` reading `topics`.
    pub fn new(brokers: Vec<String>, group: &str, topics: Vec<String>) -> Self {
        Self {
            brokers,
            group: group.to_string(),
            topics,
            client_id: "logit".to_string(),
            assignment: Assignor::Range,
            start: StartPosition::Latest,
            isolation: Isolation::ReadUncommitted,
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(3),
            rebalance_timeout: Duration::from_secs(300),
            commit_interval: Duration::from_secs(5),
            timeout: Duration::from_secs(30),
            fetch: FetchBounds::default(),
            max_batch_bytes: 64 << 20,
            metadata_max_age: Duration::from_secs(300),
            tls: None,
            sasl: None,
        }
    }
}

/// The wait before a retry: 100 ms, doubling to 10 s, back to 100 ms after a success.
#[derive(Debug, Clone)]
pub struct Backoff {
    next: Duration,
}

impl Backoff {
    pub const MIN: Duration = Duration::from_millis(100);
    pub const MAX: Duration = Duration::from_secs(10);

    pub fn new() -> Self {
        Self { next: Self::MIN }
    }

    /// The wait to take now; doubles the one after it.
    pub fn step(&mut self) -> Duration {
        let wait = self.next;
        self.next = (self.next * 2).min(Self::MAX);
        wait
    }

    pub fn reset(&mut self) {
        self.next = Self::MIN;
    }

    /// Waits [`Backoff::step`].
    pub async fn wait(&mut self) {
        tokio::time::sleep(self.step()).await;
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

/// What a group request's error code asks of the member (`docs/adr/kafka-input.md`, item 9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupCode {
    Ok,
    /// Find the coordinator again: 15, 16.
    Rediscover,
    /// Retry the same coordinator after a backoff, with a diagnostic when `Some`.
    BackOff(Option<&'static str>),
    /// Revoke, commit, and rejoin: 27.
    Rebalance,
    /// Membership lost: revoke without a commit and rejoin, under a new member id when
    /// `reset_member` (25, 82) and the same one otherwise (22).
    Fenced {
        reset_member: bool,
    },
}

pub(crate) fn group_code(error: i16) -> GroupCode {
    match error {
        code::NONE => GroupCode::Ok,
        code::COORDINATOR_NOT_AVAILABLE | code::NOT_COORDINATOR => GroupCode::Rediscover,
        code::COORDINATOR_LOAD_IN_PROGRESS => GroupCode::BackOff(None),
        code::REBALANCE_IN_PROGRESS => GroupCode::Rebalance,
        code::ILLEGAL_GENERATION => GroupCode::Fenced { reset_member: false },
        code::UNKNOWN_MEMBER_ID | code::FENCED_INSTANCE_ID => {
            GroupCode::Fenced { reset_member: true }
        }
        code::INCONSISTENT_GROUP_PROTOCOL
        | code::INVALID_SESSION_TIMEOUT
        | code::GROUP_MAX_SIZE_REACHED => GroupCode::BackOff(Some("group_refused")),
        code::GROUP_AUTHORIZATION_FAILED | code::TOPIC_AUTHORIZATION_FAILED => {
            GroupCode::BackOff(Some("not_authorized"))
        }
        code::UNSUPPORTED_VERSION => GroupCode::BackOff(Some("unsupported_version")),
        code::UNSTABLE_OFFSET_COMMIT => GroupCode::BackOff(None),
        _ => GroupCode::BackOff(Some("broker_error")),
    }
}

/// What a partition's error code in a Fetch or ListOffsets answer asks of its fetch loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartitionCode {
    Ok,
    OutOfRange,
    /// Back off and refetch from the same offset, with no end: the answer holds no batch to
    /// skip.
    Corrupt,
    /// Refresh metadata, then fetch from the leader after a backoff.
    Refresh,
    /// Back off; the partition stays assigned. A diagnostic when `Some`.
    BackOff(Option<&'static str>),
}

pub(crate) fn partition_code(error: i16) -> PartitionCode {
    match error {
        code::NONE => PartitionCode::Ok,
        code::OFFSET_OUT_OF_RANGE => PartitionCode::OutOfRange,
        code::CORRUPT_MESSAGE => PartitionCode::Corrupt,
        code::UNKNOWN_TOPIC_OR_PARTITION
        | code::LEADER_NOT_AVAILABLE
        | code::NOT_LEADER_OR_FOLLOWER
        | code::REPLICA_NOT_AVAILABLE
        | code::KAFKA_STORAGE_ERROR
        | code::FENCED_LEADER_EPOCH
        | code::UNKNOWN_LEADER_EPOCH
        | code::OFFSET_NOT_AVAILABLE => PartitionCode::Refresh,
        code::TOPIC_AUTHORIZATION_FAILED => PartitionCode::BackOff(Some("not_authorized")),
        code::UNSUPPORTED_VERSION => PartitionCode::BackOff(Some("unsupported_version")),
        _ => PartitionCode::BackOff(Some("broker_error")),
    }
}

/// The consumer's telemetry and diagnostics handles, shared by its tasks.
#[derive(Clone, Default)]
pub(crate) struct Counters {
    pub(crate) telemetry: Telemetry,
    pub(crate) diag: Diagnostics,
}

impl Counters {
    pub(crate) fn count(&self, name: &'static str, n: f64, tags: &[(&'static str, &'static str)]) {
        self.telemetry.count(name, n, tags);
    }

    /// `logit.input.broker.errors{api, error}`.
    pub(crate) fn broker_error(&self, api: ApiKey, error: &'static str) {
        self.count("logit.input.broker.errors", 1.0, &[("api", api_tag(api)), ("error", error)]);
    }

    /// [`Counters::broker_error`] for a Kafka error code.
    pub(crate) fn broker_code(&self, api: ApiKey, error: i16) {
        self.broker_error(api, code_tag(error));
    }

    /// A throttled diagnostic under `key`.
    pub(crate) fn warn(&self, key: &'static str, msg: impl std::fmt::Display) {
        self.diag.clone().warn_throttled(key, msg);
    }

    /// Counts and reports a failed dial made for `api` by the stage it failed in: `tls`, `sasl`
    /// (diagnostic `auth_failed` when refused), `unsupported_version` (diagnostic
    /// `unsupported_version`), or `transport` for a connect or `ApiVersions` failure.
    pub(crate) fn dial_failed(&self, api: ApiKey, err: &DialError) {
        let refused = logit_pipeline::classify(&err.error) == logit_pipeline::Fault::Refused;
        let error = match err.stage {
            DialStage::Connect | DialStage::ApiVersions => "transport",
            DialStage::Tls => "tls",
            DialStage::Negotiate => {
                self.warn("unsupported_version", format_args!("Kafka broker refused: {err}"));
                "unsupported_version"
            }
            DialStage::Sasl => {
                if refused {
                    self.warn("auth_failed", format_args!("Kafka authentication failed: {err}"));
                }
                "sasl"
            }
        };
        self.broker_error(api, error);
    }
}

/// A process-lifetime `&'static str` for a tag value: telemetry tags are `&'static str`, and the
/// values here come from small sets (topics, partitions, error and API names).
pub(crate) fn intern(value: &str) -> &'static str {
    static TABLE: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let mut table = TABLE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
    if let Some(&known) = table.get(value) {
        return known;
    }
    let leaked: &'static str = Box::leak(value.to_string().into_boxed_str());
    table.insert(leaked);
    leaked
}

/// `snake_case` of a `CamelCase` name.
fn snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// The lowercase Kafka error name of `error`, as `logit.input.broker.errors` tags it.
pub(crate) fn code_tag(error: i16) -> &'static str {
    match ResponseError::try_from_code(error) {
        None => "none",
        Some(ResponseError::Unknown(n)) => intern(&format!("unknown_{n}")),
        Some(err) => intern(&snake(&format!("{err:?}"))),
    }
}

/// An API's tag, `list_offsets` for `ListOffsets`.
pub(crate) fn api_tag(api: ApiKey) -> &'static str {
    intern(&snake(&format!("{api:?}")))
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn tags_are_snake_case_kafka_names() {
        assert_eq!(code_tag(code::OFFSET_OUT_OF_RANGE), "offset_out_of_range");
        assert_eq!(code_tag(code::NOT_LEADER_OR_FOLLOWER), "not_leader_or_follower");
        assert_eq!(code_tag(9999), "unknown_9999");
        assert_eq!(api_tag(ApiKey::ListOffsets), "list_offsets");
        assert_eq!(api_tag(ApiKey::Fetch), "fetch");
    }

    /// The stage decides the tag, whatever the text says.
    #[test]
    fn a_dial_failure_is_tagged_by_its_stage_not_its_text() {
        use anyhow::Context as _;
        use logit_pipeline::test_util::TelemetryProbe;
        use logit_pipeline::Fault;

        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("in", "kafka_in", "listener");
        let counters = Counters {
            telemetry: telemetry.clone(),
            diag: Diagnostics::new("in").with_telemetry(telemetry),
        };
        let failed = |stage, text: &'static str, fault: Fault| DialError {
            stage,
            error: Err::<(), _>(anyhow::anyhow!(text)).context(fault).unwrap_err(),
        };
        counters.dial_failed(ApiKey::Fetch, &failed(DialStage::Sasl, "refused", Fault::Refused));
        counters.dial_failed(ApiKey::Fetch, &failed(DialStage::Tls, "SASL", Fault::Clean));
        counters.dial_failed(ApiKey::Fetch, &failed(DialStage::Connect, "TLS", Fault::Clean));
        counters.dial_failed(ApiKey::Fetch, &failed(DialStage::Negotiate, "x", Fault::Refused));
        let errors = |probe: &mut TelemetryProbe, error| {
            probe.sum("logit.input.broker.errors", &[("api", "fetch"), ("error", error)])
        };
        assert_eq!(errors(&mut probe, "sasl"), 1.0);
        assert_eq!(errors(&mut probe, "tls"), 1.0);
        assert_eq!(errors(&mut probe, "transport"), 1.0);
        assert_eq!(errors(&mut probe, "unsupported_version"), 1.0);
        let diag = |probe: &mut TelemetryProbe, key| {
            probe.sum("logit.component.diagnostics", &[("key", key)])
        };
        assert_eq!(diag(&mut probe, "auth_failed"), 1.0);
        assert_eq!(diag(&mut probe, "unsupported_version"), 1.0);
    }

    #[test]
    fn the_backoff_doubles_from_100ms_to_10s_and_resets() {
        let mut b = Backoff::new();
        let waits: Vec<u128> = (0..9).map(|_| b.step().as_millis()).collect();
        assert_eq!(waits, [100, 200, 400, 800, 1600, 3200, 6400, 10_000, 10_000]);
        b.reset();
        assert_eq!(b.step(), Backoff::MIN);
    }
}
