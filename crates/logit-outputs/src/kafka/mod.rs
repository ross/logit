//! `kafka_out`: a pure-Rust Kafka producer on `kafka-protocol`'s message codecs, with the Java
//! producer 4.x's semantics and the divergences `docs/adr/kafka-output.md` names.
//!
//! - [`sink`] is the `Output`: one record per event, and one pending batch kept across
//!   `write_loop`'s attempts at it.
//! - [`encode`] writes each record's value, key, and timestamp.
//! - [`producer`] partitions a batch, keeps the idempotent producer's identity and sequences,
//!   and sends each attempt with a verdict per partition batch.
//! - [`conn`] is one broker connection: framing, correlation ids, version negotiation, TLS.
//! - [`batch`] and [`compression`] write record batch v2 with all five codecs through the
//!   crate's compression hook, so none of its C codecs is built.
//! - [`partitioner`] is the Java client's murmur2 keyed partitioner and the per-send sticky
//!   choice for unkeyed records.
//! - [`fault`] maps a partition's error code to what the producer does with it.
//!
//! # Response classes
//!
//! This table is the canonical account of how the producer reads every outcome of a produce
//! request, per partition batch. Each row names the test that pins it; the per-code
//! classification is pinned again row by row in `fault`'s
//! `every_error_code_row_classifies_as_its_table_says`, and through the producer against the fake
//! broker in `tests::each_error_code_row_classifies_the_partition_batch`.
//!
//! "Settles" means the partition batch is never resent for this `logit` batch. "Gap" means a
//! sequence gap: the next attempt bumps the epoch (same PID, epoch + 1, every partition's
//! sequence back to 0). "Refresh" means metadata is refreshed before the next attempt.
//!
//! | Response | Class | Why | Evidence |
//! |---|---|---|---|
//! | Connect refused or timed out, DNS failure | `Clean` | no request byte left | `tests::a_refused_bootstrap_connect_is_clean`, `tests::a_refused_leader_connect_is_clean` |
//! | TLS handshake failure | `Clean` | no request byte left | `tests::a_failed_tls_handshake_is_clean` |
//! | The first write fails or accepts nothing, on plaintext | `Clean` | the broker can't have the request | `conn::tests::a_plaintext_first_write_that_accepts_nothing_is_clean` |
//! | A pooled connection the broker closed | redialed before writing; no failure | the probe finds the close before any byte is written | `tests::a_pooled_connection_the_broker_closed_is_redialed` |
//! | A write fails after any byte, or on TLS at all | `Ambiguous` | the broker may have the request | `conn::tests::a_failure_after_part_of_the_request_is_written_is_ambiguous`, `conn::tests::a_tls_write_failure_is_ambiguous` |
//! | A close, a timeout, or a framing error while waiting for the response | `Ambiguous` | the broker may have appended | `tests::a_close_after_the_request_is_written_is_ambiguous`, `tests::a_response_timeout_is_ambiguous`, `conn::tests::a_close_while_waiting_for_the_response_is_ambiguous`, `conn::tests::a_response_with_the_wrong_correlation_id_is_ambiguous` |
//! | A partition with no leader in metadata | `Clean`, refresh | not sent; a keyed record waits for its own partition | `tests::a_keyed_record_waits_for_its_leaderless_partition` |
//! | A broker without Produce v3, or zstd below Produce v7 | `Refused` | every batch gets the same answer | `tests::a_broker_without_produce_v3_is_refused`, `tests::zstd_needs_produce_v7` |
//! | `NONE` (0), `DUPLICATE_SEQUENCE_NUMBER` (46) | accepted; settles | appended, or already appended by an earlier attempt | `tests::keyed_records_land_on_their_java_partition_through_each_leader` |
//! | `CORRUPT_MESSAGE` (2) | `Rejected`; settles, gap | the CRC is computed from the batch, so a resend carries the same bytes | `tests::a_rejected_partition_batch_bumps_the_epoch_and_sequences_restart_at_zero` |
//! | `UNKNOWN_TOPIC_OR_PARTITION` (3), `UNKNOWN_TOPIC_ID` (100), from `Produce` or `Metadata` | `Clean` and a refresh the first time; `Refused` once it persists after a completed refresh, until a partition batch is accepted. A first `Metadata` answer's refresh runs in the same attempt and polls, while the topic is still missing or leaderless, until `timeout` (`producer::MissingTopic`) | nothing was appended; a broker that auto-creates the topic answers the request that created it this way, and the refresh asks it to create the topic | `tests::a_missing_topic_is_refused_only_after_a_refresh_confirms_it`, `tests::a_missing_topic_produce_answer_is_refused_only_after_a_refresh`, `sink::tests::a_missing_topic_is_refused_and_a_created_one_is_sent` |
//! | `LEADER_NOT_AVAILABLE` (5), `NOT_LEADER_OR_FOLLOWER` (6), `KAFKA_STORAGE_ERROR` (56), `INCONSISTENT_TOPIC_ID` (103) | `Clean`, refresh | nothing was appended; the leader or the topic changed | `tests::a_moved_leader_is_followed_after_a_refresh` |
//! | `REQUEST_TIMED_OUT` (7), `NETWORK_EXCEPTION` (13), `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20) | `Ambiguous` | the leader may have appended | `tests::request_timed_out_after_an_append_is_ambiguous_and_the_retry_deduplicates` |
//! | `MESSAGE_TOO_LARGE` (10) | halved once, each half resent under fresh sequences; on one record or a half, `Rejected`; gap | the partition batch exceeds the broker's message size | `tests::message_too_large_halves_the_batch_once`, `tests::message_too_large_on_a_half_is_rejected_not_split_again`, `tests::message_too_large_on_one_record_is_rejected` |
//! | `RECORD_LIST_TOO_LARGE` (18), `POLICY_VIOLATION` (44) | `Rejected`; settles, gap | a resend gets the same answer | `tests::a_send_fails_rejected_only_when_nothing_was_accepted` |
//! | `NOT_ENOUGH_REPLICAS` (19), `UNKNOWN_PRODUCER_ID` (59), `THROTTLING_QUOTA_EXCEEDED` (89) | `Clean` | rejected before the append | `tests::several_batches_of_one_partition_go_in_order_and_stop_behind_a_failure` |
//! | `INVALID_REQUIRED_ACKS` (21), `TOPIC_AUTHORIZATION_FAILED` (29), `CLUSTER_AUTHORIZATION_FAILED` (31), `UNSUPPORTED_VERSION` (35), `UNSUPPORTED_COMPRESSION_TYPE` (76) | `Refused` | configuration, credentials, or versions: every batch gets the same answer | `tests::settled_partitions_are_not_resent_and_the_strongest_class_wins` |
//! | `INVALID_TIMESTAMP` (32), `INVALID_RECORD` (87) | the records `record_errors` names are dropped and counted, the rest resent once under fresh sequences; with none named (below Produce v8) or on the resend, `Rejected`; gap | the broker refused those records alone | `tests::named_records_are_dropped_and_the_rest_resent_once`, `tests::named_records_below_produce_v8_are_rejected` |
//! | `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) | `Ambiguous`, epoch bumped | no gap is left inside an epoch, so the broker lost its sequence state; an earlier attempt may have landed | `tests::sequence_and_epoch_errors_renew_the_producer_before_the_retry` |
//! | `INVALID_PRODUCER_EPOCH` (47) | `Clean`, a new PID from `InitProducerId` | the broker fenced the epoch before appending | `tests::sequence_and_epoch_errors_renew_the_producer_before_the_retry`, `tests::the_fake_broker_fences_a_stale_epoch` |
//! | Any other code | `Rejected`; settles, gap | the runtime's default for an answer a sink can't attribute; the message names the code | `tests::each_error_code_row_classifies_the_partition_batch` |
//!
//! A send's result covers the partition batches still unsettled: the strongest of their classes,
//! `Refused` over `Ambiguous` over `Clean`. With none unsettled, it succeeds when any was
//! accepted on this attempt or an earlier one, and otherwise fails `Rejected` with the first
//! rejection (`producer::Attempt::result`).
//!
//! The ranking doesn't depend on the posture, but what follows a `Refused` result does. Under
//! `at_most_once`, the sink settles each partition batch left `Ambiguous` as dropped before the
//! retry (`producer::Producer::drop_ambiguous`), so the retry resends only the partition batches
//! the broker can't have appended, and the send still returns `Refused` for those
//! (`sink::tests::at_most_once_drops_an_ambiguous_partition_when_another_is_refused`).

mod batch;
mod compression;
mod conn;
mod encode;
#[cfg(test)]
mod fake_broker;
mod fault;
mod partitioner;
mod producer;
mod sink;
#[cfg(test)]
mod tests;

pub use encode::{RecordFormat, RecordKey};
pub use sink::{
    KafkaAcks, KafkaCompression, KafkaOutput, KafkaSettings, TlsClientSettings,
    DEFAULT_MAX_REQUEST_BYTES, DEFAULT_TIMEOUT,
};
