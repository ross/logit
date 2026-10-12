//! `kafka_out`: a pure-Rust Kafka producer on `kafka-protocol`'s message codecs, with the Java
//! producer 4.x's semantics and the divergences `docs/adr/kafka-output.md` names.
//!
//! - [`sink`] is the `Output`: one pending batch kept across `write_loop`'s attempts at it.
//! - [`encode`] writes each record's value, key, and timestamp, one event per record or a run of
//!   events per record under `record: batch`, in each `format:`.
//! - [`producer`] partitions a batch, keeps the idempotent producer's identity and sequences,
//!   and sends each attempt with a verdict per partition batch.
//! - [`conn`] is one broker connection: framing, correlation ids, version negotiation, TLS, and
//!   the SASL session's deadline.
//! - [`sasl`] authenticates a connection: the exchange loop and the `PLAIN`, `SCRAM-SHA-256`,
//!   `SCRAM-SHA-512`, `OAUTHBEARER`, and `AWS_MSK_IAM` mechanisms, with `OAUTHBEARER`'s token
//!   endpoint, and SigV4 and the AWS credential chain for `AWS_MSK_IAM`.
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
//! | With `sasl:` set, a broker without `SaslHandshake` v1 and `SaslAuthenticate` v0 to v2 | `Refused` | every connection gets the same answer | `tests::sasl_refusals_are_refused_and_never_print_the_password` |
//! | `UNSUPPORTED_SASL_MECHANISM` (33), `ILLEGAL_SASL_STATE` (34), `UNSUPPORTED_VERSION` (35), `SASL_AUTHENTICATION_FAILED` (58), from `SaslHandshake` or `SaslAuthenticate` | `Refused` | the mechanism, the exchange, or the credentials: a retry repeats the same exchange | `tests::sasl_refusals_are_refused_and_never_print_the_password`, `sink::tests::refused_sasl_credentials_hold_the_queue_until_the_broker_takes_them` |
//! | Any other code from `SaslHandshake` or `SaslAuthenticate` | `Clean` | a state the broker leaves on its own | `fault::tests::sasl_answers_refuse_for_the_mechanism_the_state_and_the_credentials` |
//! | A SCRAM server message the client's checks reject: a server signature that doesn't match, a nonce that doesn't extend the client's, an iteration count outside 4096 to 1048576, a mandatory extension (`m=`), an `e=` error | `Refused` | a broker whose signature doesn't match doesn't hold the user's credential, and every check fails the same way on every connection | `tests::a_scram_server_signature_mismatch_is_refused`, `sasl::scram::tests::a_malformed_server_first_message_is_refused`, `sasl::scram::tests::an_iteration_count_outside_the_bounds_is_refused` |
//! | An `OAUTHBEARER` token refused with an error challenge (RFC 7628, as Kafka does), answered with a `kvsep`, then `SASL_AUTHENTICATION_FAILED` (58); or with 58 on the first message and no challenge (as Redpanda does) | `Refused`; the message carries the challenge's `status` when there is one, and a fetched token leaves the cache | the broker won't take the token; a fetched one is replaced on the next dial rather than resent | `tests::a_refused_token_is_refused_with_its_status_and_fetched_again`, `tests::a_token_refused_without_a_challenge_is_fetched_again` |
//! | An `OAUTHBEARER` token endpoint unreachable, timed out, or answering `429` or `5xx`, or a `2xx` whose body the timeout or a closed connection cuts off | `Clean` | a transient fault upstream of the broker, before any `SaslAuthenticate` | `tests::token_endpoint_faults_are_clean_when_transient_and_refused_otherwise`, `tests::a_cut_off_token_response_is_clean` |
//! | An `OAUTHBEARER` token endpoint answering any other non-`2xx` (`invalid_client`, `invalid_scope`, a redirect), or a `2xx` whose body, up to its 64 KiB cap, holds no `access_token` that is an RFC 6750 bearer token | `Refused` | the client, its secret, the scope, or the endpoint URL is wrong: every fetch gets the same answer | `tests::token_endpoint_faults_are_clean_when_transient_and_refused_otherwise`, `sasl::oauth::tests::a_token_response_without_a_usable_token_is_an_error` |
//! | `AWS_MSK_IAM` credentials the broker refuses: `SASL_AUTHENTICATION_FAILED` (58), `Access denied` | `Refused`; credentials fetched from an endpoint leave the cache | the credentials or their IAM policy are wrong; fetched ones are replaced on the next dial rather than resent | `tests::refused_aws_credentials_are_refused_and_fetched_again` |
//! | An AWS credential endpoint (STS, the container endpoint, IMDS) unreachable, timed out, or answering `429` or `5xx`, or STS's `Throttling` or `IDPCommunicationError`; IMDS unreachable with no other source configured | `Clean` | a transient fault upstream of the broker, before any `SaslAuthenticate`; a host with no IMDS can't be told from one whose IMDS is down | `tests::aws_credential_endpoint_faults_reach_the_send_with_their_class`, `sasl::aws_creds::tests::sts_faults_are_clean_when_transient_and_refused_otherwise`, `sasl::aws_creds::tests::container_endpoint_faults_are_clean_when_transient_and_refused_otherwise`, `sasl::aws_creds::tests::imds_faults_and_no_source_at_all` |
//! | An AWS credential endpoint answering any other status (`AccessDenied`, `InvalidIdentityToken`, IMDS's `404` for no role) or an answer without credentials; a source configured on this host but broken (half of a variable pair, an unreadable token file, a full container URI to a host it may not use, a profile `AWS_PROFILE` names that doesn't exist, a profile using `role_arn`, `credential_process`, or SSO); no source at all with IMDS off; no region from `region:`, the broker host, or the environment | `Refused` | the host's credentials, role, or config are wrong: every lookup gets the same answer | `tests::aws_credential_endpoint_faults_reach_the_send_with_their_class`, `tests::aws_msk_iam_reads_the_region_from_the_environment_or_refuses`, `sasl::aws_creds::tests::sts_faults_are_clean_when_transient_and_refused_otherwise`, `sasl::aws_creds::tests::half_a_configured_source_is_refused`, `sasl::aws_creds::tests::a_profile_with_an_unsupported_source_is_refused_naming_the_key`, `sasl::aws_creds::tests::imds_faults_and_no_source_at_all` |
//! | A transport failure during the SASL exchange | `Clean` | no produce byte has been written on the connection | `tests::a_transport_failure_during_the_sasl_exchange_is_clean` |
//! | A pooled connection past its SASL session's deadline (`conn::reauth_deadline`) | redialed before writing; no failure | the broker closes a connection whose session has ended on its next request | `tests::an_expiring_sasl_session_is_redialed_before_the_next_send` |
//! | The SASL deadline passing between two produce requests of one attempt to a broker | `Clean` for the partition batches not yet written; the next attempt redials | none of them was written | `tests::a_sasl_deadline_reached_between_requests_leaves_the_rest_for_a_new_connection` |
//! | The first write fails or accepts nothing, on plaintext | `Clean` | the broker can't have the request | `conn::tests::a_plaintext_first_write_that_accepts_nothing_is_clean` |
//! | A pooled connection the broker closed | redialed before writing; no failure | the probe finds the close before any byte is written | `tests::a_pooled_connection_the_broker_closed_is_redialed` |
//! | A write fails after any byte, or on TLS at all | `Ambiguous` | the broker may have the request | `conn::tests::a_failure_after_part_of_the_request_is_written_is_ambiguous`, `conn::tests::a_tls_write_failure_is_ambiguous` |
//! | A close, a timeout, or a framing error while waiting for the response | `Ambiguous` | the broker may have appended | `tests::a_close_after_the_request_is_written_is_ambiguous`, `tests::a_response_timeout_is_ambiguous`, `conn::tests::a_close_while_waiting_for_the_response_is_ambiguous`, `conn::tests::a_response_with_the_wrong_correlation_id_is_ambiguous` |
//! | A partition with no leader in metadata | `Clean`, refresh | not sent; a keyed record waits for its own partition | `tests::a_keyed_record_waits_for_its_leaderless_partition` |
//! | A broker without Produce v3, or zstd below Produce v7 | `Refused` | every batch gets the same answer | `tests::a_broker_without_produce_v3_is_refused`, `tests::zstd_needs_produce_v7` |
//! | `COORDINATOR_LOAD_IN_PROGRESS` (14), `COORDINATOR_NOT_AVAILABLE` (15), `NOT_COORDINATOR` (16), from `InitProducerId` | polled in the same attempt, from 100 ms doubling to 1 s; `Clean` once `timeout` passes | the coordinator is loading or moving, and answers on its own; a fresh broker answers the first producer 14 | `tests::a_loading_coordinator_is_polled_inside_the_attempt` |
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
mod sasl;
mod sink;
#[cfg(test)]
mod tests;

pub use encode::{EncodeStats, KafkaEncoder, RecordFormat, RecordKey, RecordMeta, RecordMode};
pub use sasl::{KafkaSasl, OAuthToken};
pub use sink::{
    KafkaAcks, KafkaCompression, KafkaOutput, KafkaSettings, TlsClientSettings,
    DEFAULT_MAX_REQUEST_BYTES, DEFAULT_TIMEOUT,
};
