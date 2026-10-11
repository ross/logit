//! What one partition's error code in a `Produce` response means for its partition batch. The
//! table this implements is [`super`]'s "Response classes"; each row there names its test.

use kafka_protocol::ResponseError;
use logit_pipeline::Fault;

/// Kafka error codes `kafka_out` reads by name.
pub(crate) mod code {
    pub(crate) const NONE: i16 = 0;
    pub(crate) const CORRUPT_MESSAGE: i16 = 2;
    pub(crate) const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub(crate) const LEADER_NOT_AVAILABLE: i16 = 5;
    pub(crate) const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    pub(crate) const REQUEST_TIMED_OUT: i16 = 7;
    pub(crate) const MESSAGE_TOO_LARGE: i16 = 10;
    pub(crate) const NETWORK_EXCEPTION: i16 = 13;
    pub(crate) const RECORD_LIST_TOO_LARGE: i16 = 18;
    pub(crate) const NOT_ENOUGH_REPLICAS: i16 = 19;
    pub(crate) const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
    pub(crate) const INVALID_REQUIRED_ACKS: i16 = 21;
    pub(crate) const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    pub(crate) const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
    pub(crate) const INVALID_TIMESTAMP: i16 = 32;
    pub(crate) const UNSUPPORTED_VERSION: i16 = 35;
    pub(crate) const POLICY_VIOLATION: i16 = 44;
    pub(crate) const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub(crate) const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    pub(crate) const INVALID_PRODUCER_EPOCH: i16 = 47;
    pub(crate) const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
    pub(crate) const KAFKA_STORAGE_ERROR: i16 = 56;
    pub(crate) const UNKNOWN_PRODUCER_ID: i16 = 59;
    pub(crate) const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
    pub(crate) const INVALID_RECORD: i16 = 87;
    pub(crate) const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
    pub(crate) const UNKNOWN_TOPIC_ID: i16 = 100;
    pub(crate) const INCONSISTENT_TOPIC_ID: i16 = 103;
}

/// What a partition's error code asks of the producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Response {
    /// Appended: `NONE`, or `DUPLICATE_SEQUENCE_NUMBER` from an older broker. Settles the
    /// partition batch.
    Accepted,
    /// Left unsettled under `fault`, and resent on the next attempt with its sequence. `refresh`
    /// asks for a metadata refresh before that attempt.
    Unsettled { fault: Fault, refresh: bool },
    /// Settles the partition batch as dropped, which leaves a sequence gap.
    Rejected,
    /// `MESSAGE_TOO_LARGE`: halve a batch of more than one record that isn't already a resend;
    /// otherwise [`Response::Rejected`].
    TooLarge,
    /// `INVALID_TIMESTAMP` or `INVALID_RECORD`: drop the records `record_errors` names and resend
    /// the rest once; with no record named, or on a resend, [`Response::Rejected`].
    NamedRecords,
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER`: `Ambiguous`, and the epoch is bumped before the next send.
    OutOfOrder,
    /// `INVALID_PRODUCER_EPOCH`: `Clean`, and the producer is re-initialized with a new PID
    /// before the next send.
    StaleEpoch,
}

/// The [`Response`] for a partition's `error_code`.
pub(crate) fn classify(error_code: i16) -> Response {
    use code::*;
    let unsettled = |fault, refresh| Response::Unsettled { fault, refresh };
    match error_code {
        NONE | DUPLICATE_SEQUENCE_NUMBER => Response::Accepted,
        UNKNOWN_TOPIC_OR_PARTITION | UNKNOWN_TOPIC_ID => unsettled(Fault::Refused, true),
        LEADER_NOT_AVAILABLE
        | NOT_LEADER_OR_FOLLOWER
        | KAFKA_STORAGE_ERROR
        | INCONSISTENT_TOPIC_ID => unsettled(Fault::Clean, true),
        NOT_ENOUGH_REPLICAS | UNKNOWN_PRODUCER_ID | THROTTLING_QUOTA_EXCEEDED => {
            unsettled(Fault::Clean, false)
        }
        REQUEST_TIMED_OUT | NETWORK_EXCEPTION | NOT_ENOUGH_REPLICAS_AFTER_APPEND => {
            unsettled(Fault::Ambiguous, false)
        }
        INVALID_REQUIRED_ACKS
        | TOPIC_AUTHORIZATION_FAILED
        | CLUSTER_AUTHORIZATION_FAILED
        | UNSUPPORTED_VERSION
        | UNSUPPORTED_COMPRESSION_TYPE => unsettled(Fault::Refused, false),
        MESSAGE_TOO_LARGE => Response::TooLarge,
        INVALID_TIMESTAMP | INVALID_RECORD => Response::NamedRecords,
        OUT_OF_ORDER_SEQUENCE_NUMBER => Response::OutOfOrder,
        INVALID_PRODUCER_EPOCH => Response::StaleEpoch,
        // CORRUPT_MESSAGE, RECORD_LIST_TOO_LARGE, POLICY_VIOLATION, and any code without a row:
        // the runtime's default for an answer a sink can't attribute.
        _ => Response::Rejected,
    }
}

/// The class of a non-zero error code in a `Metadata` or `InitProducerId` answer, which settles
/// no partition: an authorization or version refusal holds the queue, and anything else is a
/// state the broker leaves on its own.
pub(crate) fn classify_control(error_code: i16) -> Fault {
    use code::*;
    match error_code {
        UNKNOWN_TOPIC_OR_PARTITION
        | TOPIC_AUTHORIZATION_FAILED
        | CLUSTER_AUTHORIZATION_FAILED
        | TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        | UNSUPPORTED_VERSION => Fault::Refused,
        _ => Fault::Clean,
    }
}

/// `NAME (code)` for a diagnostic, from `kafka-protocol`'s table.
pub(crate) fn name(error_code: i16) -> String {
    match ResponseError::try_from_code(error_code) {
        None => format!("NONE ({error_code})"),
        Some(ResponseError::Unknown(_)) => format!("unknown error code {error_code}"),
        Some(err) => format!("{err} ({error_code})"),
    }
}

#[cfg(test)]
mod tests {
    use super::code::*;
    use super::*;

    /// The classification of every row of the module doc's table, plus a code with no row.
    /// Each row's behavior through the producer is pinned again against the fake broker in
    /// `super::tests`.
    #[test]
    fn every_error_code_row_classifies_as_its_table_says() {
        let clean = |refresh| Response::Unsettled { fault: Fault::Clean, refresh };
        let ambiguous = Response::Unsettled { fault: Fault::Ambiguous, refresh: false };
        let refused = |refresh| Response::Unsettled { fault: Fault::Refused, refresh };
        let rows = [
            (NONE, Response::Accepted),
            (CORRUPT_MESSAGE, Response::Rejected),
            (UNKNOWN_TOPIC_OR_PARTITION, refused(true)),
            (LEADER_NOT_AVAILABLE, clean(true)),
            (NOT_LEADER_OR_FOLLOWER, clean(true)),
            (REQUEST_TIMED_OUT, ambiguous),
            (MESSAGE_TOO_LARGE, Response::TooLarge),
            (NETWORK_EXCEPTION, ambiguous),
            (RECORD_LIST_TOO_LARGE, Response::Rejected),
            (NOT_ENOUGH_REPLICAS, clean(false)),
            (NOT_ENOUGH_REPLICAS_AFTER_APPEND, ambiguous),
            (INVALID_REQUIRED_ACKS, refused(false)),
            (TOPIC_AUTHORIZATION_FAILED, refused(false)),
            (CLUSTER_AUTHORIZATION_FAILED, refused(false)),
            (INVALID_TIMESTAMP, Response::NamedRecords),
            (UNSUPPORTED_VERSION, refused(false)),
            (POLICY_VIOLATION, Response::Rejected),
            (OUT_OF_ORDER_SEQUENCE_NUMBER, Response::OutOfOrder),
            (DUPLICATE_SEQUENCE_NUMBER, Response::Accepted),
            (INVALID_PRODUCER_EPOCH, Response::StaleEpoch),
            (KAFKA_STORAGE_ERROR, clean(true)),
            (UNKNOWN_PRODUCER_ID, clean(false)),
            (UNSUPPORTED_COMPRESSION_TYPE, refused(false)),
            (INVALID_RECORD, Response::NamedRecords),
            (THROTTLING_QUOTA_EXCEEDED, clean(false)),
            (UNKNOWN_TOPIC_ID, refused(true)),
            (INCONSISTENT_TOPIC_ID, clean(true)),
            // GROUP_AUTHORIZATION_FAILED: a real code with no row.
            (30, Response::Rejected),
            (-1, Response::Rejected),
            (999, Response::Rejected),
        ];
        for (code, want) in rows {
            assert_eq!(classify(code), want, "{}", name(code));
        }
    }

    #[test]
    fn names_come_from_the_protocol_table() {
        assert_eq!(name(OUT_OF_ORDER_SEQUENCE_NUMBER), "OutOfOrderSequenceNumber (45)");
        assert_eq!(name(999), "unknown error code 999");
    }

    #[test]
    fn control_answers_refuse_only_for_authorization_versions_and_a_missing_topic() {
        for code in [3, 29, 31, 53, 35] {
            assert_eq!(classify_control(code), Fault::Refused, "{}", name(code));
        }
        for code in [5, 15, 51, 7] {
            assert_eq!(classify_control(code), Fault::Clean, "{}", name(code));
        }
    }
}
