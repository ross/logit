//! What one partition's error code in a `Produce` response means for its partition batch. The
//! table this implements is [`super`]'s "Response classes"; each row there names its test.

use logit_pipeline::Fault;

pub(crate) use logit_kafka::error::{code, name};

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
    /// `UNKNOWN_TOPIC_OR_PARTITION` or `UNKNOWN_TOPIC_ID`: `Clean` with a metadata refresh the
    /// first time, `Refused` once it persists after a completed refresh
    /// (`super::producer::MissingTopic`).
    MissingTopic,
}

/// The [`Response`] for a partition's `error_code`.
pub(crate) fn classify(error_code: i16) -> Response {
    use code::*;
    let unsettled = |fault, refresh| Response::Unsettled { fault, refresh };
    match error_code {
        NONE | DUPLICATE_SEQUENCE_NUMBER => Response::Accepted,
        UNKNOWN_TOPIC_OR_PARTITION | UNKNOWN_TOPIC_ID => Response::MissingTopic,
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
/// state the broker leaves on its own. A missing topic is the producer's to classify
/// (`super::producer::MissingTopic`).
pub(crate) fn classify_control(error_code: i16) -> Fault {
    use code::*;
    match error_code {
        TOPIC_AUTHORIZATION_FAILED
        | CLUSTER_AUTHORIZATION_FAILED
        | TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        | UNSUPPORTED_VERSION => Fault::Refused,
        _ => Fault::Clean,
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
            (UNKNOWN_TOPIC_OR_PARTITION, Response::MissingTopic),
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
            (UNKNOWN_TOPIC_ID, Response::MissingTopic),
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
    fn control_answers_refuse_only_for_authorization_and_versions() {
        for code in [29, 31, 53, 35] {
            assert_eq!(classify_control(code), Fault::Refused, "{}", name(code));
        }
        for code in [5, 15, 51, 7] {
            assert_eq!(classify_control(code), Fault::Clean, "{}", name(code));
        }
    }
}
