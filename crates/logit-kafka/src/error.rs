//! Kafka's error codes by name, and the fault class of a SASL answer: what the broker connection
//! and `sasl` read, and what each direction's own per-code classes are written against.
//! `kafka_out`'s partition classes live in its `fault` module; this module holds only what both
//! directions share (`docs/adr/kafka-input.md`, item 1).

use kafka_protocol::ResponseError;
use logit_pipeline::Fault;

/// Kafka error codes `logit` reads by name. A code a classifier reaches through its default arm
/// is named here for the tests that pin its row.
pub mod code {
    pub const NONE: i16 = 0;
    pub const OFFSET_OUT_OF_RANGE: i16 = 1;
    pub const CORRUPT_MESSAGE: i16 = 2;
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub const LEADER_NOT_AVAILABLE: i16 = 5;
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    pub const REQUEST_TIMED_OUT: i16 = 7;
    pub const REPLICA_NOT_AVAILABLE: i16 = 9;
    pub const MESSAGE_TOO_LARGE: i16 = 10;
    pub const OFFSET_METADATA_TOO_LARGE: i16 = 12;
    pub const NETWORK_EXCEPTION: i16 = 13;
    pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
    pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const NOT_COORDINATOR: i16 = 16;
    pub const RECORD_LIST_TOO_LARGE: i16 = 18;
    pub const NOT_ENOUGH_REPLICAS: i16 = 19;
    pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
    pub const INVALID_REQUIRED_ACKS: i16 = 21;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const INVALID_SESSION_TIMEOUT: i16 = 26;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
    pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
    pub const INVALID_TIMESTAMP: i16 = 32;
    pub const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
    pub const ILLEGAL_SASL_STATE: i16 = 34;
    pub const UNSUPPORTED_VERSION: i16 = 35;
    pub const POLICY_VIOLATION: i16 = 44;
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    pub const INVALID_PRODUCER_EPOCH: i16 = 47;
    pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
    pub const KAFKA_STORAGE_ERROR: i16 = 56;
    pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
    pub const UNKNOWN_PRODUCER_ID: i16 = 59;
    pub const FENCED_LEADER_EPOCH: i16 = 74;
    pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
    pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
    pub const OFFSET_NOT_AVAILABLE: i16 = 78;
    pub const MEMBER_ID_REQUIRED: i16 = 79;
    pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
    pub const FENCED_INSTANCE_ID: i16 = 82;
    pub const INVALID_RECORD: i16 = 87;
    pub const UNSTABLE_OFFSET_COMMIT: i16 = 88;
    pub const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
    pub const UNKNOWN_TOPIC_ID: i16 = 100;
    pub const INCONSISTENT_TOPIC_ID: i16 = 103;
}

/// The class of a non-zero error code in a `SaslHandshake` or `SaslAuthenticate` answer. A
/// mechanism the broker doesn't offer, an exchange its state machine refuses, and credentials it
/// won't take each get the same answer on every connection, so they hold the queue; anything
/// else is a state the broker leaves on its own, as `kafka_out`'s `fault::classify_control` reads a
/// `Metadata` or `InitProducerId` answer.
pub fn classify_sasl(error_code: i16) -> Fault {
    use code::*;
    match error_code {
        UNSUPPORTED_SASL_MECHANISM
        | ILLEGAL_SASL_STATE
        | SASL_AUTHENTICATION_FAILED
        | UNSUPPORTED_VERSION => Fault::Refused,
        _ => Fault::Clean,
    }
}

/// `NAME (code)` for a diagnostic, from `kafka-protocol`'s table.
pub fn name(error_code: i16) -> String {
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

    #[test]
    fn sasl_answers_refuse_for_the_mechanism_the_state_and_the_credentials() {
        for code in [UNSUPPORTED_SASL_MECHANISM, ILLEGAL_SASL_STATE, SASL_AUTHENTICATION_FAILED, 35]
        {
            assert_eq!(classify_sasl(code), Fault::Refused, "{}", name(code));
        }
        for code in [15, 7, 999] {
            assert_eq!(classify_sasl(code), Fault::Clean, "{}", name(code));
        }
    }

    #[test]
    fn names_come_from_the_protocol_table() {
        assert_eq!(name(OUT_OF_ORDER_SEQUENCE_NUMBER), "OutOfOrderSequenceNumber (45)");
        assert_eq!(name(999), "unknown error code 999");
    }
}
