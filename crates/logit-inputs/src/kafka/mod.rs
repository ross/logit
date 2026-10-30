//! `kafka_in`: consumes Kafka topics as a consumer-group member and sends their records
//! downstream ([ADR `kafka-input`](../../../../docs/adr/kafka-input.md)). The group, fetch, and
//! commit protocol is `logit_kafka::consumer`'s; this module decodes records ([`decode`]) and
//! runs the delivery loop and the revoke and shutdown sequence ([`input`]).
//!
//! # The commit rule
//!
//! A partition's offset is committed only once every record before it is in a batch
//! `Fanout::send_resumable` accepted into every open downstream inbox, never on a sink's
//! delivery. Each `Fetched` is decoded whole, cut into batches in offset order at
//! `receive.batch_max_events` and `batch_max_bytes` (record value bytes; a record is never
//! split), and each cut's offset range goes to `Consumer::mark_accepted` once all of its batches
//! are accepted. A record that needs no delivery (skipped or dropped below) rides in the cut it
//! falls in. A dropped record is committed past, the third exception to
//! [ADR `delivery-semantics`](../../../../docs/adr/delivery-semantics.md) item 3.
//!
//! While no revoke is pending, a batch that finds no room is offered again with no end, in
//! 250 ms attempts between which the loop reads the revoke and shutdown signals. Its
//! `Reservation` keeps its place in line on the inbox it waits for, so a fan-in peer that queued
//! after it never takes a freed slot first. Once a revoke is pending, the batch in flight has
//! `drain_timeout`. Shutdown caps that deadline, whether or not a revoke's is running, at the
//! lesser of `drain_timeout` and half of `receive.shutdown_grace` from when it's seen, leaving
//! the other half for the commit and the LeaveGroup.
//!
//! # Outcomes
//!
//! The broker's outcomes, and the records the client itself skips or drops (`corrupt`,
//! `oversize`, `legacy_format`, `retention`, `aborted`), are the table in
//! `logit_kafka::consumer`'s module doc. This table is the canonical copy of the input's own;
//! each row names its test in `tests`.
//!
//! | Outcome | Counted | Committed | Test |
//! |---|---|---|---|
//! | A record with a null value (a tombstone) | `logit.input.records.skipped{reason="tombstone"}` | yes | `a_tombstone_an_empty_value_and_an_undecodable_one_are_counted_and_committed_past` |
//! | A `json`, `msgpack`, or `native` value, or an OTLP request, that decodes to no event | `logit.input.records.skipped{reason="empty"}` | yes | same, and `an_otlp_request_with_no_records_is_skipped_empty` |
//! | A value that doesn't decode in `format:`, an OTLP request of another signal than its topic's included (an `otlp_json` one decodes to no event, since OTLP/JSON ignores a field it doesn't know, and is found by its top-level `resourceLogs`, `resourceMetrics`, or `resourceSpans` key) | `logit.input.records.dropped{reason="undecodable"}`, diagnostic `records_dropped` naming topic, partition, and offset | yes | same, `a_msgpack_value_that_does_not_decode_is_dropped_and_committed_past`, `an_otlp_request_on_another_signals_topic_is_undecodable`, and `an_otlp_json_request_on_another_signals_topic_is_undecodable` |
//! | A header whose name isn't UTF-8 | `logit.input.headers.omitted{reason="invalid_name"}`; the record is delivered without it | with the record | `a_header_name_that_is_not_utf8_is_omitted_and_counted` |
//! | A batch not accepted by the drain deadline of a revoke or shutdown | `logit.input.batches.deferred`, with every batch behind it | no | `a_revoke_whose_drain_deadline_passes_defers_and_commits_nothing_past_it` |
//! | A batch no consumer took | `logit.input.batches.dropped{reason="closed_consumer"}`, the batches behind it `batches.deferred`, diagnostic `closed_consumer`; the input commits, leaves the group, and ends | no | `a_closed_consumer_stops_commits_the_batch_before_and_leaves` |
//!
//! A `text` value is never `empty`: an empty value is a log with an empty message, as
//! `kafka_out` writes one.
//!
//! # Formats and carriers
//!
//! `json` reads `logit_proto::event_json`'s objects, one or several as lines; `msgpack` the same
//! objects as maps, one or several back to back, through `logit_proto::msgpack_event` under its
//! default bounds; `native` one `CODEC_BATCH` frame filling the value, under the default
//! per-frame decode budget; `text` the value as one log, or under `record: batch` one log per
//! line through `logit_proto::kafka::text::unescape_lines`, with the record's timestamp (the
//! read time when it has none), no severity, the `raw` body format, and an empty resource.
//!
//! `otlp_proto` and `otlp_json` read one export request per value, through
//! `logit_proto::otlp::OtlpDecoder`'s `decode_signal` or `decode_signal_json` for the signal
//! the record's topic maps to in `topics:`. A request is one batch per resource and scope, and a
//! record's batches all fall in one cut, so the record is marked accepted only after its last
//! batch is; a batch deferred at a revoke leaves the whole record uncommitted, to be read again.
//! The mapping's own skips count `logit.input.metrics.skipped` as on `otlp_in`. No size cap
//! applies beyond `max_batch_bytes`: `otlp_in`'s 4 MiB is a cap on an HTTP body it reads.
//!
//! Every event of a record carries the record's metadata as private attributes:
//!
//! | Attribute | Value |
//! |---|---|
//! | `_logit.kafka.topic` | `Str` |
//! | `_logit.kafka.partition` | `I64` |
//! | `_logit.kafka.offset` | `I64` |
//! | `_logit.kafka.key` | `Str` when UTF-8, else `Bytes`; absent for a null key, `Str("")` for an empty one |
//! | `_logit.kafka.headers` | `Map` of name to value: `Str` when UTF-8, else `Bytes`, `Null` for a null value; a repeated name holds an `Array` of its values in wire order |
//! | `_logit.kafka.timestamp` | `Timestamp`, the record's milliseconds; absent for `-1` |
//! | `_logit.kafka.timestamp_type` | `Str`, `create_time` or `log_append_time` |
//!
//! `kafka_out` leaves them out of what it writes, its own family's carriers
//! ([ADR `protocol-knowledge-stays-in-family`](../../../../docs/adr/protocol-knowledge-stays-in-family.md)).

mod decode;
mod input;

#[cfg(test)]
mod tests;

pub use decode::KafkaInFormat;
pub use input::{KafkaInSettings, KafkaInput};
