//! The class check: whether what a flow's sink did with each record a case put in front of it is
//! what `spec/transit/mapping.yaml` says it does (`super::mapping`'s module doc has the table's
//! schema; `spec/transit/README.md`'s "The class check" is the reader's account).
//!
//! It compares the records that reached the sink (after every stage) with the records the sink's
//! pair decoder read back, both as `format: json` objects, so a type tag, a carrier, and a
//! resource attribute are all visible.
//!
//! **Which rows a record reaches.** A metric record reaches its kind's rows: for a `sum`, its
//! temporality and monotonicity and its scalar type; for a `gauge` or `gauge_delta`, its scalar
//! type, `negative` for a gauge below zero, and `prometheus_info` or `prometheus_stateset` for a
//! gauge whose `_logit.metrics.type`, the event's else the resource's, is `info` or `stateset`;
//! for a `histogram`, `exponential_histogram`, or `summary`, its temporality, and for a
//! `histogram` `nan_bound` and `neg_inf_bound` for each such bound it holds and `gauge_histogram`
//! when its `_logit.metrics.type` is `gaugehistogram`; any other kind, its kind's one row. It also reaches a `record` row for each of
//! `unit` (`unit_on_distribution` for a `samples` or `distribution`), `description`,
//! `start_timestamp`, `exemplars` (`exemplars_on_summary` for a `summary`), and `no_recorded_value` (`no_recorded_value_delta_sum` for a delta `sum`,
//! `no_recorded_value_other_kind` for a kind other than `sum` and `gauge`) it carries,
//! `service_check` when it carries `_logit.statsd.service_check.name`, `name_unsafe` when the
//! sink's wire rewrites a character of its name ([`Spelling`]), `exemplar_key_unsafe` for a
//! monotonic `sum` (`exemplar_key_unsafe_histogram` for a `histogram`) when an exemplar
//! `prometheus_out` writes carries an attribute key it would rewrite as a label name, `sample_rate` for a
//! `samples`
//! record whose rate rounds to a weight above 1, `non_finite` for a `sum` or
//! `gauge` holding `NaN` or an infinity, and `superseded` when a later record of the run is
//! another point of its series (the same name, kind, and public attributes), with `same_instant`
//! when that point shares its timestamp or else `same_millisecond` when it shares its
//! millisecond, each as `same_instant_equal` or `same_millisecond_equal` when the record is a
//! state ([`is_state`]) and the latest point of the series in its millisecond, the later in the
//! run on a tie, is the same state as the sink writes it ([`same_reading`]); at `datadog_out`, a `samples`, `distribution`, `set_members`, or
//! `exponential_histogram` record reaches neither, nor a `histogram` under `histograms.mode:
//! distributions`, since that sink collapses a series' points only on its series route, and the
//! distribution-points and sketches routes carry those kinds or nothing does; under
//! `histograms.send_aggregation_metrics` the two histogram kinds reach both, since their
//! aggregation series go on the series route. A `histogram` or `exponential_histogram` reaches the field row `record/min_max`
//! for its `min` and `max`, which its own comparison leaves out, as a flagged point's leaves out
//! its value: a stale marker reads back as `NaN`, and a `samples` record below rate 1 that rounds
//! to weight 1 the field row `record/sample_rate_unweighted` for its rate, left out the same way.
//! It reaches an
//! `attributes` row for each attribute, event or resource: a `_logit.<protocol>.` carrier of the
//! sink's own protocol is `private_carrier`, `_logit.prometheus.target` `prometheus_target` and
//! `_logit.metrics.closed: "timestamped"` `timestamped` at every sink, on a metric record
//! `_logit.metrics.type: "rate"` `rate` on a `gauge` and `rate_on_non_gauge` on any other kind,
//! `_logit.metrics.type: "info"` or `"stateset"` on a `gauge` and `"gaugehistogram"` on a
//! `histogram` `family_type`, and any of the three on another kind `family_type_on_other_kind`,
//! and `_logit.metrics.interval` `interval_on_distribution` on a `samples`, `distribution`, or
//! delta `exponential_histogram`, `interval_on_histogram` on a delta `histogram`, and `interval`
//! on any other kind, at every sink, a `_logit.datadog.` carrier on a log, a Datadog
//! event, or a service check `datadog_carrier_log`, `_logit.syslog.severity`
//! `severity_carrier_syslog`, any other `_logit.` key `foreign_carrier`, on a log an event
//! attribute `syslog_out` writes as a header field by its key (`syslog.facility`, `syslog.tag`,
//! `syslog.pid`, and `process.pid` `syslog_header`, `syslog.msgid` `syslog_msgid`, `syslog.sd`
//! `syslog_sd`, `syslog.timestamp` `syslog_timestamp`, and `host.name` `host_name`), an event
//! attribute by its value (`string_value`, `bool_value`, `number_value`, `array_value`,
//! `map_value`, `bytes_value`, `timestamp_value`), and any other resource attribute
//! `resource_attribute`; and `attributes/scope` when its batch has a scope. A log
//! record on an event with no metrics reaches `log/dogstatsd_event` when it carries
//! `_logit.statsd.event.title`, else `log/plain`, the message rows [`log_features`] lists, and its
//! attribute rows, with `_logit.otel.severity_*` as `severity_carrier_otel`; it also reaches a `log`
//! field row for each of its fields (`severity`, `body_format`, `event_name`,
//! `observed_timestamp`, `trace` for its trace and span ids, `trace_flags`,
//! `dropped_attributes_count`), or the field's `_unset` row (`severity_unset`,
//! `observed_timestamp_unset`, `trace_unset`) where it has none, and `log/timestamp`, or
//! `log/receipt_timestamp` when it carries `syslog.timestamp` and its timestamp is when it was
//! received. Those fields are out of a log's own comparison ([`LOG_FIELD_KEYS`]), so a log pairs
//! on its message, its message's type tags, and its attributes. A span there reaches
//! `span/alone`, and also `span/no_wire_form` when it carries a field a Datadog span has no form
//! for. An event with no metric, log, or span is a record of kind `event` that reaches
//! `event/string_only` when it carries a non-empty `_logit.influxdb.measurement`, else
//! `event/attributes_only`, and its attribute rows. A record reaches an `attributes` field row for
//! each attribute [`derived_rows`] lists, at every sink that compares the record; a sink that writes
//! none reads it `exact` when nothing stands in its place. A log or span on an event that carries
//! metrics is a field of that event: it reaches `log/beside_metrics` or `span/beside_metrics`,
//! read as `exact` when it pairs unchanged, `normalized` when it pairs changed, and `lost` when
//! nothing read back pairs with it. A log there also reaches the `log` field rows a log alone
//! does, so `log/beside_metrics` says only whether the sink carried it; its attributes are its
//! event's, judged on the event's metric records.
//!
//! **Pairing.** Each input record is paired with the records read back that account for it, in
//! passes, each claiming a read-back record at most once: (1) the same name and the same kind,
//! value, and type tags (`exact`); (2) the same name, kind, temporality, and monotonicity and the
//! same value read as a number, or for `samples` and `set_members` several read-back records whose
//! values together are the input's (`normalized`); a histogram that gained an empty `+Inf`
//! bucket is the same value; (3) once neither found a match, the same name in any metric kind,
//! the input's own included, or names under `<name>.`, the dotted series of an expansion, one of
//! each dotted name per record when several points of a series expand (`degraded`). Where both
//! the input and the read-back record are histograms (or exponential histograms), pass (3) also
//! needs the same total count and, where both carry a sum, the same sum, a non-finite sum
//! matching only the same non-finite value. Merged or dropped bounds, an added overflow bucket,
//! and a folded `NaN` or `-Inf` bound change no total, so they pair as `degraded`, and where each
//! count lands is the snapshot's to pin; a sink that loses or invents observations leaves the
//! input unread and the read-back record unclaimed. Passes (1) and (2) run by name and then by
//! the name as the sink writes it, where an `exact` pairing reads as `normalized`, before pass
//! (3) runs either way, so a dotted expansion never claims a record a respelling reads back
//! whole. Within a pass the latest record goes first, so where a sink keeps one point per series,
//! the earlier point is the one left over; among logs, one with the input's timestamp goes first,
//! so logs with one message pair by time.
//!
//! [`Spelling`] holds each sink's rewrites, from its codec's module doc: `statsd_out`'s reserved
//! characters as `_` in a name, a tag key, and a tag value (`:` allowed there), and under `format:
//! dogstatsd` a valued `host` attribute on a line with no host of its own read back as `host.name`,
//! or, empty, as the hostless mark ([`statsd`]); `prometheus_out`'s `sanitize_metric_name`,
//! `_total` on a counter, a label key's `sanitize_label_name`, and an array's last element;
//! `influxdb_out`'s `<name>.value` for a `sum`, `gauge`, or `set` off the carrier path, and an
//! array's last element; `otlp_out`'s timestamp value as an integer of nanoseconds, a `u64` above
//! `i64::MAX` as a double, and an
//! OpenMetrics `info` or `stateset` gauge read back as a cumulative non-monotonic sum, an `info`
//! one under `<name>_info`; `datadog_out`'s hostless mark read back as a v2 `host` resource with no
//! name, a log's message as its text ([`Spelling::log_message`]: a structured message as JSON,
//! bytes as lossy UTF-8), a bytes attribute on a log as base64, and the attributes it writes from a
//! record's own fields ([`Spelling::derived`]: the run's hostname on a record with no host, a log's
//! `status`, `trace_id`, and `span_id`, a span's `span.kind` and `_dd.p.tid`), each accepted only
//! with the value derived for it; a log's `trace_id` and `span_id` pair only and have no row,
//! since `log/trace` judges them; `graphite_out`'s forbidden characters as `_` in a
//! path, a tag name, and a tag value (and a tag value's leading `~`), a tag value in the encoder's
//! rendering, and the values it omits as no tag ([`graphite_value`]). Each name, key, and value set
//! is the encoder's own predicate. `syslog_out`'s rewrites are a log's message as `syslog_in`
//! reads it back ([`Spelling::log_message`]: the encoder's rendering, and under RFC 3164 with any
//! header token the decoder reads as MSG), a header attribute in the encoder's rendering
//! ([`syslog::value_for`]), a PROCID read back as `process.pid` when canonical decimal and as
//! `syslog.pid` otherwise, and the attributes it fills in ([`Spelling::derived`]: its configured
//! facility on a log with no `syslog.facility`, the run's hostname on a log with no host, and for
//! pairing only, `syslog.timestamp` written from the log's timestamp and under RFC 3164 a TAG read
//! from the message's first word). A candidate's public attributes (no `_logit.`
//! key, values compared as text, each key and value as the sink writes it) must equal the
//! input's, or failing any that do, be a subset of them; a `datadog_out` series under `<name>.`
//! may also carry the `lower_bound`, `upper_bound`, or `quantile` tag of the bucket or quantile it
//! expands. An input left unpaired was skipped or
//! dropped: the class is the one whose counter moved, or the cell's when it says the loss is
//! `uncounted`, or the expected class when it reached a `normalized` cell whose counter moved for
//! an equal point that was read back: a sink that collapses two equal readings sends one of them. One exception: under `prometheus_out` exposition (`bind:` set), an unpaired
//! record whose series has a later record in the case that was read back was overwritten in the
//! registry, and reads at its expected class when that's `normalized` or `degraded`. A read-back
//! record no input claims fails the check with both sides printed.
//!
//! The check compares a timestamp in one place: a `same_instant` record paired `exact` whose
//! read-back instant differs from its twin's reads `normalized`, since the sink moved them apart.
//!
//! **Record level.** The expected class is the worst (`exact < normalized < degraded < skipped
//! < dropped`) over the record-level cells it reaches, and the observed class must equal it; a
//! `normalized` expectation also accepts `exact`, since a normalization rewrites only the shapes
//! it names.
//!
//! **Field level.** For a record observed `exact` or `normalized`, each field row compares the
//! input's field with its first pairing's: the same value in the same place is `exact`, present
//! but changed, moved (a resource attribute read back as an event attribute), or under the
//! sink's spelling of its key is `normalized`,
//! and absent is `lost`. A log's fields follow the same rule, with three differences: a body
//! format read back as another is `lost`, since the format names how to read the message; an
//! `_unset` row is `exact` when the field reads back absent and `normalized` when the sink fills
//! it in; and a receipt timestamp read back outside its own second is `lost`, another instant
//! that no precision explains. At `syslog_out`, `syslog_in` stamps its own receipt time, so
//! `log/timestamp` compares the TIMESTAMP written, read back as `syslog.timestamp`: the input's
//! RFC 3339 text is `exact`, another rendering of its instant (microseconds, or RFC 3164's whole
//! seconds with no year) `normalized`, and a TIMESTAMP written from the log's own
//! `syslog.timestamp` leaves it `lost`; a `syslog.timestamp` the encoder can't render under the
//! sink's format reads `lost`. `prometheus_out` has one exception, which compares a value:
//! `_logit.prometheus.target` absent reads `normalized` when the read-back's `instance` equals the
//! input's, and `lost` otherwise. The observed class may be better than the cell's but never worse. A
//! degraded, skipped, or dropped record's field rows aren't looked up, so they're neither reached
//! nor unspecified there: the carriers that describe its wire form describe a shape that's gone.
//!
//! **Counters.** Every sink series a run moved whose name ends in `dropped`, `skipped`,
//! `degraded`, `normalized`, `omitted`, or `evicted` must be named by a cell the case reached. A
//! `degraded`, `skipped`, `dropped`, or `lost` cell observed at its class must see its counter
//! move. A `normalized` cell's counter needn't, since not every record it covers is rewritten.
//! Where a record reaches several cells at its worst class, the one whose counter moved decides:
//! a sink counts a record it skips once, under whichever reason it meets first (`statsd_out`
//! skips a flagged histogram as `no_recorded_value` before `multi_value: skip` sees its kind).
//! For a record flagged as holding no recorded value, its `record/no_recorded_value*` cell goes
//! ahead of a kind row's counted `degraded`, `skipped`, or `dropped` cell whose counter didn't
//! move: `statsd_out`, `influxdb_out`, `prometheus_out`, and `graphite_out` test the
//! flag before the kind, so a flagged `gauge_delta` is skipped there rather than dropped, and
//! remote-write writes a flagged delta `sum` as a stale marker rather than dropping it. A sink
//! that tests the kind first moves the kind cell's counter, and that cell decides as before
//! (`otlp_out` drops a flagged `gauge_delta` under `unresolved_gauge_delta`). A case doesn't mix
//! flagged and unflagged records of one kind: counters are flow-wide, so an unflagged record's
//! loss would move the counter a flagged one's kind cell names, and the check would misread the
//! flagged one. Telling the two apart would take a table of which sinks test the flag first.
//!
//! A `when:` key that names no field of the sink's serialized config fails, naming the row, the
//! cell, and the key: a misspelled key would otherwise never match.
//!
//! **Across every flow** ([`Ledger`]), every `normalized` or `lost` cell some record reached must
//! be observed at its class at least once, so a bound can't hide an improvement forever, and
//! `coverage.md` lists every cell no record was checked against. Each flow's test writes what its
//! runs fed the ledger as a [`Fragment`], and the spec's `ledger` test merges the committed
//! fragments to run these checks (`super::spec`'s module doc).

use super::mapping::{self, Class, Lookup, Mapping, Series};
use super::Traced;
use logit_core::attrs::{
    is_field, CLOSED_TIMESTAMPED, METRICS_CLOSED, METRICS_INTERVAL, METRICS_TYPE,
    TYPE_GAUGE_HISTOGRAM, TYPE_INFO, TYPE_RATE, TYPE_STATESET,
};
use logit_core::EventBatch;
use logit_outputs::ndjson::render_ndjson;
use logit_outputs::statsd::is_forbidden_in_name;
use logit_proto::event_json::EventJsonDecoder;
use logit_proto::graphite::encode::{
    is_forbidden_in_path, is_forbidden_in_tag_name, render_tag_value, sanitize_tag_value_into,
};
use logit_proto::influxdb::ATTR_MEASUREMENT;
use logit_proto::prometheus::text::exposed_exemplar_keys;
use logit_proto::prometheus::{sanitize_label_name, sanitize_metric_name, ATTR_TARGET};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Body keys checked by `record` field rows, not by the record's own comparison.
const FIELD_KEYS: [&str; 6] =
    ["unit", "description", "start_timestamp", "exemplars", "flags", "no_recorded_value"];

/// Log body keys checked by `log` field rows, not by the record's own comparison, so a log's core
/// is its message and the message's type tags.
const LOG_FIELD_KEYS: [&str; 8] = [
    "severity",
    "format",
    "event_name",
    "observed_timestamp",
    "trace_id",
    "span_id",
    "trace_flags",
    "dropped_attributes_count",
];

/// Series name suffixes a run's sink counters are checked for.
const OUTCOMES: [&str; 6] = ["dropped", "skipped", "degraded", "normalized", "omitted", "evicted"];

/// One metric, log, or span record, as `format: json` writes it.
#[derive(Debug, Clone)]
struct Rec {
    /// The metric kind, `log`, `span`, or `event` for an event that carries none of them.
    kind: String,
    /// The metric name, the log's message, or the span's name; empty for an `event`.
    name: String,
    /// The metric object, or the `log` or `span` object; an empty object for an `event`.
    body: Value,
    /// Whether the record's event also carries metrics: a log or span beside them is a field of
    /// that event, not a record of its own.
    beside_metrics: bool,
    /// The line's type tags under the record, keyed by the pointer below it (`/value`).
    types: BTreeMap<String, String>,
    attributes: Map<String, Value>,
    /// Each attribute key's type tags, `pointer=tag` joined, empty when it has none.
    attribute_types: BTreeMap<String, String>,
    resource: Map<String, Value>,
    resource_types: BTreeMap<String, String>,
    /// The batch's scope object, when it has one.
    scope: Option<Value>,
    /// The event's RFC 3339 timestamp.
    timestamp: String,
    /// Whether this span is the one of its trace, in its batch, that `datadog_out` gives a
    /// synthesized `_dd.p.tid` ([`mark_tid_due`]).
    tid_due: bool,
}

impl Rec {
    fn is_metric(&self) -> bool {
        !matches!(self.kind.as_str(), "log" | "span" | "event")
    }

    /// `metric`, `log`, `span`, or `event`: only records of one family pair.
    fn family(&self) -> &str {
        if self.is_metric() {
            "metric"
        } else {
            &self.kind
        }
    }

    /// The record for a failure message.
    fn show(&self) -> String {
        let mut shown =
            serde_json::json!({"name": self.name, "kind": self.kind, "body": self.body});
        if !self.attributes.is_empty() {
            shown["attributes"] = Value::Object(self.attributes.clone());
        }
        if !self.resource.is_empty() {
            shown["resource"] = Value::Object(self.resource.clone());
        }
        if !self.types.is_empty() {
            shown["types"] = serde_json::to_value(&self.types).unwrap_or_default();
        }
        shown.to_string()
    }

    /// The comparison body: the record minus its field-row keys, and its type tags.
    fn core(&self) -> (Value, BTreeMap<String, String>) {
        let stripped: &[&str] = if self.kind == "log" { &LOG_FIELD_KEYS } else { &FIELD_KEYS };
        let mut body = self.body.clone();
        if let Some(object) = body.as_object_mut() {
            for &key in stripped {
                object.remove(key);
            }
            // Pairing has matched the names already, respelled where the sink respells them.
            if self.is_metric() {
                object.remove("name");
            }
            // A point flagged as holding no recorded value has no value to compare: a stale
            // marker reads back as `NaN`.
            if self.body["no_recorded_value"] == Value::Bool(true) {
                object.remove("value");
            }
            // An unweighted rate below 1 is a field row of its own
            // (`record/sample_rate_unweighted`).
            if unweighted_rate(self) {
                object.remove("sample_rate");
            }
            // A histogram's extremes are a field row of their own (`record/min_max`).
            if matches!(self.kind.as_str(), "histogram" | "exponential_histogram") {
                object.remove("min");
                object.remove("max");
            }
        }
        let types = self
            .types
            .iter()
            .filter(|(pointer, _)| {
                !stripped.iter().any(|key| pointer.starts_with(&format!("/{key}")))
                    && (body.get("value").is_some() || pointer.as_str() != "/value")
            })
            .map(|(pointer, tag)| (pointer.clone(), tag.clone()))
            .collect();
        (body, types)
    }

    /// Kind, temporality, and monotonicity.
    fn identity(&self) -> String {
        format!("{}/{}/{}", self.kind, self.body["temporality"], self.body["monotonic"])
    }

    /// Public attributes, the resource's first and the event's over them, as text.
    fn public(&self) -> BTreeMap<String, String> {
        self.public_as(Spelling::Verbatim)
    }

    /// [`Rec::public`], each key and value as `spelling` writes it.
    fn public_as(&self, spelling: Spelling) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        let resource = self.resource.iter().map(|(k, v)| (k, v, self.resource_types.get(k)));
        let event = self.attributes.iter().map(|(k, v)| (k, v, self.attribute_types.get(k)));
        for (name, value, tags) in resource.chain(event) {
            let key = spelling.key_for(name, self);
            // A carrier is public only where the sink writes it as an ordinary attribute.
            if !key.starts_with("_logit.") {
                let tags = tags.map(String::as_str).unwrap_or("");
                if let Some(value) = spelling.value_for(name, value, tags, self) {
                    out.insert(key, value);
                }
            }
        }
        for (_, key, value) in spelling.derived(self) {
            if !key.starts_with("_logit.") {
                out.insert(key.to_string(), value);
            }
        }
        out
    }
}

/// How a sink spells a record's name and an attribute's key and value, so pairing can find what
/// it wrote (module doc's "Pairing"). One per sink kind; every respelling here is a fact of that
/// sink's encoder and its pair's decoder, cited from the codec's module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Spelling {
    /// `statsd_out`: a name and a tag key with each of `statsd_out`'s reserved characters as
    /// `_`, the name on both sides, and a tag value the same but for `:`. Under
    /// `format: dogstatsd` (`dogstatsd`), also a valued `host` attribute read back as the line's
    /// host ([`statsd`]).
    Statsd { dogstatsd: bool },
    /// `prometheus_out`: `sanitize_metric_name`, plus `_total` on a counter that lacks it;
    /// `sanitize_label_name` on every key; an array's last element as its label value.
    Prometheus,
    /// `influxdb_out`: a `sum`, `gauge`, or `set` off the carrier path leaves as field `value`
    /// of a measurement named for the record, which `influxdb_in` reads back as `<name>.value`;
    /// an array's last element as its tag value.
    Influxdb,
    /// `otlp_out`: names and keys as written, but for an OpenMetrics `info` gauge's `_info`
    /// suffix; an `info` or `stateset` gauge as a cumulative non-monotonic sum; a timestamp value
    /// as an integer of Unix nanoseconds.
    Otlp,
    /// `datadog_out`: names and keys as written, a `host` attribute an ordinary tag; the hostless
    /// mark read back, on the v2 series route, as a `host` resource with no name; and the
    /// attributes [`Spelling::derived`] lists, which the encoder writes from a record's own fields
    /// and `datadog_in` reads back as attributes the input lacks.
    Datadog,
    /// `graphite_out`: each character carbon forbids in a path, a tag name, or a tag value as
    /// `_`, and a tag value's leading `~`; a tag value as the encoder renders it (an `f64` in
    /// Rust's shortest form, `3` for `3.0`, and an array's last representable element), and a
    /// value it omits, such as a timestamp or bytes, as no tag at all ([`graphite_value`]).
    Graphite,
    /// `syslog_out`: a log's message as the encoder renders it, C0 and DEL escaped and a structured
    /// one as `{k="v"}` text; its header attributes as the encoder writes them, sanitized; no
    /// other attribute; and the attributes [`syslog_derived`] lists. Under RFC 3164 (`rfc3164`),
    /// a header `syslog_in` doesn't read back as one moves into the message ([`syslog::read`]).
    /// `facility` is the configured PRI facility; `structured_data` whether `structured_data:` is
    /// set, which this spelling doesn't model.
    Syslog { rfc3164: bool, facility: u8, structured_data: bool },
    /// Any other sink: names, keys, and values as written.
    Verbatim,
}

impl Spelling {
    /// The spelling of the sink of kind `sink` under its serialized `config`.
    fn of(sink: &str, config: &Value) -> Self {
        match sink {
            "statsd_out" => Spelling::Statsd { dogstatsd: config["format"] != "statsd" },
            "prometheus_out" => Spelling::Prometheus,
            "influxdb_out" => Spelling::Influxdb,
            "otlp_out" => Spelling::Otlp,
            "datadog_out" => Spelling::Datadog,
            "graphite_out" => Spelling::Graphite,
            "syslog_out" => Spelling::Syslog {
                rfc3164: config["format"] == "rfc3164",
                facility: serde_json::from_value::<logit_config::SyslogFacility>(
                    config["facility"].clone(),
                )
                .map_or(16, |facility| facility.as_u8()),
                structured_data: !config["structured_data"].is_null(),
            },
            _ => Spelling::Verbatim,
        }
    }

    /// `name` with the characters the sink's wire can't carry rewritten; a record whose name this
    /// changes reaches `record/name_unsafe`.
    fn sanitize(self, name: &str) -> String {
        match self {
            Spelling::Statsd { .. } => substituted(name, is_forbidden_in_name),
            Spelling::Prometheus => sanitize_metric_name(name).into_owned(),
            Spelling::Graphite => substituted(name, is_forbidden_in_path),
            Spelling::Influxdb
            | Spelling::Otlp
            | Spelling::Datadog
            | Spelling::Syslog { .. }
            | Spelling::Verbatim => name.to_string(),
        }
    }

    /// The `record/exemplar_key_unsafe*` row `rec` reaches when an exemplar the OpenMetrics
    /// writer puts on a line writes a label from an attribute key it sanitizes:
    /// `exemplar_key_unsafe` for a monotonic `sum`, `exemplar_key_unsafe_histogram` for a
    /// `histogram`, the two kinds whose exposition lines carry an exemplar. The selection is the
    /// OpenMetrics writer's (`exposed_exemplar_keys`): a counter's first, a histogram's first per
    /// bucket, each within the label budget. Remote-write writes every exemplar within the same
    /// budget, so the cell is reached only through those exemplars there too, whatever the others
    /// carry. Only
    /// `prometheus_out` writes an exemplar's attributes under a name rule; `otlp_out` writes them
    /// as is, and the other sinks write no exemplars.
    fn exemplar_key_unsafe(self, rec: &Rec) -> Option<&'static str> {
        if self != Spelling::Prometheus {
            return None;
        }
        let feature = match rec.kind.as_str() {
            "sum" if rec.body["monotonic"] == Value::Bool(true) => "exemplar_key_unsafe",
            "histogram" => "exemplar_key_unsafe_histogram",
            _ => return None,
        };
        let record = model_record(rec)?;
        let reaches = exposed_exemplar_keys(&record)
            .iter()
            .flatten()
            .any(|key| sanitize_label_name(key) != key.as_str());
        reaches.then_some(feature)
    }

    /// The name the sink's pair decoder reads `rec`, a sink input, back under: for a log, its
    /// message ([`Spelling::log_message`]).
    fn written(self, rec: &Rec) -> String {
        if let Some(message) = self.log_message(rec) {
            return message;
        }
        match self {
            Spelling::Prometheus => {
                let mut name = self.sanitize(&rec.name);
                // A flagged monotonic sum is a stale marker on a counter whatever its
                // temporality (`prometheus::stale_point`).
                let counter = rec.kind == "sum"
                    && (rec.body["temporality"] == "cumulative"
                        || rec.body["no_recorded_value"] == Value::Bool(true))
                    && rec.body["monotonic"] == Value::Bool(true);
                if counter && !name.ends_with("_total") {
                    name.push_str("_total");
                }
                name
            }
            Spelling::Influxdb => {
                let measurement = rec.attributes.get(ATTR_MEASUREMENT).and_then(Value::as_str);
                let carried = measurement.is_some_and(|m| {
                    let field = rec.name.strip_prefix(m).and_then(|f| f.strip_prefix('.'));
                    !m.is_empty() && field.is_some_and(|f| !f.is_empty())
                });
                let valued = matches!(rec.kind.as_str(), "sum" | "gauge" | "set");
                if valued && !carried {
                    format!("{}.value", rec.name)
                } else {
                    rec.name.clone()
                }
            }
            Spelling::Otlp if rec.kind == "gauge" && metric_type(rec) == Some(TYPE_INFO) => {
                format!("{}_info", rec.name)
            }
            _ => self.sanitize(&rec.name),
        }
    }

    /// The message the sink's pair decoder reads log `rec` back with, as text, where the sink's
    /// encoder rewrites it; `None` for a record that isn't a log, and at a sink that writes the
    /// message as it is. For a log alone, `datadog_out`'s logs route writes
    /// `logit_proto::json::value_text`: a `Bytes` message as lossy UTF-8 and any other non-`Str`
    /// one as its JSON text, which `datadog_in` reads back as a `Str`. `otlp_out` writes every
    /// message as an `AnyValue`. `syslog_out` writes [`logit_outputs::syslog::wire_message`],
    /// which `syslog_in` reads back as a `Str`, or as `Bytes` where it isn't UTF-8, and under RFC
    /// 3164 with any header token it doesn't read as a header ([`syslog::read`]).
    fn log_message(self, rec: &Rec) -> Option<String> {
        if rec.kind != "log" {
            return None;
        }
        match self {
            Spelling::Datadog
                if !rec.beside_metrics && datadog::Route::of(rec) == datadog::Route::Log =>
            {
                let log = model_log(rec)?;
                Some(logit_proto::json::value_text(&log.message).into_owned())
            }
            Spelling::Syslog { rfc3164, .. } => {
                syslog::read(rec, rfc3164).map(|read| text(&json_value(read.message)))
            }
            _ => None,
        }
    }

    /// [`Spelling::log_message`], but at `syslog_out` the message as the encoder renders it,
    /// before `syslog_in` decides where its header ends: what `log/message_unsafe` compares.
    fn rendered_message(self, rec: &Rec) -> Option<String> {
        match self {
            Spelling::Syslog { .. } if rec.kind == "log" && !rec.beside_metrics => {
                let log = model_log(rec)?;
                let bytes = logit_outputs::syslog::wire_message(&log.message);
                let value = match String::from_utf8(bytes) {
                    Ok(text) => logit_core::Value::str(text),
                    Err(err) => logit_core::Value::Bytes(err.into_bytes().into()),
                };
                Some(text(&json_value(value)))
            }
            _ => self.log_message(rec),
        }
    }

    /// The [`Rec::identity`] the sink's pair decoder reads `rec`, a sink input, back under:
    /// `otlp_out` writes a gauge flagged `info` or `stateset` as a cumulative non-monotonic sum.
    fn written_identity(self, rec: &Rec) -> String {
        let respelled =
            rec.kind == "gauge" && matches!(metric_type(rec), Some(TYPE_INFO | TYPE_STATESET));
        if self == Spelling::Otlp && respelled {
            "sum/\"cumulative\"/false".to_string()
        } else {
            rec.identity()
        }
    }

    /// The name a read-back record is compared under.
    fn read(self, rec: &Rec) -> String {
        match self {
            Spelling::Statsd { .. } => self.sanitize(&rec.name),
            _ => rec.name.clone(),
        }
    }

    /// The key the sink's pair decoder reads attribute `key` back under.
    fn key(self, key: &str) -> String {
        match self {
            // A `_logit.` carrier is never a label: it rides as itself or not at all.
            Spelling::Prometheus if !key.starts_with("_logit.") => {
                sanitize_label_name(key).into_owned()
            }
            Spelling::Statsd { .. } if !key.starts_with("_logit.") => {
                substituted(key, is_forbidden_in_name)
            }
            Spelling::Graphite if !key.starts_with("_logit.") => {
                substituted(key, is_forbidden_in_tag_name)
            }
            _ => key.to_string(),
        }
    }

    /// [`Spelling::key`] for attribute `key` of `rec`, where the spelling depends on the record:
    /// `statsd_out` under `format: dogstatsd` writes a `host` attribute the line's own host
    /// doesn't omit as a `host:` tag, which `statsd_in` reads back as the line's host
    /// ([`statsd::host_read_back`]), and `datadog_out` writes `container.id` as the
    /// `container_id` tag on the routes that write tags ([`datadog::tags_container_id`]). Where a
    /// sink writes Unified Service Tagging tags, a valued `env`, `version`, or `service` attribute
    /// is that tag, which the pair decoder reads back under its standard key ([`unified::key`]).
    fn key_for(self, key: &str, rec: &Rec) -> String {
        // A PROCID reads back as `process.pid` when it's canonical decimal, else as `syslog.pid`.
        if let Spelling::Syslog { rfc3164, .. } = self {
            if matches!(key, logit_core::attrs::PROCESS_PID | syslog::PID) {
                if let Some(pid) = syslog::read(rec, rfc3164).and_then(|read| read.pid) {
                    return syslog::pid_key(&pid).to_string();
                }
            }
        }
        if self == Spelling::Datadog
            && key == logit_core::attrs::CONTAINER_ID
            && datadog::tags_container_id(rec)
        {
            return "container_id".to_string();
        }
        if self == Spelling::Datadog && key == "service" && datadog::log_service_is_the_field(rec) {
            return "service.name".to_string();
        }
        if unified::writes_tags(self, rec) {
            let value = rec.attributes.get(key).or_else(|| rec.resource.get(key));
            if let Some(standard) = value.and_then(|value| unified::key(key, value)) {
                return standard.to_string();
            }
        }
        if self == (Spelling::Statsd { dogstatsd: true }) && key == "host" {
            let value = rec.attributes.get(key).or_else(|| rec.resource.get(key));
            if let Some(read) = value.and_then(statsd::host_read_back) {
                if !value.is_some_and(|v| statsd::host_is_omitted(rec, v)) {
                    return read.to_string();
                }
            }
        }
        self.key(key)
    }

    /// [`Spelling::value`] for attribute `key` of `rec`. `None` where the sink writes no tag for
    /// the value: `graphite_out`'s omitted values, `statsd_out`'s `host` beside the line's own
    /// host, `datadog_out`'s log `hostname`, and a valued `env`, `version`, or `service` beside its
    /// standard key where the sink writes Unified Service Tagging tags.
    fn value_for(self, key: &str, value: &Value, tags: &str, rec: &Rec) -> Option<String> {
        if let Spelling::Syslog { rfc3164, facility, .. } = self {
            return syslog::value_for(rec, key, value, rfc3164, facility);
        }
        if self == (Spelling::Statsd { dogstatsd: true }) && key == "host" {
            let kept = statsd::host_kept(rec, value)?;
            return Some(self.value(&kept, tags));
        }
        if self == Spelling::Datadog && datadog::reserved_log_key_is_omitted(rec, key) {
            return None;
        }
        // A log's attribute is a key of its JSON object, a `Bytes` value its base64 text.
        if self == Spelling::Datadog
            && rec.kind == "log"
            && datadog::Route::of(rec) == datadog::Route::Log
            && tags.contains("=bytes")
        {
            let mut json = Vec::new();
            logit_proto::json::write_value(&mut json, &model_attribute(value, tags)?);
            return serde_json::from_slice(&json).ok().map(|read: Value| text(&read));
        }
        if unified::tag_is_omitted(self, rec, key, value) {
            return None;
        }
        if self == Spelling::Graphite {
            return graphite_value(value, tags);
        }
        Some(self.value(value, tags))
    }

    /// The attributes the sink writes from `rec`'s own fields, which its pair decoder reads back
    /// as attributes `rec` lacks ([`Derived`]); empty for a sink that writes none:
    /// [`datadog_derived`] and [`syslog_derived`].
    fn derived(self, rec: &Rec) -> Vec<Derived> {
        match self {
            Spelling::Datadog => datadog_derived(rec),
            Spelling::Syslog { rfc3164, facility, .. } => syslog_derived(rec, rfc3164, facility),
            _ => Vec::new(),
        }
    }

    /// An attribute value as text, as the sink's pair decoder reads it back; `tags` is its
    /// `types` entry (`/=timestamp`), empty when it has none.
    fn value(self, value: &Value, tags: &str) -> String {
        match (self, value) {
            (Spelling::Prometheus | Spelling::Influxdb, Value::Array(items)) => {
                items.last().map(text).unwrap_or_default()
            }
            (Spelling::Otlp, Value::String(s)) if tags.contains("=timestamp") => {
                logit_core::time::parse_rfc3339_to_nanos(s)
                    .map_or_else(|_| s.clone(), |nanos| nanos.to_string())
            }
            // `intValue` is signed, so a `u64` above `i64::MAX` leaves as a double.
            (Spelling::Otlp, Value::Number(n))
                if tags.contains("=u64") && n.as_u64().is_some_and(|u| u > i64::MAX as u64) =>
            {
                let wide = n.as_u64().unwrap_or_default() as f64;
                Value::from(wide).to_string()
            }
            (Spelling::Statsd { .. }, Value::String(s)) => {
                substituted(s, |c| c != ':' && is_forbidden_in_name(c))
            }
            _ => text(value),
        }
    }
}

/// An attribute a sink writes from a record's own fields: its `attributes` field row, its key,
/// and its value as text. A `None` row is an attribute used only for pairing, whose field another
/// row compares.
type Derived = (Option<&'static str>, &'static str, String);

/// The `attributes` field rows `rec` reaches for what any sink derives ([`Spelling::derived`]):
/// the union of every sink's rows, chosen from the record alone, so every sink that compares the
/// record reaches them, and one that derives nothing for a row reads it `exact`.
fn derived_rows(rec: &Rec) -> Vec<&'static str> {
    let mut rows = Vec::new();
    // `syslog_out`'s rows don't depend on its config.
    let syslog = Spelling::Syslog { rfc3164: false, facility: 0, structured_data: false };
    for spelling in [Spelling::Datadog, syslog] {
        for (row, _, _) in spelling.derived(rec) {
            if let Some(row) = row.filter(|row| !rows.contains(row)) {
                rows.push(row);
            }
        }
    }
    rows
}

/// The attributes `syslog_out` writes for log `rec` from its own fields or the sink's config,
/// which `syslog_in` reads back as attributes `rec` lacks (`crates/logit-outputs/src/syslog.rs`'s
/// "Header-field precedence"):
///
/// - `syslog_facility`: the configured facility, on a log with no `syslog.facility`;
/// - `no_host`: the run's hostname, on a log that carries no host, where `syslog_in` reads it back
///   as HOSTNAME;
/// - for pairing only, the `syslog.timestamp` written from `event.timestamp` on a log with none,
///   which `log/timestamp` compares, and under RFC 3164 a `syslog.tag` and PROCID `syslog_in` reads
///   from the message's first token.
fn syslog_derived(rec: &Rec, rfc3164: bool, facility: u8) -> Vec<Derived> {
    if rec.kind != "log" {
        return Vec::new();
    }
    let Some(read) = syslog::read(rec, rfc3164) else { return Vec::new() };
    let mut out = Vec::new();
    if !rec.attributes.contains_key(syslog::FACILITY) {
        let written = logit_outputs::syslog::wire_facility(&logit_core::AttrMap::new(), facility);
        out.push((Some("syslog_facility"), syslog::FACILITY, written.to_string()));
    }
    if !syslog::carries_host(rec) {
        if let Some(host) = &read.host {
            out.push((Some("no_host"), logit_core::attrs::HOST_NAME, host.clone()));
        }
    }
    if !rec.attributes.contains_key(syslog::TIMESTAMP) {
        if let Some((written, _)) = syslog::written_timestamp(rec, rfc3164) {
            out.push((None, syslog::TIMESTAMP, text(&written)));
        }
    }
    if !read.tag_from_attribute {
        if let Some(tag) = &read.tag {
            out.push((None, syslog::TAG, tag.clone()));
            if let Some(pid) = &read.pid {
                out.push((None, syslog::pid_key(pid), pid.clone()));
            }
        }
    }
    out
}

/// `value` as `format: json` writes it, so a `Bytes` value is its `b"..."` text.
fn json_value(value: logit_core::Value) -> Value {
    let mut attributes = logit_core::AttrMap::new();
    attributes.insert("v", value);
    let batch = EventBatch {
        resource: std::sync::Arc::default(),
        scope: None,
        events: vec![logit_core::Event::empty(0, attributes)],
    };
    let mut line = String::new();
    render_ndjson(&mut line, &batch);
    serde_json::from_str::<Value>(&line).map_or(Value::Null, |line| line["attributes"]["v"].clone())
}

/// The attributes `datadog_out` writes from `rec`'s own fields, which `datadog_in` reads back as
/// attributes `rec` lacks (`logit_proto::datadog`'s encode tables):
///
/// - `no_host`: the run's hostname, on a record that carries no host and no hostless mark (the
///   encoder's default host), under the key `datadog_in` reads the route's host back as
///   (`host.name`, on the resource for a span's tracer payload); a span's host is its resource's,
///   and a relaying Agent's hostname leaves it unfilled;
/// - `log_status`: `status` as the severity's name, on a log, not a Datadog event, with a severity
///   and no `status` attribute;
/// - `span_kind`: `span.kind` as the kind's name, on a span whose kind isn't `internal` and that
///   has no `span.kind` attribute;
/// - `trace_id_high`: `_dd.p.tid` as the trace id's high 64 bits in hex, on the first span of its
///   trace in the batch, when the high half is nonzero and no span of the trace carries one
///   ([`Rec::tid_due`]);
/// - for pairing only, `trace_id` and `span_id` as the hex ids of a log's `trace`, on a log with
///   no attribute of either name; `log/trace` compares them.
fn datadog_derived(rec: &Rec) -> Vec<Derived> {
    // An event with no metric, log, or span takes no `datadog_out` route.
    if rec.kind == "event" {
        return Vec::new();
    }
    let has = |key: &str| rec.attributes.contains_key(key) || rec.resource.contains_key(key);
    let mut out = Vec::new();
    match datadog::Route::of(rec) {
        datadog::Route::Span => {
            if !datadog::carries_host(rec, datadog::Route::Span) {
                out.push((Some("no_host"), "host.name", super::HOSTNAME.to_string()));
            }
            let kind = rec.body["kind"].as_str().unwrap_or("internal");
            if kind != "internal" && !has("span.kind") {
                out.push((Some("span_kind"), "span.kind", kind.to_string()));
            }
            if rec.tid_due {
                let high = rec.body["trace_id"].as_str().and_then(|t| t.get(..16)).unwrap_or("");
                out.push((Some("trace_id_high"), "_dd.p.tid", high.to_string()));
            }
        }
        route => {
            if !datadog::carries_host(rec, route) {
                out.push((Some("no_host"), "host.name", super::HOSTNAME.to_string()));
            }
            let severity = rec.body["severity"].as_str();
            if let (datadog::Route::Log, Some(severity)) = (route, severity) {
                if !has("status") {
                    out.push((Some("log_status"), "status", severity.to_string()));
                }
            }
            // An attribute of either name suppresses both ids, so a relayed Datadog log leaves
            // with its own.
            if route == datadog::Route::Log && !has("trace_id") && !has("span_id") {
                for key in ["trace_id", "span_id"] {
                    if let Some(id) = rec.body[key].as_str() {
                        out.push((None, key, id.to_string()));
                    }
                }
            }
        }
    }
    out
}

/// Marks the first span of each trace in one batch's records as due a synthesized `_dd.p.tid`
/// when its trace id's high half is nonzero and no span of the trace carries one: the chunk
/// `datadog_out` groups a batch's spans into, by 128-bit trace id.
fn mark_tid_due(recs: &mut [Rec]) {
    let trace = |rec: &Rec| rec.body["trace_id"].as_str().unwrap_or("").to_string();
    let carried: BTreeSet<String> = recs
        .iter()
        .filter(|r| r.kind == "span")
        .filter(|r| r.attributes.contains_key("_dd.p.tid") || r.resource.contains_key("_dd.p.tid"))
        .map(trace)
        .collect();
    let mut seen = BTreeSet::new();
    for rec in recs.iter_mut().filter(|r| r.kind == "span") {
        let id = trace(rec);
        let high = id.get(..16).unwrap_or("");
        let nonzero = id.len() == 32 && high.bytes().any(|b| b != b'0');
        rec.tid_due = nonzero && !carried.contains(&id) && seen.insert(id.clone());
    }
}

/// Where `statsd_out` under `format: dogstatsd` writes a record's host (its module doc's "The
/// host"): a usable `host.name` (a non-empty `Str`) as the line's host, else the hostless mark as
/// an empty `host:` tag, and either way no `host` attribute beside it. Without either, a `host`
/// attribute is an ordinary `host:` tag. `statsd_in` reads the host back as `host.name` and an
/// empty `host:` tag as the mark (`crates/logit-proto/src/statsd/mod.rs`'s "The host").
mod statsd {
    use super::Rec;
    use serde_json::Value;

    pub(super) const HOST_NAME: &str = "host.name";
    pub(super) const HOSTLESS: &str = "_logit.datadog.hostless";

    /// An attribute, the event's over the resource's.
    fn get<'a>(rec: &'a Rec, key: &str) -> Option<&'a Value> {
        rec.attributes.get(key).or_else(|| rec.resource.get(key))
    }

    /// Whether the line names its own host: a usable `host.name`, else the hostless mark.
    fn names_host(rec: &Rec) -> bool {
        let usable = get(rec, HOST_NAME).and_then(Value::as_str).is_some_and(|h| !h.is_empty());
        usable || get(rec, HOSTLESS) == Some(&Value::Bool(true))
    }

    /// The elements of a `host` attribute holding `value` that the sink leaves off: with the line
    /// naming its own host, each valued element is a second `host:` tag the Agent consumes, and
    /// only a bare `host` token (`true`) stays. An array is judged per element, as the sink
    /// counts it.
    fn omitted_elements<'a>(rec: &Rec, value: &'a Value) -> Vec<&'a Value> {
        if !names_host(rec) {
            return Vec::new();
        }
        let elements = match value {
            Value::Array(elements) => elements.as_slice(),
            _ => std::slice::from_ref(value),
        };
        elements.iter().filter(|e| **e != Value::Bool(true)).collect()
    }

    /// Whether any element of a `host` attribute holding `value` is left off.
    pub(super) fn host_is_omitted(rec: &Rec, value: &Value) -> bool {
        !omitted_elements(rec, value).is_empty()
    }

    /// What the sink writes of a `host` attribute holding `value`: the value less its omitted
    /// elements, `None` when nothing is left.
    pub(super) fn host_kept(rec: &Rec, value: &Value) -> Option<Value> {
        let omitted = omitted_elements(rec, value);
        match value {
            Value::Array(elements) if !omitted.is_empty() => {
                let kept: Vec<Value> =
                    elements.iter().filter(|e| **e == Value::Bool(true)).cloned().collect();
                (!kept.is_empty()).then_some(Value::Array(kept))
            }
            _ if omitted.is_empty() => Some(value.clone()),
            _ => None,
        }
    }

    /// The key `statsd_in` reads a `host` attribute's `host:` tag back under, where it's written:
    /// `host.name` for a non-empty string, the hostless mark for an empty one.
    pub(super) fn host_read_back(value: &Value) -> Option<&'static str> {
        match value.as_str() {
            Some("") => Some(HOSTLESS),
            Some(_) => Some(HOST_NAME),
            None => None,
        }
    }
}

/// Datadog's Unified Service Tagging tags (`logit_core::attrs::UNIFIED_SERVICE_TAGS`):
/// `statsd_out` under `format: dogstatsd` and `datadog_out` on its tag routes write the standard
/// keys as `env:`, `version:`, and `service:` tags, which their pair decoders read back as the
/// keys, and leave off a valued attribute of a tag's name beside its key.
mod unified {
    use super::{datadog, Rec, Spelling};
    use logit_core::attrs::UNIFIED_SERVICE_TAGS;
    use serde_json::Value;

    /// Whether `spelling`'s sink writes `rec`'s service identity as Datadog tags.
    pub(super) fn writes_tags(spelling: Spelling, rec: &Rec) -> bool {
        spelling == (Spelling::Statsd { dogstatsd: true })
            || (spelling == Spelling::Datadog && datadog::tags_container_id(rec))
    }

    /// The standard key a valued `env`, `version`, or `service` attribute's tag reads back
    /// under; `None` for any other key and for a bare token (`true`), which names no value.
    pub(super) fn key(key: &str, value: &Value) -> Option<&'static str> {
        if *value == Value::Bool(true) {
            return None;
        }
        UNIFIED_SERVICE_TAGS.iter().find(|(tag, _)| *tag == key).map(|(_, standard)| *standard)
    }

    /// Whether `rec` carries the standard key a valued `key` attribute's tag names, with a value
    /// that writes a tag.
    pub(super) fn beside_its_key(rec: &Rec, key: &str, value: &Value) -> bool {
        self::key(key, value).is_some_and(|standard| {
            rec.attributes
                .get(standard)
                .or_else(|| rec.resource.get(standard))
                .is_some_and(|v| !v.is_null() && !v.is_object())
        })
    }

    /// Whether `spelling`'s sink leaves `rec`'s `key` attribute off beside its standard key.
    pub(super) fn tag_is_omitted(spelling: Spelling, rec: &Rec, key: &str, value: &Value) -> bool {
        writes_tags(spelling, rec) && beside_its_key(rec, key, value)
    }
}

/// What [`Spelling::Datadog`] reads of a record: which `datadog_out` route it takes and whether it
/// carries a host the encoder writes instead of the default (`logit_proto::datadog`'s encode
/// tables). Every route's host reads back as `host.name`.
mod datadog {
    use super::Rec;
    use serde_json::Value;

    /// The route family a record takes.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Route {
        /// Series, distribution points, or sketches.
        Metric,
        /// A gauge carrying `_logit.statsd.service_check.name`: the check route.
        Check,
        /// A log carrying `_logit.statsd.event.title`.
        Event,
        Log,
        Span,
    }

    impl Route {
        pub(super) fn of(rec: &Rec) -> Self {
            match rec.kind.as_str() {
                "span" => Route::Span,
                "log" if get(rec, "_logit.statsd.event.title").is_some() => Route::Event,
                "log" => Route::Log,
                "gauge" if get(rec, "_logit.statsd.service_check.name").is_some() => Route::Check,
                _ => Route::Metric,
            }
        }
    }

    /// An attribute, the event's over the resource's.
    fn get<'a>(rec: &'a Rec, key: &str) -> Option<&'a Value> {
        rec.attributes.get(key).or_else(|| rec.resource.get(key))
    }

    /// A `host.name` the encoder writes as the host: anything but an empty `Str`. A non-`Str`
    /// one is never replaced.
    fn named(rec: &Rec) -> bool {
        get(rec, "host.name").is_some_and(|v| v.as_str() != Some(""))
    }

    /// Whether `container.id` leaves as Datadog's `container_id` tag, which `datadog_in` reads
    /// back under that key: on a metric, a Datadog event, or a service check.
    pub(super) fn tags_container_id(rec: &Rec) -> bool {
        matches!(Route::of(rec), Route::Metric | Route::Event | Route::Check)
    }

    /// Whether a log's `hostname` attribute, or its `service` attribute beside a `service.name`
    /// the encoder writes, is left off: it would collide with the wire's own field of that name.
    pub(super) fn reserved_log_key_is_omitted(rec: &Rec, key: &str) -> bool {
        let beside = match key {
            "hostname" => true,
            "service" => service_name_written(rec),
            _ => false,
        };
        beside && Route::of(rec) == Route::Log && get(rec, key).is_some()
    }

    /// Whether a log's `service.name` is its `service` field: anything but an empty `Str`.
    fn service_name_written(rec: &Rec) -> bool {
        get(rec, "service.name").is_some_and(|v| v.as_str() != Some(""))
    }

    /// Whether a log's `service` attribute is its `service` field, which `datadog_in` reads back
    /// as `service.name` when it's a string: one with no `service.name` the encoder writes.
    pub(super) fn log_service_is_the_field(rec: &Rec) -> bool {
        Route::of(rec) == Route::Log
            && !service_name_written(rec)
            && get(rec, "service").is_some_and(|v| v.as_str().is_some())
    }

    /// Whether a span carries a field a Datadog span has no form for: `status: ok`, a status
    /// message, `trace_state`, `flags`, or a dropped count.
    pub(super) fn otel_only(rec: &Rec) -> bool {
        let body = &rec.body;
        body["status"] == "ok"
            || ["status_message", "trace_state", "flags"].iter().any(|k| body.get(*k).is_some())
            || body.as_object().is_some_and(|o| o.keys().any(|k| k.starts_with("dropped_")))
    }

    /// Whether `rec` carries a Datadog hostless mark: `_logit.datadog.hostless` `true`, or a
    /// `host` entry in `_logit.datadog.resources`.
    pub(super) fn hostless_mark(rec: &Rec) -> bool {
        let hostless = get(rec, "_logit.datadog.hostless") == Some(&Value::Bool(true));
        let host_resource = get(rec, "_logit.datadog.resources")
            .and_then(Value::as_array)
            .is_some_and(|r| r.iter().any(|e| e["type"] == "host"));
        hostless || host_resource
    }

    /// Whether `rec` carries a host or a hostless mark, so the encoder writes no default host.
    pub(super) fn carries_host(rec: &Rec, route: Route) -> bool {
        let has = |key: &str| get(rec, key).is_some();
        match route {
            // `syslog.hostname` and `host` are Datadog's reserved log host attributes.
            Route::Log => named(rec) || has("syslog.hostname") || has("host"),
            Route::Event | Route::Check | Route::Metric => named(rec) || hostless_mark(rec),
            // A span's host is its tracer payload's, the resource's `host.name`; an Agent's own
            // hostname beside an empty one stops the default host.
            Route::Span => {
                rec.resource
                    .get("host.name")
                    .is_some_and(|v| v.as_str().is_some_and(|h| !h.is_empty()))
                    || rec.resource.contains_key("_logit.datadog.agent.hostname")
            }
        }
    }
}

/// What `syslog_in` reads back from the message `syslog_out` writes for a log
/// ([`Spelling::Syslog`]): the header fields and message through the encoder's own renderers
/// (`logit_outputs::syslog`'s `wire_*` functions), and under RFC 3164 the decoder's rule for where
/// the header ends (`logit_proto::syslog`'s "The RFC 3164 header"). The spelling assumes the
/// sink's `hostname:` and `app_name:` are unset, as every flow leaves them: its HOSTNAME default is
/// the run's hostname, and it writes no default APP-NAME.
mod syslog {
    use super::{model_attribute, model_log, Rec};
    use logit_core::attrs::{HOST_NAME, PROCESS_PID};
    use logit_core::time::{format_rfc3339_utc, parse_rfc3339_to_nanos};
    use logit_core::AttrMap;
    use logit_outputs::syslog::{
        wire_app_name, wire_hostname, wire_message, wire_msgid, wire_procid, wire_timestamp, Format,
    };
    use logit_proto::syslog::is_tag_shaped;
    use serde_json::{Map, Value};
    use std::collections::BTreeMap;

    pub(super) const FACILITY: &str = "syslog.facility";
    pub(super) const TAG: &str = "syslog.tag";
    pub(super) const PID: &str = "syslog.pid";
    pub(super) const MSGID: &str = "syslog.msgid";
    pub(super) const SD: &str = "syslog.sd";
    pub(super) const TIMESTAMP: &str = "syslog.timestamp";
    pub(super) const SEVERITY: &str = "_logit.syslog.severity";

    /// One day in Unix nanoseconds.
    const DAY: i64 = 86_400 * 1_000_000_000;

    pub(super) fn format(rfc3164: bool) -> Format {
        if rfc3164 {
            Format::Rfc3164
        } else {
            Format::Rfc5424
        }
    }

    /// `map`'s attributes back in the model, each through its type tags.
    fn model_attrs(map: &Map<String, Value>, types: &BTreeMap<String, String>) -> AttrMap {
        let mut attrs = AttrMap::new();
        for (key, value) in map {
            let tags = types.get(key).map(String::as_str).unwrap_or("");
            if let Some(value) = model_attribute(value, tags) {
                attrs.insert(key, value);
            }
        }
        attrs
    }

    /// `rec`'s event attributes and its resource's, in the model.
    fn scopes(rec: &Rec) -> (AttrMap, AttrMap) {
        (
            model_attrs(&rec.attributes, &rec.attribute_types),
            model_attrs(&rec.resource, &rec.resource_types),
        )
    }

    /// The event's timestamp in Unix nanoseconds.
    fn nanos(rec: &Rec) -> Option<i64> {
        parse_rfc3339_to_nanos(&rec.timestamp).ok()
    }

    /// Whether `rec` carries a host the encoder writes as HOSTNAME: a non-empty string
    /// `host.name` on the event or its resource.
    pub(super) fn carries_host(rec: &Rec) -> bool {
        let (attrs, resource) = scopes(rec);
        wire_hostname(Format::Rfc5424, &attrs, &resource, None).is_some()
    }

    /// The `syslog.timestamp` `syslog_in` reads back for `rec` as `format: json` writes it, and
    /// whether the encoder wrote it from `rec`'s own `syslog.timestamp` rather than from its
    /// `event.timestamp`.
    pub(super) fn written_timestamp(rec: &Rec, rfc3164: bool) -> Option<(Value, bool)> {
        let (attrs, _) = scopes(rec);
        let at = nanos(rec)?;
        let format = format(rfc3164);
        let text = wire_timestamp(format, &attrs, at);
        // The attribute's instant doesn't move with the event's.
        let from_attribute = text == wire_timestamp(format, &attrs, at + DAY);
        let read = if text == "-" {
            Value::Null
        } else if rfc3164 {
            Value::String(text)
        } else {
            Value::String(format_rfc3339_utc(parse_rfc3339_to_nanos(&text).ok()?))
        };
        Some((read, from_attribute))
    }

    /// Whether `token` is a canonical decimal PROCID, which `syslog_in` reads back as
    /// `process.pid` rather than `syslog.pid`.
    fn canonical_pid(token: &str) -> bool {
        token.bytes().all(|b| b.is_ascii_digit())
            && (token == "0" || !token.starts_with('0'))
            && token.parse::<u64>().is_ok()
    }

    /// What `syslog_in` reads back from the header and message `syslog_out` writes for a log.
    pub(super) struct Read {
        /// HOSTNAME, read back as `host.name`.
        pub host: Option<String>,
        /// TAG or APP-NAME, read back as `syslog.tag`, and whether it's the input's `syslog.tag`.
        pub tag: Option<String>,
        pub tag_from_attribute: bool,
        /// PROCID, read back as `process.pid` when canonical decimal, else as `syslog.pid`; and
        /// whether it's the input's `process.pid` rather than its `syslog.pid`.
        pub pid: Option<String>,
        pub pid_from_process: bool,
        /// MSG as `format: json` writes it.
        pub message: logit_core::Value,
        /// Whether MSG reads back other than written: under RFC 3164, a header token read into
        /// the message, or the message's first token read as a TAG.
        pub boundary_moved: bool,
    }

    pub(super) fn read(rec: &Rec, rfc3164: bool) -> Option<Read> {
        let log = model_log(rec)?;
        let (attrs, resource) = scopes(rec);
        let format = format(rfc3164);
        let host = wire_hostname(format, &attrs, &resource, Some(super::super::HOSTNAME));
        let tag = wire_app_name(format, &attrs, None);
        let pid = wire_procid(format, &attrs, &resource);
        let mut without_pid = attrs.clone();
        without_pid.remove(PID);
        let pid_from_process = wire_procid(format, &without_pid, &resource).is_some();
        let message = wire_message(&log.message);
        if !rfc3164 {
            return Some(Read {
                host,
                tag_from_attribute: tag.is_some(),
                tag,
                pid,
                pid_from_process,
                message: as_value(message),
                boundary_moved: false,
            });
        }
        // `<PRI>TIMESTAMP` then each token after one space, as `write_rfc3164_header` writes them,
        // and MSG after one more when it isn't empty.
        let mut tokens: Vec<String> = Vec::new();
        tokens.extend(host.clone());
        let tag_pid = tag.as_ref().map(|tag| match &pid {
            Some(pid) => (format!("{tag}[{pid}]:"), true),
            None => (format!("{tag}:"), false),
        });
        tokens.extend(tag_pid.as_ref().map(|(token, _)| token.clone()));
        let mut after_ts = tokens.join(" ").into_bytes();
        if !message.is_empty() {
            if !after_ts.is_empty() {
                after_ts.push(b' ');
            }
            after_ts.extend_from_slice(&message);
        }
        let split = |s: &[u8]| -> (Vec<u8>, Vec<u8>) {
            match s.iter().position(|&b| b == b' ') {
                Some(i) => (s[..i].to_vec(), s[i + 1..].to_vec()),
                None => (s.to_vec(), Vec::new()),
            }
        };
        let (first, after_first) = split(&after_ts);
        let (read_host, read_tag, read_message) = if is_tag_shaped(&first) {
            (None, Some(first), after_first)
        } else {
            let (second, after_second) = split(&after_first);
            if is_tag_shaped(&second) {
                (Some(first), Some(second), after_second)
            } else {
                (None, None, after_ts.clone())
            }
        };
        let read_host =
            read_host.filter(|h| !h.is_empty()).map(|h| String::from_utf8_lossy(&h).into_owned());
        let (read_tag, read_pid) = match read_tag {
            None => (None, None),
            Some(token) => {
                let body = String::from_utf8_lossy(&token[..token.len() - 1]).into_owned();
                match body.rfind('[') {
                    Some(open) => (
                        Some(body[..open].to_string()),
                        Some(body[open + 1..body.len() - 1].to_string()),
                    ),
                    None => (Some(body), None),
                }
            }
        };
        let written_tag = tag_pid.map(|(token, _)| token);
        let tag_from_attribute =
            read_tag.is_some() && written_tag.is_some() && read_tag.as_deref() == tag.as_deref();
        Some(Read {
            host: read_host,
            tag: read_tag,
            tag_from_attribute,
            pid: read_pid,
            pid_from_process,
            boundary_moved: read_message != message,
            message: as_value(read_message),
        })
    }

    /// MSG's bytes as `syslog_in` reads them: a string when valid UTF-8, else bytes.
    fn as_value(bytes: Vec<u8>) -> logit_core::Value {
        match String::from_utf8(bytes) {
            Ok(text) => logit_core::Value::str(&text),
            Err(err) => logit_core::Value::Bytes(err.into_bytes().into()),
        }
    }

    /// The key `syslog_in` reads a written PROCID back under.
    pub(super) fn pid_key(pid: &str) -> &'static str {
        if canonical_pid(pid) {
            PROCESS_PID
        } else {
            PID
        }
    }

    /// Attribute `key` of `rec` as `syslog_in` reads it back, as text; `None` where `syslog_out`
    /// writes nothing for it. Only the header's attributes are written: `host.name`, `syslog.tag`,
    /// `syslog.facility`, `syslog.msgid`, `syslog.sd`, `syslog.timestamp`, and the PROCID's
    /// `process.pid` or `syslog.pid`.
    pub(super) fn value_for(
        rec: &Rec,
        key: &str,
        value: &Value,
        rfc3164: bool,
        facility: u8,
    ) -> Option<String> {
        let on_event = rec.attributes.get(key) == Some(value);
        let read = read(rec, rfc3164)?;
        match key {
            HOST_NAME => read.host,
            TAG if on_event && read.tag_from_attribute => read.tag,
            PROCESS_PID if read.pid_from_process => read.pid,
            PID if !read.pid_from_process => read.pid,
            FACILITY if on_event => {
                let (attrs, _) = scopes(rec);
                Some(logit_outputs::syslog::wire_facility(&attrs, facility).to_string())
            }
            MSGID if on_event => {
                let (attrs, _) = scopes(rec);
                wire_msgid(format(rfc3164), &attrs)
            }
            SD if on_event && !rfc3164 && value.is_object() => Some(super::text(value)),
            TIMESTAMP if on_event => {
                written_timestamp(rec, rfc3164).map(|(read, _)| super::text(&read))
            }
            _ => None,
        }
    }
}

/// A value as text, for comparing attributes across a wire that writes every value as text.
fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Array(items) => items.iter().map(text).collect::<Vec<_>>().join("\u{1f}"),
        other => other.to_string(),
    }
}

/// A scalar as a number: a boolean as `1` or `0`, a non-finite float's string as itself.
fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Bool(b) => Some(f64::from(u8::from(*b))),
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn same_number(a: &Value, b: &Value) -> bool {
    match (number(a), number(b)) {
        (Some(a), Some(b)) => a.to_bits() == b.to_bits() || a == b,
        _ => false,
    }
}

/// `s` with each character `forbidden` names as `_`. The predicates are the encoders' own
/// (`crates/logit-outputs/src/statsd.rs`'s and `logit_proto::graphite`'s "Sanitization"), since a
/// read-back name, key, or value is compared as the sink wrote it.
fn substituted(s: &str, forbidden: impl Fn(char) -> bool) -> String {
    s.chars().map(|c| if forbidden(c) { '_' } else { c }).collect()
}

/// An attribute value as `graphite_out` writes it as a tag value, `None` where it writes no tag:
/// the value back in the model, rendered by the encoder's own `render_tag_value` and
/// `sanitize_tag_value_into`, so the two can't drift. A value empty after that writes none.
/// `tags` is its `types` entry.
fn graphite_value(value: &Value, tags: &str) -> Option<String> {
    let mut raw = String::new();
    if !render_tag_value(&mut raw, &model_value(value, tags, "/")) {
        return None;
    }
    let mut out = String::new();
    sanitize_tag_value_into(&mut out, &raw);
    (!out.is_empty()).then_some(out)
}

/// A `format: json` attribute value as the model [`logit_core::Value`] it was written from,
/// `tags` its `types` entry and `pointer` its place there (`/`, or `/<i>` for an array element).
/// A map comes back as `Null`, since neither has a tag form; any other value is exact.
fn model_value(value: &Value, tags: &str, pointer: &str) -> logit_core::Value {
    use logit_core::Value as Model;
    let tagged = |tag: &str| tags.split(';').any(|entry| entry == format!("{pointer}={tag}"));
    match value {
        _ if tagged("bytes") => Model::Bytes(bytes::Bytes::new()),
        _ if tagged("timestamp") => Model::Timestamp(0),
        Value::String(s) if tagged("f64") => s.parse().map_or(Model::Null, Model::F64),
        Value::String(s) => Model::str(s.clone()),
        Value::Bool(b) => Model::Bool(*b),
        Value::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => Model::I64(i),
            (_, Some(u), _) => Model::U64(u),
            (_, _, Some(f)) => Model::F64(f),
            _ => Model::Null,
        },
        Value::Array(items) => Model::Array(
            items
                .iter()
                .enumerate()
                .map(|(i, item)| model_value(item, tags, &format!("/{i}")))
                .collect(),
        ),
        Value::Null | Value::Object(_) => Model::Null,
    }
}

/// A `format: json` attribute value as the model [`logit_core::Value`] it was written from, read
/// back through `logit_proto::event_json` with its `types` entry `tags`, so a `Bytes` value keeps
/// its bytes.
fn model_attribute(value: &Value, tags: &str) -> Option<logit_core::Value> {
    let types: Map<String, Value> = tags
        .split(';')
        .filter_map(|entry| entry.split_once('='))
        .map(|(pointer, tag)| {
            let below = pointer.strip_prefix('/').unwrap_or(pointer);
            let pointer = if below.is_empty() {
                "/attributes/v".to_string()
            } else {
                format!("/attributes/v/{below}")
            };
            (pointer, Value::String(tag.to_string()))
        })
        .collect();
    let line = serde_json::json!({
        "timestamp": "1970-01-01T00:00:00.000000000Z",
        "attributes": {"v": value},
        "types": types,
    });
    let batches = EventJsonDecoder::new().decode_batches(line.to_string().as_bytes()).ok()?;
    let event = batches.into_iter().next()?.events.into_iter().next()?;
    event.attributes.get("v").cloned()
}

/// RFC 6901's unescaping of one pointer token.
fn unescape(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

/// Every record in `batches`, in batch order.
fn records(batches: &[EventBatch]) -> Vec<Rec> {
    let mut out = Vec::new();
    for batch in batches {
        let start = out.len();
        out.extend(batch_records(batch));
        mark_tid_due(&mut out[start..]);
    }
    out
}

/// One batch's records, in order.
fn batch_records(batch: &EventBatch) -> Vec<Rec> {
    let mut lines = String::new();
    render_ndjson(&mut lines, batch);
    let mut out = Vec::new();
    for line in lines.lines() {
        let event: Value = serde_json::from_str(line).expect("format: json lines parse");
        let types: BTreeMap<String, String> = event["types"]
            .as_object()
            .map(|types| {
                types
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let tagged = |prefix: &str| -> BTreeMap<String, String> {
            let mut by_key: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for (pointer, tag) in &types {
                let Some(rest) = pointer.strip_prefix(prefix) else { continue };
                let (key, below) = rest.split_once('/').map_or((rest, ""), |(k, b)| (k, b));
                by_key.entry(unescape(key)).or_default().push(format!("/{below}={tag}"));
            }
            by_key.into_iter().map(|(key, tags)| (key, tags.join(";"))).collect()
        };
        let attributes = event["attributes"].as_object().cloned().unwrap_or_default();
        let resource = event["resource"]["attributes"].as_object().cloned().unwrap_or_default();
        let attribute_types = tagged("/attributes/");
        let resource_types = tagged("/resource/attributes/");
        let beside_metrics = event["metrics"].as_array().is_some_and(|m| !m.is_empty());
        let scope = event.get("scope").cloned();
        let rec = |kind: String, name: String, body: Value, types| Rec {
            kind,
            name,
            body,
            beside_metrics,
            types,
            attributes: attributes.clone(),
            attribute_types: attribute_types.clone(),
            resource: resource.clone(),
            resource_types: resource_types.clone(),
            scope: scope.clone(),
            timestamp: event["timestamp"].as_str().unwrap_or("").to_string(),
            tid_due: false,
        };
        if let Some(log) = event.get("log") {
            let name = log.get("message").map(text).unwrap_or_default();
            let types = types
                .iter()
                .filter_map(|(p, t)| {
                    p.strip_prefix("/log").map(|rest| (rest.to_string(), t.clone()))
                })
                .collect();
            out.push(rec("log".to_string(), name, log.clone(), types));
        }
        if let Some(span) = event.get("span") {
            let name = span.get("name").map(text).unwrap_or_default();
            let types = types
                .iter()
                .filter_map(|(p, t)| {
                    p.strip_prefix("/span").map(|rest| (rest.to_string(), t.clone()))
                })
                .collect();
            out.push(rec("span".to_string(), name, span.clone(), types));
        }
        for (i, metric) in event["metrics"].as_array().into_iter().flatten().enumerate() {
            let prefix = format!("/metrics/{i}");
            let types = types
                .iter()
                .filter_map(|(p, t)| {
                    let rest = p.strip_prefix(&prefix)?;
                    (rest.is_empty() || rest.starts_with('/'))
                        .then(|| (rest.to_string(), t.clone()))
                })
                .collect();
            let kind = metric["kind"].as_str().unwrap_or("").to_string();
            let name = metric["name"].as_str().unwrap_or("").to_string();
            out.push(rec(kind, name, metric.clone(), types));
        }
        // An event with no metric, log, or span still has attributes a sink may write or skip,
        // such as an InfluxDB point whose fields are all strings.
        if !beside_metrics && event.get("log").is_none() && event.get("span").is_none() {
            let body = Value::Object(Map::new());
            out.push(rec("event".to_string(), String::new(), body, BTreeMap::new()));
        }
    }
    out
}

/// What pairing found for each input record.
struct Pairing {
    /// Per input, its class as pairing saw it, `None` when nothing read back accounts for it.
    seen: Vec<Option<Class>>,
    /// Per input, the read-back records it claimed.
    claimed: Vec<Vec<usize>>,
    /// Read-back records no input claimed.
    unclaimed: Vec<usize>,
}

/// The module doc's passes.
fn pair(ins: &[Rec], outs: &[Rec], spelling: Spelling) -> Pairing {
    let mut used = vec![false; outs.len()];
    let mut seen: Vec<Option<Class>> = vec![None; ins.len()];
    let mut claimed: Vec<Vec<usize>> = vec![Vec::new(); ins.len()];
    // Passes (1) and (2) run under both spellings before pass (3) runs under either, so a dotted
    // expansion never claims a record that a respelling reads back whole.
    for (respelled, expansions) in [(false, false), (true, false), (false, true), (true, true)] {
        let in_names: Vec<String> = ins
            .iter()
            .map(|rec| if respelled { spelling.written(rec) } else { rec.name.clone() })
            .collect();
        let out_names: Vec<String> = outs
            .iter()
            .map(|rec| if respelled { spelling.read(rec) } else { rec.name.clone() })
            .collect();
        let candidates = |i: usize, used: &[bool]| -> Vec<usize> {
            let input = &ins[i];
            let mut scored: Vec<(usize, u8)> = outs
                .iter()
                .enumerate()
                .filter(|(o, out)| {
                    !used[*o] && out.family() == input.family() && out_names[*o] == in_names[i]
                })
                .map(|(o, out)| (o, score(input, out, spelling)))
                .filter(|(_, s)| *s > 0)
                .collect();
            // Logs of one message pair by their time, so a log's time row compares it with its
            // own read-back: the same instant first, then the same millisecond.
            if input.kind == "log" {
                let millis = |rec: &Rec| rec.timestamp.get(..23).map(str::to_string);
                scored.sort_by_key(|(o, _)| {
                    (outs[*o].timestamp != input.timestamp, millis(&outs[*o]) != millis(input))
                });
            }
            let best = scored.iter().map(|(_, s)| *s).max().unwrap_or(0);
            scored.into_iter().filter(|(_, s)| *s == best).map(|(o, _)| o).collect()
        };
        let renamed =
            |i: usize, o: usize| in_names[i] != ins[i].name || outs[o].name != ins[i].name;

        for i in (0..ins.len()).rev() {
            if expansions || seen[i].is_some() {
                continue;
            }
            let core = ins[i].core();
            // A read-back record holds whatever rate it reads; the input left its own out.
            let same_core = |o: usize| {
                let mut read = outs[o].core();
                if unweighted_rate(&ins[i]) {
                    read.0.as_object_mut().map(|object| object.remove("sample_rate"));
                }
                read == core
            };
            if let Some(o) = candidates(i, &used).into_iter().find(|&o| same_core(o)) {
                used[o] = true;
                claimed[i] = vec![o];
                seen[i] = Some(if renamed(i, o) { Class::Normalized } else { Class::Exact });
            }
        }

        for i in (0..ins.len()).rev() {
            if expansions || seen[i].is_some() {
                continue;
            }
            let input = &ins[i];
            let same: Vec<usize> = candidates(i, &used)
                .into_iter()
                .filter(|&o| outs[o].identity() == spelling.written_identity(input))
                .collect();
            let picked = match input.kind.as_str() {
                "samples" | "set_members" => gather(input, &same, outs),
                _ => same.into_iter().find(|&o| same_value(input, &outs[o])).into_iter().collect(),
            };
            if !picked.is_empty() {
                for &o in &picked {
                    used[o] = true;
                }
                claimed[i] = picked;
                seen[i] = Some(Class::Normalized);
            }
        }

        // Longer names first, so `latency.exp` claims `latency.exp.count` before `latency` could.
        let mut order: Vec<usize> = (0..ins.len()).collect();
        order.sort_by_key(|&i| (std::cmp::Reverse(ins[i].name.len()), std::cmp::Reverse(i)));
        for i in order {
            if !expansions || seen[i].is_some() {
                continue;
            }
            let input = &ins[i];
            let direct: Vec<usize> = candidates(i, &used)
                .into_iter()
                .filter(|&o| same_observations(input, &outs[o]))
                .collect();
            let sharing = (0..ins.len())
                .filter(|&j| {
                    seen[j].is_none()
                        && in_names[j] == in_names[i]
                        && ins[j].public() == input.public()
                })
                .count();
            let mut picked: Vec<usize> =
                if sharing == 1 { direct } else { direct.into_iter().take(1).collect() };
            // A dotted expansion hangs off the record's own name, before any respelling of the
            // whole record (`influxdb_out`'s `.value`).
            let base =
                if respelled && matches!(spelling, Spelling::Statsd { .. } | Spelling::Graphite) {
                    spelling.sanitize(&input.name)
                } else {
                    input.name.clone()
                };
            let prefix = format!("{base}.");
            let dotted: Vec<usize> = (0..outs.len())
                .filter(|&o| {
                    !used[o]
                        && !picked.contains(&o)
                        && outs[o].is_metric()
                        && input.is_metric()
                        && out_names[o].starts_with(&prefix)
                        && out_names[o] != in_names[i]
                        && score(input, &outs[o], spelling) > 0
                        && same_observations(input, &outs[o])
                })
                .collect();
            // Several points of one series each expand to one record per dotted name, so each
            // claims one of each.
            let mut names = BTreeSet::new();
            picked.extend(
                dotted.into_iter().filter(|&o| sharing == 1 || names.insert(&out_names[o])),
            );
            if !picked.is_empty() {
                for &o in &picked {
                    used[o] = true;
                }
                claimed[i] = picked;
                seen[i] = Some(Class::Degraded);
            }
        }
    }
    let unclaimed = (0..outs.len()).filter(|&o| !used[o]).collect();
    Pairing { seen, claimed, unclaimed }
}

/// The tags `datadog_out` puts on a series it derives from a record: a histogram's `.bucket`
/// bounds and a summary's `.quantile`, each from the bucket or quantile, not an attribute.
const DATADOG_EXPANSION_KEYS: [&str; 3] = ["lower_bound", "upper_bound", "quantile"];

/// 2 when `out`'s public attributes equal `input`'s as the sink spells their keys, 1 when
/// they're a subset, else 0. A `datadog_out` series under `<name>.` may also carry
/// [`DATADOG_EXPANSION_KEYS`].
fn score(input: &Rec, out: &Rec, spelling: Spelling) -> u8 {
    let (want, mut have) = (input.public_as(spelling), out.public());
    if spelling == Spelling::Datadog && out.name.starts_with(&format!("{}.", input.name)) {
        for key in DATADOG_EXPANSION_KEYS {
            if !want.contains_key(key) {
                have.remove(key);
            }
        }
    }
    if want == have {
        2
    } else if have.iter().all(|(k, v)| want.get(k) == Some(v)) {
        1
    } else {
        0
    }
}

/// Whether two records of one identity hold the same value, read as a number.
fn same_value(a: &Rec, b: &Rec) -> bool {
    match a.kind.as_str() {
        "sum" | "gauge" => same_number(&a.body["value"], &b.body["value"]),
        "gauge_delta" => same_number(&a.body["delta"], &b.body["delta"]),
        // A log pairs by its message, respelled where the sink respells it; its field rows
        // compare the rest.
        "log" => true,
        // A histogram whose last bound is finite gains an empty `+Inf` bucket on a wire that
        // states the total as one (Prometheus) or requires an overflow count (OTLP).
        "histogram" => {
            let (mut a, mut b) = (a.core().0, b.core().0);
            for body in [&mut a, &mut b] {
                if let Some(buckets) = body["buckets"].as_array_mut() {
                    let empty_inf =
                        |bucket: &Value| bucket["bound"] == "inf" && bucket["count"] == 0;
                    if buckets.last().is_some_and(empty_inf) {
                        buckets.pop();
                    }
                }
            }
            a == b
        }
        _ => a.core().0 == b.core().0,
    }
}

/// Whether `out` can stand for `input`'s observations in pass (3): when both are histograms, it
/// needs the same total count and, where both carry a sum, the same sum. A non-finite sum
/// matches only the same non-finite value, since a `NaN` or infinite observation is what made
/// it non-finite. Any other pair passes, so a histogram read back as dotted gauges still pairs.
fn same_observations(input: &Rec, out: &Rec) -> bool {
    match (histogram_count(input), histogram_count(out)) {
        (Some(want), Some(have)) => {
            want == have
                && match (input.body.get("sum"), out.body.get("sum")) {
                    (Some(a), Some(b)) => same_number(a, b),
                    _ => true,
                }
        }
        _ => true,
    }
}

/// A `histogram`'s bucket counts summed, or an `exponential_histogram`'s `count`; `None` for
/// any other kind.
fn histogram_count(rec: &Rec) -> Option<u128> {
    match rec.kind.as_str() {
        "histogram" => Some(
            rec.body["buckets"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|bucket| u128::from(bucket["count"].as_u64().unwrap_or(0)))
                .sum(),
        ),
        "exponential_histogram" => Some(u128::from(rec.body["count"].as_u64().unwrap_or(0))),
        _ => None,
    }
}

/// Read-back `samples` or `set_members` records whose values together are `input`'s, or none.
fn gather(input: &Rec, candidates: &[usize], outs: &[Rec]) -> Vec<usize> {
    let field = if input.kind == "samples" { "values" } else { "members" };
    // A member compares by its text with every character outside `[A-Za-z0-9._/-]` read as `_`,
    // since a sink may rewrite a reserved character. It's looser than any sink's set, and only
    // pairs members, never decides a row.
    let loose = |c: char| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-'));
    let values = |rec: &Rec| -> Vec<String> {
        rec.body[field]
            .as_array()
            .into_iter()
            .flatten()
            .map(|v| substituted(&v.to_string(), loose))
            .collect()
    };
    let mut want = values(input);
    let mut picked = Vec::new();
    for &o in candidates {
        if want.is_empty() {
            break;
        }
        if outs[o].body["sample_rate"] != input.body["sample_rate"] {
            continue;
        }
        let mut rest = want.clone();
        let fits = values(&outs[o])
            .iter()
            .all(|v| rest.iter().position(|w| w == v).map(|at| rest.remove(at)).is_some());
        if fits {
            want = rest;
            picked.push(o);
        }
    }
    if want.is_empty() {
        picked
    } else {
        Vec::new()
    }
}

/// The kind-level features `rec` reaches (module doc); `None` is the kind's one row.
fn kind_features(rec: &Rec) -> Vec<Option<String>> {
    let scalar = |key: &str| -> String {
        match &rec.body[key] {
            Value::Bool(_) => "bool",
            Value::Number(_) if rec.types.get(&format!("/{key}")).is_some_and(|t| t == "u64") => {
                "u64"
            }
            Value::Number(n) if n.is_f64() => "f64",
            Value::Number(_) => "i64",
            _ => "f64",
        }
        .to_string()
    };
    let temporality = || rec.body["temporality"].as_str().unwrap_or("").to_string();
    match rec.kind.as_str() {
        "sum" => {
            let monotonic = if rec.body["monotonic"] == Value::Bool(true) {
                "monotonic"
            } else {
                "nonmonotonic"
            };
            vec![Some(format!("{}_{monotonic}", temporality())), Some(scalar("value"))]
        }
        "gauge" => {
            let mut features = vec![Some(scalar("value"))];
            if number(&rec.body["value"]).is_some_and(|v| v < 0.0) {
                features.push(Some("negative".to_string()));
            }
            match metric_type(rec) {
                Some(TYPE_INFO) => features.push(Some("prometheus_info".to_string())),
                Some(TYPE_STATESET) => features.push(Some("prometheus_stateset".to_string())),
                _ => {}
            }
            features
        }
        "gauge_delta" => vec![Some(scalar("delta"))],
        // A record holding both a `NaN` and a `-Inf` bound reaches both rows.
        "histogram" => {
            let mut features = vec![Some(temporality())];
            let bounds: Vec<&str> = rec.body["buckets"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|bucket| bucket["bound"].as_str())
                .collect();
            if bounds.contains(&"NaN") {
                features.push(Some("nan_bound".to_string()));
            }
            if bounds.contains(&"-inf") {
                features.push(Some("neg_inf_bound".to_string()));
            }
            if metric_type(rec) == Some(TYPE_GAUGE_HISTOGRAM) {
                features.push(Some("gauge_histogram".to_string()));
            }
            features
        }
        "exponential_histogram" | "summary" => vec![Some(temporality())],
        "log" | "span" if rec.beside_metrics => vec![Some("beside_metrics".to_string())],
        "span" if datadog::otel_only(rec) => {
            vec![Some("alone".to_string()), Some("no_wire_form".to_string())]
        }
        "span" => vec![Some("alone".to_string())],
        "log" if rec.attributes.contains_key("_logit.statsd.event.title") => {
            vec![Some("dogstatsd_event".to_string())]
        }
        "log" => vec![Some("plain".to_string())],
        "event"
            if rec
                .attributes
                .get(ATTR_MEASUREMENT)
                .and_then(Value::as_str)
                .is_some_and(|m| !m.is_empty()) =>
        {
            vec![Some("string_only".to_string())]
        }
        "event" => vec![Some("attributes_only".to_string())],
        _ => vec![None],
    }
}

/// `rec`'s [`METRICS_TYPE`] flag, the event's value else the resource's, as
/// `logit_core::attrs::metric_type` reads it.
fn metric_type(rec: &Rec) -> Option<&str> {
    rec.attributes
        .get(METRICS_TYPE)
        .or_else(|| rec.resource.get(METRICS_TYPE))
        .and_then(Value::as_str)
}

/// The `attributes` row a [`METRICS_TYPE`] `value` on `rec` reaches, other than `rate`'s: `info`
/// and `stateset` name a gauge and `gaugehistogram` a histogram, and on another kind each is
/// ignored.
fn family_type_row(rec: &Rec, value: &Value) -> Option<&'static str> {
    let kind = match value.as_str()? {
        TYPE_INFO | TYPE_STATESET => "gauge",
        TYPE_GAUGE_HISTOGRAM => "histogram",
        _ => return None,
    };
    Some(if rec.kind == kind { "family_type" } else { "family_type_on_other_kind" })
}

/// `rec`'s metric record in the model, read back from its `format: json` body.
fn model_record(rec: &Rec) -> Option<logit_core::MetricRecord> {
    let line = serde_json::json!({"timestamp": rec.timestamp, "metrics": [rec.body]});
    let batches = EventJsonDecoder::new().decode_batches(line.to_string().as_bytes()).ok()?;
    batches.into_iter().next()?.events.into_iter().next()?.metrics.into_iter().next()
}

/// `rec`'s log record in the model, read back from its `format: json` body and type tags.
fn model_log(rec: &Rec) -> Option<logit_core::LogRecord> {
    let types: Map<String, Value> = rec
        .types
        .iter()
        .map(|(pointer, tag)| (format!("/log{pointer}"), Value::String(tag.clone())))
        .collect();
    let line = serde_json::json!({"timestamp": rec.timestamp, "log": rec.body, "types": types});
    let batches = EventJsonDecoder::new().decode_batches(line.to_string().as_bytes()).ok()?;
    batches.into_iter().next()?.events.into_iter().next()?.log
}

/// The record-level `log` features a log on an event with no metrics reaches beside its kind's
/// row: `message_unsafe` when the sink rewrites its message ([`Spelling::log_message`]),
/// `structured_message` for a `Map` or `Array` message, `bytes_message` for a `Bytes` one, and
/// `blank_message` for an empty message or none.
fn log_features(rec: &Rec, spelling: Spelling) -> Vec<&'static str> {
    let mut features = Vec::new();
    if rec.kind != "log" || rec.beside_metrics {
        return features;
    }
    let message = &rec.body["message"];
    if spelling.rendered_message(rec).is_some_and(|m| *message != Value::String(m)) {
        features.push("message_unsafe");
    }
    if let Spelling::Syslog { rfc3164: true, .. } = spelling {
        if syslog::read(rec, true).is_some_and(|read| read.boundary_moved) {
            features.push("header_in_message");
        }
    }
    if message.is_object() || message.is_array() {
        features.push("structured_message");
    }
    if rec.types.get("/message").is_some_and(|tag| tag == "bytes") {
        features.push("bytes_message");
    }
    if message.is_null() || *message == "" {
        features.push("blank_message");
    }
    features
}

/// The record-level `record` features `rec` reaches.
fn record_features(rec: &Rec, spelling: Spelling) -> Vec<&'static str> {
    let mut features = Vec::new();
    if !rec.is_metric() {
        return features;
    }
    if rec.body["no_recorded_value"] == Value::Bool(true) {
        features.push(match rec.kind.as_str() {
            "sum" if rec.body["temporality"] == "delta" => "no_recorded_value_delta_sum",
            "sum" | "gauge" => "no_recorded_value",
            _ => "no_recorded_value_other_kind",
        });
    }
    if rec.attributes.contains_key("_logit.statsd.service_check.name") {
        features.push("service_check");
    }
    if spelling.sanitize(&rec.name) != rec.name {
        features.push("name_unsafe");
    }
    if let Some(feature) = spelling.exemplar_key_unsafe(rec) {
        features.push(feature);
    }
    if rec.kind == "samples" && weight(rec) > 1 {
        features.push("sample_rate");
    }
    let non_finite = |value: &Value| matches!(value.as_str(), Some("NaN" | "inf" | "-inf"));
    if matches!(rec.kind.as_str(), "sum" | "gauge") && non_finite(&rec.body["value"]) {
        features.push("non_finite");
    }
    features
}

/// Whether `later` is another point of `rec`'s series: the same name, kind, and public attributes,
/// the resource's included.
fn same_series(rec: &Rec, later: &Rec) -> bool {
    later.is_metric()
        && later.name == rec.name
        && later.identity() == rec.identity()
        && later.public() == rec.public()
}

/// Whether `later`, another point of `rec`'s series, falls in the same millisecond.
fn same_millisecond(rec: &Rec, later: &Rec) -> bool {
    // An RFC 3339 timestamp through its milliseconds: `2027-01-15T08:00:00.000`.
    let millis = |rec: &Rec| rec.timestamp.get(..23).map(str::to_string);
    millis(rec).is_some() && millis(rec) == millis(later)
}

/// The `attributes` row `_logit.metrics.interval` reaches on metric record `rec`: datadog_out
/// writes the interval on its series routes, which have the field, and not on the
/// distribution-points and sketches routes, which don't. A delta histogram takes the sketches route
/// under one `histograms.mode` and the series route under the others, so it has a row of its own.
fn interval_row(rec: &Rec) -> &'static str {
    let delta = rec.body["temporality"] == "delta";
    match rec.kind.as_str() {
        "samples" | "distribution" => "interval_on_distribution",
        "exponential_histogram" if delta => "interval_on_distribution",
        "histogram" if delta => "interval_on_histogram",
        _ => "interval",
    }
}

/// Whether `rec` and `later`, two points of one series, are one state: each a kind a later
/// reading fully replaces ([`is_state`]), with the same metric object, attributes, and resource,
/// private carriers included.
fn same_reading(rec: &Rec, later: &Rec) -> bool {
    is_state(rec)
        && is_state(later)
        && rec.body == later.body
        && rec.attributes == later.attributes
        && rec.resource == later.resource
}

/// Whether a later reading of `rec`'s series fully replaces it: a `gauge`, or a cumulative `sum`,
/// `histogram`, `exponential_histogram`, or `summary`. Any other kind, a set included, is an
/// increment or an observation set, so two equal ones are two sets of events (`logit_proto`'s
/// `replaced_by_a_later_reading`), and so is a rate: a `gauge` whose
/// [`METRICS_TYPE`], the event's value else the resource's, is
/// `rate` (`logit_core::attrs::is_rate`).
fn is_state(rec: &Rec) -> bool {
    if rec.kind == "gauge" && metric_type(rec) == Some(TYPE_RATE) {
        return false;
    }
    match rec.kind.as_str() {
        "gauge" => true,
        "sum" | "histogram" | "exponential_histogram" | "summary" => {
            rec.body["temporality"] == "cumulative"
        }
        _ => false,
    }
}

/// Whether inputs `i` and `j`, which share an instant, read back at different ones.
fn moved_apart(pairing: &Pairing, i: usize, j: usize, outs: &[Rec]) -> bool {
    let (Some(&a), Some(&b)) = (pairing.claimed[i].first(), pairing.claimed[j].first()) else {
        return false;
    };
    outs[a].timestamp != outs[b].timestamp
}

/// Where a field row looks.
#[derive(Debug, Clone)]
enum Field {
    /// A key of the metric object.
    Body(&'static str),
    /// A key of the log object: `exact` when the read-back holds the same value, `normalized`
    /// when it holds another, `lost` when it holds none. A body format has no changed reading: a
    /// read-back format other than the input's is the pair decoder's default standing in for a
    /// format the wire has no slot for, so it reads `lost`. At `datadog_out`, a trace or span id
    /// read back as the attribute of its name instead reads `normalized`.
    LogBody(&'static str),
    /// A key of the log object the input doesn't set: `exact` when the read-back sets none
    /// either, `normalized` when the sink stamped one.
    LogUnset(&'static str),
    /// `event.timestamp`: `exact` read back the same, else `normalized`, a timestamp written at
    /// the sink's precision. A receipt time (`receipt`) read back outside its second is `lost`.
    Timestamp { receipt: bool },
    /// An event attribute, or a resource attribute.
    Attribute { key: String, resource: bool },
    /// The batch's scope.
    Scope,
    /// An attribute a sink derives from the record's own fields, by its row ([`derived_rows`]).
    Derived { feature: &'static str },
    /// A `samples` record's rate below 1 that rounds to weight 1 ([`unweighted_rate`]).
    SampleRate,
}

/// A `samples` record's weight, as `Samples::weight` rounds it: the inverse of its rate, 1 for a
/// rate that isn't a finite positive number.
fn weight(rec: &Rec) -> u64 {
    match number(&rec.body["sample_rate"]) {
        Some(rate) if rate.is_finite() && rate > 0.0 => ((1.0 / rate).round() as u64).max(1),
        _ => 1,
    }
}

/// Whether `rec` is a `samples` record whose rate is below 1 but rounds to weight 1, so a sink
/// that weights by the rate writes each value once: `record/sample_rate_unweighted`'s field,
/// which the record's own comparison leaves out.
fn unweighted_rate(rec: &Rec) -> bool {
    rec.kind == "samples"
        && number(&rec.body["sample_rate"]).is_some_and(|rate| rate < 1.0)
        && weight(rec) == 1
}

/// The field rows `rec` reaches, for a sink of `protocol`.
fn field_rows(rec: &Rec, protocol: &str) -> Vec<(String, Option<String>, Field)> {
    let mut rows = Vec::new();
    if rec.is_metric() {
        for key in mapping::RECORD_FIELDS {
            if rec.body.get(key).is_some() {
                let feature = match key {
                    "exemplars" if rec.kind == "summary" => "exemplars_on_summary",
                    "unit" if matches!(rec.kind.as_str(), "samples" | "distribution") => {
                        "unit_on_distribution"
                    }
                    key => key,
                };
                rows.push(("record".to_string(), Some(feature.to_string()), Field::Body(key)));
            }
        }
        if unweighted_rate(rec) {
            let feature = Some("sample_rate_unweighted".to_string());
            rows.push(("record".to_string(), feature, Field::SampleRate));
        }
        if matches!(rec.kind.as_str(), "histogram" | "exponential_histogram") {
            for key in ["min", "max"] {
                if rec.body.get(key).is_some() {
                    rows.push((
                        "record".to_string(),
                        Some("min_max".to_string()),
                        Field::Body(key),
                    ));
                }
            }
        }
    }
    if rec.kind == "log" {
        let has = |key: &str| rec.body.get(key).is_some();
        let mut push = |feature: &str, field: Field| {
            rows.push(("log".to_string(), Some(feature.to_string()), field));
        };
        if has("severity") {
            push("severity", Field::LogBody("severity"));
        } else {
            push("severity_unset", Field::LogUnset("severity"));
        }
        push("body_format", Field::LogBody("format"));
        if has("event_name") {
            push("event_name", Field::LogBody("event_name"));
        }
        if has("observed_timestamp") {
            push("observed_timestamp", Field::LogBody("observed_timestamp"));
        } else {
            push("observed_timestamp_unset", Field::LogUnset("observed_timestamp"));
        }
        if has("trace_id") {
            push("trace", Field::LogBody("trace_id"));
            if has("span_id") {
                push("trace", Field::LogBody("span_id"));
            }
            push("trace_flags", Field::LogBody("trace_flags"));
        } else {
            push("trace_unset", Field::LogUnset("trace_id"));
        }
        if has("dropped_attributes_count") {
            push("dropped_attributes_count", Field::LogBody("dropped_attributes_count"));
        }
        let receipt = rec.attributes.contains_key("syslog.timestamp")
            || rec.resource.contains_key("syslog.timestamp");
        let feature = if receipt { "receipt_timestamp" } else { "timestamp" };
        push(feature, Field::Timestamp { receipt });
    }
    let own = format!("_logit.{protocol}.");
    // A `_logit.datadog.*` carrier on a log, a Datadog event, or a service check: `datadog_out`'s
    // routes for those skip one they have no field for without counting it, where its metrics
    // routes count one.
    let json_route = rec.kind != "event"
        && matches!(
            datadog::Route::of(rec),
            datadog::Route::Log | datadog::Route::Event | datadog::Route::Check
        );
    let carrier = |key: &str, value: &Value| {
        if key == METRICS_CLOSED && value.as_str() == Some(CLOSED_TIMESTAMPED) {
            Some("timestamped")
        } else if key == METRICS_TYPE && value.as_str() == Some(TYPE_RATE) && rec.is_metric() {
            Some(if rec.kind == "gauge" { "rate" } else { "rate_on_non_gauge" })
        } else if key == METRICS_TYPE && rec.is_metric() && family_type_row(rec, value).is_some() {
            family_type_row(rec, value)
        } else if key == METRICS_INTERVAL && rec.is_metric() {
            Some(interval_row(rec))
        } else if key == ATTR_TARGET {
            Some("prometheus_target")
        } else if key.starts_with("_logit.otel.severity_") {
            // OTLP's raw severity, which `Severity` holds only at band resolution.
            Some("severity_carrier_otel")
        } else if key == syslog::SEVERITY {
            // syslog's PRI severity, which `Severity` holds only at band resolution.
            Some("severity_carrier_syslog")
        } else if key.starts_with("_logit.datadog.") && json_route {
            Some("datadog_carrier_log")
        } else if key.starts_with(&own) || (protocol == "influxdb" && is_field(key)) {
            // The field namespace isn't protocol-owned, so the own-prefix test misses it, and
            // `influxdb_out` is the one sink that writes it.
            Some("private_carrier")
        } else if key.starts_with("_logit.") {
            Some("foreign_carrier")
        } else {
            None
        }
    };
    // A log's own attribute `syslog_out` writes as a header field. On the resource it's a
    // resource attribute, which the header reads back from the event.
    let header = |key: &str| {
        if rec.kind != "log" || rec.beside_metrics {
            return None;
        }
        match key {
            syslog::FACILITY | syslog::TAG | syslog::PID | logit_core::attrs::PROCESS_PID => {
                Some("syslog_header")
            }
            syslog::MSGID => Some("syslog_msgid"),
            syslog::SD => Some("syslog_sd"),
            syslog::TIMESTAMP => Some("syslog_timestamp"),
            logit_core::attrs::HOST_NAME => Some("host_name"),
            _ => None,
        }
    };
    // A host attribute a sink leaves off for the record's own host: `statsd_out`'s `host` beside
    // the line's host, and `datadog_out`'s log `hostname`, and its log `service` for the log's
    // `service.name`.
    let outranked = |key: &str, value: &Value| {
        if key == "host" && statsd::host_is_omitted(rec, value) {
            Some("host_beside_host_name")
        } else if key == "hostname" && datadog::reserved_log_key_is_omitted(rec, key) {
            Some("hostname")
        } else if key == "service" && datadog::reserved_log_key_is_omitted(rec, key) {
            Some("log_service")
        } else if key == "service" && datadog::log_service_is_the_field(rec) {
            Some("log_service_field")
        } else if key == logit_core::attrs::CONTAINER_ID && value.is_string() {
            // The standard key a sink writes in its own wire's form: `statsd_out`'s `|c:`,
            // `datadog_out`'s `container_id` tag.
            Some("container_id")
        } else {
            None
        }
    };
    // An event's service identity: a standard key, or a valued attribute of a Unified Service
    // Tagging tag's name, alone or beside its key. On the resource, each is a resource attribute.
    let service = |key: &str, value: &Value| {
        if value.is_string()
            && logit_core::attrs::UNIFIED_SERVICE_TAGS.iter().any(|(_, standard)| key == *standard)
        {
            Some("service_identity")
        } else if unified::beside_its_key(rec, key, value) {
            Some("service_tag_beside_key")
        } else if value.is_string() && unified::key(key, value).is_some() {
            Some("service_tag")
        } else {
            None
        }
    };
    for (key, value) in &rec.attributes {
        let tags = rec.attribute_types.get(key).map(String::as_str).unwrap_or("");
        let feature = if let Some(feature) = carrier(key, value) {
            feature
        } else if let Some(feature) = header(key) {
            feature
        } else if let Some(feature) = outranked(key, value) {
            feature
        } else if let Some(feature) = service(key, value) {
            feature
        } else if tags.contains("=bytes") {
            "bytes_value"
        } else if tags.contains("=timestamp") {
            "timestamp_value"
        } else {
            match value {
                Value::String(_) => "string_value",
                Value::Bool(_) => "bool_value",
                Value::Number(_) => "number_value",
                Value::Array(_) => "array_value",
                Value::Object(_) => "map_value",
                Value::Null => "null_value",
            }
        };
        let field = Field::Attribute { key: key.clone(), resource: false };
        rows.push(("attributes".to_string(), Some(feature.to_string()), field));
    }
    for (key, value) in &rec.resource {
        let feature =
            carrier(key, value).or_else(|| outranked(key, value)).unwrap_or("resource_attribute");
        let field = Field::Attribute { key: key.clone(), resource: true };
        rows.push(("attributes".to_string(), Some(feature.to_string()), field));
    }
    if rec.scope.is_some() {
        rows.push(("attributes".to_string(), Some("scope".to_string()), Field::Scope));
    }
    for feature in derived_rows(rec) {
        let field = Field::Derived { feature };
        rows.push(("attributes".to_string(), Some(feature.to_string()), field));
    }
    rows
}

/// What a field reads back as in `out`.
fn observe_field(field: &Field, input: &Rec, out: &Rec, spelling: Spelling) -> Class {
    match field {
        Field::Scope => match &out.scope {
            Some(scope) if Some(scope) == input.scope.as_ref() => Class::Exact,
            Some(_) => Class::Normalized,
            None => Class::Lost,
        },
        // An attribute the input lacks, read back with the value derived for it, is the sink's
        // rewrite of a field the record already holds.
        Field::Derived { feature } => {
            // A sink that derives nothing for the row reads back only the record's own
            // attributes under the key, such as `statsd_out`'s `host` read back as `host.name`.
            let Some((_, key, value)) =
                spelling.derived(input).into_iter().find(|(row, _, _)| *row == Some(*feature))
            else {
                return Class::Exact;
            };
            // A span's derived host is its tracer payload's, on the resource, ahead of any
            // `host.name` span tag; every other derived field is the record's own.
            let span_host = key == "host.name" && datadog::Route::of(input) == datadog::Route::Span;
            let read = if span_host {
                out.resource.get(key).or(out.attributes.get(key))
            } else {
                out.attributes.get(key).or(out.resource.get(key))
            };
            match read {
                Some(read) if text(read) == value => Class::Normalized,
                _ => Class::Lost,
            }
        }
        Field::LogBody(key) => match out.body.get(*key) {
            Some(value) if *value == input.body[*key] => Class::Exact,
            Some(_) if *key == "format" => Class::Lost,
            Some(_) => Class::Normalized,
            None if spelling == Spelling::Datadog
                && matches!(*key, "trace_id" | "span_id")
                && out.attributes.get(*key) == Some(&input.body[*key]) =>
            {
                Class::Normalized
            }
            None => Class::Lost,
        },
        Field::LogUnset(key) => match out.body.get(*key) {
            Some(_) => Class::Normalized,
            None => Class::Exact,
        },
        // `syslog_in` stamps `event.timestamp` with its receipt time, so the instant written is the
        // read-back `syslog.timestamp`, and it's the event's only where the encoder wrote that.
        Field::Timestamp { .. } if matches!(spelling, Spelling::Syslog { .. }) => {
            let Spelling::Syslog { rfc3164, .. } = spelling else { return Class::Lost };
            match syslog::written_timestamp(input, rfc3164) {
                Some((written, false))
                    if out.attributes.get(syslog::TIMESTAMP) == Some(&written) =>
                {
                    if written == Value::String(input.timestamp.clone()) {
                        Class::Exact
                    } else {
                        Class::Normalized
                    }
                }
                _ => Class::Lost,
            }
        }
        // A receipt time read back outside its own second is another instant, which no
        // precision explains.
        Field::Timestamp { receipt } => {
            let second = |rec: &Rec| rec.timestamp.get(..19).map(str::to_string);
            if out.timestamp == input.timestamp {
                Class::Exact
            } else if *receipt && second(out) != second(input) {
                Class::Lost
            } else {
                Class::Normalized
            }
        }
        // A rate read back as 1 is the rate gone.
        Field::SampleRate => {
            let read = number(&out.body["sample_rate"]);
            match (read, number(&input.body["sample_rate"])) {
                (Some(read), Some(want)) if read == want => Class::Exact,
                (Some(1.0), _) => Class::Lost,
                (Some(_), _) => Class::Normalized,
                (None, _) => Class::Lost,
            }
        }
        Field::Body(key) => match out.body.get(*key) {
            Some(value) if *value == input.body[*key] => Class::Exact,
            Some(_) => Class::Normalized,
            None => Class::Lost,
        },
        Field::Attribute { key, resource } => {
            if matches!(spelling, Spelling::Prometheus) {
                // A relay re-targets: a scrape of `prometheus_out` stamps its own URL, and the
                // original target survives as the `instance` label.
                if *resource && key == ATTR_TARGET {
                    return match (
                        either_side(out, "instance", false),
                        input.resource.get("instance"),
                    ) {
                        (Some(read), Some(want)) if read == want => Class::Normalized,
                        _ => Class::Lost,
                    };
                }
            }
            let (want, want_tags) = if *resource {
                (&input.resource[key], input.resource_types.get(key))
            } else {
                (&input.attributes[key], input.attribute_types.get(key))
            };
            // A host attribute the sink leaves off for the record's own host: what reads back
            // under its key is that host.
            let outranked = match spelling {
                Spelling::Datadog => datadog::reserved_log_key_is_omitted(input, key),
                Spelling::Statsd { dogstatsd: true } => {
                    key == "host" && statsd::host_is_omitted(input, want)
                }
                _ => false,
            } || unified::tag_is_omitted(spelling, input, key, want);
            if outranked {
                return Class::Lost;
            }
            // The v2 series route spells the hostless mark as a `host` resource with no name,
            // which `datadog_in` reads into `_logit.datadog.resources`.
            if spelling == Spelling::Datadog
                && key == statsd::HOSTLESS
                && either_side(out, key, *resource).is_none()
                && datadog::hostless_mark(out)
            {
                return Class::Normalized;
            }
            // `syslog_out` writes `event.timestamp` where it can't render `syslog.timestamp`, so
            // what reads back is another instant.
            if let Spelling::Syslog { rfc3164, .. } = spelling {
                if key == syslog::TIMESTAMP && !*resource {
                    if let Some((_, false)) = syslog::written_timestamp(input, rfc3164) {
                        return Class::Lost;
                    }
                }
            }
            // A key the sink respells reads back under its wire spelling, which is a change.
            let wire = spelling.key_for(key, input);
            let respelled = wire != *key;
            // A key respelled into a private carrier (`statsd_out`'s empty `host`, read back as the
            // hostless mark) is outside every public comparison, so its value is compared here.
            if respelled && wire.starts_with("_logit.") {
                let found = either_side(out, &wire, *resource);
                // An empty `host` read back as the hostless mark says the same thing.
                let hostless = wire == statsd::HOSTLESS && found == Some(&Value::Bool(true));
                return match found {
                    Some(read) if text(read) == text(want) || hostless => Class::Normalized,
                    _ => Class::Lost,
                };
            }
            let key = &wire;
            let (same, same_tags, other) = if *resource {
                (out.resource.get(key), out.resource_types.get(key), out.attributes.get(key))
            } else {
                (out.attributes.get(key), out.attribute_types.get(key), out.resource.get(key))
            };
            match (same, other) {
                (Some(value), _) if !respelled && value == want && same_tags == want_tags => {
                    Class::Exact
                }
                (Some(_), _) | (None, Some(_)) => Class::Normalized,
                (None, None) => Class::Lost,
            }
        }
    }
}

/// `out`'s attribute `key`, on the side `resource` names first, else on the other.
fn either_side<'a>(out: &'a Rec, key: &str, resource: bool) -> Option<&'a Value> {
    let (same, other) = if resource {
        (out.resource.get(key), out.attributes.get(key))
    } else {
        (out.attributes.get(key), out.resource.get(key))
    };
    same.or(other)
}

/// Whether a field observed as `seen` fits `cell`, and its counter moved if it's lost.
fn field_verdict(
    here: &str,
    input: &Rec,
    label: &str,
    cell: &mapping::Cell,
    seen: Class,
    did_move: &impl Fn(&str) -> bool,
) -> Vec<String> {
    let fits = if cell.class == Class::Exact {
        seen == Class::Exact
    } else {
        field_rank(seen) <= field_rank(cell.class)
    };
    if !fits {
        return vec![format!(
            "{here}: {} {:?}: the table's row {label} says {}, the flow reads {}\n  sink input: {}",
            input.kind,
            input.name,
            cell.class.as_str(),
            seen.as_str(),
            input.show()
        )];
    }
    match (cell.class, seen, &cell.counter) {
        (Class::Lost, Class::Lost, Some(counter)) if !did_move(counter) => vec![format!(
            "{here}: {} {:?} is lost under row {label}, but its counter {counter} didn't move",
            input.kind, input.name
        )],
        _ => Vec::new(),
    }
}

/// A record-level order: `exact < normalized < degraded < skipped < dropped`.
fn worse(a: Class, b: Class) -> Class {
    let rank = |c: Class| match c {
        Class::Exact => 0,
        Class::Normalized => 1,
        Class::Degraded => 2,
        Class::Lost => 3,
        Class::Skipped => 4,
        Class::Dropped => 5,
    };
    if rank(b) > rank(a) {
        b
    } else {
        a
    }
}

/// A field-level order: `exact < normalized < lost`.
fn field_rank(class: Class) -> u8 {
    match class {
        Class::Exact => 0,
        Class::Normalized => 1,
        _ => 2,
    }
}

/// Where one flow and one case stand, for messages.
pub(super) struct At<'a> {
    pub flow: &'a str,
    pub case: &'a str,
}

/// What the checked runs have seen of each cell (module doc).
#[derive(Default)]
pub(super) struct Ledger {
    /// Every `(sink, row, cell index)` some record was checked against, and the classes observed
    /// under it, empty for a cell reached with no class observed.
    cells: BTreeMap<(String, String, usize), BTreeSet<Class>>,
    /// Rows a sink has no cell in yet, and the runs that reached them.
    unspecified: BTreeMap<(String, String), BTreeSet<String>>,
}

impl Ledger {
    fn reach(&mut self, sink: &str, row: &str, index: usize) -> &mut BTreeSet<Class> {
        self.cells.entry((sink.to_string(), row.to_string(), index)).or_default()
    }

    fn saw(&mut self, sink: &str, row: &str, index: usize, observed: Class) {
        self.reach(sink, row, index).insert(observed);
    }

    /// One failure per `normalized` or `lost` cell of `mapping` observed, but never at its class,
    /// and one per cell the ledger names that `mapping` doesn't have.
    pub(super) fn finish(&self, mapping: &Mapping) -> Vec<String> {
        let classes: BTreeMap<(String, String, usize), Class> = mapping
            .cells()
            .into_iter()
            .map(|(sink, row, index, cell)| ((sink, row, index), cell.class))
            .collect();
        let mut failures = Vec::new();
        for (key, observed) in &self.cells {
            let (sink, row, index) = key;
            let Some(class) = classes.get(key) else {
                failures.push(format!(
                    "mapping row {row}, {sink} cell {index}: a ledger file names the cell, which \
                     mapping.yaml doesn't have; the ledger file is stale"
                ));
                continue;
            };
            // A cell reached with no class observed was compared through another cell.
            if observed.is_empty() {
                continue;
            }
            if matches!(class, Class::Normalized | Class::Lost) && !observed.contains(class) {
                let observed: Vec<&str> = observed.iter().map(|c| c.as_str()).collect();
                failures.push(format!(
                    "mapping row {row}, {sink} cell {index}: {} in the table, but every record \
                     that reached it read {observed:?}; the table may be stale",
                    class.as_str()
                ));
            }
        }
        failures
    }

    /// Per sink, each cell of `mapping` no record was checked against: its row, its index among
    /// the sink's cells in the row, and its `when:`.
    pub(super) fn unreached(
        &self,
        mapping: &Mapping,
    ) -> BTreeMap<String, Vec<(String, usize, String)>> {
        let mut out: BTreeMap<String, Vec<(String, usize, String)>> = BTreeMap::new();
        for (sink, row, index, cell) in mapping.cells() {
            if !self.cells.contains_key(&(sink.clone(), row.clone(), index)) {
                let when = if cell.when.is_empty() {
                    "otherwise".to_string()
                } else {
                    serde_json::to_string(&cell.when).unwrap_or_default()
                };
                out.entry(sink).or_default().push((row, index, when));
            }
        }
        out
    }

    /// The rows each sink hasn't specified yet, one line each.
    pub(super) fn unspecified(&self) -> Vec<String> {
        self.unspecified
            .iter()
            .map(|((sink, row), cases)| {
                let cases: Vec<&str> = cases.iter().map(String::as_str).collect();
                format!("{sink} has no cell for row {row} yet (reached by {})", cases.join(", "))
            })
            .collect()
    }

    /// What this ledger holds, sorted, as a ledger file stores it.
    pub(super) fn fragment(&self) -> Fragment {
        let cells = self
            .cells
            .iter()
            .map(|((sink, row, cell), observed)| FragmentCell {
                sink: sink.clone(),
                row: row.clone(),
                cell: *cell,
                observed: observed.iter().copied().collect(),
            })
            .collect();
        let unspecified = self
            .unspecified
            .iter()
            .map(|((sink, row), runs)| FragmentRow {
                sink: sink.clone(),
                row: row.clone(),
                reached_by: runs.iter().cloned().collect(),
            })
            .collect();
        Fragment { cells, unspecified }
    }

    /// Adds what `fragment` saw to what this ledger has seen.
    pub(super) fn merge(&mut self, fragment: Fragment) {
        for cell in fragment.cells {
            self.reach(&cell.sink, &cell.row, cell.cell).extend(cell.observed);
        }
        for row in fragment.unspecified {
            self.unspecified.entry((row.sink, row.row)).or_default().extend(row.reached_by);
        }
    }
}

/// What one flow's runs fed a [`Ledger`], as `spec/transit/ledger/<flow>.json` holds it. It stores
/// no cell's class, which the merged ledger takes from the current table.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fragment {
    cells: Vec<FragmentCell>,
    unspecified: Vec<FragmentRow>,
}

/// A cell some record was checked against, and the classes observed under it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FragmentCell {
    sink: String,
    row: String,
    cell: usize,
    observed: Vec<Class>,
}

/// A row a sink has no cell in yet, and the runs (`<flow> × <case>`) that reached it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FragmentRow {
    sink: String,
    row: String,
    reached_by: Vec<String>,
}

impl Fragment {
    /// The ledger file's text: one entry per line, so a diff names the cell that changed.
    pub(super) fn render(&self) -> String {
        fn list<T: Serialize>(entries: &[T]) -> String {
            if entries.is_empty() {
                return "[]".to_string();
            }
            let lines: Vec<String> = entries
                .iter()
                .map(|e| format!("    {}", serde_json::to_string(e).expect("an entry serializes")))
                .collect();
            format!("[\n{}\n  ]", lines.join(",\n"))
        }
        format!(
            "{{\n  \"cells\": {},\n  \"unspecified\": {}\n}}\n",
            list(&self.cells),
            list(&self.unspecified)
        )
    }
}

/// A sink series a run moved, from a document's `counters` key.
fn moved(traced: &Traced) -> Vec<Series> {
    traced
        .outcome
        .counters
        .keys()
        .filter_map(|key| Series::parse(key).ok())
        .filter(|series| series.tags.get("role").is_some_and(|role| role == "sink"))
        .filter(|series| OUTCOMES.iter().any(|suffix| series.name.ends_with(&format!(".{suffix}"))))
        .collect()
}

/// Checks one run against the table (module doc), returning one failure per disagreement.
pub(super) fn check(
    mapping: &Mapping,
    at: &At<'_>,
    traced: &Traced,
    ledger: &mut Ledger,
) -> Vec<String> {
    let sink = traced.sink["type"].as_str().unwrap_or("").to_string();
    let protocol = sink.strip_suffix("_out").unwrap_or(&sink).to_string();
    let ins = records(&traced.sunk);
    let outs = records(&traced.decoded);
    let moved = moved(traced);
    let did_move = |counter: &str| {
        Series::parse(counter).is_ok_and(|cell| moved.iter().any(|series| cell.names(series)))
    };
    let spelling = Spelling::of(&sink, &traced.sink);
    let pairing = pair(&ins, &outs, spelling);
    let mut failures = mapping.unknown_when_keys(&sink, &traced.sink);
    let mut named: Vec<Series> = Vec::new();
    let here = format!("{} × {}", at.flow, at.case);

    for (i, input) in ins.iter().enumerate() {
        // A record reaches its field rows whether or not it's read back, and a sink may move a
        // field row's counter before it skips the record.
        for (kind, feature, _) in field_rows(input, &protocol) {
            if let Lookup::Cell(cell, _) =
                mapping.lookup(&kind, feature.as_deref(), &sink, &traced.sink)
            {
                named.extend(cell.counter.as_deref().and_then(|c| Series::parse(c).ok()));
            }
        }

        // A log or span beside metrics is a field of its event (module doc).
        let beside = !input.is_metric() && input.beside_metrics;
        let observed = if beside {
            let label = mapping::row_label(&input.kind, Some("beside_metrics"));
            match mapping.lookup(&input.kind, Some("beside_metrics"), &sink, &traced.sink) {
                Lookup::NoRow => failures.push(format!(
                    "{here}: {} {:?} reaches row {label}, which mapping.yaml doesn't have",
                    input.kind, input.name
                )),
                Lookup::NotSpecified => {
                    ledger
                        .unspecified
                        .entry((sink.clone(), label))
                        .or_default()
                        .insert(here.clone());
                }
                Lookup::Cell(cell, index) => {
                    named.extend(cell.counter.as_deref().and_then(|c| Series::parse(c).ok()));
                    let seen = match pairing.seen[i] {
                        Some(Class::Exact) => Class::Exact,
                        Some(_) => Class::Normalized,
                        None => Class::Lost,
                    };
                    failures.extend(field_verdict(&here, input, &label, cell, seen, &did_move));
                    ledger.saw(&sink, &label, index, seen);
                }
            }
            match pairing.seen[i] {
                Some(Class::Exact) => Class::Exact,
                Some(_) => Class::Normalized,
                None => continue,
            }
        } else {
            // Record level.
            let mut rows: Vec<(String, Option<String>)> =
                kind_features(input).into_iter().map(|f| (input.kind.clone(), f)).collect();
            rows.extend(
                log_features(input, spelling)
                    .into_iter()
                    .map(|feature| ("log".to_string(), Some(feature.to_string()))),
            );
            let mut features = record_features(input, spelling);
            let later: Vec<usize> = (i + 1..ins.len())
                .filter(|&j| input.is_metric() && same_series(input, &ins[j]))
                .collect();
            if !later.is_empty() {
                features.push("superseded");
            }
            // The later point of its series at this record's instant, if any.
            let twin = later.iter().copied().find(|&j| ins[j].timestamp == input.timestamp);
            // `datadog_out` collapses a series' points in one second on its series route only. A
            // `samples` record has the distribution-points route, where Datadog keeps both points, a
            // `distribution` record the sketches route, and a `set_members` record no Datadog form; the
            // sketches route also carries every `exponential_histogram`, and a `histogram` under the
            // default `histograms.mode: distributions`, unless `histograms.send_aggregation_metrics`
            // also writes its `.count`, `.sum`, `.min`, and `.max` on the series route.
            let histograms = &traced.sink["histograms"];
            let aggregations = histograms["send_aggregation_metrics"].as_bool().unwrap_or(false);
            let distributions =
                histograms["mode"].as_str().is_none_or(|mode| mode == "distributions");
            let sketched = !aggregations
                && (input.kind == "exponential_histogram"
                    || (input.kind == "histogram" && distributions));
            let collapsible = sink != "datadog_out"
                || !(matches!(input.kind.as_str(), "samples" | "distribution" | "set_members")
                    || sketched);
            // The point a sink keeping one per series and millisecond sends in this record's place:
            // the latest, the later in the run on a tie.
            let sent = later
                .iter()
                .copied()
                .filter(|&j| same_millisecond(input, &ins[j]))
                .max_by(|&a, &b| ins[a].timestamp.cmp(&ins[b].timestamp).then(a.cmp(&b)));
            let equal = sent.filter(|&j| same_reading(input, &ins[j]));
            if twin.is_some() && collapsible {
                features.push(if equal.is_some() { "same_instant_equal" } else { "same_instant" });
            } else if twin.is_none() && collapsible && sent.is_some() {
                features.push(if equal.is_some() {
                    "same_millisecond_equal"
                } else {
                    "same_millisecond"
                });
            }
            rows.extend(features.into_iter().map(|f| ("record".to_string(), Some(f.to_string()))));
            let mut cells = Vec::new();
            let mut unspecified = false;
            for (kind, feature) in &rows {
                let label = mapping::row_label(kind, feature.as_deref());
                match mapping.lookup(kind, feature.as_deref(), &sink, &traced.sink) {
                    Lookup::NoRow => failures.push(format!(
                        "{here}: {} {:?} reaches row {label}, which mapping.yaml doesn't have",
                        input.kind, input.name
                    )),
                    Lookup::NotSpecified => {
                        unspecified = true;
                        ledger
                            .unspecified
                            .entry((sink.clone(), label))
                            .or_default()
                            .insert(here.clone());
                    }
                    Lookup::Cell(cell, index) => {
                        if let Some(counter) = &cell.counter {
                            named.extend(Series::parse(counter).ok());
                        }
                        cells.push((label, cell, index));
                    }
                }
            }
            if unspecified || cells.len() < rows.len() {
                continue;
            }
            for (label, _, index) in &cells {
                ledger.reach(&sink, label, *index);
            }
            // A sink that tests the flag before the kind skips or stale-marks a flagged record without
            // reaching the kind's loss, so a counted loss cell whose counter didn't move doesn't
            // count against it (module doc's "Counters").
            let flagged =
                cells.iter().any(|(label, _, _)| label.starts_with("record/no_recorded_value"));
            let cells: Vec<_> = cells
                .into_iter()
                .filter(|(label, cell, _)| {
                    !flagged
                        || label.starts_with("record/no_recorded_value")
                        || !matches!(cell.class, Class::Degraded | Class::Skipped | Class::Dropped)
                        || cell.counter.as_deref().is_none_or(&did_move)
                })
                .collect();
            let expected = cells.iter().map(|(_, cell, _)| cell.class).fold(Class::Exact, worse);
            // A sink skips or drops a record once and counts it once, under whichever reason it meets
            // first, so of several cells at the worst class the one whose counter moved decides.
            let worst: Vec<_> =
                cells.iter().filter(|(_, cell, _)| cell.class == expected).collect();
            let deciding = worst
                .iter()
                .find(|(_, cell, _)| cell.counter.as_deref().is_some_and(&did_move))
                .or(worst.first())
                .copied();
            let observed = match pairing.seen[i] {
                // Two points sharing an instant that read back at different instants were moved
                // apart, which only a timestamp comparison shows: the check compares no other.
                Some(Class::Exact) if twin.is_some_and(|j| moved_apart(&pairing, i, j, &outs)) => {
                    Some(Class::Normalized)
                }
                Some(class) => Some(class),
                None => {
                    let loss = deciding.filter(|(_, cell, _)| {
                        matches!(cell.class, Class::Skipped | Class::Dropped)
                    });
                    let counted = loss
                        .and_then(|(_, cell, _)| cell.counter.as_deref())
                        .is_some_and(&did_move);
                    // A cell that says its loss is uncounted accounts for a record no counter does.
                    let uncounted = loss.is_some_and(|(_, cell, _)| {
                        cell.counter.is_none() && cell.reason.as_deref() == Some("uncounted")
                    });
                    // A sink that collapses two equal readings counts the one it removes under a
                    // `normalized` cell and sends the other, so the removed one reads as the other
                    // does: at its worst cell, which is that one unless the kind is written in a
                    // lesser form.
                    let collapsed = cells.iter().any(|(_, cell, _)| {
                        cell.class == Class::Normalized
                            && cell.counter.as_deref().is_some_and(&did_move)
                    }) && equal.is_some_and(|j| !pairing.claimed[j].is_empty());
                    if counted || uncounted || collapsed {
                        Some(expected)
                    } else if matches!(expected, Class::Normalized | Class::Degraded)
                        && sink == "prometheus_out"
                        && traced.sink.get("bind").is_some_and(|bind| !bind.is_null())
                        && later.iter().any(|&j| !pairing.claimed[j].is_empty())
                    {
                        // The exposition registry keeps one value per series, so a record whose
                        // series has a later record read back was overwritten by it: the pull
                        // model's normalization, which leaves the earlier record nothing to pair
                        // with. A kind the sink writes in a lesser form stays at that class.
                        Some(expected)
                    } else if moved.iter().any(|s| s.name.ends_with(".skipped")) {
                        Some(Class::Skipped)
                    } else if moved.iter().any(|s| s.name.ends_with(".dropped")) {
                        Some(Class::Dropped)
                    } else {
                        None
                    }
                }
            };
            let read_back: Vec<String> =
                pairing.claimed[i].iter().map(|&o| outs[o].show()).collect();
            let reached: Vec<String> = cells
                .iter()
                .map(|(label, cell, _)| format!("{label}: {}", cell.class.as_str()))
                .collect();
            let Some(observed) = observed else {
                failures.push(format!(
                "{here}: {} {:?} isn't read back and no sink counter says why; the table expects \
                 {} ({})\n  sink input: {}",
                input.kind,
                input.name,
                expected.as_str(),
                reached.join(", "),
                input.show()
            ));
                continue;
            };
            let agrees =
                observed == expected || (expected == Class::Normalized && observed == Class::Exact);
            if !agrees {
                failures.push(format!(
                "{here}: {} {:?}: the table expects {} ({}), the flow reads {}\n  sink input: {}\n  \
                 read back: [{}]",
                input.kind,
                input.name,
                expected.as_str(),
                reached.join(", "),
                observed.as_str(),
                input.show(),
                read_back.join(", ")
            ));
                continue;
            }
            if let Some((label, cell, index)) = deciding {
                ledger.saw(&sink, label, *index, observed);
                let loss = matches!(cell.class, Class::Degraded | Class::Skipped | Class::Dropped);
                if let (true, Some(counter)) = (loss && observed == cell.class, &cell.counter) {
                    if !did_move(counter) {
                        failures.push(format!(
                        "{here}: {} {:?} reads {} under row {label}, but its counter {counter} \
                         didn't move",
                        input.kind,
                        input.name,
                        observed.as_str()
                    ));
                    }
                }
            }
            for (label, cell, index) in &cells {
                if *label != deciding.map(|d| d.0.clone()).unwrap_or_default()
                    && cell.class == observed
                {
                    ledger.saw(&sink, label, *index, observed);
                }
            }
            observed
        };

        // Field level. A log beside metrics reaches its `log` rows; its attributes are its
        // event's, judged on the event's metric records.
        let first = pairing.claimed[i].first().copied();
        let checked = matches!(observed, Class::Exact | Class::Normalized);
        for (kind, feature, field) in field_rows(input, &protocol) {
            if beside && kind != "log" {
                continue;
            }
            // A field row is reached only where its record is compared: a record written in a
            // lesser form, or not at all, has no field to compare.
            let Some(first) = first.filter(|_| checked) else { continue };
            let label = mapping::row_label(&kind, feature.as_deref());
            let (cell, index) = match mapping.lookup(&kind, feature.as_deref(), &sink, &traced.sink)
            {
                Lookup::NoRow => {
                    failures.push(format!(
                        "{here}: {} {:?} reaches row {label} ({field:?}), which mapping.yaml \
                         doesn't have",
                        input.kind, input.name
                    ));
                    continue;
                }
                Lookup::NotSpecified => {
                    ledger
                        .unspecified
                        .entry((sink.clone(), label))
                        .or_default()
                        .insert(here.clone());
                    continue;
                }
                Lookup::Cell(cell, index) => (cell, index),
            };
            let seen = observe_field(&field, input, &outs[first], spelling);
            let fits = if cell.class == Class::Exact {
                seen == Class::Exact
            } else {
                field_rank(seen) <= field_rank(cell.class)
            };
            if !fits {
                failures.push(format!(
                    "{here}: {} {:?}, {field:?}: the table's row {label} says {}, the flow reads {}\n  \
                     sink input: {}\n  read back: {}",
                    input.kind,
                    input.name,
                    cell.class.as_str(),
                    seen.as_str(),
                    input.show(),
                    outs[first].show()
                ));
                continue;
            }
            ledger.saw(&sink, &label, index, seen);
            if let (Class::Lost, Class::Lost, Some(counter)) = (cell.class, seen, &cell.counter) {
                if !did_move(counter) {
                    failures.push(format!(
                        "{here}: {} {:?}, {field:?} is lost under row {label}, but its counter \
                         {counter} didn't move",
                        input.kind, input.name
                    ));
                }
            }
        }
    }

    for &o in &pairing.unclaimed {
        failures.push(format!(
            "{here}: read back with no sink input to account for it: {}\n  sink input: [{}]",
            outs[o].show(),
            ins.iter().map(Rec::show).collect::<Vec<_>>().join(", ")
        ));
    }
    for series in &moved {
        if !named.iter().any(|cell| cell.names(series)) {
            failures.push(format!(
                "{here}: {}{:?} moved, but no cell the case reaches names it",
                series.name, series.tags
            ));
        }
    }
    failures
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The records of `format: json` lines, each holding `{T}` for a fixed timestamp.
    fn recs(lines: &[&str]) -> Vec<Rec> {
        let text: String = lines
            .iter()
            .map(|line| line.replace("{T}", "\"timestamp\":\"2027-01-15T08:00:00.000000000Z\""))
            .map(|line| format!("{line}\n"))
            .collect();
        records(&EventJsonDecoder::new().decode_batches(text.as_bytes()).expect("JSON lines"))
    }

    /// `prometheus_out` writes a dotted name and label key with `_`, and a counter's sample with
    /// `_total`; read as written, nothing pairs.
    #[test]
    fn prometheus_pairs_a_sanitized_name_and_key_and_a_total_suffix() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"cpu.usage","kind":"gauge","value":1.5}],"attributes":{"deploy.note":"x"}}"#,
            r#"{{T},"metrics":[{"name":"jobs","kind":"sum","value":3.0,"temporality":"cumulative","monotonic":true}]}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"cpu_usage","kind":"gauge","value":1.5}],"attributes":{"deploy_note":"x"}}"#,
            r#"{{T},"metrics":[{"name":"jobs_total","kind":"sum","value":3.0,"temporality":"cumulative","monotonic":true}]}"#,
        ]);
        let pairing = pair(&ins, &outs, Spelling::Prometheus);
        assert_eq!(pairing.seen, [Some(Class::Normalized), Some(Class::Normalized)]);
        assert!(pairing.unclaimed.is_empty());
        let note = Field::Attribute { key: "deploy.note".to_string(), resource: false };
        assert_eq!(
            observe_field(&note, &ins[0], &outs[0], Spelling::Prometheus),
            Class::Normalized
        );

        let verbatim = pair(&ins, &outs, Spelling::Verbatim);
        assert_eq!(verbatim.seen, [None, None]);
    }

    /// `otlp_out` writes a gauge flagged `info` or `stateset` as a cumulative non-monotonic sum,
    /// an `info` one under `<name>_info`, the stateset's flag on the resource; with no flag, the
    /// same sum pairs nothing.
    #[test]
    fn otlp_pairs_an_info_and_a_stateset_gauge_with_their_sums() {
        let info = r#"{{T},"metrics":[{"name":"build","kind":"gauge","value":1.0}],"attributes":{"_logit.metrics.type":"info","version":"1"}}"#;
        let state = r#"{{T},"metrics":[{"name":"feature","kind":"gauge","value":0.0}],"attributes":{"feature":"a"},"resource":{"attributes":{"_logit.metrics.type":"stateset"}}}"#;
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"build_info","kind":"sum","value":1.0,"temporality":"cumulative","monotonic":false}],"attributes":{"version":"1"}}"#,
            r#"{{T},"metrics":[{"name":"feature","kind":"sum","value":0.0,"temporality":"cumulative","monotonic":false}],"attributes":{"feature":"a"}}"#,
        ]);
        let pairing = pair(&recs(&[info, state]), &outs, Spelling::Otlp);
        assert_eq!(pairing.seen, [Some(Class::Normalized), Some(Class::Normalized)]);
        assert!(pairing.unclaimed.is_empty());

        let bare = recs(&[
            r#"{{T},"metrics":[{"name":"feature","kind":"gauge","value":0.0}],"attributes":{"feature":"a"}}"#,
        ]);
        assert_eq!(pair(&bare, &outs[1..], Spelling::Otlp).seen, [Some(Class::Degraded)]);
    }

    /// `influxdb_out` writes a gauge with no measurement carrier as field `value`, read back as
    /// `<name>.value`: the same gauge renamed, not a dotted expansion of it.
    #[test]
    fn influxdb_pairs_a_value_field_as_the_same_gauge() {
        let ins =
            recs(&[r#"{{T},"metrics":[{"name":"temperature","kind":"gauge","value":21.5}]}"#]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"temperature.value","kind":"gauge","value":21.5}],"attributes":{"_logit.influxdb.measurement":"temperature"}}"#,
        ]);
        assert_eq!(pair(&ins, &outs, Spelling::Influxdb).seen, [Some(Class::Normalized)]);
        assert_eq!(pair(&ins, &outs, Spelling::Verbatim).seen, [Some(Class::Degraded)]);
    }

    /// Two points of one series that each expand into dotted fields claim one of each, so
    /// neither is left unpaired.
    #[test]
    fn two_points_of_a_series_each_claim_one_expansion() {
        let samples = r#"{{T},"metrics":[{"name":"latency","kind":"samples","values":[7.5],"sample_rate":1.0}]}"#;
        let ins = recs(&[samples, samples]);
        let field = |name: &str| {
            r#"{{T},"metrics":[{"name":"latency.NAME","kind":"gauge","value":1.0}]}"#
                .replace("NAME", name)
        };
        let outs = recs(&[&field("count"), &field("sum"), &field("count"), &field("sum")]);
        let pairing = pair(&ins, &outs, Spelling::Influxdb);
        assert_eq!(pairing.seen, [Some(Class::Degraded), Some(Class::Degraded)]);
        assert_eq!(pairing.claimed.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2]);
    }

    /// `otlp_out` writes a timestamp attribute as an integer of nanoseconds.
    #[test]
    fn otlp_pairs_a_timestamp_attribute_read_back_as_nanoseconds() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"t","kind":"gauge","value":1.0}],"attributes":{"seen":"2027-01-15T07:00:00.000000000Z"},"types":{"/attributes/seen":"timestamp"}}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"t","kind":"gauge","value":1.0}],"attributes":{"seen":1799996400000000000}}"#,
        ]);
        assert_eq!(pair(&ins, &outs, Spelling::Otlp).seen, [Some(Class::Exact)]);
        assert_eq!(pair(&ins, &outs, Spelling::Verbatim).seen, [None]);
    }

    /// `statsd_out` writes a reserved character in a name as `_`.
    #[test]
    fn statsd_pairs_a_sanitized_name() {
        let ins = recs(&[r#"{{T},"metrics":[{"name":"a:b","kind":"gauge","value":1.0}]}"#]);
        let outs = recs(&[r#"{{T},"metrics":[{"name":"a_b","kind":"gauge","value":1.0}]}"#]);
        assert_eq!(
            pair(&ins, &outs, Spelling::Statsd { dogstatsd: true }).seen,
            [Some(Class::Normalized)]
        );
        assert_eq!(pair(&ins, &outs, Spelling::Verbatim).seen, [None]);
    }

    /// `statsd_out` writes a reserved character in a tag key as `_`, and in a tag value too but
    /// for `:`, and the field row reads the rewrite as a change.
    #[test]
    fn statsd_pairs_a_sanitized_tag() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],"attributes":{"a b":"c:d e"}}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],"attributes":{"a_b":"c:d_e"}}"#,
        ]);
        assert_eq!(
            pair(&ins, &outs, Spelling::Statsd { dogstatsd: true }).seen,
            [Some(Class::Exact)]
        );
        assert_eq!(pair(&ins, &outs, Spelling::Verbatim).seen, [None]);
        let field = Field::Attribute { key: "a b".to_string(), resource: false };
        assert_eq!(
            observe_field(&field, &ins[0], &outs[0], Spelling::Statsd { dogstatsd: true }),
            Class::Normalized
        );
    }

    /// `statsd_out` writes a non-ASCII name and a `/` as they are, so neither reaches
    /// `record/name_unsafe`; a space does.
    #[test]
    fn statsd_name_unsafe_is_the_encoders_set() {
        let line = r#"{{T},"metrics":[{"name":"NAME","kind":"gauge","value":1.0}]}"#;
        let rec = |name: &str| recs(&[&line.replace("NAME", name)]).remove(0);
        for name in ["caf\u{e9}.load", "a/b", "a-b.c_d"] {
            assert!(!record_features(&rec(name), PLAIN_STATSD).contains(&"name_unsafe"), "{name}");
        }
        assert!(record_features(&rec("queue depth"), PLAIN_STATSD).contains(&"name_unsafe"));
    }

    /// `graphite_out` writes `/`, `\`, `;`, and whitespace in a path as `_`, and nothing else.
    #[test]
    fn graphite_pairs_a_sanitized_path() {
        let ins = recs(&[r#"{{T},"metrics":[{"name":"a/b c\\d","kind":"gauge","value":1.0}]}"#]);
        let outs = recs(&[r#"{{T},"metrics":[{"name":"a_b_c_d","kind":"gauge","value":1.0}]}"#]);
        assert_eq!(pair(&ins, &outs, Spelling::Graphite).seen, [Some(Class::Normalized)]);
        assert_eq!(pair(&ins, &outs, Spelling::Verbatim).seen, [None]);
        assert!(record_features(&ins[0], Spelling::Graphite).contains(&"name_unsafe"));
        let plain = recs(&[r#"{{T},"metrics":[{"name":"café:x|y","kind":"gauge","value":1.0}]}"#]);
        assert!(!record_features(&plain[0], Spelling::Graphite).contains(&"name_unsafe"));
    }

    /// An `f64` attribute in its `format: json` spelling reads back as the tag value
    /// `graphite_out`'s encoder writes for it, a non-finite one through its `types` entry.
    #[test]
    fn graphite_reads_an_f64_tag_as_the_encoder_writes_it() {
        let read = |json: &str, tags: &str| {
            graphite_value(&serde_json::from_str(json).expect("a JSON value"), tags)
        };
        for (json, want) in [
            ("3.0", "3"),
            ("1e21", "1000000000000000000000"),
            ("-0.0", "-0"),
            ("0.25", "0.25"),
            ("3", "3"),
        ] {
            assert_eq!(read(json, "").as_deref(), Some(want), "{json}");
        }
        for (json, want) in [(r#""NaN""#, "NaN"), (r#""inf""#, "inf"), (r#""-inf""#, "-inf")] {
            assert_eq!(read(json, "/=f64").as_deref(), Some(want), "{json}");
        }
        let mut written = String::new();
        for value in [3.0, 1e21, -0.0, 0.25, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(render_tag_value(&mut written, &logit_core::Value::F64(value)));
            let json = if value.is_finite() {
                serde_json::to_string(&value).expect("a finite f64")
            } else {
                format!("{written:?}")
            };
            let tags = if value.is_finite() { "" } else { "/=f64" };
            assert_eq!(read(&json, tags).as_deref(), Some(written.as_str()), "{value}");
        }
    }

    /// `graphite_out` writes a tag name's `!` and a tag value's whitespace and leading `~` as
    /// `_`, an `f64` in its shortest form and a boolean as text, an array as its last
    /// representable element, and no tag for a timestamp.
    #[test]
    fn graphite_pairs_rendered_tags() {
        let ins = recs(&[concat!(
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],"#,
            r#""attributes":{"a!b":"~x y","f":3.0,"ok":true,"arr":["one",{"m":1}],"#,
            r#""at":"2027-01-15T08:00:00.000000000Z"},"types":{"/attributes/at":"timestamp"}}"#
        )]);
        let outs = recs(&[concat!(
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],"#,
            r#""attributes":{"a_b":"_x_y","f":"3","ok":"true","arr":"one"}}"#
        )]);
        assert_eq!(pair(&ins, &outs, Spelling::Graphite).seen, [Some(Class::Exact)]);
        let field = |key: &str| Field::Attribute { key: key.to_string(), resource: false };
        let observe = |key: &str| observe_field(&field(key), &ins[0], &outs[0], Spelling::Graphite);
        assert_eq!(observe("a!b"), Class::Normalized);
        assert_eq!(observe("f"), Class::Normalized);
        assert_eq!(observe("at"), Class::Lost);
    }

    /// A `graphite_out` expansion of a record whose path it sanitizes hangs off the sanitized
    /// path.
    #[test]
    fn graphite_pairs_an_expansion_of_a_sanitized_path() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"a b","kind":"distribution","count":1,"sum":12.0,"min":12.0,"max":12.0,"avg":12.0,"p50":12.0,"p90":12.0,"p95":12.0,"p99":12.0,"sketch":{"mapping":"agent","stats_exact":true,"count":1.0,"min":12.0,"max":12.0,"positive":[{"key":1498,"count":1.0}]}}]}"#,
        ]);
        let outs = recs(&[r#"{{T},"metrics":[{"name":"a_b.count","kind":"gauge","value":1.0}]}"#]);
        assert_eq!(pair(&ins, &outs, Spelling::Graphite).seen, [Some(Class::Degraded)]);
        assert_eq!(pair(&ins, &outs, Spelling::Verbatim).seen, [None]);
    }

    /// An event with no metric, log, or span and no measurement carrier is one `event` record
    /// reaching `event/attributes_only`.
    #[test]
    fn an_event_with_attributes_only_is_an_event_record() {
        let ins = recs(&[r#"{{T},"attributes":{"_logit.field.s":"x"}}"#]);
        assert_eq!(ins.len(), 1);
        assert_eq!((ins[0].kind.as_str(), ins[0].family()), ("event", "event"));
        assert_eq!(kind_features(&ins[0]), [Some("attributes_only".to_string())]);
    }

    /// An event with no metric, log, or span that carries the InfluxDB measurement is a
    /// string-only point, which `influxdb_out` writes only then.
    #[test]
    fn an_event_with_a_measurement_is_string_only() {
        let ins = recs(&[
            r#"{{T},"attributes":{"_logit.influxdb.measurement":"m","_logit.field.s":"x"}}"#,
        ]);
        assert_eq!(kind_features(&ins[0]), [Some("string_only".to_string())]);
    }

    /// `datadog_out` writes the run's hostname as the host of a record that carries none, and
    /// `datadog_in` reads it back as `host.name`; any other value doesn't pair.
    #[test]
    fn datadog_pairs_the_default_host_only_where_the_input_has_none() {
        let gauge = |attributes: &str| {
            format!(
                r#"{{{{T}},"metrics":[{{"name":"g","kind":"gauge","value":1.0}}],"attributes":{attributes}}}"#
            )
        };
        let hostless = recs(&[&gauge("{}")]);
        let filled = recs(&[&gauge(r#"{"host.name":"transit"}"#)]);
        let other = recs(&[&gauge(r#"{"host.name":"web-2"}"#)]);
        assert_eq!(pair(&hostless, &filled, Spelling::Datadog).seen, [Some(Class::Exact)]);
        assert_eq!(pair(&hostless, &filled, Spelling::Verbatim).seen, [None]);
        assert_eq!(pair(&hostless, &other, Spelling::Datadog).seen, [None]);
        let rows = field_rows(&hostless[0], "datadog");
        let (_, _, field) = rows.iter().find(|(_, f, _)| f.as_deref() == Some("no_host")).unwrap();
        assert_eq!(
            observe_field(field, &hostless[0], &filled[0], Spelling::Datadog),
            Class::Normalized
        );
    }

    /// An input's own host or hostless mark always wins: the run's hostname read back in its
    /// place doesn't pair.
    #[test]
    fn datadog_never_pairs_the_default_host_in_place_of_the_inputs_own() {
        let gauge = |attributes: &str| {
            format!(
                r#"{{{{T}},"metrics":[{{"name":"g","kind":"gauge","value":1.0}}],"attributes":{attributes}}}"#
            )
        };
        let filled = recs(&[&gauge(r#"{"host.name":"transit"}"#)]);
        for own in [r#"{"host.name":"web-1"}"#, r#"{"_logit.datadog.hostless":true}"#] {
            let own = recs(&[&gauge(own)]);
            assert_eq!(pair(&own, &filled, Spelling::Datadog).seen, [None]);
        }
    }

    /// A log's `host.name` is its Datadog `hostname`, read back as `host.name`; a `hostname`
    /// attribute is lost, beside `host.name` or not, and the default host fills in its place.
    #[test]
    fn datadog_pairs_a_logs_host_name_and_loses_a_hostname_attribute() {
        let log = |attributes: &str| {
            format!(r#"{{{{T}},"log":{{"format":"raw","message":"m"}},"attributes":{attributes}}}"#)
        };
        let host = Field::Attribute { key: "host.name".to_string(), resource: false };
        let hostname = Field::Attribute { key: "hostname".to_string(), resource: false };
        let read = recs(&[&log(r#"{"host.name":"web-1"}"#)]);
        let ins = recs(&[&log(r#"{"host.name":"web-1"}"#)]);
        assert_eq!(observe_field(&host, &ins[0], &read[0], Spelling::Datadog), Class::Exact);
        let both = recs(&[&log(r#"{"host.name":"web-1","hostname":"h"}"#)]);
        assert_eq!(pair(&both, &read, Spelling::Datadog).seen, [Some(Class::Exact)]);
        assert_eq!(pair(&both, &both, Spelling::Datadog).seen, [None]);
        assert_eq!(observe_field(&hostname, &both[0], &read[0], Spelling::Datadog), Class::Lost);
        let alone = recs(&[&log(r#"{"hostname":"h"}"#)]);
        let filled = recs(&[&log(r#"{"host.name":"transit"}"#)]);
        assert_eq!(pair(&alone, &filled, Spelling::Datadog).seen, [Some(Class::Exact)]);
        assert_eq!(observe_field(&hostname, &alone[0], &filled[0], Spelling::Datadog), Class::Lost);
    }

    /// A log's `service.name` is its Datadog `service`, read back as `service.name`; a `service`
    /// attribute is lost beside it, and alone is the `service` field, read back as `service.name`.
    #[test]
    fn datadog_pairs_a_logs_service_name_and_loses_a_service_attribute() {
        let log = |attributes: &str| {
            format!(r#"{{{{T}},"log":{{"format":"raw","message":"m"}},"attributes":{attributes}}}"#)
        };
        let name = Field::Attribute { key: "service.name".to_string(), resource: false };
        let service = Field::Attribute { key: "service".to_string(), resource: false };
        let read = recs(&[&log(r#"{"service.name":"web"}"#)]);
        assert_eq!(observe_field(&name, &read[0], &read[0], Spelling::Datadog), Class::Exact);
        let both = recs(&[&log(r#"{"service.name":"web","service":"s"}"#)]);
        assert_eq!(observe_field(&service, &both[0], &read[0], Spelling::Datadog), Class::Lost);
        let alone = recs(&[&log(r#"{"service":"s"}"#)]);
        let moved = recs(&[&log(r#"{"service.name":"s"}"#)]);
        assert_eq!(
            observe_field(&service, &alone[0], &moved[0], Spelling::Datadog),
            Class::Normalized
        );
        let feature = |rec: &Rec| {
            field_rows(rec, "datadog")
                .into_iter()
                .find(|(_, _, f)| matches!(f, Field::Attribute { key, .. } if key == "service"))
                .and_then(|(_, feature, _)| feature)
        };
        assert_eq!(feature(&both[0]).as_deref(), Some("log_service"));
        assert_eq!(feature(&alone[0]).as_deref(), Some("log_service_field"));
        // On a metric, `service` is the tag `service.name` also writes.
        let gauge = recs(&[
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],"attributes":{"service.name":"x","service":"s"}}"#,
        ]);
        assert_eq!(feature(&gauge[0]).as_deref(), Some("service_tag_beside_key"));
    }

    /// A `host` attribute is an ordinary tag at `datadog_out`, read back as `host`, and the
    /// default host fills; it's never read back as `host.name`.
    #[test]
    fn datadog_pairs_a_host_attribute_as_an_ordinary_tag() {
        let gauge = |attributes: &str| {
            format!(
                r#"{{{{T}},"metrics":[{{"name":"g","kind":"gauge","value":1.0}}],"attributes":{attributes}}}"#
            )
        };
        let ins = recs(&[&gauge(r#"{"host":"web-1"}"#)]);
        let tagged = recs(&[&gauge(r#"{"host":"web-1","host.name":"transit"}"#)]);
        let moved = recs(&[&gauge(r#"{"host.name":"web-1"}"#)]);
        assert_eq!(pair(&ins, &tagged, Spelling::Datadog).seen, [Some(Class::Exact)]);
        assert_eq!(pair(&ins, &moved, Spelling::Datadog).seen, [None]);
        let host = Field::Attribute { key: "host".to_string(), resource: false };
        assert_eq!(observe_field(&host, &ins[0], &tagged[0], Spelling::Datadog), Class::Exact);
    }

    /// Beside the line's own host, `statsd_out` keeps a `host` array's bare `true` elements and
    /// omits its valued ones, so a mixed array is partly omitted, not omitted whole.
    #[test]
    fn statsd_judges_a_mixed_host_array_per_element() {
        let input = |host: &str| {
            format!(
                r#"{{{{T}},"metrics":[{{"name":"g","kind":"gauge","value":1.0}}],"attributes":{{"host":{host},"host.name":"h"}}}}"#
            )
        };
        let mixed = recs(&[&input(r#"[true,"web-1"]"#)]);
        let bare = recs(&[&input("[true]")]);
        let valued = recs(&[&input(r#""web-1""#)]);
        let value = |r: &Rec| r.attributes["host"].clone();
        assert!(statsd::host_is_omitted(&mixed[0], &value(&mixed[0])));
        assert_eq!(
            statsd::host_kept(&mixed[0], &value(&mixed[0])),
            Some(serde_json::json!([true]))
        );
        assert!(!statsd::host_is_omitted(&bare[0], &value(&bare[0])));
        assert_eq!(statsd::host_kept(&bare[0], &value(&bare[0])), Some(serde_json::json!([true])));
        assert_eq!(statsd::host_kept(&valued[0], &value(&valued[0])), None);
    }

    /// A Datadog event's or check's `host.name` is its host, read back as `host.name`.
    #[test]
    fn datadog_pairs_an_events_or_checks_host_name_as_itself() {
        let event = |attributes: &str| {
            format!(r#"{{{{T}},"log":{{"format":"raw","message":"m"}},"attributes":{attributes}}}"#)
        };
        let check = |attributes: &str| {
            format!(
                r#"{{{{T}},"metrics":[{{"name":"c","kind":"gauge","value":0.0}}],"attributes":{attributes}}}"#
            )
        };
        let field = Field::Attribute { key: "host.name".to_string(), resource: false };
        for input in [
            event(r#"{"_logit.statsd.event.title":"t","host.name":"h"}"#),
            check(r#"{"_logit.statsd.service_check.name":"c","host.name":"h"}"#),
        ] {
            let ins = recs(&[&input]);
            assert_eq!(pair(&ins, &ins, Spelling::Datadog).seen, [Some(Class::Exact)], "{input}");
            assert_eq!(
                observe_field(&field, &ins[0], &ins[0], Spelling::Datadog),
                Class::Exact,
                "{input}"
            );
        }
    }

    /// A collectd host is another family's key: the Datadog spelling pairs it with itself as a
    /// tag, never as the host, and a record carrying only it takes the default host.
    #[test]
    fn datadog_pairs_a_collectd_host_as_an_ordinary_tag() {
        let gauge = |attributes: &str| {
            format!(
                r#"{{{{T}},"metrics":[{{"name":"g","kind":"gauge","value":1.0}}],"attributes":{attributes}}}"#
            )
        };
        let ins = recs(&[&gauge(r#"{"collectd.host":"web-1"}"#)]);
        let moved = recs(&[&gauge(r#"{"host.name":"web-1"}"#)]);
        let kept = recs(&[&gauge(r#"{"collectd.host":"web-1"}"#)]);
        assert_eq!(pair(&ins, &moved, Spelling::Datadog).seen, [None]);
        assert_eq!(pair(&ins, &kept, Spelling::Datadog).seen, [Some(Class::Exact)]);
        assert!(derived_rows(&ins[0]).contains(&"no_host"));
    }

    const DOGSTATSD: Spelling = Spelling::Statsd { dogstatsd: true };
    const PLAIN_STATSD: Spelling = Spelling::Statsd { dogstatsd: false };

    fn statsd_gauge(attributes: &str) -> String {
        format!(
            r#"{{{{T}},"metrics":[{{"name":"g","kind":"gauge","value":1.0}}],"attributes":{attributes}}}"#
        )
    }

    /// `statsd_out` under DogStatsD writes `host.name` as the line's host, which `statsd_in`
    /// reads back as `host.name`, on a metric line and an event alike.
    #[test]
    fn dogstatsd_pairs_host_name_as_itself() {
        let event = |attributes: &str| {
            format!(r#"{{{{T}},"log":{{"format":"raw","message":"m"}},"attributes":{attributes}}}"#)
        };
        let field = Field::Attribute { key: "host.name".to_string(), resource: false };
        for input in [
            statsd_gauge(r#"{"host.name":"web-1"}"#),
            event(r#"{"_logit.statsd.event.title":"t","host.name":"web-1"}"#),
        ] {
            let ins = recs(&[&input]);
            assert_eq!(pair(&ins, &ins, DOGSTATSD).seen, [Some(Class::Exact)], "{input}");
            assert_eq!(observe_field(&field, &ins[0], &ins[0], DOGSTATSD), Class::Exact);
        }
    }

    /// A `host` attribute beside the line's own host, `host.name` or the hostless mark, is lost;
    /// with neither, it's the line's `host:` tag, read back as `host.name`, or, empty, as the
    /// mark. Plain statsd writes no tag at all.
    #[test]
    fn dogstatsd_reads_a_host_attribute_back_as_the_lines_host() {
        let host = Field::Attribute { key: "host".to_string(), resource: false };
        let both = recs(&[&statsd_gauge(r#"{"host":"web-1","host.name":"h"}"#)]);
        let read = recs(&[&statsd_gauge(r#"{"host.name":"h"}"#)]);
        assert_eq!(pair(&both, &read, DOGSTATSD).seen, [Some(Class::Exact)]);
        assert_eq!(pair(&both, &both, DOGSTATSD).seen, [None]);
        assert_eq!(observe_field(&host, &both[0], &read[0], DOGSTATSD), Class::Lost);
        let marked = recs(&[&statsd_gauge(r#"{"host":"web-1","_logit.datadog.hostless":true}"#)]);
        let read_mark = recs(&[&statsd_gauge(r#"{"_logit.datadog.hostless":true}"#)]);
        assert_eq!(pair(&marked, &read_mark, DOGSTATSD).seen, [Some(Class::Exact)]);
        assert_eq!(observe_field(&host, &marked[0], &read_mark[0], DOGSTATSD), Class::Lost);

        let ins = recs(&[&statsd_gauge(r#"{"host":"web-1"}"#)]);
        let other = recs(&[&statsd_gauge(r#"{"host.name":"web-2"}"#)]);
        let same = recs(&[&statsd_gauge(r#"{"host.name":"web-1"}"#)]);
        assert_eq!(pair(&ins, &same, DOGSTATSD).seen, [Some(Class::Exact)]);
        assert_eq!(pair(&ins, &same, PLAIN_STATSD).seen, [None]);
        assert_eq!(pair(&ins, &other, DOGSTATSD).seen, [None]);
        assert_eq!(observe_field(&host, &ins[0], &same[0], DOGSTATSD), Class::Normalized);
        let empty = recs(&[&statsd_gauge(r#"{"host":""}"#)]);
        assert_eq!(pair(&empty, &read_mark, DOGSTATSD).seen, [Some(Class::Exact)]);
        assert_eq!(observe_field(&host, &empty[0], &read_mark[0], DOGSTATSD), Class::Normalized);
    }

    /// The hostless mark on a record with no `host.name` leaves as an empty `host:` tag, read
    /// back as the mark.
    #[test]
    fn dogstatsd_pairs_the_hostless_mark_as_itself() {
        let ins = recs(&[&statsd_gauge(r#"{"_logit.datadog.hostless":true}"#)]);
        let field =
            Field::Attribute { key: "_logit.datadog.hostless".to_string(), resource: false };
        assert_eq!(observe_field(&field, &ins[0], &ins[0], DOGSTATSD), Class::Exact);
        assert_eq!(pair(&ins, &ins, DOGSTATSD).seen, [Some(Class::Exact)]);
    }

    /// A log leaves with `status` set to its severity's name, read back as an attribute; a
    /// `status` holding anything else doesn't pair.
    #[test]
    fn datadog_pairs_a_log_status_only_as_its_severity() {
        let log = |attributes: &str| {
            format!(
                r#"{{{{T}},"log":{{"severity":"warn","format":"raw","message":"m"}},"attributes":{attributes}}}"#
            )
        };
        let ins = recs(&[&log(r#"{"host.name":"h"}"#)]);
        let same = recs(&[&log(r#"{"host.name":"h","status":"warn"}"#)]);
        let other = recs(&[&log(r#"{"host.name":"h","status":"info"}"#)]);
        assert_eq!(pair(&ins, &same, Spelling::Datadog).seen, [Some(Class::Exact)]);
        assert_eq!(pair(&ins, &same, Spelling::Verbatim).seen, [None]);
        assert_eq!(pair(&ins, &other, Spelling::Datadog).seen, [None]);
    }

    /// A span leaves with `span.kind` from its kind and `_dd.p.tid` from its trace id's high 64
    /// bits, each read back as an attribute; a `span.kind` with another value doesn't pair.
    #[test]
    fn datadog_pairs_a_spans_kind_and_trace_id_high_half_only_as_derived() {
        let span = |attributes: &str| {
            format!(
                r#"{{{{T}},"span":{{"name":"s","trace_id":"0a0b0c0d0e0f10110102030405060708","span_id":"1112131415161718","kind":"client","status":"unset","start":"2027-01-15T08:00:00.000000000Z","end":"2027-01-15T08:00:00.000000000Z","duration_ns":0}},"attributes":{attributes}}}"#
            )
        };
        let ins = recs(&[&span("{}")]);
        let same = recs(&[&span(r#"{"span.kind":"client","_dd.p.tid":"0a0b0c0d0e0f1011"}"#)]);
        let wrong_kind = recs(&[&span(r#"{"span.kind":"server","_dd.p.tid":"0a0b0c0d0e0f1011"}"#)]);
        assert_eq!(pair(&ins, &same, Spelling::Datadog).seen, [Some(Class::Exact)]);
        assert_eq!(pair(&ins, &same, Spelling::Verbatim).seen, [None]);
        assert_eq!(pair(&ins, &wrong_kind, Spelling::Datadog).seen, [None]);
    }

    /// A span's derived host is read from the resource, where the tracer payload's hostname reads
    /// back, ahead of the span's own `host.name` tag.
    #[test]
    fn datadog_reads_a_spans_derived_host_from_the_resource_first() {
        let span = |rest: &str| {
            format!(
                r#"{{{{T}},"span":{{"name":"s","trace_id":"0a0b0c0d0e0f10110102030405060708","span_id":"1112131415161718","kind":"internal","status":"unset","start":"2027-01-15T08:00:00.000000000Z","end":"2027-01-15T08:00:00.000000000Z","duration_ns":0}},"attributes":{{"host.name":"span-host"}}{rest}}}"#
            )
        };
        let ins = recs(&[&span("")]);
        let filled =
            format!(r#","resource":{{"attributes":{{"host.name":"{}"}}}}"#, super::super::HOSTNAME);
        let out = recs(&[&span(&filled)]);
        let rows = field_rows(&ins[0], "datadog");
        let (_, _, field) = rows.iter().find(|(_, f, _)| f.as_deref() == Some("no_host")).unwrap();
        assert_eq!(observe_field(field, &ins[0], &out[0], Spelling::Datadog), Class::Normalized);
    }

    /// A `_dd.p.tid` other than the trace id's high 64 bits doesn't pair.
    #[test]
    fn datadog_never_pairs_a_trace_id_high_half_it_didnt_derive() {
        let span = |attributes: &str| {
            format!(
                r#"{{{{T}},"span":{{"name":"s","trace_id":"0a0b0c0d0e0f10110102030405060708","span_id":"1112131415161718","kind":"client","status":"unset","start":"2027-01-15T08:00:00.000000000Z","end":"2027-01-15T08:00:00.000000000Z","duration_ns":0}},"attributes":{attributes}}}"#
            )
        };
        let ins = recs(&[&span("{}")]);
        let wrong_tid = recs(&[&span(r#"{"span.kind":"client","_dd.p.tid":"0000000000000001"}"#)]);
        assert_eq!(pair(&ins, &wrong_tid, Spelling::Datadog).seen, [None]);
    }

    /// Every sink reaches the rows for what `datadog_out` derives, chosen from the record alone;
    /// a sink that derives nothing reads them `exact`.
    #[test]
    fn every_sink_reaches_the_rows_datadog_out_derives() {
        let ins = recs(&[r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}]}"#]);
        let rows = field_rows(&ins[0], "statsd");
        let (_, _, field) = rows.iter().find(|(_, f, _)| f.as_deref() == Some("no_host")).unwrap();
        assert_eq!(
            observe_field(field, &ins[0], &ins[0], Spelling::Statsd { dogstatsd: true }),
            Class::Exact
        );
        let span = recs(&[
            r#"{{T},"span":{"name":"s","trace_id":"0102030405060708090a0b0c0d0e0f10","span_id":"0102030405060708","kind":"server","status":"ok","start":"2027-01-15T08:00:00.000000000Z","end":"2027-01-15T08:00:00.000000000Z","duration_ns":0}}"#,
        ]);
        assert!(kind_features(&span[0]).contains(&Some("no_wire_form".to_string())));
    }

    /// `datadog_out` synthesizes `_dd.p.tid` on the first span of a trace in a batch only, and
    /// on none when a span of the trace carries one.
    #[test]
    fn only_a_traces_first_span_without_a_tid_is_due_one() {
        let span = |id: &str, attributes: &str| {
            format!(
                r#"{{{{T}},"span":{{"name":"s","trace_id":"0a0b0c0d0e0f10110102030405060708","span_id":"{id}","kind":"internal","status":"unset","start":"2027-01-15T08:00:00.000000000Z","end":"2027-01-15T08:00:00.000000000Z","duration_ns":0}},"attributes":{attributes}}}"#
            )
        };
        let pair = recs(&[&span("1112131415161718", "{}"), &span("2122232425262728", "{}")]);
        assert_eq!(pair.iter().map(|r| r.tid_due).collect::<Vec<_>>(), [true, false]);
        let carried = recs(&[
            &span("1112131415161718", "{}"),
            &span("2122232425262728", r#"{"_dd.p.tid":"0a0b0c0d0e0f1011"}"#),
        ]);
        assert_eq!(carried.iter().map(|r| r.tid_due).collect::<Vec<_>>(), [false, false]);
    }

    /// A rate that rounds to a weight above 1 reaches `record/sample_rate`; one below 1 that rounds
    /// to weight 1 reaches the field row `record/sample_rate_unweighted`, read `lost` when it reads
    /// back as 1.
    #[test]
    fn a_sample_rate_is_selected_by_its_weight() {
        let samples = |rate: f64| {
            format!(
                r#"{{{{T}},"metrics":[{{"name":"t","kind":"samples","values":[12.0],"sample_rate":{rate:?}}}]}}"#
            )
        };
        let half = recs(&[&samples(0.5)]);
        assert!(record_features(&half[0], Spelling::Datadog).contains(&"sample_rate"));
        let unweighted = recs(&[&samples(0.8)]);
        assert!(!record_features(&unweighted[0], Spelling::Datadog).contains(&"sample_rate"));
        let rows = field_rows(&unweighted[0], "datadog");
        let (_, _, field) =
            rows.iter().find(|(_, f, _)| f.as_deref() == Some("sample_rate_unweighted")).unwrap();
        let read = recs(&[&samples(1.0)]);
        assert_eq!(pair(&unweighted, &read, Spelling::Datadog).seen, [Some(Class::Exact)]);
        assert_eq!(observe_field(field, &unweighted[0], &read[0], Spelling::Datadog), Class::Lost);
    }

    /// A `samples` record's unit reaches `record/unit_on_distribution`, a gauge's `record/unit`.
    #[test]
    fn a_samples_unit_has_its_own_row() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"t","kind":"samples","values":[1.0],"sample_rate":1.0,"unit":"By"}]}"#,
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0,"unit":"By"}]}"#,
        ]);
        let unit_row = |rec: &Rec| {
            field_rows(rec, "datadog")
                .into_iter()
                .filter_map(|(_, f, _)| f)
                .find(|f| f.starts_with("unit"))
        };
        assert_eq!(unit_row(&ins[0]).as_deref(), Some("unit_on_distribution"));
        assert_eq!(unit_row(&ins[1]).as_deref(), Some("unit"));
    }

    /// The rate and interval flags reach a row named for the record's kind.
    #[test]
    fn the_rate_and_interval_flags_have_rows_by_kind() {
        let flags = r#""attributes":{"_logit.metrics.type":"rate","_logit.metrics.interval":10}"#;
        let lines = [
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],FLAGS}"#,
            r#"{{T},"metrics":[{"name":"s","kind":"sum","value":1.0,"temporality":"delta","monotonic":true}],FLAGS}"#,
            r#"{{T},"metrics":[{"name":"t","kind":"samples","values":[1.0],"sample_rate":1.0}],FLAGS}"#,
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"delta","buckets":[{"bound":1.0,"count":1},{"bound":"inf","count":0}],"sum":1.0}],FLAGS}"#,
            r#"{{T},"metrics":[{"name":"c","kind":"histogram","temporality":"cumulative","buckets":[{"bound":1.0,"count":1},{"bound":"inf","count":0}],"sum":1.0}],FLAGS}"#,
            r#"{{T},"metrics":[{"name":"e","kind":"exponential_histogram","scale":2,"temporality":"delta","count":1,"zero_count":0,"zero_threshold":0.0,"positive":{"offset":0,"counts":[1]},"negative":{"offset":0,"counts":[]},"sum":1.0}],FLAGS}"#,
            r#"{{T},"metrics":[{"name":"x","kind":"exponential_histogram","scale":2,"temporality":"cumulative","count":1,"zero_count":0,"zero_threshold":0.0,"positive":{"offset":0,"counts":[1]},"negative":{"offset":0,"counts":[]},"sum":1.0}],FLAGS}"#,
        ]
        .map(|line| line.replace("FLAGS", flags));
        let ins = recs(&lines.each_ref().map(String::as_str));
        let flag_rows = |rec: &Rec| {
            let mut rows: Vec<String> = field_rows(rec, "statsd")
                .into_iter()
                .filter_map(|(_, f, _)| f)
                .filter(|f| f.starts_with("rate") || f.starts_with("interval"))
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(flag_rows(&ins[0]), ["interval", "rate"]);
        assert_eq!(flag_rows(&ins[1]), ["interval", "rate_on_non_gauge"]);
        assert_eq!(flag_rows(&ins[2]), ["interval_on_distribution", "rate_on_non_gauge"]);
        assert_eq!(flag_rows(&ins[3]), ["interval_on_histogram", "rate_on_non_gauge"]);
        assert_eq!(flag_rows(&ins[4]), ["interval", "rate_on_non_gauge"]);
        assert_eq!(flag_rows(&ins[5]), ["interval_on_distribution", "rate_on_non_gauge"]);
        assert_eq!(flag_rows(&ins[6]), ["interval", "rate_on_non_gauge"]);
    }

    /// `info` and `stateset` reach `family_type` on a gauge and `gaugehistogram` on a histogram,
    /// each on the event or the resource; any of the three on another kind reaches
    /// `family_type_on_other_kind`, and `prometheus_in`'s carrier reaches `foreign_carrier`.
    #[test]
    fn the_family_type_flags_have_rows_by_kind() {
        let gauge = r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],AT}"#;
        let sum = r#"{{T},"metrics":[{"name":"s","kind":"sum","value":1.0,"temporality":"cumulative","monotonic":true}],AT}"#;
        let histogram = r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":1.0,"count":1},{"bound":"inf","count":0}],"sum":1.0}],AT}"#;
        let flag = |at: &str, value: &str| {
            let flag = format!(r#"{{"_logit.metrics.type":"{value}"}}"#);
            match at {
                "resource" => format!(r#""resource":{{"attributes":{flag}}}"#),
                _ => format!(r#""attributes":{flag}"#),
            }
        };
        let carrier = r#""attributes":{"_logit.prometheus.type":"info"}"#.to_string();
        let lines = [
            (gauge, flag("attributes", "info"), "family_type"),
            (gauge, flag("resource", "stateset"), "family_type"),
            (histogram, flag("attributes", "gaugehistogram"), "family_type"),
            (histogram, flag("resource", "info"), "family_type_on_other_kind"),
            (gauge, flag("attributes", "gaugehistogram"), "family_type_on_other_kind"),
            (sum, flag("attributes", "stateset"), "family_type_on_other_kind"),
            (gauge, carrier, "foreign_carrier"),
        ];
        for (line, at, expected) in lines {
            let rec = &recs(&[line.replace("AT", &at).as_str()])[0];
            let rows: Vec<String> = field_rows(rec, "otlp")
                .into_iter()
                .filter_map(|(_, f, _)| f)
                .filter(|f| f.starts_with("family_type") || f.ends_with("carrier"))
                .collect();
            assert_eq!(rows, [expected], "{at}");
        }
    }

    /// An empty `host.name` is no host, on a log as on every route, so the default fills.
    #[test]
    fn a_log_with_an_empty_host_name_is_due_the_default_host() {
        let log =
            recs(&[r#"{{T},"log":{"format":"raw","message":"m"},"attributes":{"host.name":""}}"#]);
        assert!(derived_rows(&log[0]).contains(&"no_host"));
    }

    /// The latest point of a series claims what a sink holding one value per series read back,
    /// so the earlier one is the record left over.
    #[test]
    fn the_latest_point_of_a_series_pairs_first() {
        let gauge = r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}]}"#;
        let ins = recs(&[gauge, gauge]);
        let outs = recs(&[gauge]);
        assert_eq!(pair(&ins, &outs, Spelling::Prometheus).seen, [None, Some(Class::Exact)]);
        assert!(same_series(&ins[0], &ins[1]));
    }

    /// A histogram whose last bound is finite reads back the same with an empty `+Inf` bucket.
    #[test]
    fn an_added_empty_inf_bucket_is_the_same_histogram() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":1.0,"count":2}]}]}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":1.0,"count":2},{"bound":"inf","count":0}]}]}"#,
        ]);
        assert_eq!(pair(&ins, &outs, Spelling::Prometheus).seen, [Some(Class::Normalized)]);
    }

    /// A histogram read back with its buckets merged pairs as `degraded`, since it holds the same
    /// observations.
    #[test]
    fn a_histogram_with_merged_buckets_pairs_degraded() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":1.0,"count":2},{"bound":"inf","count":1}],"sum":3.0}]}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":"inf","count":3}],"sum":3.0}]}"#,
        ]);
        assert_eq!(pair(&ins, &outs, Spelling::Prometheus).seen, [Some(Class::Degraded)]);
    }

    /// A histogram read back with another total count lost or gained observations, so it doesn't
    /// pair.
    #[test]
    fn a_histogram_whose_count_changed_does_not_pair() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":1.0,"count":2},{"bound":"inf","count":1}],"sum":3.0}]}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":"inf","count":4}],"sum":3.0}]}"#,
        ]);
        let pairing = pair(&ins, &outs, Spelling::Prometheus);
        assert_eq!(pairing.seen, [None]);
        assert_eq!(pairing.unclaimed, [0]);
    }

    /// A histogram read back with another sum changed what was observed, so it doesn't pair.
    #[test]
    fn a_histogram_whose_sum_changed_does_not_pair() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":1.0,"count":2},{"bound":"inf","count":1}],"sum":3.0}]}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":"inf","count":3}],"sum":4.0}]}"#,
        ]);
        let pairing = pair(&ins, &outs, Spelling::Prometheus);
        assert_eq!(pairing.seen, [None]);
        assert_eq!(pairing.unclaimed, [0]);
    }

    /// A point flagged as holding no recorded value pairs with its stale marker, whose value is
    /// `NaN`.
    #[test]
    fn a_flagged_point_pairs_whatever_value_it_reads_back() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":0.0,"flags":1,"no_recorded_value":true}]}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":"NaN","flags":1,"no_recorded_value":true}]}"#,
        ]);
        assert_eq!(pair(&ins, &outs, Spelling::Prometheus).seen, [Some(Class::Exact)]);
    }

    /// `prometheus_out` writes no scrape target: the original target survives as the `instance`
    /// label, so the target reads `normalized` only when that label holds the input's instance.
    #[test]
    fn a_prometheus_target_reads_normalized_when_its_instance_survives() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],"resource":{"attributes":{"_logit.prometheus.target":"http://a:9100/metrics","instance":"a:9100"}}}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],"attributes":{"instance":"a:9100"}}"#,
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],"attributes":{"instance":"b:9100"}}"#,
        ]);
        let rows = field_rows(&ins[0], "prometheus");
        let target = rows
            .iter()
            .find(|(_, f, _)| f.as_deref() == Some("prometheus_target"))
            .map(|r| &r.2)
            .expect("the target's row");
        assert_eq!(
            observe_field(target, &ins[0], &outs[0], Spelling::Prometheus),
            Class::Normalized
        );
        assert_eq!(observe_field(target, &ins[0], &outs[1], Spelling::Prometheus), Class::Lost);
        assert_eq!(observe_field(target, &ins[0], &outs[0], Spelling::Otlp), Class::Lost);
    }

    /// A histogram's `min` and `max` are `record/min_max`'s to compare, so a sink that drops them
    /// still writes the same histogram.
    #[test]
    fn a_histograms_extremes_are_left_to_their_own_row() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":"inf","count":2}],"sum":3.0,"min":1.0,"max":2.0}]}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"h","kind":"histogram","temporality":"cumulative","buckets":[{"bound":"inf","count":2}],"sum":3.0}]}"#,
        ]);
        assert_eq!(pair(&ins, &outs, Spelling::Prometheus).seen, [Some(Class::Exact)]);
        let rows = field_rows(&ins[0], "prometheus");
        let min_max: Vec<&Field> =
            rows.iter().filter(|(_, f, _)| f.as_deref() == Some("min_max")).map(|r| &r.2).collect();
        assert_eq!(min_max.len(), 2);
        for field in min_max {
            assert_eq!(observe_field(field, &ins[0], &outs[0], Spelling::Prometheus), Class::Lost);
        }
    }

    /// Two points sharing an instant that read back at different instants were moved apart.
    #[test]
    fn two_points_of_one_instant_read_back_apart_were_moved() {
        let ins = recs(&[
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}]}"#,
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":2.0}]}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"g.value","kind":"gauge","value":1.0}]}"#,
            r#"{"timestamp":"2027-01-15T08:00:00.000000001Z","metrics":[{"name":"g.value","kind":"gauge","value":2.0}]}"#,
        ]);
        let pairing = pair(&ins, &outs, Spelling::Influxdb);
        assert!(moved_apart(&pairing, 0, 1, &outs));
        assert!(!moved_apart(&pairing, 0, 0, &outs));
    }

    /// [`check`] over `ins` read back as `outs` by a `prometheus_out` with `config`, against the
    /// committed table.
    fn check_prometheus(ins: &[&str], outs: &[&str], config: Value) -> Vec<String> {
        let batches = |lines: &[&str]| {
            let text: String = lines
                .iter()
                .map(|line| line.replace("{T}", "\"timestamp\":\"2027-01-15T08:00:00.000000000Z\""))
                .map(|line| format!("{line}\n"))
                .collect();
            EventJsonDecoder::new().decode_batches(text.as_bytes()).expect("JSON lines")
        };
        let table = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../spec/transit/mapping.yaml");
        let mapping = mapping::load(&table).expect("the committed table loads");
        let traced = Traced {
            outcome: super::super::Outcome { events: Vec::new(), counters: BTreeMap::new() },
            sunk: batches(ins),
            decoded: batches(outs),
            sink: config,
        };
        let at = At { flow: "test", case: "test" };
        check(&mapping, &at, &traced, &mut Ledger::default())
    }

    /// Under exposition, a record a later record of its series overwrote in the registry is
    /// credited to its cell's `normalized` class, though nothing reads back for it.
    #[test]
    fn an_exposed_record_a_later_one_of_its_series_overwrote_is_normalized() {
        let ins = [
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1.0}],"attributes":{"a":"x"}}"#,
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":2.0}],"attributes":{"a":"x"}}"#,
        ];
        let outs =
            [r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":2.0}],"attributes":{"a":"x"}}"#];
        let exposed = serde_json::json!({"type": "prometheus_out", "bind": "127.0.0.1:9464"});
        let failures = check_prometheus(&ins, &outs, exposed);
        assert!(failures.is_empty(), "{failures:#?}");
    }

    /// The credit needs a later record of the record's own series: an unread record whose cell
    /// is `normalized` for another reason, beside a later record of another series, still fails.
    #[test]
    fn an_exposed_record_with_no_later_record_of_its_series_still_fails() {
        let ins = [
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":1}],"attributes":{"a":"x"}}"#,
            r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":2}],"attributes":{"a":"y"}}"#,
        ];
        let outs =
            [r#"{{T},"metrics":[{"name":"g","kind":"gauge","value":2.0}],"attributes":{"a":"y"}}"#];
        let exposed = serde_json::json!({"type": "prometheus_out", "bind": "127.0.0.1:9464"});
        let failures = check_prometheus(&ins, &outs, exposed);
        assert_eq!(failures.len(), 1, "{failures:#?}");
        assert!(failures[0].contains("isn't read back"), "{failures:#?}");
    }

    /// A log with `body` and `attributes`, at the fixed timestamp.
    fn log_line(body: &str, attributes: &str) -> String {
        format!(r#"{{{{T}},"log":{{{body}}},"attributes":{attributes}}}"#)
    }

    /// The class `out` reads at for `input`'s row `log/<feature>` under `spelling`.
    fn log_field(
        input: &Rec,
        out: &Rec,
        protocol: &str,
        feature: &str,
        spelling: Spelling,
    ) -> Class {
        let rows = field_rows(input, protocol);
        let (_, _, field) = rows
            .iter()
            .find(|(kind, f, _)| kind == "log" && f.as_deref() == Some(feature))
            .unwrap_or_else(|| panic!("no log/{feature} row"));
        observe_field(field, input, out, spelling)
    }

    /// A log read back with another severity still pairs, and the difference lands on
    /// `log/severity`, not on the record.
    #[test]
    fn a_log_whose_severity_differs_lands_on_its_severity_row() {
        let ins = recs(&[&log_line(r#""severity":"warn","format":"raw","message":"m""#, "{}")]);
        let outs = recs(&[&log_line(r#""severity":"info","format":"raw","message":"m""#, "{}")]);
        assert_eq!(pair(&ins, &outs, Spelling::Otlp).seen, [Some(Class::Exact)]);
        assert_eq!(
            log_field(&ins[0], &outs[0], "otlp", "severity", Spelling::Otlp),
            Class::Normalized
        );
    }

    /// A log read back with another observed time still pairs, and the difference lands on
    /// `log/observed_timestamp`.
    #[test]
    fn a_log_whose_observed_time_differs_lands_on_its_observed_timestamp_row() {
        let body = |observed: &str| {
            format!(r#""format":"raw","message":"m","observed_timestamp":"{observed}""#)
        };
        let ins = recs(&[&log_line(&body("2027-01-15T08:00:01.000000000Z"), "{}")]);
        let outs = recs(&[&log_line(&body("2027-01-15T08:00:02.000000000Z"), "{}")]);
        assert_eq!(pair(&ins, &outs, Spelling::Otlp).seen, [Some(Class::Exact)]);
        assert_eq!(
            log_field(&ins[0], &outs[0], "otlp", "observed_timestamp", Spelling::Otlp),
            Class::Normalized
        );
    }

    /// `datadog_out` writes a bytes message as lossy UTF-8, so the log pairs respelled and
    /// reaches `log/message_unsafe`, which the check reads at the pairing's class.
    #[test]
    fn datadog_pairs_a_rewritten_message_respelled() {
        let ins = recs(&[
            r#"{{T},"log":{"severity":"warn","format":"raw","message":"b\"caf\\xe9\""},"attributes":{"host.name":"h"},"types":{"/log/message":"bytes"}}"#,
        ]);
        let outs = recs(&[&log_line(
            r#""severity":"warn","format":"raw","message":"caf\ufffd""#,
            r#"{"host.name":"h","status":"warn"}"#,
        )]);
        assert_eq!(pair(&ins, &outs, Spelling::Datadog).seen, [Some(Class::Normalized)]);
        assert_eq!(pair(&ins, &outs, Spelling::Verbatim).seen, [None]);
        assert!(log_features(&ins[0], Spelling::Datadog).contains(&"message_unsafe"));
        assert!(!log_features(&ins[0], Spelling::Otlp).contains(&"message_unsafe"));
    }

    /// A log beside metrics reaches the `log` field rows a log alone does, so a severity read back
    /// changed lands on `log/severity` there too.
    #[test]
    fn a_log_beside_metrics_reaches_its_field_rows() {
        let line = |severity: &str| {
            format!(
                r#"{{{{T}},"log":{{"severity":"{severity}","format":"raw","message":"m"}},"metrics":[{{"name":"g","kind":"gauge","value":1.0}}]}}"#
            )
        };
        let ins = recs(&[&line("warn")]);
        let outs = recs(&[&line("info")]);
        let log = ins.iter().position(|rec| rec.kind == "log").expect("a log record");
        let out = outs.iter().find(|rec| rec.kind == "log").expect("a log record");
        assert!(ins[log].beside_metrics);
        assert_eq!(
            log_field(&ins[log], out, "otlp", "severity", Spelling::Otlp),
            Class::Normalized
        );
    }

    /// A log with no severity reads `log/severity_unset` exact when it reads back with none, and
    /// normalized when the sink stamps one.
    #[test]
    fn a_log_with_no_severity_reads_normalized_only_when_stamped() {
        let ins = recs(&[&log_line(r#""format":"raw","message":"m""#, "{}")]);
        let bare = recs(&[&log_line(r#""format":"raw","message":"m""#, "{}")]);
        let stamped = recs(&[&log_line(r#""severity":"info","format":"raw","message":"m""#, "{}")]);
        let at = |out: &Rec| log_field(&ins[0], out, "otlp", "severity_unset", Spelling::Otlp);
        assert_eq!(at(&bare[0]), Class::Exact);
        assert_eq!(at(&stamped[0]), Class::Normalized);
    }

    /// `datadog_out` writes a log's trace and span ids as `trace_id` and `span_id` keys, read
    /// back as attributes; they pair only holding the log's own ids.
    #[test]
    fn datadog_pairs_a_logs_trace_ids_read_back_as_attributes() {
        let trace = r#""trace_id":"0102030405060708090a0b0c0d0e0f10","span_id":"1112131415161718""#;
        let ins = recs(&[&log_line(
            &format!(r#""severity":"warn","format":"raw","message":"m",{trace}"#),
            r#"{"host.name":"h"}"#,
        )]);
        let read = |attributes: &str| {
            recs(&[&log_line(r#""severity":"warn","format":"raw","message":"m""#, attributes)])
        };
        let same = read(&format!(r#"{{"host.name":"h","status":"warn",{trace}}}"#));
        let other = read(
            r#"{"host.name":"h","status":"warn","trace_id":"00000000000000000000000000000001","span_id":"1112131415161718"}"#,
        );
        assert_eq!(pair(&ins, &same, Spelling::Datadog).seen, [Some(Class::Exact)]);
        assert_eq!(pair(&ins, &same, Spelling::Verbatim).seen, [None]);
        assert_eq!(pair(&ins, &other, Spelling::Datadog).seen, [None]);
        assert_eq!(
            log_field(&ins[0], &same[0], "datadog", "trace", Spelling::Datadog),
            Class::Normalized
        );
    }

    const SYSLOG_5424: Spelling =
        Spelling::Syslog { rfc3164: false, facility: 16, structured_data: false };
    const SYSLOG_3164: Spelling =
        Spelling::Syslog { rfc3164: true, facility: 16, structured_data: false };

    /// A log `syslog_in` reads back from `syslog_out` with the facility, host, and timestamp the
    /// sink filled in still pairs with the log that carried none of them.
    #[test]
    fn a_syslog_read_back_pairs_beside_the_header_it_fills_in() {
        let ins = recs(&[&log_line(r#""severity":"info","format":"raw","message":"m""#, "{}")]);
        let outs = recs(&[&format!(
            r#"{{{{T}},"log":{{"severity":"info","format":"raw","message":"m"}},"attributes":{{"_logit.syslog.severity":6,"host.name":"{host}","syslog.facility":16,"syslog.timestamp":"2027-01-15T08:00:00.000000000Z"}},"types":{{"/attributes/_logit.syslog.severity":"u64","/attributes/syslog.facility":"u64","/attributes/syslog.timestamp":"timestamp"}}}}"#,
            host = super::super::HOSTNAME,
        )]);
        assert_eq!(pair(&ins, &outs, SYSLOG_5424).seen, [Some(Class::Exact)]);
    }

    /// Under RFC 3164 the log's timestamp leaves in whole seconds with no year, read back as
    /// `syslog.timestamp`'s text beside a receipt time that happens to equal it.
    #[test]
    fn an_rfc3164_timestamp_reads_normalized() {
        let ins = recs(&[&log_line(r#""severity":"info","format":"raw","message":"m""#, "{}")]);
        let outs = recs(&[&log_line(
            r#""severity":"info","format":"raw","message":"m""#,
            r#"{"syslog.timestamp":"Jan 15 08:00:00"}"#,
        )]);
        assert_eq!(
            log_field(&ins[0], &outs[0], "otlp", "timestamp", SYSLOG_3164),
            Class::Normalized
        );
    }

    /// `_logit.syslog.severity` has its own row, beside every other protocol's carriers.
    #[test]
    fn the_syslog_severity_carrier_lands_on_its_own_row() {
        let ins = recs(&[
            r#"{{T},"log":{"severity":"info","format":"raw","message":"m"},"attributes":{"_logit.syslog.severity":5},"types":{"/attributes/_logit.syslog.severity":"u64"}}"#,
        ]);
        for protocol in ["syslog", "otlp"] {
            let features: Vec<_> = field_rows(&ins[0], protocol)
                .into_iter()
                .filter(|(kind, _, _)| kind == "attributes")
                .filter_map(|(_, feature, _)| feature)
                .collect();
            assert!(features.contains(&"severity_carrier_syslog".to_string()), "{features:?}");
        }
    }

    /// RFC 3164 has no MSGID: a log's `syslog.msgid` reaches its own row and reads `lost` there,
    /// and `exact` under RFC 5424.
    #[test]
    fn a_msgid_under_rfc3164_reads_lost() {
        let body = r#""severity":"info","format":"raw","message":"m""#;
        let ins = recs(&[&log_line(body, r#"{"syslog.msgid":"req","syslog.tag":"app"}"#)]);
        let without = recs(&[&log_line(body, r#"{"syslog.tag":"app"}"#)]);
        let rows = field_rows(&ins[0], "syslog");
        let (_, _, field) = rows
            .iter()
            .find(|(kind, feature, _)| {
                kind == "attributes" && feature.as_deref() == Some("syslog_msgid")
            })
            .expect("a syslog_msgid row");
        assert_eq!(observe_field(field, &ins[0], &without[0], SYSLOG_3164), Class::Lost);
        assert_eq!(observe_field(field, &ins[0], &ins[0], SYSLOG_5424), Class::Exact);
    }
}
