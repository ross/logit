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
//! type, and `negative` for a gauge below zero; for a `histogram` or `exponential_histogram`,
//! its temporality; any other kind, its kind's one row. It also reaches a `record` row for each of
//! `unit`, `description`, `start_timestamp`, `exemplars` (`exemplars_on_summary` for a
//! `summary`), and `no_recorded_value` (`no_recorded_value_delta_sum` for a delta `sum`,
//! `no_recorded_value_other_kind` for a kind other than `sum` and `gauge`) it carries,
//! `service_check` when it carries `_logit.statsd.service_check.name`, `name_unsafe` when the
//! sink's wire rewrites a character of its name ([`Spelling`]), `non_finite` for a `sum` or
//! `gauge` holding `NaN` or an infinity, and `superseded` when a later record of the run is
//! another point of its series (the same name, kind, and public attributes), with `same_instant`
//! when that point shares its timestamp or else `same_millisecond` when it shares its
//! millisecond. A `histogram` or `exponential_histogram` reaches the field row `record/min_max`
//! for its `min` and `max`, which its own comparison leaves out, as a flagged point's leaves out
//! its value: a stale marker reads back as `NaN`. It reaches an
//! `attributes` row for each attribute, event or resource: a `_logit.<protocol>.` carrier of the
//! sink's own protocol is `private_carrier`, any other `_logit.` key `foreign_carrier`, an event
//! attribute by its value (`string_value`, `bool_value`, `number_value`, `array_value`,
//! `map_value`, `bytes_value`, `timestamp_value`), and any other resource attribute
//! `resource_attribute`; and `attributes/scope` when its batch has a scope. A log
//! record on an event with no metrics reaches `log/dogstatsd_event` when it carries
//! `_logit.statsd.event.title`, else `log/plain`, and its attribute rows; a span there reaches
//! `span/alone`. A log or span on an event that carries metrics is a field of that event: it
//! reaches `log/beside_metrics` or `span/beside_metrics` alone, read as `exact` when it pairs
//! unchanged, `normalized` when it pairs changed, and `lost` when nothing read back pairs with
//! it.
//!
//! **Pairing.** Each input record is paired with the records read back that account for it, in
//! passes, each claiming a read-back record at most once: (1) the same name and the same kind,
//! value, and type tags (`exact`); (2) the same name, kind, temporality, and monotonicity and the
//! same value read as a number, or for `samples` and `set_members` several read-back records whose
//! values together are the input's (`normalized`); a histogram that gained an empty `+Inf`
//! bucket is the same value; (3) the same name in another kind, or names under `<name>.`, the
//! dotted series of an expansion, one of each dotted name per record when several points of a
//! series expand (`degraded`). Passes (1) and (2) run by name and then by the name as the sink
//! writes it, where an `exact` pairing reads as `normalized`, before pass (3) runs either way, so
//! a dotted expansion never claims a record a respelling reads back whole. Within a pass the
//! latest record goes first, so where a sink keeps one point per series, the earlier point is the
//! one left over.
//!
//! [`Spelling`] holds each sink's rewrites, from its codec's module doc: `statsd_out`'s reserved
//! characters as `_`; `prometheus_out`'s `sanitize_metric_name`, `_total` on a counter, a label
//! key's `sanitize_label_name`, and an array's last element; `influxdb_out`'s `<name>.value` for
//! a `sum`, `gauge`, or `set` off the carrier path, and an array's last element; `otlp_out`'s
//! timestamp value as an integer of nanoseconds. A candidate's public attributes (no `_logit.`
//! key, values compared as text, each key and value as the sink writes it) must equal the
//! input's, or failing any that do, be a subset of them. An input left unpaired was skipped or
//! dropped: the class is the one whose counter moved, or the cell's when it says the loss is
//! `uncounted`. A read-back record no input claims fails the check with both sides printed.
//!
//! The check compares no timestamp but one: a `same_instant` record paired `exact` whose read-back
//! instant differs from its twin's reads `normalized`, since the sink moved them apart.
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
//! and absent is `lost`. The observed class may be better than the cell's but never worse. A
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
//! move: `statsd_out`, `influxdb_out`, and `prometheus_out` test the
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
use logit_core::EventBatch;
use logit_outputs::ndjson::render_ndjson;
use logit_proto::influxdb::ATTR_MEASUREMENT;
use logit_proto::prometheus::{sanitize_label_name, sanitize_metric_name};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Body keys checked by `record` field rows, not by the record's own comparison.
const FIELD_KEYS: [&str; 6] =
    ["unit", "description", "start_timestamp", "exemplars", "flags", "no_recorded_value"];

/// Series name suffixes a run's sink counters are checked for.
const OUTCOMES: [&str; 6] = ["dropped", "skipped", "degraded", "normalized", "omitted", "evicted"];

/// One metric, log, or span record, as `format: json` writes it.
#[derive(Debug, Clone)]
struct Rec {
    /// The metric kind, `log`, or `span`.
    kind: String,
    /// The metric name, the log's message, or the span's name.
    name: String,
    /// The metric object, or the `log` or `span` object.
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
}

impl Rec {
    fn is_metric(&self) -> bool {
        !matches!(self.kind.as_str(), "log" | "span")
    }

    /// `metric`, `log`, or `span`: only records of one family pair.
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
        let mut body = self.body.clone();
        if let Some(object) = body.as_object_mut() {
            for key in FIELD_KEYS {
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
                !FIELD_KEYS.iter().any(|key| pointer.starts_with(&format!("/{key}")))
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
            if !name.starts_with("_logit.") {
                let tags = tags.map(String::as_str).unwrap_or("");
                out.insert(spelling.key(name), spelling.value(value, tags));
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
    /// `statsd_out`: a name with each character outside `[A-Za-z0-9._/-]` as `_`, on both sides.
    Statsd,
    /// `prometheus_out`: `sanitize_metric_name`, plus `_total` on a counter that lacks it;
    /// `sanitize_label_name` on every key; an array's last element as its label value.
    Prometheus,
    /// `influxdb_out`: a `sum`, `gauge`, or `set` off the carrier path leaves as field `value`
    /// of a measurement named for the record, which `influxdb_in` reads back as `<name>.value`;
    /// an array's last element as its tag value.
    Influxdb,
    /// `otlp_out`: names and keys as written; a timestamp value as an integer of Unix
    /// nanoseconds.
    Otlp,
    /// Any other sink: names, keys, and values as written.
    Verbatim,
}

impl Spelling {
    fn of(sink: &str) -> Self {
        match sink {
            "statsd_out" => Spelling::Statsd,
            "prometheus_out" => Spelling::Prometheus,
            "influxdb_out" => Spelling::Influxdb,
            "otlp_out" => Spelling::Otlp,
            _ => Spelling::Verbatim,
        }
    }

    /// `name` with the characters the sink's wire can't carry rewritten; a record whose name this
    /// changes reaches `record/name_unsafe`.
    fn sanitize(self, name: &str) -> String {
        match self {
            Spelling::Statsd => sanitized(name),
            Spelling::Prometheus => sanitize_metric_name(name).into_owned(),
            Spelling::Influxdb | Spelling::Otlp | Spelling::Verbatim => name.to_string(),
        }
    }

    /// The name the sink's pair decoder reads `rec`, a sink input, back under.
    fn written(self, rec: &Rec) -> String {
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
            _ => self.sanitize(&rec.name),
        }
    }

    /// The name a read-back record is compared under.
    fn read(self, rec: &Rec) -> String {
        match self {
            Spelling::Statsd => sanitized(&rec.name),
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
            _ => key.to_string(),
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
            _ => text(value),
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

/// `name` with each character outside `[A-Za-z0-9._/-]` read as `_`.
fn sanitized(name: &str) -> String {
    name.chars().map(|c| if safe(c) { c } else { '_' }).collect()
}

fn safe(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-')
}

/// RFC 6901's unescaping of one pointer token.
fn unescape(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

/// Every record in `batches`, in batch order.
fn records(batches: &[EventBatch]) -> Vec<Rec> {
    let mut lines = String::new();
    for batch in batches {
        render_ndjson(&mut lines, batch);
    }
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
            let scored: Vec<(usize, u8)> = outs
                .iter()
                .enumerate()
                .filter(|(o, out)| {
                    !used[*o] && out.family() == input.family() && out_names[*o] == in_names[i]
                })
                .map(|(o, out)| (o, score(input, out, spelling)))
                .filter(|(_, s)| *s > 0)
                .collect();
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
            if let Some(o) = candidates(i, &used).into_iter().find(|&o| outs[o].core() == core) {
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
                .filter(|&o| outs[o].identity() == input.identity())
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
            let direct = candidates(i, &used);
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
            let base = if respelled && spelling == Spelling::Statsd {
                sanitized(&input.name)
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

/// 2 when `out`'s public attributes equal `input`'s as the sink spells their keys, 1 when
/// they're a subset, else 0.
fn score(input: &Rec, out: &Rec, spelling: Spelling) -> u8 {
    let (want, have) = (input.public_as(spelling), out.public());
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
        "log" => a.body["message"] == b.body["message"],
        // A histogram whose last bound is finite gains an empty `+Inf` bucket on a wire that
        // states the total as one (Prometheus).
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

/// Read-back `samples` or `set_members` records whose values together are `input`'s, or none.
fn gather(input: &Rec, candidates: &[usize], outs: &[Rec]) -> Vec<usize> {
    let field = if input.kind == "samples" { "values" } else { "members" };
    // A member compares by its sanitized text, since a sink may rewrite a reserved character.
    let values = |rec: &Rec| -> Vec<String> {
        rec.body[field]
            .as_array()
            .into_iter()
            .flatten()
            .map(|v| sanitized(&v.to_string()))
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
            features
        }
        "gauge_delta" => vec![Some(scalar("delta"))],
        "histogram" | "exponential_histogram" => vec![Some(temporality())],
        "log" | "span" if rec.beside_metrics => vec![Some("beside_metrics".to_string())],
        "span" => vec![Some("alone".to_string())],
        "log" if rec.attributes.contains_key("_logit.statsd.event.title") => {
            vec![Some("dogstatsd_event".to_string())]
        }
        "log" => vec![Some("plain".to_string())],
        _ => vec![None],
    }
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
    /// An event attribute, or a resource attribute.
    Attribute { key: String, resource: bool },
    /// The batch's scope.
    Scope,
}

/// The field rows `rec` reaches, for a sink of `protocol`.
fn field_rows(rec: &Rec, protocol: &str) -> Vec<(String, Option<String>, Field)> {
    let mut rows = Vec::new();
    if rec.is_metric() {
        for key in mapping::RECORD_FIELDS {
            if rec.body.get(key).is_some() {
                let feature = match key {
                    "exemplars" if rec.kind == "summary" => "exemplars_on_summary",
                    key => key,
                };
                rows.push(("record".to_string(), Some(feature.to_string()), Field::Body(key)));
            }
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
    let own = format!("_logit.{protocol}.");
    let carrier = |key: &str| {
        if key.starts_with(&own) {
            Some("private_carrier")
        } else if key.starts_with("_logit.") {
            Some("foreign_carrier")
        } else {
            None
        }
    };
    for (key, value) in &rec.attributes {
        let tags = rec.attribute_types.get(key).map(String::as_str).unwrap_or("");
        let feature = if let Some(feature) = carrier(key) {
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
    for key in rec.resource.keys() {
        let feature = carrier(key).unwrap_or("resource_attribute");
        let field = Field::Attribute { key: key.clone(), resource: true };
        rows.push(("attributes".to_string(), Some(feature.to_string()), field));
    }
    if rec.scope.is_some() {
        rows.push(("attributes".to_string(), Some("scope".to_string()), Field::Scope));
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
        Field::Body(key) => match out.body.get(*key) {
            Some(value) if *value == input.body[*key] => Class::Exact,
            Some(_) => Class::Normalized,
            None => Class::Lost,
        },
        Field::Attribute { key, resource } => {
            let (want, want_tags) = if *resource {
                (&input.resource[key], input.resource_types.get(key))
            } else {
                (&input.attributes[key], input.attribute_types.get(key))
            };
            // A key the sink respells reads back under its wire spelling, which is a change.
            let wire = spelling.key(key);
            let respelled = wire != *key;
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
    let spelling = Spelling::of(&sink);
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
        if !input.is_metric() && input.beside_metrics {
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
            continue;
        }

        // Record level.
        let mut rows: Vec<(String, Option<String>)> =
            kind_features(input).into_iter().map(|f| (input.kind.clone(), f)).collect();
        let mut features = record_features(input, spelling);
        let later: Vec<usize> = (i + 1..ins.len())
            .filter(|&j| input.is_metric() && same_series(input, &ins[j]))
            .collect();
        if !later.is_empty() {
            features.push("superseded");
        }
        // The later point of its series at this record's instant, if any.
        let twin = later.iter().copied().find(|&j| ins[j].timestamp == input.timestamp);
        if twin.is_some() {
            features.push("same_instant");
        } else if later.iter().any(|&j| same_millisecond(input, &ins[j])) {
            features.push("same_millisecond");
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
        let worst: Vec<_> = cells.iter().filter(|(_, cell, _)| cell.class == expected).collect();
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
                let loss = deciding
                    .filter(|(_, cell, _)| matches!(cell.class, Class::Skipped | Class::Dropped));
                let counted =
                    loss.and_then(|(_, cell, _)| cell.counter.as_deref()).is_some_and(&did_move);
                // A cell that says its loss is uncounted accounts for a record no counter does.
                let uncounted = loss.is_some_and(|(_, cell, _)| {
                    cell.counter.is_none() && cell.reason.as_deref() == Some("uncounted")
                });
                if counted || uncounted {
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
        let read_back: Vec<String> = pairing.claimed[i].iter().map(|&o| outs[o].show()).collect();
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
            if *label != deciding.map(|d| d.0.clone()).unwrap_or_default() && cell.class == observed
            {
                ledger.saw(&sink, label, *index, observed);
            }
        }

        // Field level.
        let first = pairing.claimed[i].first().copied();
        let checked = matches!(observed, Class::Exact | Class::Normalized);
        for (kind, feature, field) in field_rows(input, &protocol) {
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
    use logit_proto::event_json::EventJsonDecoder;

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
            r#"{{T},"metrics":[{"name":"cpu.usage","kind":"gauge","value":1.5}],"attributes":{"influxdb.field.note":"x"}}"#,
            r#"{{T},"metrics":[{"name":"jobs","kind":"sum","value":3.0,"temporality":"cumulative","monotonic":true}]}"#,
        ]);
        let outs = recs(&[
            r#"{{T},"metrics":[{"name":"cpu_usage","kind":"gauge","value":1.5}],"attributes":{"influxdb_field_note":"x"}}"#,
            r#"{{T},"metrics":[{"name":"jobs_total","kind":"sum","value":3.0,"temporality":"cumulative","monotonic":true}]}"#,
        ]);
        let pairing = pair(&ins, &outs, Spelling::Prometheus);
        assert_eq!(pairing.seen, [Some(Class::Normalized), Some(Class::Normalized)]);
        assert!(pairing.unclaimed.is_empty());
        let note = Field::Attribute { key: "influxdb.field.note".to_string(), resource: false };
        assert_eq!(
            observe_field(&note, &ins[0], &outs[0], Spelling::Prometheus),
            Class::Normalized
        );

        let verbatim = pair(&ins, &outs, Spelling::Verbatim);
        assert_eq!(verbatim.seen, [None, None]);
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
        assert_eq!(pair(&ins, &outs, Spelling::Statsd).seen, [Some(Class::Normalized)]);
        assert_eq!(pair(&ins, &outs, Spelling::Verbatim).seen, [None]);
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
}
