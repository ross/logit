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
//! `unit`, `description`, `start_timestamp`, `exemplars`, and `no_recorded_value` it carries,
//! `service_check` when it carries `_logit.statsd.service_check.name`, and `name_unsafe` when its
//! name holds a character outside `[A-Za-z0-9._/-]`; and an `attributes` row for each attribute:
//! a `_logit.<protocol>.` carrier of the sink's own protocol is `private_carrier`, any other
//! `_logit.` key `foreign_carrier`, and the rest by value (`string_value`, `bool_value`,
//! `number_value`, `array_value`), plus `resource_attribute` for each resource attribute. A log
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
//! values together are the input's (`normalized`); (3) the same name in another kind, or names
//! under `<name>.`, the dotted series of an expansion (`degraded`). Passes (1) to (3) run once
//! by name and again by the name with each character outside `[A-Za-z0-9._/-]` read as `_`, where
//! an `exact` pairing reads as `normalized`. A candidate's public attributes (no `_logit.` key,
//! values compared as text) must equal the input's, or failing any that do, be a subset of them.
//! An input left unpaired was skipped or dropped: the class is the one whose counter moved. A
//! read-back record no input claims fails the check with both sides printed.
//!
//! **Record level.** The expected class is the worst (`exact < normalized < degraded < skipped
//! < dropped`) over the record-level cells it reaches, and the observed class must equal it; a
//! `normalized` expectation also accepts `exact`, since a normalization rewrites only the shapes
//! it names.
//!
//! **Field level.** For a record observed `exact` or `normalized`, each field row compares the
//! input's field with its first pairing's: the same value in the same place is `exact`, present
//! but changed or moved (a resource attribute read back as an event attribute) is `normalized`,
//! and absent is `lost`. The observed class may be better than the cell's but never worse. A
//! degraded record's fields aren't checked: the carriers that describe its wire form describe a
//! shape that's gone.
//!
//! **Counters.** Every sink series a run moved whose name ends in `dropped`, `skipped`,
//! `degraded`, `normalized`, `omitted`, or `evicted` must be named by a cell the case reached. A
//! `degraded`, `skipped`, `dropped`, or `lost` cell observed at its class must see its counter
//! move. A `normalized` cell's counter needn't, since not every record it covers is rewritten.
//!
//! A `when:` key that names no field of the sink's serialized config fails, naming the row, the
//! cell, and the key: a misspelled key would otherwise never match.
//!
//! **Across the whole run** ([`Ledger`]), every `normalized` or `lost` cell some record reached
//! must be observed at its class at least once, so a bound can't hide an improvement forever, and
//! `coverage.md` lists every cell no record was checked against.

use super::mapping::{self, Class, Lookup, Mapping, Series};
use super::Traced;
use logit_core::EventBatch;
use logit_outputs::ndjson::render_ndjson;
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
        }
        let types = self
            .types
            .iter()
            .filter(|(pointer, _)| {
                !FIELD_KEYS.iter().any(|key| pointer.starts_with(&format!("/{key}")))
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
        let mut out = BTreeMap::new();
        for (key, value) in self.resource.iter().chain(self.attributes.iter()) {
            if !key.starts_with("_logit.") {
                out.insert(key.clone(), text(value));
            }
        }
        out
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
fn pair(ins: &[Rec], outs: &[Rec]) -> Pairing {
    let mut used = vec![false; outs.len()];
    let mut seen: Vec<Option<Class>> = vec![None; ins.len()];
    let mut claimed: Vec<Vec<usize>> = vec![Vec::new(); ins.len()];
    for by_sanitized in [false, true] {
        let name = |s: &str| if by_sanitized { sanitized(s) } else { s.to_string() };
        let candidates = |i: usize, used: &[bool]| -> Vec<usize> {
            let input = &ins[i];
            let scored: Vec<(usize, u8)> = outs
                .iter()
                .enumerate()
                .filter(|(o, out)| {
                    !used[*o]
                        && out.family() == input.family()
                        && name(&out.name) == name(&input.name)
                })
                .map(|(o, out)| (o, score(input, out)))
                .filter(|(_, s)| *s > 0)
                .collect();
            let best = scored.iter().map(|(_, s)| *s).max().unwrap_or(0);
            scored.into_iter().filter(|(_, s)| *s == best).map(|(o, _)| o).collect()
        };
        let renamed =
            |i: usize, o: usize| name(&ins[i].name) != ins[i].name || outs[o].name != ins[i].name;

        for i in 0..ins.len() {
            if seen[i].is_some() {
                continue;
            }
            let core = ins[i].core();
            if let Some(o) = candidates(i, &used).into_iter().find(|&o| outs[o].core() == core) {
                used[o] = true;
                claimed[i] = vec![o];
                seen[i] = Some(if renamed(i, o) { Class::Normalized } else { Class::Exact });
            }
        }

        for i in 0..ins.len() {
            if seen[i].is_some() {
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
        order.sort_by_key(|&i| std::cmp::Reverse(ins[i].name.len()));
        for i in order {
            if seen[i].is_some() {
                continue;
            }
            let input = &ins[i];
            let direct = candidates(i, &used);
            let sharing = (0..ins.len())
                .filter(|&j| {
                    seen[j].is_none()
                        && name(&ins[j].name) == name(&input.name)
                        && ins[j].public() == input.public()
                })
                .count();
            let mut picked: Vec<usize> =
                if sharing == 1 { direct } else { direct.into_iter().take(1).collect() };
            let prefix = format!("{}.", name(&input.name));
            let dotted: Vec<usize> = (0..outs.len())
                .filter(|&o| {
                    !used[o]
                        && !picked.contains(&o)
                        && outs[o].is_metric()
                        && input.is_metric()
                        && name(&outs[o].name).starts_with(&prefix)
                        && score(input, &outs[o]) > 0
                })
                .collect();
            picked.extend(dotted);
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

/// 2 when `out`'s public attributes equal `input`'s, 1 when they're a subset, else 0.
fn score(input: &Rec, out: &Rec) -> u8 {
    let (want, have) = (input.public(), out.public());
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
fn record_features(rec: &Rec) -> Vec<&'static str> {
    let mut features = Vec::new();
    if !rec.is_metric() {
        return features;
    }
    if rec.body["no_recorded_value"] == Value::Bool(true) {
        features.push("no_recorded_value");
    }
    if rec.attributes.contains_key("_logit.statsd.service_check.name") {
        features.push("service_check");
    }
    if !rec.name.chars().all(safe) {
        features.push("name_unsafe");
    }
    features
}

/// Where a field row looks.
#[derive(Debug, Clone)]
enum Field {
    /// A key of the metric object.
    Body(&'static str),
    /// An event attribute, or a resource attribute.
    Attribute { key: String, resource: bool },
}

/// The field rows `rec` reaches, for a sink of `protocol`.
fn field_rows(rec: &Rec, protocol: &str) -> Vec<(String, Option<String>, Field)> {
    let mut rows = Vec::new();
    if rec.is_metric() {
        for key in mapping::RECORD_FIELDS {
            if rec.body.get(key).is_some() {
                rows.push(("record".to_string(), Some(key.to_string()), Field::Body(key)));
            }
        }
    }
    let own = format!("_logit.{protocol}.");
    for (key, value) in &rec.attributes {
        let tags = rec.attribute_types.get(key).map(String::as_str).unwrap_or("");
        let feature = if key.starts_with(&own) {
            "private_carrier"
        } else if key.starts_with("_logit.") {
            "foreign_carrier"
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
        let field = Field::Attribute { key: key.clone(), resource: true };
        rows.push(("attributes".to_string(), Some("resource_attribute".to_string()), field));
    }
    rows
}

/// What a field reads back as in `out`.
fn observe_field(field: &Field, input: &Rec, out: &Rec) -> Class {
    match field {
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
            let (same, same_tags, other) = if *resource {
                (out.resource.get(key), out.resource_types.get(key), out.attributes.get(key))
            } else {
                (out.attributes.get(key), out.attribute_types.get(key), out.resource.get(key))
            };
            match (same, other) {
                (Some(value), _) if value == want && same_tags == want_tags => Class::Exact,
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

/// What the whole run has seen of each bound cell (module doc).
#[derive(Default)]
pub(super) struct Ledger {
    /// `(sink, row, cell index)` to the cell's class and the classes observed under it.
    cells: BTreeMap<(String, String, usize), (Class, BTreeSet<Class>)>,
    /// Rows a sink has no cell in yet, and the cases that reached them.
    unspecified: BTreeMap<(String, String), BTreeSet<String>>,
    /// Every `(sink, row, cell index)` some record was checked against.
    reached: BTreeSet<(String, String, usize)>,
}

impl Ledger {
    fn saw(&mut self, sink: &str, row: &str, index: usize, cell: Class, observed: Class) {
        self.reached.insert((sink.to_string(), row.to_string(), index));
        let entry = self
            .cells
            .entry((sink.to_string(), row.to_string(), index))
            .or_insert_with(|| (cell, BTreeSet::new()));
        entry.1.insert(observed);
    }

    /// One failure per bound cell never observed at its class.
    pub(super) fn finish(&self) -> Vec<String> {
        let mut failures = Vec::new();
        for ((sink, row, index), (class, observed)) in &self.cells {
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
            if !self.reached.contains(&(sink.clone(), row.clone(), index)) {
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
    let pairing = pair(&ins, &outs);
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
                    ledger.saw(&sink, &label, index, cell.class, seen);
                }
            }
            continue;
        }

        // Record level.
        let mut rows: Vec<(String, Option<String>)> =
            kind_features(input).into_iter().map(|f| (input.kind.clone(), f)).collect();
        rows.extend(
            record_features(input).into_iter().map(|f| ("record".to_string(), Some(f.to_string()))),
        );
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
            ledger.reached.insert((sink.clone(), label.clone(), *index));
        }
        let expected = cells.iter().map(|(_, cell, _)| cell.class).fold(Class::Exact, worse);
        let deciding = cells.iter().find(|(_, cell, _)| cell.class == expected);
        let observed = match pairing.seen[i] {
            Some(class) => Some(class),
            None => {
                let counted = deciding
                    .filter(|(_, cell, _)| matches!(cell.class, Class::Skipped | Class::Dropped))
                    .and_then(|(_, cell, _)| cell.counter.as_deref())
                    .is_some_and(&did_move);
                if counted {
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
            ledger.saw(&sink, label, *index, cell.class, observed);
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
                ledger.saw(&sink, label, *index, cell.class, observed);
            }
        }

        // Field level.
        let first = pairing.claimed[i].first().copied();
        let checked = matches!(observed, Class::Exact | Class::Normalized);
        for (kind, feature, field) in field_rows(input, &protocol) {
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
            let Some(first) = first.filter(|_| checked) else { continue };
            let seen = observe_field(&field, input, &outs[first]);
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
            ledger.saw(&sink, &label, index, cell.class, seen);
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
