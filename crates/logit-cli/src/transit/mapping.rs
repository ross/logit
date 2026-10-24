//! The schema of `spec/transit/mapping.yaml`, the transit mapping table, and the rules [`load`]
//! holds it to. `spec/transit/README.md`'s "The mapping table" is the operator-facing account.
//!
//! A **row** is a `kind` and, for a kind that has them, one `feature`:
//!
//! | `kind` | `feature` | Level |
//! |---|---|---|
//! | `sum` | `delta_monotonic`, `delta_nonmonotonic`, `cumulative_monotonic`, `cumulative_nonmonotonic`, `f64`, `i64`, `u64`, `bool` | record |
//! | `gauge` | `f64`, `i64`, `u64`, `bool`, `negative`, `prometheus_info`, `prometheus_stateset` (a gauge flagged `_logit.metrics.type: "info"` or `"stateset"`, as prometheus_in decodes an OpenMetrics `info` or `stateset`) | record |
//! | `gauge_delta` | `f64`, `i64`, `u64`, `bool` | record |
//! | `histogram` | `delta`, `cumulative`, `nan_bound`, `neg_inf_bound` (a `NaN` or a `-Inf` bucket bound), `gauge_histogram` (a histogram flagged `_logit.metrics.type: "gaugehistogram"`, as prometheus_in decodes an OpenMetrics `gaugehistogram`) | record |
//! | `exponential_histogram`, `summary` | `delta`, `cumulative` | record |
//! | `samples`, `set_members`, `distribution`, `set` | none | record |
//! | `record` | `no_recorded_value`, `no_recorded_value_delta_sum`, `no_recorded_value_other_kind`, `service_check`, `name_unsafe`, `exemplar_key_unsafe`, `exemplar_key_unsafe_histogram`, `sample_rate`, `non_finite`, `superseded`, `same_instant`, `same_instant_equal`, `same_millisecond`, `same_millisecond_equal` | record |
//! | `record` | `unit`, `description`, `start_timestamp`, `exemplars`, `exemplars_on_summary`, `unit_on_distribution`, `min_max`, `sample_rate_unweighted` | field |
//! | `attributes` | `string_value`, `bool_value`, `number_value`, `array_value`, `map_value`, `bytes_value`, `timestamp_value`, `private_carrier`, `prometheus_target` (`_logit.prometheus.target`, at every sink), `foreign_carrier`, `timestamped` (`_logit.metrics.closed: "timestamped"`, at every sink), `rate` and `rate_on_non_gauge` (`_logit.metrics.type: "rate"` on a gauge and on any other metric kind, at every sink), `family_type` and `family_type_on_other_kind` (`_logit.metrics.type: "info"` or `"stateset"` on a gauge and `"gaugehistogram"` on a histogram, and any of the three on another metric kind, at every sink), `interval`, `interval_on_distribution`, and `interval_on_histogram` (`_logit.metrics.interval` on a metric record, on a `samples`, `distribution`, or delta `exponential_histogram`, and on a delta `histogram`, at every sink), `datadog_carrier_log`, `resource_attribute`, `scope` | field |
//! | `attributes` | `no_host`, `log_status`, `span_kind`, `trace_id_high` (a record `datadog_out` writes an attribute for from the record's own fields) | field |
//! | `log` | `dogstatsd_event`, `plain` (each on an event with no metrics) | record |
//! | `span` | `alone` (on an event with no metrics); `no_wire_form` (one of those carrying a field a Datadog span has no form for) | record |
//! | `log`, `span` | `beside_metrics` (on an event that carries metrics) | field |
//! | `event` | `string_only` (an InfluxDB point whose fields are all strings, its measurement carrier intact), `attributes_only` (any other event with no metric, log, or span) | record |
//!
//! `attributes/scope` is the batch's instrumentation scope, a field of every record in the batch
//! like a resource attribute, so it's an `attributes` row rather than a kind of its own. A
//! resource attribute under `_logit.` reaches `private_carrier`, `foreign_carrier`, or a flag row, as an event
//! attribute does, not `resource_attribute`; a `_logit.datadog.*` carrier on a log, a Datadog event,
//! or a service check reaches `datadog_carrier_log` in their place, at every sink.
//! `record/non_finite` is a `sum` or `gauge` holding
//! `NaN` or an infinity, a value some wires can't write. `record/superseded` is a metric record a
//! later record of the same series (name, kind, and public attributes) follows in the same run,
//! which a sink holding one value per series replaces; `record/same_instant` is one whose later
//! point shares its timestamp, and `record/same_millisecond` one whose later point falls at
//! another nanosecond of its millisecond, which a wire stamping milliseconds can't tell apart.
//! `record/same_instant_equal` and `record/same_millisecond_equal` are those two where the record
//! is a state (a `gauge` or a cumulative kind) and the latest point of the series in its
//! millisecond is equal to it, so a sink that keeps one point loses nothing; an equal increment
//! reaches the unequal row.
//! `record/name_unsafe` is a name holding a character the sink's wire rewrites, which differs by
//! sink, and `record/exemplar_key_unsafe` a monotonic `sum` whose written exemplar has an
//! attribute key holding a character the sink's label names can't carry,
//! `exemplar_key_unsafe_histogram` such a `histogram`. `record/sample_rate` is a `samples` record whose rate rounds to a weight above 1, and
//! `record/sample_rate_unweighted` the rate of one below 1 that rounds to weight 1, which the
//! record's own comparison leaves to that row. `record/unit_on_distribution` is a `samples` or
//! `distribution` record's unit, where `record/unit` is every other kind's. `record/min_max` is a histogram's `min` and `max`, which the record's own comparison
//! leaves to this row.
//!
//! A `when:` key names sink config only, so where a sink's outcome for a feature depends on the
//! record's kind, the kind is in the feature's name and only that kind reaches the row:
//! `record/no_recorded_value` is a flagged cumulative `sum` or a flagged `gauge`,
//! `record/no_recorded_value_delta_sum` a flagged delta `sum`,
//! `record/no_recorded_value_other_kind` a flagged record of any other kind, and `record/exemplars_on_summary` a `summary`'s exemplars,
//! where `record/exemplars` is every other kind's.
//!
//! A row may have no cell for a sink where no record is compared against it, such as
//! `name_unsafe` at a sink that rewrites no name, or a field row at a sink that writes the kind
//! only in a lesser form, so its fields are never compared; `mapping.yaml` says why beside the
//! row.
//!
//! A record-level row's class is `exact`, `normalized`, `degraded`, `skipped`, or `dropped`; a
//! field-level row's is `exact`, `normalized`, or `lost`.
//!
//! A row's `cells` map a sink to one cell or to a list of them. In a list, every cell but the last
//! has a `when:` map of sink config fields to values, and the first whose fields all equal the
//! flow's sink config (defaults filled in) applies; the last, with no `when:`, applies otherwise.
//! A sink with no cell in a row is "not yet specified" there. A cell needs:
//!
//! - `text`, one operator-facing sentence or two, always;
//! - `reason`, the normalization's name, on a `normalized` cell;
//! - `counter`, the series it counts under (`name{tag=value,...}`, the tags a subset of the
//!   series'), on a `degraded`, `skipped`, `dropped`, or `lost` cell, unless `reason` says why
//!   there's none. A `lost` or `degraded` cell's reason may be `structural`: the destination's
//!   format has no slot for a field (on a `degraded` cell, one the record loses while keeping its
//!   kind and shape), so every such record loses it and the table is its
//!   record; a record written as another kind or shape names its counter (ADR
//!   `telemetry-loss-vocabulary`'s amendment "structural loss has no counter"). `uncounted`, a
//!   loss the sink doesn't count yet, is accepted only on a cell [`UNCOUNTED_ALLOWED`] names, and
//!   the class check accepts it for a record nothing reads back.

use anyhow::{bail, ensure, Context};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// The cells whose loss may still be `uncounted`, as `(row kind, feature, sink)`, where `None`
/// matches any. Every other `skipped`, `dropped`, or `lost` cell names a counter, or a `lost` one
/// is `structural`. Logs and spans aren't classified yet, a `_logit.datadog.*` carrier
/// `datadog_out` leaves off a log among them.
const UNCOUNTED_ALLOWED: [(&str, Option<&str>, Option<&str>); 3] = [
    ("log", None, None),
    ("span", None, None),
    ("attributes", Some("datadog_carrier_log"), Some("datadog_out")),
];

/// Whether the cell of `sink` in row `kind`/`feature` may be `uncounted` ([`UNCOUNTED_ALLOWED`]).
fn uncounted_allowed(kind: &str, feature: Option<&str>, sink: &str) -> bool {
    UNCOUNTED_ALLOWED.iter().any(|&(k, f, s)| {
        k == kind && f.is_none_or(|f| Some(f) == feature) && s.is_none_or(|s| s == sink)
    })
}

/// The class vocabulary, in the order `mapping.yaml`'s `classes:` must list it.
pub(super) const CLASSES: [&str; 6] =
    ["exact", "normalized", "degraded", "lost", "skipped", "dropped"];

/// Each row kind and its features (module doc).
const KINDS: [(&str, &[&str]); 15] = [
    (
        "sum",
        &[
            "delta_monotonic",
            "delta_nonmonotonic",
            "cumulative_monotonic",
            "cumulative_nonmonotonic",
            "f64",
            "i64",
            "u64",
            "bool",
        ],
    ),
    ("gauge", &["f64", "i64", "u64", "bool", "negative", "prometheus_info", "prometheus_stateset"]),
    ("gauge_delta", &["f64", "i64", "u64", "bool"]),
    ("histogram", &["delta", "cumulative", "nan_bound", "neg_inf_bound", "gauge_histogram"]),
    ("exponential_histogram", &["delta", "cumulative"]),
    ("samples", &[]),
    ("set_members", &[]),
    ("distribution", &[]),
    ("set", &[]),
    ("summary", &["delta", "cumulative"]),
    (
        "record",
        &[
            "no_recorded_value",
            "no_recorded_value_delta_sum",
            "no_recorded_value_other_kind",
            "service_check",
            "name_unsafe",
            "exemplar_key_unsafe",
            "exemplar_key_unsafe_histogram",
            "sample_rate",
            "non_finite",
            "superseded",
            "same_instant",
            "same_instant_equal",
            "same_millisecond",
            "same_millisecond_equal",
            "unit",
            "description",
            "start_timestamp",
            "exemplars",
            "exemplars_on_summary",
            "unit_on_distribution",
            "min_max",
            "sample_rate_unweighted",
        ],
    ),
    (
        "attributes",
        &[
            "string_value",
            "host_beside_host_name",
            "hostname",
            "log_service",
            "log_service_field",
            "container_id",
            "bool_value",
            "number_value",
            "array_value",
            "map_value",
            "bytes_value",
            "timestamp_value",
            "private_carrier",
            "prometheus_target",
            "foreign_carrier",
            "timestamped",
            "rate",
            "rate_on_non_gauge",
            "family_type",
            "family_type_on_other_kind",
            "interval",
            "interval_on_distribution",
            "interval_on_histogram",
            "resource_attribute",
            "scope",
            "datadog_carrier_log",
            "no_host",
            "log_status",
            "span_kind",
            "trace_id_high",
        ],
    ),
    ("log", &["dogstatsd_event", "plain", "beside_metrics"]),
    ("span", &["alone", "no_wire_form", "beside_metrics"]),
    ("event", &["attributes_only", "string_only"]),
];

/// The `record` features that are field-level (module doc).
pub(super) const RECORD_FIELDS: [&str; 4] = ["unit", "description", "start_timestamp", "exemplars"];

/// A cell's class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Class {
    Exact,
    Normalized,
    Degraded,
    Lost,
    Skipped,
    Dropped,
}

impl Class {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Class::Exact => "exact",
            Class::Normalized => "normalized",
            Class::Degraded => "degraded",
            Class::Lost => "lost",
            Class::Skipped => "skipped",
            Class::Dropped => "dropped",
        }
    }

    /// Whether a record-level row may hold this class.
    fn is_record_level(self) -> bool {
        self != Class::Lost
    }

    /// Whether a field-level row may hold this class.
    fn is_field_level(self) -> bool {
        matches!(self, Class::Exact | Class::Normalized | Class::Lost)
    }
}

/// The table.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Mapping {
    pub classes: BTreeMap<String, String>,
    pub sinks: Vec<String>,
    pub rows: Vec<Row>,
}

/// One kind and feature, with a cell per sink.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Row {
    pub kind: String,
    #[serde(default)]
    pub feature: Option<String>,
    pub cells: BTreeMap<String, Cells>,
}

/// One sink's cell in a row, or its config-dependent cells.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(super) enum Cells {
    One(Cell),
    Many(Vec<Cell>),
}

impl Cells {
    pub(super) fn as_slice(&self) -> &[Cell] {
        match self {
            Cells::One(cell) => std::slice::from_ref(cell),
            Cells::Many(cells) => cells,
        }
    }
}

/// What one sink does with one row (module doc).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cell {
    #[serde(default)]
    pub when: BTreeMap<String, serde_json::Value>,
    pub class: Class,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub counter: Option<String>,
    pub text: String,
}

/// A counter series a cell names, or one a run moved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Series {
    pub name: String,
    pub tags: BTreeMap<String, String>,
}

impl Series {
    /// Parses `name{tag=value,...}` or a bare `name`.
    pub(super) fn parse(text: &str) -> anyhow::Result<Self> {
        let (name, tags) = match text.split_once('{') {
            Some((name, rest)) => {
                let inner = rest.strip_suffix('}').with_context(|| format!("{text:?}: no `}}`"))?;
                (name, inner)
            }
            None => (text, ""),
        };
        ensure!(!name.is_empty(), "{text:?}: no series name");
        let mut map = BTreeMap::new();
        for tag in tags.split(',').filter(|tag| !tag.is_empty()) {
            let (key, value) =
                tag.split_once('=').with_context(|| format!("{text:?}: a tag without `=`"))?;
            map.insert(key.to_string(), value.to_string());
        }
        Ok(Self { name: name.to_string(), tags: map })
    }

    /// Whether `self`, a cell's series, names `moved`: the same name, and each of `self`'s tags
    /// among `moved`'s.
    pub(super) fn names(&self, moved: &Series) -> bool {
        self.name == moved.name
            && self.tags.iter().all(|(key, value)| moved.tags.get(key) == Some(value))
    }
}

/// A row's place in the table, for messages: `kind` or `kind/feature`.
pub(super) fn row_label(kind: &str, feature: Option<&str>) -> String {
    match feature {
        Some(feature) => format!("{kind}/{feature}"),
        None => kind.to_string(),
    }
}

/// Whether `kind`/`feature` is a field-level row (module doc).
pub(super) fn is_field_row(kind: &str, feature: Option<&str>) -> bool {
    feature == Some("beside_metrics")
        || kind == "attributes"
        || (kind == "record"
            && feature.is_some_and(|f| {
                RECORD_FIELDS.contains(&f)
                    || matches!(
                        f,
                        "exemplars_on_summary"
                            | "unit_on_distribution"
                            | "min_max"
                            | "sample_rate_unweighted"
                    )
            }))
}

/// What the table holds for one row and one sink under one config.
#[derive(Debug, Clone)]
pub(super) enum Lookup<'a> {
    /// The table has no such row: every row a case reaches must be there.
    NoRow,
    /// The row has no cell for the sink yet.
    NotSpecified,
    /// The cell that applies, and its index among the sink's cells in the row.
    Cell(&'a Cell, usize),
}

impl Mapping {
    /// The cell for `kind`/`feature` in `sink`'s column under the sink config `config`.
    pub(super) fn lookup(
        &self,
        kind: &str,
        feature: Option<&str>,
        sink: &str,
        config: &serde_json::Value,
    ) -> Lookup<'_> {
        let Some(row) =
            self.rows.iter().find(|r| r.kind == kind && r.feature.as_deref() == feature)
        else {
            return Lookup::NoRow;
        };
        let Some(cells) = row.cells.get(sink) else {
            return Lookup::NotSpecified;
        };
        for (i, cell) in cells.as_slice().iter().enumerate() {
            if cell.when.iter().all(|(field, want)| config.get(field) == Some(want)) {
                return Lookup::Cell(cell, i);
            }
        }
        // `validate` requires a last cell with no `when:`, which matches every config.
        Lookup::NotSpecified
    }

    /// Every `(sink, row, cell index, cell)` in the table.
    pub(super) fn cells(&self) -> Vec<(String, String, usize, &Cell)> {
        let mut out = Vec::new();
        for row in &self.rows {
            let label = row_label(&row.kind, row.feature.as_deref());
            for (sink, cells) in &row.cells {
                for (i, cell) in cells.as_slice().iter().enumerate() {
                    out.push((sink.clone(), label.clone(), i, cell));
                }
            }
        }
        out
    }

    /// One failure per `when:` key in `sink`'s cells that `config`, the sink's serialized config
    /// with every default filled in, has no field for: a misspelled key would never match.
    pub(super) fn unknown_when_keys(&self, sink: &str, config: &serde_json::Value) -> Vec<String> {
        let mut failures = Vec::new();
        for (cell_sink, row, index, cell) in self.cells() {
            if cell_sink != sink {
                continue;
            }
            for key in cell.when.keys() {
                if config.get(key).is_none() {
                    failures.push(format!(
                        "mapping row {row}, {sink} cell {index}: when: names {key:?}, which isn't \
                         a {sink} config field"
                    ));
                }
            }
        }
        failures
    }

    /// Every rule in the module doc.
    fn validate(&self) -> anyhow::Result<()> {
        let names: Vec<&str> = self.classes.keys().map(String::as_str).collect();
        let mut want = CLASSES.to_vec();
        want.sort_unstable();
        ensure!(names == want, "classes: lists {names:?}, not the vocabulary {CLASSES:?}");
        for (class, text) in &self.classes {
            ensure!(!text.trim().is_empty(), "classes: {class} has no sentence");
        }
        ensure!(!self.sinks.is_empty(), "sinks: lists no sink");
        for (i, sink) in self.sinks.iter().enumerate() {
            ensure!(!self.sinks[..i].contains(sink), "sinks: {sink} is listed twice");
        }

        let mut seen = Vec::new();
        for row in &self.rows {
            let feature = row.feature.as_deref();
            let label = row_label(&row.kind, feature);
            let Some((_, features)) = KINDS.iter().find(|(k, _)| *k == row.kind) else {
                bail!("row {label}: no kind {:?}", row.kind);
            };
            match feature {
                None if !features.is_empty() => {
                    bail!("row {label}: a {} row names one of {features:?}", row.kind)
                }
                Some(f) if !features.contains(&f) => {
                    bail!("row {label}: a {} row's feature is one of {features:?}", row.kind)
                }
                _ => {}
            }
            ensure!(!seen.contains(&label), "row {label} is listed twice");
            seen.push(label.clone());

            let field = is_field_row(&row.kind, feature);
            for (sink, cells) in &row.cells {
                ensure!(self.sinks.contains(sink), "row {label}: {sink} isn't in sinks:");
                let cells = cells.as_slice();
                ensure!(!cells.is_empty(), "row {label}, {sink}: an empty list of cells");
                for (i, cell) in cells.iter().enumerate() {
                    let at = format!("row {label}, {sink}, cell {i}");
                    let last = i + 1 == cells.len();
                    ensure!(last == cell.when.is_empty(), "{at}: only the last cell has no when:");
                    if field {
                        ensure!(
                            cell.class.is_field_level(),
                            "{at}: a field row can't be {:?}",
                            cell.class
                        );
                    } else {
                        ensure!(cell.class.is_record_level(), "{at}: a record row can't be lost");
                    }
                    ensure!(!cell.text.trim().is_empty(), "{at}: no text");
                    if let Some(counter) = &cell.counter {
                        Series::parse(counter).with_context(|| format!("{at}: counter"))?;
                    }
                    let reason = cell.reason.as_deref();
                    if reason == Some("uncounted") {
                        ensure!(
                            uncounted_allowed(&row.kind, feature, sink),
                            "{at}: an uncounted loss is counted, or a lost cell is structural"
                        );
                    }
                    if reason == Some("structural") {
                        ensure!(
                            matches!(cell.class, Class::Lost | Class::Degraded)
                                && cell.counter.is_none(),
                            "{at}: only a lost or degraded cell with no counter is structural"
                        );
                    }
                    match cell.class {
                        Class::Normalized => {
                            ensure!(
                                cell.reason.is_some(),
                                "{at}: a normalized cell names its reason"
                            )
                        }
                        Class::Skipped | Class::Dropped => ensure!(
                            cell.counter.is_some() || reason == Some("uncounted"),
                            "{at}: a {} cell names its counter",
                            cell.class.as_str()
                        ),
                        Class::Lost => ensure!(
                            cell.counter.is_some()
                                || matches!(reason, Some("structural" | "uncounted")),
                            "{at}: a lost cell names its counter, or its reason is structural"
                        ),
                        Class::Degraded => ensure!(
                            cell.counter.is_some() || reason == Some("structural"),
                            "{at}: a degraded cell names its counter, or its reason is structural"
                        ),
                        Class::Exact => {}
                    }
                }
            }
        }
        Ok(())
    }
}

/// Reads and validates `path`.
pub(super) fn load(path: &Path) -> anyhow::Result<Mapping> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    parse(&text).with_context(|| format!("in {}", path.display()))
}

/// Parses and validates a table's text.
pub(super) fn parse(text: &str) -> anyhow::Result<Mapping> {
    let mapping: Mapping = serde_norway::from_str(text)?;
    mapping.validate()?;
    Ok(mapping)
}
