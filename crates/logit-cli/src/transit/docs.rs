//! The transit spec's generated docs: renders `spec/transit/mapping.yaml` into the marked regions
//! of each spec'd sink's module doc and of `docs/known-gaps/mappings.md`, and fails when a
//! committed region differs from its rendering. `spec/transit/README.md`'s "Generated docs" is the
//! reader's account.
//!
//! **Regions.** A region opens on a line ending `<!-- transit:doc:<sink> -->` or
//! `<!-- transit:gaps:<sink> -->` and closes on the next line ending `<!-- transit:end -->`.
//! Whatever precedes the opening marker on its line (`//! `, or a list item's indentation) starts
//! every line the region renders, trailing whitespace trimmed. The markers are the only part of a
//! region written by hand. A file in [`FILES`] with no region, a region for a sink `sinks:`
//! doesn't list, a sink with no region or two of one form, a nested or unterminated region, and a
//! stray end marker each fail.
//!
//! - **`doc:<sink>`** is the sink's whole column: a table of the record-level rows (every metric
//!   kind, the `record` rows that decide a record's fate, a log or span alone, or an event with no
//!   record), then one of
//!   the field-level rows ([`mapping::is_field_row`]). A row is its label, the cell's class, its
//!   reason and counter, and its `text` verbatim. Each cell of a `when:` list is its own row, the
//!   condition after the label ([`condition`]); the last cell of such a list reads "otherwise".
//! - **`gaps:<sink>`** is the sink's `degraded`, `lost`, `skipped`, and `dropped` cells in
//!   `mappings.md`'s row format: `encode (<Protocol>)`, the row and class (a field-level row
//!   prefixed `field`), the counter, and the `text`. `exact` and `normalized` cells are left out,
//!   and the last cell of a `when:` list whose earlier cell was left out names the negation of the
//!   earlier conditions ([`negation`]) in place of "otherwise".
//!
//! Rendering reads the table alone; the class check (`super::classes`) is what ties the table to
//! the code. Rows keep `mapping.yaml`'s order, which groups them by kind.
//!
//! Under `TRANSIT_UPDATE=1` the test rewrites each stale region in place; unset, it fails naming
//! the file and the region, with the lines that differ and [`REGENERATE`].

use super::mapping::{self, Cell, Class, Mapping, Row};
use super::spec::{diff, spec_dir, updating};
use anyhow::{bail, ensure, Context};
use std::collections::BTreeMap;
use std::path::Path;

/// The files holding regions, relative to the repository root.
const FILES: [&str; 10] = [
    "crates/logit-outputs/src/statsd.rs",
    "crates/logit-outputs/src/syslog.rs",
    "crates/logit-proto/src/prometheus/mod.rs",
    "crates/logit-proto/src/influxdb/mod.rs",
    "crates/logit-proto/src/otlp/mod.rs",
    "crates/logit-proto/src/datadog/mod.rs",
    "crates/logit-proto/src/graphite/mod.rs",
    "crates/logit-proto/src/splunk/mod.rs",
    "crates/logit-outputs/src/kafka/encode.rs",
    "docs/known-gaps/mappings.md",
];

/// The command that rewrites every region.
const REGENERATE: &str = "script/transit docs";

/// Each sink's protocol, as `mappings.md`'s `encode (<Protocol>)` labels name it.
const PROTOCOLS: [(&str, &str); 9] = [
    ("statsd_out", "statsd"),
    ("prometheus_out", "Prometheus"),
    ("influxdb_out", "InfluxDB"),
    ("otlp_out", "OTLP"),
    ("datadog_out", "Datadog"),
    ("graphite_out", "Graphite"),
    ("syslog_out", "syslog"),
    ("splunk_hec_out", "Splunk"),
    ("kafka_out", "Kafka"),
];

const OPEN: &str = "<!-- transit:";
const CLOSE: &str = " -->";
const END: &str = "end";

/// A region's form (module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Form {
    Doc,
    Gaps,
}

impl Form {
    fn as_str(self) -> &'static str {
        match self {
            Form::Doc => "doc",
            Form::Gaps => "gaps",
        }
    }
}

/// One region of a file: its form and sink, the prefix of its lines, and the indexes of its
/// opening and closing marker lines.
#[derive(Debug)]
struct Region {
    form: Form,
    sink: String,
    prefix: String,
    open: usize,
    close: usize,
}

impl Region {
    fn name(&self) -> String {
        format!("transit:{}:{}", self.form.as_str(), self.sink)
    }
}

/// A marker line's prefix and name, or `None` for a line holding no marker.
fn marker(line: &str) -> Option<(&str, &str)> {
    let at = line.find(OPEN)?;
    let name = line[at + OPEN.len()..].trim_end().strip_suffix(CLOSE)?;
    Some((&line[..at], name))
}

/// Every region of `text`.
fn regions(text: &str) -> anyhow::Result<Vec<Region>> {
    let mut out = Vec::new();
    let mut open: Option<Region> = None;
    for (i, line) in text.lines().enumerate() {
        let Some((prefix, name)) = marker(line) else { continue };
        let at = i + 1;
        if name == END {
            let Some(mut region) = open.take() else {
                bail!("line {at}: an end marker with no region open");
            };
            ensure!(
                prefix == region.prefix,
                "line {at}: the end of {} has another prefix than its opening line",
                region.name()
            );
            region.close = i;
            out.push(region);
            continue;
        }
        if let Some(region) = &open {
            bail!("line {at}: a region opens inside {}", region.name());
        }
        let (form, sink) = match name.split_once(':') {
            Some(("doc", sink)) => (Form::Doc, sink),
            Some(("gaps", sink)) => (Form::Gaps, sink),
            _ => bail!("line {at}: `transit:{name}` is neither `doc:<sink>` nor `gaps:<sink>`"),
        };
        open = Some(Region {
            form,
            sink: sink.to_string(),
            prefix: prefix.to_string(),
            open: i,
            close: i,
        });
    }
    if let Some(region) = open {
        bail!("{} opened on line {} has no end marker", region.name(), region.open + 1);
    }
    Ok(out)
}

/// A row's cells for `sink`, each with its condition: empty for a sink with one cell, the `when:`
/// text or "otherwise" for each cell of a list.
fn cells<'a>(row: &'a Row, sink: &str) -> Vec<(String, &'a Cell)> {
    let Some(cells) = row.cells.get(sink) else { return Vec::new() };
    let cells = cells.as_slice();
    cells
        .iter()
        .map(|cell| {
            let when = match cells.len() {
                1 => String::new(),
                _ if cell.when.is_empty() => "otherwise".to_string(),
                _ => condition(&cell.when),
            };
            (when, cell)
        })
        .collect()
}

/// A `when:` map as prose: `under format: statsd`, or `with bind: unset` for a null.
fn condition(when: &BTreeMap<String, serde_json::Value>) -> String {
    when.iter()
        .map(|(field, value)| match value {
            serde_json::Value::Null => format!("with `{field}:` unset"),
            serde_json::Value::String(s) => format!("under `{field}: {s}`"),
            other => format!("under `{field}: {other}`"),
        })
        .collect::<Vec<_>>()
        .join(" and ")
}

/// The negation of each `when:` map in `earlier`: `with bind: set` for a null, `not under
/// format: statsd` otherwise. What the last cell of a list covers, for a `gaps:` region that left
/// an earlier cell out.
fn negation(earlier: &[&Cell]) -> String {
    earlier
        .iter()
        .map(|cell| {
            cell.when
                .iter()
                .map(|(field, value)| match value {
                    serde_json::Value::Null => format!("with `{field}:` set"),
                    serde_json::Value::String(s) => format!("not under `{field}: {s}`"),
                    other => format!("not under `{field}: {other}`"),
                })
                .collect::<Vec<_>>()
                .join(" or ")
        })
        .collect::<Vec<_>>()
        .join(" and ")
}

/// Text for a table cell: one line, `|` escaped.
fn cell_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ").replace('|', "\\|")
}

/// A row's label with its condition, as the first column shows it.
fn label(row: &Row, when: &str) -> String {
    let label = format!("`{}`", mapping::row_label(&row.kind, row.feature.as_deref()));
    match when {
        "" => label,
        when => format!("{label}, {when}"),
    }
}

/// A cell's reason and counter for a module-doc row.
fn reason_and_counter(cell: &Cell) -> String {
    let mut parts = Vec::new();
    match cell.reason.as_deref() {
        Some(reason @ ("uncounted" | "structural")) => parts.push(reason.to_string()),
        Some(reason) => parts.push(format!("`{reason}`")),
        None => {}
    }
    if let Some(counter) = &cell.counter {
        parts.push(format!("`{counter}`"));
    }
    match parts.is_empty() {
        true => "--".to_string(),
        false => cell_text(&parts.join("; ")),
    }
}

/// A `doc:<sink>` region's lines.
fn render_doc(mapping: &Mapping, sink: &str) -> Vec<String> {
    let mut out = vec![String::new()];
    for (field, heading) in [
        (false, "Per metric kind, and per record feature that decides a record's fate:"),
        (true, "Per field of a record that reaches the wire, attribute shape, and payload beside metrics:"),
    ] {
        out.push(heading.to_string());
        out.push(String::new());
        out.push("| Row | Class | Reason or counter | What a consumer sees |".to_string());
        out.push("|---|---|---|---|".to_string());
        for row in &mapping.rows {
            if mapping::is_field_row(&row.kind, row.feature.as_deref()) != field {
                continue;
            }
            for (when, cell) in cells(row, sink) {
                out.push(format!(
                    "| {} | {} | {} | {} |",
                    cell_text(&label(row, &when)),
                    cell.class.as_str(),
                    reason_and_counter(cell),
                    cell_text(&cell.text),
                ));
            }
        }
        out.push(String::new());
    }
    out
}

/// A `gaps:<sink>` region's lines.
fn render_gaps(mapping: &Mapping, sink: &str, protocol: &str) -> Vec<String> {
    let mut out = vec![
        String::new(),
        "| Direction | Mapping | Counter | Why |".to_string(),
        "|---|---|---|---|".to_string(),
    ];
    for row in &mapping.rows {
        let field = mapping::is_field_row(&row.kind, row.feature.as_deref());
        let row_cells = cells(row, sink);
        let mut left_out = false;
        for (i, (when, cell)) in row_cells.iter().enumerate() {
            if matches!(cell.class, Class::Exact | Class::Normalized) {
                left_out = true;
                continue;
            }
            // A bare "otherwise" would name no condition once the cell before it is left out.
            let when = match (when.as_str(), left_out) {
                ("otherwise", true) => {
                    negation(&row_cells[..i].iter().map(|(_, c)| *c).collect::<Vec<_>>())
                }
                _ => when.clone(),
            };
            let kind = match field {
                true => format!("field {}", label(row, &when)),
                false => label(row, &when),
            };
            let counter = match (&cell.counter, cell.reason.as_deref()) {
                (Some(counter), _) => format!("`{counter}`"),
                (None, Some(reason @ ("uncounted" | "structural"))) => format!("none ({reason})"),
                (None, _) => "none".to_string(),
            };
            out.push(format!(
                "| encode ({protocol}) | {} → **{}** | {} | {} |",
                cell_text(&kind),
                cell.class.as_str(),
                cell_text(&counter),
                cell_text(&cell.text),
            ));
        }
    }
    out.push(String::new());
    out
}

/// What one file's regions hold against their renderings.
struct Synced {
    /// The file with every region rendered.
    text: String,
    /// Each region's form and sink.
    regions: Vec<(Form, String)>,
    /// One message per stale region.
    stale: Vec<String>,
}

/// Renders every region of `text`, the contents of `file`.
fn sync_text(mapping: &Mapping, file: &str, text: &str) -> anyhow::Result<Synced> {
    let lines: Vec<&str> = text.lines().collect();
    let mut out: Vec<String> = Vec::new();
    let mut names = Vec::new();
    let mut stale = Vec::new();
    let mut next = 0;
    for region in regions(text)? {
        ensure!(
            mapping.sinks.contains(&region.sink),
            "{file}: {} names a sink mapping.yaml's sinks: doesn't list",
            region.name()
        );
        let rendered = match region.form {
            Form::Doc => render_doc(mapping, &region.sink),
            Form::Gaps => {
                let Some((_, protocol)) = PROTOCOLS.iter().find(|(s, _)| *s == region.sink) else {
                    bail!("{file}: {} has no protocol name in docs.rs's PROTOCOLS", region.name());
                };
                render_gaps(mapping, &region.sink, protocol)
            }
        };
        let rendered: Vec<String> = rendered
            .iter()
            .map(|line| format!("{}{line}", region.prefix).trim_end().to_string())
            .collect();
        let committed = lines[region.open + 1..region.close].join("\n");
        let generated = rendered.join("\n");
        if committed != generated {
            stale.push(format!(
                "{file}: region {} is stale; run `{REGENERATE}` and review the diff:\n{}",
                region.name(),
                diff(&generated, &committed)
            ));
        }
        out.extend(lines[next..=region.open].iter().map(|l| l.to_string()));
        out.extend(rendered);
        next = region.close;
        names.push((region.form, region.sink));
    }
    out.extend(lines[next..].iter().map(|l| l.to_string()));
    let mut text_out = out.join("\n");
    if text.ends_with('\n') {
        text_out.push('\n');
    }
    Ok(Synced { text: text_out, regions: names, stale })
}

/// Checks, or under `update` rewrites, every region of every file in [`FILES`] under `root`.
fn sync(root: &Path, mapping: &Mapping, update: bool) -> Vec<String> {
    let mut failures = Vec::new();
    let mut seen: BTreeMap<(Form, String), Vec<&str>> = BTreeMap::new();
    for file in FILES {
        let path = root.join(file);
        let synced = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))
            .and_then(|text| Ok((sync_text(mapping, file, &text)?, text)));
        let (synced, text) = match synced {
            Ok(pair) => pair,
            Err(error) => {
                failures.push(format!("{error:#}"));
                continue;
            }
        };
        if synced.regions.is_empty() {
            failures.push(format!("{file}: holds no transit region"));
        }
        for name in &synced.regions {
            seen.entry(name.clone()).or_default().push(file);
        }
        if update {
            if synced.text != text {
                if let Err(error) = std::fs::write(&path, &synced.text) {
                    failures.push(format!("writing {}: {error}", path.display()));
                }
            }
        } else {
            failures.extend(synced.stale);
        }
    }
    for sink in &mapping.sinks {
        for form in [Form::Doc, Form::Gaps] {
            let name = format!("transit:{}:{sink}", form.as_str());
            match seen.get(&(form, sink.clone())).map(Vec::as_slice) {
                None | Some([]) => failures.push(format!(
                    "{name}: no file in docs.rs's FILES holds the region; every sink in sinks: \
                     has one of each form"
                )),
                Some([_]) => {}
                Some(files) => {
                    failures.push(format!("{name}: opened more than once, in {files:?}"))
                }
            }
        }
    }
    failures
}

/// The repository root.
fn root() -> std::path::PathBuf {
    spec_dir().join("../..")
}

/// The generated regions (module doc).
#[test]
fn transit_docs() {
    let failures = match mapping::load(&spec_dir().join("mapping.yaml")) {
        Ok(mapping) => sync(&root(), &mapping, updating()),
        Err(error) => vec![format!("{error:#}")],
    };
    assert!(failures.is_empty(), "{} failure(s):\n\n{}", failures.len(), failures.join("\n\n"));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> Mapping {
        mapping::load(&spec_dir().join("mapping.yaml")).expect("the table")
    }

    fn committed(file: &str) -> String {
        std::fs::read_to_string(root().join(file)).expect("the committed file")
    }

    #[test]
    fn a_hand_edited_region_fails_naming_the_file_and_region() {
        let mapping = table();
        let file = "docs/known-gaps/mappings.md";
        let text = committed(file);
        let row = "| encode (InfluxDB) | `set_members` → **skipped** |";
        assert!(text.contains(row), "the committed region holds {row:?}");
        let edited = text.replacen(row, "| encode (InfluxDB) | `set_members` → **dropped** |", 1);

        let synced = sync_text(&mapping, file, &edited).expect("the regions parse");
        assert_eq!(synced.stale.len(), 1, "{:?}", synced.stale);
        let failure = &synced.stale[0];
        assert!(failure.starts_with(file), "{failure}");
        assert!(failure.contains("transit:gaps:influxdb_out"), "{failure}");
        assert!(failure.contains(REGENERATE), "{failure}");
        assert_eq!(synced.text, text, "rendering restores the committed region");
    }

    #[test]
    fn a_hand_edited_module_doc_row_fails() {
        let mapping = table();
        let file = "crates/logit-outputs/src/statsd.rs";
        let text = committed(file);
        let edited = text.replacen(
            "| `sum/delta_monotonic` | exact |",
            "| `sum/delta_monotonic` | degraded |",
            1,
        );
        assert_ne!(edited, text, "the committed region holds the delta monotonic row");

        let synced = sync_text(&mapping, file, &edited).expect("the regions parse");
        assert_eq!(synced.stale.len(), 1, "{:?}", synced.stale);
        assert!(synced.stale[0].contains("transit:doc:statsd_out"), "{}", synced.stale[0]);
    }

    #[test]
    fn every_committed_region_is_current() {
        let mapping = table();
        for file in FILES {
            let text = committed(file);
            let synced = sync_text(&mapping, file, &text).expect("the regions parse");
            assert!(synced.stale.is_empty(), "{:?}", synced.stale);
            assert_eq!(synced.text, text, "{file}");
        }
    }

    #[test]
    fn a_region_renders_under_its_opening_lines_prefix() {
        let mapping = table();
        let text =
            "intro\n  <!-- transit:gaps:otlp_out -->\n  stale\n  <!-- transit:end -->\nafter\n";
        let synced = sync_text(&mapping, "x.md", text).expect("the region parses");
        assert_eq!(synced.stale.len(), 1);
        let lines: Vec<&str> = synced.text.lines().collect();
        assert_eq!(lines[0], "intro");
        assert_eq!(lines[2], "", "a blank rendered line keeps no trailing prefix");
        assert_eq!(lines[3], "  | Direction | Mapping | Counter | Why |");
        assert_eq!(lines[lines.len() - 2], "  <!-- transit:end -->");
        assert_eq!(lines[lines.len() - 1], "after");
        assert!(synced.text.contains("| encode (OTLP) | `set` → **skipped** |"));
        assert!(synced.text.ends_with('\n'));
    }

    #[test]
    fn a_when_list_renders_one_row_per_cell() {
        let lines = render_doc(&table(), "prometheus_out");
        let rows: Vec<&String> =
            lines.iter().filter(|l| l.starts_with("| `exponential_histogram/delta`")).collect();
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert!(
            rows[0].starts_with("| `exponential_histogram/delta`, with `bind:` unset | dropped |")
        );
        assert!(rows[1].starts_with("| `exponential_histogram/delta`, otherwise | skipped |"));
    }

    #[test]
    fn a_gaps_row_after_a_left_out_cell_names_the_negated_condition() {
        let lines = render_gaps(&table(), "prometheus_out", "Prometheus");
        let unit: Vec<&String> = lines.iter().filter(|l| l.contains("`record/unit`")).collect();
        assert_eq!(unit.len(), 1, "{unit:?}");
        assert!(
            unit[0].contains("field `record/unit`, with `bind:` set → **lost**"),
            "{}",
            unit[0]
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("`record/no_recorded_value`, with `bind:` set → **skipped**")),
            "{lines:#?}"
        );
        let statsd = render_gaps(&table(), "statsd_out", "statsd");
        assert!(statsd.iter().any(|l| l.contains("`distribution`, otherwise → **degraded**")));
    }

    #[test]
    fn malformed_markers_fail() {
        let mapping = table();
        for (text, want) in [
            ("<!-- transit:doc:statsd_out -->\n", "no end marker"),
            ("<!-- transit:end -->\n", "no region open"),
            ("<!-- transit:doc:statsd_out -->\n<!-- transit:gaps:statsd_out -->\n", "opens inside"),
            ("<!-- transit:table:statsd_out -->\n<!-- transit:end -->\n", "neither"),
            ("<!-- transit:doc:collectd_out -->\n<!-- transit:end -->\n", "doesn't list"),
            ("//! <!-- transit:doc:statsd_out -->\n<!-- transit:end -->\n", "another prefix"),
        ] {
            let error = sync_text(&mapping, "x.md", text).err().expect("a malformed region fails");
            assert!(format!("{error:#}").contains(want), "{text:?}: {error:#}");
        }
    }

    #[test]
    fn a_sink_without_its_regions_fails() {
        let mut mapping = table();
        mapping.sinks.push("collectd_out".to_string());
        let failures = sync(&root(), &mapping, false);
        assert!(
            failures.iter().any(|f| f.starts_with("transit:doc:collectd_out: no file")),
            "{failures:?}"
        );
        assert!(failures.iter().any(|f| f.starts_with("transit:gaps:collectd_out: no file")));
    }
}
