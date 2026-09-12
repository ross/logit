//! collectd's `types.db`: the data-set definitions that give a value list's data sources their
//! *names*.
//!
//! The wire carries no data-source names at all -- a Values part is a count, a type byte per data
//! source and a value per data source, and nothing more. collectd's own receiver resolves the list's
//! `type` against its `types.db` to learn both how many data sources the type has and what each one
//! is called (`load` is `shortterm`, `midterm`, `longterm`); `write_graphite` then renders a
//! single-data-source list as `<plugin>.<type>` and a multi-data-source one as
//! `<plugin>.<type>.<ds_name>`. This module is the parser for that file, and
//! [`super::CollectdDecoder::with_types_db`] is where the names it holds reach a record name.
//!
//! **collectd's own `types.db` is GPL and is never copied into this repository.** An operator points
//! `collectd_in`'s `types_db:` at their own installed copy (conventionally
//! `/usr/share/collectd/types.db`); every test here uses the short, hand-written fixture at the
//! bottom of this file, in the same format.
//!
//! **Names are display only.** `collectd_in -> collectd_out` fidelity rides on the `collectd.*`
//! attributes, the [`logit_core::MetricList`] order and the per-record kinds -- never on the record
//! name -- so a pipeline with no `types_db:` configured, one with a stale file, and one with the
//! matching file all relay the same bytes. What changes is what the name looks like at a
//! *cross-protocol* sink (InfluxDB, Prometheus, statsd), which is the whole reason to configure it.
//!
//! ## File format
//!
//! ```text
//! # a comment
//! load        shortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000
//! if_octets   rx:DERIVE:0:U, tx:DERIVE:0:U
//! ```
//!
//! One type per line: the type name, then one or more data-source definitions, each exactly
//! `<name>:<KIND>:<min>:<max>`. **Fields are separated by whitespace, and a trailing `,` on a field
//! is decoration** -- collectd's own `types_list.c` splits the whole line with `strsplit` and its
//! `parse_ds` strips one trailing comma off each field, so `rx:DERIVE:0:U, tx:DERIVE:0:U`,
//! `rx:DERIVE:0:U tx:DERIVE:0:U` and a line ending in a stray `,` are all the same file to
//! collectd, and all the same file here. `<KIND>` is `COUNTER`, `GAUGE`, `DERIVE` or `ABSOLUTE`
//! and `<min>`/`<max>` are numbers or `U` for unbounded, all four matched case-insensitively
//! (`parse_ds` uses `strcasecmp` throughout). The bounds are parsed for validation and then
//! discarded, since nothing in this codec range-checks a value (collectd itself only uses them
//! inside its RRD writer).
//!
//! Blank lines and lines whose first non-whitespace character is `#` are skipped. An inline `#` is
//! **not** a comment, exactly as in collectd's own parser (`types_list.c`), so a `#` inside a
//! definition is a parse error rather than a silently truncated line.
//!
//! A later definition of a type replaces an earlier one -- within one file, and across
//! [`TypesDb::load`]'s file list in order, which is how an operator overrides a stock definition
//! with a local one by listing their own file second.

use std::collections::HashMap;
use std::path::PathBuf;

/// A data source's type, the `DS_TYPE_*` vocabulary shared by `types.db` and the wire's own
/// per-value type byte ([`super::part::DS_COUNTER`] and friends). Kept as its own enum rather than
/// reusing [`super::part::DsValue`] because a `types.db` entry names a *kind* with no value attached
/// to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DsKind {
    Counter,
    Gauge,
    Derive,
    Absolute,
}

impl DsKind {
    /// The wire data-source type byte this kind corresponds to -- what
    /// [`super::CollectdDecoder`] compares a declared type against before trusting a `types.db`
    /// entry's names.
    pub fn ds_type_byte(self) -> u8 {
        match self {
            DsKind::Counter => super::part::DS_COUNTER,
            DsKind::Gauge => super::part::DS_GAUGE,
            DsKind::Derive => super::part::DS_DERIVE,
            DsKind::Absolute => super::part::DS_ABSOLUTE,
        }
    }

    /// The inverse: the kind a wire type byte names, or `None` for a byte this protocol does not
    /// define.
    pub fn from_ds_type_byte(byte: u8) -> Option<DsKind> {
        Some(match byte {
            super::part::DS_COUNTER => DsKind::Counter,
            super::part::DS_GAUGE => DsKind::Gauge,
            super::part::DS_DERIVE => DsKind::Derive,
            super::part::DS_ABSOLUTE => DsKind::Absolute,
            _ => return None,
        })
    }

    /// Parses the `<KIND>` field of a data-source definition, case-insensitively (collectd's own
    /// `parse_ds` uses `strcasecmp`).
    fn parse(text: &str) -> Option<DsKind> {
        Some(if text.eq_ignore_ascii_case("counter") {
            DsKind::Counter
        } else if text.eq_ignore_ascii_case("gauge") {
            DsKind::Gauge
        } else if text.eq_ignore_ascii_case("derive") {
            DsKind::Derive
        } else if text.eq_ignore_ascii_case("absolute") {
            DsKind::Absolute
        } else {
            return None;
        })
    }
}

/// One data source of one type: its name and its kind. The `min`/`max` bounds a `types.db` line
/// also carries are validated at parse time and then dropped -- see this module's doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSource {
    pub name: String,
    pub kind: DsKind,
}

/// A parsed `types.db`: type name → its data sources, in file order.
#[derive(Debug, Clone, Default)]
pub struct TypesDb {
    types: HashMap<String, Vec<DataSource>>,
}

impl TypesDb {
    /// Parses one `types.db`'s worth of text. See this module's doc for the format; every error
    /// names the line it was found on, because that is the only thing that makes a 200-line file
    /// with one bad entry debuggable.
    pub fn parse(text: &str) -> Result<TypesDb, TypesDbError> {
        let mut types: HashMap<String, Vec<DataSource>> = HashMap::new();
        for (index, raw) in text.lines().enumerate() {
            let line = index + 1;
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let Some((type_name, rest)) = split_once_whitespace(trimmed) else {
                return Err(TypesDbError::NoDataSources { line, type_name: trimmed.to_string() });
            };
            // Whitespace-separated fields, each allowed one decorative trailing comma -- collectd's
            // own `strsplit` + `parse_ds` pair, not a comma-separated list (see this module's
            // "File format"). A field that is *only* a comma leaves nothing behind and is skipped,
            // the way an empty `strsplit` field would be.
            let mut sources = Vec::new();
            for field in rest.split_whitespace() {
                let spec = field.strip_suffix(',').unwrap_or(field);
                if spec.is_empty() {
                    continue;
                }
                sources.push(parse_data_source(line, spec)?);
            }
            if sources.is_empty() {
                return Err(TypesDbError::NoDataSources { line, type_name: type_name.to_string() });
            }
            // A repeated type replaces the earlier one rather than appending to it -- collectd's
            // own behaviour, and what makes "list your override file second" work.
            types.insert(type_name.to_string(), sources);
        }
        Ok(TypesDb { types })
    }

    /// Folds `other` over `self`: every type `other` defines replaces `self`'s definition of the
    /// same name, and types only `self` has are untouched.
    pub fn merge(&mut self, other: TypesDb) {
        self.types.extend(other.types);
    }

    /// Reads, parses and merges every path in order -- later files override earlier ones.
    ///
    /// A path that cannot be read, or whose contents do not parse, is an **error** rather than a
    /// warning: an operator who named a `types.db` wants its names, and silently starting with no
    /// names at all would look exactly like a correctly-running pipeline whose records happen to be
    /// index-named. `logit-cli`'s `build_spec` surfaces this as a startup config error, so the
    /// process never reaches ready.
    pub fn load(paths: &[PathBuf]) -> anyhow::Result<TypesDb> {
        use anyhow::Context;

        let mut merged = TypesDb::default();
        for path in paths {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading types.db '{}'", path.display()))?;
            let parsed = TypesDb::parse(&text)
                .with_context(|| format!("parsing types.db '{}'", path.display()))?;
            merged.merge(parsed);
        }
        Ok(merged)
    }

    /// The data sources of `type_name`, or `None` when this file never defined it (routine: no
    /// `types.db` lists every type every plugin in the world emits).
    pub fn get(&self, type_name: &str) -> Option<&[DataSource]> {
        self.types.get(type_name).map(Vec::as_slice)
    }

    /// How many types are defined -- for tests and for a startup log line.
    pub fn len(&self) -> usize {
        self.types.len()
    }

    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }
}

/// Splits a line into its type name and the rest, on the first run of whitespace (a real `types.db`
/// is column-aligned with a mix of tabs and spaces, so this cannot be a single-character split).
fn split_once_whitespace(line: &str) -> Option<(&str, &str)> {
    let end = line.find(char::is_whitespace)?;
    let rest = line[end..].trim_start();
    if rest.is_empty() {
        return None;
    }
    Some((&line[..end], rest))
}

/// Parses one `<name>:<KIND>:<min>:<max>` definition.
fn parse_data_source(line: usize, spec: &str) -> Result<DataSource, TypesDbError> {
    let fields: Vec<&str> = spec.split(':').collect();
    if fields.len() != 4 {
        return Err(TypesDbError::BadDataSource {
            line,
            spec: spec.to_string(),
            fields: fields.len(),
        });
    }
    let name = fields[0];
    if name.is_empty() {
        return Err(TypesDbError::UnnamedDataSource { line, spec: spec.to_string() });
    }
    let Some(kind) = DsKind::parse(fields[1]) else {
        return Err(TypesDbError::UnknownKind { line, kind: fields[1].to_string() });
    };
    // Parsed for validation only: a bound that is neither a number nor `U` means the line is not
    // the format it claims to be, and accepting it would mean accepting an arbitrary line as a
    // type definition.
    for (bound, value) in [("min", fields[2]), ("max", fields[3])] {
        if !value.eq_ignore_ascii_case("U") && value.parse::<f64>().is_err() {
            return Err(TypesDbError::BadBound {
                line,
                name: name.to_string(),
                bound,
                value: value.to_string(),
            });
        }
    }
    Ok(DataSource { name: name.to_string(), kind })
}

/// Why a `types.db` did not parse. Every variant names the line number, which is what
/// [`TypesDb::load`] adds the file path to.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypesDbError {
    #[error("line {line}: type '{type_name}' has no data-source definitions")]
    NoDataSources { line: usize, type_name: String },
    #[error(
        "line {line}: data source '{spec}' has {fields} colon-separated field(s), expected 4 \
         (<name>:<KIND>:<min>:<max>)"
    )]
    BadDataSource { line: usize, spec: String, fields: usize },
    #[error("line {line}: data source '{spec}' has no name")]
    UnnamedDataSource { line: usize, spec: String },
    #[error("line {line}: unknown data-source kind '{kind}', expected COUNTER, GAUGE, DERIVE or ABSOLUTE")]
    UnknownKind { line: usize, kind: String },
    #[error(
        "line {line}: data source '{name}' has a {bound} of '{value}', expected a number or 'U'"
    )]
    BadBound { line: usize, name: String, bound: &'static str, value: String },
}

impl TypesDbError {
    /// The line the problem was found on -- 1-based, as an editor counts.
    pub fn line(&self) -> usize {
        match self {
            TypesDbError::NoDataSources { line, .. }
            | TypesDbError::BadDataSource { line, .. }
            | TypesDbError::UnnamedDataSource { line, .. }
            | TypesDbError::UnknownKind { line, .. }
            | TypesDbError::BadBound { line, .. } => *line,
        }
    }
}

/// A short, **hand-written** stand-in for collectd's own `types.db`, covering the type shapes this
/// codec's tests care about: multi-data-source gauges (`load`), a two-data-source DERIVE pair
/// (`if_octets`), single-data-source types of each kind (`cpu`, `memory`, `uptime`, and the four
/// stock fallback types the encoder emits), and the column alignment a real file has (tabs and
/// spaces both).
///
/// collectd's own file is GPL-licensed and is deliberately **not** vendored here (AGENTS.md's
/// fixture rule); the definitions below were written from the collectd wiki's documented data
/// sources rather than copied.
pub const TEST_TYPES_DB: &str = "\
# a hand-written fixture, not collectd's own types.db

load\t\tshortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000
if_octets\trx:DERIVE:0:U, tx:DERIVE:0:U
if_errors\trx:DERIVE:0:U, tx:DERIVE:0:U
cpu\t\tvalue:DERIVE:0:U
memory\t\tvalue:GAUGE:0:281474976710656
uptime\t\tvalue:GAUGE:0:4294967295
df_complex\tvalue:GAUGE:0:U
ps_state\tvalue:GAUGE:0:65535
counter\t\tvalue:COUNTER:U:U
derive\t\tvalue:DERIVE:U:U
absolute\tvalue:ABSOLUTE:0:U
gauge\t\tvalue:GAUGE:U:U
";

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> TypesDb {
        TypesDb::parse(TEST_TYPES_DB).expect("the fixture must parse")
    }

    #[test]
    fn the_fixture_parses_every_type_it_defines() {
        let db = fixture();
        assert_eq!(db.len(), 12);
        assert!(!db.is_empty());
    }

    #[test]
    fn a_multi_data_source_type_keeps_its_data_sources_in_file_order() {
        let db = fixture();
        let load = db.get("load").expect("load is defined");
        assert_eq!(
            load,
            &[
                DataSource { name: "shortterm".into(), kind: DsKind::Gauge },
                DataSource { name: "midterm".into(), kind: DsKind::Gauge },
                DataSource { name: "longterm".into(), kind: DsKind::Gauge },
            ]
        );
    }

    #[test]
    fn every_data_source_kind_parses() {
        let db = fixture();
        assert_eq!(db.get("counter").unwrap()[0].kind, DsKind::Counter);
        assert_eq!(db.get("gauge").unwrap()[0].kind, DsKind::Gauge);
        assert_eq!(db.get("derive").unwrap()[0].kind, DsKind::Derive);
        assert_eq!(db.get("absolute").unwrap()[0].kind, DsKind::Absolute);
        assert_eq!(db.get("if_octets").unwrap()[1].name, "tx");
        assert_eq!(db.get("if_octets").unwrap()[1].kind, DsKind::Derive);
    }

    #[test]
    fn an_undefined_type_resolves_to_nothing() {
        assert!(fixture().get("no_such_type").is_none());
    }

    /// Comments, blank lines, tabs and spaces, `U` bounds, and a kind in lower case -- the whole
    /// format's tolerance in one fixture.
    #[test]
    fn comments_blank_lines_and_mixed_whitespace_are_tolerated() {
        let db = TypesDb::parse(
            "# leading comment\n\
             \n\
             \t# an indented comment\n\
             spaced   value:gauge:U:U\n\
             tabbed\t\tvalue:GAUGE:U:U\n\
             \n",
        )
        .expect("must parse");
        assert_eq!(db.len(), 2);
        assert_eq!(db.get("spaced").unwrap()[0].kind, DsKind::Gauge);
        assert_eq!(db.get("tabbed").unwrap()[0].name, "value");
    }

    /// The comma in `rx:DERIVE:0:U, tx:DERIVE:0:U` is **decoration on a whitespace-separated
    /// field**, not the separator: collectd's `types_list.c` splits the line with `strsplit` and
    /// `parse_ds` strips one trailing `,`. So the stock file's `, ` style, a comma-free line, and a
    /// line ending in a stray comma all have to parse to the same two data sources -- a hand-written
    /// override file written in any of these styles must not refuse to start the process.
    ///
    /// Two definitions run together with **no** whitespace (`rx:DERIVE:0:U,tx:DERIVE:0:U`) are one
    /// field with too many colon fields, and stay an error: that is what collectd's own parser does
    /// with them too (see `every_malformed_line_shape_is_an_error_naming_its_line`).
    #[test]
    fn data_sources_are_whitespace_separated_with_an_optional_trailing_comma() {
        for text in [
            "if_octets\trx:DERIVE:0:U, tx:DERIVE:0:U\n",
            "if_octets\trx:DERIVE:0:U tx:DERIVE:0:U\n",
            "if_octets\trx:DERIVE:0:U, tx:DERIVE:0:U,\n",
            "if_octets\trx:DERIVE:0:U ,  tx:DERIVE:0:U ,\n",
        ] {
            let db = TypesDb::parse(text).unwrap_or_else(|e| panic!("{text:?} must parse: {e}"));
            let sources = db.get("if_octets").expect("if_octets is defined");
            assert_eq!(
                sources,
                &[
                    DataSource { name: "rx".into(), kind: DsKind::Derive },
                    DataSource { name: "tx".into(), kind: DsKind::Derive },
                ],
                "{text:?}"
            );
        }
    }

    /// collectd's `parse_ds` compares the bounds with `strcasecmp` too, not just the kind -- so a
    /// lower-case `u` is unbounded, and must not be a startup failure.
    #[test]
    fn an_unbounded_bound_is_matched_case_insensitively() {
        for text in ["t value:GAUGE:U:U\n", "t value:GAUGE:u:u\n", "t value:GAUGE:u:U\n"] {
            let db = TypesDb::parse(text).unwrap_or_else(|e| panic!("{text:?} must parse: {e}"));
            assert_eq!(db.get("t").unwrap()[0].name, "value");
        }
    }

    /// Both halves of the override rule: within one text, and across a [`TypesDb::merge`].
    #[test]
    fn a_later_definition_of_a_type_replaces_an_earlier_one() {
        let db = TypesDb::parse("t value:GAUGE:U:U\nt a:DERIVE:U:U, b:DERIVE:U:U\n").unwrap();
        assert_eq!(db.get("t").unwrap().len(), 2);

        let mut base = TypesDb::parse("t value:GAUGE:U:U\nkept value:GAUGE:U:U\n").unwrap();
        base.merge(TypesDb::parse("t other:COUNTER:U:U\n").unwrap());
        assert_eq!(base.get("t").unwrap()[0].name, "other");
        assert_eq!(base.get("kept").unwrap()[0].kind, DsKind::Gauge, "untouched types survive");
    }

    #[test]
    fn every_malformed_line_shape_is_an_error_naming_its_line() {
        let cases: &[(&str, &str)] = &[
            ("a type with no data sources", "load\n"),
            ("a type with only trailing whitespace", "load   \n"),
            ("a data source with too few fields", "load shortterm:GAUGE:0\n"),
            ("a data source with too many fields", "load shortterm:GAUGE:0:5000:extra\n"),
            ("an unknown kind", "load shortterm:FLOAT:0:5000\n"),
            ("a nameless data source", "load :GAUGE:0:5000\n"),
            ("a non-numeric bound", "load shortterm:GAUGE:zero:5000\n"),
            // Only *one* trailing comma is decoration, so this is a single field with six colon
            // fields, not two data sources -- collectd's own `parse_ds` rejects it too.
            ("two definitions run together by a doubled comma", "load a:GAUGE:U:U,,b:GAUGE:U:U\n"),
            ("an inline '#', which collectd does not treat as a comment", "load a:GAUGE:U:U # x\n"),
        ];
        for (label, text) in cases {
            // Prefixed with a comment and a blank line so the reported line number is 3, not 1 --
            // a parser that hard-coded `1` would pass a weaker version of this test.
            let parsed = TypesDb::parse(&format!("# header\n\n{text}"));
            let err = parsed.expect_err(label);
            assert_eq!(err.line(), 3, "{label} must report the line it was found on");
        }
    }

    #[test]
    fn a_malformed_line_reports_its_own_line_number() {
        for (text, expected_line) in
            [("bad\n", 3usize), ("ok value:GAUGE:U:U\nbad\n", 4), ("\n\n\nbad\n", 6)]
        {
            let err = TypesDb::parse(&format!("# header\n\n{text}")).expect_err("must not parse");
            assert_eq!(err.line(), expected_line, "{text:?}");
        }
    }

    #[test]
    fn load_reads_and_merges_files_in_order() {
        let dir = std::env::temp_dir().join(format!("logit-types-db-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let first = dir.join("first.db");
        let second = dir.join("second.db");
        std::fs::write(&first, "t value:GAUGE:U:U\nonly_first value:GAUGE:U:U\n").unwrap();
        std::fs::write(&second, "t rx:DERIVE:U:U, tx:DERIVE:U:U\n").unwrap();

        let db = TypesDb::load(&[first.clone(), second.clone()]).expect("both files must load");
        assert_eq!(db.get("t").unwrap().len(), 2, "the second file wins");
        assert_eq!(db.get("only_first").unwrap().len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_missing_file_is_an_error_naming_the_path() {
        let err = TypesDb::load(&[PathBuf::from("/nonexistent/types.db")]).unwrap_err();
        assert!(format!("{err:#}").contains("/nonexistent/types.db"), "{err:#}");
    }

    #[test]
    fn an_unparseable_file_is_an_error_naming_the_path_and_the_line() {
        let dir = std::env::temp_dir().join(format!("logit-types-db-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("broken.db");
        std::fs::write(&path, "ok value:GAUGE:U:U\nbroken\n").unwrap();

        let err = TypesDb::load(std::slice::from_ref(&path)).unwrap_err();
        let rendered = format!("{err:#}");
        assert!(rendered.contains("broken.db"), "{rendered}");
        assert!(rendered.contains("line 2"), "{rendered}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_kind_maps_to_and_from_its_wire_type_byte() {
        for kind in [DsKind::Counter, DsKind::Gauge, DsKind::Derive, DsKind::Absolute] {
            assert_eq!(DsKind::from_ds_type_byte(kind.ds_type_byte()), Some(kind));
        }
        assert_eq!(DsKind::from_ds_type_byte(99), None);
    }
}
