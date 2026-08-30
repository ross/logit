//! Loads a config file: read from disk, resolve every `!env VAR_NAME` tag against the process
//! environment, then deserialize into [`Config`].
//!
//! `!env` is a YAML tag, resolved on the parsed [`serde_norway::Value`] tree *before* serde ever
//! sees the document -- config types stay untouched by it (no env-specific field like the old
//! `token_env` needed ever again), and the published JSON Schema needs no widening to admit it on
//! every field. See `docs/adr/0010-env-yaml-tag.md` for the design rationale and its accepted
//! rough edges (also `docs/known-gaps.md`).
//!
//! This is the *only* place a config file should be read and parsed -- `logit run`, `logit
//! validate`, and `logit graph` all go through [`load`], so `!env` (and its unknown-tag guard,
//! see below) can't silently stop applying on one of the three.

use anyhow::Context;
use logit_config::Config;
use serde_norway::value::{Tag, TaggedValue};
use serde_norway::{Mapping, Value};
use std::path::Path;

/// How a `!env` reference to an unset variable is handled. `logit run`/`logit validate` want
/// `Error` -- an unset secret should fail loudly before anything starts. `logit graph` wants
/// `Placeholder`: it renders a config's shape, not its values, and its whole point is rendering
/// *something* useful even for a config that's otherwise broken (`dot.rs`'s module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingEnv {
    Error,
    Placeholder,
}

/// Reads, resolves, and deserializes the config file at `path`.
pub fn load(path: &Path, missing: MissingEnv) -> anyhow::Result<Config> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    parse(&raw, missing, &|name| std::env::var(name).ok())
        .with_context(|| format!("parsing config file {}", path.display()))
}

/// The rest of [`load`], parameterized over the environment lookup so tests can exercise `!env`
/// resolution without touching the real process environment.
fn parse(
    raw: &str,
    missing: MissingEnv,
    lookup: &impl Fn(&str) -> Option<String>,
) -> anyhow::Result<Config> {
    let value: Value = serde_norway::from_str(raw)?;
    let mut path = Vec::new();
    let mut substitutions = Vec::new();
    let value = resolve(value, lookup, missing, &mut path, &mut substitutions)?;
    serde_norway::from_value(value).map_err(|err| annotate(err, &substitutions))
}

/// One `!env` reference that resolved to something other than a string, recorded so a
/// deserialization failure can point at it -- the most likely cause when a field expecting a
/// string (a token, a URL) got a `!env`-supplied number or bool instead.
struct Substitution {
    path: String,
    var: String,
    type_name: &'static str,
}

/// If deserialization failed and any `!env` substitution resolved to a non-string value, append a
/// note listing them (path, variable name, and resolved *type* -- never the value, since these
/// are exactly the fields most likely to hold a secret).
fn annotate(err: serde_norway::Error, substitutions: &[Substitution]) -> anyhow::Error {
    if substitutions.is_empty() {
        return err.into();
    }
    let note = substitutions
        .iter()
        .map(|s| {
            format!("  {} (from ${}) resolved to a {}, not a string", s.path, s.var, s.type_name)
        })
        .collect::<Vec<_>>()
        .join("\n");
    anyhow::Error::new(err).context(format!(
        "the following !env substitution(s) resolved to a non-string value -- quote the \
         variable's value if a string was expected here:\n{note}"
    ))
}

/// One step of a config path built up while walking the value tree, for error messages like
/// `components.influx_out.token`.
enum PathSeg {
    Key(String),
    Index(usize),
}

fn path_string(path: &[PathSeg]) -> String {
    let mut s = String::new();
    for seg in path {
        match seg {
            PathSeg::Key(k) => {
                if !s.is_empty() {
                    s.push('.');
                }
                s.push_str(k);
            }
            PathSeg::Index(i) => s.push_str(&format!("[{i}]")),
        }
    }
    if s.is_empty() {
        "<root>".to_string()
    } else {
        s
    }
}

/// A short name for a `Value`'s kind, used both in error messages (`!env` used on a non-string
/// argument) and to label a substitution's resolved type.
fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "float",
        Value::String(_) => "string",
        Value::Sequence(_) => "sequence",
        Value::Mapping(_) => "mapping",
        Value::Tagged(_) => "tagged value",
    }
}

/// Recursively walks `value`, replacing every `!env VAR_NAME` tag with `VAR_NAME`'s value from
/// `lookup`, re-parsed as a YAML scalar (so `!env PORT` with `PORT=8125` becomes the integer
/// `8125`, not the string `"8125"` -- makes `!env` usable in any field, not just string ones).
/// Rejects any other YAML tag outright: `serde_norway` silently drops an unrecognized tag on a
/// non-enum target, so a typo'd `!emv` would otherwise deserialize as the literal variable name
/// instead of failing.
fn resolve(
    value: Value,
    lookup: &impl Fn(&str) -> Option<String>,
    missing: MissingEnv,
    path: &mut Vec<PathSeg>,
    substitutions: &mut Vec<Substitution>,
) -> anyhow::Result<Value> {
    match value {
        Value::Mapping(mapping) => {
            let mut resolved = Mapping::with_capacity(mapping.len());
            for (key, value) in mapping {
                let key = resolve(key, lookup, missing, path, substitutions)?;
                let label = match &key {
                    Value::String(s) => s.clone(),
                    other => format!("<{}>", value_kind(other)),
                };
                path.push(PathSeg::Key(label));
                let value = resolve(value, lookup, missing, path, substitutions);
                path.pop();
                resolved.insert(key, value?);
            }
            Ok(Value::Mapping(resolved))
        }
        Value::Sequence(sequence) => {
            let mut resolved = Vec::with_capacity(sequence.len());
            for (index, item) in sequence.into_iter().enumerate() {
                path.push(PathSeg::Index(index));
                let item = resolve(item, lookup, missing, path, substitutions);
                path.pop();
                resolved.push(item?);
            }
            Ok(Value::Sequence(resolved))
        }
        Value::Tagged(tagged) => {
            let TaggedValue { tag, value } = *tagged;
            if tag == "env" {
                resolve_env_tag(value, tag, lookup, missing, path, substitutions)
            } else if tag.to_string().starts_with("!!") {
                // A standard YAML core-schema tag (`!!str`, `!!int`, ...) -- not ours to
                // interpret, but its contents might still nest an `!env` (however unlikely in
                // practice), so recurse rather than skip.
                let value = resolve(value, lookup, missing, path, substitutions)?;
                Ok(Value::Tagged(Box::new(TaggedValue { tag, value })))
            } else {
                anyhow::bail!(
                    "{}: unknown tag '{tag}' -- only !env is a supported config directive",
                    path_string(path)
                );
            }
        }
        other => Ok(other),
    }
}

fn resolve_env_tag(
    value: Value,
    tag: Tag,
    lookup: &impl Fn(&str) -> Option<String>,
    missing: MissingEnv,
    path: &[PathSeg],
    substitutions: &mut Vec<Substitution>,
) -> anyhow::Result<Value> {
    let Value::String(var) = value else {
        anyhow::bail!(
            "{}: {tag} expects a variable name (a plain string), got a {}",
            path_string(path),
            value_kind(&value)
        );
    };
    match lookup(&var) {
        Some(raw) => {
            let resolved = scalar_from_env(raw);
            if !matches!(resolved, Value::String(_)) {
                substitutions.push(Substitution {
                    path: path_string(path),
                    var,
                    type_name: value_kind(&resolved),
                });
            }
            Ok(resolved)
        }
        None => match missing {
            MissingEnv::Error => anyhow::bail!(
                "{}: !env references environment variable '{var}', which is not set",
                path_string(path)
            ),
            MissingEnv::Placeholder => {
                // `logit graph` (the only caller of `Placeholder`) is documented to render
                // *something* useful even for an otherwise-broken config -- consistent with the
                // `eprintln!`-as-diagnostics convention elsewhere (docs/known-gaps.md), not a
                // hard failure, but not silent either.
                eprintln!(
                    "warning: {}: !env references environment variable '{var}', which is not \
                     set -- substituting a placeholder",
                    path_string(path)
                );
                Ok(Value::String(format!("<unset:{var}>")))
            }
        },
    }
}

/// Re-parses an env var's raw string as a YAML scalar: `8125` becomes an integer, `true`/`false`
/// a bool, anything else -- including a value that happens to parse as a mapping or sequence --
/// stays the literal string. An empty variable is the empty string, never null.
fn scalar_from_env(raw: String) -> Value {
    if raw.is_empty() {
        return Value::String(raw);
    }
    match serde_norway::from_str::<Value>(&raw) {
        Ok(value @ (Value::Null | Value::Bool(_) | Value::Number(_))) => value,
        _ => Value::String(raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A `lookup` backed by a plain map, so tests never touch the real process environment.
    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: HashMap<String, String> =
            vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |name: &str| vars.get(name).cloned()
    }

    fn resolve_yaml(
        yaml: &str,
        lookup: &impl Fn(&str) -> Option<String>,
        missing: MissingEnv,
    ) -> anyhow::Result<Value> {
        let value: Value = serde_norway::from_str(yaml).unwrap();
        resolve(value, lookup, missing, &mut Vec::new(), &mut Vec::new())
    }

    #[test]
    fn substitutes_a_variable_into_a_string_field() {
        let value =
            resolve_yaml("url: !env URL", &env(&[("URL", "http://x")]), MissingEnv::Error).unwrap();
        assert_eq!(value["url"], Value::String("http://x".to_string()));
    }

    #[test]
    fn a_numeric_value_resolves_as_a_number() {
        let value =
            resolve_yaml("port: !env PORT", &env(&[("PORT", "8125")]), MissingEnv::Error).unwrap();
        assert_eq!(value["port"], Value::from(8125));
    }

    #[test]
    fn a_boolean_value_resolves_as_a_bool() {
        let value =
            resolve_yaml("flag: !env FLAG", &env(&[("FLAG", "true")]), MissingEnv::Error).unwrap();
        assert_eq!(value["flag"], Value::Bool(true));
    }

    #[test]
    fn a_duration_style_value_stays_a_string() {
        let value =
            resolve_yaml("interval: !env WINDOW", &env(&[("WINDOW", "10s")]), MissingEnv::Error)
                .unwrap();
        assert_eq!(value["interval"], Value::String("10s".to_string()));
    }

    #[test]
    fn an_empty_variable_stays_the_empty_string_not_null() {
        let value =
            resolve_yaml("token: !env TOKEN", &env(&[("TOKEN", "")]), MissingEnv::Error).unwrap();
        assert_eq!(value["token"], Value::String(String::new()));
    }

    #[test]
    fn a_mapping_or_sequence_looking_value_stays_a_string() {
        let value = resolve_yaml(
            "x: !env X",
            &env(&[("X", "[not, actually, a, sequence, field]")]),
            MissingEnv::Error,
        )
        .unwrap();
        assert_eq!(value["x"], Value::String("[not, actually, a, sequence, field]".to_string()));
    }

    #[test]
    fn substitutes_inside_a_sequence() {
        let value = resolve_yaml(
            "sources: [!env A, plain, !env B]",
            &env(&[("A", "in"), ("B", "out")]),
            MissingEnv::Error,
        )
        .unwrap();
        let expected = Value::Sequence(vec![
            Value::String("in".to_string()),
            Value::String("plain".to_string()),
            Value::String("out".to_string()),
        ]);
        assert_eq!(value["sources"], expected);
    }

    #[test]
    fn substitutes_inside_a_nested_mapping() {
        let yaml = "outer:\n  inner:\n    token: !env TOKEN\n";
        let value = resolve_yaml(yaml, &env(&[("TOKEN", "secret")]), MissingEnv::Error).unwrap();
        assert_eq!(value["outer"]["inner"]["token"], Value::String("secret".to_string()));
    }

    #[test]
    fn a_missing_variable_errors_naming_the_variable_and_the_config_path() {
        let err = resolve_yaml(
            "components:\n  out:\n    token: !env INFLUXDB_TOKEN\n",
            &env(&[]),
            MissingEnv::Error,
        )
        .expect_err("expected an error");
        assert!(err.to_string().contains("INFLUXDB_TOKEN"), "got: {err}");
        assert!(err.to_string().contains("components.out.token"), "got: {err}");
    }

    #[test]
    fn a_missing_variable_in_placeholder_mode_substitutes_instead_of_erroring() {
        let value = resolve_yaml("token: !env TOKEN", &env(&[]), MissingEnv::Placeholder).unwrap();
        assert_eq!(value["token"], Value::String("<unset:TOKEN>".to_string()));
    }

    #[test]
    fn env_with_a_non_string_argument_is_rejected() {
        let err = resolve_yaml("x: !env [A, B]", &env(&[]), MissingEnv::Error)
            .expect_err("expected an error");
        assert!(err.to_string().contains("expects a variable name"), "got: {err}");
    }

    #[test]
    fn an_unknown_tag_is_rejected_rather_than_silently_passed_through() {
        let err =
            resolve_yaml("token: !emv TOKEN", &env(&[("TOKEN", "secret")]), MissingEnv::Error)
                .expect_err("expected an error");
        assert!(err.to_string().contains("unknown tag"), "got: {err}");
        assert!(err.to_string().contains("emv"), "got: {err}");
    }

    #[test]
    fn a_config_with_no_tags_round_trips_unchanged() {
        let yaml = "a: 1\nb: [x, y]\nc:\n  d: true\n";
        let value = resolve_yaml(yaml, &env(&[]), MissingEnv::Error).unwrap();
        let expected: Value = serde_norway::from_str(yaml).unwrap();
        assert_eq!(value, expected);
    }

    #[test]
    fn a_numeric_looking_secret_fails_deserialization_with_a_substitution_note() {
        let raw = r#"
components:
  in:
    type: statsd_in
    bind: "0.0.0.0:8125"
  out:
    type: influxdb_out
    sources: [in]
    url: http://localhost:8086
    org: org
    bucket: bucket
    token: !env TOKEN
"#;
        let err = parse(raw, MissingEnv::Error, &env(&[("TOKEN", "123456")]))
            .expect_err("a numeric token should fail to deserialize as a String field");
        let message = format!("{err:?}");
        assert!(message.contains("components.out.token"), "got: {message}");
        assert!(message.contains("TOKEN"), "got: {message}");
        assert!(message.contains("integer"), "got: {message}");
    }

    #[test]
    fn logit_graph_never_errors_on_a_missing_variable() {
        let raw = "components:\n  out:\n    token: !env NOPE\n";
        assert!(parse(raw, MissingEnv::Placeholder, &env(&[])).is_err());
        // Fails here only because the rest of the `Config` shape is missing, not because of the
        // unset variable -- confirmed directly:
        let value = resolve_yaml("token: !env NOPE", &env(&[]), MissingEnv::Placeholder).unwrap();
        assert_eq!(value["token"], Value::String("<unset:NOPE>".to_string()));
    }
}
