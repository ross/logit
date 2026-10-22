//! Loads a config file: read from disk, resolve every `!env VAR_NAME` tag against the process
//! environment, then deserialize into [`Config`].
//!
//! `!env` is resolved on the parsed [`serde_norway::Value`] tree before serde sees the document,
//! so config types need no env-specific fields and the JSON Schema needs no widening. See
//! `docs/adr/env-yaml-tag.md`, and `docs/known-gaps/runtime.md` for its rough edges.
//!
//! The only place a config file is read and parsed: `logit run`, `logit validate`, and
//! `logit graph` all go through [`load`], so `!env` and the unknown-tag guard apply to all three.
//! Every `!env` reference must resolve; there's no lenient mode for `logit graph` (the ADR's
//! "Alternatives considered").

use anyhow::Context;
use logit_config::Config;
use serde_norway::value::{Tag, TaggedValue};
use serde_norway::{Mapping, Value};
use std::path::Path;

/// Reads, resolves, and deserializes the config file at `path`.
pub fn load(path: &Path) -> anyhow::Result<Config> {
    load_with(path, &|name| std::env::var(name).ok())
}

/// [`load`] with an injectable environment lookup, so tests never mutate the process environment.
fn load_with(path: &Path, lookup: &impl Fn(&str) -> Option<String>) -> anyhow::Result<Config> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    parse(&raw, lookup).with_context(|| format!("parsing config file {}", path.display()))
}

/// [`load_with`] minus the file read.
fn parse(raw: &str, lookup: &impl Fn(&str) -> Option<String>) -> anyhow::Result<Config> {
    let value: Value = serde_norway::from_str(raw)?;
    let mut path = Vec::new();
    let mut substitutions = Vec::new();
    let value = resolve(value, lookup, &mut path, &mut substitutions)?;
    serde_norway::from_value(value).map_err(|err| annotate(err, &substitutions))
}

/// A `!env` reference that resolved to a non-string, recorded so a deserialization failure can
/// point at it: the likely cause when a string field (a token, a URL) got a number or bool.
struct Substitution {
    path: String,
    var: String,
    type_name: &'static str,
}

/// Appends a note naming each non-string `!env` substitution by path, variable, and resolved type,
/// never the value: these are the fields most likely to hold a secret.
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

/// A short name for a `Value`'s kind, for error messages and a substitution's resolved type.
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

/// How an `!env` value becomes a YAML value.
#[derive(Clone, Copy, PartialEq)]
enum EnvScalar {
    /// Re-parsed as a YAML scalar ([`scalar_from_env`]), so `!env PORT` works in an integer field.
    Retype,
    /// Kept as the string it is. Inside a `lua`/`lua_file` `vars:`, where every type is valid, a
    /// re-parse would silently turn a digit-only token into a number the script rounds.
    String,
}

/// Replaces every `!env VAR_NAME` tag with `VAR_NAME`'s value: a string anywhere inside a
/// `lua`/`lua_file` component's `vars:`, and re-parsed as a YAML scalar everywhere else, so
/// `!env PORT` works in an integer field.
///
/// Rejects any other non-core tag: `serde_norway` drops an unrecognized tag on a non-enum target,
/// so a typo'd `!emv` would otherwise deserialize as the literal variable name.
fn resolve(
    value: Value,
    lookup: &impl Fn(&str) -> Option<String>,
    path: &mut Vec<PathSeg>,
    substitutions: &mut Vec<Substitution>,
) -> anyhow::Result<Value> {
    resolve_as(value, lookup, path, substitutions, EnvScalar::Retype)
}

/// Whether `mapping`, at `path`, is a `lua`/`lua_file` component: `components.<id>` whose `type`
/// is `lua` or `lua_file`, written inline or as `!env VAR`. Runs before the mapping's entries
/// resolve, so an `!env` `type` is looked up here; an unset variable is reported when the entry
/// itself resolves.
fn is_lua_component(
    path: &[PathSeg],
    mapping: &Mapping,
    lookup: &impl Fn(&str) -> Option<String>,
) -> bool {
    if !matches!(path, [PathSeg::Key(root), PathSeg::Key(_)] if root == "components") {
        return false;
    }
    let kind = match mapping.get("type") {
        Some(Value::String(kind)) => Some(kind.clone()),
        Some(Value::Tagged(tagged)) if tagged.tag == "env" => match &tagged.value {
            Value::String(var) => lookup(var),
            _ => None,
        },
        _ => None,
    };
    matches!(kind.as_deref(), Some("lua" | "lua_file"))
}

fn resolve_as(
    value: Value,
    lookup: &impl Fn(&str) -> Option<String>,
    path: &mut Vec<PathSeg>,
    substitutions: &mut Vec<Substitution>,
    env: EnvScalar,
) -> anyhow::Result<Value> {
    match value {
        Value::Mapping(mapping) => {
            let lua = is_lua_component(path, &mapping, lookup);
            let mut resolved = Mapping::with_capacity(mapping.len());
            for (key, value) in mapping {
                let key = resolve_as(key, lookup, path, substitutions, env)?;
                let label = match &key {
                    Value::String(s) => s.clone(),
                    other => format!("<{}>", value_kind(other)),
                };
                let env = match lua && label == "vars" {
                    true => EnvScalar::String,
                    false => env,
                };
                path.push(PathSeg::Key(label));
                let value = resolve_as(value, lookup, path, substitutions, env);
                path.pop();
                resolved.insert(key, value?);
            }
            Ok(Value::Mapping(resolved))
        }
        Value::Sequence(sequence) => {
            let mut resolved = Vec::with_capacity(sequence.len());
            for (index, item) in sequence.into_iter().enumerate() {
                path.push(PathSeg::Index(index));
                let item = resolve_as(item, lookup, path, substitutions, env);
                path.pop();
                resolved.push(item?);
            }
            Ok(Value::Sequence(resolved))
        }
        Value::Tagged(tagged) => {
            let TaggedValue { tag, value } = *tagged;
            if tag == "env" {
                resolve_env_tag(value, tag, lookup, path, substitutions, env)
            } else if tag.to_string().starts_with("!!") {
                // A core-schema tag (`!!str`, `!!int`, ...) isn't ours to interpret, but its
                // contents could nest an `!env`.
                let value = resolve_as(value, lookup, path, substitutions, env)?;
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
    path: &[PathSeg],
    substitutions: &mut Vec<Substitution>,
    env: EnvScalar,
) -> anyhow::Result<Value> {
    let Value::String(var) = value else {
        anyhow::bail!(
            "{}: {tag} expects a variable name (a plain string), got a {}",
            path_string(path),
            value_kind(&value)
        );
    };
    let Some(raw) = lookup(&var) else {
        anyhow::bail!(
            "{}: !env references environment variable '{var}', which is not set",
            path_string(path)
        );
    };
    let resolved = match env {
        EnvScalar::Retype => scalar_from_env(raw),
        EnvScalar::String => Value::String(raw),
    };
    if !matches!(resolved, Value::String(_)) {
        substitutions.push(Substitution {
            path: path_string(path),
            var,
            type_name: value_kind(&resolved),
        });
    }
    Ok(resolved)
}

/// Re-parses an env var's value as a YAML scalar: `8125` becomes an integer, `true` a bool, and
/// anything else, including something that parses as a mapping or sequence, stays the string. An
/// empty variable is the empty string, never null.
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

    #[test]
    fn every_shipped_config_loads_and_validates() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut configs = vec![root.join("demo/logit.yaml")];
        configs.extend(
            std::fs::read_dir(root.join("fixtures"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "yaml")),
        );
        // One directory per example, each holding one or more `*.yaml` configs; a directory
        // with none fails here.
        let examples_dir = root.join("examples");
        for dir in std::fs::read_dir(&examples_dir)
            .unwrap_or_else(|err| panic!("reading {}: {err}", examples_dir.display()))
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_dir())
        {
            let found: Vec<_> = std::fs::read_dir(&dir)
                .unwrap_or_else(|err| panic!("reading {}: {err}", dir.display()))
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "yaml"))
                .collect();
            assert!(!found.is_empty(), "{} holds no *.yaml config", dir.display());
            configs.extend(found);
        }
        // `perf/scenarios/`, `script/shape-survey`'s capture configs, `script/victoria-interop`'s,
        // `script/splunk-interop`'s, and `script/kafka-interop`'s leg configs, and `script/soak`'s
        // scenario configs run only out of CI, so a field rename must fail here, not on their next
        // run.
        let perf_scenarios_dir = root.join("perf/scenarios");
        configs.extend(
            std::fs::read_dir(&perf_scenarios_dir)
                .unwrap_or_else(|err| panic!("reading {}: {err}", perf_scenarios_dir.display()))
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "yaml")),
        );
        // The transit spec's flows (`spec/transit/README.md`), which the spec's own test runs
        // through the flow driver rather than `logit run`.
        let transit_flows_dir = root.join("spec/transit/flows");
        configs.extend(
            std::fs::read_dir(&transit_flows_dir)
                .unwrap_or_else(|err| panic!("reading {}: {err}", transit_flows_dir.display()))
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "yaml")),
        );
        let shape_survey_dir = root.join("tools/shape-survey/configs");
        configs.extend(
            std::fs::read_dir(&shape_survey_dir)
                .unwrap_or_else(|err| panic!("reading {}: {err}", shape_survey_dir.display()))
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "yaml")),
        );
        // The three interop harnesses' per-leg configs and each `script/soak` scenario's
        // per-service configs; those directories' other files are compose, vmagent, and scenario
        // config, so only `logit-*.yaml`.
        let soak_scenarios_dir = root.join("tools/soak/scenarios");
        let soak_scenarios: Vec<_> = std::fs::read_dir(&soak_scenarios_dir)
            .unwrap_or_else(|err| panic!("reading {}: {err}", soak_scenarios_dir.display()))
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_dir())
            .collect();
        let harness_dirs =
            ["tools/victoria-interop", "tools/splunk-interop", "tools/kafka-interop"]
                .into_iter()
                .map(|harness| root.join(harness))
                .chain(soak_scenarios);
        for dir in harness_dirs {
            configs.extend(
                std::fs::read_dir(&dir)
                    .unwrap_or_else(|err| panic!("reading {}: {err}", dir.display()))
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.extension().is_some_and(|extension| extension == "yaml"))
                    .filter(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.starts_with("logit-"))
                    }),
            );
        }
        configs.sort();

        assert!(configs.len() > 1, "expected demo and fixture configs");
        let mut warned = Vec::new();
        for path in configs {
            let config = load_with(&path, &|name| match name {
                "INFLUXDB_TOKEN" => Some("logit-test-token".to_string()),
                "DD_API_KEY" => Some("logit-test-key".to_string()),
                "SPLUNK_OBSERVABILITY_TOKEN" => Some("logit-test-token".to_string()),
                "SPLUNK_HEC_TOKEN" => Some("logit-test-token".to_string()),
                // `script/splunk-interop`'s legs, which take their target from the environment
                // so one config serves a local Splunk and Splunk Cloud.
                "SPLUNK_INTEROP_HEC_URL" => {
                    Some("http://splunk:8088/services/collector".to_string())
                }
                "SPLUNK_INTEROP_HEC_TOKEN" | "SPLUNK_INTEROP_ACK_TOKEN" => {
                    Some("logit-test-token".to_string())
                }
                "SPLUNK_INTEROP_HEC_INSECURE" => Some("false".to_string()),
                // `script/kafka-interop`'s fixed OAUTHBEARER token, fetched at run time.
                "KAFKA_INTEROP_OAUTH_TOKEN" => Some("logit-test-token".to_string()),
                // The whole header value, `Bearer ` included: `!env` substitutes a field, it
                // doesn't interpolate into one.
                "PROMETHEUS_REMOTE_WRITE_AUTHORIZATION" => {
                    Some("Bearer logit-test-token".to_string())
                }
                // `script/soak`'s rolling-overlap instances, which share one config and stamp
                // their own compose service's name on every event.
                "SOAK_INSTANCE" => Some("logit".to_string()),
                _ => None,
            })
            .unwrap_or_else(|err| panic!("{} did not load: {err:#}", path.display()));
            let warnings = crate::pipeline::validate_semantics(config)
                .unwrap_or_else(|err| panic!("{} did not validate: {err:#}", path.display()));
            if !warnings.is_empty() {
                warned.push(format!("{}: {}", path.display(), warnings.join("; ")));
            }
        }
        // Operators copy shipped configs, so each one follows every practice `resolve` warns
        // about.
        assert!(warned.is_empty(), "shipped configs with warnings:\n{}", warned.join("\n"));
    }

    /// A `lookup` backed by a plain map, so tests never touch the real process environment.
    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: HashMap<String, String> =
            vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |name: &str| vars.get(name).cloned()
    }

    fn resolve_yaml(yaml: &str, lookup: &impl Fn(&str) -> Option<String>) -> anyhow::Result<Value> {
        let value: Value = serde_norway::from_str(yaml).unwrap();
        resolve(value, lookup, &mut Vec::new(), &mut Vec::new())
    }

    /// `BufferConfig`'s codecs, unit-tested only against `serde_json`, also work through `parse`.
    #[test]
    fn buffer_config_round_trips_through_the_real_yaml_path() {
        let yaml = r#"
components:
  in:
    type: statsd_in
    bind: 127.0.0.1:8125
  out:
    type: influxdb_out
    sources: [in]
    url: http://localhost:8086
    org: o
    bucket: b
    token: t
    buffer:
      max_batches: 4096
      max_bytes: 64MiB
      overflow: drop_oldest
      delivery: at_least_once
      retry_max_delay: 20s
      shutdown_grace: 10s
"#;
        let config = parse(yaml, &env(&[])).expect("should parse");
        let out = &config.components["out"];
        assert_eq!(out.buffer.max_batches, 4096);
        assert_eq!(out.buffer.max_bytes, 64 * 1024 * 1024, "64MiB should parse via serde_norway");
        assert_eq!(out.buffer.overflow, logit_config::OverflowPolicy::DropOldest);
        assert_eq!(out.buffer.delivery, Some(logit_config::DeliveryPosture::AtLeastOnce));
        assert_eq!(out.buffer.retry_max_delay, std::time::Duration::from_secs(20));
        assert_eq!(out.buffer.shutdown_grace, std::time::Duration::from_secs(10));
    }

    /// `sample`'s unit-variant and map `key:` forms and a `Bool` `always_keep.value` read via YAML.
    #[test]
    fn sample_key_and_override_read_through_the_real_yaml_path() {
        let yaml = r#"
components:
  in:
    type: statsd_in
    bind: 127.0.0.1:8125
  by_trace:
    type: sample
    sources: [in]
    rate: 0.5
    key: trace_id
    always_keep:
      attribute: sampling.keep
      value: true
  by_attr:
    type: sample
    sources: [in]
    rate: 0.1
    key: {attribute: request_id}
    missing: drop
"#;
        let config = parse(yaml, &env(&[])).expect("should parse");
        match &config.components["by_trace"].kind {
            logit_config::ComponentKind::Sample { key, always_keep, .. } => {
                assert_eq!(key, &Some(logit_config::SampleKey::TraceId));
                let o = always_keep.as_ref().unwrap();
                assert_eq!(o.attribute.as_deref(), Some("sampling.keep"));
                assert_eq!(o.value, Some(logit_config::SetValue::Bool(true)));
            }
            other => panic!("expected Sample, got {other:?}"),
        }
        match &config.components["by_attr"].kind {
            logit_config::ComponentKind::Sample { key, missing, .. } => {
                assert_eq!(key, &Some(logit_config::SampleKey::Attribute("request_id".into())));
                assert_eq!(missing, &Some(logit_config::SampleMissing::Drop));
            }
            other => panic!("expected Sample, got {other:?}"),
        }
    }

    /// A `lua_file`'s `vars:` reads through the real YAML path, `Component`'s flatten included,
    /// with `!env` resolved inside it.
    #[test]
    fn lua_vars_read_through_the_real_yaml_path() {
        use logit_config::LuaVar;
        let yaml = r#"
components:
  in:
    type: lines_in
    bind: 127.0.0.1:5140
  prusa:
    type: lua_file
    sources: [in]
    lua_file: prusa.lua
    vars:
      bucket: !env PRUSA_BUCKET
      port: !env PRUSA_PORT
      names:
        "10:9c:70:2d:d5:c4": sand
      hosts: [a, 2, 0.5, true]
  out:
    type: null_out
    sources: [prusa]
"#;
        let config =
            parse(yaml, &env(&[("PRUSA_BUCKET", "printers"), ("PRUSA_PORT", "8086")])).unwrap();
        let vars = match &config.components["prusa"].kind {
            logit_config::ComponentKind::LuaFile { vars, .. } => vars.clone(),
            other => panic!("expected LuaFile, got {other:?}"),
        };
        assert_eq!(vars["bucket"], LuaVar::String("printers".into()));
        assert_eq!(vars["port"], LuaVar::String("8086".into()), "an !env value is a string");
        match &vars["names"] {
            LuaVar::Map(names) => {
                assert_eq!(names["10:9c:70:2d:d5:c4"], LuaVar::String("sand".into()))
            }
            other => panic!("expected a map, got {other:?}"),
        }
        assert_eq!(
            vars["hosts"],
            LuaVar::List(vec![
                LuaVar::String("a".into()),
                LuaVar::Integer(2),
                LuaVar::Float(0.5),
                LuaVar::Bool(true),
            ])
        );
    }

    /// A literal YAML `null` and an integer past 2^53 survive parsing so validation can reject
    /// them by path without printing the value.
    #[test]
    fn lua_vars_null_and_wide_integers_fail_validation_by_path() {
        for (vars, want) in [
            ("names: {sand: ~}", "'vars.names.sand' is null"),
            ("ids: [1, 9007199254740993]", "'vars.ids[1]' is an integer larger than 2^53"),
            ("big: 18446744073709551615", "'vars.big' is an integer larger than 2^53"),
        ] {
            let yaml = format!(
                "components:\n  in: {{type: lines_in, bind: '127.0.0.1:5140'}}\n  \
                 script:\n    type: lua\n    sources: [in]\n    script: x\n    vars:\n      \
                 {vars}\n  out: {{type: null_out, sources: [script]}}\n"
            );
            let config = parse(&yaml, &env(&[])).unwrap_or_else(|e| panic!("{vars}: {e:#}"));
            let err = crate::pipeline::validate_semantics(config).unwrap_err().to_string();
            assert!(err.contains(want), "{vars}: want {want:?}, got: {err}");
            assert!(!err.contains("18446744073709551615"), "the value leaked: {err}");
            assert!(!err.contains("9007199254740993"), "the value leaked: {err}");
        }
    }

    /// An `!env` value anywhere inside a `lua`/`lua_file` `vars:` stays the string it is, however
    /// it looks, so a digit-only token never reaches a script as a rounded number. `!env` outside
    /// `vars:`, in the same component and in another, is still re-parsed.
    #[test]
    fn env_values_inside_lua_vars_are_strings() {
        use logit_config::{ComponentKind, LuaVar};
        let yaml = r#"
components:
  in:
    type: statsd_in
    bind: 127.0.0.1:8125
    transport: tcp
    max_connections: !env MAX
  inline:
    type: lua
    sources: [in]
    script: x
    jit: !env JIT
    vars:
      token: !env TOKEN
      big: !env BIG
      flag: !env JIT
      gone: !env NOTHING
      nested: {list: [!env TOKEN]}
  file:
    type: lua_file
    sources: [inline]
    lua_file: x.lua
    vars:
      token: !env TOKEN
  out:
    type: null_out
    sources: [file]
"#;
        let lookup = env(&[
            ("MAX", "7"),
            ("JIT", "true"),
            ("TOKEN", "1234567890123456"),
            ("BIG", "18446744073709551615"),
            ("NOTHING", "null"),
        ]);
        let config = parse(yaml, &lookup).unwrap();
        let s = |v: &str| LuaVar::String(v.into());
        match &config.components["inline"].kind {
            ComponentKind::Lua { jit, vars, .. } => {
                assert!(*jit, "jit, outside vars, is still a re-parsed boolean");
                assert_eq!(vars["token"], s("1234567890123456"));
                assert_eq!(vars["big"], s("18446744073709551615"));
                assert_eq!(vars["flag"], s("true"));
                assert_eq!(vars["gone"], s("null"));
                assert_eq!(
                    vars["nested"],
                    LuaVar::Map(
                        [("list".into(), LuaVar::List(vec![s("1234567890123456")]))].into()
                    )
                );
            }
            other => panic!("expected Lua, got {other:?}"),
        }
        match &config.components["file"].kind {
            ComponentKind::LuaFile { vars, .. } => {
                assert_eq!(vars["token"], s("1234567890123456"))
            }
            other => panic!("expected LuaFile, got {other:?}"),
        }
        match &config.components["in"].kind {
            ComponentKind::StatsdIn { max_connections, .. } => assert_eq!(*max_connections, 7),
            other => panic!("expected StatsdIn, got {other:?}"),
        }
        crate::pipeline::validate_semantics(config).expect("every !env value in vars is valid");
    }

    /// A `vars` key on a component that isn't `lua`/`lua_file`, or a `vars` nested deeper than a
    /// component's top level, keeps the re-parse: only a Lua component's own `vars:` is exempt.
    #[test]
    fn env_values_keep_their_type_outside_a_lua_components_vars() {
        let yaml = "components:\n  c:\n    type: aggregate\n    vars: !env N\n  \
                    d:\n    type: lua\n    other: {type: lua, vars: !env N}\n";
        let value: Value = serde_norway::from_str(yaml).unwrap();
        let resolved =
            resolve(value, &env(&[("N", "5")]), &mut Vec::new(), &mut Vec::new()).unwrap();
        assert_eq!(resolved["components"]["c"]["vars"], Value::from(5));
        assert_eq!(resolved["components"]["d"]["other"]["vars"], Value::from(5));
    }

    /// A `type` written as `!env` still marks a Lua component, so its `vars:` keeps `!env` values
    /// as strings.
    #[test]
    fn env_values_inside_vars_are_strings_when_the_type_is_env() {
        use logit_config::{ComponentKind, LuaVar};
        let yaml = "components:\n  in: {type: lines_in, bind: '127.0.0.1:5140'}\n  \
                    script:\n    type: !env KIND\n    sources: [in]\n    script: x\n    \
                    vars: {token: !env TOKEN}\n  out: {type: null_out, sources: [script]}\n";
        let config = parse(yaml, &env(&[("KIND", "lua"), ("TOKEN", "1234567890123456")])).unwrap();
        match &config.components["script"].kind {
            ComponentKind::Lua { vars, .. } => {
                assert_eq!(vars["token"], LuaVar::String("1234567890123456".into()))
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    /// `timestamp`'s unit-variant and `{pattern: ..}` `format:` forms read via YAML.
    #[test]
    fn timestamp_format_reads_through_the_real_yaml_path() {
        let yaml = r#"
components:
  in:
    type: statsd_in
    bind: 127.0.0.1:8125
  sys:
    type: timestamp
    sources: [in]
    from: syslog.timestamp
    format: rfc3164
    timezone: Europe/Berlin
  web:
    type: timestamp
    sources: [in]
    from: time_local
    format: {pattern: "%d/%b/%Y:%H:%M:%S %z"}
    max_skew: 168h
"#;
        let config = parse(yaml, &env(&[])).expect("should parse");
        match &config.components["sys"].kind {
            logit_config::ComponentKind::Timestamp { format, timezone, max_skew, .. } => {
                assert_eq!(format, &logit_config::TimestampFormat::Rfc3164);
                assert_eq!(timezone.as_deref(), Some("Europe/Berlin"));
                assert_eq!(*max_skew, logit_config::default_timestamp_max_skew());
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
        match &config.components["web"].kind {
            logit_config::ComponentKind::Timestamp { format, max_skew, .. } => {
                assert_eq!(
                    format,
                    &logit_config::TimestampFormat::Pattern("%d/%b/%Y:%H:%M:%S %z".into())
                );
                assert_eq!(*max_skew, std::time::Duration::from_secs(168 * 3600));
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    /// `route`'s `routes:` values read as a target id or a list of them, through YAML.
    #[test]
    fn route_targets_read_through_the_real_yaml_path() {
        let yaml = r#"
components:
  in:
    type: otlp_in
    bind: 127.0.0.1:4318
  split:
    type: route
    sources: [in]
    by: {attribute: stream}
    routes:
      logs: [logs_stream, archive]
      traces: traces_stream
"#;
        let config = parse(yaml, &env(&[])).expect("should parse");
        match &config.components["split"].kind {
            logit_config::ComponentKind::Route { by, routes } => {
                assert_eq!(by, &logit_config::RouteBy::Attribute("stream".into()));
                assert_eq!(
                    routes["logs"],
                    logit_config::RouteTargets(vec!["logs_stream".into(), "archive".into()])
                );
                assert_eq!(routes["traces"], "traces_stream".into());
            }
            other => panic!("expected Route, got {other:?}"),
        }
    }

    /// `route_signals`' `signals:` map and its `strip` default read through YAML.
    #[test]
    fn route_signals_reads_through_the_real_yaml_path() {
        let yaml = r#"
components:
  in:
    type: otlp_in
    bind: 127.0.0.1:4318
  by_signal:
    type: route_signals
    sources: [in]
    signals:
      metrics: metric_events
      logs: log_events
      traces: log_events
"#;
        let config = parse(yaml, &env(&[])).expect("should parse");
        match &config.components["by_signal"].kind {
            logit_config::ComponentKind::RouteSignals { signals, strip } => {
                assert!(*strip);
                assert_eq!(signals.logs.as_deref(), Some("log_events"));
                assert_eq!(signals.metrics.as_deref(), Some("metric_events"));
                assert_eq!(signals.traces.as_deref(), Some("log_events"));
            }
            other => panic!("expected RouteSignals, got {other:?}"),
        }
    }

    /// A `routes:` value of any other shape names both accepted forms in its error.
    #[test]
    fn a_route_value_of_the_wrong_shape_names_both_forms() {
        let yaml = r#"
components:
  in:
    type: statsd_in
    bind: 127.0.0.1:8125
  split:
    type: route
    sources: [in]
    by: {attribute: stream}
    routes:
      host: {target: host_stream}
"#;
        let err = format!("{:#}", parse(yaml, &env(&[])).expect_err("a map is not a route value"));
        assert!(
            err.contains("invalid type: map, expected a target id or a list of target ids"),
            "got: {err}"
        );
    }

    fn statsd_socket_mode(raw: &str) -> anyhow::Result<Option<u32>> {
        let yaml = format!(
            "components:\n  in:\n    type: statsd_in\n    bind: /run/dsd.socket\n    \
             transport: unix\n    socket_mode: {raw}\n  out:\n    type: null_out\n    \
             sources: [in]\n"
        );
        let config = parse(&yaml, &env(&[]))?;
        match &config.components["in"].kind {
            logit_config::ComponentKind::StatsdIn { socket_mode, .. } => {
                Ok(socket_mode.map(logit_config::SocketMode::bits))
            }
            other => panic!("expected StatsdIn, got {other:?}"),
        }
    }

    /// `socket_mode` through YAML: the string forms read as octal, and a YAML number is rejected
    /// with a hint to quote it.
    #[test]
    fn socket_mode_reads_through_the_real_yaml_path() {
        assert_eq!(statsd_socket_mode(r#""0660""#).unwrap(), Some(0o660));
        assert_eq!(statsd_socket_mode("'660'").unwrap(), Some(0o660));
        for raw in ["660", "0o660", "0x1b0"] {
            let err = format!("{:#}", statsd_socket_mode(raw).unwrap_err());
            assert!(err.contains("quote it"), "{raw}: {err}");
        }
        // YAML 1.2 reads a plain scalar with a leading `0` as a string, so it reaches the parser
        // as written; YAML 1.1 would read the same octal value.
        assert_eq!(statsd_socket_mode("0660").unwrap(), Some(0o660));
    }

    /// A quoted bare-number `max_bytes` reads via YAML (`human_bytes` rejects an unquoted one).
    #[test]
    fn buffer_config_quoted_bare_number_max_bytes_round_trips_through_the_real_yaml_path() {
        let yaml = r#"
components:
  in:
    type: statsd_in
    bind: 127.0.0.1:8125
  out:
    type: influxdb_out
    sources: [in]
    url: http://localhost:8086
    org: o
    bucket: b
    token: t
    buffer:
      max_bytes: "134217728"
"#;
        let config = parse(yaml, &env(&[])).expect("should parse");
        assert_eq!(config.components["out"].buffer.max_bytes, 134_217_728);
    }

    #[test]
    fn substitutes_a_variable_into_a_string_field() {
        let value = resolve_yaml("url: !env URL", &env(&[("URL", "http://x")])).unwrap();
        assert_eq!(value["url"], Value::String("http://x".to_string()));
    }

    #[test]
    fn a_numeric_value_resolves_as_a_number() {
        let value = resolve_yaml("port: !env PORT", &env(&[("PORT", "8125")])).unwrap();
        assert_eq!(value["port"], Value::from(8125));
    }

    #[test]
    fn a_boolean_value_resolves_as_a_bool() {
        let value = resolve_yaml("flag: !env FLAG", &env(&[("FLAG", "true")])).unwrap();
        assert_eq!(value["flag"], Value::Bool(true));
    }

    #[test]
    fn a_duration_style_value_stays_a_string() {
        let value = resolve_yaml("interval: !env WINDOW", &env(&[("WINDOW", "10s")])).unwrap();
        assert_eq!(value["interval"], Value::String("10s".to_string()));
    }

    #[test]
    fn an_empty_variable_stays_the_empty_string_not_null() {
        let value = resolve_yaml("token: !env TOKEN", &env(&[("TOKEN", "")])).unwrap();
        assert_eq!(value["token"], Value::String(String::new()));
    }

    #[test]
    fn a_mapping_or_sequence_looking_value_stays_a_string() {
        let value =
            resolve_yaml("x: !env X", &env(&[("X", "[not, actually, a, sequence, field]")]))
                .unwrap();
        assert_eq!(value["x"], Value::String("[not, actually, a, sequence, field]".to_string()));
    }

    #[test]
    fn substitutes_inside_a_sequence() {
        let value =
            resolve_yaml("sources: [!env A, plain, !env B]", &env(&[("A", "in"), ("B", "out")]))
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
        let value = resolve_yaml(yaml, &env(&[("TOKEN", "secret")])).unwrap();
        assert_eq!(value["outer"]["inner"]["token"], Value::String("secret".to_string()));
    }

    #[test]
    fn a_missing_variable_errors_naming_the_variable_and_the_config_path() {
        let err = resolve_yaml("components:\n  out:\n    token: !env INFLUXDB_TOKEN\n", &env(&[]))
            .expect_err("expected an error");
        assert!(err.to_string().contains("INFLUXDB_TOKEN"), "got: {err}");
        assert!(err.to_string().contains("components.out.token"), "got: {err}");
    }

    /// A missing variable errors in any field: no lenient mode (`docs/adr/env-yaml-tag.md`).
    #[test]
    fn a_missing_variable_errors_regardless_of_which_field_it_is_in() {
        let err = resolve_yaml("interval: !env WINDOW", &env(&[])).expect_err("expected an error");
        assert!(err.to_string().contains("WINDOW"), "got: {err}");
    }

    #[test]
    fn env_with_a_non_string_argument_is_rejected() {
        let err = resolve_yaml("x: !env [A, B]", &env(&[])).expect_err("expected an error");
        assert!(err.to_string().contains("expects a variable name"), "got: {err}");
    }

    #[test]
    fn an_unknown_tag_is_rejected_rather_than_silently_passed_through() {
        let err = resolve_yaml("token: !emv TOKEN", &env(&[("TOKEN", "secret")]))
            .expect_err("expected an error");
        assert!(err.to_string().contains("unknown tag"), "got: {err}");
        assert!(err.to_string().contains("emv"), "got: {err}");
    }

    #[test]
    fn a_config_with_no_tags_round_trips_unchanged() {
        let yaml = "a: 1\nb: [x, y]\nc:\n  d: true\n";
        let value = resolve_yaml(yaml, &env(&[])).unwrap();
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
        let err = parse(raw, &env(&[("TOKEN", "123456")]))
            .expect_err("a numeric token should fail to deserialize as a String field");
        let message = format!("{err:?}");
        assert!(message.contains("components.out.token"), "got: {message}");
        assert!(message.contains("TOKEN"), "got: {message}");
        assert!(message.contains("integer"), "got: {message}");
    }
}
