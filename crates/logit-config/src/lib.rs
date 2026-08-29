//! Config types for `logit`.
//!
//! Every type here derives both `Deserialize` and `JsonSchema` together (ADR 0003) so the
//! published JSON Schema (`logit schema`, `schema/logit.schema.json`) can never drift from what
//! the binary actually accepts. YAML parsing itself (via a maintained `serde_yaml` fork, per
//! ADR 0003) belongs to `logit-cli`, not here -- this crate only defines the shape.

use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Config {
    #[serde(default)]
    pub inputs: HashMap<String, InputConfig>,
    #[serde(default)]
    pub outputs: HashMap<String, OutputConfig>,
    #[serde(default)]
    #[schemars(schema_with = "non_empty_pipelines_schema")]
    pub pipelines: HashMap<String, PipelineConfig>,
}

fn non_empty_pipelines_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = HashMap::<String, PipelineConfig>::json_schema(generator);
    if let Schema::Object(schema) = &mut schema {
        schema.object().min_properties = Some(1);
    }
    schema
}

/// One named pipeline: inputs feed transforms feed outputs. See `docs/design/lua-api.md` for the
/// transform chain's config shape.
///
/// `inputs`/`outputs` are marked `minItems: 1` in the generated schema -- `logit run` rejects a
/// pipeline with either empty (see `logit-cli::pipeline::validate_semantics`), so the schema
/// shouldn't claim otherwise (ADR 0003).
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PipelineConfig {
    #[schemars(length(min = 1))]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub transforms: Vec<TransformConfig>,
    #[schemars(length(min = 1))]
    pub outputs: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputConfig {
    /// statsd / DogStatsD-style tagged metrics over UDP.
    Statsd { bind: String },
    /// RFC 3164 / RFC 5424 syslog over UDP or TCP.
    Syslog { bind: String },
    /// OpenTelemetry Protocol (logs, metrics, and/or traces).
    Otlp { bind: String },
    /// Tail one or more files as a log source, rotation- and checkpoint-aware.
    FileTail {
        paths: Vec<String>,
        #[serde(default)]
        checkpoint_path: Option<String>,
    },
    /// The native logit-to-logit protocol (`docs/design/wire-protocol.md`).
    Logit { bind: String },
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputConfig {
    InfluxDb {
        url: String,
        org: String,
        bucket: String,
        /// Referenced, not inlined -- kept out of the schema/config-dump path deliberately.
        token_env: String,
    },
    Otlp {
        endpoint: String,
    },
    /// The native logit-to-logit protocol (`docs/design/wire-protocol.md`).
    Logit {
        endpoint: String,
    },
}

/// A transform pipeline stage. Built-in native processors (no Lua VM involved) are meant to sit
/// in front of user Lua in the same chain -- "parse the JSON body, then run my logic" -- rather
/// than being an either/or with scripting. See `docs/design/lua-api.md`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum TransformConfig {
    Builtin(BuiltinTransformConfig),
    /// Inline Lua source (a YAML block scalar in practice).
    Lua {
        lua: String,
    },
    /// A `.lua` file path, relative to the config file.
    LuaFile {
        lua_file: String,
    },
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "builtin", rename_all = "snake_case")]
pub enum BuiltinTransformConfig {
    Json,
    Logfmt,
    Kv,
    Regex {
        pattern: String,
    },
    Csv,
    Rename {
        from: String,
        to: String,
    },
    Remove {
        field: String,
    },
    Filter {
        r#where: String,
    },
    Sample {
        rate: f64,
    },
    Throttle {
        limit: u64,
        #[serde(with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        window: Duration,
    },
    Dedup {
        key: String,
    },
    /// The stateful aggregator (counters/gauges/sets/distributions). Runs `flush()` on
    /// `interval`; see `docs/design/lua-api.md`.
    Aggregate {
        #[serde(with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        interval: Duration,
    },
}

/// Minimal `humantime`-flavored `(de)serialize` for `Duration` fields (`10s`, `1m`, ...), so
/// config keeps human-readable durations without pulling in a full external crate for one helper.
/// TODO: replace with the `humantime-serde` crate once the crate list is finalized.
mod humantime_serde_duration {
    use super::*;
    use serde::{de::Error as _, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{}s", d.as_secs_f64()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let raw = String::deserialize(d)?;
        let (num, unit) = raw.trim().split_at(
            raw.trim().find(|c: char| !c.is_ascii_digit() && c != '.').ok_or_else(|| {
                D::Error::custom("expected a number followed by a unit, e.g. 10s")
            })?,
        );
        let n: f64 = num.parse().map_err(D::Error::custom)?;
        let secs = match unit {
            "ms" => n / 1000.0,
            "s" => n,
            "m" => n * 60.0,
            "h" => n * 3600.0,
            other => return Err(D::Error::custom(format!("unknown duration unit '{other}'"))),
        };
        Ok(Duration::from_secs_f64(secs))
    }
}

/// Generate the published JSON Schema for [`Config`]. Backs the `logit schema` CLI command
/// (ADR 0003) -- CI regenerates `schema/logit.schema.json` from this and fails if it's stale.
pub fn json_schema() -> schemars::schema::RootSchema {
    schemars::schema_for!(Config)
}

#[cfg(test)]
mod tests {
    use schemars::schema::Schema;

    #[test]
    fn pipelines_schema_requires_at_least_one_entry() {
        let schema = super::json_schema();
        let pipelines = schema
            .schema
            .object
            .expect("config should be an object")
            .properties
            .remove("pipelines")
            .expect("config should define pipelines");
        let Schema::Object(pipelines) = pipelines else {
            panic!("pipelines should have an object schema");
        };
        assert_eq!(
            pipelines.object.expect("pipelines should be an object").min_properties,
            Some(1)
        );
    }
}
