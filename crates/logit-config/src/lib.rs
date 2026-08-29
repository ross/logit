//! Config types for `logit`.
//!
//! Every type here derives both `Deserialize` and `JsonSchema` together (ADR 0003) so the
//! published JSON Schema (`logit schema`, `schema/logit.schema.json`) can never drift from what
//! the binary actually accepts. YAML parsing itself (via a maintained `serde_yaml` fork, per
//! ADR 0003) belongs to `logit-cli`, not here -- this crate only defines the shape.

use schemars::JsonSchema;
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
    pub pipelines: HashMap<String, PipelineConfig>,
}

/// One named pipeline: inputs feed transforms feed outputs. See `docs/design/lua-api.md` for the
/// transform chain's config shape.
///
/// `inputs`/`outputs` are marked `minItems: 1` in the generated schema -- `logit run` rejects a
/// pipeline with either empty (see `logit-cli::pipeline::validate_semantics`), so the schema
/// shouldn't claim otherwise (ADR 0003). The equivalent "at least one pipeline" rule on
/// `Config::pipelines` has no schema-level expression: schemars 0.8's `length` attribute covers
/// array/string schemas, not the `minProperties` a `HashMap`-backed object schema would need --
/// `validate_semantics` is the only place that rule is enforced.
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
        /// Runs this stage's `flush()`, if the script defines one, on this interval
        /// (`docs/design/lua-api.md`'s flush contract). Omitted -- the common case -- means the
        /// stage never ticks, same as a script with no `flush()` at all.
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        interval: Option<Duration>,
    },
    /// A `.lua` file path, relative to the config file.
    LuaFile {
        lua_file: String,
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        interval: Option<Duration>,
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
        parse(&raw).map_err(D::Error::custom)
    }

    fn parse(raw: &str) -> Result<Duration, String> {
        let (num, unit) = raw.trim().split_at(
            raw.trim()
                .find(|c: char| !c.is_ascii_digit() && c != '.')
                .ok_or_else(|| "expected a number followed by a unit, e.g. 10s".to_string())?,
        );
        let n: f64 = num.parse().map_err(|e| format!("{e}"))?;
        let secs = match unit {
            "ms" => n / 1000.0,
            "s" => n,
            "m" => n * 60.0,
            "h" => n * 3600.0,
            other => return Err(format!("unknown duration unit '{other}'")),
        };
        Ok(Duration::from_secs_f64(secs))
    }

    /// The same codec, for `Option<Duration>` fields (`#[serde(default, with =
    /// "humantime_serde_duration::option")]`) -- used by the Lua transform variants' optional
    /// `interval`. A nested module because `#[serde(with = "...")]` on an `Option<Duration>`
    /// field calls *this* module's `serialize`/`deserialize` with `Option<Duration>`, not the
    /// parent's `Duration` ones.
    pub mod option {
        use super::*;

        pub fn serialize<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
            match d {
                Some(d) => super::serialize(d, s),
                None => s.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
            let raw: Option<String> = Option::deserialize(d)?;
            raw.map(|raw| parse(&raw).map_err(D::Error::custom)).transpose()
        }
    }
}

/// Generate the published JSON Schema for [`Config`]. Backs the `logit schema` CLI command
/// (ADR 0003) -- CI regenerates `schema/logit.schema.json` from this and fails if it's stale.
pub fn json_schema() -> schemars::schema::RootSchema {
    schemars::schema_for!(Config)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deserialized via `serde_json` rather than the YAML this crate is actually fed through
    // `logit-cli` (deliberately not a dependency here -- see the crate doc comment): JSON and
    // YAML are both self-describing formats that pick an `untagged` variant by field presence, so
    // this exercises exactly the same disambiguation `TransformConfig`'s real deserializer does.

    #[test]
    fn lua_stage_without_interval_deserializes_as_lua() {
        let config: TransformConfig = serde_json::from_str(r#"{"lua": "return event"}"#).unwrap();
        match config {
            TransformConfig::Lua { lua, interval } => {
                assert_eq!(lua, "return event");
                assert_eq!(interval, None);
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn lua_stage_with_interval_deserializes_as_lua_not_builtin() {
        let config: TransformConfig =
            serde_json::from_str(r#"{"lua": "return event", "interval": "10s"}"#).unwrap();
        match config {
            TransformConfig::Lua { interval, .. } => {
                assert_eq!(interval, Some(Duration::from_secs(10)));
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn lua_file_stage_with_interval_deserializes_as_lua_file() {
        let config: TransformConfig =
            serde_json::from_str(r#"{"lua_file": "x.lua", "interval": "1m"}"#).unwrap();
        match config {
            TransformConfig::LuaFile { lua_file, interval } => {
                assert_eq!(lua_file, "x.lua");
                assert_eq!(interval, Some(Duration::from_secs(60)));
            }
            other => panic!("expected LuaFile, got {other:?}"),
        }
    }

    #[test]
    fn builtin_aggregate_with_interval_deserializes_as_builtin() {
        let config: TransformConfig =
            serde_json::from_str(r#"{"builtin": "aggregate", "interval": "10s"}"#).unwrap();
        match config {
            TransformConfig::Builtin(BuiltinTransformConfig::Aggregate { interval }) => {
                assert_eq!(interval, Duration::from_secs(10));
            }
            other => panic!("expected Builtin(Aggregate), got {other:?}"),
        }
    }

    #[test]
    fn zero_interval_deserializes_fine_left_for_validation_to_reject() {
        // The codec itself has no opinion on zero -- `logit-cli::pipeline::validate_semantics` is
        // where a zero flush interval is actually rejected (it would spin the flush loop).
        let config: TransformConfig =
            serde_json::from_str(r#"{"lua": "x", "interval": "0s"}"#).unwrap();
        match config {
            TransformConfig::Lua { interval, .. } => assert_eq!(interval, Some(Duration::ZERO)),
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn negative_interval_is_rejected_by_the_codec() {
        let result: Result<TransformConfig, _> =
            serde_json::from_str(r#"{"lua": "x", "interval": "-5s"}"#);
        assert!(result.is_err(), "a negative duration should not silently parse");
    }

    #[test]
    fn interval_round_trips_through_serialize_then_deserialize() {
        let original =
            TransformConfig::Lua { lua: "x".to_string(), interval: Some(Duration::from_secs(30)) };
        let json = serde_json::to_string(&original).unwrap();
        let round_tripped: TransformConfig = serde_json::from_str(&json).unwrap();
        match round_tripped {
            TransformConfig::Lua { interval, .. } => {
                assert_eq!(interval, Some(Duration::from_secs(30)));
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }
}
