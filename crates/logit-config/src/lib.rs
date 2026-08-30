//! Config types for `logit`.
//!
//! Every type here derives both `Deserialize` and `JsonSchema` together (ADR 0003) so the
//! published JSON Schema (`logit schema`, `schema/logit.schema.json`) can never drift from what
//! the binary actually accepts. YAML parsing itself (via a maintained `serde_yaml` fork, per
//! ADR 0003) belongs to `logit-cli`, not here -- this crate only defines the shape.
//!
//! Config is one flat graph of named [`Component`]s (ADR 0009,
//! `docs/design/pipeline-graph.md`) -- there is no separate inputs/outputs/pipelines split. A
//! component's `sources` name the other components it reads from; its `type`-tagged
//! [`ComponentKind`] fixes its arity (a listener has none, a sink has at least one and is never
//! itself a source, a transform has both). Resolving that graph into something runnable -- cycle
//! detection, arity checks, topological ordering -- is `logit-cli`/`logit-pipeline`'s job, not
//! this crate's; this crate only defines the shape serde and `schemars` need to agree on.

use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Config {
    #[serde(default)]
    #[schemars(schema_with = "non_empty_components_schema")]
    pub components: HashMap<String, Component>,
}

fn non_empty_components_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = HashMap::<String, Component>::json_schema(generator);
    if let Schema::Object(schema) = &mut schema {
        schema.object().min_properties = Some(1);
    }
    schema
}

/// One node in the pipeline's component graph. `sources` names the other components this one
/// reads events from -- empty for a listener, required for everything else (enforced at
/// validation time, not in this schema: which arity is legal depends on `kind`, not something a
/// blanket `minItems` on this shared field can express). See `docs/design/pipeline-graph.md` for
/// the full arity table and validation rules.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Component {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(flatten)]
    pub kind: ComponentKind,
}

/// One `kv_metrics` entry: a metric `name`, an optional source `field`, and an optional `unit`.
/// See `ComponentKind::KvMetrics` and `docs/adr/0014-kv-metrics-semantics.md`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MetricSpec {
    /// The metric's measurement name. An empty name is rejected at graph-validation time --
    /// `influxdb_out` requires a non-empty measurement to encode a metric line.
    pub name: String,
    /// The attribute to read this metric's value from. Omitted means "+1 per event" for a
    /// counter or "set to 1" for a gauge; a distribution entry with no `field` is rejected at
    /// graph-validation time (a distribution of nothing is meaningless). Names an attribute
    /// literally -- `field: http.status` means the attribute literally named `http.status`, never
    /// a `status` key nested under `http` in a `Value::Map`; nested fields are not addressable.
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// A component's kind, tagged by `type` in config. Every protocol kind is suffixed `_in`/`_out`
/// uniformly (`docs/design/pipeline-graph.md`'s naming rationale) so a listener and a sink for the
/// same protocol never collide on one tag value; transform kinds take no suffix, since there's
/// only one direction for a transform to be.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ComponentKind {
    /// statsd / DogStatsD-style tagged metrics over UDP.
    StatsdIn {
        bind: String,
    },
    /// RFC 3164 / RFC 5424 syslog over UDP or TCP.
    SyslogIn {
        bind: String,
    },
    /// OpenTelemetry Protocol (logs, metrics, and/or traces).
    OtlpIn {
        bind: String,
    },
    /// Tail one or more files as a log source, rotation- and checkpoint-aware.
    FileTail {
        paths: Vec<String>,
        #[serde(default)]
        checkpoint_path: Option<String>,
    },
    /// The native logit-to-logit protocol (`docs/design/wire-protocol.md`).
    LogitIn {
        bind: String,
    },

    /// Inline Lua source (a YAML block scalar in practice). See `docs/design/lua-api.md`.
    Lua {
        script: String,
        /// Runs this component's `flush()`, if the script defines one, on this interval
        /// (`docs/design/lua-api.md`'s flush contract). Omitted -- the common case -- means the
        /// component never ticks, same as a script with no `flush()` at all.
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
    /// The stateful aggregator (counters/gauges/sets/distributions). Runs `flush()` on
    /// `interval`; see `docs/adr/0008-aggregation-window-semantics.md`.
    Aggregate {
        #[serde(with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        interval: Duration,
    },
    /// Parses a log record's message as JSON, merging the resulting key/values into the event's
    /// attributes. See `docs/adr/0010-json-parsing-into-attributes.md`.
    Json {
        /// Skip everything before the first `{` and parse from there -- for lines with a
        /// non-JSON prefix (`2026-08-29 INFO {"a":1}`). Off by default: the whole line is
        /// assumed to be the JSON data.
        #[serde(default)]
        skip_to_brace: bool,
    },
    /// Turns attributes already on an event (typically merged there by `json`) into metrics on
    /// that same event. See `docs/adr/0014-kv-metrics-semantics.md` for the skip rules, the
    /// numeric coercion rules, and why there is deliberately no `tags:` field here -- tag
    /// selection is `Keep`'s job, since every metrics sink already reads `event.attributes`.
    KvMetrics {
        #[serde(default)]
        counters: Vec<MetricSpec>,
        #[serde(default)]
        gauges: Vec<MetricSpec>,
        #[serde(default)]
        distributions: Vec<MetricSpec>,
    },
    /// Retains only the named attributes, dropping the rest -- an allowlist, not just a denylist:
    /// a new field appearing in a log format later must not be able to silently become a new
    /// tag dimension on a metrics sink. Place this *before* `aggregate` in a pipeline --
    /// `aggregate`'s `SeriesKey` includes the whole of `event.attributes`, so pruning first is
    /// what keeps series cardinality and per-window memory bounded. An empty `fields` list is
    /// legal and means "drop every attribute."
    Keep {
        fields: Vec<String>,
    },
    /// Drops the named attributes, keeping the rest.
    Remove {
        fields: Vec<String>,
    },
    // The rest of the built-in native transforms -- not implemented yet (`logit-transforms`),
    // carried over as unimplemented `ComponentKind` variants so config referencing one gets a
    // clear "not implemented yet" at validation time rather than a deserialization error.
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

    /// `rename`d explicitly: `rename_all = "snake_case"` alone would tag this `influx_db_out`
    /// (a word break at the embedded capital `Db`), not `influxdb_out` as published in
    /// `docs/design/pipeline-graph.md` and every example config.
    #[serde(rename = "influxdb_out")]
    InfluxDbOut {
        url: String,
        org: String,
        bucket: String,
        /// A plain string field like any other -- give it `!env INFLUXDB_TOKEN` in config to
        /// pull it from the environment (`crates/logit-cli/src/config.rs`) rather than inlining
        /// it. No env-specific field of its own: `!env` works on any field on any component, so
        /// `url`/`org`/`bucket` (just as deployment-specific) can use it too.
        token: String,
    },
    OtlpOut {
        endpoint: String,
    },
    /// The native logit-to-logit protocol (`docs/design/wire-protocol.md`).
    LogitOut {
        endpoint: String,
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
    /// "humantime_serde_duration::option")]`) -- used by the Lua component kinds' optional
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
    // YAML are both self-describing formats, so this exercises the same tagged-enum
    // disambiguation the real deserializer does.

    #[test]
    fn lua_component_without_interval_deserializes() {
        let component: Component =
            serde_json::from_str(r#"{"type": "lua", "sources": ["in"], "script": "return event"}"#)
                .unwrap();
        assert_eq!(component.sources, vec!["in".to_string()]);
        match component.kind {
            ComponentKind::Lua { script, interval } => {
                assert_eq!(script, "return event");
                assert_eq!(interval, None);
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn lua_component_with_interval_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "lua", "sources": ["in"], "script": "return event", "interval": "10s"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Lua { interval, .. } => {
                assert_eq!(interval, Some(Duration::from_secs(10)));
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn lua_file_component_with_interval_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "lua_file", "sources": ["in"], "lua_file": "x.lua", "interval": "1m"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::LuaFile { lua_file, interval } => {
                assert_eq!(lua_file, "x.lua");
                assert_eq!(interval, Some(Duration::from_secs(60)));
            }
            other => panic!("expected LuaFile, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_component_with_interval_deserializes() {
        let component: Component =
            serde_json::from_str(r#"{"type": "aggregate", "sources": ["in"], "interval": "10s"}"#)
                .unwrap();
        match component.kind {
            ComponentKind::Aggregate { interval } => assert_eq!(interval, Duration::from_secs(10)),
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn json_component_without_skip_to_brace_defaults_to_false() {
        let component: Component =
            serde_json::from_str(r#"{"type": "json", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::Json { skip_to_brace } => assert!(!skip_to_brace),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn json_component_with_skip_to_brace_deserializes() {
        let component: Component =
            serde_json::from_str(r#"{"type": "json", "sources": ["in"], "skip_to_brace": true}"#)
                .unwrap();
        match component.kind {
            ComponentKind::Json { skip_to_brace } => assert!(skip_to_brace),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn kv_metrics_component_round_trips_through_deserialization() {
        let component: Component = serde_json::from_str(
            r#"{"type": "kv_metrics", "sources": ["in"],
                "counters": [{"name": "nginx.requests"},
                             {"name": "nginx.bytes_sent", "field": "body_bytes_sent"}],
                "distributions": [{"name": "nginx.request_time", "field": "request_time",
                                    "unit": "s"}]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::KvMetrics { counters, gauges, distributions } => {
                assert_eq!(counters.len(), 2);
                assert_eq!(counters[0].name, "nginx.requests");
                assert_eq!(counters[0].field, None);
                assert_eq!(counters[1].field, Some("body_bytes_sent".to_string()));
                assert!(gauges.is_empty());
                assert_eq!(distributions.len(), 1);
                assert_eq!(distributions[0].unit, Some("s".to_string()));
            }
            other => panic!("expected KvMetrics, got {other:?}"),
        }
    }

    #[test]
    fn kv_metrics_component_defaults_every_list_to_empty() {
        let component: Component =
            serde_json::from_str(r#"{"type": "kv_metrics", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::KvMetrics { counters, gauges, distributions } => {
                assert!(counters.is_empty());
                assert!(gauges.is_empty());
                assert!(distributions.is_empty());
            }
            other => panic!("expected KvMetrics, got {other:?}"),
        }
    }

    #[test]
    fn keep_component_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "keep", "sources": ["in"], "fields": ["status", "method"]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Keep { fields } => {
                assert_eq!(fields, vec!["status".to_string(), "method".to_string()]);
            }
            other => panic!("expected Keep, got {other:?}"),
        }
    }

    #[test]
    fn remove_component_deserializes_with_multiple_fields() {
        let component: Component = serde_json::from_str(
            r#"{"type": "remove", "sources": ["in"], "fields": ["client_ip", "user_agent"]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Remove { fields } => {
                assert_eq!(fields, vec!["client_ip".to_string(), "user_agent".to_string()]);
            }
            other => panic!("expected Remove, got {other:?}"),
        }
    }

    #[test]
    fn component_with_no_sources_defaults_to_empty() {
        let component: Component =
            serde_json::from_str(r#"{"type": "statsd_in", "bind": "0.0.0.0:8125"}"#).unwrap();
        assert!(component.sources.is_empty());
        assert!(matches!(component.kind, ComponentKind::StatsdIn { .. }));
    }

    #[test]
    fn sink_component_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["enrich"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN"}"#,
        )
        .unwrap();
        assert_eq!(component.sources, vec!["enrich".to_string()]);
        assert!(matches!(component.kind, ComponentKind::InfluxDbOut { .. }));
    }

    #[test]
    fn zero_interval_deserializes_fine_left_for_validation_to_reject() {
        // The codec itself has no opinion on zero -- graph validation (`logit-pipeline`) is where
        // a zero flush interval is actually rejected (it would spin the flush loop).
        let component: Component =
            serde_json::from_str(r#"{"type": "lua", "script": "x", "interval": "0s"}"#).unwrap();
        match component.kind {
            ComponentKind::Lua { interval, .. } => assert_eq!(interval, Some(Duration::ZERO)),
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn negative_interval_is_rejected_by_the_codec() {
        let result: Result<Component, _> =
            serde_json::from_str(r#"{"type": "lua", "script": "x", "interval": "-5s"}"#);
        assert!(result.is_err(), "a negative duration should not silently parse");
    }

    #[test]
    fn interval_round_trips_through_serialize_then_deserialize() {
        let original = Component {
            sources: vec!["in".to_string()],
            kind: ComponentKind::Lua {
                script: "x".to_string(),
                interval: Some(Duration::from_secs(30)),
            },
        };
        let json = serde_json::to_string(&original).unwrap();
        let round_tripped: Component = serde_json::from_str(&json).unwrap();
        match round_tripped.kind {
            ComponentKind::Lua { interval, .. } => {
                assert_eq!(interval, Some(Duration::from_secs(30)));
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn unknown_type_tag_is_a_clear_error() {
        let result: Result<Component, _> = serde_json::from_str(r#"{"type": "nonsense"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn components_schema_requires_at_least_one_entry() {
        let schema = json_schema();
        let components = schema
            .schema
            .object
            .expect("config should be an object")
            .properties
            .remove("components")
            .expect("config should define components");
        let Schema::Object(components) = components else {
            panic!("components should have an object schema");
        };
        assert_eq!(
            components.object.expect("components should be an object").min_properties,
            Some(1)
        );
    }
}
