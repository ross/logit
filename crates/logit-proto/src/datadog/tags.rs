//! Datadog tag lists (`["env:prod", "team:a", "urgent"]`) to and from attributes, the rule every
//! Datadog codec here shares with `statsd_in`'s DogStatsD `|#` segment
//! (`crates/logit-proto/src/statsd/decode.rs`'s `insert_tags`): the key ends at the first `:`, a
//! bare token is `Bool(true)`, a repeated key folds into an `Array` in wire order, and only an
//! exact duplicate token is deduped (the Agent's own rule; `urgent` and `urgent:1` both survive).
//!
//! A key under the private `_logit.` prefix is skipped and returned to the caller to count as
//! `logit.input.tags.omitted{reason="reserved_prefix"}`, so a sender's tag never reads back as a
//! carrier
//! ([ADR `private-attribute-namespace`](../../../../docs/adr/private-attribute-namespace.md)).
//!
//! Unified Service Tagging's valued `env:`, `version:`, and `service:` tags are the standard
//! `deployment.environment.name`, `service.version`, and `service.name`
//! ([`logit_core::attrs::UNIFIED_SERVICE_TAGS`]), both ways, as `statsd_in`'s module doc,
//! "Service identity", has it for DogStatsD: a tag spelling the standard key beside the
//! Datadog-spelled one is returned to count as `reason="reserved_key"`, and [`render_tags`] writes
//! the key in Datadog's spelling. A valueless token names no value, so it keeps its own name both
//! ways.

use logit_core::attrs::{is_private, UNIFIED_SERVICE_TAGS};
use logit_core::interner::{intern, resolve};
use logit_core::{AttrMap, Symbol, Value};

/// What [`insert_tags`] left off, for the caller to count as `logit.input.tags.omitted`.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Omitted {
    /// A tag whose key is under the private `_logit.` prefix (`reason="reserved_prefix"`).
    pub reserved_prefix: usize,
    /// A tag spelling a standard service identity key beside the Datadog-spelled tag for it
    /// (`reason="reserved_key"`).
    pub reserved_key: usize,
}

/// Folds every tag of a list into `attributes` under the shared rule. A valued `env:`,
/// `version:`, or `service:` tag folds into its standard key; a valued tag spelling that key
/// itself is folded after the rest, and beside a Datadog-spelled tag for the same key it's left
/// off.
pub fn insert_tags<'a>(
    attributes: &mut AttrMap,
    tags: impl IntoIterator<Item = &'a str>,
) -> Omitted {
    let mut omitted = Omitted::default();
    // Per `UNIFIED_SERVICE_TAGS` row: whether a Datadog-spelled tag wrote the key.
    let mut unified = [false; 3];
    // Tags spelling a standard key, held until every Datadog-spelled one has been read.
    let mut spelled: Vec<(usize, Value)> = Vec::new();
    for tag in tags {
        if tag.is_empty() {
            continue;
        }
        let (key, value) = match tag.split_once(':') {
            Some((k, v)) => (k, Value::str(v)),
            None => (tag, Value::Bool(true)),
        };
        if is_private(key) {
            omitted.reserved_prefix += 1;
            continue;
        }
        let mut rows = UNIFIED_SERVICE_TAGS.iter().enumerate();
        let key = match rows.find(|(_, (dd, std))| *dd == key || *std == key) {
            Some((i, (dd, std))) if *dd == key && !matches!(value, Value::Bool(true)) => {
                unified[i] = true;
                intern(std)
            }
            Some((i, (dd, _))) if *dd != key && !matches!(value, Value::Bool(true)) => {
                spelled.push((i, value));
                continue;
            }
            _ => intern(key),
        };
        fold_tag(attributes, key, value);
    }
    for (i, value) in spelled {
        if unified[i] {
            omitted.reserved_key += 1;
        } else {
            fold_tag(attributes, intern(UNIFIED_SERVICE_TAGS[i].1), value);
        }
    }
    omitted
}

/// Folds one tag's value into `attributes` under `key`: a repeated key becomes an `Array` in wire
/// order, and only an exact duplicate token is deduped.
fn fold_tag(attributes: &mut AttrMap, key: Symbol, value: Value) {
    let merged = match attributes.remove_sym(key) {
        None => value,
        Some(Value::Array(mut arr)) => {
            if !arr.iter().any(|e| tag_element_eq(e, &value)) {
                arr.push(value);
            }
            Value::Array(arr)
        }
        Some(existing) if tag_element_eq(&existing, &value) => existing,
        Some(existing) => Value::Array(vec![existing, value]),
    };
    attributes.insert_sym(key, merged);
}

/// Splits a comma-joined `ddtags` string into tags (empty tokens dropped).
pub fn split_joined(joined: &str) -> impl Iterator<Item = &str> {
    joined.split(',').filter(|t| !t.is_empty())
}

fn tag_element_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        _ => false,
    }
}

/// What [`render_tags`] could not represent, for the caller to count
/// (`logit.output.tags.omitted{reason="unrepresentable"}`).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Dropped {
    pub unrepresentable: usize,
}

/// The standard keys a Datadog tag spells another way, the Datadog OTLP exporter's mapping
/// (`pkg/opentelemetry-mapping-go/otlp/attributes/attributes.go`, `ContainerMappings` and
/// `coreMapping`): `container.id` is the `container_id` tag, `container.image.tags`'s first
/// element the `image_tag` tag, and [`UNIFIED_SERVICE_TAGS`]' keys the `env`, `version`, and
/// `service` tags. An attribute already named for the tag would be a second one beside the key,
/// so it's left off, counted: every tag it writes for a container row, and only its valued tags
/// for a service row, since a bare `env` token names no environment.
const RESPELLED: [(&str, &str); 5] = [
    (logit_core::attrs::CONTAINER_ID, "container_id"),
    (logit_core::attrs::CONTAINER_IMAGE_TAGS, "image_tag"),
    (UNIFIED_SERVICE_TAGS[0].1, UNIFIED_SERVICE_TAGS[0].0),
    (UNIFIED_SERVICE_TAGS[1].1, UNIFIED_SERVICE_TAGS[1].0),
    (UNIFIED_SERVICE_TAGS[2].1, UNIFIED_SERVICE_TAGS[2].0),
];

/// Whether a [`RESPELLED`] row's tag-named attribute keeps its valueless tags beside the key.
fn keeps_bare(row: usize) -> bool {
    row >= 2
}

/// Renders attribute pairs as Datadog tags: `Str` → `k:v`, `Bool(true)` → `k`, `Bool(false)` →
/// `k:false`, numbers → `k:<display>`, `Timestamp` → `k:<RFC 3339>`, `Array` → one tag per
/// element under the same rules, `Map`/`Bytes`/`Null` → omitted and counted. A bare tag reads
/// back as `true`, so `true` round-trips through [`insert_tags`] and `false` reads back as the
/// string `"false"`, as `statsd_out` writes and `statsd_in` reads it. Keys in `skip` are the ones a
/// caller already consumed into a wire field of its own (`host.name`, the `_logit.datadog.*`
/// carriers) and are never rendered, and neither is any other private `_logit.` key, which is
/// never counted.
///
/// `container.id` renders as `container_id`, `container.image.tags` as one `image_tag` from its
/// first element, each further element counted unrepresentable, and `deployment.environment.name`,
/// `service.version`, and `service.name` as `env`, `version`, and `service`, a bare token among
/// their values under the key's own name ([`RESPELLED`]). A
/// `container_id` or `image_tag` attribute beside the key that renders one is left off, every tag
/// it would have written counted unrepresentable; an `env`, `version`, or `service` attribute
/// beside its key loses its valued tags the same way and keeps its bare ones.
pub fn render_tags<'a>(
    pairs: impl IntoIterator<Item = (Symbol, &'a Value)>,
    skip: &[Symbol],
    out: &mut Vec<String>,
) -> Dropped {
    let mut dropped = Dropped::default();
    // Per `RESPELLED` row: whether the standard key wrote a tag, and where the attribute already
    // named for that tag wrote its own.
    let mut keyed = [false; RESPELLED.len()];
    let mut named: [Option<std::ops::Range<usize>>; RESPELLED.len()] = Default::default();
    for (key, value) in pairs {
        let name = resolve(key);
        if skip.contains(&key) || logit_core::attrs::is_private(name) {
            continue;
        }
        let start = out.len();
        if let Some(i) = RESPELLED.iter().position(|(standard, _)| *standard == name) {
            let wire = RESPELLED[i].1;
            match value {
                Value::Array(items) if i == 1 => {
                    if let Some(first) = items.first() {
                        render_one(wire, first, out, &mut dropped);
                    }
                    dropped.unrepresentable += items.len().saturating_sub(1);
                }
                Value::Array(items) if keeps_bare(i) => {
                    for item in items {
                        render_service(name, wire, item, out, &mut dropped);
                    }
                }
                _ if keeps_bare(i) => render_service(name, wire, value, out, &mut dropped),
                _ => render_one(wire, value, out, &mut dropped),
            }
            keyed[i] |= out[start..].iter().any(|tag| tag.as_str() != name);
            continue;
        }
        render_one(name, value, out, &mut dropped);
        if let Some(i) = RESPELLED.iter().position(|(_, wire)| *wire == name) {
            named[i] = Some(start..out.len());
        }
    }
    let mut shadowed: Vec<usize> = Vec::new();
    for i in (0..RESPELLED.len()).filter(|&i| keyed[i]) {
        let Some(range) = named[i].clone() else { continue };
        let bare = RESPELLED[i].1;
        shadowed.extend(range.filter(|&at| !(keeps_bare(i) && out[at] == bare)));
    }
    shadowed.sort_unstable_by(|a, b| b.cmp(a));
    for at in shadowed {
        dropped.unrepresentable += 1;
        out.remove(at);
    }
    dropped
}

/// Renders one value of a service identity key: a bare token (`true`) under the key's own name,
/// since a bare `env` would name no environment, and anything else under Datadog's `wire` name.
fn render_service(
    name: &str,
    wire: &str,
    value: &Value,
    out: &mut Vec<String>,
    dropped: &mut Dropped,
) {
    let key = if matches!(value, Value::Bool(true)) { name } else { wire };
    render_one(key, value, out, dropped);
}

fn render_one(key: &str, value: &Value, out: &mut Vec<String>, dropped: &mut Dropped) {
    match value {
        Value::Str(s) => out.push(format!("{key}:{}", String::from_utf8_lossy(s))),
        Value::Bool(true) => out.push(key.to_string()),
        Value::Bool(false) => out.push(format!("{key}:false")),
        Value::I64(n) => out.push(format!("{key}:{n}")),
        Value::U64(n) => out.push(format!("{key}:{n}")),
        Value::F64(n) => out.push(format!("{key}:{n}")),
        Value::Timestamp(ns) => out.push(format!("{key}:{}", logit_core::format_rfc3339_utc(*ns))),
        Value::Array(items) => {
            for item in items {
                render_one(key, item, out, dropped);
            }
        }
        Value::Map(_) | Value::Bytes(_) | Value::Null => dropped.unrepresentable += 1,
    }
}

/// How many tags [`render_tags`] would write for the same pairs and `skip`, without formatting
/// any of them.
pub fn count_tags<'a>(
    pairs: impl IntoIterator<Item = (Symbol, &'a Value)>,
    skip: &[Symbol],
) -> usize {
    let mut keyed = [false; RESPELLED.len()];
    let mut named = [0usize; RESPELLED.len()];
    let mut total = 0;
    for (key, value) in pairs {
        let name = resolve(key);
        if skip.contains(&key) || logit_core::attrs::is_private(name) {
            continue;
        }
        let n = match RESPELLED.iter().position(|(standard, _)| *standard == name) {
            Some(i) => {
                let n = match value {
                    Value::Array(items) if i == 1 => items.first().map_or(0, count_one),
                    _ => count_one(value),
                };
                keyed[i] |= if keeps_bare(i) { n > count_bare(value) } else { n > 0 };
                n
            }
            None => {
                let n = count_one(value);
                if let Some(i) = RESPELLED.iter().position(|(_, wire)| *wire == name) {
                    named[i] = if keeps_bare(i) { n - count_bare(value) } else { n };
                }
                n
            }
        };
        total += n;
    }
    total - (0..RESPELLED.len()).filter(|&i| keyed[i]).map(|i| named[i]).sum::<usize>()
}

/// How many valueless tags (`Bool(true)`) a value renders.
fn count_bare(value: &Value) -> usize {
    match value {
        Value::Bool(true) => 1,
        Value::Array(items) => items.iter().map(count_bare).sum(),
        _ => 0,
    }
}

fn count_one(value: &Value) -> usize {
    match value {
        Value::Str(_)
        | Value::Bool(_)
        | Value::I64(_)
        | Value::U64(_)
        | Value::F64(_)
        | Value::Timestamp(_) => 1,
        Value::Array(items) => items.iter().map(count_one).sum(),
        Value::Map(_) | Value::Bytes(_) | Value::Null => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_fold_like_dogstatsd_tags() {
        let mut attrs = AttrMap::new();
        insert_tags(
            &mut attrs,
            ["env:prod", "team:a", "team:b", "team:a", "urgent", "urgent:1", ""],
        );
        assert_eq!(attrs.get("deployment.environment.name"), Some(&Value::str("prod")));
        assert_eq!(attrs.get("team"), Some(&Value::Array(vec![Value::str("a"), Value::str("b")])));
        assert_eq!(
            attrs.get("urgent"),
            Some(&Value::Array(vec![Value::Bool(true), Value::str("1")]))
        );
    }

    #[test]
    fn a_private_tag_is_skipped_and_returned_and_lookalikes_are_not() {
        let mut attrs = AttrMap::new();
        let skipped = insert_tags(
            &mut attrs,
            ["env:prod", "_logit.metrics.interval:10", "_logit.x", "logit.x:1", "_logitx"],
        );
        assert_eq!(skipped, Omitted { reserved_prefix: 2, reserved_key: 0 });
        assert_eq!(attrs.get("deployment.environment.name"), Some(&Value::str("prod")));
        assert_eq!(attrs.get("logit.x"), Some(&Value::str("1")));
        assert_eq!(attrs.get("_logitx"), Some(&Value::Bool(true)));
        assert_eq!(attrs.len(), 3);
    }

    #[test]
    fn rendering_inverts_folding_and_counts_the_unrepresentable() {
        let mut attrs = AttrMap::new();
        insert_tags(&mut attrs, ["env:prod", "urgent", "team:a", "team:b"]);
        attrs.insert("n", Value::I64(3));
        attrs.insert("off", Value::Bool(false));
        attrs.insert("nested", Value::Map(Box::new(AttrMap::new())));
        attrs.insert("skipped", Value::str("x"));
        let mut out = Vec::new();
        let dropped = render_tags(attrs.iter(), &[intern("skipped")], &mut out);
        out.sort();
        assert_eq!(out, ["env:prod", "n:3", "off:false", "team:a", "team:b", "urgent"]);
        assert_eq!(dropped, Dropped { unrepresentable: 1 }, "`nested`");
    }

    /// A valued `env:`, `version:`, or `service:` tag is its standard key and a valueless one an
    /// ordinary tag; a tag spelling the key beside the Datadog-spelled one is left off and
    /// counted, whichever comes first, and alone it's the key's value.
    #[test]
    fn unified_service_tags_fold_into_the_standard_keys() {
        let mut attrs = AttrMap::new();
        let omitted = insert_tags(
            &mut attrs,
            [
                "deployment.environment.name:dev",
                "env:prod",
                "env:canary",
                "env",
                "version:1.2",
                "service.version:9",
                "service.version:8",
                "service.name:api",
                "service",
            ],
        );
        assert_eq!(omitted, Omitted { reserved_prefix: 0, reserved_key: 3 });
        assert_eq!(
            attrs.get("deployment.environment.name"),
            Some(&Value::Array(vec![Value::str("prod"), Value::str("canary")]))
        );
        assert_eq!(attrs.get("env"), Some(&Value::Bool(true)));
        assert_eq!(attrs.get("service.version"), Some(&Value::str("1.2")));
        assert_eq!(attrs.get("service.name"), Some(&Value::str("api")));
        assert_eq!(attrs.get("service"), Some(&Value::Bool(true)));
        assert_eq!(attrs.get("version"), None);
        assert_eq!(attrs.len(), 5);
    }

    /// The service identity keys render in Datadog's spelling. Beside its key, an `env`,
    /// `version`, or `service` attribute loses its valued tags, counted, and keeps its bare ones;
    /// without the key it's an ordinary tag. Counting agrees.
    #[test]
    fn service_keys_render_in_datadog_spelling_and_shadow_their_valued_tag_attributes() {
        let mut attrs = AttrMap::new();
        attrs.insert("deployment.environment.name", Value::str("prod"));
        attrs.insert("env", Value::Array(vec![Value::str("dev"), Value::Bool(true)]));
        attrs.insert("service.version", Value::str("1.2"));
        attrs.insert("version", Value::str("9"));
        attrs.insert("service.name", Value::Array(vec![Value::str("api"), Value::str("web")]));
        let mut out = Vec::new();
        let dropped = render_tags(attrs.iter(), &[], &mut out);
        out.sort();
        assert_eq!(out, ["env", "env:prod", "service:api", "service:web", "version:1.2"]);
        assert_eq!(dropped, Dropped { unrepresentable: 2 }, "env:dev and version:9");
        assert_eq!(count_tags(attrs.iter(), &[]), out.len());

        // A bare token under a key stays under the key's own name and shadows nothing.
        let mut bare = AttrMap::new();
        bare.insert("service.name", Value::Array(vec![Value::Bool(true), Value::str("api")]));
        bare.insert("deployment.environment.name", Value::Bool(true));
        bare.insert("env", Value::str("dev"));
        let mut out = Vec::new();
        let dropped = render_tags(bare.iter(), &[], &mut out);
        out.sort();
        assert_eq!(out, ["deployment.environment.name", "env:dev", "service.name", "service:api"]);
        assert_eq!(dropped, Dropped::default());
        assert_eq!(count_tags(bare.iter(), &[]), out.len());
        let mut back = AttrMap::new();
        insert_tags(&mut back, out.iter().map(String::as_str));
        assert_eq!(back.get("service.name"), bare.get("service.name"));

        let mut alone = AttrMap::new();
        alone.insert("env", Value::str("dev"));
        alone.insert("service", Value::str("api"));
        let mut out = Vec::new();
        let dropped = render_tags(alone.iter(), &[], &mut out);
        out.sort();
        assert_eq!(out, ["env:dev", "service:api"]);
        assert_eq!(dropped, Dropped::default());
        assert_eq!(count_tags(alone.iter(), &[]), 2);

        let mut back = AttrMap::new();
        insert_tags(&mut back, ["env:prod", "service:api", "service:web", "version:1.2"]);
        let mut keyed = AttrMap::new();
        keyed.insert("deployment.environment.name", Value::str("prod"));
        keyed.insert("service.version", Value::str("1.2"));
        keyed.insert("service.name", Value::Array(vec![Value::str("api"), Value::str("web")]));
        assert_eq!(back, keyed);
    }

    /// `count_tags` agrees with `render_tags` on every `Value` variant, nested arrays and
    /// duplicates included.
    #[test]
    fn counting_agrees_with_rendering() {
        let mut attrs = AttrMap::new();
        attrs.insert("s", Value::str("x"));
        attrs.insert("on", Value::Bool(true));
        attrs.insert("off", Value::Bool(false));
        attrs.insert("i", Value::I64(-1));
        attrs.insert("u", Value::U64(1));
        attrs.insert("f", Value::F64(1.5));
        attrs.insert("t", Value::Timestamp(0));
        attrs.insert("m", Value::Map(Box::new(AttrMap::new())));
        attrs.insert("b", Value::Bytes(vec![1].into()));
        attrs.insert("null", Value::Null);
        attrs.insert(
            "a",
            Value::Array(vec![
                Value::str("x"),
                Value::str("x"),
                Value::Bool(false),
                Value::Null,
                Value::Array(vec![Value::I64(1), Value::Bool(true)]),
            ]),
        );
        attrs.insert("skipped", Value::str("x"));
        let skip = [intern("skipped")];
        let mut out = Vec::new();
        render_tags(attrs.iter(), &skip, &mut out);
        assert_eq!(count_tags(attrs.iter(), &skip), out.len());
        assert_eq!(out.len(), 12);
    }

    /// `container.id` is the `container_id` tag and `container.image.tags`'s first element the
    /// `image_tag` tag, its others counted; a `container_id` or `image_tag` attribute beside the
    /// key is left off and counted, and alone it's an ordinary tag. Counting agrees.
    #[test]
    fn container_keys_render_in_datadog_spelling_and_shadow_their_tag_attributes() {
        let mut attrs = AttrMap::new();
        attrs.insert("container.id", Value::str("abc"));
        attrs.insert("container_id", Value::Array(vec![Value::str("x"), Value::str("y")]));
        attrs.insert(
            "container.image.tags",
            Value::Array(vec![Value::str("v1"), Value::str("latest")]),
        );
        attrs.insert("env", Value::str("prod"));
        let mut out = Vec::new();
        let dropped = render_tags(attrs.iter(), &[], &mut out);
        out.sort();
        assert_eq!(out, ["container_id:abc", "env:prod", "image_tag:v1"]);
        assert_eq!(dropped, Dropped { unrepresentable: 3 }, "x, y, and latest");
        assert_eq!(count_tags(attrs.iter(), &[]), out.len());

        let mut alone = AttrMap::new();
        alone.insert("container_id", Value::str("x"));
        alone.insert("image_tag", Value::str("v0"));
        let mut out = Vec::new();
        let dropped = render_tags(alone.iter(), &[], &mut out);
        out.sort();
        assert_eq!(out, ["container_id:x", "image_tag:v0"]);
        assert_eq!(dropped, Dropped::default());
        assert_eq!(count_tags(alone.iter(), &[]), 2);
    }

    /// `false` leaves as `key:false` and reads back as the string; `true` leaves bare and reads
    /// back as itself.
    #[test]
    fn a_false_attribute_is_a_key_false_tag() {
        let mut attrs = AttrMap::new();
        attrs.insert("on", Value::Bool(true));
        attrs.insert("off", Value::Bool(false));
        let mut out = Vec::new();
        let dropped = render_tags(attrs.iter(), &[], &mut out);
        out.sort();
        assert_eq!(out, ["off:false", "on"]);
        assert_eq!(dropped, Dropped::default());
        let mut back = AttrMap::new();
        insert_tags(&mut back, out.iter().map(String::as_str));
        assert_eq!(back.get("on"), Some(&Value::Bool(true)));
        assert_eq!(back.get("off"), Some(&Value::str("false")));
    }

    #[test]
    fn joined_ddtags_split_on_commas() {
        assert_eq!(
            split_joined("env:prod,,version:1").collect::<Vec<_>>(),
            ["env:prod", "version:1"]
        );
    }
}
