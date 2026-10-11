//! Datadog tag lists (`["env:prod", "team:a", "urgent"]`) to and from attributes, the rule every
//! Datadog codec here shares with `statsd_in`'s DogStatsD `|#` segment
//! (`crates/logit-proto/src/statsd/decode.rs`'s `insert_tags`): the key ends at the first `:`, a
//! bare token is `Bool(true)`, a repeated key folds into an `Array` in wire order, and only an
//! exact duplicate token is deduped (the Agent's own rule; `urgent` and `urgent:1` both survive).

use logit_core::interner::{intern, resolve};
use logit_core::{AttrMap, Symbol, Value};

/// Folds one tag into `attributes` under the shared rule.
pub fn insert_tag(attributes: &mut AttrMap, tag: &str) {
    if tag.is_empty() {
        return;
    }
    let (key, value) = match tag.split_once(':') {
        Some((k, v)) => (k, Value::str(v)),
        None => (tag, Value::Bool(true)),
    };
    let key = intern(key);
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

/// Folds every tag of a list into `attributes`.
pub fn insert_tags<'a>(attributes: &mut AttrMap, tags: impl IntoIterator<Item = &'a str>) {
    for tag in tags {
        insert_tag(attributes, tag);
    }
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
/// (`logit.output.tags.dropped{reason="unrepresentable"}`).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Dropped {
    pub unrepresentable: usize,
}

/// Renders attribute pairs as Datadog tags: `Str` → `k:v`, `Bool(true)` → `k`, `Bool(false)` →
/// nothing, numbers → `k:<display>`, `Timestamp` → `k:<RFC 3339>`, `Array` → one tag per element
/// under the same rules, `Map`/`Bytes`/`Null` → dropped and counted. Keys in `skip` are the ones a
/// caller already consumed into a wire field of its own (`host.name`, the `datadog.*` carriers)
/// and are never rendered.
pub fn render_tags<'a>(
    pairs: impl IntoIterator<Item = (Symbol, &'a Value)>,
    skip: &[Symbol],
    out: &mut Vec<String>,
) -> Dropped {
    let mut dropped = Dropped::default();
    for (key, value) in pairs {
        if skip.contains(&key) {
            continue;
        }
        render_one(resolve(key), value, out, &mut dropped);
    }
    dropped
}

fn render_one(key: &str, value: &Value, out: &mut Vec<String>, dropped: &mut Dropped) {
    match value {
        Value::Str(s) => out.push(format!("{key}:{}", String::from_utf8_lossy(s))),
        Value::Bool(true) => out.push(key.to_string()),
        Value::Bool(false) => {}
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
    pairs
        .into_iter()
        .filter(|(key, _)| !skip.contains(key))
        .map(|(_, value)| count_one(value))
        .sum()
}

fn count_one(value: &Value) -> usize {
    match value {
        Value::Str(_)
        | Value::Bool(true)
        | Value::I64(_)
        | Value::U64(_)
        | Value::F64(_)
        | Value::Timestamp(_) => 1,
        Value::Array(items) => items.iter().map(count_one).sum(),
        Value::Bool(false) | Value::Map(_) | Value::Bytes(_) | Value::Null => 0,
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
        assert_eq!(attrs.get("env"), Some(&Value::str("prod")));
        assert_eq!(attrs.get("team"), Some(&Value::Array(vec![Value::str("a"), Value::str("b")])));
        assert_eq!(
            attrs.get("urgent"),
            Some(&Value::Array(vec![Value::Bool(true), Value::str("1")]))
        );
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
        assert_eq!(out, ["env:prod", "n:3", "team:a", "team:b", "urgent"]);
        assert_eq!(dropped, Dropped { unrepresentable: 1 });
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
        assert_eq!(out.len(), 10);
    }

    #[test]
    fn joined_ddtags_split_on_commas() {
        assert_eq!(
            split_joined("env:prod,,version:1").collect::<Vec<_>>(),
            ["env:prod", "version:1"]
        );
    }
}
