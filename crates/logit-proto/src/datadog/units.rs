//! A metric's unit as Datadog names it: the OTLP (UCUM) unit mapped as Datadog's OTel exporter
//! maps it, or a Datadog unit name passed through, so a series `datadog_out` writes carries a
//! unit Datadog recognizes or none.
//!
//! The UCUM mapping is a transcription of `UnitMapper.Map` in
//! `pkg/opentelemetry-mapping-go/otlp/attributes/units.go` at datadog-agent commit `45132452`
//! (the pin `tools/differential/datadog-sketch/go.mod` uses), which the translator applies to
//! every series under the exporter's `AddUnits` gate, on by default:
//!
//! - `""` and `1` (dimensionless) map to no unit.
//! - A rate (`By/s`) maps each side of its one `/`; a unit with two or more `/` doesn't map.
//! - An annotation (`{request}`) maps to its name when Datadog has a unit of that name
//!   ([`ANNOTATIONS`]), and `{datapoint}` to `item`.
//! - Any other component is a base unit ([`BASE_UNITS`]), bare or after a prefix ([`PREFIXES`])
//!   that the base unit lists as forming a Datadog unit: `KiBy` is `kibibyte`, `mBy` doesn't map.
//!
//! A Datadog unit name, which a `datadog_in` series carries, passes through unchanged ahead of
//! the mapping: any name the mapping can produce, a rate of two of them, and `nanocore`, which
//! the translator's own `metrics_remapping.go` writes. Datadog's unit catalog has names outside
//! that set; one of those is left off like an unmapped UCUM unit.

use std::borrow::Cow;

/// UCUM annotations whose name is a Datadog unit, from `allowedAnnotations`.
pub const ANNOTATIONS: &[&str] = &[
    "alert",
    "apdex",
    "assertion",
    "attempt",
    "block",
    "buffer",
    "build",
    "cent",
    "check",
    "collection",
    "column",
    "command",
    "commit",
    "connection",
    "container",
    "core",
    "cpu",
    "cursor",
    "datagram",
    "day",
    "decibel-milliwatt",
    "device",
    "document",
    "dollar",
    "email",
    "entity",
    "entry",
    "error",
    "euro",
    "event",
    "eviction",
    "exception",
    "execution",
    "fault",
    "fetch",
    "file",
    "flush",
    "fraction",
    "frame",
    "get",
    "heap",
    "hit",
    "hop",
    "host",
    "index",
    "inode",
    "instance",
    "invocation",
    "item",
    "job",
    "key",
    "location",
    "lock",
    "merge",
    "message",
    "method",
    "minute",
    "miss",
    "monitor",
    "node",
    "object",
    "occurrence",
    "offset",
    "operation",
    "packet",
    "page",
    "payload",
    "penny",
    "permille",
    "pound",
    "prediction",
    "process",
    "query",
    "question",
    "read",
    "record",
    "refresh",
    "request",
    "resource",
    "response",
    "route",
    "row",
    "run",
    "sample",
    "scan",
    "sector",
    "segment",
    "service",
    "session",
    "set",
    "shard",
    "span",
    "split",
    "stage",
    "step",
    "success",
    "table",
    "task",
    "thread",
    "throttle",
    "ticket",
    "time",
    "timeout",
    "token",
    "transaction",
    "unit",
    "update",
    "user",
    "vector",
    "view",
    "volume",
    "wait",
    "watt-hour",
    "week",
    "worker",
    "write",
];

/// The unit the translator's `metrics_remapping.go` sets on the CPU metrics it derives: a Datadog
/// name no UCUM unit maps to.
const NANOCORE: &str = "nanocore";

/// UCUM prefix symbols and their Datadog names, from `ucumPrefixes`, longest symbol first so a
/// binary prefix (`Mi`) matches before the decimal one sharing its first letter (`M`).
pub const PREFIXES: [(&str, &str); 16] = [
    ("Ei", "exbi"),
    ("Gi", "gibi"),
    ("Ki", "kibi"),
    ("Mi", "mebi"),
    ("Pi", "pebi"),
    ("Ti", "tebi"),
    ("E", "exa"),
    ("G", "giga"),
    ("M", "mega"),
    ("P", "peta"),
    ("T", "tera"),
    ("d", "deci"),
    ("k", "kilo"),
    ("m", "milli"),
    ("n", "nano"),
    ("u", "micro"),
];

/// UCUM base units: the symbol, its Datadog name, and the prefix symbols that form a Datadog unit
/// with it, from `ucumBaseUnits`.
pub const BASE_UNITS: [(&str, &str, &[&str]); 11] = [
    ("By", "byte", &["Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "k", "M", "G", "T", "P", "E"]),
    ("bit", "bit", &["k", "M", "G", "T", "P", "E"]),
    ("s", "second", &["m", "u", "n"]),
    ("h", "hour", &[]),
    ("Hz", "hertz", &["k", "M", "G"]),
    ("W", "watt", &["d", "m", "u", "n", "k", "M", "G", "T"]),
    ("V", "volt", &["m"]),
    ("J", "joule", &["n", "m", "k", "M"]),
    ("A", "ampere", &["m"]),
    ("Cel", "degree celsius", &["d"]),
    ("%", "percent", &[]),
];

/// The unit a series carries for a record's `unit`: a Datadog unit name as it is, else the UCUM
/// unit's Datadog name, else `None`, which the series writes as no unit.
pub fn to_datadog(unit: &str) -> Option<Cow<'_, str>> {
    if unit.is_empty() || unit == "1" {
        return None;
    }
    if is_datadog_unit(unit) {
        return Some(Cow::Borrowed(unit));
    }
    match unit.split_once('/') {
        None => from_ucum_component(unit),
        Some((_, denominator)) if denominator.contains('/') => None,
        Some((numerator, denominator)) => {
            let numerator = from_ucum_component(numerator)?;
            let denominator = from_ucum_component(denominator)?;
            Some(Cow::Owned(format!("{numerator}/{denominator}")))
        }
    }
}

/// Whether `unit` is a name [`to_datadog`] passes through: one Datadog unit name or a rate of
/// two.
fn is_datadog_unit(unit: &str) -> bool {
    match unit.split_once('/') {
        None => is_datadog_name(unit),
        Some((numerator, denominator)) => {
            !denominator.contains('/') && is_datadog_name(numerator) && is_datadog_name(denominator)
        }
    }
}

fn is_datadog_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    if name == NANOCORE || ANNOTATIONS.contains(&name) {
        return true;
    }
    BASE_UNITS.iter().any(|(_, base, prefixes)| {
        name == *base
            || name.strip_suffix(base).is_some_and(|prefix| {
                prefixes.iter().any(|p| PREFIXES.iter().any(|(s, n)| s == p && *n == prefix))
            })
    })
}

/// `mapUCUMComponent`: one side of a rate.
fn from_ucum_component(component: &str) -> Option<Cow<'_, str>> {
    if let Some(annotation) = component.strip_prefix('{').and_then(|c| c.strip_suffix('}')) {
        if annotation == "datapoint" {
            return Some(Cow::Borrowed("item"));
        }
        return ANNOTATIONS.contains(&annotation).then_some(Cow::Borrowed(annotation));
    }
    if let Some((_, name, _)) = BASE_UNITS.iter().find(|(symbol, _, _)| *symbol == component) {
        return Some(Cow::Borrowed(name));
    }
    for (symbol, prefix_name) in PREFIXES {
        let Some(base) = component.strip_prefix(symbol).filter(|b| !b.is_empty()) else {
            continue;
        };
        let Some((_, name, prefixes)) = BASE_UNITS.iter().find(|(s, _, _)| *s == base) else {
            continue;
        };
        if prefixes.contains(&symbol) {
            return Some(Cow::Owned(format!("{prefix_name}{name}")));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(unit: &str) -> Option<String> {
        to_datadog(unit).map(Cow::into_owned)
    }

    /// `TestUnitMapperMap` in the translator's `units_test.go` at the same commit, row for row,
    /// then `min` and `d`, which UCUM defines and the translator doesn't map.
    #[test]
    fn ucum_units_map_as_the_translator_maps_them() {
        for (ucum, want) in [
            ("By", Some("byte")),
            ("KiBy", Some("kibibyte")),
            ("MiBy", Some("mebibyte")),
            ("s", Some("second")),
            ("us", Some("microsecond")),
            ("ns", Some("nanosecond")),
            ("%", Some("percent")),
            ("Hz", Some("hertz")),
            ("MHz", Some("megahertz")),
            ("By/s", Some("byte/second")),
            ("GiBy/h", Some("gibibyte/hour")),
            ("1", None),
            ("", None),
            ("{cpu}", Some("cpu")),
            ("{connection}", Some("connection")),
            ("{pod}", None),
            ("{datapoint}", Some("item")),
            ("{datapoint}/s", Some("item/second")),
            ("{item}", Some("item")),
            ("kBy", Some("kilobyte")),
            ("EBy", Some("exabyte")),
            ("EiBy", Some("exbibyte")),
            ("kHz", Some("kilohertz")),
            ("kW", Some("kilowatt")),
            ("TW", Some("terawatt")),
            ("mV", Some("millivolt")),
            ("nJ", Some("nanojoule")),
            ("KiBy/s", Some("kibibyte/second")),
            ("{packet}/s", Some("packet/second")),
            ("dBy", None),
            ("mBy", None),
            ("kV", None),
            ("GJ", None),
            ("{core}", Some("core")),
            ("{millicore}", None),
            ("teaspoon", None),
            ("By/teaspoon", None),
            ("{teaspoon}", None),
            ("{packet}/teaspoon", None),
            ("{}", None),
            ("By//s", None),
            ("By/s/s", None),
            ("W.h", None),
            ("By{transmitted}", None),
            ("min", None),
            ("d", None),
            ("ms", Some("millisecond")),
            ("{nanocore}", None),
        ] {
            assert_eq!(map(ucum).as_deref(), want, "{ucum:?}");
        }
    }

    /// A Datadog unit name, a rate of two, and the translator's `nanocore` pass through.
    #[test]
    fn datadog_unit_names_pass_through() {
        for name in [
            "byte",
            "kibibyte",
            "millisecond",
            "request",
            "item",
            "fraction",
            "job",
            "percent",
            "degree celsius",
            "byte/second",
            "nanocore",
        ] {
            assert_eq!(map(name).as_deref(), Some(name), "{name:?}");
        }
        for not_a_name in ["bytes", "millibyte", "byte/second/second", "requests"] {
            assert_eq!(map(not_a_name), None, "{not_a_name:?}");
        }
    }
}
